use crate::{
    listeners::order_book::state::OrderBookState,
    metrics::{
        BBO_BROADCAST_LATENCY, EVENT_PROCESSING_LATENCY, EVENTS_PROCESSED_TOTAL, FILE_EVENTS_TOTAL,
        FILE_LINES_PARSED_TOTAL, L2_BROADCAST_LATENCY, L2_CONFLATION_BATCH_SIZE, ORDERBOOK_COINS_COUNT,
        ORDERBOOK_HEIGHT, ORDERBOOK_ORDERS_TOTAL, ORDERBOOK_TIME_MS, PARSE_ERRORS_TOTAL, PENDING_DIFFS_CACHE,
        PENDING_ORDERS_CACHE,
    },
    order_book::{
        Coin, Px, Snapshot, Sz,
        multi_book::{Snapshots, load_snapshots_from_cli_json},
    },
    prelude::*,
    types::{
        L4Order,
        inner::{InnerL4Order, InnerLevel},
        node_data::{Batch, EventSource, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use alloy::primitives::Address;
use log::{error, info};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{
        Mutex,
        broadcast::Sender,
        mpsc::{UnboundedSender, unbounded_channel},
    },
    time::{Instant, MissedTickBehavior, interval, sleep},
};
use utils::{EventBatch, SnapshotConfig, get_visor_path, process_rmp_file};

/// Minimum interval between L2 broadcasts. Caps the broadcast rate at 20/sec; the
/// conflation buffer accumulates dirty coins between broadcasts.
const L2_BROADCAST_THROTTLE_MS: u64 = 50;
/// How often the main loop polls to flush the conflation buffer. Must be << the
/// throttle so a quiet node between block flushes can never starve the L2 feed
/// for more than ~throttle + tick.
const L2_FLUSH_TICK_MS: u64 = 10;

mod parallel;
mod state;
mod utils;

fn fetch_snapshot(
    snapshot_config: SnapshotConfig,
    listener: Arc<Mutex<OrderBookListener>>,
    tx: UnboundedSender<Result<()>>,
    _ignore_spot: bool,
) {
    let tx = tx.clone();
    tokio::spawn(async move {
        // CRITICAL: Start caching BEFORE generating snapshot
        // This ensures we don't miss any events during snapshot generation.
        // We don't clone the existing state here - it's discarded by init_from_snapshot
        // below, and cloning the whole BTreeMap/Slab tree temporarily doubles peak RSS.
        {
            let mut listener = listener.lock().await;
            listener.begin_caching();
        }

        // Now generate snapshot - any events during this time are cached
        let visor_path = get_visor_path(&snapshot_config);
        let res = match process_rmp_file(&snapshot_config).await {
            Ok(output_fln) => {
                let snapshot =
                    load_snapshots_from_cli_json::<InnerL4Order, (Address, L4Order)>(&output_fln, &visor_path).await;
                info!("Snapshot fetched");
                // sleep to let some updates build up.
                sleep(Duration::from_secs(1)).await;
                let _cache = {
                    let mut listener = listener.lock().await;
                    listener.take_cache()
                };
                match snapshot {
                    Ok((height, expected_snapshot)) => {
                        info!("Snapshot loaded at height {}", height);
                        // Always reinitialize from snapshot to get fresh, accurate orderbook
                        // This corrects any drift from missed streaming updates
                        listener.lock().await.init_from_snapshot(expected_snapshot, height);
                        Ok(())
                    }
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(err),
        };
        let _unused = tx.send(res);
        Ok::<(), Error>(())
    });
}

pub(crate) struct OrderBookListener {
    ignore_spot: bool,
    // None if we haven't seen a valid snapshot yet
    order_book_state: Option<OrderBookState>,
    // Only Some when we want it to collect updates
    fetched_snapshot_cache: Option<VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)>>,
    internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
    // Throttle L2 broadcasts to prevent flooding clients
    last_l2_broadcast: Option<Instant>,
    // Incremental L2 snapshot cache. Each per-coin entry is Arc'd and shared with
    // the broadcast Arc, so unchanged coins cost an atomic bump rather than a
    // full level-vector clone. Invalidated in `init_from_snapshot`.
    l2_snapshot_cache: HashMap<Coin, Arc<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>>,
    // Coin-level conflation buffer for throttled L2 broadcasts. Every event unions
    // its changed_coins here; each L2 broadcast drains the full set so no coin
    // starves during throttle-suppressed windows. Without this, a coin that changed
    // during a suppressed 50ms window was never marked for rebuild and served a
    // stale cached snapshot until a later event for that exact coin happened to land
    // on an open throttle slot. Mutated only under the listener lock (like
    // last_l2_broadcast / l2_snapshot_cache). Bounded by the universe size.
    pending_dirty_l2_coins: HashSet<Coin>,
    // Shared registry of L2 variant shapes any live connection wants. Read at flush
    // time so we compute only subscribed variants per coin instead of all 7.
    active_l2_params: ActiveL2Params,
    // The active variant set used for the last L2 build. When it changes (a new
    // shape is subscribed, or the last subscriber of a shape leaves), the cache holds
    // the wrong shapes, so we clear it to force a full rebuild against the new set -
    // this is what lets a brand-new subscriber be served within one throttle window
    // even on an otherwise-quiet coin.
    last_active_l2_params: HashSet<L2SnapshotParams>,
}

impl OrderBookListener {
    pub(crate) fn new(
        internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
        ignore_spot: bool,
        active_l2_params: ActiveL2Params,
    ) -> Self {
        Self {
            ignore_spot,
            order_book_state: None,
            fetched_snapshot_cache: None,
            internal_message_tx,
            last_l2_broadcast: None,
            l2_snapshot_cache: HashMap::new(),
            pending_dirty_l2_coins: HashSet::new(),
            active_l2_params,
            last_active_l2_params: HashSet::new(),
        }
    }

    /// Clone of the shared active-variant registry, for handing to connections.
    pub(crate) fn active_l2_params(&self) -> ActiveL2Params {
        self.active_l2_params.clone()
    }

    pub(crate) const fn is_ready(&self) -> bool {
        self.order_book_state.is_some()
    }

    pub(crate) fn universe(&self) -> HashSet<Coin> {
        self.order_book_state.as_ref().map_or_else(HashSet::new, OrderBookState::compute_universe)
    }

    fn begin_caching(&mut self) {
        self.fetched_snapshot_cache = Some(VecDeque::new());
    }

    // take the cached updates and stop collecting updates
    fn take_cache(&mut self) -> VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        self.fetched_snapshot_cache.take().unwrap_or_default()
    }

    fn init_from_snapshot(&mut self, snapshot: Snapshots<InnerL4Order>, height: u64) {
        info!("Initializing from snapshot at height {}", height);
        // On initial startup, just trust the snapshot and start fresh
        // Don't try to apply cached updates - they may have gaps
        let new_order_book = OrderBookState::from_snapshot(snapshot, height, 0, true, self.ignore_spot);
        self.order_book_state = Some(new_order_book);
        // The incremental L2 cache references the previous book's coins/levels;
        // drop it so the next broadcast does a full rebuild against the new state.
        self.l2_snapshot_cache = HashMap::new();
        // The conflation buffer references the previous universe; clear it too. The
        // cache reset above makes every present coin uncached, so the next broadcast
        // recomputes all of them fresh regardless.
        self.pending_dirty_l2_coins.clear();
        // Force the next flush to treat the active variant set as "changed" so the
        // empty cache is rebuilt against whatever shapes are currently subscribed.
        self.last_active_l2_params.clear();
        // Clear any stale cache
        self.fetched_snapshot_cache = None;
        info!("Order book ready at height {}", height);
    }

    // forcibly grab current snapshot
    pub(crate) fn compute_snapshot(&mut self) -> Option<TimedSnapshots> {
        self.order_book_state.as_mut().map(|o| o.compute_snapshot())
    }
}

impl OrderBookListener {
    /// HFT version of process_data - doesn't skip first line errors since we're processing complete JSON lines
    pub(crate) fn process_data_hft(&mut self, line: String, event_source: EventSource) -> Result<()> {
        /// Largest batch we'll process. Each event is a few hundred bytes; a 100k-event
        /// batch would already block the listener for seconds and pin hundreds of MB.
        /// In normal operation a single block's batch is tens to low thousands of events.
        const MAX_EVENTS_PER_BATCH: usize = 100_000;
        // Count events for debugging
        static HFT_EVENT_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let count = HFT_EVENT_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if count % 1000 == 0 {
            info!("process_data_hft event #{}, source: {}, line_len: {}", count, event_source, line.len());
        }

        if line.is_empty() {
            return Ok(());
        }

        // Parse the batch
        let res = match event_source {
            EventSource::Fills => sonic_rs::from_str::<Batch<NodeDataFill>>(&line).map(|batch| {
                let height = batch.block_number();
                (height, EventBatch::Fills(batch))
            }),
            EventSource::OrderStatuses => sonic_rs::from_str(&line)
                .map(|batch: Batch<NodeDataOrderStatus>| (batch.block_number(), EventBatch::Orders(batch))),
            EventSource::OrderDiffs => sonic_rs::from_str(&line)
                .map(|batch: Batch<NodeDataOrderDiff>| (batch.block_number(), EventBatch::BookDiffs(batch))),
        };

        let (height, event_batch) = match res {
            Ok(data) => data,
            Err(err) => {
                // Log ALL parse errors for debugging
                let err_source_label = match event_source {
                    EventSource::Fills => "fills",
                    EventSource::OrderStatuses => "orders",
                    EventSource::OrderDiffs => "diffs",
                };
                PARSE_ERRORS_TOTAL.with_label_values(&[err_source_label]).inc();
                static PARSE_ERR_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                let err_count = PARSE_ERR_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if err_count % 1000 == 0 {
                    error!("parse error #{}: {}, source: {}, line_len: {}", err_count, err, event_source, line.len());
                }
                return Ok(()); // Skip this line but don't fail
            }
        };

        // Sanity cap on batch size. A malformed/malicious line could otherwise
        // pin hundreds of MB and freeze the listener for seconds.
        let events_len = match &event_batch {
            EventBatch::Orders(b) => b.events_len(),
            EventBatch::BookDiffs(b) => b.events_len(),
            EventBatch::Fills(b) => b.events_len(),
        };
        if events_len > MAX_EVENTS_PER_BATCH {
            let source_label = match event_source {
                EventSource::Fills => "fills",
                EventSource::OrderStatuses => "orders",
                EventSource::OrderDiffs => "diffs",
            };
            PARSE_ERRORS_TOTAL.with_label_values(&[source_label]).inc();
            error!(
                "Dropping oversize batch from {source_label}: {events_len} events (cap {MAX_EVENTS_PER_BATCH}), height={height}"
            );
            return Ok(());
        }

        // Log successful parses periodically
        static PARSE_OK_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let ok_count = PARSE_OK_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Record file watcher metrics
        let source_label = match event_source {
            EventSource::Fills => "fills",
            EventSource::OrderStatuses => "orders",
            EventSource::OrderDiffs => "diffs",
        };
        FILE_EVENTS_TOTAL.with_label_values(&[source_label]).inc();
        FILE_LINES_PARSED_TOTAL.with_label_values(&[source_label]).inc_by(line.len() as u64);
        let process_start = Instant::now();

        if ok_count % 10_000 == 0 {
            info!("parse OK #{}: height={}, source={}", ok_count, height, event_source);
        }

        if height % 100 == 0 {
            info!("{event_source} block: {height}");
        }

        // HFT mode: Process events DIRECTLY without block-level synchronization
        // This is arbor's key insight - process independently with order-level caching
        let changed_coins: HashSet<Coin> = if let Some(state) = self.order_book_state.as_mut() {
            let result = match event_batch {
                EventBatch::Orders(batch) => {
                    // Broadcast L4 order statuses for L4Book subscribers - skip the
                    // batch clone entirely when nothing is subscribed (the per-conn
                    // filter inside handle_socket already short-circuits, but the
                    // clone is what costs us in OOM scenarios).
                    if let Some(tx) = &self.internal_message_tx {
                        if tx.receiver_count() > 0 {
                            let msg = Arc::new(InternalMessage::L4OrderStatuses { batch: batch.clone() });
                            drop(tx.send(msg));
                        }
                    }
                    EVENTS_PROCESSED_TOTAL.with_label_values(&["orders"]).inc();
                    state.apply_order_statuses_hft(batch)
                }
                EventBatch::BookDiffs(batch) => {
                    // Broadcast L4 order diffs for L4Book / BookDiffs subscribers.
                    // Defense-in-depth: when running with `ignore_spot=true`, strip
                    // spot diffs from the broadcast too. Otherwise `bookDiffs` clients
                    // would see events for coins whose state we never applied locally.
                    if let Some(tx) = &self.internal_message_tx {
                        if tx.receiver_count() > 0 {
                            let to_broadcast = if state.ignore_spot() {
                                batch.filter_events(|d| !d.coin().is_spot())
                            } else {
                                batch.clone()
                            };
                            if to_broadcast.events_len() > 0 {
                                let msg = Arc::new(InternalMessage::L4OrderDiffs { batch: to_broadcast });
                                drop(tx.send(msg));
                            }
                        }
                    }
                    EVENTS_PROCESSED_TOTAL.with_label_values(&["diffs"]).inc();
                    state.apply_order_diffs_hft(batch)
                }
                EventBatch::Fills(batch) => {
                    EVENTS_PROCESSED_TOTAL.with_label_values(&["fills"]).inc();
                    // Broadcast fills (no clone needed - we own the batch and don't apply it locally)
                    if let Some(tx) = &self.internal_message_tx {
                        if tx.receiver_count() > 0 {
                            let snapshot = Arc::new(InternalMessage::Fills { batch });
                            drop(tx.send(snapshot));
                        }
                    }
                    Ok(HashSet::new())
                }
            };

            match result {
                Ok(coins) => coins,
                Err(err) => {
                    // Per-event errors (malformed Px/Sz, unrecognized diff variant) are
                    // recoverable: skip the offending batch and keep serving every other
                    // coin's state. Discarding `order_book_state` here used to take down
                    // the entire feed for ~10s on a single malformed line.
                    PARSE_ERRORS_TOTAL.with_label_values(&[source_label]).inc();
                    log::warn!(
                        "Skipping event batch at height={} source={} due to apply error: {err}",
                        height, source_label
                    );
                    HashSet::new()
                }
            }
        } else {
            HashSet::new()
        };
        EVENT_PROCESSING_LATENCY.with_label_values(&[source_label]).observe(process_start.elapsed().as_secs_f64());

        // Log HFT state progress periodically
        static HFT_STATE_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let sc = HFT_STATE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if sc % 1000 == 0 {
            if let Some(state) = &mut self.order_book_state {
                // Record health metrics
                ORDERBOOK_HEIGHT.set(state.height() as i64);
                ORDERBOOK_TIME_MS.set(state.time() as i64);
                PENDING_ORDERS_CACHE.set(state.pending_order_statuses_count() as i64);
                PENDING_DIFFS_CACHE.set(state.pending_new_diffs_count() as i64);

                // Record orderbook stats
                ORDERBOOK_ORDERS_TOTAL.set(state.order_count() as i64);
                ORDERBOOK_COINS_COUNT.set(state.coin_count() as i64);

                // Cleanup stale pending entries to prevent unbounded memory growth
                state.cleanup_stale_pending();

                info!(
                    "State progress #{}: height={}, pending_statuses={}, pending_diffs={}",
                    sc,
                    state.height(),
                    state.pending_order_statuses_count(),
                    state.pending_new_diffs_count()
                );
            }
        }

        // Fast BBO broadcast - ONLY for coins that changed AND only when someone is
        // listening. Without the receiver-count gate we'd `get_bbos_for_coins` and
        // spawn a tokio task per change even with zero subscribers, wasting CPU.
        if !changed_coins.is_empty() {
            if let Some(state) = &self.order_book_state {
                if let Some(tx) = &self.internal_message_tx {
                    if tx.receiver_count() > 0 {
                        let bbo_start = Instant::now();
                        let (time, bbos) = state.get_bbos_for_coins(&changed_coins);
                        static BBO_BROADCAST_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                        let bc = BBO_BROADCAST_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if bc % 1000 == 0 {
                            info!("Fast BBO broadcast #{} at time {} for {} coins", bc, time, changed_coins.len());
                        }
                        // broadcast::Sender::send is non-blocking; the previous
                        // tokio::spawn wrapper added task overhead with no benefit.
                        let msg = Arc::new(InternalMessage::BboUpdate { bbos, time });
                        drop(tx.send(msg));
                        BBO_BROADCAST_LATENCY.observe(bbo_start.elapsed().as_secs_f64());
                    }
                }
            }
        }

        // Throttled L2 snapshot broadcast for L2Book subscribers.
        // l2_snapshots_incremental() walks every changed coin x every aggregation
        // variant, so limit to 20 broadcasts/sec max (50ms).
        // (Heartbeat resend for quiet coins is handled per-connection in handle_socket.)
        //
        // Conflation: every event accumulates its changed coins into a persistent
        // buffer. Because L2 is throttled to one broadcast / 50ms, the buffer holds
        // EVERY coin that changed since the last broadcast - not just the coins in the
        // triggering event. Without this, a coin that changed during a throttle-
        // suppressed window was never marked for rebuild and served a stale cached
        // snapshot until a later event for that exact coin happened to land on an open
        // throttle slot (165-2260ms L2 update gaps with many active coins, while BBO -
        // which reads live state every event - stayed fresh).
        //
        // CRITICAL: the receiver_count gate must wrap the compute, not sit between
        // compute and send. A prior version updated last_l2_broadcast only when
        // receivers existed, so with zero subscribers the throttle reset never fired
        // and the par_iter ran on every event - tens of GB of allocator churn per hour
        // and a pinned listener mutex. We still set last_l2_broadcast unconditionally
        // below. The buffer is only drained inside the has_receivers + Some(state)
        // branch where we actually rebuild: draining anywhere else would clear the
        // coins without refreshing the cache, so a later-connecting subscriber would
        // be served their stale snapshots. With no subscribers the buffer keeps
        // accumulating (deduped by coin, bounded by the universe size).
        self.pending_dirty_l2_coins.extend(changed_coins.iter().cloned());
        // The L2 broadcast is NOT done inline here. It is driven by the main-loop
        // flush ticker via flush_l2_if_due(), so a quiet node between block flushes
        // (or the listener lock being busy draining a burst) can no longer starve
        // the L2 feed. The event path stays minimal: apply + BBO + accumulate.
        Ok(())
    }

    /// Flush the L2 conflation buffer if the throttle window has elapsed and there
    /// are dirty coins. Driven by the main-loop flush ticker so the L2 feed has a
    /// guaranteed maximum interval (~throttle + tick) regardless of event arrival.
    /// Safe to call on every tick: O(1) early-return when not due. Runs under the
    /// listener lock.
    pub(crate) fn flush_l2_if_due(&mut self) {
        let should_broadcast_l2 = !self.pending_dirty_l2_coins.is_empty()
            && self
                .last_l2_broadcast
                .map(|t| t.elapsed() >= Duration::from_millis(L2_BROADCAST_THROTTLE_MS))
                .unwrap_or(true);
        if !should_broadcast_l2 {
            return;
        }

        let has_receivers = self.internal_message_tx.as_ref().is_some_and(|tx| tx.receiver_count() > 0);
        // Mark the throttle as fired regardless of receivers so we don't re-run the
        // par_iter path on every subsequent tick when nobody is listening.
        self.last_l2_broadcast = Some(Instant::now());
        if !has_receivers {
            return;
        }

        // Compute only the variant shapes some connection currently wants. With no
        // L2 subscribers there is nothing to build or send.
        let active = self.active_l2_params.snapshot();
        if active.is_empty() {
            return;
        }
        // When the requested shape set changes, the cache holds the wrong shapes;
        // clear it so every present coin is rebuilt with the new set on this flush
        // (bounds a new subscriber's wait to one throttle window even on quiet coins).
        if active != self.last_active_l2_params {
            self.l2_snapshot_cache.clear();
            self.last_active_l2_params.clone_from(&active);
        }

        if let Some(state) = &self.order_book_state {
            // Drain the conflation buffer only now that we will actually rebuild.
            // mem::take yields an owned set, releasing the borrow on
            // self.pending_dirty_l2_coins before the disjoint co-borrow of
            // order_book_state (&) and l2_snapshot_cache (&mut).
            let dirty = std::mem::take(&mut self.pending_dirty_l2_coins);
            L2_CONFLATION_BATCH_SIZE.observe(dirty.len() as f64);
            let l2_start = Instant::now();
            let (time, l2_snapshots) = state.l2_snapshots_incremental(&dirty, &active, &mut self.l2_snapshot_cache);

            static L2_BROADCAST_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let bc = L2_BROADCAST_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if bc % 100 == 0 {
                info!("L2 broadcast #{} at time {} for {} dirty coins", bc, time, dirty.len());
            }

            if let Some(tx) = &self.internal_message_tx {
                let msg = Arc::new(InternalMessage::Snapshot { l2_snapshots, time });
                drop(tx.send(msg));
            }
            L2_BROADCAST_LATENCY.observe(l2_start.elapsed().as_secs_f64());
        }
    }
}

/// Per-coin L2 snapshots, one inner map per coin holding all aggregation variants.
/// The inner maps are Arc'd so the listener-side cache and the broadcast Arc can
/// share unchanged coins' data without deep-cloning their level vectors.
pub(crate) struct L2Snapshots(HashMap<Coin, Arc<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>>);

impl L2Snapshots {
    pub(crate) const fn as_ref(&self) -> &HashMap<Coin, Arc<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>> {
        &self.0
    }
}

pub(crate) struct TimedSnapshots {
    pub(crate) time: u64,
    pub(crate) height: u64,
    pub(crate) snapshot: Snapshots<InnerL4Order>,
}

// Messages sent from node data listener to websocket dispatch to support streaming
pub(crate) enum InternalMessage {
    Snapshot {
        l2_snapshots: L2Snapshots,
        time: u64,
    },
    Fills {
        batch: Batch<NodeDataFill>,
    },
    /// Fast BBO-only broadcast path - bypasses expensive L2 snapshot computation
    BboUpdate {
        bbos: HashMap<Coin, (Option<(Px, Sz, u32)>, Option<(Px, Sz, u32)>)>,
        time: u64,
    },
    /// HFT L4 streaming - order diffs without waiting for status pairing
    L4OrderDiffs {
        batch: Batch<NodeDataOrderDiff>,
    },
    /// HFT L4 streaming - order statuses without waiting for diff pairing
    L4OrderStatuses {
        batch: Batch<NodeDataOrderStatus>,
    },
}

#[derive(Eq, PartialEq, Hash, Clone, Copy)]
pub(crate) struct L2SnapshotParams {
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
}

/// Refcounted set of L2 aggregation variant *shapes* that some live connection
/// currently wants. The listener reads it at flush time and computes only those
/// variants (instead of all 7) for every dirty coin. Bounded to the handful of
/// supported shapes. Shared (Arc) between the listener and every connection.
///
/// A plain `std::sync::Mutex` is used deliberately: every access is O(1) and never
/// spans an `.await`, and the RAII guard's `Drop` (which must run on abnormal
/// disconnects too) cannot be async.
#[derive(Clone, Default)]
pub(crate) struct ActiveL2Params {
    inner: Arc<std::sync::Mutex<HashMap<L2SnapshotParams, usize>>>,
}

impl ActiveL2Params {
    pub(crate) fn new() -> Self {
        Self { inner: Arc::new(std::sync::Mutex::new(HashMap::new())) }
    }

    /// Increment the refcount for `params` and return a guard that decrements on
    /// drop. Holding the guard keeps the shape in the active set.
    #[must_use]
    pub(crate) fn acquire(&self, params: L2SnapshotParams) -> L2ParamGuard {
        if let Ok(mut map) = self.inner.lock() {
            *map.entry(params).or_insert(0) += 1;
        }
        L2ParamGuard { registry: self.inner.clone(), params }
    }

    /// Snapshot the currently-active variant shapes. Cloned under the lock; the
    /// lock is released before returning.
    pub(crate) fn snapshot(&self) -> HashSet<L2SnapshotParams> {
        self.inner.lock().map(|m| m.keys().copied().collect()).unwrap_or_default()
    }
}

/// RAII guard returned by [`ActiveL2Params::acquire`]. Decrements the refcount on
/// drop and removes the shape at zero. Cleanup is panic/disconnect-safe because
/// `Drop` runs during normal scope exit, early `return`, and unwinding alike.
pub(crate) struct L2ParamGuard {
    registry: Arc<std::sync::Mutex<HashMap<L2SnapshotParams, usize>>>,
    params: L2SnapshotParams,
}

impl Drop for L2ParamGuard {
    fn drop(&mut self) {
        if let Ok(mut map) = self.registry.lock() {
            if let Some(count) = map.get_mut(&self.params) {
                *count -= 1;
                if *count == 0 {
                    map.remove(&self.params);
                }
            }
        }
    }
}

// ============================================================================
// HFT-OPTIMIZED VERSION
// Uses parallel file watchers and immediate OrderDiff processing
// ============================================================================

/// HFT-optimized listener using parallel file watchers
/// Key differences from hl_listen:
/// 1. 3 dedicated threads for file watching (parallel I/O)
/// 2. Processes OrderDiffs immediately (doesn't wait for OrderStatuses)
/// 3. Uses process time instead of block time for lowest latency
pub(crate) async fn hl_listen_hft(listener: Arc<Mutex<OrderBookListener>>, config: crate::ServerConfig) -> Result<()> {
    let dir = match config.data_dir.clone() {
        Some(d) => d,
        None => dirs::home_dir().ok_or(
            "Could not resolve a data directory: pass --data-dir explicitly. The default \
             requires a usable HOME environment variable, which was not set or is invalid.",
        )?,
    };

    info!("Starting HFT-optimized listener");
    info!("Data directory: {:?}", dir);

    // Create SnapshotConfig from ServerConfig
    let snapshot_config = SnapshotConfig {
        mode: config.snapshot_mode,
        docker_container: config.docker_container.clone(),
        hlnode_binary: config.hlnode_binary.clone(),
        abci_state_path: config.abci_state_path.clone(),
        snapshot_output_path: config.snapshot_output_path.clone(),
        visor_state_path: config.visor_state_path.clone(),
        data_dir: dir.clone(),
    };

    let ignore_spot = {
        let listener = listener.lock().await;
        listener.ignore_spot
    };

    // Start parallel file watchers (crossbeam channel)
    let (crossbeam_rx, _handles, _last_os, _last_fills, _last_diffs) = parallel::start_parallel_file_watchers(dir);

    // Bridge crossbeam to tokio mpsc.
    // BOUNDED channel: under processing stalls (mutex contention, slow L2 compute),
    // an unbounded queue accumulates multi-KB JSON strings indefinitely - a primary
    // OOM vector. A bounded channel applies backpressure into the bridge thread,
    // which in turn lets the crossbeam buffer absorb the burst.
    let (tokio_tx, mut tokio_rx) = tokio::sync::mpsc::channel::<parallel::FileEvent>(10_000);

    // Spawn a blocking task to bridge crossbeam -> tokio
    tokio::task::spawn_blocking(move || {
        info!("Bridge task started");
        let mut event_count = 0u64;
        loop {
            match crossbeam_rx.recv() {
                Ok(event) => {
                    event_count += 1;
                    if event_count % 100_000 == 0 {
                        info!("Bridge: received {} events", event_count);
                    }
                    if tokio_tx.blocking_send(event).is_err() {
                        error!("Bridge: tokio channel closed");
                        break;
                    }
                }
                Err(_) => {
                    error!("Bridge: crossbeam channel closed");
                    break;
                }
            }
        }
    });

    // Snapshot fetch channel
    let (snapshot_fetch_task_tx, mut snapshot_fetch_task_rx) = unbounded_channel::<Result<()>>();

    let start = Instant::now() + Duration::from_secs(5);
    let mut ticker = tokio::time::interval_at(start, Duration::from_secs(10));
    let mut snapshot_fetch_pending = false;

    // Drives L2 broadcasts on a fixed cadence so the feed has a guaranteed maximum
    // interval even when no events arrive. Skip missed ticks so a busy loop resumes
    // on the next aligned tick rather than firing a catch-up burst.
    let mut l2_flush_ticker = interval(Duration::from_millis(L2_FLUSH_TICK_MS));
    l2_flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    info!("Main event loop starting");

    loop {
        tokio::select! {
            biased;

            // L2 flush FIRST under `biased`: guarantees the throttle window is
            // serviced even under continuous event load. During a burst tokio_rx is
            // perpetually ready, so a later-placed flush arm would be starved and the
            // multi-second gaps would return. flush_l2_if_due() is O(1) when not due
            // and the tick is only Ready every L2_FLUSH_TICK_MS, so it cannot starve
            // the event arm in return.
            _ = l2_flush_ticker.tick() => {
                listener.lock().await.flush_l2_if_due();
            }

            // Process events from file watchers (via bridge)
            Some(event) = tokio_rx.recv() => {
                match event {
                    parallel::FileEvent::OrderDiff(line) => {
                        // Process OrderDiff immediately - this is the BBO-critical path
                        if let Err(err) = listener.lock().await.process_data_hft(line, EventSource::OrderDiffs) {
                            error!("OrderDiff error: {err}");
                        }
                    }
                    parallel::FileEvent::OrderStatus(line) => {
                        // OrderStatuses are less latency-critical
                        if let Err(err) = listener.lock().await.process_data_hft(line, EventSource::OrderStatuses) {
                            error!("OrderStatus error: {err}");
                        }
                    }
                    parallel::FileEvent::Fill(line) => {
                        // Fills are for trade data, not BBO
                        if let Err(err) = listener.lock().await.process_data_hft(line, EventSource::Fills) {
                            error!("Fill error: {err}");
                        }
                    }
                }
            }

            // Snapshot fetch result
            snapshot_fetch_res = snapshot_fetch_task_rx.recv() => {
                snapshot_fetch_pending = false;
                match snapshot_fetch_res {
                    None => {
                        return Err("Snapshot fetch task sender dropped".into());
                    }
                    Some(Err(err)) => {
                        return Err(format!("Abci state reading error: {err}").into());
                    }
                    Some(Ok(())) => {}
                }
            }

            // Periodic snapshot fetch (initial only)
            _ = ticker.tick() => {
                let is_ready = listener.lock().await.is_ready();
                info!("Ticker: is_ready={}, snapshot_fetch_pending={}", is_ready, snapshot_fetch_pending);
                if !is_ready && !snapshot_fetch_pending {
                    snapshot_fetch_pending = true;
                    let listener = listener.clone();
                    let snapshot_fetch_task_tx = snapshot_fetch_task_tx.clone();
                    fetch_snapshot(snapshot_config.clone(), listener, snapshot_fetch_task_tx, ignore_spot);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Build a ready listener (state initialized from an empty snapshot) with a held
    /// broadcast receiver so `receiver_count() > 0`.
    fn ready_listener() -> (OrderBookListener, tokio::sync::broadcast::Receiver<Arc<InternalMessage>>) {
        let (tx, rx) = tokio::sync::broadcast::channel(32);
        let mut listener = OrderBookListener::new(Some(tx), true, ActiveL2Params::new());
        listener.init_from_snapshot(Snapshots::new(HashMap::new()), 0);
        (listener, rx)
    }

    #[test]
    fn test_flush_l2_if_due_broadcasts_and_drains_when_due() {
        let (mut listener, mut rx) = ready_listener();
        // A subscriber must want at least one variant shape, else flush is skipped.
        let _guard = listener.active_l2_params().acquire(L2SnapshotParams::new(None, None));
        listener.pending_dirty_l2_coins.insert(Coin::new("BTC"));
        listener.last_l2_broadcast = None; // due (no prior broadcast)

        listener.flush_l2_if_due();

        let msg = rx.try_recv().expect("a Snapshot must be broadcast when due and dirty");
        assert!(matches!(msg.as_ref(), InternalMessage::Snapshot { .. }));
        assert!(listener.pending_dirty_l2_coins.is_empty(), "buffer is drained on flush");
        assert!(listener.last_l2_broadcast.is_some(), "throttle timestamp is set");
    }

    #[test]
    fn test_flush_l2_if_due_noop_inside_throttle_window() {
        let (mut listener, mut rx) = ready_listener();
        listener.pending_dirty_l2_coins.insert(Coin::new("BTC"));
        listener.last_l2_broadcast = Some(Instant::now()); // just fired -> not due

        listener.flush_l2_if_due();

        assert!(rx.try_recv().is_err(), "nothing is broadcast inside the throttle window");
        assert!(!listener.pending_dirty_l2_coins.is_empty(), "buffer is retained when not due");
    }

    #[test]
    fn test_flush_l2_if_due_noop_when_buffer_empty() {
        let (mut listener, mut rx) = ready_listener();
        listener.last_l2_broadcast = None; // due, but nothing dirty

        listener.flush_l2_if_due();

        assert!(rx.try_recv().is_err(), "no broadcast when there are no dirty coins");
    }

    #[test]
    fn test_flush_l2_if_due_noop_when_no_active_variants() {
        // Due + dirty + receiver, but no connection wants any L2 variant.
        let (mut listener, mut rx) = ready_listener();
        listener.pending_dirty_l2_coins.insert(Coin::new("BTC"));
        listener.last_l2_broadcast = None;

        listener.flush_l2_if_due();

        assert!(rx.try_recv().is_err(), "no broadcast when no variant shape is subscribed");
    }

    #[test]
    fn test_active_l2_params_guard_decrements_on_drop() {
        let reg = ActiveL2Params::new();
        let p = L2SnapshotParams::new(Some(5), Some(2));
        {
            let _g1 = reg.acquire(p);
            let _g2 = reg.acquire(p); // refcount 2
            assert!(reg.snapshot().contains(&p));
        } // both guards dropped here
        assert!(reg.snapshot().is_empty(), "shape removed once refcount hits zero");
    }

    #[test]
    fn test_active_l2_params_partial_drop_keeps_shape() {
        let reg = ActiveL2Params::new();
        let p = L2SnapshotParams::new(None, None);
        let g1 = reg.acquire(p);
        {
            let _g2 = reg.acquire(p);
        } // one guard dropped, refcount back to 1
        assert!(reg.snapshot().contains(&p), "shape still referenced by g1");
        drop(g1);
        assert!(reg.snapshot().is_empty());
    }
}
