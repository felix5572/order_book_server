use crate::{
    listeners::order_book::L2Snapshots,
    order_book::{
        Coin, InnerOrder, Oid, Px, PxBand, Snapshot,
        multi_book::{OrderBooks, Snapshots},
    },
    prelude::*,
    types::{
        inner::{InnerL4Order, InnerOrderDiff},
        node_data::{Batch, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

/// Applied (block height, block time ms) of a book stream. `>=` so the time
/// still advances on a later line of the same block (a block spans many lines).
#[derive(Debug, Clone, Copy)]
struct StreamProgress {
    height: u64,
    time: u64,
}

impl StreamProgress {
    const fn advance(&mut self, height: u64, time: u64) {
        if height >= self.height {
            self.height = height;
            self.time = time;
        }
    }
}

/// A resting-type status waiting for its New diff (the two share one block).
struct PendingStatus {
    status: NodeDataOrderStatus,
    block: u64,
    // A later status for the oid came in the same block. An order that fills on
    // entry gets "open" then "filled" there and never a New diff (every unpaired
    // open in a 175-block mainnet sample, 2026-10-10), so its eviction is
    // expected, not an orphan. A New diff that does come still pairs with it
    // (rested, then filled in the same block).
    settled_in_block: bool,
    // Arrived below the status stream's high-water mark: a node restart
    // rewriting blocks already applied (within one run the stream never goes
    // back, 128M lines checked 2026-10-10). Its New diff may already have been
    // evicted as lost, which marks the desync, so its eviction is not counted
    // as an orphan (one replay counted ~4M, 2026-10-10).
    replayed: bool,
}

pub(super) struct OrderBookState {
    order_book: OrderBooks<InnerL4Order>,
    // Furthest block applied by either book stream: the loss bound and the
    // height/time metrics. L4 snapshots keep stamping it (their (time, height)
    // is the splice point for the L4 diff stream).
    latest: StreamProgress,
    // Applied progress of each book stream. The two are read by separate
    // watchers and drift apart by minutes during a node catch-up (2026-10-10),
    // so pairing eviction and the l2/bbo frame time follow them, never the
    // wall clock.
    statuses: StreamProgress,
    diffs: StreamProgress,
    ignore_spot: bool,
    // Persistent cache of OrderStatuses waiting for their New diffs
    // Allows OrderStatus and OrderDiff to arrive in any order (HFT-compatible).
    // Entries carry their block: an order's status and New diff share one block
    // (every resting order in two mainnet windows, ~1M each, 2026-10-10), so a
    // status can still pair until the diff stream applies a later block.
    pending_order_statuses: rustc_hash::FxHashMap<Oid, PendingStatus>,
    // Persistent cache of New diffs (sz + resting px + optional insertBefore anchor) waiting
    // for their OrderStatuses. This is the other half of bidirectional caching - handles when
    // Diff arrives BEFORE Status. px comes from the diff (official PR#9): the diff's px is the
    // true resting price - trigger/converted orders can carry a different px on the status.
    // The anchor must survive the cache so a late-pairing priority ALO order still splices
    // into the right queue position. The trailing u64 is the diff's block (see above).
    pending_new_diffs: rustc_hash::FxHashMap<Oid, (crate::order_book::types::Sz, Px, Option<Oid>, u64)>,
    // Orders an Update to size 0 already removed from the book (`modify_sz` drops a
    // zero-size order). For a fully filled order the node sends Update(newSz=0) and
    // then Remove (every zero update in a 28s mainnet sample, 2026-10-09); this
    // pairs that Remove so only genuinely unknown targets count as missing.
    zeroed_awaiting_remove: rustc_hash::FxHashMap<Oid, Instant>,
    // Update/Remove diffs whose target was neither booked, nor pending, nor (for a
    // Remove) just zeroed - mirrors orderbook_diff_target_missing_total for this
    // state, so tests can assert it without the process-global registry.
    missing_diff_targets: u64,
    // Statuses evicted unpaired and not settled in their block - mirrors
    // orderbook_pending_orphans_evicted_total for this state, like
    // missing_diff_targets. Expected to stay 0: a rise means New diffs were lost.
    orphan_statuses: u64,
    // New diffs evicted as lost since this book was installed. Logged with the
    // total at most every 10s: a node restart replays blocks below the stream
    // high-water mark and every cleanup pass evicts (~11k lines in 2 min,
    // 2026-10-10).
    lost_new_diffs: u64,
    last_loss_log: Option<tokio::time::Instant>, // the listener's throttle clock
    // insertBefore anchors that were missing from the book (add fell back to the back of
    // the level). Drained per batch by the listener, which converts a nonzero count into
    // a desync mark + Prometheus counter. Sticky across snapshot replay on purpose.
    insert_before_fallbacks: u64,
    // Untriggered trigger orders (stop / TP-SL waiting for triggerPx to be crossed),
    // keyed per coin so single-coin queries and bogus-coin probes never scan the
    // whole set. These never rest on the book: the hl-node snapshot appends them to
    // both sides (extracted at install), and live "open" statuses with is_trigger
    // carry them. Removed on any non-"open" status for the oid (canceled /
    // triggered / filled / rejected / ...). Orders are Arc'd so the endpoint's
    // under-lock snapshot is a refcount bump per order, not a deep String clone.
    // Replaced wholesale on every re-sync along with the book, so a missed
    // terminal status self-heals at the next snapshot install.
    untriggered_orders: rustc_hash::FxHashMap<Coin, rustc_hash::FxHashMap<Oid, std::sync::Arc<InnerL4Order>>>,
    // False in --bbo-only mode: the map stays empty there so the documented
    // lightweight memory envelope holds (the endpoint has no consumers in a
    // BBO-only deployment anyway).
    track_untriggered: bool,
}

impl OrderBookState {
    pub(super) fn from_snapshot(
        mut snapshot: Snapshots<InnerL4Order>,
        untriggered: Vec<InnerL4Order>,
        height: u64,
        time: u64,
        ignore_triggers: bool,
        ignore_spot: bool,
        track_untriggered: bool,
    ) -> Self {
        // Seed the side table from the dump's untriggered_orders section
        // (--include-trigger-orders) - the full standing stop book, one entry
        // per order. The legacy tail extraction below covers old-format dumps
        // where triggers were appended to both sides of the book instead.
        let mut untriggered_orders: rustc_hash::FxHashMap<
            Coin,
            rustc_hash::FxHashMap<Oid, std::sync::Arc<InnerL4Order>>,
        > = rustc_hash::FxHashMap::default();
        if track_untriggered {
            for order in untriggered {
                if ignore_spot && order.coin.is_spot() {
                    continue;
                }
                let oid = order.oid();
                untriggered_orders.entry(order.coin.clone()).or_default().insert(oid, std::sync::Arc::new(order));
            }
        }
        if ignore_triggers && track_untriggered {
            for order in snapshot.extract_triggers() {
                if ignore_spot && order.coin.is_spot() {
                    continue;
                }
                let oid = order.oid();
                let arc = std::sync::Arc::new(order);
                if let Some(prev) =
                    untriggered_orders.entry(arc.coin.clone()).or_default().insert(oid.clone(), arc.clone())
                    && *prev != *arc
                {
                    // The node emits one copy per side; they are expected to be
                    // identical. A mismatch means the dedupe silently picked one
                    // of two different views - surface it.
                    log::warn!("Trigger order snapshot copies differ across sides: oid={oid:?}");
                }
            }
        }
        let installed = StreamProgress { height, time };
        Self {
            ignore_spot,
            latest: installed,
            statuses: installed,
            diffs: installed,
            order_book: OrderBooks::from_snapshots(snapshot, ignore_triggers),
            pending_order_statuses: rustc_hash::FxHashMap::default(),
            pending_new_diffs: rustc_hash::FxHashMap::default(),
            zeroed_awaiting_remove: rustc_hash::FxHashMap::default(),
            missing_diff_targets: 0,
            orphan_statuses: 0,
            lost_new_diffs: 0,
            last_loss_log: None,
            insert_before_fallbacks: 0,
            untriggered_orders,
            track_untriggered,
        }
    }

    /// Drain the count of adds whose insertBefore anchor was missing (the order
    /// was rested at the back of its level instead). Nonzero means the book has
    /// diverged from the stream and should be re-synced.
    pub(super) fn take_insert_before_fallbacks(&mut self) -> u64 {
        std::mem::take(&mut self.insert_before_fallbacks)
    }

    /// Record that `insert_before` could not be honored for this order. The
    /// anchor oid and price level are logged so a fallback storm can be traced
    /// to its hole set (which orders are missing, at which levels) instead of
    /// only naming the order that fell back.
    fn note_insert_before_fallback(&mut self, oid: &Oid, coin: &Coin, anchor: Option<&Oid>, px: Px) {
        self.insert_before_fallbacks += 1;
        log::warn!(
            "insertBefore anchor {anchor:?} missing at px={} for oid={oid:?} coin={coin:?}; rested at back of level",
            px.to_str()
        );
    }

    pub(super) const fn height(&self) -> u64 {
        self.latest.height
    }

    pub(super) const fn ignore_spot(&self) -> bool {
        self.ignore_spot
    }

    pub(super) const fn time(&self) -> u64 {
        self.latest.time
    }

    pub(super) const fn status_height(&self) -> u64 {
        self.statuses.height
    }

    pub(super) const fn diff_height(&self) -> u64 {
        self.diffs.height
    }

    /// Block time both book streams have applied: the l2/bbo frame stamp. A
    /// progress mark, not a per-block consistent cut - a leading diff stream's
    /// Update/Remove are already on the book; only pairing waits for the
    /// slower stream. During a catch-up it no longer claims the faster stream's time.
    const fn book_time(&self) -> u64 {
        if self.statuses.time < self.diffs.time { self.statuses.time } else { self.diffs.time }
    }

    /// L4 snapshot of a single coin - (time, height, snapshot). Returns None when
    /// the coin has no book. Cheap enough to run under the listener lock, unlike
    /// the old all-coins snapshot.
    pub(super) fn compute_snapshot_for_coin(
        &self,
        coin: &Coin,
        band: PxBand,
    ) -> Option<(u64, u64, Snapshot<InnerL4Order>)> {
        self.order_book.snapshot_for_coin(coin, band).map(|snapshot| (self.latest.time, self.latest.height, snapshot))
    }

    /// Incremental variant: rebuilds variants only for `changed_coins` and reuses
    /// cached Arc'd entries for every other coin. The caller owns the cache so
    /// the borrow on `&self` here only touches the order book. Returns
    /// (time, snapshots, recomputed coins, whether the coin set changed).
    pub(super) fn l2_snapshots_incremental(
        &self,
        changed_coins: &HashSet<Coin>,
        active: &HashSet<crate::listeners::order_book::L2SnapshotParams>,
        cache: &mut HashMap<Coin, std::sync::Arc<HashMap<crate::listeners::order_book::L2SnapshotParams, Snapshot<crate::types::inner::InnerLevel>>>>,
    ) -> (u64, L2Snapshots, HashSet<Coin>, bool) {
        let (snapshots, recomputed, coin_set_changed) =
            crate::listeners::order_book::utils::compute_l2_snapshots_incremental(
                &self.order_book,
                changed_coins,
                active,
                cache,
            );
        (self.book_time(), snapshots, recomputed, coin_set_changed)
    }

    pub(super) fn compute_universe(&self) -> HashSet<Coin> {
        self.order_book.as_ref().keys().cloned().collect()
    }

    /// Count of OrderStatuses waiting for their OrderDiff::New to arrive
    pub(super) fn pending_order_statuses_count(&self) -> usize {
        self.pending_order_statuses.len()
    }

    /// Count of OrderDiff::New sizes waiting for their OrderStatus to arrive  
    pub(super) fn pending_new_diffs_count(&self) -> usize {
        self.pending_new_diffs.len()
    }

    /// Total number of orders currently in the orderbook
    pub(super) fn order_count(&self) -> usize {
        self.order_book.order_count()
    }

    /// Count of untriggered trigger orders in the side table
    pub(super) fn untriggered_count(&self) -> usize {
        self.untriggered_orders.values().map(rustc_hash::FxHashMap::len).sum()
    }

    /// Carry the previous state's untriggered table into this freshly-built one
    /// when the snapshot contributed nothing. The hl-node L4 dump on current
    /// node versions contains no trigger orders, so a rebuild from it alone
    /// would wipe everything accumulated from the live status stream on every
    /// re-sync. When the dump DOES yield triggers, it stays authoritative and
    /// the previous table is dropped (original wholesale-rebuild semantics).
    /// Call BEFORE replaying cached events so evictions recorded during the
    /// fetch window still apply to the carried entries. Cost: Arc bumps only.
    pub(super) fn carry_forward_untriggered(&mut self, prev: &Self) {
        if self.track_untriggered && self.untriggered_orders.is_empty() && !prev.untriggered_orders.is_empty() {
            self.untriggered_orders = prev.untriggered_orders.clone();
            log::info!(
                "Snapshot contained no untriggered trigger orders; carried {} forward from previous state",
                self.untriggered_count()
            );
        }
    }

    /// Untriggered trigger orders - all coins, or one coin's when `coin` is
    /// given - along with (time, height). Runs under the listener lock, but
    /// only bumps an Arc refcount per order (no deep clone); an unknown coin
    /// is an O(1) map miss. Callers convert/serialize off-lock (same
    /// discipline as l4Book).
    pub(super) fn untriggered_snapshot(&self, coin: Option<&Coin>) -> (u64, u64, Vec<std::sync::Arc<InnerL4Order>>) {
        let orders = match coin {
            Some(c) => self.untriggered_orders.get(c).map(|m| m.values().cloned().collect()).unwrap_or_default(),
            None => self.untriggered_orders.values().flat_map(|m| m.values().cloned()).collect(),
        };
        (self.latest.time, self.latest.height, orders)
    }

    /// Number of coins tracked in the orderbook
    pub(super) fn coin_count(&self) -> usize {
        self.order_book.as_ref().len()
    }

    /// Cleanup stale pending entries to prevent unbounded memory growth.
    ///
    /// A pending half is dropped only once it provably can no longer pair: an
    /// order's status and New diff share one block, so a half from block h is
    /// done waiting when the OTHER stream has applied a block > h (strictly: a
    /// block spans many lines, so `== h` may still deliver it). How long it
    /// waited is irrelevant - during a node catch-up the two watchers drift
    /// apart by minutes (2026-10-10: the old 60s wall-clock age dropped live
    /// halves, and those orders never reached the book).
    ///
    /// Loss semantics differ per cache:
    /// - A status passed by the diff stream is an expected orphan (an order
    ///   that never rested, e.g. a `FrontendMarket` "open") - dropped silently
    ///   and counted, NOT data loss.
    /// - A New diff passed by the status stream lost its status: the book is
    ///   missing that order, which IS data loss.
    ///
    /// The size caps remain an OOM backstop only; hitting one still force-clears
    /// (fresh `HashMap::new()` so the high-water-mark bucket capacity is
    /// actually released) and counts as data loss.
    /// Also evicts books left empty (O(coins)). Slab compaction no longer runs
    /// here: a sweep over every level of every coin on this cadence stalled the
    /// listener for milliseconds (see `PriceLevel::remove`).
    ///
    /// Returns `true` when potentially-live data was evicted; the caller must
    /// treat this as data loss and mark the book for re-sync.
    pub(super) fn cleanup_stale_pending(&mut self) -> bool {
        // ~3.5k resting orders/s on mainnet; one stream stalled for the longest
        // catch-up seen (231s) is ~0.8M. Status entries are ~0.5-0.7KB each.
        const PENDING_CAP: usize = 1_000_000;
        let cleared = self.evict_pending(PENDING_CAP);

        self.order_book.evict_empty_books();
        cleared
    }

    fn evict_pending(&mut self, cap: usize) -> bool {
        const ZEROED_MAX_AGE: Duration = Duration::from_secs(60);
        let mut cleared = false;

        // Both streams must have passed the block: the diff stream so no New
        // diff can still pair, the status stream so a settling status from a
        // later line of the same block (open -> filled) has been seen.
        let diffs_applied = self.diffs.height;
        let both_applied = diffs_applied.min(self.statuses.height);
        let mut orphans = 0;
        self.pending_order_statuses.retain(|_, pending| {
            let keep = pending.block >= both_applied;
            if !keep && !pending.settled_in_block && !pending.replayed {
                orphans += 1;
            }
            keep
        });
        if orphans > 0 {
            self.orphan_statuses += orphans;
            crate::metrics::PENDING_ORPHANS_EVICTED_TOTAL.inc_by(orphans);
            log::debug!("Evicted {orphans} orphan pending_order_statuses (both streams passed their block)");
        }

        // A zero-size Update whose Remove never followed: nothing to repair (the
        // order is already off the book), just bound the map. Single-stream and
        // metric-only, so its wall-clock age stays.
        self.zeroed_awaiting_remove.retain(|_, at| at.elapsed() < ZEROED_MAX_AGE);

        let statuses_applied = self.statuses.height;
        let before = self.pending_new_diffs.len();
        self.pending_new_diffs.retain(|_, (_, _, _, block)| *block >= statuses_applied);
        let lost = before - self.pending_new_diffs.len();
        if lost > 0 {
            self.lost_new_diffs += lost as u64;
            if super::throttled_log_due(&mut self.last_loss_log, tokio::time::Instant::now()) {
                log::warn!(
                    "Evicted {lost} pending_new_diffs: the status stream passed their block (status height \
                     {statuses_applied}) without their status - data loss; {} since install (lines at most \
                     every 10s)",
                    self.lost_new_diffs
                );
            }
            cleared = true;
        }

        if self.pending_order_statuses.len() > cap {
            log::warn!(
                "Clearing pending_order_statuses at the {cap} cap: {} entries (status height {statuses_applied}, \
                 diff height {diffs_applied})",
                self.pending_order_statuses.len()
            );
            self.pending_order_statuses = rustc_hash::FxHashMap::default();
            cleared = true;
        }

        if self.pending_new_diffs.len() > cap {
            log::warn!(
                "Clearing pending_new_diffs at the {cap} cap: {} entries (status height {statuses_applied}, \
                 diff height {diffs_applied})",
                self.pending_new_diffs.len()
            );
            self.pending_new_diffs = rustc_hash::FxHashMap::default();
            cleared = true;
        }
        cleared
    }

    /// Get BBO for specific coins only - even faster for selective broadcast
    /// Only computes BBO for coins that changed, avoiding iteration over all 150+ coins
    pub(super) fn get_bbos_for_coins(
        &self,
        coins: &HashSet<Coin>,
    ) -> (
        u64,
        HashMap<
            Coin,
            (
                Option<(Px, crate::order_book::Sz, u32)>,
                Option<(Px, crate::order_book::Sz, u32)>,
            ),
        >,
    ) {
        let bbos = self.order_book.get_bbos_for_coins(coins);
        (self.book_time(), bbos)
    }

    /// HFT-specific: Process OrderStatuses independently without block synchronization
    /// Uses bidirectional caching - if diff already arrived, add order immediately
    /// Returns the set of coins that were modified (for selective BBO broadcast)
    pub(super) fn apply_order_statuses_hft(&mut self, batch: Batch<NodeDataOrderStatus>) -> Result<HashSet<Coin>> {
        let height = batch.block_number();
        let time = batch.block_time();
        let mut changed_coins = HashSet::new();

        let replayed = height < self.statuses.height;
        self.statuses.advance(height, time);
        self.latest.advance(height, time);

        for order_status in batch.events() {
            let oid = Oid::new(order_status.order.oid);

            // Maintain the untriggered-orders side table. "open" + is_trigger is a
            // pending trigger order (never rests on the book); any other status is
            // terminal for the untriggered phase (canceled / triggered / filled /
            // rejected / ...) and evicts the oid. The eviction probe is two hash
            // lookups (coin via Borrow<str>, then oid) with no allocation - cheap
            // enough for the hot path.
            if !self.track_untriggered {
                // gated off (--bbo-only): the map stays empty
            } else if order_status.status == "open" {
                if order_status.order.is_trigger && !(self.ignore_spot && Coin::str_is_spot(&order_status.order.coin)) {
                    match InnerL4Order::try_from((order_status.user, order_status.order.clone())) {
                        Ok(inner) => {
                            self.untriggered_orders
                                .entry(inner.coin.clone())
                                .or_default()
                                .insert(oid.clone(), std::sync::Arc::new(inner));
                        }
                        Err(err) => {
                            // The endpoint under-reports this oid until the next
                            // re-sync; count it so the gap is visible in metrics,
                            // not just a log line.
                            crate::metrics::PARSE_ERRORS_TOTAL.with_label_values(&["untriggered"]).inc();
                            log::warn!("Skipping unparseable untriggered trigger order oid={oid:?}: {err}");
                        }
                    }
                }
            } else if let Some(coin_orders) = self.untriggered_orders.get_mut(order_status.order.coin.as_str())
                && coin_orders.remove(&oid).is_some()
            {
                // Labeled by status so a future non-terminal status string that
                // starts wrongly evicting live triggers shows up in Prometheus
                // immediately (today's vocabulary is all terminal-for-the-oid).
                crate::metrics::UNTRIGGERED_EVICTIONS_TOTAL.with_label_values(&[&order_status.status]).inc();
                if coin_orders.is_empty() {
                    // Drop the per-coin map once empty so delisted coins don't
                    // accumulate empty buckets forever.
                    self.untriggered_orders.remove(order_status.order.coin.as_str());
                }
            }

            // Check if there's a pending New diff for this order
            if let Some((sz, px, insert_before, _)) = self.pending_new_diffs.remove(&oid) {
                // Both arrived - add order immediately!
                let time = order_status.time.and_utc().timestamp_millis();
                let order_coin = Coin::new(&order_status.order.coin);
                let mut inner_order: InnerL4Order = order_status.try_into()?;
                inner_order.modify_sz(sz);
                // Official PR#9: resting px comes from the diff, not the status.
                inner_order.modify_px(px);
                inner_order.convert_trigger(time.max(0) as u64);
                let px = inner_order.limit_px();
                let anchor = insert_before.clone();
                if self.order_book.add_order_before(inner_order, insert_before) {
                    self.note_insert_before_fallback(&oid, &order_coin, anchor.as_ref(), px);
                } else if anchor.is_some() {
                    crate::metrics::INSERT_BEFORE_HONORED_TOTAL.inc();
                }
                changed_coins.insert(order_coin.clone());
                log::debug!("Order added (status arrived after diff): oid={:?} coin={:?}", oid, order_coin);
            } else if order_status.is_inserted_into_book() {
                // Diff hasn't arrived yet - cache the OrderStatus
                let pending = PendingStatus { status: order_status, block: height, settled_in_block: false, replayed };
                self.pending_order_statuses.insert(oid, pending);
            } else if let Some(pending) = self.pending_order_statuses.get_mut(&oid)
                && pending.block == height
            {
                pending.settled_in_block = true;
            }
        }
        Ok(changed_coins)
    }

    #[cfg(test)]
    pub(crate) fn pending_order_statuses_has(&self, oid: &Oid) -> bool {
        self.pending_order_statuses.contains_key(oid)
    }

    /// Backdate the zero-update records (the only wall-clock-aged map), so
    /// tests can exercise their eviction without sleeping.
    #[cfg(test)]
    pub(crate) fn age_zeroed_entries(&mut self, by: Duration) {
        let backdated = Instant::now().checked_sub(by).unwrap_or_else(Instant::now);
        for at in self.zeroed_awaiting_remove.values_mut() {
            *at = backdated;
        }
    }

    #[cfg(test)]
    pub(crate) fn pending_new_diffs_has(&self, oid: &Oid) -> bool {
        self.pending_new_diffs.contains_key(oid)
    }

    /// HFT-specific: Process OrderDiffs independently without block synchronization
    /// Uses bidirectional caching - if status already arrived, add order immediately
    /// Returns the set of coins that were modified (for selective BBO broadcast)
    pub(super) fn apply_order_diffs_hft(&mut self, batch: Batch<NodeDataOrderDiff>) -> Result<HashSet<Coin>> {
        let height = batch.block_number();
        let time = batch.block_time();
        let mut changed_coins = HashSet::new();

        self.diffs.advance(height, time);
        self.latest.advance(height, time);

        for diff in batch.events() {
            let oid = diff.oid();
            let coin = diff.coin();
            if coin.is_spot() && self.ignore_spot {
                continue;
            }
            let inner_diff = diff.diff().try_into()?;
            match inner_diff {
                InnerOrderDiff::New { sz, insert_before } => {
                    // Official PR#9: the diff's px is the true resting price. Parse failure =
                    // schema drift -> fail fast (silently falling back to the status px would
                    // re-introduce wrong resting prices).
                    let diff_px = Px::parse_from_str(diff.px())?;
                    // Check if OrderStatus already arrived
                    if let Some(PendingStatus { status: order, .. }) = self.pending_order_statuses.remove(&oid) {
                        // Both arrived - add order immediately!
                        let time = order.time.and_utc().timestamp_millis();
                        let order_coin = Coin::new(&order.order.coin);
                        let mut inner_order: InnerL4Order = order.try_into()?;
                        inner_order.modify_sz(sz);
                        inner_order.modify_px(diff_px);
                        #[allow(clippy::unwrap_used)]
                        inner_order.convert_trigger(time.try_into().unwrap());
                        let px = inner_order.limit_px();
                        let anchor = insert_before.clone();
                        if self.order_book.add_order_before(inner_order, insert_before) {
                            self.note_insert_before_fallback(&oid, &order_coin, anchor.as_ref(), px);
                        } else if anchor.is_some() {
                            crate::metrics::INSERT_BEFORE_HONORED_TOTAL.inc();
                        }
                        changed_coins.insert(order_coin.clone());
                        log::debug!("Order added (diff arrived after status): oid={:?} coin={:?}", oid, order_coin);
                    } else if diff.special_address() {
                        // Official PR#10: HIP-2 / assistance-fund orders never get an order
                        // status event. Without this branch the diff would sit in
                        // pending_new_diffs for 60s, be evicted as data loss (forcing a
                        // re-sync), and the spot book would permanently miss the system
                        // market maker's liquidity. Insert directly as an Alo limit order.
                        let inner_order = InnerL4Order {
                            user: diff.user(),
                            coin: coin.clone(),
                            side: diff.side(),
                            limit_px: diff_px,
                            sz,
                            oid: oid.clone().value(),
                            timestamp: time,
                            trigger_condition: "N/A".to_string(),
                            is_trigger: false,
                            trigger_px: "0.0".to_string(),
                            is_position_tpsl: false,
                            reduce_only: false,
                            order_type: "Limit".to_string(),
                            tif: Some("Alo".to_string()),
                            cloid: None,
                        };
                        // Same queue-anchor handling as a paired order (upstream insertBefore).
                        let anchor = insert_before.clone();
                        if self.order_book.add_order_before(inner_order, insert_before) {
                            self.note_insert_before_fallback(&oid, &coin, anchor.as_ref(), diff_px);
                        } else if anchor.is_some() {
                            crate::metrics::INSERT_BEFORE_HONORED_TOTAL.inc();
                        }
                        changed_coins.insert(coin);
                    } else {
                        // Status hasn't arrived yet - cache the diff size + resting px + queue anchor
                        self.pending_new_diffs.insert(oid.clone(), (sz, diff_px, insert_before, height));
                    }
                }
                // An order whose New diff still waits for its status is not on the book
                // yet: a Remove/Update arriving in that window must act on the pending
                // entry, or the late status installs a stale order (a permanent ghost /
                // wrong size - the statuses stream lags the diffs stream).
                InnerOrderDiff::Update { new_sz, .. } => {
                    if new_sz.is_zero() && self.pending_new_diffs.remove(&oid).is_some() {
                        // Filled while still waiting for its status: it never books;
                        // the Remove that follows pairs with this record, and the
                        // late status goes the existing orphan way.
                        self.zeroed_awaiting_remove.insert(oid, Instant::now());
                    } else if let Some((pending_sz, _, _, _)) = self.pending_new_diffs.get_mut(&oid) {
                        *pending_sz = new_sz;
                    } else if self.order_book.modify_sz(oid.clone(), coin.clone(), new_sz) {
                        if new_sz.is_zero() {
                            self.zeroed_awaiting_remove.insert(oid, Instant::now());
                        }
                    } else {
                        self.missing_diff_targets += 1;
                        note_diff_target_missing("update", &oid, &coin);
                    }
                    changed_coins.insert(coin);
                }
                InnerOrderDiff::Remove => {
                    if self.pending_new_diffs.remove(&oid).is_none()
                        && self.zeroed_awaiting_remove.remove(&oid).is_none()
                        && !self.order_book.cancel_order(oid.clone(), coin.clone())
                    {
                        self.missing_diff_targets += 1;
                        note_diff_target_missing("remove", &oid, &coin);
                    }
                    changed_coins.insert(coin);
                }
            }
        }
        Ok(changed_coins)
    }
}

/// An Update/Remove found its order neither on the book, nor pending, nor
/// (for a Remove) just zeroed by an Update. Counted for observation, never
/// re-synced automatically (design 000220's choice); the count implies no
/// particular cause. Logged at WARN, at most once per 10s with the running
/// total, so a real anomaly is visible without flooding the log.
fn note_diff_target_missing(diff: &'static str, oid: &Oid, coin: &Coin) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static LAST_WARN_MS: AtomicU64 = AtomicU64::new(0);
    const WARN_INTERVAL_MS: u64 = 10_000;
    let counter = crate::metrics::DIFF_TARGET_MISSING_TOTAL.with_label_values(&[diff]);
    counter.inc();
    let now_ms = super::parallel::now_unix_ms();
    let last = LAST_WARN_MS.load(Ordering::Relaxed);
    if now_ms.saturating_sub(last) >= WARN_INTERVAL_MS
        && LAST_WARN_MS.compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed).is_ok()
    {
        log::warn!(
            "{diff} diff for oid={oid:?} coin={coin:?}: order neither on the book nor pending \
             ({} {diff} misses so far)",
            counter.get()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order_book::multi_book::Snapshots;
    use crate::types::inner::InnerL4Order;
    use crate::types::{L4Order, OrderDiff};
    use alloy::primitives::Address;
    use chrono::NaiveDateTime;

    fn empty_state() -> OrderBookState {
        let snapshots = Snapshots::new(HashMap::new());
        OrderBookState::from_snapshot(snapshots, Vec::new(), 0, 0, true, false, true)
    }

    fn make_l4_order(coin: &str, oid: u64) -> L4Order {
        L4Order {
            user: None,
            coin: coin.to_string(),
            side: crate::order_book::types::Side::Bid,
            limit_px: "100.0".to_string(),
            sz: "1.0".to_string(),
            oid,
            timestamp: 1000,
            trigger_condition: "N/A".to_string(),
            is_trigger: false,
            trigger_px: "0.0".to_string(),
            children: Vec::new(),
            is_position_tpsl: false,
            reduce_only: false,
            order_type: "Limit".to_string(),
            orig_sz: "1.0".to_string(),
            tif: Some("Gtc".to_string()),
            cloid: None,
        }
    }

    fn make_order_status(coin: &str, oid: u64, status: &str) -> NodeDataOrderStatus {
        NodeDataOrderStatus {
            time: NaiveDateTime::parse_from_str("2024-01-15 10:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            user: Address::new([0; 20]),
            hash: Some("0xabc".to_string()),
            builder: None,
            status: status.to_string(),
            order: make_l4_order(coin, oid),
        }
    }

    fn make_order_diff(coin: &str, oid: u64, diff: OrderDiff) -> NodeDataOrderDiff {
        serde_json::from_value(serde_json::json!({
            "user": "0x0000000000000000000000000000000000000000",
            "oid": oid,
            "side": "B",
            "px": "100.0",
            "coin": coin,
            "raw_book_diff": diff
        })).unwrap()
    }

    fn make_status_batch(statuses: Vec<NodeDataOrderStatus>) -> Batch<NodeDataOrderStatus> {
        serde_json::from_value(serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": 100,
            "events": statuses
        })).unwrap()
    }

    fn make_diff_batch(diffs: Vec<NodeDataOrderDiff>) -> Batch<NodeDataOrderDiff> {
        serde_json::from_value(serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": 100,
            "events": diffs
        })).unwrap()
    }

    /// Block b's time: 100ms per block from the fixtures' base time.
    fn block_time_str(block: u64) -> String {
        let base = NaiveDateTime::parse_from_str("2024-01-15 10:30:00", "%Y-%m-%d %H:%M:%S").unwrap();
        (base + chrono::Duration::milliseconds(i64::try_from(block).unwrap() * 100))
            .format("%Y-%m-%dT%H:%M:%S%.9f")
            .to_string()
    }

    fn block_time_ms(block: u64) -> u64 {
        let time = NaiveDateTime::parse_from_str(&block_time_str(block), "%Y-%m-%dT%H:%M:%S%.9f").unwrap();
        u64::try_from(time.and_utc().timestamp_millis()).unwrap()
    }

    fn status_batch_at(block: u64, statuses: Vec<NodeDataOrderStatus>) -> Batch<NodeDataOrderStatus> {
        serde_json::from_value(serde_json::json!({
            "local_time": block_time_str(block),
            "block_time": block_time_str(block),
            "block_number": block,
            "events": statuses
        })).unwrap()
    }

    fn diff_batch_at(block: u64, diffs: Vec<NodeDataOrderDiff>) -> Batch<NodeDataOrderDiff> {
        serde_json::from_value(serde_json::json!({
            "local_time": block_time_str(block),
            "block_time": block_time_str(block),
            "block_number": block,
            "events": diffs
        })).unwrap()
    }

    fn new_diff(coin: &str, oid: u64) -> NodeDataOrderDiff {
        make_order_diff(coin, oid, OrderDiff::New { sz: "1.0".to_string(), insert_before: None })
    }

    /// Diff fixture with custom user/px (for special-address / resting-px tests).
    fn make_order_diff_full(coin: &str, oid: u64, user: &str, px: &str, diff: OrderDiff) -> NodeDataOrderDiff {
        serde_json::from_value(serde_json::json!({
            "user": user,
            "oid": oid,
            "side": "B",
            "px": px,
            "coin": coin,
            "raw_book_diff": diff
        })).unwrap()
    }

    // ==================== System-address & resting-px semantics ====================

    /// A New diff from HIP-2 (0xFF..FF) never gets an order status -> it must be
    /// inserted directly as a synthetic Alo order instead of sitting in
    /// pending_new_diffs until aged out as data loss (which forces a re-sync).
    #[test]
    fn test_special_address_new_diff_inserts_synthetic_order() {
        let mut state = empty_state();
        let hip2 = format!("0x{}", "ff".repeat(20));
        let diff = make_order_diff_full("@260", 7, &hip2, "325.5", OrderDiff::New { sz: "3.0".to_string(), insert_before: None });
        let changed = state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        assert!(changed.contains(&Coin::new("@260")), "synthetic insert must mark the coin changed");
        assert_eq!(state.order_count(), 1, "HIP-2 order must be inserted directly");
        assert_eq!(state.pending_new_diffs_count(), 0, "must not wait for a status that never comes");
        let (_, _, snap) = state.compute_snapshot_for_coin(&Coin::new("@260"), PxBand::default()).unwrap();
        let order = &snap.as_ref()[0][0];
        assert_eq!(order.limit_px, Px::parse_from_str("325.5").unwrap());
        assert_eq!(order.tif.as_deref(), Some("Alo"));
    }

    /// The resting price must come from the diff, not the status (trigger/converted
    /// orders can carry a different px on the status). Covers both arrival orders:
    /// diff first (px stored in pending) and status first (px applied on pairing).
    #[test]
    fn test_resting_px_comes_from_diff_not_status() {
        // status px is fixed at 100.0 (make_l4_order); diff px is 101.5 -> book must show 101.5
        for diff_first in [true, false] {
            let mut state = empty_state();
            let diff = make_order_diff_full(
                "ETH", 42, "0x0000000000000000000000000000000000000000", "101.5",
                OrderDiff::New { sz: "1.0".to_string(), insert_before: None },
            );
            let status = make_order_status("ETH", 42, "open");
            if diff_first {
                state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
                state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
            } else {
                state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
                state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
            }
            assert_eq!(state.order_count(), 1);
            let (_, _, snap) = state.compute_snapshot_for_coin(&Coin::new("ETH"), PxBand::default()).unwrap();
            let order = &snap.as_ref()[0][0];
            assert_eq!(
                order.limit_px,
                Px::parse_from_str("101.5").unwrap(),
                "resting px must come from the diff (diff_first={diff_first})"
            );
        }
    }

    /// Fail fast on unparseable diff px (= schema drift) instead of silently
    /// falling back to the status px, which would re-introduce wrong resting prices.
    #[test]
    fn test_bad_diff_px_fails_fast() {
        let mut state = empty_state();
        let diff = make_order_diff_full(
            "ETH", 43, "0x0000000000000000000000000000000000000000", "not-a-price",
            OrderDiff::New { sz: "1.0".to_string(), insert_before: None },
        );
        assert!(state.apply_order_diffs_hft(make_diff_batch(vec![diff])).is_err());
    }

    // ==================== Initialization Tests ====================

    #[test]
    fn test_from_snapshot_empty() {
        let state = empty_state();
        assert_eq!(state.height(), 0);
        assert_eq!(state.time(), 0);
        assert_eq!(state.order_count(), 0);
        assert_eq!(state.coin_count(), 0);
        assert_eq!(state.pending_order_statuses_count(), 0);
        assert_eq!(state.pending_new_diffs_count(), 0);
    }

    // ==================== Untriggered Trigger Orders ====================

    fn make_inner_order(coin: &str, oid: u64, is_trigger: bool) -> InnerL4Order {
        InnerL4Order {
            user: Address::new([1; 20]),
            coin: Coin::new(coin),
            side: crate::order_book::types::Side::Bid,
            limit_px: Px::new(100_000_000),
            sz: crate::order_book::Sz::new(100_000_000),
            oid,
            timestamp: 1000,
            trigger_condition: if is_trigger { "Price above 110".to_string() } else { "N/A".to_string() },
            is_trigger,
            trigger_px: if is_trigger { "110.0".to_string() } else { "0.0".to_string() },
            is_position_tpsl: false,
            reduce_only: false,
            order_type: if is_trigger { "Stop Market".to_string() } else { "Limit".to_string() },
            tif: None,
            cloid: None,
        }
    }

    fn make_trigger_status(coin: &str, oid: u64, status: &str, trigger_px: &str) -> NodeDataOrderStatus {
        let mut order = make_l4_order(coin, oid);
        order.is_trigger = true;
        order.trigger_px = trigger_px.to_string();
        order.trigger_condition = format!("Price above {trigger_px}");
        order.order_type = "Stop Market".to_string();
        NodeDataOrderStatus {
            time: NaiveDateTime::parse_from_str("2024-01-15 10:30:00", "%Y-%m-%d %H:%M:%S").unwrap(),
            user: Address::new([2; 20]),
            hash: Some("0xdef".to_string()),
            builder: None,
            status: status.to_string(),
            order,
        }
    }

    /// Snapshots in the hl-node CLI layout: trigger orders appended to the TAIL
    /// of BOTH sides (same oid in each), after the resting book orders.
    fn snapshot_with_triggers(coin: &str, resting_oids: &[u64], trigger_oids: &[u64]) -> Snapshots<InnerL4Order> {
        let mut bids: Vec<InnerL4Order> = resting_oids.iter().map(|&oid| make_inner_order(coin, oid, false)).collect();
        let mut asks: Vec<InnerL4Order> = Vec::new();
        for &oid in trigger_oids {
            bids.push(make_inner_order(coin, oid, true));
            asks.push(make_inner_order(coin, oid, true));
        }
        let snapshot = Snapshot::from_sides(bids, asks);
        Snapshots::new(std::iter::once((Coin::new(coin), snapshot)).collect())
    }

    #[test]
    fn test_from_snapshot_extracts_untriggered_triggers() {
        let snapshots = snapshot_with_triggers("BTC", &[1, 2], &[100, 101]);
        let state = OrderBookState::from_snapshot(snapshots, Vec::new(), 0, 0, true, false, true);
        // Triggers are kept in the side table (deduped from the two per-side
        // copies), not dropped - and never enter the book.
        assert_eq!(state.untriggered_count(), 2);
        assert_eq!(state.order_count(), 2);
        let (_, _, orders) = state.untriggered_snapshot(None);
        let mut oids: Vec<u64> = orders.iter().map(|o| o.oid).collect();
        oids.sort_unstable();
        assert_eq!(oids, vec![100, 101]);
        assert!(orders.iter().all(|o| o.is_trigger));
    }

    #[test]
    fn test_from_snapshot_ignore_triggers_false_keeps_old_semantics() {
        // Tests that opt out of trigger extraction must not populate the table.
        let snapshots = snapshot_with_triggers("BTC", &[1], &[]);
        let state = OrderBookState::from_snapshot(snapshots, Vec::new(), 0, 0, false, false, true);
        assert_eq!(state.untriggered_count(), 0);
    }

    #[test]
    fn test_open_trigger_status_upserts_untriggered() {
        let mut state = empty_state();
        let status = make_trigger_status("BTC", 500, "open", "110.0");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        assert_eq!(state.untriggered_count(), 1);
        // It never rests on the book and is NOT cached for diff pairing
        // (is_inserted_into_book is false for open triggers).
        assert_eq!(state.order_count(), 0);
        assert_eq!(state.pending_order_statuses_count(), 0);

        // A later "open" for the same oid (modify) replaces the entry.
        let status = make_trigger_status("BTC", 500, "open", "115.0");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        assert_eq!(state.untriggered_count(), 1);
        let (_, _, orders) = state.untriggered_snapshot(None);
        assert_eq!(orders[0].trigger_px, "115.0");
    }

    #[test]
    fn test_canceled_trigger_status_removes_untriggered() {
        let mut state = empty_state();
        state
            .apply_order_statuses_hft(make_status_batch(vec![make_trigger_status("BTC", 500, "open", "110.0")]))
            .unwrap();
        assert_eq!(state.untriggered_count(), 1);
        state
            .apply_order_statuses_hft(make_status_batch(vec![make_trigger_status("BTC", 500, "canceled", "110.0")]))
            .unwrap();
        assert_eq!(state.untriggered_count(), 0);
    }

    #[test]
    fn test_triggered_status_moves_order_from_untriggered_to_book() {
        let mut state = empty_state();
        state
            .apply_order_statuses_hft(make_status_batch(vec![make_trigger_status("BTC", 500, "open", "110.0")]))
            .unwrap();
        assert_eq!(state.untriggered_count(), 1);

        // Trigger fires: "triggered" status pairs with a New diff and the order
        // rests on the book; the untriggered entry must be evicted.
        state
            .apply_order_statuses_hft(make_status_batch(vec![make_trigger_status("BTC", 500, "triggered", "110.0")]))
            .unwrap();
        let diff = make_order_diff("BTC", 500, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        assert_eq!(state.untriggered_count(), 0);
        assert_eq!(state.order_count(), 1);
    }

    #[test]
    fn test_non_trigger_statuses_leave_untriggered_untouched() {
        let mut state = empty_state();
        add_resting_order(&mut state, "BTC", 42);
        state.apply_order_statuses_hft(make_status_batch(vec![make_order_status("BTC", 42, "filled")])).unwrap();
        assert_eq!(state.untriggered_count(), 0);
        assert_eq!(state.order_count(), 1);
    }

    #[test]
    fn test_spot_triggers_skipped_when_ignore_spot() {
        // Live path: spot trigger opens are not tracked under ignore_spot,
        // matching the diff path's spot filtering.
        let mut state =
            OrderBookState::from_snapshot(Snapshots::new(HashMap::new()), Vec::new(), 0, 0, true, true, true);
        state
            .apply_order_statuses_hft(make_status_batch(vec![
                make_trigger_status("@1", 1, "open", "1.0"),
                make_trigger_status("PURR/USDC", 2, "open", "1.0"),
                make_trigger_status("BTC", 3, "open", "110.0"),
            ]))
            .unwrap();
        assert_eq!(state.untriggered_count(), 1);
        let (_, _, orders) = state.untriggered_snapshot(None);
        assert_eq!(orders[0].coin, Coin::new("BTC"));

        // Snapshot path: spot triggers are likewise dropped at install.
        let spot_state = OrderBookState::from_snapshot(
            snapshot_with_triggers("@1", &[1], &[100]),
            Vec::new(),
            0,
            0,
            true,
            true,
            true,
        );
        assert_eq!(spot_state.untriggered_count(), 0);
    }

    #[test]
    fn test_from_snapshot_seeds_from_untriggered_list() {
        // The --include-trigger-orders dump path: untriggered orders arrive as a
        // flat list, not embedded in the book sides.
        let seed = vec![make_inner_order("BTC", 100, true), make_inner_order("ETH", 101, true)];
        let state = OrderBookState::from_snapshot(Snapshots::new(HashMap::new()), seed, 0, 0, true, false, true);
        assert_eq!(state.untriggered_count(), 2);
        assert_eq!(state.order_count(), 0);
        let (_, _, btc) = state.untriggered_snapshot(Some(&Coin::new("BTC")));
        assert_eq!(btc.len(), 1);
        assert_eq!(btc[0].oid, 100);
    }

    #[test]
    fn test_untriggered_tracking_gated_off() {
        // --bbo-only: neither the snapshot extraction nor live statuses populate
        // the table (the book still strips triggers), so the lightweight memory
        // envelope holds.
        let mut state = OrderBookState::from_snapshot(
            snapshot_with_triggers("BTC", &[1], &[100]),
            Vec::new(),
            0,
            0,
            true,
            false,
            false,
        );
        assert_eq!(state.untriggered_count(), 0);
        assert_eq!(state.order_count(), 1, "book triggers must still be stripped when tracking is off");
        state
            .apply_order_statuses_hft(make_status_batch(vec![make_trigger_status("BTC", 500, "open", "110.0")]))
            .unwrap();
        assert_eq!(state.untriggered_count(), 0);
    }

    #[test]
    fn test_untriggered_snapshot_coin_filter() {
        let mut state = empty_state();
        state
            .apply_order_statuses_hft(make_status_batch(vec![
                make_trigger_status("BTC", 1, "open", "110.0"),
                make_trigger_status("ETH", 2, "open", "110.0"),
                make_trigger_status("ETH", 3, "open", "110.0"),
            ]))
            .unwrap();
        let (_, _, all) = state.untriggered_snapshot(None);
        assert_eq!(all.len(), 3);
        let (_, _, eth) = state.untriggered_snapshot(Some(&Coin::new("ETH")));
        assert_eq!(eth.len(), 2);
        assert!(eth.iter().all(|o| o.coin == Coin::new("ETH")));
        let (_, _, none) = state.untriggered_snapshot(Some(&Coin::new("SOL")));
        assert!(none.is_empty());
    }

    // ==================== Bidirectional Cache: Status First ====================

    #[test]
    fn test_status_first_then_diff_adds_order() {
        let mut state = empty_state();

        // 1. OrderStatus arrives first → cached
        let status = make_order_status("BTC", 42, "open");
        let batch = make_status_batch(vec![status]);
        let changed = state.apply_order_statuses_hft(batch).unwrap();
        assert!(changed.is_empty()); // not added yet
        assert_eq!(state.pending_order_statuses_count(), 1);
        assert!(state.pending_order_statuses_has(&Oid::new(42)));

        // 2. OrderDiff::New arrives → order added immediately
        let diff = make_order_diff("BTC", 42, OrderDiff::New { sz: "1.5".to_string(), insert_before: None });
        let batch = make_diff_batch(vec![diff]);
        let changed = state.apply_order_diffs_hft(batch).unwrap();
        assert!(changed.contains(&Coin::new("BTC")));
        assert_eq!(state.pending_order_statuses_count(), 0); // consumed
        assert_eq!(state.order_count(), 1);
    }

    // ==================== insertBefore (ALO priority) ====================

    /// Rest an order via the paired status+diff HFT flow (all helpers use px 100.0,
    /// side Bid, so every order lands on the same level of the same book).
    fn add_resting_order(state: &mut OrderBookState, coin: &str, oid: u64) {
        let status = make_order_status(coin, oid, "open");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        let diff = make_order_diff(coin, oid, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
    }

    /// Bid-side queue order (front first) for the coin's single price level.
    fn bid_queue_oids(state: &OrderBookState, coin: &str) -> Vec<Oid> {
        let (_, _, snapshot) = state.compute_snapshot_for_coin(&Coin::new(coin), PxBand::default()).unwrap();
        snapshot.as_ref()[0].iter().map(InnerOrder::oid).collect()
    }

    #[test]
    fn test_insert_before_splices_ahead_of_anchor() {
        let mut state = empty_state();
        add_resting_order(&mut state, "BTC", 1);

        // Priority order 2 jumps in front of resting order 1 (status first, then diff)
        let status = make_order_status("BTC", 2, "open");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        let diff = make_order_diff("BTC", 2, OrderDiff::New { sz: "1.0".to_string(), insert_before: Some(1) });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();

        assert_eq!(bid_queue_oids(&state, "BTC"), vec![Oid::new(2), Oid::new(1)]);
        assert_eq!(state.take_insert_before_fallbacks(), 0);
    }

    #[test]
    fn test_insert_before_survives_pending_diff_cache() {
        let mut state = empty_state();
        add_resting_order(&mut state, "BTC", 1);

        // Diff for order 2 arrives BEFORE its status: the anchor must survive the cache
        let diff = make_order_diff("BTC", 2, OrderDiff::New { sz: "1.0".to_string(), insert_before: Some(1) });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        assert_eq!(state.pending_new_diffs_count(), 1);
        assert_eq!(state.order_count(), 1); // not added yet

        let status = make_order_status("BTC", 2, "open");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();

        assert_eq!(bid_queue_oids(&state, "BTC"), vec![Oid::new(2), Oid::new(1)]);
        assert_eq!(state.take_insert_before_fallbacks(), 0);
    }

    #[test]
    fn test_insert_before_missing_anchor_falls_back() {
        let mut state = empty_state();
        add_resting_order(&mut state, "BTC", 1);

        // Anchor 999 is not on the book: the order must still rest (at the back
        // of the level) and the batch must NOT error out
        let status = make_order_status("BTC", 2, "open");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        let diff = make_order_diff("BTC", 2, OrderDiff::New { sz: "1.0".to_string(), insert_before: Some(999) });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();

        assert_eq!(state.order_count(), 2);
        assert_eq!(bid_queue_oids(&state, "BTC"), vec![Oid::new(1), Oid::new(2)]);
        // The divergence is surfaced exactly once, then the counter resets
        assert_eq!(state.take_insert_before_fallbacks(), 1);
        assert_eq!(state.take_insert_before_fallbacks(), 0);
    }

    // ==================== Bidirectional Cache: Diff First ====================

    #[test]
    fn test_diff_first_then_status_adds_order() {
        let mut state = empty_state();

        // 1. OrderDiff::New arrives first → size cached
        let diff = make_order_diff("ETH", 99, OrderDiff::New { sz: "2.0".to_string(), insert_before: None });
        let batch = make_diff_batch(vec![diff]);
        let changed = state.apply_order_diffs_hft(batch).unwrap();
        assert!(changed.is_empty()); // not added yet
        assert_eq!(state.pending_new_diffs_count(), 1);
        assert!(state.pending_new_diffs_has(&Oid::new(99)));

        // 2. OrderStatus arrives → order added immediately
        let status = make_order_status("ETH", 99, "open");
        let batch = make_status_batch(vec![status]);
        let changed = state.apply_order_statuses_hft(batch).unwrap();
        assert!(changed.contains(&Coin::new("ETH")));
        assert_eq!(state.pending_new_diffs_count(), 0); // consumed
        assert_eq!(state.order_count(), 1);
    }

    // ==================== OrderDiff Update/Remove ====================

    #[test]
    fn test_diff_update_changes_coin() {
        let mut state = empty_state();
        // First add an order via the bidirectional path
        let status = make_order_status("BTC", 1, "open");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        let diff = make_order_diff("BTC", 1, OrderDiff::New { sz: "5.0".to_string(), insert_before: None });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        assert_eq!(state.order_count(), 1);

        // Now send Update
        let update = make_order_diff("BTC", 1, OrderDiff::Update { orig_sz: "5.0".to_string(), new_sz: "3.0".to_string() });
        let changed = state.apply_order_diffs_hft(make_diff_batch(vec![update])).unwrap();
        assert!(changed.contains(&Coin::new("BTC")));
    }

    #[test]
    fn test_diff_remove_changes_coin() {
        let mut state = empty_state();
        // Add order
        let status = make_order_status("BTC", 1, "open");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        let diff = make_order_diff("BTC", 1, OrderDiff::New { sz: "5.0".to_string(), insert_before: None });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();

        // Remove
        let remove = make_order_diff("BTC", 1, OrderDiff::Remove);
        let changed = state.apply_order_diffs_hft(make_diff_batch(vec![remove])).unwrap();
        assert!(changed.contains(&Coin::new("BTC")));
        assert_eq!(state.order_count(), 0);
    }

    /// Bitter-lesson regression (bm, 2026-10-09: PONS oid placed and removed one
    /// block apart): the statuses stream lags the diffs stream, so a Remove can
    /// arrive while the New still waits for its status. It must drop the pending
    /// New, or the late "open" status books a ghost that nothing ever removes.
    #[test]
    fn test_remove_before_status_drops_pending_new_and_late_status_never_books() {
        let mut state = empty_state();
        let new = make_order_diff("BTC", 1, OrderDiff::New { sz: "5.0".to_string(), insert_before: None });
        state.apply_order_diffs_hft(make_diff_batch(vec![new])).unwrap();
        assert!(state.pending_new_diffs_has(&Oid::new(1)));

        let remove = make_order_diff("BTC", 1, OrderDiff::Remove);
        let changed = state.apply_order_diffs_hft(make_diff_batch(vec![remove])).unwrap();
        assert!(changed.contains(&Coin::new("BTC")));
        assert_eq!(state.pending_new_diffs_count(), 0, "the Remove consumes the pending New");

        let late_open = make_order_status("BTC", 1, "open");
        state.apply_order_statuses_hft(make_status_batch(vec![late_open])).unwrap();
        assert_eq!(state.order_count(), 0, "the late status must not book a removed order");
    }

    /// Same race for a size change: the late status books the order with the
    /// pending New's size, so an Update in the window must resize the pending entry.
    #[test]
    fn test_update_before_status_resizes_pending_new() {
        let mut state = empty_state();
        let new = make_order_diff("BTC", 1, OrderDiff::New { sz: "5.0".to_string(), insert_before: None });
        state.apply_order_diffs_hft(make_diff_batch(vec![new])).unwrap();
        let update = make_order_diff("BTC", 1, OrderDiff::Update { orig_sz: "5.0".to_string(), new_sz: "3.0".to_string() });
        state.apply_order_diffs_hft(make_diff_batch(vec![update])).unwrap();

        state.apply_order_statuses_hft(make_status_batch(vec![make_order_status("BTC", 1, "open")])).unwrap();
        assert_eq!(state.order_count(), 1);
        let (_, _, snapshot) = state.compute_snapshot_for_coin(&Coin::new("BTC"), PxBand::default()).unwrap();
        assert_eq!(snapshot.as_ref()[0][0].sz(), crate::order_book::Sz::parse_from_str("3.0").unwrap(), "booked with the updated size");
    }

    /// A fully filled order arrives as Update(newSz=0) then Remove (mainnet,
    /// 2026-10-09: ~38/s, every zero update followed by a Remove). The zero update
    /// already drops the order, so the Remove must pair with it, not count as missing.
    #[test]
    fn test_zero_size_update_then_remove_is_paired_not_missing() {
        let mut state = empty_state();
        state.apply_order_statuses_hft(make_status_batch(vec![make_order_status("BTC", 1, "open")])).unwrap();
        let new = make_order_diff("BTC", 1, OrderDiff::New { sz: "5.0".to_string(), insert_before: None });
        state.apply_order_diffs_hft(make_diff_batch(vec![new])).unwrap();
        assert_eq!(state.order_count(), 1);

        let fill = make_order_diff("BTC", 1, OrderDiff::Update { orig_sz: "5.0".to_string(), new_sz: "0.0".to_string() });
        state.apply_order_diffs_hft(make_diff_batch(vec![fill])).unwrap();
        assert_eq!(state.order_count(), 0, "a zero-size update removes the order");
        assert!(state.zeroed_awaiting_remove.contains_key(&Oid::new(1)));

        let remove = make_order_diff("BTC", 1, OrderDiff::Remove);
        state.apply_order_diffs_hft(make_diff_batch(vec![remove])).unwrap();
        assert!(state.zeroed_awaiting_remove.is_empty(), "the Remove pairs with the zero update");
        assert_eq!(state.missing_diff_targets, 0);
    }

    /// Review 000222 P2: filled while the New still waits for its status. Both
    /// arrival orders of the late status must leave no booked order, consume the
    /// zero record, and count nothing as missing.
    #[test]
    fn test_zero_update_while_pending_then_status_and_remove_in_either_order() {
        for status_before_remove in [true, false] {
            let mut state = empty_state();
            let new = make_order_diff("BTC", 1, OrderDiff::New { sz: "5.0".to_string(), insert_before: None });
            state.apply_order_diffs_hft(make_diff_batch(vec![new])).unwrap();
            let fill = make_order_diff("BTC", 1, OrderDiff::Update { orig_sz: "5.0".to_string(), new_sz: "0.0".to_string() });
            state.apply_order_diffs_hft(make_diff_batch(vec![fill])).unwrap();
            assert_eq!(state.pending_new_diffs_count(), 0, "a zero update ends the pending New");

            let status = || make_status_batch(vec![make_order_status("BTC", 1, "open")]);
            let remove = || make_diff_batch(vec![make_order_diff("BTC", 1, OrderDiff::Remove)]);
            if status_before_remove {
                state.apply_order_statuses_hft(status()).unwrap();
                state.apply_order_diffs_hft(remove()).unwrap();
            } else {
                state.apply_order_diffs_hft(remove()).unwrap();
                state.apply_order_statuses_hft(status()).unwrap();
            }
            assert_eq!(state.order_count(), 0, "never booked (status_before_remove={status_before_remove})");
            assert!(state.zeroed_awaiting_remove.is_empty(), "the Remove consumed the zero record");
            assert_eq!(state.missing_diff_targets, 0, "nothing counted as missing");
        }
    }

    #[test]
    fn test_aged_zeroed_entries_are_dropped_without_data_loss() {
        let mut state = empty_state();
        state.apply_order_statuses_hft(make_status_batch(vec![make_order_status("BTC", 1, "open")])).unwrap();
        let new = make_order_diff("BTC", 1, OrderDiff::New { sz: "5.0".to_string(), insert_before: None });
        state.apply_order_diffs_hft(make_diff_batch(vec![new])).unwrap();
        let fill = make_order_diff("BTC", 1, OrderDiff::Update { orig_sz: "5.0".to_string(), new_sz: "0.0".to_string() });
        state.apply_order_diffs_hft(make_diff_batch(vec![fill])).unwrap();

        state.age_zeroed_entries(Duration::from_secs(61));
        assert!(!state.cleanup_stale_pending(), "an unpaired zero update loses nothing");
        assert!(state.zeroed_awaiting_remove.is_empty());
    }

    #[test]
    fn test_update_and_remove_of_unknown_order_are_counted_not_applied() {
        let mut state = empty_state();
        let metric = |diff: &str| crate::metrics::DIFF_TARGET_MISSING_TOTAL.with_label_values(&[diff]).get();
        let (updates_before, removes_before) = (metric("update"), metric("remove"));

        let update = make_order_diff("BTC", 7, OrderDiff::Update { orig_sz: "5.0".to_string(), new_sz: "3.0".to_string() });
        let remove = make_order_diff("BTC", 8, OrderDiff::Remove);
        state.apply_order_diffs_hft(make_diff_batch(vec![update, remove])).unwrap();

        assert_eq!(state.order_count(), 0);
        assert_eq!(state.missing_diff_targets, 2, "one update and one remove miss");
        // The process-global metric moves too (deltas: tests share the registry).
        assert!(metric("update") > updates_before);
        assert!(metric("remove") > removes_before);
    }

    // ==================== Status Filtering ====================

    #[test]
    fn test_non_insertable_status_not_cached() {
        let mut state = empty_state();
        // "filled" status should NOT be cached
        let status = make_order_status("BTC", 42, "filled");
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        assert_eq!(state.pending_order_statuses_count(), 0);
    }

    #[test]
    fn test_ioc_not_cached() {
        let mut state = empty_state();
        let mut status = make_order_status("BTC", 42, "open");
        status.order.tif = Some("Ioc".to_string());
        state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        assert_eq!(state.pending_order_statuses_count(), 0);
    }

    // ==================== Spot Filtering ====================

    #[test]
    fn test_spot_filtered_when_ignore_spot() {
        let snapshots = Snapshots::new(HashMap::new());
        let mut state = OrderBookState::from_snapshot(snapshots, Vec::new(), 0, 0, true, true, true); // ignore_spot=true

        let diff = make_order_diff("@1", 1, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
        let changed = state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        assert!(changed.is_empty());
        assert_eq!(state.pending_new_diffs_count(), 0); // skipped entirely
    }

    #[test]
    fn test_spot_not_filtered_when_not_ignoring() {
        let mut state = empty_state(); // ignore_spot=false
        let diff = make_order_diff("@1", 1, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
        state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        assert_eq!(state.pending_new_diffs_count(), 1); // cached
    }

    // ==================== Height/Time Tracking ====================

    #[test]
    fn test_height_updates_on_higher_block() {
        let mut state = empty_state();
        let batch: Batch<NodeDataOrderDiff> = serde_json::from_value(serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": 500,
            "events": []
        })).unwrap();
        state.apply_order_diffs_hft(batch).unwrap();
        assert_eq!(state.height(), 500);
    }

    #[test]
    fn test_height_not_downgraded() {
        let mut state = empty_state();
        // Set height to 500
        let batch: Batch<NodeDataOrderDiff> = serde_json::from_value(serde_json::json!({
            "local_time": "2024-01-15T10:31:00.000000000",
            "block_time": "2024-01-15T10:31:00.000000000",
            "block_number": 500,
            "events": []
        })).unwrap();
        state.apply_order_diffs_hft(batch).unwrap();

        // Try to go to 200
        let batch: Batch<NodeDataOrderDiff> = serde_json::from_value(serde_json::json!({
            "local_time": "2024-01-15T10:30:00.000000000",
            "block_time": "2024-01-15T10:30:00.000000000",
            "block_number": 200,
            "events": []
        })).unwrap();
        state.apply_order_diffs_hft(batch).unwrap();
        assert_eq!(state.height(), 500); // unchanged
    }

    // ==================== Cleanup Tests ====================

    /// 2026-10-10 regression: during a node catch-up the two book streams drift
    /// apart by minutes inside the server. Whichever leads, every order must
    /// still pair however many cleanups run while the other stream catches up.
    #[test]
    fn test_catch_up_drift_pairs_every_order_in_either_lead() {
        for statuses_lead in [true, false] {
            let mut state = empty_state();
            let lines = |block: u64| [block * 10, block * 10 + 1]; // two lines per block
            let feed_statuses = |state: &mut OrderBookState| {
                for block in 1..=100 {
                    for oid in lines(block) {
                        state.apply_order_statuses_hft(status_batch_at(block, vec![make_order_status("BTC", oid, "open")])).unwrap();
                    }
                    assert!(!state.cleanup_stale_pending(), "statuses_lead={statuses_lead} block={block}");
                }
            };
            let feed_diffs = |state: &mut OrderBookState| {
                for block in 1..=100 {
                    for oid in lines(block) {
                        state.apply_order_diffs_hft(diff_batch_at(block, vec![new_diff("BTC", oid)])).unwrap();
                    }
                    assert!(!state.cleanup_stale_pending(), "statuses_lead={statuses_lead} block={block}");
                }
            };
            if statuses_lead {
                feed_statuses(&mut state);
                feed_diffs(&mut state);
            } else {
                feed_diffs(&mut state);
                feed_statuses(&mut state);
            }
            assert_eq!(state.order_count(), 200, "statuses_lead={statuses_lead}");
            assert_eq!((state.pending_order_statuses_count(), state.pending_new_diffs_count()), (0, 0));
        }
    }

    /// An orphan status (its New diff never came, nor did a settling status in
    /// its block) waits while more lines of its block may come on either
    /// stream, and is dropped and counted once both streams apply a later block.
    #[test]
    fn test_orphan_status_dropped_only_after_both_streams_pass_its_block() {
        let mut state = empty_state();
        state.apply_order_statuses_hft(status_batch_at(10, vec![make_order_status("BTC", 1, "open")])).unwrap();

        state.apply_order_diffs_hft(diff_batch_at(10, Vec::new())).unwrap();
        assert!(!state.cleanup_stale_pending());
        assert!(state.pending_order_statuses_has(&Oid::new(1)), "block 10 may still carry its New diff");

        state.apply_order_diffs_hft(diff_batch_at(11, Vec::new())).unwrap();
        assert!(!state.cleanup_stale_pending());
        assert!(state.pending_order_statuses_has(&Oid::new(1)), "block 10 may still carry a settling status");

        state.apply_order_statuses_hft(status_batch_at(11, Vec::new())).unwrap();
        assert!(!state.cleanup_stale_pending(), "an orphan status is not data loss");
        assert!(!state.pending_order_statuses_has(&Oid::new(1)));
        assert_eq!(state.orphan_statuses, 1);
    }

    /// The diff stream leads and a cleanup lands between the two status lines
    /// of one block: the later "filled" still settles the entry (review 000255).
    #[test]
    fn test_settling_status_on_a_later_line_after_the_diff_stream_passed() {
        let mut state = empty_state();
        state.apply_order_statuses_hft(status_batch_at(10, vec![make_order_status("BTC", 1, "open")])).unwrap();
        state.apply_order_diffs_hft(diff_batch_at(11, Vec::new())).unwrap();
        assert!(!state.cleanup_stale_pending());

        state.apply_order_statuses_hft(status_batch_at(10, vec![make_order_status("BTC", 1, "filled")])).unwrap();
        state.apply_order_statuses_hft(status_batch_at(11, Vec::new())).unwrap();
        assert!(!state.cleanup_stale_pending());
        assert_eq!(state.pending_order_statuses_count(), 0);
        assert_eq!(state.orphan_statuses, 0);
    }

    /// An order that fills on entry gets "open" then "filled" in one block and
    /// never a New diff: evicted with the block, but not an orphan. A settled
    /// status still pairs with a New diff that does come (rested, then filled in
    /// the same block); a status from a later block does not settle it.
    #[test]
    fn test_status_settled_in_its_block_is_evicted_without_counting_as_orphan() {
        let mut state = empty_state();
        let block_10 = vec![
            make_order_status("BTC", 1, "open"),
            make_order_status("BTC", 1, "filled"),
            make_order_status("BTC", 2, "open"),
            make_order_status("BTC", 2, "filled"),
            make_order_status("BTC", 3, "open"),
        ];
        state.apply_order_statuses_hft(status_batch_at(10, block_10)).unwrap();
        state.apply_order_statuses_hft(status_batch_at(11, vec![make_order_status("BTC", 3, "canceled")])).unwrap();

        state.apply_order_diffs_hft(diff_batch_at(10, vec![new_diff("BTC", 2)])).unwrap();
        assert_eq!(state.order_count(), 1, "the settled status still pairs with its New diff");

        state.apply_order_diffs_hft(diff_batch_at(11, Vec::new())).unwrap();
        assert!(!state.cleanup_stale_pending());
        assert_eq!(state.pending_order_statuses_count(), 0, "the status stream is at 11 already");
        assert_eq!(state.orphan_statuses, 1, "only oid 3: its later status came in another block");
    }

    /// A New diff whose status never comes is data loss - but only once the
    /// status stream has applied a later block.
    #[test]
    fn test_new_diff_without_status_is_loss_once_status_stream_passes_its_block() {
        let mut state = empty_state();
        state.apply_order_diffs_hft(diff_batch_at(10, vec![new_diff("BTC", 1)])).unwrap();

        state.apply_order_statuses_hft(status_batch_at(10, vec![make_order_status("ETH", 2, "filled")])).unwrap();
        assert!(!state.cleanup_stale_pending(), "block 10 may still carry its status");
        assert!(state.pending_new_diffs_has(&Oid::new(1)));

        state.apply_order_statuses_hft(status_batch_at(11, Vec::new())).unwrap();
        assert!(state.cleanup_stale_pending(), "the status stream passed block 10: the order is lost");
        assert_eq!(state.pending_new_diffs_count(), 0);
    }

    /// A replayed status (node restart, below the status high-water mark) is
    /// evicted unpaired without counting as an orphan.
    #[test]
    fn test_replayed_status_below_the_status_high_water_mark_is_not_an_orphan() {
        let mut state = empty_state();
        state.apply_order_statuses_hft(status_batch_at(20, Vec::new())).unwrap();
        state.apply_order_statuses_hft(status_batch_at(10, vec![make_order_status("BTC", 1, "open")])).unwrap();
        assert!(state.pending_order_statuses_has(&Oid::new(1)), "a replayed status can still pair");

        state.apply_order_diffs_hft(diff_batch_at(21, Vec::new())).unwrap();
        state.apply_order_statuses_hft(status_batch_at(21, Vec::new())).unwrap();
        assert!(!state.cleanup_stale_pending());
        assert_eq!((state.pending_order_statuses_count(), state.orphan_statuses), (0, 0));
    }

    /// A node restart rewrites blocks below the status stream's high-water
    /// mark: every replayed New diff is evicted as lost (fail-closed - the
    /// rewind is never served) and each one adds to the logged total.
    #[test]
    fn test_replayed_new_diffs_below_the_status_high_water_mark_are_lost() {
        let mut state = empty_state();
        state.apply_order_statuses_hft(status_batch_at(20, Vec::new())).unwrap();
        state.apply_order_diffs_hft(diff_batch_at(10, vec![new_diff("BTC", 1), new_diff("BTC", 2)])).unwrap();
        assert!(state.cleanup_stale_pending());
        state.apply_order_diffs_hft(diff_batch_at(11, vec![new_diff("BTC", 3)])).unwrap();
        assert!(state.cleanup_stale_pending());
        assert_eq!((state.pending_new_diffs_count(), state.lost_new_diffs), (0, 3));
    }

    #[test]
    fn test_cap_overflow_clears_and_counts_as_loss() {
        let mut state = empty_state();
        for oid in 0..6 {
            state.apply_order_statuses_hft(status_batch_at(100, vec![make_order_status("BTC", oid, "open")])).unwrap();
        }
        assert!(!state.evict_pending(6), "at the cap: kept");
        assert_eq!(state.pending_order_statuses_count(), 6);
        state.apply_order_statuses_hft(status_batch_at(100, vec![make_order_status("BTC", 6, "open")])).unwrap();
        assert!(state.evict_pending(6), "over the cap: cleared as data loss");
        assert_eq!(state.pending_order_statuses_count(), 0);
    }

    /// l2/bbo frames carry the block time both streams applied; L4 snapshots
    /// keep the furthest (time, height), their splice point for L4 diffs.
    #[test]
    fn test_frames_stamp_the_slower_stream_l4_keeps_the_furthest() {
        let mut state = empty_state();
        state.apply_order_statuses_hft(status_batch_at(50, vec![make_order_status("BTC", 1, "open")])).unwrap();
        state.apply_order_diffs_hft(diff_batch_at(40, vec![new_diff("BTC", 2)])).unwrap();
        state.apply_order_statuses_hft(status_batch_at(40, vec![make_order_status("BTC", 2, "open")])).unwrap();

        assert_eq!((state.status_height(), state.diff_height()), (50, 40));
        assert_eq!(state.get_bbos_for_coins(&HashSet::from([Coin::new("BTC")])).0, block_time_ms(40));
        let (l2_time, _, _, _) = state.l2_snapshots_incremental(&HashSet::new(), &HashSet::new(), &mut HashMap::new());
        assert_eq!(l2_time, block_time_ms(40));
        let (l4_time, l4_height, _) = state.compute_snapshot_for_coin(&Coin::new("BTC"), PxBand::default()).unwrap();
        assert_eq!((l4_time, l4_height), (block_time_ms(50), 50));
    }

    #[test]
    fn test_snapshot_install_starts_both_streams_at_its_height() {
        let snapshots = Snapshots::new(HashMap::new());
        let time = block_time_ms(500);
        let state = OrderBookState::from_snapshot(snapshots, Vec::new(), 500, time, true, false, true);
        assert_eq!((state.status_height(), state.diff_height(), state.height()), (500, 500, 500));
        assert_eq!(state.book_time(), time);
    }

    #[test]
    fn test_cleanup_keeps_young_entries() {
        // Regression for the burst-nuke behavior: young in-flight halves must
        // survive cleanup so they can still pair with their other half.
        let mut state = empty_state();
        for i in 0..100u64 {
            let status = make_order_status("BTC", i, "open");
            state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
            let diff = make_order_diff("ETH", 1_000 + i, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
            state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        }
        assert!(!state.cleanup_stale_pending());
        assert_eq!(state.pending_order_statuses_count(), 100);
        assert_eq!(state.pending_new_diffs_count(), 100);
    }

    #[test]
    fn test_cleanup_below_threshold_no_op() {
        let mut state = empty_state();
        for i in 0..100u64 {
            let status = make_order_status("BTC", i, "open");
            state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        }
        assert!(!state.cleanup_stale_pending(), "below-threshold cleanup is not data loss");
        assert_eq!(state.pending_order_statuses_count(), 100); // not cleared
    }

    // ==================== Per-coin L4 snapshot ====================

    #[test]
    fn test_compute_snapshot_for_coin_returns_only_that_coin() {
        let mut state = empty_state();
        for (i, coin) in ["BTC", "ETH"].iter().enumerate() {
            let status = make_order_status(coin, i as u64, "open");
            state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
            let diff = make_order_diff(coin, i as u64, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
            state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        }

        let (_time, height, snapshot) = state.compute_snapshot_for_coin(&Coin::new("BTC"), PxBand::default()).unwrap();
        assert_eq!(height, 100); // batch helpers stamp block_number 100
        let [bids, asks] = snapshot.as_ref();
        assert_eq!(bids.len(), 1, "only BTC's single bid is included");
        assert!(asks.is_empty());

        assert!(
            state.compute_snapshot_for_coin(&Coin::new("DOGE"), PxBand::default()).is_none(),
            "unknown coin yields None"
        );
    }

    #[test]
    fn test_compute_snapshot_for_coin_band_filters_and_keeps_time_height() {
        let mut state = empty_state();
        for (oid, px) in [(1u64, "50000.0"), (2, "60000.0"), (3, "70000.0")] {
            let mut status = make_order_status("BTC", oid, "open");
            status.order.limit_px = px.to_string();
            state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
            // The diff px is the resting px (official PR#9 port), so it carries the price too.
            let new_order = OrderDiff::New { sz: "1.0".to_string(), insert_before: None };
            let diff = make_order_diff_full("BTC", oid, "0x0000000000000000000000000000000000000000", px, new_order);
            state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        }
        let (full_time, full_height, _) =
            state.compute_snapshot_for_coin(&Coin::new("BTC"), PxBand::default()).unwrap();

        let band = PxBand::parse(Some("55000"), Some("65000")).unwrap();
        let (time, height, snapshot) = state.compute_snapshot_for_coin(&Coin::new("BTC"), band).unwrap();
        assert_eq!((time, height), (full_time, full_height), "band must not change time/height stamping");
        let [bids, asks] = snapshot.as_ref();
        assert_eq!(bids.iter().map(|o| o.oid).collect::<Vec<_>>(), vec![2], "only the in-band bid survives");
        assert!(asks.is_empty());

        // A band matching nothing still yields a (time, height, empty snapshot),
        // not None - only a missing coin book is an error to the subscriber.
        let empty_band = PxBand::parse(Some("80000"), Some("90000")).unwrap();
        let (_, _, snapshot) = state.compute_snapshot_for_coin(&Coin::new("BTC"), empty_band).unwrap();
        let [bids, asks] = snapshot.as_ref();
        assert!(bids.is_empty() && asks.is_empty());
    }

    // ==================== Performance Tests ====================

    #[test]
    fn test_apply_diffs_performance() {
        let mut state = empty_state();
        // Pre-populate with order statuses
        for i in 0..1000u64 {
            let status = make_order_status("BTC", i, "open");
            state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        }

        // Time matching diffs arrival
        let start = Instant::now();
        for i in 0..1000u64 {
            let diff = make_order_diff("BTC", i, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
            state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        }
        let elapsed = start.elapsed();
        let per_event = elapsed / 1000;

        eprintln!(
            "[PERF] apply_order_diffs_hft: 1000 New diffs (with cached statuses): {:?} ({:?}/event)",
            elapsed, per_event
        );
        assert_eq!(state.order_count(), 1000);
        assert_eq!(state.pending_order_statuses_count(), 0);
    }

    #[test]
    fn test_apply_statuses_performance() {
        let mut state = empty_state();
        // Pre-populate with diffs
        for i in 0..1000u64 {
            let diff = make_order_diff("BTC", i, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
            state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        }

        let start = Instant::now();
        for i in 0..1000u64 {
            let status = make_order_status("BTC", i, "open");
            state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
        }
        let elapsed = start.elapsed();
        let per_event = elapsed / 1000;

        eprintln!(
            "[PERF] apply_order_statuses_hft: 1000 statuses (with cached diffs): {:?} ({:?}/event)",
            elapsed, per_event
        );
        assert_eq!(state.order_count(), 1000);
        assert_eq!(state.pending_new_diffs_count(), 0);
    }

    #[test]
    fn test_universe_computation() {
        let mut state = empty_state();
        // Add orders for multiple coins
        for (i, coin) in ["BTC", "ETH", "SOL"].iter().enumerate() {
            let status = make_order_status(coin, i as u64, "open");
            state.apply_order_statuses_hft(make_status_batch(vec![status])).unwrap();
            let diff = make_order_diff(coin, i as u64, OrderDiff::New { sz: "1.0".to_string(), insert_before: None });
            state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
        }
        let universe = state.compute_universe();
        assert_eq!(universe.len(), 3);
        assert!(universe.contains(&Coin::new("BTC")));
        assert!(universe.contains(&Coin::new("ETH")));
        assert!(universe.contains(&Coin::new("SOL")));
    }
    /// Official PR#9 port x upstream insertBefore: in both arrival orders (and for
    /// the HIP-2 synthetic path) the order rests at the diff px - not the status
    /// px (100.0 in `make_order_status`) - and in front of its anchor.
    #[test]
    fn test_diff_px_and_queue_anchor_survive_both_arrival_orders_and_hip2() {
        for special in [false, true] {
            for diff_first in [false, true] {
                let mut state = empty_state();
                let zero = "0x0000000000000000000000000000000000000000";
                let anchor =
                    make_order_diff_full("BTC", 1, zero, "101.5", OrderDiff::New { sz: "1".to_owned(), insert_before: None });
                state.apply_order_statuses_hft(make_status_batch(vec![make_order_status("BTC", 1, "open")])).unwrap();
                state.apply_order_diffs_hft(make_diff_batch(vec![anchor])).unwrap();
                let user = if special { "0xffffffffffffffffffffffffffffffffffffffff" } else { zero };
                let diff =
                    make_order_diff_full("BTC", 2, user, "101.5", OrderDiff::New { sz: "2".to_owned(), insert_before: Some(1) });
                let status = || make_status_batch(vec![make_order_status("BTC", 2, "open")]);
                if special {
                    state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
                } else if diff_first {
                    state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
                    state.apply_order_statuses_hft(status()).unwrap();
                } else {
                    state.apply_order_statuses_hft(status()).unwrap();
                    state.apply_order_diffs_hft(make_diff_batch(vec![diff])).unwrap();
                }
                let case = format!("special={special} diff_first={diff_first}");
                assert_eq!(bid_queue_oids(&state, "BTC"), vec![Oid::new(2), Oid::new(1)], "{case}");
                assert_eq!(state.take_insert_before_fallbacks(), 0, "{case}");
                let (_, _, snapshot) = state.compute_snapshot_for_coin(&Coin::new("BTC"), PxBand::default()).unwrap();
                let resting = Px::parse_from_str("101.5").unwrap();
                assert!(snapshot.as_ref()[0].iter().all(|order| order.limit_px() == resting), "{case}");
            }
        }
    }
}
