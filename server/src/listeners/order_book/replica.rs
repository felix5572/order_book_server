//! The pending book's replica of the listener's book (hyperliquid-trade design 000604 r1-r5, batch 1; review 000606).
//!
//! The listener forks its `OrderBookState` when the pending thread asks for a generation, then hands it a copy of
//! every book batch it applied. The pending thread applies them a block at a time, each block once both streams have
//! passed it, so the replica stands at the end of a block - a cut the listener's own book never claims
//! (`OrderBookState::book_time`: a leading diff stream's Update/Remove are already on it).
//!
//! - The pending thread names the generation it wants (`ReplicaControl::request`); whatever of another generation
//!   reaches it - a fork, a batch - is dropped, so nothing of a generation it gave up can be taken for the next one.
//! - The listener's side (`ReplicaFeed`, under the listener lock) only `try_send`s. It forks a requested generation
//!   once, and only with the batch queue empty (the old generation's backlog taken), the one-place fork slot free and
//!   both streams within `MAX_FORK_SKEW_BLOCKS` of each other (revisions 6, 10; review 000606 r1 P2-3). A full queue or
//!   its book turning untrusted - at the moment it turns (review 000606 r1 P2-4) - ends the generation it feeds.
//! - The pending thread's side (`BookReplica`) applies the heights both streams have passed (revision 1), within an
//!   event budget for the batches waiting on the slower stream (revision 6).

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use log::error;
use tokio::{
    sync::mpsc::{self, error::TrySendError},
    time::Instant,
};

use super::state::OrderBookState;
use crate::{
    metrics::{PENDING_BOOK_BREAKS_TOTAL, PENDING_BOOK_FORK_SECONDS, PENDING_BOOK_FORKS_TOTAL},
    types::node_data::{Batch, NodeDataOrderDiff, NodeDataOrderStatus},
};

/// Both streams within this many blocks of each other when the listener forks: a fork deeper in a catch-up would only
/// wait on the slower stream with a backlog of the faster (revision 6).
pub(crate) const MAX_FORK_SKEW_BLOCKS: u64 = 2;
/// Batches queued from the listener to the pending thread: ~3-6 s of mainnet's single-event book lines (1-2 x 10^4/s).
pub(crate) const REPLICA_QUEUE_BATCHES: usize = 65_536;
/// Applied batches between two cleanups of the replica's pairing caches, as the listener's cadence.
const CLEANUP_EVERY_BATCHES: usize = 1_000;

/// Shared by the listener's feed and the pending thread.
#[derive(Debug, Default)]
pub(crate) struct ReplicaControl {
    /// The generation the pending thread wants; 0 before its first request.
    requested: AtomicU64,
    /// Generations the listener ended (a full queue, its book untrusted): the pending thread then asks for another.
    ended: AtomicU64,
}

impl ReplicaControl {
    pub(crate) fn request(&self, generation: u64) {
        self.requested.store(generation, Ordering::Release);
    }

    fn requested(&self) -> u64 {
        self.requested.load(Ordering::Acquire)
    }

    pub(crate) fn ended(&self) -> u64 {
        self.ended.load(Ordering::Acquire)
    }

    /// The listener ends the generation it feeds, for `cause`.
    fn end(&self, cause: &str) {
        PENDING_BOOK_BREAKS_TOTAL.with_label_values(&[cause]).inc();
        self.ended.fetch_add(1, Ordering::AcqRel);
    }
}

/// A generation's start: the listener's state as forked.
pub(crate) struct ReplicaFork {
    pub(crate) generation: u64,
    state: Box<OrderBookState>,
}

/// A book batch the listener applied, for the generation being fed.
pub(crate) struct ReplicaBatch {
    pub(crate) generation: u64,
    pub(crate) batch: BookBatch,
}

/// A copy of a book batch the listener applied.
pub(crate) enum BookBatch {
    Statuses(Batch<NodeDataOrderStatus>),
    Diffs(Batch<NodeDataOrderDiff>),
}

impl BookBatch {
    const fn height(&self) -> u64 {
        match self {
            Self::Statuses(batch) => batch.block_number(),
            Self::Diffs(batch) => batch.block_number(),
        }
    }
}

/// The pending thread's ends of the feed.
pub(crate) struct ReplicaInputs {
    pub(crate) control: Arc<ReplicaControl>,
    pub(crate) forks: mpsc::Receiver<ReplicaFork>,
    pub(crate) batches: mpsc::Receiver<ReplicaBatch>,
}

/// The listener's end and the pending thread's, linked; the pending thread requests its first generation itself.
pub(crate) fn link(queue_batches: usize) -> (ReplicaFeed, ReplicaInputs) {
    let control = Arc::new(ReplicaControl::default());
    // one place: the listener forks only into a free one (revision 10)
    let (forks_tx, forks) = mpsc::channel(1);
    let (batches_tx, batches) = mpsc::channel(queue_batches);
    let feed = ReplicaFeed { control: Arc::clone(&control), forks: forks_tx, batches: batches_tx, feeding: None, forked: 0, gone: false };
    (feed, ReplicaInputs { control, forks, batches })
}

/// The generation being fed, with the book's withheld periods it was forked under.
#[derive(Debug, Clone, Copy)]
struct Feeding {
    generation: u64,
    withheld_periods: u64,
}

/// The listener's end of the feed, used under the listener lock.
pub(crate) struct ReplicaFeed {
    control: Arc<ReplicaControl>,
    forks: mpsc::Sender<ReplicaFork>,
    batches: mpsc::Sender<ReplicaBatch>,
    /// `None` while no generation is fed.
    feeding: Option<Feeding>,
    /// The last generation forked: each is forked once.
    forked: u64,
    /// The pending thread is gone: nothing is fed again.
    gone: bool,
}

impl ReplicaFeed {
    /// Before a book batch applies to `state`: a generation the pending thread gave up, or one the book's trust left
    /// in between, stops being fed; a newly requested one is forked when the old one's batches are all taken, the
    /// fork's place is free and both streams are near each other.
    pub(super) fn before_apply(&mut self, state: &OrderBookState, trusted: bool, withheld_periods: u64) {
        if self.gone {
            return;
        }
        let requested = self.control.requested();
        if let Some(feeding) = self.feeding {
            if feeding.generation != requested {
                self.feeding = None;
            } else if !trusted || withheld_periods != feeding.withheld_periods {
                self.end("book_untrusted");
            }
        }
        if self.feeding.is_some()
            || !trusted
            || requested <= self.forked
            || self.batches.capacity() < self.batches.max_capacity()
            || state.status_height().abs_diff(state.diff_height()) > MAX_FORK_SKEW_BLOCKS
        {
            return;
        }
        let forks = self.forks.clone();
        let permit = match forks.try_reserve() {
            Ok(permit) => permit,
            Err(TrySendError::Full(())) => return,
            Err(TrySendError::Closed(())) => return self.lose_consumer(),
        };
        let started = Instant::now();
        let state = Box::new(state.fork());
        PENDING_BOOK_FORK_SECONDS.observe(started.elapsed().as_secs_f64());
        PENDING_BOOK_FORKS_TOTAL.inc();
        self.forked = requested;
        permit.send(ReplicaFork { generation: requested, state });
        self.feeding = Some(Feeding { generation: requested, withheld_periods });
    }

    /// The listener's book turned untrusted (a desync, an apply error, a cleared pairing cache) or was replaced: the
    /// generation fed ends now, not at the next book batch (review 000606 r1 P2-4).
    pub(super) fn on_untrusted(&mut self) {
        if self.feeding.is_some() {
            self.end("book_untrusted");
        }
    }

    /// Whether the batch about to apply is to be copied for the pending thread.
    pub(super) const fn feeding(&self) -> bool {
        self.feeding.is_some()
    }

    /// After `batch` applied to the listener's book: the pending thread's copy. A full queue ends the generation.
    pub(super) fn applied(&mut self, batch: BookBatch) {
        let Some(feeding) = self.feeding else { return };
        match self.batches.try_send(ReplicaBatch { generation: feeding.generation, batch }) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => self.end("queue_full"),
            Err(TrySendError::Closed(_)) => self.lose_consumer(),
        }
    }

    fn end(&mut self, cause: &str) {
        self.control.end(cause);
        self.feeding = None;
    }

    fn lose_consumer(&mut self) {
        error!("pending book: the pending thread is gone; the listener stops feeding its replica");
        PENDING_BOOK_BREAKS_TOTAL.with_label_values(&["consumer_gone"]).inc();
        self.feeding = None;
        self.gone = true;
    }
}

/// Why a replica stops: the pending thread drops it and requests another generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReplicaStop {
    /// The batches waiting on the slower stream passed the event budget (revision 6).
    Budget { events: usize },
    /// A batch the listener applied did not apply to the replica: a contract violation.
    Apply(String),
    /// The pairing caches' cleanup evicted potentially live data.
    PendingCleared,
}

impl ReplicaStop {
    pub(crate) const fn cause(&self) -> &'static str {
        match self {
            Self::Budget { .. } => "backlog_budget",
            Self::Apply(_) => "apply_error",
            Self::PendingCleared => "pending_cleared",
        }
    }
}

/// A fork's state plus the batches applied after it, each block applied once both streams passed it (revision 1).
pub(crate) struct BookReplica {
    state: OrderBookState,
    statuses: VecDeque<Batch<NodeDataOrderStatus>>,
    diffs: VecDeque<Batch<NodeDataOrderDiff>>,
    /// The highest height each stream delivered: the fork's progress to start with.
    seen_statuses: u64,
    seen_diffs: u64,
    /// The furthest stream's height at the fork: the replica stands at a block's end only from there on.
    aligned_from: u64,
    /// Every queued batch at or below it has applied.
    through: u64,
    backlog_events: usize,
    budget_events: usize,
    applied_since_cleanup: usize,
}

impl BookReplica {
    pub(crate) fn new(fork: ReplicaFork, budget_events: usize) -> Self {
        let (seen_statuses, seen_diffs) = (fork.state.status_height(), fork.state.diff_height());
        Self {
            state: *fork.state,
            statuses: VecDeque::new(),
            diffs: VecDeque::new(),
            seen_statuses,
            seen_diffs,
            aligned_from: seen_statuses.max(seen_diffs),
            through: 0,
            backlog_events: 0,
            budget_events,
            applied_since_cleanup: 0,
        }
    }

    /// The height the replica stands at the end of: every event of both streams up to it and none beyond, once the
    /// streams passed the fork's furthest height; `None` before.
    pub(crate) const fn confirmed(&self) -> Option<u64> {
        if self.through >= self.aligned_from { Some(self.through) } else { None }
    }

    /// Blocks between the confirmed height and the newest batch received.
    pub(crate) fn alignment_blocks(&self) -> u64 {
        self.seen_statuses.max(self.seen_diffs).saturating_sub(self.through)
    }

    pub(crate) const fn backlog_events(&self) -> usize {
        self.backlog_events
    }

    /// Levels whose queue order the replica cannot prove: the fork's, carried over, and any since; no fill is to be
    /// attributed to a maker there.
    pub(crate) fn unproven_levels(&self) -> usize {
        self.state.unproven_levels()
    }

    /// A batch the listener applied: queued, then every height both streams have passed applied.
    pub(crate) fn push(&mut self, batch: BookBatch) -> Result<(), ReplicaStop> {
        let height = batch.height();
        // an empty batch takes a place in the budget too (review 000604 r3 P2-1)
        self.backlog_events += batch_events(&batch);
        match batch {
            BookBatch::Statuses(batch) => {
                self.seen_statuses = self.seen_statuses.max(height);
                self.statuses.push_back(batch);
            }
            BookBatch::Diffs(batch) => {
                self.seen_diffs = self.seen_diffs.max(height);
                self.diffs.push_back(batch);
            }
        }
        if self.backlog_events > self.budget_events {
            return Err(ReplicaStop::Budget { events: self.backlog_events });
        }
        self.apply_passed()
    }

    /// Each height both streams have passed - one of them delivered a later one - applied whole, statuses then diffs:
    /// an order's status and its New diff share a block, and the state pairs them either way round.
    fn apply_passed(&mut self) -> Result<(), ReplicaStop> {
        let passed = self.seen_statuses.min(self.seen_diffs).saturating_sub(1);
        loop {
            let next = [self.statuses.front().map(Batch::block_number), self.diffs.front().map(Batch::block_number)]
                .into_iter()
                .flatten()
                .min();
            let Some(height) = next.filter(|height| *height <= passed) else { break };
            while let Some(batch) = self.statuses.pop_front_if(|batch| batch.block_number() == height) {
                self.backlog_events -= batch_events_of(&batch);
                self.state.apply_order_statuses_hft(batch).map_err(|err| ReplicaStop::Apply(err.to_string()))?;
                self.applied()?;
            }
            while let Some(batch) = self.diffs.pop_front_if(|batch| batch.block_number() == height) {
                self.backlog_events -= batch_events_of(&batch);
                self.state.apply_order_diffs_hft(batch).map_err(|err| ReplicaStop::Apply(err.to_string()))?;
                self.applied()?;
            }
        }
        self.through = self.through.max(passed);
        Ok(())
    }

    /// The listener's cleanup cadence on the replica's own pairing caches.
    fn applied(&mut self) -> Result<(), ReplicaStop> {
        self.applied_since_cleanup += 1;
        if self.applied_since_cleanup < CLEANUP_EVERY_BATCHES {
            return Ok(());
        }
        self.applied_since_cleanup = 0;
        if self.state.cleanup_stale_pending() { Err(ReplicaStop::PendingCleared) } else { Ok(()) }
    }

    #[cfg(test)]
    pub(super) const fn state(&self) -> &OrderBookState {
        &self.state
    }
}

/// A batch's place in the backlog budget: its events, one at least.
fn batch_events(batch: &BookBatch) -> usize {
    match batch {
        BookBatch::Statuses(batch) => batch_events_of(batch),
        BookBatch::Diffs(batch) => batch_events_of(batch),
    }
}

fn batch_events_of<E>(batch: &Batch<E>) -> usize {
    batch.events_len().max(1)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::order_book::{
        Coin, PxBand,
        multi_book::{ParentLinks, Snapshots, TriggerSection},
    };

    const BTC: &str = "BTC";

    /// The listener's next book batch over an empty book, through `feed`: a fork if one is due.
    pub(crate) fn empty_book_batch(feed: &mut ReplicaFeed) {
        feed.before_apply(&empty_state(), true, 0);
    }

    /// `batch` applied by the listener, its copy through `feed`.
    pub(crate) fn listener_applied(feed: &mut ReplicaFeed, batch: BookBatch) {
        feed.applied(batch);
    }

    /// The listener's book turned untrusted.
    pub(crate) fn listener_untrusted(feed: &mut ReplicaFeed) {
        feed.on_untrusted();
    }

    /// An empty line of the status (`true`) or the diff stream at `height`.
    pub(crate) fn empty_line(statuses_stream: bool, height: u64) -> BookBatch {
        if statuses_stream { BookBatch::Statuses(statuses(height, &[])) } else { BookBatch::Diffs(diffs(height, &[])) }
    }

    /// An empty book tracking its unproven levels, as the listener's with the pending book on.
    fn empty_state() -> OrderBookState {
        let mut state =
            OrderBookState::from_snapshot(Snapshots::new(HashMap::new()), TriggerSection::Absent, &ParentLinks::default(), 0, 0, true, false, true);
        state.track_unproven_levels();
        state
    }

    fn order_json(oid: u64, side: &str, px: &str) -> serde_json::Value {
        serde_json::json!({
            "coin": BTC, "side": side, "limitPx": px, "sz": "1.0", "oid": oid, "timestamp": 1000,
            "triggerCondition": "N/A", "isTrigger": false, "triggerPx": "0.0", "children": [], "isPositionTpsl": false,
            "reduceOnly": false, "orderType": "Limit", "origSz": "1.0", "tif": "Gtc", "cloid": null
        })
    }

    /// One line of the status stream at `height`: `(oid, side, px, status)` each.
    fn statuses(height: u64, events: &[(u64, &str, &str, &str)]) -> Batch<NodeDataOrderStatus> {
        let events: Vec<_> = events
            .iter()
            .map(|(oid, side, px, status)| serde_json::json!({
                "time": "2026-10-31T00:00:00.000000000", "user": "0x0000000000000000000000000000000000000001",
                "hash": null, "status": status, "order": order_json(*oid, side, px),
            }))
            .collect();
        serde_json::from_value(serde_json::json!({
            "local_time": "2026-10-31T00:00:00.000000000", "block_time": "2026-10-31T00:00:00.000000000",
            "block_number": height, "events": events,
        }))
        .unwrap()
    }

    /// One line of the diff stream at `height`: `(oid, side, px, raw diff)` each.
    fn diffs(height: u64, events: &[(u64, &str, &str, serde_json::Value)]) -> Batch<NodeDataOrderDiff> {
        diffs_by("0x0000000000000000000000000000000000000001", height, events)
    }

    /// As `diffs`, the orders `user`'s.
    fn diffs_by(user: &str, height: u64, events: &[(u64, &str, &str, serde_json::Value)]) -> Batch<NodeDataOrderDiff> {
        let events: Vec<_> = events
            .iter()
            .map(|(oid, side, px, diff)| serde_json::json!({
                "user": user, "oid": oid, "side": side, "px": px, "coin": BTC,
                "raw_book_diff": diff,
            }))
            .collect();
        serde_json::from_value(serde_json::json!({
            "local_time": "2026-10-31T00:00:00.000000000", "block_time": "2026-10-31T00:00:00.000000000",
            "block_number": height, "events": events,
        }))
        .unwrap()
    }

    fn new_order() -> serde_json::Value {
        serde_json::json!({"new": {"sz": "1.0"}})
    }

    fn remove() -> serde_json::Value {
        serde_json::json!("remove")
    }

    /// A book's BTC orders, bids then asks.
    fn btc(state: &OrderBookState) -> [Vec<u64>; 2] {
        let Some((_, _, snapshot)) = state.compute_snapshot_for_coin(&Coin::new(BTC), PxBand::default()) else { return [vec![], vec![]] };
        snapshot.as_ref().clone().map(|side| side.iter().map(|order| order.oid).collect())
    }

    fn apply(state: &mut OrderBookState, batch: BookBatch) {
        match batch {
            BookBatch::Statuses(batch) => drop(state.apply_order_statuses_hft(batch).unwrap()),
            BookBatch::Diffs(batch) => drop(state.apply_order_diffs_hft(batch).unwrap()),
        }
    }

    fn fork_of(state: &OrderBookState, budget_events: usize) -> BookReplica {
        BookReplica::new(ReplicaFork { generation: 1, state: Box::new(state.fork()) }, budget_events)
    }

    /// Blocks 10-12: oid 1 bids 100 at 10; oid 2 asks 101 and oid 3 asks 102 at 11, each block's lines split in two;
    /// oid 1 removed at 12. Each stream's lines in order, as the node writes them.
    fn blocks() -> (Vec<BookBatch>, Vec<BookBatch>) {
        let status_lines = vec![
            BookBatch::Statuses(statuses(10, &[(1, "B", "100.0", "open")])),
            BookBatch::Statuses(statuses(11, &[(2, "A", "101.0", "open")])),
            BookBatch::Statuses(statuses(11, &[(3, "A", "102.0", "open")])),
            BookBatch::Statuses(statuses(12, &[(1, "B", "100.0", "canceled")])),
            BookBatch::Statuses(statuses(13, &[])),
        ];
        let diff_lines = vec![
            BookBatch::Diffs(diffs(10, &[(1, "B", "100.0", new_order())])),
            BookBatch::Diffs(diffs(11, &[(2, "A", "101.0", new_order())])),
            BookBatch::Diffs(diffs(11, &[(3, "A", "102.0", new_order())])),
            BookBatch::Diffs(diffs(12, &[(1, "B", "100.0", remove())])),
            BookBatch::Diffs(diffs(13, &[])),
        ];
        (status_lines, diff_lines)
    }

    /// The reference: every event of both streams up to `height`, nothing beyond.
    fn reference_through(height: u64) -> [Vec<u64>; 2] {
        let mut state = empty_state();
        let (status_lines, diff_lines) = blocks();
        for batch in status_lines.into_iter().chain(diff_lines).filter(|batch| batch.height() <= height) {
            apply(&mut state, batch);
        }
        btc(&state)
    }

    /// Review 000604 r1 P2-1: block 11 in two lines on each stream, and the diff stream's removal at 12 arriving before
    /// the status stream's second line of 11: the replica confirms 11 only once both streams passed it, and then holds
    /// every event up to 11 - oid 1 still bidding - and none of 12.
    #[test]
    fn a_block_applies_once_both_streams_passed_it_and_not_a_later_removal() {
        let listener = empty_state();
        let mut replica = fork_of(&listener, 1_000);
        let (mut status_lines, mut diff_lines) = blocks();
        let take = |lines: &mut Vec<BookBatch>| lines.remove(0);
        for batch in [take(&mut diff_lines), take(&mut status_lines), take(&mut diff_lines), take(&mut status_lines)] {
            replica.push(batch).unwrap();
        }
        assert_eq!(replica.confirmed(), Some(10), "11 not passed by either stream yet");
        replica.push(take(&mut diff_lines)).unwrap();
        // the diff stream's 12 (oid 1 removed) before the status stream's second line of 11
        replica.push(take(&mut diff_lines)).unwrap();
        assert_eq!(replica.confirmed(), Some(10), "the status stream has not passed 11");
        replica.push(take(&mut status_lines)).unwrap();
        replica.push(take(&mut status_lines)).unwrap();
        assert_eq!(replica.confirmed(), Some(11));
        assert_eq!(btc(replica.state()), reference_through(11));
        assert_eq!(btc(replica.state()), [vec![1], vec![2, 3]], "oid 1 still bids at the end of 11");
        replica.push(take(&mut status_lines)).unwrap();
        replica.push(take(&mut diff_lines)).unwrap();
        assert_eq!((replica.confirmed(), btc(replica.state())), (Some(12), reference_through(12)));
    }

    /// Review 000604 r1 P2-1: a fork with block 11 half applied by the listener (the first line of each stream) stands
    /// at no block's end until both streams passed 11; then it equals the reference.
    #[test]
    fn a_fork_inside_a_block_is_confirmed_only_past_it() {
        let mut listener = empty_state();
        let (mut status_lines, mut diff_lines) = blocks();
        for _ in 0..2 {
            apply(&mut listener, status_lines.remove(0));
            apply(&mut listener, diff_lines.remove(0));
        }
        let mut replica = fork_of(&listener, 1_000);
        assert_eq!(replica.confirmed(), None);
        replica.push(status_lines.remove(0)).unwrap();
        replica.push(diff_lines.remove(0)).unwrap();
        replica.push(status_lines.remove(0)).unwrap();
        assert_eq!(replica.confirmed(), None, "the diff stream has not passed 11");
        replica.push(diff_lines.remove(0)).unwrap();
        assert_eq!((replica.confirmed(), btc(replica.state())), (Some(11), reference_through(11)));
    }

    /// A fork with the diff stream two blocks ahead (12 against 10) waits for both streams to pass 12: it already
    /// holds 12's removal.
    #[test]
    fn a_fork_with_the_streams_apart_waits_for_the_furthest() {
        let mut listener = empty_state();
        let (mut status_lines, diff_lines) = blocks();
        apply(&mut listener, status_lines.remove(0));
        for batch in diff_lines.into_iter().take(4) {
            apply(&mut listener, batch);
        }
        let mut replica = fork_of(&listener, 1_000);
        for _ in 0..3 {
            replica.push(status_lines.remove(0)).unwrap();
        }
        assert_eq!(replica.confirmed(), None, "12 not passed by the status stream");
        replica.push(status_lines.remove(0)).unwrap();
        replica.push(BookBatch::Diffs(diffs(13, &[]))).unwrap();
        assert_eq!((replica.confirmed(), btc(replica.state())), (Some(12), reference_through(12)));
    }

    /// Review 000604 r2 P2-1, r3 P2-1: batches waiting on a stalled stream count against the budget, an empty one too.
    #[test]
    fn the_backlog_past_its_budget_stops_the_replica() {
        let mut replica = fork_of(&empty_state(), 3);
        for height in 1..=3 {
            replica.push(BookBatch::Diffs(diffs(height, &[]))).unwrap();
        }
        assert_eq!(replica.backlog_events(), 3);
        assert_eq!(replica.push(BookBatch::Diffs(diffs(4, &[]))), Err(ReplicaStop::Budget { events: 4 }));
    }

    /// A batch the replica cannot apply is a contract violation, reported for the feed to break.
    #[test]
    fn a_batch_that_does_not_apply_stops_the_replica() {
        let mut replica = fork_of(&empty_state(), 1_000);
        replica.push(BookBatch::Statuses(statuses(1, &[]))).unwrap();
        let stop = replica.push(BookBatch::Diffs(diffs(1, &[(9, "B", "not a price", new_order())]))).and_then(|()| {
            replica.push(BookBatch::Statuses(statuses(2, &[])))?;
            replica.push(BookBatch::Diffs(diffs(2, &[])))
        });
        assert!(matches!(stop, Err(ReplicaStop::Apply(_))), "{stop:?}");
    }

    fn ask_101_unproven(state: &OrderBookState) -> bool {
        state.level_unproven(BTC, crate::order_book::Side::Ask, crate::order_book::Px::parse_from_str("101.0").unwrap())
    }

    /// The reference book through `height` of `lines`, applied block by block.
    fn reference_of(lines: &[BookBatch], height: u64) -> [Vec<u64>; 2] {
        let mut state = empty_state();
        let (statuses, diffs): (Vec<_>, Vec<_>) = lines.iter().filter(|batch| batch.height() <= height).partition(|batch| matches!(batch, BookBatch::Statuses(_)));
        for batch in statuses.into_iter().chain(diffs) {
            apply(&mut state, copy(batch));
        }
        btc(&state)
    }

    /// Review 000606 r1 P2-1, r2 (first case): asks 1 and 2 at 101; at 11 the New diff of 3 goes before 1, its status
    /// still to come; at 12 1 is removed. The listener applies the diffs first and forks (statuses 10, diffs 12);
    /// 3's status then finds its anchor gone and rests last: [2, 3] where the blocks give [3, 2]. At 14 4 goes before
    /// 3 - its anchor there -, at 15 3 leaves: [2, 4] against [4, 2]. The level stays unproven throughout, until it
    /// empties at 16.
    #[test]
    fn a_level_with_a_fallen_back_order_stays_unproven_until_it_empties() {
        let before = |oid: u64| serde_json::json!({"new": {"sz": "1.0", "insertBefore": oid}});
        let lines = vec![
            BookBatch::Statuses(statuses(10, &[(1, "A", "101.0", "open"), (2, "A", "101.0", "open")])),
            BookBatch::Diffs(diffs(10, &[(1, "A", "101.0", new_order()), (2, "A", "101.0", new_order())])),
            BookBatch::Diffs(diffs(11, &[(3, "A", "101.0", before(1))])),
            BookBatch::Diffs(diffs(12, &[(1, "A", "101.0", remove())])),
            BookBatch::Statuses(statuses(11, &[(3, "A", "101.0", "open")])),
            BookBatch::Statuses(statuses(12, &[(1, "A", "101.0", "canceled")])),
            BookBatch::Statuses(statuses(13, &[])),
            BookBatch::Diffs(diffs(13, &[])),
            BookBatch::Statuses(statuses(14, &[(4, "A", "101.0", "open")])),
            BookBatch::Diffs(diffs(14, &[(4, "A", "101.0", before(3))])),
            BookBatch::Statuses(statuses(15, &[(3, "A", "101.0", "canceled")])),
            BookBatch::Diffs(diffs(15, &[(3, "A", "101.0", remove())])),
            BookBatch::Statuses(statuses(16, &[(2, "A", "101.0", "canceled"), (4, "A", "101.0", "canceled")])),
            BookBatch::Diffs(diffs(16, &[(2, "A", "101.0", remove()), (4, "A", "101.0", remove())])),
            BookBatch::Statuses(statuses(17, &[])),
            BookBatch::Diffs(diffs(17, &[])),
        ];
        let mut listener = empty_state();
        for batch in &lines[..4] {
            apply(&mut listener, copy(batch));
        }
        let mut replica = fork_of(&listener, 1_000);
        let push = |replica: &mut BookReplica, lines: &[BookBatch]| {
            for batch in lines {
                replica.push(copy(batch)).unwrap();
            }
        };
        // up to both streams' 13: 12 passed
        push(&mut replica, &lines[4..8]);
        assert_eq!((replica.confirmed(), reference_of(&lines, 12)), (Some(12), [vec![], vec![3, 2]]));
        assert_eq!(btc(replica.state()), [vec![], vec![2, 3]], "the order lost in the listener's book");
        assert!(ask_101_unproven(replica.state()));
        // up to both streams' 16: 15 passed
        push(&mut replica, &lines[8..14]);
        assert_eq!((replica.confirmed(), btc(replica.state()), reference_of(&lines, 15)), (Some(15), [vec![], vec![2, 4]], [vec![], vec![4, 2]]));
        assert!(ask_101_unproven(replica.state()), "4 placed relative to 3's unproven place; 3 gone, the level is still unproven");
        push(&mut replica, &lines[14..]);
        assert_eq!((replica.confirmed(), replica.unproven_levels()), (Some(16), 0), "the level emptied");
    }

    /// Review 000606 r2 (second case): the New diff of 3 at 11 waits for its status; at 12 a HIP-2 order 4 (no status
    /// ever) rests at the same price; 3's status then rests it last: [4, 3] where the blocks give [3, 4], with no
    /// anchor missing anywhere. The listener's level is unproven, and the fork carries it.
    #[test]
    fn a_new_diff_overtaken_while_it_waits_leaves_its_level_unproven() {
        let hip2 = |height: u64| BookBatch::Diffs(diffs_by("0xffffffffffffffffffffffffffffffffffffffff", height, &[(4, "A", "101.0", new_order())]));
        let lines = vec![
            BookBatch::Statuses(statuses(10, &[])),
            BookBatch::Diffs(diffs(10, &[])),
            BookBatch::Diffs(diffs(11, &[(3, "A", "101.0", new_order())])),
            hip2(12),
            BookBatch::Statuses(statuses(11, &[(3, "A", "101.0", "open")])),
            BookBatch::Statuses(statuses(12, &[])),
            BookBatch::Statuses(statuses(13, &[])),
            BookBatch::Diffs(diffs(13, &[])),
        ];
        let mut listener = empty_state();
        for batch in &lines[..4] {
            apply(&mut listener, copy(batch));
        }
        let mut replica = fork_of(&listener, 1_000);
        for batch in &lines[4..] {
            replica.push(copy(batch)).unwrap();
        }
        assert_eq!((replica.confirmed(), reference_of(&lines, 12)), (Some(12), [vec![], vec![3, 4]]));
        assert_eq!(btc(replica.state()), [vec![], vec![4, 3]]);
        assert!(ask_101_unproven(replica.state()));
        // anchored, and overtaken by a later order going before the same anchor: [4, 3, 1] where the blocks give
        // [3, 4, 1]
        let before_one = serde_json::json!({"new": {"sz": "1.0", "insertBefore": 1}});
        let anchored = vec![
            BookBatch::Statuses(statuses(10, &[(1, "A", "101.0", "open")])),
            BookBatch::Diffs(diffs(10, &[(1, "A", "101.0", new_order())])),
            BookBatch::Diffs(diffs(11, &[(3, "A", "101.0", before_one.clone())])),
            BookBatch::Diffs(diffs_by("0xffffffffffffffffffffffffffffffffffffffff", 12, &[(4, "A", "101.0", before_one)])),
            BookBatch::Statuses(statuses(11, &[(3, "A", "101.0", "open")])),
        ];
        let mut listener = empty_state();
        for batch in &anchored {
            apply(&mut listener, copy(batch));
        }
        assert_eq!((btc(&listener), reference_of(&anchored, 12)), ([vec![], vec![4, 3, 1]], [vec![], vec![3, 4, 1]]));
        assert!(ask_101_unproven(&listener));
        // the same in the listener, with nothing overtaking: the level stays proven
        let mut quiet = empty_state();
        for batch in [&lines[0], &lines[1], &lines[2], &lines[4]] {
            apply(&mut quiet, copy(batch));
        }
        assert_eq!((btc(&quiet), quiet.unproven_levels()), ([vec![], vec![3]], 0));
    }

    fn copy(batch: &BookBatch) -> BookBatch {
        match batch {
            BookBatch::Statuses(batch) => BookBatch::Statuses(batch.clone()),
            BookBatch::Diffs(batch) => BookBatch::Diffs(batch.clone()),
        }
    }

    /// The listener forks a generation once the pending thread requested it, the book is trusted and both streams are
    /// near; each generation once.
    #[test]
    fn the_listener_forks_a_requested_generation_once() {
        let (mut feed, mut inputs) = link(16);
        let mut listener = empty_state();
        feed.before_apply(&listener, true, 0);
        assert!(!feed.feeding(), "nothing requested");
        inputs.control.request(1);
        feed.before_apply(&listener, false, 0);
        assert!(!feed.feeding(), "untrusted");
        apply(&mut listener, BookBatch::Diffs(diffs(5, &[])));
        feed.before_apply(&listener, true, 0);
        assert!(!feed.feeding(), "the diff stream 5 blocks ahead");
        apply(&mut listener, BookBatch::Statuses(statuses(4, &[])));
        feed.before_apply(&listener, true, 0);
        assert!(feed.feeding());
        assert_eq!(inputs.forks.try_recv().unwrap().generation, 1);
        feed.applied(BookBatch::Statuses(statuses(6, &[])));
        assert_eq!(inputs.batches.try_recv().unwrap().generation, 1);
        feed.before_apply(&listener, true, 0);
        assert!(inputs.forks.try_recv().is_err(), "generation 1 forked once");
        // the pending thread gives 1 up for 2: 1 is no longer fed, 2 is forked
        inputs.control.request(2);
        feed.before_apply(&listener, true, 0);
        assert_eq!(inputs.forks.try_recv().unwrap().generation, 2);
        feed.applied(BookBatch::Statuses(statuses(7, &[])));
        assert_eq!(inputs.batches.try_recv().unwrap().generation, 2);
    }

    /// Review 000606 r1 P2-3: a full queue ends the generation; the next one requested is forked only once the old
    /// one's batches are all taken, and its fork only into a free place.
    #[test]
    fn a_new_generation_waits_for_the_old_backlog_to_be_taken() {
        let (mut feed, mut inputs) = link(4);
        let listener = empty_state();
        inputs.control.request(1);
        feed.before_apply(&listener, true, 0);
        assert_eq!(inputs.forks.try_recv().unwrap().generation, 1, "its place free again");
        for height in 1..=5 {
            feed.applied(BookBatch::Statuses(statuses(height, &[])));
        }
        assert!(!feed.feeding());
        assert_eq!(inputs.control.ended(), 1, "the fifth found the queue full");
        inputs.control.request(2);
        drop(inputs.batches.try_recv().unwrap());
        feed.before_apply(&listener, true, 0);
        assert!(!feed.feeding(), "three of generation 1 still queued");
        while inputs.batches.try_recv().is_ok() {}
        feed.before_apply(&listener, true, 0);
        assert!(feed.feeding());
        assert_eq!(inputs.forks.try_recv().unwrap().generation, 2);
        // a requested generation's fork waits for its place too
        inputs.control.request(3);
        feed.before_apply(&listener, true, 0);
        assert_eq!(inputs.forks.try_recv().unwrap().generation, 3);
        inputs.control.request(4);
        feed.before_apply(&listener, true, 0);
        inputs.control.request(5);
        feed.before_apply(&listener, true, 0);
        assert!(!feed.feeding(), "generation 4's fork still in its place");
        assert_eq!(inputs.forks.try_recv().unwrap().generation, 4);
        feed.before_apply(&listener, true, 0);
        assert_eq!(inputs.forks.try_recv().unwrap().generation, 5);
    }

    /// Review 000606 r1 P2-4: the book turning untrusted ends the generation fed at once, before any next batch; a
    /// withheld period begun and ended between two batches ends it at the next.
    #[test]
    fn the_books_trust_lost_ends_the_generation() {
        let (mut feed, inputs) = link(16);
        let listener = empty_state();
        inputs.control.request(1);
        feed.before_apply(&listener, true, 0);
        feed.on_untrusted();
        assert!(!feed.feeding());
        assert_eq!(inputs.control.ended(), 1);
        inputs.control.request(2);
        drop(inputs);
        let (mut feed, mut inputs) = link(16);
        inputs.control.request(1);
        feed.before_apply(&listener, true, 0);
        drop(inputs.forks.try_recv().unwrap());
        feed.before_apply(&listener, true, 1);
        assert!(!feed.feeding());
        assert_eq!(inputs.control.ended(), 1);
    }

    /// The pending thread gone: the listener stops copying for good.
    #[test]
    fn a_gone_consumer_stops_the_feed() {
        let (mut feed, inputs) = link(16);
        let listener = empty_state();
        inputs.control.request(1);
        drop(inputs.forks);
        feed.before_apply(&listener, true, 0);
        assert!(!feed.feeding());
        inputs.control.request(2);
        feed.before_apply(&listener, true, 0);
        assert!(!feed.feeding());
    }

    /// Batch 1's measure (review 000604 r1, r3): the fork's time under the listener lock at bm's size - 524,302
    /// resting orders over 1,238 coins on 2026-10-31 - with mainnet's strings (order type, tif, a cloid). Run in
    /// release: `cargo test -p server --release --lib -- --ignored a_fork_at_mainnet_size --nocapture`.
    #[test]
    #[ignore = "a timing, run on demand in release"]
    fn a_fork_at_mainnet_size() {
        use crate::order_book::{OrderBook, Px, Side, Sz};
        use crate::types::inner::InnerL4Order;
        const COINS: u64 = 1_238;
        const ORDERS: u64 = 524_302;
        let mut books = HashMap::new();
        for coin in 0..COINS {
            let mut book: OrderBook<InnerL4Order> = OrderBook::new();
            for index in 0..ORDERS / COINS {
                let oid = coin * 1_000_000 + index;
                // a level holds a few orders, bids below asks
                let (side, level) = if index % 2 == 0 { (Side::Bid, 10_000 - index / 8) } else { (Side::Ask, 10_001 + index / 8) };
                book.add_order(InnerL4Order {
                    user: alloy::primitives::Address::repeat_byte(u8::try_from(index % 251).unwrap()),
                    coin: Coin::new(&format!("C{coin}")),
                    side,
                    limit_px: Px::new(level * 1_000),
                    sz: Sz::new(100_000_000),
                    oid,
                    timestamp: 1_793_428_885_409,
                    trigger_condition: "N/A".to_string(),
                    is_trigger: false,
                    trigger_px: "0.0".to_string(),
                    is_position_tpsl: false,
                    reduce_only: false,
                    order_type: "Limit".to_string(),
                    tif: Some("Alo".to_string()),
                    cloid: Some(format!("0x{oid:032x}")),
                });
            }
            books.insert(Coin::new(&format!("C{coin}")), book.to_snapshot());
        }
        let state = OrderBookState::from_snapshot(Snapshots::new(books), TriggerSection::Absent, &ParentLinks::default(), 0, 0, true, false, true);
        let mut times = Vec::new();
        for _ in 0..5 {
            let started = std::time::Instant::now();
            let fork = state.fork();
            times.push(started.elapsed());
            drop(fork);
        }
        println!("fork of {} orders over {COINS} coins: {times:?}", state.order_count());
    }

    /// The fork is the listener's state, silent on the process-wide metrics: applying to it leaves the listener's
    /// untouched.
    #[test]
    fn a_fork_is_a_copy() {
        let mut listener = empty_state();
        apply(&mut listener, BookBatch::Statuses(statuses(1, &[(1, "B", "100.0", "open")])));
        apply(&mut listener, BookBatch::Diffs(diffs(1, &[(1, "B", "100.0", new_order())])));
        let mut fork = listener.fork();
        apply(&mut fork, BookBatch::Diffs(diffs(2, &[(1, "B", "100.0", remove())])));
        assert_eq!((btc(&listener), btc(&fork)), ([vec![1], vec![]], [vec![], vec![]]));
    }
}
