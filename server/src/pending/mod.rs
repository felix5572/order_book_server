//! The pending book (--pending-book, hyperliquid-trade design 000604): the listener's book as the actions in the
//! node's mempool will leave it. Batch 1 is the replica of the listener's book, on its own thread; batch 2a reads the
//! mempool (`mempool`, `feed`); batch 2b holds its lines' book actions (`actions`) in the pending set and overlays them
//! on the replica (`shadow`, `overlay`), settling them by the block log's bundle hashes (`blocks`, `record`; r6, r7),
//! serving nothing yet; the `extPendingBbo` / `extPendingTrades` channels come with batch 2c.

pub(crate) mod actions;
mod blocks;
mod feed;
pub(crate) mod mempool;
pub(crate) mod overlay;
mod record;
mod shadow;

use std::{
    io,
    sync::Arc,
    time::{Duration, Instant},
};

use log::{error, info};
use tokio::{sync::{mpsc, watch}, task::JoinHandle};

pub(crate) use self::{blocks::watch_blocks, shadow::PendingParams};
use self::{
    blocks::{BlockHashes, BlockIngest, BlockInput},
    feed::Feed,
    mempool::{MempoolIngest, MempoolInput, MempoolTx},
    shadow::{Shadow, Step},
};
use crate::{
    accounts::{book::ActionCoins, owner_thread::spawn_owner},
    listeners::order_book::replica::{BookReplica, ReplicaBatch, ReplicaControl, ReplicaFork, ReplicaInputs},
    metrics::{
        PENDING_BOOK_ALIGNMENT_BLOCKS, PENDING_BOOK_BACKLOG_EVENTS, PENDING_BOOK_BREAKS_TOTAL, PENDING_BOOK_CLOID_INDEX_ORDERS,
        PENDING_BOOK_CONFIRMED_HEIGHT, PENDING_BOOK_STEP_SECONDS, PENDING_BOOK_UNAVAILABLE_SECONDS_TOTAL,
        PENDING_BOOK_UNPROVEN_LEVELS,
    },
};

/// How often the thread looks for a generation the listener ended, exports its gauges and counts its time unavailable.
const TICK: Duration = Duration::from_millis(100);

/// The pending thread's node feeds - the mempool, the block log - and the account view's asset table to name the
/// mempool's coins by.
pub(crate) struct PendingFeeds {
    pub(crate) mempool: Feed<MempoolTx>,
    pub(crate) block_log: Feed<BlockHashes>,
    pub(crate) action_coins: watch::Receiver<Option<Arc<ActionCoins>>>,
}

/// The pending thread, owner of the replica, the feeds' reading and the pending set (`pending-book`, on its own OS
/// thread like the account owner's).
/// `spot` is whether the book keeps spot (`--markets` `spot` or `all`).
pub(crate) fn spawn_pending_book(
    inputs: ReplicaInputs,
    feeds: PendingFeeds,
    backlog_events: usize,
    params: PendingParams,
    spot: bool,
) -> io::Result<JoinHandle<()>> {
    spawn_owner("pending-book", run(inputs, feeds, backlog_events, params, spot))
}

async fn run(inputs: ReplicaInputs, feeds: PendingFeeds, backlog_events: usize, params: PendingParams, spot: bool) {
    let ReplicaInputs { control, forks, batches } = inputs;
    let mut listener = Feeds { forks, batches };
    let mut book = PendingBook::new(control, backlog_events, Shadow::new(params, spot));
    let PendingFeeds { mempool: Feed { events: mut mempool, losses: mempool_losses }, block_log, action_coins } = feeds;
    let Feed { events: mut blocks, losses: block_losses } = block_log;
    let mut readings = Readings { mempool: MempoolIngest::new(action_coins, mempool_losses), block_log: BlockIngest::new(block_losses) };
    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let closed = loop {
        tokio::select! {
            fork = listener.forks.recv() => match fork {
                Some(fork) => book.on_fork(fork),
                None => break "the listener's feed",
            },
            batch = listener.batches.recv() => match batch {
                Some(batch) => book.on_batch(batch, &mut listener.forks, readings.unreported()),
                None => break "the listener's feed",
            },
            event = mempool.recv() => match event {
                Some(event) => {
                    let input = readings.mempool.on_event(event);
                    // the predicted trades and book moves go out with batch 2c's channels
                    let _step = book.on_mempool(input, readings.mempool.action_coins().as_deref(), readings.unreported());
                }
                None => break "the mempool watcher",
            },
            // past the record's lead the log's watcher waits on its full queue (r7 revision 20)
            event = blocks.recv(), if book.takes_blocks() => match event {
                Some(event) => {
                    let input = readings.block_log.on_event(event);
                    let _step = book.on_block(input, readings.unreported());
                }
                None => break "the block log watcher",
            },
            _ = tick.tick() => book.on_tick(readings.unreported()),
        }
    };
    error!("pending book: {closed} closed; the pending thread ends");
}

/// The pending thread's readings of its two node feeds.
struct Readings {
    mempool: MempoolIngest,
    block_log: BlockIngest,
}

impl Readings {
    fn unreported(&self) -> Unreported {
        Unreported { mempool: self.mempool.loss_unreported(), block_log: self.block_log.loss_unreported() }
    }
}

/// The losses each feed's watcher found and has not reported yet.
#[derive(Debug, Clone, Copy, Default)]
struct Unreported {
    mempool: bool,
    block_log: bool,
}

/// The listener's two queues to the pending thread.
struct Feeds {
    forks: mpsc::Receiver<ReplicaFork>,
    batches: mpsc::Receiver<ReplicaBatch>,
}

/// The replica and the generation the pending thread wants - whatever of another generation reaches it is dropped -,
/// and the pending set over it.
struct PendingBook {
    control: Arc<ReplicaControl>,
    wanted: u64,
    replica: Option<BookReplica>,
    /// The listener's count of generations it ended, as last seen.
    ended: u64,
    backlog_events: usize,
    shadow: Shadow,
}

impl PendingBook {
    fn new(control: Arc<ReplicaControl>, backlog_events: usize, shadow: Shadow) -> Self {
        let ended = control.ended();
        control.request(1);
        Self { control, wanted: 1, replica: None, ended, backlog_events, shadow }
    }

    fn on_fork(&mut self, fork: ReplicaFork) {
        if fork.generation != self.wanted || self.replica.is_some() {
            return;
        }
        info!("pending book: replica generation {} forked", fork.generation);
        self.replica = Some(BookReplica::new(fork, self.backlog_events));
    }

    /// A batch of the wanted generation: its fork, queued before it, is taken first if it was not yet (review 000606
    /// r1 P2-2: the two queues keep no order between them).
    fn on_batch(&mut self, batch: ReplicaBatch, forks: &mut mpsc::Receiver<ReplicaFork>, unreported: Unreported) {
        self.sync(unreported);
        if batch.generation != self.wanted {
            return;
        }
        while self.replica.is_none() {
            let Ok(fork) = forks.try_recv() else {
                error!("pending book: a batch of generation {} came without its fork", batch.generation);
                return self.request_next("fork_missing");
            };
            self.on_fork(fork);
        }
        let Some(replica) = &mut self.replica else { return };
        let confirmed = replica.confirmed();
        match replica.push(batch.batch) {
            // a batch that only queued - its block not passed by both streams - changes no confirmed height, book or
            // time, so nothing to settle (review 000611 r3 P2-2); an aligning or advancing one does
            Ok(_) if replica.confirmed() == confirmed => {}
            Ok(changed) => {
                let started = Instant::now();
                // the predicted book's moves go out with batch 2c's channels
                let _step = self.shadow.on_applied(changed, replica);
                PENDING_BOOK_STEP_SECONDS.with_label_values(&["replica"]).observe(started.elapsed().as_secs_f64());
            }
            Err(stop) => {
                error!("pending book: the replica stops ({stop:?}); another generation follows");
                self.request_next(stop.cause());
            }
        }
    }

    /// The mempool's next input, its asset ids named by `coins`: the predicted trades and the coins moved.
    fn on_mempool(&mut self, input: MempoolInput, coins: Option<&ActionCoins>, unreported: Unreported) -> Step {
        self.sync(unreported);
        match input {
            MempoolInput::Tx { tx, after_loss } => {
                let started = Instant::now();
                // a loss found since the line was taken is not before it in the file, so the line ends no gap
                let step = self.shadow.on_tx(&tx, after_loss || unreported.mempool, coins, self.replica.as_ref());
                PENDING_BOOK_STEP_SECONDS.with_label_values(&["transaction"]).observe(started.elapsed().as_secs_f64());
                step
            }
            MempoolInput::Loss => {
                self.shadow.on_mempool_loss();
                Step::default()
            }
        }
    }

    /// The block log's next input: the coins moved by the settling it brings.
    fn on_block(&mut self, input: BlockInput, unreported: Unreported) -> Step {
        self.sync(unreported);
        match input {
            BlockInput::Block { block, after_loss } => {
                let started = Instant::now();
                // a loss found since the block was taken is not before it in the file, so the block ends no gap
                let step = self.shadow.on_block(block, after_loss || unreported.block_log, self.replica.as_ref());
                PENDING_BOOK_STEP_SECONDS.with_label_values(&["block_log"]).observe(started.elapsed().as_secs_f64());
                step
            }
            BlockInput::Loss => {
                self.shadow.on_block_log_loss();
                Step::default()
            }
        }
    }

    fn takes_blocks(&self) -> bool {
        self.shadow.takes_blocks(self.replica.as_ref())
    }

    fn on_tick(&mut self, unreported: Unreported) {
        self.sync(unreported);
        let replica = self.replica.as_ref();
        let confirmed = replica.and_then(BookReplica::confirmed).unwrap_or(0);
        PENDING_BOOK_CONFIRMED_HEIGHT.set(i64::try_from(confirmed).unwrap_or(i64::MAX));
        PENDING_BOOK_ALIGNMENT_BLOCKS.set(replica.map_or(0, |replica| i64::try_from(replica.alignment_blocks()).unwrap_or(i64::MAX)));
        PENDING_BOOK_BACKLOG_EVENTS.set(replica.map_or(0, |replica| i64::try_from(replica.backlog_events()).unwrap_or(i64::MAX)));
        PENDING_BOOK_UNPROVEN_LEVELS.set(replica.map_or(0, |replica| i64::try_from(replica.unproven_levels()).unwrap_or(i64::MAX)));
        PENDING_BOOK_CLOID_INDEX_ORDERS.set(replica.map_or(0, |replica| i64::try_from(replica.cloid_orders()).unwrap_or(i64::MAX)));
        self.shadow.export(replica);
        if let Err(unavailable) = self.shadow.availability(replica) {
            PENDING_BOOK_UNAVAILABLE_SECONDS_TOTAL.with_label_values(&[unavailable.label()]).inc_by(TICK.as_secs_f64());
        }
    }

    /// What the listener and the feeds' watchers already flagged, taken before an input is handled (review 000611 r1
    /// P1-2): a generation the listener ended takes the replica with it, and a loss a watcher found - its report still
    /// queued behind the lines it may precede - is a gap at once.
    fn sync(&mut self, unreported: Unreported) {
        self.end_if_ended();
        if unreported.mempool {
            self.shadow.on_mempool_loss();
        }
        if unreported.block_log {
            self.shadow.on_block_log_loss();
        }
    }

    /// The listener ended the generation it fed (a full queue, its book untrusted): the replica goes.
    fn end_if_ended(&mut self) {
        let ended = self.control.ended();
        if ended == self.ended {
            return;
        }
        self.ended = ended;
        if self.replica.is_some() {
            info!("pending book: the listener ended replica generation {}; another follows", self.wanted);
        }
        self.request_next_quietly();
    }

    /// The replica stops on the pending thread's side, for `cause`.
    fn request_next(&mut self, cause: &str) {
        PENDING_BOOK_BREAKS_TOTAL.with_label_values(&[cause]).inc();
        self.request_next_quietly();
    }

    /// The next generation requested: the listener forks it once the old one's batches are all taken. The pending set
    /// has no replica meanwhile.
    fn request_next_quietly(&mut self) {
        self.replica = None;
        self.shadow.on_replica_gone();
        self.wanted += 1;
        self.control.request(self.wanted);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        listeners::order_book::replica::{
            ReplicaFeed, link,
            tests::{block_lines, btc_diff, btc_status, empty_book_batch, empty_line, listener_applied, listener_untrusted, new_diff},
        },
        pending::{
            actions::tests::{coins, requested},
            mempool::{ActionKind, MempoolTx, RequestedTif, SignedAction},
        },
    };

    const NONE_UNREPORTED: Unreported = Unreported { mempool: false, block_log: false };

    /// Block `height`: its hashes (none) through the block log first, as it leads, then its lines through the
    /// listener's feed, each taken by the pending book.
    fn push_block(book: &mut PendingBook, feed: &mut ReplicaFeed, feeds: &mut Feeds, height: u64, statuses: Vec<serde_json::Value>, diffs: Vec<serde_json::Value>) {
        let block = BlockHashes { height, time_ms: height * 100, hashes: Vec::new() };
        drop(book.on_block(BlockInput::Block { block, after_loss: false }, NONE_UNREPORTED));
        for batch in block_lines(height, statuses, diffs) {
            listener_applied(feed, batch);
            book.on_batch(feeds.batches.try_recv().unwrap(), &mut feeds.forks, NONE_UNREPORTED);
        }
    }

    fn line(seen_ms: i64, hash: u8, actions: Vec<ActionKind>) -> MempoolInput {
        let tx = MempoolTx {
            seen_ns: seen_ms * 1_000_000,
            tx_hash: alloy::primitives::B256::repeat_byte(hash),
            actions: actions.into_iter().map(|kind| SignedAction { vault: None, kind }).collect(),
        };
        MempoolInput::Tx { tx, after_loss: false }
    }

    fn ioc() -> MempoolInput {
        line(3_150, 9, vec![ActionKind::Order(vec![requested(0, true, "101", RequestedTif::Ioc, Some("0x01"))])])
    }

    /// A pending book served from 3,000 ms: generation 1 forked from an empty book, a block every 100 ms (the log's
    /// first at 100 ms), a mempool line at 1,000 ms, an ask of 1.0 at 101 resting from 2,500 ms.
    fn served_book() -> (PendingBook, ReplicaFeed, Feeds) {
        let (mut book, mut feed, mut feeds) = book_and_feed(1_024, 1_000_000);
        empty_book_batch(&mut feed);
        for height in 1..=31 {
            if height == 11 {
                drop(book.on_mempool(line(1_000, 1, vec![]), Some(&coins()), NONE_UNREPORTED));
            }
            let (statuses, diffs) = if height == 25 {
                (vec![btc_status(11, "A", "101.0", "1.0", "open", Some("0x0b"), "Alo", 2_500)], vec![btc_diff(11, "A", "101.0", new_diff("1.0"))])
            } else {
                (vec![], vec![])
            };
            push_block(&mut book, &mut feed, &mut feeds, height, statuses, diffs);
        }
        assert_eq!(book.shadow.availability(book.replica.as_ref()), Ok(()));
        (book, feed, feeds)
    }

    /// The IOC crossing the resting ask predicts its trade on a book served.
    #[test]
    fn a_served_book_predicts_a_taker() {
        let (mut book, _feed, _feeds) = served_book();
        assert_eq!(book.on_mempool(ioc(), Some(&coins()), NONE_UNREPORTED).trades.len(), 1);
    }

    /// Review 000611 r3 P2-2: a batch whose block both streams have not passed yet only queues - no settling; the one
    /// moving the confirmed height settles.
    #[test]
    fn a_batch_that_only_queues_settles_nothing() {
        let (mut book, mut feed, mut feeds) = served_book();
        let settles = book.shadow.settles();
        let block = BlockHashes { height: 32, time_ms: 3_200, hashes: Vec::new() };
        drop(book.on_block(BlockInput::Block { block, after_loss: false }, NONE_UNREPORTED));
        assert_eq!(book.shadow.settles(), settles + 1, "a block of the log settles");
        let [statuses, diffs] = block_lines(32, vec![], vec![]);
        listener_applied(&mut feed, statuses);
        book.on_batch(feeds.batches.try_recv().unwrap(), &mut feeds.forks, NONE_UNREPORTED);
        assert_eq!(book.shadow.settles(), settles + 1, "block 31 not passed by the diff stream yet");
        listener_applied(&mut feed, diffs);
        book.on_batch(feeds.batches.try_recv().unwrap(), &mut feeds.forks, NONE_UNREPORTED);
        assert_eq!((book.replica.as_ref().unwrap().confirmed(), book.shadow.settles()), (Some(31), settles + 2));
    }

    /// Review 000611 r1 P1-2: the listener ended the generation before the pending thread takes a mempool line; the
    /// line finds the replica gone - no trade.
    #[test]
    fn a_generation_ended_before_a_line_predicts_nothing() {
        let (mut book, mut feed, _feeds) = served_book();
        listener_untrusted(&mut feed);
        assert_eq!(book.on_mempool(ioc(), Some(&coins()), NONE_UNREPORTED).trades, shadow::tests::NO_TRADES);
        assert!(book.replica.is_none());
        assert_eq!(book.shadow.availability(None), Err(shadow::Unavailable::Replica));
    }

    /// Review 000611 r1 P1-2: a loss a watcher found, its report still queued, is a gap before the next input - a
    /// line or a tick; the mempool's or the block log's.
    #[test]
    fn a_loss_found_before_its_report_is_a_gap_at_once() {
        let mempool_lost = Unreported { mempool: true, block_log: false };
        let (mut book, _feed, _feeds) = served_book();
        assert_eq!(book.on_mempool(ioc(), Some(&coins()), mempool_lost).trades, shadow::tests::NO_TRADES);
        assert_eq!(book.shadow.availability(book.replica.as_ref()), Err(shadow::Unavailable::Mempool("loss")));
        let (mut book, _feed, _feeds) = served_book();
        book.on_tick(mempool_lost);
        assert_eq!(book.shadow.availability(book.replica.as_ref()), Err(shadow::Unavailable::Mempool("loss")));
        let (mut book, _feed, _feeds) = served_book();
        let block_log_lost = Unreported { mempool: false, block_log: true };
        assert_eq!(book.on_mempool(ioc(), Some(&coins()), block_log_lost).trades, shadow::tests::NO_TRADES);
        assert_eq!(book.shadow.availability(book.replica.as_ref()), Err(shadow::Unavailable::BlockLog("block_log_loss")));
    }

    fn book_and_feed(queue_batches: usize, backlog_events: usize) -> (PendingBook, ReplicaFeed, Feeds) {
        let (feed, inputs) = link(queue_batches);
        let book = PendingBook::new(Arc::clone(&inputs.control), backlog_events, Shadow::new(shadow::tests::params(), true));
        (book, feed, Feeds { forks: inputs.forks, batches: inputs.batches })
    }

    /// Review 000606 r1 P2-2: the listener forks and sends a first batch between the pending thread's looks at its two
    /// queues, and the batch is taken first: its fork is installed before it, and the batch applies.
    #[test]
    fn a_first_batch_taken_before_its_fork_installs_the_fork_first() {
        let (mut book, mut feed, mut feeds) = book_and_feed(16, 1_000);
        empty_book_batch(&mut feed);
        listener_applied(&mut feed, empty_line(true, 1));
        listener_applied(&mut feed, empty_line(false, 1));
        listener_applied(&mut feed, empty_line(true, 2));
        listener_applied(&mut feed, empty_line(false, 2));
        for _ in 0..4 {
            let batch = feeds.batches.try_recv().unwrap();
            book.on_batch(batch, &mut feeds.forks, NONE_UNREPORTED);
        }
        let replica = book.replica.as_ref().unwrap();
        assert_eq!((replica.confirmed(), replica.backlog_events()), (Some(1), 2), "the first batch was not lost");
        assert!(feeds.forks.try_recv().is_err());
    }

    /// A fork or a batch of a generation the pending thread gave up is dropped.
    #[test]
    fn another_generations_fork_and_batches_are_dropped() {
        let (mut book, mut feed, mut feeds) = book_and_feed(16, 1_000);
        empty_book_batch(&mut feed);
        let fork = feeds.forks.try_recv().unwrap();
        book.request_next("test");
        book.on_fork(fork);
        assert!(book.replica.is_none(), "generation 1 given up");
        listener_applied(&mut feed, empty_line(true, 1));
        book.on_batch(feeds.batches.try_recv().unwrap(), &mut feeds.forks, NONE_UNREPORTED);
        assert!(book.replica.is_none());
        empty_book_batch(&mut feed);
        book.on_fork(feeds.forks.try_recv().unwrap());
        assert!(book.replica.is_some(), "generation 2");
    }

    /// The listener ending a generation (here its book turning untrusted, with no batch after) drops the replica at
    /// the next tick and requests the next generation.
    #[test]
    fn a_generation_the_listener_ended_is_dropped() {
        let (mut book, mut feed, mut feeds) = book_and_feed(16, 1_000);
        empty_book_batch(&mut feed);
        book.on_fork(feeds.forks.try_recv().unwrap());
        listener_untrusted(&mut feed);
        book.on_tick(NONE_UNREPORTED);
        assert!(book.replica.is_none());
        assert_eq!(book.wanted, 2);
        empty_book_batch(&mut feed);
        assert_eq!(feeds.forks.try_recv().unwrap().generation, 2);
    }

    /// The replica past its budget stops on the pending thread's side: the listener no longer feeds that generation
    /// and forks the next.
    #[test]
    fn a_replica_past_its_budget_requests_the_next_generation() {
        let (mut book, mut feed, mut feeds) = book_and_feed(16, 1);
        empty_book_batch(&mut feed);
        book.on_fork(feeds.forks.try_recv().unwrap());
        for height in 1..=2 {
            listener_applied(&mut feed, empty_line(false, height));
            book.on_batch(feeds.batches.try_recv().unwrap(), &mut feeds.forks, NONE_UNREPORTED);
        }
        assert!(book.replica.is_none());
        assert_eq!(book.wanted, 2);
        empty_book_batch(&mut feed);
        assert_eq!(feeds.forks.try_recv().unwrap().generation, 2);
        listener_applied(&mut feed, empty_line(false, 3));
        assert_eq!(feeds.batches.try_recv().unwrap().generation, 2, "generation 1 no longer fed");
    }
}
