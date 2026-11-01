//! The pending book (--pending-book, hyperliquid-trade design 000604): the listener's book as the actions in the
//! node's mempool will leave it. Batch 1 is the replica of the listener's book, on its own thread; batch 2a reads the
//! mempool (`mempool`, `feed`); batch 2b holds its lines' book actions (`actions`) in the pending set and overlays them
//! on the replica (`shadow`, `overlay`), settling them by the block log's bundle hashes (`blocks`, `record`; r6, r7);
//! batch 2c publishes the frames of the `extPendingBbo` / `extPendingTrades` channels (`publish`).

pub(crate) mod actions;
mod blocks;
mod feed;
pub(crate) mod mempool;
pub(crate) mod overlay;
pub(crate) mod publish;
mod record;
mod refusals;
mod signer;
mod twap_streams;
mod twaps;
pub(crate) mod shadow;

use std::{
    io,
    sync::Arc,
    time::{Duration, Instant},
};

use log::{error, info};
use tokio::{sync::{mpsc, watch}, task::JoinHandle};

pub(crate) use self::{
    blocks::watch_blocks,
    twap_streams::{watch_twap_fills, watch_twap_statuses},
    publish::{BboFrame, BestLevels, PendingBookHandle, PendingEvent, TradesFrame},
    shadow::{PendingParams, PendingTrade},
};
use self::{
    blocks::{LoggedBlock, BlockIngest, BlockInput},
    feed::{Feed, FeedEvent, FeedReports, Proven},
    mempool::{MempoolIngest, MempoolInput, MempoolTx},
    twap_streams::{TwapFillLine, TwapStatusLine},
    twaps::TwapStream,
    publish::Publisher,
    shadow::{Shadow, Step, wall_ms},
};
use crate::{
    accounts::{
        book::{ActionCoins, PendingAnchor},
        owner_thread::spawn_owner,
    },
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
    pub(crate) block_log: Feed<LoggedBlock>,
    /// The TWAP streams (batch 2h), each proving its read-throughs.
    pub(crate) twap_statuses: Feed<Proven<TwapStatusLine>>,
    pub(crate) twap_fills: Feed<Proven<TwapFillLine>>,
    /// The account view's anchor: its asset table and running TWAPs.
    pub(crate) anchor: watch::Receiver<Option<Arc<PendingAnchor>>>,
}

/// The pending thread, owner of the replica, the feeds' reading and the pending set (`pending-book`, on its own OS
/// thread like the account owner's).
/// `spot` is whether the book keeps spot (`--markets` `spot` or `all`); the frames go out through `handle`.
pub(crate) fn spawn_pending_book(
    inputs: ReplicaInputs,
    feeds: PendingFeeds,
    backlog_events: usize,
    params: PendingParams,
    spot: bool,
    handle: PendingBookHandle,
) -> io::Result<JoinHandle<()>> {
    let shadow = Shadow::new(params, spot, wall_ms());
    spawn_owner("pending-book", run(inputs, feeds, backlog_events, shadow, Publisher::new(handle)))
}

async fn run(inputs: ReplicaInputs, feeds: PendingFeeds, backlog_events: usize, shadow: Shadow, publisher: Publisher) {
    let ReplicaInputs { control, forks, batches } = inputs;
    let mut listener = Feeds { forks, batches };
    let mut book = PendingBook::new(control, backlog_events, shadow, publisher);
    let PendingFeeds { mempool: Feed { events: mut mempool, losses: mempool_losses }, block_log, twap_statuses, twap_fills, anchor } = feeds;
    let Feed { events: mut blocks, losses: block_losses } = block_log;
    let Feed { events: mut statuses, losses: statuses_losses } = twap_statuses;
    let Feed { events: mut fills, losses: fills_losses } = twap_fills;
    let mut anchors = anchor.clone();
    let mut anchors_open = true;
    let mut readings = Readings {
        mempool: MempoolIngest::new(anchor, mempool_losses),
        block_log: BlockIngest::new(block_losses),
        twap_statuses: FeedReports::new(statuses_losses),
        twap_fills: FeedReports::new(fills_losses),
    };
    let current = anchors.borrow_and_update().clone();
    if let Some(current) = current {
        book.on_anchor(current, readings.unreported());
    }
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
                    let anchor = readings.mempool.anchor();
                    let _published = book.on_mempool(input, anchor.as_deref().map(|anchor| &anchor.coins), readings.unreported());
                }
                None => break "the mempool watcher",
            },
            // past the record's lead the log's watcher waits on its full queue (r7 revision 20)
            event = blocks.recv(), if book.takes_blocks() => match event {
                Some(event) => {
                    let input = readings.block_log.on_event(event);
                    let _published = book.on_block(input, readings.unreported());
                }
                None => break "the block log watcher",
            },
            event = statuses.recv() => match event {
                Some(event) => {
                    let input = readings.twap_input(TwapStream::Statuses, event);
                    book.on_twap(input, readings.unreported());
                }
                None => break "the TWAP statuses watcher",
            },
            event = fills.recv() => match event {
                Some(event) => {
                    let input = readings.twap_input(TwapStream::Fills, event);
                    book.on_twap(input, readings.unreported());
                }
                None => break "the TWAP fills watcher",
            },
            changed = anchors.changed(), if anchors_open => {
                if changed.is_err() {
                    error!("pending book: the account view's anchors closed; the TWAPs wait for none");
                    anchors_open = false;
                    continue;
                }
                let anchor = anchors.borrow_and_update().clone();
                if let Some(anchor) = anchor {
                    book.on_anchor(anchor, readings.unreported());
                }
            }
            _ = tick.tick() => book.on_tick(readings.unreported()),
        }
    };
    error!("pending book: {closed} closed; the pending thread ends");
}

/// The pending thread's readings of its node feeds.
struct Readings {
    mempool: MempoolIngest,
    block_log: BlockIngest,
    twap_statuses: FeedReports,
    twap_fills: FeedReports,
}

impl Readings {
    fn unreported(&self) -> Unreported {
        Unreported {
            mempool: self.mempool.loss_unreported(),
            block_log: self.block_log.loss_unreported(),
            twaps: self.twap_statuses.loss_unreported() || self.twap_fills.loss_unreported(),
        }
    }

    /// A TWAP stream's event as an input, its losses reported.
    fn twap_input<T: Into<TwapLine>>(&mut self, stream: TwapStream, event: FeedEvent<Proven<T>>) -> TwapInput {
        let reports = match stream {
            TwapStream::Statuses => &mut self.twap_statuses,
            TwapStream::Fills => &mut self.twap_fills,
        };
        reports.take(&event);
        match event {
            FeedEvent::Line(Proven::Line(line)) => TwapInput::Line(line.into()),
            FeedEvent::Line(Proven::ReadThrough(at)) => TwapInput::ReadThrough(stream, at),
            FeedEvent::Unreadable { error, .. } => {
                error!("pending book: a {stream:?} TWAP line does not read, lost: {error}");
                TwapInput::Loss
            }
            FeedEvent::Lost { .. } => TwapInput::Loss,
        }
    }
}

/// A TWAP stream's line.
enum TwapLine {
    Statuses(TwapStatusLine),
    Fills(TwapFillLine),
}

impl From<TwapStatusLine> for TwapLine {
    fn from(line: TwapStatusLine) -> Self {
        Self::Statuses(line)
    }
}

impl From<TwapFillLine> for TwapLine {
    fn from(line: TwapFillLine) -> Self {
        Self::Fills(line)
    }
}

/// What a TWAP stream's event brings.
enum TwapInput {
    Line(TwapLine),
    ReadThrough(TwapStream, Instant),
    Loss,
}

/// The losses each feed's watcher found and has not reported yet.
#[derive(Debug, Clone, Copy, Default)]
struct Unreported {
    mempool: bool,
    block_log: bool,
    /// Either TWAP stream's.
    twaps: bool,
}

/// The listener's two queues to the pending thread.
struct Feeds {
    forks: mpsc::Receiver<ReplicaFork>,
    batches: mpsc::Receiver<ReplicaBatch>,
}

/// The replica and the generation the pending thread wants - whatever of another generation reaches it is dropped -,
/// the pending set over it, and the publisher of what it serves: every input's step goes out once handled.
struct PendingBook {
    control: Arc<ReplicaControl>,
    wanted: u64,
    replica: Option<BookReplica>,
    /// The listener's count of generations it ended, as last seen.
    ended: u64,
    backlog_events: usize,
    shadow: Shadow,
    publisher: Publisher,
}

impl PendingBook {
    fn new(control: Arc<ReplicaControl>, backlog_events: usize, shadow: Shadow, publisher: Publisher) -> Self {
        let ended = control.ended();
        control.request(1);
        Self { control, wanted: 1, replica: None, ended, backlog_events, shadow, publisher }
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
        let mut step = self.sync(unreported);
        step.absorb(self.apply_batch(batch, forks));
        self.publish(&step);
    }

    fn apply_batch(&mut self, batch: ReplicaBatch, forks: &mut mpsc::Receiver<ReplicaFork>) -> Step {
        if batch.generation != self.wanted {
            return Step::default();
        }
        while self.replica.is_none() {
            let Ok(fork) = forks.try_recv() else {
                error!("pending book: a batch of generation {} came without its fork", batch.generation);
                self.request_next("fork_missing");
                return Step::default();
            };
            self.on_fork(fork);
        }
        let Some(replica) = &mut self.replica else { return Step::default() };
        let confirmed = replica.confirmed();
        match replica.push(batch.batch) {
            // a batch that only queued - its block not passed by both streams - changes no confirmed height, book or
            // time, so nothing to settle (review 000611 r3 P2-2); an aligning or advancing one does
            Ok(_) if replica.confirmed() == confirmed => Step::default(),
            Ok(changed) => {
                let started = Instant::now();
                let step = self.shadow.on_applied(changed, replica);
                PENDING_BOOK_STEP_SECONDS.with_label_values(&["replica"]).observe(started.elapsed().as_secs_f64());
                step
            }
            Err(stop) => {
                error!("pending book: the replica stops ({stop:?}); another generation follows");
                self.request_next(stop.cause());
                Step::default()
            }
        }
    }

    /// The mempool's next input, its asset ids named by `coins`: the predicted trades and the coins moved.
    fn on_mempool(&mut self, input: MempoolInput, coins: Option<&ActionCoins>, unreported: Unreported) -> Step {
        let mut step = self.sync(unreported);
        step.absorb(match input {
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
        });
        self.publish(&step);
        step
    }

    /// The block log's next input: the coins moved by the settling it brings.
    fn on_block(&mut self, input: BlockInput, unreported: Unreported) -> Step {
        let mut step = self.sync(unreported);
        step.absorb(match input {
            BlockInput::Block { block, after_loss } => {
                let started = Instant::now();
                // a loss found since the block was taken is not before it in the file, so the block ends no gap
                let step = self.shadow.on_block(block, after_loss || unreported.block_log, self.replica.as_ref());
                PENDING_BOOK_STEP_SECONDS.with_label_values(&["block_log"]).observe(started.elapsed().as_secs_f64());
                step
            }
            BlockInput::Loss => self.shadow.on_block_log_loss(self.replica.as_ref()),
        });
        self.publish(&step);
        step
    }

    /// A TWAP stream's input (batch 2h).
    fn on_twap(&mut self, input: TwapInput, unreported: Unreported) {
        let mut step = self.sync(unreported);
        let replica = self.replica.as_ref();
        step.absorb(match input {
            TwapInput::Line(TwapLine::Statuses(line)) => self.shadow.on_twap_statuses(line.height, line.events, replica),
            TwapInput::Line(TwapLine::Fills(line)) => self.shadow.on_twap_fills(line.height, line.fills, replica),
            TwapInput::ReadThrough(stream, at) => self.shadow.on_twap_read_through(stream, at, replica),
            TwapInput::Loss => self.shadow.on_twap_loss(wall_ms(), replica),
        });
        self.publish(&step);
    }

    /// The account view's new anchor: its running TWAPs.
    fn on_anchor(&mut self, anchor: Arc<PendingAnchor>, unreported: Unreported) {
        let mut step = self.sync(unreported);
        step.absorb(self.shadow.on_anchor(anchor, self.replica.as_ref()));
        self.publish(&step);
    }

    fn takes_blocks(&self) -> bool {
        self.shadow.takes_blocks(self.replica.as_ref())
    }

    fn on_tick(&mut self, unreported: Unreported) {
        let step = self.sync(unreported);
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
        // what `sync` changed - a generation ended, a loss found - goes out though nothing else came
        self.publish(&step);
    }

    /// `step`'s frames, the availability with them; every coin's when the overlays were dropped or rebuilt whole and a
    /// replica is there to frame them on.
    fn publish(&mut self, step: &Step) {
        let aligned = self.replica.as_ref().is_some_and(|replica| replica.confirmed().is_some());
        let refresh_all = aligned && self.shadow.take_refresh_all();
        self.publisher.publish(step, refresh_all, &self.shadow, self.replica.as_ref());
    }

    /// What the listener and the feeds' watchers already flagged, taken before an input is handled (review 000611 r1
    /// P1-2): a generation the listener ended takes the replica with it, and a loss a watcher found - its report still
    /// queued behind the lines it may precede - is a gap at once. What it changed, published with the input's step
    /// (review 000624 r5 P1: the suborders a loss withdrew, in the commit telling the TWAPs unknown).
    fn sync(&mut self, unreported: Unreported) -> Step {
        self.end_if_ended();
        let mut step = Step::default();
        if unreported.mempool {
            self.shadow.on_mempool_loss();
        }
        if unreported.block_log {
            step.absorb(self.shadow.on_block_log_loss(self.replica.as_ref()));
        }
        if unreported.twaps {
            step.absorb(self.shadow.on_twap_loss(wall_ms(), self.replica.as_ref()));
        }
        step
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
    use crate::{order_book::Coin, pending::shadow::Unavailable};
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

    const NONE_UNREPORTED: Unreported = Unreported { mempool: false, block_log: false, twaps: false };

    /// Block `height`: its hashes (none) through the block log first, as it leads, then its lines through the
    /// listener's feed, each taken by the pending book.
    fn push_block(book: &mut PendingBook, feed: &mut ReplicaFeed, feeds: &mut Feeds, height: u64, statuses: Vec<serde_json::Value>, diffs: Vec<serde_json::Value>) {
        let block = LoggedBlock { height, time_ms: height * 100, hashes: Vec::new(), priorities: Vec::new(), taker_outcomes: Vec::new(), twap_cancels: Vec::new(), read_at: Instant::now() };
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
            actions: actions.into_iter().map(|kind| SignedAction { vault: None, signer: None, expires_after_ms: None, priority: 0, kind }).collect(),
        };
        MempoolInput::Tx { tx, after_loss: false }
    }

    fn ioc() -> MempoolInput {
        line(3_150, 9, vec![ActionKind::Order(vec![requested(0, true, "101", RequestedTif::Ioc, Some("0x01"))])])
    }

    /// A pending book served from 3,000 ms: generation 1 forked from an empty book, a block every 100 ms (the log's
    /// first at 100 ms), a mempool line at 1,000 ms, an ask of 1.0 at 101 resting from 2,500 ms.
    fn served_book() -> (PendingBook, ReplicaFeed, Feeds) {
        served_book_with(shadow::tests::params())
    }

    fn served_book_with(params: PendingParams) -> (PendingBook, ReplicaFeed, Feeds) {
        let (feed, inputs) = link(1_024);
        let publisher = Publisher::new(PendingBookHandle::new());
        let mut book = PendingBook::new(Arc::clone(&inputs.control), 1_000_000, Shadow::new(params, true, 0), publisher);
        let (mut feed, mut feeds) = (feed, Feeds { forks: inputs.forks, batches: inputs.batches });
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

    /// Batch 2c: the book served published the replica's coins; a line moving BTC's book sends its frame - the bid
    /// taken, the confirmed one beside it - then its trade; a line moving nothing sends nothing.
    #[test]
    fn a_line_publishes_its_frames_then_its_trades() {
        let (mut book, _feed, _feeds) = served_book();
        let handle = book.publisher.handle().clone();
        let frame = handle.snapshot().frame(&Coin::new("BTC")).unwrap();
        assert_eq!((frame.bbo, frame.basis_bbo, frame.basis.height), ((None, Some(ask_101())), (None, Some(ask_101())), 30));
        let mut events = handle.subscribe();
        drop(book.on_mempool(ioc(), Some(&coins()), NONE_UNREPORTED));
        let Ok(PendingEvent::Bbos(frames)) = events.try_recv() else { panic!("frames first") };
        let [moved] = frames.as_slice() else { panic!("{frames:?}") };
        assert_eq!((moved.coin.value(), moved.bbo, moved.basis_bbo, moved.pending.actions), ("BTC".to_string(), (None, None), (None, Some(ask_101())), 1));
        let Ok(PendingEvent::Trades(trades)) = events.try_recv() else { panic!("then the trades") };
        assert_eq!(trades.trades.len(), 1);
        // the store committed through the step's last event before it went out: a subscription reading it skips both
        assert_eq!(handle.snapshot().seq, trades.seq);
        assert!(moved.seq < trades.seq);
        drop(book.on_mempool(line(3_160, 3, vec![]), Some(&coins()), NONE_UNREPORTED));
        assert!(events.try_recv().is_err());
    }

    fn alo(px: &str, cloid: &str) -> Vec<ActionKind> {
        vec![ActionKind::Order(vec![requested(0, false, px, RequestedTif::Alo, Some(cloid))])]
    }

    /// Batch 2c: a coin whose best levels did not move sends no frame - an ask resting behind the best -, its store
    /// frame counting the action all the same; while the book is not served, none goes out, the store kept.
    #[test]
    fn frames_go_out_only_moved_and_served() {
        let (mut book, _feed, _feeds) = served_book();
        let handle = book.publisher.handle().clone();
        let mut events = handle.subscribe();
        drop(book.on_mempool(line(3_150, 3, alo("105", "0x0a")), Some(&coins()), NONE_UNREPORTED));
        assert!(events.try_recv().is_err(), "the best ask still 101");
        assert_eq!(btc_frame(&handle).pending.actions, 1);
        drop(book.on_mempool(MempoolInput::Loss, Some(&coins()), NONE_UNREPORTED));
        let Ok(PendingEvent::Availability(frame)) = events.try_recv() else { panic!("unavailable") };
        assert_eq!(frame.availability(), Err(Unavailable::Mempool("loss")));
        drop(book.on_mempool(line(3_160, 4, alo("100.5", "0x0b")), Some(&coins()), NONE_UNREPORTED));
        let Ok(PendingEvent::Availability(frame)) = events.try_recv() else { panic!("the fresh line ends the gap") };
        assert_eq!(frame.availability(), Err(Unavailable::WarmingUp));
        assert!(events.try_recv().is_err(), "not served: no frame");
        let latest = handle.stored_frame(&Coin::new("BTC"));
        assert_eq!(latest.bbo.1.unwrap().0, crate::order_book::Px::parse_from_str("100.5").unwrap());
    }

    /// Batch 2c: a new generation refreshes every coin - BTC's ask gone from the new fork's book, though no block of
    /// it changed BTC.
    #[test]
    fn a_new_generation_refreshes_every_coin() {
        let (mut book, mut feed, mut feeds) = served_book();
        let handle = book.publisher.handle().clone();
        listener_untrusted(&mut feed);
        book.on_tick(NONE_UNREPORTED);
        assert_eq!(handle.snapshot().availability(), Err(Unavailable::Replica));
        // the listener's next book batch: generation 2 forked from an empty book
        empty_book_batch(&mut feed);
        for height in 32..=33 {
            push_block(&mut book, &mut feed, &mut feeds, height, vec![], vec![]);
        }
        assert_eq!(book.replica.as_ref().and_then(BookReplica::confirmed), Some(32));
        assert_eq!(btc_frame(&handle).basis_bbo, (None, None));
    }

    /// Batch 2c: the generation ending goes out as an availability change - store first, then the broadcast.
    #[test]
    fn availability_changes_go_out() {
        let (mut book, mut feed, _feeds) = served_book();
        let handle = book.publisher.handle().clone();
        let mut events = handle.subscribe();
        listener_untrusted(&mut feed);
        book.on_tick(NONE_UNREPORTED);
        assert_eq!(handle.snapshot().availability(), Err(Unavailable::Replica));
        let Ok(PendingEvent::Availability(frame)) = events.try_recv() else { panic!("an availability change") };
        assert_eq!(frame.availability(), Err(Unavailable::Replica));
    }

    fn btc_frame(handle: &PendingBookHandle) -> Arc<BboFrame> {
        handle.snapshot().frame(&Coin::new("BTC")).unwrap()
    }

    /// Review 000614 r1 P1-1: an ask pending at 100 under the confirmed 101; a line past the cap drops the set. With
    /// no other change to BTC, the frame the store serves on recovery is the merged book's: 101, nothing pending.
    #[test]
    fn the_set_dropped_whole_refreshes_every_frame() {
        let (mut book, mut feed, mut feeds) = served_book_with(PendingParams::from_ms(1_000, 1_000, 10_000, 2, 60_000, 300_000, 1_000).unwrap());
        let handle = book.publisher.handle().clone();
        drop(book.on_mempool(line(3_150, 3, alo("100", "0x0a")), Some(&coins()), NONE_UNREPORTED));
        assert_eq!((btc_frame(&handle).bbo.1.unwrap().0.to_str(), btc_frame(&handle).pending.actions), ("100".to_string(), 1));
        let eth = vec![ActionKind::Order(vec![requested(1, false, "3000", RequestedTif::Alo, Some("0x0e")); 2])];
        drop(book.on_mempool(line(3_160, 4, eth), Some(&coins()), NONE_UNREPORTED));
        for height in 32..=45 {
            push_block(&mut book, &mut feed, &mut feeds, height, vec![], vec![]);
        }
        let frame = handle.snapshot().frame(&Coin::new("BTC")).unwrap();
        let view = book.shadow.coin_view(&Coin::new("BTC"), book.replica.as_ref().unwrap());
        assert_eq!((frame.bbo, frame.pending), (view.bbo, view.pending));
        assert_eq!((frame.bbo.1.unwrap().0.to_str(), frame.pending.actions), ("101".to_string(), 0));
    }

    /// Review 000614 r2 P1-1: the recovery after the set dropped whole carries the book of its commit - BTC at 101,
    /// nothing pending, at its height 42 -, and keeps it while the store moves on: the replica at 44, an ask pending at
    /// 100.5 after.
    #[test]
    fn a_recovery_carries_the_book_of_its_commit() {
        let (mut book, mut feed, mut feeds) = served_book_with(PendingParams::from_ms(1_000, 1_000, 10_000, 2, 60_000, 300_000, 1_000).unwrap());
        let handle = book.publisher.handle().clone();
        let mut events = handle.subscribe();
        drop(book.on_mempool(line(3_150, 3, alo("100", "0x0a")), Some(&coins()), NONE_UNREPORTED));
        let eth = vec![ActionKind::Order(vec![requested(1, false, "3000", RequestedTif::Alo, Some("0x0e")); 2])];
        drop(book.on_mempool(line(3_160, 4, eth), Some(&coins()), NONE_UNREPORTED));
        for height in 32..=45 {
            push_block(&mut book, &mut feed, &mut feeds, height, vec![], vec![]);
        }
        let recovery = std::iter::from_fn(|| events.try_recv().ok())
            .filter_map(|event| match event {
                PendingEvent::Availability(state) => Some(state),
                _ => None,
            })
            .last()
            .unwrap();
        assert_eq!(recovery.availability(), Ok(()));
        drop(book.on_mempool(line(4_600, 5, alo("100.5", "0x0b")), Some(&coins()), NONE_UNREPORTED));
        let told = recovery.frame(&Coin::new("BTC")).unwrap();
        assert_eq!((told.bbo.1.unwrap().0.to_str(), told.pending.actions, told.basis.height), ("101".to_string(), 0, 42));
        let stored = btc_frame(&handle);
        assert_eq!((stored.bbo.1.unwrap().0.to_str(), stored.basis.height), ("100.5".to_string(), 44));
    }

    fn ask_101() -> overlay::BestLevel {
        (crate::order_book::Px::parse_from_str("101").unwrap(), crate::order_book::Sz::parse_from_str("1").unwrap(), 1)
    }

    /// Review 000611 r3 P2-2: a batch whose block both streams have not passed yet only queues - no settling; the one
    /// moving the confirmed height settles.
    #[test]
    fn a_batch_that_only_queues_settles_nothing() {
        let (mut book, mut feed, mut feeds) = served_book();
        let settles = book.shadow.settles();
        let block = LoggedBlock { height: 32, time_ms: 3_200, hashes: Vec::new(), priorities: Vec::new(), taker_outcomes: Vec::new(), twap_cancels: Vec::new(), read_at: Instant::now() };
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
        assert_eq!(book.shadow.availability(None), Err(Unavailable::Replica));
    }

    /// Review 000611 r1 P1-2: a loss a watcher found, its report still queued, is a gap before the next input - a
    /// line or a tick; the mempool's or the block log's.
    #[test]
    fn a_loss_found_before_its_report_is_a_gap_at_once() {
        let mempool_lost = Unreported { mempool: true, block_log: false, twaps: false };
        let (mut book, _feed, _feeds) = served_book();
        assert_eq!(book.on_mempool(ioc(), Some(&coins()), mempool_lost).trades, shadow::tests::NO_TRADES);
        assert_eq!(book.shadow.availability(book.replica.as_ref()), Err(Unavailable::Mempool("loss")));
        let (mut book, _feed, _feeds) = served_book();
        book.on_tick(mempool_lost);
        assert_eq!(book.shadow.availability(book.replica.as_ref()), Err(Unavailable::Mempool("loss")));
        let (mut book, _feed, _feeds) = served_book();
        let block_log_lost = Unreported { mempool: false, block_log: true, twaps: false };
        assert_eq!(book.on_mempool(ioc(), Some(&coins()), block_log_lost).trades, shadow::tests::NO_TRADES);
        assert_eq!(book.shadow.availability(book.replica.as_ref()), Err(Unavailable::BlockLog("block_log_loss")));
    }

    const TWAPS_LOST: Unreported = Unreported { mempool: false, block_log: false, twaps: true };

    /// The served book with the TWAPs known: a TWAP's suborder due next taking 0.1 of BTC's ask at 101.
    fn served_book_with_a_twap() -> (PendingBook, ReplicaFeed, Feeds) {
        let (mut book, feed, feeds) = served_book();
        book.on_anchor(shadow::tests::twap_anchor(29), NONE_UNREPORTED);
        let at = Instant::now() + Duration::from_millis(1);
        book.on_twap(TwapInput::ReadThrough(TwapStream::Statuses, at), NONE_UNREPORTED);
        book.on_twap(TwapInput::ReadThrough(TwapStream::Fills, at), NONE_UNREPORTED);
        let handle = book.publisher.handle();
        let tenth_taken = (ask_101().0, crate::order_book::Sz::parse_from_str("0.9").unwrap(), 1);
        assert!(handle.snapshot().twaps_known());
        assert_eq!(btc_frame(handle).bbo.1, Some(tenth_taken));
        (book, feed, feeds)
    }

    /// Review 000624 r5 P1: a TWAP loss the watcher found, its report still queued, is taken at the next input - a
    /// tick -: the suborder it withdrew goes out in the commit telling the TWAPs unknown, BTC's ask back at 101 x 1.
    #[test]
    fn a_twap_loss_found_withdraws_the_suborder_in_its_commit() {
        let (mut book, _feed, _feeds) = served_book_with_a_twap();
        let handle = book.publisher.handle().clone();
        let mut events = handle.subscribe();
        book.on_tick(TWAPS_LOST);
        let told = std::iter::from_fn(|| events.try_recv().ok())
            .find_map(|event| match event {
                PendingEvent::Availability(state) => Some(state),
                _ => None,
            })
            .unwrap();
        assert!(!told.twaps_known());
        assert_eq!(told.frame(&Coin::new("BTC")).unwrap().bbo.1, Some(ask_101()));
    }

    /// Review 000624 r5 P1: a new anchor finds the loss the watcher found first - not one taken after it.
    #[test]
    fn an_anchor_takes_a_loss_found_first() {
        let (mut book, _feed, _feeds) = served_book_with_a_twap();
        book.on_anchor(shadow::tests::twap_anchor(30), TWAPS_LOST);
        assert!(!book.shadow.twaps_known());
        assert_eq!(btc_frame(book.publisher.handle()).bbo.1, Some(ask_101()));
    }

    fn book_and_feed(queue_batches: usize, backlog_events: usize) -> (PendingBook, ReplicaFeed, Feeds) {
        let (feed, inputs) = link(queue_batches);
        let publisher = Publisher::new(PendingBookHandle::new());
        let book = PendingBook::new(Arc::clone(&inputs.control), backlog_events, Shadow::new(shadow::tests::params(), true, 0), publisher);
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
