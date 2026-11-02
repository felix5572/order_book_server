//! The pending book (--pending-book, hyperliquid-trade design 000604): the listener's book as the actions in the
//! node's mempool will leave it. Batch 1 is the replica of the listener's book, on its own thread; batch 2a reads the
//! mempool (`mempool`, `feed`); batch 2b holds its lines' book actions (`actions`) in the pending set and overlays them
//! on the replica (`shadow`, `overlay`), settling them by the block log's bundle hashes (`blocks`, `record`; r6, r7);
//! batch 2c publishes the frames of the `extPendingBbo` / `extPendingTrades` channels (`publish`).

pub(crate) mod actions;
mod blocks;
mod feed;
mod mark_forecast;
pub(crate) mod mempool;
pub(crate) mod overlay;
pub(crate) mod publish;
mod record;
mod refusals;
mod signer;
mod signer_accounts;
mod trailing;
pub(crate) mod triggers;
mod twap_streams;
mod twaps;
pub(crate) mod shadow;

use std::{
    io,
    sync::Arc,
    time::{Duration, Instant},
};

use log::{error, info};
use rustc_hash::FxHashMap;
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
    mark_forecast::{ForecastBook, ServedBook, SgaForecasts},
    trailing::TrailingBasis,
    triggers::SetGlobalCoin,
    mempool::{ActionKind, MempoolIngest, MempoolInput, MempoolTx},
    twap_streams::{TwapFillLine, TwapStatusLine},
    twaps::TwapStream,
    publish::Publisher,
    shadow::{Shadow, Step, unix_ns, wall_ms},
};
use crate::{
    accounts::{
        book::{ActionCoins, PendingAnchor},
        flow_accounts::{FlowAccounts, FlowAccountsEvent},
        owner_thread::spawn_owner,
        publish::NodePrices,
    },
    listeners::order_book::replica::{BookReplica, ReplicaBatch, ReplicaControl, ReplicaFork, ReplicaInputs},
    metrics::{
        PENDING_BOOK_ALIGNMENT_BLOCKS, PENDING_BOOK_BACKLOG_EVENTS, PENDING_BOOK_BREAKS_TOTAL, PENDING_BOOK_CLOID_INDEX_ORDERS,
        PENDING_BOOK_CONFIRMED_HEIGHT, PENDING_BOOK_STEP_SECONDS, PENDING_BOOK_TRIGGER_COINS_TOTAL, PENDING_BOOK_UNAVAILABLE_SECONDS_TOTAL,
        PENDING_BOOK_UNPROVEN_LEVELS,
    },
    order_book::{Coin, Px},
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
    /// The account view's node prices after every applied block: what the `SetGlobalAction` forecasts read and are
    /// checked against (batch 2l).
    pub(crate) prices: watch::Receiver<NodePrices>,
    /// The thread's copy of the account view and the view's events to it: the positions a `SetGlobalAction` freezes
    /// (batch 2m, design 000652 r2).
    pub(crate) accounts: FlowAccounts,
    pub(crate) account_events: mpsc::Receiver<FlowAccountsEvent>,
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
    let PendingFeeds { mempool: Feed { events: mut mempool, losses: mempool_losses }, block_log, twap_statuses, twap_fills, anchor, mut prices, accounts, mut account_events } =
        feeds;
    let mut book = PendingBook::new(control, backlog_events, shadow, publisher, accounts);
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
    let mut prices_open = book.take_prices(&mut prices, true);
    let mut account_events_open = true;
    let mut account_validity_open = true;
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
            changed = prices.changed(), if prices_open => prices_open = book.take_prices(&mut prices, changed.is_ok()),
            // the copy's validity is the owner's watch: once the owner is gone it serves nothing
            event = account_events.recv(), if account_events_open => {
                let Some(event) = event else {
                    error!("pending book: the account view's events closed; no SetGlobalAction freezes positions");
                    account_events_open = false;
                    continue;
                };
                book.accounts.apply(event);
                book.on_accounts(readings.unreported());
            }
            // the owner's watch withdrawing the copy, or serving it again, with no event (review 000662 r1 P2-2)
            changed = book.accounts.validity_changed(), if account_validity_open => {
                if changed.is_err() {
                    error!("pending book: the account view's owner ended; no trailing stop is known");
                    account_validity_open = false;
                }
                book.on_accounts(readings.unreported());
            }
            _ = tick.tick() => book.on_tick(readings.unreported()),
        }
    };
    error!("pending book: {closed} closed; the pending thread ends");
}

/// The main-dex coins a `SetGlobalAction` line prices, in asset order: by the asset table - each it names, priced or in
/// part -, else those its forecast named (review 000662 r1 P2-5: a coin's place is kept whatever its forecast).
fn set_global_coins(tx: &MempoolTx, named: Option<&ActionCoins>, forecast: &FxHashMap<u32, (String, Result<Px, &'static str>)>) -> Vec<(u32, String)> {
    let submitted = tx.actions.iter().find_map(|action| match &action.kind {
        ActionKind::SetGlobal(submitted) => Some(submitted),
        _ => None,
    });
    let priced = submitted.into_iter().flatten().enumerate().filter(|(_, prices)| prices.priced() != Some(false));
    let by_table = priced.filter_map(|(asset, _)| {
        let asset = u32::try_from(asset).ok()?;
        Some((asset, named?.coins.get(&asset)?.coin.clone()))
    });
    let mut coins: Vec<(u32, String)> = by_table.collect();
    for (asset, (name, _)) in forecast {
        if !coins.iter().any(|(held, _)| held == asset) {
            coins.push((*asset, name.clone()));
        }
    }
    coins.sort_unstable();
    coins
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
    /// The `SetGlobalAction` forecasts (batch 2l): the marks its trigger orders fire on.
    forecasts: SgaForecasts,
    /// The thread's copy of the account view (batch 2m).
    accounts: FlowAccounts,
    /// The copy's (generation, height) the shadow's trailing stops were last read at; `None` with none serving.
    trailing_read: Option<(u64, u64)>,
}

impl PendingBook {
    fn new(control: Arc<ReplicaControl>, backlog_events: usize, shadow: Shadow, publisher: Publisher, accounts: FlowAccounts) -> Self {
        let ended = control.ended();
        control.request(1);
        Self { control, wanted: 1, replica: None, ended, backlog_events, shadow, publisher, forecasts: SgaForecasts::default(), accounts, trailing_read: None }
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
        self.publish(&mut step);
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
        // its own loss taken with those unreported, before any trailing stop is read anew (review 000662 r4 P2-1)
        let mut step = self.sync(Unreported { mempool: unreported.mempool || matches!(input, MempoolInput::Loss), ..unreported });
        step.absorb(match input {
            MempoolInput::Tx { tx, after_loss } => {
                let started = Instant::now();
                // a loss found since the line was taken is not before it in the file, so the line ends no gap
                let mut step = self.shadow.on_tx(&tx, after_loss || unreported.mempool, coins, self.replica.as_ref());
                PENDING_BOOK_STEP_SECONDS.with_label_values(&["transaction"]).observe(started.elapsed().as_secs_f64());
                // on the book the line leaves - a `SetGlobalAction`'s holds no book action
                let forecast = {
                    let served = ServedBook::of(&self.shadow, self.replica.as_ref());
                    let book: Option<&dyn ForecastBook> = served.as_ref().map(|served| -> &dyn ForecastBook { served });
                    self.forecasts.on_tx(&tx, coins, book, unix_ns(wall_ms()))
                };
                // every `SetGlobalAction`: a coin not decided where it comes keeps its place forecast or not (review
                // 000662 r1 P2-5)
                if tx.actions.iter().any(|action| matches!(action.kind, ActionKind::SetGlobal(_))) {
                    step.absorb(self.set_global(&tx, coins));
                }
                drop(forecast);
                step
            }
            MempoolInput::Loss => Step::default(),
        });
        self.publish(&mut step);
        step
    }

    /// The block log's next input: the coins moved by the settling it brings.
    fn on_block(&mut self, input: BlockInput, unreported: Unreported) -> Step {
        let mut step = self.sync(Unreported { block_log: unreported.block_log || matches!(input, BlockInput::Loss), ..unreported });
        step.absorb(match input {
            BlockInput::Block { block, after_loss } => {
                let _report = self.forecasts.on_block(&block);
                let started = Instant::now();
                // a loss found since the block was taken is not before it in the file, so the block ends no gap
                let step = self.shadow.on_block(block, after_loss || unreported.block_log, self.replica.as_ref());
                PENDING_BOOK_STEP_SECONDS.with_label_values(&["block_log"]).observe(started.elapsed().as_secs_f64());
                step
            }
            BlockInput::Loss => Step::default(),
        });
        self.publish(&mut step);
        step
    }

    /// A TWAP stream's input (batch 2h).
    fn on_twap(&mut self, input: TwapInput, unreported: Unreported) {
        let mut step = self.sync(Unreported { twaps: unreported.twaps || matches!(input, TwapInput::Loss), ..unreported });
        let replica = self.replica.as_ref();
        step.absorb(match input {
            TwapInput::Line(TwapLine::Statuses(line)) => self.shadow.on_twap_statuses(line.height, line.events, replica),
            TwapInput::Line(TwapLine::Fills(line)) => self.shadow.on_twap_fills(line.height, line.fills, replica),
            TwapInput::ReadThrough(stream, at) => self.shadow.on_twap_read_through(stream, at, replica),
            TwapInput::Loss => Step::default(),
        });
        self.publish(&mut step);
    }

    /// The account view's new anchor: its running TWAPs.
    fn on_anchor(&mut self, anchor: Arc<PendingAnchor>, unreported: Unreported) {
        let mut step = self.sync(unreported);
        step.absorb(self.shadow.on_anchor(anchor, self.replica.as_ref()));
        self.publish(&mut step);
    }

    /// An account copy event applied, or its validity turned: its trailing stops as they now serve (batch 2n-2), the coins they move replayed.
    fn on_accounts(&mut self, unreported: Unreported) {
        let mut step = self.sync(unreported);
        step.absorb(self.take_trailing());
        self.publish(&mut step);
    }

    fn take_trailing(&mut self) -> Step {
        self.trailing_read = self.accounts.serving_at();
        let basis = TrailingBasis::of(self.accounts.copy().ok());
        self.shadow.on_trailing(basis, self.replica.as_ref())
    }

    /// A `SetGlobalAction` line held where it stands, a coin at a time (batch 2m, design 000652 r2): its mark the one its
    /// forecast gave the merged book there, frozen with the positions of the accounts of the untriggered orders it
    /// reaches then; a coin of no mark counted.
    fn set_global(&mut self, tx: &MempoolTx, named: Option<&ActionCoins>) -> Step {
        let started = Instant::now();
        let replica = self.replica.as_ref();
        let copy = self.accounts.copy().ok();
        let mut forecast: FxHashMap<u32, (String, Result<Px, &'static str>)> =
            self.forecasts.merged_marks(&tx.tx_hash).into_iter().map(|(asset, name, mark_px)| (asset, (name, mark_px))).collect();
        let mut coins = Vec::new();
        for (asset, name) in set_global_coins(tx, named, &forecast) {
            let coin = Coin::new(&name);
            // the coin's run deciding nothing where the line comes: held as its place, nothing frozen (design 000659 r4)
            if let Some(reason) = self.shadow.undecided_at(&coin, replica, tx.seen_ns) {
                coins.push((coin, asset, SetGlobalCoin::undecided(reason)));
                continue;
            }
            // a coin of no forecast is not held, as before; one whose forecast failed counted
            let Some((_, mark_px)) = forecast.remove(&asset) else { continue };
            let mark_px = match mark_px {
                Ok(mark_px) => mark_px,
                Err(reason) => {
                    PENDING_BOOK_TRIGGER_COINS_TOTAL.with_label_values(&[reason]).inc();
                    continue;
                }
            };
            let accounts = self.shadow.reached_accounts(&coin, replica, mark_px, tx.seen_ns);
            coins.push((coin, asset, SetGlobalCoin::frozen(&name, mark_px, &accounts, copy)));
        }
        let step = self.shadow.on_set_global(tx, coins, replica);
        PENDING_BOOK_STEP_SECONDS.with_label_values(&["sga_triggers"]).observe(started.elapsed().as_secs_f64());
        step
    }

    /// The account view's node prices as the watch now has them (batch 2l), `open` while it has a sender: once closed,
    /// the prices it last gave are withdrawn (review 000639 r2 P2) and it is read no more. Whether it stays open.
    fn take_prices(&mut self, prices: &mut watch::Receiver<NodePrices>, open: bool) -> bool {
        if !open {
            error!("pending book: the account view's node prices closed; no SetGlobalAction is forecast");
            self.on_prices(&Err("the account view's node prices closed".to_string()));
            return false;
        }
        let current = prices.borrow_and_update().clone();
        self.on_prices(&current);
        true
    }

    fn on_prices(&mut self, prices: &NodePrices) {
        let _report = self.forecasts.on_prices(prices, unix_ns(wall_ms()));
    }

    fn takes_blocks(&self) -> bool {
        self.shadow.takes_blocks(self.replica.as_ref())
    }

    fn on_tick(&mut self, unreported: Unreported) {
        let mut step = self.sync(unreported);
        // the copy's validity may have turned with no event: its trailing stops read again
        step.absorb(self.take_trailing());
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
        self.publish(&mut step);
    }

    /// `step`'s frames, the availability with them; every coin's when the overlays were dropped or rebuilt whole and a
    /// replica is there to frame them on.
    fn publish(&mut self, step: &mut Step) {
        // the copy turned while the step was computed (review 000662 r2 P2-3): its trailing stops read again, the coins
        // they move replayed, and of those coins' trades only what the new version yields alike told (r3 P2-1)
        if self.accounts.serving_at() != self.trailing_read {
            let read = self.take_trailing();
            if let Some(replica) = &self.replica {
                self.shadow.retain_yielded(&mut step.trades, &read.changed, replica);
            }
            step.absorb(read);
        }
        // the suborders the step's trailing reads held back, as its final runs fill them (review 000662 r5 P2), the
        // frame in the order its runs take them (r6 P2)
        self.shadow.tell_held_back(&mut step.trades, self.replica.as_ref());
        self.shadow.order_trades(&mut step.trades);
        let aligned = self.replica.as_ref().is_some_and(|replica| replica.confirmed().is_some());
        let refresh_all = aligned && self.shadow.take_refresh_all();
        let told = self.publisher.publish(step, refresh_all, &self.shadow, self.replica.as_ref());
        // the step's suborder slots told only as its trades went out (review 000662 r4 P2-2)
        self.shadow.on_committed(if told { &step.trades } else { &[] });
    }

    /// What the listener and the feeds' watchers already flagged, taken before an input is handled (review 000611 r1
    /// P1-2): a generation the listener ended takes the replica with it, and a loss a watcher found - its report still
    /// queued behind the lines it may precede, or the input itself (review 000662 r4 P2-1) - is a gap at once. What it changed, published with the input's step
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
        // before any input reads them, the losses known taken first (review 000662 r2 P2-4): the trailing stops as the
        // copy now serves (r1 P2-2)
        if self.accounts.serving_at() != self.trailing_read {
            step.absorb(self.take_trailing());
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
    use crate::{order_book::Coin, pending::{shadow::{Taker, Unavailable}, triggers::TriggerSource}};
    use crate::{
        listeners::order_book::replica::{
            ReplicaFeed, link,
            tests::{block_lines, btc_diff, btc_status, btc_stop_status, empty_book_batch, empty_line, listener_applied, listener_untrusted, new_diff},
        },
        pending::{
            actions::tests::{coins, requested},
            mempool::{ActionKind, MempoolTx, RequestedTif, SignedAction},
        },
    };

    const NONE_UNREPORTED: Unreported = Unreported { mempool: false, block_log: false, twaps: false };

    /// The thread's account copy, its view never sent: no positions known.
    fn no_accounts() -> FlowAccounts {
        crate::accounts::flow_accounts::link(crate::accounts::flow_accounts::CopyOwner::PendingBook).1
    }

    /// Block `height`: its hashes (none) through the block log first, as it leads, then its lines through the
    /// listener's feed, each taken by the pending book.
    fn push_block(book: &mut PendingBook, feed: &mut ReplicaFeed, feeds: &mut Feeds, height: u64, statuses: Vec<serde_json::Value>, diffs: Vec<serde_json::Value>) {
        let block = LoggedBlock { height, time_ms: height * 100, hashes: Vec::new(), priorities: Vec::new(), taker_outcomes: Vec::new(), signer_accounts: Vec::new(), twap_cancels: Vec::new(), set_global: false, read_at: Instant::now() };
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
        served_book_with_accounts(params, no_accounts())
    }

    fn served_book_with_accounts(params: PendingParams, accounts: FlowAccounts) -> (PendingBook, ReplicaFeed, Feeds) {
        let (feed, inputs) = link(1_024);
        let publisher = Publisher::new(PendingBookHandle::new());
        let mut book = PendingBook::new(Arc::clone(&inputs.control), 1_000_000, Shadow::new(params, true, 0), publisher, accounts);
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

    /// Batch 2l, review 000639 r2 P2: the node prices' watch closing withdraws the prices it last gave - a
    /// `SetGlobalAction` then forecasts nothing - while the book is still served.
    #[test]
    fn closed_node_prices_withdraw_the_last_ones() {
        let (mut book, _feed, _feeds) = served_book();
        let btc = crate::local_marks::compute::tests::btc_state(0.0, 1_000_000_000);
        let snapshot = crate::accounts::marks::NativePriceSnapshot {
            seed_height: 1,
            height: 30,
            block_time_ms: 3_000,
            coins: FxHashMap::from_iter([("BTC".to_string(), btc)]),
            hip3: FxHashMap::default(),
        };
        let (sender, mut prices) = watch::channel(Ok(Arc::new(snapshot)));
        assert!(book.take_prices(&mut prices, true));
        let set_global = |hash: u8| {
            let btc = crate::accounts::marks::actions::SubmittedPrices::Priced { oracle: "101".to_string(), mark: "101".to_string() };
            let MempoolInput::Tx { tx, .. } = line(3_150, hash, vec![ActionKind::SetGlobal(vec![btc])]) else { unreachable!() };
            tx
        };
        let forecast = |book: &mut PendingBook, hash: u8| {
            let served = ServedBook::of(&book.shadow, book.replica.as_ref());
            let served: Option<&dyn ForecastBook> = served.as_ref().map(|served| -> &dyn ForecastBook { served });
            book.forecasts.on_tx(&set_global(hash), Some(&coins()), served, 3_150_000_000).lines
        };
        assert_eq!(forecast(&mut book, 1), ["forecast"]);
        drop(sender);
        let open = prices.has_changed().is_ok();
        assert!(!book.take_prices(&mut prices, open), "closed");
        assert_eq!(forecast(&mut book, 2), ["no_prices"]);
        assert_eq!(book.on_mempool(ioc(), Some(&coins()), NONE_UNREPORTED).trades.len(), 1, "still served");
    }

    /// On a book served - ask 101 -, the node prices of BTC as of block 30 (last trade 100), a pending ALO bid of 1 at 99,
    /// then a `SetGlobalAction` whose mark comes to 100 (submitted 100, L median(99, 101, 100)): the action's step, and the
    /// prices' sender to keep them.
    fn bid_then_set_global(book: &mut PendingBook) -> (watch::Sender<NodePrices>, Step) {
        bid_then_set_global_at(book, 3_250)
    }

    /// `bid_then_set_global`, the action's line seen at `seen_ms`.
    fn bid_then_set_global_at(book: &mut PendingBook, seen_ms: i64) -> (watch::Sender<NodePrices>, Step) {
        let btc = crate::accounts::marks::native::CoinPriceState { last_trade: 1_000, ..crate::local_marks::compute::tests::btc_state(0.0, 1_000_000_000) };
        let snapshot = crate::accounts::marks::NativePriceSnapshot {
            seed_height: 1,
            height: 30,
            block_time_ms: 3_000,
            coins: FxHashMap::from_iter([("BTC".to_string(), btc)]),
            hip3: FxHashMap::default(),
        };
        let (sender, mut prices) = watch::channel(Ok(Arc::new(snapshot)));
        assert!(book.take_prices(&mut prices, true));
        let bid = ActionKind::Order(vec![requested(0, true, "99", RequestedTif::Alo, Some("0x08"))]);
        drop(book.on_mempool(line(3_210, 8, vec![bid]), Some(&coins()), NONE_UNREPORTED));
        let submitted = crate::accounts::marks::actions::SubmittedPrices::Priced { oracle: "100".to_string(), mark: "100".to_string() };
        (sender, book.on_mempool(line(seen_ms, 1, vec![ActionKind::SetGlobal(vec![submitted])]), Some(&coins()), NONE_UNREPORTED))
    }

    /// Batch 2m: on a book served - bid 99 (a pending ALO), ask 101 - with the replica's market stop selling a fall to
    /// 100, a `SetGlobalAction` whose mark comes to 100 (submitted 100, L median(99, 101, last trade 100)) fires it: it
    /// sells 1 into the bid, its taker the stop. A position stop, the thread's account copy holding no view, runs none.
    #[test]
    fn a_set_global_action_fires_the_trigger_orders_its_mark_reaches() {
        for position in [false, true] {
            let (mut book, mut feed, mut feeds) = served_book();
            // the stop's block confirmed once the next one's lines come
            push_block(&mut book, &mut feed, &mut feeds, 32, vec![btc_stop_status(77, "A", None, position, "open", None, 3_200)], vec![]);
            push_block(&mut book, &mut feed, &mut feeds, 33, vec![], vec![]);
            let (_prices, step) = bid_then_set_global(&mut book);
            let fired_trades: Vec<_> = step.trades.iter().map(|trade| (trade.taker.clone(), trade.px, trade.sz)).collect();
            let expected = (!position).then(|| {
                let source = TriggerSource::Confirmed { oid: 77, user: shadow::tests::ACCOUNT, cloid: None };
                let set_global = actions::ActionId { tx_hash: alloy::primitives::B256::repeat_byte(1), action: 0, item: 0 };
                (Taker::Trigger { source, set_global, seen_ns: 3_250_000_000 }, Px::parse_from_str("99").unwrap(), crate::order_book::Sz::parse_from_str("1").unwrap())
            });
            assert_eq!(fired_trades, expected.into_iter().collect::<Vec<_>>(), "position {position}");
        }
    }

    /// Batch 2m (design 000652 r2): the action freezes, as its line comes, the positions of the accounts of the orders it
    /// reaches from the thread's account copy - `ACCOUNT` long 1 -: its position stop sells that 1 into the bid.
    #[test]
    fn a_set_global_action_freezes_the_positions_its_orders_need() {
        let snapshot = crate::accounts::flow_accounts::AccountsSnapshot {
            generation: 1,
            height: 30,
            time_ms: 3_000,
            coins: FxHashMap::from_iter([("BTC".to_string(), 5)]),
            marks: FxHashMap::default(),
            positions: FxHashMap::from_iter([("BTC".to_string(), FxHashMap::from_iter([(shadow::tests::ACCOUNT, 100_000)]))]),
            candidates: Err("none".to_string()),
            trailing: None,
        };
        let (accounts, _validity) = crate::accounts::flow_accounts::tests::copy_of(snapshot);
        let (mut book, mut feed, mut feeds) = served_book_with_accounts(shadow::tests::params(), accounts);
        push_block(&mut book, &mut feed, &mut feeds, 32, vec![btc_stop_status(77, "A", None, true, "open", None, 3_200)], vec![]);
        push_block(&mut book, &mut feed, &mut feeds, 33, vec![], vec![]);
        let (_prices, step) = bid_then_set_global(&mut book);
        let sold: Vec<_> = step.trades.iter().map(|trade| (trade.px, trade.sz)).collect();
        assert_eq!(sold, [(Px::parse_from_str("99").unwrap(), crate::order_book::Sz::parse_from_str("1").unwrap())]);
    }

    /// The IOC crossing the resting ask predicts its trade on a book served.
    #[test]
    fn a_served_book_predicts_a_taker() {
        let (mut book, _feed, _feeds) = served_book();
        assert_eq!(book.on_mempool(ioc(), Some(&coins()), NONE_UNREPORTED).trades.len(), 1);
    }

    /// Batch 2n-2 (design 000659 r3 P2-3, r4): a `SetGlobalAction` that may fire a trailing stop not known - the
    /// thread's account copy serving none - leaves BTC not decided: its frame says so, to the subscribers and in the
    /// store; a second one coming meanwhile is held as its place, nothing frozen. The first's block confirmed, BTC is
    /// still not decided; the second's too, it is decided again with its best levels as they were - the pending bid
    /// still held -: that goes out too, and a new subscriber reads it.
    #[test]
    fn a_coin_undecided_then_decided_again_goes_out_both_times() {
        let (mut book, mut feed, mut feeds) = served_book();
        let mut trailing = btc_stop_status(70, "A", None, false, "open", None, 3_200);
        trailing["order"]["orderType"] = serde_json::json!("Trailing Stop Market");
        trailing["order"]["triggerCondition"] = serde_json::json!("Activation immediate, retracement 5, best 120");
        push_block(&mut book, &mut feed, &mut feeds, 32, vec![trailing], vec![]);
        push_block(&mut book, &mut feed, &mut feeds, 33, vec![], vec![]);
        let handle = book.publisher.handle().clone();
        let btc = Coin::new("BTC");
        let mut events = handle.subscribe();
        let (_prices, step) = bid_then_set_global(&mut book);
        assert_eq!(step.trades.len(), 0);
        let undecided = handle.snapshot().frame(&btc).unwrap();
        assert_eq!(undecided.undecided, Some("trailing_unknown"));
        // a second action coming meanwhile: held as its place, nothing frozen (design 000659 r4)
        let submitted = crate::accounts::marks::actions::SubmittedPrices::Priced { oracle: "100".to_string(), mark: "100".to_string() };
        drop(book.on_mempool(line(3_280, 2, vec![ActionKind::SetGlobal(vec![submitted])]), Some(&coins()), NONE_UNREPORTED));
        let held: Vec<Option<&str>> = book.shadow.held_set_globals(&btc).iter().map(|sga| sga.undecided_input).collect();
        assert_eq!(held, [None, Some("trailing_unknown")]);
        let told_btc = |events: &mut tokio::sync::broadcast::Receiver<PendingEvent>| -> Vec<Option<&'static str>> {
            std::iter::from_fn(|| events.try_recv().ok())
                .filter_map(|event| match event {
                    PendingEvent::Bbos(frames) => frames.iter().find(|frame| frame.coin == btc).map(|frame| frame.undecided),
                    _ => None,
                })
                .collect()
        };
        assert_eq!(told_btc(&mut events).last(), Some(&Some("trailing_unknown")));
        // each action's block: its hash through the block log, then its lines, confirmed once the next block's lines come
        let mut confirm_line = |book: &mut PendingBook, height: u64, hash: u8| {
            let block = LoggedBlock {
                height, time_ms: height * 100, hashes: vec![alloy::primitives::B256::repeat_byte(hash)], priorities: Vec::new(), taker_outcomes: Vec::new(),
                signer_accounts: Vec::new(), twap_cancels: Vec::new(), set_global: true, read_at: Instant::now(),
            };
            drop(book.on_block(BlockInput::Block { block, after_loss: false }, NONE_UNREPORTED));
            for batch in block_lines(height, vec![], vec![]) {
                listener_applied(&mut feed, batch);
                book.on_batch(feeds.batches.try_recv().unwrap(), &mut feeds.forks, NONE_UNREPORTED);
            }
        };
        confirm_line(&mut book, 34, 1);
        confirm_line(&mut book, 35, 2);
        assert_eq!(handle.snapshot().frame(&btc).unwrap().undecided, Some("trailing_unknown"), "the first confirmed, the second still held");
        drop(told_btc(&mut events));
        push_block(&mut book, &mut feed, &mut feeds, 36, vec![], vec![]);
        let decided = handle.snapshot().frame(&btc).unwrap();
        assert_eq!((decided.undecided, decided.bbo), (None, undecided.bbo), "the bid still pending: the levels as they were");
        let told = told_btc(&mut events);
        assert_eq!(told.last(), Some(&None), "decided again, told: {told:?}");
    }

    /// BTC's trailing stop market sale of 1 at 3,200 ms, `condition`.
    fn trailing_open(oid: u64, condition: &str) -> serde_json::Value {
        let mut status = btc_stop_status(oid, "A", None, false, "open", None, 3_200);
        status["order"]["orderType"] = serde_json::json!("Trailing Stop Market");
        status["order"]["triggerCondition"] = serde_json::json!(condition);
        status
    }

    /// Review 000662 r1 P2-5: the node prices withdrawn after a first `SetGlobalAction` left BTC not decided, a second
    /// one - forecast none - is held all the same as BTC's place; the first's block confirmed, BTC stays not decided.
    #[test]
    fn a_set_global_forecast_none_keeps_an_undecided_coins_place() {
        let (mut book, mut feed, mut feeds) = served_book();
        push_block(&mut book, &mut feed, &mut feeds, 32, vec![trailing_open(70, "Activation immediate, retracement 5, best 120")], vec![]);
        push_block(&mut book, &mut feed, &mut feeds, 33, vec![], vec![]);
        let (_prices, _step) = bid_then_set_global(&mut book);
        let btc = Coin::new("BTC");
        book.on_prices(&Err("withdrawn".to_string()));
        let submitted = crate::accounts::marks::actions::SubmittedPrices::Priced { oracle: "100".to_string(), mark: "100".to_string() };
        drop(book.on_mempool(line(3_280, 2, vec![ActionKind::SetGlobal(vec![submitted])]), Some(&coins()), NONE_UNREPORTED));
        let held: Vec<Option<&str>> = book.shadow.held_set_globals(&btc).iter().map(|sga| sga.undecided_input).collect();
        assert_eq!(held, [None, Some("trailing_unknown")]);
        let block = LoggedBlock {
            height: 34, time_ms: 3_400, hashes: vec![alloy::primitives::B256::repeat_byte(1)], priorities: Vec::new(), taker_outcomes: Vec::new(),
            signer_accounts: Vec::new(), twap_cancels: Vec::new(), set_global: true, read_at: Instant::now(),
        };
        drop(book.on_block(BlockInput::Block { block, after_loss: false }, NONE_UNREPORTED));
        for batch in block_lines(34, vec![], vec![]) {
            listener_applied(&mut feed, batch);
            book.on_batch(feeds.batches.try_recv().unwrap(), &mut feeds.forks, NONE_UNREPORTED);
        }
        push_block(&mut book, &mut feed, &mut feeds, 35, vec![], vec![]);
        assert_eq!(book.publisher.handle().snapshot().frame(&btc).unwrap().undecided, Some("trailing_unknown"), "the second still held");
    }

    /// Review 000662 r1 P2-2: the owner's watch withdrawing the copy with no event, the next input reads the trailing
    /// stops anew before it runs: BTC's stop, known (best 100, trigger price 95: a `SetGlobalAction` at 100 runs it
    /// none), is not known any more, and BTC is not decided.
    #[test]
    fn a_copy_withdrawn_with_no_event_is_read_before_the_next_input() {
        let (accounts, validity) = trailing_accounts();
        let (mut book, mut feed, mut feeds) = served_book_with_accounts(shadow::tests::params(), accounts);
        push_block(&mut book, &mut feed, &mut feeds, 32, vec![trailing_open(70, "Activation immediate, retracement 5, best 100")], vec![]);
        push_block(&mut book, &mut feed, &mut feeds, 33, vec![], vec![]);
        let (_prices, _step) = bid_then_set_global(&mut book);
        let btc = Coin::new("BTC");
        let handle = book.publisher.handle().clone();
        assert_eq!(handle.snapshot().frame(&btc).unwrap().undecided, None, "the stop known and not reached");
        validity.send_replace(crate::accounts::flow_accounts::FlowAccountsValidity { generation: 1, state: Err("withdrawn".to_string()) });
        drop(book.on_mempool(line(3_260, 3, vec![]), Some(&coins()), NONE_UNREPORTED));
        assert_eq!(handle.snapshot().frame(&btc).unwrap().undecided, Some("trailing_unknown"));
    }

    /// Batch 2n-3: the thread's account copy one block ahead of the replica - its snapshot at 30, blocks 31 to 34
    /// applied; the replica confirmed through 33, block 34 logged with no `SetGlobalAction` -: stop 70 known (best 100,
    /// trigger price 95), a `SetGlobalAction` at 100 reaches none, BTC decided. Review 000666 r1 P2-1: block 34 setting
    /// stop 70 anew (best 108, trigger price 103) - its state at 33 not held -, BTC is not decided and no trade told of it.
    /// Review 000666 r2: blocks 31 to 34 come as one, stop 70 alike at both ends - what the blocks between set not seen -:
    /// BTC not decided either.
    #[test]
    fn a_copy_one_block_ahead_of_the_replica_decides_its_coins() {
        use crate::accounts::{flow_accounts::TrailingChanges, marks::trailing::{Retracement, TrailingState, TrailingStop}};
        let undecided_with = |blocks: Vec<(u64, Option<TrailingChanges>)>| {
            let (accounts, _owner) = trailing_accounts();
            let (mut book, mut feed, mut feeds) = served_book_with_accounts(shadow::tests::params(), accounts);
            push_block(&mut book, &mut feed, &mut feeds, 32, vec![trailing_open(70, "Activation immediate, retracement 5, best 100")], vec![]);
            push_block(&mut book, &mut feed, &mut feeds, 33, vec![], vec![]);
            push_block(&mut book, &mut feed, &mut feeds, 34, vec![], vec![]);
            for (height, trailing) in blocks {
                book.accounts.apply(FlowAccountsEvent::Block { generation: 1, height, time_ms: height * 100, marks: Vec::new(), positions: Vec::new(), trailing });
            }
            assert_eq!((book.replica.as_ref().and_then(BookReplica::confirmed), book.accounts.serving_at()), (Some(33), Some((1, 34))));
            let (_prices, step) = bid_then_set_global(&mut book);
            (book.publisher.handle().snapshot().frame(&Coin::new("BTC")).unwrap().undecided, step.trades.len())
        };
        let one_by_one = |block_34: Option<TrailingChanges>| (31..=34).map(|height| (height, if height == 34 { block_34.clone() } else { None })).collect();
        assert_eq!(undecided_with(one_by_one(None)), (None, 0));
        let stop = TrailingStop { side: crate::order_book::Side::Ask, activation: None, retracement: Retracement::Price(50), state: TrailingState::Active { best: 1_080 } };
        let reset = TrailingChanges { updates: vec![(70, Some(("BTC".to_string(), Ok(stop))))], refresh_due_ms: None, claimed: None };
        assert_eq!(undecided_with(one_by_one(Some(reset))), (Some("trailing_unknown"), 0));
        assert_eq!(undecided_with(vec![(34, None)]), (Some("trailing_unknown"), 0), "blocks 31 to 34 as one");
    }

    /// The thread's account copy at 30 holding BTC's trailing stop 70 (best 100, retracement 5: trigger price 95), its
    /// mark claimed; with the owner's watch.
    fn trailing_accounts() -> (FlowAccounts, watch::Sender<crate::accounts::flow_accounts::FlowAccountsValidity>) {
        crate::accounts::flow_accounts::tests::copy_of(trailing_snapshot(1, 1_000))
    }

    /// The account view at 30 of `generation`: BTC's trailing stop 70 a sale of best `best` (price units of 5 size
    /// decimals) and retracement 5, its mark claimed.
    fn trailing_snapshot(generation: u64, best: u64) -> crate::accounts::flow_accounts::AccountsSnapshot {
        use crate::accounts::{flow_accounts::TrailingCopy, marks::trailing::{Retracement, TrailingState, TrailingStop}};
        let stop = TrailingStop { side: crate::order_book::Side::Ask, activation: None, retracement: Retracement::Price(50), state: TrailingState::Active { best } };
        crate::accounts::flow_accounts::AccountsSnapshot {
            generation,
            height: 30,
            time_ms: 3_000,
            coins: FxHashMap::from_iter([("BTC".to_string(), 5)]),
            marks: FxHashMap::from_iter([("BTC".to_string(), 1_000)]),
            positions: FxHashMap::default(),
            candidates: Err("none".to_string()),
            trailing: Some(TrailingCopy {
                stops: FxHashMap::from_iter([(70, ("BTC".to_string(), Ok(stop)))]),
                refresh_due_ms: None,
                claimed: std::iter::once("BTC".to_string()).collect(),
            }),
        }
    }

    fn validity(state: Result<(), String>) -> crate::accounts::flow_accounts::FlowAccountsValidity {
        crate::accounts::flow_accounts::FlowAccountsValidity { generation: 1, state }
    }

    /// The step of a mempool line on `book` as computed before it is told: the input's sync, then the line held.
    fn untold_step(book: &mut PendingBook, input: MempoolInput) -> Step {
        let MempoolInput::Tx { tx, .. } = input else { unreachable!() };
        let mut step = book.sync(NONE_UNREPORTED);
        step.absorb(book.shadow.on_tx(&tx, false, Some(&coins()), book.replica.as_ref()));
        step
    }

    /// The trades `events` told since last read, in order.
    fn told_trades(events: &mut tokio::sync::broadcast::Receiver<PendingEvent>) -> Vec<PendingTrade> {
        std::iter::from_fn(|| events.try_recv().ok())
            .flat_map(|event| match event {
                PendingEvent::Trades(frame) => frame.trades.clone(),
                _ => Vec::new(),
            })
            .collect()
    }

    /// The anchor at `anchor_height` - at most the confirmed height 32 - holding TWAP 7, both streams read through after
    /// the log's block 33: its suborder due.
    fn prove_twaps(book: &mut PendingBook, anchor_height: u64) {
        book.on_anchor(shadow::tests::twap_anchor(anchor_height), NONE_UNREPORTED);
        for stream in [TwapStream::Statuses, TwapStream::Fills] {
            book.on_twap(TwapInput::ReadThrough(stream, Instant::now()), NONE_UNREPORTED);
        }
        assert!(book.shadow.twaps_known());
    }

    /// Review 000662 r2 P2-3, r3 P2-1: the copy withdrawn while a step was computed - after its input began, before it
    /// is told -, the trailing stops are read again at the step's commit. A sale into the pending bid after a
    /// `SetGlobalAction` stop 70 does not reach - known: best 100, trigger price 95, the mark 100 - is not told: read
    /// anew, the stop is not known and the action leaves BTC not decided before the sale.
    #[test]
    fn a_copy_withdrawn_before_a_step_is_told_drops_a_trade_after_an_undecided_set_global() {
        let (accounts, owner) = trailing_accounts();
        let (mut book, mut feed, mut feeds) = served_book_with_accounts(shadow::tests::params(), accounts);
        push_block(&mut book, &mut feed, &mut feeds, 32, vec![trailing_open(70, "Activation immediate, retracement 5, best 100")], vec![]);
        push_block(&mut book, &mut feed, &mut feeds, 33, vec![], vec![]);
        let (_prices, _step) = bid_then_set_global(&mut book);
        let handle = book.publisher.handle().clone();
        let btc = Coin::new("BTC");
        assert_eq!(handle.snapshot().frame(&btc).unwrap().undecided, None, "the stop known and not reached");
        let mut events = handle.subscribe();
        let sale = ActionKind::Order(vec![requested(0, false, "99", RequestedTif::Ioc, Some("0x04"))]);
        let mut step = untold_step(&mut book, line(3_260, 4, vec![sale]));
        assert_eq!(step.trades.len(), 1, "1 at 99 into the bid");
        owner.send_replace(validity(Err("withdrawn".to_string())));
        book.publish(&mut step);
        assert_eq!(step.trades, shadow::tests::NO_TRADES);
        assert_eq!(told_trades(&mut events), shadow::tests::NO_TRADES);
        assert_eq!(handle.snapshot().frame(&btc).unwrap().undecided, Some("trailing_unknown"));
    }

    /// Review 000662 r3 P2-1: the copy withdrawn before a step is told, BTC - no trailing stop, no `SetGlobalAction` - is
    /// read anew and replayed; its IOC's trade, the new version yielding it alike, is told, BTC decided.
    #[test]
    fn a_copy_withdrawn_before_a_step_is_told_keeps_a_trade_of_no_trailing_stop() {
        let (accounts, owner) = trailing_accounts();
        let (mut book, _feed, _feeds) = served_book_with_accounts(shadow::tests::params(), accounts);
        let handle = book.publisher.handle().clone();
        let mut events = handle.subscribe();
        let mut step = untold_step(&mut book, ioc());
        owner.send_replace(validity(Err("withdrawn".to_string())));
        book.publish(&mut step);
        let told: Vec<(Px, crate::order_book::Sz)> = told_trades(&mut events).iter().map(|trade| (trade.px, trade.sz)).collect();
        assert_eq!(told, [(Px::parse_from_str("101").unwrap(), crate::order_book::Sz::parse_from_str("1").unwrap())]);
        assert_eq!(handle.snapshot().frame(&Coin::new("BTC")).unwrap().undecided, None);
    }

    /// Review 000662 r3 P2-1: BTC's TWAP suborder due, the cut at 3,980 ms: an IOC seen at 3,970 runs before a
    /// `SetGlobalAction` seen at 3,990 though held after it; stop 70 known and not reached. The copy withdrawn before the
    /// IOC's step is told, BTC is not decided from the action on - its frame says so -, and the IOC's trade, before it,
    /// is told.
    #[test]
    fn a_copy_withdrawn_before_a_step_is_told_keeps_a_trade_before_the_first_undecided_set_global() {
        let (accounts, owner) = trailing_accounts();
        let (mut book, mut feed, mut feeds) = served_book_with_accounts(shadow::tests::params(), accounts);
        push_block(&mut book, &mut feed, &mut feeds, 32, vec![trailing_open(70, "Activation immediate, retracement 5, best 100")], vec![]);
        push_block(&mut book, &mut feed, &mut feeds, 33, vec![], vec![]);
        prove_twaps(&mut book, 32);
        let (_prices, _step) = bid_then_set_global_at(&mut book, 3_990);
        let handle = book.publisher.handle().clone();
        let btc = Coin::new("BTC");
        assert_eq!(handle.snapshot().frame(&btc).unwrap().undecided, None, "the stop known and not reached");
        let mut events = handle.subscribe();
        let buy = ActionKind::Order(vec![requested(0, true, "101", RequestedTif::Ioc, Some("0x09"))]);
        let mut step = untold_step(&mut book, line(3_970, 9, vec![buy]));
        let computed = step.trades.clone();
        assert_eq!(computed.iter().map(|trade| trade.px).collect::<Vec<_>>(), [Px::parse_from_str("101").unwrap()], "the ask left after the suborder");
        owner.send_replace(validity(Err("withdrawn".to_string())));
        book.publish(&mut step);
        assert_eq!(told_trades(&mut events), computed);
        assert_eq!(handle.snapshot().frame(&btc).unwrap().undecided, Some("trailing_unknown"));
    }

    /// Review 000662 r3 P2-1: a trade the copy's new version yields otherwise is not told as computed either. Bids at 99
    /// and 98 pending, a sale - limit 98 - after a `SetGlobalAction` stop 70 does not reach is computed into the bid at
    /// 99; the copy turning before it is told to one whose stop 70 the action reaches (best 106: trigger price 101, the
    /// mark 100), the stop sells into that bid first and the sale would take the one at 98. Nothing is told, BTC decided.
    #[test]
    fn a_copy_turned_before_a_step_is_told_drops_a_trade_its_new_version_moves() {
        let (accounts, owner) = trailing_accounts();
        let (mut book, mut feed, mut feeds) = served_book_with_accounts(shadow::tests::params(), accounts);
        push_block(&mut book, &mut feed, &mut feeds, 32, vec![trailing_open(70, "Activation immediate, retracement 5, best 100")], vec![]);
        push_block(&mut book, &mut feed, &mut feeds, 33, vec![], vec![]);
        let (_prices, _step) = bid_then_set_global(&mut book);
        let bid = ActionKind::Order(vec![requested(0, true, "98", RequestedTif::Alo, Some("0x06"))]);
        drop(book.on_mempool(line(3_255, 6, vec![bid]), Some(&coins()), NONE_UNREPORTED));
        let handle = book.publisher.handle().clone();
        let mut events = handle.subscribe();
        let sale = ActionKind::Order(vec![requested(0, false, "98", RequestedTif::Ioc, Some("0x04"))]);
        let mut step = untold_step(&mut book, line(3_260, 4, vec![sale]));
        assert_eq!(step.trades.iter().map(|trade| trade.px).collect::<Vec<_>>(), [Px::parse_from_str("99").unwrap()]);
        book.accounts.apply(FlowAccountsEvent::Snapshot(Box::new(trailing_snapshot(2, 1_060))));
        owner.send_replace(crate::accounts::flow_accounts::FlowAccountsValidity { generation: 2, state: Ok(()) });
        book.publish(&mut step);
        assert_eq!(told_trades(&mut events), shadow::tests::NO_TRADES);
        assert_eq!(handle.snapshot().frame(&Coin::new("BTC")).unwrap().undecided, None);
    }

    /// Review 000662 r3 P2-2: BTC's TWAP suborder runs after a `SetGlobalAction` stop 70, known, does not reach. The copy
    /// withdrawn before the step proving the TWAPs is told, BTC is not decided from the action on and the suborder's
    /// trade is not told; the copy restored, BTC replayed tells it once, a later replay not again.
    #[test]
    fn a_suborder_withdrawn_before_told_goes_out_once_the_copy_is_restored() {
        let (accounts, owner) = trailing_accounts();
        let (mut book, mut feed, mut feeds) = served_book_with_accounts(shadow::tests::params(), accounts);
        push_block(&mut book, &mut feed, &mut feeds, 32, vec![trailing_open(70, "Activation immediate, retracement 5, best 100")], vec![]);
        push_block(&mut book, &mut feed, &mut feeds, 33, vec![], vec![]);
        let (_prices, _step) = bid_then_set_global(&mut book);
        let handle = book.publisher.handle().clone();
        let mut events = handle.subscribe();
        let suborders = |trades: Vec<PendingTrade>| trades.into_iter().filter(|trade| matches!(trade.taker, Taker::Twap { .. })).collect::<Vec<_>>();
        book.on_anchor(shadow::tests::twap_anchor(32), NONE_UNREPORTED);
        book.on_twap(TwapInput::ReadThrough(TwapStream::Statuses, Instant::now()), NONE_UNREPORTED);
        let mut step = book.sync(NONE_UNREPORTED);
        step.absorb(book.shadow.on_twap_read_through(TwapStream::Fills, Instant::now(), book.replica.as_ref()));
        let computed = suborders(step.trades.clone());
        assert_eq!(computed.iter().map(|trade| trade.px).collect::<Vec<_>>(), [Px::parse_from_str("101").unwrap()]);
        owner.send_replace(validity(Err("withdrawn".to_string())));
        book.publish(&mut step);
        assert_eq!(suborders(told_trades(&mut events)), shadow::tests::NO_TRADES, "withdrawn: none told");
        assert_eq!(handle.snapshot().frame(&Coin::new("BTC")).unwrap().undecided, Some("trailing_unknown"));
        owner.send_replace(validity(Ok(())));
        book.on_accounts(NONE_UNREPORTED);
        assert_eq!(suborders(told_trades(&mut events)), computed, "restored: told once");
        let ask = ActionKind::Order(vec![requested(0, false, "105", RequestedTif::Alo, Some("0x05"))]);
        drop(book.on_mempool(line(3_300, 5, vec![ask]), Some(&coins()), NONE_UNREPORTED));
        assert_eq!(suborders(told_trades(&mut events)), shadow::tests::NO_TRADES, "BTC replayed: not again");
    }

    /// The trades told as `input` comes, the copy - withheld while BTC's suborder (TWAP 7's slot 0, 0.1 at the cut) was
    /// held back, BTC not decided - restored by the owner's watch with no event just before it. The TWAPs anchored at 31,
    /// 32 proven complete; ETH's ask resting from 34.
    fn told_restored_before(input: impl FnOnce(&mut PendingBook)) -> Vec<PendingTrade> {
        let (accounts, owner) = trailing_accounts();
        owner.send_replace(validity(Err("withheld".to_string())));
        let (mut book, mut feed, mut feeds) = served_book_with_accounts(shadow::tests::params(), accounts);
        push_block(&mut book, &mut feed, &mut feeds, 32, vec![trailing_open(70, "Activation immediate, retracement 5, best 100")], vec![]);
        push_block(&mut book, &mut feed, &mut feeds, 33, vec![], vec![]);
        let (_prices, _step) = bid_then_set_global(&mut book);
        prove_twaps(&mut book, 31);
        let (status, diff) = eth_ask();
        push_block(&mut book, &mut feed, &mut feeds, 34, vec![status], vec![diff]);
        push_block(&mut book, &mut feed, &mut feeds, 35, vec![], vec![]);
        let handle = book.publisher.handle().clone();
        assert_eq!(handle.snapshot().frame(&Coin::new("BTC")).unwrap().undecided, Some("trailing_unknown"), "the suborder held back");
        let mut events = handle.subscribe();
        owner.send_replace(validity(Ok(())));
        input(&mut book);
        told_trades(&mut events)
    }

    fn suborders_told_restored_before(input: impl FnOnce(&mut PendingBook)) -> Vec<PendingTrade> {
        told_restored_before(input).into_iter().filter(|trade| matches!(trade.taker, Taker::Twap { .. })).collect()
    }

    /// A buy IOC of `sz` BTC at 101 seen at `seen_ms`.
    fn ioc_of(seen_ms: i64, sz: &str) -> MempoolInput {
        line(seen_ms, 9, vec![btc_buy(sz)])
    }

    fn btc_buy(sz: &str) -> ActionKind {
        let order = mempool::RequestedOrder {
            sz: crate::order_book::Sz::parse_from_str(sz).unwrap(),
            sz_shape: mempool::DecimalShape::of(sz),
            ..requested(0, true, "101", RequestedTif::Ioc, Some("0x09"))
        };
        ActionKind::Order(vec![order])
    }

    /// ETH's ask of 1 at 2,000 (oid 21): its status and its diff.
    fn eth_ask() -> (serde_json::Value, serde_json::Value) {
        let mut status = btc_status(21, "A", "2000.0", "1.0", "open", Some("0x15"), "Alo", 3_300);
        status["order"]["coin"] = serde_json::json!("ETH");
        let mut diff = btc_diff(21, "A", "2000.0", new_diff("1.0"));
        diff["coin"] = serde_json::json!("ETH");
        (status, diff)
    }

    /// Review 000662 r6 P2: BTC's suborder held back to the commit takes its place in the frame among a line's trades on
    /// other coins: before an ETH IOC seen at the cut (3,980 ms), before a line's ETH then BTC IOCs after it; after them,
    /// as they came, before it.
    #[test]
    fn a_held_back_suborder_takes_its_place_among_the_frames_trades() {
        let eth_buy = || ActionKind::Order(vec![requested(1, true, "2000", RequestedTif::Ioc, Some("0x0e"))]);
        let told_of = |input: MempoolInput| -> Vec<(String, bool)> {
            let told = told_restored_before(|book| drop(book.on_mempool(input, Some(&coins()), NONE_UNREPORTED)));
            told.iter().map(|trade| (trade.coin.value(), matches!(trade.taker, Taker::Twap { .. }))).collect()
        };
        let order = |entries: &[(&str, bool)]| entries.iter().map(|(coin, twap)| ((*coin).to_string(), *twap)).collect::<Vec<_>>();
        assert_eq!(told_of(line(3_980, 9, vec![eth_buy()])), order(&[("BTC", true), ("ETH", false)]), "at the cut");
        assert_eq!(told_of(line(3_990, 9, vec![eth_buy(), btc_buy("0.5")])), order(&[("BTC", true), ("ETH", false), ("BTC", false)]));
        assert_eq!(told_of(line(3_970, 9, vec![eth_buy(), btc_buy("0.5")])), order(&[("ETH", false), ("BTC", false), ("BTC", true)]), "before the cut");
    }

    /// Review 000662 r5 P2: the copy restored in the step of a line, BTC's suborder - its slot not told - is told as the
    /// step's final run fills it, at its place in the run. A buy of 1 before the cut takes the ask whole: the suborder
    /// takes nothing. A buy of 0.5 before the cut: its trade, then the suborder's. After the cut: the suborder's first.
    #[test]
    fn a_restored_copys_suborder_is_told_as_the_steps_final_run_fills_it() {
        let told_of = |input: MempoolInput| -> Vec<(bool, String)> {
            let told = told_restored_before(|book| drop(book.on_mempool(input, Some(&coins()), NONE_UNREPORTED)));
            told.iter().map(|trade| (matches!(trade.taker, Taker::Twap { .. }), format!("{} @ {}", trade.sz.to_str(), trade.px.to_str()))).collect()
        };
        let told = |entries: &[(bool, &str)]| entries.iter().map(|(twap, trade)| (*twap, (*trade).to_string())).collect::<Vec<_>>();
        assert_eq!(told_of(ioc_of(3_270, "1")), told(&[(false, "1 @ 101")]), "the ask taken before the suborder");
        assert_eq!(told_of(ioc_of(3_270, "0.5")), told(&[(false, "0.5 @ 101"), (true, "0.1 @ 101")]));
        assert_eq!(told_of(ioc_of(3_990, "0.5")), told(&[(true, "0.1 @ 101"), (false, "0.5 @ 101")]), "after the cut");
    }

    /// Review 000662 r5 P2: the copy restored in the step of an input that finds a gap - the block log skipping 36, a
    /// TWAP status line of the complete height 32, above the anchor, coming late -, BTC's suborder is not told.
    #[test]
    fn a_restored_copys_suborder_is_not_told_past_a_gap_its_step_finds() {
        let skipping = |book: &mut PendingBook| {
            let block = LoggedBlock {
                height: 37, time_ms: 3_700, hashes: Vec::new(), priorities: Vec::new(), taker_outcomes: Vec::new(), signer_accounts: Vec::new(),
                twap_cancels: Vec::new(), set_global: false, read_at: Instant::now(),
            };
            drop(book.on_block(BlockInput::Block { block, after_loss: false }, NONE_UNREPORTED));
        };
        assert_eq!(suborders_told_restored_before(skipping), shadow::tests::NO_TRADES, "a block log hole");
        let late = |book: &mut PendingBook| book.on_twap(TwapInput::Line(TwapLine::Statuses(TwapStatusLine { height: 32, events: Vec::new() })), NONE_UNREPORTED);
        assert_eq!(suborders_told_restored_before(late), shadow::tests::NO_TRADES, "a late TWAP line");
    }

    /// Review 000662 r4 P2-1: an input that is itself a loss - a TWAP stream's, the mempool's, the block log's - takes it
    /// before the trailing stops the restored copy serves are read: no suborder trade told. An input of no loss tells it.
    #[test]
    fn an_input_bringing_a_loss_takes_it_before_the_trailing_stops_are_read() {
        let twap_loss = suborders_told_restored_before(|book| book.on_twap(TwapInput::Loss, NONE_UNREPORTED));
        let mempool_loss = suborders_told_restored_before(|book| drop(book.on_mempool(MempoolInput::Loss, Some(&coins()), NONE_UNREPORTED)));
        let block_log_loss = suborders_told_restored_before(|book| drop(book.on_block(BlockInput::Loss, NONE_UNREPORTED)));
        assert_eq!((twap_loss.len(), mempool_loss.len(), block_log_loss.len()), (0, 0, 0));
        let no_loss = suborders_told_restored_before(|book| drop(book.on_mempool(line(3_300, 3, vec![]), Some(&coins()), NONE_UNREPORTED)));
        assert_eq!(no_loss.iter().map(|trade| (trade.px, trade.sz)).collect::<Vec<_>>(), [(Px::parse_from_str("101").unwrap(), crate::order_book::Sz::parse_from_str("0.1").unwrap())]);
    }

    /// Review 000662 r4 P2-2: a suborder's trade computed in a step whose replica stops before it is told - no trades
    /// frame - is not told; the next generation served with the ask again - forked at height 0 here, its TWAPs proven
    /// anew over its blocks - tells it once.
    #[test]
    fn a_suborder_computed_as_the_replica_stops_goes_out_with_the_next_generation() {
        let (mut book, mut feed, mut feeds) = served_book();
        push_block(&mut book, &mut feed, &mut feeds, 32, vec![], vec![]);
        push_block(&mut book, &mut feed, &mut feeds, 33, vec![], vec![]);
        let handle = book.publisher.handle().clone();
        let mut events = handle.subscribe();
        book.on_anchor(shadow::tests::twap_anchor(32), NONE_UNREPORTED);
        book.on_twap(TwapInput::ReadThrough(TwapStream::Statuses, Instant::now()), NONE_UNREPORTED);
        let mut step = book.sync(NONE_UNREPORTED);
        step.absorb(book.shadow.on_twap_read_through(TwapStream::Fills, Instant::now(), book.replica.as_ref()));
        let computed = step.trades.clone();
        assert_eq!(computed.iter().map(|trade| (trade.px, trade.sz)).collect::<Vec<_>>(), [(Px::parse_from_str("101").unwrap(), crate::order_book::Sz::parse_from_str("0.1").unwrap())]);
        // as a batch past the replica's budget stops it
        book.request_next("backlog_budget");
        book.publish(&mut step);
        assert_eq!(told_trades(&mut events), shadow::tests::NO_TRADES, "no replica to frame them on");
        empty_book_batch(&mut feed);
        let ask = (vec![btc_status(11, "A", "101.0", "1.0", "open", Some("0x0b"), "Alo", 2_500)], vec![btc_diff(11, "A", "101.0", new_diff("1.0"))]);
        push_block(&mut book, &mut feed, &mut feeds, 34, ask.0, ask.1);
        push_block(&mut book, &mut feed, &mut feeds, 35, vec![], vec![]);
        for stream in [TwapStream::Statuses, TwapStream::Fills] {
            book.on_twap(TwapInput::ReadThrough(stream, Instant::now()), NONE_UNREPORTED);
        }
        assert!(book.shadow.twaps_known());
        assert_eq!(told_trades(&mut events), computed, "told once");
        push_block(&mut book, &mut feed, &mut feeds, 36, vec![], vec![]);
        assert_eq!(told_trades(&mut events), shadow::tests::NO_TRADES, "not again");
    }

    /// Review 000662 r4 P2-3: two `SetGlobalAction`s whose lines share a `local_time`, marks 100 then 95; stop 70 - best
    /// 100, trigger price 95 - fired by the second sells 1 into the bid at 99. The copy turning before the second's step
    /// is told to one whose stop the first fires (best 106: trigger price 101), the second fires nothing: its trade -
    /// the same order's, the same fill - is not told as the first's.
    #[test]
    fn a_trigger_trade_is_told_only_as_the_action_firing_it_yields_it() {
        let (accounts, owner) = trailing_accounts();
        let (mut book, mut feed, mut feeds) = served_book_with_accounts(shadow::tests::params(), accounts);
        push_block(&mut book, &mut feed, &mut feeds, 32, vec![trailing_open(70, "Activation immediate, retracement 5, best 100")], vec![]);
        push_block(&mut book, &mut feed, &mut feeds, 33, vec![], vec![]);
        let bid = ActionKind::Order(vec![requested(0, true, "99", RequestedTif::Alo, Some("0x08"))]);
        drop(book.on_mempool(line(3_210, 8, vec![bid]), Some(&coins()), NONE_UNREPORTED));
        let btc = Coin::new("BTC");
        let untold_set_global = |book: &mut PendingBook, hash: u8, mark: &str| {
            let MempoolInput::Tx { tx, .. } = line(3_250, hash, vec![]) else { unreachable!() };
            let mark_px = Px::parse_from_str(mark).unwrap();
            let reached = book.shadow.reached_accounts(&btc, book.replica.as_ref(), mark_px, tx.seen_ns);
            let sga = SetGlobalCoin::frozen("BTC", mark_px, &reached, book.accounts.copy().ok());
            let mut step = book.sync(NONE_UNREPORTED);
            step.absorb(book.shadow.on_set_global(&tx, vec![(btc.clone(), 0, sga)], book.replica.as_ref()));
            step
        };
        let mut first = untold_set_global(&mut book, 1, "100");
        assert_eq!(first.trades, shadow::tests::NO_TRADES);
        book.publish(&mut first);
        let mut second = untold_set_global(&mut book, 2, "95");
        assert_eq!(second.trades.iter().map(|trade| trade.px).collect::<Vec<_>>(), [Px::parse_from_str("99").unwrap()]);
        let handle = book.publisher.handle().clone();
        let mut events = handle.subscribe();
        book.accounts.apply(FlowAccountsEvent::Snapshot(Box::new(trailing_snapshot(2, 1_060))));
        owner.send_replace(crate::accounts::flow_accounts::FlowAccountsValidity { generation: 2, state: Ok(()) });
        book.publish(&mut second);
        assert_eq!(told_trades(&mut events), shadow::tests::NO_TRADES);
    }

    /// Review 000662 r2 P2-4: an input restoring the account copy and bringing a TWAP loss takes the loss first: BTC,
    /// not decided while the copy was withheld - its suborder held back -, is replayed decided again with its TWAPs
    /// unknown: no suborder trade told.
    #[test]
    fn a_copy_restored_with_a_twap_loss_tells_no_suborder_trade() {
        let (accounts, owner) = trailing_accounts();
        owner.send_replace(validity(Err("withheld".to_string())));
        let (mut book, mut feed, mut feeds) = served_book_with_accounts(shadow::tests::params(), accounts);
        push_block(&mut book, &mut feed, &mut feeds, 32, vec![trailing_open(70, "Activation immediate, retracement 5, best 100")], vec![]);
        push_block(&mut book, &mut feed, &mut feeds, 33, vec![], vec![]);
        let (_prices, _step) = bid_then_set_global(&mut book);
        // the confirmed block's TWAPs proven - the log read past it, both streams read through after that - while BTC is
        // not decided: its suborder runs, told nothing
        book.on_anchor(shadow::tests::twap_anchor(32), NONE_UNREPORTED);
        let twap_trades = |step: &Step| step.trades.iter().filter(|trade| matches!(trade.taker, Taker::Twap { .. })).count();
        for stream in [TwapStream::Statuses, TwapStream::Fills] {
            let mut step = book.sync(NONE_UNREPORTED);
            step.absorb(book.shadow.on_twap_read_through(stream, Instant::now(), book.replica.as_ref()));
            assert_eq!(twap_trades(&step), 0);
            book.publish(&mut step);
        }
        assert!(book.shadow.twaps_known());
        let handle = book.publisher.handle().clone();
        assert_eq!(handle.snapshot().frame(&Coin::new("BTC")).unwrap().undecided, Some("trailing_unknown"), "the suborder held back");
        let mut events = handle.subscribe();
        owner.send_replace(validity(Ok(())));
        book.on_accounts(Unreported { twaps: true, ..NONE_UNREPORTED });
        let told_twap = std::iter::from_fn(|| events.try_recv().ok()).any(|event| match event {
            PendingEvent::Trades(frame) => frame.trades.iter().any(|trade| matches!(trade.taker, Taker::Twap { .. })),
            _ => false,
        });
        assert!(!told_twap);
        assert_eq!(handle.snapshot().frame(&Coin::new("BTC")).unwrap().undecided, None, "decided again");
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
        assert_eq!(latest.bbo.1.unwrap().0, Px::parse_from_str("100.5").unwrap());
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
        (Px::parse_from_str("101").unwrap(), crate::order_book::Sz::parse_from_str("1").unwrap(), 1)
    }

    /// Review 000611 r3 P2-2: a batch whose block both streams have not passed yet only queues - no settling; the one
    /// moving the confirmed height settles.
    #[test]
    fn a_batch_that_only_queues_settles_nothing() {
        let (mut book, mut feed, mut feeds) = served_book();
        let settles = book.shadow.settles();
        let block = LoggedBlock { height: 32, time_ms: 3_200, hashes: Vec::new(), priorities: Vec::new(), taker_outcomes: Vec::new(), signer_accounts: Vec::new(), twap_cancels: Vec::new(), set_global: false, read_at: Instant::now() };
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
        let book = PendingBook::new(Arc::clone(&inputs.control), backlog_events, Shadow::new(shadow::tests::params(), true, 0), publisher, no_accounts());
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
