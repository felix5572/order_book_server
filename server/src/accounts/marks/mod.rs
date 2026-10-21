//! The node's marks, block by block (design 000439 r2). HIP-3 coins come from the node's oracle update stream, which
//! writes each mark the node applies (research `hl_node_mark_state_20261019` "HIP-3：直接读节点的流": 930/930 stored
//! marks and times equal the last written ones). The main dex's prices are replayed action by action (`native`): its
//! books follow the node's raw book diffs order by order (`book`), each price action reads them at its place in the
//! block (`execution`), and the node's rules move the prices (`rules`).
//!
//! `NodeMarks` lives in the account view's owner task and gets every completed block before the account view does, so
//! both stand at one height. Trust (revision 2):
//!
//! - a checkpoint's seed installs once every block up to it is applied, or at once while the marks are unavailable;
//! - at each grid height G whose seed is not installed yet the state is kept as a snapshot and a log of the mark inputs
//!   after G begins; it lasts only until a seed installs (revision 2: "只需覆盖 G 到它被装入的这段时间"), since every
//!   later seed is at a later grid height;
//! - installing checkpoint G compares the snapshot with it (counts and evidence; a mismatch does not withhold the marks,
//!   the rebuild corrects them), then rebuilds the state from it and the logged blocks above it;
//! - a loss on a stream the marks read, a seed breaking its contract or a log overflow withholds the marks until a later
//!   checkpoint rebuilds them from a complete log.

pub(crate) mod actions;
pub(crate) mod book;
pub(crate) mod decimal;
pub(crate) mod execution;
pub(crate) mod native;
pub(crate) mod pairing;
pub(crate) mod rules;
pub(crate) mod seed;

use rustc_hash::FxHashMap;

use self::{
    actions::{MarkActions, Rank},
    book::{Applied, BookDiff, BooksCheck, NativeBooks},
    execution::{BlockFill, BlockStatuses, BookCuts, price_points},
    native::{CoinCut, CoinPriceState, NativePrices, PricesCheck},
    rules::wire_units,
    seed::{Hip3Coin, MarkSeed, StoredMark, exact_price_units},
};
use super::{
    book::{AnchorInfo, Stream, StreamProgress, Unavailable},
    checkpoint::node_time_ns,
};
use crate::types::node_data::{Batch, NodeDataOrderDiff, NodeDataOrderStatus, OracleUpdateEvent};

/// Logged inputs (HIP-3 mark updates, diffs, block times, actions, statuses, fills) at most. About 12,000 diffs, 6,800
/// statuses and a few hundred others are written a second (2026-10-20); the log spans a grid height to that checkpoint's
/// install (~13 s on bm, ~250k inputs), so this is ~2.5 minutes of a checkpoint late or skipped, not a normal state.
pub(crate) const MAX_LOG_INPUTS: usize = 2_000_000;
/// Mismatches listed per check (all are counted), and ambiguous cuts listed per seed interval.
const MAX_CHECK_SAMPLES: usize = 20;
/// The streams the marks read; a log overflow is a loss of each.
pub(crate) const MARK_STREAMS: [Stream; 5] = [Stream::Fills, Stream::BlockActions, Stream::Hip3OracleUpdates, Stream::BookDiffs, Stream::OrderStatuses];

/// The node's checkpoint heights: `anchor + k * interval` (`checkpoint_grid`, the two newest on disk at start).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CheckpointGrid {
    pub(crate) anchor: u64,
    pub(crate) interval: u64,
}

impl CheckpointGrid {
    const fn contains(self, height: u64) -> bool {
        height.abs_diff(self.anchor).is_multiple_of(self.interval)
    }
}

/// One written HIP-3 mark (`oracle_pxs.coin_to_mark_px` entry), its price still the wire decimal: its units need the
/// coin's `szDecimals`, which only a seed's universe gives.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Hip3MarkUpdate {
    coin: String,
    px: String,
    /// `last_update_time`, unix ns (the block time on all 215,769 entries of 2026-10-20 11 UTC).
    time_ns: u64,
}

/// A completed block's lines of the streams the marks read.
#[derive(Debug)]
pub(crate) struct MarkInputs {
    pub(crate) hip3_oracle_updates: Vec<Batch<OracleUpdateEvent>>,
    /// Every market's diffs, in the node's execution order.
    pub(crate) book_diffs: Vec<Batch<NodeDataOrderDiff>>,
    /// The marks' reading of the block's `replica_cmds` line; an error is a loss of that stream for the marks.
    pub(crate) mark_actions: Result<MarkActions, String>,
    /// Cancels of orders with a cloid and triggers.
    pub(crate) order_statuses: Vec<Batch<NodeDataOrderStatus>>,
    /// Every fill of the block, in order.
    pub(crate) fills: Vec<BlockFill>,
}

/// One block's mark inputs as logged and applied.
#[derive(Debug, Default)]
struct BlockInputs {
    hip3: Vec<Hip3MarkUpdate>,
    diffs: Vec<BookDiff>,
    /// `None` when the line broke the marks' contract.
    actions: Option<MarkActions>,
    statuses: BlockStatuses,
    fills: Vec<BlockFill>,
}

impl BlockInputs {
    /// Converts a block's lines; a line that breaks its contract is reported against its stream and left out.
    fn of(height: u64, inputs: MarkInputs, report: &mut MarksBlockReport) -> Self {
        let mut out = Self::default();
        for event in inputs.hip3_oracle_updates.into_iter().flat_map(Batch::events) {
            for (coin, written) in event.oracle_pxs.coin_to_mark_px {
                match node_time_ns(&written.last_update_time) {
                    Ok(time_ns) => out.hip3.push(Hip3MarkUpdate { coin, px: written.px, time_ns }),
                    Err(err) => report.errors.push((Stream::Hip3OracleUpdates, format!("block {height} {coin}: {err}"))),
                }
            }
        }
        for diff in inputs.book_diffs.into_iter().flat_map(Batch::events) {
            match BookDiff::of(&diff) {
                Ok(diff) => out.diffs.push(diff),
                Err(err) => report.errors.push((Stream::BookDiffs, format!("block {height} {}: {err}", diff.coin_name()))),
            }
        }
        match inputs.mark_actions {
            Ok(actions) => out.actions = Some(actions),
            Err(err) => report.errors.push((Stream::BlockActions, err)),
        }
        match BlockStatuses::of(inputs.order_statuses.into_iter().flat_map(Batch::events)) {
            Ok(statuses) => out.statuses = statuses,
            Err(err) => report.errors.push((Stream::OrderStatuses, format!("block {height}: {err}"))),
        }
        out.fills = inputs.fills;
        out
    }

    /// The block's time counts as one: it refreshes the reference caches with no action at all (review 000444 r1 P2-1).
    fn len(&self) -> usize {
        let actions = self.actions.as_ref().map_or(0, |actions| 1 + actions.actions.len());
        self.hip3.len() + self.diffs.len() + actions + self.statuses.len() + self.fills.len()
    }
}

/// The marks at `height`.
#[derive(Debug, Clone)]
struct MarkState {
    /// The checkpoint it was rebuilt from.
    seed: AnchorInfo,
    /// Highest block applied; the seed's height while no later block is.
    height: u64,
    /// The block time of `height`, unix ms (the seed's for the seed).
    time_ms: u64,
    hip3: FxHashMap<String, Hip3Coin>,
    books: NativeBooks,
    prices: NativePrices,
    /// A diff did not fit the books, or the block's price actions broke the contract: the books, or the prices, stay
    /// as they were until a seed replaces them (the loss withholds the marks).
    books_broken: bool,
    prices_broken: bool,
}

impl MarkState {
    fn seeded(seed: MarkSeed) -> Self {
        let MarkSeed { height, time_ms, native, hip3 } = seed;
        let prices = NativePrices::seeded(&native);
        let books = NativeBooks::seeded(native);
        Self { seed: AnchorInfo { height, time_ms }, height, time_ms, hip3, books, prices, books_broken: false, prices_broken: false }
    }

    /// Applies block `height`; coins outside the seed's universe (listed since) are counted and skipped, a value that
    /// does not convert, a diff that does not fit or a price action that contradicts the block breaks the contract.
    fn apply(&mut self, height: u64, inputs: &BlockInputs, report: &mut MarksBlockReport) {
        self.apply_hip3(height, &inputs.hip3, report);
        if let Some(actions) = &inputs.actions {
            self.prices.begin_block(actions.time_ns);
            self.time_ms = actions.time_ns / 1_000_000;
        }
        let cuts = self.apply_diffs(height, &inputs.diffs, report);
        let trades = self.block_trades(height, &inputs.fills, report);
        if let Some(actions) = inputs.actions.as_ref().filter(|_| !self.books_broken && !self.prices_broken)
            && let Err(err) = self.apply_prices(actions, inputs, &cuts, &trades, report)
        {
            report.errors.push((Stream::BlockActions, format!("block {height}: {err}")));
            self.prices_broken = true;
        }
        for (asset, trades) in trades {
            if let Some((_, px)) = trades.last() {
                self.prices.trade(asset, *px);
            }
        }
        self.height = height;
    }

    fn apply_hip3(&mut self, height: u64, updates: &[Hip3MarkUpdate], report: &mut MarksBlockReport) {
        for update in updates {
            let Some(coin) = self.hip3.get_mut(&update.coin) else {
                report.unknown_coin_updates += 1;
                continue;
            };
            match exact_price_units(&update.px, coin.size_decimals) {
                Ok(px) => coin.mark = Some(StoredMark { px, time_ns: update.time_ns }),
                Err(err) => report.errors.push((Stream::Hip3OracleUpdates, format!("block {height} {}: {err}", update.coin))),
            }
        }
    }

    /// Applies the block's diffs in order, recording each line that moved a book's best prices.
    fn apply_diffs(&mut self, height: u64, diffs: &[BookDiff], report: &mut MarksBlockReport) -> BookCuts {
        let mut cuts = BookCuts::default();
        for (line, diff) in diffs.iter().enumerate() {
            if self.books_broken {
                break;
            }
            let before = self.books.book(diff.coin()).map(|book| self.books.best(book));
            match self.books.apply(diff) {
                Ok(Applied::Book(book)) => {
                    let after = self.books.best(book);
                    if let Some(before) = before.filter(|before| *before != after) {
                        cuts.record(book, line, before, after);
                    }
                }
                Ok(Applied::OtherMarket) => {}
                Ok(Applied::UnknownCoin) => report.unknown_coin_diffs += 1,
                Err(err) => {
                    report.errors.push((Stream::BookDiffs, format!("block {height} {err}")));
                    self.books_broken = true;
                }
            }
        }
        cuts
    }

    /// Per main-dex asset, the block's crossed fills: (fill index, price units).
    fn block_trades(&self, height: u64, fills: &[BlockFill], report: &mut MarksBlockReport) -> FxHashMap<usize, Vec<(usize, u64)>> {
        let mut trades: FxHashMap<usize, Vec<(usize, u64)>> = FxHashMap::default();
        for (index, fill) in fills.iter().enumerate().filter(|(_, fill)| fill.crossed) {
            let Some(asset) = self.prices.asset(&fill.coin) else { continue };
            match wire_units(&fill.px, self.prices.size_decimals(asset)) {
                Ok(px) => trades.entry(asset).or_default().push((index, px)),
                Err(err) => report.errors.push((Stream::Fills, format!("block {height} {} tid {}: {err}", fill.coin, fill.tid))),
            }
        }
        trades
    }

    /// The block's price actions in execution order, each at its cut.
    fn apply_prices(
        &mut self,
        actions: &MarkActions,
        inputs: &BlockInputs,
        cuts: &BookCuts,
        trades: &FxHashMap<usize, Vec<(usize, u64)>>,
        report: &mut MarksBlockReport,
    ) -> Result<(), String> {
        let books = &self.books;
        for point in price_points(actions, &inputs.statuses, &inputs.diffs, &inputs.fills, |coin| books.order_asset(coin))? {
            if point.cut.lo < point.cut.hi {
                report.ambiguous_cuts += 1;
                report.ambiguous_samples.push(AmbiguousCut { rank: point.action.rank, lo: point.cut.lo, hi: point.cut.hi });
            }
            let cut_of = |asset: usize| CoinCut {
                best: cuts.at(asset, point.cut.lo, books.best(asset)),
                spot_reference: books.spot_reference(asset).map(|book| cuts.at(book, point.cut.lo, books.best(book))),
                block_trade: trades
                    .get(&asset)
                    .and_then(|trades| trades.iter().take_while(|(index, _)| *index < point.cut.fills_lo).last())
                    .map(|(_, px)| *px),
            };
            self.prices.apply(&point.action.action, point.action.user, actions.time_ns, cut_of)?;
        }
        Ok(())
    }
}

/// While started, every completed block above `base` is logged (blocks without mark inputs as nothing), up to
/// `capacity` inputs.
#[derive(Debug)]
struct InputLog {
    capacity: usize,
    /// `None` while no rebuild can need the blocks: before the first grid height, and from an install to the next one.
    base: Option<u64>,
    blocks: Vec<(u64, BlockInputs)>,
    inputs: usize,
}

impl InputLog {
    const fn new(capacity: usize) -> Self {
        Self { capacity, base: None, blocks: Vec::new(), inputs: 0 }
    }

    /// Logs block `height` while started; returns false when that overflowed the log, which then starts over after it.
    fn push(&mut self, height: u64, block: BlockInputs) -> bool {
        let Some(base) = self.base else {
            return true;
        };
        if height <= base || block.len() == 0 {
            return true;
        }
        self.inputs += block.len();
        self.blocks.push((height, block));
        if self.inputs <= self.capacity {
            return true;
        }
        self.restart(height);
        false
    }

    /// Logs the blocks above `height` from now on, forgetting everything before.
    fn restart(&mut self, height: u64) {
        self.blocks.clear();
        self.inputs = 0;
        self.base = Some(height);
    }

    /// Logs nothing until the next restart: an installed seed covers every block a later seed could need.
    fn stop(&mut self) {
        self.blocks.clear();
        self.inputs = 0;
        self.base = None;
    }

    /// Whether every block above `height` up to the last applied one is here.
    fn reaches(&self, height: u64) -> bool {
        self.base.is_some_and(|base| base <= height)
    }

    fn after(&self, height: u64) -> impl Iterator<Item = &(u64, BlockInputs)> {
        self.blocks.iter().filter(move |(block, _)| *block > height)
    }
}

/// The progress of the streams the marks read: every assembled one but the misc events.
#[derive(Debug)]
struct MarkStreams {
    fills: StreamProgress,
    block_actions: StreamProgress,
    hip3_oracle_updates: StreamProgress,
    book_diffs: StreamProgress,
    order_statuses: StreamProgress,
}

impl MarkStreams {
    const fn starting(started_ms: u64) -> Self {
        Self {
            fills: StreamProgress::starting(started_ms),
            block_actions: StreamProgress::starting(started_ms),
            hip3_oracle_updates: StreamProgress::starting(started_ms),
            book_diffs: StreamProgress::starting(started_ms),
            order_statuses: StreamProgress::starting(started_ms),
        }
    }

    const fn get_mut(&mut self, stream: Stream) -> Option<&mut StreamProgress> {
        match stream {
            Stream::Fills => Some(&mut self.fills),
            Stream::BlockActions => Some(&mut self.block_actions),
            Stream::Hip3OracleUpdates => Some(&mut self.hip3_oracle_updates),
            Stream::BookDiffs => Some(&mut self.book_diffs),
            Stream::OrderStatuses => Some(&mut self.order_statuses),
            Stream::MiscEvents => None,
        }
    }

    fn check(&self, seed: AnchorInfo) -> Result<(), Unavailable> {
        self.fills.check(Stream::Fills, seed)?;
        self.block_actions.check(Stream::BlockActions, seed)?;
        self.hip3_oracle_updates.check(Stream::Hip3OracleUpdates, seed)?;
        self.book_diffs.check(Stream::BookDiffs, seed)?;
        self.order_statuses.check(Stream::OrderStatuses, seed)?;
        Ok(())
    }

    /// The streams a block's inputs show read up to it (`replica_cmds` has a line for every block).
    fn record_read(&mut self, height: u64, inputs: &MarkInputs) {
        self.block_actions.record_read(height);
        let read = [
            (&mut self.fills, inputs.fills.is_empty()),
            (&mut self.hip3_oracle_updates, inputs.hip3_oracle_updates.is_empty()),
            (&mut self.book_diffs, inputs.book_diffs.is_empty()),
            (&mut self.order_statuses, inputs.order_statuses.is_empty()),
        ];
        for (progress, empty) in read {
            if !empty {
                progress.record_read(height);
            }
        }
    }
}

/// What one completed block did to the marks.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct MarksBlockReport {
    /// HIP-3 updates of coins outside the seed's universe.
    pub(crate) unknown_coin_updates: usize,
    /// Main-dex diffs of coins outside the seed's universe.
    pub(crate) unknown_coin_diffs: usize,
    /// Contract violations by stream; each is a loss of that stream up to the block.
    pub(crate) errors: Vec<(Stream, String)>,
    /// The log overflowed with this block, a loss of every stream the marks read up to it.
    pub(crate) log_overflowed: bool,
    /// Price actions whose lines before them are not pinned to one count: each reads the book at `lo`, the first line
    /// not known to run before it (design 000439 r1 3, r2 revision 5). All are counted.
    pub(crate) ambiguous_cuts: u64,
    /// The first of them while the seed interval's `MAX_CHECK_SAMPLES` last (review 000444 r1 P3).
    pub(crate) ambiguous_samples: Vec<AmbiguousCut>,
}

/// A price action's ambiguous cut: the diff lines `[lo, hi)` may run before or after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AmbiguousCut {
    pub(crate) rank: Rank,
    pub(crate) lo: usize,
    pub(crate) hi: usize,
}

/// The snapshot at a checkpoint's height against the checkpoint (design 000439 r2 revision 2).
#[derive(Debug, PartialEq, Eq)]
#[expect(clippy::large_enum_variant, reason = "one per checkpoint install, moved once into the log")]
pub(crate) enum MarksCheck {
    Checked { hip3: Hip3Check, books: BooksCheck, prices: PricesCheck },
    /// `noSnapshot`: no trusted state was kept at the checkpoint's height (the first after start, the marks were
    /// unavailable there, a node grid other than the inferred one, or a later grid height replaced it).
    Skipped(&'static str),
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Hip3Check {
    pub(crate) equal: u64,
    pub(crate) differ: u64,
    /// Coins of the checkpoint the state did not know (listed since its seed).
    pub(crate) unknown: u64,
    pub(crate) samples: Vec<Hip3Mismatch>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Hip3Mismatch {
    pub(crate) coin: String,
    pub(crate) replayed: Option<StoredMark>,
    pub(crate) checkpoint: Option<StoredMark>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Rebuild {
    /// From the seed and the logged blocks above it, up to the last applied block.
    Replayed { blocks: usize },
    /// The seed is above the last applied block (installed at once while unavailable): blocks up to it are covered.
    Ahead,
    /// The log does not hold every block above the seed (a grid height after it started the log over, or an overflow);
    /// the state is kept as it was.
    LogDoesNotReach,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MarksInstallReport {
    pub(crate) previous_seed: Option<u64>,
    pub(crate) check: MarksCheck,
    pub(crate) rebuild: Rebuild,
    /// Logged inputs that break the contract once replayed onto the new seed (a coin listed since the previous seed,
    /// whose units only the new universe gives), by stream; each is a loss up to its block.
    pub(crate) errors: Vec<(Stream, String)>,
}

/// The main-dex node price state after one applied block, for the local marks (design 000454 r1 1.3): every coin the
/// replay claims, the checkpoint the state was rebuilt from, and the block's height and time.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NativePriceSnapshot {
    pub(crate) seed_height: u64,
    pub(crate) height: u64,
    pub(crate) block_time_ms: u64,
    pub(crate) coins: FxHashMap<String, CoinPriceState>,
}

/// The marks of one height, as the account view reads them (design 000442 r1 1): the node marks of the applied height,
/// or a checkpoint's stored marks for the row check at an anchor.
pub(crate) trait MarksAt {
    fn height(&self) -> u64;
    /// The coin's mark in price units (10^-(6 - szDecimals)); `None` when there is none to claim: a main-dex coin
    /// marked unsupported, a coin without a stored mark or outside the universe.
    fn mark(&self, coin: &str) -> Option<u64>;
}

impl MarksAt for MarkState {
    fn height(&self) -> u64 {
        self.height
    }

    fn mark(&self, coin: &str) -> Option<u64> {
        match self.prices.asset(coin) {
            Some(asset) => self.prices.mark(asset),
            None => self.hip3.get(coin)?.mark.map(|mark| mark.px),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MarksStatus {
    pub(crate) availability: Result<u64, Unavailable>,
    pub(crate) seed_height: Option<u64>,
    pub(crate) logged_inputs: usize,
}

#[derive(Debug)]
pub(crate) struct NodeMarks {
    grid: CheckpointGrid,
    /// Highest completed block handed over.
    applied: Option<u64>,
    streams: MarkStreams,
    state: Option<MarkState>,
    /// The trusted state at the latest grid height reached since the last install.
    snapshot: Option<MarkState>,
    log: InputLog,
    /// The newest checkpoint whose seed failed; cleared by a newer seed once it is installed.
    seed_failure: Option<(u64, String)>,
    /// Permanent: an input thread or the poller is gone (`AccountBook::stop`'s reason).
    stopped: Option<String>,
    /// Ambiguous cuts still listed before the next install.
    ambiguous_samples_left: usize,
    /// Unknown IOC item-pairing evidence shares the seed lifetime, but has its own quota.
    ioc_pairing_samples_left: usize,
}

impl NodeMarks {
    /// `started_ms`: nothing written before it was read.
    pub(crate) const fn new(grid: CheckpointGrid, started_ms: u64) -> Self {
        Self {
            grid,
            applied: None,
            streams: MarkStreams::starting(started_ms),
            state: None,
            snapshot: None,
            log: InputLog::new(MAX_LOG_INPUTS),
            seed_failure: None,
            stopped: None,
            ambiguous_samples_left: MAX_CHECK_SAMPLES,
            ioc_pairing_samples_left: pairing::MAX_PAIRING_SAMPLES,
        }
    }

    /// Called once at ingress, never during seed replay. A failed or unreachable seed does not renew the quota.
    pub(crate) fn sample_ioc_pairings(&mut self, diagnostics: &mut pairing::PairingDiagnostics) {
        diagnostics.samples.truncate(self.ioc_pairing_samples_left);
        self.ioc_pairing_samples_left -= diagnostics.samples.len();
    }

    /// Height of the trusted marks.
    pub(crate) fn availability(&self) -> Result<u64, Unavailable> {
        if let Some(reason) = &self.stopped {
            return Err(Unavailable::Stopped(reason.clone()));
        }
        if let Some((height, error)) = &self.seed_failure {
            return Err(Unavailable::CheckpointFailed { height: *height, error: error.clone() });
        }
        let state = self.state.as_ref().ok_or(Unavailable::Initializing)?;
        self.streams.check(state.seed)?;
        Ok(state.height)
    }

    /// The trusted state's main-dex prices for the local marks; why not while the marks are unavailable.
    pub(crate) fn price_snapshot(&self) -> Result<NativePriceSnapshot, Unavailable> {
        self.availability()?;
        let state = self.state.as_ref().ok_or(Unavailable::Initializing)?;
        let coins = state.prices.price_states().map(|(coin, price)| (coin.to_string(), price)).collect();
        Ok(NativePriceSnapshot { seed_height: state.seed.height, height: state.height, block_time_ms: state.time_ms, coins })
    }

    /// The marks while they are trusted, of their height.
    pub(crate) fn at(&self) -> Option<&dyn MarksAt> {
        self.availability().ok()?;
        let state: &MarkState = self.state.as_ref()?;
        Some(state)
    }

    pub(crate) fn status(&self) -> MarksStatus {
        MarksStatus {
            availability: self.availability(),
            seed_height: self.state.as_ref().map(|state| state.seed.height),
            logged_inputs: self.log.inputs,
        }
    }

    /// The marks' inputs of completed block `height` (blocks come in height order, each once).
    pub(crate) fn apply_block(&mut self, height: u64, inputs: MarkInputs) -> MarksBlockReport {
        let mut report = MarksBlockReport::default();
        self.applied = Some(height);
        self.streams.record_read(height, &inputs);
        let block = BlockInputs::of(height, inputs, &mut report);
        if let Some(state) = self.state.as_mut().filter(|state| state.height < height) {
            state.apply(height, &block, &mut report);
        }
        report.ambiguous_samples.truncate(self.ambiguous_samples_left);
        self.ambiguous_samples_left -= report.ambiguous_samples.len();
        if !self.log.push(height, block) {
            report.log_overflowed = true;
            for stream in MARK_STREAMS {
                self.record_lost_block(stream, height);
            }
        }
        for (stream, _) in &report.errors {
            self.record_lost_block(*stream, height);
        }
        if self.grid.contains(height) && self.state.as_ref().is_none_or(|state| state.seed.height < height) {
            self.snapshot = self.trusted_state_at(height).cloned();
            self.log.restart(height);
        }
        report
    }

    fn trusted_state_at(&self, height: u64) -> Option<&MarkState> {
        self.availability().ok().filter(|available| *available == height)?;
        self.state.as_ref()
    }

    /// A loss of `stream` up to `block`; a stream the marks do not read leaves them as they are.
    pub(crate) fn record_lost_block(&mut self, stream: Stream, block: u64) {
        if let Some(progress) = self.streams.get_mut(stream) {
            progress.record_lost_block(block);
        }
    }

    /// A loss of `stream` of unknown blocks, detected at `detected_ms`; a stream the marks do not read leaves them as
    /// they are.
    pub(crate) fn record_unknown_loss(&mut self, stream: Stream, detected_ms: u64) {
        if let Some(progress) = self.streams.get_mut(stream) {
            progress.record_unknown_loss(detected_ms);
        }
    }

    /// Permanent until restart: an input the marks need is gone.
    pub(crate) fn stop(&mut self, reason: String) {
        self.stopped.get_or_insert(reason);
    }

    /// The checkpoint at `height` has no usable seed: withholds the marks until a newer seed installs.
    pub(crate) fn record_seed_failure(&mut self, height: u64, error: String) {
        self.seed_failure = Some((height, error));
    }

    /// Whether a seed at `height` installs now: once every block up to it is applied, so its check finds the snapshot;
    /// at once while the marks are unavailable, which has nothing to check and recovers soonest.
    pub(crate) fn seed_ready(&self, height: u64) -> bool {
        self.availability().is_err() || self.applied.is_some_and(|applied| applied >= height)
    }

    /// Installs a newer seed: checks the snapshot at its height, then rebuilds from it.
    pub(crate) fn install(&mut self, seed: MarkSeed) -> MarksInstallReport {
        let height = seed.height;
        let previous_seed = self.state.as_ref().map(|state| state.seed.height);
        assert!(previous_seed.is_none_or(|previous| previous < height), "seed {height} is not newer than {previous_seed:?}");
        let check = match self.snapshot.take_if(|snapshot| snapshot.height <= height) {
            Some(snapshot) if snapshot.height == height => MarksCheck::Checked {
                hip3: check_hip3(&snapshot.hip3, &seed.hip3),
                books: snapshot.books.check_against(&seed.native),
                prices: snapshot.prices.check_against(&seed.native),
            },
            _ => MarksCheck::Skipped("noSnapshot"),
        };
        let mut errors = Vec::new();
        let rebuild = self.rebuild(seed, &mut errors);
        // only a seed the state now stands on clears an older failure; one the log could not reach changed nothing
        if rebuild != Rebuild::LogDoesNotReach && self.seed_failure.as_ref().is_some_and(|(failed, _)| *failed < height) {
            self.seed_failure = None;
        }
        MarksInstallReport { previous_seed, check, rebuild, errors }
    }

    fn rebuild(&mut self, seed: MarkSeed, errors: &mut Vec<(Stream, String)>) -> Rebuild {
        let height = seed.height;
        let rebuild = match self.applied {
            Some(applied) if applied > height => {
                if !self.log.reaches(height) {
                    return Rebuild::LogDoesNotReach;
                }
                let mut state = MarkState::seeded(seed);
                let mut replayed_errors = Vec::new();
                let mut blocks = 0;
                for (block, inputs) in self.log.after(height) {
                    // coins outside the universe were counted when the block was applied
                    let mut replayed = MarksBlockReport::default();
                    state.apply(*block, inputs, &mut replayed);
                    replayed_errors.extend(replayed.errors.into_iter().map(|error| (*block, error)));
                    blocks += 1;
                }
                for (block, (stream, error)) in replayed_errors {
                    self.record_lost_block(stream, block);
                    errors.push((stream, error));
                }
                state.height = applied;
                self.state = Some(state);
                Rebuild::Replayed { blocks }
            }
            Some(applied) if applied == height => {
                self.state = Some(MarkState::seeded(seed));
                Rebuild::Replayed { blocks: 0 }
            }
            _ => {
                self.state = Some(MarkState::seeded(seed));
                Rebuild::Ahead
            }
        };
        self.log.stop();
        self.ambiguous_samples_left = MAX_CHECK_SAMPLES;
        self.ioc_pairing_samples_left = pairing::MAX_PAIRING_SAMPLES;
        rebuild
    }
}

/// Every HIP-3 coin of the checkpoint against the state.
fn check_hip3(replayed: &FxHashMap<String, Hip3Coin>, checkpoint: &FxHashMap<String, Hip3Coin>) -> Hip3Check {
    let mut check = Hip3Check::default();
    for (coin, stored) in checkpoint {
        let Some(state) = replayed.get(coin) else {
            check.unknown += 1;
            continue;
        };
        if state.mark == stored.mark {
            check.equal += 1;
            continue;
        }
        check.differ += 1;
        if check.samples.len() < MAX_CHECK_SAMPLES {
            check.samples.push(Hip3Mismatch { coin: coin.clone(), replayed: state.mark, checkpoint: stored.mark });
        }
    }
    check
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::accounts::{
        book::{CLOCK_MARGIN_MS, LossBound},
        marks::{
            book::{BookMismatch, tests::diff_line},
            seed::{RestingOrder, SpotReferenceBook},
        },
        test_support::{HIP3_LINE_TIME_NS, SEED_TIME_MS, hip3_line as line, marks_seed, status_line},
    };
    use alloy::primitives::Address;
    use crate::order_book::Side;

    const GRID: CheckpointGrid = CheckpointGrid { anchor: 1_000, interval: 100 };
    const STARTED_MS: u64 = SEED_TIME_MS - 60_000;

    fn seed(height: u64) -> MarkSeed {
        marks_seed(height)
    }

    fn tsla(marks: &NodeMarks) -> Option<StoredMark> {
        marks.state.as_ref().unwrap().hip3["xyz:TSLA"].mark
    }

    /// A mark written by `line`.
    const fn written(px: u64) -> StoredMark {
        StoredMark { px, time_ns: HIP3_LINE_TIME_NS }
    }

    /// A block whose `replica_cmds` line holds no price or book action: no stream but `replica_cmds` read.
    impl Default for MarkInputs {
        fn default() -> Self {
            let mark_actions = Ok(MarkActions { height: 0, time_ns: 0, actions: Vec::new() });
            Self { hip3_oracle_updates: Vec::new(), book_diffs: Vec::new(), mark_actions, order_statuses: Vec::new(), fills: Vec::new() }
        }
    }

    /// Only HIP-3 lines.
    fn hip3_only(lines: Vec<Batch<OracleUpdateEvent>>) -> MarkInputs {
        MarkInputs { hip3_oracle_updates: lines, ..MarkInputs::default() }
    }

    /// A fill and a status that touch nothing the tests check: the fills and statuses streams are read.
    fn quiet_fill_and_status(height: u64) -> (Vec<BlockFill>, Vec<Batch<NodeDataOrderStatus>>) {
        let fill = BlockFill { coin: "PUMP".into(), px: "0.004".into(), sz: "1".into(), crossed: true, oid: 1, tid: height, twap: false };
        (vec![fill], vec![status_line(height, "triggered", Address::repeat_byte(1), 1, None)])
    }

    /// The prices of the fixture's seed against themselves: every field equal on both coins.
    fn seed_prices() -> PricesCheck {
        let native = seed(0).native;
        NativePrices::seeded(&native).check_against(&native)
    }

    /// A PUMP order placed and removed in block `height`: the books read a diff and stay as they were.
    fn no_op_diffs(height: u64) -> Vec<Batch<NodeDataOrderDiff>> {
        vec![
            diff_line(height, "PUMP", height, "B", "0.004", r#"{"new":{"sz":"1"}}"#),
            diff_line(height, "PUMP", height, "B", "0.004", r#""remove""#),
        ]
    }

    /// The books seeded by the fixture: BTC's two orders, PUMP empty.
    fn seed_books() -> BooksCheck {
        BooksCheck { books_equal: 2, orders_equal: 2, ..BooksCheck::default() }
    }

    /// Marks applied through `blocks`, with xyz:TSLA lines at the given heights; those blocks also carry `no_op_diffs`
    /// and `quiet_fill_and_status`, so every stream is read with the HIP-3 one.
    fn run(marks: &mut NodeMarks, blocks: std::ops::RangeInclusive<u64>, lines: &[(u64, &str)]) {
        for height in blocks {
            let hip3_oracle_updates: Vec<_> =
                lines.iter().filter(|(at, _)| *at == height).map(|(at, px)| line(*at, &[("xyz:TSLA", px)])).collect();
            let mut inputs = MarkInputs::default();
            if !hip3_oracle_updates.is_empty() {
                (inputs.fills, inputs.order_statuses) = quiet_fill_and_status(height);
                inputs.book_diffs = no_op_diffs(height);
                inputs.hip3_oracle_updates = hip3_oracle_updates;
            }
            assert_eq!(marks.apply_block(height, inputs), MarksBlockReport::default(), "block {height}");
        }
    }

    /// Marks trusted at 1001: seeded at 995, every stream read past it.
    pub(crate) fn trusted_marks() -> NodeMarks {
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        run(&mut marks, 990..=995, &[]);
        marks.install(seed(995));
        run(&mut marks, 996..=1_001, &[(997, "360.0")]);
        assert_eq!(marks.availability(), Ok(1_001));
        marks
    }

    #[test]
    fn the_grid_holds_its_anchor_and_every_interval_both_ways() {
        assert!([800, 1_000, 1_100, 2_000].into_iter().all(|height| GRID.contains(height)));
        assert!(![999, 1_001, 1_050].into_iter().any(|height| GRID.contains(height)));
    }

    /// Revision 2: a seed waits for its blocks; the snapshot at its height is checked against it, then the state is
    /// rebuilt from it and the blocks logged above it.
    #[test]
    fn a_seed_at_a_grid_height_checks_the_snapshot_and_rebuilds_from_the_log() {
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        run(&mut marks, 990..=995, &[]);
        assert!(marks.seed_ready(1_000), "unavailable before the first seed: installs at once");
        let first = marks.install(seed(995));
        let expected = MarksInstallReport {
            previous_seed: None,
            check: MarksCheck::Skipped("noSnapshot"),
            rebuild: Rebuild::Replayed { blocks: 0 },
            errors: Vec::new(),
        };
        assert_eq!(first, expected);
        assert_eq!(marks.availability(), Err(Unavailable::Initializing), "no HIP-3 line read yet");
        run(&mut marks, 996..=1_003, &[(997, "360.0"), (1_002, "361.5")]);
        assert_eq!(marks.availability(), Ok(1_003));
        assert_eq!(tsla(&marks), Some(written(361_500)));
        assert!(!marks.seed_ready(1_100), "trusted: waits for block 1100");
        // checkpoint 1000 holds what the stream wrote by then
        let mut checkpoint = seed(1_000);
        checkpoint.hip3.get_mut("xyz:TSLA").unwrap().mark = Some(written(360_000));
        let report = marks.install(checkpoint);
        let hip3 = Hip3Check { equal: 2, ..Hip3Check::default() };
        assert_eq!(report.check, MarksCheck::Checked { hip3, books: seed_books(), prices: seed_prices() });
        assert_eq!(report.rebuild, Rebuild::Replayed { blocks: 3 }, "blocks 1001 to 1003 replayed onto the seed");
        assert_eq!((marks.availability(), tsla(&marks)), (Ok(1_003), Some(written(361_500))));
    }

    #[test]
    fn a_differing_snapshot_is_counted_and_the_seed_corrects_the_state() {
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        run(&mut marks, 995..=995, &[]);
        marks.install(seed(995));
        run(&mut marks, 996..=1_001, &[(997, "360.0")]);
        let report = marks.install(seed(1_000));
        let mismatch = Hip3Mismatch {
            coin: "xyz:TSLA".to_string(),
            replayed: Some(written(360_000)),
            checkpoint: Some(StoredMark { px: 358_320, time_ns: 1_792_391_842_000_000_000 }),
        };
        let hip3 = Hip3Check { equal: 1, differ: 1, unknown: 0, samples: vec![mismatch] };
        assert_eq!(report.check, MarksCheck::Checked { hip3, books: seed_books(), prices: seed_prices() });
        assert_eq!(tsla(&marks).map(|mark| mark.px), Some(358_320), "the checkpoint's value, nothing logged above it");
    }

    #[test]
    fn a_loss_withholds_the_marks_until_a_covering_seed_rebuilds_them() {
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        run(&mut marks, 995..=995, &[]);
        marks.install(seed(995));
        run(&mut marks, 996..=1_010, &[(996, "359.0")]);
        marks.record_lost_block(Stream::MiscEvents, 1_005);
        assert_eq!(marks.availability(), Ok(1_010), "the marks do not read the misc events");
        marks.record_lost_block(Stream::Hip3OracleUpdates, 1_005);
        let lost = Unavailable::DataLoss { stream: Stream::Hip3OracleUpdates, bound: LossBound::Block(1_005) };
        assert_eq!(marks.availability(), Err(lost));
        assert!(marks.seed_ready(1_100), "unavailable: a seed installs at once");
        run(&mut marks, 1_011..=1_012, &[(1_012, "362.0")]);
        let report = marks.install(seed(1_010));
        assert_eq!((report.check, report.rebuild), (MarksCheck::Skipped("noSnapshot"), Rebuild::Replayed { blocks: 2 }));
        assert_eq!((marks.availability(), tsla(&marks)), (Ok(1_012), Some(written(362_000))));
    }

    /// The state at a grid height is kept for the check only while trusted: an untrusted one would be checked as if it
    /// were the node's.
    #[test]
    fn an_untrusted_state_at_a_grid_height_is_not_checked() {
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        run(&mut marks, 995..=996, &[(996, "359.0")]);
        marks.install(seed(995));
        marks.record_lost_block(Stream::Hip3OracleUpdates, 998);
        run(&mut marks, 997..=1_001, &[]);
        let report = marks.install(seed(1_000));
        assert_eq!((report.check, report.rebuild), (MarksCheck::Skipped("noSnapshot"), Rebuild::Replayed { blocks: 1 }));
        assert_eq!(marks.availability(), Ok(1_001), "the seed covers the loss at 998");
    }

    #[test]
    fn a_seed_above_the_applied_blocks_covers_them_when_they_come() {
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        run(&mut marks, 990..=990, &[]);
        assert_eq!(marks.install(seed(995)).rebuild, Rebuild::Ahead);
        run(&mut marks, 991..=996, &[(993, "1.0"), (996, "359.5")]);
        assert_eq!((marks.availability(), tsla(&marks)), (Ok(996), Some(written(359_500))), "993 is the seed's, 996 applies");
    }

    #[test]
    fn a_grid_height_after_the_seed_starts_the_log_over() {
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        run(&mut marks, 990..=990, &[]);
        marks.install(seed(990));
        run(&mut marks, 991..=1_105, &[(992, "359.0"), (1_050, "360.0"), (1_101, "361.0")]);
        let late = marks.install(seed(1_000));
        assert_eq!((late.check, late.rebuild), (MarksCheck::Skipped("noSnapshot"), Rebuild::LogDoesNotReach), "1100 replaced it");
        assert_eq!(tsla(&marks), Some(written(361_000)), "kept as it was");
        let report = marks.install(seed(1_100));
        assert!(matches!(report.check, MarksCheck::Checked { hip3: Hip3Check { differ: 1, .. }, .. }), "{:?}", report.check);
        assert_eq!(report.rebuild, Rebuild::Replayed { blocks: 5 });
    }

    #[test]
    fn a_written_price_breaking_the_contract_is_a_loss_and_unknown_coins_are_counted() {
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        run(&mut marks, 995..=995, &[]);
        marks.install(seed(995));
        run(&mut marks, 996..=996, &[(996, "359.0")]);
        let report = marks.apply_block(997, hip3_only(vec![line(997, &[("xyz:NEW", "1.0"), ("xyz:TSLA", "359.0001")])]));
        assert_eq!(report.unknown_coin_updates, 1);
        assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
        assert!(report.errors[0].0 == Stream::Hip3OracleUpdates && report.errors[0].1.contains("xyz:TSLA"), "{:?}", report.errors);
        let lost = Unavailable::DataLoss { stream: Stream::Hip3OracleUpdates, bound: LossBound::Block(997) };
        assert_eq!(marks.availability(), Err(lost));
    }

    /// A coin listed after the seed: its updates are skipped while applied, logged, and replayed once a seed knows it;
    /// one the new universe cannot convert is a loss at its block.
    #[test]
    fn a_coin_listed_since_the_seed_is_replayed_once_a_seed_knows_it() {
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        run(&mut marks, 995..=995, &[]);
        marks.install(seed(995));
        run(&mut marks, 996..=1_000, &[(996, "359.0")]);
        assert_eq!(marks.apply_block(1_001, hip3_only(vec![line(1_001, &[("xyz:NEW", "2.5")])])).unknown_coin_updates, 1);
        assert_eq!(marks.apply_block(1_002, hip3_only(vec![line(1_002, &[("xyz:NEW", "2.75")])])).unknown_coin_updates, 1);
        let mut listing = seed(1_000);
        listing.hip3.insert("xyz:NEW".to_string(), Hip3Coin { size_decimals: 4, mark: None });
        let report = marks.install(listing);
        assert_eq!((report.rebuild, report.errors.as_slice()), (Rebuild::Replayed { blocks: 2 }, [].as_slice()));
        assert_eq!(marks.state.as_ref().unwrap().hip3["xyz:NEW"].mark, Some(written(275)), "block 1002, at 2 decimals");
        run(&mut marks, 1_003..=1_100, &[]);
        marks.apply_block(1_101, hip3_only(vec![line(1_101, &[("xyz:NEW", "2.75")])]));
        let mut coarser = seed(1_100);
        coarser.hip3.insert("xyz:NEW".to_string(), Hip3Coin { size_decimals: 5, mark: None });
        let report = marks.install(coarser);
        assert_eq!(report.errors.len(), 1, "2.75 has more than one decimal at szDecimals 5");
        let lost = Unavailable::DataLoss { stream: Stream::Hip3OracleUpdates, bound: LossBound::Block(1_101) };
        assert_eq!(marks.availability(), Err(lost));
    }

    /// Revision 2's log bound: blocks are logged only from a grid height whose seed is not installed yet until that
    /// seed installs; nothing before the first grid height or between an install and the next grid height.
    #[test]
    fn the_log_runs_only_from_a_grid_height_to_its_install() {
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        run(&mut marks, 990..=999, &[(992, "359.0")]);
        assert_eq!((marks.log.base, marks.log.inputs), (None, 0), "nothing before the first grid height");
        run(&mut marks, 1_000..=1_003, &[(1_002, "359.5")]);
        let logged = "the times of 1001 to 1003; at 1002 an update, two diffs, a fill, a status";
        assert_eq!((marks.log.base, marks.log.inputs), (Some(1_000), 8), "{logged}");
        marks.install(seed(1_000));
        run(&mut marks, 1_004..=1_099, &[(1_050, "360.0")]);
        assert_eq!((marks.log.base, marks.log.inputs), (None, 0), "the installed seed covers what a later one needs");
        run(&mut marks, 1_100..=1_101, &[(1_101, "360.5")]);
        assert_eq!((marks.log.base, marks.log.inputs), (Some(1_100), 6), "1101: its time and five inputs");
        let mut ahead = NodeMarks::new(GRID, STARTED_MS);
        run(&mut ahead, 1_098..=1_098, &[]);
        assert_eq!(ahead.install(seed(1_100)).rebuild, Rebuild::Ahead);
        run(&mut ahead, 1_099..=1_101, &[(1_101, "360.5")]);
        assert_eq!(ahead.log.base, None, "the grid height of an installed seed starts no log");
    }

    #[test]
    fn the_start_is_a_loss_until_a_seed_past_it() {
        let mut marks = NodeMarks::new(GRID, SEED_TIME_MS + 1_000);
        run(&mut marks, 995..=995, &[]);
        marks.install(seed(995));
        run(&mut marks, 996..=996, &[(996, "359.0")]);
        let started = Unavailable::DataLoss { stream: Stream::Fills, bound: LossBound::WrittenBy(SEED_TIME_MS + 1_000) };
        assert_eq!(marks.availability(), Err(started));
        let covering = 1_000 + CLOCK_MARGIN_MS;
        run(&mut marks, 997..=covering + 1, &[]);
        marks.install(seed(covering + 1));
        assert_eq!(marks.availability(), Ok(covering + 1));
    }

    #[test]
    fn ioc_evidence_quota_only_renews_when_a_seed_actually_rebuilds() {
        use self::actions::tests::{ok, replica_line};
        use crate::accounts::replica::ReplicaLine;
        let order = r#"{"a":41,"b":true,"p":"0.67331","s":"34.9","r":false,"t":{"limit":{"tif":"Ioc"}}}"#;
        let action = format!(r#"{{"type":"order","orders":[{order}]}}"#);
        let response = ok(r#"{"type":"order","data":{"statuses":[{"error":"unknown"},{"resting":{"oid":99}}]}}"#);
        let diagnostics = |count| {
            let pairs = vec![(action.as_str(), response.as_str()); count];
            let line = replica_line(995, &[&pairs]);
            let replica = ReplicaLine::parse(&line).unwrap();
            pairing::PairingDiagnostics::collect(&MarkActions::of(&replica).unwrap(), &replica, &line)
        };
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        let mut known = pairing::PairingDiagnostics { batch_rejected: 100, ..Default::default() };
        marks.sample_ioc_pairings(&mut known);
        assert_eq!(marks.ioc_pairing_samples_left, 20, "known rejections consume no quota");
        let mut first = diagnostics(25);
        marks.sample_ioc_pairings(&mut first);
        assert_eq!((first.unknown, first.samples.len()), (25, 20), "bounded even before first seed");
        let mut later = diagnostics(1);
        marks.sample_ioc_pairings(&mut later);
        assert_eq!((later.unknown, later.samples.len()), (1, 0), "counts continue across blocks");
        run(&mut marks, 995..=995, &[]);
        assert_eq!(marks.install(seed(995)).rebuild, Rebuild::Replayed { blocks: 0 });
        let mut renewed = diagnostics(1);
        marks.sample_ioc_pairings(&mut renewed);
        assert_eq!(renewed.samples.len(), 1);
        let mut remaining = diagnostics(25);
        marks.sample_ioc_pairings(&mut remaining);
        assert_eq!(remaining.samples.len(), 19, "one quota across multiple inputs");
        marks.record_lost_block(Stream::BlockActions, 997);
        marks.record_seed_failure(1_000, "seed failed".to_string());
        run(&mut marks, 996..=1_201, &[]);
        assert_eq!(marks.install(seed(1_100)).rebuild, Rebuild::LogDoesNotReach);
        let mut exhausted = diagnostics(25);
        marks.sample_ioc_pairings(&mut exhausted);
        assert_eq!(exhausted.samples.len(), 0, "losses and unsuccessful seeds do not reset quota");
        assert_eq!(marks.install(seed(1_200)).rebuild, Rebuild::Replayed { blocks: 1 });
        let mut fresh = diagnostics(25);
        marks.sample_ioc_pairings(&mut fresh);
        assert_eq!(fresh.samples.len(), 20, "successful rebuild starts a new interval");
    }

    #[test]
    fn a_seed_failure_withholds_the_marks_until_a_newer_seed() {
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        run(&mut marks, 995..=996, &[(996, "359.0")]);
        marks.install(seed(995));
        marks.record_seed_failure(1_000, "main dex: BTC (asset 0): no book".to_string());
        assert!(matches!(marks.availability(), Err(Unavailable::CheckpointFailed { height: 1_000, .. })));
        run(&mut marks, 997..=1_010, &[]);
        marks.install(seed(1_010));
        assert_eq!(marks.availability(), Ok(1_010));
    }

    /// Review 000440 r1 P2-1: a seed the log cannot reach is not installed, so it leaves an older seed failure in place;
    /// the next one that rebuilds clears it.
    #[test]
    fn a_seed_not_installed_keeps_an_older_seed_failure() {
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        run(&mut marks, 995..=996, &[(996, "359.0")]);
        marks.install(seed(995));
        marks.record_seed_failure(1_000, "main dex: BTC (asset 0): no book".to_string());
        run(&mut marks, 997..=1_201, &[]);
        assert_eq!(marks.install(seed(1_100)).rebuild, Rebuild::LogDoesNotReach, "the log starts at 1200");
        assert!(matches!(marks.availability(), Err(Unavailable::CheckpointFailed { height: 1_000, .. })));
        assert_eq!(marks.install(seed(1_200)).rebuild, Rebuild::Replayed { blocks: 1 });
        assert_eq!(marks.availability(), Ok(1_201));
    }

    /// Review 000440 r1 P2-2: a stop holds through later seeds.
    #[test]
    fn a_stop_holds_until_restart() {
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        run(&mut marks, 995..=996, &[(996, "359.0")]);
        marks.install(seed(995));
        marks.stop("HIP-3 oracle updates watcher thread exited".to_string());
        run(&mut marks, 997..=1_001, &[]);
        marks.install(seed(1_000));
        assert_eq!(marks.availability(), Err(Unavailable::Stopped("HIP-3 oracle updates watcher thread exited".to_string())));
    }

    /// Design 000439 r2 batch 2: the books follow the diffs; at a grid height they are kept and checked order by
    /// order against that checkpoint, which then replaces them with the logged diffs above it replayed.
    #[test]
    fn the_books_follow_the_diffs_and_each_checkpoint_checks_them() {
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        run(&mut marks, 995..=995, &[]);
        marks.install(seed(995));
        run(&mut marks, 996..=996, &[(996, "359.0")]);
        let fill = diff_line(997, "BTC", 9, "A", "82870.0", r#"{"update":{"origSz":"0.00005","newSz":"0.00001"}}"#);
        assert_eq!(marks.apply_block(997, MarkInputs { book_diffs: vec![fill], ..MarkInputs::default() }), MarksBlockReport::default());
        run(&mut marks, 998..=1_001, &[]);
        let place = diff_line(1_001, "BTC", 12, "B", "82855.0", r#"{"new":{"sz":"0.00002"}}"#);
        marks.apply_block(1_002, MarkInputs { book_diffs: vec![place], ..MarkInputs::default() });
        let mut checkpoint = seed(1_000);
        checkpoint.native.assets[0].orders[1].remaining = 1;
        let report = marks.install(checkpoint);
        assert!(matches!(&report.check, MarksCheck::Checked { books, .. } if *books == seed_books()), "{:?}", report.check);
        let mut differing = seed(1_100);
        run(&mut marks, 1_003..=1_100, &[]);
        differing.native.assets[0].orders[1].remaining = 2;
        let MarksCheck::Checked { books, .. } = marks.install(differing).check else { panic!("checked at 1100") };
        let mismatch = BookMismatch {
            coin: "BTC".to_string(),
            oid: 9,
            replayed: Some(RestingOrder { oid: 9, side: Side::Ask, limit_px: 828_700, remaining: 1 }),
            checkpoint: Some(RestingOrder { oid: 9, side: Side::Ask, limit_px: 828_700, remaining: 2 }),
        };
        let extra = BookMismatch {
            coin: "BTC".to_string(),
            oid: 12,
            replayed: Some(RestingOrder { oid: 12, side: Side::Bid, limit_px: 828_550, remaining: 2 }),
            checkpoint: None,
        };
        let expected = (1, 1, 1, 1, 1, vec![mismatch, extra]);
        assert_eq!((books.books_equal, books.books_differ, books.orders_equal, books.orders_differ, books.extra, books.samples), expected,
                   "oid 12 rests in the replay only");
        assert_eq!(marks.state.as_ref().unwrap().books.check_against(&seed(1_100).native).books_differ, 1, "the checkpoint's books now");
    }

    #[test]
    fn a_diff_that_does_not_fit_the_books_is_a_loss_of_their_stream() {
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        run(&mut marks, 995..=995, &[]);
        marks.install(seed(995));
        run(&mut marks, 996..=996, &[(996, "359.0")]);
        let stray = diff_line(997, "BTC", 8, "B", "82850.0", r#""remove""#);
        let report = marks.apply_block(997, MarkInputs { book_diffs: vec![stray], ..MarkInputs::default() });
        assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
        assert!(report.errors[0].0 == Stream::BookDiffs && report.errors[0].1.contains("BTC oid 8: no such resting order"), "{:?}", report.errors);
        let lost = Unavailable::DataLoss { stream: Stream::BookDiffs, bound: LossBound::Block(997) };
        assert_eq!(marks.availability(), Err(lost));
        let next = diff_line(998, "BTC", 8, "B", "82850.0", r#""remove""#);
        let quiet = marks.apply_block(998, MarkInputs { book_diffs: vec![next], ..MarkInputs::default() });
        assert_eq!(quiet.errors, [], "broken books take no more diffs until a seed replaces them");
        let unknown = diff_line(999, "LISTED", 1, "B", "1.0", r#""remove""#);
        assert_eq!(marks.apply_block(999, MarkInputs { book_diffs: vec![unknown], ..MarkInputs::default() }).unknown_coin_diffs, 0, "broken");
        run(&mut marks, 1_000..=1_001, &[(1_001, "359.5")]);
        marks.install(seed(1_000));
        assert_eq!(marks.availability(), Ok(1_001));
        let unknown = diff_line(1_002, "LISTED", 1, "B", "1.0", r#""remove""#);
        assert_eq!(marks.apply_block(1_002, MarkInputs { book_diffs: vec![unknown], ..MarkInputs::default() }).unknown_coin_diffs, 1);
    }

    /// Design 000439 r1 section 3: a `SetGlobalAction` reads the book at its place in the block. An order placed before
    /// it moves the bid it reads; one placed after does not.
    #[test]
    fn a_set_global_action_reads_the_book_at_its_place_in_the_block() {
        use self::actions::{BookAnchors, MarkAction, RankedAction};
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        run(&mut marks, 995..=995, &[]);
        marks.install(seed(995));
        let time_ns = 1_792_391_844_000_000_000;
        let placed = |rank, oid| RankedAction {
            rank,
            user: Some(Address::repeat_byte(1)),
            action: MarkAction::Book(BookAnchors { created: vec![oid], ..BookAnchors::default() }),
        };
        let set_global = RankedAction {
            rank: (1, 3, 0),
            user: None,
            action: MarkAction::SetGlobal(vec![Some(("82890.0".to_string(), "82870.0".to_string())), None]),
        };
        let actions = MarkActions { height: 996, time_ns, actions: vec![placed((0, 1, 0), 11), set_global, placed((2, 1, 0), 12)] };
        let inputs = MarkInputs {
            book_diffs: vec![
                diff_line(996, "BTC", 11, "B", "82860.0", r#"{"new":{"sz":"0.001"}}"#),
                diff_line(996, "BTC", 12, "B", "82865.0", r#"{"new":{"sz":"0.001"}}"#),
            ],
            mark_actions: Ok(actions),
            ..MarkInputs::default()
        };
        assert_eq!(marks.apply_block(996, inputs), MarksBlockReport::default());
        let (mark, unsupported) = marks.state.as_ref().unwrap().prices.mark_of(0);
        assert_eq!(unsupported, None);
        // L = median(bid 828600 at the cut, ask 828700, last trade 828600); at the block's end the bid is 828650
        assert_eq!(mark, StoredMark { px: 828_600, time_ns }, "median(828700, Mark cache 828590, L 828600)");
    }

    /// A reference action reads a `psro` coin's spot book at its place in the block: a bid placed before it moves the
    /// midpoint to 82,870.25 (→ 828,702, half to even), one placed after it to 82,870.35 (→ 828,704).
    #[test]
    fn a_reference_action_reads_the_spot_book_at_its_place_in_the_block() {
        use self::actions::{BookAnchors, MarkAction, RankedAction};
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        run(&mut marks, 995..=995, &[]);
        let mut seeded = seed(995);
        let order = |oid, side, limit_px| RestingOrder { oid, side, limit_px, remaining: 100 };
        seeded.native.spot_references.push(SpotReferenceBook {
            asset: 0,
            coin: "@1".to_string(),
            size_decimals: 5,
            price_decimals: 3,
            orders: vec![order(41, Side::Bid, 82_870_000), order(42, Side::Ask, 82_870_400)],
        });
        marks.install(seeded);
        let validator = Address::repeat_byte(0xee);
        let placed = |rank, oid| RankedAction {
            rank,
            user: Some(Address::repeat_byte(1)),
            action: MarkAction::Book(BookAnchors { created: vec![oid], ..BookAnchors::default() }),
        };
        let reference =
            RankedAction { rank: (1, 3, 0), user: Some(validator), action: MarkAction::Reference { oracle: Vec::new(), external: Vec::new() } };
        let actions = MarkActions { height: 996, time_ns: 1_792_391_844_000_000_000, actions: vec![placed((0, 1, 0), 31), reference, placed((2, 1, 0), 32)] };
        let inputs = MarkInputs {
            book_diffs: vec![
                diff_line(996, "@1", 31, "B", "82870.1", r#"{"new":{"sz":"0.001"}}"#),
                diff_line(996, "@1", 32, "B", "82870.3", r#"{"new":{"sz":"0.001"}}"#),
            ],
            mark_actions: Ok(actions),
            ..MarkInputs::default()
        };
        assert_eq!(marks.apply_block(996, inputs), MarksBlockReport::default());
        let prices = &marks.state.as_ref().unwrap().prices;
        assert_eq!(prices.spot_member(0, validator), Some((828_702, 1_792_391_844_000_000_000)));
        assert_eq!(prices.mark_of(0).1, None);
    }

    /// Review 000444 r1 P3: a price action whose cut is not unique (order 13's line is anchored by no action) is counted
    /// in every block and listed while the seed interval's samples last.
    #[test]
    fn an_ambiguous_cut_is_counted_and_listed_while_samples_last() {
        use self::actions::{BookAnchors, MarkAction, RankedAction};
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        run(&mut marks, 995..=995, &[]);
        marks.install(seed(995));
        let placed = |rank, oid| RankedAction {
            rank,
            user: Some(Address::repeat_byte(1)),
            action: MarkAction::Book(BookAnchors { created: vec![oid], ..BookAnchors::default() }),
        };
        let block = |height, oid| {
            let set_global = RankedAction { rank: (1, 3, 0), user: None, action: MarkAction::SetGlobal(Vec::new()) };
            let actions = vec![placed((0, 1, 0), oid), set_global, placed((2, 1, 0), oid + 2)];
            let new = r#"{"new":{"sz":"0.001"}}"#;
            let book_diffs = [oid, oid + 1, oid + 2].map(|placed| diff_line(height, "BTC", placed, "B", "82860.0", new)).to_vec();
            MarkInputs { book_diffs, mark_actions: Ok(MarkActions { height, time_ns: 1_792_391_844_000_000_000, actions }), ..MarkInputs::default() }
        };
        marks.ambiguous_samples_left = 1;
        let first = marks.apply_block(996, block(996, 11));
        assert_eq!((first.ambiguous_cuts, first.ambiguous_samples), (1, vec![AmbiguousCut { rank: (1, 3, 0), lo: 1, hi: 2 }]));
        let second = marks.apply_block(997, block(997, 21));
        assert_eq!((second.ambiguous_cuts, second.ambiguous_samples.len(), second.errors.len()), (1, 0, 0), "counted only");
    }

    #[test]
    fn an_overflowing_log_is_a_loss_of_both_streams() {
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        run(&mut marks, 995..=995, &[]);
        marks.install(seed(995));
        run(&mut marks, 996..=996, &[(996, "359.0")]);
        marks.log = InputLog::new(6);
        marks.log.restart(996);
        run(&mut marks, 997..=997, &[(997, "359.5")]);
        let report = marks.apply_block(998, MarkInputs { book_diffs: no_op_diffs(998), ..MarkInputs::default() });
        assert!(report.log_overflowed, "6 inputs at 997 (its time included), 3 more at 998");
        let seed_at = AnchorInfo { height: 995, time_ms: SEED_TIME_MS + 995 };
        for stream in [Stream::Hip3OracleUpdates, Stream::BookDiffs] {
            let lost = Unavailable::DataLoss { stream, bound: LossBound::Block(998) };
            assert_eq!(marks.streams.get_mut(stream).unwrap().check(stream, seed_at), Err(lost));
        }
        assert_eq!((marks.log.base, marks.log.inputs), (Some(998), 0), "starts over after the block");
    }

    /// Review 000444 r1 P2-1: a block with no input but its time moves BTC's Spot cache into a new bucket (its member is
    /// fresh); a seed of the grid height below it rebuilds the same cache, not the one before the block.
    #[test]
    fn a_rebuild_replays_the_cache_refresh_of_a_block_without_inputs() {
        const T0: u64 = 1_792_391_843_000_000_000;
        const T1: u64 = T0 + 1_000_000_000;
        let mut checkpoint = seed(995);
        checkpoint.time_ms = T0 / 1_000_000;
        for asset in &mut checkpoint.native.assets {
            for cache in [&mut asset.references.spot, &mut asset.references.external_perp, &mut asset.references.mark] {
                cache.last_bucket = T0 / 3_000_000_000;
            }
        }
        let spot = &mut checkpoint.native.assets[0].references.spot;
        spot.median = Some(828_000);
        spot.members = vec![(Address::repeat_byte(1), 828_720, T0)];
        let mut marks = NodeMarks::new(GRID, 0);
        marks.install(checkpoint.clone());
        for stream in [Stream::Fills, Stream::BlockActions, Stream::Hip3OracleUpdates, Stream::BookDiffs, Stream::OrderStatuses] {
            marks.streams.get_mut(stream).unwrap().record_read(1_000);
        }
        let empty = |height, time_ns| MarkInputs {
            mark_actions: Ok(MarkActions { height, time_ns, actions: Vec::new() }),
            ..MarkInputs::default()
        };
        assert_eq!(marks.apply_block(1_000, empty(1_000, T0)).errors, []);
        assert_eq!(marks.apply_block(1_001, empty(1_001, T1)).errors, []);
        let mut refreshed = checkpoint.native.clone();
        refreshed.assets[0].references.spot.median = Some(828_720);
        refreshed.assets[0].references.spot.last_bucket = T1 / 3_000_000_000;
        let spot_cache = |marks: &NodeMarks| {
            let check = marks.state.as_ref().unwrap().prices.check_against(&refreshed);
            check.fields.into_iter().find(|(field, _, _)| *field == "spotCache")
        };
        assert_eq!(spot_cache(&marks), Some(("spotCache", 2, 0)), "the block's time refreshed the cache");
        checkpoint.height = 1_000;
        let report = marks.install(checkpoint);
        assert_eq!(report.rebuild, Rebuild::Replayed { blocks: 1 });
        assert_eq!((marks.availability(), spot_cache(&marks)), (Ok(1_001), Some(("spotCache", 2, 0))));
    }

    #[test]
    fn an_overflowing_log_is_a_loss_and_starts_over() {
        let mut log = InputLog::new(2);
        log.restart(9);
        let update = Hip3MarkUpdate { coin: "xyz:TSLA".to_string(), px: "1.0".to_string(), time_ns: 1 };
        assert!(log.push(10, BlockInputs { hip3: vec![update.clone(); 2], ..BlockInputs::default() }));
        assert!(!log.push(11, BlockInputs { hip3: vec![update], ..BlockInputs::default() }), "one more overflows");
        assert_eq!((log.base, log.inputs, log.reaches(10), log.reaches(11)), (Some(11), 0, false, true));
    }
}
