//! The node's marks, block by block (design 000439 r2). HIP-3 coins come from the node's oracle update stream, which
//! writes each mark the node applies (research `hl_node_mark_state_20261019` "HIP-3：直接读节点的流": 930/930 stored
//! marks and times equal the last written ones). The main dex takes a per-action replay in batch 2; its first part
//! (here) keeps the main dex's books order by order from the seed and the node's raw book diffs.
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

pub(crate) mod book;
pub(crate) mod decimal;
pub(crate) mod seed;

use rustc_hash::FxHashMap;

use self::{
    book::{BookDiff, BooksCheck, NativeBooks},
    seed::{Hip3Coin, MarkSeed, StoredMark, exact_price_units},
};
use super::{
    book::{AnchorInfo, Stream, StreamProgress, Unavailable},
    checkpoint::node_time_ns,
};
use crate::types::node_data::{Batch, NodeDataOrderDiff, OracleUpdateEvent};

/// Logged inputs (HIP-3 mark updates and main-dex diffs) at most. About 7,400 diffs and 60 updates are written a second
/// (2026-10-20 11 UTC); the log spans a grid height to that checkpoint's install (~13 s on bm, ~100k inputs), so this
/// is ~4.5 minutes of a checkpoint late or skipped, not a normal state.
pub(crate) const MAX_LOG_INPUTS: usize = 2_000_000;
/// Mismatches listed per check (all are counted).
const MAX_CHECK_SAMPLES: usize = 20;

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
#[derive(Debug, Default)]
pub(crate) struct MarkInputs {
    pub(crate) hip3_oracle_updates: Vec<Batch<OracleUpdateEvent>>,
    /// The main dex's diffs, in the node's execution order.
    pub(crate) book_diffs: Vec<Batch<NodeDataOrderDiff>>,
}

/// One block's mark inputs as logged and applied.
#[derive(Debug, Default)]
struct BlockInputs {
    hip3: Vec<Hip3MarkUpdate>,
    diffs: Vec<BookDiff>,
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
        out
    }

    const fn len(&self) -> usize {
        self.hip3.len() + self.diffs.len()
    }
}

/// The marks at `height`.
#[derive(Debug, Clone)]
struct MarkState {
    /// The checkpoint it was rebuilt from.
    seed: AnchorInfo,
    /// Highest block applied; the seed's height while no later block is.
    height: u64,
    hip3: FxHashMap<String, Hip3Coin>,
    books: NativeBooks,
    /// A diff did not fit the books: they stay as they were until a seed replaces them (the loss withholds the marks).
    books_broken: bool,
}

impl MarkState {
    /// The main dex's price state other than its books is not kept before the per-action replay.
    fn seeded(seed: MarkSeed) -> Self {
        let MarkSeed { height, time_ms, native, hip3 } = seed;
        let books = NativeBooks::seeded(native);
        Self { seed: AnchorInfo { height, time_ms }, height, hip3, books, books_broken: false }
    }

    /// Applies block `height`; coins outside the seed's universe (listed since) are counted and skipped, a value that
    /// does not convert or a diff that does not fit breaks the contract.
    fn apply(&mut self, height: u64, inputs: &BlockInputs, report: &mut MarksBlockReport) {
        for update in &inputs.hip3 {
            let Some(coin) = self.hip3.get_mut(&update.coin) else {
                report.unknown_coin_updates += 1;
                continue;
            };
            match exact_price_units(&update.px, coin.size_decimals) {
                Ok(px) => coin.mark = Some(StoredMark { px, time_ns: update.time_ns }),
                Err(err) => report.errors.push((Stream::Hip3OracleUpdates, format!("block {height} {}: {err}", update.coin))),
            }
        }
        for diff in &inputs.diffs {
            if self.books_broken {
                break;
            }
            match self.books.apply(diff) {
                Ok(true) => {}
                Ok(false) => report.unknown_coin_diffs += 1,
                Err(err) => {
                    report.errors.push((Stream::BookDiffs, format!("block {height} {err}")));
                    self.books_broken = true;
                }
            }
        }
        self.height = height;
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

/// The progress of the streams the marks read.
#[derive(Debug)]
struct MarkStreams {
    hip3_oracle_updates: StreamProgress,
    book_diffs: StreamProgress,
}

impl MarkStreams {
    const fn get_mut(&mut self, stream: Stream) -> Option<&mut StreamProgress> {
        match stream {
            Stream::Hip3OracleUpdates => Some(&mut self.hip3_oracle_updates),
            Stream::BookDiffs => Some(&mut self.book_diffs),
            Stream::Fills | Stream::MiscEvents | Stream::BlockActions => None,
        }
    }

    fn check(&self, seed: AnchorInfo) -> Result<(), Unavailable> {
        self.hip3_oracle_updates.check(Stream::Hip3OracleUpdates, seed)?;
        self.book_diffs.check(Stream::BookDiffs, seed)?;
        Ok(())
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
    /// The log overflowed with this block, a loss of both streams up to it.
    pub(crate) log_overflowed: bool,
}

/// The snapshot at a checkpoint's height against the checkpoint (design 000439 r2 revision 2).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MarksCheck {
    Checked { hip3: Hip3Check, books: BooksCheck },
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
}

impl NodeMarks {
    /// `started_ms`: nothing written before it was read.
    pub(crate) const fn new(grid: CheckpointGrid, started_ms: u64) -> Self {
        let streams = MarkStreams {
            hip3_oracle_updates: StreamProgress::starting(started_ms),
            book_diffs: StreamProgress::starting(started_ms),
        };
        Self {
            grid,
            applied: None,
            streams,
            state: None,
            snapshot: None,
            log: InputLog::new(MAX_LOG_INPUTS),
            seed_failure: None,
            stopped: None,
        }
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
        if !inputs.hip3_oracle_updates.is_empty() {
            self.streams.hip3_oracle_updates.record_read(height);
        }
        if !inputs.book_diffs.is_empty() {
            self.streams.book_diffs.record_read(height);
        }
        let block = BlockInputs::of(height, inputs, &mut report);
        if let Some(state) = self.state.as_mut().filter(|state| state.height < height) {
            state.apply(height, &block, &mut report);
        }
        if !self.log.push(height, block) {
            report.log_overflowed = true;
            self.streams.hip3_oracle_updates.record_lost_block(height);
            self.streams.book_diffs.record_lost_block(height);
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
mod tests {
    use super::*;
    use crate::accounts::{
        book::{CLOCK_MARGIN_MS, LossBound},
        checkpoint::{parse_checkpoint_bytes, tests::fixture},
        marks::{
            book::{BookMismatch, tests::diff_line},
            seed::RestingOrder,
        },
        test_support::{HIP3_LINE_TIME_NS, hip3_line as line},
    };
    use crate::order_book::Side;

    const GRID: CheckpointGrid = CheckpointGrid { anchor: 1_000, interval: 100 };
    /// 2026-10-19T06:37:23.556 UTC, the fixture checkpoint's time.
    const SEED_TIME_MS: u64 = 1_792_391_843_556;
    const STARTED_MS: u64 = SEED_TIME_MS - 60_000;

    /// The fixture's seed at `height` (its HIP-3 coins: xyz:TSLA at 358.32, flx:GOLD at 41883.0).
    fn seed(height: u64) -> MarkSeed {
        let mut seed = MarkSeed::from_checkpoint(parse_checkpoint_bytes(&fixture()).unwrap()).unwrap();
        seed.height = height;
        seed.time_ms = SEED_TIME_MS + height;
        seed
    }

    fn tsla(marks: &NodeMarks) -> Option<StoredMark> {
        marks.state.as_ref().unwrap().hip3["xyz:TSLA"].mark
    }

    /// A mark written by `line`.
    const fn written(px: u64) -> StoredMark {
        StoredMark { px, time_ns: HIP3_LINE_TIME_NS }
    }

    /// Only HIP-3 lines.
    fn hip3_only(lines: Vec<Batch<OracleUpdateEvent>>) -> MarkInputs {
        MarkInputs { hip3_oracle_updates: lines, book_diffs: Vec::new() }
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

    /// Marks applied through `blocks`, with xyz:TSLA lines at the given heights; those blocks also carry `no_op_diffs`,
    /// so the books stream is read with the HIP-3 one and only blocks with a line are logged.
    fn run(marks: &mut NodeMarks, blocks: std::ops::RangeInclusive<u64>, lines: &[(u64, &str)]) {
        for height in blocks {
            let hip3_oracle_updates: Vec<_> =
                lines.iter().filter(|(at, _)| *at == height).map(|(at, px)| line(*at, &[("xyz:TSLA", px)])).collect();
            let book_diffs = if hip3_oracle_updates.is_empty() { Vec::new() } else { no_op_diffs(height) };
            let inputs = MarkInputs { hip3_oracle_updates, book_diffs };
            assert_eq!(marks.apply_block(height, inputs), MarksBlockReport::default(), "block {height}");
        }
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
        assert_eq!(report.check, MarksCheck::Checked { hip3: Hip3Check { equal: 2, ..Hip3Check::default() }, books: seed_books() });
        assert_eq!(report.rebuild, Rebuild::Replayed { blocks: 1 }, "block 1002 replayed onto the seed");
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
        assert_eq!(report.check, MarksCheck::Checked { hip3, books: seed_books() });
        assert_eq!(tsla(&marks).map(|mark| mark.px), Some(358_320), "the checkpoint's value, nothing logged above it");
    }

    #[test]
    fn a_loss_withholds_the_marks_until_a_covering_seed_rebuilds_them() {
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        run(&mut marks, 995..=995, &[]);
        marks.install(seed(995));
        run(&mut marks, 996..=1_010, &[(996, "359.0")]);
        marks.record_lost_block(Stream::Fills, 1_005);
        assert_eq!(marks.availability(), Ok(1_010), "the marks do not read fills");
        marks.record_lost_block(Stream::Hip3OracleUpdates, 1_005);
        let lost = Unavailable::DataLoss { stream: Stream::Hip3OracleUpdates, bound: LossBound::Block(1_005) };
        assert_eq!(marks.availability(), Err(lost));
        assert!(marks.seed_ready(1_100), "unavailable: a seed installs at once");
        run(&mut marks, 1_011..=1_012, &[(1_012, "362.0")]);
        let report = marks.install(seed(1_010));
        assert_eq!((report.check, report.rebuild), (MarksCheck::Skipped("noSnapshot"), Rebuild::Replayed { blocks: 1 }));
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
        assert_eq!((report.check, report.rebuild), (MarksCheck::Skipped("noSnapshot"), Rebuild::Replayed { blocks: 0 }));
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
        assert_eq!(report.rebuild, Rebuild::Replayed { blocks: 1 });
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
        assert_eq!((marks.log.base, marks.log.inputs), (Some(1_000), 3), "block 1002: one update and two diffs");
        marks.install(seed(1_000));
        run(&mut marks, 1_004..=1_099, &[(1_050, "360.0")]);
        assert_eq!((marks.log.base, marks.log.inputs), (None, 0), "the installed seed covers what a later one needs");
        run(&mut marks, 1_100..=1_101, &[(1_101, "360.5")]);
        assert_eq!((marks.log.base, marks.log.inputs), (Some(1_100), 3));
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
        let started = Unavailable::DataLoss { stream: Stream::Hip3OracleUpdates, bound: LossBound::WrittenBy(SEED_TIME_MS + 1_000) };
        assert_eq!(marks.availability(), Err(started));
        let covering = 1_000 + CLOCK_MARGIN_MS;
        run(&mut marks, 997..=covering + 1, &[]);
        marks.install(seed(covering + 1));
        assert_eq!(marks.availability(), Ok(covering + 1));
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
        assert_eq!(marks.install(seed(1_200)).rebuild, Rebuild::Replayed { blocks: 0 });
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

    #[test]
    fn an_overflowing_log_is_a_loss_of_both_streams() {
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        run(&mut marks, 995..=995, &[]);
        marks.install(seed(995));
        run(&mut marks, 996..=996, &[(996, "359.0")]);
        marks.log = InputLog::new(3);
        marks.log.restart(996);
        run(&mut marks, 997..=997, &[(997, "359.5")]);
        let report = marks.apply_block(998, MarkInputs { book_diffs: no_op_diffs(998), ..MarkInputs::default() });
        assert!(report.log_overflowed, "3 inputs at 997, 2 more at 998");
        let seed_at = AnchorInfo { height: 995, time_ms: SEED_TIME_MS + 995 };
        for stream in [Stream::Hip3OracleUpdates, Stream::BookDiffs] {
            let lost = Unavailable::DataLoss { stream, bound: LossBound::Block(998) };
            assert_eq!(marks.streams.get_mut(stream).unwrap().check(stream, seed_at), Err(lost));
        }
        assert_eq!((marks.log.base, marks.log.inputs), (Some(998), 0), "starts over after the block");
    }

    #[test]
    fn an_overflowing_log_is_a_loss_and_starts_over() {
        let mut log = InputLog::new(2);
        log.restart(9);
        let update = Hip3MarkUpdate { coin: "xyz:TSLA".to_string(), px: "1.0".to_string(), time_ns: 1 };
        assert!(log.push(10, BlockInputs { hip3: vec![update.clone(); 2], diffs: Vec::new() }));
        assert!(!log.push(11, BlockInputs { hip3: vec![update], diffs: Vec::new() }), "one more overflows");
        assert_eq!((log.base, log.inputs, log.reaches(10), log.reaches(11)), (Some(11), 0, false, true));
    }
}
