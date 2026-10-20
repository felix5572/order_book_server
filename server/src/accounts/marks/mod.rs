//! The node's marks, block by block (design 000439 r2). HIP-3 coins come from the node's oracle update stream, which
//! writes each mark the node applies (research `hl_node_mark_state_20261019` "HIP-3：直接读节点的流": 930/930 stored
//! marks and times equal the last written ones). The main dex takes a per-action replay in batch 2, from the seed's
//! main-dex part; batch 1 checks that part at every checkpoint but keeps nothing of it.
//!
//! `NodeMarks` lives in the account view's owner task and gets every completed block before the account view does, so
//! both stand at one height. Trust (revision 2):
//!
//! - a checkpoint's seed installs once every block up to it is applied, or at once while the marks are unavailable;
//! - at each grid height G the state is kept as a snapshot, and the log of mark inputs starts over after G;
//! - installing checkpoint G compares the snapshot with it (counts and evidence; a mismatch does not withhold the marks,
//!   the rebuild corrects them), then rebuilds the state from it and the logged blocks above it;
//! - a loss on a stream the marks read, a seed breaking its contract or a log overflow withholds the marks until a later
//!   checkpoint rebuilds them from a complete log.

pub(crate) mod seed;

use rustc_hash::FxHashMap;

use self::seed::{Hip3Coin, MarkSeed, StoredMark, exact_price_units};
use super::{
    book::{AnchorInfo, Stream, StreamProgress, Unavailable},
    checkpoint::node_time_ns,
};
use crate::types::node_data::{Batch, OracleUpdateEvent};

/// Logged HIP-3 mark updates at most. About 60 a second are written (2026-10-20 11 UTC: 215,769 in an hour); the log
/// starts over at every grid height and at every install, so this is hours without either, not a normal state.
pub(crate) const MAX_LOG_UPDATES: usize = 500_000;
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

/// The marks at `height`.
#[derive(Debug, Clone)]
struct MarkState {
    /// The checkpoint it was rebuilt from.
    seed: AnchorInfo,
    /// Highest block applied; the seed's height while no later block is.
    height: u64,
    hip3: FxHashMap<String, Hip3Coin>,
}

impl MarkState {
    /// Batch 1 replays no main-dex coin: the seed's main-dex part is not kept.
    fn seeded(seed: MarkSeed) -> Self {
        let MarkSeed { height, time_ms, native: _, hip3 } = seed;
        Self { seed: AnchorInfo { height, time_ms }, height, hip3 }
    }

    /// Writes the updates of block `height`; a coin outside the seed's universe (listed since) is counted and skipped,
    /// a price that does not convert breaks the contract.
    fn apply(&mut self, height: u64, updates: &[Hip3MarkUpdate], report: &mut MarksBlockReport) {
        for update in updates {
            let Some(coin) = self.hip3.get_mut(&update.coin) else {
                report.unknown_coin_updates += 1;
                continue;
            };
            match exact_price_units(&update.px, coin.size_decimals) {
                Ok(px) => coin.mark = Some(StoredMark { px, time_ns: update.time_ns }),
                Err(err) => report.errors.push(format!("block {height} {}: {err}", update.coin)),
            }
        }
        self.height = height;
    }
}

/// Every completed block above `base` is logged (blocks without updates as nothing).
#[derive(Debug, Default)]
struct InputLog {
    /// `None` until the first block reaches the marks.
    base: Option<u64>,
    blocks: Vec<(u64, Vec<Hip3MarkUpdate>)>,
    updates: usize,
}

impl InputLog {
    /// Logs block `height`; returns false when that overflowed the log, which then starts over after it.
    fn push(&mut self, height: u64, updates: Vec<Hip3MarkUpdate>) -> bool {
        let base = *self.base.get_or_insert_with(|| height.saturating_sub(1));
        if height <= base || updates.is_empty() {
            return true;
        }
        self.updates += updates.len();
        self.blocks.push((height, updates));
        if self.updates <= MAX_LOG_UPDATES {
            return true;
        }
        self.restart(height);
        false
    }

    /// Forgets everything up to `height`, which becomes the base.
    fn restart(&mut self, height: u64) {
        self.blocks.clear();
        self.updates = 0;
        self.base = Some(height);
    }

    /// Forgets the blocks up to `height`: a seed there covers them.
    fn drop_through(&mut self, height: u64) {
        self.blocks.retain(|(block, _)| *block > height);
        self.updates = self.blocks.iter().map(|(_, updates)| updates.len()).sum();
        self.base = self.base.map(|base| base.max(height));
    }

    /// Whether every block above `height` up to the last applied one is here.
    fn reaches(&self, height: u64) -> bool {
        self.base.is_some_and(|base| base <= height)
    }

    fn after(&self, height: u64) -> impl Iterator<Item = &(u64, Vec<Hip3MarkUpdate>)> {
        self.blocks.iter().filter(move |(block, _)| *block > height)
    }
}

/// What one completed block did to the marks.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct MarksBlockReport {
    /// Updates of coins outside the seed's universe.
    pub(crate) unknown_coin_updates: usize,
    /// Contract violations; each is a loss up to the block.
    pub(crate) errors: Vec<String>,
    /// The log overflowed with this block, a loss up to it.
    pub(crate) log_overflowed: bool,
}

/// The snapshot at a checkpoint's height against the checkpoint (design 000439 r2 revision 2).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MarksCheck {
    Checked(Hip3Check),
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
    /// Logged updates the new seed's universe now converts and that break the contract (a coin listed since the
    /// previous seed); each is a loss up to its block.
    pub(crate) errors: Vec<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MarksStatus {
    pub(crate) availability: Result<u64, Unavailable>,
    pub(crate) seed_height: Option<u64>,
    pub(crate) logged_updates: usize,
}

#[derive(Debug)]
pub(crate) struct NodeMarks {
    grid: CheckpointGrid,
    /// Highest completed block handed over.
    applied: Option<u64>,
    hip3_stream: StreamProgress,
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
    pub(crate) fn new(grid: CheckpointGrid, started_ms: u64) -> Self {
        Self {
            grid,
            applied: None,
            hip3_stream: StreamProgress::starting(started_ms),
            state: None,
            snapshot: None,
            log: InputLog::default(),
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
        self.hip3_stream.check(Stream::Hip3OracleUpdates, state.seed)?;
        Ok(state.height)
    }

    pub(crate) fn status(&self) -> MarksStatus {
        MarksStatus {
            availability: self.availability(),
            seed_height: self.state.as_ref().map(|state| state.seed.height),
            logged_updates: self.log.updates,
        }
    }

    /// The marks' inputs of completed block `height` (blocks come in height order, each once).
    pub(crate) fn apply_block(&mut self, height: u64, hip3_oracle_updates: Vec<Batch<OracleUpdateEvent>>) -> MarksBlockReport {
        let mut report = MarksBlockReport::default();
        self.applied = Some(height);
        if !hip3_oracle_updates.is_empty() {
            self.hip3_stream.record_read(height);
        }
        let updates = hip3_mark_updates(hip3_oracle_updates).unwrap_or_else(|err| {
            report.errors.push(format!("block {height}: {err}"));
            Vec::new()
        });
        if let Some(state) = self.state.as_mut().filter(|state| state.height < height) {
            state.apply(height, &updates, &mut report);
        }
        if !self.log.push(height, updates) {
            report.log_overflowed = true;
        }
        if !report.errors.is_empty() || report.log_overflowed {
            self.hip3_stream.record_lost_block(height);
        }
        if self.grid.contains(height) {
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
        if stream == Stream::Hip3OracleUpdates {
            self.hip3_stream.record_lost_block(block);
        }
    }

    /// A loss of `stream` of unknown blocks, detected at `detected_ms`; a stream the marks do not read leaves them as
    /// they are.
    pub(crate) fn record_unknown_loss(&mut self, stream: Stream, detected_ms: u64) {
        if stream == Stream::Hip3OracleUpdates {
            self.hip3_stream.record_unknown_loss(detected_ms);
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
            Some(snapshot) if snapshot.height == height => MarksCheck::Checked(check_hip3(&snapshot.hip3, &seed.hip3)),
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

    fn rebuild(&mut self, seed: MarkSeed, errors: &mut Vec<String>) -> Rebuild {
        let height = seed.height;
        let rebuild = match self.applied {
            Some(applied) if applied > height => {
                if !self.log.reaches(height) {
                    return Rebuild::LogDoesNotReach;
                }
                let mut state = MarkState::seeded(seed);
                let mut blocks = 0;
                for (block, updates) in self.log.after(height) {
                    // unknown coins were counted when the block was applied
                    let mut replayed = MarksBlockReport::default();
                    state.apply(*block, updates, &mut replayed);
                    if !replayed.errors.is_empty() {
                        self.hip3_stream.record_lost_block(*block);
                        errors.append(&mut replayed.errors);
                    }
                    blocks += 1;
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
        self.log.drop_through(height);
        rebuild
    }
}

/// The written marks of a block's lines, in the node's write order.
fn hip3_mark_updates(lines: Vec<Batch<OracleUpdateEvent>>) -> Result<Vec<Hip3MarkUpdate>, String> {
    let mut updates = Vec::new();
    for event in lines.into_iter().flat_map(Batch::events) {
        for (coin, written) in event.oracle_pxs.coin_to_mark_px {
            let time_ns = node_time_ns(&written.last_update_time).map_err(|err| format!("{coin}: {err}"))?;
            updates.push(Hip3MarkUpdate { coin, px: written.px, time_ns });
        }
    }
    Ok(updates)
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
        test_support::{HIP3_LINE_TIME_NS, hip3_line as line},
    };

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

    /// Marks applied through `through`, with the fixture's HIP-3 lines at the given heights.
    fn run(marks: &mut NodeMarks, blocks: std::ops::RangeInclusive<u64>, lines: &[(u64, &str)]) {
        for height in blocks {
            let block_lines: Vec<_> =
                lines.iter().filter(|(at, _)| *at == height).map(|(at, px)| line(*at, &[("xyz:TSLA", px)])).collect();
            assert_eq!(marks.apply_block(height, block_lines), MarksBlockReport::default(), "block {height}");
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
        assert_eq!(report.check, MarksCheck::Checked(Hip3Check { equal: 2, ..Hip3Check::default() }));
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
        assert_eq!(report.check, MarksCheck::Checked(Hip3Check { equal: 1, differ: 1, unknown: 0, samples: vec![mismatch] }));
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
        assert!(matches!(report.check, MarksCheck::Checked(Hip3Check { differ: 1, .. })), "{:?}", report.check);
        assert_eq!(report.rebuild, Rebuild::Replayed { blocks: 1 });
    }

    #[test]
    fn a_written_price_breaking_the_contract_is_a_loss_and_unknown_coins_are_counted() {
        let mut marks = NodeMarks::new(GRID, STARTED_MS);
        run(&mut marks, 995..=995, &[]);
        marks.install(seed(995));
        run(&mut marks, 996..=996, &[(996, "359.0")]);
        let report = marks.apply_block(997, vec![line(997, &[("xyz:NEW", "1.0"), ("xyz:TSLA", "359.0001")])]);
        assert_eq!(report.unknown_coin_updates, 1);
        assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
        assert!(report.errors[0].contains("xyz:TSLA"), "{:?}", report.errors);
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
        assert_eq!(marks.apply_block(996, vec![line(996, &[("xyz:NEW", "2.5")])]).unknown_coin_updates, 1);
        assert_eq!(marks.apply_block(997, vec![line(997, &[("xyz:NEW", "2.75")])]).unknown_coin_updates, 1);
        let mut listing = seed(996);
        listing.hip3.insert("xyz:NEW".to_string(), Hip3Coin { size_decimals: 4, mark: None });
        let report = marks.install(listing);
        assert_eq!((report.rebuild, report.errors.as_slice()), (Rebuild::Replayed { blocks: 1 }, [].as_slice()));
        assert_eq!(marks.state.as_ref().unwrap().hip3["xyz:NEW"].mark, Some(written(275)), "block 997, at 2 decimals");
        let mut coarser = seed(997);
        coarser.hip3.insert("xyz:NEW".to_string(), Hip3Coin { size_decimals: 5, mark: None });
        marks.apply_block(998, vec![line(998, &[("xyz:NEW", "2.75")])]);
        let report = marks.install(coarser);
        assert_eq!(report.errors.len(), 1, "2.75 has more than one decimal at szDecimals 5");
        let lost = Unavailable::DataLoss { stream: Stream::Hip3OracleUpdates, bound: LossBound::Block(998) };
        assert_eq!(marks.availability(), Err(lost));
    }

    #[test]
    fn the_start_is_a_loss_until_a_seed_past_it() {
        let mut marks = NodeMarks::new(GRID, SEED_TIME_MS + 1_000);
        run(&mut marks, 995..=996, &[(996, "359.0")]);
        marks.install(seed(995));
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

    #[test]
    fn an_overflowing_log_is_a_loss_and_starts_over() {
        let mut log = InputLog::default();
        let update = Hip3MarkUpdate { coin: "xyz:TSLA".to_string(), px: "1.0".to_string(), time_ns: 1 };
        assert!(log.push(10, vec![update.clone(); MAX_LOG_UPDATES]));
        assert!(!log.push(11, vec![update]), "one more overflows");
        assert_eq!((log.base, log.updates, log.reaches(10), log.reaches(11)), (Some(11), 0, false, true));
    }
}
