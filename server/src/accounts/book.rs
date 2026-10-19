//! `AccountBook`: the single owner of the account view (design 000419 r2-r4). Pure state; the runtime feeds it.
//!
//! - **Positions** are the latest anchor projected forward by observed fills. A fill carries the absolute size after
//!   it (`startPosition ± sz`); a record remembers the block that set it and the fill identities of that block already
//!   applied, so replays and duplicates are skipped (r3 revision 2). Zero sizes stay as versioned "closed" markers
//!   until an anchor covers them. Sizes are the checkpoint's raw integers (10^-szDecimals), exact.
//! - **Fills outside the anchor universe** (spot `@N`, HIP-4 outcomes `#N`, a perp listed after the anchor, and every
//!   fill before the first anchor) are held and replayed against the next anchor's universe; the ones that anchor
//!   covers are dropped. The hold is bounded: overflowing it is a fills data loss.
//! - **Anchor rows** (margin mode, pool size, liquidation price) and account modes are the anchor's own and are never
//!   re-derived between anchors.
//! - **Change marks**: per account, the first block after the anchor of each observed source (fill, ledger, funding):
//!   from there on the anchor's liquidation price may be stale. A mark is set once per anchor interval, so an account
//!   trading every block changes its rows once, not per block (review of the push volume, 10-19). The mark events since
//!   the anchor are logged so the next anchor can rebuild the first marks after itself. An empty set does not prove the
//!   account unchanged: isolated margin moves, mode switches and withdrawal requests are block actions this view does
//!   not read.
//! - **Trust**: served only with an anchor covering every recorded loss of both streams, no newer checkpoint that
//!   failed to parse, and live inputs. A loss of known blocks (an unparsable line whose header still reads, a fill
//!   breaking the size contract, an overflowing hold) is covered by an anchor at or above its highest block. A loss of
//!   unknown blocks (data the watcher discarded, a line without a readable header, and everything written before the
//!   watcher started) was written by the time it was detected, so it is covered by an anchor whose block time is
//!   `CLOCK_MARGIN_MS` past the detection: block heights grow with block time, whatever order the lines came in.

use std::{
    cmp::Ordering,
    collections::{VecDeque, hash_map::Entry},
    fmt,
};

use alloy::primitives::Address;
use rustc_hash::{FxHashMap, FxHashSet};

use super::{
    anchor::{Anchor, AnchorCoin, AnchorPosition, AnchorRow},
    checkpoint::AccountMode,
    misc::MiscEvent,
    size::parse_size_raw,
};
use crate::{
    order_book::Side,
    types::{
        Fill,
        node_data::{Batch, NodeDataFill},
    },
};

/// A block's time trails the node's write of its lines: on bm the node wrote every line 123 ms or more after its
/// block time (800k fills and misc lines, 2026-10-19, NTP-synced host). An anchor whose block time is this far past a
/// loss detection therefore lies above every block lost by then.
pub(crate) const CLOCK_MARGIN_MS: u64 = 1_000;

/// Mark events kept to rebuild the marks after the next anchor: one anchor interval is ~80k fills plus, across an hour
/// boundary, ~400k funding deltas; this is hours without an anchor. Overflowing drops the oldest event, a loss up to
/// its block on its stream.
const MAX_MARK_LOG: usize = 2_000_000;

/// Silent position changes listed per anchor install (all are counted): evidence of backstop liquidation or ADL comes
/// a few at a time; a systematic contradiction needs only examples.
const MAX_SILENT_SAMPLES: usize = 100;

/// Held fills outside the anchor universe. About 2 per second arrive outside it (spot and outcome markets,
/// 2026-10-19) and ~110 per second before the first anchor, so this is hours of anchor outage, not a normal state.
const MAX_HELD_FILLS: usize = 500_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Stream {
    Fills,
    MiscEvents,
}

impl Stream {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Fills => "fills",
            Self::MiscEvents => "misc_events",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AnchorInfo {
    pub(crate) height: u64,
    pub(crate) time_ms: u64,
}

/// A servable view: its anchor and how far each stream has been read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Available {
    pub(crate) anchor: AnchorInfo,
    pub(crate) fills_height: u64,
    pub(crate) misc_events_height: u64,
}

/// Why the view may not be served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Unavailable {
    /// A watcher, the checkpoint poller or the owner itself is gone; permanent until restart.
    Stopped(String),
    /// The newest checkpoint failed to parse; cleared by a newer anchor.
    CheckpointFailed { height: u64, error: String },
    /// No anchor yet, or a stream has not read a line yet.
    Initializing,
    /// A stream lost data that no installed anchor covers yet.
    DataLoss { stream: Stream, bound: LossBound },
}

/// What an anchor must reach to cover a stream's losses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LossBound {
    /// Highest lost block.
    Block(u64),
    /// Losses of unknown blocks detected by this wall time (unix ms).
    WrittenBy(u64),
}

impl fmt::Display for Unavailable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stopped(reason) => write!(formatter, "stopped: {reason}"),
            Self::CheckpointFailed { height, error } => write!(formatter, "checkpoint {height} failed to parse: {error}"),
            Self::Initializing => formatter.write_str("initializing: waiting for the first checkpoint and stream lines"),
            Self::DataLoss { stream, bound: LossBound::Block(block) } => {
                write!(formatter, "{} lost lines up to block {block}, waiting for a checkpoint covering it", stream.label())
            }
            Self::DataLoss { stream, bound: LossBound::WrittenBy(detected_ms) } => write!(
                formatter,
                "{} lost lines written by unix ms {detected_ms}, waiting for a checkpoint {CLOCK_MARGIN_MS} ms past it",
                stream.label()
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QueryError {
    Unavailable(Unavailable),
    /// Not a coin of the installed anchor's universe.
    UnknownCoin,
}

/// First block per observed source that touched an account after the anchor.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ChangeMarks {
    pub(crate) fill: Option<u64>,
    pub(crate) ledger: Option<u64>,
    pub(crate) funding: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarkKind {
    Fill,
    Ledger,
    Funding,
}

impl MarkKind {
    const fn stream(self) -> Stream {
        match self {
            Self::Fill => Stream::Fills,
            Self::Ledger | Self::Funding => Stream::MiscEvents,
        }
    }
}

impl ChangeMarks {
    /// Lowers the kind's mark to `block` (a replay of an older block can come late); returns whether it changed.
    const fn mark(&mut self, kind: MarkKind, block: u64) -> bool {
        let slot = match kind {
            MarkKind::Fill => &mut self.fill,
            MarkKind::Ledger => &mut self.ledger,
            MarkKind::Funding => &mut self.funding,
        };
        if let Some(first) = *slot
            && first <= block
        {
            return false;
        }
        *slot = Some(block);
        true
    }

    const fn is_empty(self) -> bool {
        self.fill.is_none() && self.ledger.is_none() && self.funding.is_none()
    }
}

/// Raises an optional running maximum.
fn raise(maximum: &mut Option<u64>, value: u64) {
    *maximum = Some(maximum.map_or(value, |current| current.max(value)));
}

#[derive(Debug, Default)]
struct AccountRecord {
    /// The anchor's account mode; `None` when the account held no position at the anchor.
    mode: Option<AccountMode>,
    marks: ChangeMarks,
}

/// A fill's identity on one position: both sides of a trade share `tid`, so the side completes it (the user is the
/// record's key). Unique within and across blocks over one full hour (000419 r3).
type FillIdentity = (u64, Side);

#[derive(Debug, Clone, PartialEq)]
enum Applied {
    /// Set by the anchor at `version_block`, which contains that whole block.
    Anchor,
    /// Fills of `version_block` already applied.
    Fills(Vec<FillIdentity>),
}

#[derive(Debug, Clone, PartialEq)]
struct PositionRecord {
    size_raw: i64,
    /// The anchor height, or the block of the last applied fill.
    version_block: u64,
    applied: Applied,
    /// The anchor's row for this position; `None` when the anchor held no such position (`pending`).
    anchor_row: Option<AnchorRow>,
}

impl PositionRecord {
    fn anchored(height: u64, position: AnchorPosition) -> Self {
        Self { size_raw: position.size_raw, version_block: height, applied: Applied::Anchor, anchor_row: Some(position.row) }
    }

    fn opened(block: u64, identity: FillIdentity, size_after: i64) -> Self {
        Self { size_raw: size_after, version_block: block, applied: Applied::Fills(vec![identity]), anchor_row: None }
    }

    /// Returns whether the fill was applied.
    fn apply_fill(&mut self, block: u64, identity: FillIdentity, size_after: i64) -> bool {
        match block.cmp(&self.version_block) {
            // a replay of an older block
            Ordering::Less => false,
            Ordering::Equal => match &mut self.applied {
                Applied::Anchor => false,
                Applied::Fills(applied) if applied.contains(&identity) => false,
                Applied::Fills(applied) => {
                    applied.push(identity);
                    self.size_raw = size_after;
                    true
                }
            },
            Ordering::Greater => {
                self.version_block = block;
                self.applied = Applied::Fills(vec![identity]);
                self.size_raw = size_after;
                true
            }
        }
    }
}

#[derive(Debug)]
struct CoinBook {
    size_decimals: u32,
    records: FxHashMap<Address, PositionRecord>,
    /// Bumped by every frame pushed for this coin (a `changes` batch or the snapshot after an anchor install); a new
    /// subscriber's snapshot carries it without bumping (design 000419 r2 4.2).
    version: u64,
}

impl CoinBook {
    fn anchored(height: u64, coin: AnchorCoin, version: u64) -> Self {
        let records = coin.positions.into_iter().map(|(user, position)| (user, PositionRecord::anchored(height, position)));
        Self { size_decimals: coin.size_decimals, records: records.collect(), version }
    }

    /// Keeps records set after `height`, takes the anchor's value for the rest, and counts (and samples) the ones the
    /// anchor contradicts: a size change without an observed fill (backstop liquidation, ADL) or a fill the stream lost.
    fn merge(&mut self, name: &str, height: u64, mut positions: FxHashMap<Address, AnchorPosition>, report: &mut InstallReport) {
        let size_decimals = self.size_decimals;
        let mut silent = |user: Address, projected: Option<(i64, u64)>, anchor_size_raw: i64| {
            report.silent_position_changes += 1;
            if report.silent_samples.len() < MAX_SILENT_SAMPLES {
                report.silent_samples.push(SilentChange { coin: name.to_string(), user, size_decimals, projected, anchor_size_raw });
            }
        };
        self.records.retain(|user, record| {
            let anchored = positions.remove(user);
            if record.version_block > height {
                record.anchor_row = anchored.map(|position| position.row);
                return true;
            }
            let anchor_size_raw = anchored.as_ref().map_or(0, |position| position.size_raw);
            if record.size_raw != anchor_size_raw {
                silent(*user, Some((record.size_raw, record.version_block)), anchor_size_raw);
            }
            anchored.is_some_and(|position| {
                *record = PositionRecord::anchored(height, position);
                true
            })
        });
        for (user, position) in &positions {
            silent(*user, None, position.size_raw);
        }
        self.records.extend(positions.into_iter().map(|(user, position)| (user, PositionRecord::anchored(height, position))));
    }
}

/// The fields of a fill the positions need.
#[derive(Debug)]
struct ObservedFill {
    coin: String,
    tid: u64,
    side: Side,
    start_position: String,
    sz: String,
}

impl ObservedFill {
    fn new(fill: Fill) -> Self {
        Self { coin: fill.coin, tid: fill.tid, side: fill.side, start_position: fill.start_position, sz: fill.sz }
    }

    fn size_after(&self, size_decimals: u32) -> Result<i64, String> {
        let start = parse_size_raw(&self.start_position, size_decimals)?;
        let size = parse_size_raw(&self.sz, size_decimals)?;
        if size <= 0 {
            return Err(format!("fill size {:?} is not positive", self.sz));
        }
        let delta = match self.side {
            Side::Bid => size,
            Side::Ask => -size,
        };
        start.checked_add(delta).ok_or_else(|| format!("startPosition {:?} + {delta} overflows", self.start_position))
    }
}

/// What a stream has read and what it is known to have lost; losses only accumulate (anchors only move forward).
#[derive(Debug)]
struct StreamProgress {
    /// Highest block read.
    height: Option<u64>,
    /// Highest block of a line known to be lost.
    lost_block: Option<u64>,
    /// Latest wall time (unix ms) a loss of unknown blocks was detected.
    lost_by_ms: Option<u64>,
}

impl StreamProgress {
    /// Nothing written before the watcher started was read.
    const fn starting(started_ms: u64) -> Self {
        Self { height: None, lost_block: None, lost_by_ms: Some(started_ms) }
    }

    fn check(&self, stream: Stream, anchor: AnchorInfo) -> Result<u64, Unavailable> {
        if let Some(block) = self.lost_block.filter(|block| *block > anchor.height) {
            return Err(Unavailable::DataLoss { stream, bound: LossBound::Block(block) });
        }
        if let Some(detected_ms) = self.lost_by_ms.filter(|detected| anchor.time_ms < detected + CLOCK_MARGIN_MS) {
            return Err(Unavailable::DataLoss { stream, bound: LossBound::WrittenBy(detected_ms) });
        }
        self.height.ok_or(Unavailable::Initializing)
    }
}

/// The rows a batch changed: every row of `accounts` (a mark appeared or moved earlier) and the row of each `positions`
/// entry (coin, user).
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Touched {
    pub(crate) accounts: Vec<Address>,
    pub(crate) positions: Vec<(String, Address)>,
}

/// What one fills batch did.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct FillsReport {
    /// The installed anchor already contains the block.
    pub(crate) stale: bool,
    /// Fills held outside the universe.
    pub(crate) held: usize,
    /// The hold overflowed and was discarded (recorded as a fills data loss).
    pub(crate) hold_overflowed: bool,
    /// Fills of universe coins that violate the size contract (recorded as a fills data loss).
    pub(crate) errors: Vec<String>,
    pub(crate) touched: Touched,
    /// Oldest mark events dropped for room (recorded as losses up to their blocks).
    pub(crate) marks_dropped: usize,
}

/// What one misc events batch did.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct MiscReport {
    pub(crate) stale: bool,
    pub(crate) touched: Touched,
    /// Oldest mark events dropped for room (recorded as losses up to their blocks).
    pub(crate) marks_dropped: usize,
    pub(crate) unknown_events: usize,
    /// Unknown kinds seen for the first time.
    pub(crate) new_unknown_kinds: Vec<String>,
}

/// A position an anchor contradicts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SilentChange {
    pub(crate) coin: String,
    pub(crate) user: Address,
    pub(crate) size_decimals: u32,
    /// The fill projection's size and the block that set it; `None` when the view held no record (opened silently).
    pub(crate) projected: Option<(i64, u64)>,
    /// 0 when the anchor holds no such position (closed silently).
    pub(crate) anchor_size_raw: i64,
}

/// What an anchor install did.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct InstallReport {
    /// The anchor this one replaced.
    pub(crate) previous_anchor: Option<u64>,
    pub(crate) silent_position_changes: u64,
    /// Up to `MAX_SILENT_SAMPLES` of them.
    pub(crate) silent_samples: Vec<SilentChange>,
    /// Records of coins that left the universe, dropped.
    pub(crate) dropped_coin_records: usize,
    /// Held fills the anchor covers, dropped.
    pub(crate) covered_held_fills: usize,
    /// Held fills applied to coins the anchor lists.
    pub(crate) replayed_fills: usize,
    /// Coins whose records could not be merged (recorded as a fills data loss).
    pub(crate) merge_errors: Vec<String>,
    /// Replayed fills that violate the size contract (recorded as a fills data loss).
    pub(crate) replay_errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PositionView {
    pub(crate) user: Address,
    pub(crate) size_raw: i64,
    /// The block that set the size: the anchor height or the block of its last fill.
    pub(crate) position_height: u64,
    pub(crate) account_mode: Option<AccountMode>,
    /// The anchor's row (at `CoinPositions::anchor`); `None` = opened after the anchor.
    pub(crate) anchor_row: Option<AnchorRow>,
    pub(crate) changes: ChangeMarks,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CoinPositions {
    pub(crate) coin: String,
    /// The coin's version (see `CoinBook::version`).
    pub(crate) version: u64,
    pub(crate) anchor: AnchorInfo,
    pub(crate) fills_height: u64,
    pub(crate) misc_events_height: u64,
    pub(crate) size_decimals: u32,
    /// Open positions, by user.
    pub(crate) positions: Vec<PositionView>,
}

/// The rows of one coin a batch touched: the frame body of a `changes` push.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CoinChanges {
    pub(crate) coin: String,
    pub(crate) version: u64,
    pub(crate) anchor: AnchorInfo,
    pub(crate) fills_height: u64,
    pub(crate) misc_events_height: u64,
    pub(crate) size_decimals: u32,
    /// Touched holders by user: the open position, or `None` once it is closed.
    pub(crate) rows: Vec<(Address, Option<PositionView>)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BookStatus {
    pub(crate) availability: Result<Available, Unavailable>,
    pub(crate) fills_height: Option<u64>,
    pub(crate) misc_events_height: Option<u64>,
    pub(crate) open_positions: usize,
    pub(crate) held_fills: usize,
}

#[derive(Debug)]
pub(crate) struct AccountBook {
    anchor: Option<AnchorInfo>,
    /// Every coin of the anchor's universe.
    coins: FxHashMap<String, CoinBook>,
    accounts: FxHashMap<Address, AccountRecord>,
    held_fills: Vec<(u64, Address, ObservedFill)>,
    /// Mark events after the anchor, oldest first.
    mark_log: VecDeque<(u64, MarkKind, Address)>,
    fills: StreamProgress,
    misc_events: StreamProgress,
    checkpoint_failure: Option<(u64, String)>,
    stopped: Option<String>,
    unknown_misc_kinds: FxHashSet<String>,
}

impl AccountBook {
    /// `started_ms`: wall time (unix ms) the watchers started; everything written before is lost to this view.
    pub(crate) fn new(started_ms: u64) -> Self {
        Self {
            anchor: None,
            coins: FxHashMap::default(),
            accounts: FxHashMap::default(),
            held_fills: Vec::new(),
            mark_log: VecDeque::new(),
            fills: StreamProgress::starting(started_ms),
            misc_events: StreamProgress::starting(started_ms),
            checkpoint_failure: None,
            stopped: None,
            unknown_misc_kinds: FxHashSet::default(),
        }
    }

    fn covered(&self, block: u64) -> bool {
        self.anchor.is_some_and(|anchor| block <= anchor.height)
    }

    const fn progress(&mut self, stream: Stream) -> &mut StreamProgress {
        match stream {
            Stream::Fills => &mut self.fills,
            Stream::MiscEvents => &mut self.misc_events,
        }
    }

    pub(crate) fn apply_fills(&mut self, batch: Batch<NodeDataFill>) -> FillsReport {
        let block = batch.block_number();
        raise(&mut self.fills.height, block);
        let mut report = FillsReport::default();
        if self.covered(block) {
            report.stale = true;
            return report;
        }
        for NodeDataFill(user, fill) in batch.events() {
            if self.mark(user, MarkKind::Fill, block, &mut report.marks_dropped) {
                report.touched.accounts.push(user);
            }
            self.apply_or_hold(block, user, ObservedFill::new(fill), &mut report);
        }
        report
    }

    /// Logs a mark event and lowers the account's mark; returns whether the mark changed.
    fn mark(&mut self, user: Address, kind: MarkKind, block: u64, dropped: &mut usize) -> bool {
        if self.mark_log.len() >= MAX_MARK_LOG
            && let Some((oldest, oldest_kind, _)) = self.mark_log.pop_front()
        {
            raise(&mut self.progress(oldest_kind.stream()).lost_block, oldest);
            *dropped += 1;
        }
        self.mark_log.push_back((block, kind, user));
        self.accounts.entry(user).or_default().marks.mark(kind, block)
    }

    fn apply_or_hold(&mut self, block: u64, user: Address, fill: ObservedFill, report: &mut FillsReport) {
        let Some(coin) = self.coins.get_mut(&fill.coin) else {
            if self.held_fills.len() >= MAX_HELD_FILLS {
                let newest = self.held_fills.iter().map(|(held_block, _, _)| *held_block).max();
                self.held_fills.clear();
                if let Some(newest) = newest {
                    raise(&mut self.fills.lost_block, newest);
                }
                report.hold_overflowed = true;
            }
            self.held_fills.push((block, user, fill));
            report.held += 1;
            return;
        };
        let size_after = match fill.size_after(coin.size_decimals) {
            Ok(size_after) => size_after,
            Err(err) => {
                report.errors.push(format!("block {block} {user} {} tid {}: {err}", fill.coin, fill.tid));
                raise(&mut self.fills.lost_block, block);
                return;
            }
        };
        let identity = (fill.tid, fill.side);
        let applied = match coin.records.entry(user) {
            Entry::Vacant(entry) => {
                entry.insert(PositionRecord::opened(block, identity, size_after));
                true
            }
            Entry::Occupied(mut entry) => entry.get_mut().apply_fill(block, identity, size_after),
        };
        if applied {
            report.touched.positions.push((fill.coin, user));
        }
    }

    pub(crate) fn apply_misc_events(&mut self, batch: Batch<MiscEvent>) -> MiscReport {
        let block = batch.block_number();
        raise(&mut self.misc_events.height, block);
        let mut report = MiscReport::default();
        if self.covered(block) {
            report.stale = true;
            return report;
        }
        for event in batch.events() {
            match event {
                MiscEvent::Ledger { users, .. } => self.mark_all(users, MarkKind::Ledger, block, &mut report),
                MiscEvent::Funding(users) => self.mark_all(users, MarkKind::Funding, block, &mut report),
                MiscEvent::Irrelevant => {}
                MiscEvent::Unknown(kind) => {
                    report.unknown_events += 1;
                    if self.unknown_misc_kinds.insert(kind.clone()) {
                        report.new_unknown_kinds.push(kind);
                    }
                }
            }
        }
        report
    }

    fn mark_all(&mut self, users: Vec<Address>, kind: MarkKind, block: u64, report: &mut MiscReport) {
        for user in users {
            if self.mark(user, kind, block, &mut report.marks_dropped) {
                report.touched.accounts.push(user);
            }
        }
    }

    /// Installs a newer anchor. Heights only increase: the poller sends each checkpoint once, newest first.
    pub(crate) fn install_anchor(&mut self, anchor: Anchor) -> InstallReport {
        let Anchor { height, time_ms, coins: mut anchor_coins, account_modes } = anchor;
        assert!(
            self.anchor.is_none_or(|current| current.height < height),
            "anchor {height} is not newer than the installed {:?}",
            self.anchor
        );
        let mut report = InstallReport { previous_anchor: self.anchor.map(|anchor| anchor.height), ..InstallReport::default() };
        let mut lost_block = self.fills.lost_block;
        self.coins.retain(|name, coin| {
            let Some(anchor_coin) = anchor_coins.remove(name) else {
                report.dropped_coin_records += coin.records.len();
                return false;
            };
            if anchor_coin.size_decimals != coin.size_decimals {
                // records newer than the anchor are in the old unit: drop them (a loss up to the newest) and take the
                // anchor's
                report.merge_errors.push(format!(
                    "{name}: szDecimals changed from {} to {}",
                    coin.size_decimals, anchor_coin.size_decimals
                ));
                let newest = coin.records.values().map(|record| record.version_block).filter(|block| *block > height).max();
                if let Some(newest) = newest {
                    raise(&mut lost_block, newest);
                }
                *coin = CoinBook::anchored(height, anchor_coin, coin.version);
                return true;
            }
            coin.merge(name, height, anchor_coin.positions, &mut report);
            true
        });
        self.fills.lost_block = lost_block;
        self.coins.extend(anchor_coins.into_iter().map(|(name, coin)| (name, CoinBook::anchored(height, coin, 0))));
        self.rebuild_marks(height);
        for (user, mode) in account_modes {
            self.accounts.entry(user).or_default().mode = Some(mode);
        }
        self.anchor = Some(AnchorInfo { height, time_ms });
        if self.checkpoint_failure.as_ref().is_some_and(|(failed, _)| *failed < height) {
            self.checkpoint_failure = None;
        }
        self.replay_held_fills(height, &mut report);
        report
    }

    /// The marks after `height`: the first logged block of each kind above it; accounts with neither marks nor (soon) a
    /// mode are forgotten.
    fn rebuild_marks(&mut self, height: u64) {
        for account in self.accounts.values_mut() {
            account.mode = None;
            account.marks = ChangeMarks::default();
        }
        self.mark_log.retain(|(block, _, _)| *block > height);
        for (block, kind, user) in &self.mark_log {
            self.accounts.entry(*user).or_default().marks.mark(*kind, *block);
        }
        self.accounts.retain(|_, account| !account.marks.is_empty());
    }

    /// Applies the held fills the anchor does not cover, in arrival order; the ones still outside the universe stay
    /// held (the hold only shrinks here, so it cannot overflow).
    fn replay_held_fills(&mut self, height: u64, report: &mut InstallReport) {
        let mut fills_report = FillsReport::default();
        let mut uncovered = 0;
        for (block, user, fill) in std::mem::take(&mut self.held_fills) {
            if block <= height {
                report.covered_held_fills += 1;
                continue;
            }
            uncovered += 1;
            self.apply_or_hold(block, user, fill, &mut fills_report);
        }
        report.replayed_fills = uncovered - fills_report.held - fills_report.errors.len();
        report.replay_errors = fills_report.errors;
    }

    /// A line of `block` was lost.
    pub(crate) fn record_lost_block(&mut self, stream: Stream, block: u64) {
        raise(&mut self.progress(stream).lost_block, block);
    }

    /// Lines of unknown blocks were lost, detected at wall time `detected_ms` (unix ms).
    pub(crate) fn record_unknown_loss(&mut self, stream: Stream, detected_ms: u64) {
        raise(&mut self.progress(stream).lost_by_ms, detected_ms);
    }

    pub(crate) fn record_checkpoint_failure(&mut self, height: u64, error: String) {
        self.checkpoint_failure = Some((height, error));
    }

    /// Permanent: a watcher, the poller or the owner is gone.
    pub(crate) fn stop(&mut self, reason: String) {
        self.stopped.get_or_insert(reason);
    }

    pub(crate) fn availability(&self) -> Result<Available, Unavailable> {
        if let Some(reason) = &self.stopped {
            return Err(Unavailable::Stopped(reason.clone()));
        }
        if let Some((height, error)) = &self.checkpoint_failure {
            return Err(Unavailable::CheckpointFailed { height: *height, error: error.clone() });
        }
        let anchor = self.anchor.ok_or(Unavailable::Initializing)?;
        let fills_height = self.fills.check(Stream::Fills, anchor)?;
        let misc_events_height = self.misc_events.check(Stream::MiscEvents, anchor)?;
        Ok(Available { anchor, fills_height, misc_events_height })
    }

    pub(crate) fn status(&self) -> BookStatus {
        BookStatus {
            availability: self.availability(),
            fills_height: self.fills.height,
            misc_events_height: self.misc_events.height,
            open_positions: self.coins.values().flat_map(|coin| coin.records.values()).filter(|r| r.size_raw != 0).count(),
            held_fills: self.held_fills.len(),
        }
    }

    pub(crate) fn coin_positions(&self, name: &str) -> Result<CoinPositions, QueryError> {
        let available = self.availability().map_err(QueryError::Unavailable)?;
        let coin = self.coins.get(name).ok_or(QueryError::UnknownCoin)?;
        let mut positions: Vec<PositionView> = coin
            .records
            .iter()
            .filter(|(_, record)| record.size_raw != 0)
            .map(|(user, record)| self.position_view(*user, record))
            .collect();
        positions.sort_unstable_by_key(|position| position.user);
        Ok(CoinPositions {
            coin: name.to_string(),
            version: coin.version,
            anchor: available.anchor,
            fills_height: available.fills_height,
            misc_events_height: available.misc_events_height,
            size_decimals: coin.size_decimals,
            positions,
        })
    }

    /// The view right after an anchor install, pushed to the coin's subscribers: bumps the coin's version.
    pub(crate) fn push_snapshot(&mut self, name: &str) -> Result<CoinPositions, QueryError> {
        let coin = self.coins.get_mut(name).ok_or(QueryError::UnknownCoin)?;
        coin.version += 1;
        self.coin_positions(name)
    }

    /// The changed rows of `name` (closed ones as `None`) for a `changes` push: every row of `accounts` plus the rows
    /// of `positions`, among the coin's holders. Bumps the coin's version when any; `None` when no row changed.
    pub(crate) fn push_changes(
        &mut self,
        name: &str,
        accounts: &FxHashSet<Address>,
        positions: &[Address],
    ) -> Result<Option<CoinChanges>, QueryError> {
        let available = self.availability().map_err(QueryError::Unavailable)?;
        let coin = self.coins.get(name).ok_or(QueryError::UnknownCoin)?;
        let mut changed: FxHashSet<Address> = if accounts.len() < coin.records.len() {
            accounts.iter().filter(|user| coin.records.contains_key(*user)).copied().collect()
        } else {
            coin.records.keys().filter(|user| accounts.contains(*user)).copied().collect()
        };
        changed.extend(positions.iter().filter(|user| coin.records.contains_key(*user)));
        if changed.is_empty() {
            return Ok(None);
        }
        let mut rows: Vec<(Address, Option<PositionView>)> = changed
            .into_iter()
            .map(|user| {
                let record = &coin.records[&user];
                (user, (record.size_raw != 0).then(|| self.position_view(user, record)))
            })
            .collect();
        rows.sort_unstable_by_key(|(user, _)| *user);
        let size_decimals = coin.size_decimals;
        let coin = self.coins.get_mut(name).ok_or(QueryError::UnknownCoin)?;
        coin.version += 1;
        Ok(Some(CoinChanges {
            coin: name.to_string(),
            version: coin.version,
            anchor: available.anchor,
            fills_height: available.fills_height,
            misc_events_height: available.misc_events_height,
            size_decimals,
            rows,
        }))
    }

    fn position_view(&self, user: Address, record: &PositionRecord) -> PositionView {
        let account = self.accounts.get(&user);
        PositionView {
            user,
            size_raw: record.size_raw,
            position_height: record.version_block,
            account_mode: account.and_then(|account| account.mode.clone()),
            anchor_row: record.anchor_row.clone(),
            changes: account.map_or_else(ChangeMarks::default, |account| account.marks),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::test_support::{PUMP, anchor, fills, misc, row, trusted_book, user};

    const fn fills_lost_up_to(block: u64) -> Unavailable {
        Unavailable::DataLoss { stream: Stream::Fills, bound: LossBound::Block(block) }
    }

    fn size(book: &AccountBook, coin: &str, holder: Address) -> Option<i64> {
        book.coins[coin].records.get(&holder).map(|record| record.size_raw)
    }

    fn shown(book: &AccountBook, coin: &str) -> Vec<(Address, i64, u64)> {
        let view = book.coin_positions(coin).unwrap();
        view.positions.iter().map(|position| (position.user, position.size_raw, position.position_height)).collect()
    }

    // ---- r3 revision 2: the five replay scenarios, fed as real one-fill lines ----

    #[test]
    fn a_close_while_parsing_is_not_revived_by_the_older_anchor() {
        let mut book = trusted_book(&[(user(1), 10)]);
        book.apply_fills(fills(1_500, &[(user(1), PUMP, "A", 7, "10.0", "10.0")]));
        let report = book.install_anchor(anchor(1_400, &[(PUMP, 0, &[(user(1), 10)])]));
        assert_eq!(size(&book, PUMP, user(1)), Some(0), "the close stays as a versioned marker");
        assert_eq!(shown(&book, PUMP).len(), 0);
        assert_eq!(report.silent_position_changes, 0);
    }

    #[test]
    fn an_older_batch_read_after_a_newer_anchor_is_skipped() {
        let mut book = trusted_book(&[(user(1), 2)]);
        book.install_anchor(anchor(2_000, &[(PUMP, 0, &[(user(1), 5)])]));
        let report = book.apply_fills(fills(1_800, &[(user(1), PUMP, "B", 7, "2.0", "3.0")]));
        assert!(report.stale);
        assert_eq!(shown(&book, PUMP), [(user(1), 5, 2_000)]);
    }

    #[test]
    fn a_duplicate_right_after_two_fills_of_the_block_is_skipped() {
        let mut book = trusted_book(&[]);
        let mut sizes = Vec::new();
        for (tid, start) in [(1, "0.0"), (2, "1.0"), (1, "0.0")] {
            book.apply_fills(fills(1_100, &[(user(1), PUMP, "B", tid, start, "1.0")]));
            sizes.push(size(&book, PUMP, user(1)).unwrap());
        }
        assert_eq!(sizes, [1, 2, 2]);
    }

    #[test]
    fn a_whole_block_replayed_changes_nothing() {
        let mut book = trusted_book(&[]);
        for _ in 0..2 {
            book.apply_fills(fills(1_100, &[(user(1), PUMP, "B", 1, "0.0", "1.0")]));
            book.apply_fills(fills(1_100, &[(user(1), PUMP, "B", 2, "1.0", "1.0")]));
        }
        assert_eq!(shown(&book, PUMP), [(user(1), 2, 1_100)]);
    }

    #[test]
    fn replaying_an_older_block_keeps_the_newer_fill() {
        let mut book = trusted_book(&[]);
        book.apply_fills(fills(1_100, &[(user(1), PUMP, "B", 1, "0.0", "1.0")]));
        book.apply_fills(fills(1_101, &[(user(1), PUMP, "B", 3, "1.0", "2.0")]));
        book.apply_fills(fills(1_100, &[(user(1), PUMP, "B", 1, "0.0", "1.0")]));
        assert_eq!(shown(&book, PUMP), [(user(1), 3, 1_101)]);
    }

    #[test]
    fn both_sides_of_one_trade_are_distinct_fills() {
        let mut book = trusted_book(&[]);
        book.apply_fills(fills(1_100, &[(user(1), PUMP, "B", 9, "0.0", "4.0"), (user(1), PUMP, "A", 9, "4.0", "1.0")]));
        assert_eq!(size(&book, PUMP, user(1)), Some(3), "a self-trade: same tid, both sides");
    }

    // ---- anchor merge ----

    #[test]
    fn merge_keeps_newer_fills_takes_the_anchor_otherwise_and_counts_contradictions() {
        let mut book = trusted_book(&[(user(1), 5), (user(2), 7), (user(3), 1)]);
        book.apply_fills(fills(1_200, &[(user(1), PUMP, "B", 1, "5.0", "1.0")]));
        // user 2 changed and user 3 closed without an observed fill; user 4 appeared without one
        let report = book.install_anchor(anchor(1_100, &[(PUMP, 0, &[(user(1), 5), (user(2), 8), (user(4), 3)])]));
        assert_eq!(shown(&book, PUMP), [(user(1), 6, 1_200), (user(2), 8, 1_100), (user(4), 3, 1_100)]);
        assert_eq!((report.silent_position_changes, report.previous_anchor), (3, Some(1_000)));
        let mut samples: Vec<_> =
            report.silent_samples.iter().map(|change| (change.user, change.projected, change.anchor_size_raw)).collect();
        samples.sort_unstable();
        let expected = [(user(2), Some((7, 1_000)), 8), (user(3), Some((1, 1_000)), 0), (user(4), None, 3)];
        assert_eq!(samples, expected, "changed, closed and opened without a fill");
        let view = book.coin_positions(PUMP).unwrap();
        assert!(view.positions.iter().all(|position| position.anchor_row == Some(row())), "rows come from the new anchor");
    }

    #[test]
    fn a_position_opened_after_the_anchor_is_pending_until_the_next() {
        let mut book = trusted_book(&[]);
        book.apply_fills(fills(1_100, &[(user(1), PUMP, "B", 1, "0.0", "2.0")]));
        let view = book.coin_positions(PUMP).unwrap();
        assert_eq!(view.positions[0].anchor_row, None);
        assert_eq!(view.positions[0].account_mode, None);
        assert_eq!(view.positions[0].changes, ChangeMarks { fill: Some(1_100), ..ChangeMarks::default() });
        book.install_anchor(anchor(1_200, &[(PUMP, 0, &[(user(1), 2)])]));
        let view = book.coin_positions(PUMP).unwrap();
        assert_eq!(view.positions[0].anchor_row, Some(row()));
        assert_eq!(view.positions[0].account_mode, Some(AccountMode::Unified));
        assert_eq!(view.positions[0].changes, ChangeMarks::default(), "the anchor covers the fill");
    }

    #[test]
    fn a_coin_leaving_the_universe_drops_its_records() {
        let mut book = trusted_book(&[(user(1), 5)]);
        let report = book.install_anchor(anchor(1_100, &[("BTC", 5, &[])]));
        assert_eq!(report.dropped_coin_records, 1);
        assert_eq!(book.coin_positions(PUMP), Err(QueryError::UnknownCoin));
    }

    #[test]
    fn a_size_decimals_change_resyncs_the_coin_from_anchors() {
        let mut book = trusted_book(&[(user(1), 5)]);
        book.apply_fills(fills(1_200, &[(user(1), PUMP, "B", 1, "5.0", "1.0")]));
        let report = book.install_anchor(anchor(1_100, &[(PUMP, 1, &[(user(1), 50)])]));
        assert_eq!(report.merge_errors.len(), 1);
        assert_eq!(size(&book, PUMP, user(1)), Some(50));
        assert_eq!(book.availability(), Err(fills_lost_up_to(1_200)), "the dropped fill record");
        book.install_anchor(anchor(1_300, &[(PUMP, 1, &[(user(1), 60)])]));
        assert!(book.availability().is_ok());
    }

    // ---- fills outside the universe ----

    #[test]
    fn fills_before_the_first_anchor_are_held_and_replayed_against_it() {
        let mut book = AccountBook::new(0);
        book.apply_fills(fills(500, &[(user(1), PUMP, "B", 1, "0.0", "2.0")]));
        book.apply_misc_events(misc(600, &[]));
        book.apply_fills(fills(1_200, &[(user(1), PUMP, "B", 2, "2.0", "1.0"), (user(1), "@107", "B", 3, "0.0", "0.123456789")]));
        assert_eq!(book.availability(), Err(Unavailable::Initializing));
        let report = book.install_anchor(anchor(1_000, &[(PUMP, 0, &[(user(1), 2)])]));
        assert_eq!((report.covered_held_fills, report.replayed_fills), (1, 1));
        assert_eq!(shown(&book, PUMP), [(user(1), 3, 1_200)]);
        assert_eq!(book.status().held_fills, 1, "the spot fill waits for an anchor covering it");
        let report = book.install_anchor(anchor(1_300, &[(PUMP, 0, &[(user(1), 3)])]));
        assert_eq!(report.covered_held_fills, 1);
        assert_eq!(book.status().held_fills, 0);
    }

    #[test]
    fn a_coin_listed_after_the_anchor_replays_its_held_fills() {
        let mut book = trusted_book(&[]);
        let report = book.apply_fills(fills(1_100, &[(user(1), "xyz:NEW", "B", 1, "4.0", "1.5")]));
        assert_eq!(report.held, 1);
        let report = book.install_anchor(anchor(1_050, &[(PUMP, 0, &[]), ("xyz:NEW", 1, &[(user(1), 40)])]));
        assert_eq!(report.replayed_fills, 1);
        assert_eq!(shown(&book, "xyz:NEW"), [(user(1), 55, 1_100)]);
    }

    #[test]
    fn an_overflowing_hold_is_a_fills_loss() {
        let mut book = trusted_book(&[]);
        let mut report = FillsReport::default();
        for tid in 0..=MAX_HELD_FILLS as u64 {
            let fill = ObservedFill { coin: "@1".to_string(), tid, side: Side::Bid, start_position: "0".into(), sz: "1".into() };
            book.apply_or_hold(1_100, user(1), fill, &mut report);
        }
        assert!(report.hold_overflowed);
        assert_eq!(book.status().held_fills, 1);
        assert_eq!(book.availability(), Err(fills_lost_up_to(1_100)));
    }

    #[test]
    fn a_universe_fill_breaking_the_size_contract_is_a_fills_loss() {
        let mut book = trusted_book(&[]);
        let report = book.apply_fills(fills(1_100, &[(user(1), PUMP, "B", 1, "0.0", "0.5")]));
        assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
        assert!(report.errors[0].contains("PUMP tid 1"), "{}", report.errors[0]);
        assert_eq!(size(&book, PUMP, user(1)), None);
        assert_eq!(book.availability(), Err(fills_lost_up_to(1_100)));
    }

    #[test]
    fn a_replayed_fill_breaking_the_size_contract_is_a_fills_loss() {
        let mut book = trusted_book(&[]);
        book.apply_fills(fills(1_100, &[(user(1), "xyz:NEW", "B", 1, "0.0", "0.25")]));
        let report = book.install_anchor(anchor(1_050, &[(PUMP, 0, &[]), ("xyz:NEW", 1, &[])]));
        assert_eq!((report.replay_errors.len(), report.replayed_fills), (1, 0));
        assert_eq!(book.availability(), Err(fills_lost_up_to(1_100)));
    }

    #[test]
    fn a_float_printed_start_position_applies_exactly() {
        let mut book = trusted_book(&[]);
        book.install_anchor(anchor(1_050, &[(PUMP, 0, &[]), ("STRK", 1, &[(user(1), 859_820_891)])]));
        book.apply_fills(fills(1_100, &[(user(1), "STRK", "B", 1, "85982089.099999994", "197.4")]));
        assert_eq!(shown(&book, "STRK"), [(user(1), 859_822_865, 1_100)]);
    }

    // ---- pushes (design 000419 r2 4.2) ----

    #[test]
    fn every_push_bumps_the_coin_version_and_a_query_does_not() {
        let mut book = trusted_book(&[(user(1), 5), (user(2), 3)]);
        assert_eq!(book.coin_positions(PUMP).unwrap().version, 0);
        let report = book.apply_fills(fills(1_100, &[(user(1), PUMP, "A", 1, "5.0", "5.0")]));
        let changes = push(&mut book, &report.touched).unwrap();
        assert_eq!((changes.version, changes.rows), (1, vec![(user(1), None)]), "the close is a removed row");
        assert_eq!(book.coin_positions(PUMP).unwrap().version, 1, "a subscriber's snapshot carries the version as is");
        let untouched: FxHashSet<Address> = std::iter::once(user(9)).collect();
        assert_eq!(book.push_changes(PUMP, &untouched, &[user(9)]), Ok(None), "no holder touched: no frame, no bump");
        book.install_anchor(anchor(1_200, &[(PUMP, 0, &[(user(2), 3)])]));
        assert_eq!(book.push_snapshot(PUMP).map(|view| view.version), Ok(2));
        assert_eq!(book.push_snapshot("BTC").map(|view| view.version), Err(QueryError::UnknownCoin));
    }

    #[test]
    fn a_funding_batch_changes_the_rows_of_every_touched_holder() {
        let mut book = trusted_book(&[(user(1), 5), (user(2), -3), (user(3), 1)]);
        let funding = format!(r#"{{"Funding":{{"deltas":[{{"user":"{:#x}"}},{{"user":"{:#x}"}}]}}}}"#, user(1), user(2));
        let report = book.apply_misc_events(misc(1_100, &[&funding]));
        let changes = push(&mut book, &report.touched).unwrap();
        let rows: Vec<(Address, Option<u64>)> =
            changes.rows.iter().map(|(user, row)| (*user, row.as_ref().and_then(|row| row.changes.funding))).collect();
        assert_eq!(rows, [(user(1), Some(1_100)), (user(2), Some(1_100))]);
    }

    /// The PUMP push of a batch, as the publisher builds it.
    fn push(book: &mut AccountBook, touched: &Touched) -> Option<CoinChanges> {
        let accounts: FxHashSet<Address> = touched.accounts.iter().copied().collect();
        let positions: Vec<Address> =
            touched.positions.iter().filter(|(coin, _)| coin == PUMP).map(|(_, user)| *user).collect();
        book.push_changes(PUMP, &accounts, &positions).unwrap()
    }

    /// An account trading every block changes its rows once per anchor interval (its first mark), not per block.
    #[test]
    fn only_a_new_mark_or_a_moved_position_changes_a_row() {
        let mut book = trusted_book(&[(user(1), 5)]);
        let first = book.apply_fills(fills(1_100, &[(user(1), "BTC", "B", 1, "0.0", "1.0")]));
        assert_eq!(first.touched.accounts, [user(1)], "the first fill after the anchor marks the account");
        assert_eq!(push(&mut book, &first.touched).unwrap().rows.len(), 1, "its PUMP row shows the new mark");
        let again = book.apply_fills(fills(1_101, &[(user(1), "BTC", "B", 2, "0.0", "1.0")]));
        assert!(again.touched.accounts.is_empty(), "the mark stays at the first block");
        assert_eq!(push(&mut book, &again.touched), None, "a later fill elsewhere leaves the PUMP row alone");
        let in_pump = book.apply_fills(fills(1_102, &[(user(1), PUMP, "B", 3, "5.0", "1.0")]));
        assert_eq!(in_pump.touched.positions, [(PUMP.to_string(), user(1))]);
        assert_eq!(push(&mut book, &in_pump.touched).unwrap().rows[0].1.as_ref().map(|row| row.size_raw), Some(6));
        let duplicate = book.apply_fills(fills(1_102, &[(user(1), PUMP, "B", 3, "5.0", "1.0")]));
        assert_eq!(duplicate.touched, Touched::default(), "a duplicate fill changes nothing");
    }

    #[test]
    fn a_size_decimals_reset_keeps_the_coin_version() {
        let mut book = trusted_book(&[(user(1), 5)]);
        book.push_snapshot(PUMP).unwrap();
        book.install_anchor(anchor(1_100, &[(PUMP, 1, &[(user(1), 50)])]));
        assert_eq!(book.coins[PUMP].version, 1, "subscribers must never see a version go back");
    }

    // ---- change marks ----

    #[test]
    fn marks_keep_the_first_block_and_the_next_anchor_rebuilds_them_from_the_log() {
        let mut book = trusted_book(&[(user(1), 5)]);
        let ledger = format!(r#"{{"LedgerUpdate":{{"users":["{:#x}","{:#x}"],"delta":{{"type":"send"}}}}}}"#, user(1), user(2));
        let funding = format!(r#"{{"Funding":{{"deltas":[{{"user":"{:#x}"}}]}}}}"#, user(1));
        book.apply_misc_events(misc(1_100, &[&ledger]));
        book.apply_misc_events(misc(1_250, &[&ledger]));
        book.apply_misc_events(misc(1_300, &[&ledger, &funding]));
        assert!(book.apply_misc_events(misc(900, &[&ledger])).stale);
        let marks = book.coin_positions(PUMP).unwrap().positions[0].changes;
        assert_eq!(marks, ChangeMarks { fill: None, ledger: Some(1_100), funding: Some(1_300) }, "first after 1000");
        book.install_anchor(anchor(1_200, &[(PUMP, 0, &[(user(1), 5)])]));
        let marks = book.coin_positions(PUMP).unwrap().positions[0].changes;
        assert_eq!(marks, ChangeMarks { fill: None, ledger: Some(1_250), funding: Some(1_300) }, "first after 1200");
        book.install_anchor(anchor(1_300, &[(PUMP, 0, &[(user(1), 5)])]));
        assert_eq!(book.coin_positions(PUMP).unwrap().positions[0].changes, ChangeMarks::default());
        assert!(!book.accounts.contains_key(&user(2)), "a covered account without a position is forgotten");
        assert_eq!(book.mark_log.len(), 0, "the anchor covers every logged event");
    }

    #[test]
    fn a_late_replay_of_an_older_block_lowers_the_mark() {
        let mut book = trusted_book(&[(user(1), 5)]);
        book.apply_fills(fills(1_200, &[(user(1), "BTC", "B", 1, "0.0", "1.0")]));
        let replay = book.apply_fills(fills(1_150, &[(user(1), "BTC", "B", 2, "0.0", "1.0")]));
        assert_eq!(replay.touched.accounts, [user(1)]);
        assert_eq!(book.coin_positions(PUMP).unwrap().positions[0].changes.fill, Some(1_150));
    }

    #[test]
    fn an_overflowing_mark_log_drops_the_oldest_as_a_loss_of_its_stream() {
        let mut book = trusted_book(&[]);
        let mut dropped = 0;
        for tid in 0..MAX_MARK_LOG as u64 {
            book.mark(user(1), MarkKind::Ledger, 1_100 + tid, &mut dropped);
        }
        assert_eq!(dropped, 0);
        assert!(book.availability().is_ok());
        book.mark(user(1), MarkKind::Fill, 5_000_000, &mut dropped);
        assert_eq!((dropped, book.mark_log.len()), (1, MAX_MARK_LOG));
        let lost = Unavailable::DataLoss { stream: Stream::MiscEvents, bound: LossBound::Block(1_100) };
        assert_eq!(book.availability(), Err(lost), "the dropped ledger event bounds a misc events loss");
    }

    #[test]
    fn a_spot_fill_marks_the_account() {
        let mut book = trusted_book(&[(user(1), 5)]);
        book.apply_fills(fills(1_100, &[(user(1), "@107", "B", 1, "0.0", "1.0")]));
        assert_eq!(book.coin_positions(PUMP).unwrap().positions[0].changes.fill, Some(1_100));
    }

    #[test]
    fn unknown_misc_kinds_are_reported_once() {
        let mut book = trusted_book(&[]);
        let report = book.apply_misc_events(misc(1_100, &[r#"{"NewKind":{}}"#, r#"{"NewKind":{}}"#]));
        assert_eq!((report.unknown_events, report.new_unknown_kinds), (2, vec!["NewKind".to_string()]));
        let report = book.apply_misc_events(misc(1_101, &[r#"{"NewKind":{}}"#]));
        assert_eq!((report.unknown_events, report.new_unknown_kinds.len()), (1, 0));
    }

    // ---- trust ----

    #[test]
    fn the_first_anchor_must_be_a_margin_past_the_watcher_start() {
        let started_ms = 2_000_000;
        let mut book = AccountBook::new(started_ms);
        book.apply_fills(fills(2_000, &[]));
        book.apply_misc_events(misc(2_000, &[]));
        assert_eq!(book.availability(), Err(Unavailable::Initializing));
        // within the margin of the start: lines written just before it may belong to blocks above this anchor
        book.install_anchor(Anchor { time_ms: started_ms + CLOCK_MARGIN_MS - 1, ..anchor(2_000, &[(PUMP, 0, &[])]) });
        let before_start = LossBound::WrittenBy(started_ms);
        assert_eq!(book.availability(), Err(Unavailable::DataLoss { stream: Stream::Fills, bound: before_start }));
        book.install_anchor(Anchor { time_ms: started_ms + CLOCK_MARGIN_MS, ..anchor(2_001, &[(PUMP, 0, &[])]) });
        assert!(book.availability().is_ok());
    }

    #[test]
    fn a_stream_that_read_no_line_keeps_the_view_initializing() {
        let mut book = AccountBook::new(0);
        book.apply_fills(fills(100, &[]));
        book.install_anchor(anchor(1_000, &[(PUMP, 0, &[])]));
        assert_eq!(book.availability(), Err(Unavailable::Initializing));
    }

    /// Review 000422 P2: the lost line's own block bounds the loss; a later replay of an older block proves nothing.
    #[test]
    fn a_lost_line_above_the_anchor_is_not_covered_by_a_later_replay() {
        let mut book = AccountBook::new(0);
        book.apply_fills(fills(1_200, &[]));
        book.apply_misc_events(misc(1_200, &[]));
        book.install_anchor(anchor(1_300, &[(PUMP, 0, &[(user(1), 5)])]));
        // the close at 1500 is lost (its body broke the schema, its header still reads), then 1100 is replayed
        book.record_lost_block(Stream::Fills, 1_500);
        book.apply_fills(fills(1_100, &[]));
        assert_eq!(book.availability(), Err(fills_lost_up_to(1_500)));
        book.install_anchor(anchor(1_499, &[(PUMP, 0, &[(user(1), 5)])]));
        assert_eq!(book.availability(), Err(fills_lost_up_to(1_500)));
        book.install_anchor(anchor(1_500, &[(PUMP, 0, &[])]));
        assert!(book.availability().is_ok());
        assert_eq!(shown(&book, PUMP).len(), 0, "the anchor carries the close");
    }

    #[test]
    fn a_loss_of_unknown_blocks_waits_for_an_anchor_a_margin_past_its_detection() {
        let mut book = trusted_book(&[]);
        book.record_unknown_loss(Stream::MiscEvents, 1_500_000);
        book.apply_misc_events(misc(900, &[]));
        let bound = LossBound::WrittenBy(1_500_000);
        assert_eq!(book.availability(), Err(Unavailable::DataLoss { stream: Stream::MiscEvents, bound }));
        book.install_anchor(anchor(1_500, &[(PUMP, 0, &[])]));
        assert_eq!(book.availability(), Err(Unavailable::DataLoss { stream: Stream::MiscEvents, bound }));
        book.install_anchor(anchor(1_501, &[(PUMP, 0, &[])]));
        assert!(book.availability().is_ok());
    }

    #[test]
    fn a_covered_loss_does_not_excuse_a_later_one() {
        let mut book = trusted_book(&[]);
        book.record_lost_block(Stream::Fills, 900);
        assert!(book.availability().is_ok(), "inside the installed anchor");
        book.record_lost_block(Stream::Fills, 1_100);
        assert_eq!(book.availability(), Err(fills_lost_up_to(1_100)));
    }

    #[test]
    fn a_failed_checkpoint_withholds_until_a_newer_anchor_and_stop_is_permanent() {
        let mut book = trusted_book(&[]);
        book.record_checkpoint_failure(1_100, "truncated".to_string());
        assert_eq!(
            book.availability(),
            Err(Unavailable::CheckpointFailed { height: 1_100, error: "truncated".to_string() })
        );
        book.install_anchor(anchor(1_200, &[(PUMP, 0, &[])]));
        assert!(book.availability().is_ok());
        book.stop("fills watcher exited".to_string());
        book.install_anchor(anchor(1_300, &[(PUMP, 0, &[])]));
        assert_eq!(book.availability(), Err(Unavailable::Stopped("fills watcher exited".to_string())));
        assert!(matches!(book.coin_positions(PUMP), Err(QueryError::Unavailable(Unavailable::Stopped(_)))));
    }

    #[test]
    #[should_panic(expected = "is not newer")]
    fn anchors_only_move_forward() {
        let mut book = trusted_book(&[]);
        book.install_anchor(anchor(1_000, &[(PUMP, 0, &[])]));
    }

    #[test]
    fn the_view_lists_open_positions_by_user_with_stream_heights() {
        let mut book = trusted_book(&[(user(3), 1), (user(1), -4)]);
        book.apply_fills(fills(1_100, &[(user(2), PUMP, "A", 1, "0.0", "2.0"), (user(3), PUMP, "A", 2, "1.0", "1.0")]));
        let view = book.coin_positions(PUMP).unwrap();
        assert_eq!((view.fills_height, view.misc_events_height, view.size_decimals), (1_100, 100, 0));
        assert_eq!(shown(&book, PUMP), [(user(1), -4, 1_000), (user(2), -2, 1_100)]);
        assert_eq!(book.status().open_positions, 2);
    }
}
