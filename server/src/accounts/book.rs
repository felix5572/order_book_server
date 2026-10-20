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
//! - **Change marks**: per account, the first block after the anchor of each observed source (fill, ledger, funding,
//!   and the block actions margin, account mode, withdrawal): from there on the anchor's liquidation price may be
//!   stale. A mark is set once per anchor interval, so an account trading every block changes its rows once, not per
//!   block (review of the push volume, 10-19). The account events since the anchor are logged so the next anchor can
//!   rebuild the first marks after itself, and check and rebuild the cash replay (`replay.rs`, design 000433). A block-action mark means a successful action that may invalidate the
//!   anchor, not that the state changed (setting the mode an account already has marks it too). An empty set does not
//!   prove the account unchanged: the marks cover the sources above, not mark price moves of other positions.
//! - **Trust**: served only with an anchor covering every recorded loss of the three streams, no newer checkpoint that
//!   failed to parse, and live inputs. A loss of known blocks (an unparsable line whose header still reads, a fill
//!   breaking the size contract, an overflowing hold, a gap in the block heights of `replica_cmds`) is covered by an
//!   anchor at or above its highest block. A loss of unknown blocks (data the watcher discarded, a line without a
//!   readable header, and everything written before the watcher started) was written by the time it was detected, so
//!   it is covered by an anchor whose block time is `CLOCK_MARGIN_MS` past the detection: block heights grow with block
//!   time, whatever order the lines came in.

use std::{
    cmp::Ordering,
    collections::{VecDeque, hash_map::Entry},
    fmt,
};

use alloy::primitives::Address;
use rustc_hash::{FxHashMap, FxHashSet};

use super::{
    actions::{BlockActionKind, BlockActions},
    anchor::{Anchor, AnchorCoin, AnchorPosition, AnchorRow},
    checkpoint::{AccountMode, mark_px},
    misc::{FundingDelta, MiscEvent},
    liquidation::{LiquidationPx, RowMark, UnsupportedReason},
    marks::MarksAt,
    replay::{AccountEvent, FillEvent, HeldPosition, MarginStates, ReplayedRow, SelfCheck, Stop},
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

/// Account events kept to rebuild the marks and the cash replay at the next anchor: one anchor interval is ~80k fills,
/// ~6k block actions plus, across an hour boundary, ~400k funding deltas; this is hours without an anchor. Overflowing
/// drops the oldest event, a loss up to its block on its stream.
const MAX_EVENT_LOG: usize = 2_000_000;

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
    /// `replica_cmds`: every block, with its actions and their responses.
    BlockActions,
    /// `hip3_oracle_updates_streaming`: read by the node marks (design 000439), not by the account view.
    Hip3OracleUpdates,
    /// `node_raw_book_diffs_streaming`: read by the node marks, not by the account view.
    BookDiffs,
    /// `node_order_statuses_streaming`, cancels by cloid and triggers: read by the node marks, not by the account view.
    OrderStatuses,
}

impl Stream {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Fills => "fills",
            Self::MiscEvents => "misc_events",
            Self::BlockActions => "block_actions",
            Self::Hip3OracleUpdates => "hip3_oracle_updates",
            Self::BookDiffs => "book_diffs",
            Self::OrderStatuses => "order_statuses",
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
    pub(crate) block_actions_height: u64,
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
    pub(crate) margin: Option<u64>,
    pub(crate) account_mode: Option<u64>,
    pub(crate) withdrawal: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarkKind {
    Fill,
    Ledger,
    Funding,
    Action(BlockActionKind),
}

impl MarkKind {
    const fn of(event: &AccountEvent) -> Self {
        match event {
            AccountEvent::Fill(_) | AccountEvent::UnreadableFill(_) => Self::Fill,
            AccountEvent::Funding { .. } => Self::Funding,
            AccountEvent::Ledger(_) => Self::Ledger,
            AccountEvent::Action(action) => Self::Action(action.kind()),
        }
    }

    const fn stream(self) -> Stream {
        match self {
            Self::Fill => Stream::Fills,
            Self::Ledger | Self::Funding => Stream::MiscEvents,
            Self::Action(_) => Stream::BlockActions,
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
            MarkKind::Action(BlockActionKind::Margin) => &mut self.margin,
            MarkKind::Action(BlockActionKind::AccountMode) => &mut self.account_mode,
            MarkKind::Action(BlockActionKind::Withdrawal) => &mut self.withdrawal,
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
        let Self { fill, ledger, funding, margin, account_mode, withdrawal } = self;
        fill.is_none()
            && ledger.is_none()
            && funding.is_none()
            && margin.is_none()
            && account_mode.is_none()
            && withdrawal.is_none()
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
    /// The margin table of the position the record holds (design 000437): the anchor's `M`, or the coin's current
    /// table for a position opened from zero; a flip keeps it (research: 4/4 telling samples). `None` when unknown (a
    /// first fill that does not start from zero, a later anchor contradicting the record).
    table_id: Option<u32>,
    /// The block after the anchor where this position was opened from zero; `None` for one the anchor holds.
    opened_after: Option<u64>,
}

/// A fill applied to a position: its block, identity, the size before and after it, and the coin's current table.
struct SizeChange {
    block: u64,
    identity: FillIdentity,
    start: i64,
    size_after: i64,
    current_table: u32,
}

impl PositionRecord {
    fn anchored(height: u64, position: AnchorPosition) -> Self {
        Self {
            size_raw: position.size_raw,
            version_block: height,
            applied: Applied::Anchor,
            anchor_row: Some(position.row),
            table_id: Some(position.table_id),
            opened_after: None,
        }
    }

    fn opened(change: &SizeChange) -> Self {
        let mut record = Self {
            size_raw: change.start,
            version_block: change.block,
            applied: Applied::Fills(vec![change.identity]),
            anchor_row: None,
            table_id: None,
            opened_after: None,
        };
        record.set_size(change);
        record
    }

    /// Returns whether the fill was applied.
    fn apply_fill(&mut self, change: &SizeChange) -> bool {
        match change.block.cmp(&self.version_block) {
            // a replay of an older block
            Ordering::Less => false,
            Ordering::Equal => match &mut self.applied {
                Applied::Anchor => false,
                Applied::Fills(applied) if applied.contains(&change.identity) => false,
                Applied::Fills(applied) => {
                    applied.push(change.identity);
                    self.set_size(change);
                    true
                }
            },
            Ordering::Greater => {
                self.version_block = change.block;
                self.applied = Applied::Fills(vec![change.identity]);
                self.set_size(change);
                true
            }
        }
    }

    /// A position opened from zero takes the coin's current table; any other change keeps the position's.
    const fn set_size(&mut self, change: &SizeChange) {
        if change.start == 0 && change.size_after != 0 {
            self.table_id = Some(change.current_table);
            self.opened_after = Some(change.block);
        }
        self.size_raw = change.size_after;
    }

    /// The record of a position set after anchor `height` meets that anchor: its row is the anchor's, and so is its
    /// table unless this position was opened after the anchor (an anchor without it leaves the table unknown).
    fn meet_earlier_anchor(&mut self, height: u64, anchored: Option<AnchorPosition>) {
        if self.opened_after.is_none_or(|block| block <= height) {
            self.table_id = anchored.as_ref().map(|position| position.table_id);
            self.opened_after = None;
        }
        self.anchor_row = anchored.map(|position| position.row);
    }
}

#[derive(Debug)]
struct CoinBook {
    size_decimals: u32,
    /// The anchor's current table of the coin: a position opened from zero takes it.
    table_id: u32,
    records: FxHashMap<Address, PositionRecord>,
    /// Bumped by every frame pushed for this coin (a `changes` batch or the snapshot after an anchor install); a new
    /// subscriber's snapshot carries it without bumping (design 000419 r2 4.2).
    version: u64,
}

impl CoinBook {
    fn anchored(height: u64, coin: AnchorCoin, version: u64) -> Self {
        let records = coin.positions.into_iter().map(|(user, position)| (user, PositionRecord::anchored(height, position)));
        Self { size_decimals: coin.size_decimals, table_id: coin.table_id, records: records.collect(), version }
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
                record.meet_earlier_anchor(height, anchored);
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

    /// The size before and after the fill.
    fn sizes(&self, size_decimals: u32) -> Result<(i64, i64), String> {
        let start = parse_size_raw(&self.start_position, size_decimals)?;
        let size = parse_size_raw(&self.sz, size_decimals)?;
        if size <= 0 {
            return Err(format!("fill size {:?} is not positive", self.sz));
        }
        let delta = match self.side {
            Side::Bid => size,
            Side::Ask => -size,
        };
        let after = start.checked_add(delta).ok_or_else(|| format!("startPosition {:?} + {delta} overflows", self.start_position))?;
        Ok((start, after))
    }
}

/// What a stream has read and what it is known to have lost; losses only accumulate (anchors only move forward).
#[derive(Debug)]
pub(crate) struct StreamProgress {
    /// Highest block read.
    height: Option<u64>,
    /// Highest block of a line known to be lost.
    lost_block: Option<u64>,
    /// Latest wall time (unix ms) a loss of unknown blocks was detected.
    lost_by_ms: Option<u64>,
}

impl StreamProgress {
    /// Nothing written before the watcher started was read.
    pub(crate) const fn starting(started_ms: u64) -> Self {
        Self { height: None, lost_block: None, lost_by_ms: Some(started_ms) }
    }

    pub(crate) fn record_read(&mut self, block: u64) {
        raise(&mut self.height, block);
    }

    pub(crate) fn record_lost_block(&mut self, block: u64) {
        raise(&mut self.lost_block, block);
    }

    pub(crate) fn record_unknown_loss(&mut self, detected_ms: u64) {
        raise(&mut self.lost_by_ms, detected_ms);
    }

    /// The highest block read, once `anchor` covers every loss.
    pub(crate) fn check(&self, stream: Stream, anchor: AnchorInfo) -> Result<u64, Unavailable> {
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

impl Touched {
    /// Adds the rows another batch changed.
    pub(crate) fn absorb(&mut self, other: Self) {
        self.accounts.extend(other.accounts);
        self.positions.extend(other.positions);
    }
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

/// What one `replica_cmds` block did.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct BlockActionsReport {
    pub(crate) stale: bool,
    /// Blocks between the previous height read and this one that never arrived (recorded as a loss up to the last).
    pub(crate) missing: Option<(u64, u64)>,
    pub(crate) touched: Touched,
    /// Oldest mark events dropped for room (recorded as losses up to their blocks).
    pub(crate) marks_dropped: usize,
}

/// What one released block did, per stream.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct BlockReport {
    pub(crate) block_actions: BlockActionsReport,
    pub(crate) fills: Vec<FillsReport>,
    pub(crate) misc_events: Vec<MiscReport>,
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
#[derive(Debug, Default, PartialEq)]
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
    pub(crate) replay_check: ReplayCheck,
}

impl Default for ReplayCheck {
    fn default() -> Self {
        Self::Skipped("noPreviousAnchor")
    }
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
    /// The replayed row (design 000437); when present it replaces the anchor's row.
    pub(crate) replayed: Option<Replayed>,
    /// Why the account's replay stopped, and where; its rows keep the anchor's values.
    pub(crate) replay_stop: Option<Stop>,
}

/// A replayed row with the block whose state it is, and the node mark of that block (design 000442).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Replayed {
    pub(crate) height: u64,
    pub(crate) row: ReplayedRow,
    pub(crate) mark: RowMark,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CoinPositions {
    pub(crate) coin: String,
    /// The coin's version (see `CoinBook::version`).
    pub(crate) version: u64,
    pub(crate) anchor: AnchorInfo,
    pub(crate) fills_height: u64,
    pub(crate) misc_events_height: u64,
    pub(crate) block_actions_height: u64,
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
    pub(crate) block_actions_height: u64,
    pub(crate) size_decimals: u32,
    /// Touched holders by user: the open position, or `None` once it is closed.
    pub(crate) rows: Vec<(Address, Option<PositionView>)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BookStatus {
    pub(crate) availability: Result<Available, Unavailable>,
    pub(crate) fills_height: Option<u64>,
    pub(crate) misc_events_height: Option<u64>,
    pub(crate) block_actions_height: Option<u64>,
    pub(crate) open_positions: usize,
    pub(crate) held_fills: usize,
}

/// The coins each account holds a non-zero position in (designs 000437, 000442): the members of a cross position's pool.
#[derive(Debug, Default)]
struct Holdings(FxHashMap<Address, FxHashSet<String>>);

impl Holdings {
    fn record(&mut self, user: Address, coin: &str, size_raw: i64) {
        if size_raw != 0 {
            self.0.entry(user).or_default().insert(coin.to_string());
            return;
        }
        if let Entry::Occupied(mut held) = self.0.entry(user) {
            held.get_mut().remove(coin);
            if held.get().is_empty() {
                held.remove();
            }
        }
    }

    fn of(&self, user: &Address) -> impl Iterator<Item = &str> {
        self.0.get(user).into_iter().flatten().map(String::as_str)
    }

    /// Every open position of the books, after an anchor install.
    fn rebuild(coins: &FxHashMap<String, CoinBook>) -> Self {
        let mut holdings = Self::default();
        for (name, coin) in coins {
            for (user, record) in &coin.records {
                holdings.record(*user, name, record.size_raw);
            }
        }
        holdings
    }
}

/// An account event after the anchor.
#[derive(Debug)]
struct LoggedEvent {
    block: u64,
    user: Address,
    event: AccountEvent,
}

/// The cash replay (design 000433 r2 3.6): the anchor's margin state, and that state moved by every event since.
#[derive(Debug)]
struct Replay {
    anchored: MarginStates,
    current: MarginStates,
}

/// The previous interval's replay checked against a new anchor, or why it was not.
#[derive(Debug, PartialEq)]
pub(crate) enum ReplayCheck {
    Checked { cash: SelfCheck, rows: RowCheck },
    Skipped(&'static str),
}

/// Rows within this fraction of the larger of the anchor's price and the coin's mark are equal: the cash is equal to the
/// last 10^-8, so the solver sees the same input but for the order of a float sum. A price far below the mark loses
/// digits to cancellation, so it is not measured against itself (bm 2026-10-20: DOGE 5.9e-6 and PUMP 3.1e-8 differed
/// by 1.1e-12 and 8.8e-12 of themselves).
const ROW_TOLERANCE: f64 = 1e-12;
/// Row mismatches kept as evidence per anchor.
const MAX_ROW_SAMPLES: usize = 20;

/// The replayed rows at the new anchor against its own (design 000437 r2 revision 3, 000442 r2 revision 3): for every
/// position the anchor holds of an account replaying to it on a USDC perp, the replayed cash at the anchor, the
/// anchor's size and pool members, the records' tables as of the anchor and the anchor's stored marks give a row. It
/// checks the pool membership, the members' valuation and the table choice given the anchor's sizes and marks; the
/// live sizes and holdings are checked by the silent position changes and the tests, the node marks by their own check.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct RowCheck {
    pub(crate) equal: u64,
    pub(crate) differ: u64,
    /// The replay gives no row where the anchor has a price to compare, or a pool of another size.
    pub(crate) class_differs: u64,
    /// Positions whose account has a record that moved after the anchor (its own, or a member's its row needs): their
    /// table as of the anchor is not kept.
    pub(crate) moved_after: u64,
    pub(crate) samples: Vec<RowMismatch>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RowMismatch {
    pub(crate) coin: String,
    pub(crate) user: Address,
    pub(crate) replayed: Option<ReplayedRow>,
    pub(crate) anchor: AnchorRow,
}

/// Whether two rows' prices agree within `ROW_TOLERANCE` of `right` or of `mark`, the coin's mark in USD.
fn same_price(left: &LiquidationPx, right: &LiquidationPx, mark: Option<f64>) -> bool {
    match (left, right) {
        (LiquidationPx::Computed(left), LiquidationPx::Computed(right)) => {
            (left - right).abs() <= ROW_TOLERANCE * right.abs().max(mark.unwrap_or(0.0))
        }
        _ => left == right,
    }
}

#[derive(Debug)]
pub(crate) struct AccountBook {
    anchor: Option<AnchorInfo>,
    /// Every coin of the anchor's universe.
    coins: FxHashMap<String, CoinBook>,
    accounts: FxHashMap<Address, AccountRecord>,
    held_fills: Vec<(u64, Address, ObservedFill)>,
    /// Account events after the anchor, oldest first.
    events: VecDeque<LoggedEvent>,
    /// Whether the event log dropped events since the anchor (the replay of the interval is then incomplete).
    events_dropped: bool,
    replay: Option<Replay>,
    holdings: Holdings,
    fills: StreamProgress,
    misc_events: StreamProgress,
    block_actions: StreamProgress,
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
            events: VecDeque::new(),
            events_dropped: false,
            replay: None,
            holdings: Holdings::default(),
            fills: StreamProgress::starting(started_ms),
            misc_events: StreamProgress::starting(started_ms),
            block_actions: StreamProgress::starting(started_ms),
            checkpoint_failure: None,
            stopped: None,
            unknown_misc_kinds: FxHashSet::default(),
        }
    }

    fn covered(&self, block: u64) -> bool {
        self.anchor.is_some_and(|anchor| block <= anchor.height)
    }

    /// The view's progress on `stream`; `None` for the node marks' streams, which it does not read.
    const fn progress(&mut self, stream: Stream) -> Option<&mut StreamProgress> {
        match stream {
            Stream::Fills => Some(&mut self.fills),
            Stream::MiscEvents => Some(&mut self.misc_events),
            Stream::BlockActions => Some(&mut self.block_actions),
            Stream::Hip3OracleUpdates | Stream::BookDiffs | Stream::OrderStatuses => None,
        }
    }

    /// The one entry of a released block (design 000433 r3, 3.4): its `replica_cmds` actions, then its fills, then its
    /// misc events, each through the stream's own application below (the block's other streams are the node marks').
    pub(crate) fn apply_block(
        &mut self,
        actions: BlockActions,
        fills: Vec<Batch<NodeDataFill>>,
        misc_events: Vec<Batch<MiscEvent>>,
    ) -> BlockReport {
        BlockReport {
            block_actions: self.apply_block_actions(actions),
            fills: fills.into_iter().map(|batch| self.apply_fills(batch)).collect(),
            misc_events: misc_events.into_iter().map(|batch| self.apply_misc_events(batch)).collect(),
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
            let event = FillEvent::of(&fill).map_or_else(
                |err| {
                    report.errors.push(format!("block {block} {user}: {err}"));
                    raise(&mut self.fills.lost_block, block);
                    AccountEvent::UnreadableFill(err)
                },
                AccountEvent::Fill,
            );
            if self.record(user, event, block, &mut report.marks_dropped) {
                report.touched.accounts.push(user);
            }
            self.apply_or_hold(block, user, ObservedFill::new(fill), &mut report);
        }
        report
    }

    /// Logs an account event, moves the replayed cash and lowers the account's mark; returns whether the account's rows
    /// may have changed: a new mark, or a replaying account (design 000437 4).
    fn record(&mut self, user: Address, event: AccountEvent, block: u64, dropped: &mut usize) -> bool {
        if self.events.len() >= MAX_EVENT_LOG
            && let Some(oldest) = self.events.pop_front()
        {
            if let Some(progress) = self.progress(MarkKind::of(&oldest.event).stream()) {
                progress.record_lost_block(oldest.block);
            }
            self.events_dropped = true;
            *dropped += 1;
        }
        let replaying = self.replay.as_mut().is_some_and(|replay| replay.current.apply(block, user, &event));
        let marked = self.accounts.entry(user).or_default().marks.mark(MarkKind::of(&event), block);
        self.events.push_back(LoggedEvent { block, user, event });
        marked || replaying
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
        let (start, size_after) = match fill.sizes(coin.size_decimals) {
            Ok(sizes) => sizes,
            Err(err) => {
                report.errors.push(format!("block {block} {user} {} tid {}: {err}", fill.coin, fill.tid));
                raise(&mut self.fills.lost_block, block);
                return;
            }
        };
        let change = SizeChange { block, identity: (fill.tid, fill.side), start, size_after, current_table: coin.table_id };
        let applied = match coin.records.entry(user) {
            Entry::Vacant(entry) => {
                entry.insert(PositionRecord::opened(&change));
                true
            }
            Entry::Occupied(mut entry) => entry.get_mut().apply_fill(&change),
        };
        if applied {
            self.holdings.record(user, &fill.coin, size_after);
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
                MiscEvent::Ledger { users, delta } => {
                    let events = users.into_iter().map(|user| (user, AccountEvent::Ledger(delta.clone())));
                    self.record_all(events, block, &mut report.touched, &mut report.marks_dropped);
                }
                MiscEvent::Funding(deltas) => {
                    let events = deltas
                        .into_iter()
                        .map(|FundingDelta { user, coin, amount_wei }| (user, AccountEvent::Funding { coin, amount_wei }));
                    self.record_all(events, block, &mut report.touched, &mut report.marks_dropped);
                }
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

    /// Records each account's event of `block`; the accounts whose marks changed go to `touched`.
    fn record_all(
        &mut self,
        events: impl IntoIterator<Item = (Address, AccountEvent)>,
        block: u64,
        touched: &mut Touched,
        dropped: &mut usize,
    ) {
        for (user, event) in events {
            if self.record(user, event, block, dropped) {
                touched.accounts.push(user);
            }
        }
    }

    /// Every block has a line, so a height past the next one is a loss of the blocks between.
    pub(crate) fn apply_block_actions(&mut self, block: BlockActions) -> BlockActionsReport {
        let BlockActions { height, actions } = block;
        let mut report = BlockActionsReport::default();
        if let Some(previous) = self.block_actions.height
            && height > previous + 1
        {
            raise(&mut self.block_actions.lost_block, height - 1);
            report.missing = Some((previous + 1, height - 1));
        }
        raise(&mut self.block_actions.height, height);
        if self.covered(height) {
            report.stale = true;
            return report;
        }
        let events = actions.into_iter().map(|(action, user)| (user, AccountEvent::Action(action)));
        self.record_all(events, height, &mut report.touched, &mut report.marks_dropped);
        report
    }

    /// Installs a newer anchor. Heights only increase: the poller sends each checkpoint once, newest first.
    pub(crate) fn install_anchor(&mut self, anchor: Anchor) -> InstallReport {
        let Anchor { height, time_ms, coins: mut anchor_coins, account_modes, margins } = anchor;
        assert!(
            self.anchor.is_none_or(|current| current.height < height),
            "anchor {height} is not newer than the installed {:?}",
            self.anchor
        );
        let replay_check = self.check_replay(&margins, &anchor_coins, height);
        let mut report =
            InstallReport { previous_anchor: self.anchor.map(|anchor| anchor.height), replay_check, ..InstallReport::default() };
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
            coin.table_id = anchor_coin.table_id;
            coin.merge(name, height, anchor_coin.positions, &mut report);
            true
        });
        self.fills.lost_block = lost_block;
        self.coins.extend(anchor_coins.into_iter().map(|(name, coin)| (name, CoinBook::anchored(height, coin, 0))));
        self.rebuild_marks(height);
        self.rebuild_replay(margins);
        for (user, mode) in account_modes {
            self.accounts.entry(user).or_default().mode = Some(mode);
        }
        self.anchor = Some(AnchorInfo { height, time_ms });
        if self.checkpoint_failure.as_ref().is_some_and(|(failed, _)| *failed < height) {
            self.checkpoint_failure = None;
        }
        self.replay_held_fills(height, &mut report);
        self.holdings = Holdings::rebuild(&self.coins);
        report
    }

    /// The marks after `height`: the first logged block of each kind above it; accounts with neither marks nor (soon) a
    /// mode are forgotten.
    fn rebuild_marks(&mut self, height: u64) {
        for account in self.accounts.values_mut() {
            account.mode = None;
            account.marks = ChangeMarks::default();
        }
        self.events.retain(|logged| logged.block > height);
        for LoggedEvent { block, user, event } in &self.events {
            self.accounts.entry(*user).or_default().marks.mark(MarkKind::of(event), *block);
        }
        self.accounts.retain(|_, account| !account.marks.is_empty());
    }

    /// Whether installing an anchor at `height` now keeps the interval it closes checkable (design 000433 r2 3.4): with
    /// a replay to check, every block up to it is applied first. The first anchor, an untrusted view and an overflowed
    /// log have nothing to check, so an anchor installs at once.
    pub(crate) fn anchor_ready(&self, height: u64) -> bool {
        self.replay.is_none()
            || self.availability().is_err()
            || self.events_dropped
            || self.block_actions.height.is_some_and(|applied| applied >= height)
    }

    /// Step 1 of an install (design 000433 r2 3.6): the previous anchor's state moved by the logged events up to the
    /// new anchor, compared with the new anchor's state. Only a complete interval is checked: trusted under the
    /// previous anchor (no loss), every event logged, and every block up to the new anchor applied.
    fn check_replay(&self, anchor: &MarginStates, anchor_coins: &FxHashMap<String, AnchorCoin>, height: u64) -> ReplayCheck {
        let Some(replay) = &self.replay else {
            return ReplayCheck::Skipped("noPreviousAnchor");
        };
        if self.availability().is_err() {
            return ReplayCheck::Skipped("untrusted");
        }
        if self.events_dropped {
            return ReplayCheck::Skipped("eventLogOverflowed");
        }
        if self.block_actions.height.is_none_or(|applied| applied < height) {
            return ReplayCheck::Skipped("behindTheAnchor");
        }
        let mut replayed = replay.anchored.clone();
        for LoggedEvent { block, user, event } in self.events.iter().filter(|logged| logged.block <= height) {
            replayed.apply(*block, *user, event);
        }
        let cash = replayed.check_against(anchor, &replay.anchored);
        ReplayCheck::Checked { cash, rows: self.check_rows(&replayed, anchor_coins, height) }
    }

    /// See `RowCheck`.
    fn check_rows(&self, replayed: &MarginStates, anchor_coins: &FxHashMap<String, AnchorCoin>, height: u64) -> RowCheck {
        let mut held: FxHashMap<Address, Vec<HeldPosition<'_>>> = FxHashMap::default();
        let mut moved: FxHashSet<Address> = FxHashSet::default();
        for (name, coin) in anchor_coins {
            for (user, position) in &coin.positions {
                let record = self.coins.get(name).and_then(|book| book.records.get(user));
                if record.is_some_and(|record| record.version_block > height) {
                    moved.insert(*user);
                }
                let table_id = record.filter(|record| record.version_block <= height).and_then(|record| record.table_id);
                held.entry(*user).or_default().push(HeldPosition { coin: name, size_raw: position.size_raw, table_id });
            }
        }
        let mark_of = |coin: &str| anchor_coins.get(coin)?.mark_raw;
        let mut check = RowCheck::default();
        for (name, coin) in anchor_coins.iter().filter(|(name, _)| replayed.usdc_perp(name)) {
            let mark = coin.mark_raw.map(|raw| mark_px(raw, coin.size_decimals));
            for (user, position) in coin.positions.iter().filter(|(user, _)| replayed.replays(user)) {
                let Some(record) = self.coins.get(name).and_then(|book| book.records.get(user)) else {
                    continue;
                };
                if record.version_block > height {
                    check.moved_after += 1;
                    continue;
                }
                // a record whose table is unknown has no replayed row, as in the view
                let row = record.table_id.and_then(|table_id| {
                    replayed.replayed_row(user, name, position.size_raw, table_id, held[user].iter().copied(), mark_of)
                });
                let anchor = &position.row;
                match &row {
                    Some(row) if Some(row.pool_positions) != anchor.pool_positions => check.class_differs += 1,
                    Some(row) if same_price(&row.liquidation_px, &anchor.liquidation_px, mark) => {
                        check.equal += 1;
                        continue;
                    }
                    Some(_) => check.differ += 1,
                    None if moved.contains(user) => {
                        check.moved_after += 1;
                        continue;
                    }
                    // a member without a stored mark: neither side has a price
                    None if anchor.liquidation_px == LiquidationPx::Unsupported(UnsupportedReason::MissingMark) => continue,
                    None => check.class_differs += 1,
                }
                if check.samples.len() < MAX_ROW_SAMPLES {
                    check.samples.push(RowMismatch { coin: name.clone(), user: *user, replayed: row, anchor: anchor.clone() });
                }
            }
        }
        check
    }

    /// Step 2: the new anchor's state moved by the logged events above it (the log holds only those by now).
    fn rebuild_replay(&mut self, anchored: MarginStates) {
        let mut current = anchored.clone();
        for LoggedEvent { block, user, event } in &self.events {
            current.apply(*block, *user, event);
        }
        self.replay = Some(Replay { anchored, current });
        self.events_dropped = false;
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

    /// A line of `block` was lost; a stream the view does not read leaves it as it is.
    pub(crate) fn record_lost_block(&mut self, stream: Stream, block: u64) {
        if let Some(progress) = self.progress(stream) {
            progress.record_lost_block(block);
        }
    }

    /// Lines of unknown blocks were lost, detected at wall time `detected_ms` (unix ms); a stream the view does not read
    /// leaves it as it is.
    pub(crate) fn record_unknown_loss(&mut self, stream: Stream, detected_ms: u64) {
        if let Some(progress) = self.progress(stream) {
            progress.record_unknown_loss(detected_ms);
        }
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
        let block_actions_height = self.block_actions.check(Stream::BlockActions, anchor)?;
        Ok(Available { anchor, fills_height, misc_events_height, block_actions_height })
    }

    pub(crate) fn status(&self) -> BookStatus {
        BookStatus {
            availability: self.availability(),
            fills_height: self.fills.height,
            misc_events_height: self.misc_events.height,
            block_actions_height: self.block_actions.height,
            open_positions: self.coins.values().flat_map(|coin| coin.records.values()).filter(|r| r.size_raw != 0).count(),
            held_fills: self.held_fills.len(),
        }
    }

    /// The coin's open positions; `marks` the trusted node marks, if any (rows read them at the applied height only).
    pub(crate) fn coin_positions(&self, name: &str, marks: Option<&dyn MarksAt>) -> Result<CoinPositions, QueryError> {
        let available = self.availability().map_err(QueryError::Unavailable)?;
        let coin = self.coins.get(name).ok_or(QueryError::UnknownCoin)?;
        let mut positions: Vec<PositionView> = coin
            .records
            .iter()
            .filter(|(_, record)| record.size_raw != 0)
            .map(|(user, record)| self.position_view(*user, name, record, marks))
            .collect();
        positions.sort_unstable_by_key(|position| position.user);
        Ok(CoinPositions {
            coin: name.to_string(),
            version: coin.version,
            anchor: available.anchor,
            fills_height: available.fills_height,
            misc_events_height: available.misc_events_height,
            block_actions_height: available.block_actions_height,
            size_decimals: coin.size_decimals,
            positions,
        })
    }

    /// The view right after an anchor install, pushed to the coin's subscribers: bumps the coin's version.
    pub(crate) fn push_snapshot(&mut self, name: &str, marks: Option<&dyn MarksAt>) -> Result<CoinPositions, QueryError> {
        let coin = self.coins.get_mut(name).ok_or(QueryError::UnknownCoin)?;
        coin.version += 1;
        self.coin_positions(name, marks)
    }

    /// The changed rows of `name` (closed ones as `None`) for a `changes` push: every row of `accounts` plus the rows
    /// of `positions`, among the coin's holders. Bumps the coin's version when any; `None` when no row changed.
    pub(crate) fn push_changes(
        &mut self,
        name: &str,
        accounts: &FxHashSet<Address>,
        positions: &[Address],
        marks: Option<&dyn MarksAt>,
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
                (user, (record.size_raw != 0).then(|| self.position_view(user, name, record, marks)))
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
            block_actions_height: available.block_actions_height,
            size_decimals,
            rows,
        }))
    }

    fn position_view(&self, user: Address, coin: &str, record: &PositionRecord, marks: Option<&dyn MarksAt>) -> PositionView {
        let account = self.accounts.get(&user);
        PositionView {
            user,
            size_raw: record.size_raw,
            position_height: record.version_block,
            account_mode: account.and_then(|account| account.mode.clone()),
            anchor_row: record.anchor_row.clone(),
            changes: account.map_or_else(ChangeMarks::default, |account| account.marks),
            replayed: self.replayed_row(user, coin, record, marks),
            replay_stop: self.replay.as_ref().and_then(|replay| replay.current.stop(&user)).cloned(),
        }
    }

    /// The position's replayed row (designs 000437, 000442), once every block up to the anchor is applied: the cash,
    /// the sizes and the marks are then all those of the applied height, which the row carries. Marks of another height
    /// count as none: a pool with other members then has no replayed row.
    fn replayed_row(&self, user: Address, coin: &str, record: &PositionRecord, marks: Option<&dyn MarksAt>) -> Option<Replayed> {
        let replay = self.replay.as_ref()?;
        let anchor = self.anchor?;
        let applied = self.block_actions.height.filter(|applied| *applied >= anchor.height)?;
        let table_id = record.table_id.filter(|_| record.size_raw != 0)?;
        let marks = marks.filter(|marks| marks.height() == applied);
        let mark_of = |held: &str| marks.and_then(|marks| marks.mark(held));
        let held = self.holdings.of(&user).map(|held| {
            // every held coin has an open record (`Holdings`)
            let record = &self.coins[held].records[&user];
            HeldPosition { coin: held, size_raw: record.size_raw, table_id: record.table_id }
        });
        let row = replay.current.replayed_row(&user, coin, record.size_raw, table_id, held, mark_of)?;
        let mark = RowMark::of(record.size_raw, self.coins[coin].size_decimals, mark_of(coin), &row.liquidation_px);
        Some(Replayed { height: applied, row, mark })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::{
        actions::BlockAction,
        checkpoint::{
            parse_checkpoint_bytes,
            tests::{HOLDER, ISOLATED, fixture as checkpoint_fixture, user as checkpoint_user},
        },
        liquidation::{HeldAtMark, LiquidationPx, solve, solve_held},
        margin::MarginTable,
        misc::LedgerDelta,
        replay::{Compared, ReplayedRow, Stop, StopReason, tests::per_dex_with_main_cash},
        test_support::{ANCHOR_TABLE, CURRENT_TABLE, FillSpec, ListedMarks},
    };
    use crate::accounts::test_support::{
        AVAX_ADL_BLOCK, AVAX_ADL_FULL, CoinSpec, ETH_ADL_BLOCK, ETH_ADL_PARTIAL, FARTCOIN_LIQUIDATED_LONG, PUMP,
        SOL_ADL_BLOCK, SOL_ADL_LONG, SOL_LIQUIDATED_SHORT, anchor, block_actions, fills, funding_inner, misc, real_fills, row,
        send_inner, trusted_book, user,
    };

    const fn fills_lost_up_to(block: u64) -> Unavailable {
        Unavailable::DataLoss { stream: Stream::Fills, bound: LossBound::Block(block) }
    }

    fn size(book: &AccountBook, coin: &str, holder: Address) -> Option<i64> {
        book.coins[coin].records.get(&holder).map(|record| record.size_raw)
    }

    fn shown(book: &AccountBook, coin: &str) -> Vec<(Address, i64, u64)> {
        let view = book.coin_positions(coin, None).unwrap();
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

    // ---- ADL: official fills of 2025-10-10 in test envelopes (test_support) ----

    const ADL_ANCHOR: u64 = 758_800_000;

    fn address(text: &str) -> Address {
        text.parse().unwrap()
    }

    /// A trusted book anchored at `ADL_ANCHOR` with these coins.
    fn adl_book(coins: &[CoinSpec<'_>]) -> AccountBook {
        let mut book = AccountBook::new(0);
        book.apply_fills(fills(100, &[]));
        book.apply_misc_events(misc(100, &[]));
        book.apply_block_actions(block_actions(ADL_ANCHOR, &[]));
        book.install_anchor(anchor(ADL_ANCHOR, coins));
        assert_eq!(book.availability().map(|available| available.anchor.height), Ok(ADL_ANCHOR));
        book
    }

    #[test]
    fn adl_fills_set_the_size_from_start_position_partial_and_full() {
        let (eth_user, avax_user) = (address(ETH_ADL_PARTIAL.0), address(AVAX_ADL_FULL.0));
        let mut book = adl_book(&[("ETH", 4, &[(eth_user, -950_367_854)]), ("AVAX", 2, &[(avax_user, -2_853)])]);
        book.apply_fills(real_fills(AVAX_ADL_BLOCK, &[AVAX_ADL_FULL]));
        book.apply_fills(real_fills(ETH_ADL_BLOCK, &[ETH_ADL_PARTIAL]));
        assert_eq!(size(&book, "ETH", eth_user), Some(-461_272_925), "-95,036.7854 + 48,909.4929");
        assert_eq!(size(&book, "AVAX", avax_user), Some(0));
        assert_eq!(shown(&book, "AVAX"), [], "the closed position is not listed");
    }

    #[test]
    fn both_legs_of_an_adl_trade_share_the_tid_and_each_applies_once() {
        let (long, short) = (address(SOL_ADL_LONG.0), address(SOL_LIQUIDATED_SHORT.0));
        let mut book = adl_book(&[("SOL", 2, &[(long, 54), (short, -1)]), ("FARTCOIN", 1, &[(short, 456_680)])]);
        let line = [SOL_ADL_LONG, SOL_LIQUIDATED_SHORT, FARTCOIN_LIQUIDATED_LONG];
        let legs = [("SOL".to_string(), long), ("SOL".to_string(), short), ("FARTCOIN".to_string(), short)];
        for applied in [legs.as_slice(), &[]] {
            let report = book.apply_fills(real_fills(SOL_ADL_BLOCK, &line));
            assert_eq!(report.touched.positions, applied, "every leg once, then none on the replay");
            assert_eq!(shown(&book, "SOL"), [(long, 53, SOL_ADL_BLOCK)]);
            assert_eq!(size(&book, "SOL", short), Some(0));
            assert_eq!(size(&book, "FARTCOIN", short), Some(0), "the liquidated account's other coin");
        }
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
        let view = book.coin_positions(PUMP, None).unwrap();
        assert!(view.positions.iter().all(|position| position.anchor_row == Some(row())), "rows come from the new anchor");
    }

    #[test]
    fn a_position_opened_after_the_anchor_is_pending_until_the_next() {
        let mut book = trusted_book(&[]);
        book.apply_fills(fills(1_100, &[(user(1), PUMP, "B", 1, "0.0", "2.0")]));
        let view = book.coin_positions(PUMP, None).unwrap();
        assert_eq!(view.positions[0].anchor_row, None);
        assert_eq!(view.positions[0].account_mode, None);
        assert_eq!(view.positions[0].changes, ChangeMarks { fill: Some(1_100), ..ChangeMarks::default() });
        book.install_anchor(anchor(1_200, &[(PUMP, 0, &[(user(1), 2)])]));
        let view = book.coin_positions(PUMP, None).unwrap();
        assert_eq!(view.positions[0].anchor_row, Some(row()));
        assert_eq!(view.positions[0].account_mode, Some(AccountMode::Unified));
        assert_eq!(view.positions[0].changes, ChangeMarks::default(), "the anchor covers the fill");
    }

    #[test]
    fn a_coin_leaving_the_universe_drops_its_records() {
        let mut book = trusted_book(&[(user(1), 5)]);
        let report = book.install_anchor(anchor(1_100, &[("BTC", 5, &[])]));
        assert_eq!(report.dropped_coin_records, 1);
        assert_eq!(book.coin_positions(PUMP, None), Err(QueryError::UnknownCoin));
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
        book.apply_block_actions(block_actions(600, &[]));
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
        assert_eq!(book.coin_positions(PUMP, None).unwrap().version, 0);
        let report = book.apply_fills(fills(1_100, &[(user(1), PUMP, "A", 1, "5.0", "5.0")]));
        let changes = push(&mut book, &report.touched).unwrap();
        assert_eq!((changes.version, changes.rows), (1, vec![(user(1), None)]), "the close is a removed row");
        assert_eq!(book.coin_positions(PUMP, None).unwrap().version, 1, "a subscriber's snapshot carries the version as is");
        let untouched: FxHashSet<Address> = std::iter::once(user(9)).collect();
        assert_eq!(book.push_changes(PUMP, &untouched, &[user(9)], None), Ok(None), "no holder touched: no frame, no bump");
        book.install_anchor(anchor(1_200, &[(PUMP, 0, &[(user(2), 3)])]));
        assert_eq!(book.push_snapshot(PUMP, None).map(|view| view.version), Ok(2));
        assert_eq!(book.push_snapshot("BTC", None).map(|view| view.version), Err(QueryError::UnknownCoin));
    }

    #[test]
    fn a_funding_batch_changes_the_rows_of_every_touched_holder() {
        let mut book = trusted_book(&[(user(1), 5), (user(2), -3), (user(3), 1)]);
        let funding = funding_inner(&[(user(1), PUMP, "0.1"), (user(2), PUMP, "-0.1")]);
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
        book.push_changes(PUMP, &accounts, &positions, None).unwrap()
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
        book.push_snapshot(PUMP, None).unwrap();
        book.install_anchor(anchor(1_100, &[(PUMP, 1, &[(user(1), 50)])]));
        assert_eq!(book.coins[PUMP].version, 1, "subscribers must never see a version go back");
    }

    // ---- change marks ----

    #[test]
    fn marks_keep_the_first_block_and_the_next_anchor_rebuilds_them_from_the_log() {
        let mut book = trusted_book(&[(user(1), 5)]);
        let ledger = send_inner((user(1), ""), (user(2), ""), "1.0");
        let funding = funding_inner(&[(user(1), PUMP, "0.1")]);
        book.apply_misc_events(misc(1_100, &[&ledger]));
        book.apply_misc_events(misc(1_250, &[&ledger]));
        book.apply_misc_events(misc(1_300, &[&ledger, &funding]));
        assert!(book.apply_misc_events(misc(900, &[&ledger])).stale);
        let marks = book.coin_positions(PUMP, None).unwrap().positions[0].changes;
        assert_eq!(marks, ChangeMarks { ledger: Some(1_100), funding: Some(1_300), ..ChangeMarks::default() }, "first after 1000");
        book.install_anchor(anchor(1_200, &[(PUMP, 0, &[(user(1), 5)])]));
        let marks = book.coin_positions(PUMP, None).unwrap().positions[0].changes;
        assert_eq!(marks, ChangeMarks { ledger: Some(1_250), funding: Some(1_300), ..ChangeMarks::default() }, "first after 1200");
        book.install_anchor(anchor(1_300, &[(PUMP, 0, &[(user(1), 5)])]));
        assert_eq!(book.coin_positions(PUMP, None).unwrap().positions[0].changes, ChangeMarks::default());
        assert!(!book.accounts.contains_key(&user(2)), "a covered account without a position is forgotten");
        assert_eq!(book.events.len(), 0, "the anchor covers every logged event");
    }

    #[test]
    fn a_late_replay_of_an_older_block_lowers_the_mark() {
        let mut book = trusted_book(&[(user(1), 5)]);
        book.apply_fills(fills(1_200, &[(user(1), "BTC", "B", 1, "0.0", "1.0")]));
        let replay = book.apply_fills(fills(1_150, &[(user(1), "BTC", "B", 2, "0.0", "1.0")]));
        assert_eq!(replay.touched.accounts, [user(1)]);
        assert_eq!(book.coin_positions(PUMP, None).unwrap().positions[0].changes.fill, Some(1_150));
    }

    #[test]
    fn an_overflowing_event_log_drops_the_oldest_as_a_loss_of_its_stream() {
        const LEDGER: AccountEvent = AccountEvent::Ledger(LedgerDelta::Withdraw);
        let mut book = trusted_book(&[]);
        let mut dropped = 0;
        for tid in 0..MAX_EVENT_LOG as u64 {
            book.record(user(1), LEDGER, 1_100 + tid, &mut dropped);
        }
        assert_eq!(dropped, 0);
        assert!(book.availability().is_ok());
        book.record(user(1), AccountEvent::UnreadableFill(String::new()), 5_000_000, &mut dropped);
        assert_eq!((dropped, book.events.len(), book.events_dropped), (1, MAX_EVENT_LOG, true));
        let lost = Unavailable::DataLoss { stream: Stream::MiscEvents, bound: LossBound::Block(1_100) };
        assert_eq!(book.availability(), Err(lost), "the dropped ledger event bounds a misc events loss");
        // review 000429 r2: a block action pushing out a ledger event is a loss of the ledger's stream, not its own
        book.record(user(1), AccountEvent::Action(BlockAction::SetAbstraction), 5_000_001, &mut dropped);
        assert_eq!((book.misc_events.lost_block, book.block_actions.lost_block), (Some(1_101), None));
        for tid in 0..MAX_EVENT_LOG as u64 {
            book.record(user(1), LEDGER, 6_000_000 + tid, &mut dropped);
        }
        assert_eq!(book.block_actions.lost_block, Some(5_000_001), "the dropped block action bounds its own stream");
    }

    /// Design 000433 r2 3.6: an install checks the replay of the interval it closes against its own state, then
    /// replays the logged events above it onto that state.
    #[test]
    fn an_anchor_checks_the_replayed_interval_and_rebuilds_from_its_own_state() {
        let holder = checkpoint_user(HOLDER);
        let funding = |block, amount| misc(block, &[&funding_inner(&[(holder, "BTC", amount)])]);
        let anchor_with = |height, main_cash_micro| {
            let mut anchor = anchor(height, &[(PUMP, 0, &[(holder, 5)])]);
            anchor.margins = per_dex_with_main_cash(main_cash_micro);
            anchor
        };
        let mut book = AccountBook::new(0);
        book.apply_fills(fills(100, &[]));
        book.apply_misc_events(misc(100, &[]));
        book.apply_block_actions(block_actions(1_000, &[]));
        assert_eq!(book.install_anchor(anchor_with(1_000, -12_000_000_000)).replay_check, ReplayCheck::Skipped("noPreviousAnchor"));
        book.apply_block_actions(block_actions(1_001, &[]));
        book.apply_misc_events(funding(1_001, "1.0"));
        book.apply_block_actions(block_actions(1_002, &[]));
        let early = book.install_anchor(anchor_with(1_003, -11_999_000_000)).replay_check;
        assert_eq!(early, ReplayCheck::Skipped("behindTheAnchor"), "block 1003 is not applied yet");
        book.apply_block_actions(block_actions(1_003, &[]));
        book.apply_block_actions(block_actions(1_004, &[]));
        book.apply_misc_events(funding(1_004, "2.0"));
        let ReplayCheck::Checked { cash: check, .. } = book.install_anchor(anchor_with(1_004, -11_997_000_000)).replay_check else {
            panic!("a complete interval is checked")
        };
        let compared = |exact, moved, mismatched| Compared { exact, moved, mismatched };
        let both_exact = (compared(2, 1, 0), 1);
        assert_eq!((check.compared["perDex"], check.stopped["portfolioMargin"]), both_exact, "HOLDER moved, ISOLATED did not");
        book.apply_block_actions(block_actions(1_005, &[]));
        book.apply_misc_events(funding(1_005, "4.0"));
        let rebuilt = &book.replay.as_ref().unwrap().current;
        let expected = per_dex_with_main_cash(-11_993_000_000);
        assert_eq!(rebuilt.check_against(&expected, &expected).compared["perDex"], compared(2, 0, 0));
        book.apply_block_actions(block_actions(1_006, &[]));
        let ReplayCheck::Checked { cash: check, .. } = book.install_anchor(anchor_with(1_006, -11_997_000_000)).replay_check else {
            panic!("checked")
        };
        assert_eq!((check.compared["perDex"], check.samples[0].user), (compared(1, 0, 1), holder), "the anchor contradicts +4 USDC");
    }

    // ---- replayed rows (design 000437), through the view every output uses ----

    /// A book whose anchor at `height` holds these PUMP positions, with the fixture's margins (HOLDER default with main
    /// dex cash -12000, ISOLATED's isolated PUMP cash 12000); every block up to `applied` is applied.
    fn replay_book(height: u64, applied: u64, holders: &[(Address, i64)]) -> AccountBook {
        let mut book = AccountBook::new(0);
        book.apply_fills(fills(100, &[]));
        book.apply_misc_events(misc(100, &[]));
        book.apply_block_actions(block_actions(applied, &[]));
        // the fixture's universe has BTC (szDecimals 5) on the main dex too
        let mut first = anchor(height, &[(PUMP, 0, holders), ("BTC", 5, &[])]);
        first.margins = per_dex_with_main_cash(-12_000_000_000);
        book.install_anchor(first);
        book
    }

    fn view_of(book: &AccountBook, holder: Address) -> PositionView {
        let view = book.coin_positions(PUMP, None).unwrap();
        view.positions.into_iter().find(|position| position.user == holder).unwrap()
    }

    fn fixture_table(table_id: u32) -> MarginTable {
        parse_checkpoint_bytes(&checkpoint_fixture()).unwrap().dexes[0].table(table_id).clone()
    }

    /// A replayed row of a pool of one, read without marks.
    fn replayed(height: u64, isolated: bool, liquidation_px: LiquidationPx) -> Replayed {
        let row = ReplayedRow { isolated, pool_positions: 1, liquidation_px };
        Replayed { height, row, mark: RowMark { mark_raw: None, distance_bps: None } }
    }

    #[test]
    fn a_cross_position_alone_in_its_pool_shows_its_replayed_row_and_a_second_one_falls_back() {
        let holder = checkpoint_user(HOLDER);
        let mut book = replay_book(1_000, 1_000, &[(holder, 1_000_000)]);
        let table = fixture_table(ANCHOR_TABLE);
        let alone = solve(-12_000.0, 0.0, 1_000_000.0, &table);
        assert_eq!(view_of(&book, holder).replayed, Some(replayed(1_000, false, alone.clone())));
        book.apply_block_actions(block_actions(1_001, &[]));
        book.apply_fills(fills(1_001, &[(holder, "BTC", "B", 1, "0.0", "0.001")]));
        let shared = view_of(&book, holder);
        assert_eq!((shared.replayed, shared.anchor_row.is_some()), (None, true), "BTC joined its pool: the anchor's row");
        book.apply_block_actions(block_actions(1_002, &[]));
        book.apply_fills(fills(1_002, &[(holder, "BTC", "A", 2, "0.001", "0.001")]));
        assert_eq!(view_of(&book, holder).replayed, Some(replayed(1_002, false, alone)), "alone again, cash back to -12000");
    }

    #[test]
    fn an_isolated_position_is_its_own_pool() {
        let isolated = checkpoint_user(ISOLATED);
        let book = replay_book(1_000, 1_000, &[(isolated, -2_000_000)]);
        let expected = solve(12_000.0, 0.0, -2_000_000.0, &fixture_table(ANCHOR_TABLE));
        assert_eq!(view_of(&book, isolated).replayed, Some(replayed(1_000, true, expected)));
    }

    #[test]
    fn a_stopped_account_shows_the_anchors_row_with_its_reason_and_marks() {
        let holder = checkpoint_user(HOLDER);
        let mut book = replay_book(1_000, 1_000, &[(holder, 1_000_000)]);
        book.apply_block_actions(block_actions(1_001, &[(BlockAction::SetAbstraction, holder)]));
        let view = view_of(&book, holder);
        assert_eq!((view.replayed, view.anchor_row.is_some()), (None, true));
        assert_eq!(view.replay_stop, Some(Stop { block: Some(1_001), reason: StopReason::AccountModeSwitch }));
        assert_eq!(view.changes.account_mode, Some(1_001), "the marks show again");
    }

    /// Review 000437 r1 P2: a position closed and reopened after the anchor takes the coin's current table, and a late
    /// anchor from before the close (holding the old position at table 20) does not take it back.
    #[test]
    fn a_reopened_position_keeps_the_current_table_through_a_late_anchor() {
        let holder = checkpoint_user(HOLDER);
        let mut book = replay_book(1_000, 1_000, &[(holder, 1_000_000)]);
        book.apply_block_actions(block_actions(1_001, &[]));
        book.apply_block_actions(block_actions(1_002, &[]));
        book.apply_fills(fills(1_002, &[(holder, PUMP, "A", 1, "1000000.0", "1000000.0")]));
        book.apply_block_actions(block_actions(1_003, &[]));
        book.apply_fills(fills(1_003, &[(holder, PUMP, "B", 2, "0.0", "2000000.0")]));
        let mut late = anchor(1_001, &[(PUMP, 0, &[(holder, 1_000_000)]), ("BTC", 5, &[])]);
        late.margins = per_dex_with_main_cash(-12_000_000_000);
        book.install_anchor(late);
        // -12000 + 1e6 (sold at 1.0) - 2e6 (bought at 1.0)
        let current = solve(-1_012_000.0, 0.0, 2_000_000.0, &fixture_table(CURRENT_TABLE));
        assert_ne!(current, solve(-1_012_000.0, 0.0, 2_000_000.0, &fixture_table(ANCHOR_TABLE)), "the tables tell apart");
        assert_eq!(view_of(&book, holder).replayed, Some(replayed(1_003, false, current)));
    }

    /// Review 000437 r2 / research 52af3097: a fill flipping the position keeps its table.
    #[test]
    fn a_flip_keeps_the_positions_table() {
        let holder = checkpoint_user(HOLDER);
        let mut book = replay_book(1_000, 1_000, &[(holder, 1_000_000)]);
        book.apply_block_actions(block_actions(1_001, &[]));
        book.apply_fills(fills(1_001, &[(holder, PUMP, "A", 1, "1000000.0", "3000000.0")]));
        // -12000 + 3e6 (sold at 1.0), short 2e6 at the anchor's table 20
        let kept = solve(2_988_000.0, 0.0, -2_000_000.0, &fixture_table(ANCHOR_TABLE));
        assert_ne!(kept, solve(2_988_000.0, 0.0, -2_000_000.0, &fixture_table(CURRENT_TABLE)));
        assert_eq!(view_of(&book, holder).replayed, Some(replayed(1_001, false, kept)));
    }

    /// Review 000437 r1 P2: an anchor installed ahead of the applied blocks (the first, or a recovery) shows its own
    /// rows until every block up to it is applied.
    #[test]
    fn no_replayed_row_until_the_applied_blocks_reach_the_anchor() {
        let holder = checkpoint_user(HOLDER);
        let mut book = replay_book(1_000, 998, &[(holder, 1_000_000)]);
        book.apply_block_actions(block_actions(999, &[]));
        let view = view_of(&book, holder);
        assert_eq!((view.replayed, view.anchor_row.is_some()), (None, true), "the cash is of 1000, the blocks of 999");
        book.apply_block_actions(block_actions(1_000, &[]));
        assert!(view_of(&book, holder).replayed.is_some());
    }

    /// Design 000437 r2 revision 3: at an install, the replayed row of each position the anchor holds (replayed cash at
    /// the anchor, the anchor's size and members, the record's table) is compared with the anchor's own row.
    #[test]
    fn an_install_compares_the_replayed_rows_with_the_anchors() {
        let holder = checkpoint_user(HOLDER);
        let mut book = replay_book(1_000, 1_000, &[(holder, 1_000_000)]);
        book.apply_block_actions(block_actions(1_001, &[]));
        let anchor_at = |height, liquidation_px: LiquidationPx| {
            let mut anchor = anchor(height, &[(PUMP, 0, &[(holder, 1_000_000)]), ("BTC", 5, &[])]);
            anchor.margins = per_dex_with_main_cash(-12_000_000_000);
            anchor.coins.get_mut(PUMP).unwrap().positions.get_mut(&holder).unwrap().row.liquidation_px = liquidation_px;
            anchor
        };
        let expected = solve(-12_000.0, 0.0, 1_000_000.0, &fixture_table(ANCHOR_TABLE));
        let ReplayCheck::Checked { rows, .. } = book.install_anchor(anchor_at(1_001, expected.clone())).replay_check else {
            panic!("checked")
        };
        assert_eq!((rows.equal, rows.differ, rows.class_differs, rows.moved_after), (1, 0, 0, 0));
        book.apply_block_actions(block_actions(1_002, &[]));
        let ReplayCheck::Checked { rows, .. } = book.install_anchor(anchor_at(1_002, LiquidationPx::Computed(0.004))).replay_check else {
            panic!("checked")
        };
        assert_eq!((rows.equal, rows.differ, rows.samples.len()), (0, 1, 1), "the anchor's row says otherwise");
        assert_eq!(rows.samples[0].replayed.as_ref().map(|row| row.liquidation_px.clone()), Some(expected));
    }

    fn view_at(book: &AccountBook, holder: Address, marks: &ListedMarks) -> PositionView {
        let view = book.coin_positions(PUMP, Some(marks)).unwrap();
        view.positions.into_iter().find(|position| position.user == holder).unwrap()
    }

    /// Design 000442: a cross position sharing its pool replays with the other members at the marks of the applied
    /// height; marks of another height, or a member without a mark, leave the anchor's row.
    #[test]
    fn a_shared_pool_replays_at_the_marks_of_its_height() {
        let holder = checkpoint_user(HOLDER);
        let mut book = replay_book(1_000, 1_000, &[(holder, 1_000_000)]);
        book.apply_block_actions(block_actions(1_001, &[]));
        book.apply_fills(fills(1_001, &[(holder, "BTC", "B", 1, "0.0", "0.001")]));
        let both = ListedMarks { height: 1_001, marks: &[(PUMP, 4_000), ("BTC", 828_576)] };
        let shared = view_at(&book, holder, &both).replayed.expect("both members have a mark");
        // -12000 less 0.001 BTC bought at 1.0; BTC opened from zero takes the coin's current table
        let btc_table = fixture_table(CURRENT_TABLE);
        let btc = HeldAtMark { size_raw: 100, size_decimals: 5, mark_raw: 828_576, table: &btc_table };
        let expected = solve_held(-12_000.001, &[btc], 1_000_000.0, &fixture_table(ANCHOR_TABLE));
        assert_eq!(shared.row, ReplayedRow { isolated: false, pool_positions: 2, liquidation_px: expected.clone() });
        assert_eq!(shared.mark, RowMark::of(1_000_000, 0, Some(4_000), &expected));
        assert!(shared.mark.distance_bps.is_some());
        let earlier = ListedMarks { height: 1_000, marks: both.marks };
        assert_eq!(view_at(&book, holder, &earlier).replayed, None, "marks of another height");
        let view = view_at(&book, holder, &ListedMarks { height: 1_001, marks: &[(PUMP, 4_000)] });
        assert_eq!((view.replayed, view.anchor_row.is_some()), (None, true), "BTC has no mark: the anchor's row");
    }

    /// Design 000442 r2 revision 3: at an install a pool of two checks with the anchor's sizes, tables and stored marks;
    /// a member whose record moved after the anchor skips the row.
    #[test]
    fn an_install_compares_shared_pool_rows_at_the_anchors_marks() {
        let holder = checkpoint_user(HOLDER);
        let anchor_at = |height| {
            let mut anchor = anchor(height, &[(PUMP, 0, &[(holder, 1_000_000)]), ("BTC", 5, &[(holder, 10_000)])]);
            anchor.margins = per_dex_with_main_cash(-12_000_000_000);
            anchor
        };
        let mut book = AccountBook::new(0);
        book.apply_fills(fills(100, &[]));
        book.apply_misc_events(misc(100, &[]));
        book.apply_block_actions(block_actions(1_000, &[]));
        book.install_anchor(anchor_at(1_000));
        let table = fixture_table(ANCHOR_TABLE);
        let pump = HeldAtMark { size_raw: 1_000_000, size_decimals: 0, mark_raw: 4_477, table: &table };
        let btc = HeldAtMark { size_raw: 10_000, size_decimals: 5, mark_raw: 828_576, table: &table };
        let with_rows = |mut anchor: Anchor| {
            for (name, mark_raw, liquidation_px) in [
                (PUMP, 4_477, solve_held(-12_000.0, &[btc], 1_000_000.0, &table)),
                ("BTC", 828_576, solve_held(-12_000.0, &[pump], 0.1, &table)),
            ] {
                let coin = anchor.coins.get_mut(name).unwrap();
                coin.mark_raw = Some(mark_raw);
                let row = &mut coin.positions.get_mut(&holder).unwrap().row;
                (row.pool_positions, row.liquidation_px) = (Some(2), liquidation_px);
            }
            anchor
        };
        book.apply_block_actions(block_actions(1_001, &[]));
        let ReplayCheck::Checked { rows, .. } = book.install_anchor(with_rows(anchor_at(1_001))).replay_check else {
            panic!("checked")
        };
        assert_eq!((rows.equal, rows.differ, rows.class_differs, rows.moved_after), (2, 0, 0, 0), "{:?}", rows.samples);
        book.apply_block_actions(block_actions(1_002, &[]));
        book.apply_block_actions(block_actions(1_003, &[]));
        book.apply_fills(fills(1_003, &[(holder, "BTC", "A", 3, "0.1", "0.001")]));
        let ReplayCheck::Checked { rows, .. } = book.install_anchor(with_rows(anchor_at(1_002))).replay_check else {
            panic!("checked")
        };
        assert_eq!((rows.equal, rows.class_differs, rows.moved_after), (0, 0, 2), "BTC moved after 1002: neither row checks");
    }

    /// Design 000437 4: every event of a replaying account touches its rows, not only its first mark.
    #[test]
    fn a_replaying_accounts_cash_change_touches_its_rows() {
        let holder = checkpoint_user(HOLDER);
        let mut book = replay_book(1_000, 1_000, &[(holder, 1_000_000)]);
        let funding = |block, amount| misc(block, &[&funding_inner(&[(holder, PUMP, amount)])]);
        assert_eq!(book.apply_misc_events(funding(1_001, "1.0")).touched.accounts, [holder]);
        assert_eq!(book.apply_misc_events(funding(1_002, "2.0")).touched.accounts, [holder], "no new mark, new cash");
    }

    /// Review 000435 r1 P2: an isolated position closed in the block that switches its asset back to cross; the block's
    /// actions apply before its fills, so the close must not count as a cross fill. A switch a block earlier is plain.
    #[test]
    fn a_fill_in_the_block_that_switched_its_asset_stops_the_replay() {
        let isolated = checkpoint_user(ISOLATED);
        let apply = |book: &mut AccountBook, height, actions: &[(BlockAction, Address)], fills_specs: &[FillSpec<'_>]| {
            book.apply_block(block_actions(height, actions), vec![fills(height, fills_specs)], Vec::new())
        };
        let mut book = AccountBook::new(0);
        book.apply_fills(fills(100, &[]));
        book.apply_misc_events(misc(100, &[]));
        book.apply_block_actions(block_actions(1_000, &[]));
        let mut first = anchor(1_000, &[(PUMP, 0, &[(isolated, -2_000_000)])]);
        first.margins = per_dex_with_main_cash(-12_000_000_000);
        book.install_anchor(first);
        let to_cross = |asset| (BlockAction::Leverage { asset, is_cross: true }, isolated);
        // BTC (asset 0, set isolated without a position) back to cross, a BTC fill one block later: cross
        apply(&mut book, 1_001, &[to_cross(0)], &[]);
        apply(&mut book, 1_002, &[], &[(isolated, "BTC", "B", 1, "0.0", "0.001")]);
        assert_eq!(book.replay.as_ref().unwrap().current.stop_reason(&isolated), None);
        // the isolated PUMP short closed and PUMP switched to cross in one block
        apply(&mut book, 1_003, &[to_cross(1)], &[(isolated, PUMP, "B", 2, "-2000000.0", "2000000.0")]);
        assert_eq!(book.replay.as_ref().unwrap().current.stop_reason(&isolated), Some(&StopReason::IsolatedFill));
    }

    /// Review 000435 r1 P2: an anchor parsed before its blocks are applied waits (the owner holds it while
    /// `anchor_ready` is false), so the interval it closes is still checked once they are.
    #[test]
    fn an_anchor_ahead_of_its_blocks_waits_and_its_interval_is_checked() {
        let holder = checkpoint_user(HOLDER);
        let anchor_with = |height, main_cash_micro| {
            let mut anchor = anchor(height, &[(PUMP, 0, &[(holder, 5)])]);
            anchor.margins = per_dex_with_main_cash(main_cash_micro);
            anchor
        };
        let mut book = AccountBook::new(0);
        book.apply_fills(fills(100, &[]));
        book.apply_misc_events(misc(100, &[]));
        book.apply_block_actions(block_actions(1_000, &[]));
        assert!(book.anchor_ready(1_000), "the first anchor has no interval to check");
        book.install_anchor(anchor_with(1_000, -12_000_000_000));
        book.apply_block_actions(block_actions(1_001, &[]));
        book.apply_misc_events(misc(1_001, &[&funding_inner(&[(holder, "BTC", "1.0")])]));
        assert!(!book.anchor_ready(1_002), "block 1002 is not applied yet");
        book.apply_block_actions(block_actions(1_002, &[]));
        assert!(book.anchor_ready(1_002));
        // the anchor says -11998: the replayed -11999 differs by 1 USDC, and the check reports it
        let ReplayCheck::Checked { cash: check, .. } = book.install_anchor(anchor_with(1_002, -11_998_000_000)).replay_check else {
            panic!("checked once its blocks are applied")
        };
        assert_eq!((check.compared["perDex"].mismatched, check.samples[0].user), (1, holder));
    }

    /// Design 000429 r2: the first block of each block-action kind after the anchor, rebuilt by the next anchor.
    #[test]
    fn block_actions_mark_the_first_block_of_each_kind() {
        const MARGIN: BlockAction = BlockAction::Leverage { asset: 0, is_cross: true };
        const MODE: BlockAction = BlockAction::SetAbstraction;
        const WITHDRAWAL: BlockAction = BlockAction::Withdraw { amount_wei: 100_000_000 };
        let mut book = trusted_book(&[(user(1), 5)]);
        let first = book.apply_block_actions(block_actions(1_001, &[(MARGIN, user(1)), (MARGIN, user(2))]));
        assert_eq!(first.touched.accounts, [user(1), user(2)]);
        assert_eq!(push(&mut book, &first.touched).unwrap().rows.len(), 1, "only the PUMP holder has a row");
        book.apply_block_actions(block_actions(1_002, &[(MODE, user(1))]));
        let later = book.apply_block_actions(block_actions(1_003, &[(MARGIN, user(1)), (WITHDRAWAL, user(1))]));
        assert_eq!(later.touched.accounts, [user(1)], "only the withdrawal is a new mark");
        let marks = book.coin_positions(PUMP, None).unwrap().positions[0].changes;
        let expected =
            ChangeMarks { margin: Some(1_001), account_mode: Some(1_002), withdrawal: Some(1_003), ..ChangeMarks::default() };
        assert_eq!(marks, expected);
        book.install_anchor(anchor(1_002, &[(PUMP, 0, &[(user(1), 5)])]));
        let marks = book.coin_positions(PUMP, None).unwrap().positions[0].changes;
        assert_eq!(marks, ChangeMarks { margin: Some(1_003), withdrawal: Some(1_003), ..ChangeMarks::default() });
        assert_eq!(book.coin_positions(PUMP, None).unwrap().block_actions_height, 1_003);
        assert_eq!(book.apply_block_actions(block_actions(1_004, &[])).touched, Touched::default());
    }

    /// Every block has a `replica_cmds` line: a skipped height is a loss up to the last missing block; a replay is not.
    #[test]
    fn a_gap_in_the_block_heights_is_a_loss_up_to_the_last_missing_block() {
        let mut book = trusted_book(&[(user(1), 5)]);
        assert_eq!(book.apply_block_actions(block_actions(1_001, &[])).missing, None);
        let report = book.apply_block_actions(block_actions(1_005, &[(BlockAction::Leverage { asset: 0, is_cross: true }, user(1))]));
        assert_eq!(report.missing, Some((1_002, 1_004)));
        let lost = Unavailable::DataLoss { stream: Stream::BlockActions, bound: LossBound::Block(1_004) };
        assert_eq!(book.availability(), Err(lost.clone()));
        let replay = book.apply_block_actions(block_actions(1_003, &[]));
        assert_eq!((replay.missing, book.status().block_actions_height), (None, Some(1_005)), "a replay raises nothing");
        book.install_anchor(anchor(1_003, &[(PUMP, 0, &[(user(1), 5)])]));
        assert_eq!(book.availability(), Err(lost));
        book.install_anchor(anchor(1_004, &[(PUMP, 0, &[(user(1), 5)])]));
        assert_eq!(book.coin_positions(PUMP, None).unwrap().positions[0].changes.margin, Some(1_005));
    }

    /// The first block-actions line after the start or a discard has no previous height to compare: the unknown loss
    /// already bounds it.
    #[test]
    fn a_block_action_gap_needs_a_previous_height() {
        let mut book = AccountBook::new(0);
        assert_eq!(book.apply_block_actions(block_actions(5_000, &[])).missing, None);
        book.record_unknown_loss(Stream::BlockActions, 1_000);
        assert_eq!(book.apply_block_actions(block_actions(5_001, &[])).missing, None);
    }

    #[test]
    fn a_spot_fill_marks_the_account() {
        let mut book = trusted_book(&[(user(1), 5)]);
        book.apply_fills(fills(1_100, &[(user(1), "@107", "B", 1, "0.0", "1.0")]));
        assert_eq!(book.coin_positions(PUMP, None).unwrap().positions[0].changes.fill, Some(1_100));
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
        book.apply_block_actions(block_actions(2_000, &[]));
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
        book.apply_block_actions(block_actions(1_200, &[]));
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
        assert!(matches!(book.coin_positions(PUMP, None), Err(QueryError::Unavailable(Unavailable::Stopped(_)))));
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
        let view = book.coin_positions(PUMP, None).unwrap();
        assert_eq!((view.fills_height, view.misc_events_height, view.size_decimals), (1_100, 100, 0));
        assert_eq!(shown(&book, PUMP), [(user(1), -4, 1_000), (user(2), -2, 1_100)]);
        assert_eq!(book.status().open_positions, 2);
    }
}
