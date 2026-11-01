//! The pending set and its overlays (hyperliquid-trade design 000604 section 3; r6 revisions 15-16, r7 revisions
//! 18-20, batch 2b): the mempool's lines held, their book actions by coin in execution order, until a block the replica
//! confirmed holds the line's hash - a line is one bundle, its `tx_hash` the bundle's - or they expire; each coin's
//! overlay the replay of its known actions over the replica; and whether the result may be served.
//!
//! - Arrival (r5 revision 13, r6 revision 15): a line held already is dropped; one expired by the confirmed block time
//!   is dropped; one whose hash a block at or below the replica's confirmed height holds - late into the mempool - is
//!   skipped whole; one that would take the set past its cap drops the set, itself with it; the rest held, applied to
//!   their coins' overlays at once unless unknown, the fills the predicted trades: out that once, while the book may be
//!   served (revision 2; a replay sends none). A line whose block the log showed above the confirmed height is held
//!   until the replica gets there.
//! - Settling (r7 revision 18), the one step after the replica's confirmed height moves, a new generation aligns or a
//!   block's hashes come: the lines a block at or below the confirmed height holds, and those expired (E after their
//!   line, by block time), leave whole; their coins and those whose book changed are replayed (on a new generation,
//!   every coin held). Only then is anything published.
//! - Gaps (r6 revision 16, r7 revision 19), each input its own:
//!   - the mempool's (a loss - taken as soon as found, before its report -, a line taken after a loss not yet reported,
//!     no asset table, silence past S before the confirmed block time, the set past its cap): lines missing, the set
//!     otherwise sound - nothing served from the gap until the confirmed block time is past `T_end` + E, `T_end` the
//!     first line no more than S older than the confirmed block time (the newest for the cap);
//!   - the replica's (none aligned): nothing served; a new generation settles by hash at once, the log's record having
//!     no hole;
//!   - the block log's (its start, a loss, a hole in its heights): a line its blocks held can neither leave nor be told
//!     apart when late. What arrives while it is open, and what is held when it opens, is unknown - never overlaid -;
//!     it ends at the next block's time `T_end`, W = max(W, `T_end` + H, the newest line then), served again from W + E.
//!   - Served also only with the log at or past the confirmed height and the mempool's newest line no more than the
//!     span limit ahead of the confirmed block time.

use std::collections::HashSet;

use alloy::primitives::{Address, B256};
use rustc_hash::{FxHashMap, FxHashSet};

use super::{
    actions::{ActionId, BookActions, PendingAction, Placement, book_actions},
    blocks::LoggedBlock,
    mempool::MempoolTx,
    overlay::{Basis, BestLevel, CoinBase, Fill, Maker, Overlay, PRIORITY_WINDOW_MS},
    publish::{BestLevels, CoinPending},
    record::{BlockRecord, MAX_LEAD_BLOCKS},
    refusals::{TakerKey, TakerRefusals},
    twap_streams::{TwapEvent, TwapFill},
    twaps::{DueSlices, TwapBook, TwapSlice, TwapStream, TwapsUnknown},
};
use crate::{
    accounts::book::ActionCoins,
    listeners::order_book::replica::BookReplica,
    metrics::{
        PENDING_BOOK_ACTIONS_TOTAL, PENDING_BOOK_BLOCK_LOG_LEAD_BLOCKS, PENDING_BOOK_GAPS_TOTAL, PENDING_BOOK_HELD_ACTIONS,
        PENDING_BOOK_MOVED_COINS, PENDING_BOOK_OVERLAY_TOTAL, PENDING_BOOK_PREDICTED_FILLS_TOTAL, PENDING_BOOK_SPAN_SECONDS,
    },
    order_book::{Coin, Px, Side, Sz},
};

const NS_PER_MS: i64 = 1_000_000;

/// The pending set's times and limits (design 000604 r1, revisions 7, 13; r6 revision 17).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PendingParams {
    /// E: a line no block confirmed this long after it is dropped, by the confirmed block time.
    pub(crate) expiry_ns: i64,
    /// H: how late after its block a line may reach the mempool and still be told apart (the record keeps R = H + E);
    /// the warm-up after a block log gap.
    pub(crate) max_late_ns: i64,
    /// The mempool's newest line this far ahead of the confirmed block time at most, for the book to be served.
    pub(crate) max_span_ns: i64,
    /// Actions held at most: past it the set is dropped.
    pub(crate) max_actions: usize,
    /// S: no line for this long before the confirmed block time is a gap.
    pub(crate) silence_ns: i64,
    /// M: a taker's margin refusal stands for its orders whose lines come this long after its block, ms (batch 2g).
    pub(crate) margin_refusal_ms: u64,
    /// W: a TWAP suborder's trades go out once the confirmed block time is this close to the next second, ms (batch 2i).
    pub(crate) twap_trade_window_ms: u64,
}

impl PendingParams {
    /// From milliseconds, each positive.
    pub(crate) fn from_ms(
        expiry_ms: u64,
        max_late_ms: u64,
        max_span_ms: u64,
        max_actions: usize,
        silence_ms: u64,
        margin_refusal_ms: u64,
        twap_trade_window_ms: u64,
    ) -> Result<Self, String> {
        let ns = |name: &str, ms: u64| -> Result<i64, String> {
            if ms == 0 {
                return Err(format!("--pending-{name}-ms must be positive"));
            }
            i64::try_from(ms).ok().and_then(|ms| ms.checked_mul(NS_PER_MS)).ok_or_else(|| format!("--pending-{name}-ms {ms} too large"))
        };
        if max_actions == 0 {
            return Err("--pending-max-actions must be positive".to_string());
        }
        if margin_refusal_ms == 0 {
            return Err("--pending-margin-refusal-ms must be positive".to_string());
        }
        if twap_trade_window_ms == 0 {
            return Err("--pending-twap-trade-window-ms must be positive".to_string());
        }
        // a confirmed order's ALO priority must outlive the window it is compared in, the record's retention R = H + E
        // and the warm-up after a block log gap H both (review 000620 r1 P2-2)
        if max_late_ms < PRIORITY_WINDOW_MS {
            return Err(format!("--pending-max-late-ms {max_late_ms} under the ALO priority window, {PRIORITY_WINDOW_MS} ms"));
        }
        Ok(Self {
            expiry_ns: ns("expiry", expiry_ms)?,
            max_late_ns: ns("max-late", max_late_ms)?,
            max_span_ns: ns("max-span", max_span_ms)?,
            max_actions,
            silence_ns: ns("mempool-silence", silence_ms)?,
            margin_refusal_ms,
            twap_trade_window_ms,
        })
    }

    /// R = H + E: how long the record keeps a block behind the confirmed block time (r5 revision 13).
    const fn retention_ns(&self) -> i64 {
        self.max_late_ns + self.expiry_ns
    }

    /// Whether a line seen at `seen_ns` expired by the confirmed block time (at E exactly, r5 revision 13).
    const fn expired(&self, seen_ns: i64, confirmed_ns: i64) -> bool {
        seen_ns + self.expiry_ns <= confirmed_ns
    }
}

/// Why the pending book may not be served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unavailable {
    /// No replica aligned.
    Replica,
    /// A mempool gap open, by cause.
    Mempool(&'static str),
    /// A block log gap open, by cause.
    BlockLog(&'static str),
    /// The block log behind the replica's confirmed height.
    BlockLogBehind,
    /// A gap's recovery: the confirmed block time not yet past its bound.
    WarmingUp,
    /// The mempool past the span limit ahead of the confirmed blocks.
    Span,
    /// The pending thread ended: for good.
    Stopped,
}

impl Unavailable {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Replica => "replica",
            Self::Mempool(cause) | Self::BlockLog(cause) => cause,
            Self::BlockLogBehind => "block_log_behind",
            Self::WarmingUp => "warming_up",
            Self::Span => "span",
            Self::Stopped => "stopped",
        }
    }
}

/// A fill predicted for a pending taker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingTrade {
    pub(crate) coin: Coin,
    /// The taker's side, as `trades`' `side`.
    pub(crate) side: Side,
    pub(crate) px: Px,
    pub(crate) sz: Sz,
    pub(crate) maker: Maker,
    pub(crate) taker: Taker,
}

/// Who a predicted fill's taker is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Taker {
    /// A mempool order: its action, its line's `local_time` (unix ns), its cloid, the vault it acts for.
    Pending { id: ActionId, seen_ns: i64, cloid: Option<String>, vault: Option<Address> },
    /// A TWAP's suborder of `slot` (batch 2h), `user` the TWAP's.
    Twap { twap_id: u64, user: Address, slot: u64 },
}

/// What a step changed: the trades predicted, and the coins whose merged book may have moved. A change of every coin -
/// the overlays dropped whole, a new generation - is the set's `take_refresh_all`.
#[derive(Debug, Default)]
pub(crate) struct Step {
    pub(crate) trades: Vec<PendingTrade>,
    pub(crate) changed: HashSet<Coin>,
}

impl Step {
    /// `later`'s changes taken in after this step's: its trades after these, its coins with these.
    pub(crate) fn absorb(&mut self, later: Self) {
        self.trades.extend(later.trades);
        self.changed.extend(later.changed);
    }
}

/// One coin as the pending book serves it: the merged book's best levels, the replica's, and what the set holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CoinView {
    pub(crate) bbo: BestLevels,
    pub(crate) basis_bbo: BestLevels,
    pub(crate) pending: CoinPending,
}

/// A coin's held actions in execution order, and their overlay.
#[derive(Default)]
struct CoinActions {
    actions: Vec<PendingAction>,
    overlay: Overlay,
}

impl CoinActions {
    /// The overlay anew: the TWAP suborders the next first-of-second block runs first - at its start, before any user
    /// action (batch 2h) -, then the known actions. Each suborder's fills.
    fn replay(&mut self, base: CoinBase<'_>, basis: Basis<'_>, slices: &[TwapSlice]) -> Vec<Vec<Fill>> {
        self.overlay = Overlay::default();
        let fills = slices.iter().map(|slice| self.overlay.take_slice(base, slice)).collect();
        for action in self.actions.iter().filter(|action| !action.unknown) {
            self.overlay.apply(base, basis, action);
        }
        fills
    }

    /// Whether it holds a taker order of `keys` (batch 2g: their margin refusals changed).
    fn holds_taker_of(&self, keys: &FxHashSet<TakerKey>) -> bool {
        self.actions.iter().any(|action| action.taker_key().is_some_and(|key| keys.contains(&key)))
    }

    /// Whether it holds an order with an ALO priority: its place in its queue moves with the confirmed block time, every
    /// pending order being taken to be placed no earlier (review 000620 r2, r4 - an older order of none, compared at the
    /// new time, may fall within its window). An order without one never moves; it is compared at the confirmed block of
    /// each comparison (`Basis::placed_ms`).
    fn holds_priority(&self) -> bool {
        self.actions.iter().any(|action| action.op.placement().is_some_and(|order| order.priority > 0))
    }
}

/// A mempool line held: its time and the coins its actions are on.
struct HeldLine {
    seen_ns: i64,
    coins: Vec<Coin>,
}

/// The inputs' gaps.
struct Gaps {
    /// The mempool's open gap, by cause.
    mempool: Option<&'static str>,
    /// The block log's open gap, by cause.
    block_log: Option<&'static str>,
    /// No replica aligned: the overlays wait for one.
    replica: bool,
    /// W of the block log's gaps: the actions seen up to it are unknown.
    warm_until_ns: i64,
    /// The confirmed block time the book may be served from: past every gap's recovery bound.
    ready_from_ns: i64,
}

impl Gaps {
    const fn unknown_at(&self, seen_ns: i64) -> bool {
        self.block_log.is_some() || seen_ns <= self.warm_until_ns
    }

    fn ready_from(&mut self, bound_ns: i64) {
        self.ready_from_ns = self.ready_from_ns.max(bound_ns);
    }
}

/// The replica aligned: (it, its confirmed height, its confirmed block time in unix ns).
type Aligned<'a> = (&'a BookReplica, u64, i64);

/// The pending set, its overlays and the block record, and the inputs' gaps.
pub(crate) struct Shadow {
    params: PendingParams,
    /// Whether the book keeps spot (`--markets` `spot` or `all`): otherwise, as the replica's book, the set leaves it out - whether
    /// a replica is there or not (review 000611 r3 P2-1).
    spot: bool,
    coins: FxHashMap<Coin, CoinActions>,
    lines: FxHashMap<B256, HeldLine>,
    record: BlockRecord,
    /// The takers' margin refusals by the block log's trusted blocks (batch 2g).
    refusals: TakerRefusals,
    /// The running TWAPs (batch 2h); the suborders overlaid, as of the book's version and the confirmed block time's
    /// second they were known at - kept through that second while the next height is not proven, `None` while none are
    /// (batch 2i) -; the (twap, slot) whose trades went out, with the slot's due time: kept until the slot is past,
    /// unknown spells between; the confirmed height the trade window last replayed its coins at (review 000631 r1
    /// P2-2).
    twaps: TwapBook,
    due: DueSlices,
    due_of: Option<(u64, u64)>,
    emitted: FxHashMap<(u64, u64), u64>,
    window_height: Option<u64>,
    gaps: Gaps,
    held: usize,
    newest_seen_ns: Option<i64>,
    /// The overlays were dropped or rebuilt whole since the publisher last framed every coin (review 000614 r1 P1-1).
    refresh_all: bool,
    /// The confirmed block time the overlays were last replayed against, unix ms (batch 2f).
    basis_ms: u64,
    /// Overlays were applied while the block record was behind the confirmed height, their confirmed orders' ALO
    /// priorities possibly missing: replayed once it catches up, before the book is served (review 000620 r1 P2-2).
    priorities_behind: bool,
    /// Settling steps taken with a replica aligned.
    #[cfg(test)]
    settles: usize,
}

impl Shadow {
    /// Every input in a gap until each has begun; lines of the TWAP streams written before `started_ms` (unix ms) not
    /// read.
    pub(crate) fn new(params: PendingParams, spot: bool, started_ms: u64) -> Self {
        Self {
            params,
            spot,
            coins: FxHashMap::default(),
            lines: FxHashMap::default(),
            record: BlockRecord::default(),
            refusals: TakerRefusals::new(params.margin_refusal_ms),
            twaps: TwapBook::new(started_ms),
            due: DueSlices::default(),
            due_of: None,
            emitted: FxHashMap::default(),
            window_height: None,
            gaps: Gaps {
                mempool: Some("start"),
                block_log: Some("block_log_start"),
                replica: true,
                warm_until_ns: i64::MIN,
                ready_from_ns: i64::MIN,
            },
            held: 0,
            newest_seen_ns: None,
            refresh_all: false,
            basis_ms: 0,
            priorities_behind: false,
            #[cfg(test)]
            settles: 0,
        }
    }

    /// A mempool line; `after_loss` when the watcher had found a loss not yet reported, so it may follow it. `replica`
    /// is the current one, aligned or not.
    pub(crate) fn on_tx(&mut self, tx: &MempoolTx, after_loss: bool, coins: Option<&ActionCoins>, replica: Option<&BookReplica>) -> Step {
        let mut step = Step::default();
        if self.lines.contains_key(&tx.tx_hash) {
            PENDING_BOOK_ACTIONS_TOTAL.with_label_values(&["line", "held_already"]).inc();
            return step;
        }
        let aligned = aligned(replica);
        self.mempool_gaps(tx.seen_ns, after_loss, coins.is_some(), aligned);
        self.newest_seen_ns = Some(self.newest_seen_ns.map_or(tx.seen_ns, |newest| newest.max(tx.seen_ns)));
        let Some(coins) = coins else { return step };
        let BookActions { held: actions, refused } = book_actions(tx, coins, self.spot);
        for labels in refused {
            PENDING_BOOK_ACTIONS_TOTAL.with_label_values(&labels).inc();
        }
        if let Some(outcome) = self.not_held(tx, aligned) {
            for (_, action) in &actions {
                count_action(action, outcome);
            }
            return step;
        }
        if actions.is_empty() {
            return step;
        }
        // the cap judged before any of it applies: nothing of a line dropping the set trades (review 000611 r1)
        if self.held + actions.len() > self.params.max_actions {
            self.drop_over_cap(actions.len());
            return step;
        }
        let emit = self.availability(replica).is_ok();
        let unknown = self.gaps.unknown_at(tx.seen_ns);
        let mut line = HeldLine { seen_ns: tx.seen_ns, coins: Vec::new() };
        for (coin, mut action) in actions {
            action.unknown = unknown;
            if !line.coins.contains(&coin) {
                line.coins.push(coin.clone());
            }
            self.hold(coin, action, aligned, emit, &mut step);
        }
        self.lines.insert(tx.tx_hash, line);
        step
    }

    /// A block of the log; `after_loss` when the watcher had found a loss not yet reported. Its taker outcomes are kept
    /// for its height's confirmation when no block log gap is open after it - one it ends included (design 000621 r3).
    pub(crate) fn on_block(&mut self, mut block: LoggedBlock, after_loss: bool, replica: Option<&BookReplica>) -> Step {
        if !self.record.follows(&block) {
            self.open_block_log_gap("block_log_hole");
        }
        if after_loss {
            self.open_block_log_gap("block_log_after_loss");
        } else {
            self.end_block_log_gap(unix_ns(block.time_ms));
        }
        if self.gaps.block_log.is_none() {
            self.refusals.stage(block.height, block.time_ms, std::mem::take(&mut block.taker_outcomes));
        }
        self.twaps.on_block(block.height, block.read_at, block.time_ms, &block.twap_cancels);
        self.record.add(block);
        self.settle(replica, HashSet::new())
    }

    /// The replica applied batches, changing `changed`'s books.
    pub(crate) fn on_applied(&mut self, changed: HashSet<Coin>, replica: &BookReplica) -> Step {
        self.settle(Some(replica), changed)
    }

    /// A TWAP statuses line (batch 2h).
    pub(crate) fn on_twap_statuses(&mut self, height: u64, events: Vec<TwapEvent>, replica: Option<&BookReplica>) -> Step {
        self.twaps.on_statuses(height, events);
        self.twap_step(replica)
    }

    /// A fills line's TWAP fills.
    pub(crate) fn on_twap_fills(&mut self, height: u64, fills: Vec<TwapFill>, replica: Option<&BookReplica>) -> Step {
        self.twaps.on_fills(height, fills);
        self.twap_step(replica)
    }

    /// A TWAP stream proved a read-through at `at`.
    pub(crate) fn on_twap_read_through(&mut self, stream: TwapStream, at: std::time::Instant, replica: Option<&BookReplica>) -> Step {
        self.twaps.on_read_through(stream, at);
        self.twap_step(replica)
    }

    /// A TWAP stream lost lines, detected at wall time `detected_ms`.
    pub(crate) fn on_twap_loss(&mut self, detected_ms: u64, replica: Option<&BookReplica>) -> Step {
        self.twaps.on_loss(detected_ms);
        self.twap_step(replica)
    }

    /// The account view's new anchor: its running TWAPs.
    pub(crate) fn on_anchor(&mut self, anchor: std::sync::Arc<crate::accounts::book::PendingAnchor>, replica: Option<&BookReplica>) -> Step {
        self.twaps.on_anchor(anchor);
        self.twap_step(replica)
    }

    /// Whether the suborders overlaid are those of a TWAP state proven complete, at the confirmed height or earlier in
    /// its block time's second (batch 2i, user 11-01; design 000624 r4 section 5 otherwise).
    pub(crate) const fn twaps_known(&self) -> bool {
        self.due_of.is_some()
    }

    /// A TWAP input's step: the TWAP book settled at the confirmed height, the coins whose suborders changed replayed.
    fn twap_step(&mut self, replica: Option<&BookReplica>) -> Step {
        let mut step = Step::default();
        let Some((replica, height, confirmed_ns)) = aligned(replica).filter(|_| !self.gaps.replica) else { return step };
        let emit = self.availability(Some(replica)).is_ok();
        step.changed = self.settle_twaps(height, unix_ms(confirmed_ns), emit);
        self.replay_coins(replica, &step.changed, emit, &mut step.trades);
        step
    }

    /// The TWAP book settled at `height`: the suborders due anew when what they stand on changed, and the coins the
    /// trade window replays. The coins whose suborders changed, each holding an entry while it has any (design 000624
    /// r4: no other coin replays for it).
    fn settle_twaps(&mut self, height: u64, basis_ms: u64, emit: bool) -> HashSet<Coin> {
        let second = basis_ms / 1_000;
        let due_of = match self.twaps.settle(height) {
            Ok(()) => Some((self.twaps.version(), second)),
            // the next height not proven yet: this second's suborders stand (batch 2i, design 000631 r2)
            Err(TwapsUnknown::Incomplete) if self.due_of.is_some_and(|(_, of)| of == second) => self.due_of,
            Err(_) => None,
        };
        let mut affected = self.due_anew(due_of, basis_ms);
        affected.extend(self.trade_window_coins(height, basis_ms, emit));
        affected
    }

    /// The suborders due as of `due_of`, when it is not what they stand on: none while `None`. The coins whose
    /// suborders changed.
    fn due_anew(&mut self, due_of: Option<(u64, u64)>, basis_ms: u64) -> HashSet<Coin> {
        if due_of == self.due_of {
            return HashSet::new();
        }
        let known = due_of.is_some();
        let due = if known { self.twaps.due(basis_ms) } else { DueSlices::default() };
        let mut affected: HashSet<Coin> = due.iter().filter(|(coin, slices)| self.due.get(*coin) != Some(slices)).map(|(coin, _)| coin.clone()).collect();
        affected.extend(self.due.keys().filter(|coin| !due.contains_key(*coin)).cloned());
        for coin in due.keys() {
            self.coins.entry(coin.clone()).or_default();
        }
        self.coins.retain(|coin, actions| !actions.actions.is_empty() || due.contains_key(coin));
        if let Some(first_of_second_ms) = self.twaps.first_of_second_ms().filter(|_| known) {
            self.emitted.retain(|_, due_ms| *due_ms >= first_of_second_ms);
        }
        self.due = due;
        self.due_of = due_of;
        affected
    }

    /// Whether a confirmed block time is in the trade window: within W of the next second, the start of the block that
    /// runs the suborders due (batch 2i); a W of a second or more takes every one (review 000631 r3 P3: no sum to
    /// overflow).
    const fn in_trade_window(&self, basis_ms: u64) -> bool {
        self.params.twap_trade_window_ms >= 1_000 - basis_ms % 1_000
    }

    /// The coins holding a suborder whose trades have not gone out, once the confirmed block time is in the trade window:
    /// at most once a confirmed height, while the book may be served (review 000631 r1 P2-2).
    fn trade_window_coins(&mut self, height: u64, basis_ms: u64, emit: bool) -> HashSet<Coin> {
        if !emit || !self.in_trade_window(basis_ms) || self.window_height == Some(height) {
            return HashSet::new();
        }
        self.window_height = Some(height);
        let emitted = &self.emitted;
        self.due
            .iter()
            .filter(|(_, slices)| slices.iter().any(|slice| !emitted.contains_key(&(slice.twap_id, slice.slot))))
            .map(|(coin, _)| coin.clone())
            .collect()
    }

    /// `coins` replayed over the replica, each one's TWAP suborders first; a suborder's fills go out once, the first time
    /// it fills while `emit` in the trade window.
    fn replay_coins(&mut self, replica: &BookReplica, coins: &HashSet<Coin>, emit: bool, trades: &mut Vec<PendingTrade>) {
        let emit_twaps = emit && self.in_trade_window(self.basis_ms);
        let Self { coins: held, due, emitted, record, refusals, basis_ms, .. } = self;
        let basis = Basis::new(record.priorities(), refusals, *basis_ms);
        for coin in coins {
            let Some(actions) = held.get_mut(coin) else { continue };
            let slices = due.get(coin).map_or(&[][..], Vec::as_slice);
            let filled = actions.replay(replica.coin(coin), basis, slices);
            for (slice, fills) in slices.iter().zip(filled) {
                if !emit_twaps || fills.is_empty() || emitted.insert((slice.twap_id, slice.slot), slice.due_ms).is_some() {
                    continue;
                }
                for fill in &fills {
                    PENDING_BOOK_PREDICTED_FILLS_TOTAL.with_label_values(&[fill.maker.label()]).inc();
                }
                trades.extend(fills.into_iter().map(|fill| twap_trade(coin, slice, fill)));
            }
        }
    }

    /// The mempool lost lines.
    pub(crate) fn on_mempool_loss(&mut self) {
        self.open_mempool_gap("loss");
    }

    /// The block log lost lines: its `twapCancel`s too, the TWAPs settled unknown - their coins replayed without
    /// their suborders.
    pub(crate) fn on_block_log_loss(&mut self, replica: Option<&BookReplica>) -> Step {
        self.open_block_log_gap("block_log_loss");
        self.twaps.on_loss(wall_ms());
        self.twap_step(replica)
    }

    /// The replica went; another generation follows. The overlays wait for it.
    pub(crate) fn on_replica_gone(&mut self) {
        if self.gaps.replica {
            return;
        }
        PENDING_BOOK_GAPS_TOTAL.with_label_values(&["replica"]).inc();
        self.gaps.replica = true;
        self.coins.values_mut().for_each(|actions| actions.overlay = Overlay::default());
        self.refresh_all = true;
    }

    /// Whether every coin's frame is to be built again - the overlays dropped or rebuilt whole since it last was -, the
    /// flag taken. The publisher takes it when it can frame, a replica aligned.
    pub(crate) const fn take_refresh_all(&mut self) -> bool {
        std::mem::replace(&mut self.refresh_all, false)
    }

    /// Whether the pending thread takes the log's next block: the record holds fewer than `MAX_LEAD_BLOCKS` above the
    /// confirmed height (r7 revision 20); past it the log's watcher waits on its full queue.
    pub(crate) fn takes_blocks(&self, replica: Option<&BookReplica>) -> bool {
        self.record.lead(replica.and_then(BookReplica::confirmed)) < MAX_LEAD_BLOCKS
    }

    pub(crate) fn availability(&self, replica: Option<&BookReplica>) -> Result<(), Unavailable> {
        if self.gaps.replica {
            return Err(Unavailable::Replica);
        }
        if let Some(cause) = self.gaps.mempool {
            return Err(Unavailable::Mempool(cause));
        }
        if let Some(cause) = self.gaps.block_log {
            return Err(Unavailable::BlockLog(cause));
        }
        let (_, height, confirmed_ns) = aligned(replica).ok_or(Unavailable::Replica)?;
        if !self.record_covers(height) {
            return Err(Unavailable::BlockLogBehind);
        }
        if confirmed_ns < self.gaps.ready_from_ns {
            return Err(Unavailable::WarmingUp);
        }
        if self.newest_seen_ns.is_some_and(|seen_ns| seen_ns - confirmed_ns > self.params.max_span_ns) {
            return Err(Unavailable::Span);
        }
        Ok(())
    }

    /// The gauges: actions held, known and unknown; the mempool's lead on the confirmed blocks; the coins whose
    /// merged best levels differ from the replica's; the log's blocks above the confirmed height.
    pub(crate) fn export(&self, replica: Option<&BookReplica>) {
        let unknown = self.coins.values().flat_map(|actions| &actions.actions).filter(|action| action.unknown).count();
        PENDING_BOOK_HELD_ACTIONS.with_label_values(&["unknown"]).set(gauge(unknown));
        PENDING_BOOK_HELD_ACTIONS.with_label_values(&["known"]).set(gauge(self.held - unknown));
        let aligned = aligned(replica);
        let span_ns = self.newest_seen_ns.zip(aligned).map_or(0, |(seen_ns, (_, _, confirmed_ns))| seen_ns - confirmed_ns);
        PENDING_BOOK_SPAN_SECONDS.set(span_ns as f64 / 1e9);
        let moved = aligned.map_or(0, |(replica, _, _)| {
            self.coins.keys().filter(|coin| self.bbo(coin, replica) != Overlay::default().bbo(replica.coin(coin))).count()
        });
        PENDING_BOOK_MOVED_COINS.set(gauge(moved));
        let lead = self.record.lead(replica.and_then(BookReplica::confirmed));
        PENDING_BOOK_BLOCK_LOG_LEAD_BLOCKS.set(i64::try_from(lead).unwrap_or(i64::MAX));
    }

    /// `coin`'s merged book's best levels, (bid, ask).
    fn bbo(&self, coin: &Coin, replica: &BookReplica) -> (Option<BestLevel>, Option<BestLevel>) {
        let base = replica.coin(coin);
        self.coins.get(coin).map_or_else(|| Overlay::default().bbo(base), |actions| actions.overlay.bbo(base))
    }

    /// `coin` as served: its merged and its replica's best levels, and its actions held.
    pub(crate) fn coin_view(&self, coin: &Coin, replica: &BookReplica) -> CoinView {
        let actions = self.coins.get(coin).map_or(&[][..], |actions| actions.actions.as_slice());
        let seen_ms = |seen_ns: Option<i64>| seen_ns.map(unix_ms);
        let pending = CoinPending {
            actions: actions.len(),
            oldest_seen_ms: seen_ms(actions.iter().map(|action| action.seen_ns).min()),
            newest_seen_ms: seen_ms(actions.iter().map(|action| action.seen_ns).max()),
        };
        CoinView { bbo: self.bbo(coin, replica), basis_bbo: Overlay::default().bbo(replica.coin(coin)), pending }
    }

    /// The coins the set holds actions on.
    pub(crate) fn coins(&self) -> impl Iterator<Item = &Coin> {
        self.coins.keys()
    }

    /// A line's bearing on the mempool's gaps: one taken after a loss, or without an asset table, opens one; a fresh one
    /// - no more than S older than the confirmed block time - ends it (review 000611 r1 P1-2).
    fn mempool_gaps(&mut self, seen_ns: i64, after_loss: bool, named: bool, aligned: Option<Aligned<'_>>) {
        match (after_loss, named) {
            (true, _) => self.open_mempool_gap("read_after_loss"),
            (false, false) => self.open_mempool_gap("no_asset_table"),
            (false, true) if aligned.is_none_or(|(_, _, confirmed_ns)| confirmed_ns - seen_ns <= self.params.silence_ns) => {
                if self.gaps.mempool.take().is_some() {
                    self.gaps.ready_from(seen_ns + self.params.expiry_ns);
                }
            }
            (false, true) => {}
        }
    }

    /// Why a line is not held: expired by the confirmed block time, or its block confirmed already (late into the
    /// mempool).
    fn not_held(&self, tx: &MempoolTx, aligned: Option<Aligned<'_>>) -> Option<&'static str> {
        let (_, height, confirmed_ns) = aligned?;
        if self.params.expired(tx.seen_ns, confirmed_ns) {
            return Some("expired_on_arrival");
        }
        self.record.height_of(&tx.tx_hash).is_some_and(|block| block <= height).then_some("late")
    }

    fn hold(&mut self, coin: Coin, action: PendingAction, aligned: Option<Aligned<'_>>, emit: bool, step: &mut Step) {
        self.held += 1;
        let behind = aligned.is_some_and(|(_, height, _)| !self.record_covers(height));
        let (record, refusals) = (&self.record, &self.refusals);
        let base = aligned
            .filter(|_| !self.gaps.replica)
            .map(|(replica, _, confirmed_ns)| (replica.coin(&coin), Basis::new(record.priorities(), refusals, unix_ms(confirmed_ns))));
        let actions = self.coins.entry(coin.clone()).or_default();
        if let (false, Some((base, basis))) = (action.unknown, base) {
            self.priorities_behind |= behind;
            let effect = actions.overlay.apply(base, basis, &action);
            PENDING_BOOK_OVERLAY_TOTAL.with_label_values(&[action.op.label(), effect.outcome]).inc();
            // only an action placing an order fills
            if let (true, Some(order)) = (emit, action.op.placement()) {
                for fill in &effect.fills {
                    PENDING_BOOK_PREDICTED_FILLS_TOTAL.with_label_values(&[fill.maker.label()]).inc();
                }
                step.trades.extend(effect.fills.into_iter().map(|fill| predicted(&coin, &action, order, fill)));
            }
        }
        actions.actions.push(action);
        // its frame's pending counts change even when its book does not
        step.changed.insert(coin);
    }

    /// The one settling step (r7 revision 18): the lines a block at or below the confirmed height holds, and those
    /// expired, leave; the takers' outcomes of the blocks up to it apply (batch 2g, whichever input moved); the coins
    /// they were on, those holding a taker whose refusal changed, those in `changed` and - on a new generation - every
    /// coin held are replayed.
    fn settle(&mut self, replica: Option<&BookReplica>, mut changed: HashSet<Coin>) -> Step {
        let Some((replica, height, confirmed_ns)) = aligned(replica) else { return Step { trades: Vec::new(), changed } };
        #[cfg(test)]
        {
            self.settles += 1;
        }
        let new_generation = std::mem::replace(&mut self.gaps.replica, false);
        if self.newest_seen_ns.is_none_or(|seen_ns| confirmed_ns - seen_ns > self.params.silence_ns) {
            self.open_mempool_gap("silence");
        }
        self.remove_settled(height, confirmed_ns, &mut changed);
        self.record.evict(height, unix_ms(confirmed_ns - self.params.retention_ns()));
        let refusals_changed = self.refusals.apply_through(height);
        if !refusals_changed.is_empty() {
            changed.extend(self.coins.iter().filter(|(_, actions)| actions.holds_taker_of(&refusals_changed)).map(|(coin, _)| coin.clone()));
        }
        // an action held came no earlier than E before the confirmed block time: a refusal out of its reach goes unreplayed
        self.refusals.evict(unix_ms(confirmed_ns - self.params.expiry_ns));
        if new_generation {
            changed.extend(self.coins.keys().cloned());
            self.refresh_all = true;
        }
        let covered = self.record_covers(height);
        if covered && std::mem::take(&mut self.priorities_behind) {
            changed.extend(self.coins.keys().cloned());
        }
        let basis_ms = unix_ms(confirmed_ns);
        if basis_ms != self.basis_ms {
            changed.extend(self.coins.iter().filter(|(_, actions)| actions.holds_priority()).map(|(coin, _)| coin.clone()));
            self.basis_ms = basis_ms;
        }
        let emit = self.availability(Some(replica)).is_ok();
        changed.extend(self.settle_twaps(height, basis_ms, emit));
        self.priorities_behind |= !covered && changed.iter().any(|coin| self.coins.contains_key(coin));
        let mut trades = Vec::new();
        self.replay_coins(replica, &changed, emit, &mut trades);
        Step { trades, changed }
    }

    /// Whether the block record reaches the confirmed `height`: every confirmed order's ALO priority known.
    fn record_covers(&self, height: u64) -> bool {
        self.record.newest().is_some_and(|newest| newest >= height)
    }

    /// The lines a block at or below `height` holds, and those expired, out of the set; their coins into `changed`.
    fn remove_settled(&mut self, height: u64, confirmed_ns: i64, changed: &mut HashSet<Coin>) {
        let Self { lines, record, params, coins, held, due, .. } = self;
        let mut gone: FxHashMap<B256, &'static str> = FxHashMap::default();
        lines.retain(|hash, line| {
            let outcome = if record.height_of(hash).is_some_and(|block| block <= height) {
                "confirmed"
            } else if params.expired(line.seen_ns, confirmed_ns) {
                "expired"
            } else {
                return true;
            };
            gone.insert(*hash, outcome);
            changed.extend(line.coins.iter().cloned());
            false
        });
        if gone.is_empty() {
            return;
        }
        coins.retain(|coin, actions| {
            if !changed.contains(coin) {
                return true;
            }
            actions.actions.retain(|action| {
                let Some(outcome) = gone.get(&action.id.tx_hash) else { return true };
                count_action(action, outcome);
                *held -= 1;
                false
            });
            !actions.actions.is_empty() || due.contains_key(coin)
        });
    }

    fn open_mempool_gap(&mut self, cause: &'static str) {
        if self.gaps.mempool.is_some() {
            return;
        }
        PENDING_BOOK_GAPS_TOTAL.with_label_values(&[cause]).inc();
        self.gaps.mempool = Some(cause);
    }

    /// A block log gap: every action held unknown, every overlay empty, the takers' refusals forgotten (their outcomes
    /// in the gap unknown).
    fn open_block_log_gap(&mut self, cause: &'static str) {
        if self.gaps.block_log.is_some() {
            return;
        }
        PENDING_BOOK_GAPS_TOTAL.with_label_values(&[cause]).inc();
        self.gaps.block_log = Some(cause);
        self.refusals.clear();
        for actions in self.coins.values_mut() {
            actions.actions.iter_mut().for_each(|action| action.unknown = true);
            actions.overlay = Overlay::default();
        }
        self.refresh_all = true;
    }

    /// The block log's gap ended at `t_end_ns`, the time of the block after it (r7 revision 19).
    fn end_block_log_gap(&mut self, t_end_ns: i64) {
        if self.gaps.block_log.take().is_none() {
            return;
        }
        let warm_until_ns = (t_end_ns + self.params.max_late_ns).max(self.newest_seen_ns.unwrap_or(i64::MIN));
        self.gaps.warm_until_ns = self.gaps.warm_until_ns.max(warm_until_ns);
        self.gaps.ready_from(self.gaps.warm_until_ns + self.params.expiry_ns);
    }

    /// The set and `arriving` actions past its cap: dropped whole, the newest line ending that gap.
    fn drop_over_cap(&mut self, arriving: usize) {
        PENDING_BOOK_GAPS_TOTAL.with_label_values(&["over_cap"]).inc();
        PENDING_BOOK_ACTIONS_TOTAL.with_label_values(&["any", "over_cap"]).inc_by((self.held + arriving) as u64);
        self.coins.clear();
        self.lines.clear();
        self.held = 0;
        self.refresh_all = true;
        if let Some(newest_seen_ns) = self.newest_seen_ns {
            self.gaps.ready_from(newest_seen_ns + self.params.expiry_ns);
        }
    }

    #[cfg(test)]
    pub(crate) const fn settles(&self) -> usize {
        self.settles
    }

    #[cfg(test)]
    fn held(&self, coin: &str) -> Vec<(usize, bool)> {
        self.coins.get(&Coin::new(coin)).map_or_else(Vec::new, |actions| actions.actions.iter().map(|action| (action.id.action, action.unknown)).collect())
    }
}

impl PendingTrade {
    /// The taker's line's `local_time`, unix ms; a TWAP suborder has none.
    pub(crate) fn seen_ms(&self) -> Option<u64> {
        match &self.taker {
            Taker::Pending { seen_ns, .. } => Some(unix_ms(*seen_ns)),
            Taker::Twap { .. } => None,
        }
    }

    /// The account the taker acts for, as known: a pending order's vault, a TWAP's user.
    pub(crate) const fn taker_account(&self) -> Option<Address> {
        match &self.taker {
            Taker::Pending { vault, .. } => *vault,
            Taker::Twap { user, .. } => Some(*user),
        }
    }
}

fn predicted(coin: &Coin, action: &PendingAction, order: &Placement, fill: Fill) -> PendingTrade {
    PendingTrade {
        coin: coin.clone(),
        side: order.side,
        px: fill.px,
        sz: fill.sz,
        maker: fill.maker,
        taker: Taker::Pending { id: action.id, seen_ns: action.seen_ns, cloid: order.cloid.clone(), vault: action.vault },
    }
}

fn twap_trade(coin: &Coin, slice: &TwapSlice, fill: Fill) -> PendingTrade {
    PendingTrade {
        coin: coin.clone(),
        side: if slice.is_buy { Side::Bid } else { Side::Ask },
        px: fill.px,
        sz: fill.sz,
        maker: fill.maker,
        taker: Taker::Twap { twap_id: slice.twap_id, user: slice.user, slot: slice.slot },
    }
}

/// The wall clock, unix ms: when a loss of unknown lines was detected.
#[expect(clippy::expect_used, reason = "a host clock before 1970 or past u64 ms is broken")]
pub(crate) fn wall_ms() -> u64 {
    let since_epoch = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("the host clock is after 1970");
    u64::try_from(since_epoch.as_millis()).expect("unix ms fit u64")
}

fn count_action(action: &PendingAction, outcome: &str) {
    PENDING_BOOK_ACTIONS_TOTAL.with_label_values(&[action.op.label(), outcome]).inc();
}

/// The replica, when aligned.
fn aligned(replica: Option<&BookReplica>) -> Option<Aligned<'_>> {
    let replica = replica?;
    let height = replica.confirmed()?;
    Some((replica, height, unix_ns(replica.confirmed_time_ms()?)))
}

/// A block time's unix ms in ns.
#[expect(clippy::expect_used, reason = "a block time is a chrono time's unix ms, an i64 to begin with")]
fn unix_ns(ms: u64) -> i64 {
    i64::try_from(ms).expect("a block time in unix ms fits in i64") * NS_PER_MS
}

/// Unix ns to ms, a time before 1970 as 0 (nothing is older).
fn unix_ms(ns: i64) -> u64 {
    (ns.max(0) / NS_PER_MS).unsigned_abs()
}

fn gauge(count: usize) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        listeners::order_book::replica::tests::{block_lines, block_time_ms, btc_diff, btc_status, new_diff, remove, replica_of_empty_book},
        order_book::Oid,
        pending::{
            actions::tests::{coins, requested},
            blocks::tests::hash,
            mempool::{ActionKind, RequestedTif, SignedAction},
            refusals::TakerOutcome,
            signer::ActingIdentity,
        },
    };

    /// E 1 s, H 1 s, the span 10 s, 1,000 actions; silence 60 s, out of the way unless a test sets it.
    pub(crate) fn params() -> PendingParams {
        PendingParams::from_ms(1_000, 1_000, 10_000, 1_000, 60_000, 300_000, 1_000).unwrap()
    }

    /// As `params`, H as r6 revision 17's default.
    fn params_late(max_late_ms: u64) -> PendingParams {
        PendingParams::from_ms(1_000, max_late_ms, 10_000, 1_000, 60_000, 300_000, 1_000).unwrap()
    }

    const BTC: &str = "BTC";
    const NONE_HELD: [(usize, bool); 0] = [];
    pub(crate) const NO_TRADES: [PendingTrade; 0] = [];

    type Events = Vec<serde_json::Value>;

    /// The replica (blocks at height x 100 ms), the block log and the pending set over them; one clock for the log's
    /// reads and the TWAP streams' read-throughs.
    struct Harness {
        replica: BookReplica,
        shadow: Shadow,
        /// The last height the replica confirmed.
        height: u64,
        /// The last height the log showed.
        logged: u64,
        clock: std::time::Instant,
    }

    impl Harness {
        fn new(params: PendingParams) -> Self {
            Self { replica: replica_of_empty_book(), shadow: Shadow::new(params, true, 0), height: 0, logged: 0, clock: std::time::Instant::now() }
        }

        fn tick(&mut self) -> std::time::Instant {
            self.clock += std::time::Duration::from_micros(1);
            self.clock
        }

        /// Both TWAP streams read through now.
        fn prove_twaps(&mut self) -> Step {
            let at = self.tick();
            drop(self.shadow.on_twap_read_through(TwapStream::Statuses, at, Some(&self.replica)));
            self.shadow.on_twap_read_through(TwapStream::Fills, at, Some(&self.replica))
        }

        /// The TWAPs known at the confirmed height: the log's next block read, both streams read through after it.
        fn twaps_known_now(&mut self) -> Step {
            if self.logged == self.height {
                self.log(&[]);
            }
            self.prove_twaps()
        }

        /// The log's next block, holding the lines of `hashes`.
        fn log(&mut self, hashes: &[u8]) -> Step {
            self.log_placing(hashes, Vec::new())
        }

        /// The log's next block, holding the lines of `hashes`, placing orders with ALO `priorities` (oid, priority).
        fn log_placing(&mut self, hashes: &[u8], priorities: Vec<(u64, u64)>) -> Step {
            self.logged += 1;
            let hashes = hashes.iter().map(|byte| hash(*byte)).collect();
            let read_at = self.tick();
            let block = LoggedBlock { height: self.logged, time_ms: block_time_ms(self.logged), hashes, priorities, taker_outcomes: Vec::new(), twap_cancels: Vec::new(), read_at };
            self.shadow.on_block(block, false, Some(&self.replica))
        }

        /// The log's next block, with its takers' `outcomes`; `after_loss` as the watcher tells it.
        fn log_outcomes(&mut self, outcomes: Vec<TakerOutcome>, after_loss: bool) -> Step {
            self.logged += 1;
            let read_at = self.tick();
            let block = LoggedBlock { height: self.logged, time_ms: block_time_ms(self.logged), hashes: Vec::new(), priorities: Vec::new(), taker_outcomes: outcomes, twap_cancels: Vec::new(), read_at };
            self.shadow.on_block(block, after_loss, Some(&self.replica))
        }

        /// The replica confirms its next block, with `statuses` and `diffs` (the next height's lines come along, empty).
        fn confirm(&mut self, statuses: Events, diffs: Events) -> Step {
            self.height += 1;
            let mut changed = HashSet::new();
            for batch in block_lines(self.height, statuses, diffs).into_iter().chain(block_lines(self.height + 1, vec![], vec![])) {
                changed.extend(self.replica.push(batch).unwrap());
            }
            self.shadow.on_applied(changed, &self.replica)
        }

        /// The next block: the log's first, as it leads the book streams, then the replica's.
        fn block(&mut self, hashes: &[u8], statuses: Events, diffs: Events) -> Step {
            self.log(hashes);
            self.confirm(statuses, diffs)
        }

        /// Empty blocks until the one at `ms`.
        fn idle_until(&mut self, ms: u64) {
            while block_time_ms(self.height) < ms {
                self.block(&[], vec![], vec![]);
            }
        }

        /// A line seen at `seen_ms`, its hash from `byte`, with `actions`.
        fn line(&mut self, seen_ms: i64, byte: u8, actions: Vec<ActionKind>) -> Step {
            self.priority_line(seen_ms, byte, 0, actions)
        }

        /// A line as `line`'s, its actions with the ALO priority `priority`.
        fn priority_line(&mut self, seen_ms: i64, byte: u8, priority: u64, actions: Vec<ActionKind>) -> Step {
            self.signed_line(seen_ms, byte, priority, None, actions)
        }

        /// A line as `line`'s, its actions signed by `signer`, recovered.
        fn signed_line(&mut self, seen_ms: i64, byte: u8, priority: u64, signer: Option<Address>, actions: Vec<ActionKind>) -> Step {
            let tx = MempoolTx {
                seen_ns: seen_ms * NS_PER_MS,
                tx_hash: hash(byte),
                actions: actions.into_iter().map(|kind| SignedAction { vault: None, signer, expires_after_ms: None, priority, kind }).collect(),
            };
            self.shadow.on_tx(&tx, false, Some(&coins()), Some(&self.replica))
        }

        fn available(&self) -> Result<(), Unavailable> {
            self.shadow.availability(Some(&self.replica))
        }

        fn bbo(&self) -> (Option<BestLevel>, Option<BestLevel>) {
            self.shadow.bbo(&Coin::new(BTC), &self.replica)
        }

        /// Served from `start_ms` + H + E: the replica and the log from the block at `start_ms`, a line then.
        fn ready_with(params: PendingParams, start_ms: u64) -> Self {
            let mut harness = Self::new(params);
            harness.height = start_ms / 100 - 1;
            harness.logged = harness.height;
            harness.replica = replica_at(harness.height);
            harness.block(&[], vec![], vec![]);
            harness.line(i64::try_from(start_ms).unwrap(), 0, vec![]);
            harness.idle_until(start_ms + u64::try_from((params.max_late_ns + params.expiry_ns) / NS_PER_MS).unwrap());
            assert_eq!(harness.available(), Ok(()));
            harness
        }

        fn ready_from(start_ms: u64) -> Self {
            Self::ready_with(params(), start_ms)
        }

        /// An ask of 1.0 at 101, oid `oid`, resting in the next block.
        fn rest_ask(&mut self, oid: u64) {
            let at = block_time_ms(self.height + 1);
            self.block(&[], vec![btc_status(oid, "A", "101.0", "1.0", "open", None, "Alo", at)], vec![btc_diff(oid, "A", "101.0", new_diff("1.0"))]);
        }
    }

    /// A replica standing at the end of `height`.
    fn replica_at(height: u64) -> BookReplica {
        let mut replica = replica_of_empty_book();
        for batch in block_lines(height, vec![], vec![]) {
            replica.push(batch).unwrap();
        }
        replica
    }

    fn ioc(cloid: &str) -> ActionKind {
        ActionKind::Order(vec![requested(0, true, "101", RequestedTif::Ioc, Some(cloid))])
    }

    fn alo_bid(px: &str, cloid: &str) -> ActionKind {
        ActionKind::Order(vec![requested(0, true, px, RequestedTif::Alo, Some(cloid))])
    }

    fn alo_ask(px: &str, cloid: &str) -> ActionKind {
        ActionKind::Order(vec![requested(0, false, px, RequestedTif::Alo, Some(cloid))])
    }

    /// The maker of the one trade `step` predicts.
    fn maker_of(step: &Step) -> &Maker {
        let [trade] = step.trades.as_slice() else { panic!("{step:?}") };
        &trade.maker
    }

    fn level(px: &str, sz: &str, orders: u32) -> BestLevel {
        (Px::parse_from_str(px).unwrap(), Sz::parse_from_str(sz).unwrap(), orders)
    }

    /// The IOC's fill of the ask oid 11: its status and the ask's, the ask's removal.
    fn ioc_fill(cloid: &str, at: u64) -> (Events, Events) {
        let statuses = vec![
            btc_status(20, "B", "101.0", "0.0", "filled", Some(cloid), "Ioc", at),
            btc_status(11, "A", "101.0", "0.0", "filled", None, "Alo", 1_000),
        ];
        (statuses, vec![btc_diff(11, "A", "101.0", remove())])
    }

    /// An IOC's trade goes out at its arrival, never on a replay; the block holding its line takes it out.
    #[test]
    fn a_taker_predicts_its_trade_once_and_leaves_with_its_block() {
        let mut harness = Harness::ready_from(1_000);
        harness.rest_ask(11);
        assert_eq!(harness.bbo().1, Some(level("101", "1", 1)));
        let step = harness.line(3_150, 1, vec![ioc("0x01")]);
        let [trade] = step.trades.as_slice() else { panic!("{step:?}") };
        assert!(matches!(&trade.taker, Taker::Pending { cloid, .. } if cloid.as_deref() == Some("0x01")));
        assert_eq!((trade.side, trade.sz), (Side::Bid, Sz::parse_from_str("1").unwrap()));
        assert!(matches!(&trade.maker, Maker::Confirmed { oid, .. } if *oid == Oid::new(11)));
        assert_eq!(harness.bbo().1, None);
        assert_eq!(harness.block(&[], vec![], vec![]).trades, NO_TRADES);
        let (statuses, diffs) = ioc_fill("0x01", block_time_ms(harness.height + 1));
        let step = harness.block(&[1], statuses, diffs);
        assert_eq!(step.trades, NO_TRADES);
        assert!(step.changed.contains(&Coin::new(BTC)));
        assert_eq!(harness.shadow.held(BTC), NONE_HELD);
        assert_eq!(harness.bbo(), (None, None));
    }

    /// R6 revision 15: a line 2.25 s late into the mempool (the 10-31 sample's latest 2.34 s), its block confirmed: told
    /// apart by its hash with H at 5 s, skipped whole - no trade, the other ask untouched.
    #[test]
    fn a_line_late_into_the_mempool_is_skipped_whole() {
        let mut harness = Harness::ready_with(params_late(5_000), 1_000);
        harness.rest_ask(11);
        harness.rest_ask(12);
        let (statuses, diffs) = ioc_fill("0x01", block_time_ms(harness.height + 1));
        harness.block(&[1], statuses, diffs);
        let block_ms = block_time_ms(harness.height);
        harness.idle_until(block_ms + 2_200);
        let step = harness.line(i64::try_from(block_ms + 2_250).unwrap(), 1, vec![ioc("0x01")]);
        assert_eq!(step.trades, NO_TRADES);
        assert_eq!(harness.shadow.held(BTC), NONE_HELD);
        assert_eq!(harness.bbo().1, Some(level("101", "1", 1)));
    }

    /// A line whose block the log showed but the replica has not confirmed yet is held - its book not there yet - and
    /// leaves when the replica gets there.
    #[test]
    fn a_line_whose_block_is_above_the_replica_waits_for_it() {
        let mut harness = Harness::ready_from(1_000);
        harness.rest_ask(11);
        harness.log(&[1]);
        assert_eq!(harness.line(3_150, 1, vec![ioc("0x01")]).trades.len(), 1);
        assert_eq!(harness.shadow.held(BTC), [(0, false)]);
        let (statuses, diffs) = ioc_fill("0x01", block_time_ms(harness.height + 1));
        harness.confirm(statuses, diffs);
        assert_eq!(harness.shadow.held(BTC), NONE_HELD);
        assert_eq!(harness.bbo(), (None, None));
    }

    /// R7 revision 18: the replica confirms the block resting the pending ALO before the log shows it - the book is
    /// not served -; the block's hashes then settle at once, without another block: the bid is 1, not 2.
    #[test]
    fn the_block_log_catching_up_settles_at_once() {
        let mut harness = Harness::ready_from(1_000);
        harness.line(3_050, 1, vec![alo_bid("100", "0x0a")]);
        assert_eq!(harness.bbo().0, Some(level("100", "1", 1)));
        let at = block_time_ms(harness.height + 1);
        harness.confirm(vec![btc_status(30, "B", "100.0", "1.0", "open", Some("0x0a"), "Alo", at)], vec![btc_diff(30, "B", "100.0", new_diff("1.0"))]);
        assert_eq!(harness.available(), Err(Unavailable::BlockLogBehind));
        harness.log(&[1]);
        assert_eq!(harness.available(), Ok(()));
        assert_eq!(harness.shadow.held(BTC), NONE_HELD);
        assert_eq!(harness.bbo().0, Some(level("100", "1", 1)));
    }

    /// A line delivered again while held is dropped; once its block is confirmed, skipped.
    #[test]
    fn a_repeated_line_is_dropped_and_once_confirmed_skipped() {
        let mut harness = Harness::ready_from(1_000);
        harness.rest_ask(11);
        assert_eq!(harness.line(3_150, 1, vec![ioc("0x01")]).trades.len(), 1);
        assert_eq!(harness.line(3_150, 1, vec![ioc("0x01")]).trades, NO_TRADES);
        assert_eq!(harness.shadow.held(BTC), [(0, false)]);
        let (statuses, diffs) = ioc_fill("0x01", block_time_ms(harness.height + 1));
        harness.block(&[1], statuses, diffs);
        assert_eq!(harness.line(3_150, 1, vec![ioc("0x01")]).trades, NO_TRADES);
        assert_eq!(harness.shadow.held(BTC), NONE_HELD);
    }

    /// R6 revision 16: the replica goes while the log goes on; the block holding a held line is confirmed in the gap.
    /// The new generation's first confirmation takes the line out by its hash and is served at once - no warm-up -,
    /// every coin held replayed on it: the ETH bid no block took is back on its merged book.
    #[test]
    fn a_new_generation_settles_by_hash_without_warm_up() {
        let mut harness = Harness::ready_from(1_000);
        harness.line(3_050, 1, vec![alo_bid("100", "0x0a")]);
        harness.line(3_060, 3, vec![ActionKind::Order(vec![requested(1, true, "2000", RequestedTif::Alo, Some("0x0e"))])]);
        harness.shadow.on_replica_gone();
        assert_eq!(harness.shadow.bbo(&Coin::new("ETH"), &harness.replica).0, None, "no replica to overlay");
        assert_eq!(harness.available(), Err(Unavailable::Replica));
        assert_eq!(harness.shadow.held(BTC), [(0, false)], "known still");
        harness.log(&[]);
        harness.log(&[1]);
        harness.log(&[]);
        harness.line(3_350, 2, vec![]);
        harness.height = harness.logged - 1;
        harness.replica = replica_at(harness.height);
        harness.confirm(vec![], vec![]);
        assert_eq!(harness.shadow.held(BTC), NONE_HELD);
        assert_eq!(harness.available(), Ok(()));
        assert_eq!(harness.shadow.bbo(&Coin::new("ETH"), &harness.replica).0, Some(level("2000", "1", 1)));
    }

    /// R7 revision 19's timeline: H 5 s; the log's gap ends at its block at `T_end` 10,000 while the replica is at
    /// 16,000 and an ALO seen at 15,900 was taken unknown: W = 15,900, served from 16,900, the ALO expired then.
    #[test]
    fn a_block_log_gap_warms_up_past_the_newest_line() {
        let mut harness = Harness::ready_with(params_late(5_000), 1_000);
        harness.idle_until(9_900);
        drop(harness.shadow.on_block_log_loss(Some(&harness.replica)));
        assert_eq!(harness.available(), Err(Unavailable::BlockLog("block_log_loss")));
        while block_time_ms(harness.height) < 15_800 {
            harness.confirm(vec![], vec![]);
        }
        harness.line(15_850, 1, vec![]);
        harness.line(15_900, 2, vec![alo_bid("100", "0x0a")]);
        assert_eq!(harness.shadow.held(BTC), [(0, true)]);
        assert_eq!(harness.bbo().0, None, "unknown: not overlaid");
        harness.confirm(vec![], vec![]);
        harness.confirm(vec![], vec![]);
        while harness.logged < harness.height {
            harness.log(&[]);
        }
        assert_eq!(harness.shadow.gaps.warm_until_ns, 15_900 * NS_PER_MS);
        assert_eq!(harness.available(), Err(Unavailable::WarmingUp));
        harness.idle_until(16_800);
        assert_eq!(harness.available(), Err(Unavailable::WarmingUp));
        harness.idle_until(16_900);
        assert_eq!(harness.available(), Ok(()));
        assert_eq!(harness.shadow.held(BTC), NONE_HELD, "expired at 16,900");
    }

    /// A hole in the log's heights is a gap ending at once, at the block after it: what is held turns unknown.
    #[test]
    fn a_hole_in_the_block_log_is_a_gap() {
        let mut harness = Harness::ready_from(1_000);
        harness.line(3_050, 1, vec![alo_bid("100", "0x0a")]);
        harness.logged += 1;
        harness.block(&[], vec![], vec![]);
        assert_eq!(harness.shadow.held(BTC), [(0, true)]);
        assert_eq!(harness.bbo().0, None);
        assert_eq!(harness.available(), Err(Unavailable::WarmingUp));
    }

    /// Review 000611 r3 P1: the log read to block 30, the replica on to 90, the record's blocks evicted; the log's next
    /// line is block 90. The jump is a hole all the same - a gap, what arrives then unknown: the IOC rejected in 89 and
    /// late into the mempool takes nothing of the ask resting at 85.
    #[test]
    fn a_jump_after_the_record_emptied_is_a_gap() {
        let mut harness = Harness::ready_from(1_000);
        let resting_at = 85;
        while harness.height < 89 {
            if harness.height + 1 == resting_at {
                let at = block_time_ms(resting_at);
                harness.confirm(vec![btc_status(11, "A", "101.0", "1.0", "open", None, "Alo", at)], vec![btc_diff(11, "A", "101.0", new_diff("1.0"))]);
            } else {
                harness.confirm(vec![], vec![]);
            }
        }
        harness.line(8_850, 1, vec![]);
        assert_eq!(harness.available(), Err(Unavailable::BlockLogBehind));
        harness.logged = 89;
        harness.log(&[]);
        assert_eq!(harness.available(), Err(Unavailable::WarmingUp));
        assert_eq!(harness.line(8_950, 2, vec![ioc("0x01")]).trades, NO_TRADES);
        assert_eq!(harness.shadow.held(BTC), [(0, true)]);
        assert_eq!(harness.bbo().1, Some(level("101", "1", 1)));
    }

    /// Review 000611 r3 P2-1: with spot left out of the book, the set leaves it out too - a replica there or not.
    #[test]
    fn spot_is_left_out_with_no_replica_there() {
        let mut shadow = Shadow::new(params(), false, 0);
        let tx = MempoolTx {
            seen_ns: 1_000 * NS_PER_MS,
            tx_hash: hash(1),
            actions: vec![SignedAction { vault: None, signer: None, expires_after_ms: None, priority: 0, kind: ActionKind::Order(vec![requested(10_107, true, "100", RequestedTif::Alo, Some("0x0a"))]) }],
        };
        shadow.on_tx(&tx, false, Some(&coins()), None);
        assert_eq!(shadow.held("@107"), NONE_HELD);
    }

    /// R6 revision 16: a mempool loss leaves what is held known and overlaid; nothing is served until a fresh line ends
    /// the gap and the confirmed block time passes it by E.
    #[test]
    fn a_mempool_gap_keeps_what_is_held() {
        let mut harness = Harness::ready_from(1_000);
        harness.line(3_050, 1, vec![alo_bid("100", "0x0a")]);
        harness.shadow.on_mempool_loss();
        assert_eq!(harness.available(), Err(Unavailable::Mempool("loss")));
        assert_eq!((harness.shadow.held(BTC), harness.bbo().0), (vec![(0, false)], Some(level("100", "1", 1))));
        harness.line(3_060, 2, vec![]);
        assert_eq!(harness.available(), Err(Unavailable::WarmingUp));
        harness.idle_until(4_060);
        assert_eq!(harness.available(), Ok(()));
    }

    /// Review 000611 r1 P1-2: silence past S; a line read late (3,100 with the blocks at 6,000) ends no gap; the next
    /// fresh one does, served E after it.
    #[test]
    fn a_stale_line_ends_no_silence() {
        let mut harness = Harness::new(PendingParams::from_ms(1_000, 1_000, 10_000, 1_000, 500, 300_000, 1_000).unwrap());
        harness.height = 9;
        harness.logged = 9;
        harness.replica = replica_at(9);
        while block_time_ms(harness.height) < 3_000 {
            harness.block(&[], vec![], vec![]);
            harness.line(i64::try_from(block_time_ms(harness.height)).unwrap(), 0, vec![]);
        }
        assert_eq!(harness.available(), Ok(()));
        harness.idle_until(6_000);
        assert_eq!(harness.available(), Err(Unavailable::Mempool("silence")));
        harness.line(3_100, 1, vec![]);
        assert_eq!(harness.available(), Err(Unavailable::Mempool("silence")));
        harness.line(6_050, 2, vec![]);
        assert_eq!((harness.available(), harness.shadow.gaps.ready_from_ns), (Err(Unavailable::WarmingUp), 7_050 * NS_PER_MS));
    }

    /// Review 000611 r1 P1-2: a line that would take the set past its cap drops it before any of it applies.
    #[test]
    fn a_line_past_the_cap_trades_nothing() {
        let mut harness = Harness::ready_with(PendingParams::from_ms(1_000, 1_000, 10_000, 1, 60_000, 300_000, 1_000).unwrap(), 1_000);
        harness.rest_ask(11);
        harness.line(3_150, 1, vec![alo_bid("99", "0x0a")]);
        assert_eq!(harness.line(3_160, 2, vec![ioc("0x01")]).trades, NO_TRADES);
        assert_eq!(harness.shadow.held(BTC), NONE_HELD);
        assert_eq!(harness.available(), Err(Unavailable::WarmingUp));
    }

    /// A line no block took leaves E after it, by block time; one expired on arrival is not held.
    #[test]
    fn a_line_no_block_took_expires() {
        let mut harness = Harness::ready_from(1_000);
        harness.line(2_000, 1, vec![ioc("0x01")]);
        assert_eq!(harness.shadow.held(BTC), NONE_HELD, "seen at 2,000 with the blocks at 3,000");
        harness.line(2_960, 2, vec![ioc("0x02")]);
        harness.idle_until(3_900);
        assert_eq!(harness.shadow.held(BTC), [(0, false)]);
        harness.idle_until(3_960);
        assert_eq!(harness.shadow.held(BTC), NONE_HELD);
    }

    /// A block changing a coin's book replays its actions: an ALO bid resting at 100.5 under the ask at 101 crosses the
    /// ask a block rests at 100.4, and is rejected.
    #[test]
    fn a_block_changing_the_book_replays_its_coin() {
        let mut harness = Harness::ready_from(1_000);
        harness.rest_ask(11);
        harness.line(3_150, 1, vec![alo_bid("100.5", "0x0a")]);
        assert_eq!(harness.bbo().0, Some(level("100.5", "1", 1)));
        let at = block_time_ms(harness.height + 1);
        harness.block(&[], vec![btc_status(12, "A", "100.4", "1.0", "open", None, "Alo", at)], vec![btc_diff(12, "A", "100.4", new_diff("1.0"))]);
        assert_eq!(harness.shadow.held(BTC), [(0, false)]);
        assert_eq!(harness.bbo(), (None, Some(level("100.4", "1", 1))));
    }

    /// Review 000620 r3 P2: an ask of no priority seen at 3,100 ms, empty blocks to 3,500, an ask of 9 seen at 3,550:
    /// the first is taken to be placed at 3,500 now, 50 ms before the second, which goes in front of it - an IOC meets
    /// the second first, as a replay of both would have it.
    #[test]
    fn an_order_resting_from_before_the_confirmed_block_is_placed_at_it() {
        let mut harness = Harness::ready_from(1_000);
        harness.line(3_100, 1, vec![alo_ask("101", "0x0a")]);
        harness.idle_until(3_500);
        harness.priority_line(3_550, 2, 9, vec![alo_ask("101", "0x0b")]);
        let step = harness.line(3_560, 3, vec![ioc("0x01")]);
        assert!(matches!(maker_of(&step), Maker::Pending { id, .. } if id.tx_hash == hash(2)), "{step:?}");
    }

    /// Review 000620 r4 P2: an ask of none at 3,100 ms and one of 9 at 3,500 - 400 ms apart, the second behind -; empty
    /// blocks to 3,400 put the first at 3,400, 100 ms before the second, which now goes in front: an IOC meets it first.
    #[test]
    fn the_confirmed_block_time_requeues_orders_both_held() {
        let mut harness = Harness::ready_from(1_000);
        harness.line(3_100, 1, vec![alo_ask("101", "0x0a")]);
        harness.priority_line(3_500, 2, 9, vec![alo_ask("101", "0x0b")]);
        harness.idle_until(3_400);
        let step = harness.line(3_501, 3, vec![ioc("0x01")]);
        assert!(matches!(maker_of(&step), Maker::Pending { id, .. } if id.tx_hash == hash(2)), "{step:?}");
    }

    /// Batch 2f: an ask of priority 6 seen at 3,350 ms goes in front of the ask at 101 resting from 3,100 (250 ms
    /// before) - an IOC then meets it first. Once the confirmed block is at 3,500 it is taken to be placed then, 400 ms
    /// after: the empty blocks replay the coin (review 000620 r2), and an IOC meets the replica's ask first.
    #[test]
    fn the_confirmed_block_time_alone_requeues_a_priority_order() {
        let pending = Maker::Pending { id: ActionId { tx_hash: hash(1), action: 0, item: 0 }, cloid: Some("0x0a".to_string()) };
        let replica = Maker::Confirmed { user: Address::with_last_byte(1), oid: Oid::new(11), cloid: None };
        for (blocks_until_ms, first) in [(3_100, pending), (3_500, replica)] {
            let mut harness = Harness::ready_from(1_000);
            harness.rest_ask(11);
            harness.priority_line(3_350, 1, 6, vec![alo_ask("101", "0x0a")]);
            harness.idle_until(blocks_until_ms);
            assert_eq!(maker_of(&harness.line(3_360, 2, vec![ioc("0x01")])), &first, "blocks until {blocks_until_ms}");
        }
    }

    /// Review 000620 r1 P2-2: the same facts, the replica ahead of the log or behind it. The ask at 101 rests with a
    /// priority of 9, an ask of 5 seen 200 ms after it stays behind it: with the replica first the ask of 5 is overlaid
    /// while the record lacks the 9, and replayed once the log brings it, before the book is served.
    #[test]
    fn a_priority_the_log_brings_late_is_replayed_before_serving() {
        for replica_first in [false, true] {
            let mut harness = Harness::ready_from(1_000);
            let at = block_time_ms(harness.height + 1);
            let seen_ms = i64::try_from(at).unwrap() + 200;
            let statuses = vec![btc_status(11, "A", "101.0", "1.0", "open", None, "Alo", at)];
            let diffs = vec![btc_diff(11, "A", "101.0", new_diff("1.0"))];
            if replica_first {
                harness.confirm(statuses, diffs);
                harness.priority_line(seen_ms, 1, 5, vec![alo_ask("101", "0x0a")]);
                assert_eq!(harness.available(), Err(Unavailable::BlockLogBehind));
                harness.log_placing(&[], vec![(11, 9)]);
            } else {
                harness.log_placing(&[], vec![(11, 9)]);
                harness.confirm(statuses, diffs);
                harness.priority_line(seen_ms, 1, 5, vec![alo_ask("101", "0x0a")]);
            }
            assert_eq!(harness.available(), Ok(()));
            let step = harness.line(seen_ms + 10, 2, vec![ioc("0x01")]);
            assert!(matches!(maker_of(&step), Maker::Confirmed { oid, .. } if *oid == Oid::new(11)), "replica first {replica_first}: {step:?}");
        }
    }

    /// The taker of the margin tests, and its outcome on BTC's bid side.
    const TAKER: Address = Address::repeat_byte(0x5a);

    fn taker_outcome(refused: bool) -> TakerOutcome {
        TakerOutcome { key: TakerKey { identity: ActingIdentity::Signer(TAKER), asset: 0, is_buy: true }, refused }
    }

    fn gtc_bid(px: &str, cloid: &str) -> ActionKind {
        ActionKind::Order(vec![requested(0, true, px, RequestedTif::Gtc, Some(cloid))])
    }

    /// Batch 2g (design 000621 r3): a block's refusal applies once the replica confirms its height, by whichever input
    /// comes last - the log first, or the replica first and the log catching up -: at the confirmed height before it
    /// the taker's IOC takes alike, after it neither does.
    #[test]
    fn a_refusal_applies_at_its_confirmed_height_by_either_input() {
        for replica_first in [false, true] {
            let mut harness = Harness::ready_from(1_000);
            harness.rest_ask(11);
            let refusal_ms = block_time_ms(harness.height + 1);
            let seen_ms = i64::try_from(refusal_ms).unwrap() + 50;
            if replica_first {
                harness.confirm(vec![], vec![]);
                let step = harness.signed_line(seen_ms, 1, 0, Some(TAKER), vec![ioc("0x01")]);
                assert_eq!((harness.available(), step.trades.len()), (Err(Unavailable::BlockLogBehind), 0));
                assert_eq!(harness.bbo().1, None, "replica first: taken as at the height before");
                harness.log_outcomes(vec![taker_outcome(true)], false);
            } else {
                harness.log_outcomes(vec![taker_outcome(true)], false);
                let step = harness.signed_line(seen_ms, 1, 0, Some(TAKER), vec![ioc("0x01")]);
                assert_eq!(step.trades.len(), 1, "log first: the refusal waits for its height");
                harness.confirm(vec![], vec![]);
            }
            assert_eq!(harness.available(), Ok(()));
            assert_eq!(harness.bbo().1, Some(level("101", "1", 1)), "replica first {replica_first}: refused once confirmed");
            let step = harness.signed_line(seen_ms + 10, 2, 0, Some(TAKER), vec![ioc("0x02")]);
            assert_eq!(step.trades, NO_TRADES, "replica first {replica_first}");
            assert_eq!(harness.signed_line(seen_ms + 20, 3, 0, None, vec![ioc("0x03")]).trades.len(), 1, "an unknown signer takes");
        }
    }

    /// A GTC not yet in a block, its book unchanged, goes with a refusal of its taker at or before its line, comes back
    /// with a clear and goes again with a later refusal still at or before its line; a refusal after its line does not
    /// reach it, nor a line before the refusal's block.
    #[test]
    fn a_held_order_follows_its_takers_refusals() {
        let mut harness = Harness::ready_from(1_000);
        let first_ms = block_time_ms(harness.height + 1);
        let seen_ms = i64::try_from(first_ms).unwrap() + 350;
        harness.signed_line(seen_ms, 1, 0, Some(TAKER), vec![gtc_bid("100", "0x01")]);
        let bid = Some(level("100", "1", 1));
        assert_eq!(harness.bbo().0, bid);
        let changed = |step: Step| step.changed.contains(&Coin::new(BTC));
        harness.log_outcomes(vec![taker_outcome(true)], false);
        assert!(changed(harness.confirm(vec![], vec![])));
        assert_eq!(harness.bbo().0, None, "refused at its line's block time minus 350 ms");
        harness.log_outcomes(vec![taker_outcome(false)], false);
        assert!(changed(harness.confirm(vec![], vec![])));
        assert_eq!(harness.bbo().0, bid, "cleared");
        harness.log_outcomes(vec![taker_outcome(true)], false);
        harness.confirm(vec![], vec![]);
        assert_eq!(harness.bbo().0, None, "refused again, 150 ms before its line");
        harness.block(&[], vec![], vec![]);
        harness.log_outcomes(vec![taker_outcome(false), taker_outcome(true)], false);
        harness.confirm(vec![], vec![]);
        assert_eq!(harness.bbo().0, bid, "a refusal 50 ms after its line does not reach it");
        harness.signed_line(seen_ms - 10, 2, 0, Some(TAKER), vec![gtc_bid("99", "0x02")]);
        assert_eq!(harness.bbo().0, bid, "a line before the refusal's block is not refused either");
        assert_eq!(harness.shadow.held(BTC).len(), 2);
    }

    /// Review 000621 r2: a refusal is kept while a held order may fall within it - its line no more than E before the
    /// confirmed block time -, past `t_r + M` too: the IOC seen 50 ms before the end of M, replayed as the book changes
    /// after M, is still refused.
    #[test]
    fn a_refusal_stays_while_an_order_it_reaches_is_held() {
        let mut harness = Harness::ready_with(PendingParams::from_ms(1_000, 1_000, 10_000, 1_000, 60_000, 1_000, 1_000).unwrap(), 1_000);
        harness.rest_ask(11);
        harness.log_outcomes(vec![taker_outcome(true)], false);
        harness.confirm(vec![], vec![]);
        let refused_ms = block_time_ms(harness.height);
        harness.signed_line(i64::try_from(refused_ms).unwrap() + 950, 1, 0, Some(TAKER), vec![ioc("0x01")]);
        harness.idle_until(refused_ms + 1_100);
        harness.rest_ask(12);
        assert_eq!(harness.shadow.held(BTC).len(), 1);
        assert_eq!(harness.bbo().1, Some(level("101", "2", 2)));
    }

    /// Review 000621 r2: a block log gap ends the refusals - one applied before it, and one a block queued before the
    /// loss's report brings (`after_loss`) -, the block of the taker's next fill lost: the taker's first new order once
    /// served is not refused. Without the gap the refusal stands.
    #[test]
    fn a_refusal_before_a_block_log_gap_does_not_survive_it() {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        enum Loss {
            None,
            ReportedBeforeTheRefusal,
            AfterTheRefusal,
        }
        for loss in [Loss::None, Loss::ReportedBeforeTheRefusal, Loss::AfterTheRefusal] {
            let mut harness = Harness::ready_from(1_000);
            harness.rest_ask(11);
            if loss == Loss::ReportedBeforeTheRefusal {
                drop(harness.shadow.on_block_log_loss(Some(&harness.replica)));
            }
            harness.log_outcomes(vec![taker_outcome(true)], loss == Loss::ReportedBeforeTheRefusal);
            harness.confirm(vec![], vec![]);
            if loss != Loss::None {
                if loss == Loss::AfterTheRefusal {
                    drop(harness.shadow.on_block_log_loss(Some(&harness.replica)));
                }
                // the next block's, a fill of the taker clearing it, lost
                harness.logged += 1;
                harness.confirm(vec![], vec![]);
            }
            let served_ms = block_time_ms(harness.height) + 3_000;
            harness.idle_until(served_ms);
            assert_eq!(harness.available(), Ok(()));
            let step = harness.signed_line(i64::try_from(served_ms).unwrap() + 10, 1, 0, Some(TAKER), vec![ioc("0x01")]);
            assert_eq!(step.trades.len(), usize::from(loss != Loss::None), "{loss:?}");
        }
    }

    /// A TWAP buying 1.1 BTC over 5 minutes from 2.5 s: slot 0 due at 3.5 s, 0.1 BTC.
    pub(crate) fn twap_anchor(height: u64) -> std::sync::Arc<crate::accounts::book::PendingAnchor> {
        let twap = crate::accounts::twap_tracker::CheckpointTwap {
            id: 7,
            user: TAKER,
            asset: 0,
            is_buy: true,
            total: 110_000,
            executed: 0,
            duration_s: 300,
            start_ms: 2_500,
            next_due_ms: 3_500,
            slot_count: 1,
            randomize: false,
            reduce_only: false,
            limit_px_raw: None,
        };
        std::sync::Arc::new(crate::accounts::book::PendingAnchor { height, coins: coins(), twaps: vec![twap], waiting_twaps: Vec::new() })
    }

    fn twap_trades(step: &Step) -> Vec<(u64, u64, Sz, Px)> {
        step.trades
            .iter()
            .filter_map(|trade| match trade.taker {
                Taker::Twap { twap_id, slot, .. } => Some((twap_id, slot, trade.sz, trade.px)),
                Taker::Pending { .. } => None,
            })
            .collect()
    }

    /// Batch 2h: a TWAP suborder due in the next first-of-second block takes before the pending takers - at its block's
    /// start -, its fill out once when it first fills; a block of its second whose next is not proven yet keeps it
    /// (batch 2i), and its being proven sends nothing again.
    #[test]
    fn a_twap_suborder_takes_first_and_trades_once() {
        let mut harness = Harness::ready_from(1_000);
        harness.rest_ask(11);
        drop(harness.shadow.on_anchor(twap_anchor(harness.height), Some(&harness.replica)));
        assert!(!harness.shadow.twaps_known(), "the anchor's height not proven complete");
        let step = harness.twaps_known_now();
        assert!(harness.shadow.twaps_known());
        let tenth = Sz::parse_from_str("0.1").unwrap();
        assert_eq!(twap_trades(&step), [(7, 0, tenth, Px::parse_from_str("101").unwrap())]);
        assert_eq!(harness.bbo().1, Some(level("101", "0.9", 1)));
        let step = harness.line(3_150, 1, vec![ioc("0x01")]);
        assert_eq!(step.trades.iter().map(|trade| trade.sz).collect::<Vec<_>>(), [Sz::parse_from_str("0.9").unwrap()], "behind the suborder");
        let step = harness.confirm(vec![], vec![]);
        assert!(harness.shadow.twaps_known(), "kept through its second");
        assert_eq!(twap_trades(&step), []);
        let step = harness.twaps_known_now();
        assert!(harness.shadow.twaps_known());
        assert_eq!(twap_trades(&step), [], "slot 0 went out already");
    }

    /// Batch 2h: the TWAPs turning known replay the coin with the suborder before the takers held.
    #[test]
    fn a_suborder_replays_before_the_takers_held() {
        let mut harness = Harness::ready_from(1_000);
        harness.rest_ask(11);
        drop(harness.shadow.on_anchor(twap_anchor(harness.height), Some(&harness.replica)));
        let step = harness.line(3_150, 1, vec![ioc("0x01")]);
        assert_eq!(step.trades.iter().map(|trade| trade.sz).collect::<Vec<_>>(), [Sz::parse_from_str("1").unwrap()]);
        let step = harness.twaps_known_now();
        assert_eq!(twap_trades(&step), [(7, 0, Sz::parse_from_str("0.1").unwrap(), Px::parse_from_str("101").unwrap())]);
    }

    /// Batch 2i (design 000631 r2): the next height not proven yet, the suborders of the second stand - no replay, the
    /// TWAPs known -; into the next second unproven they go, their block having run them - the coin replayed -, and
    /// that second's come once proven; a TWAP stream's loss within a second, or the block log's, withdraws them at once.
    #[test]
    fn a_suborder_stays_through_its_second() {
        let mut harness = Harness::ready_from(1_000);
        harness.rest_ask(11);
        drop(harness.shadow.on_anchor(twap_anchor(harness.height), Some(&harness.replica)));
        drop(harness.twaps_known_now());
        assert_eq!(harness.bbo().1, Some(level("101", "0.9", 1)));
        let step = harness.confirm(vec![], vec![]);
        assert!(!step.changed.contains(&Coin::new(BTC)));
        assert!(harness.shadow.twaps_known());
        assert_eq!(harness.bbo().1, Some(level("101", "0.9", 1)));
        let mut step = Step::default();
        while block_time_ms(harness.height) < 4_000 {
            step = harness.block(&[], vec![], vec![]);
            assert_eq!(harness.bbo().1.map(|level| level.1), Some(Sz::parse_from_str(if block_time_ms(harness.height) < 4_000 { "0.9" } else { "1" }).unwrap()));
        }
        assert!(step.changed.contains(&Coin::new(BTC)), "withdrawn into second 4");
        assert!(!harness.shadow.twaps_known());
        drop(harness.twaps_known_now());
        assert!(harness.shadow.twaps_known(), "second 4 proven: slot 1 due past it, none overlaid");
        assert_eq!(harness.bbo().1, Some(level("101", "1", 1)));
        drop(harness.shadow.on_block_log_loss(Some(&harness.replica)));
        assert!(!harness.shadow.twaps_known());
        let mut harness = Harness::ready_from(1_000);
        harness.rest_ask(11);
        drop(harness.shadow.on_anchor(twap_anchor(harness.height), Some(&harness.replica)));
        drop(harness.twaps_known_now());
        let step = harness.shadow.on_twap_loss(wall_ms(), Some(&harness.replica));
        assert!(step.changed.contains(&Coin::new(BTC)));
        assert!(!harness.shadow.twaps_known());
        assert_eq!(harness.bbo().1, Some(level("101", "1", 1)));
    }

    /// Batch 2i (design 000631 r2): with no suborder due, a block of the second proven later sends nothing at all - no
    /// event a subscription would be told -; into the next second the state change alone goes out, no frame, and back.
    #[test]
    fn the_twaps_state_changes_alone_by_the_second() {
        let mut harness = Harness::ready_from(1_000);
        harness.rest_ask(11);
        let mut anchor = twap_anchor(harness.height);
        std::sync::Arc::make_mut(&mut anchor).twaps.clear();
        drop(harness.shadow.on_anchor(anchor, Some(&harness.replica)));
        assert!(harness.twaps_known_now().changed.is_empty());
        assert!(harness.shadow.twaps_known());
        let handle = crate::pending::publish::PendingBookHandle::new();
        let mut events = handle.subscribe();
        let mut publisher = crate::pending::publish::Publisher::new(handle.clone());
        publisher.publish(&Step::default(), true, &harness.shadow, Some(&harness.replica));
        while events.try_recv().is_ok() {}
        let told = |event: Result<crate::pending::publish::PendingEvent, tokio::sync::broadcast::error::TryRecvError>| match event {
            Ok(crate::pending::publish::PendingEvent::Availability(state)) => (state.availability(), state.twaps_known()),
            other => panic!("{other:?}"),
        };
        while block_time_ms(harness.height + 1) < 4_000 {
            let step = harness.block(&[], vec![], vec![]);
            publisher.publish(&step, false, &harness.shadow, Some(&harness.replica));
            assert!(events.try_recv().is_err(), "nothing within the second, at {}", block_time_ms(harness.height));
        }
        let into_next = harness.block(&[], vec![], vec![]);
        assert!(into_next.changed.is_empty());
        publisher.publish(&into_next, false, &harness.shadow, Some(&harness.replica));
        assert_eq!(told(events.try_recv()), (Ok(()), false));
        assert!(events.try_recv().is_err(), "no frame");
        assert!(!handle.snapshot().twaps_known());
        let known = harness.twaps_known_now();
        assert!(known.changed.is_empty());
        publisher.publish(&known, false, &harness.shadow, Some(&harness.replica));
        assert_eq!(told(events.try_recv()), (Ok(()), true));
        assert!(events.try_recv().is_err());
        assert!(handle.snapshot().twaps_known());
    }

    fn params_window(window_ms: u64) -> PendingParams {
        PendingParams::from_ms(1_000, 1_000, 10_000, 1_000, 60_000, 300_000, window_ms).unwrap()
    }

    /// Batch 2i: W 400 ms - the suborder overlaid from 3.1 s, its trade goes out once the confirmed block time reaches
    /// 3.6 s (the window's edge), its second's blocks between proven or not, and once.
    #[test]
    fn a_suborder_trades_in_the_window_alone() {
        let mut harness = Harness::ready_with(params_window(400), 1_000);
        harness.rest_ask(11);
        drop(harness.shadow.on_anchor(twap_anchor(harness.height), Some(&harness.replica)));
        assert_eq!(twap_trades(&harness.twaps_known_now()), []);
        assert_eq!(harness.bbo().1, Some(level("101", "0.9", 1)), "overlaid out of the window");
        let mut traded_at = Vec::new();
        while block_time_ms(harness.height) < 3_900 {
            let step = harness.block(&[], vec![], vec![]);
            if !twap_trades(&step).is_empty() {
                traded_at.push(block_time_ms(harness.height));
            }
        }
        assert_eq!(traded_at, [3_600]);
    }

    /// Review 000631 r1 P2-2: the window replays a coin at most once a confirmed height - two read-through proofs at it
    /// replay nothing, the suborder taking nothing (no ask) -; the next height's ask, its book changed, takes it, the
    /// trade out once. Entered while the book may not be served, the window does not spend the height.
    #[test]
    fn the_window_replays_a_coin_once_a_height() {
        let mut harness = Harness::ready_with(params_window(400), 1_000);
        drop(harness.shadow.on_anchor(twap_anchor(harness.height), Some(&harness.replica)));
        drop(harness.twaps_known_now());
        let mut step = Step::default();
        while block_time_ms(harness.height) < 3_600 {
            step = harness.block(&[], vec![], vec![]);
        }
        assert!(step.changed.contains(&Coin::new(BTC)), "replayed entering the window");
        assert_eq!(twap_trades(&step), []);
        assert!(harness.prove_twaps().changed.is_empty());
        assert!(harness.prove_twaps().changed.is_empty());
        let at = block_time_ms(harness.height + 1);
        let step = harness.block(&[], vec![btc_status(11, "A", "101.0", "1.0", "open", None, "Alo", at)], vec![btc_diff(11, "A", "101.0", new_diff("1.0"))]);
        assert_eq!(twap_trades(&step), [(7, 0, Sz::parse_from_str("0.1").unwrap(), Px::parse_from_str("101").unwrap())], "the ask's block");
        let traded: Vec<_> = std::iter::repeat_with(|| harness.block(&[], vec![], vec![])).take(2).flat_map(|step| twap_trades(&step)).collect();
        assert_eq!(traded, [], "once");
    }

    /// Review 000631 r3 P3: the window's edge, and a W past a second taking every block time, `u64::MAX` too.
    #[test]
    fn the_trade_window_takes_its_edge_and_any_wide_w() {
        let edge = Shadow::new(params_window(400), true, 0);
        assert_eq!([3_599, 3_600, 3_999, 4_000].map(|basis_ms| edge.in_trade_window(basis_ms)), [false, true, true, false]);
        for window_ms in [1_000, u64::MAX] {
            let wide = Shadow::new(params_window(window_ms), true, 0);
            assert!([3_000, 3_600, 3_999, u64::MAX].iter().all(|basis_ms| wide.in_trade_window(*basis_ms)), "W {window_ms}");
        }
    }

    /// Review 000631 r1 P2-2: the window entered while the book may not be served keeps the height for when it may.
    #[test]
    fn the_window_waits_for_the_book_served() {
        let mut harness = Harness::ready_with(params_window(400), 1_000);
        drop(harness.shadow.on_anchor(twap_anchor(harness.height), Some(&harness.replica)));
        drop(harness.twaps_known_now());
        while block_time_ms(harness.height) < 3_500 {
            drop(harness.block(&[], vec![], vec![]));
        }
        let (height, basis_ms) = (harness.height, block_time_ms(harness.height) + 100);
        assert!(harness.shadow.trade_window_coins(height, basis_ms, false).is_empty());
        assert_eq!(harness.shadow.trade_window_coins(height, basis_ms, true), std::iter::once(Coin::new(BTC)).collect());
        assert!(harness.shadow.trade_window_coins(height, basis_ms, true).is_empty(), "once a height");
    }

    /// Review 000620 r1 P2-2: H under the ALO priority window is refused at start.
    #[test]
    fn a_max_late_under_the_priority_window_is_refused() {
        assert!(PendingParams::from_ms(1_000, 399, 10_000, 1_000, 60_000, 300_000, 1_000).is_err());
        assert!(PendingParams::from_ms(1_000, 400, 10_000, 1_000, 60_000, 300_000, 1_000).is_ok());
    }

    /// R7 revision 20: past `MAX_LEAD_BLOCKS` above the confirmed height (all of them without a replica) the log's
    /// blocks are not taken.
    #[test]
    fn the_block_log_lead_is_capped() {
        let mut shadow = Shadow::new(params(), true, 0);
        for height in 1..=MAX_LEAD_BLOCKS {
            assert!(shadow.takes_blocks(None));
            shadow.on_block(LoggedBlock { height, time_ms: height * 100, hashes: Vec::new(), priorities: Vec::new(), taker_outcomes: Vec::new(), twap_cancels: Vec::new(), read_at: std::time::Instant::now() }, false, None);
        }
        assert!(!shadow.takes_blocks(None));
    }
}
