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

use std::{collections::HashSet, sync::Arc};

use alloy::primitives::{Address, B256};
use rustc_hash::{FxHashMap, FxHashSet};

use super::{
    actions::{ActionId, BookActions, BookOp, PendingAction, Placement, band_orders, book_actions},
    blocks::LoggedBlock,
    mempool::MempoolTx,
    overlay::{Basis, BestLevel, CoinBase, Effect, Fill, Maker, Overlay, PRIORITY_WINDOW_MS, TriggerFills, below_price_band},
    publish::{BestLevels, CoinPending},
    record::{BlockRecord, MAX_LEAD_BLOCKS},
    refusals::{TakerKey, TakerRefusals},
    signer_accounts::SignerAccounts,
    trailing::{CoinTrailing, TrailingBasis},
    triggers::{SetGlobalCoin, TriggerSource},
    twap_streams::{TwapEvent, TwapFill},
    twaps::{DueSlices, TwapBook, TwapSlice, TwapStream, TwapsUnknown},
};
use crate::{
    accounts::book::ActionCoins,
    listeners::order_book::replica::BookReplica,
    metrics::{
        PENDING_BOOK_ACTIONS_TOTAL, PENDING_BOOK_BLOCK_LOG_LEAD_BLOCKS, PENDING_BOOK_GAPS_TOTAL, PENDING_BOOK_HELD_ACTIONS,
        PENDING_BOOK_MOVED_COINS, PENDING_BOOK_OVERLAY_TOTAL, PENDING_BOOK_PREDICTED_FILLS_TOTAL, PENDING_BOOK_SIGNER_ACCOUNTS,
        PENDING_BOOK_SPAN_SECONDS, PENDING_BOOK_TRAILING_READS_TOTAL, PENDING_BOOK_TRIGGER_COINS_TOTAL, PENDING_BOOK_TRIGGERS_TOTAL,
    },
    order_book::{Coin, Px, Side, Sz},
};

const NS_PER_MS: i64 = 1_000_000;

/// A second's first block runs its TWAP suborders after the actions of the mempool lines seen before the second's start
/// minus this (batch 2j, design 000634 r2; research `mempool_pending_book_20261031`, bm 11-01 12:10-12:20: of the lines
/// seen 0-19 ms before a second 29.9% ran before its first block, 20-39 ms 60.0%, 60-79 ms 86.1%, 80 ms or more 98.9-100%).
const SLICE_CUT_MS: u64 = 20;

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
    /// A TWAP's suborder of `slot` (batch 2h), `user` the TWAP's; due at `due_ms` (unix ms): its place among the
    /// suborders a block runs (review 000662 r6 P2).
    Twap { twap_id: u64, user: Address, slot: u64, due_ms: u64 },
    /// A trigger order a `SetGlobalAction` fired (batch 2l; two sources since 2m): `set_global` the action on the coin
    /// (review 000662 r4 P2-3: two lines may share a `local_time`), `seen_ns` its line's `local_time`.
    Trigger { source: TriggerSource, set_global: ActionId, seen_ns: i64 },
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

/// One coin as the pending book serves it: the merged book's best levels, the replica's, and what the set holds; or why
/// its pending book is not decided (batch 2n-2: a trailing stop not known a `SetGlobalAction` may fire).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CoinView {
    pub(crate) bbo: BestLevels,
    pub(crate) basis_bbo: BestLevels,
    pub(crate) pending: CoinPending,
    pub(crate) undecided: Option<&'static str>,
}

/// A coin's held actions in execution order, and their overlay.
#[derive(Default)]
struct CoinActions {
    actions: Vec<PendingAction>,
    overlay: Overlay,
}

/// A line's trades as its coins are held: its actions', each with its place in the line, and the TWAP suborders' their
/// replays sent.
#[derive(Default)]
struct LineTrades {
    actions: Vec<(usize, PendingTrade)>,
    suborders: Vec<PendingTrade>,
}

/// A replay's results: each TWAP suborder's fills, and the effects of the actions asked for, by their index.
struct Replayed {
    slices: Vec<Vec<Fill>>,
    reported: Vec<(usize, Effect)>,
}

/// The TWAP suborders' slots whose trades went out once (batch 2i), each with its due time - kept until the slot is
/// past, unknown spells between -, and those the step under way computed: a run of the step computes none of them
/// again; its commit tells those whose trades went out and leaves the others to a later run (review 000662 r3 P2-2, r4
/// P2-2: trades withdrawn as the trailing stops are read anew, or no trades frame as the replica went).
#[derive(Default)]
struct SuborderSlots {
    told: FxHashMap<(u64, u64), u64>,
    computed: FxHashMap<(u64, u64), u64>,
}

impl SuborderSlots {
    /// Whether `slice`'s trades are computed now - neither told nor computed in the step -; so from now on.
    fn compute(&mut self, slice: &TwapSlice) -> bool {
        let awaited = self.awaited(slice);
        if awaited {
            self.computed.insert((slice.twap_id, slice.slot), slice.due_ms);
        }
        awaited
    }

    /// Whether `slice`'s trades are still to come: neither told nor computed in the step.
    fn awaited(&self, slice: &TwapSlice) -> bool {
        let key = (slice.twap_id, slice.slot);
        !self.told.contains_key(&key) && !self.computed.contains_key(&key)
    }

    /// The step committed, `told` its trades that went out: the slots computed of them told, the others forgotten.
    fn commit(&mut self, told: &[PendingTrade]) {
        let computed = std::mem::take(&mut self.computed);
        let slots = told.iter().filter_map(|trade| match trade.taker {
            Taker::Twap { twap_id, slot, .. } => Some((twap_id, slot)),
            Taker::Pending { .. } | Taker::Trigger { .. } => None,
        });
        self.told.extend(slots.filter_map(|key| computed.get(&key).map(|due_ms| (key, *due_ms))));
    }

    /// The told slots due before `first_of_second_ms` forgotten: past.
    fn forget_before(&mut self, first_of_second_ms: u64) {
        self.told.retain(|_, due_ms| *due_ms >= first_of_second_ms);
    }
}

/// A run on a fresh overlay: the overlay it leaves, each known action's effect by its index, each suborder's fills.
struct Run {
    overlay: Overlay,
    effects: Vec<(usize, Effect)>,
    slices: Vec<Vec<Fill>>,
}

impl CoinActions {
    /// The overlay anew (`run`): each suborder's fills, and the effects of the actions from `report_from` on.
    fn replay(&mut self, base: CoinBase<'_>, basis: Basis<'_>, slices: &[TwapSlice], cut_ns: i64, report_from: usize) -> Replayed {
        let Run { overlay, mut effects, slices } = self.run(base, basis, slices, cut_ns);
        self.overlay = overlay;
        self.keep_stale(&effects);
        effects.retain(|(index, _)| *index >= report_from);
        effects.sort_unstable_by_key(|(index, _)| *index);
        Replayed { slices, reported: effects }
    }

    /// The overlay anew in a block's order: the known actions as they came - with TWAP suborders due, those of the lines
    /// seen before `cut_ns` first, then the suborders the next first-of-second block runs at its start, then the others
    /// (batch 2j; 2h ran the suborders first; with none, no action moves: review 000634 r3 P2-1).
    fn run(&self, base: CoinBase<'_>, basis: Basis<'_>, slices: &[TwapSlice], cut_ns: i64) -> Run {
        let mut overlay = Overlay::default();
        if slices.is_empty() {
            let effects = self.apply_where(&mut overlay, base, basis, |_| true);
            return Run { overlay, effects, slices: Vec::new() };
        }
        let mut effects = self.apply_where(&mut overlay, base, basis, |action| action.seen_ns < cut_ns);
        let filled = slices.iter().map(|slice| overlay.take_slice(base, slice)).collect();
        effects.extend(self.apply_where(&mut overlay, base, basis, |action| action.seen_ns >= cut_ns));
        Run { overlay, effects, slices: filled }
    }

    /// Every trade a run anew yields - each known action's, each suborder's -, told or not: what a step's trades are
    /// judged by when the trailing stops turn before it is told (review 000662 r3 P2-1).
    fn yielded(&self, coin: &Coin, base: CoinBase<'_>, basis: Basis<'_>, slices: &[TwapSlice], cut_ns: i64) -> Vec<PendingTrade> {
        let Run { effects, slices: filled, .. } = self.run(base, basis, slices, cut_ns);
        let actions = effects.into_iter().flat_map(|(index, effect)| effect_trades(coin, &self.actions[index], effect));
        let suborders = slices.iter().zip(filled).flat_map(|(slice, fills)| fills.into_iter().map(|fill| twap_trade(coin, slice, fill)));
        actions.chain(suborders).collect()
    }

    /// The accounts each `SetGlobal` applied found stale, kept on it until its line leaves (design 000652 r4, r5): a
    /// replay without the fill that moved them does not trust their frozen positions again.
    fn keep_stale(&mut self, effects: &[(usize, Effect)]) {
        for (index, effect) in effects.iter().filter(|(_, effect)| !effect.stale.is_empty()) {
            if let BookOp::SetGlobal(sga) = &mut self.actions[*index].op {
                sga.stale.extend(effect.stale.iter().copied());
            }
        }
    }

    /// The overlay as the known actions seen before `cut_ns` leave it, before the TWAP suborders due: the first part of
    /// `replay` (batch 2l, review 000642 r1 P2-2).
    fn overlay_before_cut(&self, base: CoinBase<'_>, basis: Basis<'_>, cut_ns: i64) -> Overlay {
        let mut overlay = Overlay::default();
        drop(self.apply_where(&mut overlay, base, basis, |action| action.seen_ns < cut_ns));
        overlay
    }

    /// The known actions `takes` admits applied on `overlay` in their order, each one's effect by its index.
    fn apply_where(&self, overlay: &mut Overlay, base: CoinBase<'_>, basis: Basis<'_>, takes: impl Fn(&PendingAction) -> bool) -> Vec<(usize, Effect)> {
        let known = self.actions.iter().enumerate().filter(|(_, action)| !action.unknown && takes(action));
        known.map(|(index, action)| (index, overlay.apply(base, basis, action))).collect()
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
    /// The signers' accounts the block log taught (batch 2m-2): an action without a vault acts for its signer's.
    signer_accounts: SignerAccounts,
    /// The running TWAPs (batch 2h); the suborders overlaid, as of the book's version and the confirmed block time's
    /// second they were known at - kept through that second while the next height is not proven, `None` while none are
    /// (batch 2i) -; the suborders' slots computed and told; the confirmed height the trade window last replayed its
    /// coins at (review 000631 r1 P2-2).
    twaps: TwapBook,
    due: DueSlices,
    due_of: Option<(u64, u64)>,
    slots: SuborderSlots,
    window_height: Option<u64>,
    /// The signed actions held with no replica aligned to judge their price band by, each with the lowest limit of its
    /// orders the band judges on each coin: judged once one is, before any overlay applies them (batch 2k, review 000637
    /// r2, r3); entered once held, dropped with the set; their (coin, limit) slots, counted against its cap with the
    /// actions held (r4).
    band_unjudged: FxHashMap<(B256, usize), Vec<(Coin, Px)>>,
    band_unjudged_slots: usize,
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
    /// The account copy's trailing stops as last read (batch 2n-2), and each held coin's as its runs stand on them: read
    /// again as the copy or the replica moves, a coin whose stops read otherwise replayed; the coins a read replayed in
    /// the step under way, their suborders' trades held back to its commit (`tell_held_back`).
    trailing_basis: TrailingBasis,
    trailing: FxHashMap<Coin, CoinTrailing>,
    held_back: HashSet<Coin>,
    /// Settling steps taken with a replica aligned.
    #[cfg(test)]
    settles: usize,
}

/// A coin holding no trailing stop, or none read.
static NO_TRAILING: CoinTrailing = CoinTrailing { size_decimals: None, stops: Vec::new() };

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
            signer_accounts: SignerAccounts::default(),
            twaps: TwapBook::new(started_ms),
            due: DueSlices::default(),
            due_of: None,
            slots: SuborderSlots::default(),
            window_height: None,
            band_unjudged: FxHashMap::default(),
            band_unjudged_slots: 0,
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
            trailing_basis: TrailingBasis::default(),
            trailing: FxHashMap::default(),
            held_back: HashSet::new(),
            #[cfg(test)]
            settles: 0,
        }
    }

    /// The account copy's trailing stops as they now serve (batch 2n-2): the held coins whose stops read otherwise
    /// replayed - a best moved with no book event, the copy turned unavailable or back (design 000659 r2 s3). Their
    /// suborders' trades held back to the step's commit, as its input may move them (review 000662 r5 P2).
    pub(crate) fn on_trailing(&mut self, basis: TrailingBasis, replica: Option<&BookReplica>) -> Step {
        if basis == self.trailing_basis {
            return Step::default();
        }
        self.trailing_basis = basis;
        // a new generation is replayed whole as it settles
        let Some((replica, height, _)) = aligned(replica).filter(|_| !self.gaps.replica) else { return Step::default() };
        let changed: HashSet<Coin> = self.read_trailing(replica, height).into_iter().collect();
        self.replay_coins(replica, &changed, false, &mut Vec::new());
        self.held_back.extend(changed.iter().cloned());
        Step { trades: Vec::new(), changed }
    }

    /// The suborders' trades of the coins a trailing read replayed in the step, as its final runs fill them (review
    /// 000662 r5 P2): while the book may be served in the trade window, each slot neither told nor computed in the step,
    /// into the step's `trades` (`order_trades` places them).
    pub(crate) fn tell_held_back(&mut self, trades: &mut Vec<PendingTrade>, replica: Option<&BookReplica>) {
        let held_back = std::mem::take(&mut self.held_back);
        let Some(replica) = replica.filter(|replica| self.availability(Some(replica)).is_ok() && self.in_trade_window(self.basis_ms)) else { return };
        let cut_ns = slice_cut_ns(self.basis_ms);
        let Self { coins, due, slots, record, refusals, basis_ms, trailing, .. } = self;
        let basis = Basis::new(record.priorities(), refusals, *basis_ms);
        for coin in held_back {
            let (Some(actions), Some(slices)) = (coins.get(&coin), due.get(&coin)) else { continue };
            let base = replica.coin(&coin).with_trailing(trailing.get(&coin).unwrap_or(&NO_TRAILING));
            slice_trades(&coin, slices, actions.run(base, basis, slices, cut_ns).slices, true, slots, trades);
        }
    }

    /// A step's `trades` in the order its runs take them (review 000662 r6 P2): the actions seen before the cut as they
    /// came, the TWAP suborders by due time and id, the actions at or after the cut as they came - each one's fills as
    /// they came. A step of no deferred suborder is so already.
    pub(crate) fn order_trades(&self, trades: &mut [PendingTrade]) {
        let cut_ns = slice_cut_ns(self.basis_ms);
        trades.sort_by_key(|trade| run_order(trade, cut_ns));
    }

    /// Each held coin's trailing stops read at the replica's `height`; those reading otherwise than its runs stood on.
    fn read_trailing(&mut self, replica: &BookReplica, height: u64) -> Vec<Coin> {
        let Self { coins, trailing, trailing_basis, record, .. } = self;
        trailing.retain(|coin, _| coins.contains_key(coin));
        let at = trailing_basis.at(record, height);
        PENDING_BOOK_TRAILING_READS_TOTAL.with_label_values(&[at.result()]).inc();
        let mut changed = Vec::new();
        for coin in coins.keys() {
            let read = at.coin(replica.trigger_table(), coin);
            if trailing.get(coin).unwrap_or(&NO_TRAILING) != &read {
                trailing.insert(coin.clone(), read);
                changed.push(coin.clone());
            }
        }
        changed
    }

    /// Of a step's `trades`, those of a taker on `coins` - their runs replayed on trailing stops read anew before the step
    /// is told - that the runs no longer yield alike withdrawn (review 000662 r3 P2-1): an action before its coin's first
    /// undecided `SetGlobalAction` goes out as computed, one after it does not. A withdrawn suborder's slot is not told
    /// with the step (`on_committed`). Every other trade stands; none told before is told again.
    pub(crate) fn retain_yielded(&self, trades: &mut Vec<PendingTrade>, coins: &HashSet<Coin>, replica: &BookReplica) {
        let judged: HashSet<&Coin> = trades.iter().map(|trade| &trade.coin).filter(|coin| coins.contains(*coin)).collect();
        let yielded: Vec<PendingTrade> = judged.into_iter().flat_map(|coin| self.yielded(coin, replica)).collect();
        let alike = |taker: &Taker| trades.iter().filter(|trade| trade.taker == *taker).eq(yielded.iter().filter(|trade| trade.taker == *taker));
        let withdrawn: Vec<Taker> = trades.iter().filter(|trade| coins.contains(&trade.coin) && !alike(&trade.taker)).map(|trade| trade.taker.clone()).collect();
        trades.retain(|trade| !withdrawn.contains(&trade.taker));
    }

    /// A step committed, `told` the trades that went out with it: their suborders' slots told, none of them again; the
    /// others it computed - withdrawn, or no trades frame went out - a later run computes again (review 000662 r4 P2-2).
    pub(crate) fn on_committed(&mut self, told: &[PendingTrade]) {
        self.slots.commit(told);
    }

    /// Every trade `coin`'s run yields anew on `replica`, told or not.
    fn yielded(&self, coin: &Coin, replica: &BookReplica) -> Vec<PendingTrade> {
        let Some(actions) = self.coins.get(coin) else { return Vec::new() };
        let slices = self.due.get(coin).map_or(&[][..], Vec::as_slice);
        let base = replica.coin(coin).with_trailing(self.trailing.get(coin).unwrap_or(&NO_TRAILING));
        let basis = Basis::new(self.record.priorities(), &self.refusals, self.basis_ms);
        actions.yielded(coin, base, basis, slices, slice_cut_ns(self.basis_ms))
    }

    /// `coin`'s trailing stops as its runs stand on them: a held coin's as last read, another's read now.
    fn coin_trailing(&self, coin: &Coin, replica: &BookReplica, height: u64) -> CoinTrailing {
        self.trailing.get(coin).cloned().unwrap_or_else(|| self.trailing_basis.at(&self.record, height).coin(replica.trigger_table(), coin))
    }

    /// A mempool line; `after_loss` when the watcher had found a loss not yet reported, so it may follow it. `replica`
    /// is the current one, aligned or not.
    pub(crate) fn on_tx(&mut self, tx: &MempoolTx, after_loss: bool, coins: Option<&ActionCoins>, replica: Option<&BookReplica>) -> Step {
        let step = Step::default();
        if self.lines.contains_key(&tx.tx_hash) {
            PENDING_BOOK_ACTIONS_TOTAL.with_label_values(&["line", "held_already"]).inc();
            return step;
        }
        let aligned = aligned(replica);
        self.mempool_gaps(tx.seen_ns, after_loss, coins.is_some(), aligned);
        self.newest_seen_ns = Some(self.newest_seen_ns.map_or(tx.seen_ns, |newest| newest.max(tx.seen_ns)));
        let Some(coins) = coins else { return step };
        let below_band = |coin: &str, limit_px: Px| aligned.is_some_and(|(replica, _, _)| below_price_band(replica.coin(&Coin::new(coin)), limit_px));
        let BookActions { held: actions, refused } = book_actions(tx, coins, self.spot, &below_band, &self.signer_accounts);
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
        // with no replica to judge them by, the price band's slots of its held signed actions wait with them (r4)
        let unjudged = if aligned.is_none() { unjudged_bands(tx, &actions, coins) } else { Vec::new() };
        let slots: usize = unjudged.iter().map(|(_, lowest)| lowest.len()).sum();
        // the cap judged before any of it applies: nothing of a line dropping the set trades (review 000611 r1)
        if self.over_cap(actions.len() + slots) {
            self.drop_over_cap(actions.len());
            return step;
        }
        self.band_unjudged_slots += slots;
        self.band_unjudged.extend(unjudged.into_iter().map(|(action, lowest)| ((tx.tx_hash, action), lowest)));
        self.hold_line(tx, actions, replica)
    }

    /// A `SetGlobalAction` line's coins, each with its forecast mark and frozen positions (batch 2m, design 000652 r2):
    /// held as the line's actions where it stands, one a coin in asset order, settled with its block as any line's.
    pub(crate) fn on_set_global(&mut self, tx: &MempoolTx, coins: Vec<(Coin, u32, SetGlobalCoin)>, replica: Option<&BookReplica>) -> Step {
        if coins.is_empty() || self.lines.contains_key(&tx.tx_hash) || self.not_held(tx, aligned(replica)).is_some() {
            return Step::default();
        }
        let actions: Vec<(Coin, PendingAction)> = coins
            .into_iter()
            .enumerate()
            .map(|(action, (coin, asset, sga))| {
                let id = ActionId { tx_hash: tx.tx_hash, action, item: 0 };
                let op = BookOp::SetGlobal(Box::new(sga));
                (coin, PendingAction { id, seen_ns: tx.seen_ns, vault: None, signer: None, account: None, asset, op, unknown: false })
            })
            .collect();
        if self.over_cap(actions.len()) {
            self.drop_over_cap(actions.len());
            return Step::default();
        }
        self.hold_line(tx, actions, replica)
    }

    /// The accounts, as known, of `coin`'s untriggered orders a mark at `mark_px` reaches where a line seen at `seen_ns`
    /// runs - the replica's and the pending ones -: whose positions a `SetGlobalAction` freezes (design 000652 r2, r5),
    /// on the same overlay as its mark (review 000652 r6 P2-3). None while no replica is aligned or the coin's orders are
    /// not all known (the action tells why as it applies).
    pub(crate) fn reached_accounts(&self, coin: &Coin, replica: Option<&BookReplica>, mark_px: Px, seen_ns: i64) -> Vec<Address> {
        let Some((replica, height, _)) = aligned(replica).filter(|_| !self.gaps.replica) else { return Vec::new() };
        let trailing = self.coin_trailing(coin, replica, height);
        let base = replica.coin(coin).with_trailing(&trailing);
        self.at_line(coin, base, seen_ns, |overlay| overlay.reached_accounts(base, mark_px))
    }

    /// Why `coin`'s run decides nothing of it where a line seen at `seen_ns` runs (batch 2n-2, design 000659 r4): a
    /// `SetGlobalAction` coming there freezes neither mark nor positions. `None` with none to judge on.
    pub(crate) fn undecided_at(&self, coin: &Coin, replica: Option<&BookReplica>, seen_ns: i64) -> Option<&'static str> {
        let (replica, height, _) = aligned(replica).filter(|_| !self.gaps.replica)?;
        let trailing = self.coin_trailing(coin, replica, height);
        let base = replica.coin(coin).with_trailing(&trailing);
        self.at_line(coin, base, seen_ns, Overlay::undecided)
    }

    /// Whether `arriving` more orders take the set past its cap, the price band's slots waiting counted (r4).
    const fn over_cap(&self, arriving: usize) -> bool {
        self.held + self.band_unjudged_slots + arriving > self.params.max_actions
    }

    /// A line's actions held, a coin at a time, each with its place in the line (review 000634 r1 P2-1): its coins
    /// changed, its trades in its order, then the suborders' its replays sent (r2).
    fn hold_line(&mut self, tx: &MempoolTx, actions: Vec<(Coin, PendingAction)>, replica: Option<&BookReplica>) -> Step {
        let aligned = aligned(replica);
        let emit = self.availability(replica).is_ok();
        let unknown = self.gaps.unknown_at(tx.seen_ns);
        let mut by_coin: Vec<(Coin, Vec<(usize, PendingAction)>)> = Vec::new();
        for (order, (coin, mut action)) in actions.into_iter().enumerate() {
            action.unknown = unknown;
            match by_coin.iter_mut().find(|(held, _)| *held == coin) {
                Some((_, coin_actions)) => coin_actions.push((order, action)),
                None => by_coin.push((coin, vec![(order, action)])),
            }
        }
        let line = HeldLine { seen_ns: tx.seen_ns, coins: by_coin.iter().map(|(coin, _)| coin.clone()).collect() };
        let mut step = Step::default();
        let mut trades = LineTrades::default();
        for (coin, coin_actions) in by_coin {
            // its frame's pending counts change even when its book does not
            step.changed.insert(coin.clone());
            self.hold(coin, coin_actions, tx.seen_ns, aligned, emit, &mut trades);
        }
        trades.actions.sort_by_key(|(order, _)| *order);
        step.trades.extend(trades.actions.into_iter().map(|(_, trade)| trade));
        step.trades.extend(trades.suborders);
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
        // what a block ran is so whatever gap the log is in: a signer's account taught by any block read, the expired
        // forgotten first (review 000656 r1 P2-1); what was learned of a signer forgotten or turned conflicting taken
        // back (r1 P2-2, r2 P2)
        let mut unlearned = FxHashSet::default();
        self.signer_accounts.evict(block.time_ms, &mut unlearned);
        for (signer, account) in std::mem::take(&mut block.signer_accounts) {
            if self.signer_accounts.learn(block.time_ms, signer, account) {
                unlearned.insert(signer);
            }
        }
        self.signer_accounts.evict(block.time_ms, &mut unlearned);
        let changed = self.unlearn(&unlearned);
        self.twaps.on_block(block.height, block.read_at, block.time_ms, &block.twap_cancels);
        self.record.add(block);
        self.settle(replica, changed)
    }

    /// What was learned of `signers` - forgotten, or turned out to act for two accounts - no longer stands: the actions
    /// held of them without a vault lose the account learned of them (review 000656 r1 P2-2, r2 P2). Their coins, to
    /// replay; what they predicted before stands.
    fn unlearn(&mut self, signers: &FxHashSet<Address>) -> HashSet<Coin> {
        let mut changed = HashSet::new();
        if signers.is_empty() {
            return changed;
        }
        for (coin, actions) in &mut self.coins {
            let learned_of = |action: &&mut PendingAction| action.vault.is_none() && action.signer.is_some_and(|signer| signers.contains(&signer));
            for action in actions.actions.iter_mut().filter(learned_of) {
                if action.account.take().is_some() {
                    changed.insert(coin.clone());
                }
            }
        }
        changed
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
    pub(crate) fn on_anchor(&mut self, anchor: Arc<crate::accounts::book::PendingAnchor>, replica: Option<&BookReplica>) -> Step {
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
            self.slots.forget_before(first_of_second_ms);
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
        let slots = &self.slots;
        self.due
            .iter()
            .filter(|(_, slices)| slices.iter().any(|slice| slots.awaited(slice)))
            .map(|(coin, _)| coin.clone())
            .collect()
    }

    /// `coins` replayed over the replica, each one's TWAP suborders first; a suborder's fills go out once, the first time
    /// it fills while `emit` in the trade window.
    fn replay_coins(&mut self, replica: &BookReplica, coins: &HashSet<Coin>, emit: bool, trades: &mut Vec<PendingTrade>) {
        let emit_twaps = emit && self.in_trade_window(self.basis_ms);
        let cut_ns = slice_cut_ns(self.basis_ms);
        let Self { coins: held, due, slots, record, refusals, basis_ms, trailing, .. } = self;
        let basis = Basis::new(record.priorities(), refusals, *basis_ms);
        for coin in coins {
            let Some(actions) = held.get_mut(coin) else { continue };
            let slices = due.get(coin).map_or(&[][..], Vec::as_slice);
            let base = replica.coin(coin).with_trailing(trailing.get(coin).unwrap_or(&NO_TRAILING));
            let replayed = actions.replay(base, basis, slices, cut_ns, actions.actions.len());
            slice_trades(coin, slices, replayed.slices, emit_twaps, slots, trades);
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
        let (known, conflicting) = self.signer_accounts.counts();
        PENDING_BOOK_SIGNER_ACCOUNTS.with_label_values(&["known"]).set(gauge(known));
        PENDING_BOOK_SIGNER_ACCOUNTS.with_label_values(&["conflicting"]).set(gauge(conflicting));
    }

    /// The `SetGlobalAction`s held on `coin`, in order.
    #[cfg(test)]
    pub(crate) fn held_set_globals(&self, coin: &Coin) -> Vec<SetGlobalCoin> {
        let actions = self.coins.get(coin).map_or(&[][..], |actions| actions.actions.as_slice());
        actions.iter().filter_map(|action| if let BookOp::SetGlobal(sga) = &action.op { Some(SetGlobalCoin::clone(sga)) } else { None }).collect()
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
        let (bbo, basis_bbo) = self.best_levels(coin, replica);
        let undecided = self.coins.get(coin).and_then(|actions| actions.overlay.undecided());
        CoinView { bbo, basis_bbo, pending, undecided }
    }

    /// `coin`'s merged and its replica's best levels.
    pub(crate) fn best_levels(&self, coin: &Coin, replica: &BookReplica) -> (BestLevels, BestLevels) {
        (self.bbo(coin, replica), Overlay::default().bbo(replica.coin(coin)))
    }

    /// `best_levels` where a line seen at `seen_ns` runs (`at_line`).
    pub(crate) fn best_at_line(&self, coin: &Coin, replica: &BookReplica, seen_ns: i64) -> (BestLevels, BestLevels) {
        // the forecast reads the book the runs stand on: with the coin's trailing stops (review 000662 r1 P2-4)
        let trailing = aligned(Some(replica)).map_or_else(CoinTrailing::default, |(replica, height, _)| self.coin_trailing(coin, replica, height));
        let base = replica.coin(coin).with_trailing(&trailing);
        (self.at_line(coin, base, seen_ns, |overlay| overlay.bbo(base)), Overlay::default().bbo(replica.coin(coin)))
    }

    /// What `read` takes of `coin`'s overlay where a line seen at `seen_ns` runs: before the coin's TWAP suborders due
    /// when it comes before their cut, as `CoinActions::replay` runs it (batch 2l, review 000642 r1 P2-2); with every
    /// action held otherwise.
    fn at_line<R>(&self, coin: &Coin, base: CoinBase<'_>, seen_ns: i64, read: impl FnOnce(&Overlay) -> R) -> R {
        let due = self.due.get(coin).is_some_and(|slices| !slices.is_empty());
        let cut_ns = slice_cut_ns(self.basis_ms);
        match self.coins.get(coin) {
            Some(actions) if due && seen_ns < cut_ns => {
                let basis = Basis::new(self.record.priorities(), &self.refusals, self.basis_ms);
                read(&actions.overlay_before_cut(base, basis, cut_ns))
            }
            Some(actions) => read(&actions.overlay),
            None => read(&Overlay::default()),
        }
    }

    /// Whether the block log showed the block holding `tx_hash`, still recorded (batch 2l).
    pub(crate) fn logged(&self, tx_hash: &B256) -> bool {
        self.record.height_of(tx_hash).is_some()
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

    /// A line's actions on `coin`, seen at `seen_ns`, each with its place in the line: applied on the overlay as they
    /// come - or, the coin holding TWAP suborders the line comes before, the coin replayed with them, the suborders after
    /// it (batch 2j) -, their trades into `trades` while `emit`.
    fn hold(&mut self, coin: Coin, line_actions: Vec<(usize, PendingAction)>, seen_ns: i64, aligned: Option<Aligned<'_>>, emit: bool, trades: &mut LineTrades) {
        self.held += line_actions.len();
        let behind = aligned.is_some_and(|(_, height, _)| !self.record_covers(height));
        let emit_twaps = emit && self.in_trade_window(self.basis_ms);
        // a coin first held: its trailing stops read now, as its runs will stand on them
        if let Some((replica, height, _)) = aligned.filter(|_| !self.gaps.replica)
            && !self.trailing.contains_key(&coin)
        {
            let read = self.trailing_basis.at(&self.record, height).coin(replica.trigger_table(), &coin);
            self.trailing.insert(coin.clone(), read);
        }
        let Self { coins, due, slots, record, refusals, gaps, priorities_behind, trailing, .. } = self;
        let coin_trailing = trailing.get(&coin).unwrap_or(&NO_TRAILING);
        let base = aligned
            .filter(|_| !gaps.replica)
            .map(|(replica, _, confirmed_ns)| {
                (replica.coin(&coin).with_trailing(coin_trailing), Basis::new(record.priorities(), refusals, unix_ms(confirmed_ns)), unix_ms(confirmed_ns))
            });
        let actions = coins.entry(coin.clone()).or_default();
        let first = actions.actions.len();
        let (orders, line_actions): (Vec<usize>, Vec<PendingAction>) = line_actions.into_iter().unzip();
        actions.actions.extend(line_actions);
        let Some((base, basis, basis_ms)) = base else { return };
        let slices = due.get(&coin).map_or(&[][..], Vec::as_slice);
        let cut_ns = slice_cut_ns(basis_ms);
        let effects = if !slices.is_empty() && seen_ns < cut_ns {
            let replayed = actions.replay(base, basis, slices, cut_ns, first);
            slice_trades(&coin, slices, replayed.slices, emit_twaps, slots, &mut trades.suborders);
            replayed.reported
        } else {
            let CoinActions { actions: held, overlay } = &mut *actions;
            let effects: Vec<(usize, Effect)> =
                (first..held.len()).filter(|index| !held[*index].unknown).map(|index| (index, overlay.apply(base, basis, &held[index]))).collect();
            actions.keep_stale(&effects);
            effects
        };
        *priorities_behind |= behind && !effects.is_empty();
        let in_line = effects.into_iter().map(|(index, effect)| (orders[index - first], &actions.actions[index], effect));
        trades_of(&coin, in_line, emit, &mut trades.actions);
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
        self.judge_bands(replica, &mut changed);
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
        // the replica's height moved: each held coin's trailing stops read there (batch 2n-2)
        changed.extend(self.read_trailing(replica, height));
        self.priorities_behind |= !covered && changed.iter().any(|coin| self.coins.contains_key(coin));
        let mut trades = Vec::new();
        self.replay_coins(replica, &changed, emit, &mut trades);
        Step { trades, changed }
    }

    /// The signed actions held with no replica aligned, their price band judged against `replica` before any overlay
    /// applies them: one an order of which is under it leaves whole, its coins replayed (batch 2k, review 000637 r2).
    fn judge_bands(&mut self, replica: &BookReplica, changed: &mut HashSet<Coin>) {
        if self.band_unjudged.is_empty() {
            return;
        }
        self.band_unjudged_slots = 0;
        let banded: FxHashSet<(B256, usize)> = std::mem::take(&mut self.band_unjudged)
            .into_iter()
            .filter(|(_, orders)| orders.iter().any(|(coin, limit_px)| below_price_band(replica.coin(coin), *limit_px)))
            .map(|(key, _)| key)
            .collect();
        for (coin, actions) in &mut self.coins {
            let before = actions.actions.len();
            actions.actions.retain(|action| {
                let out = banded.contains(&(action.id.tx_hash, action.id.action));
                if out {
                    count_action(action, "price_band");
                }
                !out
            });
            if actions.actions.len() < before {
                self.held -= before - actions.actions.len();
                changed.insert(coin.clone());
            }
        }
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
        self.band_unjudged.clear();
        self.band_unjudged_slots = 0;
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
    /// The taker's line's `local_time`, unix ms - a triggered order's, its `SetGlobalAction`'s -; a TWAP suborder has
    /// none.
    pub(crate) fn seen_ms(&self) -> Option<u64> {
        match &self.taker {
            Taker::Pending { seen_ns, .. } | Taker::Trigger { seen_ns, .. } => Some(unix_ms(*seen_ns)),
            Taker::Twap { .. } => None,
        }
    }

    /// The account the taker acts for, as known: a pending order's vault, a TWAP's user, a trigger order's account.
    pub(crate) const fn taker_account(&self) -> Option<Address> {
        match &self.taker {
            Taker::Pending { vault, .. } => *vault,
            Taker::Twap { user, .. } => Some(*user),
            Taker::Trigger { source, .. } => source.account(),
        }
    }
}

/// Each signed action of `tx` an action of which is held, with the lowest limit of its orders the price band judges on
/// each coin - all it reads of them (batch 2k, review 000637 r4).
fn unjudged_bands(tx: &MempoolTx, held: &[(Coin, PendingAction)], coins: &ActionCoins) -> Vec<(usize, Vec<(Coin, Px)>)> {
    let mut actions: Vec<usize> = held.iter().map(|(_, action)| action.id.action).collect();
    actions.dedup();
    actions
        .into_iter()
        .map(|action| {
            let mut lowest: Vec<(&str, Px)> = Vec::new();
            for (coin, limit_px) in band_orders(&tx.actions[action].kind, coins) {
                match lowest.iter_mut().find(|(held, _)| *held == coin) {
                    Some((_, low)) => *low = (*low).min(limit_px),
                    None => lowest.push((coin, limit_px)),
                }
            }
            (action, lowest.into_iter().map(|(coin, limit_px)| (Coin::new(coin), limit_px)).collect())
        })
        .collect()
}

/// The next second's start after the confirmed block time `basis_ms` less `SLICE_CUT_MS`, unix ns: the actions of the
/// lines seen before it run before that second's TWAP suborders.
fn slice_cut_ns(basis_ms: u64) -> i64 {
    unix_ns((basis_ms / 1_000 + 1) * 1_000 - SLICE_CUT_MS)
}

/// A line's actions' first applications - (place in the line, action, effect) -, counted; their trades by their place,
/// while `emit`.
fn trades_of<'a>(coin: &Coin, applied: impl Iterator<Item = (usize, &'a PendingAction, Effect)>, emit: bool, trades: &mut Vec<(usize, PendingTrade)>) {
    for (order_in_line, action, effect) in applied {
        count_effect(action, &effect, emit);
        if emit {
            trades.extend(effect_trades(coin, action, effect).into_iter().map(|trade| (order_in_line, trade)));
        }
    }
}

/// An action's first application counted: its outcome; a `SetGlobal`'s on its coin and each order it triggered; the
/// fills while `emit`.
fn count_effect(action: &PendingAction, effect: &Effect, emit: bool) {
    PENDING_BOOK_OVERLAY_TOTAL.with_label_values(&[action.op.label(), effect.outcome]).inc();
    if let BookOp::SetGlobal(_) = &action.op {
        PENDING_BOOK_TRIGGER_COINS_TOTAL.with_label_values(&[coin_outcome(effect)]).inc();
        for triggered in &effect.triggered {
            count_triggered(triggered, emit);
        }
        return;
    }
    if emit && action.op.placement().is_some() {
        count_fills(&effect.fills);
    }
}

/// An action's effect as trades: the fills of the order it places, or of each order a `SetGlobal` triggered.
fn effect_trades(coin: &Coin, action: &PendingAction, effect: Effect) -> Vec<PendingTrade> {
    if let BookOp::SetGlobal(_) = &action.op {
        return effect.triggered.into_iter().flat_map(|triggered| trigger_trades(coin, action, triggered)).collect();
    }
    // only an action placing an order fills
    let Some(order) = action.op.placement() else { return Vec::new() };
    effect.fills.into_iter().map(|fill| predicted(coin, action, order, fill)).collect()
}

/// The TWAP suborders' fills of a replay as trades, each (twap, slot) once, the first time it fills while `emit_twaps`
/// (in the trade window, batch 2i) - again only when the step computing them did not tell them (`SuborderSlots`).
fn slice_trades(coin: &Coin, slices: &[TwapSlice], filled: Vec<Vec<Fill>>, emit_twaps: bool, slots: &mut SuborderSlots, trades: &mut Vec<PendingTrade>) {
    for (slice, fills) in slices.iter().zip(filled) {
        if !emit_twaps || fills.is_empty() || !slots.compute(slice) {
            continue;
        }
        count_fills(&fills);
        trades.extend(fills.into_iter().map(|fill| twap_trade(coin, slice, fill)));
    }
}

/// A trade's place in a run cut at `cut_ns`: an action seen before the cut, a suborder by its due time and TWAP id, an
/// action at or after the cut.
const fn run_order(trade: &PendingTrade, cut_ns: i64) -> (u8, u64, u64) {
    match &trade.taker {
        Taker::Pending { seen_ns, .. } | Taker::Trigger { seen_ns, .. } if *seen_ns < cut_ns => (0, 0, 0),
        Taker::Twap { twap_id, due_ms, .. } => (1, *due_ms, *twap_id),
        Taker::Pending { .. } | Taker::Trigger { .. } => (2, 0, 0),
    }
}

/// A `SetGlobal`'s first application on its coin, as counted: why none ran, or whether any fired.
fn coin_outcome(effect: &Effect) -> &'static str {
    match (effect.outcome, effect.triggered.is_empty()) {
        ("set_global", true) => "none_fired",
        ("set_global", false) => "executed",
        (reason, _) => reason,
    }
}

/// A batch's triggered order counted: its outcome and source; its fills while `emit`.
fn count_triggered(triggered: &TriggerFills, emit: bool) {
    PENDING_BOOK_OVERLAY_TOTAL.with_label_values(&["trigger", triggered.outcome]).inc();
    PENDING_BOOK_TRIGGERS_TOTAL.with_label_values(&[triggered.source.label()]).inc();
    if triggered.outcome == "zero_size" {
        PENDING_BOOK_TRIGGERS_TOTAL.with_label_values(&["zero_size"]).inc();
    }
    if emit {
        count_fills(&triggered.fills);
    }
}

fn count_fills(fills: &[Fill]) {
    for fill in fills {
        PENDING_BOOK_PREDICTED_FILLS_TOTAL.with_label_values(&[fill.maker.label()]).inc();
    }
}

/// A batch's triggered order's trades, its taker the order `set_global` fired.
fn trigger_trades<'a>(coin: &'a Coin, set_global: &PendingAction, triggered: TriggerFills) -> impl Iterator<Item = PendingTrade> + 'a {
    let TriggerFills { source, side, fills, .. } = triggered;
    let taker = Taker::Trigger { source, set_global: set_global.id, seen_ns: set_global.seen_ns };
    fills.into_iter().map(move |fill| PendingTrade { coin: coin.clone(), side, px: fill.px, sz: fill.sz, maker: fill.maker, taker: taker.clone() })
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
        taker: Taker::Twap { twap_id: slice.twap_id, user: slice.user, slot: slice.slot, due_ms: slice.due_ms },
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
pub(crate) fn unix_ns(ms: u64) -> i64 {
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
        listeners::order_book::replica::tests::{block_lines, block_time_ms, btc_diff, btc_status, btc_stop_status, new_diff, remove, replica_of_empty_book},
        order_book::Oid,
        pending::{
            actions::tests::{coins, requested},
            blocks::tests::hash,
            mempool::{ActionKind, RequestedTif, RequestedTrigger, SignedAction, TriggerGrouping},
            refusals::{Refusal, TakerOutcome},
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
            let block = LoggedBlock { height: self.logged, time_ms: block_time_ms(self.logged), hashes, priorities, taker_outcomes: Vec::new(), signer_accounts: Vec::new(), twap_cancels: Vec::new(), set_global: false, read_at };
            self.shadow.on_block(block, false, Some(&self.replica))
        }

        /// The log's next block, with its takers' `outcomes`; `after_loss` as the watcher tells it.
        fn log_outcomes(&mut self, outcomes: Vec<TakerOutcome>, after_loss: bool) -> Step {
            self.logged += 1;
            let read_at = self.tick();
            let block = LoggedBlock { height: self.logged, time_ms: block_time_ms(self.logged), hashes: Vec::new(), priorities: Vec::new(), taker_outcomes: outcomes, signer_accounts: Vec::new(), twap_cancels: Vec::new(), set_global: false, read_at };
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

    /// The replica's stops' account in these tests (`btc_stop_status`'s user), not its resting orders' (`0x..01`).
    pub(crate) const ACCOUNT: Address = Address::with_last_byte(2);

    /// A `SetGlobalAction`'s line seen at `seen_ms`, its hash from `byte`.
    fn set_global_line(seen_ms: i64, byte: u8) -> MempoolTx {
        let kind = ActionKind::SetGlobal(Vec::new());
        MempoolTx { seen_ns: seen_ms * NS_PER_MS, tx_hash: hash(byte), actions: vec![SignedAction { vault: None, signer: None, expires_after_ms: None, priority: 0, kind }] }
    }

    /// BTC's coin of a `SetGlobalAction` at mark 100, the positions (whole units) of `positions` frozen.
    fn btc_at_100(positions: &[(Address, i128)]) -> Vec<(Coin, u32, SetGlobalCoin)> {
        let positions = positions.iter().map(|(account, units)| (*account, units * 100_000_000)).collect();
        let sga = SetGlobalCoin { mark_px: Px::parse_from_str("100").unwrap(), positions, positions_known: true, stale: std::collections::BTreeSet::new(), undecided_input: None };
        vec![(Coin::new(BTC), 0, sga)]
    }

    impl Harness {
        /// A confirmed stop of `ACCOUNT` opened in the next block (`btc_stop_status`).
        fn open_stop(&mut self, oid: u64, side: &str, limit: Option<&str>, position: bool, cloid: Option<&str>) {
            let at = block_time_ms(self.height + 1);
            self.block(&[], vec![btc_stop_status(oid, side, limit, position, "open", cloid, at)], vec![]);
        }

        /// A bid of `sz` at 99, oid `oid`, resting in the next block.
        fn rest_bid(&mut self, oid: u64, sz: &str) {
            let at = block_time_ms(self.height + 1);
            self.block(&[], vec![btc_status(oid, "B", "99.0", sz, "open", None, "Alo", at)], vec![btc_diff(oid, "B", "99.0", new_diff(sz))]);
        }

        /// The `SetGlobal` held on BTC: the accounts kept stale on it.
        fn stale_on_set_global(&self) -> Vec<Address> {
            let actions = &self.shadow.coins[&Coin::new(BTC)].actions;
            actions.iter().find_map(|action| if let BookOp::SetGlobal(sga) = &action.op { Some(sga.stale.iter().copied().collect()) } else { None }).unwrap()
        }
    }

    /// BTC's coin of a `SetGlobalAction` at `mark`, no position frozen.
    fn btc_at(mark: &str) -> Vec<(Coin, u32, SetGlobalCoin)> {
        let sga = SetGlobalCoin { mark_px: Px::parse_from_str(mark).unwrap(), positions: Vec::new(), positions_known: true, stale: std::collections::BTreeSet::new(), undecided_input: None };
        vec![(Coin::new(BTC), 0, sga)]
    }

    /// The account copy's trailing stops at `height` (BTC's szDecimals 5), each a sale of retracement 5 at `best` (price
    /// units of 0.1).
    fn trailing_basis(height: u64, stops: &[(u64, u64)]) -> TrailingBasis {
        use crate::accounts::{flow_accounts::{AccountsSnapshot, TrailingCopy}, marks::trailing::{Retracement, TrailingState, TrailingStop}};
        let stop = |best| TrailingStop { side: Side::Ask, activation: None, retracement: Retracement::Price(50), state: TrailingState::Active { best } };
        let stops = stops.iter().map(|(oid, best)| (*oid, (BTC.to_string(), Ok(stop(*best))))).collect();
        let snapshot = AccountsSnapshot {
            generation: 1,
            height,
            time_ms: block_time_ms(height),
            coins: FxHashMap::from_iter([(BTC.to_string(), 5)]),
            marks: FxHashMap::from_iter([(BTC.to_string(), 1_000)]),
            positions: FxHashMap::default(),
            candidates: Err("none".to_string()),
            trailing: Some(TrailingCopy { stops, refresh_due_ms: None, claimed: std::iter::once(BTC.to_string()).collect() }),
        };
        let (accounts, _validity) = crate::accounts::flow_accounts::tests::copy_of(snapshot);
        TrailingBasis::of(accounts.copy().ok())
    }

    impl Harness {
        /// A BTC trailing stop market sale of 1 of `ACCOUNT`, `condition`, opened in the next block.
        fn open_trailing(&mut self, oid: u64, condition: &str) {
            let at = block_time_ms(self.height + 1);
            let mut status = btc_stop_status(oid, "A", None, false, "open", None, at);
            status["order"]["orderType"] = serde_json::json!("Trailing Stop Market");
            status["order"]["triggerCondition"] = serde_json::json!(condition);
            status["order"]["triggerPx"] = serde_json::json!("95.0");
            self.block(&[], vec![status], vec![]);
        }
    }

    /// Batch 2n-2 (design 000659 r2 s3): a trailing stop's best the account copy moves, with no book event, replays its
    /// coin - the `SetGlobalAction` held at 104 fires the stop once its best is 110 (trigger price 105), as an IOC sale
    /// taking the bid at 99; at best 100 (95) it does not.
    #[test]
    fn a_trailing_stops_best_moving_in_the_copy_replays_its_coin() {
        let mut harness = Harness::ready_from(1_000);
        harness.rest_bid(12, "1.0");
        harness.open_trailing(70, "Activation immediate, retracement 5, best 100");
        assert!(harness.shadow.on_trailing(trailing_basis(harness.height, &[(70, 1_000)]), Some(&harness.replica)).changed.is_empty(), "no coin held");
        let tx = set_global_line(i64::try_from(block_time_ms(harness.height)).unwrap() + 50, 1);
        let step = harness.shadow.on_set_global(&tx, btc_at("104"), Some(&harness.replica));
        assert_eq!((step.trades.len(), harness.bbo().0), (0, Some(level("99", "1", 1))));
        let step = harness.shadow.on_trailing(trailing_basis(harness.height, &[(70, 1_100)]), Some(&harness.replica));
        assert_eq!((step.changed.contains(&Coin::new(BTC)), harness.bbo().0), (true, None), "replayed: the stop took the bid");
        assert!(harness.shadow.on_trailing(trailing_basis(harness.height, &[(70, 1_100)]), Some(&harness.replica)).changed.is_empty(), "as it was");
    }

    /// Design 000659 r3 P2-2: the account copy at A, the replica confirming a block above it that holds a
    /// `SetGlobalAction` - a mark the copy has not applied yet - turns BTC's trailing stop not known as the replica
    /// settles: the held action at 104 then leaves BTC not decided; the copy reaching that block, it is decided again.
    #[test]
    fn a_mark_written_above_the_copy_leaves_its_stops_not_known() {
        let mut harness = Harness::ready_from(1_000);
        harness.rest_bid(12, "1.0");
        harness.open_trailing(70, "Activation immediate, retracement 5, best 100");
        let copy_height = harness.height;
        harness.shadow.on_trailing(trailing_basis(copy_height, &[(70, 1_000)]), Some(&harness.replica));
        let btc = Coin::new(BTC);
        let seen_ms = i64::try_from(block_time_ms(harness.height)).unwrap() + 50;
        harness.shadow.on_set_global(&set_global_line(seen_ms, 1), btc_at("104"), Some(&harness.replica));
        assert_eq!(harness.shadow.coin_view(&btc, &harness.replica).undecided, None, "95 not reached");
        // a block above the copy's holding a SetGlobalAction, then the next one's lines confirming it
        harness.logged += 1;
        let read_at = harness.tick();
        let written = LoggedBlock {
            height: harness.logged, time_ms: block_time_ms(harness.logged), hashes: Vec::new(), priorities: Vec::new(), taker_outcomes: Vec::new(),
            signer_accounts: Vec::new(), twap_cancels: Vec::new(), set_global: true, read_at,
        };
        harness.shadow.on_block(written, false, Some(&harness.replica));
        harness.confirm(vec![], vec![]);
        assert_eq!(harness.shadow.coin_view(&btc, &harness.replica).undecided, Some("trailing_unknown"));
        harness.shadow.on_trailing(trailing_basis(harness.height, &[(70, 1_000)]), Some(&harness.replica));
        assert_eq!(harness.shadow.coin_view(&btc, &harness.replica).undecided, None, "the copy at the replica's height");
    }

    /// Design 000659 r3 P2-3, r4: a `SetGlobalAction` that may fire a trailing stop not known - the account copy not
    /// serving - leaves BTC not decided from its place; a second one coming meanwhile is held as a place, nothing
    /// frozen; the first's block confirmed, BTC is still not decided, until the second's is too.
    #[test]
    fn a_coin_stays_undecided_until_the_last_action_depending_on_it_leaves() {
        let mut harness = Harness::ready_from(1_000);
        harness.rest_bid(12, "1.0");
        harness.open_trailing(70, "Activation immediate, retracement 5, best 100");
        let btc = Coin::new(BTC);
        let seen_ms = i64::try_from(block_time_ms(harness.height)).unwrap() + 50;
        assert_eq!(harness.shadow.undecided_at(&btc, Some(&harness.replica), seen_ms * NS_PER_MS), None);
        harness.shadow.on_set_global(&set_global_line(seen_ms, 1), btc_at("104"), Some(&harness.replica));
        assert_eq!(harness.shadow.coin_view(&btc, &harness.replica).undecided, Some("trailing_unknown"));
        let later_ns = (seen_ms + 20) * NS_PER_MS;
        assert_eq!(harness.shadow.undecided_at(&btc, Some(&harness.replica), later_ns), Some("trailing_unknown"));
        let second = vec![(btc.clone(), 0, SetGlobalCoin::undecided("trailing_unknown"))];
        harness.shadow.on_set_global(&set_global_line(seen_ms + 20, 2), second, Some(&harness.replica));
        harness.block(&[1], vec![], vec![]);
        assert_eq!(harness.shadow.coin_view(&btc, &harness.replica).undecided, Some("trailing_unknown"), "the second still held");
        harness.block(&[2], vec![], vec![]);
        let view = harness.shadow.coin_view(&btc, &harness.replica);
        assert_eq!((view.undecided, view.bbo.0), (None, Some(level("99", "1", 1))), "decided again, the book as confirmed");
    }

    /// Batch 2m: a `SetGlobalAction`'s coin is held with its line, fires the replica's stop where it stands - its taker
    /// the trigger order, its `seenMs` the line's - and leaves with the line's block; the line again holds none.
    #[test]
    fn a_set_global_fires_with_its_line_and_leaves_with_its_block() {
        let mut harness = Harness::ready_from(1_000);
        harness.rest_ask(11);
        harness.open_stop(77, "B", None, false, None);
        let tx = set_global_line(3_150, 1);
        let step = harness.shadow.on_set_global(&tx, btc_at_100(&[]), Some(&harness.replica));
        let [trade] = step.trades.as_slice() else { panic!("{step:?}") };
        let source = TriggerSource::Confirmed { oid: 77, user: ACCOUNT, cloid: None };
        let taker = Taker::Trigger { source, set_global: ActionId { tx_hash: tx.tx_hash, action: 0, item: 0 }, seen_ns: tx.seen_ns };
        assert_eq!((&trade.taker, trade.seen_ms(), trade.taker_account()), (&taker, Some(3_150), Some(ACCOUNT)));
        assert!(matches!(&trade.maker, Maker::Confirmed { oid, .. } if *oid == Oid::new(11)));
        assert_eq!((harness.shadow.held, harness.bbo().1, step.changed.len()), (1, None, 1));
        assert!(harness.shadow.on_set_global(&tx, btc_at_100(&[]), Some(&harness.replica)).trades.is_empty(), "held already");
        harness.block(&[1], vec![], vec![]);
        assert_eq!((harness.shadow.held, harness.bbo().1), (0, Some(level("101", "1", 1))), "its block confirmed it");
    }

    /// Design 000652 r2 P2-1 acceptance: two actions of different lines reaching the stop fire it once; the first's
    /// block triggering it there, the second replayed fires nothing.
    #[test]
    fn two_set_globals_fire_a_stop_once() {
        let mut harness = Harness::ready_from(1_000);
        harness.rest_ask(11);
        harness.open_stop(77, "B", None, false, None);
        let first = harness.shadow.on_set_global(&set_global_line(3_150, 1), btc_at_100(&[]), Some(&harness.replica));
        let second = harness.shadow.on_set_global(&set_global_line(3_160, 2), btc_at_100(&[]), Some(&harness.replica));
        assert_eq!((first.trades.len(), second.trades.len(), harness.bbo().1), (1, 0, None));
        let at = block_time_ms(harness.height + 1);
        let fired = vec![btc_stop_status(77, "B", None, false, "triggered", None, at), btc_status(11, "A", "101.0", "0.0", "filled", None, "Alo", 1_000)];
        let step = harness.block(&[1], fired, vec![btc_diff(11, "A", "101.0", remove())]);
        assert_eq!((step.trades.len(), harness.shadow.held, harness.bbo().1), (0, 1, None));
    }

    /// Design 000652 r2 P2-1 acceptance: a pending limit stop's remainder rests where the action fires it; its line
    /// confirmed without the order opening - rejected -, the action replayed leaves no remainder.
    #[test]
    fn a_rejected_pending_stop_leaves_no_remainder() {
        let mut harness = Harness::ready_from(1_000);
        harness.rest_ask(11);
        let mut order = requested(0, false, "100.5", RequestedTif::Trigger, Some("0x0c"));
        order.trigger = Some(RequestedTrigger { trigger_px: Px::parse_from_str("100").unwrap(), is_market: false, take_profit: false, grouping: TriggerGrouping::Alone });
        drop(harness.line(3_150, 2, vec![ActionKind::Order(vec![order])]));
        drop(harness.shadow.on_set_global(&set_global_line(3_160, 1), btc_at_100(&[]), Some(&harness.replica)));
        assert_eq!(harness.bbo().1, Some(level("100.5", "1", 1)));
        harness.block(&[2], vec![], vec![]);
        assert_eq!(harness.bbo().1, Some(level("101", "1", 1)));
    }

    /// Batch 2m (design 000652 r2 P2-3): a pending cancel by cloid of the stop's account takes the replica's stop out
    /// of the untriggered ones - the cloid index holding it -: the action fires nothing.
    #[test]
    fn a_pending_cancel_by_cloid_takes_a_confirmed_stop_out() {
        for (vault, trades) in [(ACCOUNT, 0), (Address::with_last_byte(3), 1)] {
            let mut harness = Harness::ready_from(1_000);
            harness.rest_ask(11);
            harness.open_stop(77, "B", None, false, Some("0x0e"));
            let cancel = ActionKind::CancelByCloid { fast: false, cancels: vec![crate::accounts::replica::WireCancelByCloid { asset: 0, cloid: "0x0e".to_string() }] };
            let tx = MempoolTx { seen_ns: 3_140 * NS_PER_MS, tx_hash: hash(2), actions: vec![SignedAction { vault: Some(vault), signer: None, expires_after_ms: None, priority: 0, kind: cancel }] };
            drop(harness.shadow.on_tx(&tx, false, Some(&coins()), Some(&harness.replica)));
            let step = harness.shadow.on_set_global(&set_global_line(3_150, 1), btc_at_100(&[]), Some(&harness.replica));
            assert_eq!(step.trades.len(), trades, "vault {vault}");
        }
    }

    impl Harness {
        /// The log's next block, teaching `signer_accounts`; `after_loss` as the watcher tells it.
        fn log_teaching(&mut self, signer_accounts: Vec<(Address, Address)>, after_loss: bool) -> Step {
            let time_ms = block_time_ms(self.logged + 1);
            self.log_teaching_at(time_ms, signer_accounts, after_loss)
        }

        /// `log_teaching`, its block at `time_ms`.
        fn log_teaching_at(&mut self, time_ms: u64, signer_accounts: Vec<(Address, Address)>, after_loss: bool) -> Step {
            self.logged += 1;
            let read_at = self.tick();
            let block = LoggedBlock {
                height: self.logged,
                time_ms,
                hashes: Vec::new(),
                priorities: Vec::new(),
                taker_outcomes: Vec::new(),
                signer_accounts,
                twap_cancels: Vec::new(),
                set_global: false,
                read_at,
            };
            self.shadow.on_block(block, after_loss, Some(&self.replica))
        }

        /// A reduce-only stop limit selling 1 at 99 on a fall to 100, placed without a vault by `signer` at `seen_ms`.
        fn place_close_by(&mut self, seen_ms: i64, byte: u8, signer: Address) {
            let mut close = requested(0, false, "99", RequestedTif::Trigger, None);
            close.reduce_only = true;
            close.trigger = Some(RequestedTrigger { trigger_px: Px::parse_from_str("100").unwrap(), is_market: false, take_profit: false, grouping: TriggerGrouping::Alone });
            drop(self.signed_line(seen_ms, byte, 0, Some(signer), vec![ActionKind::Order(vec![close])]));
        }

        fn held_accounts(&self) -> Vec<Option<Address>> {
            self.shadow.coins[&Coin::new(BTC)].actions.iter().map(|action| action.account).collect()
        }
    }

    /// Batch 2m-2: a block teaches a signer's account - a block log gap notwithstanding: what a block ran is so -; that
    /// signer's trigger order placed without a vault then acts for it.
    #[test]
    fn a_learned_signer_gives_a_pending_trigger_order_its_account() {
        let mut harness = Harness::ready_from(1_000);
        let signer = Address::with_last_byte(0x51);
        drop(harness.log_teaching(vec![(signer, ACCOUNT)], true));
        harness.place_close_by(3_150, 2, signer);
        assert_eq!(harness.held_accounts(), [Some(ACCOUNT)]);
    }

    /// Review 000656 r1 P2-1: a block a day after a signer was last seen forgets it before learning its own: the account
    /// it names is the signer's anew, not a conflict.
    #[test]
    fn a_signer_expired_by_a_block_is_learned_anew_there() {
        let mut harness = Harness::ready_from(1_000);
        let signer = Address::with_last_byte(0x51);
        drop(harness.log_teaching(vec![(signer, ACCOUNT)], false));
        let day_later = block_time_ms(harness.logged) + 25 * 3_600_000;
        drop(harness.log_teaching_at(day_later, vec![(signer, Address::with_last_byte(0xb0))], true));
        assert_eq!(harness.shadow.signer_accounts.account(signer), Ok(Address::with_last_byte(0xb0)));
    }

    /// Review 000656 r2 P2: a signer forgotten - unseen past its time, or the longest unseen past the cap - takes the
    /// account it gave back from the actions held of it, whatever account it is learned anew for: the close of
    /// `ACCOUNT`, long 1, sells nothing on the action reaching it.
    #[test]
    fn a_signer_forgotten_takes_its_account_back() {
        for (anew, sold) in [(ACCOUNT, 0), (Address::with_last_byte(0xb0), 0)] {
            let mut harness = Harness::ready_from(1_000);
            harness.rest_bid(12, "5.0");
            let signer = Address::with_last_byte(0x51);
            drop(harness.log_teaching(vec![(signer, ACCOUNT)], false));
            harness.place_close_by(3_150, 2, signer);
            let day_later = block_time_ms(harness.logged) + 25 * 3_600_000;
            drop(harness.log_teaching_at(day_later, vec![(signer, anew)], false));
            let accounts = harness.held_accounts();
            let step = harness.shadow.on_set_global(&set_global_line(3_160, 1), btc_at_100(&[(ACCOUNT, 1)]), Some(&harness.replica));
            assert_eq!((accounts, step.trades.len()), (vec![None], sold), "learned anew for {anew}");
        }
    }

    /// Review 000656 r1 P2-2: a signer turned conflicting takes the account it gave back from the actions held of it:
    /// the reduce-only close of `ACCOUNT`, long 1, is then of no known account - the action reaching it runs none on the
    /// coin, where the old account would have sold 1 at 99.
    #[test]
    fn a_signer_turned_conflicting_takes_its_account_back() {
        for conflicting in [false, true] {
            let mut harness = Harness::ready_from(1_000);
            harness.rest_bid(12, "5.0");
            let signer = Address::with_last_byte(0x51);
            drop(harness.log_teaching(vec![(signer, ACCOUNT)], false));
            harness.place_close_by(3_150, 2, signer);
            if conflicting {
                drop(harness.log_teaching(vec![(signer, Address::with_last_byte(0xb0))], false));
            }
            let accounts = harness.held_accounts();
            let step = harness.shadow.on_set_global(&set_global_line(3_160, 1), btc_at_100(&[(ACCOUNT, 1)]), Some(&harness.replica));
            let expected = if conflicting { (vec![None], 0) } else { (vec![Some(ACCOUNT)], 1) };
            assert_eq!((accounts, step.trades.len()), expected, "conflicting {conflicting}");
        }
    }

    /// Design 000652 r3 P2-2: a confirmed cancel of a stop - no pending cancel, no book change - replays its coin: the
    /// remainder its action rested goes.
    #[test]
    fn a_confirmed_cancel_of_a_stop_replays_its_coin() {
        let mut harness = Harness::ready_from(1_000);
        harness.rest_ask(11);
        harness.open_stop(77, "A", Some("100.5"), false, None);
        drop(harness.shadow.on_set_global(&set_global_line(3_150, 1), btc_at_100(&[(ACCOUNT, 0)]), Some(&harness.replica)));
        assert_eq!(harness.bbo().1, Some(level("100.5", "1", 1)));
        let at = block_time_ms(harness.height + 1);
        let step = harness.block(&[], vec![btc_stop_status(77, "A", Some("100.5"), false, "canceled", None, at)], vec![]);
        assert_eq!((step.changed.contains(&Coin::new(BTC)), harness.bbo().1), (true, Some(level("101", "1", 1))));
    }

    /// Design 000652 r5 acceptance: long 2, `ACCOUNT`'s vault IOC sells 1 before the action, which reaches only its plain
    /// stop - run, `ACCOUNT` kept stale on it -; the IOC confirmed and a position stop of `ACCOUNT` opened, the action
    /// replayed runs none: no 2 offered.
    #[test]
    fn a_stale_account_stays_stale_after_the_fill_confirms() {
        let mut harness = Harness::ready_from(1_000);
        harness.rest_bid(12, "5.0");
        harness.open_stop(77, "A", None, false, None);
        let sells = ActionKind::Order(vec![requested(0, false, "99", RequestedTif::Ioc, Some("0x05"))]);
        let tx = MempoolTx { seen_ns: 3_150 * NS_PER_MS, tx_hash: hash(2), actions: vec![SignedAction { vault: Some(ACCOUNT), signer: None, expires_after_ms: None, priority: 0, kind: sells }] };
        drop(harness.shadow.on_tx(&tx, false, Some(&coins()), Some(&harness.replica)));
        let step = harness.shadow.on_set_global(&set_global_line(3_160, 1), btc_at_100(&[(ACCOUNT, 2)]), Some(&harness.replica));
        assert_eq!((step.trades.len(), harness.stale_on_set_global()), (1, vec![ACCOUNT]));
        let at = block_time_ms(harness.height + 1);
        harness.block(&[2], vec![btc_stop_status(78, "A", Some("100.5"), true, "open", None, at)], vec![]);
        assert_eq!((harness.bbo(), harness.stale_on_set_global()), ((Some(level("99", "5", 1)), None), vec![ACCOUNT]));
    }

    /// Review 000662 r1 P2-4: with a TWAP suborder due, a line before its cut reads the book the runs stand on - with
    /// the trailing stops -: a `SetGlobalAction` at 104 having fired the stop of best 110 (trigger price 105) into the
    /// only bid, the next line reads no bid.
    #[test]
    fn a_line_before_the_cut_reads_the_book_with_the_trailing_stops() {
        let mut harness = Harness::ready_from(1_000);
        harness.rest_ask(11);
        harness.rest_bid(12, "1.0");
        harness.open_trailing(70, "Activation immediate, retracement 5, best 110");
        harness.shadow.on_trailing(trailing_basis(harness.height, &[(70, 1_100)]), Some(&harness.replica));
        drop(harness.shadow.on_anchor(twap_anchor(harness.height), Some(&harness.replica)));
        drop(harness.twaps_known_now());
        harness.shadow.on_set_global(&set_global_line(3_150, 1), btc_at("104"), Some(&harness.replica));
        let btc = Coin::new(BTC);
        assert_eq!(harness.shadow.best_at_line(&btc, &harness.replica, 3_160 * NS_PER_MS).0.0, None, "the stop took the bid before the cut");
    }

    /// Review 000642 r1 P2-2: a line before the suborder's cut runs before it - its book the one the suborder has not
    /// taken yet -; one at the cut after it.
    #[test]
    fn a_line_before_the_cut_reads_the_book_before_the_suborder() {
        let mut harness = Harness::ready_from(1_000);
        harness.rest_ask(11);
        drop(harness.shadow.on_anchor(twap_anchor(harness.height), Some(&harness.replica)));
        drop(harness.twaps_known_now());
        let btc = Coin::new(BTC);
        let ask = |seen_ms: i64| harness.shadow.best_at_line(&btc, &harness.replica, seen_ms * NS_PER_MS).0.1;
        assert_eq!(ask(3_150), Some(level("101", "1", 1)), "before the cut: the suborder not run yet");
        assert_eq!(ask(3_980), Some(level("101", "0.9", 1)), "at the cut: after it");
        assert_eq!(harness.shadow.best_at_line(&btc, &harness.replica, 3_150 * NS_PER_MS).1, harness.shadow.best_levels(&btc, &harness.replica).1);
    }

    /// Review 000652 r6 P2-3: the accounts a `SetGlobalAction` freezes are those of the orders it reaches where its line
    /// runs - before the TWAP suborder's cut, ahead of a cancel seen at the cut though received first -; at the cut,
    /// after that cancel.
    #[test]
    fn the_accounts_frozen_are_those_where_the_line_runs() {
        let mut harness = Harness::ready_from(1_000);
        harness.rest_ask(11);
        harness.open_stop(77, "A", None, true, None);
        drop(harness.shadow.on_anchor(twap_anchor(harness.height), Some(&harness.replica)));
        drop(harness.twaps_known_now());
        let cancel = ActionKind::Cancel { fast: false, cancels: vec![crate::accounts::replica::WireCancel { a: 0, o: 77 }] };
        let tx = MempoolTx { seen_ns: 3_981 * NS_PER_MS, tx_hash: hash(2), actions: vec![SignedAction { vault: Some(ACCOUNT), signer: None, expires_after_ms: None, priority: 0, kind: cancel }] };
        drop(harness.shadow.on_tx(&tx, false, Some(&coins()), Some(&harness.replica)));
        let (btc, mark_px) = (Coin::new(BTC), Px::parse_from_str("100").unwrap());
        let reached = |seen_ms: i64| harness.shadow.reached_accounts(&btc, Some(&harness.replica), mark_px, seen_ms * NS_PER_MS);
        assert_eq!((reached(3_979), reached(3_990)), (vec![ACCOUNT], vec![]));
    }

    /// Batch 2l: a `SetGlobalAction`'s line holds no book action - its forecast is `mark_forecast`'s alone -: no coin
    /// changes, nothing trades.
    #[test]
    fn a_set_global_action_line_leaves_the_book() {
        let mut harness = Harness::ready_from(1_000);
        harness.rest_ask(11);
        let before = harness.bbo();
        let btc = crate::accounts::marks::actions::SubmittedPrices::Priced { oracle: "101".to_string(), mark: "101".to_string() };
        let step = harness.line(3_150, 1, vec![ActionKind::SetGlobal(vec![btc])]);
        assert_eq!((step.trades.as_slice(), step.changed.len(), harness.bbo()), (&NO_TRADES[..], 0, before));
        assert_eq!(harness.shadow.held, 0);
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
        let pending = Maker::Pending { id: ActionId { tx_hash: hash(1), action: 0, item: 0 }, cloid: Some("0x0a".to_string()), account: None };
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
        TakerOutcome { key: TakerKey { identity: ActingIdentity::Signer(TAKER), asset: 0, is_buy: true }, reduce_only: false, refused: refused.then_some(Refusal::Margin) }
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
    pub(crate) fn twap_anchor(height: u64) -> Arc<crate::accounts::book::PendingAnchor> {
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
        Arc::new(crate::accounts::book::PendingAnchor { height, coins: coins(), twaps: vec![twap], waiting_twaps: Vec::new() })
    }

    fn twap_trades(step: &Step) -> Vec<(u64, u64, Sz, Px)> {
        step.trades
            .iter()
            .filter_map(|trade| match trade.taker {
                Taker::Twap { twap_id, slot, .. } => Some((twap_id, slot, trade.sz, trade.px)),
                Taker::Pending { .. } | Taker::Trigger { .. } => None,
            })
            .collect()
    }

    /// Batch 2h: a TWAP suborder due in the next first-of-second block takes from the book, its fill out once when it
    /// first fills; a block of its second whose next is not proven yet keeps it (batch 2i), and its being proven sends
    /// nothing again. An IOC whose line comes before the second's start - 20 ms runs before it (batch 2j).
    #[test]
    fn a_twap_suborder_trades_once() {
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
        assert_eq!(step.trades.iter().map(|trade| trade.sz).collect::<Vec<_>>(), [Sz::parse_from_str("1").unwrap()], "ahead of the suborder");
        assert_eq!(harness.bbo().1, None);
        let step = harness.confirm(vec![], vec![]);
        assert!(harness.shadow.twaps_known(), "kept through its second");
        assert_eq!(twap_trades(&step), []);
        let step = harness.twaps_known_now();
        assert!(harness.shadow.twaps_known());
        assert_eq!(twap_trades(&step), [], "slot 0 went out already");
    }

    /// Batch 2j: a taker held, its line before the second's start - 20 ms (3,980 ms), runs before the suborder - the
    /// suborder takes nothing -; one seen at the cut, or after, runs after it.
    #[test]
    fn a_taker_seen_before_the_cut_runs_ahead_of_the_suborder() {
        for (seen_ms, suborder) in [(3_150, None), (3_979, None), (3_980, Some("0.1"))] {
            let mut harness = Harness::ready_from(1_000);
            harness.rest_ask(11);
            drop(harness.shadow.on_anchor(twap_anchor(harness.height), Some(&harness.replica)));
            let step = harness.line(seen_ms, 1, vec![ioc("0x01")]);
            assert_eq!(step.trades.iter().map(|trade| trade.sz).collect::<Vec<_>>(), [Sz::parse_from_str("1").unwrap()], "seen at {seen_ms}");
            let step = harness.twaps_known_now();
            let expected: Vec<_> = suborder.map(|sz| (7, 0, Sz::parse_from_str(sz).unwrap(), Px::parse_from_str("101").unwrap())).into_iter().collect();
            assert_eq!(twap_trades(&step), expected, "seen at {seen_ms}");
        }
    }

    /// Review 000634 r2: a line taking on BTC, ETH, then BTC again - BTC holding a suborder, its line before the cut -
    /// sends its trades in that order, though its coins are held a coin at a time.
    #[test]
    fn a_lines_trades_keep_its_order_across_coins() {
        let mut harness = Harness::ready_from(1_000);
        let at = block_time_ms(harness.height + 1);
        let eth = |mut event: serde_json::Value, path: &[&str]| {
            let mut at = &mut event;
            for key in path {
                at = &mut at[*key];
            }
            *at = serde_json::json!("ETH");
            event
        };
        harness.block(
            &[],
            vec![btc_status(11, "A", "101.0", "1.0", "open", None, "Alo", at), btc_status(13, "A", "101.0", "1.0", "open", None, "Alo", at), eth(btc_status(12, "A", "3000.0", "1.0", "open", None, "Alo", at), &["order", "coin"])],
            vec![btc_diff(11, "A", "101.0", new_diff("1.0")), btc_diff(13, "A", "101.0", new_diff("1.0")), eth(btc_diff(12, "A", "3000.0", new_diff("1.0")), &["coin"])],
        );
        drop(harness.shadow.on_anchor(twap_anchor(harness.height), Some(&harness.replica)));
        drop(harness.twaps_known_now());
        let eth_ioc = ActionKind::Order(vec![requested(1, true, "3000", RequestedTif::Ioc, Some("0x0e"))]);
        let step = harness.line(3_150, 1, vec![ioc("0x01"), eth_ioc, ioc("0x02")]);
        let coins: Vec<_> = step.trades.iter().map(|trade| (trade.coin.value(), matches!(trade.taker, Taker::Twap { .. }))).collect();
        let expected = [("BTC", false), ("ETH", false), ("BTC", false)].map(|(coin, twap)| (coin.to_string(), twap));
        assert_eq!(coins, expected);
    }

    /// Review 000637 r2 P2: the book 99 / 101; a line taken while no replica is aligned is not judged then - it is once
    /// one is, before any overlay applies it: its signed action selling at 4 never rests, another of the line does, and
    /// an IOC buying at 101 after takes the ask 101.
    #[test]
    fn a_line_held_with_no_replica_is_judged_once_one_is_aligned() {
        let mut harness = Harness::ready_from(1_000);
        let at = block_time_ms(harness.height + 1);
        harness.block(
            &[],
            vec![btc_status(21, "B", "99.0", "1.0", "open", None, "Alo", at), btc_status(11, "A", "101.0", "1.0", "open", None, "Alo", at)],
            vec![btc_diff(21, "B", "99.0", new_diff("1.0")), btc_diff(11, "A", "101.0", new_diff("1.0"))],
        );
        harness.shadow.on_replica_gone();
        let signed = |kind| SignedAction { vault: None, signer: None, expires_after_ms: None, priority: 0, kind };
        let banded = ActionKind::Order(vec![requested(0, false, "4", RequestedTif::Gtc, Some("0x0a")), requested(0, false, "103", RequestedTif::Alo, Some("0x0c"))]);
        let tx = MempoolTx { seen_ns: 3_150 * NS_PER_MS, tx_hash: hash(1), actions: vec![signed(banded), signed(alo_ask("102", "0x0b"))] };
        drop(harness.shadow.on_tx(&tx, false, Some(&coins()), None));
        assert_eq!(harness.shadow.held(BTC).len(), 3, "held, not judged");
        drop(harness.block(&[], vec![], vec![]));
        assert_eq!(harness.shadow.held(BTC), [(1, false)], "the action selling at 4 judged out whole, its ALO at 103 too; the ALO at 102 kept");
        assert_eq!((harness.shadow.band_unjudged.len(), harness.shadow.band_unjudged_slots), (0, 0));
        assert_eq!(harness.bbo(), (Some(level("99", "1", 1)), Some(level("101", "1", 1))));
        assert_eq!(harness.available(), Ok(()));
        let step = harness.line(3_250, 2, vec![ioc("0x02")]);
        assert_eq!(step.trades.iter().map(|trade| trade.px.to_str()).collect::<Vec<_>>(), ["101"]);
    }

    /// Review 000637 r3 P2-1: the ask 101 alone - a signed action selling 100,000,000 at 4 (under the band, and too large:
    /// refused one by one before the book) with an IOC buying at 101 leaves the book as it was, taken with a replica
    /// aligned or judged after one held with none.
    #[test]
    fn an_action_too_large_and_under_the_band_is_judged_whole_either_way() {
        let large = || crate::pending::mempool::RequestedOrder {
            sz: Sz::parse_from_str("100000000").unwrap(),
            sz_shape: crate::pending::mempool::DecimalShape::of("100000000"),
            ..requested(0, false, "4", RequestedTif::Gtc, Some("0x0a"))
        };
        let tx = |hash_byte: u8| MempoolTx {
            seen_ns: 3_150 * NS_PER_MS,
            tx_hash: hash(hash_byte),
            actions: vec![SignedAction { vault: None, signer: None, expires_after_ms: None, priority: 0, kind: ActionKind::Order(vec![large(), requested(0, true, "101", RequestedTif::Ioc, Some("0x0b"))]) }],
        };
        for aligned in [true, false] {
            let mut harness = Harness::ready_from(1_000);
            harness.rest_ask(11);
            if aligned {
                drop(harness.shadow.on_tx(&tx(1), false, Some(&coins()), Some(&harness.replica)));
            } else {
                harness.shadow.on_replica_gone();
                drop(harness.shadow.on_tx(&tx(1), false, Some(&coins()), None));
                drop(harness.block(&[], vec![], vec![]));
            }
            assert_eq!((harness.shadow.held(BTC), harness.bbo().1), (Vec::new(), Some(level("101", "1", 1))), "aligned {aligned}");
        }
    }

    /// Review 000637 r3 P2-2, r4: what waits unjudged with no replica stays within the set's cap - 3 here -, its slots
    /// counted with the actions held: lines of an action of 100 sells too large (each refused by itself, all read by the
    /// band) on BTC and ETH and an ALO kept, the set dropped over and over, the slots each coin's lowest limit alone.
    #[test]
    fn unjudged_actions_stay_within_the_cap() {
        let mut harness = Harness::ready_with(PendingParams::from_ms(1_000, 1_000, 10_000, 3, 60_000, 300_000, 1_000).unwrap(), 1_000);
        harness.shadow.on_replica_gone();
        let large = |asset: u32, px: &str| crate::pending::mempool::RequestedOrder {
            sz: Sz::parse_from_str("100000000").unwrap(),
            sz_shape: crate::pending::mempool::DecimalShape::of("100000000"),
            ..requested(asset, false, px, RequestedTif::Gtc, None)
        };
        for byte in 1..=30 {
            let mut orders: Vec<_> = (0..50).flat_map(|_| [large(0, "4"), large(1, "40")]).collect();
            orders.push(requested(0, false, "102", RequestedTif::Alo, Some("0x0a")));
            let kind = ActionKind::Order(orders);
            let tx = MempoolTx { seen_ns: 3_150 * NS_PER_MS, tx_hash: hash(byte), actions: vec![SignedAction { vault: None, signer: None, expires_after_ms: None, priority: 0, kind }] };
            drop(harness.shadow.on_tx(&tx, false, Some(&coins()), None));
            let slots: usize = harness.shadow.band_unjudged.values().map(Vec::len).sum();
            assert_eq!(slots, harness.shadow.band_unjudged_slots, "line {byte}");
            assert!(harness.shadow.held + slots <= 3, "line {byte}");
            assert!(harness.shadow.band_unjudged.values().all(|lowest| lowest.len() <= 2), "a coin's lowest alone");
            if byte == 1 {
                assert_eq!((harness.shadow.held, slots), (1, 2), "the first line kept: its ALO, BTC's lowest and ETH's");
            }
        }
    }

    /// Review 000637 r1 P2-1: the ask 101 alone - a signed action buying at 4 (under 5.05, the band of its one side)
    /// and at 101 is refused whole, the book left; another signed action of the line takes the ask.
    #[test]
    fn an_action_under_the_price_band_trades_nothing() {
        let mut harness = Harness::ready_from(1_000);
        harness.rest_ask(11);
        let banded = || ActionKind::Order(vec![requested(0, true, "4", RequestedTif::Ioc, Some("0x0a")), requested(0, true, "101", RequestedTif::Ioc, Some("0x0b"))]);
        let step = harness.line(3_150, 1, vec![banded()]);
        assert_eq!((step.trades.len(), harness.bbo().1), (0, Some(level("101", "1", 1))));
        let step = harness.line(3_160, 2, vec![banded(), ioc("0x02")]);
        let cloids: Vec<_> = step.trades.iter().map(|trade| match &trade.taker {
            Taker::Pending { cloid, .. } => cloid.clone(),
            Taker::Twap { .. } | Taker::Trigger { .. } => None,
        }).collect();
        assert_eq!(cloids, [Some("0x02".to_string())]);
        assert_eq!(harness.bbo().1, None);
    }

    /// Review 000634 r3 P2-1: with no suborder due, a replay keeps the actions as they came - an ask seen at 3,981 ms,
    /// then an IOC seen at 3,979 ms taking it: the next block's replay leaves the ask taken.
    #[test]
    fn with_no_suborder_a_replay_keeps_the_actions_as_they_came() {
        let mut harness = Harness::ready_from(1_000);
        drop(harness.line(3_981, 1, vec![alo_ask("100", "0x0a")]));
        let step = harness.line(3_979, 2, vec![ioc("0x01")]);
        assert_eq!(step.trades.iter().map(|trade| (trade.px.to_str(), trade.sz.to_str())).collect::<Vec<_>>(), [("100".to_string(), "1".to_string())]);
        assert_eq!(harness.bbo().1, None);
        let at = block_time_ms(harness.height + 1);
        drop(harness.block(&[], vec![btc_status(21, "B", "90.0", "1.0", "open", None, "Alo", at)], vec![btc_diff(21, "B", "90.0", new_diff("1.0"))]));
        assert_eq!(harness.bbo(), (Some(level("90", "1", 1)), None), "the ask stays taken");
    }

    /// Batch 2j: a line before the cut on a coin holding a suborder sends its own trades, then the suborder's; a later
    /// line replaying the coin sends its own alone, none held before it again.
    #[test]
    fn a_replayed_line_sends_its_own_trades_then_the_suborders() {
        let mut harness = Harness::ready_from(1_000);
        drop(harness.shadow.on_anchor(twap_anchor(harness.height), Some(&harness.replica)));
        drop(harness.twaps_known_now());
        let taker = |trade: &PendingTrade| (matches!(trade.taker, Taker::Twap { .. }), trade.px.to_str(), trade.sz.to_str());
        let step = harness.line(3_150, 1, vec![alo_ask("100", "0x0a"), alo_ask("101", "0x0b"), ioc("0x01")]);
        let expected = [(false, "100", "1"), (true, "101", "0.1")].map(|(twap, px, sz)| (twap, px.to_string(), sz.to_string()));
        assert_eq!(step.trades.iter().map(taker).collect::<Vec<_>>(), expected);
        let step = harness.line(3_160, 2, vec![ioc("0x02")]);
        assert_eq!(step.trades.iter().map(taker).collect::<Vec<_>>(), [(false, "101".to_string(), "1".to_string())]);
    }

    /// Review 000634 r1 P2-1: the window open, a buy suborder not out, no ask; one line before the cut rests asks at 101
    /// then 100, each enough - the suborder's trade goes out once, at 100, as the book shows it, and no replay after
    /// sends it again.
    #[test]
    fn a_line_before_the_cut_is_held_whole_before_the_suborder_trades() {
        let mut harness = Harness::ready_from(1_000);
        drop(harness.shadow.on_anchor(twap_anchor(harness.height), Some(&harness.replica)));
        assert_eq!(twap_trades(&harness.twaps_known_now()), [], "no ask to take");
        let step = harness.line(3_150, 1, vec![alo_ask("101", "0x0a"), alo_ask("100", "0x0b")]);
        assert_eq!(twap_trades(&step), [(7, 0, Sz::parse_from_str("0.1").unwrap(), Px::parse_from_str("100").unwrap())]);
        assert_eq!(harness.bbo().1, Some(level("100", "0.9", 1)));
        let replays: Vec<_> = [harness.confirm(vec![], vec![]), harness.twaps_known_now()].iter().flat_map(twap_trades).collect();
        assert_eq!(replays, []);
    }

    /// Review 000662 r6 P2: a step's trades in the order its runs take them, the cut at 980 ms (confirmed block time 0):
    /// the actions seen before it as they came, the suborders by due time and TWAP id - TWAP 8's two fills as they came
    /// -, then the actions at or after it.
    #[test]
    fn a_steps_trades_are_ordered_as_its_runs_take_them() {
        let shadow = Shadow::new(params(), true, 0);
        let trade = |coin: &str, px: &str, taker: Taker| PendingTrade {
            coin: Coin::new(coin), side: Side::Bid, px: Px::parse_from_str(px).unwrap(), sz: Sz::parse_from_str("1").unwrap(), maker: Maker::Unattributed, taker,
        };
        let action = |coin: &str, seen_ms: i64, byte: u8| {
            trade(coin, "101", Taker::Pending { id: ActionId { tx_hash: hash(byte), action: 0, item: 0 }, seen_ns: seen_ms * NS_PER_MS, cloid: None, vault: None })
        };
        let suborder = |twap_id: u64, due_ms: u64, px: &str| trade(BTC, px, Taker::Twap { twap_id, user: TAKER, slot: 0, due_ms });
        let mut trades = vec![
            suborder(8, 1_000, "101"), action("ETH", 990, 1), action(BTC, 970, 2), suborder(7, 1_000, "101"), suborder(8, 1_000, "102"),
            suborder(6, 2_000, "101"), action("ETH", 975, 3), action(BTC, 980, 4),
        ];
        shadow.order_trades(&mut trades);
        let order: Vec<String> = trades
            .iter()
            .map(|trade| match &trade.taker {
                Taker::Pending { seen_ns, .. } => format!("{} {}", trade.coin.value(), seen_ns / NS_PER_MS),
                Taker::Twap { twap_id, .. } => format!("twap {twap_id} @ {}", trade.px.to_str()),
                Taker::Trigger { .. } => unreachable!(),
            })
            .collect();
        assert_eq!(order, ["BTC 970", "ETH 975", "twap 7 @ 101", "twap 8 @ 101", "twap 8 @ 102", "twap 6 @ 101", "ETH 990", "BTC 980"]);
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
        Arc::make_mut(&mut anchor).twaps.clear();
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
            shadow.on_block(LoggedBlock { height, time_ms: height * 100, hashes: Vec::new(), priorities: Vec::new(), taker_outcomes: Vec::new(), signer_accounts: Vec::new(), twap_cancels: Vec::new(), set_global: false, read_at: std::time::Instant::now() }, false, None);
        }
        assert!(!shadow.takes_blocks(None));
    }
}
