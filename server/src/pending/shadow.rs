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
use rustc_hash::FxHashMap;

use super::{
    actions::{ActionId, PendingAction, Placement, book_actions},
    blocks::BlockHashes,
    mempool::MempoolTx,
    overlay::{BestLevel, CoinBase, Fill, Maker, Overlay},
    record::{BlockRecord, MAX_LEAD_BLOCKS},
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
}

impl PendingParams {
    /// From milliseconds, each positive.
    pub(crate) fn from_ms(expiry_ms: u64, max_late_ms: u64, max_span_ms: u64, max_actions: usize, silence_ms: u64) -> Result<Self, String> {
        let ns = |name: &str, ms: u64| -> Result<i64, String> {
            if ms == 0 {
                return Err(format!("--pending-{name}-ms must be positive"));
            }
            i64::try_from(ms).ok().and_then(|ms| ms.checked_mul(NS_PER_MS)).ok_or_else(|| format!("--pending-{name}-ms {ms} too large"))
        };
        if max_actions == 0 {
            return Err("--pending-max-actions must be positive".to_string());
        }
        Ok(Self {
            expiry_ns: ns("expiry", expiry_ms)?,
            max_late_ns: ns("max-late", max_late_ms)?,
            max_span_ns: ns("max-span", max_span_ms)?,
            max_actions,
            silence_ns: ns("mempool-silence", silence_ms)?,
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
}

impl Unavailable {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Replica => "replica",
            Self::Mempool(cause) | Self::BlockLog(cause) => cause,
            Self::BlockLogBehind => "block_log_behind",
            Self::WarmingUp => "warming_up",
            Self::Span => "span",
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
    /// The taker's line's `local_time`, unix ns.
    pub(crate) seen_ns: i64,
    pub(crate) maker: Maker,
    pub(crate) taker: ActionId,
    pub(crate) taker_cloid: Option<String>,
    pub(crate) vault: Option<Address>,
}

/// What a step changed: the trades predicted, and the coins whose merged book may have moved.
#[derive(Debug, Default)]
pub(crate) struct Step {
    pub(crate) trades: Vec<PendingTrade>,
    pub(crate) changed: HashSet<Coin>,
}

/// A coin's held actions in execution order, and their overlay.
#[derive(Default)]
struct CoinActions {
    actions: Vec<PendingAction>,
    overlay: Overlay,
}

impl CoinActions {
    fn replay(&mut self, base: CoinBase<'_>) {
        self.overlay = Overlay::default();
        for action in self.actions.iter().filter(|action| !action.unknown) {
            self.overlay.apply(base, action);
        }
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
    gaps: Gaps,
    held: usize,
    newest_seen_ns: Option<i64>,
    /// Settling steps taken with a replica aligned.
    #[cfg(test)]
    settles: usize,
}

impl Shadow {
    /// Every input in a gap until each has begun.
    pub(crate) fn new(params: PendingParams, spot: bool) -> Self {
        Self {
            params,
            spot,
            coins: FxHashMap::default(),
            lines: FxHashMap::default(),
            record: BlockRecord::default(),
            gaps: Gaps {
                mempool: Some("start"),
                block_log: Some("block_log_start"),
                replica: true,
                warm_until_ns: i64::MIN,
                ready_from_ns: i64::MIN,
            },
            held: 0,
            newest_seen_ns: None,
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
        let actions = book_actions(tx, coins, self.spot);
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

    /// A block of the log; `after_loss` when the watcher had found a loss not yet reported.
    pub(crate) fn on_block(&mut self, block: BlockHashes, after_loss: bool, replica: Option<&BookReplica>) -> Step {
        if !self.record.follows(&block) {
            self.open_block_log_gap("block_log_hole");
        }
        if after_loss {
            self.open_block_log_gap("block_log_after_loss");
        } else {
            self.end_block_log_gap(unix_ns(block.time_ms));
        }
        self.record.add(block);
        self.settle(replica, HashSet::new())
    }

    /// The replica applied batches, changing `changed`'s books.
    pub(crate) fn on_applied(&mut self, changed: HashSet<Coin>, replica: &BookReplica) -> Step {
        self.settle(Some(replica), changed)
    }

    /// The mempool lost lines.
    pub(crate) fn on_mempool_loss(&mut self) {
        self.open_mempool_gap("loss");
    }

    /// The block log lost lines.
    pub(crate) fn on_block_log_loss(&mut self) {
        self.open_block_log_gap("block_log_loss");
    }

    /// The replica went; another generation follows. The overlays wait for it.
    pub(crate) fn on_replica_gone(&mut self) {
        if self.gaps.replica {
            return;
        }
        PENDING_BOOK_GAPS_TOTAL.with_label_values(&["replica"]).inc();
        self.gaps.replica = true;
        self.coins.values_mut().for_each(|actions| actions.overlay = Overlay::default());
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
        if self.record.newest().is_none_or(|newest| newest < height) {
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
        let base = aligned.filter(|_| !self.gaps.replica).map(|(replica, _, _)| replica.coin(&coin));
        let actions = self.coins.entry(coin.clone()).or_default();
        if let (false, Some(base)) = (action.unknown, base) {
            let effect = actions.overlay.apply(base, &action);
            PENDING_BOOK_OVERLAY_TOTAL.with_label_values(&[action.op.label(), effect.outcome]).inc();
            // only an action placing an order fills
            if let (true, Some(order)) = (emit, action.op.placement()) {
                for fill in &effect.fills {
                    PENDING_BOOK_PREDICTED_FILLS_TOTAL.with_label_values(&[fill.maker.label()]).inc();
                }
                step.trades.extend(effect.fills.into_iter().map(|fill| predicted(&coin, &action, order, fill)));
            }
            step.changed.insert(coin);
        }
        actions.actions.push(action);
    }

    /// The one settling step (r7 revision 18): the lines a block at or below the confirmed height holds, and those
    /// expired, leave; the coins they were on, those in `changed` and - on a new generation - every coin held are
    /// replayed.
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
        if new_generation {
            changed.extend(self.coins.keys().cloned());
        }
        for coin in &changed {
            if let Some(actions) = self.coins.get_mut(coin) {
                actions.replay(replica.coin(coin));
            }
        }
        Step { trades: Vec::new(), changed }
    }

    /// The lines a block at or below `height` holds, and those expired, out of the set; their coins into `changed`.
    fn remove_settled(&mut self, height: u64, confirmed_ns: i64, changed: &mut HashSet<Coin>) {
        let Self { lines, record, params, coins, held, .. } = self;
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
            !actions.actions.is_empty()
        });
    }

    fn open_mempool_gap(&mut self, cause: &'static str) {
        if self.gaps.mempool.is_some() {
            return;
        }
        PENDING_BOOK_GAPS_TOTAL.with_label_values(&[cause]).inc();
        self.gaps.mempool = Some(cause);
    }

    /// A block log gap: every action held unknown, every overlay empty.
    fn open_block_log_gap(&mut self, cause: &'static str) {
        if self.gaps.block_log.is_some() {
            return;
        }
        PENDING_BOOK_GAPS_TOTAL.with_label_values(&[cause]).inc();
        self.gaps.block_log = Some(cause);
        for actions in self.coins.values_mut() {
            actions.actions.iter_mut().for_each(|action| action.unknown = true);
            actions.overlay = Overlay::default();
        }
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

fn predicted(coin: &Coin, action: &PendingAction, order: &Placement, fill: Fill) -> PendingTrade {
    PendingTrade {
        coin: coin.clone(),
        side: order.side,
        px: fill.px,
        sz: fill.sz,
        seen_ns: action.seen_ns,
        maker: fill.maker,
        taker: action.id,
        taker_cloid: order.cloid.clone(),
        vault: action.vault,
    }
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
        },
    };

    /// E 1 s, H 1 s, the span 10 s, 1,000 actions; silence 60 s, out of the way unless a test sets it.
    pub(crate) fn params() -> PendingParams {
        PendingParams::from_ms(1_000, 1_000, 10_000, 1_000, 60_000).unwrap()
    }

    /// As `params`, H as r6 revision 17's default.
    fn params_late(max_late_ms: u64) -> PendingParams {
        PendingParams::from_ms(1_000, max_late_ms, 10_000, 1_000, 60_000).unwrap()
    }

    const BTC: &str = "BTC";
    const NONE_HELD: [(usize, bool); 0] = [];
    pub(crate) const NO_TRADES: [PendingTrade; 0] = [];

    type Events = Vec<serde_json::Value>;

    /// The replica (blocks at height x 100 ms), the block log and the pending set over them.
    struct Harness {
        replica: BookReplica,
        shadow: Shadow,
        /// The last height the replica confirmed.
        height: u64,
        /// The last height the log showed.
        logged: u64,
    }

    impl Harness {
        fn new(params: PendingParams) -> Self {
            Self { replica: replica_of_empty_book(), shadow: Shadow::new(params, true), height: 0, logged: 0 }
        }

        /// The log's next block, holding the lines of `hashes`.
        fn log(&mut self, hashes: &[u8]) -> Step {
            self.logged += 1;
            let block = BlockHashes { height: self.logged, time_ms: block_time_ms(self.logged), hashes: hashes.iter().map(|byte| hash(*byte)).collect() };
            self.shadow.on_block(block, false, Some(&self.replica))
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
            let tx = MempoolTx {
                seen_ns: seen_ms * NS_PER_MS,
                tx_hash: hash(byte),
                actions: actions.into_iter().map(|kind| SignedAction { vault: None, kind }).collect(),
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
        assert_eq!((trade.side, trade.sz, trade.taker_cloid.as_deref()), (Side::Bid, Sz::parse_from_str("1").unwrap(), Some("0x01")));
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
        harness.shadow.on_block_log_loss();
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
        let mut shadow = Shadow::new(params(), false);
        let tx = MempoolTx {
            seen_ns: 1_000 * NS_PER_MS,
            tx_hash: hash(1),
            actions: vec![SignedAction { vault: None, kind: ActionKind::Order(vec![requested(10_107, true, "100", RequestedTif::Alo, Some("0x0a"))]) }],
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
        let mut harness = Harness::new(PendingParams::from_ms(1_000, 1_000, 10_000, 1_000, 500).unwrap());
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
        let mut harness = Harness::ready_with(PendingParams::from_ms(1_000, 1_000, 10_000, 1, 60_000).unwrap(), 1_000);
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

    /// R7 revision 20: past `MAX_LEAD_BLOCKS` above the confirmed height (all of them without a replica) the log's
    /// blocks are not taken.
    #[test]
    fn the_block_log_lead_is_capped() {
        let mut shadow = Shadow::new(params(), true);
        for height in 1..=MAX_LEAD_BLOCKS {
            assert!(shadow.takes_blocks(None));
            shadow.on_block(BlockHashes { height, time_ms: height * 100, hashes: Vec::new() }, false, None);
        }
        assert!(!shadow.takes_blocks(None));
    }
}
