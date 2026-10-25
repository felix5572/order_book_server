//! Where inside its block each price action runs (design 000439 r1 section 3; port of the research
//! `execution_point_replay.py`, exact for every `SetGlobalAction` and reference action of three intervals).
//!
//! A block's diff lines are in execution order. For a price action P, the other book actions of the block anchor the
//! lines they produced before or after P: the orders they created, the orders they cancelled (a modify's old order
//! included), and, fill by fill, the resting orders their takers hit (fills pair by tid; TWAP slices run before every
//! action). In a block with one
//! `SetGlobalAction`, the orders it triggered run inside it, after its selector. The anchors bound "lines before P" to an
//! interval [lo, hi]; P reads the book at `lo` (the start, research method 3) and the last trade before its fills cut.
//! An anchor of a later action before one of an earlier action contradicts the line order: the block breaks the
//! contract.
//!
//! A trigger order's placement is not its activation. Only a main-dex trigger with exactly one `SetGlobalAction`
//! is placed by the rule above. HIP-3 triggers, and native triggers with zero/multiple `SetGlobalAction`s, stay
//! unplaced instead of falling back to their initial order's rank (bm xyz:SP500 1191957330).
//!
//! A no-fill IOC emits its `iocCancelRejected` status followed by its self-trade cancels. `self_trade` preserves that
//! run and matches the taker one-to-one to the exact rejected request. This also places old resting orders and
//! cross-vault cancels (bm ZETA 1191392472, APE 1191420380, PROVE 1191436351); it does not guess vault ownership.
//!
//! Successful IOC runs (full or partial fills) use `filled_ioc`: matching open/filled endpoints, receipt oid and actual
//! taker/maker fills place their trailing self-trade cancels, including old cross-vault orders. Interrupted or
//! competing runs remain unplaced. BM PROVE 1192106175 and PURR 1192117608 otherwise sampled a removed ask.
//!
//! For cancellations without that evidence, the earlier conservative attribution remains:
//! A self-trade cancel (`selfTradeCanceled`) removes a resting order inside the order that would have traded with it.
//! Its remove anchors to a book action only when that action is the one possible cause:
//! - the removed order was created in the block, by a book action: one resting before it could have met a block-start
//!   taker the marks do not model, a TWAP slice (review 000452 r2);
//! - the one book action after that creation of that account submitting an order that may take (a limit order but
//!   ALO) on that coin's asset, the opposite side and a limit reaching the removed order, whatever its status, while
//!   the creating action itself submits no such order (a batch's taker may remove its own sibling, review 000452 r3);
//! - no order of the block's `triggered` statuses of that account, coin and opposite side reaches it (a triggered order
//!   runs inside the `SetGlobalAction`, review 000452 r1).
//!
//! bm block 1191373718: the account's ALO batch created a RUNE sell, then its IOC buy of the same bundle, with an order
//! error and no fill, removed it before the `SetGlobalAction` of the next bundle (the ALO batch also bought RUNE: an ALO
//! takes nothing; research `rune_1191370000_1191380000`). Otherwise the remove anchors nothing, as before.
//!
//! A reference by cloid names the order holding the cloid when the action runs: a modify keeps the cloid on its new
//! order, so one cloid can name several orders in a block (bm block 1190808326: a batchModify in bundle 0 and another in
//! bundle 1 of the same cloid; resolving through the last cancelled status, as the research did, placed bundle 1's
//! remove before the price action between them). In execution order: the latest order of the block created with the
//! cloid, else the cloid's first `canceled` status (the order resting before the block).
//!
//! A maker's fills match its `update` lines by the size they removed, in order: an order can shrink without a fill (a
//! reduce-only order shrinking with its position: bm block 1190821813, a ZRO order went 33.6 → 24.9 → 12.9 → 6.0 → 0.2
//! with no fill and then 0.2 → 0 with its one fill; the research's "k-th update is the k-th fill" put the first shrink on
//! that fill, after the price action, and the block contradicted itself). A shrink may remove a fill's size too (review
//! 000444 r1 P2-2), so an update is a fill's only when every order-preserving size match puts that fill on it; any other
//! update anchors nothing.

pub(crate) mod fill_order;
mod self_trade;
mod filled_ioc;
mod resting_reduce_only;
mod vault;
mod replacement;
mod action_position;

use self::self_trade::IocRejections;
use alloy::primitives::Address;
use rustc_hash::{FxHashMap, FxHashSet};

use super::{
    actions::{MarkAction, MarkActions, Rank, RankedAction, Taker},
    book::{BestPrices, BookDiff, DiffKind},
};
use super::decimal::ExactDecimal;
use crate::{
    order_book::Side as OrderSide,
    types::node_data::{NodeDataFill, NodeDataOrderStatus},
};

/// A fill of the block as the anchors read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlockFill {
    pub(crate) coin: Box<str>,
    pub(crate) px: Box<str>,
    pub(crate) sz: Box<str>,
    /// The taker's leg.
    pub(crate) crossed: bool,
    pub(crate) oid: u64,
    pub(crate) tid: u64,
    /// Optional node L1 hash locator; zero/system/unknown encodings prove no ordinary action.
    pub(crate) action_position: Option<action_position::L1ActionPosition>,
    /// A TWAP slice: it runs at the start of the block.
    pub(crate) twap: bool,
    /// Node-reported mark for this market liquidation, shared by its maker and taker legs.
    pub(crate) liquidation_mark_px: Option<Box<str>>,
}

impl BlockFill {
    pub(crate) fn of(NodeDataFill(_, fill): &NodeDataFill) -> Self {
        Self {
            coin: fill.coin.as_str().into(),
            px: fill.px.as_str().into(),
            sz: fill.sz.as_str().into(),
            crossed: fill.crossed,
            oid: fill.oid,
            tid: fill.tid,
            action_position: action_position::L1ActionPosition::of(&fill.hash),
            twap: fill.twap_id.is_some(),
            liquidation_mark_px: fill.liquidation.as_ref().filter(|v| v.method == "market")
                .map(|v| v.mark_px.as_str().into()),
        }
    }
}

/// What the block's order statuses tell the anchors.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BlockStatuses {
    /// The first `canceled` status of each (user, cloid): the order that held the cloid before the block.
    pub(crate) cancelled_cloids: FxHashMap<(Address, String), u64>,
    /// `triggered` statuses.
    pub(crate) triggered: FxHashSet<u64>,
    /// The `triggered` statuses' orders: a possible cause of a self-trade cancel.
    pub(crate) triggered_orders: Vec<StatusOrder>,
    /// Fixed thresholds for price attribution; duplicate or unknown statuses retain no proof.
    pub(crate) trigger_prices: FxHashMap<u64, Option<fill_order::TriggerPrice>>,
    /// `selfTradeCanceled` statuses.
    pub(crate) self_trade_cancelled: Vec<StatusOrder>,
    /// No-fill IOC statuses and their contiguous self-trade cancellations, in stream order.
    rejected_iocs: IocRejections,
    /// Completed IOC status runs with explicit receipt and fill evidence.
    filled_iocs: filled_ioc::FilledIocs,
    /// No-fill resting reduce-only placements and their contiguous old-order cleanup.
    resting_reduce_only: resting_reduce_only::RestingReduceOnly,
    /// Automatic close orders produced by vault withdrawals, not ordinary order receipts.
    vault_closes: vault::VaultCloses,
    replacements: replacement::Replacements,
    interleaved_triggers: fill_order::InterleavedTriggers,
}

/// An order as a status names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatusOrder {
    pub(crate) user: Address,
    pub(crate) coin: Box<str>,
    pub(crate) side: OrderSide,
    pub(crate) limit_px: ExactDecimal,
    pub(crate) oid: u64,
}

impl StatusOrder {
    fn of(status: &NodeDataOrderStatus) -> Result<Self, String> {
        let order = &status.order;
        let limit_px = ExactDecimal::parse(&order.limit_px).map_err(|err| format!("{} status of oid {}: limit price {err}", status.status, order.oid))?;
        Ok(Self { user: status.user, coin: order.coin.as_str().into(), side: order.side, limit_px, oid: order.oid })
    }

    /// Whether an order of the other side at `limit_px` reaches this resting order.
    fn reached_by(&self, limit_px: ExactDecimal) -> bool {
        match self.side {
            OrderSide::Ask => limit_px >= self.limit_px,
            OrderSide::Bid => limit_px <= self.limit_px,
        }
    }
}

impl BlockStatuses {
    /// A status whose limit price does not read breaks the contract.
    pub(crate) fn of(statuses: impl IntoIterator<Item = NodeDataOrderStatus>) -> Result<Self, String> {
        let mut out = Self::default();
        let mut resting_reduce_only = resting_reduce_only::StatusRuns::default();
        for status in statuses {
            out.rejected_iocs.observe(&status)?;
            out.filled_iocs.observe(&status)?;
            resting_reduce_only.observe(&status)?;
            out.vault_closes.observe(&status);
            out.replacements.observe(&status)?;
            out.interleaved_triggers.observe(&status);
            match (status.status.as_str(), &status.order.cloid) {
                ("canceled", Some(cloid)) => {
                    out.cancelled_cloids.entry((status.user, cloid.clone())).or_insert(status.order.oid);
                }
                ("triggered", _) => {
                    out.triggered.insert(status.order.oid);
                    out.trigger_prices.entry(status.order.oid).and_modify(|price| *price = None)
                        .or_insert_with(|| fill_order::TriggerPrice::of(&status));
                    out.triggered_orders.push(StatusOrder::of(&status)?);
                }
                ("selfTradeCanceled", _) => out.self_trade_cancelled.push(StatusOrder::of(&status)?),
                _ => {}
            }
        }
        out.resting_reduce_only = resting_reduce_only.finish();
        Ok(out)
    }

    pub(crate) fn replacement_orders(&self, actions: &MarkActions, asset: &impl Fn(&str) -> Option<u32>,
        system_claims: &impl Fn(u64) -> bool) -> Vec<(Rank, u64)> {
        self.replacements.attributed(actions, asset, system_claims)
    }

    pub(crate) fn interleaved_trigger_ranks(&self, actions: &MarkActions) -> FxHashMap<u64, Rank> {
        self.interleaved_triggers.attributed(actions, &self.triggered)
    }

    pub(crate) fn system_ioc_cancels(&self, oid: u64) -> Vec<u64> {
        self.rejected_iocs.cancels_for(oid)
    }

    pub(crate) fn len(&self) -> usize {
        self.cancelled_cloids.len() + self.triggered.len() + self.self_trade_cancelled.len() + self.rejected_iocs.len() + self.vault_closes.len() + self.replacements.len() + self.filled_iocs.len() + self.interleaved_triggers.len() + self.resting_reduce_only.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Before,
    After,
}

/// The cut of a price action: diff lines `[0, lo)` and fills `[0, fills_lo)` ran before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Cut {
    pub(crate) lo: usize,
    pub(crate) hi: usize,
    pub(crate) fills_lo: usize,
}

/// One price action of the block, in execution order, with its cut.
#[derive(Debug)]
pub(crate) struct PricePoint<'b> {
    pub(crate) action: &'b RankedAction,
    pub(crate) cut: Cut,
    /// Fills at or above this index certainly run after the price action. Between `fills_lo` and this bound, either order is possible.
    pub(crate) fills_hi: usize,
}

/// The price actions of a block in execution order, each with its cut; `order_asset` gives a coin's asset id as an
/// order names it (`None` for a market the marks cannot name).
#[cfg(test)]
pub(crate) fn price_points<'b>(
    actions: &'b MarkActions,
    statuses: &BlockStatuses,
    diffs: &[BookDiff],
    fills: &[BlockFill],
    order_asset: impl Fn(&str) -> Option<u32>,
) -> Result<Vec<PricePoint<'b>>, String> {
    price_points_with_system(actions, statuses, diffs, fills, &super::system::SystemAnchors::default(), order_asset)
}

pub(crate) fn price_points_with_system<'b>(
    actions: &'b MarkActions,
    statuses: &BlockStatuses,
    diffs: &[BookDiff],
    fills: &[BlockFill],
    system: &super::system::SystemAnchors,
    order_asset: impl Fn(&str) -> Option<u32>,
) -> Result<Vec<PricePoint<'b>>, String> {
    let mut points: Vec<&RankedAction> = actions.actions.iter().filter(|action| action.action.is_price()).collect();
    if points.is_empty() {
        return Ok(Vec::new());
    }
    points.sort_by_key(|action| action.rank);
    let set_globals: Vec<Rank> =
        points.iter().filter(|action| matches!(action.action, MarkAction::SetGlobal(_))).map(|action| action.rank).collect();
    // A native SetGlobalAction cannot trigger a HIP-3 (or spot) order. HIP-3 setOracle can execute in an earlier
    // bundle: treating its fills as native post-SGA fills manufactured a contradiction at bm block 1191473229.
    let native_triggered = statuses.triggered_orders.iter()
        .filter(|order| super::book::is_main_perp(&order.coin) && order_asset(&order.coin).is_some())
        .map(|order| order.oid).collect();
    let cancelled = cancelled_by_action(actions, statuses, fills, &order_asset, system);
    let vault_closes = statuses.vault_closes.attributed(actions, &system.unplaced_vault_withdrawals);
    let replacements = statuses.replacement_orders(actions, &order_asset, &|oid|
        system.competing_orders.contains(&oid) || system.before.created.contains(&oid) || system.after.created.contains(&oid));
    points
        .into_iter()
        .map(|action| {
            let mut anchors = Anchors::around(action.rank, &cancelled, &native_triggered, &statuses.triggered, &set_globals);
            for (rank, oid) in vault_closes.iter().chain(&replacements) {
                let side = usize::from(*rank > action.rank);
                anchors.created[side].insert(*oid);
            }
            anchors.created[0].extend(&system.before.created);
            anchors.created[1].extend(&system.after.created);
            anchors.cancelled[0].extend(&system.before.cancelled);
            anchors.cancelled[1].extend(&system.after.cancelled);
            let (cut, fills_hi) = anchors.cut(diffs, fills).map_err(|err| format!("block {} action at {:?}: {err}", actions.height, action.rank))?;
            Ok(PricePoint { action, cut, fills_hi })
        })
        .collect()
}

/// A book action of the block with every order it removed, its cloid references resolved.
struct Removing<'b> {
    rank: Rank,
    created: &'b [u64],
    cancelled: Vec<u64>,
}

/// Every book action's removed orders: by oid, by cloid (resolved in execution order) and by self-trade checks.
fn cancelled_by_action<'b>(actions: &'b MarkActions, statuses: &BlockStatuses, fills: &[BlockFill], order_asset: impl Fn(&str) -> Option<u32>, system: &super::system::SystemAnchors) -> Vec<Removing<'b>> {
    let mut books: Vec<(&RankedAction, &super::actions::BookAnchors)> = actions
        .actions
        .iter()
        .filter_map(|action| match &action.action {
            MarkAction::Book(book) => Some((action, book)),
            _ => None,
        })
        .collect();
    books.sort_by_key(|(action, _)| action.rank);
    let mut held: FxHashMap<(Address, &str), u64> = FxHashMap::default();
    let mut out = Vec::with_capacity(books.len());
    for &(action, book) in &books {
        let mut cancelled = book.cancelled.clone();
        if let Some(user) = action.user {
            for cloid in &book.cancelled_cloids {
                let holder = held.get(&(user, cloid.as_str())).or_else(|| statuses.cancelled_cloids.get(&(user, cloid.clone())));
                cancelled.extend(holder);
            }
            for (cloid, oid) in &book.created_cloids {
                held.insert((user, cloid.as_str()), *oid);
            }
        }
        out.push(Removing { rank: action.rank, created: &book.created, cancelled });
    }
    let mut attributed = statuses.rejected_iocs.attributed(actions, &order_asset);
    attributed.extend(statuses.filled_iocs.attributed(actions, fills, &order_asset, system));
    attributed.extend(statuses.resting_reduce_only.attributed(actions, fills, &statuses.triggered_orders,
        &order_asset, system, &|oid| out.iter().any(|action| action.cancelled.contains(&oid))));
    for (rank, oids) in &attributed {
        if let Some(action) = out.iter_mut().find(|action| action.rank == *rank) {
            action.cancelled.extend(oids.iter().filter(|oid| !system.competing_ioc_cancels.contains(oid)));
        }
    }
    for cancel in &statuses.self_trade_cancelled {
        // A witnessed IOC run that failed the one-to-one match stays unplaced; do not replace that ambiguity
        // with the weaker same-account heuristic below.
        if statuses.rejected_iocs.contains_cancel(cancel.oid) || statuses.filled_iocs.contains_cancel(cancel.oid) {
            continue;
        }
        let Some((creator, created_at)) = books.iter().find(|(_, book)| book.created.contains(&cancel.oid)).map(|(action, book)| (*book, action.rank))
        else {
            continue;
        };
        let Some(asset) = order_asset(&cancel.coin) else { continue };
        let is_buy = cancel.side == OrderSide::Ask;
        let reaches = |taker: &Taker| (taker.asset, taker.is_buy) == (asset, is_buy) && cancel.reached_by(taker.limit_px);
        // an order of the creating batch itself may have removed its sibling (review 000452 r3)
        if creator.takers.iter().any(reaches) {
            continue;
        }
        let triggered_cause = statuses.triggered_orders.iter().any(|triggered| {
            triggered.user == cancel.user && triggered.coin == cancel.coin && triggered.side != cancel.side && cancel.reached_by(triggered.limit_px)
        });
        if triggered_cause {
            continue;
        }
        let mut causes = books
            .iter()
            .enumerate()
            .filter(|(_, (action, book))| action.rank > created_at && action.user == Some(cancel.user) && book.takers.iter().any(reaches));
        if let (Some((index, _)), None) = (causes.next(), causes.next()) {
            out[index].cancelled.push(cancel.oid);
        }
    }
    out
}

/// The other book actions of the block, before or after one price action.
#[derive(Default)]
struct Anchors<'s> {
    created: [FxHashSet<u64>; 2],
    cancelled: [FxHashSet<u64>; 2],
    /// The orders the block's one `SetGlobalAction` triggered, and their side.
    triggered: Option<(&'s FxHashSet<u64>, Side)>,
    /// Activation is observed, but the triggering action has not been located. Initial placement anchors nothing.
    unplaced_triggered: FxHashSet<u64>,
}

const fn index(side: Side) -> usize {
    match side {
        Side::Before => 0,
        Side::After => 1,
    }
}

impl<'s> Anchors<'s> {
    fn around(rank: Rank, books: &[Removing<'_>], triggered: &'s FxHashSet<u64>, all_triggered: &FxHashSet<u64>, set_globals: &[Rank]) -> Self {
        let mut anchors = Self::default();
        for other in books {
            let side = index(if other.rank < rank { Side::Before } else { Side::After });
            anchors.created[side].extend(other.created);
            anchors.cancelled[side].extend(&other.cancelled);
        }
        // the orders a lone SetGlobalAction triggers run inside it, after its selector, even when an earlier action of
        // the block placed them (bm block 1189996766)
        if let [set_global] = set_globals {
            anchors.triggered = Some((triggered, if *set_global < rank { Side::Before } else { Side::After }));
            anchors.unplaced_triggered.extend(all_triggered.difference(triggered));
        } else {
            anchors.unplaced_triggered.clone_from(all_triggered);
        }
        anchors
    }

    fn sides_of(sets: &[FxHashSet<u64>; 2], oid: u64) -> Vec<Side> {
        [Side::Before, Side::After].into_iter().filter(|side| sets[index(*side)].contains(&oid)).collect()
    }

    fn triggered_side(&self, oid: u64) -> Option<Side> {
        self.triggered.filter(|(triggered, _)| triggered.contains(&oid)).map(|(_, side)| side)
    }

    /// Where a taker leg's action runs.
    fn taker_sides(&self, taker: &BlockFill) -> Vec<Side> {
        if taker.twap {
            return vec![Side::Before];
        }
        if let Some(side) = self.triggered_side(taker.oid) {
            return vec![side];
        }
        if self.unplaced_triggered.contains(&taker.oid) {
            return Vec::new();
        }
        Self::sides_of(&self.created, taker.oid)
    }

    fn cut(&self, diffs: &[BookDiff], fills: &[BlockFill]) -> Result<(Cut, usize), String> {
        let (lo, hi) = interval(self.line_sides(diffs, fills).into_iter().enumerate())
            .map_err(|(lo, hi)| format!("diff anchors contradict the line order: before up to {lo}, after from {hi}"))?;
        let takers: FxHashMap<u64, Option<Side>> = fills
            .iter()
            .filter(|fill| fill.crossed)
            .map(|fill| (fill.tid, single(self.taker_sides(fill))))
            .collect();
        let (fills_lo, fills_hi) = interval(fills.iter().map(|fill| takers.get(&fill.tid).copied().flatten()).enumerate())
            .map_err(|(lo, hi)| format!("fill anchors contradict the fill order: before up to {lo}, after from {hi}"))?;
        Ok((Cut { lo, hi, fills_lo }, fills_hi))
    }

    /// The side of each diff line, `None` when nothing places it.
    fn line_sides(&self, diffs: &[BookDiff], fills: &[BlockFill]) -> Vec<Option<Side>> {
        let fill_sides = self.fill_sides(diffs, fills);
        let mut last_fill_side: FxHashMap<u64, Option<Side>> = FxHashMap::default();
        let mut zeroed = FxHashSet::default();
        diffs
            .iter()
            .enumerate()
            .map(|(line, diff)| {
                let oid = diff.oid();
                if let Some(side) = self.triggered_side(oid) {
                    return Some(side);
                }
                if self.unplaced_triggered.contains(&oid) {
                    return None;
                }
                match diff.kind() {
                    DiffKind::New => single(Self::sides_of(&self.created, oid)),
                    DiffKind::Update { to_zero } => {
                        let side = fill_sides.get(&line).copied().flatten();
                        last_fill_side.insert(oid, side);
                        if to_zero {
                            zeroed.insert(oid);
                        }
                        side
                    }
                    // the remove right after a fill to zero takes that fill's side
                    DiffKind::Remove if zeroed.contains(&oid) => last_fill_side.get(&oid).copied().flatten(),
                    DiffKind::Remove => single(Self::sides_of(&self.cancelled, oid)),
                }
            })
            .collect()
    }

    /// {update line: side of the fill it is}, for the updates a maker's fill determines.
    fn fill_sides(&self, diffs: &[BookDiff], fills: &[BlockFill]) -> FxHashMap<usize, Option<Side>> {
        let mut updates: FxHashMap<u64, Vec<(usize, f64)>> = FxHashMap::default();
        for (line, diff) in diffs.iter().enumerate() {
            let removed = diff.update_sizes().and_then(|(from, to)| Some(from.parse::<f64>().ok()? - to.parse::<f64>().ok()?));
            if let Some(removed) = removed {
                updates.entry(diff.oid()).or_default().push((line, removed));
            }
        }
        let mut sides = FxHashMap::default();
        for (oid, fills) in self.maker_fills(fills) {
            let Some(updates) = updates.get(&oid) else { continue };
            let removed: Vec<f64> = updates.iter().map(|(_, removed)| *removed).collect();
            let filled: Vec<f64> = fills.iter().map(|(filled, _)| *filled).collect();
            for (fill, update) in determined_matches(&removed, &filled) {
                sides.insert(updates[update].0, fills[fill].1);
            }
        }
        sides
    }

    /// {maker oid: (size, side) of each of its fills, in block order}: the side is its taker's, `None` when unplaced.
    fn maker_fills(&self, fills: &[BlockFill]) -> FxHashMap<u64, Vec<(f64, Option<Side>)>> {
        let takers: FxHashMap<u64, &BlockFill> = fills.iter().filter(|fill| fill.crossed).map(|fill| (fill.tid, fill)).collect();
        let mut out: FxHashMap<u64, Vec<(f64, Option<Side>)>> = FxHashMap::default();
        for maker in fills.iter().filter(|fill| !fill.crossed) {
            let Ok(filled) = maker.sz.parse::<f64>() else { continue };
            let side = takers.get(&maker.tid).and_then(|taker| single(self.taker_sides(taker)));
            out.entry(maker.oid).or_default().push((filled, side));
        }
        out
    }
}

/// (fill, update) index pairs that every order-preserving match of `filled` into `removed` by size shares: the earliest
/// and the latest match agree on that fill. Sizes are printed through floats: equal within 1e-9 of the fill. Empty when
/// no match exists.
fn determined_matches(removed: &[f64], filled: &[f64]) -> Vec<(usize, usize)> {
    let same = |update: usize, fill: usize| (filled[fill] - removed[update]).abs() <= 1e-9 * filled[fill].max(1.0);
    let mut earliest = Vec::with_capacity(filled.len());
    for update in 0..removed.len() {
        if earliest.len() < filled.len() && same(update, earliest.len()) {
            earliest.push(update);
        }
    }
    if earliest.len() < filled.len() {
        return Vec::new();
    }
    let mut latest = vec![0; filled.len()];
    let mut fill = filled.len();
    for update in (0..removed.len()).rev() {
        if fill > 0 && same(update, fill - 1) {
            fill -= 1;
            latest[fill] = update;
        }
    }
    earliest.into_iter().zip(latest).enumerate().filter(|(_, (first, last))| first == last).map(|(fill, (update, _))| (fill, update)).collect()
}

fn single(sides: Vec<Side>) -> Option<Side> {
    match sides.as_slice() {
        [side] => Some(*side),
        _ => None,
    }
}

/// [lo, hi]: lo after the last item before, hi at the first item after; `Err((lo, hi))` when they cross.
fn interval(sides: impl Iterator<Item = (usize, Option<Side>)>) -> Result<(usize, usize), (usize, usize)> {
    let (mut lo, mut hi) = (0, usize::MAX);
    let mut count = 0;
    for (position, side) in sides {
        count = position + 1;
        match side {
            Some(Side::Before) => lo = lo.max(position + 1),
            Some(Side::After) => hi = hi.min(position),
            None => {}
        }
    }
    let hi = hi.min(count);
    if lo > hi { Err((lo, hi)) } else { Ok((lo, hi)) }
}

/// The best prices of each asset after each diff line that moved it: `at(asset, cut)` is the book after lines
/// `[0, cut)`.
#[derive(Debug, Default)]
pub(crate) struct BookCuts {
    /// Per asset with lines in the block: (lines applied, best prices), the first entry at 0.
    moves: FxHashMap<usize, Vec<(usize, BestPrices)>>,
}

impl BookCuts {
    /// Records that line `line` moved `asset` from `before` to `after`.
    pub(crate) fn record(&mut self, asset: usize, line: usize, before: BestPrices, after: BestPrices) {
        self.moves.entry(asset).or_insert_with(|| vec![(0, before)]).push((line + 1, after));
    }

    /// The asset's best prices after `cut` lines; `current` when no line of the block moved it.
    pub(crate) fn at(&self, asset: usize, cut: usize, current: BestPrices) -> BestPrices {
        let Some(moves) = self.moves.get(&asset) else {
            return current;
        };
        let after = moves.partition_point(|(lines, _)| *lines <= cut);
        moves[after - 1].1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::marks::{
        actions::{BookAnchors, RankedAction},
        book::tests::diff_line,
    };

    pub(super) fn diff(oid: u64, raw: &str) -> BookDiff {
        BookDiff::of(&diff_line(1, "BTC", oid, "B", "82850.0", raw).events()[0]).unwrap()
    }

    pub(super) fn book(rank: Rank, anchors: BookAnchors) -> RankedAction {
        RankedAction { rank, user: Some(Address::repeat_byte(1)), action: MarkAction::Book(anchors) }
    }

    pub(super) fn set_global(rank: Rank) -> RankedAction {
        RankedAction { rank, user: None, action: MarkAction::SetGlobal(Vec::new()) }
    }

    pub(super) fn fill(tid: u64, oid: u64, crossed: bool) -> BlockFill {
        BlockFill { coin: "BTC".into(), px: "82850.0".into(), sz: "0.002".into(), crossed, oid, tid, action_position: None, twap: false, liquidation_mark_px: None }
    }

    /// No coin named: the blocks of these tests carry no self-trade cancel.
    fn no_asset(_: &str) -> Option<u32> {
        None
    }

    pub(super) const NEW: &str = r#"{"new":{"sz":"0.001"}}"#;
    const REMOVE: &str = r#""remove""#;

    /// An order created before the price action, one cancelled after, and a maker hit by an earlier taker: the cut
    /// sits between them.
    #[test]
    fn the_anchors_bound_the_cut() {
        let created = BookAnchors { created: vec![10], ..BookAnchors::default() };
        let taker = BookAnchors { created: vec![11], ..BookAnchors::default() };
        let cancel = BookAnchors { cancelled: vec![12], ..BookAnchors::default() };
        let actions = MarkActions {
            height: 1,
            time_ns: 1,
            actions: vec![book((0, 1, 0), created), book((0, 3, 1), taker), set_global((1, 3, 0)), book((2, 2, 0), cancel)],
        };
        let diffs = [
            diff(10, NEW),
            diff(20, r#"{"update":{"origSz":"0.002","newSz":"0.0"}}"#),
            diff(20, REMOVE),
            diff(12, REMOVE),
            diff(13, NEW),
        ];
        let fills = [fill(1, 11, true), fill(1, 20, false)];
        let points = price_points(&actions, &BlockStatuses::default(), &diffs, &fills, no_asset).unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].cut, Cut { lo: 3, hi: 3, fills_lo: 2 }, "after the maker's remove, before the cancel");
    }

    /// Block 1189996766: an order placed earlier in the block and triggered by the lone `SetGlobalAction` runs after its
    /// selector, not where it was placed.
    #[test]
    fn a_triggered_order_runs_inside_the_lone_set_global_action() {
        let placed = BookAnchors { created: vec![30], ..BookAnchors::default() };
        let actions = MarkActions { height: 1, time_ns: 1, actions: vec![book((0, 3, 0), placed), set_global((1, 3, 0))] };
        let statuses = BlockStatuses::of(crate::accounts::test_support::status_line(1, "triggered", Address::repeat_byte(1), 30, None).events()).unwrap();
        let diffs = [diff(31, NEW), diff(30, NEW)];
        let btc = |coin: &str| (coin == "BTC").then_some(0);
        let points = price_points(&actions, &statuses, &diffs, &[], btc).unwrap();
        assert_eq!((points[0].cut.lo, points[0].cut.hi), (0, 1));
        let without = price_points(&actions, &BlockStatuses::default(), &diffs, &[], no_asset).unwrap();
        assert_eq!(without[0].cut.lo, 2, "as the placing action's");
    }

    /// BM 1191957330: xyz:SP500 trigger 581440698483 was placed at (0,3,1513), before the reference at
    /// (0,3,1529), but filled after bundle 1's ordinary orders. Its placement is not its execution anchor.
    #[test]
    fn an_unplaced_trigger_does_not_inherit_its_placing_action_for_fills_or_resting_lines() {
        let trigger = 581_440_698_483;
        let earlier = 581_440_698_481;
        let later = 581_440_698_488;
        let maker = 581_440_591_248;
        let coin = "xyz:SP500";
        let actions = MarkActions { height: 1_191_957_330, time_ns: 1, actions: vec![
            book((0, 3, 1511), BookAnchors { created: vec![earlier], ..BookAnchors::default() }),
            book((0, 3, 1513), BookAnchors { created: vec![trigger], ..BookAnchors::default() }),
            RankedAction { rank: (0, 3, 1529), user: None, action: MarkAction::Reference { oracle: vec![], external: vec![] } },
            book((1, 1, 620), BookAnchors { created: vec![later], ..BookAnchors::default() }),
        ] };
        let statuses = BlockStatuses { triggered: [trigger].into_iter().collect(), triggered_orders: vec![StatusOrder {
            user: Address::repeat_byte(1), coin: coin.into(), side: OrderSide::Bid,
            limit_px: ExactDecimal::parse("7850").unwrap(), oid: trigger,
        }], ..BlockStatuses::default() };
        let maker_diff = BookDiff::of(&diff_line(1, coin, maker, "A", "7737.3",
            r#"{"update":{"origSz":"0.635","newSz":"0.624"}}"#).events()[0]).unwrap();
        let fills = [true, false].map(|crossed| BlockFill { action_position: None, coin: coin.into(), px: "7737.3".into(), sz: "0.011".into(),
            crossed, oid: if crossed { trigger } else { maker }, tid: 948_392_423_242_106, twap: false, liquidation_mark_px: None });
        let diffs = [diff(earlier, NEW), diff(later, NEW), maker_diff];
        let points = price_points(&actions, &statuses, &diffs, &fills, no_asset).unwrap();
        assert_eq!(points[0].cut, Cut { lo: 1, hi: 1, fills_lo: 0 }, "ordinary orders fix the reference cut; the triggered fill is unplaced");
        let resting = [diff(earlier, NEW), diff(later, NEW), diff(trigger, NEW)];
        let points = price_points(&actions, &statuses, &resting, &[], no_asset).unwrap();
        assert_eq!((points[0].cut.lo, points[0].cut.hi), (1, 1), "resting after activation is not initial placement either");
    }

    /// Full raw block kept outside Git in `ob_hourly_watch_20261021/20261021T130935Z`. Set
    /// `HL_MARK_BLOCK_1191957330` to that directory; the small regression above runs without external data.
    #[test]
    #[ignore = "requires the archived raw block via HL_MARK_BLOCK_1191957330"]
    fn archived_hip3_trigger_block_has_consistent_reference_cuts() {
        use std::io::Read;
        use crate::{accounts::replica::ReplicaLine, types::node_data::{Batch, NodeDataOrderDiff}};
        let root = std::path::PathBuf::from(std::env::var("HL_MARK_BLOCK_1191957330").unwrap());
        let read = |name: &str| {
            let file = std::fs::File::open(root.join(format!("{name}_1191957330.jsonl.gz"))).unwrap();
            let mut text = String::new();
            flate2::read::GzDecoder::new(file).read_to_string(&mut text).unwrap();
            text
        };
        let raw = read("replica_cmds");
        let actions = MarkActions::of(&ReplicaLine::parse(raw.trim()).unwrap()).unwrap();
        let wire_statuses: Vec<_> = read("node_order_statuses_streaming").lines()
            .flat_map(|line| sonic_rs::from_str::<Batch<NodeDataOrderStatus>>(line).unwrap().events()).collect();
        assert_eq!(wire_statuses.len(), 3102);
        let statuses = BlockStatuses::of(wire_statuses).unwrap();
        assert!(statuses.self_trade_cancelled.is_empty(), "this fixture needs no asset mapping for self-trade attribution");
        assert_eq!(statuses.triggered_orders.len(), 1);
        assert_eq!(statuses.triggered_orders[0].coin.as_ref(), "xyz:SP500");
        assert!(read("system_and_core_writer_actions_streaming").trim().is_empty());
        let diffs: Vec<_> = read("node_raw_book_diffs_streaming").lines()
            .flat_map(|line| sonic_rs::from_str::<Batch<NodeDataOrderDiff>>(line).unwrap().events())
            .map(|diff| BookDiff::of(&diff).unwrap()).collect();
        let fills: Vec<_> = read("node_fills_streaming").lines()
            .flat_map(|line| sonic_rs::from_str::<Batch<NodeDataFill>>(line).unwrap().events())
            .map(|fill| BlockFill::of(&fill)).collect();
        assert_eq!((diffs.len(), fills.len()), (2365, 20));
        let points = price_points(&actions, &statuses, &diffs, &fills, no_asset).unwrap();
        assert_eq!(points.len(), 2);
        let point = points.iter().find(|point| point.action.rank == (0, 3, 1529)).unwrap();
        assert_eq!((point.cut.lo, point.cut.hi), (1365, 1365));
        println!("1191957330 reference cuts: {:?}", points.iter().map(|p| (p.action.rank, p.cut)).collect::<Vec<_>>());
    }

    #[test]
    fn multiple_set_globals_leave_trigger_timing_unplaced_instead_of_using_order_placement() {
        let trigger = 30;
        let actions = MarkActions { height: 7, time_ns: 1, actions: vec![
            book((0, 3, 0), BookAnchors { created: vec![trigger], ..BookAnchors::default() }),
            set_global((0, 3, 1)), set_global((1, 3, 0)),
        ] };
        let statuses = BlockStatuses::of(crate::accounts::test_support::status_line(7, "triggered", Address::repeat_byte(1), trigger, None).events()).unwrap();
        let btc = |coin: &str| (coin == "BTC").then_some(0);
        let diffs = [diff(trigger, NEW)];
        let points = price_points(&actions, &statuses, &diffs, &[], btc).unwrap();
        for point in points { assert_eq!((point.cut.lo, point.cut.hi), (0, 1), "the triggering SGA has not been identified"); }
    }

    #[test]
    fn a_cancel_by_cloid_resolves_through_the_statuses_and_a_contradiction_fails() {
        let cancel = BookAnchors { cancelled_cloids: vec!["0x0a".to_string()], ..BookAnchors::default() };
        let created = BookAnchors { created: vec![41], ..BookAnchors::default() };
        let actions = MarkActions { height: 7, time_ns: 1, actions: vec![book((0, 2, 0), cancel), set_global((1, 3, 0)), book((2, 1, 0), created)] };
        let statuses = BlockStatuses {
            cancelled_cloids: std::iter::once(((Address::repeat_byte(1), "0x0a".to_string()), 40)).collect(),
            ..BlockStatuses::default()
        };
        let points = price_points(&actions, &statuses, &[diff(40, REMOVE), diff(41, NEW)], &[], no_asset).unwrap();
        assert_eq!((points[0].cut.lo, points[0].cut.hi), (1, 1));
        let error = price_points(&actions, &statuses, &[diff(41, NEW), diff(40, REMOVE)], &[], no_asset).unwrap_err();
        assert!(error.contains("block 7 action at (1, 3, 0): diff anchors contradict the line order"), "{error}");
    }

    /// A self-trade cancel's remove runs inside the account's one order that could have taken it (that coin, the
    /// opposite side, a limit reaching it); with none or several, or a triggered order of the block that could have taken
    /// it too, it anchors nothing (research `rune_1191370000_1191380000`, review 000452 r1).
    #[test]
    fn a_self_trade_cancel_runs_inside_the_one_order_that_caused_it() {
        let user = Address::repeat_byte(1);
        let px = |text: &str| ExactDecimal::parse(text).unwrap();
        let btc = |coin: &str| (coin == "BTC").then_some(0);
        // the ALO batch also bought BTC: an ALO takes nothing, so it records no taker
        let alo = || book((1, 1, 481), BookAnchors { created: vec![7], ..BookAnchors::default() });
        let ioc = |rank, is_buy, limit: &str| {
            book(rank, BookAnchors { takers: vec![Taker { asset: 0, is_buy, limit_px: px(limit) }], ..BookAnchors::default() })
        };
        let later = book((3, 1, 0), BookAnchors { created: vec![9], ..BookAnchors::default() });
        let order = |oid, side, limit: &str| StatusOrder { user, coin: "BTC".into(), side, limit_px: px(limit), oid };
        let removed = order(7, OrderSide::Ask, "82850.0");
        let diffs = [diff(7, NEW), diff(7, REMOVE), diff(9, NEW)];
        let cut = |actions: Vec<RankedAction>, triggered: Vec<StatusOrder>, asset: &dyn Fn(&str) -> Option<u32>| {
            let statuses = BlockStatuses {
                triggered: triggered.iter().map(|order| order.oid).collect(),
                triggered_orders: triggered,
                self_trade_cancelled: vec![removed.clone()],
                ..BlockStatuses::default()
            };
            let actions = MarkActions { height: 1_191_373_718, time_ns: 1, actions };
            let points = price_points(&actions, &statuses, &diffs, &[], asset).unwrap();
            (points[0].cut.lo, points[0].cut.hi)
        };
        // bm block 1191373718: the IOC buy at 0.67331 (an order error, no fill) of bundle 1 removed the account's ALO
        // sell at 0.67330; the SetGlobalAction of bundle 2 reads the book after it
        let caused = vec![alo(), ioc((1, 3, 533), true, "82851"), set_global((2, 3, 0)), later.clone()];
        assert_eq!(cut(caused.clone(), vec![], &btc), (2, 2), "after the remove");
        assert_eq!(cut(caused.clone(), vec![], &no_asset), (1, 2), "a coin the marks cannot name: unplaced, as before");
        let two = vec![alo(), ioc((1, 3, 533), true, "82851"), set_global((2, 3, 0)), ioc((2, 3, 5), true, "82850"), later.clone()];
        assert_eq!(cut(two, vec![], &btc), (1, 2), "two orders could have caused it: unplaced");
        let same_side = vec![alo(), ioc((1, 3, 533), false, "82851"), set_global((2, 3, 0)), later.clone()];
        assert_eq!(cut(same_side, vec![], &btc), (1, 2), "a sell cannot trade with the account's own sell");
        // review 000452 r1: an IOC buy that cannot reach the sell, and a buy the SetGlobalAction triggers that can
        let short = vec![alo(), ioc((1, 3, 533), true, "80000"), set_global((2, 3, 0)), later];
        let triggered_buy = order(30, OrderSide::Bid, "90000");
        assert_eq!(cut(short, vec![triggered_buy.clone()], &btc), (1, 2), "the IOC could not have caused it");
        assert_eq!(cut(caused.clone(), vec![triggered_buy], &btc), (1, 2), "a triggered buy could have caused it too");
        assert_eq!(cut(caused, vec![order(30, OrderSide::Bid, "80000")], &btc), (2, 2), "a triggered buy that cannot reach it");
        let early = book((0, 3, 0), BookAnchors { takers: vec![Taker { asset: 0, is_buy: true, limit_px: px("82851") }], ..BookAnchors::default() });
        let before_creation = vec![early, alo(), set_global((2, 3, 0)), book((3, 1, 0), BookAnchors { created: vec![9], ..BookAnchors::default() })];
        assert_eq!(cut(before_creation, vec![], &btc), (1, 2), "an IOC before the sell existed cannot have removed it");
    }

    /// Review 000452 r3: a batch creates the sell 7 and a buy 8 that reaches it; the account's later IOC reaching it too
    /// is no proof against the batch's own buy, and the remove stays unanchored (the batch's new 8 places the price
    /// action after the three lines).
    #[test]
    fn a_self_trade_cancel_the_creating_batch_could_have_caused_anchors_nothing() {
        use crate::accounts::{
            marks::actions::tests::{mark_actions, ok, replica_line},
            test_support::status_line,
        };
        let user: Address = "0x00000000000000000000000000000000000000aa".parse().unwrap();
        let batch = r#"{"type":"order","orders":[{"a":0,"b":false,"p":"82850","s":"0.001","r":false,"t":{"limit":{"tif":"Gtc"}}},{"a":0,"b":true,"p":"82851","s":"0.001","r":false,"t":{"limit":{"tif":"Gtc"}}}],"grouping":"na"}"#;
        let placed = ok(r#"{"type":"order","data":{"statuses":[{"resting":{"oid":7}},{"resting":{"oid":8}}]}}"#);
        let sga = r#"{"type":"SetGlobalAction","pxs":[["82800","82800"]]}"#;
        let ioc = r#"{"type":"order","orders":[{"a":0,"b":true,"p":"82851","s":"0.001","r":false,"t":{"limit":{"tif":"Ioc"}}}],"grouping":"na"}"#;
        let rejected = ok(r#"{"type":"order","data":{"statuses":[{"error":"Order could not immediately match against any resting orders. asset=0"}]}}"#);
        let actions = mark_actions(&replica_line(9, &[&[(batch, &placed)], &[(sga, &ok(r#"{"type":"default"}"#))], &[(ioc, &rejected)]]));
        let mut cancelled = status_line(9, "selfTradeCanceled", user, 7, None).events().remove(0);
        cancelled.order.side = OrderSide::Ask;
        let statuses = BlockStatuses::of([cancelled]).unwrap();
        let line = |oid, side, px: &str, raw: &str| BookDiff::of(&diff_line(9, "BTC", oid, side, px, raw).events()[0]).unwrap();
        let diffs = [line(7, "A", "82850", NEW), line(7, "A", "82850", REMOVE), line(8, "B", "82851", NEW)];
        let btc = |coin: &str| (coin == "BTC").then_some(0);
        let without = price_points(&actions, &BlockStatuses::default(), &diffs, &[], btc).unwrap();
        assert_eq!((without[0].cut.lo, without[0].cut.hi), (3, 3));
        let with = price_points(&actions, &statuses, &diffs, &[], btc).unwrap();
        assert_eq!((with[0].cut.lo, with[0].cut.hi), (3, 3));
    }

    /// Review 000452 r2: an order resting before the block may have been removed by the block-start TWAP that then filled
    /// the next sell (its fills anchor the price action after the three lines); a later IOC of the account that could
    /// have taken it is no proof, and the remove stays unanchored instead of contradicting the fills.
    #[test]
    fn a_self_trade_cancel_of_an_order_resting_before_the_block_anchors_nothing() {
        use crate::accounts::{
            marks::actions::tests::{mark_actions, ok, replica_line},
            test_support::status_line,
        };
        let user: Address = "0x00000000000000000000000000000000000000aa".parse().unwrap();
        let ioc = r#"{"type":"order","orders":[{"a":0,"b":true,"p":"82851","s":"0.001","r":false,"t":{"limit":{"tif":"Ioc"}}}],"grouping":"na"}"#;
        let rejected = ok(r#"{"type":"order","data":{"statuses":[{"error":"Order could not immediately match against any resting orders. asset=0"}]}}"#);
        let sga = r#"{"type":"SetGlobalAction","pxs":[["82800","82800"]]}"#;
        let actions = mark_actions(&replica_line(9, &[&[(sga, &ok(r#"{"type":"default"}"#))], &[(ioc, &rejected)]]));
        let mut cancelled = status_line(9, "selfTradeCanceled", user, 7, None).events().remove(0);
        cancelled.order.side = OrderSide::Ask;
        let statuses = BlockStatuses::of([cancelled]).unwrap();
        let line = |oid, px: &str, raw: &str| BookDiff::of(&diff_line(9, "BTC", oid, "A", px, raw).events()[0]).unwrap();
        let diffs = [line(7, "82850", REMOVE), line(20, "82851", r#"{"update":{"origSz":"0.002","newSz":"0.0"}}"#), line(20, "82851", REMOVE)];
        let mut taker = fill(777, 50, true);
        taker.twap = true;
        taker.px = "82851".into();
        let mut maker = fill(777, 20, false);
        maker.px = "82851".into();
        let fills = [taker, maker];
        let btc = |coin: &str| (coin == "BTC").then_some(0);
        let without = price_points(&actions, &BlockStatuses::default(), &diffs, &fills, btc).unwrap();
        assert_eq!((without[0].cut.lo, without[0].cut.hi), (3, 3));
        let with = price_points(&actions, &statuses, &diffs, &fills, btc).unwrap();
        assert_eq!((with[0].cut.lo, with[0].cut.hi), (3, 3));
    }

    /// bm block 1190808326: two modifies by one cloid, one before and one after the price action. Each names the order
    /// holding the cloid when it runs (the first the order resting before the block, the second the first's new order).
    #[test]
    fn a_cloid_names_the_order_holding_it_when_the_action_runs() {
        let user = Address::repeat_byte(1);
        let modify = |rank, new_oid| {
            book(rank, BookAnchors {
                created: vec![new_oid],
                created_cloids: vec![("0xc1".to_string(), new_oid)],
                cancelled_cloids: vec!["0xc1".to_string()],
                ..BookAnchors::default()
            })
        };
        let point = RankedAction { rank: (0, 3, 1), user: None, action: MarkAction::Reference { oracle: vec![], external: vec![] } };
        let actions = MarkActions { height: 1, time_ns: 1, actions: vec![modify((0, 1, 0), 2), point, modify((1, 1, 0), 3)] };
        let mut statuses = BlockStatuses::default();
        // the cancelled statuses in stream order: 1, then 2; the first held the cloid before the block
        statuses.cancelled_cloids.insert((user, "0xc1".to_string()), 1);
        let diffs = [diff(1, REMOVE), diff(2, NEW), diff(2, REMOVE), diff(3, NEW)];
        let points = price_points(&actions, &statuses, &diffs, &[], no_asset).unwrap();
        assert_eq!((points[0].cut.lo, points[0].cut.hi), (2, 2), "the second modify's remove of 2 runs after");
    }

    /// bm block 1190821813: a maker shrinks without a fill before its one fill, whose taker runs after the price
    /// action; only the update that removed the fill's size takes the fill's side.
    #[test]
    fn an_update_takes_the_fill_that_removed_its_size() {
        let taker = BookAnchors { created: vec![50], ..BookAnchors::default() };
        let actions = MarkActions { height: 1, time_ns: 1, actions: vec![set_global((0, 3, 0)), book((1, 3, 0), taker)] };
        let diffs = [diff(20, r#"{"update":{"origSz":"0.009","newSz":"0.004"}}"#), diff(20, r#"{"update":{"origSz":"0.004","newSz":"0.002"}}"#)];
        let fills = [fill(7, 50, true), fill(7, 20, false)];
        let points = price_points(&actions, &BlockStatuses::default(), &diffs, &fills, no_asset).unwrap();
        assert_eq!((points[0].cut.lo, points[0].cut.hi), (0, 1), "the shrink by 0.005 anchors nothing, the fill of 0.002 is after");
    }

    /// Review 000444 r1 P2-2: a TWAP shrinks reduce-only order 20 by 0.002 before ALO order 10 rests and the price action
    /// runs; taker 50 then fills order 20 for 0.002 after it. Either update may be the fill: neither anchors, so the block
    /// holds together.
    #[test]
    fn a_shrink_of_a_fills_size_leaves_both_updates_unplaced() {
        let alo = BookAnchors { created: vec![10], ..BookAnchors::default() };
        let taker = BookAnchors { created: vec![50], ..BookAnchors::default() };
        let actions =
            MarkActions { height: 1, time_ns: 1, actions: vec![book((0, 1, 0), alo), set_global((1, 3, 0)), book((2, 3, 0), taker)] };
        let diffs = [
            diff(20, r#"{"update":{"origSz":"0.006","newSz":"0.004"}}"#),
            diff(10, r#"{"new":{"sz":"0.001"}}"#),
            diff(20, r#"{"update":{"origSz":"0.004","newSz":"0.002"}}"#),
        ];
        let fills = [fill(7, 50, true), fill(7, 20, false)];
        let points = price_points(&actions, &BlockStatuses::default(), &diffs, &fills, no_asset).unwrap();
        assert_eq!((points[0].cut.lo, points[0].cut.hi), (2, 3), "after order 10's line, the rest open");
    }

    #[test]
    fn a_fill_is_placed_only_where_every_match_puts_it() {
        assert_eq!(determined_matches(&[0.005, 0.002], &[0.002]), [(0, 1)], "one update of that size");
        assert_eq!(determined_matches(&[0.002, 0.002], &[0.002]), [], "either update");
        assert_eq!(determined_matches(&[0.002, 0.002], &[0.002, 0.002]), [(0, 0), (1, 1)], "both, in order");
        assert_eq!(determined_matches(&[0.002, 0.003, 0.002], &[0.003, 0.002]), [(0, 1), (1, 2)], "the order fixes them");
        assert_eq!(determined_matches(&[0.003], &[0.002]), [], "no match at all");
    }

    #[test]
    fn book_cuts_read_the_book_after_a_cut() {
        let mut cuts = BookCuts::default();
        cuts.record(3, 1, (Some(10), Some(12)), (Some(11), Some(12)));
        cuts.record(3, 4, (Some(11), Some(12)), (Some(11), None));
        assert_eq!(cuts.at(3, 0, (Some(11), None)), (Some(10), Some(12)));
        assert_eq!(cuts.at(3, 2, (Some(11), None)), (Some(11), Some(12)));
        assert_eq!(cuts.at(3, 5, (Some(11), None)), (Some(11), None));
        assert_eq!(cuts.at(4, 2, (Some(1), Some(2))), (Some(1), Some(2)), "no line of the block");
    }
}

#[cfg(test)]
mod regression;
