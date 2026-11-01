//! The pending actions' effect on one coin's confirmed book (hyperliquid-trade design 000604 section 3, revisions 4,
//! 8, 12). The replica is the base, read only; the overlay keeps what the actions took off its orders and the orders
//! they rest, and walks both merged: a level's replica orders in their queue, then its pending ones in arrival order.
//!
//! - ALO: a price crossing the merged opposite side is rejected (the node's `badAloPxRejected`), else it rests.
//! - GTC takes, its remainder rests; IOC and `FrontendMarket` take, their remainder goes.
//! - A cancel's or a modify's target is looked for as the merged book stands (revision 12): by oid in the replica, an
//!   order the overlay took out or filled not found; by cloid among the pending resting orders first, then the
//!   replica's; more than one live order with the cloid is ambiguous and touches nothing (the signer is unknown).
//! - A modify by the admission table of revision 8: with `always_place` its target out when found and its new order
//!   placed by its own rules; without, a missing or ambiguous target rejects it, an ALO replaces the target, a GTC
//!   replaces it as an ALO when it would not take once the target is out, and any other is not supported: rejected,
//!   the target left.
//! - A pending order with an ALO priority queues by HL's rule (batch 2f; HL docs "Priority fees"): from the back of its
//!   level's merged queue it passes each order placed under `PRIORITY_WINDOW_MS` before it with a lower priority,
//!   stopping at the first other; its place is fixed then, as HL fixes it on placement.
//! - A level whose queue order the replica cannot prove names no maker at all (review 000620 r2): what a taker takes
//!   there is one fill without a maker - a pending order's place there cannot be proven either (it rests behind the
//!   replica's orders but for a priority).
//! - An order worth less than HL's minimum, 10 of the quote at the price it would first trade at - its limit, or the
//!   merged opposite best when that is better -, is rejected (the node's `minTradeNtlRejected`); a modify placing one
//!   is rejected whole, its target left (an inference: no case seen).
//! - A new order that may take, of a taker HL lately refused for margin on its coin and side, is rejected (batch 2g,
//!   `TakerRefusals`) when the minimum value does not reject it first; a modify is not judged by it.
//! - Self-trades, reduce-only, price bands above the reference and margin otherwise are not modelled (design 000604's
//!   non-goals).

use std::collections::{BTreeMap, VecDeque};

use alloy::primitives::Address;
use rustc_hash::FxHashMap;

use super::{
    actions::{ActionId, BookOp, PendingAction, Placement, Tif},
    refusals::{TakerKey, TakerRefusals},
    twaps::TwapSlice,
};
use crate::{
    accounts::replica::OrderRef,
    order_book::{Oid, OrderBook, PriceLevel, Px, Side, Sz},
    types::inner::InnerL4Order,
};

/// HL's minimum order value, 10 of the quote, in a price's units times a size's (1e-8 each).
const MIN_NOTIONAL: u128 = 10 * 100_000_000 * 100_000_000;

/// HL refuses an order whose limit is under the reference price divided by this - "more than 95% away" (research
/// `mempool_pending_book_20261031`, bm 11-01 13:30-14:20, 4.2M orders against the coin's last trade: refused at 0.03 of
/// it or less, 229 buys and 2 sells; taken from 0.105 for buys, 0.4999 for sells, up to 2.62 above).
const PRICE_BAND_DIVISOR: u128 = 20;

/// HL's ALO priority window T: a level's tail of orders placed within it is sorted by priority (HL docs "Priority
/// fees"; research `mempool_pending_book_20261031`, bm 10-31 20:15-20:20: of the node's 17,187 `insertBefore` diffs,
/// the 15,516 whose jumped order's priority a response names each jumped one placed under 400 ms before, of a lower
/// priority; the oldest 399 ms).
pub(crate) const PRIORITY_WINDOW_MS: u64 = 400;

/// A coin's orders by cloid in the replica.
pub(crate) type CoinCloids = FxHashMap<String, Vec<Oid>>;

/// A side's best level: (price, size, orders), as `OrderBook::get_bbo`.
pub(crate) type BestLevel = (Px, Sz, u32);

/// One coin of the replica, as the overlay reads it.
#[derive(Clone, Copy)]
pub(crate) struct CoinBase<'a> {
    book: Option<&'a OrderBook<InnerL4Order>>,
    /// The levels whose queue order the replica cannot prove (review 000606): no fill there is attributed to a maker.
    unproven: &'a [(Side, Px)],
    cloids: Option<&'a CoinCloids>,
}

impl<'a> CoinBase<'a> {
    pub(crate) const fn new(book: Option<&'a OrderBook<InnerL4Order>>, unproven: &'a [(Side, Px)], cloids: Option<&'a CoinCloids>) -> Self {
        Self { book, unproven, cloids }
    }

    fn levels_from_best(self, side: Side) -> impl Iterator<Item = (Px, &'a PriceLevel<InnerL4Order>)> {
        self.book.into_iter().flat_map(move |book| book.levels_from_best(side)).map(|(px, level)| (*px, level))
    }

    fn level(self, side: Side, px: Px) -> Option<&'a PriceLevel<InnerL4Order>> {
        self.book?.level(side, px)
    }

    fn order(self, oid: &Oid) -> Option<&'a InnerL4Order> {
        self.book?.order(oid)
    }

    /// Whether the book holds `oid`.
    #[cfg(test)]
    pub(crate) fn holds(self, oid: &Oid) -> bool {
        self.order(oid).is_some()
    }

    fn unproven(self, side: Side, px: Px) -> bool {
        self.unproven.contains(&(side, px))
    }

    /// The book's orders with `cloid` (the index's entries the book still holds).
    fn with_cloid(self, cloid: &'a str) -> impl Iterator<Item = &'a InnerL4Order> {
        let oids = self.cloids.and_then(|cloids| cloids.get(cloid)).into_iter().flatten();
        oids.filter_map(move |oid| self.order(oid)).filter(move |order| order.cloid.as_deref() == Some(cloid))
    }
}

/// What the overlay reads of the confirmed state besides the book: for a pending order's place in its level's queue
/// (batch 2f), and its taker's margin refusals (batch 2g).
#[derive(Clone, Copy)]
pub(crate) struct Basis<'a> {
    /// The confirmed orders' ALO priorities by oid; one not here has none.
    priorities: &'a FxHashMap<u64, u64>,
    /// The takers' margin refusals as of the confirmed height.
    refusals: &'a TakerRefusals,
    /// The confirmed block time, unix ms: no pending order is placed before it.
    time_ms: u64,
}

impl<'a> Basis<'a> {
    pub(crate) const fn new(priorities: &'a FxHashMap<u64, u64>, refusals: &'a TakerRefusals, time_ms: u64) -> Self {
        Self { priorities, refusals, time_ms }
    }

    /// When an order whose line came at `seen_ms` is taken to be placed, unix ms: its block's time is past both its
    /// arrival and the confirmed block's (review 000620 r1 P2-1). Judged at each comparison against the confirmed block
    /// then, a pending order resting since an earlier one too (review 000620 r3 P2).
    const fn placed_ms(self, seen_ms: u64) -> u64 {
        if seen_ms > self.time_ms { seen_ms } else { self.time_ms }
    }

    fn priority(self, oid: u64) -> u64 {
        self.priorities.get(&oid).copied().unwrap_or(0)
    }
}

/// Whom a predicted fill trades with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Maker {
    /// A replica order.
    Confirmed { user: Address, oid: Oid, cloid: Option<String> },
    /// A pending order resting in the overlay.
    Pending { id: ActionId, cloid: Option<String> },
    /// A replica level whose queue order is not proven: the size taken there, its makers unknown.
    Unattributed,
}

impl Maker {
    pub(crate) const fn label(&self) -> &'static str {
        match self {
            Self::Confirmed { .. } => "confirmed",
            Self::Pending { .. } => "pending",
            Self::Unattributed => "unattributed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Fill {
    /// The maker's price.
    pub(crate) px: Px,
    pub(crate) sz: Sz,
    pub(crate) maker: Maker,
}

/// What an action did: its fills, and its outcome for the counts.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Effect {
    pub(crate) fills: Vec<Fill>,
    pub(crate) outcome: &'static str,
}

impl Effect {
    const fn of(outcome: &'static str) -> Self {
        Self { fills: Vec::new(), outcome }
    }
}

/// A cancel's or a modify's target as the merged book stands.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Target {
    /// A replica order with `left` of it live.
    Confirmed { oid: Oid, side: Side, px: Px, left: u64 },
    Pending { id: ActionId, side: Side, px: Px },
    Missing,
    Ambiguous,
}

impl Target {
    /// The one live order of `candidates`.
    fn one_of(mut candidates: impl Iterator<Item = Self>) -> Self {
        match (candidates.next(), candidates.next()) {
            (None, _) => Self::Missing,
            (Some(target), None) => target,
            (Some(_), Some(_)) => Self::Ambiguous,
        }
    }

    /// What of a replica level at (side, px) this target holds.
    fn confirmed_at(&self, at_side: Side, at_px: Px) -> u64 {
        match self {
            Self::Confirmed { side, px, left, .. } if *side == at_side && *px == at_px => *left,
            _ => 0,
        }
    }

    fn is_pending(&self, order: ActionId) -> bool {
        matches!(self, Self::Pending { id, .. } if *id == order)
    }
}

#[derive(Debug, Clone)]
struct RestingOrder {
    id: ActionId,
    cloid: Option<String>,
    sz: u64,
    /// The replica order it queues in front of; `None` behind them all.
    ahead_of: Option<Oid>,
    /// Its line's arrival, unix ms (`Basis::placed_ms`).
    seen_ms: u64,
    priority: u64,
}

/// One coin's overlay: the replay of its known pending actions, in execution order, over the replica.
#[derive(Default)]
pub(crate) struct Overlay {
    /// Replica orders the actions reduced: the size left, zero once gone.
    left: FxHashMap<Oid, u64>,
    /// Per side (`at`) and replica level: the (size, orders) `left` took off it.
    taken: [FxHashMap<Px, (u64, usize)>; 2],
    /// Pending orders resting, per side: price to queue, never an empty one, in their order in the merged queue - those
    /// in front of a replica order before those in front of a later one, those behind them all last.
    resting: [BTreeMap<Px, VecDeque<RestingOrder>>; 2],
}

impl Overlay {
    pub(crate) fn apply(&mut self, base: CoinBase<'_>, basis: Basis<'_>, action: &PendingAction) -> Effect {
        let placing = Placing { basis, id: action.id, seen_ms: u64::try_from(action.seen_ns / 1_000_000).unwrap_or(0) };
        match &action.op {
            BookOp::Place(order) => self.place_new(base, placing, action.taker_key(), order),
            BookOp::Cancel(oid) => self.cancel(self.confirmed_target(base, oid)),
            BookOp::CancelByCloid(cloid) => self.cancel(self.cloid_target(base, cloid)),
            BookOp::Modify { target, always_place, order } => self.modify(base, placing, target, *always_place, order),
        }
    }

    /// A TWAP suborder (batch 2h): a taker through the merged opposite side from its best, at most 3% past it (HL docs
    /// "TWAP details": a suborder's max slippage 3%, its reference unstated - the best taken, an approximation) and never
    /// past its parent's limit. Its fills.
    pub(crate) fn take_slice(&mut self, base: CoinBase<'_>, slice: &TwapSlice) -> Vec<Fill> {
        let side = if slice.is_buy { Side::Bid } else { Side::Ask };
        let Some(best) = self.best(base, side.opposite()) else { return Vec::new() };
        let slipped = u128::from(best.value()) * if slice.is_buy { 103 } else { 97 } / 100;
        let slipped = Px::new(u64::try_from(slipped).unwrap_or(u64::MAX));
        let limit = match (side, slice.limit_px) {
            (Side::Bid, Some(parent)) => slipped.min(parent),
            (Side::Ask, Some(parent)) => slipped.max(parent),
            (_, None) => slipped,
        };
        self.take(base, side, limit, slice.sz.value()).0
    }

    /// The merged book's best levels, (bid, ask).
    pub(crate) fn bbo(&self, base: CoinBase<'_>) -> (Option<BestLevel>, Option<BestLevel>) {
        let best = |side| {
            let px = self.best(base, side)?;
            let (sz, orders) = self.level_at(base, side, px);
            Some((px, Sz::new(sz), u32::try_from(orders).unwrap_or(u32::MAX)))
        };
        (best(Side::Bid), best(Side::Ask))
    }

    /// A new order: one its taker's recent margin refusal takes to be refused too goes (batch 2g), when HL's minimum
    /// value does not refuse it first (design 000621 r3).
    fn place_new(&mut self, base: CoinBase<'_>, placing: Placing<'_>, taker: Option<TakerKey>, order: &Placement) -> Effect {
        let refused = taker.is_some_and(|taker| placing.basis.refusals.refused(&taker, placing.seen_ms));
        if refused && !self.below_min_notional(base, order, &Target::Missing) {
            return Effect::of("margin_refused_recently");
        }
        self.place(base, placing, order)
    }

    fn place(&mut self, base: CoinBase<'_>, placing: Placing<'_>, order: &Placement) -> Effect {
        if self.below_min_notional(base, order, &Target::Missing) {
            return Effect::of("min_notional");
        }
        match order.tif {
            Tif::Alo if self.crosses(base, order.side, order.limit_px) => Effect::of("alo_crossed"),
            Tif::Alo => {
                self.rest(base, placing, order, order.sz.value());
                Effect::of("rested")
            }
            Tif::Gtc => {
                let (fills, left) = self.take(base, order.side, order.limit_px, order.sz.value());
                if left > 0 {
                    self.rest(base, placing, order, left);
                }
                Effect { outcome: if fills.is_empty() { "rested" } else { "took" }, fills }
            }
            Tif::Ioc | Tif::FrontendMarket => {
                let (fills, _) = self.take(base, order.side, order.limit_px, order.sz.value());
                Effect { outcome: if fills.is_empty() { "missed" } else { "took" }, fills }
            }
        }
    }

    fn cancel(&mut self, target: Target) -> Effect {
        match target {
            Target::Missing => Effect::of("target_missing"),
            Target::Ambiguous => Effect::of("target_ambiguous"),
            found => {
                self.remove(found);
                Effect::of("canceled")
            }
        }
    }

    /// Revision 8's admission table.
    fn modify(&mut self, base: CoinBase<'_>, placing: Placing<'_>, target: &OrderRef, always_place: bool, order: &Placement) -> Effect {
        let target = match target {
            OrderRef::Oid(oid) => self.confirmed_target(base, &Oid::new(*oid)),
            OrderRef::Cloid(cloid) => self.cloid_target(base, cloid),
        };
        if self.below_min_notional(base, order, &target) {
            return Effect::of("min_notional");
        }
        if always_place {
            self.remove(target);
            return self.place(base, placing, order);
        }
        match (order.tif, target) {
            (_, Target::Missing) => Effect::of("target_missing"),
            (_, Target::Ambiguous) => Effect::of("target_ambiguous"),
            (Tif::Alo, found) => {
                self.remove(found);
                self.place(base, placing, order)
            }
            (Tif::Gtc, found) if !self.crosses_without(base, order.side, order.limit_px, &found) => {
                self.remove(found);
                self.rest(base, placing, order, order.sz.value());
                Effect::of("rested")
            }
            (Tif::Gtc, _) => Effect::of("gtc_would_take"),
            (Tif::Ioc | Tif::FrontendMarket, _) => Effect::of("taker_not_supported"),
        }
    }

    /// A replica order by oid, live in the merged book.
    fn confirmed_target(&self, base: CoinBase<'_>, oid: &Oid) -> Target {
        base.order(oid).map_or(Target::Missing, |order| self.live(order).unwrap_or(Target::Missing))
    }

    /// By cloid: the pending resting orders first, then the replica's live ones.
    fn cloid_target(&self, base: CoinBase<'_>, cloid: &str) -> Target {
        let pending = Target::one_of(self.resting.iter().zip([Side::Bid, Side::Ask]).flat_map(|(levels, side)| {
            levels.iter().flat_map(move |(px, queue)| {
                queue.iter().filter(|order| order.cloid.as_deref() == Some(cloid)).map(move |order| Target::Pending { id: order.id, side, px: *px })
            })
        }));
        if pending != Target::Missing {
            return pending;
        }
        Target::one_of(base.with_cloid(cloid).filter_map(|order| self.live(order)))
    }

    /// A replica order as a target, when the overlay left some of it.
    fn live(&self, order: &InnerL4Order) -> Option<Target> {
        let oid = Oid::new(order.oid);
        let left = self.left.get(&oid).copied().unwrap_or_else(|| order.sz.value());
        (left > 0).then_some(Target::Confirmed { oid, side: order.side, px: order.limit_px, left })
    }

    /// Takes a found target out; a missing or ambiguous one touches nothing.
    fn remove(&mut self, target: Target) {
        match target {
            Target::Confirmed { oid, side, px, left } => self.reduce(side, px, oid, left, left),
            Target::Pending { id, side, px } => {
                let levels = &mut self.resting[at(side)];
                #[expect(clippy::expect_used, reason = "a pending target was found resting at its level just before")]
                let queue = levels.get_mut(&px).expect("a pending target rests at its level");
                queue.retain(|order| order.id != id);
                if queue.is_empty() {
                    levels.remove(&px);
                }
            }
            Target::Missing | Target::Ambiguous => {}
        }
    }

    /// Whether HL refuses `order` for its value: under `MIN_NOTIONAL` at the price it would first trade at, its limit
    /// or the merged opposite best (`target` out) when better - an order resting, and an ALO, is worth its limit (research
    /// `mempool_pending_book_20261031`, bm 10-31 18:00-18:05: 8,949 non-ALO orders at the last trade price, every one
    /// as the node ruled; 18:01-18:03, 425k ALOs at their limit, likewise; reduce-only ones too but for one closing a
    /// whole position, which the set cannot tell).
    fn below_min_notional(&self, base: CoinBase<'_>, order: &Placement, target: &Target) -> bool {
        // an ALO never takes: its limit, crossing or not (review 000617 r1 P2-2)
        let first = (order.tif != Tif::Alo)
            .then(|| self.best_without(base, order.side.opposite(), target).filter(|best| reaches(order.side, order.limit_px, *best)))
            .flatten();
        let px = first.unwrap_or(order.limit_px);
        u128::from(px.value()) * u128::from(order.sz.value()) < MIN_NOTIONAL
    }

    /// Whether an ALO of `side` at `px` would take: the merged opposite side's best reaches it.
    fn crosses(&self, base: CoinBase<'_>, side: Side, px: Px) -> bool {
        self.crosses_without(base, side, px, &Target::Missing)
    }

    /// `crosses` once `target` is out (revision 8: a GTC modify's new order would take after its target's cancel).
    fn crosses_without(&self, base: CoinBase<'_>, side: Side, px: Px, target: &Target) -> bool {
        self.best_without(base, side.opposite(), target).is_some_and(|best| reaches(side, px, best))
    }

    /// A taker of `side` up to `limit_px`, `sz` at most, through the merged opposite side from its best, each level in
    /// its merged queue. The fills, and the size left.
    fn take(&mut self, base: CoinBase<'_>, side: Side, limit_px: Px, mut sz: u64) -> (Vec<Fill>, u64) {
        let makers = side.opposite();
        let mut fills = Vec::new();
        while sz > 0 {
            let Some(px) = self.best(base, makers).filter(|best| reaches(side, limit_px, *best)) else { break };
            sz = self.take_level(base, makers, px, sz, &mut fills);
        }
        (fills, sz)
    }

    /// `sz` at most of the merged level at (side, px), in its queue: before each replica order the pending ones in
    /// front of it, then it; the pending ones behind them all last. A level whose queue order is not proven gives one
    /// fill without a maker for all it gives. The size left.
    fn take_level(&mut self, base: CoinBase<'_>, side: Side, px: Px, mut sz: u64, fills: &mut Vec<Fill>) -> u64 {
        let unproven = base.unproven(side, px);
        let mut level_fills = Vec::new();
        for order in base.level(side, px).into_iter().flat_map(PriceLevel::orders) {
            if sz == 0 {
                break;
            }
            let oid = Oid::new(order.oid);
            sz = self.take_pending(side, px, Some(&oid), sz, &mut level_fills);
            let left = self.left.get(&oid).copied().unwrap_or_else(|| order.sz.value());
            let fill = left.min(sz);
            if fill == 0 {
                continue;
            }
            sz -= fill;
            self.reduce(side, px, oid.clone(), left, fill);
            level_fills.push(Fill { px, sz: Sz::new(fill), maker: Maker::Confirmed { user: order.user, oid, cloid: order.cloid.clone() } });
        }
        sz = self.take_pending(side, px, None, sz, &mut level_fills);
        if unproven && !level_fills.is_empty() {
            let unattributed = level_fills.iter().map(|fill| fill.sz.value()).sum();
            fills.push(Fill { px, sz: Sz::new(unattributed), maker: Maker::Unattributed });
        } else {
            fills.extend(level_fills);
        }
        sz
    }

    /// `sz` at most of the level's pending orders from the front, while they queue in front of the replica order
    /// `before` - all of them for `None`, the back of the queue.
    fn take_pending(&mut self, side: Side, px: Px, before: Option<&Oid>, mut sz: u64, fills: &mut Vec<Fill>) -> u64 {
        let levels = &mut self.resting[at(side)];
        let Some(queue) = levels.get_mut(&px) else { return sz };
        while sz > 0
            && let Some(maker) = queue.front_mut().filter(|maker| before.is_none() || maker.ahead_of.as_ref() == before)
        {
            let fill = maker.sz.min(sz);
            maker.sz -= fill;
            sz -= fill;
            fills.push(Fill { px, sz: Sz::new(fill), maker: Maker::Pending { id: maker.id, cloid: maker.cloid.clone() } });
            if maker.sz == 0 {
                queue.pop_front();
            }
        }
        if queue.is_empty() {
            levels.remove(&px);
        }
        sz
    }

    /// Replica order `oid` at (side, px), `left` of it live, reduced by `by`.
    fn reduce(&mut self, side: Side, px: Px, oid: Oid, left: u64, by: u64) {
        self.left.insert(oid, left - by);
        let taken = self.taken[at(side)].entry(px).or_insert((0, 0));
        taken.0 += by;
        if left == by {
            taken.1 += 1;
        }
    }

    fn rest(&mut self, base: CoinBase<'_>, placing: Placing<'_>, order: &Placement, sz: u64) {
        let (index, ahead_of) = self.queue_place(base, placing, order);
        let queue = self.resting[at(order.side)].entry(order.limit_px).or_default();
        let resting = RestingOrder { id: placing.id, cloid: order.cloid.clone(), sz, ahead_of, seen_ms: placing.seen_ms, priority: order.priority };
        queue.insert(index, resting);
    }

    /// Where `order` queues in its level: its index among the level's pending orders, and the replica order it goes in
    /// front of. From the back of the merged queue it passes each order placed under `PRIORITY_WINDOW_MS` before it
    /// with a lower priority and stops at the first other - "Each new order compares against the tail of orders less
    /// than T old at placement time" (HL docs "Priority fees") -; a replica order the overlay took out is no longer
    /// there to stop it. Without a priority, or at a level whose queue order is not proven: at the back.
    fn queue_place(&self, base: CoinBase<'_>, placing: Placing<'_>, order: &Placement) -> (usize, Option<Oid>) {
        let empty = VecDeque::new();
        let pending = self.resting[at(order.side)].get(&order.limit_px).unwrap_or(&empty);
        let mut index = pending.len();
        if order.priority == 0 || base.unproven(order.side, order.limit_px) {
            return (index, None);
        }
        let basis = placing.basis;
        let placed_ms = basis.placed_ms(placing.seen_ms);
        let passes = |other_ms: u64, priority: u64| placed_ms.saturating_sub(other_ms) < PRIORITY_WINDOW_MS && priority < order.priority;
        let mut ahead_of = None;
        let mut confirmed = base.level(order.side, order.limit_px).into_iter().flat_map(PriceLevel::orders_rev);
        loop {
            while let Some(before) = index.checked_sub(1).map(|before| &pending[before]).filter(|before| before.ahead_of == ahead_of) {
                if !passes(basis.placed_ms(before.seen_ms), before.priority) {
                    return (index, ahead_of);
                }
                index -= 1;
            }
            let Some(before) = confirmed.next() else { return (index, ahead_of) };
            let oid = Oid::new(before.oid);
            let live = self.left.get(&oid).is_none_or(|left| *left > 0);
            if live && !passes(before.timestamp, basis.priority(before.oid)) {
                return (index, ahead_of);
            }
            ahead_of = Some(oid);
        }
    }

    /// The merged side's best price: the replica's first level the overlay left some of, or a better pending one.
    fn best(&self, base: CoinBase<'_>, side: Side) -> Option<Px> {
        self.best_without(base, side, &Target::Missing)
    }

    /// `best` with `target` out.
    fn best_without(&self, base: CoinBase<'_>, side: Side, target: &Target) -> Option<Px> {
        let confirmed = base
            .levels_from_best(side)
            .find(|(px, level)| self.left_of(side, *px, level).0 > target.confirmed_at(side, *px))
            .map(|(px, _)| px);
        let live = |queue: &VecDeque<RestingOrder>| queue.iter().any(|order| !target.is_pending(order.id));
        let levels = &self.resting[at(side)];
        let pending = match side {
            Side::Bid => levels.iter().rev().find(|(_, queue)| live(queue)),
            Side::Ask => levels.iter().find(|(_, queue)| live(queue)),
        };
        match (confirmed, pending.map(|(px, _)| *px)) {
            (Some(confirmed), Some(pending)) => Some(if at_least_as_good(side, confirmed, pending) { confirmed } else { pending }),
            (confirmed, pending) => confirmed.or(pending),
        }
    }

    /// The merged level's (size, orders).
    fn level_at(&self, base: CoinBase<'_>, side: Side, px: Px) -> (u64, usize) {
        let (confirmed_sz, confirmed_orders) = base.level(side, px).map_or((0, 0), |level| self.left_of(side, px, level));
        let pending = self.resting[at(side)].get(&px);
        let pending_sz: u64 = pending.into_iter().flatten().map(|order| order.sz).sum();
        (confirmed_sz + pending_sz, confirmed_orders + pending.map_or(0, VecDeque::len))
    }

    /// A replica level's (size, orders) the overlay left.
    #[expect(clippy::expect_used, reason = "the overlay is replayed whenever its coin's replica book changes")]
    fn left_of(&self, side: Side, px: Px, level: &PriceLevel<InnerL4Order>) -> (u64, usize) {
        let (sz, orders) = self.taken[at(side)].get(&px).copied().unwrap_or((0, 0));
        let sz = level.total_sz().value().checked_sub(sz).expect("the overlay takes no more than a replica level holds");
        (sz, level.len().checked_sub(orders).expect("the overlay takes no more orders than a replica level holds"))
    }
}

/// An action placing an order: the confirmed state it is placed on, its id and its line's arrival, unix ms.
#[derive(Clone, Copy)]
struct Placing<'a> {
    basis: Basis<'a>,
    id: ActionId,
    seen_ms: u64,
}

const fn at(side: Side) -> usize {
    match side {
        Side::Bid => 0,
        Side::Ask => 1,
    }
}

/// Whether an order of `side` limited at `limit` reaches a maker at `px`: a bid at or above it, an ask at or below.
fn reaches(side: Side, limit: Px, px: Px) -> bool {
    match side {
        Side::Bid => px <= limit,
        Side::Ask => px >= limit,
    }
}

/// Whether `px` is at least as good as `other` among `side`'s levels: a bid as high, an ask as low.
fn at_least_as_good(side: Side, px: Px, other: Px) -> bool {
    match side {
        Side::Bid => px >= other,
        Side::Ask => px <= other,
    }
}

/// Whether HL refuses an order of `limit_px` for its price: under `PRICE_BAND_DIVISOR`th of the reference, taken as the
/// confirmed book's midpoint - compared undivided (review 000637 r1 P2-2) -, or its one side (batch 2k); with no level
/// at all, not judged.
pub(crate) fn below_price_band(base: CoinBase<'_>, limit_px: Px) -> bool {
    let best = |side| base.levels_from_best(side).next().map(|(px, _)| u128::from(px.value()));
    let limit = u128::from(limit_px.value()) * PRICE_BAND_DIVISOR;
    match (best(Side::Bid), best(Side::Ask)) {
        (Some(bid), Some(ask)) => 2 * limit < bid + ask,
        (Some(one), None) | (None, Some(one)) => limit < one,
        (None, None) => false,
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::B256;

    use super::*;

    const ALICE: Address = Address::repeat_byte(0xa1);
    const BOB: Address = Address::repeat_byte(0xb0);

    fn px(text: &str) -> Px {
        Px::parse_from_str(text).unwrap()
    }

    fn sz(text: &str) -> Sz {
        Sz::parse_from_str(text).unwrap()
    }

    fn resting(oid: u64, user: Address, side: Side, at: &str, quantity: &str, cloid: Option<&str>) -> InnerL4Order {
        InnerL4Order {
            user,
            coin: crate::order_book::Coin::new("BTC"),
            side,
            limit_px: px(at),
            sz: sz(quantity),
            oid,
            timestamp: 0,
            trigger_condition: "N/A".to_string(),
            is_trigger: false,
            trigger_px: "0.0".to_string(),
            is_position_tpsl: false,
            reduce_only: false,
            order_type: "Limit".to_string(),
            tif: Some("Alo".to_string()),
            cloid: cloid.map(str::to_string),
        }
    }

    /// The replica's BTC: bids 99 (oid 1, 1.0) and 98 (oid 2, 2.0); asks 101 (oids 3 then 4, 1.0 each, oid 3 cloid
    /// 0x03) and 102 (oid 5, 1.0).
    fn book() -> (OrderBook<InnerL4Order>, CoinCloids) {
        let mut book = OrderBook::new();
        for order in [
            resting(1, ALICE, Side::Bid, "99", "1", None),
            resting(2, BOB, Side::Bid, "98", "2", None),
            resting(3, ALICE, Side::Ask, "101", "1", Some("0x03")),
            resting(4, BOB, Side::Ask, "101", "1", None),
            resting(5, BOB, Side::Ask, "102", "1", None),
        ] {
            book.add_order(order);
        }
        let cloids = std::iter::once(("0x03".to_string(), vec![Oid::new(3)])).collect();
        (book, cloids)
    }

    fn id(action: usize) -> ActionId {
        ActionId { tx_hash: B256::repeat_byte(1), action, item: 0 }
    }

    fn action(index: usize, op: BookOp) -> PendingAction {
        PendingAction { id: id(index), seen_ns: 0, vault: None, signer: None, asset: 0, op, unknown: false }
    }

    fn order(side: Side, at: &str, quantity: &str, tif: Tif, cloid: Option<&str>) -> Placement {
        Placement { side, limit_px: px(at), sz: sz(quantity), tif, cloid: cloid.map(str::to_string), priority: 0 }
    }

    fn place(index: usize, side: Side, at: &str, quantity: &str, tif: Tif, cloid: Option<&str>) -> PendingAction {
        action(index, BookOp::Place(order(side, at, quantity, tif, cloid)))
    }

    fn level(at: &str, size: &str, orders: u32) -> BestLevel {
        (px(at), sz(size), orders)
    }

    fn no_priorities() -> Basis<'static> {
        static NONE: std::sync::LazyLock<FxHashMap<u64, u64>> = std::sync::LazyLock::new(FxHashMap::default);
        Basis::new(&NONE, no_refusals(), 0)
    }

    /// No taker refused for margin.
    fn no_refusals() -> &'static TakerRefusals {
        static NONE: std::sync::LazyLock<TakerRefusals> = std::sync::LazyLock::new(|| TakerRefusals::new(300_000));
        &NONE
    }

    /// Applies `actions` in order: their outcomes, then the merged book's best levels.
    fn run(actions: &[PendingAction]) -> (Vec<Effect>, (Option<BestLevel>, Option<BestLevel>)) {
        let (book, cloids) = book();
        let base = CoinBase::new(Some(&book), &[], Some(&cloids));
        let mut overlay = Overlay::default();
        let effects = actions.iter().map(|action| overlay.apply(base, no_priorities(), action)).collect();
        (effects, overlay.bbo(base))
    }

    fn outcomes(effects: &[Effect]) -> Vec<&'static str> {
        effects.iter().map(|effect| effect.outcome).collect()
    }

    fn confirmed(user: Address, oid: u64, cloid: Option<&str>) -> Maker {
        Maker::Confirmed { user, oid: Oid::new(oid), cloid: cloid.map(str::to_string) }
    }

    /// Batch 2k: the price band against the confirmed book - its midpoint 100 (99 / 101): 4.99 under it, 5 not; the
    /// midpoint undivided (review 000637 r1 P2-2): 0.00002000 / 0.00002001, a limit of 0.00000100 under it (x 20 is
    /// 0.00002000 < 0.000020005); one side alone its own; no level, not judged.
    #[test]
    fn the_price_band_is_judged_against_the_confirmed_book() {
        let (book, cloids) = book();
        let base = CoinBase::new(Some(&book), &[], Some(&cloids));
        assert_eq!(["4.99", "5"].map(|limit| below_price_band(base, px(limit))), [true, false]);
        let mut thin = OrderBook::new();
        thin.add_order(resting(1, ALICE, Side::Bid, "0.00002", "1", None));
        thin.add_order(resting(2, BOB, Side::Ask, "0.00002001", "1", None));
        let thin_base = CoinBase::new(Some(&thin), &[], None);
        assert!(below_price_band(thin_base, px("0.000001")));
        let mut one_side = OrderBook::new();
        one_side.add_order(resting(3, BOB, Side::Ask, "101", "1", None));
        let one_base = CoinBase::new(Some(&one_side), &[], None);
        assert_eq!(["5.04", "5.05"].map(|limit| below_price_band(one_base, px(limit))), [true, false]);
        assert!(!below_price_band(CoinBase::new(None, &[], None), px("0.000001")));
    }

    /// Batch 2h: a TWAP suborder takes up to 3% past the best opposite level, and no further than its parent's limit.
    #[test]
    fn a_twap_suborder_takes_to_its_slippage_and_its_parents_limit() {
        let (book, cloids) = book();
        let base = CoinBase::new(Some(&book), &[], Some(&cloids));
        let slice = |is_buy: bool, limit: Option<&str>| TwapSlice { twap_id: 7, user: ALICE, slot: 0, is_buy, sz: sz("5"), limit_px: limit.map(px), due_ms: 0 };
        let taken = |slice: TwapSlice| {
            let mut overlay = Overlay::default();
            drop(overlay.apply(base, no_priorities(), &place(0, Side::Ask, "105", "1", Tif::Alo, None)));
            overlay.take_slice(base, &slice).into_iter().map(|fill| (fill.px, fill.sz)).collect::<Vec<_>>()
        };
        let one = sz("1");
        assert_eq!(taken(slice(true, None)), [(px("101"), one), (px("101"), one), (px("102"), one)], "105 past 101 x 1.03");
        assert_eq!(taken(slice(true, Some("101.5"))), [(px("101"), one), (px("101"), one)]);
        assert_eq!(taken(slice(false, None)), [(px("99"), one), (px("98"), sz("2"))]);
        assert_eq!(taken(slice(false, Some("98.5"))), [(px("99"), one)]);
    }

    #[test]
    fn with_nothing_pending_the_best_levels_are_the_replicas() {
        assert_eq!(run(&[]).1, (Some(level("99", "1", 1)), Some(level("101", "2", 2))));
    }

    /// An IOC walks the asks in queue order, past the level, up to its limit; its remainder goes.
    #[test]
    fn an_ioc_takes_in_queue_order_up_to_its_limit() {
        let (effects, bbo) = run(&[place(0, Side::Bid, "102", "2.5", Tif::Ioc, None)]);
        assert_eq!(
            effects[0].fills,
            [
                Fill { px: px("101"), sz: sz("1"), maker: confirmed(ALICE, 3, Some("0x03")) },
                Fill { px: px("101"), sz: sz("1"), maker: confirmed(BOB, 4, None) },
                Fill { px: px("102"), sz: sz("0.5"), maker: confirmed(BOB, 5, None) },
            ]
        );
        assert_eq!(bbo, (Some(level("99", "1", 1)), Some(level("102", "0.5", 1))));
    }

    /// A replica order taken in part is taken for what is left of it, one cancelled not at all.
    #[test]
    fn a_replica_order_taken_or_cancelled_gives_only_what_is_left() {
        let (effects, bbo) = run(&[
            place(0, Side::Bid, "101", "0.5", Tif::Ioc, None),
            place(1, Side::Bid, "101", "1", Tif::Ioc, None),
            action(2, BookOp::Cancel(Oid::new(5))),
            place(3, Side::Bid, "102", "1", Tif::Ioc, None),
        ]);
        assert_eq!(
            effects[1].fills,
            [
                Fill { px: px("101"), sz: sz("0.5"), maker: confirmed(ALICE, 3, Some("0x03")) },
                Fill { px: px("101"), sz: sz("0.5"), maker: confirmed(BOB, 4, None) },
            ]
        );
        assert_eq!(effects[3].fills, [Fill { px: px("101"), sz: sz("0.5"), maker: confirmed(BOB, 4, None) }]);
        assert_eq!(bbo.1, None);
    }

    /// A GTC takes what crosses and rests its remainder, which an ALO then crosses; an IOC takes the next level.
    #[test]
    fn a_gtc_rests_what_it_did_not_take() {
        let (effects, bbo) = run(&[
            place(0, Side::Ask, "99", "1.5", Tif::Gtc, Some("0xg")),
            place(1, Side::Bid, "100", "0.5", Tif::Alo, None),
            place(2, Side::Ask, "98", "1", Tif::Ioc, None),
        ]);
        assert_eq!(effects[0].fills, [Fill { px: px("99"), sz: sz("1"), maker: confirmed(ALICE, 1, None) }]);
        assert_eq!(outcomes(&effects), ["took", "alo_crossed", "took"]);
        assert_eq!(effects[2].fills, [Fill { px: px("98"), sz: sz("1"), maker: confirmed(BOB, 2, None) }]);
        assert_eq!(bbo, (Some(level("98", "1", 1)), Some(level("99", "0.5", 1))));
    }

    /// An ALO resting at a replica level queues behind its orders: a taker meets them first.
    #[test]
    fn a_pending_alo_queues_behind_the_replicas_orders_and_rests_alone_inside_the_spread() {
        let (effects, bbo) = run(&[
            place(0, Side::Ask, "101", "1", Tif::Alo, Some("0xa")),
            place(1, Side::Ask, "100.5", "1", Tif::Alo, None),
            place(2, Side::Bid, "101", "3", Tif::Ioc, None),
        ]);
        assert_eq!(
            effects[2].fills.iter().map(|fill| (fill.px, fill.maker.clone())).collect::<Vec<_>>(),
            [
                (px("100.5"), Maker::Pending { id: id(1), cloid: None }),
                (px("101"), confirmed(ALICE, 3, Some("0x03"))),
                (px("101"), confirmed(BOB, 4, None)),
            ]
        );
        assert_eq!(bbo, (Some(level("99", "1", 1)), Some(level("101", "1", 1))));
    }

    /// Revision 4's example: the only ask at 101 cancelled and an ALO bid at 101 in one bundle - the ALO runs first
    /// (`actions` orders it so) and is rejected; the cancel takes the ask out after.
    #[test]
    fn an_alo_crossing_the_merged_book_is_rejected() {
        let (effects, bbo) = run(&[place(0, Side::Bid, "101", "1", Tif::Alo, None), action(1, BookOp::Cancel(Oid::new(3)))]);
        assert_eq!(outcomes(&effects), ["alo_crossed", "canceled"]);
        assert_eq!(bbo, (Some(level("99", "1", 1)), Some(level("101", "1", 1))));
    }

    /// Revision 8: a cloid cancel takes a pending resting order first; revision 12: an oid already taken out is not
    /// found again, an order filled out neither.
    #[test]
    fn targets_are_found_as_the_merged_book_stands() {
        let (effects, bbo) = run(&[
            place(0, Side::Bid, "99.5", "1", Tif::Alo, Some("0x03")),
            action(1, BookOp::CancelByCloid("0x03".to_string())),
            action(2, BookOp::CancelByCloid("0x03".to_string())),
            action(3, BookOp::CancelByCloid("0x03".to_string())),
            action(4, BookOp::Cancel(Oid::new(4))),
            place(5, Side::Ask, "99", "1", Tif::Ioc, None),
            action(6, BookOp::Cancel(Oid::new(1))),
        ]);
        assert_eq!(outcomes(&effects), ["rested", "canceled", "canceled", "target_missing", "canceled", "took", "target_missing"]);
        assert_eq!(bbo, (Some(level("98", "2", 1)), Some(level("102", "1", 1))));
    }

    /// Two live orders with a cloid (two signers) make it ambiguous: nothing is taken out.
    #[test]
    fn a_cloid_on_two_live_orders_is_ambiguous() {
        let (effects, bbo) = run(&[
            place(0, Side::Bid, "97", "1", Tif::Alo, Some("0xd")),
            place(1, Side::Bid, "96", "1", Tif::Alo, Some("0xd")),
            action(2, BookOp::CancelByCloid("0xd".to_string())),
        ]);
        assert_eq!(outcomes(&effects), ["rested", "rested", "target_ambiguous"]);
        assert_eq!(bbo.0, Some(level("99", "1", 1)));
    }

    fn modify(index: usize, target: OrderRef, always_place: bool, new: Placement) -> PendingAction {
        action(index, BookOp::Modify { target, always_place, order: new })
    }

    /// Revision 12's examples: oid 3 cancelled (or filled out) by an earlier action; a plain ALO modify of it then
    /// places nothing, one with `always_place` places its new order.
    #[test]
    fn a_modify_of_a_target_gone_is_rejected_unless_always_place() {
        let gone = [action(0, BookOp::Cancel(Oid::new(3))), place(0, Side::Bid, "101", "1", Tif::Ioc, None)];
        for earlier in gone {
            let new = order(Side::Ask, "100", "1", Tif::Alo, None);
            let (effects, bbo) = run(&[earlier.clone(), modify(1, OrderRef::Oid(3), false, new.clone())]);
            assert_eq!(effects[1].outcome, "target_missing");
            assert_eq!(bbo.1, Some(level("101", "1", 1)));
            let (effects, bbo) = run(&[earlier, modify(1, OrderRef::Oid(3), true, new)]);
            assert_eq!(effects[1].outcome, "rested");
            assert_eq!(bbo.1, Some(level("100", "1", 1)));
        }
    }

    /// Revision 3's examples: a plain modify whose GTC would take leaves its target; with `always_place` it takes
    /// the target out and trades; a plain one whose GTC would not take replaces the target as an ALO.
    #[test]
    fn a_plain_modify_never_takes() {
        let taking = order(Side::Ask, "99", "1", Tif::Gtc, None);
        let (effects, bbo) = run(&[modify(0, OrderRef::Cloid("0x03".to_string()), false, taking.clone())]);
        assert_eq!(effects[0].outcome, "gtc_would_take");
        assert_eq!(bbo, (Some(level("99", "1", 1)), Some(level("101", "2", 2))));
        let (effects, bbo) = run(&[modify(0, OrderRef::Cloid("0x03".to_string()), true, taking)]);
        assert_eq!(effects[0].fills, [Fill { px: px("99"), sz: sz("1"), maker: confirmed(ALICE, 1, None) }]);
        assert_eq!(bbo, (Some(level("98", "2", 1)), Some(level("101", "1", 1))));
        let (effects, bbo) = run(&[modify(0, OrderRef::Oid(3), false, order(Side::Ask, "100", "1", Tif::Gtc, None))]);
        assert_eq!(effects[0].outcome, "rested");
        assert_eq!(bbo.1, Some(level("100", "1", 1)));
        let ioc = order(Side::Ask, "100", "1", Tif::Ioc, None);
        assert_eq!(run(&[modify(0, OrderRef::Oid(3), false, ioc)]).0[0].outcome, "taker_not_supported");
    }

    /// Review 000611 r1 P2-4: a plain GTC modify changing side is judged with its target out (revision 8). The bid at
    /// 99 turned into an ask at 99 would take its own old bid, not the next one at 98: it rests. The bid at 98 turned
    /// into an ask at 99 meets the bid at 99: rejected, the target left. The ask at 101 turned into a bid at 100.5,
    /// under the other ask at 101: it rests.
    #[test]
    fn a_plain_gtc_modify_changing_side_is_judged_with_its_target_out() {
        let (effects, bbo) = run(&[modify(0, OrderRef::Oid(1), false, order(Side::Ask, "99", "1", Tif::Gtc, None))]);
        assert_eq!(effects[0].outcome, "rested");
        assert_eq!(bbo, (Some(level("98", "2", 1)), Some(level("99", "1", 1))));
        let (effects, bbo) = run(&[modify(0, OrderRef::Oid(2), false, order(Side::Ask, "99", "1", Tif::Gtc, None))]);
        assert_eq!(effects[0].outcome, "gtc_would_take");
        assert_eq!(bbo, (Some(level("99", "1", 1)), Some(level("101", "2", 2))));
        let (effects, bbo) = run(&[modify(0, OrderRef::Oid(3), false, order(Side::Bid, "100.5", "1", Tif::Gtc, None))]);
        assert_eq!(effects[0].outcome, "rested");
        assert_eq!(bbo, (Some(level("100.5", "1", 1)), Some(level("101", "1", 1))));
    }

    /// A pending target out of its own level when judging: an ALO modify's earlier pending bid at 100 is the only
    /// order there; turned into an ask at 100 by a plain GTC modify, it meets the replica's bid at 99 - no take.
    #[test]
    fn a_pending_target_is_out_when_judging() {
        let (effects, bbo) = run(&[
            place(0, Side::Bid, "100", "1", Tif::Alo, Some("0xp")),
            modify(1, OrderRef::Cloid("0xp".to_string()), false, order(Side::Ask, "100", "1", Tif::Gtc, None)),
        ]);
        assert_eq!(outcomes(&effects), ["rested", "rested"]);
        assert_eq!(bbo, (Some(level("99", "1", 1)), Some(level("100", "1", 1))));
    }

    /// A level whose queue order the replica cannot prove gives one fill without a maker for all a taker takes there,
    /// its pending orders' part too: one with a priority may queue in front of the replica's (review 000620 r2; the
    /// replica's part alone before, review 000611 r1 P2-3).
    #[test]
    fn an_unproven_level_names_no_maker() {
        let (book, cloids) = book();
        let unproven = [(Side::Ask, px("101"))];
        let base = CoinBase::new(Some(&book), &unproven, Some(&cloids));
        let mut overlay = Overlay::default();
        overlay.apply(base, no_priorities(), &place(0, Side::Ask, "101", "1", Tif::Alo, Some("0xa")));
        let effect = overlay.apply(base, no_priorities(), &place(1, Side::Bid, "102", "3.5", Tif::Ioc, None));
        assert_eq!(
            effect.fills,
            [Fill { px: px("101"), sz: sz("3"), maker: Maker::Unattributed }, Fill { px: px("102"), sz: sz("0.5"), maker: confirmed(BOB, 5, None) }]
        );
    }

    /// A coin without a replica book: the pending orders alone.
    #[test]
    fn a_coin_without_a_book_has_its_pending_orders_alone() {
        let base = CoinBase::new(None, &[], None);
        let mut overlay = Overlay::default();
        let effect = overlay.apply(base, no_priorities(), &place(0, Side::Bid, "5", "3", Tif::Gtc, None));
        assert_eq!(effect.outcome, "rested");
        assert_eq!(overlay.bbo(base), (Some(level("5", "3", 1)), None));
    }

    /// Batch 2d: HL's minimum, 10 of the quote at the price an order would first trade at. An IOC buying 0.099 up to
    /// 200 is worth 9.999 at the best ask 101: refused, nothing taken; one selling 0.101 down to 50 is worth 9.999 at
    /// the best bid 99. An ALO rests at its limit and is worth it: 0.0995 at 100.5 refused, 0.1 rests, 0.1 at 100 (10
    /// exactly) too; an IOC of 0.1 then takes at 100.5.
    #[test]
    fn an_order_under_the_minimum_value_is_refused() {
        let (effects, bbo) = run(&[
            place(0, Side::Bid, "200", "0.099", Tif::Ioc, None),
            place(1, Side::Ask, "50", "0.101", Tif::Ioc, None),
            place(2, Side::Ask, "100.5", "0.0995", Tif::Alo, None),
            place(3, Side::Ask, "100.5", "0.1", Tif::Alo, None),
            place(4, Side::Bid, "100", "0.1", Tif::Alo, None),
            place(5, Side::Bid, "200", "0.1", Tif::Ioc, None),
        ]);
        assert_eq!(outcomes(&effects), ["min_notional", "min_notional", "min_notional", "rested", "rested", "took"]);
        assert_eq!(effects[5].fills, [Fill { px: px("100.5"), sz: sz("0.1"), maker: Maker::Pending { id: id(3), cloid: None } }]);
        assert_eq!(bbo, (Some(level("100", "0.1", 1)), Some(level("101", "2", 2))));
    }

    /// Batch 2d: a modify whose new order is under the minimum is refused whole, its target left.
    #[test]
    fn a_modify_under_the_minimum_value_leaves_its_target() {
        let modify = BookOp::Modify { target: OrderRef::Oid(1), always_place: true, order: order(Side::Bid, "99", "0.05", Tif::Alo, None) };
        let (effects, bbo) = run(&[action(0, modify)]);
        assert_eq!(outcomes(&effects), ["min_notional"]);
        assert_eq!(bbo.0, Some(level("99", "1", 1)));
    }

    /// Review 000617 r1 P2-2: an ALO modify is worth its limit, crossing or not. Selling 0.102 at 98 is 9.996 - refused,
    /// the ask 101 (oid 3) it targets left whole -, though 10.098 at the bid 99; buying 0.095 at 110 with `always_place` is
    /// 10.45 - its target, the bid 99, goes, and the ALO crossing the ask 101 is refused -, though 9.595 at the ask.
    #[test]
    fn an_alo_modify_is_worth_its_limit() {
        let sell = BookOp::Modify { target: OrderRef::Oid(3), always_place: false, order: order(Side::Ask, "98", "0.102", Tif::Alo, None) };
        let (effects, bbo) = run(&[action(0, sell)]);
        assert_eq!((outcomes(&effects), bbo.1), (vec!["min_notional"], Some(level("101", "2", 2))));
        let buy = BookOp::Modify { target: OrderRef::Oid(1), always_place: true, order: order(Side::Bid, "110", "0.095", Tif::Alo, None) };
        let (effects, bbo) = run(&[action(0, buy)]);
        assert_eq!((outcomes(&effects), bbo.0), (vec!["alo_crossed"], Some(level("98", "2", 1))));
    }

    /// Asks at 105, 1.0 each: `orders` (oid, placed ms) in queue order.
    fn asks_at_105(orders: &[(u64, u64)]) -> OrderBook<InnerL4Order> {
        let mut book = OrderBook::new();
        for (oid, placed_ms) in orders {
            book.add_order(InnerL4Order { timestamp: *placed_ms, ..resting(*oid, ALICE, Side::Ask, "105", "1", None) });
        }
        book
    }

    /// An ALO ask of 1.0 at 105 with `priority`, seen at `seen_ms`.
    fn alo_at_105(index: usize, priority: u64, seen_ms: i64) -> PendingAction {
        let alo = Placement { priority, ..order(Side::Ask, "105", "1", Tif::Alo, None) };
        PendingAction { seen_ns: seen_ms * 1_000_000, ..action(index, BookOp::Place(alo)) }
    }

    fn pending(index: usize) -> Maker {
        Maker::Pending { id: id(index), cloid: None }
    }

    fn mine(oid: u64) -> Maker {
        confirmed(ALICE, oid, None)
    }

    /// The fills of `takers` (IOC bids at 105 of each size) after `actions` over `book`, the confirmed orders'
    /// `priorities`, at the confirmed block time `basis_ms`; `unproven`, the level's queue order not proven.
    fn takes(book: &OrderBook<InnerL4Order>, priorities: &[(u64, u64)], basis_ms: u64, unproven: bool, actions: &[PendingAction], takers: &[&str]) -> Vec<Vec<(Sz, Maker)>> {
        let priorities = priorities.iter().copied().collect();
        let basis = Basis::new(&priorities, no_refusals(), basis_ms);
        let levels = [(Side::Ask, px("105"))];
        let base = CoinBase::new(Some(book), if unproven { &levels } else { &[] }, None);
        let mut overlay = Overlay::default();
        for action in actions {
            overlay.apply(base, basis, action);
        }
        takers
            .iter()
            .enumerate()
            .map(|(index, size)| {
                let ioc = action(100 + index, BookOp::Place(order(Side::Bid, "105", size, Tif::Ioc, None)));
                overlay.apply(base, basis, &ioc).fills.into_iter().map(|fill| (fill.sz, fill.maker)).collect()
            })
            .collect()
    }

    /// The makers an IOC of 3 meets.
    fn queue(book: &OrderBook<InnerL4Order>, priorities: &[(u64, u64)], basis_ms: u64, actions: &[PendingAction]) -> Vec<Maker> {
        takes(book, priorities, basis_ms, false, actions, &["3"]).remove(0).into_iter().map(|(_, maker)| maker).collect()
    }

    /// Batch 2f, HL docs "Priority fees"' example: A (no fee) placed at t', B (fee p) at t' + T - 1 ms goes in front of
    /// it, C (fee p' > p) at t' + T + 1 ms behind it - B, A, C -, whether A is a replica order or pending.
    #[test]
    fn the_docs_example_queues_b_a_c() {
        let (b, c) = (alo_at_105(1, 5, 1_399), alo_at_105(2, 9, 1_401));
        assert_eq!(queue(&asks_at_105(&[(10, 1_000)]), &[], 0, &[b.clone(), c.clone()]), [pending(1), mine(10), pending(2)]);
        assert_eq!(queue(&asks_at_105(&[]), &[], 0, &[alo_at_105(0, 0, 1_000), b, c]), [pending(1), pending(0), pending(2)]);
    }

    /// Batch 2f: a pending ALO passes from the back each order placed under 400 ms before it with a lower priority,
    /// and stops at the first other: one of 6 at 1,300 ms passes a replica order of 3 placed at 1,000, not one placed
    /// at 800 or one of 9; nor one of none behind a younger one of 9 (only the tail compares); it passes two young
    /// orders of none and stops at an old one, taken between them.
    #[test]
    fn a_pending_alo_passes_the_tail_of_lower_priorities_under_the_window() {
        let alo = alo_at_105(1, 6, 1_300);
        let book = asks_at_105(&[(10, 1_000)]);
        assert_eq!(queue(&book, &[(10, 3)], 0, std::slice::from_ref(&alo)), [pending(1), mine(10)]);
        assert_eq!(queue(&asks_at_105(&[(10, 800)]), &[(10, 3)], 0, std::slice::from_ref(&alo)), [mine(10), pending(1)]);
        assert_eq!(queue(&book, &[(10, 9)], 0, std::slice::from_ref(&alo)), [mine(10), pending(1)]);
        assert_eq!(queue(&asks_at_105(&[(10, 1_200), (11, 1_250)]), &[(11, 9)], 0, std::slice::from_ref(&alo)), [mine(10), mine(11), pending(1)]);
        let fills = takes(&asks_at_105(&[(10, 500), (11, 1_200), (12, 1_250)]), &[], 0, false, &[alo], &["1.5"]);
        assert_eq!(fills, [vec![(sz("1"), mine(10)), (sz("0.5"), pending(1))]]);
    }

    /// Review 000620 r1 P2-1: a pending order is placed no earlier than the confirmed block - seen at 1,100 ms with the
    /// block at 1,800, a replica order of 1,000 is 800 ms older -; 400 ms exactly is out of the window; two pending ones
    /// in one ms compare in execution order, a later higher priority going in front, an equal one behind.
    #[test]
    fn the_window_runs_from_the_confirmed_block_and_excludes_400_ms() {
        let book = asks_at_105(&[(10, 1_000)]);
        assert_eq!(queue(&book, &[], 1_800, &[alo_at_105(1, 6, 1_100)]), [mine(10), pending(1)]);
        assert_eq!(queue(&book, &[], 0, &[alo_at_105(1, 6, 1_400)]), [mine(10), pending(1)]);
        assert_eq!(queue(&book, &[], 0, &[alo_at_105(1, 6, 1_399)]), [pending(1), mine(10)]);
        let empty = asks_at_105(&[]);
        assert_eq!(queue(&empty, &[], 0, &[alo_at_105(1, 3, 2_000), alo_at_105(2, 6, 2_000)]), [pending(2), pending(1)]);
        assert_eq!(queue(&empty, &[], 0, &[alo_at_105(1, 3, 2_000), alo_at_105(2, 3, 2_050)]), [pending(1), pending(2)]);
    }

    /// Batch 2f: without a priority an ALO queues behind; a replica order the overlay took out stops nothing - its
    /// cancel by oid ahead, the ALO passes the young one of none before it -; at a level whose queue order is not proven
    /// the ALO queues behind and the taker's fills there name no maker (review 000620 r2: the ALO would go in front of
    /// both replica orders, the second taker's naming it wrong).
    #[test]
    fn no_priority_an_order_taken_out_and_an_unproven_level() {
        let book = asks_at_105(&[(10, 1_200), (11, 1_250)]);
        assert_eq!(queue(&book, &[(11, 9)], 0, &[alo_at_105(1, 0, 1_300)]), [mine(10), mine(11), pending(1)]);
        let cancel = action(0, BookOp::Cancel(Oid::new(11)));
        assert_eq!(queue(&book, &[(11, 9)], 0, &[cancel, alo_at_105(1, 6, 1_300)]), [pending(1), mine(10)]);
        let unproven = takes(&book, &[], 0, true, &[alo_at_105(1, 5, 1_300)], &["1", "2"]);
        assert_eq!(unproven, [vec![(sz("1"), Maker::Unattributed)], vec![(sz("2"), Maker::Unattributed)]]);
        // behind them there: an IOC takes the replica's first, whose cancel then finds it gone; the next IOC takes 2
        let cancel = action(3, BookOp::Cancel(Oid::new(10)));
        let unproven = takes(&book, &[], 0, true, &[alo_at_105(1, 5, 1_300), action(2, BookOp::Place(order(Side::Bid, "105", "1", Tif::Ioc, None))), cancel], &["2"]);
        assert_eq!(unproven, [vec![(sz("2"), Maker::Unattributed)]]);
    }

    /// Review 000620 r1 P2-4: a replica order A and a pending one P of 1.0 each at 105, an IOC of 1, a cancel of A by oid,
    /// another IOC of 1. P in front of A, the first IOC takes P and the cancel A: the second takes nothing at 105; with P
    /// behind A, the first takes A, the cancel finds it gone and the second takes P.
    #[test]
    fn the_queue_decides_what_a_cancel_leaves() {
        let book = asks_at_105(&[(10, 1_200)]);
        let basis = no_priorities();
        let base = CoinBase::new(Some(&book), &[], None);
        for (priority, expected) in [(5, [vec![pending(1)], vec![]]), (0, [vec![mine(10)], vec![pending(1)]])] {
            let mut overlay = Overlay::default();
            overlay.apply(base, basis, &alo_at_105(1, priority, 1_300));
            let first = overlay.apply(base, basis, &action(2, BookOp::Place(order(Side::Bid, "105", "1", Tif::Ioc, None))));
            overlay.apply(base, basis, &action(3, BookOp::Cancel(Oid::new(10))));
            let second = overlay.apply(base, basis, &action(4, BookOp::Place(order(Side::Bid, "105", "1", Tif::Ioc, None))));
            let makers = [first, second].map(|effect| effect.fills.into_iter().map(|fill| fill.maker).collect::<Vec<_>>());
            assert_eq!(makers, expected, "priority {priority}");
        }
    }

    /// Batch 2g: a new order that may take, of a taker refused for margin on its coin and side at block time 1,000 ms,
    /// its line coming within [1,000, 1,000 + M]: rejected, after the minimum value's rule; an ALO, a modify, another
    /// identity, side or asset, an earlier or a later line, or no identity, as before.
    #[test]
    fn a_taker_lately_refused_for_margin_is_refused_again() {
        use crate::pending::{
            refusals::{TakerKey, TakerOutcome},
            signer::ActingIdentity,
        };
        const M: u64 = 300_000;
        let taker = ActingIdentity::Signer(ALICE);
        let mut refusals = TakerRefusals::new(M);
        refusals.stage(7, 1_000, vec![TakerOutcome { key: TakerKey { identity: taker, asset: 0, is_buy: true }, refused: true }]);
        refusals.apply_through(7);
        let priorities = FxHashMap::default();
        let basis = Basis::new(&priorities, &refusals, 1_000);
        let (book, cloids) = book();
        let base = CoinBase::new(Some(&book), &[], Some(&cloids));
        let by = |op: BookOp, vault: Option<Address>, signer: Option<Address>, asset: u32, seen_ms: i64| {
            let action = PendingAction { seen_ns: seen_ms * 1_000_000, vault, signer, asset, ..action(0, op) };
            Overlay::default().apply(base, basis, &action).outcome
        };
        let ioc = |side, at: &str| BookOp::Place(order(side, at, "1", Tif::Ioc, None));
        let signed = |op: BookOp, seen_ms: i64| by(op, None, Some(ALICE), 0, seen_ms);
        let refused = "margin_refused_recently";
        assert_eq!(signed(ioc(Side::Bid, "102"), 1_000), refused);
        assert_eq!(signed(ioc(Side::Bid, "102"), 1_000 + 300_000), refused);
        assert_eq!(signed(BookOp::Place(order(Side::Bid, "102", "1", Tif::FrontendMarket, None)), 2_000), refused);
        assert_eq!(signed(BookOp::Place(order(Side::Bid, "100", "1", Tif::Gtc, None)), 2_000), refused, "a GTC not crossing too");
        assert_eq!(signed(ioc(Side::Bid, "101"), 999), "took", "a line before the refusal's block");
        assert_eq!(signed(ioc(Side::Bid, "101"), 1_001 + 300_000), "took");
        assert_eq!(signed(ioc(Side::Ask, "99"), 2_000), "took", "the other side");
        assert_eq!(signed(BookOp::Place(order(Side::Bid, "100", "1", Tif::Alo, None)), 2_000), "rested");
        assert_eq!(signed(BookOp::Place(order(Side::Bid, "101", "0.01", Tif::Ioc, None)), 2_000), "min_notional", "the minimum value first");
        let modify = BookOp::Modify { target: OrderRef::Oid(1), always_place: true, order: order(Side::Bid, "102", "1", Tif::Ioc, None) };
        assert_eq!(signed(modify, 2_000), "took", "a modify is not judged by it");
        assert_eq!(by(ioc(Side::Bid, "101"), None, Some(ALICE), 1, 2_000), "took", "another asset");
        assert_eq!(by(ioc(Side::Bid, "101"), None, Some(BOB), 0, 2_000), "took", "another signer");
        assert_eq!(by(ioc(Side::Bid, "101"), Some(ALICE), None, 0, 2_000), "took", "a vault of the same address");
        assert_eq!(by(ioc(Side::Bid, "101"), None, None, 0, 2_000), "took", "a signer unknown");
    }
}
