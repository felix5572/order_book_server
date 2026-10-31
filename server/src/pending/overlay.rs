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
//! - A level whose queue order the replica cannot prove names none of its replica orders as a fill's maker; its
//!   pending orders queue behind them all the same, so what a taker takes past the replica's part is theirs.
//! - Self-trades, margin, reduce-only and price bands are not modelled (design 000604's non-goals).

use std::collections::{BTreeMap, VecDeque};

use alloy::primitives::Address;
use rustc_hash::FxHashMap;

use super::actions::{ActionId, BookOp, PendingAction, Placement, Tif};
use crate::{
    accounts::replica::OrderRef,
    order_book::{Oid, OrderBook, PriceLevel, Px, Side, Sz},
    types::inner::InnerL4Order,
};

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
}

/// One coin's overlay: the replay of its known pending actions, in execution order, over the replica.
#[derive(Default)]
pub(crate) struct Overlay {
    /// Replica orders the actions reduced: the size left, zero once gone.
    left: FxHashMap<Oid, u64>,
    /// Per side (`at`) and replica level: the (size, orders) `left` took off it.
    taken: [FxHashMap<Px, (u64, usize)>; 2],
    /// Pending orders resting, per side: price to queue in arrival order, never an empty one.
    resting: [BTreeMap<Px, VecDeque<RestingOrder>>; 2],
}

impl Overlay {
    pub(crate) fn apply(&mut self, base: CoinBase<'_>, action: &PendingAction) -> Effect {
        match &action.op {
            BookOp::Place(order) => self.place(base, action.id, order),
            BookOp::Cancel(oid) => self.cancel(self.confirmed_target(base, oid)),
            BookOp::CancelByCloid(cloid) => self.cancel(self.cloid_target(base, cloid)),
            BookOp::Modify { target, always_place, order } => self.modify(base, action.id, target, *always_place, order),
        }
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

    fn place(&mut self, base: CoinBase<'_>, id: ActionId, order: &Placement) -> Effect {
        match order.tif {
            Tif::Alo if self.crosses(base, order.side, order.limit_px) => Effect::of("alo_crossed"),
            Tif::Alo => {
                self.rest(order, id, order.sz.value());
                Effect::of("rested")
            }
            Tif::Gtc => {
                let (fills, left) = self.take(base, order.side, order.limit_px, order.sz.value());
                if left > 0 {
                    self.rest(order, id, left);
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
    fn modify(&mut self, base: CoinBase<'_>, id: ActionId, target: &OrderRef, always_place: bool, order: &Placement) -> Effect {
        let target = match target {
            OrderRef::Oid(oid) => self.confirmed_target(base, &Oid::new(*oid)),
            OrderRef::Cloid(cloid) => self.cloid_target(base, cloid),
        };
        if always_place {
            self.remove(target);
            return self.place(base, id, order);
        }
        match (order.tif, target) {
            (_, Target::Missing) => Effect::of("target_missing"),
            (_, Target::Ambiguous) => Effect::of("target_ambiguous"),
            (Tif::Alo, found) => {
                self.remove(found);
                self.place(base, id, order)
            }
            (Tif::Gtc, found) if !self.crosses_without(base, order.side, order.limit_px, &found) => {
                self.remove(found);
                self.rest(order, id, order.sz.value());
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

    /// Whether an ALO of `side` at `px` would take: the merged opposite side's best reaches it.
    fn crosses(&self, base: CoinBase<'_>, side: Side, px: Px) -> bool {
        self.crosses_without(base, side, px, &Target::Missing)
    }

    /// `crosses` once `target` is out (revision 8: a GTC modify's new order would take after its target's cancel).
    fn crosses_without(&self, base: CoinBase<'_>, side: Side, px: Px, target: &Target) -> bool {
        self.best_without(base, side.opposite(), target).is_some_and(|best| reaches(side, px, best))
    }

    /// A taker of `side` up to `limit_px`, `sz` at most, through the merged opposite side from its best: at each level
    /// the replica's orders in queue order, then the pending ones (they rest behind). The fills, and the size left.
    fn take(&mut self, base: CoinBase<'_>, side: Side, limit_px: Px, mut sz: u64) -> (Vec<Fill>, u64) {
        let makers = side.opposite();
        let mut fills = Vec::new();
        while sz > 0 {
            let Some(px) = self.best(base, makers).filter(|best| reaches(side, limit_px, *best)) else { break };
            sz = self.take_confirmed(base, makers, px, sz, &mut fills);
            sz = self.take_pending(makers, px, sz, &mut fills);
        }
        (fills, sz)
    }

    fn take_confirmed(&mut self, base: CoinBase<'_>, side: Side, px: Px, mut sz: u64, fills: &mut Vec<Fill>) -> u64 {
        let Some(level) = base.level(side, px) else { return sz };
        let unproven = base.unproven(side, px);
        let mut unattributed = 0;
        for order in level.orders() {
            if sz == 0 {
                break;
            }
            let oid = Oid::new(order.oid);
            let left = self.left.get(&oid).copied().unwrap_or_else(|| order.sz.value());
            let fill = left.min(sz);
            if fill == 0 {
                continue;
            }
            sz -= fill;
            self.reduce(side, px, oid.clone(), left, fill);
            if unproven {
                unattributed += fill;
            } else {
                fills.push(Fill { px, sz: Sz::new(fill), maker: Maker::Confirmed { user: order.user, oid, cloid: order.cloid.clone() } });
            }
        }
        if unattributed > 0 {
            fills.push(Fill { px, sz: Sz::new(unattributed), maker: Maker::Unattributed });
        }
        sz
    }

    fn take_pending(&mut self, side: Side, px: Px, mut sz: u64, fills: &mut Vec<Fill>) -> u64 {
        let levels = &mut self.resting[at(side)];
        let Some(queue) = levels.get_mut(&px) else { return sz };
        while sz > 0
            && let Some(maker) = queue.front_mut()
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

    fn rest(&mut self, order: &Placement, id: ActionId, sz: u64) {
        let queue = self.resting[at(order.side)].entry(order.limit_px).or_default();
        queue.push_back(RestingOrder { id, cloid: order.cloid.clone(), sz });
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
        PendingAction { id: id(index), seen_ns: 0, vault: None, op, unknown: false }
    }

    fn order(side: Side, at: &str, quantity: &str, tif: Tif, cloid: Option<&str>) -> Placement {
        Placement { side, limit_px: px(at), sz: sz(quantity), tif, cloid: cloid.map(str::to_string) }
    }

    fn place(index: usize, side: Side, at: &str, quantity: &str, tif: Tif, cloid: Option<&str>) -> PendingAction {
        action(index, BookOp::Place(order(side, at, quantity, tif, cloid)))
    }

    fn level(at: &str, size: &str, orders: u32) -> BestLevel {
        (px(at), sz(size), orders)
    }

    /// Applies `actions` in order: their outcomes, then the merged book's best levels.
    fn run(actions: &[PendingAction]) -> (Vec<Effect>, (Option<BestLevel>, Option<BestLevel>)) {
        let (book, cloids) = book();
        let base = CoinBase::new(Some(&book), &[], Some(&cloids));
        let mut overlay = Overlay::default();
        let effects = actions.iter().map(|action| overlay.apply(base, action)).collect();
        (effects, overlay.bbo(base))
    }

    fn outcomes(effects: &[Effect]) -> Vec<&'static str> {
        effects.iter().map(|effect| effect.outcome).collect()
    }

    fn confirmed(user: Address, oid: u64, cloid: Option<&str>) -> Maker {
        Maker::Confirmed { user, oid: Oid::new(oid), cloid: cloid.map(str::to_string) }
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

    /// A level whose queue order the replica cannot prove gives one fill without a maker for its replica orders, the
    /// size taken of them; a pending order there queues behind them all, so what is taken past them is its own (review
    /// 000611 r1 P2-3).
    #[test]
    fn an_unproven_level_names_no_maker_of_its_replica_orders() {
        let (book, cloids) = book();
        let unproven = [(Side::Ask, px("101"))];
        let base = CoinBase::new(Some(&book), &unproven, Some(&cloids));
        let mut overlay = Overlay::default();
        overlay.apply(base, &place(0, Side::Ask, "101", "1", Tif::Alo, Some("0xa")));
        let effect = overlay.apply(base, &place(1, Side::Bid, "102", "3.5", Tif::Ioc, None));
        assert_eq!(
            effect.fills,
            [
                Fill { px: px("101"), sz: sz("2"), maker: Maker::Unattributed },
                Fill { px: px("101"), sz: sz("1"), maker: Maker::Pending { id: id(0), cloid: Some("0xa".to_string()) } },
                Fill { px: px("102"), sz: sz("0.5"), maker: confirmed(BOB, 5, None) },
            ]
        );
    }

    /// A coin without a replica book: the pending orders alone.
    #[test]
    fn a_coin_without_a_book_has_its_pending_orders_alone() {
        let base = CoinBase::new(None, &[], None);
        let mut overlay = Overlay::default();
        let effect = overlay.apply(base, &place(0, Side::Bid, "5", "1", Tif::Gtc, None));
        assert_eq!(effect.outcome, "rested");
        assert_eq!(overlay.bbo(base), (Some(level("5", "1", 1)), None));
    }
}
