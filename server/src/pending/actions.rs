//! A mempool transaction's book actions, each on its named coin, in execution order (hyperliquid-trade design 000604
//! revision 4): a line is one bundle, run whole in the order bm read the lines (research `mempool_pending_book_20261031`,
//! 13,989 bundles with book actions, none run after a later one); inside it the signed actions by `placing_category`,
//! each category in listed order, and each action's orders, cancels or modifies in their own order. A cancel's `f`
//! (fast) does not move it there (1.8M fast and 43k plain cancels alike), and trigger orders - a modify placing one too -
//! never touch the book before they trigger: left out. HL refuses some before any book by rule, and so does the set:
//! a signed action past its `expiresAfter` when its line came, or with an order off its coin's price or size grid -
//! the whole action, its orders on the grid with it (HL's pre-validation, review 000617 r1 P2-1).

use alloy::primitives::{Address, B256};

use super::{
    mempool::{ActionKind, MempoolTx, RequestedOrder, RequestedTif, RequestedTrigger, SignedAction, TriggerGrouping},
    refusals::TakerKey,
    signer::ActingIdentity,
    signer_accounts::SignerAccounts,
    triggers::SetGlobalCoin,
};
use crate::{
    accounts::{
        book::{ActionAsset, ActionCoins},
        forced_flow::OrderKind,
        replica::{CANCEL_CATEGORY, OrderRef, placing_category},
    },
    listeners::order_book::triggers::Direction,
    metrics::PENDING_BOOK_TRIGGER_ACCOUNTS_TOTAL,
    order_book::{Coin, Px, Side, Sz},
};

/// One order, cancel or modify of the mempool: its transaction, its signed action's place there, its own place in the
/// action (design 000604 revision 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ActionId {
    pub(crate) tx_hash: B256,
    pub(crate) action: usize,
    pub(crate) item: usize,
}

/// A book order as requested.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Placement {
    pub(crate) side: Side,
    pub(crate) limit_px: Px,
    pub(crate) sz: Sz,
    pub(crate) tif: Tif,
    pub(crate) cloid: Option<String>,
    /// Its action's ALO priority (batch 2f; `Action::alo_priority`): a modify's new order has none.
    pub(crate) priority: u64,
}

/// A book order's time in force.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Tif {
    Alo,
    Gtc,
    Ioc,
    /// Taking as an IOC does, at its limit price.
    FrontendMarket,
}

impl Tif {
    pub(crate) const fn of_requested(tif: RequestedTif) -> Option<Self> {
        match tif {
            RequestedTif::Alo => Some(Self::Alo),
            RequestedTif::Gtc => Some(Self::Gtc),
            RequestedTif::Ioc => Some(Self::Ioc),
            RequestedTif::FrontendMarket => Some(Self::FrontendMarket),
            RequestedTif::Trigger => None,
        }
    }

    /// Whether it may take on placing: an IOC, a `FrontendMarket` or a GTC, not an ALO - the orders HL refuses for a
    /// taker's margin as the pending set takes them (batch 2g, design 000621 r3).
    pub(crate) const fn may_take(self) -> bool {
        !matches!(self, Self::Alo)
    }
}

/// What an action does to its coin's book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BookOp {
    Place(Placement),
    /// A `cancel`'s or a `cancelByCloid`'s one: its target out; a fast one (`f`) refused for an untriggered trigger order
    /// (HL docs "Cancel order(s)"; review 000652 r6 P2-1).
    Cancel { target: OrderRef, fast: bool },
    /// A `modify`'s or one of a `batchModify`'s: its target out, its new order in (design 000604 revision 8).
    Modify { target: OrderRef, always_place: bool, order: Placement },
    /// A trigger order placed (batch 2m): untriggered from now on, until a `SetGlobal` fires it or an action takes it out.
    PlaceTrigger(PendingTrigger),
    /// A modify whose new order is a trigger order: its target out, the trigger order in.
    ModifyToTrigger { target: OrderRef, always_place: bool, order: PendingTrigger },
    /// A `SetGlobalAction` on the coin (batch 2m): it fires the untriggered orders its forecast mark reaches.
    SetGlobal(Box<SetGlobalCoin>),
}

/// A trigger order as the mempool places it (batch 2m, hyperliquid-trade design 000652): its direction the mark reaches
/// it from - a stop loss's sale and a take profit's buy on a fall, the others on a rise (bm 11-01 19:00-20:00, 122,804
/// trigger orders opened, every one so) -, its limit its own (a market one runs past it: `forced_flow::trigger_limit`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingTrigger {
    pub(crate) side: Side,
    pub(crate) trigger_px: Px,
    pub(crate) direction: Direction,
    pub(crate) kind: OrderKind,
    pub(crate) limit_px: Px,
    pub(crate) sz: Sz,
    pub(crate) reduce_only: bool,
    pub(crate) position_tpsl: bool,
    pub(crate) cloid: Option<String>,
}

impl PendingTrigger {
    fn of(order: &RequestedOrder, trigger: RequestedTrigger) -> Self {
        let side = if order.is_buy { Side::Bid } else { Side::Ask };
        let direction = match (side, trigger.take_profit) {
            (Side::Ask, false) | (Side::Bid, true) => Direction::Down,
            (Side::Bid, false) | (Side::Ask, true) => Direction::Up,
        };
        Self {
            side,
            trigger_px: trigger.trigger_px,
            direction,
            kind: if trigger.is_market { OrderKind::Market } else { OrderKind::Limit },
            limit_px: order.limit_px,
            sz: order.sz,
            reduce_only: order.reduce_only,
            position_tpsl: trigger.grouping == TriggerGrouping::Position,
            cloid: order.cloid.clone(),
        }
    }
}

impl BookOp {
    /// The order it places.
    pub(crate) const fn placement(&self) -> Option<&Placement> {
        match self {
            Self::Place(order) | Self::Modify { order, .. } => Some(order),
            Self::Cancel { .. } | Self::PlaceTrigger(_) | Self::ModifyToTrigger { .. } | Self::SetGlobal(_) => None,
        }
    }

    pub(crate) const fn label(&self) -> &'static str {
        match self {
            Self::Place(Placement { tif: Tif::Alo, .. }) => "place_alo",
            Self::Place(Placement { tif: Tif::Gtc, .. }) => "place_gtc",
            Self::Place(Placement { tif: Tif::Ioc | Tif::FrontendMarket, .. }) => "place_ioc",
            Self::Cancel { target: OrderRef::Oid(_), .. } => "cancel",
            Self::Cancel { target: OrderRef::Cloid(_), .. } => "cancel_by_cloid",
            Self::Modify { .. } => "modify",
            Self::PlaceTrigger(_) => "place_trigger",
            Self::ModifyToTrigger { .. } => "modify_trigger",
            Self::SetGlobal(_) => "set_global",
        }
    }
}

/// An action held in the pending set until a block confirms it or it expires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingAction {
    pub(crate) id: ActionId,
    /// Its line's `local_time`, unix ns.
    pub(crate) seen_ns: i64,
    /// The vault or subaccount it acts for, when the line names one.
    pub(crate) vault: Option<Address>,
    /// Its signer, when recovered (`SignedAction::signer`).
    pub(crate) signer: Option<Address>,
    /// The account it acts for as known as it came (batch 2m-2, `account_of`): its vault, else its signer's as the block
    /// log taught it (`SignerAccounts`).
    pub(crate) account: Option<Address>,
    /// The asset id it names.
    pub(crate) asset: u32,
    pub(crate) op: BookOp,
    /// Never overlaid: seen in a gap or its warm-up (revisions 7, 11), or not told apart from a confirmed one
    /// (revision 2).
    pub(crate) unknown: bool,
}

impl PendingAction {
    /// The key of its taker's margin refusals (batch 2g): a new order that may take, of a known identity.
    pub(crate) fn taker_key(&self) -> Option<TakerKey> {
        let BookOp::Place(order) = &self.op else { return None };
        let identity = ActingIdentity::of(self.vault, self.signer).filter(|_| order.tif.may_take())?;
        Some(TakerKey { identity, asset: self.asset, is_buy: order.side == Side::Bid })
    }
}

/// A line's book actions: those held, in execution order, and those HL refuses by rule before any book - each one's
/// [op label, why] (`expires_after`; `off_grid`, or `off_grid_batch` for one on the grid refused with its action;
/// `price_band` for every order of an action one of whose orders is under the price band; `too_large`).
#[derive(Debug, Default)]
pub(crate) struct BookActions {
    pub(crate) held: Vec<(Coin, PendingAction)>,
    pub(crate) refused: Vec<[&'static str; 2]>,
}

/// `tx`'s book actions on the coins `coins` names, in execution order; spot left out when `spot` is not kept (the
/// replica's book has none then). An asset the table does not name is the mempool reading's count. `below_band(coin,
/// limit)`: whether an order's limit is under its coin's price band (batch 2k) - HL refuses its whole signed action
/// before any of it, one status for all (review 000637 r1 P2-1; bm 11-01 13:20-14:30, 479 actions of several orders
/// answered with the one error, none a status per order).
pub(crate) fn book_actions(tx: &MempoolTx, coins: &ActionCoins, spot: bool, below_band: &dyn Fn(&str, Px) -> bool, accounts: &SignerAccounts) -> BookActions {
    let mut ranked: Vec<(u8, usize)> = tx.actions.iter().enumerate().filter_map(|(index, action)| Some((category(&action.kind)?, index))).collect();
    ranked.sort_unstable();
    let named = |asset: u32| coins.coins.get(&asset).filter(|named| spot || !Coin::str_is_spot(&named.coin));
    let off_grid = |order: &RequestedOrder| coins.coins.get(&order.asset).is_some_and(|named| !on_grid(order, named));
    let seen_ms = tx.seen_ns / 1_000_000;
    let mut actions = BookActions::default();
    for (_, index) in ranked {
        let signed = &tx.actions[index];
        let expired = signed.expires_after_ms.is_some_and(|expires_ms| i64::try_from(expires_ms).is_ok_and(|expires_ms| expires_ms < seen_ms));
        // an order off the grid fails the whole signed action, trigger orders' prices too; one under the price band too,
        // trigger orders not judged (no sample of theirs)
        let batch_off_grid = signed.kind.orders().any(off_grid);
        let banded = band_orders(&signed.kind, coins).any(|(coin, limit_px)| below_band(coin, limit_px));
        for (item, asset, op, order) in book_ops(&signed.kind, signed.priority) {
            let Some(named) = named(asset) else { continue };
            if expired || batch_off_grid || banded {
                let why = if expired {
                    "expires_after"
                } else if batch_off_grid {
                    if order.is_some_and(off_grid) { "off_grid" } else { "off_grid_batch" }
                } else {
                    "price_band"
                };
                actions.refused.push([op.label(), why]);
                continue;
            }
            if order.is_some_and(|order| too_large(order, named)) {
                actions.refused.push([op.label(), "too_large"]);
                continue;
            }
            let id = ActionId { tx_hash: tx.tx_hash, action: index, item };
            let account = account_of(signed, accounts, &op);
            let action = PendingAction { id, seen_ns: tx.seen_ns, vault: signed.vault, signer: signed.signer, account, asset, op, unknown: false };
            actions.held.push((Coin::new(&named.coin), action));
        }
    }
    actions
}

/// The account `action` acts for as known as it comes (batch 2m-2): its vault, else its signer's as the block log taught
/// it; for a trigger order placed, how it was known counted.
fn account_of(action: &SignedAction, accounts: &SignerAccounts, op: &BookOp) -> Option<Address> {
    let known = match (action.vault, action.signer) {
        (Some(vault), _) => Ok((vault, "vault")),
        (None, Some(signer)) => accounts.account(signer).map(|account| (account, "learned")),
        (None, None) => Err("no_signer"),
    };
    if matches!(op, BookOp::PlaceTrigger(_) | BookOp::ModifyToTrigger { .. }) {
        PENDING_BOOK_TRIGGER_ACCOUNTS_TOTAL.with_label_values(&[known.map_or_else(|why| why, |(_, how)| how)]).inc();
    }
    known.ok().map(|(account, _)| account)
}

/// The (coin, limit) of each order of a signed action the price band judges (batch 2k): every one but a trigger order,
/// on a coin the table names - those `book_actions` refuses one by one included (review 000637 r3 P2-1).
pub(crate) fn band_orders<'a>(kind: &'a ActionKind, coins: &'a ActionCoins) -> impl Iterator<Item = (&'a str, Px)> + 'a {
    kind.orders()
        .filter(|order| order.tif != RequestedTif::Trigger)
        .filter_map(|order| coins.coins.get(&order.asset).map(|named| (named.coin.as_str(), order.limit_px)))
}

/// Whether an order is on its coin's grid: a size of at most `szDecimals` decimals; a price of at most `max_decimals -
/// szDecimals` decimals and 5 significant figures, any integer price (HL API docs "Tick and lot size"). Off it, HL's
/// pre-validation fails the whole signed action, without a status for any of its orders (API docs "Error responses":
/// one error for the payload, "some forms of tick size validation"; research `mempool_pending_book_20261031`, bm 10-31
/// 18:00-18:20: 4 actions mixing orders off and on the price grid, none of their orders with a status, the block
/// replying one tick error; 18:01-18:03: of 445,318 orders by cloid, the 66 off the price grid had none, but one whose
/// cloid another order shared). A size off it, never seen mixed, is taken as the same: a payload's own fault.
const fn on_grid(order: &RequestedOrder, asset: &ActionAsset) -> bool {
    let (Some(px), Some(sz)) = (order.px_shape, order.sz_shape) else { return false };
    let px_ok = px.decimals == 0 || (px.decimals + asset.size_decimals <= asset.max_decimals && px.significant <= 5);
    px_ok && sz.decimals <= asset.size_decimals
}

/// A book action's category (`placing_category`); `None` for any other type.
fn category(kind: &ActionKind) -> Option<u8> {
    match kind {
        ActionKind::Order(orders) => Some(placing_category(orders.iter().all(|order| order.tif == RequestedTif::Alo))),
        ActionKind::Modify { modifies, .. } => Some(placing_category(modifies.iter().all(|modify| modify.order.tif == RequestedTif::Alo))),
        ActionKind::Cancel { .. } | ActionKind::CancelByCloid { .. } => Some(CANCEL_CATEGORY),
        ActionKind::SetGlobal(_) | ActionKind::TrailingStop | ActionKind::Other => None,
    }
}

/// Whether HL refuses an order for its value: worth more at its limit than its coin's largest market order value, a
/// limit order 10 times it - an IOC taken as a limit order, an inference (HL docs "Contract specifications"). HL fails
/// that order alone, the action's others going on (bm 10-31 block responses, "Order value too large. Max is $500000":
/// LIT market orders from $509,879; HYPE's "Max is $2000000" at $2,027,003; every one a `FrontendMarket`).
fn too_large(order: &RequestedOrder, asset: &ActionAsset) -> bool {
    let Some(market) = asset.max_market_value else { return false };
    let cap = if order.tif == RequestedTif::FrontendMarket { market } else { market * 10 };
    u128::from(order.limit_px.value()) * u128::from(order.sz.value()) > u128::from(cap) * 10_000_000_000_000_000
}

/// Each (place in the action, asset, op, the order it places as requested) of an action, in its order, its orders
/// placed with `priority`; a trigger order placed, or a modify placing one, an op of its own (batch 2m; design 000604
/// revision 8 had left them out).
fn book_ops(kind: &ActionKind, priority: u64) -> Vec<(usize, u32, BookOp, Option<&RequestedOrder>)> {
    let ops: Vec<Option<(u32, BookOp, Option<&RequestedOrder>)>> = match kind {
        ActionKind::Order(orders) => orders.iter().map(|order| Some((order.asset, order_op(order, priority)?, Some(order)))).collect(),
        ActionKind::Cancel { fast, cancels } => {
            cancels.iter().map(|cancel| Some((cancel.a, BookOp::Cancel { target: OrderRef::Oid(cancel.o), fast: *fast }, None))).collect()
        }
        ActionKind::CancelByCloid { fast, cancels } => {
            cancels.iter().map(|cancel| Some((cancel.asset, BookOp::Cancel { target: OrderRef::Cloid(cancel.cloid.clone()), fast: *fast }, None))).collect()
        }
        ActionKind::Modify { always_place, modifies } => modifies
            .iter()
            .map(|modify| {
                let (target, always_place) = (modify.target.clone(), *always_place);
                let op = match modify.order.trigger {
                    Some(trigger) => BookOp::ModifyToTrigger { target, always_place, order: PendingTrigger::of(&modify.order, trigger) },
                    None => BookOp::Modify { target, always_place, order: placement(&modify.order, 0)? },
                };
                Some((modify.order.asset, op, Some(&modify.order)))
            })
            .collect(),
        ActionKind::SetGlobal(_) | ActionKind::TrailingStop | ActionKind::Other => Vec::new(),
    };
    ops.into_iter().enumerate().filter_map(|(item, op)| op.map(|(asset, op, order)| (item, asset, op, order))).collect()
}

/// An `order` action's order: a book order, or a trigger order of its own or of the whole position; a `normalTpsl`'s
/// child, waiting for its parent, is left out (batch 2m: not modelled).
fn order_op(order: &RequestedOrder, priority: u64) -> Option<BookOp> {
    match order.trigger {
        Some(RequestedTrigger { grouping: TriggerGrouping::Child, .. }) => None,
        Some(trigger) => Some(BookOp::PlaceTrigger(PendingTrigger::of(order, trigger))),
        None => Some(BookOp::Place(placement(order, priority)?)),
    }
}

fn placement(order: &RequestedOrder, priority: u64) -> Option<Placement> {
    Some(Placement {
        side: if order.is_buy { Side::Bid } else { Side::Ask },
        limit_px: order.limit_px,
        sz: order.sz,
        tif: Tif::of_requested(order.tif)?,
        cloid: order.cloid.clone(),
        priority,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use rustc_hash::FxHashMap;

    use super::*;
    use crate::{
        accounts::replica::{WireCancel, WireCancelByCloid},
        pending::mempool::{DecimalShape, Modify, SignedAction},
    };

    /// BTC, ETH and a spot pair, at their mainnet `szDecimals`; ETH's largest market order $500k, as a 5x coin's.
    pub(crate) fn coins() -> ActionCoins {
        let coins: FxHashMap<u32, ActionAsset> = [(0, "BTC", 5, 6, Some(30_000_000)), (1, "ETH", 4, 6, Some(500_000)), (10_107, "@107", 2, 8, None)]
            .into_iter()
            .map(|(asset, coin, size_decimals, max_decimals, max_market_value)| {
                (asset, ActionAsset { coin: coin.to_string(), size_decimals, max_decimals, max_market_value })
            })
            .collect();
        ActionCoins { coins }
    }

    pub(crate) fn requested(asset: u32, is_buy: bool, px: &str, tif: RequestedTif, cloid: Option<&str>) -> RequestedOrder {
        RequestedOrder {
            asset,
            is_buy,
            limit_px: Px::parse_from_str(px).unwrap(),
            sz: Sz::parse_from_str("1").unwrap(),
            px_shape: DecimalShape::of(px),
            sz_shape: DecimalShape::of("1"),
            reduce_only: false,
            cloid: cloid.map(str::to_string),
            tif,
            trigger: None,
        }
    }

    /// Batch 2d: what HL refuses by rule before any book, the set refuses - every op of a signed action past its
    /// `expiresAfter` when its line came (1,000 ms), not one expiring then; an order off its coin's grid, BTC's
    /// (szDecimals 5): a price of 6 significant figures, or of 2 decimals, a size of 6 decimals - an integer price of
    /// Review 000637 r1 P2-1: an action one of whose orders is under the price band is refused whole, every order of
    /// it `price_band`; another action of the line is held; a trigger order is not judged.
    #[test]
    fn an_action_under_the_price_band_is_refused_whole() {
        let below = |_: &str, px: Px| px < Px::parse_from_str("5").unwrap();
        let banded = ActionKind::Order(vec![requested(0, true, "4", RequestedTif::Ioc, None), requested(0, true, "101", RequestedTif::Ioc, None)]);
        let other = ActionKind::Order(vec![requested(0, true, "101", RequestedTif::Ioc, None)]);
        let actions = book_actions(&tx(vec![banded, other]), &coins(), false, &below, &SignerAccounts::default());
        assert_eq!(actions.refused, [["place_ioc", "price_band"]; 2]);
        assert_eq!(actions.held.iter().map(|(_, action)| action.id.action).collect::<Vec<_>>(), [1]);
        let trigger = ActionKind::Order(vec![requested(0, true, "4", RequestedTif::Trigger, None), requested(0, true, "101", RequestedTif::Ioc, None)]);
        let actions = book_actions(&tx(vec![trigger]), &coins(), false, &below, &SignerAccounts::default());
        assert_eq!((actions.held.len(), actions.refused.len()), (1, 0), "the trigger order's price not judged");
    }

    /// any length is on it, trailing zeros count for nothing.
    #[test]
    fn orders_off_the_grid_and_actions_expired_are_refused() {
        let order = |px: &str, size: &str| RequestedOrder {
            sz: Sz::parse_from_str(size).unwrap(),
            sz_shape: DecimalShape::of(size),
            ..requested(0, true, px, RequestedTif::Alo, None)
        };
        let mut line = tx(vec![ActionKind::Order(vec![order("123456", "0.001"), order("77655", "0.00100")])]);
        assert_eq!(book_actions(&line, &coins(), false, &|_, _| false, &SignerAccounts::default()).held.len(), 2);
        line.actions[0].expires_after_ms = Some(999);
        let actions = book_actions(&line, &coins(), false, &|_, _| false, &SignerAccounts::default());
        assert_eq!((actions.held.len(), actions.refused), (0, vec![["place_alo", "expires_after"]; 2]));
        line.actions[0].expires_after_ms = Some(1_000);
        assert_eq!(book_actions(&line, &coins(), false, &|_, _| false, &SignerAccounts::default()).held.len(), 2);
        for off in [order("77655.5", "0.001"), order("7765.55", "0.001"), order("77655", "0.000001")] {
            let line = tx(vec![ActionKind::Order(vec![off.clone()])]);
            assert_eq!(book_actions(&line, &coins(), false, &|_, _| false, &SignerAccounts::default()).refused, [["place_alo", "off_grid"]], "{off:?}");
        }
    }

    /// Review 000617 r1 P2-1: bm 10-31 18:04:08.825, line 0x71469c86..., signed action 230 - ten BTC ALOs, nine priced
    /// to 6 significant figures, one at 77679 -, block 1204250452 replying one `Price must be divisible by tick size.
    /// asset=0` and no order status at all: the action is refused whole, its order on the grid with it; another signed
    /// action of the same line, all on the grid, is held.
    #[test]
    fn an_order_off_the_grid_refuses_its_signed_action_whole() {
        let line = format!(r#"["2026-10-31T18:04:08.825404321",{{"tx_hash":"0x71469c86420999a72184eec1f563d9330d974568da21b0d26ca676fe99b70a3d","signed_actions":[{MIXED_TICK_ACTION},{ON_GRID_ACTION}]}}]"#);
        let tx = crate::pending::mempool::parse_line(&line).unwrap();
        let actions = book_actions(&tx, &coins(), false, &|_, _| false, &SignerAccounts::default());
        let mut refused = vec![["place_alo", "off_grid"]; 10];
        refused[4] = ["place_alo", "off_grid_batch"];
        assert_eq!(actions.refused, refused);
        assert_eq!(actions.held.iter().map(|(coin, action)| (coin.value(), action.id.action)).collect::<Vec<_>>(), [("BTC".to_string(), 1)]);
    }

    /// Batch 2e: an order worth more at its limit than its coin's largest - ETH's market order $500k here, a limit
    /// order 10 times it - is refused alone, the action's others going on.
    #[test]
    fn an_order_worth_too_much_is_refused_alone() {
        use RequestedTif::{FrontendMarket, Ioc};
        let order = |px: &str, size: &str, tif| RequestedOrder {
            sz: Sz::parse_from_str(size).unwrap(),
            sz_shape: DecimalShape::of(size),
            ..requested(1, true, px, tif, None)
        };
        let orders = vec![order("2000", "255", FrontendMarket), order("2000", "227", FrontendMarket), order("2000", "255", Ioc), order("2000", "2501", Ioc)];
        let actions = book_actions(&tx(vec![ActionKind::Order(orders)]), &coins(), false, &|_, _| false, &SignerAccounts::default());
        assert_eq!(actions.held.iter().map(|(_, action)| action.id.item).collect::<Vec<_>>(), [1, 2]);
        assert_eq!(actions.refused, [["place_ioc", "too_large"]; 2]);
    }

    const MIXED_TICK_ACTION: &str = r#"{"signature":{"r":"0xb8e8a2ed193ba5cda5b6599d5e5d67c476c9e115b9f86ce17fe347b1f2f8e145","s":"0x89a7eab30cebce29300ad9ac703e761085230a00b6253aa91ae9b6c2b9dad44","v":27},"vaultAddress":"0xf36c562be0598b8ce109998b6aa2d53ffcd8316b","action":{"type":"order","orders":[{"a":0,"b":false,"p":"77896.3","s":"0.62909","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x16173310847065010000000012405259"},{"a":0,"b":false,"p":"77807.6","s":"0.47182","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x16173310847065010000000012405258"},{"a":0,"b":false,"p":"77741.1","s":"0.31454","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x16173310847065010000000012405257"},{"a":0,"b":false,"p":"77696.7","s":"0.12582","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x16173310847065010000000012405256"},{"a":0,"b":false,"p":"77679","s":"0.03145","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x16173310847065010000000012405255"},{"a":0,"b":true,"p":"77395.7","s":"0.64086","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x16173310847065000000000012405254"},{"a":0,"b":true,"p":"77501.4","s":"0.48064","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x16173310847065000000000012405253"},{"a":0,"b":true,"p":"77580.7","s":"0.32043","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x16173310847065000000000012405252"},{"a":0,"b":true,"p":"77633.6","s":"0.12817","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x16173310847065000000000012405251"},{"a":0,"b":true,"p":"77653.5","s":"0.03204","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x16173310847065000000000012405250"}],"grouping":"na"},"nonce":1793469848762,"expiresAfter":1793469858741}"#;
    const ON_GRID_ACTION: &str = r#"{"signature":{"r":"0x1","s":"0x2","v":27},"action":{"type":"order","orders":[{"a":0,"b":true,"p":"77387","s":"0.135","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x01"}],"grouping":"na"},"nonce":1793469848000}"#;

    /// Batch 2m-2: an action's account as it comes - its vault; else its signer's as the block log taught it; none for a
    /// signer seen for two accounts, a signer not seen, or none recovered.
    #[test]
    fn an_actions_account_is_its_vault_else_its_signers() {
        let (signer, shared, alice, bob) = (Address::repeat_byte(0x51), Address::repeat_byte(0x52), Address::repeat_byte(0xa1), Address::repeat_byte(0xb0));
        let mut accounts = SignerAccounts::default();
        accounts.learn(1, signer, alice);
        accounts.learn(1, shared, alice);
        accounts.learn(2, shared, bob);
        let account_of = |vault, signer| {
            let mut line = tx(vec![ActionKind::Order(vec![requested(0, true, "100", RequestedTif::Ioc, None)])]);
            line.actions[0] = SignedAction { vault, signer, ..line.actions[0].clone() };
            book_actions(&line, &coins(), false, &|_, _| false, &accounts).held[0].1.account
        };
        let unseen = Address::repeat_byte(0x53);
        let accounts = [account_of(Some(bob), Some(signer)), account_of(None, Some(signer)), account_of(None, Some(shared)), account_of(None, Some(unseen)), account_of(None, None)];
        assert_eq!(accounts, [Some(bob), Some(alice), None, None, None]);
    }

    pub(crate) fn tx(actions: Vec<ActionKind>) -> MempoolTx {
        MempoolTx {
            seen_ns: 1_000_000_000,
            tx_hash: B256::repeat_byte(7),
            actions: actions.into_iter().map(|kind| SignedAction { vault: None, signer: None, expires_after_ms: None, priority: 0, kind }).collect(),
        }
    }

    fn ops(tx: &MempoolTx, spot: bool) -> Vec<(String, usize, usize, &'static str)> {
        book_actions(tx, &coins(), spot, &|_, _| false, &SignerAccounts::default()).held.into_iter().map(|(coin, action)| (coin.value(), action.id.action, action.id.item, action.op.label())).collect()
    }

    /// A bundle as `0xc9ba...` lists it - cancels, then ALOs, then takers - runs ALOs, then cancels, then the rest,
    /// each in listed order; a modify goes by its new orders, an order action mixing ALO and IOC with the takers.
    #[test]
    fn a_bundle_runs_alo_actions_then_cancels_then_the_rest() {
        let alo = |asset| requested(asset, true, "100", RequestedTif::Alo, None);
        let tx = tx(vec![
            ActionKind::Cancel { fast: true, cancels: vec![WireCancel { a: 0, o: 11 }] },
            ActionKind::Order(vec![requested(0, false, "99", RequestedTif::Ioc, Some("0x01"))]),
            ActionKind::Order(vec![alo(0), alo(1)]),
            ActionKind::CancelByCloid { fast: false, cancels: vec![WireCancelByCloid { asset: 1, cloid: "0x02".to_string() }] },
            ActionKind::Other,
            ActionKind::Modify { always_place: false, modifies: vec![Modify { target: OrderRef::Oid(12), order: alo(0) }] },
            ActionKind::Order(vec![alo(1), requested(1, true, "100", RequestedTif::Gtc, None)]),
        ]);
        assert_eq!(
            ops(&tx, true),
            [
                ("BTC".to_string(), 2, 0, "place_alo"),
                ("ETH".to_string(), 2, 1, "place_alo"),
                ("BTC".to_string(), 5, 0, "modify"),
                ("BTC".to_string(), 0, 0, "cancel"),
                ("ETH".to_string(), 3, 0, "cancel_by_cloid"),
                ("BTC".to_string(), 1, 0, "place_ioc"),
                ("ETH".to_string(), 6, 0, "place_alo"),
                ("ETH".to_string(), 6, 1, "place_gtc"),
            ]
        );
    }

    /// Trigger orders, a modify placing one, assets the table does not name and spot when the book has none are left
    /// out; the ids keep their places in the line (the ALO-only action first, the one with a trigger order after).
    #[test]
    fn what_never_touches_the_book_is_left_out() {
        let tx = tx(vec![
            ActionKind::Order(vec![requested(0, true, "1", RequestedTif::Trigger, None), requested(0, true, "100", RequestedTif::Alo, None)]),
            ActionKind::Modify {
                always_place: true,
                modifies: vec![Modify { target: OrderRef::Oid(1), order: requested(0, true, "1", RequestedTif::Trigger, None) }],
            },
            ActionKind::Order(vec![requested(99, true, "1", RequestedTif::Alo, None), requested(10_107, true, "1", RequestedTif::Alo, None)]),
        ]);
        assert_eq!(ops(&tx, true), [("@107".to_string(), 2, 1, "place_alo"), ("BTC".to_string(), 0, 1, "place_alo")]);
        assert_eq!(ops(&tx, false), [("BTC".to_string(), 0, 1, "place_alo")]);
    }
}
