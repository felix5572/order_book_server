//! A mempool transaction's book actions, each on its named coin, in execution order (hyperliquid-trade design 000604
//! revision 4): a line is one bundle, run whole in the order bm read the lines (research `mempool_pending_book_20261031`,
//! 13,989 bundles with book actions, none run after a later one); inside it the signed actions by `placing_category`,
//! each category in listed order, and each action's orders, cancels or modifies in their own order. A cancel's `f`
//! (fast) does not move it there (1.8M fast and 43k plain cancels alike), and trigger orders - a modify placing one too -
//! never touch the book before they trigger: left out. HL refuses some before any book by rule, and so does the set:
//! a signed action past its `expiresAfter` when its line came, or with an order off its coin's price or size grid -
//! the whole action, its orders on the grid with it (HL's pre-validation, review 000617 r1 P2-1).

use alloy::primitives::{Address, B256};

use super::mempool::{ActionKind, MempoolTx, RequestedOrder, RequestedTif};
use crate::{
    accounts::{
        book::{ActionAsset, ActionCoins},
        replica::{CANCEL_CATEGORY, OrderRef, placing_category},
    },
    order_book::{Coin, Oid, Px, Side, Sz},
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
    const fn of_requested(tif: RequestedTif) -> Option<Self> {
        match tif {
            RequestedTif::Alo => Some(Self::Alo),
            RequestedTif::Gtc => Some(Self::Gtc),
            RequestedTif::Ioc => Some(Self::Ioc),
            RequestedTif::FrontendMarket => Some(Self::FrontendMarket),
            RequestedTif::Trigger => None,
        }
    }
}

/// What an action does to its coin's book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BookOp {
    Place(Placement),
    Cancel(Oid),
    CancelByCloid(String),
    /// A `modify`'s or one of a `batchModify`'s: its target out, its new order in (design 000604 revision 8).
    Modify { target: OrderRef, always_place: bool, order: Placement },
}

impl BookOp {
    /// The order it places.
    pub(crate) const fn placement(&self) -> Option<&Placement> {
        match self {
            Self::Place(order) | Self::Modify { order, .. } => Some(order),
            Self::Cancel(_) | Self::CancelByCloid(_) => None,
        }
    }

    pub(crate) const fn label(&self) -> &'static str {
        match self {
            Self::Place(Placement { tif: Tif::Alo, .. }) => "place_alo",
            Self::Place(Placement { tif: Tif::Gtc, .. }) => "place_gtc",
            Self::Place(Placement { tif: Tif::Ioc | Tif::FrontendMarket, .. }) => "place_ioc",
            Self::Cancel(_) => "cancel",
            Self::CancelByCloid(_) => "cancel_by_cloid",
            Self::Modify { .. } => "modify",
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
    pub(crate) op: BookOp,
    /// Never overlaid: seen in a gap or its warm-up (revisions 7, 11), or not told apart from a confirmed one
    /// (revision 2).
    pub(crate) unknown: bool,
}

/// A line's book actions: those held, in execution order, and those HL refuses by rule before any book - each one's
/// [op label, why] (`expires_after`; `off_grid`, or `off_grid_batch` for one on the grid refused with its action).
#[derive(Debug, Default)]
pub(crate) struct BookActions {
    pub(crate) held: Vec<(Coin, PendingAction)>,
    pub(crate) refused: Vec<[&'static str; 2]>,
}

/// `tx`'s book actions on the coins `coins` names, in execution order; spot left out when `spot` is not kept (the
/// replica's book has none then). An asset the table does not name is the mempool reading's count.
pub(crate) fn book_actions(tx: &MempoolTx, coins: &ActionCoins, spot: bool) -> BookActions {
    let mut ranked: Vec<(u8, usize)> = tx.actions.iter().enumerate().filter_map(|(index, action)| Some((category(&action.kind)?, index))).collect();
    ranked.sort_unstable();
    let named = |asset: u32| coins.coins.get(&asset).filter(|named| spot || !Coin::str_is_spot(&named.coin));
    let off_grid = |order: &RequestedOrder| coins.coins.get(&order.asset).is_some_and(|named| !on_grid(order, named));
    let seen_ms = tx.seen_ns / 1_000_000;
    let mut actions = BookActions::default();
    for (_, index) in ranked {
        let signed = &tx.actions[index];
        let expired = signed.expires_after_ms.is_some_and(|expires_ms| i64::try_from(expires_ms).is_ok_and(|expires_ms| expires_ms < seen_ms));
        // an order off the grid fails the whole signed action, trigger orders' prices too
        let batch_off_grid = signed.kind.orders().any(off_grid);
        for (item, asset, op, order) in book_ops(&signed.kind) {
            let Some(named) = named(asset) else { continue };
            if expired || batch_off_grid {
                let why = if expired { "expires_after" } else if order.is_some_and(off_grid) { "off_grid" } else { "off_grid_batch" };
                actions.refused.push([op.label(), why]);
                continue;
            }
            let id = ActionId { tx_hash: tx.tx_hash, action: index, item };
            actions.held.push((Coin::new(&named.coin), PendingAction { id, seen_ns: tx.seen_ns, vault: signed.vault, op, unknown: false }));
        }
    }
    actions
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
        ActionKind::Other => None,
    }
}

/// Each (place in the action, asset, op, the order it places as requested) of an action, in its order; a trigger
/// order, or a modify placing one, left out (no target of a modify placing a trigger order is taken out either: design
/// 000604 revision 8).
fn book_ops(kind: &ActionKind) -> Vec<(usize, u32, BookOp, Option<&RequestedOrder>)> {
    let ops: Vec<Option<(u32, BookOp, Option<&RequestedOrder>)>> = match kind {
        ActionKind::Order(orders) => orders.iter().map(|order| Some((order.asset, BookOp::Place(placement(order)?), Some(order)))).collect(),
        ActionKind::Cancel { cancels, .. } => cancels.iter().map(|cancel| Some((cancel.a, BookOp::Cancel(Oid::new(cancel.o)), None))).collect(),
        ActionKind::CancelByCloid { cancels, .. } => {
            cancels.iter().map(|cancel| Some((cancel.asset, BookOp::CancelByCloid(cancel.cloid.clone()), None))).collect()
        }
        ActionKind::Modify { always_place, modifies } => modifies
            .iter()
            .map(|modify| {
                let order = placement(&modify.order)?;
                Some((modify.order.asset, BookOp::Modify { target: modify.target.clone(), always_place: *always_place, order }, Some(&modify.order)))
            })
            .collect(),
        ActionKind::Other => Vec::new(),
    };
    ops.into_iter().enumerate().filter_map(|(item, op)| op.map(|(asset, op, order)| (item, asset, op, order))).collect()
}

fn placement(order: &RequestedOrder) -> Option<Placement> {
    Some(Placement {
        side: if order.is_buy { Side::Bid } else { Side::Ask },
        limit_px: order.limit_px,
        sz: order.sz,
        tif: Tif::of_requested(order.tif)?,
        cloid: order.cloid.clone(),
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

    /// BTC, ETH and a spot pair, at their mainnet `szDecimals`.
    pub(crate) fn coins() -> ActionCoins {
        let coins: FxHashMap<u32, ActionAsset> = [(0, "BTC", 5, 6), (1, "ETH", 4, 6), (10_107, "@107", 2, 8)]
            .into_iter()
            .map(|(asset, coin, size_decimals, max_decimals)| (asset, ActionAsset { coin: coin.to_string(), size_decimals, max_decimals }))
            .collect();
        ActionCoins { anchor_height: 1, coins }
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
        }
    }

    /// Batch 2d: what HL refuses by rule before any book, the set refuses - every op of a signed action past its
    /// `expiresAfter` when its line came (1,000 ms), not one expiring then; an order off its coin's grid, BTC's
    /// (szDecimals 5): a price of 6 significant figures, or of 2 decimals, a size of 6 decimals - an integer price of
    /// any length is on it, trailing zeros count for nothing.
    #[test]
    fn orders_off_the_grid_and_actions_expired_are_refused() {
        let order = |px: &str, size: &str| RequestedOrder {
            sz: Sz::parse_from_str(size).unwrap(),
            sz_shape: DecimalShape::of(size),
            ..requested(0, true, px, RequestedTif::Alo, None)
        };
        let mut line = tx(vec![ActionKind::Order(vec![order("123456", "0.001"), order("77655", "0.00100")])]);
        assert_eq!(book_actions(&line, &coins(), false).held.len(), 2);
        line.actions[0].expires_after_ms = Some(999);
        let actions = book_actions(&line, &coins(), false);
        assert_eq!((actions.held.len(), actions.refused), (0, vec![["place_alo", "expires_after"]; 2]));
        line.actions[0].expires_after_ms = Some(1_000);
        assert_eq!(book_actions(&line, &coins(), false).held.len(), 2);
        for off in [order("77655.5", "0.001"), order("7765.55", "0.001"), order("77655", "0.000001")] {
            let line = tx(vec![ActionKind::Order(vec![off.clone()])]);
            assert_eq!(book_actions(&line, &coins(), false).refused, [["place_alo", "off_grid"]], "{off:?}");
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
        let actions = book_actions(&tx, &coins(), false);
        let mut refused = vec![["place_alo", "off_grid"]; 10];
        refused[4] = ["place_alo", "off_grid_batch"];
        assert_eq!(actions.refused, refused);
        assert_eq!(actions.held.iter().map(|(coin, action)| (coin.value(), action.id.action)).collect::<Vec<_>>(), [("BTC".to_string(), 1)]);
    }

    const MIXED_TICK_ACTION: &str = r#"{"signature":{"r":"0xb8e8a2ed193ba5cda5b6599d5e5d67c476c9e115b9f86ce17fe347b1f2f8e145","s":"0x89a7eab30cebce29300ad9ac703e761085230a00b6253aa91ae9b6c2b9dad44","v":27},"vaultAddress":"0xf36c562be0598b8ce109998b6aa2d53ffcd8316b","action":{"type":"order","orders":[{"a":0,"b":false,"p":"77896.3","s":"0.62909","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x16173310847065010000000012405259"},{"a":0,"b":false,"p":"77807.6","s":"0.47182","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x16173310847065010000000012405258"},{"a":0,"b":false,"p":"77741.1","s":"0.31454","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x16173310847065010000000012405257"},{"a":0,"b":false,"p":"77696.7","s":"0.12582","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x16173310847065010000000012405256"},{"a":0,"b":false,"p":"77679","s":"0.03145","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x16173310847065010000000012405255"},{"a":0,"b":true,"p":"77395.7","s":"0.64086","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x16173310847065000000000012405254"},{"a":0,"b":true,"p":"77501.4","s":"0.48064","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x16173310847065000000000012405253"},{"a":0,"b":true,"p":"77580.7","s":"0.32043","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x16173310847065000000000012405252"},{"a":0,"b":true,"p":"77633.6","s":"0.12817","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x16173310847065000000000012405251"},{"a":0,"b":true,"p":"77653.5","s":"0.03204","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x16173310847065000000000012405250"}],"grouping":"na"},"nonce":1793469848762,"expiresAfter":1793469858741}"#;
    const ON_GRID_ACTION: &str = r#"{"signature":{"r":"0x1","s":"0x2","v":27},"action":{"type":"order","orders":[{"a":0,"b":true,"p":"77387","s":"0.135","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x01"}],"grouping":"na"},"nonce":1793469848000}"#;

    pub(crate) fn tx(actions: Vec<ActionKind>) -> MempoolTx {
        MempoolTx {
            seen_ns: 1_000_000_000,
            tx_hash: B256::repeat_byte(7),
            actions: actions.into_iter().map(|kind| SignedAction { vault: None, expires_after_ms: None, kind }).collect(),
        }
    }

    fn ops(tx: &MempoolTx, spot: bool) -> Vec<(String, usize, usize, &'static str)> {
        book_actions(tx, &coins(), spot).held.into_iter().map(|(coin, action)| (coin.value(), action.id.action, action.id.item, action.op.label())).collect()
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
