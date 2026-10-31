//! A mempool transaction's book actions, each on its named coin, in execution order (hyperliquid-trade design 000604
//! revision 4): a line is one bundle, run whole in the order bm read the lines (research `mempool_pending_book_20261031`,
//! 13,989 bundles with book actions, none run after a later one); inside it the signed actions by `placing_category`,
//! each category in listed order, and each action's orders, cancels or modifies in their own order. A cancel's `f`
//! (fast) does not move it there (1.8M fast and 43k plain cancels alike), and trigger orders - a modify placing one too -
//! never touch the book before they trigger: left out.

use alloy::primitives::{Address, B256};

use super::mempool::{ActionKind, MempoolTx, RequestedOrder, RequestedTif};
use crate::{
    accounts::{
        book::ActionCoins,
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

/// `tx`'s book actions on the coins `coins` names, in execution order; spot left out when `spot` is not kept (the
/// replica's book has none then). An asset the table does not name is the mempool reading's count.
pub(crate) fn book_actions(tx: &MempoolTx, coins: &ActionCoins, spot: bool) -> Vec<(Coin, PendingAction)> {
    let mut ranked: Vec<(u8, usize)> = tx.actions.iter().enumerate().filter_map(|(index, action)| Some((category(&action.kind)?, index))).collect();
    ranked.sort_unstable();
    let named = |asset: u32| coins.coins.get(&asset).map(|coin| Coin::new(coin)).filter(|coin| spot || !coin.is_spot());
    let mut actions = Vec::new();
    for (_, index) in ranked {
        let signed = &tx.actions[index];
        for (item, asset, op) in book_ops(&signed.kind) {
            let Some(coin) = named(asset) else { continue };
            let id = ActionId { tx_hash: tx.tx_hash, action: index, item };
            actions.push((coin, PendingAction { id, seen_ns: tx.seen_ns, vault: signed.vault, op, unknown: false }));
        }
    }
    actions
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

/// Each (place in the action, asset, op) of an action, in its order; a trigger order, or a modify placing one, left
/// out (no target of a modify placing a trigger order is taken out either: design 000604 revision 8).
fn book_ops(kind: &ActionKind) -> Vec<(usize, u32, BookOp)> {
    let ops: Vec<Option<(u32, BookOp)>> = match kind {
        ActionKind::Order(orders) => orders.iter().map(|order| Some((order.asset, BookOp::Place(placement(order)?)))).collect(),
        ActionKind::Cancel { cancels, .. } => cancels.iter().map(|cancel| Some((cancel.a, BookOp::Cancel(Oid::new(cancel.o))))).collect(),
        ActionKind::CancelByCloid { cancels, .. } => {
            cancels.iter().map(|cancel| Some((cancel.asset, BookOp::CancelByCloid(cancel.cloid.clone())))).collect()
        }
        ActionKind::Modify { always_place, modifies } => modifies
            .iter()
            .map(|modify| {
                let order = placement(&modify.order)?;
                Some((modify.order.asset, BookOp::Modify { target: modify.target.clone(), always_place: *always_place, order }))
            })
            .collect(),
        ActionKind::Other => Vec::new(),
    };
    ops.into_iter().enumerate().filter_map(|(item, op)| op.map(|(asset, op)| (item, asset, op))).collect()
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
        pending::mempool::{Modify, SignedAction},
    };

    pub(crate) fn coins() -> ActionCoins {
        let coins: FxHashMap<u32, String> =
            [(0, "BTC"), (1, "ETH"), (10_107, "@107")].into_iter().map(|(asset, coin)| (asset, coin.to_string())).collect();
        ActionCoins { anchor_height: 1, coins }
    }

    pub(crate) fn requested(asset: u32, is_buy: bool, px: &str, tif: RequestedTif, cloid: Option<&str>) -> RequestedOrder {
        RequestedOrder {
            asset,
            is_buy,
            limit_px: Px::parse_from_str(px).unwrap(),
            sz: Sz::parse_from_str("1").unwrap(),
            reduce_only: false,
            cloid: cloid.map(str::to_string),
            tif,
        }
    }

    pub(crate) fn tx(actions: Vec<ActionKind>) -> MempoolTx {
        MempoolTx {
            seen_ns: 1_000_000_000,
            tx_hash: B256::repeat_byte(7),
            actions: actions.into_iter().map(|kind| SignedAction { vault: None, kind }).collect(),
        }
    }

    fn ops(tx: &MempoolTx, spot: bool) -> Vec<(String, usize, usize, &'static str)> {
        book_actions(tx, &coins(), spot).into_iter().map(|(coin, action)| (coin.value(), action.id.action, action.id.item, action.op.label())).collect()
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
