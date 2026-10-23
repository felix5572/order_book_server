//! Routing inputs for the narrow Unified funding projection. Reuse parsed receipts/statuses; no second replica
//! parser or guessed action order. Only a single ordinary ALO action, with all its open receipts and no competing
//! cash/fill event for the account, establishes post-order retained cash. Other proven cancellations update the
//! tracker but do not invent the pending-set sweep time.
use alloy::primitives::Address;
use rustc_hash::{FxHashMap, FxHashSet};
use super::{book::AccountBook, marks::{MarkInputs, NodeMarks, actions::{MarkAction, RankedAction}},
    replay::routing::{OrderChange, RoutingUpdate}, actions::BlockActions, misc::MiscEvent};
use crate::types::node_data::{Batch, NodeDataFill, NodeDataOrderStatus};

#[derive(Default)]
pub(crate) struct RoutingBlock {
    pub(crate) before: Vec<(Address, RoutingUpdate)>,
    pub(crate) after: Vec<(Address, RoutingUpdate)>,
    participants: FxHashSet<Address>,
}

impl RoutingBlock {
    #[expect(clippy::expect_used, reason = "participants were selected from this immutable anchor, which cannot change during projection")]
    pub(crate) fn of(book: &AccountBook, inputs: &MarkInputs, fills: &[Batch<NodeDataFill>], marks: &NodeMarks,
        cash_actions: &BlockActions, misc: &[Batch<MiscEvent>]) -> Self {
        let mut result = Self::default();
        let mut statuses: FxHashMap<Address, Vec<&NodeDataOrderStatus>> = FxHashMap::default();
        let mut own: FxHashMap<Address, Vec<&RankedAction>> = FxHashMap::default();
        let mut traders = FxHashSet::default();
        for NodeDataFill(user, _) in fills.iter().flat_map(Batch::events_ref) {
            if book.routing_coin(user).is_some() { traders.insert(*user); }
        }
        for status in inputs.order_statuses.iter().flat_map(Batch::events_ref) {
            if book.routing_coin(&status.user).is_some() { statuses.entry(status.user).or_default().push(status); }
        }
        for action in inputs.mark_actions.as_ref().into_iter().flat_map(|actions| &actions.actions) {
            if let Some(user) = action.user.filter(|user| book.routing_coin(user).is_some()) {
                own.entry(user).or_default().push(action);
            }
        }
        let users: FxHashSet<_> = statuses.keys().chain(own.keys()).chain(&traders).copied().collect();
        let mut cash_users: FxHashSet<_> = cash_actions.actions.iter().map(|(_, user)| *user).collect();
        let mut unknown_cash = false;
        for event in misc.iter().flat_map(Batch::events_ref) {
            match event {
                MiscEvent::Ledger { users, .. } => cash_users.extend(users),
                MiscEvent::Unknown(_) => unknown_cash = true,
                MiscEvent::Funding(_) | MiscEvent::Irrelevant => {},
            }
        }
        result.participants = users;
        for user in result.participants.iter().copied() {
            if unknown_cash || cash_users.contains(&user) || inputs.mark_actions.is_err() {
                result.before.push((user, RoutingUpdate::Unavailable));
                continue;
            }
            let rows = statuses.remove(&user).unwrap_or_default();
            let acts = own.remove(&user).unwrap_or_default();
            // A maker fill consumes its resting quantity, so its terminal filled status is not another cancel.
            let changes: Vec<_> = rows.iter().copied().filter(|row| row.status != "filled").collect();
            if changes.is_empty() && acts.is_empty() { continue; }
            let coin = book.routing_coin(&user).expect("selected routing account remains present during read-only projection");
            let update = Self::orders(coin, &changes, &acts, inputs, marks);
            if traders.contains(&user) {
                result.before.push((user, RoutingUpdate::Unavailable));
            } else {
                result.after.push((user, update));
            }
        }
        result
    }

    fn orders(coin: &str, rows: &[&NodeDataOrderStatus], actions: &[&RankedAction], inputs: &MarkInputs, marks: &NodeMarks) -> RoutingUpdate {
        let [action] = actions else { return RoutingUpdate::Unavailable; };
        let MarkAction::Book(anchors) = &action.action else { return RoutingUpdate::Unavailable; };
        if anchors.unreceipted_modify.is_some() || anchors.ioc_pairing_mismatch.is_some() {
            return RoutingUpdate::Unavailable;
        }
        let expected_open: FxHashSet<_> = anchors.created.iter().copied().collect();
        let mut expected_cancel: FxHashSet<_> = anchors.cancelled.iter().copied().collect();
        for cloid in &anchors.cancelled_cloids {
            let mut matching = rows.iter().filter(|row| row.status == "canceled" && row.order.cloid.as_ref() == Some(cloid));
            let Some(row) = matching.next() else { return RoutingUpdate::Unavailable; };
            if matching.next().is_some() || !expected_cancel.insert(row.order.oid) { return RoutingUpdate::Unavailable; }
        }
        let mut seen_open = FxHashSet::default();
        let mut seen_cancel = FxHashSet::default();
        let mut changes = Vec::new();
        for row in rows {
            let order = &row.order;
            if order.coin != coin || order.is_trigger || order.reduce_only || order.order_type != "Limit" { return RoutingUpdate::Unavailable; }
            let remove = match row.status.as_str() {
                "open" if expected_open.contains(&order.oid) && order.tif.as_deref() == Some("Alo") && seen_open.insert(order.oid) => false,
                "canceled" if expected_cancel.contains(&order.oid) && seen_cancel.insert(order.oid) => true,
                _ => return RoutingUpdate::Unavailable,
            };
            changes.push(OrderChange { coin: coin.into(), side: order.side, px: order.limit_px.clone(), size: order.sz.clone(), remove });
        }
        if seen_open != expected_open || seen_cancel != expected_cancel || changes.is_empty() { return RoutingUpdate::Unavailable; }
        let prefund = anchors.ordinary_order && action.rank.1 == 1 && expected_cancel.is_empty();
        if !expected_open.is_empty() && !prefund { return RoutingUpdate::Unavailable; }
        // Equal endpoints do not prove absence of an intermediate price round trip. A price-writing block does
        // not supply an action mark to this adapter; retain quantities but discard the cached-price proof.
        let price_write = inputs.mark_actions.as_ref().is_ok_and(|actions| actions.actions.iter().any(|a| a.action.is_price())) ||
            inputs.hip3_oracle_updates.iter().flat_map(Batch::events_ref).any(|event|
                event.oracle_pxs.coin_to_mark_px.iter().any(|(name, _)| name == coin));
        let mark_raw = (!price_write).then(|| marks.at()?.mark(coin)).flatten();
        RoutingUpdate::Orders { changes, mark_raw, prefund }
    }

    pub(crate) fn require_trusted(&mut self, trusted: bool) {
        if trusted { return; }
        self.after.clear();
        self.before = self.participants.iter().map(|user| (*user, RoutingUpdate::Unavailable)).collect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::{marks::{CheckpointGrid, actions::{BookAnchors, MarkActions}}, test_support::{user, status_line, block_actions, fills, batch_line},
        anchor::Anchor, replay::routing::tests::checkpoint};

    fn marks() -> NodeMarks { NodeMarks::new(CheckpointGrid { anchor: 0, interval: 10_000 }, 0) }
    fn row() -> NodeDataOrderStatus {
        let mut row = status_line(8, "open", user(1), 7, Some("0xa")).events().remove(0);
        row.order.sz = "0.014".into();
        row
    }
    fn action() -> RankedAction { RankedAction { rank: (0, 1, 1), user: Some(user(1)),
        action: MarkAction::Book(BookAnchors { ordinary_order: true, created: vec![7], ..BookAnchors::default() }) } }

    #[test]
    fn only_receipt_backed_ordinary_alo_establishes_retained_cash() {
        let row = row();
        let action = action();
        assert!(matches!(RoutingBlock::orders("BTC", &[&row], &[&action], &MarkInputs::default(), &marks()),
            RoutingUpdate::Orders { prefund: true, mark_raw: None, .. }));
        for variant in 0..7 {
            let mut row = row.clone();
            let mut action = action.clone();
            match variant {
                0 => row.order.is_trigger = true,
                1 => row.order.reduce_only = true,
                2 => row.order.tif = Some("Gtc".into()),
                3 => row.order.oid = 9,
                4 => row.order.coin = "ETH".into(),
                5 => { let MarkAction::Book(anchors) = &mut action.action else { unreachable!() }; anchors.ordinary_order = false; },
                6 => row.status = "rejected".into(),
                _ => unreachable!(),
            }
            assert_eq!(RoutingBlock::orders("BTC", &[&row], &[&action], &MarkInputs::default(), &marks()), RoutingUpdate::Unavailable);
        }
        assert_eq!(RoutingBlock::orders("BTC", &[&row, &row], &[&action], &MarkInputs::default(), &marks()), RoutingUpdate::Unavailable);
        assert_eq!(RoutingBlock::orders("BTC", &[&row], &[&action, &action], &MarkInputs::default(), &marks()), RoutingUpdate::Unavailable);
    }

    #[test]
    fn successful_cloid_cancel_requires_one_matching_status_and_keeps_sweep_unknown() {
        let mut row = row(); row.status = "canceled".into();
        let action = RankedAction { rank: (0, 2, 1), user: Some(user(1)), action: MarkAction::Book(BookAnchors {
            cancelled_cloids: vec!["0xa".into()], ..BookAnchors::default() }) };
        assert!(matches!(RoutingBlock::orders("BTC", &[&row], &[&action], &MarkInputs::default(), &marks()), RoutingUpdate::Orders { prefund: false, .. }));
        let mut other = row.clone(); other.order.oid = 8;
        assert_eq!(RoutingBlock::orders("BTC", &[&row, &other], &[&action], &MarkInputs::default(), &marks()), RoutingUpdate::Unavailable);
        row.order.cloid = Some("0xb".into());
        assert_eq!(RoutingBlock::orders("BTC", &[&row], &[&action], &MarkInputs::default(), &marks()), RoutingUpdate::Unavailable);
    }

    #[test]
    fn mixed_cash_or_fill_invalidates_only_the_affected_account() {
        let (cp, _, who) = checkpoint();
        let mut book = AccountBook::new(0);
        book.install_anchor(Anchor::from_checkpoint(&cp).unwrap());
        let mut row = row(); row.user = who; row.order.coin = "xyz:TSLA".into();
        let mut action = action(); action.user = Some(who);
        let height = cp.height + 1;
        let inputs = MarkInputs { order_statuses: vec![sonic_rs::from_str(&batch_line(height, &serde_json::to_string(&row).unwrap())).unwrap()],
            mark_actions: Ok(MarkActions { height, time_ns: 0, actions: vec![action] }), ..MarkInputs::default() };
        let unrelated_cash = block_actions(height, &[(crate::accounts::actions::BlockAction::Withdraw { amount_wei: 100 }, user(9))]);
        let plain = RoutingBlock::of(&book, &inputs, &[], &marks(), &unrelated_cash, &[]);
        assert_eq!(plain.before.len(), 0); assert_eq!(plain.after.len(), 1);
        let own_cash = block_actions(height, &[(crate::accounts::actions::BlockAction::Withdraw { amount_wei: 100 }, who)]);
        let competing = RoutingBlock::of(&book, &inputs, &[], &marks(), &own_cash, &[]);
        assert_eq!(competing.before, [(who, RoutingUpdate::Unavailable)]);
        let trades = [fills(height, &[(who, "xyz:TSLA", "A", 1, "1", "0.1")])];
        let mixed = RoutingBlock::of(&book, &inputs, &trades, &marks(), &unrelated_cash, &[]);
        assert_eq!(mixed.before, [(who, RoutingUpdate::Unavailable)]);
        // No own order action: a mark-data gap must still invalidate physical capacity before the maker fill.
        let mut fills_only = RoutingBlock::of(&book, &MarkInputs::default(), &trades, &marks(), &unrelated_cash, &[]);
        assert_eq!(fills_only.before.len(), 0);
        fills_only.require_trusted(false);
        assert_eq!(fills_only.before, [(who, RoutingUpdate::Unavailable)]);
    }
}
