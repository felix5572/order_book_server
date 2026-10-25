//! Routing inputs for the narrow Unified funding projection. Reuse parsed receipts/statuses; no second replica
//! parser or guessed action order. Only a single ordinary ALO action, with all its open receipts and no competing
//! cash/fill event for the account, establishes post-order retained cash. Other proven cancellations update the
//! tracker but do not invent the pending-set sweep time. A single ordinary IOC boundary can instead preserve
//! an unchanged other-dex reservation bound, with all its receipt/status/fill identities accounted for.
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
        let mut executions: FxHashMap<Address, Vec<&crate::types::Fill>> = FxHashMap::default();
        for NodeDataFill(user, fill) in fills.iter().flat_map(Batch::events_ref) {
            if book.routing_coin(user).is_some() { executions.entry(*user).or_default().push(fill); }
        }
        for status in inputs.order_statuses.iter().flat_map(Batch::events_ref) {
            if book.routing_coin(&status.user).is_some() { statuses.entry(status.user).or_default().push(status); }
        }
        for action in inputs.mark_actions.as_ref().into_iter().flat_map(|actions| &actions.actions) {
            if let Some(user) = action.user.filter(|user| book.routing_coin(user).is_some()) {
                own.entry(user).or_default().push(action);
            }
        }
        let mut users: FxHashSet<_> = statuses.keys().chain(own.keys()).chain(executions.keys()).copied().collect();
        let mut cash_users: FxHashSet<_> = cash_actions.actions.iter().map(|(_, user)| *user).collect();
        // System requests are not receipts and can execute on a different phase. Without a complete hold
        // lifecycle, discard any affected account's routing bound, including when it has no status/fill.
        for action in inputs.system_actions.iter().flat_map(Batch::events_ref) {
            if book.routing_coin(&action.user()).is_some() { users.insert(action.user()); cash_users.insert(action.user()); }
        }
        let mut unknown_cash = false;
        for event in misc.iter().flat_map(Batch::events_ref) {
            match event {
                MiscEvent::Ledger { users, .. } => cash_users.extend(users),
                MiscEvent::Unknown(_) => unknown_cash = true,
                MiscEvent::Funding(deltas) => cash_users.extend(deltas.iter().filter(|delta|
                    book.routing_coin(&delta.user).is_some_and(|coin| coin != delta.coin)).map(|delta| delta.user)),
                MiscEvent::Irrelevant => {},
            }
        }
        result.participants = users;
        if unknown_cash || inputs.mark_actions.is_err() {
            result.participants.extend(book.routing_users());
        }
        for user in result.participants.iter().copied() {
            if unknown_cash || cash_users.contains(&user) || inputs.mark_actions.is_err() {
                result.before.push((user, RoutingUpdate::Unavailable));
                continue;
            }
            let rows = statuses.remove(&user).unwrap_or_default();
            let acts = own.remove(&user).unwrap_or_default();
            let coin = book.routing_coin(&user).expect("selected routing account remains present during read-only projection");
            if rows.iter().any(|row| row.order.coin != coin) || executions.get(&user).is_some_and(|fills| fills.iter().any(|fill| fill.coin != coin)) {
                result.before.push((user, RoutingUpdate::Unavailable));
                continue;
            }
            // A maker fill consumes its resting quantity, so its terminal filled status is not another cancel.
            let changes: Vec<_> = rows.iter().copied().filter(|row| row.status != "filled").collect();
            if changes.is_empty() && acts.is_empty() { continue; }
            if executions.contains_key(&user) {
                let update = if ordinary_ioc(coin, &rows, &acts, &executions[&user]) { RoutingUpdate::OrdinaryIoc }
                    else if let Some(tid) = triggered_taker(coin, &rows, &acts, &executions[&user]) { RoutingUpdate::TriggeredTaker { tid } }
                    else { RoutingUpdate::Unavailable };
                result.before.push((user, update));
            } else {
                result.after.push((user, Self::orders(coin, &changes, &acts, inputs, marks)));
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

    pub(crate) fn require_trusted(&mut self, trusted: bool, book: &AccountBook) {
        if trusted { return; }
        self.after.clear();
        self.participants.extend(book.routing_users());
        self.before = self.participants.iter().map(|user| (*user, RoutingUpdate::Unavailable)).collect();
    }
}

/// One ordinary IOC, one open and one terminal status, all executions from that receipt. This only preserves
/// other-dex reservation bounds; it neither establishes a maker's retained balance nor guesses system timing.
fn ordinary_ioc(coin: &str, rows: &[&NodeDataOrderStatus], actions: &[&RankedAction], fills: &[&crate::types::Fill]) -> bool {
    let [action] = actions else { return false; };
    let MarkAction::Book(anchors) = &action.action else { return false; };
    let [oid] = anchors.created.as_slice() else { return false; };
    if !anchors.single_ioc_order || !anchors.ordinary_order || action.rank.1 != 3 || anchors.takers.len() != 1
        || !anchors.cancelled.is_empty() || !anchors.cancelled_cloids.is_empty() || !anchors.rejected_iocs.is_empty()
        || anchors.ioc_pairing_mismatch.is_some() || anchors.unreceipted_modify.is_some() { return false; }
    let [open, terminal] = rows else { return false; };
    if open.status != "open" || terminal.status != "filled" || open.order.side != terminal.order.side { return false; }
    if rows.iter().any(|row| row.order.oid != *oid || row.order.coin != coin || row.order.is_trigger || row.order.reduce_only
        || row.order.order_type != "Limit" || row.order.tif.as_deref() != Some("Ioc")) { return false; }
    !fills.is_empty() && fills.iter().all(|fill| fill.oid == *oid && fill.coin == coin && fill.side == open.order.side && fill.crossed && fill.liquidation.is_none())
}

/// The captured ETH boundary has one automatic trailing stop and one full taker fill. Zero-size position
/// TP/SL cancellations cannot reserve spot USDC. Reject all other actions/statuses rather than replaying a
/// partial hold lifecycle or guessing system-action ordering. This is a numerical bound, still estimated.
fn triggered_taker(coin: &str, rows: &[&NodeDataOrderStatus], actions: &[&RankedAction], fills: &[&crate::types::Fill]) -> Option<u64> {
    if !actions.is_empty() { return None; }
    let [fill] = fills else { return None; };
    if fill.coin != coin || !fill.crossed || fill.liquidation.is_some() || fill.twap_id.is_some() || fill.fee_token != "USDC" { return None; }
    let [trigger, terminal, cancellations @ ..] = rows else { return None; };
    if trigger.status != "triggered" || terminal.status != "filled" || !trigger.order.is_trigger || terminal.order.is_trigger
        || trigger.order.tif.is_some() || terminal.order.tif.as_deref() != Some("Gtc") { return None; }
    if [trigger, terminal].iter().any(|row| row.order.oid != fill.oid || row.order.coin != coin || row.order.side != fill.side
        || row.order.order_type != "Trailing Stop Market" || row.order.reduce_only || row.order.is_position_tpsl
        || !row.order.children.is_empty()) { return None; }
    // Compare canonical raw quantities using the existing wire-size parser, not textual formatting.
    let quantity = |value: &str| super::size::parse_size_raw(value, 8).ok();
    let filled = quantity(&fill.sz)?;
    if filled <= 0 || quantity(&trigger.order.sz) != Some(filled) || quantity(&trigger.order.orig_sz) != Some(filled)
        || quantity(&terminal.order.orig_sz) != Some(filled) || quantity(&terminal.order.sz) != Some(0) { return None; }
    let mut cancelled = FxHashSet::default();
    for row in cancellations {
        let order = &row.order;
        if row.status != "reduceOnlyCanceled" || order.coin != coin || order.oid == fill.oid || !cancelled.insert(order.oid)
            || !order.is_trigger || !order.reduce_only || !order.is_position_tpsl || order.order_type != "Stop Market"
            || !order.children.is_empty() || quantity(&order.sz) != Some(0) || quantity(&order.orig_sz) != Some(0) { return None; }
    }
    Some(fill.tid)
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
        fills_only.require_trusted(false, &book);
        assert_eq!(fills_only.before, [(who, RoutingUpdate::Unavailable)]);
        let other_fill = [fills(height, &[(who, "BTC", "A", 2, "1", "0.1")])];
        let foreign = RoutingBlock::of(&book, &MarkInputs::default(), &other_fill, &marks(), &unrelated_cash, &[]);
        assert_eq!(foreign.before, [(who, RoutingUpdate::Unavailable)], "foreign fills invalidate before any own-market execution");
        let funding = crate::accounts::test_support::funding_inner(&[(who, "BTC", "1")]);
        let settled = crate::accounts::test_support::misc(height, &[&funding]);
        let funding_block = RoutingBlock::of(&book, &MarkInputs::default(), &trades, &marks(), &unrelated_cash, &[settled]);
        assert_eq!(funding_block.before, [(who, RoutingUpdate::Unavailable)]);
        let unknown = crate::accounts::test_support::misc(height, &[r#"{"NewKind":{}}"#]);
        let idle = RoutingBlock::of(&book, &MarkInputs::default(), &[], &marks(), &unrelated_cash, &[unknown]);
        assert_eq!(idle.before, [(who, RoutingUpdate::Unavailable)], "unknown cash can affect an idle account");
        let mut idle = RoutingBlock::of(&book, &MarkInputs::default(), &[], &marks(), &unrelated_cash, &[]);
        idle.require_trusted(false, &book);
        assert_eq!(idle.before, [(who, RoutingUpdate::Unavailable)], "a gap cannot leave idle checkpoint trackers trusted");
    }

    #[test]
    fn gold_ioc_boundary_uses_original_request_receipt_statuses_and_all_six_fills() {
        use crate::accounts::marks::actions::tests::{replica_line, mark_actions};
        let golden: serde_json::Value = serde_json::from_str(include_str!("replay/gold_other_dex_golden.json")).unwrap();
        let pair = &golden["actions"][0]["selected_actions"][0];
        let request = pair["action"].to_string();
        let response = pair["response"].to_string();
        let parsed = mark_actions(&replica_line(1_194_477_717, &[&[(&request, &response)]]));
        let rows: Vec<NodeDataOrderStatus> = serde_json::from_value(golden["statuses"].clone()).unwrap();
        let fills: Vec<crate::types::Fill> = golden["fills"].as_array().unwrap().iter()
            .map(|row| serde_json::from_value(row["fill"].clone()).unwrap()).collect();
        let valid = |rows: &[NodeDataOrderStatus], actions: &[RankedAction], fills: &[crate::types::Fill]| ordinary_ioc("xyz:GOLD",
            &rows.iter().collect::<Vec<_>>(), &actions.iter().collect::<Vec<_>>(), &fills.iter().collect::<Vec<_>>());
        assert!(valid(&rows, &parsed.actions, &fills));
        for variant in 0..9 {
            let mut rows = rows.clone(); let mut actions = parsed.actions.clone(); let mut fills = fills.clone();
            match variant {
                0 => rows[0].order.coin = "BTC".into(),
                1 => { rows.remove(0); },
                2 => rows[0].order.tif = Some("Gtc".into()),
                3 => rows[0].order.is_trigger = true,
                4 => fills[1].coin = "BTC".into(),
                5 => fills[0].oid += 1,
                6 => fills[0].crossed = false,
                7 => actions.push(actions[0].clone()),
                8 => { let MarkAction::Book(a) = &mut actions[0].action else { panic!("book") }; a.ordinary_order = false; },
                _ => unreachable!(),
            }
            assert!(!valid(&rows, &actions, &fills), "variant {variant}");
        }
        // One accepted IOC plus a rejected ALO on another dex can still have one created oid and one taker.
        // Request cardinality, not the apparent receipts, must prevent this false funding boundary.
        let mut request = pair["action"].clone();
        let mut rejected = request["orders"][0].clone();
        rejected["a"] = 0.into(); rejected["t"]["limit"]["tif"] = "Alo".into();
        request["orders"].as_array_mut().unwrap().push(rejected);
        let mut response = pair["response"].clone();
        response["res"]["response"]["data"]["statuses"].as_array_mut().unwrap().push(serde_json::json!({"error":"Insufficient margin"}));
        let parsed = mark_actions(&replica_line(1_194_477_717, &[&[(&request.to_string(), &response.to_string())]]));
        assert!(!valid(&rows, &parsed.actions, &fills));
    }

    #[test]
    fn recorded_trailing_stop_needs_complete_identity_and_no_competing_orders() {
        let golden: serde_json::Value = serde_json::from_str(include_str!("replay/eth_frozen_spot_golden.json")).unwrap();
        let rows: Vec<NodeDataOrderStatus> = serde_json::from_value(golden["statuses"].clone()).unwrap();
        let fill: crate::types::Fill = serde_json::from_value(golden["recorded_fill"].clone()).unwrap();
        let valid = |rows: &[NodeDataOrderStatus], actions: &[RankedAction], fills: &[crate::types::Fill]| triggered_taker("ETH",
            &rows.iter().collect::<Vec<_>>(), &actions.iter().collect::<Vec<_>>(), &fills.iter().collect::<Vec<_>>());
        assert_eq!(valid(&rows, &[], std::slice::from_ref(&fill)), Some(fill.tid));
        for variant in 0..14 {
            let mut rows = rows.clone(); let mut fills = vec![fill.clone()]; let mut actions = vec![];
            match variant {
                0 => { rows.remove(0); },
                1 => rows.swap(0, 1),
                2 => rows[1].order.oid += 1,
                3 => rows[0].order.sz = "0.8".into(),
                4 => rows[1].order.sz = "0.0001".into(),
                5 => rows[2].order.coin = "@0".into(),
                6 => rows[2].order.sz = "1".into(),
                7 => fills[0].crossed = false,
                8 => fills[0].oid += 1,
                9 => fills.push(fill.clone()),
                10 => rows.push(rows[2].clone()),
                11 => actions.push(action()),
                12 => rows[2].order.is_position_tpsl = false,
                13 => fills[0].fee_token = "HYPE".into(),
                _ => unreachable!(),
            }
            assert_eq!(valid(&rows, &actions, &fills), None, "variant {variant}");
        }
    }

    #[test]
    fn frozen_spot_adapter_invalidates_cash_spot_system_and_stream_boundaries() {
        let (mut cp, _, who) = checkpoint();
        cp.accounts.get_mut(&who).unwrap().dex_states[0].cash_micro = 0;
        cp.spot_usdc_holds.insert(who, 14_473_500_000);
        let mut book = AccountBook::new(0);
        book.install_anchor(Anchor::from_checkpoint(&cp).unwrap());
        assert_eq!(book.routing_coin(&who), Some("xyz:TSLA"));
        let golden: serde_json::Value = serde_json::from_str(include_str!("replay/eth_frozen_spot_golden.json")).unwrap();
        let mut rows: Vec<NodeDataOrderStatus> = serde_json::from_value(golden["statuses"].clone()).unwrap();
        for row in &mut rows { row.user = who; row.order.coin = "xyz:TSLA".into(); }
        let mut fill: crate::types::Fill = serde_json::from_value(golden["recorded_fill"].clone()).unwrap();
        fill.coin = "xyz:TSLA".into();
        let height = cp.height + 1;
        let inputs = || MarkInputs { order_statuses: rows.iter().map(|row|
            sonic_rs::from_str(&batch_line(height, &serde_json::to_string(row).unwrap())).unwrap()).collect(), ..MarkInputs::default() };
        let trades = [sonic_rs::from_str(&batch_line(height, &serde_json::to_string(&(who, &fill)).unwrap())).unwrap()];
        let cash = block_actions(height, &[]);
        let plain = RoutingBlock::of(&book, &inputs(), &trades, &marks(), &cash, &[]);
        assert_eq!(plain.before, [(who, RoutingUpdate::TriggeredTaker { tid: fill.tid })]);
        let mut lost = plain; lost.require_trusted(false, &book);
        assert_eq!(lost.before, [(who, RoutingUpdate::Unavailable)]);
        let own_cash = block_actions(height, &[(crate::accounts::actions::BlockAction::Withdraw { amount_wei: 100 }, who)]);
        assert_eq!(RoutingBlock::of(&book, &inputs(), &trades, &marks(), &own_cash, &[]).before, [(who, RoutingUpdate::Unavailable)]);
        let mut spot = rows[2].clone(); spot.order.coin = "@0".into(); spot.status = "filled".into();
        let mut mixed = inputs(); mixed.order_statuses.push(sonic_rs::from_str(&batch_line(height, &serde_json::to_string(&spot).unwrap())).unwrap());
        assert_eq!(RoutingBlock::of(&book, &mixed, &trades, &marks(), &cash, &[]).before, [(who, RoutingUpdate::Unavailable)]);
        // Even a terminal spot status without a paired fill must not leave a frozen hold usable later.
        mixed.order_statuses.remove(0); mixed.order_statuses.remove(1);
        assert_eq!(RoutingBlock::of(&book, &mixed, &[], &marks(), &cash, &[]).before, [(who, RoutingUpdate::Unavailable)]);
        let system = serde_json::json!({"user":who,"evm_tx_hash":null,"nonce":null,"action":{"type":"cancel","cancels":[]}});
        let mut system_inputs = MarkInputs::default();
        system_inputs.system_actions.push(sonic_rs::from_str(&batch_line(height, &system.to_string())).unwrap());
        assert_eq!(RoutingBlock::of(&book, &system_inputs, &[], &marks(), &cash, &[]).before, [(who, RoutingUpdate::Unavailable)]);
    }

}
