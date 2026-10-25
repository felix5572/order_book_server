use crate::accounts::marks::{actions::{MarkAction, MarkActions}, actions::tests::{mark_actions, ok, replica_line},
    book::BookDiff, execution::{BlockFill, BlockStatuses, Cut, price_points_with_system}, system::SystemAnchors};
use crate::types::node_data::{NodeDataOrderDiff, NodeDataOrderStatus};
use serde::Deserialize;

#[derive(Deserialize)]
struct Golden {
    height: u64, coin: String, asset: u32, block_time: String,
    action: serde_json::Value, response: serde_json::Value,
    statuses: Vec<NodeDataOrderStatus>, diffs: Vec<NodeDataOrderDiff>,
}

fn golden() -> Golden { serde_json::from_str(include_str!("golden.json")).unwrap() }

fn actions(g: &Golden, after: bool) -> MarkActions {
    let order = (g.action.to_string(), g.response.to_string());
    let sga = (r#"{"type":"SetGlobalAction","pxs":[]}"#, ok(r#"{"type":"default"}"#));
    let first = [(order.0.as_str(), order.1.as_str())];
    let second = [(sga.0, sga.1.as_str())];
    let bundles = if after { [&second[..], &first[..]] } else { [&first[..], &second[..]] };
    mark_actions(&replica_line(g.height, &bundles).replace("2026-10-20T13:36:16.650019121", &g.block_time))
}

fn cut(g: &Golden, a: &MarkActions, fills: &[BlockFill], system: &SystemAnchors) -> Cut {
    let statuses = BlockStatuses::of(g.statuses.clone()).unwrap();
    let diffs = g.diffs.iter().map(BookDiff::of).collect::<Result<Vec<_>, _>>().unwrap();
    price_points_with_system(a, &statuses, &diffs, fills, system,
        |coin| (coin == g.coin).then_some(g.asset)).unwrap()[0].cut
}

#[test]
fn archived_resting_reduce_only_cleanup_precedes_later_price_action() {
    let g = golden();
    assert_eq!(cut(&g, &actions(&g, false), &[], &SystemAnchors::default()),
        Cut { lo: 2, hi: 2, fills_lo: 0 });
    assert_eq!(cut(&g, &actions(&g, true), &[], &SystemAnchors::default()),
        Cut { lo: 0, hi: 0, fills_lo: 0 });
}

#[test]
fn resting_cleanup_requires_adjacent_unique_status_identity() {
    for case in ["head", "gap", "duplicateHead", "duplicateTail", "wrongUser", "wrongCoin", "wrongSide",
        "newOid", "newAge", "notReduceOnly", "trigger", "children", "positionTpsl", "market", "ioc",
        "headTime", "headPrice", "headSize", "headRemaining", "headCloid", "headSide", "headReduceOnly"] {
        let mut g = golden();
        let a = actions(&g, false);
        match case {
            "head" => { g.statuses.remove(0); },
            "gap" => { let mut gap = g.statuses[0].clone(); gap.status = "futureStatus".into(); g.statuses.insert(1, gap); },
            "duplicateHead" => g.statuses.push(g.statuses[0].clone()),
            "duplicateTail" => g.statuses.push(g.statuses[1].clone()),
            "wrongUser" => g.statuses[1].user = alloy::primitives::Address::repeat_byte(9),
            "wrongCoin" => g.statuses[1].order.coin = "BTC".into(),
            "wrongSide" => g.statuses[1].order.side = crate::order_book::Side::Bid,
            "newOid" => g.statuses[1].order.oid = g.statuses[0].order.oid,
            "newAge" => g.statuses[1].order.timestamp = g.statuses[0].order.timestamp,
            "notReduceOnly" => g.statuses[1].order.reduce_only = false,
            "trigger" => g.statuses[1].order.is_trigger = true,
            "children" => g.statuses[1].order.children.push(serde_json::json!({})),
            "positionTpsl" => g.statuses[1].order.is_position_tpsl = true,
            "market" => g.statuses[1].order.order_type = "Market".into(),
            "ioc" => g.statuses[1].order.tif = Some("Ioc".into()),
            "headTime" => g.statuses[0].order.timestamp -= 1,
            "headPrice" => g.statuses[0].order.limit_px = "1193".into(),
            "headSize" => g.statuses[0].order.orig_sz = "0.03".into(),
            "headRemaining" => g.statuses[0].order.sz = "0.01".into(),
            "headCloid" => g.statuses[0].order.cloid = None,
            "headSide" => g.statuses[0].order.side = crate::order_book::Side::Bid,
            "headReduceOnly" => g.statuses[0].order.reduce_only = false,
            _ => unreachable!(),
        }
        assert_eq!(cut(&g, &a, &[], &SystemAnchors::default()).lo, 1, "{case}");
    }
}

#[test]
fn resting_cleanup_requires_single_resting_request_and_receipt() {
    for case in ["batch", "filled", "error", "mixedReceipt", "unknownReceipt", "receiptCloid", "alo", "ioc", "notReduceOnly", "unknownUser"] {
        let mut g = golden();
        match case {
            "batch" => { let another = g.action["orders"][0].clone(); g.action["orders"].as_array_mut().unwrap().push(another); },
            "filled" => g.response["res"]["response"]["data"]["statuses"][0] = serde_json::json!({"filled":{"oid":585152654719u64}}),
            "error" => g.response["res"]["response"]["data"]["statuses"][0]["error"] = "rejected".into(),
            "mixedReceipt" => g.response["res"]["response"]["data"]["statuses"][0]["filled"] = serde_json::json!({"oid":585152654719u64}),
            "unknownReceipt" => g.response["res"]["response"]["data"]["statuses"].as_array_mut().unwrap().push(serde_json::json!({"error":"another item"})),
            "receiptCloid" => g.response["res"]["response"]["data"]["statuses"][0]["resting"]["cloid"] = "different".into(),
            "alo" => g.action["orders"][0]["t"]["limit"]["tif"] = "Alo".into(),
            "ioc" => g.action["orders"][0]["t"]["limit"]["tif"] = "Ioc".into(),
            "notReduceOnly" => g.action["orders"][0]["r"] = false.into(),
            "unknownUser" => g.response["user"] = serde_json::Value::Null,
            _ => unreachable!(),
        }
        assert_eq!(cut(&g, &actions(&g, false), &[], &SystemAnchors::default()).lo, 1, "{case}");
    }
}

#[test]
fn resting_cleanup_rejects_competing_owners_and_fill_paths() {
    for case in ["duplicateOwner", "noOwner", "systemNew", "systemOld", "systemCancel", "vault", "manualCancel", "fill", "twap", "trigger"] {
        let mut g = golden();
        let mut a = actions(&g, false);
        let mut system = SystemAnchors::default();
        let mut fills = Vec::new();
        let new = g.statuses[0].order.oid;
        let old = g.statuses[1].order.oid;
        match case {
            "duplicateOwner" => { let mut other = a.actions[0].clone(); other.rank = (2, 3, 0); a.actions.push(other); },
            "noOwner" => { let MarkAction::Book(book) = &mut a.actions[0].action else { panic!() }; book.resting_reduce_only = None; },
            "systemNew" => { system.competing_orders.insert(new); },
            "systemOld" => { system.competing_orders.insert(old); },
            "systemCancel" => { system.competing_ioc_cancels.insert(old); },
            "vault" => { system.unplaced_vault_withdrawals.insert(g.statuses[0].user); },
            "manualCancel" => {
                let MarkAction::Book(book) = &mut a.actions[0].action else { panic!() };
                book.cancelled.push(old);
                let statuses = BlockStatuses::of(g.statuses.clone()).unwrap();
                assert!(statuses.resting_reduce_only.attributed(&a, &[], &[], &|_| Some(g.asset), &system, &|oid| oid == old).is_empty());
                continue;
            },
            "fill" | "twap" => fills.push(BlockFill { coin: g.coin.clone().into(), px: "1192".into(), sz: "0.02".into(), crossed: true,
                oid: new, tid: 1, action_position: None, twap: case == "twap", liquidation_mark_px: None }),
            "trigger" => { let mut t = g.statuses[1].clone(); t.status = "triggered".into(); t.order.oid += 1; g.statuses.push(t); },
            _ => unreachable!(),
        }
        assert_eq!(cut(&g, &a, &fills, &system).lo, if case == "duplicateOwner" { 0 } else { 1 }, "{case}");
    }
}
