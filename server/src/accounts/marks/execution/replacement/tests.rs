use super::*;
use super::super::{BlockFill, BlockStatuses, price_points};
use super::super::super::{actions::{BookAnchors, RankedAction, tests::{mark_actions, replica_line}}, book::BookDiff, system::{SystemAction, SystemBlock}};
use crate::types::node_data::{NodeDataFill, NodeDataOrderDiff};
use serde::Deserialize;

#[derive(Deserialize)]
struct Golden { action: serde_json::Value, response: serde_json::Value, statuses: Vec<NodeDataOrderStatus>, diffs: Vec<NodeDataOrderDiff>, fills: Vec<NodeDataFill> }
fn golden() -> Golden { serde_json::from_str(include_str!("golden.json")).unwrap() }
fn actions(g: &Golden) -> MarkActions {
    let mut a = mark_actions(&replica_line(1_191_964_185, &[&[(&g.action.to_string(), &g.response.to_string())]]));
    a.time_ns = 1_792_588_226_518_424_241;
    if let Some(modify) = a.actions.first_mut() { modify.rank = (0, 3, 1264); }
    a.actions.push(RankedAction { rank: (1, 3, 4), user: None, action: MarkAction::SetGlobal(Vec::new()) });
    a
}
fn asset(coin: &str) -> Option<u32> { (coin == "ZEC").then_some(214) }
fn attributed(g: &Golden, a: &MarkActions) -> Vec<(Rank, u64)> {
    BlockStatuses::of(g.statuses.clone()).unwrap().replacements.attributed(a, &asset, &SystemAnchors::default())
}

#[test]
fn archived_default_modify_fill_is_before_the_next_sga() {
    let g = golden(); let a = actions(&g);
    let s = BlockStatuses::of(g.statuses).unwrap();
    let diffs: Vec<_> = g.diffs.iter().map(|d| BookDiff::of(d).unwrap()).collect();
    let fills: Vec<_> = g.fills.iter().map(BlockFill::of).collect();
    let points = price_points(&a, &s, &diffs, &fills, asset).unwrap();
    assert_eq!((points[0].cut.lo, points[0].cut.hi, points[0].cut.fills_lo), (2, 2, 2));
    assert_eq!(fills[points[0].cut.fills_lo - 1].px.as_ref(), "1313.1");
    let mut later = a.clone(); later.actions[0].rank = (1, 3, 5);
    let points = price_points(&later, &s, &diffs, &fills, asset).unwrap();
    assert_eq!((points[0].cut.lo, points[0].cut.hi, points[0].cut.fills_lo), (0, 0, 0));
}

#[test]
fn every_request_field_old_order_and_creation_time_must_match() {
    for field in ["a", "b", "p", "s", "r", "c", "t", "old", "user", "time"] {
        let mut g = golden();
        match field {
            "a" => g.action["order"]["a"] = 215.into(),
            "b" | "r" => g.action["order"][field] = true.into(),
            "p" => g.action["order"][field] = "1311".into(),
            "s" => g.action["order"][field] = "15.21".into(),
            "c" => g.action["order"][field] = "0x01".into(),
            "t" => g.action["order"][field] = serde_json::json!({"limit":{"tif":"Ioc"}}),
            "old" => g.action["oid"] = 9.into(),
            "user" => g.response["user"] = alloy::primitives::Address::ZERO.to_string().into(),
            _ => g.statuses[0].order.timestamp -= 1,
        }
        assert!(attributed(&g, &actions(&g)).is_empty(), "{field}");
    }
    let mut g = golden(); g.action["order"]["p"] = "1312.000".into();
    assert_eq!(attributed(&g, &actions(&g)).len(), 1, "numeric, not string equality");
}

#[test]
fn ambiguous_requests_pairs_and_interrupted_status_runs_stay_unplaced() {
    let mut g = golden(); let mut a = actions(&g);
    a.actions.push(a.actions[0].clone());
    assert!(attributed(&g, &a).is_empty());
    let a = actions(&g);
    g.statuses.extend(g.statuses.clone());
    assert!(attributed(&g, &a).is_empty());
    let mut g = golden(); let mut boundary = g.statuses[2].clone(); boundary.status = "futureStatus".into();
    g.statuses.insert(1, boundary);
    assert!(attributed(&g, &a).is_empty());
    let mut g = golden(); g.statuses.remove(0);
    assert!(attributed(&g, &a).is_empty());
}

#[test]
fn only_known_successful_default_modify_uses_the_status_pair() {
    for response in [serde_json::json!({"status":"err","response":"cannot modify"}),
        serde_json::json!({"status":"ok","response":{"type":"future"}}),
        serde_json::json!({"status":"ok","response":{"type":"order","data":{"statuses":[]}}})] {
        let mut g = golden(); g.response["res"] = response;
        assert!(attributed(&g, &actions(&g)).is_empty());
    }
    let g = golden(); let mut a = actions(&g);
    a.actions.push(RankedAction { rank: (0, 3, 123), user: None,
        action: MarkAction::Book(BookAnchors { created: vec![g.statuses[0].order.oid], ..BookAnchors::default() }) });
    assert!(attributed(&g, &a).is_empty(), "a receipt already owns the new oid");
}

#[test]
fn a_cloid_can_name_the_old_order_but_not_an_unrelated_cancel() {
    let mut g = golden(); g.action["oid"] = "0x01".into();
    assert!(attributed(&g, &actions(&g)).is_empty());
    g.statuses[1].order.cloid = Some("0x01".into());
    assert_eq!(attributed(&g, &actions(&g)).len(), 1);
    g.statuses[1].user = alloy::primitives::Address::ZERO;
    assert!(attributed(&g, &actions(&g)).is_empty());
}

#[test]
fn corewriter_and_default_modify_compete_symmetrically_for_the_same_open() {
    let g = golden(); let a = actions(&g);
    let event: SystemAction = serde_json::from_value(serde_json::json!({"user":g.response["user"],
        "evm_tx_hash":format!("0x{}","01".repeat(32)),
        "action":{"type":"order","orders":[g.action["order"]],"grouping":"na"}})).unwrap();
    let system = SystemBlock::of([event], &g.statuses).unwrap();
    let statuses = BlockStatuses::of(g.statuses).unwrap();
    let anchors = system.anchors(&a, &statuses, Some((0.33,1.0)), asset).unwrap();
    assert!(anchors.before.created.is_empty());
    assert!(anchors.after.created.is_empty());
    assert!(statuses.replacements.attributed(&a, &asset, &anchors).is_empty());
}
