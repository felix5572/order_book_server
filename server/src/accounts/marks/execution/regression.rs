//! Reduced raw-wire regressions. Full blocks/checkpoints stay under research-data, source hashes accompany fixtures.
use super::*;
use crate::accounts::{marks::actions::tests::{mark_actions, ok, replica_line}, test_support::status_line};
use crate::types::node_data::NodeDataOrderDiff;
use serde::Deserialize;

#[derive(Deserialize)]
struct Golden {
    height: u64,
    coin: String,
    asset: u32,
    block_time: String,
    action: serde_json::Value,
    response: serde_json::Value,
    statuses: Vec<NodeDataOrderStatus>,
    diffs: Vec<NodeDataOrderDiff>,
}

fn goldens() -> Vec<Golden> {
    serde_json::from_str(include_str!("self_trade_goldens.json")).unwrap()
}

fn actions(g: &Golden, after: bool) -> MarkActions {
    let ioc = (g.action.to_string(), g.response.to_string());
    let sga = (r#"{"type":"SetGlobalAction","pxs":[]}"#, ok(r#"{"type":"default"}"#));
    let first = [(ioc.0.as_str(), ioc.1.as_str())];
    let second = [(sga.0, sga.1.as_str())];
    let bundles = if after { [&second[..], &first[..]] } else { [&first[..], &second[..]] };
    let wire = replica_line(g.height, &bundles).replace("2026-10-20T13:36:16.650019121", &g.block_time);
    mark_actions(&wire)
}

fn cut(g: &Golden, actions: &MarkActions, statuses: Vec<NodeDataOrderStatus>) -> Cut {
    let statuses = BlockStatuses::of(statuses).unwrap();
    let diffs = g.diffs.iter().map(BookDiff::of).collect::<Result<Vec<_>, _>>().unwrap();
    price_points(actions, &statuses, &diffs, &[], |coin| (coin == g.coin).then_some(g.asset)).unwrap()[0].cut
}

#[test]
fn archived_old_cross_vault_cancels_are_placed_on_the_rejected_ioc() {
    for g in goldens() {
        assert!(g.statuses[1..].iter().all(|s| s.order.timestamp < g.statuses[0].order.timestamp));
        assert!(g.statuses[1..].iter().any(|s| s.user != g.statuses[0].user));
        assert_eq!(cut(&g, &actions(&g, false), g.statuses.clone()), Cut { lo: g.diffs.len(), hi: g.diffs.len(), fills_lo: 0 }, "{} before SGA", g.coin);
        assert_eq!(cut(&g, &actions(&g, true), g.statuses.clone()), Cut { lo: 0, hi: 0, fills_lo: 0 }, "{} after SGA", g.coin);
        assert_eq!(cut(&g, &actions(&g, false), g.statuses[1..].to_vec()).lo, 0, "without IOC head, old orders remain unplaced");
    }
}

#[test]
fn a_rejected_ioc_must_match_every_request_field_and_the_block_time() {
    for field in ["user", "asset", "side", "price", "size", "reduceOnly", "cloid", "time", "response"] {
        let mut g = goldens().remove(0);
        match field {
            "user" => g.response["user"] = serde_json::json!(Address::repeat_byte(9)),
            "asset" => g.action["orders"][0]["a"] = serde_json::json!(g.asset + 1),
            "side" => g.action["orders"][0]["b"] = serde_json::json!(true),
            "price" => g.action["orders"][0]["p"] = serde_json::json!("0.04207"),
            "size" => g.action["orders"][0]["s"] = serde_json::json!("382"),
            "reduceOnly" => g.action["orders"][0]["r"] = serde_json::json!(true),
            "cloid" => g.action["orders"][0]["c"] = serde_json::json!("0x00000000000000000000000000000001"),
            "time" => g.statuses[0].order.timestamp -= 1,
            "response" => g.response["res"]["response"]["data"]["statuses"][0]["error"] = serde_json::json!("Insufficient margin to place order. asset=108"),
            _ => unreachable!(),
        }
        assert_eq!(cut(&g, &actions(&g, false), g.statuses.clone()).lo, 0, "mismatched {field} must not place old cancels");
    }
}

#[test]
fn a_status_boundary_prevents_joining_unrelated_self_trade_cancels() {
    for boundary in ["open", "filled", "triggered", "canceled", "futureStatus"] {
        let g = goldens().remove(0);
        let mut statuses = g.statuses.clone();
        let mut unrelated = statuses[0].clone();
        unrelated.status = boundary.to_string();
        unrelated.order.coin = "xyz:SPCX".to_string();
        unrelated.order.oid += 1;
        statuses.insert(1, unrelated);
        assert_eq!(cut(&g, &actions(&g, false), statuses).lo, 0, "{boundary} closes the run");
    }
}

#[test]
fn repeated_requests_or_status_heads_are_not_arbitrarily_assigned() {
    let g = goldens().remove(0);
    let mut a = actions(&g, false);
    let mut copy = a.actions[0].clone();
    copy.rank = (2, 3, 0);
    a.actions.push(copy);
    assert_eq!(cut(&g, &a, g.statuses.clone()).lo, 0, "same request across SGA");
    let mut a = actions(&g, false);
    let MarkAction::Book(book) = &mut a.actions[0].action else { panic!("book") };
    book.rejected_iocs.push(book.rejected_iocs[0].clone());
    assert_eq!(cut(&g, &a, g.statuses.clone()).lo, 0, "duplicate within a batch");
    let mut statuses = g.statuses.clone();
    let mut other = statuses[0].clone();
    other.order.oid += 1;
    statuses.push(other);
    assert_eq!(cut(&g, &actions(&g, false), statuses).lo, 0, "an indistinguishable second rejection, even with no cancel");
}

#[test]
fn a_malformed_ioc_head_does_not_silently_anchor_a_cancel() {
    let mut g = goldens().remove(0);
    g.statuses[0].order.orig_sz = "garbage".to_string();
    assert!(BlockStatuses::of(g.statuses).unwrap_err().contains("original size"));
}

/// bm 1191473229: xyz stop fills precede a native WLD IOC, which precedes native SGA. A native trigger really
/// does occur after its selector. Keep both scopes: scoping away *all* triggers would miss the upper bound.
#[test]
fn a_hip3_trigger_before_a_native_ioc_does_not_belong_to_native_sga() {
    use super::tests::{book, diff, fill, set_global, NEW};
    let actions = MarkActions {
        height: 1_191_473_229, time_ns: 1,
        actions: vec![book((1, 3, 193), super::super::actions::BookAnchors { created: vec![20], ..Default::default() }), set_global((2, 3, 0))],
    };
    let mut hip3 = status_line(1_191_473_229, "triggered", Address::repeat_byte(1), 10, None).events().remove(0);
    hip3.order.coin = "xyz:SPCX".to_string();
    let native = status_line(1_191_473_229, "triggered", Address::repeat_byte(2), 30, None).events().remove(0);
    let statuses = BlockStatuses::of([hip3, native]).unwrap();
    let diffs = [diff(11, r#"{"update":{"origSz":"0.003","newSz":"0.001"}}"#), diff(21, r#"{"update":{"origSz":"0.003","newSz":"0.001"}}"#), diff(30, NEW)];
    let mut fills = [fill(1, 10, true), fill(1, 11, false), fill(2, 20, true), fill(2, 21, false)];
    fills[0].coin = "xyz:SPCX".into();
    fills[1].coin = "xyz:SPCX".into();
    let points = price_points(&actions, &statuses, &diffs, &fills, |coin| (coin == "BTC").then_some(0)).unwrap();
    assert_eq!(points[0].cut, Cut { lo: 2, hi: 2, fills_lo: 4 });
}

#[test]
fn one_batch_error_is_not_zipped_onto_the_first_of_several_orders() {
    let mut g = goldens().remove(0);
    let second = g.action["orders"][0].clone();
    g.action["orders"].as_array_mut().unwrap().push(second);
    // Includes the observed whole-batch validation error of bm 1191390021, and an unpaired IOC error: neither
    // identifies a request in the batch, so neither may place an old resting-order cancellation.
    for error in ["Price must be divisible by tick size. asset=108", "Order could not immediately match against any resting orders. asset=108"] {
        g.response["res"]["response"]["data"]["statuses"][0]["error"] = serde_json::json!(error);
        assert_eq!(cut(&g, &actions(&g, false), g.statuses.clone()).lo, 0);
    }
}

#[test]
fn an_ambiguous_ioc_run_cannot_fall_back_to_the_same_block_account_guess() {
    let g = goldens().remove(0);
    let maker = &g.statuses[1];
    let mut actions = actions(&g, false);
    actions.actions.push(RankedAction {
        rank: (0, 1, 0), user: Some(maker.user),
        action: MarkAction::Book(super::super::actions::BookAnchors { created: vec![maker.order.oid], ..Default::default() }),
    });
    let mut statuses = g.statuses.clone();
    let mut indistinguishable = statuses[0].clone();
    indistinguishable.order.oid += 1;
    statuses.push(indistinguishable);
    let statuses = BlockStatuses::of(statuses).unwrap();
    let new = crate::accounts::marks::book::tests::diff_line(g.height, &g.coin, maker.order.oid, "B", &maker.order.limit_px, r#"{"new":{"sz":"4011.7"}}"#);
    let mut diffs = vec![BookDiff::of(&new.events()[0]).unwrap()];
    diffs.extend(g.diffs.iter().map(|d| BookDiff::of(d).unwrap()));
    let points = price_points(&actions, &statuses, &diffs, &[], |coin| (coin == g.coin).then_some(g.asset)).unwrap();
    assert_eq!(points[0].cut.lo, 1, "only creation is placed; ambiguous cancellations stay unplaced even for the same-block same-user maker");
}

#[test]
fn unpaired_item_responses_preserve_placed_orders_but_do_not_attribute_cancels() {
    for requests in [1, 3] {
        let mut g = goldens().remove(0);
        let order = g.action["orders"][0].clone();
        g.action["orders"] = serde_json::json!(vec![order; requests]);
        g.response["res"]["response"]["data"]["statuses"].as_array_mut().unwrap()
            .push(serde_json::json!({"resting": {"oid": 99}}));
        let a = actions(&g, false);
        let MarkAction::Book(book) = &a.actions[0].action else { panic!("book") };
        assert_eq!(book.ioc_pairing_mismatch, Some(crate::accounts::marks::pairing::IocPairingMismatch::Unknown { requests, responses: 2 }));
        assert_eq!(book.created, [99], "response oids remain authoritative without request pairing");
        assert_eq!(book.rejected_iocs, []);
        assert!(matches!(a.actions[1].action, MarkAction::SetGlobal(_)), "price action is still parsed");
        assert_eq!(cut(&g, &a, g.statuses.clone()).lo, 0, "old cross-vault cancels stay unplaced");

        // Even if the maker was created in this block by the same user, a known IOC status run must not fall
        // back to the older RUNE heuristic when the action's item responses cannot be paired.
        let maker = &g.statuses[1];
        let mut a = a;
        a.actions.push(RankedAction {
            rank: (0, 1, 0), user: Some(maker.user),
            action: MarkAction::Book(super::super::actions::BookAnchors { created: vec![maker.order.oid], ..Default::default() }),
        });
        let new = crate::accounts::marks::book::tests::diff_line(g.height, &g.coin, maker.order.oid, "B", &maker.order.limit_px, r#"{"new":{"sz":"4011.7"}}"#);
        let mut diffs = vec![BookDiff::of(&new.events()[0]).unwrap()];
        diffs.extend(g.diffs.iter().map(|d| BookDiff::of(d).unwrap()));
        let statuses = BlockStatuses::of(g.statuses).unwrap();
        let points = price_points(&a, &statuses, &diffs, &[], |coin| (coin == g.coin).then_some(g.asset)).unwrap();
        assert_eq!(points[0].cut.lo, 1, "creation is placed, the unpaired IOC's cancels are not");
    }
}

#[test]
fn an_unpaired_request_must_not_make_another_action_appear_unique() {
    let mut g = goldens().remove(0);
    let mut a = actions(&g, false);
    g.response["res"]["response"]["data"]["statuses"].as_array_mut().unwrap()
        .push(serde_json::json!({"error": "unclassified item response"}));
    let mut unknown = actions(&g, false).actions.remove(0);
    unknown.rank = (2, 3, 0);
    let MarkAction::Book(book) = &unknown.action else { panic!("book") };
    assert_eq!(book.ioc_pairing_mismatch, Some(crate::accounts::marks::pairing::IocPairingMismatch::Unknown { requests: 1, responses: 2 }));
    a.actions.push(unknown);
    assert_eq!(cut(&g, &a, g.statuses.clone()).lo, 0, "the known response is not proof of a unique request");
    let MarkAction::Book(book) = &mut a.actions.last_mut().unwrap().action else { panic!("book") };
    book.takers[0].asset += 1;
    assert_eq!(cut(&g, &a, g.statuses.clone()).lo, g.diffs.len(), "an unrelated unknown response does not block attribution");
}
