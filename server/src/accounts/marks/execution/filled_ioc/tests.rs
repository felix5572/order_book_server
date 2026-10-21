use super::*;
use crate::accounts::marks::{actions::tests::{mark_actions, ok, replica_line},
    book::BookDiff, execution::{price_points_with_system, BlockStatuses, Cut}};
use crate::types::node_data::{NodeDataFill, NodeDataOrderDiff};
use serde::Deserialize;

#[derive(Deserialize)]
struct Golden {
    height: u64, coin: String, asset: u32, block_time: String,
    action: serde_json::Value, response: serde_json::Value,
    statuses: Vec<NodeDataOrderStatus>, diffs: Vec<NodeDataOrderDiff>, fills: Vec<NodeDataFill>,
}

fn goldens() -> Vec<Golden> { serde_json::from_str(include_str!("goldens.json")).unwrap() }

fn actions(g: &Golden, after: bool) -> MarkActions {
    let ioc = (g.action.to_string(), g.response.to_string());
    let sga = (r#"{"type":"SetGlobalAction","pxs":[]}"#, ok(r#"{"type":"default"}"#));
    let first = [(ioc.0.as_str(), ioc.1.as_str())];
    let second = [(sga.0, sga.1.as_str())];
    let bundles = if after { [&second[..], &first[..]] } else { [&first[..], &second[..]] };
    mark_actions(&replica_line(g.height, &bundles).replace("2026-10-20T13:36:16.650019121", &g.block_time))
}

fn cut(g: &Golden, a: &MarkActions, system: &SystemAnchors) -> Cut {
    let statuses = BlockStatuses::of(g.statuses.clone()).unwrap();
    let diffs = g.diffs.iter().map(BookDiff::of).collect::<Result<Vec<_>, _>>().unwrap();
    let fills = g.fills.iter().map(BlockFill::of).collect::<Vec<_>>();
    price_points_with_system(a, &statuses, &diffs, &fills, system,
        |coin| (coin == g.coin).then_some(g.asset)).unwrap()[0].cut
}

#[test]
fn archived_filled_ioc_cancels_run_with_the_receipted_taker() {
    for g in goldens() {
        assert!(g.statuses.iter().filter(|s| s.status == "selfTradeCanceled")
            .all(|s| s.order.timestamp < g.statuses[0].order.timestamp));
        assert_eq!(cut(&g, &actions(&g, false), &SystemAnchors::default()).lo, g.diffs.len(), "{} before SGA", g.coin);
        assert_eq!(cut(&g, &actions(&g, true), &SystemAnchors::default()), Cut { lo: 0, hi: 0, fills_lo: 0 }, "{} after SGA", g.coin);
    }
    let g = goldens().remove(0);
    assert!(g.statuses.iter().any(|s| s.status == "selfTradeCanceled" && s.user != g.statuses[0].user), "PROVE includes a different vault address");
}

#[test]
fn missing_or_interrupted_endpoints_leave_old_cancels_unplaced() {
    for case in ["head", "tail", "unknown", "nested", "partial", "duplicateHead", "duplicateRun", "makerAfterCancel"] {
        let mut g = goldens().remove(0);
        let a = actions(&g, false);
        match case {
            "head" => { g.statuses.remove(0); },
            "tail" => { g.statuses.pop(); },
            "unknown" | "nested" => {
                let mut boundary = g.statuses[0].clone();
                boundary.order.oid += 1;
                if case == "unknown" { boundary.status = "futureStatus".into(); }
                g.statuses.insert(2, boundary);
            },
            "partial" => g.statuses.last_mut().unwrap().status = "canceled".into(),
            "duplicateHead" => g.statuses.push(g.statuses[0].clone()),
            "duplicateRun" => g.statuses.extend(g.statuses.clone()),
            "makerAfterCancel" => {
                let mut maker = g.statuses[1].clone(); maker.status = "filled".into(); maker.order.sz = "0".into();
                g.statuses.insert(2, maker);
            },
            _ => unreachable!(),
        }
        assert_eq!(cut(&g, &a, &SystemAnchors::default()).lo, 0, "{case}");
    }
}

#[test]
fn endpoints_and_cancel_prices_must_agree() {
    for field in ["oid", "user", "coin", "price", "origSz", "sz", "tif", "cloid", "time", "reduceOnly", "trigger", "cancelPrice", "cancelSide"] {
        let mut g = goldens().remove(0);
        let a = actions(&g, false);
        let tail = g.statuses.last_mut().unwrap();
        match field {
            "oid" => tail.order.oid += 1,
            "user" => tail.user = alloy::primitives::Address::repeat_byte(9),
            "coin" => tail.order.coin = "BTC".into(),
            "price" => tail.order.limit_px = "0.2".into(),
            "origSz" => tail.order.orig_sz = "101".into(),
            "sz" => tail.order.sz.clone_from(&tail.order.orig_sz),
            "tif" => tail.order.tif = Some("Gtc".into()),
            "cloid" => tail.order.cloid = Some("another".into()),
            "time" => tail.order.timestamp -= 1,
            "reduceOnly" => tail.order.reduce_only = true,
            "trigger" => tail.order.is_trigger = true,
            "cancelPrice" => g.statuses[1].order.limit_px = "0.3".into(),
            "cancelSide" => g.statuses[1].order.side = OrderSide::Bid,
            _ => unreachable!(),
        }
        assert_eq!(cut(&g, &a, &SystemAnchors::default()).lo, 0, "{field}");
    }
}

#[test]
fn filled_ioc_needs_a_unique_receipt_and_actual_non_twap_fills() {
    for case in ["noFills", "twap", "unrelatedFill", "makerMismatch", "noReceipt", "duplicateReceipt", "duplicateItem", "systemOwner", "systemCompetitor", "wrongTime"] {
        let mut g = goldens().remove(1);
        let mut a = actions(&g, false);
        let mut system = SystemAnchors::default();
        let oid = g.statuses[0].order.oid;
        match case {
            "noFills" => g.fills.clear(),
            "twap" => { for f in &mut g.fills { f.1.twap_id = Some(1); } },
            "unrelatedFill" => { for f in &mut g.fills { if f.1.crossed { f.1.oid += 1; } } },
            "makerMismatch" => g.statuses[1].order.oid += 1,
            "noReceipt" => { let MarkAction::Book(b) = &mut a.actions[0].action else { panic!() }; b.created.clear(); },
            "duplicateReceipt" => { let mut copy = a.actions[0].clone(); copy.rank = (2, 3, 0); a.actions.push(copy); },
            "duplicateItem" => { let MarkAction::Book(b) = &mut a.actions[0].action else { panic!() }; b.created.push(oid); },
            "systemOwner" => system.before.created.push(oid),
            "systemCompetitor" => { system.competing_orders.insert(oid); },
            "wrongTime" => a.time_ns += 1_000_000,
            _ => unreachable!(),
        }
        assert_eq!(cut(&g, &a, &system).lo, 0, "{case}");
    }
}

#[test]
fn incomplete_filled_ioc_run_blocks_the_weaker_same_account_guess() {
    use crate::accounts::marks::{actions::{BookAnchors, RankedAction}, book::tests::diff_line};
    let mut g = goldens().remove(1);
    g.statuses.pop();
    let maker = g.statuses.last().unwrap();
    let cancel = maker.order.oid;
    let new = diff_line(g.height, &g.coin, cancel, "A", &maker.order.limit_px,
        r#"{"new":{"sz":"1895.0"}}"#).events().remove(0);
    g.diffs.insert(0, new);
    let mut a = actions(&g, false);
    a.actions.push(RankedAction { rank: (0, 1, 0), user: Some(maker.user),
        action: MarkAction::Book(BookAnchors { created: vec![cancel], ..Default::default() }) });
    assert_eq!(cut(&g, &a, &SystemAnchors::default()).lo, 1,
        "only the creation anchors; incomplete IOC evidence must block the same-account guess");
}

#[test]
fn partial_ioc_terminal_status_still_anchors_its_cancellation() {
    let mut g = goldens().into_iter().find(|g| g.height == 1_192_236_393).unwrap();
    assert_eq!(g.statuses.last().unwrap().order.sz, "33.0");
    assert_eq!(g.statuses[0].order.orig_sz, "99.0");
    assert_eq!(g.fills.iter().find(|fill| fill.1.crossed).unwrap().1.sz, "66.0");
    let a = actions(&g, false);
    assert_eq!(cut(&g, &a, &SystemAnchors::default()).lo, 3,
        "the removal after the maker fill belongs before SGA");
    g.statuses.pop();
    assert_eq!(cut(&g, &a, &SystemAnchors::default()).lo, 2,
        "without a terminal status only the actual maker fill anchors");
}

#[test]
fn terminal_remainder_cannot_equal_or_exceed_the_original_size() {
    for size in ["99", "100"] {
        let mut g = goldens().into_iter().find(|g| g.height == 1_192_236_393).unwrap();
        let a = actions(&g, false);
        g.statuses.last_mut().unwrap().order.sz = size.into();
        assert_eq!(cut(&g, &a, &SystemAnchors::default()).lo, 2, "remainder {size}");
    }
}
