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

fn close_golden() -> Golden { serde_json::from_str(include_str!("reduce_only_close_golden.json")).unwrap() }

#[test]
fn full_reduce_only_ioc_cleanup_precedes_the_next_price_action() {
    let g = close_golden();
    assert_eq!(g.statuses.iter().map(|s| s.status.as_str()).collect::<Vec<_>>(),
        ["open", "filled", "reduceOnlyCanceled"]);
    assert_eq!(cut(&g, &actions(&g, false), &SystemAnchors::default()), Cut { lo: 2, hi: 2, fills_lo: 2 });
    assert_eq!(cut(&g, &actions(&g, true), &SystemAnchors::default()), Cut { lo: 0, hi: 0, fills_lo: 0 });
    let mut with_boundary = close_golden();
    let mut boundary = with_boundary.statuses[0].clone(); boundary.status = "futureStatus".into();
    with_boundary.statuses.push(boundary);
    assert_eq!(cut(&with_boundary, &actions(&with_boundary, false), &SystemAnchors::default()).lo, 2,
        "a following boundary preserves an already witnessed cleanup");
}

#[test]
fn reduce_only_cleanup_requires_complete_contiguous_identity_and_paired_quantity() {
    for case in ["noHead", "noTerminal", "interrupted", "unknownCancel", "cancelNotReduceOnly",
        "wrongUser", "wrongCoin", "wrongSide", "triggerCancel", "takerNotReduceOnly", "partialTerminal",
        "wrongTerminalKey", "duplicateHead", "noMaker", "duplicateMaker", "duplicateTaker", "makerSize",
        "makerPrice", "makerTwap", "shortQuantity", "systemCancel", "multipleRequests", "noReceipt", "duplicateCleanup", "notOldOrder"] {
        let mut g = close_golden();
        let mut a = actions(&g, false);
        let mut system = SystemAnchors::default();
        match case {
            "duplicateCleanup" => g.statuses.push(g.statuses[2].clone()),
            "notOldOrder" => g.statuses[2].order.timestamp = g.statuses[0].order.timestamp,
            "noHead" => { g.statuses.remove(0); },
            "noTerminal" => { g.statuses.remove(1); },
            "interrupted" => { let mut b = g.statuses[0].clone(); b.status = "futureStatus".into(); g.statuses.insert(2, b); },
            "unknownCancel" => g.statuses[2].status = "futureStatus".into(),
            "cancelNotReduceOnly" => g.statuses[2].order.reduce_only = false,
            "wrongUser" => g.statuses[2].user = alloy::primitives::Address::repeat_byte(9),
            "wrongCoin" => g.statuses[2].order.coin = "BTC".into(),
            "wrongSide" => g.statuses[2].order.side = OrderSide::Bid,
            "triggerCancel" => g.statuses[2].order.is_trigger = true,
            "takerNotReduceOnly" => { g.statuses[0].order.reduce_only = false; g.statuses[1].order.reduce_only = false; },
            "partialTerminal" => g.statuses[1].order.sz = "1".into(),
            "wrongTerminalKey" => g.statuses[1].order.cloid = Some("another".into()),
            "duplicateHead" => g.statuses.push(g.statuses[0].clone()),
            "noMaker" => g.fills.retain(|f| f.1.crossed),
            "duplicateMaker" => g.fills.push(g.fills[0].clone()),
            "duplicateTaker" => g.fills.push(g.fills[1].clone()),
            "makerSize" => g.fills[0].1.sz = "1".into(),
            "makerPrice" => g.fills[0].1.px = "0.1".into(),
            "makerTwap" => g.fills[0].1.twap_id = Some(1),
            "shortQuantity" => { for f in &mut g.fills { f.1.sz = "1".into(); } },
            "systemCancel" => system.before.cancelled.push(g.statuses[2].order.oid),
            "multipleRequests" => { let MarkAction::Book(b) = &mut a.actions[0].action else { panic!() }; b.single_ioc_order = false; },
            "noReceipt" => { let MarkAction::Book(b) = &mut a.actions[0].action else { panic!() }; b.created.clear(); },
            _ => unreachable!(),
        }
        let statuses = BlockStatuses::of(g.statuses.clone()).unwrap();
        let fills = g.fills.iter().map(BlockFill::of).collect::<Vec<_>>();
        let attributed = statuses.filled_iocs.attributed(&a, &fills, &|coin| (coin == g.coin).then_some(g.asset), &system);
        assert!(!attributed.iter().any(|(_, oids)| oids.contains(&584_547_511_107)), "{case}");
    }
}

fn margin_golden() -> Golden { serde_json::from_str(include_str!("margin_cancel_golden.json")).unwrap() }

#[test]
fn partially_filled_maker_margin_cancel_runs_inside_frontend_market_taker() {
    let g = margin_golden();
    assert_eq!(g.statuses[7].status, "marginCanceled");
    assert_eq!(g.statuses[7].order.sz, "0.00008");
    assert_eq!(cut(&g, &actions(&g, false), &SystemAnchors::default()),
        Cut { lo: g.diffs.len(), hi: g.diffs.len(), fills_lo: g.fills.len() });
    assert_eq!(cut(&g, &actions(&g, true), &SystemAnchors::default()), Cut { lo: 0, hi: 0, fills_lo: 0 });
    // The same witnessed immediate execution also applies to ordinary limit IOC presentation.
    let mut limit = margin_golden();
    for i in [0, 8] { limit.statuses[i].order.order_type = "Limit".into(); limit.statuses[i].order.tif = Some("Ioc".into()); }
    limit.action["orders"][0]["t"]["limit"]["tif"] = "Ioc".into();
    assert_eq!(cut(&limit, &actions(&limit, false), &SystemAnchors::default()).lo, limit.diffs.len());
}

#[test]
fn maker_margin_cancel_requires_an_uninterrupted_unique_run_and_complete_paired_fills() {
    let oid = 584_936_839_855;
    for case in ["head", "terminal", "interrupted", "duplicateHead", "duplicateCancel", "unknownStatus",
        "wrongCoin", "wrongSide", "trigger", "unreachedPrice", "makerPrice", "zeroRemainder", "unfilled",
        "terminalTif", "terminalUser", "terminalOrderType", "partialTerminal", "partialTaker", "noReceipt", "duplicateReceipt",
        "systemOwner", "systemCancel", "competingCancel", "noFills", "noMaker", "duplicateMaker",
        "duplicateTaker", "makerQuantity", "makerTwap", "unrelatedMaker", "noPair"] {
        let mut g = margin_golden();
        let mut a = actions(&g, false);
        let mut system = SystemAnchors::default();
        match case {
            "head" => { g.statuses.remove(0); },
            "terminal" => { g.statuses.pop(); },
            "interrupted" => { let mut s = g.statuses[0].clone(); s.status = "futureStatus".into(); g.statuses.insert(7, s); },
            "duplicateHead" => g.statuses.push(g.statuses[0].clone()),
            "duplicateCancel" => g.statuses.insert(7, g.statuses[7].clone()),
            "unknownStatus" => g.statuses[7].status = "canceled".into(),
            "wrongCoin" => g.statuses[7].order.coin = "ETH".into(),
            "wrongSide" => g.statuses[7].order.side = OrderSide::Ask,
            "trigger" => g.statuses[7].order.is_trigger = true,
            "unreachedPrice" => g.statuses[7].order.limit_px = "1".into(),
            "makerPrice" => { for f in &mut g.fills { if f.1.tid == 650_604_234_805_654 { f.1.px = "80586".into(); } } },
            "zeroRemainder" => g.statuses[7].order.sz = "0".into(),
            "unfilled" => g.statuses[7].order.sz = g.statuses[7].order.orig_sz.clone(),
            "terminalTif" => g.statuses[8].order.tif = Some("Ioc".into()),
            "terminalUser" => g.statuses[8].user = alloy::primitives::Address::repeat_byte(9),
            "terminalOrderType" => g.statuses[8].order.order_type = "Limit".into(),
            "partialTerminal" => g.statuses[8].order.sz = "0.00001".into(),
            "partialTaker" => { g.statuses[8].order.sz = "0.00001".into(); g.fills[14].1.sz = "0.00069".into(); g.fills[15].1.sz = "0.00069".into(); },
            "noReceipt" => { let MarkAction::Book(b) = &mut a.actions[0].action else { panic!() }; b.created.clear(); },
            "duplicateReceipt" => { let mut b = a.actions[0].clone(); b.rank = (2, 3, 0); a.actions.push(b); },
            "systemOwner" => system.before.created.push(g.statuses[0].order.oid),
            "systemCancel" => system.before.cancelled.push(oid),
            "competingCancel" => { system.competing_orders.insert(oid); },
            "noFills" => g.fills.clear(),
            "noMaker" => g.fills.retain(|f| f.1.oid != oid),
            "duplicateMaker" => g.fills.push(g.fills[12].clone()),
            "duplicateTaker" => g.fills.push(g.fills[13].clone()),
            "makerQuantity" => g.fills[12].1.sz = "0.00095".into(),
            "makerTwap" => g.fills[12].1.twap_id = Some(1),
            "unrelatedMaker" => g.fills[12].1.oid += 1,
            "noPair" => g.fills[12].1.tid += 1,
            _ => unreachable!(),
        }
        let statuses = BlockStatuses::of(g.statuses).unwrap();
        let fills = g.fills.iter().map(BlockFill::of).collect::<Vec<_>>();
        let attributed = statuses.filled_iocs.attributed(&a, &fills, &|coin| (coin == "BTC").then_some(0), &system);
        assert!(!attributed.iter().any(|(_, oids)| oids.contains(&oid)), "{case}");
    }
}
