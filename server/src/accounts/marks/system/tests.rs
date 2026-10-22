use crate::accounts::marks::decimal::ExactDecimal;
use crate::order_book::Side;
use super::*;
use crate::{accounts::marks::{actions::RankedAction, book::BookDiff, execution::{BlockFill, price_points_with_system}}, types::node_data::{NodeDataFill, NodeDataOrderDiff}};

#[derive(Deserialize)]
struct Golden { time_ns: u64, system: Vec<SystemAction>, statuses: Vec<NodeDataOrderStatus>, fills: Vec<NodeDataFill>, diffs: Vec<NodeDataOrderDiff> }
fn golden() -> Golden { serde_json::from_str(include_str!("core_writer_eth_golden.json")).unwrap() }
fn asset(coin: &str) -> Option<u32> { match coin { "ETH" => Some(1), "ZEC" => Some(2), _ => None } }
fn actions(time_ns: u64) -> MarkActions {
    MarkActions { height: 1_191_429_459, time_ns, actions: vec![RankedAction { rank: (0, 3, 0), user: None, action: MarkAction::SetGlobal(Vec::new()) }] }
}
const TIMING: Option<(f64, f64)> = Some((0.33, 1.0));

#[test]
fn archived_eth_corewriter_trade_precedes_sga_but_the_zec_trigger_does_not() {
    let g = golden();
    let system = SystemBlock::of(g.system, &g.statuses).unwrap();
    let statuses = BlockStatuses::of(g.statuses).unwrap();
    let actions = actions(g.time_ns);
    let anchors = system.anchors(&actions, &statuses, TIMING, asset).unwrap();
    assert_eq!(anchors.before.created, [581_053_416_509]);
    assert_eq!(anchors.after.created, Vec::<u64>::new());
    let diffs: Vec<_> = g.diffs.iter().map(|d| BookDiff::of(d).unwrap()).collect();
    let fills: Vec<_> = g.fills.iter().map(BlockFill::of).collect();
    let points = price_points_with_system(&actions, &statuses, &diffs, &fills, &anchors, asset).unwrap();
    let cut = points[0].cut;
    assert_eq!(cut.fills_lo, 2, "the two ETH legs precede SGA, the ZEC trigger legs follow it");
    assert_eq!((cut.lo, cut.hi), (1, 1));
    assert_eq!(fills[..cut.fills_lo].iter().find(|f| f.crossed).unwrap().px.as_ref(), "2446.7");
}

#[test]
fn every_request_field_and_unique_status_are_required() {
    for field in ["a", "b", "p", "s", "r", "c", "t"] {
        let mut wire: serde_json::Value = serde_json::from_str(include_str!("core_writer_eth_golden.json")).unwrap();
        let order = &mut wire["system"][0]["action"]["orders"][0];
        order[field] = match field {
            "a" => serde_json::json!(99), "b" | "r" => serde_json::json!(true),
            "p" => serde_json::json!("2440.6"), "s" => serde_json::json!("0.4173"),
            "c" => serde_json::json!("0x0000000000000000000000000000002d"),
            _ => serde_json::json!({"limit":{"tif":"Gtc"}}),
        };
        let g: Golden = serde_json::from_value(wire).unwrap();
        let s = SystemBlock::of(g.system, &g.statuses).unwrap();
        let statuses = BlockStatuses::of(g.statuses).unwrap();
        assert!(s.anchors(&actions(g.time_ns), &statuses, TIMING, asset).unwrap().before.created.is_empty(), "{field}");
    }
    let mut g = golden();
    g.system.push(g.system[0].clone());
    let s = SystemBlock::of(g.system, &g.statuses).unwrap();
    assert_eq!(s.anchors(&actions(g.time_ns), &BlockStatuses::default(), TIMING, asset).unwrap().before.created, Vec::<u64>::new());
    let mut g = golden();
    g.statuses.push(g.statuses[0].clone());
    let s = SystemBlock::of(g.system, &g.statuses).unwrap();
    assert_eq!(s.anchors(&actions(g.time_ns), &BlockStatuses::default(), TIMING, asset).unwrap().before.created, Vec::<u64>::new());
}

#[test]
fn a_failed_attempt_has_no_book_effect_and_missing_timing_is_not_guessed() {
    let g = golden();
    let s = SystemBlock::of(g.system.clone(), &[]).unwrap();
    assert_eq!(s.anchors(&actions(g.time_ns), &BlockStatuses::default(), None, asset).unwrap().before.created, Vec::<u64>::new());
    let s = SystemBlock::of(g.system, &g.statuses).unwrap();
    for timing in [None, Some((0.0, 1.0)), Some((0.33, 0.0)), Some((f64::NAN, 1.0))] {
        assert!(s.anchors(&actions(g.time_ns), &BlockStatuses::default(), timing, asset).is_err());
    }
}

#[test]
fn native_orders_are_not_reassigned_to_corewriter() {
    let g = golden();
    let s = SystemBlock::of(g.system, &g.statuses).unwrap();
    let mut a = actions(g.time_ns);
    a.actions.push(RankedAction { rank: (1, 3, 0), user: Some(g.statuses[0].user),
        action: MarkAction::Book(BookAnchors { created: vec![g.statuses[0].order.oid], ..BookAnchors::default() }) });
    assert_eq!(s.anchors(&a, &BlockStatuses::default(), TIMING, asset).unwrap().before.created, Vec::<u64>::new());
}

#[test]
fn zero_delay_alo_stays_after_the_native_price_action() {
    let mut g = golden();
    let Action::Order { orders, .. } = &mut g.system[0].action else { panic!() };
    orders[0].t.limit.as_mut().unwrap().tif = "Alo".into();
    g.statuses[0].order.tif = Some("Alo".into());
    let s = SystemBlock::of(g.system, &g.statuses).unwrap();
    let anchors = s.anchors(&actions(g.time_ns), &BlockStatuses::default(), None, asset).unwrap();
    assert_eq!(anchors.before.created, Vec::<u64>::new());
    assert_eq!(anchors.after.created, [581_053_416_509]);
}

#[test]
fn cancels_require_success_status_and_follow_their_delay_flag() {
    let mut g = golden();
    g.statuses[0].status = "canceled".into();
    let user = g.statuses[0].user;
    let oid = g.statuses[0].order.oid;
    for flag in [true, false] {
        g.system[0].action = Action::Cancel { cancels: vec![Cancel { a: 1, o: oid }], f: flag };
        let s = SystemBlock::of(g.system.clone(), &g.statuses).unwrap();
        let anchors = s.anchors(&actions(g.time_ns), &BlockStatuses::default(), TIMING, asset).unwrap();
        assert_eq!(if flag { anchors.after.cancelled } else { anchors.before.cancelled }, [oid]);
        let s = SystemBlock::of(g.system.clone(), &[]).unwrap();
        assert_eq!(s.anchors(&actions(g.time_ns), &BlockStatuses::default(), TIMING, asset).unwrap().after.cancelled, Vec::<u64>::new());
    }
    g.system[0].action = Action::CancelByCloid { cancels: vec![CancelByCloid { asset: 1, cloid: g.statuses[0].order.cloid.clone().unwrap() }], f: true };
    let s = SystemBlock::of(g.system, &g.statuses).unwrap();
    let mut a = actions(g.time_ns);
    a.actions.push(RankedAction { rank: (1, 2, 0), user: Some(user), action: MarkAction::Book(BookAnchors { cancelled: vec![oid], ..BookAnchors::default() }) });
    assert!(s.anchors(&a, &BlockStatuses::default(), TIMING, asset).unwrap().after.cancelled.is_empty(), "a competing native cancel makes attribution ambiguous");
}

#[test]
fn a_competing_corewriter_rejection_cannot_be_assigned_to_a_native_ioc() {
    use crate::accounts::marks::actions::{RejectedIoc, Taker};
    let mut g = golden();
    g.statuses.truncate(1);
    g.statuses[0].status = "iocCancelRejected".into();
    let mut canceled = g.statuses[0].clone();
    canceled.status = "selfTradeCanceled".into();
    canceled.order.oid = 7;
    canceled.order.side = Side::Bid;
    canceled.order.limit_px = "2441".into();
    g.statuses.push(canceled);
    let system = SystemBlock::of(g.system, &g.statuses).unwrap();
    let user = g.statuses[0].user;
    let statuses = BlockStatuses::of(g.statuses).unwrap();
    let mut a = actions(g.time_ns);
    a.actions[0].rank = (1, 3, 0);
    a.actions.push(RankedAction { rank: (0, 3, 0), user: Some(user), action: MarkAction::Book(BookAnchors {
        rejected_iocs: vec![RejectedIoc { taker: Taker { asset: 1, is_buy: false, limit_px: ExactDecimal::parse("2440.5").unwrap() },
            sz: ExactDecimal::parse("0.4172").unwrap(), reduce_only: false, cloid: Some("0x0000000000000000000000000000002c".into()) }],
        ..BookAnchors::default()
    }) });
    let anchors = system.anchors(&a, &statuses, TIMING, asset).unwrap();
    assert_eq!(anchors.before.cancelled, Vec::<u64>::new());
    assert!(anchors.competing_ioc_cancels.contains(&7));
    let diff = BookDiff::of(&serde_json::from_value::<NodeDataOrderDiff>(serde_json::json!({
        "user":user,"coin":"ETH","oid":7,"side":"B","px":"2441","raw_book_diff":"remove"
    })).unwrap()).unwrap();
    let points = price_points_with_system(&a, &statuses, &[diff], &[], &anchors, asset).unwrap();
    assert_eq!((points[0].cut.lo, points[0].cut.hi), (0, 1), "neither competing request owns the cancel");
}

#[test]
fn a_cloid_cancel_with_two_possible_outcomes_remains_ambiguous() {
    let mut g = golden();
    g.statuses.truncate(1);
    g.statuses[0].status = "canceled".into();
    let mut second = g.statuses[0].clone();
    second.order.oid += 1;
    g.statuses.push(second);
    g.system[0].action = Action::CancelByCloid { cancels: vec![CancelByCloid {
        asset: 1, cloid: g.statuses[0].order.cloid.clone().unwrap(),
    }], f: true };
    let s = SystemBlock::of(g.system, &g.statuses).unwrap();
    assert_eq!(s.anchors(&actions(g.time_ns), &BlockStatuses::default(), TIMING, asset).unwrap().after.cancelled, Vec::<u64>::new());
}


#[test]
fn a_rejected_ioc_needs_phase_evidence_only_when_it_removes_orders() {
    for known_grouping in [true, false] {
        let mut g = golden();
        g.statuses.truncate(1);
        g.statuses[0].status = "iocCancelRejected".into();
        if !known_grouping {
            let Action::Order { grouping, .. } = &mut g.system[0].action else { panic!() };
            *grouping = serde_json::json!("unverified-grouping");
        }
        let system = SystemBlock::of(g.system.clone(), &g.statuses).unwrap();
        let statuses = BlockStatuses::of(g.statuses.clone()).unwrap();
        for timing in [None, Some((0.0, 1.0)), TIMING] {
            let anchors = system.anchors(&actions(g.time_ns), &statuses, timing, asset).unwrap();
            assert!(anchors.before.created.is_empty() && anchors.before.cancelled.is_empty());
            assert!(anchors.after.created.is_empty() && anchors.after.cancelled.is_empty());
        }
        let mut canceled = g.statuses[0].clone();
        canceled.status = "selfTradeCanceled".into();
        canceled.order.oid = 7;
        canceled.order.side = Side::Bid;
        canceled.order.limit_px = "2441".into();
        g.statuses.push(canceled);
        let system = SystemBlock::of(g.system, &g.statuses).unwrap();
        let statuses = BlockStatuses::of(g.statuses).unwrap();
        assert!(system.anchors(&actions(g.time_ns), &statuses, None, asset).is_err(), "a real removal still needs its phase");
        let anchors = system.anchors(&actions(g.time_ns), &statuses, TIMING, asset);
        if known_grouping {
            assert_eq!(anchors.unwrap().before.cancelled, [7]);
        } else {
            assert!(anchors.is_err());
        }
    }
}


#[test]
fn unused_nonce_is_not_required_but_order_fields_remain_strict() {
    let mut wire: serde_json::Value = serde_json::from_str(include_str!("core_writer_eth_golden.json")).unwrap();
    let event = &mut wire["system"][0];
    event.as_object_mut().unwrap().remove("nonce");
    assert!(serde_json::from_value::<SystemAction>(event.clone()).is_ok());
    event["action"]["orders"][0].as_object_mut().unwrap().remove("p");
    assert!(serde_json::from_value::<SystemAction>(event.clone()).is_err());
}

#[test]
fn archived_corewriter_cancel_without_flag_precedes_the_price_action() {
    let g: Golden = serde_json::from_str(include_str!("core_writer_cancel_golden.json")).unwrap();
    let system = SystemBlock::of(g.system, &g.statuses).unwrap();
    let statuses = BlockStatuses::of(g.statuses).unwrap();
    let actions = actions(g.time_ns);
    let asset = |coin: &str| (coin == "BTC").then_some(0);
    let anchors = system.anchors(&actions, &statuses, TIMING, asset).unwrap();
    assert_eq!(anchors.before.cancelled, [581_429_565_034]);
    assert_eq!(anchors.before.created, [582_848_552_400]);
    assert!(anchors.after.cancelled.is_empty() && anchors.after.created.is_empty());
    let diffs: Vec<_> = g.diffs.iter().map(|d| BookDiff::of(d).unwrap()).collect();
    let fills: Vec<_> = g.fills.iter().map(BlockFill::of).collect();
    let points = price_points_with_system(&actions, &statuses, &diffs, &fills, &anchors, asset).unwrap();
    assert_eq!((points[0].cut.lo, points[0].cut.hi, points[0].cut.fills_lo), (2, 2, 2));
}

#[test]
fn cancel_wire_omission_means_delayed_but_invalid_flags_are_rejected() {
    let original: serde_json::Value = serde_json::from_str(include_str!("core_writer_cancel_golden.json")).unwrap();
    let g: Golden = serde_json::from_value(original.clone()).unwrap();
    let asset = |coin: &str| (coin == "BTC").then_some(0);
    for kind in ["cancel", "cancelByCloid"] {
        for flag in [None, Some(false), Some(true)] {
            let mut wire = original["system"][0].clone();
            if kind == "cancel" {
                wire["action"] = serde_json::json!({"type":"cancel","cancels":[{"a":0,"o":581_429_565_034_u64}]});
            }
            if let Some(flag) = flag { wire["action"]["f"] = serde_json::json!(flag); }
            let event = serde_json::from_value::<SystemAction>(wire.clone()).unwrap();
            let system = SystemBlock::of([event], &g.statuses).unwrap();
            let anchors = system.anchors(&actions(g.time_ns), &BlockStatuses::default(), TIMING, asset).unwrap();
            assert_eq!(if flag == Some(true) { anchors.after.cancelled } else { anchors.before.cancelled }, [581_429_565_034]);
            if flag != Some(true) {
                assert!(system.anchors(&actions(g.time_ns), &BlockStatuses::default(), None, asset).is_err(), "ordinary cancel still requires verified delay");
            }
            for invalid in [serde_json::Value::Null, serde_json::json!(0), serde_json::json!("false")] {
                wire["action"]["f"] = invalid;
                assert!(serde_json::from_value::<SystemAction>(wire.clone()).is_err(), "{kind}: invalid flag is not omission");
            }
        }
    }
}
