//! Backstop takeovers in the cash replay, on the bm 2026-10-29 goldens (`backstop_goldens.json`). The accounts' cash
//! before the takeover is synthetic: the pool value the record states at the fills' marks (the checkpoints around
//! these blocks were not kept), so what is exact here is the rule's arithmetic, not a node balance.

use serde_json::{Value, json};

use super::{tests::states, *};
use crate::{
    accounts::{
        backstop::Takeovers,
        checkpoint::tests::user,
        misc::MiscEvent,
    },
    types::node_data::{Batch, NodeDataFill},
};

pub(crate) const ISOLATED_USER: &str = "0xba1e2daed5777184e779312711b4b5acb2cf2710";
const CROSS_USER: &str = "0x937613997fb4119e563baee0f2122b753bb2633f";
const LIQUIDATOR: &str = "0x4000000000000000000000000000000000000001";
/// xyz:AMZN's index in the fixture's xyz universe (`xyz:TSLA` keeps 0, xyz:INTC is 2; asset ids 10000 + index).
const AMZN: usize = 1;
const XYZ: usize = 1;

pub(crate) fn golden(index: usize) -> Value {
    let golden: Value = serde_json::from_str(include_str!("../backstop_goldens.json")).unwrap();
    golden["cases"][index].clone()
}

/// The case's misc line.
pub(crate) fn misc_batch(case: &Value) -> Batch<MiscEvent> {
    serde_json::from_value(case["misc"].clone()).unwrap()
}

fn misc(case: &Value) -> Vec<MiscEvent> {
    misc_batch(case).events()
}

/// The case's fills lines, as the fills stream delivers them.
pub(crate) fn fill_batches(case: &Value) -> Vec<Batch<NodeDataFill>> {
    case["fills"].as_array().unwrap().iter().map(|line| serde_json::from_value(line.clone()).unwrap()).collect()
}

fn fills(case: &Value) -> Vec<NodeDataFill> {
    fill_batches(case).into_iter().flat_map(Batch::events).collect()
}

/// The fixture with xyz:AMZN and xyz:INTC listed and the golden accounts' xyz pools at the records' values:
/// the isolated AMZN 2.776 holding 10.044596 at mark 241.76, the cross INTC 12.28 and AMZN 3.649 holding 48.901984
/// at marks 89.122 and 233.28 (micro USDC: cash = value − size × mark), the liquidator with a cross AMZN position.
pub(crate) fn replay(cross_user_mode: Value, cross_amzn_isolated: bool) -> MarginStates {
    states(|checkpoint| {
        let locus = &mut checkpoint["exchange"]["locus"];
        locus["cls"][1]["meta"]["universe"].as_array_mut().unwrap().extend([
            json!({"szDecimals": 3, "name": "xyz:AMZN", "marginTableId": 10, "marginMode": "normal"}),
            json!({"szDecimals": 2, "name": "xyz:INTC", "marginTableId": 10, "marginMode": "normal"}),
        ]);
        // the oracle's (oracle, mark, external) per asset, at the takeovers' marks
        let prices = |raw: i64| {
            let price = json!({"u": "2026-10-19T06:37:22", "p": raw, "d": 1});
            json!([price.clone(), price.clone(), price])
        };
        locus["cls"][1]["oracle"]["pxs2"].as_array_mut().unwrap().extend([prices(241_760), prices(891_220)]);
        let amzn_margin = if cross_amzn_isolated { json!({"I": {"l": 10, "u": -851_238_720_i64}}) } else { json!({"C": 10}) };
        let cross_cash = if cross_amzn_isolated { -1_045_516_176_i64 } else { -1_896_754_896_i64 };
        locus["cls"][1]["user_states"]["user_to_state"].as_array_mut().unwrap().extend([
            json!([ISOLATED_USER, {"u": 0, "p": {"p": [[10_001, {"s": 2776, "l": {"I": {"l": 20, "u": -661_081_164_i64}}, "M": 10}]]}}]),
            json!([CROSS_USER, {"u": cross_cash, "p": {"p": [
                [10_002, {"s": 1228, "l": {"C": 10}, "M": 10}],
                [10_001, {"s": 3649, "l": amzn_margin, "M": 10}]
            ]}}]),
            json!([LIQUIDATOR, {"u": 1_000_000_000_i64, "p": {"p": [[10_001, {"s": 1_000_000, "l": {"C": 10}, "M": 10}]]}}]),
        ]);
        locus["ust"].as_array_mut().unwrap().extend([
            json!([ISOLATED_USER, {"s": "v"}]), json!([CROSS_USER, cross_user_mode]), json!([LIQUIDATOR, {"s": "v"}]),
        ]);
    })
}

/// The golden block applied as the book does: fills (checked against the block's records), then misc events.
fn apply_block(states: &mut MarginStates, case: &Value) {
    let (events, fills) = (misc(case), fills(case));
    let takeovers = Takeovers::of(events.iter(), fills.iter());
    let block = case["block"].as_u64().unwrap();
    for NodeDataFill(user, fill) in &fills {
        states.apply(block, *user, &AccountEvent::Fill(FillEvent::in_block(fill, *user, None, &takeovers).unwrap()));
    }
    for event in events {
        let MiscEvent::Ledger { users, delta } = event else { panic!() };
        for user in users {
            states.apply(block, user, &AccountEvent::Ledger(delta.clone()));
        }
    }
}

fn account<'s>(states: &'s MarginStates, address: &str) -> &'s AccountMargin {
    &states.accounts[&user(address)]
}

fn reason(states: &MarginStates, address: &str) -> Option<StopReason> {
    account(states, address).stop.as_ref().map(|stop| stop.reason.clone())
}

/// The takeover price leaves the isolated pool −0.001748 USDC: 2.776 × (238.141630… − 238.141), the rounding the
/// liquidator's way, which the close moves to cross like any isolated close. The record then reads.
#[test]
fn backstop_takeover_of_an_isolated_position_is_its_close_at_the_rule_price() {
    let mut states = replay(json!({"s": "v"}), false);
    apply_block(&mut states, &golden(0));
    let liquidated = account(&states, ISOLATED_USER);
    assert_eq!(liquidated.stop, None);
    assert_eq!(liquidated.isolated.get(&(XYZ, AMZN)), None);
    assert_eq!(liquidated.pools, Pools::PerDex(std::iter::once((XYZ, -174_800)).collect()));
    assert!(liquidated.estimated, "where the rounding goes is not measured");
    assert_eq!(reason(&states, LIQUIDATOR), Some(StopReason::BackstopLiquidation("liquidator".to_string())));
}

/// Both cross positions at the rule's prices leave the xyz pool −0.002952 USDC.
#[test]
fn backstop_takeover_of_a_cross_pool_takes_every_position_at_the_rule_price() {
    let mut states = replay(json!({"s": "v"}), false);
    apply_block(&mut states, &golden(2));
    let liquidated = account(&states, CROSS_USER);
    assert_eq!(liquidated.stop, None);
    assert_eq!(liquidated.pools, Pools::PerDex(std::iter::once((XYZ, -295_200)).collect()));
    assert!(liquidated.estimated);
}

/// A unified account's cross pool, a pool the replay holds otherwise than the record, or a record without its
/// takeover in the block is not read.
#[test]
fn backstop_takeovers_no_measured_rule_covers_stop_the_account() {
    let mut unified = replay(json!({"a": "u"}), false);
    apply_block(&mut unified, &golden(2));
    assert_eq!(reason(&unified, CROSS_USER), Some(StopReason::BackstopLiquidation("unified cross pool".to_string())));

    let mut isolated_amzn = replay(json!({"s": "v"}), true);
    apply_block(&mut isolated_amzn, &golden(2));
    assert!(matches!(reason(&isolated_amzn, CROSS_USER), Some(StopReason::Contradiction(why)) if why.contains("a Cross takeover of a position the replay holds isolated")));

    let mut without_fills = replay(json!({"s": "v"}), false);
    let Some(MiscEvent::Ledger { delta, .. }) = misc(&golden(2)).pop() else { panic!() };
    without_fills.apply(golden(2)["block"].as_u64().unwrap(), user(CROSS_USER), &AccountEvent::Ledger(delta));
    assert_eq!(reason(&without_fills, CROSS_USER),
        Some(StopReason::Contradiction("a liquidation record without a takeover fill in its block".to_string())));

    // the same fills without their block's record
    let mut unrecorded = replay(json!({"s": "v"}), false);
    let case = golden(0);
    let fills = fills(&case);
    let takeovers = Takeovers::of(std::iter::empty(), fills.iter());
    for NodeDataFill(user, fill) in &fills {
        unrecorded.apply(case["block"].as_u64().unwrap(), *user, &AccountEvent::Fill(FillEvent::in_block(fill, *user, None, &takeovers).unwrap()));
    }
    assert_eq!(reason(&unrecorded, ISOLATED_USER), Some(StopReason::BackstopLiquidation("0 takeover records in the block".to_string())));
}
