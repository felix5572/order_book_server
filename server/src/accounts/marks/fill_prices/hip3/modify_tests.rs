use super::*;
use crate::accounts::marks::{
    actions::{RankedAction, tests::{mark_actions, replica_line}}, seed::StoredMark,
    system::{SystemAction, SystemBlock},
};
use crate::types::node_data::{NodeDataFill, NodeDataOrderStatus};
use serde_json::Value;

fn golden() -> Value { serde_json::from_str(include_str!("modify_golden.json")).unwrap() }

fn inputs(g: &Value, systems: Vec<SystemAction>) -> (FxHashMap<String, Hip3Coin>, BlockInputs) {
    let height = g["height"].as_u64().unwrap();
    let time_ns = g["time_ns"].as_u64().unwrap();
    let coin = g["coin"].as_str().unwrap();
    let decimals = u32::try_from(g["size_decimals"].as_u64().unwrap()).unwrap();
    let mut actions = mark_actions(&replica_line(height, &[&[(&g["action"].to_string(), &g["response"].to_string())]]));
    actions.time_ns = time_ns;
    for action in &mut actions.actions { action.rank = serde_json::from_value(g["modify_rank"].clone()).unwrap(); }
    actions.actions.push(RankedAction { rank: serde_json::from_value(g["oracle_rank"].clone()).unwrap(), user: None,
        action: MarkAction::Hip3Oracle { dex: coin.split_once(':').unwrap().0.into() } });
    assert!(actions.actions.iter().all(|a| !a.action.is_price()), "system competition must work without a native price action");
    let statuses: Vec<NodeDataOrderStatus> = serde_json::from_value(g["statuses"].clone()).unwrap();
    let system = SystemBlock::of(systems, &statuses).unwrap();
    let coins = FxHashMap::from_iter([(coin.into(), Hip3Coin {
        order_asset: u32::try_from(g["order_asset"].as_u64().unwrap()).unwrap(),
        size_decimals: decimals, oracle: None,
        mark: Some(StoredMark { px: exact_price_units(g["before_mark"].as_str().unwrap(), decimals).unwrap(), time_ns: time_ns - 1 }),
    })]);
    let inputs = BlockInputs { fills: g["legs"].as_array().unwrap().iter().map(|leg|
        BlockFill::of(&serde_json::from_value::<NodeDataFill>(leg.clone()).unwrap())).collect(),
        hip3: vec![Hip3PriceUpdate { normal: true, oracle: false, coin: coin.into(),
            px: g["after_mark"].as_str().unwrap().into(), time_ns }],
        actions: Some(actions), statuses: BlockStatuses::of(statuses).unwrap(), system, ..BlockInputs::default() };
    (coins, inputs)
}

fn assert_range(coins: &FxHashMap<String, Hip3Coin>, inputs: &BlockInputs, low: u64, high: u64) {
    let mut builder = super::super::FillPriceBuilder::new(&inputs.fills);
    builder.hip3(coins, inputs);
    let height = inputs.actions.as_ref().unwrap().height;
    let prices = builder.finish(height, &inputs.fills);
    for fill in &inputs.fills {
        let px = prices.get(height, &fill.coin, fill.tid).unwrap();
        assert_eq!((px.min_px_raw, px.max_px_raw, px.size_decimals), (low, high, 3));
    }
}

#[test]
fn real_orcl_default_modify_places_both_fill_legs_before_set_oracle() {
    let g = golden(); let (coins, inputs) = inputs(&g, Vec::new());
    assert_range(&coins, &inputs, 124_520, 124_520);
    let mut after = g; after["modify_rank"] = serde_json::json!([2, 3, 725]);
    let (coins, inputs) = self::inputs(&after, Vec::new());
    assert_range(&coins, &inputs, 124_570, 124_570);
}

#[test]
fn mismatched_modify_identity_or_status_adjacency_keeps_the_price_range() {
    let changes: [fn(&mut Value); 7] = [
        |g| g["order_asset"] = 110_012.into(),
        |g| g["action"]["oid"] = 1.into(),
        |g| g["action"]["order"]["s"] = "0.26".into(),
        |g| g["statuses"][0]["order"]["timestamp"] = 0.into(),
        |g| g["statuses"].as_array_mut().unwrap().swap(1, 2),
        |g| g["response"]["res"] = serde_json::json!({"status":"err","response":"cannot modify"}),
        |g| g["response"]["res"]["response"]["type"] = "futureResponse".into(),
    ];
    for change in changes {
        let mut g = golden(); change(&mut g);
        let (coins, inputs) = inputs(&g, Vec::new());
        assert_range(&coins, &inputs, 124_520, 124_570);
    }
    let (coins, mut inputs) = inputs(&golden(), Vec::new());
    let actions = inputs.actions.as_mut().unwrap();
    actions.actions.push(actions.actions[0].clone());
    assert_range(&coins, &inputs, 124_520, 124_570);
}

#[test]
fn corewriter_competing_for_the_same_open_prevents_default_modify_attribution() {
    let g = golden();
    let system = serde_json::json!({"user":g["response"]["user"], "evm_tx_hash":format!("0x{}", "01".repeat(32)),
        "action":{"type":"order", "orders":[g["action"]["order"]], "grouping":"na"}});
    let (coins, block) = inputs(&g, vec![serde_json::from_value(system.clone()).unwrap()]);
    assert_range(&coins, &block, 124_520, 124_570);
    let mut unrelated = system; unrelated["action"]["orders"][0]["a"] = 110_012.into();
    let (coins, block) = inputs(&g, vec![serde_json::from_value(unrelated).unwrap()]);
    assert_range(&coins, &block, 124_520, 124_520);
}

#[test]
fn a_modified_order_that_triggers_later_does_not_inherit_its_modification_rank() {
    let mut g = golden();
    let mut trigger = g["statuses"][2].clone(); trigger["status"] = "triggered".into();
    g["statuses"].as_array_mut().unwrap().push(trigger);
    let (coins, block) = inputs(&g, Vec::new());
    assert_range(&coins, &block, 124_520, 124_570);
}
