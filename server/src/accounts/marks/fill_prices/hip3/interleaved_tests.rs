use super::*;
use crate::{accounts::marks::{actions::tests::{mark_actions, replica_line}, seed::StoredMark}, types::node_data::NodeDataFill};
use serde_json::Value;

fn golden() -> Value { serde_json::from_str(include_str!("interleaved_golden.json")).unwrap() }

pub(crate) fn interleaved_golden_prices(event: &Value) -> super::super::FillMarkPrices {
    let height = event["height"].as_u64().unwrap();
    let time_ns = event["time_ns"].as_u64().unwrap();
    let coin = event["coin"].as_str().unwrap();
    let decimals = u32::try_from(event["size_decimals"].as_u64().unwrap()).unwrap();
    let mut actions = MarkActions { height, time_ns, actions: Vec::new() };
    for item in event["actions"].as_array().unwrap() {
        let mut parsed = mark_actions(&replica_line(height, &[&[(&item["action"].to_string(), &item["response"].to_string())]]));
        for action in &mut parsed.actions { action.rank = serde_json::from_value(item["rank"].clone()).unwrap(); }
        actions.actions.extend(parsed.actions);
    }
    let coins = FxHashMap::from_iter([(coin.into(), Hip3Coin { external: None, last_trade: None,
        order_asset: u32::try_from(event["order_asset"].as_u64().unwrap()).unwrap(), size_decimals: decimals, oracle: None, oracle_time_ns: None, relayer_gap: Err(crate::accounts::marks::seed::NoRelayerGap::SinceCheckpoint),
        mark: Some(StoredMark { px: exact_price_units(event["before_mark"].as_str().unwrap(), decimals).unwrap(), time_ns: time_ns - 1 }),
    })]);
    let fills: Vec<_> = event["legs"].as_array().unwrap().iter()
        .map(|v| BlockFill::of(&serde_json::from_value::<NodeDataFill>(v.clone()).unwrap())).collect();
    let inputs = BlockInputs { fills: fills.clone(), actions: Some(actions),
        statuses: BlockStatuses::of(serde_json::from_value::<Vec<_>>(event["statuses"].clone()).unwrap()).unwrap(),
        hip3: vec![Hip3PriceUpdate { coin: coin.into(), px: event["after_mark"].as_str().unwrap().into(), normal: true, kind: Hip3PriceKind::Mark, time_ns }],
        ..BlockInputs::default() };
    let mut builder = super::super::FillPriceBuilder::new(&fills);
    builder.hip3(&coins, &inputs);
    builder.finish(height, &fills)
}

fn assert_range(event: &Value, low: u64, high: u64) {
    let out = interleaved_golden_prices(event);
    for leg in event["legs"].as_array().unwrap() {
        let price = out.get(event["height"].as_u64().unwrap(), event["coin"].as_str().unwrap(), leg[1]["tid"].as_u64().unwrap()).unwrap();
        assert_eq!((price.min_px_raw, price.max_px_raw), (low, high));
    }
}

#[test]
fn real_dram_trigger_between_same_batch_orders_uses_execution_mark() {
    let event = golden()["event"].clone();
    assert_range(&event, 461_190, 461_190);
    let mut before = event.clone(); before["actions"][0]["rank"] = serde_json::json!([2, 3, 0]);
    assert_range(&before, 461_120, 461_120);
    let mut equal = event; equal["actions"][0]["rank"] = equal["actions"][1]["rank"].clone();
    assert_range(&equal, 461_120, 461_190);
}

#[test]
fn incomplete_or_competing_status_brackets_keep_both_prices() {
    let changes: [fn(&mut Value); 10] = [
        |e| { e["statuses"].as_array_mut().unwrap().remove(0); },
        |e| { e["statuses"].as_array_mut().unwrap().pop(); },
        |e| { e["statuses"].as_array_mut().unwrap().remove(4); },
        |e| e["statuses"][0]["user"] = "0x0000000000000000000000000000000000000001".into(),
        |e| e["statuses"][5]["order"]["timestamp"] = 0.into(),
        |e| { let dup = e["statuses"][1].clone(); e["statuses"].as_array_mut().unwrap().insert(2, dup); },
        |e| { let dup = e["statuses"][4].clone(); e["statuses"].as_array_mut().unwrap().insert(5, dup); },
        |e| { let dup = e["statuses"][0].clone(); e["statuses"].as_array_mut().unwrap().insert(0, dup); },
        |e| { let dup = e["actions"][1].clone(); e["actions"].as_array_mut().unwrap().push(dup); },
        |e| e["statuses"].as_array_mut().unwrap().swap(4, 5),
    ];
    for change in changes {
        let mut event = golden()["event"].clone(); change(&mut event);
        assert_range(&event, 461_120, 461_190);
    }
}

#[test]
fn opens_from_different_actions_do_not_claim_the_trigger() {
    let mut event = golden()["event"].clone();
    let mut later = event["actions"][1].clone(); later["rank"] = serde_json::json!([2, 1, 0]);
    let responses = event["actions"][1]["response"]["res"]["response"]["data"]["statuses"].as_array_mut().unwrap();
    let after = responses.remove(1);
    later["response"]["res"]["response"]["data"]["statuses"] = serde_json::json!([after]);
    // Request/receipt lengths are irrelevant to created oid ownership; no extra IOC evidence is inferred.
    event["actions"].as_array_mut().unwrap().push(later);
    assert_range(&event, 461_120, 461_190);
}
