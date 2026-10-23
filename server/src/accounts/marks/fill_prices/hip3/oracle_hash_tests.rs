use super::*;
use crate::{accounts::marks::{actions::tests::{mark_actions, replica_line}, seed::StoredMark}, types::node_data::NodeDataFill};
use serde_json::Value;

fn golden() -> Value { serde_json::from_str(include_str!("oracle_hash_goldens.json")).unwrap() }

struct Fixture {
    coins: FxHashMap<String, Hip3Coin>,
    inputs: BlockInputs,
    height: u64,
}

impl Fixture {
    fn of(event: &Value) -> Self {
        let height = event["height"].as_u64().unwrap();
        let time_ns = event["time_ns"].as_u64().unwrap();
        let coin = event["coin"].as_str().unwrap();
        let decimals = u32::try_from(event["size_decimals"].as_u64().unwrap()).unwrap();
        let mut actions = mark_actions(&replica_line(height, &[&[(&event["action"].to_string(), &event["response"].to_string())]]));
        actions.time_ns = time_ns;
        // Compact fixture projects these two positions from the archived full block. The ignored archive
        // check below separately verifies that the production parser derives both from the complete wire.
        for action in &mut actions.actions {
            action.rank = serde_json::from_value(event["rank"].clone()).unwrap();
            if let MarkAction::Hip3Oracle { execution_index, .. } = &mut action.action {
                *execution_index = Some(usize::try_from(event["execution_index"].as_u64().unwrap()).unwrap());
            }
        }
        Self {
            height,
            coins: FxHashMap::from_iter([(coin.into(), Hip3Coin {
                order_asset: u32::try_from(event["order_asset"].as_u64().unwrap()).unwrap(), size_decimals: decimals, oracle: None,
                mark: Some(StoredMark { px: exact_price_units(event["before_mark"].as_str().unwrap(), decimals).unwrap(), time_ns: time_ns - 1 }),
            })]),
            inputs: BlockInputs {
                fills: event["legs"].as_array().unwrap().iter().map(|v| BlockFill::of(&serde_json::from_value::<NodeDataFill>(v.clone()).unwrap())).collect(),
                actions: Some(actions),
                statuses: BlockStatuses::of(serde_json::from_value::<Vec<_>>(event["statuses"].clone()).unwrap()).unwrap(),
                hip3: vec![Hip3PriceUpdate { coin: coin.into(), px: event["after_mark"].as_str().unwrap().into(), normal: true, oracle: false, time_ns }],
                ..BlockInputs::default()
            },
        }
    }

    fn prices(&self) -> super::super::super::FillMarkPrices {
        let mut builder = super::super::super::FillPriceBuilder::new(&self.inputs.fills);
        builder.hip3(&self.coins, &self.inputs);
        builder.finish(self.height, &self.inputs.fills)
    }

    fn assert_range(&self, event: &Value, exact: bool) {
        let decimals = self.coins[event["coin"].as_str().unwrap()].size_decimals;
        let first = exact_price_units(event["before_mark"].as_str().unwrap(), decimals).unwrap();
        let last = exact_price_units(event["after_mark"].as_str().unwrap(), decimals).unwrap();
        let expected = if exact { (last, last) } else { (first.min(last), first.max(last)) };
        let prices = self.prices();
        for fill in &self.inputs.fills {
            let observed = prices.get(self.height, &fill.coin, fill.tid).unwrap();
            assert_eq!((observed.min_px_raw, observed.max_px_raw), expected);
        }
    }
}

pub(crate) fn oracle_golden_prices(event: &Value) -> super::super::super::FillMarkPrices { Fixture::of(event).prices() }

#[test]
fn already_eligible_triggers_use_the_successful_oracle_hash_execution_locator() {
    for case in golden()["cases"].as_array().unwrap() {
        let event = &case["oracle_event"];
        Fixture::of(event).assert_range(event, true);
    }
}

#[test]
fn missing_or_conflicting_locator_and_oracle_evidence_keep_the_range() {
    let changes: [fn(&mut Fixture); 9] = [
        |f| f.inputs.fills[0].action_position = None,
        |f| f.inputs.fills[0].action_position.as_mut().unwrap().height += 1,
        |f| f.inputs.fills[0].action_position.as_mut().unwrap().index += 1,
        |f| f.inputs.statuses.triggered.clear(),
        |f| f.inputs.actions.as_mut().unwrap().actions.clear(),
        |f| { let a = f.inputs.actions.as_ref().unwrap().actions[0].clone(); f.inputs.actions.as_mut().unwrap().actions.push(a); },
        |f| { let u = f.inputs.hip3[0].clone(); f.inputs.hip3.push(u); },
        |f| f.inputs.hip3[0].normal = false,
        |f| f.inputs.hip3[0].time_ns += 1,
    ];
    for case in golden()["cases"].as_array().unwrap() {
        let event = &case["oracle_event"];
        for change in changes { let mut f = Fixture::of(event); change(&mut f); f.assert_range(event, false); }
        let mut refused = event.clone();
        refused["response"]["res"] = serde_json::json!({"status":"err","response":"business refusal"});
        Fixture::of(&refused).assert_range(event, false);
    }
}

#[test]
#[ignore = "requires HIP3_ORACLE_BLOCKS containing the two archived uncompressed replica lines"]
fn full_archived_blocks_derive_oracle_execution_positions_without_projection() {
    let directory = std::path::PathBuf::from(std::env::var("HIP3_ORACLE_BLOCKS").expect("explicit archive directory"));
    for case in golden()["cases"].as_array().unwrap() {
        let event = &case["oracle_event"];
        let mut fixture = Fixture::of(event);
        let line = std::fs::read_to_string(directory.join(format!("{}_replica.jsonl", fixture.height))).unwrap();
        let parsed = mark_actions(&line);
        let oracle = parsed.actions.iter().find(|a| matches!(&a.action, MarkAction::Hip3Oracle { dex, .. } if dex == "io")).unwrap();
        assert_eq!(oracle.rank, serde_json::from_value(event["rank"].clone()).unwrap());
        assert_eq!(oracle.action, MarkAction::Hip3Oracle { dex: "io".into(), execution_index: Some(usize::try_from(event["execution_index"].as_u64().unwrap()).unwrap()) });
        fixture.inputs.actions = Some(parsed);
        fixture.assert_range(event, true);
    }
}

#[test]
fn conflicting_status_bracket_cannot_override_a_matched_oracle_locator() {
    let oracle = (1, 3, 20);
    assert!(matches!(trigger_timing(Placement::Trigger(1), Some(oracle), Some(oracle)), Some(FillTiming::InsideOracle)));
    assert!(trigger_timing(Placement::Trigger(1), Some((1, 3, 21)), Some(oracle)).is_none());
    assert!(matches!(trigger_timing(Placement::Trigger(1), Some((1, 3, 21)), None), Some(FillTiming::Placed(Placement::Action((1, 3, 21))))));
}
