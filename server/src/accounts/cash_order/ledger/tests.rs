use super::*;
use crate::accounts::{actions::parse_block_actions, marks::{MarkInputs, actions::MarkActions, execution::BlockFill}, replica::ReplicaLine};
use serde_json::{Value, json};

pub(crate) fn golden() -> Value {
    serde_json::from_str::<Value>(include_str!("eur_system_send_window.json")).unwrap()["case"].clone()
}

pub(crate) fn block(raw: &Value) -> (BlockActions, Vec<Batch<NodeDataFill>>, Vec<Batch<MiscEvent>>, MarkInputs) {
    let line = raw["replica"].to_string();
    let replica = ReplicaLine::parse(&line).unwrap();
    let actions = parse_block_actions(&line).unwrap();
    let fills: Vec<Batch<NodeDataFill>> = serde_json::from_value(raw["fills"].clone()).unwrap();
    let misc = serde_json::from_value(raw["misc"].clone()).unwrap();
    let inputs = MarkInputs { fills: fills.iter().flat_map(Batch::events_ref).map(BlockFill::of).collect(),
        mark_actions: MarkActions::of(&replica), order_statuses: serde_json::from_value(raw["statuses"].clone()).unwrap(),
        system_actions: serde_json::from_value(raw["system"].clone()).unwrap(), ..MarkInputs::default() };
    (actions, fills, misc, inputs)
}

#[test]
fn successful_eur_send_moves_once_and_never_uses_attempt_as_cash() {
    let case = golden();
    let (actions, fills, mut misc, inputs) = block(&case["blocks"][0]);
    let plan = PreBundleLedger::of(&actions, &fills, &misc, &inputs, true);
    let moved = plan.take(&mut misc, true);
    assert_eq!(moved.iter().flat_map(Batch::events_ref).count(), 1);
    assert_eq!(misc.iter().flat_map(Batch::events_ref).count(), 1);
    assert!(PreBundleLedger::of(&actions, &fills, &misc, &inputs, true).take(&mut misc, true).is_empty());
    let send = confirmed_send(&moved[0].events_ref()[0]).unwrap();
    assert_eq!((send.nonce, send.wei), (4_464_363, 858_980_000_000));
}

#[test]
fn missing_timing_or_untrusted_inputs_preserve_the_original_event() {
    for (delay, trusted) in [(false, true), (true, false)] {
        let (actions, fills, mut misc, inputs) = block(&golden()["blocks"][0]);
        assert!(PreBundleLedger::of(&actions, &fills, &misc, &inputs, delay).take(&mut misc, trusted).is_empty());
        assert_eq!(misc.iter().flat_map(Batch::events_ref).count(), 2);
    }
}

#[test]
fn ambiguous_or_mismatching_system_and_ledger_evidence_is_not_reordered() {
    for variant in 0..14 {
        let mut case = golden();
        let raw = &mut case["blocks"][0];
        match variant {
            0 => raw["system"] = json!([]),
            1 => raw["system"][0]["events"][0]["nonce"] = 1.into(),
            2 => raw["system"][0]["events"][0]["action"]["wei"] = 1.into(),
            3 => raw["system"][0]["events"][0]["evm_tx_hash"] = Value::Null,
            4 => raw["system"][0]["events"][0]["action"]["token"] = 1.into(),
            5 => raw["system"][0]["events"][0]["action"]["sourceDexOrSpot"] = 0.into(),
            6 => raw["system"][0]["events"][0]["action"]["destinationDexOrSpot"] = Value::Null,
            7 => { let v = raw["system"][0]["events"][0].clone(); raw["system"][0]["events"].as_array_mut().unwrap().push(v); },
            8 => { let v = raw["misc"][0].clone(); raw["misc"].as_array_mut().unwrap().push(v); },
            9 => { raw["misc"][0]["events"][0]["inner"]["LedgerUpdate"]["delta"].as_object_mut().unwrap().remove("nonce"); },
            10 => raw["misc"][0]["events"][0]["inner"]["LedgerUpdate"]["users"] = json!(["0xb20a59ed4d31f1fb86f8f12d7806bae80ac1a907"]),
            11 => raw["system"][0]["events"][0]["action"]["destination"] = "0x0000000000000000000000000000000000000001".into(),
            12 => raw["system"][0]["events"][0]["action"]["fromSubAccount"] = "0x0000000000000000000000000000000000000001".into(),
            _ => { raw["misc"][0]["events"][0]["inner"]["LedgerUpdate"]["delta"]["fee"] = "1.0".into(); raw["misc"][0]["events"][0]["inner"]["LedgerUpdate"]["delta"]["feeToken"] = "USDC".into(); },
        }
        let (actions, fills, mut misc, inputs) = block(&case["blocks"][0]);
        assert!(PreBundleLedger::of(&actions, &fills, &misc, &inputs, true).take(&mut misc, true).is_empty(), "variant {variant}");
    }
}

#[test]
fn competing_cash_or_nonordinary_executions_keep_the_estimate() {
    for variant in 0..4 {
        let case = golden();
        let (mut actions, fills, mut misc, mut inputs) = block(&case["blocks"][0]);
        match variant {
            0 => inputs.mark_actions = Err("missing receipt".into()),
            1 => inputs.fills.clear(),
            2 => for fill in &mut inputs.fills { fill.twap = true; },
            _ => actions.actions.push((crate::accounts::actions::BlockAction::ReserveRequestWeight { cost_wei: 1 }, case["user"].as_str().unwrap().parse().unwrap())),
        }
        assert!(PreBundleLedger::of(&actions, &fills, &misc, &inputs, true).take(&mut misc, true).is_empty(), "variant {variant}");
    }
}

#[test]
fn missing_nullable_route_fields_cannot_claim_spot_to_main() {
    for field in ["fromSubAccount", "sourceDexOrSpot", "destinationDexOrSpot"] {
        let mut raw = golden()["blocks"][0]["system"][0]["events"][0].clone();
        raw["action"].as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<crate::accounts::marks::system::SystemAction>(raw).is_err(), "{field}");
    }
}
