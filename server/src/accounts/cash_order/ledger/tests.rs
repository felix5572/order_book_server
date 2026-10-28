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
        assert!(serde_json::from_value::<SystemAction>(raw).is_err(), "{field}");
    }
}

pub(crate) fn trump_golden() -> Value {
    serde_json::from_str::<Value>(include_str!("trump_signed_send_window.json")).unwrap()["case"].clone()
}

fn address(case: &Value, key: &str) -> Address {
    case[key].as_str().unwrap().parse().unwrap()
}

#[test]
fn trump_signed_send_ranked_before_the_receivers_open_settles_before_fills() {
    let case = trump_golden();
    let (actions, fills, mut misc, inputs) = block(&case["blocks"][0]);
    let send = SpotToMainSend { user: address(&case, "sender"), destination: address(&case, "receiver"), nonce: 1_793_207_566_111, wei: 1_580_031_100 };
    assert_eq!(actions.sends, vec![RankedSend { rank: (0, 3, 0), send }]);
    // No delayed system phase: the signed send's rank alone orders it before the receiver's open a bundle later.
    let moved = PreBundleLedger::of(&actions, &fills, &misc, &inputs, false).take(&mut misc, true);
    assert_eq!(moved.iter().flat_map(Batch::events_ref).count(), 1);
    assert_eq!(misc.iter().flat_map(Batch::events_ref).count(), 0);
    assert!(PreBundleLedger::of(&actions, &fills, &misc, &inputs, false).take(&mut misc, true).is_empty());
}

/// The golden's signed `sendAsset` payload.
fn send_action(raw: &mut Value) -> &mut Value {
    &mut raw["replica"]["abci_block"]["signed_action_bundles"][0][1]["signed_actions"][0]["action"]
}

#[test]
fn trump_send_without_a_proven_earlier_rank_keeps_the_estimate() {
    for variant in 0..7 {
        let mut case = trump_golden();
        let raw = &mut case["blocks"][0];
        match variant {
            // the send a bundle after the open
            0 => for list in ["/abci_block/signed_action_bundles", "/resps/Full"] {
                raw["replica"].pointer_mut(list).unwrap().as_array_mut().unwrap().swap(0, 1);
            },
            1 => raw["replica"]["resps"]["Full"][0][1][0]["res"] = json!({"status": "err", "response": "Insufficient balance for token transfer"}),
            2 => send_action(raw)["nonce"] = 1.into(),
            3 => send_action(raw)["amount"] = "15.8".into(),
            4 => send_action(raw)["destination"] = "0x0000000000000000000000000000000000000001".into(),
            5 => send_action(raw)["destinationDex"] = "xyz".into(),
            _ => { let ledger = raw["misc"][0].clone(); raw["misc"].as_array_mut().unwrap().push(ledger); },
        }
        let (actions, fills, mut misc, inputs) = block(raw);
        assert!(PreBundleLedger::of(&actions, &fills, &misc, &inputs, false).take(&mut misc, true).is_empty(), "variant {variant}");
    }
}

#[test]
fn trump_send_needs_trusted_ranked_executions() {
    let (actions, fills, mut misc, mut inputs) = block(&trump_golden()["blocks"][0]);
    assert!(PreBundleLedger::of(&actions, &fills, &misc, &inputs, false).take(&mut misc, false).is_empty());
    inputs.mark_actions = Err("missing receipt".into());
    assert!(PreBundleLedger::of(&actions, &fills, &misc, &inputs, false).take(&mut misc, true).is_empty());
}

#[test]
fn signed_send_fields_that_do_not_read_break_the_block_contract() {
    for field in ["token", "sourceDex", "destinationDex", "fromSubAccount", "amount", "nonce", "destination"] {
        let mut raw = trump_golden()["blocks"][0]["replica"].clone();
        raw["abci_block"]["signed_action_bundles"][0][1]["signed_actions"][0]["action"].as_object_mut().unwrap().remove(field);
        assert!(parse_block_actions(&raw.to_string()).is_err(), "{field}");
    }
}
