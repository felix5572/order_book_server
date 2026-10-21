use super::*;
use super::super::{BlockFill, BlockStatuses, price_points};
use super::super::super::{actions::{RankedAction, tests::{mark_actions, replica_line, ok}}, book::BookDiff, system::{SystemAction, SystemBlock}};
use crate::types::node_data::{NodeDataFill, NodeDataOrderDiff};
use serde::Deserialize;

#[derive(Deserialize)]
struct Golden { action: serde_json::Value, response: serde_json::Value, statuses: Vec<NodeDataOrderStatus>, diffs: Vec<NodeDataOrderDiff>, fills: Vec<NodeDataFill> }
fn golden() -> Golden { serde_json::from_str(include_str!("golden.json")).unwrap() }
fn actions(g: &Golden) -> MarkActions {
    let mut a = mark_actions(&replica_line(1_191_962_420, &[&[(&g.action.to_string(), &g.response.to_string())]]));
    a.time_ns = 1_792_588_100_329_326_281;
    a.actions[0].rank = (0, 3, 624);
    a.actions.push(RankedAction { rank: (1, 3, 0), user: None, action: MarkAction::SetGlobal(Vec::new()) });
    a
}

#[test]
fn archived_vault_withdrawal_closes_precede_the_next_bundles_price_action() {
    let g = golden(); let a = actions(&g);
    assert_ne!(a.actions[0].user, Some(g.statuses[0].user), "receipt user is depositor, not vault");
    let s = BlockStatuses::of(g.statuses).unwrap();
    let diffs: Vec<_> = g.diffs.iter().map(|d| BookDiff::of(d).unwrap()).collect();
    let fills: Vec<_> = g.fills.iter().map(BlockFill::of).collect();
    assert_eq!(s.vault_closes.len(), 16);
    let points = price_points(&a, &s, &diffs, &fills, |_| None).unwrap();
    assert_eq!(points.len(), 1, "withdrawal is not a price action");
    assert_eq!((points[0].cut.lo, points[0].cut.hi, points[0].cut.fills_lo), (52, 52, 68));
    // Same-bundle ordering is category then proposal position, not unconditional block-start execution.
    let mut later = a.clone(); later.actions[0].rank = (1, 3, 1);
    let points = price_points(&later, &s, &diffs, &fills, |_| None).unwrap();
    assert_eq!((points[0].cut.lo, points[0].cut.hi, points[0].cut.fills_lo), (0, 0, 0));
}

#[test]
fn only_a_unique_withdrawal_of_the_same_vault_and_creation_time_is_evidence() {
    let g = golden(); let a = actions(&g); let s = BlockStatuses::of(g.statuses.clone()).unwrap();
    let attributed = |a: &MarkActions| s.vault_closes.attributed(a, &FxHashSet::default());
    assert_eq!(attributed(&a).len(), 16);
    let mut duplicate = a.clone(); duplicate.actions.push(a.actions[0].clone());
    assert!(attributed(&duplicate).is_empty());
    let mut missing = a.clone(); missing.actions.remove(0);
    assert!(attributed(&missing).is_empty());
    let mut other = a.clone(); other.actions[0].action = MarkAction::VaultWithdraw { vault: Address::ZERO };
    assert!(attributed(&other).is_empty());
    let mut stale = a.clone(); stale.time_ns += 1_000_000;
    assert!(attributed(&stale).is_empty());
    // An unrelated vault's withdrawal does not suppress the unique source.
    other.actions.push(a.actions[0].clone());
    assert_eq!(attributed(&other).len(), 16);
}

#[test]
fn deposits_and_failed_withdrawals_have_no_close_anchor() {
    let mut g = golden();
    g.action["isDeposit"] = true.into();
    assert!(mark_actions(&replica_line(1, &[&[(&g.action.to_string(), &g.response.to_string())]])).actions.is_empty());
    g.action["isDeposit"] = false.into();
    g.response["res"] = serde_json::json!({"status":"err","response":"Vault withdrawal rejected"});
    assert!(mark_actions(&replica_line(1, &[&[(&g.action.to_string(), &g.response.to_string())]])).actions.is_empty());
    let malformed = serde_json::json!({"type":"vaultTransfer","isDeposit":false});
    let line = replica_line(1, &[&[(&malformed.to_string(), &ok(r#"{"type":"default"}"#))]]);
    assert!(MarkActions::of(&crate::accounts::replica::ReplicaLine::parse(&line).unwrap()).unwrap_err().contains("vaultAddress"));
}

#[test]
fn other_order_types_or_non_ioc_shapes_cannot_become_vault_closes() {
    for change in 0..4 {
        let mut g = golden(); let mut status = g.statuses.remove(0);
        match change { 0 => status.order.order_type = "Limit".into(), 1 => status.order.is_trigger = true,
            2 => status.order.reduce_only = false, _ => status.order.tif = Some("Gtc".into()) }
        assert_eq!(BlockStatuses::of([status]).unwrap().vault_closes.len(), 0);
    }
}

#[test]
fn multisig_withdrawal_retains_the_outer_rank_and_competes_with_native_withdrawal() {
    let g = golden(); let mut a = actions(&g);
    let wrapped = serde_json::json!({"type":"multiSig","payload":{"action":g.action}});
    let parsed = mark_actions(&replica_line(1, &[&[(&wrapped.to_string(), &g.response.to_string())]]));
    assert_eq!(parsed.actions[0].action, a.actions[0].action);
    assert_eq!(parsed.actions[0].rank, (0, 3, 0));
    a.actions.extend(parsed.actions);
    let s = BlockStatuses::of(g.statuses).unwrap();
    assert!(s.vault_closes.attributed(&a, &FxHashSet::default()).is_empty());
}

#[test]
fn corewriter_withdrawal_attempt_cannot_be_mistaken_for_native_withdrawal_effects() {
    let g = golden(); let a = actions(&g);
    for deposit in [false, true] {
        let mut action = g.action.clone(); action["isDeposit"] = deposit.into();
        let event: SystemAction = serde_json::from_value(serde_json::json!({"user":g.response["user"],"evm_tx_hash":null,"action":action})).unwrap();
        let system = SystemBlock::of([event], &g.statuses).unwrap();
        let statuses = BlockStatuses::of(g.statuses.clone()).unwrap();
        let anchors = system.anchors(&a, &statuses, None, |_| None).unwrap();
        let placed = statuses.vault_closes.attributed(&a, &anchors.unplaced_vault_withdrawals);
        assert_eq!(placed.len(), if deposit { 16 } else { 0 });
    }
}
