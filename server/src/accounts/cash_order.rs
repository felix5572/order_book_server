//! Place manual isolated-margin transfers after receipt-backed fills when their execution order is proven.
//! MRVL H1192678957: the IOC at (0,3,1040) precedes the transfer at (0,3,1049), despite reversed nonces.
//! Applying that transfer before the reduction moved an extra 112.580034 USDC back into cross cash.

use alloy::primitives::Address;
use rustc_hash::{FxHashMap, FxHashSet};

use super::{
    actions::{BlockAction, BlockActions, KeptAction},
    marks::{MarkInputs, actions::Rank, execution::fill_order::{Placement, takers}},
};
use crate::types::node_data::{Batch, NodeDataFill};

struct MarginAfterFills {
    earliest: Rank,
    has_fill: bool,
    eligible: bool,
}

/// All kept actions for a deferred account must be manual margin transfers after ALL its fills, including maker
/// legs and fills on other assets sharing its cash. Mixed/interleaved actions, missing receipts and triggered/system
/// fills keep the existing explicit estimate. This does not infer cross free collateral or execution marks.
/// The retained event log receives the resulting order, so checkpoint rebuilds replay the same cash allocation.
#[derive(Default)]
pub(crate) struct MarginOrder {
    after_fills: FxHashSet<Address>,
}

impl MarginOrder {
    pub(crate) fn of(actions: &BlockActions, fills: &[Batch<NodeDataFill>], inputs: &MarkInputs) -> Self {
        if fills.is_empty() || !actions.actions.iter().any(|(action, _)| matches!(action, BlockAction::IsolatedMargin { .. })) {
            return Self::default();
        }
        let Some(ranks) = &actions.ranks else { return Self::default(); };
        let Ok(mark_actions) = &inputs.mark_actions else { return Self::default(); };
        // Both vectors are produced together by the action adapter; None is reserved for unranked synthetic inputs.
        assert_eq!(actions.actions.len(), ranks.len(), "kept cash actions must retain their execution ranks");
        let mut accounts: FxHashMap<Address, MarginAfterFills> = FxHashMap::default();
        for ((action, user), rank) in actions.actions.iter().zip(ranks) {
            if matches!(action, BlockAction::IsolatedMargin { .. }) {
                accounts.entry(*user).and_modify(|state| state.earliest = state.earliest.min(*rank))
                    .or_insert(MarginAfterFills { earliest: *rank, has_fill: false, eligible: true });
            }
        }
        for (action, user) in &actions.actions {
            if !matches!(action, BlockAction::IsolatedMargin { .. }) && let Some(state) = accounts.get_mut(user) {
                state.eligible = false;
            }
        }
        let triggered = inputs.order_statuses.iter().flat_map(Batch::events_ref)
            .filter(|status| status.status == "triggered").map(|status| status.order.oid).collect();
        let takers = takers(&inputs.fills, mark_actions, &triggered);
        for NodeDataFill(user, fill) in fills.iter().flat_map(Batch::events_ref) {
            let Some(state) = accounts.get_mut(user) else { continue; };
            state.has_fill = true;
            state.eligible &= match takers.get(&(fill.coin.as_str(), fill.tid)).copied().flatten() {
                Some(Placement::BeforeActions) => true,
                Some(Placement::Action(rank)) => rank < state.earliest,
                Some(Placement::Trigger(_)) | None => false,
            };
        }
        Self { after_fills: accounts.into_iter().filter_map(|(user, state)|
            (state.eligible && state.has_fill).then_some(user)).collect() }
    }

    /// Consume the proof only after the mark owner validates this block's stream continuity and inputs.
    /// Missing status history could hide a trigger: a parseable receipt alone must not promote it to exact cash.
    pub(crate) fn defer(self, actions: &mut BlockActions, inputs_trusted: bool) -> Vec<KeptAction> {
        if !inputs_trusted || self.after_fills.is_empty() { return Vec::new(); }

        let mut after = Vec::new();
        let Some(retained_ranks) = actions.ranks.as_mut() else { return Vec::new(); };
        let original = std::mem::take(&mut actions.actions);
        let original_ranks = std::mem::take(retained_ranks);
        for ((action, user), rank) in original.into_iter().zip(original_ranks) {
            if self.after_fills.contains(&user) {
                after.push((action, user));
            } else {
                actions.actions.push((action, user));
                retained_ranks.push(rank);
            }
        }
        after
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::accounts::{
        actions::parse_block_actions,
        marks::{actions::{BookAnchors, MarkAction, MarkActions, RankedAction}, execution::BlockFill},
        replica::ReplicaLine,
        test_support::{batch_line, block_actions, fills, status_line, user},
    };
    use serde_json::json;

    pub(crate) fn defer_margin_after_fills(actions: &mut BlockActions, fills: &[Batch<NodeDataFill>], inputs: &MarkInputs) -> Vec<KeptAction> {
        MarginOrder::of(actions, fills, inputs).defer(actions, true)
    }

    #[test]
    fn lost_or_invalid_status_history_never_promotes_a_parseable_receipt() {
        let (mut actions, fills, inputs) = case();
        let original = actions.clone();
        let order = MarginOrder::of(&actions, &fills, &inputs);
        assert!(order.defer(&mut actions, false).is_empty());
        assert_eq!(actions, original, "untrusted attribution leaves the existing estimated replay intact");
    }

    fn case() -> (BlockActions, Vec<Batch<NodeDataFill>>, MarkInputs) {
        let mut actions = block_actions(8, &[(BlockAction::IsolatedMargin { asset: 0, ntli_micro: 100 }, user(1))]);
        actions.ranks = Some(vec![(0, 3, 5)]);
        let fills = vec![fills(8, &[(user(1), "BTC", "A", 1, "1", "0.1")])];
        let mark_actions = MarkActions { height: 8, time_ns: actions.time_ms * 1_000_000, actions: vec![RankedAction {
            rank: (0, 3, 4), user: Some(user(1)), action: MarkAction::Book(BookAnchors { created: vec![7], ..BookAnchors::default() }),
        }] };
        let inputs = MarkInputs { fills: fills.iter().flat_map(Batch::events_ref).map(BlockFill::of).collect(),
            mark_actions: Ok(mark_actions), ..MarkInputs::default() };
        (actions, fills, inputs)
    }

    #[test]
    fn receipt_order_moves_only_the_proven_accounts_margin() {
        let (mut actions, fills, inputs) = case();
        let wanted = actions.actions.clone();
        actions.actions.push((BlockAction::Withdraw { amount_wei: 100 }, user(2)));
        actions.ranks.as_mut().unwrap().push((0, 3, 6));
        assert_eq!(defer_margin_after_fills(&mut actions, &fills, &inputs), wanted);
        assert_eq!(actions.actions, [(BlockAction::Withdraw { amount_wei: 100 }, user(2))]);
        assert_eq!(actions.ranks, Some(vec![(0, 3, 6)]));
    }

    #[test]
    fn all_fills_and_all_kept_actions_of_an_account_must_fit() {
        for action_rank in [(0, 3, 3), (0, 3, 4)] {
            let (mut actions, fills, inputs) = case();
            actions.ranks = Some(vec![action_rank]);
            assert!(defer_margin_after_fills(&mut actions, &fills, &inputs).is_empty());
        }
        let (mut actions, mut fills, mut inputs) = case();
        let another = crate::accounts::test_support::fills(8, &[(user(1), "ETH", "B", 2, "1", "0.1")]);
        let mut unplaced: Vec<_> = another.events_ref().iter().map(BlockFill::of).collect();
        unplaced[0].oid = 99;
        inputs.fills.extend(unplaced);
        fills.push(another);
        assert!(defer_margin_after_fills(&mut actions, &fills, &inputs).is_empty(), "another asset also shares this account's cash");
        for other in [BlockAction::Withdraw { amount_wei: 100 }, BlockAction::Leverage { asset: 0, is_cross: false, leverage: 3 }] {
            let (mut actions, fills, inputs) = case();
            actions.actions.push((other, user(1)));
            actions.ranks.as_mut().unwrap().push((0, 3, 6));
            assert!(defer_margin_after_fills(&mut actions, &fills, &inputs).is_empty(), "no partial reordering of competing cash actions");
        }
    }

    #[test]
    fn missing_ambiguous_and_triggered_takers_keep_the_estimate() {
        for variant in 0..6 {
            let (mut actions, fills, mut inputs) = case();
            match variant {
                0 => actions.ranks = None,
                1 => inputs.mark_actions = Err("unavailable replica attribution".into()),
                2 => inputs.mark_actions.as_mut().unwrap().actions.clear(),
                3 => inputs.fills.push(inputs.fills[0].clone()),
                4 => inputs.order_statuses.push(status_line(8, "triggered", user(1), 7, None)),
                5 => { let duplicate = inputs.mark_actions.as_ref().unwrap().actions[0].clone(); inputs.mark_actions.as_mut().unwrap().actions.push(duplicate); },
                _ => unreachable!(),
            }
            let before = actions.clone();
            assert!(defer_margin_after_fills(&mut actions, &fills, &inputs).is_empty(), "variant {variant}");
            assert_eq!(actions, before);
        }
    }

    #[test]
    fn maker_uses_external_takers_rank_not_its_resting_orders_rank() {
        let (mut actions, fills, mut inputs) = case();
        inputs.fills[0].crossed = false;
        assert!(defer_margin_after_fills(&mut actions, &fills, &inputs).is_empty(), "maker's old receipt cannot place the trade");
        let mut taker = inputs.fills[0].clone();
        taker.oid = 99;
        taker.crossed = true;
        inputs.fills.push(taker);
        let mut taker_action = inputs.mark_actions.as_ref().unwrap().actions[0].clone();
        taker_action.user = Some(user(2));
        taker_action.action = MarkAction::Book(BookAnchors { created: vec![99], ..BookAnchors::default() });
        inputs.mark_actions.as_mut().unwrap().actions.push(taker_action);
        assert_eq!(defer_margin_after_fills(&mut actions, &fills, &inputs).len(), 1);
    }

    #[test]
    fn bundles_and_multiple_transfers_preserve_execution_order() {
        let (mut actions, fills, inputs) = case();
        actions.ranks = Some(vec![(1, 3, 0)]);
        actions.actions.push((BlockAction::IsolatedMargin { asset: 0, ntli_micro: -50 }, user(1)));
        actions.ranks.as_mut().unwrap().push((1, 3, 2));
        let expected = actions.actions.clone();
        assert_eq!(defer_margin_after_fills(&mut actions, &fills, &inputs), expected);
        let (mut actions, fills, mut inputs) = case();
        inputs.mark_actions.as_mut().unwrap().actions[0].rank = (1, 1, 0);
        assert!(defer_margin_after_fills(&mut actions, &fills, &inputs).is_empty(), "later bundle runs after earlier margin even for ALO category");
    }

    /// Real request/receipt pairs, padded with inert noops to preserve the original bundle positions. The padding
    /// is a test envelope, not archived block contents. Both nonces and fill data remain unchanged.
    fn recorded_case(name: &str) -> (BlockActions, Vec<Batch<NodeDataFill>>, MarkInputs) {
        let golden: serde_json::Value = serde_json::from_str(include_str!("replay/isolated_cash_edge_goldens.json")).unwrap();
        let case = &golden[name];
        let height = case["height"].as_u64().unwrap();
        let last = usize::try_from(case["margin_action"]["action_index"].as_u64().unwrap()).unwrap();
        let mut signed = vec![json!({"action":{"type":"noop"}}); last + 1];
        let mut responses = vec![json!({"user":null,"res":{"status":"ok","response":{"type":"default"}}}); signed.len()];
        for key in ["order_action", "margin_action"] {
            let item = &case[key];
            let index = usize::try_from(item["action_index"].as_u64().unwrap()).unwrap();
            signed[index] = item["signed_action"].clone();
            responses[index] = item["receipt"].clone();
        }
        let hash = &case["margin_action"]["bundle_id"];
        let raw = json!({"height":height,"abci_block":{"time":case["block_time"],
            "signed_action_bundles":[[hash,{"signed_actions":signed}]]}, "resps":{"Full":[[hash,responses]]}}).to_string();
        let actions = parse_block_actions(&raw).unwrap();
        let replica = ReplicaLine::parse(&raw).unwrap();
        let events = json!([case["user"], case["fill"]]).to_string();
        let batch: Batch<NodeDataFill> = sonic_rs::from_str(&batch_line(height, &events)).unwrap();
        let inputs = MarkInputs { fills: batch.events_ref().iter().map(BlockFill::of).collect(),
            mark_actions: Ok(MarkActions::of(&replica).unwrap()), ..MarkInputs::default() };
        (actions, vec![batch], inputs)
    }

    #[test]
    fn mrvl_real_receipt_places_margin_after_reduction_despite_earlier_nonce() {
        let (mut actions, fills, inputs) = recorded_case("mrvl");
        assert_eq!(actions.ranks, Some(vec![(0, 3, 1049)]));
        assert_eq!(inputs.mark_actions.as_ref().unwrap().actions[0].rank, (0, 3, 1040));
        let expected = actions.actions.clone();
        assert_eq!(defer_margin_after_fills(&mut actions, &fills, &inputs), expected);
        assert!(actions.actions.is_empty());
        assert_eq!(actions.ranks, Some(Vec::new()));
    }
    #[test]
    fn live_ewy_estimated_split_has_the_same_receipt_backed_order() {
        let (mut actions, fills, inputs) = recorded_case("ewy");
        assert_eq!(actions.ranks, Some(vec![(0, 3, 827)]));
        assert_eq!(inputs.mark_actions.as_ref().unwrap().actions[0].rank, (0, 3, 765));
        let expected = actions.actions.clone();
        assert_eq!(defer_margin_after_fills(&mut actions, &fills, &inputs), expected);
        assert!(actions.actions.is_empty());
    }

}
