//! Successful leverage table selection, including anchors installed behind the action stream.
use super::*;
use crate::accounts::{
    actions::{BlockAction, parse_block_actions, tests::action_line},
    checkpoint::tests::{HOLDER, ISOLATED, user as checkpoint_user},
    liquidation::solve,
    margin::MarginTable,
    replay::tests::per_dex_with_main_cash,
    test_support::{ANCHOR_TABLE, CURRENT_TABLE, PUMP, anchor, block_actions, fills, misc, replay_book, row},
};

fn leverage(is_cross: bool) -> BlockAction { BlockAction::Leverage { asset: 1, is_cross, leverage: 3 } }

fn later_anchor(height: u64, holder: Address, size_raw: i64) -> Anchor {
    let mut next = anchor(height, &[(PUMP, 0, &[(holder, size_raw)]), ("BTC", 5, &[])]);
    next.margins = per_dex_with_main_cash(-12_000_000_000);
    next
}

#[test]
fn successful_leverage_refreshes_table_without_changing_fill_watermark() {
    for (holder, is_cross, size_raw) in [(checkpoint_user(HOLDER), true, 1_000_000), (checkpoint_user(ISOLATED), false, -2_000_000)] {
        let mut book = replay_book(1_000, 1_000, &[(holder, size_raw)]);
        book.apply_block_actions(block_actions(1_001, &[(BlockAction::IsolatedMargin { asset: 1, ntli_micro: 0 }, holder)]));
        assert_eq!(book.coins[PUMP].records[&holder].table_id, Some(ANCHOR_TABLE));
        // ISOLATED already has leverage3: the accepted setter still refreshes M.
        book.apply_block_actions(block_actions(1_002, &[(leverage(is_cross), holder)]));
        let position = &book.coins[PUMP].records[&holder];
        assert_eq!((position.size_raw, position.version_block, &position.applied), (size_raw, 1_000, &Applied::Anchor));
        assert_eq!((position.table_id, position.table_set_after), (Some(CURRENT_TABLE), Some(1_002)));
        let expected = position.clone();
        book.apply_block_actions(block_actions(1_002, &[(leverage(is_cross), holder)]));
        assert_eq!(book.coins[PUMP].records[&holder], expected);
    }
}

#[test]
fn leverage_after_a_delayed_anchor_is_not_backdated_or_lost() {
    let holder = checkpoint_user(ISOLATED);
    for add in [false, true] {
        let mut book = replay_book(1_000, 1_000, &[(holder, -2_000_000)]);
        book.apply_block_actions(block_actions(1_001, &[]));
        book.apply_block_actions(block_actions(1_002, &[(leverage(false), holder)]));
        if add { book.apply_fills(fills(1_002, &[(holder, PUMP, "A", 1, "-2000000", "1")])); }
        let ReplayCheck::Checked { rows, .. } = book.install_anchor(later_anchor(1_001, holder, -2_000_000)).replay_check else { panic!("checked") };
        assert_eq!((rows.moved_after, rows.differ, rows.class_differs), (1, 0, 0));
        assert_eq!(book.coins[PUMP].records[&holder].table_id, Some(CURRENT_TABLE));
        assert_eq!(book.coins[PUMP].records[&holder].table_set_after, Some(1_002));
        // A covering anchor is authoritative, and an old receipt cannot override it.
        book.install_anchor(later_anchor(1_003, holder, -2_000_000 - i64::from(add)));
        assert_eq!(book.coins[PUMP].records[&holder].table_id, Some(ANCHOR_TABLE));
        assert!(book.apply_block_actions(block_actions(1_002, &[(leverage(false), holder)])).stale);
        assert_eq!(book.coins[PUMP].records[&holder].table_id, Some(ANCHOR_TABLE));
    }
}

#[test]
fn first_anchor_replays_retained_leverage_and_held_fill() {
    let holder = checkpoint_user(ISOLATED);
    let mut book = AccountBook::new(0);
    book.apply_fills(fills(100, &[]));
    book.apply_misc_events(misc(100, &[]));
    book.apply_block_actions(block_actions(1_000, &[]));
    book.apply_block_actions(block_actions(1_001, &[(leverage(false), holder)]));
    book.apply_fills(fills(1_002, &[(holder, PUMP, "A", 1, "-2000000", "1")]));
    book.install_anchor(later_anchor(1_000, holder, -2_000_000));
    let record = &book.coins[PUMP].records[&holder];
    assert_eq!((record.size_raw, record.table_id, record.table_set_after), (-2_000_001, Some(CURRENT_TABLE), Some(1_001)));
}

#[test]
fn an_older_leverage_receipt_cannot_override_a_later_reopen_table() {
    let holder = checkpoint_user(ISOLATED);
    let mut book = replay_book(1_000, 1_000, &[(holder, -2_000_000)]);
    book.apply_fills(fills(1_002, &[(holder, PUMP, "B", 1, "-2000000", "2000000")]));
    book.apply_fills(fills(1_003, &[(holder, PUMP, "A", 2, "0", "1")]));
    book.coins.get_mut(PUMP).unwrap().table_id = ANCHOR_TABLE;
    book.apply_block_actions(block_actions(1_001, &[(leverage(false), holder)]));
    let record = &book.coins[PUMP].records[&holder];
    assert_eq!((record.table_id, record.table_set_after), (Some(CURRENT_TABLE), Some(1_003)));
}

#[test]
fn sei_table_transition_golden_given_reconciled_terminal_cash() {
    // Cash replay was already exact in production. This golden isolates the table transition and derived price;
    // it does not re-verify physical cross-dex transfers or the full account cash replay.
    let golden: serde_json::Value = serde_json::from_str(include_str!("sei_leverage_table_golden.json")).unwrap();
    let before = &golden["position_before"];
    let after = &golden["position_after"];
    let holder: Address = golden["user"].as_str().unwrap().parse().unwrap();
    let mut receipt = golden["successful_leverage_action"].clone();
    receipt["receipt"] = receipt["response"].clone();
    receipt["bundle"] = serde_json::json!("0x00");
    receipt["time"] = serde_json::json!("2026-10-25T18:15:40.000000000");
    let line = action_line(&receipt); // Test envelope; original action/receipt and source hash are in the golden.
    let action = parse_block_actions(&line).unwrap();
    assert_eq!(action.actions, [(BlockAction::Leverage { asset: 40, is_cross: false, leverage: 3 }, holder)]);
    let failed = line.replace(r#""status":"ok""#, r#""status":"err""#);
    assert!(parse_block_actions(&failed).unwrap().actions.is_empty());
    let mut position = PositionRecord::anchored(1_196_990_000, AnchorPosition {
        size_raw: before["s"].as_i64().unwrap(), table_id: before["M"].as_u64().unwrap() as u32, row: row(),
    });
    let tables = FxHashMap::from_iter([
        (51, MarginTable::listed([(0, 10, 0), (3_000_000_000_000, 5, 150_000_000_000)]).unwrap()),
        (5, MarginTable::single(5).unwrap()),
    ]);
    let terminal_cash_usdc = after["l"]["I"]["u"].as_i64().unwrap() as f64 / 1_000_000.0;
    let terminal_size = after["s"].as_i64().unwrap();
    let old = solve(terminal_cash_usdc, 0.0, terminal_size as f64, &tables[&51]);
    assert!(same_price(&old, &LiquidationPx::Computed(golden["replayed_liquidation_px"].as_f64().unwrap()), None));
    position.set_table(action.height, golden["current_asset_metadata"]["marginTableId"].as_u64().unwrap() as u32);
    assert_eq!(position.size_raw, 16, "table changes before the later add fill");
    position.apply_fill(&SizeChange { block: 1_196_992_681, identity: (427_442_828_730_831, Side::Bid), start: 16, size_after: terminal_size, current_table: 5 });
    let corrected = solve(terminal_cash_usdc, 0.0, terminal_size as f64, &tables[&position.table_id.unwrap()]);
    assert!(same_price(&corrected, &LiquidationPx::Computed(golden["checkpoint_liquidation_px"].as_f64().unwrap()), None));
    assert_ne!(corrected, old);
}
