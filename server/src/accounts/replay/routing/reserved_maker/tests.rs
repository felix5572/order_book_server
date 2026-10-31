use std::sync::Arc;
use super::*;
use crate::accounts::{checkpoint::{DexAccount, OrderTrackerSnapshot, Position}, margin::MarginTable,
    replay::{AccountMargin, Pools, PerpMarket, FillMarkRange, apply_perp_fill, cross_capacity::CrossCapacityProjection}};
use super::super::{apply_update, RoutingUpdate};
use crate::types::Fill;

fn recorded() -> serde_json::Value { serde_json::from_str(include_str!("../../prl_reserved_maker_golden.json")).unwrap() }

// Native positions, metadata, tracker, money and fills. Remap executing dex slot10 to fixture slot1;
// keep its native PDI10/asset10. Unrelated checkpoint sections are scaffold, not a full native wrapper.
fn seeded() -> (CheckpointAccounts, Markets, Address) { seeded_case(&recorded()) }

fn seeded_case(case: &serde_json::Value) -> (CheckpointAccounts, Markets, Address) {
    let (mut cp, _, who) = super::super::tests::checkpoint();
    cp.dexes[1].pdi = 10;
    let mut markets = Markets { usdc_dexes: vec![true; 2], tables: vec![Default::default(); 2],
        asset_names: vec![vec![]; 2], ..Default::default() };
    for row in case["markets"].as_array().unwrap() {
        let dex = usize::from(row["dex"].as_u64().unwrap() == 10);
        let asset = row["asset"].as_u64().unwrap() as usize;
        let value = &row["meta"];
        let name = value["name"].as_str().unwrap();
        if markets.asset_names[dex].len() <= asset { markets.asset_names[dex].resize(asset + 1, "unused fixture slot".into()); }
        markets.asset_names[dex][asset] = name.into();
        markets.perps.insert(name.into(), PerpMarket { dex, asset,
            size_decimals: value["szDecimals"].as_u64().unwrap() as u32,
            margin_mode: if dex == 1 { AssetMarginMode::NoCross } else { AssetMarginMode::Normal }, max_leverage: 3 });
    }
    markets.tables[1].insert(3, MarginTable::single(3).unwrap());
    let source = cp.accounts.get_mut(&who).unwrap();
    source.spot = vec![(0, case["before"]["spot_usdc_wei"].as_i64().unwrap())];
    source.dex_states = ["0", "10"].into_iter().enumerate().map(|(dex, key)| {
        let state = &case["before"]["states"][key];
        let positions = state["p"]["p"].as_array().unwrap().iter().filter_map(|p| {
            let size_raw = p[1]["s"].as_i64().unwrap_or(0);
            if size_raw == 0 { return None; }
            let margin = if dex == 0 { PositionMargin::Cross { leverage: p[1]["l"]["C"].as_u64().unwrap() as u32 } }
                else { PositionMargin::Isolated { leverage: 3, cash_micro: p[1]["l"]["I"]["u"].as_i64().unwrap() } };
            Some(Position { asset: p[0].as_u64().unwrap() as usize % 10_000, size_raw,
                table_id: p[1]["M"].as_u64().unwrap() as u32, margin })
        }).collect();
        DexAccount { dex, cash_micro: state["u"].as_i64().unwrap(), positions, isolated_settings: vec![] }
    }).collect();
    for (dex, key) in ["0", "10"].into_iter().enumerate() {
        if !case["before"]["trackers"][key].is_null() {
            let tracker: OrderTrackerSnapshot = serde_json::from_value(case["before"]["trackers"][key].clone()).unwrap();
            cp.order_trackers[dex].as_mut().unwrap().insert(who, tracker);
        }
    }
    (cp, markets, who)
}

fn event(row: &serde_json::Value) -> FillEvent {
    let wire: Fill = serde_json::from_value(row["fill"].clone()).unwrap();
    let mark = row["mark_raw"].as_u64().unwrap();
    let mut fill = FillEvent::at_mark(&wire, Some(FillMarkRange { min_px_raw: mark, max_px_raw: mark, size_decimals: 1 })).unwrap();
    fill.collateral_marks = Some(Arc::new(row["collateral"].as_object().unwrap().iter().map(|(coin, v)| {
        (coin.clone(), FillMarkRange { min_px_raw: v["min_px_raw"].as_u64().unwrap(),
            max_px_raw: v["max_px_raw"].as_u64().unwrap(), size_decimals: v["size_decimals"].as_u64().unwrap() as u32 })
    }).collect()));
    fill
}

fn seeded_account() -> (AccountMargin, Markets) {
    let (cp, markets, who) = seeded();
    let mut account = AccountMargin::of(&cp.accounts[&who], &markets);
    account.routing = RoutingProjection::seeds(&cp, &markets).remove(&who);
    assert_eq!(account.routing.as_ref().unwrap().path, FundingPath::ReservedMaker);
    (account, markets)
}

#[test]
fn prl_two_makers_use_prefunded_cash_across_blocks_despite_zero_aggregate_availability() {
    let case = recorded();
    for with_route in [false, true] {
        let (mut account, markets) = seeded_account();
        if !with_route { account.routing = None; }
        for (i, row) in case["fills"].as_array().unwrap().iter().enumerate() {
            let fill = event(row);
            let Pools::Unified(pool_before) = account.pools else { panic!("Unified fixture") };
            assert_eq!(CrossCapacityProjection::combined_available_micro(pool_before, account.cross_capacity.values(),
                fill.collateral_marks.as_ref().unwrap()).unwrap(), Some(0));
            let before = pool_before + account.isolated.values().sum::<i64>();
            apply_perp_fill(&mut account, &markets, row["height"].as_u64().unwrap(), &fill).unwrap();
            let Pools::Unified(pool_after) = account.pools else { unreachable!() };
            assert_eq!(pool_after + account.isolated.values().sum::<i64>(), before + fill.cash_wei(1, 5).unwrap());
            if with_route {
                let native = &case["native_steps"][i];
                assert_eq!(account.isolated[&(1, 10)] / WEI_PER_MICRO, native["isolated_cash_micro"].as_i64().unwrap());
                assert_eq!(account.routing.as_ref().unwrap().cash_micro, native["cash_floor_micro"].as_i64());
            }
        }
        let difference = if with_route { 0 } else { 33_153_000 };
        assert_eq!(account.isolated[&(1, 10)] / WEI_PER_MICRO, -1_145_402_159 - difference);
        assert_eq!(account.pools, Pools::Unified(-3_933_018_073_168 + difference * WEI_PER_MICRO));
        assert!(account.estimated, "retained cash sufficiency does not establish general physical routing");
    }
}

#[test]
fn reserved_maker_requires_complete_retention_and_bounded_opposing_orders() {
    for variant in 0..10 {
        let (mut cp, mut markets, who) = seeded();
        match variant {
            0 => cp.order_trackers[0] = None,
            1 => { cp.spot_usdc_holds.insert(who, 1); },
            2 => cp.order_trackers[1].as_mut().unwrap().get_mut(&who).unwrap().reserve_micro += 1,
            3 => cp.order_trackers[1].as_mut().unwrap().get_mut(&who).unwrap().assets[0].1.position_raw += 1,
            4 => cp.order_trackers[1].as_mut().unwrap().get_mut(&who).unwrap().assets[0].1.leverage = 2,
            5 => cp.accounts.get_mut(&who).unwrap().dex_states[1].cash_micro -= 1,
            6 => { markets.perps.get_mut("io:PRL").unwrap().margin_mode = AssetMarginMode::Normal; },
            7 => { markets.tables[1].insert(3, MarginTable::listed([(0, 3, 0), (100_000_000, 2, 0)]).unwrap()); },
            8 => { cp.order_trackers[1].as_mut().unwrap().get_mut(&who).unwrap().assets.clear(); },
            9 => { cp.order_trackers[1].as_mut().unwrap().get_mut(&who).unwrap().assets[0].1.size_raw[1] = 16_907; },
            _ => unreachable!(),
        }
        assert!(RoutingProjection::seeds(&cp, &markets).get(&who).is_none_or(|r| r.path != FundingPath::ReservedMaker), "variant {variant}");
    }
}

#[test]
fn insufficient_unknown_or_opposite_maker_never_turns_cash_floor_into_upper_cap() {
    for variant in 0..11 {
        let (mut account, _) = seeded_account();
        let route = account.routing.as_mut().unwrap();
        let mut fill = event(&recorded()["fills"][0]);
        match variant {
            0 => route.cash_micro = Some(1),
            1 => fill.mark = None,
            2 => fill.mark.as_mut().unwrap().max_px_raw += 1,
            3 => fill.crossed = true,
            4 => fill.backstop = Some(crate::accounts::backstop::Backstop::Liquidator),
            5 => fill.usdc_fee_wei = -100,
            6 => fill.side = Side::Ask,
            7 => fill.start_position = "0".into(),
            8 => fill.coin = "ETH".into(),
            9 => route.reserve.size_raw[0] = 1,
            10 => route.invalidate(),
            _ => unreachable!(),
        }
        assert_eq!(route.capacity(1195567690, &fill, -1_079_093_295, -3_929_702_773_168), None, "variant {variant}");
        assert_eq!(route.cash_micro, None);
    }
}

#[test]
fn order_ioc_and_unattributed_updates_discard_retention_proof() {
    for update in [RoutingUpdate::Unavailable, RoutingUpdate::OrdinaryIoc,
        RoutingUpdate::Orders { changes: vec![], mark_raw: Some(193_080), prefund: true }] {
        let (mut account, _) = seeded_account();
        apply_update(&mut account, 1195567689, &update);
        assert!(account.routing.is_none());
    }
    let (account, _) = seeded_account();
    let route = account.routing.as_ref().unwrap();
    assert!(route.expires_on_funding("io:PRL"));
    assert!(route.expires_on_funding("ETH"));
}

#[test]
fn prl_full_maker_consumes_reserve_despite_positive_but_insufficient_aggregate_capacity() {
    let case: serde_json::Value = serde_json::from_str(include_str!("../../prl_consumed_reserve_golden.json")).unwrap();
    for with_route in [false, true] {
        let (cp, markets, who) = seeded_case(&case);
        let mut account = AccountMargin::of(&cp.accounts[&who], &markets);
        if with_route {
            account.routing = RoutingProjection::seeds(&cp, &markets).remove(&who);
            assert_eq!(account.routing.as_ref().unwrap().path, FundingPath::ReservedMaker);
        }
        let row = &case["fills"][0];
        let fill = event(row);
        let Pools::Unified(pool_before) = account.pools else { panic!("Unified fixture") };
        assert_eq!(CrossCapacityProjection::combined_available_micro(pool_before, account.cross_capacity.values(),
            fill.collateral_marks.as_ref().unwrap()).unwrap(), Some(69_466_132));
        let total_before = pool_before + account.isolated.values().sum::<i64>();
        apply_perp_fill(&mut account, &markets, row["height"].as_u64().unwrap(), &fill).unwrap();
        let difference = if with_route { 0 } else { 68_727_201 };
        assert_eq!(account.isolated[&(1, 10)] / WEI_PER_MICRO, -655_012_420 - difference);
        assert_eq!(account.pools, Pools::Unified(-1_008_695_003_064 + difference * WEI_PER_MICRO));
        let Pools::Unified(pool_after) = account.pools else { unreachable!() };
        assert_eq!(pool_after + account.isolated.values().sum::<i64>(), total_before + fill.cash_wei(1, 5).unwrap());
        if with_route {
            let route = account.routing.as_ref().unwrap();
            assert_eq!(route.reserve.size_raw, [0, 0]);
            assert_eq!(route.reserve.required_micro(), Some(0));
            assert_eq!(route.cash_micro, Some(0), "fully consumed reservation leaves no guaranteed retained cash");
        }
        assert!(account.estimated, "a sufficient retained-cash bound does not prove all routing timings");
    }
}
