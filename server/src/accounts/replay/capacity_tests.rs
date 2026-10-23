//! Complete recorded account windows complement the native arithmetic and isolated price-attribution tests.
use super::*;
use crate::accounts::checkpoint::{DexAccount, Position};

fn source(case: &serde_json::Value) -> (Account, Markets) {
    let mut markets = Markets { usdc_dexes: vec![true], asset_names: vec![Vec::new()], ..Markets::default() };
    let mut positions = Vec::new();
    let mut isolated_settings = Vec::new();
    for (coin, meta) in case["markets"].as_object().unwrap() {
        let asset = markets.asset_names[0].len();
        markets.asset_names[0].push(coin.clone());
        markets.perps.insert(coin.clone(), PerpMarket { dex: 0, asset, margin_mode: AssetMarginMode::Normal,
            default_leverage: 4, size_decimals: u32::try_from(meta["size_decimals"].as_u64().unwrap()).unwrap() });
        let raw = &case["before"]["positions"][coin];
        let size_raw = raw["s"].as_i64().unwrap();
        let margin = if let Some(isolated) = raw["l"].get("I") {
            let leverage = u32::try_from(isolated["l"].as_u64().unwrap()).unwrap();
            if size_raw == 0 { isolated_settings.push((asset, leverage)); continue; }
            PositionMargin::Isolated { cash_micro: isolated["u"].as_i64().unwrap(), leverage }
        } else {
            if size_raw == 0 { continue; }
            PositionMargin::Cross { leverage: u32::try_from(raw["l"]["C"].as_u64().unwrap()).unwrap() }
        };
        positions.push(Position { asset, size_raw, margin, table_id: u32::try_from(raw["M"].as_u64().unwrap()).unwrap() });
    }
    let account = Account { mode: AccountMode::Unified, spot: vec![(USDC, case["before"]["spot_wei"].as_i64().unwrap())], lending: vec![], dex_states: vec![DexAccount {
        dex: 0, cash_micro: case["before"]["cash_micro"].as_i64().unwrap(), positions, isolated_settings,
    }] };
    (account, markets)
}

fn replay(case: &serde_json::Value, invalidate_on_cross_fill: bool) -> (AccountMargin, Markets) {
    let (source, markets) = source(case);
    let mut account = AccountMargin::of(&source, &markets);
    for event in case["events"].as_array().unwrap() {
        if let Some(funding) = event.get("funding") {
            apply_funding(&mut account, &markets, funding["coin"].as_str().unwrap(),
                usdc_wei(funding["funding_amount"].as_str().unwrap()).unwrap()).unwrap();
            continue;
        }
        let wire: Fill = serde_json::from_value(event["fill"].clone()).unwrap();
        let market = &markets.perps[&wire.coin];
        let mark = |coin: &str, px: u64| FillMarkRange {
            min_px_raw: px, max_px_raw: px, size_decimals: markets.perps[coin].size_decimals,
        };
        let mut fill = FillEvent::at_mark(&wire, Some(mark(&wire.coin, event["mark_raw"].as_u64().unwrap()))).unwrap();
        fill.collateral_marks = Some(Arc::new(event["collateral"].as_object().unwrap().iter()
            .map(|(coin, px)| (coin.clone(), mark(coin, px.as_u64().unwrap()))).collect()));
        if invalidate_on_cross_fill && !account.isolated_leverage.contains_key(&(market.dex, market.asset)) {
            account.cross_capacity.remove(&market.dex);
        }
        apply_perp_fill(&mut account, &markets, event["height"].as_u64().unwrap(), &fill).unwrap();
    }
    (account, markets)
}

#[test]
fn ewy_two_complete_windows_reconcile_dynamic_capacity_and_funding() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!("cross_capacity_windows.json")).unwrap();
    for (case, old_error_micro) in fixture["cases"].as_array().unwrap().iter().zip([10_044, 485]) {
        let (account, markets) = replay(case, false);
        let expected: FxHashMap<_, _> = case["after"]["positions"].as_object().unwrap().iter().filter_map(|(coin, raw)| {
            let isolated = raw["l"].get("I")?;
            (raw["s"].as_i64().unwrap() != 0).then(|| ((0, markets.perps[coin].asset), isolated["u"].as_i64().unwrap() * WEI_PER_MICRO))
        }).collect();
        let expected_cash = case["after"]["aggregate_cash_micro"].as_i64().unwrap() * WEI_PER_MICRO;
        assert_eq!(account.isolated, expected, "{}", case["window"]);
        assert_eq!(account.pools, Pools::Unified(expected_cash));
        assert_eq!(account.estimated, case["before"]["spot_wei"].as_i64().unwrap() != 0,
            "amounts reconcile, but initial spot funds retain the separate physical-routing boundary");
        let (old, _) = replay(case, true);
        let key = (0, markets.perps["xyz:EWY"].asset);
        assert_eq!(old.isolated[&key] - account.isolated[&key], old_error_micro * WEI_PER_MICRO);
        assert!(old.estimated, "the former invalidation loses the capacity proof");
        if case["before"]["spot_wei"].as_i64().unwrap() == 0 {
            let mut maker_case = case.clone();
            for event in maker_case["events"].as_array_mut().unwrap() {
                if let Some(fill) = event.get_mut("fill") { fill["crossed"] = false.into(); }
            }
            let (maker, _) = replay(&maker_case, false);
            assert_eq!(maker.isolated, account.isolated, "the aggregate estimate remains available");
            assert!(maker.estimated, "maker fills do not prove prefunding after cross cash can sweep");
        }
    }
}

#[test]
fn zhipu_complete_window_preserves_capacity_and_spot_dust() {
    // Independent account: static cross holdings, 40 IOC fills, and only three wei of spot USDC.
    // Two native caps explain 2_284 + 28_075 micro; no capacity/endpoint correction is an input.
    let case: serde_json::Value = serde_json::from_str(include_str!("zhipu_capacity_window.json")).unwrap();
    let (account, markets) = replay(&case, false);
    let expected: FxHashMap<_, _> = case["after"]["positions"].as_object().unwrap().iter().filter_map(|(coin, raw)| {
        let isolated = raw["l"].get("I")?;
        (raw["s"].as_i64().unwrap() != 0).then(|| ((0, markets.perps[coin].asset), isolated["u"].as_i64().unwrap() * WEI_PER_MICRO))
    }).collect();
    assert_eq!(account.isolated, expected);
    assert_eq!(account.pools, Pools::Unified(
        case["after"]["aggregate_cash_micro"].as_i64().unwrap() * WEI_PER_MICRO
            + case["after"]["spot_dust_wei"].as_i64().unwrap(),
    ));
    assert!(account.estimated, "aggregate equality does not establish Unified spot routing");
}

fn cross_dex_source(case: &serde_json::Value) -> (Account, Markets) {
    let count = case["before"]["dex_states"].as_array().unwrap().iter()
        .map(|dex| usize::try_from(dex["dex"].as_u64().unwrap()).unwrap()).max().unwrap() + 1;
    let mut markets = Markets { usdc_dexes: vec![false; count], asset_names: vec![Vec::new(); count], ..Markets::default() };
    for (coin, meta) in case["markets"].as_object().unwrap() {
        let dex = usize::try_from(meta["dex"].as_u64().unwrap()).unwrap();
        let asset = usize::try_from(meta["asset"].as_u64().unwrap()).unwrap();
        let names = &mut markets.asset_names[dex];
        names.resize(names.len().max(asset + 1), String::new());
        names[asset] = coin.clone();
        markets.perps.insert(coin.clone(), PerpMarket { dex, asset,
            margin_mode: match meta["margin_mode"].as_str().unwrap() {
                "normal" => AssetMarginMode::Normal, "noCross" => AssetMarginMode::NoCross,
                mode => panic!("unhandled recorded margin mode {mode}"),
            },
            default_leverage: u32::try_from(meta["default_leverage"].as_u64().unwrap()).unwrap(),
            size_decimals: u32::try_from(meta["size_decimals"].as_u64().unwrap()).unwrap(),
        });
        let action_asset = if dex == 0 { asset } else { 100_000 + dex * 10_000 + asset };
        markets.action_assets.insert(u32::try_from(action_asset).unwrap(), coin.clone());
    }
    let dex_states = case["before"]["dex_states"].as_array().unwrap().iter().map(|dex| {
        let dex_index = usize::try_from(dex["dex"].as_u64().unwrap()).unwrap();
        markets.usdc_dexes[dex_index] = true;
        let (mut positions, mut isolated_settings) = (Vec::new(), Vec::new());
        for (coin, raw) in dex["positions"].as_object().unwrap() {
            let market = &markets.perps[coin];
            let size_raw = raw.get("s").map_or(0, |size| size.as_i64().unwrap()); // Native serialized zero.
            let margin = if let Some(isolated) = raw["l"].get("I") {
                let leverage = u32::try_from(isolated["l"].as_u64().unwrap()).unwrap();
                if size_raw == 0 { isolated_settings.push((market.asset, leverage)); continue; }
                PositionMargin::Isolated { cash_micro: isolated["u"].as_i64().unwrap(), leverage }
            } else {
                PositionMargin::Cross { leverage: u32::try_from(raw["l"]["C"].as_u64().unwrap()).unwrap() }
            };
            positions.push(Position { asset: market.asset, size_raw, margin,
                table_id: u32::try_from(case["markets"][coin]["table_id"].as_u64().unwrap()).unwrap() });
        }
        DexAccount { dex: dex_index, cash_micro: dex["cash_micro"].as_i64().unwrap(), positions, isolated_settings }
    }).collect();
    (Account { mode: AccountMode::Unified, spot: vec![(USDC, case["before"]["spot_wei"].as_i64().unwrap())],
        lending: vec![], dex_states }, markets)
}

fn recorded_range(raw: &serde_json::Value) -> FillMarkRange {
    FillMarkRange { min_px_raw: raw["min_px_raw"].as_u64().unwrap(), max_px_raw: raw["max_px_raw"].as_u64().unwrap(),
        size_decimals: u32::try_from(raw["size_decimals"].as_u64().unwrap()).unwrap() }
}

fn replay_cross_dex(case: &serde_json::Value, omit_capacity: bool) -> (AccountMargin, Markets) {
    let (source, markets) = cross_dex_source(case);
    let mut account = AccountMargin::of(&source, &markets);
    if omit_capacity { account.cross_capacity.clear(); }
    for event in case["events"].as_array().unwrap() {
        let height = event["height"].as_u64().unwrap();
        if let Some(action) = event.get("action") {
            assert_eq!(action["type"], "updateLeverage");
            apply_action(&mut account, &markets, height, &BlockAction::Leverage {
                asset: u32::try_from(action["asset"].as_u64().unwrap()).unwrap(),
                is_cross: action["isCross"].as_bool().unwrap(),
                leverage: u32::try_from(action["leverage"].as_u64().unwrap()).unwrap(),
            }).unwrap();
            continue;
        }
        if let Some(funding) = event.get("funding") {
            apply_funding(&mut account, &markets, funding["coin"].as_str().unwrap(),
                usdc_wei(funding["funding_amount"].as_str().unwrap()).unwrap()).unwrap();
            continue;
        }
        let wire: Fill = serde_json::from_value(event["fill"].clone()).unwrap();
        let mut fill = FillEvent::at_mark(&wire, (!event["mark"].is_null()).then(|| recorded_range(&event["mark"]))).unwrap();
        fill.collateral_marks = Some(Arc::new(event["collateral"].as_object().unwrap().iter()
            .map(|(coin, price)| (coin.clone(), recorded_range(price))).collect()));
        apply_perp_fill(&mut account, &markets, height, &fill).unwrap();
    }
    (account, markets)
}

#[test]
fn cl_and_crcl_complete_cross_dex_windows_reconcile_without_claiming_routing_proof() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!("unified_cross_dex_capacity_windows.json")).unwrap();
    for (case, old_error_micro, coin) in fixture["cases"].as_array().unwrap().iter()
        .zip([4_428, 11_466]).zip(["xyz:CL", "xyz:CRCL"]).map(|((case, error), coin)| (case, error, coin)) {
        let (account, markets) = replay_cross_dex(case, false);
        let mut expected = FxHashMap::default();
        let mut cash = case["after"]["spot_wei"].as_i64().unwrap();
        for dex in case["after"]["dex_states"].as_array().unwrap() {
            cash += dex["cash_micro"].as_i64().unwrap() * WEI_PER_MICRO;
            for (coin, raw) in dex["positions"].as_object().unwrap() {
                let Some(isolated) = raw["l"].get("I") else { continue; };
                if raw.get("s").is_none_or(|size| size.as_i64().unwrap() == 0) { continue; }
                let market = &markets.perps[coin];
                expected.insert((market.dex, market.asset), isolated["u"].as_i64().unwrap() * WEI_PER_MICRO);
            }
        }
        assert_eq!(account.isolated, expected, "{}", case["window"]);
        assert_eq!(account.pools, Pools::Unified(cash));
        assert!(account.estimated, "matching amounts do not prove the physical Unified sweep sequence");
        let (old, _) = replay_cross_dex(case, true);
        let market = &markets.perps[coin];
        assert_eq!(old.isolated[&(market.dex, market.asset)] - account.isolated[&(market.dex, market.asset)], old_error_micro * WEI_PER_MICRO);
    }
}

#[test]
fn unified_foreign_collateral_is_requested_and_unknown_portfolios_cannot_be_omitted() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!("unified_cross_dex_capacity_windows.json")).unwrap();
    let case = &fixture["cases"][0];
    let (source, markets) = cross_dex_source(case);
    let owner: Address = case["user"].as_str().unwrap().parse().unwrap();
    let account = AccountMargin::of(&source, &markets);
    let wire: Fill = serde_json::from_value(case["events"][0]["fill"].clone()).unwrap();
    let states = MarginStates { markets, accounts: [(owner, account)].into_iter().collect() };
    assert_eq!(states.collateral_coins([crate::types::node_data::NodeDataFill(owner, wire)].iter()), ["HYPE"]);

    for variant in 0..3 {
        let mut missing = case.clone();
        for event in missing["events"].as_array_mut().unwrap().iter_mut().filter(|e| e.get("fill").is_some()) {
            match variant {
                0 => { event["collateral"].as_object_mut().unwrap().clear(); }
                1 => { event["collateral"]["HYPE"]["max_px_raw"] = (event["collateral"]["HYPE"]["min_px_raw"].as_u64().unwrap() + 1).into(); }
                _ => { event["collateral"]["HYPE"]["size_decimals"] = 3.into(); }
            }
        }
        let (account, markets) = replay_cross_dex(&missing, false);
        let (without, _) = replay_cross_dex(case, true);
        let key = (1, markets.perps["xyz:CL"].asset);
        assert_eq!(account.isolated[&key], without.isolated[&key], "unknown foreign prices cannot mean zero exposure");
        assert!(account.estimated);
    }
}

#[test]
fn missing_cross_dex_projection_cannot_turn_a_partial_portfolio_into_capacity() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!("unified_cross_dex_capacity_windows.json")).unwrap();
    let case = &fixture["cases"][0];
    let (source, markets) = cross_dex_source(case);
    let mut account = AccountMargin::of(&source, &markets);
    // Cash before the last recorded CL fill, derived by the complete-window test above.
    account.pools = Pools::Unified(-96_483_280_364);
    let event = case["events"].as_array().unwrap().last().unwrap();
    let wire: Fill = serde_json::from_value(event["fill"].clone()).unwrap();
    let mut fill = FillEvent::at_mark(&wire, Some(recorded_range(&event["mark"]))).unwrap();
    fill.collateral_marks = Some(Arc::new(event["collateral"].as_object().unwrap().iter()
        .map(|(coin, price)| (coin.clone(), recorded_range(price))).collect()));
    let mut known = account.clone();
    apply_perp_fill(&mut known, &markets, event["height"].as_u64().unwrap(), &fill).unwrap();
    assert_eq!(known.isolated[&(1, 29)], 43_098_149 * WEI_PER_MICRO);

    // A different dex has an unprojectable cross fill or leverage change. HYPE's valid projection alone
    // must not provide the capacity; treating the missing portfolio as empty would silently overstate it.
    account.cross_exposure_seen.insert(1);
    assert!(account.cross_capacity.contains_key(&0));
    assert!(!account.cross_capacity.contains_key(&1));
    apply_perp_fill(&mut account, &markets, event["height"].as_u64().unwrap(), &fill).unwrap();
    assert_eq!(account.isolated[&(1, 29)], 43_102_577 * WEI_PER_MICRO);
    assert!(account.estimated);
}
