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
