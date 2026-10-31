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
            max_leverage: 4, size_decimals: u32::try_from(meta["size_decimals"].as_u64().unwrap()).unwrap() });
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
            max_leverage: u32::try_from(meta["default_leverage"].as_u64().unwrap()).unwrap(),
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
    let (source, mut markets) = cross_dex_source(case);
    if case["main_dex"].as_bool() == Some(true) { markets.dexes.insert(String::new(), 0); }
    let mut account = AccountMargin::of(&source, &markets);
    if omit_capacity { account.cross_capacity.clear(); }
    for event in case["events"].as_array().unwrap() {
        let height = event["height"].as_u64().unwrap();
        if let Some(ledger) = event.get("ledger") {
            let delta: LedgerDelta = serde_json::from_value(ledger["delta"].clone()).unwrap();
            apply_ledger(&mut account, &markets, height, case["user"].as_str().unwrap().parse().unwrap(), &delta).unwrap();
            continue;
        }
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

fn softbank_source() -> (serde_json::Value, MarginStates, Address) {
    let fixture: serde_json::Value = serde_json::from_str(include_str!("softbank_maker_window.json")).unwrap();
    let case = fixture["cases"][0].clone();
    let (source, mut markets) = cross_dex_source(&case);
    let who: Address = case["user"].as_str().unwrap().parse().unwrap();
    let (mut checkpoint, _, _) = routing::tests::checkpoint();
    checkpoint.accounts = [(who, source)].into_iter().collect();
    checkpoint.spot_usdc_holds.clear();
    checkpoint.order_trackers = (0..2).map(|dex| Some(case["trackers"][dex.to_string()].as_object().map_or_else(
        FxHashMap::default, |tracker| [(who, crate::accounts::checkpoint::OrderTrackerSnapshot {
            reserve_micro: tracker["e"].as_i64().unwrap(),
            assets: serde_json::from_value(tracker["a"].clone()).unwrap(),
        })].into_iter().collect()))).collect();
    // The candidate's recorded table10 is the native implicit one-tier table. Other tables are not
    // admission inputs; physical capacity instead uses each cross position's explicit stored leverage.
    markets.tables = vec![FxHashMap::default(); 2];
    markets.tables[1].insert(10, MarginTable::single(10).unwrap());
    let routes = routing::RoutingProjection::seeds(&checkpoint, &markets);
    let mut account = AccountMargin::of(&checkpoint.accounts[&who], &markets);
    account.routing = Some(routes[&who].clone());
    (case, MarginStates { markets, accounts: [(who, account)].into_iter().collect() }, who)
}

fn softbank_fill(case: &serde_json::Value) -> FillEvent {
    let event = &case["events"][0];
    let wire: Fill = serde_json::from_value(event["fill"].clone()).unwrap();
    let mut fill = FillEvent::at_mark(&wire, Some(recorded_range(&event["mark"]))).unwrap();
    fill.collateral_marks = Some(Arc::new(event["collateral"].as_object().unwrap().iter()
        .map(|(coin, price)| (coin.clone(), recorded_range(price))).collect()));
    fill
}

#[test]
fn softbank_complete_window_caps_first_maker_by_executing_dex_capacity() {
    let (case, mut states, who) = softbank_source();
    let fill = softbank_fill(&case);
    let account = &states.accounts[&who];
    let capacity = account.routing.as_ref().unwrap().anchored_cross_capacity(&fill, 14_638_714, &account.cross_capacity, Some(4_659_033_447)).unwrap();
    assert_eq!(capacity, Some(3_668_831));
    let height = case["events"][0]["height"].as_u64().unwrap();
    states.apply(height, who, &AccountEvent::Fill(fill));
    assert_eq!(states.accounts[&who].isolated[&(1, 43)], 29_317_175 * WEI_PER_MICRO);
    assert!(states.accounts[&who].routing.is_none(), "a first maker consumes the checkpoint physical-balance proof");
    for event in case["events"].as_array().unwrap().iter().skip(1) {
        assert!(event.get("funding").is_none() && event.get("action").is_none());
        let wire: Fill = serde_json::from_value(event["fill"].clone()).unwrap();
        states.apply(event["height"].as_u64().unwrap(), who, &AccountEvent::Fill(FillEvent::of(&wire).unwrap()));
    }
    let account = &states.accounts[&who];
    assert!(account.stop.is_none(), "{:?}", account.stop);
    let mut cash = case["after"]["spot_wei"].as_i64().unwrap();
    let mut expected = FxHashMap::default();
    for dex in case["after"]["dex_states"].as_array().unwrap() {
        cash += dex["cash_micro"].as_i64().unwrap() * WEI_PER_MICRO;
        for (coin, raw) in dex["positions"].as_object().unwrap() {
            if raw.get("s").is_none_or(|s| s.as_i64().unwrap() == 0) { continue; }
            if let Some(isolated) = raw["l"].get("I") {
                let market = &states.markets.perps[coin];
                expected.insert((market.dex, market.asset), isolated["u"].as_i64().unwrap() * WEI_PER_MICRO);
            }
        }
    }
    assert_eq!(account.isolated, expected, "all isolated cash, not only the focal position");
    assert_eq!(account.pools, Pools::Unified(cash));
    assert!(account.estimated, "native scalar admission does not prove the complete Unified wrapper");
    let (old, _) = replay_cross_dex(&case, false);
    assert_eq!(old.isolated[&(1, 43)] - account.isolated[&(1, 43)], 1_155 * WEI_PER_MICRO);
}

#[test]
fn softbank_physical_capacity_expires_on_boundary_and_requires_complete_prices() {
    for variant in 0..11 {
        let (case, mut states, who) = softbank_source();
        let mut fill = softbank_fill(&case);
        let account = states.accounts.get_mut(&who).unwrap();
        match variant {
            0 => routing::apply_update(account, 1, &routing::RoutingUpdate::Unavailable),
            1 => routing::apply_update(account, 1, &routing::RoutingUpdate::OrdinaryIoc),
            2 => fill.collateral_marks = None,
            3 => { Arc::make_mut(fill.collateral_marks.as_mut().unwrap()).get_mut("xyz:META").unwrap().max_px_raw += 1; },
            4 => account.cross_capacity.get_mut(&1).unwrap().advanced = true,
            5 => fill.crossed = true,
            6 => fill.sz = "0.402".into(),
            7 => fill.mark = None,
            8 => { Arc::make_mut(fill.collateral_marks.as_mut().unwrap()).remove("xyz:META"); },
            9 => { Arc::make_mut(fill.collateral_marks.as_mut().unwrap()).get_mut("xyz:META").unwrap().size_decimals = 2; },
            10 => fill.mark = Some(FillMarkRange { min_px_raw: 50_000, max_px_raw: 50_000, size_decimals: 3 }),
            _ => unreachable!(),
        }
        let capacity = account.routing.as_ref().and_then(|route|
            route.anchored_cross_capacity(&fill, 14_638_714, &account.cross_capacity, Some(4_659_033_447)).unwrap());
        assert_eq!(capacity, None, "variant {variant} cannot retain a checkpoint physical cap");
    }
    let (case, mut states, who) = softbank_source();
    states.apply(1, who, &AccountEvent::Action(BlockAction::Withdraw { amount_wei: 0 }));
    assert!(states.accounts[&who].routing.is_none(), "cash actions invalidate the anchor even before a later fill");
    let (_, mut funding_states, funding_user) = softbank_source();
    funding_states.apply(1, funding_user, &AccountEvent::Funding { coin: "xyz:SOFTBANK".into(), amount_wei: 100 });
    assert!(funding_states.accounts[&funding_user].routing.is_none(), "even own-position funding consumes the first-maker boundary");
    let mut fill = softbank_fill(&case);
    fill.start_position = "-0.405".into();
    let (_, states, who) = softbank_source();
    let a = &states.accounts[&who];
    assert_eq!(a.routing.as_ref().unwrap().anchored_cross_capacity(&fill, 14_638_714, &a.cross_capacity, Some(4_659_033_447)).unwrap(), None);
}

fn assert_recorded_cash(case: &serde_json::Value, account: &AccountMargin, markets: &Markets) {
    let mut cash = case["after"]["spot_wei"].as_i64().unwrap();
    let mut isolated = FxHashMap::default();
    for dex in case["after"]["dex_states"].as_array().unwrap() {
        cash += dex["cash_micro"].as_i64().unwrap() * WEI_PER_MICRO;
        for (coin, position) in dex["positions"].as_object().unwrap() {
            if position.get("s").is_none_or(|s| s.as_i64().unwrap() == 0) { continue; }
            if let Some(margin) = position["l"].get("I") {
                let market = &markets.perps[coin];
                isolated.insert((market.dex, market.asset), margin["u"].as_i64().unwrap() * WEI_PER_MICRO);
            }
        }
    }
    assert_eq!(account.isolated, isolated, "{}", case["window"]);
    assert_eq!(account.pools, Pools::Unified(cash), "{}", case["window"]);
}

#[test]
fn eur_delayed_system_send_funds_the_recorded_four_fill_window() {
    use crate::accounts::cash_order::ledger::{PreBundleLedger, tests::{golden, block}};
    let case = golden();
    let who: Address = case["user"].as_str().unwrap().parse().unwrap();
    let (source, mut markets) = cross_dex_source(&case);
    markets.dexes.insert(String::new(), 0);
    let mut results = Vec::new();
    for reorder in [false, true] {
        let mut account = AccountMargin::of(&source, &markets);
        for raw in case["blocks"].as_array().unwrap() {
            let height = raw["height"].as_u64().unwrap();
            let (actions, fills, mut misc, inputs) = block(raw);
            let early = PreBundleLedger::of(&actions, &fills, &misc, &inputs, reorder).take(&mut misc, true);
            for batch in early {
                for event in batch.events() {
                    let crate::accounts::misc::MiscEvent::Ledger { delta, .. } = event else { panic!() };
                    apply_ledger(&mut account, &markets, height, who, &delta).unwrap();
                }
            }
            for event in case["events"].as_array().unwrap().iter().filter(|e| e["height"].as_u64() == Some(height)) {
                let fill: Fill = serde_json::from_value(event["fill"].clone()).unwrap();
                apply_perp_fill(&mut account, &markets, height, &FillEvent::at_mark(&fill, Some(recorded_range(&event["mark"]))).unwrap()).unwrap();
            }
            for batch in misc {
                for event in batch.events() {
                    let crate::accounts::misc::MiscEvent::Ledger { delta, .. } = event else { panic!() };
                    apply_ledger(&mut account, &markets, height, who, &delta).unwrap();
                }
            }
        }
        if reorder { assert_recorded_cash(&case, &account, &markets); }
        results.push(account);
    }
    assert_eq!(results[1].isolated[&(1, 25)] - results[0].isolated[&(1, 25)], 849_373_668 * WEI_PER_MICRO);
    assert!(results[1].estimated, "corrected event order does not prove the Unified physical sweep path");
}

#[test]
fn near_complete_flip_windows_preserve_signed_cash_and_spot_dust() {
    for raw in [include_str!("near_flip_a.json"), include_str!("near_flip_b.json")] {
        let fixture: serde_json::Value = serde_json::from_str(raw).unwrap();
        let case = &fixture["case"];
        let (account, markets) = replay_cross_dex(case, false);
        assert_recorded_cash(case, &account, &markets);
        assert!(account.estimated);
    }
}

#[test]
fn repeated_closed_isolated_leverage_retains_cross_capacity() {
    let case: serde_json::Value = serde_json::from_str(include_str!("nil_idempotent_leverage_golden.json")).unwrap();
    for already_unknown in [false, true] {
        let (mut anchor, mut markets) = source(&case);
        anchor.mode = AccountMode::Default;
        let key = (0, markets.perps["NIL"].asset);
        let raw = &case["leverage_action"]["action"];
        let action_asset = u32::try_from(raw["asset"].as_u64().unwrap()).unwrap();
        markets.action_assets.insert(action_asset, "NIL".into());
        let holder = case["provenance"]["user"].as_str().unwrap().parse::<Address>().unwrap();
        let mut account = AccountMargin::of(&anchor, &markets);
        assert!(!account.isolated.contains_key(&key), "NIL is closed with a saved3x setting");
        assert_eq!(account.isolated_leverage[&key], 3);
        if already_unknown { account.cross_capacity.remove(&0); }
        let mut states = MarginStates { markets, accounts: [(holder, account)].into_iter().collect() };
        let action = BlockAction::Leverage { asset: action_asset, is_cross: raw["isCross"].as_bool().unwrap(),
            leverage: u32::try_from(raw["leverage"].as_u64().unwrap()).unwrap() };
        let action_height = case["leverage_action"]["height"].as_u64().unwrap();
        for _ in 0..2 {
            assert!(states.apply(action_height, holder, &AccountEvent::Action(action.clone())));
            assert_eq!(states.accounts[&holder].cross_capacity.contains_key(&0), !already_unknown,
                "a no-op preserves an existing proof and never recreates a missing one");
        }
        for row in case["events"].as_array().unwrap() {
            let wire: Fill = serde_json::from_value(row["fill"].clone()).unwrap();
            let requested = states.collateral_coins([crate::types::node_data::NodeDataFill(holder, wire.clone())].iter());
            assert_eq!(requested.len(), if already_unknown { 0 } else { 10 });
            let mut fill = FillEvent::at_mark(&wire, Some(FillMarkRange {
                min_px_raw: row["mark_raw"].as_u64().unwrap(), max_px_raw: row["mark_raw"].as_u64().unwrap(), size_decimals: 0,
            })).unwrap();
            if !already_unknown {
                let prices = row["collateral"].as_object().unwrap().iter().map(|(coin, px)| (coin.clone(), FillMarkRange {
                    min_px_raw: px.as_u64().unwrap(), max_px_raw: px.as_u64().unwrap(), size_decimals: states.markets.perps[coin].size_decimals,
                })).collect();
                fill.collateral_marks = Some(Arc::new(prices));
                let account = &states.accounts[&holder];
                let Pools::PerDex(cash) = &account.pools else { unreachable!() };
                assert_eq!(account.cross_capacity[&0].available_micro(cash[&0], fill.collateral_marks.as_ref().unwrap()).unwrap(),
                    Some(row["capacity_micro"].as_i64().unwrap()));
            }
            assert!(states.apply(row["height"].as_u64().unwrap(), holder, &AccountEvent::Fill(fill)));
        }
        let account = &states.accounts[&holder];
        assert!(account.stop.is_none());
        let error = if already_unknown { case["provenance"]["original_error_micro"].as_i64().unwrap() } else { 0 };
        assert_eq!(account.isolated[&key], (case["after"]["isolated_micro"].as_i64().unwrap() + error) * WEI_PER_MICRO);
        assert_eq!(account.pools, Pools::PerDex([(0, (case["after"]["cash_micro"].as_i64().unwrap() - error) * WEI_PER_MICRO)].into_iter().collect()));
        assert_eq!(account.estimated, already_unknown);
        // The new proof is deliberately limited to a closed position. Active-position leverage handling
        // retains its existing conservative invalidation, even for the same numerical setting.
        assert!(states.apply(1196045897, holder, &AccountEvent::Action(action)));
        assert!(!states.accounts[&holder].cross_capacity.contains_key(&0));
    }
}

#[test]
fn btc_closed_leverage_switches_preserve_capacity_across_the_complete_window() {
    let case: serde_json::Value = serde_json::from_str(include_str!("btc_closed_leverage_capacity_golden.json")).unwrap();
    let (account, markets) = replay_cross_dex(&case, false);
    assert_recorded_cash(&case, &account, &markets);
    assert!(account.estimated, "initial spot and Unified physical routing remain a separate proof boundary");
    let (unknown, _) = replay_cross_dex(&case, true);
    assert_eq!(unknown.isolated[&(0, 0)] - account.isolated[&(0, 0)], 57_205 * WEI_PER_MICRO,
        "discarding the complete portfolio loses the last fill's funding cap");
    assert!(unknown.estimated);
}

#[test]
fn closed_leverage_projection_preserves_other_holdings_but_never_recovers_unknown_state() {
    let case: serde_json::Value = serde_json::from_str(include_str!("btc_closed_leverage_capacity_golden.json")).unwrap();
    let (source, markets) = cross_dex_source(&case);
    let closed = |account: &mut AccountMargin| { account.isolated.clear(); };
    for unknown in [false, true] {
        let mut account = AccountMargin::of(&source, &markets);
        closed(&mut account);
        if unknown { account.cross_capacity.clear(); }
        for (height, cross, leverage) in [(1, true, 20), (2, true, 30), (3, false, 15), (4, false, 10)] {
            apply_action(&mut account, &markets, height, &BlockAction::Leverage { asset: 0, is_cross: cross, leverage }).unwrap();
            assert_eq!(account.cross_capacity.contains_key(&0), !unknown);
            if !unknown {
                let holdings = &account.cross_capacity[&0].holdings;
                assert_eq!(holdings.len(), usize::from(cross));
                if cross { assert_eq!((holdings[0].size_raw, holdings[0].leverage), (0, leverage)); }
            }
        }
    }
    let mut account = AccountMargin::of(&source, &markets);
    closed(&mut account);
    let other = cross_capacity::Holding { coin: "OTHER".into(), size_raw: -8, leverage: 5, size_decimals: 2 };
    account.cross_capacity.get_mut(&0).unwrap().holdings.push(other.clone());
    apply_action(&mut account, &markets, 10, &BlockAction::Leverage { asset: 0, is_cross: false, leverage: 15 }).unwrap();
    assert_eq!(account.cross_capacity[&0].holdings, [other]);
    // Active isolated or cross exposure cannot use the closed-position proof.
    for cross in [false, true] {
        let mut account = AccountMargin::of(&source, &markets);
        if cross {
            closed(&mut account);
            account.cross_capacity.get_mut(&0).unwrap().holdings.push(cross_capacity::Holding {
                coin: "BTC".into(), size_raw: -22_599, leverage: 40, size_decimals: 5,
            });
        }
        apply_action(&mut account, &markets, 20, &BlockAction::Leverage { asset: 0, is_cross: cross, leverage: 15 }).unwrap();
        assert!(!account.cross_capacity.contains_key(&0));
    }
    let mut account = AccountMargin::of(&source, &markets);
    closed(&mut account);
    account.isolated_leverage.clear();
    apply_action(&mut account, &markets, 30, &BlockAction::Leverage { asset: 0, is_cross: true, leverage: 20 }).unwrap();
    let raw: Fill = serde_json::from_value(case["events"].as_array().unwrap().iter()
        .find(|e| e["height"] == 1196393892_u64).unwrap()["fill"].clone()).unwrap();
    let fill = FillEvent::of(&raw).unwrap();
    assert_eq!(apply_perp_fill(&mut account, &markets, 30, &fill), Err(StopReason::IsolatedFill),
        "same-block fill/setting order cannot use the new divisor");
    assert!(apply_perp_fill(&mut account, &markets, 31, &fill).is_ok());
    assert!(account.cross_capacity.contains_key(&0));
    assert!(apply_perp_fill(&mut account, &markets, 32, &fill).is_ok());
    assert!(!account.cross_capacity.contains_key(&0), "repeated startPosition must withdraw the portfolio");
}

#[test]
fn zhipu_closed_leverage_preserves_foreign_collateral_capacity() {
    let case: serde_json::Value = serde_json::from_str(include_str!("zhipu_closed_leverage_capacity_golden.json")).unwrap();
    let (account, markets) = replay_cross_dex(&case, false);
    assert_recorded_cash(&case, &account, &markets);
    assert!(account.estimated, "cross-dex cash movement retains the Unified physical-routing boundary");
    let (unknown, _) = replay_cross_dex(&case, true);
    let market = &markets.perps["xyz:ZHIPU"];
    assert_eq!(unknown.isolated[&(market.dex, market.asset)] - account.isolated[&(market.dex, market.asset)],
        1_071 * WEI_PER_MICRO, "discarding one dex invalidates the combined collateral proof");
    assert!(unknown.estimated);
}
