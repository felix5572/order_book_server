//! A reservation bound for one isolated execution market with untouched, zero-position order trackers on
//! other USDC dexes. It bounds aggregate collateral; it does not replay general Unified physical routing.
use alloy::primitives::Address;
use crate::accounts::{checkpoint::{Account, CheckpointAccounts, PositionMargin}, order_reserve::{Reserve, retained_micro}};
use super::{FundingPath, Markets, RoutingProjection};

impl RoutingProjection {
    pub(super) fn other_dex_seed(cp: &CheckpointAccounts, markets: &Markets, user: &Address, account: &Account) -> Option<Self> {
        if !account.lending.is_empty() || account.dex_states.iter().any(|state| !markets.usdc_dexes[state.dex]
            || state.cash_micro < 0 || state.positions.iter().any(|p| !matches!(p.margin, PositionMargin::Isolated { .. }))) {
            return None;
        }
        let mut candidates = account.dex_states.iter().filter_map(|state| {
            let [position] = state.positions.as_slice() else { return None; };
            let PositionMargin::Isolated { leverage, .. } = position.margin else { return None; };
            let tracker = cp.order_trackers[state.dex].as_ref()?.get(user);
            if tracker.is_some_and(|t| t.reserve_micro != 0 || !t.assets.is_empty()) { return None; }
            let locked_micro = locked_other_dexes(cp, markets, user, account, state.dex)?;
            if locked_micro == 0 { return None; }
            let coin = &markets.asset_names[state.dex][position.asset];
            Some(Self { coin: coin.clone(), decimals: markets.perps[coin].size_decimals,
                reserve: Reserve { notional_micro: [0, 0], size_raw: [0, 0], position_raw: position.size_raw,
                    price_raw: 0, leverage }, cash_micro: None, last_fill_block: None, tracker_trusted: true,
                path: FundingPath::OtherDexReserve { locked_micro, ioc_block: None } })
        });
        let result = candidates.next()?;
        // No arbitrary choice between execution markets: their funding dependencies differ.
        candidates.next().is_none().then_some(result)
    }
}

fn locked_other_dexes(cp: &CheckpointAccounts, markets: &Markets, user: &Address, account: &Account, executing: usize) -> Option<i64> {
    let mut locked = 0_i64;
    for (dex, section) in cp.order_trackers.iter().enumerate().filter(|(dex, _)| *dex != executing) {
        let Some(tracker) = section.as_ref()?.get(user) else { continue; };
        if tracker.assets.is_empty() && tracker.reserve_micro == 0 { continue; }
        if !markets.usdc_dexes[dex] { return None; }
        let state = account.dex_states.iter().find(|state| state.dex == dex);
        let mut required = 0_i64;
        for (asset_id, reserve) in &tracker.assets {
            let asset = usize::try_from(asset_id.checked_sub(cp.dexes[dex].pdi.checked_mul(10_000)?)?).ok()?;
            // With zero position the order requirement is independent of cached mark changes. Any position,
            // order, funding, ledger or leverage change on this dex discards the projection before more fills.
            if reserve.position_raw != 0 || asset >= markets.asset_names[dex].len()
                || state.is_some_and(|s| s.positions.iter().any(|p| p.asset == asset && p.size_raw != 0)) { return None; }
            required = required.checked_add(reserve.required_micro()?)?;
        }
        if required != tracker.reserve_micro { return None; }
        let cash = state.map_or(0, |state| state.cash_micro);
        locked = locked.checked_add(retained_micro(required)?.min(cash))?;
    }
    Some(locked)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::{checkpoint::{DexAccount, Position, OrderTrackerSnapshot}, replay::{AccountMargin, Pools,
        apply_perp_fill, FillEvent, FillMarkRange, PerpMarket, AssetMarginMode}, usdc::WEI_PER_MICRO};
    use super::super::{RoutingUpdate, apply_update};
    use crate::types::Fill;

    fn recorded() -> serde_json::Value { serde_json::from_str(include_str!("../gold_other_dex_golden.json")).unwrap() }

    // Native account quantities, cash and tracker are preserved. The unrelated metadata/test tables are scaffolding;
    // this regression tests the typed cash projection, not full checkpoint parsing or a live wrapper execution.
    fn seeded() -> (CheckpointAccounts, Markets, Address) {
        let (mut cp, mut markets, who) = super::super::tests::checkpoint();
        let golden = recorded();
        let source = cp.accounts.get_mut(&who).unwrap();
        source.spot = vec![(0, 17_116_302_300)];
        source.dex_states = ["main_state", "executing_state"].into_iter().enumerate().map(|(dex, key)| {
            let state = &golden["before"][key];
            let mut positions = Vec::new();
            let mut settings = Vec::new();
            for row in state["p"]["p"].as_array().unwrap() {
                let asset = row[0].as_u64().unwrap() as usize % 10_000;
                let p = &row[1];
                let leverage = p["l"]["I"]["l"].as_u64().unwrap() as u32;
                let size_raw = p["s"].as_i64().unwrap_or(0);
                if size_raw == 0 { settings.push((asset, leverage)); continue; }
                positions.push(Position { asset, size_raw, table_id: p["M"].as_u64().unwrap() as u32,
                    margin: PositionMargin::Isolated { leverage, cash_micro: p["l"]["I"]["u"].as_i64().unwrap() } });
            }
            DexAccount { dex, cash_micro: state["u"].as_i64().unwrap_or(0), positions, isolated_settings: settings }
        }).collect();
        let tracker: OrderTrackerSnapshot = serde_json::from_value(golden["before"]["main_tracker"].clone()).unwrap();
        cp.order_trackers[0].as_mut().unwrap().insert(who, tracker);
        markets.asset_names[0].resize(128, "unused fixture slot".into());
        markets.asset_names[1] = vec!["unused fixture slot".into(); 4];
        markets.asset_names[1][3] = "xyz:GOLD".into();
        markets.perps.insert("xyz:GOLD".into(), PerpMarket { dex: 1, asset: 3, size_decimals: 4,
            margin_mode: AssetMarginMode::Normal, default_leverage: 20 });
        (cp, markets, who)
    }

    #[test]
    fn gold_six_native_steps_preserve_frozen_other_dex_cash_and_close_recorded_endpoint() {
        let golden = recorded();
        for with_reserve in [false, true] {
            let (cp, markets, who) = seeded();
            let source = &cp.accounts[&who];
            let mut account = AccountMargin::of(source, &markets);
            if with_reserve {
                account.routing = RoutingProjection::seeds(&cp, &markets).remove(&who);
                assert!(matches!(account.routing.as_ref().unwrap().path,
                    FundingPath::OtherDexReserve { locked_micro: 456_515_934, ioc_block: None }));
                apply_update(&mut account, 1_194_477_717, &RoutingUpdate::OrdinaryIoc);
            }
            let total = |account: &AccountMargin| {
                let Pools::Unified(cash) = account.pools else { panic!("Unified fixture") };
                cash + account.isolated.values().sum::<i64>()
            };
            for (index, row) in golden["fills"].as_array().unwrap().iter().enumerate() {
                let fill: Fill = serde_json::from_value(row["fill"].clone()).unwrap();
                let event = FillEvent::at_mark(&fill, Some(FillMarkRange { min_px_raw: 399_980, max_px_raw: 399_980, size_decimals: 4 })).unwrap();
                let before = total(&account);
                apply_perp_fill(&mut account, &markets, 1_194_477_717, &event).unwrap();
                assert_eq!(total(&account), before + event.cash_wei(4, 2).unwrap());
                if with_reserve {
                    let step = &golden["native_steps"][index];
                    assert_eq!(account.isolated[&(1, 3)] / WEI_PER_MICRO, step["isolated_micro"].as_i64().unwrap());
                    assert_eq!(account.pools, Pools::Unified((456_515_934 + step["cash_micro"].as_i64().unwrap()) * WEI_PER_MICRO));
                }
            }
            let difference = if with_reserve { 0 } else { 14_459_715 };
            assert_eq!(account.isolated[&(1, 3)] / WEI_PER_MICRO, 11_479_565_555 + difference);
            assert!(account.estimated, "an available-cash bound does not prove general physical routing");
        }
    }

    #[test]
    fn other_dex_bound_requires_complete_zero_position_trackers_and_cash_only_capacity() {
        let (cp, markets, who) = seeded();
        assert!(RoutingProjection::seeds(&cp, &markets).contains_key(&who));
        for variant in 0..8 {
            let (mut cp, mut markets, who) = seeded();
            match variant {
                0 => cp.order_trackers[0] = None,
                1 => cp.order_trackers[0].as_mut().unwrap().get_mut(&who).unwrap().reserve_micro += 1,
                2 => cp.order_trackers[0].as_mut().unwrap().get_mut(&who).unwrap().assets[0].1.position_raw = 1,
                3 => cp.accounts.get_mut(&who).unwrap().dex_states[0].cash_micro = -1,
                4 => cp.accounts.get_mut(&who).unwrap().dex_states[0].positions[0].margin = PositionMargin::Cross { leverage: 4 },
                5 => { cp.spot_usdc_holds.insert(who, 1); },
                6 => cp.accounts.get_mut(&who).unwrap().dex_states[0].positions[0].asset = 0,
                7 => markets.usdc_dexes[0] = false,
                _ => unreachable!(),
            }
            assert!(!RoutingProjection::seeds(&cp, &markets).contains_key(&who), "variant {variant}");
        }
    }

    #[test]
    fn other_dex_bound_needs_an_ioc_boundary_each_block_and_is_lost_on_unknown_updates() {
        let (cp, markets, who) = seeded();
        let mut account = AccountMargin::of(&cp.accounts[&who], &markets);
        account.routing = RoutingProjection::seeds(&cp, &markets).remove(&who);
        let fill: Fill = serde_json::from_value(recorded()["fills"][0]["fill"].clone()).unwrap();
        let event = FillEvent::at_mark(&fill, None).unwrap();
        let cash = 62_767_895_700;
        assert_eq!(account.routing.as_mut().unwrap().capacity(8, &event, 0, cash), None);
        apply_update(&mut account, 8, &RoutingUpdate::OrdinaryIoc);
        assert_eq!(account.routing.as_mut().unwrap().capacity(8, &event, 0, cash), Some(171_163_023));
        assert_eq!(account.routing.as_mut().unwrap().capacity(8, &event, 0, 100), Some(0));
        assert_eq!(account.routing.as_mut().unwrap().capacity(9, &event, 0, cash), None);
        let mut maker = event; maker.crossed = false;
        assert_eq!(account.routing.as_mut().unwrap().capacity(8, &maker, 0, cash), None);
        apply_update(&mut account, 9, &RoutingUpdate::Unavailable);
        apply_update(&mut account, 10, &RoutingUpdate::OrdinaryIoc);
        assert!(account.routing.is_none(), "no event can restore a discarded checkpoint tracker");
    }
}
