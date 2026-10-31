//! A one-fill capacity bound for a Unified account whose only open perp is isolated, with all dex cash zero
//! and all perp trackers empty. Spot USDC held by untouched orders is not spendable. Any competing order,
//! cash event, funding or input gap discards the projection; a later checkpoint must establish it again.
use alloy::primitives::Address;
use crate::accounts::{checkpoint::{Account, CheckpointAccounts, PositionMargin}, order_reserve::Reserve, portfolio::USDC};
use super::{FundingPath, Markets, RoutingProjection};

impl RoutingProjection {
    pub(super) fn frozen_spot_seed(cp: &CheckpointAccounts, markets: &Markets, user: &Address, account: &Account) -> Option<Self> {
        let held_wei = i64::try_from(*cp.spot_usdc_holds.get(user)?).ok()?;
        let spot_wei = account.spot.iter().find(|(token, _)| *token == USDC)?.1;
        if held_wei <= 0 || held_wei > spot_wei || !account.lending.is_empty()
            || account.dex_states.iter().any(|state| !markets.usdc_dexes[state.dex] || state.cash_micro != 0)
            || cp.order_trackers.iter().any(|section| section.as_ref().is_none_or(|users|
                users.get(user).is_some_and(|tracker| tracker.reserve_micro != 0 || !tracker.assets.is_empty()))) { return None; }
        let mut positions = account.dex_states.iter().flat_map(|state| state.positions.iter().map(move |position| (state.dex, position)));
        let (dex, position) = positions.next()?;
        if positions.next().is_some() || position.size_raw == 0 { return None; }
        let PositionMargin::Isolated { leverage, .. } = position.margin else { return None; };
        let coin = &markets.asset_names[dex][position.asset];
        Some(Self { coin: coin.clone(), decimals: markets.perps[coin].size_decimals,
            reserve: Reserve { notional_micro: [0, 0], size_raw: [0, 0], position_raw: position.size_raw,
                price_raw: 0, leverage }, cash_micro: None, last_fill_block: None, tracker_trusted: true,
            path: FundingPath::FrozenSpot { held_wei, execution: None } })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{accounts::{checkpoint::{DexAccount, OrderTrackerSnapshot}, replay::{AccountMargin, AccountMode,
        AssetMarginMode, FillEvent, FillMarkRange, MarginStates, PerpMarket, Pools, apply_perp_fill,
        routing::{apply_update, RoutingUpdate, tests::checkpoint}}, usdc::WEI_PER_MICRO}, types::Fill};

    fn recorded() -> serde_json::Value { serde_json::from_str(include_str!("../eth_frozen_spot_golden.json")).unwrap() }

    fn seeded() -> (CheckpointAccounts, Markets, Address) {
        let (mut cp, mut markets, who) = checkpoint();
        let source = cp.accounts.get_mut(&who).unwrap();
        source.spot = vec![(USDC, 19_195_642_707), (150, 150_000_000)];
        source.dex_states[0].cash_micro = 0;
        let position = &mut source.dex_states[0].positions[0];
        position.size_raw = 1726;
        position.margin = PositionMargin::Isolated { leverage: 25, cash_micro: -386_112_040 };
        cp.spot_usdc_holds.insert(who, 14_473_500_000);
        // Keep the checkpoint scaffold's dex/asset indices; the ETH fill arithmetic uses recorded precision.
        markets.asset_names[1][0] = "ETH".into();
        markets.perps.insert("ETH".into(), PerpMarket { dex: 1, asset: 0, size_decimals: 4,
            margin_mode: AssetMarginMode::Normal, max_leverage: 20 });
        (cp, markets, who)
    }

    #[test]
    fn eth_recorded_flip_excludes_frozen_spot_and_conserves_cash_even_without_a_mark() {
        let golden = recorded();
        let fill: Fill = serde_json::from_value(golden["recorded_fill"].clone()).unwrap();
        for mark in [None, Some(230_000), Some(235_470), Some(235_480), Some(240_000)] {
            let (cp, markets, who) = seeded();
            let mut account = AccountMargin::of(&cp.accounts[&who], &markets);
            account.routing = RoutingProjection::seeds(&cp, &markets).remove(&who);
            apply_update(&mut account, 1_196_963_557, &RoutingUpdate::TriggeredTaker { tid: fill.tid });
            let event = FillEvent::at_mark(&fill, mark.map(|value| FillMarkRange {
                min_px_raw: value, max_px_raw: value, size_decimals: 4 })).unwrap();
            let before = 19_195_642_707 - 386_112_040 * WEI_PER_MICRO;
            apply_perp_fill(&mut account, &markets, 1_196_963_557, &event).unwrap();
            let isolated = account.isolated[&(1, 0)];
            assert_eq!(isolated / WEI_PER_MICRO, golden["position_after"]["l"]["I"]["u"].as_i64().unwrap());
            assert_eq!(account.pools, Pools::Unified(14_473_500_007));
            assert_eq!(isolated + 14_473_500_007, before + event.cash_wei(4, 2).unwrap());
            assert!(account.estimated, "this is a bounded estimate, not a proof of the general transfer scheduler");
            assert!(account.routing.is_none(), "consumed once; do not reuse holds after a fill");
        }
        let (cp, markets, who) = seeded();
        let mut control = AccountMargin::of(&cp.accounts[&who], &markets);
        let event = FillEvent::at_mark(&fill, Some(FillMarkRange { min_px_raw: 235_480, max_px_raw: 235_480, size_decimals: 4 })).unwrap();
        apply_perp_fill(&mut control, &markets, 1_196_963_557, &event).unwrap();
        assert_eq!(control.isolated[&(1, 0)] / WEI_PER_MICRO, 1_748_825_187, "inferred-mark arithmetic control reproduces the 705288-micro split");
    }

    #[test]
    fn frozen_spot_seed_rejects_unobserved_or_competing_capacity() {
        for variant in 0..9 {
            let (mut cp, mut markets, who) = seeded();
            let source = cp.accounts.get_mut(&who).unwrap();
            match variant {
                0 => cp.order_trackers[0] = None,
                1 => { cp.spot_usdc_holds.insert(who, u64::MAX); },
                2 => source.dex_states[0].cash_micro = 1,
                3 => source.dex_states[0].positions[0].margin = PositionMargin::Cross { leverage: 25 },
                4 => { let position = source.dex_states[0].positions[0].clone(); source.dex_states[0].positions.push(position); },
                5 => { cp.order_trackers[0].as_mut().unwrap().insert(who, OrderTrackerSnapshot { reserve_micro: 1, assets: vec![] }); },
                6 => markets.usdc_dexes[1] = false,
                7 => source.mode = AccountMode::Default,
                8 => source.dex_states.push(DexAccount { dex: 0, cash_micro: -1, positions: vec![], isolated_settings: vec![] }),
                _ => unreachable!(),
            }
            assert!(!RoutingProjection::seeds(&cp, &markets).contains_key(&who), "variant {variant}");
        }
    }

    #[test]
    fn frozen_spot_capacity_requires_the_exact_fill_and_loses_stale_evidence() {
        let (cp, markets, who) = seeded();
        let fill: Fill = serde_json::from_value(recorded()["recorded_fill"].clone()).unwrap();
        let event = FillEvent::at_mark(&fill, None).unwrap();
        let fresh = || {
            let mut account = AccountMargin::of(&cp.accounts[&who], &markets);
            account.routing = RoutingProjection::seeds(&cp, &markets).remove(&who);
            account
        };
        let mut account = fresh();
        assert_eq!(account.routing.as_mut().unwrap().capacity(8, &event, 0, 19_195_642_707), None);
        apply_update(&mut account, 8, &RoutingUpdate::TriggeredTaker { tid: fill.tid });
        assert_eq!(account.routing.as_mut().unwrap().capacity(8, &event, 0, 19_195_642_707), Some(47_221_427));
        assert_eq!(account.routing.as_mut().unwrap().capacity(9, &event, 0, 19_195_642_707), None);
        let mut wrong_fill = event.clone(); wrong_fill.tid += 1;
        assert_eq!(account.routing.as_mut().unwrap().capacity(8, &wrong_fill, 0, 19_195_642_707), None);
        assert!(account.routing.as_ref().unwrap().expires_on_funding("ETH"));
        for update in [RoutingUpdate::Unavailable, RoutingUpdate::OrdinaryIoc,
            RoutingUpdate::Orders { changes: vec![], mark_raw: None, prefund: false }] {
            let mut account = fresh();
            apply_update(&mut account, 7, &update);
            apply_update(&mut account, 8, &RoutingUpdate::TriggeredTaker { tid: fill.tid });
            assert!(account.routing.is_none(), "discarded holds cannot be revived by a later receipt");
        }
        // Checkpoint loading can lag the stream. Keep observing this account, but do not trust the first
        // checkpoint's holds until its account had routing coverage through a complete interval.
        let mut states = MarginStates { markets: markets.clone(), accounts: [(who, fresh())].into_iter().collect() };
        states.restrict_routing_coverage(None);
        let account = states.accounts.get_mut(&who).unwrap();
        apply_update(account, 8, &RoutingUpdate::TriggeredTaker { tid: fill.tid });
        assert!(account.routing.is_none());
    }
}
