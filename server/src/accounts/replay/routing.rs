//! Scoped Unified funding projections: executing-dex maker cash, retained-cash sufficiency, and ordinary IOC
//! capacity excluding untouched other-dex reservations. Every path preserves the estimated boundary.
//! The aggregate pool remains the cash authority. This projection distinguishes a resting order's retained cash
//! from spot cash. Native 6911b792: 0x44c9000 reserve, 0x3c92a80 101% retention, 0x5b370f0 order prefund/sweep.
//! Maker admission uses the observed fee as a rate estimate; its caller's encoding and deferred sweep sets are
//! not fully recovered. Consequently transfers using this projection remain cashEstimated. Unknown boundaries
//! discard physical capacity, never invent a per-fill/per-block sweep or treat aggregate cash as physical cash.

use rustc_hash::FxHashMap;
use alloy::primitives::Address;
use crate::accounts::order_reserve::Reserve;
use crate::{accounts::{checkpoint::{AccountMode, CheckpointAccounts, PositionMargin}, size::parse_size_raw,
    usdc::WEI_PER_MICRO}, order_book::Side};
use super::{AccountMargin, FillEvent, Markets, Pools};

mod other_dex;
mod frozen_spot;
mod anchor_cross;
mod reserved_maker;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RoutingUpdate {
    /// A whole block with only receipt-backed resting/cancel updates for this account and no cash event/fill.
    Orders { changes: Vec<OrderChange>, mark_raw: Option<u64>, prefund: bool },
    /// Receipt/status-backed ordinary IOC; only the other-dex reservation bound survives this boundary.
    OrdinaryIoc,
    /// One status-backed automatic trailing-stop fill; only an untouched frozen-spot bound may survive.
    TriggeredTaker { tid: u64 },
    /// Competing cash/order events or missing attribution: forget capacity and tracker until the next anchor.
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OrderChange {
    pub(crate) coin: String,
    pub(crate) side: Side,
    pub(crate) px: String,
    pub(crate) size: String,
    pub(crate) remove: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FundingPath {
    RestingMaker,
    /// Spot hold at the checkpoint, usable once at an attributed trigger boundary. Never a live hold cache.
    FrozenSpot { held_wei: i64, execution: Option<(u64, u64)> },
    /// Lower bound on already retained cash; never predicts a replenishment or sweep timestamp.
    ReservedMaker,
    /// Checkpoint executing-dex cash, valid only until its first observed routing boundary/fill.
    AnchoredCrossMaker { dex: usize },
    /// Known locked funds bound available collateral; they do not establish physical prefunding/sweep order.
    OtherDexReserve { locked_micro: i64, ioc_block: Option<u64> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RoutingProjection {
    coin: String,
    decimals: u32,
    reserve: Reserve,
    /// Exact observed/projected cash, or a conservative lower bound for ReservedMaker.
    /// `None` distinguishes unobserved deferred routing from a known empty executing balance.
    cash_micro: Option<i64>,
    last_fill_block: Option<u64>,
    tracker_trusted: bool,
    path: FundingPath,
}

impl RoutingProjection {
    pub(super) fn seeds(checkpoint: &CheckpointAccounts, markets: &Markets) -> FxHashMap<Address, Self> {
        let mut result = FxHashMap::default();
        // Old archive fixtures omit this entire section. Missing is not an empty tracker.
        if checkpoint.order_trackers.iter().any(Option::is_none) { return result; }
        for (user, account) in &checkpoint.accounts {
            if account.mode != AccountMode::Unified { continue; }
            if checkpoint.spot_usdc_holds.get(user).copied().unwrap_or(0) != 0 {
                if let Some(route) = Self::frozen_spot_seed(checkpoint, markets, user, account) { result.insert(*user, route); }
                continue;
            }
            if let Some(route) = Self::anchored_cross_seed(checkpoint, markets, user, account) {
                result.insert(*user, route);
                continue;
            }
            if let Some(route) = Self::other_dex_seed(checkpoint, markets, user, account) {
                result.insert(*user, route);
                continue;
            }
            if let Some(route) = Self::reserved_maker_seed(checkpoint, markets, user, account) {
                result.insert(*user, route);
                continue;
            }
            let Some(dex) = AccountMargin::single_usdc_dex(account, markets) else { continue; };
            let Some(state) = account.dex_states.iter().find(|state| state.dex == dex) else { continue; };
            if state.positions.len() != 1 || state.cash_micro < 0 { continue; }
            let position = &state.positions[0];
            let PositionMargin::Isolated { leverage, .. } = position.margin else { continue; };
            if checkpoint.order_trackers.iter().enumerate().any(|(index, tracker)| index != dex &&
                tracker.as_ref().and_then(|users| users.get(user)).is_some_and(|tracker| tracker.reserve_micro != 0 || !tracker.assets.is_empty())) { continue; }
            let coin = &markets.asset_names[dex][position.asset];
            // The recovered maker limit is the isolated-only, single-tier branch. Other collateral and lending
            // can introduce routes that are not modelled by this projection.
            if !matches!(markets.perps[coin].margin_mode, super::AssetMarginMode::NoCross | super::AssetMarginMode::StrictIsolated)
                || checkpoint.dexes[dex].table(position.table_id).tiers().len() != 1
                || !account.lending.is_empty()
                || account.dex_states.iter().any(|state| state.dex != dex && (state.cash_micro != 0 || !state.positions.is_empty())) {
                continue;
            }
            let asset_id = checkpoint.dexes[dex].pdi.checked_mul(10_000).and_then(|id| id.checked_add(u32::try_from(position.asset).ok()?));
            let tracker = checkpoint.order_trackers[dex].as_ref().and_then(|users| users.get(user));
            let tracker = tracker.filter(|tracker| tracker.reserve_micro != 0 || !tracker.assets.is_empty());
            let reserve = match tracker {
                Some(tracker) if tracker.assets.len() == 1 && Some(tracker.assets[0].0) == asset_id => {
                    let reserve = &tracker.assets[0].1;
                    if reserve.position_raw != position.size_raw || reserve.leverage != leverage ||
                        reserve.required_micro() != Some(tracker.reserve_micro) { continue; }
                    reserve.clone()
                }
                None => Reserve { notional_micro: [0, 0], size_raw: [0, 0], position_raw: position.size_raw,
                    price_raw: checkpoint.dexes[dex].assets[position.asset].mark_raw.unwrap_or(0), leverage },
                _ => continue,
            };
            result.insert(*user, Self { coin: coin.clone(), decimals: markets.perps[coin].size_decimals,
                reserve, cash_micro: Some(state.cash_micro), last_fill_block: None, tracker_trusted: true, path: FundingPath::RestingMaker });
        }
        result
    }

    pub(super) fn coin(&self) -> &str { &self.coin }

    pub(super) fn expires_on_funding(&self, coin: &str) -> bool {
        self.coin != coin || matches!(self.path, FundingPath::AnchoredCrossMaker { .. } | FundingPath::ReservedMaker | FundingPath::FrozenSpot { .. })
    }

    pub(super) const fn invalidate(&mut self) { self.tracker_trusted = false; self.cash_micro = None; }

    pub(super) fn orders(&mut self, changes: &[OrderChange], mark_raw: Option<u64>, prefund: bool, aggregate_wei: i64) -> Option<()> {
        self.cash_micro = None;
        if !self.tracker_trusted || self.path != FundingPath::RestingMaker { return None; }
        for change in changes {
            if change.coin != self.coin { return None; }
            let px = u64::try_from(parse_size_raw(&change.px, 6 - self.decimals).ok()?).ok()?;
            let quantity = u64::try_from(parse_size_raw(&change.size, self.decimals).ok()?).ok()?;
            if px == 0 || quantity == 0 { return None; }
            let notional = px.checked_mul(quantity)?;
            let side = usize::from(change.side == Side::Ask);
            if change.remove {
                self.reserve.size_raw[side] = self.reserve.size_raw[side].checked_sub(quantity)?;
                self.reserve.notional_micro[side] = self.reserve.notional_micro[side].checked_sub(notional)?;
            } else {
                self.reserve.size_raw[side] = self.reserve.size_raw[side].checked_add(quantity)?;
                self.reserve.notional_micro[side] = self.reserve.notional_micro[side].checked_add(notional)?;
            }
        }
        self.reserve.price_raw = mark_raw.unwrap_or(0);
        // Ordinary ALO wrapper, only this dex/asset and no frozen spot/other reserve: fund from aggregate,
        // update the tracker, then sweep the positive excess. Cancel-only scheduling is deliberately unknown.
        if prefund && aggregate_wei >= 0 {
            self.cash_micro = self.reserve.retained_micro().map(|retain| retain.min(aggregate_wei / WEI_PER_MICRO));
            self.last_fill_block = None;
        }
        Some(())
    }

    pub(super) fn capacity(&mut self, block: u64, fill: &FillEvent, cash_before: i64, aggregate_wei: i64) -> Option<i64> {
        if !self.tracker_trusted { self.cash_micro = None; return None; }
        let capacity = match self.path {
            FundingPath::FrozenSpot { held_wei, execution: Some((height, tid)) }
                if height == block && tid == fill.tid && fill.coin == self.coin && fill.crossed && !fill.backstop =>
                aggregate_wei.checked_sub(held_wei).map(|cash| cash.max(0) / WEI_PER_MICRO),
            FundingPath::FrozenSpot { .. } => None,
            FundingPath::RestingMaker => self.maker_capacity(block, fill, cash_before, aggregate_wei),
            FundingPath::ReservedMaker => self.reserved_maker_capacity(fill, cash_before),
            FundingPath::OtherDexReserve { locked_micro, ioc_block: Some(height) }
                if height == block && fill.coin == self.coin && fill.crossed && !fill.backstop =>
                (aggregate_wei / WEI_PER_MICRO).checked_sub(locked_micro).map(|cash| cash.max(0)),
            FundingPath::OtherDexReserve { .. } | FundingPath::AnchoredCrossMaker { .. } => None,
        };
        if capacity.is_none() { self.cash_micro = None; }
        capacity
    }

    #[allow(clippy::cast_precision_loss)]
    fn maker_capacity(&mut self, block: u64, fill: &FillEvent, cash_before: i64, aggregate_wei: i64) -> Option<i64> {
        if fill.coin != self.coin || fill.backstop || fill.crossed { return None; }
        if self.last_fill_block.is_some_and(|last| last != block) { self.cash_micro = None; }
        let cash = self.cash_micro?;
        let mark = fill.mark.filter(|mark| mark.min_px_raw == mark.max_px_raw).filter(|mark| mark.size_decimals == self.decimals)?;
        let start = parse_size_raw(&fill.start_position, self.decimals).ok()?;
        let quantity = parse_size_raw(&fill.sz, self.decimals).ok()?;
        let price = parse_size_raw(&fill.px, 6 - self.decimals).ok()?;
        if start != self.reserve.position_raw || quantity <= 0 || price <= 0 || fill.usdc_fee_wei < 0 { return None; }
        let side = usize::from(fill.side == Side::Ask);
        let q = u64::try_from(quantity).ok()?;
        let notional = u64::try_from(price).ok()?.checked_mul(q)?;
        if self.reserve.size_raw[side] < q || self.reserve.notional_micro[side] < notional || cash > aggregate_wei / WEI_PER_MICRO { return None; }
        // Fee rate is an estimate from this recorded execution, not a recovered fee-tier state. Keep estimated.
        let fee_micro = fill.usdc_fee_wei / WEI_PER_MICRO;
        let maximum = maker_size(cash, cash_before, start, price, mark.min_px_raw, fill.side, self.reserve.leverage,
            fee_micro as f64 / (price as f64 * quantity as f64))?;
        let physical = if maximum >= quantity { cash } else { aggregate_wei / WEI_PER_MICRO };
        self.cash_micro = Some(physical);
        Some(physical.max(0))
    }

    pub(super) fn filled(&mut self, block: u64, fill: &FillEvent, end: i64, cross_delta_wei: i64) -> Option<()> {
        if !self.tracker_trusted || fill.coin != self.coin || cross_delta_wei % WEI_PER_MICRO != 0 { return None; }
        // Consume the one observed trigger boundary, including on an unrecognized fill. A later fill must not
        // inherit checkpoint holds after physical routing has changed.
        if matches!(self.path, FundingPath::AnchoredCrossMaker { .. } | FundingPath::FrozenSpot { .. }) { return None; }
        if let FundingPath::OtherDexReserve { ioc_block, .. } = self.path {
            return (ioc_block == Some(block) && fill.crossed && !fill.backstop).then_some(());
        }
        let quantity = u64::try_from(parse_size_raw(&fill.sz, self.decimals).ok()?).ok()?;
        let price = u64::try_from(parse_size_raw(&fill.px, 6 - self.decimals).ok()?).ok()?;
        if !fill.crossed {
            let side = usize::from(fill.side == Side::Ask);
            self.reserve.size_raw[side] = self.reserve.size_raw[side].checked_sub(quantity)?;
            self.reserve.notional_micro[side] = self.reserve.notional_micro[side].checked_sub(quantity.checked_mul(price)?)?;
        }
        self.reserve.position_raw = end;
        self.reserve.price_raw = fill.mark.filter(|m| m.min_px_raw == m.max_px_raw).map_or(0, |m| m.min_px_raw);
        self.cash_micro = self.cash_micro.and_then(|cash| cash.checked_add(cross_delta_wei / WEI_PER_MICRO));
        if self.path == FundingPath::ReservedMaker {
            // Either no sweep ran, or excess above 101% of the reduced reservation returned to spot.
            // Keep the lower of those balances; never claim which sweep schedule actually ran.
            self.cash_micro = self.cash_micro.zip(self.reserve.retained_micro()).map(|(cash, retained)| cash.min(retained));
        }
        self.last_fill_block = Some(block);
        Some(())
    }
}

/// Native 0x3c96173..0x3c962a9 / 0x44ccc90 in the nonnegative closing-equity, isolated-only branch.
/// Outside that branch, tiered/negative-equity checks are not substituted. This is a routing estimate, not an
/// authorization check: the actual accepted fill is already a node fact.
#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
#[expect(clippy::too_many_arguments, reason = "explicit native scalar inputs preserve the checked ABI arithmetic") ]
fn maker_size(cross_micro: i64, isolated_micro: i64, position: i64, price: i64, mark: u64, side: Side, leverage: u32, fee_rate: f64) -> Option<i64> {
    if leverage == 0 || cross_micro < 0 || mark == 0 { return None; }
    let signed_favorable = if side == Side::Bid { mark as f64 - price as f64 } else { price as f64 - mark as f64 };
    let per_unit = mark as f64 / f64::from(leverage) - signed_favorable.min(0.0);
    if !per_unit.is_finite() || per_unit <= 0.0 { return None; }
    let opposing = position != 0 && (position > 0) != (side == Side::Bid);
    let quantity = if opposing {
        let fee = (position as f64 * price as f64 * fee_rate.max(0.0)) as i64;
        let closing = isolated_micro.checked_add(position.checked_mul(price)?)?.checked_sub(fee.checked_abs()?)?;
        if closing < 0 { return None; }
        position.unsigned_abs() as f64 + cross_micro.checked_add(closing)?.max(0) as f64 / per_unit
    } else { cross_micro as f64 / per_unit };
    (quantity.is_finite() && quantity >= 0.0 && quantity < i64::MAX as f64).then_some(quantity.floor() as i64)
}

pub(super) fn apply_update(account: &mut AccountMargin, block: u64, update: &RoutingUpdate) {
    let (Some(route), Pools::Unified(total)) = (&mut account.routing, &account.pools) else { return; };
    let valid = match update {
        RoutingUpdate::Orders { changes, mark_raw, prefund } => route.orders(changes, *mark_raw, *prefund, *total).is_some(),
        RoutingUpdate::OrdinaryIoc => match &mut route.path {
            FundingPath::OtherDexReserve { ioc_block, .. } if route.tracker_trusted => { *ioc_block = Some(block); true },
            _ => false,
        },
        RoutingUpdate::TriggeredTaker { tid } => match &mut route.path {
            FundingPath::FrozenSpot { execution, .. } if route.tracker_trusted => { *execution = Some((block, *tid)); true },
            _ => false,
        },
        RoutingUpdate::Unavailable => false,
    };
    if !valid { account.routing = None; }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::accounts::{checkpoint::{parse_checkpoint_bytes, Account, DexAccount, Position, tests::{fixture, user, HOLDER}},
        replay::{apply_perp_fill, FillMarkRange, PerpMarket, AccountMode, AssetMarginMode}, portfolio::USDC};
    use crate::types::Fill;

    pub(crate) fn checkpoint() -> (CheckpointAccounts, Markets, Address) {
        let mut cp = parse_checkpoint_bytes(&fixture()).unwrap();
        let who = user(HOLDER);
        let account = cp.accounts.get_mut(&who).unwrap();
        account.mode = AccountMode::Unified;
        account.lending.clear();
        account.spot = vec![(USDC, 100_000_000_000)];
        account.dex_states = vec![DexAccount { dex: 1, cash_micro: 1_000_000,
            positions: vec![Position { asset: 0, size_raw: 1, table_id: 10,
                margin: PositionMargin::Isolated { leverage: 4, cash_micro: -200_000 } }], isolated_settings: vec![] }];
        cp.order_trackers = (0..cp.dexes.len()).map(|_| Some(FxHashMap::default())).collect();
        cp.accounts.retain(|address, _| *address == who);
        let markets = Markets::of(&cp).unwrap();
        (cp, markets, who)
    }

    fn route() -> RoutingProjection {
        let (cp, markets, who) = checkpoint();
        RoutingProjection::seeds(&cp, &markets).remove(&who).unwrap()
    }

    fn totals(account: &AccountMargin) -> i64 {
        let Pools::Unified(cash) = account.pools else { panic!("unified fixture") };
        cash + account.isolated.values().sum::<i64>()
    }

    fn event() -> FillEvent {
        let fixture: serde_json::Value = serde_json::from_str(include_str!("unified_routing_golden.json")).unwrap();
        let row = &fixture["fills"][7];
        let fill: Fill = serde_json::from_value(row["fill"].clone()).unwrap();
        let mark = row["mark_raw"].as_u64().unwrap();
        FillEvent::at_mark(&fill, Some(FillMarkRange { min_px_raw: mark, max_px_raw: mark, size_decimals: 3 })).unwrap()
    }

    fn anth_route() -> RoutingProjection {
        RoutingProjection { coin: "io:ANTH".into(), decimals: 3,
            reserve: Reserve { notional_micro: [0, 26_881_400], size_raw: [0, 14], position_raw: 1, price_raw: 1_923_200, leverage: 4 },
            cash_micro: Some(5_816_337), last_fill_block: None, tracker_trusted: true, path: FundingPath::RestingMaker }
    }

    #[test]
    fn archived_client_reserve_and_maker_size_matrix() {
        let golden: serde_json::Value = serde_json::from_str(include_str!("routing_native_golden.json")).unwrap();
        let reserve_cases = golden["reserve_cases"].as_array().unwrap();
        let maker_cases = golden["maker_cases"].as_array().unwrap();
        assert_eq!((reserve_cases.len(), maker_cases.len()), (50, 90));
        for row in reserve_cases {
            let reserve: Reserve = serde_json::from_value(row["tracker"].clone()).unwrap();
            assert_eq!(reserve.required_micro(), row["required_micro"].as_i64(), "{row}");
        }
        for row in maker_cases {
            let i = |key: &str| row[key].as_i64().unwrap();
            let rate = i("fee_micro") as f64 / (i("price_raw") as f64 * i("quantity_raw") as f64);
            let side = if row["side"] == "B" { Side::Bid } else { Side::Ask };
            assert_eq!(maker_size(i("cross_micro"), i("isolated_micro"), i("position_raw"), i("price_raw"),
                row["mark_raw"].as_u64().unwrap(), side, 4, rate), row["native_max_size_raw"].as_i64(), "{row}");
        }
        assert_eq!(anth_route().reserve.retained_micro(), Some(5_816_337));
    }

    #[test]
    fn checkpoint_requires_complete_single_market_unfrozen_routing_inputs() {
        let (cp, markets, who) = checkpoint();
        assert_eq!(RoutingProjection::seeds(&cp, &markets)[&who].cash_micro, Some(1_000_000));
        for variant in 0..6 {
            let (mut cp, markets, who) = checkpoint();
            match variant {
                0 => cp.order_trackers[0] = None,
                1 => { cp.spot_usdc_holds.insert(who, 1); },
                2 => { cp.order_trackers[0].as_mut().unwrap().insert(who,
                    crate::accounts::checkpoint::OrderTrackerSnapshot { reserve_micro: 1, assets: vec![] }); },
                3 => cp.accounts.get_mut(&who).unwrap().dex_states.push(DexAccount { dex: 0, cash_micro: -1, positions: vec![], isolated_settings: vec![] }),
                4 => { cp.order_trackers[1].as_mut().unwrap().insert(who,
                    crate::accounts::checkpoint::OrderTrackerSnapshot { reserve_micro: 0, assets: vec![(10_000, Reserve {
                        notional_micro: [0, 0], size_raw: [0, 0], position_raw: 2, price_raw: 358_320, leverage: 4 })] }); },
                5 => cp.accounts.get_mut(&who).unwrap().mode = AccountMode::Default,
                _ => unreachable!(),
            }
            assert!(!RoutingProjection::seeds(&cp, &markets).contains_key(&who), "variant {variant}");
        }
    }

    #[test]
    fn cancel_does_not_sweep_and_later_alo_establishes_new_funding() {
        let mut route = route();
        let open = OrderChange { coin: route.coin.clone(), side: Side::Ask, px: "358.32".into(), size: "0.014".into(), remove: false };
        route.orders(std::slice::from_ref(&open), Some(358_320), true, 100_000_000_000).unwrap();
        let retained = route.cash_micro.unwrap();
        assert_eq!(retained, route.reserve.retained_micro().unwrap());
        route.last_fill_block = Some(8);
        route.orders(&[OrderChange { remove: true, ..open.clone() }], Some(358_320), false, 100_000_000_000).unwrap();
        assert_eq!(route.cash_micro, None, "cancel scheduling is unproved; neither zero nor retained cash is substituted");
        route.orders(&[open], Some(358_320), true, 100_000_000_000).unwrap();
        assert_eq!(route.cash_micro, Some(retained));
        assert_eq!(route.last_fill_block, None, "an earlier fill cannot invalidate this new ALO wrapper");
    }

    #[test]
    fn unknown_maker_inputs_clear_capacity_and_no_per_block_sweep_is_invented() {
        let fill = event();
        assert_eq!(anth_route().capacity(8, &fill, -1_441_089, 10_000_000_000), Some(5_816_337));
        let mut insufficient = anth_route();
        insufficient.cash_micro = Some(1_000_000);
        assert_eq!(insufficient.capacity(8, &fill, -1_441_089, 10_000_000_000), Some(100_000_000), "maker precheck calls prefund when physical funds cannot execute this size");
        for variant in 0..8 {
            let mut route = anth_route();
            let mut event = fill.clone();
            match variant {
                0 => event.mark = None,
                1 => event.crossed = true,
                2 => event.usdc_fee_wei = -100,
                3 => route.reserve.size_raw[1] = 13,
                4 => route.last_fill_block = Some(7),
                5 => route.invalidate(),
                6 => route.cash_micro = Some(100_000_001),
                7 => event.start_position = "0.002".into(),
                _ => unreachable!(),
            }
            assert_eq!(route.capacity(8, &event, -1_441_089, 10_000_000_000), None, "variant {variant}");
            assert_eq!(route.cash_micro, None, "unknown capacity must not survive a subsequent fill");
        }
        assert_eq!(maker_size(0, -2_000_000, 1, 1_920_100, 1_923_200, Side::Ask, 4, 0.0), None);
    }

    #[test]
    fn anth_nine_fills_close_the_split_under_native_last_alo_reservation() {
        let golden: serde_json::Value = serde_json::from_str(include_str!("unified_routing_golden.json")).unwrap();
        let market = PerpMarket { dex: 10, asset: 1, size_decimals: 3, margin_mode: AssetMarginMode::NoCross, default_leverage: 4 };
        let markets = Markets { perps: std::iter::once(("io:ANTH".into(), market)).collect(), usdc_dexes: vec![true; 11], ..Markets::default() };
        let before = &golden["before"];
        let source = Account { mode: AccountMode::Unified, spot: vec![(USDC, before["spot_usdc_wei"].as_i64().unwrap())], lending: vec![],
            dex_states: vec![DexAccount { dex: 10, cash_micro: before["dex_cash_micro"].as_i64().unwrap(), positions: vec![Position {
                asset: 1, size_raw: -56, table_id: 6, margin: PositionMargin::Isolated { leverage: 4, cash_micro: 134_577_519 } }], isolated_settings: vec![] }] };
        let mut account = AccountMargin::of(&source, &markets);
        for (index, row) in golden["fills"].as_array().unwrap().iter().enumerate() {
            let fill: Fill = serde_json::from_value(row["fill"].clone()).unwrap();
            let mark = row["mark_raw"].as_u64().unwrap();
            let event = FillEvent::at_mark(&fill, Some(FillMarkRange { min_px_raw: mark, max_px_raw: mark, size_decimals: 3 })).unwrap();
            if index == 7 {
                assert_eq!(account.isolated[&(10, 1)] / WEI_PER_MICRO, -1_441_089);
                // Conditional mechanism fixture: actual last ALO size/price, explicitly assume all older orders
                // removed. The archive lacks the full tracker, so this does NOT promote historical confidence.
                let mut route = anth_route();
                route.reserve.notional_micro = [0, 0];
                route.reserve.size_raw = [0, 0];
                route.cash_micro = None;
                account.routing = Some(route);
                apply_update(&mut account, row["height"].as_u64().unwrap(), &RoutingUpdate::Orders { changes: vec![OrderChange {
                    coin: "io:ANTH".into(), side: Side::Ask, px: "1920.1".into(), size: "0.014".into(), remove: false }],
                    mark_raw: Some(1_923_200), prefund: true });
                assert_eq!(account.routing.as_ref().unwrap().cash_micro, Some(5_816_337));
            }
            let total_before = totals(&account);
            apply_perp_fill(&mut account, &markets, row["height"].as_u64().unwrap(), &event).unwrap();
            assert_eq!(totals(&account), total_before + event.cash_wei(3, 3).unwrap());
            if index == 7 { assert_eq!(account.isolated[&(10, 1)] / WEI_PER_MICRO, 31_249_122); }
        }
        assert_eq!(account.isolated[&(10, 1)] / WEI_PER_MICRO, 7_210_095);
        assert_eq!(account.pools, Pools::Unified(golden["after"]["spot_usdc_wei"].as_i64().unwrap()));
        assert!(account.estimated, "caller fee encoding / historical tracker remain unproved");
    }
}
