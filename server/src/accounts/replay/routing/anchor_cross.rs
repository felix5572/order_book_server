//! Checkpoint-local maker capacity before any observed funding/order boundary.
//! SOFTBANK H1194820315: xyz cash -136.644499 plus META/SNDK/SKHX collateral leaves 3.668831 USDC;
//! the native maker limit admits 403 raw units, then the transfer consumes that capacity, not Unified spot.
use super::{FundingPath, RoutingProjection, maker_size};
use crate::accounts::{checkpoint::{Account, CheckpointAccounts, PositionMargin, AssetMarginMode}, size::parse_size_raw,
    usdc::WEI_PER_MICRO};
use crate::order_book::Side;
use super::super::{Markets, FillEvent, cross_capacity::CrossCapacityProjection};
use alloy::primitives::Address;
use rustc_hash::FxHashMap;

impl RoutingProjection {
    pub(super) fn anchored_cross_seed(checkpoint: &CheckpointAccounts, markets: &Markets, user: &Address, account: &Account) -> Option<Self> {
        if !account.lending.is_empty() { return None; }
        let mut candidate = None;
        for state in account.dex_states.iter().filter(|state| markets.usdc_dexes[state.dex]) {
            if !state.positions.iter().any(|p| p.size_raw != 0 && matches!(p.margin, PositionMargin::Cross { .. })) { continue; }
            let Some(tracker) = checkpoint.order_trackers[state.dex].as_ref()?.get(user) else { continue; };
            let total = tracker.assets.iter().try_fold(0_i64, |sum, (_, reserve)| sum.checked_add(reserve.required_micro()?))?;
            if total != tracker.reserve_micro { return None; }
            let base = checkpoint.dexes[state.dex].pdi.checked_mul(10_000)?;
            for (id, reserve) in &tracker.assets {
                let asset = usize::try_from(id.checked_sub(base)?).ok()?;
                let Some(position) = state.positions.iter().find(|p| p.asset == asset) else { continue; };
                let PositionMargin::Isolated { leverage, .. } = position.margin else { continue; };
                if reserve.required_micro()? == 0 { continue; }
                let coin = &markets.asset_names[state.dex][asset];
                let market = &markets.perps[coin];
                if !matches!(market.margin_mode, AssetMarginMode::NoCross | AssetMarginMode::StrictIsolated)
                    || markets.tables[state.dex].get(&position.table_id)?.tiers().len() != 1
                    || reserve.position_raw != position.size_raw || reserve.leverage != leverage { return None; }
                // A single tracked target lets the existing routing adapter invalidate on ANY other
                // account fill/order/cash boundary. No cross-dex transfer ledger is invented here.
                if candidate.is_some() { return None; }
                candidate = Some(Self { coin: coin.clone(), decimals: market.size_decimals, reserve: reserve.clone(),
                    cash_micro: Some(state.cash_micro), last_fill_block: None, tracker_trusted: true,
                    path: FundingPath::AnchoredCrossMaker { dex: state.dex } });
            }
        }
        candidate
    }

    #[allow(clippy::cast_precision_loss)]
    pub(in super::super) fn anchored_cross_capacity(&self, fill: &FillEvent, cash_before: i64,
        portfolios: &FxHashMap<usize, CrossCapacityProjection>, aggregate_micro: Option<i64>) -> Result<Option<i64>, String> {
        let FundingPath::AnchoredCrossMaker { dex } = self.path else { return Ok(None); };
        if !self.tracker_trusted || self.cash_micro.is_none() || fill.coin != self.coin || fill.crossed || fill.backstop
            || fill.usdc_fee_wei < 0 { return Ok(None); }
        let Some(mark) = fill.mark.filter(|m| m.min_px_raw > 0 && m.min_px_raw == m.max_px_raw)
            else { return Ok(None); };
        if mark.size_decimals != self.decimals { return Ok(None); }
        let Some(portfolio) = portfolios.get(&dex).filter(|p| !p.advanced) else { return Ok(None); };
        let Some(prices) = &fill.collateral_marks else { return Ok(None); };
        let Some(cash) = self.cash_micro.and_then(|cash| cash.checked_mul(WEI_PER_MICRO)) else { return Ok(None); };
        let Some(available) = portfolio.available_micro(cash, prices)? else { return Ok(None); };
        if aggregate_micro.is_none_or(|aggregate| available > aggregate) { return Ok(None); }
        let start = parse_size_raw(&fill.start_position, self.decimals)?;
        let quantity = parse_size_raw(&fill.sz, self.decimals)?;
        let price = parse_size_raw(&fill.px, 6 - self.decimals)?;
        if start != self.reserve.position_raw || quantity <= 0 || price <= 0 { return Ok(None); }
        let side = usize::from(fill.side == Side::Ask);
        // Only the complete remaining side is established by this anchor. Partial fills/deferred sweeps
        // consume the proof without advancing a physical balance to another execution.
        if u64::try_from(quantity).ok() != Some(self.reserve.size_raw[side])
            || i128::from(price) * i128::from(quantity) != i128::from(self.reserve.notional_micro[side]) { return Ok(None); }
        let fee_rate = (fill.usdc_fee_wei / WEI_PER_MICRO) as f64 / (price as f64 * quantity as f64);
        let maximum = maker_size(available, cash_before, start, price, mark.min_px_raw, fill.side, self.reserve.leverage, fee_rate);
        // Insufficient physical admission may enter the replenishment wrapper; do not cap it here.
        Ok(maximum.filter(|maximum| *maximum >= quantity).map(|_| available))
    }
}
