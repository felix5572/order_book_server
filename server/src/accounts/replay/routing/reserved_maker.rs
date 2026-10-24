//! An existing isolated-only dex reservation can fund a maker increase even when another dex's
//! cross margin makes the aggregate available amount zero. PRL1195567690/1195567713 supplies the
//! complete tracker and two endpoints. Bound possible sweeps, rather than invent their timing.
use alloy::primitives::Address;
use crate::accounts::{checkpoint::{Account, CheckpointAccounts, PositionMargin, AssetMarginMode},
    size::parse_size_raw, usdc::WEI_PER_MICRO};
use crate::order_book::Side;
use super::{FundingPath, RoutingProjection, Markets, FillEvent, maker_size};

impl RoutingProjection {
    pub(super) fn reserved_maker_seed(cp: &CheckpointAccounts, markets: &Markets, user: &Address, account: &Account) -> Option<Self> {
        if !account.lending.is_empty() || account.dex_states.iter().any(|state| !markets.usdc_dexes[state.dex]) { return None; }
        let mut candidate = None;
        for state in &account.dex_states {
            if !markets.usdc_dexes[state.dex] || state.cash_micro <= 0 { continue; }
            let [position] = state.positions.as_slice() else { continue; };
            let PositionMargin::Isolated { leverage, .. } = position.margin else { continue; };
            // This path addresses foreign cross exposure, leaving the established single-dex path unchanged.
            if !account.dex_states.iter().any(|other| other.dex != state.dex && other.positions.iter()
                .any(|p| p.size_raw != 0 && matches!(p.margin, PositionMargin::Cross { .. }))) { continue; }
            let coin = &markets.asset_names[state.dex][position.asset];
            let market = &markets.perps[coin];
            if !matches!(market.margin_mode, AssetMarginMode::NoCross | AssetMarginMode::StrictIsolated)
                || markets.tables[state.dex].get(&position.table_id)?.tiers().len() != 1 { continue; }
            let tracker = cp.order_trackers[state.dex].as_ref()?.get(user)?;
            let [(id, reserve)] = tracker.assets.as_slice() else { continue; };
            let base = cp.dexes[state.dex].pdi.checked_mul(10_000)?;
            if usize::try_from(id.checked_sub(base)?).ok()? != position.asset || position.size_raw == 0
                || reserve.position_raw != position.size_raw || reserve.leverage != leverage
                || reserve.required_micro()? != tracker.reserve_micro
                || tracker.reserve_micro <= 0 || reserve.retained_micro()? != state.cash_micro { continue; }
            // Opposing resting orders cannot exceed the current absolute exposure after a flip. Adding-side notional
            // alone determines the reserve, independent of intervening mark-only updates.
            let opposite = usize::from(position.size_raw > 0);
            if reserve.size_raw[opposite] > position.size_raw.unsigned_abs().checked_mul(2)? { continue; }
            if candidate.is_some() { return None; }
            candidate = Some(Self { coin: coin.clone(), decimals: market.size_decimals, reserve: reserve.clone(),
                cash_micro: Some(state.cash_micro), last_fill_block: None, tracker_trusted: true, path: FundingPath::ReservedMaker });
        }
        candidate
    }

    #[allow(clippy::cast_precision_loss)]
    pub(super) fn reserved_maker_capacity(&self, fill: &FillEvent, cash_before: i64) -> Option<i64> {
        if fill.coin != self.coin || fill.crossed || fill.backstop || fill.usdc_fee_wei < 0 { return None; }
        let mark = fill.mark.filter(|m| m.size_decimals == self.decimals && m.min_px_raw > 0 && m.min_px_raw == m.max_px_raw)?;
        let start = parse_size_raw(&fill.start_position, self.decimals).ok()?;
        let quantity = parse_size_raw(&fill.sz, self.decimals).ok()?;
        let price = parse_size_raw(&fill.px, 6 - self.decimals).ok()?;
        if start != self.reserve.position_raw || start == 0 || (start > 0) != (fill.side == Side::Bid)
            || quantity <= 0 || price <= 0 { return None; }
        let side = usize::from(fill.side == Side::Ask);
        let quantity_unsigned = u64::try_from(quantity).ok()?;
        let notional = u64::try_from(price).ok()?.checked_mul(quantity_unsigned)?;
        if quantity_unsigned > self.reserve.size_raw[side] || notional > self.reserve.notional_micro[side] { return None; }
        let floor = self.cash_micro?;
        let rate = (fill.usdc_fee_wei / WEI_PER_MICRO) as f64 / (price as f64 * quantity as f64);
        let maximum = maker_size(floor, cash_before, start, price, mark.min_px_raw, fill.side, self.reserve.leverage, rate)?;
        // Only a proven sufficient lower bound is useful. Otherwise the replenishment path is unknown;
        // do not turn the lower bound into a false upper cap on the position's transfer.
        (maximum >= quantity).then_some(floor)
    }
}

#[cfg(test)]
mod tests;
