//! Capacity projection of the checkpoint cross portfolio, advanced by the cash owner’s validated fills.
//! The client 6911b792, 0x466d6b0 / 0x44ccb20, sums signed mark notionals and integer initial margins.
//! GIGADEV1193768936: free121.594406 caps requested121.728550; nine fills reconcile the complete pool.

use rustc_hash::FxHashMap;
use super::{FillMarkRange, WEI_PER_MICRO};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Holding {
    pub(super) coin: String,
    pub(super) size_raw: i64,
    pub(super) leverage: u32,
    pub(super) size_decimals: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CrossCapacityProjection {
    pub(super) holdings: Vec<Holding>,
    /// A maker's Unified physical funding path is unproved once intervening cross fills can sweep cash.
    pub(super) advanced: bool,
}

impl CrossCapacityProjection {
    /// The anchor retains leverage for open cross positions; successful closed-asset settings can add zero
    /// entries. Keep them through close/reopen; unknown assets or contradictory sizes cannot invent settings.
    pub(super) fn advance(&mut self, coin: &str, start_raw: i64, delta_raw: i64) -> Result<bool, String> {
        let Some(held) = self.holdings.iter_mut().find(|held| held.coin == coin) else { return Ok(false); };
        if held.size_raw != start_raw { return Ok(false); }
        self.advanced = true;
        held.size_raw = start_raw.checked_add(delta_raw).ok_or_else(|| format!("{coin} cross capacity size overflows"))?;
        Ok(true)
    }

    /// A complete portfolio proves an absent/zero holding is closed. Its leverage can change without
    /// changing equity or the other holdings. Never use this to reconstruct a discarded portfolio.
    pub(super) fn set_closed_leverage(&mut self, coin: &str, cross_leverage: Option<u32>, size_decimals: u32) -> bool {
        let index = self.holdings.iter().position(|held| held.coin == coin);
        if index.is_some_and(|index| self.holdings[index].size_raw != 0) { return false; }
        match (index, cross_leverage) {
            (Some(index), Some(leverage)) => self.holdings[index].leverage = leverage,
            (None, Some(leverage)) => self.holdings.push(Holding { coin: coin.into(), size_raw: 0, leverage, size_decimals }),
            (Some(index), None) => { self.holdings.remove(index); }
            (None, None) => {}
        }
        true
    }

    /// A block envelope must be a single price for every holding. A changing/missing mark or scale mismatch
    /// supplies no exact proof. Sub-micro cash remains in the pool; only whole micro USDC can fund perps.
    pub(super) fn available_micro(&self, cash_wei: i64, prices: &FxHashMap<String, FillMarkRange>) -> Result<Option<i64>, String> {
        Self::combined_available_micro(cash_wei, std::iter::once(self), prices)
    }

    /// Unified cash spans clearinghouses. Sum their validated holdings without flooring/clamping each dex;
    /// a deficit in one pool must offset another pool's equity. Only the final positive amount funds perps.
    pub(super) fn combined_available_micro<'a>(cash_wei: i64, portfolios: impl Iterator<Item = &'a Self>,
        prices: &FxHashMap<String, FillMarkRange>) -> Result<Option<i64>, String> {
        let mut available_wei = i128::from(cash_wei);
        for held in portfolios.flat_map(|portfolio| &portfolio.holdings).filter(|held| held.size_raw != 0) {
            let Some(mark) = prices.get(&held.coin) else { return Ok(None); };
            if mark.size_decimals != held.size_decimals || mark.min_px_raw == 0 || mark.min_px_raw != mark.max_px_raw { return Ok(None); }
            if held.leverage == 0 { return Err(format!("{} cross capacity leverage is zero", held.coin)); }
            let notional = i128::from(held.size_raw) * i128::from(mark.min_px_raw);
            let equity_micro = notional - notional.abs() / i128::from(held.leverage);
            available_wei = equity_micro.checked_mul(i128::from(WEI_PER_MICRO)).and_then(|wei| available_wei.checked_add(wei))
                .ok_or_else(|| format!("{} cross capacity overflows", held.coin))?;
        }
        // Negative cash plus positive collateral can become spendable. Truncating the negative cash first
        // rounds this amount UP by one micro when spot dust exists (CL1194857392 has 36 remaining wei).
        i64::try_from(available_wei.max(0) / i128::from(WEI_PER_MICRO)).map(Some)
            .map_err(|err| format!("cross capacity overflows: {err}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_holdings_close_flip_and_reopen_without_inventing_settings() {
        let mut projection = CrossCapacityProjection { advanced: false, holdings: vec![Holding {
            coin: "X".into(), size_raw: 2, leverage: 4, size_decimals: 0,
        }] };
        assert!(projection.advance("X", 2, -2).unwrap());
        assert_eq!(projection.available_micro(500, &FxHashMap::default()).unwrap(), Some(5), "zero holdings need no mark");
        assert!(projection.advance("X", 0, -3).unwrap());
        assert!(projection.advance("X", -3, 4).unwrap());
        assert_eq!(projection.holdings[0].size_raw, 1);
        assert_eq!(projection.holdings[0].leverage, 4);
        assert!(!projection.advance("X", -3, 4).unwrap(), "duplicate/out-of-order start size");
        assert!(!projection.advance("new", 0, 1).unwrap(), "checkpoint omitted this asset's zero-size setting");
        assert_eq!(projection.holdings.len(), 1);
    }

    #[test]
    fn unknown_or_moving_prices_are_not_capacity_and_rounding_is_per_position() {
        let snapshot = CrossCapacityProjection { advanced: false, holdings: vec![
            Holding { coin: "A".into(), size_raw: 2, leverage: 3, size_decimals: 0 },
            Holding { coin: "B".into(), size_raw: -1, leverage: 2, size_decimals: 0 },
        ] };
        let price = |low, high| FillMarkRange { min_px_raw: low, max_px_raw: high, size_decimals: 0 };
        let mut prices = FxHashMap::from_iter([("A".into(), price(5, 5)), ("B".into(), price(3, 3))]);
        assert_eq!(snapshot.available_micro(-199, &prices).unwrap(), Some(1)); // (-199 + (10 -3 -3 -1) *100) /100
        prices.insert("A".into(), price(5, 6));
        assert_eq!(snapshot.available_micro(100_000, &prices).unwrap(), None);
        prices.remove("A");
        assert_eq!(snapshot.available_micro(100_000, &prices).unwrap(), None);
        prices.insert("A".into(), FillMarkRange { size_decimals: 1, ..price(5, 5) });
        assert_eq!(snapshot.available_micro(100_000, &prices).unwrap(), None);
    }
}
