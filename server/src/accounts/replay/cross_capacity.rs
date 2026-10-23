//! Capacity of a per-dex cross portfolio whose holdings and leverage have not changed since its checkpoint.
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
pub(super) struct CrossCapacitySnapshot {
    pub(super) holdings: Vec<Holding>,
}

impl CrossCapacitySnapshot {
    /// A block envelope must be a single price for every holding. A changing/missing mark or scale mismatch
    /// supplies no exact proof. Sub-micro cash remains in the pool; only whole micro USDC can fund perps.
    pub(super) fn available_micro(&self, cash_wei: i64, prices: &FxHashMap<String, FillMarkRange>) -> Result<Option<i64>, String> {
        let mut available = i128::from(cash_wei / WEI_PER_MICRO);
        for held in &self.holdings {
            let Some(mark) = prices.get(&held.coin) else { return Ok(None); };
            if mark.size_decimals != held.size_decimals || mark.min_px_raw == 0 || mark.min_px_raw != mark.max_px_raw { return Ok(None); }
            if held.leverage == 0 { return Err(format!("{} cross snapshot leverage is zero", held.coin)); }
            let notional = i128::from(held.size_raw) * i128::from(mark.min_px_raw);
            available = available.checked_add(notional - notional.abs() / i128::from(held.leverage))
                .ok_or_else(|| format!("{} cross capacity overflows", held.coin))?;
        }
        i64::try_from(available.max(0)).map(Some).map_err(|err| format!("cross capacity overflows: {err}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_or_moving_prices_are_not_capacity_and_rounding_is_per_position() {
        let snapshot = CrossCapacitySnapshot { holdings: vec![
            Holding { coin: "A".into(), size_raw: 2, leverage: 3, size_decimals: 0 },
            Holding { coin: "B".into(), size_raw: -1, leverage: 2, size_decimals: 0 },
        ] };
        let price = |low, high| FillMarkRange { min_px_raw: low, max_px_raw: high, size_decimals: 0 };
        let mut prices = FxHashMap::from_iter([("A".into(), price(5, 5)), ("B".into(), price(3, 3))]);
        assert_eq!(snapshot.available_micro(-199, &prices).unwrap(), Some(2)); // -1 +10 -3 -3 -1
        prices.insert("A".into(), price(5, 6));
        assert_eq!(snapshot.available_micro(100_000, &prices).unwrap(), None);
        prices.remove("A");
        assert_eq!(snapshot.available_micro(100_000, &prices).unwrap(), None);
        prices.insert("A".into(), FillMarkRange { size_decimals: 1, ..price(5, 5) });
        assert_eq!(snapshot.available_micro(100_000, &prices).unwrap(), None);
    }
}
