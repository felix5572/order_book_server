//! Margin tables and maintenance margin (official "Margin tiers"):
//! `maintenance_margin = notional * rate - deduction`, `rate = 1 / (2 * max_leverage)` of the tier the notional falls
//! in, deductions chosen so the margin is continuous across tiers. Table ids below 50 are a single tier whose max
//! leverage is the id; larger ids are listed in the clearinghouse meta.

/// One tier: from `lower_bound_usd` (inclusive) up to the next tier's lower bound.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Tier {
    pub(crate) lower_bound_usd: f64,
    pub(crate) maintenance_rate: f64,
    pub(crate) deduction_usd: f64,
}

/// Tiers ascending by lower bound; the first starts at 0.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MarginTable {
    tiers: Vec<Tier>,
}

impl MarginTable {
    /// The implicit table of an id below 50: one tier at that max leverage (which must be positive).
    pub(crate) fn single(max_leverage: u32) -> Result<Self, String> {
        Ok(Self { tiers: vec![Tier { lower_bound_usd: 0.0, maintenance_rate: rate(max_leverage)?, deduction_usd: 0.0 }] })
    }

    /// A listed table from the clearinghouse meta, `(lower bound micro-USD, max leverage, deduction micro-USD)` per tier.
    pub(crate) fn listed(tiers: impl IntoIterator<Item = (i64, u32, i64)>) -> Result<Self, String> {
        let mut tiers: Vec<Tier> = tiers
            .into_iter()
            .map(|(lower_bound, max_leverage, deduction)| {
                Ok(Tier {
                    lower_bound_usd: micro_usd(lower_bound),
                    maintenance_rate: rate(max_leverage).map_err(|err| format!("tier from {lower_bound}: {err}"))?,
                    deduction_usd: micro_usd(deduction),
                })
            })
            .collect::<Result<_, String>>()?;
        tiers.sort_by(|a, b| a.lower_bound_usd.total_cmp(&b.lower_bound_usd));
        match tiers.first() {
            Some(first) if first.lower_bound_usd == 0.0 => Ok(Self { tiers }),
            Some(first) => Err(format!("margin table starts at {} USD, not 0", first.lower_bound_usd)),
            None => Err("margin table without tiers".to_string()),
        }
    }

    pub(crate) fn tiers(&self) -> &[Tier] {
        &self.tiers
    }

    /// Upper bound (exclusive) of tier `index`; infinite for the last tier.
    pub(crate) fn upper_bound_usd(&self, index: usize) -> f64 {
        self.tiers.get(index + 1).map_or(f64::INFINITY, |next| next.lower_bound_usd)
    }

    /// The tier a non-negative notional falls in.
    pub(crate) fn tier_for(&self, notional_usd: f64) -> &Tier {
        self.tiers.iter().rev().find(|tier| notional_usd >= tier.lower_bound_usd).unwrap_or(&self.tiers[0])
    }

    pub(crate) fn maintenance_usd(&self, notional_usd: f64) -> f64 {
        let tier = self.tier_for(notional_usd);
        notional_usd.mul_add(tier.maintenance_rate, -tier.deduction_usd)
    }
}

/// The only place a maintenance rate is made: a max leverage of 0 would give an infinite rate.
fn rate(max_leverage: u32) -> Result<f64, String> {
    if max_leverage == 0 {
        return Err("max leverage 0".to_string());
    }
    Ok(1.0 / (2.0 * f64::from(max_leverage)))
}

/// Micro-USD integers from the checkpoint; magnitudes stay far below 2^53.
fn micro_usd(value: i64) -> f64 {
    value as f64 / 1e6
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Table 52 in the 10-19 checkpoint: 0-20M at 10x, above at 5x with a 1M deduction.
    fn tiered_10x() -> MarginTable {
        MarginTable::listed([(0, 10, 0), (20_000_000_000_000, 5, 1_000_000_000_000)]).unwrap()
    }

    #[test]
    fn single_tier_rate_is_half_the_initial_margin_at_max_leverage() {
        assert_eq!(MarginTable::single(20).unwrap().maintenance_usd(1000.0), 25.0);
    }

    #[test]
    fn a_zero_max_leverage_is_rejected() {
        assert!(MarginTable::single(0).is_err());
        assert!(MarginTable::listed([(0, 10, 0), (1_000_000, 0, 0)]).is_err());
    }

    #[test]
    fn tiered_margin_is_continuous_and_uses_the_tier_of_the_notional() {
        let table = tiered_10x();
        assert!((table.maintenance_usd(20_000_000.0) - 1_000_000.0).abs() < 1e-6);
        assert!((table.maintenance_usd(34_781_088.0) - 2_478_108.8).abs() < 1e-6);
        assert!((table.maintenance_usd(1_000_000.0) - 50_000.0).abs() < 1e-9);
    }

    #[test]
    fn listed_table_must_start_at_zero() {
        assert!(MarginTable::listed([(5, 10, 0)]).is_err());
        assert!(MarginTable::listed([]).is_err());
    }
}
