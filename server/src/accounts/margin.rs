//! Margin tables and maintenance margin (official "Margin tiers"):
//! `maintenance_margin = notional * rate - deduction`, `rate = 1 / (2 * max_leverage)` of the tier the notional falls
//! in, deductions chosen so the margin is continuous across tiers. Table ids below 50 are a single tier whose max
//! leverage is the id; larger ids are listed in the clearinghouse meta.
//!
//! A position held at its mark has the node's integer maintenance (`maintenance_micro`); the continuous form (the
//! `f64` fields) is what a liquidation price solves along. Both come from the same checkpoint integers.

/// One tier: from its lower bound (inclusive) up to the next tier's lower bound.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Tier {
    pub(crate) lower_bound_usd: f64,
    pub(crate) maintenance_rate: f64,
    pub(crate) deduction_usd: f64,
    lower_bound_micro: u64,
    max_leverage: u32,
    deduction_micro: i64,
}

impl Tier {
    fn new(lower_bound_micro: i64, max_leverage: u32, deduction_micro: i64) -> Result<Self, String> {
        let lower_bound = u64::try_from(lower_bound_micro).map_err(|_| format!("negative tier lower bound {lower_bound_micro}"))?;
        Ok(Self {
            lower_bound_usd: micro_usd(lower_bound_micro),
            maintenance_rate: rate(max_leverage).map_err(|err| format!("tier from {lower_bound_micro}: {err}"))?,
            deduction_usd: micro_usd(deduction_micro),
            lower_bound_micro: lower_bound,
            max_leverage,
            deduction_micro,
        })
    }
}

/// Tiers ascending by lower bound; the first starts at 0.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MarginTable {
    tiers: Vec<Tier>,
}

impl MarginTable {
    /// The implicit table of an id below 50: one tier at that max leverage (which must be positive).
    pub(crate) fn single(max_leverage: u32) -> Result<Self, String> {
        Ok(Self { tiers: vec![Tier::new(0, max_leverage, 0)?] })
    }

    /// A listed table from the clearinghouse meta, `(lower bound micro-USD, max leverage, deduction micro-USD)` per tier.
    pub(crate) fn listed(tiers: impl IntoIterator<Item = (i64, u32, i64)>) -> Result<Self, String> {
        let mut tiers: Vec<Tier> = tiers
            .into_iter()
            .map(|(lower_bound, max_leverage, deduction)| Tier::new(lower_bound, max_leverage, deduction))
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

    /// The first tier's maximum leverage; account initialization applies its separate default cap.
    pub(crate) fn first_max_leverage(&self) -> u32 {
        self.tiers[0].max_leverage
    }

    /// Upper bound (exclusive) of tier `index`; infinite for the last tier.
    pub(crate) fn upper_bound_usd(&self, index: usize) -> f64 {
        self.tiers.get(index + 1).map_or(f64::INFINITY, |next| next.lower_bound_usd)
    }

    /// Maintenance of a position held at its mark, as the node computes it: micro-USD integers, the exact notional's
    /// tier, `floor(notional / (2 * max_leverage)) - deduction`. bm 2026-10-19: equal to the node's
    /// `crossMaintenanceMarginUsed` in 271 of 271 single-tier pools and the one tiered pool checked; the `f64` form
    /// matched in 40 (research README "维持保证金的整数规则").
    #[expect(clippy::expect_used, reason = "a u128 divided by at least 2 (max leverage >= 1) is at most i128::MAX")]
    pub(crate) fn maintenance_micro(&self, notional_micro: u128) -> i128 {
        let tier = self.tiers.iter().rev().find(|tier| notional_micro >= u128::from(tier.lower_bound_micro)).unwrap_or(&self.tiers[0]);
        let floored = notional_micro / (2 * u128::from(tier.max_leverage));
        i128::try_from(floored).expect("u128 notional over 2 fits i128") - i128::from(tier.deduction_micro)
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
        assert_eq!(MarginTable::single(20).unwrap().maintenance_micro(1_000_000_000), 25_000_000);
    }

    /// The node floors each position's maintenance to micro-USD: an APT position of 0.007639 USD notional at 10x has
    /// 381 micro-USD (381.95 continuous), the case that left a unified account with 196 such positions 6.6e-5 USD off.
    #[test]
    fn maintenance_at_the_mark_is_floored_to_micro_usd() {
        assert_eq!(MarginTable::single(10).unwrap().maintenance_micro(7_639), 381);
        assert_eq!(MarginTable::single(3).unwrap().maintenance_micro(5), 0);
    }

    #[test]
    fn a_zero_max_leverage_is_rejected() {
        assert!(MarginTable::single(0).is_err());
        assert!(MarginTable::listed([(0, 10, 0), (1_000_000, 0, 0)]).is_err());
    }

    #[test]
    fn tiered_margin_is_continuous_and_uses_the_tier_of_the_notional() {
        let table = tiered_10x();
        assert_eq!(table.maintenance_micro(20_000_000_000_000), 1_000_000_000_000);
        assert_eq!(table.maintenance_micro(19_999_999_999_999), 999_999_999_999);
        assert_eq!(table.maintenance_micro(34_781_088_000_001), 2_478_108_800_000);
        assert_eq!(table.maintenance_micro(1_000_000_000_000), 50_000_000_000);
    }

    #[test]
    fn listed_table_must_start_at_zero() {
        assert!(MarginTable::listed([(5, 10, 0)]).is_err());
        assert!(MarginTable::listed([]).is_err());
    }
}
