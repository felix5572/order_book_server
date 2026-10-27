//! One coin's local marks (design 000454 r1 1.3, r2 revisions 2–3): the node's own rules (`accounts::marks::rules`, the
//! replay's implementation) on the node price state of the applied block, the HL book now, and the venues' oracle and
//! external price now.
//!
//! - A = oracle × (1 + basis), the basis msoe previewed at now with the book's midpoint against the stored oracle, as a
//!   validator's reference action previews it at its cut; L = median(bid, ask, last trade); C = the external price.
//! - reference = median(A, L, C): what a validator submitting our venue prices now would store as its Mark member.
//! - submitted = the reference as a `SetGlobalAction` carries it: to 5 significant figures (members keep the coin's
//!   price units).
//! - predicted = median(submitted, Mark cache, L): a Main-dex `SetGlobalAction` now whose submission is the reference.
//!   Main bypasses the Hip3 1% limit (binary 6911b792, 0x2965e0a..0x2965e1b; `native.rs`). A conditional estimate,
//!   not the next mark: the cache refreshes per bucket and later submissions and books are unknown. Nothing is written back.

use super::price::{MAX_PERP_DECIMALS, VenuePrice};
use crate::accounts::marks::{
    native::CoinPriceState,
    rules::{internal_round, local_price, median_units},
};

/// A `SetGlobalAction`'s prices carry at most this many significant figures.
const SUBMITTED_SIGNIFICANT_FIGURES: u32 = 5;

/// The price where the cumulative weight, ascending, first passes half the total (the trading system's
/// `hl_mark_prices` `weighted_median`, over the venues quoting now); `None` without a price.
pub(crate) fn weighted_median(mut prices: Vec<(VenuePrice, u32)>) -> Option<VenuePrice> {
    prices.sort_unstable();
    let total: u32 = prices.iter().map(|(_, weight)| weight).sum();
    let mut cumulative = 0;
    prices
        .into_iter()
        .find(|(_, weight)| {
            cumulative += weight;
            cumulative * 2 > total
        })
        .map(|(price, _)| price)
}

/// A price in the coin's units (10^-(6 - szDecimals)) as a `SetGlobalAction` carries it: to 5 significant figures, half
/// to even, every integer digit kept (HL's order price rule: an integer price is valid at any length).
///
/// bm 10-27 11:00–12:00: every one of 187,792 set-global oracle and mark prices over 176 coins had at most 5 significant
/// figures, while 15% of the reference members had more; the reference so rounded matched the submitted mark in 39.3% of
/// them against 36.4% unrounded (ETH 48% against 17%, XRP 52% against 10%). The integer case is HL's documented rule,
/// unseen in a submission (no coin at 100,000 or above); half to even and half up matched alike (39.3%, 39.2%).
pub(crate) fn submitted_units(units: u64, size_decimals: u32) -> u64 {
    let digits = units.checked_ilog10().map_or(1, |log| log + 1);
    let dropped = digits.saturating_sub(SUBMITTED_SIGNIFICANT_FIGURES).min(MAX_PERP_DECIMALS - size_decimals);
    let step = 10_u64.pow(dropped);
    let (quotient, remainder) = (units / step, units % step);
    let half = step / 2;
    let up = remainder > half || (step > 1 && remainder == half && quotient % 2 == 1);
    (quotient + u64::from(up)) * step
}

/// A coin's inputs now, in its price units.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct MarkInputs<'a> {
    pub(crate) node: &'a CoinPriceState,
    /// The node's Mark reference cache median.
    pub(crate) mark_cache: u64,
    pub(crate) bid: u64,
    pub(crate) ask: u64,
    pub(crate) oracle: u64,
    pub(crate) external: u64,
    pub(crate) now_ns: u64,
}

/// The local marks and the terms they took, in the coin's price units.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct LocalMark {
    pub(crate) reference: u64,
    /// The reference as a `SetGlobalAction` would carry it.
    pub(crate) submitted: u64,
    pub(crate) predicted: u64,
    /// A, L and C.
    pub(crate) basis_adjusted_oracle: u64,
    pub(crate) local: u64,
    pub(crate) external: u64,
    pub(crate) oracle: u64,
    /// The previewed msoe's ratio and sample count.
    pub(crate) basis_ratio: f64,
    pub(crate) basis_samples: u64,
}

/// `Err` when a rule's calculated price leaves the node's range.
pub(crate) fn local_mark(inputs: MarkInputs<'_>) -> Result<LocalMark, String> {
    let MarkInputs { node, mark_cache, bid, ask, oracle, external, now_ns } = inputs;
    let basis = node.emas.basis.basis_advanced(bid.midpoint(ask), node.stored_oracle, now_ns);
    let basis_ratio = basis.basis_ratio();
    #[expect(clippy::cast_precision_loss, reason = "prices are below 2^53")]
    let basis_adjusted_oracle = internal_round(oracle as f64 * (1.0 + basis_ratio))?;
    let local = local_price(bid, ask, node.last_trade);
    let reference = median_units(&[basis_adjusted_oracle, local, external]).ok_or("no reference candidates")?;
    let submitted = submitted_units(reference, node.size_decimals);
    let predicted = median_units(&[submitted, mark_cache, local]).ok_or("no mark candidates")?;
    Ok(LocalMark { reference, submitted, predicted, basis_adjusted_oracle, local, external, oracle, basis_ratio, basis_samples: basis.samples })
}

#[cfg(test)]
pub(crate) mod tests {
    use alloy::primitives::Address;

    use super::*;
    use crate::accounts::marks::{
        native::MarkUpdate,
        seed::{Ema, NativeEmas, ReferenceCache},
    };

    fn price(text: &str) -> VenuePrice {
        VenuePrice::parse(text).unwrap()
    }

    /// The Python rule's cases: the lowest price past half the weight; an exact half moves on to the next price.
    #[test]
    fn the_weighted_median_takes_the_price_past_half_the_weight() {
        let median = |prices: &[(&str, u32)]| weighted_median(prices.iter().map(|(px, weight)| (price(px), *weight)).collect());
        assert_eq!(median(&[("100", 3), ("101", 2), ("102", 2), ("103", 1), ("104", 1)]), Some(price("101")));
        assert_eq!(median(&[("104", 3), ("100", 1), ("101", 1), ("102", 1)]), Some(price("104")), "3 of 6 is not past half");
        assert_eq!(median(&[("101", 1), ("100", 1)]), Some(price("101")));
        assert_eq!(median(&[("0.000004507", 1)]), Some(price("0.000004507")));
        assert_eq!(median(&[]), None);
    }

    /// A member of `btc_state`'s reference caches.
    pub(crate) const VALIDATOR: Address = Address::repeat_byte(0xee);

    /// A warm basis EMA of `ratio`, last sampled at `last_ns` by a `SetGlobalAction`, and BTC's other node state: one
    /// validator's member in each cache, refreshed in `last_ns`'s bucket.
    pub(crate) fn btc_state(ratio: f64, last_ns: u64) -> CoinPriceState {
        let warm = |numerator: f64| Ema { numerator, denominator_s: 100.0, decay_parameter: 120.0, samples: 1_000, last_sample_ns: Some(last_ns) };
        let cache = |px: u64| ReferenceCache { last_bucket: last_ns / 3_000_000_000, median: Some(px), members: vec![(VALIDATOR, px, last_ns)] };
        CoinPriceState {
            size_decimals: 5,
            stored_oracle: 1_000_000,
            stored_mark: 1_000_200,
            mark_time_ns: last_ns,
            mark_update: MarkUpdate::SetGlobal,
            emas: NativeEmas { basis: warm(ratio * 100.0), local_price: warm(10_000.0) },
            submitted: Some((1_000_010, 1_000_210)),
            spot_cache: cache(1_000_050),
            mark_cache: cache(1_000_100),
            last_trade: 1_000_300,
        }
    }

    const NOW_NS: u64 = 1_000_000_000_000;

    fn inputs(node: &CoinPriceState, oracle: u64, external: u64) -> MarkInputs<'_> {
        MarkInputs { node, mark_cache: node.mark_cache.median.unwrap(), bid: 1_000_000, ask: 1_000_400, oracle, external, now_ns: NOW_NS }
    }

    /// BTC (prices in 0.1): the basis 2 bp (the book's midpoint keeps it) moves the oracle 100,000.0 to A = 100,020.0;
    /// L = median(100,000.0, 100,040.0, last trade 100,030.0) = 100,030.0 is the median of A, L and C = 100,040.0; the
    /// outer median(100,030.0, cache 100,010.0, L) = 100,030.0, within 1% of the stored mark.
    #[test]
    fn the_reference_is_the_median_of_a_l_and_c_and_the_prediction_takes_the_outer_median() {
        let node = btc_state(0.0002, NOW_NS - 1_000_000_000);
        let mark = local_mark(inputs(&node, 1_000_000, 1_000_400)).unwrap();
        assert_eq!((mark.basis_adjusted_oracle, mark.local, mark.external), (1_000_200, 1_000_300, 1_000_400));
        assert_eq!((mark.reference, mark.predicted), (1_000_300, 1_000_300));
        assert_eq!(mark.basis_samples, 1_001, "previewed with the book's midpoint now");
        assert!((mark.basis_ratio - 0.0002).abs() < 1e-6);
        let high = local_mark(inputs(&node, 1_100_000, 1_100_000)).unwrap();
        assert_eq!((high.reference, high.predicted), (1_100_000, 1_000_300), "the outer median keeps L");
    }

    /// The prediction takes the reference as a `SetGlobalAction` carries it: A = 100,025.5 is the reference
    /// (L = 100,030.0, C = 100,020.0), submitted as the integer 100,026 (half to even), which the outer median keeps.
    #[test]
    fn the_prediction_takes_the_reference_as_submitted() {
        let node = btc_state(0.0002, NOW_NS - 1_000_000_000);
        let mark = local_mark(inputs(&node, 1_000_055, 1_000_200)).unwrap();
        assert_eq!((mark.basis_adjusted_oracle, mark.local, mark.external), (1_000_255, 1_000_300, 1_000_200));
        assert_eq!((mark.reference, mark.submitted, mark.predicted), (1_000_255, 1_000_260, 1_000_260));
    }

    /// Five significant figures in the coin's units, half to even; an integer price keeps its integer digits.
    #[test]
    fn a_submitted_price_keeps_five_significant_figures_and_every_integer_digit() {
        let cases = [
            // BTC (0.1): 80,169.5 and 80,168.5 to even, 80,169.6 up, 80,169.4 down
            (801_695, 5, 801_700),
            (801_685, 5, 801_680),
            (801_696, 5, 801_700),
            (801_694, 5, 801_690),
            // 123,456.7: the integer digits stay; 99,999.9 carries to 100,000
            (1_234_567, 5, 1_234_570),
            (999_999, 5, 1_000_000),
            // ETH (0.01) 2,315.99; INIT (0.000001) 0.158995; HEMI 0.004545 already has 4
            (231_599, 4, 231_600),
            (158_995, 0, 159_000),
            (4_545, 0, 4_545),
            (7, 0, 7),
        ];
        for (units, size_decimals, submitted) in cases {
            assert_eq!(submitted_units(units, size_decimals), submitted, "{units} at szDecimals {size_decimals}");
        }
    }

    /// The basis gap: an EMA last sampled longer ago than its decay resets, so the oracle stands unadjusted.
    #[test]
    fn a_basis_older_than_its_decay_previews_as_reset() {
        let node = btc_state(0.0002, NOW_NS - 121_000_000_000);
        let mark = local_mark(inputs(&node, 1_000_000, 1_000_400)).unwrap();
        assert_eq!((mark.basis_adjusted_oracle, mark.basis_samples, mark.basis_ratio), (1_000_000, 0, 0.0));
    }

    /// Main-dex preview uses the same uncapped median as the replay; changing the old mark cannot clip it.
    #[test]
    fn the_native_prediction_does_not_apply_the_hip3_step_limit() {
        let mut node = btc_state(0.0, NOW_NS - 1_000_000_000);
        node.mark_cache.median = Some(1_200_000);
        node.last_trade = 1_200_000;
        let mut far = inputs(&node, 1_200_000, 1_200_000);
        (far.bid, far.ask) = (1_199_900, 1_200_100);
        let mark = local_mark(far).unwrap();
        assert_eq!((mark.reference, mark.predicted), (1_200_000, 1_200_000));
        node.stored_mark = 1_400_000;
        let mut far = inputs(&node, 1_200_000, 1_200_000);
        (far.bid, far.ask) = (1_199_900, 1_200_100);
        assert_eq!(local_mark(far).unwrap().predicted, 1_200_000, "also no downward clipping");
    }
}
