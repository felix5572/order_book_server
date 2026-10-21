//! One coin's local marks (design 000454 r1 1.3, r2 revisions 2–3): the node's own rules (`accounts::marks::rules`, the
//! replay's implementation) on the node price state of the applied block, the HL book now, and the venues' oracle and
//! external price now.
//!
//! - A = oracle × (1 + basis), the basis msoe previewed at now with the book's midpoint against the stored oracle, as a
//!   validator's reference action previews it at its cut; L = median(bid, ask, last trade); C = the external price.
//! - reference = median(A, L, C): what a validator submitting our venue prices now would store as its Mark member.
//! - predicted = the 1% limit of median(reference, Mark cache, L) from the stored mark: a `SetGlobalAction` now whose
//!   submission is the reference. A conditional estimate, not the next mark: the cache refreshes per bucket and later
//!   submissions and books are unknown. Nothing is written back; the limit's base is always the node's stored mark.

use super::price::VenuePrice;
use crate::accounts::marks::{
    native::CoinPriceState,
    rules::{internal_round, local_price, median_units, step_limited_mark},
};

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
    let candidate = median_units(&[reference, mark_cache, local]).ok_or("no mark candidates")?;
    let predicted = step_limited_mark(candidate, node.stored_mark)?;
    Ok(LocalMark { reference, predicted, basis_adjusted_oracle, local, external, oracle, basis_ratio, basis_samples: basis.samples })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::accounts::marks::seed::{Ema, NativeEmas};

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

    /// A warm basis EMA of `ratio`, last sampled at `last_ns`, and BTC's other node state.
    pub(crate) fn btc_state(ratio: f64, last_ns: u64) -> CoinPriceState {
        let warm = |numerator: f64| Ema { numerator, denominator_s: 100.0, decay_parameter: 120.0, samples: 1_000, last_sample_ns: Some(last_ns) };
        CoinPriceState {
            size_decimals: 5,
            stored_oracle: 1_000_000,
            stored_mark: 1_000_200,
            emas: NativeEmas { basis: warm(ratio * 100.0), local_price: warm(10_000.0) },
            mark_cache: Some(1_000_100),
            last_trade: 1_000_300,
        }
    }

    const NOW_NS: u64 = 1_000_000_000_000;

    fn inputs(node: &CoinPriceState, oracle: u64, external: u64) -> MarkInputs<'_> {
        MarkInputs { node, mark_cache: node.mark_cache.unwrap(), bid: 1_000_000, ask: 1_000_400, oracle, external, now_ns: NOW_NS }
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

    /// The basis gap: an EMA last sampled longer ago than its decay resets, so the oracle stands unadjusted.
    #[test]
    fn a_basis_older_than_its_decay_previews_as_reset() {
        let node = btc_state(0.0002, NOW_NS - 121_000_000_000);
        let mark = local_mark(inputs(&node, 1_000_000, 1_000_400)).unwrap();
        assert_eq!((mark.basis_adjusted_oracle, mark.basis_samples, mark.basis_ratio), (1_000_000, 0, 0.0));
    }

    /// The prediction moves at most 1% of the stored mark, by the node's rule.
    #[test]
    fn the_prediction_is_step_limited_from_the_stored_mark() {
        let mut node = btc_state(0.0, NOW_NS - 1_000_000_000);
        node.mark_cache = Some(1_200_000);
        node.last_trade = 1_200_000;
        let mut far = inputs(&node, 1_200_000, 1_200_000);
        (far.bid, far.ask) = (1_199_900, 1_200_100);
        let mark = local_mark(far).unwrap();
        assert_eq!((mark.reference, mark.predicted), (1_200_000, step_limited_mark(1_200_000, 1_000_200).unwrap()));
        assert_eq!(mark.predicted, 1_010_202);
    }
}
