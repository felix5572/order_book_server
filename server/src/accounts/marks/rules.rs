//! The node's price rules for main-dex perps (design 000439 r1 section 3), ported from the trading system's
//! `mark_rules.rs` (Codex, binary 6911b792) and the research replay (`hl_node_mark_state_20261019`: 181 of 183 coins
//! bit-exact over three intervals). Every float operation keeps the node's order: the replay is checked bit for bit
//! against each checkpoint. The shared goldens (`golden_6911b792.json`, copied from the trading system) bind both ports.

use super::{book::BestPrices, decimal::ExactDecimal, seed::Ema};

const NANOS_PER_SECOND: u64 = 1_000_000_000;
/// Prices are positive integers below 2^53 (`mark_rules.rs` `price`).
const MAX_PRICE_UNITS: u64 = 1 << 53;

/// A validator's or deployer's wire price (`"82855.45"`) in integer price units, rounded half to even: the reference
/// ingestion rule (7,766 archived wire-to-state comparisons, exact half ties included), not the order grid.
pub(crate) fn wire_units(text: &str, size_decimals: u32) -> Result<u64, String> {
    let decimals = 6_u32.checked_sub(size_decimals).ok_or_else(|| format!("szDecimals {size_decimals}"))?;
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    let digits = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
    if whole.is_empty() || !digits(whole) || !digits(fraction) || (text.contains('.') && fraction.is_empty()) {
        return Err(format!("wire price {text:?} is not a decimal"));
    }
    let keep = (decimals as usize).min(fraction.len());
    let (kept, dropped) = fraction.split_at(keep);
    let padding = 10_u128.pow(decimals - u32::try_from(keep).map_err(|err| format!("{text:?}: {err}"))?);
    let truncated = format!("{whole}{kept}").parse::<u128>().map_err(|err| format!("wire price {text:?}: {err}"))? * padding;
    // half to even on the dropped digits
    let round_up = dropped.bytes().next().is_some_and(|first| {
        let rest_zero = dropped.bytes().skip(1).all(|byte| byte == b'0');
        first > b'5' || (first == b'5' && (!rest_zero || truncated % 2 == 1))
    });
    let units = truncated + u128::from(round_up);
    let units = u64::try_from(units).map_err(|err| format!("wire price {text:?}: {err}"))?;
    if units == 0 || units >= MAX_PRICE_UNITS {
        return Err(format!("wire price {text:?} outside the positive price range"));
    }
    Ok(units)
}

/// The Spot input a spot book gives the main-dex perp `psro` maps to it (Codex 0x2da7470..0x2da7942): the floor of the
/// integer midpoint, read as a price with the book's `price_decimals`, in the perp's units rounded half to even
/// (`wire_units`; not the order grid). `None`, no input, with an empty side or a zero midpoint (0x2da772e).
pub(crate) fn spot_reference_units((bid, ask): BestPrices, price_decimals: u32, size_decimals: u32) -> Result<Option<u64>, String> {
    let (Some(bid), Some(ask)) = (bid, ask) else { return Ok(None) };
    let mid = bid.midpoint(ask);
    if mid == 0 {
        return Ok(None);
    }
    wire_units(&ExactDecimal::from_units(mid, price_decimals).to_string(), size_decimals).map(Some)
}

/// The node's float-to-units rounding (0x644af40): half away from zero for positive values, at least 1.
pub(crate) fn internal_round(value: f64) -> Result<u64, String> {
    #[expect(clippy::cast_precision_loss, reason = "2^52 is exact in f64")]
    let limit = (1_u64 << 52) as f64;
    if !value.is_finite() || !(0.0..limit).contains(&value) {
        return Err(format!("calculated price {value} outside the normal range"));
    }
    let almost_half = f64::from_bits(0.5_f64.to_bits() - 1);
    #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss, reason = "a non-negative value below 2^52")]
    Ok(((value + almost_half).trunc() as u64).max(1))
}

/// The node's median (0x3b56140): the middle value, or the floor of the middle pair's mean; `None` for no value.
pub(crate) fn median_units(values: &[u64]) -> Option<u64> {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let middle = sorted.len() / 2;
    match sorted.len() {
        0 => None,
        count if count % 2 == 1 => Some(sorted[middle]),
        _ => Some((sorted[middle - 1] & sorted[middle]) + ((sorted[middle - 1] ^ sorted[middle]) >> 1)),
    }
}

/// The local price `L`: median(bid, ask, last trade).
pub(crate) fn local_price(bid: u64, ask: u64, last_trade: u64) -> u64 {
    median_units(&[bid, ask, last_trade]).unwrap_or(last_trade)
}

/// Undoes the basis on the local price for the stored oracle's third candidate; the binary's order, not
/// `local / (1 + ratio)`.
pub(crate) fn inverse_basis(local: u64, ratio: f64) -> Result<u64, String> {
    let correction = if (1.0 + ratio).abs() >= 1e-15 { -ratio / (1.0 + ratio) } else { 0.0 };
    if correction.abs() >= 0.5 {
        return Ok(local);
    }
    #[expect(clippy::cast_precision_loss, reason = "prices are below 2^53")]
    internal_round(local as f64 * (1.0 + correction))
}

/// `a * b + c` rounded twice, as the node computes the EMA denominator (only the numerator is fused).
#[expect(clippy::suboptimal_flops, reason = "the node multiplies, then adds")]
fn unfused_mul_add(a: f64, b: f64, c: f64) -> f64 {
    a * b + c
}

impl Ema {
    /// Seconds since the last sample as the node counts them: whole seconds plus the remaining nanoseconds, 1.0 before
    /// the first sample; an execution time before the last sample counts 0.
    fn elapsed_s(&self, at_ns: u64) -> f64 {
        let Some(last) = self.last_sample_ns else {
            return 1.0;
        };
        let elapsed = at_ns.saturating_sub(last);
        #[expect(clippy::cast_precision_loss, reason = "seconds and nanoseconds below 2^53")]
        let seconds = (elapsed / NANOS_PER_SECOND) as f64 + (elapsed % NANOS_PER_SECOND) as f64 / 1e9;
        seconds
    }

    /// One sample `sample` taken `dt` seconds after the last: `numerator' = fma(sample, dt, numerator q)`,
    /// `denominator' = denominator q + dt`, `q = 1 - 1 / decay_parameter`.
    fn sampled(&self, sample: f64, dt: f64, at_ns: u64) -> Self {
        let keep = 1.0 - 1.0 / self.decay_parameter;
        Self {
            numerator: sample.mul_add(dt, self.numerator * keep),
            denominator_s: unfused_mul_add(self.denominator_s, keep, dt),
            decay_parameter: self.decay_parameter,
            samples: self.samples + 1,
            last_sample_ns: Some(at_ns),
        }
    }

    /// msoe after one sample (0x37454a0): the sample is `mid / stored oracle - 1` clipped to [-1, 1]; a gap longer
    /// than the decay parameter resets the EMA and skips the sample.
    pub(crate) fn basis_advanced(&self, mid: u64, stored_oracle: u64, at_ns: u64) -> Self {
        let dt = self.elapsed_s(at_ns);
        if dt > self.decay_parameter {
            return Self { numerator: 0.0, denominator_s: 0.0, decay_parameter: self.decay_parameter, samples: 0, last_sample_ns: None };
        }
        #[expect(clippy::cast_precision_loss, reason = "prices are below 2^53")]
        let sample = (mid as f64 / stored_oracle as f64 - 1.0).clamp(-1.0, 1.0);
        self.sampled(sample, dt, at_ns)
    }

    /// `decay_parameter` samples or more and a non-zero denominator.
    fn is_warm(&self) -> bool {
        #[expect(clippy::cast_precision_loss, reason = "a sample count below 2^53")]
        let samples = self.samples as f64;
        samples >= self.decay_parameter && self.denominator_s >= 1e-15
    }

    /// The basis the selector applies (0x37451e0): zero until warm, clipped to [-1, 1].
    pub(crate) fn basis_ratio(&self) -> f64 {
        if !self.is_warm() {
            return 0.0;
        }
        let ratio = self.numerator / self.denominator_s;
        if ratio.is_finite() { ratio.clamp(-1.0, 1.0) } else { 0.0 }
    }

    /// The average a two-candidate reference reads from previewed lmpe (0x458b440), in prices; `None` before it is
    /// warm, a branch never observed (Codex's replay stops there).
    pub(crate) fn warm_average(&self) -> Option<f64> {
        self.is_warm().then(|| self.numerator / self.denominator_s)
    }

    /// lmpe after one sample of the local price in prices (`L / 10^(6 - szDecimals)`); `None` for a gap longer than
    /// the decay parameter, whose branch was never observed (research `lmpe_replay_check.py`).
    pub(crate) fn local_price_advanced(&self, sample: f64, at_ns: u64) -> Option<Self> {
        let dt = self.elapsed_s(at_ns);
        (dt <= self.decay_parameter).then(|| self.sampled(sample, dt, at_ns))
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    /// bm 2026-10-20, the validator member written in each checkpoint's own block against that block's last spot
    /// book: PURR/USDC (8 price decimals, perp szDecimals 0) at 1190920000 and 1190890000, both half ties to even;
    /// HYPE/USDC `@107` (6, perp 2) at 1190920000; Codex's HYPE sample at block 1189999977 (78.690 / 78.691).
    #[test]
    fn a_spot_book_gives_its_floor_midpoint_half_to_even() {
        assert_eq!(spot_reference_units((Some(9_721_800), Some(9_735_500)), 8, 0), Ok(Some(97_286)), "97,286.5");
        assert_eq!(spot_reference_units((Some(9_729_900), Some(9_751_600)), 8, 0), Ok(Some(97_408)), "97,407.5");
        assert_eq!(spot_reference_units((Some(79_288_000), Some(79_289_000)), 6, 2), Ok(Some(792_885)));
        assert_eq!(spot_reference_units((Some(78_690_000), Some(78_691_000)), 6, 2), Ok(Some(786_905)));
        assert_eq!(spot_reference_units((Some(3), Some(4)), 2, 4), Ok(Some(3)), "the floor midpoint 0.03, not 0.035");
        assert_eq!(spot_reference_units((None, Some(4)), 2, 4), Ok(None));
        assert_eq!(spot_reference_units((Some(0), Some(1)), 2, 4), Ok(None), "a zero midpoint");
    }

    #[derive(Deserialize)]
    struct SpotGolden {
        cases: Vec<SpotCase>,
    }

    #[derive(Deserialize)]
    struct SpotCase {
        coin: String,
        spot_bbo: (u64, u64),
        spot_units: u64,
    }

    /// Codex's 132 members (66 HYPE, 66 PURR, 10 half ties) at three checkpoints of 2026-10-19: the spot book at the
    /// validator's action gives the Spot member the checkpoint stored (checked 132 of 132, value and time).
    #[test]
    fn spot_reference_goldens() {
        let golden: SpotGolden = serde_json::from_str(include_str!("spot_reference_goldens.json")).unwrap();
        assert_eq!(golden.cases.len(), 132);
        for case in golden.cases {
            let (price_decimals, size_decimals) = match case.coin.as_str() {
                "HYPE" => (6, 2),
                "PURR" => (8, 0),
                other => panic!("{other}"),
            };
            let (bid, ask) = case.spot_bbo;
            assert_eq!(spot_reference_units((Some(bid), Some(ask)), price_decimals, size_decimals), Ok(Some(case.spot_units)), "{} {bid}/{ask}", case.coin);
        }
    }

    #[derive(Deserialize)]
    struct WireCase {
        wire: String,
        sz_decimals: u32,
        expected_units: u64,
    }

    #[derive(Deserialize)]
    struct SelectorCase {
        coin: String,
        submitted_wire: String,
        sz_decimals: u32,
        cached_reference_units: u64,
        book_units: Vec<u64>,
        expected_units: u64,
    }

    #[derive(Deserialize)]
    struct CacheCase {
        previous_values: Vec<u64>,
        expected_units: u64,
    }

    #[derive(Deserialize)]
    struct Golden {
        wire: Vec<WireCase>,
        selectors: Vec<SelectorCase>,
        caches: Vec<CacheCase>,
    }

    /// The trading system's goldens (`mark_rules/golden_6911b792.json`, copied): checkpoint and wire observations.
    #[test]
    fn the_shared_goldens_hold() {
        let golden: Golden = serde_json::from_str(include_str!("golden_6911b792.json")).unwrap();
        assert!(golden.wire.len() >= 200);
        for row in golden.wire {
            assert_eq!(wire_units(&row.wire, row.sz_decimals), Ok(row.expected_units), "{}", row.wire);
        }
        for row in golden.selectors {
            let submitted = wire_units(&row.submitted_wire, row.sz_decimals).unwrap();
            let local = median_units(&row.book_units).unwrap();
            assert_eq!(median_units(&[submitted, row.cached_reference_units, local]), Some(row.expected_units), "{}", row.coin);
        }
        for row in golden.caches {
            assert_eq!(median_units(&row.previous_values), Some(row.expected_units));
        }
    }

    fn seeded() -> Ema {
        Ema { numerator: 3.6, denominator_s: 360.0, decay_parameter: 120.0, samples: 119, last_sample_ns: Some(1_000_000_000) }
    }

    /// `mark_rules.rs` tests: warm-up counts samples; a preview is a value, not a commit.
    #[test]
    fn warm_up_counts_samples() {
        let before = seeded();
        assert_eq!(before.basis_ratio(), 0.0);
        let after = before.basis_advanced(10_100, 10_000, 4_000_000_000);
        assert!((after.numerator - 3.6).abs() < 1e-14);
        assert_eq!((after.denominator_s, after.samples), (360.0, 120));
        assert!((after.basis_ratio() - 0.01).abs() < 1e-15);
        assert_eq!(before, seeded(), "advancing returns a new state");
    }

    #[test]
    fn irregular_elapsed_decays_per_update_and_a_gap_resets_skipping_the_sample() {
        let state = seeded().basis_advanced(10_000, 10_000, 8_000_000_000);
        assert_eq!(state.denominator_s, 364.0, "360 * 119/120 + 7");
        let reset = state.basis_advanced(10_000, 10_000, 129_000_000_000);
        assert_eq!((reset.denominator_s, reset.samples, reset.last_sample_ns), (0.0, 0, None));
        let first = reset.basis_advanced(10_000, 10_000, 130_000_000_000);
        assert_eq!(first.denominator_s, 1.0);
        let backwards = first.basis_advanced(10_000, 10_000, 129_000_000_000);
        assert_eq!((backwards.denominator_s, backwards.samples), (119.0 / 120.0, 2), "a regressing clock counts 0 s");
        assert_eq!(seeded().local_price_advanced(1.0, 200_000_000_000), None, "an unobserved lmpe gap");
    }

    #[test]
    fn rounding_layers_are_distinct() {
        assert_eq!(wire_units("82855.45", 5), Ok(828_554), "half to even");
        assert_eq!(wire_units("82855.55", 5), Ok(828_556));
        assert_eq!(wire_units("82855.451", 5), Ok(828_555), "above the half");
        assert_eq!(wire_units("2433.105", 4), Ok(243_310));
        assert_eq!(internal_round(243_310.5), Ok(243_311));
        assert_eq!(internal_round(0.2), Ok(1), "at least 1");
        assert_eq!(median_units(&[11, 14]), Some(12));
        assert_eq!(median_units(&[]), None);
        for (text, size_decimals) in [("NaN", 5), ("0", 5), ("100", 7), ("1.", 2), (".5", 2), ("-1", 2), ("1e3", 2)] {
            assert!(wire_units(text, size_decimals).is_err(), "{text:?}");
        }
    }

    #[test]
    fn the_inverse_basis_keeps_the_binarys_order() {
        assert_eq!(inverse_basis(10_000, 0.0), Ok(10_000));
        assert_eq!(inverse_basis(10_100, 0.01), Ok(10_000), "10100 * (1 - 0.01 / 1.01)");
        assert_eq!(inverse_basis(10_000, -0.9), Ok(10_000), "a correction from 0.5 up leaves L");
    }
}
