//! Conversion between the node's decimal size strings and raw integer sizes in 10^-szDecimals base units, the unit
//! the checkpoint stores positions in.
//!
//! The node prints fill sizes from binary floating point: `startPosition` of large positions carries the f64 tail
//! ("85982089.099999994" for STRK's 85982089.1, "-2420832.7999999998"; 2026-10-19, `node_fills_streaming`), while
//! others read exact ("26982.0"). Parsing strategy, not a proof of where extra digits come from: a string of at most
//! `EXACT_DIGITS` significant digits is parsed as an exact decimal and must have at most szDecimals decimals; a longer
//! one is parsed as f64 and rounded to szDecimals, accepted when the rounding stays within an empirical tolerance
//! (which, for a long enough string, also accepts a genuine extra decimal such as "10000000000000.01" at 0 decimals).

/// f64 holds 15 significant decimal digits exactly; longer strings take the rounding path.
const EXACT_DIGITS: usize = 15;
/// Relative distance to the rounded raw size that float printing explains (f64 carries ~2.2e-16 relative precision
/// and scaling adds an ulp or two). Empirical: it rounds away the measured tails above.
const ROUNDING_TOLERANCE: f64 = 1e-14;
/// Raw sizes from 2^53 on are no longer exact in f64.
const MAX_EXACT_RAW: f64 = 9_007_199_254_740_992.0;

/// Parses a node size string (`-?digits[.digits]`) of a coin with `size_decimals`.
pub(crate) fn parse_size_raw(text: &str, size_decimals: u32) -> Result<i64, String> {
    let (negative, unsigned) = text.strip_prefix('-').map_or((false, text), |rest| (true, rest));
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, "0"));
    let all_digits = |part: &str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit());
    if !all_digits(whole) || !all_digits(fraction) {
        return Err(format!("size {text:?} is not a decimal"));
    }
    let fraction = fraction.trim_end_matches('0');
    let significant_digits = format!("{whole}{fraction}").trim_start_matches('0').len();
    let magnitude = if significant_digits > EXACT_DIGITS {
        float_print_raw(text, unsigned, size_decimals)?
    } else {
        exact_raw(text, whole, fraction, size_decimals)?
    };
    Ok(if negative { -magnitude } else { magnitude })
}

fn exact_raw(text: &str, whole: &str, fraction: &str, size_decimals: u32) -> Result<i64, String> {
    let padding = u32::try_from(fraction.len())
        .ok()
        .and_then(|decimals| size_decimals.checked_sub(decimals))
        .ok_or_else(|| format!("size {text:?} has more than {size_decimals} decimals"))?;
    format!("{whole}{fraction}")
        .parse::<i64>()
        .ok()
        .and_then(|digits| digits.checked_mul(10i64.checked_pow(padding)?))
        .ok_or_else(|| format!("size {text:?} overflows i64 at {size_decimals} decimals"))
}

fn float_print_raw(text: &str, unsigned: &str, size_decimals: u32) -> Result<i64, String> {
    let value: f64 = unsigned.parse().map_err(|err| format!("size {text:?}: {err}"))?;
    let scaled = value * 10f64.powi(size_decimals.cast_signed());
    let raw = scaled.round();
    if raw >= MAX_EXACT_RAW {
        return Err(format!("size {text:?} is beyond exact range at {size_decimals} decimals"));
    }
    if (scaled - raw).abs() > ROUNDING_TOLERANCE * raw.max(1.0) {
        return Err(format!("size {text:?} has more than {size_decimals} significant decimals"));
    }
    #[expect(clippy::cast_possible_truncation, reason = "an integral f64 below 2^53 converts exactly")]
    Ok(raw as i64)
}

/// The node's own spelling: at least one fraction digit, no trailing zeros beyond it ("269620.0", "-0.0335").
pub(crate) fn format_size_raw(size_raw: i64, size_decimals: u32) -> String {
    let sign = if size_raw < 0 { "-" } else { "" };
    let digits = size_raw.unsigned_abs().to_string();
    let decimals = size_decimals as usize;
    let padded = format!("{digits:0>width$}", width = decimals + 1);
    let (whole, fraction) = padded.split_at(padded.len() - decimals);
    let fraction = fraction.trim_end_matches('0');
    let fraction = if fraction.is_empty() { "0" } else { fraction };
    format!("{sign}{whole}.{fraction}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_node_spellings() {
        assert_eq!(parse_size_raw("26982.0", 0), Ok(26_982));
        assert_eq!(parse_size_raw("-2159.89", 2), Ok(-215_989));
        assert_eq!(parse_size_raw("0.00335", 5), Ok(335));
        assert_eq!(parse_size_raw("12", 3), Ok(12_000));
        assert_eq!(parse_size_raw("-0.0", 2), Ok(0));
    }

    /// Measured startPosition strings of 2026-10-19 (STRK and XAI have 1 decimal).
    #[test]
    fn rounds_away_the_float_tail_the_node_prints() {
        assert_eq!(parse_size_raw("85982089.099999994", 1), Ok(859_820_891));
        assert_eq!(parse_size_raw("-2420832.7999999998", 1), Ok(-24_208_328));
        assert_eq!(parse_size_raw("6460084.0999999996", 1), Ok(64_600_841));
        assert_eq!(parse_size_raw("-2422807.1000000001", 1), Ok(-24_228_071));
        assert_eq!(parse_size_raw("0.30000000000000004", 1), Ok(3));
    }

    #[test]
    fn rejects_extra_decimals_and_malformed_text() {
        for (text, decimals) in
            [("0.001", 2), ("0.15", 1), ("85982089.15", 1), ("1.0000001", 1), ("12.5", 0), ("10000000000000.1", 0)]
        {
            assert!(parse_size_raw(text, decimals).is_err(), "{text:?} at {decimals} should be rejected");
        }
        for text in ["1e3", "", "-", ".5", "5.", "1.2.3", "+1", " 1", "NaN", "inf"] {
            assert!(parse_size_raw(text, 2).is_err(), "{text:?} should be rejected");
        }
        assert!(parse_size_raw("99999999999999999.0", 3).is_err(), "beyond the exact range");
        assert!(parse_size_raw("999999999999999.0", 5).is_err(), "15 exact digits overflowing i64 at 5 decimals");
        assert_eq!(parse_size_raw("123456789012345.0", 0), Ok(123_456_789_012_345), "15 digits stay exact");
    }

    #[test]
    fn formats_like_the_node() {
        assert_eq!(format_size_raw(26_982, 0), "26982.0");
        assert_eq!(format_size_raw(-215_989, 2), "-2159.89");
        assert_eq!(format_size_raw(335, 5), "0.00335");
        assert_eq!(format_size_raw(-12_000, 3), "-12.0");
        assert_eq!(format_size_raw(0, 4), "0.0");
        assert_eq!(format_size_raw(859_820_891, 1), "85982089.1");
        for (text, decimals) in [("230.561", 3), ("-0.0335", 4), ("1.0", 1)] {
            assert_eq!(format_size_raw(parse_size_raw(text, decimals).unwrap(), decimals), text);
        }
    }
}
