//! Exact decimal wire values (`"82160.0"`, `"0.00123"`) and their integer units. The node stores perp prices in
//! 10^-(6 - szDecimals) and sizes in 10^-szDecimals, so a written price or size with more decimals than its unit breaks
//! the contract; nothing is rounded.

use std::{cmp::Ordering, fmt};

/// `mantissa / 10^scale`, trailing fraction zeros dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ExactDecimal {
    mantissa: u64,
    scale: u32,
}

impl ExactDecimal {
    /// Digits with an optional fraction (`1.`, `.5`, signs and exponents are not decimals the node writes).
    pub(crate) fn parse(text: &str) -> Result<Self, String> {
        let (whole, fraction) = text.split_once('.').map_or((text, None), |(whole, fraction)| (whole, Some(fraction)));
        let digits = |part: &str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit());
        if !digits(whole) || fraction.is_some_and(|fraction| !digits(fraction)) {
            return Err(format!("{text:?} is not a decimal"));
        }
        let fraction = fraction.unwrap_or("").trim_end_matches('0');
        let scale = u32::try_from(fraction.len()).map_err(|err| format!("{text:?}: {err}"))?;
        let mantissa = format!("{whole}{fraction}").parse::<u64>().map_err(|err| format!("{text:?}: {err}"))?;
        Ok(Self { mantissa, scale })
    }

    /// `units` in 10^-decimals.
    pub(crate) const fn from_units(units: u64, decimals: u32) -> Self {
        let (mut mantissa, mut scale) = (units, decimals);
        while scale > 0 && mantissa % 10 == 0 {
            mantissa /= 10;
            scale -= 1;
        }
        Self { mantissa, scale }
    }

    /// The value in 10^-decimals.
    pub(crate) fn units(self, decimals: u32) -> Result<u64, String> {
        let shift = decimals.checked_sub(self.scale).ok_or_else(|| format!("{self} has more than {decimals} decimals"))?;
        10_u64
            .checked_pow(shift)
            .and_then(|scale| self.mantissa.checked_mul(scale))
            .ok_or_else(|| format!("{self} in 10^-{decimals} is out of range"))
    }
}

/// By value: the trailing zeros are dropped at the parse, so equal values have equal fields.
impl Ord for ExactDecimal {
    fn cmp(&self, other: &Self) -> Ordering {
        let scale = self.scale.max(other.scale);
        let scaled = |decimal: &Self| 10_u128.checked_pow(scale - decimal.scale).and_then(|factor| factor.checked_mul(u128::from(decimal.mantissa)));
        match (scaled(self), scaled(other)) {
            (Some(mine), Some(theirs)) => mine.cmp(&theirs),
            // only the one of fewer decimals is scaled up: overflowing, it holds more than any u64 of the other
            (None, _) => Ordering::Greater,
            (_, None) => Ordering::Less,
        }
    }
}

impl PartialOrd for ExactDecimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for ExactDecimal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let digits = self.mantissa.to_string();
        let scale = usize::try_from(self.scale).map_err(|_| fmt::Error)?;
        if scale == 0 {
            return formatter.write_str(&digits);
        }
        let padded = format!("{digits:0>width$}", width = scale + 1);
        let (whole, fraction) = padded.split_at(padded.len() - scale);
        write!(formatter, "{whole}.{fraction}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn units(text: &str, decimals: u32) -> Result<u64, String> {
        ExactDecimal::parse(text)?.units(decimals)
    }

    #[test]
    fn a_value_converts_exactly_or_fails() {
        assert_eq!(units("82160.0", 1), Ok(821_600));
        assert_eq!(units("2", 1), Ok(20));
        assert_eq!(units("0.00123", 5), Ok(123));
        assert_eq!(units("0.0", 3), Ok(0), "a size may be zero");
        assert_eq!(units("140.2385", 4), Ok(1_402_385));
        let error = units("1.23", 1).unwrap_err();
        assert!(error.contains("1.23 has more than 1 decimals"), "{error}");
        for text in ["", ".5", "1.", "-1.0", "1e3", "1,5", " 1"] {
            assert!(ExactDecimal::parse(text).is_err(), "{text:?}");
        }
        assert!(units("18446744073709551615", 1).is_err(), "overflows the unit");
    }

    #[test]
    fn decimals_order_by_value() {
        let parse = |text: &str| ExactDecimal::parse(text).unwrap();
        assert!(parse("0.67331") > parse("0.6733"));
        assert_eq!(parse("0.6733").cmp(&parse("0.67330")), Ordering::Equal);
        assert!(parse("80000") < parse("82850.0"));
        assert!(parse("18446744073709551615") > parse("0.00000000000000000001"), "an overflowing scale-up is the greater");
    }

    #[test]
    fn units_make_the_same_decimal_as_their_text() {
        assert_eq!(ExactDecimal::from_units(9_728_650, 8), ExactDecimal::parse("0.0972865").unwrap());
        assert_eq!(ExactDecimal::from_units(79_288_000, 6).to_string(), "79.288");
        assert_eq!(ExactDecimal::from_units(500, 0).to_string(), "500");
    }

    #[test]
    fn it_prints_as_written_without_trailing_zeros() {
        for (text, printed) in [("82160.0", "82160"), ("0.00123", "0.00123"), ("140.2385", "140.2385"), ("7", "7")] {
            assert_eq!(ExactDecimal::parse(text).unwrap().to_string(), printed);
        }
    }
}
