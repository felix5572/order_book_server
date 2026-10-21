//! A venue's price as written (`"82806.00000000"`, `0.00000451000`, `1.2e-5`), exact: `mantissa / 10^scale`. Prices
//! and the listings' multipliers are scaled without rounding; the node units are taken at the median (batch 2).

use std::{cmp::Ordering, fmt};

use crate::accounts::marks::rules::wire_units;

/// Decimals a price may carry once its exponent is applied (u128 holds 38 digits).
const MAX_SCALE: u32 = 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct VenuePrice {
    mantissa: u128,
    /// Trailing fraction zeros dropped, so equal values have equal fields.
    scale: u32,
}

impl VenuePrice {
    /// Digits with an optional fraction and exponent; signs on the value, `inf` or `NaN` are not prices.
    pub(crate) fn parse(text: &str) -> Result<Self, String> {
        let (number, exponent) = match text.split_once(['e', 'E']) {
            Some((number, exponent)) => (number, exponent.parse::<i32>().map_err(|err| format!("{text:?}: exponent {err}"))?),
            None => (text, 0),
        };
        let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
        if (whole.is_empty() && fraction.is_empty()) || !whole.bytes().chain(fraction.bytes()).all(|byte| byte.is_ascii_digit()) {
            return Err(format!("{text:?} is not a decimal price"));
        }
        let out_of_range = || format!("{text:?} is out of the price range");
        let mut mantissa = 0_u128;
        for digit in whole.bytes().chain(fraction.bytes()) {
            mantissa = mantissa.checked_mul(10).and_then(|value| value.checked_add(u128::from(digit - b'0'))).ok_or_else(out_of_range)?;
        }
        let scale = i64::try_from(fraction.len()).map_err(|_| out_of_range())? - i64::from(exponent);
        Self::from_parts(mantissa, scale).ok_or_else(out_of_range)
    }

    /// `mantissa / 10^scale` for a scale of either sign, normalized; `None` out of range.
    fn from_parts(mut mantissa: u128, mut scale: i64) -> Option<Self> {
        while scale < 0 {
            mantissa = mantissa.checked_mul(10)?;
            scale += 1;
        }
        while scale > 0 && mantissa.is_multiple_of(10) {
            mantissa /= 10;
            scale -= 1;
        }
        let scale = u32::try_from(scale).ok().filter(|scale| *scale <= MAX_SCALE)?;
        Some(Self { mantissa, scale })
    }

    pub(crate) const fn is_positive(self) -> bool {
        self.mantissa > 0
    }

    /// The price times `multiplier` (a listing's), exactly; `None` out of range.
    pub(crate) fn scaled(self, multiplier: Self) -> Option<Self> {
        Self::from_parts(self.mantissa.checked_mul(multiplier.mantissa)?, i64::from(self.scale) + i64::from(multiplier.scale))
    }

    /// The midpoint of two prices, exactly: `(a + b) / 2 = (a + b) × 5 / 10`.
    pub(crate) fn midpoint(self, other: Self) -> Option<Self> {
        let scale = self.scale.max(other.scale);
        let aligned = |price: Self| 10_u128.checked_pow(scale - price.scale).and_then(|factor| factor.checked_mul(price.mantissa));
        let sum = aligned(self)?.checked_add(aligned(other)?)?;
        Self::from_parts(sum.checked_mul(5)?, i64::from(scale) + 1)
    }

    /// In the node's price units (10^-(6 - szDecimals)), rounded half to even: the reference ingestion rule.
    pub(crate) fn node_units(self, size_decimals: u32) -> Result<u64, String> {
        wire_units(&self.to_string(), size_decimals)
    }

    /// The value in 10^-decimals; `None` when it has more decimals or is out of range.
    #[cfg(test)]
    pub(crate) fn exact_units(self, decimals: u32) -> Option<u128> {
        self.mantissa.checked_mul(10_u128.checked_pow(decimals.checked_sub(self.scale)?)?)
    }
}

/// By value; only the one of fewer decimals is scaled up, and overflowing it holds more than any of the other.
impl Ord for VenuePrice {
    fn cmp(&self, other: &Self) -> Ordering {
        let scale = self.scale.max(other.scale);
        let scaled = |price: &Self| 10_u128.checked_pow(scale - price.scale).and_then(|factor| factor.checked_mul(price.mantissa));
        match (scaled(self), scaled(other)) {
            (Some(mine), Some(theirs)) => mine.cmp(&theirs),
            (None, _) => Ordering::Greater,
            (_, None) => Ordering::Less,
        }
    }
}

impl PartialOrd for VenuePrice {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for VenuePrice {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let digits = self.mantissa.to_string();
        let scale = self.scale as usize;
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

    fn price(text: &str) -> VenuePrice {
        VenuePrice::parse(text).unwrap()
    }

    /// The forms the feeds write (bm 10-21 frames): Binance's padded strings, MEXC's JSON numbers with trailing zeros,
    /// and the exponents a JSON number can carry.
    #[test]
    fn venue_prices_parse_exactly_in_every_written_form() {
        assert_eq!(price("82806.00000000"), price("82806"));
        assert_eq!(price("0.00000451000").to_string(), "0.00000451");
        assert_eq!(price("1.2e-5"), price("0.000012"));
        assert_eq!(price("4.5E3").to_string(), "4500");
        assert!(!price("0").is_positive() && price("0.000001").is_positive());
        assert!(price("0.000004507") < price("0.000004508"));
        assert!(price("82810") > price("82809.9"));
        for bad in ["", ".", "-1", "+1", "1.2.3", "1e", "NaN", "inf", "0x10", "1e-99"] {
            assert!(VenuePrice::parse(bad).is_err(), "{bad:?}");
        }
    }

    /// A listing's multiplier: kPEPE is 1,000 PEPE; Bybit's 1000TURBOUSDT is 0.001 of HL's TURBO.
    #[test]
    fn multipliers_scale_without_rounding() {
        assert_eq!(price("0.000004507").scaled(price("1000")).unwrap().to_string(), "0.004507");
        assert_eq!(price("1.2345").scaled(price("0.001")).unwrap().to_string(), "0.0012345");
        assert_eq!(price("0.004507").scaled(price("1")).unwrap(), price("0.004507"));
        assert_eq!(price("82806.01").exact_units(2), Some(8_280_601));
        assert_eq!(price("82806.015").exact_units(2), None, "more decimals than the unit");
    }

    #[test]
    fn midpoints_are_exact_and_node_units_round_half_to_even() {
        assert_eq!(price("2390.15").midpoint(price("2390.16")).unwrap().to_string(), "2390.155");
        assert_eq!(price("0.000004507").midpoint(price("0.000004508")).unwrap().to_string(), "0.0000045075");
        assert_eq!(price("100").midpoint(price("101")).unwrap().to_string(), "100.5");
        // ETH (szDecimals 4): 2 decimals; the half tie goes to even
        assert_eq!(price("2390.155").node_units(4), Ok(239_016));
        assert_eq!(price("2390.145").node_units(4), Ok(239_014));
        assert_eq!(price("82806.005").node_units(5), Ok(828_060));
    }
}
