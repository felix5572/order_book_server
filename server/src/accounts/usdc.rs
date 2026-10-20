//! USDC amounts of the cash replay (design 000433): integers in 10^-8 USDC, USDC's `weiDecimals`, so a spot balance
//! `t` (10^-8) and a dex's cash `u` (micro) both convert exactly.

use super::size::parse_size_raw;

/// Decimals of a replay amount.
pub(crate) const WEI_DECIMALS: u32 = 8;
/// Dex cash `u` and `ntli` are micro USDC.
pub(crate) const WEI_PER_MICRO: i64 = 100;

/// A node decimal amount in 10^-8 USDC, parsed as `parse_size_raw` parses sizes: exact up to 15 significant digits,
/// a float print tail beyond that rounded away at 8 decimals.
pub(crate) fn usdc_wei(text: &str) -> Result<i64, String> {
    parse_size_raw(text, WEI_DECIMALS).map_err(|err| format!("USDC amount: {err}"))
}

/// Micro USDC (a dex's `u`, an `ntli`) in 10^-8 USDC.
pub(crate) fn micro_to_wei(micro: i64) -> Result<i64, String> {
    micro.checked_mul(WEI_PER_MICRO).ok_or_else(|| format!("{micro} micro USDC overflows 10^-8 units"))
}

/// `px × sz` in 10^-8 USDC for a market whose sizes carry `size_decimals` and prices `price_decimals`
/// (perps: 6 − szDecimals; spot: 8 − the base token's szDecimals), so the product is a whole number of 10^-8.
pub(crate) fn notional_wei(px: &str, sz: &str, size_decimals: u32, price_decimals: u32) -> Result<i64, String> {
    let size_raw = parse_size_raw(sz, size_decimals)?;
    let price_raw = parse_size_raw(px, price_decimals).map_err(|err| format!("price: {err}"))?;
    let scale = WEI_DECIMALS
        .checked_sub(size_decimals + price_decimals)
        .ok_or_else(|| format!("{size_decimals} size and {price_decimals} price decimals exceed {WEI_DECIMALS}"))?;
    let wei = i128::from(size_raw) * i128::from(price_raw) * 10_i128.pow(scale);
    i64::try_from(wei).map_err(|err| format!("px {px:?} × sz {sz:?} overflows: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn amounts_are_exact_in_ten_to_the_minus_eight() {
        assert_eq!(usdc_wei("1.311558"), Ok(131_155_800));
        assert_eq!(usdc_wei("-0.00000001"), Ok(-1));
        assert_eq!(usdc_wei("0.30000000000000004"), Ok(30_000_000), "a float print tail");
        assert!(usdc_wei("0.000000001").is_err(), "finer than 10^-8");
        assert_eq!(micro_to_wei(-4_950_000), Ok(-495_000_000));
    }

    #[test]
    fn a_notional_is_price_times_size_at_the_markets_decimals() {
        // BTC: szDecimals 5, price decimals 1
        assert_eq!(notional_wei("82857.6", "0.00335", 5, 1), Ok(27_757_296_000));
        // a large STRK size printed with its float tail (szDecimals 1, price decimals 5)
        assert_eq!(notional_wei("0.12345", "85982089.099999994", 1, 5), Ok(1_061_448_889_939_500));
        // spot PURR/USDC: base szDecimals 0, price decimals 8
        assert_eq!(notional_wei("0.12345678", "3.0", 0, 8), Ok(37_037_034));
        assert!(notional_wei("82857.65", "0.00335", 5, 1).is_err(), "a price finer than the market");
    }
}
