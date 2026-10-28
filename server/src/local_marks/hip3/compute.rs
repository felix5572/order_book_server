//! One HIP-3 coin's local mark (design 000535 r1 §3): median((1), (2), (3)) in the coin's price units.
//!
//! - (1) P: the session's quote midpoint (the SIP's NBBO, BOATS's overnight: design 000543) plus the offset carry when
//!   there is one (design 000537), as the relayer publishes
//!   prices (`compute::submitted_units`: five significant figures, half to even), clamped to the stored oracle +-50 bp
//!   (the relayer clamps each update to +-50 bp of the current value).
//! - (2) P + the EMA gap, rounded alike.
//! - (3) median(HL bid, HL ask, last trade) now.
//!
//! Left out: the relayer's read delay (the midpoint now, not 1 s before the block) and any mark clamp.

use crate::{
    accounts::marks::rules::{internal_round, local_price, median_units},
    local_marks::{compute::submitted_units, price::VenuePrice},
};

/// The relayer's per-update clamp, in bp of the current value (docs.trade.xyz mark price).
const RELAYER_CLAMP_BPS: u64 = 50;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Hip3Inputs {
    pub(crate) size_decimals: u32,
    pub(crate) stored_oracle: u64,
    pub(crate) quote_bid: VenuePrice,
    pub(crate) quote_ask: VenuePrice,
    /// The EMA gap (2) - (1), price units.
    pub(crate) gap: f64,
    /// The offset carry added to the midpoint, price units; `None`: none (v1's (1)).
    pub(crate) carry: Option<f64>,
    pub(crate) bid: u64,
    pub(crate) ask: u64,
    pub(crate) last_trade: u64,
}

/// The terms and their median, in the coin's price units.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Hip3LocalMark {
    pub(crate) predicted: u64,
    /// (1) after rounding and the clamp.
    pub(crate) oracle: u64,
    /// The quote's midpoint rounded, before the clamp.
    pub(crate) quote_mid: u64,
    /// The midpoint plus the carry, rounded, before the clamp; `None` without a carry.
    pub(crate) carried: Option<u64>,
    /// (2).
    pub(crate) ema: u64,
    /// (3).
    pub(crate) book: u64,
}

/// `Err` when a price leaves the units' range.
pub(crate) fn hip3_local_mark(inputs: Hip3Inputs) -> Result<Hip3LocalMark, String> {
    let Hip3Inputs { size_decimals, stored_oracle, quote_bid, quote_ask, gap, carry, bid, ask, last_trade } = inputs;
    let midpoint = quote_bid.midpoint(quote_ask).ok_or("the quote's midpoint is out of the price range")?;
    let quote_mid = submitted_units(midpoint.node_units(size_decimals)?, size_decimals);
    let carried = carry.map(|carry| internal_round(midpoint.units_f64(size_decimals) + carry)).transpose()?.map(|units| submitted_units(units, size_decimals));
    let oracle = carried.unwrap_or(quote_mid).clamp(clamp_floor(stored_oracle), clamp_ceiling(stored_oracle));
    #[expect(clippy::cast_precision_loss, reason = "prices are below 2^53")]
    let ema = submitted_units(internal_round(oracle as f64 + gap)?, size_decimals);
    let book = local_price(bid, ask, last_trade);
    let predicted = median_units(&[oracle, ema, book]).ok_or("no mark candidates")?;
    Ok(Hip3LocalMark { predicted, oracle, quote_mid, carried, ema, book })
}

fn clamp_floor(current: u64) -> u64 {
    let scaled = u128::from(current) * u128::from(10_000 - RELAYER_CLAMP_BPS);
    u64::try_from(scaled.div_ceil(10_000)).unwrap_or_else(|_| unreachable!("below the current value"))
}

fn clamp_ceiling(current: u64) -> u64 {
    let scaled = u128::from(current) * u128::from(10_000 + RELAYER_CLAMP_BPS) / 10_000;
    u64::try_from(scaled).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn price(text: &str) -> VenuePrice {
        VenuePrice::parse(text).unwrap()
    }

    /// bm 10-27 17:51:02 TSLA (szDecimals 3, units 0.001): the relayer's (1) 405.77 and (2) 405.75, HL's (3) 405.76; the
    /// stored mark 405.76. Here the SIP stands at 405.765 x 405.775 and the gap at -0.02.
    #[test]
    fn the_median_of_the_three_terms() {
        let inputs = Hip3Inputs {
            size_decimals: 3, stored_oracle: 405_770, quote_bid: price("405.765"), quote_ask: price("405.775"), gap: -20.0,
            carry: None, bid: 405_750, ask: 405_770, last_trade: 405_760,
        };
        let mark = hip3_local_mark(inputs).unwrap();
        assert_eq!(mark, Hip3LocalMark { predicted: 405_760, oracle: 405_770, quote_mid: 405_770, carried: None, ema: 405_750, book: 405_760 });
    }

    #[test]
    fn the_oracle_is_rounded_as_published_and_clamped_to_fifty_bp() {
        // NVDA-like, szDecimals 3: 218.065 rounds to five figures, half to even (218.06)
        let base = Hip3Inputs { size_decimals: 3, stored_oracle: 218_060, quote_bid: price("218.06"), quote_ask: price("218.07"), gap: 0.0, carry: None, bid: 218_060, ask: 218_070, last_trade: 218_060 };
        assert_eq!(hip3_local_mark(base).unwrap().oracle, 218_060);
        // a jump of 1%: (1) holds at the stored oracle + 50 bp (219.150), the midpoint stays shown
        let jump = Hip3Inputs { quote_bid: price("220.24"), quote_ask: price("220.26"), ..base };
        let mark = hip3_local_mark(jump).unwrap();
        assert_eq!((mark.quote_mid, mark.oracle), (220_250, 219_150));
        let fall = Hip3Inputs { quote_bid: price("215.00"), quote_ask: price("215.02"), ..base };
        assert_eq!(hip3_local_mark(fall).unwrap().oracle, 216_970, "218.06 x 0.995 = 216.9697, up to the unit inside");
    }

    /// Design 000537: (1) is the midpoint plus the carry, rounded as published, then clamped; (2) follows it.
    #[test]
    fn the_carry_moves_the_oracle_term_before_the_clamp() {
        // NET-like post-market (szDecimals 3): the SIP 199.80 x 200.20, the aggregate 0.30 above the midpoint
        let base = Hip3Inputs { size_decimals: 3, stored_oracle: 200_300, quote_bid: price("199.8"), quote_ask: price("200.2"), gap: 10.0, carry: Some(297.5), bid: 200_250, ask: 200_350, last_trade: 200_300 };
        let mark = hip3_local_mark(base).unwrap();
        // 200.000 + 0.2975 = 200.2975: five figures, 200.30
        assert_eq!((mark.quote_mid, mark.carried, mark.oracle, mark.ema, mark.predicted), (200_000, Some(200_300), 200_300, 200_310, 200_300));
        // a carry past 50 bp of the stored oracle holds at the clamp, shown before it
        let far = Hip3Inputs { carry: Some(1_500.0), ..base };
        let mark = hip3_local_mark(far).unwrap();
        assert_eq!((mark.carried, mark.oracle), (Some(201_500), 201_301), "200.300 x 1.005, down to the unit inside");
        let none = hip3_local_mark(Hip3Inputs { carry: None, ..base }).unwrap();
        assert_eq!((none.carried, none.oracle), (None, 200_000), "no carry: v1's midpoint");
    }
}
