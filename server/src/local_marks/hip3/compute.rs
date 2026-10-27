//! One HIP-3 coin's local mark (design 000535 r1 §3): median((1), (2), (3)) in the coin's price units.
//!
//! - (1) P: the SIP NBBO midpoint, as the relayer publishes prices (`compute::submitted_units`: five significant
//!   figures, half to even), clamped to the stored oracle +-50 bp (the relayer clamps each update to +-50 bp of the
//!   current value).
//! - (2) P + the EMA gap, rounded alike.
//! - (3) median(HL bid, HL ask, last trade) now.
//!
//! v1 leaves out the pre-market offset carry, the relayer's read delay and any mark clamp (r1 §3: measured first).

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
    pub(crate) sip_bid: VenuePrice,
    pub(crate) sip_ask: VenuePrice,
    /// The EMA gap (2) - (1), price units.
    pub(crate) gap: f64,
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
    /// The SIP midpoint rounded, before the clamp.
    pub(crate) sip_mid: u64,
    /// (2).
    pub(crate) ema: u64,
    /// (3).
    pub(crate) book: u64,
}

/// `Err` when a price leaves the units' range.
pub(crate) fn hip3_local_mark(inputs: Hip3Inputs) -> Result<Hip3LocalMark, String> {
    let Hip3Inputs { size_decimals, stored_oracle, sip_bid, sip_ask, gap, bid, ask, last_trade } = inputs;
    let midpoint = sip_bid.midpoint(sip_ask).ok_or("the SIP midpoint is out of the price range")?;
    let sip_mid = submitted_units(midpoint.node_units(size_decimals)?, size_decimals);
    let oracle = sip_mid.clamp(clamp_floor(stored_oracle), clamp_ceiling(stored_oracle));
    #[expect(clippy::cast_precision_loss, reason = "prices are below 2^53")]
    let ema = submitted_units(internal_round(oracle as f64 + gap)?, size_decimals);
    let book = local_price(bid, ask, last_trade);
    let predicted = median_units(&[oracle, ema, book]).ok_or("no mark candidates")?;
    Ok(Hip3LocalMark { predicted, oracle, sip_mid, ema, book })
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
            size_decimals: 3, stored_oracle: 405_770, sip_bid: price("405.765"), sip_ask: price("405.775"), gap: -20.0,
            bid: 405_750, ask: 405_770, last_trade: 405_760,
        };
        let mark = hip3_local_mark(inputs).unwrap();
        assert_eq!(mark, Hip3LocalMark { predicted: 405_760, oracle: 405_770, sip_mid: 405_770, ema: 405_750, book: 405_760 });
    }

    #[test]
    fn the_oracle_is_rounded_as_published_and_clamped_to_fifty_bp() {
        // NVDA-like, szDecimals 3: 218.065 rounds to five figures, half to even (218.06)
        let base = Hip3Inputs { size_decimals: 3, stored_oracle: 218_060, sip_bid: price("218.06"), sip_ask: price("218.07"), gap: 0.0, bid: 218_060, ask: 218_070, last_trade: 218_060 };
        assert_eq!(hip3_local_mark(base).unwrap().oracle, 218_060);
        // a jump of 1%: (1) holds at the stored oracle + 50 bp (219.150), the midpoint stays shown
        let jump = Hip3Inputs { sip_bid: price("220.24"), sip_ask: price("220.26"), ..base };
        let mark = hip3_local_mark(jump).unwrap();
        assert_eq!((mark.sip_mid, mark.oracle), (220_250, 219_150));
        let fall = Hip3Inputs { sip_bid: price("215.00"), sip_ask: price("215.02"), ..base };
        assert_eq!(hip3_local_mark(fall).unwrap().oracle, 216_970, "218.06 x 0.995 = 216.9697, up to the unit inside");
    }
}
