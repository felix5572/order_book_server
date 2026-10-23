//! Native resting-order reservation arithmetic, shared by checkpoint validation and cash routing.
use serde::Deserialize;

/// Native tracker aggregate, not individual resting orders. Price is the tracker's cached mark, NOT today's mark.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub(crate) struct Reserve {
    #[serde(rename = "n")]
    pub(crate) notional_micro: [u64; 2],
    #[serde(rename = "s")]
    pub(crate) size_raw: [u64; 2],
    #[serde(rename = "P", default)]
    pub(crate) position_raw: i64,
    #[serde(rename = "p")]
    pub(crate) price_raw: u64,
    #[serde(rename = "l")]
    pub(crate) leverage: u32,
}

impl Reserve {
    /// 0x44c9000: opposite-side orders first close existing exposure; both the notional and integer average-price
    /// quantity bounds must remain nonnegative. The larger remaining side, divided once by leverage, is reserved.
    pub(crate) fn required_micro(&self) -> Option<i64> {
        if self.leverage == 0 || self.price_raw == 0 { return None; }
        let opposite = usize::from(self.position_raw > 0);
        let adding = 1 - opposite;
        let mut requirement = self.notional_micro[adding];
        let quantity = self.size_raw[opposite];
        let notional = self.notional_micro[opposite];
        if quantity != 0 && notional >= quantity {
            let twice_position = self.position_raw.unsigned_abs().checked_mul(2)?;
            let remaining_quantity = quantity.checked_sub(twice_position);
            let remaining_notional = notional.checked_sub(twice_position.checked_mul(self.price_raw)?);
            if let (Some(q), Some(n)) = (remaining_quantity, remaining_notional) {
                requirement = requirement.max(n.min((notional / quantity).checked_mul(q)?));
            }
        }
        i64::try_from(requirement / u64::from(self.leverage)).ok()
    }

    pub(crate) fn retained_micro(&self) -> Option<i64> { retained_micro(self.required_micro()?) }
}

/// Native 0x3c92a80 retains 101% of the aggregate reserve, not a rounded sum of per-asset retention.
#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
pub(crate) fn retained_micro(reserve_micro: i64) -> Option<i64> {
    // f64 multiply then truncate: rational 101/100 differs at floating-point boundaries.
    let value = reserve_micro as f64 * 1.01;
    (value >= 0.0 && value < i64::MAX as f64).then_some(value as i64)
}
