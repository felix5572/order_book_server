//! Initial isolated margin includes the immediate loss at mark, with fill-notional/leverage as a floor.
//! Checkpoints 1192420000→1192440000: 815/815 opens/increases and 58/58 flips explained by recorded marks.
//! These are measured client rules, not a claim that its closed-source implementation was recovered instruction by
//! instruction. See the golden's provenance. Missing/ambiguous execution marks retain the explicit old estimate.

use crate::{accounts::marks::fill_prices::FillMarkRange, order_book::Side};

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Transfer {
    pub(super) micro_usdc: i64,
    pub(super) estimated: bool,
}

/// `size_raw` is newly opened exposure (only the residual on a flip). The signed cap is pre-fill cross free margin
/// plus the old leg's net release. `None` means unknown cross valuation, not unlimited funding; keep it estimated.
/// Price and size scales add to six: the integral product is micro USDC, floored once after division.
pub(super) fn transfer(
    size_raw: i64, fill_px_raw: i64, side: Side, leverage: u32, size_decimals: u32, mark: Option<FillMarkRange>, cap_micro: Option<i64>,
) -> Result<Transfer, String> {
    if size_raw < 0 || fill_px_raw <= 0 || leverage == 0 { return Err("invalid isolated transfer size, price or leverage".into()); }
    let quantity = i128::from(size_raw);
    let price = i128::from(fill_px_raw);
    let lev = i128::from(leverage);
    let baseline = quantity * price / lev;
    let amount = |mark_raw: u64| -> Result<i128, String> {
        let mark = i128::from(mark_raw);
        let signed_loss = if side == Side::Bid { price - mark } else { mark - price };
        let requirement = price.max(mark + lev * signed_loss);
        quantity.checked_mul(requirement).map(|total| total / lev).ok_or_else(|| "isolated margin product overflows".into())
    };
    let capped = |amount: i128| cap_micro.map_or(amount, |cap| amount.min(i128::from(cap)));
    let exact = match mark {
        Some(mark) => {
            if mark.size_decimals != size_decimals || mark.min_px_raw == 0 || mark.min_px_raw > mark.max_px_raw {
                return Err("invalid isolated execution mark units or bounds".into());
            }
            let first = capped(amount(mark.min_px_raw)?);
            let last = capped(amount(mark.max_px_raw)?);
            // The requirement is monotone in mark for each side, including the flat fill-notional floor.
            (first == last).then_some(first)
        }
        // All possible marks require at least the fill-notional floor. A cap below that floor therefore
        // determines the exact transfer even when the mark is absent.
        None => cap_micro.filter(|cap| i128::from(*cap) <= baseline).map(i128::from),
    };
    let micro_usdc = i64::try_from(exact.unwrap_or_else(|| capped(baseline))).map_err(|err| format!("isolated transfer overflows: {err}"))?;
    Ok(Transfer { micro_usdc, estimated: cap_micro.is_none() || exact.is_none() })
}

/// The client 6911b792, 0x44cd549..0x44cd697, evaluates `cash * (closed / held)` in f64 before
/// truncating, not as an exact rational. PUMP 1192669509 differs by one micro on that boundary.
/// At negative equity at fill (0x44cd531..0x44cd540 -> 0x44cd7e0), it keeps the trade cash isolated.
#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
pub(super) fn reduced_cash(cash_after_fee: i64, start_size: i64, closed_size: i64, price_raw: i64) -> Result<i64, String> {
    if start_size == 0 || closed_size < 0 || closed_size.unsigned_abs() > start_size.unsigned_abs() || price_raw <= 0 {
        return Err("invalid isolated reduction size or price".into());
    }
    let equity = i128::from(cash_after_fee) + i128::from(start_size) * i128::from(price_raw);
    if equity < 0 {
        let cash = i128::from(cash_after_fee) + i128::from(start_size.signum()) * i128::from(closed_size) * i128::from(price_raw);
        return i64::try_from(cash).map_err(|err| format!("isolated reduction cash overflows: {err}"));
    }
    let fraction = closed_size as f64 / start_size.unsigned_abs() as f64;
    let moved = (cash_after_fee as f64 * fraction) as i64;
    cash_after_fee.checked_sub(moved).ok_or_else(|| "isolated reduction cash overflows".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(low: u64, high: u64) -> FillMarkRange {
        FillMarkRange { min_px_raw: low, max_px_raw: high, size_decimals: 0 }
    }

    #[test]
    fn adverse_execution_is_funded_in_both_directions_and_ambiguity_is_explicit() {
        assert_eq!(transfer(10, 100, Side::Bid, 5, 0, Some(range(90, 90)), Some(i64::MAX)).unwrap(), Transfer { micro_usdc: 280, estimated: false });
        assert_eq!(transfer(10, 90, Side::Ask, 5, 0, Some(range(100, 100)), Some(i64::MAX)).unwrap(), Transfer { micro_usdc: 300, estimated: false });
        assert_eq!(transfer(10, 100, Side::Bid, 5, 0, Some(range(100, 110)), Some(i64::MAX)).unwrap(), Transfer { micro_usdc: 200, estimated: false });
        assert_eq!(transfer(10, 100, Side::Bid, 5, 0, Some(range(90, 110)), Some(i64::MAX)).unwrap(), Transfer { micro_usdc: 200, estimated: true });
        assert_eq!(transfer(10, 100, Side::Bid, 5, 0, None, Some(i64::MAX)).unwrap(), Transfer { micro_usdc: 200, estimated: true });
        assert_eq!(transfer(10, 100, Side::Bid, 1, 0, Some(range(90, 110)), Some(i64::MAX)).unwrap(), Transfer { micro_usdc: 1_000, estimated: false });
    }

    #[test]
    fn one_floor_and_invalid_inputs() {
        assert_eq!(transfer(9, 10, Side::Bid, 3, 0, Some(range(9, 9)), Some(i64::MAX)).unwrap().micro_usdc, 36);
        assert!(transfer(1, 10, Side::Bid, 0, 0, Some(range(9, 9)), Some(i64::MAX)).is_err());
        assert!(transfer(1, 10, Side::Bid, 1, 2, Some(range(9, 9)), Some(i64::MAX)).is_err());
        assert!(transfer(i64::MAX, i64::MAX, Side::Ask, u32::MAX, 0, Some(range(u64::MAX, u64::MAX)), Some(i64::MAX)).is_err());
    }

    #[test]
    fn capacity_can_resolve_mark_ambiguity_but_unknown_capacity_cannot() {
        let capped = |mark, cap| transfer(10, 100, Side::Bid, 5, 0, mark, cap).unwrap();
        assert_eq!(capped(None, Some(150)), Transfer { micro_usdc: 150, estimated: false });
        assert_eq!(capped(Some(range(90, 110)), Some(150)), Transfer { micro_usdc: 150, estimated: false });
        assert_eq!(capped(Some(range(90, 110)), Some(250)), Transfer { micro_usdc: 200, estimated: true });
        assert_eq!(capped(Some(range(90, 90)), None), Transfer { micro_usdc: 280, estimated: true });
        assert_eq!(capped(None, Some(-5)), Transfer { micro_usdc: -5, estimated: false }, "old deficit stays isolated");
        assert!(transfer(10, 100, Side::Bid, 5, 0, Some(range(0, 90)), Some(0)).is_err(), "a cap never hides invalid input");
    }


    #[test]
    fn reduction_matches_client_float_boundary_and_negative_equity_branch() {
        let golden: serde_json::Value = serde_json::from_str(include_str!("isolated_cash_edge_goldens.json")).unwrap();
        let pump = &golden["pump"];
        let value = |key: &str| pump[key].as_i64().unwrap();
        assert_eq!(reduced_cash(value("cash_after_fee_micro"), value("held_size_raw"), value("closed_size_raw"), value("fill_price_raw")).unwrap(),
            value("checkpoint_cash_after_micro"), "PUMP H1192669509: float fraction matches the checkpoint, integer ratio does not");
        assert_eq!(reduced_cash(-1_000, 10, 2, 90).unwrap(), -820, "underwater long retains closing proceeds");
        assert_eq!(reduced_cash(800, -10, 2, 90).unwrap(), 620, "underwater short pays its own closing notional");
        assert_eq!(reduced_cash(-900, 10, 2, 90).unwrap(), -720, "zero equity takes the proportional branch");
        assert!(reduced_cash(1, 0, 1, 90).is_err());
        assert!(reduced_cash(1, 2, 3, 90).is_err());
    }

}
