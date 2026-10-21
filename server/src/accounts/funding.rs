//! Cash-only residual of a full dex funding batch. Archived client 6911b792: per-user 0x4264330,
//! per-dex 0x3c8f2d0. The node gives rounding residue to the largest single eligible position at stored oracle,
//! lower address first on ties, original position iteration order within one user. This does not alter the wire
//! funding payments or cumulative funding. See research-data/ob_hourly_watch_20261021/funding_rule_followup_0825.

use alloy::primitives::Address;
use rustc_hash::FxHashMap;

use super::{misc::FundingDelta, size::parse_size_raw};

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct CashAdjustment {
    pub(crate) user: Address,
    pub(crate) coin: String,
    pub(crate) amount_wei: i64,
}

/// `oracle` is from immediately before the settlement block, in 10^-(6-szDecimals), with its size decimals.
/// Every wire delta is already eligible, even an amount/rate printed as zero. Do not filter or select by rate text.
pub(crate) fn balance(deltas: &[FundingDelta], oracle: impl Fn(&str) -> Option<(u64, u32)>)
    -> Result<Option<CashAdjustment>, String>
{
    fn dex_of(coin: &str) -> &str { coin.split_once(':').map_or("", |(dex, _)| dex) }
    let Some(first) = deltas.first() else { return Ok(None) };
    let dex = dex_of(&first.coin);
    let mut net = 0_i128;
    for delta in deltas {
        if dex_of(&delta.coin) != dex { return Err("Funding batch spans multiple dexes".to_string()); }
        net += i128::from(delta.amount_wei);
    }
    if net == 0 { return Ok(None); }
    let amount_wei = i64::try_from(-net).map_err(|_| format!("{dex} funding residual overflows"))?;
    let mut prices = FxHashMap::default();
    let mut selected: Option<(&FundingDelta, u128)> = None;
    for delta in deltas {
        let (px, decimals) = *match prices.entry(delta.coin.as_str()) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let price = oracle(&delta.coin).ok_or_else(|| format!("{}: settlement-time stored oracle unavailable", delta.coin))?;
                if price.0 == 0 { return Err(format!("{}: zero stored oracle", delta.coin)); }
                entry.insert(price)
            }
        };
        let size = parse_size_raw(&delta.size, decimals).map_err(|err| format!("{} funding size: {err}", delta.coin))?;
        if size == 0 { return Err(format!("{}: zero-size funding candidate", delta.coin)); }
        let notional = u128::from(px) * u128::from(size.unsigned_abs());
        if selected.is_none_or(|(old, largest)| notional > largest || (notional == largest && delta.user < old.user)) {
            selected = Some((delta, notional));
        }
    }
    let (selected, _) = selected.ok_or("funding residual without a recipient")?;
    Ok(Some(CashAdjustment { user: selected.user, coin: selected.coin.clone(), amount_wei }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delta(user: u8, coin: &str, size: &str, amount_wei: i64) -> FundingDelta {
        FundingDelta { user: Address::repeat_byte(user), coin: coin.to_string(), size: size.to_string(), amount_wei }
    }

    #[test]
    fn selects_single_largest_oracle_notional_from_every_delta_including_zero_amounts() {
        let rows = vec![delta(1, "BTC", "2", -100), delta(1, "ETH", "2", 200), delta(2, "ETH", "-4", 0)];
        let oracle = |coin: &str| Some((if coin == "BTC" { 150 } else { 100 }, 0));
        let expected = CashAdjustment { user: Address::repeat_byte(2), coin: "ETH".to_string(), amount_wei: -100 };
        assert_eq!(balance(&rows, oracle), Ok(Some(expected)));
        let rows = vec![delta(9, "xyz:X", "3", -100), delta(1, "xyz:X", "1", 0)];
        assert_eq!(balance(&rows, |_| Some((100, 0))).unwrap().unwrap().amount_wei, 100, "positive adjustment too");
    }

    #[test]
    fn ties_choose_lowest_user_then_first_wire_position_of_that_user() {
        let rows = vec![delta(2, "BTC", "1", 100), delta(1, "ETH", "-2", 0), delta(1, "BTC", "1", 0)];
        let got = balance(&rows, |coin| Some((if coin == "BTC" { 200 } else { 100 }, 0))).unwrap().unwrap();
        assert_eq!((got.user, got.coin.as_str()), (Address::repeat_byte(1), "ETH"));
    }

    #[test]
    fn zero_net_needs_no_price_but_missing_price_never_guesses_a_recipient() {
        assert_eq!(balance(&[], |_| panic!()), Ok(None));
        assert_eq!(balance(&[delta(1, "BTC", "1", 100), delta(2, "BTC", "-1", -100)], |_| panic!()), Ok(None));
        let rows = [delta(1, "BTC", "1", 100), delta(2, "ETH", "1", 0)];
        assert!(balance(&rows, |coin| (coin == "BTC").then_some((100, 0))).unwrap_err().contains("ETH"));
        assert!(balance(&[delta(1, "BTC", "1", 100), delta(2, "xyz:X", "1", 100)], |_| Some((1, 0))).is_err());
        assert!(balance(&[delta(1, "BTC", "0", 100)], |_| Some((1, 0))).is_err());
    }

    #[test]
    fn size_print_tails_use_existing_size_normalization_and_the_right_units() {
        let rows = [delta(1, "X", "85982089.099999994", 100), delta(2, "BTC", "0.01", 0)];
        let got = balance(&rows, |coin| Some(if coin == "X" { (12345, 1) } else { (828_576, 5) })).unwrap().unwrap();
        assert_eq!(got.user, Address::repeat_byte(1));
    }
    /// Full original batches stay outside git. Uses preceding checkpoint oracles, not a fabricated event-time
    /// oracle; checks recipient stability and the ten observed cash residuals, not whole-window mark replay.
    #[test]
    #[ignore = "requires OB_FUNDING_ARCHIVE pointing to the dedicated research data root"]
    fn archived_two_hours_match_ten_observed_cash_adjustments() {
        use std::{fs::File, io::{BufRead, BufReader}, path::PathBuf};
        use flate2::read::GzDecoder;
        use crate::{accounts::misc::MiscEvent, types::node_data::Batch};
        let root = PathBuf::from(std::env::var("OB_FUNDING_ARCHIVE").expect("explicit archive root"));
        let golden: serde_json::Value = serde_json::from_str(include_str!("funding_goldens.json")).unwrap();
        let windows = [(1_191_654_810, "1191650000", "20261021T073919Z/misc_events_streaming_affected_accounts.jsonl.gz"),
            (1_191_705_145, "1191700000", "account_funding_followup_0755/funding_0800_raw.jsonl.gz")];
        let mut checked = 0;
        let mut records = 0;
        for (height, seed, file) in windows {
            let state: serde_json::Value = serde_json::from_reader(File::open(root.join(
                format!("account_funding_followup_0755/{seed}_selected_states.json"))).unwrap()).unwrap();
            let mut prices = FxHashMap::default();
            for dex in state["locus"]["cls"].as_array().unwrap() {
                for (i, asset) in dex["meta"]["universe"].as_array().unwrap().iter().enumerate() {
                    if let Some(px) = dex["oracle"]["pxs2"][i][0]["p"].as_u64().filter(|p| *p > 0) {
                        prices.insert(asset["name"].as_str().unwrap().to_string(), (px, u32::try_from(asset["szDecimals"].as_u64().unwrap()).unwrap()));
                    }
                }
            }
            let reader = BufReader::new(GzDecoder::new(File::open(root.join(file)).unwrap()));
            let mut dexes = std::collections::HashSet::new();
            for line in reader.lines() {
                let batch: Batch<MiscEvent> = sonic_rs::from_str(&line.unwrap()).unwrap();
                if batch.block_number() != height { continue; }
                for event in batch.events() {
                    let MiscEvent::Funding(deltas) = event else { continue };
                    let Some(first) = deltas.first() else { continue };
                    let dex = first.coin.split_once(':').map_or("", |(dex, _)| dex);
                    let Some(expected) = golden["cases"].as_array().unwrap().iter().find(|case|
                        case["height"] == height && case["dex"] == dex) else { continue };
                    assert!(dexes.insert(dex.to_string()), "duplicate funding batch");
                    let adjustment = balance(&deltas, |coin| prices.get(coin).copied()).unwrap().unwrap();
                    assert_eq!(adjustment.user, expected["user"].as_str().unwrap().parse::<Address>().unwrap());
                    assert_eq!(adjustment.coin, expected["coin"].as_str().unwrap());
                    assert_eq!(adjustment.amount_wei, expected["adjustment_wei"].as_i64().unwrap());
                    assert_eq!(deltas.len(), usize::try_from(expected["count"].as_u64().unwrap()).unwrap());
                    records += deltas.len(); checked += 1;
                    println!("{height} {dex:?}: {} {} adjustment {} wei", adjustment.user, adjustment.coin, adjustment.amount_wei);
                }
            }
        }
        assert_eq!((checked, records), (10, 849_268));
    }

}
