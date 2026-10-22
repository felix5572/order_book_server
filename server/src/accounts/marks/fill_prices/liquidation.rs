//! Market-liquidation fills record the execution mark on both legs. BM para:LRCX 1193180450:
//! using 232.7792 resolves five isolated maker balances exactly; the block also contained 233.52.
//! Refine only trusted HIP-3 ranges containing that price. This neither updates the mark state nor
//! supplies missing price history. Backstop/ADL and native liquidation timing remain outside this proof.

use rustc_hash::FxHashMap;

use super::{BlockFill, FillMarkPrices, FillMarkRange};
use crate::accounts::marks::seed::exact_price_units;

#[derive(Default)]
struct LiquidationLegs {
    count: u8,
    sides: u8,
    price: Option<u64>,
    invalid: bool,
}

impl LiquidationLegs {
    fn observe(&mut self, fill: &BlockFill, range: Option<FillMarkRange>) {
        self.count = self.count.saturating_add(1);
        self.sides |= if fill.crossed { 2 } else { 1 };
        let price = range.and_then(|range| {
            let px = exact_price_units(fill.liquidation_mark_px.as_deref()?, range.size_decimals).ok()?;
            (px >= range.min_px_raw && px <= range.max_px_raw).then_some(px)
        });
        self.invalid |= price.is_none() || self.price.is_some_and(|old| Some(old) != price);
        self.price = price;
    }

    fn range(&self, original: Option<FillMarkRange>) -> Option<FillMarkRange> {
        if self.invalid || self.count != 2 || self.sides != 3 { return None; }
        let px = self.price?;
        Some(FillMarkRange { min_px_raw: px, max_px_raw: px, ..original? })
    }
}

pub(super) fn refine(prices: &mut FillMarkPrices, fills: &[BlockFill]) {
    // Most blocks have no HIP-3 liquidation: no map allocation for ordinary fills.
    let mut trades: FxHashMap<(&str, u64), LiquidationLegs> = FxHashMap::default();
    for fill in fills.iter().filter(|fill| fill.coin.contains(':') && fill.liquidation_mark_px.is_some()) {
        trades.entry((&fill.coin, fill.tid)).or_default();
    }
    if trades.is_empty() { return; }
    for fill in fills {
        if let Some(legs) = trades.get_mut(&(fill.coin.as_ref(), fill.tid)) {
            legs.observe(fill, prices.get(prices.height, &fill.coin, fill.tid));
        }
    }
    for (coin, by_tid) in &mut prices.coins {
        for (tid, range) in by_tid {
            if let Some(legs) = trades.get(&(coin.as_ref(), *tid)) {
                // Contradictory or incomplete evidence withdraws only this trade's cash-price
                // projection; cash replay retains its estimated status.
                *range = legs.range(*range);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::FillPriceBuilder;
    use crate::types::node_data::NodeDataFill;

    fn fills() -> Vec<BlockFill> {
        [false, true].into_iter().map(|crossed| BlockFill { coin: "xyz:X".into(), px: "2.34".into(),
            sz: "1".into(), crossed, oid: u64::from(crossed), tid: 7, twap: false,
            liquidation_mark_px: Some("2.32".into()) }).collect()
    }

    fn prices(fills: &[BlockFill], known: bool) -> FillMarkPrices {
        let mut builder = FillPriceBuilder::new(fills);
        if known {
            for seen in &mut builder.prices { seen.observe(Some((232, 4))); seen.observe(Some((234, 4))); }
        }
        builder.finish(100, fills)
    }

    #[test]
    fn both_legs_pin_the_price_without_changing_other_trades_or_heights() {
        let mut fills = fills();
        fills[0].liquidation_mark_px = Some("2.3200".into());
        let mut ordinary = fills[0].clone(); ordinary.tid = 8; ordinary.liquidation_mark_px = None;
        fills.push(ordinary);
        let prices = prices(&fills, true);
        assert_eq!(prices.get(100, "xyz:X", 7), Some(FillMarkRange { min_px_raw: 232, max_px_raw: 232, size_decimals: 4 }));
        assert_eq!(prices.get(100, "xyz:X", 8).unwrap().max_px_raw, 234);
        assert_eq!(prices.get(101, "xyz:X", 7), None);
    }

    #[test]
    fn unsupported_or_untrusted_paths_do_not_gain_precision() {
        let mut fs = fills();
        assert_eq!(prices(&fs, false).get(100, "xyz:X", 7), None, "recorded mark cannot repair missing history");
        for f in &mut fs { f.coin = "BTC".into(); }
        assert_eq!(prices(&fs, true).get(100, "BTC", 7).unwrap().max_px_raw, 234, "native timing is unchanged");
        for f in &mut fs { f.coin = "xyz:X".into(); f.liquidation_mark_px = None; }
        assert_eq!(prices(&fs, true).get(100, "xyz:X", 7).unwrap().max_px_raw, 234);
    }

    #[test]
    fn bad_or_disagreeing_prices_withdraw_this_trade_projection() {
        for px in ["NaN", "-2.32", "0", "2.321", "2.31", "2.35", "2.33"] {
            let mut fs = fills(); fs[1].liquidation_mark_px = Some(px.into());
            assert_eq!(prices(&fs, true).get(100, "xyz:X", 7), None, "{px}");
        }
        let mut fs = fills(); fs[1].liquidation_mark_px = None;
        assert_eq!(prices(&fs, true).get(100, "xyz:X", 7), None);
    }

    #[test]
    fn a_unique_maker_and_taker_are_required() {
        let mut fs = fills();
        assert_eq!(prices(&fs[..1], true).get(100, "xyz:X", 7), None);
        fs[1].crossed = false;
        assert_eq!(prices(&fs, true).get(100, "xyz:X", 7), None);
        fs = fills(); fs.push(fs[0].clone());
        assert_eq!(prices(&fs, true).get(100, "xyz:X", 7), None);
    }

    #[test]
    fn only_market_liquidations_are_read_from_the_wire() {
        let golden: serde_json::Value = serde_json::from_str(include_str!("liquidation_cash_goldens.json")).unwrap();
        let mut leg = golden["event"]["legs"][0].clone();
        for method in ["market", "backstop", "unknown"] {
            leg[1]["liquidation"]["method"] = method.into();
            let wire: NodeDataFill = serde_json::from_value(leg.clone()).unwrap();
            assert_eq!(BlockFill::of(&wire).liquidation_mark_px.is_some(), method == "market");
        }
    }
}
