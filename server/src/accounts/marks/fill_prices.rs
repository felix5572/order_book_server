//! Execution-price evidence for cash replay. A range contains every mark that can apply to a fill, never just the
//! block's final price. Native ranges follow the existing action/fill cuts. HIP-3 uses receipt-backed taker ordering
//! around a uniquely paired Normal setOracle, including recorded fixed/trailing-market thresholds reached by that write; otherwise it retains every written mark. Ambiguity belongs to the consumer: if both
//! bounds produce the same integer margin transfer, that transfer is exact even when the mark itself is not pinned.

#[cfg(test)]
pub(crate) use hip3::{golden_prices, trigger_golden_prices, interleaved_golden_prices};
mod hip3;
mod liquidation;

use std::sync::Arc;

use rustc_hash::FxHashMap;

use super::{BlockInputs, Hip3PriceUpdate, actions::MarkActions, execution::{BlockFill, BlockStatuses}, native::NativePrices, seed::Hip3Coin};

/// Inclusive price bounds, in 10^-(6-size_decimals) units, from the same block as the fill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FillMarkRange {
    pub(crate) min_px_raw: u64,
    pub(crate) max_px_raw: u64,
    pub(crate) size_decimals: u32,
}

impl FillMarkRange {
    fn include(self, other: Self) -> Option<Self> {
        (self.size_decimals == other.size_decimals).then_some(Self {
            min_px_raw: self.min_px_raw.min(other.min_px_raw),
            max_px_raw: self.max_px_raw.max(other.max_px_raw),
            size_decimals: self.size_decimals,
        })
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct FillMarkPrices {
    height: u64,
    collateral: Arc<FxHashMap<String, FillMarkRange>>,
    // Both legs must see the union of their possible prices; missing evidence for either leg poisons the trade.
    coins: FxHashMap<Box<str>, FxHashMap<u64, Option<FillMarkRange>>>,
}

impl FillMarkPrices {
    pub(super) fn is_empty(&self) -> bool { self.coins.is_empty() }

    /// Shared block envelope, containing every observed price, including intermediate round trips.
    pub(crate) fn collateral(&self, height: u64) -> Option<Arc<FxHashMap<String, FillMarkRange>>> {
        (height == self.height && !self.collateral.is_empty()).then(|| Arc::clone(&self.collateral))
    }

    pub(crate) fn get(&self, height: u64, coin: &str, tid: u64) -> Option<FillMarkRange> {
        if height != self.height { return None; }
        self.coins.get(coin)?.get(&tid).copied().flatten()
    }
}

#[derive(Debug, Default, Clone)]
struct PricesSeen {
    range: Option<FillMarkRange>,
    unavailable: bool,
}

impl PricesSeen {
    fn observe(&mut self, price: Option<(u64, u32)>) {
        let Some((px, size_decimals)) = price.filter(|(px, _)| *px > 0) else {
            self.unavailable = true;
            return;
        };
        let next = FillMarkRange { min_px_raw: px, max_px_raw: px, size_decimals };
        self.range = self.range.map_or(Some(next), |old| old.include(next));
        self.unavailable |= self.range.is_none();
    }

    fn value(&self) -> Option<FillMarkRange> {
        self.range.filter(|_| !self.unavailable)
    }
}

pub(super) struct FillPriceBuilder {
    prices: Vec<PricesSeen>,
    collateral: FxHashMap<String, PricesSeen>,
}

impl FillPriceBuilder {
    pub(super) fn new(fills: &[BlockFill]) -> Self {
        Self { prices: vec![PricesSeen::default(); fills.len()], collateral: FxHashMap::default() }
    }

    pub(super) fn collateral(&mut self, hip3: &FxHashMap<String, Hip3Coin>, native: &NativePrices, inputs: &BlockInputs) {
        for name in &inputs.collateral_coins {
            let seen = self.collateral.entry(name.clone()).or_default();
            if let Some(coin) = hip3.get(name) {
                seen.observe(coin.mark.map(|mark| (mark.px, coin.size_decimals)));
            } else {
                seen.observe(native.asset(name).and_then(|asset| native.mark(asset).map(|px| (px, native.size_decimals(asset)))));
            }
        }
        for update in inputs.hip3.iter().filter(|update| !update.oracle) {
            if let Some(seen) = self.collateral.get_mut(&update.coin) {
                let price = hip3.get(&update.coin).and_then(|coin|
                    super::seed::exact_price_units(&update.px, coin.size_decimals).ok().map(|px| (px, coin.size_decimals)));
                seen.observe(price);
            }
        }
    }

    /// A segment between two price actions can contain fills from the preceding cut's lower bound to the following
    /// cut's upper bound. Overlapping segments deliberately retain both candidates for unplaced fills.
    pub(super) fn native_segment(&mut self, fills: &[BlockFill], prices: &NativePrices, from: usize, to: usize) {
        for (coin, seen) in &mut self.collateral {
            if let Some(asset) = prices.asset(coin) {
                seen.observe(prices.mark(asset).map(|px| (px, prices.size_decimals(asset))));
            }
        }
        for (fill, seen) in fills[from..to].iter().zip(&mut self.prices[from..to]) {
            if let Some(asset) = prices.asset(&fill.coin) {
                seen.observe(prices.mark(asset).map(|px| (px, prices.size_decimals(asset))));
            }
        }
    }

    pub(super) fn hip3(&mut self, coins: &FxHashMap<String, Hip3Coin>, inputs: &BlockInputs) {
        hip3::observe(&mut self.prices, coins, inputs);
    }

    pub(super) fn finish(self, height: u64, fills: &[BlockFill]) -> FillMarkPrices {
        if fills.is_empty() { return FillMarkPrices::default(); }
        let collateral = self.collateral.into_iter().filter_map(|(coin, seen)| seen.value().map(|price| (coin, price))).collect();
        let mut out = FillMarkPrices { height, collateral: Arc::new(collateral), ..FillMarkPrices::default() };
        for (fill, seen) in fills.iter().zip(self.prices) {
            out.coins.entry(fill.coin.clone()).or_default().entry(fill.tid)
                .and_modify(|old| *old = old.and_then(|old| seen.value().and_then(|next| old.include(next))))
                .or_insert_with(|| seen.value());
        }
        liquidation::refine(&mut out, fills);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_candidate_cannot_be_repaired_by_a_later_known_price() {
        let mut seen = PricesSeen::default();
        seen.observe(None);
        seen.observe(Some((100, 2)));
        assert_eq!(seen.value(), None);
        let mut seen = PricesSeen::default();
        seen.observe(Some((100, 2)));
        seen.observe(Some((100, 3)));
        assert_eq!(seen.value(), None, "a units mismatch is not a price range");
    }

    #[test]
    fn paired_legs_share_bounds_and_evidence_cannot_cross_blocks() {
        let fill = |crossed| BlockFill { coin: "X".into(), px: "1".into(), sz: "1".into(), crossed, oid: 1, tid: 7, twap: false, liquidation_mark_px: None };
        let fills = [fill(false), fill(true)];
        let mut builder = FillPriceBuilder::new(&fills);
        builder.prices[0].observe(Some((99, 0)));
        builder.prices[1].observe(Some((101, 0)));
        let prices = builder.finish(4, &fills);
        assert_eq!(prices.get(4, "X", 7), Some(FillMarkRange { min_px_raw: 99, max_px_raw: 101, size_decimals: 0 }));
        assert_eq!(prices.get(5, "X", 7), None);
        let mut builder = FillPriceBuilder::new(&fills);
        builder.prices[0].observe(Some((99, 0)));
        assert_eq!(builder.finish(4, &fills).get(4, "X", 7), None, "the unseen leg remains unknown");
    }
}

#[cfg(test)]
mod execution_tests {
    use super::*;
    use crate::accounts::{marks::{MarkInputs, actions::{BookAnchors, MarkAction, MarkActions, RankedAction}, tests::trusted_marks},
        test_support::hip3_line};

    fn fill(coin: &str, oid: u64, twap: bool) -> BlockFill {
        BlockFill { coin: coin.into(), px: "82850.0".into(), sz: "0.001".into(), crossed: true, oid, tid: oid, twap, liquidation_mark_px: None }
    }

    #[test]
    fn native_marks_follow_action_order_and_keep_unplaced_fills_ambiguous() {
        let mut marks = trusted_marks();
        let old = marks.at().unwrap().mark("BTC").unwrap();
        let book = |position, oid| RankedAction { rank: (0, 3, position), user: None,
            action: MarkAction::Book(BookAnchors { created: vec![oid], ..BookAnchors::default() }) };
        let actions = vec![book(0, 12), RankedAction { rank: (0, 3, 1), user: None,
            action: MarkAction::SetGlobal(vec![Some(("82870.0".into(), "82870.0".into()))]) }, book(2, 14)];
        let report = marks.apply_block(1_002, MarkInputs {
            mark_actions: Ok(MarkActions { height: 1_002, time_ns: 1_792_391_844_000_000_000, actions }),
            fills: vec![fill("BTC", 11, true), fill("BTC", 12, false), fill("BTC", 13, false), fill("BTC", 14, false)],
            ..MarkInputs::default()
        });
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let new = marks.at().unwrap().mark("BTC").unwrap();
        assert_ne!(old, new, "fixture must discriminate the two execution prices");
        let expected = |low, high| Some(FillMarkRange { min_px_raw: low, max_px_raw: high, size_decimals: 5 });
        assert_eq!(report.fill_prices.get(1_002, "BTC", 11), expected(old, old), "TWAP at block start");
        assert_eq!(report.fill_prices.get(1_002, "BTC", 12), expected(old, old), "ordinary taker before SGA");
        assert_eq!(report.fill_prices.get(1_002, "BTC", 13), expected(old.min(new), old.max(new)), "unplaced taker could be on either side");
        assert_eq!(report.fill_prices.get(1_002, "BTC", 14), expected(new, new), "ordinary taker after SGA");
        let broken = marks.apply_block(1_003, MarkInputs {
            mark_actions: Err("unread price action".into()), fills: vec![fill("BTC", 15, false)], ..MarkInputs::default()
        });
        assert_eq!(broken.fill_prices.get(1_003, "BTC", 15), None);
        let stale = marks.apply_block(1_004, MarkInputs { fills: vec![fill("BTC", 16, false)], ..MarkInputs::default() });
        assert_eq!(stale.fill_prices.get(1_004, "BTC", 16), None, "a quiet block cannot repair lost price state");
    }

    #[test]
    fn collateral_envelopes_preserve_intermediate_prices_and_withdraw_with_mark_state() {
        let mut marks = trusted_marks();
        let requested = vec!["BTC".into(), "xyz:TSLA".into(), "missing".into()];
        let report = marks.apply_block(1_002, MarkInputs { collateral_coins: requested.clone(),
            fills: vec![fill("xyz:TSLA", 11, false)], ..MarkInputs::default() });
        let collateral = report.fill_prices.collateral(1_002).unwrap();
        assert_eq!(collateral["xyz:TSLA"].min_px_raw, collateral["xyz:TSLA"].max_px_raw);
        assert!(!collateral.contains_key("missing"));
        assert!(report.fill_prices.collateral(1_003).is_none());
        let report = marks.apply_block(1_003, MarkInputs { collateral_coins: requested.clone(),
            hip3_oracle_updates: vec![hip3_line(1_003, &[("xyz:TSLA", "361.0")]), hip3_line(1_003, &[("xyz:TSLA", "360.0")])],
            fills: vec![fill("xyz:TSLA", 12, false)], ..MarkInputs::default() });
        let collateral = report.fill_prices.collateral(1_003).unwrap();
        assert_eq!((collateral["xyz:TSLA"].min_px_raw, collateral["xyz:TSLA"].max_px_raw), (360_000, 361_000));
        let broken = marks.apply_block(1_004, MarkInputs { collateral_coins: requested,
            mark_actions: Err("unread input".into()), fills: vec![fill("xyz:TSLA", 13, false)], ..MarkInputs::default() });
        assert!(broken.fill_prices.collateral(1_004).is_none());
    }

    #[test]
    fn hip3_keeps_every_within_block_price_including_a_round_trip() {
        let mut marks = trusted_marks();
        let stable = marks.apply_block(1_002, MarkInputs { fills: vec![fill("xyz:TSLA", 10, false)], ..MarkInputs::default() });
        assert_eq!(stable.fill_prices.get(1_002, "xyz:TSLA", 10), Some(FillMarkRange { min_px_raw: 360_000, max_px_raw: 360_000, size_decimals: 3 }));
        let changed = marks.apply_block(1_003, MarkInputs {
            hip3_oracle_updates: vec![hip3_line(1_003, &[("xyz:TSLA", "361.0")]), hip3_line(1_003, &[("xyz:TSLA", "360.0")])],
            fills: vec![fill("xyz:TSLA", 11, false)], ..MarkInputs::default()
        });
        assert_eq!(changed.fill_prices.get(1_003, "xyz:TSLA", 11), Some(FillMarkRange { min_px_raw: 360_000, max_px_raw: 361_000, size_decimals: 3 }),
            "equal opening/closing marks do not erase the intermediate price");
    }
}
