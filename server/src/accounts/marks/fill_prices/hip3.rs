//! Execution marks for isolated HIP-3 fills. BM 1192960000..1192970000: a unique successful setOracle
//! anchors its sole Normal write, and receipt-backed ordinary takers select the preceding/following mark.
//! Fallback writes, multiple writes/actions, triggered orders and unreceipted system effects keep the full range.
//! This refines cash inputs only: the node-mark owner still consumes every written price unchanged.

use rustc_hash::FxHashMap;

use super::{BlockFill, BlockStatuses, Hip3Coin, Hip3PriceUpdate, MarkActions, PricesSeen};
use crate::accounts::marks::{actions::{MarkAction, Rank}, seed::exact_price_units};

struct CoinPrices<'a> {
    opening: Option<(u64, u32)>,
    all: PricesSeen,
    writes: Vec<&'a Hip3PriceUpdate>,
}

#[derive(Clone, Copy)]
enum Placement {
    BeforeActions,
    Action(Rank),
}

/// A receipt identifies a taker's creation, not an old order's later trigger. Both legs use the same taker.
/// Duplicate identities stay unknown, as do `CoreWriter` and vault-close orders without ordinary book receipts.
fn takers<'a>(fills: &'a [BlockFill], actions: &MarkActions, statuses: &BlockStatuses) -> FxHashMap<(&'a str, u64), Option<Placement>> {
    let mut created = FxHashMap::default();
    for action in &actions.actions {
        if let MarkAction::Book(book) = &action.action {
            for oid in &book.created {
                created.entry(*oid).and_modify(|rank| *rank = None).or_insert(Some(action.rank));
            }
        }
    }
    let mut out = FxHashMap::default();
    for fill in fills.iter().filter(|fill| fill.crossed) {
        let placement = if fill.twap {
            Some(Placement::BeforeActions)
        } else if statuses.triggered.contains(&fill.oid) {
            None
        } else {
            created.get(&fill.oid).copied().flatten().map(Placement::Action)
        };
        out.entry((fill.coin.as_ref(), fill.tid)).and_modify(|old| *old = None).or_insert(placement);
    }
    out
}

fn oracle_ranks(actions: &MarkActions) -> FxHashMap<&str, Option<Rank>> {
    let mut out = FxHashMap::default();
    for action in &actions.actions {
        if let MarkAction::Hip3Oracle { dex } = &action.action {
            out.entry(dex.as_str()).and_modify(|old| *old = None).or_insert(Some(action.rank));
        }
    }
    out
}

impl CoinPrices<'_> {
    fn at(&self, rank: Option<Rank>, taker: Option<Placement>, size_decimals: u32, time_ns: Option<u64>) -> Option<(u64, u32)> {
        // Invalid/missing evidence cannot be repaired by selecting one side of the update.
        self.all.value()?;
        let [update] = self.writes.as_slice() else { return None; };
        if !update.normal || Some(update.time_ns) != time_ns { return None; }
        let rank = rank?;
        let before = match taker? {
            Placement::BeforeActions => true,
            Placement::Action(taker) if taker != rank => taker < rank,
            Placement::Action(_) => return None,
        };
        if before { self.opening } else { exact_price_units(&update.px, size_decimals).ok().map(|px| (px, size_decimals)) }
    }
}

pub(super) fn observe(
    prices: &mut [PricesSeen], fills: &[BlockFill], coins: &FxHashMap<String, Hip3Coin>, updates: &[Hip3PriceUpdate],
    actions: Option<&MarkActions>, statuses: &BlockStatuses,
) {
    let mut ranges: FxHashMap<&str, CoinPrices<'_>> = FxHashMap::default();
    for fill in fills {
        if let Some(coin) = coins.get(fill.coin.as_ref()) {
            ranges.entry(&fill.coin).or_insert_with(|| {
                let opening = coin.mark.map(|mark| (mark.px, coin.size_decimals));
                let mut all = PricesSeen::default();
                all.observe(opening);
                CoinPrices { opening, all, writes: Vec::new() }
            });
        }
    }
    for update in updates.iter().filter(|update| !update.oracle) {
        if let Some(range) = ranges.get_mut(update.coin.as_str()) {
            let decimals = coins[&update.coin].size_decimals;
            range.all.observe(exact_price_units(&update.px, decimals).ok().map(|px| (px, decimals)));
            range.writes.push(update);
        }
    }
    let ranks = actions.map(oracle_ranks).unwrap_or_default();
    // No receipt scan/allocation on quiet blocks or blocks with only unplaced fallback writes.
    let takers = actions.filter(|_| !ranks.is_empty()).map(|a| takers(fills, a, statuses)).unwrap_or_default();
    for (fill, seen) in fills.iter().zip(prices) {
        let Some(range) = ranges.get(fill.coin.as_ref()) else { continue; };
        let rank = fill.coin.split_once(':').and_then(|(dex, _)| ranks.get(dex)).copied().flatten();
        let taker = takers.get(&(fill.coin.as_ref(), fill.tid)).copied().flatten();
        match range.at(rank, taker, coins[fill.coin.as_ref()].size_decimals, actions.map(|a| a.time_ns)) {
            Some(px) => seen.observe(Some(px)),
            None => seen.clone_from(&range.all),
        }
    }
}

/// Real wire fills with projected action ranks, used by the cash regression as well as the mark-bound checks.
#[cfg(test)]
pub(crate) fn golden_prices(case: &serde_json::Value, event: &serde_json::Value) -> super::super::FillMarkPrices {
    use crate::{accounts::marks::{actions::{BookAnchors, RankedAction}, seed::StoredMark}, types::node_data::NodeDataFill};
    let coin = case["coin"].as_str().unwrap();
    let decimals = u32::try_from(case["size_decimals"].as_u64().unwrap()).unwrap();
    let height = event["height"].as_u64().unwrap();
    let px = exact_price_units(event["before_mark"].as_str().unwrap(), decimals).unwrap();
    let coins = FxHashMap::from_iter([(coin.into(), Hip3Coin { oracle: None, size_decimals: decimals, mark: Some(StoredMark { px, time_ns: 0 }) })]);
    let fills: Vec<_> = event["legs"].as_array().unwrap().iter().map(|leg| BlockFill::of(&serde_json::from_value::<NodeDataFill>(leg.clone()).unwrap())).collect();
    let updates: Vec<_> = event["updates"].as_array().unwrap().iter().map(|u| Hip3PriceUpdate {
        normal: u[1] == "Normal", oracle: false, coin: coin.into(), px: u[0].as_str().unwrap().into(), time_ns: 1,
    }).collect();
    let mut actions = MarkActions { height, time_ns: 1, actions: Vec::new() };
    if let Some(ranks) = event["rank"].as_array() {
        for rank in ranks[0].as_array().unwrap() {
            actions.actions.push(RankedAction { rank: serde_json::from_value(rank.clone()).unwrap(), user: None,
                action: MarkAction::Hip3Oracle { dex: coin.split_once(':').unwrap().0.into() } });
        }
        let taker = fills.iter().find(|f| f.crossed).unwrap();
        for rank in ranks[1].as_array().unwrap() {
            actions.actions.push(RankedAction { rank: serde_json::from_value(rank.clone()).unwrap(), user: None,
                action: MarkAction::Book(BookAnchors { created: vec![taker.oid], ..BookAnchors::default() }) });
        }
    }
    let mut builder = super::super::FillPriceBuilder::new(&fills);
    builder.hip3(&fills, &coins, &updates, Some(&actions), &BlockStatuses::default());
    let out = builder.finish(height, &fills);
    for fill in fills {
        let observed = out.get(height, &fill.coin, fill.tid).unwrap();
        if let Some(chosen) = event["chosen_mark"].as_str() {
            let expected = exact_price_units(chosen, decimals).unwrap();
            assert_eq!((observed.min_px_raw, observed.max_px_raw), (expected, expected), "{coin} {height}");
        }
    }
    out
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::marks::{actions::{BookAnchors, RankedAction}, fill_prices::{FillMarkRange, FillPriceBuilder}, seed::StoredMark};

    fn book(rank: Rank, oid: u64) -> RankedAction {
        RankedAction { rank, user: None, action: MarkAction::Book(BookAnchors { created: vec![oid], ..BookAnchors::default() }) }
    }
    fn oracle(rank: Rank, dex: &str) -> RankedAction {
        RankedAction { rank, user: None, action: MarkAction::Hip3Oracle { dex: dex.into() } }
    }
    fn fill(oid: u64, tid: u64, crossed: bool, twap: bool) -> BlockFill {
        BlockFill { coin: "xyz:X".into(), px: "1".into(), sz: "1".into(), oid, tid, crossed, twap }
    }
    fn update(normal: bool, price: &str) -> Hip3PriceUpdate {
        Hip3PriceUpdate { normal, oracle: false, coin: "xyz:X".into(), px: price.into(), time_ns: 1 }
    }
    fn ranges(fills: &[BlockFill], actions: &[RankedAction], updates: &[Hip3PriceUpdate], triggered: &[u64]) -> Vec<FillMarkRange> {
        let coins = FxHashMap::from_iter([("xyz:X".into(), Hip3Coin { oracle: None, size_decimals: 6, mark: Some(StoredMark { px: 100, time_ns: 0 }) })]);
        let actions = MarkActions { height: 1, time_ns: 1, actions: actions.to_vec() };
        let mut statuses = BlockStatuses::default();
        statuses.triggered.extend(triggered);
        let mut builder = FillPriceBuilder::new(fills);
        builder.hip3(fills, &coins, updates, Some(&actions), &statuses);
        let out = builder.finish(1, fills);
        fills.iter().map(|f| out.get(1, &f.coin, f.tid).unwrap()).collect()
    }
    fn range(low: u64, high: u64) -> FillMarkRange { FillMarkRange { min_px_raw: low, max_px_raw: high, size_decimals: 6 } }

    #[test]
    fn unique_normal_write_places_ordinary_and_twap_fills_on_both_legs() {
        let actions = [book((0, 3, 1), 10), oracle((0, 3, 2), "xyz"), book((1, 1, 0), 20)];
        let fills = [fill(99, 1, false, false), fill(10, 1, true, false), fill(20, 2, true, false), fill(98, 2, false, false), fill(30, 3, true, true)];
        assert_eq!(ranges(&fills, &actions, &[update(true, "110")], &[]),
            [range(100, 100), range(100, 100), range(110, 110), range(110, 110), range(100, 100)]);
    }

    #[test]
    fn ambiguous_identity_trigger_fallback_and_multiple_writes_keep_bounds() {
        let fills = [fill(10, 1, true, false)];
        let ordinary = book((0, 3, 1), 10);
        let price = oracle((0, 3, 2), "xyz");
        for (actions, updates, triggered) in [
            (vec![ordinary.clone(), price.clone()], vec![update(false, "110")], vec![]),
            (vec![ordinary.clone(), price.clone()], vec![update(true, "110"), update(true, "100")], vec![]),
            (vec![ordinary.clone(), price.clone(), oracle((1, 3, 0), "xyz")], vec![update(true, "110")], vec![]),
            (vec![ordinary.clone(), price.clone()], vec![update(true, "110")], vec![10]),
            (vec![ordinary.clone(), ordinary.clone(), price.clone()], vec![update(true, "110")], vec![]),
            (vec![ordinary.clone(), oracle((0, 3, 2), "other")], vec![update(true, "110")], vec![]),
            (vec![price], vec![update(true, "110")], vec![]),
            (vec![ordinary], vec![update(true, "110")], vec![]),
        ] {
            assert_eq!(ranges(&fills, &actions, &updates, &triggered), [range(100, 110)]);
        }
        let actions = [book((0, 3, 1), 10), oracle((0, 3, 2), "xyz")];
        assert_eq!(ranges(&[fill(99, 1, false, false)], &actions, &[update(true, "110")], &[]), [range(100, 110)], "maker alone proves no execution rank");
        assert_eq!(ranges(&[fill(10, 1, true, false), fill(10, 1, true, false)], &actions, &[update(true, "110")], &[]), [range(100, 110); 2]);
    }
}
