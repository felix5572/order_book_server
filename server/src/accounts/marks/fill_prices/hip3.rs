//! Execution marks for isolated HIP-3 fills. BM 1192960000..1192970000: a unique successful setOracle
//! anchors its sole Normal write. Ordinary receipts or uniquely matched default-modify status pairs select
//! the preceding/following mark (ORCL 1193548256); possible `CoreWriter` ownership prevents that attribution.
//! A recorded trigger threshold crossed by the sole Normal write consumes that new mark (BM NET1193124226, SPCX1193126889).
//! Trailing Stop Market uses its realized triggered-status threshold (MRNA1193502103), not a future trailing estimate.
//! A complete triggered-to-filled run bracketed by receipt-backed opens of one ordinary action proves that action
//! rank (DRAM1193759676), including already-eligible triggers executed in a later bundle. Other already-satisfied
//! triggers can use the node L1 hash locator of the successful oracle action (DRAM1193762464,
//! SNDK1193771663). Fallback/multiple writes and unreceipted effects keep the full range.
//! This refines cash inputs only: the node-mark owner still consumes every written price unchanged.

use rustc_hash::FxHashMap;

#[cfg(test)]
use super::BlockFill;

use super::{BlockInputs, BlockStatuses, Hip3Coin, Hip3PriceUpdate, MarkActions, PricesSeen};
use crate::accounts::marks::{actions::{MarkAction, Rank}, seed::exact_price_units, execution::fill_order::{Placement, takers}};

struct CoinPrices<'a> {
    opening: Option<(u64, u32)>,
    all: PricesSeen,
    writes: Vec<&'a Hip3PriceUpdate>,
}

fn oracle_ranks(actions: &MarkActions) -> FxHashMap<&str, Option<(Rank, Option<usize>)>> {
    let mut out = FxHashMap::default();
    for action in &actions.actions {
        if let MarkAction::Hip3Oracle { dex, execution_index } = &action.action {
            out.entry(dex.as_str()).and_modify(|old| *old = None).or_insert(Some((action.rank, *execution_index)));
        }
    }
    out
}

enum FillTiming {
    Placed(Placement),
    AtOrAfterOracle,
}

impl CoinPrices<'_> {
    fn at(&self, rank: Option<Rank>, taker: Option<FillTiming>, size_decimals: u32, time_ns: Option<u64>, coin: &str, statuses: &BlockStatuses) -> Option<(u64, u32)> {
        let update = self.unique_write(time_ns)?;
        let rank = rank?;
        let before = match taker? {
            FillTiming::Placed(Placement::BeforeActions) => true,
            FillTiming::Placed(Placement::Action(taker)) if taker != rank => taker < rank,
            FillTiming::Placed(Placement::Action(_)) => return None,
            FillTiming::AtOrAfterOracle => false,
            FillTiming::Placed(Placement::Trigger(oid)) => {
                let (opening, decimals) = self.opening?;
                let closing = exact_price_units(&update.px, size_decimals).ok()?;
                let trigger = statuses.trigger_prices.get(&oid)?.as_ref()?;
                if decimals != size_decimals || !trigger.crossed_by(coin, opening, closing, decimals) { return None; }
                false
            }
        };
        self.on_side(before, size_decimals, time_ns)
    }

    fn unique_write(&self, time_ns: Option<u64>) -> Option<&Hip3PriceUpdate> {
        self.all.value()?;
        let [update] = self.writes.as_slice() else { return None; };
        (update.normal && Some(update.time_ns) == time_ns).then_some(*update)
    }

    fn on_side(&self, before: bool, size_decimals: u32, time_ns: Option<u64>) -> Option<(u64, u32)> {
        let update = self.unique_write(time_ns)?;
        if before { self.opening } else { exact_price_units(&update.px, size_decimals).ok().map(|px| (px, size_decimals)) }
    }
}

/// A phase shares one immutable collateral map across all eligible trades and both legs.
/// Only already-requested same-dex collateral prices are refined; triggers and multiple/fallback writes retain bounds.
#[derive(Default)]
pub(super) struct CollateralRefinement {
    pub(super) prices: FxHashMap<String, super::FillMarkRange>,
    pub(super) trades: Vec<(Box<str>, u64)>,
}

pub(super) fn observe(prices: &mut [PricesSeen], coins: &FxHashMap<String, Hip3Coin>, inputs: &BlockInputs) -> Vec<CollateralRefinement> {
    let BlockInputs { fills, hip3: updates, actions, statuses, system, .. } = inputs;
    let actions = actions.as_ref();
    let mut ranges: FxHashMap<&str, CoinPrices<'_>> = FxHashMap::default();
    for name in fills.iter().map(|fill| fill.coin.as_ref()).chain(inputs.collateral_coins.iter().map(String::as_str)) {
        if let Some(coin) = coins.get(name) {
            ranges.entry(name).or_insert_with(|| {
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
    let trigger_ranks = actions.filter(|_| !ranks.is_empty())
        .map(|a| statuses.interleaved_trigger_ranks(a)).unwrap_or_default();
    // No receipt scan/allocation on quiet blocks or blocks with only unplaced fallback writes.
    let takers = actions.filter(|_| !ranks.is_empty()).map(|a| {
        let asset = |coin: &str| coins.get(coin).map(|c| c.order_asset);
        let competing = system.competing_orders(a.time_ns, &asset);
        let replacements = statuses.replacement_orders(a, &asset, &|oid| competing.contains(&oid));
        takers(fills, a, &statuses.triggered, &replacements)
    }).unwrap_or_default();
    for (fill, seen) in fills.iter().zip(prices) {
        let Some(range) = ranges.get(fill.coin.as_ref()) else { continue; };
        let oracle = fill.coin.split_once(':').and_then(|(dex, _)| ranks.get(dex)).copied().flatten();
        let rank = oracle.map(|(rank, _)| rank);
        let original = takers.get(&(fill.coin.as_ref(), fill.tid)).copied().flatten();
        let taker = original.and_then(|placement| {
            let Placement::Trigger(oid) = placement else { return Some(FillTiming::Placed(placement)); };
            let bracket = trigger_ranks.get(&oid).copied();
            // Each leg independently carries provenance. finish() unions their ranges; a missing
            // locator on either leg keeps the uncertainty rather than borrowing the other leg's hash.
            let matched_oracle = fill.action_position.zip(oracle).and_then(|(position, (rank, index))|
                (Some(position.height) == actions.map(|a| a.height)
                    && index.and_then(|i| u64::try_from(i).ok()) == Some(position.index)).then_some(rank));
            trigger_timing(placement, bracket, matched_oracle)
        });
        match range.at(rank, taker, coins[fill.coin.as_ref()].size_decimals, actions.map(|a| a.time_ns), &fill.coin, statuses) {
            Some(px) => seen.observe(Some(px)),
            None => seen.clone_from(&range.all),
        }
    }
    collateral_refinements(&ranges, &ranks, &takers, coins, inputs)
}

fn collateral_refinements(
    ranges: &FxHashMap<&str, CoinPrices<'_>>,
    ranks: &FxHashMap<&str, Option<(Rank, Option<usize>)>>,
    takers: &FxHashMap<(&str, u64), Option<Placement>>,
    coins: &FxHashMap<String, Hip3Coin>,
    inputs: &BlockInputs,
) -> Vec<CollateralRefinement> {
    if inputs.collateral_coins.is_empty() { return Vec::new(); }
    let mut refinements = Vec::new();
    for (dex, oracle) in ranks {
        let Some((rank, _)) = oracle else { continue; };
        for before in [true, false] {
            let trades: Vec<_> = takers.iter().filter_map(|((coin, tid), placement)| {
                if coin.split_once(':').map(|(prefix, _)| prefix) != Some(*dex) { return None; }
                let on_before = match (*placement)? {
                    Placement::BeforeActions => true,
                    Placement::Action(action) if action != *rank => action < *rank,
                    _ => return None,
                };
                (on_before == before).then(|| ((*coin).into(), *tid))
            }).collect();
            if trades.is_empty() { continue; }
            let prices = inputs.collateral_coins.iter().filter_map(|coin| {
                if coin.split_once(':').map(|(prefix, _)| prefix) != Some(*dex) { return None; }
                let range = ranges.get(coin.as_str())?;
                let (px, size_decimals) = range.on_side(before, coins[coin].size_decimals, inputs.actions.as_ref().map(|a| a.time_ns))?;
                let price = super::FillMarkRange { min_px_raw: px, max_px_raw: px, size_decimals };
                (range.all.value() != Some(price)).then(|| (coin.clone(), price))
            }).collect::<FxHashMap<_, _>>();
            if !prices.is_empty() { refinements.push(CollateralRefinement { prices, trades }); }
        }
    }
    refinements
}

/// Reconcile the price implied by each source, not an exact action identity. NBIS 1194139926 carries
/// the oracle's locator while a complete status span is bracketed by a later bundle's ordinary orders.
/// Both imply the sole Normal write's new price. A bracket before that write still conflicts and stays unknown.
/// `CoinPrices::at` separately requires one successful oracle and one same-block Normal write.
fn trigger_timing(placement: Placement, bracket: Option<Rank>, matched_oracle: Option<Rank>) -> Option<FillTiming> {
    if let Some(oracle) = matched_oracle {
        return bracket.is_none_or(|rank| rank >= oracle).then_some(FillTiming::AtOrAfterOracle);
    }
    Some(FillTiming::Placed(bracket.map_or(placement, Placement::Action)))
}

/// Real wire fills with projected action ranks, used by the cash regression as well as the mark-bound checks.
#[cfg(test)]
pub(crate) fn golden_prices(case: &serde_json::Value, event: &serde_json::Value) -> super::super::FillMarkPrices {
    use crate::{accounts::marks::{actions::{BookAnchors, RankedAction}, seed::StoredMark}, types::node_data::NodeDataFill};
    let coin = case["coin"].as_str().unwrap();
    let decimals = u32::try_from(case["size_decimals"].as_u64().unwrap()).unwrap();
    let height = event["height"].as_u64().unwrap();
    let px = exact_price_units(event["before_mark"].as_str().unwrap(), decimals).unwrap();
    let coins = FxHashMap::from_iter([(coin.into(), Hip3Coin { order_asset: 110_000, oracle: None, size_decimals: decimals, mark: Some(StoredMark { px, time_ns: 0 }) })]);
    let fills: Vec<_> = event["legs"].as_array().unwrap().iter().map(|leg| BlockFill::of(&serde_json::from_value::<NodeDataFill>(leg.clone()).unwrap())).collect();
    let updates: Vec<_> = event["updates"].as_array().unwrap().iter().map(|u| Hip3PriceUpdate {
        normal: u[1] == "Normal", oracle: false, coin: coin.into(), px: u[0].as_str().unwrap().into(), time_ns: 1,
    }).collect();
    let mut actions = MarkActions { height, time_ns: 1, actions: Vec::new() };
    if let Some(ranks) = event["rank"].as_array() {
        for rank in ranks[0].as_array().unwrap() {
            actions.actions.push(RankedAction { rank: serde_json::from_value(rank.clone()).unwrap(), user: None,
                action: MarkAction::Hip3Oracle { dex: coin.split_once(':').unwrap().0.into(), execution_index: None } });
        }
        let taker = fills.iter().find(|f| f.crossed).unwrap();
        for rank in ranks[1].as_array().unwrap() {
            actions.actions.push(RankedAction { rank: serde_json::from_value(rank.clone()).unwrap(), user: None,
                action: MarkAction::Book(BookAnchors { created: vec![taker.oid], ..BookAnchors::default() }) });
        }
    }
    let mut builder = super::super::FillPriceBuilder::new(&fills);
    builder.hip3(&coins, &BlockInputs { fills: fills.clone(), hip3: updates, actions: Some(actions), ..BlockInputs::default() });
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
struct TriggerFixture {
    height: u64,
    fills: Vec<BlockFill>,
    coins: FxHashMap<String, Hip3Coin>,
    updates: Vec<Hip3PriceUpdate>,
    actions: MarkActions,
    statuses: BlockStatuses,
}

#[cfg(test)]
impl TriggerFixture {
    fn of(case: &serde_json::Value) -> Self {
        use crate::{accounts::marks::{actions::RankedAction, seed::StoredMark}, types::node_data::NodeDataFill};
        let coin = case["coin"].as_str().unwrap();
        let decimals = u32::try_from(case["size_decimals"].as_u64().unwrap()).unwrap();
        let height = case["height"].as_u64().unwrap();
        let time_ns = case["time_ns"].as_u64().unwrap();
        Self {
            height,
            fills: case["legs"].as_array().unwrap().iter().map(|leg| BlockFill::of(&serde_json::from_value::<NodeDataFill>(leg.clone()).unwrap())).collect(),
            coins: FxHashMap::from_iter([(coin.into(), Hip3Coin { order_asset: 110_000, oracle: None, size_decimals: decimals,
                mark: Some(StoredMark { px: exact_price_units(case["before_mark"].as_str().unwrap(), decimals).unwrap(), time_ns: time_ns - 1 }) })]),
            updates: vec![Hip3PriceUpdate { normal: case["normal"].as_bool().unwrap(), oracle: false, coin: coin.into(),
                px: case["after_mark"].as_str().unwrap().into(), time_ns }],
            actions: MarkActions { height, time_ns, actions: vec![RankedAction {
                rank: serde_json::from_value(case["oracle_rank"].clone()).unwrap(), user: None,
                action: MarkAction::Hip3Oracle { dex: coin.split_once(':').unwrap().0.into(), execution_index: None },
            }] },
            statuses: BlockStatuses::of([serde_json::from_value(case["triggered_status"].clone()).unwrap()]).unwrap(),
        }
    }

    fn prices(&self) -> super::super::FillMarkPrices {
        let mut builder = super::super::FillPriceBuilder::new(&self.fills);
        builder.hip3(&self.coins, &BlockInputs { fills: self.fills.clone(), hip3: self.updates.clone(),
            actions: Some(self.actions.clone()), statuses: self.statuses.clone(), ..BlockInputs::default() });
        builder.finish(self.height, &self.fills)
    }
}

#[cfg(test)]
pub(crate) fn trigger_golden_prices(case: &serde_json::Value) -> super::super::FillMarkPrices {
    TriggerFixture::of(case).prices()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::marks::{actions::{BookAnchors, RankedAction}, fill_prices::{FillMarkRange, FillPriceBuilder}, seed::StoredMark};

    fn book(rank: Rank, oid: u64) -> RankedAction {
        RankedAction { rank, user: None, action: MarkAction::Book(BookAnchors { created: vec![oid], ..BookAnchors::default() }) }
    }
    fn oracle(rank: Rank, dex: &str) -> RankedAction {
        RankedAction { rank, user: None, action: MarkAction::Hip3Oracle { dex: dex.into(), execution_index: None } }
    }
    fn fill(oid: u64, tid: u64, crossed: bool, twap: bool) -> BlockFill {
        BlockFill { coin: "xyz:X".into(), px: "1".into(), sz: "1".into(), oid, tid, action_position: None, crossed, twap, liquidation_mark_px: None }
    }
    fn update(normal: bool, price: &str) -> Hip3PriceUpdate {
        Hip3PriceUpdate { normal, oracle: false, coin: "xyz:X".into(), px: price.into(), time_ns: 1 }
    }
    fn ranges(fills: &[BlockFill], actions: &[RankedAction], updates: &[Hip3PriceUpdate], triggered: &[u64]) -> Vec<FillMarkRange> {
        let coins = FxHashMap::from_iter([("xyz:X".into(), Hip3Coin { order_asset: 110_000, oracle: None, size_decimals: 6, mark: Some(StoredMark { px: 100, time_ns: 0 }) })]);
        let actions = MarkActions { height: 1, time_ns: 1, actions: actions.to_vec() };
        let mut statuses = BlockStatuses::default();
        statuses.triggered.extend(triggered);
        let mut builder = FillPriceBuilder::new(fills);
        builder.hip3(&coins, &BlockInputs { fills: fills.to_vec(), hip3: updates.to_vec(), actions: Some(actions), statuses, ..BlockInputs::default() });
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

    #[test]
    fn collateral_uses_the_takers_oracle_phase_and_retains_ambiguous_bounds() {
        let mut coins = FxHashMap::default();
        for (name, px) in [("xyz:X", 100), ("xyz:A", 200)] {
            coins.insert(name.into(), Hip3Coin { order_asset: 110_000, oracle: None, size_decimals: 6,
                mark: Some(StoredMark { px, time_ns: 0 }) });
        }
        let fills = vec![fill(10, 1, true, false), fill(99, 1, false, false),
            fill(20, 2, true, false), fill(30, 3, true, false)];
        for normal in [true, false] {
            let actions = MarkActions { height: 1, time_ns: 1,
                actions: vec![book((0, 3, 1), 10), oracle((0, 3, 2), "xyz"), book((0, 3, 3), 20)] };
            let updates = vec![update(normal, "110"), Hip3PriceUpdate { normal, oracle: false,
                coin: "xyz:A".into(), px: "220".into(), time_ns: 1 }];
            let mut builder = FillPriceBuilder::new(&fills);
            let mut collateral = PricesSeen::default();
            collateral.observe(Some((200, 6))); collateral.observe(Some((220, 6)));
            builder.collateral.insert("xyz:A".into(), collateral);
            builder.hip3(&coins, &BlockInputs { fills: fills.clone(), hip3: updates, actions: Some(actions),
                collateral_coins: vec!["xyz:A".into()], ..BlockInputs::default() });
            let out = builder.finish(1, &fills);
            let before = out.collateral_for_fill(1, "xyz:X", 1).unwrap();
            let after = out.collateral_for_fill(1, "xyz:X", 2).unwrap();
            assert_eq!(before["xyz:A"], if normal { range(200, 200) } else { range(200, 220) });
            assert_eq!(after["xyz:A"], if normal { range(220, 220) } else { range(200, 220) });
            assert_eq!(out.collateral_for_fill(1, "xyz:X", 3).unwrap()["xyz:A"], range(200, 220), "unplaced taker");
            assert!(out.collateral_for_fill(2, "xyz:X", 1).is_none(), "cannot carry prices to another block");
        }
    }

    #[test]
    fn ewy_and_be_recorded_orders_use_pre_write_collateral_in_the_same_block() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!("../../replay/cross_capacity_windows.json")).unwrap();
        for case in fixture["cases"].as_array().unwrap() {
            let golden = &case["price_order_golden"];
            let height = golden["height"].as_u64().unwrap();
            let time_ns = golden["time_ns"].as_u64().unwrap();
            let mut coins = FxHashMap::default();
            let mut updates = Vec::new();
            for (name, before) in golden["before"].as_object().unwrap() {
                let decimals = u32::try_from(case["markets"][name]["size_decimals"].as_u64().unwrap()).unwrap();
                coins.insert(name.clone(), Hip3Coin { order_asset: 110_000, oracle: None, size_decimals: decimals,
                    mark: Some(StoredMark { px: before.as_u64().unwrap(), time_ns: time_ns - 1 }) });
                updates.push(Hip3PriceUpdate { normal: true, oracle: false, coin: name.clone(), time_ns,
                    px: crate::accounts::size::format_size_raw(golden["after"][name].as_i64().unwrap(), 6 - decimals) });
            }
            let owner = case["user"].as_str().unwrap().parse().unwrap();
            let fills: Vec<_> = golden["fills"].as_array().unwrap().iter().map(|fill| {
                let wire = crate::types::node_data::NodeDataFill(owner, serde_json::from_value(fill.clone()).unwrap());
                BlockFill::of(&wire)
            }).collect();
            let oracle_rank: Rank = serde_json::from_value(golden["ranks"]["oracle"][0].clone()).unwrap();
            let mut actions = vec![oracle(oracle_rank, "xyz")];
            for (oid, rank) in golden["ranks"]["orders"].as_object().unwrap() {
                actions.push(book(serde_json::from_value(rank.clone()).unwrap(), oid.parse().unwrap()));
            }
            let mut builder = FillPriceBuilder::new(&fills);
            for name in coins.keys() {
                let mut seen = PricesSeen::default();
                seen.observe(Some((golden["before"][name].as_u64().unwrap(), coins[name].size_decimals)));
                seen.observe(Some((golden["after"][name].as_u64().unwrap(), coins[name].size_decimals)));
                builder.collateral.insert(name.clone(), seen);
            }
            builder.hip3(&coins, &BlockInputs { fills: fills.clone(), hip3: updates,
                actions: Some(MarkActions { height, time_ns, actions }), collateral_coins: coins.keys().cloned().collect(),
                ..BlockInputs::default() });
            let prices = builder.finish(height, &fills);
            for fill in &fills {
                let rank: Rank = serde_json::from_value(golden["ranks"]["orders"][fill.oid.to_string()].clone()).unwrap();
                assert!(rank < oracle_rank, "both raw examples execute before their price update");
                let collateral = prices.collateral_for_fill(height, &fill.coin, fill.tid).unwrap();
                for (coin, before) in golden["before"].as_object().unwrap() {
                    assert_eq!(collateral[coin].min_px_raw, before.as_u64().unwrap(), "{height} {coin}");
                    assert_eq!(collateral[coin].max_px_raw, before.as_u64().unwrap(), "{height} {coin}");
                }
            }
        }
    }

    fn trigger_cases() -> serde_json::Value {
        serde_json::from_str(include_str!("hip3_trigger_cash_goldens.json")).unwrap()
    }

    #[test]
    fn recorded_triggered_takers_give_both_legs_the_new_mark() {
        for case in trigger_cases()["cases"].as_array().unwrap() {
            let input = TriggerFixture::of(case);
            let prices = input.prices();
            let decimals = input.coins[case["coin"].as_str().unwrap()].size_decimals;
            let expected = exact_price_units(case["after_mark"].as_str().unwrap(), decimals).unwrap();
            for fill in &input.fills {
                let price = prices.get(input.height, &fill.coin, fill.tid).unwrap();
                assert_eq!((price.min_px_raw, price.max_px_raw), (expected, expected));
            }
        }
    }

    #[test]
    fn recorded_equality_triggers_select_the_new_mark_on_both_sides() {
        let golden: serde_json::Value = serde_json::from_str(include_str!("hip3_trigger_boundary_goldens.json")).unwrap();
        for case in golden["cases"].as_array().unwrap() {
            for event in case["events"].as_array().unwrap() {
                let input = TriggerFixture::of(event);
                let coin = event["coin"].as_str().unwrap();
                let decimals = input.coins[coin].size_decimals;
                let expected = exact_price_units(event["after_mark"].as_str().unwrap(), decimals).unwrap();
                let prices = input.prices();
                for fill in &input.fills {
                    let range = prices.get(input.height, &fill.coin, fill.tid).unwrap();
                    assert_eq!((range.min_px_raw, range.max_px_raw), (expected, expected), "{coin} {}", input.height);
                }
                let trigger = input.statuses.trigger_prices.values().next().unwrap().as_ref().unwrap();
                let level = exact_price_units(event["triggered_status"]["order"]["triggerPx"].as_str().unwrap(), decimals).unwrap();
                // Equality on the old price is already eligible; it cannot prove this write caused execution.
                assert!(!trigger.crossed_by(coin, level, expected, decimals));
                let opening = input.coins[coin].mark.unwrap().px;
                assert!(!trigger.crossed_by(coin, opening, opening, decimals));
            }
        }
    }

    #[test]
    fn trigger_price_proof_requires_unique_write_status_and_taker() {
        let cases = trigger_cases();
        let case = &cases["cases"][0];
        let changes: [fn(&mut TriggerFixture); 10] = [
            |v| v.updates[0].normal = false,
            |v| v.updates[0].time_ns += 1,
            |v| v.updates.push(v.updates[0].clone()),
            |v| v.actions.actions.push(v.actions.actions[0].clone()),
            |v| v.actions.actions.clear(),
            |v| v.statuses.trigger_prices.clear(),
            |v| { for value in v.statuses.trigger_prices.values_mut() { *value = None; } },
            |v| v.statuses.triggered.clear(),
            |v| v.fills.push(v.fills.iter().find(|f| f.crossed).unwrap().clone()),
            |v| v.fills.retain(|f| !f.crossed),
        ];
        for change in changes {
            let mut input = TriggerFixture::of(case); change(&mut input);
            let prices = input.prices();
            let fill = &input.fills[0];
            let range = prices.get(input.height, &fill.coin, fill.tid).unwrap();
            assert_eq!((range.min_px_raw, range.max_px_raw), (3_153_500, 3_156_700));
        }
    }

    fn trailing_stop_case() -> serde_json::Value {
        let golden: serde_json::Value = serde_json::from_str(include_str!("trailing_stop_cash_goldens.json")).unwrap();
        golden["cases"][0]["events"].as_array().unwrap().iter().find(|e|
            e["triggered_status"]["order"]["orderType"] == "Trailing Stop Market").unwrap().clone()
    }

    #[test]
    fn trailing_stop_recorded_threshold_selects_the_only_crossing_write() {
        let case = trailing_stop_case();
        let input = TriggerFixture::of(&case);
        for fill in &input.fills {
            let range = input.prices().get(input.height, &fill.coin, fill.tid).unwrap();
            assert_eq!((range.min_px_raw, range.max_px_raw), (2_018_800, 2_018_800));
        }
        // Synthetic rising counterpart checks side-to-direction conversion; the recorded case above is a sell.
        let mut rising = case;
        rising["before_mark"] = "201.88".into(); rising["after_mark"] = "202.00".into();
        rising["triggered_status"]["order"]["side"] = "B".into();
        rising["triggered_status"]["order"]["triggerCondition"] = "Activation immediate, retracement 2%, best 198".into();
        rising["triggered_status"]["order"]["triggerPx"] = "201.96".into();
        let input = TriggerFixture::of(&rising); let fill = &input.fills[0];
        let range = input.prices().get(input.height, &fill.coin, fill.tid).unwrap();
        assert_eq!((range.min_px_raw, range.max_px_raw), (2_020_000, 2_020_000));
    }

    #[test]
    fn trailing_stop_missing_or_conflicting_evidence_keeps_the_full_range() {
        let changes: [fn(&mut serde_json::Value); 6] = [
            |v| v["triggered_status"]["order"]["triggerPx"] = "0.0".into(),
            |v| v["triggered_status"]["order"]["triggerPx"] = "bad".into(),
            |v| v["triggered_status"]["order"]["triggerCondition"] = "Triggered".into(),
            |v| v["triggered_status"]["order"]["isTrigger"] = false.into(),
            |v| v["triggered_status"]["order"]["orderType"] = "Trailing Stop Limit".into(),
            |v| v["triggered_status"]["order"]["side"] = "B".into(),
        ];
        for change in changes {
            let mut case = trailing_stop_case(); change(&mut case);
            let input = TriggerFixture::of(&case); let fill = &input.fills[0];
            let range = input.prices().get(input.height, &fill.coin, fill.tid).unwrap();
            assert_eq!((range.min_px_raw, range.max_px_raw), (2_018_800, 2_019_600));
        }
        for opening in ["201.8898", "201.8850"] {
            let mut case = trailing_stop_case(); case["before_mark"] = opening.into();
            let input = TriggerFixture::of(&case); let fill = &input.fills[0];
            let range = input.prices().get(input.height, &fill.coin, fill.tid).unwrap();
            assert_ne!(range.min_px_raw, range.max_px_raw, "already eligible at the opening mark");
        }
        let mut duplicate = TriggerFixture::of(&trailing_stop_case());
        duplicate.updates.push(duplicate.updates[0].clone());
        let fill = &duplicate.fills[0];
        let range = duplicate.prices().get(duplicate.height, &fill.coin, fill.tid).unwrap();
        assert_eq!((range.min_px_raw, range.max_px_raw), (2_018_800, 2_019_600));
    }

    #[test]
    fn trigger_thresholds_do_not_promote_unsupported_or_already_satisfied_conditions() {
        let cases = trigger_cases();
        let changes: [fn(&mut serde_json::Value); 6] = [
            |v| v["triggered_status"]["order"]["isTrigger"] = false.into(),
            |v| v["triggered_status"]["order"]["triggerCondition"] = "Activation below 315.61, retracement 1%".into(),
            |v| v["triggered_status"]["order"]["triggerCondition"] = "Price below 315.62".into(),
            |v| v["triggered_status"]["order"]["triggerPx"] = "bad".into(),
            |v| v["triggered_status"]["order"]["orderType"] = "Trailing Stop Market".into(),
            |v| v["triggered_status"]["order"]["coin"] = "xyz:OTHER".into(),
        ];
        for change in changes {
            let mut case = cases["cases"][0].clone(); change(&mut case);
            let input = TriggerFixture::of(&case);
            let fill = &input.fills[0];
            let range = input.prices().get(input.height, &fill.coin, fill.tid).unwrap();
            assert_eq!((range.min_px_raw, range.max_px_raw), (3_153_500, 3_156_700));
        }
        for opening in ["315.61", "315.50"] {
            let mut case = cases["cases"][0].clone(); case["before_mark"] = opening.into();
            let input = TriggerFixture::of(&case); let fill = &input.fills[0];
            let range = input.prices().get(input.height, &fill.coin, fill.tid).unwrap();
            assert_ne!(range.min_px_raw, range.max_px_raw, "an already-met or equal boundary has no crossing proof");
        }
    }

    #[test]
    fn rising_trigger_and_duplicate_status_boundary() {
        let mut case = trigger_cases()["cases"][0].clone();
        case["before_mark"] = "315.35".into(); case["after_mark"] = "315.67".into();
        case["triggered_status"]["order"]["triggerCondition"] = "Price above 315.61".into();
        let mut input = TriggerFixture::of(&case); let fill = input.fills[0].clone();
        assert_eq!(input.prices().get(input.height, &fill.coin, fill.tid).unwrap().min_px_raw, 3_156_700);
        let status: crate::types::node_data::NodeDataOrderStatus = serde_json::from_value(case["triggered_status"].clone()).unwrap();
        input.statuses = BlockStatuses::of([status.clone(), status]).unwrap();
        let range = input.prices().get(input.height, &fill.coin, fill.tid).unwrap();
        assert_ne!(range.min_px_raw, range.max_px_raw, "duplicate trigger observations cannot supply unique proof");
    }

}

#[cfg(test)]
mod modify_tests;


#[cfg(test)]
mod interleaved_tests;
#[cfg(test)]
pub(crate) use interleaved_tests::interleaved_golden_prices;

#[cfg(test)]
mod oracle_hash_tests;

#[cfg(test)]
pub(crate) use oracle_hash_tests::oracle_golden_prices;
