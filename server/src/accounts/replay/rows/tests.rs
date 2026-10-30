use super::*;
use crate::accounts::{
    checkpoint::tests::{HOLDER, user},
    liquidation::LiquidationPx,
    replay::{Stop, StopReason, tests::per_dex_with_main_cash},
};
use std::{cell::Cell, hint::black_box, time::Instant};

/// Frozen pre-batch scan (f018925). Deliberately does not use `CrossPool`, `PoolAtMarks`, `net_micro` or `solve_net`:
/// a change to production grouping, missing-state handling or the sum must not change the reference with it.
fn original_row(
    states: &MarginStates,
    target: HeldPosition<'_>,
    held: &[HeldPosition<'_>],
    mark_of: impl Fn(&str) -> Option<u64>,
) -> Option<ReplayedRow> {
    let account = states.accounts.get(&user(HOLDER)).filter(|account| account.stop.is_none())?;
    let market = states.markets.perps.get(target.coin).filter(|market| states.markets.usdc_dexes[market.dex])?;
    let table = states.markets.tables[market.dex].get(&target.table_id?)?;
    let size = base_size(target.size_raw, market.size_decimals);
    if let Some(cash) = account.isolated.get(&(market.dex, market.asset)) {
        return Some(ReplayedRow {
            isolated: true, pool_positions: 1, liquidation_px: solve(*cash as f64 / 1e8, 0.0, size, table),
            cash_estimated: account.estimated,
        });
    }
    let mut others = Vec::new();
    for other in held.iter().filter(|other| other.coin != target.coin) {
        let market = states.markets.perps.get(other.coin)?;
        let same_pool = match &account.pools {
            Pools::PerDex(_) => market.dex == states.markets.perps[target.coin].dex,
            Pools::Unified(_) => states.markets.usdc_dexes[market.dex],
        };
        if !same_pool || account.isolated.contains_key(&(market.dex, market.asset)) {
            continue;
        }
        others.push(HeldAtMark {
            size_raw: other.size_raw, mark_raw: mark_of(other.coin)?,
            table: states.markets.tables[market.dex].get(&other.table_id?)?,
        });
    }
    let cash = match &account.pools {
        Pools::PerDex(cash) => cash.get(&market.dex).copied().unwrap_or(0),
        Pools::Unified(cash) => *cash,
    };
    let net_micro: i128 = others.iter().map(|other| {
        let notional = u128::from(other.size_raw.unsigned_abs()) * u128::from(other.mark_raw);
        i128::from(other.size_raw) * i128::from(other.mark_raw) - other.table.maintenance_micro(notional)
    }).sum();
    Some(ReplayedRow {
        isolated: false, pool_positions: others.len() + 1,
        liquidation_px: solve(cash as f64 / 1e8 + net_micro as f64 / 1e6, 0.0, size, table),
        cash_estimated: account.estimated,
    })
}

struct Fixture {
    states: MarginStates,
    coins: Vec<String>,
    positions: Vec<(i64, Option<u32>)>,
    marks: FxHashMap<String, u64>,
}

impl Fixture {
    fn new(count: usize, per_dex: bool) -> Self {
        let mut states = per_dex_with_main_cash(-12_000_000_000);
        let template = states.markets.perps["BTC"].clone();
        states.markets.perps.clear();
        let account = states.accounts.get_mut(&user(HOLDER)).unwrap();
        account.isolated.clear();
        if !per_dex { account.pools = Pools::Unified(-1_196_973_030_003); }
        let mut fixture = Self { states, coins: Vec::new(), positions: Vec::new(), marks: FxHashMap::default() };
        for index in 0..count {
            let coin = format!("C{index}");
            let market = PerpMarket { asset: index, size_decimals: u32::try_from(index % 6).unwrap(), ..template.clone() };
            fixture.states.markets.perps.insert(coin.clone(), market);
            fixture.positions.push((if index % 3 == 1 { -17 } else { 19 }, Some(20)));
            fixture.marks.insert(coin.clone(), 828_577 + index as u64 * 11);
            fixture.coins.push(coin);
        }
        fixture
    }

    fn held(&self) -> Vec<HeldPosition<'_>> {
        self.coins.iter().zip(&self.positions).map(|(coin, (size_raw, table_id))|
            HeldPosition { coin, size_raw: *size_raw, table_id: *table_id }).collect()
    }

    fn original<'a>(&self, held: &[HeldPosition<'a>]) -> Vec<(HeldPosition<'a>, ReplayedRow)> {
        held.iter().copied().filter(|target| target.size_raw != 0).filter_map(|target|
            Some((target, original_row(&self.states, target, held, |coin| self.marks.get(coin).copied())?))).collect()
    }

    fn check(&self, held: &[HeldPosition<'_>]) {
        let want = self.original(held);
        let got = self.states.replayed_rows(&user(HOLDER), held.iter().copied(), |coin| self.marks.get(coin).copied());
        assert_eq!(got.len(), want.len(), "different available rows");
        for ((position, row), (expected_position, expected_row)) in got.iter().zip(&want) {
            assert_eq!(position.coin, expected_position.coin, "holdings order");
            assert_eq!(position.size_raw, expected_position.size_raw);
            assert_row_bits(row, expected_row);
        }
        for &target in held {
            let Some(table_id) = target.table_id else { continue };
            let actual = self.states.replayed_row(&user(HOLDER), target.coin, target.size_raw, table_id,
                held.iter().copied(), |coin| self.marks.get(coin).copied());
            let expected = original_row(&self.states, target, held, |coin| self.marks.get(coin).copied());
            assert_eq!(actual.is_some(), expected.is_some(), "single query {}", target.coin);
            if let (Some(actual), Some(expected)) = (actual, expected) { assert_row_bits(&actual, &expected); }
        }
    }
}

fn assert_row_bits(actual: &ReplayedRow, expected: &ReplayedRow) {
    assert_eq!(actual, expected);
    if let (LiquidationPx::Computed(actual), LiquidationPx::Computed(expected)) = (&actual.liquidation_px, &expected.liquidation_px) {
        assert_eq!(actual.to_bits(), expected.to_bits(), "no floating-point tolerance");
    }
}

#[test]
fn batch_matches_original_across_pool_modes_and_missing_inputs() {
    for per_dex in [false, true] {
        // Each bit removes one mark: includes no marks, only the target missing, and multiple missing members.
        for missing_mask in 0..64 {
            let mut fixture = Fixture::new(6, per_dex);
            fixture.states.markets.perps.get_mut("C2").unwrap().dex = 1;
            fixture.states.markets.perps.get_mut("C3").unwrap().dex = 2; // non-USDC
            fixture.states.accounts.get_mut(&user(HOLDER)).unwrap().isolated.insert((0, 4), -12_345_678_901);
            fixture.states.accounts.get_mut(&user(HOLDER)).unwrap().estimated = missing_mask % 2 == 1;
            for index in 0..6 {
                if missing_mask & (1 << index) != 0 { fixture.marks.remove(&format!("C{index}")); }
            }
            for bad_table in [None, Some(None), Some(Some(99_999))] {
                if let Some(table) = bad_table { fixture.positions[1].1 = table; }
                let mut held = fixture.held();
                for _ in 0..held.len() {
                    fixture.check(&held);
                    held.rotate_left(1);
                }
            }
        }
    }
}

#[test]
fn missing_target_mark_does_not_withhold_its_own_row() {
    let mut fixture = Fixture::new(3, false);
    fixture.states.accounts.get_mut(&user(HOLDER)).unwrap().isolated.insert((0, 2), 10_000_000_000);
    fixture.marks.remove("C0");
    fixture.marks.remove("C2");
    let rows = fixture.states.replayed_rows(&user(HOLDER), fixture.held(), |coin| fixture.marks.get(coin).copied());
    assert_eq!(rows.iter().map(|(position, _)| position.coin).collect::<Vec<_>>(), ["C0", "C2"]);
    assert_eq!(rows[0].1.pool_positions, 2);
    assert!(rows[1].1.isolated);
    fixture.check(&fixture.held());
}

#[test]
fn unknown_market_still_withholds_cross_rows_but_not_isolated_rows() {
    let mut fixture = Fixture::new(3, true);
    fixture.states.accounts.get_mut(&user(HOLDER)).unwrap().isolated.insert((0, 1), 123);
    fixture.states.markets.perps.remove("C2");
    fixture.check(&fixture.held());
    let rows = fixture.states.replayed_rows(&user(HOLDER), fixture.held(), |_| None);
    assert_eq!(rows.iter().map(|(position, _)| position.coin).collect::<Vec<_>>(), ["C1"]);
    fixture.states.accounts.get_mut(&user(HOLDER)).unwrap().stop = Some(Stop { block: Some(7), reason: StopReason::AccountModeSwitch });
    fixture.check(&fixture.held());
    assert!(fixture.states.replayed_rows(&user(HOLDER), fixture.held(), |_| Some(1)).is_empty());
    fixture.states.accounts.remove(&user(HOLDER));
    fixture.check(&fixture.held());
}

#[test]
fn one_read_does_not_reuse_previous_marks_tables_cash_or_membership() {
    let mut fixture = Fixture::new(5, true);
    fixture.check(&fixture.held());
    fixture.marks.insert("C0".into(), 3_000_000);
    fixture.positions[1].0 = -51; // fill / flip
    fixture.positions[2].0 = 0; // close
    fixture.check(&fixture.held());
    fixture.states.accounts.get_mut(&user(HOLDER)).unwrap().pools = Pools::Unified(55_123_456_789);
    fixture.states.accounts.get_mut(&user(HOLDER)).unwrap().isolated.insert((0, 3), 9_000_001);
    fixture.states.markets.perps.get_mut("C4").unwrap().dex = 1;
    fixture.states.markets.tables[0].insert(20, MarginTable::single(3).unwrap());
    fixture.check(&fixture.held());
    fixture.states.accounts.get_mut(&user(HOLDER)).unwrap().isolated.remove(&(0, 3));
    fixture.marks.remove("C1");
    fixture.check(&fixture.held());
    fixture.marks.insert("C1".into(), 999_999);
    fixture.check(&fixture.held());
}

#[test]
fn exact_tier_boundaries_and_mixed_signs_match_the_original() {
    let mut fixture = Fixture::new(4, false);
    let table = MarginTable::listed([(0, 10, 0), (20_000_000, 5, 1_000_000)]).unwrap();
    fixture.states.markets.tables[0].insert(52, table);
    for (index, position) in fixture.positions.iter_mut().enumerate() {
        *position = (if index % 2 == 0 { 1 } else { -1 }, Some(52));
    }
    for offset in [-1_i64, 0, 1] {
        for mark in fixture.marks.values_mut() { *mark = u64::try_from(20_000_000 + offset).unwrap(); }
        for cash in [-100_000_000_001, -1, 0, 1, 100_000_000_001] {
            fixture.states.accounts.get_mut(&user(HOLDER)).unwrap().pools = Pools::Unified(cash);
            fixture.check(&fixture.held());
            let mut held = fixture.held();
            held.reverse();
            fixture.check(&held);
        }
    }
}

#[test]
fn recorded_aster_zero_boundary_matches_with_wei_perturbations() {
    let golden: serde_json::Value = serde_json::from_str(include_str!("../../aster_zero_boundary_golden.json")).unwrap();
    let positions = golden["positions"].as_array().unwrap();
    let mut fixture = Fixture::new(positions.len(), false);
    for (index, position) in positions.iter().enumerate() {
        let table = MarginTable::listed(position["tiers"].as_array().unwrap().iter().map(|tier|
            (tier[0].as_i64().unwrap(), u32::try_from(tier[1].as_u64().unwrap()).unwrap(), tier[2].as_i64().unwrap()))).unwrap();
        let table_id = 1000 + u32::try_from(index).unwrap();
        fixture.states.markets.tables[0].insert(table_id, table);
        fixture.states.markets.perps.get_mut(&fixture.coins[index]).unwrap().size_decimals = u32::try_from(position["size_decimals"].as_u64().unwrap()).unwrap();
        fixture.positions[index] = (position["size_raw"].as_i64().unwrap(), Some(table_id));
        fixture.marks.insert(fixture.coins[index].clone(), position["mark_raw"].as_u64().unwrap());
    }
    let target = positions.iter().position(|position| position["coin"] == "ASTER").unwrap();
    for delta_wei in [-1, 0, 1] {
        fixture.states.accounts.get_mut(&user(HOLDER)).unwrap().pools = Pools::Unified(golden["cash_micro"].as_i64().unwrap() * 100 + delta_wei);
        let mut held = fixture.held();
        for reverse in [false, true] {
            if reverse { held.reverse(); }
            for _ in 0..held.len() {
                fixture.check(&held);
                held.rotate_left(1);
            }
        }
        let rows = fixture.states.replayed_rows(&user(HOLDER), fixture.held(), |coin| fixture.marks.get(coin).copied());
        let price = &rows.iter().find(|(position, _)| position.coin == fixture.coins[target]).unwrap().1.liquidation_px;
        if delta_wei == 0 { assert_eq!(*price, LiquidationPx::NoPositiveSolution); }
        if delta_wei == -1 { assert!(matches!(price, LiquidationPx::Computed(_))); }
    }
}

#[test]
fn batch_reads_each_cross_mark_once_and_preserves_single_target_overrides() {
    let fixture = Fixture::new(64, false);
    let calls = Cell::new(0);
    let rows = fixture.states.replayed_rows(&user(HOLDER), fixture.held(), |coin| {
        calls.set(calls.get() + 1);
        fixture.marks.get(coin).copied()
    });
    assert_eq!((rows.len(), calls.get()), (64, 64), "one held-mark valuation per member, not 64 * 63");
    let mut held = fixture.held();
    let target = HeldPosition { size_raw: -321, table_id: Some(52), ..held[0] };
    held[0].table_id = None; // supplied target table/size override its unusable held record
    for include_target in [true, false] {
        if !include_target { held.remove(0); }
        let want = original_row(&fixture.states, target, &held, |coin| fixture.marks.get(coin).copied()).unwrap();
        let actual = fixture.states.replayed_row(&user(HOLDER), target.coin, target.size_raw, 52, held.iter().copied(),
            |coin| { assert_ne!(coin, target.coin); fixture.marks.get(coin).copied() }).unwrap();
        assert_row_bits(&actual, &want);
    }
}

#[test]
fn subtraction_stays_integer_above_float_exact_range() {
    let mut fixture = Fixture::new(3, false);
    fixture.states.accounts.get_mut(&user(HOLDER)).unwrap().pools = Pools::Unified(-123);
    fixture.marks.insert("C0".into(), 1);
    fixture.marks.insert("C1".into(), 7);
    fixture.marks.insert("C2".into(), 3);
    fixture.positions[1].0 = 1;
    fixture.positions[2].0 = -1;
    for sign in [-1, 1] {
        fixture.positions[0].0 = sign * ((1_i64 << 53) + 1);
        let mut held = fixture.held();
        for _ in 0..held.len() {
            fixture.check(&held);
            held.rotate_left(1);
        }
    }
}

#[test]
fn empty_and_single_member_reads_need_no_pool_sum_or_mark() {
    let mut fixture = Fixture::new(1, false);
    fixture.check(&[]);
    fixture.marks.clear();
    for isolated in [false, true] {
        if isolated { fixture.states.accounts.get_mut(&user(HOLDER)).unwrap().isolated.insert((0, 0), 123); }
        fixture.check(&fixture.held());
        let rows = fixture.states.replayed_rows(&user(HOLDER), fixture.held(), |_| panic!("a lone target needs no mark"));
        assert_eq!(rows.len(), 1);
        assert_eq!((rows[0].1.isolated, rows[0].1.pool_positions), (isolated, 1));
    }
    for table in [None, Some(99_999)] {
        fixture.positions[0].1 = table;
        fixture.check(&fixture.held());
        assert!(fixture.states.replayed_rows(&user(HOLDER), fixture.held(), |_| None).is_empty());
    }
    fixture.positions[0] = (0, Some(20));
    fixture.check(&fixture.held());
    assert!(fixture.states.replayed_rows(&user(HOLDER), fixture.held(), |_| None).is_empty());
}

/// Isolated microbenchmark, not a service-wide speedup. Run release, ignored, nocapture; no runtime or network.
#[test]
#[ignore = "release timing benchmark; run explicitly with --ignored --nocapture"]
fn benchmark_pool_batch_against_original_scan() {
    for count in [1, 4, 16, 64, 196] {
        let fixture = Fixture::new(count, false);
        let held = fixture.held();
        fixture.check(&held);
        let iterations = 200_000 / count;
        let mut scans = Vec::new();
        let mut batches = Vec::new();
        for round in 0..5 {
            for batch in if round % 2 == 0 { [false, true] } else { [true, false] } {
                let start = Instant::now();
                for _ in 0..iterations {
                    if batch {
                        black_box(fixture.states.replayed_rows(black_box(&user(HOLDER)), held.iter().copied(),
                            |coin| fixture.marks.get(coin).copied()));
                    } else {
                        black_box(fixture.original(black_box(&held)));
                    }
                }
                if batch { batches.push(start.elapsed()); } else { scans.push(start.elapsed()); }
            }
        }
        scans.sort();
        batches.sort();
        eprintln!("positions={count} iterations={iterations} original_median_ms={:.3} batch_median_ms={:.3} ratio={:.2}",
            scans[2].as_secs_f64() * 1000.0, batches[2].as_secs_f64() * 1000.0, scans[2].as_secs_f64() / batches[2].as_secs_f64());
    }
}
