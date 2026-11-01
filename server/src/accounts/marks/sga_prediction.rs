//! Research (hyperliquid-trade `mempool_pending_book_20261031`, trigger orders): a block's `SetGlobalAction` marks as a
//! pending book could predict them before the block - the block's price actions, in order, on the state of the block
//! before, each reading the book as the block began and no trade of the block - against the marks the block wrote.
//! Real data, ignored:
//!
//! `SGA_CHECKPOINT=<.rmp> SGA_BLOCKS=<replica_cmds files,...> SGA_DIFFS=<hour files,...> SGA_FILLS=... SGA_STATUSES=...
//! SGA_SYSTEM=... SGA_HIP3=... SGA_BLOCK_COUNT=<n> cargo test --release -p server --lib -- --ignored
//! sga_marks_from_the_block_before --nocapture`
//!
//! The same inputs with `SGA_NEXT_CHECKPOINT=<.rmp>` in place of `SGA_BLOCK_COUNT` replay to that checkpoint and check
//! every main-dex coin against it (`replay_checks_against_the_next_checkpoint`; XTEN's oracle without a Spot key, bm 11-01).

use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader},
};

use serde::de::DeserializeOwned;

use super::{actions::SubmittedPrices, *};
use crate::{
    accounts::{checkpoint::parse_checkpoint_file, replica::ReplicaLine},
    types::node_data::NodeDataFill,
};

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name}"))
}

#[expect(clippy::needless_collect, reason = "the iterator returned outlives the variable's text")]
fn lines(name: &str) -> Box<dyn Iterator<Item = String>> {
    let paths: Vec<String> = env(name).split(',').map(str::to_string).collect();
    Box::new(paths.into_iter().flat_map(|path| BufReader::new(std::fs::File::open(path).unwrap()).lines().map(Result::unwrap)))
}

/// A stream's batches, block by block.
struct Blocks<E> {
    lines: Box<dyn Iterator<Item = String>>,
    next: Option<Batch<E>>,
}

impl<E: DeserializeOwned> Blocks<E> {
    fn open(name: &str) -> Self {
        Self { lines: lines(name), next: None }
    }

    /// The batches of block `height`, those of earlier blocks dropped.
    fn take(&mut self, height: u64) -> Vec<Batch<E>> {
        let mut taken = Vec::new();
        loop {
            if self.next.is_none() {
                self.next = self.lines.next().map(|line| sonic_rs::from_str(&line).unwrap());
            }
            let Some(batch) = self.next.take() else { return taken };
            if batch.block_number() > height {
                self.next = Some(batch);
                return taken;
            }
            if batch.block_number() == height {
                taken.push(batch);
            }
        }
    }
}

/// Each main-dex asset's mark after the block's price actions on `state`, every one reading the book as the block began.
fn predicted_marks(state: &MarkState, height: u64, actions: &MarkActions) -> Vec<Option<u64>> {
    let mut state = state.clone();
    state.prices.begin_block(actions.time_ns);
    let books = &state.books;
    let cut_of = |asset: usize| CoinCut { best: books.best(asset), spot_reference: books.spot_reference(asset).map(|book| books.best(book)), block_trade: None };
    state.prices.refresh_stale(height, actions.time_ns, cut_of).unwrap();
    for action in &actions.actions {
        if matches!(action.action, MarkAction::SetGlobal(_) | MarkAction::Reference { .. }) {
            state.prices.apply(&action.action, action.user, actions.time_ns, cut_of).unwrap();
        }
    }
    (0..state.prices.coin_count()).map(|asset| state.prices.mark(asset)).collect()
}

/// The heights of the blocks holding a `SetGlobalAction`, from a cheap scan of the lines (a line's height is its last key).
fn set_global_heights() -> std::collections::BTreeSet<u64> {
    lines("SGA_BLOCKS")
        .filter(|line| line.contains("\"SetGlobalAction\""))
        .map(|line| {
            let tail = &line[line.rfind("\"height\":").unwrap() + 9..];
            tail.trim_end_matches('}').trim().parse().unwrap()
        })
        .collect()
}

#[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_precision_loss, reason = "a quantile's index")]
fn quantiles(mut values: Vec<f64>) -> String {
    if values.is_empty() {
        return "-".to_string();
    }
    values.sort_by(f64::total_cmp);
    let pick = |q: f64| values[((values.len() - 1) as f64 * q) as usize];
    format!("p50 {:.2} p90 {:.2} p99 {:.2} max {:.2}", pick(0.5), pick(0.9), pick(0.99), pick(1.0))
}

/// As `sga_marks_from_the_block_before`, the state and book taken `k` blocks before the `SetGlobalAction`'s block, for k 1
/// to 4 (the pending book's replica is ~3 blocks behind when the action's line comes): the marks exact, and off.
#[test]
#[ignore = "requires real node data: see the module's doc"]
fn sga_marks_from_blocks_before() {
    let lags = [1_u64, 2, 3, 4];
    let set_globals = set_global_heights();
    let checkpoint = parse_checkpoint_file(std::path::Path::new(&env("SGA_CHECKPOINT"))).unwrap();
    let seed = MarkSeed::from_checkpoint(checkpoint).unwrap();
    let start = seed.height;
    let mut state = MarkState::seeded(seed);
    let count: u64 = env("SGA_BLOCK_COUNT").parse().unwrap();
    let (mut diffs, mut fills, mut statuses) = (Blocks::<NodeDataOrderDiff>::open("SGA_DIFFS"), Blocks::<NodeDataFill>::open("SGA_FILLS"), Blocks::<NodeDataOrderStatus>::open("SGA_STATUSES"));
    let (mut system, mut hip3) = (Blocks::<system::SystemAction>::open("SGA_SYSTEM"), Blocks::<OracleUpdateEvent>::open("SGA_HIP3"));
    // the states kept for an upcoming SetGlobalAction: (its height, lag) -> the state after the block lag before it
    let mut kept: BTreeMap<(u64, u64), MarkState> = BTreeMap::new();
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    let mut triggered_counts: BTreeMap<String, u64> = BTreeMap::new();
    for line in lines("SGA_BLOCKS") {
        let actions = MarkActions::of(&ReplicaLine::parse(&line).unwrap()).unwrap();
        let height = actions.height;
        if height <= start {
            continue;
        }
        if height > start + count {
            break;
        }
        let block_statuses = statuses.take(height);
        let triggered: Vec<String> =
            block_statuses.iter().flat_map(Batch::events_ref).filter(|status| status.status == "triggered").map(|status| status.order.coin.clone()).collect();
        let inputs = MarkInputs {
            hip3_oracle_updates: hip3.take(height),
            book_diffs: diffs.take(height),
            mark_actions: Ok(actions.clone()),
            order_statuses: block_statuses,
            fills: fills.take(height).iter().flat_map(Batch::events_ref).map(BlockFill::of).collect(),
            system_actions: system.take(height),
            collateral_coins: Vec::new(),
        };
        let mut report = MarksBlockReport::default();
        let block = BlockInputs::of(height, inputs, &mut report);
        let set_global = actions.actions.iter().find_map(|action| match &action.action {
            MarkAction::SetGlobal(pxs) => Some(pxs.clone()),
            _ => None,
        });
        let predicted: Vec<(u64, Vec<Option<u64>>)> = match &set_global {
            Some(_) => lags.iter().filter_map(|lag| kept.remove(&(height, *lag)).map(|before| (*lag, predicted_marks(&before, height, &actions)))).collect(),
            None => Vec::new(),
        };
        state.apply(height, &block, &mut report);
        assert!(report.errors.is_empty(), "{height}: {:?}", report.errors);
        for lag in lags {
            if set_globals.contains(&(height + lag)) {
                kept.insert((height + lag, lag), state.clone());
            }
        }
        let Some(pxs) = set_global else { continue };
        for (lag, marks) in predicted {
            for (asset, prices) in pxs.iter().enumerate() {
                let (true, Some(actual), Some(guess)) = (matches!(prices, SubmittedPrices::Priced { .. }), state.prices.mark(asset), marks.get(asset).copied().flatten()) else { continue };
                let outcome = if guess == actual { "exact" } else { "off" };
                *counts.entry(format!("lag {lag} {outcome}")).or_default() += 1;
                if triggered.iter().any(|coin| state.prices.asset(coin) == Some(asset)) {
                    *triggered_counts.entry(format!("lag {lag} {outcome}")).or_default() += 1;
                }
            }
        }
    }
    println!("all coins {counts:?}");
    println!("coins with a trigger fired {triggered_counts:?}");
}

#[test]
#[ignore = "requires real node data: see the module's doc"]
fn sga_marks_from_the_block_before() {
    let checkpoint = parse_checkpoint_file(std::path::Path::new(&env("SGA_CHECKPOINT"))).unwrap();
    let seed = MarkSeed::from_checkpoint(checkpoint).unwrap();
    let start = seed.height;
    let mut state = MarkState::seeded(seed);
    let count: u64 = env("SGA_BLOCK_COUNT").parse().unwrap();
    let (mut diffs, mut fills, mut statuses) = (Blocks::<NodeDataOrderDiff>::open("SGA_DIFFS"), Blocks::<NodeDataFill>::open("SGA_FILLS"), Blocks::<NodeDataOrderStatus>::open("SGA_STATUSES"));
    let (mut system, mut hip3) = (Blocks::<system::SystemAction>::open("SGA_SYSTEM"), Blocks::<OracleUpdateEvent>::open("SGA_HIP3"));
    let mut counts: BTreeMap<&str, u64> = BTreeMap::new();
    let (mut errors_bps, mut triggered_errors_bps) = (Vec::new(), Vec::new());
    for line in lines("SGA_BLOCKS") {
        let actions = MarkActions::of(&ReplicaLine::parse(&line).unwrap()).unwrap();
        let height = actions.height;
        if height <= start {
            continue;
        }
        if height > start + count {
            break;
        }
        let block_statuses = statuses.take(height);
        let triggered: Vec<String> = block_statuses
            .iter()
            .flat_map(Batch::events_ref)
            .filter(|status| status.status == "triggered")
            .map(|status| status.order.coin.clone())
            .collect();
        let inputs = MarkInputs {
            hip3_oracle_updates: hip3.take(height),
            book_diffs: diffs.take(height),
            mark_actions: Ok(actions.clone()),
            order_statuses: block_statuses,
            fills: fills.take(height).iter().flat_map(Batch::events_ref).map(BlockFill::of).collect(),
            system_actions: system.take(height),
            collateral_coins: Vec::new(),
        };
        let mut report = MarksBlockReport::default();
        let block = BlockInputs::of(height, inputs, &mut report);
        let set_global = actions.actions.iter().find_map(|action| match &action.action {
            MarkAction::SetGlobal(pxs) => Some(pxs.clone()),
            _ => None,
        });
        let predicted = set_global.as_ref().map(|_| predicted_marks(&state, height, &actions));
        state.apply(height, &block, &mut report);
        assert!(report.errors.is_empty(), "{height}: {:?}", report.errors);
        *counts.entry("blocks").or_default() += 1;
        let (Some(pxs), Some(predicted)) = (set_global, predicted) else { continue };
        *counts.entry("set globals").or_default() += 1;
        for (asset, prices) in pxs.iter().enumerate() {
            if !matches!(prices, SubmittedPrices::Priced { .. }) {
                continue;
            }
            let (Some(actual), Some(guess)) = (state.prices.mark(asset), predicted.get(asset).copied().flatten()) else {
                *counts.entry("coin not replayed").or_default() += 1;
                continue;
            };
            let bps = (guess as f64 - actual as f64).abs() / actual as f64 * 1e4;
            let fired = triggered.iter().any(|coin| state.prices.asset(coin) == Some(asset));
            *counts.entry(if guess == actual { "coin mark exact" } else { "coin mark off" }).or_default() += 1;
            errors_bps.push(bps);
            if fired {
                *counts.entry(if guess == actual { "triggered coin mark exact" } else { "triggered coin mark off" }).or_default() += 1;
                triggered_errors_bps.push(bps);
            }
        }
    }
    println!("{counts:?}");
    println!("off by, bp: all {}", quantiles(errors_bps.into_iter().filter(|bps| *bps > 0.0).collect()));
    println!("off by, bp: coins with a trigger fired {}", quantiles(triggered_errors_bps.into_iter().filter(|bps| *bps > 0.0).collect()));
}

/// From `SGA_CHECKPOINT` to `SGA_NEXT_CHECKPOINT`, every block applied as the account view applies it; each main-dex coin's
/// prices and caches against the next checkpoint's, the mismatches printed.
#[test]
#[ignore = "requires real node data: see the module's doc"]
fn replay_checks_against_the_next_checkpoint() {
    let seed = MarkSeed::from_checkpoint(parse_checkpoint_file(std::path::Path::new(&env("SGA_CHECKPOINT"))).unwrap()).unwrap();
    let next = MarkSeed::from_checkpoint(parse_checkpoint_file(std::path::Path::new(&env("SGA_NEXT_CHECKPOINT"))).unwrap()).unwrap();
    let (start, end) = (seed.height, next.height);
    let mut state = MarkState::seeded(seed);
    let (mut diffs, mut fills, mut statuses) = (Blocks::<NodeDataOrderDiff>::open("SGA_DIFFS"), Blocks::<NodeDataFill>::open("SGA_FILLS"), Blocks::<NodeDataOrderStatus>::open("SGA_STATUSES"));
    let (mut system, mut hip3) = (Blocks::<system::SystemAction>::open("SGA_SYSTEM"), Blocks::<OracleUpdateEvent>::open("SGA_HIP3"));
    let mut blocks = 0_u64;
    for line in lines("SGA_BLOCKS") {
        let actions = MarkActions::of(&ReplicaLine::parse(&line).unwrap()).unwrap();
        let height = actions.height;
        if height <= start {
            continue;
        }
        if height > end {
            break;
        }
        let inputs = MarkInputs {
            hip3_oracle_updates: hip3.take(height),
            book_diffs: diffs.take(height),
            mark_actions: Ok(actions),
            order_statuses: statuses.take(height),
            fills: fills.take(height).iter().flat_map(Batch::events_ref).map(BlockFill::of).collect(),
            system_actions: system.take(height),
            collateral_coins: Vec::new(),
        };
        let mut report = MarksBlockReport::default();
        let block = BlockInputs::of(height, inputs, &mut report);
        state.apply(height, &block, &mut report);
        assert!(report.errors.is_empty(), "{height}: {:?}", report.errors);
        blocks += 1;
    }
    let check = state.prices.check_against(&next.native);
    println!("{start} -> {end}: {blocks} blocks");
    println!("fields (equal, differ) {:?}", check.fields);
    println!("unsupported {:?}", check.unsupported);
    for sample in &check.samples {
        println!("mismatch {} {}: replayed {} checkpoint {}", sample.coin, sample.field, sample.replayed, sample.checkpoint);
    }
}
