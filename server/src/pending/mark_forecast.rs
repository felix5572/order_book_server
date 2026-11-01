//! Batch 2l, first step (hyperliquid-trade design 000639): the marks a `SetGlobalAction` the mempool shows will store,
//! forecast as its line comes - on the account view's node prices (`NodePrices`) and each coin's best prices, the pending
//! book's merged ones and the replica's confirmed ones apart - and checked against the marks the account view applies
//! with its block. Metrics and logs only: nothing the pending book serves changes.

use std::{cmp::Ordering, collections::VecDeque, sync::Arc};

use alloy::primitives::B256;
use log::info;
use tokio::time::Instant;

use super::{
    blocks::LoggedBlock,
    mempool::{ActionKind, MempoolTx},
    publish::BestLevels,
    shadow::Shadow,
};
use crate::{
    accounts::{
        book::ActionCoins,
        marks::{
            NativePriceSnapshot,
            actions::SubmittedPrices,
            native::{CoinPriceState, MarkUpdate, reference_bucket},
            rules::wire_units,
        },
        publish::NodePrices,
    },
    listeners::order_book::{replica::BookReplica, throttled_log_due},
    metrics::{
        PENDING_BOOK_SGA_BUCKET_TOTAL, PENDING_BOOK_SGA_LINES_TOTAL, PENDING_BOOK_SGA_MARKS_TOTAL, PENDING_BOOK_SGA_MARK_ERROR_BPS,
        PENDING_BOOK_SGA_PRICES_BEHIND_BLOCKS, PENDING_BOOK_STEP_SECONDS,
    },
    order_book::{Coin, Px},
};

/// Forecasts waiting for their check: a `SetGlobalAction` comes about every 3 s, so one is in flight as a rule.
const MAX_IN_FLIGHT: usize = 8;
/// A forecast unchecked this long after its line - its block not in the log, the account view stopped - expires.
const EXPIRY_NS: i64 = 30_000_000_000;
/// The books a forecast reads its coins' best prices from, in `ForecastBook::best`'s order.
const BOOKS: [&str; 2] = ["pending", "confirmed"];

/// What a forecast reads of the pending book while it may be served.
pub(crate) trait ForecastBook {
    /// The coin's best levels where a line seen at `seen_ns` runs: the merged book's, the replica's confirmed ones
    /// (review 000642 r1 P2-2).
    fn best(&self, coin: &Coin, seen_ns: i64) -> (BestLevels, BestLevels);
    /// Whether the block log showed the line's block already.
    fn logged(&self, tx_hash: &B256) -> bool;
}

/// The pending book as served: the pending set over its replica.
pub(crate) struct ServedBook<'a> {
    shadow: &'a Shadow,
    replica: &'a BookReplica,
}

impl<'a> ServedBook<'a> {
    /// The pending book while it may be served.
    pub(crate) fn of(shadow: &'a Shadow, replica: Option<&'a BookReplica>) -> Option<Self> {
        let replica = replica?;
        shadow.availability(Some(replica)).is_ok().then_some(Self { shadow, replica })
    }
}

impl ForecastBook for ServedBook<'_> {
    fn best(&self, coin: &Coin, seen_ns: i64) -> (BestLevels, BestLevels) {
        self.shadow.best_at_line(coin, self.replica, seen_ns)
    }

    fn logged(&self, tx_hash: &B256) -> bool {
        self.shadow.logged(tx_hash)
    }
}

/// A coin's forecast on one book: its best prices in price units and the mark they give.
#[derive(Debug, Clone, Copy)]
struct BookForecast {
    bid: u64,
    ask: u64,
    mark: u64,
}

/// A coin's forecast marks and the node prices' inputs they read (for the evidence of one off).
#[derive(Debug)]
struct CoinForecast {
    asset: u32,
    coin: String,
    submitted: u64,
    last_trade: u64,
    /// The Mark cache's stored (median, bucket).
    cache: Option<(Option<u64>, u64)>,
    /// On each of `BOOKS`; `None` where it gave none (counted then).
    books: [Option<BookForecast>; 2],
}

/// One `SetGlobalAction`'s forecast.
#[derive(Debug)]
struct Forecast {
    tx_hash: B256,
    seen_ns: i64,
    /// The node prices' height it read.
    prices_height: u64,
    coins: Vec<CoinForecast>,
    /// Its block as the log shows it: (height, block time ms).
    block: Option<(u64, u64)>,
}

/// The forecasts in flight and the node prices they are made on and checked against.
#[derive(Debug, Default)]
pub(crate) struct SgaForecasts {
    prices: Option<Arc<NativePriceSnapshot>>,
    in_flight: VecDeque<Forecast>,
    last_off_log: Option<Instant>,
}

/// What an input came to, as the metrics count it.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct ForecastReport {
    /// The lines' outcomes (`PENDING_BOOK_SGA_LINES_TOTAL`).
    pub(crate) lines: Vec<&'static str>,
    /// Each coin's (book, outcome).
    pub(crate) marks: Vec<(&'static str, &'static str)>,
    /// Each off mark's (book, error in bp).
    pub(crate) errors_bps: Vec<(&'static str, f64)>,
    /// Whether a line time fell in its block time's bucket.
    pub(crate) buckets: Vec<&'static str>,
    /// Each checked forecast's blocks between the node prices it read and its block.
    pub(crate) behind_blocks: Vec<u64>,
}

impl ForecastReport {
    fn counted(self) -> Self {
        for outcome in &self.lines {
            PENDING_BOOK_SGA_LINES_TOTAL.with_label_values(&[outcome]).inc();
        }
        for (book, outcome) in &self.marks {
            PENDING_BOOK_SGA_MARKS_TOTAL.with_label_values(&[book, outcome]).inc();
        }
        for (book, bps) in &self.errors_bps {
            PENDING_BOOK_SGA_MARK_ERROR_BPS.with_label_values(&[book]).observe(*bps);
        }
        for outcome in &self.buckets {
            PENDING_BOOK_SGA_BUCKET_TOTAL.with_label_values(&[outcome]).inc();
        }
        for blocks in &self.behind_blocks {
            #[expect(clippy::cast_precision_loss, reason = "a count of blocks")]
            PENDING_BOOK_SGA_PRICES_BEHIND_BLOCKS.observe(*blocks as f64);
        }
        self
    }

    fn both(&mut self, outcome: &'static str) {
        self.marks.extend(BOOKS.map(|book| (book, outcome)));
    }
}

impl SgaForecasts {
    /// The account view's node prices as they now stand: the forecasts whose block they reach are checked; a reason
    /// keeps none.
    pub(crate) fn on_prices(&mut self, prices: &NodePrices, now_ns: i64) -> ForecastReport {
        let mut report = ForecastReport::default();
        self.expire(now_ns, &mut report);
        let Ok(prices) = prices else {
            self.prices = None;
            return report.counted();
        };
        self.prices = Some(Arc::clone(prices));
        let (reached, waiting): (VecDeque<Forecast>, VecDeque<Forecast>) =
            std::mem::take(&mut self.in_flight).into_iter().partition(|forecast| forecast.block.is_some_and(|(height, _)| height <= prices.height));
        self.in_flight = waiting;
        for forecast in reached {
            self.check(&forecast, prices, &mut report);
        }
        report.counted()
    }

    /// A mempool line: its `SetGlobalAction`'s marks forecast, its asset ids named by `coins`, on `book` while the
    /// pending book may be served.
    pub(crate) fn on_tx(&mut self, tx: &MempoolTx, coins: Option<&ActionCoins>, book: Option<&dyn ForecastBook>, now_ns: i64) -> ForecastReport {
        let mut report = ForecastReport::default();
        let Some(submitted) = tx.actions.iter().find_map(|action| match &action.kind {
            ActionKind::SetGlobal(submitted) => Some(submitted),
            _ => None,
        }) else {
            return report;
        };
        self.expire(now_ns, &mut report);
        if self.in_flight.iter().any(|forecast| forecast.tx_hash == tx.tx_hash) {
            return report.counted();
        }
        let outcome = match (coins, &self.prices, book) {
            (None, _, _) => "no_asset_table",
            (_, None, _) => "no_prices",
            (_, _, None) => "unavailable",
            (_, _, Some(book)) if book.logged(&tx.tx_hash) => "late",
            (Some(coins), Some(prices), Some(book)) => {
                let started = Instant::now();
                let forecast = forecast(tx, submitted, coins, prices, book, &mut report);
                PENDING_BOOK_STEP_SECONDS.with_label_values(&["sga_forecast"]).observe(started.elapsed().as_secs_f64());
                self.track(forecast, &mut report);
                "forecast"
            }
        };
        report.lines.push(outcome);
        report.counted()
    }

    /// A block of the log: the forecast of a line it holds learns its block, and whether its line time told the block's
    /// reference cache bucket.
    pub(crate) fn on_block(&mut self, block: &LoggedBlock) -> ForecastReport {
        let mut report = ForecastReport::default();
        for forecast in self.in_flight.iter_mut().filter(|forecast| forecast.block.is_none() && block.hashes.contains(&forecast.tx_hash)) {
            forecast.block = Some((block.height, block.time_ms));
            let same = reference_bucket(nonnegative(forecast.seen_ns)) == reference_bucket(block.time_ms * 1_000_000);
            report.buckets.push(if same { "same" } else { "different" });
        }
        report.counted()
    }

    /// The marks the merged book forecast for the line `tx_hash` in flight - (asset, coin, mark), `None` where it gave
    /// none - in asset order: what its triggers fire on (batch 2l second step).
    pub(crate) fn merged_marks(&self, tx_hash: &B256) -> Vec<(u32, String, Option<u64>)> {
        let forecast = self.in_flight.iter().find(|forecast| forecast.tx_hash == *tx_hash);
        let coins = forecast.into_iter().flat_map(|forecast| &forecast.coins);
        coins.map(|coin| (coin.asset, coin.coin.clone(), coin.books[0].map(|book| book.mark))).collect()
    }

    fn track(&mut self, forecast: Forecast, report: &mut ForecastReport) {
        self.in_flight.push_back(forecast);
        if self.in_flight.len() > MAX_IN_FLIGHT {
            self.in_flight.pop_front();
            report.lines.push("expired");
        }
    }

    fn expire(&mut self, now_ns: i64, report: &mut ForecastReport) {
        let before = self.in_flight.len();
        self.in_flight.retain(|forecast| now_ns - forecast.seen_ns <= EXPIRY_NS);
        report.lines.extend(std::iter::repeat_n("expired", before - self.in_flight.len()));
    }

    /// A forecast whose block the node prices reach: each coin's marks against the one stored.
    fn check(&mut self, forecast: &Forecast, prices: &NativePriceSnapshot, report: &mut ForecastReport) {
        let (height, time_ms) = forecast.block.expect("checked once its block is known");
        if forecast.prices_height >= height {
            report.lines.push("late");
            return;
        }
        report.lines.push("checked");
        report.behind_blocks.push(height - forecast.prices_height);
        for coin in &forecast.coins {
            let state = prices.coins.get(&coin.coin);
            for (book, forecast_book) in BOOKS.into_iter().zip(&coin.books) {
                let Some(forecast_book) = forecast_book else { continue };
                let outcome = outcome(state, forecast_book.mark, time_ms);
                report.marks.push((book, outcome));
                let (Some(state), "off") = (state, outcome) else { continue };
                report.errors_bps.push((book, error_bps(forecast_book.mark, state.stored_mark)));
                if throttled_log_due(&mut self.last_off_log, Instant::now()) {
                    info!(
                        "pending book: a SetGlobalAction's forecast mark off: {} block {height} ({book} book) forecast {} stored {} - \
                         submitted {}, bid {} ask {}, last trade {}, Mark cache (median, bucket) {:?} on node prices {}, line bucket {}, \
                         block bucket {}",
                        coin.coin,
                        forecast_book.mark,
                        state.stored_mark,
                        coin.submitted,
                        forecast_book.bid,
                        forecast_book.ask,
                        coin.last_trade,
                        coin.cache,
                        forecast.prices_height,
                        reference_bucket(nonnegative(forecast.seen_ns)),
                        reference_bucket(time_ms * 1_000_000),
                    );
                }
            }
        }
    }
}

/// The marks of a `SetGlobalAction` submitting `submitted` per main-dex asset, on `prices` and `book`'s best prices, its
/// block time taken as its line's.
fn forecast(
    tx: &MempoolTx,
    submitted: &[SubmittedPrices],
    coins: &ActionCoins,
    prices: &NativePriceSnapshot,
    book: &dyn ForecastBook,
    report: &mut ForecastReport,
) -> Forecast {
    let block_ns = nonnegative(tx.seen_ns);
    let coins = submitted
        .iter()
        .enumerate()
        .filter_map(|(asset, submitted)| match submitted {
            SubmittedPrices::Priced { mark, .. } => Some((asset, mark)),
            SubmittedPrices::Unpriced | SubmittedPrices::Partial => None,
        })
        .filter_map(|(asset, mark)| {
            let asset = u32::try_from(asset).ok()?;
            let named = coins.coins.get(&asset);
            let Some((name, state)) = named.and_then(|named| prices.coins.get_key_value(&named.coin)) else {
                report.both("no_state");
                return None;
            };
            let Ok(submitted) = wire_units(mark, state.size_decimals) else {
                report.both("unreadable");
                return None;
            };
            let books = <[BestLevels; 2]>::from(book.best(&Coin::new(name), tx.seen_ns)).map(|best| book_forecast(state, submitted, block_ns, best));
            let books = [0, 1].map(|index| books[index].inspect_err(|outcome| report.marks.push((BOOKS[index], outcome))).ok());
            let cache = state.mark_cache.as_ref().map(|cache| (cache.median, cache.last_bucket));
            Some(CoinForecast { asset, coin: name.clone(), submitted, last_trade: state.last_trade, cache, books })
        })
        .collect();
    Forecast { tx_hash: tx.tx_hash, seen_ns: tx.seen_ns, prices_height: prices.height, coins, block: None }
}

/// A coin's forecast on one book's best levels, or why there is none.
fn book_forecast(state: &CoinPriceState, submitted: u64, block_ns: u64, best: BestLevels) -> Result<BookForecast, &'static str> {
    let (Some((bid, _, _)), Some((ask, _, _))) = best else { return Err("no_book") };
    let (Some(bid), Some(ask)) = (price_units(bid, state.size_decimals), price_units(ask, state.size_decimals)) else { return Err("unit_mismatch") };
    let mark = state.set_global_mark(submitted, block_ns, bid, ask).ok_or("unselected")?;
    Ok(BookForecast { bid, ask, mark })
}

/// A coin's stored mark against its forecast, the block at `time_ms` holding the `SetGlobalAction`.
fn outcome(state: Option<&CoinPriceState>, mark: u64, time_ms: u64) -> &'static str {
    let Some(state) = state else { return "unclaimed" };
    match (state.mark_time_ns / 1_000_000).cmp(&time_ms) {
        Ordering::Greater => "superseded",
        Ordering::Less => "not_set",
        Ordering::Equal if state.mark_update != MarkUpdate::SetGlobal => "not_set",
        Ordering::Equal if state.stored_mark == mark => "exact",
        Ordering::Equal => "off",
    }
}

/// A book price (1e-8 units) in a perp's price units (10^-(6 - szDecimals)); `None` when it is not a whole number of them.
fn price_units(px: Px, size_decimals: u32) -> Option<u64> {
    let step = 10_u64.pow(2 + size_decimals);
    px.value().is_multiple_of(step).then(|| px.value() / step)
}

#[expect(clippy::cast_precision_loss, reason = "prices are below 2^53")]
fn error_bps(forecast: u64, stored: u64) -> f64 {
    forecast.abs_diff(stored) as f64 / stored as f64 * 1e4
}

/// A line time, unix ns, as the marks' block times are.
fn nonnegative(ns: i64) -> u64 {
    u64::try_from(ns).expect("a node time after 1970")
}

#[cfg(test)]
mod tests {
    use rustc_hash::FxHashMap;

    use super::*;
    use crate::{
        accounts::marks::seed::ReferenceMode,
        local_marks::compute::tests::btc_state,
        order_book::Sz,
        pending::{actions::tests::coins, blocks::tests::hash, mempool::SignedAction, overlay::BestLevel},
    };

    /// 2026-10-19T06:37:25.4 UTC, a line's time: its 3 s bucket started 1.4 s before, the next starts 1.6 s on.
    const SEEN_NS: i64 = 1_792_391_845_400_000_000;
    const SEEN_MS: u64 = 1_792_391_845_400;

    /// A best level at `units` of BTC's price units (0.1; 1e-8 book units of 10^7).
    fn level(units: u64) -> Option<BestLevel> {
        Some((Px::new(units * 10_000_000), Sz::new(1), 1))
    }

    /// BTC's book: merged (999,000 / 999,100), confirmed (1,000,000 / 1,000,400) unless set.
    struct Book {
        merged: BestLevels,
        confirmed: BestLevels,
        logged: bool,
    }

    impl Default for Book {
        fn default() -> Self {
            Self { merged: (level(999_000), level(999_100)), confirmed: (level(1_000_000), level(1_000_400)), logged: false }
        }
    }

    impl ForecastBook for Book {
        fn best(&self, coin: &Coin, _: i64) -> (BestLevels, BestLevels) {
            assert_eq!(coin, &Coin::new("BTC"));
            (self.merged, self.confirmed)
        }

        fn logged(&self, _: &B256) -> bool {
            self.logged
        }
    }

    /// BTC's node prices a second before the line: Mark cache 1,000,100 (its one member then), last trade 1,000,300.
    fn btc() -> CoinPriceState {
        btc_state(0.0, nonnegative(SEEN_NS) - 1_000_000_000)
    }

    fn prices(height: u64, btc: CoinPriceState) -> NodePrices {
        let coins = FxHashMap::from_iter([("BTC".to_string(), btc)]);
        Ok(Arc::new(NativePriceSnapshot { seed_height: 1, height, block_time_ms: SEEN_MS, coins, hip3: FxHashMap::default() }))
    }

    /// BTC as the `SetGlobalAction` of `SEEN_MS + after_ms`'s block stored it.
    fn stored(mark: u64, after_ms: u64) -> CoinPriceState {
        CoinPriceState { stored_mark: mark, mark_time_ns: (SEEN_MS + after_ms) * 1_000_000, mark_update: MarkUpdate::SetGlobal, ..btc() }
    }

    /// A `SetGlobalAction` line (hash `byte`) seen `after_ms` after `SEEN_NS`, submitting BTC's mark `mark`, ETH not.
    fn line(byte: u8, after_ms: i64, mark: &str) -> MempoolTx {
        let btc = SubmittedPrices::Priced { oracle: "100000.0".to_string(), mark: mark.to_string() };
        let kind = ActionKind::SetGlobal(vec![btc, SubmittedPrices::Unpriced]);
        MempoolTx {
            seen_ns: SEEN_NS + after_ms * 1_000_000,
            tx_hash: hash(byte),
            actions: vec![SignedAction { vault: None, signer: None, expires_after_ms: None, priority: 0, kind }],
        }
    }

    fn block(height: u64, after_ms: u64, bytes: &[u8]) -> LoggedBlock {
        LoggedBlock {
            height,
            time_ms: SEEN_MS + after_ms,
            hashes: bytes.iter().map(|byte| hash(*byte)).collect(),
            priorities: Vec::new(),
            taker_outcomes: Vec::new(),
            twap_cancels: Vec::new(),
            read_at: std::time::Instant::now(),
        }
    }

    /// Forecasts on node prices of height 10 (BTC as `btc`), the line of hash 1 forecast on `book`.
    fn forecast_one(btc: CoinPriceState, book: &Book, mark: &str) -> (SgaForecasts, ForecastReport) {
        let mut forecasts = SgaForecasts::default();
        assert_eq!(forecasts.on_prices(&prices(10, btc), SEEN_NS), ForecastReport::default());
        let report = forecasts.on_tx(&line(1, 0, mark), Some(&coins()), Some(book), SEEN_NS);
        (forecasts, report)
    }

    fn lines(outcomes: &[&'static str]) -> ForecastReport {
        ForecastReport { lines: outcomes.to_vec(), ..ForecastReport::default() }
    }

    /// Submitted 1,000,200, cache 1,000,100: the merged book's L 999,100 leaves the cache's median the mark, the
    /// confirmed book's L 1,000,300 the submission. The block stores the submission: the merged book's forecast is off by
    /// 1 bp; the check waits for node prices holding the block, three blocks on from those forecast on.
    #[test]
    fn a_forecast_is_checked_against_the_mark_its_block_stores() {
        let (mut forecasts, report) = forecast_one(btc(), &Book::default(), "100020.0");
        assert_eq!(report, lines(&["forecast"]));
        let report = forecasts.on_block(&block(13, 5, &[7, 1]));
        assert_eq!(report.buckets, ["same"]);
        assert_eq!(forecasts.on_prices(&prices(12, btc()), SEEN_NS), ForecastReport::default(), "below its block");
        let report = forecasts.on_prices(&prices(13, stored(1_000_200, 5)), SEEN_NS);
        assert_eq!((report.lines, report.marks, report.behind_blocks), (vec!["checked"], vec![("pending", "off"), ("confirmed", "exact")], vec![3]));
        let [("pending", bps)] = report.errors_bps.as_slice() else { panic!("{:?}", report.errors_bps) };
        assert!((bps - 0.99_98).abs() < 1e-3, "{bps}");
        assert!(forecasts.in_flight.is_empty());
    }

    /// The block refreshes the Mark cache when it is the first of a newer bucket: the forecast does when its line time
    /// is - the cache's one member a second old is the median then -, and the log tells when the two buckets differ.
    #[test]
    fn the_cache_refreshes_by_the_line_time_bucket_and_the_block_tells_it() {
        let mut btc = btc();
        btc.mark_cache.as_mut().unwrap().members[0].1 = 999_500;
        let (mut forecasts, _) = forecast_one(btc.clone(), &Book::default(), "100020.0");
        assert_eq!(forecasts.in_flight[0].coins[0].books.map(|book| book.unwrap().mark), [1_000_100, 1_000_200], "the line's bucket: no refresh");
        btc.mark_cache.as_mut().unwrap().last_bucket -= 1;
        let (mut refreshed, _) = forecast_one(btc, &Book::default(), "100020.0");
        assert_eq!(refreshed.in_flight[0].coins[0].books.map(|book| book.unwrap().mark), [999_500, 1_000_200], "refreshed to the member");
        assert_eq!(forecasts.on_block(&block(13, 1_600, &[1])).buckets, ["different"], "the next bucket starts 1.6 s on");
        assert_eq!(refreshed.on_block(&block(13, 1_599, &[1])).buckets, ["same"]);
    }

    /// A line forecasts nothing without the asset table, the node prices, a book that may be served, or once its block
    /// is logged; again only once.
    #[test]
    fn a_line_without_its_inputs_forecasts_nothing() {
        let mut forecasts = SgaForecasts::default();
        let tx = line(1, 0, "100020.0");
        let book = Book::default();
        assert_eq!(forecasts.on_tx(&tx, Some(&coins()), Some(&book), SEEN_NS), lines(&["no_prices"]));
        drop(forecasts.on_prices(&prices(10, btc()), SEEN_NS));
        assert_eq!(forecasts.on_tx(&tx, None, Some(&book), SEEN_NS), lines(&["no_asset_table"]));
        assert_eq!(forecasts.on_tx(&tx, Some(&coins()), None, SEEN_NS), lines(&["unavailable"]));
        let logged = Book { logged: true, ..Book::default() };
        assert_eq!(forecasts.on_tx(&tx, Some(&coins()), Some(&logged), SEEN_NS), lines(&["late"]));
        assert_eq!(forecasts.on_tx(&tx, Some(&coins()), Some(&book), SEEN_NS).lines, ["forecast"]);
        assert_eq!(forecasts.on_tx(&tx, Some(&coins()), Some(&book), SEEN_NS), ForecastReport::default(), "the same line again");
        assert_eq!(forecasts.on_prices(&Err("the account view is unavailable".to_string()), SEEN_NS), ForecastReport::default());
        assert_eq!(forecasts.on_tx(&line(2, 0, "100020.0"), Some(&coins()), Some(&book), SEEN_NS), lines(&["no_prices"]), "a reason keeps no prices");
        let no_set_global = MempoolTx { actions: Vec::new(), ..line(3, 0, "1") };
        assert_eq!(forecasts.on_tx(&no_set_global, Some(&coins()), Some(&book), SEEN_NS), ForecastReport::default());
    }

    /// A coin's forecast on a book with an empty side, a price off its units, no Mark cache median, its node prices or
    /// its submission unreadable: none, counted once made.
    #[test]
    fn a_coin_without_its_inputs_is_counted_on_each_book() {
        let half_unit = Some((Px::new(9_990_005_000_000), Sz::new(1), 1));
        let book = Book { merged: (level(999_000), None), confirmed: (half_unit, level(1_000_400)), logged: false };
        assert_eq!(forecast_one(btc(), &book, "100020.0").1.marks, [("pending", "no_book"), ("confirmed", "unit_mismatch")]);
        let mut no_median = btc();
        no_median.mark_cache = None;
        assert_eq!(forecast_one(no_median, &Book::default(), "100020.0").1.marks, [("pending", "unselected"), ("confirmed", "unselected")]);
        let mut submitted = btc();
        submitted.reference_modes.mark = ReferenceMode::Submitted;
        submitted.mark_cache = None;
        let (forecasts, report) = forecast_one(submitted, &Book::default(), "100020.0");
        assert_eq!((report.marks.len(), forecasts.in_flight[0].coins[0].books.map(|book| book.unwrap().mark)), (0, [1_000_200; 2]));
        assert_eq!(forecast_one(btc(), &Book::default(), "1e5").1.marks, [("pending", "unreadable"), ("confirmed", "unreadable")]);
        let mut forecasts = SgaForecasts::default();
        drop(forecasts.on_prices(&prices(10, btc()), SEEN_NS));
        let mut eth = line(1, 0, "100020.0");
        let ActionKind::SetGlobal(submitted) = &mut eth.actions[0].kind else { unreachable!() };
        submitted.swap(0, 1);
        assert_eq!(forecasts.on_tx(&eth, Some(&coins()), Some(&Book::default()), SEEN_NS).marks, [("pending", "no_state"), ("confirmed", "no_state")]);
    }

    /// Checked: a later mark stored, none stored by the block, the coin gone from the node prices; node prices that held
    /// the block already when the forecast read them make it late.
    #[test]
    fn a_checked_forecast_tells_what_its_block_stored() {
        let unclaimed = Ok(Arc::new(NativePriceSnapshot { coins: FxHashMap::default(), ..(*prices(13, btc()).unwrap()).clone() }));
        for (case, checked_on, outcome) in [
            ("a later SetGlobalAction", prices(13, stored(1_000_200, 3_005)), "superseded"),
            ("an earlier one's", prices(13, stored(1_000_200, 0)), "not_set"),
            ("an automatic refresh in the block", prices(13, CoinPriceState { mark_update: MarkUpdate::AutomaticRefresh, ..stored(1_000_200, 5) }), "not_set"),
            ("unsupported since", unclaimed, "unclaimed"),
        ] {
            let (mut forecasts, _) = forecast_one(btc(), &Book::default(), "100020.0");
            drop(forecasts.on_block(&block(13, 5, &[1])));
            assert_eq!(forecasts.on_prices(&checked_on, SEEN_NS).marks, [("pending", outcome), ("confirmed", outcome)], "{case}");
        }
        let (mut forecasts, _) = forecast_one(btc(), &Book::default(), "100020.0");
        drop(forecasts.on_block(&block(10, 5, &[1])));
        assert_eq!(forecasts.on_prices(&prices(10, stored(1_000_200, 5)), SEEN_NS), lines(&["late"]), "read node prices of its block");
    }

    /// Unchecked 30 s after its line, or pushed out by eight newer ones: expired.
    #[test]
    fn a_forecast_unchecked_expires() {
        let (mut forecasts, _) = forecast_one(btc(), &Book::default(), "100020.0");
        assert_eq!(forecasts.on_prices(&prices(11, btc()), SEEN_NS + EXPIRY_NS), ForecastReport::default());
        assert_eq!(forecasts.on_prices(&prices(11, btc()), SEEN_NS + EXPIRY_NS + 1), lines(&["expired"]));
        let book = Book::default();
        for byte in 1..=8 {
            assert_eq!(forecasts.on_tx(&line(byte, 0, "100020.0"), Some(&coins()), Some(&book), SEEN_NS).lines, ["forecast"]);
        }
        assert_eq!(forecasts.on_tx(&line(9, 0, "100020.0"), Some(&coins()), Some(&book), SEEN_NS).lines, ["expired", "forecast"]);
        assert_eq!(forecasts.in_flight.front().map(|forecast| forecast.tx_hash), Some(hash(2)));
    }

    #[test]
    fn a_book_price_is_a_whole_number_of_price_units() {
        assert_eq!(price_units(Px::new(10_000_000_000_000), 5), Some(1_000_000));
        assert_eq!(price_units(Px::new(10_000_005_000_000), 5), None);
        assert_eq!(price_units(Px::new(123_400_000), 0), Some(1_234_000), "1.234 in units of 1e-6");
        assert_eq!(price_units(Px::new(123_400_001), 0), None);
    }
}
