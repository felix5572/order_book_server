//! The HIP-3 coins' external quotes at a tick (hyperliquid-trade design 000664 §2): each coin of the external sources
//! takes Binance's `TRADIFI_PERPETUAL` contract as its external bid and ask - one venue, so its quote itself, exact,
//! the listing's multiplier applied, off HL's price grid, as the main dex's `ExternalQuotes` - and its normal basis to
//! them, the main dex's rule (`external_basis`: HL's book midpoint over theirs - 1, a sample a second, the median of 10
//! minutes, published from 120 samples).
//!
//! The feed's partitions are copied after the tick's other reads and judged at a publish time taken after the copy
//! (design 000664 r4): no quote a snapshot holds was received after its `publishedTsMs`, and a coin's quote older than
//! the feed's age limit then is withheld (`expired`), as one not quoted on its connection, no two-sided quote or its
//! connection down. A sample is taken only from a quote valid then and HL's book usable (read, trusted, not behind);
//! the book behind withholds no quote - it is not one of their inputs - but takes no sample.

use rustc_hash::FxHashMap;

use super::external_sources::Hip3ExternalSources;
use crate::local_marks::{
    external_basis::{self, ExternalBasis, ExternalBasisWindow},
    price::VenuePrice,
    slot::Validity,
    snapshot::{ExternalQuotes, Tick, VenueQuote, Withheld},
    sources::Feed,
    subscriptions::FeedProjection,
};

/// The external feed's reading of one snapshot: its partitions copied, then judged at `published_ms`, a wall-clock
/// time taken after the copy - the snapshot's `publishedTsMs` -, by the feed's age limit.
pub(crate) struct Hip3ExternalReading {
    pub(crate) published_ms: u64,
    pub(crate) quote_max_age_ms: u64,
    pub(crate) projection: FeedProjection,
}

/// The external sources' coins, in their order (each one's market index is its own), with their HL book's place in the
/// tick's book read (`CoinTable::book_coins`).
#[derive(Debug, Default)]
pub(crate) struct Hip3ExternalTable {
    names: Vec<String>,
    by_name: FxHashMap<String, usize>,
    multipliers: Vec<VenuePrice>,
    book_indices: Vec<usize>,
}

impl Hip3ExternalTable {
    /// `book_index`: the coin's place in the book read, given or added there by the caller (each coin read once).
    pub(crate) fn new(sources: Option<&Hip3ExternalSources>, mut book_index: impl FnMut(&str) -> usize) -> Self {
        let coins = sources.map_or(&[][..], |sources| &sources.markets.coins);
        let names: Vec<String> = coins.iter().map(|(coin, _)| coin.clone()).collect();
        let by_name = names.iter().enumerate().map(|(index, coin)| (coin.clone(), index)).collect();
        let multipliers = coins.iter().map(|(_, multiplier)| *multiplier).collect();
        let book_indices = names.iter().map(|coin| book_index(coin)).collect();
        Self { names, by_name, multipliers, book_indices }
    }

    pub(crate) fn names(&self) -> &[String] {
        &self.names
    }

    pub(crate) fn index(&self, coin: &str) -> Option<usize> {
        self.by_name.get(coin).copied()
    }

    /// Each coin's place in the book read.
    #[cfg(test)]
    pub(crate) fn book_indices(&self) -> &[usize] {
        &self.book_indices
    }
}

/// A coin's external quote and its normal basis to it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Hip3ExternalLeg {
    pub(crate) quotes: ExternalQuotes,
    /// Withheld below its minimum samples.
    pub(crate) basis: Result<ExternalBasis, Withheld>,
}

/// One snapshot's external quotes of every coin, in the table's order.
#[derive(Debug, Clone)]
pub(crate) struct Hip3ExternalSnapshot {
    pub(crate) published_ms: u64,
    /// The feed's age limit, its frames' `quoteMaxAgeMs`.
    pub(crate) quote_max_age_ms: u64,
    pub(crate) coins: Vec<Result<Hip3ExternalLeg, Withheld>>,
}

/// What gates every coin at this tick: the feeds stopped or the account owner gone (every quote withheld), and the
/// HL book's usability (samples only).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Hip3ExternalGates<'a> {
    pub(crate) stopped: Option<&'a Withheld>,
    pub(crate) book_usable: bool,
}

/// Every coin's leg. First each coin's basis window moves, with this tick's sample when its quote and book allow one.
pub(crate) fn hip3_external(
    table: &Hip3ExternalTable,
    tick: &Tick<'_>,
    reading: &Hip3ExternalReading,
    gates: Hip3ExternalGates<'_>,
    windows: &mut [ExternalBasisWindow],
) -> Hip3ExternalSnapshot {
    assert_eq!(windows.len(), table.names.len(), "one basis window per HIP-3 external coin");
    assert_eq!(reading.projection.entries.len(), table.names.len(), "one market per HIP-3 external coin");
    let coins = windows
        .iter_mut()
        .enumerate()
        .map(|(index, window)| {
            let quotes = coin_quotes(table, reading, index);
            let sample = gates.stopped.is_none() && gates.book_usable;
            window.observe(tick.instant, sample.then(|| basis_sample(table, tick, index, &quotes)).flatten());
            if let Some(stopped) = gates.stopped {
                return Err(stopped.clone());
            }
            quotes.map(|quotes| Hip3ExternalLeg { quotes, basis: window.published() })
        })
        .collect();
    Hip3ExternalSnapshot { published_ms: reading.published_ms, quote_max_age_ms: reading.quote_max_age_ms, coins }
}

/// The coin's quote valid at the publish time, its multiplier applied; why not.
fn coin_quotes(table: &Hip3ExternalTable, reading: &Hip3ExternalReading, index: usize) -> Result<ExternalQuotes, Withheld> {
    let coin = &table.names[index];
    let (entry, validity) = reading.projection.entries[index];
    if validity != Validity::Valid {
        let received = entry.map_or_else(|| "none received".to_string(), |entry| format!("the last received at {} ms", entry.received_ms));
        let detail = format!("the {} quote of {coin} is {} at {} ms ({received})", Feed::HIP3_EXTERNAL.label(), validity.name(), reading.published_ms);
        return Err(Withheld::unavailable(validity.name(), detail));
    }
    let entry = entry.unwrap_or_else(|| unreachable!("a valid entry holds a quote"));
    let quote = entry.quote.unwrap_or_else(|| unreachable!("a valid entry holds a quote"));
    let multiplier = table.multipliers[index];
    let scaled = |px: VenuePrice| {
        px.scaled(multiplier).ok_or_else(|| Withheld::unavailable("calculation", format!("{coin}: {} price {px} × {multiplier} is out of range", Feed::HIP3_EXTERNAL.label())))
    };
    let venue = VenueQuote { feed: Feed::HIP3_EXTERNAL, bid: scaled(quote.bid)?, ask: scaled(quote.ask)?, received_ms: entry.received_ms };
    Ok(ExternalQuotes::of(vec![venue]).unwrap_or_else(|| unreachable!("one quote has medians")))
}

/// This tick's basis sample of the coin: its HL book's two sides against its external quote; `None` without either.
fn basis_sample(table: &Hip3ExternalTable, tick: &Tick<'_>, index: usize, quotes: &Result<ExternalQuotes, Withheld>) -> Option<f64> {
    let book = tick.book.unwrap_or_else(|| unreachable!("a usable book was read"));
    let (Some(bid), Some(ask)) = book.prices[table.book_indices[index]] else {
        return None;
    };
    quotes.as_ref().ok().map(|quotes| external_basis::sample(bid, ask, quotes))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::time::Instant;

    use super::*;
    use crate::{
        listeners::order_book::BestPricesAt,
        local_marks::{
            external_basis::{MIN_SAMPLES, SAMPLE_INTERVAL},
            slot::{FeedSlot, Quote},
            subscriptions::FeedSubscriptions,
        },
        order_book::Px,
    };

    const SOURCES: &str = r#"{"venue": "binance_perp", "coins": {"xyz:GOLD": {"symbol": "XAUUSDT", "pxMultiplier": "1"},
        "xyz:ZHIPU": {"symbol": "ZHIPUUSDT", "pxMultiplier": "10"}}}"#;

    fn price(text: &str) -> VenuePrice {
        VenuePrice::parse(text).unwrap()
    }

    fn quote(bid: &str, ask: &str) -> Quote {
        Quote { bid: price(bid), ask: price(ask) }
    }

    fn table(sources: &Hip3ExternalSources) -> Hip3ExternalTable {
        let mut next = 0;
        Hip3ExternalTable::new(Some(sources), |_| {
            next += 1;
            next - 1
        })
    }

    /// A book of both coins at `px` (1e-8) each side.
    fn book(bid: u64, ask: u64) -> BestPricesAt {
        BestPricesAt { time: 0, height: 1, trusted: true, prices: vec![(Some(Px::new(bid)), Some(Px::new(ask))); 2] }
    }

    struct World {
        table: Hip3ExternalTable,
        feed: FeedSubscriptions,
        windows: Vec<ExternalBasisWindow>,
    }

    impl World {
        fn new() -> Self {
            let sources = Hip3ExternalSources::parse(SOURCES).unwrap();
            let feed = FeedSubscriptions::with_expiry(Feed::HIP3_EXTERNAL, &sources.markets, Some(1_000));
            Self { table: table(&sources), feed, windows: vec![ExternalBasisWindow::default(); 2] }
        }

        fn slot(&self) -> &Arc<FeedSlot> {
            &self.feed.subscriptions[0].slot
        }

        fn tick(&mut self, instant: Instant, published_ms: u64, book: Option<&BestPricesAt>, stopped: Option<&Withheld>) -> Hip3ExternalSnapshot {
            let reading = Hip3ExternalReading { published_ms, quote_max_age_ms: 1_000, projection: self.feed.copy().projection(published_ms) };
            let tick = Tick { now_ms: published_ms, instant, stopped: None, node: &Err(String::new()), book, readings: &[], equity: None, hip3_external: None };
            let gates = Hip3ExternalGates { stopped, book_usable: book.is_some() };
            hip3_external(&self.table, &tick, &reading, gates, &mut self.windows)
        }
    }

    /// Design 000664 §2.3: a coin's quote is Binance's own, scaled by its multiplier; a coin silent past the age limit
    /// while its partition's other coin quotes is withheld alone (`expired`), as one never quoted on the connection.
    #[test]
    fn each_coin_quotes_alone_and_a_silent_one_expires() {
        let mut world = World::new();
        let epoch = world.slot().connected(0);
        world.slot().record(epoch, 1_000, [(0, Some(quote("3900", "3901")), Some(990))]);
        let book = book(390_050_000_000, 390_060_000_000);
        let ready = world.tick(Instant::now(), 1_500, Some(&book), None);
        let gold = ready.coins[0].as_ref().unwrap();
        assert_eq!((gold.quotes.bid, gold.quotes.ask, gold.quotes.venues[0].received_ms), (price("3900"), price("3901"), 1_000));
        assert_eq!(gold.basis.as_ref().unwrap_err().code, "externalBasisSamples");
        assert_eq!(ready.coins[1].as_ref().unwrap_err().code, "notQuoted", "ZHIPU never quoted on the connection");
        world.slot().record(epoch, 1_900, [(1, Some(quote("6.1", "6.2")), None)]);
        let later = world.tick(Instant::now(), 2_001, Some(&book), None);
        let expired = later.coins[0].as_ref().unwrap_err();
        assert_eq!(expired.code, "expired", "GOLD silent 1,001 ms, over the 1,000 ms limit");
        assert!(expired.detail.contains("the last received at 1000 ms"), "{}", expired.detail);
        let zhipu = later.coins[1].as_ref().unwrap();
        assert_eq!((zhipu.quotes.bid, zhipu.quotes.ask), (price("61"), price("62")), "× 10");
        world.slot().disconnected("EOF".to_string(), 2_100);
        assert_eq!(world.tick(Instant::now(), 2_200, Some(&book), None).coins[1].as_ref().unwrap_err().code, "notConnected");
        let epoch = world.slot().connected(2_300);
        assert_eq!(world.tick(Instant::now(), 2_400, Some(&book), None).coins[1].as_ref().unwrap_err().code, "notQuoted", "reconnected, no first quote yet");
        world.slot().record(epoch, 2_500, [(1, None, None)]);
        assert_eq!(world.tick(Instant::now(), 2_600, Some(&book), None).coins[1].as_ref().unwrap_err().code, "noQuote");
    }

    /// Review 000664 r1 P2-1: the basis is published from the 120th valid sample, not the 119th; a tick without a
    /// usable book or a valid quote takes none; the feeds stopped withhold every coin.
    #[test]
    fn the_basis_publishes_from_the_minimum_samples_and_samples_only_valid_inputs() {
        let mut world = World::new();
        let epoch = world.slot().connected(0);
        let book = book(390_050_000_000, 390_060_000_000);
        let start = Instant::now();
        // the second `second`'s tick, its quote just received; ticks without a book, and the other coin never quoted,
        // sample nothing
        let second_tick = |world: &mut World, second: u32| {
            let (at, now_ms) = (start + SAMPLE_INTERVAL * second, 10_000 + 1_000 * u64::from(second));
            world.slot().record(epoch, now_ms, [(0, Some(quote("3900", "3901")), None)]);
            world.tick(at + SAMPLE_INTERVAL / 2, now_ms + 500, None, None);
            world.tick(at, now_ms, Some(&book), None)
        };
        let below = u32::try_from(MIN_SAMPLES - 1).unwrap();
        let withheld = (0..below).map(|second| second_tick(&mut world, second)).last().unwrap();
        let detail = &withheld.coins[0].as_ref().unwrap().basis.as_ref().unwrap_err().detail;
        assert!(detail.starts_with("119 of the 120 samples"), "{detail}");
        assert!(withheld.coins[1].is_err());
        let published = second_tick(&mut world, below);
        let (at, now_ms) = (start + SAMPLE_INTERVAL * below, 10_000 + 1_000 * u64::from(below));
        let basis = published.coins[0].as_ref().unwrap().basis.clone().unwrap();
        assert_eq!(basis.samples, MIN_SAMPLES);
        let expected = 3900.55 / 3900.5 - 1.0;
        assert!((basis.ratio - expected).abs() < 1e-12, "{} against {expected}", basis.ratio);
        let stopped = Withheld::unavailable("stopped", "the perp_binance task failed");
        let gone = world.tick(at + SAMPLE_INTERVAL, now_ms + 1_000, Some(&book), Some(&stopped));
        assert!(gone.coins.iter().all(|coin| coin.as_ref().unwrap_err().code == "stopped"));
    }
}
