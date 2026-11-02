//! The HIP-3 coins' external quotes at a tick (hyperliquid-trade design 000664 §2, 000669 r1–r3 §1): each coin of the
//! channel (`Hip3ExternalCoins`: Binance's table and the SIP table, united) has two legs, each `unlisted` where its
//! table lists no such coin.
//! - `external`: Binance's `TRADIFI_PERPETUAL` contract as its external bid and ask - one venue, so its quote itself,
//!   exact, the listing's multiplier applied, off HL's price grid, as the main dex's `ExternalQuotes` - and its normal
//!   basis to them, the main dex's rule (`external_basis`: HL's book midpoint over theirs - 1, a sample a second, the
//!   median of 10 minutes, published from 120 samples);
//! - `equity`: the US stock's quote of the SIP or BOATS, whichever counts now (`external_equity`), and its basis so.
//!
//! The Binance feed's partitions are copied after the tick's other reads and judged at a publish time taken after the
//! copy (design 000664 r4): no quote a snapshot holds was received after its `publishedTsMs`, and a coin's quote older
//! than the feed's age limit then is withheld (`expired`), as one not quoted on its connection, no two-sided quote or
//! its connection down. The US stock feeds' readings are the tick's, read before that copy. A sample is taken only from
//! a quote valid then and HL's book usable (read, trusted, not behind) at the publish time; the book behind withholds
//! no quote - it is not one of their inputs - but takes no sample.
//!
//! The external quote tick (`quote_tick`, hyperliquid-trade design 000673) judges each leg's quote by the same rules
//! (`binance_quote`, `external_equity::session_quote`) at its own publish time, and takes the bases the owner publishes
//! here (`Hip3ExternalBases`): its windows are moved by the owner only.

use std::sync::Arc;

use super::{
    external_coins::{BinanceListing, Hip3ExternalCoins},
    external_equity::{EquityBases, EquityBasisWindows, EquityPlace, EquityTick, Hip3EquityLeg, equity_leg},
    session::equity_session,
    snapshot::Hip3Table,
};
use crate::{
    local_marks::{
        external_basis::{self, ExternalBasis, ExternalBasisWindow},
        price::VenuePrice,
        slot::{Entry, Validity},
        snapshot::{ExternalLeg, ExternalQuotes, Tick, VenueQuote, Withheld},
        sources::Feed,
        subscriptions::FeedProjection,
    },
    order_book::Px,
};

/// The Binance feed's reading of one snapshot: its partitions copied, then judged at `published_ms`, a wall-clock
/// time taken after the copy - the snapshot's `publishedTsMs` -, by the feed's age limit.
pub(crate) struct Hip3ExternalReading {
    pub(crate) published_ms: u64,
    pub(crate) quote_max_age_ms: u64,
    pub(crate) projection: FeedProjection,
}

/// The channel's coins, in their order, with their HL book's place in the tick's book read (`CoinTable::book_coins`)
/// and their place among the SIP feeds' markets.
#[derive(Debug, Default)]
pub(crate) struct Hip3ExternalTable {
    coins: Arc<Hip3ExternalCoins>,
    book_indices: Vec<usize>,
    equity: Vec<EquityPlace>,
}

impl Hip3ExternalTable {
    /// `sip_feeds`: the SIP feeds' coins, `None` without them; `book_index`: the coin's place in the book read, given
    /// or added there by the caller (each coin read once).
    pub(crate) fn new(coins: Option<&Arc<Hip3ExternalCoins>>, sip_feeds: Option<&Hip3Table>, mut book_index: impl FnMut(&str) -> usize) -> Self {
        let coins = coins.map_or_else(Arc::default, Arc::clone);
        let book_indices = coins.names().iter().map(|coin| book_index(coin)).collect();
        let equity = coins.names().iter().enumerate().map(|(index, coin)| equity_place(coin, coins.sip_listed(index), sip_feeds)).collect();
        Self { coins, book_indices, equity }
    }

    pub(crate) fn names(&self) -> &[String] {
        self.coins.names()
    }

    /// The coin's place in the channel (`Hip3ExternalCoins::index`); `None` outside it.
    pub(crate) fn index(&self, coin: &str) -> Option<usize> {
        self.coins.index(coin)
    }

    /// The coin's Binance contract; `None` where Binance's table lists none.
    pub(crate) fn binance(&self, index: usize) -> Option<BinanceListing> {
        self.coins.binance(index)
    }

    /// The coin's place among the SIP feeds' markets.
    pub(crate) fn equity_place(&self, index: usize) -> EquityPlace {
        self.equity[index]
    }

    /// Each coin's place in the book read.
    #[cfg(test)]
    pub(crate) fn book_indices(&self) -> &[usize] {
        &self.book_indices
    }

    /// Each coin's place among the SIP feeds' markets.
    #[cfg(test)]
    pub(crate) fn equity_places(&self) -> &[EquityPlace] {
        &self.equity
    }
}

/// A SIP coin's market among the SIP feeds', when they run: the feeds and the channel read one SIP table (loaded once
/// at start).
fn equity_place(coin: &str, sip_listed: bool, sip_feeds: Option<&Hip3Table>) -> EquityPlace {
    match (sip_listed, sip_feeds) {
        (false, _) => EquityPlace::Unlisted,
        (true, None) => EquityPlace::Off,
        (true, Some(feeds)) => EquityPlace::Read(feeds.index(coin).unwrap_or_else(|| unreachable!("the SIP feeds read the channel's SIP table: {coin}"))),
    }
}

/// A coin's leg of one source: not in that source's table, or the leg or why it is withheld.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Hip3SourceLeg<T> {
    /// The source's table lists no such coin: never a quote, not an unknown one.
    Unlisted,
    Listed(Result<T, Withheld>),
}

/// A coin's two legs: `external`, Binance's quote and its basis; `equity`, the SIP's or BOATS's.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Hip3ExternalCoin {
    pub(crate) external: Hip3SourceLeg<ExternalLeg>,
    pub(crate) equity: Hip3SourceLeg<Hip3EquityLeg>,
}

/// A coin's bases as its windows publish them at the owner's tick, whatever its quotes and the feeds' stop: Binance's
/// where Binance lists it, each US stock feed's where the SIP feeds read it - the external quote tick's, which judges
/// its own quotes and samples nothing (hyperliquid-trade design 000673 r4).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Hip3ExternalBases {
    pub(crate) binance: Option<Result<ExternalBasis, Withheld>>,
    pub(crate) equity: Option<EquityBases>,
}

/// One snapshot's legs of every coin, in the table's order.
#[derive(Debug, Clone)]
pub(crate) struct Hip3ExternalSnapshot {
    pub(crate) published_ms: u64,
    /// The Binance feed's age limit, its frames' `quoteMaxAgeMs`.
    pub(crate) quote_max_age_ms: u64,
    pub(crate) coins: Vec<Hip3ExternalCoin>,
    /// Each coin's bases, in the table's order.
    pub(crate) bases: Vec<Hip3ExternalBases>,
}

/// A coin's basis windows across ticks: Binance's, and one a US stock feed.
#[derive(Debug, Clone, Default)]
pub(crate) struct Hip3ExternalWindows {
    external: ExternalBasisWindow,
    equity: EquityBasisWindows,
}

/// What gates every coin at this tick: the feeds stopped or the account owner gone (every quote withheld), and the
/// HL book's usability at the publish time (samples only).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Hip3ExternalGates<'a> {
    pub(crate) stopped: Option<&'a Withheld>,
    pub(crate) book_usable: bool,
}

/// Every coin's legs. First each leg's basis windows move, with this tick's sample when its quote and the book allow
/// one.
pub(crate) fn hip3_external(
    table: &Hip3ExternalTable,
    tick: &Tick<'_>,
    reading: &Hip3ExternalReading,
    gates: Hip3ExternalGates<'_>,
    windows: &mut [Hip3ExternalWindows],
) -> Hip3ExternalSnapshot {
    assert_eq!(windows.len(), table.names().len(), "one set of basis windows per HIP-3 external coin");
    assert_eq!(reading.projection.entries.len(), table.coins.binance_markets(), "one Binance market per coin it lists");
    let published_ms = reading.published_ms;
    let equity_tick = EquityTick { readings: tick.equity, published_ms, session: equity_session(published_ms), instant: tick.instant };
    let (coins, bases) = windows
        .iter_mut()
        .enumerate()
        .map(|(index, windows)| {
            let coin = &table.names()[index];
            let book = sampled_book(tick, gates, table.book_indices[index]);
            let external = external_leg(coin, table.coins.binance(index), tick, reading, gates.stopped, book, &mut windows.external);
            let equity = equity_leg(coin, table.equity[index], equity_tick, gates.stopped, book, &mut windows.equity);
            let bases = Hip3ExternalBases {
                binance: table.coins.binance(index).map(|_| windows.external.published()),
                equity: matches!(table.equity[index], EquityPlace::Read(_)).then(|| windows.equity.published()),
            };
            (Hip3ExternalCoin { external, equity }, bases)
        })
        .unzip();
    Hip3ExternalSnapshot { published_ms, quote_max_age_ms: reading.quote_max_age_ms, coins, bases }
}

/// HL's two sides of the coin when this tick may sample them: the feeds running, the book usable at the publish time,
/// neither side empty.
fn sampled_book(tick: &Tick<'_>, gates: Hip3ExternalGates<'_>, book_index: usize) -> Option<(Px, Px)> {
    if gates.stopped.is_some() || !gates.book_usable {
        return None;
    }
    let (bid, ask) = tick.book.unwrap_or_else(|| unreachable!("a usable book was read")).prices[book_index];
    bid.zip(ask)
}

/// The coin's Binance leg. First its window moves, with this tick's sample when its quote is valid and `book` is
/// given; `stopped` withholds the leg after.
fn external_leg(
    coin: &str,
    listing: Option<BinanceListing>,
    tick: &Tick<'_>,
    reading: &Hip3ExternalReading,
    stopped: Option<&Withheld>,
    book: Option<(Px, Px)>,
    window: &mut ExternalBasisWindow,
) -> Hip3SourceLeg<ExternalLeg> {
    let Some(listing) = listing else { return Hip3SourceLeg::Unlisted };
    let quotes = binance_quote(coin, listing, reading.projection.entries[listing.market], reading.published_ms);
    let sample = book.zip(quotes.as_ref().ok()).map(|((bid, ask), quotes)| external_basis::sample(bid, ask, quotes.bid, quotes.ask));
    window.observe(tick.instant, sample);
    if let Some(stopped) = stopped {
        return Hip3SourceLeg::Listed(Err(stopped.clone()));
    }
    Hip3SourceLeg::Listed(quotes.map(|quotes| ExternalLeg { quotes, basis: window.published() }))
}

/// The coin's Binance quote valid at `published_ms`, its multiplier applied, from its market's entry and validity
/// judged then (`observed`); why not: the owner's and the external quote tick's one rule.
pub(crate) fn binance_quote(coin: &str, listing: BinanceListing, observed: (Option<Entry>, Validity), published_ms: u64) -> Result<ExternalQuotes, Withheld> {
    let (entry, validity) = observed;
    if validity != Validity::Valid {
        let received = entry.map_or_else(|| "none received".to_string(), |entry| format!("the last received at {} ms", entry.received_ms));
        let detail = format!("the {} quote of {coin} is {} at {published_ms} ms ({received})", Feed::HIP3_EXTERNAL.label(), validity.name());
        return Err(Withheld::unavailable(validity.name(), detail));
    }
    let entry = entry.unwrap_or_else(|| unreachable!("a valid entry holds a quote"));
    received_by(Feed::HIP3_EXTERNAL.label(), coin, entry.received_ms, published_ms)?;
    let quote = entry.quote.unwrap_or_else(|| unreachable!("a valid entry holds a quote"));
    let multiplier = listing.multiplier;
    let scaled = |px: VenuePrice| {
        px.scaled(multiplier).ok_or_else(|| Withheld::unavailable("calculation", format!("{coin}: {} price {px} × {multiplier} is out of range", Feed::HIP3_EXTERNAL.label())))
    };
    let venue = VenueQuote { feed: Feed::HIP3_EXTERNAL, bid: scaled(quote.bid)?, ask: scaled(quote.ask)?, received_ms: entry.received_ms };
    Ok(ExternalQuotes::of(vec![venue]).unwrap_or_else(|| unreachable!("one quote has medians")))
}

/// A quote received after the publish time - the wall clock stepped back between the tick's reads - is no quote as of
/// it: neither leg publishes a `receivedTsMs` after `publishedTsMs` (design 000669 r3 §2.1), the leg withheld this tick,
/// its basis not sampled, the other leg judged alone (review 000672 P2). The external quote tick holds each main-dex
/// venue's quote so too (hyperliquid-trade design 000673).
pub(crate) fn received_by(feed: &str, coin: &str, received_ms: u64, published_ms: u64) -> Result<(), Withheld> {
    if received_ms <= published_ms {
        return Ok(());
    }
    let detail = format!("the {feed} quote of {coin} was received at {received_ms} ms, after the publish time {published_ms} ms: the wall clock stepped back");
    Err(Withheld::unavailable("receivedAfterPublish", detail))
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
            hip3::{external_sources::Hip3ExternalSources, sources::Hip3SipSources},
            slot::{FeedSlot, Quote},
            subscriptions::FeedSubscriptions,
        },
        order_book::Px,
    };

    const SOURCES: &str = r#"{"venue": "binance_perp", "coins": {"xyz:GOLD": {"symbol": "XAUUSDT", "pxMultiplier": "1"},
        "xyz:ZHIPU": {"symbol": "ZHIPUUSDT", "pxMultiplier": "10"}}}"#;
    /// TSLA the SIP's only: the channel is GOLD, TSLA, ZHIPU.
    const SIP: &str = r#"{"feed": "sip", "coins": {"xyz:TSLA": {"symbol": "TSLA"}}}"#;
    const GOLD: usize = 0;
    const TSLA: usize = 1;
    const ZHIPU: usize = 2;

    fn price(text: &str) -> VenuePrice {
        VenuePrice::parse(text).unwrap()
    }

    fn quote(bid: &str, ask: &str) -> Quote {
        Quote { bid: price(bid), ask: price(ask) }
    }

    /// A book of every coin at `px` (1e-8) each side.
    fn book(bid: u64, ask: u64) -> BestPricesAt {
        BestPricesAt { time: 0, height: 1, trusted: true, prices: vec![(Some(Px::new(bid)), Some(Px::new(ask))); 3] }
    }

    /// The coin's Binance leg, listed.
    fn external(coin: &Hip3ExternalCoin) -> &Result<ExternalLeg, Withheld> {
        let Hip3SourceLeg::Listed(leg) = &coin.external else { panic!("unlisted: {coin:?}") };
        leg
    }

    struct World {
        table: Hip3ExternalTable,
        feed: FeedSubscriptions,
        windows: Vec<Hip3ExternalWindows>,
    }

    impl World {
        fn new() -> Self {
            let sources = Hip3ExternalSources::parse(SOURCES).unwrap();
            let coins = Arc::new(Hip3ExternalCoins::new(&sources, &Hip3SipSources::parse(SIP).unwrap()));
            let feed = FeedSubscriptions::with_expiry(Feed::HIP3_EXTERNAL, &sources.markets, Some(1_000));
            let mut next = 0;
            let table = Hip3ExternalTable::new(Some(&coins), None, |_| {
                next += 1;
                next - 1
            });
            Self { table, feed, windows: vec![Hip3ExternalWindows::default(); 3] }
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
    /// Design 000669 r1 1.1: TSLA, the SIP's only, has no Binance leg (`unlisted`) and, the SIP feeds off, its US stock
    /// leg `equityOff`; GOLD has no US stock leg.
    #[test]
    fn each_coin_quotes_alone_and_a_silent_one_expires() {
        let mut world = World::new();
        let epoch = world.slot().connected(0);
        world.slot().record(epoch, 1_000, [(0, Some(quote("3900", "3901")), Some(990))]);
        let book = book(390_050_000_000, 390_060_000_000);
        let ready = world.tick(Instant::now(), 1_500, Some(&book), None);
        let gold = external(&ready.coins[GOLD]).as_ref().unwrap();
        assert_eq!((gold.quotes.bid, gold.quotes.ask, gold.quotes.venues[0].received_ms), (price("3900"), price("3901"), 1_000));
        assert_eq!(gold.basis.as_ref().unwrap_err().code, "externalBasisSamples");
        assert_eq!(ready.coins[GOLD].equity, Hip3SourceLeg::Unlisted);
        assert_eq!(external(&ready.coins[ZHIPU]).as_ref().unwrap_err().code, "notQuoted", "ZHIPU never quoted on the connection");
        assert_eq!(ready.coins[TSLA].external, Hip3SourceLeg::Unlisted);
        let Hip3SourceLeg::Listed(Err(off)) = &ready.coins[TSLA].equity else { panic!("{:?}", ready.coins[TSLA]) };
        assert_eq!(off.code, "equityOff");
        world.slot().record(epoch, 1_900, [(1, Some(quote("6.1", "6.2")), None)]);
        let later = world.tick(Instant::now(), 2_001, Some(&book), None);
        let expired = external(&later.coins[GOLD]).as_ref().unwrap_err();
        assert_eq!(expired.code, "expired", "GOLD silent 1,001 ms, over the 1,000 ms limit");
        assert!(expired.detail.contains("the last received at 1000 ms"), "{}", expired.detail);
        let zhipu = external(&later.coins[ZHIPU]).as_ref().unwrap();
        assert_eq!((zhipu.quotes.bid, zhipu.quotes.ask), (price("61"), price("62")), "× 10");
        world.slot().disconnected("EOF".to_string(), 2_100);
        assert_eq!(external(&world.tick(Instant::now(), 2_200, Some(&book), None).coins[ZHIPU]).as_ref().unwrap_err().code, "notConnected");
        let epoch = world.slot().connected(2_300);
        let reconnected = world.tick(Instant::now(), 2_400, Some(&book), None);
        assert_eq!(external(&reconnected.coins[ZHIPU]).as_ref().unwrap_err().code, "notQuoted", "reconnected, no first quote yet");
        world.slot().record(epoch, 2_500, [(1, None, None)]);
        assert_eq!(external(&world.tick(Instant::now(), 2_600, Some(&book), None).coins[ZHIPU]).as_ref().unwrap_err().code, "noQuote");
    }

    /// Review 000672 P2: a quote received after the publish time - the wall clock stepped back - is withheld, its basis
    /// not sampled; received at the publish time it is the leg.
    #[test]
    fn a_quote_received_after_the_publish_time_is_withheld() {
        let mut world = World::new();
        let epoch = world.slot().connected(0);
        world.slot().record(epoch, 1_000, [(0, Some(quote("3900", "3901")), Some(990))]);
        let book = book(390_050_000_000, 390_060_000_000);
        // a sampling interval apart: a sample the first tick took would show as a second one (review 000672 r2 P3)
        let start = Instant::now();
        let stepped_back = world.tick(start, 999, Some(&book), None);
        let withheld = external(&stepped_back.coins[GOLD]).as_ref().unwrap_err();
        assert_eq!(withheld.code, "receivedAfterPublish", "{}", withheld.detail);
        let at_publish = world.tick(start + SAMPLE_INTERVAL, 1_000, Some(&book), None);
        let gold = external(&at_publish.coins[GOLD]).as_ref().unwrap();
        assert_eq!(gold.quotes.venues[0].received_ms, 1_000);
        assert_eq!(gold.basis.as_ref().unwrap_err().detail, "1 of the 120 samples needed over the last 600 s", "the withheld tick sampled nothing");
    }

    /// Review 000664 r1 P2-1: the basis is published from the 120th valid sample, not the 119th; a tick without a
    /// usable book or a valid quote takes none; the feeds stopped withhold every listed leg, an unlisted one stays so.
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
        let detail = &external(&withheld.coins[GOLD]).as_ref().unwrap().basis.as_ref().unwrap_err().detail;
        assert!(detail.starts_with("119 of the 120 samples"), "{detail}");
        assert!(external(&withheld.coins[ZHIPU]).is_err());
        let published = second_tick(&mut world, below);
        let (at, now_ms) = (start + SAMPLE_INTERVAL * below, 10_000 + 1_000 * u64::from(below));
        let basis = external(&published.coins[GOLD]).as_ref().unwrap().basis.clone().unwrap();
        assert_eq!(basis.samples, MIN_SAMPLES);
        let expected = 3900.55 / 3900.5 - 1.0;
        assert!((basis.ratio - expected).abs() < 1e-12, "{} against {expected}", basis.ratio);
        let stopped = Withheld::unavailable("stopped", "the perp_binance task failed");
        let gone = world.tick(at + SAMPLE_INTERVAL, now_ms + 1_000, Some(&book), Some(&stopped));
        assert!([GOLD, ZHIPU].iter().all(|index| external(&gone.coins[*index]).as_ref().unwrap_err().code == "stopped"));
        assert_eq!(gone.coins[TSLA].external, Hip3SourceLeg::Unlisted);
    }
}
