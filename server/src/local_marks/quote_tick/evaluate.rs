//! One external quote tick's reading and legs (hyperliquid-trade design 000673 r4 读数): the partitions of the
//! subscribed coins' quotes, each copied once, then judged at one publish time taken after every copy - as the owner's
//! HIP-3 reading (`owner::hip3_external_reading`): a quote received during the copies is the next tick's, never judged
//! from the future -, and each coin's legs by the owner's own rules (`CoinTable::perp_quotes`, `binance_quote`,
//! `session_quote`, `received_by`) with the owner's bases, read only.

use std::time::Duration;

use rustc_hash::{FxHashMap, FxHashSet};
use tokio::time::Instant;

use super::snapshot::{QuoteChannel, QuoteKey};
use crate::local_marks::{
    external_basis::{ExternalBasis, LongerBases},
    feeds::LocalMarkFeeds,
    hip3::{
        external::{Hip3BinanceLeg, Hip3ExternalBases, Hip3ExternalCoin, Hip3ExternalTable, Hip3SourceLeg, binance_quote},
        external_equity::{EquityPlace, Hip3EquityLeg, equity_market, session_quote},
        protocol::EquityFeed,
        session::equity_session,
    },
    slot::{Entry, FeedReading, Validity},
    snapshot::{CoinTable, ExternalLeg, LocalMarksSnapshot, MainExternalLeg, Withheld},
    subscriptions::FeedSubscriptions,
};

/// The oldest basis of the owner's the tick publishes (design 000673 r4 r3 P1): past it the owner has stalled - waiting
/// for the listener lock, say -, and a heartbeat must not carry its last basis as fresh. 10 s (hyperliquid-trade
/// 2026-11-04, user: "基差这种东西快点慢点咋了"): a basis is the median of the last 10 minutes' samples, one a second
/// (published from 120 of them), 10 s late its window moved by 10 samples; at four of the owner's 50 ms ticks (200 ms)
/// its waits for the lock (202-204 ms) withheld every coin's Binance leg 6 times in 14 minutes, and with it every HIP-3
/// quote of the trader's (its dryrun 11-04 14:02-14:16).
pub(crate) const BASIS_MAX_AGE: Duration = Duration::from_secs(10);

/// A feed whose partitions the tick copies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[expect(variant_size_differences, reason = "a small key, copied; a main-dex feed's place is the larger")]
enum FeedRef {
    /// A main-dex feed, by its place in the readings (`LocalMarkFeeds::readings`).
    Main(usize),
    Hip3Binance,
    Equity(EquityFeed),
}

impl FeedRef {
    fn subscriptions(self, feeds: &LocalMarkFeeds) -> &FeedSubscriptions {
        match self {
            Self::Main(reading) => feeds.main_feed(reading),
            Self::Hip3Binance => feeds.hip3_external_feed().unwrap_or_else(|| unreachable!("a coin with a Binance contract runs with its feed")),
            Self::Equity(feed) => feeds.equity_feed(feed).unwrap_or_else(|| unreachable!("a coin read among the SIP feeds' markets runs with them")),
        }
    }
}

/// The subscribed coins' partitions, judged at the tick's publish time.
pub(crate) struct QuoteReading<'f> {
    feeds: &'f LocalMarkFeeds,
    /// The publish time (wall clock, ms), taken after every copy: the frames' `publishedTsMs`.
    pub(crate) published_ms: u64,
    /// The same moment on the monotonic clock, the owner's bases' age is judged at.
    pub(crate) instant: Instant,
    parts: FxHashMap<(FeedRef, usize), FeedReading>,
}

impl<'f> QuoteReading<'f> {
    /// Copies each partition of `coins`' quotes once, then takes the publish time from `clock` and judges every copy
    /// at it.
    pub(crate) fn take(feeds: &'f LocalMarkFeeds, table: &CoinTable, coins: impl Iterator<Item = QuoteKey>, clock: impl FnOnce() -> (u64, Instant)) -> Self {
        let mut wanted = FxHashSet::default();
        for coin in coins {
            wanted.extend(markets(table, coin).into_iter().map(|(feed, market)| (feed, feed.subscriptions(feeds).part_of(market).0.shard)));
        }
        #[expect(clippy::needless_collect, reason = "every copy is taken before the publish time")]
        let copies: Vec<_> = wanted.into_iter().map(|(feed, shard)| ((feed, shard), feed.subscriptions(feeds).subscriptions[shard].slot.copy())).collect();
        let (published_ms, instant) = clock();
        let parts = copies.into_iter().map(|(part, copy)| (part, copy.reading(published_ms))).collect();
        Self { feeds, published_ms, instant, parts }
    }

    /// The market's entry and validity at the publish time; its partition was copied (`take`).
    fn observed(&self, feed: FeedRef, market: usize) -> (Option<Entry>, Validity) {
        let (part, index) = feed.subscriptions(self.feeds).part_of(market);
        self.parts[&(feed, part.shard)].entries[index]
    }
}

/// The feeds and markets a coin's legs read.
fn markets(table: &CoinTable, coin: QuoteKey) -> Vec<(FeedRef, usize)> {
    match coin.channel {
        QuoteChannel::Main => table.perp_markets(coin.index).map(|(reading, market)| (FeedRef::Main(reading), market)).collect(),
        QuoteChannel::Hip3 => {
            let external = table.hip3_external();
            let binance = external.binance(coin.index).map(|listing| (FeedRef::Hip3Binance, listing.market));
            // both feeds: the session that picks one is the publish time's, taken after the copies
            let equity = match external.equity_place(coin.index) {
                EquityPlace::Read(market) => EquityFeed::ALL.map(|feed| (FeedRef::Equity(feed), market)).to_vec(),
                EquityPlace::Unlisted | EquityPlace::Off => Vec::new(),
            };
            binance.into_iter().chain(equity).collect()
        }
    }
}

/// A main-dex coin's external leg at the tick: its perp quotes valid at the publish time and their medians, each
/// received by then - else the leg is withheld (`ExternalQuotes::received_by`), as the owner's and a HIP-3 leg are -,
/// the owner's basis to the medians and each quoting venue's own (hyperliquid-trade design 000681), each judged by its
/// age alike.
pub(crate) fn main_leg(table: &CoinTable, index: usize, reading: &QuoteReading<'_>, owner: &LocalMarksSnapshot) -> Result<MainExternalLeg, Withheld> {
    let coin = &table.names()[index];
    let quotes = table.perp_quotes(index, |feed, market| reading.observed(FeedRef::Main(feed), market))?.received_by(coin, reading.published_ms)?;
    let marks = owner.coin(coin).unwrap_or_else(|| unreachable!("{coin} is of the owner's table"));
    let taken = |basis: &Result<ExternalBasis, Withheld>| owner_basis(basis, owner, reading.instant);
    Ok(MainExternalLeg::of(quotes, marks.external_basis.clone(), &marks.venue_bases, taken))
}

/// A HIP-3 coin's two legs at the tick, each with the owner's basis of its source.
pub(crate) fn hip3_legs(table: &Hip3ExternalTable, index: usize, reading: &QuoteReading<'_>, owner: &LocalMarksSnapshot) -> Hip3ExternalCoin {
    let coin = &table.names()[index];
    let external = owner.hip3_external().unwrap_or_else(|| unreachable!("a HIP-3 coin evaluated runs with the external quotes"));
    let bases = &external.bases[index];
    let binance = table.binance(index).map_or(Hip3SourceLeg::Unlisted, |listing| {
        let quotes = binance_quote(coin, listing, reading.observed(FeedRef::Hip3Binance, listing.market), reading.published_ms);
        let binance = bases.binance.as_ref().unwrap_or_else(|| unreachable!("the owner publishes the bases of a coin Binance lists"));
        let taken = |basis| owner_basis(basis, owner, reading.instant);
        let longer = LongerBases { hour: taken(&binance.longer.hour), four_hours: taken(&binance.longer.four_hours) };
        Hip3SourceLeg::Listed(quotes.map(|quotes| Hip3BinanceLeg { leg: ExternalLeg { quotes, basis: taken(&binance.near) }, longer }))
    });
    Hip3ExternalCoin { external: binance, equity: equity_leg(coin, table.equity_place(index), reading, bases, owner) }
}

/// The US stock leg: the quote of the feed of the relayer's session at the publish time, and the owner's basis of that
/// feed - both feeds' published for every coin the SIP feeds read, a cold one withheld below its samples, so a quote
/// never takes another feed's window (review 000673 r2).
fn equity_leg(coin: &str, place: EquityPlace, reading: &QuoteReading<'_>, bases: &Hip3ExternalBases, owner: &LocalMarksSnapshot) -> Hip3SourceLeg<Hip3EquityLeg> {
    let market = match equity_market(coin, place) {
        Ok(market) => market,
        Err(leg) => return leg,
    };
    let published_ms = reading.published_ms;
    let quote = session_quote(coin, published_ms, equity_session(published_ms), |feed| reading.observed(FeedRef::Equity(feed), market));
    let feed_bases = bases.equity.as_ref().unwrap_or_else(|| unreachable!("the owner publishes both feeds' bases of a coin the SIP feeds read"));
    Hip3SourceLeg::Listed(quote.map(|quote| Hip3EquityLeg { quote, basis: owner_basis(feed_bases.of(quote.feed), owner, reading.instant) }))
}

/// The owner's `basis` as the tick publishes it at `instant`: withheld (`basisStale`) once more than `BASIS_MAX_AGE`
/// after the owner computed it (`LocalMarksSnapshot::computed_at`), until the owner publishes anew.
fn owner_basis(basis: &Result<ExternalBasis, Withheld>, owner: &LocalMarksSnapshot, instant: Instant) -> Result<ExternalBasis, Withheld> {
    let age = instant.saturating_duration_since(owner.computed_at);
    if age > BASIS_MAX_AGE {
        let detail = format!("the local marks owner computed the basis {} ms ago, over the {} ms the quote tick takes", age.as_millis(), BASIS_MAX_AGE.as_millis());
        return Err(Withheld::unavailable("basisStale", detail));
    }
    basis.clone()
}
