//! `GET /extMarkInputs` (design 000454 batch 1): every feed's connection, and per coin each listing's latest quote as
//! the venue wrote it with its multiplier, whether it holds now, and which venues price the oracle and the external
//! price. For checking the feeds; the marks themselves come in batch 2. With the HIP-3 equity feeds (design 000535 r2
//! revision 1, 000543 §5), a `sip` and a `boats` section: each feed's connection and each HIP-3 coin's latest quote;
//! the main-dex part is unchanged and, without the feeds, so is the whole answer.

use std::collections::BTreeMap;

use serde::Serialize;

use super::{
    feeds::{EquityReadings, LocalMarkFeeds},
    hip3::{protocol::EquityFeed, sources::Hip3SipSources},
    slot::{Connection, Entry, Validity},
    subscriptions::{ConnectionProjection, FeedProjection},
    snapshot::Coverage,
    sources::{Feed, FeedMarkets, Kind},
};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InputsView {
    time_ms: u64,
    /// Why every feed stopped (a task's internal failure) until restart; `null` while they run.
    stopped: Option<String>,
    feeds: Vec<FeedView>,
    coins: Vec<CoinInputs>,
    /// The two HIP-3 equity feeds; absent without them.
    #[serde(skip_serializing_if = "Option::is_none")]
    sip: Option<EquityInputs>,
    #[serde(skip_serializing_if = "Option::is_none")]
    boats: Option<EquityInputs>,
}

/// A HIP-3 equity feed and its coins' quotes (all, or the queried HIP-3 coin's).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EquityInputs {
    feed: FeedView,
    coins: Vec<EquityCoinInputs>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EquityCoinInputs {
    coin: String,
    quote: ListingView,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FeedView {
    feed: &'static str,
    connection: ConnectionView,
    connections: Vec<ShardView>,
    frames: u64,
    markets: usize,
    /// Markets whose quote holds now.
    quoting: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ShardView {
    shard: usize,
    markets: usize,
    connection: ConnectionView,
    frames: u64,
    quoting: usize,
}

#[derive(Serialize)]
#[serde(tag = "state", rename_all = "camelCase", rename_all_fields = "camelCase")]
enum ConnectionView {
    /// A partitioned feed is not fully connected; per-partition reasons remain in `connections`.
    Degraded { connected: usize, total: usize },
    Connecting,
    Connected { since_ms: u64 },
    Disconnected { reason: String, at_ms: u64 },
    Stopped { reason: String, at_ms: u64 },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CoinInputs {
    coin: String,
    spot: BTreeMap<&'static str, ListingView>,
    perp: BTreeMap<&'static str, ListingView>,
    oracle_coverage: Coverage,
    external_coverage: Coverage,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ListingView {
    symbol: String,
    px_multiplier: String,
    /// The venue's own prices, as it wrote them; `null` without a two-sided quote.
    bid: Option<String>,
    ask: Option<String>,
    received_ts_ms: Option<u64>,
    event_ts_ms: Option<u64>,
    /// `valid`, `notQuoted` (nothing on the current connection yet), `noQuote` (no two-sided book), `notConnected`,
    /// `expired` (a polled quote too old).
    status: &'static str,
}

const fn status(validity: Validity) -> &'static str {
    match validity {
        Validity::Valid => "valid",
        Validity::NotQuoted => "notQuoted",
        Validity::NoQuote => "noQuote",
        Validity::NotConnected => "notConnected",
        Validity::Expired => "expired",
    }
}

fn connection(connection: &Connection) -> ConnectionView {
    match connection {
        Connection::Connecting => ConnectionView::Connecting,
        Connection::Connected { since_ms } => ConnectionView::Connected { since_ms: *since_ms },
        Connection::Disconnected { reason, at_ms } => ConnectionView::Disconnected { reason: reason.clone(), at_ms: *at_ms },
        Connection::Stopped { reason, at_ms } => ConnectionView::Stopped { reason: reason.clone(), at_ms: *at_ms },
    }
}

fn feed_connection(connections: &[ConnectionProjection]) -> ConnectionView {
    if connections.len() == 1 { return connection(&connections[0].connection); }
    let (connected, latest) = connections.iter().fold((0, None::<u64>), |(count, latest), part| {
        match part.connection {
            Connection::Connected { since_ms } => (count + 1, Some(latest.map_or(since_ms, |old| old.max(since_ms)))),
            _ => (count, latest),
        }
    });
    if connected == connections.len() && let Some(since_ms) = latest {
        return ConnectionView::Connected { since_ms };
    }
    ConnectionView::Degraded { connected, total: connections.len() }
}

fn feed_view(feed: Feed, markets: &FeedMarkets, reading: &FeedProjection) -> FeedView {
    FeedView {
        feed: feed.label(),
        connection: feed_connection(&reading.connections),
        connections: reading.connections.iter().map(|part| ShardView {
            shard: part.shard, markets: part.markets, connection: connection(&part.connection), frames: part.frames, quoting: part.quoting,
        }).collect(),
        frames: reading.messages,
        markets: markets.symbols.len(),
        quoting: reading.entries.iter().filter(|(_, validity)| *validity == Validity::Valid).count(),
    }
}

/// The view now; `coin` narrows the coins (the feeds are always all): a main-dex coin's listings, or a HIP-3 coin's
/// quotes. `Err` for a coin in neither source.
pub(crate) fn inputs_view(feeds: &LocalMarkFeeds, coin: Option<&str>, now_ms: u64) -> Result<InputsView, String> {
    let readings = feeds.readings(now_ms);
    let by_feed: BTreeMap<Feed, (&FeedMarkets, &FeedProjection)> = readings.iter().map(|(feed, markets, reading)| (*feed, (*markets, reading))).collect();
    let hip3 = feeds.hip3.as_deref();
    let is_hip3 = |coin: &str| hip3.is_some_and(|sources| sources.markets.coins.iter().any(|(listed, _)| listed == coin));
    let (coins, equity_coins): (Vec<&str>, Option<Vec<&str>>) = match coin {
        Some(coin) if feeds.sources.listings(coin).is_some() => (vec![coin], None),
        Some(coin) if is_hip3(coin) => (Vec::new(), Some(vec![coin])),
        Some(coin) => return Err(format!("{coin} is not in the local marks' sources")),
        None => (feeds.sources.coins().collect(), hip3.map(|sources| sources.coins().collect())),
    };
    let feed_views = readings.iter().map(|(feed, markets, reading)| feed_view(*feed, markets, reading)).collect();
    let coins = coins.into_iter().map(|coin| coin_inputs(feeds, &by_feed, coin)).collect();
    let equity = equity_coins.map(|equity_coins| {
        let sources = hip3.unwrap_or_else(|| unreachable!("equity coins come from the HIP-3 sources"));
        let readings = feeds.equity_readings(now_ms).unwrap_or_else(|| unreachable!("the HIP-3 sources run the equity feeds"));
        (equity_inputs(EquityFeed::Sip, sources, &readings, &equity_coins), equity_inputs(EquityFeed::Boats, sources, &readings, &equity_coins))
    });
    let (sip, boats) = equity.unzip();
    Ok(InputsView { time_ms: now_ms, stopped: feeds.stopped(), feeds: feed_views, coins, sip, boats })
}

fn equity_inputs(feed: EquityFeed, sources: &Hip3SipSources, readings: &EquityReadings, coins: &[&str]) -> EquityInputs {
    let (markets, reading) = (&sources.markets, readings.of(feed));
    let coins = coins.iter().map(|coin| {
        let index = markets.coins.iter().position(|(listed, _)| listed == coin).unwrap_or_else(|| unreachable!("a HIP-3 coin"));
        let (entry, validity) = reading.entries[index];
        EquityCoinInputs { coin: (*coin).to_string(), quote: listing_view(&markets.symbols[index], "1".to_string(), entry, validity) }
    }).collect();
    EquityInputs { feed: feed_view(feed.feed(), markets, reading), coins }
}

fn listing_view(symbol: &str, px_multiplier: String, entry: Option<Entry>, validity: Validity) -> ListingView {
    let quote = entry.and_then(|entry| entry.quote);
    ListingView {
        symbol: symbol.to_string(),
        px_multiplier,
        bid: quote.map(|quote| quote.bid.to_string()),
        ask: quote.map(|quote| quote.ask.to_string()),
        received_ts_ms: entry.map(|entry| entry.received_ms),
        event_ts_ms: entry.and_then(|entry| entry.event_ms),
        status: status(validity),
    }
}

fn coin_inputs(feeds: &LocalMarkFeeds, by_feed: &BTreeMap<Feed, (&FeedMarkets, &FeedProjection)>, coin: &str) -> CoinInputs {
    let listings = feeds.sources.listings(coin).unwrap_or_else(|| unreachable!("a coin of the sources"));
    let mut views: BTreeMap<Kind, BTreeMap<&'static str, ListingView>> = BTreeMap::new();
    let mut coverage: BTreeMap<Kind, Coverage> = BTreeMap::new();
    for (feed, listing) in listings {
        let (markets, reading) = by_feed[feed];
        let index = markets.index(&listing.symbol).unwrap_or_else(|| unreachable!("a listing of the feed's markets"));
        let (entry, validity) = reading.entries[index];
        views.entry(feed.kind).or_default().insert(feed.venue.name(), listing_view(&listing.symbol, listing.px_multiplier.to_string(), entry, validity));
        coverage.entry(feed.kind).or_default().add(*feed, validity);
    }
    let mut take = |kind| coverage.remove(&kind).unwrap_or_else(|| unreachable!("every coin has a spot and a perp listing"));
    CoinInputs {
        coin: coin.to_string(),
        oracle_coverage: take(Kind::Spot),
        external_coverage: take(Kind::Perp),
        spot: views.remove(&Kind::Spot).unwrap_or_default(),
        perp: views.remove(&Kind::Perp).unwrap_or_default(),
    }
}
