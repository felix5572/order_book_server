//! `GET /extMarkInputs` (design 000454 batch 1): every feed's connection, and per coin each listing's latest quote as
//! the venue wrote it with its multiplier, whether it holds now, and which venues price the oracle and the external
//! price. For checking the feeds; the marks themselves come in batch 2.

use std::collections::BTreeMap;

use serde::Serialize;

use super::{
    feeds::LocalMarkFeeds,
    slot::{Connection, Validity},
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

/// The view now; `coin` narrows the coins (the feeds are always all). `Err` for a coin outside the sources.
pub(crate) fn inputs_view(feeds: &LocalMarkFeeds, coin: Option<&str>, now_ms: u64) -> Result<InputsView, String> {
    let readings = feeds.readings(now_ms);
    let by_feed: BTreeMap<Feed, (&FeedMarkets, &FeedProjection)> = readings.iter().map(|(feed, markets, reading)| (*feed, (*markets, reading))).collect();
    let coins: Vec<&str> = match coin {
        Some(coin) if feeds.sources.listings(coin).is_none() => return Err(format!("{coin} is not in the local marks' sources")),
        Some(coin) => vec![coin],
        None => feeds.sources.coins().collect(),
    };
    let feed_views = readings
        .iter()
        .map(|(feed, markets, reading)| FeedView {
            feed: feed.label(),
            connection: feed_connection(&reading.connections),
            connections: reading.connections.iter().map(|part| ShardView {
                shard: part.shard, markets: part.markets, connection: connection(&part.connection), frames: part.frames, quoting: part.quoting,
            }).collect(),
            frames: reading.messages,
            markets: markets.symbols.len(),
            quoting: reading.entries.iter().filter(|(_, validity)| *validity == Validity::Valid).count(),
        })
        .collect();
    let coins = coins.into_iter().map(|coin| coin_inputs(feeds, &by_feed, coin)).collect();
    Ok(InputsView { time_ms: now_ms, stopped: feeds.stopped(), feeds: feed_views, coins })
}

fn coin_inputs(feeds: &LocalMarkFeeds, by_feed: &BTreeMap<Feed, (&FeedMarkets, &FeedProjection)>, coin: &str) -> CoinInputs {
    let listings = feeds.sources.listings(coin).unwrap_or_else(|| unreachable!("a coin of the sources"));
    let mut views: BTreeMap<Kind, BTreeMap<&'static str, ListingView>> = BTreeMap::new();
    let mut coverage: BTreeMap<Kind, Coverage> = BTreeMap::new();
    for (feed, listing) in listings {
        let (markets, reading) = by_feed[feed];
        let index = markets.index(&listing.symbol).unwrap_or_else(|| unreachable!("a listing of the feed's markets"));
        let (entry, validity) = reading.entries[index];
        let quote = entry.and_then(|entry| entry.quote);
        views.entry(feed.kind).or_default().insert(
            feed.venue.name(),
            ListingView {
                symbol: listing.symbol.clone(),
                px_multiplier: listing.px_multiplier.to_string(),
                bid: quote.map(|quote| quote.bid.to_string()),
                ask: quote.map(|quote| quote.ask.to_string()),
                received_ts_ms: entry.map(|entry| entry.received_ms),
                event_ts_ms: entry.and_then(|entry| entry.event_ms),
                status: status(validity),
            },
        );
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
