//! A venue's fixed connection partitions and their derived reading. Each connection owns its quote epochs;
//! joining readings preserves the source file's market order and never changes a quote's validity.
use std::sync::Arc;

use super::{
    slot::{Connection, Entry, FeedReading, FeedSlot, SlotCopy, Validity},
    sources::{Feed, FeedMarkets, Kind, Venue},
};

/// BM 2026-10-21, same remote IP: 173 perp bookTickers ended at 36.2 s; 50/50/50/23 all lasted 182 s.
/// This is an empirical connection-size choice, not a Binance protocol limit or a proven server root cause.
const BINANCE_PERP_MARKETS_PER_CONNECTION: usize = 50;
/// The first 50-symbol Rust partition still lost 7 connections while the other 123 markets stayed connected.
/// A same-IP 120 s count put 66% of that partition's messages in BTC/ETH. Give them separate connections to
/// avoid concentrating their bursts; the network/server cause is not established by this mitigation.
const DEDICATED_BINANCE_PERP_MARKETS: [&str; 2] = ["BTCUSDT", "ETHUSDT"];
const MEXC_SPOT_EXPIRY_MS: u64 = 5_000;

pub(crate) struct Subscription {
    pub(crate) shard: usize,
    pub(crate) markets: FeedMarkets,
    pub(crate) slot: Arc<FeedSlot>,
}

pub(crate) struct FeedSubscriptions {
    pub(crate) feed: Feed,
    pub(crate) subscriptions: Vec<Arc<Subscription>>,
}

#[derive(Debug, Clone)]
pub(crate) struct ConnectionProjection {
    pub(crate) shard: usize,
    pub(crate) markets: usize,
    pub(crate) connection: Connection,
    pub(crate) frames: u64,
    pub(crate) quoting: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct FeedProjection {
    pub(crate) connections: Vec<ConnectionProjection>,
    pub(crate) messages: u64,
    /// Full feed market order; each validity belongs to that market's connection epoch.
    pub(crate) entries: Vec<(Option<Entry>, Validity)>,
}

impl FeedSubscriptions {
    /// `MarkSources` only registers feeds with at least one validated market.
    pub(crate) fn new(feed: Feed, markets: &FeedMarkets) -> Self {
        let expiry = (feed == (Feed { kind: Kind::Spot, venue: Venue::Mexc })).then_some(MEXC_SPOT_EXPIRY_MS);
        Self::with_expiry(feed, markets, expiry)
    }

    /// `expiry`: every partition's quote age limit (`FeedSlot`). Binance's perp feeds, the HIP-3 external quotes' too,
    /// take `BINANCE_PERP_MARKETS_PER_CONNECTION` markets a connection.
    pub(crate) fn with_expiry(feed: Feed, markets: &FeedMarkets, expiry: Option<u64>) -> Self {
        let binance_perp = feed.venue == Venue::Binance && feed.kind != Kind::Spot;
        let width = if binance_perp { BINANCE_PERP_MARKETS_PER_CONNECTION } else { markets.symbols.len() };
        let dedicated = |symbol: &String| binance_perp && DEDICATED_BINANCE_PERP_MARKETS.contains(&symbol.as_str());
        let mut subscriptions = Vec::new();
        let mut first = 0;
        while first < markets.symbols.len() {
            let limit = (first + width).min(markets.symbols.len());
            let end = if dedicated(&markets.symbols[first]) { first + 1 } else {
                markets.symbols[first..limit].iter().position(dedicated).map_or(limit, |offset| first + offset)
            };
            let markets = markets.slice(first..end);
            let slot = Arc::new(FeedSlot::new(feed, markets.symbols.len(), expiry));
            subscriptions.push(Arc::new(Subscription { shard: subscriptions.len(), markets, slot }));
            first = end;
        }
        Self { feed, subscriptions }
    }

    pub(crate) fn read(&self, now_ms: u64) -> FeedProjection {
        projection(self.subscriptions.iter().map(|part| (part.shard, part.markets.symbols.len(), part.slot.read(now_ms))))
    }

    /// Every partition's slot as it stands, its validity judged later (`FeedCopy::projection`).
    pub(crate) fn copy(&self) -> FeedCopy {
        FeedCopy { parts: self.subscriptions.iter().map(|part| (part.shard, part.markets.symbols.len(), part.slot.copy())).collect() }
    }

    /// The partition holding the feed's `market` (its index in the feed's market order) and the market's index there.
    pub(crate) fn part_of(&self, market: usize) -> (&Subscription, usize) {
        let mut first = 0;
        for part in &self.subscriptions {
            let end = first + part.markets.symbols.len();
            if market < end {
                return (part.as_ref(), market - first);
            }
            first = end;
        }
        unreachable!("{} market {market} is past its partitions' {first}", self.feed.label())
    }
}

/// A feed's partitions copied at one moment (`SlotCopy`), by shard: (shard, markets, copy).
pub(crate) struct FeedCopy {
    parts: Vec<(usize, usize, SlotCopy)>,
}

impl FeedCopy {
    pub(crate) fn projection(self, now_ms: u64) -> FeedProjection {
        projection(self.parts.into_iter().map(|(shard, markets, copy)| (shard, markets, copy.reading(now_ms))))
    }
}

/// The partitions' readings joined in the feed's market order: (shard, markets, reading) each.
fn projection(parts: impl ExactSizeIterator<Item = (usize, usize, FeedReading)>) -> FeedProjection {
    let mut connections = Vec::with_capacity(parts.len());
    let mut entries = Vec::new();
    let mut messages = 0;
    for (shard, markets, reading) in parts {
        connections.push(ConnectionProjection {
            shard, markets, connection: reading.connection,
            frames: reading.messages, quoting: reading.entries.iter().filter(|(_, validity)| *validity == Validity::Valid).count(),
        });
        messages += reading.messages;
        entries.extend(reading.entries);
    }
    FeedProjection { connections, messages, entries }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_marks::{price::VenuePrice, protocol::{Parsed, Protocol}, slot::Quote, sources::MarkSources};

    const BINANCE: Feed = Feed { kind: Kind::Perp, venue: Venue::Binance };

    #[test]
    fn partitions_cover_each_symbol_once_in_original_order_with_local_indices() {
        let sources = MarkSources::embedded().unwrap();
        for (feed, markets) in sources.feeds() {
            let group = FeedSubscriptions::new(feed, markets);
            if feed == BINANCE {
                assert_eq!(group.subscriptions.iter().map(|s| s.markets.symbols.len()).collect::<Vec<_>>(), [31, 1, 16, 1, 50, 50, 24]);
                for symbol in DEDICATED_BINANCE_PERP_MARKETS {
                    let part = group.subscriptions.iter().find(|part| part.markets.index(symbol).is_some()).unwrap();
                    assert_eq!(part.markets.symbols, [symbol]);
                }
            } else { assert_eq!(group.subscriptions.len(), 1); }
            let symbols: Vec<_> = group.subscriptions.iter().flat_map(|s| s.markets.symbols.iter()).collect();
            assert_eq!(symbols, markets.symbols.iter().collect::<Vec<_>>());
            let coins: Vec<_> = group.subscriptions.iter().flat_map(|s| s.markets.coins.iter()).collect();
            assert_eq!(coins, markets.coins.iter().collect::<Vec<_>>());
            for part in &group.subscriptions {
                for (index, symbol) in part.markets.symbols.iter().enumerate() { assert_eq!(part.markets.index(symbol), Some(index)); }
            }
        }
    }

    /// Design 000664 §2.1: the HIP-3 external quotes' partitions are their own, 50 markets a connection, every quote
    /// expiring past the age limit; the main-dex Binance perps' never expire.
    #[test]
    fn the_hip3_external_partitions_are_their_own_and_expire() {
        let external = crate::local_marks::hip3::external_sources::Hip3ExternalSources::embedded().unwrap();
        let group = FeedSubscriptions::with_expiry(Feed::HIP3_EXTERNAL, &external.markets, Some(2_000));
        assert_eq!(group.subscriptions.iter().map(|part| part.markets.symbols.len()).collect::<Vec<_>>(), [50, 42]);
        let quote = Quote { bid: VenuePrice::parse("100").unwrap(), ask: VenuePrice::parse("101").unwrap() };
        let sources = MarkSources::embedded().unwrap();
        let main = FeedSubscriptions::new(BINANCE, sources.markets(BINANCE).unwrap());
        for part in group.subscriptions.iter().chain(&main.subscriptions) {
            let epoch = part.slot.connected(0);
            part.slot.record(epoch, 1_000, (0..part.markets.symbols.len()).map(|i| (i, Some(quote), None)));
        }
        assert!(group.read(3_000).entries.iter().all(|(_, validity)| *validity == Validity::Valid));
        assert!(group.read(3_001).entries.iter().all(|(_, validity)| *validity == Validity::Expired));
        assert!(main.read(3_600_000).entries.iter().all(|(_, validity)| *validity == Validity::Valid));
    }

    #[test]
    fn a_disconnect_reconnect_or_stop_only_withdraws_that_shards_quotes() {
        let sources = MarkSources::embedded().unwrap();
        let markets = sources.markets(BINANCE).unwrap();
        let group = FeedSubscriptions::new(BINANCE, markets);
        let quote = Quote { bid: VenuePrice::parse("100").unwrap(), ask: VenuePrice::parse("101").unwrap() };
        for part in &group.subscriptions {
            let epoch = part.slot.connected(1);
            part.slot.record(epoch, 2, (0..part.markets.symbols.len()).map(|i| (i, Some(quote), None)));
        }
        assert!(group.read(3).entries.iter().all(|(_, v)| *v == Validity::Valid));
        let broken = &group.subscriptions[4];
        let first = 49;
        let end = 99;
        broken.slot.disconnected("EOF".into(), 4);
        let reading = group.read(4);
        for (index, (_, validity)) in reading.entries.iter().enumerate() {
            assert_eq!(*validity, if (first..end).contains(&index) { Validity::NotConnected } else { Validity::Valid });
        }
        let epoch = broken.slot.connected(5);
        // Connecting alone and a late frame from the old connection cannot revive prior quotes.
        broken.slot.record(epoch - 1, 6, [(0, Some(quote), None)]);
        assert_eq!(group.read(6).entries[first].1, Validity::NotQuoted);
        broken.slot.record(epoch, 7, [(0, Some(quote), None)]);
        let reading = group.read(7);
        assert_eq!(reading.entries[first].1, Validity::Valid);
        assert_eq!(reading.entries[first + 1].1, Validity::NotQuoted);
        assert_eq!(reading.entries[end].1, Validity::Valid);
        broken.slot.stopped("refused subscription".into(), 8);
        let reading = group.read(8);
        assert_eq!(reading.entries[first].1, Validity::NotConnected);
        assert_eq!(reading.entries[0].1, Validity::Valid);
        assert_eq!(reading.entries[end].1, Validity::Valid);
    }

    #[test]
    fn wire_parsing_cannot_write_a_quote_into_another_shard() {
        let sources = MarkSources::embedded().unwrap();
        let group = FeedSubscriptions::new(BINANCE, sources.markets(BINANCE).unwrap());
        let protocol = Protocol { kind: BINANCE.kind, venue: BINANCE.venue };
        let part = &group.subscriptions[4];
        let symbol = &part.markets.symbols[0];
        let wire = serde_json::json!({"stream":format!("{}@bookTicker",symbol.to_lowercase()),"data":{"e":"bookTicker","s":symbol,"b":"100","a":"101","E":10,"T":10}}).to_string();
        assert!(matches!(protocol.parse(&wire, &part.markets).unwrap(), Parsed::Quotes(q) if q[0].0 == 0));
        assert!(protocol.parse(&wire, &group.subscriptions[0].markets).is_err());
        let session = protocol.session(&part.markets.symbols, 0, None);
        assert_eq!(session.url.matches("@bookTicker").count(), 50);
    }
}
