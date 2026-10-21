//! A venue's fixed connection partitions and their derived reading. Each connection owns its quote epochs;
//! joining readings preserves the source file's market order and never changes a quote's validity.
use std::sync::Arc;

use super::{
    slot::{Connection, Entry, FeedSlot, Validity},
    sources::{Feed, FeedMarkets, Kind, Venue},
};

/// BM 2026-10-21, same backend: 173 perp bookTickers ended at 36.2 s; 50/50/50/23 all lasted 182 s.
/// This is an empirical connection-size choice, not a Binance protocol limit or a proven server root cause.
const BINANCE_PERP_MARKETS_PER_CONNECTION: usize = 50;
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
        let width = if feed == (Feed { kind: Kind::Perp, venue: Venue::Binance }) {
            BINANCE_PERP_MARKETS_PER_CONNECTION
        } else { markets.symbols.len() };
        let expiry = (feed == (Feed { kind: Kind::Spot, venue: Venue::Mexc })).then_some(MEXC_SPOT_EXPIRY_MS);
        let subscriptions = (0..markets.symbols.len()).step_by(width).enumerate().map(|(shard, first)| {
            let markets = markets.slice(first..(first + width).min(markets.symbols.len()));
            let slot = Arc::new(FeedSlot::new(feed, markets.symbols.len(), expiry));
            Arc::new(Subscription { shard, markets, slot })
        }).collect();
        Self { feed, subscriptions }
    }

    pub(crate) fn read(&self, now_ms: u64) -> FeedProjection {
        let mut connections = Vec::with_capacity(self.subscriptions.len());
        let mut entries = Vec::with_capacity(self.subscriptions.iter().map(|part| part.markets.symbols.len()).sum());
        let mut messages = 0;
        for part in &self.subscriptions {
            let reading = part.slot.read(now_ms);
            connections.push(ConnectionProjection {
                shard: part.shard, markets: part.markets.symbols.len(), connection: reading.connection,
                frames: reading.messages, quoting: reading.entries.iter().filter(|(_, validity)| *validity == Validity::Valid).count(),
            });
            messages += reading.messages;
            entries.extend(reading.entries);
        }
        FeedProjection { connections, messages, entries }
    }
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
                assert_eq!(group.subscriptions.iter().map(|s| s.markets.symbols.len()).collect::<Vec<_>>(), [50, 50, 50, 23]);
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
        let broken = &group.subscriptions[1];
        broken.slot.disconnected("EOF".into(), 4);
        let reading = group.read(4);
        for (index, (_, validity)) in reading.entries.iter().enumerate() {
            assert_eq!(*validity, if (50..100).contains(&index) { Validity::NotConnected } else { Validity::Valid });
        }
        let epoch = broken.slot.connected(5);
        // Connecting alone and a late frame from the old connection cannot revive prior quotes.
        broken.slot.record(epoch - 1, 6, [(0, Some(quote), None)]);
        assert_eq!(group.read(6).entries[50].1, Validity::NotQuoted);
        broken.slot.record(epoch, 7, [(0, Some(quote), None)]);
        let reading = group.read(7);
        assert_eq!(reading.entries[50].1, Validity::Valid);
        assert_eq!(reading.entries[51].1, Validity::NotQuoted);
        assert_eq!(reading.entries[100].1, Validity::Valid);
        broken.slot.stopped("refused subscription".into(), 8);
        let reading = group.read(8);
        assert_eq!(reading.entries[50].1, Validity::NotConnected);
        assert_eq!(reading.entries[0].1, Validity::Valid);
        assert_eq!(reading.entries[100].1, Validity::Valid);
    }

    #[test]
    fn wire_parsing_cannot_write_a_quote_into_another_shard() {
        let sources = MarkSources::embedded().unwrap();
        let group = FeedSubscriptions::new(BINANCE, sources.markets(BINANCE).unwrap());
        let protocol = Protocol { kind: BINANCE.kind, venue: BINANCE.venue };
        let part = &group.subscriptions[1];
        let symbol = &part.markets.symbols[0];
        let wire = serde_json::json!({"stream":format!("{}@bookTicker",symbol.to_lowercase()),"data":{"e":"bookTicker","s":symbol,"b":"100","a":"101","E":10,"T":10}}).to_string();
        assert!(matches!(protocol.parse(&wire, &part.markets).unwrap(), Parsed::Quotes(q) if q[0].0 == 0));
        assert!(protocol.parse(&wire, &group.subscriptions[0].markets).is_err());
        let session = protocol.session(&part.markets.symbols, 0, None);
        assert_eq!(session.url.matches("@bookTicker").count(), 50);
    }
}
