//! `extHip3ExternalQuotes` (hyperliquid-trade design 000664 §2.4, 000669 r1–r3 §1.2, 1.4): one HIP-3 coin's external
//! legs at a snapshot, the frame `data` of the WS subscription and the rows of `GET /extHip3ExternalQuotes`.
//!
//! Schema 2 (schema 1 had `external` only): `external` is Binance's leg, its fields named as `extMarkPrices`' local
//! leg's, its `externalQuotes.binance.receivedTsMs` when this server received the quote; `equity` the US stock leg -
//! `feed` (`sip` or `boats`, the one counting now), `bidPx` and `askPx` as Alpaca sent them, `receivedTsMs`,
//! `quoteTsMs` (the exchange time) and `basis`. Neither leg's `receivedTsMs` is after `publishedTsMs` (both this
//! server's wall clock). A leg is `{"status":"unlisted"}` where its table lists no such coin. `quoteMaxAgeMs` is the
//! age past which this server withholds a Binance quote, its own expiry only - a reader holds a quote by its own clock,
//! from its socket's receipt of the frame (the trader's `external_max_age_ms`, design 000669 r3 §2.1, review 000672
//! P3) -; the US stock quotes have no age limit here (their feeds push on change only).
//!
//! The channel's refusals - not enabled, a coin outside it - keep their text and carry `schemaVersion` beside it
//! (`refusal_wire`): a schema 1 client reads them as before, a schema 2 client knows the server's.
//!
//! The WS frames are the external quote tick's (`quote_tick`, hyperliquid-trade design 000673 r4), `intervalMs` 5: a
//! tick every 5 ms, a coin's frame when a decision field of a leg changed, and every coin's every 50 ms (the
//! heartbeat). `GET /extHip3ExternalQuotes` reads the owner's snapshot, every 50 ms, `intervalMs` 50.

use serde::Serialize;

use super::{
    external::{Hip3BinanceLeg, Hip3ExternalCoin, Hip3SourceLeg},
    external_equity::Hip3EquityLeg,
};
use crate::local_marks::{
    prices_view::{ExternalBasisView, ExternalView, LegView, OWNER_INTERVAL_MS, external_basis_view, external_view, versioned_refusal, withheld},
    snapshot::LocalMarksSnapshot,
};

pub(crate) const SCHEMA_VERSION: u32 = 2;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Hip3ExternalView<'a> {
    schema_version: u32,
    coin: &'a str,
    interval_ms: u64,
    published_ts_ms: u64,
    quote_max_age_ms: u64,
    external: LegView<BinanceView>,
    equity: LegView<EquityView>,
}

/// The Binance leg as `extMarkPrices`' local leg writes its quotes and 10-minute basis (`ExternalView`), field for
/// field, then its 60- and 240-minute bases (user 2026-11-04, design 000741 r2): fields added after the others, the
/// schema unchanged - a reader of schema 2 that knows them not reads the rest as ever.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BinanceView {
    #[serde(flatten)]
    external: ExternalView,
    external_basis_60m: LegView<ExternalBasisView>,
    external_basis_240m: LegView<ExternalBasisView>,
}

/// The US stock quote of the feed counting now, exact as sent (multiplier 1, off HL's grid), and the coin's normal
/// basis to its midpoint on that feed.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EquityView {
    feed: &'static str,
    bid_px: String,
    ask_px: String,
    received_ts_ms: u64,
    quote_ts_ms: u64,
    basis: LegView<ExternalBasisView>,
}

impl<'a> Hip3ExternalView<'a> {
    /// The owner's snapshot's (`GET /extHip3ExternalQuotes`): `snapshot` runs with the external quotes, `legs` its
    /// coin's.
    pub(crate) fn of(snapshot: &'a LocalMarksSnapshot, coin: &'a str, legs: &Hip3ExternalCoin) -> Self {
        let external = snapshot.hip3_external().unwrap_or_else(|| unreachable!("a coin's legs are of a snapshot with the external quotes"));
        Self::new(coin, OWNER_INTERVAL_MS, external.published_ms, external.quote_max_age_ms, legs)
    }

    /// The coin's `legs` judged at `published_ms`, of a publisher of `interval_ms`.
    pub(crate) fn new(coin: &'a str, interval_ms: u64, published_ms: u64, quote_max_age_ms: u64, legs: &Hip3ExternalCoin) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            coin,
            interval_ms,
            published_ts_ms: published_ms,
            quote_max_age_ms,
            external: leg_view(&legs.external, binance_view),
            equity: leg_view(&legs.equity, equity_view),
        }
    }

    /// The WS frame `{"channel":"extHip3ExternalQuotes","data":<view>}`.
    #[expect(clippy::expect_used, reason = "string keys and finite numbers only; serializing cannot fail")]
    pub(crate) fn to_wire(&self) -> bytes::Bytes {
        #[derive(Serialize)]
        struct Envelope<'a, 'b> {
            channel: &'static str,
            data: &'b Hip3ExternalView<'a>,
        }
        let channel = crate::types::subscription::EXT_HIP3_EXTERNAL_QUOTES;
        bytes::Bytes::from(serde_json::to_vec(&Envelope { channel, data: self }).expect("serializable DTOs"))
    }
}

/// `{"channel":"error","data":<text>,"schemaVersion":2}`: a refusal of the channel - not enabled, a coin outside it -,
/// its text as the server always answered it, its schema beside it (design 000669 r2 1.4).
pub(crate) fn refusal_wire(text: &str) -> bytes::Bytes {
    versioned_refusal(text, SCHEMA_VERSION)
}

fn leg_view<T, V>(leg: &Hip3SourceLeg<T>, ready: impl FnOnce(&T) -> V) -> LegView<V> {
    match leg {
        Hip3SourceLeg::Unlisted => LegView::Unlisted,
        Hip3SourceLeg::Listed(leg) => leg.as_ref().map_or_else(withheld, |leg| LegView::Ready(ready(leg))),
    }
}

fn binance_view(leg: &Hip3BinanceLeg) -> BinanceView {
    BinanceView {
        external: external_view(&leg.leg.quotes, &leg.leg.basis),
        external_basis_60m: external_basis_view(&leg.longer.hour),
        external_basis_240m: external_basis_view(&leg.longer.four_hours),
    }
}

fn equity_view(leg: &Hip3EquityLeg) -> EquityView {
    let quote = &leg.quote;
    EquityView {
        feed: quote.feed.name(),
        bid_px: quote.quote.bid.to_string(),
        ask_px: quote.quote.ask.to_string(),
        received_ts_ms: quote.received_ms,
        quote_ts_ms: quote.quote_ms,
        basis: external_basis_view(&leg.basis),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rustc_hash::FxHashMap;
    use tokio::time::Instant;

    use super::*;
    use crate::{
        accounts::{marks::NativePriceSnapshot, publish::NodePrices},
        listeners::order_book::BestPricesAt,
        local_marks::{
            external_basis::SAMPLE_INTERVAL,
            feeds::EquityReadings,
            hip3::{external::Hip3ExternalReading, external_coins::Hip3ExternalCoins, external_sources::Hip3ExternalSources, sources::Hip3SipSources, view::Hip3MarksView},
            price::VenuePrice,
            prices_view::CoinMarksView,
            slot::{Entry, Quote, Validity},
            snapshot::{CoinTable, OwnerState, Tick, assemble},
            sources::MarkSources,
            subscriptions::FeedProjection,
        },
        order_book::Px,
    };

    /// Tuesday 2026-10-27 13:51:02 in New York: the regular session, on the SIP.
    const NOW_MS: u64 = 1_793_123_462_000;
    const MAIN: &str = r#"{"coins": {"BTC": {"spot": {"okx": {"symbol": "BTC-USDT", "pxMultiplier": "1"}}, "perp": {"okx": {"symbol": "BTC-USDT-SWAP", "pxMultiplier": "1"}}}}}"#;
    /// GOLD Binance's only, TSLA in both tables, BB the SIP's only.
    const BINANCE: &str = r#"{"venue": "binance_perp", "coins": {"xyz:GOLD": {"symbol": "XAUUSDT", "pxMultiplier": "1"}, "xyz:TSLA": {"symbol": "TSLAUSDT", "pxMultiplier": "1"}}}"#;
    const SIP: &str = r#"{"feed": "sip", "coins": {"xyz:TSLA": {"symbol": "TSLA"}, "xyz:BB": {"symbol": "BB"}}}"#;

    fn quote(bid: &str, ask: &str) -> Quote {
        Quote { bid: VenuePrice::parse(bid).unwrap(), ask: VenuePrice::parse(ask).unwrap() }
    }

    /// The SIP feeds on. Binance: GOLD unquoted, TSLA 405.7 / 405.8 received 30 ms before the tick. The SIP: BB 3.1 /
    /// 3.12 and TSLA 405.765 / 405.775, stamped a second before the tick, received 80 ms later. HL's book (BTC, then
    /// the SIP feeds' BB and TSLA, then GOLD): TSLA 405.75 / 405.77, BB 3.11 / 3.12. The external quotes published a
    /// millisecond after the tick.
    struct World {
        sources: MarkSources,
        sip: Hip3SipSources,
        coins: Arc<Hip3ExternalCoins>,
        node: NodePrices,
        book: BestPricesAt,
        equity: EquityReadings,
        binance: FeedProjection,
    }

    impl World {
        fn new() -> Self {
            let sip = Hip3SipSources::parse(SIP).unwrap();
            let coins = Arc::new(Hip3ExternalCoins::new(&Hip3ExternalSources::parse(BINANCE).unwrap(), &sip));
            let node = Ok(Arc::new(NativePriceSnapshot { seed_height: 900, height: 1_000, block_time_ms: NOW_MS - 500, coins: FxHashMap::default(), hip3: FxHashMap::default() }));
            let side = |px: u64| Some(Px::new(px));
            let prices = vec![(None, None), (side(311_000_000), side(312_000_000)), (side(40_575_000_000), side(40_577_000_000)), (None, None)];
            let book = BestPricesAt { time: NOW_MS - 400, height: 1_001, trusted: true, prices };
            let sip_quote = |quote: Quote| (Some(Entry::for_test_at(Some(quote), NOW_MS - 920, Some(NOW_MS - 1_000), 1)), Validity::Valid);
            let sip_entries = vec![sip_quote(quote("3.1", "3.12")), sip_quote(quote("405.765", "405.775"))];
            let projection = |entries| FeedProjection { connections: Vec::new(), messages: 1, entries };
            let equity = EquityReadings { sip: projection(sip_entries), boats: projection(vec![(None, Validity::NotConnected); 2]) };
            let binance_entries = vec![(None, Validity::NotQuoted), (Some(Entry::for_test(Some(quote("405.7", "405.8")), NOW_MS - 30, 1)), Validity::Valid)];
            Self { sources: MarkSources::parse(MAIN).unwrap(), sip, coins, node, book, equity, binance: projection(binance_entries) }
        }

        /// The table with the SIP feeds, the external quotes on or off.
        fn table(&self, external: bool) -> Arc<CoinTable> {
            Arc::new(CoinTable::new(&self.sources, Some(&self.sip), external.then_some(&self.coins)))
        }

        /// A tick of `table` (`self.table(external)`), the external quotes read with them on.
        fn snapshot(&self, (table, external): (&Arc<CoinTable>, bool), state: &mut OwnerState, instant: Instant, stopped: Option<&str>) -> LocalMarksSnapshot {
            let readings: Vec<_> = self.sources.feeds().map(|(feed, markets)| {
                (feed, markets, FeedProjection { connections: Vec::new(), messages: 0, entries: vec![(None, Validity::NotConnected); markets.symbols.len()] })
            }).collect();
            let reading = Hip3ExternalReading { published_ms: NOW_MS + 1, quote_max_age_ms: 60_000, projection: self.binance.clone() };
            let tick = Tick {
                now_ms: NOW_MS, instant, stopped: stopped.map(str::to_string), node: &self.node, book: Some(&self.book), readings: &readings,
                equity: Some(&self.equity), hip3_external: external.then_some(&reading),
            };
            assemble(table, &tick, state)
        }
    }

    fn frame(snapshot: &LocalMarksSnapshot, coin: &str) -> bytes::Bytes {
        Hip3ExternalView::of(snapshot, coin, snapshot.hip3_external_coin(coin).unwrap()).to_wire()
    }

    /// Design 000669 r1 1.2, the schema 2 wire, byte for byte - the Binance leg's 60- and 240-minute bases after its
    /// fields since design 000741 r2: TSLA's two legs ready - Binance's as schema 1's leg, the
    /// SIP's quote as sent with its receipt and exchange times -, each received before `publishedTsMs`, each basis
    /// from its first sample; GOLD's US stock leg and BB's Binance leg `unlisted`.
    #[test]
    fn the_schema_2_frame_carries_both_legs() {
        let world = World::new();
        let table = world.table(true);
        let snapshot = world.snapshot((&table, true), &mut table.owner_state(), Instant::now(), None);
        let withheld = r#"{"status":"unavailable","reasonCode":"externalBasisSamples","reason":"1 of the 120 samples needed over the last 600 s"}"#;
        let expected = format!(
            r#"{{"channel":"extHip3ExternalQuotes","data":{{"schemaVersion":2,"coin":"xyz:TSLA","intervalMs":50,"publishedTsMs":{published},"quoteMaxAgeMs":60000,"#,
            published = NOW_MS + 1
        ) + &format!(
            r#""external":{{"status":"ready","externalBidPx":"405.7","externalAskPx":"405.8","externalQuotes":{{"binance":{{"bidPx":"405.7","askPx":"405.8","receivedTsMs":{binance}}}}},"externalBasis":{withheld},"externalBasis60m":{hour},"externalBasis240m":{four_hours}}},"#,
            binance = NOW_MS - 30,
            hour = r#"{"status":"unavailable","reasonCode":"externalBasisSamples","reason":"1 of the 60 samples needed over the last 3600 s"}"#,
            four_hours = r#"{"status":"unavailable","reasonCode":"externalBasisSamples","reason":"1 of the 60 samples needed over the last 14400 s"}"#
        ) + &format!(
            r#""equity":{{"status":"ready","feed":"sip","bidPx":"405.765","askPx":"405.775","receivedTsMs":{received},"quoteTsMs":{quoted},"basis":{withheld}}}}}}}"#,
            received = NOW_MS - 920,
            quoted = NOW_MS - 1_000
        );
        assert_eq!(String::from_utf8(frame(&snapshot, "xyz:TSLA").to_vec()).unwrap(), expected);
        let gold: serde_json::Value = serde_json::from_slice(&frame(&snapshot, "xyz:GOLD")).unwrap();
        assert_eq!((gold["data"]["external"]["reasonCode"].as_str(), &gold["data"]["equity"]), (Some("notQuoted"), &serde_json::json!({"status": "unlisted"})));
        let bb: serde_json::Value = serde_json::from_slice(&frame(&snapshot, "xyz:BB")).unwrap();
        assert_eq!(bb["data"]["external"], serde_json::json!({"status": "unlisted"}));
        assert_eq!((bb["data"]["equity"]["status"].as_str(), bb["data"]["equity"]["bidPx"].as_str()), (Some("ready"), Some("3.1")), "the SIP's only coin on its own leg");
        assert_eq!(snapshot.hip3_external_coins().map(|(coin, _)| coin).collect::<Vec<_>>(), ["xyz:BB", "xyz:GOLD", "xyz:TSLA"]);
    }

    /// Each leg's basis from its 120th sample: TSLA's book midpoint 405.76 over Binance's 405.75 and over the SIP's
    /// 405.77; the feeds stopped withhold both listed legs, an unlisted one stays so.
    #[test]
    fn each_leg_publishes_its_own_basis_and_the_feeds_stopped_withhold_both() {
        let world = World::new();
        let table = world.table(true);
        let mut state = table.owner_state();
        let start = Instant::now();
        let last = (0..120).map(|second| world.snapshot((&table, true), &mut state, start + SAMPLE_INTERVAL * second, None)).last().unwrap();
        let tsla: serde_json::Value = serde_json::from_slice(&frame(&last, "xyz:TSLA")).unwrap();
        let (external, equity) = (&tsla["data"]["external"]["externalBasis"], &tsla["data"]["equity"]["basis"]);
        assert_eq!((external["samples"].as_u64(), equity["samples"].as_u64()), (Some(120), Some(120)));
        assert!((external["ratio"].as_f64().unwrap() - (405.76 / 405.75 - 1.0)).abs() < 1e-12, "{external}");
        assert!((equity["ratio"].as_f64().unwrap() - (405.76 / 405.77 - 1.0)).abs() < 1e-12, "{equity}");
        let stopped = world.snapshot((&table, true), &mut state, start + SAMPLE_INTERVAL * 120, Some("the equity_alpaca shard 0 task failed"));
        let tsla: serde_json::Value = serde_json::from_slice(&frame(&stopped, "xyz:TSLA")).unwrap();
        assert_eq!((tsla["data"]["external"]["reasonCode"].as_str(), tsla["data"]["equity"]["reasonCode"].as_str()), (Some("stopped"), Some("stopped")));
        let bb: serde_json::Value = serde_json::from_slice(&frame(&stopped, "xyz:BB")).unwrap();
        assert_eq!(bb["data"]["external"], serde_json::json!({"status": "unlisted"}));
    }

    /// Design 000669 测试要点: the external quotes on leave `extMarkPrices`' and `extHip3MarkPrices`' frames byte for
    /// byte as they are off.
    #[test]
    fn the_external_quotes_leave_the_other_channels_unchanged() {
        let world = World::new();
        let (on, off) = (world.table(true), world.table(false));
        let instant = Instant::now();
        let with = world.snapshot((&on, true), &mut on.owner_state(), instant, None);
        let without = world.snapshot((&off, false), &mut off.owner_state(), instant, None);
        let main = |snapshot: &LocalMarksSnapshot| {
            snapshot.coins().map(|(coin, marks)| CoinMarksView::of(snapshot, coin, marks).to_wire()).collect::<Vec<_>>()
        };
        let hip3 = |snapshot: &LocalMarksSnapshot| {
            snapshot.hip3_coins().map(|(coin, marks)| Hip3MarksView::of(snapshot, coin, marks).to_wire()).collect::<Vec<_>>()
        };
        assert_eq!(main(&with), main(&without));
        assert_eq!(hip3(&with), hip3(&without));
        assert_eq!(hip3(&with).len(), 2);
        assert!(with.hip3_external_coin("xyz:TSLA").is_some() && without.hip3_external().is_none());
    }

    /// Design 000669 r2 1.4: the refusal's text as the server answered it, `schemaVersion` beside it.
    #[test]
    fn a_refusal_carries_the_schema_beside_its_text() {
        let wire = refusal_wire("extHip3ExternalQuotes xyz:KIOXIA is not in the HIP-3 external quote sources");
        assert_eq!(&wire[..], br#"{"channel":"error","data":"extHip3ExternalQuotes xyz:KIOXIA is not in the HIP-3 external quote sources","schemaVersion":2}"#);
    }
}
