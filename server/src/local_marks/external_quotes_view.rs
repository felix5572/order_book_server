//! `extExternalQuotes` (hyperliquid-trade design 000673 §4; schema 2, design 000681): one main-dex coin's external
//! leg (the perp venues' quotes valid now, their bids' and asks' weighted medians and the coin's normal basis to them,
//! and each venue's own normal basis to its quote), apart from `extMarkPrices`' node and local legs, so a node behind
//! withholds none of it. Its `external` holds `externalBidPx`, `externalAskPx`, `externalQuotes` by venue - whichever
//! venues price the coin now, Binance among them or not -, each with its `receivedTsMs` and its own `basis`
//! (`{"status":"ready","ratio","samples"}` or a withheld one, as `externalBasis` reads: the median basis' rule on the
//! venue's quote alone, its window the venue's whether it quotes or not), and `externalBasis`; the first three and the
//! last as `extMarkPrices`' local leg writes its own, which carries no venue's basis. A venue's `basis.ratio` is its
//! lasting offset to HL. A withheld leg reads `{"status":"unavailable","reasonCode","reason"}`: `externalCoverage` (no
//! perp venue quotes the coin now), `calculation`, `receivedAfterPublish` (a quote received after `publishedTsMs`), and
//! `stopped` in the HTTP rows only - the WS channel is withdrawn then. No ready leg holds a `receivedTsMs` after its
//! `publishedTsMs`.
//!
//! The WS frames are the external quote tick's (`quote_tick`), `intervalMs` 5: a tick every 5 ms, a coin's frame when a
//! decision field changed - the status and reason code, the venues taken, the external bid and ask, Binance's own bid
//! and ask, the basis' and each venue's basis' status and ratio -, and every coin's every 50 ms (the heartbeat); its
//! quotes copied before its publish time is taken, a quote received after it means the wall clock stepped back; a
//! basis the owner computed more than `BASIS_MAX_AGE` (10 s) before reads `basisStale`. `GET /extExternalQuotes` reads the owner's
//! snapshot, every 50 ms, `intervalMs` 50: its quotes are `extMarkPrices`' of the same tick, read after the tick's
//! time, so a quote received during that read withholds the row's leg (`receivedAfterPublish`, review 000676 r1 P2)
//! where `extMarkPrices`' local leg, on its own time as before, carries it.
//!
//! The channel's refusals - not served, a coin outside the local marks' sources - carry `"schemaVersion":2` beside
//! their text (`refusal_wire`).

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::Serialize;

use super::{
    prices_view::{ExternalBasisView, LegView, OWNER_INTERVAL_MS, external_basis_view, versioned_refusal, withheld},
    snapshot::{CoinMarks, LocalMarksSnapshot, MainExternalLeg, VenueBasis, VenueQuote, Withheld},
};

pub(crate) const SCHEMA_VERSION: u32 = 2;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExternalQuotesView<'a> {
    schema_version: u32,
    coin: &'a str,
    interval_ms: u64,
    published_ts_ms: u64,
    external: LegView<ExternalView>,
}

/// The external bid and ask (the quotes' weighted medians, exact, the listings' multipliers applied, off HL's price
/// grid), each quote with its venue's own basis, and the coin's normal basis to the medians.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExternalView {
    external_bid_px: String,
    external_ask_px: String,
    external_quotes: BTreeMap<&'static str, VenueQuoteView>,
    external_basis: LegView<ExternalBasisView>,
}

/// A venue's quote, its listing's multiplier applied, its receipt time (this server's wall clock), and the venue's own
/// basis to it.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct VenueQuoteView {
    bid_px: String,
    ask_px: String,
    received_ts_ms: u64,
    basis: LegView<ExternalBasisView>,
}

fn external_view(leg: &MainExternalLeg) -> ExternalView {
    let quotes = &leg.external.quotes;
    let venue = |(quote, venue): (&VenueQuote, &VenueBasis)| {
        let view = VenueQuoteView { bid_px: quote.bid.to_string(), ask_px: quote.ask.to_string(), received_ts_ms: quote.received_ms, basis: external_basis_view(&venue.basis) };
        (quote.feed.venue.name(), view)
    };
    ExternalView {
        external_bid_px: quotes.bid.to_string(),
        external_ask_px: quotes.ask.to_string(),
        external_quotes: quotes.venues.iter().zip(&leg.venue_bases).map(venue).collect(),
        external_basis: external_basis_view(&leg.external.basis),
    }
}

impl<'a> ExternalQuotesView<'a> {
    /// The owner's snapshot's (`GET /extExternalQuotes`): the coin's perp quotes of the tick and the bases then.
    pub(crate) fn of(snapshot: &LocalMarksSnapshot, coin: &'a str, marks: &CoinMarks) -> Self {
        let leg = marks.external.clone().map(|quotes| MainExternalLeg::of(quotes, marks.external_basis.clone(), &marks.venue_bases, Clone::clone));
        Self::new(coin, OWNER_INTERVAL_MS, snapshot.computed_ms, &leg)
    }

    /// The coin's `leg` judged at `published_ms`, of a publisher of `interval_ms` (the external quote tick's).
    pub(crate) fn new(coin: &'a str, interval_ms: u64, published_ms: u64, leg: &Result<MainExternalLeg, Withheld>) -> Self {
        let external = leg.as_ref().map_or_else(withheld, |leg| LegView::Ready(external_view(leg)));
        Self { schema_version: SCHEMA_VERSION, coin, interval_ms, published_ts_ms: published_ms, external }
    }

    /// The WS frame `{"channel":"extExternalQuotes","data":<view>}`.
    #[expect(clippy::expect_used, reason = "string keys and finite numbers only; serializing cannot fail")]
    pub(crate) fn to_wire(&self) -> Bytes {
        #[derive(Serialize)]
        struct Envelope<'a, 'b> {
            channel: &'static str,
            data: &'b ExternalQuotesView<'a>,
        }
        let channel = crate::types::subscription::EXT_EXTERNAL_QUOTES;
        Bytes::from(serde_json::to_vec(&Envelope { channel, data: self }).expect("serializable DTOs"))
    }
}

/// `{"channel":"error","data":<text>,"schemaVersion":2}`: a refusal of the channel - not served, a coin outside the
/// sources -, its schema beside its text.
pub(crate) fn refusal_wire(text: &str) -> Bytes {
    versioned_refusal(text, SCHEMA_VERSION)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::local_marks::{
        prices_view::CoinMarksView,
        snapshot::tests::{NOW_MS, World},
    };

    /// The ready `external` leg's fields, as written, of a row or frame whose last field it is, `enclosing` objects
    /// closing after it (a row's one, a frame's two).
    pub(crate) fn ready_external_fields(wire: &[u8], enclosing: usize) -> String {
        let text = std::str::from_utf8(wire).unwrap();
        let start = text.find(r#""external":{"status":"ready","#).unwrap() + r#""external":{"status":"ready","#.len();
        let leg = &text[start..text.len() - enclosing];
        leg.strip_suffix('}').unwrap().to_string()
    }

    /// Design 000681: the schema 2 row byte for byte - each venue's quote with its own basis -; the medians, their
    /// basis and each quote as `extMarkPrices`' local leg writes them, which carries no venue's basis; HTTP says 50 ms.
    #[test]
    fn the_schema_2_row_byte_for_byte() {
        let snapshot = World::new().snapshot(NOW_MS);
        let btc = snapshot.coin("BTC").unwrap();
        let row = String::from_utf8(serde_json::to_vec(&ExternalQuotesView::of(&snapshot, "BTC", btc)).unwrap()).unwrap();
        let withheld = r#"{"status":"unavailable","reasonCode":"externalBasisSamples","reason":"1 of the 120 samples needed over the last 600 s"}"#;
        let expected = format!(
            r#"{{"schemaVersion":2,"coin":"BTC","intervalMs":50,"publishedTsMs":{NOW_MS},"external":{{"status":"ready","externalBidPx":"100003.95","externalAskPx":"100004.05","#
        ) + &format!(r#""externalQuotes":{{"binance":{{"bidPx":"100003.95","askPx":"100004.05","receivedTsMs":{NOW_MS},"basis":{withheld}}}}},"externalBasis":{withheld}}}}}"#);
        assert_eq!(row, expected);
        let marks: serde_json::Value = serde_json::from_slice(&CoinMarksView::of(&snapshot, "BTC", btc).to_wire()).unwrap();
        let (local, mut external) = (&marks["data"]["local"], serde_json::from_str::<serde_json::Value>(&row).unwrap()["external"].clone());
        for field in ["externalBidPx", "externalAskPx", "externalBasis"] {
            assert_eq!(local[field], external[field], "{field}");
        }
        external["externalQuotes"]["binance"].as_object_mut().unwrap().remove("basis");
        assert_eq!(local["externalQuotes"], external["externalQuotes"], "the quote as the local leg's, its basis apart");
    }

    /// The refusal's text, `"schemaVersion":2` beside it.
    #[test]
    fn a_refusal_carries_schema_2_beside_its_text() {
        let wire = refusal_wire("extExternalQuotes HYPE is not in the local marks' sources");
        assert_eq!(&wire[..], br#"{"channel":"error","data":"extExternalQuotes HYPE is not in the local marks' sources","schemaVersion":2}"#);
    }
}
