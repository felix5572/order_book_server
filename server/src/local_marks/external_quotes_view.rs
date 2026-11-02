//! `extExternalQuotes` (hyperliquid-trade design 000673 §4, schema 1): one main-dex coin's external leg - the perp
//! venues' quotes valid now, their bids' and asks' weighted medians and the coin's normal basis to them -, apart from
//! `extMarkPrices`' node and local legs, so a node behind withholds none of it. Its `external` fields are
//! `extMarkPrices`' local leg's external ones, field for field (`prices_view::external_view`): `externalBidPx`,
//! `externalAskPx`, `externalQuotes` by venue - whichever venues price the coin now, Binance among them or not -, each
//! with its `receivedTsMs`, and `externalBasis`. A withheld leg reads `{"status":"unavailable","reasonCode","reason"}`:
//! `externalCoverage` (no perp venue quotes the coin now), `calculation`, `receivedAfterPublish` (a quote received
//! after `publishedTsMs`), and `stopped` in the HTTP rows only - the WS channel is withdrawn then. No ready leg holds a
//! `receivedTsMs` after its `publishedTsMs`.
//!
//! The WS frames are the external quote tick's (`quote_tick`), `intervalMs` 5: a tick every 5 ms, a coin's frame when a
//! decision field changed - the status and reason code, the venues taken, the external bid and ask, the basis' status
//! and ratio -, and every coin's every 50 ms (the heartbeat); its quotes copied before its publish time is taken, a
//! quote received after it means the wall clock stepped back. `GET /extExternalQuotes` reads the owner's snapshot,
//! every 50 ms, `intervalMs` 50: its quotes are `extMarkPrices`' of the same tick, read after the tick's time, so a
//! quote received during that read withholds the row's leg (`receivedAfterPublish`, review 000676 r1 P2) where
//! `extMarkPrices`' local leg, on its own time as before, carries it.
//!
//! The channel's refusals - not served, a coin outside the local marks' sources - carry `"schemaVersion":1` beside
//! their text (`refusal_wire`).

use bytes::Bytes;
use serde::Serialize;

use super::{
    prices_view::{ExternalView, LegView, OWNER_INTERVAL_MS, external_view, versioned_refusal, withheld},
    snapshot::{CoinMarks, ExternalLeg, LocalMarksSnapshot, Withheld},
};

pub(crate) const SCHEMA_VERSION: u32 = 1;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExternalQuotesView<'a> {
    schema_version: u32,
    coin: &'a str,
    interval_ms: u64,
    published_ts_ms: u64,
    external: LegView<ExternalView>,
}

impl<'a> ExternalQuotesView<'a> {
    /// The owner's snapshot's (`GET /extExternalQuotes`): the coin's perp quotes of the tick and its basis then.
    pub(crate) fn of(snapshot: &LocalMarksSnapshot, coin: &'a str, marks: &CoinMarks) -> Self {
        let external = marks.external.as_ref().map_or_else(withheld, |quotes| LegView::Ready(external_view(quotes, &marks.external_basis)));
        Self { schema_version: SCHEMA_VERSION, coin, interval_ms: OWNER_INTERVAL_MS, published_ts_ms: snapshot.computed_ms, external }
    }

    /// The coin's `leg` judged at `published_ms`, of a publisher of `interval_ms` (the external quote tick's).
    pub(crate) fn new(coin: &'a str, interval_ms: u64, published_ms: u64, leg: &Result<ExternalLeg, Withheld>) -> Self {
        let external = leg.as_ref().map_or_else(withheld, |leg| LegView::Ready(external_view(&leg.quotes, &leg.basis)));
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

/// `{"channel":"error","data":<text>,"schemaVersion":1}`: a refusal of the channel - not served, a coin outside the
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

    /// Design 000673 §4: the channel's `external` fields are `extMarkPrices`' local leg's external ones byte for byte -
    /// `externalBidPx`, `externalAskPx`, `externalQuotes`, `externalBasis`, in that order -, of one owner snapshot; the
    /// HTTP row says 50 ms.
    #[test]
    fn the_external_fields_are_the_local_legs_byte_for_byte() {
        let snapshot = World::new().snapshot(NOW_MS);
        let btc = snapshot.coin("BTC").unwrap();
        let marks = CoinMarksView::of(&snapshot, "BTC", btc).to_wire();
        let row = serde_json::to_vec(&ExternalQuotesView::of(&snapshot, "BTC", btc)).unwrap();
        let fields = ready_external_fields(&row, 1);
        assert!(fields.starts_with(r#""externalBidPx":"100003.95","externalAskPx":"100004.05","externalQuotes":{"binance":"#), "{fields}");
        assert!(fields.ends_with(r#""externalBasis":{"status":"unavailable","reasonCode":"externalBasisSamples","reason":"1 of the 120 samples needed over the last 600 s"}"#), "{fields}");
        assert!(String::from_utf8(marks.to_vec()).unwrap().contains(&fields), "{fields}");
        let row: serde_json::Value = serde_json::from_slice(&row).unwrap();
        assert_eq!((row["schemaVersion"].as_u64(), row["intervalMs"].as_u64(), row["publishedTsMs"].as_u64()), (Some(1), Some(50), Some(NOW_MS)));
        let eth = serde_json::to_value(ExternalQuotesView::of(&snapshot, "ETH", snapshot.coin("ETH").unwrap())).unwrap();
        assert_eq!(eth["external"]["status"], "ready", "ETH's node state unclaimed: its local leg withheld, its external leg not");
    }

    /// The refusal's text, `"schemaVersion":1` beside it.
    #[test]
    fn a_refusal_carries_schema_1_beside_its_text() {
        let wire = refusal_wire("extExternalQuotes HYPE is not in the local marks' sources");
        assert_eq!(&wire[..], br#"{"channel":"error","data":"extExternalQuotes HYPE is not in the local marks' sources","schemaVersion":1}"#);
    }
}
