//! `extHip3ExternalQuotes` (hyperliquid-trade design 000664 §2.4): one HIP-3 coin's external quote and its normal
//! basis at a snapshot, the frame `data` of the WS subscription and the rows of `GET /extHip3ExternalQuotes`. The leg's
//! fields are named as `extMarkPrices`' local leg's; `externalQuotes.binance.receivedTsMs` is when this server received
//! the quote, never after `publishedTsMs` (both its wall clock), and `quoteMaxAgeMs` the age past which it withholds
//! one: a reader holds a quote no longer than `receivedTsMs + quoteMaxAgeMs`.

use std::collections::BTreeMap;

use serde::Serialize;

use super::external::Hip3ExternalLeg;
use crate::local_marks::{
    prices_view::{ExternalBasisView, ExternalQuoteView, INTERVAL_MS, LegView, external_basis_view, external_quotes, withheld},
    snapshot::{LocalMarksSnapshot, Withheld},
};

pub(crate) const SCHEMA_VERSION: u32 = 1;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Hip3ExternalView<'a> {
    schema_version: u32,
    coin: &'a str,
    interval_ms: u64,
    published_ts_ms: u64,
    quote_max_age_ms: u64,
    external: LegView<ExternalView>,
}

/// The external bid and ask (one venue: its quote, exact, its multiplier applied, off HL's price grid), the quote, and
/// the coin's normal basis to it.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExternalView {
    external_bid_px: String,
    external_ask_px: String,
    external_quotes: BTreeMap<&'static str, ExternalQuoteView>,
    external_basis: LegView<ExternalBasisView>,
}

impl<'a> Hip3ExternalView<'a> {
    /// `snapshot` runs with the external quotes, `leg` its coin's.
    pub(crate) fn of(snapshot: &'a LocalMarksSnapshot, coin: &'a str, leg: &Result<Hip3ExternalLeg, Withheld>) -> Self {
        let external = snapshot.hip3_external().unwrap_or_else(|| unreachable!("a coin's leg is of a snapshot with the external quotes"));
        Self {
            schema_version: SCHEMA_VERSION,
            coin,
            interval_ms: INTERVAL_MS,
            published_ts_ms: external.published_ms,
            quote_max_age_ms: external.quote_max_age_ms,
            external: leg.as_ref().map_or_else(withheld, |leg| {
                LegView::Ready(ExternalView {
                    external_bid_px: leg.quotes.bid.to_string(),
                    external_ask_px: leg.quotes.ask.to_string(),
                    external_quotes: external_quotes(&leg.quotes),
                    external_basis: external_basis_view(&leg.basis),
                })
            }),
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
