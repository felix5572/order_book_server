//! `extMarkPrices` (design 000454 r1 1.4): one coin's node and local legs at a tick, the frame `data` of the WS
//! subscription and the rows of `GET /extMarkPrices`. Prices are decimal text in HL's units of the coin.
//!
//! Schema 2 (the trading system's Python `hl_mark_prices` service wrote 1): the node leg is the OB's own replay, the
//! local leg is computed here; each venue's quote is in `GET /extMarkInputs`, each reference cache's members in
//! `GET /extMarkReferenceCaches`: each reference action updates its validator's member (the median refreshes per 3 s
//! bucket), and they run to kilobytes a coin, so the frames leave them out (user 2026-10-27); a read samples the state,
//! not every member update.
//!
//! The local leg's `externalBidPx`, `externalAskPx` and `externalQuotes` (design 000588), then `externalBasis` (design
//! 000594), were added within schema 2: a frame without them is of a server before them.

use std::collections::BTreeMap;

use alloy::primitives::Address;
use serde::Serialize;

use super::{
    external_basis::ExternalBasis,
    price::MAX_PERP_DECIMALS,
    snapshot::{CoinMarks, Coverage, ExternalQuotes, LocalLeg, LocalMarksSnapshot, NodeMark, Status, VenueQuote, Withheld},
};
use crate::accounts::marks::{
    decimal::ExactDecimal,
    native::MarkUpdate,
    seed::{Ema, ReferenceCache},
};

pub(crate) const SCHEMA_VERSION: u32 = 2;
/// The owner's tick: a frame of every subscribed coin each tick, on the mark channels; and the HTTP views', the
/// external quote channels' among them, which read the owner's snapshot.
pub(crate) const OWNER_INTERVAL_MS: u64 = 50;
/// The external quote tick's (`quote_tick`, hyperliquid-trade design 000673 r4): the two external quote channels' WS
/// frames, published as a coin's decision changes and as a heartbeat, never more often.
pub(crate) const QUOTE_TICK_INTERVAL_MS: u64 = 5;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CoinMarksView<'a> {
    schema_version: u32,
    coin: &'a str,
    interval_ms: u64,
    published_ts_ms: u64,
    node: LegView<NodeView>,
    local: LegView<LocalView>,
    oracle_coverage: &'a Coverage,
    external_coverage: &'a Coverage,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub(crate) enum LegView<T> {
    Ready(T),
    #[serde(rename_all = "camelCase")]
    Unavailable { reason_code: &'static str, reason: String },
    #[serde(rename_all = "camelCase")]
    Stale { reason_code: &'static str, reason: String },
    /// `{"status":"unlisted"}`: the coin is not in this leg's source table - never a price, not an unknown one
    /// (`extHip3ExternalQuotes` schema 2, hyperliquid-trade design 000669 r1 1.2); no other channel's leg is.
    Unlisted,
}

/// The replay's state of the coin at the node state's height: what the next `SetGlobalAction` takes, the stored oracle
/// median(submitted, Spot cache, L with the basis undone) and the mark median(submitted, Mark cache, L).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeView {
    mark_px: String,
    /// When the node set `markPx` (block time, unix ns), and by what: `setGlobal` (a validator's `SetGlobalAction`),
    /// `automaticRefresh` (no `SetGlobalAction` for the stale window: the last submission again) or `checkpoint` (the
    /// seed; a checkpoint does not record which).
    mark_time_ns: u64,
    mark_update: &'static str,
    stored_oracle_px: String,
    /// The last `SetGlobalAction`'s (oracle, mark) for the coin; `null` before one.
    submitted_oracle_px: Option<String>,
    submitted_mark_px: Option<String>,
    /// The reference caches' medians (`null` before one has a median) and the unix ms from which a block refreshes
    /// them: the first block of a newer 3 s bucket takes the median of the members under 10 s old, and keeps the old one
    /// when none is.
    spot_cache_px: Option<String>,
    spot_cache_next_refresh_ms: Option<u64>,
    mark_cache_px: Option<String>,
    mark_cache_next_refresh_ms: Option<u64>,
    /// The stored msoe (the basis) and lmpe (the local price, in prices).
    basis_ema: EmaView,
    local_price_ema: EmaView,
    last_trade_px: String,
    seed_height: u64,
    height: u64,
    block_time_ms: u64,
}

/// An EMA as the node stores it. Each price update `dt` s after the last sample: numerator' = sample × dt + numerator
/// × (1 - 1 / decayParameter), denominatorS' = denominatorS × (1 - 1 / decayParameter) + dt; a gap longer than
/// decayParameter resets it. Its average numerator / denominatorS applies from decayParameter samples on.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EmaView {
    numerator: f64,
    denominator_s: f64,
    decay_parameter: f64,
    samples: u64,
    /// Block time, unix ns; `null` before the first sample.
    last_sample_time_ns: Option<u64>,
}

impl EmaView {
    const fn of(ema: &Ema) -> Self {
        Self {
            numerator: ema.numerator,
            denominator_s: ema.denominator_s,
            decay_parameter: ema.decay_parameter,
            samples: ema.samples,
            last_sample_time_ns: ema.last_sample_ns,
        }
    }
}

const fn update_label(update: MarkUpdate) -> &'static str {
    match update {
        MarkUpdate::SetGlobal => "setGlobal",
        MarkUpdate::AutomaticRefresh => "automaticRefresh",
        MarkUpdate::Checkpoint => "checkpoint",
    }
}

/// The local marks, their terms and the book they read.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LocalView {
    /// The Mark member a validator submitting our venue prices now would leave.
    reference_mark_px: String,
    /// The reference as a `SetGlobalAction` carries it (5 significant figures), beside the node leg's submission.
    submitted_mark_px: String,
    /// A conditional estimate: the mark a `SetGlobalAction` now would store if validators submitted the reference.
    predicted_mark_px: String,
    /// A, L and C of the reference.
    basis_adjusted_oracle_px: String,
    local_px: String,
    external_px: String,
    /// The bids' and asks' weighted medians of the perp quotes C took, with C's weights: exact, in HL's price scale of
    /// the coin (the listing's multiplier applied), not on its price grid as C is.
    external_bid_px: String,
    external_ask_px: String,
    /// Each of those quotes, so scaled (`GET /extMarkInputs` has the venues' own prices), by venue.
    external_quotes: BTreeMap<&'static str, ExternalQuoteView>,
    /// The coin's normal basis to those medians (`external_basis`): an external side × (1 + ratio) is that side at HL's
    /// level; withheld below its minimum samples.
    external_basis: LegView<ExternalBasisView>,
    /// The venues' oracle, before the basis.
    oracle_px: String,
    basis_ratio: f64,
    basis_samples: u64,
    bid_px: String,
    ask_px: String,
    /// The node state the marks read.
    node_height: u64,
    node_block_time_ms: u64,
    book_time_ms: u64,
    book_height: u64,
}

/// The median of HL's book midpoint over the external bid and ask medians' midpoint - 1, of `samples` taken once a
/// second over the window.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExternalBasisView {
    ratio: f64,
    samples: usize,
}

/// The basis, or why it is withheld (`extMarkPrices`' local leg, `extHip3ExternalQuotes`, `extExternalQuotes`).
pub(crate) fn external_basis_view(basis: &Result<ExternalBasis, Withheld>) -> LegView<ExternalBasisView> {
    basis.as_ref().map_or_else(withheld, |basis| LegView::Ready(ExternalBasisView { ratio: basis.ratio, samples: basis.samples }))
}

/// The external quotes and the basis to them, as `extMarkPrices`' local leg names them, in its order: the Binance leg
/// of `extHip3ExternalQuotes` (`extExternalQuotes`' schema 2 adds each venue's basis, `external_quotes_view`). The
/// external bid and ask are the quotes' weighted medians (one venue: its quote), exact, the listings' multipliers
/// applied, off HL's price grid.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExternalView {
    external_bid_px: String,
    external_ask_px: String,
    external_quotes: BTreeMap<&'static str, ExternalQuoteView>,
    external_basis: LegView<ExternalBasisView>,
}

/// `quotes` and `basis` as `extMarkPrices`' local leg writes them, field for field (`local_view`).
pub(crate) fn external_view(quotes: &ExternalQuotes, basis: &Result<ExternalBasis, Withheld>) -> ExternalView {
    ExternalView {
        external_bid_px: quotes.bid.to_string(),
        external_ask_px: quotes.ask.to_string(),
        external_quotes: external_quotes(quotes),
        external_basis: external_basis_view(basis),
    }
}

/// `{"channel":"error","data":<text>,"schemaVersion":<schema>}`: a refusal of an external quote channel, its text
/// beside its schema (hyperliquid-trade design 000669 r2 1.4, 000673 r3 §3), so a reader of either schema reads the
/// text as before and knows the server's schema.
#[expect(clippy::expect_used, reason = "a string and a number only; serializing cannot fail")]
pub(crate) fn versioned_refusal(text: &str, schema_version: u32) -> bytes::Bytes {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Refusal<'a> {
        channel: &'static str,
        data: &'a str,
        schema_version: u32,
    }
    let refusal = Refusal { channel: "error", data: text, schema_version };
    bytes::Bytes::from(serde_json::to_vec(&refusal).expect("a serializable DTO"))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExternalQuoteView {
    bid_px: String,
    ask_px: String,
    received_ts_ms: u64,
}

/// Each quote by venue, scaled to HL's price scale of the coin, with its receipt time (the server's wall clock).
pub(crate) fn external_quotes(quotes: &ExternalQuotes) -> BTreeMap<&'static str, ExternalQuoteView> {
    let view = |quote: &VenueQuote| ExternalQuoteView { bid_px: quote.bid.to_string(), ask_px: quote.ask.to_string(), received_ts_ms: quote.received_ms };
    quotes.venues.iter().map(|quote| (quote.feed.venue.name(), view(quote))).collect()
}

pub(crate) fn price(units: u64, size_decimals: u32) -> String {
    ExactDecimal::from_units(units, MAX_PERP_DECIMALS - size_decimals).to_string()
}

pub(crate) fn withheld<T>(withheld: &Withheld) -> LegView<T> {
    let (reason_code, reason) = (withheld.code, withheld.detail.clone());
    match withheld.status {
        Status::Unavailable => LegView::Unavailable { reason_code, reason },
        Status::Stale => LegView::Stale { reason_code, reason },
    }
}

impl<'a> CoinMarksView<'a> {
    pub(crate) fn of(snapshot: &'a LocalMarksSnapshot, coin: &'a str, marks: &'a CoinMarks) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            coin,
            interval_ms: OWNER_INTERVAL_MS,
            published_ts_ms: snapshot.computed_ms,
            node: marks.node.as_ref().map_or_else(withheld, |node| node_view(snapshot, node)),
            local: marks.local.as_ref().map_or_else(withheld, |local| local_view(snapshot, local)),
            oracle_coverage: &marks.oracle_coverage,
            external_coverage: &marks.external_coverage,
        }
    }

    /// The WS frame `{"channel":"extMarkPrices","data":<view>}`.
    #[expect(clippy::expect_used, reason = "string keys and finite numbers only; serializing cannot fail")]
    pub(crate) fn to_wire(&self) -> bytes::Bytes {
        #[derive(Serialize)]
        struct Envelope<'a, 'b> {
            channel: &'static str,
            data: &'b CoinMarksView<'a>,
        }
        let channel = crate::types::subscription::EXT_MARK_PRICES;
        bytes::Bytes::from(serde_json::to_vec(&Envelope { channel, data: self }).expect("serializable DTOs"))
    }
}

/// A ready leg reads a node state: the snapshot has its progress.
pub(crate) fn node_progress(snapshot: &LocalMarksSnapshot) -> super::snapshot::NodeProgress {
    snapshot.node.clone().unwrap_or_else(|_| unreachable!("a ready leg reads the node state"))
}

fn node_view(snapshot: &LocalMarksSnapshot, node: &NodeMark) -> LegView<NodeView> {
    let progress = node_progress(snapshot);
    let px = |units| price(units, node.size_decimals);
    LegView::Ready(NodeView {
        mark_px: px(node.mark),
        mark_time_ns: node.mark_time_ns,
        mark_update: update_label(node.mark_update),
        stored_oracle_px: px(node.stored_oracle),
        submitted_oracle_px: node.submitted.map(|(oracle, _)| px(oracle)),
        submitted_mark_px: node.submitted.map(|(_, mark)| px(mark)),
        spot_cache_px: node.spot_cache.map(px),
        spot_cache_next_refresh_ms: node.spot_cache_next_refresh_ms,
        mark_cache_px: node.mark_cache.map(px),
        mark_cache_next_refresh_ms: node.mark_cache_next_refresh_ms,
        basis_ema: EmaView::of(&node.emas.basis),
        local_price_ema: EmaView::of(&node.emas.local_price),
        last_trade_px: px(node.last_trade),
        seed_height: progress.seed_height,
        height: progress.height,
        block_time_ms: progress.block_time_ms,
    })
}

fn local_view(snapshot: &LocalMarksSnapshot, local: &LocalLeg) -> LegView<LocalView> {
    let progress = node_progress(snapshot);
    let book = snapshot.book.unwrap_or_else(|| unreachable!("a ready local leg read the book"));
    let px = |units| price(units, local.size_decimals);
    let mark = &local.mark;
    LegView::Ready(LocalView {
        reference_mark_px: px(mark.reference),
        submitted_mark_px: px(mark.submitted),
        predicted_mark_px: px(mark.predicted),
        basis_adjusted_oracle_px: px(mark.basis_adjusted_oracle),
        local_px: px(mark.local),
        external_px: px(mark.external),
        external_bid_px: local.external_quotes.bid.to_string(),
        external_ask_px: local.external_quotes.ask.to_string(),
        external_quotes: external_quotes(&local.external_quotes),
        external_basis: external_basis_view(&local.external_basis),
        oracle_px: px(mark.oracle),
        basis_ratio: mark.basis_ratio,
        basis_samples: mark.basis_samples,
        bid_px: px(local.bid),
        ask_px: px(local.ask),
        node_height: progress.height,
        node_block_time_ms: progress.block_time_ms,
        book_time_ms: book.time_ms,
        book_height: book.height,
    })
}

/// A row of `GET /extMarkReferenceCaches`: the coin's Spot and Mark reference caches with their members, of the same
/// tick and node state as its `extMarkPrices` node leg, withheld as it is.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CoinCachesView<'a> {
    schema_version: u32,
    coin: &'a str,
    published_ts_ms: u64,
    node: LegView<CachesView>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CachesView {
    spot_cache: Option<CacheView>,
    mark_cache: Option<CacheView>,
    seed_height: u64,
    height: u64,
    block_time_ms: u64,
}

/// A reference cache: its median, the unix ms from which a block refreshes it (see `extMarkPrices`), and each
/// validator's latest value by validator: a Spot member is the validator's submitted oracle, a Mark member median(A,
/// L, C) at its reference action.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CacheView {
    median_px: Option<String>,
    next_refresh_ms: u64,
    members: Vec<MemberView>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MemberView {
    validator: Address,
    px: String,
    /// Its reference action's block time, unix ns.
    block_time_ns: u64,
}

impl CacheView {
    fn of(cache: &ReferenceCache, size_decimals: u32) -> Self {
        let px = |units| price(units, size_decimals);
        let members = cache.members.iter().map(|(validator, units, block_time_ns)| MemberView {
            validator: *validator,
            px: px(*units),
            block_time_ns: *block_time_ns,
        });
        Self { median_px: cache.median.map(px), next_refresh_ms: cache.next_refresh_ms(), members: members.collect() }
    }
}

impl<'a> CoinCachesView<'a> {
    pub(crate) fn of(snapshot: &'a LocalMarksSnapshot, coin: &'a str, marks: &'a CoinMarks) -> Self {
        let node = marks.node.as_ref().map_or_else(withheld, |_| {
            let state = snapshot.node_state(coin).unwrap_or_else(|| unreachable!("a ready node leg read its node state"));
            let progress = node_progress(snapshot);
            LegView::Ready(CachesView {
                spot_cache: state.spot_cache.as_ref().map(|cache| CacheView::of(cache, state.size_decimals)),
                mark_cache: state.mark_cache.as_ref().map(|cache| CacheView::of(cache, state.size_decimals)),
                seed_height: progress.seed_height,
                height: progress.height,
                block_time_ms: progress.block_time_ms,
            })
        });
        Self { schema_version: SCHEMA_VERSION, coin, published_ts_ms: snapshot.computed_ms, node }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_marks::snapshot::tests::{NOW_MS, World};

    /// Prices are decimal text in the coin's units (BTC: one decimal, trailing zeros dropped); a withheld leg carries its
    /// status, code and reason.
    #[test]
    fn a_frame_shows_both_legs_in_prices_and_a_withheld_leg_with_its_reason() {
        let snapshot = World::new().snapshot(NOW_MS);
        let wire = CoinMarksView::of(&snapshot, "BTC", snapshot.coin("BTC").unwrap()).to_wire();
        let frame: serde_json::Value = serde_json::from_slice(&wire).unwrap();
        let data = &frame["data"];
        assert_eq!((frame["channel"].as_str(), data["schemaVersion"].as_u64(), data["intervalMs"].as_u64()), (Some("extMarkPrices"), Some(2), Some(50)));
        assert_eq!((data["coin"].as_str(), data["publishedTsMs"].as_u64()), (Some("BTC"), Some(NOW_MS)));
        let node = &data["node"];
        assert_eq!((node["status"].as_str(), node["markPx"].as_str(), node["markCachePx"].as_str()), (Some("ready"), Some("100020"), Some("100010")));
        assert_eq!((node["height"].as_u64(), node["blockTimeMs"].as_u64()), (Some(1_000), Some(NOW_MS - 500)));
        let set_ns = NOW_MS * 1_000_000 - 1_000_000_000;
        assert_eq!((node["markTimeNs"].as_u64(), node["markUpdate"].as_str()), (Some(set_ns), Some("setGlobal")));
        let submitted = (node["submittedOraclePx"].as_str(), node["submittedMarkPx"].as_str(), node["spotCachePx"].as_str());
        assert_eq!(submitted, (Some("100001"), Some("100021"), Some("100005")));
        let next_refresh_ms = (set_ns / 3_000_000_000 + 1) * 3_000;
        assert_eq!((node["spotCacheNextRefreshMs"].as_u64(), node["markCacheNextRefreshMs"].as_u64()), (Some(next_refresh_ms), Some(next_refresh_ms)));
        let basis = &node["basisEma"];
        assert_eq!((basis["numerator"].as_f64(), basis["denominatorS"].as_f64(), basis["decayParameter"].as_f64()), (Some(0.0), Some(100.0), Some(120.0)));
        assert_eq!((basis["samples"].as_u64(), basis["lastSampleTimeNs"].as_u64(), node["localPriceEma"]["decayParameter"].as_f64()), (Some(1_000), Some(set_ns), Some(120.0)));
        assert!(!String::from_utf8_lossy(&wire).contains("members"), "the frames leave the caches' members out");
        let local = &data["local"];
        assert_eq!((local["status"].as_str(), local["referenceMarkPx"].as_str(), local["predictedMarkPx"].as_str()), (Some("ready"), Some("100002"), Some("100002")));
        assert_eq!(local["submittedMarkPx"].as_str(), Some("100002"), "an integer price keeps every digit");
        assert_eq!((local["oraclePx"].as_str(), local["externalPx"].as_str(), local["bidPx"].as_str()), (Some("100000.1"), Some("100004"), Some("100001")));
        assert_eq!((local["externalBidPx"].as_str(), local["externalAskPx"].as_str()), (Some("100003.95"), Some("100004.05")), "off BTC's 0.1 grid, unrounded");
        assert_eq!(local["externalQuotes"], serde_json::json!({"binance": {"bidPx": "100003.95", "askPx": "100004.05", "receivedTsMs": NOW_MS}}));
        let basis_withheld = serde_json::json!({"status": "unavailable", "reasonCode": "externalBasisSamples", "reason": "1 of the 120 samples needed over the last 600 s"});
        assert_eq!(local["externalBasis"], basis_withheld, "the first tick's sample alone");
        assert_eq!((local["nodeHeight"].as_u64(), local["nodeBlockTimeMs"].as_u64(), local["bookTimeMs"].as_u64()), (Some(1_000), Some(NOW_MS - 500), Some(NOW_MS - 400)));
        assert_eq!(data["oracleCoverage"]["usedVenues"], serde_json::json!(["binance", "okx"]));
        let eth = CoinMarksView::of(&snapshot, "ETH", snapshot.coin("ETH").unwrap()).to_wire();
        let eth: serde_json::Value = serde_json::from_slice(&eth).unwrap();
        assert_eq!((eth["data"]["local"]["status"].as_str(), eth["data"]["local"]["reasonCode"].as_str()), (Some("unavailable"), Some("nodeCoin")));
        assert!(eth["data"]["local"]["reason"].as_str().unwrap().contains("does not claim ETH"));
        assert!(eth["data"]["local"].get("externalQuotes").is_none(), "a withheld leg has no external quotes");
        assert!(eth["data"]["local"].get("externalBasis").is_none(), "nor a basis");
    }

    /// Hyperliquid-trade design 000681 r2: `extMarkPrices`' frames byte for byte as before each venue's own basis - a
    /// ready coin's and a withheld one's, the basis withheld and ready.
    #[test]
    fn the_frames_are_byte_for_byte_as_before_the_venues_bases() {
        let world = World::new();
        let mut state = world.owner_state();
        let start = tokio::time::Instant::now();
        let first = world.snapshot_at(&mut state, NOW_MS, start);
        let wire = |snapshot: &LocalMarksSnapshot, coin: &str| String::from_utf8(CoinMarksView::of(snapshot, coin, snapshot.coin(coin).unwrap()).to_wire().to_vec()).unwrap();
        assert_eq!(wire(&first, "BTC"), GOLDEN_BTC_FIRST);
        assert_eq!(wire(&first, "ETH"), GOLDEN_ETH);
        let ready = (1..120).map(|second| world.snapshot_at(&mut state, NOW_MS, start + std::time::Duration::from_secs(second))).last().unwrap();
        assert_eq!(wire(&ready, "BTC"), GOLDEN_BTC_READY);
    }

    const GOLDEN_BTC_FIRST: &str = r#"{"channel":"extMarkPrices","data":{"schemaVersion":2,"coin":"BTC","intervalMs":50,"publishedTsMs":1000000,"node":{"status":"ready","markPx":"100020","markTimeNs":999000000000,"markUpdate":"setGlobal","storedOraclePx":"100000","submittedOraclePx":"100001","submittedMarkPx":"100021","spotCachePx":"100005","spotCacheNextRefreshMs":1002000,"markCachePx":"100010","markCacheNextRefreshMs":1002000,"basisEma":{"numerator":0.0,"denominatorS":100.0,"decayParameter":120.0,"samples":1000,"lastSampleTimeNs":999000000000},"localPriceEma":{"numerator":10000.0,"denominatorS":100.0,"decayParameter":120.0,"samples":1000,"lastSampleTimeNs":999000000000},"lastTradePx":"100002","seedHeight":900,"height":1000,"blockTimeMs":999500},"local":{"status":"ready","referenceMarkPx":"100002","submittedMarkPx":"100002","predictedMarkPx":"100002","basisAdjustedOraclePx":"100000.1","localPx":"100002","externalPx":"100004","externalBidPx":"100003.95","externalAskPx":"100004.05","externalQuotes":{"binance":{"bidPx":"100003.95","askPx":"100004.05","receivedTsMs":1000000}},"externalBasis":{"status":"unavailable","reasonCode":"externalBasisSamples","reason":"1 of the 120 samples needed over the last 600 s"},"oraclePx":"100000.1","basisRatio":1.9966722129692824e-7,"basisSamples":1001,"bidPx":"100001","askPx":"100003","nodeHeight":1000,"nodeBlockTimeMs":999500,"bookTimeMs":999600,"bookHeight":1001},"oracleCoverage":{"usedVenues":["binance","okx"],"missingVenues":[],"usedWeight":5,"configuredWeight":5},"externalCoverage":{"usedVenues":["binance"],"missingVenues":[],"usedWeight":3,"configuredWeight":3}}}"#;
    const GOLDEN_ETH: &str = r#"{"channel":"extMarkPrices","data":{"schemaVersion":2,"coin":"ETH","intervalMs":50,"publishedTsMs":1000000,"node":{"status":"unavailable","reasonCode":"nodeCoin","reason":"the node replay does not claim ETH: unsupported until the next checkpoint seed, or not listed"},"local":{"status":"unavailable","reasonCode":"nodeCoin","reason":"the node replay does not claim ETH: unsupported until the next checkpoint seed, or not listed"},"oracleCoverage":{"usedVenues":["binance"],"missingVenues":[],"usedWeight":3,"configuredWeight":3},"externalCoverage":{"usedVenues":["binance"],"missingVenues":[],"usedWeight":3,"configuredWeight":3}}}"#;
    const GOLDEN_BTC_READY: &str = r#"{"channel":"extMarkPrices","data":{"schemaVersion":2,"coin":"BTC","intervalMs":50,"publishedTsMs":1000000,"node":{"status":"ready","markPx":"100020","markTimeNs":999000000000,"markUpdate":"setGlobal","storedOraclePx":"100000","submittedOraclePx":"100001","submittedMarkPx":"100021","spotCachePx":"100005","spotCacheNextRefreshMs":1002000,"markCachePx":"100010","markCacheNextRefreshMs":1002000,"basisEma":{"numerator":0.0,"denominatorS":100.0,"decayParameter":120.0,"samples":1000,"lastSampleTimeNs":999000000000},"localPriceEma":{"numerator":10000.0,"denominatorS":100.0,"decayParameter":120.0,"samples":1000,"lastSampleTimeNs":999000000000},"lastTradePx":"100002","seedHeight":900,"height":1000,"blockTimeMs":999500},"local":{"status":"ready","referenceMarkPx":"100002","submittedMarkPx":"100002","predictedMarkPx":"100002","basisAdjustedOraclePx":"100000.1","localPx":"100002","externalPx":"100004","externalBidPx":"100003.95","externalAskPx":"100004.05","externalQuotes":{"binance":{"bidPx":"100003.95","askPx":"100004.05","receivedTsMs":1000000}},"externalBasis":{"status":"ready","ratio":-0.000019999200032039255,"samples":120},"oraclePx":"100000.1","basisRatio":1.9966722129692824e-7,"basisSamples":1001,"bidPx":"100001","askPx":"100003","nodeHeight":1000,"nodeBlockTimeMs":999500,"bookTimeMs":999600,"bookHeight":1001},"oracleCoverage":{"usedVenues":["binance","okx"],"missingVenues":[],"usedWeight":5,"configuredWeight":5},"externalCoverage":{"usedVenues":["binance"],"missingVenues":[],"usedWeight":3,"configuredWeight":3}}}"#;

    /// Design 000594: from its minimum samples on, the basis is their median and count: BTC's 100,002 / 100,004 - 1.
    #[test]
    fn a_frame_shows_the_basis_from_its_minimum_samples_on() {
        let world = World::new();
        let mut state = world.owner_state();
        let start = tokio::time::Instant::now();
        let at = |second| start + std::time::Duration::from_secs(second);
        for second in 0..119 {
            world.snapshot_at(&mut state, NOW_MS, at(second));
        }
        let snapshot = world.snapshot_at(&mut state, NOW_MS, at(119));
        let frame: serde_json::Value = serde_json::from_slice(&CoinMarksView::of(&snapshot, "BTC", snapshot.coin("BTC").unwrap()).to_wire()).unwrap();
        let basis = &frame["data"]["local"]["externalBasis"];
        assert_eq!((basis["status"].as_str(), basis["samples"].as_u64()), (Some("ready"), Some(120)));
        assert!((basis["ratio"].as_f64().unwrap() - (100_002.0 / 100_004.0 - 1.0)).abs() < 1e-15, "{basis}");
    }

    /// `GET /extMarkReferenceCaches`' rows: each cache's median, refresh time and members, of the node leg's state and
    /// withheld as it is.
    #[test]
    fn a_caches_row_shows_the_members_of_the_node_legs_state() {
        let snapshot = World::new().snapshot(NOW_MS);
        let row = serde_json::to_value(CoinCachesView::of(&snapshot, "BTC", snapshot.coin("BTC").unwrap())).unwrap();
        let node = &row["node"];
        assert_eq!((row["coin"].as_str(), row["publishedTsMs"].as_u64(), node["status"].as_str()), (Some("BTC"), Some(NOW_MS), Some("ready")));
        assert_eq!((node["seedHeight"].as_u64(), node["height"].as_u64(), node["blockTimeMs"].as_u64()), (Some(900), Some(1_000), Some(NOW_MS - 500)));
        let set_ns = NOW_MS * 1_000_000 - 1_000_000_000;
        let validator = serde_json::to_value(crate::local_marks::compute::tests::VALIDATOR).unwrap();
        let cache = |median: &str| serde_json::json!({
            "medianPx": median,
            "nextRefreshMs": (set_ns / 3_000_000_000 + 1) * 3_000,
            "members": [{"validator": validator, "px": median, "blockTimeNs": set_ns}],
        });
        assert_eq!((&node["spotCache"], &node["markCache"]), (&cache("100005"), &cache("100010")));
        let eth = serde_json::to_value(CoinCachesView::of(&snapshot, "ETH", snapshot.coin("ETH").unwrap())).unwrap();
        assert_eq!((eth["node"]["status"].as_str(), eth["node"]["reasonCode"].as_str()), (Some("unavailable"), Some("nodeCoin")));
    }
}
