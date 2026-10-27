//! `extMarkPrices` (design 000454 r1 1.4): one coin's node and local legs at a tick, the frame `data` of the WS
//! subscription and the rows of `GET /extMarkPrices`. Prices are decimal text in HL's units of the coin.
//!
//! Schema 2 (the trading system's Python `hl_mark_prices` service wrote 1): the node leg is the OB's own replay, the
//! local leg is computed here; each venue's quote is in `GET /extMarkInputs`, each reference cache's members in
//! `GET /extMarkReferenceCaches`: each reference action updates its validator's member (the median refreshes per 3 s
//! bucket), and they run to kilobytes a coin, so the frames leave them out (user 2026-10-27); a read samples the state,
//! not every member update.

use alloy::primitives::Address;
use serde::Serialize;

use super::{
    price::MAX_PERP_DECIMALS,
    snapshot::{CoinMarks, Coverage, LocalLeg, LocalMarksSnapshot, NodeMark, Status, Withheld},
};
use crate::accounts::marks::{
    decimal::ExactDecimal,
    native::MarkUpdate,
    seed::{Ema, ReferenceCache},
};

pub(crate) const SCHEMA_VERSION: u32 = 2;
/// The owner's tick: a frame of every subscribed coin each tick.
pub(crate) const INTERVAL_MS: u64 = 50;

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
            interval_ms: INTERVAL_MS,
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
        assert_eq!((local["nodeHeight"].as_u64(), local["nodeBlockTimeMs"].as_u64(), local["bookTimeMs"].as_u64()), (Some(1_000), Some(NOW_MS - 500), Some(NOW_MS - 400)));
        assert_eq!(data["oracleCoverage"]["usedVenues"], serde_json::json!(["binance", "okx"]));
        let eth = CoinMarksView::of(&snapshot, "ETH", snapshot.coin("ETH").unwrap()).to_wire();
        let eth: serde_json::Value = serde_json::from_slice(&eth).unwrap();
        assert_eq!((eth["data"]["local"]["status"].as_str(), eth["data"]["local"]["reasonCode"].as_str()), (Some("unavailable"), Some("nodeCoin")));
        assert!(eth["data"]["local"]["reason"].as_str().unwrap().contains("does not claim ETH"));
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
