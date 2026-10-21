//! `extMarkPrices` (design 000454 r1 1.4): one coin's node and local legs at a tick, the frame `data` of the WS
//! subscription and the rows of `GET /extMarkPrices`. Prices are decimal text in HL's units of the coin.
//!
//! Schema 2 (the trading system's Python `hl_mark_prices` service wrote 1): the node leg is the OB's own replay, the
//! local leg is computed here; each venue's quote is in `GET /extMarkInputs`.

use serde::Serialize;

use super::snapshot::{CoinMarks, Coverage, LocalLeg, LocalMarksSnapshot, NodeMark, Status, Withheld};
use crate::accounts::marks::decimal::ExactDecimal;

pub(crate) const SCHEMA_VERSION: u32 = 2;
/// The owner's tick: a frame of every subscribed coin each tick.
pub(crate) const INTERVAL_MS: u64 = 50;
/// A price's decimals are `MAX_PERP_DECIMALS - szDecimals`.
const MAX_PERP_DECIMALS: u32 = 6;

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
enum LegView<T> {
    Ready(T),
    #[serde(rename_all = "camelCase")]
    Unavailable { reason_code: &'static str, reason: String },
    #[serde(rename_all = "camelCase")]
    Stale { reason_code: &'static str, reason: String },
}

/// The replay's state of the coin at the node state's height.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeView {
    mark_px: String,
    stored_oracle_px: String,
    /// `null` before the Mark reference cache has a median.
    mark_cache_px: Option<String>,
    last_trade_px: String,
    seed_height: u64,
    height: u64,
    block_time_ms: u64,
}

/// The local marks, their terms and the book they read.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LocalView {
    reference_mark_px: String,
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

fn price(units: u64, size_decimals: u32) -> String {
    ExactDecimal::from_units(units, MAX_PERP_DECIMALS - size_decimals).to_string()
}

fn withheld<T>(withheld: &Withheld) -> LegView<T> {
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
fn node_progress(snapshot: &LocalMarksSnapshot) -> super::snapshot::NodeProgress {
    snapshot.node.clone().unwrap_or_else(|_| unreachable!("a ready leg reads the node state"))
}

fn node_view(snapshot: &LocalMarksSnapshot, node: &NodeMark) -> LegView<NodeView> {
    let progress = node_progress(snapshot);
    let px = |units| price(units, node.size_decimals);
    LegView::Ready(NodeView {
        mark_px: px(node.mark),
        stored_oracle_px: px(node.stored_oracle),
        mark_cache_px: node.mark_cache.map(px),
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
        let local = &data["local"];
        assert_eq!((local["status"].as_str(), local["referenceMarkPx"].as_str(), local["predictedMarkPx"].as_str()), (Some("ready"), Some("100002"), Some("100002")));
        assert_eq!((local["oraclePx"].as_str(), local["externalPx"].as_str(), local["bidPx"].as_str()), (Some("100000.1"), Some("100004"), Some("100001")));
        assert_eq!((local["nodeHeight"].as_u64(), local["nodeBlockTimeMs"].as_u64(), local["bookTimeMs"].as_u64()), (Some(1_000), Some(NOW_MS - 500), Some(NOW_MS - 400)));
        assert_eq!(data["oracleCoverage"]["usedVenues"], serde_json::json!(["binance", "okx"]));
        let eth = CoinMarksView::of(&snapshot, "ETH", snapshot.coin("ETH").unwrap()).to_wire();
        let eth: serde_json::Value = serde_json::from_slice(&eth).unwrap();
        assert_eq!((eth["data"]["local"]["status"].as_str(), eth["data"]["local"]["reasonCode"].as_str()), (Some("unavailable"), Some("nodeCoin")));
        assert!(eth["data"]["local"]["reason"].as_str().unwrap().contains("does not claim ETH"));
    }
}
