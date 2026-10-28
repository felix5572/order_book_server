//! `extHip3MarkPrices` (design 000535 r2 revision 1): one HIP-3 coin's node and local legs at a tick, the frame `data`
//! of the WS subscription and the rows of `GET /extHip3MarkPrices`. Prices are decimal text in HL's units of the coin.
//! Its own channel, so `extMarkPrices` (schema 2) keeps its frames byte for byte. Schema 2 (design 000537): (1) carries
//! the oracle's offset from the SIP midpoint, shown in the `carry*` fields.

use serde::Serialize;

use super::{
    carry::CarryStatus,
    snapshot::{Hip3CoinMarks, Hip3LocalLeg, Hip3NodeMark},
};
use crate::local_marks::{
    price::MAX_PERP_DECIMALS,
    prices_view::{INTERVAL_MS, LegView, node_progress, price, withheld},
    slot::Validity,
    snapshot::LocalMarksSnapshot,
};

pub(crate) const SCHEMA_VERSION: u32 = 2;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Hip3MarksView<'a> {
    schema_version: u32,
    coin: &'a str,
    interval_ms: u64,
    published_ts_ms: u64,
    node: LegView<NodeView>,
    local: LegView<LocalView>,
    /// The SIP quote's validity: `valid`, `notQuoted`, `noQuote` (no two-sided or a crossed NBBO), `notConnected`.
    sip_status: &'static str,
}

/// The coin's stored prices at the node state's height (the node's HIP-3 oracle stream; the last trade from its fills).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeView {
    mark_px: String,
    /// When the node set `markPx` (block time, unix ns).
    mark_time_ns: u64,
    stored_oracle_px: String,
    /// When the relayer last wrote `storedOraclePx` (block time, unix ns); `null` since the checkpoint until a write.
    stored_oracle_time_ns: Option<u64>,
    /// `null` until known.
    external_px: Option<String>,
    last_trade_px: Option<String>,
    seed_height: u64,
    height: u64,
    block_time_ms: u64,
}

/// median(oracle, ema, book), and the inputs they took.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LocalView {
    predicted_mark_px: String,
    /// (1): the SIP midpoint plus the offset carry when `carryStatus` is `ready` (`carriedPx`, else `sipMidPx`), as
    /// published, clamped to the stored oracle +-50 bp.
    oracle_px: String,
    sip_mid_px: String,
    /// The midpoint plus the carry, rounded as published, before the clamp; `null` unless `carryStatus` is `ready`.
    carried_px: Option<String>,
    /// `ready`; `warmup`: this session's writes cannot estimate the persistence yet (too few, or every offset zero);
    /// `offsetUnknown`: estimated, the last write's offset unknown.
    carry_status: &'static str,
    /// `carryRho` times `carryOffsetPx`, USD; `null` unless `ready`.
    carry_px: Option<f64>,
    /// The persistence estimated this session; `null` before.
    carry_rho: Option<f64>,
    /// This session's pairs, weighted by age.
    carry_pairs: f64,
    /// The last write's oracle minus the SIP midpoint at its read, USD; `null` unknown.
    carry_offset_px: Option<f64>,
    /// (2): the oracle plus the EMA gap; `emaGapPx` is the gap in USD, `emaSampledS` how long the EMA has sampled.
    ema_px: String,
    ema_gap_px: f64,
    ema_sampled_s: f64,
    /// (3): median(bid, ask, last trade).
    book_px: String,
    bid_px: String,
    ask_px: String,
    sip_bid_px: String,
    sip_ask_px: String,
    /// The quote's SIP time (`null` if the quote carried none) and when it arrived, unix ms.
    sip_quote_time_ms: Option<u64>,
    sip_received_ms: u64,
    node_height: u64,
    node_block_time_ms: u64,
    book_time_ms: u64,
    book_height: u64,
}

const fn carry_label(status: CarryStatus) -> &'static str {
    match status {
        CarryStatus::Ready => "ready",
        CarryStatus::Warmup => "warmup",
        CarryStatus::OffsetUnknown => "offsetUnknown",
    }
}

/// Price units of a coin in USD.
fn usd(units: f64, size_decimals: u32) -> f64 {
    units / 10_f64.powi(MAX_PERP_DECIMALS.cast_signed() - size_decimals.cast_signed())
}

const fn validity_label(validity: Validity) -> &'static str {
    match validity {
        Validity::Valid => "valid",
        Validity::NotQuoted => "notQuoted",
        Validity::NoQuote => "noQuote",
        Validity::NotConnected => "notConnected",
        Validity::Expired => "expired",
    }
}

impl<'a> Hip3MarksView<'a> {
    pub(crate) fn of(snapshot: &'a LocalMarksSnapshot, coin: &'a str, marks: &'a Hip3CoinMarks) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            coin,
            interval_ms: INTERVAL_MS,
            published_ts_ms: snapshot.computed_ms,
            node: marks.node.as_ref().map_or_else(withheld, |node| node_view(snapshot, node)),
            local: marks.local.as_ref().map_or_else(withheld, |local| local_view(snapshot, local)),
            sip_status: validity_label(marks.sip),
        }
    }

    /// The WS frame `{"channel":"extHip3MarkPrices","data":<view>}`.
    #[expect(clippy::expect_used, reason = "string keys and finite numbers only; serializing cannot fail")]
    pub(crate) fn to_wire(&self) -> bytes::Bytes {
        #[derive(Serialize)]
        struct Envelope<'a, 'b> {
            channel: &'static str,
            data: &'b Hip3MarksView<'a>,
        }
        let channel = crate::types::subscription::EXT_HIP3_MARK_PRICES;
        bytes::Bytes::from(serde_json::to_vec(&Envelope { channel, data: self }).expect("serializable DTOs"))
    }
}

fn node_view(snapshot: &LocalMarksSnapshot, node: &Hip3NodeMark) -> LegView<NodeView> {
    let progress = node_progress(snapshot);
    let px = |units| price(units, node.size_decimals);
    LegView::Ready(NodeView {
        mark_px: px(node.mark),
        mark_time_ns: node.mark_time_ns,
        stored_oracle_px: px(node.stored_oracle),
        stored_oracle_time_ns: node.oracle_time_ns,
        external_px: node.external.map(px),
        last_trade_px: node.last_trade.map(px),
        seed_height: progress.seed_height,
        height: progress.height,
        block_time_ms: progress.block_time_ms,
    })
}

fn local_view(snapshot: &LocalMarksSnapshot, local: &Hip3LocalLeg) -> LegView<LocalView> {
    let progress = node_progress(snapshot);
    let book = snapshot.book.unwrap_or_else(|| unreachable!("a ready local leg read the book"));
    let px = |units| price(units, local.size_decimals);
    let mark = &local.mark;
    let carry = &local.carry;
    LegView::Ready(LocalView {
        predicted_mark_px: px(mark.predicted),
        oracle_px: px(mark.oracle),
        sip_mid_px: px(mark.sip_mid),
        carried_px: mark.carried.map(px),
        carry_status: carry_label(carry.status),
        carry_px: carry.carry.map(|units| usd(units, local.size_decimals)),
        carry_rho: carry.rho,
        carry_pairs: carry.pairs,
        carry_offset_px: carry.offset.map(|units| usd(units, local.size_decimals)),
        ema_px: px(mark.ema),
        ema_gap_px: usd(local.gap, local.size_decimals),
        ema_sampled_s: local.ema_sampled.as_secs_f64(),
        book_px: px(mark.book),
        bid_px: px(local.bid),
        ask_px: px(local.ask),
        sip_bid_px: local.sip.bid.to_string(),
        sip_ask_px: local.sip.ask.to_string(),
        sip_quote_time_ms: local.sip_event_ms,
        sip_received_ms: local.sip_received_ms,
        node_height: progress.height,
        node_block_time_ms: progress.block_time_ms,
        book_time_ms: book.time_ms,
        book_height: book.height,
    })
}
