//! `extForcedFlow` frames (design 000451 r4 revisions 9–10): one coin's frame of a tier, as one subscription sees it -
//! its entries within the subscription's distance, of its sources and at least its notional, and the summary bands
//! of its sources (by either calibre, not filtered by notional), each with its cumulative walk (user 2026-10-25).
//! Prices in USD, sizes in the coin's units.

use std::collections::BTreeMap;

use serde::Serialize;

use super::{
    candidates_view::{ExpectedOrderDto, WalkPartDto},
    checkpoint::MAX_PERP_DECIMALS,
    forced_flow::{Band, Calibre, Entry, SizeReason, Source, SourceKind, Tier, Totals, Unplaced, bands},
    forced_flow_owner::{FlowContent, FlowFrame, FlowHeights, LocalCalibre, ReadyFlow, TriggerStatus, WalkStatus},
    size::format_size_raw,
    view::user_text,
    walk::walk_units,
};
use crate::{listeners::order_book::triggers::Direction, order_book::Side, types::subscription::EXT_FORCED_FLOW};

/// A subscription's view (r4 revision 9): `coin` one coin or every coin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FlowFilter {
    pub(crate) coin: Option<String>,
    pub(crate) max_distance_bps: u32,
    pub(crate) sources: Vec<SourceKind>,
    pub(crate) exclude_coins: Vec<String>,
    pub(crate) min_notional_usd: u64,
}

/// The notional an entry needs by default (r4 revision 10): the forced order's size × its level.
pub(crate) const DEFAULT_MIN_NOTIONAL_USD: u64 = 10_000;

impl FlowFilter {
    /// A subscription's or a query's parameters, checked: a distance up to 1,000 bp, known sources (both when absent),
    /// the coin not also excluded; no coin excluded and 10,000 USD when absent.
    pub(crate) fn of(
        coin: Option<String>,
        max_distance_bps: u32,
        sources: Option<Vec<String>>,
        exclude_coins: Option<Vec<String>>,
        min_notional_usd: Option<u64>,
    ) -> Result<Self, String> {
        if Tier::of(max_distance_bps).is_none() {
            return Err(format!("maxDistanceBps {max_distance_bps} is over 1000"));
        }
        if coin.as_deref() == Some("") {
            return Err("an empty coin".to_string());
        }
        let sources = match sources {
            None => vec![SourceKind::Trigger, SourceKind::Liquidation],
            Some(names) if names.is_empty() => return Err("sources is empty".to_string()),
            Some(names) => names
                .iter()
                .map(|name| match name.as_str() {
                    "trigger" => Ok(SourceKind::Trigger),
                    "liquidation" => Ok(SourceKind::Liquidation),
                    other => Err(format!("unknown source {other:?}: trigger or liquidation")),
                })
                .collect::<Result<Vec<_>, _>>()?,
        };
        let exclude_coins = exclude_coins.unwrap_or_default();
        if let Some(coin) = coin.as_ref().filter(|coin| exclude_coins.contains(coin)) {
            return Err(format!("{coin} is both the coin and excluded"));
        }
        Ok(Self { coin, max_distance_bps, sources, exclude_coins, min_notional_usd: min_notional_usd.unwrap_or(DEFAULT_MIN_NOTIONAL_USD) })
    }

    pub(crate) fn tier(&self) -> Tier {
        Tier::of(self.max_distance_bps).unwrap_or_else(|| unreachable!("a validated distance"))
    }

    /// Whether `frame` is of this subscription's tier and coins.
    pub(crate) fn admits(&self, frame: &FlowFrame) -> bool {
        frame.tier == self.tier() && self.coin.as_ref().is_none_or(|coin| *coin == frame.coin) && !self.exclude_coins.contains(&frame.coin)
    }

    fn takes(&self, entry: &Entry) -> bool {
        #[expect(clippy::cast_precision_loss, reason = "a notional threshold far below 2^53 USD")]
        let min = self.min_notional_usd as f64;
        entry.in_range(self.max_distance_bps) && self.sources.contains(&entry.source.kind()) && entry.notional >= min
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FlowFrameDto<'a> {
    coin: &'a str,
    tier_bps: u32,
    max_distance_bps: u32,
    seq: u64,
    heights: HeightsDto,
    #[serde(flatten)]
    body: BodyDto<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HeightsDto {
    accounts_height: u64,
    accounts_time_ms: u64,
    positions_height: u64,
    positions_time_ms: u64,
    trigger_generation: u64,
    trigger_version: u64,
    listener_height: u64,
    listener_time: u64,
    trigger_status_height: u64,
    local_published_ms: Option<u64>,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "camelCase", rename_all_fields = "camelCase")]
enum BodyDto<'a> {
    Unavailable { reason: &'a str },
    Ready(Box<ReadyDto<'a>>),
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReadyDto<'a> {
    node_mark_px: String,
    local: LocalDto<'a>,
    triggers: StatusDto<'a>,
    walks: WalksDto,
    entries: Vec<EntryDto>,
    bands: BandsDto,
    /// The band's trigger orders not placed, by why.
    unplaced: BTreeMap<&'static str, u64>,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "camelCase", rename_all_fields = "camelCase")]
enum LocalDto<'a> {
    Ready { mark_px: String },
    Withheld { reason_code: &'static str, reason: &'a str },
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
enum StatusDto<'a> {
    Complete,
    Incomplete { reason: &'a str },
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "camelCase", rename_all_fields = "camelCase")]
enum WalksDto {
    Walked { books: Vec<WalkBookDto> },
    Untrusted,
    NoBook,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WalkBookDto {
    /// The forced orders' side: `A` sells into the bids.
    taker: &'static str,
    height: u64,
    time: u64,
    mid_px: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EntryDto {
    source: &'static str,
    direction: &'static str,
    side: &'static str,
    user: String,
    level_px: f64,
    distance_node_bps: f64,
    distance_local_bps: Option<f64>,
    size: String,
    size_reason: &'static str,
    notional_usd: f64,
    position_szi: String,
    /// `null` for a liquidation, which takes any price.
    limit_px: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trigger: Option<TriggerDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expected_order: Option<ExpectedOrderDto>,
    /// A liquidation's level from a modelled cash replay (`extLiquidationCandidates`' `cashEstimated`); absent on a trigger.
    #[serde(skip_serializing_if = "Option::is_none")]
    cash_estimated: Option<bool>,
    /// `null` unless the walks are `walked`.
    walk: Option<WalkPartDto>,
    /// Its prefix walked from the side's best level: the market trigger orders and liquidations of its path at its level
    /// or nearer, itself included, every one of them whatever the subscription's sources or notional (no limit, no
    /// steps); `null` for a limit trigger order, for a prefix of nothing, and as `walk`.
    cumulative_walk: Option<WalkPartDto>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TriggerDto {
    oid: u64,
    order_type: String,
    trigger_condition: String,
    trigger_px: String,
    reduce_only: bool,
    is_position_tpsl: bool,
    trailing: bool,
    order_sz: String,
}

#[derive(Serialize)]
struct BandsDto {
    node: Vec<SideBandsDto>,
    local: Vec<SideBandsDto>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SideBandsDto {
    direction: &'static str,
    side: &'static str,
    bands: Vec<BandDto>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BandDto {
    edge_bps: u32,
    triggers: TotalsDto,
    liquidations: TotalsDto,
    market_triggers: TotalsDto,
    limit_triggers: TotalsDto,
    reduce_only_triggers: TotalsDto,
    other_triggers: TotalsDto,
    /// The band's market trigger orders and liquidations together, walked from the side's best level (no limit, no
    /// steps); `null` with neither, or unless the walks are `walked` with this side's book.
    cumulative_walk: Option<WalkPartDto>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TotalsDto {
    count: u64,
    size: String,
    notional_usd: f64,
}

const fn side_text(side: Side) -> &'static str {
    match side {
        Side::Ask => "A",
        Side::Bid => "B",
    }
}

const fn direction_text(direction: Direction) -> &'static str {
    match direction {
        Direction::Down => "down",
        Direction::Up => "up",
    }
}

const fn size_reason_text(reason: SizeReason) -> &'static str {
    match reason {
        SizeReason::OrderSize => "orderSize",
        SizeReason::CappedAtPosition => "cappedAtPosition",
        SizeReason::WholePosition => "wholePosition",
        SizeReason::NoPositionToReduce => "noPositionToReduce",
        SizeReason::LiquidationOrder => "liquidationOrder",
    }
}

const fn unplaced_text(unplaced: Unplaced) -> &'static str {
    match unplaced {
        Unplaced::TrailingStopWaiting => "trailingStopWaiting",
        Unplaced::UnknownCondition => "unknownCondition",
        Unplaced::UnknownOrderType => "unknownOrderType",
    }
}

fn mark_text(mark_raw: u64, size_decimals: u32) -> String {
    format_size_raw(mark_raw.cast_signed(), MAX_PERP_DECIMALS - size_decimals)
}

impl<'a> FlowFrameDto<'a> {
    pub(crate) fn of(frame: &'a FlowFrame, filter: &FlowFilter) -> Self {
        let FlowHeights {
            accounts_height,
            accounts_time_ms,
            positions_height,
            positions_time_ms,
            trigger_generation,
            trigger_version,
            listener_height,
            listener_time,
            trigger_status_height,
            local_published_ms,
            ..
        } = frame.heights;
        let body = match &frame.content {
            FlowContent::Unavailable(reason) => BodyDto::Unavailable { reason },
            FlowContent::Ready(ready) => BodyDto::Ready(Box::new(ready_dto(ready, &frame.heights, filter))),
        };
        Self {
            coin: &frame.coin,
            tier_bps: frame.tier.bps(),
            max_distance_bps: filter.max_distance_bps,
            seq: frame.seq,
            heights: HeightsDto {
                accounts_height,
                accounts_time_ms,
                positions_height,
                positions_time_ms,
                trigger_generation,
                trigger_version,
                listener_height,
                listener_time,
                trigger_status_height,
                local_published_ms,
            },
            body,
        }
    }

    /// The WS frame `{"channel":"extForcedFlow","data":<view>}`.
    #[expect(clippy::expect_used, reason = "string keys and finite numbers only; serializing cannot fail")]
    pub(crate) fn to_wire(&self) -> bytes::Bytes {
        #[derive(Serialize)]
        struct Envelope<'a, 'b> {
            channel: &'static str,
            data: &'b FlowFrameDto<'a>,
        }
        bytes::Bytes::from(serde_json::to_vec(&Envelope { channel: EXT_FORCED_FLOW, data: self }).expect("serializable DTOs"))
    }
}

fn ready_dto<'a>(ready: &'a ReadyFlow, heights: &FlowHeights, filter: &FlowFilter) -> ReadyDto<'a> {
    let in_range: Vec<Entry> =
        ready.entries.iter().map(|flow_entry| flow_entry.entry.clone()).filter(|entry| entry.in_range(filter.max_distance_bps)).collect();
    let mid_of = |side: Side| match &ready.walks {
        WalkStatus::Walked(books) => books.iter().find(|(taker, _)| *taker == side).and_then(|(_, mid_px)| *mid_px),
        WalkStatus::Untrusted | WalkStatus::NoBook => None,
    };
    let entries = ready
        .entries
        .iter()
        .filter(|flow_entry| filter.takes(&flow_entry.entry))
        .map(|flow_entry| {
            let mid_px = mid_of(flow_entry.entry.side);
            let walk = flow_entry.walk.as_ref().map(|walk| WalkPartDto::with_steps(walk, mid_px));
            let cumulative = flow_entry.cumulative.as_ref().map(|walk| WalkPartDto::new(walk, mid_px));
            entry_dto(&flow_entry.entry, walk, cumulative, ready.size_decimals)
        })
        .collect();
    let mut unplaced: BTreeMap<&'static str, u64> = BTreeMap::new();
    for (_, reason) in &ready.unplaced {
        *unplaced.entry(unplaced_text(*reason)).or_default() += 1;
    }
    ReadyDto {
        node_mark_px: mark_text(ready.node_mark_raw, ready.size_decimals),
        local: match &ready.local {
            LocalCalibre::Ready { mark_raw } => LocalDto::Ready { mark_px: mark_text(*mark_raw, ready.size_decimals) },
            LocalCalibre::Withheld { code, reason } => LocalDto::Withheld { reason_code: code, reason },
        },
        triggers: match &ready.triggers {
            TriggerStatus::Complete => StatusDto::Complete,
            TriggerStatus::Incomplete(reason) => StatusDto::Incomplete { reason },
        },
        walks: match &ready.walks {
            WalkStatus::Walked(books) => WalksDto::Walked {
                books: books
                    .iter()
                    .map(|(taker, mid_px)| {
                        let (height, time) = match taker {
                            Side::Ask => heights.sell_walk_book,
                            Side::Bid => heights.buy_walk_book,
                        }
                        .unwrap_or_else(|| unreachable!("a walked side has its book's heights"));
                        WalkBookDto { taker: side_text(*taker), height, time, mid_px: mid_px.map(|mid| mid.to_string()) }
                    })
                    .collect(),
            },
            WalkStatus::Untrusted => WalksDto::Untrusted,
            WalkStatus::NoBook => WalksDto::NoBook,
        },
        entries,
        bands: BandsDto { node: side_bands(&in_range, Calibre::Node, filter, ready), local: side_bands(&in_range, Calibre::Local, filter, ready) },
        unplaced,
    }
}

fn entry_dto(entry: &Entry, walk: Option<WalkPartDto>, cumulative_walk: Option<WalkPartDto>, size_decimals: u32) -> EntryDto {
    let size = |raw: u64| format_size_raw(raw.cast_signed(), size_decimals);
    let (trigger, expected_order, cash_estimated) = match &entry.source {
        Source::Trigger { order, .. } => (
            Some(TriggerDto {
                oid: order.oid,
                order_type: order.order_type.clone(),
                trigger_condition: order.trigger_condition.clone(),
                trigger_px: order.trigger_px.clone(),
                reduce_only: order.reduce_only,
                is_position_tpsl: order.is_position_tpsl,
                trailing: entry.trailing,
                order_sz: order.sz.to_str(),
            }),
            None,
            None,
        ),
        Source::Liquidation { order, cash_estimated, .. } => {
            (None, Some(ExpectedOrderDto::new(order, entry.position_raw, size_decimals)), Some(*cash_estimated))
        }
    };
    EntryDto {
        source: match entry.source.kind() {
            SourceKind::Trigger => "trigger",
            SourceKind::Liquidation => "liquidation",
        },
        direction: direction_text(entry.direction),
        side: side_text(entry.side),
        user: user_text(entry.source.user()),
        level_px: entry.level_px,
        distance_node_bps: entry.distance_node_bps,
        distance_local_bps: entry.distance_local_bps,
        size: size(entry.size_raw),
        size_reason: size_reason_text(entry.size_reason),
        notional_usd: entry.notional,
        position_szi: format_size_raw(entry.position_raw, size_decimals),
        limit_px: entry.limit_px.map(crate::order_book::Px::to_str),
        trigger,
        expected_order,
        cash_estimated,
        walk,
        cumulative_walk,
    }
}

fn side_bands(entries: &[Entry], calibre: Calibre, filter: &FlowFilter, ready: &ReadyFlow) -> Vec<SideBandsDto> {
    let size_decimals = ready.size_decimals;
    #[expect(clippy::expect_used, reason = "a coin's summed sizes stay far below 2^63 raw units")]
    let totals = |totals: Totals| TotalsDto {
        count: totals.count,
        size: format_size_raw(i64::try_from(totals.size_raw).expect("a band's total size fits i64"), size_decimals),
        notional_usd: totals.notional,
    };
    let band = |side: Side, band: &Band| BandDto {
        edge_bps: band.edge_bps,
        triggers: totals(band.triggers),
        liquidations: totals(band.liquidations),
        market_triggers: totals(band.market_triggers),
        limit_triggers: totals(band.limit_triggers),
        reduce_only_triggers: totals(band.reduce_only_triggers),
        other_triggers: totals(band.other_triggers),
        cumulative_walk: cumulative_walk(ready, side, band),
    };
    bands(entries, calibre, filter.max_distance_bps, &filter.sources)
        .iter()
        .map(|((direction, side), side_bands)| SideBandsDto {
            direction: direction_text(*direction),
            side: side_text(*side),
            bands: side_bands.iter().map(|one| band(*side, one)).collect(),
        })
        .collect()
}

/// The band's cumulative size as the owner walked it (it walks every band's of every subscription up to the tier).
fn cumulative_walk(ready: &ReadyFlow, side: Side, band: &Band) -> Option<WalkPartDto> {
    let WalkStatus::Walked(books) = &ready.walks else { return None };
    let (_, mid_px) = books.iter().find(|(taker, _)| *taker == side)?;
    let raw = band.cumulative_raw();
    if raw == 0 {
        return None;
    }
    #[expect(clippy::expect_used, reason = "a coin's summed sizes stay far below 2^63 raw units")]
    let units = walk_units(u64::try_from(raw).expect("a band's cumulative size fits u64"), ready.size_decimals);
    let walk = ready.cumulative.get(&(side, units)).unwrap_or_else(|| unreachable!("the owner walks every band's cumulative size of a walked side"));
    Some(WalkPartDto::new(walk, *mid_px))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;

    use super::*;
    use crate::accounts::{
        candidates::ExpectedOrder,
        cooldown::Cooldown,
        forced_flow::Tier,
        forced_flow_owner::{FlowEntry, ReadyFlow},
    };

    /// A long's liquidation of `size` PUMP at `level`, `distance` bp below the node mark 0.0125.
    fn liquidation(byte: u8, size: u64, level: f64, distance: f64) -> FlowEntry {
        let size_raw = size.cast_signed();
        let entry = Entry {
            source: Source::Liquidation {
                user: Address::repeat_byte(byte),
                order: Box::new(ExpectedOrder::of(size_raw, 12_500, Cooldown::None)),
                cash_estimated: false,
            },
            direction: Direction::Down,
            side: Side::Ask,
            level_px: level,
            distance_node_bps: distance,
            distance_local_bps: None,
            position_raw: size_raw,
            size_raw: size,
            size_reason: SizeReason::LiquidationOrder,
            #[expect(clippy::cast_precision_loss, reason = "test sizes")]
            notional: size as f64 * level,
            limit_px: None,
            trailing: false,
        };
        FlowEntry { entry, walk: None, cumulative: None }
    }

    fn frame(entries: Vec<FlowEntry>) -> FlowFrame {
        let ready = ReadyFlow {
            size_decimals: 0,
            node_mark_raw: 12_500,
            local: LocalCalibre::Withheld { code: "localMarksOff", reason: "off".to_string() },
            triggers: TriggerStatus::Complete,
            walks: WalkStatus::NoBook,
            entries,
            unplaced: vec![(9, Unplaced::TrailingStopWaiting)],
            cumulative: BTreeMap::new(),
        };
        FlowFrame { coin: "PUMP".to_string(), tier: Tier::Mid, seq: 4, heights: FlowHeights::default(), content: FlowContent::Ready(Box::new(ready)) }
    }

    fn view(frame: &FlowFrame, filter: &FlowFilter) -> serde_json::Value {
        serde_json::from_slice(&FlowFrameDto::of(frame, filter).to_wire()).unwrap()
    }

    /// r4 revision 10: the notional filters the entries, never the bands; the subscription's distance narrows both.
    #[test]
    fn the_notional_filters_the_entries_not_the_bands() {
        // 100,000 PUMP at 0.0124 is 1,240 USD; 1,000,000 at 0.0123 is 12,300 USD; one 150 bp away
        let frame = frame(vec![liquidation(1, 100_000, 0.0124, 80.0), liquidation(2, 1_000_000, 0.0123, 90.0), liquidation(3, 1_000_000, 0.0120, 150.0)]);
        let filter = FlowFilter::of(None, 100, None, None, None).unwrap();
        let data = &view(&frame, &filter)["data"];
        assert_eq!((data["coin"].as_str(), data["tierBps"].as_u64(), data["seq"].as_u64(), data["status"].as_str()), (Some("PUMP"), Some(100), Some(4), Some("ready")));
        let entries = data["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 1, "only the 12,300 USD one");
        assert_eq!((entries[0]["size"].as_str(), entries[0]["source"].as_str(), entries[0]["expectedOrder"]["walkSize"].as_str()), (Some("1000000.0"), Some("liquidation"), Some("1000000.0")));
        assert_eq!(entries[0]["cashEstimated"], serde_json::json!(false), "a liquidation says whether its cash is modelled");
        assert!(data["heights"]["triggerStatusHeight"].is_u64(), "the trigger table's own height beside the listener's");
        assert!(data["heights"]["positionsHeight"].is_u64() && data["heights"]["positionsTimeMs"].is_u64(), "the mark's block beside the lower bound");
        let bands = &data["bands"]["node"][0];
        assert_eq!((bands["direction"].as_str(), bands["side"].as_str()), (Some("down"), Some("A")));
        let hundred = bands["bands"].as_array().unwrap().iter().find(|band| band["edgeBps"] == 100).unwrap();
        assert_eq!((hundred["liquidations"]["count"].as_u64(), hundred["liquidations"]["size"].as_str()), (Some(2), Some("1100000.0")), "both within 100 bp");
        assert_eq!(data["bands"]["local"], serde_json::json!([]), "no local mark");
        assert_eq!(data["unplaced"], serde_json::json!({"trailingStopWaiting": 1}));
        let narrow = FlowFilter::of(None, 85, None, None, Some(0)).unwrap();
        let narrow_data = &view(&frame, &narrow)["data"];
        assert_eq!(narrow_data["entries"].as_array().unwrap().len(), 1, "85 bp, no notional floor");
        assert!(narrow_data["bands"]["node"][0]["bands"].as_array().unwrap().iter().all(|band| band["edgeBps"].as_u64().unwrap() <= 85));
    }

    /// Design 000486: a walked entry carries the levels it takes and its span from the first; one walked that took
    /// nothing an empty list and no span.
    #[test]
    fn a_walked_entry_carries_its_steps_and_span() {
        use crate::{
            accounts::walk::{WalkEnd, WalkPart, WalkStep},
            order_book::Px,
        };
        let unit = 100_000_000;
        let mut walked = liquidation(1, 1_000_000, 0.0124, 1.0);
        walked.walk = Some(WalkPart {
            filled: 1_000_000 * unit,
            vwap_px: Some(0.012_36),
            first_px: Some(Px::new(1_240_000)),
            deepest_px: Some(Px::new(1_230_000)),
            levels: 2,
            end: WalkEnd::Filled,
            steps: vec![
                WalkStep { px: Px::new(1_240_000), level_sz: 600_000 * unit, taken: 600_000 * unit },
                WalkStep { px: Px::new(1_230_000), level_sz: 1_000_000 * unit, taken: 400_000 * unit },
            ],
        });
        let mut untouched = liquidation(2, 2_000_000, 0.0124, 2.0);
        untouched.walk = Some(WalkPart { filled: 0, vwap_px: None, first_px: None, deepest_px: None, levels: 0, end: WalkEnd::BookExhausted, steps: Vec::new() });
        let filter = FlowFilter::of(None, 100, None, None, Some(0)).unwrap();
        let data = view(&frame(vec![walked, untouched]), &filter);
        let entries = data["data"]["entries"].as_array().unwrap();
        let walk = &entries[0]["walk"];
        assert_eq!((walk["firstPx"].as_str(), walk["deepestPx"].as_str(), walk["levels"].as_u64()), (Some("0.0124"), Some("0.0123"), Some(2)));
        assert!((walk["spanBps"].as_f64().unwrap() - 80.645_161).abs() < 1e-3, "0.0124 to 0.0123");
        assert_eq!(
            walk["steps"],
            serde_json::json!([{"px": "0.0124", "levelSize": "600000", "takenSize": "600000"}, {"px": "0.0123", "levelSize": "1000000", "takenSize": "400000"}])
        );
        assert_eq!((&entries[1]["walk"]["steps"], &entries[1]["walk"]["spanBps"]), (&serde_json::json!([]), &serde_json::Value::Null), "walked, nothing taken");
    }

    /// User 2026-10-25: each band carries the walk of its market trigger orders and liquidations together, as the
    /// owner walked that size; a band with neither, a source set without them, or unwalked books carry none.
    #[test]
    fn a_band_carries_its_cumulative_walk() {
        use crate::{
            accounts::walk::{WalkEnd, WalkPart},
            order_book::Px,
        };
        let unit = 100_000_000;
        let mut walked = frame(vec![liquidation(1, 100_000, 0.0124, 80.0), liquidation(2, 1_000_000, 0.0123, 90.0)]);
        walked.heights.sell_walk_book = Some((7, 5));
        let FlowContent::Ready(ready) = &mut walked.content else { unreachable!("a ready frame") };
        ready.walks = WalkStatus::Walked(vec![(Side::Ask, Some(0.0125))]);
        let both = WalkPart {
            filled: 1_100_000 * unit,
            vwap_px: Some(0.012_3),
            first_px: Some(Px::new(1_240_000)),
            deepest_px: Some(Px::new(1_220_000)),
            levels: 3,
            end: WalkEnd::Filled,
            steps: Vec::new(),
        };
        ready.cumulative.insert((Side::Ask, 1_100_000 * unit), both);
        let filter = FlowFilter::of(None, 100, None, None, Some(0)).unwrap();
        let bands = view(&walked, &filter)["data"]["bands"]["node"][0]["bands"].clone();
        let at = |edge: u64| bands.as_array().unwrap().iter().find(|band| band["edgeBps"] == edge).unwrap()["cumulativeWalk"].clone();
        assert_eq!(at(50), serde_json::Value::Null, "nothing within 50 bp");
        let hundred = at(100);
        assert_eq!((hundred["filledSize"].as_str(), hundred["firstPx"].as_str(), hundred["deepestPx"].as_str()), (Some("1100000"), Some("0.0124"), Some("0.0122")));
        assert_eq!((hundred["levels"].as_u64(), hundred["end"].as_str(), hundred.get("steps")), (Some(3), Some("filled"), None));
        assert!((hundred["deepestBpsFromMid"].as_f64().unwrap() - 240.0).abs() < 1e-6, "0.0122 against the mid 0.0125");
        let only_triggers = FlowFilter::of(None, 100, Some(vec!["trigger".to_string()]), None, Some(0)).unwrap();
        assert_eq!(view(&walked, &only_triggers)["data"]["bands"]["node"], serde_json::json!([]), "no trigger order, no band");
        let unwalked = frame(vec![liquidation(2, 1_000_000, 0.0123, 90.0)]);
        let bands = view(&unwalked, &filter)["data"]["bands"]["node"][0]["bands"].clone();
        assert!(bands.as_array().unwrap().iter().all(|band| band["cumulativeWalk"].is_null()), "no book walked");
        // only the asks the buys take were walked: the sales' bands have no walk
        let mut buys_only = frame(vec![liquidation(2, 1_000_000, 0.0123, 90.0)]);
        buys_only.heights.buy_walk_book = Some((7, 5));
        let FlowContent::Ready(ready) = &mut buys_only.content else { unreachable!("a ready frame") };
        ready.walks = WalkStatus::Walked(vec![(Side::Bid, Some(0.0125))]);
        let bands = view(&buys_only, &filter)["data"]["bands"]["node"][0]["bands"].clone();
        assert!(bands.as_array().unwrap().iter().all(|band| band["cumulativeWalk"].is_null()), "the other side's book");
    }

    /// User 2026-10-25: an entry carries its prefix's walk, without steps; one without a prefix walk none.
    #[test]
    fn an_entry_carries_its_prefix_walk() {
        use crate::{
            accounts::walk::{WalkEnd, WalkPart},
            order_book::Px,
        };
        let unit = 100_000_000;
        let mut nearer = liquidation(1, 1_000_000, 0.0124, 80.0);
        nearer.cumulative = Some(WalkPart {
            filled: 1_000_000 * unit,
            vwap_px: Some(0.012_35),
            first_px: Some(Px::new(1_240_000)),
            deepest_px: Some(Px::new(1_230_000)),
            levels: 2,
            end: WalkEnd::Filled,
            steps: Vec::new(),
        });
        let without = liquidation(2, 1_000_000, 0.0123, 90.0);
        let mut walked = frame(vec![nearer, without]);
        walked.heights.sell_walk_book = Some((7, 5));
        let FlowContent::Ready(ready) = &mut walked.content else { unreachable!("a ready frame") };
        ready.walks = WalkStatus::Walked(vec![(Side::Ask, Some(0.0125))]);
        // the 100 bp band's size, as the owner walks every band's
        ready.cumulative.insert((Side::Ask, 2_000_000 * unit), WalkPart { filled: 2_000_000 * unit, ..ready.entries[0].cumulative.clone().unwrap() });
        let filter = FlowFilter::of(None, 100, None, None, Some(0)).unwrap();
        let data = view(&walked, &filter);
        let entries = data["data"]["entries"].as_array().unwrap();
        let prefix = &entries[0]["cumulativeWalk"];
        assert_eq!((prefix["filledSize"].as_str(), prefix["deepestPx"].as_str(), prefix.get("steps")), (Some("1000000"), Some("0.0123"), None));
        assert_eq!(entries[1]["cumulativeWalk"], serde_json::Value::Null);
    }

    #[test]
    fn a_subscription_admits_its_tier_and_coins_only() {
        let empty = frame(Vec::new());
        assert!(FlowFilter::of(None, 100, None, None, None).unwrap().admits(&empty));
        assert!(!FlowFilter::of(None, 10, None, None, None).unwrap().admits(&empty), "another tier");
        assert!(!FlowFilter::of(None, 50, None, None, None).unwrap().admits(&empty), "the 50 bp tier, not the 100 bp one");
        assert!(!FlowFilter::of(Some("BTC".to_string()), 100, None, None, None).unwrap().admits(&empty));
        assert!(!FlowFilter::of(None, 100, None, Some(vec!["PUMP".to_string()]), None).unwrap().admits(&empty), "excluded");
        let only_triggers = FlowFilter::of(None, 100, Some(vec!["trigger".to_string()]), None, Some(0)).unwrap();
        let with_entry = frame(vec![liquidation(1, 1_000_000, 0.0123, 90.0)]);
        assert_eq!(view(&with_entry, &only_triggers)["data"]["entries"], serde_json::json!([]));
    }

    #[test]
    fn parameters_are_checked() {
        let error = |coin: Option<&str>, max: u32, sources: Option<Vec<&str>>, exclude: Option<Vec<&str>>| {
            let owned = |list: Option<Vec<&str>>| list.map(|list| list.into_iter().map(str::to_string).collect());
            FlowFilter::of(coin.map(str::to_string), max, owned(sources), owned(exclude), None).unwrap_err()
        };
        assert_eq!(error(None, 1_001, None, None), "maxDistanceBps 1001 is over 1000");
        assert_eq!(error(Some(""), 10, None, None), "an empty coin");
        assert_eq!(error(None, 10, Some(Vec::new()), None), "sources is empty");
        assert_eq!(error(None, 10, Some(vec!["stops"]), None), "unknown source \"stops\": trigger or liquidation");
        assert_eq!(error(Some("BTC"), 10, None, Some(vec!["BTC"])), "BTC is both the coin and excluded");
        let defaults = FlowFilter::of(None, 10, None, None, None).unwrap();
        assert_eq!((defaults.sources.len(), defaults.exclude_coins.len(), defaults.min_notional_usd), (2, 0, DEFAULT_MIN_NOTIONAL_USD));
    }
}
