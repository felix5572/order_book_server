//! One coin's forced flow (design 000451 r1–r5): the trigger orders and the liquidations a mark move would force out,
//! as facts. Each entry has its size alone - by the node's rules, against the account's current position - and its
//! distance from the node mark and from the local predicted mark; the walks are added by the owner. No scenario: an
//! entry's size ignores every other entry, so two entries of one account may each cover the same position.
//!
//! Pure: the owner hands in the coin's trigger orders of the band, its liquidation candidates of both calibres and the
//! positions at the marks' height. Sizes are raw, 10^-szDecimals of the coin; prices USD.
//!
//! A band's cumulative size (`Band::cumulative_raw`) sums its market trigger orders and liquidations (user 2026-10-25).
//! That fixed sum is independent of entry order; actual execution can change remaining positions and book liquidity.
//! `prefix_sizes` likewise sums those entries of a path at its level or nearer, including every equal level; neither
//! walk simulates successive executions or guarantees their aggregate filled size.
//!
//! BM 2026-10-26/27 evidence fits same-action, same-coin trigger activation by effective size, then address, then oid
//! ascending (README: "Observed trigger activation order"). Accounts can interleave; the size is frozen before the
//! batch, not the remaining position after earlier triggers. This is an observed rule, not a protocol guarantee or a
//! liquidation-order rule. `path_order` remains presentation order, not an activation simulator. Limit triggers stay
//! outside cumulative walks: modeling them would also require their ordered taking/resting effects and subsequent
//! position changes, not merely sorting this list. Replay uses the node's actual stream order instead.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use alloy::primitives::Address;

use super::{candidates::ExpectedOrder, checkpoint::mark_px};
use crate::{
    listeners::order_book::triggers::{Direction, TriggerRule, stop_direction},
    order_book::{Px, Side},
    types::inner::InnerL4Order,
};

/// The listener's fixed-point unit: its prices and sizes are integers of 1e-8.
const FIXED_POINT_DECIMALS: u32 = 8;
/// The summary's distances, bp; a band holds every entry up to its edge (a negative distance included).
pub(crate) const BAND_EDGES_BPS: [u32; 9] = [1, 5, 10, 25, 50, 100, 200, 500, 1_000];

/// A subscription's cadence by its distance (r4 revision 9): up to 10 bp at most every 100 ms (the near entries move
/// with every mark), up to 50 bp every 100 ms too (bm 2026-10-23: the costly small-coin orders were reached at ~6 bp a
/// second, a 20 bp watch a second apart gained little), up to 100 bp every second, up to 1,000 bp every 5 seconds.
/// Each tier is computed for its own distance; a subscription's narrower distance filters its tier's frames, and a
/// frame goes out when the tier's content changed, so the 10 bp tier stays apart from the 50 bp one: its subscribers
/// get no frame for a change between 10 and 50 bp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Tier {
    Near,
    NearWide,
    Mid,
    Far,
}

impl Tier {
    pub(crate) const fn of(max_distance_bps: u32) -> Option<Self> {
        match max_distance_bps {
            0..=10 => Some(Self::Near),
            11..=50 => Some(Self::NearWide),
            51..=100 => Some(Self::Mid),
            101..=1_000 => Some(Self::Far),
            _ => None,
        }
    }

    pub(crate) const fn bps(self) -> u32 {
        match self {
            Self::Near => 10,
            Self::NearWide => 50,
            Self::Mid => 100,
            Self::Far => 1_000,
        }
    }

    pub(crate) const fn period(self) -> std::time::Duration {
        std::time::Duration::from_millis(match self {
            Self::Near | Self::NearWide => 100,
            Self::Mid => 1_000,
            Self::Far => 5_000,
        })
    }

    /// The near tiers start on each book revision as well as on their period (hyperliquid-trade design 000587 r2).
    pub(crate) const fn follows_book(self) -> bool {
        matches!(self, Self::Near | Self::NearWide)
    }
}

/// Market or limit once triggered, from `orderType` (bm 10-21: `Stop Market`, `Take Profit Market`, `Trailing Stop
/// Market`, `Stop Limit`, `Take Profit Limit`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OrderKind {
    Market,
    Limit,
}

impl OrderKind {
    fn of(order_type: &str) -> Option<Self> {
        if order_type.ends_with(" Market") {
            Some(Self::Market)
        } else if order_type.ends_with(" Limit") {
            Some(Self::Limit)
        } else {
            None
        }
    }
}

/// A trigger order of the band, with its trigger price as indexed.
#[derive(Debug, Clone)]
pub(crate) struct TriggerInput {
    pub(crate) trigger_px: Px,
    pub(crate) order: Arc<InnerL4Order>,
}

/// A liquidation candidate: its position, static liquidation price and next market liquidation order.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct LiquidationInput {
    pub(crate) user: Address,
    pub(crate) size_raw: i64,
    pub(crate) liquidation_px: f64,
    /// True when the account's replayed cash went through an isolated open, increase or flip since the anchor, whose margin transfer is modelled (research "逐仓成交的划转"): its isolated cash may be short and its cross pool long of the node's by the same amount, so the price may sit nearer the mark (isolated) or farther (cross) than the node's, until the next anchor.
    pub(crate) cash_estimated: bool,
    pub(crate) order: ExpectedOrder,
}

/// One coin's inputs at one height.
pub(crate) struct CoinInputs<'a> {
    pub(crate) size_decimals: u32,
    /// The node mark, 10^-(6 - szDecimals) USD.
    pub(crate) node_mark_raw: u64,
    /// The local predicted mark, same units; `None` while the local leg is withheld.
    pub(crate) local_mark_raw: Option<u64>,
    pub(crate) triggers: &'a [TriggerInput],
    pub(crate) liquidations: &'a [LiquidationInput],
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Source {
    Trigger { order: Arc<InnerL4Order>, kind: OrderKind },
    /// `order` boxed to keep the trigger entries small; `cash_estimated` as `LiquidationInput`'s.
    Liquidation { user: Address, order: Box<ExpectedOrder>, cash_estimated: bool },
}

/// The source without its details (a subscription's `sources`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum SourceKind {
    Trigger,
    Liquidation,
}

impl Source {
    pub(crate) const fn kind(&self) -> SourceKind {
        match self {
            Self::Trigger { .. } => SourceKind::Trigger,
            Self::Liquidation { .. } => SourceKind::Liquidation,
        }
    }

    pub(crate) fn user(&self) -> Address {
        match self {
            Self::Trigger { order, .. } => order.user,
            Self::Liquidation { user, .. } => *user,
        }
    }
}

/// Why an entry's size is what it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SizeReason {
    /// The order's own size: not reduce-only, or reduce-only within the position.
    OrderSize,
    /// A reduce-only order larger than the position: the position.
    CappedAtPosition,
    /// A position TP/SL: the whole position.
    WholePosition,
    /// A reduce-only order with no position on the side it reduces: nothing (the node rejects it).
    NoPositionToReduce,
    /// A liquidation: its next market order.
    LiquidationOrder,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Entry {
    pub(crate) source: Source,
    pub(crate) direction: Direction,
    /// The forced order's side: `Ask` sells into the bids.
    pub(crate) side: Side,
    /// The trigger price or the liquidation price, USD.
    pub(crate) level_px: f64,
    /// How far the mark is from the level, bp of the mark: positive while it has not reached it.
    pub(crate) distance_node_bps: f64,
    /// The same from the local predicted mark; `None` while the local leg is withheld.
    pub(crate) distance_local_bps: Option<f64>,
    /// The account's signed position at the height, raw.
    pub(crate) position_raw: i64,
    pub(crate) size_raw: u64,
    pub(crate) size_reason: SizeReason,
    /// size × level, USD.
    pub(crate) notional: f64,
    /// The worst price it takes: a trigger order's limit; a liquidation takes any.
    pub(crate) limit_px: Option<Px>,
    /// A trailing stop, at its trigger price as it stands.
    pub(crate) trailing: bool,
}

impl Entry {
    /// Within `max_bps` by either calibre (a negative distance included).
    pub(crate) fn in_range(&self, max_bps: u32) -> bool {
        let max = f64::from(max_bps);
        self.distance_node_bps <= max || self.distance_local_bps.is_some_and(|distance| distance <= max)
    }

    pub(crate) const fn distance_bps(&self, calibre: Calibre) -> Option<f64> {
        match calibre {
            Calibre::Node => Some(self.distance_node_bps),
            Calibre::Local => self.distance_local_bps,
        }
    }

    /// A market trigger order or a liquidation: what walks the book together with the others (the module's doc).
    pub(crate) const fn walks_together(&self) -> bool {
        match &self.source {
            Source::Liquidation { .. } => true,
            Source::Trigger { kind, .. } => matches!(kind, OrderKind::Market),
        }
    }
}

/// A trigger order the flow cannot place.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Unplaced {
    /// A trailing stop waiting for its activation price (`triggerPx` 0).
    TrailingStopWaiting,
    /// A `triggerCondition` of another form.
    UnknownCondition,
    /// An `orderType` neither market nor limit.
    UnknownOrderType,
}

/// One coin's entries, per direction from the nearest level on (down: the highest first; up: the lowest), equal levels
/// trigger orders first, then by oid or address: a stable order, not the node's.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CoinFlow {
    pub(crate) entries: Vec<Entry>,
    /// (oid, why), of the band's trigger orders not placed.
    pub(crate) unplaced: Vec<(u64, Unplaced)>,
}

/// `position_raw` is an account's signed position at the marks' height. `Err` when a trigger order's size is finer than
/// the coin's lot.
pub(crate) fn coin_flow(inputs: &CoinInputs<'_>, position_raw: impl Fn(Address) -> i64) -> Result<CoinFlow, String> {
    let node_mark = mark_px(inputs.node_mark_raw, inputs.size_decimals);
    let local_mark = inputs.local_mark_raw.map(|raw| mark_px(raw, inputs.size_decimals));
    let marks = Marks { node: node_mark, local: local_mark };
    let mut entries = Vec::with_capacity(inputs.triggers.len() + inputs.liquidations.len());
    let mut unplaced = Vec::new();
    for trigger in inputs.triggers {
        match trigger_entry(trigger, inputs.size_decimals, marks, position_raw(trigger.order.user))? {
            Ok(entry) => entries.push(entry),
            Err(reason) => unplaced.push((trigger.order.oid, reason)),
        }
    }
    entries.extend(inputs.liquidations.iter().map(|liquidation| liquidation_entry(liquidation, inputs.size_decimals, marks)));
    entries.sort_by(path_order);
    Ok(CoinFlow { entries, unplaced })
}

#[derive(Debug, Clone, Copy)]
struct Marks {
    node: f64,
    local: Option<f64>,
}

impl Marks {
    fn distances(self, direction: Direction, level_px: f64) -> (f64, Option<f64>) {
        (distance_bps(direction, self.node, level_px), self.local.map(|local| distance_bps(direction, local, level_px)))
    }
}

/// A trigger order's entry with its size against the current position (r2 revision 1), or why it has none.
fn trigger_entry(trigger: &TriggerInput, size_decimals: u32, marks: Marks, position_raw: i64) -> Result<Result<Entry, Unplaced>, String> {
    let order = &trigger.order;
    let Some(kind) = OrderKind::of(&order.order_type) else { return Ok(Err(Unplaced::UnknownOrderType)) };
    let (direction, trailing) = match TriggerRule::of(&order.trigger_condition) {
        TriggerRule::At(direction) => (direction, false),
        TriggerRule::Trailing if trigger.trigger_px.value() == 0 => return Ok(Err(Unplaced::TrailingStopWaiting)),
        // a trailing stop sells into a fall from its best price, buys into a rise
        TriggerRule::Trailing => (stop_direction(order.side), true),
        TriggerRule::Unknown => return Ok(Err(Unplaced::UnknownCondition)),
    };
    let (size_raw, size_reason) = trigger_size(order, order_size_raw(order, size_decimals)?, position_raw);
    let level_px = listener_px(trigger.trigger_px);
    let (distance_node_bps, distance_local_bps) = marks.distances(direction, level_px);
    Ok(Ok(Entry {
        source: Source::Trigger { order: Arc::clone(order), kind },
        direction,
        side: order.side,
        level_px,
        distance_node_bps,
        distance_local_bps,
        position_raw,
        size_raw,
        size_reason,
        notional: notional(size_raw, size_decimals, level_px),
        limit_px: Some(order.limit_px),
        trailing,
    }))
}

/// What a trigger order of `size_raw` takes of `position_raw`: a reduce-only order at most what it reduces, a position
/// TP/SL all of it, any other order its size.
const fn trigger_size(order: &InnerL4Order, size_raw: u64, position_raw: i64) -> (u64, SizeReason) {
    let reducible = reducible_raw(order.side, position_raw);
    match (order.is_position_tpsl, order.reduce_only) {
        (true, _) | (false, true) if reducible == 0 => (0, SizeReason::NoPositionToReduce),
        (true, _) => (reducible, SizeReason::WholePosition),
        (false, true) if size_raw > reducible => (reducible, SizeReason::CappedAtPosition),
        (false, _) => (size_raw, SizeReason::OrderSize),
    }
}

/// What an order of `side` can reduce of `position_raw`: a sale a long, a buy a short.
const fn reducible_raw(side: Side, position_raw: i64) -> u64 {
    match side {
        Side::Ask if position_raw > 0 => position_raw.unsigned_abs(),
        Side::Bid if position_raw < 0 => position_raw.unsigned_abs(),
        _ => 0,
    }
}

/// The order's size in the coin's raw units (a position TP/SL carries 0).
fn order_size_raw(order: &InnerL4Order, size_decimals: u32) -> Result<u64, String> {
    let scale = 10u64.pow(FIXED_POINT_DECIMALS - size_decimals);
    let units = order.sz.value();
    if !units.is_multiple_of(scale) {
        return Err(format!(
            "trigger order {} of {:?} has size {} finer than the coin's lot (szDecimals {size_decimals})",
            order.oid,
            order.coin,
            order.sz.to_str()
        ));
    }
    Ok(units / scale)
}

fn liquidation_entry(liquidation: &LiquidationInput, size_decimals: u32, marks: Marks) -> Entry {
    // a long is sold into a fall, a short bought back in a rise
    let side = if liquidation.size_raw > 0 { Side::Ask } else { Side::Bid };
    let direction = stop_direction(side);
    let (distance_node_bps, distance_local_bps) = marks.distances(direction, liquidation.liquidation_px);
    let size_raw = liquidation.order.walk_raw;
    Entry {
        source: Source::Liquidation { user: liquidation.user, order: Box::new(liquidation.order), cash_estimated: liquidation.cash_estimated },
        direction,
        side,
        level_px: liquidation.liquidation_px,
        distance_node_bps,
        distance_local_bps,
        position_raw: liquidation.size_raw,
        size_raw,
        size_reason: SizeReason::LiquidationOrder,
        notional: notional(size_raw, size_decimals, liquidation.liquidation_px),
        limit_px: None,
        trailing: false,
    }
}

fn distance_bps(direction: Direction, mark: f64, level_px: f64) -> f64 {
    let room = match direction {
        Direction::Down => mark - level_px,
        Direction::Up => level_px - mark,
    };
    room / mark * 1e4
}

#[expect(clippy::cast_precision_loss, reason = "sizes below 2^53 raw units")]
fn notional(size_raw: u64, size_decimals: u32, level_px: f64) -> f64 {
    size_raw as f64 / 10f64.powi(size_decimals.cast_signed()) * level_px
}

/// A listener price in USD.
#[expect(clippy::cast_precision_loss, reason = "prices are below 2^53 in 1e-8 units")]
fn listener_px(px: Px) -> f64 {
    px.value() as f64 / 10f64.powi(FIXED_POINT_DECIMALS.cast_signed())
}

/// Stable presentation by distance/level, not the node's trigger activation priority (see the module docs).
fn path_order(left: &Entry, right: &Entry) -> Ordering {
    let by_level = nearest_first(left.direction, left.level_px, right.level_px);
    left.direction.cmp(&right.direction).then(by_level).then(left.source.kind().cmp(&right.source.kind())).then_with(|| {
        match (&left.source, &right.source) {
            (Source::Trigger { order: left, .. }, Source::Trigger { order: right, .. }) => left.oid.cmp(&right.oid),
            _ => left.source.user().cmp(&right.source.user()),
        }
    })
}

/// Which mark a distance is from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Calibre {
    Node,
    Local,
}

/// Entries summed: how many, their sizes (raw) and notional (USD).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct Totals {
    pub(crate) count: u64,
    pub(crate) size_raw: u128,
    pub(crate) notional: f64,
}

impl Totals {
    fn add(&mut self, entry: &Entry) {
        self.count += 1;
        self.size_raw += u128::from(entry.size_raw);
        self.notional += entry.notional;
    }
}

/// One band of one (direction, side): every entry up to its edge, by source; the trigger orders also by market or
/// limit, and by reduce-only or not (two views of the same orders, not to be added).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct Band {
    pub(crate) edge_bps: u32,
    pub(crate) triggers: Totals,
    pub(crate) liquidations: Totals,
    pub(crate) market_triggers: Totals,
    pub(crate) limit_triggers: Totals,
    pub(crate) reduce_only_triggers: Totals,
    pub(crate) other_triggers: Totals,
}

impl Band {
    /// What walks the book together, raw: the market trigger orders and the liquidations (see the module's doc).
    pub(crate) const fn cumulative_raw(&self) -> u128 {
        self.market_triggers.size_raw + self.liquidations.size_raw
    }
}

/// Every source set a subscription can take (`FlowFilter::sources`: either kind, or both).
pub(crate) const SOURCE_SETS: [&[SourceKind]; 3] = [&[SourceKind::Trigger], &[SourceKind::Liquidation], &[SourceKind::Trigger, SourceKind::Liquidation]];

/// Per taker side, every cumulative size (raw, none zero) a frame up to `max_bps` can show: each band's of any
/// subscription (either calibre, every source set) and each entry's prefix.
pub(crate) fn cumulative_sizes(entries: &[Entry], max_bps: u32) -> BTreeMap<Side, BTreeSet<u128>> {
    let mut sizes: BTreeMap<Side, BTreeSet<u128>> = BTreeMap::new();
    for calibre in [Calibre::Node, Calibre::Local] {
        for sources in SOURCE_SETS {
            for ((_, side), side_bands) in bands(entries, calibre, max_bps, sources) {
                sizes.entry(side).or_default().extend(side_bands.iter().map(Band::cumulative_raw).filter(|raw| *raw > 0));
            }
        }
    }
    for (entry, prefix) in entries.iter().zip(prefix_sizes(entries)) {
        sizes.entry(entry.side).or_default().extend(prefix.filter(|raw| *raw > 0));
    }
    sizes.retain(|_, side_sizes| !side_sizes.is_empty());
    sizes
}

/// Each entry's prefix (raw), in the entries' order: the market trigger orders and liquidations of its (direction,
/// side) at its level or nearer the mark, itself and every equal level included - what a mark move reaching its level
/// fires with it, whichever the node runs first. `None` for a limit trigger order. Every entry of the path counts,
/// whatever a subscription's sources or notional.
pub(crate) fn prefix_sizes(entries: &[Entry]) -> Vec<Option<u128>> {
    let mut paths: BTreeMap<(Direction, Side), Vec<(f64, u128)>> = BTreeMap::new();
    for entry in entries.iter().filter(|entry| entry.walks_together()) {
        paths.entry((entry.direction, entry.side)).or_default().push((entry.level_px, u128::from(entry.size_raw)));
    }
    for ((direction, _), levels) in &mut paths {
        levels.sort_by(|left, right| nearest_first(*direction, left.0, right.0));
        let mut running = 0;
        for level in levels.iter_mut() {
            running += level.1;
            level.1 = running;
        }
    }
    entries
        .iter()
        .map(|entry| {
            if !entry.walks_together() {
                return None;
            }
            let levels = &paths[&(entry.direction, entry.side)];
            // the levels at or nearer than the entry's: the last of them sums its equal levels too
            let reached = levels.partition_point(|(level, _)| nearest_first(entry.direction, *level, entry.level_px) != Ordering::Greater);
            Some(levels[reached - 1].1)
        })
        .collect()
}

/// Nearer the mark first: a fall reaches the highest level first, a rise the lowest.
fn nearest_first(direction: Direction, left: f64, right: f64) -> Ordering {
    let by_level = left.total_cmp(&right);
    if direction == Direction::Down { by_level.reverse() } else { by_level }
}

/// The bands up to `max_bps` of every (direction, side) by `calibre`, of the entries of `sources`; not filtered by
/// notional (r4 revision 10). An entry without a distance in the calibre (no local mark) is in none.
pub(crate) fn bands(entries: &[Entry], calibre: Calibre, max_bps: u32, sources: &[SourceKind]) -> BTreeMap<(Direction, Side), Vec<Band>> {
    let edges: Vec<u32> = BAND_EDGES_BPS.into_iter().filter(|edge| *edge <= max_bps).collect();
    let mut out: BTreeMap<(Direction, Side), Vec<Band>> = BTreeMap::new();
    for entry in entries.iter().filter(|entry| sources.contains(&entry.source.kind())) {
        let Some(distance) = entry.distance_bps(calibre) else { continue };
        let side_bands = out
            .entry((entry.direction, entry.side))
            .or_insert_with(|| edges.iter().map(|edge| Band { edge_bps: *edge, ..Band::default() }).collect());
        for band in side_bands.iter_mut().filter(|band| distance <= f64::from(band.edge_bps)) {
            match &entry.source {
                Source::Liquidation { .. } => band.liquidations.add(entry),
                Source::Trigger { order, kind } => {
                    band.triggers.add(entry);
                    match kind {
                        OrderKind::Market => band.market_triggers.add(entry),
                        OrderKind::Limit => band.limit_triggers.add(entry),
                    }
                    if order.reduce_only {
                        band.reduce_only_triggers.add(entry);
                    } else {
                        band.other_triggers.add(entry);
                    }
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use rustc_hash::FxHashMap;

    use super::*;
    use crate::{
        accounts::cooldown::Cooldown,
        order_book::{Coin, Sz},
    };

    const UNIT: u64 = 100_000_000;
    /// szDecimals 0: a mark of 100 USD is 100 × 10^6 price units.
    const MARK_RAW: u64 = 100 * 1_000_000;

    fn user(byte: u8) -> Address {
        Address::repeat_byte(byte)
    }

    struct Order {
        user: u8,
        oid: u64,
        side: Side,
        condition: &'static str,
        trigger_px: u64,
        sz: u64,
        reduce_only: bool,
        position_tpsl: bool,
        order_type: &'static str,
    }

    fn stop(user: u8, oid: u64, side: Side, condition: &'static str, trigger_px: u64, sz: u64) -> Order {
        Order { user, oid, side, condition, trigger_px, sz, reduce_only: true, position_tpsl: false, order_type: "Stop Market" }
    }

    fn input(order: &Order) -> TriggerInput {
        TriggerInput {
            trigger_px: Px::new(order.trigger_px * UNIT),
            order: Arc::new(InnerL4Order {
                user: user(order.user),
                coin: Coin::new("BTC"),
                side: order.side,
                limit_px: Px::new(order.trigger_px * UNIT * 9 / 10),
                sz: Sz::new(order.sz),
                oid: order.oid,
                timestamp: 0,
                trigger_condition: order.condition.to_string(),
                is_trigger: true,
                trigger_px: order.trigger_px.to_string(),
                is_position_tpsl: order.position_tpsl,
                reduce_only: order.reduce_only,
                order_type: order.order_type.to_string(),
                tif: None,
                cloid: None,
            }),
        }
    }

    fn liquidation(byte: u8, size_raw: i64, liquidation_px: f64) -> LiquidationInput {
        LiquidationInput { user: user(byte), size_raw, liquidation_px, cash_estimated: false, order: ExpectedOrder::of(size_raw, MARK_RAW, Cooldown::None) }
    }

    fn build(orders: &[Order], liquidations: &[LiquidationInput], positions: &[(u8, i64)], local_mark_raw: Option<u64>) -> CoinFlow {
        let triggers: Vec<TriggerInput> = orders.iter().map(input).collect();
        let positions: FxHashMap<Address, i64> = positions.iter().map(|(byte, size)| (user(*byte), *size)).collect();
        let inputs = CoinInputs { size_decimals: 0, node_mark_raw: MARK_RAW, local_mark_raw, triggers: &triggers, liquidations };
        coin_flow(&inputs, |account| positions.get(&account).copied().unwrap_or(0)).unwrap()
    }

    /// (oid or 0 for a liquidation, size, reason).
    fn sizes(flow: &CoinFlow) -> Vec<(u64, u64, SizeReason)> {
        let id = |entry: &Entry| match &entry.source {
            Source::Trigger { order, .. } => order.oid,
            Source::Liquidation { .. } => 0,
        };
        flow.entries.iter().map(|entry| (id(entry), entry.size_raw, entry.size_reason)).collect()
    }

    /// r4 revision 7: a long of 10 with a stop of 6 at 99, a position stop at 97 and its liquidation at 95; each takes
    /// from the current position alone, the earlier ones changing nothing.
    #[test]
    fn each_entry_takes_its_size_alone_from_the_current_position() {
        let whole = Order { position_tpsl: true, ..stop(1, 2, Side::Ask, "Price below 97", 97, 0) };
        let flow = build(&[whole, stop(1, 1, Side::Ask, "Price below 99", 99, 6 * UNIT)], &[liquidation(1, 10, 95.0)], &[(1, 10)], None);
        assert_eq!(
            sizes(&flow),
            [(1, 6, SizeReason::OrderSize), (2, 10, SizeReason::WholePosition), (0, 10, SizeReason::LiquidationOrder)]
        );
        let distances: Vec<f64> = flow.entries.iter().map(|entry| entry.distance_node_bps).collect();
        assert_eq!(distances, [100.0, 300.0, 500.0]);
        assert_eq!(flow.entries.iter().map(|entry| entry.notional).collect::<Vec<_>>(), [594.0, 970.0, 950.0]);
        assert!(flow.entries.iter().all(|entry| entry.direction == Direction::Down && entry.side == Side::Ask && entry.position_raw == 10));
        assert_eq!(flow.entries[2].limit_px, None, "a liquidation takes any price");
        assert_eq!(flow.entries[0].limit_px, Some(Px::new(99 * UNIT * 9 / 10)));
    }

    #[test]
    fn a_reduce_only_order_is_capped_by_the_side_it_reduces() {
        let orders = [
            // user 1 holds 4: a sale of 6 takes 4
            stop(1, 1, Side::Ask, "Price below 99", 99, 6 * UNIT),
            // user 2 is short: a reduce-only sale reduces nothing, a reduce-only buy above does
            stop(2, 2, Side::Ask, "Price below 98", 98, UNIT),
            stop(2, 3, Side::Bid, "Price above 102", 102, 5 * UNIT),
            // user 3 holds nothing: an order that is not reduce-only takes its size
            Order { reduce_only: false, ..stop(3, 4, Side::Bid, "Price above 101", 101, 3 * UNIT) },
        ];
        let flow = build(&orders, &[], &[(1, 4), (2, -2)], None);
        assert_eq!(
            sizes(&flow),
            [
                (1, 4, SizeReason::CappedAtPosition),
                (2, 0, SizeReason::NoPositionToReduce),
                (4, 3, SizeReason::OrderSize),
                (3, 2, SizeReason::CappedAtPosition),
            ]
        );
        let directions: Vec<Direction> = flow.entries.iter().map(|entry| entry.direction).collect();
        assert_eq!(directions, [Direction::Down, Direction::Down, Direction::Up, Direction::Up], "up from the lowest level");
    }

    /// r4 revision 8: the local predicted mark gives a second distance, negative once it is past the level; either
    /// calibre within the subscription's distance takes the entry in.
    #[test]
    fn the_local_calibre_can_take_an_entry_in_alone() {
        // node 100, local 99.8: a stop at 99.85 is 15 bp away by the node, about -5 bp by the local mark
        let orders = [Order { trigger_px: 0, ..stop(1, 1, Side::Ask, "Price below 99.85", 0, UNIT) }];
        let mut trigger = input(&orders[0]);
        trigger.trigger_px = Px::new(9_985_000_000);
        let inputs = CoinInputs { size_decimals: 0, node_mark_raw: MARK_RAW, local_mark_raw: Some(99_800_000), triggers: &[trigger], liquidations: &[] };
        let flow = coin_flow(&inputs, |_| 10).unwrap();
        let entry = &flow.entries[0];
        assert!((entry.distance_node_bps - 15.0).abs() < 1e-9);
        assert!((entry.distance_local_bps.unwrap() + 5.01).abs() < 0.01, "{:?}", entry.distance_local_bps);
        assert!(entry.in_range(10) && !Entry { distance_local_bps: None, ..entry.clone() }.in_range(10));
        // review 000451 r4: node 100, local 99, a long liquidated at 89.5 is 1,050 bp by the node, 959.6 by the local
        let flow = build(&[], &[liquidation(1, 10, 89.5)], &[(1, 10)], Some(99_000_000));
        let entry = &flow.entries[0];
        assert!((entry.distance_node_bps - 1_050.0).abs() < 1e-9 && (entry.distance_local_bps.unwrap() - 959.596).abs() < 1e-3);
        assert!(entry.in_range(1_000));
    }

    /// Equal levels: trigger orders before liquidations, then by oid.
    #[test]
    fn equal_levels_take_a_stable_order() {
        let orders = [stop(1, 9, Side::Ask, "Price below 95", 95, 4 * UNIT), stop(1, 3, Side::Ask, "Price below 95", 95, 4 * UNIT)];
        let flow = build(&orders, &[liquidation(1, 10, 95.0)], &[(1, 10)], None);
        assert_eq!(sizes(&flow).iter().map(|(id, _, _)| *id).collect::<Vec<_>>(), [3, 9, 0]);
    }

    /// An activated trailing stop stands at its trigger price as it is, with its size; one waiting for activation, a
    /// condition of another form, or an order type neither market nor limit is not placed.
    #[test]
    fn trailing_stops_and_unknown_forms() {
        let orders = [
            Order { order_type: "Trailing Stop Market", ..stop(1, 1, Side::Ask, "Activation immediate, retracement 5%, best 103", 98, UNIT) },
            Order { order_type: "Trailing Stop Market", trigger_px: 0, ..stop(1, 3, Side::Ask, "Activation above 120, retracement 5%, best waiting", 0, UNIT) },
            stop(1, 4, Side::Ask, "Price near 96", 96, UNIT),
            Order { order_type: "Stop Iceberg", ..stop(1, 5, Side::Ask, "Price below 96", 96, UNIT) },
        ];
        let flow = build(&orders, &[], &[(1, 10)], None);
        assert_eq!(sizes(&flow), [(1, 1, SizeReason::OrderSize)]);
        assert!(flow.entries[0].trailing && flow.entries[0].direction == Direction::Down);
        assert_eq!(flow.unplaced, [(3, Unplaced::TrailingStopWaiting), (4, Unplaced::UnknownCondition), (5, Unplaced::UnknownOrderType)]);
    }

    #[test]
    fn a_size_finer_than_the_lot_is_an_error() {
        let triggers = [input(&stop(1, 7, Side::Ask, "Price below 99", 99, UNIT / 2))];
        let inputs = CoinInputs { size_decimals: 0, node_mark_raw: MARK_RAW, local_mark_raw: None, triggers: &triggers, liquidations: &[] };
        let error = coin_flow(&inputs, |_| 10).unwrap_err();
        assert!(error.contains("trigger order 7") && error.contains("szDecimals 0"), "{error}");
    }

    /// r4 revision 10: each band holds every entry up to its edge, by source, the trigger orders also by kind and by
    /// reduce-only; an entry without a local distance is in no local band.
    #[test]
    fn bands_sum_the_entries_up_to_each_edge() {
        let orders = [
            stop(1, 1, Side::Ask, "Price below 99.995", 0, 2 * UNIT),
            Order { order_type: "Take Profit Limit", reduce_only: false, ..stop(2, 2, Side::Ask, "Price below 99", 99, 3 * UNIT) },
        ];
        let mut triggers: Vec<TriggerInput> = orders.iter().map(input).collect();
        triggers[0].trigger_px = Px::new(9_999_500_000);
        let liquidations = [liquidation(3, 5, 98.0)];
        let inputs = CoinInputs { size_decimals: 0, node_mark_raw: MARK_RAW, local_mark_raw: None, triggers: &triggers, liquidations: &liquidations };
        let flow = coin_flow(&inputs, |account| if account == user(1) { 10 } else { 0 }).unwrap();
        let all = [SourceKind::Trigger, SourceKind::Liquidation];
        let node_bands = bands(&flow.entries, Calibre::Node, 200, &all);
        let down = &node_bands[&(Direction::Down, Side::Ask)];
        assert_eq!(down.iter().map(|band| band.edge_bps).collect::<Vec<_>>(), [1, 5, 10, 25, 50, 100, 200]);
        assert_eq!((down[0].triggers.count, down[0].triggers.size_raw), (1, 2), "1 bp: the stop at 99.995");
        assert_eq!((down[5].triggers.count, down[5].market_triggers.count, down[5].limit_triggers.count), (2, 1, 1));
        assert_eq!((down[5].reduce_only_triggers.count, down[5].other_triggers.count, down[5].liquidations.count), (1, 1, 0));
        assert_eq!((down[6].liquidations.count, down[6].liquidations.size_raw), (1, 5), "200 bp: the liquidation at 98");
        assert!(bands(&flow.entries, Calibre::Local, 200, &all).is_empty(), "no local mark");
        let liquidations_only = bands(&flow.entries, Calibre::Node, 200, &[SourceKind::Liquidation]);
        assert_eq!(liquidations_only[&(Direction::Down, Side::Ask)][6].triggers.count, 0);
        // the market stop and the liquidation walk together, the limit take profit not
        assert_eq!(down.iter().map(Band::cumulative_raw).collect::<Vec<_>>(), [2, 2, 2, 2, 2, 2, 7]);
        assert_eq!(liquidations_only[&(Direction::Down, Side::Ask)].iter().map(Band::cumulative_raw).collect::<Vec<_>>(), [0, 0, 0, 0, 0, 0, 5]);
    }

    /// Every cumulative size any subscription's bands show, by side: both source sets alone and together, none zero.
    #[test]
    fn the_cumulative_sizes_cover_every_source_set_and_calibre() {
        let orders = [stop(1, 1, Side::Ask, "Price below 99.5", 0, 2 * UNIT), stop(2, 2, Side::Bid, "Price above 100.5", 0, 4 * UNIT)];
        let mut triggers: Vec<TriggerInput> = orders.iter().map(input).collect();
        triggers[0].trigger_px = Px::new(9_950_000_000);
        triggers[1].trigger_px = Px::new(10_050_000_000);
        let liquidations = [liquidation(3, 5, 99.0)];
        let inputs = CoinInputs { size_decimals: 0, node_mark_raw: MARK_RAW, local_mark_raw: Some(MARK_RAW), triggers: &triggers, liquidations: &liquidations };
        let flow = coin_flow(&inputs, |account| if account == user(1) { 10 } else { -10 }).unwrap();
        let sizes = cumulative_sizes(&flow.entries, 200);
        // sales: the stop alone (2), the liquidation alone (5), both (7); buys: the stop (4)
        assert_eq!(sizes[&Side::Ask].iter().copied().collect::<Vec<_>>(), [2, 5, 7]);
        assert_eq!(sizes[&Side::Bid].iter().copied().collect::<Vec<_>>(), [4]);
        // within 50 bp, as the owner hands in a tier's entries: the stops, 50 bp away; the liquidation is 100 bp away
        let in_tier = |flow: &CoinFlow| flow.entries.iter().filter(|entry| entry.in_range(50)).cloned().collect::<Vec<_>>();
        let near = cumulative_sizes(&in_tier(&flow), 50);
        assert_eq!((near[&Side::Ask].iter().copied().collect::<Vec<_>>(), near[&Side::Bid].iter().copied().collect::<Vec<_>>()), (vec![2], vec![4]));
        // a local mark at 99.4: the liquidation is 40.2 bp away by it (100 bp by the node mark), the sale stop past it
        let local = CoinInputs { local_mark_raw: Some(99_400_000), ..inputs };
        let flow = coin_flow(&local, |account| if account == user(1) { 10 } else { -10 }).unwrap();
        assert_eq!(cumulative_sizes(&in_tier(&flow), 50)[&Side::Ask].iter().copied().collect::<Vec<_>>(), [2, 5, 7], "the local bands' sizes too");
    }

    /// User 2026-10-25: an entry's prefix sums the market trigger orders and liquidations of its path at its level or
    /// nearer, equal levels together, whatever comes first in the flow's order; a limit trigger order has none and adds
    /// to none; the other side and the other direction are other paths.
    #[test]
    fn a_prefix_sums_its_path_up_to_its_level() {
        let orders = [
            stop(1, 1, Side::Ask, "Price below 99.9", 0, 2 * UNIT),
            stop(1, 2, Side::Ask, "Price below 99.5", 0, 3 * UNIT),
            Order { order_type: "Stop Limit", ..stop(1, 3, Side::Ask, "Price below 99.7", 0, 4 * UNIT) },
            stop(2, 4, Side::Bid, "Price above 100.5", 0, 6 * UNIT),
        ];
        let mut triggers: Vec<TriggerInput> = orders.iter().map(input).collect();
        for (trigger, px) in triggers.iter_mut().zip([9_990_000_000, 9_950_000_000, 9_970_000_000, 10_050_000_000]) {
            trigger.trigger_px = Px::new(px);
        }
        // a liquidation at the second stop's level fires with it
        let liquidations = [liquidation(3, 1, 99.5)];
        let inputs = CoinInputs { size_decimals: 0, node_mark_raw: MARK_RAW, local_mark_raw: None, triggers: &triggers, liquidations: &liquidations };
        let flow = coin_flow(&inputs, |account| if account == user(1) { 10 } else { -10 }).unwrap();
        let prefixes: Vec<(u64, Option<u128>)> = flow
            .entries
            .iter()
            .zip(prefix_sizes(&flow.entries))
            .map(|(entry, prefix)| (if let Source::Trigger { order, .. } = &entry.source { order.oid } else { 0 }, prefix))
            .collect();
        assert_eq!(prefixes, [(1, Some(2)), (3, None), (2, Some(6)), (0, Some(6)), (4, Some(6))]);
    }

    /// Two stops between the same band edges (20 and 22 bp): the 25 bp band sums both, the nearer one's prefix is only
    /// itself - a size of no band, walked too.
    #[test]
    fn a_prefix_between_band_edges_is_walked_too() {
        let orders = [stop(1, 1, Side::Ask, "Price below 99.8", 0, 2 * UNIT), stop(1, 2, Side::Ask, "Price below 99.78", 0, 3 * UNIT)];
        let mut triggers: Vec<TriggerInput> = orders.iter().map(input).collect();
        triggers[0].trigger_px = Px::new(9_980_000_000);
        triggers[1].trigger_px = Px::new(9_978_000_000);
        let inputs = CoinInputs { size_decimals: 0, node_mark_raw: MARK_RAW, local_mark_raw: None, triggers: &triggers, liquidations: &[] };
        let flow = coin_flow(&inputs, |_| 10).unwrap();
        assert_eq!(cumulative_sizes(&flow.entries, 50)[&Side::Ask].iter().copied().collect::<Vec<_>>(), [2, 5]);
    }

    #[test]
    fn subscriptions_fall_into_three_tiers() {
        assert_eq!(
            [0, 10, 11, 50, 51, 100, 101, 1_000, 1_001].map(Tier::of),
            [Some(Tier::Near), Some(Tier::Near), Some(Tier::NearWide), Some(Tier::NearWide), Some(Tier::Mid), Some(Tier::Mid), Some(Tier::Far), Some(Tier::Far), None]
        );
        assert_eq!([Tier::Near, Tier::NearWide, Tier::Mid].map(|tier| (tier.bps(), tier.period().as_millis())), [(10, 100), (50, 100), (100, 1_000)]);
        assert_eq!(Tier::Far.period(), std::time::Duration::from_secs(5));
    }

    #[test]
    fn trigger_conditions_and_order_types_read_as_the_node_writes_them() {
        assert_eq!(TriggerRule::of("Price below 0.44152"), TriggerRule::At(Direction::Down));
        assert_eq!(TriggerRule::of("Price above 57.77"), TriggerRule::At(Direction::Up));
        assert_eq!(TriggerRule::of("Activation below 94, retracement 1.5%, best waiting"), TriggerRule::Trailing);
        assert_eq!(TriggerRule::of("N/A"), TriggerRule::Unknown);
        for (order_type, kind) in [("Stop Market", OrderKind::Market), ("Take Profit Market", OrderKind::Market), ("Trailing Stop Market", OrderKind::Market), ("Stop Limit", OrderKind::Limit), ("Take Profit Limit", OrderKind::Limit)] {
            assert_eq!(OrderKind::of(order_type), Some(kind), "{order_type}");
        }
    }
}
