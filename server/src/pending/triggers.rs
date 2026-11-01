//! Batch 2l, second step (hyperliquid-trade design 000642 r1-r3): the trigger orders a `SetGlobalAction` fires on one
//! coin, as one batch held with its line and run by the overlay at its place.
//!
//! - Which fire: the coin's trigger orders in the forced flow's frame (`trigger_frames`), against the mark forecast on
//!   the merged book at the action's place - a fall's at or above it, a rise's at or below it (HL fires on equality,
//!   research `forced_flow_trigger_timing_20261027`), one past its price at once; a trailing stop is left out. A mark
//!   past the frame's band leaves the coin's orders unknown: none run.
//! - In what order: ascending (effective size, account, oid) - the node's observed activation order (README "Observed
//!   trigger activation order"), the effective size the frame's, as the action began.
//! - At what limit: the entry's, the worst price the node runs it at (`forced_flow::trigger_limit`: a market trigger
//!   order 10% past its trigger price whatever its own limit, as an IOC; a limit one at its own, as a GTC).
//! - How much: a plain order its size; a reduce-only one its size at most what it closes of its account's position, a
//!   position TP/SL all of it - each time the batch runs, from the positions it began with, every fill of the batch
//!   moving its taker's and its known maker's position (review 000642 r1 P2-1, r3).

use alloy::primitives::Address;
use rustc_hash::FxHashMap;

use super::{
    actions::Tif,
    overlay::Fill,
    trigger_frames::{TRIGGER_TIER, TriggerFrames},
};
use crate::{
    accounts::{
        forced_flow::{OrderKind, Source},
        forced_flow_owner::ReadyFlow,
    },
    listeners::order_book::triggers::Direction,
    metrics::{PENDING_BOOK_TRIGGER_COINS_TOTAL, PENDING_BOOK_TRIGGERS_TOTAL},
    order_book::{Coin, Px, Side, Sz},
};

/// A `SetGlobalAction`'s trigger batches, one a coin that fired any, in asset order; each coin's outcome and each
/// candidate's, as the metrics count them.
#[derive(Default)]
pub(crate) struct SgaTriggers {
    pub(crate) batches: Vec<(Coin, u32, TriggerBatch)>,
    pub(crate) coins: Vec<&'static str>,
    pub(crate) candidates: Vec<&'static str>,
}

impl SgaTriggers {
    /// The batches the marks forecast on the merged book - (asset, coin, mark), in asset order - fire on `frames`'
    /// trigger orders.
    pub(crate) fn of(marks: Vec<(u32, String, Option<u64>)>, frames: &TriggerFrames) -> Self {
        let mut triggers = Self::default();
        for (asset, coin, mark) in marks {
            let outcome = match coin_fired(&coin, mark, frames) {
                Ok(Fired { batch, outcomes }) => {
                    triggers.candidates.extend(outcomes);
                    match batch {
                        Some(batch) => {
                            triggers.batches.push((Coin::new(&coin), asset, batch));
                            "executed"
                        }
                        None => "none_fired",
                    }
                }
                Err(reason) => reason,
            };
            triggers.coins.push(outcome);
        }
        triggers
    }

    pub(crate) fn counted(self) -> Vec<(Coin, u32, TriggerBatch)> {
        for outcome in self.coins {
            PENDING_BOOK_TRIGGER_COINS_TOTAL.with_label_values(&[outcome]).inc();
        }
        for outcome in self.candidates {
            PENDING_BOOK_TRIGGERS_TOTAL.with_label_values(&[outcome]).inc();
        }
        self.batches
    }
}

/// A coin's candidates against its forecast mark (price units), or why its trigger orders are unknown.
fn coin_fired(coin: &str, mark: Option<u64>, frames: &TriggerFrames) -> Result<Fired, &'static str> {
    let mark = mark.ok_or("no_forecast")?;
    let ready = frames.ready(coin)?;
    let mark_px = u128::from(mark) * 10_u128.pow(2 + ready.size_decimals);
    let mark_px = Px::new(u64::try_from(mark_px).map_err(|_| "mark_overflow")?);
    fired(ready, mark_px, TRIGGER_TIER.bps())
}

/// The book's units (1e-8) of one raw size unit (10^-szDecimals).
const fn sz_scale(size_decimals: u32) -> i128 {
    10_i128.pow(8 - size_decimals)
}

/// How a triggered order's size is set as it runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Sizing {
    /// Its own size.
    Order,
    /// Its own size, at most what it closes of its account's position.
    ReduceOnly,
    /// What it closes of its account's position (a position TP/SL, of no size).
    WholePosition,
}

/// A triggered order as it runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TriggerOrder {
    pub(crate) oid: u64,
    pub(crate) user: Address,
    pub(crate) side: Side,
    pub(crate) limit_px: Px,
    /// A market trigger order takes as an IOC; a limit one as a GTC.
    pub(crate) tif: Tif,
    pub(crate) sz: Sz,
    pub(crate) sizing: Sizing,
}

/// A `SetGlobalAction`'s triggered orders on one coin in activation order, and their accounts' positions as it began
/// (signed, 1e-8 units).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TriggerBatch {
    pub(crate) orders: Vec<TriggerOrder>,
    pub(crate) positions: Vec<(Address, i128)>,
}

/// The coin's candidates against a mark: the batch of those fired (none fired: `None`), with each candidate's outcome
/// (`fired`, `not_reached`, `trailing`).
pub(crate) struct Fired {
    pub(crate) batch: Option<TriggerBatch>,
    pub(crate) outcomes: Vec<&'static str>,
}

/// The trigger orders of `ready` a mark of `mark_px` fires, or why the coin's are unknown (`beyond_band`: the mark past
/// the frame's band of its node mark).
pub(crate) fn fired(ready: &ReadyFlow, mark_px: Px, band_bps: u32) -> Result<Fired, &'static str> {
    let step = 10_u128.pow(2 + ready.size_decimals);
    let node_px = u128::from(ready.node_mark_raw) * step;
    if u128::from(mark_px.value()).abs_diff(node_px) * 10_000 > node_px * u128::from(band_bps) {
        return Err("beyond_band");
    }
    let mut outcomes = Vec::new();
    let mut fired = Vec::new();
    for entry in ready.entries.iter().map(|flow| &flow.entry) {
        let Source::Trigger { order, kind, trigger_px } = &entry.source else { continue };
        if entry.trailing {
            outcomes.push("trailing");
            continue;
        }
        let reached = match entry.direction {
            Direction::Down => mark_px <= *trigger_px,
            Direction::Up => mark_px >= *trigger_px,
        };
        if !reached {
            outcomes.push("not_reached");
            continue;
        }
        outcomes.push("fired");
        let sizing = match (order.is_position_tpsl, order.reduce_only) {
            (true, _) => Sizing::WholePosition,
            (false, true) => Sizing::ReduceOnly,
            (false, false) => Sizing::Order,
        };
        let tif = match kind {
            OrderKind::Market => Tif::Ioc,
            OrderKind::Limit => Tif::Gtc,
        };
        let limit_px = entry.limit_px.expect("a trigger order's entry has its limit");
        let position = i128::from(entry.position_raw) * sz_scale(ready.size_decimals);
        let run = TriggerOrder { oid: order.oid, user: order.user, side: order.side, limit_px, tif, sz: order.sz, sizing };
        fired.push(((entry.size_raw, order.user, order.oid), run, position));
    }
    if fired.is_empty() {
        return Ok(Fired { batch: None, outcomes });
    }
    fired.sort_unstable_by_key(|(key, _, _)| *key);
    let mut positions: Vec<(Address, i128)> = fired.iter().map(|(_, order, position)| (order.user, *position)).collect();
    positions.sort_unstable_by_key(|(user, _)| *user);
    positions.dedup_by_key(|(user, _)| *user);
    let orders = fired.into_iter().map(|(_, order, _)| order).collect();
    Ok(Fired { batch: Some(TriggerBatch { orders, positions }), outcomes })
}

/// The batch's positions as its orders run: the one book of a fill's bearing on them (review 000642 r3).
pub(crate) struct BatchPositions(FxHashMap<Address, i128>);

impl BatchPositions {
    /// As the batch began.
    pub(crate) fn of(batch: &TriggerBatch) -> Self {
        Self(batch.positions.iter().copied().collect())
    }

    /// What `order` places now: `None` when it closes nothing.
    pub(crate) fn size(&self, order: &TriggerOrder) -> Option<Sz> {
        let position = self.0.get(&order.user).copied().unwrap_or_default();
        let closes = match order.side {
            Side::Ask => position.max(0),
            Side::Bid => (-position).max(0),
        };
        let closes = u64::try_from(closes).unwrap_or(u64::MAX);
        let size = match order.sizing {
            Sizing::Order => order.sz.value(),
            Sizing::ReduceOnly => order.sz.value().min(closes),
            Sizing::WholePosition => closes,
        };
        (size > 0).then_some(Sz::new(size))
    }

    /// A fill of the batch's order of `taker` on `side`: the taker's position and a maker's of the batch the other way.
    pub(crate) fn apply_fill(&mut self, taker: Address, side: Side, fill: &Fill) {
        let signed = i128::from(fill.sz.value()) * if side == Side::Bid { 1 } else { -1 };
        if let Some(position) = self.0.get_mut(&taker) {
            *position += signed;
        }
        if let Some(position) = fill.maker.account().and_then(|maker| self.0.get_mut(&maker)) {
            *position -= signed;
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use super::*;
    use crate::{
        accounts::{
            forced_flow::{Entry, SizeReason, trigger_limit},
            forced_flow_owner::{FlowEntry, LocalCalibre, TriggerStatus, WalkStatus},
        },
        order_book::{Coin as BookCoin, Oid},
        pending::{actions::ActionId, overlay::Maker},
        types::inner::InnerL4Order,
    };

    /// BTC's: prices in units of 0.1 (10^7 book units), sizes in units of 1e-5 (10^3 book units).
    pub(crate) const SIZE_DECIMALS: u32 = 5;

    pub(crate) fn px(text: &str) -> Px {
        Px::parse_from_str(text).unwrap()
    }

    pub(crate) fn sz(text: &str) -> Sz {
        Sz::parse_from_str(text).unwrap()
    }

    /// A trigger order of the frame: `user`'s, `side` on the fall (`Down`) or rise (`Up`) to `trigger`, `size` its own,
    /// limit `limit`; effective size and position as the frame gives them (raw: 1e-5).
    #[derive(Clone)]
    pub(crate) struct Candidate {
        pub(crate) oid: u64,
        pub(crate) user: Address,
        pub(crate) side: Side,
        pub(crate) direction: Direction,
        pub(crate) trigger: &'static str,
        pub(crate) limit: &'static str,
        pub(crate) size: &'static str,
        pub(crate) kind: OrderKind,
        pub(crate) reduce_only: bool,
        pub(crate) position_tpsl: bool,
        pub(crate) size_raw: u64,
        pub(crate) position_raw: i64,
        pub(crate) trailing: bool,
    }

    /// A plain market stop of 1 BTC selling a fall to `trigger`, of an account with no position.
    pub(crate) fn stop(oid: u64, user: u8, trigger: &'static str) -> Candidate {
        Candidate {
            oid,
            user: Address::repeat_byte(user),
            side: Side::Ask,
            direction: Direction::Down,
            trigger,
            limit: "1",
            size: "1",
            kind: OrderKind::Market,
            reduce_only: false,
            position_tpsl: false,
            size_raw: 100_000,
            position_raw: 0,
            trailing: false,
        }
    }

    pub(crate) fn entry(candidate: Candidate) -> FlowEntry {
        let order = InnerL4Order {
            user: candidate.user,
            coin: BookCoin::new("BTC"),
            side: candidate.side,
            limit_px: px(candidate.limit),
            sz: sz(candidate.size),
            oid: candidate.oid,
            timestamp: 0,
            trigger_condition: format!("Price {} {}", if candidate.direction == Direction::Down { "below" } else { "above" }, candidate.trigger),
            is_trigger: true,
            trigger_px: candidate.trigger.to_string(),
            is_position_tpsl: candidate.position_tpsl,
            reduce_only: candidate.reduce_only,
            order_type: if candidate.kind == OrderKind::Market { "Stop Market" } else { "Stop Limit" }.to_string(),
            tif: None,
            cloid: None,
        };
        let entry = Entry {
            source: Source::Trigger { order: Arc::new(order), kind: candidate.kind, trigger_px: px(candidate.trigger) },
            direction: candidate.direction,
            side: candidate.side,
            level_px: candidate.trigger.parse().unwrap(),
            distance_node_bps: 0.0,
            distance_local_bps: None,
            position_raw: candidate.position_raw,
            size_raw: candidate.size_raw,
            size_reason: SizeReason::OrderSize,
            notional: 0.0,
            limit_px: Some(trigger_limit(candidate.kind, candidate.side, px(candidate.limit), px(candidate.trigger)).unwrap()),
            trailing: candidate.trailing,
        };
        FlowEntry { entry, walk: None, cumulative: None }
    }

    /// BTC's flow: its node mark `node_mark` (a price), its trigger orders `candidates`.
    pub(crate) fn ready(node_mark: &str, candidates: Vec<Candidate>) -> ReadyFlow {
        ReadyFlow {
            size_decimals: SIZE_DECIMALS,
            node_mark_raw: px(node_mark).value() / 10_000_000,
            local: LocalCalibre::Withheld { code: "notInSources", reason: String::new() },
            triggers: TriggerStatus::Complete,
            walks: WalkStatus::NoBook,
            entries: candidates.into_iter().map(entry).collect(),
            unplaced: Vec::new(),
            cumulative: BTreeMap::new(),
        }
    }

    fn oids(fired: &Fired) -> Vec<u64> {
        fired.batch.as_ref().map_or_else(Vec::new, |batch| batch.orders.iter().map(|order| order.oid).collect())
    }

    /// A batch's order takes the entry's limit: the node's (`forced_flow::trigger_limit`), never the order's own.
    #[test]
    fn a_batch_order_takes_its_entrys_limit() {
        let sale = Candidate { trigger: "87.98", limit: "87.98", ..stop(1, 1, "87.98") };
        let ready = ReadyFlow { node_mark_raw: px("87.98").value() / 10_000_000, ..ready("1", vec![sale]) };
        let entry_limit = ready.entries[0].entry.limit_px;
        assert_eq!(entry_limit, Some(px("79.182")));
        assert_eq!(fired(&ready, px("87.98"), 50).unwrap().batch.unwrap().orders[0].limit_px, px("79.182"));
    }

    /// A fall's order fires at its price and above it, a rise's at its price and below; one the mark is past fires at
    /// once (HL fires on equality: research `forced_flow_trigger_timing_20261027`); a trailing stop is left out.
    #[test]
    fn an_order_fires_on_its_price_reached_or_passed() {
        let take_profit = Candidate { oid: 2, side: Side::Ask, direction: Direction::Up, trigger: "101", ..stop(2, 2, "101") };
        let past = stop(3, 3, "102");
        let trailing = Candidate { trailing: true, ..stop(4, 4, "100") };
        let flow = ready("100.2", vec![stop(1, 1, "100"), take_profit, past, stop(5, 5, "99.9"), trailing]);
        let at_100 = fired(&flow, px("100"), 50).unwrap();
        assert_eq!(oids(&at_100), [1, 3], "equal and past, both of size 1: by account");
        assert_eq!(at_100.outcomes, ["fired", "not_reached", "fired", "not_reached", "trailing"]);
        let at_101 = fired(&flow, px("101"), 100).unwrap();
        assert_eq!(oids(&at_101), [2, 3], "the rise's at its price; the fall's past ones");
        let none = fired(&ready("100", vec![stop(1, 1, "99")]), px("100"), 50).unwrap();
        assert!(none.batch.is_none());
    }

    /// Node mark 100, band 50 bp: 100.5 and 99.5 within (the edge in), 100.51 past it - the coin unknown, none run.
    #[test]
    fn a_mark_past_the_band_leaves_the_coin_unknown() {
        let flow = ready("100", vec![stop(1, 1, "100")]);
        assert!(fired(&flow, px("100.5"), 50).is_ok());
        assert!(fired(&flow, px("99.5"), 50).is_ok());
        assert_eq!(fired(&flow, px("100.51"), 50).err(), Some("beyond_band"));
        assert_eq!(fired(&flow, px("99.49"), 50).err(), Some("beyond_band"));
    }

    /// Ascending (effective size, account, oid), the frame's effective size as the action began; every account's
    /// position once, in 1e-8 units; a reduce-only and a position TP/SL so sized, a limit order a GTC.
    #[test]
    fn a_batch_runs_by_effective_size_then_account_then_oid() {
        let two = |oid: u64, user: u8| Candidate { size_raw: 200_000, size: "2", ..stop(oid, user, "100") };
        let reduce = Candidate { reduce_only: true, kind: OrderKind::Limit, position_raw: 300_000, ..stop(9, 3, "100") };
        let whole = Candidate { position_tpsl: true, size: "0", size_raw: 300_000, position_raw: 300_000, ..stop(8, 3, "100") };
        let flow = ready("100", vec![two(5, 2), reduce, two(7, 1), whole]);
        let batch = fired(&flow, px("99.9"), 50).unwrap().batch.unwrap();
        assert_eq!(batch.orders.iter().map(|order| (order.oid, order.user)).collect::<Vec<_>>(), [
            (9, Address::repeat_byte(3)),
            (7, Address::repeat_byte(1)),
            (5, Address::repeat_byte(2)),
            (8, Address::repeat_byte(3)),
        ]);
        assert_eq!(batch.orders.iter().map(|order| order.limit_px).collect::<Vec<_>>(), [px("1"), px("90"), px("90"), px("90")], "the limit order its own; a market sale 10% under its trigger price, its own limit (1) not");
        assert_eq!(batch.orders.iter().map(|order| (order.tif, order.sizing)).collect::<Vec<_>>(), [
            (Tif::Gtc, Sizing::ReduceOnly),
            (Tif::Ioc, Sizing::Order),
            (Tif::Ioc, Sizing::Order),
            (Tif::Ioc, Sizing::WholePosition),
        ]);
        let three = 3 * 100_000_000;
        assert_eq!(batch.positions, [(Address::repeat_byte(1), 0), (Address::repeat_byte(2), 0), (Address::repeat_byte(3), three)]);
    }

    fn sized(user: u8, side: Side, size: &str, sizing: Sizing) -> TriggerOrder {
        TriggerOrder { oid: 1, user: Address::repeat_byte(user), side, limit_px: px("100"), tif: Tif::Ioc, sz: sz(size), sizing }
    }

    /// Long 10: a reduce-only sale of 6 places 6, of 12 places 10; a position TP/SL 10; a reduce-only buy nothing; a
    /// plain order its size. A sale of 6 filled leaves 4 to close.
    #[test]
    fn a_reduce_only_or_whole_position_order_closes_what_is_left() {
        let ten = 10 * 100_000_000;
        let batch = TriggerBatch { orders: Vec::new(), positions: vec![(Address::repeat_byte(1), ten)] };
        let mut positions = BatchPositions::of(&batch);
        assert_eq!(positions.size(&sized(1, Side::Ask, "6", Sizing::ReduceOnly)), Some(sz("6")));
        assert_eq!(positions.size(&sized(1, Side::Ask, "12", Sizing::ReduceOnly)), Some(sz("10")));
        assert_eq!(positions.size(&sized(1, Side::Ask, "0", Sizing::WholePosition)), Some(sz("10")));
        assert_eq!(positions.size(&sized(1, Side::Bid, "1", Sizing::ReduceOnly)), None);
        assert_eq!(positions.size(&sized(1, Side::Bid, "3", Sizing::Order)), Some(sz("3")));
        positions.apply_fill(Address::repeat_byte(1), Side::Ask, &Fill { px: px("99"), sz: sz("6"), maker: Maker::Unattributed });
        assert_eq!(positions.size(&sized(1, Side::Ask, "0", Sizing::WholePosition)), Some(sz("4")));
    }

    /// A fill moves its taker's position and, the other way, a maker's of the batch whose account is known - a replica
    /// order's, a triggered order's remainder -; a maker of no known account, or not of the batch, moves none (review
    /// 000642 r3).
    #[test]
    fn a_fill_moves_its_taker_and_a_known_maker_of_the_batch() {
        let (taker, maker, other) = (Address::repeat_byte(1), Address::repeat_byte(2), Address::repeat_byte(3));
        let ten = 10 * 100_000_000;
        let batch = TriggerBatch { orders: Vec::new(), positions: vec![(taker, 0), (maker, ten)] };
        let id = ActionId { tx_hash: alloy::primitives::B256::ZERO, action: 0, item: 0 };
        let whole = |user: Address| TriggerOrder { user, ..sized(0, Side::Ask, "0", Sizing::WholePosition) };
        for (case, fill_maker, left) in [
            ("a replica order's", Maker::Confirmed { user: maker, oid: Oid::new(1), cloid: None }, Some(sz("9"))),
            ("a triggered remainder's", Maker::Pending { id, cloid: None, owner: Some(maker) }, Some(sz("9"))),
            ("a mempool order's", Maker::Pending { id, cloid: None, owner: None }, Some(sz("10"))),
            ("unattributed", Maker::Unattributed, Some(sz("10"))),
            ("not of the batch", Maker::Confirmed { user: other, oid: Oid::new(1), cloid: None }, Some(sz("10"))),
        ] {
            let mut positions = BatchPositions::of(&batch);
            positions.apply_fill(taker, Side::Bid, &Fill { px: px("100"), sz: sz("1"), maker: fill_maker });
            assert_eq!(positions.size(&whole(maker)), left, "{case}");
            assert_eq!(positions.size(&whole(taker)), Some(sz("1")), "{case}: the taker bought 1");
        }
    }
}
