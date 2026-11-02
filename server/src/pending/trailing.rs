//! Batch 2n-2 (hyperliquid-trade design 000659 r2 s3-s4, r3 P2-2 and P2-3, r4): the trailing stops a run of the
//! pending book stands on. The account view's node marks own each main-dex stop's activation and best (batch 2n-1);
//! the pending thread's account copy holds them at its height A, a few blocks off the replica's R - behind it, or
//! ahead by one or so (bm 11-02: ahead in 11 of 60 samples). A coin's stops in the replica's trigger table are read so:
//! - none of them is known unless no mark was written between the two - above the lower through the higher: the
//!   block log holds each of those blocks, none holds a `SetGlobalAction`, each is before the refresh deadline
//!   (`BlockRecord::marks_unwritten`; batch 2n-3: the copy ahead too) -, the copy serving;
//! - one the copy holds, as it holds it (not known when it holds it so): no mark written, its state at R is its state
//!   at A - but, the copy ahead, one set after R (an `open` of its oid, the marks rebuilt or seeded, its entry
//!   forgotten) is not known there (`set_after_replica`, review 000666 r1); one it does not hold - the copy holding
//!   every stop the node marks keep -, behind R opened after A, as its `open` line states it, ahead of R gone after R
//!   (`left_after_replica`: its state at R is not held);
//! - none of a coin whose node mark the view does not claim at A, as the node marks hold them.
//!
//! The stops move in price units (10^-(6 - szDecimals)), by the node marks' own rule (`TrailingStop`).

use std::{borrow::Borrow, sync::Arc};

use rustc_hash::FxHashMap;

use super::record::BlockRecord;
use crate::{
    accounts::{
        flow_accounts::{AccountsCopy, CopiedTrailing},
        marks::trailing::{TrailingEntry, TrailingStop},
    },
    listeners::order_book::triggers::TriggerTable,
    order_book::Coin,
    types::inner::InnerL4Order,
};

/// The account copy's trailing stops as the pending thread last read them, or why none: at the copy's height, with its
/// coins' size decimals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrailingBasis(Result<CopyTrailing, &'static str>);

#[derive(Debug, Clone, PartialEq, Eq)]
struct CopyTrailing {
    height: u64,
    trailing: CopiedTrailing,
    size_decimals: FxHashMap<String, u32>,
}

impl Default for TrailingBasis {
    fn default() -> Self {
        Self(Err("account_copy_unavailable"))
    }
}

impl TrailingBasis {
    /// The copy as it serves now; `None` when it does not.
    pub(crate) fn of(copy: Option<&AccountsCopy>) -> Self {
        let Some(copy) = copy else { return Self::default() };
        let Some(trailing) = copy.trailing() else { return Self(Err("no_trailing_copy")) };
        let (height, _) = copy.positions_at();
        Self(Ok(CopyTrailing { height, trailing: trailing.clone(), size_decimals: copy.size_decimals_by_coin().clone() }))
    }

    /// The copy's stops as they stand at the replica's height `replica_height`: proven when no mark was written between
    /// the copy's height and it, or why not.
    pub(crate) fn at<'a>(&'a self, record: &BlockRecord, replica_height: u64) -> TrailingAt<'a> {
        let copy = self.0.as_ref().map_err(|reason| *reason);
        TrailingAt(copy.and_then(|copy| {
            record.marks_unwritten(copy.height, replica_height, copy.trailing.refresh_due_ms)?;
            Ok(ProvenCopy { copy, replica_height })
        }))
    }
}

/// The copy's stops at the replica's height, or why they are not known there.
pub(crate) struct TrailingAt<'a>(Result<ProvenCopy<'a>, &'static str>);

/// A copy whose stops are proven at the replica's height `replica_height`.
#[derive(Clone, Copy)]
struct ProvenCopy<'a> {
    copy: &'a CopyTrailing,
    replica_height: u64,
}

impl TrailingAt<'_> {
    /// `proven`, or why the copy's stops are not known at the replica's height: a read's result as counted.
    pub(crate) fn result(&self) -> &'static str {
        self.0.as_ref().map_or_else(|reason| *reason, |_| "proven")
    }

    /// `coin`'s trailing stops in `table` as a run stands on them.
    pub(crate) fn coin(&self, table: &TriggerTable, coin: &Coin) -> CoinTrailing {
        let orders = table.trailing(coin).cloned();
        let read = || {
            let proven = self.0?;
            let name: &str = coin.borrow();
            let size_decimals = *proven.copy.size_decimals.get(name).ok_or("coin_outside_copy")?;
            // a coin whose mark the node marks do not claim: none of its stops known, as they hold them (review 000662
            // r1 P2-3, r2 P2-2: the claims are the stops' own)
            if !proven.copy.trailing.claimed.contains(name) {
                return Err("mark_unclaimed");
            }
            Ok((proven, size_decimals))
        };
        match read() {
            Ok((proven, size_decimals)) => {
                let stops = orders.map(|order| {
                    let entry = proven.copy.entry(&order, size_decimals, proven.replica_height);
                    (order, entry)
                });
                CoinTrailing { size_decimals: Some(size_decimals), stops: stops.collect() }
            }
            Err(reason) => CoinTrailing { size_decimals: None, stops: orders.map(|order| (order, Err(reason))).collect() },
        }
    }
}

impl CopyTrailing {
    /// A table's stop at the replica's height: the copy's - not known, the copy ahead, when set after it (review 000666
    /// r1) -; one it does not hold, the copy ahead, gone since (batch 2n-3), else - opened after the copy's height - as
    /// its line states it.
    fn entry(&self, order: &InnerL4Order, size_decimals: u32, replica_height: u64) -> TrailingEntry {
        let ahead = self.height > replica_height;
        match self.trailing.stop(order.oid) {
            Some(stop) if ahead && stop.set_at > replica_height => Err("set_after_replica"),
            Some(stop) => stop.entry,
            None if ahead => Err("left_after_replica"),
            None => TrailingStop::of_condition(order.side, &order.trigger_condition, size_decimals).map_err(|_| "line_unparsed"),
        }
    }
}

/// A coin's trailing stops as a run stands on them: each of the replica's table, in oid order, with its stop or why it is
/// not known (each so when the copy's are not proven at the replica's height); the size decimals of their price units
/// while they are read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CoinTrailing {
    pub(crate) size_decimals: Option<u32>,
    pub(crate) stops: Vec<(Arc<InnerL4Order>, TrailingEntry)>,
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;

    use super::*;
    use crate::{
        accounts::{
            flow_accounts::{AccountsSnapshot, FlowAccountsEvent, TrailingChanges, TrailingCopy},
            marks::trailing::{Retracement, TrailingState},
        },
        listeners::order_book::replica::tests::block_time_ms,
        order_book::{Px, Side, Sz, multi_book::TriggerSection},
        pending::blocks::LoggedBlock,
    };

    fn sale(best: u64) -> TrailingStop {
        TrailingStop { side: Side::Ask, activation: None, retracement: Retracement::Price(50), state: TrailingState::Active { best } }
    }

    fn trailing_order(oid: u64, condition: &str) -> InnerL4Order {
        InnerL4Order {
            user: Address::with_last_byte(1),
            coin: Coin::new("BTC"),
            side: Side::Ask,
            limit_px: Px::parse_from_str("90").unwrap(),
            sz: Sz::parse_from_str("1").unwrap(),
            oid,
            timestamp: 1_000,
            trigger_condition: condition.to_string(),
            is_trigger: true,
            trigger_px: "95.0".to_string(),
            is_position_tpsl: false,
            reduce_only: false,
            order_type: "Trailing Stop Market".to_string(),
            tif: None,
            cloid: None,
        }
    }

    fn basis(height: u64, stops: &[(u64, TrailingStop)]) -> TrailingBasis {
        let stops = stops.iter().map(|(oid, stop)| (*oid, ("BTC".to_string(), Ok(*stop)))).collect();
        let snapshot = AccountsSnapshot {
            generation: 1,
            height,
            time_ms: block_time_ms(height),
            coins: FxHashMap::from_iter([("BTC".to_string(), 5)]),
            marks: FxHashMap::from_iter([("BTC".to_string(), 1_000)]),
            positions: FxHashMap::default(),
            candidates: Err("none".to_string()),
            trailing: Some(TrailingCopy { stops, refresh_due_ms: None, claimed: std::iter::once("BTC".to_string()).collect() }),
        };
        let (accounts, _validity) = crate::accounts::flow_accounts::tests::copy_of(snapshot);
        TrailingBasis::of(accounts.copy().ok())
    }

    /// The copy as of `basis`'s snapshot at `height`, then its next block, `changes` the trailing stops it changed.
    fn basis_after_block(height: u64, stops: &[(u64, TrailingStop)], changes: Option<TrailingChanges>) -> TrailingBasis {
        let stops = stops.iter().map(|(oid, stop)| (*oid, ("BTC".to_string(), Ok(*stop)))).collect();
        let snapshot = AccountsSnapshot {
            generation: 1,
            height,
            time_ms: block_time_ms(height),
            coins: FxHashMap::from_iter([("BTC".to_string(), 5)]),
            marks: FxHashMap::from_iter([("BTC".to_string(), 1_000)]),
            positions: FxHashMap::default(),
            candidates: Err("none".to_string()),
            trailing: Some(TrailingCopy { stops, refresh_due_ms: None, claimed: std::iter::once("BTC".to_string()).collect() }),
        };
        let (mut accounts, _validity) = crate::accounts::flow_accounts::tests::copy_of(snapshot);
        let next = height + 1;
        accounts.apply(FlowAccountsEvent::Block { generation: 1, height: next, time_ms: block_time_ms(next), marks: Vec::new(), positions: Vec::new(), trailing: changes });
        TrailingBasis::of(accounts.copy().ok())
    }

    fn record(through: u64, set_global_at: Option<u64>) -> BlockRecord {
        let mut record = BlockRecord::default();
        for height in 1..=through {
            record.add(LoggedBlock {
                height, time_ms: height * 100, hashes: Vec::new(), priorities: Vec::new(), taker_outcomes: Vec::new(), signer_accounts: Vec::new(),
                twap_cancels: Vec::new(), set_global: set_global_at == Some(height), read_at: std::time::Instant::now(),
            });
        }
        record
    }

    /// Design 000659 r2 s3, r3 P2-2: a stop the copy holds as it holds it, one it does not - opened after its height -
    /// as its line states it; every one not known when a block above the copy's through the replica's may have written
    /// a mark, or the copy does not serve; a coin holding none, none.
    #[test]
    fn a_coins_stops_read_from_the_copy_or_their_lines_while_proven() {
        let orders = vec![trailing_order(70, "Activation immediate, retracement 5, best 100"), trailing_order(71, "Activation immediate, retracement 5, best 120")];
        let table = TriggerTable::seeded(TriggerSection::Complete(orders), &FxHashMap::default(), Vec::new(), true, false);
        let coin = Coin::new("BTC");
        let copy = basis(10, &[(70, sale(1_100))]);
        let read = copy.at(&record(12, None), 12).coin(&table, &coin);
        let entries: Vec<(u64, TrailingEntry)> = read.stops.iter().map(|(order, entry)| (order.oid, *entry)).collect();
        assert_eq!((read.size_decimals, entries), (Some(5), vec![(70, Ok(sale(1_100))), (71, Ok(sale(1_200)))]), "71's line: best 120");
        let written = copy.at(&record(12, Some(11)), 12).coin(&table, &coin);
        assert!(written.stops.iter().all(|(_, entry)| *entry == Err("mark_written")), "{written:?}");
        let unserved = TrailingBasis::of(None).at(&record(12, None), 12).coin(&table, &coin);
        assert!(unserved.stops.iter().all(|(_, entry)| *entry == Err("account_copy_unavailable")), "{unserved:?}");
        assert_eq!(copy.at(&record(12, None), 12).coin(&table, &Coin::new("ETH")), CoinTrailing::default());
    }

    /// Batch 2n-3: the copy one block ahead of the replica - its snapshot at 12, block 13 applied -, no mark written in
    /// between - block 13 of no `SetGlobalAction` -: a stop it holds as it holds it; one the table holds and it does
    /// not, gone after R, not known. A `SetGlobalAction` in block 13: none known. Review 000666 r1 P2-1, P2-2: a stop
    /// block 13 set (an `open` of its oid, the marks rebuilt), or the copy's snapshot at 13, is not known at 12.
    #[test]
    fn a_copy_ahead_of_the_replica_reads_its_stops_while_no_mark_was_written_between() {
        let orders = vec![trailing_order(70, "Activation immediate, retracement 5, best 100"), trailing_order(71, "Activation immediate, retracement 5, best 120")];
        let table = TriggerTable::seeded(TriggerSection::Complete(orders), &FxHashMap::default(), Vec::new(), true, false);
        let coin = Coin::new("BTC");
        let entries = |at: &TrailingAt<'_>| -> Vec<(u64, TrailingEntry)> { at.coin(&table, &coin).stops.iter().map(|(order, entry)| (order.oid, *entry)).collect() };
        let copy = basis_after_block(12, &[(70, sale(1_100))], None);
        let at = copy.at(&record(13, None), 12);
        assert_eq!(at.result(), "proven");
        assert_eq!(entries(&at), [(70, Ok(sale(1_100))), (71, Err("left_after_replica"))]);
        let written = copy.at(&record(13, Some(13)), 12);
        assert_eq!(written.result(), "mark_written");
        assert!(entries(&written).iter().all(|(_, entry)| *entry == Err("mark_written")));
        let reset = TrailingChanges { updates: vec![(70, Some(("BTC".to_string(), Ok(sale(1_080)))))], refresh_due_ms: None, claimed: None };
        let set_after = basis_after_block(12, &[(70, sale(1_100))], Some(reset));
        assert_eq!(entries(&set_after.at(&record(13, None), 12)), [(70, Err("set_after_replica")), (71, Err("left_after_replica"))]);
        assert_eq!(entries(&set_after.at(&record(13, None), 13)), [(70, Ok(sale(1_080))), (71, Ok(sale(1_200)))], "at A, as held and as its line");
        let seeded_at_a = basis(13, &[(70, sale(1_100))]);
        assert_eq!(entries(&seeded_at_a.at(&record(13, None), 12))[0], (70, Err("set_after_replica")));
    }

    /// Review 000662 r1 P2-3: a coin whose node mark the view does not claim at the copy's height - its size decimals
    /// known - reads none of its stops, a new one's line neither.
    #[test]
    fn a_coin_of_no_claimed_mark_reads_no_stop() {
        let orders = vec![trailing_order(71, "Activation immediate, retracement 5, best 120")];
        let table = TriggerTable::seeded(TriggerSection::Complete(orders), &FxHashMap::default(), Vec::new(), true, false);
        let snapshot = AccountsSnapshot {
            generation: 1,
            height: 10,
            time_ms: block_time_ms(10),
            coins: FxHashMap::from_iter([("BTC".to_string(), 5)]),
            marks: FxHashMap::default(),
            positions: FxHashMap::default(),
            candidates: Err("none".to_string()),
            trailing: Some(TrailingCopy::default()),
        };
        let (accounts, _validity) = crate::accounts::flow_accounts::tests::copy_of(snapshot);
        let read = TrailingBasis::of(accounts.copy().ok()).at(&record(11, None), 11).coin(&table, &Coin::new("BTC"));
        assert_eq!(read.stops.iter().map(|(order, entry)| (order.oid, *entry)).collect::<Vec<_>>(), [(71, Err("mark_unclaimed"))]);
    }
}
