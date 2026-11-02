//! Batch 2n-2 (hyperliquid-trade design 000659 r2 s3-s4, r3 P2-2 and P2-3, r4): the trailing stops a run of the
//! pending book stands on. The account view's node marks own each main-dex stop's activation and best (batch 2n-1);
//! the pending thread's account copy holds them at its height A, a few blocks behind the replica's R. A coin's stops in
//! the replica's trigger table are read so:
//! - none of them is known unless no mark was written in (A, R]: the block log holds each of those blocks, none holds
//!   a `SetGlobalAction`, each is before the refresh deadline (`BlockRecord::marks_unwritten`), the copy serving;
//! - one the copy holds, as it holds it (not known when it holds it so); one it does not hold - opened after A, the
//!   copy holding every stop the node marks keep - as its `open` line states it;
//! - none of a coin whose node mark the view does not claim at A, as the node marks hold them.
//!
//! The stops move in price units (10^-(6 - szDecimals)), by the node marks' own rule (`TrailingStop`).

use std::{borrow::Borrow, sync::Arc};

use rustc_hash::FxHashMap;

use super::record::BlockRecord;
use crate::{
    accounts::{
        flow_accounts::{AccountsCopy, TrailingCopy},
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
    stops: TrailingCopy,
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
        let Some(stops) = copy.trailing() else { return Self(Err("no_trailing_copy")) };
        let (height, _) = copy.positions_at();
        Self(Ok(CopyTrailing { height, stops: stops.clone(), size_decimals: copy.size_decimals_by_coin().clone() }))
    }

    /// The copy's stops as they stand at the replica's height `replica_height`: proven when no mark was written above
    /// the copy's height through it, or why not.
    pub(crate) fn at<'a>(&'a self, record: &BlockRecord, replica_height: u64) -> TrailingAt<'a> {
        let copy = self.0.as_ref().map_err(|reason| *reason);
        TrailingAt(copy.and_then(|copy| record.marks_unwritten(copy.height, replica_height, copy.stops.refresh_due_ms).map(|()| copy)))
    }
}

/// The copy's stops at the replica's height, or why they are not known there.
pub(crate) struct TrailingAt<'a>(Result<&'a CopyTrailing, &'static str>);

impl TrailingAt<'_> {
    /// `coin`'s trailing stops in `table` as a run stands on them.
    pub(crate) fn coin(&self, table: &TriggerTable, coin: &Coin) -> CoinTrailing {
        let orders = table.trailing(coin).cloned();
        let read = || {
            let copy = self.0?;
            let name: &str = coin.borrow();
            let size_decimals = *copy.size_decimals.get(name).ok_or("coin_outside_copy")?;
            // a coin whose mark the node marks do not claim: none of its stops known, as they hold them (review 000662
            // r1 P2-3, r2 P2-2: the claims are the stops' own)
            if !copy.stops.claimed.contains(name) {
                return Err("mark_unclaimed");
            }
            Ok((copy, size_decimals))
        };
        match read() {
            Ok((copy, size_decimals)) => {
                let stops = orders.map(|order| {
                    let entry = copy.entry(&order, size_decimals);
                    (order, entry)
                });
                CoinTrailing { size_decimals: Some(size_decimals), stops: stops.collect() }
            }
            Err(reason) => CoinTrailing { size_decimals: None, stops: orders.map(|order| (order, Err(reason))).collect() },
        }
    }
}

impl CopyTrailing {
    /// A table's stop: the copy's, or - opened after its height - as its line states it.
    fn entry(&self, order: &InnerL4Order, size_decimals: u32) -> TrailingEntry {
        match self.stops.stops.get(&order.oid) {
            Some((_, entry)) => *entry,
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
            flow_accounts::AccountsSnapshot,
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
