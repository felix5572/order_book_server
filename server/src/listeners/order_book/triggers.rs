//! The untriggered trigger orders (stops / TP-SL waiting for their trigger price), one owner (design 000451 r3): the
//! orders per coin, an index by trigger price, a change version per coin, and whether the table is complete.
//!
//! These orders never rest on the book. A snapshot install seeds the table from the dump's trigger section
//! (`--include-trigger-orders`); live `open` statuses with `isTrigger` add or replace an order, and any other status of
//! its oid removes it (canceled / triggered / filled / rejected / ...). Orders are `Arc`'d so a read under the listener
//! lock is a refcount bump per order, not a deep clone.
//!
//! Complete means: the last install's dump carried the section for every market and every order of it converted
//! (an empty section is a complete empty table), and every live trigger order since parsed. Anything else is a gap,
//! kept until the next complete install; the orders the table does hold are still served (`/untriggeredOrders`).

use std::{borrow::Borrow, collections::BTreeMap, ops::Bound, sync::Arc};

use rustc_hash::FxHashMap;

use crate::{
    order_book::{Coin, InnerOrder, Oid, Px, multi_book::TriggerSection},
    prelude::Result,
    types::inner::InnerL4Order,
};

/// Why the table may miss orders.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TriggerGap {
    /// The table is not kept (`--bbo-only`), or this coin's market is not (a spot coin under the spot filter).
    TrackingDisabled,
    /// The last install's dump carried no trigger section for some market (for none: the previous table's orders were
    /// kept).
    SnapshotWithoutTriggers,
    /// An order of the last install's trigger section did not convert.
    SnapshotParseFailed,
    /// A live trigger order did not parse.
    LiveParseFailed,
}

impl TriggerGap {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::TrackingDisabled => "trackingDisabled",
            Self::SnapshotWithoutTriggers => "snapshotWithoutTriggers",
            Self::SnapshotParseFailed => "snapshotParseFailed",
            Self::LiveParseFailed => "liveParseFailed",
        }
    }
}

/// Whether the orders the table holds are all the standing ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TriggerAvailability {
    Complete,
    Gap(TriggerGap),
}

/// One coin's trigger orders with a trigger price in a band.
#[derive(Debug, Clone)]
pub(crate) struct TriggerBand {
    pub(crate) availability: TriggerAvailability,
    /// Installs since start: a new install renumbers every coin's version.
    pub(crate) generation: u64,
    /// Changes of this coin's orders within the generation.
    pub(crate) version: u64,
    /// By trigger price, then oid, with the trigger price as indexed; at most the requested count.
    pub(crate) orders: Vec<(Px, Arc<InnerL4Order>)>,
    /// The coin's orders without a trigger price yet (`triggerPx` 0: trailing stops waiting for their activation), by
    /// oid, when the band does not hold them; at most the requested count.
    pub(crate) unpriced: Vec<Arc<InnerL4Order>>,
    /// More orders were in the band, or unpriced, than the requested count.
    pub(crate) truncated: bool,
}

/// One coin's orders: held once, in trigger-price order, and found by oid through their trigger price.
#[derive(Debug, Clone, Default)]
struct CoinTriggers {
    by_trigger_px: BTreeMap<(Px, u64), Arc<InnerL4Order>>,
    trigger_px_of: FxHashMap<Oid, Px>,
}

impl CoinTriggers {
    fn get(&self, oid: &Oid) -> Option<&Arc<InnerL4Order>> {
        let trigger_px = *self.trigger_px_of.get(oid)?;
        self.by_trigger_px.get(&(trigger_px, oid.clone().value()))
    }

    fn remove(&mut self, oid: &Oid) -> bool {
        let Some(trigger_px) = self.trigger_px_of.remove(oid) else { return false };
        self.by_trigger_px.remove(&(trigger_px, oid.clone().value()));
        true
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TriggerTable {
    track: bool,
    ignore_spot: bool,
    coins: FxHashMap<Coin, CoinTriggers>,
    /// Kept when a coin's orders are all gone, so a coin that gets orders again does not reuse a version.
    versions: FxHashMap<Coin, u64>,
    generation: u64,
    availability: TriggerAvailability,
    /// The install's dump carried no trigger section at all: the previous table's orders stand in (`succeed`).
    section_absent: bool,
}

impl TriggerTable {
    /// The table of an install: `section` from the dump, then `legacy` trigger orders taken off the book's sides (old
    /// dumps appended them to both sides; the copies are expected equal).
    pub(crate) fn seeded(
        section: TriggerSection<InnerL4Order>,
        legacy: impl IntoIterator<Item = InnerL4Order>,
        track: bool,
        ignore_spot: bool,
    ) -> Self {
        let availability = match (&section, track) {
            (_, false) => TriggerAvailability::Gap(TriggerGap::TrackingDisabled),
            (TriggerSection::Complete(_), true) => TriggerAvailability::Complete,
            (TriggerSection::Partial { skipped: 0, .. } | TriggerSection::Absent, true) => {
                TriggerAvailability::Gap(TriggerGap::SnapshotWithoutTriggers)
            }
            (TriggerSection::Partial { .. }, true) => TriggerAvailability::Gap(TriggerGap::SnapshotParseFailed),
        };
        let mut table = Self {
            track,
            ignore_spot,
            coins: FxHashMap::default(),
            versions: FxHashMap::default(),
            generation: 0,
            availability,
            section_absent: matches!(section, TriggerSection::Absent),
        };
        if !track {
            return table;
        }
        let orders = match section {
            TriggerSection::Complete(orders) | TriggerSection::Partial { orders, .. } => orders,
            TriggerSection::Absent => Vec::new(),
        };
        for order in orders {
            table.seed(order);
        }
        for order in legacy {
            let oid = order.oid();
            if let Some(previous) = table.coins.get(&order.coin).and_then(|coin| coin.get(&oid))
                && **previous != order
            {
                // a mismatch means the dedupe picked one of two different views - surface it
                log::warn!("Trigger order snapshot copies differ across sides: oid={oid:?}");
            }
            table.seed(order);
        }
        table
    }

    fn seed(&mut self, order: InnerL4Order) {
        if self.ignore_spot && order.coin.is_spot() {
            return;
        }
        if self.insert(order).is_err() {
            self.mark_gap(TriggerGap::SnapshotParseFailed);
        }
    }

    /// Records a gap; the first one stands until the next complete install.
    fn mark_gap(&mut self, gap: TriggerGap) {
        if self.availability == TriggerAvailability::Complete {
            self.availability = TriggerAvailability::Gap(gap);
        }
    }

    /// Adds or replaces an order; its trigger price must parse (the index needs it).
    fn insert(&mut self, order: InnerL4Order) -> Result<()> {
        let trigger_px = Px::parse_from_str(&order.trigger_px)?;
        let oid = order.oid();
        self.bump(order.coin.borrow());
        let coin = self.coins.entry(order.coin.clone()).or_default();
        coin.remove(&oid);
        coin.trigger_px_of.insert(oid, trigger_px);
        coin.by_trigger_px.insert((trigger_px, order.oid), Arc::new(order));
        Ok(())
    }

    fn bump(&mut self, coin: &str) {
        match self.versions.get_mut(coin) {
            Some(version) => *version += 1,
            None => {
                self.versions.insert(Coin::new(coin), 1);
            }
        }
    }

    /// Whether a trigger order of `coin` is kept: not with the table off, nor for a spot coin under the spot filter.
    pub(crate) fn tracks(&self, coin: &str) -> bool {
        self.track && !(self.ignore_spot && Coin::str_is_spot(coin))
    }

    /// A live `open` status of a trigger order: added or replaced, or, when it does not parse, a gap.
    pub(crate) fn open(&mut self, order: Result<InnerL4Order>, coin: &str, oid: &Oid) {
        if !self.tracks(coin) {
            return;
        }
        match order.and_then(|order| self.insert(order)) {
            Ok(()) => {}
            Err(err) => {
                // the table under-reports this oid until a complete install: counted and marked
                crate::metrics::PARSE_ERRORS_TOTAL.with_label_values(&["untriggered"]).inc();
                log::warn!("Skipping unparseable untriggered trigger order oid={oid:?}: {err}");
                self.mark_gap(TriggerGap::LiveParseFailed);
            }
        }
    }

    /// Any other status of an oid: the order leaves the table. Whether it was held. Allocation-free unless it was (the
    /// order statuses hot path).
    pub(crate) fn close(&mut self, coin: &str, oid: &Oid) -> bool {
        let Some(coin_triggers) = self.coins.get_mut(coin) else { return false };
        if !coin_triggers.remove(oid) {
            return false;
        }
        if coin_triggers.trigger_px_of.is_empty() {
            // drop the coin once empty so delisted coins don't accumulate empty buckets
            self.coins.remove(coin);
        }
        self.bump(coin);
        true
    }

    pub(crate) fn count(&self) -> usize {
        self.coins.values().map(|coin| coin.trigger_px_of.len()).sum()
    }

    /// This install's table replaces `previous`: the next generation; when the dump carried no trigger section at all
    /// it keeps the previous table's orders (still a gap); a section of some markets only replaces the table like a
    /// complete one. Call before replaying the events cached during the fetch, so their evictions apply to the kept
    /// orders. Arc bumps only.
    pub(crate) fn succeed(&mut self, previous: &Self) {
        self.generation = previous.generation + 1;
        if self.track && self.section_absent && self.coins.is_empty() && !previous.coins.is_empty() {
            self.coins.clone_from(&previous.coins);
            log::info!("Snapshot carried no trigger section; kept {} untriggered trigger orders from the previous state", self.count());
        }
    }

    /// Whether the orders held for `coin` (all coins: `None`) are all its standing ones: a coin of a market the table
    /// does not keep is never complete; all coins means all of the kept markets.
    pub(crate) fn availability(&self, coin: Option<&Coin>) -> TriggerAvailability {
        match coin {
            Some(coin) if !self.tracks(coin.borrow()) => TriggerAvailability::Gap(TriggerGap::TrackingDisabled),
            _ => self.availability,
        }
    }

    /// All coins' orders, or one coin's.
    pub(crate) fn snapshot(&self, coin: Option<&Coin>) -> Vec<Arc<InnerL4Order>> {
        match coin {
            Some(coin) => self.coins.get(coin).map(|coin| coin.by_trigger_px.values().cloned().collect()).unwrap_or_default(),
            None => self.coins.values().flat_map(|coin| coin.by_trigger_px.values().cloned()).collect(),
        }
    }

    /// The coin's orders with a trigger price in `[lo, hi]`, at most `max` of them, and those without a trigger price
    /// (a band above 0 does not hold them).
    pub(crate) fn in_band(&self, coin: &Coin, lo: Px, hi: Px, max: usize) -> TriggerBand {
        let availability = self.availability(Some(coin));
        let version = self.versions.get(coin).copied().unwrap_or_default();
        let Some(coin_triggers) = self.coins.get(coin) else {
            return TriggerBand { availability, generation: self.generation, version, orders: Vec::new(), unpriced: Vec::new(), truncated: false };
        };
        let mut in_band = coin_triggers.by_trigger_px.range((Bound::Included((lo, 0)), Bound::Included((hi, u64::MAX))));
        let orders: Vec<(Px, Arc<InnerL4Order>)> =
            in_band.by_ref().take(max).map(|((trigger_px, _), order)| (*trigger_px, order.clone())).collect();
        let zero = Px::new(0);
        let mut unpriced = coin_triggers.by_trigger_px.range((Bound::Included((zero, 0)), Bound::Included((zero, u64::MAX)))).filter(|_| lo > zero);
        let unpriced_orders: Vec<Arc<InnerL4Order>> = unpriced.by_ref().take(max).map(|(_, order)| order.clone()).collect();
        let truncated = in_band.next().is_some() || unpriced.next().is_some();
        TriggerBand { availability, generation: self.generation, version, orders, unpriced: unpriced_orders, truncated }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;

    use super::*;
    use crate::{order_book::Side, prelude::Error};

    fn px(value: &str) -> Px {
        Px::parse_from_str(value).unwrap()
    }

    fn stop(coin: &str, oid: u64, trigger_px: &str) -> InnerL4Order {
        InnerL4Order {
            user: Address::new([2; 20]),
            coin: Coin::new(coin),
            side: Side::Ask,
            limit_px: px("90.0"),
            sz: crate::order_book::Sz::parse_from_str("1.0").unwrap(),
            oid,
            timestamp: 1000,
            trigger_condition: format!("Price below {trigger_px}"),
            is_trigger: true,
            trigger_px: trigger_px.to_string(),
            is_position_tpsl: false,
            reduce_only: true,
            order_type: "Stop Market".to_string(),
            tif: None,
            cloid: None,
        }
    }

    fn table(section: TriggerSection<InnerL4Order>) -> TriggerTable {
        TriggerTable::seeded(section, Vec::new(), true, true)
    }

    fn oids(band: &TriggerBand) -> Vec<u64> {
        band.orders.iter().map(|(_, order)| order.oid).collect()
    }

    fn band(table: &TriggerTable, coin: &str, lo: &str, hi: &str, max: usize) -> TriggerBand {
        table.in_band(&Coin::new(coin), px(lo), px(hi), max)
    }

    #[test]
    fn a_band_reads_by_trigger_price_then_oid_and_says_when_it_was_cut() {
        let section =
            vec![stop("BTC", 3, "110"), stop("BTC", 1, "105"), stop("BTC", 2, "110"), stop("BTC", 4, "120"), stop("ETH", 5, "110")];
        let table = table(TriggerSection::Complete(section));
        let all = band(&table, "BTC", "100", "120", 10);
        assert_eq!(all.availability, TriggerAvailability::Complete);
        assert_eq!((oids(&all), all.truncated), (vec![1, 2, 3, 4], false));
        assert_eq!(all.orders.iter().map(|(trigger_px, _)| *trigger_px).collect::<Vec<_>>(), [px("105"), px("110"), px("110"), px("120")]);
        let inner = band(&table, "BTC", "106", "115", 10);
        assert_eq!(oids(&inner), vec![2, 3], "both ends inclusive, same trigger price by oid");
        let cut = band(&table, "BTC", "100", "120", 2);
        assert_eq!((oids(&cut), cut.truncated), (vec![1, 2], true));
        let exact = band(&table, "BTC", "100", "120", 4);
        assert!(!exact.truncated, "the requested count reached exactly is not a cut");
        assert_eq!(oids(&band(&table, "SOL", "0", "1000", 10)), Vec::<u64>::new());
        assert_eq!(table.count(), 5);
    }

    /// Review 000460 r1 P2-3: the orders without a trigger price yet (trailing stops waiting for their activation) come
    /// with every band above 0, which never holds them; a band from 0 holds them itself.
    #[test]
    fn a_band_brings_the_unpriced_orders_along() {
        let table = table(TriggerSection::Complete(vec![stop("BTC", 1, "110"), stop("BTC", 7, "0"), stop("BTC", 8, "0")]));
        let above = band(&table, "BTC", "100", "120", 10);
        assert_eq!((oids(&above), above.unpriced.iter().map(|order| order.oid).collect::<Vec<_>>(), above.truncated), (vec![1], vec![7, 8], false));
        let from_zero = band(&table, "BTC", "0", "120", 10);
        assert_eq!((oids(&from_zero), from_zero.unpriced.len()), (vec![7, 8, 1], 0));
        assert!(band(&table, "BTC", "100", "120", 1).truncated, "more unpriced than the count");
    }

    #[test]
    fn an_open_replaces_and_a_close_removes_with_its_index_and_versions_never_repeat() {
        let mut table = table(TriggerSection::Complete(Vec::new()));
        let version = |table: &TriggerTable| band(table, "BTC", "0", "1000", 10).version;
        table.open(Ok(stop("BTC", 1, "110")), "BTC", &Oid::new(1));
        assert_eq!((oids(&band(&table, "BTC", "100", "115", 10)), version(&table)), (vec![1], 1));
        // a modified trigger price moves the order in the index
        table.open(Ok(stop("BTC", 1, "130")), "BTC", &Oid::new(1));
        assert_eq!(oids(&band(&table, "BTC", "100", "115", 10)), Vec::<u64>::new());
        assert_eq!((oids(&band(&table, "BTC", "125", "135", 10)), version(&table)), (vec![1], 2));
        assert_eq!(table.count(), 1);
        assert!(table.close("BTC", &Oid::new(1)));
        assert_eq!((table.count(), version(&table)), (0, 3));
        assert!(!table.close("BTC", &Oid::new(1)), "a second status of the oid holds nothing");
        assert!(!table.close("ETH", &Oid::new(1)));
        assert_eq!(version(&table), 3, "closing what is not held changes nothing");
        // the coin emptied and was dropped; its version carries on
        table.open(Ok(stop("BTC", 2, "110")), "BTC", &Oid::new(2));
        assert_eq!(version(&table), 4);
        assert_eq!(table.availability(None), TriggerAvailability::Complete);
    }

    #[test]
    fn a_section_that_did_not_fully_convert_or_was_absent_is_a_gap_until_a_complete_install() {
        let partial = TriggerSection::Partial { orders: vec![stop("BTC", 1, "110")], skipped: 1, markets_without: 0 };
        let partial = table(partial);
        assert_eq!(partial.availability(None), TriggerAvailability::Gap(TriggerGap::SnapshotParseFailed));
        assert_eq!(partial.count(), 1, "the orders that converted are still held");
        assert_eq!(table(TriggerSection::Absent).availability(None), TriggerAvailability::Gap(TriggerGap::SnapshotWithoutTriggers));
        let market_without = table(TriggerSection::Partial { orders: vec![stop("BTC", 1, "110")], skipped: 0, markets_without: 1 });
        assert_eq!(market_without.availability(None), TriggerAvailability::Gap(TriggerGap::SnapshotWithoutTriggers));
        let unparsed_trigger_px = table(TriggerSection::Complete(vec![stop("BTC", 1, "110"), stop("BTC", 2, "x")]));
        assert_eq!(unparsed_trigger_px.availability(None), TriggerAvailability::Gap(TriggerGap::SnapshotParseFailed));
        assert_eq!(unparsed_trigger_px.count(), 1);

        let mut live = table(TriggerSection::Complete(vec![stop("BTC", 1, "110")]));
        live.open(Err(Error::from("bad order")), "BTC", &Oid::new(2));
        assert_eq!(live.availability(None), TriggerAvailability::Gap(TriggerGap::LiveParseFailed));
        live.open(Ok(stop("BTC", 3, "y")), "BTC", &Oid::new(3));
        assert_eq!(live.count(), 1, "an order whose trigger price does not parse is not held");
        assert_eq!(band(&live, "BTC", "0", "1000", 10).availability, TriggerAvailability::Gap(TriggerGap::LiveParseFailed));

        let mut first_gap_stands = table(TriggerSection::Partial { orders: Vec::new(), skipped: 1, markets_without: 0 });
        first_gap_stands.open(Err(Error::from("bad order")), "BTC", &Oid::new(2));
        assert_eq!(first_gap_stands.availability(None), TriggerAvailability::Gap(TriggerGap::SnapshotParseFailed));
        // only a complete install recovers
        let mut next = table(TriggerSection::Complete(Vec::new()));
        next.succeed(&first_gap_stands);
        assert_eq!(next.availability(None), TriggerAvailability::Complete);
    }

    #[test]
    fn each_install_is_a_new_generation_and_keeps_the_orders_only_when_its_dump_had_no_section() {
        let first = table(TriggerSection::Complete(vec![stop("BTC", 1, "110")]));
        assert_eq!(band(&first, "BTC", "0", "1000", 10).generation, 0);
        let mut without_section = table(TriggerSection::Absent);
        without_section.succeed(&first);
        let kept = band(&without_section, "BTC", "0", "1000", 10);
        assert_eq!((kept.generation, kept.version, oids(&kept)), (1, 0, vec![1]));
        assert_eq!(kept.availability, TriggerAvailability::Gap(TriggerGap::SnapshotWithoutTriggers));
        // the kept orders still leave on their statuses
        assert!(without_section.close("BTC", &Oid::new(1)));
        let mut complete_empty = table(TriggerSection::Complete(Vec::new()));
        complete_empty.succeed(&first);
        let replaced = band(&complete_empty, "BTC", "0", "1000", 10);
        assert_eq!((replaced.generation, oids(&replaced)), (1, Vec::<u64>::new()), "an empty section is an empty table");
        assert_eq!(replaced.availability, TriggerAvailability::Complete);
        // review 000453 r1 P2-2: BTC's section is there and empty, another market's is missing - the converted orders
        // replace the table; only a dump without any section keeps the previous orders
        let mut partial_empty = table(TriggerSection::Partial { orders: Vec::new(), skipped: 0, markets_without: 1 });
        partial_empty.succeed(&first);
        assert_eq!(partial_empty.count(), 0, "a section of some markets is not a missing section");
        assert_eq!(partial_empty.availability(None), TriggerAvailability::Gap(TriggerGap::SnapshotWithoutTriggers));
    }

    #[test]
    fn a_table_that_is_not_kept_says_so() {
        let mut off = TriggerTable::seeded(TriggerSection::Complete(vec![stop("BTC", 1, "110")]), Vec::new(), false, false);
        off.open(Ok(stop("BTC", 2, "110")), "BTC", &Oid::new(2));
        assert_eq!(off.count(), 0);
        assert_eq!(band(&off, "BTC", "0", "1000", 10).availability, TriggerAvailability::Gap(TriggerGap::TrackingDisabled));

        let mut spot_filtered = table(TriggerSection::Complete(vec![stop("@107", 1, "110"), stop("BTC", 2, "110")]));
        spot_filtered.open(Ok(stop("@107", 3, "110")), "@107", &Oid::new(3));
        assert_eq!(spot_filtered.count(), 1);
        let spot = band(&spot_filtered, "@107", "0", "1000", 10);
        assert_eq!((spot.availability, oids(&spot)), (TriggerAvailability::Gap(TriggerGap::TrackingDisabled), Vec::<u64>::new()));
        assert_eq!(band(&spot_filtered, "BTC", "0", "1000", 10).availability, TriggerAvailability::Complete);
        // review 000453 r1 P2-1: the snapshot read says the same per coin; all coins are all of the kept markets
        assert_eq!(spot_filtered.availability(Some(&Coin::new("@107"))), TriggerAvailability::Gap(TriggerGap::TrackingDisabled));
        assert_eq!(spot_filtered.availability(Some(&Coin::new("BTC"))), TriggerAvailability::Complete);
        assert_eq!(spot_filtered.availability(None), TriggerAvailability::Complete);
    }
}
