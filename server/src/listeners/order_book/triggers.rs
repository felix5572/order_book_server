//! The untriggered trigger orders (stops / TP-SL waiting for their trigger price), one owner (design 000451 r3): the
//! orders per coin, an index by trigger price, a change version per coin, and whether the table is complete.
//!
//! These orders never rest on the book. A snapshot install seeds the table from the dump's trigger section
//! (`--include-trigger-orders`); live `open` statuses with `isTrigger` add or replace an order, and any other status of
//! its oid removes it (canceled / triggered / filled / rejected / ...). Orders are `Arc`'d so a read under the listener
//! lock is a refcount bump per order, not a deep clone.
//!
//! A trigger order placed with another order (a TP/SL attached to a limit order: the parent's `children`) waits while
//! its parent is live: the node does not trigger it before the parent leaves the book. A waiting child stays out of
//! the trigger price index - no band read, so no forced flow, sees it - and is still served with the table. bm 10-21
//! hours 08-09, every child of a parent leaving:
//! - `filled`: the children take part (48 triggered after it, 21 `siblingFilledCanceled`, 16 `reduceOnlyCanceled`);
//! - `marginCanceled`: they take part too (one of 12 triggered later);
//! - `canceled`: 226 children got their own `canceled` in the block, 78 a modify moved to the parent's successor
//!   (listed in its later statuses under their own oids; its `open` lists none), none triggered later;
//! - a rejected parent's children never opened.
//!
//! Hyperliquid's TP/SL documentation says the same: a parent's full fill activates its children, a partial fill keeps
//! them waiting, canceling the parent cancels them, a cancel for insufficient margin after a partial fill activates
//! them. So `filled` and `marginCanceled` release - the children the table linked to the parent and those its line
//! lists under their own oids (a modify's successor) -, and every other status of the parent leaves them waiting until
//! their own statuses end them (review 000463 r1 P2-1: a parent's `canceled` activated them until their own came).
//! A parent that fills in its own block (a market or IOC order, a crossing limit order) fills before its children's
//! `open` lines: they take part as they open (bm 10-21 14:35-15:07: 84 such children were held waiting for good, and
//! 28 more through a later modify of them).
//!
//! A modify of a parent moves its children to the successor: bm 10-22 hours 01-02, 308 of the 310 hash-less `canceled`
//! of a parent listing children came right after the same account's `open` of a new order of the coin (none right
//! before it); the successor's lines may list the children (its `canceled` at its own modify, its `filled`) or not (a
//! successor filling at once), and 8 of the children triggered later. So the children wait for the successor from the
//! modify on, and their own modifies follow them there.
//!
//! A modify of a waiting child keeps its successor waiting: the modify opens the new oid and cancels the old one right
//! after it, hash-less, in the block (bm 10-21 hour 11: 129,306 of 129,342 such pairs were modifies by oid or cloid,
//! all 111 of trigger orders; a bot's `batchModify` of a limit order with its TP and SL moves all three so, and the new
//! parent's `open` lists no children).
//!
//! Complete means: the last install's dump carried the section for every market and every order of it converted
//! (an empty section is a complete empty table), and every live trigger order since parsed. Anything else is a gap,
//! kept until the next complete install; the orders the table does hold are still served (`/untriggeredOrders`).

use std::{borrow::Borrow, collections::BTreeMap, ops::Bound, sync::Arc};

use alloy::primitives::Address;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::{
    order_book::{
        Coin, InnerOrder, Oid, Px, Side,
        multi_book::{ParentLinks, TriggerSection},
    },
    prelude::{Error, Result},
    types::{L4Order, inner::InnerL4Order},
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
    /// A child a live parent announced did not open in its block: it may be held as a trigger order the node does not
    /// trigger yet, or its status was missed.
    ChildUnmatched,
    /// A trigger order is held for one status (a modify's successor of a waiting child, or not): out of the band until
    /// that status comes (review 000464 r1). Not kept: it ends with the next status.
    SuccessorPending,
}

impl TriggerGap {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::TrackingDisabled => "trackingDisabled",
            Self::SnapshotWithoutTriggers => "snapshotWithoutTriggers",
            Self::SnapshotParseFailed => "snapshotParseFailed",
            Self::LiveParseFailed => "liveParseFailed",
            Self::ChildUnmatched => "childUnmatched",
            Self::SuccessorPending => "successorPending",
        }
    }
}

/// Whether the orders the table holds are all the standing ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TriggerAvailability {
    Complete,
    Gap(TriggerGap),
}

/// Which way the mark moves to reach a trigger order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Direction {
    Down,
    Up,
}

/// How a trigger order fires, from its `triggerCondition`. bm 10-21 (137,123 untriggered orders): every
/// `Price above|below X` has X equal to `triggerPx`; the trailing stops read `Activation …`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TriggerRule {
    /// Fires when the mark reaches the trigger price from this direction.
    At(Direction),
    /// A trailing stop: its trigger price follows the best price since activation.
    Trailing,
    Unknown,
}

impl TriggerRule {
    pub(crate) fn of(condition: &str) -> Self {
        if condition.starts_with("Price below ") {
            Self::At(Direction::Down)
        } else if condition.starts_with("Price above ") {
            Self::At(Direction::Up)
        } else if condition.starts_with("Activation ") {
            Self::Trailing
        } else {
            Self::Unknown
        }
    }
}

/// A stop's direction by its side: a sale fires on a fall, a buy on a rise.
pub(crate) const fn stop_direction(side: Side) -> Direction {
    match side {
        Side::Ask => Direction::Down,
        Side::Bid => Direction::Up,
    }
}

/// Where a coin's index holds an order: by the direction the mark reaches its trigger price from, or nowhere (a
/// trailing stop waiting for its activation - `triggerPx` 0 -, a condition of another form).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Place {
    At(Direction, Px),
    Nowhere,
}

impl Place {
    fn of(order: &InnerL4Order, trigger_px: Px) -> Self {
        match TriggerRule::of(&order.trigger_condition) {
            TriggerRule::At(direction) => Self::At(direction, trigger_px),
            TriggerRule::Trailing if trigger_px.value() == 0 => Self::Nowhere,
            TriggerRule::Trailing => Self::At(stop_direction(order.side), trigger_px),
            TriggerRule::Unknown => Self::Nowhere,
        }
    }
}

/// The orders a mark in a range reaches or is past already, of one coin.
#[derive(Debug, Clone)]
pub(crate) struct TriggerBand {
    pub(crate) availability: TriggerAvailability,
    /// Installs since start: a new install renumbers every coin's version.
    pub(crate) generation: u64,
    /// Changes of this coin's orders within the generation.
    pub(crate) version: u64,
    /// By trigger price, then oid, with the trigger price as indexed: those a fall reaches at or above the range's
    /// low end, those a rise reaches at or below its high end (an order the mark is past is in: it fires at the next
    /// price action); at most the requested count.
    pub(crate) orders: Vec<(Px, Arc<InnerL4Order>)>,
    /// The coin's orders placed nowhere (a trailing stop waiting for its activation, a condition of another form), by
    /// oid; at most the requested count.
    pub(crate) placed_nowhere: Vec<Arc<InnerL4Order>>,
    /// More orders were in the range, or placed nowhere, than the requested count.
    pub(crate) truncated: bool,
}

/// One coin's orders: held once, by direction in trigger-price order, and found by oid through their place.
#[derive(Debug, Clone, Default)]
struct CoinTriggers {
    down: BTreeMap<(Px, u64), Arc<InnerL4Order>>,
    up: BTreeMap<(Px, u64), Arc<InnerL4Order>>,
    nowhere: BTreeMap<u64, Arc<InnerL4Order>>,
    place_of: FxHashMap<Oid, Place>,
}

impl CoinTriggers {
    fn get(&self, oid: &Oid) -> Option<&Arc<InnerL4Order>> {
        match *self.place_of.get(oid)? {
            Place::At(Direction::Down, trigger_px) => self.down.get(&(trigger_px, oid.clone().value())),
            Place::At(Direction::Up, trigger_px) => self.up.get(&(trigger_px, oid.clone().value())),
            Place::Nowhere => self.nowhere.get(&oid.clone().value()),
        }
    }

    fn insert(&mut self, trigger_px: Px, order: Arc<InnerL4Order>) {
        let place = Place::of(&order, trigger_px);
        let oid = order.oid;
        match place {
            Place::At(Direction::Down, _) => self.down.insert((trigger_px, oid), order),
            Place::At(Direction::Up, _) => self.up.insert((trigger_px, oid), order),
            Place::Nowhere => self.nowhere.insert(oid, order),
        };
        self.place_of.insert(Oid::new(oid), place);
    }

    fn remove(&mut self, oid: &Oid) -> bool {
        let Some(place) = self.place_of.remove(oid) else { return false };
        match place {
            Place::At(Direction::Down, trigger_px) => self.down.remove(&(trigger_px, oid.clone().value())),
            Place::At(Direction::Up, trigger_px) => self.up.remove(&(trigger_px, oid.clone().value())),
            Place::Nowhere => self.nowhere.remove(&oid.clone().value()),
        };
        true
    }

    fn orders(&self) -> impl Iterator<Item = &Arc<InnerL4Order>> {
        self.down.values().chain(self.up.values()).chain(self.nowhere.values())
    }
}

/// The trigger orders waiting for their parent, and who is whose (child and parent oids).
#[derive(Debug, Clone, Default)]
struct Families {
    parent_of: FxHashMap<u64, u64>,
    children_of: FxHashMap<u64, Vec<u64>>,
    /// The waiting children the table holds, with their trigger price as parsed.
    waiting: FxHashMap<u64, (Px, Arc<InnerL4Order>)>,
    /// How many of them each account holds per coin: whether its new trigger order may succeed one.
    owners: FxHashMap<Address, FxHashMap<String, usize>>,
}

impl Families {
    fn link(&mut self, child: u64, parent: u64) {
        self.parent_of.insert(child, parent);
        self.children_of.entry(parent).or_default().push(child);
    }

    /// A modify of a parent: the children still waiting for it wait for its successor.
    fn adopt(&mut self, old: u64, new: u64) {
        let Some(listed) = self.children_of.remove(&old) else { return };
        let waiting: Vec<u64> = listed.into_iter().filter(|child| self.parent_of.get(child) == Some(&old)).collect();
        for &child in &waiting {
            self.parent_of.insert(child, new);
        }
        self.children_of.entry(new).or_default().extend(waiting);
    }

    /// A modify's successor of a waiting child waits for the same parent (on its list while the parent is live).
    fn inherit(&mut self, old: u64, new: u64) {
        let Some(&parent) = self.parent_of.get(&old) else { return };
        self.parent_of.insert(new, parent);
        if let Some(children) = self.children_of.get_mut(&parent) {
            children.push(new);
        }
    }

    fn hold(&mut self, trigger_px: Px, order: Arc<InnerL4Order>) {
        let (user, coin) = (order.user, order.coin.value());
        if self.waiting.insert(order.oid, (trigger_px, order)).is_none() {
            *self.owners.entry(user).or_default().entry(coin).or_default() += 1;
        }
    }

    fn take(&mut self, oid: u64) -> Option<(Px, Arc<InnerL4Order>)> {
        let taken = self.waiting.remove(&oid)?;
        let user = taken.1.user;
        if let Some(coins) = self.owners.get_mut(&user) {
            let coin = taken.1.coin.value();
            if let Some(count) = coins.get_mut(&coin) {
                *count -= 1;
                if *count == 0 {
                    coins.remove(&coin);
                }
            }
            if coins.is_empty() {
                self.owners.remove(&user);
            }
        }
        Some(taken)
    }

    fn holds_any(&self, user: Address, coin: &str) -> bool {
        self.owners.get(&user).is_some_and(|coins| coins.contains_key(coin))
    }

    /// The parent left: its children and `listed` (those its status lists under their own oids), unlinked, and those
    /// the table held waiting.
    fn release(&mut self, parent: u64, listed: impl IntoIterator<Item = u64>) -> Vec<(Px, Arc<InnerL4Order>)> {
        let linked = self.children_of.remove(&parent).unwrap_or_default();
        linked
            .into_iter()
            .chain(listed)
            .filter_map(|child| {
                self.parent_of.remove(&child);
                self.take(child)
            })
            .collect()
    }

    /// The parent left without releasing its children: they stay waiting, for their own statuses or a successor's.
    fn leave_waiting(&mut self, parent: u64) {
        self.children_of.remove(&parent);
    }

    /// A child's own terminal status: unlinked (its parent's list keeps the oid until the parent leaves). Whether it
    /// was held waiting.
    fn forget(&mut self, child: u64) -> bool {
        self.parent_of.remove(&child);
        self.take(child).is_some()
    }
}

/// A trigger order opened by an account holding a waiting child of its coin, held for one status: a modify's successor
/// is told by the next line (the old child's hash-less `canceled`); out of the band meanwhile, so a read between the
/// two lines (two receive batches) does not list it.
#[derive(Debug, Clone)]
struct Successor {
    height: u64,
    user: Address,
    coin: String,
    oid: u64,
    /// As `open` takes it (an error kept as text: the table stays `Clone`).
    order: std::result::Result<InnerL4Order, String>,
}

/// A non-trigger order opened by an account holding a waiting child of its coin, kept for one status: when that status
/// is the hash-less `canceled` of the coin's parent, the order is the parent's modify and the children wait for it (bm
/// 10-22 01:26:09 block 1192577049: a limit sell's modify opens the new oid, cancels the old one listing the TP and SL
/// under their own oids, and the new order, its `open` and `filled` listing none, fills at once; the node triggered the
/// TP 8 minutes later). Nothing is held out of the band meanwhile.
#[derive(Debug, Clone)]
struct ParentSuccessor {
    height: u64,
    user: Address,
    coin: String,
    oid: u64,
}

impl ParentSuccessor {
    fn modified_by(&self, height: u64, user: Address, status: &str, hash: Option<&str>, order: &L4Order) -> bool {
        status == "canceled" && hash.is_none() && height == self.height && user == self.user && order.coin == self.coin
    }
}

/// A child as its parent's live `open` lists it - under the parent's oid -, to be told by its own `open` line, which
/// follows in the same block (bm 10-21 hour 09: 1,745 children, each after its parent, each matched by these fields).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ChildKey {
    user: Address,
    coin: String,
    timestamp: u64,
    side: Side,
    trigger_px: String,
    trigger_condition: String,
    order_type: String,
    sz: String,
    reduce_only: bool,
}

impl ChildKey {
    fn of(user: Address, order: &L4Order) -> Self {
        Self {
            user,
            coin: order.coin.clone(),
            timestamp: order.timestamp,
            side: order.side,
            trigger_px: order.trigger_px.clone(),
            trigger_condition: order.trigger_condition.clone(),
            order_type: order.order_type.clone(),
            sz: order.sz.clone(),
            reduce_only: order.reduce_only,
        }
    }
}

/// The children announced by the parents placed in one block, each key with its parents' oids.
#[derive(Debug, Clone, Default)]
struct ExpectedChildren {
    height: u64,
    parents: FxHashMap<ChildKey, Vec<u64>>,
    /// The parents of the block that filled (or were margin canceled) before their announced children opened: a
    /// market, IOC or crossing order fills in its own block and its children's `open` lines follow its `filled` (bm
    /// 10-21 14:35-15:07: 84 children, 47 of market orders, 28 of IOC, 9 of GTC). Those children take part at once.
    released: FxHashSet<u64>,
}

impl ExpectedChildren {
    /// `parent` left releasing its children: those it announced in the block and not yet opened take part as they open.
    fn note_release(&mut self, parent: u64) {
        if self.parents.values().any(|parents| parents.contains(&parent)) {
            self.released.insert(parent);
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TriggerTable {
    track: bool,
    ignore_spot: bool,
    coins: FxHashMap<Coin, CoinTriggers>,
    families: Families,
    expected: ExpectedChildren,
    successor: Option<Successor>,
    parent_successor: Option<ParentSuccessor>,
    /// Kept when a coin's orders are all gone, so a coin that gets orders again does not reuse a version.
    versions: FxHashMap<Coin, u64>,
    generation: u64,
    availability: TriggerAvailability,
    /// The install's dump carried no trigger section at all: the previous table's orders stand in (`succeed`).
    section_absent: bool,
}

impl TriggerTable {
    /// The table of an install: `section` from the dump, its children waiting for the parents `parents` names, then
    /// `legacy` trigger orders taken off the book's sides (old dumps appended them to both sides; the copies are
    /// expected equal).
    pub(crate) fn seeded(
        section: TriggerSection<InnerL4Order>,
        parents: &ParentLinks,
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
            families: Families::default(),
            expected: ExpectedChildren::default(),
            successor: None,
            parent_successor: None,
            versions: FxHashMap::default(),
            generation: 0,
            availability,
            section_absent: matches!(section, TriggerSection::Absent),
        };
        if !track {
            return table;
        }
        for (&child, &parent) in parents {
            table.families.link(child, parent);
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
            if let Some(previous) = table.held(order.coin.borrow(), &oid)
                && *previous != order
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

    /// Adds or replaces an order, waiting while it has a live parent; its trigger price must parse (the index needs
    /// it).
    fn insert(&mut self, order: InnerL4Order) -> Result<()> {
        let trigger_px = Px::parse_from_str(&order.trigger_px)?;
        if self.families.parent_of.contains_key(&order.oid) {
            self.unindex(order.coin.borrow(), &order.oid());
            self.families.hold(trigger_px, Arc::new(order));
            return Ok(());
        }
        self.index(trigger_px, Arc::new(order));
        Ok(())
    }

    fn index(&mut self, trigger_px: Px, order: Arc<InnerL4Order>) {
        let oid = order.oid();
        self.bump(order.coin.borrow());
        let coin = self.coins.entry(order.coin.clone()).or_default();
        coin.remove(&oid);
        coin.insert(trigger_px, order);
    }

    /// The held order of an oid, indexed or waiting.
    fn held(&self, coin: &str, oid: &Oid) -> Option<&InnerL4Order> {
        let indexed = self.coins.get(coin).and_then(|coin| coin.get(oid));
        indexed.or_else(|| self.families.waiting.get(&oid.clone().value()).map(|(_, order)| order)).map(Arc::as_ref)
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

    /// One status of the order statuses stream, `height` its block's: an `open` adds a trigger order and announces a
    /// parent's children; any other status of an oid ends its order, a parent's `filled` or `marginCanceled` releasing
    /// the children waiting for it, its modify moving them to the successor. Whether a held order left (the order
    /// statuses hot path: allocation-free for an order without children or announced siblings).
    pub(crate) fn on_status(&mut self, height: u64, user: Address, status: &str, hash: Option<&str>, order: &L4Order) -> bool {
        self.expire_expected(height);
        self.settle_successor(height, user, status, hash, order);
        let parent_successor = self.parent_successor.take();
        let oid = Oid::new(order.oid);
        if status == "open" {
            if !order.children.is_empty() && self.tracks(&order.coin) {
                self.announce_children(height, user, order);
            }
            if order.is_trigger && self.tracks(&order.coin) {
                let of_a_filled_parent = self.claim_parent(user, order);
                let parsed = InnerL4Order::try_from((user, order.clone()));
                // a child known to take part is no modify's successor (review 000465 r1 P2)
                if !of_a_filled_parent && hash.is_some() && !self.families.parent_of.contains_key(&order.oid) && self.families.holds_any(user, &order.coin) {
                    let order_text = parsed.map_err(|err| err.to_string());
                    self.successor = Some(Successor { height, user, coin: order.coin.clone(), oid: order.oid, order: order_text });
                    return false;
                }
                self.open(parsed, &order.coin, &oid);
            }
            if !order.is_trigger && hash.is_some() && self.families.holds_any(user, &order.coin) {
                self.parent_successor = Some(ParentSuccessor { height, user, coin: order.coin.clone(), oid: order.oid });
            }
            return false;
        }
        match status {
            "filled" | "marginCanceled" => {
                self.release_children(order);
                if !self.expected.parents.is_empty() {
                    self.expected.note_release(order.oid);
                }
            }
            "triggered" => {}
            _ => match parent_successor.filter(|successor| successor.modified_by(height, user, status, hash, order)) {
                Some(successor) => self.families.adopt(order.oid, successor.oid),
                // a cancel, a rejection, any other status: the children wait for their own statuses or a successor's
                None => self.families.leave_waiting(order.oid),
            },
        }
        self.close(&order.coin, &oid)
    }

    /// The order held for one status: the successor of the waiting child this status cancels hash-less right after it
    /// (a modify) waits for the same parent; any other order opens as it came.
    fn settle_successor(&mut self, height: u64, user: Address, status: &str, hash: Option<&str>, order: &L4Order) {
        let Some(successor) = self.successor.take() else { return };
        let modifies = status == "canceled" && hash.is_none() && height == successor.height && user == successor.user && order.coin == successor.coin;
        if modifies {
            self.families.inherit(order.oid, successor.oid);
        }
        self.open(successor.order.map_err(Error::from), &successor.coin, &Oid::new(successor.oid));
    }

    /// The parent left the book: its children take part.
    fn release_children(&mut self, parent: &L4Order) {
        let listed = match parent.children_orders() {
            Ok(children) => children.into_iter().map(|child| child.oid).filter(|&oid| oid != parent.oid).collect(),
            Err(err) => {
                crate::metrics::PARSE_ERRORS_TOTAL.with_label_values(&["untriggered"]).inc();
                log::warn!("Unparseable children of oid={}: {err}", parent.oid);
                self.mark_gap(TriggerGap::LiveParseFailed);
                Vec::new()
            }
        };
        for (trigger_px, child) in self.families.release(parent.oid, listed) {
            self.index(trigger_px, child);
        }
    }

    /// A live parent's children: those listed under their own oid are linked at once, those under the parent's are
    /// expected in this block.
    fn announce_children(&mut self, height: u64, user: Address, parent: &L4Order) {
        let children = match parent.children_orders() {
            Ok(children) => children,
            Err(err) => {
                // its children may be served as active until a complete install: counted and marked
                crate::metrics::PARSE_ERRORS_TOTAL.with_label_values(&["untriggered"]).inc();
                log::warn!("Unparseable children of oid={}: {err}", parent.oid);
                self.mark_gap(TriggerGap::LiveParseFailed);
                return;
            }
        };
        self.expected.height = height;
        for child in children {
            if child.oid == parent.oid {
                self.expected.parents.entry(ChildKey::of(user, &child)).or_default().push(parent.oid);
            } else {
                self.families.link(child.oid, parent.oid);
            }
        }
    }

    /// A trigger order's own `open`: linked to the parent that announced it in this block, if one did and is still
    /// live. Whether it is the child of a parent that already filled in the block: it takes part as it opens.
    fn claim_parent(&mut self, user: Address, order: &L4Order) -> bool {
        if self.expected.parents.is_empty() || self.families.parent_of.contains_key(&order.oid) {
            return false;
        }
        let key = ChildKey::of(user, order);
        let Some(parents) = self.expected.parents.get_mut(&key) else { return false };
        let parent = parents.remove(0);
        if parents.is_empty() {
            self.expected.parents.remove(&key);
        }
        if self.expected.released.contains(&parent) {
            return true;
        }
        self.families.link(order.oid, parent);
        false
    }

    /// A status of another block: the announcements of the previous one end; a child never seen is counted, and the
    /// table is a gap until a complete install.
    fn expire_expected(&mut self, height: u64) {
        if self.expected.height == height {
            return;
        }
        if !self.expected.released.is_empty() {
            self.expected.released.clear();
        }
        if self.expected.parents.is_empty() {
            return;
        }
        let unmatched: usize = self.expected.parents.values().map(Vec::len).sum();
        crate::metrics::UNTRIGGERED_UNMATCHED_CHILDREN_TOTAL.inc_by(unmatched as u64);
        log::warn!("{unmatched} children announced in block {} never opened: {:?}", self.expected.height, self.expected.parents);
        self.expected.parents.clear();
        // review 000463 r1 P2-2: the table no longer knows its active set
        self.mark_gap(TriggerGap::ChildUnmatched);
    }

    /// Any other status of an oid: the order leaves the table. Whether it was held. Allocation-free unless it was (the
    /// order statuses hot path).
    pub(crate) fn close(&mut self, coin: &str, oid: &Oid) -> bool {
        if self.families.forget(oid.clone().value()) {
            return true;
        }
        self.unindex(coin, oid)
    }

    /// The order leaves the price index. Whether it was there.
    fn unindex(&mut self, coin: &str, oid: &Oid) -> bool {
        let Some(coin_triggers) = self.coins.get_mut(coin) else { return false };
        if !coin_triggers.remove(oid) {
            return false;
        }
        if coin_triggers.place_of.is_empty() {
            // drop the coin once empty so delisted coins don't accumulate empty buckets
            self.coins.remove(coin);
        }
        self.bump(coin);
        true
    }

    pub(crate) fn count(&self) -> usize {
        let held_back = self.successor.as_ref().is_some_and(|successor| successor.order.is_ok());
        self.coins.values().map(|coin| coin.place_of.len()).sum::<usize>() + self.families.waiting.len() + usize::from(held_back)
    }

    /// This install's table replaces `previous`: the next generation; when the dump carried no trigger section at all
    /// it keeps the previous table's orders (still a gap); a section of some markets only replaces the table like a
    /// complete one. Call before replaying the events cached during the fetch, so their evictions apply to the kept
    /// orders. Arc bumps only.
    pub(crate) fn succeed(&mut self, previous: &Self) {
        self.generation = previous.generation + 1;
        // the held orders, waiting children included (review 000463 r1 P2-3)
        if self.track && self.section_absent && self.count() == 0 && previous.count() > 0 {
            self.coins.clone_from(&previous.coins);
            self.families.clone_from(&previous.families);
            self.successor.clone_from(&previous.successor);
            self.parent_successor.clone_from(&previous.parent_successor);
            log::info!("Snapshot carried no trigger section; kept {} untriggered trigger orders from the previous state", self.count());
        }
    }

    /// Whether the orders held for `coin` (all coins: `None`) are all its standing ones: a coin of a market the table
    /// does not keep is never complete; all coins means all of the kept markets.
    pub(crate) fn availability(&self, coin: Option<&Coin>) -> TriggerAvailability {
        let held_back = |coin: &str| self.successor.as_ref().is_some_and(|successor| successor.coin == coin);
        match coin {
            Some(coin) if !self.tracks(coin.borrow()) => TriggerAvailability::Gap(TriggerGap::TrackingDisabled),
            // a kept gap stands over a pending one
            _ if self.availability != TriggerAvailability::Complete => self.availability,
            Some(coin) if held_back(coin.borrow()) => TriggerAvailability::Gap(TriggerGap::SuccessorPending),
            None if self.successor.is_some() => TriggerAvailability::Gap(TriggerGap::SuccessorPending),
            _ => self.availability,
        }
    }

    /// All coins' orders, or one coin's, the waiting children with them.
    pub(crate) fn snapshot(&self, coin: Option<&Coin>) -> Vec<Arc<InnerL4Order>> {
        let held_back = self.successor.as_ref().and_then(|successor| successor.order.as_ref().ok()).map(|order| Arc::new(order.clone()));
        let waiting = self
            .families
            .waiting
            .values()
            .map(|(_, order)| order.clone())
            .chain(held_back)
            .filter(|order| coin.is_none_or(|coin| order.coin == *coin));
        match coin {
            Some(coin) => self.coins.get(coin).map(|coin| coin.orders().cloned().collect::<Vec<_>>()).unwrap_or_default().into_iter().chain(waiting).collect(),
            None => self.coins.values().flat_map(|coin| coin.orders().cloned()).chain(waiting).collect(),
        }
    }

    /// The coin's orders a mark in `[lo, hi]` reaches or is past already - those a fall reaches at or above `lo`, those
    /// a rise reaches at or below `hi` -, at most `max` of them, and those placed nowhere (forced flow acceptance, bm
    /// 10-21: an order placed past its level fires at the next price action, so a band around the mark alone missed it).
    pub(crate) fn in_band(&self, coin: &Coin, lo: Px, hi: Px, max: usize) -> TriggerBand {
        let availability = self.availability(Some(coin));
        let version = self.versions.get(coin).copied().unwrap_or_default();
        let Some(coin_triggers) = self.coins.get(coin) else {
            return TriggerBand { availability, generation: self.generation, version, orders: Vec::new(), placed_nowhere: Vec::new(), truncated: false };
        };
        let mut fallen_to = coin_triggers.down.range((Bound::Included((lo, 0)), Bound::Unbounded));
        let mut risen_to = coin_triggers.up.range((Bound::Unbounded, Bound::Included((hi, u64::MAX))));
        let mut orders: Vec<(Px, Arc<InnerL4Order>)> =
            fallen_to.by_ref().take(max).chain(risen_to.by_ref().take(max)).map(|((trigger_px, _), order)| (*trigger_px, order.clone())).collect();
        orders.sort_unstable_by_key(|(trigger_px, order)| (*trigger_px, order.oid));
        let mut nowhere = coin_triggers.nowhere.values();
        let placed_nowhere: Vec<Arc<InnerL4Order>> = nowhere.by_ref().take(max).cloned().collect();
        let truncated = fallen_to.next().is_some() || risen_to.next().is_some() || orders.len() > max || nowhere.next().is_some();
        orders.truncate(max);
        TriggerBand { availability, generation: self.generation, version, orders, placed_nowhere, truncated }
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
        TriggerTable::seeded(section, &ParentLinks::default(), Vec::new(), true, true)
    }

    fn oids(band: &TriggerBand) -> Vec<u64> {
        band.orders.iter().map(|(_, order)| order.oid).collect()
    }

    fn band(table: &TriggerTable, coin: &str, lo: &str, hi: &str, max: usize) -> TriggerBand {
        table.in_band(&Coin::new(coin), px(lo), px(hi), max)
    }

    /// A rise's order (`Price above`): `stop` with the condition flipped.
    fn take_profit(coin: &str, oid: u64, trigger_px: &str) -> InnerL4Order {
        InnerL4Order { trigger_condition: format!("Price above {trigger_px}"), order_type: "Take Profit Market".to_string(), ..stop(coin, oid, trigger_px) }
    }

    /// A band is the mark's range: a fall's orders from its low end up - those above the mark are past their level
    /// and fire at the next price action (forced flow acceptance, bm 10-21) -, a rise's orders up to its high end;
    /// by trigger price, then oid, cut at the count.
    #[test]
    fn a_band_reads_by_trigger_price_then_oid_and_says_when_it_was_cut() {
        let section = vec![
            stop("BTC", 3, "110"),
            stop("BTC", 1, "105"),
            stop("BTC", 2, "110"),
            stop("BTC", 4, "120"),
            stop("BTC", 6, "95"),
            take_profit("BTC", 7, "112"),
            take_profit("BTC", 8, "90"),
            take_profit("BTC", 9, "130"),
            stop("ETH", 5, "110"),
        ];
        let table = table(TriggerSection::Complete(section));
        let all = band(&table, "BTC", "100", "120", 10);
        assert_eq!(all.availability, TriggerAvailability::Complete);
        assert_eq!((oids(&all), all.truncated), (vec![8, 1, 2, 3, 7, 4], false), "a fall's above 100 (95 is not), a rise's below 120 (130 is not)");
        assert_eq!(all.orders.iter().map(|(trigger_px, _)| *trigger_px).collect::<Vec<_>>(), [px("90"), px("105"), px("110"), px("110"), px("112"), px("120")]);
        let inner = band(&table, "BTC", "106", "111", 10);
        assert_eq!(oids(&inner), vec![8, 2, 3, 4], "both ends inclusive, same trigger price by oid; 105 below the low end, 112 above the high");
        let cut = band(&table, "BTC", "100", "120", 2);
        assert_eq!((oids(&cut), cut.truncated), (vec![8, 1], true));
        let exact = band(&table, "BTC", "100", "120", 6);
        assert!(!exact.truncated, "the requested count reached exactly is not a cut");
        assert_eq!(oids(&band(&table, "SOL", "0", "1000", 10)), Vec::<u64>::new());
        assert_eq!(table.count(), 9);
    }

    /// Review 000460 r1 P2-3: the orders placed nowhere - a trailing stop waiting for its activation (`triggerPx` 0),
    /// a condition of another form - come with every band; an activated trailing stop is a fall's (a sale) or a
    /// rise's (a buy).
    #[test]
    fn a_band_brings_the_orders_placed_nowhere_along() {
        let trailing = |oid: u64, trigger_px: &str, side: Side| InnerL4Order {
            trigger_condition: "Activation above 0.02, retracement 5%, best waiting".to_string(),
            order_type: "Trailing Stop Market".to_string(),
            side,
            ..stop("BTC", oid, trigger_px)
        };
        let odd = InnerL4Order { trigger_condition: "Price crosses 110".to_string(), ..stop("BTC", 9, "110") };
        let table = table(TriggerSection::Complete(vec![stop("BTC", 1, "110"), trailing(7, "0", Side::Ask), trailing(8, "0", Side::Bid), odd, trailing(10, "125", Side::Ask), trailing(11, "125", Side::Bid)]));
        let band_of = band(&table, "BTC", "100", "120", 10);
        let nowhere: Vec<u64> = band_of.placed_nowhere.iter().map(|order| order.oid).collect();
        assert_eq!((oids(&band_of), nowhere, band_of.truncated), (vec![1, 10], vec![7, 8, 9], false), "the activated sale at 125 is past");
        assert_eq!(oids(&band(&table, "BTC", "130", "140", 10)), vec![11], "the activated buy, reached by a rise");
        assert!(band(&table, "BTC", "100", "120", 2).truncated, "more placed nowhere than the count");
    }

    #[test]
    fn an_open_replaces_and_a_close_removes_with_its_index_and_versions_never_repeat() {
        let mut table = table(TriggerSection::Complete(Vec::new()));
        let version = |table: &TriggerTable| band(table, "BTC", "0", "1000", 10).version;
        table.open(Ok(stop("BTC", 1, "110")), "BTC", &Oid::new(1));
        assert_eq!((oids(&band(&table, "BTC", "100", "115", 10)), version(&table)), (vec![1], 1));
        // a modified trigger price moves the order in the index: below the low end, a fall to 100 does not reach it
        table.open(Ok(stop("BTC", 1, "95")), "BTC", &Oid::new(1));
        assert_eq!(oids(&band(&table, "BTC", "100", "115", 10)), Vec::<u64>::new());
        assert_eq!((oids(&band(&table, "BTC", "90", "100", 10)), version(&table)), (vec![1], 2));
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
        let mut off = TriggerTable::seeded(TriggerSection::Complete(vec![stop("BTC", 1, "110")]), &ParentLinks::default(), Vec::new(), false, false);
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

    const USER: Address = Address::new([2; 20]);
    /// An `open` carries its action's hash; a cancel or a fill none.
    const HASH: Option<&str> = Some("0x01");

    /// The wire order of `stop`, or of a limit buy with `children` (each listed under `listed_oid`, `None`: its own).
    fn wire(order: &InnerL4Order) -> L4Order {
        L4Order {
            user: None,
            coin: order.coin.value(),
            side: order.side,
            limit_px: "90.0".to_string(),
            sz: "1.0".to_string(),
            oid: order.oid,
            timestamp: order.timestamp,
            trigger_condition: order.trigger_condition.clone(),
            is_trigger: order.is_trigger,
            trigger_px: order.trigger_px.clone(),
            children: Vec::new(),
            is_position_tpsl: order.is_position_tpsl,
            reduce_only: order.reduce_only,
            order_type: order.order_type.clone(),
            orig_sz: "1.0".to_string(),
            tif: None,
            cloid: None,
        }
    }

    fn parent(oid: u64, children: &[&InnerL4Order], listed_oid: Option<u64>) -> L4Order {
        let mut limit = wire(&stop("BTC", oid, "0"));
        (limit.trigger_condition, limit.is_trigger, limit.order_type, limit.side) = ("N/A".to_string(), false, "Limit".to_string(), Side::Bid);
        limit.children = children
            .iter()
            .map(|child| {
                let mut listed = wire(child);
                listed.oid = listed_oid.unwrap_or(child.oid);
                serde_json::to_value(listed).unwrap()
            })
            .collect();
        limit
    }

    fn held(table: &TriggerTable) -> Vec<u64> {
        let mut oids: Vec<u64> = table.snapshot(None).iter().map(|order| order.oid).collect();
        oids.sort_unstable();
        oids
    }

    /// bm 10-21: a dump lists a resting limit order's TP and SL under their own oids, in its children and in the
    /// trigger section; the node triggers neither before the parent fills. They wait out of the band - still held -,
    /// and the parent's `filled` brings them in.
    #[test]
    fn a_dumps_children_wait_for_their_parent_to_fill() {
        let (take_profit, stop_loss, alone) = (stop("BTC", 2, "110"), stop("BTC", 3, "105"), stop("BTC", 9, "108"));
        let parents = ParentLinks::from_iter([(2, 1), (3, 1)]);
        let mut table =
            TriggerTable::seeded(TriggerSection::Complete(vec![take_profit.clone(), stop_loss.clone(), alone]), &parents, Vec::new(), true, true);
        let before = band(&table, "BTC", "100", "120", 10);
        assert_eq!((oids(&before), held(&table), table.count()), (vec![9], vec![2, 3, 9], 3));
        // the parent's partial fills say nothing; its `filled` lists the children under their own oids
        assert!(!table.on_status(5, USER, "filled", None, &parent(1, &[&take_profit, &stop_loss], None)), "the parent is not a trigger order");
        let after = band(&table, "BTC", "100", "120", 10);
        assert_eq!(oids(&after), vec![3, 9, 2]);
        assert!(after.version > before.version, "the band changed");
        assert!(table.families.parent_of.is_empty() && table.families.children_of.is_empty());
    }

    /// bm 10-21 hour 09: a parent's live `open` lists its children under its own oid; their own `open` lines follow in
    /// the block. A child canceled with its parent gets its own `canceled`; one the parent's `marginCanceled` leaves
    /// behind takes part (the node triggered it later).
    #[test]
    fn live_children_wait_from_their_open_and_leave_or_take_part_with_their_parent() {
        let (take_profit, stop_loss) = (stop("BTC", 11, "110"), stop("BTC", 12, "105"));
        let mut table = table(TriggerSection::Complete(Vec::new()));
        table.on_status(7, USER, "open", HASH, &parent(10, &[&take_profit, &stop_loss], Some(10)));
        table.on_status(7, USER, "open", HASH, &wire(&take_profit));
        table.on_status(7, USER, "open", HASH, &wire(&stop_loss));
        assert_eq!((oids(&band(&table, "BTC", "100", "120", 10)), held(&table)), (Vec::<u64>::new(), vec![11, 12]));
        // a waiting child modified stays waiting
        table.on_status(8, USER, "open", HASH, &wire(&stop("BTC", 12, "104")));
        assert_eq!(oids(&band(&table, "BTC", "100", "120", 10)), Vec::<u64>::new());
        // canceled with its parent: the parent's line releases nothing, the children's own remove them
        table.on_status(9, USER, "canceled", None, &parent(10, &[&take_profit, &stop_loss], None));
        assert_eq!((oids(&band(&table, "BTC", "100", "120", 10)), held(&table)), (Vec::<u64>::new(), vec![11, 12]));
        assert!(table.on_status(9, USER, "canceled", None, &wire(&take_profit)));
        assert!(table.on_status(9, USER, "canceled", None, &wire(&stop_loss)));
        assert_eq!(held(&table), Vec::<u64>::new());

        // a parent rejected or ended otherwise releases nothing either
        let (rejected_child, other_child) = (stop("BTC", 16, "110"), stop("BTC", 18, "110"));
        table.on_status(10, USER, "open", HASH, &parent(15, &[&rejected_child], Some(15)));
        table.on_status(10, USER, "open", HASH, &wire(&rejected_child));
        table.on_status(10, USER, "open", HASH, &parent(17, &[&other_child], Some(17)));
        table.on_status(10, USER, "open", HASH, &wire(&other_child));
        table.on_status(10, USER, "perpMarginRejected", None, &parent(15, &[&rejected_child], None));
        table.on_status(10, USER, "reduceOnlyCanceled", None, &parent(17, &[&other_child], None));
        assert_eq!((oids(&band(&table, "BTC", "100", "120", 10)), held(&table)), (Vec::<u64>::new(), vec![16, 18]));
        assert!(table.on_status(10, USER, "canceled", None, &wire(&rejected_child)));
        assert!(table.on_status(10, USER, "canceled", None, &wire(&other_child)));

        let survivor = stop("BTC", 21, "110");
        table.on_status(10, USER, "open", HASH, &parent(20, &[&survivor], Some(20)));
        table.on_status(10, USER, "open", HASH, &wire(&survivor));
        assert_eq!(oids(&band(&table, "BTC", "100", "120", 10)), Vec::<u64>::new());
        table.on_status(11, USER, "marginCanceled", None, &parent(20, &[&survivor], None));
        assert_eq!(oids(&band(&table, "BTC", "100", "120", 10)), vec![21]);
        assert!(table.on_status(12, USER, "triggered", None, &wire(&survivor)));
        assert_eq!(table.count(), 0);
    }

    /// Review 000465 r1 P2: the child of a parent that filled (or was margin canceled) before it opened takes part at
    /// once, even beside a waiting child of its account and coin: not held as a modify's successor, so the next line,
    /// that waiting child's hash-less `canceled`, does not move it under the waiting child's parent.
    #[test]
    fn the_child_of_a_filled_parent_takes_part_beside_a_waiting_child() {
        for released_by in ["filled", "marginCanceled"] {
            let waiting = stop("BTC", 2, "110");
            let mut table = TriggerTable::seeded(TriggerSection::Complete(vec![waiting.clone()]), &ParentLinks::from_iter([(2, 1)]), Vec::new(), true, true);
            let child = stop("BTC", 11, "108");
            table.on_status(7, USER, "open", HASH, &parent(10, &[&child], Some(10)));
            table.on_status(7, USER, released_by, None, &parent(10, &[], None));
            table.on_status(7, USER, "open", HASH, &wire(&child));
            assert_eq!(oids(&band(&table, "BTC", "100", "120", 10)), vec![11], "{released_by}");
            assert!(table.on_status(7, USER, "canceled", None, &wire(&waiting)));
            assert_eq!(oids(&band(&table, "BTC", "100", "120", 10)), vec![11], "{released_by}");
            assert_eq!((table.families.parent_of.get(&11), table.availability(None)), (None, TriggerAvailability::Complete), "{released_by}");
        }
    }

    /// Review 000463 r1 P2-3: a dump without a trigger section keeps the previous table, a table holding waiting
    /// children only included, with their links.
    #[test]
    fn a_dump_without_a_section_keeps_a_table_of_waiting_children() {
        let child = stop("BTC", 2, "110");
        let first = TriggerTable::seeded(TriggerSection::Complete(vec![child.clone()]), &ParentLinks::from_iter([(2, 1)]), Vec::new(), true, true);
        assert_eq!((first.count(), oids(&band(&first, "BTC", "100", "120", 10))), (1, Vec::<u64>::new()));
        let mut next = table(TriggerSection::Absent);
        next.succeed(&first);
        assert_eq!(held(&next), vec![2]);
        next.on_status(5, USER, "filled", None, &parent(1, &[&child], None));
        assert_eq!(oids(&band(&next, "BTC", "100", "120", 10)), vec![2]);
    }

    /// An account holding a waiting child opens another trigger order of the coin: held for one status - served, out
    /// of the band -, then opened as it came unless that status is the child's hash-less `canceled` of the block (a
    /// modify: the successor waits).
    #[test]
    fn a_new_trigger_order_beside_a_waiting_child_is_told_by_the_next_status() {
        let (child, other, successor) = (stop("BTC", 2, "110"), stop("BTC", 3, "108"), stop("BTC", 4, "111"));
        let mut table = TriggerTable::seeded(TriggerSection::Complete(vec![child.clone()]), &ParentLinks::from_iter([(2, 1)]), Vec::new(), true, true);
        table.on_status(7, USER, "open", HASH, &wire(&other));
        assert_eq!((oids(&band(&table, "BTC", "100", "120", 10)), held(&table)), (Vec::<u64>::new(), vec![2, 3]), "held for one status");
        // review 000464 r1: meanwhile the coin's band is not complete, another coin's is
        assert_eq!(band(&table, "BTC", "100", "120", 10).availability, TriggerAvailability::Gap(TriggerGap::SuccessorPending));
        assert_eq!(band(&table, "ETH", "100", "120", 10).availability, TriggerAvailability::Complete);
        assert_eq!(table.availability(None), TriggerAvailability::Gap(TriggerGap::SuccessorPending));
        // the next status is no modify of the child: the order opens as it came, the band complete again
        table.on_status(7, USER, "open", HASH, &parent(9, &[], None));
        let settled = band(&table, "BTC", "100", "120", 10);
        assert_eq!((oids(&settled), settled.availability), (vec![3], TriggerAvailability::Complete));
        // a modify of the child in another block, or a hashed cancel, is not its successor
        table.on_status(8, USER, "open", HASH, &wire(&successor));
        table.on_status(9, USER, "canceled", None, &wire(&child));
        assert_eq!(oids(&band(&table, "BTC", "100", "120", 10)), vec![3, 4]);
        let (child, successor) = (stop("BTC", 12, "110"), stop("BTC", 14, "111"));
        let mut table = TriggerTable::seeded(TriggerSection::Complete(vec![child.clone()]), &ParentLinks::from_iter([(12, 1)]), Vec::new(), true, true);
        table.on_status(8, USER, "open", HASH, &wire(&successor));
        table.on_status(8, USER, "canceled", HASH, &wire(&child));
        assert_eq!(oids(&band(&table, "BTC", "100", "120", 10)), vec![14]);
        // the modify: the successor's `open`, then the child's hash-less `canceled` in the block
        let (child, successor) = (stop("BTC", 22, "110"), stop("BTC", 24, "111"));
        let mut table = TriggerTable::seeded(TriggerSection::Complete(vec![child.clone()]), &ParentLinks::from_iter([(22, 1)]), Vec::new(), true, true);
        table.on_status(8, USER, "open", HASH, &wire(&successor));
        table.on_status(8, USER, "canceled", None, &wire(&child));
        let modified = band(&table, "BTC", "100", "120", 10);
        assert_eq!((oids(&modified), held(&table), modified.availability), (Vec::<u64>::new(), vec![24], TriggerAvailability::Complete));
        table.on_status(9, USER, "filled", None, &parent(1, &[&successor], None));
        assert_eq!(oids(&band(&table, "BTC", "100", "120", 10)), vec![24]);
        // a kept gap stands while an order is held, and after
        table.mark_gap(TriggerGap::LiveParseFailed);
        table.on_status(10, USER, "open", HASH, &parent(30, &[&stop("BTC", 31, "110")], Some(30)));
        table.on_status(10, USER, "open", HASH, &wire(&stop("BTC", 31, "110")));
        table.on_status(10, USER, "open", HASH, &wire(&stop("BTC", 32, "109")));
        assert_eq!(table.availability(None), TriggerAvailability::Gap(TriggerGap::LiveParseFailed));
        table.on_status(10, USER, "filled", None, &parent(40, &[], None));
        assert_eq!(table.availability(None), TriggerAvailability::Gap(TriggerGap::LiveParseFailed));
    }

    /// bm 10-21 block 1191742333: a modify of a parent cancels its oid - the line lists the children under their own
    /// oids, which get no status - and opens its successor, whose `open` lists none; the children wait on, and the
    /// successor's `filled`, listing them, releases them.
    #[test]
    fn a_modified_parents_children_wait_for_its_successor() {
        let (stop_loss, take_profit) = (stop("BTC", 792, "105"), stop("BTC", 793, "110"));
        let mut table = table(TriggerSection::Complete(Vec::new()));
        table.on_status(7, USER, "open", HASH, &parent(791, &[&stop_loss, &take_profit], Some(791)));
        table.on_status(7, USER, "open", HASH, &wire(&stop_loss));
        table.on_status(7, USER, "open", HASH, &wire(&take_profit));
        table.on_status(8, USER, "canceled", None, &parent(791, &[&stop_loss, &take_profit], None));
        table.on_status(8, USER, "open", HASH, &parent(78, &[], None));
        assert_eq!((oids(&band(&table, "BTC", "100", "120", 10)), held(&table)), (Vec::<u64>::new(), vec![792, 793]));
        table.on_status(9, USER, "filled", None, &parent(78, &[&stop_loss, &take_profit], None));
        assert_eq!(oids(&band(&table, "BTC", "100", "120", 10)), vec![792, 793]);
        assert!(table.families.parent_of.is_empty() && table.families.children_of.is_empty());
    }

    /// bm 10-22 block 1192577049 (forced flow acceptance run9): a modify of a parent opens the successor and cancels the
    /// old oid right after it, hash-less; the successor, its `open` and `filled` listing no children, fills at once and
    /// the node triggered the TP 8 minutes later. The children wait for the successor from the modify on, through its
    /// own modifies (blocks 1192588386 and 1192588523, the second listing the child under its oid), and its `filled`
    /// releases them. A hashed cancel, a successor of another coin, or another line between the two is no modify.
    #[test]
    fn a_parents_modify_moves_its_waiting_children_to_the_successor() {
        const OTHER: Address = Address::new([3; 20]);
        let (stop_loss, take_profit) = (stop("BTC", 11, "105"), stop("BTC", 12, "110"));
        let mut filled_at_once = table(TriggerSection::Complete(Vec::new()));
        filled_at_once.on_status(7, USER, "open", HASH, &parent(10, &[&stop_loss, &take_profit], Some(10)));
        filled_at_once.on_status(7, USER, "open", HASH, &wire(&stop_loss));
        filled_at_once.on_status(7, USER, "open", HASH, &wire(&take_profit));
        filled_at_once.on_status(8, USER, "open", HASH, &parent(20, &[], None));
        filled_at_once.on_status(8, USER, "canceled", None, &parent(10, &[&stop_loss, &take_profit], None));
        assert_eq!((oids(&band(&filled_at_once, "BTC", "100", "120", 10)), held(&filled_at_once)), (Vec::<u64>::new(), vec![11, 12]));
        filled_at_once.on_status(8, USER, "filled", None, &parent(20, &[], None));
        assert_eq!(oids(&band(&filled_at_once, "BTC", "100", "120", 10)), vec![11, 12]);
        assert!(filled_at_once.families.parent_of.is_empty() && filled_at_once.families.children_of.is_empty());

        let child = stop("BTC", 31, "105");
        let mut modified_again = table(TriggerSection::Complete(Vec::new()));
        modified_again.on_status(7, USER, "open", HASH, &parent(30, &[&child], Some(30)));
        modified_again.on_status(7, USER, "open", HASH, &wire(&child));
        modified_again.on_status(8, USER, "open", HASH, &parent(40, &[], None));
        modified_again.on_status(8, USER, "canceled", None, &parent(30, &[&child], None));
        modified_again.on_status(9, USER, "open", HASH, &parent(50, &[], None));
        modified_again.on_status(9, USER, "canceled", None, &parent(40, &[&child], None));
        assert_eq!((oids(&band(&modified_again, "BTC", "100", "120", 10)), held(&modified_again)), (Vec::<u64>::new(), vec![31]));
        modified_again.on_status(10, USER, "filled", None, &parent(50, &[], None));
        assert_eq!(oids(&band(&modified_again, "BTC", "100", "120", 10)), vec![31]);

        for (cancel_hash, successor_coin, line_between) in [(HASH, "BTC", false), (None, "ETH", false), (None, "BTC", true)] {
            let child = stop("BTC", 61, "105");
            let mut unmodified = table(TriggerSection::Complete(Vec::new()));
            unmodified.on_status(7, USER, "open", HASH, &parent(60, &[&child], Some(60)));
            unmodified.on_status(7, USER, "open", HASH, &wire(&child));
            let mut successor = parent(70, &[], None);
            successor.coin = successor_coin.to_string();
            unmodified.on_status(8, USER, "open", HASH, &successor);
            if line_between {
                unmodified.on_status(8, OTHER, "canceled", HASH, &wire(&stop("ETH", 99, "1")));
            }
            unmodified.on_status(8, USER, "canceled", cancel_hash, &parent(60, &[&child], None));
            unmodified.on_status(8, USER, "filled", None, &successor);
            let case = format!("{cancel_hash:?} {successor_coin} {line_between}");
            assert_eq!((oids(&band(&unmodified, "BTC", "100", "120", 10)), held(&unmodified)), (Vec::<u64>::new(), vec![61]), "{case}");
            assert!(unmodified.on_status(8, USER, "canceled", None, &wire(&child)), "{case}");
        }
    }

    /// A trigger order with children keeps them waiting through its `triggered`; a child the block announced but whose
    /// own `open` never came is counted when the next block's status arrives; a child canceled alone is gone for good.
    #[test]
    fn a_triggered_parent_keeps_its_children_and_an_unmatched_announcement_is_counted() {
        let child = stop("BTC", 31, "110");
        let mut trigger_parent = wire(&stop("BTC", 30, "100"));
        trigger_parent.children = vec![serde_json::to_value(wire(&child)).unwrap()];
        let mut table = table(TriggerSection::Complete(Vec::new()));
        table.on_status(7, USER, "open", HASH, &trigger_parent);
        table.on_status(7, USER, "open", HASH, &wire(&child));
        assert_eq!(oids(&band(&table, "BTC", "90", "120", 10)), vec![30]);
        assert!(table.on_status(8, USER, "triggered", None, &trigger_parent));
        assert_eq!((oids(&band(&table, "BTC", "90", "120", 10)), held(&table)), (Vec::<u64>::new(), vec![31]));
        assert!(table.on_status(8, USER, "canceled", None, &wire(&child)));
        table.on_status(9, USER, "filled", None, &trigger_parent);
        assert_eq!(table.count(), 0, "the parent's fill brings back no canceled child");

        let unmatched = crate::metrics::UNTRIGGERED_UNMATCHED_CHILDREN_TOTAL.get();
        table.on_status(20, USER, "open", HASH, &parent(40, &[&stop("BTC", 41, "110")], Some(40)));
        // its own `open` differs (another trigger price): not the announced child
        table.on_status(20, USER, "open", HASH, &wire(&stop("BTC", 41, "111")));
        assert_eq!(oids(&band(&table, "BTC", "100", "120", 10)), vec![41]);
        assert_eq!(table.availability(None), TriggerAvailability::Complete);
        table.on_status(21, USER, "open", HASH, &wire(&stop("BTC", 50, "110")));
        assert_eq!(crate::metrics::UNTRIGGERED_UNMATCHED_CHILDREN_TOTAL.get(), unmatched + 1);
        assert!(table.expected.parents.is_empty());
        // review 000463 r1 P2-2: the active set is no longer known
        assert_eq!(band(&table, "BTC", "100", "120", 10).availability, TriggerAvailability::Gap(TriggerGap::ChildUnmatched));
        // a market the table does not keep announces nothing
        let spot_child = stop("@107", 61, "110");
        let mut spot_parent = parent(60, &[&spot_child], Some(60));
        spot_parent.coin = "@107".to_string();
        table.on_status(30, USER, "open", HASH, &spot_parent);
        assert!(table.expected.parents.is_empty());
    }
}
