//! Liquidation candidates (design 000443 r1–r3): the positions whose node mark is within `MAX_CANDIDATE_DISTANCE_BPS`
//! of their liquidation price, with the market liquidation order each would take.
//!
//! Only replayed rows of the applied height count (r2 revision 1): their size, pool cash, members and node marks are all
//! of that block. An anchor's row stays in `extCoinPositions` and never becomes a candidate. The rows are indexed per coin
//! and side by liquidation price, so a mark move is a range query. A block re-reads the rows of the accounts its events
//! touched, and of the accounts sharing a pool with a coin whose mark moved (their prices move with it); a coin is
//! evaluated again when its mark moved, one of its rows changed, or it has candidates (their cooldown runs on block
//! time). Candidates are kept as published: an update goes out when a field changed, the distance to the bp.

use std::{collections::BTreeSet, sync::Arc};

use alloy::primitives::Address;
use rustc_hash::{FxHashMap, FxHashSet};

use super::{
    book::{AccountBook, Replayed},
    forced_flow::LiquidationInput,
    cooldown::Cooldown,
    liquidation::{LiquidationPx, RowMark},
    marks::MarksAt,
    walk::{WalkKey, WalkMailbox, WalkRequest, WalkResult, WalkShare, walk_units},
};
use crate::{listeners::order_book::BookTrust, order_book::Side};

/// The widest distance a subscription may ask for, and the server's candidate set.
pub(crate) const MAX_CANDIDATE_DISTANCE_BPS: u32 = 1_000;
/// A market liquidation up to this notional takes the whole position, above it a fifth (research
/// `hl_liquidation_rules_20261020`, 21,846 node liquidations), micro-USD.
const WHOLE_ORDER_MAX_NOTIONAL_MICRO: u128 = 100_000 * 1_000_000;
/// The range query reads this much wider than the cap; the exact distance filters.
const RANGE_MARGIN: f64 = 1e-6;

/// The next market liquidation order of a candidate (r2 revision 2, r3 revision 1). Sizes are raw, 10^-szDecimals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ExpectedOrder {
    pub(crate) cooldown: Cooldown,
    /// Without a cooldown: the whole position up to 100k notional, else a fifth, floored (research: 236 of 236 partial
    /// sizes are the floor).
    pub(crate) first_order_raw: u64,
    /// In a cooldown: the whole position.
    pub(crate) cooldown_order_raw: u64,
    /// What a book walk takes: the first order only when the cooldown is known to be none.
    pub(crate) walk_raw: u64,
    /// |size| × mark, micro-USD (exact: raw size times the mark's price units).
    pub(crate) notional_micro: u128,
}

impl ExpectedOrder {
    pub(crate) fn of(size_raw: i64, mark_raw: u64, cooldown: Cooldown) -> Self {
        let whole = size_raw.unsigned_abs();
        let notional_micro = u128::from(whole) * u128::from(mark_raw);
        let first_order_raw = if notional_micro <= WHOLE_ORDER_MAX_NOTIONAL_MICRO { whole } else { whole / 5 };
        let walk_raw = if cooldown == Cooldown::None { first_order_raw } else { whole };
        Self { cooldown, first_order_raw, cooldown_order_raw: whole, walk_raw, notional_micro }
    }
}

/// A candidate's static book walk (r2 revisions 3–4, r3 revisions 1–2).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BookWalk {
    /// The process has no order book (the follow probe).
    NoBook,
    /// The book is not trusted: no estimate is claimed.
    Untrusted,
    /// Requested and not answered yet.
    Pending,
    Walked {
        share: WalkShare,
        /// This candidate or one before it has an unknown cooldown, walked as the whole position.
        unknown_cooldown_as_full: bool,
        /// The book's own progress at the copy, not the candidate's height.
        book_height: u64,
        book_time: u64,
        /// The book's withheld periods at the copy: the estimate holds only while the book stays trusted in it.
        withheld_periods: u64,
        /// The book's mid at the copy; `None` with an empty side.
        mid_px: Option<f64>,
    },
}

/// A walk as it may be published now (r3 revision 2, at every publishing boundary): `book` is the order book's
/// (trusted, withheld periods) now, `None` without one. An untrusted book claims nothing; a walk of another period, or
/// one voided by an untrusted period, waits for its new result.
pub(crate) fn walk_as_of(walk: &BookWalk, book: Option<(bool, u64)>) -> BookWalk {
    match (book, walk) {
        (None, _) => walk.clone(),
        (Some((false, _)), _) => BookWalk::Untrusted,
        (Some((true, periods)), BookWalk::Walked { withheld_periods, .. }) if *withheld_periods == periods => walk.clone(),
        (Some((true, _)), _) => BookWalk::Pending,
    }
}

/// A position within the server's distance of its liquidation price at `height`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Candidate {
    pub(crate) size_raw: i64,
    pub(crate) size_decimals: u32,
    pub(crate) height: u64,
    pub(crate) mark_raw: u64,
    pub(crate) liquidation_px: f64,
    pub(crate) distance_bps: f64,
    pub(crate) isolated: bool,
    pub(crate) pool_positions: usize,
    /// True when the account's replayed cash went through an isolated open, increase or flip since the anchor, whose margin transfer is modelled (research "逐仓成交的划转"): its isolated cash may be short and its cross pool long of the node's by the same amount, so the price may sit nearer the mark (isolated) or farther (cross) than the node's, until the next anchor.
    pub(crate) cash_estimated: bool,
    /// The other coins of its cross pool, which may be liquidated with it (not a guarantee).
    pub(crate) cross_pool_coins: Vec<String>,
    pub(crate) order: ExpectedOrder,
    pub(crate) walk: BookWalk,
}

impl Candidate {
    /// The walk's key: longs are sold into the bids, shorts bought from the asks.
    const fn walk_side(&self) -> Side {
        if self.size_raw > 0 { Side::Ask } else { Side::Bid }
    }

    /// Whether a subscriber holding `self` needs `other` (r1 section 4): the distance to the bp, the price, the size,
    /// the pool and the order's cooldown and sizes. The mark and the notional alone move with every mark update and push
    /// nothing; a sent candidate keeps the ones it was sent with.
    fn differs(&self, other: &Self) -> bool {
        let fields = |candidate: &Self| {
            let order = &candidate.order;
            (
                candidate.size_raw,
                candidate.liquidation_px.to_bits(),
                candidate.distance_bps.round().to_bits(),
                candidate.isolated,
                candidate.pool_positions,
                candidate.cash_estimated,
                (order.cooldown, order.first_order_raw, order.cooldown_order_raw, order.walk_raw),
            )
        };
        fields(self) != fields(other) || self.cross_pool_coins != other.cross_pool_coins || self.walk != other.walk
    }
}

/// The candidates one refresh changed (one frame per refresh, every coin at once).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CandidateChanges {
    /// One more than the previous changes'.
    pub(crate) version: u64,
    pub(crate) height: u64,
    pub(crate) time_ms: u64,
    pub(crate) enter: Vec<(String, Address, Candidate)>,
    pub(crate) update: Vec<(String, Address, Candidate)>,
    pub(crate) exit: Vec<(String, Address)>,
}

/// Every published candidate, at the version of the last changes.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CandidatesSnapshot {
    pub(crate) version: u64,
    pub(crate) height: u64,
    pub(crate) time_ms: u64,
    pub(crate) candidates: Vec<(String, Address, Candidate)>,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Refresh {
    Unchanged,
    Changes(CandidateChanges),
    /// The candidates stopped being trustworthy: every subscription ends.
    Withdrawn(String),
}

/// A replayed row with a computed price.
#[derive(Debug, Clone, Copy, PartialEq)]
struct IndexedRow {
    size_raw: i64,
    liquidation_px: f64,
    isolated: bool,
    pool_positions: usize,
    cash_estimated: bool,
}

/// One coin's indexed rows: by side, (price bits, user), positive prices ordering as their bits.
#[derive(Debug, Default)]
struct CoinIndex {
    size_decimals: u32,
    longs: BTreeSet<(u64, Address)>,
    shorts: BTreeSet<(u64, Address)>,
    rows: FxHashMap<Address, IndexedRow>,
}

impl CoinIndex {
    const fn side(&mut self, size_raw: i64) -> &mut BTreeSet<(u64, Address)> {
        if size_raw > 0 { &mut self.longs } else { &mut self.shorts }
    }

    fn insert(&mut self, user: Address, row: IndexedRow) {
        self.side(row.size_raw).insert((row.liquidation_px.to_bits(), user));
        self.rows.insert(user, row);
    }

    fn remove(&mut self, user: Address) {
        if let Some(row) = self.rows.remove(&user) {
            self.side(row.size_raw).remove(&(row.liquidation_px.to_bits(), user));
        }
    }

    /// The rows whose price is within `max_bps` of either mark (USD), and some just outside it: a long's sale below the
    /// higher mark, a short's buy above the lower one, both past it included.
    fn within(&self, marks: (f64, f64), max_bps: u32) -> impl Iterator<Item = (Address, IndexedRow)> + '_ {
        let cap = f64::from(max_bps) / 1e4;
        let low = (marks.0.min(marks.1) * (1.0 - cap - RANGE_MARGIN)).max(0.0).to_bits();
        let high = (marks.0.max(marks.1) * (1.0 + cap + RANGE_MARGIN)).to_bits();
        let longs = self.longs.range((low, Address::ZERO)..);
        let shorts = self.shorts.range(..=(high, Address::repeat_byte(0xff)));
        longs.chain(shorts).map(|(_, user)| (*user, self.rows[user]))
    }

    /// The rows whose price is within the cap of `mark` (USD), and some just outside it.
    fn near(&self, mark: f64) -> impl Iterator<Item = (Address, IndexedRow)> + '_ {
        self.within((mark, mark), MAX_CANDIDATE_DISTANCE_BPS)
    }
}

/// The trusted rows' height and block time, and the anchor they replay from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Trusted {
    anchor: u64,
    height: u64,
    time_ms: u64,
}

/// The order book the walks read: its trust mirror and the walk task's mailbox.
pub(crate) struct BookLink {
    pub(crate) trust: BookTrust,
    pub(crate) mailbox: Arc<WalkMailbox>,
}

/// The owner's candidate set; lives in the account view's owner task.
pub(crate) struct Candidates {
    /// `None` while the rows or the marks are not trusted: the index is then empty.
    trusted: Option<Trusted>,
    index: FxHashMap<String, CoinIndex>,
    /// The coins each account has indexed rows in.
    user_coins: FxHashMap<Address, Vec<String>>,
    /// The accounts holding several coins, under each: a pool of several positions moves with its members' marks, and
    /// one without rows (a member without a mark) gets them once the mark is back.
    shared: FxHashMap<String, FxHashSet<Address>>,
    /// The coins each account is listed under in `shared`.
    shared_coins: FxHashMap<Address, Vec<String>>,
    /// The mark each indexed coin was last evaluated at.
    marks_seen: FxHashMap<String, Option<u64>>,
    published: FxHashMap<String, FxHashMap<Address, Candidate>>,
    version: u64,
    /// `None` without an order book: every walk is `NoBook`.
    book: Option<BookLink>,
    /// The book's (trusted, withheld periods) at the last check.
    book_state: Option<(bool, u64)>,
    /// The newest request of each key, while the book stays in the period it was sent in.
    requested: FxHashMap<WalkKey, WalkRequest>,
    walk_version: u64,
    /// Permanent until restart: the walk task is gone.
    stopped: Option<String>,
}

impl Candidates {
    pub(crate) fn new(book: Option<BookLink>) -> Self {
        Self {
            trusted: None,
            index: FxHashMap::default(),
            user_coins: FxHashMap::default(),
            shared: FxHashMap::default(),
            shared_coins: FxHashMap::default(),
            marks_seen: FxHashMap::default(),
            published: FxHashMap::default(),
            version: 0,
            book,
            book_state: None,
            requested: FxHashMap::default(),
            walk_version: 0,
            stopped: None,
        }
    }

    /// Ends the feed until restart; withdraws it when it was available.
    pub(crate) fn stop(&mut self, reason: String) -> Refresh {
        self.stopped.get_or_insert_with(|| reason.clone());
        self.withdraw(reason)
    }

    /// After the owner applied blocks or installed an anchor: `touched` are the accounts whose rows may have changed.
    pub(crate) fn refresh(&mut self, book: &AccountBook, marks: Option<&dyn MarksAt>, touched: &[Address]) -> Refresh {
        if let Some(reason) = &self.stopped {
            return self.withdraw(reason.clone());
        }
        let trusted = match trust(book, marks) {
            Ok(trusted) => trusted,
            Err(reason) => return self.withdraw(reason),
        };
        let Some(marks) = marks else { unreachable!("trusted marks are present") };
        let mut changes = self.changes_at(trusted);
        // no block since the last refresh: only the book's trust may have moved
        let reevaluated = self.trusted != Some(trusted) || !touched.is_empty();
        if reevaluated {
            self.reevaluate(book, marks, touched, trusted, &mut changes);
        }
        self.walk_book(&mut changes, reevaluated);
        self.finish(changes)
    }

    /// One block's rows re-read and coins evaluated.
    fn reevaluate(&mut self, book: &AccountBook, marks: &dyn MarksAt, touched: &[Address], trusted: Trusted, changes: &mut CandidateChanges) {
        let mut coins: FxHashSet<String> = self.published.keys().cloned().collect();
        let rebuild = self.trusted.is_none_or(|previous| previous.anchor != trusted.anchor);
        self.trusted = Some(trusted);
        if rebuild {
            self.clear();
            for user in book.holders() {
                coins.extend(self.reindex(book, marks, user));
            }
        } else {
            let moved = self.moved_coins(marks);
            let mut users: FxHashSet<Address> = touched.iter().copied().collect();
            users.extend(moved.iter().filter_map(|coin| self.shared.get(coin)).flatten());
            for user in users {
                coins.extend(self.reindex(book, marks, user));
            }
            coins.extend(moved);
        }
        let mut coins: Vec<String> = coins.into_iter().collect();
        coins.sort_unstable();
        for coin in coins {
            self.evaluate(book, marks, &coin, changes);
        }
    }

    /// A walk result (r3 revision 2): taken only while the book is trusted in the period it was read in, for the newest
    /// request of its key, with every candidate of the request still published at its size.
    pub(crate) fn apply_walk(&mut self, result: WalkResult) -> Refresh {
        let (Some(trusted), Some(link)) = (self.trusted, self.book.as_ref()) else { return Refresh::Unchanged };
        if !result.trusted || (link.trust.is_trusted(), link.trust.withheld_periods()) != (true, result.withheld_periods) {
            return Refresh::Unchanged;
        }
        let mut changes = self.changes_at(trusted);
        let Some(request) = self.requested.get(&result.key).filter(|request| request.version == result.version) else {
            return Refresh::Unchanged;
        };
        let Some(published) = self.published.get_mut(&result.key.0) else { return Refresh::Unchanged };
        let still = |(user, size, _): &(Address, u64, bool)| {
            published.get(user).is_some_and(|candidate| walk_units(candidate.order.walk_raw, candidate.size_decimals) == *size)
        };
        if !request.sequence.iter().all(still) {
            return Refresh::Unchanged;
        }
        let mut unknown = false;
        for ((user, _, cooldown_unknown), share) in request.sequence.iter().zip(result.shares) {
            unknown |= *cooldown_unknown;
            let walk = BookWalk::Walked {
                share,
                unknown_cooldown_as_full: unknown,
                book_height: result.book_height,
                book_time: result.book_time,
                withheld_periods: result.withheld_periods,
                mid_px: result.mid_px,
            };
            let candidate = published.get_mut(user).unwrap_or_else(|| unreachable!("checked above"));
            if candidate.walk != walk {
                candidate.walk = walk;
                changes.update.push((result.key.0.clone(), *user, candidate.clone()));
            }
        }
        self.finish(changes)
    }

    /// The book's trust, then the walk requests (r2 revision 3, r3 revision 2): an untrusted book voids every estimate
    /// and request; a trusted one in a new period voids the requests sent before it. Only when the candidates were
    /// re-evaluated or the book's state moved are the walk orders rebuilt (review 000446 r1 P2-1): a key whose order
    /// changed gets a new request, and its estimates from the first changed place on go back to pending until it is
    /// answered (P2-3: an estimate holds only for the list it walked).
    fn walk_book(&mut self, changes: &mut CandidateChanges, reevaluated: bool) {
        let Some(link) = &self.book else { return };
        let (trust, mailbox) = (link.trust.clone(), Arc::clone(&link.mailbox));
        let state = (trust.is_trusted(), trust.withheld_periods());
        let moved = self.book_state.replace(state) != Some(state);
        if moved {
            self.requested.clear();
            let walk = if state.0 { BookWalk::Pending } else { BookWalk::Untrusted };
            self.set_walks(&walk, changes);
        }
        if !state.0 || !(moved || reevaluated) {
            return;
        }
        let sequences = self.sequences();
        self.requested.retain(|key, _| sequences.contains_key(key));
        for (key, sequence) in sequences {
            let previous = self.requested.get(&key).map(|request| request.sequence.as_slice());
            if previous == Some(sequence.as_slice()) {
                continue;
            }
            let unchanged = previous.map_or(0, |previous| previous.iter().zip(&sequence).take_while(|(old, new)| old == new).count());
            self.set_suffix_pending(&key.0, &sequence[unchanged..], changes);
            self.walk_version += 1;
            let request = WalkRequest { version: self.walk_version, sequence };
            mailbox.post(key.clone(), request.clone());
            self.requested.insert(key, request);
        }
    }

    /// The listed candidates' estimates back to pending, as updates.
    fn set_suffix_pending(&mut self, coin: &str, suffix: &[(Address, u64, bool)], changes: &mut CandidateChanges) {
        let Some(published) = self.published.get_mut(coin) else { return };
        for (user, _, _) in suffix {
            let Some(candidate) = published.get_mut(user).filter(|candidate| candidate.walk != BookWalk::Pending) else { continue };
            candidate.walk = BookWalk::Pending;
            note_update(changes, coin, *user, candidate);
        }
    }

    /// Every published candidate's walk set to `walk`, as updates.
    fn set_walks(&mut self, walk: &BookWalk, changes: &mut CandidateChanges) {
        for (coin, published) in &mut self.published {
            for (user, candidate) in published.iter_mut().filter(|(_, candidate)| candidate.walk != *walk) {
                candidate.walk = walk.clone();
                note_update(changes, coin, *user, candidate);
            }
        }
    }

    /// Each key's candidates in walk order (r2 revision 3): longs by liquidation price from the highest, shorts from the
    /// lowest, equal prices by address; with their walk sizes and whether their cooldown is unknown.
    fn sequences(&self) -> FxHashMap<WalkKey, Vec<(Address, u64, bool)>> {
        let mut keyed: FxHashMap<WalkKey, Vec<(f64, Address, u64, bool)>> = FxHashMap::default();
        for (coin, published) in &self.published {
            for (user, candidate) in published {
                let size = walk_units(candidate.order.walk_raw, candidate.size_decimals);
                let unknown = candidate.order.cooldown == Cooldown::Unknown;
                keyed.entry((coin.clone(), candidate.walk_side())).or_default().push((candidate.liquidation_px, *user, size, unknown));
            }
        }
        keyed
            .into_iter()
            .map(|(key, mut candidates)| {
                candidates.sort_unstable_by(|left, right| {
                    let by_price = left.0.total_cmp(&right.0);
                    let by_price = if key.1 == Side::Ask { by_price.reverse() } else { by_price };
                    by_price.then(left.1.cmp(&right.1))
                });
                (key, candidates.into_iter().map(|(_, user, size, unknown)| (user, size, unknown)).collect())
            })
            .collect()
    }

    const fn changes_at(&self, trusted: Trusted) -> CandidateChanges {
        CandidateChanges {
            version: self.version + 1,
            height: trusted.height,
            time_ms: trusted.time_ms,
            enter: Vec::new(),
            update: Vec::new(),
            exit: Vec::new(),
        }
    }

    fn finish(&mut self, changes: CandidateChanges) -> Refresh {
        if changes.enter.is_empty() && changes.update.is_empty() && changes.exit.is_empty() {
            return Refresh::Unchanged;
        }
        self.version = changes.version;
        Refresh::Changes(changes)
    }

    /// The coin's indexed rows within `max_bps` of the node mark or of the local mark (both 10^-(6 - szDecimals) USD),
    /// each with its next market liquidation order (design 000451 r5 revision 15: the index, not the published list cut
    /// at the node distance); `Err` while the candidates are not trusted.
    pub(crate) fn liquidation_inputs(
        &self,
        book: &AccountBook,
        coin: &str,
        node_mark_raw: u64,
        local_mark_raw: Option<u64>,
        max_bps: u32,
    ) -> Result<Vec<LiquidationInput>, String> {
        if let Some(reason) = &self.stopped {
            return Err(reason.clone());
        }
        self.trusted.ok_or("the liquidation candidates are not available")?;
        let Some(index) = self.index.get(coin) else { return Ok(Vec::new()) };
        let mark = |raw: u64| super::checkpoint::mark_px(raw, index.size_decimals);
        let marks = (mark(node_mark_raw), mark(local_mark_raw.unwrap_or(node_mark_raw)));
        Ok(index
            .within(marks, max_bps)
            .map(|(user, row)| LiquidationInput {
                user,
                size_raw: row.size_raw,
                liquidation_px: row.liquidation_px,
                cash_estimated: row.cash_estimated,
                order: ExpectedOrder::of(row.size_raw, node_mark_raw, book.cooldown(&user)),
            })
            .collect())
    }

    pub(crate) fn count(&self) -> usize {
        self.published.values().map(FxHashMap::len).sum()
    }

    /// Every published candidate; `Err` while untrusted.
    pub(crate) fn snapshot(&self) -> Result<CandidatesSnapshot, String> {
        if let Some(reason) = &self.stopped {
            return Err(reason.clone());
        }
        let trusted = self.trusted.ok_or("the liquidation candidates are not available")?;
        let book = self.book.as_ref().map(|link| (link.trust.is_trusted(), link.trust.withheld_periods()));
        let as_of = |candidate: &Candidate| Candidate { walk: walk_as_of(&candidate.walk, book), ..candidate.clone() };
        let mut candidates: Vec<(String, Address, Candidate)> = self
            .published
            .iter()
            .flat_map(|(coin, published)| published.iter().map(move |(user, candidate)| (coin.clone(), *user, as_of(candidate))))
            .collect();
        candidates.sort_unstable_by(|left, right| (&left.0, left.1).cmp(&(&right.0, right.1)));
        Ok(CandidatesSnapshot { version: self.version, height: trusted.height, time_ms: trusted.time_ms, candidates })
    }

    fn withdraw(&mut self, reason: String) -> Refresh {
        if self.trusted.take().is_none() {
            return Refresh::Unchanged;
        }
        self.clear();
        self.published.clear();
        self.requested.clear();
        Refresh::Withdrawn(reason)
    }

    /// Empties the index (not the published candidates, which a rebuild compares with).
    fn clear(&mut self) {
        self.index.clear();
        self.user_coins.clear();
        self.shared.clear();
        self.shared_coins.clear();
        self.marks_seen.clear();
    }

    /// A new candidate's walk: none without a book, nothing claimed while it is untrusted, else requested.
    const fn entering_walk(&self) -> BookWalk {
        match (&self.book, self.book_state) {
            (None, _) => BookWalk::NoBook,
            (Some(_), Some((false, _))) => BookWalk::Untrusted,
            (Some(_), _) => BookWalk::Pending,
        }
    }

    /// The indexed coins whose mark moved since they were last evaluated.
    fn moved_coins(&self, marks: &dyn MarksAt) -> Vec<String> {
        self.index.keys().chain(self.shared.keys()).filter(|coin| self.marks_seen.get(*coin).copied().flatten() != marks.mark(coin)).cloned().collect()
    }

    /// Re-reads `user`'s replayed rows; returns the coins whose rows it held before or holds now.
    fn reindex(&mut self, book: &AccountBook, marks: &dyn MarksAt, user: Address) -> Vec<String> {
        let mut coins = self.user_coins.remove(&user).unwrap_or_default();
        for coin in &coins {
            if let Some(index) = self.index.get_mut(coin) {
                index.remove(user);
            }
        }
        for coin in self.shared_coins.remove(&user).unwrap_or_default() {
            if let Some(users) = self.shared.get_mut(&coin) {
                users.remove(&user);
            }
        }
        let held = book.held_coins(user);
        if held.len() > 1 {
            for coin in &held {
                self.shared.entry((*coin).to_string()).or_default().insert(user);
            }
            self.shared_coins.insert(user, held.iter().map(|coin| (*coin).to_string()).collect());
        }
        let mut indexed = Vec::new();
        for (coin, size_raw, replayed) in &book.replayed_rows(user, Some(marks)) {
            let Some(row) = indexed_row(*size_raw, replayed) else { continue };
            let size_decimals = book.size_decimals(coin).unwrap_or_else(|| unreachable!("a held coin is in the universe"));
            self.index.entry((*coin).to_string()).or_insert_with(|| CoinIndex { size_decimals, ..CoinIndex::default() }).insert(user, row);
            indexed.push((*coin).to_string());
        }
        coins.extend(indexed.iter().cloned());
        if !indexed.is_empty() {
            self.user_coins.insert(user, indexed);
        }
        coins
    }

    /// The coin's candidates at its current mark, against the published ones.
    fn evaluate(&mut self, book: &AccountBook, marks: &dyn MarksAt, coin: &str, changes: &mut CandidateChanges) {
        let entering = self.entering_walk();
        let mark_raw = marks.mark(coin);
        self.marks_seen.insert(coin.to_string(), mark_raw);
        let mut current: FxHashMap<Address, Candidate> = FxHashMap::default();
        if let (Some(mark_raw), Some(index)) = (mark_raw, self.index.get(coin)) {
            let mark = super::checkpoint::mark_px(mark_raw, index.size_decimals);
            for (user, row) in index.near(mark) {
                let distance = RowMark::of(row.size_raw, index.size_decimals, Some(mark_raw), &LiquidationPx::Computed(row.liquidation_px));
                let Some(distance_bps) = distance.distance_bps.filter(|distance| *distance <= f64::from(MAX_CANDIDATE_DISTANCE_BPS)) else {
                    continue;
                };
                let shared = !row.isolated && row.pool_positions > 1;
                current.insert(user, Candidate {
                    size_raw: row.size_raw,
                    size_decimals: index.size_decimals,
                    height: changes.height,
                    mark_raw,
                    liquidation_px: row.liquidation_px,
                    distance_bps,
                    isolated: row.isolated,
                    pool_positions: row.pool_positions,
                    cash_estimated: row.cash_estimated,
                    cross_pool_coins: if shared { book.cross_pool_coins(user, coin) } else { Vec::new() },
                    order: ExpectedOrder::of(row.size_raw, mark_raw, book.cooldown(&user)),
                    // the last result stays until a newer one replaces it
                    walk: self.published.get(coin).and_then(|published| published.get(&user)).map_or_else(|| entering.clone(), |old| old.walk.clone()),
                });
            }
        }
        let published = self.published.entry(coin.to_string()).or_default();
        let mut exits: Vec<Address> = published.keys().filter(|user| !current.contains_key(*user)).copied().collect();
        exits.sort_unstable();
        for user in exits {
            published.remove(&user);
            changes.exit.push((coin.to_string(), user));
        }
        let mut users: Vec<Address> = current.keys().copied().collect();
        users.sort_unstable();
        for user in users {
            let candidate = current.remove(&user).unwrap_or_else(|| unreachable!("listed above"));
            match published.get(&user) {
                None => changes.enter.push((coin.to_string(), user, candidate.clone())),
                Some(old) if old.differs(&candidate) => changes.update.push((coin.to_string(), user, candidate.clone())),
                Some(_) => continue,
            }
            published.insert(user, candidate);
        }
        if published.is_empty() {
            self.published.remove(coin);
        }
    }
}

/// `candidate` into this refresh's changes: in place of its own entry or update, else as an update.
fn note_update(changes: &mut CandidateChanges, coin: &str, user: Address, candidate: &Candidate) {
    let same = |(entry_coin, entry_user, _): &&mut (String, Address, Candidate)| entry_coin == coin && *entry_user == user;
    if let Some(entry) = changes.enter.iter_mut().chain(changes.update.iter_mut()).find(same) {
        entry.2 = candidate.clone();
        return;
    }
    changes.update.push((coin.to_string(), user, candidate.clone()));
}

/// The row's index entry: a computed price only.
const fn indexed_row(size_raw: i64, replayed: &Replayed) -> Option<IndexedRow> {
    let LiquidationPx::Computed(liquidation_px) = replayed.row.liquidation_px else { return None };
    Some(IndexedRow {
        size_raw,
        liquidation_px,
        isolated: replayed.row.isolated,
        pool_positions: replayed.row.pool_positions,
        cash_estimated: replayed.row.cash_estimated,
    })
}

/// The rows and the marks are trusted at one height: the view available with its replayed rows, the node marks of that
/// height.
fn trust(book: &AccountBook, marks: Option<&dyn MarksAt>) -> Result<Trusted, String> {
    book.availability().map_err(|reason| format!("the account view is unavailable: {reason}"))?;
    let (anchor, height, time_ms) = book.replay_at().ok_or("the account view has no replayed rows yet")?;
    let marks = marks.ok_or("the node marks are unavailable")?;
    if marks.height() != height {
        return Err(format!("the node marks are of block {}, the account view of {height}", marks.height()));
    }
    Ok(Trusted { anchor: anchor.height, height, time_ms })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        accounts::{
            checkpoint::tests::{HOLDER, user as checkpoint_user},
            liquidation::solve,
            test_support::{ANCHOR_TABLE, ListedMarks, PUMP, block_actions, fills, fixture_table, replay_book},
        },
        types::node_data::{Batch, NodeDataFill},
    };

    /// PUMP (szDecimals 0) price units of `px` USD.
    fn units(px: f64) -> u64 {
        #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss, reason = "test prices are small and positive")]
        let units = (px * 1e6).round() as u64;
        units
    }

    /// HOLDER's replayed liquidation price: long 1,000,000 PUMP alone in its pool at cash `cash`.
    fn alone_px(cash: f64, size: f64) -> f64 {
        match solve(cash, 0.0, size, &fixture_table(ANCHOR_TABLE)) {
            LiquidationPx::Computed(px) => px,
            other => panic!("{other:?}"),
        }
    }

    fn changes(refresh: Refresh) -> CandidateChanges {
        match refresh {
            Refresh::Changes(changes) => changes,
            other => panic!("{other:?}"),
        }
    }

    fn marks(height: u64, marks: &[(&'static str, u64)]) -> ListedMarks {
        ListedMarks { height, marks: marks.to_vec() }
    }

    /// A fills line of block `block` at block time `block` seconds (as `block_actions`), with HOLDER's market liquidation
    /// of `sz` PUMP from a long of `start` (trade `tid`).
    fn liquidation(block: u64, tid: u64, start: &str, sz: &str) -> Batch<NodeDataFill> {
        let holder = checkpoint_user(HOLDER);
        let time = chrono::DateTime::from_timestamp_millis((block * 1_000).cast_signed()).unwrap().naive_utc().format("%Y-%m-%dT%H:%M:%S%.f");
        let fill = format!(
            r#"["{holder:#x}",{{"coin":"PUMP","px":"0.0125","sz":"{sz}","side":"A","time":1,"startPosition":"{start}","dir":"Close Long","closedPnl":"0","hash":"0xabc","oid":7,"crossed":true,"fee":"0","tid":{tid},"feeToken":"USDC","liquidation":{{"liquidatedUser":"{holder:#x}","markPx":"0.0125","method":"market"}}}}]"#
        );
        let line = format!(r#"{{"local_time":"{time}","block_time":"{time}","block_number":{block},"events":[{fill}]}}"#);
        sonic_rs::from_str(&line).unwrap()
    }

    /// Review 000446 r1 P2-4: one completed block's liquidation is one event however its fills split into lines: a whole
    /// position taken in two lines is not partial.
    #[test]
    fn a_whole_liquidation_split_across_lines_is_not_partial() {
        let holder = checkpoint_user(HOLDER);
        let cooldown_after = |lines: Vec<Batch<NodeDataFill>>| {
            let mut book = replay_book(1_000, 1_000, &[(holder, 1_000_000)]);
            for block in 1_001..=1_031 {
                book.apply_block(block_actions(block, &[]), Vec::new(), Vec::new(), |_| None, &crate::accounts::marks::fill_prices::FillMarkPrices::default(), Default::default());
            }
            book.apply_block(block_actions(1_032, &[]), lines, Vec::new(), |_| None, &crate::accounts::marks::fill_prices::FillMarkPrices::default(), Default::default());
            book.cooldown(&holder)
        };
        let (first, second) = (liquidation(1_032, 1, "1000000.0", "200000.0"), liquidation(1_032, 2, "800000.0", "800000.0"));
        assert_eq!(cooldown_after(vec![first, second]), Cooldown::None, "200,000 then 800,000 of 1,000,000: whole");
        let partial = liquidation(1_032, 1, "1000000.0", "200000.0");
        assert_eq!(cooldown_after(vec![partial]), Cooldown::Active);
    }

    #[test]
    fn expected_orders_follow_the_notional_and_the_cooldown() {
        // 1,000,000 at 0.1: 100,000 USD, the whole position
        let whole = ExpectedOrder::of(1_000_000, 100_000, Cooldown::None);
        assert_eq!((whole.first_order_raw, whole.walk_raw, whole.notional_micro), (1_000_000, 1_000_000, 100_000_000_000));
        // above it a fifth, floored
        let fifth = ExpectedOrder::of(-1_000_003, 100_000, Cooldown::None);
        assert_eq!((fifth.first_order_raw, fifth.cooldown_order_raw, fifth.walk_raw), (200_000, 1_000_003, 200_000));
        assert_eq!(ExpectedOrder::of(-1_000_003, 100_000, Cooldown::Active).walk_raw, 1_000_003);
        assert_eq!(ExpectedOrder::of(-1_000_003, 100_000, Cooldown::Unknown).walk_raw, 1_000_003, "unknown walks the whole");
    }

    /// r1 section 4: the distance to the bp and the fields a subscriber acts on; the mark and notional alone are not.
    #[test]
    fn an_update_needs_a_field_or_a_bp_of_distance() {
        let candidate = Candidate {
            size_raw: 1_000_000,
            size_decimals: 0,
            height: 1,
            mark_raw: 12_500,
            liquidation_px: 0.012,
            distance_bps: 196.2,
            isolated: false,
            pool_positions: 1,
            cash_estimated: false,
            cross_pool_coins: Vec::new(),
            order: ExpectedOrder::of(1_000_000, 12_500, Cooldown::Unknown),
            walk: BookWalk::NoBook,
        };
        let moved = |edit: &dyn Fn(&mut Candidate)| {
            let mut other = candidate.clone();
            edit(&mut other);
            candidate.differs(&other)
        };
        assert!(!moved(&|other| other.distance_bps = 196.4), "the same bp");
        assert!(moved(&|other| other.distance_bps = 196.6));
        assert!(!moved(&|other| (other.mark_raw, other.order.notional_micro, other.height) = (12_501, 1, 2)));
        assert!(moved(&|other| other.order.cooldown = Cooldown::Active), "the assumption alone (r3)");
        assert!(moved(&|other| other.liquidation_px = 0.012_000_1));
        assert!(moved(&|other| other.cross_pool_coins = vec!["BTC".to_string()]));
    }

    /// r2 revision 1, r1 section 4: a replayed row enters within the cap of its mark and exits beyond it; a refresh
    /// without a block changes nothing; marks of another height withdraw the candidates.
    #[test]
    fn a_replayed_row_enters_and_exits_with_its_mark() {
        let holder = checkpoint_user(HOLDER);
        let mut book = replay_book(1_000, 1_000, &[(holder, 1_000_000)]);
        let px = alone_px(-12_000.0, 1_000_000.0);
        let mut candidates = Candidates::new(None);
        let near = marks(1_000, &[(PUMP, units(px * 1.02))]);
        let first = changes(candidates.refresh(&book, Some(&near), &[]));
        assert_eq!((first.version, first.enter.len(), first.update.len(), first.exit.len()), (1, 1, 0, 0));
        let (coin, user, candidate) = &first.enter[0];
        assert_eq!((coin.as_str(), *user, candidate.size_raw, candidate.height, candidate.liquidation_px), (PUMP, holder, 1_000_000, 1_000, px));
        assert!((candidate.distance_bps - 196.1).abs() < 0.5, "{}", candidate.distance_bps);
        assert_eq!(candidate.order.cooldown, Cooldown::Unknown, "30 s of blocks not seen yet");
        assert_eq!(candidates.refresh(&book, Some(&near), &[]), Refresh::Unchanged, "no block since");
        assert_eq!(candidates.snapshot().unwrap().candidates.len(), 1);
        book.apply_block_actions(block_actions(1_001, &[]));
        let far = marks(1_001, &[(PUMP, units(px * 1.2))]);
        let second = changes(candidates.refresh(&book, Some(&far), &[]));
        assert_eq!((second.version, second.exit), (2, vec![(PUMP.to_string(), holder)]));
        assert_eq!(candidates.snapshot().unwrap().candidates, []);
        book.apply_block_actions(block_actions(1_002, &[]));
        let withdrawn = candidates.refresh(&book, Some(&far), &[]);
        assert!(matches!(&withdrawn, Refresh::Withdrawn(reason) if reason.contains("of block 1001")), "{withdrawn:?}");
        assert!(candidates.snapshot().is_err());
        assert_eq!(candidates.refresh(&book, None, &[]), Refresh::Unchanged, "withdrawn once");
    }

    /// A shared pool's row moves with another member's mark: the account is re-read though nothing touched it.
    #[test]
    fn a_shared_pool_is_reread_when_a_member_mark_moves() {
        let holder = checkpoint_user(HOLDER);
        let mut book = replay_book(1_000, 1_000, &[(holder, 1_000_000)]);
        book.apply_block_actions(block_actions(1_001, &[]));
        book.apply_fills(fills(1_001, &[(holder, "BTC", "B", 1, "0.0", "0.001")]));
        let pump = units(alone_px(-12_000.001, 1_000_000.0) * 1.02);
        let mut candidates = Candidates::new(None);
        let first = changes(candidates.refresh(&book, Some(&marks(1_001, &[(PUMP, pump), ("BTC", 828_576)])), &[holder]));
        let (_, _, entered) = &first.enter[0];
        assert_eq!((entered.pool_positions, entered.cross_pool_coins.as_slice()), (2, ["BTC".to_string()].as_slice()));
        book.apply_block_actions(block_actions(1_002, &[]));
        let second = changes(candidates.refresh(&book, Some(&marks(1_002, &[(PUMP, pump), ("BTC", 728_576)])), &[]));
        let [(coin, user, updated)] = second.update.as_slice() else { panic!("{second:?}") };
        assert_eq!((coin.as_str(), *user), (PUMP, holder));
        assert!(updated.liquidation_px > entered.liquidation_px, "BTC down 12%: the pool holds less");
    }

    /// r2 revision 2 / r3 revision 3: a partial market liquidation puts the account in a cooldown (the whole position
    /// walks); 30 s later, every block of them applied, it is none.
    #[test]
    fn a_partial_liquidation_holds_the_cooldown_for_thirty_seconds() {
        let holder = checkpoint_user(HOLDER);
        let mut book = replay_book(1_000, 1_000, &[(holder, 1_000_000)]);
        book.apply_block(block_actions(1_001, &[]), vec![liquidation(1_001, 1, "1000000.0", "100000.0")], Vec::new(), |_| None, &crate::accounts::marks::fill_prices::FillMarkPrices::default(), Default::default());
        let mark = units(alone_px(-12_000.0 + 1_250.0, 900_000.0) * 1.02);
        let mut candidates = Candidates::new(None);
        let first = changes(candidates.refresh(&book, Some(&marks(1_001, &[(PUMP, mark)])), &[holder]));
        let (_, _, candidate) = &first.enter[0];
        assert_eq!((candidate.size_raw, candidate.order.cooldown, candidate.order.walk_raw), (900_000, Cooldown::Active, 900_000));
        for block in 1_002..=1_030 {
            book.apply_block_actions(block_actions(block, &[]));
        }
        assert_eq!(candidates.refresh(&book, Some(&marks(1_030, &[(PUMP, mark)])), &[]), Refresh::Unchanged, "29 s after it");
        book.apply_block_actions(block_actions(1_031, &[]));
        let expired = changes(candidates.refresh(&book, Some(&marks(1_031, &[(PUMP, mark)])), &[]));
        let [(_, _, updated)] = expired.update.as_slice() else { panic!("{expired:?}") };
        assert_eq!((updated.order.cooldown, updated.order.walk_raw), (Cooldown::None, 900_000), "11,250 USD: whole either way");
    }
}

#[cfg(test)]
mod walk_tests {
    use super::*;
    use crate::{
        accounts::{
            checkpoint::tests::{HOLDER, user as checkpoint_user},
            liquidation::solve,
            test_support::{ANCHOR_TABLE, ListedMarks, PUMP, block_actions, fixture_table, replay_book},
            walk::{WalkEnd, WalkPart},
        },
        order_book::Px,
    };

    /// HOLDER long 1,000,000 PUMP, 2% above its liquidation price at block 1000.
    fn book_and_marks() -> (AccountBook, ListedMarks) {
        let holder = checkpoint_user(HOLDER);
        let book = replay_book(1_000, 1_000, &[(holder, 1_000_000)]);
        let LiquidationPx::Computed(px) = solve(-12_000.0, 0.0, 1_000_000.0, &fixture_table(ANCHOR_TABLE)) else { panic!() };
        #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss, reason = "a small positive test price")]
        let mark = (px * 1.02 * 1e6).round() as u64;
        (book, ListedMarks { height: 1_000, marks: vec![(PUMP, mark)] })
    }

    fn result(key: &WalkKey, version: u64, withheld_periods: u64) -> WalkResult {
        let part = WalkPart { filled: 7, vwap_px: Some(0.012), first_px: None, deepest_px: Some(Px::new(1)), levels: 3, end: WalkEnd::Filled, steps: Vec::new() };
        let share = WalkShare { ahead: 0, together: part.clone(), alone: part };
        let mid_px = Some(0.0125);
        WalkResult { key: key.clone(), version, book_height: 9, book_time: 10, trusted: true, withheld_periods, mid_px, shares: vec![share] }
    }

    fn walk_of(refresh: &Refresh) -> Vec<BookWalk> {
        let Refresh::Changes(changes) = refresh else { panic!("{refresh:?}") };
        changes.enter.iter().chain(&changes.update).map(|(_, _, candidate)| candidate.walk.clone()).collect()
    }

    /// r2 revisions 3–4, r3 revision 2: an entering candidate is pending and requested; the newest request's result in
    /// the same trusted period is taken (assumption from the unknown cooldown); an older one, or one from before an
    /// untrusted period, is not; untrusted voids every estimate, trusted again re-requests.
    #[test]
    fn walk_results_count_only_for_the_newest_request_of_a_trusted_period() {
        let (mut book, marks) = book_and_marks();
        let trust = BookTrust::new();
        let mailbox = Arc::new(WalkMailbox::default());
        let mut candidates = Candidates::new(Some(BookLink { trust: trust.clone(), mailbox: Arc::clone(&mailbox) }));
        let entered = candidates.refresh(&book, Some(&marks), &[]);
        assert_eq!(walk_of(&entered), [BookWalk::Pending]);
        let key: WalkKey = (PUMP.to_string(), Side::Ask);
        let requests = mailbox.drain();
        let holder = checkpoint_user(HOLDER);
        assert_eq!(requests[&key], WalkRequest { version: 1, sequence: vec![(holder, 1_000_000 * 100_000_000, true)] });
        assert_eq!(candidates.apply_walk(result(&key, 0, 0)), Refresh::Unchanged, "an older request");
        let walked = candidates.apply_walk(result(&key, 1, 0));
        let [BookWalk::Walked { unknown_cooldown_as_full: true, book_height: 9, .. }] = walk_of(&walked).as_slice() else { panic!("{walked:?}") };
        trust.set_for_test(false);
        assert_eq!(walk_of(&candidates.refresh(&book, Some(&marks), &[])), [BookWalk::Untrusted], "no block needed");
        assert_eq!(candidates.apply_walk(result(&key, 1, 0)), Refresh::Unchanged, "untrusted now");
        trust.set_for_test(true);
        book.apply_block_actions(block_actions(1_001, &[]));
        let marks = ListedMarks { height: 1_001, marks: marks.marks.clone() };
        assert_eq!(walk_of(&candidates.refresh(&book, Some(&marks), &[])), [BookWalk::Pending], "trusted again");
        assert_eq!(mailbox.drain()[&key].version, 2, "requested anew");
        assert_eq!(candidates.apply_walk(result(&key, 2, 0)), Refresh::Unchanged, "read in the period before");
        assert!(matches!(candidates.apply_walk(result(&key, 2, 1)), Refresh::Changes(_)));
        assert!(mailbox.drain().is_empty(), "the same sequence is not requested again");
    }

    fn linked() -> (Candidates, BookTrust, Arc<WalkMailbox>) {
        let trust = BookTrust::new();
        let mailbox = Arc::new(WalkMailbox::default());
        let candidates = Candidates::new(Some(BookLink { trust: trust.clone(), mailbox: Arc::clone(&mailbox) }));
        (candidates, trust, mailbox)
    }

    /// Review 000446 r1 P2-1: an input without a block rebuilds no walk order (were it rebuilt, the forgotten request
    /// would be sent again).
    #[test]
    fn an_input_without_a_block_rebuilds_no_walk_order() {
        let (book, marks) = book_and_marks();
        let (mut candidates, _trust, mailbox) = linked();
        candidates.refresh(&book, Some(&marks), &[]);
        assert_eq!(mailbox.drain().len(), 1);
        candidates.requested.clear();
        assert_eq!(candidates.refresh(&book, Some(&marks), &[]), Refresh::Unchanged);
        assert!(mailbox.drain().is_empty(), "no block, the book's state unchanged: no order rebuilt");
    }

    /// Review 000446 r1 P2-3: an order change voids the estimates from the first changed place on; the ones before it
    /// walked the same list and stay. X sells first, then A, then B; A's walk size changes.
    #[test]
    fn a_changed_order_voids_the_estimates_after_it() {
        let (mut candidates, _trust, mailbox) = linked();
        candidates.trusted = Some(Trusted { anchor: 1, height: 2, time_ms: 3 });
        let candidate = |px: f64| Candidate {
            size_raw: 1_000,
            size_decimals: 0,
            height: 2,
            mark_raw: 4_000,
            liquidation_px: px,
            distance_bps: 100.0,
            isolated: false,
            pool_positions: 1,
            cash_estimated: false,
            cross_pool_coins: Vec::new(),
            order: ExpectedOrder::of(1_000, 4_000, Cooldown::None),
            walk: BookWalk::Pending,
        };
        let (x, a, b) = (Address::repeat_byte(1), Address::repeat_byte(2), Address::repeat_byte(3));
        let published: FxHashMap<Address, Candidate> = [(x, candidate(0.0039)), (a, candidate(0.0038)), (b, candidate(0.0037))].into_iter().collect();
        candidates.published.insert(PUMP.to_string(), published);
        let mut changes = candidates.changes_at(Trusted { anchor: 1, height: 2, time_ms: 3 });
        candidates.walk_book(&mut changes, true);
        let key: WalkKey = (PUMP.to_string(), Side::Ask);
        let request = mailbox.drain().remove(&key).unwrap();
        assert_eq!(request.sequence.iter().map(|(user, _, _)| *user).collect::<Vec<_>>(), [x, a, b], "from the highest price");
        let part = WalkPart { filled: 1, vwap_px: Some(1.0), first_px: None, deepest_px: None, levels: 1, end: WalkEnd::Filled, steps: Vec::new() };
        let share = WalkShare { ahead: 0, together: part.clone(), alone: part };
        let walked = WalkResult {
            key: key.clone(),
            version: request.version,
            book_height: 9,
            book_time: 10,
            trusted: true,
            withheld_periods: 0,
            mid_px: None,
            shares: vec![share; 3],
        };
        assert!(matches!(candidates.apply_walk(walked), Refresh::Changes(_)));
        candidates.published.get_mut(PUMP).unwrap().get_mut(&a).unwrap().order.walk_raw = 2_000;
        let mut changes = candidates.changes_at(Trusted { anchor: 1, height: 3, time_ms: 4 });
        candidates.walk_book(&mut changes, true);
        let pending: Vec<Address> = changes.update.iter().filter(|(_, _, candidate)| candidate.walk == BookWalk::Pending).map(|(_, user, _)| *user).collect();
        assert_eq!(pending, [a, b], "X walked the same prefix");
        assert!(matches!(candidates.published[PUMP][&x].walk, BookWalk::Walked { .. }));
        assert_eq!(mailbox.drain()[&key].version, request.version + 1);
    }

    /// Review 000446 r1 P2-2: a walk estimated before the book was withheld is not published as one, by the snapshot or
    /// by a frame, though no refresh ran since.
    #[test]
    fn a_walk_from_before_a_withheld_book_is_not_published() {
        let (book, marks) = book_and_marks();
        let (mut candidates, trust, mailbox) = linked();
        candidates.refresh(&book, Some(&marks), &[]);
        let key: WalkKey = (PUMP.to_string(), Side::Ask);
        let version = mailbox.drain()[&key].version;
        let Refresh::Changes(walked) = candidates.apply_walk(result(&key, version, 0)) else { panic!("taken") };
        let walk_now = |candidates: &Candidates| candidates.snapshot().unwrap().candidates[0].2.walk.clone();
        assert!(matches!(walk_now(&candidates), BookWalk::Walked { withheld_periods: 0, .. }));
        trust.set_for_test(false);
        assert_eq!(walk_now(&candidates), BookWalk::Untrusted);
        trust.set_for_test(true);
        assert_eq!(walk_now(&candidates), BookWalk::Pending, "trusted again in a later period, not walked anew yet");
        let walk = &walked.update[0].2.walk;
        assert_eq!(walk_as_of(walk, Some((true, 1))), BookWalk::Pending, "a frame sent before, going out now");
        assert_eq!(walk_as_of(walk, Some((true, 0))), walk.clone());
        assert_eq!(walk_as_of(walk, None), walk.clone(), "no book: as recorded");
    }
}
