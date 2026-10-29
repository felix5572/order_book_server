//! The forced flow's copy of the account view (hyperliquid-trade review 000563 r1–r2): what the forced flow reads of it
//! (the coins' size decimals, the node marks, the open positions and the liquidation candidates, with the heights they
//! stand at), kept on the forced flow thread from the account owner's events, so that a long synchronous step of the
//! owner (a candidates refresh, an anchor install) no longer holds the forced flow's frames. The account view stays the
//! one owner of these facts: the copy computes nothing of them, it applies what the owner sends, in order.
//!
//! - `Snapshot`: everything at one height under a new generation; when the copy holds none of the view (at start,
//!   after a new anchor, the view or the candidates coming back, a full queue, a copy asking for it).
//! - `Block`: after an input applied blocks, before the candidates' refresh, the positions those blocks touched and the
//!   marks that moved, at the new height (strictly increasing).
//! - `Candidates` / `CandidatesWithdrawn`: every change of the candidates (refresh, book walk, stop) through the
//!   publisher's one exit, by `version` each the previous plus one; `CandidatesConfirmed`: the height a refresh
//!   confirmed, changed or not, never going back.
//!
//! The owner never waits for the copy: a full queue drops the increments, and the next input sends a snapshot. Whether
//! the copy may serve is the owner's `watch` (`FlowAccountsValidity`), apart from the queue, so a full queue or the
//! owner's end withdraws the copy at once: the copy serves only the generation the watch says is valid, after that
//! generation's snapshot; a copy finding an event out of order stops serving and asks the owner for a snapshot.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use alloy::primitives::Address;
use log::error;
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::{
    mpsc::{self, error::TrySendError},
    watch,
};

use super::{
    book::{AccountBook, Touched},
    candidates::{Candidate, CandidateChanges, Candidates, CandidatesSnapshot, ExpectedOrder, in_liquidation_band, liquidation_band},
    checkpoint::mark_px,
    forced_flow::LiquidationInput,
    marks::MarksAt,
};
use crate::metrics::{FORCED_FLOW_ACCOUNTS_QUEUE_DEPTH, FORCED_FLOW_ACCOUNTS_RESYNC_TOTAL, FORCED_FLOW_ACCOUNTS_SNAPSHOT_SECONDS};

/// Events queued for the forced flow thread. A block's event holds a few to some hundred positions, a candidates'
/// change shares its `Arc` with the broadcast; a full queue costs a snapshot, not a wait (r2 revision 5: measured live).
pub(crate) const FLOW_ACCOUNTS_CAPACITY: usize = 256;

pub(crate) const OWNER_ENDED: &str = "the account view's owner ended";
const BEHIND: &str = "the forced flow fell behind the account view: resynchronizing";
const NO_SNAPSHOT: &str = "waiting for the account view's snapshot";

/// Whether the copy may serve, as the account owner says it (r2 revision 2): `state` for `generation`, the last one
/// it sent whole - `Ok` once that snapshot is queued, the reason while the view is unavailable or the copy behind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FlowAccountsValidity {
    pub(crate) generation: u64,
    pub(crate) state: Result<(), String>,
}

/// What the forced flow reads of the account view, whole, at one height.
#[derive(Debug)]
pub(crate) struct AccountsSnapshot {
    pub(crate) generation: u64,
    /// The account view's applied block and its time: the positions' and the marks'.
    pub(crate) height: u64,
    pub(crate) time_ms: u64,
    /// Every coin of the anchor's universe and its size decimals, held or not (`book.coins()`, `size_decimals`).
    pub(crate) coins: FxHashMap<String, u32>,
    /// The node marks the view claims (`MarksAt::mark`), price units.
    pub(crate) marks: FxHashMap<String, u64>,
    /// Each coin's open positions: signed sizes, raw.
    pub(crate) positions: FxHashMap<String, FxHashMap<Address, i64>>,
    pub(crate) candidates: Result<CandidatesSnapshot, String>,
}

impl AccountsSnapshot {
    /// The account view as it stands; `None` before it has replayed rows.
    pub(crate) fn of(generation: u64, book: &AccountBook, marks: &dyn MarksAt, candidates: &Candidates) -> Option<Self> {
        let (_, height, time_ms) = book.replay_at()?;
        let coins: FxHashMap<String, u32> = book.coins().filter_map(|coin| Some((coin.to_string(), book.size_decimals(coin)?))).collect();
        let claimed = coins.keys().filter_map(|coin| Some((coin.clone(), marks.mark(coin)?))).collect();
        let positions = coins
            .keys()
            .map(|coin| (coin.clone(), book.open_positions_raw(coin).collect::<FxHashMap<Address, i64>>()))
            .filter(|(_, held)| !held.is_empty())
            .collect();
        Some(Self { generation, height, time_ms, coins, marks: claimed, positions, candidates: candidates.snapshot() })
    }
}

/// The account owner's events to the forced flow's copy, in the order it made them.
#[derive(Debug)]
pub(crate) enum FlowAccountsEvent {
    Snapshot(Box<AccountsSnapshot>),
    /// The blocks an input applied: the positions they touched as they stand now (0: closed) and the marks that moved
    /// (`None`: no longer claimed).
    Block { generation: u64, height: u64, time_ms: u64, marks: Vec<(String, Option<u64>)>, positions: Vec<(String, Address, i64)> },
    Candidates { generation: u64, changes: Arc<CandidateChanges> },
    CandidatesConfirmed { generation: u64, height: u64, time_ms: u64 },
    CandidatesWithdrawn { generation: u64, reason: String },
}

impl FlowAccountsEvent {
    const fn generation(&self) -> u64 {
        match self {
            Self::Snapshot(snapshot) => snapshot.generation,
            Self::Block { generation, .. }
            | Self::Candidates { generation, .. }
            | Self::CandidatesConfirmed { generation, .. }
            | Self::CandidatesWithdrawn { generation, .. } => *generation,
        }
    }
}

/// The owner's feed and the forced flow's copy, linked: the copy's events arrive on the receiver.
pub(crate) fn link() -> (FlowAccountsFeed, FlowAccounts, mpsc::Receiver<FlowAccountsEvent>) {
    let (events, received) = mpsc::channel(FLOW_ACCOUNTS_CAPACITY);
    let (validity, watched) = watch::channel(FlowAccountsValidity { generation: 0, state: Err(NO_SNAPSHOT.to_string()) });
    let resync = Arc::new(AtomicBool::new(false));
    let feed = FlowAccountsFeed { events, validity, resync: Arc::clone(&resync), generation: 0, sent: None, ended: None, closed: false };
    (feed, FlowAccounts { validity: watched, generation: 0, copy: None, broken: None, resync }, received)
}

/// The forced flow's copy.
pub(crate) struct FlowAccounts {
    validity: watch::Receiver<FlowAccountsValidity>,
    /// The last generation installed.
    generation: u64,
    copy: Option<AccountsCopy>,
    /// An event out of order: nothing is served until a newer generation's snapshot.
    broken: Option<String>,
    /// Set for the owner when the copy needs a snapshot.
    resync: Arc<AtomicBool>,
}

impl FlowAccounts {
    /// The current generation's copy while the owner's watch holds it valid, else why not (r2 revision 2: checked
    /// whenever a frame is computed or published, the owner's end included, not only when the thread's loop takes the
    /// watch's notice): its coins, marks and positions hold.
    pub(crate) fn copy(&self) -> Result<&AccountsCopy, String> {
        if self.validity.has_changed().is_err() {
            return Err(OWNER_ENDED.to_string());
        }
        let generation = {
            let validity = self.validity.borrow();
            validity.state.clone()?;
            validity.generation
        };
        if let Some(reason) = &self.broken {
            return Err(reason.clone());
        }
        self.copy.as_ref().filter(|copy| copy.generation == generation).ok_or_else(|| NO_SNAPSHOT.to_string())
    }

    /// The copy while a frame may stand on it: `copy`, its candidates available too (review 000566 r1 P2-2: a withdrawal
    /// of the candidates alone withdraws every frame and voids the reads under way, as the view's does). The forced
    /// flow's one test of it, before a computation, a read taken back or a frame published.
    pub(crate) fn serving(&self) -> Result<&AccountsCopy, String> {
        let copy = self.copy()?;
        copy.candidates.as_ref().map_err(Clone::clone)?;
        Ok(copy)
    }

    /// The owner's watch moved; `Err` once the owner is gone. Cancel-safe.
    pub(crate) async fn validity_changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.validity.changed().await
    }

    pub(crate) fn apply(&mut self, event: FlowAccountsEvent) {
        let generation = event.generation();
        if let FlowAccountsEvent::Snapshot(snapshot) = event {
            return self.install(*snapshot);
        }
        // a generation the copy has left behind: its events are void
        if generation < self.generation || self.broken.is_some() {
            return;
        }
        let Some(copy) = self.copy.as_mut().filter(|copy| copy.generation == generation) else {
            return self.break_off(format!("an account event of generation {generation} before its snapshot"));
        };
        let applied = match event {
            FlowAccountsEvent::Snapshot(_) => unreachable!("installed above"),
            FlowAccountsEvent::Block { height, time_ms, marks, positions, .. } => copy.apply_block(height, time_ms, marks, positions),
            FlowAccountsEvent::Candidates { changes, .. } => copy.apply_candidates(&changes),
            FlowAccountsEvent::CandidatesConfirmed { height, time_ms, .. } => copy.confirm_candidates(height, time_ms),
            FlowAccountsEvent::CandidatesWithdrawn { reason, .. } => {
                copy.candidates = Err(reason);
                Ok(())
            }
        };
        if let Err(reason) = applied {
            self.break_off(reason);
        }
    }

    fn install(&mut self, snapshot: AccountsSnapshot) {
        if snapshot.generation <= self.generation {
            return self.break_off(format!("an account snapshot of generation {} after {}", snapshot.generation, self.generation));
        }
        self.generation = snapshot.generation;
        self.broken = None;
        self.copy = Some(AccountsCopy::of(snapshot));
    }

    /// Fail closed until the owner's next snapshot, which it is asked for.
    fn break_off(&mut self, reason: String) {
        error!("forced flow: the account copy stops serving until the next snapshot: {reason}");
        self.copy = None;
        self.broken = Some(format!("the account copy is resynchronizing: {reason}"));
        self.resync.store(true, Ordering::Relaxed);
    }
}

/// One generation's state since its snapshot.
#[derive(Debug)]
pub(crate) struct AccountsCopy {
    generation: u64,
    height: u64,
    time_ms: u64,
    coins: FxHashMap<String, u32>,
    marks: FxHashMap<String, u64>,
    positions: FxHashMap<String, FxHashMap<Address, i64>>,
    candidates: Result<CandidatesCopy, String>,
}

#[derive(Debug)]
struct CandidatesCopy {
    version: u64,
    /// The (height, block time) a refresh confirmed last.
    confirmed: (u64, u64),
    by_coin: FxHashMap<String, FxHashMap<Address, Candidate>>,
}

impl AccountsCopy {
    fn of(snapshot: AccountsSnapshot) -> Self {
        let candidates = snapshot.candidates.map(|candidates| {
            let mut by_coin: FxHashMap<String, FxHashMap<Address, Candidate>> = FxHashMap::default();
            for (coin, user, candidate) in candidates.candidates {
                by_coin.entry(coin).or_default().insert(user, candidate);
            }
            CandidatesCopy { version: candidates.version, confirmed: (candidates.height, candidates.time_ms), by_coin }
        });
        Self {
            generation: snapshot.generation,
            height: snapshot.height,
            time_ms: snapshot.time_ms,
            coins: snapshot.coins,
            marks: snapshot.marks,
            positions: snapshot.positions,
            candidates,
        }
    }

    fn apply_block(&mut self, height: u64, time_ms: u64, marks: Vec<(String, Option<u64>)>, positions: Vec<(String, Address, i64)>) -> Result<(), String> {
        if height <= self.height {
            return Err(format!("account block {height} after block {}", self.height));
        }
        (self.height, self.time_ms) = (height, time_ms);
        for (coin, mark) in marks {
            match mark {
                Some(mark) => self.marks.insert(coin, mark),
                None => self.marks.remove(&coin),
            };
        }
        for (coin, user, size_raw) in positions {
            let held = self.positions.entry(coin).or_default();
            if size_raw == 0 {
                held.remove(&user);
            } else {
                held.insert(user, size_raw);
            }
        }
        Ok(())
    }

    fn apply_candidates(&mut self, changes: &CandidateChanges) -> Result<(), String> {
        let candidates = self.candidates.as_mut().map_err(|reason| format!("candidate changes {} while withdrawn ({reason})", changes.version))?;
        if changes.version != candidates.version + 1 {
            return Err(format!("candidate changes {} after {}", changes.version, candidates.version));
        }
        candidates.version = changes.version;
        for (coin, user) in &changes.exit {
            if let Some(published) = candidates.by_coin.get_mut(coin) {
                published.remove(user);
            }
        }
        for (coin, user, candidate) in changes.enter.iter().chain(&changes.update) {
            candidates.by_coin.entry(coin.clone()).or_default().insert(*user, candidate.clone());
        }
        Ok(())
    }

    /// Never back; the same height again changes nothing (review 000563 r2).
    fn confirm_candidates(&mut self, height: u64, time_ms: u64) -> Result<(), String> {
        let candidates = self.candidates.as_mut().map_err(|reason| format!("candidates confirmed at {height} while withdrawn ({reason})"))?;
        if height < candidates.confirmed.0 {
            return Err(format!("candidates confirmed at {height} after {}", candidates.confirmed.0));
        }
        candidates.confirmed = (height, time_ms);
        Ok(())
    }

    /// The generation of the view the copy holds: frames and reads stand on one (review 000566 r1 P2-1).
    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    /// The (height, block time) of a frame's account part (r2 revision 1): the older of the positions' and marks' block
    /// and the candidates' confirmed one - a common lower bound of every account input, not one block of them all; the
    /// positions' alone while the candidates are withdrawn (no frame is served then).
    pub(crate) const fn accounts_at(&self) -> (u64, u64) {
        let positions = (self.height, self.time_ms);
        match &self.candidates {
            Ok(candidates) if candidates.confirmed.0 < self.height => candidates.confirmed,
            _ => positions,
        }
    }

    /// The coin's size decimals; `None` outside the anchor's universe.
    pub(crate) fn size_decimals(&self, coin: &str) -> Option<u32> {
        self.coins.get(coin).copied()
    }

    /// The node mark the view claims for the coin, price units.
    pub(crate) fn mark(&self, coin: &str) -> Option<u64> {
        self.marks.get(coin).copied()
    }

    /// `user`'s signed position in `coin`, raw; 0 without one.
    pub(crate) fn position_raw(&self, user: Address, coin: &str) -> i64 {
        self.positions.get(coin).and_then(|held| held.get(&user)).copied().unwrap_or(0)
    }

    /// The coins of the universe with a node mark, sorted.
    pub(crate) fn marked_coins(&self) -> Vec<String> {
        let mut coins: Vec<String> = self.coins.keys().filter(|coin| self.marks.contains_key(*coin)).cloned().collect();
        coins.sort_unstable();
        coins
    }

    /// The coin's candidates within `max_bps` of the node mark or the local one (the band `Candidates`'s index reads),
    /// each with its next market liquidation order at the node mark. Only the published candidates are here - within
    /// 1000 bp of the node mark at their refresh - whatever the tier, and the order's cooldown is the refresh's (r2
    /// revision 4, the scope accepted). `Err` while withdrawn.
    pub(crate) fn liquidation_inputs(&self, coin: &str, node_mark_raw: u64, local_mark_raw: Option<u64>, max_bps: u32) -> Result<Vec<LiquidationInput>, String> {
        let candidates = self.candidates.as_ref().map_err(Clone::clone)?;
        let (Some(published), Some(size_decimals)) = (candidates.by_coin.get(coin), self.size_decimals(coin)) else { return Ok(Vec::new()) };
        let mark = |raw: u64| mark_px(raw, size_decimals);
        let band = liquidation_band((mark(node_mark_raw), mark(local_mark_raw.unwrap_or(node_mark_raw))), max_bps);
        Ok(published
            .iter()
            .filter(|(_, candidate)| in_liquidation_band(candidate.size_raw, candidate.liquidation_px, band))
            .map(|(user, candidate)| LiquidationInput {
                user: *user,
                size_raw: candidate.size_raw,
                liquidation_px: candidate.liquidation_px,
                cash_estimated: candidate.cash_estimated,
                order: ExpectedOrder::of(candidate.size_raw, node_mark_raw, candidate.order.cooldown),
            })
            .collect())
    }
}

/// The account owner's side (r2): what the copy holds, so that only what moved goes out; it never waits for the copy.
pub(crate) struct FlowAccountsFeed {
    events: mpsc::Sender<FlowAccountsEvent>,
    validity: watch::Sender<FlowAccountsValidity>,
    resync: Arc<AtomicBool>,
    /// The last generation sent whole.
    generation: u64,
    /// What the copy holds since the last snapshot; `None` while it needs one.
    sent: Option<Sent>,
    /// The forced flow thread found gone, for the owner to withdraw the forced flow once.
    ended: Option<String>,
    closed: bool,
}

struct Sent {
    anchor_height: u64,
    height: u64,
    marks: FxHashMap<String, u64>,
    /// The candidates' confirmed height in the copy; `None` while they are withdrawn there.
    candidates_confirmed: Option<u64>,
}

/// An event that did not go out.
struct Lost;

impl FlowAccountsFeed {
    /// After an input or an anchor (`touched`: the rows the applied blocks changed): the copy brought to the view as
    /// it stands - a snapshot when it holds none of it (or another anchor), else the new block's positions and marks;
    /// the view or its marks unavailable withdraw it.
    pub(crate) fn on_applied(&mut self, book: &AccountBook, marks: Option<&dyn MarksAt>, touched: &Touched, candidates: &Candidates) {
        if self.closed {
            return;
        }
        if self.resync.swap(false, Ordering::Relaxed) {
            FORCED_FLOW_ACCOUNTS_RESYNC_TOTAL.with_label_values(&["requested"]).inc();
            self.sent = None;
        }
        let ready = book
            .availability()
            .map_err(|reason| format!("the account view is unavailable: {reason}"))
            .and_then(|_| marks.ok_or_else(|| "the node marks are unavailable".to_string()))
            .and_then(|marks| {
                let (anchor, height, time_ms) = book.replay_at().ok_or_else(|| "the account view has no replayed rows yet".to_string())?;
                Ok((marks, anchor.height, height, time_ms))
            });
        let (marks, anchor_height, height, time_ms) = match ready {
            Ok(ready) => ready,
            Err(reason) => {
                self.sent = None;
                return self.withhold(reason);
            }
        };
        match &self.sent {
            Some(sent) if sent.anchor_height == anchor_height => {
                if height > sent.height {
                    self.send_block(book, marks, touched, height, time_ms);
                }
            }
            _ => self.send_snapshot(book, marks, candidates, anchor_height),
        }
    }

    /// A change of the candidates, from the publisher's one exit (r2 revision 3).
    pub(crate) fn on_candidates(&mut self, changes: &Arc<CandidateChanges>) {
        let Some(sent) = &mut self.sent else { return };
        if sent.candidates_confirmed.is_none() {
            // back after a withdrawal: the copy takes them whole
            self.sent = None;
            return;
        }
        let generation = self.generation;
        drop(self.send(FlowAccountsEvent::Candidates { generation, changes: Arc::clone(changes) }));
    }

    pub(crate) fn on_candidates_withdrawn(&mut self, reason: &str) {
        let Some(sent) = &mut self.sent else { return };
        if sent.candidates_confirmed.take().is_none() {
            return;
        }
        let generation = self.generation;
        drop(self.send(FlowAccountsEvent::CandidatesWithdrawn { generation, reason: reason.to_string() }));
    }

    /// After a refresh: the height the candidates are confirmed at, changed or not (r2 revision 3).
    pub(crate) fn on_candidates_confirmed(&mut self, candidates: &Candidates) {
        let Some(sent) = &self.sent else { return };
        let Some((height, time_ms)) = candidates.confirmed() else { return };
        match sent.candidates_confirmed {
            // trusted again without a change to send: the copy takes them whole
            None => self.sent = None,
            Some(confirmed) if height > confirmed => {
                let generation = self.generation;
                if self.send(FlowAccountsEvent::CandidatesConfirmed { generation, height, time_ms }).is_ok()
                    && let Some(sent) = &mut self.sent
                {
                    sent.candidates_confirmed = Some(height);
                }
            }
            Some(_) => {}
        }
    }

    /// The forced flow thread found gone since the last call, once: the owner withdraws the forced flow.
    pub(crate) const fn take_ended(&mut self) -> Option<String> {
        self.ended.take()
    }

    fn send_block(&mut self, book: &AccountBook, marks: &dyn MarksAt, touched: &Touched, height: u64, time_ms: u64) {
        let Some(sent) = &self.sent else { return };
        let moved: Vec<(String, Option<u64>)> = book
            .coins()
            .filter_map(|coin| {
                let mark = marks.mark(coin);
                (sent.marks.get(coin).copied() != mark).then(|| (coin.to_string(), mark))
            })
            .collect();
        let mut seen: FxHashSet<(&str, Address)> = FxHashSet::default();
        let positions = touched
            .positions
            .iter()
            .filter(|(coin, user)| seen.insert((coin.as_str(), *user)))
            .map(|(coin, user)| (coin.clone(), *user, book.position_raw(*user, coin)))
            .collect();
        let generation = self.generation;
        let block = FlowAccountsEvent::Block { generation, height, time_ms, marks: moved.clone(), positions };
        if self.send(block).is_err() {
            return;
        }
        let Some(sent) = &mut self.sent else { return };
        sent.height = height;
        for (coin, mark) in moved {
            match mark {
                Some(mark) => sent.marks.insert(coin, mark),
                None => sent.marks.remove(&coin),
            };
        }
    }

    fn send_snapshot(&mut self, book: &AccountBook, marks: &dyn MarksAt, candidates: &Candidates, anchor_height: u64) {
        let started = Instant::now();
        let generation = self.generation + 1;
        let Some(snapshot) = AccountsSnapshot::of(generation, book, marks, candidates) else {
            return self.withhold("the account view has no replayed rows yet".to_string());
        };
        FORCED_FLOW_ACCOUNTS_SNAPSHOT_SECONDS.observe(started.elapsed().as_secs_f64());
        let sent = Sent {
            anchor_height,
            height: snapshot.height,
            marks: snapshot.marks.clone(),
            candidates_confirmed: snapshot.candidates.as_ref().ok().map(|candidates| candidates.height),
        };
        if self.send(FlowAccountsEvent::Snapshot(Box::new(snapshot))).is_err() {
            return;
        }
        // queued before it is declared valid: the copy serves it only once installed
        self.generation = generation;
        self.sent = Some(sent);
        self.validity.send_replace(FlowAccountsValidity { generation, state: Ok(()) });
    }

    /// Queues `event`. A full queue withdraws the copy until the next snapshot (r2 revision 2, before the queue has
    /// room again); a closed one ends the feed.
    fn send(&mut self, event: FlowAccountsEvent) -> Result<(), Lost> {
        match self.events.try_send(event) {
            Ok(()) => {
                let queued = self.events.max_capacity() - self.events.capacity();
                FORCED_FLOW_ACCOUNTS_QUEUE_DEPTH.set(i64::try_from(queued).unwrap_or(i64::MAX));
                Ok(())
            }
            Err(TrySendError::Full(_)) => {
                FORCED_FLOW_ACCOUNTS_RESYNC_TOTAL.with_label_values(&["full"]).inc();
                self.sent = None;
                self.withhold(BEHIND.to_string());
                Err(Lost)
            }
            Err(TrySendError::Closed(_)) => {
                FORCED_FLOW_ACCOUNTS_RESYNC_TOTAL.with_label_values(&["closed"]).inc();
                error!("forced flow: its thread is gone; the forced flow stays withdrawn until restart");
                self.sent = None;
                self.closed = true;
                self.ended = Some("the forced flow thread ended".to_string());
                Err(Lost)
            }
        }
    }

    fn withhold(&self, reason: String) {
        self.validity.send_if_modified(|validity| {
            let next = Err(reason);
            let changed = validity.state != next;
            validity.state = next;
            changed
        });
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::accounts::{
        candidates::BookWalk,
        checkpoint::tests::{HOLDER, user as checkpoint_user},
        cooldown::Cooldown,
        test_support::{ListedMarks, PUMP, block_actions, replay_book},
    };

    /// A copy serving the view as it stands (the owner's snapshot of it), with the watch that keeps it valid.
    pub(crate) fn serving(book: &AccountBook, marks: &dyn MarksAt, candidates: &Candidates) -> (FlowAccounts, watch::Sender<FlowAccountsValidity>) {
        let (validity, watched) = watch::channel(FlowAccountsValidity { generation: 1, state: Ok(()) });
        let mut accounts = FlowAccounts { validity: watched, generation: 0, copy: None, broken: None, resync: Arc::new(AtomicBool::new(false)) };
        let snapshot = AccountsSnapshot::of(1, book, marks, candidates).unwrap();
        accounts.apply(FlowAccountsEvent::Snapshot(Box::new(snapshot)));
        (accounts, validity)
    }

    fn holder() -> Address {
        checkpoint_user(HOLDER)
    }

    /// HOLDER long 1,000,000 PUMP at mark 0.0125, BTC at 100,020: the candidates trusted at block 1,000.
    fn view() -> (AccountBook, ListedMarks, Candidates) {
        let book = replay_book(1_000, 1_000, &[(holder(), 1_000_000)]);
        let marks = ListedMarks { height: 1_000, marks: vec![(PUMP, 12_500), ("BTC", 1_000_200)] };
        let mut candidates = Candidates::new(None);
        drop(candidates.refresh(&book, Some(&marks), &[]));
        (book, marks, candidates)
    }

    fn candidate(size_raw: i64, liquidation_px: f64) -> Candidate {
        Candidate {
            size_raw,
            size_decimals: 0,
            height: 1_000,
            mark_raw: 12_500,
            liquidation_px,
            distance_bps: 10.0,
            isolated: false,
            pool_positions: 1,
            cash_estimated: false,
            cross_pool_coins: Vec::new(),
            order: ExpectedOrder::of(size_raw, 12_500, Cooldown::Active),
            walk: BookWalk::Pending,
        }
    }

    fn changes(version: u64, enter: Vec<(&str, Address, Candidate)>, exit: Vec<(&str, Address)>) -> Arc<CandidateChanges> {
        Arc::new(CandidateChanges {
            version,
            height: 1_000,
            time_ms: 1_000_000,
            enter: enter.into_iter().map(|(coin, user, candidate)| (coin.to_string(), user, candidate)).collect(),
            update: Vec::new(),
            exit: exit.into_iter().map(|(coin, user)| (coin.to_string(), user)).collect(),
        })
    }

    fn other() -> Address {
        Address::repeat_byte(0x42)
    }

    /// The snapshot's facts, then blocks: positions and marks moved, heights strictly increasing; an older height is
    /// out of order and stops the copy until the next snapshot, which it asks for.
    #[test]
    fn blocks_move_the_copy_and_one_out_of_order_stops_it() {
        let (book, marks, candidates) = view();
        let (mut accounts, _validity) = serving(&book, &marks, &candidates);
        let copy = accounts.serving().unwrap();
        assert_eq!((copy.size_decimals(PUMP), copy.size_decimals("BTC"), copy.mark(PUMP)), (Some(0), Some(5), Some(12_500)));
        assert_eq!((copy.position_raw(holder(), PUMP), copy.marked_coins()), (1_000_000, vec!["BTC".to_string(), PUMP.to_string()]));
        accounts.apply(FlowAccountsEvent::Block {
            generation: 1,
            height: 1_001,
            time_ms: 1_001_000,
            marks: vec![(PUMP.to_string(), Some(12_600)), ("BTC".to_string(), None)],
            positions: vec![(PUMP.to_string(), holder(), 0), (PUMP.to_string(), other(), -5)],
        });
        let copy = accounts.serving().unwrap();
        assert_eq!((copy.mark(PUMP), copy.mark("BTC"), copy.marked_coins()), (Some(12_600), None, vec![PUMP.to_string()]));
        assert_eq!((copy.position_raw(holder(), PUMP), copy.position_raw(other(), PUMP)), (0, -5));
        assert_eq!(copy.accounts_at(), (1_000, 1_000_000), "the candidates are confirmed at 1,000 still");
        accounts.apply(FlowAccountsEvent::Block { generation: 1, height: 1_001, time_ms: 1_001_000, marks: Vec::new(), positions: Vec::new() });
        assert!(accounts.serving().unwrap_err().contains("account block 1001 after block 1001"));
        assert!(accounts.resync.load(Ordering::Relaxed), "asks the owner for a snapshot");
        accounts.apply(FlowAccountsEvent::Snapshot(Box::new(AccountsSnapshot::of(2, &book, &marks, &candidates).unwrap())));
        assert_eq!(accounts.serving().unwrap_err(), NO_SNAPSHOT, "the watch still names generation 1");
    }

    /// Review 000563 r1 P2-2: changes by version each the previous plus one (refresh, walk, refresh), the confirmed
    /// height moving on its own and never back; a version skipped stops the copy.
    #[test]
    fn candidates_follow_their_versions_and_confirmations() {
        let (book, marks, candidates) = view();
        let (mut accounts, _validity) = serving(&book, &marks, &candidates);
        let base = candidates.snapshot().unwrap().version;
        accounts.apply(FlowAccountsEvent::Block { generation: 1, height: 1_002, time_ms: 1_002_000, marks: Vec::new(), positions: Vec::new() });
        assert_eq!(accounts.serving().unwrap().accounts_at(), (1_000, 1_000_000), "the older of the two");
        for (version, enter) in [(base + 1, vec![(PUMP, other(), candidate(-5, 0.013))]), (base + 2, Vec::new()), (base + 3, Vec::new())] {
            accounts.apply(FlowAccountsEvent::Candidates { generation: 1, changes: changes(version, enter, Vec::new()) });
        }
        accounts.apply(FlowAccountsEvent::CandidatesConfirmed { generation: 1, height: 1_002, time_ms: 1_002_000 });
        accounts.apply(FlowAccountsEvent::CandidatesConfirmed { generation: 1, height: 1_002, time_ms: 1_002_000 });
        let copy = accounts.serving().unwrap();
        assert_eq!(copy.accounts_at(), (1_002, 1_002_000));
        let inputs = copy.liquidation_inputs(PUMP, 12_500, None, 1_000).unwrap();
        assert!(inputs.iter().any(|input| input.user == other() && input.size_raw == -5));
        accounts.apply(FlowAccountsEvent::CandidatesConfirmed { generation: 1, height: 1_001, time_ms: 1_001_000 });
        assert!(accounts.serving().unwrap_err().contains("confirmed at 1001 after 1002"));
        let (mut accounts, _validity) = serving(&book, &marks, &candidates);
        accounts.apply(FlowAccountsEvent::Candidates { generation: 1, changes: changes(base + 2, Vec::new(), Vec::new()) });
        assert!(accounts.serving().unwrap_err().contains(&format!("candidate changes {} after {base}", base + 2)));
    }

    /// The band of `Candidates`'s index: a short's within the tier of either mark, past it included; the order at the
    /// node mark with the refresh's cooldown; withdrawn candidates are no empty list.
    #[test]
    fn liquidation_inputs_read_the_band_of_either_mark() {
        let (book, marks, candidates) = view();
        let (mut accounts, _validity) = serving(&book, &marks, &candidates);
        let base = candidates.snapshot().unwrap().version;
        // shorts liquidated above 0.01258 (64 bp over the node mark) and 0.01249 (past it)
        let enter = vec![(PUMP, other(), candidate(-5, 0.012_58)), (PUMP, Address::repeat_byte(0x43), candidate(-7, 0.012_49))];
        accounts.apply(FlowAccountsEvent::Candidates { generation: 1, changes: changes(base + 1, enter, Vec::new()) });
        let users = |local: Option<u64>| {
            let inputs = accounts.serving().unwrap().liquidation_inputs(PUMP, 12_500, local, 50).unwrap();
            let mut users: Vec<_> = inputs.iter().filter(|input| input.size_raw < 0).map(|input| (input.user, input.order.cooldown)).collect();
            users.sort_unstable_by_key(|(user, _)| *user);
            users
        };
        assert_eq!(users(None), [(Address::repeat_byte(0x43), Cooldown::Active)], "the one past the node mark");
        assert_eq!(users(Some(12_530)).len(), 2, "within 50 bp of the local mark");
        accounts.apply(FlowAccountsEvent::CandidatesWithdrawn { generation: 1, reason: "gone".to_string() });
        assert_eq!(accounts.serving().unwrap_err(), "gone", "review 000566 r1 P2-2: no frame stands without them");
        let copy = accounts.copy().unwrap();
        assert_eq!((copy.liquidation_inputs(PUMP, 12_500, None, 50).unwrap_err(), copy.marked_coins().len()), ("gone".to_string(), 2));
    }

    /// The watch decides (review 000563 r2 revision 2): withheld, another generation, or the owner gone.
    #[test]
    fn the_owners_watch_decides_whether_the_copy_serves() {
        let (book, marks, candidates) = view();
        let (accounts, validity) = serving(&book, &marks, &candidates);
        validity.send_replace(FlowAccountsValidity { generation: 1, state: Err(BEHIND.to_string()) });
        assert_eq!(accounts.serving().unwrap_err(), BEHIND);
        validity.send_replace(FlowAccountsValidity { generation: 2, state: Ok(()) });
        assert_eq!(accounts.serving().unwrap_err(), NO_SNAPSHOT);
        validity.send_replace(FlowAccountsValidity { generation: 1, state: Ok(()) });
        assert!(accounts.serving().is_ok());
        drop(validity);
        assert_eq!(accounts.serving().unwrap_err(), OWNER_ENDED, "before the loop takes the notice");
    }

    /// The feed: a snapshot first, declared valid once queued; then blocks of what moved; the queue full withdraws the
    /// copy at once and drops what follows - while it stays full the copy serves nothing -, and the first input with
    /// room sends a new generation whole.
    #[test]
    fn a_full_queue_withdraws_the_copy_until_a_new_snapshot() {
        let (mut book, mut marks, candidates) = view();
        let (mut feed, mut accounts, mut events) = link();
        feed.on_applied(&book, Some(&marks), &Touched::default(), &candidates);
        let Ok(FlowAccountsEvent::Snapshot(snapshot)) = events.try_recv() else { panic!("a snapshot") };
        assert_eq!((snapshot.generation, snapshot.height), (1, 1_000));
        accounts.apply(FlowAccountsEvent::Snapshot(snapshot));
        assert!(accounts.serving().is_ok());
        book.apply_block_actions(block_actions(1_001, &[]));
        marks.height = 1_001;
        marks.marks[0].1 = 12_600;
        let touched = Touched { accounts: Vec::new(), positions: vec![(PUMP.to_string(), holder()), (PUMP.to_string(), holder())] };
        feed.on_applied(&book, Some(&marks), &touched, &candidates);
        let Ok(FlowAccountsEvent::Block { height, marks: moved, positions, .. }) = events.try_recv() else { panic!("a block") };
        assert_eq!((height, moved, positions), (1_001, vec![(PUMP.to_string(), Some(12_600))], vec![(PUMP.to_string(), holder(), 1_000_000)]));
        feed.on_applied(&book, Some(&marks), &touched, &candidates);
        assert!(events.try_recv().is_err(), "no new block");
        // fill the queue
        let mut height = 1_001;
        while feed.sent.is_some() {
            height += 1;
            book.apply_block_actions(block_actions(height, &[]));
            feed.on_applied(&book, Some(&marks), &Touched::default(), &candidates);
        }
        assert_eq!(accounts.serving().unwrap_err(), BEHIND, "withdrawn at once, the queue still full");
        feed.on_candidates_confirmed(&candidates);
        while let Ok(event) = events.try_recv() {
            accounts.apply(event);
        }
        assert_eq!(accounts.serving().unwrap_err(), BEHIND, "the increments drained are not the resynchronization");
        feed.on_applied(&book, Some(&marks), &Touched::default(), &candidates);
        let Ok(event @ FlowAccountsEvent::Snapshot(_)) = events.try_recv() else { panic!("a new snapshot") };
        assert_eq!(event.generation(), 2);
        accounts.apply(event);
        assert_eq!(accounts.serving().unwrap().accounts_at().0, 1_000, "the candidates' confirmation, older than block {height}");
    }

    /// The candidates' changes go out with their versions, confirmations only forward; a withdrawal, then the
    /// candidates trusted again with no change, makes the copy take them whole.
    #[test]
    fn candidates_back_after_a_withdrawal_go_out_whole() {
        let (book, marks, mut candidates) = view();
        let (mut feed, mut accounts, mut events) = link();
        feed.on_applied(&book, Some(&marks), &Touched::default(), &candidates);
        accounts.apply(events.try_recv().unwrap());
        feed.on_candidates_confirmed(&candidates);
        assert!(events.try_recv().is_err(), "the snapshot's own height");
        feed.on_candidates(&changes(candidates.snapshot().unwrap().version + 1, Vec::new(), Vec::new()));
        accounts.apply(events.try_recv().unwrap());
        assert!(accounts.serving().is_ok());
        let crate::accounts::candidates::Refresh::Withdrawn(reason) = candidates.stop("the book walk task ended".to_string()) else { panic!("withdrawn") };
        feed.on_candidates_withdrawn(&reason);
        accounts.apply(events.try_recv().unwrap());
        assert!(accounts.serving().is_err() && accounts.copy().unwrap().liquidation_inputs(PUMP, 12_500, None, 50).is_err());
        let (_, marks2, trusted) = view();
        feed.on_candidates_confirmed(&trusted);
        assert!(feed.sent.is_none(), "trusted again: a snapshot follows");
        feed.on_applied(&book, Some(&marks2), &Touched::default(), &trusted);
        let Ok(FlowAccountsEvent::Snapshot(snapshot)) = events.try_recv() else { panic!("a snapshot") };
        assert!(snapshot.candidates.is_ok());
    }

    /// The view or its marks unavailable withdraw the copy with the reason; a gone forced flow thread ends the feed once.
    #[test]
    fn an_unavailable_view_or_a_gone_thread() {
        let (book, marks, candidates) = view();
        let (mut feed, accounts, events) = link();
        feed.on_applied(&book, None, &Touched::default(), &candidates);
        assert_eq!(accounts.serving().unwrap_err(), "the node marks are unavailable");
        drop(events);
        feed.on_applied(&book, Some(&marks), &Touched::default(), &candidates);
        assert_eq!(feed.take_ended(), Some("the forced flow thread ended".to_string()));
        assert_eq!(feed.take_ended(), None);
        feed.on_applied(&book, Some(&marks), &Touched::default(), &candidates);
        assert_eq!(feed.take_ended(), None, "once");
    }
}
