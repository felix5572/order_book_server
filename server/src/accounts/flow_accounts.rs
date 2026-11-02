//! The forced flow's copy of the account view (hyperliquid-trade review 000563 r1–r2) - and since batch 2m the pending
//! book's, a second one fed the same way (design 000652 r2, `CopyOwner`): what the forced flow reads of it
//! (the coins' size decimals, the node marks, the open positions and the liquidation candidates, with the heights they
//! stand at), kept on the forced flow thread from the account owner's events, so that a long synchronous step of the
//! owner (a candidates refresh, an anchor install) no longer holds the forced flow's frames. The account view stays the
//! one owner of these facts: the copy computes nothing of them, it applies what the owner sends, in order.
//!
//! - `Snapshot`: everything at one height under a new generation; when the copy holds none of the view (at start,
//!   after a new anchor, the view or the candidates coming back, a full queue, a copy asking for it).
//! - `Block`: after an input applied blocks, before the candidates' refresh, the positions those blocks touched and the
//!   marks that moved, at the new height (strictly increasing).
//! - `Trailing` (the pending book's copy alone, batch 2n-2): at the copy's height, the trailing stops the node marks
//!   changed with no block - rebuilt at it - and the refresh deadline; a block's own changes go with its `Block`, so the
//!   copy never stands at a height with another height's stops (review 000662 r1 P2-1).
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
use prometheus::{IntCounterVec, IntGauge};
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
    marks::{MarksAt, trailing::TrailingEntry},
};
use crate::metrics::{
    FORCED_FLOW_ACCOUNTS_QUEUE_DEPTH, FORCED_FLOW_ACCOUNTS_RESYNC_TOTAL, FORCED_FLOW_ACCOUNTS_SNAPSHOT_SECONDS, PENDING_BOOK_ACCOUNTS_QUEUE_DEPTH,
    PENDING_BOOK_ACCOUNTS_RESYNC_TOTAL, PENDING_BOOK_ACCOUNTS_SNAPSHOT_SECONDS,
};

/// Events queued for the forced flow thread. A block's event holds a few to some hundred positions, a candidates'
/// change shares its `Arc` with the broadcast; a full queue costs a snapshot, not a wait (r2 revision 5: measured live).
pub(crate) const FLOW_ACCOUNTS_CAPACITY: usize = 256;

pub(crate) const OWNER_ENDED: &str = "the account view's owner ended";
const BEHIND: &str = "the copy fell behind the account view: resynchronizing";
const NO_SNAPSHOT: &str = "waiting for the account view's snapshot";

/// The thread a copy of the view serves, each copy its own queue, watch and counts: one ending or falling behind leaves
/// the other as it is (design 000652 r2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CopyOwner {
    ForcedFlow,
    PendingBook,
}

impl CopyOwner {
    const fn label(self) -> &'static str {
        match self {
            Self::ForcedFlow => "forced flow",
            Self::PendingBook => "pending book",
        }
    }

    fn resyncs(self, cause: &str) {
        let resyncs: &IntCounterVec = match self {
            Self::ForcedFlow => &FORCED_FLOW_ACCOUNTS_RESYNC_TOTAL,
            Self::PendingBook => &PENDING_BOOK_ACCOUNTS_RESYNC_TOTAL,
        };
        resyncs.with_label_values(&[cause]).inc();
    }

    fn queued(self, events: usize) {
        let depth: &IntGauge = match self {
            Self::ForcedFlow => &FORCED_FLOW_ACCOUNTS_QUEUE_DEPTH,
            Self::PendingBook => &PENDING_BOOK_ACCOUNTS_QUEUE_DEPTH,
        };
        depth.set(i64::try_from(events).unwrap_or(i64::MAX));
    }

    fn snapshot_built(self, seconds: f64) {
        match self {
            Self::ForcedFlow => FORCED_FLOW_ACCOUNTS_SNAPSHOT_SECONDS.observe(seconds),
            Self::PendingBook => PENDING_BOOK_ACCOUNTS_SNAPSHOT_SECONDS.observe(seconds),
        }
    }
}

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
    /// The pending book's copy alone (batch 2n-2).
    pub(crate) trailing: Option<TrailingCopy>,
}

/// The main dex's trailing stops the node marks hold at the copy's height (batch 2n-2, design 000659 r2 s2): each by
/// oid with its coin, price units; the earliest block time an automatic refresh may write the marks (`None`: off); and
/// the main-dex coins whose mark the node marks claim - a coin's stops are not known without (review 000662 r2 P2-2: a
/// claim withdrawn at the same height changes them as their stops do).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TrailingCopy {
    pub(crate) stops: FxHashMap<u64, (String, TrailingEntry)>,
    pub(crate) refresh_due_ms: Option<u64>,
    pub(crate) claimed: FxHashSet<String>,
}

impl TrailingCopy {
    /// What changed from `self` to `now`: each stop changed, added (`Some`) or gone (`None`), and the deadline.
    fn changes(&self, now: &Self) -> Option<TrailingChanges> {
        let changed = now.stops.iter().filter(|(oid, stop)| self.stops.get(*oid) != Some(*stop)).map(|(oid, stop)| (*oid, Some(stop.clone())));
        let gone = self.stops.keys().filter(|oid| !now.stops.contains_key(*oid)).map(|oid| (*oid, None));
        let updates: Vec<_> = changed.chain(gone).collect();
        let claimed = (self.claimed != now.claimed).then(|| now.claimed.clone());
        let unchanged = updates.is_empty() && self.refresh_due_ms == now.refresh_due_ms && claimed.is_none();
        (!unchanged).then_some(TrailingChanges { updates, refresh_due_ms: now.refresh_due_ms, claimed })
    }
}

/// The trailing stops as the pending book's copy holds them (batch 2n-2): the owner's, each with the height it was last
/// set at - its snapshot's, or that of the block, or of the marks rebuilt, changing it (review 000666 r1: a stop set
/// after a height does not tell its state there).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CopiedTrailing {
    stops: FxHashMap<u64, CopiedStop>,
    pub(crate) refresh_due_ms: Option<u64>,
    pub(crate) claimed: FxHashSet<String>,
}

/// A stop the copy holds: its coin, as the node marks hold it, and the height it was last set at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CopiedStop {
    pub(crate) coin: String,
    pub(crate) entry: TrailingEntry,
    pub(crate) set_at: u64,
}

impl CopiedTrailing {
    fn of(sent: TrailingCopy, height: u64) -> Self {
        let stops = sent.stops.into_iter().map(|(oid, (coin, entry))| (oid, CopiedStop { coin, entry, set_at: height })).collect();
        Self { stops, refresh_due_ms: sent.refresh_due_ms, claimed: sent.claimed }
    }

    /// `changes` at `height`.
    fn apply(&mut self, changes: TrailingChanges, height: u64) {
        for (oid, stop) in changes.updates {
            match stop {
                Some((coin, entry)) => self.stops.insert(oid, CopiedStop { coin, entry, set_at: height }),
                None => self.stops.remove(&oid),
            };
        }
        self.refresh_due_ms = changes.refresh_due_ms;
        if let Some(claimed) = changes.claimed {
            self.claimed = claimed;
        }
    }

    /// Every stop taken as set at `height`: the blocks up to it came in one, what each set between them not seen (review
    /// 000666 r2: equal ends prove nothing of the blocks between).
    fn set_all_at(&mut self, height: u64) {
        self.stops.values_mut().for_each(|stop| stop.set_at = height);
    }

    pub(crate) fn stop(&self, oid: u64) -> Option<&CopiedStop> {
        self.stops.get(&oid)
    }

    /// The stops as the owner sent them.
    #[cfg(test)]
    pub(crate) fn sent(&self) -> TrailingCopy {
        let stops = self.stops.iter().map(|(oid, stop)| (*oid, (stop.coin.clone(), stop.entry))).collect();
        TrailingCopy { stops, refresh_due_ms: self.refresh_due_ms, claimed: self.claimed.clone() }
    }
}

/// The trailing stops changed at one height: (oid, the stop as it stands, `None` gone), the refresh deadline, and the
/// claimed coins when they changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrailingChanges {
    pub(crate) updates: Vec<(u64, Option<(String, TrailingEntry)>)>,
    pub(crate) refresh_due_ms: Option<u64>,
    pub(crate) claimed: Option<FxHashSet<String>>,
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
        Some(Self { generation, height, time_ms, coins, marks: claimed, positions, candidates: candidates.snapshot(), trailing: None })
    }
}

/// The account owner's events to the forced flow's copy, in the order it made them.
#[derive(Debug)]
pub(crate) enum FlowAccountsEvent {
    Snapshot(Box<AccountsSnapshot>),
    /// The blocks an input applied: the positions they touched as they stand now (0: closed) and the marks that moved
    /// (`None`: no longer claimed).
    Block {
        generation: u64,
        height: u64,
        time_ms: u64,
        marks: Vec<(String, Option<u64>)>,
        positions: Vec<(String, Address, i64)>,
        /// The pending book's copy alone: what of the trailing stops the blocks changed (batch 2n-2).
        trailing: Option<TrailingChanges>,
    },
    /// The pending book's copy alone: at the copy's height, what of the trailing stops changed (batch 2n-2).
    Trailing { generation: u64, height: u64, changes: TrailingChanges },
    Candidates { generation: u64, changes: Arc<CandidateChanges> },
    CandidatesConfirmed { generation: u64, height: u64, time_ms: u64 },
    CandidatesWithdrawn { generation: u64, reason: String },
}

impl FlowAccountsEvent {
    const fn generation(&self) -> u64 {
        match self {
            Self::Snapshot(snapshot) => snapshot.generation,
            Self::Block { generation, .. }
            | Self::Trailing { generation, .. }
            | Self::Candidates { generation, .. }
            | Self::CandidatesConfirmed { generation, .. }
            | Self::CandidatesWithdrawn { generation, .. } => *generation,
        }
    }
}

/// The owner's feed and `owner`'s copy, linked: the copy's events arrive on the receiver.
pub(crate) fn link(owner: CopyOwner) -> (FlowAccountsFeed, FlowAccounts, mpsc::Receiver<FlowAccountsEvent>) {
    let (events, received) = mpsc::channel(FLOW_ACCOUNTS_CAPACITY);
    let (validity, watched) = watch::channel(FlowAccountsValidity { generation: 0, state: Err(NO_SNAPSHOT.to_string()) });
    let resync = Arc::new(AtomicBool::new(false));
    let feed = FlowAccountsFeed { owner, events, validity, resync: Arc::clone(&resync), generation: 0, sent: None, ended: None, closed: false };
    (feed, FlowAccounts { owner, validity: watched, generation: 0, copy: None, broken: None, resync }, received)
}

/// A thread's copy.
pub(crate) struct FlowAccounts {
    owner: CopyOwner,
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
    /// The (generation, height) of the copy serving now; `None` while none does (batch 2n-2: what the pending thread
    /// read its trailing stops at, checked before each input).
    pub(crate) fn serving_at(&self) -> Option<(u64, u64)> {
        self.copy().ok().map(|copy| (copy.generation(), copy.positions_at().0))
    }

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
            FlowAccountsEvent::Block { height, time_ms, marks, positions, trailing, .. } => copy.apply_block(height, time_ms, marks, positions, trailing),
            FlowAccountsEvent::Trailing { height, changes, .. } => copy.apply_trailing(height, changes),
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
        error!("{}: the account copy stops serving until the next snapshot: {reason}", self.owner.label());
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
    changed: ChangedSinceConfirmed,
    /// The pending book's copy alone (batch 2n-2).
    trailing: Option<CopiedTrailing>,
}

#[derive(Debug)]
struct CandidatesCopy {
    version: u64,
    /// The (height, block time) a refresh confirmed last.
    confirmed: (u64, u64),
    by_coin: FxHashMap<String, FxHashMap<Address, Candidate>>,
}

/// The positions the copy saw change after the candidates' confirmed height (hyperliquid-trade review 000569): a
/// candidate's liquidation price is its refresh's, of the account as it stood then, so a candidate whose account moved
/// since is withheld until a refresh confirms past the change - a liquidation that happened, a partial one's remainder,
/// the holder's own trade alike.
#[derive(Debug)]
struct ChangedSinceConfirmed {
    /// Each user's coins whose size changed, at the latest height it did.
    by_user: FxHashMap<Address, FxHashMap<String, u64>>,
    /// A snapshot's height while its positions were newer than its candidates: which changed in between is unknown.
    unknown_through: Option<u64>,
}

impl ChangedSinceConfirmed {
    fn of(positions_height: u64, candidates_height: Option<u64>) -> Self {
        let unknown_through = candidates_height.filter(|height| *height < positions_height).map(|_| positions_height);
        Self { by_user: FxHashMap::default(), unknown_through }
    }

    fn note(&mut self, user: Address, coin: &str, height: u64) {
        self.by_user.entry(user).or_default().insert(coin.to_string(), height);
    }

    /// A refresh at `height` re-evaluated every account touched through it: what changed then is in its candidates.
    fn confirm(&mut self, height: u64) {
        self.by_user.retain(|_, coins| {
            coins.retain(|_, changed| *changed > height);
            !coins.is_empty()
        });
        self.unknown_through = self.unknown_through.filter(|through| *through > height);
    }

    /// An isolated position's price is its own coin's alone; a cross one's moves with any position of its pool, which
    /// a refresh lists (`cross_pool_coins`) but a position opened since is not in: any coin of the user's counts.
    fn withholds(&self, user: Address, coin: &str, candidate: &Candidate) -> bool {
        if self.unknown_through.is_some() {
            return true;
        }
        let Some(coins) = self.by_user.get(&user) else { return false };
        if candidate.isolated { coins.contains_key(coin) } else { !coins.is_empty() }
    }
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
        // the owner sends a snapshot after an input and before its refresh: its positions may be a block ahead
        let changed = ChangedSinceConfirmed::of(snapshot.height, candidates.as_ref().ok().map(|candidates| candidates.confirmed.0));
        Self {
            generation: snapshot.generation,
            height: snapshot.height,
            time_ms: snapshot.time_ms,
            coins: snapshot.coins,
            marks: snapshot.marks,
            positions: snapshot.positions,
            candidates,
            changed,
            trailing: snapshot.trailing.map(|sent| CopiedTrailing::of(sent, snapshot.height)),
        }
    }

    /// At the copy's height (after its block, or the marks rebuilt at it): never another height, nor a copy keeping none.
    fn apply_trailing(&mut self, height: u64, changes: TrailingChanges) -> Result<(), String> {
        if height != self.height {
            return Err(format!("trailing stops of block {height} at block {}", self.height));
        }
        let trailing = self.trailing.as_mut().ok_or("trailing stops sent a copy keeping none")?;
        trailing.apply(changes, height);
        Ok(())
    }

    /// The trailing stops the node marks hold at the copy's height (`positions_at`); `None` for a copy keeping none.
    pub(crate) const fn trailing(&self) -> Option<&CopiedTrailing> {
        self.trailing.as_ref()
    }

    fn apply_block(
        &mut self,
        height: u64,
        time_ms: u64,
        marks: Vec<(String, Option<u64>)>,
        positions: Vec<(String, Address, i64)>,
        trailing: Option<TrailingChanges>,
    ) -> Result<(), String> {
        if height <= self.height {
            return Err(format!("account block {height} after block {}", self.height));
        }
        if let Some(changes) = trailing {
            self.trailing.as_mut().ok_or("a block's trailing stops sent a copy keeping none")?.apply(changes, height);
        }
        if height > self.height + 1
            && let Some(trailing) = self.trailing.as_mut()
        {
            trailing.set_all_at(height);
        }
        (self.height, self.time_ms) = (height, time_ms);
        for (coin, mark) in marks {
            match mark {
                Some(mark) => self.marks.insert(coin, mark),
                None => self.marks.remove(&coin),
            };
        }
        for (coin, user, size_raw) in positions {
            let held = self.positions.entry(coin.clone()).or_default();
            let before = if size_raw == 0 { held.remove(&user) } else { held.insert(user, size_raw) };
            // touched at the same size changes no price
            if before.unwrap_or(0) != size_raw {
                self.changed.note(user, &coin, height);
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
        self.changed.confirm(height);
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

    /// The (height, block time) of the positions and node marks the copy holds: the account view's applied block, at or
    /// above `accounts_at`.
    pub(crate) const fn positions_at(&self) -> (u64, u64) {
        (self.height, self.time_ms)
    }

    /// The coin's size decimals; `None` outside the anchor's universe.
    pub(crate) fn size_decimals(&self, coin: &str) -> Option<u32> {
        self.coins.get(coin).copied()
    }

    /// Every coin of the anchor's universe and its size decimals.
    pub(crate) const fn size_decimals_by_coin(&self) -> &FxHashMap<String, u32> {
        &self.coins
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
    /// revision 4, the scope accepted); one whose account moved since its refresh is withheld until a refresh confirms
    /// past the change (review 000569). In user order, so two calls over the same facts compare equal. `Err` while
    /// withdrawn.
    pub(crate) fn liquidation_inputs(&self, coin: &str, node_mark_raw: u64, local_mark_raw: Option<u64>, max_bps: u32) -> Result<Vec<LiquidationInput>, String> {
        let candidates = self.candidates.as_ref().map_err(Clone::clone)?;
        let (Some(published), Some(size_decimals)) = (candidates.by_coin.get(coin), self.size_decimals(coin)) else { return Ok(Vec::new()) };
        let mark = |raw: u64| mark_px(raw, size_decimals);
        let band = liquidation_band((mark(node_mark_raw), mark(local_mark_raw.unwrap_or(node_mark_raw))), max_bps);
        let mut inputs: Vec<LiquidationInput> = published
            .iter()
            .filter(|(_, candidate)| in_liquidation_band(candidate.size_raw, candidate.liquidation_px, band))
            .filter(|(user, candidate)| !self.changed.withholds(**user, coin, candidate))
            .map(|(user, candidate)| LiquidationInput {
                user: *user,
                size_raw: candidate.size_raw,
                liquidation_px: candidate.liquidation_px,
                cash_estimated: candidate.cash_estimated,
                order: ExpectedOrder::of(candidate.size_raw, node_mark_raw, candidate.order.cooldown),
            })
            .collect();
        inputs.sort_unstable_by_key(|input| input.user);
        Ok(inputs)
    }
}

/// The account owner's side (r2): what the copy holds, so that only what moved goes out; it never waits for the copy.
pub(crate) struct FlowAccountsFeed {
    owner: CopyOwner,
    events: mpsc::Sender<FlowAccountsEvent>,
    validity: watch::Sender<FlowAccountsValidity>,
    resync: Arc<AtomicBool>,
    /// The last generation sent whole.
    generation: u64,
    /// What the copy holds since the last snapshot; `None` while it needs one.
    sent: Option<Sent>,
    /// The copy's thread found gone, once: the owner withdraws the forced flow on it.
    ended: Option<String>,
    closed: bool,
}

struct Sent {
    anchor_height: u64,
    height: u64,
    marks: FxHashMap<String, u64>,
    /// The candidates' confirmed height in the copy; `None` while they are withdrawn there.
    candidates_confirmed: Option<u64>,
    /// The trailing stops the copy holds: the pending book's alone.
    trailing: Option<TrailingCopy>,
    /// The marks' revision they were last read at (`MarksAt::revision`).
    trailing_revision: u64,
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
            self.owner.resyncs("requested");
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
        // the trailing stops are of the marks' height: a copy standing at the view's needs both there (review 000662 r2
        // P2-1: marks rebuilt from a newer checkpoint ahead of the view); a full snapshot once they are again
        if self.keeps_trailing() && marks.height() != height {
            self.sent = None;
            return self.withhold(format!("the node marks stand at block {}, the account view at {height}", marks.height()));
        }
        match &self.sent {
            Some(sent) if sent.anchor_height == anchor_height => {
                if height > sent.height {
                    self.send_block(book, marks, touched, height, time_ms);
                } else {
                    self.send_trailing(marks);
                }
            }
            _ => self.send_snapshot(book, marks, candidates, anchor_height),
        }
    }

    /// Only the pending book's copy keeps the trailing stops.
    fn keeps_trailing(&self) -> bool {
        self.owner == CopyOwner::PendingBook
    }

    /// The trailing stops changed since sent, with them as they now stand: the pending book's copy alone.
    fn trailing_changes(&self, marks: &dyn MarksAt) -> Option<(TrailingChanges, TrailingCopy)> {
        let held = self.sent.as_ref().filter(|_| self.keeps_trailing())?.trailing.as_ref()?;
        let now = marks.trailing().unwrap_or_default();
        Some((held.changes(&now)?, now))
    }

    /// The trailing stops changed with no block, at the height the copy holds (the marks rebuilt at it): read once a
    /// revision of the marks.
    fn send_trailing(&mut self, marks: &dyn MarksAt) {
        let revision = marks.revision();
        let Some(sent) = self.sent.as_mut().filter(|sent| sent.trailing_revision != revision) else { return };
        sent.trailing_revision = revision;
        let Some((changes, now)) = self.trailing_changes(marks) else { return };
        let Some(height) = self.sent.as_ref().map(|sent| sent.height) else { return };
        let generation = self.generation;
        if self.send(FlowAccountsEvent::Trailing { generation, height, changes }).is_ok()
            && let Some(sent) = &mut self.sent
        {
            sent.trailing = Some(now);
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

    /// The copy's thread found gone since the last call, once.
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
        let (trailing, trailing_now) = self.trailing_changes(marks).unzip();
        let block = FlowAccountsEvent::Block { generation, height, time_ms, marks: moved.clone(), positions, trailing };
        if self.send(block).is_err() {
            return;
        }
        let Some(sent) = &mut self.sent else { return };
        sent.height = height;
        sent.trailing_revision = marks.revision();
        if let Some(now) = trailing_now {
            sent.trailing = Some(now);
        }
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
        self.owner.snapshot_built(started.elapsed().as_secs_f64());
        let mut snapshot = snapshot;
        snapshot.trailing = self.keeps_trailing().then(|| marks.trailing().unwrap_or_default());
        let sent = Sent {
            anchor_height,
            height: snapshot.height,
            marks: snapshot.marks.clone(),
            candidates_confirmed: snapshot.candidates.as_ref().ok().map(|candidates| candidates.height),
            trailing: snapshot.trailing.clone(),
            trailing_revision: marks.revision(),
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
                self.owner.queued(self.events.max_capacity() - self.events.capacity());
                Ok(())
            }
            Err(TrySendError::Full(_)) => {
                self.owner.resyncs("full");
                self.sent = None;
                self.withhold(BEHIND.to_string());
                Err(Lost)
            }
            Err(TrySendError::Closed(_)) => {
                self.owner.resyncs("closed");
                let owner = self.owner.label();
                error!("{owner}: its thread is gone; its account copy stays closed until restart");
                self.sent = None;
                self.closed = true;
                self.ended = Some(format!("the {owner} thread ended"));
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
        let mut accounts = FlowAccounts {
            owner: CopyOwner::ForcedFlow,
            validity: watched,
            generation: 0,
            copy: None,
            broken: None,
            resync: Arc::new(AtomicBool::new(false)),
        };
        let snapshot = AccountsSnapshot::of(1, book, marks, candidates).unwrap();
        accounts.apply(FlowAccountsEvent::Snapshot(Box::new(snapshot)));
        (accounts, validity)
    }

    /// A copy of the pending book serving `snapshot` as it is, with the watch that keeps it valid.
    pub(crate) fn copy_of(snapshot: AccountsSnapshot) -> (FlowAccounts, watch::Sender<FlowAccountsValidity>) {
        let (validity, watched) = watch::channel(FlowAccountsValidity { generation: snapshot.generation, state: Ok(()) });
        let mut accounts = FlowAccounts {
            owner: CopyOwner::PendingBook,
            validity: watched,
            generation: 0,
            copy: None,
            broken: None,
            resync: Arc::new(AtomicBool::new(false)),
        };
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
            trailing: None,
        });
        let copy = accounts.serving().unwrap();
        assert_eq!((copy.mark(PUMP), copy.mark("BTC"), copy.marked_coins()), (Some(12_600), None, vec![PUMP.to_string()]));
        assert_eq!((copy.position_raw(holder(), PUMP), copy.position_raw(other(), PUMP)), (0, -5));
        assert_eq!(copy.accounts_at(), (1_000, 1_000_000), "the candidates are confirmed at 1,000 still");
        accounts.apply(FlowAccountsEvent::Block { generation: 1, height: 1_001, time_ms: 1_001_000, marks: Vec::new(), positions: Vec::new(), trailing: None });
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
        accounts.apply(FlowAccountsEvent::Block { generation: 1, height: 1_002, time_ms: 1_002_000, marks: Vec::new(), positions: Vec::new(), trailing: None });
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

    /// The users listed within 50 bp of the node mark 0.0125, shorts only (HOLDER's long is no candidate there).
    fn listed_shorts(accounts: &FlowAccounts) -> Vec<(Address, i64)> {
        let inputs = accounts.serving().unwrap().liquidation_inputs(PUMP, 12_500, None, 50).unwrap();
        inputs.iter().filter(|input| input.size_raw < 0).map(|input| (input.user, input.size_raw)).collect()
    }

    fn block(height: u64, positions: Vec<(&str, Address, i64)>) -> FlowAccountsEvent {
        let positions = positions.into_iter().map(|(coin, user, size_raw)| (coin.to_string(), user, size_raw)).collect();
        FlowAccountsEvent::Block { generation: 1, height, time_ms: height * 1_000, marks: Vec::new(), positions, trailing: None }
    }

    fn confirmed(height: u64) -> FlowAccountsEvent {
        FlowAccountsEvent::CandidatesConfirmed { generation: 1, height, time_ms: height * 1_000 }
    }

    /// Review 000569: a cross candidate is withheld from the block its account's size changed in - its own coin or
    /// another, a partial liquidation's remainder, a full one - until a refresh confirms past that block; a position
    /// touched at the same size withholds nothing; a confirmation clears the changes through its height only.
    #[test]
    fn a_candidate_whose_account_moved_waits_for_a_refresh_past_it() {
        let (book, marks, candidates) = view();
        let (mut accounts, _validity) = serving(&book, &marks, &candidates);
        let base = candidates.snapshot().unwrap().version;
        // a short liquidated above 0.01249, past the node mark
        accounts.apply(FlowAccountsEvent::Candidates { generation: 1, changes: changes(base + 1, vec![(PUMP, other(), candidate(-5, 0.012_49))], Vec::new()) });
        assert_eq!(listed_shorts(&accounts), [(other(), -5)]);
        accounts.apply(block(1_001, vec![(PUMP, other(), -5)]));
        assert!(listed_shorts(&accounts).is_empty(), "opened in the copy's sight: a change");
        accounts.apply(confirmed(1_001));
        assert_eq!(listed_shorts(&accounts), [(other(), -5)]);
        accounts.apply(block(1_002, vec![(PUMP, other(), -5)]));
        assert_eq!(listed_shorts(&accounts), [(other(), -5)], "touched at the same size");
        accounts.apply(block(1_003, vec![(PUMP, other(), -3)]));
        assert!(listed_shorts(&accounts).is_empty(), "a partial liquidation's remainder");
        accounts.apply(block(1_004, vec![("BTC", other(), 2)]));
        accounts.apply(FlowAccountsEvent::Candidates { generation: 1, changes: changes(base + 2, vec![(PUMP, other(), candidate(-3, 0.012_49))], Vec::new()) });
        accounts.apply(confirmed(1_003));
        assert!(listed_shorts(&accounts).is_empty(), "its BTC changed at 1,004, after the refresh");
        accounts.apply(confirmed(1_004));
        assert_eq!(listed_shorts(&accounts), [(other(), -3)], "the refresh's new size");
        accounts.apply(block(1_005, vec![(PUMP, other(), 0)]));
        assert!(listed_shorts(&accounts).is_empty(), "liquidated whole, before the refresh");
        accounts.apply(FlowAccountsEvent::Candidates { generation: 1, changes: changes(base + 3, Vec::new(), vec![(PUMP, other())]) });
        accounts.apply(confirmed(1_005));
        assert!(listed_shorts(&accounts).is_empty(), "gone from the candidates");
    }

    /// Review 000569: an isolated candidate's price is its own coin's alone - another coin of the user changing withholds
    /// only its cross candidates -, its own coin changing withholds it.
    #[test]
    fn an_isolated_candidate_waits_for_its_own_coin_alone() {
        let (book, marks, candidates) = view();
        let (mut accounts, _validity) = serving(&book, &marks, &candidates);
        let base = candidates.snapshot().unwrap().version;
        let (isolated_user, cross_user) = (other(), Address::repeat_byte(0x43));
        let isolated = Candidate { isolated: true, ..candidate(-5, 0.012_49) };
        let enter = vec![(PUMP, isolated_user, isolated), (PUMP, cross_user, candidate(-7, 0.012_49))];
        accounts.apply(FlowAccountsEvent::Candidates { generation: 1, changes: changes(base + 1, enter, Vec::new()) });
        assert_eq!(listed_shorts(&accounts), [(isolated_user, -5), (cross_user, -7)], "in user order");
        accounts.apply(block(1_001, vec![("BTC", isolated_user, 2), ("BTC", cross_user, 1)]));
        assert_eq!(listed_shorts(&accounts), [(isolated_user, -5)]);
        accounts.apply(block(1_002, vec![(PUMP, isolated_user, -4)]));
        assert_eq!(listed_shorts(&accounts), Vec::new(), "its own coin moved");
    }

    /// Review 000569: a snapshot whose positions are a block ahead of its candidates (sent after an input, before its
    /// refresh) cannot say which changed in between: no liquidation is listed until a refresh confirms its height.
    #[test]
    fn a_snapshot_ahead_of_its_candidates_lists_no_liquidation_until_confirmed() {
        let (book, marks, candidates) = view();
        let (validity, watched) = watch::channel(FlowAccountsValidity { generation: 1, state: Ok(()) });
        let mut accounts = FlowAccounts {
            owner: CopyOwner::ForcedFlow,
            validity: watched,
            generation: 0,
            copy: None,
            broken: None,
            resync: Arc::new(AtomicBool::new(false)),
        };
        let mut snapshot = AccountsSnapshot::of(1, &book, &marks, &candidates).unwrap();
        (snapshot.height, snapshot.time_ms) = (1_005, 1_005_000);
        accounts.apply(FlowAccountsEvent::Snapshot(Box::new(snapshot)));
        let listed = |accounts: &FlowAccounts| accounts.serving().unwrap().liquidation_inputs(PUMP, 12_500, None, 1_000).unwrap().len();
        assert_eq!(listed(&accounts), 0, "HOLDER's within 1000 bp, withheld");
        accounts.apply(confirmed(1_004));
        assert_eq!(listed(&accounts), 0);
        accounts.apply(confirmed(1_005));
        assert_eq!(listed(&accounts), 1);
        drop(validity);
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

    /// The listed marks with the trailing stops their node marks hold, and their revision.
    struct TrailingMarks {
        marks: ListedMarks,
        trailing: TrailingCopy,
        revision: u64,
    }

    impl MarksAt for TrailingMarks {
        fn height(&self) -> u64 {
            self.marks.height
        }

        fn mark(&self, coin: &str) -> Option<u64> {
            self.marks.mark(coin)
        }

        fn trailing(&self) -> Option<TrailingCopy> {
            Some(self.trailing.clone())
        }

        fn revision(&self) -> u64 {
            self.revision
        }
    }

    /// Batch 2n-2: the pending book's copy takes the trailing stops whole with its snapshot, then each change at the
    /// copy's height - the marks rebuilt at it -, or with its block (review 000662 r1 P2-1: never a height with another
    /// height's stops); the forced flow's takes none; a change of another height breaks the copy off.
    #[test]
    fn the_pending_books_copy_keeps_the_trailing_stops() {
        use crate::{accounts::marks::trailing::{Retracement, TrailingState, TrailingStop}, order_book::Side};
        let (mut book, marks, candidates) = view();
        let stop = |best| (PUMP.to_string(), Ok(TrailingStop { side: Side::Ask, activation: None, retracement: Retracement::Price(5), state: TrailingState::Active { best } }));
        let mut held = TrailingMarks { marks, trailing: TrailingCopy { stops: FxHashMap::from_iter([(7, stop(100))]), refresh_due_ms: Some(5_000), claimed: std::iter::once(PUMP.to_string()).collect() }, revision: 1 };
        let (mut feed, mut accounts, mut events) = link(CopyOwner::PendingBook);
        feed.on_applied(&book, Some(&held), &Touched::default(), &candidates);
        let Ok(FlowAccountsEvent::Snapshot(snapshot)) = events.try_recv() else { panic!("a snapshot") };
        assert_eq!(snapshot.trailing.as_ref(), Some(&held.trailing));
        accounts.apply(FlowAccountsEvent::Snapshot(snapshot));
        // the marks rebuilt at the copy's height: read once they take a new revision, not at each input before
        held.trailing.stops.extend([(7, stop(110)), (8, stop(90))]);
        feed.on_applied(&book, Some(&held), &Touched::default(), &candidates);
        assert!(events.try_recv().is_err(), "the revision the copy read");
        held.revision = 2;
        feed.on_applied(&book, Some(&held), &Touched::default(), &candidates);
        let Ok(event @ FlowAccountsEvent::Trailing { height: 1_000, .. }) = events.try_recv() else { panic!("the change at 1000") };
        accounts.apply(event);
        assert_eq!(accounts.copy().unwrap().trailing().map(CopiedTrailing::sent), Some(held.trailing.clone()));
        // a block, one gone with it
        book.apply_block_actions(block_actions(1_001, &[]));
        held.marks.height = 1_001;
        held.revision = 3;
        held.trailing.stops.remove(&8);
        held.trailing.stops.insert(9, stop(80));
        feed.on_applied(&book, Some(&held), &Touched::default(), &candidates);
        let Ok(block @ FlowAccountsEvent::Block { trailing: Some(_), .. }) = events.try_recv() else { panic!("the block with its change") };
        assert!(events.try_recv().is_err(), "nothing apart from the block");
        accounts.apply(block);
        assert_eq!(accounts.copy().unwrap().trailing().map(CopiedTrailing::sent), Some(held.trailing.clone()));
        // each stop with the height it was last set at (review 000666 r1): 7 as the marks were rebuilt at 1000, 9 at 1001
        let set_at = |accounts: &FlowAccounts, oid| accounts.copy().unwrap().trailing().and_then(|trailing| trailing.stop(oid)).map(|stop| stop.set_at);
        assert_eq!((set_at(&accounts, 7), set_at(&accounts, 8), set_at(&accounts, 9)), (Some(1_000), None, Some(1_001)));
        feed.on_applied(&book, Some(&held), &Touched::default(), &candidates);
        assert!(events.try_recv().is_err(), "nothing changed");
        // a coin's claim withdrawn at the same height, its stops as they were (review 000662 r2 P2-2)
        held.trailing.claimed.clear();
        held.revision = 4;
        feed.on_applied(&book, Some(&held), &Touched::default(), &candidates);
        let Ok(event @ FlowAccountsEvent::Trailing { height: 1_001, .. }) = events.try_recv() else { panic!("the claim withdrawn") };
        accounts.apply(event);
        assert_eq!(accounts.copy().unwrap().trailing().map(CopiedTrailing::sent), Some(held.trailing.clone()));
        // the marks rebuilt ahead of the view (review 000662 r2 P2-1): the copy withdrawn until both stand at one height
        held.marks.height = 1_100;
        feed.on_applied(&book, Some(&held), &Touched::default(), &candidates);
        assert!(events.try_recv().is_err() && accounts.copy().is_err(), "nothing sent, the copy withdrawn");
        held.marks.height = 1_001;
        feed.on_applied(&book, Some(&held), &Touched::default(), &candidates);
        let Ok(FlowAccountsEvent::Snapshot(snapshot)) = events.try_recv() else { panic!("a snapshot once they stand together") };
        assert_eq!((snapshot.height, snapshot.trailing.as_ref()), (1_001, Some(&held.trailing)));
        let generation = snapshot.generation;
        accounts.apply(FlowAccountsEvent::Snapshot(snapshot));
        // two blocks published as one, their ends alike (review 000666 r2): every stop taken as set at the last
        book.apply_block_actions(block_actions(1_002, &[]));
        book.apply_block_actions(block_actions(1_003, &[]));
        held.marks.height = 1_003;
        feed.on_applied(&book, Some(&held), &Touched::default(), &candidates);
        let Ok(block @ FlowAccountsEvent::Block { height: 1_003, trailing: None, .. }) = events.try_recv() else { panic!("one block through 1003, no change") };
        accounts.apply(block);
        assert_eq!((set_at(&accounts, 7), set_at(&accounts, 9)), (Some(1_003), Some(1_003)));
        let changes = TrailingChanges { updates: Vec::new(), refresh_due_ms: None, claimed: None };
        accounts.apply(FlowAccountsEvent::Trailing { generation, height: 999, changes });
        assert!(accounts.copy().is_err(), "a change of another height breaks the copy off");
        let (mut forced, _, mut forced_events) = link(CopyOwner::ForcedFlow);
        forced.on_applied(&book, Some(&held), &Touched::default(), &candidates);
        let Ok(FlowAccountsEvent::Snapshot(snapshot)) = forced_events.try_recv() else { panic!("a snapshot") };
        assert_eq!(snapshot.trailing, None, "the forced flow's copy keeps none");
    }

    /// The feed: a snapshot first, declared valid once queued; then blocks of what moved; the queue full withdraws the
    /// copy at once and drops what follows - while it stays full the copy serves nothing -, and the first input with
    /// room sends a new generation whole.
    #[test]
    fn a_full_queue_withdraws_the_copy_until_a_new_snapshot() {
        let (mut book, mut marks, candidates) = view();
        let (mut feed, mut accounts, mut events) = link(CopyOwner::ForcedFlow);
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
        let (mut feed, mut accounts, mut events) = link(CopyOwner::ForcedFlow);
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
        let (mut feed, accounts, events) = link(CopyOwner::ForcedFlow);
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
