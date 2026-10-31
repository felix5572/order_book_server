//! What the pending book serves (hyperliquid-trade design 000604 section 4, r3 revision 9; batch 2c): each coin's best
//! levels as the pending actions will leave the book beside the replica's own (`basisBbo`), the predicted trades, and
//! whether the book may be served.
//!
//! After every input the pending thread's publisher computes the frames of the coins the step changed (every coin on a
//! new generation) and keeps each coin's latest in a store a subscription takes its first frame from; it broadcasts a
//! coin's frame only when its `bbo` or `basisBbo` moved, and only while the book may be served. The store takes a step
//! whole - frames, basis, availability, the sequence number reached - in one write, before the broadcast sends it: an
//! availability change first, then the frames, then the step's trades. Every event carries the publisher's sequence
//! number, so a subscription that read the store skips every event the store already held (review 000614 r1). An
//! availability change carries the book's state as of it - served, every coin's frame committed with it -, which a
//! subscription following the broadcast tells as it comes, never the store's later one (r2 P1-1).

use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

use rustc_hash::FxHashMap;
use tokio::sync::broadcast;

use super::{
    overlay::BestLevel,
    shadow::{PendingTrade, Shadow, Step, Unavailable},
};
use crate::{listeners::order_book::replica::BookReplica, metrics::PENDING_BOOK_FRAMES_TOTAL, order_book::Coin};

/// Events queued for a connection: ~40 steps a second.
const EVENTS: usize = 4_096;

/// A side's best level, (bid, ask).
pub(crate) type BestLevels = (Option<BestLevel>, Option<BestLevel>);

/// One coin's `extPendingBbo` frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BboFrame {
    pub(crate) seq: u64,
    pub(crate) coin: Coin,
    /// The merged book's best levels.
    pub(crate) bbo: BestLevels,
    /// The replica's: the confirmed book at `basis_height`.
    pub(crate) basis_bbo: BestLevels,
    pub(crate) basis: Basis,
    pub(crate) pending: CoinPending,
    /// The publisher's wall clock, unix ms.
    pub(crate) published_ms: u64,
}

/// The replica's confirmed height and its block time, unix ms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Basis {
    pub(crate) height: u64,
    pub(crate) time_ms: u64,
}

/// What the pending set holds of a coin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct CoinPending {
    pub(crate) actions: usize,
    /// Its lines' oldest and newest `local_time`, unix ms; `None` without an action.
    pub(crate) oldest_seen_ms: Option<u64>,
    pub(crate) newest_seen_ms: Option<u64>,
}

/// Every coin's latest frame at one basis: the store's, and a recovery's. Shared copy-on-write: a snapshot held
/// elsewhere costs the store one copy at its next commit changing a frame.
#[derive(Debug, Clone)]
pub(crate) struct FrameSet {
    basis: Basis,
    /// The publisher's clock when it was committed, unix ms.
    published_ms: u64,
    frames: Arc<FxHashMap<Coin, Arc<BboFrame>>>,
}

impl FrameSet {
    /// `coin`'s frame at this basis: a coin's frame is published again whenever its book or its actions change - and
    /// every coin's when the overlays were dropped or rebuilt whole -, so the one kept holds at every basis since. A
    /// coin without one has no level on either side and nothing pending (the publisher frames every coin of the
    /// replica's book and of the set): its frame is the empty one.
    pub(crate) fn frame(&self, coin: &Coin) -> Arc<BboFrame> {
        let (basis, published_ms) = (self.basis, self.published_ms);
        let frame = self.frames.get(coin).map_or_else(
            || BboFrame { seq: 0, coin: coin.clone(), bbo: (None, None), basis_bbo: (None, None), basis, pending: CoinPending::default(), published_ms },
            |frame| BboFrame { basis, ..BboFrame::clone(frame) },
        );
        Arc::new(frame)
    }
}

/// A step's predicted trades, the replica at `basis`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TradesFrame {
    pub(crate) seq: u64,
    pub(crate) basis: Basis,
    pub(crate) trades: Vec<PendingTrade>,
}

#[derive(Debug, Clone)]
pub(crate) enum PendingEvent {
    /// The book's state at the change: a subscription told it a recovery shows the book of it, never one of a failure
    /// after (review 000614 r2 P1-1).
    Availability(Arc<Snapshot>),
    /// The frames of the coins whose `bbo` or `basisBbo` moved, in sequence.
    Bbos(Arc<Vec<Arc<BboFrame>>>),
    Trades(Arc<TradesFrame>),
}

/// The latest of each, for a subscription's first frames: each step committed whole, under one write (review 000614
/// r1 P1-2).
struct Latest {
    /// The sequence number the last commit reached.
    seq: u64,
    availability: Result<(), Unavailable>,
    /// From the first commit with a replica aligned.
    frames: Option<FrameSet>,
}

/// The book's state through `seq` - the store's, a subscription's start, and an availability change's: served, every
/// coin's frame; or why not.
#[derive(Debug, Clone)]
pub(crate) struct Snapshot {
    pub(crate) seq: u64,
    pub(crate) served: Result<FrameSet, Unavailable>,
}

impl Snapshot {
    pub(crate) fn availability(&self) -> Result<(), Unavailable> {
        self.served.as_ref().map(|_| ()).map_err(|reason| *reason)
    }

    /// `coin`'s frame (`FrameSet::frame`) when served.
    pub(crate) fn frame(&self, coin: &Coin) -> Result<Arc<BboFrame>, Unavailable> {
        self.served.as_ref().map(|frames| frames.frame(coin)).map_err(|reason| *reason)
    }
}

/// The server's side of the pending book: its latest frames and its events.
#[derive(Clone)]
pub(crate) struct PendingBookHandle {
    latest: Arc<RwLock<Latest>>,
    events: broadcast::Sender<PendingEvent>,
}

impl PendingBookHandle {
    /// Nothing served before the first publish: no replica yet.
    pub(crate) fn new() -> Self {
        let latest = Latest { seq: 0, availability: Err(Unavailable::Replica), frames: None };
        Self { latest: Arc::new(RwLock::new(latest)), events: broadcast::channel(EVENTS).0 }
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<PendingEvent> {
        self.events.subscribe()
    }

    /// The store through its last commit.
    pub(crate) fn snapshot(&self) -> Snapshot {
        let latest = self.read();
        Snapshot { seq: latest.seq, served: served(latest.availability, latest.frames.as_ref()) }
    }

    #[expect(clippy::expect_used, reason = "a writer panicking ends the pending thread; nothing reads a half-written store")]
    fn read(&self) -> std::sync::RwLockReadGuard<'_, Latest> {
        self.latest.read().expect("the pending book's latest frames")
    }

    #[expect(clippy::expect_used, reason = "a writer panicking ends the pending thread; nothing reads a half-written store")]
    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Latest> {
        self.latest.write().expect("the pending book's latest frames")
    }

    /// The pending thread ended: the book is never served again, past every frame's sequence number.
    pub(crate) fn end(&self) {
        let ended = Snapshot { seq: u64::MAX, served: Err(Unavailable::Stopped) };
        {
            let mut latest = self.write();
            latest.seq = ended.seq;
            latest.availability = ended.availability();
        }
        self.send(PendingEvent::Availability(Arc::new(ended)));
    }

    /// An event to every connection listening; none listening is no loss.
    fn send(&self, event: PendingEvent) {
        let _receivers = self.events.send(event);
    }

    /// The store as a publisher would leave it.
    #[cfg(test)]
    pub(crate) fn store_for_test(&self, seq: u64, availability: Result<(), Unavailable>, frames: Vec<BboFrame>, basis: Basis) {
        let mut latest = self.write();
        latest.seq = seq;
        latest.availability = availability;
        latest.frames = Some(FrameSet::for_test(frames, basis));
    }

    /// `coin`'s frame in the store, served or not.
    #[cfg(test)]
    pub(crate) fn stored_frame(&self, coin: &Coin) -> Arc<BboFrame> {
        self.read().frames.as_ref().unwrap().frame(coin)
    }

    #[cfg(test)]
    pub(crate) fn send_for_test(&self, event: PendingEvent) {
        self.send(event);
    }
}

/// The pending thread's publisher.
pub(crate) struct Publisher {
    handle: PendingBookHandle,
    seq: u64,
    availability: Result<(), Unavailable>,
    /// Each coin's (`bbo`, `basisBbo`) as last framed.
    framed: FxHashMap<Coin, (BestLevels, BestLevels)>,
}

impl Publisher {
    pub(crate) fn new(handle: PendingBookHandle) -> Self {
        Self { handle, seq: 0, availability: Err(Unavailable::Replica), framed: FxHashMap::default() }
    }

    /// What `step` changed, published: the frames of its coins (every coin with `refresh_all`), built first, then
    /// committed to the store with the availability in one write; then broadcast - the availability when it changed,
    /// the frames whose best levels moved while the book may be served, the trades.
    pub(crate) fn publish(&mut self, step: &Step, refresh_all: bool, shadow: &Shadow, replica: Option<&BookReplica>) {
        let aligned = replica.and_then(|replica| Some((replica, basis_of(replica)?)));
        let published_ms = now_ms();
        let (framed, moved) =
            aligned.map_or_else(Default::default, |(replica, basis)| self.frames(step, refresh_all, shadow, replica, basis, published_ms));
        let availability = shadow.availability(replica);
        let changed = (availability != self.availability).then(|| self.next_seq());
        let trades = aligned
            .filter(|_| !step.trades.is_empty())
            .map(|(_, basis)| Arc::new(TradesFrame { seq: self.next_seq(), basis, trades: step.trades.clone() }));
        self.availability = availability;
        let committed = self.commit(aligned.map(|(_, basis)| (basis, published_ms)), framed, availability);
        if let Some(seq) = changed {
            PENDING_BOOK_FRAMES_TOTAL.with_label_values(&["availability"]).inc();
            let state = Snapshot { seq, served: served(availability, committed.as_ref()) };
            self.handle.send(PendingEvent::Availability(Arc::new(state)));
        }
        if availability.is_ok() && !moved.is_empty() {
            PENDING_BOOK_FRAMES_TOTAL.with_label_values(&["bbo"]).inc_by(moved.len() as u64);
            self.handle.send(PendingEvent::Bbos(Arc::new(moved)));
        }
        if let Some(trades) = trades {
            PENDING_BOOK_FRAMES_TOTAL.with_label_values(&["trades"]).inc();
            self.handle.send(PendingEvent::Trades(trades));
        }
    }

    /// The frames of the coins `step` changed - every coin with `refresh_all` -, and of them those whose best levels
    /// moved.
    fn frames(
        &mut self,
        step: &Step,
        refresh_all: bool,
        shadow: &Shadow,
        replica: &BookReplica,
        basis: Basis,
        published_ms: u64,
    ) -> (Vec<Arc<BboFrame>>, Vec<Arc<BboFrame>>) {
        let coins: HashSet<Coin> = if refresh_all {
            replica.coins().chain(shadow.coins()).chain(self.framed.keys()).cloned().collect()
        } else {
            step.changed.clone()
        };
        let mut framed = Vec::with_capacity(coins.len());
        let mut moved = Vec::new();
        for coin in coins {
            let view = shadow.coin_view(&coin, replica);
            let frame = Arc::new(BboFrame {
                seq: self.next_seq(),
                coin: coin.clone(),
                bbo: view.bbo,
                basis_bbo: view.basis_bbo,
                basis,
                pending: view.pending,
                published_ms,
            });
            if self.framed.insert(coin, (view.bbo, view.basis_bbo)) != Some((view.bbo, view.basis_bbo)) {
                moved.push(Arc::clone(&frame));
            }
            framed.push(frame);
        }
        (framed, moved)
    }

    /// The step into the store in one write, through the last sequence number it took: a reader sees the step whole
    /// or not at all. The frames as committed.
    fn commit(&self, basis: Option<(Basis, u64)>, framed: Vec<Arc<BboFrame>>, availability: Result<(), Unavailable>) -> Option<FrameSet> {
        let mut latest = self.handle.write();
        if let Some((basis, published_ms)) = basis {
            let set = latest.frames.get_or_insert_with(|| FrameSet { basis, published_ms, frames: Arc::default() });
            set.basis = basis;
            set.published_ms = published_ms;
            // a snapshot held elsewhere costs a copy only when a frame changes (review 000614 r3)
            if !framed.is_empty() {
                let kept = Arc::make_mut(&mut set.frames);
                for frame in framed {
                    kept.insert(frame.coin.clone(), frame);
                }
            }
        }
        latest.availability = availability;
        latest.seq = self.seq;
        latest.frames.clone()
    }

    #[cfg(test)]
    pub(crate) const fn handle(&self) -> &PendingBookHandle {
        &self.handle
    }

    const fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }
}

#[cfg(test)]
impl FrameSet {
    pub(crate) fn for_test(frames: Vec<BboFrame>, basis: Basis) -> Self {
        let frames = frames.into_iter().map(|frame| (frame.coin.clone(), Arc::new(frame))).collect();
        Self { basis, published_ms: 0, frames: Arc::new(frames) }
    }
}

/// Served, the frames committed with it: the book is served only with the replica aligned, and every aligned step
/// commits its basis.
#[expect(clippy::expect_used, reason = "served only aligned; an aligned step commits the frames")]
fn served(availability: Result<(), Unavailable>, frames: Option<&FrameSet>) -> Result<FrameSet, Unavailable> {
    availability.map(|()| frames.cloned().expect("a served book's frames"))
}

/// The replica's basis, when aligned.
fn basis_of(replica: &BookReplica) -> Option<Basis> {
    Some(Basis { height: replica.confirmed()?, time_ms: replica.confirmed_time_ms()? })
}

#[expect(clippy::expect_used, reason = "a host clock before 1970 or past u64 ms is broken")]
fn now_ms() -> u64 {
    let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH).expect("the host clock is after 1970");
    u64::try_from(since_epoch.as_millis()).expect("unix ms fit u64")
}
