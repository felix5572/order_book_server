//! Live push of `extCoinPositions` (design 000419 r2 4.2): the per-coin subscriber registry the WS connections hold
//! guards in, and the frames the owner broadcasts after each step. Frames are built only for coins with a subscriber,
//! serialized once and shared by every connection.
//!
//! Ordering: a connection registers its guard before asking the owner for the snapshot, so every frame the owner
//! builds after answering carries a newer version; the connection forwards only versions above the one it holds.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use alloy::primitives::Address;
use log::{error, warn};
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::broadcast;

use super::{
    book::{AccountBook, QueryError, Touched},
    candidates::{CandidateChanges, Refresh},
    marks::{MarksAt, NodeMarks},
    view::CoinPositionsFrame,
};
use crate::metrics::ACCOUNTS_FRAMES_TOTAL;

/// Same depth as the book's broadcast: seconds of headroom at peak fill rates; a connection that still falls behind is
/// withdrawn (its subscriptions end), never fed a gapped stream.
pub(crate) const FRAME_CHANNEL_CAPACITY: usize = 16_384;

#[derive(Debug)]
pub(crate) enum AccountsFrame {
    /// A frame of one coin: the snapshot after an anchor install, or a changes batch.
    Coin { coin: String, version: u64, wire: bytes::Bytes },
    /// The view became unavailable: every subscription ends.
    WithdrawAll { reason: String },
    /// The coin left the anchor universe: its subscriptions end.
    WithdrawCoin { coin: String, reason: String },
    /// One refresh of the liquidation candidates (design 000443): each candidate subscription projects it.
    Candidates(CandidateChanges),
    /// The liquidation candidates stopped being available: every candidate subscription ends.
    CandidatesWithdrawn { reason: String },
}

/// Live subscriber counts per coin.
#[derive(Clone, Default)]
pub(crate) struct CoinSubscriptions {
    counts: Arc<Mutex<FxHashMap<String, usize>>>,
}

impl CoinSubscriptions {
    pub(crate) fn acquire(&self, coin: &str) -> CoinSubscriptionGuard {
        *lock(&self.counts).entry(coin.to_string()).or_default() += 1;
        CoinSubscriptionGuard { counts: Arc::clone(&self.counts), coin: coin.to_string() }
    }

    fn subscribed(&self) -> Vec<String> {
        lock(&self.counts).keys().cloned().collect()
    }
}

/// The counts are plain integers updated in one statement: a panic elsewhere cannot leave them half-written.
fn lock(counts: &Mutex<FxHashMap<String, usize>>) -> MutexGuard<'_, FxHashMap<String, usize>> {
    counts.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One subscription's share of a coin's count; dropping it (unsubscribe, withdrawal, disconnect) releases it.
pub(crate) struct CoinSubscriptionGuard {
    counts: Arc<Mutex<FxHashMap<String, usize>>>,
    coin: String,
}

impl Drop for CoinSubscriptionGuard {
    fn drop(&mut self) {
        let mut counts = lock(&self.counts);
        if let Some(count) = counts.get_mut(&self.coin) {
            *count -= 1;
            if *count == 0 {
                counts.remove(&self.coin);
            }
        }
    }
}

/// The owner's side of the push: what to broadcast after each step.
pub(crate) struct Publisher {
    subscriptions: CoinSubscriptions,
    frames: broadcast::Sender<Arc<AccountsFrame>>,
    /// Whether the view was available after the previous step.
    available: bool,
    /// Whether the node marks were.
    marks_available: bool,
}

impl Publisher {
    pub(crate) const fn new(subscriptions: CoinSubscriptions, frames: broadcast::Sender<Arc<AccountsFrame>>) -> Self {
        Self { subscriptions, frames, available: false, marks_available: false }
    }

    /// Withdraws every subscription when the view stops being available; returns whether it is available now.
    pub(crate) fn track(&mut self, book: &AccountBook) -> bool {
        let availability = book.availability();
        if self.available
            && let Err(reason) = &availability
        {
            self.send(AccountsFrame::WithdrawAll { reason: reason.to_string() }, "withdrawn");
        }
        self.available = availability.is_ok();
        self.available
    }

    /// A snapshot of every subscribed coin when the node marks become unavailable or available again while the view is
    /// (design 000442 r2 revision 1): rows that read them fall back to the anchor's, or replay again. Called after every
    /// step that may change them, once the view is tracked; the marks' ordinary moves push nothing.
    pub(crate) fn track_marks(&mut self, book: &mut AccountBook, marks: &NodeMarks) {
        let available = marks.at().is_some();
        let changed = available != self.marks_available;
        self.marks_available = available;
        if changed && self.available {
            self.push_snapshots(book, marks.at());
        }
    }

    /// After an input batch: a changes frame for every subscribed coin with a changed row.
    pub(crate) fn push_changes(&self, book: &mut AccountBook, touched: &Touched, marks: Option<&dyn MarksAt>) {
        if touched.accounts.is_empty() && touched.positions.is_empty() {
            return;
        }
        let coins = self.subscriptions.subscribed();
        if coins.is_empty() {
            return;
        }
        let accounts: FxHashSet<Address> = touched.accounts.iter().copied().collect();
        for coin in coins {
            let positions: Vec<Address> =
                touched.positions.iter().filter(|(name, _)| *name == coin).map(|(_, user)| *user).collect();
            match book.push_changes(&coin, &accounts, &positions, marks) {
                Ok(Some(changes)) => {
                    let wire = CoinPositionsFrame::changes(&changes).to_wire();
                    self.send(AccountsFrame::Coin { coin, version: changes.version, wire }, "changes");
                }
                // no touched holder; or a coin whose subscriber is still waiting for a rejected snapshot
                Ok(None) | Err(QueryError::UnknownCoin) => {}
                Err(QueryError::Unavailable(reason)) => error!("accounts: changes of {coin} with an unavailable view: {reason}"),
            }
        }
    }

    /// After an anchor install: the new snapshot of every subscribed coin.
    pub(crate) fn push_snapshots(&self, book: &mut AccountBook, marks: Option<&dyn MarksAt>) {
        for coin in self.subscriptions.subscribed() {
            match book.push_snapshot(&coin, marks) {
                Ok(view) => {
                    let wire = CoinPositionsFrame::snapshot(&view).to_wire();
                    self.send(AccountsFrame::Coin { coin, version: view.version, wire }, "snapshot");
                }
                Err(QueryError::UnknownCoin) => {
                    warn!("accounts: subscribed coin {coin} is not in the new anchor's universe; withdrawing it");
                    let reason = "the coin left the account anchor universe".to_string();
                    self.send(AccountsFrame::WithdrawCoin { coin, reason }, "withdrawn");
                }
                Err(QueryError::Unavailable(reason)) => error!("accounts: snapshot of {coin} with an unavailable view: {reason}"),
            }
        }
    }

    /// A candidates refresh as one frame.
    pub(crate) fn publish_candidates(&self, refresh: Refresh) {
        match refresh {
            Refresh::Unchanged => {}
            Refresh::Changes(changes) => self.send(AccountsFrame::Candidates(changes), "candidates"),
            Refresh::Withdrawn(reason) => self.send(AccountsFrame::CandidatesWithdrawn { reason }, "candidatesWithdrawn"),
        }
    }

    fn send(&self, frame: AccountsFrame, kind: &str) {
        ACCOUNTS_FRAMES_TOTAL.with_label_values(&[kind]).inc();
        // an error only means no connection is listening right now
        drop(self.frames.send(Arc::new(frame)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::{
        book::Stream,
        marks::{CheckpointGrid, tests::trusted_marks},
        test_support::{PUMP, SEED_TIME_MS, anchor, marks_seed, trusted_book, user},
    };

    fn wire_json(frame: &AccountsFrame) -> serde_json::Value {
        let AccountsFrame::Coin { wire, .. } = frame else { panic!("not a coin frame: {frame:?}") };
        serde_json::from_slice(wire).unwrap()
    }

    #[test]
    fn frames_only_for_subscribed_coins_and_one_withdrawal_when_the_view_goes() {
        let mut book = trusted_book(&[(user(1), 5)]);
        let subscriptions = CoinSubscriptions::default();
        let (frames, mut received) = broadcast::channel(16);
        let mut publisher = Publisher::new(subscriptions.clone(), frames);
        assert!(publisher.track(&book));
        let touched = Touched { accounts: vec![user(1)], positions: Vec::new() };
        publisher.push_changes(&mut book, &touched, None);
        assert!(received.try_recv().is_err(), "nobody subscribed");
        let _guard = subscriptions.acquire(PUMP);
        publisher.push_changes(&mut book, &touched, None);
        let frame = received.try_recv().unwrap();
        assert!(matches!(frame.as_ref(), AccountsFrame::Coin { coin, version: 1, .. } if coin == PUMP));
        let json = wire_json(&frame);
        assert_eq!((json["channel"].as_str(), json["data"]["kind"].as_str()), (Some("extCoinPositions"), Some("changes")));
        book.record_lost_block(Stream::Fills, 2_000);
        assert!(!publisher.track(&book));
        assert!(matches!(received.try_recv().unwrap().as_ref(), AccountsFrame::WithdrawAll { reason } if reason.contains("2000")));
        assert!(!publisher.track(&book));
        assert!(received.try_recv().is_err(), "withdrawn once per loss of availability");
    }

    #[test]
    fn an_anchor_pushes_snapshots_and_withdraws_coins_that_left_the_universe() {
        let mut book = trusted_book(&[(user(1), 5)]);
        let subscriptions = CoinSubscriptions::default();
        let (frames, mut received) = broadcast::channel(16);
        let publisher = Publisher::new(subscriptions.clone(), frames);
        let _pump = subscriptions.acquire(PUMP);
        let _gone = subscriptions.acquire("xyz:GONE");
        book.install_anchor(anchor(1_100, &[(PUMP, 0, &[(user(1), 6)])]));
        publisher.push_snapshots(&mut book, None);
        let mut kinds = Vec::new();
        while let Ok(frame) = received.try_recv() {
            kinds.push(match frame.as_ref() {
                AccountsFrame::Coin { coin, version, .. } => {
                    let json = wire_json(&frame);
                    format!("{coin} v{version} {}", json["data"]["kind"].as_str().unwrap())
                }
                AccountsFrame::WithdrawCoin { coin, .. } => format!("{coin} withdrawn"),
                AccountsFrame::WithdrawAll { .. } => "all withdrawn".to_string(),
                AccountsFrame::Candidates(_) | AccountsFrame::CandidatesWithdrawn { .. } => "candidates".to_string(),
            });
        }
        kinds.sort_unstable();
        assert_eq!(kinds, ["PUMP v1 snapshot", "xyz:GONE withdrawn"]);
    }

    /// Design 000442 r2 revision 1: the marks turning unavailable or available again push one snapshot each while the
    /// view is available; staying so pushes nothing, and a withdrawn view gets nothing.
    #[test]
    fn the_marks_trust_changing_pushes_one_snapshot_each_way() {
        let mut book = trusted_book(&[(user(1), 5)]);
        let subscriptions = CoinSubscriptions::default();
        let (frames, mut received) = broadcast::channel(16);
        let mut publisher = Publisher::new(subscriptions.clone(), frames);
        let _guard = subscriptions.acquire(PUMP);
        assert!(publisher.track(&book));
        let mut marks = NodeMarks::new(CheckpointGrid { anchor: 1_000, interval: 100 }, SEED_TIME_MS - 60_000);
        let mut kinds = || {
            let mut kinds = Vec::new();
            while let Ok(frame) = received.try_recv() {
                kinds.push(match frame.as_ref() {
                    AccountsFrame::Coin { version, .. } => format!("v{version} {}", wire_json(&frame)["data"]["kind"].as_str().unwrap()),
                    AccountsFrame::WithdrawAll { .. } => "withdrawn".to_string(),
                    AccountsFrame::WithdrawCoin { .. } => "coin withdrawn".to_string(),
                    AccountsFrame::Candidates(_) | AccountsFrame::CandidatesWithdrawn { .. } => "candidates".to_string(),
                });
            }
            kinds
        };
        publisher.track_marks(&mut book, &marks);
        assert_eq!(kinds(), Vec::<String>::new(), "unavailable from the start");
        marks = trusted_marks();
        publisher.track_marks(&mut book, &marks);
        publisher.track_marks(&mut book, &marks);
        assert_eq!(kinds(), ["v1 snapshot"]);
        marks.record_lost_block(Stream::BookDiffs, 1_001);
        publisher.track_marks(&mut book, &marks);
        publisher.track_marks(&mut book, &marks);
        assert_eq!(kinds(), ["v2 snapshot"]);
        marks.install(marks_seed(1_100));
        publisher.track_marks(&mut book, &marks);
        assert_eq!(kinds(), ["v3 snapshot"], "recovered");
        book.record_lost_block(Stream::Fills, 2_000);
        assert!(!publisher.track(&book));
        marks.record_lost_block(Stream::BookDiffs, 1_105);
        publisher.track_marks(&mut book, &marks);
        assert_eq!(kinds(), ["withdrawn"], "the view's withdrawal only");
    }

    #[test]
    fn guards_count_subscribers_per_coin() {
        let subscriptions = CoinSubscriptions::default();
        let first = subscriptions.acquire("PUMP");
        let second = subscriptions.acquire("PUMP");
        let other = subscriptions.acquire("BTC");
        let mut coins = subscriptions.subscribed();
        coins.sort_unstable();
        assert_eq!(coins, ["BTC", "PUMP"]);
        drop(first);
        drop(other);
        assert_eq!(subscriptions.subscribed(), ["PUMP"]);
        drop(second);
        assert_eq!(subscriptions.subscribed().len(), 0);
    }
}
