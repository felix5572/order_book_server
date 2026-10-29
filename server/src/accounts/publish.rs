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
use tokio::sync::{broadcast, watch};

use super::{
    book::{AccountBook, QueryError, Touched, Unavailable},
    candidates::{CandidateChanges, Candidates, Refresh},
    flow_accounts::FlowAccountsFeed,
    forced_flow_owner::{FlowFrame, FlowOutput},
    marks::{MarksAt, NativePriceSnapshot, NodeMarks},
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
    /// One change of the liquidation candidates (design 000443): each candidate subscription projects it; the forced
    /// flow's copy of the view takes the same `Arc` (review 000563 r2).
    Candidates(Arc<CandidateChanges>),
    /// The liquidation candidates stopped being available: every candidate subscription ends.
    CandidatesWithdrawn { reason: String },
    /// A coin's new forced-flow content of one tier (design 000451): each subscription admitting it sends its view.
    ForcedFlow(Arc<FlowFrame>),
    /// The forced flow ended until restart: every forced-flow subscription ends.
    ForcedFlowWithdrawn { reason: String },
}

/// The node price state the local marks read (design 000454 r2 revision 1): the trusted state after the latest applied
/// block, or why there is none (the account view or the node marks are unavailable).
pub(crate) type NodePrices = Result<Arc<NativePriceSnapshot>, String>;

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
    prices: watch::Sender<NodePrices>,
    /// Whether the view was available after the previous step.
    available: bool,
    /// Whether the node marks were.
    marks_available: bool,
    /// The forced flow's copy of the view (review 000563): `None` without a forced flow thread.
    flow: Option<FlowAccountsFeed>,
}

impl Publisher {
    pub(crate) fn new(subscriptions: CoinSubscriptions, frames: broadcast::Sender<Arc<AccountsFrame>>) -> Self {
        let (prices, _) = watch::channel(Err(marks_unavailable(&Unavailable::Initializing)));
        Self { subscriptions, frames, prices, available: false, marks_available: false, flow: None }
    }

    /// The forced flow thread's copy of the view, fed from now on.
    pub(crate) fn attach_flow(&mut self, feed: FlowAccountsFeed) {
        self.flow = Some(feed);
    }

    /// After an input or an anchor, before the candidates' refresh: the forced flow's copy brought to the view as it
    /// stands (`FlowAccountsFeed::on_applied`).
    pub(crate) fn flow_applied(&mut self, book: &AccountBook, marks: Option<&dyn MarksAt>, touched: &Touched, candidates: &Candidates) {
        self.with_flow(|feed| feed.on_applied(book, marks, touched, candidates));
    }

    /// After a refresh: the height the candidates are confirmed at, for the forced flow's copy.
    pub(crate) fn flow_candidates_confirmed(&mut self, candidates: &Candidates) {
        self.with_flow(|feed| feed.on_candidates_confirmed(candidates));
    }

    /// A step of the feed; the forced flow thread found gone withdraws the forced flow, once.
    fn with_flow(&mut self, step: impl FnOnce(&mut FlowAccountsFeed)) {
        let Some(feed) = &mut self.flow else { return };
        step(feed);
        if let Some(reason) = feed.take_ended() {
            send_frame(&self.frames, AccountsFrame::ForcedFlowWithdrawn { reason }, "forcedFlowWithdrawn");
        }
    }

    /// The node prices from now on; closed once the owner (holding this publisher) is gone.
    pub(crate) fn subscribe_prices(&self) -> watch::Receiver<NodePrices> {
        self.prices.subscribe()
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
    /// step that may change them, once the view is tracked; the marks' ordinary moves push nothing. The node prices
    /// follow every step (`publish_prices`).
    pub(crate) fn track_marks(&mut self, book: &mut AccountBook, marks: &NodeMarks) {
        self.publish_prices(book, marks);
        let available = marks.at().is_some();
        let changed = available != self.marks_available;
        self.marks_available = available;
        if changed && self.available {
            self.push_snapshots(book, marks.at());
        }
    }

    /// The node prices of the state as it stands: a new snapshot once a block applies or a seed rebuilds the state, the
    /// reason the moment the view or the marks become unavailable (design 000454 r2 revision 1 and acceptance point 2:
    /// every applied block, whoever subscribes); nothing when neither moved.
    fn publish_prices(&self, book: &AccountBook, marks: &NodeMarks) {
        let status = marks.status();
        let next = book
            .availability()
            .map_err(|reason| format!("the account view is unavailable: {reason}"))
            .and_then(|_| status.availability.map_err(|reason| marks_unavailable(&reason)));
        let unchanged = match (&*self.prices.borrow(), &next) {
            (Ok(published), Ok(height)) => (Some(published.seed_height), published.height) == (status.seed_height, *height),
            (Err(published), Err(reason)) => published == reason,
            _ => false,
        };
        if unchanged {
            return;
        }
        let prices = next.and_then(|_| marks.price_snapshot().map(Arc::new).map_err(|reason| marks_unavailable(&reason)));
        drop(self.prices.send_replace(prices));
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
    /// The candidates' one exit (review 000563 r2 revision 3: refresh, book walk and stop alike): the broadcast and the
    /// forced flow's copy.
    pub(crate) fn publish_candidates(&mut self, refresh: Refresh) {
        match refresh {
            Refresh::Unchanged => {}
            Refresh::Changes(changes) => {
                let changes = Arc::new(changes);
                self.with_flow(|feed| feed.on_candidates(&changes));
                self.send(AccountsFrame::Candidates(changes), "candidates");
            }
            Refresh::Withdrawn(reason) => {
                self.with_flow(|feed| feed.on_candidates_withdrawn(&reason));
                self.send(AccountsFrame::CandidatesWithdrawn { reason }, "candidatesWithdrawn");
            }
        }
    }

    fn send(&self, frame: AccountsFrame, kind: &str) {
        send_frame(&self.frames, frame, kind);
    }
}

/// The forced flow thread's side of the push (review 000563): its frames on the same broadcast.
pub(crate) struct FlowPublisher {
    frames: broadcast::Sender<Arc<AccountsFrame>>,
}

impl FlowPublisher {
    pub(crate) const fn new(frames: broadcast::Sender<Arc<AccountsFrame>>) -> Self {
        Self { frames }
    }

    /// The forced-flow frames of one step.
    pub(crate) fn publish(&self, output: FlowOutput) {
        for frame in output.frames {
            send_frame(&self.frames, AccountsFrame::ForcedFlow(frame), "forcedFlow");
        }
    }

    /// The forced flow ended until restart.
    pub(crate) fn withdraw(&self, reason: String) {
        send_frame(&self.frames, AccountsFrame::ForcedFlowWithdrawn { reason }, "forcedFlowWithdrawn");
    }
}

fn send_frame(frames: &broadcast::Sender<Arc<AccountsFrame>>, frame: AccountsFrame, kind: &str) {
    ACCOUNTS_FRAMES_TOTAL.with_label_values(&[kind]).inc();
    // an error only means no connection is listening right now
    drop(frames.send(Arc::new(frame)));
}

fn marks_unavailable(reason: &Unavailable) -> String {
    format!("the node marks are unavailable: {reason}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::{
        book::Stream,
        marks::{CheckpointGrid, MarkInputs, tests::trusted_marks},
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
                AccountsFrame::ForcedFlow(_) | AccountsFrame::ForcedFlowWithdrawn { .. } => "forcedFlow".to_string(),
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
                AccountsFrame::ForcedFlow(_) | AccountsFrame::ForcedFlowWithdrawn { .. } => "forcedFlow".to_string(),
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

    /// Design 000454 r2 revision 1 and acceptance point 2: the node prices follow every applied block and every rebuild,
    /// whoever subscribes to the coins; the reason is published the moment the marks or the view stop being available,
    /// and a step that moved neither publishes nothing.
    #[test]
    fn the_node_prices_follow_every_block_and_withdraw_at_once() {
        let mut book = trusted_book(&[(user(1), 5)]);
        let (frames, _received) = broadcast::channel(16);
        let mut publisher = Publisher::new(CoinSubscriptions::default(), frames);
        let mut prices = publisher.subscribe_prices();
        assert!(publisher.track(&book));
        let mut marks = trusted_marks();
        let mut step = |publisher: &mut Publisher, book: &mut AccountBook, marks: &NodeMarks| {
            publisher.track_marks(book, marks);
            prices.has_changed().unwrap().then(|| prices.borrow_and_update().clone())
        };
        let first = step(&mut publisher, &mut book, &marks).unwrap().unwrap();
        assert_eq!((first.seed_height, first.height), (995, 1_001));
        assert!(first.coins.contains_key("BTC"), "{:?}", first.coins.keys().collect::<Vec<_>>());
        assert!(step(&mut publisher, &mut book, &marks).is_none(), "nothing moved");
        marks.install(marks_seed(1_000));
        let rebuilt = step(&mut publisher, &mut book, &marks).unwrap().unwrap();
        assert_eq!((rebuilt.seed_height, rebuilt.height), (1_000, 1_001), "a rebuild at the same height");
        marks.apply_block(1_002, MarkInputs::default());
        assert_eq!(step(&mut publisher, &mut book, &marks).unwrap().unwrap().height, 1_002);
        marks.record_lost_block(Stream::BookDiffs, 1_002);
        let lost = step(&mut publisher, &mut book, &marks).unwrap().unwrap_err();
        assert!(lost.starts_with("the node marks are unavailable: "), "{lost}");
        assert!(step(&mut publisher, &mut book, &marks).is_none(), "the same reason once");
        marks.install(marks_seed(1_100));
        assert_eq!(step(&mut publisher, &mut book, &marks).unwrap().unwrap().seed_height, 1_100, "a rebuild");
        book.record_lost_block(Stream::Fills, 2_000);
        assert!(!publisher.track(&book));
        let view_lost = step(&mut publisher, &mut book, &marks).unwrap().unwrap_err();
        assert!(view_lost.starts_with("the account view is unavailable: "), "{view_lost}");
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
