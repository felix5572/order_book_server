//! The forced flow's owner side (design 000451 r4 revisions 9–11, r5 revisions 13–14). Lives in the account view's
//! owner task: which (coin, tier) is demanded - by a WS subscription's guard or an HTTP query -, each one's pipeline of
//! two reads (the band's trigger orders, then the entries' walks, both in the flow reader task), and its latest frame.
//!
//! A demanded (coin, tier) runs its pipeline once per tier period (100 ms for 10 and 50 bp, 1 s, 5 s); a local leg that turns withheld
//! or ended starts it at once. The frame is published when its content differs from the last published one (r5:
//! compared before `seq`, which numbers the published contents); the heights it carries are those of its inputs.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Instant,
};

use log::error;
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::{oneshot, watch};

use super::{
    book::AccountBook,
    candidates::Candidates,
    forced_flow::{CoinFlow, CoinInputs, Entry, Tier, TriggerInput, Unplaced, coin_flow, cumulative_sizes},
    forced_flow_reader::{FlowJob, FlowJobKind, FlowMailbox, FlowRead, SideJob, SideWalks, WalkItem},
    marks::MarksAt,
    walk::{Limit, WalkPart, walk_units},
};
use crate::{
    listeners::order_book::{BookTrust, TriggerBandAt, triggers::TriggerAvailability},
    local_marks::snapshot::LocalMarksSnapshot,
    order_book::{Px, Side},
};

/// The listener's prices are integers of 1e-8.
const PX_SCALE: f64 = 1e8;
/// A band is read this much wider than the distance; the entries' exact distances filter.
const RANGE_MARGIN: f64 = 1e-6;

/// `None`: every coin.
pub(crate) type DemandKey = (Option<String>, Tier);

/// Live demand per (coin or every coin, tier): the WS subscriptions hold guards.
#[derive(Clone, Default)]
pub(crate) struct FlowDemand {
    counts: Arc<Mutex<FxHashMap<DemandKey, usize>>>,
}

impl FlowDemand {
    pub(crate) fn acquire(&self, key: DemandKey) -> FlowDemandGuard {
        *lock(&self.counts).entry(key.clone()).or_default() += 1;
        FlowDemandGuard { counts: Arc::clone(&self.counts), key }
    }

    fn demanded(&self) -> Vec<DemandKey> {
        lock(&self.counts).keys().cloned().collect()
    }
}

/// The counts are plain integers updated in one statement: a panic elsewhere cannot leave them half-written.
fn lock(counts: &Mutex<FxHashMap<DemandKey, usize>>) -> MutexGuard<'_, FxHashMap<DemandKey, usize>> {
    counts.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One subscription's share of a key's demand; dropping it releases it.
pub(crate) struct FlowDemandGuard {
    counts: Arc<Mutex<FxHashMap<DemandKey, usize>>>,
    key: DemandKey,
}

impl Drop for FlowDemandGuard {
    fn drop(&mut self) {
        let mut counts = lock(&self.counts);
        if let Some(count) = counts.get_mut(&self.key) {
            *count -= 1;
            if *count == 0 {
                counts.remove(&self.key);
            }
        }
    }
}

/// The local calibre of a coin's frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LocalCalibre {
    /// The local predicted mark, 10^-(6 - szDecimals) USD.
    Ready { mark_raw: u64 },
    /// `code` as the local marks give it, or `localMarksOff` / `localMarksEnded` / `notInSources`.
    Withheld { code: &'static str, reason: String },
}

/// Whether the trigger part holds every standing trigger order of the band.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TriggerStatus {
    Complete,
    Incomplete(String),
}

/// Whether the entries' walks read a trusted book.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum WalkStatus {
    /// Per taker side walked, the book's mid at the copy (its height and time are in the frame's heights).
    Walked(Vec<(Side, Option<f64>)>),
    /// The book was withheld, or became so before the walks came back.
    Untrusted,
    /// The coin has no book.
    NoBook,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FlowEntry {
    pub(crate) entry: Entry,
    /// `None` unless the walks are `Walked` (and for a side without a book).
    pub(crate) walk: Option<WalkPart>,
}

/// What a frame says of a coin; compared to decide whether a frame goes out.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FlowContent {
    /// The flow of the coin is not available: the account view, the node marks or the book, with the reason.
    Unavailable(String),
    Ready(Box<ReadyFlow>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReadyFlow {
    pub(crate) size_decimals: u32,
    pub(crate) node_mark_raw: u64,
    pub(crate) local: LocalCalibre,
    pub(crate) triggers: TriggerStatus,
    pub(crate) walks: WalkStatus,
    /// Within the tier by either calibre, in the flow's order.
    pub(crate) entries: Vec<FlowEntry>,
    pub(crate) unplaced: Vec<(u64, Unplaced)>,
    /// Each band's cumulative size walked (`forced_flow::cumulative_sizes`), by taker side and size in 1e-8 units; empty
    /// unless the walks are `Walked` (and for a side without a book).
    pub(crate) cumulative: BTreeMap<(Side, u64), WalkPart>,
}

/// Where a frame's inputs stood; not compared.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct FlowHeights {
    pub(crate) accounts_height: u64,
    pub(crate) accounts_time_ms: u64,
    pub(crate) trigger_generation: u64,
    pub(crate) trigger_version: u64,
    pub(crate) listener_height: u64,
    pub(crate) listener_time: u64,
    /// `TriggerBandAt::status_height`: how far the trigger part is, where `listener_height` is the furthest stream.
    pub(crate) trigger_status_height: u64,
    pub(crate) local_published_ms: Option<u64>,
    /// (height, time) of the bids the sales walked, and of the asks the buys walked.
    pub(crate) sell_walk_book: Option<(u64, u64)>,
    pub(crate) buy_walk_book: Option<(u64, u64)>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FlowFrame {
    pub(crate) coin: String,
    pub(crate) tier: Tier,
    /// The published contents of (coin, tier), numbered from 1.
    pub(crate) seq: u64,
    pub(crate) heights: FlowHeights,
    pub(crate) content: FlowContent,
}

/// The order book the reads go to: its trust mirror and the reader's mailbox.
pub(crate) struct FlowLink {
    pub(crate) trust: BookTrust,
    pub(crate) mailbox: Arc<FlowMailbox>,
}

enum LocalLink {
    Off,
    Attached(watch::Receiver<Arc<LocalMarksSnapshot>>),
    Ended,
}

/// What a band read left for the walks to complete.
struct Walking {
    version: u64,
    heights: FlowHeights,
    ready: ReadyFlow,
    trust: (bool, u64),
    /// The cumulative sizes posted per side (1e-8 units), in the job's order.
    cumulative: Vec<(Side, Vec<u64>)>,
}

enum Stage {
    Idle,
    /// `local_ready`: whether the local leg was ready when the read was posted.
    Reading { version: u64, lo: Px, hi: Px, local_ready: bool },
    Walking(Box<Walking>),
}

impl Pipeline {
    /// Whether the local leg was ready for the content under way or last computed; `None` before any.
    fn local_ready(&self) -> Option<bool> {
        match &self.stage {
            Stage::Reading { local_ready, .. } => Some(*local_ready),
            Stage::Walking(walking) => Some(matches!(walking.ready.local, LocalCalibre::Ready { .. })),
            Stage::Idle => match &self.frame.as_ref()?.content {
                FlowContent::Ready(ready) => Some(matches!(ready.local, LocalCalibre::Ready { .. })),
                FlowContent::Unavailable(_) => None,
            },
        }
    }
}

struct Pipeline {
    started: Option<Instant>,
    stage: Stage,
    /// The latest frame, published or not: its `seq` is the last published content's.
    frame: Option<Arc<FlowFrame>>,
}

/// An HTTP query (or a new subscription) waiting for its coins' frames of one tier: each from a computation started no
/// earlier than the query (review 000460 r2 P2-2), so a later query never gets older facts.
struct Waiter {
    tier: Tier,
    since: Instant,
    remaining: FxHashSet<String>,
    frames: Vec<Arc<FlowFrame>>,
    reply: oneshot::Sender<Result<Vec<Arc<FlowFrame>>, String>>,
}

/// What a step reads of the account view.
pub(crate) struct OwnerView<'a> {
    pub(crate) book: &'a AccountBook,
    pub(crate) marks: Option<&'a dyn MarksAt>,
    pub(crate) candidates: &'a Candidates,
}

pub(crate) struct ForcedFlow {
    demand: FlowDemand,
    /// `None` without an order book: every frame is unavailable.
    link: Option<FlowLink>,
    local: LocalLink,
    pipelines: FxHashMap<(String, Tier), Pipeline>,
    version: u64,
    waiters: Vec<Waiter>,
    /// Permanent until restart: the reader task is gone.
    stopped: Option<String>,
}

/// What the owner broadcasts after a step.
#[derive(Debug, Default)]
pub(crate) struct FlowOutput {
    pub(crate) frames: Vec<Arc<FlowFrame>>,
}

impl ForcedFlow {
    pub(crate) fn new(demand: FlowDemand, link: Option<FlowLink>) -> Self {
        Self { demand, link, local: LocalLink::Off, pipelines: FxHashMap::default(), version: 0, waiters: Vec::new(), stopped: None }
    }

    /// The local marks' snapshots from now on (r5 revision 13).
    pub(crate) fn attach_local(&mut self, snapshots: watch::Receiver<Arc<LocalMarksSnapshot>>) {
        self.local = LocalLink::Attached(snapshots);
    }

    /// The local snapshots' receiver while attached (the owner waits on it).
    pub(crate) const fn local_snapshots(&mut self) -> Option<&mut watch::Receiver<Arc<LocalMarksSnapshot>>> {
        match &mut self.local {
            LocalLink::Attached(snapshots) => Some(snapshots),
            LocalLink::Off | LocalLink::Ended => None,
        }
    }

    /// The reader task is gone: every frame unavailable until restart, every waiting query answered.
    pub(crate) fn stop(&mut self, reason: String) {
        self.stopped.get_or_insert_with(|| reason.clone());
        self.pipelines.clear();
        for waiter in self.waiters.drain(..) {
            drop(waiter.reply.send(Err(reason.clone())));
        }
    }

    /// Every demanded (coin, tier) whose period has run starts its pipeline.
    pub(crate) fn tick(&mut self, now: Instant, view: &OwnerView<'_>) -> FlowOutput {
        let mut output = FlowOutput::default();
        for (coin, tier) in self.demanded(view) {
            let due = self.pipelines.get(&(coin.clone(), tier)).is_none_or(|pipeline| {
                matches!(pipeline.stage, Stage::Idle) && pipeline.started.is_none_or(|started| now.duration_since(started) >= tier.period())
            });
            if due {
                self.start(&coin, tier, now, view, &mut output);
            }
        }
        output
    }

    /// A new local snapshot, or its channel closed: every wanted (coin, tier) checked against the local leg now.
    pub(crate) fn on_local(&mut self, closed: bool, now: Instant, view: &OwnerView<'_>) -> FlowOutput {
        if closed {
            self.local = LocalLink::Ended;
        }
        let wanted = self.wanted(view);
        let mut output = FlowOutput::default();
        let mut keys: Vec<(String, Tier)> = self.pipelines.keys().filter(|key| wanted.contains(*key)).cloned().collect();
        keys.sort_unstable();
        for key in keys {
            self.revalidate_local(&key, now, view, &mut output);
        }
        self.answer_waiters();
        output
    }

    /// The key against the local leg now (review 000460 r2 P2-1): a published frame claiming a local mark that no
    /// longer holds is withdrawn at once, before any read; a content under way or last computed with the other validity
    /// starts again, whatever stage it is in (r1 P2-1: the newer read voids the older ones).
    fn revalidate_local(&mut self, key: &(String, Tier), now: Instant, view: &OwnerView<'_>, output: &mut FlowOutput) {
        let now_ready = self.local_ready(&key.0);
        let was_ready = self.pipelines.get(key).and_then(Pipeline::local_ready);
        if !now_ready {
            self.withdraw_local(key, output);
        }
        if was_ready.is_some_and(|was| was != now_ready) {
            self.start(&key.0, key.1, now, view, output);
        }
    }

    /// The last frame without its local calibre: the local leg as it is now, no entry's local distance, and the entries
    /// only it took in gone. Not an answer to a query (its other facts are not newer than the query).
    fn withdraw_local(&mut self, key: &(String, Tier), output: &mut FlowOutput) {
        let Some(frame) = self.pipelines.get(key).and_then(|pipeline| pipeline.frame.clone()) else { return };
        let FlowContent::Ready(ready) = &frame.content else { return };
        if !matches!(ready.local, LocalCalibre::Ready { .. }) {
            return;
        }
        let (local, local_published_ms) = self.local_calibre(&key.0);
        let mut ready = ready.clone();
        ready.local = local;
        for flow_entry in &mut ready.entries {
            flow_entry.entry.distance_local_bps = None;
        }
        ready.entries.retain(|flow_entry| flow_entry.entry.in_range(key.1.bps()));
        // the node calibre's sizes stay, walked on the same books; the local calibre's go with its distances
        let kept = cumulative_units(&ready.entries, key.1.bps(), ready.size_decimals);
        ready.cumulative.retain(|(side, units), _| kept.get(side).is_some_and(|sizes| sizes.contains(units)));
        let heights = FlowHeights { local_published_ms, ..frame.heights };
        self.publish(key, heights, FlowContent::Ready(ready), false, output);
    }

    /// The (coin, tier) pairs a subscription or a waiting query wants.
    fn wanted(&self, view: &OwnerView<'_>) -> FxHashSet<(String, Tier)> {
        let mut wanted: FxHashSet<(String, Tier)> = self.demanded(view).into_iter().collect();
        for waiter in &self.waiters {
            wanted.extend(waiter.remaining.iter().map(|coin| (coin.clone(), waiter.tier)));
        }
        wanted
    }

    fn local_ready(&self, coin: &str) -> bool {
        matches!(self.local_calibre(coin).0, LocalCalibre::Ready { .. })
    }

    /// An HTTP query (r5 revision 14), or a new subscription's first frames: every coin from a computation started no
    /// earlier (review 000460 r1 P2-2: a frame's age does not prove its inputs unchanged). `coin: None` is every coin with
    /// a node mark.
    pub(crate) fn query(
        &mut self,
        coin: Option<String>,
        tier: Tier,
        reply: oneshot::Sender<Result<Vec<Arc<FlowFrame>>, String>>,
        now: Instant,
        view: &OwnerView<'_>,
    ) -> FlowOutput {
        if let Some(reason) = &self.stopped {
            drop(reply.send(Err(reason.clone())));
            return FlowOutput::default();
        }
        let coins = coin.map_or_else(|| marked_coins(view), |coin| vec![coin]);
        // waiting first: a pipeline may finish as it starts (no book, no mark)
        self.waiters.push(Waiter { tier, since: now, remaining: coins.into_iter().collect(), frames: Vec::new(), reply });
        let mut output = FlowOutput::default();
        self.follow_ups(now, view, &mut output);
        self.answer_waiters();
        output
    }

    /// A computation for every idle key a query still waits for (review 000460 r2 P2-2): a computation under way is
    /// not voided by a new caller - it serves the callers that came before it started, and the next one the others.
    fn follow_ups(&mut self, now: Instant, view: &OwnerView<'_>, output: &mut FlowOutput) {
        let mut keys: Vec<(String, Tier)> = self
            .waiters
            .iter()
            .flat_map(|waiter| waiter.remaining.iter().map(|coin| (coin.clone(), waiter.tier)))
            .filter(|key| self.pipelines.get(key).is_none_or(|pipeline| matches!(pipeline.stage, Stage::Idle)))
            .collect();
        keys.sort_unstable();
        keys.dedup();
        for (coin, tier) in keys {
            self.start(&coin, tier, now, view, output);
        }
    }

    /// A read back from the reader task.
    pub(crate) fn on_read(&mut self, read: FlowRead, now: Instant, view: &OwnerView<'_>) -> FlowOutput {
        let mut output = FlowOutput::default();
        match read {
            FlowRead::Band { coin, tier, version, read } => self.on_band(&coin, tier, version, read, now, view, &mut output),
            FlowRead::Walk { coin, tier, version, sides } => self.on_walks(&coin, tier, version, sides, now, view, &mut output),
        }
        self.follow_ups(now, view, &mut output);
        self.answer_waiters();
        output
    }

    /// The demanded (coin, tier) pairs, `None` coins expanded to every coin with a node mark.
    fn demanded(&self, view: &OwnerView<'_>) -> Vec<(String, Tier)> {
        let mut pairs: Vec<(String, Tier)> = Vec::new();
        for (coin, tier) in self.demand.demanded() {
            match coin {
                Some(coin) => pairs.push((coin, tier)),
                None => pairs.extend(marked_coins(view).into_iter().map(|coin| (coin, tier))),
            }
        }
        pairs.sort_unstable();
        pairs.dedup();
        pairs
    }

    /// The coin's local calibre now, and the snapshot's publication time.
    fn local_calibre(&self, coin: &str) -> (LocalCalibre, Option<u64>) {
        let withheld = |code: &'static str, reason: &str| LocalCalibre::Withheld { code, reason: reason.to_string() };
        let ended = || withheld("localMarksEnded", "the local marks owner task ended");
        let snapshots = match &self.local {
            LocalLink::Off => return (withheld("localMarksOff", "the server runs without --local-marks"), None),
            LocalLink::Ended => return (ended(), None),
            // closed before the owner took the notice (review 000460 r2 P2-1): its last snapshot no longer holds
            LocalLink::Attached(snapshots) if snapshots.has_changed().is_err() => return (ended(), None),
            LocalLink::Attached(snapshots) => snapshots,
        };
        let snapshot = Arc::clone(&snapshots.borrow());
        let calibre = snapshot.coin(coin).map_or_else(
            || withheld("notInSources", "the coin is not in the local marks' sources"),
            |marks| match &marks.local {
                Ok(leg) => LocalCalibre::Ready { mark_raw: leg.mark.predicted },
                Err(withheld) => LocalCalibre::Withheld { code: withheld.code, reason: withheld.detail.clone() },
            },
        );
        (calibre, Some(snapshot.computed_ms))
    }

    /// Reads the coin's band for the tier around both marks, or finishes it unavailable.
    fn start(&mut self, coin: &str, tier: Tier, now: Instant, view: &OwnerView<'_>, output: &mut FlowOutput) {
        let key = (coin.to_string(), tier);
        self.pipelines.entry(key.clone()).or_insert(Pipeline { started: None, stage: Stage::Idle, frame: None }).started = Some(now);
        let unavailable = |reason: String| FlowContent::Unavailable(reason);
        if let Some(reason) = self.stopped.clone() {
            return self.finish(&key, FlowHeights::default(), unavailable(reason), output);
        }
        if self.link.is_none() {
            return self.finish(&key, FlowHeights::default(), unavailable("the server runs without an order book".to_string()), output);
        }
        let (Some(marks), Some(size_decimals)) = (view.marks, view.book.size_decimals(coin)) else {
            return self.finish(&key, FlowHeights::default(), unavailable(format!("no node marks of {coin}")), output);
        };
        let Some(node_mark_raw) = marks.mark(coin) else {
            return self.finish(&key, FlowHeights::default(), unavailable(format!("the node replay claims no mark of {coin}")), output);
        };
        let (local, _) = self.local_calibre(coin);
        let local_ready = matches!(local, LocalCalibre::Ready { .. });
        let (lo, hi) = band(node_mark_raw, local_mark_raw(&local), size_decimals, tier);
        self.version += 1;
        let version = self.version;
        self.post((coin.to_string(), tier, FlowJobKind::Band), FlowJob::Band { version, lo, hi });
        if let Some(pipeline) = self.pipelines.get_mut(&key) {
            pipeline.stage = Stage::Reading { version, lo, hi, local_ready };
        }
    }

    /// The band came back: the entries at the current marks, then their walks (r2 revision 2: the band read must cover
    /// the range the current marks need, else it is read again).
    #[expect(clippy::too_many_arguments, reason = "one read's parts, handed in by the dispatch")]
    fn on_band(&mut self, coin: &str, tier: Tier, version: u64, read: Option<TriggerBandAt>, now: Instant, view: &OwnerView<'_>, output: &mut FlowOutput) {
        let key = (coin.to_string(), tier);
        let Some(Stage::Reading { version: expected, lo, hi, .. }) = self.pipelines.get(&key).map(|pipeline| &pipeline.stage) else { return };
        if *expected != version {
            return;
        }
        let (lo, hi) = (*lo, *hi);
        let Some(read) = read else {
            return self.finish(&key, FlowHeights::default(), FlowContent::Unavailable("the order book has no snapshot yet".to_string()), output);
        };
        let (Some(marks), Some(size_decimals)) = (view.marks, view.book.size_decimals(coin)) else {
            return self.finish(&key, FlowHeights::default(), FlowContent::Unavailable(format!("no node marks of {coin}")), output);
        };
        let Some(node_mark_raw) = marks.mark(coin) else {
            return self.finish(&key, FlowHeights::default(), FlowContent::Unavailable(format!("the node replay claims no mark of {coin}")), output);
        };
        let (local, local_published_ms) = self.local_calibre(coin);
        let local_raw = local_mark_raw(&local);
        let (need_lo, need_hi) = band(node_mark_raw, local_raw, size_decimals, tier);
        if need_lo < lo || need_hi > hi {
            // the marks moved past the band read: read the new one
            return self.start(coin, tier, now, view, output);
        }
        let (accounts_height, accounts_time_ms) = view.book.replay_at().map_or((0, 0), |(_, height, time_ms)| (height, time_ms));
        let heights = FlowHeights {
            accounts_height,
            accounts_time_ms,
            trigger_generation: read.band.generation,
            trigger_version: read.band.version,
            listener_height: read.height,
            listener_time: read.time,
            trigger_status_height: read.status_height,
            local_published_ms,
            sell_walk_book: None,
            buy_walk_book: None,
        };
        let liquidations = match view.candidates.liquidation_inputs(view.book, coin, node_mark_raw, local_raw, tier.bps()) {
            Ok(liquidations) => liquidations,
            Err(reason) => return self.finish(&key, heights, FlowContent::Unavailable(reason), output),
        };
        // the orders placed nowhere (a trailing stop waiting for its activation, a condition of another form): their
        // count is in `unplaced`
        let unpriced = read.band.placed_nowhere.iter().map(|order| TriggerInput { trigger_px: Px::new(0), order: Arc::clone(order) });
        let triggers: Vec<TriggerInput> =
            read.band.orders.iter().map(|(trigger_px, order)| TriggerInput { trigger_px: *trigger_px, order: Arc::clone(order) }).chain(unpriced).collect();
        let inputs = CoinInputs { size_decimals, node_mark_raw, local_mark_raw: local_raw, triggers: &triggers, liquidations: &liquidations };
        let flow = match coin_flow(&inputs, |user| view.book.position_raw(user, coin)) {
            Ok(flow) => flow,
            Err(err) => {
                error!("forced flow {coin}: {err}");
                return self.finish(&key, heights, FlowContent::Unavailable(err), output);
            }
        };
        let triggers_status = trigger_status(&read);
        let ready = ready_flow(flow, tier, size_decimals, node_mark_raw, local, triggers_status);
        self.walk(&key, heights, ready, (read.trusted, read.withheld_periods), output);
    }

    /// Posts the entries' walks, or finishes at once without entries or while the book is withheld.
    fn walk(&mut self, key: &(String, Tier), heights: FlowHeights, mut ready: ReadyFlow, trust: (bool, u64), output: &mut FlowOutput) {
        if !trust.0 {
            ready.walks = WalkStatus::Untrusted;
            return self.finish(key, heights, FlowContent::Ready(Box::new(ready)), output);
        }
        if ready.entries.is_empty() {
            ready.walks = WalkStatus::Walked(Vec::new());
            return self.finish(key, heights, FlowContent::Ready(Box::new(ready)), output);
        }
        let mut cumulative_by_side = cumulative_units(&ready.entries, key.1.bps(), ready.size_decimals);
        let mut sides: Vec<SideJob> = Vec::new();
        for taker in [Side::Ask, Side::Bid] {
            let items: Vec<WalkItem> = ready
                .entries
                .iter()
                .filter(|flow_entry| flow_entry.entry.side == taker)
                .map(|flow_entry| {
                    let entry = &flow_entry.entry;
                    (walk_units(entry.size_raw, ready.size_decimals), entry.limit_px.map(|px| Limit { taker, px }))
                })
                .collect();
            if items.is_empty() {
                continue;
            }
            // a side of only limit trigger orders has no cumulative size
            let cumulative = cumulative_by_side.remove(&taker).unwrap_or_default();
            sides.push(SideJob { taker, items, cumulative });
        }
        let cumulative = sides.iter().map(|side| (side.taker, side.cumulative.clone())).collect();
        self.version += 1;
        let version = self.version;
        self.post((key.0.clone(), key.1, FlowJobKind::Walk), FlowJob::Walk { version, sides });
        if let Some(pipeline) = self.pipelines.get_mut(key) {
            pipeline.stage = Stage::Walking(Box::new(Walking { version, heights, ready, trust, cumulative }));
        }
    }

    fn post(&self, key: (String, Tier, FlowJobKind), job: FlowJob) {
        let Some(link) = &self.link else { unreachable!("a pipeline reads only with an order book") };
        link.mailbox.post(key, job);
    }

    /// The walks came back: taken with the local leg as valid as when the content was started (else started again,
    /// review 000460 r1 P2-1), and only while the book stayed trusted in the period the band was read in - a book
    /// withheld since may also have missed trigger statuses, so the trigger part is incomplete too.
    #[expect(clippy::too_many_arguments, reason = "one read's parts, handed in by the dispatch")]
    fn on_walks(&mut self, coin: &str, tier: Tier, version: u64, sides: Vec<SideWalks>, now: Instant, view: &OwnerView<'_>, output: &mut FlowOutput) {
        let key = (coin.to_string(), tier);
        let Some(pipeline) = self.pipelines.get(&key) else { return };
        let Stage::Walking(walking) = &pipeline.stage else { return };
        if walking.version != version {
            return;
        }
        if matches!(walking.ready.local, LocalCalibre::Ready { .. }) != self.local_ready(coin) {
            return self.revalidate_local(&key, now, view, output);
        }
        let Some(pipeline) = self.pipelines.get_mut(&key) else { unreachable!("found above") };
        let Stage::Walking(walking) = std::mem::replace(&mut pipeline.stage, Stage::Idle) else { unreachable!("matched above") };
        let Walking { mut heights, mut ready, trust, cumulative, .. } = *walking;
        let link_trust = self.link.as_ref().map(|link| (link.trust.is_trusted(), link.trust.withheld_periods()));
        let read_trusted = sides.iter().filter_map(|side| side.book).all(|book| (book.trusted, book.withheld_periods) == trust);
        if link_trust != Some(trust) || !read_trusted {
            ready.walks = WalkStatus::Untrusted;
            ready.triggers = TriggerStatus::Incomplete("bookWithheld".to_string());
        } else if sides.iter().all(|side| side.book.is_none()) {
            ready.walks = WalkStatus::NoBook;
        } else {
            attach_walks(&mut ready.entries, &sides);
            ready.cumulative = cumulative_walks(&cumulative, &sides);
            for side in &sides {
                let at = side.book.map(|book| (book.height, book.time));
                match side.taker {
                    Side::Ask => heights.sell_walk_book = at,
                    Side::Bid => heights.buy_walk_book = at,
                }
            }
            ready.walks = WalkStatus::Walked(sides.iter().filter_map(|side| side.book.map(|book| (side.taker, book.mid_px))).collect());
        }
        self.finish(&key, heights, FlowContent::Ready(Box::new(ready)), output);
    }

    /// The pipeline's frame: a new `seq` and published when the content changed.
    fn finish(&mut self, key: &(String, Tier), heights: FlowHeights, content: FlowContent, output: &mut FlowOutput) {
        let Some(pipeline) = self.pipelines.get_mut(key) else { return };
        pipeline.stage = Stage::Idle;
        self.publish(key, heights, content, true, output);
    }

    /// The key's frame: a new `seq` and published when the content changed. With `answers`, it serves the queries
    /// that came before its computation started.
    fn publish(&mut self, key: &(String, Tier), heights: FlowHeights, content: FlowContent, answers: bool, output: &mut FlowOutput) {
        let Some(pipeline) = self.pipelines.get_mut(key) else { return };
        let previous = pipeline.frame.as_ref();
        let changed = previous.is_none_or(|frame| frame.content != content);
        let seq = previous.map_or(0, |frame| frame.seq) + u64::from(changed);
        let frame = Arc::new(FlowFrame { coin: key.0.clone(), tier: key.1, seq, heights, content });
        pipeline.frame = Some(Arc::clone(&frame));
        if changed {
            output.frames.push(Arc::clone(&frame));
        }
        let Some(started) = pipeline.started.filter(|_| answers) else { return };
        for waiter in self.waiters.iter_mut().filter(|waiter| waiter.tier == key.1 && waiter.since <= started) {
            if waiter.remaining.remove(&key.0) {
                waiter.frames.push(Arc::clone(&frame));
            }
        }
    }

    /// Answers every query whose coins are all in.
    fn answer_waiters(&mut self) {
        let (done, waiting): (Vec<Waiter>, Vec<Waiter>) = self.waiters.drain(..).partition(|waiter| waiter.remaining.is_empty());
        self.waiters = waiting;
        for mut waiter in done {
            waiter.frames.sort_unstable_by(|left, right| left.coin.cmp(&right.coin));
            drop(waiter.reply.send(Ok(waiter.frames)));
        }
    }
}

/// The coins of the account view with a node mark.
fn marked_coins(view: &OwnerView<'_>) -> Vec<String> {
    let Some(marks) = view.marks else { return Vec::new() };
    let mut coins: Vec<String> = view.book.coins().filter(|coin| marks.mark(coin).is_some()).map(str::to_string).collect();
    coins.sort_unstable();
    coins
}

const fn local_mark_raw(local: &LocalCalibre) -> Option<u64> {
    match local {
        LocalCalibre::Ready { mark_raw } => Some(*mark_raw),
        LocalCalibre::Withheld { .. } => None,
    }
}

/// The trigger prices within the tier of either mark, a little wider, in listener units.
fn band(node_mark_raw: u64, local_mark_raw: Option<u64>, size_decimals: u32, tier: Tier) -> (Px, Px) {
    let mark = |raw: u64| super::checkpoint::mark_px(raw, size_decimals);
    let (node, local) = (mark(node_mark_raw), local_mark_raw.map_or_else(|| mark(node_mark_raw), mark));
    let cap = f64::from(tier.bps()) / 1e4;
    let low = (node.min(local) * (1.0 - cap - RANGE_MARGIN)).max(0.0);
    let high = node.max(local) * (1.0 + cap + RANGE_MARGIN);
    #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss, reason = "non-negative prices below 2^53 in 1e-8 units")]
    let px = |usd: f64, round: fn(f64) -> f64| Px::new(round(usd * PX_SCALE) as u64);
    (px(low, f64::floor), px(high, f64::ceil))
}

/// Complete only with the whole table, an untruncated band and a trusted book (r1 P2-3: a loss of the statuses stream
/// may have dropped trigger statuses too).
fn trigger_status(read: &TriggerBandAt) -> TriggerStatus {
    if let TriggerAvailability::Gap(gap) = read.band.availability {
        return TriggerStatus::Incomplete(gap.label().to_string());
    }
    if read.band.truncated {
        return TriggerStatus::Incomplete("truncated".to_string());
    }
    if !read.trusted {
        return TriggerStatus::Incomplete("bookWithheld".to_string());
    }
    TriggerStatus::Complete
}

fn ready_flow(flow: CoinFlow, tier: Tier, size_decimals: u32, node_mark_raw: u64, local: LocalCalibre, triggers: TriggerStatus) -> ReadyFlow {
    let entries = flow.entries.into_iter().filter(|entry| entry.in_range(tier.bps())).map(|entry| FlowEntry { entry, walk: None }).collect();
    ReadyFlow { size_decimals, node_mark_raw, local, triggers, walks: WalkStatus::NoBook, entries, unplaced: flow.unplaced, cumulative: BTreeMap::new() }
}

/// The bands' cumulative sizes to walk (`forced_flow::cumulative_sizes`) per taker side, in 1e-8 units, ascending.
fn cumulative_units(entries: &[FlowEntry], max_bps: u32, size_decimals: u32) -> BTreeMap<Side, Vec<u64>> {
    let entries: Vec<Entry> = entries.iter().map(|flow_entry| flow_entry.entry.clone()).collect();
    #[expect(clippy::expect_used, reason = "a coin's summed sizes stay far below 2^63 raw units")]
    let units = |raw: u128| walk_units(u64::try_from(raw).expect("a band's cumulative size fits u64"), size_decimals);
    cumulative_sizes(&entries, max_bps).into_iter().map(|(side, sizes)| (side, sizes.into_iter().map(units).collect())).collect()
}

/// The cumulative sizes' walks by (side, size), from each walked side's results in the job's order.
fn cumulative_walks(posted: &[(Side, Vec<u64>)], sides: &[SideWalks]) -> BTreeMap<(Side, u64), WalkPart> {
    let mut walks = BTreeMap::new();
    for walked in sides.iter().filter(|walked| walked.book.is_some()) {
        let (_, posted_sizes) = posted.iter().find(|(taker, _)| *taker == walked.taker).unwrap_or_else(|| unreachable!("the reader answers the posted sides"));
        assert_eq!(posted_sizes.len(), walked.cumulative.len(), "the reader walks every posted cumulative size of a side with a book");
        walks.extend(posted_sizes.iter().zip(&walked.cumulative).map(|(size, walk)| ((walked.taker, *size), walk.clone())));
    }
    walks
}

/// Each entry's walk from its side's results, in the order the job listed them.
fn attach_walks(entries: &mut [FlowEntry], sides: &[SideWalks]) {
    for side in sides {
        let mut walks = side.walks.iter();
        for flow_entry in entries.iter_mut().filter(|flow_entry| flow_entry.entry.side == side.taker) {
            flow_entry.walk = walks.next().cloned();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{
        accounts::{
            checkpoint::tests::{HOLDER, user as checkpoint_user},
            forced_flow::{Source, SourceKind},
            forced_flow_reader::{BookAt, FlowJobKey},
            test_support::{ListedMarks, PUMP, replay_book},
            walk::WalkEnd,
        },
        listeners::order_book::triggers::{TriggerBand, TriggerGap},
        local_marks::snapshot::tests::{NOW_MS, World},
        order_book::{Coin, Sz},
        types::inner::InnerL4Order,
    };

    const UNIT: u64 = 100_000_000;

    /// HOLDER's reduce-only stop selling 100 PUMP below `trigger_px` (1e-8 USD).
    fn stop(oid: u64, trigger_px: u64) -> (Px, Arc<InnerL4Order>) {
        let order = InnerL4Order {
            user: checkpoint_user(HOLDER),
            coin: Coin::new(PUMP),
            side: Side::Ask,
            limit_px: Px::new(trigger_px * 9 / 10),
            sz: Sz::new(100 * UNIT),
            oid,
            timestamp: 0,
            trigger_condition: format!("Price below {trigger_px}"),
            is_trigger: true,
            trigger_px: trigger_px.to_string(),
            is_position_tpsl: false,
            reduce_only: true,
            order_type: "Stop Market".to_string(),
            tif: None,
            cloid: None,
        };
        (Px::new(trigger_px), Arc::new(order))
    }

    /// A stop selling 100 BTC below `trigger_px` (1e-8 USD), not reduce-only.
    fn btc_stop(oid: u64, trigger_px: u64) -> (Px, Arc<InnerL4Order>) {
        let (px, order) = stop(oid, trigger_px);
        let mut order = (*order).clone();
        (order.coin, order.reduce_only) = (Coin::new("BTC"), false);
        (px, Arc::new(order))
    }

    fn band_at(orders: Vec<(Px, Arc<InnerL4Order>)>, availability: TriggerAvailability, trusted: bool) -> TriggerBandAt {
        let band = TriggerBand { availability, generation: 1, version: 3, orders, placed_nowhere: Vec::new(), truncated: false };
        TriggerBandAt { time: 5, height: 7, status_height: 6, trusted, withheld_periods: 0, band }
    }

    /// HOLDER's trailing stop waiting for its activation (`triggerPx` 0).
    fn waiting_trailing_stop(oid: u64) -> Arc<InnerL4Order> {
        let (_, order) = stop(oid, 0);
        let mut order = (*order).clone();
        (order.trigger_condition, order.order_type) = ("Activation above 0.02, retracement 5%, best waiting".to_string(), "Trailing Stop Market".to_string());
        Arc::new(order)
    }

    /// The account view: HOLDER long 1,000,000 PUMP at mark 0.0125 (and BTC at 100,020), candidates trusted.
    struct Owner {
        book: AccountBook,
        marks: ListedMarks,
        candidates: Candidates,
        mailbox: Arc<FlowMailbox>,
        trust: BookTrust,
        demand: FlowDemand,
        flow: ForcedFlow,
    }

    impl Owner {
        fn new() -> Self {
            let book = replay_book(1_000, 1_000, &[(checkpoint_user(HOLDER), 1_000_000)]);
            let marks = ListedMarks { height: 1_000, marks: vec![(PUMP, 12_500), ("BTC", 1_000_200)] };
            let mut candidates = Candidates::new(None);
            drop(candidates.refresh(&book, Some(&marks), &[]));
            let (mailbox, trust, demand) = (Arc::new(FlowMailbox::default()), BookTrust::new(), FlowDemand::default());
            let flow = ForcedFlow::new(demand.clone(), Some(FlowLink { trust: trust.clone(), mailbox: Arc::clone(&mailbox) }));
            Self { book, marks, candidates, mailbox, trust, demand, flow }
        }

        fn query(&mut self, coin: &str, tier: Tier, now: Instant) -> oneshot::Receiver<Result<Vec<Arc<FlowFrame>>, String>> {
            let (reply, answer) = oneshot::channel();
            let view = OwnerView { book: &self.book, marks: Some(&self.marks), candidates: &self.candidates };
            drop(self.flow.query(Some(coin.to_string()), tier, reply, now, &view));
            answer
        }

        fn local(&mut self, closed: bool, now: Instant) -> FlowOutput {
            let view = OwnerView { book: &self.book, marks: Some(&self.marks), candidates: &self.candidates };
            self.flow.on_local(closed, now, &view)
        }

        fn tick(&mut self, now: Instant) -> FlowOutput {
            let view = OwnerView { book: &self.book, marks: Some(&self.marks), candidates: &self.candidates };
            self.flow.tick(now, &view)
        }

        fn read(&mut self, read: FlowRead, now: Instant) -> FlowOutput {
            let view = OwnerView { book: &self.book, marks: Some(&self.marks), candidates: &self.candidates };
            self.flow.on_read(read, now, &view)
        }

        /// The one pending job.
        fn job(&self) -> (FlowJobKey, FlowJob) {
            let mut jobs: Vec<_> = self.mailbox.drain().into_iter().collect();
            assert_eq!(jobs.len(), 1, "{jobs:?}");
            jobs.remove(0)
        }

        /// The pending band job's version, answered with `read`; then the walk job's, answered with a fill of each.
        fn answer(&mut self, coin: &str, tier: Tier, read: TriggerBandAt, now: Instant) -> FlowOutput {
            self.answer_at(coin, tier, read, now, 7)
        }

        /// As `answer`, the walked book at `book_height`.
        fn answer_at(&mut self, coin: &str, tier: Tier, read: TriggerBandAt, now: Instant, book_height: u64) -> FlowOutput {
            let output = self.answer_band(coin, tier, read, now);
            let jobs = self.mailbox.drain();
            let Some(FlowJob::Walk { version, sides }) = jobs.into_values().next() else { return output };
            self.answer_walks(coin, tier, version, sides, now, book_height)
        }

        /// The pending band job's version, answered with `read`.
        fn answer_band(&mut self, coin: &str, tier: Tier, read: TriggerBandAt, now: Instant) -> FlowOutput {
            let ((_, _, kind), FlowJob::Band { version, .. }) = self.job() else { panic!("a band job") };
            assert_eq!(kind, FlowJobKind::Band);
            self.read(FlowRead::Band { coin: coin.to_string(), tier, version, read: Some(read) }, now)
        }

        /// A walk job answered with a fill of each entry over the book at `book_height`.
        fn answer_walks(&mut self, coin: &str, tier: Tier, version: u64, sides: Vec<SideJob>, now: Instant, book_height: u64) -> FlowOutput {
            let book = BookAt { height: book_height, time: 5, trusted: true, withheld_periods: 0, mid_px: Some(0.0125) };
            let fill = |size: u64| WalkPart { filled: size, vwap_px: Some(0.0124), first_px: None, deepest_px: None, levels: 1, end: WalkEnd::Filled, steps: Vec::new() };
            let sides = sides
                .into_iter()
                .map(|side| SideWalks {
                    taker: side.taker,
                    book: Some(book),
                    walks: side.items.iter().map(|(size, _)| fill(*size)).collect(),
                    cumulative: side.cumulative.iter().map(|size| fill(*size)).collect(),
                })
                .collect();
            self.read(FlowRead::Walk { coin: coin.to_string(), tier, version, sides }, now)
        }
    }

    fn ready(frame: &FlowFrame) -> &ReadyFlow {
        let FlowContent::Ready(ready) = &frame.content else { panic!("{:?}", frame.content) };
        ready
    }

    fn trigger_oids(frame: &FlowFrame) -> Vec<u64> {
        ready(frame)
            .entries
            .iter()
            .filter_map(|flow_entry| match &flow_entry.entry.source {
                Source::Trigger { order, .. } => Some(order.oid),
                Source::Liquidation { .. } => None,
            })
            .collect()
    }

    fn ended() -> LocalCalibre {
        LocalCalibre::Withheld { code: "localMarksEnded", reason: "the local marks owner task ended".to_string() }
    }

    /// r4 revisions 9 and 11: a demanded (coin, tier) reads its band, then walks its entries, and publishes; within the
    /// period nothing runs; after it the same content publishes nothing; another content a new `seq`.
    #[test]
    fn a_demanded_tier_reads_walks_and_publishes_each_new_content() {
        let mut owner = Owner::new();
        let _guard = owner.demand.acquire((Some(PUMP.to_string()), Tier::Mid));
        let start = Instant::now();
        assert_eq!(owner.tick(start).frames.len(), 0);
        // 40 bp below the mark
        let read = band_at(vec![stop(1, 1_245_000)], TriggerAvailability::Complete, true);
        let published = owner.answer(PUMP, Tier::Mid, read.clone(), start);
        assert_eq!(published.frames.len(), 1);
        let frame = &published.frames[0];
        assert_eq!((frame.seq, frame.heights.listener_height, frame.heights.trigger_status_height, frame.heights.trigger_version), (1, 7, 6, 3));
        let flow = ready(frame);
        assert_eq!((flow.triggers.clone(), flow.entries.len()), (TriggerStatus::Complete, 1));
        let entry = &flow.entries[0];
        assert_eq!((entry.entry.size_raw, entry.walk.as_ref().map(|walk| walk.filled)), (100, Some(100 * UNIT)));
        assert!((entry.entry.distance_node_bps - 40.0).abs() < 1e-6);
        assert_eq!(flow.local, LocalCalibre::Withheld { code: "localMarksOff", reason: "the server runs without --local-marks".to_string() });
        // the 50 and 100 bp bands hold the stop: one cumulative size, walked
        assert_eq!(flow.cumulative.iter().map(|((side, size), walk)| (*side, *size, walk.filled)).collect::<Vec<_>>(), [(Side::Ask, 100 * UNIT, 100 * UNIT)]);
        owner.tick(start + Duration::from_millis(500));
        assert!(owner.mailbox.drain().is_empty(), "within the tier's second");
        owner.tick(start + Duration::from_secs(1));
        let again = owner.answer(PUMP, Tier::Mid, read, start + Duration::from_secs(1));
        assert!(again.frames.is_empty(), "the same content");
        owner.tick(start + Duration::from_secs(2));
        let another = band_at(vec![stop(1, 1_245_000), stop(2, 1_240_000)], TriggerAvailability::Complete, true);
        let changed = owner.answer(PUMP, Tier::Mid, another, start + Duration::from_secs(2));
        assert_eq!((changed.frames.len(), changed.frames[0].seq, ready(&changed.frames[0]).entries.len()), (1, 2, 2));
        assert_eq!(ready(&changed.frames[0]).entries[0].entry.source.kind(), SourceKind::Trigger);
        // 40 and 80 bp: the 50 bp band holds one stop, the 100 bp band both
        assert_eq!(ready(&changed.frames[0]).cumulative.keys().copied().collect::<Vec<_>>(), [(Side::Ask, 100 * UNIT), (Side::Ask, 200 * UNIT)]);
    }

    /// A read of an older version is dropped; a band the current marks moved past is read again (r2 revision 2).
    #[test]
    fn a_stale_read_or_a_band_the_marks_left_is_not_taken() {
        let mut owner = Owner::new();
        let _guard = owner.demand.acquire((Some(PUMP.to_string()), Tier::Near));
        let now = Instant::now();
        owner.tick(now);
        let ((_, _, _), FlowJob::Band { version, .. }) = owner.job() else { panic!("a band job") };
        let read = band_at(vec![stop(1, 1_249_000)], TriggerAvailability::Complete, true);
        let stale = owner.read(FlowRead::Band { coin: PUMP.to_string(), tier: Tier::Near, version: version - 1, read: Some(read.clone()) }, now);
        assert_eq!((stale.frames.len(), owner.mailbox.drain().len()), (0, 0));
        owner.marks.marks[0].1 = 12_600;
        assert_eq!(owner.read(FlowRead::Band { coin: PUMP.to_string(), tier: Tier::Near, version, read: Some(read) }, now).frames.len(), 0);
        let ((_, _, kind), FlowJob::Band { lo, hi, .. }) = owner.job() else { panic!("the band read again") };
        assert_eq!(kind, FlowJobKind::Band);
        assert!(lo.value() <= 1_258_740 && hi.value() >= 1_261_260, "the new mark's 10 bp: {lo:?} {hi:?}");
    }

    /// r3 revision 5, r1 P2-3: a gap in the table or a withheld book marks the trigger part incomplete; a withheld book
    /// leaves the entries unwalked, and so does a book withheld while the walks were out.
    #[test]
    fn incomplete_tables_and_withheld_books_are_marked() {
        let mut owner = Owner::new();
        let _guard = owner.demand.acquire((Some(PUMP.to_string()), Tier::Mid));
        let now = Instant::now();
        owner.tick(now);
        let gap = band_at(vec![stop(1, 1_245_000)], TriggerAvailability::Gap(TriggerGap::LiveParseFailed), true);
        let frame = owner.answer(PUMP, Tier::Mid, gap, now).frames.remove(0);
        assert_eq!(ready(&frame).triggers, TriggerStatus::Incomplete("liveParseFailed".to_string()));
        owner.tick(now + Duration::from_secs(1));
        let withheld = band_at(vec![stop(1, 1_245_000)], TriggerAvailability::Complete, false);
        let frame = owner.answer(PUMP, Tier::Mid, withheld, now + Duration::from_secs(1)).frames.remove(0);
        assert_eq!((ready(&frame).triggers.clone(), ready(&frame).walks.clone()), (TriggerStatus::Incomplete("bookWithheld".to_string()), WalkStatus::Untrusted));
        assert!(ready(&frame).entries[0].walk.is_none());
        // the walks go out with a trusted book, which turns withheld before they come back
        owner.tick(now + Duration::from_secs(2));
        let ((_, _, _), FlowJob::Band { version, .. }) = owner.job() else { panic!("a band job") };
        let complete = band_at(vec![stop(1, 1_245_000)], TriggerAvailability::Complete, true);
        owner.read(FlowRead::Band { coin: PUMP.to_string(), tier: Tier::Mid, version, read: Some(complete) }, now);
        let ((_, _, _), FlowJob::Walk { version, sides }) = owner.job() else { panic!("a walk job") };
        owner.trust.set_for_test(false);
        let book = BookAt { height: 7, time: 5, trusted: true, withheld_periods: 0, mid_px: None };
        let sides = sides.into_iter().map(|side| SideWalks { taker: side.taker, book: Some(book), walks: Vec::new(), cumulative: Vec::new() }).collect();
        // the same content as the withheld read above: nothing goes out, the kept frame says it
        assert_eq!(owner.read(FlowRead::Walk { coin: PUMP.to_string(), tier: Tier::Mid, version, sides }, now).frames.len(), 0);
        let kept = owner.flow.pipelines[&(PUMP.to_string(), Tier::Mid)].frame.clone().unwrap();
        assert_eq!(ready(&kept).walks, WalkStatus::Untrusted);
        assert_eq!(ready(&kept).triggers, TriggerStatus::Incomplete("bookWithheld".to_string()), "review 000460 r1 P2-1");
    }

    /// r5 revision 14, review 000460 r1 P2-2: every HTTP query computes anew - a frame's age does not prove its inputs
    /// unchanged -: an order changed 1 ms after the first query is in the second's answer.
    #[test]
    fn every_query_computes_anew() {
        let mut owner = Owner::new();
        let now = Instant::now();
        let mut answer = owner.query(PUMP, Tier::Far, now);
        assert!(answer.try_recv().is_err(), "waits for the reads");
        owner.answer(PUMP, Tier::Far, band_at(vec![stop(1, 1_245_000)], TriggerAvailability::Complete, true), now);
        let triggers = |frame: &FlowFrame| ready(frame).entries.iter().filter(|entry| entry.entry.source.kind() == SourceKind::Trigger).count();
        let frames = answer.try_recv().unwrap().unwrap();
        assert_eq!((frames.len(), frames[0].seq, triggers(&frames[0])), (1, 1, 1), "the stop, beside HOLDER's liquidation");
        let mut answer = owner.query(PUMP, Tier::Far, now + Duration::from_millis(1));
        assert!(answer.try_recv().is_err(), "computed again within the period");
        owner.answer(PUMP, Tier::Far, band_at(Vec::new(), TriggerAvailability::Complete, true), now + Duration::from_millis(1));
        let frames = answer.try_recv().unwrap().unwrap();
        assert_eq!((frames[0].seq, triggers(&frames[0])), (2, 0), "the order is gone");
    }

    /// Review 000460 r2 P2-2: queries arriving while a read is out do not void it - each read answers the queries that
    /// came before it started, a follow-up the later ones -: 32 rounds of "a new query, then the read under way back"
    /// answer one query each, never with a read started before it.
    #[test]
    fn overlapping_queries_make_progress() {
        let mut owner = Owner::new();
        let start = Instant::now();
        let mut previous = owner.query(PUMP, Tier::Mid, start);
        for round in 1..=32 {
            let now = start + Duration::from_millis(round);
            let mut current = owner.query(PUMP, Tier::Mid, now);
            // the read started for the previous query finds order `round`
            owner.answer(PUMP, Tier::Mid, band_at(vec![stop(round, 1_245_000)], TriggerAvailability::Complete, true), now);
            assert_eq!(trigger_oids(&previous.try_recv().unwrap().unwrap()[0]), [round]);
            assert!(current.try_recv().is_err(), "round {round}: the read started before the query");
            previous = current;
        }
        owner.answer(PUMP, Tier::Mid, band_at(vec![stop(33, 1_245_000)], TriggerAvailability::Complete, true), start + Duration::from_millis(33));
        assert_eq!(trigger_oids(&previous.try_recv().unwrap().unwrap()[0]), [33]);
    }

    /// Review 000460 r1 P2-1: the local leg turning withheld while a read is out - before the first frame, or while an
    /// older frame stands - voids the read: the pipeline starts again with the leg as it is.
    #[test]
    fn a_local_leg_changing_validity_voids_the_reads_under_way() {
        let mut owner = Owner::new();
        let world = World::new();
        let (local, snapshots) = watch::channel(Arc::new(world.snapshot(NOW_MS)));
        owner.flow.attach_local(snapshots);
        let _guard = owner.demand.acquire((Some("BTC".to_string()), Tier::Far));
        let now = Instant::now();
        // no frame yet: the band is read with the leg ready, and the walks are out when the local marks end
        owner.tick(now);
        let ((_, _, _), FlowJob::Band { version, .. }) = owner.job() else { panic!("a band job") };
        let trigger = |oid: u64| {
            let (_, order) = stop(oid, 0);
            let mut order = (*order).clone();
            order.coin = Coin::new("BTC");
            (Px::new(9_990_000_000_000), Arc::new(order))
        };
        let read = band_at(vec![trigger(1)], TriggerAvailability::Complete, true);
        drop(owner.read(FlowRead::Band { coin: "BTC".to_string(), tier: Tier::Far, version, read: Some(read.clone()) }, now));
        let ((_, _, _), FlowJob::Walk { version: walk_version, sides }) = owner.job() else { panic!("a walk job") };
        drop(local);
        owner.local(true, now + Duration::from_millis(10));
        let book = BookAt { height: 7, time: 5, trusted: true, withheld_periods: 0, mid_px: None };
        let walked = sides
            .into_iter()
            .map(|side| SideWalks {
                taker: side.taker,
                book: Some(book),
                walks: side.items.iter().map(|_| WalkPart { filled: 0, vwap_px: None, first_px: None, deepest_px: None, levels: 0, end: WalkEnd::BookExhausted, steps: Vec::new() }).collect(),
                cumulative: side.cumulative.iter().map(|_| WalkPart { filled: 0, vwap_px: None, first_px: None, deepest_px: None, levels: 0, end: WalkEnd::BookExhausted, steps: Vec::new() }).collect(),
            })
            .collect();
        let late = owner.read(FlowRead::Walk { coin: "BTC".to_string(), tier: Tier::Far, version: walk_version, sides: walked }, now);
        assert_eq!(late.frames.len(), 0, "the walks of the voided read are not taken");
        let frame = owner.answer("BTC", Tier::Far, read.clone(), now).frames.remove(0);
        assert_eq!(frame.seq, 1);
        assert!(matches!(&ready(&frame).local, LocalCalibre::Withheld { code: "localMarksEnded", .. }), "{:?}", ready(&frame).local);
        // a frame stands; its refresh is out when the leg comes back... a new channel, ready again
        let (local, snapshots) = watch::channel(Arc::new(world.snapshot(NOW_MS)));
        owner.flow.attach_local(snapshots);
        owner.tick(now + Duration::from_secs(6));
        let ((_, _, _), FlowJob::Band { version, .. }) = owner.job() else { panic!("the refresh's band job") };
        let mut withheld = World::new();
        withheld.book = None;
        local.send_replace(Arc::new(withheld.snapshot(NOW_MS)));
        owner.local(false, now + Duration::from_secs(6));
        drop(owner.read(FlowRead::Band { coin: "BTC".to_string(), tier: Tier::Far, version, read: Some(read.clone()) }, now));
        let jobs = owner.mailbox.drain();
        assert!(jobs.keys().all(|(_, _, kind)| *kind == FlowJobKind::Band), "the voided read walks nothing: {jobs:?}");
        // the walks come back before the owner saw the local snapshot that changed: they are not taken either
        local.send_replace(Arc::new(world.snapshot(NOW_MS)));
        owner.local(false, now + Duration::from_secs(7));
        let ((_, _, _), FlowJob::Band { version, .. }) = owner.job() else { panic!("a band job") };
        drop(owner.read(FlowRead::Band { coin: "BTC".to_string(), tier: Tier::Far, version, read: Some(read) }, now));
        let ((_, _, _), FlowJob::Walk { version, sides }) = owner.job() else { panic!("a walk job") };
        local.send_replace(Arc::new(withheld.snapshot(NOW_MS)));
        let walked = sides
            .into_iter()
            .map(|side| SideWalks {
                taker: side.taker,
                book: Some(book),
                walks: side.items.iter().map(|_| WalkPart { filled: 0, vwap_px: None, first_px: None, deepest_px: None, levels: 0, end: WalkEnd::BookExhausted, steps: Vec::new() }).collect(),
                cumulative: side.cumulative.iter().map(|_| WalkPart { filled: 0, vwap_px: None, first_px: None, deepest_px: None, levels: 0, end: WalkEnd::BookExhausted, steps: Vec::new() }).collect(),
            })
            .collect();
        let early = owner.read(FlowRead::Walk { coin: "BTC".to_string(), tier: Tier::Far, version, sides: walked }, now);
        assert_eq!(early.frames.len(), 0, "the local leg is no longer as the read found it");
        assert!(matches!(owner.job(), ((_, _, FlowJobKind::Band), FlowJob::Band { .. })), "started again");
    }

    /// Review 000460 r1 P2-3: a trailing stop waiting for activation comes from the table's read to the frame's
    /// `unplaced`, whatever the band.
    #[test]
    fn an_unpriced_order_is_counted_unplaced() {
        let mut owner = Owner::new();
        let _guard = owner.demand.acquire((Some(PUMP.to_string()), Tier::Near));
        let now = Instant::now();
        owner.tick(now);
        let mut read = band_at(Vec::new(), TriggerAvailability::Complete, true);
        read.band.placed_nowhere = vec![waiting_trailing_stop(9)];
        let frame = owner.answer(PUMP, Tier::Near, read, now).frames.remove(0);
        assert_eq!(ready(&frame).unplaced, [(9, Unplaced::TrailingStopWaiting)]);
    }

    /// Review 000460 r1 P3: only the walked book's height and time moved: no new frame goes out; the kept frame has
    /// the new heights.
    #[test]
    fn the_walked_books_progress_alone_publishes_nothing() {
        let mut owner = Owner::new();
        let _guard = owner.demand.acquire((Some(PUMP.to_string()), Tier::Mid));
        let now = Instant::now();
        owner.tick(now);
        let read = band_at(vec![stop(1, 1_245_000)], TriggerAvailability::Complete, true);
        let first = owner.answer_at(PUMP, Tier::Mid, read.clone(), now, 7).frames.remove(0);
        assert_eq!(first.heights.sell_walk_book, Some((7, 5)));
        owner.tick(now + Duration::from_secs(1));
        assert_eq!(owner.answer_at(PUMP, Tier::Mid, read, now + Duration::from_secs(1), 8).frames.len(), 0);
        let mut answer = owner.query(PUMP, Tier::Mid, now + Duration::from_secs(1));
        let read = band_at(vec![stop(1, 1_245_000)], TriggerAvailability::Complete, true);
        owner.answer_at(PUMP, Tier::Mid, read, now + Duration::from_secs(1), 9);
        let kept = answer.try_recv().unwrap().unwrap().remove(0);
        assert_eq!((kept.seq, kept.heights.sell_walk_book), (1, Some((9, 5))));
    }

    /// r5 revision 13: the local leg of a demanded coin turning withheld, or the local marks ending, starts its pipeline
    /// at once, within the period (the withdrawal goes out first, review 000460 r2 P2-1; the same content again
    /// publishes nothing); turning ready again does too.
    #[test]
    fn a_local_leg_changing_validity_restarts_at_once() {
        let mut owner = Owner::new();
        let world = World::new();
        let (local, snapshots) = watch::channel(Arc::new(world.snapshot(NOW_MS)));
        owner.flow.attach_local(snapshots);
        let _guard = owner.demand.acquire((Some("BTC".to_string()), Tier::Far));
        let now = Instant::now();
        owner.tick(now);
        let empty = band_at(Vec::new(), TriggerAvailability::Complete, true);
        let frame = owner.answer("BTC", Tier::Far, empty.clone(), now).frames.remove(0);
        assert_eq!(ready(&frame).local, LocalCalibre::Ready { mark_raw: 1_000_020 });
        let mut withheld = World::new();
        withheld.book = None;
        local.send_replace(Arc::new(withheld.snapshot(NOW_MS)));
        let frame = owner.local(false, now + Duration::from_millis(50)).frames.remove(0);
        assert!(matches!(&ready(&frame).local, LocalCalibre::Withheld { code: "hlBook", .. }), "{:?}", ready(&frame).local);
        assert_eq!(owner.answer("BTC", Tier::Far, empty.clone(), now).frames.len(), 0);
        // back to ready, then the channel closes
        local.send_replace(Arc::new(world.snapshot(NOW_MS)));
        assert_eq!(owner.local(false, now + Duration::from_millis(100)).frames.len(), 0);
        let frame = owner.answer("BTC", Tier::Far, empty.clone(), now).frames.remove(0);
        assert_eq!(ready(&frame).local, LocalCalibre::Ready { mark_raw: 1_000_020 });
        drop(local);
        let frame = owner.local(true, now + Duration::from_millis(150)).frames.remove(0);
        assert_eq!(ready(&frame).local, ended());
        assert_eq!(owner.answer("BTC", Tier::Far, empty, now).frames.len(), 0);
        assert!(owner.flow.local_snapshots().is_none());
    }

    /// Review 000460 r2 P2-1: the local marks ending withdraw the published local calibre at once, before the new read
    /// is back: no entry keeps a local distance, the entry only the local mark took in is gone, and the content starts
    /// again. The withdrawal answers no query.
    #[test]
    fn an_ended_local_leg_is_withdrawn_before_the_new_read() {
        let mut owner = Owner::new();
        let (local, snapshots) = watch::channel(Arc::new(World::new().snapshot(NOW_MS)));
        owner.flow.attach_local(snapshots);
        let _guard = owner.demand.acquire((Some("BTC".to_string()), Tier::Far));
        let now = Instant::now();
        owner.tick(now);
        // order 1: 999.2 bp below the local mark (100,002), 1,000.8 bp below the node mark (100,020); order 2: 12 bp
        let read = band_at(vec![btc_stop(1, 9_001_000_000_000), btc_stop(2, 9_990_000_000_000)], TriggerAvailability::Complete, true);
        let published = owner.answer("BTC", Tier::Far, read, now).frames.remove(0);
        assert_eq!(trigger_oids(&published), [2, 1], "nearest first");
        owner.tick(now + Duration::from_secs(5));
        let mut answer = owner.query("BTC", Tier::Far, now + Duration::from_secs(5));
        drop(local);
        let withdrawn = owner.local(true, now + Duration::from_secs(5)).frames;
        assert_eq!(withdrawn.len(), 1, "before any read is back");
        let flow = ready(&withdrawn[0]);
        assert_eq!((withdrawn[0].seq, flow.local.clone(), trigger_oids(&withdrawn[0])), (published.seq + 1, ended(), vec![2]));
        assert!(flow.entries.iter().all(|flow_entry| flow_entry.entry.distance_local_bps.is_none()));
        assert_eq!(withdrawn[0].heights.local_published_ms, None);
        assert!(answer.try_recv().is_err(), "a withdrawal is no newer than the query");
        let read = band_at(vec![btc_stop(1, 9_001_000_000_000), btc_stop(2, 9_990_000_000_000)], TriggerAvailability::Complete, true);
        assert_eq!(owner.answer("BTC", Tier::Far, read, now + Duration::from_secs(5)).frames.len(), 0, "the same content");
        assert_eq!(trigger_oids(&answer.try_recv().unwrap().unwrap()[0]), [2]);
    }

    /// Review 000460 r2 P2-1: the walks come back after the local marks' channel closed but before the owner took its
    /// notice: they are not taken with the old local mark - the published calibre is withdrawn and the content starts
    /// again -, and the notice then changes nothing more.
    #[test]
    fn walks_back_before_the_close_notice_are_not_taken() {
        let mut owner = Owner::new();
        let (local, snapshots) = watch::channel(Arc::new(World::new().snapshot(NOW_MS)));
        owner.flow.attach_local(snapshots);
        let _guard = owner.demand.acquire((Some("BTC".to_string()), Tier::Far));
        let now = Instant::now();
        owner.tick(now);
        let read = band_at(vec![btc_stop(1, 9_990_000_000_000)], TriggerAvailability::Complete, true);
        let published = owner.answer("BTC", Tier::Far, read.clone(), now).frames.remove(0);
        assert_eq!(ready(&published).local, LocalCalibre::Ready { mark_raw: 1_000_020 });
        owner.tick(now + Duration::from_secs(5));
        drop(owner.answer_band("BTC", Tier::Far, read, now + Duration::from_secs(5)));
        let ((_, _, _), FlowJob::Walk { version, sides }) = owner.job() else { panic!("a walk job") };
        drop(local);
        let output = owner.answer_walks("BTC", Tier::Far, version, sides, now + Duration::from_secs(5), 7);
        assert_eq!(output.frames.len(), 1, "the withdrawal alone");
        assert_eq!((output.frames[0].seq, ready(&output.frames[0]).local.clone()), (published.seq + 1, ended()));
        assert_eq!(owner.local(true, now + Duration::from_secs(5)).frames.len(), 0);
        let ((_, _, kind), FlowJob::Band { version: restarted, .. }) = owner.job() else { panic!("a band job") };
        assert_eq!((kind, restarted), (FlowJobKind::Band, version + 1), "started again once");
    }

    /// Without an order book every frame is unavailable; a stopped reader answers every query with the reason.
    #[test]
    fn without_a_book_or_a_reader_the_flow_is_unavailable() {
        let book = replay_book(1_000, 1_000, &[]);
        let marks = ListedMarks { height: 1_000, marks: vec![(PUMP, 12_500)] };
        let candidates = Candidates::new(None);
        let demand = FlowDemand::default();
        let mut flow = ForcedFlow::new(demand.clone(), None);
        let _guard = demand.acquire((None, Tier::Near));
        let view = OwnerView { book: &book, marks: Some(&marks), candidates: &candidates };
        let output = flow.tick(Instant::now(), &view);
        assert_eq!(output.frames.len(), 1, "every coin with a node mark");
        assert_eq!(output.frames[0].content, FlowContent::Unavailable("the server runs without an order book".to_string()));
        flow.stop("the forced flow read task ended".to_string());
        let (reply, mut answer) = oneshot::channel();
        drop(flow.query(None, Tier::Near, reply, Instant::now(), &view));
        assert_eq!(answer.try_recv().unwrap(), Err("the forced flow read task ended".to_string()));
    }
}
