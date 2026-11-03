//! The forced flow's own thread (hyperliquid-trade review 000563 r1–r2): its pipelines (`ForcedFlow`), the flow
//! reader's reads, the local marks, its queries and the account owner's events, apart from the account owner, so that
//! a long synchronous step there (a candidates refresh, an anchor install) no longer holds a frame. The frames are
//! computed from the copy of the account view (`flow_accounts`) only while it serves; the copy stopping serving
//! withdraws every frame at once, and the owner's end ends the thread.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use log::{error, info};
use tokio::{
    sync::{
        mpsc::{self, error::TrySendError},
        oneshot, watch,
    },
    task::JoinHandle,
    time::MissedTickBehavior,
};

use super::{
    flow_accounts::{AccountsCopy, FlowAccounts, FlowAccountsEvent, OWNER_ENDED},
    flow_staleness::{CopyTimes, FlowStaleness, StaleSpell},
    forced_flow::Tier,
    forced_flow_owner::{FlowDemand, FlowFrame, FlowLink, FlowOutput, ForcedFlow},
    forced_flow_reader::{FlowMailbox, FlowRead, run_flow_reader},
    owner_thread::spawn_owner,
    publish::FlowPublisher,
    runtime::{BookSource, now_ms},
};
use crate::{
    local_marks::snapshot::LocalMarksSnapshot,
    mailbox::Mailbox,
    metrics::{FORCED_FLOW_OWNER_SECONDS, FORCED_FLOW_STALE_SPELLS_DROPPED_TOTAL},
};

/// Reads in flight back to the forced flow: at most one per (coin, tier, kind) pending.
const FLOW_READS_CAPACITY: usize = 1_024;
/// Queries waiting for the forced flow thread.
const FLOW_QUERIES_CAPACITY: usize = 64;
/// The thread's tick: the tiers' periods are its multiples, and a near key the book floor held, or a computation under
/// way kept, is taken up at the next one (hyperliquid-trade design 000587 r2).
const TICK: Duration = Duration::from_millis(50);
/// Stale spells ended, waiting for their log line: the forced flow thread never waits on the log (review 000693 r1
/// P2-1); a full queue drops the line, counted. A spell ends a second at most.
const SPELL_LOG_CAPACITY: usize = 16;

/// What the WS connections ask the forced flow thread.
pub(crate) enum FlowQuery {
    /// The coin's, or every coin's, latest frames of the tier, computed first when stale (design 000451 r5).
    Frames { coin: Option<String>, tier: Tier, reply: oneshot::Sender<Result<Vec<Arc<FlowFrame>>, String>> },
    /// The local marks' snapshots, for the forced flow's local calibre.
    AttachLocalMarks(watch::Receiver<Arc<LocalMarksSnapshot>>),
}

/// The forced flow, its reads coming back (design 000451), the book's revisions (design 000587 r2) and its trigger
/// fires (design 000591); `reads`, `revisions` and `fires` are `None` without a book.
struct FlowFeed {
    flow: ForcedFlow,
    reads: Option<mpsc::Receiver<FlowRead>>,
    revisions: Option<watch::Receiver<u64>>,
    fires: Option<Arc<Mailbox<String, ()>>>,
}

impl FlowFeed {
    /// With `book`, the flow reader task starts here.
    fn start(book: Option<&BookSource>, demand: FlowDemand) -> Self {
        let Some(BookSource { trust, listener, revision, fires }) = book else {
            return Self { flow: ForcedFlow::new(demand, None), reads: None, revisions: None, fires: None };
        };
        let mailbox = Arc::new(FlowMailbox::default());
        let (results, reads) = mpsc::channel(FLOW_READS_CAPACITY);
        tokio::spawn(run_flow_reader(Arc::clone(listener), Arc::clone(&mailbox), results));
        let flow = ForcedFlow::new(demand, Some(FlowLink { trust: trust.clone(), mailbox }));
        Self { flow, reads: Some(reads), revisions: Some(revision.clone()), fires: Some(Arc::clone(fires)) }
    }
}

/// Starts the forced flow thread (with `book`, its reader task too); the queries go to the returned sender.
pub(crate) fn spawn_forced_flow(
    book: Option<&BookSource>,
    demand: FlowDemand,
    accounts: FlowAccounts,
    events: mpsc::Receiver<FlowAccountsEvent>,
    publisher: FlowPublisher,
) -> std::io::Result<(mpsc::Sender<FlowQuery>, JoinHandle<()>)> {
    let feed = FlowFeed::start(book, demand);
    let (queries_tx, queries) = mpsc::channel(FLOW_QUERIES_CAPACITY);
    let sink = FlowSink::start(publisher)?;
    let thread = spawn_owner("forced-flow", run_flow(feed, accounts, events, queries, sink))?;
    Ok((queries_tx, thread))
}

async fn run_flow(
    feed: FlowFeed,
    mut accounts: FlowAccounts,
    mut events: mpsc::Receiver<FlowAccountsEvent>,
    mut queries: mpsc::Receiver<FlowQuery>,
    mut sink: FlowSink,
) {
    let FlowFeed { mut flow, reads: mut flow_reads, mut revisions, fires } = feed;
    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut served = Served::default();
    loop {
        tokio::select! {
            changed = accounts.validity_changed() => {
                if changed.is_err() {
                    return end(&mut flow, &sink.publisher);
                }
            }
            event = events.recv() => {
                let Some(event) = event else { return end(&mut flow, &sink.publisher) };
                let started = Instant::now();
                accounts.apply(event);
                FORCED_FLOW_OWNER_SECONDS.with_label_values(&["accounts"]).observe(started.elapsed().as_secs_f64());
            }
            query = queries.recv() => {
                let Some(query) = query else {
                    info!("forced flow: every handle dropped, the thread exits");
                    return;
                };
                match query {
                    FlowQuery::Frames { coin, tier, reply } => {
                        sink.publish(time_flow_step("query", || flow.query(coin, tier, reply, handled_now(), &accounts)), &accounts);
                    }
                    FlowQuery::AttachLocalMarks(snapshots) => flow.attach_local(snapshots),
                }
            }
            read = next_flow_read(&mut flow_reads) => {
                let Some(read) = read else {
                    error!("forced flow: the read task ended; the forced flow stays unavailable until restart");
                    flow_reads = None;
                    let reason = "the forced flow read task ended".to_string();
                    flow.stop(reason.clone());
                    sink.publisher.withdraw(reason);
                    continue;
                };
                sink.publish(time_flow_step("read", || flow.on_read(read, handled_now(), &accounts)), &accounts);
            }
            revision = next_revision(&mut revisions) => {
                let Some(revision) = revision else {
                    error!("forced flow: the book revision ended; the forced flow stays unavailable until restart");
                    revisions = None;
                    let reason = "the book revision ended".to_string();
                    flow.stop(reason.clone());
                    sink.publisher.withdraw(reason);
                    continue;
                };
                sink.publish(time_flow_step("book", || flow.on_book(revision, handled_now(), &accounts)), &accounts);
            }
            coins = next_fires(fires.as_deref()) => {
                sink.publish(time_flow_step("fires", || flow.on_fires(&coins, handled_now(), &accounts)), &accounts);
            }
            scheduled = tick.tick() => {
                // the tick's time on its schedule: two handling times a period apart come out short of it about half the
                // time, and the key waited a tick more (bm 2026-10-30: a Near key started 6.4 times a second, not 10)
                sink.publish(time_flow_step("tick", || flow.tick(scheduled.into_std(), handled_now(), &accounts)), &accounts);
            }
            closed = next_local_snapshot(&mut flow) => {
                sink.publish(time_flow_step("local", || flow.on_local(closed, handled_now(), &accounts)), &accounts);
            }
        }
        if let Some(reason) = served.after_step(&accounts) {
            sink.publish(time_flow_step("accounts", || flow.withdraw_accounts(&reason, handled_now(), &accounts)), &accounts);
        }
    }
}

/// The frames' way out of the thread: published, and their account data's age followed by spell (`flow_staleness`),
/// each spell ended handed to the log task.
struct FlowSink {
    publisher: FlowPublisher,
    staleness: FlowStaleness,
    spells: mpsc::Sender<StaleSpell>,
}

impl FlowSink {
    /// With the log's own thread, writing each spell to the log.
    fn start(publisher: FlowPublisher) -> std::io::Result<Self> {
        Self::start_writing(publisher, |spell| info!("{spell}"))
    }

    /// With the log's own thread, started once: a write waiting holds that thread alone, not the forced flow's, nor a
    /// worker of the runtime the network and the book's reads run on (review 000693 r3).
    fn start_writing(publisher: FlowPublisher, write: impl Fn(&StaleSpell) + Send + 'static) -> std::io::Result<Self> {
        let (spells, lines) = mpsc::channel(SPELL_LOG_CAPACITY);
        std::thread::Builder::new().name("flow-spell-log".into()).spawn(move || log_spells(lines, &write))?;
        Ok(Self { publisher, staleness: FlowStaleness::default(), spells })
    }

    /// The step's frames; the copy's own blocks read only when there are frames.
    fn publish(&mut self, output: FlowOutput, accounts: &FlowAccounts) {
        let copy = if output.frames.is_empty() {
            None
        } else {
            accounts.serving().ok().map(|copy| CopyTimes { accounts_time_ms: copy.accounts_at().1, positions_time_ms: copy.positions_at().1 })
        };
        self.deliver(output, now_ms(), copy);
    }

    /// Each spell the frames ended to the log task's queue, never waited on; then the frames.
    fn deliver(&mut self, output: FlowOutput, now_ms: u64, copy: Option<CopyTimes>) {
        for spell in self.staleness.observe(&output.frames, now_ms, copy) {
            if let Err(error) = self.spells.try_send(spell) {
                let cause = match error {
                    TrySendError::Full(_) => "full",
                    TrySendError::Closed(_) => "closed",
                };
                FORCED_FLOW_STALE_SPELLS_DROPPED_TOTAL.with_label_values(&[cause]).inc();
            }
        }
        self.publisher.publish(output);
    }
}

/// The stale spells' log lines on the log's own thread, until the sink is gone.
fn log_spells(mut spells: mpsc::Receiver<StaleSpell>, write: &dyn Fn(&StaleSpell)) {
    while let Some(spell) = spells.blocking_recv() {
        write(&spell);
    }
}

/// The account copy's generation the frames stand on, while it serves.
#[derive(Default)]
struct Served(Option<u64>);

impl Served {
    /// After a step: why every frame is withdrawn now, not at each key's next period - the copy stopped serving (r2
    /// revision 2), or it serves another generation, a snapshot having replaced the facts the frames stand on, whether
    /// or not the loop saw it unavailable in between (review 000566 r1 P2-1).
    fn after_step(&mut self, accounts: &FlowAccounts) -> Option<String> {
        let now = accounts.serving().map(AccountsCopy::generation);
        let before = std::mem::replace(&mut self.0, now.as_ref().ok().copied());
        match (before, now) {
            (Some(before), Ok(generation)) if before != generation => Some(format!("the account view was sent anew (generation {generation})")),
            (Some(_), Err(reason)) => Some(reason),
            _ => None,
        }
    }
}

/// The account owner ended: the forced flow is withdrawn for good and the thread ends, the queue left unread.
fn end(flow: &mut ForcedFlow, publisher: &FlowPublisher) {
    error!("forced flow: {OWNER_ENDED}; the forced flow stays withdrawn until restart");
    flow.stop(OWNER_ENDED.to_string());
    publisher.withdraw(OWNER_ENDED.to_string());
}

/// When a step is handled, by the runtime's clock: the system's, paused in a test.
fn handled_now() -> Instant {
    tokio::time::Instant::now().into_std()
}

/// One forced flow step, timed (design 000451 batch 3).
fn time_flow_step(step: &str, work: impl FnOnce() -> FlowOutput) -> FlowOutput {
    let started = Instant::now();
    let output = work();
    FORCED_FLOW_OWNER_SECONDS.with_label_values(&[step]).observe(started.elapsed().as_secs_f64());
    output
}

/// The next forced-flow read; pending forever without a book. Cancel-safe.
async fn next_flow_read(reads: &mut Option<mpsc::Receiver<FlowRead>>) -> Option<FlowRead> {
    match reads {
        Some(reads) => reads.recv().await,
        None => std::future::pending().await,
    }
}

/// The book's next revision; `None` once its sender is gone; pending forever without a book. Cancel-safe.
async fn next_revision(revisions: &mut Option<watch::Receiver<u64>>) -> Option<u64> {
    match revisions {
        Some(revisions) => revisions.changed().await.ok().map(|()| *revisions.borrow_and_update()),
        None => std::future::pending().await,
    }
}

/// The coins whose held trigger orders fired since the last take; pending forever without a book. Cancel-safe: the
/// mailbox is taken only once it holds coins.
async fn next_fires(fires: Option<&Mailbox<String, ()>>) -> Vec<String> {
    match fires {
        Some(fires) => fires.take().await.into_keys().collect(),
        None => std::future::pending().await,
    }
}

/// Whether the local marks' next snapshot came (`false`) or their channel closed (`true`); pending forever while they
/// are not attached. Cancel-safe.
async fn next_local_snapshot(flow: &mut ForcedFlow) -> bool {
    match flow.local_snapshots() {
        Some(snapshots) => snapshots.changed().await.is_err(),
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::broadcast;

    use super::*;
    use crate::accounts::{
        book::Touched,
        candidates::Candidates,
        checkpoint::tests::{HOLDER, user as checkpoint_user},
        flow_accounts::{AccountsSnapshot, CopyOwner, FlowAccountsValidity, link, tests::serving},
        forced_flow_owner::{FlowContent, FlowDemandGuard, FlowHeights},
        forced_flow_reader::{FlowJob, FlowJobKind},
        publish::AccountsFrame,
        test_support::{ListedMarks, PUMP, replay_book},
    };
    use crate::listeners::order_book::BookTrust;

    /// The loop over PUMP's near demand on a paused clock: the test answers its reads and raises the book revision.
    struct Harness {
        mailbox: Arc<FlowMailbox>,
        reads_back: mpsc::Sender<FlowRead>,
        revision: watch::Sender<u64>,
        fires: Arc<Mailbox<String, ()>>,
        thread: JoinHandle<()>,
        // held for the loop: its validity, demand, events, queries and frames' other ends
        _validity: watch::Sender<FlowAccountsValidity>,
        _near: FlowDemandGuard,
        _events: mpsc::Sender<FlowAccountsEvent>,
        _queries: mpsc::Sender<FlowQuery>,
        _frames: broadcast::Receiver<Arc<AccountsFrame>>,
    }

    impl Harness {
        fn start() -> Self {
            let book = replay_book(1_000, 1_000, &[(checkpoint_user(HOLDER), 1_000_000)]);
            let marks = ListedMarks { height: 1_000, marks: vec![(PUMP, 12_500)] };
            let mut candidates = Candidates::new(None);
            drop(candidates.refresh(&book, Some(&marks), &[]));
            let (accounts, validity) = serving(&book, &marks, &candidates);
            let (mailbox, demand) = (Arc::new(FlowMailbox::default()), FlowDemand::default());
            let near = demand.acquire((Some(PUMP.to_string()), Tier::Near));
            let (reads_back, reads) = mpsc::channel(FLOW_READS_CAPACITY);
            let (revision, revisions) = watch::channel(0);
            let flow = ForcedFlow::new(demand, Some(FlowLink { trust: BookTrust::new(), mailbox: Arc::clone(&mailbox) }));
            let (events_in, events) = mpsc::channel(1);
            let (queries_in, queries) = mpsc::channel(1);
            let (frames, received) = broadcast::channel(16);
            let fires = Arc::new(Mailbox::default());
            let feed = FlowFeed { flow, reads: Some(reads), revisions: Some(revisions), fires: Some(Arc::clone(&fires)) };
            let thread = tokio::spawn(run_flow(feed, accounts, events, queries, FlowSink::start(FlowPublisher::new(frames)).unwrap()));
            Self {
                mailbox,
                reads_back,
                revision,
                fires,
                thread,
                _validity: validity,
                _near: near,
                _events: events_in,
                _queries: queries_in,
                _frames: received,
            }
        }

        /// The band read the loop started since the last look, if any.
        async fn started(&self) -> Option<u64> {
            settle().await;
            let jobs: Vec<_> = self.mailbox.drain().into_iter().collect();
            match jobs.as_slice() {
                [] => None,
                [((_, Tier::Near, FlowJobKind::Band), FlowJob::Band { version, .. })] => Some(*version),
                _ => panic!("one band read at most: {jobs:?}"),
            }
        }

        /// The read back without a book snapshot: the computation ends, the key idle.
        async fn end(&self, version: u64) {
            self.reads_back.send(FlowRead::Band { coin: PUMP.to_string(), tier: Tier::Near, version, read: None }).await.unwrap();
            settle().await;
        }

        fn raise(&self, revision: u64) {
            self.revision.send_replace(revision);
        }

        fn fire(&self) {
            self.fires.post(PUMP.to_string(), ());
        }
    }

    /// Lets the spawned loop handle what is ready.
    async fn settle() {
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
    }

    /// Review 000586: the period is measured on the ticks' schedule - a start whose tick was handled 3 ms late, the
    /// next period's tick handled on time, is a period apart (bm 2026-10-30: a Near key started 6.4 times a second,
    /// not 10).
    #[tokio::test(start_paused = true)]
    async fn a_near_key_period_is_measured_on_the_ticks_schedule() {
        let harness = Harness::start();
        let first = harness.started().await.expect("the first tick");
        harness.end(first).await;
        tokio::time::advance(TICK).await;
        assert_eq!(harness.started().await, None, "the period not run");
        tokio::time::advance(TICK + Duration::from_millis(3)).await;
        let late = harness.started().await.expect("the period's tick, handled 3 ms late");
        harness.end(late).await;
        tokio::time::advance(Duration::from_millis(47)).await;
        assert_eq!(harness.started().await, None, "half a period");
        tokio::time::advance(TICK).await;
        assert!(harness.started().await.is_some(), "a period on the schedule since the late one");
        harness.thread.abort();
    }

    /// Design 000587 r2: a book revision starts a near key without waiting for a tick, once 50 ms are past since its
    /// last start; one within that waits.
    #[tokio::test(start_paused = true)]
    async fn a_book_revision_starts_a_near_key_without_waiting_for_the_tick() {
        let harness = Harness::start();
        let first = harness.started().await.expect("the first tick");
        harness.end(first).await;
        tokio::time::advance(TICK).await;
        assert_eq!(harness.started().await, None, "the tick at 50 ms: no revision, the period not run");
        tokio::time::advance(Duration::from_millis(10)).await;
        harness.raise(1);
        let revised = harness.started().await.expect("started on the revision at 60 ms");
        harness.end(revised).await;
        tokio::time::advance(Duration::from_millis(10)).await;
        harness.raise(2);
        assert_eq!(harness.started().await, None, "10 ms after its start");
        tokio::time::advance(Duration::from_millis(30)).await;
        assert_eq!(harness.started().await, None, "the tick at 100 ms: 40 ms after its start");
        tokio::time::advance(Duration::from_millis(10)).await;
        harness.raise(3);
        assert!(harness.started().await.is_some(), "the revision at 110 ms: 50 ms after its start");
        harness.thread.abort();
    }

    /// Review 000693 r1 P2-1: the stale spells' log is a queue the thread never waits on - full, a spell's line is
    /// dropped and counted, and every frame still goes out.
    /// 2026-11-03 03:01:00.000 UTC.
    const AT: u64 = 1_793_674_860_000;

    /// A step's one frame of PUMP, `age_ms` old in all three at `at_ms`.
    fn aged_output(at_ms: u64, age_ms: u64) -> FlowOutput {
        let heights = FlowHeights { accounts_time_ms: at_ms - age_ms, positions_time_ms: at_ms - age_ms, listener_time: at_ms - age_ms, ..FlowHeights::default() };
        FlowOutput { frames: vec![Arc::new(FlowFrame { coin: PUMP.to_string(), tier: Tier::Near, seq: 1, heights, content: FlowContent::Unavailable(String::new()) })] }
    }

    /// Review 000693 r3: the log's write waiting holds neither the forced flow's deliveries nor the runtime's only worker.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn a_log_write_waiting_holds_neither_the_frames_nor_the_runtime() {
        let (frames, mut received) = broadcast::channel(16);
        let (entered, writing) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel::<()>();
        let released = std::sync::Mutex::new(released);
        let write = move |spell: &StaleSpell| {
            drop(entered.send(spell.to_string()));
            released.lock().unwrap().recv().unwrap_or_default();
        };
        let mut sink = FlowSink::start_writing(FlowPublisher::new(frames), write).unwrap();
        for (at_ms, age_ms) in [(AT, 3_000), (AT + 1_000, 300)] {
            sink.deliver(aged_output(at_ms, age_ms), at_ms, None);
        }
        let line = tokio::task::spawn_blocking(move || writing.recv_timeout(Duration::from_secs(5))).await.unwrap().unwrap();
        assert!(line.contains("from 03:01:00.000 to 03:01:00.000"), "{line}");
        // the write waits: another task of the runtime runs (waited for off the worker, so a held worker fails here), and
        // the next frames go out
        let (ran, running) = std::sync::mpsc::channel();
        drop(tokio::spawn(async move { ran.send(()).unwrap_or_default() }));
        let ran = tokio::task::spawn_blocking(move || running.recv_timeout(Duration::from_millis(500))).await.unwrap();
        assert!(ran.is_ok(), "the runtime's only worker is held by the log");
        sink.deliver(aged_output(AT + 2_000, 300), AT + 2_000, None);
        assert_eq!((0..3).filter(|_| received.try_recv().is_ok()).count(), 3);
        release.send(()).unwrap();
    }

    #[test]
    fn a_full_spell_log_drops_the_line_and_the_frames_go_out() {
        let output = aged_output;
        let (frames, mut received) = broadcast::channel(16);
        // a queue of one nobody reads
        let (spells, mut lines) = mpsc::channel(1);
        let mut sink = FlowSink { publisher: FlowPublisher::new(frames), staleness: FlowStaleness::default(), spells };
        let dropped = || FORCED_FLOW_STALE_SPELLS_DROPPED_TOTAL.with_label_values(&["full"]).get();
        let before = dropped();
        // two spells, each ended by a frame back within a second after it
        for (at_ms, age_ms) in [(AT, 3_000), (AT + 1_000, 300), (AT + 2_000, 3_000), (AT + 3_000, 300)] {
            sink.deliver(output(at_ms, age_ms), at_ms, None);
        }
        assert_eq!(dropped() - before, 1);
        assert_eq!((0..4).filter(|_| received.try_recv().is_ok()).count(), 4);
        assert!(lines.try_recv().is_ok_and(|spell| spell.to_string().contains("from 03:01:00.000 to 03:01:00.000")));
        assert!(lines.try_recv().is_err());
    }

    /// Review 000563 r1 test 4: the account owner busy in a long synchronous step does not hold the forced flow - a
    /// query is answered while it runs - and its end withdraws the forced flow and ends the thread.
    #[tokio::test]
    async fn the_forced_flow_answers_while_the_owner_is_busy_and_ends_with_it() {
        let book = replay_book(1_000, 1_000, &[(checkpoint_user(HOLDER), 1_000_000)]);
        let marks = ListedMarks { height: 1_000, marks: vec![(PUMP, 12_500)] };
        let mut candidates = Candidates::new(None);
        drop(candidates.refresh(&book, Some(&marks), &[]));
        let (mut feed, accounts, events) = link(CopyOwner::ForcedFlow);
        feed.on_applied(&book, Some(&marks), &Touched::default(), &candidates);
        let (frames, mut received) = broadcast::channel(16);
        let (queries, thread) = spawn_forced_flow(None, FlowDemand::default(), accounts, events, FlowPublisher::new(frames)).unwrap();
        // the owner's thread holds the feed and is busy for a second
        let owner = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(1));
            feed
        });
        let started = Instant::now();
        let (reply, answer) = oneshot::channel();
        queries.send(FlowQuery::Frames { coin: Some(PUMP.to_string()), tier: Tier::Near, reply }).await.unwrap();
        let answered = answer.await.unwrap().unwrap();
        assert!(started.elapsed() < Duration::from_millis(500), "answered while the owner is busy: {:?}", started.elapsed());
        assert_eq!(answered[0].content, FlowContent::Unavailable("the server runs without an order book".to_string()));
        drop(owner.join().unwrap());
        thread.await.unwrap();
        let withdrawn = loop {
            if let AccountsFrame::ForcedFlowWithdrawn { reason } = &*received.recv().await.unwrap() {
                break reason.clone();
            }
        };
        assert_eq!(withdrawn, OWNER_ENDED);
    }

    /// Review 000566 r1: the loop withdraws every frame when the copy serves a new generation without having been seen
    /// unavailable between (its snapshot handled before the watch's notice), and when the candidates alone are
    /// withdrawn - once each.
    #[test]
    fn frames_are_withdrawn_when_the_copy_changes_generation_or_loses_its_candidates() {
        let book = replay_book(1_000, 1_000, &[(checkpoint_user(HOLDER), 1_000_000)]);
        let marks = ListedMarks { height: 1_000, marks: vec![(PUMP, 12_500)] };
        let mut candidates = Candidates::new(None);
        drop(candidates.refresh(&book, Some(&marks), &[]));
        let (mut accounts, validity) = serving(&book, &marks, &candidates);
        let mut served = Served::default();
        assert_eq!((served.after_step(&accounts), served.after_step(&accounts)), (None, None), "served, then the same generation");
        accounts.apply(FlowAccountsEvent::Snapshot(Box::new(AccountsSnapshot::of(2, &book, &marks, &candidates).unwrap())));
        validity.send_replace(FlowAccountsValidity { generation: 2, state: Ok(()) });
        assert_eq!(served.after_step(&accounts), Some("the account view was sent anew (generation 2)".to_string()));
        accounts.apply(FlowAccountsEvent::CandidatesWithdrawn { generation: 2, reason: "the book walk task ended".to_string() });
        assert_eq!(served.after_step(&accounts), Some("the book walk task ended".to_string()));
        assert_eq!(served.after_step(&accounts), None, "withdrawn once");
    }

    /// Design 000591: a fire of the coin's held trigger orders starts its near key without waiting for the floor or a
    /// tick - 7 ms after a revision's start, the triggered statuses coming last in their block.
    #[tokio::test(start_paused = true)]
    async fn a_trigger_fire_starts_a_near_key_at_once() {
        let harness = Harness::start();
        let first = harness.started().await.expect("the first tick");
        harness.end(first).await;
        tokio::time::advance(TICK).await;
        assert_eq!(harness.started().await, None);
        tokio::time::advance(Duration::from_millis(10)).await;
        harness.raise(1);
        let revised = harness.started().await.expect("started on the revision at 60 ms");
        harness.end(revised).await;
        tokio::time::advance(Duration::from_millis(7)).await;
        harness.fire();
        assert!(harness.started().await.is_some(), "the fire at 67 ms, within the floor");
        harness.thread.abort();
    }
}
