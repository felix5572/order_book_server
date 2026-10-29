//! The forced flow's own thread (hyperliquid-trade review 000563 r1–r2): its pipelines (`ForcedFlow`), the flow
//! reader's reads, the local marks, its queries and the account owner's events, apart from the account owner, so that
//! a long synchronous step there (a candidates refresh, an anchor install) no longer holds a frame. The frames are
//! computed from the copy of the account view (`flow_accounts`) only while it serves; the copy stopping serving
//! withdraws every frame at once, and the owner's end ends the thread.

use std::{sync::Arc, time::Instant};

use log::{error, info};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::MissedTickBehavior,
};

use super::{
    flow_accounts::{AccountsCopy, FlowAccounts, FlowAccountsEvent, OWNER_ENDED},
    forced_flow::Tier,
    forced_flow_owner::{FlowDemand, FlowFrame, FlowLink, FlowOutput, ForcedFlow},
    forced_flow_reader::{FlowMailbox, FlowRead, run_flow_reader},
    owner_thread::spawn_owner,
    publish::FlowPublisher,
    runtime::BookSource,
};
use crate::{local_marks::snapshot::LocalMarksSnapshot, metrics::FORCED_FLOW_OWNER_SECONDS};

/// Reads in flight back to the forced flow: at most one per (coin, tier, kind) pending.
const FLOW_READS_CAPACITY: usize = 1_024;
/// Queries waiting for the forced flow thread.
const FLOW_QUERIES_CAPACITY: usize = 64;

/// What the WS connections ask the forced flow thread.
pub(crate) enum FlowQuery {
    /// The coin's, or every coin's, latest frames of the tier, computed first when stale (design 000451 r5).
    Frames { coin: Option<String>, tier: Tier, reply: oneshot::Sender<Result<Vec<Arc<FlowFrame>>, String>> },
    /// The local marks' snapshots, for the forced flow's local calibre.
    AttachLocalMarks(watch::Receiver<Arc<LocalMarksSnapshot>>),
}

/// The forced flow and its reads coming back (design 000451); `reads` is `None` without a book.
struct FlowFeed {
    flow: ForcedFlow,
    reads: Option<mpsc::Receiver<FlowRead>>,
}

impl FlowFeed {
    /// With `book`, the flow reader task starts here.
    fn start(book: Option<&BookSource>, demand: FlowDemand) -> Self {
        let Some(BookSource { trust, listener }) = book else {
            return Self { flow: ForcedFlow::new(demand, None), reads: None };
        };
        let mailbox = Arc::new(FlowMailbox::default());
        let (results, reads) = mpsc::channel(FLOW_READS_CAPACITY);
        tokio::spawn(run_flow_reader(Arc::clone(listener), Arc::clone(&mailbox), results));
        Self { flow: ForcedFlow::new(demand, Some(FlowLink { trust: trust.clone(), mailbox })), reads: Some(reads) }
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
    let thread = spawn_owner("forced-flow", run_flow(feed, accounts, events, queries, publisher))?;
    Ok((queries_tx, thread))
}

async fn run_flow(
    feed: FlowFeed,
    mut accounts: FlowAccounts,
    mut events: mpsc::Receiver<FlowAccountsEvent>,
    mut queries: mpsc::Receiver<FlowQuery>,
    publisher: FlowPublisher,
) {
    let FlowFeed { mut flow, reads: mut flow_reads } = feed;
    let mut tick = tokio::time::interval(Tier::Near.period());
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut served = Served::default();
    loop {
        tokio::select! {
            changed = accounts.validity_changed() => {
                if changed.is_err() {
                    return end(&mut flow, &publisher);
                }
            }
            event = events.recv() => {
                let Some(event) = event else { return end(&mut flow, &publisher) };
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
                        publisher.publish(time_flow_step("query", || flow.query(coin, tier, reply, Instant::now(), &accounts)));
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
                    publisher.withdraw(reason);
                    continue;
                };
                publisher.publish(time_flow_step("read", || flow.on_read(read, Instant::now(), &accounts)));
            }
            _ = tick.tick() => {
                publisher.publish(time_flow_step("tick", || flow.tick(Instant::now(), &accounts)));
            }
            closed = next_local_snapshot(&mut flow) => {
                publisher.publish(time_flow_step("local", || flow.on_local(closed, Instant::now(), &accounts)));
            }
        }
        if let Some(reason) = served.after_step(&accounts) {
            publisher.publish(time_flow_step("accounts", || flow.withdraw_accounts(&reason, Instant::now(), &accounts)));
        }
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
        flow_accounts::{AccountsSnapshot, FlowAccountsValidity, link, tests::serving},
        forced_flow_owner::FlowContent,
        publish::AccountsFrame,
        test_support::{ListedMarks, PUMP, replay_book},
    };

    /// Review 000563 r1 test 4: the account owner busy in a long synchronous step does not hold the forced flow - a
    /// query is answered while it runs - and its end withdraws the forced flow and ends the thread.
    #[tokio::test]
    async fn the_forced_flow_answers_while_the_owner_is_busy_and_ends_with_it() {
        let book = replay_book(1_000, 1_000, &[(checkpoint_user(HOLDER), 1_000_000)]);
        let marks = ListedMarks { height: 1_000, marks: vec![(PUMP, 12_500)] };
        let mut candidates = Candidates::new(None);
        drop(candidates.refresh(&book, Some(&marks), &[]));
        let (mut feed, accounts, events) = link();
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
}
