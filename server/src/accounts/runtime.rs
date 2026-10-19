//! Process wiring of the account view (design 000419 r2 3.1 / 3.4): the fills and misc events line watchers, the
//! checkpoint poller thread and the owner task holding the `AccountBook`. Every input reaches the owner through a
//! bounded channel whose full state blocks the producer, so a backlog stays in the node's files. Lines are parsed on
//! the watcher threads; the owner only applies them. A finished watcher or poller stops the view for good (503 until
//! restart); a gone owner answers every query with the same.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use log::{error, info, warn};
use serde::{Deserialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc, oneshot};

use super::{
    anchor::Anchor,
    book::{
        AccountBook, BookStatus, CoinPositions, FillsReport, InstallReport, MiscReport, QueryError, Stream, Touched,
        Unavailable,
    },
    checkpoint::parse_checkpoint_file,
    misc::MiscEvent,
    publish::{AccountsFrame, CoinSubscriptionGuard, CoinSubscriptions, FRAME_CHANNEL_CAPACITY, Publisher},
};
use crate::{
    listeners::order_book::{
        parallel::{LineStream, spawn_line_watcher},
        utils::list_checkpoints,
    },
    metrics::{
        ACCOUNTS_ANCHOR_HEIGHT, ACCOUNTS_ANCHOR_PARSE_SECONDS, ACCOUNTS_ANCHOR_SKIPPED_TOTAL,
        ACCOUNTS_CHECKPOINT_FAILURES_TOTAL, ACCOUNTS_DATA_LOSS_TOTAL, ACCOUNTS_HELD_FILLS, ACCOUNTS_OPEN_POSITIONS,
        ACCOUNTS_SILENT_POSITION_CHANGES_TOTAL, ACCOUNTS_STALE_FILL_BATCHES_SKIPPED_TOTAL, ACCOUNTS_STREAM_HEIGHT,
        ACCOUNTS_TRUSTED, ACCOUNTS_UNKNOWN_MISC_EVENTS_TOTAL,
    },
    prelude::Result,
    types::node_data::{Batch, NodeDataFill},
};

/// Same bound as the book's watchers. A Funding line (one per dex per hour) is the largest item: ~300k users.
const INPUT_CHANNEL_CAPACITY: usize = 10_000;
/// One Funding line of the main dex measured 42 MB (2026-10-19), above the book streams' 16 MiB cap.
const MISC_EVENTS_MAX_LINE_BYTES: usize = 256 * 1024 * 1024;
const FILLS_MAX_LINE_BYTES: usize = 16 * 1024 * 1024;
/// Checkpoints appear complete (a new file has its final size at first sight, 2026-10-19), every ~12 minutes.
const CHECKPOINT_POLL_INTERVAL: Duration = Duration::from_secs(1);
const SUPERVISION_INTERVAL: Duration = Duration::from_secs(1);
const OWNER_GONE: &str = "account owner task exited";
/// How much of an unparsable line goes into the error.
const LINE_PREFIX_CHARS: usize = 300;

enum Input {
    Fills(std::result::Result<Batch<NodeDataFill>, LineError>),
    MiscEvents(std::result::Result<Batch<MiscEvent>, LineError>),
    /// The watcher did not deliver lines of unknown blocks: the skip while positioning at its start, or a discard.
    Loss(Stream),
}

/// An unparsable line; `block` when its header still reads (the body broke the schema).
#[derive(Debug)]
struct LineError {
    block: Option<u64>,
    error: String,
}

#[derive(Deserialize)]
struct LineHeader {
    block_number: u64,
}

enum AnchorMessage {
    Parsed { anchor: Anchor, parse_seconds: f64, skipped: usize },
    Failed { height: u64, error: String },
}

enum Query {
    CoinPositions { coin: String, reply: oneshot::Sender<std::result::Result<CoinPositions, QueryError>> },
    Status { reply: oneshot::Sender<BookStatus> },
}

/// Queries the owner task and joins its `coinPositions` push.
#[derive(Clone)]
pub(crate) struct AccountsHandle {
    queries: mpsc::Sender<Query>,
    subscriptions: CoinSubscriptions,
    /// Weak: only the owner's publisher keeps the broadcast open, so its exit closes every receiver (review 000423 P2).
    frames: broadcast::WeakSender<Arc<AccountsFrame>>,
}

impl AccountsHandle {
    /// Every frame the owner broadcasts from now on; `None` once the owner is gone.
    pub(crate) fn frames(&self) -> Option<broadcast::Receiver<Arc<AccountsFrame>>> {
        self.frames.upgrade().map(|frames| frames.subscribe())
    }

    /// Counts a subscriber of `coin`, so the owner builds its frames; take it before asking for the snapshot.
    pub(crate) fn acquire(&self, coin: &str) -> CoinSubscriptionGuard {
        self.subscriptions.acquire(coin)
    }

    pub(crate) async fn coin_positions(&self, coin: &str) -> std::result::Result<CoinPositions, QueryError> {
        let (reply, answer) = oneshot::channel();
        let gone = || QueryError::Unavailable(Unavailable::Stopped(OWNER_GONE.to_string()));
        self.queries.send(Query::CoinPositions { coin: coin.to_string(), reply }).await.map_err(|_| gone())?;
        answer.await.map_err(|_| gone())?
    }

    pub(crate) async fn status(&self) -> std::result::Result<BookStatus, Unavailable> {
        let (reply, answer) = oneshot::channel();
        let gone = || Unavailable::Stopped(OWNER_GONE.to_string());
        self.queries.send(Query::Status { reply }).await.map_err(|_| gone())?;
        answer.await.map_err(|_| gone())
    }
}

/// A thread the owner supervises.
struct Supervised {
    name: &'static str,
    handle: thread::JoinHandle<()>,
}

/// Starts the watchers, the poller and the owner task. Fails when the checkpoint directory cannot be listed.
pub(crate) fn start_accounts(data_dir: &Path) -> Result<(AccountsHandle, tokio::task::JoinHandle<()>)> {
    // checkpoints already on disk predate the watchers: the first anchor is the next one (design r2 3.5)
    let start_height = list_checkpoints(data_dir)?.last().map_or(0, |checkpoint| checkpoint.height);
    let (input_tx, inputs) = mpsc::channel(INPUT_CHANNEL_CAPACITY);
    let (anchor_tx, anchors) = mpsc::channel(1);
    let (queries_tx, queries) = mpsc::channel(64);
    let subscriptions = CoinSubscriptions::default();
    let (frames, _) = broadcast::channel(FRAME_CHANNEL_CAPACITY);
    let fills = LineStream {
        name: "AccountFills",
        lag_label: "accounts_fills",
        dir: data_dir.join("node_fills_streaming"),
        max_line_bytes: FILLS_MAX_LINE_BYTES,
        start_skip_is_loss: true,
    };
    let misc_events = LineStream {
        name: "AccountMiscEvents",
        lag_label: "accounts_misc_events",
        dir: data_dir.join("misc_events_streaming"),
        max_line_bytes: MISC_EVENTS_MAX_LINE_BYTES,
        start_skip_is_loss: true,
    };
    let threads = vec![
        Supervised {
            name: "fills watcher",
            handle: spawn_line_watcher(
                fills,
                input_tx.clone(),
                None,
                |line| Input::Fills(parse_line(&line)),
                || Input::Loss(Stream::Fills),
            ),
        },
        Supervised {
            name: "misc events watcher",
            handle: spawn_line_watcher(
                misc_events,
                input_tx,
                None,
                |line| Input::MiscEvents(parse_line(&line)),
                || Input::Loss(Stream::MiscEvents),
            ),
        },
        Supervised {
            name: "checkpoint poller",
            handle: spawn_checkpoint_poller(data_dir.to_path_buf(), start_height, anchor_tx),
        },
    ];
    info!("accounts started; first anchor is the first checkpoint above {start_height}");
    let handle = AccountsHandle { queries: queries_tx, subscriptions: subscriptions.clone(), frames: frames.downgrade() };
    let publisher = Publisher::new(subscriptions, frames);
    let owner = tokio::spawn(run_owner(AccountBook::new(now_ms()), publisher, inputs, anchors, queries, threads));
    Ok((handle, owner))
}

fn parse_line<E: DeserializeOwned>(line: &str) -> std::result::Result<Batch<E>, LineError> {
    sonic_rs::from_str(line).map_err(|err| {
        let prefix: String = line.chars().take(LINE_PREFIX_CHARS).collect();
        LineError {
            block: sonic_rs::from_str::<LineHeader>(line).ok().map(|header| header.block_number),
            error: format!("{err} (line of {} bytes starting {prefix:?})", line.len()),
        }
    })
}

#[expect(clippy::expect_used, reason = "a host clock before 1970 or past u64 ms is broken; loss bounds depend on it")]
fn now_ms() -> u64 {
    let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH).expect("the host clock is after 1970");
    u64::try_from(since_epoch.as_millis()).expect("unix ms fit u64")
}

/// Parses only the newest checkpoint above the last one seen; the ones that appeared meanwhile are counted as skipped.
fn spawn_checkpoint_poller(
    data_dir: PathBuf,
    start_height: u64,
    anchors: mpsc::Sender<AnchorMessage>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut last_height = start_height;
        loop {
            thread::sleep(CHECKPOINT_POLL_INTERVAL);
            if anchors.is_closed() {
                return;
            }
            let checkpoints = match list_checkpoints(&data_dir) {
                Ok(checkpoints) => checkpoints,
                // a day directory pruned mid-listing; the next poll sees a consistent directory
                Err(err) => {
                    warn!("accounts: listing checkpoints failed, retrying: {err}");
                    continue;
                }
            };
            let Some(newest) = checkpoints.last().filter(|newest| newest.height > last_height) else {
                continue;
            };
            let skipped = checkpoints.iter().filter(|c| c.height > last_height && c.height < newest.height).count();
            last_height = newest.height;
            let started = Instant::now();
            let parsed = parse_checkpoint_file(&newest.path)
                .and_then(|checkpoint| Anchor::from_checkpoint(&checkpoint).map_err(Into::into));
            let message = match parsed {
                Ok(anchor) => AnchorMessage::Parsed { anchor, parse_seconds: started.elapsed().as_secs_f64(), skipped },
                Err(err) => AnchorMessage::Failed { height: newest.height, error: format!("{}: {err}", newest.path.display()) },
            };
            if anchors.blocking_send(message).is_err() {
                return;
            }
        }
    })
}

async fn run_owner(
    mut book: AccountBook,
    mut publisher: Publisher,
    mut inputs: mpsc::Receiver<Input>,
    mut anchors: mpsc::Receiver<AnchorMessage>,
    mut queries: mpsc::Receiver<Query>,
    threads: Vec<Supervised>,
) {
    let mut supervision = tokio::time::interval(SUPERVISION_INTERVAL);
    publisher.track(&book);
    loop {
        tokio::select! {
            Some(input) = inputs.recv() => {
                let touched = apply_input(&mut book, input);
                if publisher.track(&book) {
                    publisher.push_changes(&mut book, &touched);
                }
            }
            Some(message) = anchors.recv() => {
                let installed = apply_anchor(&mut book, message);
                if publisher.track(&book) && installed {
                    publisher.push_snapshots(&mut book);
                }
            }
            query = queries.recv() => {
                let Some(query) = query else {
                    info!("accounts: every handle dropped, owner exits");
                    return;
                };
                answer(&book, query);
            }
            _ = supervision.tick() => {
                supervise(&mut book, &threads);
                publisher.track(&book);
                export_status(&book.status());
            }
        }
    }
}

/// Applies one input; returns the rows it changed.
fn apply_input(book: &mut AccountBook, input: Input) -> Touched {
    match input {
        Input::Fills(Ok(batch)) => report_fills(book.apply_fills(batch)),
        Input::MiscEvents(Ok(batch)) => report_misc(book.apply_misc_events(batch)),
        Input::Fills(Err(err)) => lost(book, Stream::Fills, "parse_error", &err),
        Input::MiscEvents(Err(err)) => lost(book, Stream::MiscEvents, "parse_error", &err),
        Input::Loss(stream) => {
            let error = "the watcher skipped lines while positioning at its start, or discarded buffered data".to_string();
            lost(book, stream, "watcher_discard", &LineError { block: None, error })
        }
    }
}

fn lost(book: &mut AccountBook, stream: Stream, reason: &str, err: &LineError) -> Touched {
    let at = err.block.map_or_else(|| "of unknown blocks".to_string(), |block| format!("of block {block}"));
    error!("accounts {} data loss ({reason}) {at}: {}", stream.label(), err.error);
    ACCOUNTS_DATA_LOSS_TOTAL.with_label_values(&[stream.label(), reason]).inc();
    match err.block {
        Some(block) => book.record_lost_block(stream, block),
        None => book.record_unknown_loss(stream, now_ms()),
    }
    Touched::default()
}

fn report_fills(report: FillsReport) -> Touched {
    if report.stale {
        ACCOUNTS_STALE_FILL_BATCHES_SKIPPED_TOTAL.inc();
    }
    if report.hold_overflowed {
        error!("accounts fills data loss (hold_overflow): the hold of fills outside the anchor universe overflowed");
        ACCOUNTS_DATA_LOSS_TOTAL.with_label_values(&[Stream::Fills.label(), "hold_overflow"]).inc();
    }
    for err in &report.errors {
        error!("accounts fills data loss (size_contract): {err}");
        ACCOUNTS_DATA_LOSS_TOTAL.with_label_values(&[Stream::Fills.label(), "size_contract"]).inc();
    }
    report_dropped_marks(report.marks_dropped);
    report.touched
}

/// Oldest change-mark events dropped for room: hours without an anchor (losses up to their blocks are recorded).
fn report_dropped_marks(dropped: usize) {
    if dropped > 0 {
        error!("accounts data loss (mark_log_overflow): {dropped} oldest change-mark events dropped");
        ACCOUNTS_DATA_LOSS_TOTAL.with_label_values(&["marks", "mark_log_overflow"]).inc_by(dropped as u64);
    }
}

fn report_misc(report: MiscReport) -> Touched {
    ACCOUNTS_UNKNOWN_MISC_EVENTS_TOTAL.inc_by(report.unknown_events as u64);
    for kind in &report.new_unknown_kinds {
        warn!("accounts: unknown misc event kind {kind:?} (skipped; counted from now on)");
    }
    report_dropped_marks(report.marks_dropped);
    report.touched
}

/// Applies a poller message; returns whether an anchor was installed.
fn apply_anchor(book: &mut AccountBook, message: AnchorMessage) -> bool {
    match message {
        AnchorMessage::Parsed { anchor, parse_seconds, skipped } => {
            let height = anchor.height;
            let report = book.install_anchor(anchor);
            log_install(height, parse_seconds, skipped, &report);
            ACCOUNTS_ANCHOR_HEIGHT.set(height.cast_signed());
            ACCOUNTS_ANCHOR_PARSE_SECONDS.set(parse_seconds);
            ACCOUNTS_ANCHOR_SKIPPED_TOTAL.inc_by(skipped as u64);
            ACCOUNTS_SILENT_POSITION_CHANGES_TOTAL.inc_by(report.silent_position_changes);
            true
        }
        AnchorMessage::Failed { height, error } => {
            error!("accounts: checkpoint {height} failed to parse, withholding the view: {error}");
            ACCOUNTS_CHECKPOINT_FAILURES_TOTAL.inc();
            book.record_checkpoint_failure(height, error);
            false
        }
    }
}

fn log_install(height: u64, parse_seconds: f64, skipped: usize, report: &InstallReport) {
    info!(
        "accounts: installed anchor {height} (parsed in {parse_seconds:.2}s, {skipped} skipped): {} silent position \
         changes, {} held fills covered, {} replayed, {} records of coins that left the universe",
        report.silent_position_changes, report.covered_held_fills, report.replayed_fills, report.dropped_coin_records
    );
    if report.dropped_coin_records > 0 {
        warn!("accounts: anchor {height} dropped {} records of coins no longer listed", report.dropped_coin_records);
    }
    for err in &report.merge_errors {
        error!("accounts fills data loss (anchor {height} merge): {err}");
        ACCOUNTS_DATA_LOSS_TOTAL.with_label_values(&[Stream::Fills.label(), "merge"]).inc();
    }
    for err in &report.replay_errors {
        error!("accounts fills data loss (size_contract, replayed at anchor {height}): {err}");
        ACCOUNTS_DATA_LOSS_TOTAL.with_label_values(&[Stream::Fills.label(), "size_contract"]).inc();
    }
}

fn answer(book: &AccountBook, query: Query) {
    // a dropped receiver is a caller that gave up; nothing to undo
    match query {
        Query::CoinPositions { coin, reply } => drop(reply.send(book.coin_positions(&coin))),
        Query::Status { reply } => drop(reply.send(book.status())),
    }
}

fn supervise(book: &mut AccountBook, threads: &[Supervised]) {
    if matches!(book.availability(), Err(Unavailable::Stopped(_))) {
        return;
    }
    if let Some(thread) = threads.iter().find(|thread| thread.handle.is_finished()) {
        error!("accounts: the {} thread exited; the view stays unavailable until restart", thread.name);
        book.stop(format!("{} thread exited", thread.name));
    }
}

fn export_status(status: &BookStatus) {
    ACCOUNTS_TRUSTED.set(i64::from(status.availability.is_ok()));
    ACCOUNTS_OPEN_POSITIONS.set((status.open_positions as u64).cast_signed());
    ACCOUNTS_HELD_FILLS.set((status.held_fills as u64).cast_signed());
    for (stream, height) in [(Stream::Fills, status.fills_height), (Stream::MiscEvents, status.misc_events_height)] {
        if let Some(height) = height {
            ACCOUNTS_STREAM_HEIGHT.with_label_values(&[stream.label()]).set(height.cast_signed());
        }
    }
}

/// An owner without watchers or poller, fed directly by the test.
#[cfg(test)]
pub(crate) struct TestOwner {
    pub(crate) handle: AccountsHandle,
    inputs: mpsc::Sender<Input>,
    anchors: mpsc::Sender<AnchorMessage>,
    owner: tokio::task::JoinHandle<()>,
}

#[cfg(test)]
impl TestOwner {
    /// Spawns the owner on the current tokio runtime.
    pub(crate) fn start(book: AccountBook) -> Self {
        let (inputs, input_rx) = mpsc::channel(16);
        let (anchors, anchor_rx) = mpsc::channel(1);
        let (queries, query_rx) = mpsc::channel(16);
        let subscriptions = CoinSubscriptions::default();
        let (frames, _) = broadcast::channel(FRAME_CHANNEL_CAPACITY);
        let handle = AccountsHandle { queries, subscriptions: subscriptions.clone(), frames: frames.downgrade() };
        let owner = tokio::spawn(run_owner(book, Publisher::new(subscriptions, frames), input_rx, anchor_rx, query_rx, Vec::new()));
        Self { handle, inputs, anchors, owner }
    }

    /// Ends the owner task (as a panic would) and waits until it is gone.
    pub(crate) async fn kill(&self) {
        self.owner.abort();
        while !self.owner.is_finished() {
            tokio::task::yield_now().await;
        }
    }

    pub(crate) async fn fills_line(&self, line: &str) {
        self.inputs.send(Input::Fills(parse_line(line))).await.unwrap();
    }

    pub(crate) async fn loss(&self, stream: Stream) {
        self.inputs.send(Input::Loss(stream)).await.unwrap();
    }

    pub(crate) async fn anchor(&self, anchor: Anchor) {
        self.anchors.send(AnchorMessage::Parsed { anchor, parse_seconds: 0.0, skipped: 0 }).await.unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use rustc_hash::FxHashMap;

    use super::*;
    use crate::accounts::{
        anchor::AnchorCoin,
        book::{CLOCK_MARGIN_MS, LossBound},
    };

    const USER: &str = "0x5b51db9432bc4dd2c30cadc4ab6ab44565419c9a";

    fn batch_line(block: u64, events: &str) -> String {
        format!(
            "{{\"local_time\":\"2026-10-19T06:00:00.1\",\"block_time\":\"2026-10-19T06:00:00.0\",\"block_number\":{block},\"events\":[{events}]}}\n"
        )
    }

    fn ledger_line(block: u64) -> String {
        let event = format!(
            r#"{{"time":"2026-10-19T06:00:00.0","hash":"0x00","inner":{{"LedgerUpdate":{{"users":["{USER}"],"delta":{{}}}}}}}}"#
        );
        batch_line(block, &event)
    }

    fn append(path: &Path, line: &str) {
        std::fs::OpenOptions::new().append(true).open(path).unwrap().write_all(line.as_bytes()).unwrap();
    }

    fn recv_within(inputs: &mut mpsc::Receiver<Input>, wait: Duration) -> Input {
        let deadline = Instant::now() + wait;
        loop {
            match inputs.try_recv() {
                Ok(input) => return input,
                Err(mpsc::error::TryRecvError::Empty) if Instant::now() < deadline => thread::sleep(Duration::from_millis(5)),
                Err(err) => panic!("no watcher input within {wait:?}: {err}"),
            }
        }
    }

    fn empty_anchor(height: u64, time_ms: u64) -> Anchor {
        let coin = AnchorCoin { size_decimals: 0, positions: FxHashMap::default() };
        Anchor { height, time_ms, coins: std::iter::once(("PUMP".to_string(), coin)).collect(), account_modes: FxHashMap::default() }
    }

    /// Review 000422 r2 P2, across the real watcher and the owner's input path: the content skipped while the watcher
    /// positions at EOF (here the line that triggered it) is a loss of unknown blocks; an anchor older than the moment
    /// the owner learns of it cannot clear it, however many later lines flow.
    #[test]
    fn lines_skipped_while_positioning_withhold_the_view() {
        let dir = std::env::temp_dir().join(format!("accounts_start_skip_test_{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("10");
        std::fs::write(&file, ledger_line(1_100)).unwrap();
        let (tx, mut inputs) = mpsc::channel(16);
        let stream = LineStream { name: "TestMisc", lag_label: "test", dir, max_line_bytes: 1 << 20, start_skip_is_loss: true };
        let _watcher = spawn_line_watcher(
            stream,
            tx,
            None,
            |line| Input::MiscEvents(parse_line(&line)),
            || Input::Loss(Stream::MiscEvents),
        );
        // let the watcher register its inotify watch before the first append
        thread::sleep(Duration::from_millis(200));
        let mut book = AccountBook::new(0);
        apply_input(&mut book, Input::Fills(parse_line(&batch_line(1_200, ""))));
        book.install_anchor(empty_anchor(1_300, now_ms()));

        append(&file, &ledger_line(1_500));
        let skipped = recv_within(&mut inputs, Duration::from_secs(2));
        assert!(matches!(skipped, Input::Loss(Stream::MiscEvents)), "the positioning skip is reported");
        apply_input(&mut book, skipped);
        append(&file, &ledger_line(1_600));
        let next = recv_within(&mut inputs, Duration::from_secs(2));
        assert!(matches!(next, Input::MiscEvents(Ok(_))), "lines after the skip flow");
        apply_input(&mut book, next);
        assert!(matches!(
            book.availability(),
            Err(Unavailable::DataLoss { stream: Stream::MiscEvents, bound: LossBound::WrittenBy(_) })
        ));
        book.install_anchor(empty_anchor(1_700, now_ms() + CLOCK_MARGIN_MS));
        assert!(book.availability().is_ok());
    }

    #[test]
    fn an_unparsable_line_keeps_its_block_when_the_header_reads() {
        let schema_broken = r#"{"local_time":"2026-10-19T06:00:00.1","block_time":"2026-10-19T06:00:00.0","block_number":1500,"events":[["0x01",{"coin":"PUMP","fee":null}]]}"#;
        let err = parse_line::<NodeDataFill>(schema_broken).expect_err("the body breaks the fill schema");
        assert_eq!(err.block, Some(1_500));
        let truncated = r#"{"local_time":"2026-10-19T06:00:00.1","block_time":"2026-10-19T06:00:00.0","block_number":1500,"events":[["0x"#;
        assert_eq!(parse_line::<NodeDataFill>(truncated).expect_err("not JSON").block, None);
        assert!(parse_line::<MiscEvent>("{}").expect_err("no header").block.is_none());
    }

    /// Review 000423 P2: a real owner exit closes the push for every receiver (handles only hold a weak sender) and
    /// fails every later query.
    #[tokio::test]
    async fn the_owner_exit_closes_the_push_and_fails_queries() {
        use tokio::sync::broadcast::error::RecvError;

        use crate::accounts::test_support::{PUMP, trusted_book, user};
        let owner = TestOwner::start(trusted_book(&[(user(1), 5)]));
        let mut frames = owner.handle.frames().expect("the owner runs");
        assert!(owner.handle.coin_positions(PUMP).await.is_ok());
        owner.kill().await;
        let closed = tokio::time::timeout(Duration::from_secs(2), frames.recv()).await.expect("closed, not pending");
        assert!(matches!(closed, Err(RecvError::Closed)), "{closed:?}");
        let gone = QueryError::Unavailable(Unavailable::Stopped(OWNER_GONE.to_string()));
        assert_eq!(owner.handle.coin_positions(PUMP).await, Err(gone));
        assert!(owner.handle.frames().is_none(), "no receiver from a gone owner");
    }

    /// Review 000422 P3-1: the poller stops at its next poll once the owner is gone, not after the next checkpoint.
    #[test]
    fn the_checkpoint_poller_exits_once_the_owner_is_gone() {
        let data_dir = std::env::temp_dir().join(format!("accounts_poller_test_{}", std::process::id()));
        std::fs::create_dir_all(data_dir.join("periodic_abci_states")).unwrap();
        let (tx, rx) = mpsc::channel(1);
        let handle = spawn_checkpoint_poller(data_dir, 0, tx);
        drop(rx);
        let deadline = Instant::now() + CHECKPOINT_POLL_INTERVAL * 3;
        while !handle.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(handle.is_finished(), "the poller should notice the closed channel at its next poll");
    }
}
