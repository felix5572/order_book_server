//! Process wiring of the account view (design 000419 r2 3.1 / 3.4, 000429 r2, 000433 r3) and the node marks (design
//! 000439 r2): the fills, misc events, HIP-3 oracle updates and block actions line watchers, the checkpoint poller
//! thread and the owner task holding the `AccountBook` and the `NodeMarks`. Every input reaches the owner through a
//! bounded channel whose full state blocks the producer, so a backlog stays in the node's files. Lines are parsed on the
//! watcher threads; the owner hands them to the block assembly and applies the complete blocks it releases, in height
//! order, to the marks and then to the account view. The other watchers also send read-through proofs, in line with
//! their data; block actions lines carry the instant they were read. Each loss goes to both owners, which keep the
//! streams they read. A finished watcher or poller stops the view for good (503 until restart); a gone owner answers
//! every query with the same.

use alloy::primitives::Address;
use std::{
    hash::{DefaultHasher, Hash, Hasher},
    path::{Path, PathBuf},
    sync::Arc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use log::{error, info, warn};
use serde::{Deserialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc, oneshot};

use super::{
    actions::{BlockActions, block_actions},
    replica::{ReplicaLine, line_height},
    anchor::Anchor,
    blocks::{Assembly, BlockAssembler, CompletedBlock, LineKey},
    book::{
        AccountBook, BlockActionsReport, BlockReport, BookStatus, CoinPositions, FillsReport, InstallReport, MiscReport,
        QueryError, ReplayCheck, RowCheck, Stream, Touched, Unavailable,
    },
    checkpoint::parse_checkpoint_file,
    marks::{
        CheckpointGrid, MARK_STREAMS, MarkInputs, MarksBlockReport, MarksCheck, MarksInstallReport, MarksStatus, NodeMarks, Rebuild,
        actions::MarkActions, execution::BlockFill, seed::MarkSeed,
    },
    misc::MiscEvent,
    evidence::{
        LineEvidence, ambiguous_cut_json, book_mismatch_json, fill_line_reason, hip3_mark_mismatch_json, misc_line_reason,
        price_mismatch_json, replay_mismatch_json, replay_row_mismatch_json, silent_change_json,
    },
    publish::{AccountsFrame, CoinSubscriptionGuard, CoinSubscriptions, FRAME_CHANNEL_CAPACITY, Publisher},
    candidates::{BookLink, Candidates, CandidatesSnapshot},
    walk::{WalkMailbox, WalkResult, run_walker},
};
use crate::{
    listeners::order_book::{
        BookTrust, OrderBookListener,
        parallel::{FileLayout, LineStream, spawn_line_watcher},
        utils::{checkpoint_grid, list_checkpoints},
    },
    metrics::{
        ACCOUNTS_ANCHOR_HEIGHT, ACCOUNTS_ANCHOR_PARSE_SECONDS, ACCOUNTS_ANCHOR_SKIPPED_TOTAL, ACCOUNTS_EVIDENCE_TOTAL,
        ACCOUNTS_APPLIED_HEIGHT, ACCOUNTS_CHECKPOINT_FAILURES_TOTAL, ACCOUNTS_DATA_LOSS_TOTAL, ACCOUNTS_DUPLICATE_INPUTS_TOTAL,
        ACCOUNTS_HELD_FILLS, ACCOUNTS_OPEN_POSITIONS, ACCOUNTS_PENDING_BLOCKS, ACCOUNTS_REPLAY_CHECKED_TOTAL,
        ACCOUNTS_REPLAY_CHECKS_SKIPPED_TOTAL, ACCOUNTS_REPLAY_ROWS_CHECKED_TOTAL, ACCOUNTS_REPLAY_STOPPED_TOTAL, ACCOUNTS_SILENT_POSITION_CHANGES_TOTAL,
        ACCOUNTS_STALE_FILL_BATCHES_SKIPPED_TOTAL, ACCOUNTS_STREAM_HEIGHT, ACCOUNTS_TRUSTED, ACCOUNTS_UNKNOWN_MISC_EVENTS_TOTAL,
        LIQUIDATION_CANDIDATES, LIQUIDATION_CANDIDATES_REFRESH_SECONDS,
        NODE_MARKS_AVAILABLE, NODE_MARKS_CHECKED_TOTAL, NODE_MARKS_CHECKS_SKIPPED_TOTAL, NODE_MARKS_HEIGHT,
        NODE_MARKS_LOGGED_INPUTS, NODE_MARKS_REBUILDS_TOTAL, NODE_MARKS_SEED_FAILURES_TOTAL, NODE_MARKS_SEED_HEIGHT,
        NODE_MARKS_AMBIGUOUS_CUTS_TOTAL, NODE_MARKS_UNKNOWN_COIN_INPUTS_TOTAL,
    },
    prelude::Result,
    types::node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus, OracleUpdateEvent},
};

/// Same bound as the book's watchers. A Funding line (one per dex per hour) is the largest item: ~300k users.
const INPUT_CHANNEL_CAPACITY: usize = 10_000;
/// One Funding line of the main dex measured 42 MB (2026-10-19), above the book streams' 16 MiB cap.
const MISC_EVENTS_MAX_LINE_BYTES: usize = 256 * 1024 * 1024;
const FILLS_MAX_LINE_BYTES: usize = 16 * 1024 * 1024;
/// One deployer's update per line: ~44 coins on average, 4 price lists each (2026-10-20 11 UTC).
const HIP3_ORACLE_UPDATES_MAX_LINE_BYTES: usize = 16 * 1024 * 1024;
/// One diff per line: 277 bytes median, 318 at most over 43,224,015 lines (2026-10-20 11 UTC).
const BOOK_DIFFS_MAX_LINE_BYTES: usize = 1024 * 1024;
/// One status per line: 604 bytes median over 76,031,570 lines (2026-10-20 12 UTC).
const ORDER_STATUSES_MAX_LINE_BYTES: usize = 1024 * 1024;
/// The largest `replica_cmds` line measured 6.4 MB (bm, 2026-10-19).
const BLOCK_ACTIONS_MAX_LINE_BYTES: usize = 64 * 1024 * 1024;
/// Checkpoints appear complete (a new file has its final size at first sight, 2026-10-19), every ~12 minutes.
const CHECKPOINT_POLL_INTERVAL: Duration = Duration::from_secs(1);
const SUPERVISION_INTERVAL: Duration = Duration::from_secs(1);
const OWNER_GONE: &str = "account owner task exited";
/// How much of an unparsable line goes into the error.
const LINE_PREFIX_CHARS: usize = 300;

/// A parsed line, with the raw line kept when it is rare-event evidence.
enum Input {
    Fills(std::result::Result<Batch<NodeDataFill>, LineError>, Option<LineEvidence>),
    /// With the line's identity within its block.
    MiscEvents(std::result::Result<(Batch<MiscEvent>, LineKey), LineError>, Option<LineEvidence>),
    /// With the line's identity within its block.
    Hip3OracleUpdates(std::result::Result<(Batch<OracleUpdateEvent>, LineKey), LineError>),
    /// With the line's identity within its block.
    BookDiffs(std::result::Result<(Batch<NodeDataOrderDiff>, LineKey), LineError>),
    /// A status the node marks read, with the line's identity; `None` for any other line.
    OrderStatuses(std::result::Result<Option<(Batch<NodeDataOrderStatus>, LineKey)>, LineError>),
    /// The account view's and the node marks' reading of the line (the latter fails apart), with the instant it was read.
    BlockActions(std::result::Result<(BlockActions, std::result::Result<MarkActions, String>), LineError>, Instant),
    /// The stream's reader proved that every line written before the instant reached the owner (design 000433 r3).
    ReadThrough(Stream, Instant),
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
    /// The checkpoint's anchor and, checked apart, its marks' seed.
    Parsed { parsed: Box<ParsedAnchor>, seed: std::result::Result<Box<MarkSeed>, String> },
    Failed { height: u64, error: String },
}

/// A checkpoint parsed into an anchor, with how long that took and the older checkpoints it overtook.
struct ParsedAnchor {
    anchor: Anchor,
    parse_seconds: f64,
    skipped: usize,
}

enum Query {
    CoinPositions { coin: String, reply: oneshot::Sender<std::result::Result<CoinPositions, QueryError>> },
    Status { reply: oneshot::Sender<BookStatus> },
    Candidates { reply: oneshot::Sender<std::result::Result<CandidatesSnapshot, String>> },
}

/// Queries the owner task and joins its `extCoinPositions` push.
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

    /// Every published liquidation candidate; `Err` with the reason while they are not available.
    pub(crate) async fn liquidation_candidates(&self) -> std::result::Result<CandidatesSnapshot, String> {
        let (reply, answer) = oneshot::channel();
        self.queries.send(Query::Candidates { reply }).await.map_err(|_| OWNER_GONE.to_string())?;
        answer.await.map_err(|_| OWNER_GONE.to_string())?
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

/// Starts the watchers, the poller and the owner task. Fails when the checkpoint directory cannot be listed or holds
/// fewer than two checkpoints (the node marks take their grid from the two newest).
/// Walk results in flight to the owner: at most one per (coin, side) request pending, so a few hundred.
const WALK_RESULTS_CAPACITY: usize = 1_024;

/// The order book the liquidation candidates walk (design 000443): the listener and its trust mirror.
pub(crate) struct BookSource {
    pub(crate) trust: BookTrust,
    pub(crate) listener: Arc<tokio::sync::Mutex<OrderBookListener>>,
}

/// The liquidation candidates and the walk results coming back to the owner; `walks` is `None` without a book.
pub(crate) struct CandidateFeed {
    candidates: Candidates,
    walks: Option<mpsc::Receiver<WalkResult>>,
}

impl CandidateFeed {
    /// With `book`, the walk task starts here.
    fn start(book: Option<BookSource>) -> Self {
        let Some(BookSource { trust, listener }) = book else {
            return Self { candidates: Candidates::new(None), walks: None };
        };
        let mailbox = Arc::new(WalkMailbox::default());
        let (results, walks) = mpsc::channel(WALK_RESULTS_CAPACITY);
        tokio::spawn(run_walker(listener, Arc::clone(&mailbox), results));
        Self { candidates: Candidates::new(Some(BookLink { trust, mailbox })), walks: Some(walks) }
    }
}

/// The next walk result; pending forever without a book. Cancel-safe.
async fn next_walk(walks: &mut Option<mpsc::Receiver<WalkResult>>) -> Option<WalkResult> {
    match walks {
        Some(walks) => walks.recv().await,
        None => std::future::pending().await,
    }
}

/// Starts the watchers, the poller and the owner task, and with `book` the candidates' walk task. Fails when the
/// checkpoint directory cannot be listed or holds fewer than two checkpoints (the node marks take their grid from the
/// two newest).
pub(crate) fn start_accounts(data_dir: &Path, book: Option<BookSource>) -> Result<(AccountsHandle, tokio::task::JoinHandle<()>)> {
    let checkpoints = list_checkpoints(data_dir)?;
    let (anchor, interval) = checkpoint_grid(&checkpoints)?;
    // checkpoints already on disk predate the watchers: the first anchor is the next one (design r2 3.5)
    let start_height = anchor;
    let (input_tx, inputs) = mpsc::channel(INPUT_CHANNEL_CAPACITY);
    let (anchor_tx, anchors) = mpsc::channel(1);
    let (queries_tx, queries) = mpsc::channel(64);
    let subscriptions = CoinSubscriptions::default();
    let (frames, _) = broadcast::channel(FRAME_CHANNEL_CAPACITY);
    let fills = hourly("AccountFills", "accounts_fills", data_dir.join("node_fills_streaming"), FILLS_MAX_LINE_BYTES);
    let misc_events =
        hourly("AccountMiscEvents", "accounts_misc_events", data_dir.join("misc_events_streaming"), MISC_EVENTS_MAX_LINE_BYTES);
    let hip3_oracle_updates = hourly(
        "AccountHip3OracleUpdates",
        "accounts_hip3_oracle_updates",
        data_dir.join("hip3_oracle_updates_streaming"),
        HIP3_ORACLE_UPDATES_MAX_LINE_BYTES,
    );
    let book_diffs =
        hourly("AccountBookDiffs", "accounts_book_diffs", data_dir.join("node_raw_book_diffs_streaming"), BOOK_DIFFS_MAX_LINE_BYTES);
    let order_statuses = hourly(
        "AccountOrderStatuses",
        "accounts_order_statuses",
        data_dir.join("node_order_statuses_streaming"),
        ORDER_STATUSES_MAX_LINE_BYTES,
    );
    let block_actions = LineStream {
        name: "AccountBlockActions",
        lag_label: "accounts_block_actions",
        dir: data_dir.join("replica_cmds"),
        max_line_bytes: BLOCK_ACTIONS_MAX_LINE_BYTES,
        start_skip_is_loss: true,
        layout: FileLayout::ReplicaSessions,
    };
    let threads = vec![
        proving_watcher("fills watcher", fills, Stream::Fills, fills_input, input_tx.clone()),
        proving_watcher("misc events watcher", misc_events, Stream::MiscEvents, misc_input, input_tx.clone()),
        proving_watcher("HIP-3 oracle updates watcher", hip3_oracle_updates, Stream::Hip3OracleUpdates, hip3_input, input_tx.clone()),
        proving_watcher("book diffs watcher", book_diffs, Stream::BookDiffs, book_diffs_input, input_tx.clone()),
        proving_watcher("order statuses watcher", order_statuses, Stream::OrderStatuses, order_statuses_input, input_tx.clone()),
        Supervised {
            name: "block actions watcher",
            handle: spawn_line_watcher(
                block_actions,
                input_tx,
                None,
                block_actions_input,
                || Input::Loss(Stream::BlockActions),
                None,
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
    let started_ms = now_ms();
    let marks = NodeMarks::new(CheckpointGrid { anchor, interval }, started_ms);
    let feed = CandidateFeed::start(book);
    let owner = tokio::spawn(run_owner(AccountBook::new(started_ms), marks, publisher, inputs, anchors, queries, threads, feed));
    Ok((handle, owner))
}

/// An hourly stream whose start skip is a loss.
const fn hourly(name: &'static str, lag_label: &'static str, dir: PathBuf, max_line_bytes: usize) -> LineStream {
    LineStream { name, lag_label, dir, max_line_bytes, start_skip_is_loss: true, layout: FileLayout::Hourly }
}

/// The watcher of a stream whose read-throughs join the block assembly (every stream but `replica_cmds`).
fn proving_watcher(
    name: &'static str,
    lines: LineStream,
    stream: Stream,
    wrap: fn(String) -> Input,
    inputs: mpsc::Sender<Input>,
) -> Supervised {
    let read_through = Box::new(move |at| Input::ReadThrough(stream, at));
    Supervised { name, handle: spawn_line_watcher(lines, inputs, None, wrap, move || Input::Loss(stream), Some(read_through)) }
}

fn fills_input(line: String) -> Input {
    let parsed = parse_line(&line);
    let evidence = parsed.as_ref().ok().and_then(fill_line_reason).map(|reason| LineEvidence { reason, raw_line: line });
    Input::Fills(parsed, evidence)
}

fn misc_input(line: String) -> Input {
    let parsed = parse_line(&line).and_then(|batch| {
        let key = line_key(&line, batch.block_number())?;
        Ok((batch, key))
    });
    let evidence =
        parsed.as_ref().ok().and_then(|(batch, _)| misc_line_reason(batch)).map(|reason| LineEvidence { reason, raw_line: line });
    Input::MiscEvents(parsed, evidence)
}

fn hip3_input(line: String) -> Input {
    Input::Hip3OracleUpdates(parse_line(&line).and_then(|batch| {
        let key = line_key(&line, batch.block_number())?;
        Ok((batch, key))
    }))
}

/// Every market's diffs: the node marks keep the main dex's books, and every diff anchors the execution points.
fn book_diffs_input(line: String) -> Input {
    Input::BookDiffs(parse_line::<NodeDataOrderDiff>(&line).and_then(|batch| {
        let key = line_key(&line, batch.block_number())?;
        Ok((batch, key))
    }))
}

/// A status the marks read: a cancel of an order with a cloid (it resolves a `cancelByCloid`) or a trigger. One status
/// per line, ~21,000 a second, of which ~6,800 qualify (2026-10-20 12 UTC): the text decides before the parse.
fn order_statuses_input(line: String) -> Input {
    let wanted = line.contains(r#""status":"triggered""#) || (line.contains(r#""status":"canceled""#) && line.contains(r#""cloid":"0x"#));
    if !wanted {
        return Input::OrderStatuses(Ok(None));
    }
    Input::OrderStatuses(parse_line::<NodeDataOrderStatus>(&line).and_then(|batch| {
        let key = line_key(&line, batch.block_number())?;
        Ok(Some((batch, key)))
    }))
}

/// A misc events, HIP-3 oracle updates or book diffs line's identity within its block (`blocks::LineKey`): its text from
/// `"events":` on, without the node's `local_time`, which a replayed line writes anew. A line written otherwise has no
/// identity: an error.
fn line_key(line: &str, block: u64) -> std::result::Result<LineKey, LineError> {
    let Some(start) = line.find("\"events\":") else {
        return Err(line_error(line, &"no \"events\": text to take the line's identity from", Some(block)));
    };
    let mut hasher = DefaultHasher::new();
    line[start..].hash(&mut hasher);
    Ok(hasher.finish())
}

fn block_actions_input(line: String) -> Input {
    let read_at = Instant::now();
    let parsed = ReplicaLine::parse(&line).and_then(|replica| Ok((block_actions(&replica)?, MarkActions::of(&replica))));
    Input::BlockActions(parsed.map_err(|err| line_error(&line, &err, line_height(&line))), read_at)
}

fn parse_line<E: DeserializeOwned>(line: &str) -> std::result::Result<Batch<E>, LineError> {
    sonic_rs::from_str(line).map_err(|err| {
        line_error(line, &err, sonic_rs::from_str::<LineHeader>(line).ok().map(|header| header.block_number))
    })
}

fn line_error(line: &str, err: &dyn std::fmt::Display, block: Option<u64>) -> LineError {
    let prefix: String = line.chars().take(LINE_PREFIX_CHARS).collect();
    LineError { block, error: format!("{err} (line of {} bytes starting {prefix:?})", line.len()) }
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
            let parsed = parse_checkpoint_file(&newest.path).and_then(|checkpoint| {
                let anchor = Anchor::from_checkpoint(&checkpoint)?;
                let seed = MarkSeed::from_checkpoint(checkpoint).map(Box::new);
                Ok((anchor, seed))
            });
            let message = match parsed {
                Ok((anchor, seed)) => AnchorMessage::Parsed {
                    parsed: Box::new(ParsedAnchor { anchor, parse_seconds: started.elapsed().as_secs_f64(), skipped }),
                    seed: seed.map_err(|err| format!("{}: {err}", newest.path.display())),
                },
                Err(err) => AnchorMessage::Failed { height: newest.height, error: format!("{}: {err}", newest.path.display()) },
            };
            if anchors.blocking_send(message).is_err() {
                return;
            }
        }
    })
}

#[expect(clippy::too_many_arguments, reason = "the owner task's parts, moved in once at start")]
async fn run_owner(
    mut book: AccountBook,
    mut marks: NodeMarks,
    mut publisher: Publisher,
    mut inputs: mpsc::Receiver<Input>,
    mut anchors: mpsc::Receiver<AnchorMessage>,
    mut queries: mpsc::Receiver<Query>,
    threads: Vec<Supervised>,
    feed: CandidateFeed,
) {
    let CandidateFeed { mut candidates, mut walks } = feed;
    let mut supervision = tokio::time::interval(SUPERVISION_INTERVAL);
    let mut assembler = BlockAssembler::default();
    // a parsed anchor whose blocks are not applied yet (design 000433 r2 3.4), and a seed the marks are not ready for
    let mut held: Option<Box<ParsedAnchor>> = None;
    let mut held_seed: Option<Box<MarkSeed>> = None;
    publisher.track(&book);
    loop {
        tokio::select! {
            Some(input) = inputs.recv() => {
                let touched = apply_input(&mut book, &mut marks, &mut assembler, input);
                // a seed made ready by these blocks installs before anything is pushed: the frames carry the marks a
                // query would read (review 000445 r1 P2)
                install_seed_ready(&mut marks, &mut held_seed);
                if publisher.track(&book) {
                    publisher.push_changes(&mut book, &touched, marks.at());
                }
                if install_ready(&mut book, &mut held) && publisher.track(&book) {
                    publisher.push_snapshots(&mut book, marks.at());
                }
                publisher.track_marks(&mut book, &marks);
                refresh_candidates(&mut candidates, &book, &marks, &touched, &publisher);
            }
            Some(message) = anchors.recv() => {
                let installed = receive_anchor(&mut book, &mut marks, &mut held, &mut held_seed, message);
                if publisher.track(&book) && installed {
                    publisher.push_snapshots(&mut book, marks.at());
                }
                publisher.track_marks(&mut book, &marks);
                refresh_candidates(&mut candidates, &book, &marks, &Touched::default(), &publisher);
            }
            walk = next_walk(&mut walks) => {
                let Some(result) = walk else {
                    error!("liquidation candidates: the book walk task ended; the candidates stay unavailable until restart");
                    walks = None;
                    publisher.publish_candidates(candidates.stop("the book walk task ended".to_string()));
                    continue;
                };
                publisher.publish_candidates(candidates.apply_walk(result));
            }
            query = queries.recv() => {
                let Some(query) = query else {
                    info!("accounts: every handle dropped, owner exits");
                    return;
                };
                answer(&book, &marks, &candidates, query);
            }
            _ = supervision.tick() => {
                supervise(&mut book, &mut marks, &threads);
                publisher.track(&book);
                publisher.track_marks(&mut book, &marks);
                refresh_candidates(&mut candidates, &book, &marks, &Touched::default(), &publisher);
                export_status(&book.status());
                export_marks_status(&marks.status());
            }
        }
    }
}

/// Hands one input to the block assembly and applies the blocks it releases; returns the rows they changed.
fn apply_input(book: &mut AccountBook, marks: &mut NodeMarks, assembler: &mut BlockAssembler, input: Input) -> Touched {
    let assembly = match input {
        Input::Fills(Ok(batch), evidence) => {
            if let Some(evidence) = evidence {
                record_evidence(&evidence.json(Stream::Fills.label(), batch.block_number()));
            }
            assembler.receive_fills(batch)
        }
        Input::MiscEvents(Ok((batch, key)), evidence) => {
            if let Some(evidence) = evidence {
                record_evidence(&evidence.json(Stream::MiscEvents.label(), batch.block_number()));
            }
            assembler.receive_misc_events(batch, key)
        }
        Input::Hip3OracleUpdates(Ok((batch, key))) => assembler.receive_hip3_oracle_updates(batch, key),
        Input::BookDiffs(Ok((batch, key))) => assembler.receive_book_diffs(batch, key),
        Input::OrderStatuses(Ok(Some((batch, key)))) => assembler.receive_order_statuses(batch, key),
        Input::OrderStatuses(Ok(None)) => return Touched::default(),
        Input::BlockActions(Ok((block, mark_actions)), read_at) => assembler.receive_block_actions(block, mark_actions, read_at),
        Input::ReadThrough(stream, at) => assembler.receive_read_through(stream, at),
        Input::Fills(Err(err), _) => return lost(book, marks, Stream::Fills, "parse_error", &err),
        Input::MiscEvents(Err(err), _) => return lost(book, marks, Stream::MiscEvents, "parse_error", &err),
        Input::Hip3OracleUpdates(Err(err)) => return lost(book, marks, Stream::Hip3OracleUpdates, "parse_error", &err),
        Input::BookDiffs(Err(err)) => return lost(book, marks, Stream::BookDiffs, "parse_error", &err),
        Input::OrderStatuses(Err(err)) => return lost(book, marks, Stream::OrderStatuses, "parse_error", &err),
        Input::BlockActions(Err(err), _) => return lost(book, marks, Stream::BlockActions, "parse_error", &err),
        Input::Loss(stream) => {
            let error = "the watcher skipped lines while positioning at its start, or discarded buffered data".to_string();
            return lost(book, marks, stream, "watcher_discard", &LineError { block: None, error });
        }
    };
    apply_assembly(book, marks, assembler, assembly)
}

const ASSEMBLED_STREAMS: [Stream; 6] =
    [Stream::Fills, Stream::MiscEvents, Stream::Hip3OracleUpdates, Stream::BookDiffs, Stream::OrderStatuses, Stream::BlockActions];

/// Records what the assembly could not apply, then applies the released blocks in order: the marks first, so the
/// account view's block finds them at its height (design 000439 r1 4.1).
fn apply_assembly(book: &mut AccountBook, marks: &mut NodeMarks, assembler: &BlockAssembler, assembly: Assembly) -> Touched {
    let Assembly { completed, lost: dropped, duplicates, missing_actions, overflowed_to } = assembly;
    for (stream, count) in duplicates {
        ACCOUNTS_DUPLICATE_INPUTS_TOTAL.with_label_values(&[stream.label()]).inc_by(count as u64);
    }
    // one record per stream and input: a gap or an overflow drops many held blocks at once
    for stream in ASSEMBLED_STREAMS {
        let blocks: Vec<u64> = dropped.iter().filter(|(of, _)| *of == stream).map(|(_, block)| *block).collect();
        if let Some(highest) = blocks.iter().max() {
            let error = format!(
                "{} inputs up to block {highest} will not be applied: their block was released already (a node replay \
                 or a broken write order) or dropped with a gap or an overflow",
                blocks.len()
            );
            lost(book, marks, stream, "late_or_dropped", &LineError { block: Some(*highest), error });
        }
    }
    if let Some((first, last)) = missing_actions {
        error!("accounts block_actions data loss (height_gap): blocks {first} to {last} never arrived");
        ACCOUNTS_DATA_LOSS_TOTAL.with_label_values(&[Stream::BlockActions.label(), "height_gap"]).inc();
        record_lost_block(book, marks, Stream::BlockActions, last);
    }
    if let Some(block) = overflowed_to {
        error!("accounts data loss (pending_overflow): the pending blocks overflowed and were dropped up to {block}");
        for stream in ASSEMBLED_STREAMS {
            ACCOUNTS_DATA_LOSS_TOTAL.with_label_values(&[stream.label(), "pending_overflow"]).inc();
            record_lost_block(book, marks, stream, block);
        }
    }
    let mut touched = Touched::default();
    for block in completed {
        let CompletedBlock { actions, fills, misc_events, hip3_oracle_updates, book_diffs, mark_actions, order_statuses } = block;
        let height = actions.height;
        let mark_fills = fills.iter().flat_map(Batch::events_ref).map(BlockFill::of).collect();
        let inputs = MarkInputs { hip3_oracle_updates, book_diffs, mark_actions, order_statuses, fills: mark_fills };
        report_marks_block(height, marks.apply_block(height, inputs));
        touched.absorb(report_block(book.apply_block(actions, fills, misc_events)));
        ACCOUNTS_APPLIED_HEIGHT.set(height.cast_signed());
    }
    ACCOUNTS_PENDING_BLOCKS.set((assembler.pending_blocks() as u64).cast_signed());
    touched
}

fn report_block(report: BlockReport) -> Touched {
    let BlockReport { block_actions, fills, misc_events } = report;
    let mut touched = report_block_actions(block_actions);
    for report in fills {
        touched.absorb(report_fills(report));
    }
    for report in misc_events {
        touched.absorb(report_misc(report));
    }
    touched
}

/// One `ACCOUNTS_EVIDENCE` line (see `evidence.rs`).
fn record_evidence(evidence: &serde_json::Value) {
    warn!("ACCOUNTS_EVIDENCE {evidence}");
    let kind = evidence["kind"].as_str().unwrap_or("unlabelled");
    ACCOUNTS_EVIDENCE_TOTAL.with_label_values(&[kind]).inc();
}

fn lost(book: &mut AccountBook, marks: &mut NodeMarks, stream: Stream, reason: &str, err: &LineError) -> Touched {
    let at = err.block.map_or_else(|| "of unknown blocks".to_string(), |block| format!("of block {block}"));
    error!("accounts {} data loss ({reason}) {at}: {}", stream.label(), err.error);
    ACCOUNTS_DATA_LOSS_TOTAL.with_label_values(&[stream.label(), reason]).inc();
    if let Some(block) = err.block {
        record_lost_block(book, marks, stream, block);
    } else {
        let detected_ms = now_ms();
        book.record_unknown_loss(stream, detected_ms);
        marks.record_unknown_loss(stream, detected_ms);
    }
    Touched::default()
}

/// Each owner keeps the losses of the streams it reads.
fn record_lost_block(book: &mut AccountBook, marks: &mut NodeMarks, stream: Stream, block: u64) {
    book.record_lost_block(stream, block);
    marks.record_lost_block(stream, block);
}

fn report_marks_block(height: u64, report: MarksBlockReport) {
    let MarksBlockReport { unknown_coin_updates, unknown_coin_diffs, errors, log_overflowed, ambiguous_cuts, ambiguous_samples } =
        report;
    NODE_MARKS_UNKNOWN_COIN_INPUTS_TOTAL.with_label_values(&[Stream::Hip3OracleUpdates.label()]).inc_by(unknown_coin_updates as u64);
    NODE_MARKS_UNKNOWN_COIN_INPUTS_TOTAL.with_label_values(&[Stream::BookDiffs.label()]).inc_by(unknown_coin_diffs as u64);
    NODE_MARKS_AMBIGUOUS_CUTS_TOTAL.inc_by(ambiguous_cuts);
    for cut in &ambiguous_samples {
        record_evidence(&ambiguous_cut_json(height, cut));
    }
    report_contract_errors(&errors, "");
    if log_overflowed {
        error!("node marks data loss (mark_log_overflow): the input log overflowed at block {height}");
        for stream in MARK_STREAMS {
            ACCOUNTS_DATA_LOSS_TOTAL.with_label_values(&[stream.label(), "mark_log_overflow"]).inc();
        }
    }
}

/// Inputs the marks could not apply; `when` says where they were met (empty while applying the block).
fn report_contract_errors(errors: &[(Stream, String)], when: &str) {
    for (stream, err) in errors {
        error!("accounts {} data loss (contract{when}): {err}", stream.label());
        ACCOUNTS_DATA_LOSS_TOTAL.with_label_values(&[stream.label(), "contract"]).inc();
    }
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

fn report_block_actions(report: BlockActionsReport) -> Touched {
    if let Some((first, last)) = report.missing {
        error!("accounts block_actions data loss (height_gap): blocks {first} to {last} never arrived");
        ACCOUNTS_DATA_LOSS_TOTAL.with_label_values(&[Stream::BlockActions.label(), "height_gap"]).inc();
    }
    report_dropped_marks(report.marks_dropped);
    report.touched
}

/// A poller message: a parsed anchor is held until `AccountBook::anchor_ready` and then installed (a newer one replaces
/// a held one), its seed likewise until `NodeMarks::seed_ready`; returns whether an anchor was installed.
fn receive_anchor(
    book: &mut AccountBook,
    marks: &mut NodeMarks,
    held: &mut Option<Box<ParsedAnchor>>,
    held_seed: &mut Option<Box<MarkSeed>>,
    message: AnchorMessage,
) -> bool {
    match message {
        AnchorMessage::Parsed { parsed, seed } => {
            receive_seed(marks, held_seed, parsed.anchor.height, seed);
            if let Some(replaced) = held.replace(parsed) {
                warn!("accounts: anchor {} replaced while waiting for its blocks", replaced.anchor.height);
                ACCOUNTS_ANCHOR_SKIPPED_TOTAL.inc();
            }
            let installed = install_ready(book, held);
            if let Some(waiting) = held {
                info!("accounts: anchor {} waits until its blocks are applied", waiting.anchor.height);
            }
            installed
        }
        AnchorMessage::Failed { height, error } => {
            error!("accounts: checkpoint {height} failed to parse, withholding the view and the node marks: {error}");
            ACCOUNTS_CHECKPOINT_FAILURES_TOTAL.inc();
            marks.record_seed_failure(height, error.clone());
            book.record_checkpoint_failure(height, error);
            false
        }
    }
}

fn receive_seed(
    marks: &mut NodeMarks,
    held_seed: &mut Option<Box<MarkSeed>>,
    height: u64,
    seed: std::result::Result<Box<MarkSeed>, String>,
) {
    match seed {
        Ok(seed) => {
            if let Some(replaced) = held_seed.replace(seed) {
                warn!("node marks: seed {} replaced while waiting for its blocks", replaced.height);
            }
            install_seed_ready(marks, held_seed);
        }
        Err(error) => {
            error!("node marks: the seed of checkpoint {height} breaks its contract, withholding the marks: {error}");
            NODE_MARKS_SEED_FAILURES_TOTAL.inc();
            marks.record_seed_failure(height, error);
        }
    }
}

/// Installs the held seed once the marks are ready for it.
fn install_seed_ready(marks: &mut NodeMarks, held_seed: &mut Option<Box<MarkSeed>>) {
    let Some(seed) = held_seed.take_if(|seed| marks.seed_ready(seed.height)) else {
        return;
    };
    let height = seed.height;
    log_marks_install(height, &marks.install(*seed));
}

fn log_marks_install(height: u64, report: &MarksInstallReport) {
    let MarksInstallReport { previous_seed, check, rebuild, errors } = report;
    let rebuild_label = match rebuild {
        Rebuild::Replayed { .. } => "replayed",
        Rebuild::Ahead => "ahead",
        Rebuild::LogDoesNotReach => "logDoesNotReach",
    };
    NODE_MARKS_REBUILDS_TOTAL.with_label_values(&[rebuild_label]).inc();
    match check {
        MarksCheck::Skipped(reason) => {
            info!("node marks: seed {height} (previous {previous_seed:?}) not checked ({reason}); rebuild {rebuild:?}");
            NODE_MARKS_CHECKS_SKIPPED_TOTAL.with_label_values(&[reason]).inc();
        }
        MarksCheck::Checked { hip3, books, prices } => {
            info!(
                "node marks: seed {height} (previous {previous_seed:?}) against the snapshot: HIP-3 {} equal, {} differ, {} \
                 unknown; main books {} equal, {} differ, {} unknown (orders {} equal, {} differ, {} missing, {} extra); \
                 rebuild {rebuild:?}",
                hip3.equal, hip3.differ, hip3.unknown, books.books_equal, books.books_differ, books.unknown,
                books.orders_equal, books.orders_differ, books.missing, books.extra
            );
            let counts = [
                ("hip3", "equal", hip3.equal),
                ("hip3", "differ", hip3.differ),
                ("hip3", "unknown", hip3.unknown),
                ("nativeBooks", "equal", books.books_equal),
                ("nativeBooks", "differ", books.books_differ),
                ("nativeBooks", "unknown", books.unknown),
                ("nativeOrders", "equal", books.orders_equal),
                ("nativeOrders", "differ", books.orders_differ),
                ("nativeOrders", "missing", books.missing),
                ("nativeOrders", "extra", books.extra),
            ];
            for (market, result, count) in counts {
                NODE_MARKS_CHECKED_TOTAL.with_label_values(&[market, result]).inc_by(count);
            }
            for mismatch in &hip3.samples {
                record_evidence(&hip3_mark_mismatch_json(mismatch, *previous_seed, height));
            }
            for mismatch in &books.samples {
                record_evidence(&book_mismatch_json(mismatch, *previous_seed, height));
            }
            let fields: Vec<String> =
                prices.fields.iter().map(|(field, equal, differ)| format!("{field} {equal}/{}", equal + differ)).collect();
            let unsupported: Vec<String> =
                prices.unsupported.iter().map(|(reason, coins)| format!("{reason} {} ({})", coins.len(), coins.join(", "))).collect();
            let limited: Vec<String> = prices.step_limited.iter().map(|(coin, times)| format!("{coin} {times}")).collect();
            info!(
                "node marks: seed {height} main prices equal: {}; not replayed: {}; marks moved by the 1% limit since the last seed: {}",
                fields.join(", "),
                if unsupported.is_empty() { "none".to_string() } else { unsupported.join(", ") },
                if limited.is_empty() { "none".to_string() } else { limited.join(", ") }
            );
            for (field, equal, differ) in &prices.fields {
                NODE_MARKS_CHECKED_TOTAL.with_label_values(&[field, "equal"]).inc_by(*equal);
                NODE_MARKS_CHECKED_TOTAL.with_label_values(&[field, "differ"]).inc_by(*differ);
            }
            for (reason, coins) in &prices.unsupported {
                NODE_MARKS_CHECKED_TOTAL.with_label_values(&["nativeUnsupported", reason]).inc_by(coins.len() as u64);
            }
            for mismatch in &prices.samples {
                record_evidence(&price_mismatch_json(mismatch, *previous_seed, height));
            }
        }
    }
    report_contract_errors(errors, &format!(", replayed at seed {height}"));
}

/// Installs the held anchor once the book is ready for it; returns whether it did.
fn install_ready(book: &mut AccountBook, held: &mut Option<Box<ParsedAnchor>>) -> bool {
    let Some(parsed) = held.take_if(|parsed| book.anchor_ready(parsed.anchor.height)) else {
        return false;
    };
    let ParsedAnchor { anchor, parse_seconds, skipped } = *parsed;
    let height = anchor.height;
    let report = book.install_anchor(anchor);
    log_install(height, parse_seconds, skipped, &report);
    ACCOUNTS_ANCHOR_HEIGHT.set(height.cast_signed());
    ACCOUNTS_ANCHOR_PARSE_SECONDS.set(parse_seconds);
    ACCOUNTS_ANCHOR_SKIPPED_TOTAL.inc_by(skipped as u64);
    ACCOUNTS_SILENT_POSITION_CHANGES_TOTAL.inc_by(report.silent_position_changes);
    true
}

fn log_install(height: u64, parse_seconds: f64, skipped: usize, report: &InstallReport) {
    info!(
        "accounts: installed anchor {height} (parsed in {parse_seconds:.2}s, {skipped} skipped): {} silent position \
         changes, {} held fills covered, {} replayed, {} records of coins that left the universe",
        report.silent_position_changes, report.covered_held_fills, report.replayed_fills, report.dropped_coin_records
    );
    for change in &report.silent_samples {
        record_evidence(&silent_change_json(change, report.previous_anchor, height));
    }
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
    log_replay_check(height, report);
}

/// The replayed rows at the anchor against its own (design 000437): counts, metrics, and mismatches as evidence.
fn log_row_check(height: u64, previous: Option<u64>, rows: &RowCheck) {
    info!(
        "accounts: replayed rows at anchor {height}: {} equal, {} differ, {} classed differently, {} moved after the anchor",
        rows.equal, rows.differ, rows.class_differs, rows.moved_after
    );
    for (result, count) in [("equal", rows.equal), ("differ", rows.differ), ("classDiffers", rows.class_differs)] {
        ACCOUNTS_REPLAY_ROWS_CHECKED_TOTAL.with_label_values(&[result]).inc_by(count);
    }
    for mismatch in &rows.samples {
        record_evidence(&replay_row_mismatch_json(mismatch, previous, height));
    }
}

/// The cash replay of the interval ending at this anchor (design 000433): counts, metrics, and mismatches as evidence.
fn log_replay_check(height: u64, report: &InstallReport) {
    let check = match &report.replay_check {
        ReplayCheck::Skipped(reason) => {
            info!("accounts: replay to anchor {height} not checked: {reason}");
            ACCOUNTS_REPLAY_CHECKS_SKIPPED_TOTAL.with_label_values(&[reason]).inc();
            return;
        }
        ReplayCheck::Checked { cash, rows } => {
            log_row_check(height, report.previous_anchor, rows);
            cash
        }
    };
    let sorted = |counts: Vec<(&str, String)>| {
        let mut counts = counts;
        counts.sort_unstable();
        counts.iter().map(|(label, count)| format!("{label} {count}")).collect::<Vec<_>>().join(", ")
    };
    let compared = check.compared.iter().map(|(mode, counts)| {
        (*mode, format!("{} exact ({} moved) / {} mismatched", counts.exact, counts.moved, counts.mismatched))
    });
    let stopped = check.stopped.iter().map(|(reason, count)| (*reason, count.to_string()));
    info!(
        "accounts: replay {:?} -> {height}: {}; stopped: {}",
        report.previous_anchor,
        sorted(compared.collect()),
        sorted(stopped.collect())
    );
    for (mode, counts) in &check.compared {
        ACCOUNTS_REPLAY_CHECKED_TOTAL.with_label_values(&[mode, "exact"]).inc_by(counts.exact);
        ACCOUNTS_REPLAY_CHECKED_TOTAL.with_label_values(&[mode, "exactMoved"]).inc_by(counts.moved);
        ACCOUNTS_REPLAY_CHECKED_TOTAL.with_label_values(&[mode, "mismatch"]).inc_by(counts.mismatched);
    }
    for (reason, count) in &check.stopped {
        ACCOUNTS_REPLAY_STOPPED_TOTAL.with_label_values(&[reason]).inc_by(*count);
    }
    for mismatch in &check.samples {
        record_evidence(&replay_mismatch_json(mismatch, report.previous_anchor, height));
    }
}

fn answer(book: &AccountBook, marks: &NodeMarks, candidates: &Candidates, query: Query) {
    // a dropped receiver is a caller that gave up; nothing to undo
    match query {
        Query::CoinPositions { coin, reply } => drop(reply.send(book.coin_positions(&coin, marks.at()))),
        Query::Status { reply } => drop(reply.send(book.status())),
        Query::Candidates { reply } => drop(reply.send(candidates.snapshot())),
    }
}

/// The candidates after a step (design 000443), broadcast as one frame; a withdrawal ends every subscription.
fn refresh_candidates(candidates: &mut Candidates, book: &AccountBook, marks: &NodeMarks, touched: &Touched, publisher: &Publisher) {
    let started = Instant::now();
    let mut users: Vec<Address> = touched.accounts.clone();
    users.extend(touched.positions.iter().map(|(_, user)| *user));
    let refresh = candidates.refresh(book, marks.at(), &users);
    LIQUIDATION_CANDIDATES_REFRESH_SECONDS.observe(started.elapsed().as_secs_f64());
    LIQUIDATION_CANDIDATES.set((candidates.count() as u64).cast_signed());
    publisher.publish_candidates(refresh);
}

/// A finished thread stops the view and the marks for good: every input thread feeds the block assembly both apply,
/// and the poller their seeds (review 000440 r1 P2-2).
fn supervise(book: &mut AccountBook, marks: &mut NodeMarks, threads: &[Supervised]) {
    if matches!(book.availability(), Err(Unavailable::Stopped(_))) {
        return;
    }
    if let Some(thread) = threads.iter().find(|thread| thread.handle.is_finished()) {
        error!("accounts: the {} thread exited; the view and the node marks stay unavailable until restart", thread.name);
        let reason = format!("{} thread exited", thread.name);
        marks.stop(reason.clone());
        book.stop(reason);
    }
}

fn export_marks_status(status: &MarksStatus) {
    NODE_MARKS_AVAILABLE.set(i64::from(status.availability.is_ok()));
    if let Ok(height) = status.availability {
        NODE_MARKS_HEIGHT.set(height.cast_signed());
    }
    if let Some(height) = status.seed_height {
        NODE_MARKS_SEED_HEIGHT.set(height.cast_signed());
    }
    NODE_MARKS_LOGGED_INPUTS.set((status.logged_inputs as u64).cast_signed());
}

fn export_status(status: &BookStatus) {
    ACCOUNTS_TRUSTED.set(i64::from(status.availability.is_ok()));
    ACCOUNTS_OPEN_POSITIONS.set((status.open_positions as u64).cast_signed());
    ACCOUNTS_HELD_FILLS.set((status.held_fills as u64).cast_signed());
    let heights = [
        (Stream::Fills, status.fills_height),
        (Stream::MiscEvents, status.misc_events_height),
        (Stream::BlockActions, status.block_actions_height),
    ];
    for (stream, height) in heights {
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
    /// Spawns the owner on the current tokio runtime (its marks get no seed: unavailable throughout).
    pub(crate) fn start(book: AccountBook) -> Self {
        let (inputs, input_rx) = mpsc::channel(16);
        let (anchors, anchor_rx) = mpsc::channel(1);
        let (queries, query_rx) = mpsc::channel(16);
        let subscriptions = CoinSubscriptions::default();
        let (frames, _) = broadcast::channel(FRAME_CHANNEL_CAPACITY);
        let handle = AccountsHandle { queries, subscriptions: subscriptions.clone(), frames: frames.downgrade() };
        let marks = NodeMarks::new(tests::TEST_GRID, 0);
        let publisher = Publisher::new(subscriptions, frames);
        let owner = tokio::spawn(run_owner(book, marks, publisher, input_rx, anchor_rx, query_rx, Vec::new(), CandidateFeed::start(None)));
        Self { handle, inputs, anchors, owner }
    }

    /// Ends the owner task (as a panic would) and waits until it is gone.
    pub(crate) async fn kill(&self) {
        self.owner.abort();
        while !self.owner.is_finished() {
            tokio::task::yield_now().await;
        }
    }

    /// Feeds these fills lines of block `height` and completes the block: the `replica_cmds` lines of it and of the
    /// next block, then a read-through of the other readers after them (design 000433 r3).
    pub(crate) async fn complete_block(&self, height: u64, fills_lines: &[String]) {
        for line in fills_lines {
            self.inputs.send(fills_input(line.clone())).await.unwrap();
        }
        for block in [height, height + 1] {
            let line = Ok((BlockActions { height: block, time_ms: block * 1_000, actions: Vec::new() }, Err("an account test line".to_string())));
            self.inputs.send(Input::BlockActions(line, Instant::now())).await.unwrap();
        }
        let after = Instant::now() + Duration::from_micros(1);
        for stream in PROVING_STREAMS {
            self.inputs.send(Input::ReadThrough(stream, after)).await.unwrap();
        }
    }

    pub(crate) async fn loss(&self, stream: Stream) {
        self.inputs.send(Input::Loss(stream)).await.unwrap();
    }

    pub(crate) async fn anchor(&self, anchor: Anchor) {
        self.anchors.send(tests::parsed_without_seed(anchor)).await.unwrap();
    }
}

/// The streams whose readers send read-through proofs.
#[cfg(test)]
const PROVING_STREAMS: [Stream; 5] =
    [Stream::Fills, Stream::MiscEvents, Stream::Hip3OracleUpdates, Stream::BookDiffs, Stream::OrderStatuses];

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use rustc_hash::FxHashMap;

    use super::*;
    use crate::accounts::{
        anchor::AnchorCoin,
        book::{CLOCK_MARGIN_MS, LossBound},
        replay::MarginStates,
    };

    const USER: &str = "0x5b51db9432bc4dd2c30cadc4ab6ab44565419c9a";
    pub(super) const TEST_GRID: CheckpointGrid = CheckpointGrid { anchor: 1_000, interval: 10_000 };

    /// An account view test's anchor message: its checkpoint seeds no marks.
    pub(super) fn parsed_without_seed(anchor: Anchor) -> AnchorMessage {
        let parsed = Box::new(ParsedAnchor { anchor, parse_seconds: 0.0, skipped: 0 });
        AnchorMessage::Parsed { parsed, seed: Err("an account view test seeds no marks".to_string()) }
    }

    fn batch_line(block: u64, events: &str) -> String {
        format!(
            "{{\"local_time\":\"2026-10-19T06:00:00.1\",\"block_time\":\"2026-10-19T06:00:00.0\",\"block_number\":{block},\"events\":[{events}]}}\n"
        )
    }

    fn ledger_line(block: u64) -> String {
        let event = format!(
            r#"{{"time":"2026-10-19T06:00:00.0","hash":"0x00","inner":{{"LedgerUpdate":{{"users":["{USER}"],"delta":{{"type":"deposit","usdc":"1.0"}}}}}}}}"#
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
        let coin = AnchorCoin { size_decimals: 0, table_id: 52, mark_raw: None, positions: FxHashMap::default() };
        let coins = std::iter::once(("PUMP".to_string(), coin)).collect();
        Anchor { height, time_ms, coins, account_modes: FxHashMap::default(), margins: MarginStates::default() }
    }

    /// The `replica_cmds` lines of `blocks`, then a read-through of the other readers after them: the blocks before
    /// the last one complete.
    fn complete_through(
        book: &mut AccountBook,
        marks: &mut NodeMarks,
        assembler: &mut BlockAssembler,
        blocks: std::ops::RangeInclusive<u64>,
    ) {
        for height in blocks {
            let parsed = (BlockActions { height, time_ms: height * 1_000, actions: Vec::new() }, Err("an account test line".to_string()));
            let line = Input::BlockActions(Ok(parsed), Instant::now());
            apply_input(book, marks, assembler, line);
        }
        let after = Instant::now() + Duration::from_micros(1);
        for stream in PROVING_STREAMS {
            apply_input(book, marks, assembler, Input::ReadThrough(stream, after));
        }
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
        let stream = LineStream {
            name: "TestMisc",
            lag_label: "test",
            dir,
            max_line_bytes: 1 << 20,
            start_skip_is_loss: true,
            layout: FileLayout::Hourly,
        };
        let _watcher = spawn_line_watcher(stream, tx, None, misc_input, || Input::Loss(Stream::MiscEvents), None);
        // let the watcher register its inotify watch before the first append
        thread::sleep(Duration::from_millis(200));
        let mut book = AccountBook::new(0);
        let mut marks = NodeMarks::new(TEST_GRID, 0);
        let mut assembler = BlockAssembler::default();
        apply_input(&mut book, &mut marks, &mut assembler, fills_input(batch_line(1_200, "")));
        complete_through(&mut book, &mut marks, &mut assembler, 1_200..=1_201);
        book.install_anchor(empty_anchor(1_300, now_ms()));

        append(&file, &ledger_line(1_500));
        let skipped = recv_within(&mut inputs, Duration::from_secs(2));
        assert!(matches!(skipped, Input::Loss(Stream::MiscEvents)), "the positioning skip is reported");
        apply_input(&mut book, &mut marks, &mut assembler, skipped);
        append(&file, &ledger_line(1_202));
        let next = recv_within(&mut inputs, Duration::from_secs(2));
        assert!(matches!(next, Input::MiscEvents(Ok(_), None)), "lines after the skip flow");
        apply_input(&mut book, &mut marks, &mut assembler, next);
        complete_through(&mut book, &mut marks, &mut assembler, 1_202..=1_203);
        assert_eq!(assembler.released(), Some(1_202));
        assert!(matches!(
            book.availability(),
            Err(Unavailable::DataLoss { stream: Stream::MiscEvents, bound: LossBound::WrittenBy(_) })
        ));
        book.install_anchor(empty_anchor(1_700, now_ms() + CLOCK_MARGIN_MS));
        assert!(book.availability().is_ok());
    }

    #[test]
    fn only_a_rare_line_keeps_its_raw_text() {
        use crate::accounts::test_support::{PUMP, fills_line, user};
        let ordinary = fills_line(1_100, &[(user(1), PUMP, "B", 1, "0.0", "1.0")]);
        assert!(matches!(fills_input(ordinary.clone()), Input::Fills(Ok(_), None)));
        let rare = ordinary.replace("\"Open Long\"", "\"Auto-Deleveraging\"");
        let Input::Fills(Ok(_), Some(evidence)) = fills_input(rare.clone()) else { panic!("rare line without evidence") };
        assert_eq!(evidence, LineEvidence { reason: "fill dir \"Auto-Deleveraging\"".to_string(), raw_line: rare });
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

    /// Review 000435 r1 P2: an anchor whose blocks are not applied waits in the owner, a newer one replaces it, and it
    /// installs after the input that applies its block.
    #[test]
    fn an_anchor_waits_for_its_blocks_and_a_newer_one_replaces_it() {
        use crate::accounts::test_support::{PUMP, anchor as test_anchor, block_actions, trusted_book, user};
        let parsed = |height| parsed_without_seed(test_anchor(height, &[(PUMP, 0, &[(user(1), 5)])]));
        let installed_height = |book: &AccountBook| book.availability().map(|available| available.anchor.height);
        let mut book = trusted_book(&[(user(1), 5)]);
        let mut marks = NodeMarks::new(TEST_GRID, 0);
        let (mut held, mut held_seed) = (None, None);
        assert!(!receive_anchor(&mut book, &mut marks, &mut held, &mut held_seed, parsed(1_002)), "block 1002 is not applied");
        assert!(!receive_anchor(&mut book, &mut marks, &mut held, &mut held_seed, parsed(1_003)));
        assert_eq!(held.as_ref().map(|held| held.anchor.height), Some(1_003), "the newer one is held");
        book.apply_block_actions(block_actions(1_001, &[]));
        book.apply_block_actions(block_actions(1_002, &[]));
        assert!(!install_ready(&mut book, &mut held));
        assert_eq!(installed_height(&book), Ok(1_000));
        book.apply_block_actions(block_actions(1_003, &[]));
        assert!(install_ready(&mut book, &mut held));
        assert_eq!((installed_height(&book), held.is_none()), (Ok(1_003), true));
        book.apply_block_actions(block_actions(1_004, &[]));
        assert!(receive_anchor(&mut book, &mut marks, &mut held, &mut held_seed, parsed(1_004)), "applied already: installs at once");
    }

    #[test]
    fn a_misc_line_is_identified_by_its_events_text_or_fails() {
        let key = |line: &str| match misc_input(line.to_string()) {
            Input::MiscEvents(parsed, _) => parsed.map(|(_, key)| key),
            _ => panic!("a misc events input"),
        };
        let replayed = ledger_line(9).replace("06:00:00.1", "07:00:00.1");
        assert_eq!(key(&ledger_line(9)).unwrap(), key(&replayed).unwrap(), "local_time is not part of the identity");
        let spaced = ledger_line(9).replace("\"events\":", "\"events\" :");
        let err = key(&spaced).unwrap_err();
        assert_eq!(err.block, Some(9));
        assert!(err.error.starts_with("no \"events\": text to take the line's identity from"), "{}", err.error);
    }

    /// The marks read cancels of orders with a cloid and triggers; any other status reaches the owner as nothing.
    #[test]
    fn an_order_statuses_line_is_kept_only_for_a_cloid_cancel_or_a_trigger() {
        let line = |status: &str, cloid: &str| {
            format!(
                r#"{{"local_time":"2026-10-20T12:00:00.1","block_time":"2026-10-20T12:00:00.0","block_number":9,"events":[{{"time":"2026-10-20T12:00:00.0","user":"0x00000000000000000000000000000000000000aa","hash":null,"builder":null,"status":"{status}","order":{{"coin":"BTC","side":"B","limitPx":"82850.0","sz":"0.0","oid":7,"timestamp":1,"triggerCondition":"N/A","isTrigger":false,"triggerPx":"0.0","children":[],"isPositionTpsl":false,"reduceOnly":false,"orderType":"Limit","origSz":"0.001","tif":"Alo","cloid":{cloid}}}}}]}}"#
            )
        };
        for (status, cloid) in [("canceled", r#""0x0a""#), ("triggered", "null")] {
            let Input::OrderStatuses(Ok(Some((batch, _)))) = order_statuses_input(line(status, cloid)) else { panic!("{status} kept") };
            assert_eq!(batch.events_len(), 1);
        }
        for (status, cloid) in [("canceled", "null"), ("open", r#""0x0a""#), ("filled", r#""0x0a""#)] {
            assert!(matches!(order_statuses_input(line(status, cloid)), Input::OrderStatuses(Ok(None))), "{status} {cloid}");
        }
    }

    #[test]
    fn a_block_actions_line_breaking_the_contract_keeps_its_height() {
        let empty = r#"{"abci_block":{"time":"2026-10-19T06:00:00.0","signed_action_bundles":[]},"resps":{"Full":[]},"height":8}"#;
        let Input::BlockActions(Ok((block, _)), _) = block_actions_input(empty.to_string()) else { panic!("an empty block reads") };
        assert_eq!(block, BlockActions { height: 8, time_ms: 1_792_389_600_000, actions: Vec::new() });
        let unpaired = r#"{"abci_block":{"time":"2026-10-19T06:00:00.0","signed_action_bundles":[["0x01",{"signed_actions":[]}]]},"resps":{"Full":[]},"height":7}"#;
        let Input::BlockActions(Err(err), _) = block_actions_input(unpaired.to_string()) else { panic!("no responses") };
        assert_eq!(err.block, Some(7));
        assert!(err.error.starts_with("block 7: 1 bundles but 0 response groups (line of "), "{}", err.error);
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

    /// Review 000440 r1 P2-2, through the owner's supervision and status export: an exited input thread stops the
    /// account view and the node marks alike, and a later seed does not revive the marks.
    #[test]
    fn an_exited_thread_stops_the_view_and_the_marks() {
        use crate::accounts::test_support::{trusted_book, user};
        let mut book = trusted_book(&[(user(1), 5)]);
        let mut marks = NodeMarks::new(TEST_GRID, 0);
        let handle = thread::spawn(|| {});
        while !handle.is_finished() {
            thread::sleep(Duration::from_millis(1));
        }
        let threads = [Supervised { name: "HIP-3 oracle updates watcher", handle }];
        supervise(&mut book, &mut marks, &threads);
        let stopped = Unavailable::Stopped("HIP-3 oracle updates watcher thread exited".to_string());
        assert_eq!(book.availability().map(|_| ()), Err(stopped.clone()));
        assert_eq!(marks.status().availability, Err(stopped));
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

    /// Review 000445 r1 P2: an anchor and its seed waiting for the same blocks both install before anything is pushed,
    /// so the anchor's snapshot values the shared pool at the seed's marks, as a query at that height does. The seed
    /// at 1100 moves BTC's mark; HOLDER holds PUMP and BTC in one pool.
    #[tokio::test]
    async fn a_snapshot_after_a_waiting_seed_reads_the_marks_a_query_reads() {
        use crate::accounts::{
            checkpoint::tests::{HOLDER, user as checkpoint_user},
            marks::tests::trusted_marks,
            replay::tests::per_dex_with_main_cash,
            test_support::{PUMP, SEED_TIME_MS, anchor, block_actions, fills, marks_seed, misc},
            view::CoinPositionsDto,
        };
        let shared_pool = |height| {
            let holder = checkpoint_user(HOLDER);
            let mut anchor = anchor(height, &[(PUMP, 0, &[(holder, 1_000_000)]), ("BTC", 5, &[(holder, 10_000)])]);
            anchor.margins = per_dex_with_main_cash(-12_000_000_000);
            anchor
        };
        let mut book = AccountBook::new(0);
        book.apply_fills(fills(100, &[]));
        book.apply_misc_events(misc(100, &[]));
        book.apply_block_actions(block_actions(1_001, &[]));
        book.install_anchor(shared_pool(1_000));
        let marks = trusted_marks();
        let subscriptions = CoinSubscriptions::default();
        let (frames, mut received) = broadcast::channel(16);
        let (input_tx, input_rx) = mpsc::channel(256);
        let (anchor_tx, anchor_rx) = mpsc::channel(1);
        let (query_tx, query_rx) = mpsc::channel(16);
        let mut publisher = Publisher::new(subscriptions.clone(), frames);
        // the view and the marks are available before anyone subscribes
        publisher.track(&book);
        publisher.track_marks(&mut book, &marks);
        let _guard = subscriptions.acquire(PUMP);
        let owner = tokio::spawn(run_owner(book, marks, publisher, input_rx, anchor_rx, query_rx, Vec::new(), CandidateFeed::start(None)));
        let query = || async {
            let (reply, answer) = oneshot::channel();
            query_tx.send(Query::CoinPositions { coin: PUMP.to_string(), reply }).await.unwrap();
            answer.await.unwrap().unwrap()
        };
        let mut seed = marks_seed(1_100);
        seed.native.assets.iter_mut().find(|asset| asset.name == "BTC").unwrap().mark.px = 900_000;
        let parsed = Box::new(ParsedAnchor { anchor: shared_pool(1_100), parse_seconds: 0.0, skipped: 0 });
        anchor_tx.send(AnchorMessage::Parsed { parsed, seed: Ok(Box::new(seed)) }).await.unwrap();
        assert_eq!(query().await.anchor.height, 1_000, "the anchor waits for its blocks");
        for height in 1_002..=1_101 {
            let actions = MarkActions { height, time_ns: (SEED_TIME_MS + height) * 1_000_000, actions: Vec::new() };
            input_tx.send(Input::BlockActions(Ok((block_actions(height, &[]), Ok(actions))), Instant::now())).await.unwrap();
        }
        let read_through = Instant::now() + Duration::from_millis(1);
        for stream in [Stream::Fills, Stream::MiscEvents, Stream::Hip3OracleUpdates, Stream::BookDiffs, Stream::OrderStatuses] {
            input_tx.send(Input::ReadThrough(stream, read_through)).await.unwrap();
        }
        let installed = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let view = query().await;
                if view.anchor.height == 1_100 {
                    break view;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let queried = serde_json::to_value(CoinPositionsDto::new(&installed)).unwrap();
        let mut snapshots = Vec::new();
        while let Ok(frame) = received.try_recv() {
            if let AccountsFrame::Coin { wire, .. } = frame.as_ref() {
                let frame: serde_json::Value = serde_json::from_slice(wire).unwrap();
                if frame["data"]["kind"] == "snapshot" {
                    snapshots.push(frame);
                }
            }
        }
        owner.abort();
        let [snapshot] = snapshots.as_slice() else { panic!("one snapshot: {snapshots:?}") };
        let (pushed, read) = (&snapshot["data"]["positions"][0], &queried["positions"][0]);
        assert_eq!(pushed["liquidationPxSource"], "replay");
        assert_eq!((&pushed["liquidationPx"], &pushed["markPx"]), (&read["liquidationPx"], &read["markPx"]));
    }
}
