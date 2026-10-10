// HFT-optimized parallel file watcher
// Each event source runs on its own thread for maximum throughput

use crate::{
    metrics::NODE_LINE_LAG,
    types::node_data::{EventSource, line_local_time_unix_nanos},
};
use log::{error, info, warn};
use notify::{Event, RecursiveMode, Watcher, recommended_watcher};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    os::unix::fs::MetadataExt,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
    thread,
    time::Duration,
};

/// Message sent from file watcher threads to the main processor
#[derive(Debug)]
pub(crate) enum FileEvent {
    OrderStatus(String),
    OrderDiff(String),
    Fill(String),
    /// HIP-3 deployer oracle update line (side stream, never book-affecting).
    OracleUpdate(String),
    /// The watcher had to discard buffered data (oversized partial line). The
    /// book may have missed events and must be re-synced from a snapshot.
    Desync(EventSource),
}

/// Hard cap on a single un-terminated JSON line. The streaming files write
/// newline-delimited JSON; this bound is a safety net against a corrupt/partial
/// flush from the node that would otherwise let `partial_line` grow without
/// limit and OOM the host.
const MAX_PARTIAL_LINE_BYTES: usize = 16 * 1024 * 1024;

/// Upper bound on bytes read per `on_modify` call. After a consumer stall the
/// on-disk backlog can be hundreds of MB; the old read-to-EOF materialized all
/// of it in memory in one shot (plus a full copy when prepending the partial
/// tail) at the worst possible moment. One chunk per call keeps resident
/// memory bounded - the 1ms poll loop (and `on_create`'s drain loop) comes
/// straight back for the remainder.
const READ_CHUNK_BYTES: u64 = 8 * 1024 * 1024;

/// Wall-clock milliseconds since the unix epoch, for the watcher health
/// timestamps. (The previous `Instant::now().elapsed()` measured elapsed time
/// since *now* - always ~0 - making the health values meaningless.)
pub(super) fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Seconds since a node write stamped `node_unix_nanos` (same host clock).
pub(super) fn seconds_since_node_write(node_unix_nanos: i64) -> f64 {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_nanos());
    (i128::try_from(now).unwrap_or(i128::MAX) - i128::from(node_unix_nanos)) as f64 / 1e9
}

/// How fresh the data is when the watcher sees it: lag of the newest line read.
fn observe_read_lag(source: EventSource, lines: &[String]) {
    if let Some(node_ns) = lines.last().and_then(|line| line_local_time_unix_nanos(line)) {
        NODE_LINE_LAG.with_label_values(&["read", source.metric_label()]).observe(seconds_since_node_write(node_ns));
    }
}

/// File reader state for a single source
struct FileReader {
    current_path: Option<PathBuf>,
    // Open handle to current_path, reused across reads. Re-opening per modify
    // event (plus per poll-timeout fallback) cost an open+stat syscall pair
    // thousands of times per second per watcher thread.
    file: Option<File>,
    file_position: u64,
    // Unterminated tail of the last read, awaiting its newline. Raw bytes, not
    // String: a bounded read can split a multi-byte character at the chunk
    // boundary, which must not fail the whole read.
    partial_line: Vec<u8>,
    base_dir: PathBuf, // Base streaming directory to scan for new files
    // Set when buffered data had to be discarded (oversized partial line);
    // drained by take_desynced so the watcher can notify the listener.
    desynced: bool,
    // Last path whose open failed, so the 1ms poll loop logs the failure once
    // instead of thousands of times per second while the path is unopenable.
    open_error_path: Option<PathBuf>,
    // (dev, ino) of the file that file_position refers to. Catches same-path
    // recreation: node catch-up writes by BLOCK time, so after a long stop it
    // recreates the very hour file retention pruned - same path, new inode,
    // where our position is meaningless.
    open_file_id: Option<(u64, u64)>,
}

/// (dev, ino) identity of a metadata handle.
fn file_id(meta: &std::fs::Metadata) -> (u64, u64) {
    (meta.dev(), meta.ino())
}

impl FileReader {
    fn new(base_dir: PathBuf) -> Self {
        Self {
            current_path: None,
            file: None,
            file_position: 0,
            partial_line: Vec::new(),
            base_dir,
            desynced: false,
            open_error_path: None,
            open_file_id: None,
        }
    }

    /// True once if data was discarded since the last call.
    fn take_desynced(&mut self) -> bool {
        std::mem::take(&mut self.desynced)
    }

    /// Find the latest file in the streaming directory tree
    /// Scans hourly/YYYYMMDD/HH structure and returns the most recently modified file
    fn find_latest_file(&self) -> Option<PathBuf> {
        let hourly_dir = self.base_dir.join("hourly");
        if !hourly_dir.exists() {
            return None;
        }

        // Find the latest day directory
        let mut latest_day: Option<PathBuf> = None;
        if let Ok(entries) = std::fs::read_dir(&hourly_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    if latest_day.is_none() || path > latest_day.clone().unwrap() {
                        latest_day = Some(path);
                    }
                }
            }
        }

        let day_dir = latest_day?;

        // Find the latest hour file in this day
        let mut latest_file: Option<PathBuf> = None;
        let mut latest_mtime: Option<std::time::SystemTime> = None;

        if let Ok(entries) = std::fs::read_dir(&day_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    if let Ok(metadata) = path.metadata() {
                        if let Ok(mtime) = metadata.modified() {
                            if latest_mtime.is_none() || mtime > latest_mtime.unwrap() {
                                latest_mtime = Some(mtime);
                                latest_file = Some(path);
                            }
                        }
                    }
                }
            }
        }

        latest_file
    }

    /// Check if there's a newer file than what we're currently tracking
    fn check_for_newer_file(&mut self) -> Option<PathBuf> {
        let latest = self.find_latest_file()?;
        let Some(current) = self.current_path.clone() else {
            return Some(latest); // no current file, use the latest
        };
        if latest == current {
            // Same path is NOT the same file after a long stop: node catch-up
            // writes by block time and recreates the very hour file retention
            // pruned. If the inode on disk no longer matches the one our
            // position refers to, force a switch. Deliberately NO desync flag
            // here: on_create makes that call - a held handle is drained to
            // EOF (gapless, no flag), a handle-less stale file flags desync.
            // Don't "simplify" this into flagging (or never flagging) here.
            let recreated = latest
                .metadata()
                .ok()
                .zip(self.open_file_id)
                .is_some_and(|(meta, open_id)| file_id(&meta) != open_id);
            return recreated.then_some(latest);
        }
        let Ok(current_meta) = current.metadata() else {
            // The tracked file vanished from disk (node down long enough for
            // retention to prune it). This polling fallback is then the ONLY
            // re-attach path - modify events don't adopt a file while
            // current_path is set, and the mtime comparison below could never
            // run again - so switch unconditionally. Jumping straight to the
            // latest file may skip intermediate files wholesale; flag the loss
            // so the book re-syncs from a covering snapshot instead of
            // continuing with a hole.
            warn!(
                "Tracked file {} vanished; force-switching to {} and flagging desync",
                current.display(),
                latest.display()
            );
            self.desynced = true;
            return Some(latest);
        };
        // Only switch when the new file has data (modification time is newer).
        let latest_mtime = latest.metadata().and_then(|m| m.modified()).ok()?;
        let current_mtime = current_meta.modified().ok()?;
        (latest_mtime > current_mtime).then_some(latest)
    }

    /// Process file modification - read new data and return lines
    fn on_modify(&mut self) -> Vec<String> {
        static MODIFY_COUNT: AtomicU64 = AtomicU64::new(0);
        let count = MODIFY_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let mut lines = Vec::new();
        if let Some(ref path) = self.current_path {
            // Open once and reuse the handle; a fresh fstat on the cached handle
            // still observes appended data (the node only ever appends).
            if self.file.is_none() {
                match File::open(path) {
                    Ok(file) => {
                        if let Ok(meta) = file.metadata() {
                            let id = file_id(&meta);
                            // Same path recreated under our feet while we held
                            // no handle: file_position points into the OLD
                            // inode, whose tail is gone. Read the new file
                            // from the start and flag the loss.
                            if self.file_position > 0 && self.open_file_id.is_some_and(|open_id| open_id != id) {
                                warn!(
                                    "{} was recreated in place; reading from start and flagging desync",
                                    path.display()
                                );
                                self.desynced = true;
                                self.file_position = 0;
                                self.partial_line.clear();
                            }
                            self.open_file_id = Some(id);
                        }
                        self.file = Some(file);
                        self.open_error_path = None;
                    }
                    Err(err) => {
                        if self.open_error_path.as_ref() != Some(path) {
                            error!("Failed to open {} (retrying silently): {err}", path.display());
                            self.open_error_path = Some(path.clone());
                        }
                        return lines;
                    }
                }
            }
            let mut read_failed = false;
            if let Some(file) = self.file.as_mut() {
                // Get fresh file size from the handle
                if let Ok(metadata) = file.metadata() {
                    let file_size = metadata.len();

                    // Only read if there's new data
                    if file_size > self.file_position {
                        // Log every read attempt
                        if count % 10_000 == 0 {
                            log::debug!(
                                "on_modify #{}: reading {} bytes (pos {} -> {})",
                                count,
                                file_size - self.file_position,
                                self.file_position,
                                file_size
                            );
                        }

                        if file.seek(SeekFrom::Start(self.file_position)).is_ok() {
                            // Bounded read (see READ_CHUNK_BYTES): one chunk per
                            // call so a catch-up backlog never pins itself in
                            // memory whole; the caller loops right back for more.
                            let mut buf = Vec::new();
                            match Read::by_ref(file).take(READ_CHUNK_BYTES).read_to_end(&mut buf) {
                                Ok(bytes_read) => {
                                    if bytes_read > 0 {
                                        // Update position
                                        self.file_position += bytes_read as u64;

                                        // Prepend any partial line from last read
                                        let mut full_buf = std::mem::take(&mut self.partial_line);
                                        full_buf.extend_from_slice(&buf);

                                        if count % 10_000 == 0 {
                                            // The line count walks the whole buffer - only
                                            // pay for it when this debug line actually fires.
                                            log::debug!(
                                                "on_modify #{}: read {} bytes, {} segments, ends_newline={}",
                                                count,
                                                bytes_read,
                                                full_buf.split(|&b| b == b'\n').count(),
                                                buf.last() == Some(&b'\n')
                                            );
                                        }

                                        // Only the unterminated tail may go to `partial_line`.
                                        // A newline-TERMINATED line that fails the JSON shape
                                        // check (or is not valid UTF-8) is complete-but-corrupt:
                                        // buffering it (the old behavior) prepended the garbage
                                        // to the next read and corrupted the following valid
                                        // line too. Discard it and flag the data loss so the
                                        // book re-syncs.
                                        for segment in full_buf.split_inclusive(|&b| b == b'\n') {
                                            if segment.last() == Some(&b'\n') {
                                                let Ok(text) = std::str::from_utf8(segment) else {
                                                    error!(
                                                        "discarding non-UTF-8 terminated line ({} bytes); flagging desync",
                                                        segment.len()
                                                    );
                                                    self.desynced = true;
                                                    continue;
                                                };
                                                let line = text.trim_end();
                                                if line.is_empty() {
                                                    continue;
                                                }
                                                if line.starts_with('{') && line.ends_with('}') {
                                                    lines.push(line.to_string());
                                                } else {
                                                    error!(
                                                        "discarding malformed terminated line ({} bytes); flagging desync",
                                                        line.len()
                                                    );
                                                    self.desynced = true;
                                                }
                                            } else {
                                                // Unterminated tail - buffer until the newline arrives.
                                                self.partial_line = segment.to_vec();
                                            }
                                        }

                                        // Bound the partial-line buffer. If the upstream goes wedged
                                        // mid-JSON (corrupt flush, mmap weirdness, multi-MB single line),
                                        // we'd otherwise grow `partial_line` until we OOM. Drop, flag the
                                        // data loss so the book re-syncs, and resync on the next newline.
                                        if self.partial_line.len() > MAX_PARTIAL_LINE_BYTES {
                                            error!(
                                                "partial_line exceeded {} bytes ({} bytes buffered); discarding and resyncing",
                                                MAX_PARTIAL_LINE_BYTES,
                                                self.partial_line.len()
                                            );
                                            self.partial_line.clear();
                                            self.desynced = true;
                                        }

                                        // Log result
                                        if count % 10_000 == 0 {
                                            log::debug!("on_modify #{}: returning {} lines", count, lines.len());
                                        }
                                    }
                                }
                                Err(err) => {
                                    error!("Read error: {}", err);
                                    read_failed = true;
                                }
                            }
                        }
                    }
                }
            }
            // Drop a handle that failed to read so the next call re-opens fresh.
            if read_failed {
                self.file = None;
            }
        }
        lines
    }

    /// Switch to a new file (on create event)
    fn on_create(&mut self, path: &PathBuf) -> Vec<String> {
        // A handle-less tracked file that vanished OR was recreated in place
        // (same path, new inode) is unrecoverable: everything past our last
        // read is gone, and a drain would open the NEW inode at a stale
        // offset. Flag desync and skip the drain. With a live handle neither
        // applies - the drain below reads the old (possibly deleted) inode to
        // EOF, so the old file is provably drained and the handoff is gapless.
        let open_id = self.open_file_id;
        let stale_handleless = self.file.is_none()
            && self.current_path.as_ref().is_some_and(|current| match current.metadata() {
                Err(_) => true, // vanished
                Ok(meta) => open_id.is_some_and(|id| id != file_id(&meta)), // recreated in place
            });
        if stale_handleless {
            warn!(
                "Tracked file {} vanished/recreated with no open handle; switching to {} and flagging desync",
                self.current_path.as_ref().expect("checked above").display(),
                path.display()
            );
            self.desynced = true;
        }

        // Drain the old file until it goes quiet: a single read raced the
        // node's final appends (anything written between the read and the
        // switch was silently lost). Each pass observes the size at read time,
        // so the loop ends only after a read that consumed no bytes. Progress,
        // not "returned no complete line", is the stop condition: a bounded
        // READ_CHUNK_BYTES read inside one long line advances the position and
        // returns nothing, and stopping there would clear `partial_line` below
        // and silently drop the rest of the old file (review 000212).
        let mut old_lines = Vec::new();
        if !stale_handleless {
            loop {
                let position_before = self.file_position;
                old_lines.extend(self.on_modify());
                if self.file_position == position_before {
                    break;
                }
            }
        }

        // Start tracking new file from beginning
        self.current_path = Some(path.clone());
        self.file = None;
        self.file_position = 0;
        self.partial_line.clear();
        self.open_file_id = None; // position 0 is valid in any inode; id set on open

        old_lines
    }

    /// Track an existing file (first event we see for it)
    fn start_tracking(&mut self, path: &PathBuf) {
        // Get current file size to start from end
        if let Ok(metadata) = std::fs::metadata(path) {
            self.file_position = metadata.len();
            self.open_file_id = Some(file_id(&metadata));
        } else {
            self.file_position = 0;
            self.open_file_id = None;
        }
        self.current_path = Some(path.clone());
        self.file = None;
        self.partial_line.clear();
    }
}

/// Spawn a file watcher thread for a single event source
/// Uses polling with inotify hints for streaming files
pub(super) fn spawn_file_watcher(
    source: EventSource,
    dir: PathBuf,
    tx: tokio::sync::mpsc::Sender<FileEvent>,
    last_event: Arc<AtomicU64>,
) -> thread::JoinHandle<()> {
    let source_name = match source {
        EventSource::OrderStatuses => "OrderStatuses",
        EventSource::Fills => "Fills",
        EventSource::OrderDiffs => "OrderDiffs",
        EventSource::OracleUpdates => "OracleUpdates",
    };

    thread::spawn(move || {
        info!("{} watcher thread started for {:?}", source_name, dir);

        let mut reader = FileReader::new(dir.clone());

        // Create watcher with callback
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let mut watcher = match recommended_watcher(move |res: Result<Event, _>| {
            drop(event_tx.send(res));
        }) {
            Ok(w) => w,
            Err(err) => {
                error!("{} watcher failed to create: {}", source_name, err);
                return;
            }
        };

        if let Err(err) = watcher.watch(&dir, RecursiveMode::Recursive) {
            error!("{} watcher failed to start: {}", source_name, err);
            return;
        }

        // HFT CRITICAL: Use fast polling (1ms) for lowest latency
        // inotify provides immediate notifications when available, but polling ensures we never wait
        let poll_interval = Duration::from_millis(1);

        // Main event loop - primarily event-driven with fallback polling
        let mut poll_count = 0u64;
        loop {
            poll_count += 1;

            // Wait for inotify events (with fallback timeout)
            match event_rx.recv_timeout(poll_interval) {
                Ok(Ok(event)) => {
                    if event.kind.is_create() || event.kind.is_modify() {
                        let path = &event.paths[0];
                        if path.is_dir() {
                            continue;
                        }

                        if event.kind.is_create() {
                            info!("{} new file: {:?}", source_name, path.file_name());
                            let old_lines = reader.on_create(path);
                            for line in old_lines {
                                let evt = match source {
                                    EventSource::OrderStatuses => FileEvent::OrderStatus(line),
                                    EventSource::OrderDiffs => FileEvent::OrderDiff(line),
                                    EventSource::Fills => FileEvent::Fill(line),
                                    EventSource::OracleUpdates => FileEvent::OracleUpdate(line),
                                };
                                if tx.blocking_send(evt).is_err() {
                                    error!("{} channel closed, exiting", source_name);
                                    return;
                                }
                            }
                        } else if reader.current_path.is_none() {
                            // First time seeing this file
                            info!("{} tracking: {:?}", source_name, path.file_name());
                            reader.start_tracking(path);
                        }

                        // EVENT-DRIVEN: Read data when inotify fires modify event
                        let lines = reader.on_modify();
                        observe_read_lag(source, &lines);
                        for line in lines {
                            let event = match source {
                                EventSource::OrderStatuses => FileEvent::OrderStatus(line),
                                EventSource::OrderDiffs => FileEvent::OrderDiff(line),
                                EventSource::Fills => FileEvent::Fill(line),
                                EventSource::OracleUpdates => FileEvent::OracleUpdate(line),
                            };

                            if tx.blocking_send(event).is_err() {
                                error!("{} channel closed, exiting", source_name);
                                return;
                            }

                            // Update health timestamp
                            last_event.store(now_unix_ms(), AtomicOrdering::Relaxed);
                        }
                    }
                }
                Ok(Err(err)) => {
                    error!("{} watcher error: {}", source_name, err);
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    // Fallback polling - safety net for missed events
                    // This runs every 500ms instead of every 10ms
                    let lines = reader.on_modify();
                    observe_read_lag(source, &lines);
                    for line in lines {
                        let event = match source {
                            EventSource::OrderStatuses => FileEvent::OrderStatus(line),
                            EventSource::OrderDiffs => FileEvent::OrderDiff(line),
                            EventSource::Fills => FileEvent::Fill(line),
                            EventSource::OracleUpdates => FileEvent::OracleUpdate(line),
                        };

                        if tx.blocking_send(event).is_err() {
                            error!("{} channel closed, exiting", source_name);
                            return;
                        }

                        last_event.store(now_unix_ms(), AtomicOrdering::Relaxed);
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    error!("{} event channel closed, exiting", source_name);
                    return;
                }
            }

            // If the reader had to discard buffered data, tell the listener so it
            // can re-sync the book from a fresh snapshot.
            if reader.take_desynced() && tx.blocking_send(FileEvent::Desync(source)).is_err() {
                error!("{source_name} channel closed, exiting");
                return;
            }

            // Every 100000 polls, log status
            if poll_count % 100_000 == 0 {
                if let Some(ref path) = reader.current_path {
                    if let Ok(file) = File::open(path) {
                        if let Ok(metadata) = file.metadata() {
                            log::debug!(
                                "{} poll {} - pos {} / size {}",
                                source_name,
                                poll_count,
                                reader.file_position,
                                metadata.len()
                            );
                        }
                    }
                }
            }

            // Every 10000 polls (~10 seconds), check for newer files (handles day rotation)
            if poll_count % 10_000 == 0 {
                if let Some(newer_file) = reader.check_for_newer_file() {
                    info!("{} detected newer file (day rotation?): {:?}", source_name, newer_file.file_name());
                    // Switch to the new file
                    let old_lines = reader.on_create(&newer_file);
                    for line in old_lines {
                        let evt = match source {
                            EventSource::OrderStatuses => FileEvent::OrderStatus(line),
                            EventSource::OrderDiffs => FileEvent::OrderDiff(line),
                            EventSource::Fills => FileEvent::Fill(line),
                            EventSource::OracleUpdates => FileEvent::OracleUpdate(line),
                        };
                        if tx.blocking_send(evt).is_err() {
                            error!("{} channel closed, exiting", source_name);
                            return;
                        }
                    }
                }
            }
        }
    })
}

/// Start the per-source file watcher threads (order statuses / diffs / fills /
/// oracle updates), returns receiver for events
/// Uses *_streaming directories (for --stream-with-block-info mode)
/// Each watcher tracks its stream from the end of the current file; nothing
/// already on disk is read (the snapshot handoff starts at a future checkpoint).
///
/// The watcher threads send straight into a tokio mpsc via `blocking_send` -
/// the old crossbeam channel + spawn_blocking bridge added a thread and a
/// queue hop per event for nothing.
pub(crate) fn start_parallel_file_watchers(
    data_dir: PathBuf,
) -> (
    tokio::sync::mpsc::Receiver<FileEvent>,
    Vec<thread::JoinHandle<()>>,
    Arc<AtomicU64>,
    Arc<AtomicU64>,
    Arc<AtomicU64>,
    Arc<AtomicU64>,
)
{
    // BOUNDED so a slow downstream actually back-pressures the file readers
    // (blocking_send parks until a slot frees up). Under processing stalls an
    // unbounded queue would accumulate multi-KB JSON strings indefinitely - a
    // primary OOM vector; the events sit on disk, no need to mirror them in
    // memory.
    let (tx, rx) = tokio::sync::mpsc::channel(10_000);
    let mut handles = Vec::new();

    // Health monitoring
    let last_order_status = Arc::new(AtomicU64::new(0));
    let last_fills = Arc::new(AtomicU64::new(0));
    let last_order_diffs = Arc::new(AtomicU64::new(0));

    // HFT mode uses streaming directories (for --stream-with-block-info)
    // Spawn watcher for OrderStatuses
    let order_statuses_dir = EventSource::OrderStatuses.event_source_dir_streaming(&data_dir);
    info!("OrderStatuses dir: {:?}", order_statuses_dir);
    handles.push(spawn_file_watcher(
        EventSource::OrderStatuses,
        order_statuses_dir,
        tx.clone(),
        last_order_status.clone(),
    ));

    // Spawn watcher for Fills
    let fills_dir = EventSource::Fills.event_source_dir_streaming(&data_dir);
    info!("Fills dir: {:?}", fills_dir);
    handles.push(spawn_file_watcher(EventSource::Fills, fills_dir, tx.clone(), last_fills.clone()));

    // Spawn watcher for OrderDiffs
    let order_diffs_dir = EventSource::OrderDiffs.event_source_dir_streaming(&data_dir);
    info!("OrderDiffs dir: {:?}", order_diffs_dir);
    handles.push(spawn_file_watcher(
        EventSource::OrderDiffs,
        order_diffs_dir,
        tx.clone(),
        last_order_diffs.clone(),
    ));

    // Spawn watcher for HIP-3 oracle updates (side stream: its
    // losses never mark the book desynced - see the Desync handling in mod.rs).
    let last_oracle = Arc::new(AtomicU64::new(0));
    let oracle_dir = EventSource::OracleUpdates.event_source_dir_streaming(&data_dir);
    info!("OracleUpdates dir: {:?}", oracle_dir);
    handles.push(spawn_file_watcher(EventSource::OracleUpdates, oracle_dir, tx, last_oracle.clone()));

    (rx, handles, last_order_status, last_fills, last_order_diffs, last_oracle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("obs_watcher_test_{}_{name}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn append(path: &PathBuf, data: &str) {
        let mut file = std::fs::OpenOptions::new().create(true).append(true).open(path).unwrap();
        file.write_all(data.as_bytes()).unwrap();
    }

    /// Replace `path` with a NEW inode holding `data` (write-to-tmp + rename,
    /// so the test can't be fooled by inode-number reuse).
    fn replace_file(path: &PathBuf, data: &str) {
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, data).unwrap();
        std::fs::rename(&tmp, path).unwrap();
    }

    #[test]
    fn test_on_modify_reads_appended_lines_and_buffers_partials() {
        let dir = test_dir("appended");
        let path = dir.join("0");
        append(&path, "");
        let mut reader = FileReader::new(dir.clone());
        reader.start_tracking(&path); // position = EOF (0 bytes so far)

        append(&path, "{\"a\":1}\n{\"b\":2}\n{\"c\":");
        let lines = reader.on_modify();
        assert_eq!(lines, vec!["{\"a\":1}".to_string(), "{\"b\":2}".to_string()]);

        // The unterminated tail was buffered, and the SAME cached handle picks up
        // the continuation on the next read (persistent-fd reuse path).
        append(&path, "3}\n");
        let lines = reader.on_modify();
        assert_eq!(lines, vec!["{\"c\":3}".to_string()]);
        assert!(!reader.take_desynced());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_start_tracking_existing_content_starts_at_eof() {
        let dir = test_dir("eof");
        let path = dir.join("0");
        append(&path, "{\"old\":1}\n");
        let mut reader = FileReader::new(dir.clone());
        reader.start_tracking(&path);
        assert!(reader.on_modify().is_empty(), "pre-existing content is skipped (covered by the snapshot)");
        append(&path, "{\"new\":2}\n");
        assert_eq!(reader.on_modify(), vec!["{\"new\":2}".to_string()]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_on_create_reads_old_tail_then_switches() {
        let dir = test_dir("rotate");
        let old = dir.join("0");
        let new = dir.join("1");
        append(&old, "");
        let mut reader = FileReader::new(dir.clone());
        reader.start_tracking(&old);

        append(&old, "{\"tail\":1}\n");
        append(&new, "{\"first\":2}\n");
        let old_lines = reader.on_create(&new);
        assert_eq!(old_lines, vec!["{\"tail\":1}".to_string()], "old file's tail is drained before switching");
        // After the switch the new file is read from position 0.
        assert_eq!(reader.on_modify(), vec!["{\"first\":2}".to_string()]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_malformed_terminated_line_is_discarded_not_buffered() {
        // Regression: a newline-TERMINATED line failing the JSON shape check
        // used to be stored into `partial_line` as if it were a partial tail,
        // then got prepended to the next read - corrupting the next valid line
        // too. It must be discarded (flagging desync) and later lines kept.
        let dir = test_dir("malformed_mid");
        let path = dir.join("0");
        append(&path, "");
        let mut reader = FileReader::new(dir.clone());
        reader.start_tracking(&path);

        append(&path, "{\"a\":1}\ngarbage\n{\"b\":2}\n");
        let lines = reader.on_modify();
        assert_eq!(lines, vec!["{\"a\":1}".to_string(), "{\"b\":2}".to_string()]);
        assert!(reader.take_desynced(), "a discarded complete line is data loss");

        // The garbage must NOT contaminate the next read.
        append(&path, "{\"c\":3}\n");
        assert_eq!(reader.on_modify(), vec!["{\"c\":3}".to_string()]);
        assert!(!reader.take_desynced());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_catchup_backlog_drained_in_bounded_chunks() {
        // A backlog larger than READ_CHUNK_BYTES must be drained one chunk per
        // on_modify call (never materialized whole), with the line split at
        // the chunk boundary surviving via partial_line. The second line is
        // multi-byte on purpose: the boundary lands mid-character, which the
        // byte-based buffering must tolerate (a String read would fail).
        let dir = test_dir("chunked_catchup");
        let path = dir.join("0");
        append(&path, "");
        let mut reader = FileReader::new(dir.clone());
        reader.start_tracking(&path);

        let ascii = "x".repeat(6 * 1024 * 1024); // even length: boundary is odd within the é-run
        let multibyte = "é".repeat(2 * 1024 * 1024);
        append(&path, &format!("{{\"a\":\"{ascii}\"}}\n{{\"b\":\"{multibyte}\"}}\n"));

        let first = reader.on_modify();
        assert_eq!(first.len(), 1, "first chunk yields only the first complete line");
        assert!(first[0].starts_with("{\"a\""));
        let second = reader.on_modify();
        assert_eq!(second.len(), 1, "the split line completes on the next chunk");
        assert!(second[0].starts_with("{\"b\"") && second[0].contains('é'));
        assert!(reader.on_modify().is_empty(), "backlog fully drained");
        assert!(!reader.take_desynced(), "a chunked catch-up is not data loss");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_partial_line_overflow_sets_desynced() {
        let dir = test_dir("overflow");
        let path = dir.join("0");
        append(&path, "");
        let mut reader = FileReader::new(dir.clone());
        reader.start_tracking(&path);

        // A single unterminated line larger than the cap must be discarded and
        // flagged as data loss (the listener re-syncs the book on this signal).
        // It arrives across several bounded READ_CHUNK_BYTES reads; the cap
        // trips once the buffered partial exceeds MAX_PARTIAL_LINE_BYTES.
        let huge = "{".repeat(MAX_PARTIAL_LINE_BYTES + 2);
        append(&path, &huge);
        let mut desynced = false;
        for _ in 0..8 {
            assert!(reader.on_modify().is_empty(), "an unterminated line never yields lines");
            if reader.take_desynced() {
                desynced = true;
                break;
            }
        }
        assert!(desynced, "discarding buffered data must flag a desync");
        assert!(!reader.take_desynced(), "the flag is drained by take_desynced");
        std::fs::remove_dir_all(&dir).ok();
    }

    // ==================== Rotation / recreate ====================

    fn bn_line(height: u64) -> String {
        format!("{{\"block_number\":{height},\"events\":[]}}")
    }

    #[test]
    fn test_vanished_tracked_file_force_switches_and_flags_desync() {
        // Node down long enough for retention to prune the tracked file: the
        // watcher must re-attach to the newest file via the polling fallback
        // (the old mtime comparison dead-locked on the deleted path forever)
        // and flag desync so the book re-syncs from a covering snapshot.
        let dir = test_dir("vanished");
        let day = dir.join("hourly").join("20240101");
        std::fs::create_dir_all(&day).unwrap();
        let old = day.join("0");
        append(&old, &format!("{}\n", bn_line(10)));
        let mut reader = FileReader::new(dir.clone());
        reader.start_tracking(&old);

        std::fs::remove_file(&old).unwrap();
        // Nothing to switch to yet: no crash, no desync, on_modify stays quiet.
        assert_eq!(reader.check_for_newer_file(), None);
        assert!(reader.on_modify().is_empty());
        assert!(!reader.take_desynced());

        // Node comes back and writes a new file: force-switch + desync flag.
        let new = day.join("1");
        append(&new, &format!("{}\n", bn_line(20)));
        assert_eq!(reader.check_for_newer_file(), Some(new.clone()));
        assert!(reader.take_desynced(), "skipping over a vanished file is potential data loss");

        // The switch itself works end-to-end: new file is read from the start.
        reader.on_create(&new);
        assert_eq!(reader.on_modify(), vec![bn_line(20)]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_create_after_vanish_with_open_handle_drains_tail_without_desync() {
        // Common recovery: the watcher holds the fd across the deletion, so
        // the deleted inode is still drained to EOF and the handoff to the
        // create-event file is gapless - no snapshot re-sync required.
        let dir = test_dir("vanish_fd_held");
        let day = dir.join("hourly").join("20240101");
        std::fs::create_dir_all(&day).unwrap();
        let old = day.join("0");
        append(&old, "");
        let mut reader = FileReader::new(dir.clone());
        reader.start_tracking(&old);
        append(&old, &format!("{}\n", bn_line(10)));
        assert_eq!(reader.on_modify(), vec![bn_line(10)]); // opens + holds the fd

        // A tail lands, then the file is pruned: the held fd still reads it.
        append(&old, &format!("{}\n", bn_line(11)));
        std::fs::remove_file(&old).unwrap();
        let new = day.join("1");
        append(&new, &format!("{}\n", bn_line(12)));
        assert_eq!(reader.on_create(&new), vec![bn_line(11)], "tail of the deleted inode is drained");
        assert!(!reader.take_desynced(), "a provably gapless handoff must not force a re-sync");
        assert_eq!(reader.on_modify(), vec![bn_line(12)]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_create_after_vanish_without_handle_flags_desync() {
        // No handle was ever held on the vanished file: whatever sat past
        // file_position is unrecoverable, so the create-event switch must
        // flag desync (this was the fast path the polling-only fix missed).
        let dir = test_dir("vanish_no_fd");
        let day = dir.join("hourly").join("20240101");
        std::fs::create_dir_all(&day).unwrap();
        let old = day.join("0");
        append(&old, &format!("{}\n", bn_line(10)));
        let mut reader = FileReader::new(dir.clone());
        reader.start_tracking(&old); // tracks position, opens no handle

        std::fs::remove_file(&old).unwrap();
        let new = day.join("1");
        append(&new, &format!("{}\n", bn_line(20)));
        assert!(reader.on_create(&new).is_empty(), "nothing to drain from the vanished file");
        assert!(reader.take_desynced(), "unread tail of the vanished file is unrecoverable");
        assert_eq!(reader.on_modify(), vec![bn_line(20)]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_same_path_recreate_without_handle_resets_and_flags_desync() {
        // Node catch-up writes by BLOCK time: after a long stop it recreates
        // the very hour file retention pruned - same path, new inode. With no
        // handle held and no create event, on_modify itself must notice the
        // inode swap, reset to the head of the new file and flag desync
        // (the old code silently read the new file at the stale offset).
        let dir = test_dir("recreate_no_fd");
        let day = dir.join("hourly").join("20240101");
        std::fs::create_dir_all(&day).unwrap();
        let path = day.join("12");
        append(&path, &format!("{}\n", bn_line(10)));
        let mut reader = FileReader::new(dir.clone());
        reader.start_tracking(&path); // stale position, no handle

        replace_file(&path, &format!("{}\n", bn_line(20)));
        assert_eq!(reader.on_modify(), vec![bn_line(20)], "new content is read from the start");
        assert!(reader.take_desynced(), "the old inode's unread tail is unrecoverable");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_same_path_recreate_with_held_handle_switches_gapless_via_poll() {
        // Same recreate, but the watcher holds the old fd and the create event
        // was missed: the polling fallback must detect the inode swap (the old
        // `latest == current -> None` never did), and the on_create handoff
        // drains the held fd - provably gapless, so NO desync.
        let dir = test_dir("recreate_fd_held");
        let day = dir.join("hourly").join("20240101");
        std::fs::create_dir_all(&day).unwrap();
        let path = day.join("12");
        append(&path, "");
        let mut reader = FileReader::new(dir.clone());
        reader.start_tracking(&path);
        append(&path, &format!("{}\n", bn_line(10)));
        assert_eq!(reader.on_modify(), vec![bn_line(10)]); // opens + holds the fd

        replace_file(&path, &format!("{}\n", bn_line(20)));
        assert!(reader.on_modify().is_empty(), "held fd still reads the drained old inode");
        assert_eq!(reader.check_for_newer_file(), Some(path.clone()), "inode swap must force a switch");
        assert!(reader.on_create(&path).is_empty(), "old inode was already drained");
        assert!(!reader.take_desynced(), "a fully drained handoff must not force a re-sync");
        assert_eq!(reader.on_modify(), vec![bn_line(20)]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_create_event_same_path_recreate_without_handle_flags_desync() {
        // The create event for the recreated path arrives while no handle is
        // held: the switch must flag desync and skip the drain (a drain would
        // read the NEW inode at the stale offset), then read from the start.
        let dir = test_dir("recreate_create_event");
        let day = dir.join("hourly").join("20240101");
        std::fs::create_dir_all(&day).unwrap();
        let path = day.join("12");
        append(&path, &format!("{}\n", bn_line(10)));
        let mut reader = FileReader::new(dir.clone());
        reader.start_tracking(&path); // stale position, no handle

        replace_file(&path, &format!("{}\n", bn_line(20)));
        assert!(reader.on_create(&path).is_empty(), "no drain through a stale offset");
        assert!(reader.take_desynced(), "the old inode's unread tail is unrecoverable");
        assert_eq!(reader.on_modify(), vec![bn_line(20)]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Review 000212: a bounded read that lands inside one long line returns no
    /// complete line; the rotation drain must keep reading until the position
    /// stops advancing, or the old file's tail is dropped without a desync.
    #[test]
    fn test_rotation_drains_a_line_spanning_bounded_reads_before_switching() {
        let dir = test_dir("rotation_partial_chunk");
        let old = dir.join("0");
        let new = dir.join("1");
        append(&old, "");
        let mut reader = FileReader::new(dir.clone());
        reader.start_tracking(&old);
        // The first complete row fills exactly one read; the next one spans two
        // reads (still below the partial-line cap); a short row follows.
        let first = format!("{{\"x\":\"{}\"}}\n", "a".repeat(READ_CHUNK_BYTES as usize - 9));
        assert_eq!(first.len(), READ_CHUNK_BYTES as usize);
        let second = format!("{{\"x\":\"{}\"}}\n", "b".repeat(9 * 1024 * 1024));
        append(&old, &(first + &second + "{\"tail\":1}\n"));
        append(&new, "{\"new\":1}\n");
        let drained = reader.on_create(&new);
        let loss_reported = reader.take_desynced();
        std::fs::remove_dir_all(&dir).unwrap();
        assert_eq!(drained.len(), 3, "all three old rows survive the switch");
        assert!(drained[2].contains("tail"));
        assert!(!loss_reported, "a complete drain is not data loss");
    }
}
