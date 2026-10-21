// HFT-optimized parallel file watcher
// Each event source runs on its own thread for maximum throughput

use crate::{
    metrics::{NODE_LINE_LAG, NODE_FILE_IO_ERRORS_TOTAL},
    types::node_data::{EventSource, line_local_time_unix_nanos},
};
use log::{error, info, warn};
use notify::{Event, RecursiveMode, Watcher, recommended_watcher};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    os::unix::fs::MetadataExt,
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
    thread,
    time::{Duration, Instant},
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

/// How often a watcher asked for read-through proofs tries one (design 000433 r3): each try scans the stream's
/// directory for a newer file.
const READ_THROUGH_INTERVAL: Duration = Duration::from_millis(5);

/// One read-through check (design 000433 r3, revision 1).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ReadThrough {
    /// Every line written to the stream before this instant has been returned by an earlier read.
    Proven(Instant),
    /// The check read new lines; they go out first and a later check may prove.
    Lines(Vec<String>),
    /// No proof now: no file tracked yet, a newer file or a recreated one, a read that failed, a buffered partial line
    /// or a pending discard.
    Unproven,
}

/// What one bounded read of the tracked file found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadStatus {
    /// The handle's size is the position: nothing was written since the last read.
    AtEnd,
    /// Bytes were read (possibly only part of a line).
    Read,
    /// Nothing was read and the end is not established: no tracked file, or opening, sizing, seeking or reading it
    /// failed (logged; a failed handle is dropped and the next read re-opens).
    NotRead,
}

/// The handle operations one read needs: `File` in production, a handle failing on demand in tests.
trait ReadHandle: Read + Seek {
    fn size(&self) -> std::io::Result<u64>;
}

impl ReadHandle for File {
    fn size(&self) -> std::io::Result<u64> {
        Ok(self.metadata()?.len())
    }
}

/// One read of at most `READ_CHUNK_BYTES` from `position` (so a catch-up backlog never pins itself in memory whole;
/// the caller loops right back for more), or `None` when the handle's size is the position. Sizing, seeking and
/// reading errors all come back as errors: none of them shows the end was reached (review 000434 r1 P2).
fn read_chunk(handle: &mut impl ReadHandle, position: u64) -> std::io::Result<Option<Vec<u8>>> {
    if handle.size()? <= position {
        return Ok(None);
    }
    handle.seek(SeekFrom::Start(position))?;
    let mut buf = Vec::new();
    Read::by_ref(handle).take(READ_CHUNK_BYTES).read_to_end(&mut buf)?;
    Ok(Some(buf))
}

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
fn observe_read_lag(label: &str, lines: &[String]) {
    if let Some(node_ns) = lines.last().and_then(|line| line_local_time_unix_nanos(line)) {
        NODE_LINE_LAG.with_label_values(&["read", label]).observe(seconds_since_node_write(node_ns));
    }
}

/// How a stream lays out its files below its directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileLayout {
    /// `hourly/<YYYYMMDD>/<H>`: the newest day by name, then its most recently modified file.
    Hourly,
    /// `<session>/<YYYYMMDD>/<first height>` (`replica_cmds`; a node restart opens a new session, named by its ISO start
    /// time): the newest session and day by name, then the greatest first height.
    ReplicaSessions,
}

impl FileLayout {
    /// Notification paths must name a data leaf, even when a short-lived path has vanished before delivery.
    /// A metadata `is_dir()` check alone treats a vanished directory as a file. At the 2026-10-21 node restart,
    /// `<stream>/20261021` stole the live hourly cursor; falling back to the hour then replayed its entire prefix.
    fn contains_file(self, base: &Path, path: &Path) -> bool {
        let Ok(relative) = path.strip_prefix(base) else { return false; };
        let mut parts = relative.components();
        let (Some(Component::Normal(first)), Some(Component::Normal(day)),
            Some(Component::Normal(leaf)), None) = (parts.next(), parts.next(), parts.next(), parts.next())
        else { return false; };
        let Some(day) = day.to_str() else { return false; };
        if day.len() != 8 || !day.bytes().all(|byte| byte.is_ascii_digit()) { return false; }
        let Some(leaf) = leaf.to_str() else { return false; };
        if leaf.is_empty() || !leaf.bytes().all(|byte| byte.is_ascii_digit()) { return false; }
        match self {
            Self::Hourly => first == "hourly" && leaf.parse::<u8>().is_ok_and(|hour| hour < 24),
            Self::ReplicaSessions => leaf.parse::<u64>().is_ok(),
        }
    }
}

/// The child directory with the greatest name.
fn newest_child_dir(dir: &Path) -> Option<PathBuf> {
    std::fs::read_dir(dir).ok()?.flatten().map(|entry| entry.path()).filter(|path| path.is_dir()).max()
}

/// See `FileLayout::ReplicaSessions`; names that are not a height are not stream files.
fn latest_replica_file(base_dir: &Path) -> Option<PathBuf> {
    let day_dir = newest_child_dir(&newest_child_dir(base_dir)?)?;
    std::fs::read_dir(day_dir)
        .ok()?
        .flatten()
        .filter_map(|entry| Some((entry.file_name().to_str()?.parse::<u64>().ok()?, entry.path())))
        .filter(|(_, path)| FileLayout::ReplicaSessions.contains_file(base_dir, path) && path.is_file())
        .max_by_key(|(first_height, _)| *first_height)
        .map(|(_, path)| path)
}

/// What the periodic file check found.
#[derive(Debug, PartialEq, Eq)]
enum FileCheck {
    /// Nothing is tracked yet: follow this file from its end.
    Adopt(PathBuf),
    /// A newer file than the tracked one: drain the tracked one, then read this one from its start.
    Switch(PathBuf),
}

/// Initial EOF is a byte offset, not necessarily a record boundary. Only the first adopted
/// record may be skipped; after its newline all records belong to the live stream.
#[derive(Clone, Copy, PartialEq, Eq)]
enum StartupBoundary {
    Unpositioned,
    AwaitingNewline,
    Aligned,
}

/// File reader state for a single source
struct FileReader {
    current_path: Option<PathBuf>,
    // Open handle to current_path, reused across reads. Re-opening per modify
    // event (plus per poll-timeout fallback) cost an open+stat syscall pair
    // thousands of times per second per watcher thread.
    file: Option<File>,
    file_position: u64,
    startup_boundary: StartupBoundary,
    // Unterminated tail of the last read, awaiting its newline. Raw bytes, not
    // String: a bounded read can split a multi-byte character at the chunk
    // boundary, which must not fail the whole read.
    partial_line: Vec<u8>,
    base_dir: PathBuf, // Base streaming directory to scan for new files
    // Set when buffered data had to be discarded (oversized partial line);
    // drained by take_desynced so the watcher can notify the listener.
    desynced: bool,
    // Failed opens and reads share an episode: reopening a bad reader is not recovery.
    io_failures: FileIoFailures,
    // (dev, ino) of the file that file_position refers to. Catches same-path
    // recreation: node catch-up writes by BLOCK time, so after a long stop it
    // recreates the very hour file retention pruned - same path, new inode,
    // where our position is meaningless.
    open_file_id: Option<(u64, u64)>,
    // Cap on one un-terminated line (MAX_PARTIAL_LINE_BYTES for the book streams).
    max_line_bytes: usize,
    layout: FileLayout,
}

/// File I/O retry diagnostics belong to the reader; failures never change its cursor or validity rules.
#[derive(Default)]
struct FileIoFailures {
    path: Option<PathBuf>,
    attempts: u64,
}
impl FileIoFailures {
    fn failed(&mut self, path: &Path, operation: &'static str, err: &std::io::Error) {
        NODE_FILE_IO_ERRORS_TOTAL.with_label_values(&[operation]).inc();
        if self.path.as_deref() != Some(path) {
            warn!("node stream {operation} unavailable at {} (retrying with the cursor retained): {err}", path.display());
            self.path = Some(path.to_path_buf());
            self.attempts = 0;
        }
        self.attempts += 1;
    }
    fn read_succeeded(&mut self, path: &Path) {
        if let Some(failed) = self.path.take() {
            info!("node stream I/O available at {} after {} failed attempts (previous failure at {})",
                path.display(), self.attempts, failed.display());
            self.attempts = 0;
        }
    }
}

/// (dev, ino) identity of a metadata handle.
fn file_id(meta: &std::fs::Metadata) -> (u64, u64) {
    (meta.dev(), meta.ino())
}

impl FileReader {
    fn new(base_dir: PathBuf, max_line_bytes: usize, layout: FileLayout) -> Self {
        Self {
            current_path: None,
            file: None,
            file_position: 0,
            startup_boundary: StartupBoundary::Aligned,
            partial_line: Vec::new(),
            base_dir,
            desynced: false,
            io_failures: FileIoFailures::default(),
            open_file_id: None,
            max_line_bytes,
            layout,
        }
    }

    /// True once if data was discarded since the last call.
    fn take_desynced(&mut self) -> bool {
        std::mem::take(&mut self.desynced)
    }

    /// Find the latest file in the streaming directory tree (see `FileLayout`).
    fn find_latest_file(&self) -> Option<PathBuf> {
        match self.layout {
            FileLayout::Hourly => self.latest_hourly_file(),
            FileLayout::ReplicaSessions => latest_replica_file(&self.base_dir),
        }
    }

    /// Scans hourly/YYYYMMDD/HH structure and returns the most recently modified file
    fn latest_hourly_file(&self) -> Option<PathBuf> {
        let day_dir = newest_child_dir(&self.base_dir.join("hourly"))?;

        // Find the latest hour file in this day
        let mut latest_file: Option<PathBuf> = None;
        let mut latest_mtime: Option<std::time::SystemTime> = None;

        if let Ok(entries) = std::fs::read_dir(&day_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if self.layout.contains_file(&self.base_dir, &path) && path.is_file() {
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

    /// The periodic file check: with nothing tracked yet the newest file is adopted at its end, like at a first modify
    /// event (bm 2026-10-21: misc events, quiet for the check's first 10 s, re-read a whole hour file as if created);
    /// else a newer file to switch to.
    fn periodic_check(&mut self) -> Option<FileCheck> {
        if self.current_path.is_none() {
            return self.find_latest_file().map(FileCheck::Adopt);
        }
        self.check_for_newer_file().map(FileCheck::Switch)
    }

    /// A newer file than the tracked one; `None` while nothing is tracked.
    fn check_for_newer_file(&mut self) -> Option<PathBuf> {
        let current = self.current_path.clone()?;
        let latest = self.find_latest_file()?;
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

    /// Filter inotify hints before they can replace a live cursor. Missing paths that do name a data file still
    /// reach the normal open/drain error handling, so a genuine lost file is not silently treated as harmless.
    fn notified_file<'a>(&self, event: &'a Event) -> Option<&'a PathBuf> {
        if !(event.kind.is_create() || event.kind.is_modify())
            || matches!(event.kind, notify::EventKind::Create(notify::event::CreateKind::Folder))
        { return None; }
        let path = event.paths.first()?;
        (self.layout.contains_file(&self.base_dir, path) && !path.is_dir()).then_some(path)
    }

    /// Process file modification - read new data and return lines
    fn on_modify(&mut self) -> Vec<String> {
        self.read_lines().0
    }

    /// One bounded read of the tracked file from the position: its complete lines (only with `ReadStatus::Read`) and
    /// what the read found.
    fn read_lines(&mut self) -> (Vec<String>, ReadStatus) {
        static MODIFY_COUNT: AtomicU64 = AtomicU64::new(0);
        let count = MODIFY_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let mut lines = Vec::new();
        if self.startup_boundary == StartupBoundary::Unpositioned && !self.position_at_end() {
            return (lines, ReadStatus::NotRead);
        }
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
                                self.startup_boundary = StartupBoundary::Aligned;
                            }
                            self.open_file_id = Some(id);
                        }
                        self.file = Some(file);
                        // Only a successful read or EOF clears an I/O failure episode.
                    }
                    Err(err) => {
                        self.io_failures.failed(path, "open", &err);
                        return (lines, ReadStatus::NotRead);
                    }
                }
            }
            let Some(file) = self.file.as_mut() else {
                return (lines, ReadStatus::NotRead);
            };
            let read = read_chunk(file, self.file_position);
            if read.is_ok() { self.io_failures.read_succeeded(path); }
            let buf = match read {
                Ok(Some(buf)) if !buf.is_empty() => buf,
                Ok(Some(_)) => return (lines, ReadStatus::NotRead),
                Ok(None) => return (lines, ReadStatus::AtEnd),
                Err(err) => {
                    // drop the failed handle so the next call re-opens fresh
                    self.io_failures.failed(path, "read", &err);
                    self.file = None;
                    return (lines, ReadStatus::NotRead);
                }
            };
            // Update position
            let bytes_read = buf.len();
            self.file_position += bytes_read as u64;

            // Discard the remainder of the record that was already in flight at adoption.
            // Do not buffer it: it can span arbitrarily many bounded reads. Until its newline
            // arrives, an EOF cannot prove a fully consumed record boundary.
            let live_start = if self.startup_boundary == StartupBoundary::AwaitingNewline {
                let Some(newline) = buf.iter().position(|&b| b == b'\n') else {
                    return (lines, ReadStatus::Read);
                };
                self.startup_boundary = StartupBoundary::Aligned;
                info!("node stream startup boundary reached path={} inode={:?} offset={}",
                    path.display(), self.open_file_id, self.file_position - bytes_read as u64 + newline as u64 + 1);
                newline + 1
            } else { 0 };
            let mut segment_offset = self.file_position - bytes_read as u64 + live_start as u64
                - self.partial_line.len() as u64;
            let mut full_buf = std::mem::take(&mut self.partial_line);
            full_buf.extend_from_slice(&buf[live_start..]);

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
                let start_offset = segment_offset;
                segment_offset += segment.len() as u64;
                if segment.last() == Some(&b'\n') {
                    let Ok(text) = std::str::from_utf8(segment) else {
                        error!(
                            "discarding non-UTF-8 terminated line; flagging desync path={} inode={:?} bytes={} offsets={}..{}",
                            path.display(), self.open_file_id, segment.len(), start_offset, segment_offset
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
                            "discarding malformed terminated line; flagging desync path={} inode={:?} bytes={} offsets={}..{}",
                            path.display(), self.open_file_id, segment.len(), start_offset, segment_offset
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
            if self.partial_line.len() > self.max_line_bytes {
                error!(
                    "partial_line exceeded limit; discarding and resyncing path={} inode={:?} limit={} bytes={} offsets={}..{}",
                    path.display(), self.open_file_id, self.max_line_bytes, self.partial_line.len(),
                    self.file_position - self.partial_line.len() as u64, self.file_position
                );
                self.partial_line.clear();
                self.desynced = true;
            }

            // Log result
            if count % 10_000 == 0 {
                log::debug!("on_modify #{}: returning {} lines", count, lines.len());
            }
            return (lines, ReadStatus::Read);
        }
        (lines, ReadStatus::NotRead)
    }

    /// Switch to a new file (on create event), first handing the old file's remaining lines to `deliver` one bounded
    /// read at a time, so the drain holds one read plus one line however much is left (review 000429 r1 P2).
    /// `deliver` returns false once the consumer is gone: the switch is then abandoned and false returned.
    fn on_create(&mut self, path: &PathBuf, mut deliver: impl FnMut(Vec<String>) -> bool) -> bool {
        // Polling can discover a file before its queued create notification is handled. A second notice for the
        // same inode must preserve the cursor and any partial line, including after a read-through proof. A
        // same-path replacement still takes the drain/loss path below: path equality alone is not file identity.
        if self.current_path.as_ref() == Some(path) {
            let same_inode = self.open_file_id.is_some_and(|open_id| {
                path.metadata().is_ok_and(|meta| file_id(&meta) == open_id)
            });
            // on_create selected this path but no read/open has happened yet; there is no consumed position to reset.
            let unopened = self.open_file_id.is_none() && self.file.is_none()
                && self.file_position == 0 && self.partial_line.is_empty();
            if same_inode || unopened {
                return true;
            }
        }

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
        // so the loop ends only at a read that established the end. Bytes read,
        // not "returned no complete line", keep it going: a bounded
        // READ_CHUNK_BYTES read inside one long line returns nothing, and
        // stopping there would drop the rest of the old file (review 000212).
        // A read that failed leaves the old tail unread, and a partial line
        // left at the end is dropped by the switch: either is a loss
        // (review 000434 r2 P2). Before the first file there is nothing to drain.
        if let Some(old) = self.current_path.clone().filter(|_| !stale_handleless) {
            loop {
                let (lines, status) = self.read_lines();
                if !deliver(lines) {
                    return false;
                }
                match status {
                    ReadStatus::Read => {}
                    ReadStatus::AtEnd => break,
                    ReadStatus::NotRead => {
                        warn!("Could not drain {}; switching to {} and flagging desync", old.display(), path.display());
                        self.desynced = true;
                        break;
                    }
                }
            }
            if !self.partial_line.is_empty() || self.startup_boundary != StartupBoundary::Aligned {
                warn!(
                    "{} has no complete record boundary (buffered={} startup_pending={}); switching to {} and flagging desync",
                    old.display(), self.partial_line.len(), self.startup_boundary != StartupBoundary::Aligned, path.display()
                );
                self.desynced = true;
            }
        }

        // Start tracking new file from beginning
        self.current_path = Some(path.clone());
        self.file = None;
        self.file_position = 0;
        self.startup_boundary = StartupBoundary::Aligned;
        self.partial_line.clear();
        self.open_file_id = None; // position 0 is valid in any inode; id set on open
        true
    }

    /// Whether every line written before the check began has been returned (design 000433 r3, revision 1). The
    /// instant is taken first; then the newest file must be the tracked one with the inode the position refers to, and
    /// a read from the position must establish the end (the handle's size is the position: sizing, seeking or reading
    /// that fails proves nothing), leave no partial line and flag no discard.
    /// Anything appended before that instant is in the tracked file (the read would have returned it) or in a newer
    /// file (the scan would have seen it), so a proof covers every line written before its instant. A read that
    /// returns lines, or one cut at `READ_CHUNK_BYTES`, proves nothing yet.
    fn read_through(&mut self) -> ReadThrough {
        let at = Instant::now();
        let Some(current) = self.current_path.clone() else {
            return ReadThrough::Unproven;
        };
        let recreated = current.metadata().ok().map(|meta| file_id(&meta)) != self.open_file_id;
        if recreated || self.find_latest_file().as_ref() != Some(&current) {
            return ReadThrough::Unproven;
        }
        match self.read_lines() {
            (lines, ReadStatus::Read) => ReadThrough::Lines(lines),
            (_, ReadStatus::AtEnd) if self.partial_line.is_empty() && !self.desynced
                && self.startup_boundary == StartupBoundary::Aligned => ReadThrough::Proven(at),
            (_, ReadStatus::AtEnd | ReadStatus::NotRead) => ReadThrough::Unproven,
        }
    }

    /// Adopt the current byte EOF using one handle for identity, length and boundary inspection.
    /// Failed positioning is retried by the normal reader; it never becomes a read-through proof.
    fn start_tracking(&mut self, path: &PathBuf) {
        self.current_path = Some(path.clone());
        self.file = None;
        self.file_position = 0;
        self.open_file_id = None;
        self.partial_line.clear();
        self.startup_boundary = StartupBoundary::Unpositioned;
        self.position_at_end();
    }

    fn position_at_end(&mut self) -> bool {
        let Some(path) = self.current_path.as_ref() else { return false; };
        let positioning = (|| -> std::io::Result<_> {
            let mut file = File::open(path)?;
            let metadata = file.metadata()?;
            let position = metadata.len();
            let mut last = [b'\n'];
            if position > 0 {
                file.seek(SeekFrom::Start(position - 1))?;
                file.read_exact(&mut last)?;
            }
            Ok((position, file_id(&metadata), last[0] == b'\n'))
        })();
        match positioning {
            Ok((position, id, aligned)) => {
                self.file_position = position;
                self.open_file_id = Some(id);
                self.startup_boundary = if aligned { StartupBoundary::Aligned } else { StartupBoundary::AwaitingNewline };
                self.io_failures.read_succeeded(path);
                info!("node stream startup positioning path={} inode={id:?} offset={position} awaiting_newline={}",
                    path.display(), !aligned);
                true
            }
            Err(err) => {
                self.io_failures.failed(path, "position", &err);
                false
            }
        }
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
    let name = match source {
        EventSource::OrderStatuses => "OrderStatuses",
        EventSource::Fills => "Fills",
        EventSource::OrderDiffs => "OrderDiffs",
        EventSource::OracleUpdates => "OracleUpdates",
    };
    // The book's start contract is a future snapshot, so the lines skipped while positioning are not a loss to it.
    let stream = LineStream {
        name,
        lag_label: source.metric_label(),
        dir,
        max_line_bytes: MAX_PARTIAL_LINE_BYTES,
        start_skip_is_loss: false,
        layout: FileLayout::Hourly,
    };
    spawn_line_watcher(stream, tx, Some(last_event), move |line| file_event(source, line), move |_| FileEvent::Desync(source), None)
}

const fn file_event(source: EventSource, line: String) -> FileEvent {
    match source {
        EventSource::OrderStatuses => FileEvent::OrderStatus(line),
        EventSource::OrderDiffs => FileEvent::OrderDiff(line),
        EventSource::Fills => FileEvent::Fill(line),
        EventSource::OracleUpdates => FileEvent::OracleUpdate(line),
    }
}

/// Wraps a proven read-through instant for the channel.
pub(crate) type ReadThroughMark<E> = Box<dyn Fn(Instant) -> E + Send>;

/// Both withdraw state until an anchor covers them, but initial EOF positioning is expected initialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WatchLoss { Positioning, Discard }

/// One newline-delimited node stream for `spawn_line_watcher`.
pub(crate) struct LineStream {
    /// Log name.
    pub(crate) name: &'static str,
    /// `NODE_LINE_LAG` stream label.
    pub(crate) lag_label: &'static str,
    /// Streaming directory, its files laid out as `layout` says.
    pub(crate) dir: PathBuf,
    /// Cap on one un-terminated line; a longer one is discarded and `desync` is sent.
    pub(crate) max_line_bytes: usize,
    /// The watcher positions at the end of the current file on its first modify event, skipping the content written
    /// before that moment (including the line that triggered it); when set, it sends `desync` right then.
    pub(crate) start_skip_is_loss: bool,
    /// Which file below `dir` is the newest one.
    pub(crate) layout: FileLayout,
}

/// One read-through attempt: lines it read go out through `send`, a proven instant goes out as `mark(at)` after them.
/// False once the consumer is gone.
fn send_read_through<E>(
    reader: &mut FileReader,
    send: &impl Fn(Vec<String>) -> bool,
    tx: &tokio::sync::mpsc::Sender<E>,
    mark: &ReadThroughMark<E>,
    name: &str,
) -> bool {
    match reader.read_through() {
        ReadThrough::Lines(lines) => send(lines),
        ReadThrough::Proven(at) => tx.blocking_send(mark(at)).inspect_err(|_| info!("{name} consumer closed, exiting")).is_ok(),
        ReadThrough::Unproven => true,
    }
}

/// Watch one node stream from the end of its current file, sending every complete line wrapped by `wrap`
/// (blocking on a full channel, so a slow consumer back-pressures the reader) and `desync()` whenever buffered data
/// had to be discarded; `last_event` (unix ms of the last line sent) feeds the book's health monitor. The thread
/// returns when the channel closes (checked every poll) or the inotify watch cannot be set up. With `read_through`,
/// every `READ_THROUGH_INTERVAL` the reader tries a read-through proof (`FileReader::read_through`) and sends a proven
/// instant in line with the data, after every line read before it (design 000433 r3).
pub(crate) fn spawn_line_watcher<E, F, D>(
    stream: LineStream,
    tx: tokio::sync::mpsc::Sender<E>,
    last_event: Option<Arc<AtomicU64>>,
    wrap: F,
    desync: D,
    read_through: Option<ReadThroughMark<E>>,
) -> thread::JoinHandle<()>
where
    E: Send + 'static,
    F: Fn(String) -> E + Send + 'static,
    D: Fn(WatchLoss) -> E + Send + 'static,
{
    let LineStream { name, lag_label, dir, max_line_bytes, start_skip_is_loss, layout } = stream;
    thread::spawn(move || {
        info!("{} watcher thread started for {:?}", name, dir);

        let mut reader = FileReader::new(dir.clone(), max_line_bytes, layout);
        // Every read's lines go out here, in order, blocking while the channel is full; false once the consumer is gone.
        let send = |lines: Vec<String>| {
            observe_read_lag(lag_label, &lines);
            for line in lines {
                if tx.blocking_send(wrap(line)).is_err() {
                    info!("{name} consumer closed, exiting");
                    return false;
                }
                // Update health timestamp
                if let Some(last_event) = &last_event {
                    last_event.store(now_unix_ms(), AtomicOrdering::Relaxed);
                }
            }
            true
        };
        // A file already on disk is followed from its end: nothing written before the watcher started is read, and the
        // skip is a loss where the stream says so. False once the consumer is gone.
        let adopt_at_end = |reader: &mut FileReader, path: &PathBuf| {
            info!("{} tracking: {:?}", name, path.file_name());
            reader.start_tracking(path);
            !(start_skip_is_loss && tx.blocking_send(desync(WatchLoss::Positioning)).is_err())
        };

        // Create watcher with callback
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let mut watcher = match recommended_watcher(move |res: Result<Event, _>| {
            drop(event_tx.send(res));
        }) {
            Ok(w) => w,
            Err(err) => {
                error!("{} watcher failed to create: {}", name, err);
                return;
            }
        };

        if let Err(err) = watcher.watch(&dir, RecursiveMode::Recursive) {
            error!("{} watcher failed to start: {}", name, err);
            return;
        }

        // HFT CRITICAL: Use fast polling (1ms) for lowest latency
        // inotify provides immediate notifications when available, but polling ensures we never wait
        let poll_interval = Duration::from_millis(1);

        // Main event loop - primarily event-driven with fallback polling
        let mut poll_count = 0u64;
        let mut last_read_through: Option<Instant> = None;
        loop {
            poll_count += 1;
            // The consumer is gone: stop now rather than at the next line (a quiet stream may have none for long).
            if tx.is_closed() {
                info!("{name} channel closed, exiting");
                return;
            }

            // Wait for inotify events (with fallback timeout)
            match event_rx.recv_timeout(poll_interval) {
                Ok(Ok(event)) => {
                    if let Some(path) = reader.notified_file(&event) {
                        if event.kind.is_create() {
                            info!("{} new file: {:?}", name, path.file_name());
                            if !reader.on_create(path, &send) {
                                return;
                            }
                        } else if reader.current_path.is_none() && !adopt_at_end(&mut reader, path) {
                            info!("{name} consumer closed, exiting");
                            return;
                        }

                        // EVENT-DRIVEN: Read data when inotify fires modify event
                        if !send(reader.on_modify()) {
                            return;
                        }
                    }
                }
                Ok(Err(err)) => {
                    error!("{} watcher error: {}", name, err);
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    // Fallback polling - safety net for missed events
                    // This runs every 500ms instead of every 10ms
                    if !send(reader.on_modify()) {
                        return;
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    error!("{} event channel closed, exiting", name);
                    return;
                }
            }

            // If the reader had to discard buffered data, tell the listener so it
            // can re-sync the book from a fresh snapshot.
            if reader.take_desynced() && tx.blocking_send(desync(WatchLoss::Discard)).is_err() {
                info!("{name} consumer closed, exiting");
                return;
            }

            if let Some(mark) = &read_through
                && last_read_through.is_none_or(|tried| tried.elapsed() >= READ_THROUGH_INTERVAL)
            {
                last_read_through = Some(Instant::now());
                if !send_read_through(&mut reader, &send, &tx, mark, name) {
                    return;
                }
            }

            // Every 100000 polls, log status
            if poll_count % 100_000 == 0 {
                if let Some(ref path) = reader.current_path {
                    if let Ok(file) = File::open(path) {
                        if let Ok(metadata) = file.metadata() {
                            log::debug!(
                                "{} poll {} - pos {} / size {}",
                                name,
                                poll_count,
                                reader.file_position,
                                metadata.len()
                            );
                        }
                    }
                }
            }

            // Every 10000 polls (~10 seconds): adopt the newest file if no modify event came yet, else check for a
            // newer file (handles day rotation)
            if poll_count % 10_000 == 0 {
                match reader.periodic_check() {
                    Some(FileCheck::Adopt(path)) => {
                        if !adopt_at_end(&mut reader, &path) {
                            info!("{name} consumer closed, exiting");
                            return;
                        }
                    }
                    Some(FileCheck::Switch(newer_file)) => {
                        info!("{} detected newer file (day rotation?): {:?}", name, newer_file.file_name());
                        if !reader.on_create(&newer_file, &send) {
                            return;
                        }
                    }
                    None => {}
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

    fn test_stream(dir: PathBuf, start_skip_is_loss: bool) -> LineStream {
        let max_line_bytes = MAX_PARTIAL_LINE_BYTES;
        LineStream { name: "Test", lag_label: "test", dir, max_line_bytes, start_skip_is_loss, layout: FileLayout::Hourly }
    }

    /// `on_create` with a consumer that takes every delivery.
    fn switch_collecting(reader: &mut FileReader, path: &PathBuf) -> Vec<String> {
        let mut lines = Vec::new();
        assert!(reader.on_create(path, |delivered| {
            lines.extend(delivered);
            true
        }));
        lines
    }

    /// The skip of the content present at the first modify event is reported only when asked for (review 000422 r2).
    #[test]
    fn test_line_watcher_reports_the_positioning_skip_only_when_asked() {
        for start_skip_is_loss in [true, false] {
            let dir = test_dir(&format!("start_skip_{start_skip_is_loss}"));
            let path = hourly_file(&dir, "20261021", "10");
            append(&path, "{\"old\":0}\n");
            let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(8);
            let stream = test_stream(dir, start_skip_is_loss);
            let _watcher = spawn_line_watcher(stream, tx, None, |line| line, |_| "DESYNC".to_string(), None);
            thread::sleep(Duration::from_millis(200));
            append(&path, "{\"skipped\":1}\n");
            thread::sleep(Duration::from_millis(200));
            append(&path, "{\"read\":2}\n");
            let deadline = Instant::now() + Duration::from_secs(2);
            let mut received = Vec::new();
            while !received.contains(&"{\"read\":2}".to_string()) && Instant::now() < deadline {
                match rx.try_recv() {
                    Ok(line) => received.push(line),
                    Err(_) => thread::sleep(Duration::from_millis(5)),
                }
            }
            let expected: Vec<String> =
                if start_skip_is_loss { vec!["DESYNC".into(), "{\"read\":2}".into()] } else { vec!["{\"read\":2}".into()] };
            assert_eq!(received, expected, "start_skip_is_loss = {start_skip_is_loss}");
        }
    }

    /// Review 000422 P3-1: a quiet stream must not keep a watcher whose consumer is gone alive.
    #[test]
    fn test_line_watcher_exits_once_its_consumer_is_gone_without_new_lines() {
        let dir = test_dir("consumer_gone");
        let (tx, rx) = tokio::sync::mpsc::channel::<String>(1);
        let stream = test_stream(dir, false);
        let handle = spawn_line_watcher(stream, tx, None, |line| line, |_| String::new(), None);
        drop(rx);
        let deadline = Instant::now() + Duration::from_secs(2);
        while !handle.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(handle.is_finished(), "the watcher should notice the closed channel within a poll");
    }

    /// `hourly/<day>/<hour>` below `dir` (the layout the newest-file scan reads).
    fn hourly_file(dir: &Path, day: &str, hour: &str) -> PathBuf {
        let day_dir = dir.join("hourly").join(day);
        std::fs::create_dir_all(&day_dir).unwrap();
        day_dir.join(hour)
    }

    #[test]
    fn test_read_through_proves_only_a_drained_file_without_a_partial_line() {
        let dir = test_dir("read_through");
        let path = hourly_file(&dir, "20261020", "5");
        append(&path, "{\"old\":0}\n");
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        assert_eq!(reader.read_through(), ReadThrough::Unproven, "no file tracked yet");
        reader.start_tracking(&path);
        assert!(reader.on_modify().is_empty(), "opens the handle at the end");
        let before = Instant::now();
        let ReadThrough::Proven(at) = reader.read_through() else { panic!("a drained newest file proves") };
        assert!(at >= before, "the instant is taken by the check");

        append(&path, "{\"a\":1}\n{\"b\":");
        assert_eq!(reader.read_through(), ReadThrough::Lines(vec!["{\"a\":1}".to_string()]), "new lines go out first");
        assert_eq!(reader.read_through(), ReadThrough::Unproven, "a partial line is buffered");
        append(&path, "2}\n");
        assert_eq!(reader.read_through(), ReadThrough::Lines(vec!["{\"b\":2}".to_string()]));
        assert!(matches!(reader.read_through(), ReadThrough::Proven(_)));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Fault {
        Size,
        Seek,
        Read,
    }

    /// A handle over `data` whose `fault` operation fails.
    struct FaultyHandle {
        data: std::io::Cursor<Vec<u8>>,
        fault: Option<Fault>,
    }

    impl FaultyHandle {
        fn fail(&self, operation: Fault) -> std::io::Result<()> {
            if self.fault == Some(operation) { Err(std::io::Error::other(format!("injected {operation:?}"))) } else { Ok(()) }
        }
    }

    impl Read for FaultyHandle {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.fail(Fault::Read)?;
            self.data.read(buf)
        }
    }

    impl Seek for FaultyHandle {
        fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
            self.fail(Fault::Seek)?;
            self.data.seek(position)
        }
    }

    impl ReadHandle for FaultyHandle {
        fn size(&self) -> std::io::Result<u64> {
            self.fail(Fault::Size)?;
            Ok(self.data.get_ref().len() as u64)
        }
    }

    /// Review 000434 r1 P2: sizing, seeking and reading failures all come back as errors, never as the end.
    #[test]
    fn test_read_chunk_reports_every_failed_operation_with_a_line_unread() {
        let line = b"{\"block\":1}\n".to_vec();
        for fault in [Fault::Size, Fault::Seek, Fault::Read] {
            let mut handle = FaultyHandle { data: std::io::Cursor::new(line.clone()), fault: Some(fault) };
            let err = read_chunk(&mut handle, 0).expect_err("a failed operation is an error");
            assert_eq!(err.to_string(), format!("injected {fault:?}"));
        }
        let mut sound = FaultyHandle { data: std::io::Cursor::new(line.clone()), fault: None };
        assert_eq!(read_chunk(&mut sound, 0).unwrap(), Some(line.clone()));
        assert_eq!(read_chunk(&mut sound, line.len() as u64).unwrap(), None, "the end");
        let mut unsized_at_end = FaultyHandle { data: std::io::Cursor::new(line.clone()), fault: Some(Fault::Size) };
        assert!(read_chunk(&mut unsized_at_end, line.len() as u64).is_err(), "no size, no end");
    }

    /// Review 000434 r1 P2, through the reader: a failed read with a line unread proves nothing; the reader re-opens,
    /// delivers the line, and only then proves.
    #[test]
    fn test_read_through_after_a_failed_read_delivers_the_line_before_proving() {
        let dir = test_dir("read_through_failed_read");
        let path = hourly_file(&dir, "20261020", "5");
        append(&path, "{\"old\":0}\n");
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        reader.start_tracking(&path);
        assert!(reader.on_modify().is_empty(), "opens the handle at the end");
        // a handle that sizes and seeks but cannot read
        reader.file = Some(std::fs::OpenOptions::new().append(true).open(&path).unwrap());
        append(&path, "{\"block\":1}\n");
        assert_eq!(reader.read_through(), ReadThrough::Unproven, "the read failed");
        assert!(reader.file.is_none(), "the failed handle is dropped");
        assert_eq!(reader.read_through(), ReadThrough::Lines(vec!["{\"block\":1}".to_string()]), "re-opened");
        assert!(matches!(reader.read_through(), ReadThrough::Proven(_)));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Review 000433 r2 P1: the node already wrote the next block into the next hour's file while the reader sits at
    /// the end of the old one; that end proves nothing until the reader switched and drained the new file.
    #[test]
    fn test_read_through_is_unproven_while_a_newer_file_waits() {
        let dir = test_dir("read_through_rotation");
        let old = hourly_file(&dir, "20261020", "5");
        append(&old, "{\"old\":0}\n");
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        reader.start_tracking(&old);
        assert!(reader.on_modify().is_empty(), "opens the handle at the end");
        assert!(matches!(reader.read_through(), ReadThrough::Proven(_)));
        thread::sleep(Duration::from_millis(20)); // a later mtime for the new file
        let new = hourly_file(&dir, "20261020", "6");
        append(&new, "{\"block\":1}\n");
        assert_eq!(reader.read_through(), ReadThrough::Unproven, "the old file's end proves nothing");
        assert_eq!(switch_collecting(&mut reader, &new), Vec::<String>::new());
        assert_eq!(reader.read_through(), ReadThrough::Unproven, "no handle on the new file yet");
        assert_eq!(reader.on_modify(), vec!["{\"block\":1}".to_string()]);
        assert!(matches!(reader.read_through(), ReadThrough::Proven(_)));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The watcher sends a proven instant after every line read before it, in line with the data.
    #[test]
    fn test_line_watcher_sends_read_through_after_the_lines() {
        let dir = test_dir("read_through_watcher");
        let path = hourly_file(&dir, "20261020", "5");
        append(&path, "");
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(64);
        let mark: ReadThroughMark<String> = Box::new(|_| "PROVEN".to_string());
        let _watcher = spawn_line_watcher(test_stream(dir, false), tx, None, |line| line, |_| "DESYNC".to_string(), Some(mark));
        thread::sleep(Duration::from_millis(200));
        append(&path, "{\"positioned\":0}\n");
        thread::sleep(Duration::from_millis(200));
        append(&path, "{\"a\":1}\n");
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut received = Vec::new();
        while Instant::now() < deadline {
            match rx.try_recv() {
                Ok(item) => received.push(item),
                Err(_) => thread::sleep(Duration::from_millis(5)),
            }
            if received.iter().position(|item| item == "{\"a\":1}").is_some_and(|line| received[line..].contains(&"PROVEN".to_string())) {
                break;
            }
        }
        let line = received.iter().position(|item| item == "{\"a\":1}").expect("the line arrives");
        assert!(received[line..].iter().any(|item| item == "PROVEN"), "a proof follows the line: {received:?}");
    }

    #[test]
    fn test_on_modify_reads_appended_lines_and_buffers_partials() {
        let dir = test_dir("appended");
        let path = dir.join("0");
        append(&path, "");
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
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

    /// bm 2026-10-21 00:54Z: the misc events watcher saw no modify event before its first periodic check, which treated
    /// the newest hour file as created and re-read it (61 MB, 4,413 already released blocks). Untracked, the check adopts
    /// the newest file at its end; once tracking, a newer file is a switch.
    #[test]
    fn test_periodic_check_adopts_an_untracked_file_at_its_end() {
        let dir = test_dir("periodic_adopt");
        let current = hourly_file(&dir, "20261021", "0");
        append(&current, "{\"old\":0}\n");
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        assert_eq!(reader.periodic_check(), Some(FileCheck::Adopt(current.clone())));
        reader.start_tracking(&current);
        append(&current, "{\"new\":1}\n");
        assert_eq!(reader.on_modify(), vec!["{\"new\":1}".to_string()], "the content before adoption is not read");
        assert_eq!(reader.periodic_check(), None);
        thread::sleep(Duration::from_millis(20));
        let next = hourly_file(&dir, "20261021", "1");
        append(&next, "{\"next\":2}\n");
        assert_eq!(reader.periodic_check(), Some(FileCheck::Switch(next)));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_startup_partial_record_waits_for_boundary_without_corruption() {
        let dir = test_dir("startup_partial_boundary");
        let path = hourly_file(&dir, "20261021", "16");
        append(&path, "{\"in_progress\":\"");
        let mut reader = FileReader::new(dir.clone(), 8, FileLayout::Hourly);
        reader.start_tracking(&path);
        assert_eq!(reader.read_through(), ReadThrough::Unproven);
        for _ in 0..3 {
            append(&path, "a long fragment beyond the buffer limit");
            assert!(reader.on_modify().is_empty());
            assert!(reader.partial_line.is_empty());
            assert!(!reader.take_desynced());
            assert_eq!(reader.read_through(), ReadThrough::Unproven);
        }
        // A duplicate create must not forget that the initial record is incomplete.
        assert!(switch_collecting(&mut reader, &path).is_empty());
        append(&path, "\"}\n{\"next\":1}\n");
        assert_eq!(reader.on_modify(), vec!["{\"next\":1}".to_string()]);
        assert!(!reader.take_desynced());
        assert!(matches!(reader.read_through(), ReadThrough::Proven(_)));
        append(&path, "truly corrupt\n{\"next\":2}\n");
        assert_eq!(reader.on_modify(), vec!["{\"next\":2}".to_string()]);
        assert!(reader.take_desynced(), "only the startup fragment may be skipped");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_startup_boundary_does_not_leak_across_rotation_or_replacement() {
        for replace in [false, true] {
            let dir = test_dir(if replace { "startup_replace" } else { "startup_rotate" });
            let path = hourly_file(&dir, "20261021", "16");
            append(&path, "{\"partial\":");
            let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
            reader.start_tracking(&path);
            if replace {
                replace_file(&path, "{\"next\":1}\n");
            } else {
                let next = hourly_file(&dir, "20261021", "17");
                append(&next, "{\"next\":1}\n");
                assert!(switch_collecting(&mut reader, &next).is_empty());
            }
            assert_eq!(reader.on_modify(), vec!["{\"next\":1}".to_string()]);
            assert!(reader.take_desynced(), "unfinished/lost old file must still be visible");
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn test_startup_positioning_error_cannot_prove_eof_and_recovers() {
        let dir = test_dir("startup_position_failure");
        let path = hourly_file(&dir, "20261021", "16");
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        reader.start_tracking(&path);
        assert_eq!(reader.read_lines().1, ReadStatus::NotRead);
        assert_eq!(reader.read_through(), ReadThrough::Unproven);
        append(&path, "");
        assert_eq!(reader.read_lines().1, ReadStatus::AtEnd);
        assert!(matches!(reader.read_through(), ReadThrough::Proven(_)));
        append(&path, "{\"first\":1}\n");
        assert_eq!(reader.on_modify(), vec!["{\"first\":1}".to_string()]);
        assert!(!reader.take_desynced());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_start_tracking_existing_content_starts_at_eof() {
        let dir = test_dir("eof");
        let path = dir.join("0");
        append(&path, "{\"old\":1}\n");
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
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
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        reader.start_tracking(&old);

        append(&old, "{\"tail\":1}\n");
        append(&new, "{\"first\":2}\n");
        let old_lines = switch_collecting(&mut reader, &new);
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
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
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
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
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
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
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

    /// BM 2026-10-21 13:00 UTC: polling switched AccountOrderStatuses to hour 13 before its queued create
    /// notification arrived. Resetting the same inode replayed 5,952 lines from blocks 1191955447..1191955451
    /// after their release, withdrawing marks and the following funding settlement's cash replay.
    #[test]
    fn test_delayed_create_after_poll_switch_preserves_position_and_partial_line() {
        let dir = test_dir("rotation_delayed_create");
        let old = hourly_file(&dir, "20261021", "12");
        append(&old, "");
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        reader.start_tracking(&old);
        assert!(reader.on_modify().is_empty());
        thread::sleep(Duration::from_millis(20));
        let new = hourly_file(&dir, "20261021", "13");
        let lines: Vec<_> = (1_191_955_447..=1_191_955_451).map(bn_line).collect();
        append(&new, &(lines.join("\n") + "\n"));
        assert_eq!(reader.periodic_check(), Some(FileCheck::Switch(new.clone())));
        assert!(switch_collecting(&mut reader, &new).is_empty());
        assert_eq!(reader.on_modify(), lines);
        assert!(matches!(reader.read_through(), ReadThrough::Proven(_)));
        let position = reader.file_position;

        assert!(switch_collecting(&mut reader, &new).is_empty(), "a delayed notification delivers no old lines");
        assert_eq!(reader.file_position, position, "the same inode must not rewind after a read-through proof");
        assert!(reader.on_modify().is_empty());
        append(&new, "{\"block_number\":1191955452,\"events\":[");
        assert!(reader.on_modify().is_empty());
        assert_eq!(reader.read_through(), ReadThrough::Unproven);
        let partial = reader.partial_line.clone();
        assert!(switch_collecting(&mut reader, &new).is_empty());
        assert_eq!(reader.partial_line, partial, "another duplicate must retain the unfinished line");
        assert!(!reader.take_desynced());
        append(&new, "]}\n");
        assert_eq!(reader.on_modify(), vec![bn_line(1_191_955_452)]);
        assert!(matches!(reader.read_through(), ReadThrough::Proven(_)));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_duplicate_create_before_first_read_does_not_drain_then_replay() {
        let dir = test_dir("duplicate_create_unopened");
        let path = hourly_file(&dir, "20261021", "13");
        append(&path, &(bn_line(1_191_955_447) + "\n"));
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        assert!(switch_collecting(&mut reader, &path).is_empty());
        assert!(switch_collecting(&mut reader, &path).is_empty(), "the first read is still pending");
        assert_eq!(reader.on_modify(), vec![bn_line(1_191_955_447)]);
        assert!(reader.on_modify().is_empty());
        assert!(!reader.take_desynced());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_delayed_create_after_adoption_keeps_start_skip_and_reads_only_new_tail() {
        let dir = test_dir("duplicate_create_adopted");
        let path = hourly_file(&dir, "20261021", "13");
        append(&path, &(bn_line(1_191_955_447) + "\n"));
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        reader.start_tracking(&path);
        append(&path, &(bn_line(1_191_955_448) + "\n"));
        assert!(switch_collecting(&mut reader, &path).is_empty(), "the pending read keeps its startup cursor");
        assert_eq!(reader.on_modify(), vec![bn_line(1_191_955_448)]);
        assert!(!reader.take_desynced());
        std::fs::remove_dir_all(&dir).unwrap();
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
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
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
        switch_collecting(&mut reader, &new);
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
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        reader.start_tracking(&old);
        append(&old, &format!("{}\n", bn_line(10)));
        assert_eq!(reader.on_modify(), vec![bn_line(10)]); // opens + holds the fd

        // A tail lands, then the file is pruned: the held fd still reads it.
        append(&old, &format!("{}\n", bn_line(11)));
        std::fs::remove_file(&old).unwrap();
        let new = day.join("1");
        append(&new, &format!("{}\n", bn_line(12)));
        assert_eq!(switch_collecting(&mut reader, &new), vec![bn_line(11)], "tail of the deleted inode is drained");
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
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        reader.start_tracking(&old); // tracks position, opens no handle

        std::fs::remove_file(&old).unwrap();
        let new = day.join("1");
        append(&new, &format!("{}\n", bn_line(20)));
        assert!(switch_collecting(&mut reader, &new).is_empty(), "nothing to drain from the vanished file");
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
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
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
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        reader.start_tracking(&path);
        append(&path, &format!("{}\n", bn_line(10)));
        assert_eq!(reader.on_modify(), vec![bn_line(10)]); // opens + holds the fd

        replace_file(&path, &format!("{}\n", bn_line(20)));
        assert!(reader.on_modify().is_empty(), "held fd still reads the drained old inode");
        assert_eq!(reader.check_for_newer_file(), Some(path.clone()), "inode swap must force a switch");
        assert!(switch_collecting(&mut reader, &path).is_empty(), "old inode was already drained");
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
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        reader.start_tracking(&path); // stale position, no handle

        replace_file(&path, &format!("{}\n", bn_line(20)));
        assert!(switch_collecting(&mut reader, &path).is_empty(), "no drain through a stale offset");
        assert!(reader.take_desynced(), "the old inode's unread tail is unrecoverable");
        assert_eq!(reader.on_modify(), vec![bn_line(20)]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Review 000429 r1 P2: the rotation drain hands over one bounded read at a time, in order, and gives up (without
    /// switching) once the consumer is gone.
    #[test]
    fn test_rotation_drain_delivers_one_read_at_a_time() {
        let dir = test_dir("rotation_chunked_delivery");
        let old = dir.join("0");
        let new = dir.join("1");
        append(&old, "");
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        reader.start_tracking(&old);
        let lines: Vec<String> = (0..20).map(|n| format!("{{\"n\":{n},\"x\":\"{}\"}}", "a".repeat(1 << 20))).collect();
        append(&old, &(lines.join("\n") + "\n"));
        append(&new, "{\"new\":1}\n");
        let mut deliveries: Vec<Vec<String>> = Vec::new();
        assert!(reader.on_create(&new, |delivered| {
            deliveries.push(delivered);
            true
        }));
        let largest = deliveries.iter().map(|chunk| chunk.iter().map(|line| line.len() + 1).sum::<usize>()).max();
        assert!(deliveries.len() >= 3, "{} deliveries", deliveries.len());
        let bound = usize::try_from(READ_CHUNK_BYTES).unwrap() + lines[0].len() + 1;
        assert!(largest.unwrap() <= bound, "one read plus the line it split");
        assert_eq!(deliveries.concat(), lines, "every line, in order");
        assert_eq!(reader.on_modify(), vec!["{\"new\":1}".to_string()]);

        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        reader.start_tracking(&new);
        append(&new, &(lines.join("\n") + "\n"));
        let mut calls = 0;
        assert!(!reader.on_create(&old, |_| {
            calls += 1;
            false
        }));
        assert_eq!((calls, reader.current_path.as_ref()), (1, Some(&new)), "no switch once the consumer is gone");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A rotation backlog reaches a consumer behind a one-slot channel complete and in order, then the watcher exits
    /// once the consumer is gone.
    #[test]
    fn test_line_watcher_hands_a_rotation_backlog_to_a_slow_consumer() {
        let dir = test_dir("slow_consumer");
        let old = hourly_file(&dir, "20261021", "0");
        append(&old, "");
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(1);
        let handle = spawn_line_watcher(test_stream(dir.clone(), false), tx, None, |line| line, |_| "DESYNC".to_string(), None);
        thread::sleep(Duration::from_millis(200));
        append(&old, "{\"skipped\":0}\n");
        thread::sleep(Duration::from_millis(200));
        let backlog: Vec<String> = (0..40).map(|n| format!("{{\"n\":{n},\"x\":\"{}\"}}", "b".repeat(1 << 19))).collect();
        append(&old, &(backlog.join("\n") + "\n"));
        append(&hourly_file(&dir, "20261021", "1"), "{\"new\":1}\n");
        let expected: Vec<String> = backlog.into_iter().chain(["{\"new\":1}".to_string()]).collect();
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut received = Vec::new();
        while received.len() < expected.len() && Instant::now() < deadline {
            match rx.try_recv() {
                Ok(line) => received.push(line),
                Err(_) => thread::sleep(Duration::from_millis(2)),
            }
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(received.len(), expected.len());
        assert!(received == expected, "every line once, in order");
        drop(rx);
        let deadline = Instant::now() + Duration::from_secs(2);
        while !handle.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(handle.is_finished(), "the watcher exits once the consumer is gone");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Live restart sample: the ephemeral date path must not steal the hour cursor or discard a partial row.
    #[test]
    fn test_vanished_non_stream_notification_preserves_hourly_cursor() {
        let dir = test_dir("vanished_notification");
        let path = hourly_file(&dir, "20261021", "14");
        append(&path, "{\"old\":0}\n");
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        reader.start_tracking(&path);
        assert!(reader.on_modify().is_empty());
        append(&path, "{\"next\":");
        assert!(reader.on_modify().is_empty());
        let position = reader.file_position;
        for temporary in [dir.join("20261021"), dir.join("hourly/20261022")] {
            std::fs::create_dir_all(&temporary).unwrap();
            std::fs::remove_dir(&temporary).unwrap();
            assert!(!temporary.is_dir(), "reproduces the old metadata-only filter's false result");
            for kind in [notify::EventKind::Create(notify::event::CreateKind::Any),
                notify::EventKind::Modify(notify::event::ModifyKind::Any)]
            {
                let event = Event::new(kind).add_path(temporary.clone());
                if let Some(path) = reader.notified_file(&event) { switch_collecting(&mut reader, path); }
            }
        }
        assert_eq!(reader.current_path.as_ref(), Some(&path));
        assert_eq!(reader.file_position, position);
        assert_eq!(reader.partial_line, b"{\"next\":");
        assert!(!reader.take_desynced());
        assert_eq!(reader.periodic_check(), None, "no fallback that replays the hour from zero");
        append(&path, "1}\n");
        assert_eq!(reader.on_modify(), vec!["{\"next\":1}".to_string()]);
        assert!(reader.on_modify().is_empty(), "each row is delivered once");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn test_stream_notifications_retain_real_rotation_replacement_and_loss_paths() {
        let dir = test_dir("notification_layout");
        for (layout, relative) in [(FileLayout::Hourly, "hourly/20261021/14"),
            (FileLayout::ReplicaSessions, "2026-10-21T14:02:07Z/20261021/1192000000")]
        {
            let reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, layout);
            let path = dir.join(relative);
            for kind in [notify::EventKind::Create(notify::event::CreateKind::File),
                notify::EventKind::Modify(notify::event::ModifyKind::Any)]
            {
                let event = Event::new(kind).add_path(path.clone());
                assert_eq!(reader.notified_file(&event), Some(&path), "even a vanished real leaf must report loss");
            }
            let event = Event::new(notify::EventKind::Create(notify::event::CreateKind::Folder)).add_path(path);
            assert_eq!(reader.notified_file(&event), None, "a known directory is never a data leaf");
        }
        let reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        for relative in ["hourly/20261021/24", "hourly/20261021/14.tmp", "hourly/bad/14", "other/20261021/14",
            "hourly/20261021/14/child", "../hourly/20261021/14"]
        {
            let event = Event::new(notify::EventKind::Create(notify::event::CreateKind::Any)).add_path(dir.join(relative));
            assert_eq!(reader.notified_file(&event), None, "{relative}");
        }
        assert_eq!(reader.notified_file(&Event::new(notify::EventKind::Any)), None);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// `replica_cmds`: the newest session and day by name, then the greatest first height (not the greatest name).
    #[test]
    fn test_replica_layout_follows_files_and_sessions() {
        let dir = test_dir("replica_layout");
        let day = dir.join("2026-10-13T00:38:01Z").join("20261019");
        std::fs::create_dir_all(&day).unwrap();
        append(&day.join("999"), "");
        append(&day.join("1000"), "");
        append(&day.join("notes"), "");
        std::fs::create_dir(day.join("2000")).unwrap();
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::ReplicaSessions);
        assert_eq!(reader.find_latest_file(), Some(day.join("1000")), "heights compare as numbers");
        reader.start_tracking(&day.join("1000"));
        thread::sleep(Duration::from_millis(20));
        append(&day.join("1010"), "{\"height\":1010}\n");
        assert_eq!(reader.check_for_newer_file(), Some(day.join("1010")), "next file of the session");
        assert_eq!(switch_collecting(&mut reader, &day.join("1010")), Vec::<String>::new());

        let session = dir.join("2026-10-19T21:00:00Z");
        std::fs::create_dir_all(&session).unwrap();
        assert_eq!(reader.check_for_newer_file(), None, "a new session without files yet");
        let restarted = session.join("20261019");
        std::fs::create_dir_all(&restarted).unwrap();
        thread::sleep(Duration::from_millis(20));
        append(&restarted.join("1020"), "{\"height\":1020}\n");
        assert_eq!(reader.check_for_newer_file(), Some(restarted.join("1020")), "the newest session");
        assert_eq!(switch_collecting(&mut reader, &restarted.join("1020")), vec!["{\"height\":1010}".to_string()]);
        assert_eq!(reader.on_modify(), vec!["{\"height\":1020}".to_string()]);
        assert!(!reader.take_desynced());
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
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        reader.start_tracking(&old);
        // The first complete row fills exactly one read; the next one spans two
        // reads (still below the partial-line cap); a short row follows.
        let first = format!("{{\"x\":\"{}\"}}\n", "a".repeat(READ_CHUNK_BYTES as usize - 9));
        assert_eq!(first.len(), READ_CHUNK_BYTES as usize);
        let second = format!("{{\"x\":\"{}\"}}\n", "b".repeat(9 * 1024 * 1024));
        append(&old, &(first + &second + "{\"tail\":1}\n"));
        append(&new, "{\"new\":1}\n");
        let drained = switch_collecting(&mut reader, &new);
        let loss_reported = reader.take_desynced();
        std::fs::remove_dir_all(&dir).unwrap();
        assert_eq!(drained.len(), 3, "all three old rows survive the switch");
        assert!(drained[2].contains("tail"));
        assert!(!loss_reported, "a complete drain is not data loss");
    }

    #[test]
    fn repeated_read_failures_keep_the_cursor_and_clear_only_after_a_read() {
        let dir = test_dir("read_error_episode");
        let path = hourly_file(&dir, "20261021", "14");
        append(&path, "");
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        reader.start_tracking(&path);
        append(&path, "{\"block\":100}\n");
        for attempts in 1..=100 {
            reader.file = Some(std::fs::OpenOptions::new().append(true).open(&path).unwrap());
            assert_eq!(reader.read_lines().1, ReadStatus::NotRead);
            assert_eq!(reader.file_position, 0);
            assert_eq!(reader.io_failures.attempts, attempts);
            assert_eq!(reader.io_failures.path.as_ref(), Some(&path));
        }
        assert_eq!(reader.on_modify(), vec!["{\"block\":100}".to_string()]);
        assert_eq!(reader.io_failures.path, None);
        assert_eq!(reader.io_failures.attempts, 0);
        assert!(reader.on_modify().is_empty());
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Review 000434 r2 P2: the old file's read fails with a line of block 100 unread; the switch leaves that tail
    /// behind, so it is a loss, reported before the new file can prove a read-through.
    #[test]
    fn test_rotation_after_a_failed_read_flags_the_unread_tail() {
        let dir = test_dir("rotation_failed_read");
        let old = hourly_file(&dir, "20261020", "5");
        append(&old, "{\"old\":0}\n");
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        reader.start_tracking(&old);
        assert!(reader.on_modify().is_empty(), "opens the handle at the end");
        // a handle that sizes and seeks but cannot read
        reader.file = Some(std::fs::OpenOptions::new().append(true).open(&old).unwrap());
        append(&old, "{\"block\":100}\n");
        thread::sleep(Duration::from_millis(20)); // a later mtime for the new file
        let new = hourly_file(&dir, "20261020", "6");
        append(&new, "{\"block\":101}\n");
        assert_eq!(switch_collecting(&mut reader, &new), Vec::<String>::new(), "the old tail was not read");
        assert_eq!(reader.on_modify(), vec!["{\"block\":101}".to_string()], "the watcher reads the new file next");
        assert_eq!(reader.read_through(), ReadThrough::Unproven, "the loss is reported first");
        assert!(reader.take_desynced(), "the unread tail is a loss");
        assert!(matches!(reader.read_through(), ReadThrough::Proven(_)));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Review 000434 r2 P2: a partial line left at the old file's end is dropped by the switch: a loss.
    #[test]
    fn test_rotation_with_a_partial_tail_flags_the_dropped_line() {
        let dir = test_dir("rotation_partial_tail");
        let old = hourly_file(&dir, "20261020", "5");
        append(&old, "");
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        reader.start_tracking(&old);
        append(&old, "{\"block\":100}\n{\"block\":");
        assert_eq!(reader.on_modify(), vec!["{\"block\":100}".to_string()]);
        thread::sleep(Duration::from_millis(20)); // a later mtime for the new file
        let new = hourly_file(&dir, "20261020", "6");
        append(&new, "{\"block\":101}\n");
        assert_eq!(switch_collecting(&mut reader, &new), Vec::<String>::new());
        assert!(reader.partial_line.is_empty(), "the switch starts the new file clean");
        assert!(reader.take_desynced(), "the dropped partial line is a loss");
        std::fs::remove_dir_all(&dir).ok();
    }
}
