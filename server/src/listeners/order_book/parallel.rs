// HFT-optimized parallel file watcher
// Each event source runs on its own thread for maximum throughput

use crate::{
    metrics::{NODE_LINE_LAG, NODE_FILE_IO_ERRORS_TOTAL},
    types::node_data::{EventSource, array_line_local_time_unix_nanos, line_local_time_unix_nanos},
};
use log::{error, info, warn};
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

/// How long a watcher sleeps once a read finds nothing new. Pure polling (design 000767): inotify woke the readers on
/// every node write, about 100k a second at the 2026-11-05 US open, and the node's writing thread paid for every
/// wakeup (a third of its time in the kernel, its block apply falling behind).
const POLL_INTERVAL: Duration = Duration::from_millis(1);

/// How often a watcher looks for the stream's next file, or for the file to start from (design 000767).
const FILE_CHECK_INTERVAL: Duration = Duration::from_millis(10);

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
pub(crate) fn seconds_since_node_write(node_unix_nanos: i64) -> f64 {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_nanos());
    (i128::try_from(now).unwrap_or(i128::MAX) - i128::from(node_unix_nanos)) as f64 / 1e9
}

/// How fresh the data is when the watcher sees it: lag of the newest line read.
fn observe_read_lag(label: &str, shape: LineShape, lines: &[String]) {
    if let Some(node_ns) = lines.last().and_then(|line| shape.local_time_unix_nanos(line)) {
        NODE_LINE_LAG.with_label_values(&["read", label]).observe(seconds_since_node_write(node_ns));
    }
}

/// The JSON value each line of a stream holds: an object in the node's book, fill and account streams, an array in its
/// mempool (`[local_time, tx]`). A terminated line of another shape is discarded as malformed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LineShape {
    Object,
    Array,
}

impl LineShape {
    fn holds(self, line: &str) -> bool {
        match self {
            Self::Object => line.starts_with('{') && line.ends_with('}'),
            Self::Array => line.starts_with('[') && line.ends_with(']'),
        }
    }

    /// The node's write time of a line of this shape, read from its prefix.
    fn local_time_unix_nanos(self, line: &str) -> Option<i64> {
        match self {
            Self::Object => line_local_time_unix_nanos(line),
            Self::Array => array_line_local_time_unix_nanos(line),
        }
    }
}

/// How a stream lays out its files below its directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileLayout {
    /// `hourly/<YYYYMMDD>/<H>`: in order by day, then hour.
    Hourly,
    /// `<session>/<YYYYMMDD>/<first height>` (`replica_cmds`; a node restart opens a new session, named by its ISO start
    /// time): in order by session and day by name, then first height.
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
        if !day.to_str().is_some_and(is_day_name) { return false; }
        let Some(leaf) = leaf.to_str() else { return false; };
        if leaf.is_empty() || !leaf.bytes().all(|byte| byte.is_ascii_digit()) { return false; }
        match self {
            Self::Hourly => first == "hourly" && leaf.parse::<u8>().is_ok_and(|hour| hour < 24),
            Self::ReplicaSessions => leaf.parse::<u64>().is_ok(),
        }
    }

}

/// A stream file's place in its stream: hourly files by (day, hour), replica files by (session, day, first height).
/// The hourly layout's session part is empty.
type FileKey = (String, String, u64);

impl FileLayout {
    fn order_key(self, base: &Path, path: &Path) -> Option<FileKey> {
        if !self.contains_file(base, path) { return None; }
        let mut parts = path.strip_prefix(base).ok()?.components().map(|part| part.as_os_str().to_str());
        let (first, day, leaf) = (parts.next()??, parts.next()??, parts.next()??);
        let session = match self { Self::Hourly => String::new(), Self::ReplicaSessions => first.to_owned() };
        Some((session, day.to_owned(), leaf.parse().ok()?))
    }

    /// The directories holding day directories, in order, with the session part of their files' keys.
    fn session_dirs(self, base: &Path) -> std::io::Result<Vec<(String, PathBuf)>> {
        Ok(match self {
            Self::Hourly => vec![(String::new(), base.join("hourly"))],
            Self::ReplicaSessions => child_dirs(base)?.into_iter()
                .filter_map(|dir| Some((dir.file_name()?.to_str()?.to_owned(), dir))).collect(),
        })
    }

    /// A day directory's stream files, in order.
    fn day_files(self, base: &Path, day_dir: &Path) -> std::io::Result<Vec<(FileKey, PathBuf)>> {
        let mut files = Vec::new();
        for path in dir_entries(day_dir)? {
            let Some(key) = self.order_key(base, &path) else { continue };
            if path_kind(&path)?.is_some_and(|kind| kind.is_file()) { files.push((key, path)); }
        }
        files.sort();
        Ok(files)
    }

    /// The stream's first file after `after` in its order (its first file at all without one); `None` when there is none
    /// yet. Several new files are taken one at a time, so none is passed over (design 000767). A directory that cannot
    /// be read fails the listing: going on past its files would leave a silent gap.
    fn next_file(self, base: &Path, after: Option<&FileKey>) -> std::io::Result<Option<PathBuf>> {
        for (session, session_dir) in self.session_dirs(base)? {
            if after.is_some_and(|after| session < after.0) { continue; }
            for (day, day_dir) in day_dirs(&session_dir)? {
                if after.is_some_and(|after| (&session, &day) < (&after.0, &after.1)) { continue; }
                let next = self.day_files(base, &day_dir)?.into_iter()
                    .find(|(key, _)| after.is_none_or(|after| key > after));
                if let Some((_, path)) = next { return Ok(Some(path)); }
            }
        }
        Ok(None)
    }

    /// The stream's last file in its order, from the newest day directory that holds one: an empty newer day or session
    /// does not hide the files before it (review 000769 r1 P2-2). Following any earlier file from its end would have
    /// the ordered switch read the files after it from their start.
    fn last_file(self, base: &Path) -> std::io::Result<Option<PathBuf>> {
        for (_, session_dir) in self.session_dirs(base)?.into_iter().rev() {
            for (_, day_dir) in day_dirs(&session_dir)?.into_iter().rev() {
                if let Some((_, path)) = self.day_files(base, &day_dir)?.pop() { return Ok(Some(path)); }
            }
        }
        Ok(None)
    }
}

/// A day directory's name: `YYYYMMDD`.
fn is_day_name(name: &str) -> bool {
    name.len() == 8 && name.bytes().all(|byte| byte.is_ascii_digit())
}

/// The paths in `dir`, sorted; none when it does not exist (not yet, or no longer). Any other failure is an error, not
/// an empty directory.
fn dir_entries(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let listing = std::fs::read_dir(dir)
        .and_then(|entries| entries.map(|entry| entry.map(|entry| entry.path())).collect::<std::io::Result<Vec<_>>>());
    let mut paths = match listing {
        Ok(paths) => paths,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(std::io::Error::new(err.kind(), format!("listing {}: {err}", dir.display()))),
    };
    paths.sort();
    Ok(paths)
}

/// The type of what `path` names, symlinks followed (as `Path::is_file` does); `None` when it is gone (removed after its
/// directory was listed). Any other failure is an error, not "neither file nor directory" (review 000769 r2 P3).
fn path_kind(path: &Path) -> std::io::Result<Option<std::fs::FileType>> {
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(Some(metadata.file_type())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(std::io::Error::new(err.kind(), format!("inspecting {}: {err}", path.display()))),
    }
}

/// The child directories of `dir`, sorted.
fn child_dirs(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut dirs = Vec::new();
    for path in dir_entries(dir)? {
        if path_kind(&path)?.is_some_and(|kind| kind.is_dir()) { dirs.push(path); }
    }
    Ok(dirs)
}

/// A session directory's day directories, sorted, with their days.
fn day_dirs(session_dir: &Path) -> std::io::Result<Vec<(String, PathBuf)>> {
    Ok(child_dirs(session_dir)?.into_iter()
        .filter_map(|dir| Some((dir.file_name()?.to_str().filter(|day| is_day_name(day))?.to_owned(), dir)))
        .collect())
}

/// What the periodic file check found.
#[derive(Debug, PartialEq, Eq)]
enum FileCheck {
    /// Nothing is tracked yet: follow this file from its end.
    Adopt(PathBuf),
    /// The next file (or the tracked path recreated): drain the tracked one, then read this one from its start.
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
    // Data had to be discarded (an oversized partial line, a malformed line, a file lost); drained by take_desynced
    // so the watcher can notify the listener.
    losses: Losses,
    // Failed opens and reads share an episode: reopening a bad reader is not recovery.
    io_failures: FileIoFailures,
    // Failed directory listings, an episode of their own: a read of the tracked file does not end it.
    listing_failures: FileIoFailures,
    // (dev, ino) of the file that file_position refers to. Catches same-path
    // recreation: node catch-up writes by BLOCK time, so after a long stop it
    // recreates the very hour file retention pruned - same path, new inode,
    // where our position is meaningless.
    open_file_id: Option<(u64, u64)>,
    // Cap on one un-terminated line (MAX_PARTIAL_LINE_BYTES for the book streams).
    max_line_bytes: usize,
    layout: FileLayout,
    shape: LineShape,
    // A listing found no stream file while nothing was tracked: the stream's first file is read from its start. Until a
    // listing has answered, the stream's last file is followed from its end.
    started_empty: bool,
}

/// A stream's loss counts, for a consumer that must know of a loss before the lines after it (the pending book's
/// mempool, review 000609 r1-r7): `found` the moment a loss is known and `flagged` as the watcher flags it for its next
/// `desync`, which reports every loss flagged before it and no other. The reader finds and flags its losses together;
/// the mempool's own findings (`pending/mempool.rs`) count found only.
#[derive(Clone, Default)]
pub(crate) struct LossCounts {
    pub(crate) found: Arc<AtomicU64>,
    pub(crate) flagged: Arc<AtomicU64>,
}

/// The reader's losses: one waiting to be reported, and the stream's counts of them where it keeps them.
#[derive(Default)]
struct Losses {
    desynced: bool,
    counts: Option<LossCounts>,
}

impl Losses {
    /// A loss the reader found: found and flagged at once, its desync out after this read's lines.
    fn flag(&mut self) {
        self.desynced = true;
        if let Some(counts) = &self.counts {
            counts.found.fetch_add(1, AtomicOrdering::Release);
            counts.flagged.fetch_add(1, AtomicOrdering::Release);
        }
    }
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
    fn succeeded(&mut self, path: &Path) {
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
            losses: Losses::default(),
            io_failures: FileIoFailures::default(),
            listing_failures: FileIoFailures::default(),
            open_file_id: None,
            max_line_bytes,
            layout,
            shape: LineShape::Object,
            started_empty: false,
        }
    }

    /// The reader of a stream whose lines hold `shape`.
    const fn with_shape(mut self, shape: LineShape) -> Self {
        self.shape = shape;
        self
    }

    /// The reader counting its losses in `counts`.
    fn with_loss_counts(mut self, counts: Option<LossCounts>) -> Self {
        self.losses.counts = counts;
        self
    }

    /// True once if data was discarded since the last call.
    fn take_desynced(&mut self) -> bool {
        std::mem::take(&mut self.losses.desynced)
    }

    /// The file check, every `FILE_CHECK_INTERVAL` (design 000767). Tracking: the tracked path recreated in place (node
    /// catch-up writes by block time and recreates an hour file retention pruned: same path, new inode) comes before any
    /// file after it (review 000769 r1 P2-1); otherwise the stream's next file in order, one at a time. A switch drains
    /// the tracked file first; `on_create` decides whether the handoff is gapless or a loss (a vanished or recreated file
    /// without a held handle).
    fn check_files(&mut self) -> Option<FileCheck> {
        let Some(current) = self.current_path.clone() else { return self.check_untracked(); };
        let recreated = current.metadata().ok().zip(self.open_file_id).is_some_and(|(meta, open_id)| file_id(&meta) != open_id);
        if recreated { return Some(FileCheck::Switch(current)); }
        let key = self.layout.order_key(&self.base_dir, &current)?;
        let next = self.layout.next_file(&self.base_dir, Some(&key));
        self.listed(next)?.map(FileCheck::Switch)
    }

    /// Nothing tracked. Until a listing answers, the stream's last file, followed from its end (`Adopt`; bm 2026-10-21:
    /// re-reading a whole hour file as if created replayed 4,413 released blocks). Once a listing found no file at all,
    /// the stream's first file, from its start. A failed listing knows neither: the next check asks again.
    fn check_untracked(&mut self) -> Option<FileCheck> {
        if self.started_empty {
            let first = self.layout.next_file(&self.base_dir, None);
            return self.listed(first)?.map(FileCheck::Switch);
        }
        let last = self.layout.last_file(&self.base_dir);
        let last = self.listed(last)?;
        self.started_empty = last.is_none();
        last.map(FileCheck::Adopt)
    }

    /// A listing's answer, or `None` when it failed (the failure logged once per episode and counted).
    fn listed<T>(&mut self, listing: std::io::Result<T>) -> Option<T> {
        match listing {
            Ok(found) => {
                self.listing_failures.succeeded(&self.base_dir);
                Some(found)
            }
            Err(err) => {
                self.listing_failures.failed(&self.base_dir, "list", &err);
                None
            }
        }
    }

    /// One read's complete lines (tests; the watcher reads through `read_lines`, which also says what the read found).
    #[cfg(test)]
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
                                self.losses.flag();
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
            if read.is_ok() { self.io_failures.succeeded(path); }
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
                        self.losses.flag();
                        continue;
                    };
                    let line = text.trim_end();
                    if line.is_empty() {
                        continue;
                    }
                    if self.shape.holds(line) {
                        lines.push(line.to_string());
                    } else {
                        error!(
                            "discarding malformed terminated line; flagging desync path={} inode={:?} bytes={} offsets={}..{}",
                            path.display(), self.open_file_id, segment.len(), start_offset, segment_offset
                        );
                        self.losses.flag();
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
                self.losses.flag();
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
            self.losses.flag();
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
                        self.losses.flag();
                        break;
                    }
                }
            }
            if !self.partial_line.is_empty() || self.startup_boundary != StartupBoundary::Aligned {
                warn!(
                    "{} has no complete record boundary (buffered={} startup_pending={}); switching to {} and flagging desync",
                    old.display(), self.partial_line.len(), self.startup_boundary != StartupBoundary::Aligned, path.display()
                );
                self.losses.flag();
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
    /// instant is taken first; then the stream's last file in order must be the tracked one with the inode the position
    /// refers to (a listing that fails proves nothing), and a read from the position must establish the end (the
    /// handle's size is the position: sizing, seeking or reading that fails proves nothing), leave no partial line and
    /// flag no discard.
    /// The node appends to the tracked file or a file after it in order: anything appended before that instant is in the
    /// tracked file (the read would have returned it) or in a file after it (the listing would have seen it), so a proof
    /// covers every such line written before its instant. A file the reader has passed is not read again (design
    /// 000767): lines the node writes back into one are not covered. A read that returns lines, or one cut at
    /// `READ_CHUNK_BYTES`, proves nothing yet.
    fn read_through(&mut self) -> ReadThrough {
        let at = Instant::now();
        let Some(current) = self.current_path.clone() else {
            return ReadThrough::Unproven;
        };
        let recreated = current.metadata().ok().map(|meta| file_id(&meta)) != self.open_file_id;
        if recreated {
            return ReadThrough::Unproven;
        }
        let last = self.layout.last_file(&self.base_dir);
        if self.listed(last).flatten().as_ref() != Some(&current) {
            return ReadThrough::Unproven;
        }
        match self.read_lines() {
            (lines, ReadStatus::Read) => ReadThrough::Lines(lines),
            (_, ReadStatus::AtEnd) if self.partial_line.is_empty() && !self.losses.desynced
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
                self.io_failures.succeeded(path);
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

/// Spawn a file watcher thread for a single event source (pure polling, design 000767).
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
        shape: LineShape::Object,
        loss_counts: None,
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
    /// The watcher positions at the end of the newest file at its first file check, skipping the content already on
    /// disk; when set, it sends `desync` right then.
    pub(crate) start_skip_is_loss: bool,
    /// Which file below `dir` is the newest one.
    pub(crate) layout: FileLayout,
    /// What each line holds.
    pub(crate) shape: LineShape,
    /// The stream's loss counts (the pending book's mempool; `None` for the other streams).
    pub(crate) loss_counts: Option<LossCounts>,
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
/// had to be discarded; `last_event` (unix ms of the last line sent) feeds the book's health monitor. Pure polling
/// (design 000767): a read that found data is followed at once by the next, one that found nothing by a
/// `POLL_INTERVAL` sleep; every `FILE_CHECK_INTERVAL` the reader looks for its next file (`FileReader::check_files`).
/// The thread returns when the channel closes (checked every poll). With `read_through`,
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
    let LineStream { name, lag_label, dir, max_line_bytes, start_skip_is_loss, layout, shape, loss_counts } = stream;
    thread::spawn(move || {
        info!("{} watcher thread started for {:?}", name, dir);

        let mut reader = FileReader::new(dir.clone(), max_line_bytes, layout).with_shape(shape).with_loss_counts(loss_counts.clone());
        // Every read's lines go out here, in order, blocking while the channel is full; false once the consumer is gone.
        let send = |lines: Vec<String>| {
            observe_read_lag(lag_label, shape, &lines);
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

        let mut polls = 0u64;
        let mut last_file_check: Option<Instant> = None;
        let mut last_read_through: Option<Instant> = None;
        loop {
            polls += 1;
            // The consumer is gone: stop now rather than at the next line (a quiet stream may have none for long).
            if tx.is_closed() {
                info!("{name} channel closed, exiting");
                return;
            }

            if last_file_check.is_none_or(|checked| checked.elapsed() >= FILE_CHECK_INTERVAL) {
                last_file_check = Some(Instant::now());
                match reader.check_files() {
                    Some(FileCheck::Adopt(path)) => {
                        if !adopt_at_end(&mut reader, &path) {
                            info!("{name} consumer closed, exiting");
                            return;
                        }
                    }
                    Some(FileCheck::Switch(path)) => {
                        info!("{} next file: {:?}", name, path.file_name());
                        if !reader.on_create(&path, &send) {
                            return;
                        }
                    }
                    None => {}
                }
            }

            let (lines, status) = reader.read_lines();
            if !send(lines) {
                return;
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

            if polls.is_multiple_of(100_000)
                && let Some(size) = reader.current_path.as_ref().and_then(|path| path.metadata().ok()).map(|meta| meta.len())
            {
                log::debug!("{} poll {} - pos {} / size {}", name, polls, reader.file_position, size);
            }

            // Data keeps the reader going (a catch-up backlog comes in bounded chunks); an idle stream is polled.
            if status != ReadStatus::Read {
                thread::sleep(POLL_INTERVAL);
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
        LineStream {
            name: "Test",
            lag_label: "test",
            dir,
            max_line_bytes,
            start_skip_is_loss,
            layout: FileLayout::Hourly,
            shape: LineShape::Object,
            loss_counts: None,
        }
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

    /// The skip of the content on disk at the first file check is reported only when asked for (review 000422 r2); every
    /// line written after it is read (design 000767: no longer skipped up to a first modify notice).
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
            append(&path, "{\"live\":1}\n");
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
            let live = ["{\"live\":1}".to_string(), "{\"read\":2}".to_string()];
            let expected: Vec<String> = if start_skip_is_loss { std::iter::once("DESYNC".to_string()).chain(live).collect() } else { live.to_vec() };
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
    /// the newest hour file as created and re-read it (61 MB, 4,413 already released blocks). At the first check the
    /// newest file is adopted at its end; once tracking, the next file is a switch.
    #[test]
    fn test_first_file_check_adopts_the_newest_file_at_its_end() {
        let dir = test_dir("periodic_adopt");
        let current = hourly_file(&dir, "20261021", "0");
        append(&current, "{\"old\":0}\n");
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        assert_eq!(reader.check_files(), Some(FileCheck::Adopt(current.clone())));
        reader.start_tracking(&current);
        append(&current, "{\"new\":1}\n");
        assert_eq!(reader.on_modify(), vec!["{\"new\":1}".to_string()], "the content before adoption is not read");
        assert_eq!(reader.check_files(), None);
        let next = hourly_file(&dir, "20261021", "1");
        append(&next, "{\"next\":2}\n");
        assert_eq!(reader.check_files(), Some(FileCheck::Switch(next)));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Design 000767: a stream with no file when the watcher started reads its first file from the start, like the
    /// create notice did.
    #[test]
    fn test_a_stream_without_files_at_start_reads_its_first_file_from_the_start() {
        let dir = test_dir("started_empty");
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        assert_eq!(reader.check_files(), None, "nothing to adopt");
        let first = hourly_file(&dir, "20261105", "17");
        append(&first, "{\"first\":1}\n");
        assert_eq!(reader.check_files(), Some(FileCheck::Switch(first.clone())));
        assert!(switch_collecting(&mut reader, &first).is_empty());
        assert_eq!(reader.on_modify(), vec!["{\"first\":1}".to_string()], "read from its start");
        assert!(!reader.take_desynced());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Review 000769 r1 P2-2: an empty newer day or session directory does not make a stream with files look empty
    /// (which would read its earlier file from the start: content written before the watcher started); the stream's
    /// last file is followed from its end.
    #[test]
    fn test_first_file_check_adopts_the_last_file_past_empty_newer_directories() {
        let dir = test_dir("adopt_past_empty_dirs");
        let historical = hourly_file(&dir, "20261104", "23");
        append(&historical, "{\"historical\":1}\n");
        std::fs::create_dir_all(dir.join("hourly/20261105")).unwrap();
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        assert_eq!(reader.check_files(), Some(FileCheck::Adopt(historical.clone())));
        reader.start_tracking(&historical);
        assert!(reader.on_modify().is_empty(), "the content before startup is not read");
        assert_eq!(reader.check_files(), None);
        append(&historical, "{\"live\":2}\n");
        assert_eq!(reader.on_modify(), vec!["{\"live\":2}".to_string()]);

        let replica = test_dir("adopt_past_empty_replica_dirs");
        let session = replica.join("2026-11-04T10:00:00Z");
        let last = session.join("20261104").join("1000");
        std::fs::create_dir_all(last.parent().unwrap()).unwrap();
        append(&last, "{\"historical\":1}\n");
        std::fs::create_dir_all(session.join("20261105")).unwrap();
        std::fs::create_dir_all(replica.join("2026-11-05T09:00:00Z")).unwrap();
        std::fs::create_dir_all(replica.join("2026-11-05T10:00:00Z").join("20261105")).unwrap();
        let mut reader = FileReader::new(replica.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::ReplicaSessions);
        assert_eq!(reader.check_files(), Some(FileCheck::Adopt(last)));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&replica).ok();
    }

    /// A listing that fails is not an empty stream: nothing is adopted or read from its start until a listing answers,
    /// and then the last file is followed from its end.
    #[test]
    fn test_a_failed_listing_neither_adopts_nor_starts_empty() {
        let dir = test_dir("failed_listing");
        append(&dir.join("hourly"), ""); // a file where the hourly directory belongs: listing it fails (not NotFound)
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        assert_eq!(reader.check_files(), None);
        assert_eq!(reader.check_files(), None);
        assert!(!reader.started_empty);
        assert_eq!(reader.listing_failures.attempts, 2);
        std::fs::remove_file(dir.join("hourly")).unwrap();
        let file = hourly_file(&dir, "20261105", "3");
        append(&file, "{\"before\":1}\n");
        assert_eq!(reader.check_files(), Some(FileCheck::Adopt(file)), "followed from its end, not read from its start");
        assert_eq!(reader.listing_failures.path, None, "the episode ends with a listing that answers");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Review 000769 r2 P3: a stream file whose type cannot be read (its day directory listable, not searchable) fails
    /// the listing rather than counting as no file: the stream is not taken as empty (which would read the file from
    /// its start once readable); once readable it is followed from its end.
    #[test]
    fn test_an_unreadable_file_type_fails_the_listing() {
        use std::os::unix::fs::PermissionsExt;
        let dir = test_dir("unreadable_kind");
        let file = hourly_file(&dir, "20261104", "23");
        append(&file, "{\"historical\":1}\n");
        let day = file.parent().unwrap().to_path_buf();
        std::fs::set_permissions(&day, std::fs::Permissions::from_mode(0o400)).unwrap();
        if std::fs::metadata(&file).is_ok() {
            std::fs::set_permissions(&day, std::fs::Permissions::from_mode(0o755)).unwrap();
            std::fs::remove_dir_all(&dir).ok();
            eprintln!("skipped: the directory mode does not stop this user (root)");
            return;
        }
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        let first = reader.check_files();
        std::fs::set_permissions(&day, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(first, None);
        assert!(!reader.started_empty, "not an empty stream");
        assert_eq!(reader.listing_failures.attempts, 1);
        assert_eq!(reader.check_files(), Some(FileCheck::Adopt(file)), "followed from its end, not read from its start");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Review 000769 r1 P2-1: the tracked path recreated in place while the next file also exists. The recreated file
    /// comes first: the held handle's tail, then the recreated file from its start, then the next file, each once.
    #[test]
    fn test_recreated_tracked_path_is_read_before_the_next_file() {
        let dir = test_dir("recreated_then_next");
        let path = hourly_file(&dir, "20261105", "12");
        append(&path, "");
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        reader.start_tracking(&path);
        assert!(reader.on_modify().is_empty(), "opens and holds the handle");
        append(&path, "{\"old_tail\":1}\n");
        replace_file(&path, "{\"recreated\":2}\n");
        let next = hourly_file(&dir, "20261105", "13");
        append(&next, "{\"next\":3}\n");
        let mut read = Vec::new();
        for expected in [&path, &next] {
            assert_eq!(reader.check_files(), Some(FileCheck::Switch(expected.clone())));
            read.extend(switch_collecting(&mut reader, expected));
            read.extend(reader.on_modify());
        }
        assert_eq!(reader.check_files(), None);
        assert_eq!(read, ["{\"old_tail\":1}", "{\"recreated\":2}", "{\"next\":3}"]);
        assert!(!reader.take_desynced(), "the held handle makes the handoff gapless");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Design 000767: several new files between two checks are taken one at a time in order (hour by number, then the
    /// next day), each drained before the next; none is passed over the way the old newest-file switch could.
    #[test]
    fn test_file_check_switches_to_each_next_file_in_order() {
        let dir = test_dir("next_in_order");
        let current = hourly_file(&dir, "20261105", "9");
        append(&current, "");
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        reader.start_tracking(&current);
        assert!(reader.on_modify().is_empty());
        let (ten, eleven, next_day) = (hourly_file(&dir, "20261105", "10"), hourly_file(&dir, "20261105", "11"),
            hourly_file(&dir, "20261106", "0"));
        append(&current, "{\"h\":9}\n");
        append(&ten, "{\"h\":10}\n");
        append(&eleven, "{\"h\":11}\n");
        append(&next_day, "{\"h\":24}\n");
        let mut read = Vec::new();
        for expected in [&ten, &eleven, &next_day] {
            assert_eq!(reader.check_files(), Some(FileCheck::Switch(expected.clone())));
            read.extend(switch_collecting(&mut reader, expected));
            read.extend(reader.on_modify());
        }
        assert_eq!(reader.check_files(), None);
        assert_eq!(read, ["{\"h\":9}", "{\"h\":10}", "{\"h\":11}", "{\"h\":24}"], "every file, in order, once");
        assert!(!reader.take_desynced());
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
        assert_eq!(reader.check_files(), Some(FileCheck::Switch(new.clone())));
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
        assert_eq!(reader.check_files(), None);
        assert!(reader.on_modify().is_empty());
        assert!(!reader.take_desynced());

        // Node comes back and writes a new file: the check switches to it, and the switch flags the loss (no handle
        // was held on the vanished file, so its unread tail is gone).
        let new = day.join("1");
        append(&new, &format!("{}\n", bn_line(20)));
        assert_eq!(reader.check_files(), Some(FileCheck::Switch(new.clone())));
        switch_collecting(&mut reader, &new);
        assert!(reader.take_desynced(), "skipping over a vanished file is potential data loss");
        assert_eq!(reader.on_modify(), vec![bn_line(20)], "the new file is read from the start");
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
        assert_eq!(reader.check_files(), Some(FileCheck::Switch(path.clone())), "inode swap must force a switch");
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
        append(&old, "{\"live\":0}\n");
        thread::sleep(Duration::from_millis(200));
        let backlog: Vec<String> = (0..40).map(|n| format!("{{\"n\":{n},\"x\":\"{}\"}}", "b".repeat(1 << 19))).collect();
        append(&old, &(backlog.join("\n") + "\n"));
        append(&hourly_file(&dir, "20261021", "1"), "{\"new\":1}\n");
        let expected: Vec<String> = std::iter::once("{\"live\":0}".to_string()).chain(backlog).chain(std::iter::once("{\"new\":1}".to_string())).collect();
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


    /// A loss the reader finds is found and flagged together (review 000609 r7).
    #[test]
    fn test_losses_count_found_and_flagged() {
        let counts = LossCounts::default();
        let mut losses = Losses { desynced: false, counts: Some(counts.clone()) };
        losses.flag();
        assert_eq!((counts.found.load(AtomicOrdering::Acquire), counts.flagged.load(AtomicOrdering::Acquire)), (1, 1));
        assert!(losses.desynced);
    }


    /// Live restart sample: an ephemeral date directory (stream-looking but no data leaf) must not steal the hour cursor
    /// or discard a partial row; the file check sees only stream files.
    #[test]
    fn test_ephemeral_non_stream_directories_preserve_hourly_cursor() {
        let dir = test_dir("ephemeral_dirs");
        let path = hourly_file(&dir, "20261021", "14");
        append(&path, "{\"old\":0}\n");
        let mut reader = FileReader::new(dir.clone(), MAX_PARTIAL_LINE_BYTES, FileLayout::Hourly);
        reader.start_tracking(&path);
        assert!(reader.on_modify().is_empty());
        append(&path, "{\"next\":");
        assert!(reader.on_modify().is_empty());
        let position = reader.file_position;
        for temporary in [dir.join("20261021"), dir.join("hourly/20261022"), dir.join("hourly/20261021/15.tmp")] {
            std::fs::create_dir_all(&temporary).unwrap();
            assert_eq!(reader.check_files(), None, "{}", temporary.display());
            std::fs::remove_dir_all(&temporary).unwrap();
        }
        assert_eq!(reader.current_path.as_ref(), Some(&path));
        assert_eq!(reader.file_position, position);
        assert_eq!(reader.partial_line, b"{\"next\":");
        assert!(!reader.take_desynced());
        append(&path, "1}\n");
        assert_eq!(reader.on_modify(), vec!["{\"next\":1}".to_string()]);
        assert!(reader.on_modify().is_empty(), "each row is delivered once");
        std::fs::remove_dir_all(dir).unwrap();
    }


    /// `replica_cmds`: in order by session and day by name, then first height (not by name).
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
        assert_eq!(reader.check_files(), Some(FileCheck::Adopt(day.join("1000"))), "heights compare as numbers");
        reader.start_tracking(&day.join("1000"));
        thread::sleep(Duration::from_millis(20));
        append(&day.join("1010"), "{\"height\":1010}\n");
        assert_eq!(reader.check_files(), Some(FileCheck::Switch(day.join("1010"))), "next file of the session");
        assert_eq!(switch_collecting(&mut reader, &day.join("1010")), Vec::<String>::new());

        let session = dir.join("2026-10-19T21:00:00Z");
        std::fs::create_dir_all(&session).unwrap();
        assert_eq!(reader.check_files(), None, "a new session without files yet");
        let restarted = session.join("20261019");
        std::fs::create_dir_all(&restarted).unwrap();
        thread::sleep(Duration::from_millis(20));
        append(&restarted.join("1020"), "{\"height\":1020}\n");
        assert_eq!(reader.check_files(), Some(FileCheck::Switch(restarted.join("1020"))), "the next session");
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
