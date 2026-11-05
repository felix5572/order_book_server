//! The record's files (hyperliquid-trade design 000751 r2 rule 5), on a thread of their own: a tick's rows come
//! through a queue of `QUEUE_TICKS` ticks - full, the tick is dropped and counted, the sampler never waits - and each
//! row is a line of `<dir>/basis_record.<UTC date of its t_ms>.jsonl`, appended (a restart appends to the day's file;
//! the gap is missing lines, not filled).
//!
//! Finishing (review 000751 r1 P2): at start and on every change of day, every file of an earlier day than the one
//! being written is closed, so it is compressed with zstd to `.jsonl.zst.tmp`, the copy decompressed and compared byte
//! for byte, renamed to `.jsonl.zst` only when equal, then the original deleted. A step that fails leaves the original
//! and deletes the temporary copy, and the next finishing tries again; a temporary copy left by a crash is deleted
//! first. Files of a day before today minus the kept days are deleted, compressed or not.
//!
//! Whole lines only (review 000752 r1 P2): a tick's rows are serialized first and appended in one write; a write that
//! fails part way - a full disk writes some bytes, then errs - cuts the file back to its last whole tick, and a file
//! opened again or finished loses a partial line a crash left at its end first. A line is never joined to a fragment.
//!
//! Errors are counted (`orderbook_basis_record_errors_total` by operation) and warned at most once a minute with the
//! error as it came; they lose that tick or that day's finishing, never stop the server.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel},
    time::{Duration, Instant},
};

use chrono::{DateTime, NaiveDate};
use log::{info, warn};
use serde::Serialize;

use crate::metrics::{BASIS_RECORD_DROPPED_TOTAL, BASIS_RECORD_ERRORS_TOTAL};

/// Ticks the queue holds: a writer stuck this long behind loses ticks, the sampler none of its time.
const QUEUE_TICKS: usize = 3;
const PREFIX: &str = "basis_record.";
const OPEN: &str = ".jsonl";
const DONE: &str = ".jsonl.zst";
const TEMPORARY: &str = ".jsonl.zst.tmp";
/// zstd's level: a day's file compressed once, off every path the server serves, so a ratio nearer the top levels' at a
/// fraction of their time (a few GB a day).
const LEVEL: i32 = 9;
const WARN_EVERY: Duration = Duration::from_secs(60);
/// Bytes compared at a time when checking a compressed copy.
const CHUNK: usize = 1 << 16;

/// One tick's rows, all of its time.
pub(crate) struct Tick<R> {
    pub(crate) t_ms: u64,
    pub(crate) rows: Vec<R>,
}

/// The sampler's end of the queue.
pub(crate) struct RecordWriter<R> {
    sender: SyncSender<Tick<R>>,
}

impl<R: Serialize + Send + 'static> RecordWriter<R> {
    /// The writer thread over `dir`, keeping `days` days; it finishes the earlier days' files first.
    pub(crate) fn spawn(dir: PathBuf, days: u32, today_ms: u64) -> io::Result<Self> {
        let (sender, receiver) = sync_channel(QUEUE_TICKS);
        std::thread::Builder::new().name("basis-record".to_string()).spawn(move || run(&mut Files::new(dir, days), receiver, today_ms))?;
        Ok(Self { sender })
    }

    /// Queues `tick`; a full queue drops it, counted.
    pub(crate) fn offer(&self, tick: Tick<R>) {
        match self.sender.try_send(tick) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => BASIS_RECORD_DROPPED_TOTAL.with_label_values(&["full"]).inc(),
            Err(TrySendError::Disconnected(_)) => BASIS_RECORD_DROPPED_TOTAL.with_label_values(&["writer_gone"]).inc(),
        }
    }
}

fn run<R: Serialize>(files: &mut Files, receiver: Receiver<Tick<R>>, today_ms: u64) {
    files.finish_before(date_of(today_ms));
    for tick in receiver {
        files.write(&tick);
    }
    info!("basis record: the writer's queue closed, the writer thread ends");
}

/// The UTC date of a unix ms time.
fn date_of(t_ms: u64) -> NaiveDate {
    let ms = i64::try_from(t_ms).unwrap_or(i64::MAX);
    DateTime::from_timestamp_millis(ms).map_or(NaiveDate::MAX, |time| time.date_naive())
}

fn file_name(date: NaiveDate, suffix: &str) -> String {
    format!("{PREFIX}{}{suffix}", date.format("%Y-%m-%d"))
}

/// The date and suffix of one of the record's files; `None` for any other file.
fn parse_name(name: &str) -> Option<(NaiveDate, &'static str)> {
    let rest = name.strip_prefix(PREFIX)?;
    let (date, suffix) = rest.split_at_checked(10)?;
    let suffix = [TEMPORARY, DONE, OPEN].into_iter().find(|known| suffix == *known)?;
    Some((NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()?, suffix))
}

/// The day's file being appended: its date, the file, and its length up to the last whole tick.
struct Current {
    date: NaiveDate,
    file: File,
    committed: u64,
}

struct Files {
    dir: PathBuf,
    days: u32,
    current: Option<Current>,
    last_warning: Option<Instant>,
}

impl Files {
    const fn new(dir: PathBuf, days: u32) -> Self {
        Self { dir, days, current: None, last_warning: None }
    }

    fn write<R: Serialize>(&mut self, tick: &Tick<R>) {
        let date = date_of(tick.t_ms);
        if self.current.as_ref().is_some_and(|current| current.date != date) {
            self.current = None;
            self.finish_before(date);
        }
        let bytes = match lines(&tick.rows) {
            Ok(bytes) => bytes,
            Err(err) => return self.failed("serialize", &format!("a tick of {}: {err}", file_name(date, OPEN))),
        };
        if let Err(err) = self.append(date, &bytes) {
            self.rewind();
            self.failed("write", &format!("{}: {err}", file_name(date, OPEN)));
        }
    }

    fn append(&mut self, date: NaiveDate, bytes: &[u8]) -> io::Result<()> {
        if self.current.is_none() {
            let path = self.dir.join(file_name(date, OPEN));
            let mut file = OpenOptions::new().create(true).read(true).append(true).open(&path)?;
            let committed = cut_partial_line(&mut file)?;
            self.current = Some(Current { date, file, committed });
        }
        let current = self.current.as_mut().unwrap_or_else(|| unreachable!("opened above"));
        current.file.write_all(bytes)?;
        current.committed += bytes.len() as u64;
        Ok(())
    }

    /// After a failed append: the file cut back to its last whole tick; that failing too, closed, so the next open cuts
    /// it back to its last whole line.
    fn rewind(&mut self) {
        let Some(current) = self.current.as_mut() else { return };
        let Err(err) = current.file.set_len(current.committed) else { return };
        let name = file_name(current.date, OPEN);
        self.current = None;
        self.failed("rewind", &format!("{name}: {err}"));
    }

    /// Finishes every file of a day before `date`, then deletes the files past the kept days.
    fn finish_before(&mut self, date: NaiveDate) {
        let names = match self.names() {
            Ok(names) => names,
            Err(err) => return self.failed("finish", &format!("listing {}: {err}", self.dir.display())),
        };
        let oldest_kept = date.checked_sub_days(chrono::Days::new(u64::from(self.days))).unwrap_or(NaiveDate::MIN);
        for (day, suffix) in &names {
            if *suffix == TEMPORARY {
                self.remove(&file_name(*day, TEMPORARY));
            }
        }
        for (day, suffix) in &names {
            if *suffix == OPEN && *day < date && *day >= oldest_kept && let Err(err) = self.finish(*day) {
                self.remove(&file_name(*day, TEMPORARY));
                self.failed("finish", &format!("{}: {err}", file_name(*day, OPEN)));
            }
        }
        for (day, suffix) in names {
            if suffix != TEMPORARY && day < oldest_kept {
                self.remove(&file_name(day, suffix));
            }
        }
    }

    fn names(&self) -> io::Result<Vec<(NaiveDate, &'static str)>> {
        let mut names = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            if let Some(parsed) = entry?.file_name().to_str().and_then(parse_name) {
                names.push(parsed);
            }
        }
        Ok(names)
    }

    /// `day`'s file compressed, checked and published, the original deleted; a copy already published (a crash
    /// between its rename and the delete) is checked again instead of rewritten.
    fn finish(&self, day: NaiveDate) -> io::Result<()> {
        let open = self.dir.join(file_name(day, OPEN));
        let done = self.dir.join(file_name(day, DONE));
        // a crash's partial line is no record: the day finished on its whole lines
        cut_partial_line(&mut OpenOptions::new().read(true).write(true).open(&open)?)?;
        if done.exists() {
            same(&open, &done)?;
        } else {
            let temporary = self.dir.join(file_name(day, TEMPORARY));
            compress(&open, &temporary)?;
            same(&open, &temporary)?;
            fs::rename(&temporary, &done)?;
        }
        fs::remove_file(&open)?;
        info!("basis record: {} finished as {}", file_name(day, OPEN), file_name(day, DONE));
        Ok(())
    }

    fn remove(&mut self, name: &str) {
        if let Err(err) = fs::remove_file(self.dir.join(name)) {
            self.failed("remove", &format!("{name}: {err}"));
        }
    }

    fn failed(&mut self, operation: &str, detail: &str) {
        BASIS_RECORD_ERRORS_TOTAL.with_label_values(&[operation]).inc();
        if self.last_warning.is_none_or(|at| at.elapsed() >= WARN_EVERY) {
            warn!("basis record {operation} failed (counted; at most one warning a minute): {detail}");
            self.last_warning = Some(Instant::now());
        }
    }
}

/// The rows as lines, serialized before any byte is written.
fn lines<R: Serialize>(rows: &[R]) -> serde_json::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for row in rows {
        serde_json::to_writer(&mut bytes, row)?;
        bytes.push(b'\n');
    }
    Ok(bytes)
}

/// Cuts `file` back to just after its last newline - nothing without one - and gives that length.
fn cut_partial_line(file: &mut File) -> io::Result<u64> {
    let length = file.metadata()?.len();
    let mut end = length;
    let mut buffer = vec![0; CHUNK];
    let whole = loop {
        if end == 0 {
            break 0;
        }
        let start = end.saturating_sub(CHUNK as u64);
        let size = usize::try_from(end - start).unwrap_or_else(|_| unreachable!("at most a chunk"));
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut buffer[..size])?;
        if let Some(newline) = buffer[..size].iter().rposition(|byte| *byte == b'\n') {
            break start + newline as u64 + 1;
        }
        end = start;
    };
    if whole != length {
        file.set_len(whole)?;
        warn!("basis record: a partial line of {} bytes cut from a file's end", length - whole);
    }
    Ok(whole)
}

fn compress(from: &Path, to: &Path) -> io::Result<()> {
    let mut input = BufReader::new(File::open(from)?);
    let mut encoder = zstd::stream::write::Encoder::new(BufWriter::new(File::create(to)?), LEVEL)?;
    io::copy(&mut input, &mut encoder)?;
    let mut out = encoder.finish()?;
    out.flush()?;
    out.get_ref().sync_all()
}

/// `Ok` when the decompressed `compressed` is `original` byte for byte.
fn same(original: &Path, compressed: &Path) -> io::Result<()> {
    let mut plain = BufReader::new(File::open(original)?);
    let mut unpacked = zstd::stream::read::Decoder::new(File::open(compressed)?)?;
    let (mut left, mut right) = (vec![0; CHUNK], vec![0; CHUNK]);
    loop {
        let read = fill(&mut plain, &mut left)?;
        if fill(&mut unpacked, &mut right[..read])? != read || left[..read] != right[..read] {
            return Err(io::Error::other(format!("{} decompressed differs from {}", compressed.display(), original.display())));
        }
        if read < CHUNK {
            let mut extra = [0_u8; 1];
            return if unpacked.read(&mut extra)? == 0 { Ok(()) } else { Err(io::Error::other(format!("{} decompressed is longer", compressed.display()))) };
        }
    }
}

/// Reads until `buffer` is full or the input ends; the bytes read.
fn fill(input: &mut impl Read, buffer: &mut [u8]) -> io::Result<usize> {
    let mut read = 0;
    while read < buffer.len() {
        match input.read(&mut buffer[read..])? {
            0 => break,
            count => read += count,
        }
    }
    Ok(read)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    const DAY_MS: u64 = 86_400_000;
    /// 2026-11-05T00:00:00Z.
    const NOV_5: u64 = 1_793_836_800_000;

    #[derive(Serialize)]
    struct Row {
        t_ms: u64,
        coin: &'static str,
    }

    fn scratch() -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!("basis_record_{}_{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn tick(t_ms: u64, coins: &[&'static str]) -> Tick<Row> {
        Tick { t_ms, rows: coins.iter().map(|coin| Row { t_ms, coin }).collect() }
    }

    fn listing(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(dir).unwrap().map(|entry| entry.unwrap().file_name().into_string().unwrap()).collect();
        names.sort();
        names
    }

    fn unpacked(path: &Path) -> String {
        let mut text = String::new();
        zstd::stream::read::Decoder::new(File::open(path).unwrap()).unwrap().read_to_string(&mut text).unwrap();
        text
    }

    #[test]
    fn rows_go_to_their_days_file_and_a_new_day_finishes_the_last() {
        let dir = scratch();
        let mut files = Files::new(dir.clone(), 90);
        files.finish_before(date_of(NOV_5));
        files.write(&tick(NOV_5 + 10_000, &["BTC", "xyz:SNDK"]));
        files.write(&tick(NOV_5 + 20_000, &["BTC"]));
        let day_one = fs::read_to_string(dir.join("basis_record.2026-11-05.jsonl")).unwrap();
        assert_eq!(day_one.lines().count(), 3);
        assert_eq!(day_one.lines().next().unwrap(), r#"{"t_ms":1793836810000,"coin":"BTC"}"#);
        files.write(&tick(NOV_5 + DAY_MS, &["BTC"]));
        assert_eq!(listing(&dir), vec!["basis_record.2026-11-05.jsonl.zst", "basis_record.2026-11-06.jsonl"]);
        assert_eq!(unpacked(&dir.join("basis_record.2026-11-05.jsonl.zst")), day_one, "the compressed day is the day's lines");
    }

    /// Review 000751 r1 P2: a day closed by a stop past midnight is finished at the next start.
    #[test]
    fn a_start_finishes_the_days_a_stop_left_open() {
        let dir = scratch();
        let mut before = Files::new(dir.clone(), 90);
        before.write(&tick(NOV_5 + 5_000, &["BTC"]));
        drop(before);
        let mut after = Files::new(dir.clone(), 90);
        after.finish_before(date_of(NOV_5 + DAY_MS + 60_000));
        assert_eq!(listing(&dir), vec!["basis_record.2026-11-05.jsonl.zst"]);
        after.write(&tick(NOV_5 + DAY_MS + 60_000, &["BTC"]));
        after.write(&tick(NOV_5 + DAY_MS + 70_000, &["BTC"]));
        assert_eq!(fs::read_to_string(dir.join("basis_record.2026-11-06.jsonl")).unwrap().lines().count(), 2, "appended");
    }

    /// Review 000751 r1 P2: a failed finishing keeps the original and tries again later; a crash's temporary copy goes.
    #[test]
    fn a_failed_finishing_keeps_the_day_and_is_tried_again() {
        let dir = scratch();
        fs::write(dir.join("basis_record.2026-11-04.jsonl"), "{\"a\":1}\n").unwrap();
        fs::write(dir.join("basis_record.2026-11-04.jsonl.zst.tmp"), "half a copy").unwrap();
        // a directory where the published copy goes: the rename fails
        fs::create_dir(dir.join("basis_record.2026-11-03.jsonl.zst")).unwrap();
        fs::write(dir.join("basis_record.2026-11-03.jsonl"), "{\"b\":2}\n").unwrap();
        let mut files = Files::new(dir.clone(), 90);
        files.finish_before(date_of(NOV_5));
        assert_eq!(listing(&dir), vec!["basis_record.2026-11-03.jsonl", "basis_record.2026-11-03.jsonl.zst", "basis_record.2026-11-04.jsonl.zst"]);
        assert_eq!(unpacked(&dir.join("basis_record.2026-11-04.jsonl.zst")), "{\"a\":1}\n");
        fs::remove_dir(dir.join("basis_record.2026-11-03.jsonl.zst")).unwrap();
        files.finish_before(date_of(NOV_5));
        assert_eq!(listing(&dir), vec!["basis_record.2026-11-03.jsonl.zst", "basis_record.2026-11-04.jsonl.zst"]);
    }

    #[test]
    fn days_past_the_kept_ones_are_deleted_compressed_or_not() {
        let dir = scratch();
        for name in ["basis_record.2026-11-01.jsonl.zst", "basis_record.2026-11-01.jsonl", "basis_record.2026-11-02.jsonl.zst", "notes.txt"] {
            fs::write(dir.join(name), "x").unwrap();
        }
        // kept: 2026-11-02 to 2026-11-05 (today minus 3 days on)
        Files::new(dir.clone(), 3).finish_before(date_of(NOV_5));
        assert_eq!(listing(&dir), vec!["basis_record.2026-11-02.jsonl.zst", "notes.txt"]);
    }

    #[test]
    fn a_published_copy_left_with_its_original_is_checked_not_rewritten() {
        let dir = scratch();
        fs::write(dir.join("basis_record.2026-11-04.jsonl"), "{\"a\":1}\n").unwrap();
        compress(&dir.join("basis_record.2026-11-04.jsonl"), &dir.join("basis_record.2026-11-04.jsonl.zst")).unwrap();
        fs::write(dir.join("basis_record.2026-11-03.jsonl"), "{\"b\":2}\n").unwrap();
        fs::write(dir.join("basis_record.2026-11-03.jsonl.zst"), "not its copy").unwrap();
        Files::new(dir.clone(), 90).finish_before(date_of(NOV_5));
        assert_eq!(listing(&dir), vec!["basis_record.2026-11-03.jsonl", "basis_record.2026-11-03.jsonl.zst", "basis_record.2026-11-04.jsonl.zst"]);
    }

    /// Every line of the text parses: no row joined to a fragment.
    fn whole(text: &str) -> usize {
        for line in text.lines() {
            if let Err(err) = serde_json::from_str::<serde_json::Value>(line) {
                panic!("{line:?}: {err}");
            }
        }
        text.lines().count()
    }

    /// Review 000752 r1 P2: a write that failed part way is cut back to the last whole tick; the next tick lands whole.
    #[test]
    fn a_failed_write_is_cut_back_and_the_next_tick_lands_whole() {
        let dir = scratch();
        let mut files = Files::new(dir.clone(), 90);
        files.write(&tick(NOV_5 + 10_000, &["BTC", "ETH"]));
        // a full disk wrote part of the next tick, then failed
        let path = dir.join("basis_record.2026-11-05.jsonl");
        OpenOptions::new().append(true).open(&path).unwrap().write_all(br#"{"t_ms":1793836820000,"co"#).unwrap();
        files.rewind();
        files.write(&tick(NOV_5 + 30_000, &["BTC"]));
        assert_eq!(whole(&fs::read_to_string(&path).unwrap()), 3, "two of the first tick, one of the third");
    }

    /// Review 000752 r1 P2: a partial line a crash left at a file's end is cut when the file is opened again, and when an
    /// earlier day's file is finished.
    #[test]
    fn a_crashs_partial_line_is_cut_before_appending_or_finishing() {
        let dir = scratch();
        fs::write(dir.join("basis_record.2026-11-05.jsonl"), "{\"a\":1}\n{\"a\":2}\n{\"a\":").unwrap();
        fs::write(dir.join("basis_record.2026-11-04.jsonl"), "{\"b\":1}\n{\"b\"").unwrap();
        let mut files = Files::new(dir.clone(), 90);
        files.finish_before(date_of(NOV_5));
        assert_eq!(unpacked(&dir.join("basis_record.2026-11-04.jsonl.zst")), "{\"b\":1}\n");
        files.write(&tick(NOV_5 + 10_000, &["BTC"]));
        assert_eq!(whole(&fs::read_to_string(dir.join("basis_record.2026-11-05.jsonl")).unwrap()), 3);
        fs::write(dir.join("basis_record.2026-11-03.jsonl"), "no newline at all").unwrap();
        files.finish_before(date_of(NOV_5));
        assert_eq!(unpacked(&dir.join("basis_record.2026-11-03.jsonl.zst")), "", "a file without a whole line finishes empty");
    }

    #[test]
    fn a_full_queue_drops_the_tick_and_never_waits() {
        let (sender, receiver) = sync_channel(QUEUE_TICKS);
        let writer = RecordWriter { sender };
        let before = BASIS_RECORD_DROPPED_TOTAL.with_label_values(&["full"]).get();
        for n in 0..5 {
            writer.offer(tick(NOV_5 + n, &["BTC"]));
        }
        assert_eq!(BASIS_RECORD_DROPPED_TOTAL.with_label_values(&["full"]).get() - before, 2);
        assert_eq!(receiver.try_iter().count(), QUEUE_TICKS);
    }

    #[test]
    fn names_of_other_files_are_not_the_records() {
        assert_eq!(parse_name("basis_record.2026-11-05.jsonl"), Some((NaiveDate::from_ymd_opt(2026, 11, 5).unwrap(), OPEN)));
        assert_eq!(parse_name("basis_record.2026-11-05.jsonl.zst.tmp").map(|(_, suffix)| suffix), Some(TEMPORARY));
        for other in ["basis_record.2026-13-05.jsonl", "basis_record.2026-11-05.json", "x.2026-11-05.jsonl", "basis_record.jsonl"] {
            assert_eq!(parse_name(other), None, "{other}");
        }
    }
}
