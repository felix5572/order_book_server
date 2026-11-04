//! The basis windows' samples across OB restarts (user 2026-11-04 "OB 服务借助 redis 处理重启", hyperliquid-trade design
//! 000741 r2), in a Redis of this host (`--basis-store-redis`): every window's sample of each tick written as it is
//! taken, and an earlier run's samples taken back before the owner's first publish. A cache of the owner's own: a
//! store unreachable starts the windows empty, as without one - every basis withheld below its samples, never a
//! value made up -, and the book serves alike.
//!
//! Each sample is a field of its bucket's hash - its series, `"<sampled at, wall-clock ms> <ratio>"` -, one bucket a
//! second of the 10-minute windows' samples (`s1`), one every 10 s of the longer windows' (`s10`), each expiring 60 s
//! after its window passes it (review 000741 r1 P2: the windows sample at ticks of their own, a bucket's series written
//! at different ticks, none overwriting another). A series samples at most once in its bucket's span while the wall
//! clock runs on; set back across a bucket's edge, its later sample overwrites the earlier one there (r2 P3).
//!
//! The writes are the writer task's, never the owner's: the owner hands each tick's samples over a bounded queue, a
//! full one dropping them (`orderbook_basis_store_dropped_total`); a write's error is counted and logged at most once a
//! minute, with Redis' own words.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use log::{info, warn};
use redis::aio::ConnectionManager;
use tokio::{
    sync::mpsc::{self, error::TrySendError},
    time::{Instant, timeout, timeout_at},
};

use super::{
    external_basis::{LONG_WINDOWS, WINDOW},
    hip3::protocol::EquityFeed,
    snapshot::{CoinTable, OwnerState},
};
use crate::metrics::{BASIS_STORE_DROPPED_TOTAL, BASIS_STORE_ERRORS_TOTAL};

/// What a restore may take - connecting, reading, the windows rebuilt - before the owner starts without it.
pub(crate) const RESTORE_BUDGET: Duration = Duration::from_secs(5);
const KEY_PREFIX: &str = "ob:basis:v1";
/// A bucket lives this long past its window.
const KEY_MARGIN: Duration = Duration::from_secs(60);
/// Ticks of samples waiting for the writer: past them a tick's are dropped.
const QUEUE: usize = 30;
/// A write's own budget, its connection's included.
const WRITE_BUDGET: Duration = Duration::from_secs(2);
const WARN_EVERY: Duration = Duration::from_secs(60);
/// Buckets read in one round trip: a 10-minute bucket holds every window's sample of its second (~50 KB), each answer
/// kept well inside the client's own response timeout (review 000743 r1: `redis` 1.2.4, 500 ms).
const READ_CHUNK: usize = 50;

/// The windows a sample is of: the 10-minute ones, a sample a second, or the longer ones, one every 10 s.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Cadence {
    Second,
    TenSeconds,
}

impl Cadence {
    const ALL: [Self; 2] = [Self::Second, Self::TenSeconds];

    const fn tag(self) -> &'static str {
        match self {
            Self::Second => "s1",
            Self::TenSeconds => "s10",
        }
    }

    const fn bucket_ms(self) -> u64 {
        match self {
            Self::Second => 1_000,
            Self::TenSeconds => 10_000,
        }
    }

    /// The longest window its samples are of.
    const fn span(self) -> Duration {
        match self {
            Self::Second => WINDOW,
            Self::TenSeconds => LONG_WINDOWS[1],
        }
    }

    fn key(self, bucket: u64) -> String {
        format!("{KEY_PREFIX}:{}:{bucket}", self.tag())
    }
}

/// A window's sample as stored: its series, when it was taken by the wall clock, its ratio.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StoredSample {
    pub(crate) cadence: Cadence,
    pub(crate) series: Arc<str>,
    pub(crate) at_ms: u64,
    pub(crate) ratio: f64,
}

/// A window of the owner's state.
#[derive(Debug, Clone, Copy)]
enum WindowRef {
    Main(usize),
    Venue(usize, usize),
    Hip3Binance(usize),
    Hip3BinanceLong(usize),
    Equity(usize, EquityFeed),
}

/// Every window of the owner's state by its series: `main:<coin>`, `venue:<coin>:<venue>`, `hip3:<coin>:binance` (its
/// 10-minute window and its longer ones), `hip3:<coin>:<sip|boats>`.
pub(crate) struct SeriesTable {
    windows: Vec<(Cadence, Arc<str>, WindowRef)>,
    by_series: HashMap<(Cadence, Arc<str>), usize>,
}

impl SeriesTable {
    pub(crate) fn new(table: &CoinTable) -> Self {
        let mut windows = Vec::new();
        for (index, coin) in table.names().iter().enumerate() {
            windows.push((Cadence::Second, Arc::from(format!("main:{coin}")), WindowRef::Main(index)));
            for (listing, venue) in table.perp_venues(index).enumerate() {
                windows.push((Cadence::Second, Arc::from(format!("venue:{coin}:{}", venue.name())), WindowRef::Venue(index, listing)));
            }
        }
        for (index, coin) in table.hip3_external().names().iter().enumerate() {
            let binance: Arc<str> = Arc::from(format!("hip3:{coin}:binance"));
            windows.push((Cadence::Second, Arc::clone(&binance), WindowRef::Hip3Binance(index)));
            windows.push((Cadence::TenSeconds, binance, WindowRef::Hip3BinanceLong(index)));
            for feed in EquityFeed::ALL {
                windows.push((Cadence::Second, Arc::from(format!("hip3:{coin}:{}", feed.name())), WindowRef::Equity(index, feed)));
            }
        }
        let by_series = windows.iter().enumerate().map(|(place, (cadence, series, _))| ((*cadence, Arc::clone(series)), place)).collect();
        Self { windows, by_series }
    }

    /// The samples the windows took this tick, at `at_ms`, each handed over once.
    pub(crate) fn entered(&self, state: &mut OwnerState, at_ms: u64) -> Vec<StoredSample> {
        let entered = |window: WindowRef, state: &mut OwnerState| match window {
            WindowRef::Main(index) => state.external_basis[index].take_entered(),
            WindowRef::Venue(index, listing) => state.venue_basis[index][listing].take_entered(),
            WindowRef::Hip3Binance(index) => state.hip3_external[index].windows_mut().0.take_entered(),
            WindowRef::Hip3BinanceLong(index) => state.hip3_external[index].windows_mut().1.take_entered(),
            WindowRef::Equity(index, feed) => state.hip3_external[index].windows_mut().2.of_mut(feed).take_entered(),
        };
        self.windows
            .iter()
            .filter_map(|(cadence, series, window)| {
                entered(*window, state).map(|ratio| StoredSample { cadence: *cadence, series: Arc::clone(series), at_ms, ratio })
            })
            .collect()
    }

    /// Takes back an earlier run's samples into windows that have taken none yet: each sample taken `now_ms` - its
    /// sampling time ago, by the monotonic clock from `now`, within its window's span; a series no longer in the table
    /// left out.
    pub(crate) fn restore(&self, state: &mut OwnerState, samples: Vec<StoredSample>, now_ms: u64, now: Instant) -> Restored {
        let mut restored = Restored::default();
        let mut by_window: BTreeMap<usize, Vec<(u64, f64)>> = BTreeMap::new();
        for sample in samples {
            let Some(&place) = self.by_series.get(&(sample.cadence, Arc::clone(&sample.series))) else {
                restored.unknown += 1;
                continue;
            };
            let age = now_ms.checked_sub(sample.at_ms).map(Duration::from_millis);
            if age.is_none_or(|age| age > sample.cadence.span()) {
                restored.outside += 1;
                continue;
            }
            by_window.entry(place).or_default().push((sample.at_ms, sample.ratio));
        }
        for (place, mut samples) in by_window {
            samples.sort_by_key(|&(at_ms, _)| at_ms);
            let at = |at_ms: u64| now.checked_sub(Duration::from_millis(now_ms - at_ms));
            let samples: Vec<(Instant, f64)> = samples.into_iter().filter_map(|(at_ms, ratio)| at(at_ms).map(|at| (at, ratio))).collect();
            restored.samples += samples.len();
            match self.windows[place].2 {
                WindowRef::Main(index) => state.external_basis[index].restore(&samples),
                WindowRef::Venue(index, listing) => state.venue_basis[index][listing].restore(&samples),
                WindowRef::Hip3Binance(index) => state.hip3_external[index].windows_mut().0.restore(&samples),
                WindowRef::Hip3BinanceLong(index) => state.hip3_external[index].windows_mut().1.restore(&samples, now),
                WindowRef::Equity(index, feed) => state.hip3_external[index].windows_mut().2.of_mut(feed).restore(&samples),
            }
        }
        restored
    }
}

/// What a restore took back and left out.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Restored {
    pub(crate) samples: usize,
    /// Past their window's span, or taken after now by the wall clock.
    pub(crate) outside: usize,
    /// Of a series no longer in the table.
    pub(crate) unknown: usize,
}

/// One tick's samples as their buckets' fields: each bucket's key, its time to live and its fields.
fn writes(samples: &[StoredSample]) -> BTreeMap<String, (u64, Vec<(Arc<str>, String)>)> {
    let mut buckets: BTreeMap<String, (u64, Vec<(Arc<str>, String)>)> = BTreeMap::new();
    for sample in samples {
        let key = sample.cadence.key(sample.at_ms / sample.cadence.bucket_ms());
        let ttl = (sample.cadence.span() + KEY_MARGIN).as_secs();
        buckets.entry(key).or_insert_with(|| (ttl, Vec::new())).1.push((Arc::clone(&sample.series), format!("{} {}", sample.at_ms, sample.ratio)));
    }
    buckets
}

/// A bucket's fields as written (`writes`); a field off that form refuses the whole store's restore.
fn parse_bucket(cadence: Cadence, key: &str, fields: HashMap<String, String>) -> Result<Vec<StoredSample>, String> {
    fields
        .into_iter()
        .map(|(series, value)| {
            let off = || format!("{key} field {series:?}: {value:?} is not \"<ms> <ratio>\"");
            let (at_ms, ratio) = value.split_once(' ').ok_or_else(off)?;
            Ok(StoredSample { cadence, series: Arc::from(series.as_str()), at_ms: at_ms.parse().map_err(|_| off())?, ratio: ratio.parse().map_err(|_| off())? })
        })
        .collect()
}

/// Every bucket that may hold a sample within its window's span at `now_ms`, each cadence's, oldest first.
fn buckets_at(now_ms: u64) -> Vec<(Cadence, String)> {
    Cadence::ALL
        .into_iter()
        .flat_map(|cadence| {
            let bucket_ms = cadence.bucket_ms();
            let span_ms = u64::try_from(cadence.span().as_millis()).unwrap_or_else(|_| unreachable!("a window of hours"));
            let first = now_ms.saturating_sub(span_ms) / bucket_ms;
            (first..=now_ms / bucket_ms).map(move |bucket| (cadence, cadence.key(bucket)))
        })
        .collect()
}

/// An earlier run's samples within their windows' spans at `now_ms`.
pub(crate) async fn read(client: &redis::Client, now_ms: u64) -> Result<Vec<StoredSample>, String> {
    let mut connection = client.get_multiplexed_async_connection().await.map_err(|error| format!("connecting: {error}"))?;
    let buckets = buckets_at(now_ms);
    let mut samples = Vec::new();
    for chunk in buckets.chunks(READ_CHUNK) {
        let mut pipe = redis::pipe();
        for (_, key) in chunk {
            pipe.hgetall(key);
        }
        let fields: Vec<HashMap<String, String>> = pipe.query_async(&mut connection).await.map_err(|error| format!("reading buckets: {error}"))?;
        for ((cadence, key), fields) in chunk.iter().zip(fields) {
            samples.extend(parse_bucket(*cadence, key, fields)?);
        }
    }
    Ok(samples)
}

/// Restores `state`'s windows from `client`'s store within `RESTORE_BUDGET` - connecting, reading, the windows rebuilt
/// (review 000743 r1 P3: rebuilt into a copy, taken only within the budget) -; the windows left empty, said, when it
/// cannot.
pub(crate) async fn restore(client: &redis::Client, series: &SeriesTable, state: &mut OwnerState, now_ms: impl Fn() -> u64) {
    let started = Instant::now();
    let deadline = started + RESTORE_BUDGET;
    let budget = || format!("past the {} s budget", RESTORE_BUDGET.as_secs());
    let rebuilt = match timeout_at(deadline, read(client, now_ms())).await {
        Ok(Ok(samples)) => {
            let mut rebuilt = state.clone();
            let restored = series.restore(&mut rebuilt, samples, now_ms(), Instant::now());
            if Instant::now() > deadline { Err(format!("rebuilt {} ms in, {}", started.elapsed().as_millis(), budget())) } else { Ok((rebuilt, restored)) }
        }
        Ok(Err(error)) => Err(error),
        Err(_) => Err(format!("no answer {}", budget())),
    };
    match rebuilt {
        Ok((rebuilt, restored)) => {
            *state = rebuilt;
            info!(
                "basis store: restored {} samples in {} ms ({} past their window, {} of series no longer listed)",
                restored.samples,
                started.elapsed().as_millis(),
                restored.outside,
                restored.unknown
            );
        }
        Err(error) => {
            BASIS_STORE_ERRORS_TOTAL.with_label_values(&["restore"]).inc();
            warn!("basis store: nothing restored, every basis window starts empty: {error}");
        }
    }
}

/// The writer task's end of the queue: the owner's.
pub(crate) struct BasisWriter {
    queue: mpsc::Sender<Vec<StoredSample>>,
}

impl BasisWriter {
    /// Starts the writer task over `client`.
    pub(crate) fn spawn(client: redis::Client) -> Self {
        let (queue, samples) = mpsc::channel(QUEUE);
        tokio::spawn(write_samples(client, samples));
        Self { queue }
    }

    /// Hands a tick's samples to the writer, never waiting: a full queue, or the writer gone, drops them, counted.
    pub(crate) fn offer(&self, samples: Vec<StoredSample>) {
        if samples.is_empty() {
            return;
        }
        match self.queue.try_send(samples) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => BASIS_STORE_DROPPED_TOTAL.with_label_values(&["queue_full"]).inc(),
            Err(TrySendError::Closed(_)) => BASIS_STORE_DROPPED_TOTAL.with_label_values(&["writer_gone"]).inc(),
        }
    }
}

async fn write_samples(client: redis::Client, mut samples: mpsc::Receiver<Vec<StoredSample>>) {
    let mut connection: Option<ConnectionManager> = None;
    let mut warned: Option<Instant> = None;
    while let Some(batch) = samples.recv().await {
        let written = timeout(WRITE_BUDGET, write(&client, &mut connection, &batch)).await.unwrap_or_else(|_| Err(format!("no answer within {} s", WRITE_BUDGET.as_secs())));
        let Err(error) = written else { continue };
        BASIS_STORE_ERRORS_TOTAL.with_label_values(&["write"]).inc();
        if warned.is_none_or(|at| at.elapsed() >= WARN_EVERY) {
            warn!("basis store: a tick's {} samples not written (said at most once a minute): {error}", batch.len());
            warned = Some(Instant::now());
        }
    }
}

/// One tick's samples, each bucket's fields set and its time to live renewed in one transaction.
async fn write(client: &redis::Client, connection: &mut Option<ConnectionManager>, batch: &[StoredSample]) -> Result<(), String> {
    if connection.is_none() {
        *connection = Some(ConnectionManager::new(client.clone()).await.map_err(|error| format!("connecting: {error}"))?);
    }
    let manager = connection.as_mut().unwrap_or_else(|| unreachable!("connected above"));
    let mut pipe = redis::pipe();
    pipe.atomic();
    for (key, (ttl, fields)) in writes(batch) {
        let fields: Vec<(&str, String)> = fields.iter().map(|(series, value)| (series.as_ref(), value.clone())).collect();
        pipe.hset_multiple(&key, &fields).ignore();
        pipe.expire(&key, i64::try_from(ttl).unwrap_or_else(|_| unreachable!("hours of seconds"))).ignore();
    }
    pipe.query_async::<()>(manager).await.map_err(|error| format!("writing: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_marks::external_basis::{ExternalBasisWindow, LongBasisWindow, MIN_SAMPLES};

    /// The store as Redis keeps it: each bucket's fields, a field written again replaced.
    #[derive(Default)]
    struct Buckets(HashMap<String, HashMap<String, String>>);

    impl Buckets {
        fn write(&mut self, samples: &[StoredSample]) {
            for (key, (_, fields)) in writes(samples) {
                let bucket = self.0.entry(key).or_default();
                bucket.extend(fields.into_iter().map(|(series, value)| (series.to_string(), value)));
            }
        }

        fn read(&self, now_ms: u64) -> Vec<StoredSample> {
            buckets_at(now_ms).into_iter().filter_map(|(cadence, key)| self.0.get(&key).map(|fields| parse_bucket(cadence, &key, fields.clone()).unwrap())).flatten().collect()
        }
    }

    /// Review 000741 r1 P2's case: A samples at +50 ms each second, B at +250 ms - two ticks of each bucket -, and A
    /// holds 0 bp three minutes, then +20 bp two; B stops after three. Every sample of both comes back, each window as
    /// in memory: count and median, the 10-minute ones' and the longer ones'.
    #[test]
    fn series_sampled_at_ticks_of_their_own_share_a_bucket_each_kept() {
        let start = Instant::now();
        const START_MS: u64 = 1_793_000_000_000;
        let (mut a, mut b) = (ExternalBasisWindow::default(), ExternalBasisWindow::default());
        let (mut a_long, mut b_long) = (LongBasisWindow::default(), LongBasisWindow::default());
        let mut store = Buckets::default();
        let series = |name: &str| -> Arc<str> { Arc::from(name) };
        for tick in 0..6_000u64 {
            let (at, at_ms) = (start + Duration::from_millis(50 * tick), START_MS + 50 * tick);
            let a_ratio = if tick < 3_600 { 0.0 } else { 0.002 };
            let a_sample = (tick % 20 == 1).then_some(a_ratio);
            let b_sample = (tick % 20 == 5 && tick < 3_600).then_some(0.001);
            a.observe(at, a_sample);
            a_long.observe(at, a_sample);
            b.observe(at, b_sample);
            b_long.observe(at, b_sample);
            let mut entered = Vec::new();
            let mut take = |cadence, name: &str, ratio: Option<f64>| entered.extend(ratio.map(|ratio| StoredSample { cadence, series: series(name), at_ms, ratio }));
            take(Cadence::Second, "a", a.take_entered());
            take(Cadence::TenSeconds, "a", a_long.take_entered());
            take(Cadence::Second, "b", b.take_entered());
            take(Cadence::TenSeconds, "b", b_long.take_entered());
            store.write(&entered);
        }
        let (now, now_ms) = (start + Duration::from_millis(50 * 5_999), START_MS + 50 * 5_999);
        let samples = store.read(now_ms);
        let restored = |name: &str, cadence| -> Vec<(Instant, f64)> {
            let mut kept: Vec<_> = samples.iter().filter(|s| &*s.series == name && s.cadence == cadence).map(|s| (now - Duration::from_millis(now_ms - s.at_ms), s.ratio)).collect();
            kept.sort_by(|x, y| x.0.cmp(&y.0));
            kept
        };
        let (mut a_again, mut b_again) = (ExternalBasisWindow::default(), ExternalBasisWindow::default());
        let (mut a_long_again, mut b_long_again) = (LongBasisWindow::default(), LongBasisWindow::default());
        a_again.restore(&restored("a", Cadence::Second));
        b_again.restore(&restored("b", Cadence::Second));
        a_long_again.restore(&restored("a", Cadence::TenSeconds), now);
        b_long_again.restore(&restored("b", Cadence::TenSeconds), now);
        assert_eq!(a.published().unwrap().samples, 300);
        assert_eq!(a.published().unwrap().ratio, 0.0, "180 of 0 against 120 of +20 bp");
        assert_eq!(a_again.published(), a.published());
        assert_eq!(b_again.published(), b.published());
        assert_eq!(a_long_again.published(), a_long.published());
        assert_eq!(b_long_again.published(), b_long.published());
        assert!(b.published().unwrap().samples >= MIN_SAMPLES);
    }

    /// A bucket's field off its form refuses the restore; one in form reads back exact, f64's shortest round trip.
    #[test]
    fn a_bucket_reads_back_exact_and_refuses_a_field_off_its_form() {
        let sample = StoredSample { cadence: Cadence::Second, series: Arc::from("main:BTC"), at_ms: 1_793_000_000_123, ratio: -0.000_123_456_789_012_345_6 };
        let mut store = Buckets::default();
        store.write(std::slice::from_ref(&sample));
        assert_eq!(store.read(1_793_000_000_500), vec![sample]);
        let fields = HashMap::from([("main:BTC".to_string(), "1793000000123".to_string())]);
        assert_eq!(parse_bucket(Cadence::Second, "k", fields).unwrap_err(), "k field \"main:BTC\": \"1793000000123\" is not \"<ms> <ratio>\"");
    }

    /// Each bucket's key, its fields and its time to live: its window's span and a minute.
    #[test]
    fn a_ticks_samples_go_to_their_buckets() {
        let sample = |cadence, series: &str, at_ms| StoredSample { cadence, series: Arc::from(series), at_ms, ratio: 0.5 };
        let buckets = writes(&[sample(Cadence::Second, "a", 1_999), sample(Cadence::Second, "b", 1_000), sample(Cadence::TenSeconds, "a", 19_999)]);
        let keys: Vec<(&str, u64, usize)> = buckets.iter().map(|(key, (ttl, fields))| (key.as_str(), *ttl, fields.len())).collect();
        assert_eq!(keys, vec![("ob:basis:v1:s10:1", 14_460, 1), ("ob:basis:v1:s1:1", 660, 2)], "the keys' order");
        assert_eq!(buckets["ob:basis:v1:s1:1"].1[0].1, "1999 0.5");
    }

    /// The buckets read: the 10-minute windows' last 600 s and the longer ones' last 14,400 s, each the bucket holding
    /// its span's start and the one of now.
    #[test]
    fn a_restore_reads_each_cadences_span() {
        let buckets = buckets_at(20_000_000);
        let count = |cadence| buckets.iter().filter(|(of, _)| *of == cadence).count();
        assert_eq!((count(Cadence::Second), count(Cadence::TenSeconds)), (601, 1_441));
        assert_eq!(buckets.first().map(|(_, key)| key.as_str()), Some("ob:basis:v1:s1:19400"));
        assert_eq!(buckets.last().map(|(_, key)| key.as_str()), Some("ob:basis:v1:s10:2000"));
    }

    /// A store that cannot be reached is an error at once, for the owner to start empty.
    #[tokio::test]
    async fn an_unreachable_store_is_an_error() {
        let client = redis::Client::open("redis://127.0.0.1:1/0").unwrap();
        let read = timeout(RESTORE_BUDGET, read(&client, 1_793_000_000_000)).await.expect("refused at once, not waited out");
        assert!(read.unwrap_err().starts_with("connecting: "));
    }

    /// Against a real Redis (`OB_BASIS_STORE_TEST_REDIS`, e.g. `redis://127.0.0.1:6379/15`): what the writer wrote reads
    /// back exact, each bucket expiring its window and a minute on - its series this run's alone (`test:<ms>:`), an
    /// earlier run's still in their windows.
    #[tokio::test]
    #[ignore = "needs a Redis: OB_BASIS_STORE_TEST_REDIS"]
    async fn a_real_store_reads_back_what_the_writer_wrote() {
        let url = std::env::var("OB_BASIS_STORE_TEST_REDIS").expect("OB_BASIS_STORE_TEST_REDIS");
        let client = redis::Client::open(url.as_str()).unwrap();
        let now_ms = crate::local_marks::now_ms();
        let run = format!("test:{now_ms}:");
        let sample = |cadence, series: &str, ago_ms: u64, ratio| StoredSample { cadence, series: Arc::from(format!("{run}{series}")), at_ms: now_ms - ago_ms, ratio };
        let mut written: Vec<StoredSample> = (0..5u32)
            .flat_map(|i| {
                let ago = u64::from(i) * 1_000;
                [sample(Cadence::Second, "a", ago + 50, f64::from(i) * 1e-4), sample(Cadence::Second, "b", ago + 250, -f64::from(i) * 1e-5)]
            })
            .chain([sample(Cadence::TenSeconds, "a", 3_000, 0.000_123)])
            .collect();
        write(&client, &mut None, &written).await.unwrap();
        let mut read_back: Vec<StoredSample> = read(&client, now_ms).await.unwrap().into_iter().filter(|s| s.series.starts_with(&run)).collect();
        let order = |s: &StoredSample| (s.cadence, s.series.clone(), s.at_ms);
        read_back.sort_by_key(order);
        written.sort_by_key(order);
        assert_eq!(read_back, written);
        let mut connection = client.get_multiplexed_async_connection().await.unwrap();
        let key = Cadence::Second.key(written[0].at_ms / 1_000);
        let ttl: i64 = redis::cmd("TTL").arg(&key).query_async(&mut connection).await.unwrap();
        assert!((655..=660).contains(&ttl), "{key}: {ttl}");
    }

    /// The queue full, a tick's samples are dropped and counted, never waited for.
    #[tokio::test]
    async fn a_full_queue_drops_a_ticks_samples() {
        let (queue, _samples) = mpsc::channel(1);
        let writer = BasisWriter { queue };
        let sample = || vec![StoredSample { cadence: Cadence::Second, series: Arc::from("a"), at_ms: 1, ratio: 0.0 }];
        let before = BASIS_STORE_DROPPED_TOTAL.with_label_values(&["queue_full"]).get();
        writer.offer(sample());
        writer.offer(sample());
        assert_eq!(BASIS_STORE_DROPPED_TOTAL.with_label_values(&["queue_full"]).get(), before + 1);
    }
}
