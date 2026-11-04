//! Each main-dex coin's normal basis to the external venues (user 2026-10-30, design 000594): a sample is HL's book
//! midpoint over the midpoint of the external perp quotes' bid and ask medians - 1, taken once a second, and the basis
//! is the median of the samples of the last `WINDOW`, published from `MIN_SAMPLES` on. A reader takes an external side
//! × (1 + ratio) as that side at HL's level: the forced flow trader's catch is not to be more aggressive than it.
//!
//! The median damps the samples a move holds while they are a minority - HL lagging the venues, a liquidation sweeping
//! the book: with the window full and sampled throughout, a lasting move takes about half the window to lead it; after
//! a restart or a gap, its share of the samples in the window decides (review 000595). It follows the basis' drift (bm
//! 10-27..10-30, 176 coins: the 10-minute median moves 1.9 bp in an hour at the median coin). A sample reads the book
//! and the venues only, not the node state: a node behind still samples. Where HL's price grid is coarse (a tick of 2
//! bp and more on 28 coins, 72 bp on HMSTR), its book is often one tick wide and the basis has half a tick's precision
//! (user 2026-10-30: left so).
//!
//! Each perp venue of a coin has its own window too (hyperliquid-trade design 000681): the same rule on that venue's
//! quote alone, its multiplier applied - its lasting offset to HL, Binance's a cancel's reference of its own.
//!
//! The HIP-3 Binance leg has two longer windows besides (user 2026-11-04, hyperliquid-trade design 000741 r2): its 60- and
//! 240-minute medians, of a sample every `LONG_SAMPLE_INTERVAL` - the same tick's sample as the 10-minute window's, the
//! raw samples, not medians of medians -, each published from `LONG_MIN_SAMPLES` on (`LongBasisWindow`): the forced
//! flow trader's quotes take the most conservative of the three.
//!
//! Each window hands over the sample it took at a tick (`take_entered`) and takes back samples of an earlier run
//! (`restore`): the samples' store across OB restarts (`basis_store`).
//!
//! No basis past `MAX_BASIS_RATIO` either way, nor one not finite, is published - every window's, the main dex's
//! medians', each venue's, the HIP-3 Binance and US stock legs' (user 2026-11-02, review 000683 r1-r2: a quote far off
//! HL's, OKX at 10^24 against HL's 100,002, gave a median of -1, an external side × (1 + ratio) at zero): that basis is
//! withheld, `externalBasisOutOfRange`, its quote and every other leg published as ever.

use std::{collections::VecDeque, time::Duration};

use tokio::time::Instant;

use super::{price::VenuePrice, snapshot::Withheld};
use crate::order_book::Px;

pub(crate) const WINDOW: Duration = Duration::from_secs(600);
pub(crate) const MIN_SAMPLES: usize = 120;
pub(crate) const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
/// The longer windows' sampling: one every 10 s, 1,440 over 240 minutes.
pub(crate) const LONG_SAMPLE_INTERVAL: Duration = Duration::from_secs(10);
/// The longer windows, the shorter first: 60 and 240 minutes.
pub(crate) const LONG_WINDOWS: [Duration; 2] = [Duration::from_secs(3_600), Duration::from_secs(14_400)];
/// Each longer window's least samples: ten minutes' (the trader's own long bases needed ten minute samples).
pub(crate) const LONG_MIN_SAMPLES: usize = 60;
/// The largest basis published, either way: a guard against a quote far off HL's, the user's (2026-11-02: "我最多能接受
/// 10% 基差当作防呆"). Far past any seen: bm 11-02 17:04Z's live bases reached 47.5 bp over the main dex's 176 coins,
/// 30 bp over the HIP-3 Binance legs' 92, 36 bp over the SIP legs' 85; the largest ever, HMSTR's +109 bp on 10-30.
///
/// It bounds the window's median of the f64 ratios `sample` computes - HL's midpoint over the external midpoint - 1,
/// each midpoint turned to f64 first -, so only to f64's rounding (about 1e-16 relative): books exactly ±10% apart may
/// fall on either side. That is meant (review 000683 r3, the coordinator's call): a guard has only to tell a normal
/// basis (seen at most about 1%) from one far off (well past 10%), not to own the bound's last digit.
pub(crate) const MAX_BASIS_RATIO: f64 = 0.10;
/// HL book prices are 1e-8.
const BOOK_PRICE_SCALE: f64 = 1e8;

/// The basis published: the samples' median and their count.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ExternalBasis {
    pub(crate) ratio: f64,
    pub(crate) samples: usize,
}

/// HL's book midpoint over the external bid and ask's midpoint - 1: the main dex's and the HIP-3 Binance leg's
/// medians, the HIP-3 US stock leg's quote.
pub(crate) fn sample(bid: Px, ask: Px, external_bid: VenuePrice, external_ask: VenuePrice) -> f64 {
    #[expect(clippy::cast_precision_loss, reason = "book prices are below 2^53")]
    let book_mid = f64::midpoint(bid.value() as f64, ask.value() as f64) / BOOK_PRICE_SCALE;
    let external_midpoint = f64::midpoint(external_bid.to_f64(), external_ask.to_f64());
    book_mid / external_midpoint - 1.0
}

/// One coin's samples of the last `WINDOW` by the monotonic clock and their median: the local marks owner's across
/// ticks, emptied by a restart, refilled from the store (`restore`).
#[derive(Debug, Clone, Default)]
pub(crate) struct ExternalBasisWindow {
    samples: VecDeque<(Instant, f64)>,
    /// The samples' median, recomputed as they change; `None` without a sample.
    median: Option<f64>,
    /// The median's working copy, reused.
    scratch: Vec<f64>,
    /// The sample the last tick took, until handed over (`take_entered`).
    entered: Option<f64>,
}

impl ExternalBasisWindow {
    /// A tick at `now`: the samples older than `WINDOW` leave, and `sample` enters once `SAMPLE_INTERVAL` has passed
    /// since the last one.
    pub(crate) fn observe(&mut self, now: Instant, sample: Option<f64>) {
        let before = self.samples.len();
        while self.samples.front().is_some_and(|&(at, _)| now.saturating_duration_since(at) > WINDOW) {
            self.samples.pop_front();
        }
        let due = self.samples.back().is_none_or(|&(last, _)| now.saturating_duration_since(last) >= SAMPLE_INTERVAL);
        let entered = sample.filter(|_| due);
        self.entered = entered;
        if let Some(ratio) = entered {
            self.samples.push_back((now, ratio));
        }
        if entered.is_some() || self.samples.len() != before {
            self.median = upper_median(&mut self.scratch, self.samples.iter().map(|&(_, ratio)| ratio));
        }
    }

    /// The median once the window holds `MIN_SAMPLES`, by every basis' one rule (`published`).
    pub(crate) fn published(&self) -> Result<ExternalBasis, Withheld> {
        published(self.median, self.samples.len(), MIN_SAMPLES, WINDOW)
    }

    /// The sample the last tick took, once.
    pub(crate) const fn take_entered(&mut self) -> Option<f64> {
        self.entered.take()
    }

    /// Takes back an earlier run's samples, oldest first, into a window that has taken none yet; the next tick moves
    /// the window as ever.
    pub(crate) fn restore(&mut self, samples: &[(Instant, f64)]) {
        assert!(self.samples.is_empty(), "samples restored into a window that has sampled");
        self.samples.extend(samples.iter().copied());
        self.median = upper_median(&mut self.scratch, self.samples.iter().map(|&(_, ratio)| ratio));
    }
}

/// The HIP-3 Binance leg's longer windows (design 000741 r2): one sample every `LONG_SAMPLE_INTERVAL` over the longest
/// of `LONG_WINDOWS`, a median of each window's samples, each published from `LONG_MIN_SAMPLES` on.
#[derive(Debug, Clone, Default)]
pub(crate) struct LongBasisWindow {
    samples: VecDeque<(Instant, f64)>,
    /// Each window's median and samples, recomputed as its samples change.
    medians: [(Option<f64>, usize); 2],
    scratch: Vec<f64>,
    entered: Option<f64>,
}

/// The longer windows' bases, the 60-minute one first.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LongerBases {
    pub(crate) hour: Result<ExternalBasis, Withheld>,
    pub(crate) four_hours: Result<ExternalBasis, Withheld>,
}

impl LongBasisWindow {
    /// A tick at `now`: the samples older than the longest window leave, and `sample` enters once
    /// `LONG_SAMPLE_INTERVAL` has passed since the last one; each window's median follows its own samples.
    pub(crate) fn observe(&mut self, now: Instant, sample: Option<f64>) {
        let longest = LONG_WINDOWS[1];
        while self.samples.front().is_some_and(|&(at, _)| now.saturating_duration_since(at) > longest) {
            self.samples.pop_front();
        }
        let due = self.samples.back().is_none_or(|&(last, _)| now.saturating_duration_since(last) >= LONG_SAMPLE_INTERVAL);
        let entered = sample.filter(|_| due);
        self.entered = entered;
        if let Some(ratio) = entered {
            self.samples.push_back((now, ratio));
        }
        self.recompute(now, entered.is_some());
    }

    /// Each window's median, recomputed when a sample entered or its count changed (one aged out of it).
    fn recompute(&mut self, now: Instant, entered: bool) {
        for (window, median) in LONG_WINDOWS.iter().zip(&mut self.medians) {
            let start = self.samples.partition_point(|&(at, _)| now.saturating_duration_since(at) > *window);
            let count = self.samples.len() - start;
            if entered || count != median.1 {
                *median = (upper_median(&mut self.scratch, self.samples.range(start..).map(|&(_, ratio)| ratio)), count);
            }
        }
    }

    /// Each longer window's basis by every basis' one rule (`published`).
    pub(crate) fn published(&self) -> LongerBases {
        let [(hour, hour_samples), (four_hours, four_hours_samples)] = self.medians;
        LongerBases {
            hour: published(hour, hour_samples, LONG_MIN_SAMPLES, LONG_WINDOWS[0]),
            four_hours: published(four_hours, four_hours_samples, LONG_MIN_SAMPLES, LONG_WINDOWS[1]),
        }
    }

    /// The sample the last tick took, once.
    pub(crate) const fn take_entered(&mut self) -> Option<f64> {
        self.entered.take()
    }

    /// Takes back an earlier run's samples, oldest first, into a window that has taken none yet, its medians as at
    /// `now`.
    pub(crate) fn restore(&mut self, samples: &[(Instant, f64)], now: Instant) {
        assert!(self.samples.is_empty(), "samples restored into a window that has sampled");
        self.samples.extend(samples.iter().copied());
        self.recompute(now, true);
    }
}

/// The upper median of an even count, as `weighted_median` at equal weights; `None` without a ratio.
fn upper_median(scratch: &mut Vec<f64>, ratios: impl Iterator<Item = f64>) -> Option<f64> {
    scratch.clear();
    scratch.extend(ratios);
    let middle = scratch.len() / 2;
    (!scratch.is_empty()).then(|| *scratch.select_nth_unstable_by(middle, f64::total_cmp).1)
}

/// Every basis' one rule: a window's `median` of its `samples` once it holds `min_samples` - never a zero for too few
/// (`externalBasisSamples`, judged first) - and finite within `MAX_BASIS_RATIO` either way, `MAX_BASIS_RATIO` itself
/// published; else withheld, `externalBasisOutOfRange`.
fn published(median: Option<f64>, samples: usize, min_samples: usize, window: Duration) -> Result<ExternalBasis, Withheld> {
    let Some(ratio) = median.filter(|_| samples >= min_samples) else {
        let detail = format!("{samples} of the {min_samples} samples needed over the last {} s", window.as_secs());
        return Err(Withheld::unavailable("externalBasisSamples", detail));
    };
    if !ratio.is_finite() || ratio.abs() > MAX_BASIS_RATIO {
        let detail = format!(
            "median {ratio} of {samples} samples is past the ±{}% a normal basis stays within: its quote is far off HL's",
            MAX_BASIS_RATIO * 100.0
        );
        return Err(Withheld::unavailable("externalBasisOutOfRange", detail));
    }
    Ok(ExternalBasis { ratio, samples })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `count` samples of `ratio`, a second apart from `start`; the instant after the last.
    fn fill(window: &mut ExternalBasisWindow, start: Instant, count: u32, ratio: f64) -> Instant {
        (0..count).fold(start, |at, _| {
            window.observe(at, Some(ratio));
            at + SAMPLE_INTERVAL
        })
    }

    fn ratio(window: &ExternalBasisWindow) -> f64 {
        window.published().unwrap().ratio
    }

    #[test]
    fn the_basis_publishes_from_the_minimum_samples_on() {
        let mut window = ExternalBasisWindow::default();
        let next = fill(&mut window, Instant::now(), 119, 0.001);
        let withheld = window.published().unwrap_err();
        assert_eq!((withheld.code, withheld.detail.as_str()), ("externalBasisSamples", "119 of the 120 samples needed over the last 600 s"));
        window.observe(next, Some(0.001));
        assert_eq!(window.published(), Ok(ExternalBasis { ratio: 0.001, samples: 120 }));
    }

    /// Review 000594 r1's example: 60 samples of 0 and 60 of +100 bp take the upper one.
    #[test]
    fn the_median_of_an_even_count_is_the_upper_one() {
        let mut window = ExternalBasisWindow::default();
        let next = fill(&mut window, Instant::now(), 60, 0.0);
        let next = fill(&mut window, next, 60, 0.01);
        assert_eq!(ratio(&window), 0.01);
        window.observe(next, Some(0.0));
        assert_eq!(ratio(&window), 0.0, "121: the middle one");
    }

    /// 300 s at the normal basis, then 100 s of HL 100 bp off: the median stays; the move stands for half the window
    /// and it is the median.
    #[test]
    fn a_move_shorter_than_half_the_window_leaves_the_median() {
        let mut window = ExternalBasisWindow::default();
        let next = fill(&mut window, Instant::now(), 300, -0.0025);
        let next = fill(&mut window, next, 100, 0.0075);
        assert_eq!(ratio(&window), -0.0025);
        fill(&mut window, next, 200, 0.0075);
        assert_eq!(ratio(&window), 0.0075);
    }

    /// The samples leave `WINDOW` after their own, whether or not one enters.
    #[test]
    fn the_samples_leave_the_window_after_it() {
        let mut window = ExternalBasisWindow::default();
        let start = Instant::now();
        let next = fill(&mut window, start, 150, 0.0);
        fill(&mut window, next, 150, 0.002);
        // the first 100 samples (0 .. 99 s) older than 600 s at 700 s
        window.observe(start + Duration::from_secs(700), None);
        assert_eq!(window.published(), Ok(ExternalBasis { ratio: 0.002, samples: 200 }));
        window.observe(start + Duration::from_secs(1_000), None);
        assert_eq!(window.published().unwrap_err().detail, "0 of the 120 samples needed over the last 600 s");
    }

    /// 50 ms ticks for 3 s, each with a sample: one a second enters.
    #[test]
    fn a_sample_enters_once_a_second() {
        let mut window = ExternalBasisWindow::default();
        let start = Instant::now();
        for tick in 0..60 {
            window.observe(start + Duration::from_millis(50 * tick), Some(0.0));
        }
        assert_eq!(window.samples.len(), 3);
    }

    /// User 2026-11-02, review 000683 r2: a median within ±10% is published, ±0.10 itself included; past it either way,
    /// at -1 (a quote 10^24 against HL's 100,002 rounds there in f64), infinite or NaN, withheld as out of range with
    /// its median; too few samples withheld as before, judged first.
    #[test]
    fn a_basis_publishes_only_a_finite_median_within_ten_percent() {
        let published = |count: u32, ratio: f64| {
            let mut window = ExternalBasisWindow::default();
            fill(&mut window, Instant::now(), count, ratio);
            window.published()
        };
        for ratio in [0.10, -0.10, 0.0, 0.0999] {
            assert_eq!(published(120, ratio), Ok(ExternalBasis { ratio, samples: 120 }), "{ratio}");
        }
        let book_mid = f64::midpoint(10_000_100_000_000.0, 10_000_300_000_000.0) / BOOK_PRICE_SCALE;
        assert_eq!(book_mid / 1e24 - 1.0, -1.0, "the review's OKX quote");
        for ratio in [0.100_000_1, -0.100_000_1, -1.0, f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            let withheld = published(120, ratio).unwrap_err();
            assert_eq!(withheld.code, "externalBasisOutOfRange", "{ratio}");
            let reason = format!("median {ratio} of 120 samples is past the ±10% a normal basis stays within: its quote is far off HL's");
            assert_eq!(withheld.detail, reason);
        }
        let few = published(119, -1.0).unwrap_err();
        assert_eq!((few.code, few.detail.as_str()), ("externalBasisSamples", "119 of the 120 samples needed over the last 600 s"), "too few judged first");
    }

    /// Review 000683 r3: from books through `sample`, the window and `published`: HL at 109.99 and 90.01 against an
    /// external 100 (±9.99%) publish; at 110.01 and 89.99 (±10.01%) withheld as out of range. Books exactly ±10% apart
    /// are left unasserted: f64's rounding decides their side (`MAX_BASIS_RATIO`).
    #[test]
    fn books_inside_ten_percent_publish_and_past_it_are_withheld() {
        let external = VenuePrice::parse("100").unwrap();
        let published = |hl_px: u64| {
            let ratio = sample(Px::new(hl_px), Px::new(hl_px), external, external);
            let mut window = ExternalBasisWindow::default();
            fill(&mut window, Instant::now(), 120, ratio);
            (ratio, window.published())
        };
        for (hl_px, expected) in [(10_999_000_000, 0.0999), (9_001_000_000, -0.0999)] {
            let (ratio, basis) = published(hl_px);
            assert!((ratio - expected).abs() < 1e-12, "{hl_px}: {ratio}");
            assert_eq!(basis, Ok(ExternalBasis { ratio, samples: 120 }), "{hl_px}");
        }
        for hl_px in [11_001_000_000, 8_999_000_000] {
            assert_eq!(published(hl_px).1.unwrap_err().code, "externalBasisOutOfRange", "{hl_px}");
        }
    }

    /// `count` longer-window samples of `ratio`, 10 s apart from `start`; the instant after the last.
    fn fill_long(window: &mut LongBasisWindow, start: Instant, count: u32, ratio: f64) -> Instant {
        (0..count).fold(start, |at, _| {
            window.observe(at, Some(ratio));
            at + LONG_SAMPLE_INTERVAL
        })
    }

    /// Design 000741 r2: a sample every 10 s, however many ticks; each longer window published from its 60th sample.
    #[test]
    fn the_longer_windows_take_a_sample_every_ten_seconds_and_publish_from_sixty() {
        let mut window = LongBasisWindow::default();
        let start = Instant::now();
        for tick in 0..200 {
            window.observe(start + Duration::from_millis(50 * tick), Some(0.001));
        }
        assert_eq!(window.samples.len(), 1, "one in 10 s of ticks");
        let next = fill_long(&mut window, start + LONG_SAMPLE_INTERVAL, 58, 0.001);
        let withheld = window.published().hour.unwrap_err();
        assert_eq!((withheld.code, withheld.detail.as_str()), ("externalBasisSamples", "59 of the 60 samples needed over the last 3600 s"));
        assert_eq!(window.published().four_hours.unwrap_err().detail, "59 of the 60 samples needed over the last 14400 s");
        window.observe(next, Some(0.001));
        let ready = ExternalBasis { ratio: 0.001, samples: 60 };
        assert_eq!(window.published(), LongerBases { hour: Ok(ready), four_hours: Ok(ready) });
    }

    /// Three hours at 0, then one at +20 bp: the hour's median is the last hour's (its 360 and the zero exactly 3,600 s
    /// old), the four hours' still 0 (1,080 of 1,440); with no sample after, the hour empties as its samples age out,
    /// the four hours lose the first hour's.
    #[test]
    fn each_longer_window_takes_its_own_span_of_the_samples() {
        let mut window = LongBasisWindow::default();
        let start = Instant::now();
        let next = fill_long(&mut window, start, 1_080, 0.0);
        let next = fill_long(&mut window, next, 360, 0.002);
        assert_eq!(window.published(), LongerBases {
            hour: Ok(ExternalBasis { ratio: 0.002, samples: 361 }),
            four_hours: Ok(ExternalBasis { ratio: 0.0, samples: 1_440 }),
        });
        window.observe(next + Duration::from_secs(3_600), None);
        let basis = window.published();
        assert_eq!(basis.hour.unwrap_err().detail, "0 of the 60 samples needed over the last 3600 s");
        assert_eq!(basis.four_hours.map(|basis| basis.samples), Ok(1_080), "the first hour's left the four hours");
    }

    /// The samples handed over (`take_entered`, once each) restore a fresh window to the same bases, either kind.
    #[test]
    fn a_window_restored_from_its_handed_samples_publishes_alike() {
        let start = Instant::now();
        let (mut near, mut long) = (ExternalBasisWindow::default(), LongBasisWindow::default());
        let (mut near_taken, mut long_taken) = (Vec::new(), Vec::new());
        for second in 0..900u32 {
            let at = start + Duration::from_secs(u64::from(second));
            let ratio = f64::from(second % 7) * 1e-4;
            near.observe(at, Some(ratio));
            long.observe(at, Some(ratio));
            near_taken.extend(near.take_entered().map(|ratio| (at, ratio)));
            long_taken.extend(long.take_entered().map(|ratio| (at, ratio)));
            assert_eq!((near.take_entered(), long.take_entered()), (None, None), "each once");
        }
        let now = start + Duration::from_secs(899);
        let (mut near_again, mut long_again) = (ExternalBasisWindow::default(), LongBasisWindow::default());
        near_again.restore(&near_taken[near_taken.len() - near.samples.len()..]);
        long_again.restore(&long_taken, now);
        assert_eq!((near_again.published(), long_again.published()), (near.published(), long.published()));
    }

    /// The book 100.0 / 100.2 against the external medians 99.9 / 100.1: 100.1 / 100 - 1.
    #[test]
    fn a_sample_is_the_book_midpoint_over_the_external_midpoint() {
        let price = |text| VenuePrice::parse(text).unwrap();
        let ratio = sample(Px::new(10_000_000_000), Px::new(10_020_000_000), price("99.9"), price("100.1"));
        assert!((ratio - 0.001).abs() < 1e-15, "{ratio}");
    }
}
