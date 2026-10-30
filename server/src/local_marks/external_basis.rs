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

use std::{collections::VecDeque, time::Duration};

use tokio::time::Instant;

use super::snapshot::{ExternalQuotes, Withheld};
use crate::order_book::Px;

pub(crate) const WINDOW: Duration = Duration::from_secs(600);
pub(crate) const MIN_SAMPLES: usize = 120;
pub(crate) const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
/// HL book prices are 1e-8.
const BOOK_PRICE_SCALE: f64 = 1e8;

/// The basis published: the samples' median and their count.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ExternalBasis {
    pub(crate) ratio: f64,
    pub(crate) samples: usize,
}

/// HL's book midpoint over the external bid and ask medians' midpoint - 1.
pub(crate) fn sample(bid: Px, ask: Px, external: &ExternalQuotes) -> f64 {
    #[expect(clippy::cast_precision_loss, reason = "book prices are below 2^53")]
    let book_mid = f64::midpoint(bid.value() as f64, ask.value() as f64) / BOOK_PRICE_SCALE;
    let external_mid = f64::midpoint(external.bid.to_f64(), external.ask.to_f64());
    book_mid / external_mid - 1.0
}

/// One coin's samples of the last `WINDOW` by the monotonic clock and their median: the local marks owner's across
/// ticks, emptied by a restart only.
#[derive(Debug, Clone, Default)]
pub(crate) struct ExternalBasisWindow {
    samples: VecDeque<(Instant, f64)>,
    /// The samples' median, recomputed as they change; `None` without a sample.
    median: Option<f64>,
    /// The median's working copy, reused.
    scratch: Vec<f64>,
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
        if let Some(ratio) = entered {
            self.samples.push_back((now, ratio));
        }
        if entered.is_some() || self.samples.len() != before {
            self.median = self.computed_median();
        }
    }

    /// The upper median of an even count, as `weighted_median` at equal weights.
    fn computed_median(&mut self) -> Option<f64> {
        self.scratch.clear();
        self.scratch.extend(self.samples.iter().map(|&(_, ratio)| ratio));
        let middle = self.scratch.len() / 2;
        (!self.scratch.is_empty()).then(|| *self.scratch.select_nth_unstable_by(middle, f64::total_cmp).1)
    }

    /// The median once the window holds `MIN_SAMPLES`; never a zero for too few.
    pub(crate) fn published(&self) -> Result<ExternalBasis, Withheld> {
        let samples = self.samples.len();
        let Some(ratio) = self.median.filter(|_| samples >= MIN_SAMPLES) else {
            let detail = format!("{samples} of the {MIN_SAMPLES} samples needed over the last {} s", WINDOW.as_secs());
            return Err(Withheld::unavailable("externalBasisSamples", detail));
        };
        Ok(ExternalBasis { ratio, samples })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_marks::price::VenuePrice;

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

    /// The book 100.0 / 100.2 against the external medians 99.9 / 100.1: 100.1 / 100 - 1.
    #[test]
    fn a_sample_is_the_book_midpoint_over_the_external_midpoint() {
        let price = |text| VenuePrice::parse(text).unwrap();
        let external = ExternalQuotes { bid: price("99.9"), ask: price("100.1"), venues: Vec::new() };
        let ratio = sample(Px::new(10_000_000_000), Px::new(10_020_000_000), &external);
        assert!((ratio - 0.001).abs() < 1e-15, "{ratio}");
    }
}
