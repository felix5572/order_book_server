//! When the L2 conflation buffer is published (design 000260).
//!
//! In `--stream-with-block-info` mode a block arrives as a burst of hundreds to
//! thousands of diff/status lines over 1-20ms, about 67ms apart. Publishing once
//! the book streams have gone quiet for [`QUIET`] sends each block's settled
//! state ~2ms after its last line. [`MAX_DELAY`] bounds the wait while lines
//! flow without a gap (a node catch-up). The fixed 50ms throttle this replaces
//! fired on a block's first lines and then held the settled state back for
//! 50-60ms (l2 p50 36ms behind the node's write, bbo ~0ms; bm 2026-10-10).

use crate::order_book::Coin;
use std::collections::HashSet;
use tokio::time::{Duration, Instant};

/// Both book streams quiet this long = the current burst has ended. A merge
/// window, not proof that the block is complete: in 2026-10-10 samples 90% of
/// blocks had no gap over 0.72ms inside, ~5% one over 2ms (the longest 16.6ms).
/// Such a block publishes its state so far, then the rest once it settles.
const QUIET: Duration = Duration::from_millis(2);
/// Longest a dirty coin waits for a quiet window, and the recheck spacing while
/// nothing can be published (user's choice 2026-10-10: 20ms, not 50ms).
pub(super) const MAX_DELAY: Duration = Duration::from_millis(20);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FlushTrigger {
    Quiet,
    MaxDelay,
    Recheck,
}

impl FlushTrigger {
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::Quiet => "quiet",
            Self::MaxDelay => "max_delay",
            Self::Recheck => "recheck",
        }
    }
}

/// Coins changed since the last L2 publish, and when to publish them. Owns the
/// invariant `dirty_since.is_some() == !coins.is_empty()`.
#[derive(Debug, Default)]
pub(super) struct L2Conflation {
    coins: HashSet<Coin>,
    // When the set went from empty to non-empty; later marks do not move it.
    dirty_since: Option<Instant>,
    last_book_event_at: Option<Instant>,
    // Set when a due publish had nobody to publish to. While set it alone is
    // the deadline, so an old quiet deadline cannot keep the loop due (000260 r1).
    recheck_at: Option<Instant>,
}

impl L2Conflation {
    /// A diff or status batch was applied (whether or not it changed a coin).
    pub(super) const fn note_book_event(&mut self, now: Instant) {
        self.last_book_event_at = Some(now);
    }

    pub(super) fn mark(&mut self, coins: impl IntoIterator<Item = Coin>, now: Instant) {
        self.coins.extend(coins);
        if self.dirty_since.is_none() && !self.coins.is_empty() {
            self.dirty_since = Some(now);
        }
    }

    /// When the pending coins are due; `None` when nothing is pending.
    pub(super) fn deadline(&self) -> Option<Instant> {
        let dirty_since = self.dirty_since?;
        if let Some(recheck_at) = self.recheck_at {
            return Some(recheck_at);
        }
        let max_delay = dirty_since + MAX_DELAY;
        Some(self.last_book_event_at.map_or(max_delay, |event_at| (event_at + QUIET).min(max_delay)))
    }

    pub(super) fn due(&self, now: Instant) -> Option<FlushTrigger> {
        if now < self.deadline()? {
            return None;
        }
        if self.recheck_at.is_some() {
            return Some(FlushTrigger::Recheck);
        }
        let quiet = self.last_book_event_at.is_some_and(|event_at| now >= event_at + QUIET);
        Some(if quiet { FlushTrigger::Quiet } else { FlushTrigger::MaxDelay })
    }

    /// Due, but nobody to publish to: keep the coins for a later subscriber and
    /// look again after [`MAX_DELAY`].
    pub(super) fn defer(&mut self, now: Instant) {
        self.recheck_at = Some(now + MAX_DELAY);
    }

    /// Hands the coins over for a publish.
    pub(super) fn take(&mut self) -> HashSet<Coin> {
        self.dirty_since = None;
        self.recheck_at = None;
        std::mem::take(&mut self.coins)
    }

    /// An install: the coins refer to the outgoing book.
    pub(super) fn clear(&mut self) {
        drop(self.take());
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.coins.is_empty()
    }

    #[cfg(test)]
    pub(super) fn contains(&self, coin: &str) -> bool {
        self.coins.contains(&Coin::new(coin))
    }

    /// Backdates every timestamp so tests can make a publish due without sleeping.
    #[cfg(test)]
    pub(super) fn age_for_test(&mut self, by: Duration) {
        for at in [&mut self.dirty_since, &mut self.last_book_event_at, &mut self.recheck_at].into_iter().flatten() {
            *at -= by;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn marked(at: Instant) -> L2Conflation {
        let mut conflation = L2Conflation::default();
        conflation.note_book_event(at);
        conflation.mark([Coin::new("BTC")], at);
        conflation
    }

    #[test]
    fn test_nothing_pending_has_no_deadline() {
        let mut conflation = L2Conflation::default();
        assert_eq!(conflation.deadline(), None);
        conflation.note_book_event(Instant::now());
        conflation.mark([], Instant::now());
        assert_eq!(conflation.deadline(), None, "events without a changed coin publish nothing");
    }

    #[test]
    fn test_due_once_the_streams_go_quiet() {
        let t0 = Instant::now();
        let conflation = marked(t0);
        assert_eq!(conflation.deadline(), Some(t0 + QUIET));
        assert_eq!(conflation.due(t0 + ms(1)), None);
        assert_eq!(conflation.due(t0 + QUIET), Some(FlushTrigger::Quiet));
    }

    #[test]
    fn test_continuous_events_are_bounded_by_max_delay_from_the_first_mark() {
        let t0 = Instant::now();
        let mut conflation = marked(t0);
        for step in 1..=25 {
            let now = t0 + ms(step);
            conflation.note_book_event(now);
            conflation.mark([Coin::new("ETH")], now);
            let expected = if now + QUIET < t0 + MAX_DELAY { now + QUIET } else { t0 + MAX_DELAY };
            assert_eq!(conflation.deadline(), Some(expected), "step {step}");
        }
        assert_eq!(conflation.due(t0 + MAX_DELAY), Some(FlushTrigger::MaxDelay));
    }

    /// Review 000260 r1 P2: with no subscriber the quiet deadline is long past;
    /// each recheck must land strictly later, and events must not pull it in.
    #[test]
    fn test_deferred_recheck_moves_forward_and_ignores_new_events() {
        let t0 = Instant::now();
        let mut conflation = marked(t0);
        let mut now = t0 + QUIET;
        let mut previous = now;
        for _ in 0..3 {
            assert!(conflation.due(now).is_some());
            conflation.defer(now);
            let deadline = conflation.deadline().expect("coins stay pending");
            assert_eq!(deadline, now + MAX_DELAY);
            assert!(deadline > previous);
            conflation.note_book_event(now + ms(1));
            conflation.mark([Coin::new("ETH")], now + ms(1));
            assert_eq!(conflation.deadline(), Some(deadline), "events do not bring the recheck forward");
            assert_eq!(conflation.due(now + ms(5)), None);
            previous = deadline;
            now = deadline;
        }
        assert_eq!(conflation.due(now), Some(FlushTrigger::Recheck));
        let coins = conflation.take();
        assert_eq!(coins.len(), 2);
        assert_eq!(conflation.deadline(), None, "a publish clears the schedule");
    }

    #[test]
    fn test_take_resets_and_a_new_mark_restarts_the_wait() {
        let t0 = Instant::now();
        let mut conflation = marked(t0);
        conflation.defer(t0 + QUIET);
        drop(conflation.take());
        assert!(conflation.is_empty());
        let t1 = t0 + ms(100);
        conflation.note_book_event(t1);
        conflation.mark([Coin::new("SOL")], t1);
        assert_eq!(conflation.deadline(), Some(t1 + QUIET), "no stale recheck or dirty_since survives");
    }
}
