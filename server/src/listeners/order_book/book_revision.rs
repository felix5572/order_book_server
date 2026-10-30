//! The book revision (hyperliquid-trade design 000587 r2): raised once the book inputs applied since the last raise -
//! live diff and status batches, the trigger table's changes with them, an install with its replay - have gone quiet
//! or waited the maximum delay, the L2 publish's rule (a block arrives as hundreds of lines over 10-50 ms: bm
//! 2026-10-30, 13 ms median). The forced flow starts its near computations on it. Not tied to an L2 subscriber or to
//! the book's trust - a frame states its trust itself - and not proof that a block is complete.

use tokio::{sync::watch, time::Instant};

use super::l2_conflation::burst_deadline;

#[derive(Debug)]
pub(super) struct BookRevision {
    revision: watch::Sender<u64>,
    /// When the first input not yet announced was applied; `None`: nothing pending.
    pending_since: Option<Instant>,
    last_input_at: Option<Instant>,
}

impl BookRevision {
    pub(super) fn new() -> Self {
        Self { revision: watch::channel(0).0, pending_since: None, last_input_at: None }
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<u64> {
        self.revision.subscribe()
    }

    /// A book input was applied.
    pub(super) fn note_input(&mut self, now: Instant) {
        self.last_input_at = Some(now);
        self.pending_since.get_or_insert(now);
    }

    /// When the pending inputs are due; `None` when nothing is pending.
    pub(super) fn deadline(&self) -> Option<Instant> {
        Some(burst_deadline(self.pending_since?, self.last_input_at))
    }

    /// Raised when due: the inputs pending so far are announced, the ones after wait for the next raise.
    pub(super) fn raise_if_due(&mut self, now: Instant) {
        if self.deadline().is_none_or(|deadline| now < deadline) {
            return;
        }
        self.pending_since = None;
        self.revision.send_modify(|revision| *revision += 1);
    }

    /// Backdates the pending inputs so tests can make a raise due without sleeping.
    #[cfg(test)]
    pub(super) fn age_for_test(&mut self, by: std::time::Duration) {
        for at in [&mut self.pending_since, &mut self.last_input_at].into_iter().flatten() {
            *at -= by;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::listeners::order_book::l2_conflation::{MAX_DELAY, QUIET};

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    /// One block's lines in several batches: raised once, the quiet after its last batch, not at its first.
    #[test]
    fn a_burst_raises_once_when_it_goes_quiet() {
        let (mut revision, start) = (BookRevision::new(), Instant::now());
        let watched = revision.subscribe();
        for at in [0, 1, 2] {
            revision.note_input(start + ms(at));
            revision.raise_if_due(start + ms(at));
        }
        assert_eq!(*watched.borrow(), 0, "not within the burst");
        assert_eq!(revision.deadline(), Some(start + ms(2) + QUIET));
        revision.raise_if_due(start + ms(2) + QUIET);
        assert_eq!((*watched.borrow(), revision.deadline()), (1, None));
        revision.raise_if_due(start + ms(10));
        assert_eq!(*watched.borrow(), 1, "nothing pending, nothing raised");
    }

    /// Lines without a gap: raised at the maximum delay; the ones after wait for the next raise.
    #[test]
    fn a_burst_without_a_gap_raises_at_the_maximum_delay() {
        let (mut revision, start) = (BookRevision::new(), Instant::now());
        let watched = revision.subscribe();
        let mut at = Duration::ZERO;
        while at < MAX_DELAY {
            revision.note_input(start + at);
            revision.raise_if_due(start + at);
            at += ms(1);
        }
        revision.note_input(start + MAX_DELAY);
        revision.raise_if_due(start + MAX_DELAY);
        assert_eq!(*watched.borrow(), 1);
        revision.note_input(start + MAX_DELAY + ms(1));
        assert_eq!(revision.deadline(), Some(start + MAX_DELAY + ms(1) + QUIET), "pending again");
    }

    /// The same block's later lines after a raise (design 000587 r1 P2-1): raised again, not taken by the first.
    #[test]
    fn inputs_after_a_raise_raise_again() {
        let (mut revision, start) = (BookRevision::new(), Instant::now());
        let watched = revision.subscribe();
        revision.note_input(start);
        revision.raise_if_due(start + QUIET);
        revision.note_input(start + ms(5));
        revision.raise_if_due(start + ms(5) + QUIET);
        assert_eq!(*watched.borrow(), 2);
    }

    /// Raised with nobody watching, and a later subscriber sees the count.
    #[test]
    fn raised_without_a_subscriber() {
        let (mut revision, start) = (BookRevision::new(), Instant::now());
        revision.note_input(start);
        revision.raise_if_due(start + QUIET);
        assert_eq!(*revision.subscribe().borrow(), 1);
    }
}
