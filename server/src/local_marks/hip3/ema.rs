//! The relayer's component (2) gap (design 000582): a 150 s continuous-time EMA of x = HL mid - stored oracle (price
//! units), anchored at each relayer write to the (2) - (1) its setOracle submitted, rounded as published, and carried
//! forward from there by our own ticks: each tick's x held until the next and folded in with weight 1 - exp(-dt / 150 s).
//! bm 10-27 17:47–18:38 (`ema_check.py`): our own EMA against the published (2) - (1), median 0.05–0.24 bp and P90
//! 0.11–0.41 bp over five stocks, about the cent's rounding. xyz writes about every 3 s (bm 10-30: 73 writes in 216 s).
//!
//! An anchor is taken from its write's block time to the tick with the tick's x: the relayer read the book before the
//! block, so the value at a tick is an estimate (1.5 s weigh 1.0%, 3 s 2.0%, of x - gap). Only a tick with a trusted HL
//! book and node state anchors or samples. A sampling gap over `MAX_GAP` (the node state's freshness limit) drops the
//! value until a write newer than the one taken: once the node catches up, its blocks carry the relayer's latest. The
//! SIP takes no part: its outage only withholds the prediction.

use std::time::Duration;

use tokio::time::Instant;

use crate::accounts::marks::seed::RelayerGap;

const TAU_S: f64 = 150.0;
const MAX_GAP: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq)]
struct State {
    value: f64,
    /// The last sample, held until the next one.
    held: f64,
    last: Instant,
}

/// One coin's EMA, owned by the local marks' owner across ticks.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct Hip3Ema {
    state: Option<State>,
    /// The block time (unix ns) of the last write anchored at: kept when the value drops, so only a newer write
    /// anchors again.
    anchored_ns: Option<u64>,
}

impl Hip3Ema {
    /// A trusted tick's x with the coin's last relayer gap: a write newer than the one taken anchors, otherwise x folds in.
    pub(crate) fn observe(&mut self, now: Instant, now_ms: u64, x: f64, relayer: RelayerGap) {
        if self.anchored_ns.is_none_or(|anchored| relayer.time_ns > anchored) {
            let since_write_s = now_ms.saturating_sub(relayer.time_ns / 1_000_000) as f64 / 1_000.0;
            #[expect(clippy::cast_precision_loss, reason = "a gap in price units is far below 2^53")]
            let gap = relayer.gap_raw as f64;
            self.state = Some(State { value: weight(since_write_s).mul_add(x - gap, gap), held: x, last: now });
            self.anchored_ns = Some(relayer.time_ns);
            return;
        }
        let Some(state) = &mut self.state else { return };
        let dt = now.saturating_duration_since(state.last);
        if dt > MAX_GAP {
            self.state = None;
            return;
        }
        state.value = weight(dt.as_secs_f64()).mul_add(state.held - state.value, state.value);
        state.held = x;
        state.last = now;
    }

    /// The node state lists no usable relayer gap for the coin: the value goes, the write taken stays.
    pub(crate) const fn withdraw(&mut self) {
        self.state = None;
    }

    /// An EMA anchored at `gap` by the write of block time `anchored_ns`, last sampled at `at` (tests).
    #[cfg(test)]
    pub(crate) const fn anchored_for_test(gap: f64, anchored_ns: u64, at: Instant) -> Self {
        Self { state: Some(State { value: gap, held: gap, last: at }), anchored_ns: Some(anchored_ns) }
    }

    /// The gap in price units and the block time of the write it is anchored at; `None` without one since the start
    /// or since the value dropped.
    pub(crate) fn gap(&self) -> Option<(f64, u64)> {
        Some((self.state?.value, self.anchored_ns?))
    }
}

fn weight(dt_s: f64) -> f64 {
    1.0 - (-dt_s / TAU_S).exp()
}

#[cfg(test)]
mod tests {
    use super::*;

    const WRITE_NS: u64 = 1_793_123_461_500_000_000;
    const WRITE_MS: u64 = WRITE_NS / 1_000_000;

    fn write(gap_raw: i64, time_ns: u64) -> RelayerGap {
        RelayerGap { gap_raw, time_ns }
    }

    /// A first write anchors at once (no warmup), carried from its block to the tick with the tick's x.
    #[test]
    fn a_write_anchors_at_once_from_its_block() {
        let start = Instant::now();
        let mut ema = Hip3Ema::default();
        assert_eq!(ema.gap(), None);
        ema.observe(start, WRITE_MS + 1_500, -10.0, write(-20, WRITE_NS));
        let (gap, anchored) = ema.gap().unwrap();
        assert!((gap - weight(1.5).mul_add(10.0, -20.0)).abs() < 1e-12, "{gap}");
        assert_eq!(anchored, WRITE_NS);
    }

    #[test]
    fn the_ema_folds_held_samples_in_continuous_time() {
        let start = Instant::now();
        let mut ema = Hip3Ema::default();
        ema.observe(start, WRITE_MS, 0.0, write(0, WRITE_NS));
        // 1.0 held for one tau, ticks of 50 ms, the write unchanged: 1 - 1/e of the way
        let mut now = start;
        ema.observe(now, WRITE_MS, 1.0, write(0, WRITE_NS));
        for _ in 0..3_000 {
            now += Duration::from_millis(50);
            ema.observe(now, WRITE_MS, 1.0, write(0, WRITE_NS));
        }
        let (gap, _) = ema.gap().unwrap();
        assert!((gap - (1.0 - (-1.0_f64).exp())).abs() < 1e-9, "{gap}");
    }

    /// A newer write replaces what our ticks carried; the same write again does not.
    #[test]
    fn a_newer_write_anchors_again() {
        let start = Instant::now();
        let mut ema = Hip3Ema::anchored_for_test(5.0, WRITE_NS, start);
        ema.observe(start + Duration::from_secs(1), WRITE_MS + 1_000, 5.0, write(-30, WRITE_NS));
        assert_eq!(ema.gap(), Some((5.0, WRITE_NS)), "the write taken: x folds in");
        let later = WRITE_NS + 3_000_000_000;
        ema.observe(start + Duration::from_secs(3), WRITE_MS + 3_000, -30.0, write(-30, later));
        assert_eq!(ema.gap(), Some((-30.0, later)));
    }

    /// Over the node's freshness limit unsampled, the value drops until a newer write; the one taken does not return.
    #[test]
    fn a_gap_over_the_limit_waits_for_a_newer_write() {
        let start = Instant::now();
        let mut ema = Hip3Ema::anchored_for_test(5.0, WRITE_NS, start);
        ema.observe(start + MAX_GAP, WRITE_MS + 5_000, 6.0, write(5, WRITE_NS));
        assert!(ema.gap().is_some(), "a gap of the limit itself is sampling");
        let stalled = start + MAX_GAP + MAX_GAP + Duration::from_millis(1);
        ema.observe(stalled, WRITE_MS + 10_001, 7.0, write(5, WRITE_NS));
        assert_eq!(ema.gap(), None);
        ema.observe(stalled + Duration::from_millis(50), WRITE_MS + 10_051, 7.0, write(5, WRITE_NS));
        assert_eq!(ema.gap(), None, "the write taken before the gap");
        ema.observe(stalled + Duration::from_millis(100), WRITE_MS + 10_101, 7.0, write(6, WRITE_NS + 9_000_000_000));
        assert_eq!(ema.gap().map(|(_, anchored)| anchored), Some(WRITE_NS + 9_000_000_000), "caught up: anchored at once");
    }

    #[test]
    fn a_withdrawal_keeps_the_write_taken() {
        let start = Instant::now();
        let mut ema = Hip3Ema::anchored_for_test(5.0, WRITE_NS, start);
        ema.withdraw();
        assert_eq!(ema.gap(), None);
        ema.observe(start + Duration::from_millis(50), WRITE_MS + 50, 5.0, write(5, WRITE_NS));
        assert_eq!(ema.gap(), None, "the same write does not come back");
    }
}
