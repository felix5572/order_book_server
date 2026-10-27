//! The relayer's component (2) gap, computed here (design 000535 r2 revision 4): a 150 s continuous-time EMA of x = HL
//! mid - stored oracle (price units), each tick's x held until the next sample and folded in with weight
//! 1 - exp(-dt / 150 s). bm 10-27 17:47–18:38 (`ema_check.py`): against the published (2) - (1), median 0.05–0.24 bp
//! and P90 0.11–0.41 bp over five stocks, about the cent's rounding; a 150 s half-life fit worse.
//!
//! Only a tick with a trusted HL book and node state samples. Two samples further apart than `MAX_GAP` start it over:
//! an unobserved long gap is not sampling. It is ready after `WARMUP` of continuous sampling (5 tau). The SIP takes no
//! part: its outage only withholds the prediction.

use std::time::Duration;

use tokio::time::Instant;

use crate::local_marks::snapshot::MAX_PROGRESS_LAG_MS;

const TAU_S: f64 = 150.0;
pub(crate) const WARMUP: Duration = Duration::from_secs(750);
const MAX_GAP: Duration = Duration::from_millis(MAX_PROGRESS_LAG_MS);

#[derive(Debug, Clone, Copy, PartialEq)]
struct State {
    value: f64,
    /// The last sample, held until the next one.
    held: f64,
    last: Instant,
    sampled: Duration,
}

/// One coin's EMA, owned by the local marks' owner across ticks.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct Hip3Ema {
    state: Option<State>,
}

impl Hip3Ema {
    pub(crate) fn sample(&mut self, now: Instant, x: f64) {
        match &mut self.state {
            Some(state) if now.saturating_duration_since(state.last) <= MAX_GAP => {
                let dt = now.saturating_duration_since(state.last);
                let weight = 1.0 - (-dt.as_secs_f64() / TAU_S).exp();
                state.value = weight.mul_add(state.held - state.value, state.value);
                state.held = x;
                state.last = now;
                state.sampled += dt;
            }
            _ => self.state = Some(State { value: x, held: x, last: now, sampled: Duration::ZERO }),
        }
    }

    /// An EMA at `gap`, warm, last sampled at `at` (tests).
    #[cfg(test)]
    pub(crate) const fn warm_for_test(gap: f64, at: Instant) -> Self {
        Self { state: Some(State { value: gap, held: gap, last: at, sampled: WARMUP }) }
    }

    /// The gap in price units once warm, and how long it has sampled; `Err` with the sampled time before.
    pub(crate) fn gap(&self) -> Result<(f64, Duration), Duration> {
        match self.state {
            Some(state) if state.sampled >= WARMUP => Ok((state.value, state.sampled)),
            Some(state) => Err(state.sampled),
            None => Err(Duration::ZERO),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ema_folds_held_samples_in_continuous_time() {
        let start = Instant::now();
        let mut ema = Hip3Ema::default();
        ema.sample(start, 10.0);
        // 150 s of 10, then 20 held: after one tau the value has gone 1 - 1/e of the way
        let mut now = start;
        for _ in 0..3_000 {
            now += Duration::from_millis(50);
            ema.sample(now, 10.0);
        }
        ema.sample(now + Duration::from_millis(50), 20.0);
        now += Duration::from_millis(50);
        for _ in 0..3_000 {
            now += Duration::from_millis(50);
            ema.sample(now, 20.0);
        }
        assert_eq!(ema.gap(), Err(Duration::from_millis(300_050)), "not warm before 750 s");
        for _ in 0..9_000 {
            now += Duration::from_millis(50);
            ema.sample(now, 20.0);
        }
        let (gap, sampled) = ema.gap().unwrap();
        // 20 held for 12,000 ticks of 50 ms: 600 s, 4 tau, from 10
        assert!((gap - 10.0_f64.mul_add(-(-4.0_f64).exp(), 20.0)).abs() < 1e-9, "{gap}");
        assert_eq!(sampled, Duration::from_millis(750_050));
    }

    #[test]
    fn one_tau_moves_one_minus_one_over_e() {
        let start = Instant::now();
        let mut ema = Hip3Ema { state: Some(State { value: 0.0, held: 1.0, last: start, sampled: WARMUP }) };
        let mut now = start;
        for _ in 0..3_000 {
            now += Duration::from_millis(50);
            ema.sample(now, 1.0);
        }
        let (gap, _) = ema.gap().unwrap();
        assert!((gap - (1.0 - (-1.0_f64).exp())).abs() < 1e-9, "{gap}");
    }

    #[test]
    fn a_long_gap_starts_over() {
        let start = Instant::now();
        let mut ema = Hip3Ema { state: Some(State { value: 5.0, held: 5.0, last: start, sampled: WARMUP }) };
        ema.sample(start + MAX_GAP, 6.0);
        assert!(ema.gap().is_ok(), "a gap of the limit itself is sampling");
        ema.sample(start + MAX_GAP + MAX_GAP + Duration::from_millis(1), 7.0);
        assert_eq!(ema.gap(), Err(Duration::ZERO));
        assert_eq!(ema.state.map(|state| state.value), Some(7.0));
    }
}
