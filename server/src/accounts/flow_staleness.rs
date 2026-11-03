//! How far the forced flow frames' account data stands behind the wall clock, by spell (hyperliquid-trade 2026-11-03):
//! the trader refuses a frame whose `accountsTimeMs` is more than 4 s old by its clock; 11-03 03:01:07–10 it refused
//! four with the node at most 1.14 s behind, and nothing on record told where the rest was held. A spell opens at the
//! first frame published with its account data more than `STALE_AFTER_MS` old and ends at the first one back within
//! published `QUIET_MS` after its last stale one; it is logged once, ended, with the frame whose account data was the
//! oldest. Its ages are those of the inputs the frame was computed from, read when its computation began - the forced
//! flow's own computation and walks since included, not one owner's lag each (review 000693 r1 P3-1) -, set beside
//! the account copy's own at the frame's publication: a copy fresher than the frame's inputs puts the gap on the
//! forced flow's side.

use std::{fmt, sync::Arc};

use super::forced_flow_owner::{FlowFrame, FlowHeights};

/// Account data this much older than the wall clock opens a spell (bm 11-03 08:45Z, 64,240 frames over 120 s: p50
/// 395 ms, p99 780 ms).
const STALE_AFTER_MS: u64 = 1_000;
/// A spell ends only at a frame back within this long after its last stale one: frames computed from inputs of
/// different ages interleave within a step and from one step to the next (review 000693 r1: 200 frames of one instant
/// alternating ended 100 spells), and the log takes a line a spell, a second at most.
const QUIET_MS: u64 = 1_000;

/// The account copy's own blocks at a publication (`AccountsCopy::accounts_at` and `positions_at`), block times ms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CopyTimes {
    pub(crate) accounts_time_ms: u64,
    pub(crate) positions_time_ms: u64,
}

/// The frame whose account data was a spell's oldest, as published; `copy` is `None` while the copy did not serve.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OldestFrame {
    coin: String,
    at_ms: u64,
    heights: FlowHeights,
    copy: Option<CopyTimes>,
}

impl OldestFrame {
    const fn accounts_age_ms(&self) -> u64 {
        age_ms(self.at_ms, self.heights.accounts_time_ms)
    }
}

/// A spell: its frames past the line so far.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Spell {
    started_ms: u64,
    last_stale_ms: u64,
    frames: u64,
    oldest: OldestFrame,
    /// The book's oldest of the spell: the node's lag need not peak with the account data's.
    book_oldest_ms: u64,
}

impl Spell {
    fn new(frame: &FlowFrame, now_ms: u64, copy: Option<CopyTimes>) -> Self {
        let oldest = OldestFrame { coin: frame.coin.clone(), at_ms: now_ms, heights: frame.heights, copy };
        Self { started_ms: now_ms, last_stale_ms: now_ms, frames: 1, oldest, book_oldest_ms: age_ms(now_ms, frame.heights.listener_time) }
    }

    fn add(&mut self, frame: &FlowFrame, now_ms: u64, copy: Option<CopyTimes>) {
        self.frames += 1;
        self.last_stale_ms = now_ms;
        self.book_oldest_ms = self.book_oldest_ms.max(age_ms(now_ms, frame.heights.listener_time));
        if age_ms(now_ms, frame.heights.accounts_time_ms) > self.oldest.accounts_age_ms() {
            self.oldest = OldestFrame { coin: frame.coin.clone(), at_ms: now_ms, heights: frame.heights, copy };
        }
    }
}

/// A spell over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StaleSpell(Spell);

impl fmt::Display for StaleSpell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Spell { started_ms, last_stale_ms, frames, oldest, book_oldest_ms } = &self.0;
        let OldestFrame { coin, at_ms, heights, copy } = oldest;
        write!(
            f,
            "forced flow: the frames' account data over {STALE_AFTER_MS} ms old from {} to {} ({} ms, {frames} frames); the oldest at {}, {coin}, its inputs: the accounts {} ms (block {}), the positions {} ms (block {}), the book {} ms (block {}); ",
            utc_clock(*started_ms),
            utc_clock(*last_stale_ms),
            last_stale_ms.saturating_sub(*started_ms),
            utc_clock(*at_ms),
            oldest.accounts_age_ms(),
            heights.accounts_height,
            age_ms(*at_ms, heights.positions_time_ms),
            heights.positions_height,
            age_ms(*at_ms, heights.listener_time),
            heights.listener_height,
        )?;
        match copy {
            Some(copy) => write!(f, "the copy then: the accounts {} ms, the positions {} ms", age_ms(*at_ms, copy.accounts_time_ms), age_ms(*at_ms, copy.positions_time_ms))?,
            None => write!(f, "the copy then not serving")?,
        }
        write!(f, "; the book at most {book_oldest_ms} ms old in the spell")
    }
}

/// The spell under way, if any.
#[derive(Debug, Default)]
pub(crate) struct FlowStaleness {
    open: Option<Spell>,
}

impl FlowStaleness {
    /// One step's frames, published at `now_ms` by the wall clock with the copy at `copy`; the spells they ended. A
    /// frame without account data (its heights all 0: the copy not serving, no book yet) neither opens nor ends one.
    pub(crate) fn observe(&mut self, frames: &[Arc<FlowFrame>], now_ms: u64, copy: Option<CopyTimes>) -> Vec<StaleSpell> {
        frames.iter().filter(|frame| frame.heights.accounts_time_ms != 0).filter_map(|frame| self.take(frame, now_ms, copy)).collect()
    }

    fn take(&mut self, frame: &FlowFrame, now_ms: u64, copy: Option<CopyTimes>) -> Option<StaleSpell> {
        if age_ms(now_ms, frame.heights.accounts_time_ms) <= STALE_AFTER_MS {
            let quiet = self.open.as_ref().is_some_and(|spell| now_ms.saturating_sub(spell.last_stale_ms) >= QUIET_MS);
            return if quiet { self.open.take().map(StaleSpell) } else { None };
        }
        match &mut self.open {
            Some(spell) => spell.add(frame, now_ms, copy),
            None => self.open = Some(Spell::new(frame, now_ms, copy)),
        }
        None
    }
}

/// How old a block time is at `at_ms`; 0 for one ahead of the clock.
const fn age_ms(at_ms: u64, time_ms: u64) -> u64 {
    at_ms.saturating_sub(time_ms)
}

/// `HH:MM:SS.mmm` of a unix time in ms, UTC (the journal's line carries the date).
fn utc_clock(ms: u64) -> String {
    let seconds = ms / 1_000 % 86_400;
    format!("{:02}:{:02}:{:02}.{:03}", seconds / 3_600, seconds / 60 % 60, seconds % 60, ms % 1_000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::{forced_flow::Tier, forced_flow_owner::FlowContent};

    /// 2026-11-03 03:01:00.000 UTC.
    const T0: u64 = 1_793_674_860_000;

    /// A frame of `coin` whose accounts, positions and book are of those block times (each block numbered by its time).
    fn frame(coin: &str, accounts_time_ms: u64, positions_time_ms: u64, listener_time: u64) -> Arc<FlowFrame> {
        let heights = FlowHeights {
            accounts_height: accounts_time_ms / 100,
            accounts_time_ms,
            positions_height: positions_time_ms / 100,
            positions_time_ms,
            listener_height: listener_time / 100,
            listener_time,
            ..FlowHeights::default()
        };
        Arc::new(FlowFrame { coin: coin.to_string(), tier: Tier::Near, seq: 1, heights, content: FlowContent::Unavailable(String::new()) })
    }

    /// A frame `age_ms` old in all three, published at `at_ms`.
    fn aged(coin: &str, at_ms: u64, age_ms: u64) -> Arc<FlowFrame> {
        frame(coin, at_ms - age_ms, at_ms - age_ms, at_ms - age_ms)
    }

    fn unavailable(coin: &str) -> Arc<FlowFrame> {
        Arc::new(FlowFrame { coin: coin.to_string(), tier: Tier::Near, seq: 1, heights: FlowHeights::default(), content: FlowContent::Unavailable(String::new()) })
    }

    const fn copy_aged(at_ms: u64, accounts_age_ms: u64, positions_age_ms: u64) -> CopyTimes {
        CopyTimes { accounts_time_ms: at_ms - accounts_age_ms, positions_time_ms: at_ms - positions_age_ms }
    }

    #[test]
    fn a_spell_runs_from_the_first_stale_frame_to_one_back_within_a_second_after_the_last() {
        let mut staleness = FlowStaleness::default();
        // at the line exactly: within
        assert_eq!(staleness.observe(&[aged("BTC", T0, 1_000)], T0, None), Vec::new());
        assert!(staleness.open.is_none());
        let at = T0 + 6_000;
        assert_eq!(staleness.observe(&[frame("KAS", at - 1_200, at - 1_100, at - 500)], at, Some(copy_aged(at, 1_200, 1_100))), Vec::new());
        let at = T0 + 7_457;
        let kbonk = frame("kBONK", at - 4_063, at - 3_900, at - 1_137);
        assert_eq!(staleness.observe(&[Arc::clone(&kbonk)], at, Some(copy_aged(at, 3_000, 2_900))), Vec::new());
        // back within, but under a second after the last stale frame: the spell goes on
        assert_eq!(staleness.observe(&[aged("BTC", T0 + 7_600, 300)], T0 + 7_600, None), Vec::new());
        assert_eq!(staleness.observe(&[aged("KAS", T0 + 8_000, 2_500)], T0 + 8_000, None), Vec::new());
        assert_eq!(staleness.observe(&[aged("BTC", T0 + 8_999, 300)], T0 + 8_999, None), Vec::new());
        let ended = staleness.observe(&[aged("BTC", T0 + 9_000, 300)], T0 + 9_000, None);
        let oldest = OldestFrame { coin: "kBONK".to_string(), at_ms: at, heights: kbonk.heights, copy: Some(copy_aged(at, 3_000, 2_900)) };
        let spell = Spell { started_ms: T0 + 6_000, last_stale_ms: T0 + 8_000, frames: 3, oldest, book_oldest_ms: 2_500 };
        assert_eq!(ended, vec![StaleSpell(spell)]);
        assert!(staleness.open.is_none());
        assert_eq!(
            ended[0].to_string(),
            "forced flow: the frames' account data over 1000 ms old from 03:01:06.000 to 03:01:08.000 (2000 ms, 3 frames); the oldest at 03:01:07.457, kBONK, its inputs: the accounts 4063 ms (block 17936748633), the positions 3900 ms (block 17936748635), the book 1137 ms (block 17936748663); the copy then: the accounts 3000 ms, the positions 2900 ms; the book at most 2500 ms old in the spell"
        );
    }

    #[test]
    fn frames_of_inputs_old_and_new_interleaved_keep_one_spell() {
        let mut staleness = FlowStaleness::default();
        // review 000693 r1: 200 frames of one instant, stale and fresh by turns
        let frames: Vec<Arc<FlowFrame>> = (0..200).map(|index| aged("KAS", T0, if index % 2 == 0 { 3_000 } else { 400 })).collect();
        assert_eq!(staleness.observe(&frames, T0, None), Vec::new());
        assert_eq!(staleness.open.as_ref().map(|spell| spell.frames), Some(100));
        assert_eq!(staleness.observe(&[aged("BTC", T0 + 999, 400)], T0 + 999, None), Vec::new());
        let ended = staleness.observe(&[aged("BTC", T0 + 1_000, 400)], T0 + 1_000, None);
        assert_eq!(ended.iter().map(|spell| spell.0.frames).collect::<Vec<_>>(), vec![100]);
    }

    #[test]
    fn a_frame_without_account_data_neither_opens_nor_ends_a_spell() {
        let mut staleness = FlowStaleness::default();
        assert_eq!(staleness.observe(&[unavailable("KAS")], T0, None), Vec::new());
        assert!(staleness.open.is_none());
        assert_eq!(staleness.observe(&[aged("KAS", T0, 2_000)], T0, None), Vec::new());
        assert_eq!(staleness.observe(&[unavailable("KAS")], T0 + 1_500, None), Vec::new());
        let open = staleness.open.as_ref().expect("the spell stays open");
        assert_eq!((open.started_ms, open.frames), (T0, 1));
        let ended = staleness.observe(&[aged("KAS", T0 + 2_000, 500)], T0 + 2_000, None);
        assert_eq!(ended.iter().map(|spell| (spell.0.frames, spell.0.last_stale_ms)).collect::<Vec<_>>(), vec![(1, T0)]);
        assert!(ended[0].to_string().contains("the copy then not serving; the book at most 2000 ms old in the spell"));
    }

    #[test]
    fn the_clock_reads_utc_to_the_ms() {
        assert_eq!(utc_clock(T0 + 7_457), "03:01:07.457");
        assert_eq!(utc_clock(T0 - 3 * 3_600_000 - 60_000 + 5), "00:00:00.005");
    }
}
