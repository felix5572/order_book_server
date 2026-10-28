//! The oracle term's offset carry (design 000537 r1–r2). The relayer's aggregate stands off the SIP midpoint, and the
//! offset persists from one write to the next by a share of the coin's own (bm 10-27: 0.52–0.77 pre-market, about 0 in
//! the regular session, up to 0.99 post-market in thin names). (1) is predicted as the midpoint now plus rho times the
//! last write's offset, rho the through-zero least-squares slope of each offset on the one before over the session's
//! consecutive writes, each pair weighted by its age in block time: the coefficient of least squared error.
//!
//! - A write's offset is its oracle minus the midpoint of its session's quote feed (the SIP's, BOATS's overnight)
//!   `RELAYER_READ_LEAD_MS` before its block, the relayer's read (at the open, against the midpoint when the write
//!   reached us the carry did worse: P90 4.44 vs 3.74 bp, 20 stocks; BOATS's lead is the SIP's, bm 10-28).
//! - A pair is two writes at most `MAX_UPDATE_GAP_NS` apart: the relayer writes every ~3 s and the share is per write.
//! - Each session of each New York day starts over (pre-market's estimate carried into the open: P90 4.14 vs 3.74 bp),
//!   overnight one too (design 000543), and a reading applies the carry only within the session its pairs belong to.
//! - With weights below `MIN_PAIRS`, or every previous offset zero, there is no carry and (1) is v1's midpoint.

use std::collections::VecDeque;

use super::{
    protocol::EquityFeed,
    session::{EquitySessionId, equity_session},
};
use crate::local_marks::{price::VenuePrice, snapshot::MAX_PROGRESS_LAG_MS};

/// The relayer reads the NBBO about this long before the block (bm 10-27: its external price is closest to the SIP
/// midpoint 0.5–1.5 s before).
pub(crate) const RELAYER_READ_LEAD_MS: u64 = 1_000;
/// Two writes further apart are not consecutive (the relayer writes about every 3 s).
const MAX_UPDATE_GAP_NS: u64 = 4_000_000_000;
/// The pairs' age weight exp(-age / tau): about 200 pairs, a slope's standard error ~0.05; liquidity within a session
/// moves over tens of minutes.
const TAU_NS: f64 = 600e9;
/// Weights to estimate from: ~33 writes, ~99 s; a standard error ~0.13 (a 15 bp standing offset left ~2 bp off).
const MIN_PAIRS: f64 = 30.0;
/// The midpoints kept: a write the node state is trusted with is at most `MAX_PROGRESS_LAG_MS` old, read
/// `RELAYER_READ_LEAD_MS` before its block.
const HISTORY_SPAN_MS: u64 = RELAYER_READ_LEAD_MS + MAX_PROGRESS_LAG_MS;

/// A valid quote's midpoint at its feed's time, on its feed's connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QuoteMid {
    pub(crate) feed: EquityFeed,
    pub(crate) epoch: u64,
    pub(crate) quote_ms: u64,
    pub(crate) mid: VenuePrice,
}

/// A coin's recent midpoints of the session's feed by the feed's time, one per tick it changed (the ticks' 50 ms
/// sampling, not every quote).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct QuoteMidHistory {
    /// The feed and connection of the midpoints.
    source: Option<(EquityFeed, u64)>,
    mids: VecDeque<(u64, VenuePrice)>,
}

impl QuoteMidHistory {
    /// This tick's quote; `None` (no valid quote), another feed's or another connection's starts over: what came
    /// before cannot stand for a gap. Keeps `HISTORY_SPAN_MS` before `now_ms` and the one standing at its start.
    pub(crate) fn observe(&mut self, quote: Option<QuoteMid>, now_ms: u64) {
        let Some(QuoteMid { feed, epoch, quote_ms, mid }) = quote else {
            *self = Self::default();
            return;
        };
        if self.source != Some((feed, epoch)) {
            *self = Self { source: Some((feed, epoch)), mids: VecDeque::new() };
        }
        if self.mids.back().is_none_or(|&(last_ms, _)| quote_ms > last_ms) {
            self.mids.push_back((quote_ms, mid));
        }
        let start_ms = now_ms.saturating_sub(HISTORY_SPAN_MS);
        while self.mids.get(1).is_some_and(|&(second_ms, _)| second_ms <= start_ms) {
            self.mids.pop_front();
        }
    }

    /// `feed`'s midpoint standing at its time `quote_ms`; `None` before the history or for another feed's (a write
    /// of the session before, reaching us after the switch, review 000543 r1).
    pub(crate) fn at(&self, feed: EquityFeed, quote_ms: u64) -> Option<VenuePrice> {
        if self.source.is_none_or(|(source, _)| source != feed) {
            return None;
        }
        let after = self.mids.partition_point(|&(time_ms, _)| time_ms <= quote_ms);
        after.checked_sub(1).map(|index| self.mids[index].1)
    }
}

/// A write of the coin's stored oracle, as the node state shows it at a tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OracleWrite {
    /// Block time, unix ns.
    pub(crate) time_ns: u64,
    /// Price units.
    pub(crate) oracle: u64,
    pub(crate) size_decimals: u32,
    /// The relayer on its external price (external == oracle); otherwise it priced internally and the offset says
    /// nothing of the aggregate.
    pub(crate) on_external: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct Pairs {
    /// Sums of w * previous * offset, w * previous^2, w; price units.
    cross: f64,
    square: f64,
    weight: f64,
    /// The block time they are weighted as of.
    at_ns: u64,
}

impl Pairs {
    fn folded(self, at_ns: u64, previous: f64, offset: f64) -> Self {
        #[expect(clippy::cast_precision_loss, reason = "ns spans well below 2^53")]
        let decay = (-(at_ns.saturating_sub(self.at_ns) as f64) / TAU_NS).exp();
        Self {
            cross: previous.mul_add(offset, self.cross * decay),
            square: previous.mul_add(previous, self.square * decay),
            weight: self.weight.mul_add(decay, 1.0),
            at_ns,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct LastWrite {
    time_ns: u64,
    offset: Option<f64>,
}

/// One coin's carry state, owned by the local marks' owner across ticks.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct Hip3Carry {
    /// The last write taken in, by its time: a tick showing it again is not a write.
    seen_ns: Option<u64>,
    /// The session the pairs and `last` belong to.
    session: Option<EquitySessionId>,
    pairs: Option<Pairs>,
    last: Option<LastWrite>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CarryStatus {
    Ready,
    /// This session's pairs cannot estimate the slope yet: too few, or every previous offset zero.
    Warmup,
    /// Estimated, but the last write's offset is unknown (no SIP midpoint at its read, or the relayer priced
    /// internally).
    OffsetUnknown,
}

/// The carry as of a tick, price units; `rho` and `carry` only when estimated.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Hip3CarryReading {
    pub(crate) status: CarryStatus,
    pub(crate) rho: Option<f64>,
    pub(crate) pairs: f64,
    pub(crate) offset: Option<f64>,
    /// rho * offset, when `Ready`.
    pub(crate) carry: Option<f64>,
}

impl Hip3Carry {
    /// Takes in the node state's write, if it is a new one: its offset against `mids`, and the pair with the write
    /// before. The caller passes only writes of a trusted node state.
    pub(crate) fn write(&mut self, write: OracleWrite, mids: &QuoteMidHistory) {
        if self.seen_ns == Some(write.time_ns) {
            return;
        }
        self.seen_ns = Some(write.time_ns);
        let block_ms = write.time_ns / 1_000_000;
        let Some(session) = equity_session(block_ms) else {
            self.last = Some(LastWrite { time_ns: write.time_ns, offset: None });
            return;
        };
        if self.session != Some(session) {
            *self = Self { seen_ns: self.seen_ns, session: Some(session), pairs: None, last: None };
        }
        let feed = EquityFeed::of(session.session);
        let offset = write.on_external.then(|| mids.at(feed, block_ms.saturating_sub(RELAYER_READ_LEAD_MS))).flatten().map(|mid| {
            #[expect(clippy::cast_precision_loss, reason = "prices are below 2^53")]
            let oracle = write.oracle as f64;
            oracle - mid.units_f64(write.size_decimals)
        });
        let previous = self.last.filter(|last| write.time_ns.saturating_sub(last.time_ns) <= MAX_UPDATE_GAP_NS).and_then(|last| last.offset);
        if let (Some(previous), Some(offset)) = (previous, offset) {
            let start = Pairs { cross: 0.0, square: 0.0, weight: 0.0, at_ns: write.time_ns };
            self.pairs = Some(self.pairs.unwrap_or(start).folded(write.time_ns, previous, offset));
        }
        self.last = Some(LastWrite { time_ns: write.time_ns, offset });
    }

    /// The carry at a tick of unix ms `now_ms`: none outside the session the pairs belong to.
    pub(crate) fn reading(&self, now_ms: u64) -> Hip3CarryReading {
        let warmup = Hip3CarryReading { status: CarryStatus::Warmup, rho: None, pairs: 0.0, offset: None, carry: None };
        if self.session.is_none() || self.session != equity_session(now_ms) {
            return warmup;
        }
        let offset = self.last.and_then(|last| last.offset);
        let Some(pairs) = self.pairs.filter(|pairs| pairs.weight >= MIN_PAIRS && pairs.square > 0.0) else {
            return Hip3CarryReading { pairs: self.pairs.map_or(0.0, |pairs| pairs.weight), offset, ..warmup };
        };
        let rho = (pairs.cross / pairs.square).clamp(0.0, 1.0);
        let Some(last_offset) = offset else {
            return Hip3CarryReading { status: CarryStatus::OffsetUnknown, rho: Some(rho), pairs: pairs.weight, offset: None, carry: None };
        };
        Hip3CarryReading { status: CarryStatus::Ready, rho: Some(rho), pairs: pairs.weight, offset, carry: Some(rho * last_offset) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-10-27T17:00:00Z, a Tuesday 13:00 in New York (the regular session).
    const REGULAR_MS: u64 = 1_793_120_400_000;
    /// 2026-10-27T13:29:00Z, 09:29 in New York (pre-market).
    const PRE_MS: u64 = 1_793_107_740_000;

    fn price(text: &str) -> VenuePrice {
        VenuePrice::parse(text).unwrap()
    }

    fn mid(epoch: u64, quote_ms: u64, text: &str) -> QuoteMid {
        QuoteMid { feed: EquityFeed::Sip, epoch, quote_ms, mid: price(text) }
    }

    /// A coin of szDecimals 4 (units 0.01) whose SIP midpoint stands at 100.00 since `from_ms`.
    fn standing(from_ms: u64, now_ms: u64) -> QuoteMidHistory {
        let mut mids = QuoteMidHistory::default();
        mids.observe(Some(mid(1, from_ms, "100")), now_ms);
        mids
    }

    fn write(time_ms: u64, oracle: u64) -> OracleWrite {
        OracleWrite { time_ns: time_ms * 1_000_000, oracle, size_decimals: 4, on_external: true }
    }

    /// Writes every 3 s from `start_ms` with offsets `offsets` (units of 0.01) over a midpoint of 100.00.
    fn carry_of(start_ms: u64, offsets: &[i64]) -> Hip3Carry {
        let mut carry = Hip3Carry::default();
        for (index, offset) in offsets.iter().enumerate() {
            let time_ms = start_ms + 3_000 * index as u64;
            carry.write(write(time_ms, (10_000 + offset).unsigned_abs()), &standing(start_ms - 10_000, time_ms));
        }
        carry
    }

    #[test]
    fn the_history_answers_the_midpoint_standing_then() {
        let mut mids = QuoteMidHistory::default();
        mids.observe(Some(mid(1, 1_000, "100")), 1_050);
        mids.observe(Some(mid(1, 1_000, "100")), 1_100);
        mids.observe(Some(mid(1, 1_400, "100.5")), 1_450);
        assert_eq!(mids.mids.len(), 2, "the same SIP time once");
        assert_eq!((mids.at(EquityFeed::Sip, 999), mids.at(EquityFeed::Sip, 1_000), mids.at(EquityFeed::Sip, 1_399), mids.at(EquityFeed::Sip, 5_000)), (None, Some(price("100")), Some(price("100")), Some(price("100.5"))));
        // HISTORY_SPAN_MS later the first is dropped once the second stands at the span's start
        mids.observe(Some(mid(1, 1_400, "100.5")), 1_400 + HISTORY_SPAN_MS);
        assert_eq!((mids.at(EquityFeed::Sip, 1_000), mids.at(EquityFeed::Sip, 1_400)), (None, Some(price("100.5"))));
        // a quiet quote stays: the one standing at the span's start
        mids.observe(Some(mid(1, 1_400, "100.5")), 60_000);
        assert_eq!(mids.at(EquityFeed::Sip, 59_000), Some(price("100.5")));
    }

    /// Review 000537 r1 P3: another connection (even valid at both ticks) or no valid quote starts the history over.
    /// Review 000543 r1: another feed's midpoints start the history over, and a query for one feed never reads the
    /// other's (a session's write reaching us after the switch).
    #[test]
    fn the_history_belongs_to_one_feed() {
        let mut mids = QuoteMidHistory::default();
        mids.observe(Some(mid(1, 1_000, "100")), 1_000);
        assert_eq!(mids.at(EquityFeed::Boats, 1_000), None);
        mids.observe(Some(QuoteMid { feed: EquityFeed::Boats, epoch: 1, quote_ms: 2_000, mid: price("101") }), 2_000);
        assert_eq!((mids.at(EquityFeed::Sip, 1_500), mids.at(EquityFeed::Boats, 1_500), mids.at(EquityFeed::Boats, 2_000)), (None, None, Some(price("101"))));
    }

    #[test]
    fn the_history_starts_over_on_another_connection_or_no_quote() {
        let mut mids = QuoteMidHistory::default();
        mids.observe(Some(mid(1, 1_000, "100")), 1_000);
        mids.observe(Some(mid(2, 3_000, "101")), 3_000);
        assert_eq!((mids.at(EquityFeed::Sip, 2_000), mids.at(EquityFeed::Sip, 3_000)), (None, Some(price("101"))), "nothing known for the outage");
        mids.observe(None, 3_050);
        assert_eq!(mids.at(EquityFeed::Sip, 3_000), None);
    }

    /// Offsets alternating 8 and 4: pairs (8, 4) and (4, 8), slope (32 + 32) / (64 + 16) = 0.8 unweighted; with each
    /// pair's weight exp(-age / 600 s) in 3 s steps, the weighted sums.
    #[test]
    fn the_slope_of_each_offset_on_the_one_before() {
        let offsets: Vec<i64> = (0..40).map(|index| if index % 2 == 0 { 8 } else { 4 }).collect();
        let reading = carry_of(REGULAR_MS, &offsets).reading(REGULAR_MS + 120_000);
        let rho = reading.rho.unwrap();
        let weight = |pair: usize| (-3.0 * (38 - pair) as f64 / 600.0).exp();
        let (cross, square) = offsets.windows(2).enumerate().fold((0.0, 0.0), |(cross, square), (pair, window)| {
            let (previous, offset) = (window[0] as f64, window[1] as f64);
            (weight(pair).mul_add(previous * offset, cross), weight(pair).mul_add(previous * previous, square))
        });
        assert!((rho - cross / square).abs() < 1e-9 && (rho - 0.8).abs() < 0.02, "{rho} vs {}", cross / square);
        assert_eq!((reading.status, reading.offset), (CarryStatus::Ready, Some(4.0)));
        assert!(4.0_f64.mul_add(-rho, reading.carry.unwrap()).abs() < 1e-12);
        assert_eq!(carry_of(REGULAR_MS, &[0; 40]).reading(REGULAR_MS + 120_000).status, CarryStatus::Warmup, "review 000537 r1 P2-2: every offset zero");
        let mut steady = carry_of(REGULAR_MS, &[15; 40]);
        let reading = steady.reading(REGULAR_MS + 120_000);
        assert_eq!((reading.status, reading.rho, reading.offset, reading.carry), (CarryStatus::Ready, Some(1.0), Some(15.0), Some(15.0)));
        // a zero offset after standing ones: the slope moves, no NaN
        steady.write(write(REGULAR_MS + 120_000, 10_000), &standing(REGULAR_MS, REGULAR_MS + 120_000));
        let reading = steady.reading(REGULAR_MS + 120_000);
        assert_eq!((reading.status, reading.carry), (CarryStatus::Ready, Some(0.0)));
        assert!(reading.rho.unwrap() < 1.0);
        // a negative slope is none
        let flipping: Vec<i64> = (0..40).map(|index| if index % 2 == 0 { 6 } else { -6 }).collect();
        assert_eq!(carry_of(REGULAR_MS, &flipping).reading(REGULAR_MS + 120_000).rho, Some(0.0));
    }

    /// 33 pairs 3 s apart (34 writes, ~99 s) reach the weights; 32 are warming up with the weights so far.
    #[test]
    fn the_weights_decay_in_block_time() {
        let warming = carry_of(REGULAR_MS, &[10; 33]).reading(REGULAR_MS + 100_000);
        assert_eq!((warming.status, warming.rho, warming.carry), (CarryStatus::Warmup, None, None));
        assert!((29.0..30.0).contains(&warming.pairs), "{}", warming.pairs);
        assert_eq!(carry_of(REGULAR_MS, &[10; 34]).reading(REGULAR_MS + 102_000).status, CarryStatus::Ready);
        let pairs = Pairs { cross: 1.0, square: 1.0, weight: 1.0, at_ns: 0 }.folded(600_000_000_000, 0.0, 0.0);
        assert!((pairs.weight - (1.0 + (-1.0_f64).exp())).abs() < 1e-12, "one tau later: {pairs:?}");
    }

    #[test]
    fn writes_pair_only_when_consecutive_and_on_the_external_price() {
        let mids = standing(REGULAR_MS - 10_000, REGULAR_MS + 10_000);
        let mut carry = Hip3Carry::default();
        carry.write(write(REGULAR_MS, 10_010), &mids);
        carry.write(write(REGULAR_MS, 10_010), &mids);
        assert_eq!(carry.pairs, None, "the same write seen again");
        carry.write(write(REGULAR_MS + 4_001, 10_010), &mids);
        assert_eq!(carry.pairs, None, "4 s apart at most");
        carry.write(OracleWrite { on_external: false, ..write(REGULAR_MS + 7_000, 10_010) }, &mids);
        assert_eq!((carry.pairs, carry.last.unwrap().offset), (None, None), "priced internally: no offset");
        carry.write(write(REGULAR_MS + 10_000, 10_010), &mids);
        carry.write(write(REGULAR_MS + 13_000, 10_020), &mids);
        let pairs = carry.pairs.unwrap();
        assert_eq!((pairs.weight, pairs.cross, pairs.square), (1.0, 200.0, 100.0));
        // the midpoint read 1 s before the block: a write whose read precedes the history has no offset
        let mut late = Hip3Carry::default();
        late.write(write(REGULAR_MS - 9_500, 10_010), &mids);
        assert_eq!(late.last.unwrap().offset, None);
    }

    /// Review 000537 r1 P2-1: the reading belongs to its session; a late write of the old session does not bring it back,
    /// and the next day's same session starts over.
    #[test]
    fn a_session_change_withdraws_the_carry() {
        let carry = carry_of(PRE_MS - 120_000, &[15; 40]);
        assert_eq!(carry.reading(PRE_MS - 1_000).status, CarryStatus::Ready);
        let open_ms = PRE_MS + 60_000; // 09:30:00
        let reading = carry.reading(open_ms);
        assert_eq!((reading.status, reading.rho, reading.carry, reading.pairs), (CarryStatus::Warmup, None, None, 0.0), "the clock crossed 09:30, no write yet");
        let mut late = carry;
        late.write(write(open_ms - 500, 10_015), &standing(open_ms - 20_000, open_ms));
        assert_eq!(late.reading(open_ms + 100).status, CarryStatus::Warmup, "a late pre-market write");
        late.write(write(open_ms + 2_000, 10_015), &standing(open_ms - 20_000, open_ms + 2_000));
        let fresh = late.reading(open_ms + 2_100);
        assert_eq!((fresh.status, fresh.pairs, fresh.offset), (CarryStatus::Warmup, 0.0, Some(15.0)), "the open's first write starts over");
        let next_day = PRE_MS + 86_400_000 - 120_000;
        assert_eq!(carry.reading(next_day).status, CarryStatus::Warmup, "the next day's pre-market");
    }

    /// Review 000543 r1: at 04:00 the overnight carry stops at once, and an overnight write reaching us after the
    /// switch finds no BOATS midpoint in the SIP's history.
    #[test]
    fn the_overnight_carry_ends_at_the_pre_market() {
        const NIGHT_MS: u64 = 1_793_087_880_000; // 2026-10-27T07:58:00Z, 03:58 in New York: Monday's night
        let boats = |now_ms| {
            let mut mids = QuoteMidHistory::default();
            mids.observe(Some(QuoteMid { feed: EquityFeed::Boats, epoch: 1, quote_ms: NIGHT_MS - 200_000, mid: price("100") }), now_ms);
            mids
        };
        let mut carry = Hip3Carry::default();
        for index in 0..40 {
            let time_ms = NIGHT_MS - 120_000 + 3_000 * index;
            carry.write(write(time_ms, 10_015), &boats(time_ms));
        }
        let night = carry.reading(NIGHT_MS);
        assert_eq!((night.status, night.carry), (CarryStatus::Ready, Some(15.0)));
        let pre_ms = NIGHT_MS + 120_000; // 04:00:00
        assert_eq!(carry.reading(pre_ms).status, CarryStatus::Warmup, "no pre-market write yet");
        let mut late = carry;
        late.write(write(pre_ms - 500, 10_015), &standing(pre_ms - 10_000, pre_ms + 200));
        assert_eq!(late.last.unwrap().offset, None, "the night's write against the SIP's history");
        assert_eq!(late.reading(pre_ms + 200).status, CarryStatus::Warmup);
    }

    #[test]
    fn an_unknown_last_offset_is_not_carried() {
        let mut carry = carry_of(REGULAR_MS, &[15; 40]);
        carry.write(OracleWrite { on_external: false, ..write(REGULAR_MS + 120_000, 10_015) }, &standing(REGULAR_MS, REGULAR_MS + 120_000));
        let reading = carry.reading(REGULAR_MS + 120_100);
        assert_eq!((reading.status, reading.rho, reading.carry), (CarryStatus::OffsetUnknown, Some(1.0), None));
    }
}
