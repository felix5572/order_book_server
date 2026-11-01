//! The running TWAPs and the suborders the next "first block of a second" runs (batch 2h, hyperliquid-trade design
//! 000624 r4).
//!
//! - **State**: seeded from the account view's anchor (`PendingAnchor::twaps`, the checkpoint's trackers), then
//!   followed by height: an activation (`node_twap_statuses_streaming`), a cancel (a successful `twapCancel` in the
//!   block log), an end (`finished` / `terminated`), a suborder's fills (`node_fills_streaming`, by `twapId`, a trade
//!   once by its `tid`). A newer anchor replaces the state, the events after it replayed from those kept.
//! - **Complete**: a height's events apply only once its block log line and the next one were read, the next at `r`,
//!   and both TWAP streams proved a read-through after `r` - the account view's rule (`accounts/blocks.rs`, design
//!   000433 r3); the two streams' lines of a block are read before the next block's `replica_cmds` line on bm, a run
//!   assumption. A line of a height already complete is a loss of it. Losses are bound as the account view's: a known
//!   block, or lines written by a wall time an anchor's block time must pass by `CLOCK_MARGIN_MS` (the start too).
//! - **Slots** (research `mempool_pending_book_20261031`, 5,774 of 5,774 suborders): slot k of a TWAP of `m` minutes
//!   is due at its grid's origin + 30 s x k, N = 2m + 1 slots - the origin the checkpoint's next due time on the grid
//!   (start + 1 s on bm's 401 of 422 TWAPs of 11-01 07:09, start + 0 s on 21), a TWAP activated since its activation
//!   plus 1 s, or 0 s after waiting for its trigger (research `twap_randomize_20261101`: 55 of 55 and 12 of 12) -; it
//!   sends a suborder when its executed size is under total x (k + c) / N, c the checkpoint's count `S` less the slot
//!   of its next due time (0 or 1; 1 for a TWAP activated since), run at the start of the first block of a second
//!   after it is due. A slot due before the latest complete first-of-second block is past - sent, skipped, refused or
//!   unfilled -: the cursor moves by block time alone.
//! - **Size**: a randomized TWAP's suborder adds (its block's unix ms % 40 - 20)% of a normal suborder to the target
//!   (research `twap_randomize_20261101`, 829 of 829): unknown before its block, so the deficit, the expectation. Else
//!   the deficit rounded down to the size grid, or the TWAP's estimated floor (a size seen above its deficit
//!   at two slots, never for a randomized TWAP - an estimate, not a protocol bound), capped at the rest and at 3 x the
//!   normal suborder (HL docs "TWAP details"): max(total / N, the floor) once a floor is estimated; with none, a deficit
//!   over 3 x total / N is not predicted, its cap unknown.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Instant,
};

use alloy::primitives::Address;
use rustc_hash::{FxHashMap, FxHashSet};

use super::twap_streams::{TwapActivation, TwapEvent, TwapFill};
use crate::{
    accounts::{
        book::{ActionCoins, CLOCK_MARGIN_MS, PendingAnchor},
        twap_tracker::CheckpointTwap,
    },
    metrics::PENDING_BOOK_TWAP_EVENTS_TOTAL,
    order_book::{Coin, Px, Sz},
};

/// A slot every 30 s.
pub(crate) const SLOT_MS: u64 = 30_000;
/// A TWAP activated at once: slot 0 is due 1 s after its start (one waiting for its trigger: at its activation).
const FIRST_SLOT_MS: u64 = 1_000;
/// Applied events kept for a newer anchor's replay: an anchor comes every ~12 minutes, parsed in a few.
const RETAIN_MS: u64 = 30 * 60 * 1_000;
/// Heights held above the complete height at most, each of block log reads and of staged events (~20 minutes of
/// blocks; the replica stalled, the streams going on: review 000624 r5 P2-2): past it what they hold is given up, a
/// loss of those heights, the TWAPs waiting for an anchor above them.
const MAX_AHEAD_HEIGHTS: usize = 20_000;
/// Sizes and prices in 1e-8 units.
const UNIT_DECIMALS: u32 = 8;

/// A suborder the next first-of-second block runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TwapSlice {
    pub(crate) twap_id: u64,
    pub(crate) user: Address,
    pub(crate) slot: u64,
    pub(crate) is_buy: bool,
    pub(crate) sz: Sz,
    /// The parent's max buy / min sell price.
    pub(crate) limit_px: Option<Px>,
    pub(crate) due_ms: u64,
}

/// The suborders the next first-of-second block runs, by coin, each coin's in the order taken: due time, then id - an
/// approximation (35% of bm's blocks of several TWAPs ran in it; no rule found).
pub(crate) type DueSlices = FxHashMap<Coin, Vec<TwapSlice>>;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Twap {
    user: Address,
    coin: Coin,
    is_buy: bool,
    /// Sizes in 1e-8 units.
    total: u64,
    executed: u64,
    /// One step of the coin's size grid, in 1e-8 units.
    lot: u64,
    slots: u64,
    /// Slot 0's due time, unix ms.
    origin_ms: u64,
    /// What its target's count adds to the slot: total x (slot + `count_offset`) / N.
    count_offset: u64,
    randomize: bool,
    limit_px: Option<Px>,
    floor: FloorEstimate,
}

/// A TWAP's suborder floor as estimated: a size seen above the deficit at two slots.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct FloorEstimate {
    /// A size seen above the deficit, and the slots it was seen at.
    candidate: Option<(u64, Vec<u64>)>,
    estimated: Option<u64>,
}

impl FloorEstimate {
    fn observe(&mut self, slot: u64, size: u64) {
        if self.estimated.is_some() {
            return;
        }
        match &mut self.candidate {
            Some((value, slots)) if *value == size => {
                if !slots.contains(&slot) {
                    slots.push(slot);
                }
                if slots.len() >= 2 {
                    self.estimated = Some(size);
                }
            }
            _ => self.candidate = Some((size, vec![slot])),
        }
    }
}

/// A height's events, as they arrive.
#[derive(Debug, Default, Clone)]
struct HeightEvents {
    waiting: Vec<u64>,
    activations: Vec<TwapActivation>,
    fills: Vec<TwapFill>,
    /// (twap, tid) of each fill taken.
    fill_ids: FxHashSet<(u64, u64)>,
    cancels: Vec<u64>,
    ends: Vec<u64>,
}

/// Which TWAP stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TwapStream {
    Statuses,
    Fills,
}

/// Why the TWAPs are not known at a height.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TwapsUnknown {
    /// No anchor covering the losses - the start, a stream's - installed.
    NoAnchor,
    /// The height's events are not proven complete.
    Incomplete,
}

#[derive(Debug)]
pub(crate) struct TwapBook {
    /// Each block log line read above the complete height: (read instant, block time ms).
    log_reads: BTreeMap<u64, (Instant, u64)>,
    statuses_through: Option<Instant>,
    fills_through: Option<Instant>,
    /// The highest height whose events applied, and its block time ms.
    complete: Option<(u64, u64)>,
    staged: BTreeMap<u64, HeightEvents>,
    /// Applied events, (height, block time ms, events), the last `RETAIN_MS`.
    retained: VecDeque<(u64, u64, HeightEvents)>,
    /// Lines lost up to a known block, and lines written by a wall time (unix ms; the start too).
    lost_block: Option<u64>,
    lost_by_ms: u64,
    anchor_waiting: Option<Arc<PendingAnchor>>,
    /// The installed anchor's (height, block time ms).
    anchored: Option<(u64, u64)>,
    /// The installed anchor's table by coin: (asset, `szDecimals`, max decimals).
    coins: FxHashMap<Coin, (u32, u32, u32)>,
    twaps: FxHashMap<u64, Twap>,
    /// The TWAPs waiting for their trigger: activated, their grid starts at the activation.
    waiting: FxHashSet<u64>,
    /// The limit seen for each running TWAP, as evidence apart from whether it is followed: its activation's (taken at
    /// its height's completion, an anchor installed or not), else the first checkpoint's listing it on a known market;
    /// its cancel or end takes it. A checkpoint giving another keeps the TWAP out (review 000624 r5 P2-1, r6 P2, r7 P2).
    limits_seen: FxHashMap<u64, Option<Px>>,
    /// The latest complete first-of-second block's time: slots due before it are past.
    first_of_second_ms: Option<u64>,
    /// Raised whenever what `due` reads changes: a height applied, an anchor installed.
    version: u64,
}

impl TwapBook {
    /// Nothing known: lines written before `started_ms` were not read.
    pub(crate) fn new(started_ms: u64) -> Self {
        Self {
            log_reads: BTreeMap::new(),
            statuses_through: None,
            fills_through: None,
            complete: None,
            staged: BTreeMap::new(),
            retained: VecDeque::new(),
            lost_block: None,
            lost_by_ms: started_ms,
            anchor_waiting: None,
            anchored: None,
            coins: FxHashMap::default(),
            twaps: FxHashMap::default(),
            waiting: FxHashSet::default(),
            limits_seen: FxHashMap::default(),
            first_of_second_ms: None,
            version: 0,
        }
    }

    /// Raised whenever what `due` reads changes.
    pub(crate) const fn version(&self) -> u64 {
        self.version
    }

    /// The latest complete first-of-second block's time: a slot due before it is past.
    pub(crate) const fn first_of_second_ms(&self) -> Option<u64> {
        self.first_of_second_ms
    }

    /// A block log line, read at `read_at`: its height and time, its successful `twapCancel`s. A hole before it loses
    /// the heights missing.
    pub(crate) fn on_block(&mut self, height: u64, read_at: Instant, time_ms: u64, cancels: &[u64]) {
        let last = self.log_reads.keys().next_back().copied().or_else(|| self.complete.map(|(complete, _)| complete));
        if last.is_some_and(|last| height <= last) {
            return;
        }
        if last.is_some_and(|last| height > last + 1) {
            PENDING_BOOK_TWAP_EVENTS_TOTAL.with_label_values(&["block_log_hole"]).inc();
            self.skip_to(height - 1);
        }
        self.log_reads.insert(height, (read_at, time_ms));
        if !cancels.is_empty() {
            self.staged.entry(height).or_default().cancels.extend_from_slice(cancels);
            self.bound_staged();
        }
        if self.log_reads.len() > MAX_AHEAD_HEIGHTS {
            PENDING_BOOK_TWAP_EVENTS_TOTAL.with_label_values(&["incomplete_too_long"]).inc();
            self.skip_to(height);
        }
    }

    /// A TWAP statuses line.
    pub(crate) fn on_statuses(&mut self, height: u64, events: Vec<TwapEvent>) {
        if self.late(height) {
            return;
        }
        let staged = self.staged.entry(height).or_default();
        for event in events {
            match event {
                TwapEvent::Activated(activation) => staged.activations.push(activation),
                TwapEvent::Ended { twap_id } => staged.ends.push(twap_id),
                TwapEvent::Refused { .. } => PENDING_BOOK_TWAP_EVENTS_TOTAL.with_label_values(&["suborder_refused"]).inc(),
                TwapEvent::Waiting { twap_id } => {
                    PENDING_BOOK_TWAP_EVENTS_TOTAL.with_label_values(&["waiting_for_trigger"]).inc();
                    staged.waiting.push(twap_id);
                }
            }
        }
        self.bound_staged();
    }

    /// A fills line's TWAP fills.
    pub(crate) fn on_fills(&mut self, height: u64, fills: Vec<TwapFill>) {
        if self.late(height) || fills.is_empty() {
            return;
        }
        let staged = self.staged.entry(height).or_default();
        for fill in fills {
            if staged.fill_ids.insert((fill.twap_id, fill.tid)) {
                staged.fills.push(fill);
            } else {
                PENDING_BOOK_TWAP_EVENTS_TOTAL.with_label_values(&["fill_repeated"]).inc();
            }
        }
        self.bound_staged();
    }

    pub(crate) fn on_read_through(&mut self, stream: TwapStream, at: Instant) {
        let through = match stream {
            TwapStream::Statuses => &mut self.statuses_through,
            TwapStream::Fills => &mut self.fills_through,
        };
        *through = Some(through.map_or(at, |before| before.max(at)));
    }

    /// Lines of unknown blocks lost - a TWAP stream's or the block log's -, detected at wall time `detected_ms`.
    pub(crate) fn on_loss(&mut self, detected_ms: u64) {
        PENDING_BOOK_TWAP_EVENTS_TOTAL.with_label_values(&["loss"]).inc();
        self.lost_by_ms = self.lost_by_ms.max(detected_ms);
    }

    /// A new anchor: installed once the complete height reaches it.
    pub(crate) fn on_anchor(&mut self, anchor: Arc<PendingAnchor>) {
        if self.anchored.is_some_and(|(height, _)| anchor.height <= height) {
            return;
        }
        self.anchor_waiting = Some(anchor);
    }

    /// The events of every height up to `height` proven complete, applied; a waiting anchor installed once reached.
    /// Whether the TWAPs are known at `height`.
    pub(crate) fn settle(&mut self, height: u64) -> Result<(), TwapsUnknown> {
        while let Some(next) = self.next_complete(height) {
            self.apply_height(next);
        }
        self.install_waiting_anchor();
        self.known_at(height)
    }

    /// The suborders the first first-of-second block after the confirmed block time `basis_ms` runs.
    pub(crate) fn due(&self, basis_ms: u64) -> DueSlices {
        let Some(first_of_second_ms) = self.first_of_second_ms else { return DueSlices::default() };
        let boundary_ms = (basis_ms / 1_000 + 1) * 1_000;
        let mut due = DueSlices::default();
        for (twap_id, twap) in &self.twaps {
            if let Some(slice) = twap.slice(*twap_id, first_of_second_ms, boundary_ms) {
                due.entry(twap.coin.clone()).or_default().push(slice);
            }
        }
        for slices in due.values_mut() {
            slices.sort_unstable_by_key(|slice| (slice.due_ms, slice.twap_id));
        }
        due
    }

    /// The next height to apply, when proven complete: its block log line and the next read, the next at `r`, both
    /// streams proven read through after `r`.
    fn next_complete(&self, height: u64) -> Option<u64> {
        let next = self.complete.map_or_else(|| self.log_reads.keys().next().copied(), |(complete, _)| Some(complete + 1))?;
        if next > height || !self.log_reads.contains_key(&next) {
            return None;
        }
        let (read_at, _) = self.log_reads.get(&(next + 1))?;
        let proven = |through: Option<Instant>| through.is_some_and(|at| at > *read_at);
        (proven(self.statuses_through) && proven(self.fills_through)).then_some(next)
    }

    fn apply_height(&mut self, height: u64) {
        #[expect(clippy::expect_used, reason = "next_complete returns a height with a block log read")]
        let (_, time_ms) = self.log_reads.remove(&height).expect("a complete height's block log read");
        let events = self.staged.remove(&height).unwrap_or_default();
        if self.complete.is_some_and(|(_, before_ms)| time_ms / 1_000 > before_ms / 1_000) {
            self.first_of_second_ms = Some(time_ms);
        }
        self.see_limits(&events);
        if self.anchored.is_some() {
            self.apply_events(time_ms, &events);
        }
        self.complete = Some((height, time_ms));
        self.version += 1;
        self.retained.push_back((height, time_ms, events));
        while self.retained.front().is_some_and(|(_, kept_ms, _)| kept_ms + RETAIN_MS < time_ms) {
            self.retained.pop_front();
        }
    }

    /// A height's events in a block's order: its suborders run at its start, then the actions and what they end.
    fn apply_events(&mut self, time_ms: u64, events: &HeightEvents) {
        let mut slices: FxHashMap<u64, u64> = FxHashMap::default();
        for fill in &events.fills {
            *slices.entry(fill.twap_id).or_default() += fill.sz.value();
        }
        for (twap_id, size) in slices {
            let Some(twap) = self.twaps.get_mut(&twap_id) else {
                PENDING_BOOK_TWAP_EVENTS_TOTAL.with_label_values(&["fill_unknown_twap"]).inc();
                continue;
            };
            twap.filled(time_ms, size);
        }
        self.waiting.extend(&events.waiting);
        for activation in &events.activations {
            let waited = self.waiting.remove(&activation.twap_id);
            match self.activated(activation, waited) {
                Ok(twap) => {
                    self.twaps.insert(activation.twap_id, twap);
                }
                Err(reason) => PENDING_BOOK_TWAP_EVENTS_TOTAL.with_label_values(&[reason]).inc(),
            }
        }
        for twap_id in events.cancels.iter().chain(&events.ends) {
            self.twaps.remove(twap_id);
            self.waiting.remove(twap_id);
        }
    }

    /// A height's evidence of limits, in its order: each activation's - one whose limit does not read gives none -, then
    /// each cancel and end taking its TWAP's.
    fn see_limits(&mut self, events: &HeightEvents) {
        for activation in &events.activations {
            if let Ok(limit) = activation_limit(activation) {
                self.limits_seen.insert(activation.twap_id, limit);
            }
        }
        for twap_id in events.cancels.iter().chain(&events.ends) {
            self.limits_seen.remove(twap_id);
        }
    }

    /// An activation as a TWAP, its coin named by the anchor's table; `waited` for its trigger before.
    fn activated(&self, activation: &TwapActivation, waited: bool) -> Result<Twap, &'static str> {
        let (_, size_decimals, _) = *self.coins.get(&activation.coin).ok_or("activation_unknown_coin")?;
        let lot = lot(size_decimals).ok_or("activation_unknown_coin")?;
        let limit_px = activation_limit(activation)?;
        Ok(Twap {
            user: activation.user,
            coin: activation.coin.clone(),
            is_buy: activation.is_buy,
            total: activation.total.value(),
            executed: 0,
            lot,
            slots: 2 * activation.minutes + 1,
            origin_ms: activation.start_ms + if waited { 0 } else { FIRST_SLOT_MS },
            count_offset: 1,
            randomize: activation.randomize,
            limit_px,
            floor: FloorEstimate::default(),
        })
    }

    /// The waiting anchor, once the complete height reaches it: its TWAPs replace the state - each the same parent keeps
    /// its floor estimate -, the events kept since replayed. One the losses or the kept heights do not let link waits
    /// for a newer anchor.
    fn install_waiting_anchor(&mut self) {
        let Some((complete, _)) = self.complete else { return };
        let Some(anchor) = self.anchor_waiting.take_if(|anchor| anchor.height <= complete) else { return };
        let anchor_time_ms = self.retained.iter().find(|(height, _, _)| *height == anchor.height).map(|(_, time_ms, _)| *time_ms);
        let Some(anchor_time_ms) = anchor_time_ms.filter(|time_ms| self.covers_losses(anchor.height, *time_ms)) else {
            PENDING_BOOK_TWAP_EVENTS_TOTAL.with_label_values(&["anchor_not_linked"]).inc();
            return;
        };
        self.coins = anchor_coins(&anchor.coins);
        // the evidence of limits as of the anchor: of the TWAPs it lists, followed or not; the replay brings it on
        let listed: FxHashSet<u64> = anchor.twaps.iter().map(|seed| seed.id).collect();
        self.limits_seen.retain(|twap_id, _| listed.contains(twap_id));
        let mut twaps = FxHashMap::default();
        for seed in &anchor.twaps {
            match self.reseeded(seed) {
                Ok(twap) => {
                    twaps.insert(seed.id, twap);
                }
                Err(reason) => PENDING_BOOK_TWAP_EVENTS_TOTAL.with_label_values(&[reason]).inc(),
            }
        }
        self.twaps = twaps;
        self.waiting = anchor.waiting_twaps.iter().copied().collect();
        self.anchored = Some((anchor.height, anchor_time_ms));
        self.version += 1;
        let replays: Vec<(u64, HeightEvents)> =
            self.retained.iter().filter(|(height, _, _)| *height > anchor.height).map(|(_, time_ms, events)| (*time_ms, events.clone())).collect();
        for (time_ms, events) in replays {
            self.see_limits(&events);
            self.apply_events(time_ms, &events);
        }
        PENDING_BOOK_TWAP_EVENTS_TOTAL.with_label_values(&["anchor_installed"]).inc();
    }

    /// A checkpoint's TWAP as the new anchor seeds it: its limit, once its market is known, checked against the one
    /// seen - recorded when none was -, before its grid is read; the same parent keeps its floor estimate.
    fn reseeded(&mut self, seed: &CheckpointTwap) -> Result<Twap, &'static str> {
        let market = self.seed_market(seed)?;
        if *self.limits_seen.entry(seed.id).or_insert(market.limit_px) != market.limit_px {
            return Err("limit_conflict");
        }
        Ok(self.carried_over(seed.id, seeded(seed, market)?))
    }

    /// A checkpoint's TWAP as the state before it carries it over: the same parent keeps its floor estimate.
    fn carried_over(&self, twap_id: u64, mut twap: Twap) -> Twap {
        if let Some(before) = self.twaps.get(&twap_id).filter(|before| before.same_parent(&twap)) {
            twap.floor = before.floor.clone();
        }
        twap
    }

    /// A checkpoint TWAP's market by the anchor's table: its coin, lot and limit in this book's units.
    fn seed_market(&self, seed: &CheckpointTwap) -> Result<SeedMarket, &'static str> {
        let (coin, (_, size_decimals, max_decimals)) =
            self.coins.iter().find(|(_, (asset, _, _))| *asset == seed.asset).ok_or("seed_unknown_asset")?;
        let lot = lot(*size_decimals).ok_or("seed_unknown_asset")?;
        let price_decimals = max_decimals.checked_sub(*size_decimals).filter(|decimals| *decimals <= UNIT_DECIMALS).ok_or("seed_bad_limit")?;
        let limit_px = seed
            .limit_px_raw
            .map(|raw| raw.checked_mul(10u64.pow(UNIT_DECIMALS - price_decimals)).map(Px::new).ok_or("seed_bad_limit"))
            .transpose()?;
        Ok(SeedMarket { coin: coin.clone(), lot, limit_px })
    }

    /// Whether an anchor at (`height`, `time_ms`) covers every loss: a known block at or below it, and a block time
    /// `CLOCK_MARGIN_MS` past the latest wall time lines were lost by.
    fn covers_losses(&self, height: u64, time_ms: u64) -> bool {
        self.lost_block.is_none_or(|block| height >= block) && time_ms >= self.lost_by_ms + CLOCK_MARGIN_MS
    }

    /// Whether the TWAPs are known at `height`: an anchor installed covering every loss, the events complete through it.
    fn known_at(&self, height: u64) -> Result<(), TwapsUnknown> {
        let Some((anchor_height, anchor_time_ms)) = self.anchored else { return Err(TwapsUnknown::NoAnchor) };
        if !self.covers_losses(anchor_height, anchor_time_ms) {
            return Err(TwapsUnknown::NoAnchor);
        }
        if self.complete.is_none_or(|(complete, _)| complete < height) {
            return Err(TwapsUnknown::Incomplete);
        }
        Ok(())
    }

    /// Events staged for more than `MAX_AHEAD_HEIGHTS` heights - the complete height held back, the streams going on
    /// -: given up, a loss of every height they were staged for (review 000624 r5 P2-2).
    fn bound_staged(&mut self) {
        let Some(&highest) = self.staged.keys().next_back().filter(|_| self.staged.len() > MAX_AHEAD_HEIGHTS) else { return };
        PENDING_BOOK_TWAP_EVENTS_TOTAL.with_label_values(&["staged_too_long"]).inc();
        self.lost_block = Some(self.lost_block.map_or(highest, |block| block.max(highest)));
        self.staged.clear();
    }

    /// A line of a height already complete: it went out after the height's read-through proof, a loss of that height
    /// (as the account view's late line).
    fn late(&mut self, height: u64) -> bool {
        let late = self.complete.is_some_and(|(complete, _)| height <= complete);
        if late {
            PENDING_BOOK_TWAP_EVENTS_TOTAL.with_label_values(&["late_line"]).inc();
            self.lost_block = Some(self.lost_block.map_or(height, |block| block.max(height)));
        }
        late
    }

    /// The heights through `height` given up: what waits for them dropped, a loss of them.
    fn skip_to(&mut self, height: u64) {
        self.lost_block = Some(self.lost_block.map_or(height, |block| block.max(height)));
        let time_ms = self.log_reads.range(..=height).next_back().map(|(_, (_, time_ms))| *time_ms).or_else(|| self.complete.map(|(_, time_ms)| time_ms));
        self.log_reads.retain(|kept, _| *kept > height);
        self.staged.retain(|kept, _| *kept > height);
        if let Some(time_ms) = time_ms {
            self.complete = Some((height, time_ms));
        }
    }

    #[cfg(test)]
    fn executed(&self, twap_id: u64) -> Option<u64> {
        self.twaps.get(&twap_id).map(|twap| twap.executed)
    }

    #[cfg(test)]
    fn floor(&self, twap_id: u64) -> Option<u64> {
        self.twaps.get(&twap_id).and_then(|twap| twap.floor.estimated)
    }
}

impl Twap {
    /// The slot whose suborder a block at `time_ms` ran: the latest due before it.
    fn slot_at(&self, time_ms: u64) -> Option<u64> {
        time_ms.checked_sub(self.origin_ms).map(|since| since / SLOT_MS)
    }

    const fn due_ms(&self, slot: u64) -> u64 {
        self.origin_ms + SLOT_MS * slot
    }

    /// Its target by slot `slot`: total x (slot + `count_offset`) / N.
    fn target(&self, slot: u64) -> u64 {
        let target = u128::from(self.total) * u128::from(slot + self.count_offset) / u128::from(self.slots);
        u64::try_from(target).unwrap_or(u64::MAX)
    }

    /// A suborder's fills at block time `time_ms`: the executed size, and the floor's evidence.
    fn filled(&mut self, time_ms: u64, size: u64) {
        if let Some(slot) = self.slot_at(time_ms) {
            let deficit = self.target(slot).saturating_sub(self.executed) / self.lot * self.lot;
            let rest = self.total.saturating_sub(self.executed);
            if !self.randomize && size > deficit && size < rest {
                self.floor.observe(slot, size);
            }
        }
        self.executed = self.executed.saturating_add(size);
    }

    /// The suborder of its first slot due at or after `first_of_second_ms`, when due by `boundary_ms`: the next
    /// second's first block, at or past it, runs it (one due later in that second runs there only when due before the
    /// block's time, not known ahead of it - research `mempool_pending_book_20261031`).
    fn slice(&self, twap_id: u64, first_of_second_ms: u64, boundary_ms: u64) -> Option<TwapSlice> {
        let slot = first_of_second_ms.saturating_sub(self.origin_ms).div_ceil(SLOT_MS);
        let due_ms = self.due_ms(slot);
        if slot >= self.slots || due_ms > boundary_ms {
            return None;
        }
        let deficit = self.target(slot).checked_sub(self.executed).filter(|deficit| *deficit > 0)?;
        let rounded = deficit / self.lot * self.lot;
        let even = self.total / self.slots;
        let floor = self.floor.estimated;
        let cap = match floor {
            Some(floor) => 3 * even.max(floor),
            None if rounded > 3 * even => {
                PENDING_BOOK_TWAP_EVENTS_TOTAL.with_label_values(&["catch_up_unknown"]).inc();
                return None;
            }
            None => rounded,
        };
        let rest = self.total.saturating_sub(self.executed);
        let size = rounded.max(floor.unwrap_or(0)).min(cap).min(rest);
        (size > 0).then(|| TwapSlice { twap_id, user: self.user, slot, is_buy: self.is_buy, sz: Sz::new(size), limit_px: self.limit_px, due_ms })
    }

    /// The same parent: user, coin, side, total, slots and grid.
    fn same_parent(&self, other: &Self) -> bool {
        (self.user, &self.coin, self.is_buy, self.total, self.slots, self.origin_ms) == (other.user, &other.coin, other.is_buy, other.total, other.slots, other.origin_ms)
    }
}

/// A checkpoint TWAP's market in this book's units.
struct SeedMarket {
    coin: Coin,
    lot: u64,
    limit_px: Option<Px>,
}

/// A checkpoint's TWAP on its market: its grid on the one of its next due time `n`, its target's count from `S`.
fn seeded(seed: &CheckpointTwap, market: SeedMarket) -> Result<Twap, &'static str> {
    let SeedMarket { coin, lot, limit_px } = market;
    let origin_ms = seed.start_ms + seed.next_due_ms.checked_sub(seed.start_ms).ok_or("seed_bad_due")? % SLOT_MS;
    let next_slot = (seed.next_due_ms - origin_ms) / SLOT_MS;
    let count_offset = seed.slot_count.checked_sub(next_slot).filter(|offset| *offset <= 1).ok_or("seed_bad_slot_count")?;
    Ok(Twap {
        user: seed.user,
        coin,
        is_buy: seed.is_buy,
        total: seed.total.checked_mul(lot).ok_or("seed_bad_size")?,
        executed: seed.executed.checked_mul(lot).ok_or("seed_bad_size")?,
        lot,
        slots: seed.duration_s / (SLOT_MS / 1_000) + 1,
        origin_ms,
        count_offset,
        randomize: seed.randomize,
        limit_px,
        floor: FloorEstimate::default(),
    })
}

/// An activation's max buy / min sell price, when it has one.
fn activation_limit(activation: &TwapActivation) -> Result<Option<Px>, &'static str> {
    activation.limit_px.as_deref().map(|text| Px::parse_from_str(text).map_err(|_| "activation_bad_limit")).transpose()
}

/// One step of a coin's size grid in 1e-8 units.
fn lot(size_decimals: u32) -> Option<u64> {
    UNIT_DECIMALS.checked_sub(size_decimals).map(|decimals| 10u64.pow(decimals))
}

/// The anchor's table by coin: (asset, `szDecimals`, max decimals).
fn anchor_coins(coins: &ActionCoins) -> FxHashMap<Coin, (u32, u32, u32)> {
    coins.coins.iter().map(|(asset, named)| (Coin::new(&named.coin), (*asset, named.size_decimals, named.max_decimals))).collect()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{accounts::book::ActionAsset, pending::twap_streams::TwapActivation};

    const HYPE: u32 = 159;
    const USER: Address = Address::repeat_byte(0x5a);
    /// 1e-8 units of one HYPE (`szDecimals` 2: a lot of 0.01).
    const ONE: u64 = 100_000_000;
    const LOT: u64 = 1_000_000;
    /// Block heights at 100 ms each from 1,000 s.
    const T0_MS: u64 = 1_000_000;

    fn time_ms(height: u64) -> u64 {
        T0_MS + height * 100
    }

    fn coins() -> ActionCoins {
        let asset = ActionAsset { coin: "HYPE".to_string(), size_decimals: 2, max_decimals: 6, max_market_value: None };
        ActionCoins { coins: std::iter::once((HYPE, asset)).collect() }
    }

    fn anchor(height: u64, twaps: Vec<CheckpointTwap>) -> Arc<PendingAnchor> {
        Arc::new(PendingAnchor { height, coins: coins(), twaps, waiting_twaps: vec![9] })
    }

    /// A coin listed after HYPE, asset 200, of HYPE's decimals.
    const NEW: u32 = 200;

    fn anchor_listing_new(height: u64, twaps: Vec<CheckpointTwap>) -> Arc<PendingAnchor> {
        let mut coins = coins();
        coins.coins.insert(NEW, ActionAsset { coin: "NEW".to_string(), size_decimals: 2, max_decimals: 6, max_market_value: None });
        Arc::new(PendingAnchor { height, coins, twaps, waiting_twaps: Vec::new() })
    }

    fn activation(twap_id: u64, total: u64, minutes: u64, start_ms: u64) -> TwapActivation {
        TwapActivation {
            twap_id,
            user: USER,
            coin: Coin::new("HYPE"),
            is_buy: true,
            total: Sz::new(total),
            minutes,
            randomize: false,
            reduce_only: false,
            start_ms,
            limit_px: None,
        }
    }

    fn fill(twap_id: u64, tid: u64, sz: u64) -> TwapFill {
        TwapFill { twap_id, tid, sz: Sz::new(sz) }
    }

    /// The block log, the two streams and the replica, driven block by block on one clock.
    struct Harness {
        book: TwapBook,
        clock: Instant,
        /// The last height the log showed.
        logged: u64,
    }

    impl Harness {
        /// Started at wall time 0: an anchor at height 1 covers the start.
        fn new() -> Self {
            Self { book: TwapBook::new(0), clock: Instant::now(), logged: 0 }
        }

        fn tick(&mut self) -> Instant {
            self.clock += Duration::from_millis(1);
            self.clock
        }

        /// The log's next block, with `cancels`.
        fn log(&mut self, cancels: &[u64]) {
            self.logged += 1;
            let at = self.tick();
            self.book.on_block(self.logged, at, time_ms(self.logged), cancels);
        }

        fn prove(&mut self) {
            let at = self.tick();
            self.book.on_read_through(TwapStream::Statuses, at);
            self.book.on_read_through(TwapStream::Fills, at);
        }

        /// Blocks through `height` logged, the next one too, both streams proven after it; settled at `height`.
        fn through(&mut self, height: u64) -> Result<(), TwapsUnknown> {
            while self.logged <= height {
                self.log(&[]);
            }
            self.prove();
            self.book.settle(height)
        }

        /// Anchored at height 1 with `twaps`, settled there.
        fn anchored(twaps: Vec<CheckpointTwap>) -> Self {
            let mut harness = Self::new();
            harness.book.on_anchor(anchor(1, twaps));
            assert_eq!(harness.through(1), Ok(()));
            harness
        }

        fn due_sizes(&self, height: u64) -> Vec<(u64, u64, u64)> {
            let due = self.book.due(time_ms(height));
            due.values().flatten().map(|slice| (slice.twap_id, slice.slot, slice.sz.value())).collect()
        }
    }

    /// A TWAP of `minutes` buying `total` (1e-8 units), activated at `height` (its start that block's time).
    fn activate(harness: &mut Harness, twap_id: u64, total: u64, minutes: u64, height: u64) {
        harness.book.on_statuses(height, vec![TwapEvent::Activated(activation(twap_id, total, minutes, time_ms(height)))]);
    }

    /// The first first-of-second block whose time is past `ms`.
    fn first_block_after(ms: u64) -> u64 {
        let second = (ms / 1_000 + 1) * 1_000;
        (second - T0_MS).div_ceil(100)
    }

    /// Review 000624 r2 P1: a height's fills split over two lines, the TWAP's after another's - nothing applies until
    /// both streams prove a read-through after the next block's log line.
    #[test]
    fn a_height_applies_once_proven_after_the_next_log_line() {
        let mut harness = Harness::anchored(Vec::new());
        activate(&mut harness, 7, 110 * ONE, 5, 3);
        harness.log(&[]);
        harness.prove();
        harness.log(&[]);
        assert_eq!(harness.book.settle(3), Err(TwapsUnknown::Incomplete), "the log of 4 read after the proofs");
        let read_at = harness.clock;
        harness.book.on_read_through(TwapStream::Statuses, read_at);
        harness.book.on_read_through(TwapStream::Fills, read_at);
        assert_eq!(harness.book.settle(3), Err(TwapsUnknown::Incomplete), "a read-through at the read itself proves nothing after it");
        harness.prove();
        assert_eq!(harness.book.settle(3), Ok(()));
        let slot0 = first_block_after(time_ms(3) + FIRST_SLOT_MS);
        harness.through(slot0 - 1).unwrap();
        harness.book.on_fills(slot0, vec![fill(8, 1, ONE)]);
        harness.log(&[]);
        harness.log(&[]);
        harness.book.on_fills(slot0, vec![fill(7, 2, 20 * ONE)]);
        assert_eq!(harness.book.settle(slot0), Err(TwapsUnknown::Incomplete), "no proof after the next block's log line");
        harness.prove();
        assert_eq!(harness.book.settle(slot0), Ok(()));
        assert_eq!(harness.book.executed(7), Some(20 * ONE), "both lines of the height");
        // a line of the height once applied: a loss of it, unknown until an anchor above it
        harness.book.on_fills(slot0, vec![fill(7, 3, ONE)]);
        assert_eq!(harness.through(slot0 + 1), Err(TwapsUnknown::NoAnchor));
        assert_eq!(harness.book.executed(7), Some(20 * ONE));
    }

    /// Review 000624 r1 P2-2: total 110 over 11 slots; slot 0 filled 20, slot 1's target 20 sends nothing, slot 2 is
    /// predicted 10 - the cursor moved by the first-of-second blocks alone.
    #[test]
    fn the_cursor_moves_by_first_of_second_blocks_alone() {
        let mut harness = Harness::anchored(Vec::new());
        activate(&mut harness, 7, 110 * ONE, 5, 3);
        let start_ms = time_ms(3);
        let slot0 = first_block_after(start_ms + FIRST_SLOT_MS);
        harness.through(slot0 - 1).unwrap();
        assert_eq!(harness.due_sizes(slot0 - 1), [(7, 0, 10 * ONE)], "slot 0 due in the next first-of-second block");
        harness.book.on_fills(slot0, vec![fill(7, 1, 20 * ONE)]);
        let slot1 = first_block_after(start_ms + FIRST_SLOT_MS + SLOT_MS);
        harness.through(slot1 - 1).unwrap();
        assert_eq!(harness.due_sizes(slot1 - 1), [], "slot 1's target 20 is met");
        let slot2 = first_block_after(start_ms + FIRST_SLOT_MS + 2 * SLOT_MS);
        harness.through(slot2 - 1).unwrap();
        assert_eq!(harness.due_sizes(slot2 - 1), [(7, 2, 10 * ONE)]);
        harness.through(slot2 + 3).unwrap();
        assert_eq!(harness.due_sizes(slot2 + 3), [], "slot 2 past, unfilled, until slot 3 nears");
    }

    /// Review 000624 r2 P2-1: 10 a slot; the first three slots unfilled, the fourth would catch up 40. With the floor
    /// estimated at 10, it is capped at 3 x 10; without one, not predicted.
    #[test]
    fn a_catch_up_is_capped_at_three_normal_suborders() {
        for floor in [true, false] {
            let mut harness = Harness::anchored(Vec::new());
            activate(&mut harness, 7, 110 * ONE, 5, 3);
            harness.through(3).unwrap();
            if floor {
                harness.book.twaps.get_mut(&7).unwrap().floor.estimated = Some(10 * ONE);
            }
            let slot3 = first_block_after(time_ms(3) + FIRST_SLOT_MS + 3 * SLOT_MS);
            harness.through(slot3 - 1).unwrap();
            let expected = if floor { vec![(7, 3, 30 * ONE)] } else { vec![] };
            assert_eq!(harness.due_sizes(slot3 - 1), expected, "floor {floor}");
        }
    }

    /// A size above the deficit seen at two slots is the estimated floor, one seen once or twice at one slot is not; a
    /// randomized TWAP has none. Once estimated, a deficit under it sends the floor.
    #[test]
    fn a_floor_is_estimated_from_two_slots() {
        let mut harness = Harness::anchored(Vec::new());
        // 1 HYPE over 5 minutes: a slot's share 0.09, under a floor of 0.14
        activate(&mut harness, 7, ONE, 5, 3);
        let slot = |k: u64| first_block_after(time_ms(3) + FIRST_SLOT_MS + k * SLOT_MS);
        harness.through(slot(0) - 1).unwrap();
        harness.book.on_fills(slot(0), vec![fill(7, 1, 7 * LOT), fill(7, 2, 7 * LOT)]);
        harness.through(slot(0)).unwrap();
        assert_eq!(harness.book.floor(7), None, "seen once");
        harness.through(slot(2) - 1).unwrap();
        assert_eq!(harness.due_sizes(slot(2) - 1), [(7, 2, 13 * LOT)], "the deficit before the floor is known");
        harness.book.on_fills(slot(2), vec![fill(7, 3, 14 * LOT)]);
        harness.through(slot(3) - 1).unwrap();
        assert_eq!(harness.book.floor(7), Some(14 * LOT));
        assert_eq!(harness.due_sizes(slot(3) - 1), [(7, 3, 14 * LOT)], "the floor over a deficit of 0.08");
        harness.through(slot(4) - 1).unwrap();
        assert_eq!(harness.due_sizes(slot(4) - 1), [(7, 4, 17 * LOT)], "a deficit of 0.17 over the floor, slot 3 unfilled");
    }

    /// A size seen above the deficit twice at one slot is no floor; a randomized TWAP's sizes estimate none.
    #[test]
    fn a_floor_needs_two_slots_and_no_randomizing() {
        for randomize in [false, true] {
            let mut harness = Harness::anchored(Vec::new());
            let twap = TwapActivation { randomize, ..activation(7, ONE, 5, time_ms(3)) };
            harness.book.on_statuses(3, vec![TwapEvent::Activated(twap)]);
            let slot = |k: u64| first_block_after(time_ms(3) + FIRST_SLOT_MS + k * SLOT_MS);
            harness.book.on_fills(slot(0), vec![fill(7, 1, 14 * LOT)]);
            harness.book.on_fills(slot(0) + 1, vec![fill(7, 2, 14 * LOT)]);
            harness.through(slot(1)).unwrap();
            assert_eq!(harness.book.floor(7), None, "twice at one slot");
            harness.book.on_fills(slot(2), vec![fill(7, 3, 14 * LOT)]);
            harness.through(slot(2)).unwrap();
            assert_eq!(harness.book.floor(7), (!randomize).then_some(14 * LOT), "randomize {randomize}");
        }
    }

    /// A suborder sends no more than the rest: 0.9 of 1 HYPE executed, the floor 0.14 over the rest 0.1.
    #[test]
    fn a_suborder_never_sends_past_the_rest() {
        let mut harness = Harness::anchored(Vec::new());
        activate(&mut harness, 7, ONE, 5, 3);
        harness.book.on_fills(4, vec![fill(7, 1, 90 * LOT)]);
        harness.through(4).unwrap();
        harness.book.twaps.get_mut(&7).unwrap().floor.estimated = Some(14 * LOT);
        let slot9 = first_block_after(time_ms(3) + FIRST_SLOT_MS + 9 * SLOT_MS);
        harness.through(slot9 - 1).unwrap();
        assert_eq!(harness.due_sizes(slot9 - 1), [(7, 9, 10 * LOT)]);
    }

    /// A slot due at the second's start runs in that second's first block, its time at or past it: predicted on the
    /// block before; one due 1 ms later waits for the next second's.
    #[test]
    fn a_slot_due_at_the_second_runs_in_its_first_block() {
        let mut harness = Harness::anchored(Vec::new());
        activate(&mut harness, 7, 110 * ONE, 5, 10);
        let later = activation(8, 110 * ONE, 5, time_ms(10) + 1);
        harness.book.on_statuses(10, vec![TwapEvent::Activated(later)]);
        harness.through(19).unwrap();
        assert_eq!(harness.due_sizes(19), [(7, 0, 10 * ONE)], "due at {}", time_ms(20));
        harness.book.on_fills(20, vec![fill(7, 1, 10 * ONE)]);
        harness.through(29).unwrap();
        assert_eq!(harness.due_sizes(29), [(8, 0, 10 * ONE)]);
    }

    /// A newer anchor replaces the state and replays the events after it; a TWAP the same parent keeps its floor.
    #[test]
    fn an_anchor_replays_the_events_after_it_and_keeps_floors() {
        let seed = CheckpointTwap {
            id: 7,
            user: USER,
            asset: HYPE,
            is_buy: true,
            total: 10_000,
            executed: 0,
            duration_s: 300,
            start_ms: time_ms(1),
            next_due_ms: time_ms(1) + FIRST_SLOT_MS,
            slot_count: 1,
            randomize: false,
            reduce_only: false,
            limit_px_raw: Some(870_000),
        };
        let mut harness = Harness::anchored(vec![seed.clone()]);
        assert_eq!(harness.book.twaps[&7].limit_px, Some(Px::parse_from_str("87").unwrap()), "sp in price units, 4 decimals");
        harness.book.twaps.get_mut(&7).unwrap().floor.estimated = Some(14 * LOT);
        harness.through(19).unwrap();
        harness.book.on_fills(20, vec![fill(7, 1, 2 * ONE)]);
        harness.book.on_fills(25, vec![fill(7, 2, ONE)]);
        harness.through(30).unwrap();
        assert_eq!(harness.book.executed(7), Some(3 * ONE));
        // the checkpoint of height 20: its own fill in it; the fill of 25 replayed over it
        harness.book.on_anchor(anchor(20, vec![CheckpointTwap { executed: 200, ..seed }]));
        harness.through(31).unwrap();
        assert_eq!((harness.book.anchored, harness.book.executed(7), harness.book.floor(7)), (Some((20, time_ms(20))), Some(3 * ONE), Some(14 * LOT)));
        let other = CheckpointTwap { total: 20_000, ..seed };
        harness.book.on_anchor(anchor(30, vec![other]));
        harness.through(32).unwrap();
        assert_eq!(harness.book.floor(7), None, "another parent under the id learns anew");
    }

    /// Review 000624 r5 P2-1, r6 P2: a parent activated with `stopPx` 100 that a checkpoint gives `sp` 120 is kept out -
    /// its activation seen before the first anchor too, and past a checkpoint whose state of it does not read -, while
    /// one the checkpoint alone shows is followed; the same limit both ways is followed.
    #[test]
    fn a_checkpoint_limit_not_the_one_seen_keeps_the_parent_out() {
        let seed = |id: u64, raw: u64| CheckpointTwap {
            id,
            user: USER,
            asset: HYPE,
            is_buy: true,
            total: 10_000,
            executed: 0,
            duration_s: 300,
            start_ms: time_ms(3),
            next_due_ms: time_ms(3) + FIRST_SLOT_MS,
            slot_count: 1,
            randomize: false,
            reduce_only: false,
            limit_px_raw: Some(raw),
        };
        let followed = |book: &TwapBook| (book.twaps.contains_key(&7), book.twaps.contains_key(&8));
        for (raw, kept) in [(1_200_000, false), (1_000_000, true)] {
            for anchored_before in [true, false] {
                let mut harness = if anchored_before { Harness::anchored(Vec::new()) } else { Harness::new() };
                let parent = TwapActivation { limit_px: Some("100".to_string()), ..activation(7, 100 * ONE, 5, time_ms(3)) };
                harness.book.on_statuses(3, vec![TwapEvent::Activated(parent)]);
                assert_eq!(harness.through(10).is_ok(), anchored_before);
                harness.book.on_anchor(anchor(10, vec![seed(7, raw), seed(8, 1_200_000)]));
                assert_eq!(harness.through(11), Ok(()));
                assert_eq!(followed(&harness.book), (kept, true), "sp {raw}, anchored before {anchored_before}");
                let unread = CheckpointTwap { slot_count: 3, ..seed(7, raw) };
                harness.book.on_anchor(anchor(11, vec![unread, seed(8, 1_200_000)]));
                harness.through(12).unwrap();
                assert_eq!(followed(&harness.book), (false, true), "7's state not read");
                harness.book.on_anchor(anchor(12, vec![seed(7, raw), seed(8, 1_200_000)]));
                harness.through(13).unwrap();
                assert_eq!(followed(&harness.book), (kept, true), "sp {raw}, anchored before {anchored_before}, read again");
            }
        }
    }

    /// Review 000624 r7 P2: the limit is evidence once its market is known, whether the TWAP can be followed or not -
    /// an activation on a coin the anchor before it did not list, a checkpoint whose grid of it does not read -: a later
    /// checkpoint's other limit keeps the parent out, the same one follows it; one the checkpoints alone show is
    /// followed.
    #[test]
    fn a_limit_seen_of_a_twap_not_followed_still_counts() {
        let seed = |id: u64, asset: u32, raw: u64| CheckpointTwap {
            id,
            user: USER,
            asset,
            is_buy: true,
            total: 10_000,
            executed: 0,
            duration_s: 300,
            start_ms: time_ms(3),
            next_due_ms: time_ms(3) + FIRST_SLOT_MS,
            slot_count: 1,
            randomize: false,
            reduce_only: false,
            limit_px_raw: Some(raw),
        };
        let followed = |book: &TwapBook| (book.twaps.contains_key(&7), book.twaps.contains_key(&8));
        for (raw, kept) in [(1_200_000, false), (1_000_000, true)] {
            // activated at 3 on NEW, the first anchor (at 2) not listing NEW
            let mut harness = Harness::new();
            let parent = TwapActivation { coin: Coin::new("NEW"), limit_px: Some("100".to_string()), ..activation(7, 100 * ONE, 5, time_ms(3)) };
            harness.book.on_statuses(3, vec![TwapEvent::Activated(parent)]);
            assert_eq!(harness.through(10), Err(TwapsUnknown::NoAnchor));
            harness.book.on_anchor(anchor(2, Vec::new()));
            assert_eq!(harness.through(11), Ok(()));
            assert_eq!(followed(&harness.book), (false, false), "NEW not listed");
            harness.book.on_anchor(anchor_listing_new(11, vec![seed(7, NEW, raw), seed(8, HYPE, 1_200_000)]));
            harness.through(12).unwrap();
            assert_eq!(followed(&harness.book), (kept, true), "activated, sp {raw}");
            // first seen in a checkpoint at 100 whose grid of it does not read
            let mut harness = Harness::anchored(Vec::new());
            let unread = CheckpointTwap { slot_count: 3, ..seed(7, HYPE, 1_000_000) };
            harness.book.on_anchor(anchor(10, vec![unread, seed(8, HYPE, 1_200_000)]));
            harness.through(11).unwrap();
            assert_eq!(followed(&harness.book), (false, true), "7's grid not read");
            harness.book.on_anchor(anchor(11, vec![seed(7, HYPE, raw), seed(8, HYPE, 1_200_000)]));
            harness.through(12).unwrap();
            assert_eq!(followed(&harness.book), (kept, true), "seeded unread, sp {raw}");
        }
    }

    /// Review 000624 r7 P3: no anchor installed, a TWAP's cancel or end takes its limit's evidence; an anchor not listing
    /// one whose end was not seen takes it too.
    #[test]
    fn a_limit_seen_goes_with_its_twap() {
        let mut harness = Harness::new();
        for twap_id in [7, 8, 9] {
            let parent = TwapActivation { limit_px: Some("100".to_string()), ..activation(twap_id, ONE, 5, time_ms(3)) };
            harness.book.on_statuses(3, vec![TwapEvent::Activated(parent)]);
        }
        harness.book.on_statuses(4, vec![TwapEvent::Ended { twap_id: 8 }]);
        harness.through(5).unwrap_err();
        let seen = |book: &TwapBook| book.limits_seen.keys().copied().collect::<std::collections::BTreeSet<_>>();
        assert_eq!(seen(&harness.book), [7, 9].into());
        harness.logged += 1;
        let at = harness.tick();
        harness.book.on_block(7, at, time_ms(7), &[7]);
        harness.through(7).unwrap_err();
        assert_eq!(seen(&harness.book), [9].into());
        harness.book.on_anchor(anchor(7, Vec::new()));
        assert_eq!(harness.through(8), Ok(()));
        assert!(harness.book.limits_seen.is_empty());
    }

    /// Review 000624 r6 P3: a block log `twapCancel` staged at a height of its own counts toward the bound too.
    #[test]
    fn a_cancel_staged_counts_toward_the_bound() {
        let mut harness = Harness::anchored(Vec::new());
        harness.through(5).unwrap();
        for height in 10..10 + u64::try_from(MAX_AHEAD_HEIGHTS).unwrap() {
            harness.book.on_fills(height, vec![fill(7, height, LOT)]);
        }
        assert_eq!(harness.book.staged.len(), MAX_AHEAD_HEIGHTS);
        harness.log(&[7]);
        assert!(harness.book.staged.len() <= MAX_AHEAD_HEIGHTS);
        assert_eq!(harness.book.settle(5), Err(TwapsUnknown::NoAnchor));
    }

    /// Review 000624 r5 P2-2: the complete height held at 5 - the replica stalled -, the streams going on with a fill a
    /// height: past `MAX_AHEAD_HEIGHTS` heights staged they are given up, a loss of them - unknown at once -, until an
    /// anchor at or above the highest.
    #[test]
    fn events_staged_too_far_ahead_are_a_loss() {
        let mut harness = Harness::anchored(Vec::new());
        harness.through(5).unwrap();
        let ahead = u64::try_from(MAX_AHEAD_HEIGHTS).unwrap() + 1;
        for height in 6..6 + ahead {
            harness.book.on_fills(height, vec![fill(7, height, LOT)]);
            assert!(harness.book.staged.len() <= MAX_AHEAD_HEIGHTS);
        }
        assert_eq!(harness.book.settle(5), Err(TwapsUnknown::NoAnchor));
        let highest = 5 + ahead;
        for height in (1_000..highest).step_by(1_000) {
            assert_eq!(harness.through(height), Err(TwapsUnknown::NoAnchor));
        }
        harness.book.on_anchor(anchor(highest - 1, Vec::new()));
        assert_eq!(harness.through(highest), Err(TwapsUnknown::NoAnchor), "an anchor under the loss");
        harness.book.on_anchor(anchor(highest, Vec::new()));
        assert_eq!(harness.through(highest + 1), Ok(()));
    }

    /// A line of a height already complete is a loss of it: unknown until an anchor at or above it. A loss of unknown
    /// blocks needs an anchor whose block time is 1 s past its detection; the start is one.
    #[test]
    fn losses_wait_for_a_covering_anchor() {
        let mut harness = Harness::new();
        harness.book.on_anchor(anchor(1, Vec::new()));
        harness.book.lost_by_ms = time_ms(1);
        assert_eq!(harness.through(1), Err(TwapsUnknown::NoAnchor), "the start not covered");
        harness.book.on_anchor(anchor(12, Vec::new()));
        assert_eq!(harness.through(12), Ok(()));
        harness.book.on_fills(10, vec![fill(7, 1, ONE)]);
        assert_eq!(harness.through(13), Ok(()), "a late line below the anchor: covered");
        harness.book.on_fills(13, vec![fill(7, 2, ONE)]);
        assert_eq!(harness.through(14), Err(TwapsUnknown::NoAnchor), "a late line above it");
        harness.book.on_anchor(anchor(13, Vec::new()));
        assert_eq!(harness.through(14), Ok(()));
        harness.book.on_loss(time_ms(14));
        assert_eq!(harness.through(15), Err(TwapsUnknown::NoAnchor));
        harness.book.on_anchor(anchor(23, Vec::new()));
        assert_eq!(harness.through(24), Err(TwapsUnknown::NoAnchor), "23's block time 0.9 s past the loss");
        harness.book.on_anchor(anchor(24, Vec::new()));
        assert_eq!(harness.through(25), Ok(()), "24's 1 s past it");
    }

    /// A hole in the block log is a loss of the heights missing.
    #[test]
    fn a_block_log_hole_is_a_loss() {
        let mut harness = Harness::anchored(Vec::new());
        harness.through(5).unwrap();
        harness.logged += 3;
        assert_eq!(harness.through(12), Err(TwapsUnknown::NoAnchor));
        harness.book.on_anchor(anchor(12, Vec::new()));
        assert_eq!(harness.through(13), Ok(()));
    }

    /// A checkpoint's TWAP takes its grid from its next due time `n` and its target's count from `S`: a grid of start
    /// + 0 s, `S` the slot itself - total x k / N -, against start + 1 s and `S` one past it.
    #[test]
    fn a_seed_takes_its_grid_from_n_and_its_count_from_s() {
        let seed = |next_due_ms: u64, slot_count: u64| CheckpointTwap {
            id: 7,
            user: USER,
            asset: HYPE,
            is_buy: true,
            total: 11_000,
            executed: 0,
            duration_s: 300,
            start_ms: time_ms(1),
            next_due_ms,
            slot_count,
            randomize: false,
            reduce_only: false,
            limit_px_raw: None,
        };
        let cases = [(time_ms(1) + 30_000, 1, time_ms(1), 10 * ONE), (time_ms(1) + 31_000, 2, time_ms(1) + FIRST_SLOT_MS, 20 * ONE)];
        for (next_due_ms, slot_count, origin_ms, slot1_target) in cases {
            let harness = Harness::anchored(vec![seed(next_due_ms, slot_count)]);
            let twap = &harness.book.twaps[&7];
            assert_eq!((twap.origin_ms, twap.target(1)), (origin_ms, slot1_target), "n {next_due_ms}");
        }
        let book = Harness::anchored(vec![seed(time_ms(1) + 30_000, 3)]).book;
        assert!(book.twaps.is_empty(), "an S two past its slot is not followed");
    }

    /// A trade's fill once by its tid, whatever lines repeat it.
    #[test]
    fn a_fill_counts_once_by_its_tid() {
        let mut harness = Harness::anchored(Vec::new());
        activate(&mut harness, 7, ONE, 5, 3);
        harness.book.on_fills(5, vec![fill(7, 1, 7 * LOT), fill(7, 1, 7 * LOT), fill(7, 2, LOT)]);
        harness.book.on_fills(5, vec![fill(7, 1, 7 * LOT)]);
        harness.through(5).unwrap();
        assert_eq!(harness.book.executed(7), Some(8 * LOT));
    }

    /// Research `twap_randomize_20261101`: a TWAP waiting for its trigger - in the anchor, or by a `waitingForTrigger`
    /// since - starts its grid at its activation; one activated at once 1 s after it.
    #[test]
    fn a_twap_that_waited_for_its_trigger_starts_its_grid_at_activation() {
        let mut harness = Harness::anchored(Vec::new());
        harness.book.on_statuses(3, vec![TwapEvent::Waiting { twap_id: 8 }]);
        for twap_id in [7, 8, 9] {
            activate(&mut harness, twap_id, ONE, 5, 4);
        }
        harness.through(4).unwrap();
        let origin = |twap_id: u64| harness.book.twaps[&twap_id].origin_ms;
        assert_eq!((origin(7), origin(8), origin(9)), (time_ms(4) + FIRST_SLOT_MS, time_ms(4), time_ms(4)), "9 waits in the anchor, 8 since");
    }

    /// A cancel in the block log, an end in the statuses: the TWAP goes at its height; an unknown coin's activation is
    /// not followed.
    #[test]
    fn a_cancel_or_an_end_takes_the_twap_out() {
        let mut harness = Harness::anchored(Vec::new());
        activate(&mut harness, 7, ONE, 5, 3);
        activate(&mut harness, 8, ONE, 5, 3);
        let mut other = activation(9, ONE, 5, time_ms(3));
        other.coin = Coin::new("NOPE");
        harness.book.on_statuses(3, vec![TwapEvent::Activated(other)]);
        harness.through(4).unwrap();
        assert_eq!((harness.book.executed(7), harness.book.executed(8), harness.book.executed(9)), (Some(0), Some(0), None));
        while harness.logged < 5 {
            harness.log(&[]);
        }
        harness.logged += 1;
        let at = harness.tick();
        harness.book.on_block(6, at, time_ms(6), &[7]);
        harness.book.on_statuses(6, vec![TwapEvent::Ended { twap_id: 8 }]);
        harness.through(6).unwrap();
        assert_eq!((harness.book.executed(7), harness.book.executed(8)), (None, None));
    }
}

/// Real data: a checkpoint seeds the book, then bm's block log, TWAP statuses and fills follow, block by block (each
/// height's lines before the next block's log line, proven through after it); at each block before a first-of-second
/// block the predicted suborders are compared with that block's TWAP fills and refusals.
///
/// `PENDING_TWAP_CHECKPOINT=<.rmp> PENDING_TWAP_BLOCKS=<replica_cmds files,...> PENDING_TWAP_STATUSES=<hour files,...>
/// PENDING_TWAP_FILLS=<hour files,...> PENDING_TWAP_BLOCK_COUNT=<n> cargo test --release -p server --lib -- --ignored
/// real_twap_suborders --nocapture`
#[cfg(test)]
mod real_data {
    use std::{
        collections::BTreeMap,
        io::{BufRead, BufReader},
        sync::Arc,
        time::{Duration, Instant},
    };

    use rustc_hash::FxHashMap;

    use super::*;
    use crate::{
        accounts::twap_tracker::tests::real_pending_anchor,
        pending::{
            blocks::parse_block,
            twap_streams::{parse_fill_line, parse_status_line},
        },
    };

    fn env(name: &str) -> String {
        std::env::var(name).unwrap_or_else(|_| panic!("{name}"))
    }

    fn lines(paths: &str) -> impl Iterator<Item = String> + '_ {
        paths.split(',').flat_map(|path| BufReader::new(std::fs::File::open(path).unwrap()).lines().map(Result::unwrap))
    }

    #[test]
    #[ignore = "needs PENDING_TWAP_* archives"]
    #[expect(clippy::too_many_lines, reason = "one replay: read, follow, count")]
    fn real_twap_suborders() {
        let started = Instant::now();
        let anchor = real_pending_anchor(std::path::Path::new(&env("PENDING_TWAP_CHECKPOINT")));
        println!("checkpoint {} parsed in {:.1} s: {} running TWAPs", anchor.height, started.elapsed().as_secs_f64(), anchor.twaps.len());
        let count: u64 = env("PENDING_TWAP_BLOCK_COUNT").parse().unwrap();
        // the floors estimated over the first blocks: none counted
        let warm_up: u64 = std::env::var("PENDING_TWAP_WARM_UP_BLOCKS").map_or(0, |blocks| blocks.parse().unwrap());
        let (first, last) = (anchor.height - 20, anchor.height + count);
        let pending_anchor_height = anchor.height;
        let pending_anchor = Arc::new(anchor);
        // each stream's lines by height
        let mut statuses: BTreeMap<u64, Vec<TwapEvent>> = BTreeMap::new();
        let mut refused: FxHashMap<u64, Vec<u64>> = FxHashMap::default();
        for line in lines(&env("PENDING_TWAP_STATUSES")) {
            let parsed = parse_status_line(&line).unwrap();
            if (first..=last).contains(&parsed.height) {
                for event in &parsed.events {
                    if let TwapEvent::Refused { twap_id } = event {
                        refused.entry(parsed.height).or_default().push(*twap_id);
                    }
                }
                statuses.entry(parsed.height).or_default().extend(parsed.events);
            }
        }
        let mut fills: BTreeMap<u64, Vec<TwapFill>> = BTreeMap::new();
        for line in lines(&env("PENDING_TWAP_FILLS")) {
            let parsed = parse_fill_line(&line).unwrap();
            if (first..=last).contains(&parsed.height) && !parsed.fills.is_empty() {
                fills.entry(parsed.height).or_default().extend(parsed.fills);
            }
        }
        let mut blocks: BTreeMap<u64, (u64, Vec<u64>)> = BTreeMap::new();
        for line in lines(&env("PENDING_TWAP_BLOCKS")) {
            let Some(height) = crate::accounts::replica::line_height(&line) else { continue };
            if (first..=last + 1).contains(&height) {
                let block = parse_block(&line).unwrap();
                blocks.insert(height, (block.time_ms, block.twap_cancels));
            }
        }
        println!("inputs read in {:.1} s: {} blocks", started.elapsed().as_secs_f64(), blocks.len());
        let mut book = TwapBook::new(0);
        book.on_anchor(pending_anchor);
        let mut clock = Instant::now();
        let mut tick = || {
            clock += Duration::from_micros(1);
            clock
        };
        let mut counts: BTreeMap<String, u64> = BTreeMap::new();
        let mut samples: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let heights: Vec<u64> = blocks.keys().copied().collect();
        for window in heights.windows(2) {
            let (height, next) = (window[0], window[1]);
            if let Some(events) = statuses.remove(&height) {
                book.on_statuses(height, events);
            }
            if let Some(height_fills) = fills.get(&height) {
                book.on_fills(height, height_fills.clone());
            }
            let (next_ms, next_cancels) = &blocks[&next];
            if height == heights[0] {
                let (time_ms, cancels) = &blocks[&height];
                book.on_block(height, tick(), *time_ms, cancels);
            }
            book.on_block(next, tick(), *next_ms, next_cancels);
            let at = tick();
            book.on_read_through(TwapStream::Statuses, at);
            book.on_read_through(TwapStream::Fills, at);
            let known = book.settle(height);
            let (time_ms, _) = &blocks[&height];
            // the next block is a first-of-second block: what it runs, predicted at this basis
            if next_ms / 1_000 <= time_ms / 1_000 {
                continue;
            }
            if known.is_err() {
                *counts.entry("first-of-second blocks unknown".to_string()).or_default() += 1;
                continue;
            }
            if height < pending_anchor_height + warm_up {
                continue;
            }
            *counts.entry("first-of-second blocks".to_string()).or_default() += 1;
            let due = book.due(*time_ms);
            let predicted: FxHashMap<u64, u64> = due.values().flatten().map(|slice| (slice.twap_id, slice.sz.value())).collect();
            let slot_of: FxHashMap<u64, u64> = due.values().flatten().map(|slice| (slice.twap_id, slice.slot)).collect();
            let mut actual: FxHashMap<u64, u64> = FxHashMap::default();
            for fill in fills.get(&next).into_iter().flatten() {
                *actual.entry(fill.twap_id).or_default() += fill.sz.value();
            }
            let refusals = refused.get(&next).cloned().unwrap_or_default();
            for (twap_id, size) in &predicted {
                let kind = match actual.get(twap_id) {
                    Some(filled) if filled == size => "predicted, filled the size",
                    Some(filled) if filled < size => "predicted, filled less",
                    Some(_) => "predicted, filled more",
                    None if refusals.contains(twap_id) => "predicted, refused",
                    None => "predicted, no fill",
                };
                let twap = &book.twaps[twap_id];
                let class = if twap.randomize { "randomized" } else if twap.floor.estimated.is_some() { "floor" } else { "plain" };
                *counts.entry(format!("{kind} / {class}")).or_default() += 1;
                let samples = samples.entry(format!("{kind} / {class}")).or_default();
                if samples.len() < 4 {
                    samples.push(format!(
                        "twap {twap_id} slot {} predicted {size} filled {:?} total {} executed {} slots {} origin {} block {} due {}",
                        slot_of[twap_id], actual.get(twap_id), twap.total, twap.executed, twap.slots, twap.origin_ms, next_ms, twap.due_ms(slot_of[twap_id])
                    ));
                }
            }
            for twap_id in actual.keys().filter(|twap_id| !predicted.contains_key(twap_id)) {
                let kind = if book.twaps.contains_key(twap_id) { "filled, not predicted (followed)" } else { "filled, not predicted (unknown TWAP)" };
                *counts.entry(kind.to_string()).or_default() += 1;
                if let Some(twap) = book.twaps.get(twap_id) {
                    let samples = samples.entry(kind.to_string()).or_default();
                    if samples.len() < 6 {
                        let slot = twap.slot_at(*next_ms);
                        samples.push(format!(
                            "twap {twap_id} filled {} slot_at {slot:?} due {:?} total {} executed {} slots {} origin {} block {} prev {} randomize {}",
                            actual[twap_id], slot.map(|slot| twap.due_ms(slot)), twap.total, twap.executed, twap.slots, twap.origin_ms, next_ms, time_ms, twap.randomize
                        ));
                    }
                }
            }
        }
        println!("replayed in {:.1} s", started.elapsed().as_secs_f64());
        for (kind, count) in counts {
            println!("{kind}: {count}");
        }
        for (kind, lines) in samples {
            for line in lines {
                println!("  {kind}: {line}");
            }
        }
    }
}
