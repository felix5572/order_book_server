//! The main dex's trailing stops as the node keeps them (batch 2n-1, hyperliquid-trade design 000659 r2-r4): the one
//! owner of each one's activation and best price, which the node moves at every mark it writes and prints only in the
//! stop's status lines - none while its best moves (research `mempool_pending_book_20261031` README "移动止损").
//!
//! - Seeded from the checkpoint the marks are rebuilt from: `perp_dexs[0].books[i].user_states[].U`'s `t` entries hold
//!   each one's state (bm 1205940000: 289; the node's dump of 11-01 22:48 differed from the stops' last lines in 111 of
//!   203, all toward the trail).
//! - Each block, in its execution order: an `open` line adds a stop as it states it; each mark a `SetGlobalAction`
//!   writes moves its coin's stops - a waiting one activates at a mark at or past its activation price (a sale's at or
//!   above, a buy's at or below), its best that mark; an active one's best is the highest mark since for a sale, the
//!   lowest for a buy (user 2026-11-02, nine bm samples; bm 11-02 01:05-02:05: 6 of 6 replays) -; the line a stop leaves
//!   the untriggered orders by is checked against it, then the stop is gone (review 000659 r3: the `filled` after a
//!   `triggered` states no best).
//! - Not known, until a checkpoint seeds it again: the stops of a coin an automatic refresh wrote (whether it moves a
//!   best is unproven; none on bm 10-31..11-02); every stop after a block whose price actions were not applied, and one
//!   opened then; a stop opened in a block that writes its coin's mark without its placement's rank; the stops of a coin
//!   whose mark the replay does not claim; one whose `open` line, or checkpoint entry, does not read; those a checkpoint
//!   the replay cannot rebuild from shows otherwise (a coin's all when the checkpoint holds one the replay does not).

use std::collections::BTreeMap;

use rustc_hash::{FxHashMap, FxHashSet};

use super::{actions::Rank, rules::wire_units};
use crate::{
    listeners::order_book::triggers::{Direction, RetracementText, TrailingTerms},
    order_book::Side,
    types::node_data::NodeDataOrderStatus,
};

/// Mismatch evidence kept per block and per install.
const MAX_SAMPLES: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Retracement {
    /// Thousandths of a percent, as a checkpoint stores it (`Pct` 2000 for 2%).
    Percent(u32),
    /// Price units.
    Price(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrailingState {
    Waiting,
    /// Price units.
    Active { best: u64 },
}

/// One trailing stop: its side, its rule in price units, and where it stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TrailingStop {
    pub(crate) side: Side,
    /// `None` for an immediate one.
    pub(crate) activation: Option<u64>,
    pub(crate) retracement: Retracement,
    pub(crate) state: TrailingState,
}

impl TrailingStop {
    /// The stop as the node holds it after writing `mark`.
    #[must_use]
    pub(crate) fn advanced(self, mark: u64) -> Self {
        let state = match (self.state, self.side) {
            (TrailingState::Waiting, _) if self.activates_at(mark) => TrailingState::Active { best: mark },
            (TrailingState::Waiting, _) => TrailingState::Waiting,
            (TrailingState::Active { best }, Side::Ask) => TrailingState::Active { best: best.max(mark) },
            (TrailingState::Active { best }, Side::Bid) => TrailingState::Active { best: best.min(mark) },
        };
        Self { state, ..self }
    }

    fn activates_at(&self, mark: u64) -> bool {
        self.activation.is_some_and(|activation| match self.side {
            Side::Ask => mark >= activation,
            Side::Bid => mark <= activation,
        })
    }

    /// The trigger price at `best`: a sale's `best` less the retracement, a buy's plus it, rounded half up to the price
    /// units (bm 11-01/02: 270 status lines); `None` below zero or past the units.
    pub(crate) fn trigger_px(&self, best: u64) -> Option<u64> {
        let best = u128::from(best);
        let raw = match (self.retracement, self.side) {
            (Retracement::Percent(pct), Side::Ask) => (best * u128::from(100_000_u32.checked_sub(pct)?) + 50_000) / 100_000,
            (Retracement::Percent(pct), Side::Bid) => (best * (100_000 + u128::from(pct)) + 50_000) / 100_000,
            (Retracement::Price(px), Side::Ask) => best.checked_sub(u128::from(px))?,
            (Retracement::Price(px), Side::Bid) => best + u128::from(px),
        };
        u64::try_from(raw).ok()
    }

    /// The stop a line states, in price units of `size_decimals`.
    fn of_line(line: &TrailingLine, size_decimals: u32) -> Result<Self, String> {
        Self::of_condition(line.side, &line.condition, size_decimals)
    }

    /// The stop a `triggerCondition` of a `side` states - a line's, a trigger table's order's -, in price units of
    /// `size_decimals`.
    pub(crate) fn of_condition(side: Side, condition: &str, size_decimals: u32) -> Result<Self, String> {
        let terms = TrailingTerms::parse(condition)?;
        let activation = terms.activation.map(|(direction, px)| activation_px(side, direction, px, size_decimals)).transpose()?;
        let retracement = match terms.retracement {
            RetracementText::Percent(text) => Retracement::Percent(percent_thousandths(text)?),
            RetracementText::Price(text) => Retracement::Price(wire_units(text, size_decimals)?),
        };
        let state = match (terms.best, activation) {
            (Some(best), _) => TrailingState::Active { best: wire_units(best, size_decimals)? },
            (None, Some(_)) => TrailingState::Waiting,
            (None, None) => return Err("an immediate stop waiting".to_string()),
        };
        Ok(Self { side, activation, retracement, state })
    }
}

fn activation_px(side: Side, direction: Direction, px: &str, size_decimals: u32) -> Result<u64, String> {
    if direction != activation_direction(side) {
        return Err(format!("a {side:?} activating on a {direction:?}"));
    }
    wire_units(px, size_decimals)
}

/// The way a stop's mark reaches its activation price: a sale's on a rise (`above`), a buy's on a fall (bm 11-02 00-02:
/// 64 of 64 sales, 23 of 23 buys).
const fn activation_direction(side: Side) -> Direction {
    match side {
        Side::Ask => Direction::Up,
        Side::Bid => Direction::Down,
    }
}

/// A percent as a line prints a checkpoint's thousandths (`0.33999999999999997` for 340): within 10^-6 of a whole
/// number of thousandths, or not one.
fn percent_thousandths(text: &str) -> Result<u32, String> {
    let malformed = || format!("retracement {text:?}% is no thousandths of a percent");
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    if whole.is_empty() || !whole.bytes().chain(fraction.bytes()).all(|byte| byte.is_ascii_digit()) {
        return Err(malformed());
    }
    let digits = format!("{fraction:0<9}");
    let (kept, rest) = digits.split_at(3);
    let floor: u32 = format!("{whole}{kept}").parse().map_err(|_| malformed())?;
    match &rest[..6] {
        "000000" => Ok(floor),
        "999999" => floor.checked_add(1).ok_or_else(malformed),
        _ => Err(malformed()),
    }
}

/// A trailing stop's status line, in the block's stream order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrailingLine {
    oid: u64,
    coin: Box<str>,
    side: Side,
    status: Box<str>,
    condition: Box<str>,
    trigger_px: Box<str>,
}

impl TrailingLine {
    /// The block's trailing stops' lines (an order type the node names `Trailing …`), in stream order.
    pub(crate) fn of_block(statuses: &[NodeDataOrderStatus]) -> Vec<Self> {
        let trailing = statuses.iter().filter(|status| status.order.order_type.starts_with("Trailing"));
        trailing
            .map(|status| Self {
                oid: status.order.oid,
                coin: status.order.coin.as_str().into(),
                side: status.order.side,
                status: status.status.as_str().into(),
                condition: status.order.trigger_condition.as_str().into(),
                trigger_px: status.order.trigger_px.as_str().into(),
            })
            .collect()
    }

    fn evidence(&self) -> String {
        format!("oid {} {} {} {:?} triggerPx {}", self.oid, self.coin, self.status, self.condition, self.trigger_px)
    }
}

/// A main-dex mark the node wrote at a `SetGlobalAction`'s rank; `None` for a coin whose mark the replay does not claim
/// there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MarkWrite {
    pub(crate) rank: Rank,
    pub(crate) asset: usize,
    pub(crate) mark: Option<u64>,
}

/// What one block brings the stops.
#[derive(Debug)]
pub(crate) struct TrailingBlock<'a> {
    pub(crate) lines: &'a [TrailingLine],
    /// In execution order.
    pub(crate) writes: &'a [MarkWrite],
    /// The stops the block's `trailingStop` actions placed, at their ranks.
    pub(crate) placements: &'a FxHashMap<u64, Rank>,
    /// Why each `trailingStop` receipt that did not read did not.
    pub(crate) unread_receipts: &'a [String],
    /// The assets an automatic refresh wrote at the block's start.
    pub(crate) refreshed: &'a [usize],
    /// The block's price actions were not applied (its actions broke the contract, or the prices or books are broken):
    /// its writes are not known.
    pub(crate) marks_unapplied: bool,
}

/// What a block's lines found, by [line, result], with samples of what was found otherwise.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct TrailingReport {
    pub(crate) lines: BTreeMap<[&'static str; 2], u64>,
    pub(crate) samples: Vec<String>,
}

impl TrailingReport {
    fn count(&mut self, line: &'static str, result: &'static str) {
        *self.lines.entry([line, result]).or_default() += 1;
    }

    fn sample(&mut self, evidence: impl FnOnce() -> String) {
        if self.samples.len() < MAX_SAMPLES {
            self.samples.push(evidence());
        }
    }

    /// A leaving line against the stop as tracked.
    fn check(&mut self, line: &TrailingLine, entry: &Entry, size_decimals: u32) {
        let result = leaving_check(line, entry, size_decimals);
        self.count("leaving", result);
        if result != "match" && result != "unknown" {
            self.sample(|| format!("{result}: {}, tracked {entry:?}", line.evidence()));
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TrailingCounts {
    pub(crate) waiting: usize,
    pub(crate) active: usize,
    pub(crate) unknown: usize,
}

/// A stop, or why it is not known.
pub(crate) type TrailingEntry = Result<TrailingStop, &'static str>;
type Entry = TrailingEntry;

fn leaving_check(line: &TrailingLine, entry: &Entry, size_decimals: u32) -> &'static str {
    let Ok(stop) = entry else { return "unknown" };
    let Ok(stated) = TrailingStop::of_line(line, size_decimals) else { return "unparsed" };
    if (stated.activation, stated.retracement) != (stop.activation, stop.retracement) {
        return "rule_mismatch";
    }
    if stated.state != stop.state {
        return "best_mismatch";
    }
    let TrailingState::Active { best } = stop.state else { return "match" };
    if wire_units(&line.trigger_px, size_decimals).ok() != stop.trigger_px(best) {
        return "trigger_mismatch";
    }
    "match"
}

/// Every main-dex trailing stop the node holds, by asset, then oid.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TrailingStops {
    by_asset: FxHashMap<usize, BTreeMap<u64, Entry>>,
}

impl TrailingStops {
    /// A checkpoint's stops, and its untriggered orders that did not read (a trailing stop's fields, or another kind):
    /// held as not known (review 000661 r1 P2-4).
    pub(crate) fn seeded(stops: &[(usize, u64, TrailingStop)], unread: &[(usize, u64, String)]) -> Self {
        let mut seeded = Self::default();
        for (asset, oid, stop) in stops {
            seeded.by_asset.entry(*asset).or_default().insert(*oid, Ok(*stop));
        }
        for (asset, oid, _) in unread {
            seeded.by_asset.entry(*asset).or_default().insert(*oid, Err("seed_unread"));
        }
        seeded
    }

    /// Applies one block; `coin` gives a main-dex coin's asset and size decimals.
    pub(crate) fn apply_block(&mut self, block: &TrailingBlock<'_>, coin: impl Fn(&str) -> Option<(usize, u32)>, report: &mut TrailingReport) {
        for asset in block.refreshed {
            self.forget_asset(*asset, "automatic_refresh");
        }
        if block.marks_unapplied {
            self.forget_all("marks_unapplied");
        }
        for err in block.unread_receipts {
            report.count("receipt", "unread");
            report.sample(|| format!("trailingStop receipt unread: {err}"));
        }
        let written: FxHashSet<usize> = block.writes.iter().map(|write| write.asset).collect();
        let mut placed_at: FxHashMap<u64, Rank> = FxHashMap::default();
        let mut leaving = Vec::new();
        for line in block.lines {
            let Some((asset, size_decimals)) = coin(&line.coin) else { continue };
            if &*line.status == "open" {
                let entry = if block.marks_unapplied {
                    report.count("open", "unapplied");
                    Err("marks_unapplied")
                } else {
                    opened(line, size_decimals, written.contains(&asset), block.placements, &mut placed_at, report)
                };
                self.by_asset.entry(asset).or_default().insert(line.oid, entry);
                continue;
            }
            let Some(entry) = self.by_asset.get(&asset).and_then(|stops| stops.get(&line.oid)).copied() else {
                // the lines after its leaving one (a `filled` after a `triggered`) name a stop no longer held
                if TrailingTerms::parse(&line.condition).is_ok() {
                    report.count("leaving", "untracked");
                    report.sample(|| format!("untracked: {}", line.evidence()));
                }
                continue;
            };
            if written.contains(&asset) {
                leaving.push((line, asset, size_decimals, entry));
                continue;
            }
            report.check(line, &entry, size_decimals);
            self.remove(asset, line.oid);
        }
        for write in block.writes {
            self.write(write, &placed_at);
        }
        for (line, asset, size_decimals, before) in leaving {
            let Some(after) = self.by_asset.get(&asset).and_then(|stops| stops.get(&line.oid)).copied() else { continue };
            let writes = block.writes.iter().filter(|write| write.asset == asset).count();
            // a stop fires at the write that crosses its trigger price, which moves no best
            if (&*line.status == "triggered" && writes == 1) || before == after {
                report.check(line, &after, size_decimals);
            } else {
                report.count("leaving", "unordered");
            }
            self.remove(asset, line.oid);
        }
    }

    /// The assets holding a stop.
    pub(crate) fn assets(&self) -> impl Iterator<Item = usize> + '_ {
        self.by_asset.keys().copied()
    }

    /// Every stop: (asset, oid, the stop or why it is not known).
    pub(crate) fn entries(&self) -> impl Iterator<Item = (usize, u64, Entry)> + '_ {
        self.by_asset.iter().flat_map(|(asset, stops)| stops.iter().map(|(oid, entry)| (*asset, *oid, *entry)))
    }

    /// One mark write moves its coin's stops but those placed after it in the block.
    fn write(&mut self, write: &MarkWrite, placed_at: &FxHashMap<u64, Rank>) {
        let Some(stops) = self.by_asset.get_mut(&write.asset) else { return };
        for (oid, entry) in stops.iter_mut() {
            if placed_at.get(oid).is_some_and(|placed| write.rank < *placed) {
                continue;
            }
            *entry = match (*entry, write.mark) {
                (Ok(stop), Some(mark)) => Ok(stop.advanced(mark)),
                (Ok(_), None) => Err("mark_unclaimed"),
                (unknown @ Err(_), _) => unknown,
            };
        }
    }

    fn remove(&mut self, asset: usize, oid: u64) {
        let Some(stops) = self.by_asset.get_mut(&asset) else { return };
        stops.remove(&oid);
        if stops.is_empty() {
            self.by_asset.remove(&asset);
        }
    }

    fn forget_all(&mut self, reason: &'static str) {
        for entry in self.by_asset.values_mut().flat_map(BTreeMap::values_mut) {
            *entry = Err(reason);
        }
    }

    fn forget_asset(&mut self, asset: usize, reason: &'static str) {
        for entry in self.by_asset.get_mut(&asset).into_iter().flat_map(BTreeMap::values_mut) {
            *entry = Err(reason);
        }
    }

    fn forget(&mut self, asset: usize, oid: u64, reason: &'static str) {
        if let Some(entry) = self.by_asset.get_mut(&asset).and_then(|stops| stops.get_mut(&oid)) {
            *entry = Err(reason);
        }
    }

    /// The stops of every coin whose mark `claimed` says the replay does not claim turn unknown.
    pub(crate) fn forget_unclaimed(&mut self, claimed: impl Fn(usize) -> bool) {
        let unclaimed: Vec<usize> = self.assets().filter(|asset| !claimed(*asset)).collect();
        for asset in unclaimed {
            self.forget_asset(asset, "mark_unclaimed");
        }
    }

    /// A checkpoint the replay could not rebuild from: the stops it shows otherwise turn unknown; a stop it holds the
    /// replay does not is held as not known - it may have left since, so never as known (review 000661 r1 P2-4, r2
    /// P2: its coin's set is not complete, its line no stranger) -, and every other stop of its coin turns unknown.
    pub(crate) fn withdraw(&mut self, check: &TrailingCheck) {
        for (asset, oid) in check.differ.iter().chain(&check.extra) {
            self.forget(*asset, *oid, "anchor_mismatch");
        }
        for (asset, oid) in &check.missing {
            self.forget_asset(*asset, "anchor_mismatch");
            self.by_asset.entry(*asset).or_default().insert(*oid, Err("anchor_missing"));
        }
    }

    pub(crate) fn counts(&self) -> TrailingCounts {
        let mut counts = TrailingCounts::default();
        for entry in self.by_asset.values().flat_map(BTreeMap::values) {
            match entry {
                Ok(TrailingStop { state: TrailingState::Waiting, .. }) => counts.waiting += 1,
                Ok(TrailingStop { state: TrailingState::Active { .. }, .. }) => counts.active += 1,
                Err(_) => counts.unknown += 1,
            }
        }
        counts
    }

    /// Stop by stop against a checkpoint's at the same height.
    pub(crate) fn check_against(&self, checkpoint: &Self) -> TrailingCheck {
        let mut check = TrailingCheck::default();
        for (asset, stops) in &checkpoint.by_asset {
            for (oid, stored) in stops {
                match self.by_asset.get(asset).and_then(|ours| ours.get(oid)) {
                    None => check.missing.push((*asset, *oid)),
                    Some(Err(_)) => check.unknown += 1,
                    Some(_) if stored.is_err() => check.unknown += 1,
                    Some(ours) if ours == stored => check.equal += 1,
                    Some(ours) => {
                        check.differ.push((*asset, *oid));
                        if check.samples.len() < MAX_SAMPLES {
                            check.samples.push(format!("asset {asset} oid {oid}: replayed {ours:?}, checkpoint {stored:?}"));
                        }
                    }
                }
            }
        }
        let held = |asset: &usize, oid: &u64| checkpoint.by_asset.get(asset).is_some_and(|stops| stops.contains_key(oid));
        let ours = self.by_asset.iter().flat_map(|(asset, stops)| stops.keys().map(move |oid| (*asset, *oid)));
        check.extra = ours.filter(|(asset, oid)| !held(asset, oid)).collect();
        check
    }
}

/// An `open` line's stop: as it states it, unless the line does not read, or the block writes its coin's mark with no
/// rank for its placement.
fn opened(line: &TrailingLine, size_decimals: u32, written: bool, placements: &FxHashMap<u64, Rank>, placed_at: &mut FxHashMap<u64, Rank>,
          report: &mut TrailingReport) -> Entry {
    let stop = match TrailingStop::of_line(line, size_decimals) {
        Ok(stop) => stop,
        Err(err) => {
            report.count("open", "unparsed");
            report.sample(|| format!("unparsed: {}: {err}", line.evidence()));
            return Err("line_unparsed");
        }
    };
    if !written {
        report.count("open", "added");
        return Ok(stop);
    }
    let Some(rank) = placements.get(&line.oid) else {
        report.count("open", "unordered");
        return Err("placement_unordered");
    };
    placed_at.insert(line.oid, *rank);
    report.count("open", "added");
    Ok(stop)
}

/// The replayed stops against a checkpoint's at its height.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct TrailingCheck {
    pub(crate) equal: u64,
    /// (asset, oid) of each stop whose state or rule differs.
    pub(crate) differ: Vec<(usize, u64)>,
    /// (asset, oid) of the checkpoint's stops the replay does not hold, and of the replay's the checkpoint does not.
    pub(crate) missing: Vec<(usize, u64)>,
    pub(crate) extra: Vec<(usize, u64)>,
    /// The checkpoint's stops either side holds as not known.
    pub(crate) unknown: u64,
    pub(crate) samples: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// BTC's units: one decimal (szDecimals 5).
    const BTC: (usize, u32) = (0, 5);

    fn sale(best: u64, retracement: Retracement) -> TrailingStop {
        TrailingStop { side: Side::Ask, activation: None, retracement, state: TrailingState::Active { best } }
    }

    fn line(oid: u64, status: &str, condition: &str, trigger_px: &str) -> TrailingLine {
        TrailingLine { oid, coin: "BTC".into(), side: Side::Ask, status: status.into(), condition: condition.into(), trigger_px: trigger_px.into() }
    }

    fn write(rank: Rank, mark: u64) -> MarkWrite {
        MarkWrite { rank, asset: BTC.0, mark: Some(mark) }
    }

    fn coin(name: &str) -> Option<(usize, u32)> {
        (name == "BTC").then_some(BTC)
    }

    /// Applies a block of these lines and writes, its placements those given.
    fn apply(stops: &mut TrailingStops, lines: &[TrailingLine], writes: &[MarkWrite], placements: &[(u64, Rank)]) -> TrailingReport {
        let placements: FxHashMap<u64, Rank> = placements.iter().copied().collect();
        let block = TrailingBlock { lines, writes, placements: &placements, unread_receipts: &[], refreshed: &[], marks_unapplied: false };
        let mut report = TrailingReport::default();
        stops.apply_block(&block, coin, &mut report);
        report
    }

    fn held(stops: &TrailingStops, oid: u64) -> Option<Entry> {
        stops.by_asset.get(&BTC.0).and_then(|held| held.get(&oid)).copied()
    }

    fn results(report: &TrailingReport) -> Vec<(&'static str, &'static str, u64)> {
        report.lines.iter().map(|([line, result], count)| (*line, *result, *count)).collect()
    }

    /// User 2026-11-02: a waiting stop activates at a mark at or past its price - a sale's at or above, a buy's at or
    /// below -, its best that mark, one past the price at once included; then the best trails.
    #[test]
    fn a_stop_activates_at_or_past_its_price_and_its_best_trails() {
        let waiting_sale = TrailingStop { activation: Some(1_000), state: TrailingState::Waiting, ..sale(0, Retracement::Price(5)) };
        assert_eq!(waiting_sale.advanced(999).state, TrailingState::Waiting);
        assert_eq!(waiting_sale.advanced(1_000).state, TrailingState::Active { best: 1_000 });
        assert_eq!(waiting_sale.advanced(1_007).advanced(1_003).state, TrailingState::Active { best: 1_007 });
        let waiting_buy = TrailingStop { side: Side::Bid, ..waiting_sale };
        assert_eq!(waiting_buy.advanced(1_001).state, TrailingState::Waiting);
        assert_eq!(waiting_buy.advanced(950).advanced(960).advanced(940).state, TrailingState::Active { best: 940 });
    }

    /// bm lines: BTC 591778253588's 0.34% buy at best 77611.8 triggers at 77875.7 (half up from 77875.68); a sale less.
    #[test]
    fn the_trigger_price_rounds_half_up_to_the_price_units() {
        let buy = TrailingStop { side: Side::Bid, ..sale(776_118, Retracement::Percent(340)) };
        assert_eq!(buy.trigger_px(776_118), Some(778_757));
        assert_eq!(sale(828_700, Retracement::Percent(2_000)).trigger_px(828_700), Some(812_126));
        assert_eq!(sale(100, Retracement::Percent(12_500)).trigger_px(100), Some(88), "12.5% of 100 off: 87.5 rounds up");
        assert_eq!(sale(100, Retracement::Price(5)).trigger_px(110), Some(105));
        assert_eq!(sale(3, Retracement::Price(5)).trigger_px(3), None);
    }

    #[test]
    fn a_printed_percent_reads_into_thousandths() {
        assert_eq!(percent_thousandths("0.33999999999999997"), Ok(340));
        assert_eq!(percent_thousandths("0.15"), Ok(150));
        assert_eq!(percent_thousandths("2"), Ok(2_000));
        assert_eq!(percent_thousandths("0.6000"), Ok(600));
        for malformed in ["0.1234567", "", "1e-3", "-1", "0.34%"] {
            assert!(percent_thousandths(malformed).is_err(), "{malformed}");
        }
    }

    /// Review 000659 r1 P2-1: best 100, retracement 5, marks 110 and 107 in separate blocks, then 104 - the stop the
    /// node fires at 104 (trigger 105) leaves by a line the tracked stop matches.
    #[test]
    fn writes_over_blocks_keep_the_extreme_and_the_leaving_line_matches() {
        let mut stops = TrailingStops::seeded(&[(BTC.0, 7, sale(1_000, Retracement::Price(50)))], &[]);
        apply(&mut stops, &[], &[write((0, 3, 0), 1_100)], &[]);
        apply(&mut stops, &[], &[write((0, 3, 0), 1_070)], &[]);
        assert_eq!(held(&stops, 7), Some(Ok(sale(1_100, Retracement::Price(50)))));
        let fired = line(7, "triggered", "Activation immediate, retracement 5, best 110", "105.0");
        let report = apply(&mut stops, &[fired], &[write((0, 3, 0), 1_040)], &[]);
        assert_eq!((results(&report), held(&stops, 7)), (vec![("leaving", "match", 1)], None));
    }

    /// Review 000659 r3 P2-1 (bm 1205810990, oid 592186048229): a `triggered` line with its best, then the same
    /// block's `filled` saying `Triggered` - one check, the stop gone, nothing untracked.
    #[test]
    fn only_the_line_a_stop_leaves_by_is_checked() {
        let mut stops = TrailingStops::seeded(&[(BTC.0, 7, sale(1_100, Retracement::Price(50)))], &[]);
        let lines = [line(7, "triggered", "Activation immediate, retracement 5, best 110", "105.0"), line(7, "filled", "Triggered", "105.0")];
        let report = apply(&mut stops, &lines, &[], &[]);
        assert_eq!((results(&report), held(&stops, 7), report.samples.len()), (vec![("leaving", "match", 1)], None, 0));
    }

    /// A stop opened in a block that writes its coin's mark moves by the writes after its placement only; with no rank
    /// for its placement it is not known.
    #[test]
    fn a_stop_opened_beside_a_write_moves_by_the_writes_after_it() {
        let opened = line(8, "open", "Activation immediate, retracement 5, best 100", "95.0");
        let mut before = TrailingStops::default();
        apply(&mut before, std::slice::from_ref(&opened), &[write((1, 3, 0), 1_200)], &[(8, (2, 3, 0))]);
        assert_eq!(held(&before, 8), Some(Ok(sale(1_000, Retracement::Price(50)))), "the write ran before it");
        let mut after = TrailingStops::default();
        apply(&mut after, std::slice::from_ref(&opened), &[write((3, 3, 0), 1_200)], &[(8, (2, 3, 0))]);
        assert_eq!(held(&after, 8), Some(Ok(sale(1_200, Retracement::Price(50)))));
        let mut unranked = TrailingStops::default();
        let report = apply(&mut unranked, &[opened], &[write((3, 3, 0), 1_200)], &[]);
        assert_eq!((held(&unranked, 8), results(&report)), (Some(Err("placement_unordered")), vec![("open", "unordered", 1)]));
    }

    /// A stop leaving in a block that writes its coin's mark is checked when the write left it as it was, or fired it
    /// (a `triggered` at the one write); otherwise its line's place is not known.
    #[test]
    fn a_line_beside_a_write_that_moved_its_stop_is_unordered() {
        let cancelled = |best| line(7, "canceled", &format!("Activation immediate, retracement 5, best {best}"), "0");
        let mut unmoved = TrailingStops::seeded(&[(BTC.0, 7, sale(1_100, Retracement::Price(50)))], &[]);
        let report = apply(&mut unmoved, &[cancelled("110")], &[write((0, 3, 0), 1_050)], &[]);
        assert_eq!(results(&report), [("leaving", "trigger_mismatch", 1)], "checked: the line's trigger price 0 is not 105");
        let mut moved = TrailingStops::seeded(&[(BTC.0, 7, sale(1_100, Retracement::Price(50)))], &[]);
        let report = apply(&mut moved, &[cancelled("110")], &[write((0, 3, 0), 1_150)], &[]);
        assert_eq!((results(&report), held(&moved, 7)), (vec![("leaving", "unordered", 1)], None));
    }

    /// A line stating another best is a mismatch, kept as evidence; an automatic refresh leaves the stops of the coins
    /// it wrote not known, those of others as they were (review 000661 r1 P2-3); an unread receipt is counted.
    #[test]
    fn mismatches_refreshes_and_unread_receipts_are_reported() {
        let mut stops = TrailingStops::seeded(&[(BTC.0, 7, sale(1_100, Retracement::Price(50))), (BTC.0, 9, sale(1_000, Retracement::Price(50)))], &[]);
        let report = apply(&mut stops, &[line(7, "canceled", "Activation immediate, retracement 5, best 111", "106.0")], &[], &[]);
        assert_eq!((results(&report), report.samples.len()), (vec![("leaving", "best_mismatch", 1)], 1));
        let placements = FxHashMap::default();
        let unread = ["trailingStop receipt: no oid".to_string()];
        let refresh = |stops: &mut TrailingStops, refreshed: &[usize]| {
            let block = TrailingBlock { lines: &[], writes: &[], placements: &placements, unread_receipts: &unread, refreshed, marks_unapplied: false };
            let mut report = TrailingReport::default();
            stops.apply_block(&block, coin, &mut report);
            report
        };
        refresh(&mut stops, &[3]);
        assert_eq!(held(&stops, 9), Some(Ok(sale(1_000, Retracement::Price(50)))), "another coin refreshed");
        let report = refresh(&mut stops, &[BTC.0]);
        assert_eq!((held(&stops, 9), results(&report)), (Some(Err("automatic_refresh")), vec![("receipt", "unread", 1)]));
        let report = apply(&mut stops, &[line(9, "canceled", "Activation immediate, retracement 5, best 100", "95.0")], &[], &[]);
        assert_eq!(results(&report), [("leaving", "unknown", 1)]);
        assert_eq!(stops.counts(), TrailingCounts::default(), "both gone");
    }

    /// The replay's stops against a checkpoint's: equal, differing, missing, extra and not known each counted.
    #[test]
    fn a_checkpoint_checks_each_stop() {
        let mut ours = TrailingStops::seeded(&[(0, 1, sale(1_000, Retracement::Price(5))), (0, 2, sale(1_010, Retracement::Price(5))),
            (0, 3, sale(1_000, Retracement::Price(5))), (0, 4, sale(1_000, Retracement::Price(5)))], &[]);
        ours.forget(0, 3, "automatic_refresh");
        let checkpoint = TrailingStops::seeded(&[(0, 1, sale(1_000, Retracement::Price(5))), (0, 2, sale(1_020, Retracement::Price(5))),
            (0, 3, sale(1_000, Retracement::Price(5))), (0, 5, sale(1_000, Retracement::Price(5)))], &[]);
        let check = ours.check_against(&checkpoint);
        assert_eq!((check.equal, check.differ.clone(), check.missing.clone(), check.extra.clone(), check.unknown, check.samples.len()),
            (1, vec![(0, 2)], vec![(0, 5)], vec![(0, 4)], 1, 1));
        // a checkpoint the replay could not rebuild from: the differing and the extra stop, and the coin missing one
        let mut other_coin = TrailingStops::seeded(&[(1, 21, sale(1_000, Retracement::Price(5)))], &[]);
        other_coin.withdraw(&TrailingCheck { differ: vec![(0, 2)], extra: vec![(0, 4)], ..TrailingCheck::default() });
        assert_eq!(other_coin.counts(), TrailingCounts { active: 1, ..TrailingCounts::default() }, "another coin as it was");
        ours.withdraw(&TrailingCheck { differ: vec![(0, 2)], extra: vec![(0, 4)], ..TrailingCheck::default() });
        assert_eq!(ours.counts(), TrailingCounts { active: 1, unknown: 3, ..TrailingCounts::default() });
        ours.withdraw(&TrailingCheck { missing: vec![(0, 5)], ..TrailingCheck::default() });
        assert_eq!(ours.counts(), TrailingCounts { unknown: 5, ..TrailingCounts::default() });
        // review 000661 r2 P2: a coin holding none, the checkpoint one - held as not known, its line no stranger
        let mut empty = TrailingStops::default();
        empty.withdraw(&TrailingCheck { missing: vec![(BTC.0, 7)], ..TrailingCheck::default() });
        assert_eq!(held(&empty, 7), Some(Err("anchor_missing")));
        let report = apply(&mut empty, &[line(7, "canceled", "Activation immediate, retracement 5, best 100", "95.0")], &[], &[]);
        assert_eq!((results(&report), held(&empty, 7)), (vec![("leaving", "unknown", 1)], None));
    }

    /// Review 000661 r2 P2: a write withdrawing the coin's mark at its rank leaves a line after it unordered, not
    /// checked against the best from before; the stop leaves all the same.
    #[test]
    fn a_line_after_a_withdrawn_mark_is_unordered() {
        let mut stops = TrailingStops::seeded(&[(BTC.0, 7, sale(1_100, Retracement::Price(50)))], &[]);
        let withdrawn = MarkWrite { rank: (0, 3, 0), asset: BTC.0, mark: None };
        let report = apply(&mut stops, &[line(7, "canceled", "Activation immediate, retracement 5, best 110", "105.0")], &[withdrawn], &[]);
        assert_eq!((results(&report), held(&stops, 7)), (vec![("leaving", "unordered", 1)], None));
    }

    /// Review 000661 r1 P2-2: a block whose price actions were not applied leaves the stops it opens not known too.
    #[test]
    fn a_stop_opened_in_a_block_not_applied_is_not_known() {
        let mut stops = TrailingStops::default();
        let placements = FxHashMap::default();
        let opened = [line(8, "open", "Activation immediate, retracement 5, best 100", "95.0")];
        let block = TrailingBlock { lines: &opened, writes: &[], placements: &placements, unread_receipts: &[], refreshed: &[], marks_unapplied: true };
        let mut report = TrailingReport::default();
        stops.apply_block(&block, coin, &mut report);
        assert_eq!((held(&stops, 8), results(&report)), (Some(Err("marks_unapplied")), vec![("open", "unapplied", 1)]));
    }

    /// Review 000661 r1 P2-3, P2-4: a coin whose mark the replay does not claim leaves its stops not known; a checkpoint
    /// entry that did not read is held, not known, so its leaving line is no stranger.
    #[test]
    fn unclaimed_marks_and_unread_entries_are_held_not_known() {
        let mut stops = TrailingStops::seeded(&[(BTC.0, 7, sale(1_100, Retracement::Price(50))), (1, 21, sale(1_000, Retracement::Price(5)))],
            &[(BTC.0, 9, "a trailing stop: unknown variant `Bps`".to_string())]);
        stops.forget_unclaimed(|asset| asset != BTC.0);
        assert_eq!(held(&stops, 7), Some(Err("mark_unclaimed")));
        assert_eq!(stops.counts(), TrailingCounts { active: 1, unknown: 2, ..TrailingCounts::default() });
        let report = apply(&mut stops, &[line(9, "canceled", "Activation immediate, retracement 5, best 100", "95.0")], &[], &[]);
        assert_eq!((results(&report), held(&stops, 9)), (vec![("leaving", "unknown", 1)], None));
    }
}
