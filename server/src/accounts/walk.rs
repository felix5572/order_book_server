//! Static book walks of the liquidation candidates (design 000443 r1 section 3, r2 revisions 3–4, r3 revisions 1, 2,
//! 4). The candidates of one coin and side walk the public book together: ordered by when the mark reaches them (a
//! long's sale first at the highest liquidation price, a short's buy at the lowest; equal prices by address), each
//! takes its walk size from where the ones before it stopped. Each also walks its size alone from the best level (the
//! book as if no other candidate sold first). Static: no cascade, no refill. The forced flow (design 000451 r4
//! revision 7) walks each entry alone only, up to its limit price (`alone_walks`), and each band's cumulative size from
//! the best level (`cumulative_walks`, user 2026-10-25).
//!
//! The account owner sends a request per (coin, side) whose ordered sizes changed; a mailbox keeps only the newest of
//! each. The walk task copies the side's levels under the listener lock (`PriceLevel::total_sz`, at most
//! `MAX_WALK_LEVELS`), allocates them outside it, and returns the shares with the book's progress, trust and withheld
//! periods; the owner takes a result only while the book is trusted in the same period and the request is the newest.

use std::{future::Future, sync::Arc, time::Instant};

use alloy::primitives::Address;
use log::error;
use prometheus::Histogram;
use tokio::sync::mpsc;

use crate::{
    listeners::order_book::{OrderBookListener, TakerLevels},
    mailbox::Mailbox,
    metrics::{LIQUIDATION_WALK_LOCK_HOLD_SECONDS, LIQUIDATION_WALK_LOCK_WAIT_SECONDS},
    order_book::{Coin, Px, Side, Sz},
};

/// Levels copied per request at most; a walk that does not fill within them is `TruncatedAtLevels`.
pub(crate) const MAX_WALK_LEVELS: usize = 2_000;
/// A forced flow walk's steps published at most (design 000486): its first levels; `levels` stays the whole count.
pub(crate) const MAX_PUBLISHED_STEPS: usize = 100;
/// The listener's fixed-point unit: sizes and prices are integers of 1e-8.
const FIXED_POINT_DECIMALS: u32 = 8;

/// A walk's key: the coin and the liquidation orders' side (`Ask` sells longs into the bids, `Bid` buys shorts back).
pub(crate) type WalkKey = (String, Side);

/// The candidates of one key in walk order, as requested.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WalkRequest {
    pub(crate) version: u64,
    /// (user, walk size in 1e-8 units, cooldown unknown).
    pub(crate) sequence: Vec<(Address, u64, bool)>,
}

/// How a candidate's share of the walk ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WalkEnd {
    Filled,
    /// The next level is beyond the order's limit price.
    LimitReached,
    /// The book had no more levels.
    BookExhausted,
    /// The copied levels ran out at `MAX_WALK_LEVELS`: the book may hold more.
    TruncatedAtLevels,
}

impl WalkEnd {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Filled => "filled",
            Self::LimitReached => "limitReached",
            Self::BookExhausted => "bookExhausted",
            Self::TruncatedAtLevels => "truncatedAtLevels",
        }
    }
}

/// The worst price a taker order takes: a sale takes the bids down to it, a buy the asks up to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Limit {
    pub(crate) taker: Side,
    pub(crate) px: Px,
}

impl Limit {
    fn admits(self, px: Px) -> bool {
        match self.taker {
            Side::Ask => px >= self.px,
            Side::Bid => px <= self.px,
        }
    }
}

/// One walk of a size over the levels; sizes in 1e-8 units.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WalkPart {
    pub(crate) filled: u64,
    /// Size-weighted price of what it filled; `None` when nothing.
    pub(crate) vwap_px: Option<f64>,
    /// The first level it took (from where its cursor stood); `None` when nothing.
    pub(crate) first_px: Option<Px>,
    /// The last level it reached; `None` when nothing.
    pub(crate) deepest_px: Option<Px>,
    /// Levels it touched.
    pub(crate) levels: usize,
    pub(crate) end: WalkEnd,
    /// The levels it took, best first, when the walk records them (the forced flow's, at most `MAX_PUBLISHED_STEPS`;
    /// design 000486); empty otherwise.
    pub(crate) steps: Vec<WalkStep>,
}

/// One level a walk took; sizes in 1e-8 units.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WalkStep {
    pub(crate) px: Px,
    /// The level's whole size in the copy the walk read.
    pub(crate) level_sz: u64,
    /// What this walk takes of it.
    pub(crate) taken: u64,
}

/// One candidate's walks; sizes in 1e-8 units.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WalkShare {
    /// What the candidates before it take.
    pub(crate) ahead: u64,
    /// Its share of the cumulative walk, from where the candidates before it stopped.
    pub(crate) together: WalkPart,
    /// Its size walked alone from the best level.
    pub(crate) alone: WalkPart,
}

/// The shares of one request, with the book they were read from.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WalkResult {
    pub(crate) key: WalkKey,
    pub(crate) version: u64,
    pub(crate) book_height: u64,
    pub(crate) book_time: u64,
    pub(crate) trusted: bool,
    pub(crate) withheld_periods: u64,
    /// The book's mid at the copy (the walked side's best and the taker side's own best); `None` with an empty side.
    pub(crate) mid_px: Option<f64>,
    /// In the request's order.
    pub(crate) shares: Vec<WalkShare>,
}

/// The walks of `sizes` (in order) over `levels` (best first), each cumulative and alone; `truncated` when the levels
/// stop at the copy limit.
pub(crate) fn allocate(levels: &[(Px, Sz)], sizes: &[u64], truncated: bool) -> Vec<WalkShare> {
    let mut together = LevelCursor::default();
    let mut ahead = 0_u64;
    let mut shares = Vec::with_capacity(sizes.len());
    for &size in sizes {
        let share = WalkShare {
            ahead,
            together: together.take(levels, size, None, truncated, None),
            alone: LevelCursor::default().take(levels, size, None, truncated, None),
        };
        shares.push(share);
        ahead += size;
    }
    shares
}

/// Each item's size walked alone from the best level, up to its limit when it has one (a trigger order's; a liquidation
/// takes any price), with its first `MAX_PUBLISHED_STEPS` steps; `truncated` when the levels stop at the copy limit.
pub(crate) fn alone_walks(levels: &[(Px, Sz)], items: &[(u64, Option<Limit>)], truncated: bool) -> Vec<WalkPart> {
    items.iter().map(|&(size, limit)| LevelCursor::default().take(levels, size, limit, truncated, Some(MAX_PUBLISHED_STEPS))).collect()
}

/// Each size walked from the best level without a limit or steps: a forced flow band's or entry's market trigger orders
/// and liquidations together (`forced_flow::cumulative_sizes`); `truncated` when the levels stop at the copy limit.
/// One pass over the levels for every size (review 000490 P3: an entry's prefix each makes a size), so `sizes` come
/// ascending; each result is `LevelCursor::take` of that size from the best level, to the bit.
pub(crate) fn cumulative_walks(levels: &[(Px, Sz)], sizes: &[u64], truncated: bool) -> Vec<WalkPart> {
    assert!(sizes.is_sorted(), "cumulative sizes are walked in ascending order");
    // the levels wholly within the current size, their size and notional, accumulated as `take` does
    let (mut passed, mut held, mut notional) = (0_usize, 0_u64, 0.0_f64);
    let mut walks = Vec::with_capacity(sizes.len());
    for &size in sizes {
        while passed < levels.len() && held + levels[passed].1.value() < size {
            let (px, level_sz) = levels[passed];
            held += level_sz.value();
            notional = (px.value() as f64).mul_add(level_sz.value() as f64, notional);
            passed += 1;
        }
        walks.push(match levels.get(passed) {
            _ if size == 0 => WalkPart { filled: 0, vwap_px: None, first_px: None, deepest_px: None, levels: 0, end: WalkEnd::Filled, steps: Vec::new() },
            Some(&(px, _)) => {
                let total = (px.value() as f64).mul_add((size - held) as f64, notional);
                let vwap_px = Some(total / size as f64 / fixed_point_scale());
                WalkPart { filled: size, vwap_px, first_px: Some(levels[0].0), deepest_px: Some(px), levels: passed + 1, end: WalkEnd::Filled, steps: Vec::new() }
            }
            None => WalkPart {
                filled: held,
                vwap_px: (held > 0).then(|| notional / held as f64 / fixed_point_scale()),
                first_px: levels.first().map(|level| level.0),
                deepest_px: levels.last().map(|level| level.0),
                levels: levels.len(),
                end: if truncated { WalkEnd::TruncatedAtLevels } else { WalkEnd::BookExhausted },
                steps: Vec::new(),
            },
        });
    }
    walks
}

/// Where a walk stands in the levels: the level and what of it is already taken.
#[derive(Debug, Default)]
struct LevelCursor {
    level: usize,
    used_in_level: u64,
}

impl LevelCursor {
    /// Takes `size` from here on, no level beyond `limit`, recording its first `record_steps` steps when given.
    fn take(&mut self, levels: &[(Px, Sz)], size: u64, limit: Option<Limit>, truncated: bool, record_steps: Option<usize>) -> WalkPart {
        let (mut filled, mut notional, mut touched, mut first, mut deepest) = (0_u64, 0.0_f64, 0, None, None);
        let mut steps = Vec::new();
        let mut limit_reached = false;
        while filled < size && self.level < levels.len() {
            let (px, level_sz) = levels[self.level];
            if limit.is_some_and(|limit| !limit.admits(px)) {
                limit_reached = true;
                break;
            }
            let take = (level_sz.value() - self.used_in_level).min(size - filled);
            filled += take;
            notional = (px.value() as f64).mul_add(take as f64, notional);
            touched += 1;
            first = first.or(Some(px));
            deepest = Some(px);
            if record_steps.is_some_and(|cap| steps.len() < cap) {
                steps.push(WalkStep { px, level_sz: level_sz.value(), taken: take });
            }
            self.used_in_level += take;
            if self.used_in_level == level_sz.value() {
                self.level += 1;
                self.used_in_level = 0;
            }
        }
        let end = if filled == size {
            WalkEnd::Filled
        } else if limit_reached {
            WalkEnd::LimitReached
        } else if truncated {
            WalkEnd::TruncatedAtLevels
        } else {
            WalkEnd::BookExhausted
        };
        let vwap_px = (filled > 0).then(|| notional / filled as f64 / fixed_point_scale());
        WalkPart { filled, vwap_px, first_px: first, deepest_px: deepest, levels: touched, end, steps }
    }
}

/// 10^8: a listener price or size integer per unit.
fn fixed_point_scale() -> f64 {
    10f64.powi(FIXED_POINT_DECIMALS.cast_signed())
}

/// A candidate's walk size in the listener's units.
pub(crate) const fn walk_units(raw: u64, size_decimals: u32) -> u64 {
    #[expect(clippy::expect_used, reason = "a position in 1e-8 units fits u64 (the listener's own unit)")]
    let units = raw.checked_mul(10u64.pow(FIXED_POINT_DECIMALS - size_decimals)).expect("a position size in 1e-8 units");
    units
}

/// The walk task's mailbox.
pub(crate) type WalkMailbox = Mailbox<WalkKey, WalkRequest>;


/// The histograms a reader times its copies under the listener lock with: waiting for the lock, holding it.
#[derive(Clone)]
pub(crate) struct LockTimers {
    pub(crate) wait: Histogram,
    pub(crate) hold: Histogram,
}

impl LockTimers {
    /// The liquidation candidates' book walks (design 000443 r2 revision 4 acceptance).
    fn liquidation_walks() -> Self {
        Self { wait: LIQUIDATION_WALK_LOCK_WAIT_SECONDS.clone(), hold: LIQUIDATION_WALK_LOCK_HOLD_SECONDS.clone() }
    }
}

/// `copy` under the listener lock, the wait and the hold timed.
pub(crate) async fn under_lock<T: Send>(
    listener: &tokio::sync::Mutex<OrderBookListener>,
    timers: &LockTimers,
    copy: impl FnOnce(&OrderBookListener) -> T + Send,
) -> T {
    let waited = Instant::now();
    let guard = listener.lock().await;
    let locked = Instant::now();
    timers.wait.observe(locked.duration_since(waited).as_secs_f64());
    let copied = copy(&guard);
    drop(guard);
    timers.hold.observe(locked.elapsed().as_secs_f64());
    copied
}

/// Where a walk reads its levels.
pub(crate) trait LevelSource: Send + Sync + 'static {
    fn taker_levels(&self, coin: &str, taker: Side, needed: Sz, max_levels: usize, timers: &LockTimers) -> impl Future<Output = Option<TakerLevels>> + Send;
}

impl LevelSource for Arc<tokio::sync::Mutex<OrderBookListener>> {
    async fn taker_levels(&self, coin: &str, taker: Side, needed: Sz, max_levels: usize, timers: &LockTimers) -> Option<TakerLevels> {
        // the lock is held for the level copy only
        under_lock(self, timers, |listener| listener.taker_levels(&Coin::new(coin), taker, needed, max_levels)).await
    }
}

/// The walk task: every pending request, its levels copied under the source's lock and allocated outside it. It ends
/// when the owner drops the results' receiver.
pub(crate) async fn run_walker(source: impl LevelSource, mailbox: Arc<WalkMailbox>, results: mpsc::Sender<WalkResult>) {
    let timers = LockTimers::liquidation_walks();
    loop {
        for (key, request) in mailbox.take().await {
            let Some(result) = walk(&source, &timers, key, request).await else { continue };
            if results.send(result).await.is_err() {
                error!("liquidation candidates: the account owner is gone; the book walks stop");
                return;
            }
        }
    }
}

/// The copied levels stop at the copy limit before holding `needed`: the book may hold more.
pub(crate) fn levels_truncated(levels: &TakerLevels, needed: Sz) -> bool {
    let held: u64 = levels.levels.iter().map(|(_, sz)| sz.value()).sum();
    levels.levels.len() == MAX_WALK_LEVELS && held < needed.value()
}

/// The book's mid at the copy (the walked side's best and the taker side's own best); `None` with an empty side.
pub(crate) fn mid_px(levels: &TakerLevels) -> Option<f64> {
    #[expect(clippy::cast_precision_loss, reason = "prices are below 2^53 in 1e-8 units")]
    let mid = levels.levels.first().zip(levels.own_side_best).map(|((best, _), own)| f64::midpoint(best.value() as f64, own.value() as f64) / fixed_point_scale());
    mid
}

/// One request; `None` when the coin has no book (outside the market filter, or before the first snapshot).
async fn walk(source: &impl LevelSource, timers: &LockTimers, key: WalkKey, request: WalkRequest) -> Option<WalkResult> {
    let sizes: Vec<u64> = request.sequence.iter().map(|(_, size, _)| *size).collect();
    let needed = Sz::new(sizes.iter().sum());
    let levels = source.taker_levels(&key.0, key.1, needed, MAX_WALK_LEVELS, timers).await?;
    let truncated = levels_truncated(&levels, needed);
    Some(WalkResult {
        shares: allocate(&levels.levels, &sizes, truncated),
        mid_px: mid_px(&levels),
        key,
        version: request.version,
        book_height: levels.height,
        book_time: levels.time,
        trusted: levels.trusted,
        withheld_periods: levels.withheld_periods,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn level(px: u64, sz: u64) -> (Px, Sz) {
        (Px::new(px * 100_000_000), Sz::new(sz * 100_000_000))
    }

    /// Design 000451 r4 revision 7: a stop's sale takes no bid below its limit, a stop's buy no ask above it; a
    /// liquidation takes any; a limit before the best level fills nothing.
    #[test]
    fn an_alone_walk_stops_at_its_limit() {
        let unit = 100_000_000;
        let bids = [level(99, 1), level(98, 1), level(97, 1)];
        let at_98 = Some(Limit { taker: Side::Ask, px: Px::new(98 * unit) });
        let sales = alone_walks(&bids, &[(3 * unit, at_98), (3 * unit, None), (unit, at_98)], false);
        assert_eq!((sales[0].filled, sales[0].end, sales[0].deepest_px), (2 * unit, WalkEnd::LimitReached, Some(Px::new(98 * unit))));
        assert_eq!((sales[1].filled, sales[1].end), (3 * unit, WalkEnd::Filled));
        assert_eq!((sales[2].filled, sales[2].end, sales[2].vwap_px), (unit, WalkEnd::Filled, Some(99.0)), "each walks from the best");
        let asks = [level(101, 1), level(102, 1)];
        let buy = alone_walks(&asks, &[(2 * unit, Some(Limit { taker: Side::Bid, px: Px::new(101 * unit) }))], false);
        assert_eq!((buy[0].filled, buy[0].end), (unit, WalkEnd::LimitReached));
        let none = alone_walks(&asks, &[(unit, Some(Limit { taker: Side::Bid, px: Px::new(100 * unit) }))], false);
        assert_eq!((none[0].filled, none[0].end, none[0].vwap_px), (0, WalkEnd::LimitReached, None));
        let thin = alone_walks(&asks, &[(5 * unit, None)], true);
        assert_eq!(thin[0].end, WalkEnd::TruncatedAtLevels);
    }

    /// Codex's 000443 r1 counterexample: bids 99×1, 98×1, 97×1; A then B each sell 1, then A sells 2. Alone, each
    /// walks from the best bid.
    #[test]
    fn candidates_share_the_depth_in_order() {
        let book = [level(99, 1), level(98, 1), level(97, 1)];
        let unit = 100_000_000;
        let shares = allocate(&book, &[unit, unit], false);
        let second = &shares[1];
        assert_eq!((second.ahead, second.together.vwap_px, second.together.deepest_px), (unit, Some(98.0), Some(Px::new(98 * unit))));
        assert_eq!((second.alone.vwap_px, second.alone.deepest_px, second.alone.levels), (Some(99.0), Some(Px::new(99 * unit)), 1));
        let shares = allocate(&book, &[2 * unit, unit], false);
        assert_eq!((shares[0].together.vwap_px, shares[0].together.levels, shares[0].together.end), (Some(98.5), 2, WalkEnd::Filled));
        assert_eq!(shares[0].alone, shares[0].together, "the first walks alone");
        let second = &shares[1];
        assert_eq!((second.ahead, second.together.vwap_px, second.together.deepest_px), (2 * unit, Some(97.0), Some(Px::new(97 * unit))));
        let short = allocate(&book, &[2 * unit, 2 * unit], false);
        assert_eq!((short[1].together.filled, short[1].together.end), (unit, WalkEnd::BookExhausted));
        assert_eq!((short[1].alone.filled, short[1].alone.end, short[1].alone.vwap_px), (2 * unit, WalkEnd::Filled, Some(98.5)));
        let truncated = allocate(&book, &[2 * unit, 2 * unit], true);
        assert_eq!(truncated[1].together.end, WalkEnd::TruncatedAtLevels, "the copy stopped, not the book");
        let empty = allocate(&[], &[unit], false);
        assert_eq!((empty[0].together.filled, empty[0].together.vwap_px, empty[0].alone.deepest_px), (0, None, None));
    }

    /// User 2026-10-25: a band's cumulative size walks from the best level, through any price, recording no steps.
    #[test]
    fn a_cumulative_walk_takes_its_whole_size_from_the_best_level() {
        let unit = 100_000_000;
        let bids = [level(100, 1), level(99, 2), level(98, 5)];
        let walks = cumulative_walks(&bids, &[unit, 4 * unit, 9 * unit], false);
        assert_eq!((walks[0].vwap_px, walks[0].levels, walks[0].end), (Some(100.0), 1, WalkEnd::Filled));
        assert_eq!((walks[1].first_px, walks[1].deepest_px, walks[1].vwap_px), (Some(Px::new(100 * unit)), Some(Px::new(98 * unit)), Some(99.0)));
        assert_eq!((walks[2].filled, walks[2].end), (8 * unit, WalkEnd::BookExhausted));
        assert!(walks.iter().all(|walk| walk.steps.is_empty()));
        assert_eq!(cumulative_walks(&bids, &[9 * unit], true)[0].end, WalkEnd::TruncatedAtLevels);
    }

    /// Review 000490 P3: the one pass gives every size exactly what a walk of it alone from the best level gives -
    /// within a level, at a level's end, past the copy (exhausted or truncated), zero, an empty book.
    #[test]
    fn the_one_pass_equals_a_walk_per_size() {
        let unit = 100_000_000;
        let books = [vec![level(100, 1), level(99, 2), level(98, 5)], vec![(Px::new(97 * unit), Sz::new(unit / 3)), level(96, 1)], Vec::new()];
        for book in &books {
            let total: u64 = book.iter().map(|(_, sz)| sz.value()).sum();
            let sizes: Vec<u64> = (0..=12).map(|step| step * unit / 2).chain([total, total + 1, unit / 3]).collect::<std::collections::BTreeSet<_>>().into_iter().collect();
            for truncated in [false, true] {
                let alone: Vec<WalkPart> = sizes.iter().map(|&size| LevelCursor::default().take(book, size, None, truncated, None)).collect();
                assert_eq!(cumulative_walks(book, &sizes, truncated), alone, "{book:?} truncated {truncated}");
            }
        }
    }

    /// Design 000486: a forced flow walk records each level it takes, best first, the deepest possibly in part; up to
    /// its limit; at most its cap of steps while `levels` stays whole; a candidate's walk records none.
    #[test]
    fn an_alone_walk_records_the_levels_it_takes() {
        let unit = 100_000_000;
        let bids = [level(100, 1), level(99, 2), level(98, 5)];
        let sale = &alone_walks(&bids, &[(4 * unit, None)], false)[0];
        assert_eq!((sale.first_px, sale.deepest_px, sale.levels, sale.filled), (Some(Px::new(100 * unit)), Some(Px::new(98 * unit)), 3, 4 * unit));
        let steps: Vec<_> = sale.steps.iter().map(|step| (step.px.value() / unit, step.level_sz / unit, step.taken / unit)).collect();
        assert_eq!(steps, [(100, 1, 1), (99, 2, 2), (98, 5, 1)], "the deepest level taken in part");
        let limited = &alone_walks(&bids, &[(4 * unit, Some(Limit { taker: Side::Ask, px: Px::new(99 * unit) }))], false)[0];
        assert_eq!(limited.steps.iter().map(|step| step.px.value() / unit).collect::<Vec<_>>(), [100, 99], "none beyond its limit");
        let capped = LevelCursor::default().take(&bids, 4 * unit, None, false, Some(2));
        assert_eq!((capped.steps.len(), capped.levels, capped.deepest_px), (2, 3, Some(Px::new(98 * unit))), "the steps a prefix, the walk whole");
        let shares = allocate(&bids, &[unit, unit], false);
        assert!(shares.iter().all(|share| share.together.steps.is_empty() && share.alone.steps.is_empty()), "a candidate's walk records none");
        assert_eq!(shares[1].together.first_px, Some(Px::new(99 * unit)), "a cumulative walk starts where the one before it stopped");
    }

    /// Design 000486: the deepest level's own size changing, the fill alike, is a different walk (a frame goes out).
    #[test]
    fn the_deepest_levels_size_tells_walks_apart() {
        let unit = 100_000_000;
        let before = &alone_walks(&[level(100, 1), level(99, 5)], &[(2 * unit, None)], false)[0];
        let after = &alone_walks(&[level(100, 1), level(99, 6)], &[(2 * unit, None)], false)[0];
        assert_eq!((before.filled, before.vwap_px, before.deepest_px), (after.filled, after.vwap_px, after.deepest_px));
        assert_ne!(before, after);
    }

    #[test]
    fn walk_sizes_scale_to_the_listener_units() {
        assert_eq!(walk_units(12_345, 5), 12_345_000);
        assert_eq!(walk_units(7, 0), 700_000_000);
    }

    struct Fixed(TakerLevels);

    impl LevelSource for Fixed {
        fn taker_levels(&self, _: &str, taker: Side, _: Sz, max_levels: usize, _: &LockTimers) -> impl Future<Output = Option<TakerLevels>> + Send {
            assert_eq!((taker, max_levels), (Side::Ask, MAX_WALK_LEVELS), "a long is sold into the bids");
            std::future::ready(Some(self.0.clone()))
        }
    }

    /// The task drains the newest request of each key and returns its shares with the book's state.
    #[tokio::test]
    async fn the_walker_answers_the_newest_request() {
        let mailbox = Arc::new(WalkMailbox::default());
        let (results_tx, mut results) = mpsc::channel(4);
        let levels = TakerLevels {
            time: 5,
            height: 6,
            trusted: true,
            withheld_periods: 2,
            levels: vec![level(99, 1), level(98, 1)],
            own_side_best: Some(Px::new(100 * 100_000_000)),
        };
        let key: WalkKey = ("PUMP".to_string(), Side::Ask);
        let user = Address::repeat_byte(1);
        mailbox.post(key.clone(), WalkRequest { version: 1, sequence: vec![(user, 300_000_000, false)] });
        mailbox.post(key.clone(), WalkRequest { version: 2, sequence: vec![(user, 100_000_000, true)] });
        let task = tokio::spawn(run_walker(Fixed(levels), mailbox, results_tx));
        let result = results.recv().await.unwrap();
        assert_eq!((result.version, result.book_height, result.book_time, result.withheld_periods), (2, 6, 5, 2));
        assert_eq!(result.shares.iter().map(|share| share.together.vwap_px).collect::<Vec<_>>(), [Some(99.0)]);
        assert_eq!(result.mid_px, Some(99.5), "the best bid 99 and the best ask 100");
        drop(results);
        task.abort();
    }
}
