//! Static book walks of the liquidation candidates (design 000443 r1 section 3, r2 revisions 3–4, r3 revisions 1, 2,
//! 4). The candidates of one coin and side walk the public book together: ordered by when the mark reaches them (a
//! long's sale first at the highest liquidation price, a short's buy at the lowest; equal prices by address), each
//! takes its walk size from where the ones before it stopped. Static: no cascade, no refill.
//!
//! The account owner sends a request per (coin, side) whose ordered sizes changed; a mailbox keeps only the newest of
//! each. The walk task copies the side's levels under the listener lock (`PriceLevel::total_sz`, at most
//! `MAX_WALK_LEVELS`), allocates them outside it, and returns the shares with the book's progress, trust and withheld
//! periods; the owner takes a result only while the book is trusted in the same period and the request is the newest.

use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex, PoisonError},
    time::Instant,
};

use alloy::primitives::Address;
use log::error;
use tokio::sync::{Notify, mpsc};

use crate::{
    listeners::order_book::{OrderBookListener, TakerLevels},
    metrics::{LIQUIDATION_WALK_LOCK_HOLD_SECONDS, LIQUIDATION_WALK_LOCK_WAIT_SECONDS},
    order_book::{Coin, Px, Side, Sz},
};

/// Levels copied per request at most; a walk that does not fill within them is `TruncatedAtLevels`.
pub(crate) const MAX_WALK_LEVELS: usize = 2_000;
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
    /// The book had no more levels.
    BookExhausted,
    /// The copied levels ran out at `MAX_WALK_LEVELS`: the book may hold more.
    TruncatedAtLevels,
}

/// One candidate's share of a cumulative walk; sizes in 1e-8 units.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WalkShare {
    /// What the candidates before it take.
    pub(crate) ahead: u64,
    pub(crate) filled: u64,
    /// Size-weighted price of what it filled; `None` when nothing.
    pub(crate) vwap_px: Option<f64>,
    /// The last level it reached; `None` when nothing.
    pub(crate) deepest_px: Option<Px>,
    /// Levels it touched.
    pub(crate) levels: usize,
    pub(crate) end: WalkEnd,
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
    /// In the request's order.
    pub(crate) shares: Vec<WalkShare>,
}

/// The cumulative walk of `sizes` (in order) over `levels` (best first); `truncated` when the levels stop at the copy
/// limit.
pub(crate) fn allocate(levels: &[(Px, Sz)], sizes: &[u64], truncated: bool) -> Vec<WalkShare> {
    let mut level = 0;
    let mut used_in_level = 0_u64;
    let mut ahead = 0_u64;
    let mut shares = Vec::with_capacity(sizes.len());
    for &size in sizes {
        let (mut filled, mut notional, mut touched, mut deepest) = (0_u64, 0.0_f64, 0, None);
        while filled < size && level < levels.len() {
            let (px, level_sz) = levels[level];
            let take = (level_sz.value() - used_in_level).min(size - filled);
            filled += take;
            notional = (px.value() as f64).mul_add(take as f64, notional);
            touched += 1;
            deepest = Some(px);
            used_in_level += take;
            if used_in_level == level_sz.value() {
                level += 1;
                used_in_level = 0;
            }
        }
        let end = if filled == size {
            WalkEnd::Filled
        } else if truncated {
            WalkEnd::TruncatedAtLevels
        } else {
            WalkEnd::BookExhausted
        };
        let vwap_px = (filled > 0).then(|| notional / filled as f64 / 10f64.powi(FIXED_POINT_DECIMALS.cast_signed()));
        shares.push(WalkShare { ahead, filled, vwap_px, deepest_px: deepest, levels: touched, end });
        ahead += size;
    }
    shares
}

/// A candidate's walk size in the listener's units.
pub(crate) const fn walk_units(raw: u64, size_decimals: u32) -> u64 {
    #[expect(clippy::expect_used, reason = "a position in 1e-8 units fits u64 (the listener's own unit)")]
    let units = raw.checked_mul(10u64.pow(FIXED_POINT_DECIMALS - size_decimals)).expect("a position size in 1e-8 units");
    units
}

/// The newest pending request of each key; the walk task drains it.
#[derive(Debug, Default)]
pub(crate) struct WalkMailbox {
    pending: Mutex<HashMap<WalkKey, WalkRequest>>,
    notify: Notify,
}

impl WalkMailbox {
    /// Replaces the key's pending request.
    pub(crate) fn post(&self, key: WalkKey, request: WalkRequest) {
        self.lock().insert(key, request);
        self.notify.notify_one();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<WalkKey, WalkRequest>> {
        // the map is replaced or drained in one statement: a panic elsewhere cannot leave it half-written
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The pending requests, taken (a test's view of what the walk task would get).
    #[cfg(test)]
    pub(crate) fn drain(&self) -> HashMap<WalkKey, WalkRequest> {
        std::mem::take(&mut *self.lock())
    }

    async fn take(&self) -> HashMap<WalkKey, WalkRequest> {
        loop {
            let pending = std::mem::take(&mut *self.lock());
            if !pending.is_empty() {
                return pending;
            }
            self.notify.notified().await;
        }
    }
}

/// Where a walk reads its levels.
pub(crate) trait LevelSource: Send + Sync + 'static {
    fn taker_levels(&self, coin: &str, taker: Side, needed: Sz, max_levels: usize) -> impl Future<Output = Option<TakerLevels>> + Send;
}

impl LevelSource for Arc<tokio::sync::Mutex<OrderBookListener>> {
    async fn taker_levels(&self, coin: &str, taker: Side, needed: Sz, max_levels: usize) -> Option<TakerLevels> {
        let waited = Instant::now();
        let listener = self.lock().await;
        let copied = Instant::now();
        LIQUIDATION_WALK_LOCK_WAIT_SECONDS.observe(copied.duration_since(waited).as_secs_f64());
        // the lock is held for the level copy only
        let levels = listener.taker_levels(&Coin::new(coin), taker, needed, max_levels);
        drop(listener);
        LIQUIDATION_WALK_LOCK_HOLD_SECONDS.observe(copied.elapsed().as_secs_f64());
        levels
    }
}

/// The walk task: every pending request, its levels copied under the source's lock and allocated outside it. It ends
/// when the owner drops the results' receiver.
pub(crate) async fn run_walker(source: impl LevelSource, mailbox: Arc<WalkMailbox>, results: mpsc::Sender<WalkResult>) {
    loop {
        for (key, request) in mailbox.take().await {
            let Some(result) = walk(&source, key, request).await else { continue };
            if results.send(result).await.is_err() {
                error!("liquidation candidates: the account owner is gone; the book walks stop");
                return;
            }
        }
    }
}

/// One request; `None` when the coin has no book (outside the market filter, or before the first snapshot).
async fn walk(source: &impl LevelSource, key: WalkKey, request: WalkRequest) -> Option<WalkResult> {
    let sizes: Vec<u64> = request.sequence.iter().map(|(_, size, _)| *size).collect();
    let needed = Sz::new(sizes.iter().sum());
    let levels = source.taker_levels(&key.0, key.1, needed, MAX_WALK_LEVELS).await?;
    let held: u64 = levels.levels.iter().map(|(_, sz)| sz.value()).sum();
    let truncated = levels.levels.len() == MAX_WALK_LEVELS && held < needed.value();
    Some(WalkResult {
        shares: allocate(&levels.levels, &sizes, truncated),
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

    /// Codex's 000443 r1 counterexample: bids 99×1, 98×1, 97×1; A then B each sell 1, then A sells 2.
    #[test]
    fn candidates_share_the_depth_in_order() {
        let book = [level(99, 1), level(98, 1), level(97, 1)];
        let unit = 100_000_000;
        let shares = allocate(&book, &[unit, unit], false);
        assert_eq!((shares[1].ahead, shares[1].vwap_px, shares[1].deepest_px), (unit, Some(98.0), Some(Px::new(98 * unit))));
        let shares = allocate(&book, &[2 * unit, unit], false);
        assert_eq!((shares[0].vwap_px, shares[0].levels, shares[0].end), (Some(98.5), 2, WalkEnd::Filled));
        assert_eq!((shares[1].ahead, shares[1].vwap_px, shares[1].deepest_px), (2 * unit, Some(97.0), Some(Px::new(97 * unit))));
        let short = allocate(&book, &[2 * unit, 2 * unit], false);
        assert_eq!((short[1].filled, short[1].end), (unit, WalkEnd::BookExhausted));
        assert_eq!(allocate(&book, &[2 * unit, 2 * unit], true)[1].end, WalkEnd::TruncatedAtLevels, "the copy stopped, not the book");
        let empty = allocate(&[], &[unit], false);
        assert_eq!((empty[0].filled, empty[0].vwap_px, empty[0].deepest_px), (0, None, None));
    }

    #[test]
    fn walk_sizes_scale_to_the_listener_units() {
        assert_eq!(walk_units(12_345, 5), 12_345_000);
        assert_eq!(walk_units(7, 0), 700_000_000);
    }

    struct Fixed(TakerLevels);

    impl LevelSource for Fixed {
        fn taker_levels(&self, _: &str, taker: Side, _: Sz, max_levels: usize) -> impl Future<Output = Option<TakerLevels>> + Send {
            assert_eq!((taker, max_levels), (Side::Ask, MAX_WALK_LEVELS), "a long is sold into the bids");
            std::future::ready(Some(self.0.clone()))
        }
    }

    /// The task drains the newest request of each key and returns its shares with the book's state.
    #[tokio::test]
    async fn the_walker_answers_the_newest_request() {
        let mailbox = Arc::new(WalkMailbox::default());
        let (results_tx, mut results) = mpsc::channel(4);
        let levels = TakerLevels { time: 5, height: 6, trusted: true, withheld_periods: 2, levels: vec![level(99, 1), level(98, 1)] };
        let key: WalkKey = ("PUMP".to_string(), Side::Ask);
        let user = Address::repeat_byte(1);
        mailbox.post(key.clone(), WalkRequest { version: 1, sequence: vec![(user, 300_000_000, false)] });
        mailbox.post(key.clone(), WalkRequest { version: 2, sequence: vec![(user, 100_000_000, true)] });
        let task = tokio::spawn(run_walker(Fixed(levels), mailbox, results_tx));
        let result = results.recv().await.unwrap();
        assert_eq!((result.version, result.book_height, result.book_time, result.withheld_periods), (2, 6, 5, 2));
        assert_eq!(result.shares.iter().map(|share| share.vwap_px).collect::<Vec<_>>(), [Some(99.0)]);
        drop(results);
        task.abort();
    }
}
