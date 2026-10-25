//! The forced flow's reads of the order book (design 000451 r4 revision 11): a coin's trigger orders in a band, and
//! the levels its entries walk alone and its bands' cumulative sizes walk (user 2026-10-25). Copied under the listener
//! lock, walked outside it; the owner sends a job per (coin, tier, kind), a mailbox keeps only the newest of each, and
//! the results come back over a bounded channel.

use std::{future::Future, sync::Arc};

use log::error;
use tokio::sync::{Mutex, mpsc};

use super::{
    forced_flow::Tier,
    walk::{LevelSource, Limit, LockTimers, MAX_WALK_LEVELS, Mailbox, WalkPart, alone_walks, cumulative_walks, levels_truncated, mid_px, under_lock},
};
use crate::{
    listeners::order_book::{OrderBookListener, TriggerBandAt},
    metrics::{FORCED_FLOW_LOCK_HOLD_SECONDS, FORCED_FLOW_LOCK_WAIT_SECONDS, FORCED_FLOW_WALKED_ENTRIES_TOTAL},
    order_book::{Coin, Px, Side, Sz},
};

/// Trigger orders copied per band read at most; more is `truncated` (bm 10-21: 14,094 BTC orders within 1,000 bp).
pub(crate) const MAX_BAND_ORDERS: usize = 50_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum FlowJobKind {
    Band,
    Walk,
}

pub(crate) type FlowJobKey = (String, Tier, FlowJobKind);

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FlowJob {
    /// The coin's trigger orders with a trigger price in `[lo, hi]`.
    Band { version: u64, lo: Px, hi: Px },
    /// Per taker side, its entries and its bands' cumulative sizes.
    Walk { version: u64, sides: Vec<SideJob> },
}

/// One entry of a walk job: its size (1e-8 units) and limit.
pub(crate) type WalkItem = (u64, Option<Limit>);

/// One taker side of a walk job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SideJob {
    pub(crate) taker: Side,
    /// Each entry's size and limit, in the entries' order.
    pub(crate) items: Vec<WalkItem>,
    /// The bands' cumulative sizes (1e-8 units), each walked from the best level.
    pub(crate) cumulative: Vec<u64>,
}

pub(crate) type FlowMailbox = Mailbox<FlowJobKey, FlowJob>;

/// The book a side was copied from.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct BookAt {
    pub(crate) height: u64,
    pub(crate) time: u64,
    pub(crate) trusted: bool,
    pub(crate) withheld_periods: u64,
    pub(crate) mid_px: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SideWalks {
    pub(crate) taker: Side,
    /// `None` without the coin's book (outside the market filter, or before the first snapshot).
    pub(crate) book: Option<BookAt>,
    /// In the job's order; empty without a book.
    pub(crate) walks: Vec<WalkPart>,
    /// The cumulative sizes' walks, in the job's order; empty without a book.
    pub(crate) cumulative: Vec<WalkPart>,
}

#[derive(Debug, Clone)]
pub(crate) enum FlowRead {
    /// `read` is `None` before the listener's first snapshot.
    Band { coin: String, tier: Tier, version: u64, read: Option<TriggerBandAt> },
    Walk { coin: String, tier: Tier, version: u64, sides: Vec<SideWalks> },
}

/// Where the reads go: the trigger orders of a band besides the levels.
pub(crate) trait FlowSource: LevelSource {
    fn trigger_band(&self, coin: &str, lo: Px, hi: Px, max: usize, timers: &LockTimers) -> impl Future<Output = Option<TriggerBandAt>> + Send;
}

impl FlowSource for Arc<Mutex<OrderBookListener>> {
    async fn trigger_band(&self, coin: &str, lo: Px, hi: Px, max: usize, timers: &LockTimers) -> Option<TriggerBandAt> {
        // the lock is held for the copy of the band's `Arc`s only
        under_lock(self, timers, |listener| listener.untriggered_in_band(&Coin::new(coin), lo, hi, max)).await
    }
}

/// The forced flow's timers of one read (design 000451 batch 3).
fn read_timers(read: &str) -> LockTimers {
    LockTimers { wait: FORCED_FLOW_LOCK_WAIT_SECONDS.with_label_values(&[read]), hold: FORCED_FLOW_LOCK_HOLD_SECONDS.with_label_values(&[read]) }
}

/// The reader task: every pending job, read and sent back. It ends when the owner drops the results' receiver.
pub(crate) async fn run_flow_reader(source: impl FlowSource, mailbox: Arc<FlowMailbox>, results: mpsc::Sender<FlowRead>) {
    let (band_timers, walk_timers) = (read_timers("band"), read_timers("walk"));
    loop {
        for ((coin, tier, _), job) in mailbox.take().await {
            let read = match job {
                FlowJob::Band { version, lo, hi } => {
                    let read = source.trigger_band(&coin, lo, hi, MAX_BAND_ORDERS, &band_timers).await;
                    FlowRead::Band { coin, tier, version, read }
                }
                FlowJob::Walk { version, sides } => {
                    let mut walked = Vec::with_capacity(sides.len());
                    for side in sides {
                        walked.push(side_walks(&source, &walk_timers, &coin, &side).await);
                    }
                    FlowRead::Walk { coin, tier, version, sides: walked }
                }
            };
            if results.send(read).await.is_err() {
                error!("forced flow: the account owner is gone; the reads stop");
                return;
            }
        }
    }
}

/// One side's levels, deep enough for the largest entry and the largest cumulative size, and each walked over them.
async fn side_walks(source: &impl FlowSource, timers: &LockTimers, coin: &str, side: &SideJob) -> SideWalks {
    let taker = side.taker;
    let needed = Sz::new(side.items.iter().map(|(size, _)| *size).chain(side.cumulative.iter().copied()).max().unwrap_or(0));
    let Some(levels) = source.taker_levels(coin, taker, needed, MAX_WALK_LEVELS, timers).await else {
        return SideWalks { taker, book: None, walks: Vec::new(), cumulative: Vec::new() };
    };
    FORCED_FLOW_WALKED_ENTRIES_TOTAL.inc_by(side.items.len() as u64);
    let book = BookAt { height: levels.height, time: levels.time, trusted: levels.trusted, withheld_periods: levels.withheld_periods, mid_px: mid_px(&levels) };
    let truncated = levels_truncated(&levels, needed);
    SideWalks {
        taker,
        book: Some(book),
        walks: alone_walks(&levels.levels, &side.items, truncated),
        cumulative: cumulative_walks(&levels.levels, &side.cumulative, truncated),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use super::*;
    use crate::listeners::order_book::TakerLevels;

    /// A book that records how much each copy asked for.
    struct Recording {
        needed: StdMutex<Vec<u64>>,
        levels: TakerLevels,
    }

    impl LevelSource for Recording {
        fn taker_levels(&self, _: &str, _: Side, needed: Sz, _: usize, _: &LockTimers) -> impl Future<Output = Option<TakerLevels>> + Send {
            self.needed.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(needed.value());
            std::future::ready(Some(self.levels.clone()))
        }
    }

    impl FlowSource for Recording {
        fn trigger_band(&self, _: &str, _: Px, _: Px, _: usize, _: &LockTimers) -> impl Future<Output = Option<TriggerBandAt>> + Send {
            std::future::ready(None)
        }
    }

    /// User 2026-10-25: the copy holds the largest cumulative size too (a copy only deep enough for the largest entry
    /// would end the cumulative walk at its bottom), and each cumulative size walks from the best level.
    #[tokio::test]
    async fn a_side_is_copied_deep_enough_for_its_largest_cumulative_size() {
        let unit = 100_000_000;
        let levels = TakerLevels {
            time: 5,
            height: 6,
            trusted: true,
            withheld_periods: 0,
            levels: vec![(Px::new(99 * unit), Sz::new(2 * unit)), (Px::new(98 * unit), Sz::new(5 * unit))],
            own_side_best: Some(Px::new(100 * unit)),
        };
        let source = Recording { needed: StdMutex::new(Vec::new()), levels };
        let side = SideJob { taker: Side::Ask, items: vec![(unit, None)], cumulative: vec![unit, 6 * unit] };
        let walked = side_walks(&source, &read_timers("walk"), "PUMP", &side).await;
        assert_eq!(*source.needed.lock().unwrap_or_else(std::sync::PoisonError::into_inner), [6 * unit]);
        assert_eq!(walked.walks.len(), 1);
        let cumulative: Vec<_> = walked.cumulative.iter().map(|walk| (walk.filled, walk.first_px, walk.deepest_px)).collect();
        assert_eq!(cumulative, [(unit, Some(Px::new(99 * unit)), Some(Px::new(99 * unit))), (6 * unit, Some(Px::new(99 * unit)), Some(Px::new(98 * unit)))]);
    }
}
