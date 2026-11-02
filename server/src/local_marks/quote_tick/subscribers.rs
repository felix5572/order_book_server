//! Each external quote channel's subscribers of each coin (hyperliquid-trade design 000673 r3 §2, r4 首帧): a count and
//! a subscription epoch in one atomic word, so the quote tick reads both at once. The count rising from zero opens the
//! next epoch. The tick publishes a coin's first frame of an epoch whatever its fingerprint, from slots it copied after
//! reading that epoch, and a WS session takes only frames of the epoch it subscribed in: a frame evaluated before then,
//! an old tick's published after the coin was unsubscribed and subscribed again, is never its first. A session joining
//! a coin already subscribed shares its epoch, and its current frame.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use super::snapshot::{QuoteChannel, QuoteKey};
use crate::local_marks::snapshot::CoinTable;

/// The count is the word's low half, the epoch its high half.
const COUNT_BITS: u32 = 32;
const COUNT_MASK: u64 = (1 << COUNT_BITS) - 1;

/// A coin's subscribers on a channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Subscribers {
    pub(crate) count: u32,
    pub(crate) epoch: u32,
}

impl Subscribers {
    /// Each half of the word is 32 bits.
    const fn of(word: u64) -> Self {
        Self { count: (word & COUNT_MASK) as u32, epoch: (word >> COUNT_BITS) as u32 }
    }

    fn word(self) -> u64 {
        (u64::from(self.epoch) << COUNT_BITS) | u64::from(self.count)
    }

    /// One more: the first opens the next epoch. A count is at most the connections', far below 2^32.
    const fn joined(self) -> Self {
        if self.count == 0 { Self { count: 1, epoch: self.epoch.wrapping_add(1) } } else { Self { count: self.count + 1, epoch: self.epoch } }
    }
}

/// Both channels' words, by each channel's coin index.
#[derive(Debug)]
pub(crate) struct QuoteSubscribers {
    main: Vec<AtomicU64>,
    hip3: Vec<AtomicU64>,
}

impl QuoteSubscribers {
    /// No subscriber of any coin of `table`'s, main-dex or HIP-3 external.
    pub(crate) fn new(table: &CoinTable) -> Self {
        let words = |coins: usize| (0..coins).map(|_| AtomicU64::new(0)).collect();
        Self { main: words(table.names().len()), hip3: words(table.hip3_external().names().len()) }
    }

    fn word(&self, key: QuoteKey) -> &AtomicU64 {
        match key.channel {
            QuoteChannel::Main => &self.main[key.index],
            QuoteChannel::Hip3 => &self.hip3[key.index],
        }
    }

    /// One more subscriber of the coin, its epoch; the guard takes it back when dropped.
    pub(crate) fn acquire(self: &Arc<Self>, key: QuoteKey) -> SubscriberGuard {
        let previous = self.word(key).update(Ordering::SeqCst, Ordering::SeqCst, |word| Subscribers::of(word).joined().word());
        SubscriberGuard { subscribers: Arc::clone(self), key, epoch: Subscribers::of(previous).joined().epoch }
    }

    /// The coin's subscribers now.
    pub(crate) fn read(&self, key: QuoteKey) -> Subscribers {
        Subscribers::of(self.word(key).load(Ordering::SeqCst))
    }

    /// Every coin subscribed now with its epoch, the main-dex ones then the HIP-3 ones, each in its table's order.
    pub(crate) fn subscribed(&self) -> Vec<(QuoteKey, u32)> {
        let channel = |channel: QuoteChannel, words: &[AtomicU64]| {
            (0..words.len()).map(move |index| QuoteKey { channel, index })
        };
        channel(QuoteChannel::Main, &self.main)
            .chain(channel(QuoteChannel::Hip3, &self.hip3))
            .filter_map(|key| Some(self.read(key)).filter(|subscribers| subscribers.count > 0).map(|subscribers| (key, subscribers.epoch)))
            .collect()
    }
}

/// A session's subscription of a coin on a channel: one of its count, and the epoch it subscribed in.
#[derive(Debug)]
pub(crate) struct SubscriberGuard {
    subscribers: Arc<QuoteSubscribers>,
    key: QuoteKey,
    epoch: u32,
}

impl SubscriberGuard {
    pub(crate) const fn key(&self) -> QuoteKey {
        self.key
    }

    pub(crate) const fn epoch(&self) -> u32 {
        self.epoch
    }
}

impl Drop for SubscriberGuard {
    fn drop(&mut self) {
        // the count is at least this guard's one: the low half only moves
        self.subscribers.word(self.key).fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subscribers(coins: usize) -> Arc<QuoteSubscribers> {
        let words = |coins: usize| (0..coins).map(|_| AtomicU64::new(0)).collect();
        Arc::new(QuoteSubscribers { main: words(coins), hip3: words(coins) })
    }

    const BTC: QuoteKey = QuoteKey { channel: QuoteChannel::Main, index: 0 };
    const GOLD: QuoteKey = QuoteKey { channel: QuoteChannel::Hip3, index: 0 };

    /// The first subscriber opens an epoch the others share; the last one leaving keeps it, and the next subscriber
    /// opens the next one. The channels count apart.
    #[test]
    fn the_count_rising_from_zero_opens_the_next_epoch() {
        let subscribers = subscribers(2);
        assert_eq!(subscribers.subscribed(), Vec::new());
        let first = subscribers.acquire(BTC);
        let second = subscribers.acquire(BTC);
        assert_eq!((first.epoch(), second.epoch()), (1, 1));
        assert_eq!(subscribers.read(BTC), Subscribers { count: 2, epoch: 1 });
        assert_eq!(subscribers.read(GOLD), Subscribers { count: 0, epoch: 0 }, "the other channel's coin of the same index");
        drop(first);
        assert_eq!(subscribers.subscribed(), [(BTC, 1)]);
        drop(second);
        assert_eq!(subscribers.read(BTC), Subscribers { count: 0, epoch: 1 });
        assert_eq!(subscribers.subscribed(), Vec::new());
        let again = subscribers.acquire(BTC);
        let gold = subscribers.acquire(GOLD);
        assert_eq!((again.epoch(), gold.epoch()), (2, 1));
        assert_eq!(subscribers.subscribed(), [(BTC, 2), (GOLD, 1)]);
    }
}
