//! Publish only subscribed, changed quotes. This is deduplication, not a timer:
//! every observed price/size transition (including A -> B -> A) remains ordered.
use super::{BboUpdate, CoinBbo, RawBbo, SharedFrame};
use crate::{metrics::BBO_PUBLICATION_QUOTES_TOTAL, order_book::Coin};
use std::{collections::{HashMap, HashSet}, sync::{Arc, Mutex}};

/// The existing wire change predicate excludes the level's order count.
pub(crate) type BboKey = (Option<(u64, u64)>, Option<(u64, u64)>);

pub(crate) fn key(raw: &RawBbo) -> BboKey {
    (raw.0.map(|(px, sz, _)| (px.value(), sz.value())), raw.1.map(|(px, sz, _)| (px.value(), sz.value())))
}

#[derive(Default)]
struct CoinSubscription {
    subscribers: usize,
    /// Trust period prevents a recovered, unchanged quote from being suppressed.
    last: Option<(u64, BboKey)>,
}

/// Shared by listener and connection guards. State is bounded by coins with live
/// subscriptions; the last guard removes the coin and its dedup state together.
/// The short lock spans only registration or pure top-of-book reads/comparisons:
/// no serialization, channel waits, I/O, or awaits under it.
#[derive(Clone, Default)]
pub(super) struct BboSubscriptions(Arc<Mutex<HashMap<Coin, CoinSubscription>>>);

#[allow(clippy::expect_used, reason = "lock guards only fixed-point bookkeeping and in-memory book reads")]
impl BboSubscriptions {
    pub(super) fn acquire(&self, coin: &str) -> BboSubscriptionGuard {
        let coin = Coin::new(coin);
        let mut coins = self.0.lock().expect("BBO subscriptions lock not poisoned");
        let entry = coins.entry(coin.clone()).or_default();
        entry.subscribers += 1;
        // A new receiver has not seen the previous quote. Its first book event
        // must pass even if other clients already received the same price/size.
        entry.last = None;
        drop(coins);
        BboSubscriptionGuard { registry: self.clone(), coin }
    }

    pub(super) fn changes(
        &self,
        changed: &HashSet<Coin>,
        withheld_period: u64,
        read: impl FnOnce(&HashSet<Coin>) -> (u64, HashMap<Coin, RawBbo>),
    ) -> Option<BboUpdate> {
        let mut coins = self.0.lock().expect("BBO subscriptions lock not poisoned");
        let wanted: HashSet<_> = changed.iter().filter(|coin| coins.contains_key(*coin)).cloned().collect();
        BBO_PUBLICATION_QUOTES_TOTAL.with_label_values(&["not_subscribed"]).inc_by((changed.len() - wanted.len()) as u64);
        if wanted.is_empty() { return None; }
        let (time, quotes) = read(&wanted);
        let mut bbos = HashMap::new();
        for (coin, raw) in quotes {
            let entry = coins.get_mut(&coin).expect("book read only returns requested, still-registered coins");
            let current = (withheld_period, key(&raw));
            if entry.last == Some(current) {
                BBO_PUBLICATION_QUOTES_TOTAL.with_label_values(&["unchanged"]).inc();
                continue;
            }
            entry.last = Some(current);
            bbos.insert(coin, CoinBbo { raw, frame: SharedFrame::new() });
        }
        drop(coins);
        BBO_PUBLICATION_QUOTES_TOTAL.with_label_values(&["published"]).inc_by(bbos.len() as u64);
        (!bbos.is_empty()).then_some(BboUpdate { bbos, time, withheld_period })
    }
}

pub(super) struct BboSubscriptionGuard { registry: BboSubscriptions, coin: Coin }

impl Drop for BboSubscriptionGuard {
    #[allow(clippy::expect_used, reason = "each live guard owns one registration; bookkeeping cannot poison the lock")]
    fn drop(&mut self) {
        let mut coins = self.registry.0.lock().expect("BBO subscriptions lock not poisoned");
        let entry = coins.get_mut(&self.coin).expect("live guard has a registered coin");
        entry.subscribers -= 1;
        if entry.subscribers == 0 { coins.remove(&self.coin); }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order_book::{Px, Sz};

    fn quote(px: u64, sz: u64) -> RawBbo { (Some((Px::new(px), Sz::new(sz), 1)), None) }
    fn emit(registry: &BboSubscriptions, coin: &str, raw: RawBbo, period: u64) -> Option<BboUpdate> {
        let coin = Coin::new(coin);
        registry.changes(&HashSet::from([coin.clone()]), period, |_| (123, HashMap::from([(coin, raw)])))
    }

    #[test]
    fn preserves_each_price_size_and_empty_transition_without_conflation() {
        let registry = BboSubscriptions::default();
        let _guard = registry.acquire("BTC");
        for raw in [quote(10, 1), quote(11, 1), quote(10, 1), quote(10, 2), (None, None)] {
            let update = emit(&registry, "BTC", raw, 0).unwrap();
            assert_eq!(update.bbos["BTC"].raw, raw);
            assert_eq!(update.time, 123);
            assert!(emit(&registry, "BTC", raw, 0).is_none());
        }
    }

    #[test]
    fn new_subscriber_resubscribe_and_trust_recovery_republish_same_quote() {
        let registry = BboSubscriptions::default();
        let guard = registry.acquire("BTC");
        let raw = quote(10, 1);
        assert!(emit(&registry, "BTC", raw, 0).is_some());
        assert!(emit(&registry, "BTC", raw, 0).is_none());
        let second = registry.acquire("BTC");
        assert!(emit(&registry, "BTC", raw, 0).is_some());
        drop(second);
        assert!(emit(&registry, "BTC", raw, 0).is_none(), "remaining subscriber still owns dedup state");
        assert!(emit(&registry, "BTC", raw, 1).is_some(), "recovery has a new trust period");
        drop(guard);
        assert!(registry.0.lock().unwrap().is_empty());
        let _resubscribed = registry.acquire("BTC");
        assert!(emit(&registry, "BTC", raw, 1).is_some());
    }

    #[test]
    fn unrequested_coins_are_neither_read_nor_queued() {
        let registry = BboSubscriptions::default();
        let _guard = registry.acquire("ETH");
        assert!(registry.changes(&HashSet::from([Coin::new("BTC")]), 0, |_| panic!("no BBO reader work")).is_none());
        let update = registry.changes(&HashSet::from([Coin::new("BTC"), Coin::new("ETH")]), 0, |wanted| {
            assert_eq!(wanted, &HashSet::from([Coin::new("ETH")]));
            (1, HashMap::from([(Coin::new("ETH"), quote(10, 1))]))
        }).unwrap();
        assert_eq!(update.bbos.len(), 1);
        assert!(update.bbos.contains_key("ETH"));
    }

    #[test]
    fn keeps_existing_wire_predicate_when_only_order_count_changes() {
        let registry = BboSubscriptions::default();
        let _guard = registry.acquire("BTC");
        assert!(emit(&registry, "BTC", quote(10, 1), 0).is_some());
        let count_only = (Some((Px::new(10), Sz::new(1), 2)), None);
        assert!(emit(&registry, "BTC", count_only, 0).is_none());
    }
}
