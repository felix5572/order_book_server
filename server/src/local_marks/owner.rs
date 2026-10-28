//! The local marks' owner (design 000454 r1 1.3, r2 revision 1): every 50 ms it reads the node prices the account owner
//! published, the HL book under the listener lock and every feed, and publishes every coin's marks (`snapshot`).
//!
//! The account owner gone (its price channel closed) stops the local marks until restart: every leg is withheld with
//! the reason, still published each tick. The owner task ending - a panic, an internal error - closes the snapshot
//! channel: its readers withdraw, and the order book keeps serving.

use std::{sync::Arc, time::Duration};

use log::error;
use tokio::{
    sync::{Mutex, watch},
    time::{Instant, MissedTickBehavior},
};

use super::{
    feeds::LocalMarkFeeds,
    now_ms,
    snapshot::{CoinTable, LocalMarksSnapshot, Tick, assemble},
};
use crate::{
    accounts::publish::NodePrices,
    listeners::order_book::{BestPricesAt, OrderBookListener},
    order_book::Coin,
};

pub(crate) const TICK: Duration = Duration::from_millis(50);
const OWNER_GONE: &str = "the account owner is gone";
const NOT_TICKED: &str = "the local marks have not ticked yet";

/// Where a tick reads the HL book's best prices.
pub(crate) trait BookPrices: Send + Sync + 'static {
    fn best_prices(&self, coins: &[Coin]) -> impl Future<Output = Option<BestPricesAt>> + Send;
}

impl BookPrices for Arc<Mutex<OrderBookListener>> {
    async fn best_prices(&self, coins: &[Coin]) -> Option<BestPricesAt> {
        // the lock is held for the copy only
        self.lock().await.best_prices(coins)
    }
}

/// The feeds and the latest snapshot of the owner task.
#[derive(Clone)]
pub(crate) struct LocalMarks {
    pub(crate) feeds: Arc<LocalMarkFeeds>,
    snapshots: watch::Receiver<Arc<LocalMarksSnapshot>>,
}

impl LocalMarks {
    /// Spawns the owner task, and a watcher logging its end.
    pub(crate) fn start(feeds: Arc<LocalMarkFeeds>, node_prices: watch::Receiver<NodePrices>, book: impl BookPrices) -> Self {
        let table = Arc::new(CoinTable::new(&feeds.sources, feeds.hip3.as_deref()));
        let now_ms = now_ms();
        let readings = feeds.readings(now_ms);
        let sip = feeds.sip_reading(now_ms);
        let tick = Tick { now_ms, instant: Instant::now(), stopped: None, node: &Err(NOT_TICKED.to_string()), book: None, readings: &readings, sip: sip.as_ref() };
        let initial = assemble(&table, &tick, &mut table.hip3_states());
        let (snapshots, receiver) = watch::channel(Arc::new(initial));
        let owner = tokio::spawn(run_owner(table, Arc::clone(&feeds), node_prices, book, snapshots));
        tokio::spawn(async move {
            let ended = owner.await;
            error!("local marks: the owner task ended; the marks stay withdrawn until restart: {ended:?}");
        });
        Self { feeds, snapshots: receiver }
    }

    /// Every snapshot from now on; its channel closes when the owner task ends.
    pub(crate) fn subscribe(&self) -> watch::Receiver<Arc<LocalMarksSnapshot>> {
        self.snapshots.clone()
    }

    /// The latest snapshot; `Err` once the owner task ended.
    pub(crate) fn latest(&self) -> Result<Arc<LocalMarksSnapshot>, &'static str> {
        if self.snapshots.has_changed().is_err() {
            return Err(OWNER_TASK_ENDED);
        }
        Ok(Arc::clone(&self.snapshots.borrow()))
    }
}

pub(crate) const OWNER_TASK_ENDED: &str = "the local marks owner task ended";

#[cfg(test)]
impl LocalMarks {
    /// Marks whose snapshots the test publishes.
    pub(crate) const fn of_snapshots_for_test(feeds: Arc<LocalMarkFeeds>, snapshots: watch::Receiver<Arc<LocalMarksSnapshot>>) -> Self {
        Self { feeds, snapshots }
    }
}

async fn run_owner(
    table: Arc<CoinTable>,
    feeds: Arc<LocalMarkFeeds>,
    mut node_prices: watch::Receiver<NodePrices>,
    book: impl BookPrices,
    snapshots: watch::Sender<Arc<LocalMarksSnapshot>>,
) {
    let mut ticker = tokio::time::interval(TICK);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut owner_gone = false;
    // the HIP-3 coins' state lives across ticks (design 000535 r2 revision 4, 000537)
    let mut hip3_states = table.hip3_states();
    loop {
        ticker.tick().await;
        if !owner_gone && node_prices.has_changed().is_err() {
            error!("local marks: {OWNER_GONE}; every local and node leg is withdrawn until restart");
            owner_gone = true;
        }
        let node = if owner_gone { Err(OWNER_GONE.to_string()) } else { node_prices.borrow_and_update().clone() };
        let best = book.best_prices(table.book_coins()).await;
        let now_ms = now_ms();
        let readings = feeds.readings(now_ms);
        let sip = feeds.sip_reading(now_ms);
        let stopped = if owner_gone { Some(OWNER_GONE.to_string()) } else { feeds.stopped() };
        let tick = Tick { now_ms, instant: Instant::now(), stopped, node: &node, book: best.as_ref(), readings: &readings, sip: sip.as_ref() };
        let snapshot = assemble(&table, &tick, &mut hip3_states);
        drop(snapshots.send_replace(Arc::new(snapshot)));
    }
}

#[cfg(test)]
mod tests {
    use rustc_hash::FxHashMap;

    use super::*;
    use crate::{
        accounts::marks::NativePriceSnapshot,
        local_marks::{compute::tests::btc_state, sources::MarkSources},
        order_book::Px,
    };

    const SOURCES: &str = r#"{"coins": {"BTC": {"spot": {"kraken": {"symbol": "XBT/USDT", "pxMultiplier": "1"}},
        "perp": {"gate": {"symbol": "BTC_USDT", "pxMultiplier": "1"}}}}}"#;

    /// A book always at the local clock.
    struct FreshBook;

    impl BookPrices for FreshBook {
        fn best_prices(&self, coins: &[Coin]) -> impl Future<Output = Option<BestPricesAt>> + Send {
            let px = Px::new(10_000_000_000_000);
            let prices = coins.iter().map(|_| (Some(px), Some(px))).collect();
            std::future::ready(Some(BestPricesAt { time: now_ms(), height: 1, trusted: true, prices }))
        }
    }

    /// BTC's node state, its block a minute ahead: never behind.
    fn node_prices() -> Arc<NativePriceSnapshot> {
        let node = btc_state(0.0, now_ms() * 1_000_000);
        let coins = FxHashMap::from_iter([("BTC".to_string(), node)]);
        Arc::new(NativePriceSnapshot { seed_height: 1, height: 2, block_time_ms: now_ms() + 60_000, coins, hip3: FxHashMap::default() })
    }

    async fn next(snapshots: &mut watch::Receiver<Arc<LocalMarksSnapshot>>) -> Arc<LocalMarksSnapshot> {
        tokio::time::timeout(Duration::from_secs(5), snapshots.changed()).await.unwrap().unwrap();
        Arc::clone(&snapshots.borrow_and_update())
    }

    /// Design 000454 r2 revision 1: the account owner's price channel closing withdraws every leg with the reason, and
    /// it stays withdrawn; the owner keeps publishing. The feeds here run no task, so they quote nothing.
    #[tokio::test]
    async fn the_account_owner_gone_withdraws_every_leg_until_restart() {
        let sources = Arc::new(MarkSources::parse(SOURCES).unwrap());
        let feeds = Arc::new(LocalMarkFeeds::idle_for_test(sources, None));
        let (prices, receiver) = watch::channel(Ok(node_prices()));
        let marks = LocalMarks::start(feeds, receiver, FreshBook);
        let mut snapshots = marks.subscribe();
        let ready = next(&mut snapshots).await;
        let btc = ready.coin("BTC").unwrap();
        assert!(btc.node.is_ok(), "{:?}", btc.node);
        assert_eq!(btc.local.as_ref().unwrap_err().code, "oracleCoverage", "the idle feeds quote nothing");
        drop(prices);
        // a tick under way when the channel closed may still carry the last prices
        let mut seen = 0;
        while next(&mut snapshots).await.coin("BTC").unwrap().node.is_ok() {
            seen += 1;
            assert!(seen < 2, "withdrawn from the tick after the close");
        }
        for _ in 0..3 {
            let gone = next(&mut snapshots).await;
            let btc = gone.coin("BTC").unwrap();
            assert_eq!(btc.node.as_ref().unwrap_err().detail, OWNER_GONE);
            let local = btc.local.as_ref().unwrap_err();
            assert_eq!((local.code, local.detail.as_str()), ("stopped", OWNER_GONE));
        }
        assert!(marks.latest().is_ok(), "the owner task runs on");
    }
}
