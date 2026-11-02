//! The external quote tick (hyperliquid-trade design 000673 r3–r4): the one producer of the two external quote
//! channels' WS frames, `extExternalQuotes` (the main-dex coins', `external_quotes_view`) and `extHip3ExternalQuotes`
//! (the HIP-3 coins', `hip3::external_view`), a task of its own beside the local marks' owner, whose 50 ms tick waits
//! for the listener lock and assembles every coin's marks; the mark channels and the HTTP views stay the owner's.
//!
//! Every `QUOTE_TICK` (5 ms; a late tick delays the next ones, it never bunches them) it judges the coins with at least
//! one subscriber on either channel, and only those (`subscribers`): it copies the partitions their quotes are in, each
//! once, then takes one publish time and judges every leg at it by the owner's own rules - the slots' validity,
//! `received_by`, the listings' multipliers, the main dex's weighted medians, the US stocks' sessions (`evaluate`). It
//! reads no book and takes no lock but the slots'. The bases are the owner's, read from its snapshot and sampled by it
//! only: one computed more than `BASIS_MAX_AGE` before is withheld (`basisStale`), its quote still published.
//!
//! A coin's frame is serialized once and published with the tick's version when its decision fingerprint changed, on
//! the first tick of a subscription epoch, and for every subscribed coin every `HEARTBEAT_TICKS` ticks (`publish`).
//! Each tick takes the latest quotes: a change shorter than a tick may merge into the next, as one shorter than the
//! owner's 50 ms tick did. The owner's snapshot channel closed - its task ended -, or the owner stopped - the feeds or
//! the account owner gone, either final until restart -, withdraws both channels: the tick's last snapshot names why,
//! and the tick ends.

pub(crate) mod evaluate;
pub(crate) mod publish;
pub(crate) mod snapshot;
pub(crate) mod subscribers;

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use log::error;
use tokio::{
    sync::watch,
    time::{Instant, MissedTickBehavior},
};

use self::{
    evaluate::{QuoteReading, hip3_legs, main_leg},
    publish::{Evaluated, Publication, Publisher},
    snapshot::{QuoteChannel, QuoteKey, QuoteSnapshot},
    subscribers::QuoteSubscribers,
};
use super::{
    external_quotes_view::ExternalQuotesView,
    feeds::LocalMarkFeeds,
    hip3::external_view::Hip3ExternalView,
    now_ms,
    owner::OWNER_TASK_ENDED,
    prices_view::QUOTE_TICK_INTERVAL_MS,
    snapshot::{CoinTable, LocalMarksSnapshot},
};
use crate::metrics::{
    EXTERNAL_QUOTE_PUBLISHED_FRAMES_TOTAL, EXTERNAL_QUOTE_SAMPLE_SECONDS, EXTERNAL_QUOTE_TICK_CHANGED_COINS, EXTERNAL_QUOTE_TICK_DRIFT_SECONDS,
    EXTERNAL_QUOTE_TICK_SECONDS, EXTERNAL_QUOTE_TICK_SUBSCRIBED_COINS,
};

pub(crate) const QUOTE_TICK: Duration = Duration::from_millis(QUOTE_TICK_INTERVAL_MS);
/// Why both channels are withdrawn when the tick's task ended without naming a reason (a panic).
pub(crate) const QUOTE_TICK_ENDED: &str = "the external quote tick ended";

/// The external quote tick's coins, subscribers and snapshots, which the WS sessions hold.
#[derive(Clone)]
pub(crate) struct QuoteTickHandle {
    /// Both channels' coins: the owner's table.
    pub(crate) table: Arc<CoinTable>,
    pub(crate) subscribers: Arc<QuoteSubscribers>,
    snapshots: watch::Receiver<Arc<QuoteSnapshot>>,
}

impl QuoteTickHandle {
    /// Spawns the tick over the owner's snapshots, and a watcher logging its end.
    pub(crate) fn start(feeds: Arc<LocalMarkFeeds>, owner: watch::Receiver<Arc<LocalMarksSnapshot>>) -> Self {
        let table = Arc::clone(owner.borrow().table());
        let subscribers = Arc::new(QuoteSubscribers::new(&table));
        let (sender, snapshots) = watch::channel(Arc::new(QuoteSnapshot::default()));
        let tick = QuoteTick::new(feeds, Arc::clone(&table), Arc::clone(&subscribers));
        let task = tokio::spawn(run(tick, owner, sender));
        tokio::spawn(async move {
            let ended = task.await;
            error!("external quote tick: the task ended; both external quote channels stay withdrawn until restart: {ended:?}");
        });
        Self { table, subscribers, snapshots }
    }

    /// Every snapshot from now on; its channel closes when the tick ends.
    pub(crate) fn subscribe(&self) -> watch::Receiver<Arc<QuoteSnapshot>> {
        self.snapshots.clone()
    }
}

#[cfg(test)]
impl QuoteTickHandle {
    /// A handle whose snapshots the test publishes.
    pub(crate) const fn of_snapshots_for_test(table: Arc<CoinTable>, subscribers: Arc<QuoteSubscribers>, snapshots: watch::Receiver<Arc<QuoteSnapshot>>) -> Self {
        Self { table, subscribers, snapshots }
    }
}

/// The tick's state across ticks: what it reads, and what it last published.
pub(crate) struct QuoteTick {
    feeds: Arc<LocalMarkFeeds>,
    table: Arc<CoinTable>,
    subscribers: Arc<QuoteSubscribers>,
    /// The Binance quotes' age limit, the HIP-3 frames' `quoteMaxAgeMs`; `None` without the HIP-3 external quotes.
    quote_max_age_ms: Option<u64>,
    publisher: Publisher,
}

impl QuoteTick {
    pub(crate) fn new(feeds: Arc<LocalMarkFeeds>, table: Arc<CoinTable>, subscribers: Arc<QuoteSubscribers>) -> Self {
        let quote_max_age_ms = feeds.hip3_external.as_ref().map(|external| external.quote_max_age_ms);
        Self { feeds, table, subscribers, quote_max_age_ms, publisher: Publisher::default() }
    }

    /// One tick over the owner's latest snapshot: the coins subscribed now, their partitions copied, judged at the
    /// publish time and monotonic moment `clock` gives after the copies, and published.
    pub(crate) fn tick(&mut self, owner: &LocalMarksSnapshot, clock: impl FnOnce() -> (u64, Instant)) -> Publication {
        let coins = self.subscribers.subscribed();
        let reading = QuoteReading::take(&self.feeds, &self.table, coins.iter().map(|(key, _)| *key), clock);
        let evaluated = coins.into_iter().map(|(key, epoch)| (key, epoch, evaluate(&self.table, key, &reading, owner))).collect();
        let (table, quote_max_age_ms, published_ms) = (&self.table, self.quote_max_age_ms, reading.published_ms);
        self.publisher.publish(evaluated, (published_ms, reading.instant), |key, legs| frame(table, key, legs, published_ms, quote_max_age_ms))
    }
}

fn evaluate(table: &CoinTable, key: QuoteKey, reading: &QuoteReading<'_>, owner: &LocalMarksSnapshot) -> Evaluated {
    match key.channel {
        QuoteChannel::Main => Evaluated::Main(main_leg(table, key.index, reading, owner)),
        QuoteChannel::Hip3 => Evaluated::Hip3(hip3_legs(table.hip3_external(), key.index, reading, owner)),
    }
}

/// The coin's WS frame of its channel, `intervalMs` the tick's.
fn frame(table: &CoinTable, key: QuoteKey, legs: &Evaluated, published_ms: u64, quote_max_age_ms: Option<u64>) -> Bytes {
    match legs {
        Evaluated::Main(leg) => ExternalQuotesView::new(&table.names()[key.index], QUOTE_TICK_INTERVAL_MS, published_ms, leg).to_wire(),
        Evaluated::Hip3(legs) => {
            let quote_max_age_ms = quote_max_age_ms.unwrap_or_else(|| unreachable!("a HIP-3 coin judged runs with the external quotes"));
            let coin = &table.hip3_external().names()[key.index];
            Hip3ExternalView::new(coin, QUOTE_TICK_INTERVAL_MS, published_ms, quote_max_age_ms, legs).to_wire()
        }
    }
}

async fn run(mut tick: QuoteTick, owner: watch::Receiver<Arc<LocalMarksSnapshot>>, snapshots: watch::Sender<Arc<QuoteSnapshot>>) {
    let mut ticker = tokio::time::interval(QUOTE_TICK);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        let scheduled = ticker.tick().await;
        let woke = Instant::now();
        EXTERNAL_QUOTE_TICK_DRIFT_SECONDS.observe(woke.saturating_duration_since(scheduled).as_secs_f64());
        let latest = Arc::clone(&owner.borrow());
        if let Some(reason) = withdrawn(&owner, &latest) {
            error!("external quote tick: both external quote channels withdrawn until restart: {reason}");
            drop(snapshots.send_replace(Arc::new(QuoteSnapshot::withdrawn_for(reason))));
            return;
        }
        let publication = tick.tick(&latest, || (now_ms(), Instant::now()));
        record(&publication);
        if let Some(snapshot) = publication.snapshot {
            drop(snapshots.send_replace(Arc::new(snapshot)));
        }
        EXTERNAL_QUOTE_TICK_SECONDS.observe(woke.elapsed().as_secs_f64());
    }
}

/// Why both channels are withdrawn: the owner's snapshot channel closed (its task ended), or its snapshot stopped.
fn withdrawn(owner: &watch::Receiver<Arc<LocalMarksSnapshot>>, latest: &LocalMarksSnapshot) -> Option<String> {
    if owner.has_changed().is_err() {
        return Some(OWNER_TASK_ENDED.to_string());
    }
    latest.stopped.clone()
}

fn record(publication: &Publication) {
    EXTERNAL_QUOTE_TICK_SUBSCRIBED_COINS.set(i64::try_from(publication.evaluated).unwrap_or(i64::MAX));
    EXTERNAL_QUOTE_TICK_CHANGED_COINS.observe(publication.changed() as f64);
    for (key, cause) in &publication.published {
        EXTERNAL_QUOTE_PUBLISHED_FRAMES_TOTAL.with_label_values(&[key.channel.name(), cause.label()]).inc();
    }
    // once a sampled frame; its stages after are each connection's (`websocket_server::quote_delivery`)
    for (key, sample) in &publication.samples {
        let elapsed_ms = sample.published_ms.saturating_sub(sample.received_ms);
        EXTERNAL_QUOTE_SAMPLE_SECONDS.with_label_values(&[key.channel.name(), RECEIPT_TO_PUBLISH]).observe(Duration::from_millis(elapsed_ms).as_secs_f64());
    }
}

/// The sampled frames' first stage: from the newest receipt of the quotes that moved it to its publish time.
pub(crate) const RECEIPT_TO_PUBLISH: &str = "receipt_to_publish";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        listeners::order_book::BestPricesAt,
        local_marks::{
            external_quotes_view::tests::ready_external_fields,
            feeds::Hip3ExternalConfig,
            hip3::{external::Hip3ExternalReading, external_sources::Hip3ExternalSources, protocol::EquityFeed, sources::Hip3SipSources},
            price::VenuePrice,
            slot::{FeedSlot, Quote},
            snapshot::{OwnerState, Tick, assemble, tests::World},
            sources::{Feed, Kind, MarkSources, Venue},
            subscriptions::FeedSubscriptions,
        },
        order_book::Px,
    };
    use publish::Cause;
    use snapshot::{QuoteEntry, QuoteSample};
    use subscribers::SubscriberGuard;

    /// APEX on Bybit and MEXC only; BTC on Binance (weight 3) and OKX (2); ETH on Binance. The feeds are spot OKX, then
    /// the perps of Binance, OKX, Bybit and MEXC.
    const MAIN: &str = r#"{"coins": {
        "APEX": {"spot": {"okx": {"symbol": "APEX-USDT", "pxMultiplier": "1"}},
                 "perp": {"bybit": {"symbol": "APEXUSDT", "pxMultiplier": "1"}, "mexc": {"symbol": "APEX_USDT", "pxMultiplier": "1"}}},
        "BTC": {"spot": {"okx": {"symbol": "BTC-USDT", "pxMultiplier": "1"}},
                "perp": {"binance": {"symbol": "BTCUSDT", "pxMultiplier": "1"}, "okx": {"symbol": "BTC-USDT-SWAP", "pxMultiplier": "1"}}},
        "ETH": {"spot": {"okx": {"symbol": "ETH-USDT", "pxMultiplier": "1"}}, "perp": {"binance": {"symbol": "ETHUSDT", "pxMultiplier": "1"}}}}}"#;
    /// Binance's GOLD (market 0) and TSLA (market 1); the SIP's BB (market 0) and TSLA (market 1): the channel is BB,
    /// GOLD, TSLA.
    const BINANCE: &str = r#"{"venue": "binance_perp", "coins": {"xyz:GOLD": {"symbol": "XAUUSDT", "pxMultiplier": "1"},
        "xyz:TSLA": {"symbol": "TSLAUSDT", "pxMultiplier": "1"}}}"#;
    const SIP: &str = r#"{"feed": "sip", "coins": {"xyz:TSLA": {"symbol": "TSLA"}, "xyz:BB": {"symbol": "BB"}}}"#;
    const APEX: QuoteKey = QuoteKey { channel: QuoteChannel::Main, index: 0 };
    const BTC: QuoteKey = QuoteKey { channel: QuoteChannel::Main, index: 1 };
    const ETH: QuoteKey = QuoteKey { channel: QuoteChannel::Main, index: 2 };
    const GOLD: QuoteKey = QuoteKey { channel: QuoteChannel::Hip3, index: 1 };
    const TSLA: QuoteKey = QuoteKey { channel: QuoteChannel::Hip3, index: 2 };
    /// Tuesday 2026-10-27 13:51:02 in New York (EDT, UTC-4): the regular session.
    const NOW_MS: u64 = 1_793_123_462_000;
    /// 09:29:59.990 that day in New York: the pre-market's last 10 ms.
    const PRE_MARKET_END_MS: u64 = 1_793_107_799_990;
    const PERP_BINANCE: Feed = Feed { kind: Kind::Perp, venue: Venue::Binance };
    const PERP_OKX: Feed = Feed { kind: Kind::Perp, venue: Venue::Okx };

    fn quote(bid: &str, ask: &str) -> Quote {
        Quote { bid: VenuePrice::parse(bid).unwrap(), ask: VenuePrice::parse(ask).unwrap() }
    }

    /// The idle feeds of `MAIN`, with the HIP-3 external quotes and the SIP feeds or not, every partition connected
    /// once (its quotes of epoch 1); the owner's state, and the quote tick over them.
    struct Fixture {
        sources: Arc<MarkSources>,
        feeds: Arc<LocalMarkFeeds>,
        table: Arc<CoinTable>,
        state: OwnerState,
        tick: QuoteTick,
    }

    impl Fixture {
        fn new(hip3: bool) -> Self {
            let sources = Arc::new(MarkSources::parse(MAIN).unwrap());
            let sip = Arc::new(Hip3SipSources::parse(SIP).unwrap());
            let binance = Arc::new(Hip3ExternalSources::parse(BINANCE).unwrap());
            let external = hip3.then(|| Hip3ExternalConfig { sources: binance, sip_sources: Arc::clone(&sip), quote_max_age_ms: 60_000 });
            let feeds = Arc::new(LocalMarkFeeds::idle_with_external_for_test(Arc::clone(&sources), hip3.then_some(sip), external));
            let coins = feeds.hip3_external.as_ref().map(|external| &external.coins);
            let table = Arc::new(CoinTable::new(&sources, feeds.hip3.as_deref(), coins));
            let mut groups: Vec<&FeedSubscriptions> = (0..sources.feeds().count()).map(|reading| feeds.main_feed(reading)).collect();
            groups.extend(feeds.hip3_external_feed());
            groups.extend(EquityFeed::ALL.into_iter().filter_map(|feed| feeds.equity_feed(feed)));
            for part in groups.iter().flat_map(|group| &group.subscriptions) {
                assert_eq!(part.slot.connected(0), 1);
            }
            let tick = QuoteTick::new(Arc::clone(&feeds), Arc::clone(&table), Arc::new(QuoteSubscribers::new(&table)));
            Self { sources, state: table.owner_state(), feeds, table, tick }
        }

        fn subscribe(&self, key: QuoteKey) -> SubscriberGuard {
            self.tick.subscribers.acquire(key)
        }

        /// The main-dex feed's slot of `symbol` and its index there.
        fn main_slot(&self, feed: Feed, symbol: &str) -> (Arc<FeedSlot>, usize) {
            let reading = self.sources.feeds().position(|(each, _)| each == feed).unwrap();
            let market = self.sources.markets(feed).unwrap().index(symbol).unwrap();
            let (part, index) = self.feeds.main_feed(reading).part_of(market);
            (Arc::clone(&part.slot), index)
        }

        /// The main-dex feed's quote of `symbol`, received at `received_ms`.
        fn main_quote(&self, feed: Feed, symbol: &str, (bid, ask): (&str, &str), received_ms: u64) {
            let (slot, index) = self.main_slot(feed, symbol);
            slot.record(1, received_ms, [(index, Some(quote(bid, ask)), None)]);
        }

        /// Binance's HIP-3 quote of `market`, received at `received_ms`.
        fn binance_quote(&self, market: usize, (bid, ask): (&str, &str), received_ms: u64) {
            let (part, index) = self.feeds.hip3_external_feed().unwrap().part_of(market);
            part.slot.record(1, received_ms, [(index, Some(quote(bid, ask)), None)]);
        }

        /// The US stock feed's quote of `market`, stamped `quote_ms`, received at `received_ms`.
        fn equity_quote(&self, feed: EquityFeed, market: usize, (bid, ask): (&str, &str), quote_ms: u64, received_ms: u64) {
            let (part, index) = self.feeds.equity_feed(feed).unwrap().part_of(market);
            part.slot.record(1, received_ms, [(index, Some(quote(bid, ask)), Some(quote_ms))]);
        }

        /// The BTC quotes: Binance 100,000 / 100,001 (the medians, by its weight), OKX 100,000.5 / 100,001.5.
        fn btc_quotes(&self, received_ms: u64) {
            self.main_quote(PERP_BINANCE, "BTCUSDT", ("100000", "100001"), received_ms);
            self.main_quote(PERP_OKX, "BTC-USDT-SWAP", ("100000.5", "100001.5"), received_ms);
        }

        /// An owner tick at `now_ms` and `instant` over the feeds as they stand, without a node state.
        fn owner(&mut self, now_ms: u64, instant: Instant, book: Option<&BestPricesAt>) -> LocalMarksSnapshot {
            let readings = self.feeds.readings(now_ms);
            let equity = self.feeds.equity_readings(now_ms);
            let external = self.feeds.hip3_external.as_ref().map(|external| Hip3ExternalReading {
                published_ms: now_ms,
                quote_max_age_ms: external.quote_max_age_ms,
                projection: external.copy().projection(now_ms),
            });
            let node = Err("no node state in this test".to_string());
            let tick = Tick { now_ms, instant, stopped: None, node: &node, book, readings: &readings, equity: equity.as_ref(), hip3_external: external.as_ref() };
            assemble(&self.table, &tick, &mut self.state)
        }

        /// A book at `now_ms` of every coin at 100,001 / 100,003.
        fn book(&self, now_ms: u64) -> BestPricesAt {
            let side = (Some(Px::new(10_000_100_000_000)), Some(Px::new(10_000_300_000_000)));
            BestPricesAt { time: now_ms, height: 1, trusted: true, prices: vec![side; self.table.book_coins().len()] }
        }

        fn quote_tick(&mut self, owner: &LocalMarksSnapshot, published_ms: u64, instant: Instant) -> Publication {
            self.tick.tick(owner, || (published_ms, instant))
        }

        /// `count` ticks 5 ms apart from `NOW_MS` at `start`, the owner ticking every tenth (50 ms), `before` run ahead
        /// of each with its index: each tick's publication.
        fn ticks(&mut self, start: Instant, count: u32, mut before: impl FnMut(&Self, u32)) -> Vec<Publication> {
            let mut owner = None;
            (0..count)
                .map(|tick| {
                    let (published_ms, instant) = (NOW_MS + 5 * u64::from(tick), start + QUOTE_TICK * tick);
                    before(self, tick);
                    if tick % 10 == 0 {
                        owner = Some(self.owner(published_ms, instant, None));
                    }
                    let owner = owner.as_ref().unwrap();
                    self.quote_tick(owner, published_ms, instant)
                })
                .collect()
        }
    }

    fn causes(publication: &Publication) -> Vec<(QuoteKey, Cause)> {
        publication.published.clone()
    }

    fn entry(publication: &Publication, key: QuoteKey) -> QuoteEntry {
        publication.snapshot.as_ref().unwrap().entry(key).unwrap().clone()
    }

    fn data(publication: &Publication, key: QuoteKey) -> serde_json::Value {
        serde_json::from_slice::<serde_json::Value>(&entry(publication, key).frame).unwrap()["data"].clone()
    }

    /// Design 000673 §7: the first tick of a subscription publishes the coin's frame (`intervalMs` 5, the tick's
    /// publish time); unchanged it publishes nothing but the heartbeat every 10 ticks; a price change publishes at the
    /// next tick, under a higher version.
    #[tokio::test]
    async fn a_change_publishes_the_next_tick_and_otherwise_the_heartbeat_every_10() {
        let mut fixture = Fixture::new(false);
        fixture.btc_quotes(NOW_MS - 1_000);
        let _btc = fixture.subscribe(BTC);
        let ticks = fixture.ticks(Instant::now(), 21, |fixture, tick| {
            if tick == 3 {
                fixture.main_quote(PERP_BINANCE, "BTCUSDT", ("100000.1", "100001"), NOW_MS + 14);
            }
        });
        let published: Vec<_> = ticks.iter().enumerate().filter(|(_, tick)| !tick.published.is_empty()).map(|(index, tick)| (index, causes(tick))).collect();
        assert_eq!(published, [(0, vec![(BTC, Cause::First)]), (3, vec![(BTC, Cause::Changed)]), (10, vec![(BTC, Cause::Heartbeat)]), (20, vec![(BTC, Cause::Heartbeat)])]);
        assert!(ticks.iter().enumerate().all(|(index, tick)| tick.snapshot.is_some() == [0, 3, 10, 20].contains(&index)), "no snapshot sent unchanged");
        let first = data(&ticks[0], BTC);
        assert_eq!((first["schemaVersion"].as_u64(), first["coin"].as_str(), first["intervalMs"].as_u64(), first["publishedTsMs"].as_u64()), (Some(1), Some("BTC"), Some(5), Some(NOW_MS)));
        assert_eq!((first["external"]["externalBidPx"].as_str(), first["external"]["externalAskPx"].as_str()), (Some("100000"), Some("100001")));
        let changed = data(&ticks[3], BTC);
        assert_eq!((changed["external"]["externalBidPx"].as_str(), changed["publishedTsMs"].as_u64()), (Some("100000.1"), Some(NOW_MS + 15)));
        let versions: Vec<_> = [0, 3, 10, 20].iter().map(|index| entry(&ticks[*index], BTC).version).collect();
        assert_eq!(versions, [1, 2, 3, 4]);
        let sample = QuoteSample { received_ms: NOW_MS + 14, published_ms: NOW_MS + 15, published_at: entry(&ticks[3], BTC).sample.unwrap().published_at };
        assert_eq!((entry(&ticks[3], BTC).sample, &ticks[3].samples[..]), (Some(sample), &[(BTC, sample)][..]), "the move's receipt");
        assert!([0, 10, 20].iter().all(|index| entry(&ticks[*index], BTC).sample.is_none()), "a first frame or a heartbeat is no move");
        assert_eq!(data(&ticks[10], BTC)["publishedTsMs"].as_u64(), Some(NOW_MS + 50), "a heartbeat is a fresh frame");
    }

    /// Design 000673 §2: only the subscribed coins are judged, and only theirs are in the snapshot; a coin subscribed
    /// later is published at the next tick.
    #[tokio::test]
    async fn the_coins_without_a_subscriber_are_not_judged() {
        let mut fixture = Fixture::new(true);
        let start = Instant::now();
        let owner = fixture.owner(NOW_MS, start, None);
        let _btc = fixture.subscribe(BTC);
        let first = fixture.quote_tick(&owner, NOW_MS, start);
        assert_eq!((first.evaluated, first.snapshot.as_ref().unwrap().len()), (1, 1));
        assert!(first.snapshot.as_ref().unwrap().entry(ETH).is_none());
        let _gold = fixture.subscribe(GOLD);
        let second = fixture.quote_tick(&owner, NOW_MS + 5, start + QUOTE_TICK);
        assert_eq!((second.evaluated, causes(&second)), (2, vec![(GOLD, Cause::First)]));
    }

    /// Review 000673 r3 P2, design 000673 r4: GOLD's Binance connection down for 200 ticks - its reason naming each
    /// tick's time - and, connected, its same quote received anew every tick each publish 20 frames: the first and
    /// the heartbeats; the main dex's coverage withheld alike.
    #[tokio::test]
    async fn a_sustained_disconnect_or_same_price_refreshes_publish_only_the_heartbeat() {
        let frames = |ticks: &[Publication]| ticks.iter().map(|tick| tick.published.len()).sum::<usize>();
        let heartbeats = |ticks: &[Publication]| ticks.iter().flat_map(|tick| &tick.published).filter(|(_, cause)| *cause == Cause::Heartbeat).count();
        let mut fixture = Fixture::new(true);
        let (part, _) = fixture.feeds.hip3_external_feed().unwrap().part_of(0);
        part.slot.disconnected("EOF".to_string(), NOW_MS);
        let (binance, _) = fixture.main_slot(PERP_BINANCE, "BTCUSDT");
        binance.disconnected("EOF".to_string(), NOW_MS);
        let (_gold, _btc) = (fixture.subscribe(GOLD), fixture.subscribe(BTC));
        let down = fixture.ticks(Instant::now(), 200, |_, _| {});
        assert_eq!(data(&down[0], GOLD)["external"]["reasonCode"], "notConnected");
        assert_ne!(data(&down[0], GOLD)["external"]["reason"], data(&down[10], GOLD)["external"]["reason"], "the reason names the tick's time");
        assert_eq!(data(&down[0], BTC)["external"]["reasonCode"], "externalCoverage");
        assert_eq!((frames(&down), heartbeats(&down)), (40, 38), "GOLD's and BTC's first and 19 heartbeats each");
        let mut fixture = Fixture::new(true);
        let _gold = fixture.subscribe(GOLD);
        let refreshed = fixture.ticks(Instant::now(), 200, |fixture, tick| fixture.binance_quote(0, ("3900", "3901"), NOW_MS + 5 * u64::from(tick)));
        assert_eq!(data(&refreshed[0], GOLD)["external"]["status"], "ready");
        let received = |index: usize| data(&refreshed[index], GOLD)["external"]["externalQuotes"]["binance"]["receivedTsMs"].as_u64();
        assert_eq!((received(0), received(190)), (Some(NOW_MS), Some(NOW_MS + 950)), "the heartbeat brings the latest receipt");
        assert_eq!((frames(&refreshed), heartbeats(&refreshed)), (20, 19));
    }

    /// Review 000676 r1 P2: a frame published for a moved quote is sampled at most once a channel every 100 ms, its
    /// start the newest receipt of the moved leg's quotes - the channels apart -; the moves between are published,
    /// unsampled.
    #[tokio::test]
    async fn a_moved_quote_samples_its_frame_at_most_every_100_ms_a_channel() {
        let mut fixture = Fixture::new(true);
        fixture.btc_quotes(NOW_MS - 1_000);
        fixture.binance_quote(0, ("3900", "3901"), NOW_MS - 1_000);
        let (_btc, _gold) = (fixture.subscribe(BTC), fixture.subscribe(GOLD));
        let bid = |tick: u32| format!("{}", 100_000 + tick);
        let ticks = fixture.ticks(Instant::now(), 22, |fixture, tick| {
            if [1, 2, 21].contains(&tick) {
                fixture.main_quote(PERP_BINANCE, "BTCUSDT", (&bid(tick), "100100"), NOW_MS + 5 * u64::from(tick) - 1);
            }
            if tick == 2 {
                fixture.binance_quote(0, ("3900.5", "3901"), NOW_MS + 8);
            }
        });
        let sampled = |tick: usize| ticks[tick].samples.iter().map(|(key, sample)| (*key, sample.received_ms)).collect::<Vec<_>>();
        assert_eq!(causes(&ticks[2]), [(BTC, Cause::Changed), (GOLD, Cause::Changed)]);
        assert_eq!((sampled(1), sampled(2), sampled(21)), (vec![(BTC, NOW_MS + 4)], vec![(GOLD, NOW_MS + 8)], vec![(BTC, NOW_MS + 104)]));
        assert_eq!(causes(&ticks[21]), [(BTC, Cause::Changed)]);
    }

    /// Review 000676 r2 P2: Binance gone while OKX's quote, received 1,000 ms before, stands - the external bid moving
    /// from Binance's 100,000 to OKX's 100,000.5 - publishes that frame as any change, unsampled: no quote of it was
    /// received since the last tick; nor does it take the 100 ms sample's place: OKX's next quote, the tick after, is
    /// sampled from its receipt.
    #[tokio::test]
    async fn a_venue_gone_publishes_its_change_unsampled() {
        let mut fixture = Fixture::new(false);
        fixture.btc_quotes(NOW_MS - 1_000);
        let _btc = fixture.subscribe(BTC);
        let (binance, _) = fixture.main_slot(PERP_BINANCE, "BTCUSDT");
        let ticks = fixture.ticks(Instant::now(), 3, |fixture, tick| match tick {
            1 => binance.disconnected("EOF".to_string(), NOW_MS + 4),
            2 => fixture.main_quote(PERP_OKX, "BTC-USDT-SWAP", ("100000.6", "100001.5"), NOW_MS + 9),
            _ => {}
        });
        assert_eq!(causes(&ticks[1]), [(BTC, Cause::Changed)], "the venue gone, published");
        let external = data(&ticks[1], BTC)["external"].clone();
        assert_eq!((external["externalBidPx"].as_str(), external["externalQuotes"].as_object().unwrap().len()), (Some("100000.5"), 1));
        assert_eq!((entry(&ticks[1], BTC).sample, ticks[1].samples.len()), (None, 0));
        assert_eq!(causes(&ticks[2]), [(BTC, Cause::Changed)]);
        assert_eq!(ticks[2].samples.iter().map(|(key, sample)| (*key, sample.received_ms)).collect::<Vec<_>>(), [(BTC, NOW_MS + 9)]);
    }

    /// Design 000673 r4: the same SIP quote stamped 09:29:59.980 is TSLA's pre-market leg at 09:29:59.990, and at the
    /// next tick, 09:30:00.010, a pre-market quote in the regular session (`quoteOutsideSession`): published then, not
    /// at the heartbeat.
    #[tokio::test]
    async fn the_same_quote_across_the_regular_open_publishes_the_next_tick() {
        let mut fixture = Fixture::new(true);
        fixture.equity_quote(EquityFeed::Sip, 1, ("405.765", "405.775"), PRE_MARKET_END_MS - 10, PRE_MARKET_END_MS - 5);
        let _tsla = fixture.subscribe(TSLA);
        let start = Instant::now();
        let owner = fixture.owner(PRE_MARKET_END_MS, start, None);
        let pre = fixture.quote_tick(&owner, PRE_MARKET_END_MS, start);
        let equity = data(&pre, TSLA)["equity"].clone();
        assert_eq!((equity["status"].as_str(), equity["feed"].as_str(), equity["quoteTsMs"].as_u64()), (Some("ready"), Some("sip"), Some(PRE_MARKET_END_MS - 10)));
        let open = fixture.quote_tick(&owner, PRE_MARKET_END_MS + 20, start + Duration::from_millis(20));
        assert_eq!(causes(&open), [(TSLA, Cause::Changed)]);
        assert_eq!(data(&open, TSLA)["equity"]["reasonCode"], "quoteOutsideSession");
    }

    /// Review 000673 r3 P1, design 000673 r4: the owner's last snapshot standing - its channel open, its tick stalled -
    /// while the slots quote on, each leg's basis holds 200 ms after the owner computed it and is `basisStale` from 201
    /// ms, at once and at every heartbeat after, the quotes still published; the owner's next snapshot restores it.
    #[tokio::test]
    async fn the_owners_basis_holds_200_ms_then_every_leg_is_basis_stale() {
        let mut fixture = Fixture::new(true);
        fixture.btc_quotes(NOW_MS - 200_000);
        fixture.binance_quote(0, ("3900", "3901"), NOW_MS - 30);
        fixture.equity_quote(EquityFeed::Sip, 1, ("405.765", "405.775"), NOW_MS - 1_000, NOW_MS - 920);
        let start = Instant::now();
        let owner = (0..120_u32)
            .map(|second| {
                let now_ms = NOW_MS - 119_000 + 1_000 * u64::from(second);
                let book = fixture.book(now_ms);
                fixture.owner(now_ms, start + Duration::from_secs(u64::from(second)), Some(&book))
            })
            .last()
            .unwrap();
        let computed_at = owner.computed_at;
        let guards = [BTC, GOLD, TSLA].map(|key| fixture.subscribe(key));
        let at = |offset_ms: u64| (NOW_MS + offset_ms, computed_at + Duration::from_millis(offset_ms));
        let (published_ms, instant) = at(200);
        let held = fixture.quote_tick(&owner, published_ms, instant);
        let btc = data(&held, BTC)["external"].clone();
        assert_eq!((btc["externalBasis"]["status"].as_str(), btc["externalBasis"]["samples"].as_u64()), (Some("ready"), Some(120)));
        assert_eq!(data(&held, GOLD)["external"]["externalBasis"]["reasonCode"], "externalBasisSamples");
        assert_eq!(data(&held, TSLA)["equity"]["basis"]["reasonCode"], "externalBasisSamples");
        let (published_ms, instant) = at(201);
        let stale = fixture.quote_tick(&owner, published_ms, instant);
        assert_eq!(causes(&stale), [(BTC, Cause::Changed), (GOLD, Cause::Changed), (TSLA, Cause::Changed)]);
        assert_eq!(stale.samples, [], "the basis' move is no quote's: not sampled");
        let stale_legs = |publication: &Publication| {
            let (btc, gold, tsla) = (data(publication, BTC), data(publication, GOLD), data(publication, TSLA));
            assert_eq!((btc["external"]["status"].as_str(), btc["external"]["externalBidPx"].as_str()), (Some("ready"), Some("100000")), "the quote stands");
            assert_eq!((gold["external"]["status"].as_str(), tsla["equity"]["status"].as_str()), (Some("ready"), Some("ready")));
            [&btc["external"]["externalBasis"], &gold["external"]["externalBasis"], &tsla["equity"]["basis"]].map(|basis| basis["reasonCode"].as_str().unwrap().to_string())
        };
        assert_eq!(stale_legs(&stale), ["basisStale"; 3]);
        assert!(data(&stale, BTC)["external"]["externalBasis"]["reason"].as_str().unwrap().contains("201 ms ago"));
        let later: Vec<_> = (2..=10_u64).map(|tick| {
            let (published_ms, instant) = at(195 + 5 * tick);
            fixture.quote_tick(&owner, published_ms, instant)
        }).collect();
        assert!(later[..8].iter().all(|tick| tick.published.is_empty()));
        assert_eq!(causes(&later[8]), [(BTC, Cause::Heartbeat), (GOLD, Cause::Heartbeat), (TSLA, Cause::Heartbeat)]);
        assert_eq!(stale_legs(&later[8]), ["basisStale"; 3], "the heartbeat restores no basis");
        let (published_ms, instant) = at(250);
        let book = fixture.book(published_ms);
        let anew = fixture.owner(published_ms, instant, Some(&book));
        let restored = fixture.quote_tick(&anew, published_ms + 5, instant + QUOTE_TICK);
        assert!(causes(&restored).contains(&(BTC, Cause::Changed)));
        assert_eq!(data(&restored, BTC)["external"]["externalBasis"]["samples"].as_u64(), Some(120));
        drop(guards);
    }

    /// Design 000673 r4 首帧, review 000673 r4: subscribed, unsubscribed, the price moved while no one subscribed, and
    /// subscribed again - the next tick publishes the new price, forced, under the new epoch; the coin left the
    /// snapshot meanwhile. And the race: the last unsubscribe, the move and the resubscription all between a tick's
    /// read of the subscribers and its publication - that tick's frame, a heartbeat of the old price, is of the old
    /// epoch, which the new subscription never takes; the next tick's is the new price's, of the new epoch.
    #[tokio::test]
    async fn a_resubscription_takes_a_fresh_first_frame_never_an_old_ticks() {
        let mut fixture = Fixture::new(false);
        fixture.btc_quotes(NOW_MS - 1_000);
        let start = Instant::now();
        let owner = fixture.owner(NOW_MS, start, None);
        let at = |tick: u32| (NOW_MS + 5 * u64::from(tick), start + QUOTE_TICK * tick);
        let first = fixture.subscribe(BTC);
        let (published_ms, instant) = at(0);
        let hundred = fixture.quote_tick(&owner, published_ms, instant);
        assert_eq!((entry(&hundred, BTC).epoch, data(&hundred, BTC)["external"]["externalBidPx"].as_str()), (1, Some("100000")));
        drop(first);
        fixture.main_quote(PERP_BINANCE, "BTCUSDT", ("99990", "99991"), NOW_MS + 2);
        let (published_ms, instant) = at(1);
        let idle = fixture.quote_tick(&owner, published_ms, instant);
        assert_eq!((idle.evaluated, idle.snapshot.as_ref().unwrap().len()), (0, 0), "the coin left");
        let again = fixture.subscribe(BTC);
        let (published_ms, instant) = at(2);
        let fresh = fixture.quote_tick(&owner, published_ms, instant);
        assert_eq!(causes(&fresh), [(BTC, Cause::First)]);
        assert_eq!((entry(&fresh, BTC).epoch, again.epoch()), (2, 2));
        assert_eq!(data(&fresh, BTC)["external"]["externalBidPx"], "99990");
        for tick in 3..10 {
            let (published_ms, instant) = at(tick);
            assert_eq!(fixture.quote_tick(&owner, published_ms, instant).published, Vec::new());
        }
        // the race, at a heartbeat tick: the tick read epoch 2 and copied 99,990 before the clock
        let (binance, index) = fixture.main_slot(PERP_BINANCE, "BTCUSDT");
        let (published_ms, instant) = at(10);
        let mut again = Some(again);
        let mut resubscribed = None;
        let subscribers = Arc::clone(&fixture.tick.subscribers);
        let late = fixture.tick.tick(&owner, || {
            drop(again.take());
            binance.record(1, NOW_MS + 49, [(index, Some(quote("99980", "99981")), None)]);
            resubscribed = Some(subscribers.acquire(BTC));
            (published_ms, instant)
        });
        let resubscribed = resubscribed.unwrap();
        assert_eq!(causes(&late), [(BTC, Cause::Heartbeat)]);
        let old = entry(&late, BTC);
        assert_eq!((old.epoch, resubscribed.epoch(), data(&late, BTC)["external"]["externalBidPx"].as_str()), (2, 3, Some("99990")));
        let (published_ms, instant) = at(11);
        let next = fixture.quote_tick(&owner, published_ms, instant);
        assert_eq!(causes(&next), [(BTC, Cause::First)]);
        let new = entry(&next, BTC);
        assert_eq!((new.epoch, new.version > old.version, data(&next, BTC)["external"]["externalBidPx"].as_str()), (3, true, Some("99980")));
    }

    /// Design 000673 r4 读数: the slots are copied before the publish time is taken - a quote arriving between the two
    /// is the next tick's, the copied one judged at a time after its receipt, never from the future -; a quote received
    /// after the publish time (the wall clock stepped back) withholds the leg (`receivedAfterPublish`).
    #[tokio::test]
    async fn a_quote_arriving_during_the_read_is_the_next_ticks() {
        let mut fixture = Fixture::new(false);
        fixture.btc_quotes(NOW_MS - 10);
        let start = Instant::now();
        let owner = fixture.owner(NOW_MS, start, None);
        let _btc = fixture.subscribe(BTC);
        let (binance, index) = fixture.main_slot(PERP_BINANCE, "BTCUSDT");
        let read = fixture.tick.tick(&owner, || {
            binance.record(1, NOW_MS + 4, [(index, Some(quote("100000.2", "100001")), None)]);
            (NOW_MS + 5, start + QUOTE_TICK)
        });
        let external = data(&read, BTC)["external"].clone();
        assert_eq!((external["status"].as_str(), external["externalBidPx"].as_str()), (Some("ready"), Some("100000")));
        assert_eq!(external["externalQuotes"]["binance"]["receivedTsMs"].as_u64(), Some(NOW_MS - 10));
        let next = fixture.quote_tick(&owner, NOW_MS + 10, start + QUOTE_TICK * 2);
        assert_eq!(data(&next, BTC)["external"]["externalBidPx"], "100000.2");
        let stepped_back = fixture.quote_tick(&owner, NOW_MS + 3, start + QUOTE_TICK * 3);
        assert_eq!(data(&stepped_back, BTC)["external"]["reasonCode"], "receivedAfterPublish");
    }

    /// Design 000673 §4, review 000673 r1 P2: the venue map is whichever venues quote the coin - APEX on Bybit and
    /// MEXC, no Binance -, its medians of their weights (Bybit 2, MEXC 1).
    #[tokio::test]
    async fn a_coin_without_binance_quotes_on_its_own_venues() {
        let mut fixture = Fixture::new(false);
        fixture.main_quote(Feed { kind: Kind::Perp, venue: Venue::Bybit }, "APEXUSDT", ("0.5001", "0.5003"), NOW_MS - 20);
        fixture.main_quote(Feed { kind: Kind::Perp, venue: Venue::Mexc }, "APEX_USDT", ("0.5", "0.5002"), NOW_MS - 10);
        let start = Instant::now();
        let owner = fixture.owner(NOW_MS, start, None);
        let _apex = fixture.subscribe(APEX);
        let external = data(&fixture.quote_tick(&owner, NOW_MS + 5, start + QUOTE_TICK), APEX)["external"].clone();
        assert_eq!((external["status"].as_str(), external["externalBidPx"].as_str(), external["externalAskPx"].as_str()), (Some("ready"), Some("0.5001"), Some("0.5003")));
        let venues: Vec<_> = external["externalQuotes"].as_object().unwrap().keys().cloned().collect();
        assert_eq!(venues, ["bybit", "mexc"]);
        assert_eq!(external["externalQuotes"]["mexc"], serde_json::json!({"bidPx": "0.5", "askPx": "0.5002", "receivedTsMs": NOW_MS - 10}));
    }

    /// Design 000673 §4: the tick's frame writes the external fields as the owner's row of the same quotes and basis,
    /// byte for byte (the owner's are `extMarkPrices`' local leg's, `external_quotes_view`).
    #[tokio::test]
    async fn the_ticks_external_fields_are_the_owners() {
        let mut fixture = Fixture::new(false);
        fixture.btc_quotes(NOW_MS - 10);
        let start = Instant::now();
        let owner = fixture.owner(NOW_MS, start, None);
        let _btc = fixture.subscribe(BTC);
        let tick = fixture.quote_tick(&owner, NOW_MS + 5, start + QUOTE_TICK);
        let row = serde_json::to_vec(&ExternalQuotesView::of(&owner, "BTC", owner.coin("BTC").unwrap())).unwrap();
        assert_eq!(ready_external_fields(&entry(&tick, BTC).frame, 2), ready_external_fields(&row, 1));
    }

    /// Review 000673 r3 P1, design 000673 r4: the owner stopping (the feeds or the account owner gone) and the owner's
    /// snapshot channel closing each withdraw both channels - the tick's last snapshot names why - and end the tick.
    #[tokio::test]
    async fn the_owner_stopped_or_ended_withdraws_both_channels() {
        async fn withdrawn(quotes: &mut watch::Receiver<Arc<QuoteSnapshot>>) -> String {
            loop {
                let reason = quotes.borrow_and_update().withdrawn().map(str::to_string);
                if let Some(reason) = reason {
                    assert!(tokio::time::timeout(Duration::from_secs(5), quotes.changed()).await.unwrap().is_err(), "the tick ended");
                    return reason;
                }
                tokio::time::timeout(Duration::from_secs(5), quotes.changed()).await.unwrap().unwrap();
            }
        }
        use crate::local_marks::snapshot::tests::NOW_MS as WORLD_MS;
        let world = World::new();
        let (initial, stopped) = (world.snapshot(WORLD_MS), world.snapshot_stopped(WORLD_MS, "the account owner is gone"));
        let (owner, receiver) = watch::channel(Arc::new(initial));
        let feeds = Arc::new(LocalMarkFeeds::idle_for_test(Arc::new(world.sources), None));
        let handle = QuoteTickHandle::start(Arc::clone(&feeds), receiver.clone());
        let _btc = handle.subscribers.acquire(QuoteKey { channel: QuoteChannel::Main, index: 0 });
        let mut quotes = handle.subscribe();
        owner.send_replace(Arc::new(stopped));
        assert_eq!(withdrawn(&mut quotes).await, "the account owner is gone");
        let ended = QuoteTickHandle::start(feeds, receiver);
        let mut quotes = ended.subscribe();
        drop(owner);
        assert_eq!(withdrawn(&mut quotes).await, OWNER_TASK_ENDED);
    }
}
