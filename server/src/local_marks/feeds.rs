//! The venue feed tasks (design 000454 r1 1.2, r2 revisions 4–5): one per connection partition, each owning its slot's
//! writes. A transient failure reconnects after a bounded backoff (0.5 s doubling to 10 s, reset once a connection
//! lasted 30 s); a contract error stops that partition - its quotes withdrawn, the error logged with the frame - and only
//! that partition: the order book and the accounts never wait on a venue.
//!
//! The tasks are supervised (review 000455 r1 P2-1): one ending without its own stop - a panic or a cancellation, an
//! internal error - stops every feed and withdraws every quote until restart, as the account view's owner ending
//! withdraws the view; the order book keeps serving. Dropping the feeds aborts every task.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use hyper::Method;
use log::{error, info, warn};
use tokio::{
    task::{AbortHandle, Id, JoinError, JoinSet},
    time::Instant,
};

use super::{
    connection::{ConnectionEnd, FeedError, Timeouts, run_connection},
    hip3::{
        protocol::{AlpacaCredentials, alpaca_session},
        sources::Hip3SipSources,
    },
    protocol::{Protocol, kucoin_session, mexc_spot_book_tickers},
    rest::RestClient,
    slot::FeedSlot,
    subscriptions::{FeedProjection, FeedSubscriptions, Subscription},
    sources::{Feed, FeedMarkets, Kind, MarkSources, Venue},
};
use crate::{
    local_marks::now_ms,
    metrics::{LOCAL_MARKS_FEED_DISCONNECTS_TOTAL, LOCAL_MARKS_FEED_FRAMES_TOTAL, LOCAL_MARKS_FEED_STOPPED, LOCAL_MARKS_CONNECTION_DISCONNECTS_TOTAL},
};

const BACKOFF_MIN: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(10);
/// A connection that lasted this long resets the backoff.
const HEALTHY_FOR: Duration = Duration::from_secs(30);
/// MEXC spot is polled at this cadence; its slot owns quote expiry.
const MEXC_SPOT_POLL: Duration = Duration::from_millis(250);
/// Alpaca 406 (the account's sessions in use) is retried this long from the first refusal of a failure that no
/// subscribed session ended (design 000535 r2 revision 5): an old session not yet released, then the SIP feed stops.
const SIP_CONNECTION_LIMIT_BUDGET: Duration = Duration::from_secs(60);

/// The HIP-3 SIP feed's configuration (design 000535): its markets and the Alpaca account.
pub(crate) struct SipFeedConfig {
    pub(crate) sources: Arc<Hip3SipSources>,
    pub(crate) credentials: AlpacaCredentials,
}

/// The feeds of the sources, their slots, and the supervisor of their tasks.
pub(crate) struct LocalMarkFeeds {
    pub(crate) sources: Arc<MarkSources>,
    subscriptions: Vec<FeedSubscriptions>,
    /// The HIP-3 SIP coins and their feed's one connection; `None` without `--hip3-sip-marks`.
    pub(crate) hip3: Option<Arc<Hip3SipSources>>,
    sip: Option<FeedSubscriptions>,
    /// Why every feed stopped (a task's internal failure), until restart.
    stopped: Arc<Mutex<Option<String>>>,
    supervisor: AbortHandle,
}

impl Drop for LocalMarkFeeds {
    /// The supervisor owns the tasks: aborting it drops their set, which aborts them.
    fn drop(&mut self) {
        self.supervisor.abort();
    }
}

impl LocalMarkFeeds {
    /// Spawns one supervised task per connection partition, with independent retry/quote epochs, and the SIP feed's
    /// when configured.
    pub(crate) fn start(sources: Arc<MarkSources>, sip: Option<SipFeedConfig>) -> Self {
        let subscriptions: Vec<_> = sources.feeds().map(|(feed, markets)| FeedSubscriptions::new(feed, markets)).collect();
        let mut tasks = JoinSet::new();
        let mut by_task = HashMap::new();
        for group in &subscriptions {
            for part in &group.subscriptions {
                let part = Arc::clone(part);
                let identity = (group.feed, part.shard);
                let task = tasks.spawn(async move { run_feed(&part).await });
                by_task.insert(task.id(), identity);
            }
        }
        let hip3 = sip.map(|SipFeedConfig { sources, credentials }| {
            let group = FeedSubscriptions::new(Feed::SIP, &sources.markets);
            for part in &group.subscriptions {
                let shard = part.shard;
                let (part, credentials) = (Arc::clone(part), credentials.clone());
                let task = tasks.spawn(async move { run_sip_feed(&part, &credentials).await });
                by_task.insert(task.id(), (Feed::SIP, shard));
            }
            (sources, group)
        });
        Self::supervised(sources, subscriptions, hip3, tasks, by_task)
    }

    fn supervised(
        sources: Arc<MarkSources>,
        subscriptions: Vec<FeedSubscriptions>,
        hip3: Option<(Arc<Hip3SipSources>, FeedSubscriptions)>,
        tasks: JoinSet<()>,
        by_task: HashMap<Id, (Feed, usize)>,
    ) -> Self {
        let stopped = Arc::new(Mutex::new(None));
        let (hip3, sip) = hip3.unzip();
        let slots = subscriptions.iter().chain(&sip).flat_map(|group| group.subscriptions.iter().map(|part| Arc::clone(&part.slot))).collect();
        let supervisor = tokio::spawn(supervise(tasks, by_task, slots, Arc::clone(&stopped))).abort_handle();
        Self { sources, subscriptions, hip3, sip, stopped, supervisor }
    }

    /// The feeds' slots without a task: nothing connects or quotes.
    #[cfg(test)]
    pub(crate) fn idle_for_test(sources: Arc<MarkSources>, hip3: Option<Arc<Hip3SipSources>>) -> Self {
        let subscriptions = sources.feeds().map(|(feed, markets)| FeedSubscriptions::new(feed, markets)).collect();
        let hip3 = hip3.map(|sources| {
            let group = FeedSubscriptions::new(Feed::SIP, &sources.markets);
            (sources, group)
        });
        Self::supervised(sources, subscriptions, hip3, JoinSet::new(), HashMap::new())
    }


    /// Why every feed stopped, once a task failed.
    pub(crate) fn stopped(&self) -> Option<String> {
        self.stopped.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// The SIP feed's reading now, in its markets' (the HIP-3 coins') order; `None` without it.
    pub(crate) fn sip_reading(&self, now_ms: u64) -> Option<FeedProjection> {
        self.sip.as_ref().map(|group| group.read(now_ms))
    }

    /// Every feed's reading now, with its markets.
    pub(crate) fn readings(&self, now_ms: u64) -> Vec<(Feed, &FeedMarkets, FeedProjection)> {
        self.subscriptions
            .iter()
            .map(|group| {
                let markets = self.sources.markets(group.feed).unwrap_or_else(|| unreachable!("a subscription group per feed of the sources"));
                (group.feed, markets, group.read(now_ms))
            })
            .collect()
    }
}

/// Waits for the tasks: one returning stopped its partition; an internal task failure stops them all.
async fn supervise(mut tasks: JoinSet<()>, by_task: HashMap<Id, (Feed, usize)>, slots: Vec<Arc<FeedSlot>>, stopped: Arc<Mutex<Option<String>>>) {
    while let Some(ended) = tasks.join_next_with_id().await {
        let failure = match ended {
            Ok((id, ())) => {
                let (feed, shard) = by_task[&id];
                info!("local marks {} shard {shard}: its task ended after stopping the partition", feed.label());
                continue;
            }
            Err(err) => failure(&by_task, &err),
        };
        error!("local marks stopped until restart, every quote withdrawn: {failure}");
        tasks.abort_all();
        for slot in &slots {
            slot.stopped(failure.clone(), now_ms());
        }
        *stopped.lock().unwrap_or_else(PoisonError::into_inner) = Some(failure);
        return;
    }
}

fn failure(by_task: &HashMap<Id, (Feed, usize)>, err: &JoinError) -> String {
    let label = by_task.get(&err.id()).map_or_else(|| "an unknown feed".to_string(), |(feed, shard)| format!("{} shard {shard}", feed.label()));
    format!("the {label} task failed: {err}")
}

async fn run_feed(part: &Subscription) {
    let (slot, markets, shard) = (&*part.slot, &part.markets, part.shard);
    let feed = slot.feed;
    let shard_label = shard.to_string();
    let mut backoff = BACKOFF_MIN;
    let mut kucoin = (feed.venue == Venue::Kucoin).then(|| RestClient::new("api.kucoin.com"));
    let mut mexc = (feed == Feed { kind: Kind::Spot, venue: Venue::Mexc }).then(|| RestClient::new("api.mexc.com"));
    loop {
        let started = Instant::now();
        let error = match (&mut kucoin, &mut mexc) {
            (_, Some(client)) => poll_mexc_spot(client, markets, slot).await,
            (Some(client), None) => connect_kucoin(client, markets, slot).await,
            (None, None) => {
                let protocol = Protocol { kind: feed.kind, venue: feed.venue };
                let session = protocol.session(&markets.symbols, now_ms() / 1_000, None);
                run_connection(protocol, session, markets, slot, Timeouts::FEEDS).await.error
            }
        };
        let (reason, requested) = match error {
            FeedError::Contract(reason) => return stop(slot, shard, &reason),
            // only the SIP feed's venue answers so (`run_sip_feed` budgets it)
            FeedError::Transient(reason) | FeedError::ConnectionLimit(reason) => (reason, false),
            FeedError::ReconnectRequested(reason) => (reason, true),
        };
        pause(slot, shard, &shard_label, started, &mut backoff, reason, requested).await;
    }
}

/// A contract error: this partition stops until restart, its quotes withdrawn.
fn stop(slot: &FeedSlot, shard: usize, reason: &str) {
    let label = slot.feed.label();
    error!("local marks {label} shard {shard} stopped until restart (this partition's quotes withdrawn): {reason}");
    LOCAL_MARKS_FEED_STOPPED.with_label_values(&[label]).set(1);
    slot.stopped(reason.to_string(), now_ms());
}

/// A lost connection: counted, logged, its quotes withdrawn, then the backoff (reset by a connection that lasted).
async fn pause(slot: &FeedSlot, shard: usize, shard_label: &str, started: Instant, backoff: &mut Duration, reason: String, requested: bool) {
    let label = slot.feed.label();
    LOCAL_MARKS_FEED_DISCONNECTS_TOTAL.with_label_values(&[label]).inc();
    LOCAL_MARKS_CONNECTION_DISCONNECTS_TOTAL.with_label_values(&[label, shard_label]).inc();
    if started.elapsed() >= HEALTHY_FOR {
        *backoff = BACKOFF_MIN;
    }
    if requested {
        info!("local marks {label} shard {shard} reconnect requested by venue (this partition's quotes withdrawn until quoted anew), retrying in {backoff:?}: {reason}");
    } else {
        warn!("local marks {label} shard {shard} lost (this partition's quotes withdrawn until quoted anew), retrying in {backoff:?}: {reason}");
    }
    slot.disconnected(reason, now_ms());
    tokio::time::sleep(*backoff).await;
    *backoff = (*backoff * 2).min(BACKOFF_MAX);
}

/// The SIP feed (design 000535 r2 revision 5): as `run_feed`, and Alpaca 406 within `SIP_CONNECTION_LIMIT_BUDGET` of
/// the first refusal since the last session that subscribed. Backoff and reconnects do not renew the budget; only a
/// session answered `authenticated` and `subscription` does.
async fn run_sip_feed(part: &Subscription, credentials: &AlpacaCredentials) {
    let (slot, markets, shard) = (&*part.slot, &part.markets, part.shard);
    let shard_label = shard.to_string();
    let protocol = Protocol { kind: Kind::Equity, venue: Venue::Alpaca };
    let mut backoff = BACKOFF_MIN;
    let mut budget = ConnectionLimitBudget::new(SIP_CONNECTION_LIMIT_BUDGET);
    loop {
        let started = Instant::now();
        let end = run_connection(protocol, alpaca_session(&markets.symbols, credentials), markets, slot, Timeouts::FEEDS).await;
        match budget.on_end(end, started, Instant::now()) {
            Ok(reason) => pause(slot, shard, &shard_label, started, &mut backoff, reason, false).await,
            Err(reason) => return stop(slot, shard, &reason),
        }
    }
}

/// The 406 budget across one failure (design 000535 r2 revision 5, review 000535 r2): it starts at the first refusal
/// since a session last subscribed, and only a subscribed session (authenticated and subscription answered) clears it;
/// reconnects and accepted handshakes do not.
#[derive(Debug)]
struct ConnectionLimitBudget {
    allowed: Duration,
    refused_since: Option<Instant>,
}

impl ConnectionLimitBudget {
    const fn new(allowed: Duration) -> Self {
        Self { allowed, refused_since: None }
    }

    /// The reason to reconnect with, or `Err` with the reason to stop: a contract error, or 406 past the budget.
    fn on_end(&mut self, end: ConnectionEnd, started: Instant, now: Instant) -> Result<String, String> {
        if end.subscribed {
            self.refused_since = None;
        }
        match end.error {
            FeedError::Contract(reason) => Err(reason),
            FeedError::ConnectionLimit(reason) => {
                let since = *self.refused_since.get_or_insert(started);
                let refused_for = now.saturating_duration_since(since);
                if refused_for >= self.allowed {
                    return Err(format!("refused for {refused_for:?} since the first refusal ({:?} allowed): {reason}", self.allowed));
                }
                Ok(reason)
            }
            FeedError::Transient(reason) | FeedError::ReconnectRequested(reason) => Ok(reason),
        }
    }
}

/// Kucoin's public token is fetched anew for every connection (it expires).
async fn connect_kucoin(client: &mut RestClient, markets: &FeedMarkets, slot: &FeedSlot) -> FeedError {
    let body = match client.call(Method::POST, "/api/v1/bullet-public").await {
        Ok(body) => body,
        Err(error) => return error,
    };
    let (url, interval) = match kucoin_session(&String::from_utf8_lossy(&body), now_ms()) {
        Ok(session) => session,
        Err(err) => return FeedError::Contract(format!("spot_kucoin: {err}")),
    };
    let protocol = Protocol { kind: Kind::Spot, venue: Venue::Kucoin };
    let session = protocol.session(&markets.symbols, now_ms() / 1_000, Some((&url, interval)));
    run_connection(protocol, session, markets, slot, Timeouts::FEEDS).await.error
}

/// MEXC spot's every-market book tickers; a successful answer after a failure starts a new epoch.
async fn poll_mexc_spot(client: &mut RestClient, markets: &FeedMarkets, slot: &FeedSlot) -> FeedError {
    let mut epoch = None;
    loop {
        let body = match client.call(Method::GET, "/api/v3/ticker/bookTicker").await {
            Ok(body) => body,
            Err(error) => return error,
        };
        let observations = match mexc_spot_book_tickers(&String::from_utf8_lossy(&body), markets) {
            Ok(observations) => observations,
            Err(err) => return FeedError::Contract(format!("spot_mexc: {err}")),
        };
        let now = now_ms();
        let epoch = *epoch.get_or_insert_with(|| slot.connected(now));
        slot.record(epoch, now, observations);
        LOCAL_MARKS_FEED_FRAMES_TOTAL.with_label_values(&["spot_mexc"]).inc();
        tokio::time::sleep(MEXC_SPOT_POLL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_marks::{price::VenuePrice, slot::{Connection, Quote, Validity}};

    fn quote() -> Quote {
        Quote { bid: VenuePrice::parse("100").unwrap(), ask: VenuePrice::parse("101").unwrap() }
    }

    /// Two feeds of the embedded sources with their slots; `script` runs as the first one's task, the second idles.
    fn feeds_with(script: impl FnOnce(Arc<FeedSlot>) -> tokio::task::JoinHandle<()> + Send + 'static) -> LocalMarkFeeds {
        let sources = Arc::new(MarkSources::embedded().unwrap());
        let first = Feed { kind: Kind::Perp, venue: Venue::Binance };
        let second = Feed { kind: Kind::Spot, venue: Venue::Okx };
        let first_markets = sources.markets(first).unwrap();
        let second_markets = sources.markets(second).unwrap();
        let subscriptions = vec![FeedSubscriptions::new(first, first_markets), FeedSubscriptions::new(second, second_markets)];
        let slots: Vec<_> = subscriptions.iter().map(|g| Arc::clone(&g.subscriptions[0].slot)).collect();
        let mut tasks = JoinSet::new();
        let mut by_task = HashMap::new();
        let (scripted, idle) = (Arc::clone(&slots[0]), Arc::clone(&slots[1]));
        let handle = script(scripted);
        by_task.insert(tasks.spawn(async move { handle.await.unwrap_or_else(|err| std::panic::resume_unwind(err.into_panic())) }).id(), (first, 0));
        by_task.insert(
            tasks
                .spawn(async move {
                    let epoch = idle.connected(1);
                    idle.record(epoch, 2, [(0, Some(quote()), None)]);
                    std::future::pending::<()>().await;
                })
                .id(),
            (second, 0),
        );
        LocalMarkFeeds::supervised(sources, subscriptions, None, tasks, by_task)
    }

    async fn until(feeds: &LocalMarkFeeds, done: impl Fn(&LocalMarkFeeds) -> bool + Sync) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !done(feeds) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }

    /// Review 000455 r1 P2-1: a task panicking after it quoted withdraws every quote of every feed until restart.
    #[tokio::test]
    async fn a_failed_task_stops_every_feed_and_withdraws_their_quotes() {
        let feeds = feeds_with(|slot| {
            tokio::spawn(async move {
                let epoch = slot.connected(1);
                slot.record(epoch, 2, [(0, Some(quote()), None)]);
                tokio::time::sleep(Duration::from_millis(50)).await;
                panic!("an internal invariant");
            })
        });
        until(&feeds, |feeds| feeds.readings(3).iter().all(|(_, _, reading)| reading.entries[0].1 == Validity::Valid)).await;
        until(&feeds, |feeds| feeds.stopped().is_some()).await;
        let reason = feeds.stopped().unwrap();
        assert!(reason.contains("task failed") && reason.contains("an internal invariant"), "{reason}");
        for (_, _, reading) in feeds.readings(3_600_000) {
            assert!(reading.connections.iter().all(|part| matches!(&part.connection, Connection::Stopped { reason: stopped, .. } if *stopped == reason)), "{:?}", reading.connections);
            assert!(reading.entries.iter().all(|(_, valid)| *valid == Validity::NotConnected));
        }
        // review 000455 r2: a poll of the other feed still running when its cancellation lands writes nothing
        let other = Arc::clone(&feeds.subscriptions[1].subscriptions[0].slot);
        let epoch = other.connected(4);
        other.record(epoch, 5, [(0, Some(quote()), None)]);
        assert_eq!(feeds.readings(3_600_000)[1].2.entries[0].1, Validity::NotConnected);
    }

    /// A feed stopping itself (a venue's contract error) stops only that feed.
    #[tokio::test]
    async fn a_feed_stopping_itself_leaves_the_others_running() {
        let feeds = feeds_with(|slot| {
            tokio::spawn(async move {
                slot.connected(1);
                slot.stopped("subscription refused".to_string(), 2);
            })
        });
        until(&feeds, |feeds| matches!(feeds.readings(3)[0].2.connections[0].connection, Connection::Stopped { .. })).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(feeds.stopped(), None);
        let readings = feeds.readings(3);
        assert_eq!(readings[1].2.entries[0].1, Validity::Valid, "the other feed still quotes");
    }
    /// Review 000535 r2: a refusal persisting past the budget stops the feed though every handshake was accepted; only a
    /// subscribed session clears the budget.
    #[test]
    fn a_persistent_connection_limit_stops_only_after_its_budget() {
        let start = Instant::now();
        let at = |seconds| start + Duration::from_secs(seconds);
        let refused = |subscribed| ConnectionEnd { error: FeedError::ConnectionLimit("Alpaca error 406".into()), subscribed };
        let mut budget = ConnectionLimitBudget::new(Duration::from_secs(60));
        assert!(budget.on_end(refused(false), at(0), at(1)).is_ok());
        assert!(budget.on_end(refused(false), at(30), at(31)).is_ok());
        assert!(budget.on_end(ConnectionEnd { error: FeedError::Transient("EOF".into()), subscribed: false }, at(40), at(41)).is_ok(), "a lost connection keeps the budget running");
        let stopped = budget.on_end(refused(false), at(59), at(60)).unwrap_err();
        assert!(stopped.contains("406") && stopped.contains("60s"), "{stopped}");
        // a subscribed session between two refusals starts the budget over
        let mut budget = ConnectionLimitBudget::new(Duration::from_secs(60));
        assert!(budget.on_end(refused(false), at(0), at(1)).is_ok());
        assert!(budget.on_end(ConnectionEnd { error: FeedError::Transient("EOF".into()), subscribed: true }, at(50), at(55)).is_ok());
        assert!(budget.on_end(refused(false), at(60), at(61)).is_ok());
        assert!(budget.on_end(refused(false), at(119), at(120)).is_err());
        assert!(ConnectionLimitBudget::new(Duration::from_secs(60)).on_end(ConnectionEnd { error: FeedError::Contract("401".into()), subscribed: false }, at(0), at(0)).is_err());
    }

    /// Review 000536 P2-1: `GET /extMarkInputs` shows the SIP feed and each HIP-3 coin's quote, also once the feed is lost
    /// or stopped (its reason, the last quote, no longer valid); a main-dex coin's answer has no `sip` section.
    #[tokio::test]
    async fn the_inputs_view_shows_the_sip_feed_and_its_quotes() {
        let main = Arc::new(MarkSources::embedded().unwrap());
        let hip3 = Arc::new(Hip3SipSources::parse(r#"{"feed": "sip", "coins": {"xyz:TSLA": {"symbol": "TSLA"}, "xyz:AAPL": {"symbol": "AAPL"}}}"#).unwrap());
        let feeds = LocalMarkFeeds::idle_for_test(Arc::clone(&main), Some(hip3));
        let slot = Arc::clone(&feeds.sip.as_ref().unwrap().subscriptions[0].slot);
        let epoch = slot.connected(1);
        slot.record(epoch, 2_080, [(1, Some(quote()), Some(2_000))]);
        let view = |coin: Option<&str>| serde_json::to_value(super::super::view::inputs_view(&feeds, coin, 3_000).unwrap()).unwrap();
        let all = view(None);
        assert_eq!(all["sip"]["feed"]["feed"], "equity_alpaca");
        assert_eq!(all["sip"]["feed"]["connection"], serde_json::json!({"state": "connected", "sinceMs": 1}));
        assert_eq!(all["sip"]["coins"].as_array().unwrap().len(), 2);
        let tsla = view(Some("xyz:TSLA"));
        assert_eq!(tsla["coins"], serde_json::json!([]));
        assert_eq!(tsla["sip"]["coins"], serde_json::json!([{"coin": "xyz:TSLA", "sip": {"symbol": "TSLA", "pxMultiplier": "1", "bid": "100", "ask": "101",
            "receivedTsMs": 2_080, "eventTsMs": 2_000, "status": "valid"}}]));
        assert!(view(Some("BTC")).get("sip").is_none(), "a main-dex coin's answer is unchanged");
        slot.stopped("Alpaca error 402 auth failed".to_string(), 2_500);
        let stopped = view(Some("xyz:TSLA"));
        assert_eq!(stopped["sip"]["feed"]["connection"], serde_json::json!({"state": "stopped", "reason": "Alpaca error 402 auth failed", "atMs": 2_500}));
        assert_eq!((stopped["sip"]["coins"][0]["sip"]["bid"].as_str(), stopped["sip"]["coins"][0]["sip"]["status"].as_str()), (Some("100"), Some("notConnected")));
        let without = LocalMarkFeeds::idle_for_test(main, None);
        assert!(serde_json::to_value(super::super::view::inputs_view(&without, None, 3_000).unwrap()).unwrap().get("sip").is_none());
        assert!(super::super::view::inputs_view(&without, Some("xyz:TSLA"), 3_000).is_err());
    }

    #[tokio::test]
    async fn the_public_view_keeps_one_venue_weight_and_exposes_partial_connection_loss() {
        let feeds = LocalMarkFeeds::idle_for_test(Arc::new(MarkSources::embedded().unwrap()), None);
        for group in &feeds.subscriptions {
            for part in &group.subscriptions {
                let epoch = part.slot.connected(1);
                part.slot.record(epoch, 2, (0..part.markets.symbols.len()).map(|i| (i, Some(quote()), None)));
            }
        }
        let binance = feeds.subscriptions.iter().find(|g| g.feed == (Feed { kind: Kind::Perp, venue: Venue::Binance })).unwrap();
        let lost_coin = &binance.subscriptions[1].markets.coins[0].0;
        let healthy_coin = &binance.subscriptions[0].markets.coins[0].0;
        let view = |coin: &str| serde_json::to_value(super::super::view::inputs_view(&feeds, Some(coin), 3).unwrap()).unwrap();
        let before = view(lost_coin);
        let weight = before["coins"][0]["externalCoverage"]["usedWeight"].as_u64().unwrap();
        assert_eq!(before["feeds"].as_array().unwrap().iter().filter(|f| f["feed"] == "perp_binance").count(), 1);
        assert_eq!(before["coins"][0]["externalCoverage"]["usedVenues"].as_array().unwrap().iter().filter(|v| **v == "binance").count(), 1);
        binance.subscriptions[1].slot.disconnected("EOF".into(), 3);
        let after = view(lost_coin);
        let feed = after["feeds"].as_array().unwrap().iter().find(|f| f["feed"] == "perp_binance").unwrap();
        assert_eq!(feed["connection"], serde_json::json!({"state":"degraded","connected":6,"total":7}));
        assert_eq!(feed["connections"][1]["connection"]["state"], "disconnected");
        assert_eq!(feed["quoting"], 172);
        assert_eq!(after["coins"][0]["perp"]["binance"]["status"], "notConnected");
        assert_eq!(after["coins"][0]["externalCoverage"]["usedWeight"], weight - 3);
        assert_eq!(view(healthy_coin)["coins"][0]["perp"]["binance"]["status"], "valid");
        binance.subscriptions[1].slot.connected(4);
        let reconnected = view(lost_coin);
        let feed = reconnected["feeds"].as_array().unwrap().iter().find(|f| f["feed"] == "perp_binance").unwrap();
        assert_eq!(feed["connection"], serde_json::json!({"state":"connected","sinceMs":4}));
        assert_eq!(reconnected["coins"][0]["perp"]["binance"]["status"], "notQuoted");
    }

}
