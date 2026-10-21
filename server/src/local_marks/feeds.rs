//! The venue feed tasks (design 000454 r1 1.2, r2 revisions 4–5): one per feed of the sources, each owning its slot's
//! writes. A transient failure reconnects after a bounded backoff (0.5 s doubling to 10 s, reset once a connection
//! lasted 30 s); a contract error stops that feed - its quotes withdrawn, the error logged with the frame - and only
//! that feed: the order book and the accounts never wait on a venue.
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
    connection::{FeedError, Timeouts, run_connection},
    protocol::{Protocol, kucoin_session, mexc_spot_book_tickers},
    rest::RestClient,
    slot::{FeedReading, FeedSlot},
    sources::{Feed, FeedMarkets, Kind, MarkSources, Venue},
};
use crate::{
    local_marks::now_ms,
    metrics::{LOCAL_MARKS_FEED_DISCONNECTS_TOTAL, LOCAL_MARKS_FEED_FRAMES_TOTAL, LOCAL_MARKS_FEED_STOPPED},
};

const BACKOFF_MIN: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(10);
/// A connection that lasted this long resets the backoff.
const HEALTHY_FOR: Duration = Duration::from_secs(30);
/// MEXC spot is polled at this cadence; its quotes expire after `MEXC_SPOT_EXPIRY_MS`.
const MEXC_SPOT_POLL: Duration = Duration::from_millis(250);
const MEXC_SPOT_EXPIRY_MS: u64 = 5_000;

/// The feeds of the sources, their slots, and the supervisor of their tasks.
pub(crate) struct LocalMarkFeeds {
    pub(crate) sources: Arc<MarkSources>,
    slots: Vec<Arc<FeedSlot>>,
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
    /// Spawns a task per feed with markets, and their supervisor.
    pub(crate) fn start(sources: Arc<MarkSources>) -> Self {
        let mut slots = Vec::new();
        let mut tasks = JoinSet::new();
        let mut by_task = HashMap::new();
        for (feed, markets) in sources.feeds() {
            let expiry = (feed == (Feed { kind: Kind::Spot, venue: Venue::Mexc })).then_some(MEXC_SPOT_EXPIRY_MS);
            let slot = Arc::new(FeedSlot::new(feed, markets.symbols.len(), expiry));
            slots.push(Arc::clone(&slot));
            let sources = Arc::clone(&sources);
            let task = tasks.spawn(async move { run_feed(&sources, &slot).await });
            by_task.insert(task.id(), feed);
        }
        Self::supervised(sources, slots, tasks, by_task)
    }

    fn supervised(sources: Arc<MarkSources>, slots: Vec<Arc<FeedSlot>>, tasks: JoinSet<()>, by_task: HashMap<Id, Feed>) -> Self {
        let stopped = Arc::new(Mutex::new(None));
        let supervisor = tokio::spawn(supervise(tasks, by_task, slots.clone(), Arc::clone(&stopped))).abort_handle();
        Self { sources, slots, stopped, supervisor }
    }

    /// The feeds' slots without a task: nothing connects or quotes.
    #[cfg(test)]
    pub(crate) fn idle_for_test(sources: Arc<MarkSources>) -> Self {
        let slots = sources.feeds().map(|(feed, markets)| Arc::new(FeedSlot::new(feed, markets.symbols.len(), None))).collect();
        Self::supervised(sources, slots, JoinSet::new(), HashMap::new())
    }

    /// Why every feed stopped, once a task failed.
    pub(crate) fn stopped(&self) -> Option<String> {
        self.stopped.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// Every feed's reading now, with its markets.
    pub(crate) fn readings(&self, now_ms: u64) -> Vec<(Feed, &FeedMarkets, FeedReading)> {
        self.slots
            .iter()
            .map(|slot| {
                let markets = self.sources.markets(slot.feed).unwrap_or_else(|| unreachable!("a slot per feed of the sources"));
                (slot.feed, markets, slot.read(now_ms))
            })
            .collect()
    }
}

/// Waits for the tasks: one returning stopped its own feed; one failing stops them all.
async fn supervise(mut tasks: JoinSet<()>, by_task: HashMap<Id, Feed>, slots: Vec<Arc<FeedSlot>>, stopped: Arc<Mutex<Option<String>>>) {
    while let Some(ended) = tasks.join_next_with_id().await {
        let failure = match ended {
            Ok((id, ())) => {
                info!("local marks {}: its task ended after stopping the feed", by_task[&id].label());
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

fn failure(by_task: &HashMap<Id, Feed>, err: &JoinError) -> String {
    let label = by_task.get(&err.id()).map_or("an unknown feed", |feed| feed.label());
    format!("the {label} task failed: {err}")
}

async fn run_feed(sources: &MarkSources, slot: &FeedSlot) {
    let feed = slot.feed;
    let markets = sources.markets(feed).unwrap_or_else(|| unreachable!("a task per feed of the sources"));
    let label = feed.label();
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
                run_connection(protocol, session, markets, slot, Timeouts::FEEDS).await
            }
        };
        match error {
            FeedError::Contract(reason) => {
                error!("local marks {label} stopped until restart (its quotes withdrawn): {reason}");
                LOCAL_MARKS_FEED_STOPPED.with_label_values(&[label]).set(1);
                slot.stopped(reason, now_ms());
                return;
            }
            FeedError::Transient(reason) => {
                LOCAL_MARKS_FEED_DISCONNECTS_TOTAL.with_label_values(&[label]).inc();
                if started.elapsed() >= HEALTHY_FOR {
                    backoff = BACKOFF_MIN;
                }
                warn!("local marks {label} lost (its quotes withdrawn until quoted anew), retrying in {backoff:?}: {reason}");
                slot.disconnected(reason, now_ms());
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(BACKOFF_MAX);
            }
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
    run_connection(protocol, session, markets, slot, Timeouts::FEEDS).await
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
        let mut feeds = sources.feeds();
        let (first, first_markets) = feeds.next().unwrap();
        let (second, second_markets) = feeds.next().unwrap();
        let slots = vec![Arc::new(FeedSlot::new(first, first_markets.symbols.len(), None)), Arc::new(FeedSlot::new(second, second_markets.symbols.len(), None))];
        let mut tasks = JoinSet::new();
        let mut by_task = HashMap::new();
        let (scripted, idle) = (Arc::clone(&slots[0]), Arc::clone(&slots[1]));
        let handle = script(scripted);
        by_task.insert(tasks.spawn(async move { handle.await.unwrap_or_else(|err| std::panic::resume_unwind(err.into_panic())) }).id(), first);
        by_task.insert(
            tasks
                .spawn(async move {
                    let epoch = idle.connected(1);
                    idle.record(epoch, 2, [(0, Some(quote()), None)]);
                    std::future::pending::<()>().await;
                })
                .id(),
            second,
        );
        drop(feeds);
        LocalMarkFeeds::supervised(sources, slots, tasks, by_task)
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
            assert!(matches!(&reading.connection, Connection::Stopped { reason: stopped, .. } if *stopped == reason), "{:?}", reading.connection);
            assert_eq!(reading.entries[0].1, Validity::NotConnected);
        }
        // review 000455 r2: a poll of the other feed still running when its cancellation lands writes nothing
        let other = Arc::clone(&feeds.slots[1]);
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
        until(&feeds, |feeds| matches!(feeds.readings(3)[0].2.connection, Connection::Stopped { .. })).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(feeds.stopped(), None);
        let readings = feeds.readings(3);
        assert_eq!(readings[1].2.entries[0].1, Validity::Valid, "the other feed still quotes");
    }
}
