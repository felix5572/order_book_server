//! The external quote channels' side of a WS connection - `extExternalQuotes` (the main-dex coins', schema 2) and
//! `extHip3ExternalQuotes` (the HIP-3 coins', schema 2; hyperliquid-trade design 000673 r3 §3, r4 首帧) - and the
//! `GET /extExternalQuotes` and `GET /extHip3ExternalQuotes` handlers.
//!
//! The frames are the external quote tick's (`local_marks::quote_tick`), never the owner's. On each of its snapshots
//! a subscribed coin's frame is sent when it is of the subscription epoch this connection subscribed in and of a
//! version above the last one sent: a coin's frames rise in version, and none evaluated before the subscription is
//! sent. A subscription takes the coin's current frame at once when the coin was subscribed already, else the first
//! frame of the next completed tick, a fresh one; a slow connection skips versions, as it skipped ticks before. The
//! tick withdrawn (the owner stopped or ended) or its channel closed (the tick ended, a snapshot of it unread or not)
//! ends every subscription with the reason before any frame is forwarded (`forward`, review 000676 r1 P1), and refuses
//! new ones.
//!
//! Each channel answers a subscribe and an unsubscribe alike by whether the server serves it, then by its coins (design
//! 000664 r4, 000669 r1 1.1): `extHip3ExternalQuotes` off (`HIP3_EXTERNAL_NOT_ENABLED`) or a coin outside the union of
//! the Binance and SIP tables, `extExternalQuotes` without the local marks or a coin outside their sources - both get
//! the same refusal, never `Already unsubscribed`, so a client's unsubscribe still in flight past the refusal of its
//! subscribe meets the answer it already handled. A coin of the channel unsubscribed twice gets `Already unsubscribed`
//! as any. These refusals keep their text and carry the channel's `schemaVersion` beside it (design 000669 r2 1.4),
//! wherever the server answers them.
//!
//! The HTTP views read the owner's snapshot, every 50 ms, whether a connection subscribed or not.

use std::{collections::BTreeMap, sync::Arc};

use tokio::sync::watch::{self, error::RecvError};

use super::{
    Outbound,
    mark_prices::{MarkPricesQuery, error_response, rows_response},
    quote_delivery::{Handed, send_quote},
};
use crate::{
    local_marks::{
        external_quotes_view::{self, ExternalQuotesView},
        hip3::external_view::{self, Hip3ExternalView},
        owner::LocalMarks,
        quote_tick::{
            QUOTE_TICK_ENDED, QuoteTickHandle,
            snapshot::{QuoteChannel, QuoteEntry, QuoteKey, QuoteSnapshot},
            subscribers::SubscriberGuard,
        },
    },
    types::subscription::{ClientMessage, EXT_EXTERNAL_QUOTES, EXT_HIP3_EXTERNAL_QUOTES, ServerResponse, Subscription, SubscriptionManager},
};

/// `extHip3ExternalQuotes`' answer to a subscribe or an unsubscribe while the server runs without it, with or without
/// the local marks.
const HIP3_EXTERNAL_NOT_ENABLED: &str = "extHip3ExternalQuotes is not enabled (the server runs with --hip3-external-quotes?)";
/// `extExternalQuotes`' answer while the server runs without the local marks, or BBO-only.
const EXTERNAL_NOT_ENABLED: &str = "extExternalQuotes needs the server started with --local-marks";

/// The channel's answer wherever the server serves it not - its switch off, without the local marks, in BBO-only mode
/// -, its schema beside it.
pub(super) fn not_enabled(channel: QuoteChannel) -> bytes::Bytes {
    match channel {
        QuoteChannel::Main => external_quotes_view::refusal_wire(EXTERNAL_NOT_ENABLED),
        QuoteChannel::Hip3 => external_view::refusal_wire(HIP3_EXTERNAL_NOT_ENABLED),
    }
}

/// `extHip3ExternalQuotes`' answer to either of a coin outside the channel.
fn outside_hip3_external_sources(coin: &str) -> String {
    format!("{EXT_HIP3_EXTERNAL_QUOTES} {coin} is not in the HIP-3 external quote sources")
}

/// `extExternalQuotes`' answer to either of a coin outside the local marks' sources.
fn outside_main_sources(coin: &str) -> String {
    format!("{EXT_EXTERNAL_QUOTES} {coin} is not in the local marks' sources")
}

fn subscription(channel: QuoteChannel, coin: &str) -> Subscription {
    match channel {
        QuoteChannel::Main => Subscription::ExternalQuotes { coin: coin.to_string() },
        QuoteChannel::Hip3 => Subscription::Hip3ExternalQuotes { coin: coin.to_string() },
    }
}

/// A coin subscribed on this connection: its count among the coin's subscribers with the epoch it subscribed in, and
/// the version of the last frame sent.
#[derive(Debug)]
struct Subscribed {
    guard: SubscriberGuard,
    sent: Option<u64>,
}

impl Subscribed {
    /// A frame of this subscription's epoch, above the last one sent.
    fn due(&self, entry: &QuoteEntry) -> bool {
        entry.epoch == self.guard.epoch() && self.sent.is_none_or(|sent| entry.version > sent)
    }
}

/// A connection's `extExternalQuotes` and `extHip3ExternalQuotes` subscriptions.
pub(super) struct ExternalQuotesSession {
    quotes: QuoteTickHandle,
    snapshots: watch::Receiver<Arc<QuoteSnapshot>>,
    /// `extHip3ExternalQuotes` served (`--hip3-external-quotes`).
    hip3_enabled: bool,
    subscribed: BTreeMap<(QuoteChannel, String), Subscribed>,
}

impl ExternalQuotesSession {
    pub(super) fn new(marks: &LocalMarks) -> Self {
        let quotes = marks.quotes.clone();
        let snapshots = quotes.subscribe();
        Self { quotes, snapshots, hip3_enabled: marks.feeds.hip3_external.is_some(), subscribed: BTreeMap::new() }
    }

    /// The coin of the channel; the refusal of a subscribe or an unsubscribe of it before either: the channel off, the
    /// coin outside it.
    fn key(&self, channel: QuoteChannel, coin: &str) -> Result<QuoteKey, bytes::Bytes> {
        match channel {
            QuoteChannel::Main => {
                let index = self.quotes.table.index(coin).ok_or_else(|| external_quotes_view::refusal_wire(&outside_main_sources(coin)))?;
                Ok(QuoteKey { channel, index })
            }
            QuoteChannel::Hip3 => {
                if !self.hip3_enabled {
                    return Err(not_enabled(channel));
                }
                let index = self.quotes.table.hip3_external().index(coin).ok_or_else(|| external_view::refusal_wire(&outside_hip3_external_sources(coin)))?;
                Ok(QuoteKey { channel, index })
            }
        }
    }

    fn withdrawn(&self) -> Option<String> {
        withdrawal(&self.snapshots)
    }

    /// The tick's next snapshot while a coin is subscribed, pending otherwise; `Err` once the tick ended. Cancel-safe.
    pub(super) async fn next(&mut self) -> Result<Arc<QuoteSnapshot>, RecvError> {
        if self.subscribed.is_empty() {
            return std::future::pending().await;
        }
        self.snapshots.changed().await?;
        Ok(Arc::clone(&self.snapshots.borrow_and_update()))
    }

    /// Each subscribed coin's frame due in the snapshot, the withdrawal first (`forward`); the channel closed with its
    /// last snapshot read, its withdrawal. Returns false when the connection must be dropped.
    pub(super) async fn on_snapshot(&mut self, outbound: &Outbound, manager: &mut SubscriptionManager, snapshot: Result<Arc<QuoteSnapshot>, RecvError>) -> bool {
        let snapshot = snapshot.unwrap_or_else(|_| Arc::clone(&self.snapshots.borrow()));
        self.forward(outbound, manager, &snapshot).await
    }

    /// Every subscribed coin's frame due in `snapshot`, unless the tick withdrew or its channel closed - a snapshot of
    /// it unread or not -: then every subscription ends with the reason instead, no frame of a tick that ended sent
    /// before it (review 000676 r1 P1). Judged again for each frame with its queue slot in hand, so a tick ending
    /// while one waits for a slot sends neither it nor the coins after it (r2 P1); those enqueued before drain in
    /// order. A subscription's first frame and its later ones alike. Returns false when the connection must be dropped.
    async fn forward(&mut self, outbound: &Outbound, manager: &mut SubscriptionManager, snapshot: &QuoteSnapshot) -> bool {
        if snapshot.withdrawn().is_none() && self.withdrawn().is_none() {
            let Self { snapshots, subscribed, .. } = self;
            for ((channel, _), subscribed) in subscribed.iter_mut() {
                match send_due(outbound, *channel, snapshot, subscribed, || withdrawal(snapshots).is_some()).await {
                    None | Some(Handed::Enqueued) => {}
                    Some(Handed::Withdrawn) => break,
                    Some(Handed::Dropped) => return false,
                }
            }
        }
        let Some(reason) = snapshot.withdrawn().map(str::to_string).or_else(|| self.withdrawn()) else { return true };
        self.withdraw_all(outbound, manager, &reason).await
    }

    /// Subscribe / unsubscribe of `coin` on `channel`: a subscription takes the coin's current frame when it is of its
    /// epoch - the coin subscribed already -, else the next tick's. Returns false when the connection must be dropped.
    pub(super) async fn on_client_message(
        &mut self,
        outbound: &Outbound,
        manager: &mut SubscriptionManager,
        client_message: ClientMessage,
        channel: QuoteChannel,
        coin: &str,
    ) -> bool {
        let key = match self.key(channel, coin) {
            Ok(key) => key,
            Err(refusal) => return outbound.send_frame(refusal).await,
        };
        let subscription = subscription(channel, coin);
        // display only, as for the other subscriptions
        let sub = serde_json::to_string(&subscription).unwrap_or_default();
        if matches!(client_message, ClientMessage::Unsubscribe { .. }) {
            if !manager.unsubscribe(subscription) {
                return outbound.send_message(ServerResponse::Error(format!("Already unsubscribed: {sub}"))).await;
            }
            // its guard dropped: one subscriber less
            self.subscribed.remove(&(channel, coin.to_string()));
            return outbound.send_message(ServerResponse::SubscriptionResponse(client_message)).await;
        }
        if let Some(reason) = self.withdrawn() {
            return outbound.send_message(ServerResponse::Error(format!("Unable to grab {} snapshot: {reason}", channel.name()))).await;
        }
        match manager.subscribe(subscription) {
            Ok(true) => {}
            Ok(false) => return outbound.send_message(ServerResponse::Error(format!("Already subscribed: {sub}"))).await,
            Err(err) => return outbound.send_message(ServerResponse::Error(format!("Rejected subscription: {err}"))).await,
        }
        if !outbound.send_message(ServerResponse::SubscriptionResponse(client_message)).await {
            return false;
        }
        // counted before the current snapshot is read: a coin subscribed already has its epoch's frame there; a new
        // epoch's comes from a tick reading the count from now on. Not marked seen: the next snapshot is read whole.
        let subscribed = Subscribed { guard: self.quotes.subscribers.acquire(key), sent: None };
        self.subscribed.insert((channel, coin.to_string()), subscribed);
        let current = Arc::clone(&self.snapshots.borrow());
        self.forward(outbound, manager, &current).await
    }

    /// The tick withdrew or ended: every subscription ends with the reason.
    async fn withdraw_all(&mut self, outbound: &Outbound, manager: &mut SubscriptionManager, reason: &str) -> bool {
        for ((channel, coin), subscribed) in std::mem::take(&mut self.subscribed) {
            manager.unsubscribe(subscription(channel, &coin));
            drop(subscribed);
            let withdrawn = ServerResponse::Error(format!("{} {coin} withdrawn: {reason}", channel.name()));
            if !outbound.send_message(withdrawn).await {
                return false;
            }
        }
        true
    }
}

/// Why both channels are withdrawn: the tick's last snapshot names it, or its channel closed without one (a panic).
fn withdrawal(snapshots: &watch::Receiver<Arc<QuoteSnapshot>>) -> Option<String> {
    let named = snapshots.borrow().withdrawn().map(str::to_string);
    named.or_else(|| snapshots.has_changed().is_err().then(|| QUOTE_TICK_ENDED.to_string()))
}

/// Hands the subscribed coin's frame of `snapshot` to the connection's queue when it is due (`Subscribed::due`), unless
/// `withdrawn` says so once its slot is in hand (`quote_delivery::send_quote`); `None` when no frame is due.
async fn send_due(
    outbound: &Outbound,
    channel: QuoteChannel,
    snapshot: &QuoteSnapshot,
    subscribed: &mut Subscribed,
    withdrawn: impl FnOnce() -> bool,
) -> Option<Handed> {
    let entry = snapshot.entry(subscribed.guard.key()).filter(|entry| subscribed.due(entry))?;
    let handed = send_quote(outbound, channel, entry, withdrawn).await;
    if handed == Handed::Enqueued {
        subscribed.sent = Some(entry.version);
    }
    Some(handed)
}

/// `GET /extExternalQuotes[?coin=]`: the main-dex coins' external legs (schema 2) of the owner's snapshot, answered as
/// `GET /extMarkPrices`; 404 for a coin outside the sources, with the WS refusal's text; 503 once the owner task ended.
#[expect(clippy::unused_async, reason = "an axum handler")]
pub(super) async fn external_quotes_handler(
    axum::extract::Query(query): axum::extract::Query<MarkPricesQuery>,
    marks: LocalMarks,
) -> axum::response::Response {
    let snapshot = match marks.latest() {
        Ok(snapshot) => snapshot,
        Err(reason) => return error_response(axum::http::StatusCode::SERVICE_UNAVAILABLE, reason),
    };
    query.coin.as_deref().map_or_else(
        || rows_response(&snapshot.coins().map(|(coin, marks)| ExternalQuotesView::of(&snapshot, coin, marks)).collect::<Vec<_>>()),
        |coin| {
            snapshot.coin(coin).map_or_else(
                || error_response(axum::http::StatusCode::NOT_FOUND, &outside_main_sources(coin)),
                |marks| rows_response(&[ExternalQuotesView::of(&snapshot, coin, marks)]),
            )
        },
    )
}

/// `GET /extHip3ExternalQuotes[?coin=]`: the HIP-3 coins' external legs (schema 2), answered alike; 404 without
/// `--hip3-external-quotes` (off is not an empty set) or for a coin outside the channel, the WS channel's judgement.
#[expect(clippy::unused_async, reason = "an axum handler")]
pub(super) async fn hip3_external_quotes_handler(
    axum::extract::Query(query): axum::extract::Query<MarkPricesQuery>,
    marks: LocalMarks,
) -> axum::response::Response {
    if marks.feeds.hip3_external.is_none() {
        return error_response(axum::http::StatusCode::NOT_FOUND, "the server runs without --hip3-external-quotes");
    }
    let snapshot = match marks.latest() {
        Ok(snapshot) => snapshot,
        Err(reason) => return error_response(axum::http::StatusCode::SERVICE_UNAVAILABLE, reason),
    };
    query.coin.as_deref().map_or_else(
        || rows_response(&snapshot.hip3_external_coins().map(|(coin, legs)| Hip3ExternalView::of(&snapshot, coin, legs)).collect::<Vec<_>>()),
        |coin| {
            snapshot.hip3_external_coin(coin).map_or_else(
                || error_response(axum::http::StatusCode::NOT_FOUND, &outside_hip3_external_sources(coin)),
                |legs| rows_response(&[Hip3ExternalView::of(&snapshot, coin, legs)]),
            )
        },
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;

    use super::{super::mark_prices::tests::{json, marks, outbound}, *};
    use crate::local_marks::{
        feeds::LocalMarkFeeds,
        now_ms,
        quote_tick::subscribers::QuoteSubscribers,
        snapshot::{
            LocalMarksSnapshot,
            tests::{NOW_MS, World},
        },
    };

    const EXTERNAL: &str = r#"{"venue": "binance_perp", "coins": {"xyz:GOLD": {"symbol": "XAUUSDT", "pxMultiplier": "1"},
        "xyz:TSLA": {"symbol": "TSLAUSDT", "pxMultiplier": "1"}}}"#;
    /// TSLA in both tables, BB the SIP's only: the channel is BB, GOLD, TSLA.
    const SIP: &str = r#"{"feed": "sip", "coins": {"xyz:TSLA": {"symbol": "TSLA"}, "xyz:BB": {"symbol": "BB"}}}"#;
    const OFF: &str = "extHip3ExternalQuotes is not enabled (the server runs with --hip3-external-quotes?)";

    /// The real owner and external quote tick over idle feeds of `owner::tests::SOURCES` (BTC on Kraken spot and Gate
    /// perp) with the HIP-3 external quotes - Binance's xyz:GOLD and xyz:TSLA, its quotes held 60 s, and the SIP table
    /// of xyz:TSLA and xyz:BB -, the SIP and BOATS feeds running or not.
    fn external_marks(sip_feeds: bool) -> (LocalMarks, Arc<LocalMarkFeeds>, watch::Sender<crate::accounts::publish::NodePrices>) {
        use crate::local_marks::{
            feeds::Hip3ExternalConfig,
            hip3::{external_sources::Hip3ExternalSources, sources::Hip3SipSources},
            owner::tests,
            sources::MarkSources,
        };
        let sources = Arc::new(MarkSources::parse(tests::SOURCES).unwrap());
        let sip_sources = Arc::new(Hip3SipSources::parse(SIP).unwrap());
        let binance = Arc::new(Hip3ExternalSources::parse(EXTERNAL).unwrap());
        let external = Hip3ExternalConfig { sources: binance, sip_sources: Arc::clone(&sip_sources), quote_max_age_ms: 60_000 };
        let feeds = Arc::new(LocalMarkFeeds::idle_with_external_for_test(sources, sip_feeds.then_some(sip_sources), Some(external)));
        let (prices, receiver) = watch::channel(Ok(tests::node_prices()));
        (LocalMarks::start(Arc::clone(&feeds), receiver, tests::FreshBook), feeds, prices)
    }

    fn message(subscription: Subscription, subscribe: bool) -> ClientMessage {
        if subscribe { ClientMessage::Subscribe { subscription } } else { ClientMessage::Unsubscribe { subscription } }
    }

    fn external(coin: &str, subscribe: bool) -> ClientMessage {
        message(Subscription::Hip3ExternalQuotes { coin: coin.to_string() }, subscribe)
    }

    fn main(coin: &str, subscribe: bool) -> ClientMessage {
        message(Subscription::ExternalQuotes { coin: coin.to_string() }, subscribe)
    }

    fn quote(bid: &str, ask: &str) -> crate::local_marks::slot::Quote {
        use crate::local_marks::price::VenuePrice;
        crate::local_marks::slot::Quote { bid: VenuePrice::parse(bid).unwrap(), ask: VenuePrice::parse(ask).unwrap() }
    }

    /// The session's next frame of the tick's snapshots.
    async fn next_frame(session: &mut ExternalQuotesSession, outbound: &Outbound, manager: &mut SubscriptionManager, sent: &mut mpsc::Receiver<bytes::Bytes>) -> serde_json::Value {
        loop {
            let next = tokio::time::timeout(Duration::from_secs(5), session.next()).await.unwrap();
            assert!(session.on_snapshot(outbound, manager, next).await);
            if let Ok(frame) = sent.try_recv() {
                return json(&frame);
            }
        }
    }

    /// Design 000664 r4, 000669 r1 1.1 and r2 1.4, 000673 r3 §3: off, a subscribe and an unsubscribe of
    /// `extHip3ExternalQuotes` get the same answer; on, a coin outside both tables gets the same refusal for both -
    /// never `Already unsubscribed` -, while a coin of the channel unsubscribed twice gets it as any.
    /// `extExternalQuotes` alike, of a coin outside the local marks' sources. Each refusal is its text with its
    /// channel's `schemaVersion` beside it, byte for byte; the other channels' answers are unchanged. The SIP's only
    /// coin BB is the channel's though the SIP feeds are off, its first frame the tick's.
    #[tokio::test]
    async fn the_external_quotes_refuse_alike_when_off_or_outside_their_sources() {
        let refusal = |text: &str| serde_json::json!({"channel": "error", "data": text, "schemaVersion": 2});
        let (outbound, mut sent) = outbound();
        assert_eq!(&not_enabled(QuoteChannel::Hip3)[..], format!(r#"{{"channel":"error","data":"{OFF}","schemaVersion":2}}"#).as_bytes());
        let main_off = r#"{"channel":"error","data":"extExternalQuotes needs the server started with --local-marks","schemaVersion":2}"#;
        assert_eq!(&not_enabled(QuoteChannel::Main)[..], main_off.as_bytes());
        assert!(super::super::mark_prices::refuse_without_local_marks(&outbound, super::super::mark_prices::Market::Hip3).await);
        let other = serde_json::json!({"channel": "error", "data": "extHip3MarkPrices needs the server started with --local-marks"});
        assert_eq!(json(&sent.recv().await.unwrap()), other, "the other channels' answer unchanged");
        let (marks, _sender) = marks();
        let mut manager = SubscriptionManager::default();
        let mut session = ExternalQuotesSession::new(&marks);
        for subscribe in [true, false] {
            assert!(session.on_client_message(&outbound, &mut manager, external("xyz:TSLA", subscribe), QuoteChannel::Hip3, "xyz:TSLA").await);
            assert_eq!(json(&sent.recv().await.unwrap()), refusal(OFF));
        }
        for subscribe in [true, false, false] {
            assert!(session.on_client_message(&outbound, &mut manager, main("HYPE", subscribe), QuoteChannel::Main, "HYPE").await);
            let expected = r#"{"channel":"error","data":"extExternalQuotes HYPE is not in the local marks' sources","schemaVersion":2}"#;
            assert_eq!(&sent.recv().await.unwrap()[..], expected.as_bytes());
        }
        assert!(session.on_client_message(&outbound, &mut manager, main("BTC", false), QuoteChannel::Main, "BTC").await);
        assert!(json(&sent.recv().await.unwrap())["data"].as_str().unwrap().starts_with("Already unsubscribed"));
        let (marks, _feeds, _prices) = external_marks(false);
        let mut session = ExternalQuotesSession::new(&marks);
        for subscribe in [true, false, false] {
            assert!(session.on_client_message(&outbound, &mut manager, external("xyz:KIOXIA", subscribe), QuoteChannel::Hip3, "xyz:KIOXIA").await);
            let raw = sent.recv().await.unwrap();
            let expected = r#"{"channel":"error","data":"extHip3ExternalQuotes xyz:KIOXIA is not in the HIP-3 external quote sources","schemaVersion":2}"#;
            assert_eq!(&raw[..], expected.as_bytes());
        }
        assert!(manager.subscriptions().is_empty());
        assert!(session.on_client_message(&outbound, &mut manager, external("xyz:TSLA", false), QuoteChannel::Hip3, "xyz:TSLA").await);
        let twice = json(&sent.recv().await.unwrap());
        assert!(twice["data"].as_str().unwrap().starts_with("Already unsubscribed"), "{twice}");
        assert!(session.on_client_message(&outbound, &mut manager, external("xyz:BB", true), QuoteChannel::Hip3, "xyz:BB").await);
        assert_eq!(json(&sent.recv().await.unwrap())["channel"], "subscriptionResponse");
        let bb = next_frame(&mut session, &outbound, &mut manager, &mut sent).await;
        assert_eq!((bb["data"]["external"]["status"].as_str(), bb["data"]["equity"]["reasonCode"].as_str()), (Some("unlisted"), Some("equityOff")));
    }

    /// A coin of Binance's table, no one subscribed to it before, takes its first frame from the next tick
    /// (`intervalMs` 5), then its frames as they change: withheld while its connection is down, then Binance's quote,
    /// received no later than the frame's publish time (design 000664 r4); GOLD has no US stock leg.
    #[tokio::test]
    async fn a_coin_of_the_external_sources_takes_its_frames() {
        let (marks, feeds, _prices) = external_marks(false);
        let (outbound, mut sent) = outbound();
        let mut manager = SubscriptionManager::default();
        let mut session = ExternalQuotesSession::new(&marks);
        assert!(session.on_client_message(&outbound, &mut manager, external("xyz:GOLD", true), QuoteChannel::Hip3, "xyz:GOLD").await);
        assert_eq!(json(&sent.recv().await.unwrap())["channel"], "subscriptionResponse");
        assert!(sent.try_recv().is_err(), "a coin newly subscribed waits for the next tick");
        let first = next_frame(&mut session, &outbound, &mut manager, &mut sent).await;
        let head = (first["channel"].as_str(), first["data"]["schemaVersion"].as_u64(), first["data"]["intervalMs"].as_u64(), first["data"]["quoteMaxAgeMs"].as_u64());
        assert_eq!(head, (Some("extHip3ExternalQuotes"), Some(2), Some(5), Some(60_000)));
        assert_eq!(first["data"]["external"], serde_json::json!({"status": "unavailable", "reasonCode": "notConnected",
            "reason": first["data"]["external"]["reason"]}));
        assert_eq!(first["data"]["equity"], serde_json::json!({"status": "unlisted"}));
        let slot = &feeds.hip3_external_parts_for_test()[0].slot;
        let epoch = slot.connected(now_ms());
        let received_ms = now_ms();
        slot.record(epoch, received_ms, [(0, Some(quote("3900.5", "3901")), None)]);
        let ready = loop {
            let frame = next_frame(&mut session, &outbound, &mut manager, &mut sent).await;
            if frame["data"]["external"]["status"] == "ready" {
                break frame["data"].clone();
            }
        };
        assert_eq!((ready["coin"].as_str(), ready["external"]["externalBidPx"].as_str(), ready["external"]["externalAskPx"].as_str()), (Some("xyz:GOLD"), Some("3900.5"), Some("3901")));
        let quote = &ready["external"]["externalQuotes"]["binance"];
        assert_eq!((quote["bidPx"].as_str(), quote["receivedTsMs"].as_u64()), (Some("3900.5"), Some(received_ms)));
        assert!(received_ms <= ready["publishedTsMs"].as_u64().unwrap());
        assert_eq!(ready["external"]["externalBasis"]["reasonCode"], "externalBasisSamples");
        assert_eq!(ready["equity"], serde_json::json!({"status": "unlisted"}));
    }

    /// Design 000669 r1 1.2–1.3 through the real owner and tick, the SIP feeds on: TSLA, in both tables, takes both
    /// legs, each quote received no later than the frame's publish time. The US stock leg follows the wall clock's
    /// session: the quote of the feed counting at the publish time, `noSession` outside every session (the frame's own
    /// time decides which this run sees).
    #[tokio::test]
    async fn a_coin_of_both_tables_takes_both_legs() {
        use crate::local_marks::hip3::{
            protocol::EquityFeed,
            session::{equity_session, venue_session},
        };
        let (marks, feeds, _prices) = external_marks(true);
        let (outbound, mut sent) = outbound();
        let mut manager = SubscriptionManager::default();
        let mut session = ExternalQuotesSession::new(&marks);
        assert!(session.on_client_message(&outbound, &mut manager, external("xyz:TSLA", true), QuoteChannel::Hip3, "xyz:TSLA").await);
        assert_eq!(json(&sent.recv().await.unwrap())["channel"], "subscriptionResponse");
        assert_eq!(next_frame(&mut session, &outbound, &mut manager, &mut sent).await["data"]["schemaVersion"], 2);
        let quote_ms = now_ms();
        for feed in EquityFeed::ALL {
            let slot = &feeds.equity_parts_for_test(feed)[0].slot;
            let epoch = slot.connected(quote_ms);
            // TSLA is the SIP table's second market, after BB
            slot.record(epoch, quote_ms, [(1, Some(quote("405.765", "405.775")), Some(quote_ms))]);
        }
        let binance = &feeds.hip3_external_parts_for_test()[0].slot;
        let epoch = binance.connected(quote_ms);
        binance.record(epoch, quote_ms, [(1, Some(quote("405.7", "405.8")), None)]);
        let settled = |data: &serde_json::Value| {
            let equity_unread = data["equity"]["status"] == "unavailable" && ["notConnected", "notQuoted"].contains(&data["equity"]["reasonCode"].as_str().unwrap());
            data["external"]["status"] == "ready" && !equity_unread
        };
        let mut frames = 0;
        let data = loop {
            let data = next_frame(&mut session, &outbound, &mut manager, &mut sent).await["data"].clone();
            if settled(&data) {
                break data;
            }
            frames += 1;
            assert!(frames < 100, "{data}");
        };
        let published_ms = data["publishedTsMs"].as_u64().unwrap();
        assert!(data["external"]["externalQuotes"]["binance"]["receivedTsMs"].as_u64().unwrap() <= published_ms);
        let equity = &data["equity"];
        match equity_session(published_ms) {
            Some(session) if venue_session(quote_ms) == Some(session) => {
                assert_eq!(equity["status"], "ready", "{data}");
                assert_eq!(equity["feed"].as_str(), Some(EquityFeed::of(session.session).name()));
                assert_eq!((equity["bidPx"].as_str(), equity["askPx"].as_str()), (Some("405.765"), Some("405.775")));
                assert_eq!((equity["receivedTsMs"].as_u64(), equity["quoteTsMs"].as_u64()), (Some(quote_ms), Some(quote_ms)));
                assert!(quote_ms <= published_ms);
                assert_eq!(equity["basis"]["reasonCode"], "externalBasisSamples");
            }
            Some(_) => assert_eq!(equity["reasonCode"], "quoteOutsideSession", "the quote's session ended before the frame: {data}"),
            None => assert_eq!(equity["reasonCode"], "noSession", "{data}"),
        }
    }

    /// Design 000673 §4 through the real owner and tick: a main-dex coin's frames, `intervalMs` 5, withheld while no
    /// perp venue quotes it, then the venue's quote by its name - Gate's, no Binance -, received no later than the
    /// frame's publish time; the node state plays no part.
    #[tokio::test]
    async fn a_main_dex_coin_takes_its_external_frames() {
        let (marks, feeds, _prices) = external_marks(false);
        let (outbound, mut sent) = outbound();
        let mut manager = SubscriptionManager::default();
        let mut session = ExternalQuotesSession::new(&marks);
        assert!(session.on_client_message(&outbound, &mut manager, main("BTC", true), QuoteChannel::Main, "BTC").await);
        assert_eq!(json(&sent.recv().await.unwrap())["channel"], "subscriptionResponse");
        let first = next_frame(&mut session, &outbound, &mut manager, &mut sent).await;
        assert_eq!((first["channel"].as_str(), first["data"]["schemaVersion"].as_u64(), first["data"]["intervalMs"].as_u64()), (Some("extExternalQuotes"), Some(2), Some(5)));
        assert_eq!(first["data"]["external"]["reasonCode"], "externalCoverage");
        // the sources' feeds: spot Kraken, then perp Gate
        let (part, index) = feeds.main_feed(1).part_of(0);
        let epoch = part.slot.connected(now_ms());
        let received_ms = now_ms();
        part.slot.record(epoch, received_ms, [(index, Some(quote("100000", "100000.5")), None)]);
        let ready = loop {
            let frame = next_frame(&mut session, &outbound, &mut manager, &mut sent).await;
            if frame["data"]["external"]["status"] == "ready" {
                break frame["data"].clone();
            }
        };
        assert_eq!((ready["external"]["externalBidPx"].as_str(), ready["external"]["externalAskPx"].as_str()), (Some("100000"), Some("100000.5")));
        let basis = ready["external"]["externalQuotes"]["gate"]["basis"].clone();
        assert_eq!(basis["reasonCode"], "externalBasisSamples", "Gate's own basis: {basis}");
        assert_eq!(ready["external"]["externalQuotes"], serde_json::json!({"gate": {"bidPx": "100000", "askPx": "100000.5", "receivedTsMs": received_ms, "basis": basis}}));
        assert!(received_ms <= ready["publishedTsMs"].as_u64().unwrap());
    }

    /// Design 000673 r3 §3, r4 首帧 and review 000673 r4: a connection takes a frame of the epoch it subscribed in and
    /// above the last version it sent, so a coin's frames rise in version. A connection joining a coin subscribed
    /// already takes its current frame at once. After the last one left, a connection subscribing anew - a new epoch
    /// - takes neither the frame standing nor an old tick's published late under a higher version, only the next
    /// one of its epoch.
    #[tokio::test]
    async fn a_connection_takes_its_epochs_frames_in_rising_versions() {
        let world = World::new();
        let (owner, owner_snapshots) = watch::channel(Arc::new(world.snapshot(NOW_MS)));
        let table = Arc::clone(owner_snapshots.borrow().table());
        let feeds = Arc::new(LocalMarkFeeds::idle_for_test(Arc::new(world.sources), None));
        let subscribers = Arc::new(QuoteSubscribers::new(&table));
        let (quotes, quote_snapshots) = watch::channel(Arc::new(QuoteSnapshot::default()));
        let handle = QuoteTickHandle::of_snapshots_for_test(table, subscribers, quote_snapshots);
        let marks = LocalMarks::of_quotes_for_test(feeds, owner_snapshots, handle);
        let btc = QuoteKey { channel: QuoteChannel::Main, index: 0 };
        let publish = |version: u64, epoch: u32| {
            let frame = bytes::Bytes::from(format!(r#"{{"version":{version},"epoch":{epoch}}}"#));
            quotes.send_replace(Arc::new(QuoteSnapshot::of([(btc, QuoteEntry { version, epoch, frame, sample: None })])));
        };
        let (outbound, mut sent) = outbound();
        let (mut first_manager, mut second_manager, mut third_manager) = (SubscriptionManager::default(), SubscriptionManager::default(), SubscriptionManager::default());
        let mut first = ExternalQuotesSession::new(&marks);
        assert!(first.on_client_message(&outbound, &mut first_manager, main("BTC", true), QuoteChannel::Main, "BTC").await);
        assert_eq!(json(&sent.recv().await.unwrap())["channel"], "subscriptionResponse");
        assert!(sent.try_recv().is_err(), "no frame of its epoch yet");
        publish(5, 1);
        assert_eq!(deliver(&outbound, &mut sent, &mut first, &mut first_manager).await, Some(serde_json::json!({"version": 5, "epoch": 1})));
        publish(5, 1);
        assert_eq!(deliver(&outbound, &mut sent, &mut first, &mut first_manager).await, None, "a version sent is not sent again");
        publish(6, 1);
        assert_eq!(deliver(&outbound, &mut sent, &mut first, &mut first_manager).await, Some(serde_json::json!({"version": 6, "epoch": 1})));
        let mut second = ExternalQuotesSession::new(&marks);
        assert!(second.on_client_message(&outbound, &mut second_manager, main("BTC", true), QuoteChannel::Main, "BTC").await);
        assert_eq!(json(&sent.recv().await.unwrap())["channel"], "subscriptionResponse");
        assert_eq!(json(&sent.try_recv().unwrap()), serde_json::json!({"version": 6, "epoch": 1}), "the current frame at once");
        for (session, manager) in [(&mut first, &mut first_manager), (&mut second, &mut second_manager)] {
            assert!(session.on_client_message(&outbound, manager, main("BTC", false), QuoteChannel::Main, "BTC").await);
            assert_eq!(json(&sent.recv().await.unwrap())["channel"], "subscriptionResponse");
        }
        let mut third = ExternalQuotesSession::new(&marks);
        assert!(third.on_client_message(&outbound, &mut third_manager, main("BTC", true), QuoteChannel::Main, "BTC").await);
        assert_eq!(json(&sent.recv().await.unwrap())["channel"], "subscriptionResponse");
        assert!(sent.try_recv().is_err(), "the standing frame is of the old epoch");
        publish(7, 1);
        assert_eq!(deliver(&outbound, &mut sent, &mut third, &mut third_manager).await, None, "an old tick's frame, published late");
        publish(8, 2);
        assert_eq!(deliver(&outbound, &mut sent, &mut third, &mut third_manager).await, Some(serde_json::json!({"version": 8, "epoch": 2})));
        drop(owner);
    }

    /// Marks with the test world's owner snapshots and quote snapshots the test publishes, and their senders.
    fn quote_marks() -> (LocalMarks, watch::Sender<Arc<LocalMarksSnapshot>>, watch::Sender<Arc<QuoteSnapshot>>) {
        let world = World::new();
        let (owner, owner_snapshots) = watch::channel(Arc::new(world.snapshot(NOW_MS)));
        let table = Arc::clone(owner_snapshots.borrow().table());
        let feeds = Arc::new(LocalMarkFeeds::idle_for_test(Arc::new(world.sources), None));
        let subscribers = Arc::new(QuoteSubscribers::new(&table));
        let (quotes, quote_snapshots) = watch::channel(Arc::new(QuoteSnapshot::default()));
        let handle = QuoteTickHandle::of_snapshots_for_test(table, subscribers, quote_snapshots);
        (LocalMarks::of_quotes_for_test(feeds, owner_snapshots, handle), owner, quotes)
    }

    /// A snapshot of BTC's ready frame, version 1 of the first epoch.
    fn ready_btc() -> Arc<QuoteSnapshot> {
        ready(&["BTC"])
    }

    /// A snapshot of each of the test world's `coins` (BTC, ETH) ready, version 1 of the first epoch.
    fn ready(coins: &[&str]) -> Arc<QuoteSnapshot> {
        let entry = |coin: &str| {
            let frame = bytes::Bytes::from(format!(r#"{{"channel":"extExternalQuotes","data":{{"coin":"{coin}","external":{{"status":"ready"}}}}}}"#));
            let index = ["BTC", "ETH"].iter().position(|each| *each == coin).unwrap();
            (QuoteKey { channel: QuoteChannel::Main, index }, QuoteEntry { version: 1, epoch: 1, frame, sample: None })
        };
        Arc::new(QuoteSnapshot::of(coins.iter().map(|coin| entry(coin))))
    }

    /// Review 000676 r2 P1: BTC and ETH due in one snapshot on a full queue, BTC's frame waiting for a slot, and the
    /// tick's channel closing meanwhile: once the slot frees, neither BTC's nor ETH's ready frame is enqueued - each
    /// frame judged with its slot in hand -, only both withdrawals, after what the queue held.
    #[tokio::test(start_paused = true)]
    async fn the_tick_ending_while_a_frame_waits_for_a_slot_sends_no_later_coin() {
        let (marks, _owner, quotes) = quote_marks();
        let (data_tx, mut sent) = mpsc::channel(1);
        let (pong_tx, _pong_rx) = mpsc::channel(1);
        let outbound = Outbound::for_test(data_tx, pong_tx);
        let mut manager = SubscriptionManager::default();
        let mut session = ExternalQuotesSession::new(&marks);
        for coin in ["BTC", "ETH"] {
            assert!(session.on_client_message(&outbound, &mut manager, main(coin, true), QuoteChannel::Main, coin).await);
            assert_eq!(json(&sent.recv().await.unwrap())["channel"], "subscriptionResponse");
        }
        assert!(outbound.send_frame(bytes::Bytes::from_static(b"{}")).await, "the queue full");
        quotes.send_replace(ready(&["BTC", "ETH"]));
        let deliver = async {
            let next = session.next().await;
            session.on_snapshot(&outbound, &mut manager, next).await
        };
        // BTC's frame waits for the slot; the tick ends, then the queue drains, three messages
        let close = async {
            drop(quotes);
            let mut drained = Vec::new();
            while drained.len() < 3 {
                drained.push(sent.recv().await.unwrap());
            }
            drained
        };
        let (alive, drained) = tokio::join!(deliver, close);
        assert!(alive);
        assert_eq!(&drained[0][..], b"{}");
        let messages: Vec<_> = drained[1..].iter().map(|frame| json(frame)["data"].clone()).collect();
        assert_eq!(messages, [
            "extExternalQuotes BTC withdrawn: the external quote tick ended",
            "extExternalQuotes ETH withdrawn: the external quote tick ended",
        ]);
        assert!(sent.try_recv().is_err(), "no ready frame after them");
    }

    /// Review 000676 r1 P1: the tick's channel closed - the task ended without naming why - with its last snapshot, a
    /// ready frame, not yet read: the connection's first message after it is the withdrawal, never that frame.
    #[tokio::test]
    async fn the_tick_ending_with_a_frame_unread_withdraws_before_it() {
        let (marks, _owner, quotes) = quote_marks();
        let (outbound, mut sent) = outbound();
        let mut manager = SubscriptionManager::default();
        let mut session = ExternalQuotesSession::new(&marks);
        assert!(session.on_client_message(&outbound, &mut manager, main("BTC", true), QuoteChannel::Main, "BTC").await);
        assert_eq!(json(&sent.recv().await.unwrap())["channel"], "subscriptionResponse");
        quotes.send_replace(ready_btc());
        drop(quotes);
        let next = tokio::time::timeout(Duration::from_secs(5), session.next()).await.unwrap();
        assert!(next.is_ok(), "the unread snapshot is still read once");
        assert!(session.on_snapshot(&outbound, &mut manager, next).await);
        let first = json(&sent.try_recv().unwrap());
        assert_eq!((first["channel"].as_str(), first["data"].as_str()), (Some("error"), Some("extExternalQuotes BTC withdrawn: the external quote tick ended")));
        assert!(sent.try_recv().is_err(), "no frame after it");
        assert!(manager.subscriptions().is_empty());
    }

    /// Review 000676 r1 P1, a subscription's first frame: the tick's channel closing while the subscription's answer
    /// waits for a slot of a full queue - the coin's ready frame standing - withdraws it after its answer, without that
    /// frame.
    #[tokio::test]
    async fn the_tick_ending_during_a_subscription_withdraws_before_its_first_frame() {
        let (marks, _owner, quotes) = quote_marks();
        quotes.send_replace(ready_btc());
        let (data_tx, mut sent) = mpsc::channel(1);
        let (pong_tx, _pong_rx) = mpsc::channel(1);
        let outbound = Outbound::for_test(data_tx, pong_tx);
        assert!(outbound.send_frame(bytes::Bytes::from_static(b"{}")).await, "the queue full");
        let mut manager = SubscriptionManager::default();
        let mut session = ExternalQuotesSession::new(&marks);
        let subscribe = session.on_client_message(&outbound, &mut manager, main("BTC", true), QuoteChannel::Main, "BTC");
        // the subscription waits on the full queue; the tick ends, then the queue drains, three messages
        let close = async {
            drop(quotes);
            let mut drained = Vec::new();
            while drained.len() < 3 {
                drained.push(sent.recv().await.unwrap());
            }
            drained
        };
        let (alive, drained) = tokio::join!(subscribe, close);
        assert!(alive);
        assert_eq!(&drained[0][..], b"{}");
        let channels: Vec<_> = drained[1..].iter().map(|frame| json(frame)["channel"].as_str().unwrap().to_string()).collect();
        assert_eq!(channels, ["subscriptionResponse", "error"]);
        assert_eq!(json(&drained[2])["data"], "extExternalQuotes BTC withdrawn: the external quote tick ended");
        assert!(sent.try_recv().is_err(), "no frame after it");
    }

    /// Review 000676 r1 P2: `GET /extExternalQuotes` of an owner snapshot taken at 999,999 ms whose BTC quote was
    /// received at 1,000,000 ms - read after the snapshot's time - withholds BTC's leg (`receivedAfterPublish`), never
    /// a ready leg received after its `publishedTsMs`; `extMarkPrices`' local leg keeps its time as before.
    #[tokio::test]
    async fn the_external_quotes_http_view_withholds_a_quote_received_after_the_snapshot() {
        let world = World::new();
        let (_owner, owner_snapshots) = watch::channel(Arc::new(world.snapshot(NOW_MS - 1)));
        let feeds = Arc::new(LocalMarkFeeds::idle_for_test(Arc::new(world.sources), None));
        let marks = LocalMarks::of_snapshots_for_test(feeds, owner_snapshots);
        let (status, btc) = get(Handler::Main, &marks, Some("BTC")).await;
        assert_eq!(status, axum::http::StatusCode::OK);
        assert_eq!(btc[0]["publishedTsMs"].as_u64(), Some(999_999));
        let external = &btc[0]["external"];
        assert_eq!((external["status"].as_str(), external["reasonCode"].as_str()), (Some("unavailable"), Some("receivedAfterPublish")), "{external}");
        assert!(external["reason"].as_str().unwrap().contains("received at 1000000 ms, after the publish time 999999 ms"), "{external}");
        let query = axum::extract::Query(MarkPricesQuery { coin: Some("BTC".to_string()) });
        let response = super::super::mark_prices::mark_prices_handler(query, marks.clone()).await;
        let body: serde_json::Value = serde_json::from_slice(&axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
        let local = &body[0]["local"];
        assert_eq!((body[0]["publishedTsMs"].as_u64(), local["status"].as_str()), (Some(999_999), Some("ready")));
        assert_eq!(local["externalQuotes"]["binance"]["receivedTsMs"].as_u64(), Some(1_000_000), "the mark channel's time unchanged");
    }

    /// The session's handling of the tick's next snapshot: the frame it sent, if any.
    async fn deliver(outbound: &Outbound, sent: &mut mpsc::Receiver<bytes::Bytes>, session: &mut ExternalQuotesSession, manager: &mut SubscriptionManager) -> Option<serde_json::Value> {
        let next = tokio::time::timeout(Duration::from_secs(5), session.next()).await.unwrap();
        assert!(session.on_snapshot(outbound, manager, next).await);
        sent.try_recv().ok().map(|frame| json(&frame))
    }

    /// Review 000673 r3 P1, design 000673 r4: the owner stopped withdraws every external quote subscription with the
    /// reason, as the owner ending did, and refuses new ones; the mark channels are not this session's.
    #[tokio::test]
    async fn the_owner_stopped_withdraws_the_subscriptions() {
        let (marks, owner) = marks();
        let (outbound, mut sent) = outbound();
        let mut manager = SubscriptionManager::default();
        let mut session = ExternalQuotesSession::new(&marks);
        assert!(session.on_client_message(&outbound, &mut manager, main("BTC", true), QuoteChannel::Main, "BTC").await);
        assert_eq!(json(&sent.recv().await.unwrap())["channel"], "subscriptionResponse");
        assert_eq!(next_frame(&mut session, &outbound, &mut manager, &mut sent).await["channel"], "extExternalQuotes");
        owner.send_replace(Arc::new(World::new().snapshot_stopped(NOW_MS, "the account owner is gone")));
        let withdrawn = next_frame(&mut session, &outbound, &mut manager, &mut sent).await;
        assert_eq!((withdrawn["channel"].as_str(), withdrawn["data"].as_str()), (Some("error"), Some("extExternalQuotes BTC withdrawn: the account owner is gone")));
        assert!(manager.subscriptions().is_empty());
        assert!(session.on_client_message(&outbound, &mut manager, main("BTC", true), QuoteChannel::Main, "BTC").await);
        assert_eq!(json(&sent.recv().await.unwrap())["data"], "Unable to grab extExternalQuotes snapshot: the account owner is gone");
    }

    async fn get(handler: Handler, marks: &LocalMarks, coin: Option<&str>) -> (axum::http::StatusCode, serde_json::Value) {
        let query = axum::extract::Query(MarkPricesQuery { coin: coin.map(str::to_string) });
        let response = match handler {
            Handler::Main => external_quotes_handler(query, marks.clone()).await,
            Handler::Hip3 => hip3_external_quotes_handler(query, marks.clone()).await,
        };
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    }

    #[derive(Clone, Copy)]
    enum Handler {
        Main,
        Hip3,
    }

    /// `GET /extHip3ExternalQuotes`: 404 off; on, every coin of the channel in its order (schema 2, the owner's 50 ms),
    /// a coin outside it 404 with the WS refusal's text; BB, the SIP's only, without the SIP feeds: Binance
    /// `unlisted`, US stock off.
    #[tokio::test]
    async fn the_hip3_external_quotes_http_view_answers_404_off_or_outside_and_rows_on() {
        let (off, _sender) = marks();
        let (status, body) = get(Handler::Hip3, &off, None).await;
        assert_eq!((status, body["error"].as_str()), (axum::http::StatusCode::NOT_FOUND, Some("the server runs without --hip3-external-quotes")));
        let (marks, _feeds, _prices) = external_marks(false);
        let (status, all) = get(Handler::Hip3, &marks, None).await;
        assert_eq!(status, axum::http::StatusCode::OK);
        assert_eq!(all.as_array().unwrap().iter().map(|row| row["coin"].as_str().unwrap()).collect::<Vec<_>>(), ["xyz:BB", "xyz:GOLD", "xyz:TSLA"]);
        assert!(all.as_array().unwrap().iter().all(|row| row["schemaVersion"] == 2 && row["intervalMs"] == 50));
        let (status, bb) = get(Handler::Hip3, &marks, Some("xyz:BB")).await;
        assert_eq!((status, bb[0]["external"]["status"].as_str(), bb[0]["equity"]["reasonCode"].as_str()), (axum::http::StatusCode::OK, Some("unlisted"), Some("equityOff")));
        let (status, outside) = get(Handler::Hip3, &marks, Some("xyz:KIOXIA")).await;
        assert_eq!((status, outside["error"].as_str()), (axum::http::StatusCode::NOT_FOUND, Some("extHip3ExternalQuotes xyz:KIOXIA is not in the HIP-3 external quote sources")));
    }

    /// `GET /extExternalQuotes`: every main-dex coin's external leg of the owner's snapshot (schema 2, `intervalMs`
    /// 50), with no WS subscriber; a coin outside the sources 404 with the WS refusal's text; 503 once the owner ended.
    #[tokio::test]
    async fn the_external_quotes_http_view_answers_200_404_and_503() {
        let (marks, owner) = marks();
        let (status, all) = get(Handler::Main, &marks, None).await;
        assert_eq!(status, axum::http::StatusCode::OK);
        assert_eq!(all.as_array().unwrap().iter().map(|row| row["coin"].as_str().unwrap()).collect::<Vec<_>>(), ["BTC", "ETH"]);
        let (status, btc) = get(Handler::Main, &marks, Some("BTC")).await;
        assert_eq!(status, axum::http::StatusCode::OK);
        let btc = &btc[0];
        assert_eq!((btc["schemaVersion"].as_u64(), btc["intervalMs"].as_u64(), btc["publishedTsMs"].as_u64()), (Some(2), Some(50), Some(NOW_MS)));
        assert_eq!(btc["external"]["externalQuotes"]["binance"]["basis"]["reasonCode"], "externalBasisSamples", "each venue's basis in the rows too");
        assert_eq!((btc["external"]["status"].as_str(), btc["external"]["externalBidPx"].as_str()), (Some("ready"), Some("100003.95")));
        let (status, outside) = get(Handler::Main, &marks, Some("HYPE")).await;
        assert_eq!((status, outside["error"].as_str()), (axum::http::StatusCode::NOT_FOUND, Some("extExternalQuotes HYPE is not in the local marks' sources")));
        drop(owner);
        let (status, ended) = get(Handler::Main, &marks, Some("BTC")).await;
        assert_eq!((status, ended["error"].as_str()), (axum::http::StatusCode::SERVICE_UNAVAILABLE, Some("the local marks owner task ended")));
    }
}
