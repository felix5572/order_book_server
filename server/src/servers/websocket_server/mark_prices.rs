//! The local marks' side of a WS connection - `extMarkPrices` (design 000454 r1 1.4), `extHip3MarkPrices` (design
//! 000535 r2 revision 1) and `extHip3ExternalQuotes` (hyperliquid-trade design 000664): every tick, a frame of each
//! subscribed coin - and the `GET /extMarkPrices`, `GET /extMarkReferenceCaches`, `GET /extHip3MarkPrices` and
//! `GET /extHip3ExternalQuotes` handlers. The frames come from the local marks' own snapshot channel, never the book's
//! broadcast. The main-dex selectors read only the main-dex coins.
//!
//! `extHip3ExternalQuotes` answers a subscribe and an unsubscribe alike by its switch, then its sources (design 000664
//! r4): off, both get `HIP3_EXTERNAL_NOT_ENABLED`; a coin outside the loaded sources, both get the same refusal naming
//! it - never `Already unsubscribed` -, so a client's unsubscribe still in flight past the refusal of its subscribe
//! meets the answer it already handled. A coin of the sources unsubscribed twice gets `Already unsubscribed` as any.

use std::{collections::BTreeSet, sync::Arc};

use serde::Deserialize;
use serde_json::json;
use tokio::sync::watch::{self, error::RecvError};

use super::{Outbound, coin_positions::json_response};
use crate::{
    local_marks::{
        hip3::{external_sources::Hip3ExternalSources, external_view::Hip3ExternalView, view::Hip3MarksView},
        owner::{LocalMarks, OWNER_TASK_ENDED},
        prices_view::{CoinCachesView, CoinMarksView},
        snapshot::{CoinMarks, LocalMarksSnapshot},
    },
    types::subscription::{
        ClientMessage, EXT_HIP3_EXTERNAL_QUOTES, EXT_HIP3_MARK_PRICES, EXT_MARK_PRICES, ServerResponse, Subscription, SubscriptionManager,
    },
};

/// `extHip3ExternalQuotes`' answer to a subscribe or an unsubscribe while the server runs without it, with or without
/// the local marks.
const HIP3_EXTERNAL_NOT_ENABLED: &str = "extHip3ExternalQuotes is not enabled (the server runs with --hip3-external-quotes?)";

/// `extHip3ExternalQuotes`' answer wherever the server serves it not: its switch off, without the local marks, in
/// BBO-only mode.
pub(super) fn hip3_external_not_enabled() -> String {
    HIP3_EXTERNAL_NOT_ENABLED.to_string()
}

/// The answer to a subscribe or an unsubscribe of these channels while the server runs without the local marks: the
/// external quotes' is theirs with only their switch off (hyperliquid-trade design 000664 r4), the others' names the
/// switch.
pub(super) fn refusal_without_local_marks(market: Market) -> String {
    match market {
        Market::Hip3External => hip3_external_not_enabled(),
        Market::Main | Market::Hip3 => format!("{} needs the server started with --local-marks", market.channel()),
    }
}

/// Its answer to either of a coin outside the loaded sources.
fn outside_hip3_external_sources(coin: &str) -> String {
    format!("{EXT_HIP3_EXTERNAL_QUOTES} {coin} is not in the HIP-3 external quote sources")
}

/// Which channel a subscription is on: the main-dex coins', the HIP-3 SIP coins' or the HIP-3 coins' external
/// quotes'.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Market {
    Main,
    Hip3,
    Hip3External,
}

impl Market {
    const fn channel(self) -> &'static str {
        match self {
            Self::Main => EXT_MARK_PRICES,
            Self::Hip3 => EXT_HIP3_MARK_PRICES,
            Self::Hip3External => EXT_HIP3_EXTERNAL_QUOTES,
        }
    }

    fn subscription(self, coin: &str) -> Subscription {
        match self {
            Self::Main => Subscription::MarkPrices { coin: coin.to_string() },
            Self::Hip3 => Subscription::Hip3MarkPrices { coin: coin.to_string() },
            Self::Hip3External => Subscription::Hip3ExternalQuotes { coin: coin.to_string() },
        }
    }

    /// The coin's frame; `None` for a coin outside the market's sources.
    fn frame(self, snapshot: &LocalMarksSnapshot, coin: &str) -> Option<bytes::Bytes> {
        match self {
            Self::Main => snapshot.coin(coin).map(|marks| CoinMarksView::of(snapshot, coin, marks).to_wire()),
            Self::Hip3 => snapshot.hip3_coin(coin).map(|marks| Hip3MarksView::of(snapshot, coin, marks).to_wire()),
            Self::Hip3External => snapshot.hip3_external_coin(coin).map(|leg| Hip3ExternalView::of(snapshot, coin, leg).to_wire()),
        }
    }

    const fn sources(self) -> &'static str {
        match self {
            Self::Main => "the local marks' sources",
            Self::Hip3 => "the HIP-3 SIP sources (the server runs with --hip3-sip-marks?)",
            Self::Hip3External => "the HIP-3 external quote sources",
        }
    }
}

/// A connection's `extMarkPrices`, `extHip3MarkPrices` and `extHip3ExternalQuotes` subscriptions.
pub(super) struct MarkPricesSession {
    /// `None` once the owner task ended.
    snapshots: Option<watch::Receiver<Arc<LocalMarksSnapshot>>>,
    subscribed: BTreeSet<(Market, String)>,
    /// The loaded HIP-3 external sources; `None` without `--hip3-external-quotes`.
    hip3_external: Option<Arc<Hip3ExternalSources>>,
}

impl MarkPricesSession {
    pub(super) fn new(marks: &LocalMarks) -> Self {
        let hip3_external = marks.feeds.hip3_external.as_ref().map(|external| Arc::clone(&external.sources));
        Self { snapshots: Some(marks.subscribe()), subscribed: BTreeSet::new(), hip3_external }
    }

    /// Why a subscribe or an unsubscribe of the HIP-3 external `coin` is refused before either: the switch off, the
    /// coin outside the loaded sources.
    fn hip3_external_refusal(&self, coin: &str) -> Option<String> {
        let Some(sources) = &self.hip3_external else { return Some(HIP3_EXTERNAL_NOT_ENABLED.to_string()) };
        (!sources.coins().any(|each| each == coin)).then(|| outside_hip3_external_sources(coin))
    }

    /// The next snapshot while a coin is subscribed, pending otherwise; `Err` once the owner task ended. Cancel-safe.
    pub(super) async fn next(&mut self) -> Result<Arc<LocalMarksSnapshot>, RecvError> {
        let Some(snapshots) = self.snapshots.as_mut().filter(|_| !self.subscribed.is_empty()) else {
            return std::future::pending().await;
        };
        snapshots.changed().await?;
        Ok(Arc::clone(&snapshots.borrow_and_update()))
    }

    /// A frame of every subscribed coin. Returns false when the connection must be dropped.
    pub(super) async fn on_snapshot(
        &mut self,
        outbound: &Outbound,
        manager: &mut SubscriptionManager,
        snapshot: Result<Arc<LocalMarksSnapshot>, RecvError>,
    ) -> bool {
        let Ok(snapshot) = snapshot else {
            self.snapshots = None;
            return self.withdraw_all(outbound, manager).await;
        };
        for (market, coin) in &self.subscribed {
            let frame = market.frame(&snapshot, coin).unwrap_or_else(|| unreachable!("a subscribed coin is in the sources"));
            if !outbound.send_frame(frame).await {
                return false;
            }
        }
        true
    }

    /// Subscribe / unsubscribe of `coin`: a subscription takes the latest frame at once. Returns false when the
    /// connection must be dropped.
    pub(super) async fn on_client_message(
        &mut self,
        outbound: &Outbound,
        manager: &mut SubscriptionManager,
        client_message: ClientMessage,
        market: Market,
        coin: &str,
    ) -> bool {
        if let Some(refused) = (market == Market::Hip3External).then(|| self.hip3_external_refusal(coin)).flatten() {
            return outbound.send_message(ServerResponse::Error(refused)).await;
        }
        let subscription = market.subscription(coin);
        // display only, as for the other subscriptions
        let sub = serde_json::to_string(&subscription).unwrap_or_default();
        if matches!(client_message, ClientMessage::Unsubscribe { .. }) {
            if !manager.unsubscribe(subscription) {
                return outbound.send_message(ServerResponse::Error(format!("Already unsubscribed: {sub}"))).await;
            }
            self.subscribed.remove(&(market, coin.to_string()));
            return outbound.send_message(ServerResponse::SubscriptionResponse(client_message)).await;
        }
        let refused = |reason: &str| ServerResponse::Error(format!("Unable to grab {} snapshot: {reason}", market.channel()));
        let Some(snapshots) = self.snapshots.as_ref().filter(|snapshots| snapshots.has_changed().is_ok()) else {
            return outbound.send_message(refused(OWNER_TASK_ENDED)).await;
        };
        // not marked seen: the next tick's frames include this coin's, so no frame is skipped
        let snapshot = Arc::clone(&snapshots.borrow());
        let Some(frame) = market.frame(&snapshot, coin) else {
            return outbound.send_message(refused(&format!("{coin} is not in {}", market.sources()))).await;
        };
        match manager.subscribe(subscription) {
            Ok(true) => {}
            Ok(false) => return outbound.send_message(ServerResponse::Error(format!("Already subscribed: {sub}"))).await,
            Err(err) => return outbound.send_message(ServerResponse::Error(format!("Rejected subscription: {err}"))).await,
        }
        if !outbound.send_message(ServerResponse::SubscriptionResponse(client_message)).await {
            return false;
        }
        self.subscribed.insert((market, coin.to_string()));
        outbound.send_frame(frame).await
    }

    /// The owner task ended: every subscription ends with the reason.
    async fn withdraw_all(&mut self, outbound: &Outbound, manager: &mut SubscriptionManager) -> bool {
        for (market, coin) in std::mem::take(&mut self.subscribed) {
            manager.unsubscribe(market.subscription(&coin));
            let withdrawn = ServerResponse::Error(format!("{} {coin} withdrawn: {OWNER_TASK_ENDED}", market.channel()));
            if !outbound.send_message(withdrawn).await {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct MarkPricesQuery {
    coin: Option<String>,
}

/// `GET /extMarkPrices[?coin=]`: 200 with the coin's view, or every coin's, as a list; 404 for a coin outside the
/// sources; 503 once the owner task ended.
pub(super) async fn mark_prices_handler(
    axum::extract::Query(query): axum::extract::Query<MarkPricesQuery>,
    marks: LocalMarks,
) -> axum::response::Response {
    let snapshot = match marks.latest() {
        Ok(snapshot) => snapshot,
        Err(reason) => return error_response(axum::http::StatusCode::SERVICE_UNAVAILABLE, reason),
    };
    match queried(&query, &snapshot) {
        Ok(coins) => rows_response(&coins.map(|(coin, marks)| CoinMarksView::of(&snapshot, coin, marks)).collect::<Vec<_>>()),
        Err(error) => error_response(axum::http::StatusCode::NOT_FOUND, &error),
    }
}

/// `GET /extMarkReferenceCaches[?coin=]`: the reference caches' members of the same snapshot, answered alike.
pub(super) async fn reference_caches_handler(
    axum::extract::Query(query): axum::extract::Query<MarkPricesQuery>,
    marks: LocalMarks,
) -> axum::response::Response {
    let snapshot = match marks.latest() {
        Ok(snapshot) => snapshot,
        Err(reason) => return error_response(axum::http::StatusCode::SERVICE_UNAVAILABLE, reason),
    };
    match queried(&query, &snapshot) {
        Ok(coins) => rows_response(&coins.map(|(coin, marks)| CoinCachesView::of(&snapshot, coin, marks)).collect::<Vec<_>>()),
        Err(error) => error_response(axum::http::StatusCode::NOT_FOUND, &error),
    }
}

/// `GET /extHip3MarkPrices[?coin=]`: as `GET /extMarkPrices`, of the HIP-3 SIP coins; 404 without `--hip3-sip-marks`
/// (review 000536 P3: off is not an empty set).
#[expect(clippy::unused_async, reason = "an axum handler")]
pub(super) async fn hip3_mark_prices_handler(
    axum::extract::Query(query): axum::extract::Query<MarkPricesQuery>,
    marks: LocalMarks,
) -> axum::response::Response {
    if marks.feeds.hip3.is_none() {
        return error_response(axum::http::StatusCode::NOT_FOUND, "the server runs without --hip3-sip-marks");
    }
    let snapshot = match marks.latest() {
        Ok(snapshot) => snapshot,
        Err(reason) => return error_response(axum::http::StatusCode::SERVICE_UNAVAILABLE, reason),
    };
    query.coin.as_deref().map_or_else(
        || rows_response(&snapshot.hip3_coins().map(|(coin, marks)| Hip3MarksView::of(&snapshot, coin, marks)).collect::<Vec<_>>()),
        |coin| {
            snapshot.hip3_coin(coin).map_or_else(
                || error_response(axum::http::StatusCode::NOT_FOUND, &format!("{coin} is not in the HIP-3 SIP sources")),
                |marks| rows_response(&[Hip3MarksView::of(&snapshot, coin, marks)]),
            )
        },
    )
}

/// `GET /extHip3ExternalQuotes[?coin=]`: the HIP-3 coins' external quotes, answered alike; 404 without
/// `--hip3-external-quotes` (off is not an empty set) or for a coin outside its sources.
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
        || rows_response(&snapshot.hip3_external_coins().map(|(coin, leg)| Hip3ExternalView::of(&snapshot, coin, leg)).collect::<Vec<_>>()),
        |coin| {
            snapshot.hip3_external_coin(coin).map_or_else(
                || error_response(axum::http::StatusCode::NOT_FOUND, &outside_hip3_external_sources(coin)),
                |leg| rows_response(&[Hip3ExternalView::of(&snapshot, coin, leg)]),
            )
        },
    )
}

/// The queried main-dex coin, or every one; why not for a coin outside the sources.
fn queried<'s>(
    query: &'s MarkPricesQuery,
    snapshot: &'s LocalMarksSnapshot,
) -> Result<Box<dyn Iterator<Item = (&'s str, &'s CoinMarks)> + 's>, String> {
    let Some(coin) = query.coin.as_deref() else { return Ok(Box::new(snapshot.coins())) };
    let coin_marks = snapshot.coin(coin).ok_or_else(|| format!("{coin} is not in the local marks' sources"))?;
    Ok(Box::new(std::iter::once((coin, coin_marks))))
}

fn error_response(status: axum::http::StatusCode, error: &str) -> axum::response::Response {
    json_response(status, None, json!({"error": error}).to_string().into())
}

fn rows_response(rows: &impl serde::Serialize) -> axum::response::Response {
    #[expect(clippy::expect_used, reason = "string keys and finite numbers only; serializing cannot fail")]
    let body = serde_json::to_vec(rows).expect("the marks view serializes");
    json_response(axum::http::StatusCode::OK, None, body.into())
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use super::*;
    use crate::local_marks::{
        feeds::LocalMarkFeeds,
        snapshot::tests::{NOW_MS, World},
    };

    fn outbound() -> (Outbound, mpsc::Receiver<bytes::Bytes>) {
        let (data_tx, data_rx) = mpsc::channel(64);
        let (pong_tx, _pong_rx) = mpsc::channel(1);
        (Outbound::for_test(data_tx, pong_tx), data_rx)
    }

    fn subscribe(coin: &str) -> ClientMessage {
        ClientMessage::Subscribe { subscription: Subscription::MarkPrices { coin: coin.to_string() } }
    }

    fn json(bytes: &bytes::Bytes) -> serde_json::Value {
        serde_json::from_slice(bytes).unwrap()
    }

    /// Marks publishing the test world's snapshots, and their sender.
    fn marks() -> (LocalMarks, watch::Sender<Arc<LocalMarksSnapshot>>) {
        let world = World::new();
        let (sender, receiver) = watch::channel(Arc::new(world.snapshot(NOW_MS)));
        let feeds = Arc::new(LocalMarkFeeds::idle_for_test(Arc::new(world.sources), None));
        (LocalMarks::of_snapshots_for_test(feeds, receiver), sender)
    }

    #[tokio::test]
    async fn the_hip3_marks_answer_404_without_the_sip_feed() {
        let (marks, _sender) = marks();
        let response = hip3_mark_prices_handler(axum::extract::Query(MarkPricesQuery { coin: None }), marks).await;
        assert_eq!(response.status(), axum::http::StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn a_subscription_takes_the_latest_frame_then_each_tick_and_ends_with_the_owner() {
        let (marks, sender) = marks();
        let (outbound, mut sent) = outbound();
        let mut manager = SubscriptionManager::default();
        let mut session = MarkPricesSession::new(&marks);
        assert!(session.on_client_message(&outbound, &mut manager, subscribe("BTC"), Market::Main, "BTC").await);
        assert_eq!(json(&sent.recv().await.unwrap())["channel"], "subscriptionResponse");
        assert_eq!(json(&sent.recv().await.unwrap())["data"]["publishedTsMs"], NOW_MS);
        sender.send_replace(Arc::new(World::new().snapshot(NOW_MS + 50)));
        let next = session.next().await;
        assert!(session.on_snapshot(&outbound, &mut manager, next).await);
        assert_eq!(json(&sent.recv().await.unwrap())["data"]["publishedTsMs"], NOW_MS + 50);
        drop(sender);
        let ended = session.next().await;
        assert!(ended.is_err());
        assert!(session.on_snapshot(&outbound, &mut manager, ended).await);
        let withdrawn = json(&sent.recv().await.unwrap());
        assert_eq!((withdrawn["channel"].as_str(), withdrawn["data"].as_str()), (Some("error"), Some("extMarkPrices BTC withdrawn: the local marks owner task ended")));
        assert!(manager.subscriptions().is_empty());
        assert!(session.on_client_message(&outbound, &mut manager, subscribe("BTC"), Market::Main, "BTC").await);
        assert_eq!(json(&sent.recv().await.unwrap())["data"], "Unable to grab extMarkPrices snapshot: the local marks owner task ended");
    }

    #[tokio::test]
    async fn a_coin_outside_the_sources_is_refused_and_an_unsubscribed_one_gets_no_frame() {
        let (marks, sender) = marks();
        let (outbound, mut sent) = outbound();
        let mut manager = SubscriptionManager::default();
        let mut session = MarkPricesSession::new(&marks);
        assert!(session.on_client_message(&outbound, &mut manager, subscribe("HYPE"), Market::Main, "HYPE").await);
        assert_eq!(json(&sent.recv().await.unwrap())["data"], "Unable to grab extMarkPrices snapshot: HYPE is not in the local marks' sources");
        assert!(manager.subscriptions().is_empty());
        assert!(session.on_client_message(&outbound, &mut manager, subscribe("ETH"), Market::Main, "ETH").await);
        let unsubscribe = ClientMessage::Unsubscribe { subscription: Subscription::MarkPrices { coin: "ETH".to_string() } };
        assert!(session.on_client_message(&outbound, &mut manager, unsubscribe, Market::Main, "ETH").await);
        let kinds: Vec<_> = (0..3).map(|_| json(&sent.try_recv().unwrap())["channel"].as_str().unwrap().to_string()).collect();
        assert_eq!(kinds, ["subscriptionResponse", "extMarkPrices", "subscriptionResponse"]);
        sender.send_replace(Arc::new(World::new().snapshot(NOW_MS + 50)));
        let pending = tokio::time::timeout(std::time::Duration::from_millis(50), session.next()).await;
        assert!(pending.is_err(), "no subscription: no wake-up");
    }

    #[tokio::test]
    async fn the_http_view_answers_200_404_and_503() {
        async fn get(marks: &LocalMarks, coin: Option<&str>) -> (axum::http::StatusCode, serde_json::Value) {
            let query = MarkPricesQuery { coin: coin.map(str::to_string) };
            let response = mark_prices_handler(axum::extract::Query(query), marks.clone()).await;
            let status = response.status();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
            (status, serde_json::from_slice(&body).unwrap())
        }
        let (marks, sender) = marks();
        let (status, all) = get(&marks, None).await;
        assert_eq!((status, all.as_array().unwrap().len()), (axum::http::StatusCode::OK, 2));
        let (status, btc) = get(&marks, Some("BTC")).await;
        assert_eq!((status, btc[0]["local"]["referenceMarkPx"].as_str()), (axum::http::StatusCode::OK, Some("100002")));
        let (status, unknown) = get(&marks, Some("HYPE")).await;
        assert_eq!((status, unknown["error"].as_str()), (axum::http::StatusCode::NOT_FOUND, Some("HYPE is not in the local marks' sources")));
        let (status, caches) = get_caches(&marks, None).await;
        assert_eq!((status, caches.as_array().unwrap().len()), (axum::http::StatusCode::OK, 2));
        let (status, btc) = get_caches(&marks, Some("BTC")).await;
        assert_eq!((status, btc[0]["node"]["markCache"]["medianPx"].as_str()), (axum::http::StatusCode::OK, Some("100010")));
        let (status, unknown) = get_caches(&marks, Some("HYPE")).await;
        assert_eq!((status, unknown["error"].as_str()), (axum::http::StatusCode::NOT_FOUND, Some("HYPE is not in the local marks' sources")));
        drop(sender);
        let (status, ended) = get(&marks, Some("BTC")).await;
        assert_eq!((status, ended["error"].as_str()), (axum::http::StatusCode::SERVICE_UNAVAILABLE, Some("the local marks owner task ended")));
        let (status, ended) = get_caches(&marks, Some("BTC")).await;
        assert_eq!((status, ended["error"].as_str()), (axum::http::StatusCode::SERVICE_UNAVAILABLE, Some("the local marks owner task ended")));
    }

    const EXTERNAL: &str = r#"{"venue": "binance_perp", "coins": {"xyz:GOLD": {"symbol": "XAUUSDT", "pxMultiplier": "1"},
        "xyz:TSLA": {"symbol": "TSLAUSDT", "pxMultiplier": "1"}}}"#;

    /// The real owner over idle feeds with the HIP-3 external quotes of xyz:GOLD and xyz:TSLA, quotes held 60 s.
    fn external_marks() -> (LocalMarks, Arc<LocalMarkFeeds>, watch::Sender<crate::accounts::publish::NodePrices>) {
        use crate::local_marks::{feeds::Hip3ExternalConfig, hip3::external_sources::Hip3ExternalSources, owner::tests, sources::MarkSources};
        let sources = Arc::new(MarkSources::parse(tests::SOURCES).unwrap());
        let external = Hip3ExternalConfig { sources: Arc::new(Hip3ExternalSources::parse(EXTERNAL).unwrap()), quote_max_age_ms: 60_000 };
        let feeds = Arc::new(LocalMarkFeeds::idle_with_external_for_test(sources, None, Some(external)));
        let (prices, receiver) = watch::channel(Ok(tests::node_prices()));
        (LocalMarks::start(Arc::clone(&feeds), receiver, tests::FreshBook), feeds, prices)
    }

    fn external(coin: &str, subscribe: bool) -> ClientMessage {
        let subscription = Subscription::Hip3ExternalQuotes { coin: coin.to_string() };
        if subscribe { ClientMessage::Subscribe { subscription } } else { ClientMessage::Unsubscribe { subscription } }
    }

    /// Design 000664 r4: off, a subscribe and an unsubscribe get the same answer, without the local marks too; on, a
    /// coin outside the loaded sources gets the same refusal for both - never `Already unsubscribed`, which a client
    /// would take for a session error -, while a coin of the sources unsubscribed twice gets it as any.
    #[tokio::test]
    async fn the_external_quotes_refuse_alike_when_off_or_outside_their_sources() {
        let off = "extHip3ExternalQuotes is not enabled (the server runs with --hip3-external-quotes?)";
        assert_eq!(refusal_without_local_marks(Market::Hip3External), off);
        assert_eq!(refusal_without_local_marks(Market::Hip3), "extHip3MarkPrices needs the server started with --local-marks");
        let (marks, _sender) = marks();
        let (outbound, mut sent) = outbound();
        let mut manager = SubscriptionManager::default();
        let mut session = MarkPricesSession::new(&marks);
        for subscribe in [true, false] {
            assert!(session.on_client_message(&outbound, &mut manager, external("xyz:TSLA", subscribe), Market::Hip3External, "xyz:TSLA").await);
            assert_eq!(json(&sent.recv().await.unwrap())["data"], off);
        }
        let (marks, _feeds, _prices) = external_marks();
        let mut session = MarkPricesSession::new(&marks);
        for subscribe in [true, false, false] {
            assert!(session.on_client_message(&outbound, &mut manager, external("xyz:KIOXIA", subscribe), Market::Hip3External, "xyz:KIOXIA").await);
            assert_eq!(json(&sent.recv().await.unwrap())["data"], "extHip3ExternalQuotes xyz:KIOXIA is not in the HIP-3 external quote sources");
        }
        assert!(manager.subscriptions().is_empty());
        assert!(session.on_client_message(&outbound, &mut manager, external("xyz:TSLA", false), Market::Hip3External, "xyz:TSLA").await);
        let twice = json(&sent.recv().await.unwrap());
        assert!(twice["data"].as_str().unwrap().starts_with("Already unsubscribed"), "{twice}");
    }

    /// A coin of the sources takes its frame at once and each tick: withheld while its connection is down, then
    /// Binance's quote, received no later than the snapshot's publish time (design 000664 r4).
    #[tokio::test]
    async fn a_coin_of_the_external_sources_takes_its_frames() {
        let (marks, feeds, _prices) = external_marks();
        let (outbound, mut sent) = outbound();
        let mut manager = SubscriptionManager::default();
        let mut session = MarkPricesSession::new(&marks);
        assert!(session.on_client_message(&outbound, &mut manager, external("xyz:GOLD", true), Market::Hip3External, "xyz:GOLD").await);
        assert_eq!(json(&sent.recv().await.unwrap())["channel"], "subscriptionResponse");
        let first = json(&sent.recv().await.unwrap());
        assert_eq!((first["channel"].as_str(), first["data"]["schemaVersion"].as_u64(), first["data"]["quoteMaxAgeMs"].as_u64()), (Some("extHip3ExternalQuotes"), Some(1), Some(60_000)));
        assert_eq!(first["data"]["external"], serde_json::json!({"status": "unavailable", "reasonCode": "notConnected",
            "reason": first["data"]["external"]["reason"]}));
        let slot = &feeds.hip3_external_parts_for_test()[0].slot;
        let epoch = slot.connected(crate::local_marks::now_ms());
        let received_ms = crate::local_marks::now_ms();
        slot.record(epoch, received_ms, [(0, Some(crate::local_marks::slot::Quote {
            bid: crate::local_marks::price::VenuePrice::parse("3900.5").unwrap(),
            ask: crate::local_marks::price::VenuePrice::parse("3901").unwrap(),
        }), None)]);
        let ready = loop {
            let next = session.next().await;
            assert!(session.on_snapshot(&outbound, &mut manager, next).await);
            let frame = json(&sent.recv().await.unwrap());
            if frame["data"]["external"]["status"] == "ready" {
                break frame["data"].clone();
            }
        };
        assert_eq!((ready["coin"].as_str(), ready["external"]["externalBidPx"].as_str(), ready["external"]["externalAskPx"].as_str()), (Some("xyz:GOLD"), Some("3900.5"), Some("3901")));
        let quote = &ready["external"]["externalQuotes"]["binance"];
        assert_eq!((quote["bidPx"].as_str(), quote["receivedTsMs"].as_u64()), (Some("3900.5"), Some(received_ms)));
        assert!(received_ms <= ready["publishedTsMs"].as_u64().unwrap());
        assert_eq!(ready["external"]["externalBasis"]["reasonCode"], "externalBasisSamples");
    }

    #[tokio::test]
    async fn the_external_quotes_http_view_answers_404_off_or_outside_and_rows_on() {
        async fn get(marks: &LocalMarks, coin: Option<&str>) -> (axum::http::StatusCode, serde_json::Value) {
            let query = MarkPricesQuery { coin: coin.map(str::to_string) };
            let response = hip3_external_quotes_handler(axum::extract::Query(query), marks.clone()).await;
            let status = response.status();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
            (status, serde_json::from_slice(&body).unwrap())
        }
        let (off, _sender) = marks();
        let (status, body) = get(&off, None).await;
        assert_eq!((status, body["error"].as_str()), (axum::http::StatusCode::NOT_FOUND, Some("the server runs without --hip3-external-quotes")));
        let (marks, _feeds, _prices) = external_marks();
        let (status, all) = get(&marks, None).await;
        assert_eq!(status, axum::http::StatusCode::OK);
        assert_eq!(all.as_array().unwrap().iter().map(|row| row["coin"].as_str().unwrap()).collect::<Vec<_>>(), ["xyz:GOLD", "xyz:TSLA"]);
        let (status, outside) = get(&marks, Some("xyz:KIOXIA")).await;
        assert_eq!((status, outside["error"].as_str()), (axum::http::StatusCode::NOT_FOUND, Some("extHip3ExternalQuotes xyz:KIOXIA is not in the HIP-3 external quote sources")));
    }

    async fn get_caches(marks: &LocalMarks, coin: Option<&str>) -> (axum::http::StatusCode, serde_json::Value) {
        let query = MarkPricesQuery { coin: coin.map(str::to_string) };
        let response = reference_caches_handler(axum::extract::Query(query), marks.clone()).await;
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    }
}
