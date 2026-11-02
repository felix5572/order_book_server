//! The local marks' side of a WS connection - `extMarkPrices` (design 000454 r1 1.4), `extHip3MarkPrices` (design
//! 000535 r2 revision 1) and `extHip3ExternalQuotes` (hyperliquid-trade design 000664): every tick, a frame of each
//! subscribed coin - and the `GET /extMarkPrices`, `GET /extMarkReferenceCaches`, `GET /extHip3MarkPrices` and
//! `GET /extHip3ExternalQuotes` handlers. The frames come from the local marks' own snapshot channel, never the book's
//! broadcast. The main-dex selectors read only the main-dex coins.
//!
//! `extHip3ExternalQuotes` answers a subscribe and an unsubscribe alike by its switch, then its coins (design 000664
//! r4, 000669 r1 1.1: the union of the Binance and SIP tables, whichever feeds run): off, both get
//! `HIP3_EXTERNAL_NOT_ENABLED`; a coin outside the channel, both get the same refusal naming it - never `Already
//! unsubscribed` -, so a client's unsubscribe still in flight past the refusal of its subscribe meets the answer it
//! already handled. A coin of the channel unsubscribed twice gets `Already unsubscribed` as any. Both refusals keep
//! their text and carry `"schemaVersion":2` beside it (design 000669 r2 1.4), wherever the server answers them.

use std::{collections::BTreeSet, sync::Arc};

use serde::Deserialize;
use serde_json::json;
use tokio::sync::watch::{self, error::RecvError};

use super::{Outbound, coin_positions::json_response};
use crate::{
    local_marks::{
        hip3::{
            external_coins::Hip3ExternalCoins,
            external_view::{Hip3ExternalView, refusal_wire},
            view::Hip3MarksView,
        },
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

/// `extHip3ExternalQuotes`' answer wherever the server serves it not - its switch off, without the local marks, in
/// BBO-only mode -, its schema beside it.
pub(super) fn hip3_external_not_enabled() -> bytes::Bytes {
    refusal_wire(HIP3_EXTERNAL_NOT_ENABLED)
}

/// Answers a subscribe or an unsubscribe of these channels while the server runs without the local marks: the external
/// quotes' as with only their switch off (hyperliquid-trade design 000664 r4), the others' naming the switch. Returns
/// false when the connection must be dropped.
pub(super) async fn refuse_without_local_marks(outbound: &Outbound, market: Market) -> bool {
    match market {
        Market::Hip3External => outbound.send_frame(hip3_external_not_enabled()).await,
        Market::Main | Market::Hip3 => {
            outbound.send_message(ServerResponse::Error(format!("{} needs the server started with --local-marks", market.channel()))).await
        }
    }
}

/// Its answer to either of a coin outside the channel.
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
            Self::Hip3External => snapshot.hip3_external_coin(coin).map(|legs| Hip3ExternalView::of(snapshot, coin, legs).to_wire()),
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
    /// The HIP-3 external quotes' coins; `None` without `--hip3-external-quotes`.
    hip3_external: Option<Arc<Hip3ExternalCoins>>,
}

impl MarkPricesSession {
    pub(super) fn new(marks: &LocalMarks) -> Self {
        let hip3_external = marks.feeds.hip3_external.as_ref().map(|external| Arc::clone(&external.coins));
        Self { snapshots: Some(marks.subscribe()), subscribed: BTreeSet::new(), hip3_external }
    }

    /// Why a subscribe or an unsubscribe of the HIP-3 external `coin` is refused before either: the switch off, the
    /// coin outside the channel.
    fn hip3_external_refusal(&self, coin: &str) -> Option<String> {
        let Some(coins) = &self.hip3_external else { return Some(HIP3_EXTERNAL_NOT_ENABLED.to_string()) };
        coins.index(coin).is_none().then(|| outside_hip3_external_sources(coin))
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
            return outbound.send_frame(refusal_wire(&refused)).await;
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
    /// TSLA in both tables, BB the SIP's only: the channel is BB, GOLD, TSLA.
    const SIP: &str = r#"{"feed": "sip", "coins": {"xyz:TSLA": {"symbol": "TSLA"}, "xyz:BB": {"symbol": "BB"}}}"#;
    const OFF: &str = "extHip3ExternalQuotes is not enabled (the server runs with --hip3-external-quotes?)";

    /// The real owner over idle feeds with the HIP-3 external quotes - Binance's xyz:GOLD and xyz:TSLA, its quotes held
    /// 60 s, and the SIP table of xyz:TSLA and xyz:BB -, the SIP and BOATS feeds running or not.
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

    fn external(coin: &str, subscribe: bool) -> ClientMessage {
        let subscription = Subscription::Hip3ExternalQuotes { coin: coin.to_string() };
        if subscribe { ClientMessage::Subscribe { subscription } } else { ClientMessage::Unsubscribe { subscription } }
    }

    fn quote(bid: &str, ask: &str) -> crate::local_marks::slot::Quote {
        use crate::local_marks::price::VenuePrice;
        crate::local_marks::slot::Quote { bid: VenuePrice::parse(bid).unwrap(), ask: VenuePrice::parse(ask).unwrap() }
    }

    /// Design 000664 r4, 000669 r1 1.1 and r2 1.4: off, a subscribe and an unsubscribe get the same answer, without the
    /// local marks too; on, a coin outside both tables gets the same refusal for both - never `Already unsubscribed`,
    /// which a client would take for a session error -, while a coin of the channel unsubscribed twice gets it as any.
    /// Both refusals are their text as before with `"schemaVersion":2` beside it, byte for byte; the SIP's only coin BB
    /// is the channel's though the SIP feeds are off.
    #[tokio::test]
    async fn the_external_quotes_refuse_alike_when_off_or_outside_their_sources() {
        let refusal = |text: &str| serde_json::json!({"channel": "error", "data": text, "schemaVersion": 2});
        let (outbound, mut sent) = outbound();
        assert!(refuse_without_local_marks(&outbound, Market::Hip3External).await);
        let raw = sent.recv().await.unwrap();
        assert_eq!(&raw[..], format!(r#"{{"channel":"error","data":"{OFF}","schemaVersion":2}}"#).as_bytes());
        assert!(refuse_without_local_marks(&outbound, Market::Hip3).await);
        let other = serde_json::json!({"channel": "error", "data": "extHip3MarkPrices needs the server started with --local-marks"});
        assert_eq!(json(&sent.recv().await.unwrap()), other, "the other channels' answer unchanged");
        let (marks, _sender) = marks();
        let mut manager = SubscriptionManager::default();
        let mut session = MarkPricesSession::new(&marks);
        for subscribe in [true, false] {
            assert!(session.on_client_message(&outbound, &mut manager, external("xyz:TSLA", subscribe), Market::Hip3External, "xyz:TSLA").await);
            assert_eq!(json(&sent.recv().await.unwrap()), refusal(OFF));
        }
        let (marks, _feeds, _prices) = external_marks(false);
        let mut session = MarkPricesSession::new(&marks);
        for subscribe in [true, false, false] {
            assert!(session.on_client_message(&outbound, &mut manager, external("xyz:KIOXIA", subscribe), Market::Hip3External, "xyz:KIOXIA").await);
            let raw = sent.recv().await.unwrap();
            let expected = r#"{"channel":"error","data":"extHip3ExternalQuotes xyz:KIOXIA is not in the HIP-3 external quote sources","schemaVersion":2}"#;
            assert_eq!(&raw[..], expected.as_bytes());
        }
        assert!(manager.subscriptions().is_empty());
        assert!(session.on_client_message(&outbound, &mut manager, external("xyz:TSLA", false), Market::Hip3External, "xyz:TSLA").await);
        let twice = json(&sent.recv().await.unwrap());
        assert!(twice["data"].as_str().unwrap().starts_with("Already unsubscribed"), "{twice}");
        assert!(session.on_client_message(&outbound, &mut manager, external("xyz:BB", true), Market::Hip3External, "xyz:BB").await);
        assert_eq!(json(&sent.recv().await.unwrap())["channel"], "subscriptionResponse");
        let bb = json(&sent.recv().await.unwrap());
        assert_eq!((bb["data"]["external"]["status"].as_str(), bb["data"]["equity"]["reasonCode"].as_str()), (Some("unlisted"), Some("equityOff")));
    }

    /// A coin of Binance's table takes its frame at once and each tick: withheld while its connection is down, then
    /// Binance's quote, received no later than the snapshot's publish time (design 000664 r4); GOLD has no US stock
    /// leg.
    #[tokio::test]
    async fn a_coin_of_the_external_sources_takes_its_frames() {
        let (marks, feeds, _prices) = external_marks(false);
        let (outbound, mut sent) = outbound();
        let mut manager = SubscriptionManager::default();
        let mut session = MarkPricesSession::new(&marks);
        assert!(session.on_client_message(&outbound, &mut manager, external("xyz:GOLD", true), Market::Hip3External, "xyz:GOLD").await);
        assert_eq!(json(&sent.recv().await.unwrap())["channel"], "subscriptionResponse");
        let first = json(&sent.recv().await.unwrap());
        assert_eq!((first["channel"].as_str(), first["data"]["schemaVersion"].as_u64(), first["data"]["quoteMaxAgeMs"].as_u64()), (Some("extHip3ExternalQuotes"), Some(2), Some(60_000)));
        assert_eq!(first["data"]["external"], serde_json::json!({"status": "unavailable", "reasonCode": "notConnected",
            "reason": first["data"]["external"]["reason"]}));
        assert_eq!(first["data"]["equity"], serde_json::json!({"status": "unlisted"}));
        let slot = &feeds.hip3_external_parts_for_test()[0].slot;
        let epoch = slot.connected(crate::local_marks::now_ms());
        let received_ms = crate::local_marks::now_ms();
        slot.record(epoch, received_ms, [(0, Some(quote("3900.5", "3901")), None)]);
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
        assert_eq!(ready["equity"], serde_json::json!({"status": "unlisted"}));
    }

    /// Design 000669 r1 1.2–1.3 through the real owner, the SIP feeds on: TSLA, in both tables, takes both legs, each
    /// quote received no later than the frame's publish time. The US stock leg follows the wall clock's session: the
    /// quote of the feed counting at the publish time, `noSession` outside every session (the frame's own time decides
    /// which this run sees).
    #[tokio::test]
    async fn a_coin_of_both_tables_takes_both_legs() {
        use crate::local_marks::{
            hip3::{
                protocol::EquityFeed,
                session::{equity_session, venue_session},
            },
            now_ms,
        };
        let (marks, feeds, _prices) = external_marks(true);
        let (outbound, mut sent) = outbound();
        let mut manager = SubscriptionManager::default();
        let mut session = MarkPricesSession::new(&marks);
        assert!(session.on_client_message(&outbound, &mut manager, external("xyz:TSLA", true), Market::Hip3External, "xyz:TSLA").await);
        assert_eq!(json(&sent.recv().await.unwrap())["channel"], "subscriptionResponse");
        let first = json(&sent.recv().await.unwrap());
        assert_eq!(first["data"]["schemaVersion"], 2);
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
            let next = session.next().await;
            assert!(session.on_snapshot(&outbound, &mut manager, next).await);
            let data = json(&sent.recv().await.unwrap())["data"].clone();
            if settled(&data) {
                break data;
            }
            frames += 1;
            assert!(frames < 100, "{data}");
        };
        let published_ms = data["publishedTsMs"].as_u64().unwrap();
        assert!(data["external"]["externalQuotes"]["binance"]["receivedTsMs"].as_u64().unwrap() <= published_ms);
        let equity = &data["equity"];
        let now = equity_session(published_ms);
        match now {
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

    /// `GET /extHip3ExternalQuotes`: 404 off; on, every coin of the channel in its order (schema 2), a coin outside it
    /// 404 with the WS refusal's text; BB, the SIP's only, without the SIP feeds: Binance `unlisted`, US stock off.
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
        let (marks, _feeds, _prices) = external_marks(false);
        let (status, all) = get(&marks, None).await;
        assert_eq!(status, axum::http::StatusCode::OK);
        assert_eq!(all.as_array().unwrap().iter().map(|row| row["coin"].as_str().unwrap()).collect::<Vec<_>>(), ["xyz:BB", "xyz:GOLD", "xyz:TSLA"]);
        assert!(all.as_array().unwrap().iter().all(|row| row["schemaVersion"] == 2));
        let (status, bb) = get(&marks, Some("xyz:BB")).await;
        assert_eq!((status, bb[0]["external"]["status"].as_str(), bb[0]["equity"]["reasonCode"].as_str()), (axum::http::StatusCode::OK, Some("unlisted"), Some("equityOff")));
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
