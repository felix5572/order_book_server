//! The local marks' side of a WS connection - `extMarkPrices` (design 000454 r1 1.4): every tick, a frame of each
//! subscribed coin - and the `GET /extMarkPrices` handler. The frames come from the local marks' own snapshot channel,
//! never the book's broadcast.

use std::{collections::BTreeSet, sync::Arc};

use serde::Deserialize;
use serde_json::json;
use tokio::sync::watch::{self, error::RecvError};

use super::{Outbound, coin_positions::json_response};
use crate::{
    local_marks::{
        owner::{LocalMarks, OWNER_TASK_ENDED},
        prices_view::CoinMarksView,
        snapshot::LocalMarksSnapshot,
    },
    types::subscription::{ClientMessage, EXT_MARK_PRICES, ServerResponse, Subscription, SubscriptionManager},
};

/// A connection's `extMarkPrices` subscriptions.
pub(super) struct MarkPricesSession {
    /// `None` once the owner task ended.
    snapshots: Option<watch::Receiver<Arc<LocalMarksSnapshot>>>,
    subscribed: BTreeSet<String>,
}

impl MarkPricesSession {
    pub(super) fn new(marks: &LocalMarks) -> Self {
        Self { snapshots: Some(marks.subscribe()), subscribed: BTreeSet::new() }
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
        for coin in &self.subscribed {
            let marks = snapshot.coin(coin).unwrap_or_else(|| unreachable!("a subscribed coin is in the sources"));
            if !outbound.send_frame(CoinMarksView::of(&snapshot, coin, marks).to_wire()).await {
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
        coin: &str,
    ) -> bool {
        let subscription = Subscription::MarkPrices { coin: coin.to_string() };
        // display only, as for the other subscriptions
        let sub = serde_json::to_string(&subscription).unwrap_or_default();
        if matches!(client_message, ClientMessage::Unsubscribe { .. }) {
            if !manager.unsubscribe(subscription) {
                return outbound.send_message(ServerResponse::Error(format!("Already unsubscribed: {sub}"))).await;
            }
            self.subscribed.remove(coin);
            return outbound.send_message(ServerResponse::SubscriptionResponse(client_message)).await;
        }
        let refused = |reason: &str| ServerResponse::Error(format!("Unable to grab {EXT_MARK_PRICES} snapshot: {reason}"));
        let Some(snapshots) = self.snapshots.as_ref().filter(|snapshots| snapshots.has_changed().is_ok()) else {
            return outbound.send_message(refused(OWNER_TASK_ENDED)).await;
        };
        // not marked seen: the next tick's frames include this coin's, so no frame is skipped
        let snapshot = Arc::clone(&snapshots.borrow());
        let Some(marks) = snapshot.coin(coin) else {
            return outbound.send_message(refused(&format!("{coin} is not in the local marks' sources"))).await;
        };
        match manager.subscribe(subscription) {
            Ok(true) => {}
            Ok(false) => return outbound.send_message(ServerResponse::Error(format!("Already subscribed: {sub}"))).await,
            Err(err) => return outbound.send_message(ServerResponse::Error(format!("Rejected subscription: {err}"))).await,
        }
        if !outbound.send_message(ServerResponse::SubscriptionResponse(client_message)).await {
            return false;
        }
        self.subscribed.insert(coin.to_string());
        outbound.send_frame(CoinMarksView::of(&snapshot, coin, marks).to_wire()).await
    }

    /// The owner task ended: every subscription ends with the reason.
    async fn withdraw_all(&mut self, outbound: &Outbound, manager: &mut SubscriptionManager) -> bool {
        for coin in std::mem::take(&mut self.subscribed) {
            manager.unsubscribe(Subscription::MarkPrices { coin: coin.clone() });
            let withdrawn = ServerResponse::Error(format!("{EXT_MARK_PRICES} {coin} withdrawn: {OWNER_TASK_ENDED}"));
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
    use axum::http::StatusCode;
    let snapshot = match marks.latest() {
        Ok(snapshot) => snapshot,
        Err(reason) => return json_response(StatusCode::SERVICE_UNAVAILABLE, None, json!({"error": reason}).to_string().into()),
    };
    let views: Vec<CoinMarksView<'_>> = match query.coin.as_deref() {
        Some(coin) => {
            let Some(coin_marks) = snapshot.coin(coin) else {
                let error = format!("{coin} is not in the local marks' sources");
                return json_response(StatusCode::NOT_FOUND, None, json!({"error": error}).to_string().into());
            };
            vec![CoinMarksView::of(&snapshot, coin, coin_marks)]
        }
        None => snapshot.coins().map(|(coin, coin_marks)| CoinMarksView::of(&snapshot, coin, coin_marks)).collect(),
    };
    #[expect(clippy::expect_used, reason = "string keys and finite numbers only; serializing cannot fail")]
    let body = serde_json::to_vec(&views).expect("the marks view serializes");
    json_response(StatusCode::OK, None, body.into())
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
        let feeds = Arc::new(LocalMarkFeeds::idle_for_test(Arc::new(world.sources)));
        (LocalMarks::of_snapshots_for_test(feeds, receiver), sender)
    }

    #[tokio::test]
    async fn a_subscription_takes_the_latest_frame_then_each_tick_and_ends_with_the_owner() {
        let (marks, sender) = marks();
        let (outbound, mut sent) = outbound();
        let mut manager = SubscriptionManager::default();
        let mut session = MarkPricesSession::new(&marks);
        assert!(session.on_client_message(&outbound, &mut manager, subscribe("BTC"), "BTC").await);
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
        assert!(session.on_client_message(&outbound, &mut manager, subscribe("BTC"), "BTC").await);
        assert_eq!(json(&sent.recv().await.unwrap())["data"], "Unable to grab extMarkPrices snapshot: the local marks owner task ended");
    }

    #[tokio::test]
    async fn a_coin_outside_the_sources_is_refused_and_an_unsubscribed_one_gets_no_frame() {
        let (marks, sender) = marks();
        let (outbound, mut sent) = outbound();
        let mut manager = SubscriptionManager::default();
        let mut session = MarkPricesSession::new(&marks);
        assert!(session.on_client_message(&outbound, &mut manager, subscribe("HYPE"), "HYPE").await);
        assert_eq!(json(&sent.recv().await.unwrap())["data"], "Unable to grab extMarkPrices snapshot: HYPE is not in the local marks' sources");
        assert!(manager.subscriptions().is_empty());
        assert!(session.on_client_message(&outbound, &mut manager, subscribe("ETH"), "ETH").await);
        let unsubscribe = ClientMessage::Unsubscribe { subscription: Subscription::MarkPrices { coin: "ETH".to_string() } };
        assert!(session.on_client_message(&outbound, &mut manager, unsubscribe, "ETH").await);
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
        drop(sender);
        let (status, ended) = get(&marks, Some("BTC")).await;
        assert_eq!((status, ended["error"].as_str()), (axum::http::StatusCode::SERVICE_UNAVAILABLE, Some("the local marks owner task ended")));
    }
}
