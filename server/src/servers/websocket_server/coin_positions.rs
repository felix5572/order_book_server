//! The `coinPositions` side of a WS connection and the `GET /coinPositions` handler (design 000419 r2 4.1 / 4.2).
//! The account view reaches connections through its own broadcast, never the book's channel.

use std::{collections::HashMap, sync::Arc};

use log::error;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::broadcast::{self, error::RecvError};

use super::{Outbound, gzip_body};
use crate::{
    accounts::{
        book::QueryError,
        publish::{AccountsFrame, CoinSubscriptionGuard},
        runtime::AccountsHandle,
        view::{CoinPositionsDto, CoinPositionsFrame},
    },
    types::subscription::{ClientMessage, ServerResponse, Subscription, SubscriptionManager},
};

const OWNER_GONE: &str = "account owner task exited";

/// One live subscription: its share of the coin's subscriber count and the version last delivered.
struct Subscribed {
    _guard: CoinSubscriptionGuard,
    version: u64,
}

/// A connection's `coinPositions` subscriptions.
pub(super) struct CoinPositionsSession {
    accounts: AccountsHandle,
    /// `None` once the owner is gone.
    frames: Option<broadcast::Receiver<Arc<AccountsFrame>>>,
    subscribed: HashMap<String, Subscribed>,
}

impl CoinPositionsSession {
    pub(super) fn new(accounts: AccountsHandle) -> Self {
        // `None` when the owner is already gone: every subscription is then refused by the snapshot query
        let frames = accounts.frames();
        Self { accounts, frames, subscribed: HashMap::new() }
    }

    /// The next broadcast frame; pending forever once the owner is gone. Cancel-safe.
    pub(super) async fn next_frame(&mut self) -> Result<Arc<AccountsFrame>, RecvError> {
        match &mut self.frames {
            Some(frames) => frames.recv().await,
            None => std::future::pending().await,
        }
    }

    /// Forwards a frame to this connection's subscriptions. Returns false when the connection must be dropped.
    pub(super) async fn on_frame(
        &mut self,
        outbound: &Outbound,
        manager: &mut SubscriptionManager,
        frame: Result<Arc<AccountsFrame>, RecvError>,
    ) -> bool {
        let frame = match frame {
            Ok(frame) => frame,
            Err(RecvError::Lagged(missed)) => {
                let reason = format!("slow consumer: {missed} coinPositions frames dropped");
                return self.withdraw_all(outbound, manager, &reason).await;
            }
            Err(RecvError::Closed) => {
                self.frames = None;
                return self.withdraw_all(outbound, manager, OWNER_GONE).await;
            }
        };
        match frame.as_ref() {
            AccountsFrame::Coin { coin, version, wire } => {
                let Some(subscribed) = self.subscribed.get_mut(coin) else {
                    return true;
                };
                // at or below: already in the snapshot this connection received
                if *version <= subscribed.version {
                    return true;
                }
                // the owner numbers every frame of a coin it broadcasts; a gap is a lost frame
                if *version != subscribed.version + 1 {
                    error!("coinPositions {coin}: frame {version} after {}; withdrawing", subscribed.version);
                    let reason = format!("frames {} to {} missed", subscribed.version + 1, version - 1);
                    return self.withdraw(outbound, manager, coin, &reason).await;
                }
                subscribed.version = *version;
                outbound.send_frame(wire.clone()).await
            }
            AccountsFrame::WithdrawAll { reason } => self.withdraw_all(outbound, manager, reason).await,
            AccountsFrame::WithdrawCoin { coin, reason } => self.withdraw(outbound, manager, coin, reason).await,
        }
    }

    /// Subscribe / unsubscribe of `coinPositions` for `coin`. Returns false when the connection must be dropped.
    pub(super) async fn on_client_message(
        &mut self,
        outbound: &Outbound,
        manager: &mut SubscriptionManager,
        client_message: ClientMessage,
        coin: &str,
    ) -> bool {
        let subscription = Subscription::CoinPositions { coin: coin.to_string() };
        // display only, as for the other subscriptions
        let sub = serde_json::to_string(&subscription).unwrap_or_default();
        if matches!(client_message, ClientMessage::Unsubscribe { .. }) {
            if !manager.unsubscribe(subscription) {
                return outbound.send_message(ServerResponse::Error(format!("Already unsubscribed: {sub}"))).await;
            }
            self.subscribed.remove(coin);
            return outbound.send_message(ServerResponse::SubscriptionResponse(client_message)).await;
        }
        match manager.subscribe(subscription.clone()) {
            Ok(true) => {}
            Ok(false) => return outbound.send_message(ServerResponse::Error(format!("Already subscribed: {sub}"))).await,
            Err(err) => return outbound.send_message(ServerResponse::Error(format!("Rejected subscription: {err}"))).await,
        }
        // counted before the snapshot is taken, so every later frame of the coin is newer than it
        let guard = self.accounts.acquire(coin);
        let view = match self.accounts.coin_positions(coin).await {
            Ok(view) => view,
            Err(err) => {
                manager.unsubscribe(subscription);
                let reason = query_error_text(coin, &err);
                return outbound
                    .send_message(ServerResponse::Error(format!("Unable to grab coinPositions snapshot: {reason}")))
                    .await;
            }
        };
        if !outbound.send_message(ServerResponse::SubscriptionResponse(client_message)).await {
            return false;
        }
        self.subscribed.insert(coin.to_string(), Subscribed { _guard: guard, version: view.version });
        outbound.send_frame(CoinPositionsFrame::snapshot(&view).to_wire()).await
    }

    async fn withdraw_all(&mut self, outbound: &Outbound, manager: &mut SubscriptionManager, reason: &str) -> bool {
        let coins: Vec<String> = self.subscribed.keys().cloned().collect();
        for coin in coins {
            if !self.withdraw(outbound, manager, &coin, reason).await {
                return false;
            }
        }
        true
    }

    async fn withdraw(&mut self, outbound: &Outbound, manager: &mut SubscriptionManager, coin: &str, reason: &str) -> bool {
        if self.subscribed.remove(coin).is_none() {
            return true;
        }
        manager.unsubscribe(Subscription::CoinPositions { coin: coin.to_string() });
        let frame = CoinPositionsFrame::Withdrawn { coin: coin.to_string(), reason: reason.to_string() };
        outbound.send_frame(frame.to_wire()).await
    }
}

fn query_error_text(coin: &str, err: &QueryError) -> String {
    match err {
        QueryError::UnknownCoin => format!("{coin} is not a coin of the account anchor universe"),
        QueryError::Unavailable(reason) => reason.to_string(),
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct CoinPositionsQuery {
    coin: String,
}

/// `GET /coinPositions?coin=`: 200 with the view, 404 for a coin outside the anchor universe, 503 with the reason while
/// the view is unavailable. The view is gzipped when the client accepts it (PUMP's is ~2 MB of JSON), as `/l4Book`.
pub(super) async fn coin_positions_handler(
    axum::extract::Query(query): axum::extract::Query<CoinPositionsQuery>,
    headers: axum::http::HeaderMap,
    accounts: AccountsHandle,
) -> axum::response::Response {
    use axum::http::StatusCode;
    let view = match accounts.coin_positions(&query.coin).await {
        Ok(view) => view,
        Err(err) => {
            let status = match err {
                QueryError::UnknownCoin => StatusCode::NOT_FOUND,
                QueryError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            };
            return json_response(status, None, json!({"error": query_error_text(&query.coin, &err)}).to_string().into());
        }
    };
    let body = bytes::Bytes::from(CoinPositionsDto::new(&view).to_json());
    let accepts_gzip = headers
        .get(axum::http::header::ACCEPT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("gzip"));
    if accepts_gzip {
        let plain = body.clone();
        // a gzip write failure falls back to the plain body, as for /l4Book
        if let Ok(Some(gzipped)) = tokio::task::spawn_blocking(move || gzip_body(&plain)).await {
            return json_response(StatusCode::OK, Some("gzip"), gzipped.into());
        }
    }
    json_response(StatusCode::OK, None, body)
}

fn json_response(status: axum::http::StatusCode, encoding: Option<&str>, body: bytes::Bytes) -> axum::response::Response {
    let builder = axum::response::Response::builder().status(status).header("content-type", "application/json");
    let builder = match encoding {
        Some(encoding) => builder.header("content-encoding", encoding),
        None => builder,
    };
    builder.body(body.into()).unwrap_or_else(|_| axum::response::Response::new(String::new().into()))
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use super::*;
    use crate::accounts::{
        book::Stream,
        runtime::TestOwner,
        test_support::{PUMP, trusted_book, user},
    };

    fn outbound() -> (Outbound, mpsc::Receiver<bytes::Bytes>) {
        let (data_tx, data_rx) = mpsc::channel(64);
        let (pong_tx, _pong_rx) = mpsc::channel(1);
        (Outbound { data_tx, pong_tx }, data_rx)
    }

    fn subscribe(coin: &str) -> ClientMessage {
        ClientMessage::Subscribe { subscription: Subscription::CoinPositions { coin: coin.to_string() } }
    }

    fn json(bytes: &bytes::Bytes) -> serde_json::Value {
        serde_json::from_slice(bytes).unwrap()
    }

    fn coin_frame(version: u64) -> Arc<AccountsFrame> {
        let wire = bytes::Bytes::from(format!("v{version}"));
        Arc::new(AccountsFrame::Coin { coin: PUMP.to_string(), version, wire })
    }

    /// Subscribed to PUMP; the ack and the snapshot (version 0) consumed.
    async fn subscribed_session(owner: &TestOwner) -> (CoinPositionsSession, Outbound, mpsc::Receiver<bytes::Bytes>, SubscriptionManager) {
        let (outbound, mut data) = outbound();
        let mut manager = SubscriptionManager::default();
        let mut session = CoinPositionsSession::new(owner.handle.clone());
        assert!(session.on_client_message(&outbound, &mut manager, subscribe(PUMP), PUMP).await);
        assert_eq!(json(&data.recv().await.unwrap())["channel"], "subscriptionResponse");
        let snapshot = json(&data.recv().await.unwrap());
        assert_eq!((snapshot["data"]["kind"].as_str(), snapshot["data"]["version"].as_u64()), (Some("snapshot"), Some(0)));
        assert_eq!(snapshot["data"]["positions"][0]["szi"], "5.0");
        (session, outbound, data, manager)
    }

    #[tokio::test]
    async fn forwards_newer_versions_in_order_and_withdraws_on_a_gap() {
        let owner = TestOwner::start(trusted_book(&[(user(1), 5)]));
        let (mut session, outbound, mut data, mut manager) = subscribed_session(&owner).await;
        assert!(session.on_frame(&outbound, &mut manager, Ok(coin_frame(0))).await);
        assert!(data.try_recv().is_err(), "version 0 is the snapshot's");
        assert!(session.on_frame(&outbound, &mut manager, Ok(coin_frame(1))).await);
        assert_eq!(data.try_recv().unwrap(), "v1");
        assert!(session.on_frame(&outbound, &mut manager, Ok(coin_frame(3))).await);
        let withdrawn = json(&data.try_recv().unwrap());
        assert_eq!(withdrawn["data"], serde_json::json!({"kind": "withdrawn", "coin": PUMP, "reason": "frames 2 to 2 missed"}));
        assert!(manager.subscriptions().is_empty(), "the subscription ended");
        assert!(session.on_frame(&outbound, &mut manager, Ok(coin_frame(4))).await);
        assert!(data.try_recv().is_err());
    }

    #[tokio::test]
    async fn lag_and_a_gone_owner_withdraw_every_subscription() {
        let owner = TestOwner::start(trusted_book(&[(user(1), 5)]));
        let (mut session, outbound, mut data, mut manager) = subscribed_session(&owner).await;
        assert!(session.on_frame(&outbound, &mut manager, Err(RecvError::Lagged(7))).await);
        assert_eq!(json(&data.try_recv().unwrap())["data"]["reason"], "slow consumer: 7 coinPositions frames dropped");
        assert!(manager.subscriptions().is_empty());

        let (mut session, outbound, mut data, mut manager) = subscribed_session(&owner).await;
        assert!(session.on_frame(&outbound, &mut manager, Err(RecvError::Closed)).await);
        assert_eq!(json(&data.try_recv().unwrap())["data"]["reason"], OWNER_GONE);
        assert!(session.frames.is_none(), "no busy loop on a closed broadcast");
    }

    #[tokio::test]
    async fn subscribing_an_unknown_coin_or_an_unavailable_view_is_refused() {
        let owner = TestOwner::start(trusted_book(&[(user(1), 5)]));
        let (outbound, mut data) = outbound();
        let mut manager = SubscriptionManager::default();
        let mut session = CoinPositionsSession::new(owner.handle.clone());
        assert!(session.on_client_message(&outbound, &mut manager, subscribe("NOPE"), "NOPE").await);
        let error = json(&data.recv().await.unwrap());
        assert_eq!(error["data"], "Unable to grab coinPositions snapshot: NOPE is not a coin of the account anchor universe");
        assert!(manager.subscriptions().is_empty());
        owner.loss(Stream::Fills).await;
        wait_unavailable(&owner).await;
        assert!(session.on_client_message(&outbound, &mut manager, subscribe(PUMP), PUMP).await);
        let error = json(&data.recv().await.unwrap());
        assert!(error["data"].as_str().unwrap().starts_with("Unable to grab coinPositions snapshot: fills lost lines"), "{error}");
        assert!(manager.subscriptions().is_empty());
    }

    async fn wait_unavailable(owner: &TestOwner) {
        while owner.handle.status().await.unwrap().availability.is_ok() {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn the_http_view_answers_200_404_and_503() {
        async fn get(owner: &TestOwner, coin: &str) -> (axum::http::StatusCode, serde_json::Value) {
            let query = axum::extract::Query(CoinPositionsQuery { coin: coin.to_string() });
            let response = coin_positions_handler(query, axum::http::HeaderMap::new(), owner.handle.clone()).await;
            let status = response.status();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
            (status, serde_json::from_slice(&body).unwrap())
        }
        let owner = TestOwner::start(trusted_book(&[(user(1), 5)]));
        let (status, body) = get(&owner, PUMP).await;
        assert_eq!((status, body["positions"][0]["szi"].as_str()), (axum::http::StatusCode::OK, Some("5.0")));
        assert!(body.get("version").is_none(), "the HTTP shape has no push version");
        let (status, body) = get(&owner, "NOPE").await;
        assert_eq!((status, body["error"].as_str()), (axum::http::StatusCode::NOT_FOUND, Some("NOPE is not a coin of the account anchor universe")));
        owner.loss(Stream::Fills).await;
        wait_unavailable(&owner).await;
        let (status, body) = get(&owner, PUMP).await;
        assert_eq!(status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
        assert!(body["error"].as_str().unwrap().starts_with("fills lost lines"), "{body}");
    }

    #[tokio::test]
    async fn the_http_view_is_gzipped_when_accepted() {
        use std::io::Read as _;
        let owner = TestOwner::start(trusted_book(&[(user(1), 5)]));
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(axum::http::header::ACCEPT_ENCODING, "gzip, deflate".parse().unwrap());
        let query = axum::extract::Query(CoinPositionsQuery { coin: PUMP.to_string() });
        let response = coin_positions_handler(query, headers, owner.handle.clone()).await;
        assert_eq!(response.headers()["content-encoding"], "gzip");
        let gzipped = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let mut plain = String::new();
        flate2::read::GzDecoder::new(&gzipped[..]).read_to_string(&mut plain).unwrap();
        let body: serde_json::Value = serde_json::from_str(&plain).unwrap();
        assert_eq!(body["positions"][0]["szi"], "5.0");
    }
}
