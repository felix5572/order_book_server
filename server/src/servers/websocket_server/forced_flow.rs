//! The `extForcedFlow` subscriptions of a WS connection (design 000451 r4 revision 9), and `GET /extForcedFlow`. A
//! subscription holds its share of its (coin, tier)'s demand, takes the latest frames when it subscribes, then every
//! newer frame its filter admits; each frame is the whole content of a coin (the client replaces, never merges).

use std::{collections::HashMap, time::Duration};

use serde::Deserialize;
use serde_json::json;

use super::{Outbound, coin_positions::json_response};
use crate::{
    accounts::{
        forced_flow_owner::{FlowDemandGuard, FlowFrame},
        forced_flow_view::{FlowFilter, FlowFrameDto},
        runtime::AccountsHandle,
    },
    types::subscription::{ClientMessage, EXT_FORCED_FLOW, ForcedFlowParams, ServerResponse, Subscription, SubscriptionManager},
};

/// An HTTP query waits this long for its frames at most.
const QUERY_TIMEOUT: Duration = Duration::from_secs(10);

struct FlowSubscription {
    filter: FlowFilter,
    _guard: FlowDemandGuard,
    /// The `seq` sent last per coin: a frame at or below it is already sent.
    sent: HashMap<String, u64>,
}

pub(super) struct ForcedFlowSubscriptions {
    subscriptions: HashMap<Subscription, FlowSubscription>,
}

impl ForcedFlowSubscriptions {
    pub(super) fn new() -> Self {
        Self { subscriptions: HashMap::new() }
    }

    /// Subscribe / unsubscribe. Returns false when the connection must be dropped.
    pub(super) async fn on_client_message(
        &mut self,
        accounts: &AccountsHandle,
        outbound: &Outbound,
        manager: &mut SubscriptionManager,
        client_message: ClientMessage,
        subscription: Subscription,
    ) -> bool {
        // display only, as for the other subscriptions
        let sub = serde_json::to_string(&subscription).unwrap_or_default();
        if matches!(client_message, ClientMessage::Unsubscribe { .. }) {
            if !manager.unsubscribe(subscription.clone()) {
                return outbound.send_message(ServerResponse::Error(format!("Already unsubscribed: {sub}"))).await;
            }
            self.subscriptions.remove(&subscription);
            return outbound.send_message(ServerResponse::SubscriptionResponse(client_message)).await;
        }
        let filter = match filter_of(&subscription) {
            Ok(filter) => filter,
            Err(reason) => return outbound.send_message(ServerResponse::Error(format!("Invalid subscription: {sub}: {reason}"))).await,
        };
        match manager.subscribe(subscription.clone()) {
            Ok(true) => {}
            Ok(false) => return outbound.send_message(ServerResponse::Error(format!("Already subscribed: {sub}"))).await,
            Err(err) => return outbound.send_message(ServerResponse::Error(format!("Rejected subscription: {err}"))).await,
        }
        // the demand is counted before the frames are asked for: every later frame of the key is computed
        let guard = accounts.acquire_flow((filter.coin.clone(), filter.tier()));
        let frames = match accounts.forced_flow(filter.coin.clone(), filter.tier()).await {
            Ok(frames) => frames,
            Err(reason) => {
                manager.unsubscribe(subscription);
                return outbound.send_message(ServerResponse::Error(format!("Unable to grab {EXT_FORCED_FLOW} snapshot: {reason}"))).await;
            }
        };
        if !outbound.send_message(ServerResponse::SubscriptionResponse(client_message)).await {
            return false;
        }
        let admitted: Vec<_> = frames.iter().filter(|frame| filter.admits(frame)).collect();
        let mut subscribed = FlowSubscription { filter, _guard: guard, sent: HashMap::new() };
        for frame in admitted {
            if !send(outbound, &mut subscribed, frame).await {
                return false;
            }
        }
        self.subscriptions.insert(subscription, subscribed);
        true
    }

    /// A frame of the owner for every subscription admitting it. Returns false when the connection must be dropped.
    pub(super) async fn on_frame(&mut self, outbound: &Outbound, frame: &FlowFrame) -> bool {
        for subscribed in self.subscriptions.values_mut().filter(|subscribed| subscribed.filter.admits(frame)) {
            if !send(outbound, subscribed, frame).await {
                return false;
            }
        }
        true
    }

    /// Ends every subscription with the reason.
    pub(super) async fn withdraw_all(&mut self, outbound: &Outbound, manager: &mut SubscriptionManager, reason: &str) -> bool {
        for (subscription, _) in std::mem::take(&mut self.subscriptions) {
            let sub = serde_json::to_string(&subscription).unwrap_or_default();
            manager.unsubscribe(subscription);
            if !outbound.send_message(ServerResponse::Error(format!("{EXT_FORCED_FLOW} withdrawn: {sub}: {reason}"))).await {
                return false;
            }
        }
        true
    }
}

/// Sends `frame` unless an equal or newer one of its coin went out.
async fn send(outbound: &Outbound, subscribed: &mut FlowSubscription, frame: &FlowFrame) -> bool {
    if subscribed.sent.get(&frame.coin).is_some_and(|seq| *seq >= frame.seq) {
        return true;
    }
    subscribed.sent.insert(frame.coin.clone(), frame.seq);
    outbound.send_frame(FlowFrameDto::of(frame, &subscribed.filter).to_wire()).await
}

fn filter_of(subscription: &Subscription) -> Result<FlowFilter, String> {
    let Subscription::ForcedFlow(params) = subscription else { unreachable!("an extForcedFlow subscription") };
    let ForcedFlowParams { coin, max_distance_bps, sources, exclude_coins, min_notional_usd } = params.clone();
    FlowFilter::of(coin, max_distance_bps, sources, exclude_coins, min_notional_usd)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ForcedFlowQuery {
    coin: Option<String>,
    max_distance_bps: u32,
    /// Comma-separated.
    sources: Option<String>,
    /// Comma-separated.
    exclude_coins: Option<String>,
    min_notional_usd: Option<u64>,
}

/// `GET /extForcedFlow?maxDistanceBps=[&coin=][&sources=][&excludeCoins=][&minNotionalUsd=]`: 200 with the coins'
/// frames as a list, 400 for parameters the subscription would refuse, 503 while the forced flow is unavailable.
/// Each query is a demand of its own: a frame older than the tier's period is computed again first (r5 revision 14).
pub(super) async fn forced_flow_handler(axum::extract::Query(query): axum::extract::Query<ForcedFlowQuery>, accounts: AccountsHandle) -> axum::response::Response {
    use axum::http::StatusCode;
    let error = |status: StatusCode, reason: String| json_response(status, None, json!({"error": reason}).to_string().into());
    let ForcedFlowQuery { coin, max_distance_bps, sources, exclude_coins, min_notional_usd } = query;
    let list = |text: String| text.split(',').map(str::to_string).collect();
    let filter = match FlowFilter::of(coin, max_distance_bps, sources.map(list), exclude_coins.map(list), min_notional_usd) {
        Ok(filter) => filter,
        Err(reason) => return error(StatusCode::BAD_REQUEST, reason),
    };
    let frames = match tokio::time::timeout(QUERY_TIMEOUT, accounts.forced_flow(filter.coin.clone(), filter.tier())).await {
        Ok(Ok(frames)) => frames,
        Ok(Err(reason)) => return error(StatusCode::SERVICE_UNAVAILABLE, reason),
        Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, format!("the forced flow did not answer within {QUERY_TIMEOUT:?}")),
    };
    let views: Vec<FlowFrameDto<'_>> = frames.iter().filter(|frame| filter.admits(frame)).map(|frame| FlowFrameDto::of(frame, &filter)).collect();
    #[expect(clippy::expect_used, reason = "string keys and finite numbers only; serializing cannot fail")]
    let body = serde_json::to_vec(&views).expect("the forced flow view serializes");
    json_response(StatusCode::OK, None, body.into())
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use super::*;
    use crate::{
        accounts::{
            runtime::TestOwner,
            test_support::{PUMP, trusted_book, user},
        },
        listeners::order_book::BookTrust,
        servers::websocket_server::coin_positions::AccountsSession,
    };

    fn outbound() -> (Outbound, mpsc::Receiver<bytes::Bytes>) {
        let (data_tx, data_rx) = mpsc::channel(64);
        let (pong_tx, _pong_rx) = mpsc::channel(1);
        (Outbound::for_test(data_tx, pong_tx), data_rx)
    }

    fn subscription(coin: Option<&str>, max_distance_bps: u32) -> Subscription {
        Subscription::ForcedFlow(ForcedFlowParams { coin: coin.map(str::to_string), max_distance_bps, sources: None, exclude_coins: None, min_notional_usd: None })
    }

    fn json(bytes: &bytes::Bytes) -> serde_json::Value {
        serde_json::from_slice(bytes).unwrap()
    }

    /// A subscription takes its coin's frame at once (here unavailable: the test owner keeps no node marks), then
    /// unsubscribes; a refused one says why.
    #[tokio::test]
    async fn a_subscription_takes_its_frames_at_once() {
        let owner = TestOwner::start(trusted_book(&[(user(1), 5)]));
        let (outbound, mut data) = outbound();
        let mut manager = SubscriptionManager::default();
        let mut session = AccountsSession::new(owner.handle.clone(), BookTrust::new());
        let sub = subscription(Some(PUMP), 50);
        let subscribed = session.on_forced_flow_message(&outbound, &mut manager, ClientMessage::Subscribe { subscription: sub.clone() }, sub.clone());
        assert!(tokio::time::timeout(Duration::from_secs(5), subscribed).await.expect("the frames come at once"));
        assert_eq!(json(&data.recv().await.unwrap())["channel"], "subscriptionResponse");
        let frame = json(&data.recv().await.unwrap());
        assert_eq!((frame["channel"].as_str(), frame["data"]["coin"].as_str(), frame["data"]["tierBps"].as_u64()), (Some(EXT_FORCED_FLOW), Some(PUMP), Some(50)));
        assert_eq!((frame["data"]["status"].as_str(), frame["data"]["reason"].as_str()), (Some("unavailable"), Some("the server runs without an order book")));
        assert!(session.on_forced_flow_message(&outbound, &mut manager, ClientMessage::Unsubscribe { subscription: sub.clone() }, sub).await);
        assert_eq!(json(&data.recv().await.unwrap())["channel"], "subscriptionResponse");
        let wide = subscription(None, 1_001);
        assert!(session.on_forced_flow_message(&outbound, &mut manager, ClientMessage::Subscribe { subscription: wide.clone() }, wide).await);
        assert!(json(&data.recv().await.unwrap())["data"].as_str().unwrap().ends_with("maxDistanceBps 1001 is over 1000"));
        assert!(manager.subscriptions().is_empty());
    }

    fn parse(query: &str) -> Result<axum::extract::Query<ForcedFlowQuery>, axum::extract::rejection::QueryRejection> {
        axum::extract::Query::try_from_uri(&format!("http://localhost/extForcedFlow?{query}").parse().unwrap())
    }

    #[tokio::test]
    async fn the_http_view_answers_200_and_400() {
        async fn get(owner: &TestOwner, query: &str) -> (axum::http::StatusCode, serde_json::Value) {
            let query = parse(query).unwrap();
            let response = forced_flow_handler(query, owner.handle.clone()).await;
            let status = response.status();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
            (status, serde_json::from_slice(&body).unwrap())
        }
        let owner = TestOwner::start(trusted_book(&[(user(1), 5)]));
        let (status, frames) = get(&owner, "coin=PUMP&maxDistanceBps=10&sources=trigger,liquidation").await;
        assert_eq!((status, frames[0]["coin"].as_str(), frames[0]["tierBps"].as_u64()), (axum::http::StatusCode::OK, Some(PUMP), Some(10)));
        let (status, error) = get(&owner, "maxDistanceBps=10&sources=stops").await;
        assert_eq!((status, error["error"].as_str()), (axum::http::StatusCode::BAD_REQUEST, Some("unknown source \"stops\": trigger or liquidation")));
        assert!(parse("maxDistanceBps=10&fast=1").is_err(), "unknown parameters are refused");
    }
}
