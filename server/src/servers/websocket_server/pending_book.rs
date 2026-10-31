//! The pending book's side of a WS connection (hyperliquid-trade design 000604 section 4, r3 revision 9; batch 2c):
//! `extPendingBbo { coin }` - the coin's frame at once, then each frame whose `bbo` or `basisBbo` moved - and
//! `extPendingTrades { coin?, user? }` - the predicted trades its filter admits, `user` the maker or the taker's vault.
//! Each subscription is told whenever the book turns unavailable (`{"status": "unavailable", "reason"}`) or available
//! again (an `extPendingBbo` coin's current frame; `{"status": "ready"}` for trades). The connection listens to the
//! pending book's broadcast only while it holds one of them; a broadcast gap withdraws them.
//!
//! A subscription starts at the store's snapshot - its state, skipping every event at or below its sequence number -
//! and then follows the broadcast in order (review 000614 r1 P1-3, r2 P1-1): each availability change is told as of
//! it, a recovery with the book committed with it; no frame or trade goes to a subscription last told the book
//! unavailable. The `extPendingTrades` frames name no subscription, so a connection's trades subscriptions share one
//! channel, one state and one order: the first starts it at the store, a later one joins it where it stands and is
//! told its state. Each filter matches only the trades published after the store it joined at (r3 P2); a trade any
//! filter then matching admits goes once (r1 P2, r2 P1-2).

use std::collections::{BTreeMap, HashMap};

use alloy::primitives::{Address, B256};
use serde::Serialize;
use tokio::sync::broadcast::{self, error::RecvError};

use super::Outbound;
use crate::{
    accounts::view::user_text,
    order_book::{Coin, Side},
    pending::{
        BboFrame, BestLevels, PendingBookHandle, PendingEvent, PendingTrade, TradesFrame,
        actions::ActionId,
        overlay::Maker,
        publish::Snapshot,
        shadow::Unavailable,
    },
    types::{
        Level,
        subscription::{
            ClientMessage, EXT_PENDING_BBO, EXT_PENDING_TRADES, PendingBboParams, PendingTradesParams, ServerResponse,
            Subscription, SubscriptionManager,
        },
    },
};

/// What a subscription was told last: through which sequence number, and whether the book was served.
#[derive(Debug, Clone, Copy)]
struct Told {
    seq: u64,
    availability: Result<(), Unavailable>,
}

impl Told {
    fn of(state: &Snapshot) -> Self {
        Self { seq: state.seq, availability: state.availability() }
    }

    /// Whether an event at `seq` is news to it.
    const fn passes(&self, seq: u64) -> bool {
        seq > self.seq
    }

    /// Whether a frame or trades at `seq` go to it: news, the book served.
    const fn takes(&self, seq: u64) -> bool {
        self.availability.is_ok() && self.passes(seq)
    }
}

/// An `extPendingTrades` subscription's filters.
struct TradesFilter {
    coin: Option<String>,
    user: Option<Address>,
}

impl TradesFilter {
    fn admits(&self, trade: &PendingTrade) -> bool {
        let coin = self.coin.as_deref().is_none_or(|coin| coin == trade.coin.value());
        let user = self.user.is_none_or(|user| {
            matches!(&trade.maker, Maker::Confirmed { user: maker, .. } if *maker == user) || trade.vault == Some(user)
        });
        coin && user
    }
}

/// A connection's `extPendingTrades` channel: its subscriptions' filters, told one state in one order.
struct TradesChannel {
    told: Told,
    /// Each filter with the store's sequence number when it joined.
    filters: HashMap<PendingTradesParams, (TradesFilter, u64)>,
}

impl TradesChannel {
    /// The trades a filter joined before them admits: a filter added takes no trade published before it.
    fn admitted<'t>(&self, frame: &'t TradesFrame) -> Vec<&'t PendingTrade> {
        let joined: Vec<&TradesFilter> = self.filters.values().filter(|(_, seq)| frame.seq > *seq).map(|(filter, _)| filter).collect();
        frame.trades.iter().filter(|trade| joined.iter().any(|filter| filter.admits(trade))).collect()
    }
}

/// A connection's pending book subscriptions.
pub(super) struct PendingBookSession {
    handle: PendingBookHandle,
    /// The broadcast while anything is subscribed.
    events: Option<broadcast::Receiver<PendingEvent>>,
    bbo: BTreeMap<Coin, Told>,
    /// While any `extPendingTrades` subscription is held.
    trades: Option<TradesChannel>,
}

impl PendingBookSession {
    pub(super) const fn new(handle: PendingBookHandle) -> Self {
        Self { handle, events: None, bbo: BTreeMap::new(), trades: None }
    }

    /// The next event while anything is subscribed, pending otherwise. Cancel-safe.
    pub(super) async fn next(&mut self) -> Result<PendingEvent, RecvError> {
        let Some(events) = self.events.as_mut() else { return std::future::pending().await };
        events.recv().await
    }

    /// An event of the pending book for the subscriptions. Returns false when the connection must be dropped.
    pub(super) async fn on_event(
        &mut self,
        outbound: &Outbound,
        manager: &mut SubscriptionManager,
        received: Result<PendingEvent, RecvError>,
    ) -> bool {
        match received {
            Ok(PendingEvent::Availability(state)) => self.on_availability(outbound, &state).await,
            Ok(PendingEvent::Bbos(frames)) => {
                for frame in frames.iter() {
                    let Some(told) = self.bbo.get_mut(&frame.coin).filter(|told| told.takes(frame.seq)) else { continue };
                    told.seq = frame.seq;
                    if !outbound.send_frame(bbo_wire(frame)).await {
                        return false;
                    }
                }
                true
            }
            Ok(PendingEvent::Trades(frame)) => self.on_trades(outbound, &frame).await,
            Err(RecvError::Lagged(skipped)) => {
                let reason = format!("a broadcast gap of {skipped} events; resubscribe");
                self.withdraw_all(outbound, manager, &reason).await
            }
            Err(RecvError::Closed) => self.withdraw_all(outbound, manager, "the pending thread ended").await,
        }
    }

    /// Subscribe / unsubscribe of an `extPendingBbo` or `extPendingTrades` subscription. Returns false when the
    /// connection must be dropped.
    pub(super) async fn on_client_message(
        &mut self,
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
            self.forget(&subscription);
            return outbound.send_message(ServerResponse::SubscriptionResponse(client_message)).await;
        }
        let trades_filter = match &subscription {
            Subscription::PendingTrades(params) => match trades_filter(params) {
                Ok(filter) => Some(filter),
                Err(reason) => return outbound.send_message(ServerResponse::Error(format!("Invalid subscription: {sub}: {reason}"))).await,
            },
            _ => None,
        };
        match manager.subscribe(subscription.clone()) {
            Ok(true) => {}
            Ok(false) => return outbound.send_message(ServerResponse::Error(format!("Already subscribed: {sub}"))).await,
            Err(err) => return outbound.send_message(ServerResponse::Error(format!("Rejected subscription: {err}"))).await,
        }
        // listening before the store is read: no event after what it shows is missed
        if self.events.is_none() {
            self.events = Some(self.handle.subscribe());
        }
        if !outbound.send_message(ServerResponse::SubscriptionResponse(client_message)).await {
            return false;
        }
        match (subscription, trades_filter) {
            (Subscription::PendingBbo(PendingBboParams { coin }), _) => {
                let coin = Coin::new(&coin);
                let state = self.handle.snapshot();
                let wire = bbo_state_wire(&coin, &state);
                self.bbo.insert(coin, Told::of(&state));
                outbound.send_frame(wire).await
            }
            (Subscription::PendingTrades(params), Some(filter)) => {
                let state = self.handle.snapshot();
                let channel = self.trades.get_or_insert_with(|| TradesChannel { told: Told::of(&state), filters: HashMap::new() });
                channel.filters.insert(params, (filter, state.seq));
                outbound.send_frame(trades_state_wire(channel.told.availability)).await
            }
            (other, _) => unreachable!("the pending book's session takes its own subscriptions alone, not {other:?}"),
        }
    }

    /// The book's availability changed: each subscription not past it is told the state as of the change.
    async fn on_availability(&mut self, outbound: &Outbound, state: &Snapshot) -> bool {
        for (coin, told) in &mut self.bbo {
            if !told.passes(state.seq) {
                continue;
            }
            *told = Told::of(state);
            if !outbound.send_frame(bbo_state_wire(coin, state)).await {
                return false;
            }
        }
        let Some(channel) = self.trades.as_mut().filter(|channel| channel.told.passes(state.seq)) else { return true };
        channel.told = Told::of(state);
        outbound.send_frame(trades_state_wire(channel.told.availability)).await
    }

    /// A step's trades the channel takes: those any filter admits, once.
    async fn on_trades(&mut self, outbound: &Outbound, frame: &TradesFrame) -> bool {
        let Some(channel) = self.trades.as_mut().filter(|channel| channel.told.takes(frame.seq)) else { return true };
        channel.told.seq = frame.seq;
        let admitted = channel.admitted(frame);
        admitted.is_empty() || outbound.send_frame(trades_wire(frame, &admitted)).await
    }

    fn forget(&mut self, subscription: &Subscription) {
        match subscription {
            Subscription::PendingBbo(PendingBboParams { coin }) => drop(self.bbo.remove(&Coin::new(coin))),
            Subscription::PendingTrades(params) => self.leave_trades(params),
            _ => {}
        }
        if self.bbo.is_empty() && self.trades.is_none() {
            self.events = None;
        }
    }

    /// The channel ends with its last filter.
    fn leave_trades(&mut self, params: &PendingTradesParams) {
        let Some(channel) = self.trades.as_mut() else { return };
        channel.filters.remove(params);
        if channel.filters.is_empty() {
            self.trades = None;
        }
    }

    /// Every subscription ends with the reason.
    async fn withdraw_all(&mut self, outbound: &Outbound, manager: &mut SubscriptionManager, reason: &str) -> bool {
        let bbo = std::mem::take(&mut self.bbo).into_keys().map(|coin| Subscription::PendingBbo(PendingBboParams { coin: coin.value() }));
        let trades = self.trades.take().into_iter().flat_map(|channel| channel.filters.into_keys()).map(Subscription::PendingTrades);
        let subscriptions: Vec<Subscription> = bbo.chain(trades).collect();
        self.events = None;
        for subscription in subscriptions {
            let sub = serde_json::to_string(&subscription).unwrap_or_default();
            let channel = subscription.type_label().to_string();
            manager.unsubscribe(subscription);
            if !outbound.send_message(ServerResponse::Error(format!("{channel} withdrawn: {sub}: {reason}"))).await {
                return false;
            }
        }
        true
    }
}

/// An `extPendingTrades` subscription's filters; a user that is not an address refuses it.
fn trades_filter(params: &PendingTradesParams) -> Result<TradesFilter, String> {
    let user = params.user.as_deref().map(|user| user.parse::<Address>().map_err(|err| format!("user {user:?}: {err}"))).transpose()?;
    Ok(TradesFilter { coin: params.coin.clone(), user })
}

#[derive(Serialize)]
struct Envelope<T> {
    channel: &'static str,
    data: T,
}

#[expect(clippy::expect_used, reason = "the DTOs above serialize: plain fields, no maps with non-string keys")]
fn wire<T: Serialize>(channel: &'static str, data: T) -> bytes::Bytes {
    bytes::Bytes::from(serde_json::to_vec(&Envelope { channel, data }).expect("serializable DTOs"))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BboData {
    coin: String,
    status: &'static str,
    bbo: [Option<Level>; 2],
    basis_bbo: [Option<Level>; 2],
    basis_height: u64,
    basis_time_ms: u64,
    pending: PendingData,
    published_ms: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PendingData {
    actions: usize,
    oldest_seen_ms: Option<u64>,
    newest_seen_ms: Option<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusData {
    #[serde(skip_serializing_if = "Option::is_none")]
    coin: Option<String>,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TradesData {
    status: &'static str,
    basis_height: u64,
    basis_time_ms: u64,
    trades: Vec<TradeData>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TradeData {
    coin: String,
    /// The taker's, as `trades`' `side`.
    side: Side,
    px: String,
    sz: String,
    seen_ms: u64,
    maker: MakerData,
    taker: TakerData,
}

/// A replica order's maker; a pending order's action; or none named, the level's queue order not proven.
#[derive(Serialize)]
#[serde(untagged)]
enum MakerData {
    Confirmed { user: String, oid: u64, cloid: Option<String> },
    Pending { pending: ActionData },
    Unattributed { unattributed: bool },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ActionData {
    tx_hash: B256,
    /// The signed action's place in the line, and the order's in the action.
    action: usize,
    item: usize,
    cloid: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TakerData {
    tx_hash: B256,
    action: usize,
    item: usize,
    cloid: Option<String>,
    vault: Option<String>,
}

fn levels(best: BestLevels) -> [Option<Level>; 2] {
    <[_; 2]>::from(best).map(|level| level.map(|(px, sz, n)| Level::new(px.to_str(), sz.to_str(), n as usize)))
}

fn bbo_wire(frame: &BboFrame) -> bytes::Bytes {
    wire(EXT_PENDING_BBO, BboData {
        coin: frame.coin.value(),
        status: "ready",
        bbo: levels(frame.bbo),
        basis_bbo: levels(frame.basis_bbo),
        basis_height: frame.basis.height,
        basis_time_ms: frame.basis.time_ms,
        pending: PendingData {
            actions: frame.pending.actions,
            oldest_seen_ms: frame.pending.oldest_seen_ms,
            newest_seen_ms: frame.pending.newest_seen_ms,
        },
        published_ms: frame.published_ms,
    })
}

fn unavailable_wire(channel: &'static str, coin: Option<&Coin>, reason: &'static str) -> bytes::Bytes {
    wire(channel, StatusData { coin: coin.map(Coin::value), status: "unavailable", reason: Some(reason) })
}

/// A coin's frame in `state`, or why the book is not served.
fn bbo_state_wire(coin: &Coin, state: &Snapshot) -> bytes::Bytes {
    match state.frame(coin) {
        Ok(frame) => bbo_wire(&frame),
        Err(reason) => unavailable_wire(EXT_PENDING_BBO, Some(coin), reason.label()),
    }
}

fn trades_state_wire(availability: Result<(), Unavailable>) -> bytes::Bytes {
    match availability {
        Ok(()) => wire(EXT_PENDING_TRADES, StatusData { coin: None, status: "ready", reason: None }),
        Err(reason) => unavailable_wire(EXT_PENDING_TRADES, None, reason.label()),
    }
}

fn trades_wire(frame: &TradesFrame, trades: &[&PendingTrade]) -> bytes::Bytes {
    let trades = trades.iter().map(|trade| trade_data(trade)).collect();
    wire(EXT_PENDING_TRADES, TradesData { status: "ready", basis_height: frame.basis.height, basis_time_ms: frame.basis.time_ms, trades })
}

fn trade_data(trade: &PendingTrade) -> TradeData {
    let action = |id: ActionId, cloid: Option<&String>| ActionData { tx_hash: id.tx_hash, action: id.action, item: id.item, cloid: cloid.cloned() };
    let maker = match &trade.maker {
        Maker::Confirmed { user, oid, cloid } => MakerData::Confirmed { user: user_text(*user), oid: oid.clone().value(), cloid: cloid.clone() },
        Maker::Pending { id, cloid } => MakerData::Pending { pending: action(*id, cloid.as_ref()) },
        Maker::Unattributed => MakerData::Unattributed { unattributed: true },
    };
    TradeData {
        coin: trade.coin.value(),
        side: trade.side,
        px: trade.px.to_str(),
        sz: trade.sz.to_str(),
        seen_ms: trade.seen_ms(),
        maker,
        taker: TakerData {
            tx_hash: trade.taker.tx_hash,
            action: trade.taker.action,
            item: trade.taker.item,
            cloid: trade.taker_cloid.clone(),
            vault: trade.vault.map(user_text),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::mpsc;

    use super::*;
    use crate::{
        order_book::{Oid, Px, Sz},
        pending::publish::{Basis, CoinPending, FrameSet},
    };

    const BASIS: Basis = Basis { height: 30, time_ms: 3_000 };
    const ALICE: Address = Address::repeat_byte(0xa1);
    const VAULT: Address = Address::repeat_byte(0x7a);

    fn outbound() -> (Outbound, mpsc::Receiver<bytes::Bytes>) {
        let (data_tx, data_rx) = mpsc::channel(64);
        let (pong_tx, _pong_rx) = mpsc::channel(1);
        (Outbound::for_test(data_tx, pong_tx), data_rx)
    }

    fn json(bytes: &bytes::Bytes) -> serde_json::Value {
        serde_json::from_slice(bytes).unwrap()
    }

    fn bbo(coin: &str) -> Subscription {
        Subscription::PendingBbo(PendingBboParams { coin: coin.to_string() })
    }

    fn trades(coin: Option<&str>, user: Option<Address>) -> Subscription {
        Subscription::PendingTrades(PendingTradesParams { coin: coin.map(str::to_string), user: user.map(user_text) })
    }

    fn frame(seq: u64, coin: &str, ask: &str) -> BboFrame {
        let level = Some((Px::parse_from_str(ask).unwrap(), Sz::parse_from_str("1").unwrap(), 1));
        BboFrame { seq, coin: Coin::new(coin), bbo: (None, level), basis_bbo: (None, None), basis: BASIS, pending: CoinPending::default(), published_ms: 9 }
    }

    fn bbos(frames: Vec<BboFrame>) -> PendingEvent {
        PendingEvent::Bbos(Arc::new(frames.into_iter().map(Arc::new).collect()))
    }

    fn failure(seq: u64, reason: Unavailable) -> PendingEvent {
        PendingEvent::Availability(Arc::new(Snapshot { seq, served: Err(reason) }))
    }

    /// Served again at `seq`, the frames committed with it.
    fn recovery(seq: u64, frames: Vec<BboFrame>) -> PendingEvent {
        PendingEvent::Availability(Arc::new(Snapshot { seq, served: Ok(FrameSet::for_test(frames, BASIS)) }))
    }

    fn trades_event(seq: u64, coin: &str) -> PendingEvent {
        PendingEvent::Trades(Arc::new(TradesFrame { seq, basis: BASIS, trades: vec![trade(coin, Maker::Unattributed, None)] }))
    }

    struct Connection {
        session: PendingBookSession,
        outbound: Outbound,
        sent: mpsc::Receiver<bytes::Bytes>,
        manager: SubscriptionManager,
    }

    impl Connection {
        fn new(handle: &PendingBookHandle) -> Self {
            let (outbound, sent) = outbound();
            Self { session: PendingBookSession::new(handle.clone()), outbound, sent, manager: SubscriptionManager::default() }
        }

        /// Subscribes: the response, then the first frame's data.
        async fn subscribe(&mut self, subscription: Subscription) -> serde_json::Value {
            let message = ClientMessage::Subscribe { subscription: subscription.clone() };
            assert!(self.session.on_client_message(&self.outbound, &mut self.manager, message, subscription).await);
            assert_eq!(self.next()["channel"], "subscriptionResponse");
            self.next()["data"].clone()
        }

        async fn unsubscribe(&mut self, subscription: Subscription) {
            let message = ClientMessage::Unsubscribe { subscription: subscription.clone() };
            assert!(self.session.on_client_message(&self.outbound, &mut self.manager, message, subscription).await);
            assert_eq!(self.next()["channel"], "subscriptionResponse");
        }

        async fn deliver(&mut self, event: PendingEvent) {
            assert!(self.session.on_event(&self.outbound, &mut self.manager, Ok(event)).await);
        }

        /// What the queue holds now, in order.
        async fn take_queued(&mut self) {
            while let Ok(event) = self.session.events.as_mut().unwrap().try_recv() {
                self.deliver(event).await;
            }
        }

        fn next(&mut self) -> serde_json::Value {
            json(&self.sent.try_recv().unwrap())
        }

        fn quiet(&mut self) -> bool {
            self.sent.try_recv().is_err()
        }
    }

    /// A coin's subscription takes its latest frame at once, then each newer moved frame of its coin; an older one
    /// still queued is skipped, another coin's ignored.
    #[tokio::test]
    async fn a_bbo_subscription_takes_its_frame_then_each_newer_one() {
        let handle = PendingBookHandle::new();
        handle.store_for_test(5, Ok(()), vec![frame(5, "BTC", "101")], BASIS);
        let mut connection = Connection::new(&handle);
        let first = connection.subscribe(bbo("BTC")).await;
        assert_eq!(first["status"], "ready");
        assert_eq!(first["bbo"], serde_json::json!([null, {"px": "101", "sz": "1", "n": 1}]));
        assert_eq!((first["basisHeight"].as_u64(), first["basisTimeMs"].as_u64()), (Some(30), Some(3_000)));
        assert_eq!(first["pending"], serde_json::json!({"actions": 0, "oldestSeenMs": null, "newestSeenMs": null}));
        connection.deliver(bbos(vec![frame(4, "BTC", "100"), frame(6, "ETH", "2000"), frame(7, "BTC", "102")])).await;
        assert_eq!(connection.next()["data"]["bbo"][1]["px"], "102");
        assert!(connection.quiet());
    }

    /// The book turning unavailable and available again tells each subscription once, as of the change: an
    /// `extPendingBbo` coin its unavailability, then its frame committed with the recovery; `extPendingTrades` its
    /// status. The recovery step's frames, older than it, are not sent again.
    #[tokio::test]
    async fn availability_changes_tell_each_subscription_their_state() {
        let handle = PendingBookHandle::new();
        handle.store_for_test(2, Ok(()), vec![frame(2, "BTC", "101")], BASIS);
        let mut connection = Connection::new(&handle);
        connection.subscribe(bbo("BTC")).await;
        assert_eq!(connection.subscribe(trades(None, None)).await, serde_json::json!({"status": "ready"}));
        connection.deliver(failure(3, Unavailable::BlockLogBehind)).await;
        assert_eq!(connection.next()["data"], serde_json::json!({"coin": "BTC", "status": "unavailable", "reason": "block_log_behind"}));
        assert_eq!(connection.next()["data"], serde_json::json!({"status": "unavailable", "reason": "block_log_behind"}));
        connection.deliver(failure(3, Unavailable::BlockLogBehind)).await;
        assert!(connection.quiet(), "told already");
        connection.deliver(recovery(5, vec![frame(4, "BTC", "103")])).await;
        assert_eq!(connection.next()["data"]["bbo"][1]["px"], "103");
        assert_eq!(connection.next()["data"], serde_json::json!({"status": "ready"}));
        connection.deliver(bbos(vec![frame(4, "BTC", "103")])).await;
        assert!(connection.quiet(), "in the recovery's book");
    }

    /// Review 000614 r1 P1-3: BTC's frame and a trade wait in the connection's queue (ETH subscribed); the book then
    /// turns unavailable and the connection subscribes BTC's book and the trades - each told unavailable. The queued
    /// events are older than the store it read: neither goes out as ready, nor does the change it already holds.
    #[tokio::test]
    async fn a_subscription_skips_the_events_its_snapshot_holds() {
        let handle = PendingBookHandle::new();
        handle.store_for_test(4, Ok(()), vec![frame(4, "ETH", "2000")], BASIS);
        let mut connection = Connection::new(&handle);
        connection.subscribe(bbo("ETH")).await;
        handle.send_for_test(bbos(vec![frame(5, "BTC", "101")]));
        handle.send_for_test(trades_event(6, "BTC"));
        handle.store_for_test(7, Err(Unavailable::Mempool("loss")), vec![frame(5, "BTC", "101")], BASIS);
        handle.send_for_test(failure(7, Unavailable::Mempool("loss")));
        assert_eq!(connection.subscribe(bbo("BTC")).await["status"], "unavailable");
        assert_eq!(connection.subscribe(trades(None, None)).await["status"], "unavailable");
        connection.take_queued().await;
        assert_eq!(connection.next()["data"], serde_json::json!({"coin": "ETH", "status": "unavailable", "reason": "loss"}), "ETH alone not told yet");
        assert!(connection.quiet());
    }

    /// Review 000614 r2 P1-1: subscribed while served, a connection takes late a failure, a recovery and a failure
    /// again, the store at the last with BTC at 99: each state is told in order, the recovery with its own book (100),
    /// never the later failure's as ready.
    #[tokio::test]
    async fn a_late_consumer_is_told_each_state_as_of_it() {
        let handle = PendingBookHandle::new();
        handle.store_for_test(5, Ok(()), vec![frame(5, "BTC", "101")], BASIS);
        let mut connection = Connection::new(&handle);
        connection.subscribe(bbo("BTC")).await;
        connection.subscribe(trades(None, None)).await;
        handle.send_for_test(failure(6, Unavailable::BlockLogBehind));
        handle.send_for_test(recovery(9, vec![frame(8, "BTC", "100")]));
        handle.store_for_test(11, Err(Unavailable::Span), vec![frame(10, "BTC", "99")], BASIS);
        handle.send_for_test(failure(11, Unavailable::Span));
        connection.take_queued().await;
        let told: Vec<serde_json::Value> = std::iter::from_fn(|| connection.sent.try_recv().ok()).map(|bytes| json(&bytes)["data"].clone()).collect();
        let bbo_ready = told[2].clone();
        assert_eq!(told, [
            serde_json::json!({"coin": "BTC", "status": "unavailable", "reason": "block_log_behind"}),
            serde_json::json!({"status": "unavailable", "reason": "block_log_behind"}),
            bbo_ready.clone(),
            serde_json::json!({"status": "ready"}),
            serde_json::json!({"coin": "BTC", "status": "unavailable", "reason": "span"}),
            serde_json::json!({"status": "unavailable", "reason": "span"}),
        ]);
        assert_eq!((bbo_ready["status"].as_str(), bbo_ready["bbo"][1]["px"].as_str()), (Some("ready"), Some("100")));
    }

    /// Review 000614 r2 P1-1: a recovery, a frame and a trade queued for a connection subscribed while unavailable,
    /// taken late: each goes out in order - the trade not lost to the store having moved past it.
    #[tokio::test]
    async fn a_trade_after_a_recovery_reaches_a_late_consumer() {
        let handle = PendingBookHandle::new();
        handle.store_for_test(3, Err(Unavailable::WarmingUp), vec![], BASIS);
        let mut connection = Connection::new(&handle);
        connection.subscribe(bbo("BTC")).await;
        connection.subscribe(trades(None, None)).await;
        handle.send_for_test(recovery(6, vec![frame(5, "BTC", "101")]));
        handle.send_for_test(bbos(vec![frame(7, "BTC", "102")]));
        handle.send_for_test(three_trades(8));
        handle.store_for_test(8, Ok(()), vec![frame(7, "BTC", "102")], BASIS);
        connection.take_queued().await;
        assert_eq!(connection.next()["data"]["bbo"][1]["px"], "101");
        assert_eq!(connection.next()["data"], serde_json::json!({"status": "ready"}));
        assert_eq!(connection.next()["data"]["bbo"][1]["px"], "102");
        assert_eq!(connection.next()["data"]["trades"].as_array().unwrap().len(), 3);
        assert!(connection.quiet());
    }

    /// Review 000614 r3 P2: a BTC filter added to a connection taking ETH's trades, a BTC trade and a failure queued:
    /// told ready where the channel stands, it takes no trade published before it joined - only the failure follows.
    #[tokio::test]
    async fn a_joining_filter_takes_no_trade_before_it() {
        let handle = PendingBookHandle::new();
        handle.store_for_test(1, Ok(()), vec![], BASIS);
        let mut connection = Connection::new(&handle);
        connection.subscribe(trades(Some("ETH"), None)).await;
        handle.send_for_test(trades_event(2, "BTC"));
        handle.store_for_test(3, Err(Unavailable::BlockLogBehind), vec![], BASIS);
        handle.send_for_test(failure(3, Unavailable::BlockLogBehind));
        assert_eq!(connection.subscribe(trades(Some("BTC"), None)).await, serde_json::json!({"status": "ready"}));
        connection.take_queued().await;
        assert_eq!(connection.next()["data"], serde_json::json!({"status": "unavailable", "reason": "block_log_behind"}));
        assert!(connection.quiet());
    }

    /// Review 000614 r2 P1-2: a BTC filter added to a connection taking all trades, a BTC trade and a failure queued:
    /// it joins the channel where it stands - told ready, as the channel last was -, then the trade goes out once and
    /// the failure after it; never unavailable, then the older trade as ready. A filter joining the channel told
    /// unavailable is told its reason.
    #[tokio::test]
    async fn a_trades_filter_joins_the_channel_where_it_stands() {
        let handle = PendingBookHandle::new();
        handle.store_for_test(1, Ok(()), vec![], BASIS);
        let mut connection = Connection::new(&handle);
        connection.subscribe(trades(None, None)).await;
        handle.send_for_test(trades_event(2, "BTC"));
        handle.store_for_test(3, Err(Unavailable::BlockLogBehind), vec![], BASIS);
        handle.send_for_test(failure(3, Unavailable::BlockLogBehind));
        assert_eq!(connection.subscribe(trades(Some("BTC"), None)).await, serde_json::json!({"status": "ready"}));
        connection.take_queued().await;
        assert_eq!(connection.next()["data"]["trades"][0]["coin"], "BTC");
        assert_eq!(connection.next()["data"], serde_json::json!({"status": "unavailable", "reason": "block_log_behind"}));
        assert!(connection.quiet());
        let joined = connection.subscribe(trades(None, Some(ALICE))).await;
        assert_eq!(joined, serde_json::json!({"status": "unavailable", "reason": "block_log_behind"}));
    }

    /// A subscription last told the book unavailable takes no frame or trade, however new, until told otherwise.
    #[tokio::test]
    async fn a_subscription_told_unavailable_takes_no_data() {
        let handle = PendingBookHandle::new();
        handle.store_for_test(3, Err(Unavailable::WarmingUp), vec![], BASIS);
        let mut connection = Connection::new(&handle);
        connection.subscribe(bbo("BTC")).await;
        connection.subscribe(trades(None, None)).await;
        connection.deliver(bbos(vec![frame(4, "BTC", "101")])).await;
        connection.deliver(three_trades(5)).await;
        assert!(connection.quiet());
    }

    /// A subscription made while the book is unavailable takes the reason.
    #[tokio::test]
    async fn a_subscription_while_unavailable_takes_the_reason() {
        let handle = PendingBookHandle::new();
        let mut connection = Connection::new(&handle);
        assert_eq!(connection.subscribe(bbo("BTC")).await, serde_json::json!({"coin": "BTC", "status": "unavailable", "reason": "replica"}));
    }

    fn trade(coin: &str, maker: Maker, vault: Option<Address>) -> PendingTrade {
        PendingTrade {
            coin: Coin::new(coin),
            side: Side::Bid,
            px: Px::parse_from_str("101").unwrap(),
            sz: Sz::parse_from_str("0.5").unwrap(),
            seen_ns: 3_050_000_000,
            maker,
            taker: ActionId { tx_hash: B256::repeat_byte(1), action: 2, item: 0 },
            taker_cloid: Some("0x01".to_string()),
            vault,
        }
    }

    fn three_trades(seq: u64) -> PendingEvent {
        let pending = Maker::Pending { id: ActionId { tx_hash: B256::repeat_byte(2), action: 0, item: 1 }, cloid: None };
        let alice = Maker::Confirmed { user: ALICE, oid: Oid::new(11), cloid: Some("0x0b".to_string()) };
        let trades = vec![trade("BTC", alice, None), trade("BTC", pending, Some(VAULT)), trade("ETH", Maker::Unattributed, None)];
        PendingEvent::Trades(Arc::new(TradesFrame { seq, basis: BASIS, trades }))
    }

    /// Each trades subscription takes what its filters admit: a coin, a user as the maker or the taker's vault.
    #[tokio::test]
    async fn trades_go_to_the_subscriptions_their_filters_admit() {
        let handle = PendingBookHandle::new();
        handle.store_for_test(1, Ok(()), vec![], BASIS);
        let mut connection = Connection::new(&handle);
        let filters = [(trades(None, None), 3), (trades(Some("ETH"), None), 1), (trades(None, Some(ALICE)), 1), (trades(None, Some(VAULT)), 1)];
        for (seq, (subscription, expected)) in (2..).zip(filters) {
            connection.subscribe(subscription.clone()).await;
            connection.deliver(three_trades(seq)).await;
            assert_eq!(connection.next()["data"]["trades"].as_array().unwrap().len(), expected, "{subscription:?}");
            assert!(connection.quiet());
            connection.unsubscribe(subscription).await;
        }
        assert!(connection.session.events.is_none(), "no subscription: not listening");
    }

    /// Review 000614 r1 P2: all trades and BTC's on one connection: a BTC trade goes once; with all unsubscribed, BTC's
    /// still once and ETH's not at all.
    #[tokio::test]
    async fn a_trade_goes_once_to_a_connection() {
        let handle = PendingBookHandle::new();
        handle.store_for_test(1, Ok(()), vec![], BASIS);
        let mut connection = Connection::new(&handle);
        connection.subscribe(trades(None, None)).await;
        connection.subscribe(trades(Some("BTC"), None)).await;
        connection.deliver(three_trades(2)).await;
        assert_eq!(connection.next()["data"]["trades"].as_array().unwrap().len(), 3);
        assert!(connection.quiet(), "one message");
        connection.unsubscribe(trades(None, None)).await;
        connection.deliver(three_trades(3)).await;
        let coins: Vec<String> = connection.next()["data"]["trades"].as_array().unwrap().iter().map(|trade| trade["coin"].as_str().unwrap().to_string()).collect();
        assert_eq!(coins, ["BTC", "BTC"]);
        assert!(connection.quiet());
    }

    /// The wire shapes of a trade: a replica maker, a pending one, none named.
    #[test]
    fn a_trade_on_the_wire() {
        let frame = TradesFrame {
            seq: 1,
            basis: BASIS,
            trades: vec![
                trade("BTC", Maker::Confirmed { user: ALICE, oid: Oid::new(11), cloid: None }, Some(VAULT)),
                trade("BTC", Maker::Pending { id: ActionId { tx_hash: B256::repeat_byte(2), action: 0, item: 1 }, cloid: Some("0x0c".to_string()) }, None),
                trade("BTC", Maker::Unattributed, None),
            ],
        };
        let wire = json(&trades_wire(&frame, &frame.trades.iter().collect::<Vec<_>>()));
        assert_eq!(wire["channel"], "extPendingTrades");
        let data = &wire["data"];
        assert_eq!((data["status"].as_str(), data["basisHeight"].as_u64()), (Some("ready"), Some(30)));
        let first = &data["trades"][0];
        assert_eq!((first["side"].as_str(), first["px"].as_str(), first["sz"].as_str(), first["seenMs"].as_u64()), (Some("B"), Some("101"), Some("0.5"), Some(3_050)));
        assert_eq!(first["maker"], serde_json::json!({"user": user_text(ALICE), "oid": 11, "cloid": null}));
        assert_eq!(first["taker"], serde_json::json!({"txHash": B256::repeat_byte(1).to_string(), "action": 2, "item": 0, "cloid": "0x01", "vault": user_text(VAULT)}));
        assert_eq!(data["trades"][1]["maker"], serde_json::json!({"pending": {"txHash": B256::repeat_byte(2).to_string(), "action": 0, "item": 1, "cloid": "0x0c"}}));
        assert_eq!(data["trades"][2]["maker"], serde_json::json!({"unattributed": true}));
    }

    /// A broadcast gap withdraws every subscription; the pending thread's end tells each it stopped.
    #[tokio::test]
    async fn a_gap_withdraws_and_the_end_tells_stopped() {
        let handle = PendingBookHandle::new();
        handle.store_for_test(2, Ok(()), vec![frame(2, "BTC", "101")], BASIS);
        let mut connection = Connection::new(&handle);
        connection.subscribe(bbo("BTC")).await;
        handle.end();
        connection.take_queued().await;
        assert_eq!(connection.next()["data"]["reason"], "stopped");
        assert!(connection.session.on_event(&connection.outbound, &mut connection.manager, Err(RecvError::Lagged(3))).await);
        assert_eq!(connection.next()["data"], r#"extPendingBbo withdrawn: {"type":"extPendingBbo","coin":"BTC"}: a broadcast gap of 3 events; resubscribe"#);
        assert!(connection.manager.subscriptions().is_empty() && connection.session.events.is_none());
    }

    /// An unknown parameter refuses the subscription.
    #[test]
    fn an_unknown_parameter_refuses_the_subscription() {
        let parse = |text: &str| serde_json::from_str::<ClientMessage>(text);
        assert!(parse(r#"{"method":"subscribe","subscription":{"type":"extPendingTrades","coins":["BTC"]}}"#).is_err());
        assert!(parse(r#"{"method":"subscribe","subscription":{"type":"extPendingBbo","coin":"BTC","nSigFigs":5}}"#).is_err());
        assert!(parse(r#"{"method":"subscribe","subscription":{"type":"extPendingTrades","user":"0x1"}}"#).is_ok(), "the address is checked by validate");
    }
}
