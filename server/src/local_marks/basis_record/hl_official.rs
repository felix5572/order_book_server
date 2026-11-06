//! The record's own HL official stream (hyperliquid-trade design 000782 r2 rule 3; user 2026-11-06 "实时性不强的
//! 优先用和订阅官方api"): `activeAssetCtx` of every recorded coin on HL's public websocket - its oracle, mark, mid and
//! funding as HL's API publishes them, beside the node's leg and whatever that node's lag.
//!
//! The wire (bm 2026-11-06, read-only probe): the subscription's echo, a first frame within about 0.1 s, then a frame a
//! coin about every 1 s whether or not a value moved; `ctx` values are strings, no time. A coin HL does not list is
//! answered `{"channel":"error","data":"Invalid subscription {...}"}`, the connection and its other coins untouched.
//! HL's docs (`PerpsAssetCtx`) list `oraclePx`, `markPx`, `funding` and an optional `midPx`; the other fields are not
//! taken.
//!
//! Connections of at most `SUBSCRIPTIONS_PER_CONNECTION` coins, every attempt paced by one `ConnectPacer` the shards
//! share: one attempt at a time, the next `MIN_CONNECT_SPACING` after the previous one's subscriptions were sent or it
//! failed, so a 60 s window holds at most 9 attempts - 900 subscription messages -, under HL's 2,000 messages a minute
//! of an IP across its websockets (design 000782 r1 P2, r2 P3). No JSON `ping`: a frame comes every second, and
//! `Timeouts::FEEDS.read` without one reconnects, as the record's Binance streams do.
//!
//! A coin reads at the sampler's time: `subscriptionRejected` (HL's answer, until restart), `stopped` (its connection's
//! contract broken, until restart), `notConnected` (a lost connection's values are never shown, a shard waiting its turn
//! included), `notReceived` on this connection, `expired` past `MAX_FRAME_AGE_MS`, else its latest values.

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use log::{error, warn};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::Deserialize;
use sonic_rs::LazyValue;
use tokio::{
    sync::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard},
    time::{Instant, sleep, sleep_until, timeout},
};
use yawc::{Frame, OpCode, Options, WebSocket};

use super::binance::{positive, signed_decimal};
use crate::{
    local_marks::{
        connection::{FeedError, Timeouts, close_error, connect_error, read_error, send},
        feeds::{BACKOFF_MAX, BACKOFF_MIN, HEALTHY_FOR},
        now_ms,
        price::VenuePrice,
        protocol::{excerpt, strict},
    },
    metrics::{BASIS_RECORD_STREAM_DISCONNECTS_TOTAL, BASIS_RECORD_STREAM_STOPPED},
};

const HOST: &str = "wss://api.hyperliquid.xyz/ws";
const LABEL: &str = "hl_official";
/// Coins a connection: three connections for the record's 277, under HL's 10 connections and 1,000 subscriptions of an
/// IP (rate-limits page, 2026-11-06).
const SUBSCRIPTIONS_PER_CONNECTION: usize = 100;
/// From the end of one attempt's subscriptions (or its failure) to the start of the next attempt, any shard's.
const MIN_CONNECT_SPACING: Duration = Duration::from_secs(7);
/// A coin's frame older than this at the sampler's time is `expired`: HL pushes one about every second, and 5 s is the
/// local marks' progress limit (`MAX_PROGRESS_LAG_MS`).
const MAX_FRAME_AGE_MS: u64 = 5_000;
/// Larger than any frame of the stream (a frame is about 400 bytes).
const MAX_PAYLOAD: usize = 1 << 20;
const REJECTED_PREFIX: &str = "Invalid subscription ";
const CHANNEL: &str = "activeAssetCtx";

/// A coin's latest `activeAssetCtx`: HL's prices as it wrote them, checked positive, its funding signed, our receipt time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AssetCtx {
    pub(crate) oracle: VenuePrice,
    pub(crate) mark: VenuePrice,
    /// HL leaves it out of a book without both sides.
    pub(crate) mid: Option<VenuePrice>,
    pub(crate) funding: String,
    pub(crate) received_ms: u64,
}

/// Why a coin has no values at the sampler's time: `code` names the state, `detail` gives the evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Absent {
    pub(crate) code: &'static str,
    pub(crate) detail: String,
}

impl Absent {
    fn new(code: &'static str, detail: impl Into<String>) -> Self {
        Self { code, detail: detail.into() }
    }
}

/// One shard's state: its connection, the values received on it, HL's refusals and a stop, each until restart.
#[derive(Debug, Clone, Default)]
struct ShardState {
    connected: bool,
    /// Received on the live connection only: cleared when it ends.
    values: FxHashMap<String, AssetCtx>,
    rejected: FxHashMap<String, String>,
    stopped: Option<String>,
}

/// Every shard's state; the connections write, the sampler copies.
pub(crate) struct HlOfficialLatest {
    shards: Vec<Mutex<ShardState>>,
    shard_of: Arc<FxHashMap<String, usize>>,
}

impl HlOfficialLatest {
    fn new(shards: &[Shard]) -> Self {
        let shard_of = shards.iter().flat_map(|shard| shard.coins.iter().map(|coin| (coin.clone(), shard.index))).collect();
        Self { shards: shards.iter().map(|_| Mutex::new(ShardState::default())).collect(), shard_of: Arc::new(shard_of) }
    }

    fn lock(&self, shard: usize) -> std::sync::MutexGuard<'_, ShardState> {
        self.shards[shard].lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Every shard as it stands, judged at the sampler's time after (`HlOfficialCopy::reading`).
    pub(crate) fn copy(&self) -> HlOfficialCopy {
        HlOfficialCopy { shards: (0..self.shards.len()).map(|shard| self.lock(shard).clone()).collect(), shard_of: Arc::clone(&self.shard_of) }
    }

    fn connected(&self, shard: usize) {
        let mut state = self.lock(shard);
        state.connected = true;
        state.values.clear();
    }

    fn disconnected(&self, shard: usize) {
        let mut state = self.lock(shard);
        state.connected = false;
        state.values.clear();
    }

    fn rejected(&self, shard: usize) -> FxHashSet<String> {
        self.lock(shard).rejected.keys().cloned().collect()
    }

    fn reject(&self, shard: usize, coin: &str, answer: &str) {
        self.lock(shard).rejected.insert(coin.to_string(), answer.to_string());
    }

    fn record(&self, shard: usize, coin: &str, ctx: AssetCtx) {
        self.lock(shard).values.insert(coin.to_string(), ctx);
    }

    fn stop(&self, shard: usize, reason: &str) {
        let mut state = self.lock(shard);
        state.connected = false;
        state.values.clear();
        state.stopped = Some(reason.to_string());
    }
}

/// The shards copied at one moment.
pub(crate) struct HlOfficialCopy {
    shards: Vec<ShardState>,
    shard_of: Arc<FxHashMap<String, usize>>,
}

impl HlOfficialCopy {
    /// `coin`'s values at `read_ms`, or why there are none.
    pub(crate) fn reading(&self, coin: &str, read_ms: u64) -> Result<AssetCtx, Absent> {
        let Some(&shard) = self.shard_of.get(coin) else {
            return Err(Absent::new("notSubscribed", "the record subscribes no such coin"));
        };
        let state = &self.shards[shard];
        if let Some(answer) = state.rejected.get(coin) {
            return Err(Absent::new("subscriptionRejected", answer.clone()));
        }
        if let Some(reason) = &state.stopped {
            return Err(Absent::new("stopped", reason.clone()));
        }
        if !state.connected {
            return Err(Absent::new("notConnected", ""));
        }
        let Some(ctx) = state.values.get(coin) else {
            return Err(Absent::new("notReceived", "nothing received on this connection"));
        };
        let age_ms = read_ms.saturating_sub(ctx.received_ms);
        if age_ms > MAX_FRAME_AGE_MS {
            return Err(Absent::new("expired", format!("the last frame was received {age_ms} ms before")));
        }
        Ok(ctx.clone())
    }

    /// No shard, for a test.
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self { shards: Vec::new(), shard_of: Arc::new(FxHashMap::default()) }
    }

    /// One connected shard holding `coin`'s `ctx`, for a test.
    #[cfg(test)]
    pub(crate) fn with(coin: &str, ctx: AssetCtx) -> Self {
        let state = ShardState { connected: true, values: FxHashMap::from_iter([(coin.to_string(), ctx)]), ..ShardState::default() };
        Self { shards: vec![state], shard_of: Arc::new(FxHashMap::from_iter([(coin.to_string(), 0)])) }
    }
}

/// One connection's coins.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Shard {
    index: usize,
    coins: Vec<String>,
}

fn shards(coins: &BTreeSet<String>) -> Vec<Shard> {
    let coins: Vec<&String> = coins.iter().collect();
    coins
        .chunks(SUBSCRIPTIONS_PER_CONNECTION)
        .enumerate()
        .map(|(index, chunk)| Shard { index, coins: chunk.iter().map(|coin| (*coin).clone()).collect() })
        .collect()
}

fn subscribe_message(coin: &str) -> String {
    serde_json::json!({"method": "subscribe", "subscription": {"type": CHANNEL, "coin": coin}}).to_string()
}

/// Paces every connection attempt of the shards: one at a time, the next not before `spacing` after the previous
/// attempt's subscriptions were sent or it failed (`Turn`'s drop).
struct ConnectPacer {
    spacing: Duration,
    next: AsyncMutex<Instant>,
}

impl ConnectPacer {
    fn new(spacing: Duration) -> Self {
        Self { spacing, next: AsyncMutex::new(Instant::now()) }
    }

    /// Waits for this attempt's turn: the previous attempt done and the spacing since passed.
    async fn turn(&self) -> Turn<'_> {
        let next = self.next.lock().await;
        sleep_until(*next).await;
        Turn { next, spacing: self.spacing }
    }
}

/// An attempt's turn; dropped once its subscriptions are sent or it failed, it sets the next attempt's earliest start.
struct Turn<'a> {
    next: AsyncMutexGuard<'a, Instant>,
    spacing: Duration,
}

impl Drop for Turn<'_> {
    fn drop(&mut self) {
        *self.next = Instant::now() + self.spacing;
    }
}

/// Starts a connection per shard of `coins`; they run until the process ends or a contract error stops one.
pub(crate) fn start(coins: &BTreeSet<String>) -> Arc<HlOfficialLatest> {
    let shards = shards(coins);
    let latest = Arc::new(HlOfficialLatest::new(&shards));
    let pacer = Arc::new(ConnectPacer::new(MIN_CONNECT_SPACING));
    for shard in shards {
        let (latest, pacer) = (Arc::clone(&latest), Arc::clone(&pacer));
        tokio::spawn(async move { run_shard(&shard, &latest, &pacer).await });
    }
    latest
}

/// `turn` is moved into `read_shard`, which ends it once the subscriptions are sent (the lint does not follow the move).
#[allow(clippy::significant_drop_tightening)]
async fn run_shard(shard: &Shard, latest: &HlOfficialLatest, pacer: &ConnectPacer) {
    let mut backoff = BACKOFF_MIN;
    loop {
        // the backoff resets on the connection's own life, the wait for the turn not counted
        let turn = pacer.turn().await;
        let started = Instant::now();
        let error = read_shard(shard, latest, turn).await;
        latest.disconnected(shard.index);
        let reason = match error {
            FeedError::Contract(reason) => {
                error!("basis record {LABEL} shard {}: stopped until restart, its coins recorded unavailable: {reason}", shard.index);
                BASIS_RECORD_STREAM_STOPPED.with_label_values(&[LABEL]).set(1);
                latest.stop(shard.index, &reason);
                return;
            }
            FeedError::Transient(reason) | FeedError::ReconnectRequested(reason) | FeedError::ConnectionLimit(reason) => reason,
        };
        BASIS_RECORD_STREAM_DISCONNECTS_TOTAL.with_label_values(&[LABEL]).inc();
        if started.elapsed() >= HEALTHY_FOR {
            backoff = BACKOFF_MIN;
        }
        warn!("basis record {LABEL} shard {}: reconnecting in {backoff:?} (then paced): {reason}", shard.index);
        sleep(backoff).await;
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

/// One connection of `shard` until it fails: connect, subscribe each coin HL has not refused, end the turn, read.
async fn read_shard(shard: &Shard, latest: &HlOfficialLatest, turn: Turn<'_>) -> FeedError {
    let timeouts = Timeouts::FEEDS;
    let url = match url::Url::parse(HOST) {
        Ok(url) => url,
        Err(err) => return FeedError::Contract(format!("{LABEL}: URL: {err}")),
    };
    let options = Options::default().with_max_payload_read(MAX_PAYLOAD).with_low_latency_compression();
    let mut ws = match timeout(timeouts.connect, WebSocket::connect(url).with_options(options)).await {
        Ok(Ok(ws)) => ws,
        Ok(Err(err)) => return connect_error(LABEL, &err),
        Err(_) => return FeedError::Transient(format!("{LABEL}: no connection within {:?}", timeouts.connect)),
    };
    latest.connected(shard.index);
    let rejected = latest.rejected(shard.index);
    for coin in shard.coins.iter().filter(|coin| !rejected.contains(*coin)) {
        if let Err(error) = send(&mut ws, LABEL, Frame::text(subscribe_message(coin)), timeouts.send).await {
            return error;
        }
    }
    drop(turn);
    loop {
        let frame = match timeout(timeouts.read, ws.next_frame()).await {
            Ok(Ok(frame)) => frame,
            Ok(Err(err)) => return read_error(LABEL, &err),
            Err(_) => return FeedError::Transient(format!("{LABEL}: no frame for {:?}", timeouts.read)),
        };
        let text = match frame.opcode() {
            OpCode::Text => match std::str::from_utf8(frame.payload()) {
                Ok(text) => text,
                Err(err) => return FeedError::Contract(format!("{LABEL}: a text frame not UTF-8: {err}")),
            },
            OpCode::Close => return close_error(LABEL, &frame),
            OpCode::Ping | OpCode::Pong => continue,
            opcode => return FeedError::Contract(format!("{LABEL}: unexpected {opcode:?} frame")),
        };
        if let Err(err) = record(shard, text, latest, now_ms()) {
            return FeedError::Contract(format!("{LABEL}: {err}: {}", excerpt(text)));
        }
    }
}

#[derive(Deserialize)]
struct WireFrame<'a> {
    channel: &'a str,
    #[serde(borrow)]
    data: LazyValue<'a>,
}

#[derive(Deserialize)]
struct CtxUpdate<'a> {
    coin: &'a str,
    #[serde(borrow)]
    ctx: Ctx<'a>,
}

#[derive(Deserialize)]
struct Ctx<'a> {
    #[serde(rename = "oraclePx")]
    oracle: &'a str,
    #[serde(rename = "markPx")]
    mark: &'a str,
    #[serde(rename = "midPx")]
    mid: Option<&'a str>,
    funding: &'a str,
}

#[derive(Deserialize)]
struct Echo<'a> {
    method: &'a str,
    #[serde(borrow)]
    subscription: Subscription<'a>,
}

#[derive(Deserialize)]
struct Subscription<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    coin: &'a str,
}

/// Takes one frame of `shard` into `latest`: `Err` is the stream's contract broken.
fn record(shard: &Shard, text: &str, latest: &HlOfficialLatest, received_ms: u64) -> Result<(), String> {
    let frame: WireFrame<'_> = strict(text, "HL websocket frame")?;
    let data = frame.data.as_raw_str();
    match frame.channel {
        CHANNEL => {
            let update: CtxUpdate<'_> = strict(data, "HL activeAssetCtx")?;
            subscribed(shard, update.coin)?;
            let ctx = &update.ctx;
            let values = AssetCtx {
                oracle: positive(ctx.oracle, "oraclePx")?,
                mark: positive(ctx.mark, "markPx")?,
                mid: ctx.mid.map(|mid| positive(mid, "midPx")).transpose()?,
                funding: signed_decimal(ctx.funding, "funding")?,
                received_ms,
            };
            latest.record(shard.index, update.coin, values);
        }
        "subscriptionResponse" => {
            let echo: Echo<'_> = strict(data, "HL subscriptionResponse")?;
            if echo.method != "subscribe" || echo.subscription.kind != CHANNEL {
                return Err(format!("an echo of {} {}", echo.method, echo.subscription.kind));
            }
            subscribed(shard, echo.subscription.coin)?;
        }
        "error" => {
            let answer: String = strict(data, "HL error")?;
            let rejected = answer.strip_prefix(REJECTED_PREFIX).ok_or_else(|| format!("an error not of a subscription: {answer:?}"))?;
            let subscription: Subscription<'_> = strict(rejected, "HL rejected subscription")?;
            if subscription.kind != CHANNEL {
                return Err(format!("a rejected {} subscription", subscription.kind));
            }
            subscribed(shard, subscription.coin)?;
            warn!("basis record {LABEL}: HL refuses {} until restart: {answer}", subscription.coin);
            latest.reject(shard.index, subscription.coin, &answer);
        }
        channel => return Err(format!("channel {channel:?}")),
    }
    Ok(())
}

fn subscribed(shard: &Shard, coin: &str) -> Result<(), String> {
    if shard.coins.iter().any(|subscribed| subscribed == coin) { Ok(()) } else { Err(format!("coin {coin} not subscribed")) }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// bm 2026-11-06 09:4xZ, read-only probe of `wss://api.hyperliquid.xyz/ws`.
    const BTC_CTX: &str = r#"{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{"funding":"0.0000125","openInterest":"41583.04814","prevDayPx":"80315.0","dayNtlVlm":"5013905811.4442749023","premium":"-0.0002386973","oraclePx":"76247.2","markPx":"76230.0","midPx":"76228.5","impactPxs":["76215.5","76229.0"],"dayBaseVlm":"65127.6765800001"}}}"#;
    const SOXL_CTX: &str = r#"{"channel":"activeAssetCtx","data":{"coin":"xyz:SOXL","ctx":{"funding":"-0.0000509678","openInterest":"1819523.5200000003","prevDayPx":"60.436","dayNtlVlm":"769533938.6207995415","premium":"-0.0004721138","oraclePx":"47.658","markPx":"47.671","impactPxs":["47.635","47.636"],"dayBaseVlm":"16433702.6800000202"}}}"#;
    const ECHO: &str = r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"activeAssetCtx","coin":"BTC"}}}"#;
    const REFUSED: &str = r#"{"channel":"error","data":"Invalid subscription {\"type\":\"activeAssetCtx\",\"coin\":\"NOTACOIN\"}"}"#;

    fn shard(coins: &[&str]) -> Shard {
        Shard { index: 0, coins: coins.iter().map(|coin| (*coin).to_string()).collect() }
    }

    fn connected(coins: &[&str]) -> (Shard, HlOfficialLatest) {
        let shard = shard(coins);
        let latest = HlOfficialLatest::new(std::slice::from_ref(&shard));
        latest.connected(0);
        (shard, latest)
    }

    #[test]
    fn frames_take_oracle_mark_mid_and_funding() {
        let (shard, latest) = connected(&["BTC", "xyz:SOXL"]);
        record(&shard, ECHO, &latest, 1).unwrap();
        record(&shard, BTC_CTX, &latest, 10).unwrap();
        record(&shard, SOXL_CTX, &latest, 11).unwrap();
        let copy = latest.copy();
        let btc = copy.reading("BTC", 10).unwrap();
        assert_eq!((btc.oracle.to_string(), btc.mark.to_string(), btc.mid.map(|mid| mid.to_string()), btc.funding.as_str(), btc.received_ms),
            ("76247.2".into(), "76230".into(), Some("76228.5".into()), "0.0000125", 10));
        let soxl = copy.reading("xyz:SOXL", 11).unwrap();
        assert_eq!((soxl.mid, soxl.funding.as_str()), (None, "-0.0000509678"), "no midPx: none; a negative funding as written");
    }

    #[test]
    fn a_refused_coin_reads_refused_and_its_neighbours_go_on() {
        let (shard, latest) = connected(&["BTC", "NOTACOIN"]);
        record(&shard, REFUSED, &latest, 1).unwrap();
        record(&shard, BTC_CTX, &latest, 2).unwrap();
        let copy = latest.copy();
        let refused = copy.reading("NOTACOIN", 2).unwrap_err();
        assert_eq!((refused.code, refused.detail.as_str()), ("subscriptionRejected", r#"Invalid subscription {"type":"activeAssetCtx","coin":"NOTACOIN"}"#));
        assert!(copy.reading("BTC", 2).is_ok());
        latest.disconnected(0);
        latest.connected(0);
        assert_eq!(latest.copy().reading("NOTACOIN", 3).unwrap_err().code, "subscriptionRejected", "until restart");
        assert_eq!(latest.rejected(0), FxHashSet::from_iter(["NOTACOIN".to_string()]), "not subscribed again");
    }

    #[test]
    fn frames_off_the_contract_break_the_stream() {
        let (shard, latest) = connected(&["BTC"]);
        assert!(record(&shard, SOXL_CTX, &latest, 1).unwrap_err().contains("not subscribed"));
        assert!(record(&shard, &BTC_CTX.replace(r#""oraclePx":"76247.2","#, ""), &latest, 1).is_err(), "no oracle");
        assert!(record(&shard, &BTC_CTX.replace(r#""markPx":"76230.0""#, r#""markPx":"0""#), &latest, 1).is_err(), "a zero mark");
        assert!(record(&shard, &BTC_CTX.replace("activeAssetCtx", "l2Book"), &latest, 1).unwrap_err().contains("channel"));
        assert!(record(&shard, r#"{"channel":"error","data":"Something else"}"#, &latest, 1).is_err(), "an error HL gives no subscription of");
        assert!(record(&shard, &REFUSED.replace("NOTACOIN", "ETH"), &latest, 1).unwrap_err().contains("not subscribed"));
        assert!(record(&shard, &ECHO.replace("activeAssetCtx", "trades"), &latest, 1).is_err());
        assert!(record(&shard, r#"{"channel":"pong"}"#, &latest, 1).is_err(), "no JSON ping is sent, so no pong is expected");
        assert_eq!(latest.copy().reading("BTC", 1).unwrap_err().code, "notReceived");
    }

    /// Design 000782 r2 rule 3: a coin's state at the sampler's time.
    #[test]
    fn a_coin_reads_its_state_at_the_sampler_time() {
        let shard = shard(&["BTC"]);
        let latest = HlOfficialLatest::new(std::slice::from_ref(&shard));
        assert_eq!(latest.copy().reading("BTC", 0).unwrap_err().code, "notConnected", "before the first connection");
        latest.connected(0);
        assert_eq!(latest.copy().reading("BTC", 0).unwrap_err().code, "notReceived");
        record(&shard, BTC_CTX, &latest, 1_000).unwrap();
        let copy = latest.copy();
        assert!(copy.reading("BTC", 1_000 + MAX_FRAME_AGE_MS).is_ok());
        let expired = copy.reading("BTC", 1_001 + MAX_FRAME_AGE_MS).unwrap_err();
        assert_eq!((expired.code, expired.detail.as_str()), ("expired", "the last frame was received 5001 ms before"));
        assert_eq!(copy.reading("ETH", 1_000).unwrap_err().code, "notSubscribed");
        latest.disconnected(0);
        assert_eq!(latest.copy().reading("BTC", 1_000).unwrap_err().code, "notConnected", "a lost connection's values not shown");
        latest.connected(0);
        assert_eq!(latest.copy().reading("BTC", 1_000).unwrap_err().code, "notReceived", "nor on the next connection");
        latest.stop(0, "coin X not subscribed");
        assert_eq!(latest.copy().reading("BTC", 1_000).unwrap_err(), Absent::new("stopped", "coin X not subscribed"));
    }

    #[test]
    fn coins_split_by_connection_and_subscribe_by_name() {
        let coins: BTreeSet<String> = (0..277).map(|n| format!("C{n:03}")).collect();
        let all = shards(&coins);
        assert_eq!(all.iter().map(|shard| (shard.index, shard.coins.len())).collect::<Vec<_>>(), vec![(0, 100), (1, 100), (2, 77)]);
        let message: serde_json::Value = serde_json::from_str(&subscribe_message("xyz:SOXL")).unwrap();
        assert_eq!(message, serde_json::json!({"method": "subscribe", "subscription": {"type": "activeAssetCtx", "coin": "xyz:SOXL"}}));
    }

    /// Design 000782 r1 P2, r2 P3: three shards failing a second after each subscription, the slowest handshake taking
    /// most of `Timeouts::FEEDS.connect`: attempts one at a time, each start at least `MIN_CONNECT_SPACING` after the
    /// previous attempt's subscriptions, so any 60 s holds at most 900 subscription messages, counted when sent.
    #[tokio::test(start_paused = true)]
    async fn attempts_are_paced_across_shards() {
        let pacer = Arc::new(ConnectPacer::new(MIN_CONNECT_SPACING));
        let origin = Instant::now();
        let sends: Arc<Mutex<Vec<(Duration, usize)>>> = Arc::default();
        let mut tasks = Vec::new();
        for (index, coins) in [(0usize, 100usize), (1, 100), (2, 77)] {
            let (pacer, sends) = (Arc::clone(&pacer), Arc::clone(&sends));
            tasks.push(tokio::spawn(async move {
                let mut backoff = BACKOFF_MIN;
                while origin.elapsed() < Duration::from_secs(600) {
                    let turn = pacer.turn().await;
                    // a handshake of 9 s on the first shard, at once on the others
                    sleep(if index == 0 { Duration::from_secs(9) } else { Duration::ZERO }).await;
                    sends.lock().unwrap().push((origin.elapsed(), coins));
                    drop(turn);
                    sleep(Duration::from_secs(1)).await;
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        let sends = sends.lock().unwrap().clone();
        assert!(sends.len() > 60, "{} attempts", sends.len());
        for pair in sends.windows(2) {
            assert!(pair[1].0 >= pair[0].0 + MIN_CONNECT_SPACING, "{pair:?}");
        }
        for (start, _) in &sends {
            let window: usize = sends.iter().filter(|(at, _)| *at >= *start && *at < *start + Duration::from_secs(60)).map(|(_, coins)| coins).sum();
            assert!(window <= 900, "{window} subscriptions in the 60 s from {start:?}");
        }
    }
}
