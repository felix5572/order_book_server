//! The feeds' wire protocols (design 000454 r1 1.2, r2 revision 4): subscription requests, heartbeats, and the frames
//! they answer with. Ported from the `hl_mark_prices` service's parsers (validated on its captures) and checked on bm
//! frames of 10-21 (`local_marks/wire_fixtures.json`). A frame of a known type that does not parse, a refused
//! subscription or a crossed book is a contract error: it stops that connection partition. A frame of an unknown type is passed up as
//! `Unknown` for a warning with its raw text.

use std::time::Duration;

use serde::Deserialize;
use sonic_rs::LazyValue;

use super::{
    price::VenuePrice,
    slot::Quote,
    sources::{FeedMarkets, Kind, Venue},
};

/// Bybit subscribes 10 args per request (its documented limit); Kucoin 100 symbols per request (bm 10-21: 173 at once
/// answered error 509 "exceed max subscription count limitation of 100 per time"); the others took every market of
/// a partition in one request. Connection partitioning lives in `subscriptions`, independently of request chunking.
const BYBIT_ARGS_PER_REQUEST: usize = 10;
const KUCOIN_SYMBOLS_PER_REQUEST: usize = 100;
/// The service's heartbeat cadence; Kucoin's comes with its token.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

/// One market's observation: its index in the feed's markets, its quote (`None`: no two-sided book now) and the
/// venue's time of it.
pub(crate) type Observation = (usize, Option<Quote>, Option<u64>);

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Parsed {
    Quotes(Vec<Observation>),
    /// Subscription requests (or markets) acknowledged by this frame.
    Acked(usize),
    Pong,
    /// A documented control message asking this connection to rotate, not a quote or an acknowledgement.
    ReconnectRequested(String),
    /// The venue refuses another session of the account (Alpaca 406): retried within a fixed budget (design 000535 r2
    /// revision 5), then the feed stops.
    ConnectionLimit(String),
    /// A documented session failure that ends this connection but not the feed (Alpaca 407 slow client, 500 internal
    /// error): reconnected after the backoff.
    SessionFailed(String),
    /// A frame the protocol sends that carries nothing for us (status, welcome, the venue's own heartbeat).
    Ignored,
    /// A frame of no type we know: warned with its text, not taken.
    Unknown,
}

/// What a connection sends after connecting, and what it waits for.
#[derive(PartialEq, Eq)]
pub(crate) struct Session {
    pub(crate) url: String,
    /// The SIP session's first request carries the Alpaca key and secret: `Debug` shows only how many there are.
    pub(crate) requests: Vec<String>,
    /// Acknowledgements the requests are answered with.
    pub(crate) acks: usize,
    /// The heartbeat and its cadence; `None` where the server pings (Binance).
    pub(crate) heartbeat: Option<(Heartbeat, Duration)>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Session")
            .field("url", &self.url)
            .field("requests", &self.requests.len())
            .field("acks", &self.acks)
            .field("heartbeat", &self.heartbeat)
            .finish()
    }
}

/// How a connection keeps its heartbeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Heartbeat {
    /// The venue's text ping, answered with its pong frame.
    Text(String),
    /// A WebSocket ping, answered with a Pong frame (Alpaca defines no text ping; a quiet market sends nothing).
    WsPing,
}

/// The WS feeds; MEXC spot is polled (its WS is protobuf), Kucoin's URL needs a token first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Protocol {
    pub(crate) kind: Kind,
    pub(crate) venue: Venue,
}

impl Protocol {
    /// The session for `symbols`; `kucoin` is the token handshake's (endpoint with token, ping interval).
    pub(crate) fn session(self, symbols: &[String], now_s: u64, kucoin: Option<(&str, Duration)>) -> Session {
        let json = |value: sonic_rs::Value| value.to_string();
        let ping = |text: &str| Some((Heartbeat::Text(text.to_string()), HEARTBEAT_INTERVAL));
        match (self.kind, self.venue) {
            (Kind::Spot | Kind::Perp, Venue::Binance) => {
                let host = if self.kind == Kind::Spot { "wss://stream.binance.com:9443/stream" } else { "wss://fstream.binance.com/public/stream" };
                let streams: Vec<String> = symbols.iter().map(|symbol| format!("{}@bookTicker", symbol.to_lowercase())).collect();
                Session { url: format!("{host}?streams={}", streams.join("/")), requests: Vec::new(), acks: 0, heartbeat: None }
            }
            (Kind::Spot | Kind::Perp, Venue::Okx) => {
                let args: Vec<sonic_rs::Value> = symbols.iter().map(|symbol| sonic_rs::json!({"channel": "bbo-tbt", "instId": symbol})).collect();
                Session {
                    url: "wss://ws.okx.com:8443/ws/v5/public".to_string(),
                    requests: vec![json(sonic_rs::json!({"op": "subscribe", "args": args}))],
                    // one `subscribe` event per instrument (bm 10-21)
                    acks: symbols.len(),
                    heartbeat: ping("ping"),
                }
            }
            (Kind::Spot | Kind::Perp, Venue::Bybit) => {
                let url = if self.kind == Kind::Spot { "wss://stream.bybit.com/v5/public/spot" } else { "wss://stream.bybit.com/v5/public/linear" };
                let requests: Vec<String> = symbols
                    .chunks(BYBIT_ARGS_PER_REQUEST)
                    .map(|chunk| {
                        let args: Vec<String> = chunk.iter().map(|symbol| format!("orderbook.1.{symbol}")).collect();
                        json(sonic_rs::json!({"op": "subscribe", "args": args}))
                    })
                    .collect();
                Session { url: url.to_string(), acks: requests.len(), requests, heartbeat: ping(r#"{"op":"ping"}"#) }
            }
            (Kind::Spot | Kind::Perp, Venue::Gate) => {
                let (url, channel, ping_channel) = if self.kind == Kind::Spot {
                    ("wss://api.gateio.ws/ws/v4/", "spot.book_ticker", "spot.ping")
                } else {
                    ("wss://fx-ws.gateio.ws/v4/ws/usdt", "futures.book_ticker", "futures.ping")
                };
                Session {
                    url: url.to_string(),
                    requests: vec![json(sonic_rs::json!({"time": now_s, "channel": channel, "event": "subscribe", "payload": symbols}))],
                    acks: 1,
                    heartbeat: Some((Heartbeat::Text(json(sonic_rs::json!({"time": now_s, "channel": ping_channel}))), HEARTBEAT_INTERVAL)),
                }
            }
            (Kind::Spot, Venue::Kraken) => Session {
                url: "wss://ws.kraken.com/v2".to_string(),
                requests: vec![json(sonic_rs::json!({"method": "subscribe", "params": {"channel": "ticker", "symbol": symbols, "event_trigger": "bbo"}}))],
                // one `subscribe` answer per symbol (bm 10-21)
                acks: symbols.len(),
                heartbeat: ping(r#"{"method":"ping"}"#),
            },
            (Kind::Spot, Venue::Kucoin) => {
                let (url, interval) = kucoin.unwrap_or_else(|| unreachable!("a Kucoin session starts from its token handshake"));
                let requests: Vec<String> = symbols
                    .chunks(KUCOIN_SYMBOLS_PER_REQUEST)
                    .enumerate()
                    .map(|(index, chunk)| json(sonic_rs::json!({"id": index + 1, "type": "subscribe", "topic": format!("/market/ticker:{}", chunk.join(",")), "response": true})))
                    .collect();
                Session { url: url.to_string(), acks: requests.len(), requests, heartbeat: Some((Heartbeat::Text(r#"{"id":"ping","type":"ping"}"#.to_string()), interval)) }
            }
            (Kind::Perp, Venue::Mexc) => Session {
                url: "wss://contract.mexc.com/edge".to_string(),
                requests: symbols.iter().map(|symbol| json(sonic_rs::json!({"method": "sub.depth.full", "param": {"symbol": symbol, "limit": 5}}))).collect(),
                acks: symbols.len(),
                heartbeat: ping(r#"{"method":"ping"}"#),
            },
            (Kind::Spot, Venue::Mexc) | (Kind::Perp, Venue::Kraken | Venue::Kucoin) => unreachable!("not a WS feed: {self:?}"),
            (Kind::Equity | Kind::EquityOvernight, _) | (_, Venue::Alpaca) => {
                unreachable!("an Alpaca session carries the credentials: `hip3::protocol::alpaca_session`")
            }
        }
    }

    /// One text frame; `Err` is a contract error.
    pub(crate) fn parse(self, text: &str, markets: &FeedMarkets) -> Result<Parsed, String> {
        let parsed = match self.venue {
            Venue::Binance => binance(self.kind, text, markets),
            Venue::Okx => okx(text, markets),
            Venue::Bybit => bybit(text, markets),
            Venue::Gate => gate(self.kind, text, markets),
            Venue::Kraken => kraken(text, markets),
            Venue::Kucoin => kucoin(text, markets),
            Venue::Mexc => mexc_perp(text, markets),
            Venue::Alpaca => super::hip3::protocol::alpaca(text, markets),
        };
        parsed.map_err(|err| format!("{err}; frame: {}", excerpt(text)))
    }
}

/// The first 300 characters of a frame, for an error or a warning.
pub(crate) fn excerpt(text: &str) -> &str {
    text.char_indices().nth(300).map_or(text, |(end, _)| &text[..end])
}

/// A price field as written: a JSON string or number.
pub(crate) fn price(value: &LazyValue<'_>) -> Result<Option<VenuePrice>, String> {
    let raw = value.as_raw_str();
    let text = raw.strip_prefix('"').and_then(|raw| raw.strip_suffix('"')).unwrap_or(raw);
    if text.is_empty() {
        return Ok(None);
    }
    let price = VenuePrice::parse(text)?;
    Ok(price.is_positive().then_some(price))
}

/// A market's quote: both sides positive, the bid not above the ask; a missing or zero side is no quote.
fn two_sided(symbol: &str, bid: Option<VenuePrice>, ask: Option<VenuePrice>) -> Result<Option<Quote>, String> {
    match (bid, ask) {
        (Some(bid), Some(ask)) if bid > ask => Err(format!("{symbol}: crossed book, bid {bid} above ask {ask}")),
        (Some(bid), Some(ask)) => Ok(Some(Quote { bid, ask })),
        _ => Ok(None),
    }
}

pub(crate) fn index(markets: &FeedMarkets, symbol: &str) -> Result<usize, String> {
    markets.index(symbol).ok_or_else(|| format!("a market not subscribed to: {symbol}"))
}

fn observation(markets: &FeedMarkets, symbol: &str, bid: &LazyValue<'_>, ask: &LazyValue<'_>, event_ms: Option<u64>) -> Result<Observation, String> {
    Ok((index(markets, symbol)?, two_sided(symbol, price(bid)?, price(ask)?)?, event_ms))
}

/// The first level's price of a `[[px, ...], ...]` side.
fn best<'a>(levels: &'a [Vec<LazyValue<'a>>]) -> Option<&'a LazyValue<'a>> {
    levels.first().and_then(|level| level.first())
}

fn observation_of_levels(markets: &FeedMarkets, symbol: &str, bids: &[Vec<LazyValue<'_>>], asks: &[Vec<LazyValue<'_>>], event_ms: Option<u64>) -> Result<Observation, String> {
    let side = |levels| best(levels).map(price).transpose().map(Option::flatten);
    Ok((index(markets, symbol)?, two_sided(symbol, side(bids)?, side(asks)?)?, event_ms))
}

fn parse_ms(text: &str) -> Result<u64, String> {
    text.parse().map_err(|err| format!("time {text:?}: {err}"))
}

/// A known frame's payload as written: present and not `null` (a frame is classified first, its payload parsed after).
fn payload<'a>(data: Option<&'a LazyValue<'a>>, what: &str) -> Result<&'a str, String> {
    data.map(LazyValue::as_raw_str).filter(|raw| *raw != "null").ok_or_else(|| format!("{what} without its payload"))
}

pub(crate) fn strict<'a, T: Deserialize<'a>>(raw: &'a str, what: &str) -> Result<T, String> {
    sonic_rs::from_str(raw).map_err(|err| format!("{what}: {err}"))
}

fn raw_text<'a>(value: Option<&'a LazyValue<'a>>) -> &'a str {
    value.map_or("", LazyValue::as_raw_str)
}

#[derive(Deserialize)]
struct BinanceFrame<'a> {
    stream: Option<&'a str>,
    #[serde(borrow)]
    data: Option<LazyValue<'a>>,
}

#[derive(Deserialize)]
struct BinanceTicker<'a> {
    e: Option<&'a str>,
    s: &'a str,
    #[serde(borrow)]
    b: LazyValue<'a>,
    #[serde(borrow)]
    a: LazyValue<'a>,
    #[serde(rename = "T")]
    transaction_ms: Option<u64>,
}

/// Combined `<symbol>@bookTicker` streams: spot rows have no event type or time, perp rows `e: bookTicker` and `T`.
fn binance(kind: Kind, text: &str, markets: &FeedMarkets) -> Result<Parsed, String> {
    let frame: BinanceFrame<'_> = strict(text, "Binance frame")?;
    if !frame.stream.is_some_and(|stream| stream.ends_with("@bookTicker")) {
        return Ok(Parsed::Unknown);
    }
    let row: BinanceTicker<'_> = strict(payload(frame.data.as_ref(), "Binance bookTicker")?, "Binance bookTicker")?;
    if kind == Kind::Perp && row.e != Some("bookTicker") {
        return Err(format!("Binance perp event {:?}", row.e));
    }
    Ok(Parsed::Quotes(vec![observation(markets, row.s, &row.b, &row.a, row.transaction_ms)?]))
}

#[derive(Deserialize)]
struct OkxFrame<'a> {
    event: Option<&'a str>,
    #[serde(borrow)]
    msg: Option<LazyValue<'a>>,
    #[serde(borrow)]
    arg: Option<LazyValue<'a>>,
    #[serde(borrow)]
    data: Option<LazyValue<'a>>,
}

#[derive(Deserialize)]
struct OkxArg<'a> {
    channel: Option<&'a str>,
    #[serde(rename = "instId")]
    inst_id: Option<&'a str>,
}

#[derive(Deserialize)]
struct OkxBook<'a> {
    #[serde(borrow)]
    bids: Vec<Vec<LazyValue<'a>>>,
    #[serde(borrow)]
    asks: Vec<Vec<LazyValue<'a>>>,
    ts: &'a str,
}

/// `bbo-tbt` pushes; a `subscribe` event per instrument, `error` events, the text `pong`.
fn okx(text: &str, markets: &FeedMarkets) -> Result<Parsed, String> {
    if text == "pong" {
        return Ok(Parsed::Pong);
    }
    let frame: OkxFrame<'_> = strict(text, "OKX frame")?;
    match frame.event {
        Some("subscribe") => return Ok(Parsed::Acked(1)),
        Some("error") => return Err(format!("OKX error: {}", raw_text(frame.msg.as_ref()))),
        Some(_) => return Ok(Parsed::Unknown),
        None => {}
    }
    let Some(arg) = frame.arg.as_ref() else { return Ok(Parsed::Unknown) };
    let arg: OkxArg<'_> = strict(arg.as_raw_str(), "OKX arg")?;
    if arg.channel != Some("bbo-tbt") {
        return Ok(Parsed::Unknown);
    }
    let inst_id = arg.inst_id.ok_or("OKX bbo-tbt without its instId")?;
    let rows: Vec<OkxBook<'_>> = strict(payload(frame.data.as_ref(), "OKX bbo-tbt")?, "OKX bbo-tbt")?;
    rows.iter()
        .map(|row| observation_of_levels(markets, inst_id, &row.bids, &row.asks, Some(parse_ms(row.ts)?)))
        .collect::<Result<Vec<_>, _>>()
        .map(Parsed::Quotes)
}

#[derive(Deserialize)]
struct BybitFrame<'a> {
    op: Option<&'a str>,
    success: Option<bool>,
    #[serde(borrow)]
    ret_msg: Option<LazyValue<'a>>,
    topic: Option<&'a str>,
    #[serde(rename = "type")]
    kind: Option<&'a str>,
    ts: Option<u64>,
    #[serde(borrow)]
    data: Option<LazyValue<'a>>,
}

#[derive(Deserialize)]
struct BybitBook<'a> {
    s: &'a str,
    #[serde(borrow)]
    b: Vec<Vec<LazyValue<'a>>>,
    #[serde(borrow)]
    a: Vec<Vec<LazyValue<'a>>>,
}

/// `orderbook.1` (L1 comes as snapshots, a quiet one every 3 s); the subscribe answer per request, the ping answer.
fn bybit(text: &str, markets: &FeedMarkets) -> Result<Parsed, String> {
    let frame: BybitFrame<'_> = strict(text, "Bybit frame")?;
    match (frame.op, frame.success) {
        (Some(_), Some(false)) => return Err(format!("Bybit refused: {}", raw_text(frame.ret_msg.as_ref()))),
        (Some("subscribe"), Some(true)) => return Ok(Parsed::Acked(1)),
        (Some("pong"), _) | (Some("ping"), Some(true)) => return Ok(Parsed::Pong),
        _ => {}
    }
    let Some(symbol) = frame.topic.and_then(|topic| topic.strip_prefix("orderbook.1.")) else { return Ok(Parsed::Unknown) };
    let book: BybitBook<'_> = strict(payload(frame.data.as_ref(), "Bybit orderbook.1")?, "Bybit orderbook.1")?;
    if frame.kind != Some("snapshot") || book.s != symbol {
        return Err(format!("Bybit orderbook.1.{symbol} {:?}: not a snapshot of its own symbol {}", frame.kind, book.s));
    }
    Ok(Parsed::Quotes(vec![observation_of_levels(markets, book.s, &book.b, &book.a, frame.ts)?]))
}

#[derive(Deserialize)]
struct GateFrame<'a> {
    channel: &'a str,
    event: Option<&'a str>,
    #[serde(borrow)]
    error: Option<LazyValue<'a>>,
    #[serde(borrow)]
    result: Option<LazyValue<'a>>,
}

#[derive(Deserialize)]
struct GateStatus<'a> {
    status: &'a str,
}

#[derive(Deserialize)]
struct GateSystemNotice<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    // Parse the message only for a known type; a future notification can have another payload shape.
    #[serde(borrow)]
    msg: Option<LazyValue<'a>>,
}

#[derive(Deserialize)]
struct GateTicker<'a> {
    t: u64,
    s: &'a str,
    #[serde(borrow)]
    b: LazyValue<'a>,
    #[serde(borrow)]
    a: LazyValue<'a>,
}

/// `spot.book_ticker` / `futures.book_ticker`, their `subscribe` answer and `spot.pong` / `futures.pong`.
fn gate(kind: Kind, text: &str, markets: &FeedMarkets) -> Result<Parsed, String> {
    let frame: GateFrame<'_> = strict(text, "Gate frame")?;
    if let Some(error) = frame.error.as_ref().filter(|error| error.as_raw_str() != "null") {
        return Err(format!("Gate error: {}", error.as_raw_str()));
    }
    let (ticker, pong, system) = if kind == Kind::Spot {
        ("spot.book_ticker", "spot.pong", "spot.system")
    } else {
        ("futures.book_ticker", "futures.pong", "futures.system")
    };
    match (frame.channel, frame.event) {
        (channel, _) if channel == pong => Ok(Parsed::Pong),
        (channel, Some("update")) if channel == system => {
            let notice: GateSystemNotice<'_> = strict(payload(frame.result.as_ref(), "Gate system notice")?, "Gate system notice")?;
            if notice.kind != "upgrade" { return Ok(Parsed::Unknown); }
            let message: String = strict(payload(notice.msg.as_ref(), "Gate upgrade message")?, "Gate upgrade message")?;
            // Gate's spot/futures service-upgrade contract asks clients to reconnect promptly.
            // BM 2026-10-22 07:03:05Z: futures.system/upgrade, followed by a close ten seconds later.
            Ok(Parsed::ReconnectRequested(format!("Gate service upgrade: {}", excerpt(&message))))
        }
        (channel, Some("subscribe")) if channel == ticker => {
            let status: GateStatus<'_> = strict(payload(frame.result.as_ref(), "Gate subscribe answer")?, "Gate subscribe answer")?;
            if status.status != "success" {
                return Err(format!("Gate subscription {}", status.status));
            }
            Ok(Parsed::Acked(1))
        }
        (channel, Some("update")) if channel == ticker => {
            let row: GateTicker<'_> = strict(payload(frame.result.as_ref(), "Gate ticker")?, "Gate ticker")?;
            Ok(Parsed::Quotes(vec![observation(markets, row.s, &row.b, &row.a, Some(row.t))?]))
        }
        _ => Ok(Parsed::Unknown),
    }
}

#[derive(Deserialize)]
struct KrakenFrame<'a> {
    method: Option<&'a str>,
    success: Option<bool>,
    #[serde(borrow)]
    error: Option<LazyValue<'a>>,
    channel: Option<&'a str>,
    /// Status rows or ticker rows, by channel.
    #[serde(borrow)]
    data: Option<LazyValue<'a>>,
}

#[derive(Deserialize)]
struct KrakenTicker<'a> {
    symbol: &'a str,
    #[serde(borrow)]
    bid: LazyValue<'a>,
    #[serde(borrow)]
    ask: LazyValue<'a>,
}

/// v2 `ticker` with `event_trigger: bbo`; a `subscribe` answer per symbol, `pong`, `status` and `heartbeat` channels.
fn kraken(text: &str, markets: &FeedMarkets) -> Result<Parsed, String> {
    let frame: KrakenFrame<'_> = strict(text, "Kraken frame")?;
    match (frame.method, frame.channel) {
        (Some("subscribe"), _) if frame.success == Some(true) => Ok(Parsed::Acked(1)),
        (Some("subscribe"), _) => Err(format!("Kraken refused: {}", raw_text(frame.error.as_ref()))),
        (Some("pong"), _) => Ok(Parsed::Pong),
        (None, Some("status" | "heartbeat")) => Ok(Parsed::Ignored),
        (None, Some("ticker")) => {
            let rows: Vec<KrakenTicker<'_>> = strict(payload(frame.data.as_ref(), "Kraken ticker")?, "Kraken ticker")?;
            rows.iter().map(|row| observation(markets, row.symbol, &row.bid, &row.ask, None)).collect::<Result<Vec<_>, _>>().map(Parsed::Quotes)
        }
        _ => Ok(Parsed::Unknown),
    }
}

#[derive(Deserialize)]
struct KucoinFrame<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    topic: Option<&'a str>,
    subject: Option<&'a str>,
    #[serde(borrow)]
    data: Option<LazyValue<'a>>,
}

#[derive(Deserialize)]
struct KucoinTicker<'a> {
    #[serde(rename = "bestBid", borrow)]
    best_bid: LazyValue<'a>,
    #[serde(rename = "bestAsk", borrow)]
    best_ask: LazyValue<'a>,
    time: Option<u64>,
}

/// `/market/ticker` (best bid and ask on change); `welcome`, `ack` per request, `pong`, `error`.
fn kucoin(text: &str, markets: &FeedMarkets) -> Result<Parsed, String> {
    let frame: KucoinFrame<'_> = strict(text, "Kucoin frame")?;
    match frame.kind {
        "welcome" => Ok(Parsed::Ignored),
        "ack" => Ok(Parsed::Acked(1)),
        "pong" => Ok(Parsed::Pong),
        "error" => Err(format!("Kucoin error: {}", raw_text(frame.data.as_ref()))),
        "message" if frame.subject == Some("trade.ticker") => {
            let symbol = frame.topic.and_then(|topic| topic.strip_prefix("/market/ticker:")).ok_or("Kucoin ticker without its topic")?;
            let row: KucoinTicker<'_> = strict(payload(frame.data.as_ref(), "Kucoin ticker")?, "Kucoin ticker")?;
            Ok(Parsed::Quotes(vec![observation(markets, symbol, &row.best_bid, &row.best_ask, row.time)?]))
        }
        _ => Ok(Parsed::Unknown),
    }
}

#[derive(Deserialize)]
struct MexcFrame<'a> {
    channel: &'a str,
    symbol: Option<&'a str>,
    #[serde(borrow)]
    data: Option<LazyValue<'a>>,
}

#[derive(Deserialize)]
struct MexcDepth<'a> {
    #[serde(borrow)]
    bids: Vec<Vec<LazyValue<'a>>>,
    #[serde(borrow)]
    asks: Vec<Vec<LazyValue<'a>>>,
    version: u64,
    cts: Option<u64>,
}

/// `sub.depth.full` (the service measured `sub.ticker` lagging ~1.6 s); `rs.sub.depth.full` per symbol, `pong`.
fn mexc_perp(text: &str, markets: &FeedMarkets) -> Result<Parsed, String> {
    let frame: MexcFrame<'_> = strict(text, "MEXC frame")?;
    match frame.channel {
        "pong" => Ok(Parsed::Pong),
        "rs.sub.depth.full" if raw_text(frame.data.as_ref()) == r#""success""# => Ok(Parsed::Acked(1)),
        "rs.sub.depth.full" | "rs.error" => Err(format!("MEXC refused: {}", raw_text(frame.data.as_ref()))),
        "push.depth.full" => {
            let symbol = frame.symbol.ok_or("MEXC depth without its symbol")?;
            let depth: MexcDepth<'_> = strict(payload(frame.data.as_ref(), "MEXC depth")?, "MEXC depth")?;
            if depth.version == 0 {
                return Err(format!("MEXC {symbol} depth version 0"));
            }
            Ok(Parsed::Quotes(vec![observation_of_levels(markets, symbol, &depth.bids, &depth.asks, depth.cts)?]))
        }
        _ => Ok(Parsed::Unknown),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MexcBookTicker<'a> {
    symbol: &'a str,
    #[serde(borrow)]
    bid_price: LazyValue<'a>,
    #[serde(borrow)]
    ask_price: LazyValue<'a>,
}

/// MEXC's spot `ticker/bookTicker` (every market): the subscribed markets' quotes, a market the answer leaves out
/// having none.
pub(crate) fn mexc_spot_book_tickers(body: &str, markets: &FeedMarkets) -> Result<Vec<Observation>, String> {
    let rows: Vec<MexcBookTicker<'_>> = sonic_rs::from_str(body).map_err(|err| format!("MEXC spot bookTicker: {err}; body: {}", excerpt(body)))?;
    let mut observations: Vec<Observation> = (0..markets.symbols.len()).map(|index| (index, None, None)).collect();
    for row in rows {
        let Some(index) = markets.index(row.symbol) else { continue };
        observations[index] = (index, two_sided(row.symbol, price(&row.bid_price)?, price(&row.ask_price)?)?, None);
    }
    Ok(observations)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct KucoinBullet<'a> {
    code: &'a str,
    #[serde(borrow)]
    data: Option<KucoinBulletData<'a>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct KucoinBulletData<'a> {
    token: &'a str,
    #[serde(borrow)]
    instance_servers: Vec<KucoinServer<'a>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct KucoinServer<'a> {
    endpoint: &'a str,
    ping_interval: u64,
}

/// The public token handshake's answer: the WS URL (endpoint, token, a connection id) and the ping interval.
pub(crate) fn kucoin_session(body: &str, connect_id: u64) -> Result<(String, Duration), String> {
    let answer: KucoinBullet<'_> = sonic_rs::from_str(body).map_err(|err| format!("Kucoin bullet-public: {err}"))?;
    if answer.code != "200000" {
        return Err(format!("Kucoin bullet-public refused, code {}", answer.code));
    }
    let data = answer.data.ok_or("Kucoin bullet-public without data")?;
    let server = data.instance_servers.first().ok_or("Kucoin bullet-public without a server")?;
    Ok((format!("{}?token={}&connectId={connect_id}", server.endpoint, data.token), Duration::from_millis(server.ping_interval)))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::local_marks::sources::{Feed, MarkSources};

    /// Real frames of every feed, bm 10-21 04:23–04:25 UTC (`scripts`-free capture, the Kucoin token redacted).
    fn fixtures() -> BTreeMap<String, Vec<String>> {
        serde_json::from_str(include_str!("wire_fixtures.json")).unwrap()
    }

    fn sources() -> MarkSources {
        MarkSources::embedded().unwrap()
    }

    fn markets(sources: &MarkSources, kind: Kind, venue: Venue) -> &FeedMarkets {
        sources.markets(Feed { kind, venue }).unwrap()
    }

    fn parse_all(kind: Kind, venue: Venue, name: &str) -> Vec<Parsed> {
        let sources = sources();
        let markets = markets(&sources, kind, venue);
        fixtures()[name].iter().map(|frame| Protocol { kind, venue }.parse(frame, markets).unwrap()).collect()
    }

    fn quote_of(parsed: &Parsed, markets: &FeedMarkets) -> Vec<(String, String, String, Option<u64>)> {
        let Parsed::Quotes(observations) = parsed else { return Vec::new() };
        observations
            .iter()
            .map(|(index, quote, event_ms)| {
                let quote = quote.unwrap();
                (markets.symbols[*index].clone(), quote.bid.to_string(), quote.ask.to_string(), *event_ms)
            })
            .collect()
    }

    fn kinds(parsed: &[Parsed]) -> Vec<&'static str> {
        parsed
            .iter()
            .map(|parsed| match parsed {
                Parsed::Quotes(_) => "quote",
                Parsed::Acked(_) => "ack",
                Parsed::Pong => "pong",
                Parsed::ReconnectRequested(_) => "reconnect",
                Parsed::ConnectionLimit(_) => "connectionLimit",
                Parsed::SessionFailed(_) => "sessionFailed",
                Parsed::Ignored => "ignored",
                Parsed::Unknown => "unknown",
            })
            .collect()
    }

    #[test]
    fn every_feed_parses_its_real_frames() {
        let sources = sources();
        let cases = [
            (Kind::Spot, Venue::Binance, "spot_binance", vec!["quote", "quote"]),
            (Kind::Perp, Venue::Binance, "perp_binance", vec!["quote", "quote"]),
            (Kind::Spot, Venue::Okx, "spot_okx", vec!["ack", "quote", "pong"]),
            (Kind::Perp, Venue::Okx, "perp_okx", vec!["ack", "quote"]),
            (Kind::Spot, Venue::Bybit, "spot_bybit", vec!["ack", "quote", "pong"]),
            (Kind::Perp, Venue::Bybit, "perp_bybit", vec!["quote", "ack", "pong"]),
            (Kind::Spot, Venue::Gate, "spot_gate", vec!["ack", "quote", "pong"]),
            (Kind::Perp, Venue::Gate, "perp_gate", vec!["ack", "quote", "pong"]),
            (Kind::Spot, Venue::Kraken, "spot_kraken", vec!["ignored", "ack", "quote", "pong", "ignored"]),
            (Kind::Spot, Venue::Kucoin, "spot_kucoin", vec!["ignored", "ack", "quote", "pong"]),
            (Kind::Perp, Venue::Mexc, "perp_mexc", vec!["ack", "quote", "pong"]),
        ];
        for (kind, venue, name, expected) in cases {
            assert_eq!(kinds(&parse_all(kind, venue, name)), expected, "{name}");
        }
        // the quotes as written, at the venue's own times where it gives one
        let first_quote = |kind, venue, name: &str| {
            let parsed = parse_all(kind, venue, name);
            let quote = parsed.iter().find(|parsed| matches!(parsed, Parsed::Quotes(_))).unwrap();
            quote_of(quote, markets(&sources, kind, venue)).remove(0)
        };
        assert_eq!(first_quote(Kind::Spot, Venue::Binance, "spot_binance"), ("BTCUSDT".into(), "82806".into(), "82806.01".into(), None));
        assert_eq!(first_quote(Kind::Perp, Venue::Binance, "perp_binance"), ("BTCUSDT".into(), "82772".into(), "82772.1".into(), Some(1_792_556_633_986)));
        assert_eq!(first_quote(Kind::Perp, Venue::Okx, "perp_okx"), ("PEPE-USDT-SWAP".into(), "0.000004508".into(), "0.000004509".into(), Some(1_792_556_634_003)));
        assert_eq!(first_quote(Kind::Spot, Venue::Gate, "spot_gate"), ("BTC_USDT".into(), "82809.9".into(), "82810".into(), Some(1_792_556_633_937)));
        assert_eq!(first_quote(Kind::Spot, Venue::Kraken, "spot_kraken"), ("SHIB/USDT".into(), "0.000005118".into(), "0.000005132".into(), None));
        assert_eq!(first_quote(Kind::Perp, Venue::Mexc, "perp_mexc"), ("BTC_USDT".into(), "82771.8".into(), "82771.9".into(), Some(1_792_556_634_121)));
        assert_eq!(first_quote(Kind::Perp, Venue::Bybit, "perp_bybit").0, "BTCUSDT");
    }

    #[test]
    fn gate_upgrade_is_control_and_unknown_notices_remain_visible() {
        let sources = sources();
        let recorded = &fixtures()["perp_gate_upgrade"][0];
        assert_eq!(kinds(&parse_all(Kind::Perp, Venue::Gate, "perp_gate_upgrade")), ["reconnect"]);
        for kind in [Kind::Spot, Kind::Perp] {
            let protocol = Protocol { kind, venue: Venue::Gate };
            let markets = markets(&sources, kind, Venue::Gate);
            let wire = if kind == Kind::Spot { recorded.replace("futures.system", "spot.system") } else { recorded.clone() };
            assert!(matches!(protocol.parse(&wire, markets).unwrap(), Parsed::ReconnectRequested(reason) if reason.contains("service upgrade")));
            let mut notice: serde_json::Value = serde_json::from_str(&wire).unwrap();
            notice["result"]["msg"] = serde_json::json!("Upgrade \"scheduled\"\nplease reconnect");
            assert!(matches!(protocol.parse(&notice.to_string(), markets).unwrap(), Parsed::ReconnectRequested(_)));
            notice["result"]["type"] = serde_json::json!("futureNotice");
            notice["result"]["msg"] = serde_json::json!({"newShape": true});
            assert_eq!(protocol.parse(&notice.to_string(), markets), Ok(Parsed::Unknown));
            notice["result"]["type"] = serde_json::json!("upgrade");
            for message in [serde_json::Value::Null, serde_json::json!(23), serde_json::json!({})] {
                notice["result"]["msg"] = message;
                assert!(protocol.parse(&notice.to_string(), markets).is_err());
            }
            notice["result"].as_object_mut().unwrap().remove("msg");
            assert!(protocol.parse(&notice.to_string(), markets).is_err());
            notice["result"]["msg"] = serde_json::json!("restart");
            notice["error"] = serde_json::json!({"code": 2, "message": "refused"});
            assert!(protocol.parse(&notice.to_string(), markets).is_err(), "a notice never hides an error");
        }
    }

    #[test]
    fn refusals_crossed_books_and_unknown_markets_are_contract_errors() {
        let sources = sources();
        let okx = markets(&sources, Kind::Spot, Venue::Okx);
        let protocol = |venue| Protocol { kind: Kind::Spot, venue };
        let error = protocol(Venue::Okx).parse(r#"{"event":"error","code":"60012","msg":"Invalid request"}"#, okx).unwrap_err();
        assert!(error.contains("Invalid request") && error.contains("frame:"), "{error}");
        let crossed = r#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"data":[{"asks":[["100","1","0","1"]],"bids":[["101","1","0","1"]],"ts":"1"}]}"#;
        assert!(protocol(Venue::Okx).parse(crossed, okx).unwrap_err().contains("crossed book"));
        let unknown = r#"{"arg":{"channel":"bbo-tbt","instId":"NOPE-USDT"},"data":[{"asks":[["2","1","0","1"]],"bids":[["1","1","0","1"]],"ts":"1"}]}"#;
        assert!(protocol(Venue::Okx).parse(unknown, okx).unwrap_err().contains("not subscribed to: NOPE-USDT"));
        let bybit = markets(&sources, Kind::Spot, Venue::Bybit);
        assert!(protocol(Venue::Bybit).parse(r#"{"success":false,"ret_msg":"error:handler not found","op":"subscribe"}"#, bybit).is_err());
        let gate = markets(&sources, Kind::Spot, Venue::Gate);
        assert!(protocol(Venue::Gate).parse(r#"{"channel":"spot.book_ticker","event":"subscribe","error":{"code":2,"message":"unknown currency pair"},"result":null}"#, gate).is_err());
        // a frame of no type we know is passed up, not taken, whatever its payload (review 000455 r1 P2-2)
        assert_eq!(protocol(Venue::Okx).parse(r#"{"event":"notice","code":"64008","msg":"upgrade"}"#, okx).unwrap(), Parsed::Unknown);
        let kucoin = markets(&sources, Kind::Spot, Venue::Kucoin);
        let unknown_with_payload = [
            (Venue::Okx, okx, r#"{"event":"futureNotice","data":{"notice":"maintenance"}}"#),
            (Venue::Okx, okx, r#"{"arg":{"channel":"tickers","instId":"BTC-USDT"},"data":{"notice":"x"}}"#),
            (Venue::Bybit, bybit, r#"{"topic":"tickers.BTCUSDT","type":"snapshot","data":{"notice":"maintenance"}}"#),
            (Venue::Kucoin, kucoin, r#"{"type":"message","topic":"/market/level2:BTC-USDT","subject":"trade.l2update","data":{"notice":"x"}}"#),
            (Venue::Kucoin, kucoin, r#"{"type":"notice","data":{"notice":"maintenance"}}"#),
        ];
        for (venue, markets, frame) in unknown_with_payload {
            assert_eq!(protocol(venue).parse(frame, markets), Ok(Parsed::Unknown), "{frame}");
        }
        // a known quote frame without its payload stops the feed
        let missing = [
            (Venue::Okx, okx, r#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"data":null}"#),
            (Venue::Okx, okx, r#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT"}}"#),
            (Venue::Bybit, bybit, r#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1}"#),
            (Venue::Kucoin, kucoin, r#"{"type":"message","topic":"/market/ticker:BTC-USDT","subject":"trade.ticker"}"#),
            (Venue::Gate, gate, r#"{"channel":"spot.book_ticker","event":"update","result":null}"#),
        ];
        for (venue, markets, frame) in missing {
            let error = protocol(venue).parse(frame, markets).unwrap_err();
            assert!(error.contains("without its payload"), "{frame}: {error}");
        }
        let binance = markets(&sources, Kind::Spot, Venue::Binance);
        assert!(protocol(Venue::Binance).parse(r#"{"stream":"btcusdt@bookTicker"}"#, binance).unwrap_err().contains("without its payload"));
        assert_eq!(protocol(Venue::Binance).parse(r#"{"result":null,"id":1}"#, binance), Ok(Parsed::Unknown));
        // an empty side is no quote, not an error
        let empty = r#"{"topic":"orderbook.1.BTCUSDT","ts":1,"type":"snapshot","data":{"s":"BTCUSDT","b":[],"a":[["2","1"]],"u":1,"seq":1}}"#;
        let Parsed::Quotes(observations) = protocol(Venue::Bybit).parse(empty, bybit).unwrap() else { panic!() };
        assert_eq!(observations[0].1, None);
    }

    #[test]
    fn subscriptions_follow_each_venues_request_limits() {
        let symbols: Vec<String> = (0..25).map(|index| format!("C{index}USDT")).collect();
        let bybit = Protocol { kind: Kind::Perp, venue: Venue::Bybit }.session(&symbols, 0, None);
        assert_eq!((bybit.requests.len(), bybit.acks), (3, 3));
        let okx = Protocol { kind: Kind::Spot, venue: Venue::Okx }.session(&symbols, 0, None);
        assert_eq!((okx.requests.len(), okx.acks), (1, 25));
        let binance = Protocol { kind: Kind::Spot, venue: Venue::Binance }.session(&symbols, 0, None);
        assert!(binance.url.ends_with("c24usdt@bookTicker") && binance.heartbeat.is_none() && binance.acks == 0);
        let kucoin_symbols: Vec<String> = (0..173).map(|index| format!("C{index}-USDT")).collect();
        let kucoin = Protocol { kind: Kind::Spot, venue: Venue::Kucoin }.session(&kucoin_symbols, 0, Some(("wss://k/?token=t&connectId=1", Duration::from_secs(18))));
        assert_eq!((kucoin.requests.len(), kucoin.acks, kucoin.heartbeat.unwrap().1), (2, 2, Duration::from_secs(18)));
        // Gate spot and futures ping on their own channels (review 000454 r2 P3)
        let ping = |kind| Protocol { kind, venue: Venue::Gate }.session(&symbols, 7, None).heartbeat.unwrap().0;
        let text = |ping: &str| Heartbeat::Text(ping.to_string());
        assert_eq!((ping(Kind::Spot), ping(Kind::Perp)), (text(r#"{"time":7,"channel":"spot.ping"}"#), text(r#"{"time":7,"channel":"futures.ping"}"#)));
    }

    #[test]
    fn polled_mexc_spot_and_the_kucoin_token_answer() {
        let sources = sources();
        let mexc = markets(&sources, Kind::Spot, Venue::Mexc);
        let body = &fixtures()["spot_mexc_rest"][0];
        let observations = mexc_spot_book_tickers(body, mexc).unwrap();
        assert_eq!(observations.len(), mexc.symbols.len());
        let btc = mexc.index("BTCUSDT").unwrap();
        assert_eq!(observations[btc].1.map(|quote| (quote.bid.to_string(), quote.ask.to_string())), Some(("82799.65".to_string(), "82799.66".to_string())));
        let eth = mexc.index("ETHUSDT").unwrap();
        assert_eq!(observations[eth].1, None, "left out of this answer");
        let bullet = &fixtures()["kucoin_bullet_public"][0];
        assert_eq!(kucoin_session(bullet, 5).unwrap(), ("wss://ws-api-spot.kucoin.com/?token=<token>&connectId=5".to_string(), Duration::from_secs(18)));
        assert!(kucoin_session(r#"{"code":"400100","msg":"x"}"#, 5).unwrap_err().contains("400100"));
    }
}
