//! The record's own Binance streams (hyperliquid-trade design 000751 r2 rule 2): every recorded contract's
//! `<symbol>@markPrice` - its mark, index and funding, every 3 s (bm 2026-11-05: only on `/market/stream`; the
//! `/public` and legacy `/stream` routes push none) -, HYPEUSDT's perp `bookTicker` (HYPE is not in the local marks'
//! sources: HL prices it in USDC, its oracle HL's own spot) and USDCUSDT's spot `bookTicker`.
//!
//! Each stream keeps every symbol's latest value and when it arrived; a reader copies them. The streams are the
//! record's alone: not in the local marks' feeds nor under their supervision, so whatever ends them ends nothing else.
//! A lost connection is retried after the feeds' backoff, its values kept (each with its receipt time, judged
//! offline); a contract error - a field missing, an event of another type, a symbol not subscribed - stops that stream
//! until restart, its values then recorded unavailable.

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex, PoisonError},
};

use log::{error, warn};
use rustc_hash::FxHashMap;
use serde::Deserialize;
use sonic_rs::LazyValue;
use tokio::time::{Instant, sleep, timeout};
use yawc::{OpCode, Options, WebSocket};

use super::Latest;
use crate::{
    local_marks::{
        connection::{FeedError, Timeouts, close_error, connect_error, read_error},
        feeds::{BACKOFF_MAX, BACKOFF_MIN, HEALTHY_FOR},
        now_ms,
        price::VenuePrice,
        protocol::{excerpt, strict},
    },
    metrics::{BASIS_RECORD_STREAM_DISCONNECTS_TOTAL, BASIS_RECORD_STREAM_STOPPED},
};

const MARK_HOST: &str = "wss://fstream.binance.com/market/stream";
const PERP_BOOK_HOST: &str = "wss://fstream.binance.com/public/stream";
const SPOT_BOOK_HOST: &str = "wss://stream.binance.com:9443/stream";
/// Mark streams a connection: a frame every 3 s each, under a hundred frames a second, a URL of about 2 KB.
const MARK_STREAMS_PER_CONNECTION: usize = 100;
/// HYPE's perp contract and the USDC/USDT spot pair.
pub(crate) const HYPE_SYMBOL: &str = "HYPEUSDT";
pub(crate) const USDC_USDT_SYMBOL: &str = "USDCUSDT";
/// Larger than any frame these streams send; bounds a broken peer as the feeds' connections do.
const MAX_PAYLOAD: usize = 1 << 20;

/// A contract's latest `markPriceUpdate`: its prices as Binance wrote them (its own scale), the funding rate's text
/// (signed, so no `VenuePrice`), Binance's event time and our receipt time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MarkPrice {
    pub(crate) mark: VenuePrice,
    pub(crate) index: VenuePrice,
    pub(crate) funding_rate: String,
    pub(crate) next_funding_ms: u64,
    pub(crate) event_ms: u64,
    pub(crate) received_ms: u64,
}

/// A market's latest book ticker: best bid and ask, Binance's transaction time where it gives one, our receipt time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BookTicker {
    pub(crate) bid: VenuePrice,
    pub(crate) ask: VenuePrice,
    pub(crate) event_ms: Option<u64>,
    pub(crate) received_ms: u64,
}

/// Which of the record's streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Marks,
    PerpBook,
    SpotBook,
}

impl Kind {
    const fn label(self) -> &'static str {
        match self {
            Self::Marks => "binance_mark",
            Self::PerpBook => "binance_hype_book",
            Self::SpotBook => "binance_usdc_usdt_book",
        }
    }
}

#[derive(Default)]
struct Values {
    marks: FxHashMap<String, MarkPrice>,
    books: FxHashMap<String, BookTicker>,
    /// The stop reason of each stopped stream, by its subscribed symbols.
    stopped: FxHashMap<String, String>,
}

/// Every stream's latest values; the streams write, the sampler copies.
#[derive(Default)]
pub(crate) struct BinanceLatest {
    values: Mutex<Values>,
}

impl BinanceLatest {
    fn lock(&self) -> std::sync::MutexGuard<'_, Values> {
        self.values.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// `symbol`'s latest mark, index and funding.
    pub(crate) fn mark(&self, symbol: &str) -> Latest<MarkPrice> {
        let values = self.lock();
        match (values.marks.get(symbol), values.stopped.get(&mark_key(symbol))) {
            (_, Some(reason)) => Latest::Stopped(reason.clone()),
            (Some(mark), None) => Latest::Received(mark.clone()),
            (None, None) => Latest::NotReceived,
        }
    }

    /// `symbol`'s latest book ticker.
    pub(crate) fn book(&self, symbol: &str) -> Latest<BookTicker> {
        let values = self.lock();
        match (values.books.get(symbol), values.stopped.get(&book_key(symbol))) {
            (_, Some(reason)) => Latest::Stopped(reason.clone()),
            (Some(book), None) => Latest::Received(*book),
            (None, None) => Latest::NotReceived,
        }
    }

    /// `symbol`'s mark as received, for a test.
    #[cfg(test)]
    pub(crate) fn set_mark_for_test(&self, symbol: &str, mark: MarkPrice) {
        self.lock().marks.insert(symbol.to_string(), mark);
    }

    fn stop(&self, stream: &Stream, reason: &str) {
        let mut values = self.lock();
        for symbol in &stream.symbols {
            let key = match stream.kind {
                Kind::Marks => mark_key(symbol),
                Kind::PerpBook | Kind::SpotBook => book_key(symbol),
            };
            values.stopped.insert(key, reason.to_string());
        }
    }
}

fn mark_key(symbol: &str) -> String {
    format!("mark:{symbol}")
}

fn book_key(symbol: &str) -> String {
    format!("book:{symbol}")
}

/// One connection's subscription: its kind and symbols (Binance's upper case).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Stream {
    kind: Kind,
    shard: usize,
    symbols: BTreeSet<String>,
}

impl Stream {
    fn url(&self) -> String {
        let (host, suffix) = match self.kind {
            Kind::Marks => (MARK_HOST, "@markPrice"),
            Kind::PerpBook => (PERP_BOOK_HOST, "@bookTicker"),
            Kind::SpotBook => (SPOT_BOOK_HOST, "@bookTicker"),
        };
        let streams: Vec<String> = self.symbols.iter().map(|symbol| format!("{}{suffix}", symbol.to_lowercase())).collect();
        format!("{host}?streams={}", streams.join("/"))
    }
}

/// The streams of `mark_symbols` (each contract recorded), HYPEUSDT's perp book and USDCUSDT's spot book.
fn streams(mark_symbols: &BTreeSet<String>) -> Vec<Stream> {
    let symbols: Vec<&String> = mark_symbols.iter().collect();
    let marks = symbols.chunks(MARK_STREAMS_PER_CONNECTION).enumerate().map(|(shard, chunk)| Stream {
        kind: Kind::Marks,
        shard,
        symbols: chunk.iter().map(|symbol| (*symbol).clone()).collect(),
    });
    let one = |kind, symbol: &str| Stream { kind, shard: 0, symbols: BTreeSet::from([symbol.to_string()]) };
    marks.chain([one(Kind::PerpBook, HYPE_SYMBOL), one(Kind::SpotBook, USDC_USDT_SYMBOL)]).collect()
}

/// Starts every stream; they run until the process ends or a contract error stops one.
pub(crate) fn start(mark_symbols: &BTreeSet<String>) -> Arc<BinanceLatest> {
    let latest = Arc::new(BinanceLatest::default());
    for stream in streams(mark_symbols) {
        let latest = Arc::clone(&latest);
        tokio::spawn(async move { run_stream(stream, &latest).await });
    }
    latest
}

async fn run_stream(stream: Stream, latest: &BinanceLatest) {
    let label = stream.kind.label();
    let mut backoff = BACKOFF_MIN;
    loop {
        let started = Instant::now();
        let reason = match read_stream(&stream, latest).await {
            FeedError::Contract(reason) => {
                error!("basis record {label} shard {}: stopped until restart, its values recorded unavailable: {reason}", stream.shard);
                BASIS_RECORD_STREAM_STOPPED.with_label_values(&[label]).set(1);
                latest.stop(&stream, &reason);
                return;
            }
            FeedError::Transient(reason) | FeedError::ReconnectRequested(reason) | FeedError::ConnectionLimit(reason) => reason,
        };
        BASIS_RECORD_STREAM_DISCONNECTS_TOTAL.with_label_values(&[label]).inc();
        if started.elapsed() >= HEALTHY_FOR {
            backoff = BACKOFF_MIN;
        }
        warn!("basis record {label} shard {}: reconnecting in {backoff:?}: {reason}", stream.shard);
        sleep(backoff).await;
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

/// One connection of `stream` until it fails; Binance pings, `yawc` answers, and a stream rotates after 24 h.
async fn read_stream(stream: &Stream, latest: &BinanceLatest) -> FeedError {
    let label = stream.kind.label();
    let timeouts = Timeouts::FEEDS;
    let url = match url::Url::parse(&stream.url()) {
        Ok(url) => url,
        Err(err) => return FeedError::Contract(format!("{label}: URL: {err}")),
    };
    let options = Options::default().with_max_payload_read(MAX_PAYLOAD).with_low_latency_compression();
    let mut ws = match timeout(timeouts.connect, WebSocket::connect(url).with_options(options)).await {
        Ok(Ok(ws)) => ws,
        Ok(Err(err)) => return connect_error(label, &err),
        Err(_) => return FeedError::Transient(format!("{label}: no connection within {:?}", timeouts.connect)),
    };
    loop {
        let frame = match timeout(timeouts.read, ws.next_frame()).await {
            Ok(Ok(frame)) => frame,
            Ok(Err(err)) => return read_error(label, &err),
            Err(_) => return FeedError::Transient(format!("{label}: no frame for {:?}", timeouts.read)),
        };
        let text = match frame.opcode() {
            OpCode::Text => match std::str::from_utf8(frame.payload()) {
                Ok(text) => text,
                Err(err) => return FeedError::Contract(format!("{label}: a text frame not UTF-8: {err}")),
            },
            OpCode::Close => return close_error(label, &frame),
            OpCode::Ping | OpCode::Pong => continue,
            opcode => return FeedError::Contract(format!("{label}: unexpected {opcode:?} frame")),
        };
        if let Err(err) = record(stream, text, latest, now_ms()) {
            return FeedError::Contract(format!("{label}: {err}: {}", excerpt(text)));
        }
    }
}

#[derive(Deserialize)]
struct Frame<'a> {
    stream: &'a str,
    #[serde(borrow)]
    data: LazyValue<'a>,
}

#[derive(Deserialize)]
struct MarkUpdate<'a> {
    e: &'a str,
    #[serde(rename = "E")]
    event_ms: u64,
    s: &'a str,
    p: &'a str,
    i: &'a str,
    r: &'a str,
    #[serde(rename = "T")]
    next_funding_ms: u64,
}

#[derive(Deserialize)]
struct Ticker<'a> {
    e: Option<&'a str>,
    s: &'a str,
    b: &'a str,
    a: &'a str,
    #[serde(rename = "T")]
    transaction_ms: Option<u64>,
}

/// Takes one frame of `stream` into `latest`: `Err` is the stream's contract broken.
fn record(stream: &Stream, text: &str, latest: &BinanceLatest, received_ms: u64) -> Result<(), String> {
    let frame: Frame<'_> = strict(text, "Binance combined frame")?;
    let data = frame.data.as_raw_str();
    match stream.kind {
        Kind::Marks => {
            let update: MarkUpdate<'_> = strict(data, "Binance markPriceUpdate")?;
            if update.e != "markPriceUpdate" || !frame.stream.ends_with("@markPrice") {
                return Err(format!("event {:?} on stream {:?}", update.e, frame.stream));
            }
            subscribed(stream, update.s)?;
            let mark = MarkPrice {
                mark: positive(update.p, "mark price")?,
                index: positive(update.i, "index price")?,
                funding_rate: signed_decimal(update.r, "funding rate")?,
                next_funding_ms: update.next_funding_ms,
                event_ms: update.event_ms,
                received_ms,
            };
            latest.lock().marks.insert(update.s.to_string(), mark);
        }
        Kind::PerpBook | Kind::SpotBook => {
            let ticker: Ticker<'_> = strict(data, "Binance bookTicker")?;
            // a perp ticker names its event and time, a spot one neither (the local marks' feeds' rule)
            let perp = stream.kind == Kind::PerpBook;
            if !frame.stream.ends_with("@bookTicker") || (perp && ticker.e != Some("bookTicker")) || (perp == ticker.transaction_ms.is_none()) {
                return Err(format!("event {:?} on stream {:?}", ticker.e, frame.stream));
            }
            subscribed(stream, ticker.s)?;
            let book = BookTicker { bid: positive(ticker.b, "bid")?, ask: positive(ticker.a, "ask")?, event_ms: ticker.transaction_ms, received_ms };
            latest.lock().books.insert(ticker.s.to_string(), book);
        }
    }
    Ok(())
}

fn subscribed(stream: &Stream, symbol: &str) -> Result<(), String> {
    if stream.symbols.contains(symbol) { Ok(()) } else { Err(format!("symbol {symbol} not subscribed")) }
}

fn positive(text: &str, what: &str) -> Result<VenuePrice, String> {
    let price = VenuePrice::parse(text).map_err(|err| format!("{what} {text:?}: {err}"))?;
    if price.is_positive() { Ok(price) } else { Err(format!("{what} {text:?} not positive")) }
}

/// A decimal with an optional minus sign, as written.
fn signed_decimal(text: &str, what: &str) -> Result<String, String> {
    VenuePrice::parse(text.strip_prefix('-').unwrap_or(text)).map_err(|err| format!("{what} {text:?}: {err}"))?;
    Ok(text.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// bm 2026-11-05 10:47Z, `/market/stream`: a crypto contract's and a `TradFi` one's (`SNDKUSDT`, its fields as BTC's).
    const BTC_MARK: &str = r#"{"stream":"btcusdt@markPrice","data":{"e":"markPriceUpdate","E":1793875629000,"s":"BTCUSDT","p":"80305.60000000","ap":"80305.60000000","P":"80403.63857391","i":"80306.36934783","r":"0.00010000","T":1793894400000,"st":1}}"#;
    const SNDK_MARK: &str = r#"{"stream":"sndkusdt@markPrice","data":{"e":"markPriceUpdate","E":1793875629000,"s":"SNDKUSDT","p":"1008.69584924","ap":"1008.69584924","P":"1008.70000000","i":"1008.16911533","r":"-0.00003000","T":1793894400000,"st":1}}"#;
    const HYPE_BOOK: &str = r#"{"stream":"hypeusdt@bookTicker","data":{"e":"bookTicker","u":1,"s":"HYPEUSDT","b":"34.1230","B":"10","a":"34.1240","A":"12","T":1793875629010,"E":1793875629012}}"#;
    const USDC_BOOK: &str = r#"{"stream":"usdcusdt@bookTicker","data":{"u":2619720016,"s":"USDCUSDT","b":"1.00077000","B":"181722.00000000","a":"1.00078000","A":"2823686.00000000"}}"#;

    fn stream(kind: Kind, symbols: &[&str]) -> Stream {
        Stream { kind, shard: 0, symbols: symbols.iter().map(|symbol| (*symbol).to_string()).collect() }
    }

    #[test]
    fn mark_frames_take_mark_index_and_funding() {
        let latest = BinanceLatest::default();
        let marks = stream(Kind::Marks, &["BTCUSDT", "SNDKUSDT"]);
        record(&marks, BTC_MARK, &latest, 7).unwrap();
        record(&marks, SNDK_MARK, &latest, 8).unwrap();
        let Latest::Received(btc) = latest.mark("BTCUSDT") else { panic!("BTC received") };
        assert_eq!((btc.mark.to_string(), btc.index.to_string(), btc.funding_rate.as_str()), ("80305.6".into(), "80306.36934783".into(), "0.00010000"));
        assert_eq!((btc.next_funding_ms, btc.event_ms, btc.received_ms), (1_793_894_400_000, 1_793_875_629_000, 7));
        let Latest::Received(sndk) = latest.mark("SNDKUSDT") else { panic!("SNDK received") };
        assert_eq!(sndk.funding_rate, "-0.00003000", "a negative funding rate as written");
        assert_eq!(latest.mark("ETHUSDT"), Latest::NotReceived);
    }

    #[test]
    fn mark_frames_off_the_contract_are_refused() {
        let latest = BinanceLatest::default();
        let marks = stream(Kind::Marks, &["BTCUSDT"]);
        assert!(record(&marks, SNDK_MARK, &latest, 1).unwrap_err().contains("not subscribed"));
        assert!(record(&marks, &BTC_MARK.replace("markPriceUpdate", "kline"), &latest, 1).is_err());
        assert!(record(&marks, &BTC_MARK.replace(r#""i":"80306.36934783","#, ""), &latest, 1).is_err(), "no index");
        assert!(record(&marks, &BTC_MARK.replace("80305.60000000\",\"ap", "0\",\"ap"), &latest, 1).is_err(), "a zero mark");
        assert_eq!(latest.mark("BTCUSDT"), Latest::NotReceived);
    }

    #[test]
    fn book_tickers_of_the_perp_and_the_spot() {
        let latest = BinanceLatest::default();
        record(&stream(Kind::PerpBook, &[HYPE_SYMBOL]), HYPE_BOOK, &latest, 3).unwrap();
        record(&stream(Kind::SpotBook, &[USDC_USDT_SYMBOL]), USDC_BOOK, &latest, 4).unwrap();
        let Latest::Received(hype) = latest.book(HYPE_SYMBOL) else { panic!("HYPE received") };
        assert_eq!((hype.bid.to_string(), hype.ask.to_string(), hype.event_ms, hype.received_ms), ("34.123".into(), "34.124".into(), Some(1_793_875_629_010), 3));
        let Latest::Received(usdc) = latest.book(USDC_USDT_SYMBOL) else { panic!("USDC received") };
        assert_eq!((usdc.bid.to_string(), usdc.ask.to_string(), usdc.event_ms), ("1.00077".into(), "1.00078".into(), None));
        assert!(record(&stream(Kind::SpotBook, &[USDC_USDT_SYMBOL]), HYPE_BOOK, &latest, 5).is_err(), "a perp ticker on the spot stream");
        assert!(record(&stream(Kind::PerpBook, &[HYPE_SYMBOL]), USDC_BOOK, &latest, 5).is_err(), "a spot ticker on the perp stream");
    }

    #[test]
    fn a_stopped_stream_reads_stopped() {
        let latest = BinanceLatest::default();
        let marks = stream(Kind::Marks, &["BTCUSDT"]);
        record(&marks, BTC_MARK, &latest, 7).unwrap();
        latest.stop(&marks, "symbol X not subscribed");
        assert_eq!(latest.mark("BTCUSDT"), Latest::Stopped("symbol X not subscribed".into()));
        assert_eq!(latest.book(HYPE_SYMBOL), Latest::NotReceived, "another stream untouched");
    }

    #[test]
    fn mark_streams_split_by_connection_beside_the_two_books() {
        let symbols: BTreeSet<String> = (0..266).map(|n| format!("C{n:03}USDT")).collect();
        let all = streams(&symbols);
        let marks: Vec<&Stream> = all.iter().filter(|stream| stream.kind == Kind::Marks).collect();
        assert_eq!(marks.iter().map(|stream| stream.symbols.len()).collect::<Vec<_>>(), vec![100, 100, 66]);
        assert_eq!(marks.iter().map(|stream| stream.shard).collect::<Vec<_>>(), vec![0, 1, 2]);
        assert!(marks[0].url().starts_with("wss://fstream.binance.com/market/stream?streams=c000usdt@markPrice/c001usdt@markPrice"));
        let books: Vec<String> = all.iter().filter(|stream| stream.kind != Kind::Marks).map(Stream::url).collect();
        assert_eq!(books, vec!["wss://fstream.binance.com/public/stream?streams=hypeusdt@bookTicker", "wss://stream.binance.com:9443/stream?streams=usdcusdt@bookTicker"]);
    }
}
