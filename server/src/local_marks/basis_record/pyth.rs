//! The record's Pyth USDT/USD and USDC/USD (hyperliquid-trade design 000751 r3): one request of Hermes'
//! `/v2/updates/price/latest` for both every `POLL`, the token as a Bearer header only, never logged. The trading
//! system's stable rate reads the same feeds with the same token (`mm_configs/base.toml`).
//!
//! Each feed keeps its latest price as Hermes wrote it - `price`, `conf`, `expo`, `publish_time` (unix s) - and when
//! it arrived. A failure the REST client calls transient (no answer, 408, 429, 5xx) is counted, warned at most once a
//! minute, and the next poll asks again, the last prices kept with their own times; anything else - another status, an
//! answer off the contract - stops the poller until restart, its prices then recorded unavailable (review 000751 r3
//! P3: a deterministic error is not retried every 10 s).

#![expect(clippy::similar_names, reason = "USDT/USD and USDC/USD are the feeds' own names")]

use std::{
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use hyper::Method;
use log::{error, warn};
use serde::Deserialize;
use tokio::time::{Instant, MissedTickBehavior};

use super::Latest;
use crate::{
    local_marks::{connection::FeedError, now_ms, protocol::excerpt, rest::RestClient},
    metrics::{BASIS_RECORD_PYTH_ERRORS_TOTAL, BASIS_RECORD_STREAM_STOPPED},
};

const HOST: &str = "hermes.pyth.network";
/// Pyth's USDT/USD and USDC/USD feeds, as the trading system's `mm_configs/base.toml` names them.
pub(crate) const USDT_USD_FEED: &str = "2b89b9dc8fdf9f34709a5b106b472f0f39bb6ca9ce04b0fd7f2e971688e2e53b";
pub(crate) const USDC_USD_FEED: &str = "eaa020c61cc479712813461ce153894a96a6c00b21ed0cfc2798d1f9a9e9c94a";
const POLL: Duration = Duration::from_secs(10);
const WARN_EVERY: Duration = Duration::from_secs(60);
const LABEL: &str = "pyth_hermes";

/// A feed's price as Hermes wrote it: integer texts, the exponent, the publish time in unix seconds; and our receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PythPrice {
    pub(crate) price: String,
    pub(crate) conf: String,
    pub(crate) expo: i32,
    pub(crate) publish_time_s: u64,
    pub(crate) received_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Prices {
    usdt_usd: Latest<PythPrice>,
    usdc_usd: Latest<PythPrice>,
}

/// Both feeds' latest prices; the poller writes, the sampler copies.
pub(crate) struct PythLatest {
    prices: Mutex<Prices>,
}

impl PythLatest {
    pub(crate) const fn new() -> Self {
        Self { prices: Mutex::new(Prices { usdt_usd: Latest::NotReceived, usdc_usd: Latest::NotReceived }) }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Prices> {
        self.prices.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn usdt_usd(&self) -> Latest<PythPrice> {
        self.lock().usdt_usd.clone()
    }

    pub(crate) fn usdc_usd(&self) -> Latest<PythPrice> {
        self.lock().usdc_usd.clone()
    }

    fn set(&self, usdt_usd: PythPrice, usdc_usd: PythPrice) {
        *self.lock() = Prices { usdt_usd: Latest::Received(usdt_usd), usdc_usd: Latest::Received(usdc_usd) };
    }

    fn stop(&self, reason: &str) {
        *self.lock() = Prices { usdt_usd: Latest::Stopped(reason.to_string()), usdc_usd: Latest::Stopped(reason.to_string()) };
    }
}

/// Starts the poller with `token`, the environment's Bearer token.
pub(crate) fn start(token: String) -> Arc<PythLatest> {
    let latest = Arc::new(PythLatest::new());
    let writer = Arc::clone(&latest);
    tokio::spawn(async move { run(&token, &writer).await });
    latest
}

fn path() -> String {
    // `ids[]` percent-encoded: a request URI takes no bare brackets
    format!("/v2/updates/price/latest?ids%5B%5D=0x{USDT_USD_FEED}&ids%5B%5D=0x{USDC_USD_FEED}&parsed=true")
}

async fn run(token: &str, latest: &PythLatest) {
    let mut client = RestClient::new(HOST);
    let path = path();
    let mut ticker = tokio::time::interval(POLL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_warning: Option<Instant> = None;
    loop {
        ticker.tick().await;
        let answer = client.call_authorized(Method::GET, &path, token).await;
        let failure = match answer.map(|body| parse(&body, now_ms())) {
            Ok(Ok((usdt_usd, usdc_usd))) => {
                latest.set(usdt_usd, usdc_usd);
                continue;
            }
            Ok(Err(reason)) | Err(FeedError::Contract(reason)) => Err(reason),
            Err(FeedError::Transient(reason) | FeedError::ReconnectRequested(reason) | FeedError::ConnectionLimit(reason)) => Ok(reason),
        };
        match failure {
            Err(reason) => {
                error!("basis record {LABEL}: stopped until restart, its prices recorded unavailable: {reason}");
                BASIS_RECORD_STREAM_STOPPED.with_label_values(&[LABEL]).set(1);
                latest.stop(&reason);
                return;
            }
            Ok(reason) => {
                BASIS_RECORD_PYTH_ERRORS_TOTAL.inc();
                if last_warning.is_none_or(|at| at.elapsed() >= WARN_EVERY) {
                    warn!("basis record {LABEL}: a poll failed, the last prices kept, asked again in {POLL:?}: {reason}");
                    last_warning = Some(Instant::now());
                }
            }
        }
    }
}

#[derive(Deserialize)]
struct Answer {
    parsed: Vec<Item>,
}

#[derive(Deserialize)]
struct Item {
    id: String,
    price: Price,
}

#[derive(Deserialize)]
struct Price {
    price: String,
    conf: String,
    expo: i32,
    publish_time: u64,
}

/// Both feeds' prices of an answer; `Err` is the answer off the contract, with an excerpt of it.
fn parse(body: &[u8], received_ms: u64) -> Result<(PythPrice, PythPrice), String> {
    let text = String::from_utf8_lossy(body);
    let off = |what: String| format!("Hermes answer {what}: {}", excerpt(&text));
    let answer: Answer = serde_json::from_slice(body).map_err(|err| off(err.to_string()))?;
    let mut usdt_usd = None;
    let mut usdc_usd = None;
    for item in answer.parsed {
        let id = item.id.trim_start_matches("0x").to_ascii_lowercase();
        let slot = match id.as_str() {
            USDT_USD_FEED => &mut usdt_usd,
            USDC_USD_FEED => &mut usdc_usd,
            _ => return Err(off(format!("with feed {id} not asked for"))),
        };
        let Price { price, conf, expo, publish_time } = item.price;
        if !integer(&price, true) || !integer(&conf, false) {
            return Err(off(format!("with price {price:?} conf {conf:?} of feed {id}")));
        }
        if slot.replace(PythPrice { price, conf, expo, publish_time_s: publish_time, received_ms }).is_some() {
            return Err(off(format!("with feed {id} twice")));
        }
    }
    match (usdt_usd, usdc_usd) {
        (Some(usdt_usd), Some(usdc_usd)) => Ok((usdt_usd, usdc_usd)),
        _ => Err(off("without both feeds".to_string())),
    }
}

/// Decimal digits, a leading minus allowed where `signed`.
fn integer(text: &str, signed: bool) -> bool {
    let digits = if signed { text.strip_prefix('-').unwrap_or(text) } else { text };
    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// bm 2026-11-05 11:04Z, both feeds (`binary` and `metadata` left out: not read).
    const ANSWER: &str = r#"{"binary":{"encoding":"hex","data":[]},"parsed":[
        {"id":"2b89b9dc8fdf9f34709a5b106b472f0f39bb6ca9ce04b0fd7f2e971688e2e53b","price":{"price":"99890956","conf":"4044","expo":-8,"publish_time":1793876676},"ema_price":{"price":"99891000","conf":"4000","expo":-8,"publish_time":1793876676},"metadata":{}},
        {"id":"eaa020c61cc479712813461ce153894a96a6c00b21ed0cfc2798d1f9a9e9c94a","price":{"price":"99988394","conf":"7564","expo":-8,"publish_time":1793876676},"ema_price":{"price":"99988000","conf":"7000","expo":-8,"publish_time":1793876676},"metadata":{}}]}"#;

    #[test]
    fn both_feeds_are_taken_as_written() {
        let (usdt, usdc) = parse(ANSWER.as_bytes(), 5).unwrap();
        assert_eq!(usdt, PythPrice { price: "99890956".into(), conf: "4044".into(), expo: -8, publish_time_s: 1_793_876_676, received_ms: 5 });
        assert_eq!((usdc.price.as_str(), usdc.conf.as_str()), ("99988394", "7564"));
        assert!(path().contains(&format!("ids%5B%5D=0x{USDT_USD_FEED}&ids%5B%5D=0x{USDC_USD_FEED}")));
    }

    #[test]
    fn answers_off_the_contract_are_refused() {
        let one = ANSWER.replace(USDC_USD_FEED, USDT_USD_FEED);
        assert!(parse(one.as_bytes(), 1).unwrap_err().contains("twice"));
        let other = ANSWER.replace(USDC_USD_FEED, &"ab".repeat(32));
        assert!(parse(other.as_bytes(), 1).unwrap_err().contains("not asked for"));
        assert!(parse(ANSWER.replace(r#""conf":"4044","#, "").as_bytes(), 1).is_err(), "no conf");
        assert!(parse(ANSWER.replace(r#""99890956""#, r#""0.9989""#).as_bytes(), 1).is_err(), "a price not an integer");
        assert!(parse(br#"{"parsed":[]}"#, 1).unwrap_err().contains("without both feeds"));
        assert!(parse(b"not json", 1).is_err());
    }

    #[test]
    fn a_stopped_poller_reads_stopped() {
        let latest = PythLatest::new();
        let (usdt, usdc) = parse(ANSWER.as_bytes(), 5).unwrap();
        latest.set(usdt.clone(), usdc);
        assert_eq!(latest.usdt_usd(), Latest::Received(usdt));
        latest.stop("HTTP 401");
        assert_eq!((latest.usdt_usd(), latest.usdc_usd()), (Latest::Stopped("HTTP 401".into()), Latest::Stopped("HTTP 401".into())));
    }
}
