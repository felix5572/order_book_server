//! Alpaca's stock streams (design 000535 r1 §2.1, r2 revisions 5–6; docs.alpaca.markets/docs/streaming-market-data):
//! the SIP's and, overnight, BOATS's (design 000543; the trading system's `cross_venue/alpaca` runs both with this
//! protocol), one connection each subscribing the quotes of every source. After connecting the server answers `connected`; the auth and
//! the subscription are sent at once and answered `authenticated` and `subscription` in order (bm 10-27: the 85 sources'
//! quotes followed within a second). A message is `{"T": ...}` in a JSON array frame; a quote is
//! `{"T":"q","S":"GOOGL","bx":"Q","bp":322.9,"bs":80,"ax":"Q","ap":322.93,"as":40,"c":["R"],"z":"C","t":"2026-10-27T19:51:07.196393577Z"}`.
//!
//! Errors (the trading system's `cross_venue/alpaca/protocol.rs` classes): 406 (the account's sessions in use) is
//! `ConnectionLimit`, budgeted by the feed task; 407 (slow client) and 500 (internal error) end the session for a
//! reconnect; 400–405, 409, 410 and any other code are deterministic: a contract error, the feed stops.

use std::time::Duration;

use serde::Deserialize;
use sonic_rs::LazyValue;

use super::session::EquitySession;
use crate::local_marks::{
    protocol::{Heartbeat, Observation, Parsed, Session, index, price, strict},
    slot::Quote,
    sources::{Feed, FeedMarkets},
};

/// Alpaca defines no text ping and a quiet market sends nothing; a WebSocket ping every 15 s keeps the read deadline
/// (the 10-14 research recorder pinged every 20 s for hours, never timing out).
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const KEY_ENV: &str = "APCA_API_KEY_ID";
const SECRET_ENV: &str = "APCA_API_SECRET_KEY";

/// The Alpaca account's key (the trading system's environment names). `Debug` shows neither value.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct AlpacaCredentials {
    key_id: String,
    secret: String,
}

impl std::fmt::Debug for AlpacaCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AlpacaCredentials(..)")
    }
}

impl AlpacaCredentials {
    /// From the process environment (r2 revision 6: the unit's `User=` bashrc, loaded by `bash -ic`); `Err` names the
    /// missing or empty variable, never a value.
    pub(crate) fn from_env() -> Result<Self, String> {
        let read = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty()).ok_or_else(|| format!("--hip3-sip-marks needs {name} in the environment"));
        Ok(Self { key_id: read(KEY_ENV)?, secret: read(SECRET_ENV)? })
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self { key_id: "key".to_string(), secret: "secret".to_string() }
    }
}

/// The HIP-3 US stocks' two Alpaca feeds: the SIP's consolidated NBBO in the day sessions, BOATS's quotes overnight
/// (docs.trade.xyz: "The overnight trading session is provided by Blue Ocean ATS (BOATS)").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EquityFeed {
    Sip,
    Boats,
}

impl EquityFeed {
    pub(crate) const ALL: [Self; 2] = [Self::Sip, Self::Boats];

    /// The feed of a session's quotes.
    pub(crate) const fn of(session: EquitySession) -> Self {
        match session {
            EquitySession::Pre | EquitySession::Regular | EquitySession::Post => Self::Sip,
            EquitySession::Overnight => Self::Boats,
        }
    }

    pub(crate) const fn feed(self) -> Feed {
        match self {
            Self::Sip => Feed::SIP,
            Self::Boats => Feed::BOATS,
        }
    }

    /// `sip`, `boats`: the frames' `quoteFeed`.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Sip => "sip",
            Self::Boats => "boats",
        }
    }

    const fn url(self) -> &'static str {
        match self {
            Self::Sip => "wss://stream.data.alpaca.markets/v2/sip",
            Self::Boats => "wss://stream.data.alpaca.markets/v1beta1/boats",
        }
    }
}

/// Auth, then every symbol's quotes; both acknowledged.
pub(crate) fn alpaca_session(feed: EquityFeed, symbols: &[String], credentials: &AlpacaCredentials) -> Session {
    let auth = sonic_rs::json!({"action": "auth", "key": credentials.key_id, "secret": credentials.secret});
    let subscribe = sonic_rs::json!({"action": "subscribe", "quotes": symbols});
    Session {
        url: feed.url().to_string(),
        requests: vec![auth.to_string(), subscribe.to_string()],
        acks: 2,
        heartbeat: Some((Heartbeat::WsPing, HEARTBEAT_INTERVAL)),
    }
}

#[derive(Deserialize)]
struct Message<'a> {
    #[serde(rename = "T")]
    kind: &'a str,
    msg: Option<String>,
    code: Option<u16>,
    #[serde(rename = "S")]
    symbol: Option<&'a str>,
    #[serde(borrow)]
    bp: Option<LazyValue<'a>>,
    #[serde(borrow)]
    ap: Option<LazyValue<'a>>,
    t: Option<&'a str>,
    quotes: Option<Vec<String>>,
}

/// One frame (an array of messages); `Err` is a contract error.
pub(crate) fn alpaca(text: &str, markets: &FeedMarkets) -> Result<Parsed, String> {
    let messages: Vec<Message<'_>> = strict(text, "Alpaca frame")?;
    let (mut quotes, mut acked, mut unknown) = (Vec::new(), 0, false);
    for message in messages {
        match message.kind {
            "q" => quotes.push(quote(&message, markets)?),
            "success" => match message.msg.as_deref() {
                Some("connected") => {}
                Some("authenticated") => acked += 1,
                _ => unknown = true,
            },
            "subscription" => {
                subscribed(message.quotes.as_deref().unwrap_or_default(), markets)?;
                acked += 1;
            }
            "error" => return error(message.code, message.msg.as_deref().unwrap_or("")),
            _ => unknown = true,
        }
    }
    match (quotes.is_empty(), acked, unknown) {
        (false, 0, false) => Ok(Parsed::Quotes(quotes)),
        (true, acked, false) if acked > 0 => Ok(Parsed::Acked(acked)),
        (true, 0, false) => Ok(Parsed::Ignored),
        (true, 0, true) => Ok(Parsed::Unknown),
        _ => Err("an Alpaca frame mixing quotes, acknowledgements or unknown messages".to_string()),
    }
}

/// The subscription answer lists every symbol requested: one left out would never quote.
fn subscribed(listed: &[String], markets: &FeedMarkets) -> Result<(), String> {
    let missing: Vec<&String> = markets.symbols.iter().filter(|symbol| !listed.contains(symbol)).collect();
    if missing.is_empty() {
        return Ok(());
    }
    Err(format!("Alpaca subscribed {} quote symbols, without {missing:?}", listed.len()))
}

fn error(code: Option<u16>, msg: &str) -> Result<Parsed, String> {
    match code {
        Some(406) => Ok(Parsed::ConnectionLimit(format!("Alpaca error 406 {msg}"))),
        Some(code @ (407 | 500)) => Ok(Parsed::SessionFailed(format!("Alpaca error {code} {msg}"))),
        Some(code) => Err(format!("Alpaca error {code} {msg}")),
        None => Err(format!("an Alpaca error without its code: {msg}")),
    }
}

/// A quote: both sides positive and not crossed, else no quote now. A crossed NBBO is a state the SIP shows (locked and
/// crossed markets occur), not a broken feed: unlike the crypto venues' books it does not stop the feed.
fn quote(message: &Message<'_>, markets: &FeedMarkets) -> Result<Observation, String> {
    let symbol = message.symbol.ok_or("an Alpaca quote without its symbol")?;
    let side = |value: Option<&LazyValue<'_>>| value.map(price).transpose().map(Option::flatten);
    let (bid, ask) = (side(message.bp.as_ref())?, side(message.ap.as_ref())?);
    // the quote's age gates the prediction (`sipQuiet`): a quote without its SIP time is a broken frame (review 000536 P2-2)
    let event_ms = sip_ms(message.t.ok_or_else(|| format!("{symbol}: an Alpaca quote without its SIP time `t`"))?).map_err(|err| format!("{symbol}: {err}"))?;
    let quote = match (bid, ask) {
        (Some(bid), Some(ask)) if bid <= ask => Some(Quote { bid, ask }),
        _ => None,
    };
    Ok((index(markets, symbol)?, quote, Some(event_ms)))
}

/// The SIP time `2026-10-27T19:51:07.196393577Z` in unix ms.
fn sip_ms(text: &str) -> Result<u64, String> {
    let time = chrono::DateTime::parse_from_rfc3339(text).map_err(|err| format!("time {text:?}: {err}"))?;
    u64::try_from(time.timestamp_millis()).map_err(|_| format!("time {text:?} before 1970"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_marks::{hip3::sources::Hip3SipSources, price::VenuePrice};

    fn markets() -> FeedMarkets {
        Hip3SipSources::parse(r#"{"feed": "sip", "coins": {"xyz:GOOGL": {"symbol": "GOOGL"}, "xyz:TSLA": {"symbol": "TSLA"}}}"#).unwrap().markets
    }

    /// bm 10-27 19:51Z frames, as Alpaca sent them (the subscription answer cut to these two symbols).
    #[test]
    fn the_session_frames_parse() {
        let markets = markets();
        assert_eq!(alpaca(r#"[{"T":"success","msg":"connected"}]"#, &markets), Ok(Parsed::Ignored));
        assert_eq!(alpaca(r#"[{"T":"success","msg":"authenticated"}]"#, &markets), Ok(Parsed::Acked(1)));
        assert_eq!(alpaca(r#"[{"T":"subscription","trades":[],"quotes":["GOOGL","TSLA"],"bars":[]}]"#, &markets), Ok(Parsed::Acked(1)));
        let frame = r#"[{"T":"q","S":"GOOGL","bx":"Q","bp":322.9,"bs":80,"ax":"Q","ap":322.93,"as":40,"c":["R"],"z":"C","t":"2026-10-27T19:51:07.196393577Z"},
                        {"T":"q","S":"TSLA","bx":"P","bp":0,"bs":0,"ax":"Q","ap":405.8,"as":40,"c":["R"],"z":"C","t":"2026-10-27T19:51:07.2Z"}]"#;
        let quote = Quote { bid: VenuePrice::parse("322.9").unwrap(), ask: VenuePrice::parse("322.93").unwrap() };
        assert_eq!(alpaca(frame, &markets), Ok(Parsed::Quotes(vec![(0, Some(quote), Some(1_793_130_667_196)), (1, None, Some(1_793_130_667_200))])), "a zero bid is no quote");
        let crossed = r#"[{"T":"q","S":"TSLA","bp":405.9,"ap":405.8,"t":"2026-10-27T19:51:07.2Z"}]"#;
        assert_eq!(alpaca(crossed, &markets), Ok(Parsed::Quotes(vec![(1, None, Some(1_793_130_667_200))])), "a crossed NBBO quotes nothing, the feed runs on");
    }

    #[test]
    fn errors_are_classified_as_the_trading_system_does() {
        let markets = markets();
        assert!(matches!(alpaca(r#"[{"T":"error","code":406,"msg":"connection limit exceeded"}]"#, &markets), Ok(Parsed::ConnectionLimit(reason)) if reason.contains("406")));
        assert!(matches!(alpaca(r#"[{"T":"error","code":407,"msg":"slow client"}]"#, &markets), Ok(Parsed::SessionFailed(_))));
        assert!(matches!(alpaca(r#"[{"T":"error","code":500,"msg":"internal error"}]"#, &markets), Ok(Parsed::SessionFailed(_))));
        for code in [400, 401, 402, 405, 409, 410, 499] {
            let frame = format!(r#"[{{"T":"error","code":{code},"msg":"x"}}]"#);
            assert!(alpaca(&frame, &markets).is_err_and(|err| err.contains(&code.to_string())), "{code}");
        }
        let partial = r#"[{"T":"subscription","quotes":["GOOGL"]}]"#;
        assert!(alpaca(partial, &markets).is_err_and(|err| err.contains("TSLA")), "a symbol left out of the subscription");
        assert!(alpaca(r#"[{"T":"q","S":"AAPL","bp":1,"ap":2,"t":"2026-10-27T19:51:07Z"}]"#, &markets).is_err(), "a quote not subscribed to");
        assert_eq!(alpaca(r#"[{"T":"b","S":"TSLA"}]"#, &markets), Ok(Parsed::Unknown));
        for time in ["", r#","t":null"#, r#","t":"yesterday""#] {
            let frame = format!(r#"[{{"T":"q","S":"TSLA","bp":405.7,"ap":405.8{time}}}]"#);
            assert!(alpaca(&frame, &markets).is_err_and(|err| err.contains("TSLA")), "{frame}: a quote's age must be known");
        }
    }

    /// bm 10-28 02:42Z, a second BOATS connection beside the trading system's: its subscription answer as sent, then a
    /// quote in the REST history's fields (Blue Ocean `B` both sides, tape `N`).
    #[test]
    fn boats_answers_as_the_sip() {
        let coins = r#"{"feed": "sip", "coins": {"xyz:AAPL": {"symbol": "AAPL"}, "xyz:NVDA": {"symbol": "NVDA"}, "xyz:QNT": {"symbol": "QNT"}, "xyz:TSLA": {"symbol": "TSLA"}}}"#;
        let markets = Hip3SipSources::parse(coins).unwrap().markets;
        let ack = r#"[{"T":"subscription","trades":[],"quotes":["AAPL","NVDA","QNT","TSLA"],"bars":[],"updatedBars":[],"dailyBars":[],"statuses":[],"corrections":[],"cancelErrors":[]}]"#;
        assert_eq!(alpaca(ack, &markets), Ok(Parsed::Acked(1)));
        let frame = r#"[{"T":"q","S":"NVDA","bx":"B","bp":217.61,"bs":100,"ax":"B","ap":219.3,"as":130,"c":[],"z":"N","t":"2026-10-28T00:00:00.034772514Z"}]"#;
        let quote = Quote { bid: VenuePrice::parse("217.61").unwrap(), ask: VenuePrice::parse("219.3").unwrap() };
        assert_eq!(alpaca(frame, &markets), Ok(Parsed::Quotes(vec![(1, Some(quote), Some(1_793_145_600_034))])));
        let session = alpaca_session(EquityFeed::Boats, &["NVDA".to_string()], &AlpacaCredentials::for_test());
        assert_eq!(session.url, "wss://stream.data.alpaca.markets/v1beta1/boats");
        assert_eq!(alpaca_session(EquityFeed::Sip, &["NVDA".to_string()], &AlpacaCredentials::for_test()).url, "wss://stream.data.alpaca.markets/v2/sip");
    }

    #[test]
    fn the_session_authenticates_then_subscribes_and_hides_the_secret() {
        let session = alpaca_session(EquityFeed::Sip, &["GOOGL".to_string(), "TSLA".to_string()], &AlpacaCredentials::for_test());
        assert_eq!(session.requests[0], r#"{"action":"auth","key":"key","secret":"secret"}"#);
        assert_eq!(session.requests[1], r#"{"action":"subscribe","quotes":["GOOGL","TSLA"]}"#);
        assert_eq!((session.acks, &session.heartbeat), (2, &Some((Heartbeat::WsPing, HEARTBEAT_INTERVAL))));
        let shown = format!("{session:?} {:?}", AlpacaCredentials::for_test());
        assert!(!shown.contains("secret\"") && !shown.contains("\"key\""), "{shown}");
    }
}
