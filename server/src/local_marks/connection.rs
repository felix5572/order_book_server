//! One WS feed connection (design 000454 r2 revision 4): connect, subscribe, then read, keeping the protocol's own
//! health - its subscription acks, its heartbeat answered in time, some frame at least every `READ_TIMEOUT` - and
//! writing every quote into the feed's slot. It ends with the error that ended it: a transient one is reconnected by
//! the caller, a contract error stops this connection partition.

use std::time::Duration;

use futures_util::SinkExt;
use log::warn;
use tokio::time::{Instant, sleep_until, timeout};
use yawc::{Frame, OpCode, Options, TcpWebSocket, WebSocket};

use super::{
    protocol::{Parsed, Protocol, Session, excerpt},
    slot::FeedSlot,
    sources::FeedMarkets,
};
use crate::{
    local_marks::now_ms,
    metrics::{LOCAL_MARKS_FEED_FRAMES_TOTAL, LOCAL_MARKS_FEED_UNKNOWN_FRAMES_TOTAL},
};

/// A connection's limits.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Timeouts {
    pub(crate) connect: Duration,
    pub(crate) send: Duration,
    /// Every request acknowledged within it, else the connection is retried.
    pub(crate) ack: Duration,
    /// A heartbeat answered within it.
    pub(crate) pong: Duration,
    /// Some frame - a quote, a pong, the venue's heartbeat - within it.
    pub(crate) read: Duration,
}

impl Timeouts {
    pub(crate) const FEEDS: Self = Self {
        connect: Duration::from_secs(10),
        send: Duration::from_secs(5),
        ack: Duration::from_secs(10),
        pong: Duration::from_secs(10),
        read: Duration::from_secs(30),
    };
}
/// Kraken's status frame is the largest seen (under 2 KB); this bounds a broken peer.
const MAX_PAYLOAD: usize = 4 << 20;
/// Unknown frames warned per connection; all are counted.
const MAX_UNKNOWN_WARNINGS: u32 = 5;
/// Close codes of rotation or a transient server failure; others (protocol, policy) need a look.
const TRANSIENT_CLOSE: [u16; 6] = [1000, 1001, 1006, 1011, 1012, 1013];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FeedError {
    /// Lost or not answering: reconnected after a backoff.
    Transient(String),
    /// The venue requested a planned rotation; the caller withdraws quotes and reconnects with an info log.
    ReconnectRequested(String),
    /// Refused or unusable: the feed stops until restart.
    Contract(String),
}

/// Runs one connection of `session` until it fails.
pub(crate) async fn run_connection(protocol: Protocol, session: Session, markets: &FeedMarkets, slot: &FeedSlot, timeouts: Timeouts) -> FeedError {
    let label = slot.feed.label();
    let url = match url::Url::parse(&session.url) {
        Ok(url) => url,
        Err(err) => return FeedError::Contract(format!("{label}: URL {}: {err}", session.url)),
    };
    // Retain negotiated compression. Early BM recordings suggested fewer disconnects with it, but later
    // recordings reproduced frequent EOFs with compression too; it is not a proven fix. Binance perp
    // partitions are selected upstream from the same-backend comparison in subscriptions.rs.
    let options = Options::default().with_max_payload_read(MAX_PAYLOAD).with_low_latency_compression();
    let mut ws = match timeout(timeouts.connect, WebSocket::connect(url).with_options(options)).await {
        Ok(Ok(ws)) => ws,
        Ok(Err(err)) => return connect_error(label, &err),
        Err(_) => return FeedError::Transient(format!("{label}: no connection within {:?}", timeouts.connect)),
    };
    let epoch = slot.connected(now_ms());
    for request in &session.requests {
        if let Err(error) = send(&mut ws, label, request.clone(), timeouts.send).await {
            return error;
        }
    }
    let started = Instant::now();
    let (mut acked, mut unknown) = (0, 0);
    let mut ack_deadline = (session.acks > 0).then_some(started + timeouts.ack);
    let mut next_ping = session.heartbeat.as_ref().map(|(_, every)| started + *every);
    let mut pong_deadline: Option<Instant> = None;
    loop {
        let read_deadline = Instant::now() + timeouts.read;
        let timer = [ack_deadline, next_ping, pong_deadline].into_iter().flatten().min().unwrap_or(read_deadline).min(read_deadline);
        // `next_frame`, not the `Stream` impl: that one ends on any read error and drops it
        let frame = tokio::select! {
            frame = ws.next_frame() => frame,
            () = sleep_until(timer) => {
                let now = Instant::now();
                if ack_deadline.is_some_and(|deadline| now >= deadline) {
                    return FeedError::Transient(format!("{label}: {acked} of {} subscriptions acknowledged within {:?}", session.acks, timeouts.ack));
                }
                if pong_deadline.is_some_and(|deadline| now >= deadline) {
                    return FeedError::Transient(format!("{label}: no heartbeat answer within {:?}", timeouts.pong));
                }
                if now >= read_deadline {
                    return FeedError::Transient(format!("{label}: no frame for {:?}", timeouts.read));
                }
                if let (Some(at), Some((ping, every))) = (next_ping, &session.heartbeat) && now >= at {
                    if let Err(error) = send(&mut ws, label, ping.clone(), timeouts.send).await {
                        return error;
                    }
                    next_ping = Some(now + *every);
                    pong_deadline.get_or_insert(now + timeouts.pong);
                }
                continue;
            }
        };
        let frame = match frame {
            Ok(frame) => frame,
            Err(err) => return read_error(label, &err),
        };
        let text = match frame.opcode() {
            OpCode::Text => match std::str::from_utf8(frame.payload()) {
                Ok(text) => text,
                Err(err) => return FeedError::Contract(format!("{label}: a text frame not UTF-8: {err}")),
            },
            OpCode::Close => return close_error(label, &frame),
            // `yawc` answers pings itself
            OpCode::Ping | OpCode::Pong => continue,
            opcode => return FeedError::Contract(format!("{label}: unexpected {opcode:?} frame")),
        };
        let parsed = protocol.parse(text, markets);
        if parsed.is_ok() {
            LOCAL_MARKS_FEED_FRAMES_TOTAL.with_label_values(&[label]).inc();
        }
        match parsed {
            Ok(Parsed::Quotes(observations)) => slot.record(epoch, now_ms(), observations),
            Ok(Parsed::Acked(count)) => {
                acked += count;
                if acked >= session.acks {
                    ack_deadline = None;
                }
            }
            Ok(Parsed::Pong) => pong_deadline = None,
            Ok(Parsed::ReconnectRequested(reason)) => return FeedError::ReconnectRequested(format!("{label}: {reason}")),
            Ok(Parsed::Ignored) => {}
            Ok(Parsed::Unknown) => {
                LOCAL_MARKS_FEED_UNKNOWN_FRAMES_TOTAL.with_label_values(&[label]).inc();
                unknown += 1;
                if unknown <= MAX_UNKNOWN_WARNINGS {
                    warn!("local marks {label}: frame of an unknown type, not taken: {}", excerpt(text));
                }
            }
            Err(err) => return FeedError::Contract(format!("{label}: {err}")),
        }
    }
}

async fn send(ws: &mut TcpWebSocket, label: &str, text: String, within: Duration) -> Result<(), FeedError> {
    match timeout(within, ws.send(Frame::text(text))).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(FeedError::Transient(format!("{label}: send: {err}"))),
        Err(_) => Err(FeedError::Transient(format!("{label}: send not done within {within:?}"))),
    }
}

fn connect_error(label: &str, err: &yawc::WebSocketError) -> FeedError {
    match err {
        yawc::WebSocketError::InvalidStatusCode(code) if [408, 429, 500, 502, 503, 504].contains(code) => {
            FeedError::Transient(format!("{label}: handshake HTTP {code}"))
        }
        yawc::WebSocketError::IoError(io) if io.kind() != std::io::ErrorKind::InvalidData => FeedError::Transient(format!("{label}: connect: {err}")),
        yawc::WebSocketError::HTTPError(_) => FeedError::Transient(format!("{label}: connect: {err}")),
        // a refused handshake, a certificate the roots refuse
        _ => FeedError::Contract(format!("{label}: connect: {err}")),
    }
}

/// A failed read, with yawc's error as it gave it: the network failing or the stream ending is transient; a frame that
/// breaks the WebSocket protocol, or one over `MAX_PAYLOAD`, is the venue's contract.
fn read_error(label: &str, err: &yawc::WebSocketError) -> FeedError {
    match err {
        yawc::WebSocketError::IoError(io) => FeedError::Transient(format!("{label}: read: {io} (kind {:?})", io.kind())),
        yawc::WebSocketError::ConnectionClosed => FeedError::Transient(format!("{label}: read: {err}")),
        _ => FeedError::Contract(format!("{label}: read: {err}")),
    }
}

fn close_error(label: &str, frame: &Frame) -> FeedError {
    let code = frame.close_code().map(u16::from);
    let reason = match frame.close_reason() {
        Ok(reason) => format!("{:?}", reason.unwrap_or("")),
        Err(err) => format!("(a reason that does not read: {err})"),
    };
    match code {
        Some(code) if !TRANSIENT_CLOSE.contains(&code) => FeedError::Contract(format!("{label}: closed by the venue, code {code} {reason}")),
        _ => FeedError::Transient(format!("{label}: closed by the venue, code {code:?} {reason}")),
    }
}

#[cfg(test)]
mod tests {
    use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

    use axum::{Router, routing::get};
    use futures_util::StreamExt;
    use tokio::sync::mpsc;
    use yawc::close::CloseCode;

    use super::*;
    use crate::local_marks::{
        slot::Validity,
        sources::{Feed, Kind, MarkSources, Venue},
    };

    type Script = Arc<dyn Fn(yawc::HttpWebSocket, mpsc::UnboundedSender<String>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

    /// A local venue: each connection runs `script`, which reports the requests it read through the channel.
    async fn venue(script: Script) -> (String, mpsc::UnboundedReceiver<String>) {
        let (seen_tx, seen) = mpsc::unbounded_channel();
        let app = Router::new().route(
            "/",
            get(move |incoming: yawc::IncomingUpgrade| {
                let (script, seen_tx) = (Arc::clone(&script), seen_tx.clone());
                async move {
                    let (response, upgrade) = incoming.upgrade(Options::default()).unwrap();
                    tokio::spawn(async move { script(upgrade.await.unwrap(), seen_tx).await });
                    response
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("ws://{address}/"), seen)
    }

    async fn next_text(ws: &mut yawc::HttpWebSocket) -> Option<String> {
        loop {
            let frame = ws.next().await?;
            if frame.opcode() == OpCode::Text {
                return Some(String::from_utf8(frame.payload().to_vec()).unwrap());
            }
        }
    }

    const QUICK: Timeouts = Timeouts {
        connect: Duration::from_secs(2),
        send: Duration::from_secs(2),
        ack: Duration::from_millis(300),
        pong: Duration::from_millis(300),
        read: Duration::from_millis(500),
    };
    const OKX: Protocol = Protocol { kind: Kind::Spot, venue: Venue::Okx };
    const ACK: &str = r#"{"event":"subscribe","arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"connId":"1"}"#;
    const QUOTE: &str = r#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"data":[{"asks":[["82804","2","0","14"]],"bids":[["82803.9","1","0","17"]],"ts":"1792556634005"}]}"#;

    /// An OKX spot session of `acks` acknowledgements, with a heartbeat when given.
    async fn run(url: String, acks: usize, heartbeat: Option<Duration>) -> (FeedError, FeedSlot, usize) {
        let sources = MarkSources::embedded().unwrap();
        let markets = sources.markets(Feed { kind: Kind::Spot, venue: Venue::Okx }).unwrap();
        let slot = FeedSlot::new(Feed { kind: Kind::Spot, venue: Venue::Okx }, markets.symbols.len(), None);
        let session = Session { url, requests: vec![r#"{"op":"subscribe"}"#.to_string()], acks, heartbeat: heartbeat.map(|every| ("ping".to_string(), every)) };
        let error = run_connection(OKX, session, markets, &slot, QUICK).await;
        (error, slot, markets.index("BTC-USDT").unwrap())
    }

    #[tokio::test]
    async fn a_quote_is_kept_until_a_rotation_close_ends_the_connection() {
        let script: Script = Arc::new(|mut ws, seen| {
            Box::pin(async move {
                seen.send(next_text(&mut ws).await.unwrap()).unwrap();
                ws.send(Frame::text(ACK)).await.unwrap();
                ws.send(Frame::text(QUOTE)).await.unwrap();
                ws.send(Frame::close(CloseCode::Restart, "rotation")).await.unwrap();
            })
        });
        let (url, mut seen) = venue(script).await;
        let (error, slot, btc) = run(url, 1, None).await;
        assert!(matches!(&error, FeedError::Transient(reason) if reason.contains("1012")), "{error:?}");
        assert_eq!(seen.recv().await.unwrap(), r#"{"op":"subscribe"}"#, "the subscription went out");
        let (entry, validity) = slot.read(now_ms()).entries[btc];
        assert_eq!(validity, Validity::Valid, "still connected until the caller marks the loss");
        assert_eq!(entry.and_then(|entry| entry.quote).map(|quote| quote.bid.to_string()), Some("82803.9".to_string()));
        slot.disconnected(format!("{error:?}"), now_ms());
        assert_eq!(slot.read(now_ms()).entries[btc].1, Validity::NotConnected);
    }

    #[tokio::test]
    async fn a_gate_upgrade_requests_rotation_before_the_server_closes() {
        let fixtures: std::collections::BTreeMap<String, Vec<String>> = serde_json::from_str(include_str!("wire_fixtures.json")).unwrap();
        let frames = vec![fixtures["perp_gate"][0].clone(), fixtures["perp_gate"][1].clone(), fixtures["perp_gate_upgrade"][0].clone()];
        let script: Script = Arc::new(move |mut ws, _| {
            let frames = frames.clone();
            Box::pin(async move {
                next_text(&mut ws).await.unwrap();
                for text in frames { ws.send(Frame::text(text)).await.unwrap(); }
                // The server stays open: the upgrade notification, not EOF or a timeout, must end the client.
                while next_text(&mut ws).await.is_some() {}
            })
        });
        let (url, _) = venue(script).await;
        let sources = MarkSources::embedded().unwrap();
        let feed = Feed { kind: Kind::Perp, venue: Venue::Gate };
        let markets = sources.markets(feed).unwrap();
        let slot = FeedSlot::new(feed, markets.symbols.len(), None);
        let protocol = Protocol { kind: feed.kind, venue: feed.venue };
        let session = Session { url, ..protocol.session(&markets.symbols, 1, None) };
        let error = run_connection(protocol, session, markets, &slot, QUICK).await;
        let FeedError::ReconnectRequested(reason) = error else { panic!("{error:?}") };
        assert!(reason.contains("Gate service upgrade"));
        let index = markets.index("PEPE_USDT").unwrap();
        assert_eq!(slot.read(now_ms()).entries[index].1, Validity::Valid);
        // Both requested and failed connections use the same withdrawal/reconnect path in run_feed.
        slot.disconnected(reason, now_ms());
        assert_eq!(slot.read(now_ms()).entries[index].1, Validity::NotConnected);
        slot.connected(now_ms());
        assert_ne!(slot.read(now_ms()).entries[index].1, Validity::Valid, "rotation does not revive the old quote");
    }

    #[tokio::test]
    async fn a_refused_subscription_is_a_contract_error() {
        let script: Script = Arc::new(|mut ws, _| {
            Box::pin(async move {
                next_text(&mut ws).await.unwrap();
                ws.send(Frame::text(r#"{"event":"error","code":"60012","msg":"Invalid request"}"#)).await.unwrap();
                // held open until the client leaves
                while next_text(&mut ws).await.is_some() {}
            })
        });
        let (url, _) = venue(script).await;
        let (error, _, _) = run(url, 1, None).await;
        assert!(matches!(&error, FeedError::Contract(reason) if reason.contains("Invalid request")), "{error:?}");
    }

    #[tokio::test]
    async fn missing_acks_a_missing_pong_and_silence_end_the_connection() {
        // one of two acks
        let script: Script = Arc::new(|mut ws, _| {
            Box::pin(async move {
                next_text(&mut ws).await.unwrap();
                ws.send(Frame::text(ACK)).await.unwrap();
                while next_text(&mut ws).await.is_some() {}
            })
        });
        let (url, _) = venue(script).await;
        let (error, _, _) = run(url, 2, None).await;
        assert!(matches!(&error, FeedError::Transient(reason) if reason.contains("1 of 2 subscriptions acknowledged")), "{error:?}");
        // the first ping answered, then no more
        let script: Script = Arc::new(|mut ws, seen| {
            Box::pin(async move {
                next_text(&mut ws).await.unwrap();
                ws.send(Frame::text(ACK)).await.unwrap();
                let mut answered = false;
                while let Some(text) = next_text(&mut ws).await {
                    seen.send(text.clone()).unwrap();
                    if text == "ping" && !answered {
                        ws.send(Frame::text("pong")).await.unwrap();
                        answered = true;
                    }
                }
            })
        });
        let (url, mut seen) = venue(script).await;
        let (error, _, _) = run(url, 1, Some(Duration::from_millis(100))).await;
        assert!(matches!(&error, FeedError::Transient(reason) if reason.contains("no heartbeat answer")), "{error:?}");
        assert_eq!((seen.recv().await.as_deref(), seen.recv().await.as_deref()), (Some("ping"), Some("ping")));
        // acked, then nothing at all
        let script: Script = Arc::new(|mut ws, _| {
            Box::pin(async move {
                next_text(&mut ws).await.unwrap();
                ws.send(Frame::text(ACK)).await.unwrap();
                while next_text(&mut ws).await.is_some() {}
            })
        });
        let (url, _) = venue(script).await;
        let (error, _, _) = run(url, 1, None).await;
        assert!(matches!(&error, FeedError::Transient(reason) if reason.contains("no frame for")), "{error:?}");
    }

    /// A venue taking permessage-deflate: the quotes arrive compressed and parse the same.
    #[tokio::test]
    async fn a_compressed_connection_quotes() {
        let (seen_tx, mut seen) = mpsc::unbounded_channel();
        let app = Router::new().route(
            "/",
            get(move |incoming: yawc::IncomingUpgrade| {
                let seen_tx = seen_tx.clone();
                async move {
                    let (response, upgrade) = incoming.upgrade(Options::default().with_low_latency_compression()).unwrap();
                    // review 000461 r1 P3: the client asked for it, so the server's answer carries it
                    let extensions = response.headers().get("sec-websocket-extensions").map(|value| value.to_str().unwrap().to_string());
                    seen_tx.send(extensions.unwrap_or_default()).unwrap();
                    tokio::spawn(async move {
                        let mut ws = upgrade.await.unwrap();
                        seen_tx.send(next_text(&mut ws).await.unwrap()).unwrap();
                        ws.send(Frame::text(ACK)).await.unwrap();
                        ws.send(Frame::text(QUOTE)).await.unwrap();
                        ws.send(Frame::close(CloseCode::Restart, "rotation")).await.unwrap();
                    });
                    response
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let (error, slot, btc) = run(format!("ws://{address}/"), 1, None).await;
        assert!(matches!(&error, FeedError::Transient(reason) if reason.contains("1012")), "{error:?}");
        assert!(seen.recv().await.unwrap().contains("permessage-deflate"), "compression negotiated");
        assert_eq!(seen.recv().await.unwrap(), r#"{"op":"subscribe"}"#);
        assert_eq!(slot.read(now_ms()).entries[btc].1, Validity::Valid);
    }

    /// The venue drops the connection without a close frame: the read error is reported as yawc gave it (it used to
    /// read "closed by the venue", the `Stream` impl having dropped the error).
    #[tokio::test]
    async fn a_dropped_connection_reports_the_read_error() {
        let script: Script = Arc::new(|mut ws, _| {
            Box::pin(async move {
                next_text(&mut ws).await.unwrap();
                ws.send(Frame::text(ACK)).await.unwrap();
                ws.send(Frame::text(QUOTE)).await.unwrap();
                drop(ws);
            })
        });
        let (url, _) = venue(script).await;
        let (error, _, _) = run(url, 1, None).await;
        let FeedError::Transient(reason) = &error else { panic!("{error:?}") };
        assert!(reason.starts_with("spot_okx: read: "), "{reason}");
        assert!(!reason.contains("closed by the venue"), "{reason}");
    }

    #[test]
    fn read_errors_split_into_the_network_and_the_protocol() {
        let io = yawc::WebSocketError::IoError(std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset by peer"));
        assert!(matches!(read_error("perp_binance", &io), FeedError::Transient(reason) if reason == "perp_binance: read: reset by peer (kind ConnectionReset)"));
        assert!(matches!(read_error("perp_binance", &yawc::WebSocketError::ConnectionClosed), FeedError::Transient(_)));
        assert!(matches!(read_error("perp_binance", &yawc::WebSocketError::FrameTooLarge), FeedError::Contract(reason) if reason.contains("Frame too large")));
        assert!(matches!(read_error("perp_binance", &yawc::WebSocketError::InvalidOpCode(3)), FeedError::Contract(_)));
    }

    #[test]
    fn rotation_closes_are_transient_and_policy_closes_are_not() {
        let close = |code: u16| Frame::close(CloseCode::from(code), "bye");
        assert!(matches!(close_error("spot_okx", &close(1012)), FeedError::Transient(_)));
        assert!(matches!(close_error("spot_okx", &close(1008)), FeedError::Contract(reason) if reason.contains("code 1008")));
    }
}
