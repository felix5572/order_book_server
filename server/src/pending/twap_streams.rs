//! The node's two TWAP streams for the pending book (batch 2h, hyperliquid-trade design 000624 r4 section 1), each
//! followed by a watcher of its own that also proves its read-throughs (`feed::watch_proven`):
//!
//! - `node_twap_statuses_streaming/hourly/<date>/<hour>`: a line per block with TWAP events, `{"block_number",
//!   "events": [{"twap_id", "state": {"coin", "user", "side", "sz", "minutes", "reduceOnly", "randomize", "timestamp",
//!   "stopPx", ...}, "status"}]}`, the status `activated`, `waitingForTrigger`, `finished`, `terminated` or `{"error"}`
//!   (a suborder refused) - the five seen on bm 11-01 05:00-09:00;
//! - `node_fills_streaming/hourly/<date>/<hour>`: lines of fills, `{"block_number", "events": [[user, {"coin", "sz",
//!   "tid", "twapId", ...}]]}`; a TWAP suborder's fills carry its `twapId` (research `mempool_pending_book_20261031`:
//!   suborders show nowhere else, not in the order statuses nor the book diffs).
//!
//! A block's lines of both are read before the next block's `replica_cmds` line (bm 11-01, 300 s: 90 and 2,466 blocks,
//! at least 15.1 and 2.2 ms before; `scripts/twap_stream_arrival.py`) - a run assumption, as the account view's.

use std::path::Path;

use alloy::primitives::Address;
use serde::Deserialize;

use super::feed::{self, Feed, Proven};
use crate::{
    listeners::order_book::parallel::{FileLayout, LineShape, LineStream},
    order_book::{Coin, Sz},
};

/// Cap on one unterminated line: fills lines run to tens of kB.
const MAX_LINE_BYTES: usize = 16 << 20;

/// Lines queued for the pending thread (the fills ~40 a second on bm 11-01, the statuses well under 1).
const QUEUE_LINES: usize = 4_096;

/// A TWAP statuses line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TwapStatusLine {
    pub(crate) height: u64,
    pub(crate) events: Vec<TwapEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TwapEvent {
    /// It starts sending suborders (a `twapOrder`'s `running` response is not that: one waits for its trigger, a
    /// reduce-only one was activated 33 minutes after - research, 11-01 06:00-07:00).
    Activated(TwapActivation),
    /// Finished or terminated.
    Ended { twap_id: u64 },
    /// One of its suborders refused.
    Refused { twap_id: u64 },
    /// Placed with a trigger, waiting for it: no suborder until an activation follows.
    Waiting { twap_id: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TwapActivation {
    pub(crate) twap_id: u64,
    pub(crate) user: Address,
    pub(crate) coin: Coin,
    pub(crate) is_buy: bool,
    pub(crate) total: Sz,
    pub(crate) minutes: u64,
    pub(crate) randomize: bool,
    pub(crate) reduce_only: bool,
    /// Its start, unix ms.
    pub(crate) start_ms: u64,
    /// Its max buy / min sell price as written (`stopPx`).
    pub(crate) limit_px: Option<String>,
}

/// A fills line: its height and its TWAP suborders' fills.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TwapFillLine {
    pub(crate) height: u64,
    pub(crate) fills: Vec<TwapFill>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TwapFill {
    pub(crate) twap_id: u64,
    /// The trade's id: a fill's identity within its block (its TWAP's leg alone carries the `twapId`).
    pub(crate) tid: u64,
    pub(crate) sz: Sz,
}

#[derive(Deserialize)]
struct WireStatusLine {
    block_number: u64,
    events: Vec<WireStatusEvent>,
}

#[derive(Deserialize)]
struct WireStatusEvent {
    twap_id: u64,
    state: WireState,
    status: WireStatus,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireState {
    coin: String,
    user: Address,
    side: String,
    sz: String,
    minutes: u64,
    reduce_only: bool,
    randomize: bool,
    timestamp: u64,
    stop_px: Option<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum WireStatus {
    Text(String),
    Error {
        #[expect(dead_code, reason = "any error is a refused suborder")]
        error: String,
    },
}

#[derive(Deserialize)]
struct WireHeight {
    block_number: u64,
}

#[derive(Deserialize)]
struct WireFillLine {
    block_number: u64,
    events: Vec<(Address, WireFill)>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireFill {
    sz: String,
    tid: u64,
    twap_id: Option<u64>,
}

/// Starts the watcher of `<data_dir>/node_twap_statuses_streaming`.
pub(crate) fn watch_twap_statuses(data_dir: &Path) -> (Feed<Proven<TwapStatusLine>>, std::thread::JoinHandle<()>) {
    feed::watch_proven(stream("PendingTwapStatuses", "pending_twap_statuses", data_dir.join("node_twap_statuses_streaming")), QUEUE_LINES, parse_status_line)
}

/// Starts the pending thread's own watcher of `<data_dir>/node_fills_streaming`.
pub(crate) fn watch_twap_fills(data_dir: &Path) -> (Feed<Proven<TwapFillLine>>, std::thread::JoinHandle<()>) {
    feed::watch_proven(stream("PendingTwapFills", "pending_twap_fills", data_dir.join("node_fills_streaming")), QUEUE_LINES, parse_fill_line)
}

const fn stream(name: &'static str, lag_label: &'static str, dir: std::path::PathBuf) -> LineStream {
    LineStream {
        name,
        lag_label,
        dir,
        max_line_bytes: MAX_LINE_BYTES,
        start_skip_is_loss: true,
        layout: FileLayout::Hourly,
        shape: LineShape::Object,
        loss_counts: None,
    }
}

/// One TWAP statuses line; an `Err` says what does not read.
pub(crate) fn parse_status_line(line: &str) -> Result<TwapStatusLine, String> {
    let wire: WireStatusLine = sonic_rs::from_str(line).map_err(|err| format!("twap statuses line: {err}"))?;
    let height = wire.block_number;
    let events = wire
        .events
        .into_iter()
        .map(|event| event.read().map_err(|err| format!("block {height} twap {err}")))
        .collect::<Result<_, _>>()?;
    Ok(TwapStatusLine { height, events })
}

impl WireStatusEvent {
    fn read(self) -> Result<TwapEvent, String> {
        let twap_id = self.twap_id;
        let text = match self.status {
            WireStatus::Error { .. } => return Ok(TwapEvent::Refused { twap_id }),
            WireStatus::Text(text) => text,
        };
        match text.as_str() {
            "activated" => self.state.activation(twap_id).map(TwapEvent::Activated),
            "finished" | "terminated" => Ok(TwapEvent::Ended { twap_id }),
            "waitingForTrigger" => Ok(TwapEvent::Waiting { twap_id }),
            other => Err(format!("{twap_id}: status {other:?}")),
        }
    }
}

impl WireState {
    fn activation(self, twap_id: u64) -> Result<TwapActivation, String> {
        let is_buy = match self.side.as_str() {
            "B" => true,
            "A" => false,
            other => return Err(format!("{twap_id}: side {other:?}")),
        };
        let total = Sz::parse_from_str(&self.sz).map_err(|err| format!("{twap_id}: sz {:?}: {err}", self.sz))?;
        if self.minutes == 0 {
            return Err(format!("{twap_id}: 0 minutes"));
        }
        Ok(TwapActivation {
            twap_id,
            user: self.user,
            coin: Coin::new(&self.coin),
            is_buy,
            total,
            minutes: self.minutes,
            randomize: self.randomize,
            reduce_only: self.reduce_only,
            start_ms: self.timestamp,
            limit_px: self.stop_px,
        })
    }
}

/// One fills line, its TWAP fills alone read: the rest of a line without one is skipped unparsed.
pub(crate) fn parse_fill_line(line: &str) -> Result<TwapFillLine, String> {
    if !holds_twap_fill(line) {
        let wire: WireHeight = sonic_rs::from_str(line).map_err(|err| format!("fills line: {err}"))?;
        return Ok(TwapFillLine { height: wire.block_number, fills: Vec::new() });
    }
    let wire: WireFillLine = sonic_rs::from_str(line).map_err(|err| format!("fills line: {err}"))?;
    let height = wire.block_number;
    let mut fills = Vec::new();
    for (_, fill) in wire.events {
        let Some(twap_id) = fill.twap_id else { continue };
        let sz = Sz::parse_from_str(&fill.sz).map_err(|err| format!("block {height} tid {}: sz {:?}: {err}", fill.tid, fill.sz))?;
        fills.push(TwapFill { twap_id, tid: fill.tid, sz });
    }
    Ok(TwapFillLine { height, fills })
}

/// Whether a fills line names a TWAP: `"twapId":` and a digit (every other fill writes `null`).
fn holds_twap_fill(line: &str) -> bool {
    const KEY: &str = "\"twapId\":";
    line.match_indices(KEY).any(|(at, _)| line.as_bytes().get(at + KEY.len()).is_some_and(u8::is_ascii_digit))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// bm 11-01 06:59:54, block 1204902065: a DOGE TWAP activated; 06:59:56, its first suborder refused.
    pub(crate) const ACTIVATED: &str = r#"{"local_time":"2026-11-01T06:59:54.920288328","block_time":"2026-11-01T06:59:54.731709236","block_number":1204902065,"events":[{"time":"2026-11-01T06:59:54.731709236","twap_id":2479332,"state":{"coin":"DOGE","user":"0xbc95acb95441a0c224f5aad53edf94eab6f09266","side":"B","sz":"2000.0","executedSz":"0.0","executedNtl":"0.0","minutes":5,"reduceOnly":false,"randomize":false,"timestamp":1793516394731,"trigger":null,"stopPx":null},"status":"activated"}]}"#;
    const REFUSED: &str = r#"{"local_time":"2026-11-01T06:59:56.175647757","block_time":"2026-11-01T06:59:56.025676821","block_number":1204902083,"events":[{"time":"2026-11-01T06:59:56.025676821","twap_id":2479332,"state":{"coin":"DOGE","user":"0xbc95acb95441a0c224f5aad53edf94eab6f09266","side":"B","sz":"2000.0","executedSz":"0.0","executedNtl":"0.0","minutes":5,"reduceOnly":false,"randomize":false,"timestamp":1793516394731,"trigger":null,"stopPx":null},"status":{"error":"Insufficient margin to place order."}}]}"#;
    /// bm 11-01 06:00:00.032, block 1204851789: a BTC TWAP suborder's fill, and a fill of no TWAP.
    const FILL: &str = r#"{"local_time":"2026-11-01T06:00:00.500103439","block_time":"2026-11-01T06:00:00.032066810","block_number":1204851789,"events":[["0x856f7cb643fca326f475039702b10a9cbc2bd31a",{"coin":"BTC","px":"77889.0","sz":"0.00015","side":"B","time":1793512800032,"startPosition":"0.31756","dir":"Open Long","closedPnl":"0.0","hash":"0x0000000000000000000000000000000000000000000000000000000000000000","oid":591774083162,"crossed":true,"fee":"0.005047","tid":566368852384065,"feeToken":"USDC","twapId":2459667}]]}"#;

    #[test]
    fn a_statuses_line_reads_its_events() {
        let line = parse_status_line(ACTIVATED).unwrap();
        let TwapEvent::Activated(activation) = &line.events[0] else { panic!("{line:?}") };
        assert_eq!(line.height, 1_204_902_065);
        assert_eq!(
            (activation.twap_id, activation.coin.value(), activation.is_buy, activation.total, activation.minutes, activation.start_ms, activation.limit_px.as_deref()),
            (2_479_332, "DOGE".to_string(), true, Sz::parse_from_str("2000").unwrap(), 5, 1_793_516_394_731, None)
        );
        assert_eq!(parse_status_line(REFUSED).unwrap().events, [TwapEvent::Refused { twap_id: 2_479_332 }]);
        let waiting = ACTIVATED.replace(r#""activated""#, r#""waitingForTrigger""#);
        assert_eq!(parse_status_line(&waiting).unwrap().events, [TwapEvent::Waiting { twap_id: 2_479_332 }]);
        for ended in ["finished", "terminated"] {
            assert_eq!(parse_status_line(&ACTIVATED.replace(r#""activated""#, &format!("{ended:?}"))).unwrap().events, [TwapEvent::Ended { twap_id: 2_479_332 }]);
        }
        let stop = ACTIVATED.replace(r#""stopPx":null"#, r#""stopPx":"49.8""#);
        let TwapEvent::Activated(activation) = parse_status_line(&stop).unwrap().events.remove(0) else { panic!() };
        assert_eq!(activation.limit_px.as_deref(), Some("49.8"));
    }

    #[test]
    fn a_statuses_line_not_as_written_does_not_read() {
        assert!(parse_status_line(&ACTIVATED.replace(r#""activated""#, r#""waiting""#)).is_err());
        assert!(parse_status_line(&ACTIVATED.replace(r#""side":"B""#, r#""side":"X""#)).is_err());
        assert!(parse_status_line(&ACTIVATED.replace(r#""minutes":5"#, r#""minutes":0"#)).is_err());
        assert!(parse_status_line(&ACTIVATED.replace(r#""timestamp":1793516394731,"#, "")).is_err());
    }

    #[test]
    fn a_fills_line_reads_its_twap_fills_alone() {
        let line = parse_fill_line(FILL).unwrap();
        assert_eq!(line, TwapFillLine { height: 1_204_851_789, fills: vec![TwapFill { twap_id: 2_459_667, tid: 566_368_852_384_065, sz: Sz::parse_from_str("0.00015").unwrap() }] });
        let plain = FILL.replace(r#""twapId":2459667"#, r#""twapId":null"#);
        assert_eq!(parse_fill_line(&plain).unwrap(), TwapFillLine { height: 1_204_851_789, fills: Vec::new() });
        assert!(parse_fill_line(&FILL.replace(r#""block_number":1204851789,"#, "")).is_err());
        assert!(parse_fill_line(&plain.replace(r#""block_number":1204851789,"#, "")).is_err());
    }
}
