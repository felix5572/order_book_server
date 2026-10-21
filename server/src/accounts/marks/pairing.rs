//! Optional IOC pairing diagnostics. Known whole-batch rejections are counted only; unknown shapes retain a
//! bounded number of action/response excerpts, never the whole replica block. `NodeMarks` owns the seed quota.

use serde::Serialize;
use serde_json::json;

use super::actions::{MarkAction, MarkActions, Rank};
use crate::accounts::replica::ReplicaLine;

pub(crate) const MAX_PAIRING_SAMPLES: usize = 20;
const MAX_WIRE_BYTES: usize = 4 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IocPairingMismatch {
    BatchRejected,
    Unknown { requests: usize, responses: usize },
}

/// Transient ingestion diagnostics. Counts cover every affected action, even when samples are exhausted.
/// At most 20 bounded samples enter the input channel per block; the owner further caps them per seed interval.
#[derive(Debug, Default)]
pub(crate) struct PairingDiagnostics {
    pub(crate) batch_rejected: usize,
    pub(crate) unknown: usize,
    pub(crate) samples: Vec<PairingSample>,
}

impl PairingDiagnostics {
    pub(crate) fn collect(actions: &MarkActions, replica: &ReplicaLine<'_>, line: &str) -> Self {
        let mut out = Self::default();
        for action in &actions.actions {
            let MarkAction::Book(book) = &action.action else { continue };
            match book.ioc_pairing_mismatch {
                None => {}
                Some(IocPairingMismatch::BatchRejected) => out.batch_rejected += 1,
                Some(IocPairingMismatch::Unknown { requests, responses }) => {
                    out.unknown += 1;
                    if out.samples.len() < MAX_PAIRING_SAMPLES {
                        let wire = replica.raw_action_pair(line, action.rank.0, action.rank.2)
                            .map(|(hash, action, response)| WirePair {
                                bundle_hash: WireExcerpt::of(hash),
                                action: WireExcerpt::of(action.as_raw_str()),
                                response: WireExcerpt::of(response.as_raw_str()),
                            }).map_err(|error| WireExcerpt::of(&error));
                        out.samples.push(PairingSample { rank: action.rank, requests, responses, wire });
                    }
                }
            }
        }
        out
    }
}

#[derive(Debug)]
pub(crate) struct PairingSample {
    rank: Rank,
    requests: usize,
    responses: usize,
    /// A diagnostic capture failure is explicit evidence, not a mark-contract failure.
    wire: Result<WirePair, WireExcerpt>,
}

impl PairingSample {
    pub(crate) fn json(&self, height: u64) -> serde_json::Value {
        let mut out = json!({"kind": "nodeMarkIocPairing", "block": height, "rank": self.rank,
            "requests": self.requests, "responses": self.responses});
        match &self.wire {
            Ok(wire) => {
                out["bundleHash"] = json!(wire.bundle_hash.raw);
                out["bundleHashOriginalBytes"] = json!(wire.bundle_hash.original_bytes);
                out["bundleHashTruncated"] = json!(wire.bundle_hash.truncated);
                out["action"] = json!(wire.action);
                out["response"] = json!(wire.response);
            }
            Err(error) => out["captureError"] = json!(error),
        }
        out
    }
}

#[derive(Debug)]
struct WirePair {
    bundle_hash: WireExcerpt,
    action: WireExcerpt,
    response: WireExcerpt,
}

/// A byte-bounded UTF-8 prefix. A truncated excerpt is explicitly partial, not a complete JSON object.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WireExcerpt {
    raw: String,
    original_bytes: usize,
    truncated: bool,
}

impl WireExcerpt {
    fn of(raw: &str) -> Self {
        let mut end = raw.len().min(MAX_WIRE_BYTES);
        while !raw.is_char_boundary(end) { end -= 1; }
        Self { raw: raw[..end].to_string(), original_bytes: raw.len(), truncated: end < raw.len() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::marks::actions::tests::{ok, replica_line};

    const ORDER: &str = r#"{"a":41,"b":true,"p":"0.67331","s":"34.9","r":false,"t":{"limit":{"tif":"Ioc"}}}"#;

    fn capture(line: &str) -> PairingDiagnostics {
        let replica = ReplicaLine::parse(line).unwrap();
        let actions = MarkActions::of(&replica).unwrap();
        PairingDiagnostics::collect(&actions, &replica, line)
    }

    fn unknown_action() -> (String, String) {
        (format!(r#"{{"type":"order","orders":[{ORDER}]}}"#),
            ok(r#"{"type":"order","data":{"statuses":[{"error":"unknown shape"},{"resting":{"oid":99}}]}}"#))
    }

    #[test]
    fn known_batch_rejections_are_counted_without_capturing_any_wire() {
        let action = format!(r#"{{"type":"order","orders":[{ORDER},{ORDER}]}}"#);
        for error in ["Price must be divisible by tick size. asset=41", "Insufficient margin to place order. asset=41",
            "Order could not immediately match against any resting orders. asset=41", "future validation error"]
        {
            let response = ok(&json!({"type":"order","data":{"statuses":[{"error":error}]}}).to_string());
            let line = replica_line(55, &[&[(&action, &response)]]);
            let diagnostics = capture(&line);
            assert_eq!((diagnostics.batch_rejected, diagnostics.unknown, diagnostics.samples.len()), (1, 0, 0));
        }
        // A zero-request / one-error response is not the observed multi-order batch-rejection shape.
        let line = replica_line(55, &[&[(r#"{"type":"order","orders":[]}"#, &ok(r#"{"type":"order","data":{"statuses":[{"error":"unknown"}]}}"#))]]);
        assert_eq!(capture(&line).unknown, 1);
    }

    #[test]
    fn evidence_keeps_only_the_affected_action_and_its_response_even_with_reordered_bundles() {
        let (action, response) = unknown_action();
        let other = r#"{"type":"noop","privateTestMarker":"UNRELATED_BLOCK_PAYLOAD"}"#;
        let ordinary = ok(r#"{"type":"default"}"#);
        let line = replica_line(55, &[&[(other, &ordinary)], &[(&action, &response)]]);
        let mut wire: serde_json::Value = serde_json::from_str(&line).unwrap();
        wire["resps"]["Full"].as_array_mut().unwrap().reverse();
        let diagnostics = capture(&wire.to_string());
        assert_eq!((diagnostics.batch_rejected, diagnostics.unknown, diagnostics.samples.len()), (0, 1, 1));
        let evidence = diagnostics.samples[0].json(55);
        assert_eq!(evidence["bundleHash"], "0x01");
        assert_eq!(evidence["rank"], json!([1, 3, 0]));
        assert_eq!(serde_json::from_str::<serde_json::Value>(evidence["action"]["raw"].as_str().unwrap()).unwrap(), serde_json::from_str::<serde_json::Value>(&action).unwrap());
        assert_eq!(serde_json::from_str::<serde_json::Value>(evidence["response"]["raw"].as_str().unwrap()).unwrap(), serde_json::from_str::<serde_json::Value>(&response).unwrap());
        assert_eq!(evidence["action"]["truncated"], false);
        assert!(evidence.get("rawLine").is_none());
        assert!(!evidence.to_string().contains("UNRELATED_BLOCK_PAYLOAD"));
        // Capture problems must stay diagnostic: never manufacture a raw action or break mark reconstruction.
        let replica = ReplicaLine::parse(&line).unwrap();
        let actions = MarkActions::of(&replica).unwrap();
        let broken_capture = PairingDiagnostics::collect(&actions, &replica, "{}");
        assert!(broken_capture.samples[0].json(55).get("captureError").is_some());
    }

    #[test]
    fn oversized_wire_is_explicitly_truncated_and_ingress_sample_count_is_bounded() {
        let (mut action, mut response) = unknown_action();
        let mut a: serde_json::Value = serde_json::from_str(&action).unwrap();
        a["annotation"] = json!("测".repeat(10_000));
        action = a.to_string();
        let mut r: serde_json::Value = serde_json::from_str(&response).unwrap();
        r["annotation"] = json!("测".repeat(10_000));
        response = r.to_string();
        let pairs = vec![(action.as_str(), response.as_str()); 25];
        let diagnostics = capture(&replica_line(55, &[&pairs]));
        assert_eq!((diagnostics.unknown, diagnostics.samples.len()), (25, MAX_PAIRING_SAMPLES));
        for sample in &diagnostics.samples {
            let json = sample.json(55);
            for field in ["action", "response"] {
                assert!(json[field]["raw"].as_str().unwrap().len() <= MAX_WIRE_BYTES);
                assert_eq!(json[field]["truncated"], true);
                assert!(json[field]["originalBytes"].as_u64().unwrap() > MAX_WIRE_BYTES as u64);
            }
        }
        let total: usize = diagnostics.samples.iter().map(|sample| sample.json(55).to_string().len()).sum();
        assert!(total < 1_600_000, "bounded even after JSON escaping: {total}");
    }
}
