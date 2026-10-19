//! Rare events kept as first-hand evidence. Backstop liquidation and ADL happen only in extreme markets, so their raw
//! data must already be on record the day one happens: each piece is one `ACCOUNTS_EVIDENCE <json>` log line at WARN,
//! counted by kind in `orderbook_accounts_evidence_total`. Line evidence carries the raw node line it came from.
//!
//! "Ordinary" below is what one full hour of 2026-10-19 contained; anything else, and every non-market liquidation or
//! `liquidation` ledger record (the documented backstop record), is evidence.

use serde_json::json;

use super::{
    book::SilentChange,
    misc::MiscEvent,
    size::format_size_raw,
    view::user_text,
};
use crate::types::node_data::{Batch, NodeDataFill};

/// Fill directions of 2026-10-19 06:00-07:00 UTC (perps, spot, HIP-4 outcomes; 427,910 fills).
const ORDINARY_FILL_DIRS: [&str; 13] = [
    "Open Long",
    "Open Short",
    "Close Long",
    "Close Short",
    "Long > Short",
    "Short > Long",
    "Buy",
    "Sell",
    "Settlement",
    "Merge Outcome",
    "Negate Outcome",
    "Merge Question",
    "Split Outcome",
];
/// The only liquidation method of 752 liquidation fills (2026-10-16..19).
const MARKET_LIQUIDATION: &str = "market";
/// `LedgerUpdate` delta types of 2026-10-19 06:00-07:00 UTC (3,298 updates). `liquidation` is never ordinary.
const ORDINARY_LEDGER_TYPES: [&str; 15] = [
    "accountClassTransfer",
    "borrowLend",
    "spotTransfer",
    "rewardsClaim",
    "send",
    "internalTransfer",
    "accountActivationGas",
    "subAccountTransfer",
    "deposit",
    "cStakingTransfer",
    "vaultWithdraw",
    "gossipPriorityGasAuction",
    "withdraw",
    "vaultDeposit",
    "vaultLeaderCommission",
];

/// Why a fills line is evidence, if it is.
pub(crate) fn fill_line_reason(batch: &Batch<NodeDataFill>) -> Option<String> {
    let reasons: Vec<String> = batch
        .events_ref()
        .iter()
        .filter_map(|NodeDataFill(_, fill)| {
            if let Some(liquidation) = &fill.liquidation
                && liquidation.method != MARKET_LIQUIDATION
            {
                return Some(format!("liquidation method {:?}", liquidation.method));
            }
            (!ORDINARY_FILL_DIRS.contains(&fill.dir.as_str())).then(|| format!("fill dir {:?}", fill.dir))
        })
        .collect();
    (!reasons.is_empty()).then(|| reasons.join("; "))
}

/// Why a misc events line is evidence, if it is.
pub(crate) fn misc_line_reason(batch: &Batch<MiscEvent>) -> Option<String> {
    let reasons: Vec<String> = batch
        .events_ref()
        .iter()
        .filter_map(|event| match event {
            MiscEvent::Ledger { delta_type, .. } if !ORDINARY_LEDGER_TYPES.contains(&delta_type.as_str()) => {
                Some(format!("ledger type {delta_type:?}"))
            }
            MiscEvent::Unknown(kind) => Some(format!("misc kind {kind:?}")),
            MiscEvent::Ledger { .. } | MiscEvent::Funding(_) | MiscEvent::Irrelevant => None,
        })
        .collect();
    (!reasons.is_empty()).then(|| reasons.join("; "))
}

/// A node line kept whole because of `reason`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LineEvidence {
    pub(crate) reason: String,
    pub(crate) raw_line: String,
}

impl LineEvidence {
    pub(crate) fn json(&self, stream: &str, block: u64) -> serde_json::Value {
        json!({"kind": "rareLine", "stream": stream, "block": block, "reason": self.reason, "rawLine": self.raw_line})
    }
}

/// A position an anchor contradicts: changed, opened or closed without an observed fill.
pub(crate) fn silent_change_json(change: &SilentChange, anchor_from: Option<u64>, anchor_to: u64) -> serde_json::Value {
    let szi = |size_raw: i64| format_size_raw(size_raw, change.size_decimals);
    json!({
        "kind": "silentPositionChange",
        "anchorFrom": anchor_from,
        "anchorTo": anchor_to,
        "coin": change.coin,
        "user": user_text(change.user),
        "projectedSzi": change.projected.map(|(size_raw, _)| szi(size_raw)),
        "projectedHeight": change.projected.map(|(_, height)| height),
        "anchorSzi": szi(change.anchor_size_raw),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::test_support::{PUMP, fills_line, misc, user};

    fn fills_batch(line: &str) -> Batch<NodeDataFill> {
        sonic_rs::from_str(line).unwrap()
    }

    #[test]
    fn ordinary_fills_are_no_evidence_and_rare_ones_say_why() {
        let ordinary = fills_line(1_100, &[(user(1), PUMP, "B", 1, "0.0", "1.0")]);
        assert_eq!(fill_line_reason(&fills_batch(&ordinary)), None);
        let adl = ordinary.replace("\"Open Long\"", "\"Auto-Deleveraging\"");
        assert_eq!(fill_line_reason(&fills_batch(&adl)).as_deref(), Some("fill dir \"Auto-Deleveraging\""));
        let liquidation = r#""liquidation":{"liquidatedUser":"0x01","markPx":"1.0","method":"#;
        let market = ordinary.replace(r#""feeToken":"USDC""#, &format!(r#""feeToken":"USDC",{liquidation}"market"}}"#));
        assert_eq!(fill_line_reason(&fills_batch(&market)), None, "a market liquidation is ordinary");
        let backstop = ordinary.replace(r#""feeToken":"USDC""#, &format!(r#""feeToken":"USDC",{liquidation}"backstop"}}"#));
        assert_eq!(fill_line_reason(&fills_batch(&backstop)).as_deref(), Some("liquidation method \"backstop\""));
    }

    #[test]
    fn rare_ledger_types_and_unknown_kinds_are_evidence() {
        let ledger = |kind: &str| format!(r#"{{"LedgerUpdate":{{"users":["{:#x}"],"delta":{{"type":"{kind}"}}}}}}"#, user(1));
        assert_eq!(misc_line_reason(&misc(1_100, &[&ledger("send")])), None);
        assert_eq!(misc_line_reason(&misc(1_100, &[&ledger("liquidation")])).as_deref(), Some("ledger type \"liquidation\""));
        assert_eq!(misc_line_reason(&misc(1_100, &[r#"{"NewKind":{}}"#])).as_deref(), Some("misc kind \"NewKind\""));
        assert_eq!(misc_line_reason(&misc(1_100, &[r#"{"Funding":{"deltas":[]}}"#])), None);
    }

    #[test]
    fn a_silent_change_names_both_anchors_and_both_sizes() {
        let change = SilentChange {
            coin: PUMP.to_string(),
            user: user(1),
            size_decimals: 1,
            projected: Some((55, 1_100)),
            anchor_size_raw: 0,
        };
        assert_eq!(
            silent_change_json(&change, Some(1_000), 1_200),
            json!({"kind": "silentPositionChange", "anchorFrom": 1_000, "anchorTo": 1_200, "coin": PUMP,
                   "user": format!("{:#x}", user(1)), "projectedSzi": "5.5", "projectedHeight": 1_100, "anchorSzi": "0.0"})
        );
    }
}
