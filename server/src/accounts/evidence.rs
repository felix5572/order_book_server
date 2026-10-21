//! Rare events kept as first-hand evidence. Backstop liquidation and ADL happen only in extreme markets, so their raw
//! data must already be on record the day one happens: each piece is one `ACCOUNTS_EVIDENCE <json>` log line at WARN,
//! counted by kind in `orderbook_accounts_evidence_total`. Line evidence carries the raw node line it came from.
//!
//! "Ordinary" below is what one full hour of 2026-10-19 contained; anything else, and every non-market liquidation or
//! `liquidation` ledger record (the documented backstop record), is evidence. A non-market liquidation names its `dir`
//! too: ADL fills carry method `backstop` with dir `Auto-Deleveraging`, and the liquidated side of the same trade the
//! same method with dir `Liquidated Cross …` (official fills of 2025-10-10, Codex handoff `adl_replay_20261020`).

use serde_json::json;

use super::{
    book::{RowMismatch, SilentChange},
    liquidation::LiquidationPx,
    marks::{
        AmbiguousCut, Hip3Mismatch,
        book::BookMismatch,
        native::PriceMismatch,
        seed::{RestingOrder, StoredMark},
    },
    misc::MiscEvent,
    replay::Mismatch,
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
                return Some(format!("liquidation method {:?} dir {:?}", liquidation.method, fill.dir));
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
            MiscEvent::Ledger { delta, .. } if !ORDINARY_LEDGER_TYPES.contains(&delta.delta_type()) => {
                Some(format!("ledger type {:?}", delta.delta_type()))
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

/// A replayed account the anchor contradicts (design 000433): both states, 10^-8 USDC amounts as decimal text.
pub(crate) fn replay_mismatch_json(mismatch: &Mismatch, anchor_from: Option<u64>, anchor_to: u64) -> serde_json::Value {
    json!({
        "kind": "replayMismatch",
        "anchorFrom": anchor_from,
        "anchorTo": anchor_to,
        "user": user_text(mismatch.user),
        "replayed": mismatch.replayed.json(),
        "anchor": mismatch.anchor.json(),
    })
}

/// A replayed row the anchor's own row contradicts (design 000437).
pub(crate) fn replay_row_mismatch_json(mismatch: &RowMismatch, anchor_from: Option<u64>, anchor_to: u64) -> serde_json::Value {
    let price = |price: &LiquidationPx| format!("{price:?}");
    json!({
        "kind": "replayRowMismatch",
        "anchorFrom": anchor_from,
        "anchorTo": anchor_to,
        "coin": mismatch.coin,
        "user": user_text(mismatch.user),
        "replayed": mismatch.replayed.as_ref().map(|row| json!({"isolated": row.isolated, "poolPositions": row.pool_positions,
                                                                "liquidationPx": price(&row.liquidation_px)})),
        "anchor": json!({"isolated": mismatch.anchor.isolated, "poolPositions": mismatch.anchor.pool_positions,
                          "liquidationPx": price(&mismatch.anchor.liquidation_px)}),
    })
}

/// A price action of block `height` whose cut is not unique (review 000444 r1 P3): it read the book after `lo` lines.
pub(crate) fn ambiguous_cut_json(height: u64, cut: &AmbiguousCut) -> serde_json::Value {
    let (bundle, category, position) = cut.rank;
    json!({"kind": "nodeMarkAmbiguousCut", "height": height, "rank": [bundle, category, position], "lo": cut.lo, "hi": cut.hi})
}

/// A HIP-3 coin whose mark in the marks' snapshot differs from the checkpoint's (design 000439 r2 revision 2).
pub(crate) fn hip3_mark_mismatch_json(mismatch: &Hip3Mismatch, seed_from: Option<u64>, seed_to: u64) -> serde_json::Value {
    let mark = |mark: Option<StoredMark>| mark.map(|mark| json!({"px": mark.px, "timeNs": mark.time_ns}));
    json!({
        "kind": "nodeMarkMismatch",
        "market": "hip3",
        "seedFrom": seed_from,
        "seedTo": seed_to,
        "coin": mismatch.coin,
        "replayed": mark(mismatch.replayed),
        "checkpoint": mark(mismatch.checkpoint),
    })
}

/// A main-dex price field where the marks' snapshot and the checkpoint differ (design 000439 r2 batch 2), with both
/// values.
pub(crate) fn price_mismatch_json(mismatch: &PriceMismatch, seed_from: Option<u64>, seed_to: u64) -> serde_json::Value {
    json!({
        "kind": "nodePriceMismatch",
        "seedFrom": seed_from,
        "seedTo": seed_to,
        "coin": mismatch.coin,
        "field": mismatch.field,
        "replayed": mismatch.replayed,
        "checkpoint": mismatch.checkpoint,
    })
}

/// A main-dex resting order where the marks' snapshot and the checkpoint differ (design 000439 r2 batch 2).
pub(crate) fn book_mismatch_json(mismatch: &BookMismatch, seed_from: Option<u64>, seed_to: u64) -> serde_json::Value {
    let order = |order: Option<RestingOrder>| {
        order.map(|order| json!({"side": order.side, "limitPx": order.limit_px, "remaining": order.remaining}))
    };
    json!({
        "kind": "nodeBookMismatch",
        "seedFrom": seed_from,
        "seedTo": seed_to,
        "coin": mismatch.coin,
        "oid": mismatch.oid,
        "replayed": order(mismatch.replayed),
        "checkpoint": order(mismatch.checkpoint),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::test_support::{
        PUMP, SOL_ADL_BLOCK, SOL_ADL_LONG, SOL_LIQUIDATED_SHORT, fills_line, misc, real_fills_line, send_inner,
        unread_ledger_inner, user,
    };

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
        assert_eq!(
            fill_line_reason(&fills_batch(&backstop)).as_deref(),
            Some("liquidation method \"backstop\" dir \"Open Long\"")
        );
    }

    #[test]
    fn a_real_adl_line_names_the_method_and_both_dirs() {
        let line = real_fills_line(SOL_ADL_BLOCK, &[SOL_ADL_LONG, SOL_LIQUIDATED_SHORT]);
        assert_eq!(
            fill_line_reason(&fills_batch(&line)).as_deref(),
            Some(
                "liquidation method \"backstop\" dir \"Auto-Deleveraging\"; \
                 liquidation method \"backstop\" dir \"Liquidated Cross Short\""
            )
        );
    }

    #[test]
    fn rare_ledger_types_and_unknown_kinds_are_evidence() {
        let send = send_inner((user(1), ""), (user(2), ""), "1.0");
        assert_eq!(misc_line_reason(&misc(1_100, &[&send])), None);
        let liquidation = unread_ledger_inner(user(1), "liquidation");
        assert_eq!(misc_line_reason(&misc(1_100, &[&liquidation])).as_deref(), Some("ledger type \"liquidation\""));
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
