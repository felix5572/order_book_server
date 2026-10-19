//! The served JSON of `coinPositions` (design 000419 r2 4.1 / 4.2, with the r3/r4 field changes), built from book
//! views. One position shape for the HTTP response, the WS frames and the follow probe's dumps.

use alloy::primitives::Address;
use serde::Serialize;

use super::{
    book::{AnchorInfo, ChangeMarks, CoinChanges, CoinPositions, PositionView},
    checkpoint::AccountMode,
    liquidation::{LiquidationPx, UnsupportedReason},
    size::format_size_raw,
};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CoinPositionsDto {
    coin: String,
    fills_height: u64,
    misc_events_height: u64,
    anchor: AnchorDto,
    positions: Vec<PositionDto>,
}

#[derive(Debug, Serialize)]
struct AnchorDto {
    height: u64,
    /// Block time of the anchor, unix ms.
    time: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PositionDto {
    user: String,
    szi: String,
    /// The block that set `szi`.
    position_height: u64,
    /// The anchor's; `null` for a position opened after it.
    margin_mode: Option<&'static str>,
    /// The anchor's; `null` for an account that held no position at it.
    account_mode: Option<String>,
    pool_positions: Option<usize>,
    /// Only for `computed`.
    liquidation_px: Option<String>,
    liquidation_px_status: &'static str,
    unsupported_reason: Option<String>,
    /// The anchor height the row was computed at; `null` while `pending`.
    liquidation_px_height: Option<u64>,
    /// Observed sources that touched the account after the anchor; empty does not prove it unchanged.
    changed_since_liquidation_px: Vec<ChangeDto>,
}

#[derive(Debug, Serialize)]
struct ChangeDto {
    kind: &'static str,
    /// The first block of this kind after the anchor.
    height: u64,
}

impl CoinPositionsDto {
    pub(crate) fn new(view: &CoinPositions) -> Self {
        Self {
            coin: view.coin.clone(),
            fills_height: view.fills_height,
            misc_events_height: view.misc_events_height,
            anchor: AnchorDto { height: view.anchor.height, time: view.anchor.time_ms },
            positions: view
                .positions
                .iter()
                .map(|position| position_dto(view.size_decimals, view.anchor, position))
                .collect(),
        }
    }
}

impl CoinPositionsDto {
    /// The HTTP body.
    #[expect(clippy::expect_used, reason = "these DTOs have string keys and finite numbers only; serializing cannot fail")]
    pub(crate) fn to_json(&self) -> String {
        serde_json::to_string(self).expect("serializable DTOs")
    }
}

/// One WS frame's `data` (design 000419 r2 4.2).
#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub(crate) enum CoinPositionsFrame {
    /// The full view: on subscribe (current version) and after every anchor install (version bumped).
    Snapshot {
        version: u64,
        #[serde(flatten)]
        view: CoinPositionsDto,
    },
    /// The rows a batch touched; `position` is `null` once closed.
    Changes { coin: String, version: u64, fills_height: u64, misc_events_height: u64, changes: Vec<ChangeRowDto> },
    /// The subscription ended; resubscribe once the view is available again.
    Withdrawn { coin: String, reason: String },
}

#[derive(Debug, Serialize)]
pub(crate) struct ChangeRowDto {
    user: String,
    position: Option<PositionDto>,
}

impl CoinPositionsFrame {
    pub(crate) fn snapshot(view: &CoinPositions) -> Self {
        Self::Snapshot { version: view.version, view: CoinPositionsDto::new(view) }
    }

    pub(crate) fn changes(changes: &CoinChanges) -> Self {
        let rows = changes.rows.iter().map(|(user, position)| ChangeRowDto {
            user: user_text(*user),
            position: position.as_ref().map(|position| position_dto(changes.size_decimals, changes.anchor, position)),
        });
        Self::Changes {
            coin: changes.coin.clone(),
            version: changes.version,
            fills_height: changes.fills_height,
            misc_events_height: changes.misc_events_height,
            changes: rows.collect(),
        }
    }

    /// The wire frame `{"channel":"coinPositions","data":<frame>}`, the envelope every `ServerResponse` has.
    #[expect(clippy::expect_used, reason = "these DTOs have string keys and finite numbers only; serializing cannot fail")]
    pub(crate) fn to_wire(&self) -> bytes::Bytes {
        #[derive(Serialize)]
        struct Envelope<'a> {
            channel: &'static str,
            data: &'a CoinPositionsFrame,
        }
        let wire = serde_json::to_vec(&Envelope { channel: "coinPositions", data: self }).expect("serializable DTOs");
        bytes::Bytes::from(wire)
    }
}

fn position_dto(size_decimals: u32, anchor: AnchorInfo, position: &PositionView) -> PositionDto {
    let row = position.anchor_row.as_ref();
    let (liquidation_px_status, liquidation_px, unsupported_reason) = match row.map(|row| &row.liquidation_px) {
        None => ("pending", None, None),
        Some(LiquidationPx::Computed(price)) => ("computed", Some(price.to_string()), None),
        Some(LiquidationPx::NoPositiveSolution) => ("none", None, None),
        Some(LiquidationPx::Unsupported(reason)) => ("unsupported", None, Some(unsupported_reason_name(reason))),
    };
    PositionDto {
        user: user_text(position.user),
        szi: format_size_raw(position.size_raw, size_decimals),
        position_height: position.position_height,
        margin_mode: row.map(|row| if row.isolated { "isolated" } else { "cross" }),
        account_mode: position.account_mode.as_ref().map(account_mode_name),
        pool_positions: row.and_then(|row| row.pool_positions),
        liquidation_px,
        liquidation_px_status,
        unsupported_reason,
        liquidation_px_height: row.map(|_| anchor.height),
        changed_since_liquidation_px: changes(position.changes),
    }
}

fn changes(marks: ChangeMarks) -> Vec<ChangeDto> {
    [("fill", marks.fill), ("ledger", marks.ledger), ("funding", marks.funding)]
        .into_iter()
        .filter_map(|(kind, height)| height.map(|height| ChangeDto { kind, height }))
        .collect()
}

pub(crate) fn user_text(user: Address) -> String {
    user.to_string().to_lowercase()
}

/// The official `userAbstraction` names.
pub(crate) fn account_mode_name(mode: &AccountMode) -> String {
    match mode {
        AccountMode::Default => "default".to_string(),
        AccountMode::Disabled => "disabled".to_string(),
        AccountMode::Unified => "unifiedAccount".to_string(),
        AccountMode::PortfolioMargin => "portfolioMargin".to_string(),
        AccountMode::Unknown(code) => format!("unknown:{code}"),
    }
}

pub(crate) fn unsupported_reason_name(reason: &UnsupportedReason) -> String {
    match reason {
        UnsupportedReason::PortfolioMargin => "portfolioMargin".to_string(),
        UnsupportedReason::UnknownAccountMode(code) => format!("unknownAccountMode:{code}"),
        UnsupportedReason::MissingMark => "missingMark".to_string(),
        UnsupportedReason::TierCase => "tierCase".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::accounts::{anchor::AnchorRow, book::AnchorInfo};

    #[test]
    fn serializes_anchored_and_pending_rows() {
        let anchored = PositionView {
            user: Address::repeat_byte(0xab),
            size_raw: -2_696_200,
            position_height: 1_000,
            account_mode: Some(AccountMode::Unified),
            anchor_row: Some(AnchorRow {
                isolated: false,
                pool_positions: Some(2),
                liquidation_px: LiquidationPx::Computed(0.003_951_5),
            }),
            changes: ChangeMarks { fill: None, ledger: Some(1_005), funding: Some(1_003) },
        };
        let pending = PositionView {
            user: Address::repeat_byte(0x01),
            size_raw: 5,
            position_height: 1_010,
            account_mode: None,
            anchor_row: None,
            changes: ChangeMarks { fill: Some(1_010), ..ChangeMarks::default() },
        };
        let unsupported = PositionView {
            anchor_row: Some(AnchorRow {
                isolated: true,
                pool_positions: None,
                liquidation_px: LiquidationPx::Unsupported(UnsupportedReason::UnknownAccountMode("x".to_string())),
            }),
            account_mode: Some(AccountMode::Unknown("x".to_string())),
            changes: ChangeMarks::default(),
            ..anchored
        };
        let view = CoinPositions {
            coin: "PUMP".to_string(),
            version: 7,
            anchor: AnchorInfo { height: 1_000, time_ms: 1_792_384_440_000 },
            fills_height: 1_011,
            misc_events_height: 1_009,
            size_decimals: 1,
            positions: vec![anchored, pending, unsupported],
        };
        let value = serde_json::to_value(CoinPositionsDto::new(&view)).unwrap();
        let ab = "0xabababababababababababababababababababab";
        assert_eq!(
            value,
            json!({
                "coin": "PUMP", "fillsHeight": 1_011, "miscEventsHeight": 1_009,
                "anchor": {"height": 1_000, "time": 1_792_384_440_000u64},
                "positions": [
                    {"user": ab, "szi": "-269620.0", "positionHeight": 1_000, "marginMode": "cross",
                     "accountMode": "unifiedAccount", "poolPositions": 2, "liquidationPx": "0.0039515",
                     "liquidationPxStatus": "computed", "unsupportedReason": null, "liquidationPxHeight": 1_000,
                     "changedSinceLiquidationPx": [{"kind": "ledger", "height": 1_005}, {"kind": "funding", "height": 1_003}]},
                    {"user": "0x0101010101010101010101010101010101010101", "szi": "0.5", "positionHeight": 1_010,
                     "marginMode": null, "accountMode": null, "poolPositions": null, "liquidationPx": null,
                     "liquidationPxStatus": "pending", "unsupportedReason": null, "liquidationPxHeight": null,
                     "changedSinceLiquidationPx": [{"kind": "fill", "height": 1_010}]},
                    {"user": ab, "szi": "-269620.0", "positionHeight": 1_000, "marginMode": "isolated",
                     "accountMode": "unknown:x", "poolPositions": null, "liquidationPx": null,
                     "liquidationPxStatus": "unsupported", "unsupportedReason": "unknownAccountMode:x",
                     "liquidationPxHeight": 1_000, "changedSinceLiquidationPx": []}
                ]
            })
        );
    }
}
