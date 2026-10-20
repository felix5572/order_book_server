//! The served JSON of `coinPositions` (design 000419 r2 4.1 / 4.2, with the r3/r4 field changes), built from book
//! views. One position shape for the HTTP response, the WS frames and the follow probe's dumps.

use alloy::primitives::Address;
use serde::Serialize;

use super::{
    book::{AnchorInfo, ChangeMarks, CoinChanges, CoinPositions, PositionView},
    checkpoint::AccountMode,
    liquidation::{LiquidationPx, UnsupportedReason},
    portfolio::PortfolioMargin,
    size::format_size_raw,
};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CoinPositionsDto {
    coin: String,
    fills_height: u64,
    misc_events_height: u64,
    block_actions_height: u64,
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
    /// The replay's or the anchor's; `null` while `pending`.
    margin_mode: Option<&'static str>,
    /// The anchor's; `null` for an account that held no position at it.
    account_mode: Option<String>,
    pool_positions: Option<usize>,
    /// Only for `computed`.
    liquidation_px: Option<String>,
    liquidation_px_status: &'static str,
    unsupported_reason: Option<String>,
    /// `replay`: computed from the replayed state of block `liquidationPxHeight` (design 000437; an isolated position
    /// or a cross position alone in its pool); `anchor`: the anchor's row; `null` while `pending`.
    liquidation_px_source: Option<&'static str>,
    /// The block the row's state is of: the applied height for a replayed row, the anchor height for an anchor's row;
    /// `null` while `pending`.
    liquidation_px_height: Option<u64>,
    /// Observed sources that touched the account after the anchor; empty does not prove it unchanged (mark price moves
    /// of the pool's other positions are not a source). Always empty on a replayed row: every event up to its height
    /// is in it.
    changed_since_liquidation_px: Vec<ChangeDto>,
    /// Why the account's replay stopped (its rows then keep the anchor's values); `null` while it replays.
    replay_stopped_reason: Option<String>,
    /// The block it stopped at; `null` when it stopped at the anchor (portfolio margin, an unknown mode) or replays.
    replay_stopped_height: Option<u64>,
    /// The anchor's portfolio-margin ratio of a portfolio-margin account (node `portfolioMarginRatio`, not the official
    /// 0.95 trigger); `null` for the other modes and for pending rows.
    portfolio_margin: Option<PortfolioMarginDto>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub(crate) enum PortfolioMarginDto {
    Computed { ratio: String },
    Unsupported { reason: String },
}

impl PortfolioMarginDto {
    pub(crate) fn new(portfolio_margin: &PortfolioMargin) -> Self {
        match portfolio_margin {
            PortfolioMargin::Computed { ratio } => Self::Computed { ratio: ratio.to_string() },
            PortfolioMargin::Unsupported(reason) => Self::Unsupported { reason: unsupported_reason_name(reason) },
        }
    }
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
            block_actions_height: view.block_actions_height,
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
    Changes {
        coin: String,
        version: u64,
        fills_height: u64,
        misc_events_height: u64,
        block_actions_height: u64,
        changes: Vec<ChangeRowDto>,
    },
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
            block_actions_height: changes.block_actions_height,
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

/// What a row shows: the replayed row, else the anchor's, else nothing (`pending`).
struct Shown<'a> {
    source: &'static str,
    height: u64,
    isolated: bool,
    pool_positions: Option<usize>,
    liquidation_px: &'a LiquidationPx,
}

fn position_dto(size_decimals: u32, anchor: AnchorInfo, position: &PositionView) -> PositionDto {
    let row = position.anchor_row.as_ref();
    let shown = match (&position.replayed, row) {
        (Some((height, replayed)), _) => Some(Shown {
            source: "replay",
            height: *height,
            isolated: replayed.isolated,
            pool_positions: Some(1),
            liquidation_px: &replayed.liquidation_px,
        }),
        (None, Some(row)) => Some(Shown {
            source: "anchor",
            height: anchor.height,
            isolated: row.isolated,
            pool_positions: row.pool_positions,
            liquidation_px: &row.liquidation_px,
        }),
        (None, None) => None,
    };
    let (liquidation_px_status, liquidation_px, unsupported_reason) = match shown.as_ref().map(|shown| shown.liquidation_px) {
        None => ("pending", None, None),
        Some(LiquidationPx::Computed(price)) => ("computed", Some(price.to_string()), None),
        Some(LiquidationPx::NoPositiveSolution) => ("none", None, None),
        Some(LiquidationPx::Unsupported(reason)) => ("unsupported", None, Some(unsupported_reason_name(reason))),
    };
    let replayed = position.replayed.is_some();
    PositionDto {
        user: user_text(position.user),
        szi: format_size_raw(position.size_raw, size_decimals),
        position_height: position.position_height,
        margin_mode: shown.as_ref().map(|shown| if shown.isolated { "isolated" } else { "cross" }),
        account_mode: position.account_mode.as_ref().map(account_mode_name),
        pool_positions: shown.as_ref().and_then(|shown| shown.pool_positions),
        liquidation_px,
        liquidation_px_status,
        unsupported_reason,
        liquidation_px_source: shown.as_ref().map(|shown| shown.source),
        liquidation_px_height: shown.as_ref().map(|shown| shown.height),
        changed_since_liquidation_px: if replayed { Vec::new() } else { changes(position.changes) },
        replay_stopped_reason: position.replay_stop.as_ref().map(|stop| stop.reason.text()),
        replay_stopped_height: position.replay_stop.as_ref().and_then(|stop| stop.block),
        portfolio_margin: row.and_then(|row| row.portfolio_margin.as_ref()).filter(|_| !replayed).map(PortfolioMarginDto::new),
    }
}

fn changes(marks: ChangeMarks) -> Vec<ChangeDto> {
    let ChangeMarks { fill, ledger, funding, margin, account_mode, withdrawal } = marks;
    let kinds = [
        ("fill", fill),
        ("ledger", ledger),
        ("funding", funding),
        ("margin", margin),
        ("accountMode", account_mode),
        ("withdrawal", withdrawal),
    ];
    kinds
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
        UnsupportedReason::UnknownAccountMode(code) => format!("unknownAccountMode:{code}"),
        UnsupportedReason::PortfolioMarginNonUsdcDex => "portfolioMarginNonUsdcDex".to_string(),
        UnsupportedReason::PortfolioMarginSupplyCap => "portfolioMarginSupplyCap".to_string(),
        UnsupportedReason::PortfolioMarginBorrowCap => "portfolioMarginBorrowCap".to_string(),
        UnsupportedReason::MissingMark => "missingMark".to_string(),
        UnsupportedReason::TierCase => "tierCase".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::accounts::{
        anchor::AnchorRow,
        book::AnchorInfo,
        replay::{ReplayedRow, Stop, StopReason},
    };

    #[test]
    fn an_unsupported_portfolio_margin_names_its_reason() {
        let dto = PortfolioMarginDto::new(&PortfolioMargin::Unsupported(UnsupportedReason::PortfolioMarginSupplyCap));
        assert_eq!(serde_json::to_value(dto).unwrap(), json!({"status": "unsupported", "reason": "portfolioMarginSupplyCap"}));
    }

    #[test]
    fn serializes_anchored_and_pending_rows() {
        let anchored = PositionView {
            user: Address::repeat_byte(0xab),
            size_raw: -2_696_200,
            position_height: 1_000,
            account_mode: Some(AccountMode::PortfolioMargin),
            anchor_row: Some(AnchorRow {
                isolated: false,
                pool_positions: Some(2),
                liquidation_px: LiquidationPx::Computed(0.003_951_5),
                portfolio_margin: Some(PortfolioMargin::Computed { ratio: 0.25 }),
            }),
            changes: ChangeMarks {
                ledger: Some(1_005),
                funding: Some(1_003),
                margin: Some(1_002),
                account_mode: Some(1_004),
                withdrawal: Some(1_001),
                ..ChangeMarks::default()
            },
            replayed: None,
            replay_stop: Some(Stop { block: None, reason: StopReason::PortfolioMargin }),
        };
        let pending = PositionView {
            user: Address::repeat_byte(0x01),
            size_raw: 5,
            position_height: 1_010,
            account_mode: None,
            anchor_row: None,
            changes: ChangeMarks { fill: Some(1_010), ..ChangeMarks::default() },
            replayed: None,
            replay_stop: None,
        };
        let unsupported = PositionView {
            anchor_row: Some(AnchorRow {
                isolated: true,
                pool_positions: None,
                liquidation_px: LiquidationPx::Unsupported(UnsupportedReason::UnknownAccountMode("x".to_string())),
                portfolio_margin: None,
            }),
            account_mode: Some(AccountMode::Unknown("x".to_string())),
            changes: ChangeMarks::default(),
            replay_stop: Some(Stop { block: Some(1_007), reason: StopReason::UnreadLedger("spotGenesis".to_string()) }),
            ..anchored.clone()
        };
        // a cross position alone in its pool, replayed to block 1012: its row replaces the anchor's
        let replayed = PositionView {
            account_mode: Some(AccountMode::Unified),
            replayed: Some((1_012, ReplayedRow { isolated: false, liquidation_px: LiquidationPx::Computed(0.004_2) })),
            replay_stop: None,
            ..anchored.clone()
        };
        let view = CoinPositions {
            coin: "PUMP".to_string(),
            version: 7,
            anchor: AnchorInfo { height: 1_000, time_ms: 1_792_384_440_000 },
            fills_height: 1_011,
            misc_events_height: 1_009,
            block_actions_height: 1_012,
            size_decimals: 1,
            positions: vec![anchored, pending, unsupported, replayed],
        };
        let value = serde_json::to_value(CoinPositionsDto::new(&view)).unwrap();
        let ab = "0xabababababababababababababababababababab";
        assert_eq!(
            value,
            json!({
                "coin": "PUMP", "fillsHeight": 1_011, "miscEventsHeight": 1_009, "blockActionsHeight": 1_012,
                "anchor": {"height": 1_000, "time": 1_792_384_440_000u64},
                "positions": [
                    {"user": ab, "szi": "-269620.0", "positionHeight": 1_000, "marginMode": "cross",
                     "accountMode": "portfolioMargin", "poolPositions": 2, "liquidationPx": "0.0039515",
                     "liquidationPxStatus": "computed", "unsupportedReason": null, "liquidationPxSource": "anchor",
                     "liquidationPxHeight": 1_000,
                     "changedSinceLiquidationPx": [
                         {"kind": "ledger", "height": 1_005}, {"kind": "funding", "height": 1_003},
                         {"kind": "margin", "height": 1_002}, {"kind": "accountMode", "height": 1_004},
                         {"kind": "withdrawal", "height": 1_001}
                     ],
                     "replayStoppedReason": "portfolioMargin", "replayStoppedHeight": null,
                     "portfolioMargin": {"status": "computed", "ratio": "0.25"}},
                    {"user": "0x0101010101010101010101010101010101010101", "szi": "0.5", "positionHeight": 1_010,
                     "marginMode": null, "accountMode": null, "poolPositions": null, "liquidationPx": null,
                     "liquidationPxStatus": "pending", "unsupportedReason": null, "liquidationPxSource": null,
                     "liquidationPxHeight": null, "changedSinceLiquidationPx": [{"kind": "fill", "height": 1_010}],
                     "replayStoppedReason": null, "replayStoppedHeight": null, "portfolioMargin": null},
                    {"user": ab, "szi": "-269620.0", "positionHeight": 1_000, "marginMode": "isolated",
                     "accountMode": "unknown:x", "poolPositions": null, "liquidationPx": null,
                     "liquidationPxStatus": "unsupported", "unsupportedReason": "unknownAccountMode:x",
                     "liquidationPxSource": "anchor", "liquidationPxHeight": 1_000, "changedSinceLiquidationPx": [],
                     "replayStoppedReason": "unreadLedger:spotGenesis", "replayStoppedHeight": 1_007, "portfolioMargin": null},
                    {"user": ab, "szi": "-269620.0", "positionHeight": 1_000, "marginMode": "cross",
                     "accountMode": "unifiedAccount", "poolPositions": 1, "liquidationPx": "0.0042",
                     "liquidationPxStatus": "computed", "unsupportedReason": null, "liquidationPxSource": "replay",
                     "liquidationPxHeight": 1_012, "changedSinceLiquidationPx": [],
                     "replayStoppedReason": null, "replayStoppedHeight": null, "portfolioMargin": null}
                ]
            })
        );
    }
}
