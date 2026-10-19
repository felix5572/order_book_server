//! Offline acceptance probe: parse one checkpoint and write every position's anchored liquidation price as JSON lines,
//! for comparison with the node (3001) and with the research reference implementation.

use std::{io::Write, path::Path, time::Instant};

use serde_json::json;

use super::{
    checkpoint::{AccountMode, parse_checkpoint_file},
    liquidation::{LiquidationPx, UnsupportedReason, account_liquidations},
};
use crate::prelude::Result;

#[derive(Debug)]
pub struct ProbeSummary {
    pub height: u64,
    pub time_ms: u64,
    pub parse_seconds: f64,
    pub accounts: usize,
    pub rows: usize,
}

/// Writes one JSON line per position whose coin is in `coins` (every coin when empty).
pub fn run_checkpoint_probe(path: &Path, coins: &[String], out: &mut dyn Write) -> Result<ProbeSummary> {
    let started = Instant::now();
    let checkpoint = parse_checkpoint_file(path)?;
    let parse_seconds = started.elapsed().as_secs_f64();
    let mut rows = 0;
    for (user, account) in &checkpoint.accounts {
        for position in account_liquidations(&checkpoint, account) {
            let dex = &checkpoint.dexes[position.dex];
            let collateral = &checkpoint.collateral[&dex.collateral_token].name;
            let coin = &dex.assets[position.asset].name;
            if !coins.is_empty() && !coins.contains(coin) {
                continue;
            }
            let (status, liquidation_px, reason) = match &position.liquidation_px {
                LiquidationPx::Computed(price) => ("computed", Some(*price), None),
                LiquidationPx::NoPositiveSolution => ("none", None, None),
                LiquidationPx::Unsupported(reason) => ("unsupported", None, Some(unsupported_reason(reason))),
            };
            let row = json!({
                "user": user.to_string().to_lowercase(),
                "dex": dex.name,
                "collateral": collateral,
                "coin": coin,
                "szi": position.size,
                "marginMode": if position.isolated { "isolated" } else { "cross" },
                "accountMode": account_mode(&account.mode),
                "poolPositions": position.pool_positions,
                "liquidationPxStatus": status,
                "liquidationPx": liquidation_px,
                "unsupportedReason": reason,
            });
            writeln!(out, "{row}")?;
            rows += 1;
        }
    }
    Ok(ProbeSummary {
        height: checkpoint.height,
        time_ms: checkpoint.time_ms,
        parse_seconds,
        accounts: checkpoint.accounts.len(),
        rows,
    })
}

/// The official `userAbstraction` names.
fn account_mode(mode: &AccountMode) -> String {
    match mode {
        AccountMode::Default => "default".to_string(),
        AccountMode::Disabled => "disabled".to_string(),
        AccountMode::Unified => "unifiedAccount".to_string(),
        AccountMode::PortfolioMargin => "portfolioMargin".to_string(),
        AccountMode::Unknown(code) => format!("unknown:{code}"),
    }
}

fn unsupported_reason(reason: &UnsupportedReason) -> String {
    match reason {
        UnsupportedReason::PortfolioMargin => "portfolioMargin".to_string(),
        UnsupportedReason::UnknownAccountMode(code) => format!("unknownAccountMode:{code}"),
        UnsupportedReason::MissingMark => "missingMark".to_string(),
        UnsupportedReason::TierCase => "tierCase".to_string(),
    }
}
