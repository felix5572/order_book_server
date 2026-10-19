//! Offline acceptance probe: parse one checkpoint and write every position's anchored liquidation price as JSON lines,
//! for comparison with the node (3001) and with the research reference implementation.

use std::{io::Write, path::Path, time::Instant};

use serde_json::json;

use super::{
    checkpoint::parse_checkpoint_file,
    liquidation::{AccountLiquidations, LiquidationPx, account_liquidations},
    view::{PortfolioMarginDto, account_mode_name, unsupported_reason_name, user_text},
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
        let AccountLiquidations { portfolio_margin, positions } = account_liquidations(&checkpoint, account);
        let portfolio_margin = portfolio_margin.as_ref().map(PortfolioMarginDto::new);
        for position in positions {
            let dex = &checkpoint.dexes[position.dex];
            let collateral = &checkpoint.collateral[&dex.collateral_token].name;
            let asset = &dex.assets[position.asset];
            let coin = &asset.name;
            if !coins.is_empty() && !coins.contains(coin) {
                continue;
            }
            let (status, liquidation_px, reason) = match &position.liquidation_px {
                LiquidationPx::Computed(price) => ("computed", Some(*price), None),
                LiquidationPx::NoPositiveSolution => ("none", None, None),
                LiquidationPx::Unsupported(reason) => ("unsupported", None, Some(unsupported_reason_name(reason))),
            };
            let row = json!({
                "user": user_text(*user),
                "dex": dex.name,
                "collateral": collateral,
                "coin": coin,
                "szi": asset.size(position.size_raw),
                "marginMode": if position.isolated { "isolated" } else { "cross" },
                "accountMode": account_mode_name(&account.mode),
                "poolPositions": position.pool_positions,
                "liquidationPxStatus": status,
                "liquidationPx": liquidation_px,
                "unsupportedReason": reason,
                "portfolioMargin": portfolio_margin,
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
