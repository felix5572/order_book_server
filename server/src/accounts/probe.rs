//! Offline acceptance probe: parse one checkpoint and write every position's anchored liquidation price as JSON lines,
//! for comparison with the node (3001) and with the research reference implementation; optionally the node marks' seed
//! too (design 000439 r2), for comparison with the node's L4 export and the research checkpoint extracts.

use std::{io::Write, path::Path, time::Instant};

use serde_json::json;

use super::{
    checkpoint::parse_checkpoint_file,
    liquidation::{AccountLiquidations, LiquidationPx, account_liquidations},
    marks::seed::{Ema, MarkSeed, NativeAsset, ReferenceCache},
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
    /// Resting main-dex orders written with the seed, when asked for.
    pub seed_orders: Option<usize>,
}

/// Writes one JSON line per position whose coin is in `coins` (every coin when empty), and the marks' seed to
/// `seed_out` when given (`write_mark_seed`).
pub fn run_checkpoint_probe(
    path: &Path,
    coins: &[String],
    out: &mut dyn Write,
    seed_out: Option<&mut dyn Write>,
) -> Result<ProbeSummary> {
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
    let (height, time_ms, accounts) = (checkpoint.height, checkpoint.time_ms, checkpoint.accounts.len());
    let seed_orders = match seed_out {
        Some(seed_out) => Some(write_mark_seed(&MarkSeed::from_checkpoint(checkpoint)?, seed_out)?),
        None => None,
    };
    Ok(ProbeSummary { height, time_ms, parse_seconds, accounts, rows, seed_orders })
}

/// One JSON line per main-dex asset (prices in integer units as stored, times in unix ns, orders as
/// `[oid, side, limitPx, remaining]` in stored slot order), then one per HIP-3 coin; returns the orders written.
fn write_mark_seed(seed: &MarkSeed, out: &mut dyn Write) -> Result<usize> {
    let mut orders = 0;
    for (index, asset) in seed.native.assets.iter().enumerate() {
        writeln!(out, "{}", native_asset_json(seed.height, index, asset))?;
        orders += asset.orders.len();
    }
    let mut hip3: Vec<_> = seed.hip3.iter().collect();
    hip3.sort_unstable_by_key(|(coin, _)| *coin);
    for (coin, stored) in hip3 {
        let mark = stored.mark.map(|mark| json!({"px": mark.px, "timeNs": mark.time_ns}));
        writeln!(out, "{}", json!({"height": seed.height, "hip3": coin, "szDecimals": stored.size_decimals, "mark": mark}))?;
    }
    Ok(orders)
}

fn native_asset_json(height: u64, index: usize, asset: &NativeAsset) -> serde_json::Value {
    let ema = |ema: &Ema| {
        json!({"num": ema.numerator, "denom": ema.denominator_s, "decayParameter": ema.decay_parameter, "samples": ema.samples,
               "lastSampleNs": ema.last_sample_ns})
    };
    let cache = |cache: &ReferenceCache| {
        let members: Vec<_> = cache.members.iter().map(|(user, value, time_ns)| json!([user_text(*user), value, time_ns])).collect();
        json!({"lastBucket": cache.last_bucket, "median": cache.median, "members": members})
    };
    let references = &asset.references;
    let orders: Vec<_> =
        asset.orders.iter().map(|order| json!([order.oid, order.side, order.limit_px, order.remaining])).collect();
    json!({
        "height": height,
        "asset": index,
        "coin": asset.name,
        "szDecimals": asset.size_decimals,
        "storedOracle": asset.stored_oracle,
        "mark": {"px": asset.mark.px, "timeNs": asset.mark.time_ns},
        "basis": asset.emas.as_ref().map(|emas| ema(&emas.basis)),
        "localPrice": asset.emas.as_ref().map(|emas| ema(&emas.local_price)),
        "references": {"Spot": cache(&references.spot), "ExternalPerp": cache(&references.external_perp), "Mark": cache(&references.mark)},
        "lastTradePx": asset.last_trade_px,
        "orders": orders,
    })
}
