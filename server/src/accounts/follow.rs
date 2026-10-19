//! Live acceptance probe: runs the account view standalone (no order book, no API) and dumps the `coinPositions` JSON
//! of chosen coins right after every anchor install and at a fixed interval, for comparison with the node offline.

use std::{
    io::Write,
    path::Path,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use log::info;
use serde_json::json;

use super::{
    book::{QueryError, Unavailable},
    runtime::{AccountsHandle, start_accounts},
    view::CoinPositionsDto,
};
use crate::prelude::Result;

const POLL_INTERVAL: Duration = Duration::from_secs(1);
const STATUS_LOG_INTERVAL: Duration = Duration::from_secs(60);

pub struct FollowOptions {
    /// Coins to dump; each must be a coin of the anchor universe.
    pub coins: Vec<String>,
    /// Dump again after this long without a new anchor.
    pub dump_interval: Duration,
}

/// Runs until the account view stops; returns the reason as the error.
pub async fn run_accounts_follow(
    data_dir: &Path,
    options: &FollowOptions,
    out: &mut (dyn Write + Send),
) -> Result<()> {
    let (accounts, owner) = start_accounts(data_dir)?;
    let mut ticker = tokio::time::interval(POLL_INTERVAL);
    let mut dumped_anchor = None;
    let mut last_dump = Instant::now();
    let mut last_status_log: Option<Instant> = None;
    loop {
        ticker.tick().await;
        if owner.is_finished() {
            return Err(format!("account owner task ended: {:?}", owner.await).into());
        }
        let status = accounts.status().await.map_err(|reason| reason.to_string())?;
        if last_status_log.is_none_or(|logged| logged.elapsed() >= STATUS_LOG_INTERVAL) {
            info!("accounts status: {status:?}");
            last_status_log = Some(Instant::now());
        }
        let available = match status.availability {
            Ok(available) => available,
            Err(Unavailable::Stopped(reason)) => return Err(format!("account view stopped: {reason}").into()),
            Err(_) => continue,
        };
        let anchor = available.anchor;
        if dumped_anchor == Some(anchor.height) && last_dump.elapsed() < options.dump_interval {
            continue;
        }
        for coin in &options.coins {
            dump_coin(&accounts, coin, out).await?;
        }
        out.flush()?;
        info!("accounts: dumped {} coins at anchor {}", options.coins.len(), anchor.height);
        dumped_anchor = Some(anchor.height);
        last_dump = Instant::now();
    }
}

async fn dump_coin(accounts: &AccountsHandle, coin: &str, out: &mut (dyn Write + Send)) -> Result<()> {
    let positions = accounts.coin_positions(coin).await;
    let query_time_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let line = match positions {
        Ok(view) => json!({"queryTimeMs": query_time_ms, "coinPositions": CoinPositionsDto::new(&view)}),
        Err(QueryError::UnknownCoin) => return Err(format!("{coin} is not a coin of the anchor universe").into()),
        Err(QueryError::Unavailable(reason)) => {
            json!({"queryTimeMs": query_time_ms, "coin": coin, "unavailable": reason.to_string()})
        }
    };
    writeln!(out, "{line}")?;
    Ok(())
}
