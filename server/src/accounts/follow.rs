//! Live acceptance probe: runs the account view standalone (no order book, no API) and dumps the `coinPositions` JSON
//! of chosen coins right after every anchor install and at a fixed interval, for comparison with the node offline.
//! It also subscribes those coins to the push and logs what it receives (frame counts, bytes, version continuity), so
//! the push runs at production volume without the WS server.

use std::{
    collections::{BTreeMap, HashMap},
    io::Write,
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use log::{error, info};
use serde_json::json;
use tokio::sync::broadcast::{self, error::RecvError};

use super::{
    book::{QueryError, Unavailable},
    candidates_view::changes_json,
    publish::{AccountsFrame, CoinSubscriptionGuard},
    runtime::{AccountsHandle, start_accounts},
    view::CoinPositionsDto,
};
use crate::{prelude::Result, types::subscription::EXT_COIN_POSITIONS};

const POLL_INTERVAL: Duration = Duration::from_secs(1);
const STATUS_LOG_INTERVAL: Duration = Duration::from_secs(60);

pub struct FollowOptions {
    /// Coins to dump; each must be a coin of the anchor universe.
    pub coins: Vec<String>,
    /// Dump again after this long without a new anchor.
    pub dump_interval: Duration,
    /// Every liquidation candidates refresh, one JSON line each (design 000443 acceptance).
    pub candidates_out: Box<dyn Write + Send>,
}

/// Runs until the account view stops; returns the reason as the error.
pub async fn run_accounts_follow(data_dir: &Path, options: FollowOptions, out: &mut (dyn Write + Send)) -> Result<()> {
    let FollowOptions { coins, dump_interval, candidates_out } = options;
    // standalone: no order book, so no book walks
    let (accounts, owner) = start_accounts(data_dir, None)?;
    let guards: Vec<CoinSubscriptionGuard> = coins.iter().map(|coin| accounts.acquire(coin)).collect();
    tokio::spawn(watch_push(accounts.frames().ok_or("account owner task exited")?, guards, candidates_out));
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
        if dumped_anchor == Some(anchor.height) && last_dump.elapsed() < dump_interval {
            continue;
        }
        for coin in &coins {
            dump_coin(&accounts, coin, out).await?;
        }
        out.flush()?;
        info!("accounts: dumped {} coins at anchor {}", coins.len(), anchor.height);
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

/// What the push delivered, logged every `STATUS_LOG_INTERVAL`.
#[derive(Debug, Default)]
struct PushStats {
    frames: BTreeMap<&'static str, u64>,
    bytes: usize,
    largest: usize,
    lagged: u64,
    gaps: u64,
}

/// Receives the push for the subscribed coins (kept alive by `guards`) and checks every coin's versions run on by one;
/// records every liquidation candidates frame in `candidates_out`.
async fn watch_push(
    mut frames: broadcast::Receiver<Arc<AccountsFrame>>,
    guards: Vec<CoinSubscriptionGuard>,
    mut candidates_out: Box<dyn Write + Send>,
) {
    let _guards = guards;
    let mut versions: HashMap<String, u64> = HashMap::new();
    let mut stats = PushStats::default();
    let mut logged = Instant::now();
    loop {
        match frames.recv().await {
            Ok(frame) => {
                observe(&frame, &mut versions, &mut stats);
                if let Err(err) = record_candidates(&frame, &mut candidates_out) {
                    error!("push: writing the candidates record failed: {err}; the record stops");
                    return;
                }
            }
            Err(RecvError::Lagged(missed)) => {
                error!("push: lagged, {missed} frames dropped; restarting the version checks");
                stats.lagged += 1;
                versions.clear();
            }
            Err(RecvError::Closed) => {
                info!("push: the owner is gone; final {stats:?}");
                return;
            }
        }
        if logged.elapsed() >= STATUS_LOG_INTERVAL {
            info!("push: {stats:?}");
            stats = PushStats::default();
            logged = Instant::now();
        }
    }
}

/// One line per candidates frame: the refresh in full, or the withdrawal with its reason.
fn record_candidates(frame: &AccountsFrame, out: &mut (dyn Write + Send)) -> std::io::Result<()> {
    let line = match frame {
        AccountsFrame::Candidates(changes) => changes_json(changes),
        AccountsFrame::CandidatesWithdrawn { reason } => json!({"withdrawn": reason}),
        AccountsFrame::Coin { .. } | AccountsFrame::WithdrawAll { .. } | AccountsFrame::WithdrawCoin { .. } => return Ok(()),
    };
    writeln!(out, "{line}")?;
    out.flush()
}

fn observe(frame: &AccountsFrame, versions: &mut HashMap<String, u64>, stats: &mut PushStats) {
    match frame {
        AccountsFrame::Coin { coin, version, wire } => {
            let snapshot_prefix = format!(r#"{{"channel":"{EXT_COIN_POSITIONS}","data":{{"kind":"snapshot""#);
            let kind = if wire.starts_with(snapshot_prefix.as_bytes()) { "snapshot" } else { "changes" };
            *stats.frames.entry(kind).or_default() += 1;
            stats.bytes += wire.len();
            stats.largest = stats.largest.max(wire.len());
            if let Some(previous) = versions.insert(coin.clone(), *version)
                && *version != previous + 1
            {
                error!("push: {coin} version {version} after {previous}");
                stats.gaps += 1;
            }
        }
        AccountsFrame::WithdrawAll { reason } => {
            info!("push: withdrawn ({reason})");
            *stats.frames.entry("withdrawn").or_default() += 1;
            versions.clear();
        }
        AccountsFrame::WithdrawCoin { coin, reason } => {
            info!("push: {coin} withdrawn ({reason})");
            *stats.frames.entry("withdrawn").or_default() += 1;
            versions.remove(coin);
        }
        AccountsFrame::Candidates(_) => *stats.frames.entry("candidates").or_default() += 1,
        AccountsFrame::CandidatesWithdrawn { reason } => {
            info!("push: liquidation candidates withdrawn ({reason})");
            *stats.frames.entry("candidatesWithdrawn").or_default() += 1;
        }
    }
}
