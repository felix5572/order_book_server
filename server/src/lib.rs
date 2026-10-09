#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::significant_drop_tightening))]
mod listeners;
pub mod metrics;
mod order_book;
mod prelude;
mod servers;
mod types;

use std::path::PathBuf;

use clap::ValueEnum;

pub use prelude::Result;
pub use servers::websocket_server::run_websocket_server;

/// Snapshot fetching mode. Only `direct` is supported: snapshots are dumped
/// from the node's periodic abci checkpoints on this host. `docker` is kept as
/// a CLI value only so that selecting it fails loudly at startup instead of
/// silently running an unverified container path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum SnapshotMode {
    /// Unsupported - rejected at startup
    Docker,
    /// Call hl-node directly (for systemctl/bare metal setups)
    #[default]
    Direct,
}

/// Server configuration passed from CLI arguments
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Full address string (e.g., "0.0.0.0:8000")
    pub address: String,
    /// WebSocket compression level (0-9)
    pub compression_level: u32,
    /// Optional base directory for hlnode data
    pub data_dir: Option<PathBuf>,
    /// Include perpetual futures markets
    pub include_perps: bool,
    /// Include spot markets (@ coins, PURR/USDC)
    pub include_spot: bool,
    /// Include HIP-3 markets
    pub include_hip3: bool,
    /// Path to hl-node binary (runs compute-l4-snapshots on checkpoints)
    pub hlnode_binary: String,
    /// Path where snapshot will be written (has default)
    pub snapshot_output_path: Option<PathBuf>,
    /// Port for Prometheus metrics endpoint (0 to disable)
    pub metrics_port: u16,
    /// BBO-only mode: lightweight mode that only tracks best bid/ask per coin
    /// Disables L2/L4/Trades subscriptions but uses ~100MB RAM instead of 2-3GB
    pub bbo_only: bool,
    /// Resend the last l2Book payload every N ms when nothing has changed.
    /// 0 = disabled (default). Provides a heartbeat for low-liquidity coins
    /// whose snapshot hash rarely changes, matching the official HL API behavior.
    pub l2book_heartbeat_ms: u64,
    /// Resend the last bbo payload every N ms when nothing has changed.
    /// 0 = disabled (default).
    pub bbo_heartbeat_ms: u64,
    /// Tolerate drift: when true, data-loss events are counted in metrics but
    /// never trigger a snapshot re-fetch. The book keeps serving live events
    /// through drift and does NOT self-heal until restarted. Off by default.
    pub no_resync: bool,
    /// Cap on events cached for gapless replay while a snapshot fetch is in
    /// flight (~0.5-1KB resident each). Must cover a full fetch window at
    /// peak event rate or every market-hours re-sync overflows into another
    /// re-sync. 0 = keep the built-in default.
    pub replay_cache_events: usize,
}
