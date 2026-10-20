//! Run the account view standalone against a live node data directory and dump the `coinPositions` JSON of chosen
//! coins after every anchor install and at a fixed interval (design 000419, batch 2 acceptance). Read-only: it only
//! reads the node's streams and checkpoints.

use std::{fs::File, io::BufWriter, path::PathBuf, time::Duration};

// Dependencies of the server binary in the same package; this tool does not use them.
use {axum as _, log as _};

use clap::Parser;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
#[command(about = "Follow the account view live and dump coin positions")]
struct Args {
    /// Node data directory (holds `node_fills_streaming`, `misc_events_streaming`, `periodic_abci_states`)
    #[arg(long)]
    data_dir: PathBuf,
    /// Coins to dump, comma separated
    #[arg(long, value_delimiter = ',', required = true)]
    coins: Vec<String>,
    /// Output file, one JSON object per coin and dump
    #[arg(long)]
    out: PathBuf,
    /// Dump again after this many seconds without a new anchor
    #[arg(long)]
    dump_interval_secs: u64,
    /// Liquidation candidates record, one JSON object per refresh
    #[arg(long)]
    candidates_out: PathBuf,
}

#[tokio::main]
async fn main() -> server::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    let mut out = BufWriter::new(File::create(&args.out)?);
    let candidates_out = Box::new(BufWriter::new(File::create(&args.candidates_out)?));
    let options =
        server::FollowOptions { coins: args.coins, dump_interval: Duration::from_secs(args.dump_interval_secs), candidates_out };
    server::run_accounts_follow(&args.data_dir, options, &mut out).await
}
