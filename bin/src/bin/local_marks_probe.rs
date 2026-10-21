//! Run the local marks' venue feeds standalone (design 000454 batch 1 acceptance): beside the server, without
//! restarting it. Public market data only; prints one JSON line per interval and writes the summary and the whole
//! `/extMarkInputs` view to the output directory.

use std::{path::PathBuf, time::Duration};

// Dependencies of the server binary in the same package; this tool does not use them.
use {axum as _, log as _};

use clap::Parser;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
#[command(about = "Run the local marks' venue feeds and report their coverage")]
struct Args {
    /// How long to run
    #[arg(long)]
    seconds: u64,
    /// Report every this many seconds
    #[arg(long)]
    every_secs: u64,
    /// Output directory (created): summary.json, mark_inputs.json
    #[arg(long)]
    out: PathBuf,
}

#[tokio::main]
async fn main() -> server::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    server::run_local_marks_probe(args.seconds, Duration::from_secs(args.every_secs), &args.out).await
}
