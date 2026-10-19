//! Parse one node checkpoint and write every position's checkpoint-anchored liquidation price as JSON lines
//! (design 000419, batch 1 acceptance). Prints the parse time and the process peak RSS.

use std::{fs::File, io::BufWriter, io::Write, path::PathBuf};

// Dependencies of the server binary in the same package; this tool does not use them.
use {axum as _, env_logger as _, log as _, tokio as _};

use clap::Parser;

// Same allocator as the server, so the measured peak RSS is the one the server would see.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
#[command(about = "Anchored liquidation prices of one node checkpoint")]
struct Args {
    /// Checkpoint file: `<data-dir>/periodic_abci_states/<YYYYMMDD>/<height>.rmp`
    #[arg(long)]
    checkpoint: PathBuf,
    /// Coins to report, comma separated; every coin when omitted
    #[arg(long, value_delimiter = ',')]
    coins: Vec<String>,
    /// Output file, one JSON object per position
    #[arg(long)]
    out: PathBuf,
}

fn main() -> server::Result<()> {
    let args = Args::parse();
    let mut out = BufWriter::new(File::create(&args.out)?);
    let summary = server::run_checkpoint_probe(&args.checkpoint, &args.coins, &mut out)?;
    out.flush()?;
    println!(
        "height {} time_ms {} parse_seconds {:.2} accounts {} rows {} peak_rss_kb {}",
        summary.height,
        summary.time_ms,
        summary.parse_seconds,
        summary.accounts,
        summary.rows,
        peak_rss_kb()?
    );
    Ok(())
}

/// `VmHWM` of this process (Linux).
fn peak_rss_kb() -> server::Result<u64> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let line = status.lines().find(|line| line.starts_with("VmHWM:")).ok_or("no VmHWM in /proc/self/status")?;
    Ok(line.trim_start_matches("VmHWM:").trim().trim_end_matches("kB").trim().parse()?)
}
