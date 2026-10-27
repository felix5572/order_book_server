//! `local_marks_probe` (design 000454 batch 1 acceptance, run beside the server without restarting it): the venue
//! feeds alone. Every interval it prints one JSON line - each feed's connection and quoting markets, the coins without
//! a quoting spot or perp venue, and how many venues price the coins' oracle and external price - and at the end the
//! share of intervals each coin had both, and the whole `/extMarkInputs` view.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use serde_json::json;

use super::{feeds::LocalMarkFeeds, slot::Validity, sources::Kind, view::inputs_view};
use crate::{local_marks::now_ms, prelude::Result};

/// Per coin: (intervals with a quoting spot venue, with a quoting perp venue, with both).
type Covered = BTreeMap<String, (u64, u64, u64)>;

pub async fn run_local_marks_probe(seconds: u64, every: Duration, out: &Path) -> Result<()> {
    std::fs::create_dir_all(out)?;
    let sources = Arc::new(super::sources::MarkSources::embedded()?);
    let feeds = LocalMarkFeeds::start(Arc::clone(&sources), None);
    let started = Instant::now();
    let mut covered: Covered = sources.coins().map(|coin| (coin.to_string(), (0, 0, 0))).collect();
    let mut intervals = 0_u64;
    while started.elapsed() < Duration::from_secs(seconds) {
        tokio::time::sleep(every).await;
        intervals += 1;
        let line = interval_line(&feeds, &mut covered, started.elapsed());
        println!("{line}");
    }
    let shares: BTreeMap<&str, f64> = covered.iter().map(|(coin, (_, _, both))| (coin.as_str(), share(*both, intervals))).collect();
    let mut short: Vec<(&str, f64)> = shares.iter().filter(|(_, share)| **share < 1.0).map(|(coin, share)| (*coin, *share)).collect();
    short.sort_by(|left, right| left.1.total_cmp(&right.1));
    let summary = json!({"intervals": intervals, "coins": shares.len(), "alwaysCovered": shares.len() - short.len(), "notAlwaysCovered": short});
    println!("{summary}");
    std::fs::write(out.join("summary.json"), serde_json::to_vec_pretty(&summary)?)?;
    let view = inputs_view(&feeds, None, now_ms())?;
    std::fs::write(out.join("mark_inputs.json"), serde_json::to_vec(&view)?)?;
    Ok(())
}

fn share(count: u64, of: u64) -> f64 {
    #[expect(clippy::cast_precision_loss, reason = "interval counts are small")]
    let share = if of == 0 { 0.0 } else { count as f64 / of as f64 };
    share
}

/// One interval: the feeds, and the coins' coverage now (counted into `covered`).
fn interval_line(feeds: &LocalMarkFeeds, covered: &mut Covered, elapsed: Duration) -> serde_json::Value {
    let readings = feeds.readings(now_ms());
    let mut venues: BTreeMap<(&str, Kind), u32> = BTreeMap::new();
    let feed_lines: Vec<serde_json::Value> = readings
        .iter()
        .map(|(feed, markets, reading)| {
            for ((entry_coin, _), (_, validity)) in markets.coins.iter().zip(&reading.entries) {
                if *validity == Validity::Valid {
                    *venues.entry((entry_coin.as_str(), feed.kind)).or_default() += 1;
                }
            }
            let quoting = reading.entries.iter().filter(|(_, validity)| *validity == Validity::Valid).count();
            json!({"feed": feed.label(), "connections": reading.connections.iter().map(|part| json!({"shard":part.shard,"markets":part.markets,"state":format!("{:?}",part.connection),"frames":part.frames,"quoting":part.quoting})).collect::<Vec<_>>(), "frames": reading.messages, "markets": markets.symbols.len(), "quoting": quoting})
        })
        .collect();
    let (mut no_oracle, mut no_external) = (Vec::new(), Vec::new());
    let mut oracle_venues: BTreeMap<u32, u32> = BTreeMap::new();
    let mut external_venues: BTreeMap<u32, u32> = BTreeMap::new();
    for (coin, (spot, perp, both)) in covered.iter_mut() {
        let spot_venues = venues.get(&(coin.as_str(), Kind::Spot)).copied().unwrap_or(0);
        let perp_venues = venues.get(&(coin.as_str(), Kind::Perp)).copied().unwrap_or(0);
        *oracle_venues.entry(spot_venues).or_default() += 1;
        *external_venues.entry(perp_venues).or_default() += 1;
        *spot += u64::from(spot_venues > 0);
        *perp += u64::from(perp_venues > 0);
        *both += u64::from(spot_venues > 0 && perp_venues > 0);
        if spot_venues == 0 {
            no_oracle.push(coin.clone());
        }
        if perp_venues == 0 {
            no_external.push(coin.clone());
        }
    }
    json!({
        "elapsedS": elapsed.as_secs(),
        "feeds": feed_lines,
        "noOracle": no_oracle,
        "noExternal": no_external,
        "oracleVenues": oracle_venues,
        "externalVenues": external_venues,
    })
}
