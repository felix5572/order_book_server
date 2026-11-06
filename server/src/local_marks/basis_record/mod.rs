//! The basis record (hyperliquid-trade design 000751 r1-r3; user 2026-11-05 "也录一下这些 ... 看看有没有长期基差"):
//! every 10 s of the wall clock, a JSON line a coin to this host's files - HL's book, oracle and mark, every venue's
//! quotes of the coin, Binance's mark, index and funding, the bases the local marks publish, USDC/USDT on Binance's
//! spot and Pyth's USDT/USD and USDC/USD - for research only: nothing reads it back, nothing is published.
//!
//! A sidecar: it reads the local marks' snapshots (a brief borrow, then a copy of the `Arc`) and feeds' readings, the
//! HL book for HYPE alone, and its own Binance streams, HL official stream (`activeAssetCtx`, design 000782 r2) and
//! Pyth poller; the files are written on a thread of their own behind a bounded queue. Whatever fails in it - a stream stopped, the disk full - fails the record only: the local
//! marks, the quote tick and the book serve on.
//!
//! The 60- and 240-minute bases of the main dex are not windows here (no reader): the 10 s rows hold the books they
//! are the medians of (design 000751 r2).

pub(crate) mod binance;
pub(crate) mod hl_official;
pub(crate) mod pyth;
pub(crate) mod row;
pub(crate) mod writer;

use std::{
    collections::BTreeSet,
    fs::{self, OpenOptions},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use log::{error, info};
use rustc_hash::FxHashMap;
use tokio::sync::watch;

use self::{
    binance::{BinanceLatest, HYPE_SYMBOL},
    hl_official::HlOfficialLatest,
    pyth::PythLatest,
    row::{GroupReading, HYPE, Inputs, Listed, Row},
    writer::{RecordWriter, Tick},
};
use super::{
    feeds::LocalMarkFeeds,
    hip3::protocol::EquityFeed,
    now_ms,
    owner::{BookPrices, LocalMarks},
    price::VenuePrice,
    snapshot::LocalMarksSnapshot,
    sources::{Feed, Kind, Venue},
    subscriptions::FeedSubscriptions,
};
use crate::{metrics::BASIS_RECORD_TICKS_TOTAL, order_book::Coin};

/// The record's cadence on the wall clock.
const TICK_MS: u64 = 10_000;
const PROBE: &str = ".basis_record_probe";

/// A source's latest value, or why there is none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Latest<T> {
    /// Not received since start.
    NotReceived,
    Received(T),
    /// The source was stopped by a contract error, until restart.
    Stopped(String),
}

/// `--basis-record-dir`, `--basis-record-days` and the token `--basis-record-pyth-token-env` names.
pub(crate) struct BasisRecordConfig {
    pub(crate) dir: PathBuf,
    pub(crate) days: u32,
    pub(crate) pyth_token: String,
}

impl std::fmt::Debug for BasisRecordConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("BasisRecordConfig").field("dir", &self.dir).field("days", &self.days).field("pyth_token", &"<hidden>").finish()
    }
}

impl BasisRecordConfig {
    /// The start's checks (design 000751 r2 rule 6): a writable directory, at least a day kept, a token.
    pub(crate) fn check(&self) -> Result<(), String> {
        if self.days == 0 {
            return Err("--basis-record-days: at least 1".to_string());
        }
        if self.pyth_token.is_empty() {
            return Err("--basis-record-pyth-token-env: the variable it names is empty".to_string());
        }
        let probe = self.dir.join(PROBE);
        OpenOptions::new().create(true).write(true).truncate(true).open(&probe).map_err(|err| format!("--basis-record-dir {}: not writable: {err}", self.dir.display()))?;
        fs::remove_file(&probe).map_err(|err| format!("--basis-record-dir {}: {err}", self.dir.display()))
    }
}

/// The coins' contracts and markets, fixed for the process: the sources' and the feeds' partitions.
struct Plan {
    main_binance: FxHashMap<String, Listed>,
    hip3_binance: FxHashMap<String, Listed>,
    /// The HIP-3 feeds' markets in their readings' order: Binance's `TradFi` contracts, then the SIP's and BOATS'.
    hip3_groups: Vec<(Feed, Markets)>,
    /// Every recorded coin, whose `activeAssetCtx` HL's API streams: the sources', the HIP-3 channel's, HYPE.
    official_coins: BTreeSet<String>,
}

/// A feed's markets in its reading's order (its partitions' markets, one after another).
struct Markets {
    symbols: Vec<String>,
    coins: Vec<(String, VenuePrice)>,
}

impl Markets {
    fn of(group: &FeedSubscriptions) -> Self {
        let parts = group.subscriptions.iter().map(|part| &part.markets);
        let (mut symbols, mut coins) = (Vec::new(), Vec::new());
        for markets in parts {
            symbols.extend(markets.symbols.iter().cloned());
            coins.extend(markets.coins.iter().cloned());
        }
        Self { symbols, coins }
    }
}

impl Plan {
    fn new(feeds: &LocalMarkFeeds) -> Self {
        let binance_perp = Feed { kind: Kind::Perp, venue: Venue::Binance };
        let main_binance = feeds
            .sources
            .coins()
            .filter_map(|coin| {
                let listing = feeds.sources.listings(coin)?.get(&binance_perp)?;
                Some((coin.to_string(), Listed { symbol: listing.symbol.clone(), multiplier: listing.px_multiplier }))
            })
            .collect();
        let mut hip3_groups = Vec::new();
        let mut hip3_binance = FxHashMap::default();
        let mut official_coins: BTreeSet<String> = feeds.sources.coins().map(str::to_string).chain([HYPE.to_string()]).collect();
        if let (Some(external), Some(group)) = (&feeds.hip3_external, feeds.hip3_external_feed()) {
            let markets = Markets::of(group);
            official_coins.extend(external.coins.names().iter().cloned());
            for (index, coin) in external.coins.names().iter().enumerate() {
                if let Some(listing) = external.coins.binance(index) {
                    hip3_binance.insert(coin.clone(), Listed { symbol: markets.symbols[listing.market].clone(), multiplier: listing.multiplier });
                }
            }
            hip3_groups.push((group.feed, markets));
        }
        for feed in EquityFeed::ALL {
            if let Some(group) = feeds.equity_feed(feed) {
                hip3_groups.push((group.feed, Markets::of(group)));
            }
        }
        Self { main_binance, hip3_binance, hip3_groups, official_coins }
    }

    /// Every contract whose mark stream the record subscribes to: the sources' Binance perps and HYPEUSDT.
    fn mark_symbols(&self) -> BTreeSet<String> {
        let listed = self.main_binance.values().chain(self.hip3_binance.values());
        listed.map(|listed| listed.symbol.clone()).chain([HYPE_SYMBOL.to_string()]).collect()
    }
}

/// Starts the record over `marks`: its streams, its poller, its writer and its sampler.
pub(crate) fn start(config: BasisRecordConfig, marks: &LocalMarks, book: impl BookPrices) -> Result<(), String> {
    config.check()?;
    let plan = Plan::new(&marks.feeds);
    let mark_symbols = plan.mark_symbols();
    info!(
        "Basis record: {} main-dex coins ({} with a Binance perp), {} HIP-3 Binance contracts and HYPE, every {} s to {} ({} days kept); {} Binance mark streams, {} HL activeAssetCtx subscriptions",
        marks.feeds.sources.coins().count(),
        plan.main_binance.len(),
        plan.hip3_binance.len(),
        TICK_MS / 1_000,
        config.dir.display(),
        config.days,
        mark_symbols.len(),
        plan.official_coins.len()
    );
    let writer = RecordWriter::spawn(config.dir, config.days, now_ms()).map_err(|err| format!("basis record writer thread: {err}"))?;
    let sources = Sources { binance: binance::start(&mark_symbols), hl_official: hl_official::start(&plan.official_coins), pyth: pyth::start(config.pyth_token) };
    tokio::spawn(run_sampler(plan, Arc::clone(&marks.feeds), marks.subscribe(), book, sources, writer));
    Ok(())
}

struct Sources {
    binance: Arc<BinanceLatest>,
    hl_official: Arc<HlOfficialLatest>,
    pyth: Arc<PythLatest>,
}

async fn run_sampler(plan: Plan, feeds: Arc<LocalMarkFeeds>, mut snapshots: watch::Receiver<Arc<LocalMarksSnapshot>>, book: impl BookPrices, sources: Sources, writer: RecordWriter<Row>) {
    let hype = [Coin::new(HYPE)];
    let mut book_index: Option<FxHashMap<String, usize>> = None;
    loop {
        tokio::time::sleep(Duration::from_millis(TICK_MS - now_ms() % TICK_MS)).await;
        if snapshots.has_changed().is_err() {
            error!("basis record: the local marks' owner ended; the record stops until restart");
            return;
        }
        // a brief borrow: the owner's publish never waits on the record (review 000751 r1)
        let snapshot = Arc::clone(&snapshots.borrow_and_update());
        let book_index = book_index.get_or_insert_with(|| {
            snapshot.table().book_coins().iter().enumerate().map(|(index, coin)| (coin.value(), index)).collect()
        });
        let hype_book = book.best_prices(&hype).await;
        // every source copied first, then judged at one time after (design 000782 r2): no value read after that time
        let main_copies = feeds.copies();
        // collected, not chained: each copy is taken before `read_ms`
        #[allow(clippy::needless_collect)]
        let hip3_copies: Vec<_> = plan
            .hip3_groups
            .iter()
            .filter_map(|(feed, markets)| {
                let group = if *feed == Feed::HIP3_EXTERNAL { feeds.hip3_external_feed() } else { feeds.equity_feed(equity_feed(*feed)) }?;
                Some((*feed, markets, group.copy()))
            })
            .collect();
        let hl_official = sources.hl_official.copy();
        let read_ms = now_ms();
        let main_feeds = main_copies
            .into_iter()
            .map(|(feed, markets, copy)| GroupReading { feed, symbols: &markets.symbols, coins: &markets.coins, projection: copy.projection(read_ms) })
            .collect();
        let hip3_feeds = hip3_copies
            .into_iter()
            .map(|(feed, markets, copy)| GroupReading { feed, symbols: &markets.symbols, coins: &markets.coins, projection: copy.projection(read_ms) })
            .collect();
        let inputs = Inputs {
            snapshot: &snapshot,
            book_index,
            main_feeds,
            hip3_feeds,
            main_binance: &plan.main_binance,
            hip3_binance: &plan.hip3_binance,
            hype_book: hype_book.as_ref(),
            binance: &sources.binance,
            pyth: &sources.pyth,
            hl_official: &hl_official,
            read_ms,
        };
        let rows = row::rows(&inputs);
        BASIS_RECORD_TICKS_TOTAL.inc();
        writer.offer(Tick { t_ms: snapshot.computed_ms, rows });
    }
}

fn equity_feed(feed: Feed) -> EquityFeed {
    if feed == Feed::BOATS { EquityFeed::Boats } else { EquityFeed::Sip }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(dir: PathBuf, days: u32, token: &str) -> BasisRecordConfig {
        BasisRecordConfig { dir, days, pyth_token: token.to_string() }
    }

    /// Design 000751 r2 rule 6: a directory not writable, no day kept or an empty token refuses the start; the token
    /// is never shown.
    #[test]
    fn the_start_checks_refuse_what_cannot_run() {
        let dir = std::env::temp_dir().join(format!("basis_record_check_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        assert_eq!(config(dir.clone(), 90, "secret").check(), Ok(()));
        assert!(!dir.join(PROBE).exists(), "the probe removed");
        assert!(config(dir.clone(), 0, "secret").check().unwrap_err().contains("--basis-record-days"));
        assert!(config(dir.clone(), 90, "").check().unwrap_err().contains("--basis-record-pyth-token-env"));
        let missing = config(dir.join("missing"), 90, "secret").check().unwrap_err();
        assert!(missing.contains("not writable") && !missing.contains("secret"), "{missing}");
        assert!(!format!("{:?}", config(dir, 90, "secret")).contains("secret"), "Debug hides the token");
    }

    #[test]
    fn hype_is_subscribed_beside_the_listed_contracts() {
        let plan = Plan {
            main_binance: FxHashMap::from_iter([("BTC".to_string(), Listed { symbol: "BTCUSDT".into(), multiplier: VenuePrice::ONE })]),
            hip3_binance: FxHashMap::from_iter([("xyz:SNDK".to_string(), Listed { symbol: "SNDKUSDT".into(), multiplier: VenuePrice::ONE })]),
            hip3_groups: Vec::new(),
            official_coins: BTreeSet::new(),
        };
        assert_eq!(plan.mark_symbols().into_iter().collect::<Vec<_>>(), ["BTCUSDT", "HYPEUSDT", "SNDKUSDT"]);
    }
}
