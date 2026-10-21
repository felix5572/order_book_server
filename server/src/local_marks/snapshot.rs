//! Every coin's marks at one tick (design 000454 r1 1.3, r2 revisions 1–2), assembled from what the tick read: the node
//! price state the account owner published, the HL book under the listener lock, and every feed.
//!
//! Each coin has two legs. The node leg is the replay's stored mark; the local leg is `compute::local_mark` on the
//! node state, the book and the venues quoting now. Two progress clocks gate both legs for every coin at once, so a quiet
//! coin is never withheld for its own silence: the applied block's time, and the book's (both book streams applied).
//! Either one more than `MAX_PROGRESS_LAG_MS` behind the local clock makes the legs that read it `stale`.

use std::sync::Arc;

use rustc_hash::FxHashMap;
use serde::Serialize;

use super::{
    compute::{LocalMark, MarkInputs, local_mark, weighted_median},
    price::VenuePrice,
    slot::{FeedReading, Validity},
    sources::{Feed, FeedMarkets, Kind, MarkSources},
};
use crate::{
    accounts::{marks::native::CoinPriceState, publish::NodePrices},
    listeners::order_book::BestPricesAt,
    order_book::{Coin, Px},
};

/// The applied block runs behind the local clock 0.7 s at the median and 2.4–2.9 s at each minute's P99 (bm 10-21
/// 06:43–06:57Z, the acceptance recording), its block times sometimes 3.5 s apart: at 3 s every coin flickered stale
/// ten times in 14 minutes, each time for at most a second. 5 s withholds a stalled state, not that jitter; a state up
/// to 5 s behind may have missed one mark move (about every 3 s).
pub(crate) const MAX_PROGRESS_LAG_MS: u64 = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum Status {
    Unavailable,
    /// A progress clock is behind.
    Stale,
}

/// Why a leg has no price now: `code` names the input, `detail` gives the evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Withheld {
    pub(crate) status: Status,
    pub(crate) code: &'static str,
    pub(crate) detail: String,
}

impl Withheld {
    fn unavailable(code: &'static str, detail: impl Into<String>) -> Self {
        Self { status: Status::Unavailable, code, detail: detail.into() }
    }

    const fn stale(code: &'static str, detail: String) -> Self {
        Self { status: Status::Stale, code, detail }
    }
}

/// The node leg: the replay's state of the coin, in its price units.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct NodeMark {
    pub(crate) size_decimals: u32,
    pub(crate) mark: u64,
    pub(crate) stored_oracle: u64,
    /// The Mark reference cache median; `None` before it has one.
    pub(crate) mark_cache: Option<u64>,
    pub(crate) last_trade: u64,
}

/// The local leg: the marks and the book they read, in the coin's price units.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct LocalLeg {
    pub(crate) size_decimals: u32,
    pub(crate) bid: u64,
    pub(crate) ask: u64,
    pub(crate) mark: LocalMark,
}

/// Which venues of a kind price the coin now (`Validity::Valid`), and their weights.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Coverage {
    pub(crate) used_venues: Vec<&'static str>,
    pub(crate) missing_venues: Vec<&'static str>,
    pub(crate) used_weight: u32,
    pub(crate) configured_weight: u32,
}

impl Coverage {
    pub(crate) fn add(&mut self, feed: Feed, validity: Validity) {
        self.configured_weight += feed.weight();
        if validity == Validity::Valid {
            self.used_venues.push(feed.venue.name());
            self.used_weight += feed.weight();
        } else {
            self.missing_venues.push(feed.venue.name());
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CoinMarks {
    pub(crate) node: Result<NodeMark, Withheld>,
    pub(crate) local: Result<LocalLeg, Withheld>,
    pub(crate) oracle_coverage: Coverage,
    pub(crate) external_coverage: Coverage,
}

/// The node price state the coins read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NodeProgress {
    pub(crate) seed_height: u64,
    pub(crate) height: u64,
    pub(crate) block_time_ms: u64,
}

/// The HL book as the tick read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BookProgress {
    pub(crate) time_ms: u64,
    pub(crate) height: u64,
    pub(crate) trusted: bool,
}

/// One tick's marks of every coin of the sources.
#[derive(Debug, Clone)]
pub(crate) struct LocalMarksSnapshot {
    pub(crate) computed_ms: u64,
    /// The node price state's progress; why there is none.
    pub(crate) node: Result<NodeProgress, String>,
    /// `None` before the book's first snapshot.
    pub(crate) book: Option<BookProgress>,
    table: Arc<CoinTable>,
    coins: Vec<CoinMarks>,
}

impl LocalMarksSnapshot {
    /// `None` for a coin outside the sources.
    pub(crate) fn coin(&self, coin: &str) -> Option<&CoinMarks> {
        self.table.by_name.get(coin).map(|index| &self.coins[*index])
    }

    /// Every coin in the sources' order.
    pub(crate) fn coins(&self) -> impl Iterator<Item = (&str, &CoinMarks)> {
        self.table.names.iter().map(String::as_str).zip(&self.coins)
    }
}

/// A listing resolved to its feed's place in the readings and its market there.
#[derive(Debug, Clone, Copy)]
struct ListedMarket {
    feed: Feed,
    reading: usize,
    market: usize,
    multiplier: VenuePrice,
}

/// Every coin of the sources, with its listings resolved once, in the sources' order.
#[derive(Debug)]
pub(crate) struct CoinTable {
    names: Vec<String>,
    book_coins: Vec<Coin>,
    listings: Vec<Vec<ListedMarket>>,
    by_name: FxHashMap<String, usize>,
    /// The feeds in the readings' order (`LocalMarkFeeds::readings`, the sources' feed order).
    feeds: Vec<Feed>,
}

impl CoinTable {
    pub(crate) fn new(sources: &MarkSources) -> Self {
        let feeds: Vec<Feed> = sources.feeds().map(|(feed, _)| feed).collect();
        let names: Vec<String> = sources.coins().map(str::to_string).collect();
        let listings = names
            .iter()
            .map(|coin| {
                let listed = sources.listings(coin).unwrap_or_else(|| unreachable!("a coin of the sources"));
                listed
                    .iter()
                    .map(|(feed, listing)| {
                        let reading = feeds.iter().position(|each| each == feed).unwrap_or_else(|| unreachable!("a listed feed has markets"));
                        let markets = sources.markets(*feed).unwrap_or_else(|| unreachable!("a listed feed has markets"));
                        let market = markets.index(&listing.symbol).unwrap_or_else(|| unreachable!("a listing of its feed's markets"));
                        ListedMarket { feed: *feed, reading, market, multiplier: listing.px_multiplier }
                    })
                    .collect()
            })
            .collect();
        let by_name = names.iter().enumerate().map(|(index, coin)| (coin.clone(), index)).collect();
        let book_coins = names.iter().map(|coin| Coin::new(coin)).collect();
        Self { names, book_coins, listings, by_name, feeds }
    }

    /// The coins to read from the HL book, in the table's order.
    pub(crate) fn book_coins(&self) -> &[Coin] {
        &self.book_coins
    }
}

/// What one tick read.
pub(crate) struct Tick<'a> {
    pub(crate) now_ms: u64,
    /// Why every local leg is withheld whatever the node and the book: the feeds stopped, or the account owner is gone.
    pub(crate) stopped: Option<String>,
    pub(crate) node: &'a NodePrices,
    /// `None` before the book's first snapshot.
    pub(crate) book: Option<&'a BestPricesAt>,
    /// Every feed's reading (`LocalMarkFeeds::readings`).
    pub(crate) readings: &'a [(Feed, &'a FeedMarkets, FeedReading)],
}

pub(crate) fn assemble(table: &Arc<CoinTable>, tick: &Tick<'_>) -> LocalMarksSnapshot {
    assert!(
        tick.readings.iter().map(|(feed, _, _)| *feed).eq(table.feeds.iter().copied()),
        "the readings follow the sources' feeds"
    );
    let node_withheld = node_withheld(tick);
    let local_withheld = tick
        .stopped
        .as_ref()
        .map(|reason| Withheld::unavailable("stopped", reason.clone()))
        .or_else(|| node_withheld.clone())
        .or_else(|| book_withheld(tick));
    let coins = (0..table.names.len())
        .map(|index| coin_marks(table, index, tick, node_withheld.as_ref(), local_withheld.as_ref()))
        .collect();
    LocalMarksSnapshot {
        computed_ms: tick.now_ms,
        node: tick.node.as_ref().map(|prices| NodeProgress { seed_height: prices.seed_height, height: prices.height, block_time_ms: prices.block_time_ms }).map_err(Clone::clone),
        book: tick.book.map(|book| BookProgress { time_ms: book.time, height: book.height, trusted: book.trusted }),
        table: Arc::clone(table),
        coins,
    }
}

/// The node prices unavailable, or behind.
fn node_withheld(tick: &Tick<'_>) -> Option<Withheld> {
    let prices = match tick.node {
        Ok(prices) => prices,
        Err(reason) => return Some(Withheld::unavailable("node", reason.clone())),
    };
    let lag_ms = tick.now_ms.saturating_sub(prices.block_time_ms);
    (lag_ms > MAX_PROGRESS_LAG_MS).then(|| {
        Withheld::stale("nodeProgress", format!("the node state's block {} is {lag_ms} ms behind the local clock", prices.height))
    })
}

/// The book not yet read, withheld after a data loss, or behind.
fn book_withheld(tick: &Tick<'_>) -> Option<Withheld> {
    let Some(book) = tick.book else {
        return Some(Withheld::unavailable("hlBook", "the HL book has no snapshot yet"));
    };
    if !book.trusted {
        return Some(Withheld::unavailable("hlBook", format!("the HL book is withheld after a data loss (height {})", book.height)));
    }
    let lag_ms = tick.now_ms.saturating_sub(book.time);
    (lag_ms > MAX_PROGRESS_LAG_MS)
        .then(|| Withheld::stale("bookProgress", format!("the HL book's block {} is {lag_ms} ms behind the local clock", book.height)))
}

fn coin_marks(table: &CoinTable, index: usize, tick: &Tick<'_>, node_withheld: Option<&Withheld>, local_withheld: Option<&Withheld>) -> CoinMarks {
    let coin = table.names[index].as_str();
    let listings = &table.listings[index];
    let (mut oracle_coverage, mut external_coverage) = (Coverage::default(), Coverage::default());
    for listed in listings {
        let (_, validity) = tick.readings[listed.reading].2.entries[listed.market];
        match listed.feed.kind {
            Kind::Spot => oracle_coverage.add(listed.feed, validity),
            Kind::Perp => external_coverage.add(listed.feed, validity),
        }
    }
    let state = match (node_withheld, tick.node) {
        (Some(withheld), _) => Err(withheld.clone()),
        (None, Ok(prices)) => prices.coins.get(coin).ok_or_else(|| {
            let detail = format!("the node replay does not claim {coin}: unsupported until the next checkpoint seed, or not listed");
            Withheld::unavailable("nodeCoin", detail)
        }),
        (None, Err(_)) => unreachable!("unavailable node prices are withheld"),
    };
    let node = state.clone().map(|state| NodeMark {
        size_decimals: state.size_decimals,
        mark: state.stored_mark,
        stored_oracle: state.stored_oracle,
        mark_cache: state.mark_cache,
        last_trade: state.last_trade,
    });
    let local = match (local_withheld, state) {
        (Some(withheld), _) => Err(withheld.clone()),
        (None, Err(withheld)) => Err(withheld),
        (None, Ok(state)) => local_leg(coin, listings, tick, index, state),
    };
    CoinMarks { node, local, oracle_coverage, external_coverage }
}

fn local_leg(coin: &str, listings: &[ListedMarket], tick: &Tick<'_>, index: usize, node: &CoinPriceState) -> Result<LocalLeg, Withheld> {
    let mark_cache = node.mark_cache.ok_or_else(|| Withheld::unavailable("markCache", "the node's Mark reference cache has no median yet"))?;
    let book = tick.book.unwrap_or_else(|| unreachable!("a book is read before any local leg"));
    let (Some(bid), Some(ask)) = book.prices[index] else {
        return Err(Withheld::unavailable("hlBook", format!("the HL book of {coin} has an empty side")));
    };
    let (bid, ask) = (book_units(bid, node.size_decimals)?, book_units(ask, node.size_decimals)?);
    let oracle = venue_median(listings, tick, Kind::Spot, node.size_decimals)?
        .ok_or_else(|| Withheld::unavailable("oracleCoverage", format!("no spot venue quotes {coin} now")))?;
    let external = venue_median(listings, tick, Kind::Perp, node.size_decimals)?
        .ok_or_else(|| Withheld::unavailable("externalCoverage", format!("no perp venue quotes {coin} now")))?;
    let inputs = MarkInputs { node, mark_cache, bid, ask, oracle, external, now_ns: tick.now_ms * 1_000_000 };
    let mark = local_mark(inputs).map_err(|err| Withheld::unavailable("calculation", format!("{coin}: {err}")))?;
    Ok(LocalLeg { size_decimals: node.size_decimals, bid, ask, mark })
}

/// The weighted median of the midpoints of the kind's venues quoting now, in the coin's price units; `None` when none
/// does.
fn venue_median(listings: &[ListedMarket], tick: &Tick<'_>, kind: Kind, size_decimals: u32) -> Result<Option<u64>, Withheld> {
    let calculation = |detail: String| Withheld::unavailable("calculation", detail);
    let mut mids = Vec::new();
    for listed in listings.iter().filter(|listed| listed.feed.kind == kind) {
        let (entry, validity) = tick.readings[listed.reading].2.entries[listed.market];
        if validity != Validity::Valid {
            continue;
        }
        let quote = entry.and_then(|entry| entry.quote).unwrap_or_else(|| unreachable!("a valid entry holds a quote"));
        let mid = quote
            .bid
            .midpoint(quote.ask)
            .and_then(|mid| mid.scaled(listed.multiplier))
            .ok_or_else(|| calculation(format!("{} midpoint of {quote:?} × {} is out of range", listed.feed.label(), listed.multiplier)))?;
        mids.push((mid, listed.feed.weight()));
    }
    weighted_median(mids).map(|median| median.node_units(size_decimals).map_err(calculation)).transpose()
}

/// A book price (1e-8) in the coin's price units, `10^-(6 - szDecimals)`; the book holds only prices of that grid.
fn book_units(px: Px, size_decimals: u32) -> Result<u64, Withheld> {
    let factor = 10_u64.pow(2 + size_decimals);
    if !px.value().is_multiple_of(factor) {
        return Err(Withheld::unavailable("hlBook", format!("HL book price {} (1e-8) is off the grid of szDecimals {size_decimals}", px.value())));
    }
    Ok(px.value() / factor)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        accounts::marks::NativePriceSnapshot,
        local_marks::{
            compute::tests::btc_state,
            slot::{Connection, Entry, Quote},
            sources::Venue,
        },
    };

    const SOURCES: &str = r#"{"coins": {
        "BTC": {"spot": {"binance": {"symbol": "BTCUSDT", "pxMultiplier": "1"}, "okx": {"symbol": "BTC-USDT", "pxMultiplier": "1"}},
                "perp": {"binance": {"symbol": "BTCUSDT", "pxMultiplier": "1"}}},
        "ETH": {"spot": {"binance": {"symbol": "ETHUSDT", "pxMultiplier": "1"}},
                "perp": {"binance": {"symbol": "ETHUSDT", "pxMultiplier": "1"}}}}}"#;
    pub(crate) const NOW_MS: u64 = 1_000_000;

    fn price(text: &str) -> VenuePrice {
        VenuePrice::parse(text).unwrap()
    }

    /// The test's world: the sources, every market quoting (spot BTC 100,000 / 100,000.2, perp 100,004), the node state
    /// of BTC at `NOW_MS`, and the book.
    pub(crate) struct World {
        pub(crate) sources: MarkSources,
        table: Arc<CoinTable>,
        readings: Vec<FeedReading>,
        pub(crate) node: NodePrices,
        pub(crate) book: Option<BestPricesAt>,
    }

    impl World {
        pub(crate) fn new() -> Self {
            let sources = MarkSources::parse(SOURCES).unwrap();
            let table = Arc::new(CoinTable::new(&sources));
            let quote = |bid: &str, ask: &str| {
                (Some(Entry::for_test(Some(Quote { bid: price(bid), ask: price(ask) }), NOW_MS, 1)), Validity::Valid)
            };
            let readings = sources
                .feeds()
                .map(|(feed, markets)| {
                    let entries = markets
                        .coins
                        .iter()
                        .map(|(coin, _)| match (feed.kind, coin.as_str()) {
                            (Kind::Spot, "BTC") => quote("100000", "100000.2"),
                            (Kind::Perp, "BTC") => quote("100004", "100004"),
                            _ => quote("4000", "4000.1"),
                        })
                        .collect();
                    FeedReading { connection: Connection::Connected { since_ms: 0 }, messages: 1, entries }
                })
                .collect();
            let mut btc = btc_state(0.0, NOW_MS * 1_000_000 - 1_000_000_000);
            btc.last_trade = 1_000_020;
            let coins = FxHashMap::from_iter([("BTC".to_string(), btc)]);
            let node = Ok(Arc::new(NativePriceSnapshot { seed_height: 900, height: 1_000, block_time_ms: NOW_MS - 500, coins }));
            let book = Some(BestPricesAt {
                time: NOW_MS - 400,
                height: 1_001,
                trusted: true,
                prices: vec![(Some(Px::new(10_000_100_000_000)), Some(Px::new(10_000_300_000_000))), (None, None)],
            });
            Self { sources, table, readings, node, book }
        }

        pub(crate) fn snapshot(&self, now_ms: u64) -> LocalMarksSnapshot {
            let readings: Vec<_> = self.sources.feeds().zip(&self.readings).map(|((feed, markets), reading)| (feed, markets, reading.clone())).collect();
            assemble(&self.table, &Tick { now_ms, stopped: None, node: &self.node, book: self.book.as_ref(), readings: &readings })
        }

        fn set(&mut self, feed: Feed, coin: &str, validity: Validity) {
            let markets = self.sources.markets(feed).unwrap();
            let index = markets.coins.iter().position(|(listed, _)| listed == coin).unwrap();
            let reading = self.sources.feeds().position(|(each, _)| each == feed).unwrap();
            self.readings[reading].entries[index].1 = validity;
        }
    }

    fn statuses(snapshot: &LocalMarksSnapshot, coin: &str) -> (Option<&'static str>, Option<&'static str>) {
        let marks = snapshot.coin(coin).unwrap();
        (marks.node.as_ref().err().map(|withheld| withheld.code), marks.local.as_ref().err().map(|withheld| withheld.code))
    }

    /// BTC: spot 100,000.1 (both venues) is 1,000,001 units, perp 100,004 is C, the book 100,001 / 100,003 with the last
    /// trade 100,002 gives L = 1,000,020; the reference is median(A = 1,000,001, L, C = 1,000,040) = L.
    #[test]
    fn a_coin_with_every_input_has_both_legs() {
        let world = World::new();
        let snapshot = world.snapshot(NOW_MS);
        let btc = snapshot.coin("BTC").unwrap();
        assert_eq!(btc.node.as_ref().unwrap().mark, 1_000_200);
        let local = btc.local.as_ref().unwrap();
        assert_eq!((local.bid, local.ask), (1_000_010, 1_000_030));
        assert_eq!((local.mark.oracle, local.mark.external, local.mark.local, local.mark.reference), (1_000_001, 1_000_040, 1_000_020, 1_000_020));
        assert_eq!(btc.oracle_coverage, Coverage { used_venues: vec!["binance", "okx"], missing_venues: Vec::new(), used_weight: 5, configured_weight: 5 });
        assert_eq!(snapshot.node, Ok(NodeProgress { seed_height: 900, height: 1_000, block_time_ms: NOW_MS - 500 }));
        assert_eq!(statuses(&snapshot, "ETH"), (Some("nodeCoin"), Some("nodeCoin")), "the node replay does not claim ETH");
    }

    /// Design 000454 r2 revision 1: each progress clock behind by more than `MAX_PROGRESS_LAG_MS` withdraws the legs
    /// reading it for every coin; advancing again restores them. The venues keep quoting throughout.
    #[test]
    fn either_progress_clock_behind_withdraws_until_it_moves_again() {
        let mut world = World::new();
        let node_at_limit = NOW_MS - 500 + MAX_PROGRESS_LAG_MS;
        assert_eq!(statuses(&world.snapshot(node_at_limit), "BTC"), (None, None), "the node at the limit, the book 100 ms within it");
        let node_behind = world.snapshot(node_at_limit + 1);
        assert_eq!(statuses(&node_behind, "BTC"), (Some("nodeProgress"), Some("nodeProgress")));
        assert_eq!(node_behind.coin("BTC").unwrap().local.as_ref().unwrap_err().status, Status::Stale);
        let Ok(node) = &mut world.node else { unreachable!() };
        Arc::make_mut(node).block_time_ms = node_at_limit - 500;
        assert_eq!(statuses(&world.snapshot(node_at_limit + 1), "BTC"), (None, None), "a new block restores both legs");
        let book_at_limit = NOW_MS - 400 + MAX_PROGRESS_LAG_MS;
        let book_behind = world.snapshot(book_at_limit + 1);
        assert_eq!(statuses(&book_behind, "BTC"), (None, Some("bookProgress")), "the node leg does not read the book");
        world.book.as_mut().unwrap().time = book_at_limit - 500;
        assert_eq!(statuses(&world.snapshot(book_at_limit + 1), "BTC"), (None, None));
    }

    /// The book withheld or not read yet, the node prices unavailable, or the module stopped withdraw the legs with
    /// their reason.
    #[test]
    fn unavailable_inputs_withdraw_with_their_reason() {
        let mut world = World::new();
        world.book.as_mut().unwrap().trusted = false;
        assert_eq!(statuses(&world.snapshot(NOW_MS), "BTC"), (None, Some("hlBook")));
        world.book = None;
        assert_eq!(statuses(&world.snapshot(NOW_MS), "BTC"), (None, Some("hlBook")));
        world.node = Err("the account view is unavailable: initializing".to_string());
        let snapshot = world.snapshot(NOW_MS);
        assert_eq!(statuses(&snapshot, "BTC"), (Some("node"), Some("node")), "the node precedes the book");
        assert_eq!(snapshot.coin("BTC").unwrap().node.as_ref().unwrap_err().detail, "the account view is unavailable: initializing");
        let world = World::new();
        let readings: Vec<_> = world.sources.feeds().zip(&world.readings).map(|((feed, markets), reading)| (feed, markets, reading.clone())).collect();
        let tick = Tick { now_ms: NOW_MS, stopped: Some("gone".to_string()), node: &world.node, book: world.book.as_ref(), readings: &readings };
        let stopped = assemble(&world.table, &tick);
        assert_eq!(statuses(&stopped, "BTC"), (None, Some("stopped")), "the node leg does not read the feeds");
    }

    /// Design 000454 r1: the venues quoting now, at least one per kind; none of a kind withdraws the local leg.
    #[test]
    fn the_coverage_takes_the_venues_quoting_now() {
        let mut world = World::new();
        world.set(Feed { kind: Kind::Spot, venue: Venue::Binance }, "BTC", Validity::NotConnected);
        let snapshot = world.snapshot(NOW_MS);
        let btc = snapshot.coin("BTC").unwrap();
        assert_eq!(btc.local.as_ref().unwrap().mark.oracle, 1_000_001, "OKX alone");
        assert_eq!((btc.oracle_coverage.used_weight, btc.oracle_coverage.missing_venues.as_slice()), (2, ["binance"].as_slice()));
        world.set(Feed { kind: Kind::Perp, venue: Venue::Binance }, "BTC", Validity::NotQuoted);
        assert_eq!(statuses(&world.snapshot(NOW_MS), "BTC"), (None, Some("externalCoverage")));
        world.set(Feed { kind: Kind::Spot, venue: Venue::Okx }, "BTC", Validity::Expired);
        assert_eq!(statuses(&world.snapshot(NOW_MS), "BTC"), (None, Some("oracleCoverage")));
    }

    /// A quiet coin: nothing of it changed while both clocks advance; it keeps both legs.
    #[test]
    fn a_quiet_coin_keeps_its_marks_while_the_clocks_advance() {
        let mut world = World::new();
        for step in 1..=10 {
            let now = NOW_MS + step * 1_000;
            let Ok(node) = &mut world.node else { unreachable!() };
            Arc::make_mut(node).block_time_ms = now - 500;
            world.book.as_mut().unwrap().time = now - 400;
            assert_eq!(statuses(&world.snapshot(now), "BTC"), (None, None), "{step} s on");
        }
    }

    #[test]
    fn an_empty_book_side_or_no_cache_withdraws_the_local_leg() {
        let mut world = World::new();
        world.book.as_mut().unwrap().prices[0].1 = None;
        assert_eq!(statuses(&world.snapshot(NOW_MS), "BTC"), (None, Some("hlBook")));
        let mut world = World::new();
        let Ok(node) = &mut world.node else { unreachable!() };
        Arc::make_mut(node).coins.get_mut("BTC").unwrap().mark_cache = None;
        assert_eq!(statuses(&world.snapshot(NOW_MS), "BTC"), (None, Some("markCache")));
    }

    #[test]
    fn book_prices_convert_to_price_units_exactly() {
        assert_eq!(book_units(Px::new(10_000_100_000_000), 5), Ok(1_000_010), "100,001.0 in 0.1");
        assert_eq!(book_units(Px::new(1_234_500), 0), Ok(12_345), "0.012345 with six decimals");
        assert_eq!(book_units(Px::new(1_234_567), 0).unwrap_err().code, "hlBook");
    }
}
