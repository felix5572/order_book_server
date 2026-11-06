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
use tokio::time::Instant;

use super::{
    feeds::EquityReadings,
    compute::{LocalMark, MarkInputs, local_mark, weighted_median},
    external_basis::{self, ExternalBasis, ExternalBasisWindow},
    hip3::{
        external::{
            Hip3ExternalCoin, Hip3ExternalGates, Hip3ExternalReading, Hip3ExternalSnapshot, Hip3ExternalTable, Hip3ExternalWindows, hip3_external, received_by,
        },
        external_coins::Hip3ExternalCoins,
        snapshot::{Hip3CoinMarks, Hip3CoinState, Hip3Gates, Hip3Table, hip3_marks},
        sources::Hip3SipSources,
    },
    price::VenuePrice,
    slot::{Entry, Validity},
    subscriptions::FeedProjection,
    sources::{Feed, FeedMarkets, Kind, MarkSources, Venue},
};
use crate::{
    accounts::{
        marks::{
            NativePriceSnapshot,
            native::{CoinPriceState, MarkUpdate},
            seed::{Hip3Coin, NativeEmas},
        },
        publish::NodePrices,
    },
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
    pub(crate) fn unavailable(code: &'static str, detail: impl Into<String>) -> Self {
        Self { status: Status::Unavailable, code, detail: detail.into() }
    }

    pub(crate) const fn stale(code: &'static str, detail: String) -> Self {
        Self { status: Status::Stale, code, detail }
    }
}

/// The node leg: the replay's state of the coin, in its price units. The caches' members stay in the tick's node prices
/// (`LocalMarksSnapshot::node_prices`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct NodeMark {
    pub(crate) size_decimals: u32,
    pub(crate) mark: u64,
    pub(crate) mark_time_ns: u64,
    pub(crate) mark_update: MarkUpdate,
    pub(crate) stored_oracle: u64,
    /// The last `SetGlobalAction`'s (oracle, mark); `None` before the coin has one.
    pub(crate) submitted: Option<(u64, u64)>,
    /// The Spot and Mark reference caches' medians (`None` before one has a median) and the unix ms from which a block
    /// refreshes them.
    pub(crate) spot_cache: Option<u64>,
    pub(crate) spot_cache_next_refresh_ms: Option<u64>,
    pub(crate) mark_cache: Option<u64>,
    pub(crate) mark_cache_next_refresh_ms: Option<u64>,
    pub(crate) emas: NativeEmas,
    pub(crate) last_trade: u64,
}

/// The local leg: the marks and the book they read, in the coin's price units.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LocalLeg {
    pub(crate) size_decimals: u32,
    pub(crate) bid: u64,
    pub(crate) ask: u64,
    pub(crate) mark: LocalMark,
    pub(crate) external_quotes: ExternalQuotes,
    /// The coin's normal basis to those quotes (design 000594), withheld for too few samples.
    pub(crate) external_basis: Result<ExternalBasis, Withheld>,
}

/// The perp quotes the external price C took (user 2026-10-30: a catch's price against the venues' best bids and asks,
/// design 000588): their bids' and asks' weighted medians, of the same venues and weights as C, and each quote. Exact,
/// in HL's price scale of the coin (the listing's multiplier applied) but not on its price grid, as C is: the bid median
/// is never above the ask median, and the unrounded median of the midpoints lies between them.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExternalQuotes {
    pub(crate) bid: VenuePrice,
    pub(crate) ask: VenuePrice,
    pub(crate) venues: Vec<VenueQuote>,
}

impl ExternalQuotes {
    /// `None` without a quote.
    pub(crate) fn of(venues: Vec<VenueQuote>) -> Option<Self> {
        let median = |side: fn(&VenueQuote) -> VenuePrice| weighted_median(venues.iter().map(|quote| (side(quote), quote.feed.weight())).collect());
        let (bid, ask) = (median(|quote| quote.bid)?, median(|quote| quote.ask)?);
        Some(Self { bid, ask, venues })
    }

    /// The quotes as of `published_ms`, the time a publish of them carries: withheld (`receivedAfterPublish`,
    /// `received_by`) when one was received after it - read after the time was taken, or the wall clock stepped back
    /// -: the main dex's external leg's rule at its publish time, the owner's (`GET /extExternalQuotes`) and the
    /// external quote tick's (review 000676 r1 P2).
    pub(crate) fn received_by(self, coin: &str, published_ms: u64) -> Result<Self, Withheld> {
        for quote in &self.venues {
            received_by(quote.feed.label(), coin, quote.received_ms, published_ms)?;
        }
        Ok(self)
    }
}

/// A coin's external quotes and its normal basis to them (design 000594, hyperliquid-trade design 000664, 000673): the
/// external leg of a main-dex coin (`extExternalQuotes`), the Binance leg of a HIP-3 coin (`extHip3ExternalQuotes`).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExternalLeg {
    pub(crate) quotes: ExternalQuotes,
    /// Withheld below its minimum samples, or, on the external quote tick, older than it takes the owner's
    /// (`basisStale`).
    pub(crate) basis: Result<ExternalBasis, Withheld>,
}

/// A venue's own normal basis to its perp quote (hyperliquid-trade design 000681): the median basis' rule
/// (`external_basis`) on that venue's quote alone, a window a perp listing of the coin, kept by the venue whether it
/// quotes or not.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VenueBasis {
    pub(crate) venue: Venue,
    /// Withheld below its minimum samples, or, on the external quote tick, older than it takes the owner's
    /// (`basisStale`).
    pub(crate) basis: Result<ExternalBasis, Withheld>,
}

/// A main-dex coin's external leg as `extExternalQuotes` (schema 2) publishes it (hyperliquid-trade design 000681):
/// the perp quotes and their medians' basis, and each quoting venue's own basis, in the quotes' order.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MainExternalLeg {
    pub(crate) external: ExternalLeg,
    /// One a venue of `external.quotes.venues`, in its order.
    pub(crate) venue_bases: Vec<VenueBasis>,
}

impl MainExternalLeg {
    /// The leg of `quotes` and their medians' `basis`, each quoting venue's basis that of `venue_bases` - every perp
    /// listing's, the owner's - as `taken` takes it: the owner's HTTP rows as they are, the quote tick by their age.
    pub(crate) fn of(
        quotes: ExternalQuotes,
        basis: Result<ExternalBasis, Withheld>,
        venue_bases: &[VenueBasis],
        taken: impl Fn(&Result<ExternalBasis, Withheld>) -> Result<ExternalBasis, Withheld>,
    ) -> Self {
        let basis_of = |venue: Venue| {
            let owned = venue_bases.iter().find(|owned| owned.venue == venue).unwrap_or_else(|| unreachable!("a basis of each perp listing, {venue:?}'s among them"));
            VenueBasis { venue, basis: taken(&owned.basis) }
        };
        let quoting = quotes.venues.iter().map(|quote| basis_of(quote.feed.venue)).collect();
        Self { external: ExternalLeg { quotes, basis: taken(&basis) }, venue_bases: quoting }
    }
}

/// A venue's quote valid now, its listing's multiplier applied.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct VenueQuote {
    pub(crate) feed: Feed,
    pub(crate) bid: VenuePrice,
    pub(crate) ask: VenuePrice,
    pub(crate) received_ms: u64,
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
    /// The perp quotes valid at the tick and their medians, none received after the tick's time (`computed_ms`,
    /// `ExternalQuotes::received_by`; review 000676 r1 P2), before the node and book gates (hyperliquid-trade design
    /// 000673 §4): the local leg's own when it is ready and they are - the local leg keeps `extMarkPrices`' time, its
    /// quotes read after the tick's time as before -; why there are none, the feeds stopped among the reasons.
    pub(crate) external: Result<ExternalQuotes, Withheld>,
    /// The coin's normal basis to the perp quotes as its window publishes it at the tick, whatever the gates: the local
    /// leg's, `GET /extExternalQuotes`', and the external quote tick's (design 000673 r4).
    pub(crate) external_basis: Result<ExternalBasis, Withheld>,
    /// Each perp listing's venue's own basis (design 000681), in the listings' order, as their windows publish them at
    /// the tick, whatever the gates: `extExternalQuotes`' (schema 2), the HTTP rows' and the quote tick's.
    pub(crate) venue_bases: Vec<VenueBasis>,
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
    /// The tick's moment on the monotonic clock: every basis of the snapshot is its window's then, the age the external
    /// quote tick judges it by (hyperliquid-trade design 000673 r4).
    pub(crate) computed_at: Instant,
    /// Why every leg reading the feeds is withheld until restart: the feeds stopped, or the account owner is gone. The
    /// external quote channels are withdrawn on it (design 000673 r4).
    pub(crate) stopped: Option<String>,
    /// The node price state's progress; why there is none.
    pub(crate) node: Result<NodeProgress, String>,
    /// `None` before the book's first snapshot.
    pub(crate) book: Option<BookProgress>,
    /// The node prices the tick read, whose coins' reference cache members `GET /extMarkReferenceCaches` serves.
    node_prices: Option<Arc<NativePriceSnapshot>>,
    /// The HL book's best prices the tick read, `CoinTable::book_coins`' order: the basis samples' book, the basis
    /// record's (hyperliquid-trade design 000751 r2 rule 3); `None` before the book's first snapshot.
    book_prices: Option<Arc<BestPricesAt>>,
    table: Arc<CoinTable>,
    coins: Vec<CoinMarks>,
    /// The HIP-3 SIP coins' (design 000535), in `CoinTable::hip3`'s order; none without `--hip3-sip-marks`.
    hip3: Vec<Hip3CoinMarks>,
    /// The HIP-3 coins' external quotes (hyperliquid-trade design 000664, 000669), in `CoinTable::hip3_external`'s
    /// order; `None` without `--hip3-external-quotes`.
    hip3_external: Option<Hip3ExternalSnapshot>,
}

impl LocalMarksSnapshot {
    /// The coins of the snapshot, every snapshot of one owner's.
    pub(crate) const fn table(&self) -> &Arc<CoinTable> {
        &self.table
    }

    /// The node state a ready node leg of `coin` reads.
    pub(crate) fn node_state(&self, coin: &str) -> Option<&CoinPriceState> {
        self.node_prices.as_ref()?.coins.get(coin)
    }

    /// The node state of the HIP-3 coin `coin` the tick read, whatever the local marks' sources list.
    pub(crate) fn hip3_node_state(&self, coin: &str) -> Option<&Hip3Coin> {
        self.node_prices.as_ref()?.hip3.get(coin)
    }

    /// The snapshot with the book prices its tick read (the owner's, hyperliquid-trade design 000751 r2 rule 3).
    pub(crate) fn with_book_prices(mut self, book_prices: Option<Arc<BestPricesAt>>) -> Self {
        self.book_prices = book_prices;
        self
    }

    /// The HL book's best prices the tick read, in `CoinTable::book_coins`' order; `None` before the book's first
    /// snapshot.
    pub(crate) fn book_prices(&self) -> Option<&BestPricesAt> {
        self.book_prices.as_deref()
    }

    /// `None` for a coin outside the sources.
    pub(crate) fn coin(&self, coin: &str) -> Option<&CoinMarks> {
        self.table.by_name.get(coin).map(|index| &self.coins[*index])
    }

    /// Every coin in the sources' order.
    pub(crate) fn coins(&self) -> impl Iterator<Item = (&str, &CoinMarks)> {
        self.table.names.iter().map(String::as_str).zip(&self.coins)
    }

    /// `None` for a coin outside the HIP-3 SIP sources.
    pub(crate) fn hip3_coin(&self, coin: &str) -> Option<&Hip3CoinMarks> {
        self.table.hip3.index(coin).map(|index| &self.hip3[index])
    }

    /// Every HIP-3 SIP coin in its sources' order.
    pub(crate) fn hip3_coins(&self) -> impl Iterator<Item = (&str, &Hip3CoinMarks)> {
        self.table.hip3.names().iter().map(String::as_str).zip(&self.hip3)
    }

    /// The HIP-3 external quotes of this snapshot; `None` without `--hip3-external-quotes`.
    pub(crate) const fn hip3_external(&self) -> Option<&Hip3ExternalSnapshot> {
        self.hip3_external.as_ref()
    }

    /// The coin's external legs; `None` without `--hip3-external-quotes` or for a coin outside the channel
    /// (`Hip3ExternalCoins`).
    pub(crate) fn hip3_external_coin(&self, coin: &str) -> Option<&Hip3ExternalCoin> {
        let index = self.table.hip3_external.index(coin)?;
        self.hip3_external.as_ref().map(|external| &external.coins[index])
    }

    /// Every coin of the channel in its order; none without `--hip3-external-quotes`.
    pub(crate) fn hip3_external_coins(&self) -> impl Iterator<Item = (&str, &Hip3ExternalCoin)> {
        let coins = self.hip3_external.iter().flat_map(|external| &external.coins);
        self.table.hip3_external.names().iter().map(String::as_str).zip(coins)
    }

    /// The coin's local predicted mark (price units) whichever market it is in, why there is none, or `None` for a coin
    /// in neither source: the one entry the forced flow's local calibre reads.
    pub(crate) fn local_predicted(&self, coin: &str) -> Option<Result<u64, &Withheld>> {
        if let Some(marks) = self.coin(coin) {
            return Some(marks.local.as_ref().map(|leg| leg.mark.predicted));
        }
        self.hip3_coin(coin).map(|marks| marks.local.as_ref().map(|leg| leg.mark.predicted))
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
    /// The HIP-3 SIP coins of the SIP feeds; their books follow the main coins' in `book_coins`.
    hip3: Hip3Table,
    /// The HIP-3 external quotes' coins; a coin of the SIP feeds' book is read once, the others' follow theirs (review
    /// 000664 r1 P2-1: the books read are the union of the tables by full coin name).
    hip3_external: Hip3ExternalTable,
}

impl CoinTable {
    pub(crate) fn new(sources: &MarkSources, hip3: Option<&Hip3SipSources>, hip3_external: Option<&Arc<Hip3ExternalCoins>>) -> Self {
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
        let sip_feeds = hip3.is_some();
        let hip3 = Hip3Table::new(hip3);
        let mut book_coins: Vec<Coin> = names.iter().chain(hip3.names()).map(|coin| Coin::new(coin)).collect();
        let hip3_external = Hip3ExternalTable::new(hip3_external, sip_feeds.then_some(&hip3), |coin| {
            hip3.index(coin).map_or_else(
                || {
                    book_coins.push(Coin::new(coin));
                    book_coins.len() - 1
                },
                |index| names.len() + index,
            )
        });
        Self { names, book_coins, listings, by_name, feeds, hip3, hip3_external }
    }

    /// The owner's state across ticks, nothing sampled yet.
    pub(crate) fn owner_state(&self) -> OwnerState {
        let hip3 = vec![Hip3CoinState::default(); self.hip3.names().len()];
        let hip3_external = vec![Hip3ExternalWindows::default(); self.hip3_external.names().len()];
        let venue_basis = (0..self.names.len()).map(|index| vec![ExternalBasisWindow::default(); self.perp_markets(index).count()]).collect();
        OwnerState { hip3, external_basis: vec![ExternalBasisWindow::default(); self.names.len()], venue_basis, hip3_external }
    }

    /// The coins to read from the HL book, in the table's order.
    pub(crate) fn book_coins(&self) -> &[Coin] {
        &self.book_coins
    }

    /// The main-dex coins in the sources' order.
    pub(crate) fn names(&self) -> &[String] {
        &self.names
    }

    /// The main-dex coin's place in the table; `None` outside the sources.
    pub(crate) fn index(&self, coin: &str) -> Option<usize> {
        self.by_name.get(coin).copied()
    }

    /// The HIP-3 external quotes' coins; none without them.
    pub(crate) const fn hip3_external(&self) -> &Hip3ExternalTable {
        &self.hip3_external
    }

    /// The main-dex coin's perp listings: each one's feed's place in the readings (`LocalMarkFeeds::readings`) and its
    /// market among the feed's.
    pub(crate) fn perp_markets(&self, index: usize) -> impl Iterator<Item = (usize, usize)> + '_ {
        self.listings[index].iter().filter(|listed| listed.feed.kind == Kind::Perp).map(|listed| (listed.reading, listed.market))
    }

    /// The main-dex coin's perp listings' venues, in the listings' order: each one's basis window's (`venue_basis`).
    pub(crate) fn perp_venues(&self, index: usize) -> impl Iterator<Item = Venue> + '_ {
        self.listings[index].iter().filter(|listed| listed.feed.kind == Kind::Perp).map(|listed| listed.feed.venue)
    }

    /// The main-dex coin's perp quotes valid now and their medians (design 000588), `observed` giving a listing's entry
    /// and validity by its feed's place in the readings and its market: the coin's external leg before any gate, the
    /// owner's and the external quote tick's one rule (hyperliquid-trade design 000673 §4).
    pub(crate) fn perp_quotes(&self, index: usize, observed: impl Fn(usize, usize) -> (Option<Entry>, Validity)) -> Result<ExternalQuotes, Withheld> {
        external_quotes(&self.names[index], valid_quotes(&self.listings[index], Kind::Perp, observed))
    }
}

/// What the local marks' owner keeps across ticks, by the table's coins.
#[derive(Debug, Clone)]
pub(crate) struct OwnerState {
    /// Each HIP-3 coin's EMA, SIP midpoints and offset carry (design 000535 r2 revision 4, 000537).
    pub(crate) hip3: Vec<Hip3CoinState>,
    /// Each main-dex coin's basis samples (design 000594).
    pub(crate) external_basis: Vec<ExternalBasisWindow>,
    /// Each main-dex coin's venues' own basis samples (hyperliquid-trade design 000681): a window a perp listing, in
    /// the coin's listings' order - the venue's own, never a place among the venues quoting now.
    pub(crate) venue_basis: Vec<Vec<ExternalBasisWindow>>,
    /// Each HIP-3 external coin's basis samples, of each of its legs (design 000664, 000669).
    pub(crate) hip3_external: Vec<Hip3ExternalWindows>,
}

/// What one tick read.
pub(crate) struct Tick<'a> {
    pub(crate) now_ms: u64,
    /// The same moment on the monotonic clock (the HIP-3 EMAs' sampling).
    pub(crate) instant: Instant,
    /// Why every local leg is withheld whatever the node and the book: the feeds stopped, or the account owner is gone.
    pub(crate) stopped: Option<String>,
    pub(crate) node: &'a NodePrices,
    /// `None` before the book's first snapshot.
    pub(crate) book: Option<&'a BestPricesAt>,
    /// Every feed's reading (`LocalMarkFeeds::readings`).
    pub(crate) readings: &'a [(Feed, &'a FeedMarkets, FeedProjection)],
    /// The HIP-3 equity feeds' readings; `None` without them.
    pub(crate) equity: Option<&'a EquityReadings>,
    /// The HIP-3 external quotes' Binance reading, judged at its own publish time after the others' reads (the US
    /// stock feeds' among them, `equity`); `None` without them.
    pub(crate) hip3_external: Option<&'a Hip3ExternalReading>,
}

/// `state`: the owner's (`CoinTable::owner_state`); this tick moves it.
pub(crate) fn assemble(table: &Arc<CoinTable>, tick: &Tick<'_>, state: &mut OwnerState) -> LocalMarksSnapshot {
    assert!(
        tick.readings.iter().map(|(feed, _, _)| *feed).eq(table.feeds.iter().copied()),
        "the readings follow the sources' feeds"
    );
    assert_eq!(state.external_basis.len(), table.names.len(), "one basis window per main-dex coin");
    let node_withheld = node_withheld(tick);
    let book_withheld = book_withheld_at(tick, tick.now_ms);
    let stopped = tick.stopped.as_ref().map(|reason| Withheld::unavailable("stopped", reason.clone()));
    let local_withheld = stopped.clone().or_else(|| node_withheld.clone()).or_else(|| book_withheld.clone());
    let coin_gates = CoinGates {
        node: node_withheld.as_ref(),
        local: local_withheld.as_ref(),
        stopped: stopped.as_ref(),
        // the basis reads the book and the venues, not the node state (design 000594)
        basis_inputs: stopped.is_none() && book_withheld.is_none(),
    };
    let windows = state.external_basis.iter_mut().zip(state.venue_basis.iter_mut());
    let coins = windows.enumerate().map(|(index, (basis, venue_windows))| coin_marks(table, index, tick, coin_gates, basis, venue_windows)).collect();
    let gates = Hip3Gates { node: node_withheld.as_ref(), book: book_withheld.as_ref(), stopped: stopped.as_ref() };
    let hip3 = hip3_marks(&table.hip3, tick, table.names.len(), gates, &mut state.hip3);
    // the external quotes' basis samples the book as of their own publish time, later than the tick's (review 000665 r1 P2)
    let hip3_external = tick.hip3_external.map(|reading| {
        let gates = Hip3ExternalGates { stopped: stopped.as_ref(), book_usable: book_withheld_at(tick, reading.published_ms).is_none() };
        hip3_external(&table.hip3_external, tick, reading, gates, &mut state.hip3_external)
    });
    LocalMarksSnapshot {
        computed_ms: tick.now_ms,
        computed_at: tick.instant,
        stopped: tick.stopped.clone(),
        node: tick.node.as_ref().map(|prices| NodeProgress { seed_height: prices.seed_height, height: prices.height, block_time_ms: prices.block_time_ms }).map_err(Clone::clone),
        book: tick.book.map(|book| BookProgress { time_ms: book.time, height: book.height, trusted: book.trusted }),
        node_prices: tick.node.as_ref().ok().map(Arc::clone),
        book_prices: None,
        table: Arc::clone(table),
        coins,
        hip3,
        hip3_external,
    }
}

/// The node prices unavailable, or behind.
fn node_withheld(tick: &Tick<'_>) -> Option<Withheld> {
    match tick.node {
        Ok(prices) => node_progress_withheld(prices.height, prices.block_time_ms, tick.now_ms),
        Err(reason) => Some(Withheld::unavailable("node", reason.clone())),
    }
}

/// The node state at block `height`, of block time `block_time_ms`, more than `MAX_PROGRESS_LAG_MS` behind the local
/// clock at `now_ms`: the node leg's one rule, the basis record's too (hyperliquid-trade design 000782 r2 rule 1).
pub(crate) fn node_progress_withheld(height: u64, block_time_ms: u64, now_ms: u64) -> Option<Withheld> {
    let lag_ms = now_ms.saturating_sub(block_time_ms);
    (lag_ms > MAX_PROGRESS_LAG_MS)
        .then(|| Withheld::stale("nodeProgress", format!("the node state's block {height} is {lag_ms} ms behind the local clock")))
}

/// The book not yet read, withheld after a data loss, or behind the local clock at `now_ms`.
fn book_withheld_at(tick: &Tick<'_>, now_ms: u64) -> Option<Withheld> {
    let Some(book) = tick.book else {
        return Some(Withheld::unavailable("hlBook", "the HL book has no snapshot yet"));
    };
    book_progress_withheld(book.time, book.height, book.trusted, now_ms)
}

/// The book read at block `height`, of time `time_ms`, withheld after a data loss, or more than `MAX_PROGRESS_LAG_MS`
/// behind the local clock at `now_ms`: the book's one rule, the basis record's too (design 000782 r2 rule 1).
pub(crate) fn book_progress_withheld(time_ms: u64, height: u64, trusted: bool, now_ms: u64) -> Option<Withheld> {
    if !trusted {
        return Some(Withheld::unavailable("hlBook", format!("the HL book is withheld after a data loss (height {height})")));
    }
    let lag_ms = now_ms.saturating_sub(time_ms);
    (lag_ms > MAX_PROGRESS_LAG_MS)
        .then(|| Withheld::stale("bookProgress", format!("the HL book's block {height} is {lag_ms} ms behind the local clock")))
}

/// What gates every main-dex coin's legs at this tick.
#[derive(Debug, Clone, Copy)]
struct CoinGates<'a> {
    node: Option<&'a Withheld>,
    /// The stop's, the node's or the book's: the local leg's.
    local: Option<&'a Withheld>,
    /// The feeds stopped or the account owner gone: the external quotes' (`CoinMarks::external`).
    stopped: Option<&'a Withheld>,
    /// The book and the feeds usable this tick, whatever the node state: the basis samples'.
    basis_inputs: bool,
}

/// `basis`: the coin's medians' basis window; `venue_windows`: its perp listings' venues', in the listings' order.
fn coin_marks(
    table: &CoinTable,
    index: usize,
    tick: &Tick<'_>,
    gates: CoinGates<'_>,
    basis: &mut ExternalBasisWindow,
    venue_windows: &mut [ExternalBasisWindow],
) -> CoinMarks {
    let coin = table.names[index].as_str();
    let listings = &table.listings[index];
    let (mut oracle_coverage, mut external_coverage) = (Coverage::default(), Coverage::default());
    for listed in listings {
        let (_, validity) = tick.readings[listed.reading].2.entries[listed.market];
        match listed.feed.kind {
            Kind::Spot => oracle_coverage.add(listed.feed, validity),
            Kind::Perp => external_coverage.add(listed.feed, validity),
            Kind::Equity | Kind::EquityOvernight | Kind::Hip3Perp => unreachable!("a main-dex coin lists no Alpaca or HIP-3 market (`Feed::ALL`)"),
        }
    }
    let perp_listed: Vec<_> = listed_quotes(listings, Kind::Perp, observed_at(tick)).collect();
    let perps = external_quotes(coin, perp_listed.iter().filter_map(|(_, quote)| quote.clone().transpose()).collect());
    let book = sampled_book(tick, index, gates.basis_inputs);
    basis.observe(tick.instant, book.zip(perps.as_ref().ok()).map(|((bid, ask), quotes)| external_basis::sample(bid, ask, quotes.bid, quotes.ask)));
    let external_basis = basis.published();
    let venue_bases = venue_bases(tick, book, &perp_listed, venue_windows);
    let state = match (gates.node, tick.node) {
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
        mark_time_ns: state.mark_time_ns,
        mark_update: state.mark_update,
        stored_oracle: state.stored_oracle,
        submitted: state.submitted,
        spot_cache: state.spot_cache.as_ref().and_then(|cache| cache.median),
        spot_cache_next_refresh_ms: state.spot_cache.as_ref().map(|cache| cache.next_refresh_ms()),
        mark_cache: state.mark_cache.as_ref().and_then(|cache| cache.median),
        mark_cache_next_refresh_ms: state.mark_cache.as_ref().map(|cache| cache.next_refresh_ms()),
        emas: state.emas,
        last_trade: state.last_trade,
    });
    let local = match (gates.local, state) {
        (Some(withheld), _) => Err(withheld.clone()),
        (None, Err(withheld)) => Err(withheld),
        (None, Ok(state)) => local_leg(coin, listings, tick, index, state, perps.clone(), external_basis.clone()),
    };
    let external = gates.stopped.map_or_else(|| perps.and_then(|quotes| quotes.received_by(coin, tick.now_ms)), |stopped| Err(stopped.clone()));
    CoinMarks { node, local, external, external_basis, venue_bases, oracle_coverage, external_coverage }
}

/// Each perp listing's venue basis: its window moves - the samples past `WINDOW` leaving whether or not one enters -,
/// with this tick's sample when `book` (HL's two sides, when they may be sampled) is given and the venue's quote is
/// valid now (`perp_listed`, `listed_quotes`' rule, its multiplier applied); each window the listing's, by its place in
/// the listings, whichever venues quote now (review 000681 r1).
fn venue_bases(
    tick: &Tick<'_>,
    book: Option<(Px, Px)>,
    perp_listed: &[(Feed, Result<Option<VenueQuote>, Withheld>)],
    windows: &mut [ExternalBasisWindow],
) -> Vec<VenueBasis> {
    assert_eq!(perp_listed.len(), windows.len(), "one venue basis window per perp listing");
    let listed = perp_listed.iter().zip(windows);
    let venue_basis = |((feed, quote), window): (&(Feed, Result<Option<VenueQuote>, Withheld>), &mut ExternalBasisWindow)| {
        let quote = quote.as_ref().ok().copied().flatten();
        window.observe(tick.instant, book.zip(quote).map(|((bid, ask), quote)| external_basis::sample(bid, ask, quote.bid, quote.ask)));
        VenueBasis { venue: feed.venue, basis: window.published() }
    };
    listed.map(venue_basis).collect()
}

/// A listing's entry and validity in the tick's readings, by its feed's place there and its market.
fn observed_at<'t>(tick: &'t Tick<'_>) -> impl Fn(usize, usize) -> (Option<Entry>, Validity) + 't {
    |reading, market| tick.readings[reading].2.entries[market]
}

/// HL's two sides of the coin when this tick may sample them (`usable`: the book and the feeds usable, whatever the
/// node state): the basis samples' book, the medians' and each venue's; `None` without it or with a side empty.
fn sampled_book(tick: &Tick<'_>, index: usize, usable: bool) -> Option<(Px, Px)> {
    if !usable {
        return None;
    }
    let book = tick.book.unwrap_or_else(|| unreachable!("the book is read before a basis sample"));
    let (bid, ask) = book.prices[index];
    bid.zip(ask)
}

/// `perps`: the coin's perp quotes valid now (`CoinTable::perp_quotes`).
fn local_leg(
    coin: &str,
    listings: &[ListedMarket],
    tick: &Tick<'_>,
    index: usize,
    node: &CoinPriceState,
    perps: Result<ExternalQuotes, Withheld>,
    external_basis: Result<ExternalBasis, Withheld>,
) -> Result<LocalLeg, Withheld> {
    if !node.reference_modes.ordinary() {
        return Err(Withheld::unavailable("referenceMode", "this coin takes submitted node prices; the ordinary spot-reference local prediction does not apply"));
    }
    let mark_cache = node.mark_cache.as_ref().and_then(|cache| cache.median).ok_or_else(|| Withheld::unavailable("markCache", "the node's Mark reference cache has no median yet"))?;
    let book = tick.book.unwrap_or_else(|| unreachable!("a book is read before any local leg"));
    let (Some(bid), Some(ask)) = book.prices[index] else {
        return Err(Withheld::unavailable("hlBook", format!("the HL book of {coin} has an empty side")));
    };
    let (bid, ask) = (book_units(bid, node.size_decimals)?, book_units(ask, node.size_decimals)?);
    let oracle = mid_median(&valid_quotes(listings, Kind::Spot, observed_at(tick))?, node.size_decimals)?
        .ok_or_else(|| Withheld::unavailable("oracleCoverage", format!("no spot venue quotes {coin} now")))?;
    let external_quotes = perps?;
    let external = mid_median(&external_quotes.venues, node.size_decimals)?.unwrap_or_else(|| unreachable!("the external quotes hold a quote"));
    let inputs = MarkInputs { node, mark_cache, bid, ask, oracle, external, now_ns: tick.now_ms * 1_000_000 };
    let mark = local_mark(inputs).map_err(|err| Withheld::unavailable("calculation", format!("{coin}: {err}")))?;
    Ok(LocalLeg { size_decimals: node.size_decimals, bid, ask, mark, external_quotes, external_basis })
}

fn calculation(detail: String) -> Withheld {
    Withheld::unavailable("calculation", detail)
}

/// The perp quotes valid now and their medians; `externalCoverage` without one.
fn external_quotes(coin: &str, valid: Result<Vec<VenueQuote>, Withheld>) -> Result<ExternalQuotes, Withheld> {
    ExternalQuotes::of(valid?).ok_or_else(|| Withheld::unavailable("externalCoverage", format!("no perp venue quotes {coin} now")))
}

/// The kind's venues quoting now, in the listings' order, `observed` giving a listing's entry and validity by its
/// feed's place in the readings and its market.
fn valid_quotes(listings: &[ListedMarket], kind: Kind, observed: impl Fn(usize, usize) -> (Option<Entry>, Validity)) -> Result<Vec<VenueQuote>, Withheld> {
    listed_quotes(listings, kind, observed).filter_map(|(_, quote)| quote.transpose()).collect()
}

/// Each listing of the kind in the listings' order, its feed and its venue's quote valid now - its multiplier
/// applied -, `None` when not valid: the one rule of which quotes count (`valid_quotes`) and of each perp venue's own
/// basis samples (`venue_bases`).
fn listed_quotes<'l>(
    listings: &'l [ListedMarket],
    kind: Kind,
    observed: impl Fn(usize, usize) -> (Option<Entry>, Validity) + 'l,
) -> impl Iterator<Item = (Feed, Result<Option<VenueQuote>, Withheld>)> + 'l {
    let listed = listings.iter().filter(move |listed| listed.feed.kind == kind);
    listed.map(move |listed| (listed.feed, listed_quote(listed, observed(listed.reading, listed.market))))
}

/// The listing's quote valid now, its multiplier applied; `None` when its entry is not valid.
fn listed_quote(listed: &ListedMarket, (entry, validity): (Option<Entry>, Validity)) -> Result<Option<VenueQuote>, Withheld> {
    if validity != Validity::Valid {
        return Ok(None);
    }
    let entry = entry.unwrap_or_else(|| unreachable!("a valid entry holds a quote"));
    let quote = entry.quote.unwrap_or_else(|| unreachable!("a valid entry holds a quote"));
    let scaled = |px: VenuePrice| {
        px.scaled(listed.multiplier).ok_or_else(|| calculation(format!("{} price {px} × {} is out of range", listed.feed.label(), listed.multiplier)))
    };
    Ok(Some(VenueQuote { feed: listed.feed, bid: scaled(quote.bid)?, ask: scaled(quote.ask)?, received_ms: entry.received_ms }))
}

/// The weighted median of the quotes' midpoints, in the coin's price units; `None` without a quote.
fn mid_median(quotes: &[VenueQuote], size_decimals: u32) -> Result<Option<u64>, Withheld> {
    let mids = quotes.iter().map(|quote| {
        let mid = quote.bid.midpoint(quote.ask);
        mid.map(|mid| (mid, quote.feed.weight())).ok_or_else(|| calculation(format!("{} midpoint of {} / {} is out of range", quote.feed.label(), quote.bid, quote.ask)))
    });
    let mids = mids.collect::<Result<Vec<_>, _>>()?;
    weighted_median(mids).map(|median| median.node_units(size_decimals).map_err(calculation)).transpose()
}

/// A book price (1e-8) in the coin's price units, `10^-(6 - szDecimals)`; the book holds only prices of that grid.
pub(crate) fn book_units(px: Px, size_decimals: u32) -> Result<u64, Withheld> {
    let factor = 10_u64.pow(2 + size_decimals);
    if !px.value().is_multiple_of(factor) {
        return Err(Withheld::unavailable("hlBook", format!("HL book price {} (1e-8) is off the grid of szDecimals {size_decimals}", px.value())));
    }
    Ok(px.value() / factor)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{
        accounts::marks::NativePriceSnapshot,
        local_marks::{
            compute::tests::btc_state,
            hip3::external_sources::Hip3ExternalSources,
            slot::{Entry, Quote},
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

    /// Review 000664 r1 P2-1, design 000669 r1 1.1: the books read are the union of the main, SIP and external tables
    /// by full coin name, each coin once; the channel's coins are both tables', whether the SIP feeds run or not: with
    /// them, a coin of their table reads its book there and its quote among their markets, the others follow; without
    /// them, every channel coin's book follows the main coins', a SIP coin's US stock leg off.
    #[test]
    fn the_books_read_are_the_union_of_the_tables_each_once() {
        use crate::local_marks::hip3::external_equity::EquityPlace;
        let sources = MarkSources::parse(SOURCES).unwrap();
        let sip = Hip3SipSources::parse(r#"{"feed": "sip", "coins": {"xyz:TSLA": {"symbol": "TSLA"}, "xyz:AAPL": {"symbol": "AAPL"}}}"#).unwrap();
        let external = Hip3ExternalSources::parse(r#"{"venue": "binance_perp", "coins": {"xyz:TSLA": {"symbol": "TSLAUSDT", "pxMultiplier": "1"},
            "xyz:ZHIPU": {"symbol": "ZHIPUUSDT", "pxMultiplier": "1"}}}"#).unwrap();
        let coins = Arc::new(Hip3ExternalCoins::new(&external, &sip));
        let names = |table: &CoinTable| table.book_coins().iter().map(Coin::value).collect::<Vec<_>>();
        let both = CoinTable::new(&sources, Some(&sip), Some(&coins));
        assert_eq!(names(&both), ["BTC", "ETH", "xyz:AAPL", "xyz:TSLA", "xyz:ZHIPU"]);
        let read = |table: &CoinTable| table.hip3_external.book_indices().iter().map(|index| table.book_coins()[*index].value()).collect::<Vec<_>>();
        assert_eq!(read(&both), ["xyz:AAPL", "xyz:TSLA", "xyz:ZHIPU"]);
        assert_eq!(both.hip3_external.equity_places(), [EquityPlace::Read(0), EquityPlace::Read(1), EquityPlace::Unlisted], "the SIP feeds' AAPL, TSLA");
        let external_only = CoinTable::new(&sources, None, Some(&coins));
        assert_eq!(names(&external_only), ["BTC", "ETH", "xyz:AAPL", "xyz:TSLA", "xyz:ZHIPU"]);
        assert_eq!(read(&external_only), ["xyz:AAPL", "xyz:TSLA", "xyz:ZHIPU"]);
        assert_eq!(external_only.hip3_external.equity_places(), [EquityPlace::Off, EquityPlace::Off, EquityPlace::Unlisted]);
        assert_eq!(names(&CoinTable::new(&sources, Some(&sip), None)), ["BTC", "ETH", "xyz:AAPL", "xyz:TSLA"], "off: the books read as before");
    }

    /// Design 000741 r2: 300 s of ticks, every window's samples handed to the store (`SeriesTable::entered`); a fresh
    /// state restored from them ticks on as the one that took them - GOLD's Binance leg, its 10-minute basis ready
    /// and its longer ones counting, and BTC's medians' basis alike -, nothing left out.
    #[test]
    fn a_state_restored_from_the_stored_samples_ticks_on_as_the_one_that_took_them() {
        use crate::local_marks::{basis_store::SeriesTable, external_basis::SAMPLE_INTERVAL, hip3::external::Hip3SourceLeg};
        let world = World::new();
        let external = Hip3ExternalSources::parse(r#"{"venue": "binance_perp", "coins": {"xyz:GOLD": {"symbol": "XAUUSDT", "pxMultiplier": "1"}}}"#).unwrap();
        let sip = Hip3SipSources::parse(r#"{"feed": "sip", "coins": {"xyz:TSLA": {"symbol": "TSLA"}}}"#).unwrap();
        let table = Arc::new(CoinTable::new(&world.sources, None, Some(&Arc::new(Hip3ExternalCoins::new(&external, &sip)))));
        let readings: Vec<_> = world.sources.feeds().zip(&world.readings).map(|((feed, markets), reading)| (feed, markets, reading.clone())).collect();
        let start = Instant::now();
        let gold = Quote { bid: price("3900"), ask: price("3901") };
        let tick_at = |state: &mut OwnerState, second: u32| {
            let now_ms = NOW_MS + 1_000 * u64::from(second);
            let entries = vec![(Some(Entry::for_test(Some(gold), now_ms, 1)), Validity::Valid)];
            let reading = Hip3ExternalReading { published_ms: now_ms + 1, quote_max_age_ms: 60_000, projection: FeedProjection { connections: Vec::new(), messages: 1, entries } };
            let gold_book = (Some(Px::new(390_050_000_000 + u64::from(second % 7) * 100_000_000)), Some(Px::new(390_160_000_000)));
            let prices = vec![(Some(Px::new(10_000_100_000_000)), Some(Px::new(10_000_300_000_000))), (None, None), gold_book, (None, None)];
            let book = BestPricesAt { time: now_ms, height: 1_001, trusted: true, prices };
            let tick = Tick {
                now_ms, instant: start + SAMPLE_INTERVAL * second, stopped: None, node: &world.node, book: Some(&book), readings: &readings, equity: None,
                hip3_external: Some(&reading),
            };
            assemble(&table, &tick, state)
        };
        let gold_leg = |snapshot: &LocalMarksSnapshot| match &snapshot.hip3_external_coin("xyz:GOLD").unwrap().external {
            Hip3SourceLeg::Listed(Ok(leg)) => leg.clone(),
            other => panic!("{other:?}"),
        };
        let series = SeriesTable::new(&table);
        let (mut state, mut stored) = (table.owner_state(), Vec::new());
        for second in 0..300 {
            tick_at(&mut state, second);
            stored.extend(series.entered(&mut state, NOW_MS + 1_000 * u64::from(second)));
        }
        let mut restored = table.owner_state();
        let counts = series.restore(&mut restored, stored, NOW_MS + 299_500, start + SAMPLE_INTERVAL * 299 + Duration::from_millis(500));
        assert_eq!((counts.outside, counts.unknown), (0, 0));
        let (taken, again) = (tick_at(&mut state, 300), tick_at(&mut restored, 300));
        assert_eq!(gold_leg(&again), gold_leg(&taken));
        assert_eq!(gold_leg(&again).leg.basis.map(|basis| basis.samples), Ok(301));
        assert_eq!(gold_leg(&again).longer.hour.unwrap_err().detail, "31 of the 60 samples needed over the last 3600 s");
        assert_eq!(again.coin("BTC").unwrap().external_basis, taken.coin("BTC").unwrap().external_basis);
    }

    /// Review 000665 r1 P2: the HIP-3 external basis samples the book as of the quotes' publish time: a book 5,000 ms
    /// behind at the tick, 5,001 ms at the publish, takes no 120th sample - the quote still read -; the next tick with
    /// both fresh publishes the basis.
    #[test]
    fn the_external_basis_judges_the_book_at_its_publish_time() {
        use crate::local_marks::{external_basis::SAMPLE_INTERVAL, hip3::external::Hip3SourceLeg};
        let world = World::new();
        let external = Hip3ExternalSources::parse(r#"{"venue": "binance_perp", "coins": {"xyz:GOLD": {"symbol": "XAUUSDT", "pxMultiplier": "1"}}}"#).unwrap();
        let sip = Hip3SipSources::parse(r#"{"feed": "sip", "coins": {"xyz:TSLA": {"symbol": "TSLA"}}}"#).unwrap();
        let table = Arc::new(CoinTable::new(&world.sources, None, Some(&Arc::new(Hip3ExternalCoins::new(&external, &sip)))));
        let mut state = table.owner_state();
        let readings: Vec<_> = world.sources.feeds().zip(&world.readings).map(|((feed, markets), reading)| (feed, markets, reading.clone())).collect();
        let start = Instant::now();
        let gold = Quote { bid: price("3900"), ask: price("3901") };
        let tick_at = |state: &mut OwnerState, second: u32, book_lag_ms: u64| {
            let now_ms = NOW_MS + 1_000 * u64::from(second);
            let entries = vec![(Some(Entry::for_test(Some(gold), now_ms, 1)), Validity::Valid)];
            let reading = Hip3ExternalReading { published_ms: now_ms + 1, quote_max_age_ms: 60_000, projection: FeedProjection { connections: Vec::new(), messages: 1, entries } };
            let gold_book = (Some(Px::new(390_050_000_000)), Some(Px::new(390_060_000_000)));
            let prices = vec![(Some(Px::new(10_000_100_000_000)), Some(Px::new(10_000_300_000_000))), (None, None), gold_book, (None, None)];
            let book = BestPricesAt { time: now_ms - book_lag_ms, height: 1_001, trusted: true, prices };
            let tick = Tick {
                now_ms, instant: start + SAMPLE_INTERVAL * second, stopped: None, node: &world.node, book: Some(&book), readings: &readings, equity: None,
                hip3_external: Some(&reading),
            };
            assemble(&table, &tick, state)
        };
        let gold_leg = |snapshot: &LocalMarksSnapshot| match &snapshot.hip3_external_coin("xyz:GOLD").unwrap().external {
            Hip3SourceLeg::Listed(Ok(leg)) => leg.leg.clone(),
            other => panic!("{other:?}"),
        };
        for second in 0..119 {
            tick_at(&mut state, second, 0);
        }
        let late = gold_leg(&tick_at(&mut state, 119, MAX_PROGRESS_LAG_MS));
        assert_eq!((late.quotes.bid, late.quotes.ask), (price("3900"), price("3901")), "the quote still read");
        let withheld = &late.basis.as_ref().unwrap_err().detail;
        assert!(withheld.starts_with("119 of the 120 samples"), "{withheld}");
        let basis = gold_leg(&tick_at(&mut state, 120, 0)).basis.unwrap();
        assert_eq!(basis.samples, 120);
    }

    /// The test's world: the sources, every market quoting (spot BTC 100,000 / 100,000.2, perp 100,003.95 /
    /// 100,004.05), the node state of BTC at `NOW_MS`, and the book.
    pub(crate) struct World {
        pub(crate) sources: MarkSources,
        table: Arc<CoinTable>,
        readings: Vec<FeedProjection>,
        pub(crate) node: NodePrices,
        pub(crate) book: Option<BestPricesAt>,
    }

    impl World {
        pub(crate) fn new() -> Self {
            Self::of(SOURCES)
        }

        /// The world of `sources`, which list BTC and ETH.
        fn of(sources: &str) -> Self {
            let sources = MarkSources::parse(sources).unwrap();
            let table = Arc::new(CoinTable::new(&sources, None, None));
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
                            (Kind::Perp, "BTC") => quote("100003.95", "100004.05"),
                            _ => quote("4000", "4000.1"),
                        })
                        .collect();
                    FeedProjection { connections: Vec::new(), messages: 1, entries }
                })
                .collect();
            let mut btc = btc_state(0.0, NOW_MS * 1_000_000 - 1_000_000_000);
            btc.last_trade = 1_000_020;
            let coins = FxHashMap::from_iter([("BTC".to_string(), btc)]);
            let node = Ok(Arc::new(NativePriceSnapshot { seed_height: 900, height: 1_000, block_time_ms: NOW_MS - 500, coins, hip3: FxHashMap::default() }));
            let book = Some(BestPricesAt {
                time: NOW_MS - 400,
                height: 1_001,
                trusted: true,
                prices: vec![(Some(Px::new(10_000_100_000_000)), Some(Px::new(10_000_300_000_000))), (None, None)],
            });
            Self { sources, table, readings, node, book }
        }

        /// A first tick: the owner's state new.
        pub(crate) fn snapshot(&self, now_ms: u64) -> LocalMarksSnapshot {
            self.snapshot_at(&mut self.owner_state(), now_ms, Instant::now())
        }

        pub(crate) fn owner_state(&self) -> OwnerState {
            self.table.owner_state()
        }

        /// A tick at `now_ms` and `instant` of the owner's `state`.
        pub(crate) fn snapshot_at(&self, state: &mut OwnerState, now_ms: u64, instant: Instant) -> LocalMarksSnapshot {
            self.snapshot_of(state, now_ms, instant, None)
        }

        /// A first tick with the feeds stopped for `reason`.
        pub(crate) fn snapshot_stopped(&self, now_ms: u64, reason: &str) -> LocalMarksSnapshot {
            self.snapshot_of(&mut self.owner_state(), now_ms, Instant::now(), Some(reason))
        }

        fn snapshot_of(&self, state: &mut OwnerState, now_ms: u64, instant: Instant, stopped: Option<&str>) -> LocalMarksSnapshot {
            let readings: Vec<_> = self.sources.feeds().zip(&self.readings).map(|((feed, markets), reading)| (feed, markets, reading.clone())).collect();
            let stopped = stopped.map(str::to_string);
            let tick = Tick { now_ms, instant, stopped, node: &self.node, book: self.book.as_ref(), readings: &readings, equity: None, hip3_external: None };
            assemble(&self.table, &tick, state)
        }

        fn set(&mut self, feed: Feed, coin: &str, validity: Validity) {
            let (reading, index) = self.place(feed, coin);
            self.readings[reading].entries[index].1 = validity;
        }

        /// The venue's quote of the coin, valid, as written and received at `received_ms`.
        fn quote(&mut self, feed: Feed, coin: &str, (bid, ask): (&str, &str), received_ms: u64) {
            let (reading, index) = self.place(feed, coin);
            let entry = Entry::for_test(Some(Quote { bid: price(bid), ask: price(ask) }), received_ms, 1);
            self.readings[reading].entries[index] = (Some(entry), Validity::Valid);
        }

        /// The feed's place in the readings and the coin's market in it.
        fn place(&self, feed: Feed, coin: &str) -> (usize, usize) {
            let markets = self.sources.markets(feed).unwrap();
            let index = markets.coins.iter().position(|(listed, _)| listed == coin).unwrap();
            (self.sources.feeds().position(|(each, _)| each == feed).unwrap(), index)
        }
    }

    /// Review 000535 r2: a shared snapshot that also carries HIP-3 coins gives the main dex's output unchanged, frame
    /// for frame.
    #[test]
    fn the_hip3_coins_leave_the_main_output_unchanged() {
        let world = World::new();
        let alone = world.snapshot(NOW_MS);
        let hip3 = Hip3SipSources::parse(r#"{"feed": "sip", "coins": {"xyz:TSLA": {"symbol": "TSLA"}}}"#).unwrap();
        let table = Arc::new(CoinTable::new(&world.sources, Some(&hip3), None));
        let mut book = world.book.clone().unwrap();
        book.prices.push((None, None));
        let readings: Vec<_> = world.sources.feeds().zip(&world.readings).map(|((feed, markets), reading)| (feed, markets, reading.clone())).collect();
        let tick = Tick { now_ms: NOW_MS, instant: Instant::now(), stopped: None, node: &world.node, book: Some(&book), readings: &readings, equity: None, hip3_external: None };
        let shared = assemble(&table, &tick, &mut table.owner_state());
        let frames = |snapshot: &LocalMarksSnapshot| {
            snapshot.coins().map(|(coin, marks)| super::super::prices_view::CoinMarksView::of(snapshot, coin, marks).to_wire()).collect::<Vec<_>>()
        };
        assert_eq!(frames(&shared), frames(&alone));
        assert_eq!(shared.hip3_coin("xyz:TSLA").unwrap().local.as_ref().unwrap_err().code, "nodeCoin", "and TSLA is there");
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
        let tick = Tick { now_ms: NOW_MS, instant: Instant::now(), stopped: Some("gone".to_string()), node: &world.node, book: world.book.as_ref(), readings: &readings, equity: None, hip3_external: None };
        let stopped = assemble(&world.table, &tick, &mut world.owner_state());
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

    /// BTC on three perp venues, Bybit's contract a thousandth of HL's coin.
    const THREE_PERPS: &str = r#"{"coins": {
        "BTC": {"spot": {"binance": {"symbol": "BTCUSDT", "pxMultiplier": "1"}},
                "perp": {"binance": {"symbol": "BTCUSDT", "pxMultiplier": "1"}, "okx": {"symbol": "BTC-USDT-SWAP", "pxMultiplier": "1"},
                         "bybit": {"symbol": "MILLIBTCUSDT", "pxMultiplier": "1000"}}},
        "ETH": {"spot": {"binance": {"symbol": "ETHUSDT", "pxMultiplier": "1"}},
                "perp": {"binance": {"symbol": "ETHUSDT", "pxMultiplier": "1"}}}}}"#;

    /// Design 000588: the perp quotes C took, each scaled by its listing's multiplier, and their bids' and asks' weighted
    /// medians over C's venues and weights. BTC (0.1): Binance (weight 3) 100,003 / 100,005, OKX (2) 100,001 / 100,009,
    /// Bybit (2) 100.006 / 100.00624 × 1,000: the bid median is Binance's, the ask median Bybit's 100,006.24 off the 0.1
    /// grid, and C the midpoints' 100,005, not the side medians' midpoint 100,004.62. A venue gone leaves all three: OKX
    /// and Bybit (2 and 2) give the higher of each side, C 100,006.12 rounded to 100,006.1.
    #[test]
    fn the_external_quotes_take_the_external_prices_venues_and_weights() {
        let perp = |venue| Feed { kind: Kind::Perp, venue };
        let mut world = World::of(THREE_PERPS);
        world.quote(perp(Venue::Binance), "BTC", ("100003", "100005"), NOW_MS - 30);
        world.quote(perp(Venue::Okx), "BTC", ("100001", "100009"), NOW_MS - 20);
        world.quote(perp(Venue::Bybit), "BTC", ("100.006", "100.00624"), NOW_MS - 10);
        let local = |world: &World| world.snapshot(NOW_MS).coin("BTC").unwrap().local.clone().unwrap();
        let venues = |quotes: &ExternalQuotes| quotes.venues.iter().map(|quote| (quote.feed.venue, quote.bid, quote.ask, quote.received_ms)).collect::<Vec<_>>();
        let all = local(&world);
        assert_eq!((all.mark.external, all.external_quotes.bid, all.external_quotes.ask), (1_000_050, price("100003"), price("100006.24")));
        let (binance, okx, bybit) = (
            (Venue::Binance, price("100003"), price("100005"), NOW_MS - 30),
            (Venue::Okx, price("100001"), price("100009"), NOW_MS - 20),
            (Venue::Bybit, price("100006"), price("100006.24"), NOW_MS - 10),
        );
        assert_eq!(venues(&all.external_quotes), vec![binance, okx, bybit]);
        world.set(perp(Venue::Binance), "BTC", Validity::NotConnected);
        let without = local(&world);
        assert_eq!((without.mark.external, without.external_quotes.bid, without.external_quotes.ask), (1_000_061, price("100006"), price("100009")));
        assert_eq!(venues(&without.external_quotes), vec![okx, bybit]);
    }

    /// Design 000594: the basis samples the book and the venues once a second whatever the node state; a ready local
    /// leg carries it from its minimum samples on. BTC's book 100,001 / 100,003 against the perp 100,003.95 /
    /// 100,004.05: 100,002 / 100,004 - 1.
    #[test]
    fn the_basis_samples_the_book_and_the_venues_whatever_the_node() {
        let mut world = World::new();
        let mut state = world.owner_state();
        let start = Instant::now();
        let at = |second: u64| start + Duration::from_secs(second);
        let node_behind = NOW_MS - 500 + MAX_PROGRESS_LAG_MS + 1;
        world.book.as_mut().unwrap().time = node_behind - 400;
        for second in 0..119 {
            assert_eq!(statuses(&world.snapshot_at(&mut state, node_behind, at(second)), "BTC"), (Some("nodeProgress"), Some("nodeProgress")));
        }
        world.book.as_mut().unwrap().time = NOW_MS - 400;
        let snapshot = world.snapshot_at(&mut state, NOW_MS, at(119));
        let basis = snapshot.coin("BTC").unwrap().local.as_ref().unwrap().external_basis.clone().unwrap();
        assert_eq!(basis.samples, 120);
        assert!((basis.ratio - (100_002.0 / 100_004.0 - 1.0)).abs() < 1e-15, "{}", basis.ratio);
        assert_eq!(state.external_basis[1].published().unwrap_err().detail, "0 of the 120 samples needed over the last 600 s", "ETH has no book");
    }

    /// Hyperliquid-trade design 000673 §4, r2: the external leg and its basis stand before the node's and the book's
    /// gates - the node behind withholds the local leg, not the perp quotes nor the basis, which publishes from its
    /// 120th sample -, the local leg carrying the same quotes once ready; the feeds stopped withhold the quotes, the
    /// basis still the window's.
    #[test]
    fn the_external_leg_and_its_basis_stand_apart_from_the_node() {
        let mut world = World::new();
        let mut state = world.owner_state();
        let start = Instant::now();
        let at = |second: u64| start + Duration::from_secs(second);
        let node_behind = NOW_MS - 500 + MAX_PROGRESS_LAG_MS + 1;
        world.book.as_mut().unwrap().time = node_behind - 400;
        let behind = (0..120).map(|second| world.snapshot_at(&mut state, node_behind, at(second))).last().unwrap();
        let btc = behind.coin("BTC").unwrap();
        assert_eq!(btc.local.as_ref().unwrap_err().code, "nodeProgress");
        let quotes = btc.external.as_ref().unwrap();
        assert_eq!((quotes.bid, quotes.ask), (price("100003.95"), price("100004.05")));
        assert_eq!(btc.external_basis.as_ref().unwrap().samples, 120);
        assert_eq!(btc.venue_bases.iter().map(|venue| (venue.venue, venue.basis.as_ref().unwrap().samples)).collect::<Vec<_>>(), [(Venue::Binance, 120)], "each venue's too (design 000681)");
        assert_eq!((behind.computed_at, behind.stopped.as_deref()), (at(119), None));
        world.book.as_mut().unwrap().time = NOW_MS - 400;
        let ready = world.snapshot_at(&mut state, NOW_MS, at(120));
        let btc = ready.coin("BTC").unwrap();
        let local = btc.local.as_ref().unwrap();
        assert_eq!((&local.external_quotes, &local.external_basis), (btc.external.as_ref().unwrap(), &btc.external_basis));
        let stopped = world.snapshot_stopped(NOW_MS, "the perp_okx task failed");
        let btc = stopped.coin("BTC").unwrap();
        assert_eq!((btc.external.as_ref().unwrap_err().code, stopped.stopped.as_deref()), ("stopped", Some("the perp_okx task failed")));
        assert_eq!(btc.external_basis.as_ref().unwrap_err().code, "externalBasisSamples", "the window's, not withheld for the stop");
        world.set(Feed { kind: Kind::Perp, venue: Venue::Binance }, "BTC", Validity::NotConnected);
        let none = world.snapshot(NOW_MS);
        assert_eq!(none.coin("BTC").unwrap().external.as_ref().unwrap_err().detail, "no perp venue quotes BTC now");
    }

    /// User 2026-11-02, review 000683 r2: BTC's one perp venue at 50,000 against HL's 100,002 - a basis of +100% -:
    /// `extMarkPrices`' local leg stays ready with its external quote, its `externalBasis` alone withheld
    /// (`externalBasisOutOfRange`), as the medians' and Binance's own in the snapshot; ETH's frame as it was.
    #[test]
    fn a_basis_past_ten_percent_is_withheld_in_the_local_leg() {
        let mut world = World::new();
        let eth_before = {
            let snapshot = world.snapshot(NOW_MS);
            super::super::prices_view::CoinMarksView::of(&snapshot, "ETH", snapshot.coin("ETH").unwrap()).to_wire()
        };
        world.quote(Feed { kind: Kind::Perp, venue: Venue::Binance }, "BTC", ("50000", "50001"), NOW_MS);
        let mut state = world.owner_state();
        let start = Instant::now();
        let snapshot = (0..120).map(|second| world.snapshot_at(&mut state, NOW_MS, start + Duration::from_secs(second))).last().unwrap();
        let btc = snapshot.coin("BTC").unwrap();
        assert_eq!((btc.external_basis.as_ref().unwrap_err().code, btc.venue_bases[0].basis.as_ref().unwrap_err().code), ("externalBasisOutOfRange", "externalBasisOutOfRange"));
        let frame: serde_json::Value = serde_json::from_slice(&super::super::prices_view::CoinMarksView::of(&snapshot, "BTC", btc).to_wire()).unwrap();
        let local = &frame["data"]["local"];
        assert_eq!((local["status"].as_str(), local["externalBidPx"].as_str()), (Some("ready"), Some("50000")), "{local}");
        assert_eq!((local["externalBasis"]["status"].as_str(), local["externalBasis"]["reasonCode"].as_str()), (Some("unavailable"), Some("externalBasisOutOfRange")));
        assert_eq!(super::super::prices_view::CoinMarksView::of(&snapshot, "ETH", snapshot.coin("ETH").unwrap()).to_wire(), eth_before);
    }

    /// Hyperliquid-trade design 000681: a venue's basis follows the median basis' rule - a sample a second, published
    /// from the 120th, the samples of the last 600 s -: BTC's one perp venue gives its medians' basis tick for tick,
    /// the ticks between the seconds sampling nothing.
    #[test]
    fn a_single_venues_basis_is_its_medians() {
        let world = World::new();
        let mut state = world.owner_state();
        let start = Instant::now();
        let mut last = None;
        for tick in 0..(120 * 4) {
            let snapshot = world.snapshot_at(&mut state, NOW_MS, start + Duration::from_millis(250 * tick));
            let btc = snapshot.coin("BTC").unwrap();
            assert_eq!(btc.venue_bases, [VenueBasis { venue: Venue::Binance, basis: btc.external_basis.clone() }], "tick {tick}");
            last = Some(btc.external_basis.clone());
        }
        assert_eq!(last.unwrap().unwrap().samples, 120, "a sample a second, 0 to 119 s");
    }

    /// Hyperliquid-trade design 000681, review 000681 r1-r2: each perp listing's venue keeps its own basis window,
    /// sampled from its own quote (its multiplier applied) while valid - Binance, OKX and Bybit each their own ratio
    /// from the 120th sample, the medians' their own. Binance gone for 601 s: its window empties - its samples leave
    /// though none enters - while OKX's and Bybit's keep theirs, never moved into Binance's place; back, Binance
    /// publishes again only from its 120th new sample.
    #[test]
    fn each_venue_keeps_its_own_basis_window() {
        let perp = |venue| Feed { kind: Kind::Perp, venue };
        let mut world = World::of(THREE_PERPS);
        world.quote(perp(Venue::Binance), "BTC", ("100003", "100005"), NOW_MS - 30);
        world.quote(perp(Venue::Okx), "BTC", ("100001", "100009"), NOW_MS - 20);
        world.quote(perp(Venue::Bybit), "BTC", ("100.006", "100.00624"), NOW_MS - 10);
        let mut state = world.owner_state();
        let start = Instant::now();
        let mut tick = |world: &World, seconds: std::ops::RangeInclusive<u64>| {
            seconds.map(|second| world.snapshot_at(&mut state, NOW_MS, start + Duration::from_secs(second))).last().unwrap()
        };
        // HL's book midpoint 100,002 over each venue's midpoint - 1
        let ratio = |mid: f64| 100_002.0 / mid - 1.0;
        let (binance, okx, bybit) = (ratio(100_004.0), ratio(100_005.0), ratio(f64::midpoint(100_006.0, 100_006.24)));
        let bases = |snapshot: &LocalMarksSnapshot| {
            snapshot.coin("BTC").unwrap().venue_bases.iter().map(|venue| (venue.venue, venue.basis.clone().map(|basis| (basis.ratio, basis.samples)).map_err(|withheld| withheld.detail))).collect::<Vec<_>>()
        };
        let close = |found: &Result<(f64, usize), String>, ratio: f64| found.as_ref().is_ok_and(|(found, _)| (found - ratio).abs() < 1e-12);
        let first = tick(&world, 0..=0);
        let one = Err("1 of the 120 samples needed over the last 600 s".to_string());
        assert_eq!(bases(&first), [(Venue::Binance, one.clone()), (Venue::Okx, one.clone()), (Venue::Bybit, one)]);
        let ready = bases(&tick(&world, 1..=119));
        assert!(ready.iter().all(|(_, basis)| basis.as_ref().is_ok_and(|(_, samples)| *samples == 120)), "{ready:?}");
        assert!(close(&ready[0].1, binance) && close(&ready[1].1, okx) && close(&ready[2].1, bybit), "{ready:?}");
        world.set(perp(Venue::Binance), "BTC", Validity::NotConnected);
        let gone = tick(&world, 120..=720);
        let gone_bases = bases(&gone);
        assert_eq!(gone_bases[0], (Venue::Binance, Err("0 of the 120 samples needed over the last 600 s".to_string())), "its last sample 601 s old");
        assert!(close(&gone_bases[1].1, okx) && close(&gone_bases[2].1, bybit), "{gone_bases:?}");
        assert_eq!(gone.coin("BTC").unwrap().external.as_ref().unwrap().venues.len(), 2, "OKX and Bybit quote");
        world.quote(perp(Venue::Binance), "BTC", ("100003", "100005"), NOW_MS - 30);
        assert_eq!(bases(&tick(&world, 721..=721))[0].1, Err("1 of the 120 samples needed over the last 600 s".to_string()));
        assert_eq!(bases(&tick(&world, 722..=839))[0].1, Err("119 of the 120 samples needed over the last 600 s".to_string()));
        let back = bases(&tick(&world, 840..=840));
        assert!(close(&back[0].1, binance) && back[0].1.as_ref().unwrap().1 == 120, "{back:?}");
        assert!(close(&back[1].1, okx) && close(&back[2].1, bybit), "{back:?}");
    }

    /// Review 000681 r2's cost: an owner tick over the embedded sources - 176 coins, 832 perp listings, each venue's
    /// window and each coin's medians' full, 600 samples -, every market quoting and the book read, the node state
    /// unavailable (no local leg computed); the ticks taking a sample (one a second) and the ticks between, and the
    /// 832 venue windows' round alone. Run alone, in release:
    /// `cargo test --release -p server --lib -- --ignored --nocapture an_owner_tick_over_832_full_venue_windows`.
    #[test]
    #[ignore = "a timing: run alone, in release"]
    fn an_owner_tick_over_832_full_venue_windows() {
        let sources = MarkSources::embedded().unwrap();
        let table = Arc::new(CoinTable::new(&sources, None, None));
        let listings: usize = (0..table.names.len()).map(|index| table.perp_markets(index).count()).sum();
        let quote = (Some(Entry::for_test(Some(Quote { bid: price("100"), ask: price("100.1") }), NOW_MS, 1)), Validity::Valid);
        let readings: Vec<_> = sources
            .feeds()
            .map(|(feed, markets)| (feed, markets, FeedProjection { connections: Vec::new(), messages: 1, entries: vec![quote; markets.symbols.len()] }))
            .collect();
        let node: NodePrices = Err("no node state in this timing".to_string());
        let mut state = table.owner_state();
        let start = Instant::now();
        let mut tick = |at: Duration, wobble: u64| {
            let bid = 10_000_000_000 + wobble * 1_000_000;
            let side = (Some(Px::new(bid)), Some(Px::new(bid + 20_000_000)));
            let book = BestPricesAt { time: NOW_MS, height: 1, trusted: true, prices: vec![side; table.book_coins().len()] };
            let tick = Tick { now_ms: NOW_MS, instant: start + at, stopped: None, node: &node, book: Some(&book), readings: &readings, equity: None, hip3_external: None };
            let began = std::time::Instant::now();
            let snapshot = assemble(&table, &tick, &mut state);
            (began.elapsed(), snapshot)
        };
        for second in 0..600 {
            tick(Duration::from_secs(second), second % 7);
        }
        let sampling: Vec<_> = (600..630).map(|second| tick(Duration::from_secs(second), second % 7)).collect();
        let full = sampling.last().unwrap().1.coins().all(|(_, marks)| marks.venue_bases.iter().all(|venue| venue.basis.as_ref().is_ok_and(|basis| basis.samples >= 600)));
        let between: Vec<_> = (1..20).map(|step| tick(Duration::from_secs(629) + Duration::from_millis(50 * step), 3).0).collect();
        let summary = |mut times: Vec<Duration>| {
            times.sort();
            format!("p50 {:?}, max {:?} over {}", times[times.len() / 2], times[times.len() - 1], times.len())
        };
        let mut windows = state.venue_basis.concat();
        let began = std::time::Instant::now();
        let round: Vec<_> = windows.iter_mut().map(|window| {
            window.observe(start + Duration::from_secs(631), Some(0.0001));
            window.published()
        }).collect();
        let round_time = began.elapsed();
        println!("{} coins, {listings} perp listings ({} venue windows), every window full: {full}", table.names.len(), windows.len());
        println!("owner tick taking a sample: {}", summary(sampling.into_iter().map(|(elapsed, _)| elapsed).collect()));
        println!("owner tick between samples: {}", summary(between));
        println!("the {} venue windows' round alone: {round_time:?} ({} published)", windows.len(), round.iter().filter(|basis| basis.is_ok()).count());
        assert_eq!((listings, full), (832, true));
    }

    /// Design 000594: no sample without the book fresh and trusted with both sides, a perp quote and the feeds running.
    #[test]
    fn the_basis_takes_no_sample_without_its_inputs() {
        let sampled = |world: &World, now_ms: u64| {
            let mut state = world.owner_state();
            world.snapshot_at(&mut state, now_ms, Instant::now());
            state.external_basis[0].published().unwrap_err().detail.starts_with("1 of")
        };
        assert!(sampled(&World::new(), NOW_MS));
        assert!(!sampled(&World::new(), NOW_MS - 400 + MAX_PROGRESS_LAG_MS + 1), "the book behind");
        let mut world = World::new();
        world.book.as_mut().unwrap().trusted = false;
        assert!(!sampled(&world, NOW_MS), "the book withheld after a data loss");
        let mut world = World::new();
        world.book.as_mut().unwrap().prices[0].0 = None;
        assert!(!sampled(&world, NOW_MS), "an empty side");
        let mut world = World::new();
        world.set(Feed { kind: Kind::Perp, venue: Venue::Binance }, "BTC", Validity::NotQuoted);
        assert!(!sampled(&world, NOW_MS), "no perp quote");
        let world = World::new();
        let readings: Vec<_> = world.sources.feeds().zip(&world.readings).map(|((feed, markets), reading)| (feed, markets, reading.clone())).collect();
        let tick = Tick { now_ms: NOW_MS, instant: Instant::now(), stopped: Some("gone".to_string()), node: &world.node, book: world.book.as_ref(), readings: &readings, equity: None, hip3_external: None };
        let mut state = world.owner_state();
        assemble(&world.table, &tick, &mut state);
        assert!(state.external_basis[0].published().unwrap_err().detail.starts_with("0 of"), "the feeds stopped");
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
        Arc::make_mut(node).coins.get_mut("BTC").unwrap().mark_cache.as_mut().unwrap().median = None;
        assert_eq!(statuses(&world.snapshot(NOW_MS), "BTC"), (None, Some("markCache")));
    }

    #[test]
    fn submitted_node_prices_remain_visible_but_do_not_claim_an_ordinary_local_prediction() {
        let mut world = World::new();
        let Ok(node) = &mut world.node else { unreachable!() };
        let state = Arc::make_mut(node).coins.get_mut("BTC").unwrap();
        use crate::accounts::marks::seed::{ReferenceMode, ReferenceModes};
        state.reference_modes = ReferenceModes { spot: ReferenceMode::Submitted, external_perp: ReferenceMode::Submitted, mark: ReferenceMode::Submitted };
        state.spot_cache = None; state.mark_cache = None;
        let snapshot = world.snapshot(NOW_MS);
        assert_eq!(statuses(&snapshot, "BTC"), (None, Some("referenceMode")));
        let node = snapshot.coin("BTC").unwrap().node.as_ref().unwrap();
        assert_eq!((node.spot_cache, node.spot_cache_next_refresh_ms, node.mark_cache, node.mark_cache_next_refresh_ms), (None, None, None, None));
        let row = crate::local_marks::prices_view::CoinCachesView::of(&snapshot, "BTC", snapshot.coin("BTC").unwrap());
        let wire = serde_json::to_value(row).unwrap();
        assert_eq!(wire["node"]["status"], "ready");
        assert!(wire["node"]["spotCache"].is_null() && wire["node"]["markCache"].is_null());
    }

    #[test]
    fn book_prices_convert_to_price_units_exactly() {
        assert_eq!(book_units(Px::new(10_000_100_000_000), 5), Ok(1_000_010), "100,001.0 in 0.1");
        assert_eq!(book_units(Px::new(1_234_500), 0), Ok(12_345), "0.012345 with six decimals");
        assert_eq!(book_units(Px::new(1_234_567), 0).unwrap_err().code, "hlBook");
    }
}
