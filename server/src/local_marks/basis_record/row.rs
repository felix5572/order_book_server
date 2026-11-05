//! One tick's rows (hyperliquid-trade design 000751 r2 rule 4, r3): a JSON line a coin - every main-dex coin of the
//! sources, every HIP-3 coin of the external quotes' channel, and HYPE.
//!
//! Prices are decimal texts. A venue's prices are its own contract's, native - straight from the feeds' readings, as
//! the venue wrote them - beside the listing's `px_multiplier` (HL price = multiplier × venue price; review 000751 r1
//! P2): a reader multiplies to compare with HL, and the file explains itself without a later build's tables. HL's are
//! its own: the book's best prices of the tick (1e-8) and the node's in the coin's price units. A value there is none
//! of is `{"code", "detail"}` - why: withheld by the local marks, not received, a stopped source - never a zero or an
//! older value; each value carries its own time, judged offline (the record only records).

use std::collections::BTreeMap;

use rustc_hash::FxHashMap;
use serde::Serialize;

use super::{
    Latest,
    binance::{BinanceLatest, BookTicker, HYPE_SYMBOL, MarkPrice, USDC_USDT_SYMBOL},
    pyth::{PythLatest, PythPrice},
};
use crate::{
    accounts::marks::{decimal::ExactDecimal, native::CoinPriceState, seed::Hip3Coin},
    listeners::order_book::BestPricesAt,
    local_marks::{
        external_basis::ExternalBasis,
        hip3::{external::Hip3ExternalBases, protocol::EquityFeed},
        price::VenuePrice,
        prices_view::price,
        slot::{Entry, Validity},
        snapshot::{CoinMarks, LocalLeg, LocalMarksSnapshot, Withheld},
        sources::Feed,
        subscriptions::FeedProjection,
    },
};

/// HL's book prices are 1e-8 (`order_book::types`).
const BOOK_DECIMALS: u32 = 8;
pub(crate) const HYPE: &str = "HYPE";
/// The quote currency of HL's prices: the main dex's coins of the sources are USDT-denominated, USDC-margined (HL's
/// contract specifications); HYPE, priced on HL's own USDC spot, and the HIP-3 dexes' are USDC.
const MAIN_UNIT: &str = "USDT";
const USDC_UNIT: &str = "USDC";

/// A value, or why there is none.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub(crate) enum Field<T> {
    Known(T),
    Missing { code: &'static str, detail: String },
}

impl<T> Field<T> {
    fn missing(code: &'static str, detail: impl Into<String>) -> Self {
        Self::Missing { code, detail: detail.into() }
    }

    fn withheld(withheld: &Withheld) -> Self {
        Self::missing(withheld.code, withheld.detail.clone())
    }

    fn of<S>(latest: Latest<S>, view: impl FnOnce(S) -> T) -> Self {
        match latest {
            Latest::Received(value) => Self::Known(view(value)),
            Latest::NotReceived => Self::missing("notReceived", ""),
            Latest::Stopped(reason) => Self::missing("stopped", reason),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) struct NodeProgressView {
    height: u64,
    block_time_ms: u64,
}

/// The HL book's best prices of the coin: `null` an empty side.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct BookView {
    bid: Option<String>,
    ask: Option<String>,
    time_ms: u64,
    height: u64,
    trusted: bool,
}

/// The node's prices of the coin, in its price units turned decimal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub(crate) enum NodeView {
    /// A main-dex coin's (HYPE's too): the stored oracle and mark, and the last `SetGlobalAction`'s (oracle, mark).
    Main { oracle: String, mark: String, mark_time_ns: u64, submitted_oracle: Option<String>, submitted_mark: Option<String> },
    /// A HIP-3 coin's: the relayer's oracle and when it wrote it, the stored mark, the stored external perp price.
    Hip3 { oracle: Option<String>, oracle_time_ns: Option<u64>, mark: Option<String>, mark_time_ns: Option<u64>, external: Option<String> },
}

/// The local leg's marks (design 000454): what the local marks compute of the coin now.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct LocalMarkView {
    oracle: String,
    basis_adjusted_oracle: String,
    local: String,
    external: String,
    reference: String,
    submitted: String,
    predicted: String,
    basis_ratio: f64,
    basis_samples: u64,
}

/// A venue's market of the coin as its feed reads now: native prices, the listing's multiplier, the reading's status
/// (`valid`, `notQuoted`, `noQuote`, `notConnected`, `expired`, `stopped`), the quote's receipt and venue times.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct VenueView {
    feed: &'static str,
    symbol: String,
    px_multiplier: String,
    status: &'static str,
    bid: Option<String>,
    ask: Option<String>,
    received_ms: Option<u64>,
    event_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub(crate) struct BasisView {
    ratio: f64,
    samples: usize,
}

/// Binance's mark price stream of the coin's contract (native).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct BinanceView {
    symbol: String,
    px_multiplier: String,
    mark: String,
    index: String,
    funding_rate: String,
    next_funding_ms: u64,
    event_ms: u64,
    received_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct SpotBookView {
    bid: String,
    ask: String,
    received_ms: u64,
}

/// A Pyth price as Hermes wrote it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct PythView {
    price: String,
    conf: String,
    expo: i32,
    publish_time_s: u64,
    received_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct Row {
    t_ms: u64,
    coin: String,
    dex: String,
    hl_price_unit: &'static str,
    node_progress: Field<NodeProgressView>,
    hl_book: Field<BookView>,
    node: Field<NodeView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    local_mark: Option<Field<LocalMarkView>>,
    venues: Vec<VenueView>,
    bases: BTreeMap<String, Field<BasisView>>,
    binance: Field<BinanceView>,
    usdc_usdt: Field<SpotBookView>,
    pyth_usdt_usd: Field<PythView>,
    pyth_usdc_usd: Field<PythView>,
}

/// A contract of a coin: its symbol and multiplier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Listed {
    pub(crate) symbol: String,
    pub(crate) multiplier: VenuePrice,
}

/// One feed's reading: its markets' symbols and (coin, multiplier), in the projection's order.
pub(crate) struct GroupReading<'a> {
    pub(crate) feed: Feed,
    pub(crate) symbols: &'a [String],
    pub(crate) coins: &'a [(String, VenuePrice)],
    pub(crate) projection: FeedProjection,
}

/// What one tick's rows read.
pub(crate) struct Inputs<'a> {
    pub(crate) snapshot: &'a LocalMarksSnapshot,
    /// Each coin's place in the snapshot's book prices.
    pub(crate) book_index: &'a FxHashMap<String, usize>,
    pub(crate) main_feeds: Vec<GroupReading<'a>>,
    pub(crate) hip3_feeds: Vec<GroupReading<'a>>,
    pub(crate) main_binance: &'a FxHashMap<String, Listed>,
    pub(crate) hip3_binance: &'a FxHashMap<String, Listed>,
    /// HYPE's book, read by the record itself (HYPE is not in the owner's book read).
    pub(crate) hype_book: Option<&'a BestPricesAt>,
    pub(crate) binance: &'a BinanceLatest,
    pub(crate) pyth: &'a PythLatest,
}

/// The values every row carries.
struct Common {
    t_ms: u64,
    node_progress: Field<NodeProgressView>,
    usdc_usdt: Field<SpotBookView>,
    pyth_usdt_usd: Field<PythView>,
    pyth_usdc_usd: Field<PythView>,
}

pub(crate) fn rows(inputs: &Inputs<'_>) -> Vec<Row> {
    let snapshot = inputs.snapshot;
    let common = Common {
        t_ms: snapshot.computed_ms,
        node_progress: match &snapshot.node {
            Ok(progress) => Field::Known(NodeProgressView { height: progress.height, block_time_ms: progress.block_time_ms }),
            Err(reason) => Field::missing("node", reason.clone()),
        },
        usdc_usdt: Field::of(inputs.binance.book(USDC_USDT_SYMBOL), |book: BookTicker| SpotBookView {
            bid: book.bid.to_string(),
            ask: book.ask.to_string(),
            received_ms: book.received_ms,
        }),
        pyth_usdt_usd: Field::of(inputs.pyth.usdt_usd(), pyth_view),
        pyth_usdc_usd: Field::of(inputs.pyth.usdc_usd(), pyth_view),
    };
    let mut main_venues = venues_by_coin(&inputs.main_feeds);
    let mut hip3_venues = venues_by_coin(&inputs.hip3_feeds);
    let mut rows: Vec<Row> = snapshot.coins().map(|(coin, marks)| main_row(inputs, &common, coin, marks, main_venues.remove(coin).unwrap_or_default())).collect();
    if let Some(external) = snapshot.hip3_external() {
        for ((coin, _), bases) in snapshot.hip3_external_coins().zip(&external.bases) {
            rows.push(hip3_row(inputs, &common, coin, bases, hip3_venues.remove(coin).unwrap_or_default()));
        }
    }
    rows.push(hype_row(inputs, &common));
    rows
}

fn row(common: &Common, coin: &str, dex: &str, hl_price_unit: &'static str) -> Row {
    Row {
        t_ms: common.t_ms,
        coin: coin.to_string(),
        dex: dex.to_string(),
        hl_price_unit,
        node_progress: common.node_progress.clone(),
        hl_book: Field::missing("unset", ""),
        node: Field::missing("unset", ""),
        local_mark: None,
        venues: Vec::new(),
        bases: BTreeMap::new(),
        binance: Field::missing("noBinancePerp", "no Binance perp contract of the coin in the sources"),
        usdc_usdt: common.usdc_usdt.clone(),
        pyth_usdt_usd: common.pyth_usdt_usd.clone(),
        pyth_usdc_usd: common.pyth_usdc_usd.clone(),
    }
}

fn main_row(inputs: &Inputs<'_>, common: &Common, coin: &str, marks: &CoinMarks, venues: Vec<VenueView>) -> Row {
    let snapshot = inputs.snapshot;
    let mut bases = BTreeMap::from([("external_10m".to_string(), basis(&marks.external_basis))]);
    for venue in &marks.venue_bases {
        bases.insert(format!("perp_{}_10m", venue.venue.name()), basis(&venue.basis));
    }
    Row {
        hl_book: book_view(snapshot.book_prices(), inputs.book_index.get(coin).copied()),
        node: main_node(snapshot, coin),
        local_mark: Some(marks.local.as_ref().map_or_else(Field::withheld, |leg| Field::Known(local_view(leg)))),
        venues,
        bases,
        binance: binance_view(inputs.binance, inputs.main_binance.get(coin)),
        ..row(common, coin, "main", MAIN_UNIT)
    }
}

fn hip3_row(inputs: &Inputs<'_>, common: &Common, coin: &str, bases: &Hip3ExternalBases, venues: Vec<VenueView>) -> Row {
    let snapshot = inputs.snapshot;
    let unlisted = || Field::missing("unlisted", "the source's table lists no market of the coin");
    let mut out = BTreeMap::new();
    match &bases.binance {
        Some(binance) => {
            out.insert("binance_10m".to_string(), basis(&binance.near));
            out.insert("binance_60m".to_string(), basis(&binance.longer.hour));
            out.insert("binance_240m".to_string(), basis(&binance.longer.four_hours));
        }
        None => {
            out.insert("binance_10m".to_string(), unlisted());
        }
    }
    for (feed, key) in [(EquityFeed::Sip, "sip_10m"), (EquityFeed::Boats, "boats_10m")] {
        out.insert(key.to_string(), bases.equity.as_ref().map_or_else(unlisted, |equity| basis(equity.of(feed))));
    }
    let dex = coin.split_once(':').map_or(coin, |(dex, _)| dex);
    Row {
        hl_book: book_view(snapshot.book_prices(), inputs.book_index.get(coin).copied()),
        node: snapshot.hip3_node_state(coin).map_or_else(|| no_node(snapshot), |state| Field::Known(hip3_node(state))),
        venues,
        bases: out,
        binance: binance_view(inputs.binance, inputs.hip3_binance.get(coin)),
        ..row(common, coin, dex, USDC_UNIT)
    }
}

fn hype_row(inputs: &Inputs<'_>, common: &Common) -> Row {
    let listed = Listed { symbol: HYPE_SYMBOL.to_string(), multiplier: VenuePrice::ONE };
    let venue = match inputs.binance.book(HYPE_SYMBOL) {
        Latest::Received(book) => VenueView {
            bid: Some(book.bid.to_string()),
            ask: Some(book.ask.to_string()),
            received_ms: Some(book.received_ms),
            event_ms: book.event_ms,
            ..hype_venue("valid")
        },
        Latest::NotReceived => hype_venue(Validity::NotQuoted.name()),
        Latest::Stopped(_) => hype_venue("stopped"),
    };
    Row {
        hl_book: book_view(inputs.hype_book, Some(0)),
        node: main_node(inputs.snapshot, HYPE),
        venues: vec![venue],
        binance: binance_view(inputs.binance, Some(&listed)),
        ..row(common, HYPE, "main", USDC_UNIT)
    }
}

fn hype_venue(status: &'static str) -> VenueView {
    VenueView {
        feed: "perp_binance",
        symbol: HYPE_SYMBOL.to_string(),
        px_multiplier: VenuePrice::ONE.to_string(),
        status,
        bid: None,
        ask: None,
        received_ms: None,
        event_ms: None,
    }
}

/// Every reading's markets by coin, each as its feed reads it.
fn venues_by_coin<'a>(groups: &'a [GroupReading<'a>]) -> FxHashMap<&'a str, Vec<VenueView>> {
    let mut by_coin: FxHashMap<&str, Vec<VenueView>> = FxHashMap::default();
    for group in groups {
        let markets = group.symbols.iter().zip(group.coins).zip(&group.projection.entries);
        for ((symbol, (coin, multiplier)), (entry, validity)) in markets {
            by_coin.entry(coin.as_str()).or_default().push(venue_view(group.feed, symbol, *multiplier, entry.as_ref(), *validity));
        }
    }
    by_coin
}

/// A market's view: its prices only while its reading is valid - a slot keeps an entry past a lost connection or its
/// expiry, invalid then (review 000752 r1 P2) -; the entry's times whatever the status, to tell why.
fn venue_view(feed: Feed, symbol: &str, multiplier: VenuePrice, entry: Option<&Entry>, validity: Validity) -> VenueView {
    let quote = entry.filter(|_| validity == Validity::Valid).and_then(|entry| entry.quote);
    VenueView {
        feed: feed.label(),
        symbol: symbol.to_string(),
        px_multiplier: multiplier.to_string(),
        status: validity.name(),
        bid: quote.map(|quote| quote.bid.to_string()),
        ask: quote.map(|quote| quote.ask.to_string()),
        received_ms: entry.map(|entry| entry.received_ms),
        event_ms: entry.and_then(|entry| entry.event_ms),
    }
}

fn basis(basis: &Result<ExternalBasis, Withheld>) -> Field<BasisView> {
    basis.as_ref().map_or_else(Field::withheld, |basis| Field::Known(BasisView { ratio: basis.ratio, samples: basis.samples }))
}

fn book_view(book: Option<&BestPricesAt>, index: Option<usize>) -> Field<BookView> {
    let Some(book) = book else { return Field::missing("hlBook", "no book snapshot yet") };
    let Some((bid, ask)) = index.and_then(|index| book.prices.get(index)) else { return Field::missing("hlBook", "the coin is not in the book read") };
    let px = |side: &Option<crate::order_book::Px>| side.map(|px| ExactDecimal::from_units(px.value(), BOOK_DECIMALS).to_string());
    Field::Known(BookView { bid: px(bid), ask: px(ask), time_ms: book.time, height: book.height, trusted: book.trusted })
}

fn no_node(snapshot: &LocalMarksSnapshot) -> Field<NodeView> {
    match &snapshot.node {
        Err(reason) => Field::missing("node", reason.clone()),
        Ok(_) => Field::missing("nodeCoin", "the node prices of the tick hold no such coin"),
    }
}

fn main_node(snapshot: &LocalMarksSnapshot, coin: &str) -> Field<NodeView> {
    let Some(state) = snapshot.node_state(coin) else { return no_node(snapshot) };
    Field::Known(main_node_view(state))
}

fn main_node_view(state: &CoinPriceState) -> NodeView {
    let px = |units| price(units, state.size_decimals);
    NodeView::Main {
        oracle: px(state.stored_oracle),
        mark: px(state.stored_mark),
        mark_time_ns: state.mark_time_ns,
        submitted_oracle: state.submitted.map(|(oracle, _)| px(oracle)),
        submitted_mark: state.submitted.map(|(_, mark)| px(mark)),
    }
}

fn hip3_node(state: &Hip3Coin) -> NodeView {
    let px = |units| price(units, state.size_decimals);
    NodeView::Hip3 {
        oracle: state.oracle.map(px),
        oracle_time_ns: state.oracle_time_ns,
        mark: state.mark.map(|mark| px(mark.px)),
        mark_time_ns: state.mark.map(|mark| mark.time_ns),
        external: state.external.map(px),
    }
}

fn local_view(leg: &LocalLeg) -> LocalMarkView {
    let px = |units| price(units, leg.size_decimals);
    let mark = &leg.mark;
    LocalMarkView {
        oracle: px(mark.oracle),
        basis_adjusted_oracle: px(mark.basis_adjusted_oracle),
        local: px(mark.local),
        external: px(mark.external),
        reference: px(mark.reference),
        submitted: px(mark.submitted),
        predicted: px(mark.predicted),
        basis_ratio: mark.basis_ratio,
        basis_samples: mark.basis_samples,
    }
}

fn binance_view(latest: &BinanceLatest, listed: Option<&Listed>) -> Field<BinanceView> {
    let Some(listed) = listed else { return Field::missing("noBinancePerp", "no Binance perp contract of the coin in the sources") };
    Field::of(latest.mark(&listed.symbol), |mark: MarkPrice| BinanceView {
        symbol: listed.symbol.clone(),
        px_multiplier: listed.multiplier.to_string(),
        mark: mark.mark.to_string(),
        index: mark.index.to_string(),
        funding_rate: mark.funding_rate,
        next_funding_ms: mark.next_funding_ms,
        event_ms: mark.event_ms,
        received_ms: mark.received_ms,
    })
}

fn pyth_view(price: PythPrice) -> PythView {
    PythView { price: price.price, conf: price.conf, expo: price.expo, publish_time_s: price.publish_time_s, received_ms: price.received_ms }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::local_marks::{
        slot::Quote,
        snapshot::tests::{NOW_MS, World},
        sources::{Kind, Venue},
    };

    fn price(text: &str) -> VenuePrice {
        VenuePrice::parse(text).unwrap()
    }

    fn json<T: Serialize>(value: &T) -> serde_json::Value {
        serde_json::to_value(value).unwrap()
    }

    /// Review 000751 r1 P2: a venue's quote is its own contract's, beside the multiplier - kNEIRO is 1,000 NEIRO, so
    /// NEIROUSDT's 0.001 is recorded as 0.001, not HL's 1; a market not quoting has no price, not a zero.
    #[test]
    fn venues_are_native_beside_their_multiplier() {
        let symbols = ["NEIROUSDT".to_string(), "QUIETUSDT".to_string(), "LOSTUSDT".to_string()];
        let coins = [("kNEIRO".to_string(), price("1000")), ("QUIET".to_string(), price("1")), ("LOST".to_string(), price("1"))];
        let quote = Quote { bid: price("0.001"), ask: price("0.0010002") };
        let entries = vec![
            (Some(Entry::for_test_at(Some(quote), 5, Some(4), 1)), Validity::Valid),
            (None, Validity::NotQuoted),
            (Some(Entry::for_test_at(Some(quote), 3, Some(2), 1)), Validity::NotConnected),
        ];
        let group = GroupReading { feed: Feed { kind: Kind::Perp, venue: Venue::Binance }, symbols: &symbols, coins: &coins, projection: FeedProjection { connections: Vec::new(), messages: 1, entries } };
        let groups = [group];
        let by_coin = venues_by_coin(&groups);
        assert_eq!(
            json(&by_coin["kNEIRO"][0]),
            serde_json::json!({"feed": "perp_binance", "symbol": "NEIROUSDT", "px_multiplier": "1000", "status": "valid", "bid": "0.001",
                "ask": "0.0010002", "received_ms": 5, "event_ms": 4})
        );
        assert_eq!(
            json(&by_coin["QUIET"][0]),
            serde_json::json!({"feed": "perp_binance", "symbol": "QUIETUSDT", "px_multiplier": "1", "status": "notQuoted", "bid": null,
                "ask": null, "received_ms": null, "event_ms": null})
        );
        // review 000752 r1 P2: an entry kept past its connection's loss is no price, its times still there
        assert_eq!(
            json(&by_coin["LOST"][0]),
            serde_json::json!({"feed": "perp_binance", "symbol": "LOSTUSDT", "px_multiplier": "1", "status": "notConnected", "bid": null,
                "ask": null, "received_ms": 3, "event_ms": 2})
        );
    }

    fn mark(symbol_mark: &str, index: &str) -> MarkPrice {
        MarkPrice { mark: price(symbol_mark), index: price(index), funding_rate: "-0.0001".into(), next_funding_ms: 9, event_ms: 7, received_ms: 8 }
    }

    /// A tick's rows: each main-dex coin of the sources, then HYPE; what is known with its time, what is not with why.
    #[test]
    fn a_tick_rows_every_coin_and_hype() {
        let world = World::new();
        let snapshot = world.snapshot(NOW_MS).with_book_prices(world.book.clone().map(Arc::new));
        let book_index: FxHashMap<String, usize> = snapshot.table().book_coins().iter().enumerate().map(|(index, coin)| (coin.value(), index)).collect();
        let readings: Vec<_> = world.sources.feeds().collect();
        let main_feeds = readings
            .iter()
            .map(|(feed, markets)| {
                let entries = markets.coins.iter().map(|_| (Some(Entry::for_test(Some(Quote { bid: price("100000"), ask: price("100000.2") }), NOW_MS, 1)), Validity::Valid)).collect();
                GroupReading { feed: *feed, symbols: &markets.symbols, coins: &markets.coins, projection: FeedProjection { connections: Vec::new(), messages: 1, entries } }
            })
            .collect();
        let main_binance = FxHashMap::from_iter([
            ("BTC".to_string(), Listed { symbol: "BTCUSDT".into(), multiplier: VenuePrice::ONE }),
            ("ETH".to_string(), Listed { symbol: "ETHUSDT".into(), multiplier: VenuePrice::ONE }),
        ]);
        let binance = BinanceLatest::default();
        binance.set_mark_for_test("BTCUSDT", mark("100002", "100001.5"));
        let pyth = PythLatest::new();
        let empty = FxHashMap::default();
        let inputs = Inputs {
            snapshot: &snapshot,
            book_index: &book_index,
            main_feeds,
            hip3_feeds: Vec::new(),
            main_binance: &main_binance,
            hip3_binance: &empty,
            hype_book: None,
            binance: &binance,
            pyth: &pyth,
        };
        let rows = rows(&inputs);
        assert_eq!(rows.iter().map(|row| row.coin.as_str()).collect::<Vec<_>>(), ["BTC", "ETH", "HYPE"]);
        let btc = json(&rows[0]);
        assert_eq!((btc["t_ms"].as_u64(), btc["dex"].as_str(), btc["hl_price_unit"].as_str()), (Some(NOW_MS), Some("main"), Some("USDT")));
        assert_eq!(btc["hl_book"], serde_json::json!({"bid": "100001", "ask": "100003", "time_ms": NOW_MS - 400, "height": 1_001, "trusted": true}));
        assert_eq!(btc["node_progress"], serde_json::json!({"height": 1_000, "block_time_ms": NOW_MS - 500}));
        assert!(btc["node"]["oracle"].is_string() && btc["node"]["mark"].is_string(), "{}", btc["node"]);
        assert!(btc["local_mark"]["oracle"].is_string() || btc["local_mark"]["code"].is_string(), "{}", btc["local_mark"]);
        let feeds: Vec<&str> = btc["venues"].as_array().unwrap().iter().map(|venue| venue["feed"].as_str().unwrap()).collect();
        assert_eq!(feeds, ["spot_binance", "spot_okx", "perp_binance"]);
        assert_eq!(btc["bases"]["external_10m"]["code"], "externalBasisSamples", "one tick: too few samples, withheld");
        assert!(btc["bases"]["perp_binance_10m"].is_object());
        assert_eq!(
            btc["binance"],
            serde_json::json!({"symbol": "BTCUSDT", "px_multiplier": "1", "mark": "100002", "index": "100001.5", "funding_rate": "-0.0001",
                "next_funding_ms": 9, "event_ms": 7, "received_ms": 8})
        );
        assert_eq!(btc["usdc_usdt"], serde_json::json!({"code": "notReceived", "detail": ""}));
        assert_eq!(btc["pyth_usdt_usd"], serde_json::json!({"code": "notReceived", "detail": ""}));
        let eth = json(&rows[1]);
        assert_eq!(eth["node"]["code"], "nodeCoin", "the node holds BTC only");
        assert_eq!(eth["binance"]["code"], "notReceived");
        let hype = json(&rows[2]);
        assert_eq!((hype["dex"].as_str(), hype["hl_price_unit"].as_str()), (Some("main"), Some("USDC")));
        assert_eq!(hype["hl_book"]["code"], "hlBook");
        assert_eq!(hype["node"]["code"], "nodeCoin");
        assert_eq!(hype["venues"], serde_json::json!([{"feed": "perp_binance", "symbol": "HYPEUSDT", "px_multiplier": "1", "status": "notQuoted",
            "bid": null, "ask": null, "received_ms": null, "event_ms": null}]));
        assert!(hype.get("local_mark").is_none(), "no local leg of HYPE");
    }

    /// A HIP-3 coin's row: its dex, USDC, the Binance leg's three bases each as published, a leg its table does not list
    /// `unlisted`; no node state of the coin, `nodeCoin`.
    #[test]
    fn a_hip3_row_carries_its_legs_bases() {
        use crate::local_marks::{
            external_basis::LongerBases,
            hip3::external::BinanceBases,
            snapshot::Status,
        };
        let world = World::new();
        let snapshot = world.snapshot(NOW_MS);
        let withheld = Withheld { status: Status::Unavailable, code: "externalBasisSamples", detail: "59 of the 60 samples".into() };
        let bases = Hip3ExternalBases {
            binance: Some(BinanceBases {
                near: Ok(ExternalBasis { ratio: -0.0012, samples: 600 }),
                longer: LongerBases { hour: Ok(ExternalBasis { ratio: -0.0011, samples: 360 }), four_hours: Err(withheld) },
            }),
            equity: None,
        };
        let empty = FxHashMap::default();
        let binance = BinanceLatest::default();
        binance.set_mark_for_test("SNDKUSDT", mark("1008.69584924", "1008.16911533"));
        let hip3_binance = FxHashMap::from_iter([("xyz:SNDK".to_string(), Listed { symbol: "SNDKUSDT".into(), multiplier: VenuePrice::ONE })]);
        let pyth = PythLatest::new();
        let inputs = Inputs {
            snapshot: &snapshot,
            book_index: &empty,
            main_feeds: Vec::new(),
            hip3_feeds: Vec::new(),
            main_binance: &FxHashMap::default(),
            hip3_binance: &hip3_binance,
            hype_book: None,
            binance: &binance,
            pyth: &pyth,
        };
        let common = Common { t_ms: 1, node_progress: Field::missing("node", "x"), usdc_usdt: Field::missing("notReceived", ""), pyth_usdt_usd: Field::missing("notReceived", ""), pyth_usdc_usd: Field::missing("notReceived", "") };
        let row = json(&hip3_row(&inputs, &common, "xyz:SNDK", &bases, Vec::new()));
        assert_eq!((row["dex"].as_str(), row["hl_price_unit"].as_str()), (Some("xyz"), Some("USDC")));
        assert_eq!(
            row["bases"],
            serde_json::json!({
                "binance_10m": {"ratio": -0.0012, "samples": 600},
                "binance_60m": {"ratio": -0.0011, "samples": 360},
                "binance_240m": {"code": "externalBasisSamples", "detail": "59 of the 60 samples"},
                "sip_10m": {"code": "unlisted", "detail": "the source's table lists no market of the coin"},
                "boats_10m": {"code": "unlisted", "detail": "the source's table lists no market of the coin"},
            })
        );
        assert_eq!(row["node"]["code"], "nodeCoin");
        assert_eq!(row["hl_book"]["code"], "hlBook");
        assert_eq!(row["binance"]["index"], "1008.16911533");
        assert!(row.get("local_mark").is_none());
    }
}
