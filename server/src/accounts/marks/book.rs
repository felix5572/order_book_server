//! The main dex's resting orders as the node's raw book diffs move them (design 000439 r2): seeded with a checkpoint's
//! orders (equal to the node's L4 export order by order, research `62149c8e`), then moved by each block's diffs in the
//! node's execution order (research `hl_node_mark_state_20261019` method 1: four coins replayed between checkpoints
//! equal the later one order by order, three intervals). A diff that does not fit the book breaks the contract.
//!
//! Prices are exact decimals; sizes are printed from binary floating point like the fills' (`2460032.6000000001`, W,
//! block 1190760017 on bm 2026-10-20), so they go through the fills' rule, `size::parse_size_raw`.

use std::collections::BTreeMap;

use rustc_hash::{FxHashMap, FxHashSet};

use super::{
    decimal::ExactDecimal,
    seed::{NativeSeed, RestingOrder},
};
use crate::{
    accounts::{checkpoint::MAX_PERP_DECIMALS, size::parse_size_raw},
    order_book::Side,
    types::{OrderDiff, node_data::NodeDataOrderDiff},
};

/// Mismatches listed per check (all are counted).
const MAX_SAMPLES: usize = 20;

/// Whether `coin` names a main-dex perp: spot pairs are `@N` or `PURR/USDC`, outcomes `#N`, HIP-3 perps `dex:NAME`.
pub(crate) fn is_main_perp(coin: &str) -> bool {
    !coin.contains(':') && !coin.contains('/') && !coin.starts_with('@') && !coin.starts_with('#')
}

/// One raw book diff of a main-dex perp, its values still as written: their units need the coin's `szDecimals`, which
/// only a seed's universe gives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BookDiff {
    coin: Box<str>,
    oid: u64,
    side: Side,
    px: ExactDecimal,
    change: BookChange,
}

/// Sizes as the node printed them.
#[derive(Debug, Clone, PartialEq, Eq)]
enum BookChange {
    New { size: Box<str> },
    Update { from: Box<str>, to: Box<str> },
    Remove,
}

/// What a diff line does, as the execution points read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DiffKind {
    New,
    /// `to_zero`: the order's remaining size went to zero (a fill took it all; its `remove` follows).
    Update { to_zero: bool },
    Remove,
}

/// The best bid and ask; `None` for an empty side.
pub(crate) type BestPrices = (Option<u64>, Option<u64>);

/// What applying a diff did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Applied {
    /// It moved the book of this asset index.
    Book(usize),
    /// A spot, outcome or HIP-3 market: the marks keep no book of it.
    OtherMarket,
    /// A main-dex perp outside the seed's universe (listed since).
    UnknownCoin,
}

impl BookDiff {
    pub(crate) const fn oid(&self) -> u64 {
        self.oid
    }

    pub(crate) fn coin(&self) -> &str {
        &self.coin
    }

    /// An update's (from, to) sizes as the node printed them.
    pub(crate) fn update_sizes(&self) -> Option<(&str, &str)> {
        match &self.change {
            BookChange::Update { from, to } => Some((from, to)),
            _ => None,
        }
    }

    pub(crate) fn kind(&self) -> DiffKind {
        match &self.change {
            BookChange::New { .. } => DiffKind::New,
            BookChange::Update { to, .. } => DiffKind::Update { to_zero: to.bytes().all(|byte| matches!(byte, b'0' | b'.')) },
            BookChange::Remove => DiffKind::Remove,
        }
    }

    pub(crate) fn of(diff: &NodeDataOrderDiff) -> Result<Self, String> {
        let change = match diff.diff() {
            OrderDiff::New { sz, .. } => BookChange::New { size: sz.as_str().into() },
            OrderDiff::Update { orig_sz, new_sz } => BookChange::Update { from: orig_sz.as_str().into(), to: new_sz.as_str().into() },
            OrderDiff::Remove => BookChange::Remove,
        };
        let px = ExactDecimal::parse(diff.px())?;
        Ok(Self { coin: diff.coin_name().into(), oid: diff.oid().value(), side: diff.side(), px, change })
    }
}

#[derive(Debug, Clone)]
struct AssetBook {
    size_decimals: u32,
    orders: FxHashMap<u64, RestingOrder>,
    /// Resting orders per price level: a level stays while any order rests there, whatever its size (research
    /// `execution_point_replay.py` `Books`).
    bids: BTreeMap<u64, u32>,
    asks: BTreeMap<u64, u32>,
}

impl AssetBook {
    const fn levels(&mut self, side: Side) -> &mut BTreeMap<u64, u32> {
        match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        }
    }

    fn add_level(&mut self, order: &RestingOrder) {
        *self.levels(order.side).entry(order.limit_px).or_default() += 1;
    }

    fn drop_level(&mut self, order: &RestingOrder) {
        let levels = self.levels(order.side);
        if let Some(count) = levels.get_mut(&order.limit_px) {
            *count -= 1;
            if *count == 0 {
                levels.remove(&order.limit_px);
            }
        }
    }

    fn best(&self) -> BestPrices {
        (self.bids.last_key_value().map(|(px, _)| *px), self.asks.first_key_value().map(|(px, _)| *px))
    }
}

/// Every main-dex perp's resting orders, by asset index.
#[derive(Debug, Clone)]
pub(crate) struct NativeBooks {
    by_coin: FxHashMap<Box<str>, usize>,
    assets: Vec<AssetBook>,
}

/// A checkpoint's books against the replayed ones.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct BooksCheck {
    pub(crate) books_equal: u64,
    pub(crate) books_differ: u64,
    /// Coins of the checkpoint the replayed books did not know (listed since their seed).
    pub(crate) unknown: u64,
    pub(crate) orders_equal: u64,
    /// The same oid with another side, price or remaining size.
    pub(crate) orders_differ: u64,
    /// In the checkpoint only.
    pub(crate) missing: u64,
    /// Replayed only.
    pub(crate) extra: u64,
    pub(crate) samples: Vec<BookMismatch>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct BookMismatch {
    pub(crate) coin: String,
    pub(crate) oid: u64,
    pub(crate) replayed: Option<RestingOrder>,
    pub(crate) checkpoint: Option<RestingOrder>,
}

impl NativeBooks {
    pub(crate) fn seeded(native: NativeSeed) -> Self {
        let mut by_coin = FxHashMap::default();
        let mut assets = Vec::with_capacity(native.assets.len());
        for (index, asset) in native.assets.into_iter().enumerate() {
            by_coin.insert(asset.name.into_boxed_str(), index);
            let mut book = AssetBook {
                size_decimals: asset.size_decimals,
                orders: FxHashMap::default(),
                bids: BTreeMap::new(),
                asks: BTreeMap::new(),
            };
            for order in asset.orders {
                book.add_level(&order);
                book.orders.insert(order.oid, order);
            }
            assets.push(book);
        }
        Self { by_coin, assets }
    }

    /// The asset index of a main-dex coin of the universe.
    pub(crate) fn asset(&self, coin: &str) -> Option<usize> {
        self.by_coin.get(coin).copied()
    }

    pub(crate) fn best(&self, asset: usize) -> BestPrices {
        self.assets[asset].best()
    }

    /// Applies one diff.
    pub(crate) fn apply(&mut self, diff: &BookDiff) -> Result<Applied, String> {
        if !is_main_perp(&diff.coin) {
            return Ok(Applied::OtherMarket);
        }
        let Some(&index) = self.by_coin.get(&diff.coin) else {
            return Ok(Applied::UnknownCoin);
        };
        let book = &mut self.assets[index];
        let context = |err: String| format!("{} oid {}: {err}", diff.coin, diff.oid);
        let limit_px = diff.px.units(MAX_PERP_DECIMALS - book.size_decimals).map_err(|err| context(format!("price {err}")))?;
        let size = |text: &str| {
            let raw = parse_size_raw(text, book.size_decimals).map_err(context)?;
            u64::try_from(raw).map_err(|_| context(format!("size {text:?} is negative")))
        };
        match &diff.change {
            BookChange::New { size: placed } => {
                let order = RestingOrder { oid: diff.oid, side: diff.side, limit_px, remaining: size(placed)? };
                if book.orders.contains_key(&diff.oid) {
                    return Err(context("a new order already rests".to_string()));
                }
                book.add_level(&order);
                book.orders.insert(diff.oid, order);
            }
            BookChange::Update { from, to } => {
                let (from, to) = (size(from)?, size(to)?);
                let order = held(book, diff, limit_px).map_err(context)?;
                if order.remaining != from {
                    return Err(context(format!("updated from {from}, the book holds {}", order.remaining)));
                }
                order.remaining = to;
            }
            BookChange::Remove => {
                held(book, diff, limit_px).map_err(context)?;
                if let Some(order) = book.orders.remove(&diff.oid) {
                    book.drop_level(&order);
                }
            }
        }
        Ok(Applied::Book(index))
    }

    /// Order by order against a checkpoint's books.
    pub(crate) fn check_against(&self, checkpoint: &NativeSeed) -> BooksCheck {
        let mut check = BooksCheck::default();
        for asset in &checkpoint.assets {
            let Some(&index) = self.by_coin.get(asset.name.as_str()) else {
                check.unknown += 1;
                continue;
            };
            let replayed = &self.assets[index].orders;
            let before = (check.orders_differ, check.missing, check.extra);
            for order in &asset.orders {
                match replayed.get(&order.oid) {
                    Some(mine) if mine == order => check.orders_equal += 1,
                    Some(mine) => {
                        check.orders_differ += 1;
                        sample(&mut check, &asset.name, order.oid, Some(*mine), Some(*order));
                    }
                    None => {
                        check.missing += 1;
                        sample(&mut check, &asset.name, order.oid, None, Some(*order));
                    }
                }
            }
            let stored: FxHashSet<u64> = asset.orders.iter().map(|order| order.oid).collect();
            for mine in replayed.values().filter(|mine| !stored.contains(&mine.oid)) {
                check.extra += 1;
                sample(&mut check, &asset.name, mine.oid, Some(*mine), None);
            }
            if (check.orders_differ, check.missing, check.extra) == before {
                check.books_equal += 1;
            } else {
                check.books_differ += 1;
            }
        }
        check
    }
}

/// The resting order a diff updates or removes; it must have the diff's side and price.
fn held<'b>(book: &'b mut AssetBook, diff: &BookDiff, limit_px: u64) -> Result<&'b mut RestingOrder, String> {
    let order = book.orders.get_mut(&diff.oid).ok_or("no such resting order")?;
    if (order.side, order.limit_px) != (diff.side, limit_px) {
        return Err(format!("the diff is {:?} at {limit_px}, the order {:?} at {}", diff.side, order.side, order.limit_px));
    }
    Ok(order)
}

fn sample(check: &mut BooksCheck, coin: &str, oid: u64, replayed: Option<RestingOrder>, checkpoint: Option<RestingOrder>) {
    if check.samples.len() < MAX_SAMPLES {
        check.samples.push(BookMismatch { coin: coin.to_string(), oid, replayed, checkpoint });
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        accounts::{
            checkpoint::{parse_checkpoint_bytes, tests::fixture},
            marks::seed::MarkSeed,
        },
        types::node_data::Batch,
    };

    /// The fixture's main-dex seed: BTC (szDecimals 5) with bid oid 7 (3 @ 82850.0) and ask oid 9 (5 @ 82870.0), PUMP
    /// empty.
    pub(crate) fn native_seed() -> NativeSeed {
        MarkSeed::from_checkpoint(parse_checkpoint_bytes(&fixture()).unwrap()).unwrap().native
    }

    /// A `node_raw_book_diffs_streaming` line of block `height`: one diff, in the 2026-10-20 field set.
    pub(crate) fn diff_line(height: u64, coin: &str, oid: u64, side: &str, px: &str, raw_book_diff: &str) -> Batch<NodeDataOrderDiff> {
        let user = "0x00000000000000000000000000000000000000aa";
        sonic_rs::from_str(&format!(
            r#"{{"local_time":"2026-10-19T06:40:00.6","block_time":"2026-10-19T06:40:00.5","block_number":{height},"events":[{{"user":"{user}","oid":{oid},"side":"{side}","px":"{px}","coin":"{coin}","raw_book_diff":{raw_book_diff}}}]}}"#
        ))
        .unwrap()
    }

    fn diff(coin: &str, oid: u64, side: &str, px: &str, raw_book_diff: &str) -> BookDiff {
        BookDiff::of(&diff_line(1, coin, oid, side, px, raw_book_diff).events()[0]).unwrap()
    }

    /// bm 2026-10-20, W (szDecimals 1) block 1190760017: the diff printed 2460032.6 with an f64 tail.
    #[test]
    fn a_size_printed_with_a_float_tail_is_its_unit() {
        let mut native = native_seed();
        native.assets[1].size_decimals = 1;
        let mut books = NativeBooks::seeded(native);
        assert_eq!(books.apply(&diff("PUMP", 5, "A", "0.004", r#"{"new":{"sz":"2460032.6000000001"}}"#)), Ok(Applied::Book(1)));
        let update = diff("PUMP", 5, "A", "0.004", r#"{"update":{"origSz":"2460032.6","newSz":"2460032.5999999996"}}"#);
        assert_eq!(books.apply(&update), Ok(Applied::Book(1)));
        assert_eq!(books.assets[1].orders[&5].remaining, 24_600_326);
    }

    #[test]
    fn coin_names_tell_the_main_perps() {
        assert!(["BTC", "kPEPE", "PUMP"].into_iter().all(is_main_perp));
        assert!(!["@107", "PURR/USDC", "#114920", "xyz:TSLA"].into_iter().any(is_main_perp));
    }

    #[test]
    fn diffs_move_the_books_and_a_checkpoint_checks_them() {
        let mut books = NativeBooks::seeded(native_seed());
        assert_eq!(books.best(0), (Some(828_500), Some(828_700)));
        assert_eq!(books.apply(&diff("BTC", 11, "B", "82860.0", r#"{"new":{"sz":"0.002"}}"#)), Ok(Applied::Book(0)));
        assert_eq!(books.best(0), (Some(828_600), Some(828_700)));
        assert_eq!(books.apply(&diff("BTC", 9, "A", "82870.0", r#"{"update":{"origSz":"0.00005","newSz":"0.00002"}}"#)), Ok(Applied::Book(0)));
        assert_eq!(books.apply(&diff("BTC", 7, "B", "82850.0", r#""remove""#)), Ok(Applied::Book(0)));
        assert_eq!(books.apply(&diff("NEW", 1, "B", "1.0", r#""remove""#)), Ok(Applied::UnknownCoin), "listed since the seed");
        assert_eq!(books.apply(&diff("xyz:TSLA", 1, "B", "1.0", r#""remove""#)), Ok(Applied::OtherMarket));
        assert_eq!(books.apply(&diff("BTC", 9, "A", "82870.0", r#""remove""#)), Ok(Applied::Book(0)));
        assert_eq!(books.best(0), (Some(828_600), None), "the ask side emptied");
        assert_eq!(books.apply(&diff("BTC", 9, "A", "82870.0", r#"{"new":{"sz":"0.00002"}}"#)), Ok(Applied::Book(0)));
        let mut later = native_seed();
        later.assets[0].orders = vec![
            RestingOrder { oid: 9, side: Side::Ask, limit_px: 828_700, remaining: 2 },
            RestingOrder { oid: 11, side: Side::Bid, limit_px: 828_600, remaining: 200 },
        ];
        let equal = books.check_against(&later);
        assert_eq!((equal.books_equal, equal.books_differ, equal.orders_equal, equal.samples.len()), (2, 0, 2, 0));
        later.assets[0].orders[1].remaining = 199;
        later.assets[0].orders.push(RestingOrder { oid: 12, side: Side::Ask, limit_px: 828_800, remaining: 1 });
        later.assets[1].name = "LISTED".to_string();
        let differ = books.check_against(&later);
        let expected = (1, 0, 1, 1, 1, 0);
        assert_eq!((differ.books_differ, differ.books_equal, differ.unknown, differ.orders_differ, differ.missing, differ.extra), expected);
        assert_eq!(differ.samples[0], BookMismatch {
            coin: "BTC".to_string(),
            oid: 11,
            replayed: Some(RestingOrder { oid: 11, side: Side::Bid, limit_px: 828_600, remaining: 200 }),
            checkpoint: Some(RestingOrder { oid: 11, side: Side::Bid, limit_px: 828_600, remaining: 199 }),
        });
    }

    #[test]
    fn a_diff_that_does_not_fit_the_book_breaks_the_contract() {
        let cases = [
            (diff("BTC", 7, "B", "82850.0", r#"{"new":{"sz":"0.001"}}"#), "a new order already rests"),
            (diff("BTC", 8, "B", "82850.0", r#""remove""#), "no such resting order"),
            (diff("BTC", 7, "A", "82850.0", r#""remove""#), "the diff is Ask at 828500, the order Bid at 828500"),
            (diff("BTC", 7, "B", "82851.0", r#""remove""#), "at 828510"),
            (diff("BTC", 7, "B", "82850.0", r#"{"update":{"origSz":"0.00004","newSz":"0.0"}}"#), "updated from 4, the book holds 3"),
            (diff("BTC", 7, "B", "82850.05", r#""remove""#), "price 82850.05 has more than 1 decimals"),
            (diff("BTC", 11, "B", "82850.0", r#"{"new":{"sz":"0.000001"}}"#), "size \"0.000001\" has more than 5 decimals"),
            (diff("BTC", 11, "B", "82850.0", r#"{"new":{"sz":"-0.001"}}"#), "size \"-0.001\" is negative"),
        ];
        for (diff, expected) in cases {
            let error = NativeBooks::seeded(native_seed()).apply(&diff).unwrap_err();
            assert!(error.starts_with("BTC oid ") && error.contains(expected), "{error}");
        }
    }
}
