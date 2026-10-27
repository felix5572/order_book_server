//! The seed of the node marks (design 000439 r2, revision 1): what one checkpoint stores of the main dex's price state
//! and books, and of every HIP-3 mark. The account view's checkpoint parse reads the wire forms below in its single pass
//! over the file; `MarkSeed::from_checkpoint` checks them here, so a seed that breaks its contract withholds only the
//! marks, not the account view.
//!
//! Every shape was read from the checkpoints of 2026-10-19/20 (research `hl_node_mark_state_20261019` README
//! "checkpoint 自带完整的主站逐单盘口和最新成交价"; Codex `checkpoint_mark.py`), not from an official schema.

use std::str::FromStr;

use alloy::primitives::Address;
use rustc_hash::FxHashMap;
use serde::{
    Deserialize, Deserializer,
    de::{IgnoredAny, SeqAccess, Visitor},
};

use super::decimal::ExactDecimal;
use crate::{
    accounts::checkpoint::{CheckpointAccounts, DexMeta, MAX_PERP_DECIMALS, SpotPair, node_time_ns, spot_pair_coin},
    order_book::Side,
};

/// The reference caches' bucket (`ror.ktm[..].b.bucket_millis`), 3,000 for every entry on 2026-10-19/20; the replay
/// rules assume it.
const REFERENCE_BUCKET_MS: u64 = 3_000;

// ─── wire ───────────────────────────────────────────────────────────────────

/// One stored price slot `pxs2[asset][k]` (slots are [oracle, mark, external perp]).
#[derive(Debug, Deserialize)]
pub(crate) struct WirePx {
    pub(crate) p: i64,
    /// When the node set it; absent on the external perp slot of some delisted coins (9 of 702 slots on 1190010000).
    pub(crate) u: Option<String>,
}

/// `cls[k].oracle`: every dex stores the same keys (`pxs2`, `err_dur_guard`, `ror`, `msoe`, `lmpe`, `psro`, `pmpo`, `pmep`,
/// `mpcg` on 1190680000); the marks read the main dex's.
#[derive(Debug, Deserialize)]
pub(crate) struct WireOracle {
    pub(crate) pxs2: Vec<Vec<WirePx>>,
    msoe: WireMsoe,
    lmpe: Vec<(u32, WireEma)>,
    ror: WireReferences,
    /// (main-dex asset, spot pair): the assets whose Spot reference the node takes from that pair's book
    /// (`[[152, 0], [159, 107]]`, PURR and HYPE, on 2026-10-19/20).
    psro: Vec<(u32, u32)>,
}

/// `msoe`: the basis EMA of each priced asset (`e`, by asset index; `g` is not read).
#[derive(Debug, Deserialize)]
struct WireMsoe {
    e: Vec<(u32, WireEma)>,
}

#[derive(Debug, Deserialize)]
struct WireEma {
    num: f64,
    denom: f64,
    decay_duration: f64,
    n_samples: u64,
    last_sample_time: Option<String>,
}

/// `ror.ktm`: the validators' reference caches (`utl`, `roc` are not read).
#[derive(Debug, Deserialize)]
struct WireReferences {
    ktm: Vec<(WireReferenceKey, WireReferenceCache)>,
}

#[derive(Debug, Deserialize)]
enum WireReferenceKey {
    /// `{"p": [asset index, kind]}`.
    #[serde(rename = "p")]
    Perp(u32, String),
    /// `{"s": name}`: `usdtUsdcPx`, `nativePx`, the validators' scalar inputs (not read: no main-dex price rule takes
    /// them).
    #[serde(rename = "s")]
    Named(IgnoredAny),
}

#[derive(Debug, Deserialize)]
struct WireReferenceCache {
    b: WireBucket,
    /// `null` when the cache never had a member (51 delisted coins' `Spot` and `ExternalPerp` on 1190010000).
    m: Option<u64>,
    v: Vec<(String, WireMember)>,
}

#[derive(Debug, Deserialize)]
struct WireBucket {
    last_bucket: u64,
    bucket_millis: u64,
}

#[derive(Debug, Deserialize)]
struct WireMember {
    t: String,
    v: u64,
}

/// `exchange.perp_dexs`: only the main dex's books are kept (`perp_dexs[0]`, the main dex on 1190680000: 234 books,
/// assets 0..233; the HIP-3 dexes' follow).
#[derive(Debug)]
pub(crate) struct WireNativeBooks(pub(crate) Vec<WireBook>);

#[derive(Debug, Deserialize)]
struct WireNativePerpDex {
    books: Vec<WireBook>,
}

impl<'de> Deserialize<'de> for WireNativeBooks {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_seq(NativeBooksVisitor)
    }
}

struct NativeBooksVisitor;

impl<'de> Visitor<'de> for NativeBooksVisitor {
    type Value = WireNativeBooks;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("the exchange.perp_dexs sequence")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        use serde::de::Error as _;
        let native = seq.next_element::<WireNativePerpDex>()?.ok_or_else(|| A::Error::custom("perp_dexs is empty"))?;
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(WireNativeBooks(native.books))
    }
}

/// `perp_dexs[0].books[i]`; `halfs`, `oid_to_key`, `user_states` (trigger orders) and the rest are not read.
#[derive(Debug, Deserialize)]
pub(crate) struct WireBook {
    asset: u32,
    /// Integer price units; present on all 703 books of 1190680000 (11 dexes).
    last_trade_px: u64,
    bod: WireBookOrders,
}

#[derive(Debug, Deserialize)]
struct WireBookOrders {
    e: LiveOrders,
}

/// `bod.e`: a slot holds a resting order (`{"o": order}`) or is free (`{"v": next free}`; 338,302 free and 289,258
/// orders on 1190680000). Only the orders are kept.
#[derive(Debug)]
struct LiveOrders(Vec<WireBookOrder>);

#[derive(Debug, Deserialize)]
enum WireSlot {
    #[serde(rename = "o")]
    Order(WireBookOrder),
    #[serde(rename = "v")]
    Free(IgnoredAny),
}

impl<'de> Deserialize<'de> for LiveOrders {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_seq(LiveOrdersVisitor)
    }
}

struct LiveOrdersVisitor;

impl<'de> Visitor<'de> for LiveOrdersVisitor {
    type Value = LiveOrders;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("the bod.e slots")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut orders = Vec::new();
        while let Some(slot) = seq.next_element::<WireSlot>()? {
            if let WireSlot::Order(order) = slot {
                orders.push(order);
            }
        }
        Ok(LiveOrders(orders))
    }
}

/// A resting order: `o` oid, `r` remaining size, `c.s` side, `c.l` limit (integer price units). On 1190680000 the
/// remainders of each level's orders sum to the level's stored size, and the counts match, on 234 of 234 books.
#[derive(Debug, Deserialize)]
struct WireBookOrder {
    o: u64,
    r: u64,
    c: WireOrderTerms,
}

#[derive(Debug, Deserialize)]
struct WireOrderTerms {
    s: Side,
    l: u64,
}

/// What the account view's parse hands over for the main dex: its oracle state, its books and the spot books.
#[derive(Debug)]
pub(crate) struct WireNativePrices {
    pub(crate) oracle: WireOracle,
    /// Main clearinghouse's explicit halt set; missing is unknown, not an empty set.
    pub(crate) halted_assets: Option<Vec<u32>>,
    pub(crate) books: Vec<WireBook>,
    /// `exchange.spot_books`, each with its pair index as `asset`.
    pub(crate) spot_books: Vec<WireBook>,
    /// (ordinary order delay in seconds, delay scale); absent on older archives.
    pub(crate) system_timing: Option<(f64, f64)>,
    pub(crate) refresh: super::refresh::WireRefreshSeed,
}

// ─── domain ─────────────────────────────────────────────────────────────────

/// One checkpoint's seed of the node marks.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MarkSeed {
    pub(crate) height: u64,
    /// The checkpoint's block time, unix ms.
    pub(crate) time_ms: u64,
    pub(crate) native: NativeSeed,
    /// Every HIP-3 coin by name ("xyz:TSLA").
    pub(crate) hip3: FxHashMap<String, Hip3Coin>,
}

/// The main dex's price state, by asset index.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NativeSeed {
    pub(crate) refresh: super::refresh::RefreshState,
    pub(crate) system_timing: Option<(f64, f64)>,
    pub(crate) assets: Vec<NativeAsset>,
    /// The spot books `oracle.psro` names, in its order.
    pub(crate) spot_references: Vec<SpotReferenceBook>,
}

/// A spot book giving a main-dex asset its Spot reference (`oracle.psro`; the node reads it at each validator's
/// reference action, Codex `hype_purr_internal_reference_20261020`).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SpotReferenceBook {
    /// The main-dex asset index.
    pub(crate) asset: usize,
    /// The pair's coin in the node's book diffs (`@107`, `PURR/USDC`).
    pub(crate) coin: String,
    /// The base token's `szDecimals`: the orders' sizes are in 10^-this.
    pub(crate) size_decimals: u32,
    /// The quote's `weiDecimals` minus the base's `szDecimals`: the orders' prices are in 10^-this (8 - szDecimals
    /// for a USDC quote, the only one `psro` names), and the node reads a midpoint as a price with this many
    /// decimals (0x2da7786).
    pub(crate) price_decimals: u32,
    pub(crate) orders: Vec<RestingOrder>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NativeAsset {
    pub(crate) name: String,
    /// The checkpoint's main clearinghouse `halted_assets`; halted reference keys are not refreshed.
    pub(crate) halted: bool,
    pub(crate) size_decimals: u32,
    /// `pxs2[i][0].p`: the stored oracle, integer price units (10^-(6 - szDecimals)).
    pub(crate) stored_oracle: u64,
    /// `pxs2[i][1]`: the node's applied mark.
    pub(crate) mark: StoredMark,
    /// `msoe.e[i]` and `lmpe[i]`; `None` for a coin the node no longer prices (51 delisted coins on 1190010000 have
    /// neither).
    pub(crate) emas: Option<NativeEmas>,
    /// A cache the checkpoint lacks is empty here, and the coin `unsupported`.
    pub(crate) references: ReferenceCaches,
    /// The book's last trade price, integer price units: the local price's trade input until the coin's next trade.
    pub(crate) last_trade_px: u64,
    pub(crate) orders: Vec<RestingOrder>,
    /// Why the replay cannot claim the coin from this seed: a price state the node has not fully built (bm 10-27 XTEN,
    /// listed that day: msoe without lmpe at 1198840000, no Spot reference cache at 1198850000). The other coins seed.
    pub(crate) unsupported: Option<&'static str>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NativeEmas {
    /// `msoe`: the basis (mid over stored oracle, minus one) EMA.
    pub(crate) basis: Ema,
    /// `lmpe`: the local price EMA, in prices.
    pub(crate) local_price: Ema,
}

/// An EMA as the node stores it: `numerator / denominator_s` is the average.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Ema {
    pub(crate) numerator: f64,
    pub(crate) denominator_s: f64,
    /// The wire's `decay_duration` (120 for msoe, 30 for lmpe): a gap above it resets the EMA and each sample keeps
    /// `1 - 1 / decay_parameter` of the old sums (`mark_rules.rs` `BasisEmaSnapshot::decay_parameter`).
    pub(crate) decay_parameter: f64,
    pub(crate) samples: u64,
    /// Unix ns; `None` before the first sample.
    pub(crate) last_sample_ns: Option<u64>,
}

/// The node's stored mark and when it set it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StoredMark {
    /// Integer price units.
    pub(crate) px: u64,
    /// Block time, unix ns.
    pub(crate) time_ns: u64,
}

/// The three validator reference caches of one asset (`ror.ktm` keyed `[asset, kind]`).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReferenceCaches {
    pub(crate) spot: ReferenceCache,
    pub(crate) external_perp: ReferenceCache,
    pub(crate) mark: ReferenceCache,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ReferenceCache {
    /// Block time in ms divided by the 3,000 ms bucket, at the last refresh.
    pub(crate) last_bucket: u64,
    /// `None` when no member was ever fresh at a refresh.
    pub(crate) median: Option<u64>,
    /// Each validator's latest value (integer price units) and its block time (unix ns), in stored order.
    pub(crate) members: Vec<(Address, u64, u64)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RestingOrder {
    pub(crate) oid: u64,
    pub(crate) side: Side,
    /// Integer price units.
    pub(crate) limit_px: u64,
    /// In 10^-szDecimals.
    pub(crate) remaining: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Hip3Coin {
    /// Exchange-action asset id projected from the checkpoint universe, not inferred from order requests.
    pub(crate) order_asset: u32,
    pub(crate) oracle: Option<u64>,
    pub(crate) size_decimals: u32,
    /// `None` when the asset stores no price.
    pub(crate) mark: Option<StoredMark>,
}

impl MarkSeed {
    /// The seed of a parsed checkpoint, which it consumes (the books are not copied).
    pub(crate) fn from_checkpoint(checkpoint: CheckpointAccounts) -> Result<Self, String> {
        let CheckpointAccounts { height, time_ms, dexes, native_prices, spot_pairs, .. } = checkpoint;
        let (main, hip3_dexes) = dexes.split_first().ok_or("no dex")?;
        if main.pdi != 0 {
            return Err(format!("the first clearinghouse is pdi {}, not the main dex", main.pdi));
        }
        let native = NativeSeed::from_wire(native_prices, main, &spot_pairs).map_err(|err| format!("main dex: {err}"))?;
        let mut hip3 = FxHashMap::default();
        for dex in hip3_dexes {
            for (asset_index, asset) in dex.assets.iter().enumerate() {
                let mark = match (asset.mark_raw, asset.mark_time_ns) {
                    (None, _) => None,
                    (Some(px), Some(time_ns)) => Some(StoredMark { px, time_ns }),
                    (Some(_), None) => return Err(format!("{}: a stored mark without its time", asset.name)),
                };
                let order_asset = dex.action_asset(asset_index).ok_or_else(|| format!("{}: no exchange action asset id", asset.name))?;
                hip3.insert(asset.name.clone(), Hip3Coin { order_asset, size_decimals: asset.size_decimals, mark, oracle: asset.oracle_raw.map(price_units).transpose().map_err(|err| format!("{}: stored oracle {err}", asset.name))? });
            }
        }
        Ok(Self { height, time_ms, native, hip3 })
    }
}

impl NativeSeed {
    fn from_wire(wire: WireNativePrices, main: &DexMeta, spot_pairs: &[SpotPair]) -> Result<Self, String> {
        let WireNativePrices { oracle, halted_assets, books, spot_books, system_timing, refresh } = wire;
        let count = main.assets.len();
        let halted_assets = halted_assets.ok_or("halted_assets is missing")?;
        let mut halted = vec![false; count];
        for asset in halted_assets {
            let index = usize::try_from(asset).ok().filter(|index| *index < count).ok_or_else(|| format!("halted asset {asset} outside the {count} assets"))?;
            if std::mem::replace(&mut halted[index], true) {
                return Err(format!("halted_assets lists asset {asset} twice"));
            }
        }
        let spot_references = spot_reference_books(oracle.psro, spot_books, spot_pairs, count)?;
        let mut basis = by_asset(oracle.msoe.e, count, "msoe")?;
        let mut local_price = by_asset(oracle.lmpe, count, "lmpe")?;
        let mut references = references_by_asset(oracle.ror.ktm, count)?;
        let mut books = books_by_asset(books, count)?;
        if oracle.pxs2.len() != count {
            return Err(format!("pxs2 has {} assets, universe {count}", oracle.pxs2.len()));
        }
        let mut assets = Vec::with_capacity(count);
        for (index, (meta, pxs)) in main.assets.iter().zip(oracle.pxs2).enumerate() {
            let asset = || format!("{} (asset {index})", meta.name);
            let [stored_oracle, mark, ..] = pxs.as_slice() else {
                return Err(format!("{}: pxs2 has {} slots, the oracle and the mark are slots 0 and 1", asset(), pxs.len()));
            };
            let (emas, partial_emas) = match (basis[index].take(), local_price[index].take()) {
                (None, None) => (None, None),
                (Some(basis), Some(local_price)) => (Some(NativeEmas {
                    basis: ema(basis).map_err(|err| format!("{}: msoe {err}", asset()))?,
                    local_price: ema(local_price).map_err(|err| format!("{}: lmpe {err}", asset()))?,
                }), None),
                _ => (None, Some("partialEmas")),
            };
            let (references, missing_cache) = std::mem::take(&mut references[index]).complete();
            let (last_trade_px, orders) = books[index].take().ok_or_else(|| format!("{}: no book", asset()))?;
            assets.push(NativeAsset {
                name: meta.name.clone(),
                halted: halted[index],
                size_decimals: meta.size_decimals,
                stored_oracle: price_units(stored_oracle.p).map_err(|err| format!("{}: stored oracle {err}", asset()))?,
                mark: stored_mark(mark).map_err(|err| format!("{}: mark {err}", asset()))?,
                emas,
                references,
                last_trade_px,
                orders,
                unsupported: partial_emas.or(missing_cache),
            });
        }
        let (refresh, partial) = super::refresh::RefreshState::from_wire(refresh, count)?;
        for asset in partial {
            assets[asset].unsupported.get_or_insert("partialSubmittedPrices");
        }
        Ok(Self { system_timing, assets, spot_references, refresh })
    }
}

/// The books `psro` names, each asset and pair at most once and every one present.
fn spot_reference_books(
    psro: Vec<(u32, u32)>,
    spot_books: Vec<WireBook>,
    pairs: &[SpotPair],
    count: usize,
) -> Result<Vec<SpotReferenceBook>, String> {
    let mut by_pair: FxHashMap<u32, Book> =
        spot_books.into_iter().filter(|book| psro.iter().any(|(_, pair)| *pair == book.asset)).map(resting_orders).collect();
    let mut references: Vec<SpotReferenceBook> = Vec::with_capacity(psro.len());
    for (asset, pair) in psro {
        let context = |err: &str| format!("psro asset {asset} pair {pair}: {err}");
        let index = usize::try_from(asset).ok().filter(|index| *index < count).ok_or_else(|| context("asset outside the universe"))?;
        if references.iter().any(|reference| reference.asset == index) {
            return Err(context("asset listed twice"));
        }
        let pair_index = usize::try_from(pair).map_err(|err| context(&err.to_string()))?;
        let spot_pair = pairs.get(pair_index).ok_or_else(|| context("no such spot pair"))?;
        let price_decimals =
            spot_pair.quote_wei_decimals.checked_sub(spot_pair.size_decimals).ok_or_else(|| context("quote weiDecimals below base szDecimals"))?;
        let (_, orders) = by_pair.remove(&pair).ok_or_else(|| context("no spot book (or the pair listed twice)"))?;
        references.push(SpotReferenceBook {
            asset: index,
            coin: spot_pair_coin(pair_index),
            size_decimals: spot_pair.size_decimals,
            price_decimals,
            orders,
        });
    }
    Ok(references)
}

/// Entries keyed by asset index, each index at most once and inside the universe.
fn by_asset<T>(entries: Vec<(u32, T)>, count: usize, field: &str) -> Result<Vec<Option<T>>, String> {
    let mut out: Vec<Option<T>> = (0..count).map(|_| None).collect();
    for (index, entry) in entries {
        let slot = usize::try_from(index)
            .ok()
            .and_then(|index| out.get_mut(index))
            .ok_or_else(|| format!("{field} entry for asset {index} outside the {count} assets"))?;
        if slot.replace(entry).is_some() {
            return Err(format!("{field} lists asset {index} twice"));
        }
    }
    Ok(out)
}

fn ema(wire: WireEma) -> Result<Ema, String> {
    let last_sample_ns = wire.last_sample_time.as_deref().map(node_time_ns).transpose()?;
    Ok(Ema {
        numerator: wire.num,
        denominator_s: wire.denom,
        decay_parameter: wire.decay_duration,
        samples: wire.n_samples,
        last_sample_ns,
    })
}

fn price_units(raw: i64) -> Result<u64, String> {
    u64::try_from(raw).ok().filter(|units| *units > 0).ok_or_else(|| format!("{raw} is not positive"))
}

fn stored_mark(slot: &WirePx) -> Result<StoredMark, String> {
    let time = slot.u.as_deref().ok_or("has no update time")?;
    Ok(StoredMark { px: price_units(slot.p)?, time_ns: node_time_ns(time)? })
}

/// One asset's caches while `ror.ktm` is read.
#[derive(Default)]
struct PartialCaches {
    spot: Option<ReferenceCache>,
    external_perp: Option<ReferenceCache>,
    mark: Option<ReferenceCache>,
}

impl PartialCaches {
    /// The three caches, a missing one empty, and the unsupported reason the first missing one gives the coin.
    fn complete(self) -> (ReferenceCaches, Option<&'static str>) {
        let missing = [(self.spot.is_none(), "noSpotReferenceCache"), (self.external_perp.is_none(), "noExternalPerpReferenceCache"),
            (self.mark.is_none(), "noMarkReferenceCache")];
        let reason = missing.into_iter().find_map(|(missing, reason)| missing.then_some(reason));
        let caches = ReferenceCaches {
            spot: self.spot.unwrap_or_default(),
            external_perp: self.external_perp.unwrap_or_default(),
            mark: self.mark.unwrap_or_default(),
        };
        (caches, reason)
    }
}

fn references_by_asset(
    entries: Vec<(WireReferenceKey, WireReferenceCache)>,
    count: usize,
) -> Result<Vec<PartialCaches>, String> {
    let mut out: Vec<PartialCaches> = (0..count).map(|_| PartialCaches::default()).collect();
    for (key, wire) in entries {
        let WireReferenceKey::Perp(index, kind) = key else { continue };
        let caches = usize::try_from(index)
            .ok()
            .and_then(|index| out.get_mut(index))
            .ok_or_else(|| format!("ror entry for asset {index} outside the {count} assets"))?;
        let slot = match kind.as_str() {
            "Spot" => &mut caches.spot,
            "ExternalPerp" => &mut caches.external_perp,
            "Mark" => &mut caches.mark,
            other => return Err(format!("ror entry for asset {index} of unknown kind {other:?}")),
        };
        let cache = reference_cache(wire).map_err(|err| format!("ror asset {index} {kind}: {err}"))?;
        if slot.replace(cache).is_some() {
            return Err(format!("ror lists asset {index} {kind} twice"));
        }
    }
    Ok(out)
}

fn reference_cache(wire: WireReferenceCache) -> Result<ReferenceCache, String> {
    if wire.b.bucket_millis != REFERENCE_BUCKET_MS {
        return Err(format!("bucket of {} ms, the rules assume {REFERENCE_BUCKET_MS}", wire.b.bucket_millis));
    }
    let members = wire
        .v
        .into_iter()
        .map(|(user, member)| {
            let user = Address::from_str(&user).map_err(|err| format!("member {user:?}: {err}"))?;
            Ok((user, member.v, node_time_ns(&member.t)?))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(ReferenceCache { last_bucket: wire.b.last_bucket, median: wire.m, members })
}

type Book = (u64, Vec<RestingOrder>);

fn books_by_asset(books: Vec<WireBook>, count: usize) -> Result<Vec<Option<Book>>, String> {
    by_asset(books.into_iter().map(resting_orders).collect(), count, "perp_dexs[0].books")
}

/// A book's index (`asset`) and its last trade and resting orders.
fn resting_orders(book: WireBook) -> (u32, Book) {
    let orders = book
        .bod
        .e
        .0
        .into_iter()
        .map(|order| RestingOrder { oid: order.o, side: order.c.s, limit_px: order.c.l, remaining: order.r })
        .collect();
    (book.asset, (book.last_trade_px, orders))
}

/// A written price (`coin_to_mark_px[..].px`, a diff's `px`) in integer price units: exact (`ExactDecimal`) and
/// positive, or the contract is broken.
pub(crate) fn exact_price_units(text: &str, size_decimals: u32) -> Result<u64, String> {
    let price_decimals = MAX_PERP_DECIMALS.checked_sub(size_decimals).ok_or_else(|| format!("szDecimals {size_decimals}"))?;
    let units = ExactDecimal::parse(text)?.units(price_decimals).map_err(|err| format!("price {err}"))?;
    if units == 0 {
        return Err(format!("price {text:?} is not positive"));
    }
    Ok(units)
}

#[cfg(test)]
pub(crate) mod tests {
    use serde_json::json;

    use super::*;
    use crate::accounts::checkpoint::{parse_checkpoint_bytes, tests::fixture};

    /// A member of the fixture's reference caches.
    pub(crate) const VALIDATOR: &str = "0x00000000000000000000000000000000000000ee";

    #[test]
    fn a_written_mark_converts_exactly_or_fails() {
        assert_eq!(exact_price_units("74.428", 3), Ok(74_428));
        assert_eq!(exact_price_units("140.2385", 2), Ok(1_402_385));
        assert_eq!(exact_price_units("2", 5), Ok(20), "BTC prices carry one decimal");
        assert_eq!(exact_price_units("82160.0", 5), Ok(821_600));
        assert_eq!(exact_price_units("0.000123", 0), Ok(123));
        for (text, size_decimals) in [("1.23", 5), ("0", 2), ("-1.0", 2), ("1e3", 2), ("", 2), (".5", 2), ("1.", 2)] {
            assert!(exact_price_units(text, size_decimals).is_err(), "{text:?} at szDecimals {size_decimals}");
        }
    }

    #[test]
    fn the_fixture_seeds_both_main_coins_and_the_hip3_coins() {
        let seed = MarkSeed::from_checkpoint(parse_checkpoint_bytes(&fixture()).unwrap()).unwrap();
        assert_eq!((seed.height, seed.time_ms), (1_189_230_000, 1_792_391_843_556));
        let [btc, pump] = seed.native.assets.as_slice() else { panic!("two main assets: {:?}", seed.native.assets) };
        assert_eq!((btc.name.as_str(), btc.stored_oracle, btc.mark.px, btc.last_trade_px), ("BTC", 828_908, 828_576, 828_600));
        assert_eq!(btc.mark.time_ns, 1_792_391_843_000_000_000);
        let basis = &btc.emas.as_ref().unwrap().basis;
        assert_eq!((basis.numerator, basis.denominator_s, basis.decay_parameter, basis.samples), (-0.04, 371.5, 120.0, 2_244_587));
        assert_eq!(basis.last_sample_ns, Some(1_792_391_843_000_000_000));
        assert_eq!(btc.emas.as_ref().unwrap().local_price.decay_parameter, 30.0);
        let validator = Address::from_str(VALIDATOR).unwrap();
        assert_eq!(btc.references.mark.members, [(validator, 828_590, 1_792_391_842_000_000_000)]);
        assert_eq!((btc.references.mark.median, btc.references.mark.last_bucket), (Some(828_590), 597_463_947));
        assert_eq!(
            btc.orders,
            [
                RestingOrder { oid: 7, side: Side::Bid, limit_px: 828_500, remaining: 3 },
                RestingOrder { oid: 9, side: Side::Ask, limit_px: 828_700, remaining: 5 }
            ],
            "the free slot is skipped"
        );
        assert_eq!((pump.emas.as_ref(), pump.references.spot.median, pump.orders.len()), (None, None, 0), "an unpriced coin");
        assert_eq!(seed.hip3["xyz:TSLA"], Hip3Coin {
            order_asset: 110_000,
            oracle: Some(357_970),
            size_decimals: 3,
            mark: Some(StoredMark { px: 358_320, time_ns: 1_792_391_842_000_000_000 })
        });
        assert_eq!(seed.hip3["flx:GOLD"].mark.map(|mark| mark.px), Some(41_883_000));
    }

    fn seed_error(edit: impl FnOnce(&mut serde_json::Value)) -> String {
        let mut checkpoint: serde_json::Value = rmp_serde::from_slice(&fixture()).unwrap();
        edit(&mut checkpoint);
        let parsed = parse_checkpoint_bytes(&rmp_serde::to_vec(&checkpoint).unwrap()).expect("the account parse is not affected");
        MarkSeed::from_checkpoint(parsed).unwrap_err()
    }

    #[test]
    fn a_seed_breaking_its_contract_fails_alone() {
        let oracle = "/exchange/locus/cls/0/oracle";
        let cases: [(&str, serde_json::Value, &str); 5] = [
            ("/lmpe/0/0", json!(5), "outside the 2 assets"),
            ("/ror/ktm/0/1/b/bucket_millis", json!(1000), "bucket of 1000 ms"),
            ("/ror/ktm/1/0/p/1", json!("Index"), "unknown kind"),
            ("/pxs2/0/1/u", serde_json::Value::Null, "has no update time"),
            ("/pxs2/1/0/p", json!(0), "stored oracle 0 is not positive"),
        ];
        for (path, value, expected) in cases {
            let error = seed_error(|checkpoint| *checkpoint.pointer_mut(&format!("{oracle}{path}")).unwrap() = value);
            assert!(error.contains(expected), "{path}: {error}");
        }
        let error = seed_error(|checkpoint| {
            checkpoint["exchange"]["perp_dexs"][0]["books"].as_array_mut().unwrap().pop();
        });
        assert!(error.contains("PUMP (asset 1): no book"), "{error}");
        let error = seed_error(|checkpoint| {
            let books = &mut checkpoint["exchange"]["perp_dexs"][0]["books"];
            books[1]["asset"] = json!(0);
        });
        assert!(error.contains("lists asset 0 twice"), "{error}");
        let error = seed_error(|checkpoint| {
            checkpoint["exchange"]["locus"]["cls"][1]["oracle"]["pxs2"][0][1].as_object_mut().unwrap().remove("u");
        });
        assert!(error.contains("xyz:TSLA: a stored mark without its time"), "{error}");
    }

    fn seed_with(edit: impl FnOnce(&mut serde_json::Value)) -> MarkSeed {
        let mut checkpoint: serde_json::Value = rmp_serde::from_slice(&fixture()).unwrap();
        edit(&mut checkpoint);
        MarkSeed::from_checkpoint(parse_checkpoint_bytes(&rmp_serde::to_vec(&checkpoint).unwrap()).unwrap()).unwrap()
    }

    /// bm 10-27: XTEN, listed that day, had msoe without lmpe at 1198840000 and no Spot reference cache at 1198850000;
    /// each failed the whole seed and withheld every coin's marks. Such a coin alone is unsupported now: the replay does
    /// not claim it, and a checkpoint holding it so is not checked against.
    #[test]
    fn a_coin_whose_state_is_not_fully_built_seeds_unsupported_alone() {
        let oracle = "/exchange/locus/cls/0/oracle";
        let only_msoe = seed_with(|checkpoint| *checkpoint.pointer_mut(&format!("{oracle}/lmpe")).unwrap() = json!([]));
        let [btc, pump] = only_msoe.native.assets.as_slice() else { panic!("two main assets") };
        assert_eq!((btc.unsupported, btc.emas.as_ref(), pump.unsupported), (Some("partialEmas"), None, None));
        let no_spot = seed_with(|checkpoint| {
            checkpoint.pointer_mut(&format!("{oracle}/ror/ktm")).unwrap().as_array_mut().unwrap().remove(0);
        });
        let [btc, pump] = no_spot.native.assets.as_slice() else { panic!("two main assets") };
        assert_eq!((btc.unsupported, &btc.references.spot, pump.unsupported), (Some("noSpotReferenceCache"), &ReferenceCache::default(), None));
        assert_eq!(btc.references.mark.median, Some(828_590), "the caches it has are read");
        let cached_mark_alone = seed_with(|checkpoint| checkpoint["exchange"]["msc"]["mou"]["u"][1] = json!([[0, 828_576]]));
        let [btc, pump] = cached_mark_alone.native.assets.as_slice() else { panic!("two main assets") };
        assert_eq!((btc.unsupported, pump.unsupported, &cached_mark_alone.native.refresh.submitted), (Some("partialSubmittedPrices"), None, &vec![None, None]));
        let replay = super::super::native::NativePrices::seeded(&no_spot.native);
        assert_eq!((replay.stored_oracle(0), replay.mark(0)), (None, None), "BTC is not claimed");
        let full = MarkSeed::from_checkpoint(parse_checkpoint_bytes(&fixture()).unwrap()).unwrap();
        let check = super::super::native::NativePrices::seeded(&full.native).check_against(&no_spot.native);
        assert_eq!(check.unsupported, [("noSpotReferenceCache", vec!["BTC".to_string()])]);
        assert!(check.samples.is_empty(), "{:?}", check.samples);
    }

    #[test]
    fn the_halt_set_is_explicit_and_validated_without_blocking_account_seeding() {
        let seed = MarkSeed::from_checkpoint(parse_checkpoint_bytes(&fixture()).unwrap()).unwrap();
        assert_eq!(seed.native.assets.iter().map(|asset| asset.halted).collect::<Vec<_>>(), [false, true]);
        let error = seed_error(|checkpoint| {
            checkpoint["exchange"]["locus"]["cls"][0].as_object_mut().unwrap().remove("halted_assets");
        });
        assert!(error.contains("halted_assets is missing"), "{error}");
        for (value, expected) in [(json!([2]), "outside the 2 assets"), (json!([1, 1]), "lists asset 1 twice")] {
            let error = seed_error(|checkpoint| checkpoint["exchange"]["locus"]["cls"][0]["halted_assets"] = value);
            assert!(error.contains(expected), "{error}");
        }
    }

    /// The fixture with `psro` naming spot pair 1 (HYPE/USDC: base szDecimals 2, USDC quote) for PUMP, and the spot
    /// books of pairs 0 and 1.
    fn spot_referenced(checkpoint: &mut serde_json::Value) {
        checkpoint["exchange"]["locus"]["cls"][0]["oracle"]["psro"] = json!([[1, 1]]);
        checkpoint["exchange"]["spot_books"] = json!([
            {"asset": 0, "last_trade_px": 1, "bod": {"e": []}},
            {"asset": 1, "last_trade_px": 79_316_000, "bod": {"e": [
                {"v": 1},
                {"o": {"o": 21, "r": 300, "u": VALIDATOR, "c": {"t": 1, "S": 300, "s": "B", "l": 79_288_000}}}
            ]}}
        ]);
    }

    #[test]
    fn psro_names_the_spot_books_seeded_with_their_units() {
        let mut checkpoint: serde_json::Value = rmp_serde::from_slice(&fixture()).unwrap();
        spot_referenced(&mut checkpoint);
        let seed = MarkSeed::from_checkpoint(parse_checkpoint_bytes(&rmp_serde::to_vec(&checkpoint).unwrap()).unwrap()).unwrap();
        assert_eq!(seed.native.spot_references, [SpotReferenceBook {
            asset: 1,
            coin: "@1".to_string(),
            size_decimals: 2,
            price_decimals: 6,
            orders: vec![RestingOrder { oid: 21, side: Side::Bid, limit_px: 79_288_000, remaining: 300 }],
        }]);
        let error = seed_error(|checkpoint| {
            spot_referenced(checkpoint);
            checkpoint["exchange"]["spot_books"].as_array_mut().unwrap().pop();
        });
        assert!(error.contains("psro asset 1 pair 1: no spot book"), "{error}");
        let error = seed_error(|checkpoint| {
            spot_referenced(checkpoint);
            checkpoint["exchange"]["locus"]["cls"][0]["oracle"]["psro"] = json!([[1, 1], [1, 0]]);
        });
        assert!(error.contains("psro asset 1 pair 0: asset listed twice"), "{error}");
        let error = seed_error(|checkpoint| {
            spot_referenced(checkpoint);
            checkpoint["exchange"]["locus"]["cls"][0]["oracle"]["psro"] = json!([[2, 1]]);
        });
        assert!(error.contains("psro asset 2 pair 1: asset outside the universe"), "{error}");
        let error = seed_error(|checkpoint| {
            spot_referenced(checkpoint);
            checkpoint["exchange"]["locus"]["cls"][0]["oracle"]["psro"] = json!([[1, 3]]);
        });
        assert!(error.contains("psro asset 1 pair 3: no such spot pair"), "{error}");
    }

    #[test]
    fn a_slot_neither_order_nor_free_fails_the_parse() {
        let mut checkpoint: serde_json::Value = rmp_serde::from_slice(&fixture()).unwrap();
        checkpoint["exchange"]["perp_dexs"][0]["books"][0]["bod"]["e"][0] = json!({"x": 1});
        assert!(parse_checkpoint_bytes(&rmp_serde::to_vec(&checkpoint).unwrap()).is_err());
    }
    #[test]
    fn corewriter_delay_parameters_come_from_the_checkpoint() {
        let mut wire: serde_json::Value = rmp_serde::from_slice(&fixture()).unwrap();
        wire["exchange"]["pt"] = serde_json::json!({"prp":{"D":0.33}});
        wire["exchange"]["action_delayer"] = serde_json::json!({"delay_scale":1.0});
        let seed = MarkSeed::from_checkpoint(parse_checkpoint_bytes(&rmp_serde::to_vec(&wire).unwrap()).unwrap()).unwrap();
        assert_eq!(seed.native.system_timing, Some((0.33, 1.0)));
    }

}
