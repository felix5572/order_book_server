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
    accounts::checkpoint::{CheckpointAccounts, DexMeta, MAX_PERP_DECIMALS, node_time_ns},
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
    /// `{"s": name}`: `usdtUsdcPx`, `nativePx` (the HYPE / PURR inputs, unsupported).
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

/// What the account view's parse hands over for the main dex: its oracle state and its books.
#[derive(Debug)]
pub(crate) struct WireNativePrices {
    pub(crate) oracle: WireOracle,
    pub(crate) books: Vec<WireBook>,
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
    pub(crate) assets: Vec<NativeAsset>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NativeAsset {
    pub(crate) name: String,
    pub(crate) size_decimals: u32,
    /// `pxs2[i][0].p`: the stored oracle, integer price units (10^-(6 - szDecimals)).
    pub(crate) stored_oracle: u64,
    /// `pxs2[i][1]`: the node's applied mark.
    pub(crate) mark: StoredMark,
    /// `msoe.e[i]` and `lmpe[i]`; `None` for a coin the node no longer prices (51 delisted coins on 1190010000 have
    /// neither).
    pub(crate) emas: Option<NativeEmas>,
    pub(crate) references: ReferenceCaches,
    /// The book's last trade price, integer price units: the local price's trade input until the coin's next trade.
    pub(crate) last_trade_px: u64,
    pub(crate) orders: Vec<RestingOrder>,
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

#[derive(Debug, Clone, PartialEq)]
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
    pub(crate) size_decimals: u32,
    /// `None` when the asset stores no price.
    pub(crate) mark: Option<StoredMark>,
}

impl MarkSeed {
    /// The seed of a parsed checkpoint, which it consumes (the books are not copied).
    pub(crate) fn from_checkpoint(checkpoint: CheckpointAccounts) -> Result<Self, String> {
        let CheckpointAccounts { height, time_ms, dexes, native_prices, .. } = checkpoint;
        let (main, hip3_dexes) = dexes.split_first().ok_or("no dex")?;
        if main.pdi != 0 {
            return Err(format!("the first clearinghouse is pdi {}, not the main dex", main.pdi));
        }
        let native = NativeSeed::from_wire(native_prices, main).map_err(|err| format!("main dex: {err}"))?;
        let mut hip3 = FxHashMap::default();
        for dex in hip3_dexes {
            for asset in &dex.assets {
                let mark = match (asset.mark_raw, asset.mark_time_ns) {
                    (None, _) => None,
                    (Some(px), Some(time_ns)) => Some(StoredMark { px, time_ns }),
                    (Some(_), None) => return Err(format!("{}: a stored mark without its time", asset.name)),
                };
                hip3.insert(asset.name.clone(), Hip3Coin { size_decimals: asset.size_decimals, mark });
            }
        }
        Ok(Self { height, time_ms, native, hip3 })
    }
}

impl NativeSeed {
    fn from_wire(wire: WireNativePrices, main: &DexMeta) -> Result<Self, String> {
        let WireNativePrices { oracle, books } = wire;
        let count = main.assets.len();
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
            let emas = match (basis[index].take(), local_price[index].take()) {
                (None, None) => None,
                (Some(basis), Some(local_price)) => Some(NativeEmas {
                    basis: ema(basis).map_err(|err| format!("{}: msoe {err}", asset()))?,
                    local_price: ema(local_price).map_err(|err| format!("{}: lmpe {err}", asset()))?,
                }),
                _ => return Err(format!("{}: one of msoe and lmpe without the other", asset())),
            };
            let caches = std::mem::take(&mut references[index]);
            let (last_trade_px, orders) = books[index].take().ok_or_else(|| format!("{}: no book", asset()))?;
            assets.push(NativeAsset {
                name: meta.name.clone(),
                size_decimals: meta.size_decimals,
                stored_oracle: price_units(stored_oracle.p).map_err(|err| format!("{}: stored oracle {err}", asset()))?,
                mark: stored_mark(mark).map_err(|err| format!("{}: mark {err}", asset()))?,
                emas,
                references: caches.complete().map_err(|err| format!("{}: {err}", asset()))?,
                last_trade_px,
                orders,
            });
        }
        Ok(Self { assets })
    }
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
    fn complete(self) -> Result<ReferenceCaches, String> {
        let missing = |kind: &str| format!("no {kind} reference cache");
        Ok(ReferenceCaches {
            spot: self.spot.ok_or_else(|| missing("Spot"))?,
            external_perp: self.external_perp.ok_or_else(|| missing("ExternalPerp"))?,
            mark: self.mark.ok_or_else(|| missing("Mark"))?,
        })
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
    let entries = books.into_iter().map(|book| {
        let orders = book
            .bod
            .e
            .0
            .into_iter()
            .map(|order| RestingOrder { oid: order.o, side: order.c.s, limit_px: order.c.l, remaining: order.r })
            .collect();
        (book.asset, (book.last_trade_px, orders))
    });
    by_asset(entries.collect(), count, "perp_dexs[0].books")
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
        let cases: [(&str, serde_json::Value, &str); 6] = [
            ("/msoe/e", json!([]), "one of msoe and lmpe without the other"),
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

    #[test]
    fn a_slot_neither_order_nor_free_fails_the_parse() {
        let mut checkpoint: serde_json::Value = rmp_serde::from_slice(&fixture()).unwrap();
        checkpoint["exchange"]["perp_dexs"][0]["books"][0]["bod"]["e"][0] = json!({"x": 1});
        assert!(parse_checkpoint_bytes(&rmp_serde::to_vec(&checkpoint).unwrap()).is_err());
    }
}
