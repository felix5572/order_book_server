//! The main dex's prices block by block (design 000439 r1 section 3; research `hl_node_mark_state_20261019`
//! `native_all_coins_check.py` and `reference_mark_check.py`: 181 of 183 coins bit-exact over three intervals, every
//! observed mark of four coins exact).
//!
//! Per coin: the stored oracle, the mark, the basis EMA (msoe), the local price EMA (lmpe), the validators' Spot,
//! `ExternalPerp` and Mark reference caches and the last trade. Each block:
//!
//! 1. every cache refreshes at the block's time: in a new 3,000 ms bucket, members under 10 s old give the median; with
//!    none, the median and its bucket stay (Codex: "非空才写");
//! 2. the price actions run in execution order, each reading the book at its cut and the last trade before it:
//!    - a validator's reference action stores its Spot and `ExternalPerp` members and the Mark member median(A, L, C):
//!      A = Spot × (1 + basis previewed with the cut's mid), L = median(bid, ask, last trade), C = `ExternalPerp`;
//!    - a `SetGlobalAction` stores the oracle median(submitted, Spot cache, L with the basis undone) and the mark
//!      median(submitted, Mark cache, L), then samples msoe with the new oracle and lmpe with L.
//!
//! A coin whose inputs take a path never observed (a validator pricing it with one of the two prices, an empty book
//! side, a mark moving more than the 1% step limit, an lmpe gap, HYPE and PURR's internal reference members) is
//! `unsupported` until a seed replaces it: its values are not claimed.

use alloy::primitives::Address;
use rustc_hash::FxHashMap;

use super::{
    actions::MarkAction,
    rules::{internal_round, inverse_basis, local_price, median_units, wire_units},
    seed::{NativeAsset, NativeEmas, NativeSeed, ReferenceCache, StoredMark},
};

const BUCKET_NS: u64 = 3_000 * 1_000_000;
const MEMBER_LIFETIME_NS: u64 = 10 * 1_000_000_000;
/// Their reference members come from a price inside the node, not from the validators (research "全部主站币").
const INTERNALLY_REFERENCED: [&str; 2] = ["HYPE", "PURR"];
/// Mismatches listed per check (all are counted).
const MAX_SAMPLES: usize = 20;

/// One reference key's members, each validator's latest (value, block time ns), and the refreshed median.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MemberCache {
    members: FxHashMap<Address, (u64, u64)>,
    median: Option<u64>,
    last_bucket: u64,
}

impl MemberCache {
    fn seeded(cache: ReferenceCache) -> Self {
        let members = cache.members.into_iter().map(|(user, value, time_ns)| (user, (value, time_ns))).collect();
        Self { members, median: cache.median, last_bucket: cache.last_bucket }
    }

    fn begin_block(&mut self, block_ns: u64) {
        let bucket = block_ns / BUCKET_NS;
        if bucket <= self.last_bucket {
            return;
        }
        let fresh: Vec<u64> =
            self.members.values().filter(|(_, time_ns)| block_ns.saturating_sub(*time_ns) < MEMBER_LIFETIME_NS).map(|(value, _)| *value).collect();
        if let Some(median) = median_units(&fresh) {
            self.median = Some(median);
            self.last_bucket = bucket;
        }
    }

    fn insert(&mut self, user: Address, value: u64, time_ns: u64) {
        self.members.insert(user, (value, time_ns));
    }

    fn equals(&self, stored: &ReferenceCache) -> bool {
        let stored_members: FxHashMap<Address, (u64, u64)> =
            stored.members.iter().map(|(user, value, time_ns)| (*user, (*value, *time_ns))).collect();
        (self.median, self.last_bucket) == (stored.median, stored.last_bucket) && self.members == stored_members
    }
}

/// One main-dex coin's prices.
#[derive(Debug, Clone)]
struct CoinPrices {
    name: String,
    size_decimals: u32,
    stored_oracle: u64,
    mark: StoredMark,
    emas: Option<NativeEmas>,
    spot: MemberCache,
    external_perp: MemberCache,
    marks: MemberCache,
    last_trade: u64,
    unsupported: Option<&'static str>,
}

/// The book a price action reads for one coin, and the coin's last trade of the block before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CoinCut {
    pub(crate) best: (Option<u64>, Option<u64>),
    /// `None`: no trade of the coin in the block before the action, the last trade of earlier blocks holds.
    pub(crate) block_trade: Option<u64>,
}

impl CoinPrices {
    fn seeded(asset: NativeAsset) -> Self {
        let unsupported = INTERNALLY_REFERENCED.contains(&asset.name.as_str()).then_some("internalReference");
        Self {
            name: asset.name,
            size_decimals: asset.size_decimals,
            stored_oracle: asset.stored_oracle,
            mark: asset.mark,
            emas: asset.emas,
            spot: MemberCache::seeded(asset.references.spot),
            external_perp: MemberCache::seeded(asset.references.external_perp),
            marks: MemberCache::seeded(asset.references.mark),
            last_trade: asset.last_trade_px,
            unsupported,
        }
    }

    fn begin_block(&mut self, block_ns: u64) {
        for cache in [&mut self.spot, &mut self.external_perp, &mut self.marks] {
            cache.begin_block(block_ns);
        }
    }

    /// The local price inputs at a cut; `None` (unsupported) with an empty book side.
    fn local(&mut self, cut: CoinCut) -> Option<(u64, u64, u64)> {
        let (Some(bid), Some(ask)) = cut.best else {
            self.unsupported.get_or_insert("emptyBookSide");
            return None;
        };
        Some((bid, ask, local_price(bid, ask, cut.block_trade.unwrap_or(self.last_trade))))
    }

    /// A validator's reference action pricing this coin with `spot` and `external` (wire prices).
    fn reference(&mut self, user: Address, spot: Option<&str>, external: Option<&str>, time_ns: u64, cut: CoinCut) -> Result<(), String> {
        let spot = spot.map(|px| wire_units(px, self.size_decimals)).transpose()?;
        let external = external.map(|px| wire_units(px, self.size_decimals)).transpose()?;
        if let Some(spot) = spot {
            self.spot.insert(user, spot, time_ns);
        }
        if let Some(external) = external {
            self.external_perp.insert(user, external, time_ns);
        }
        let (Some(spot), Some(external)) = (spot, external) else {
            // the node fills the missing candidate from an lmpe preview (Codex); never observed
            self.unsupported.get_or_insert("fillInPath");
            return Ok(());
        };
        let Some((bid, ask, local)) = self.local(cut) else { return Ok(()) };
        let Some(emas) = &self.emas else {
            self.unsupported.get_or_insert("noEma");
            return Ok(());
        };
        let preview = emas.basis.basis_advanced(bid.midpoint(ask), self.stored_oracle, time_ns);
        #[expect(clippy::cast_precision_loss, reason = "prices are below 2^53")]
        let a = internal_round(spot as f64 * (1.0 + preview.basis_ratio()))?;
        let member = median_units(&[a, local, external]).ok_or("no Mark member candidates")?;
        self.marks.insert(user, member, time_ns);
        Ok(())
    }

    /// A `SetGlobalAction`'s submitted (oracle, mark) for this coin.
    fn set_global(&mut self, oracle: &str, mark: &str, time_ns: u64, cut: CoinCut) -> Result<(), String> {
        let (submitted_oracle, submitted_mark) = (wire_units(oracle, self.size_decimals)?, wire_units(mark, self.size_decimals)?);
        let Some((bid, ask, local)) = self.local(cut) else { return Ok(()) };
        let (Some(spot_median), Some(mark_median), Some(emas)) = (self.spot.median, self.marks.median, self.emas.as_ref()) else {
            self.unsupported.get_or_insert("noCacheOrEma");
            return Ok(());
        };
        let stored = median_units(&[submitted_oracle, spot_median, inverse_basis(local, emas.basis.basis_ratio())?]).ok_or("no oracle candidates")?;
        let new_mark = median_units(&[submitted_mark, mark_median, local]).ok_or("no mark candidates")?;
        // the per-update 1% Mark limit (seen once on HIP-3, never on the main dex): its main-dex rounding is unknown
        if u128::from(new_mark.abs_diff(self.mark.px)) * 100 > u128::from(self.mark.px) {
            self.unsupported.get_or_insert("markStepLimit");
            return Ok(());
        }
        #[expect(clippy::cast_precision_loss, reason = "prices are below 2^53")]
        let local_px = local as f64 / 10_f64.powi((6 - self.size_decimals).cast_signed());
        let Some(local_price_ema) = emas.local_price.local_price_advanced(local_px, time_ns) else {
            self.unsupported.get_or_insert("localPriceGap");
            return Ok(());
        };
        let basis = emas.basis.basis_advanced(bid.midpoint(ask), stored, time_ns);
        self.emas = Some(NativeEmas { basis, local_price: local_price_ema });
        self.stored_oracle = stored;
        self.mark = StoredMark { px: new_mark, time_ns };
        Ok(())
    }
}

/// Every main-dex coin's prices, by asset index.
#[derive(Debug, Clone)]
pub(crate) struct NativePrices {
    coins: Vec<CoinPrices>,
    by_name: FxHashMap<String, usize>,
}

/// A checkpoint's prices against the replayed ones, field by field over the supported coins.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct PricesCheck {
    /// (field, equal, differ): stored oracle, mark, msoe, lmpe, Spot / `ExternalPerp` / Mark caches.
    pub(crate) fields: Vec<(&'static str, u64, u64)>,
    /// Coins whose replay is not claimed, by reason.
    pub(crate) unsupported: Vec<(&'static str, Vec<String>)>,
    pub(crate) samples: Vec<(String, &'static str)>,
}

impl NativePrices {
    pub(crate) fn seeded(native: &NativeSeed) -> Self {
        let coins: Vec<CoinPrices> = native.assets.iter().cloned().map(CoinPrices::seeded).collect();
        let by_name = coins.iter().enumerate().map(|(index, coin)| (coin.name.clone(), index)).collect();
        Self { coins, by_name }
    }

    pub(crate) fn begin_block(&mut self, block_ns: u64) {
        for coin in &mut self.coins {
            coin.begin_block(block_ns);
        }
    }

    /// A crossed fill of the coin moved the last trade.
    pub(crate) fn trade(&mut self, asset: usize, px: u64) {
        self.coins[asset].last_trade = px;
    }

    pub(crate) fn size_decimals(&self, asset: usize) -> u32 {
        self.coins[asset].size_decimals
    }

    pub(crate) fn asset(&self, coin: &str) -> Option<usize> {
        self.by_name.get(coin).copied()
    }

    /// The coin's mark in price units; `None` while it is unsupported.
    pub(crate) fn mark(&self, asset: usize) -> Option<u64> {
        let coin = &self.coins[asset];
        coin.unsupported.is_none().then_some(coin.mark.px)
    }

    #[cfg(test)]
    pub(crate) fn mark_of(&self, asset: usize) -> (StoredMark, Option<&'static str>) {
        (self.coins[asset].mark, self.coins[asset].unsupported)
    }

    /// One price action at its cut; `cut_of(asset)` reads the book and last trade there.
    pub(crate) fn apply(
        &mut self,
        action: &MarkAction,
        user: Option<Address>,
        time_ns: u64,
        cut_of: impl Fn(usize) -> CoinCut,
    ) -> Result<(), String> {
        match action {
            MarkAction::Book(_) => Ok(()),
            MarkAction::SetGlobal(pxs) => {
                for (asset, pair) in pxs.iter().enumerate() {
                    let Some((oracle, mark)) = pair else { continue };
                    let coin = self.coins.get_mut(asset).ok_or_else(|| format!("SetGlobalAction prices asset {asset} outside the universe"))?;
                    coin.set_global(oracle, mark, time_ns, cut_of(asset)).map_err(|err| format!("{}: {err}", coin.name))?;
                }
                Ok(())
            }
            MarkAction::Reference { oracle, external } => {
                let user = user.ok_or("a reference action without a validator")?;
                let oracle: FxHashMap<&str, &str> = oracle.iter().map(|(coin, px)| (coin.as_str(), px.as_str())).collect();
                let external: FxHashMap<&str, &str> = external.iter().map(|(coin, px)| (coin.as_str(), px.as_str())).collect();
                let mut coins: Vec<&str> = oracle.keys().chain(external.keys()).copied().collect();
                coins.sort_unstable();
                coins.dedup();
                for name in coins {
                    // a coin outside the universe (listed since the seed) has no state yet
                    let Some(asset) = self.asset(name) else { continue };
                    let coin = &mut self.coins[asset];
                    coin.reference(user, oracle.get(name).copied(), external.get(name).copied(), time_ns, cut_of(asset))
                        .map_err(|err| format!("{name}: {err}"))?;
                }
                Ok(())
            }
        }
    }

    /// Field by field against a checkpoint's seed.
    pub(crate) fn check_against(&self, checkpoint: &NativeSeed) -> PricesCheck {
        let mut counts: FxHashMap<&'static str, (u64, u64)> = FxHashMap::default();
        let mut unsupported: FxHashMap<&'static str, Vec<String>> = FxHashMap::default();
        let mut samples = Vec::new();
        for stored in &checkpoint.assets {
            let Some(coin) = self.asset(&stored.name).map(|asset| &self.coins[asset]) else {
                unsupported.entry("unknownCoin").or_default().push(stored.name.clone());
                continue;
            };
            if let Some(reason) = coin.unsupported {
                unsupported.entry(reason).or_default().push(stored.name.clone());
                continue;
            }
            let fields = [
                ("storedOracle", coin.stored_oracle == stored.stored_oracle),
                ("mark", coin.mark == stored.mark),
                ("msoe", coin.emas.as_ref().map(|emas| &emas.basis) == stored.emas.as_ref().map(|emas| &emas.basis)),
                ("lmpe", coin.emas.as_ref().map(|emas| &emas.local_price) == stored.emas.as_ref().map(|emas| &emas.local_price)),
                ("spotCache", coin.spot.equals(&stored.references.spot)),
                ("externalPerpCache", coin.external_perp.equals(&stored.references.external_perp)),
                ("markCache", coin.marks.equals(&stored.references.mark)),
            ];
            for (field, equal) in fields {
                let entry = counts.entry(field).or_default();
                if equal {
                    entry.0 += 1;
                    continue;
                }
                entry.1 += 1;
                if samples.len() < MAX_SAMPLES {
                    samples.push((stored.name.clone(), field));
                }
            }
        }
        let mut fields: Vec<_> = counts.into_iter().map(|(field, (equal, differ))| (field, equal, differ)).collect();
        fields.sort_unstable();
        let mut unsupported: Vec<_> = unsupported.into_iter().collect();
        for (_, coins) in &mut unsupported {
            coins.sort_unstable();
        }
        unsupported.sort_unstable();
        PricesCheck { fields, unsupported, samples }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::marks::book::tests::native_seed;

    const VALIDATOR: Address = Address::repeat_byte(0xee);
    /// 2026-10-19T06:37:24 UTC, a second after the fixture's EMA samples.
    const T1: u64 = 1_792_391_844_000_000_000;

    fn cut(bid: u64, ask: u64, last_trade: u64) -> CoinCut {
        CoinCut { best: (Some(bid), Some(ask)), block_trade: Some(last_trade) }
    }

    /// The fixture's BTC (szDecimals 5): stored oracle 828,908, mark 828,576, Spot / Mark medians 828,720 / 828,590.
    #[test]
    fn a_set_global_action_stores_the_medians_and_samples_both_emas() {
        let mut prices = NativePrices::seeded(&native_seed());
        let set_global = MarkAction::SetGlobal(vec![Some(("82890.0".to_string(), "82860.0".to_string())), None]);
        prices.apply(&set_global, None, T1, |_| cut(828_500, 828_700, 828_600)).unwrap();
        let btc = &prices.coins[0];
        assert_eq!(btc.stored_oracle, 828_720, "median(828900, Spot 828720, L 828600 with a zero basis)");
        assert_eq!(btc.mark, StoredMark { px: 828_600, time_ns: T1 }, "median(828600, Mark 828590, L 828600)");
        let emas = btc.emas.as_ref().unwrap();
        assert_eq!((emas.basis.samples, emas.local_price.samples), (2_244_588, 269_230));
        assert_eq!(emas.local_price.last_sample_ns, Some(T1));
        assert_eq!(prices.coins[1].mark.px, 4477, "PUMP is not priced: unchanged");
    }

    #[test]
    fn a_reference_action_stores_three_members() {
        let mut prices = NativePrices::seeded(&native_seed());
        let reference = MarkAction::Reference {
            oracle: vec![("BTC".to_string(), "82875.0".to_string())],
            external: vec![("BTC".to_string(), "82865.0".to_string()), ("NEW".to_string(), "1.0".to_string())],
        };
        prices.apply(&reference, Some(VALIDATOR), T1, |_| cut(828_500, 828_700, 828_600)).unwrap();
        let btc = &prices.coins[0];
        assert_eq!(btc.spot.members[&VALIDATOR], (828_750, T1));
        assert_eq!(btc.external_perp.members[&VALIDATOR], (828_650, T1));
        assert_eq!(btc.marks.members[&VALIDATOR], (828_650, T1), "median(A 828750, L 828600, C 828650)");
        assert_eq!(btc.unsupported, None);
    }

    #[test]
    fn unobserved_paths_make_a_coin_unsupported() {
        let mut prices = NativePrices::seeded(&native_seed());
        let spot_only = MarkAction::Reference { oracle: vec![("BTC".to_string(), "82875.0".to_string())], external: Vec::new() };
        prices.apply(&spot_only, Some(VALIDATOR), T1, |_| cut(828_500, 828_700, 828_600)).unwrap();
        assert_eq!(prices.coins[0].unsupported, Some("fillInPath"));
        let mut prices = NativePrices::seeded(&native_seed());
        let set_global = MarkAction::SetGlobal(vec![Some(("82890.0".to_string(), "90000.0".to_string()))]);
        prices.apply(&set_global, None, T1, |_| cut(900_000, 900_100, 900_050)).unwrap();
        assert_eq!(prices.coins[0].unsupported, Some("markStepLimit"));
        assert_eq!(prices.check_against(&native_seed()).unsupported, [("markStepLimit", vec!["BTC".to_string()])], "named");
        let mut prices = NativePrices::seeded(&native_seed());
        prices.apply(&set_global, None, T1, |_| CoinCut { best: (Some(828_500), None), block_trade: None }).unwrap();
        assert_eq!(prices.coins[0].unsupported, Some("emptyBookSide"));
    }

    #[test]
    fn caches_refresh_per_bucket_from_fresh_members_and_keep_without_any() {
        let bucket = T1 / BUCKET_NS;
        let mut cache = MemberCache { members: FxHashMap::default(), median: Some(7), last_bucket: bucket };
        cache.insert(VALIDATOR, 828_000, T1);
        cache.insert(Address::repeat_byte(1), 828_100, T1 - 9_000_000_000);
        cache.begin_block(T1 + 1);
        assert_eq!((cache.median, cache.last_bucket), (Some(7), bucket), "the same bucket: no refresh");
        cache.begin_block(T1 + BUCKET_NS);
        assert_eq!((cache.median, cache.last_bucket), (Some(828_000), bucket + 1), "the 12 s old member is out");
        cache.begin_block(T1 + 20_000_000_000);
        assert_eq!((cache.median, cache.last_bucket), (Some(828_000), bucket + 1), "no member under 10 s: kept");
    }

    #[test]
    fn a_checkpoint_checks_every_field_of_supported_coins() {
        let seed = native_seed();
        let prices = NativePrices::seeded(&seed);
        let check = prices.check_against(&seed);
        assert!(check.fields.iter().all(|(_, equal, differ)| (*equal, *differ) == (2, 0)), "{:?}", check.fields);
        let mut moved = seed;
        moved.assets[0].stored_oracle += 1;
        let check = prices.check_against(&moved);
        assert!(check.fields.contains(&("storedOracle", 1, 1)), "{:?}", check.fields);
        assert_eq!(check.samples, [("BTC".to_string(), "storedOracle")]);
    }
}
