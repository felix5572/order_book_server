//! The main dex's prices block by block (design 000439 r1 section 3; research `hl_node_mark_state_20261019`
//! `native_all_coins_check.py` and `reference_mark_check.py`: 181 of 183 coins bit-exact over three intervals, every
//! observed mark of four coins exact).
//!
//! Per coin: the stored oracle, the mark, the basis EMA (msoe), the local price EMA (lmpe), the validators' Spot,
//! `ExternalPerp` and Mark reference caches and the last trade. Each block:
//!
//! 1. each non-halted coin's caches refresh at the block's time: in a new 3,000 ms bucket, members under 10 s old
//!    give the median; with none, only the old median stays. The bucket still advances, even without a price action.
//!    The explicit checkpoint halt set skips all three keys, including caches with historical members
//!    (node 0x42689de-0x4268a36). This corrects the earlier inference that lack of fresh members froze the bucket;
//! 2. the price actions run in execution order, each reading the book at its cut and the last trade before it:
//!    - a validator's reference action stores its Spot and `ExternalPerp` members and the Mark member median(A, L, C):
//!      A = Spot × (1 + basis previewed with the cut's mid), L = median(bid, ask, last trade), C = `ExternalPerp`;
//!      with two of the three, lmpe previewed with L is the third. A coin `psro` names (HYPE, PURR) takes its Spot
//!      from its spot book's midpoint at the cut instead of the validator, at every reference action (Codex
//!      `hype_purr_internal_reference_20261020`: 12,819 reference actions, both coins equal at three interval ends);
//!    - a `SetGlobalAction` stores the oracle median(submitted, Spot cache, L with the basis undone) and the mark
//!      median(submitted, Mark cache, L), then samples msoe with the new oracle and lmpe with L.
//!
//! Main-dex marks are NOT step-limited. In binary 6911b792, 0x2965e0a tests clearinghouse.moh at +0x378:
//! Main (0x8000000000000000) jumps straight to the selected-price write, bypassing the Hip3 1% limit.
//! ALT liquidation fills at blocks 1192112978 and 1192114308 confirm the uncapped middle-of-three price.
//! A checkpoint checks only the end state; later updates can hide a wrong intermediate mark.
//! A coin whose inputs take a path
//! never observed (an empty book side, an lmpe gap, a cold lmpe for the third candidate) is `unsupported` until a
//! seed replaces it: its values are not claimed.

use alloy::primitives::Address;
use rustc_hash::FxHashMap;

use super::{
    actions::MarkAction,
    book::BestPrices,
    rules::{internal_round, inverse_basis, local_price, median_units, spot_reference_units, wire_units},
    seed::{NativeAsset, NativeEmas, NativeSeed, ReferenceCache, StoredMark},
};

const BUCKET_NS: u64 = 3_000 * 1_000_000;
const MEMBER_LIFETIME_NS: u64 = 10 * 1_000_000_000;
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
        // BucketGuard advances before aggregation (0x3ca3ece); only the median write requires a nonempty result
        // (0x42c13fe). In particular, an empty live ExternalPerp cache must cross action-free blocks too.
        self.last_bucket = bucket;
        let fresh: Vec<u64> =
            self.members.values().filter(|(_, time_ns)| block_ns.saturating_sub(*time_ns) < MEMBER_LIFETIME_NS).map(|(value, _)| *value).collect();
        if let Some(median) = median_units(&fresh) {
            self.median = Some(median);
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

    fn text(&self) -> String {
        let members = self.members.iter().map(|(user, (value, time_ns))| (*user, *value, *time_ns)).collect();
        cache_text(self.median, self.last_bucket, members)
    }
}

/// A reference cache as evidence text: median, bucket and the members (validator, value, block time ns) by validator.
fn cache_text(median: Option<u64>, last_bucket: u64, mut members: Vec<(Address, u64, u64)>) -> String {
    members.sort_unstable();
    format!("median {median:?} bucket {last_bucket} members {members:?}")
}

fn stored_cache_text(stored: &ReferenceCache) -> String {
    cache_text(stored.median, stored.last_bucket, stored.members.clone())
}

/// A checked price field: its name, whether the two sides are equal, and both values for the evidence (computed only
/// for a mismatch).
type CheckedField<'a> = (&'static str, bool, &'a dyn Fn() -> (String, String));

/// Both sides of a field as `Debug` text (floats print exactly: they read back to the same value).
fn debug_pair(replayed: &impl std::fmt::Debug, checkpoint: &impl std::fmt::Debug) -> (String, String) {
    (format!("{replayed:?}"), format!("{checkpoint:?}"))
}

/// One main-dex coin's prices.
#[derive(Debug, Clone)]
struct CoinPrices {
    name: String,
    halted: bool,
    size_decimals: u32,
    stored_oracle: u64,
    mark: StoredMark,
    emas: Option<NativeEmas>,
    spot: MemberCache,
    external_perp: MemberCache,
    marks: MemberCache,
    last_trade: u64,
    /// For a coin `psro` names: its spot book's price decimals (its Spot input is that book's midpoint).
    spot_reference_decimals: Option<u32>,
    unsupported: Option<&'static str>,
}

/// The book a price action reads for one coin, and the coin's last trade of the block before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CoinCut {
    pub(crate) best: BestPrices,
    /// For a coin `psro` names, its spot book at the cut.
    pub(crate) spot_reference: Option<BestPrices>,
    /// `None`: no trade of the coin in the block before the action, the last trade of earlier blocks holds.
    pub(crate) block_trade: Option<u64>,
}

impl CoinPrices {
    fn seeded(asset: NativeAsset, spot_reference_decimals: Option<u32>) -> Self {
        Self {
            name: asset.name,
            halted: asset.halted,
            size_decimals: asset.size_decimals,
            stored_oracle: asset.stored_oracle,
            mark: asset.mark,
            emas: asset.emas,
            spot: MemberCache::seeded(asset.references.spot),
            external_perp: MemberCache::seeded(asset.references.external_perp),
            marks: MemberCache::seeded(asset.references.mark),
            last_trade: asset.last_trade_px,
            spot_reference_decimals,
            unsupported: None,
        }
    }

    fn begin_block(&mut self, block_ns: u64) {
        if self.halted {
            return;
        }
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

    /// A validator's reference action with its wire prices for this coin (Codex 0x2da7470, 0x3c90750).
    fn reference(&mut self, user: Address, oracle: Option<&str>, external: Option<&str>, time_ns: u64, cut: CoinCut) -> Result<(), String> {
        let spot = self.spot_input(oracle, cut)?;
        let external = external.map(|px| wire_units(px, self.size_decimals)).transpose()?;
        if spot.is_none() && external.is_none() {
            // a `psro` coin the validator does not price whose spot book has no midpoint: no input for it (0x2da772e)
            return Ok(());
        }
        if let Some(spot) = spot {
            self.spot.insert(user, spot, time_ns);
        }
        if let Some(external) = external {
            self.external_perp.insert(user, external, time_ns);
        }
        let Some((bid, ask, local)) = self.local(cut) else { return Ok(()) };
        let Some(emas) = &self.emas else {
            self.unsupported.get_or_insert("noEma");
            return Ok(());
        };
        let preview = emas.basis.basis_advanced(bid.midpoint(ask), self.stored_oracle, time_ns);
        #[expect(clippy::cast_precision_loss, reason = "prices are below 2^53")]
        let a = spot.map(|spot| internal_round(spot as f64 * (1.0 + preview.basis_ratio()))).transpose()?;
        let mut candidates: Vec<u64> = [a, Some(local), external].into_iter().flatten().collect();
        if candidates.len() == 2 {
            let Some(filled) = self.local_price_fill(local, time_ns)? else { return Ok(()) };
            candidates.push(filled);
        }
        let member = median_units(&candidates).ok_or("no Mark member candidates")?;
        self.marks.insert(user, member, time_ns);
        Ok(())
    }

    /// The Spot input of a reference action: for a coin `psro` names, its spot book's midpoint at the cut (any price
    /// the validator sent for it is dropped, 0x2da76dd), else the validator's price.
    fn spot_input(&self, oracle: Option<&str>, cut: CoinCut) -> Result<Option<u64>, String> {
        let Some(price_decimals) = self.spot_reference_decimals else {
            return oracle.map(|px| wire_units(px, self.size_decimals)).transpose();
        };
        let book = cut.spot_reference.ok_or("no spot reference book at the cut")?;
        spot_reference_units(book, price_decimals, self.size_decimals)
    }

    /// The third Mark candidate of a two-candidate reference: lmpe previewed with the local price (its state stays),
    /// read as a price and rounded half to even (0x3c910f9, 0x3c91116); `None` (unsupported) after a gap longer than
    /// the decay or before the EMA is warm, never observed.
    fn local_price_fill(&mut self, local: u64, time_ns: u64) -> Result<Option<u64>, String> {
        let emas = self.emas.as_ref().ok_or("no EMAs")?;
        let Some(preview) = emas.local_price.local_price_advanced(self.px(local), time_ns) else {
            self.unsupported.get_or_insert("localPriceGap");
            return Ok(None);
        };
        let Some(average) = preview.warm_average() else {
            self.unsupported.get_or_insert("coldLocalPriceFill");
            return Ok(None);
        };
        wire_units(&format!("{average}"), self.size_decimals).map(Some)
    }

    /// Price units as a price.
    fn px(&self, units: u64) -> f64 {
        #[expect(clippy::cast_precision_loss, reason = "prices are below 2^53")]
        let px = units as f64 / 10_f64.powi((6 - self.size_decimals).cast_signed());
        px
    }

    /// A `SetGlobalAction`'s submitted (oracle, mark) for this coin.
    fn set_global(&mut self, submitted_oracle: u64, submitted_mark: u64, time_ns: u64, cut: CoinCut) -> Result<(), String> {
        let Some((bid, ask, local)) = self.local(cut) else { return Ok(()) };
        let (Some(spot_median), Some(mark_median), Some(emas)) = (self.spot.median, self.marks.median, self.emas.as_ref()) else {
            self.unsupported.get_or_insert("noCacheOrEma");
            return Ok(());
        };
        let stored = median_units(&[submitted_oracle, spot_median, inverse_basis(local, emas.basis.basis_ratio())?]).ok_or("no oracle candidates")?;
        let new_mark = median_units(&[submitted_mark, mark_median, local]).ok_or("no mark candidates")?;
        let Some(local_price_ema) = emas.local_price.local_price_advanced(self.px(local), time_ns) else {
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
    refresh: super::refresh::RefreshState,
    coins: Vec<CoinPrices>,
    by_name: FxHashMap<String, usize>,
    /// The assets `psro` names: every reference action gives them a Spot input.
    spot_referenced: Vec<usize>,
}

/// A checkpoint's prices against the replayed ones, field by field over the supported coins.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct PricesCheck {
    /// (field, equal, differ): stored oracle, mark, msoe, lmpe, Spot / `ExternalPerp` / Mark caches.
    pub(crate) fields: Vec<(&'static str, u64, u64)>,
    /// Coins whose replay is not claimed, by reason.
    pub(crate) unsupported: Vec<(&'static str, Vec<String>)>,
    pub(crate) samples: Vec<PriceMismatch>,
}

/// A main-dex coin's node price state at the applied height, as the local marks preview it (design 000454 r1 1.3):
/// the stored oracle and mark, the EMAs, the Mark reference cache's median and the last trade, in price units.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CoinPriceState {
    pub(crate) size_decimals: u32,
    pub(crate) stored_oracle: u64,
    pub(crate) stored_mark: u64,
    pub(crate) emas: NativeEmas,
    /// `None` before the cache has a median.
    pub(crate) mark_cache: Option<u64>,
    pub(crate) last_trade: u64,
}

/// A main-dex price field where the replay and the checkpoint differ, both values as evidence text.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PriceMismatch {
    pub(crate) coin: String,
    pub(crate) field: &'static str,
    pub(crate) replayed: String,
    pub(crate) checkpoint: String,
}

impl NativePrices {
    pub(crate) fn seeded(native: &NativeSeed) -> Self {
        let spot_decimals: FxHashMap<usize, u32> = native.spot_references.iter().map(|spot| (spot.asset, spot.price_decimals)).collect();
        let coins: Vec<CoinPrices> = native
            .assets
            .iter()
            .enumerate()
            .map(|(index, asset)| CoinPrices::seeded(asset.clone(), spot_decimals.get(&index).copied()))
            .collect();
        let by_name = coins.iter().enumerate().map(|(index, coin)| (coin.name.clone(), index)).collect();
        let spot_referenced = native.spot_references.iter().map(|spot| spot.asset).collect();
        Self { coins, by_name, spot_referenced, refresh: native.refresh.clone() }
    }

    pub(crate) fn begin_block(&mut self, block_ns: u64) {
        for coin in &mut self.coins {
            coin.begin_block(block_ns);
        }
    }

    /// Automatic selector at block start, before this block's book effects or fills.
    pub(crate) fn refresh_stale(&mut self, height: u64, time_ns: u64, cut_of: impl Fn(usize) -> CoinCut) -> Result<bool, String> {
        if !self.refresh.due(height, time_ns) { return Ok(false); }
        for (asset, pair) in self.refresh.submitted.iter().enumerate() {
            let Some((oracle, mark)) = *pair else { continue };
            let coin = &mut self.coins[asset];
            if coin.halted { continue; }
            coin.set_global(oracle, mark, time_ns, cut_of(asset)).map_err(|err| format!("{} automatic refresh: {err}", coin.name))?;
        }
        Ok(true)
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

    /// Stored oracle at the current state, subject to the same reconstruction gate as the mark.
    pub(crate) fn stored_oracle(&self, asset: usize) -> Option<u64> {
        let coin = &self.coins[asset];
        coin.unsupported.is_none().then_some(coin.stored_oracle)
    }

    /// Every coin's state the replay claims: not one unsupported, nor one without its EMAs.
    pub(crate) fn price_states(&self) -> impl Iterator<Item = (&str, CoinPriceState)> + '_ {
        self.coins.iter().filter(|coin| coin.unsupported.is_none()).filter_map(|coin| {
            let state = CoinPriceState {
                size_decimals: coin.size_decimals,
                stored_oracle: coin.stored_oracle,
                stored_mark: coin.mark.px,
                emas: coin.emas.clone()?,
                mark_cache: coin.marks.median,
                last_trade: coin.last_trade,
            };
            Some((coin.name.as_str(), state))
        })
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

    #[cfg(test)]
    pub(crate) fn spot_member(&self, asset: usize, user: Address) -> Option<(u64, u64)> {
        self.coins[asset].spot.members.get(&user).copied()
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
            MarkAction::Book(_) | MarkAction::VaultWithdraw { .. } | MarkAction::Hip3Oracle { .. } => Ok(()),
            MarkAction::SetGlobal(pxs) => {
                self.refresh.check_coverage(pxs.iter().map(Option::is_some))?;
                self.refresh.explicit(time_ns);
                for (asset, pair) in pxs.iter().enumerate() {
                    let Some((oracle, mark)) = pair else { continue };
                    let coin = self.coins.get_mut(asset).ok_or_else(|| format!("SetGlobalAction prices asset {asset} outside the universe"))?;
                    let pair = (wire_units(oracle, coin.size_decimals)?, wire_units(mark, coin.size_decimals)?);
                    self.refresh.submitted[asset] = Some(pair);
                    coin.set_global(pair.0, pair.1, time_ns, cut_of(asset)).map_err(|err| format!("{}: {err}", coin.name))?;
                }
                Ok(())
            }
            MarkAction::Reference { oracle, external } => {
                let user = user.ok_or("a reference action without a validator")?;
                let oracle: FxHashMap<&str, &str> = oracle.iter().map(|(coin, px)| (coin.as_str(), px.as_str())).collect();
                let external: FxHashMap<&str, &str> = external.iter().map(|(coin, px)| (coin.as_str(), px.as_str())).collect();
                // a coin outside the universe (listed since the seed) has no state yet
                let named = oracle.keys().chain(external.keys()).filter_map(|name| self.asset(name));
                let mut assets: Vec<usize> = named.chain(self.spot_referenced.iter().copied()).collect();
                assets.sort_unstable();
                assets.dedup();
                for asset in assets {
                    let coin = &mut self.coins[asset];
                    let name = coin.name.as_str();
                    let (oracle, external) = (oracle.get(name).copied(), external.get(name).copied());
                    coin.reference(user, oracle, external, time_ns, cut_of(asset)).map_err(|err| format!("{}: {err}", coin.name))?;
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
            let (basis, local_price) = (coin.emas.as_ref().map(|emas| &emas.basis), coin.emas.as_ref().map(|emas| &emas.local_price));
            let (stored_basis, stored_local_price) =
                (stored.emas.as_ref().map(|emas| &emas.basis), stored.emas.as_ref().map(|emas| &emas.local_price));
            let caches = &stored.references;
            let fields: [CheckedField<'_>; 7] = [
                ("storedOracle", coin.stored_oracle == stored.stored_oracle, &|| debug_pair(&coin.stored_oracle, &stored.stored_oracle)),
                ("mark", coin.mark == stored.mark, &|| debug_pair(&coin.mark, &stored.mark)),
                ("msoe", basis == stored_basis, &|| debug_pair(&basis, &stored_basis)),
                ("lmpe", local_price == stored_local_price, &|| debug_pair(&local_price, &stored_local_price)),
                ("spotCache", coin.spot.equals(&caches.spot), &|| (coin.spot.text(), stored_cache_text(&caches.spot))),
                ("externalPerpCache", coin.external_perp.equals(&caches.external_perp), &|| {
                    (coin.external_perp.text(), stored_cache_text(&caches.external_perp))
                }),
                ("markCache", coin.marks.equals(&caches.mark), &|| (coin.marks.text(), stored_cache_text(&caches.mark))),
            ];
            for (field, equal, values) in fields {
                let entry = counts.entry(field).or_default();
                if equal {
                    entry.0 += 1;
                    continue;
                }
                entry.1 += 1;
                if samples.len() < MAX_SAMPLES {
                    let (replayed, checkpoint) = values();
                    samples.push(PriceMismatch { coin: stored.name.clone(), field, replayed, checkpoint });
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
    use crate::accounts::marks::{book::tests::native_seed, seed::SpotReferenceBook};

    const VALIDATOR: Address = Address::repeat_byte(0xee);
    /// 2026-10-19T06:37:24 UTC, a second after the fixture's EMA samples.
    const T1: u64 = 1_792_391_844_000_000_000;

    fn cut(bid: u64, ask: u64, last_trade: u64) -> CoinCut {
        CoinCut { best: (Some(bid), Some(ask)), spot_reference: None, block_trade: Some(last_trade) }
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

    /// One price of two: lmpe previewed with L (82,000.0, one second after its last sample) is the third candidate,
    /// 82,310.02443917563 → 823,100 (an independent float replay of the node's operations), the median of
    /// (A 828,637, L 820,000, 823,100); lmpe itself does not move.
    #[test]
    fn a_reference_with_two_candidates_takes_lmpe_as_the_third() {
        let mut prices = NativePrices::seeded(&native_seed());
        let spot_only = MarkAction::Reference { oracle: vec![("BTC".to_string(), "82875.0".to_string())], external: Vec::new() };
        prices.apply(&spot_only, Some(VALIDATOR), T1, |_| cut(819_900, 820_100, 820_000)).unwrap();
        let btc = &prices.coins[0];
        assert_eq!((btc.marks.members[&VALIDATOR], btc.unsupported), ((823_100, T1), None));
        assert_eq!(btc.emas.as_ref().unwrap().local_price.samples, 269_229, "a preview only");
        let mut cold = native_seed();
        // 29 samples with the preview's, below the decay parameter 30
        cold.assets[0].emas.as_mut().unwrap().local_price.samples = 28;
        let mut prices = NativePrices::seeded(&cold);
        prices.apply(&spot_only, Some(VALIDATOR), T1, |_| cut(819_900, 820_100, 820_000)).unwrap();
        assert_eq!((prices.coins[0].marks.members.get(&VALIDATOR), prices.coins[0].unsupported), (None, Some("coldLocalPriceFill")));
    }

    /// BTC given a spot book of 3 price decimals, as `psro` gives HYPE and PURR theirs.
    fn spot_referenced_seed() -> NativeSeed {
        let mut seed = native_seed();
        let book = SpotReferenceBook { asset: 0, coin: "@1".to_string(), size_decimals: 5, price_decimals: 3, orders: Vec::new() };
        seed.spot_references.push(book);
        seed
    }

    fn spot_cut(spot: BestPrices) -> CoinCut {
        CoinCut { spot_reference: Some(spot), ..cut(828_500, 828_700, 828_600) }
    }

    /// Codex `hype_purr_internal_reference_20261020`: the Spot member is the book's floor midpoint, half to even in
    /// the perp's units (82,870.05 → 82,870.0; 82,870.15 → 82,870.2), whether or not the validator priced the coin;
    /// a price it sent is dropped. Without an external price (PURR) lmpe is the third Mark candidate: the median
    /// (A 828,612, L 828,600, lmpe 823,194) is 828,600, where two candidates would give 828,606.
    #[test]
    fn a_psro_coin_takes_its_spot_from_the_book_at_every_reference_action() {
        let mut prices = NativePrices::seeded(&spot_referenced_seed());
        let submitted = MarkAction::Reference {
            oracle: vec![("BTC".to_string(), "99999.0".to_string())],
            external: vec![("BTC".to_string(), "82865.0".to_string())],
        };
        prices.apply(&submitted, Some(VALIDATOR), T1, |_| spot_cut((Some(82_870_000), Some(82_870_101)))).unwrap();
        let btc = &prices.coins[0];
        assert_eq!(btc.spot.members[&VALIDATOR], (828_700, T1));
        assert_eq!(btc.marks.members[&VALIDATOR], (828_610, T1), "median(A 828610, L 828600, C 828650)");
        let other = Address::repeat_byte(1);
        let unpriced = MarkAction::Reference { oracle: Vec::new(), external: Vec::new() };
        prices.apply(&unpriced, Some(other), T1, |_| spot_cut((Some(82_870_000), Some(82_870_301)))).unwrap();
        let btc = &prices.coins[0];
        assert_eq!((btc.spot.members[&other], btc.marks.members[&other], btc.unsupported), ((828_702, T1), (828_600, T1), None));
        assert!(!btc.external_perp.members.contains_key(&other));
    }

    /// An action-free block still refreshes live empty/stale caches; a halted coin's three keys stay untouched.
    #[test]
    fn action_free_blocks_advance_live_caches_and_skip_halted_coins() {
        let mut seed = spot_referenced_seed();
        seed.assets[0].references.external_perp.members.clear();
        seed.assets[0].references.external_perp.median = None;
        let mut prices = NativePrices::seeded(&seed);
        let halted = prices.coins[1].clone();
        prices.begin_block(T1);
        let btc = &prices.coins[0];
        assert_eq!((btc.external_perp.last_bucket, btc.external_perp.median, btc.external_perp.members.len()), (T1 / BUCKET_NS, None, 0));
        let pump = &prices.coins[1];
        assert_eq!((&pump.spot, &pump.external_perp, &pump.marks), (&halted.spot, &halted.external_perp, &halted.marks));
        // Even stale members do not freeze a live key; do not confuse absent prices with an asset halt.
        prices.coins[0].external_perp.insert(VALIDATOR, 7, T1 - 12_000_000_000);
        prices.coins[0].external_perp.median = Some(5);
        prices.begin_block(T1 + BUCKET_NS);
        let btc = &prices.coins[0];
        assert_eq!((btc.external_perp.last_bucket, btc.external_perp.median), (T1 / BUCKET_NS + 1, Some(5)));
    }

    /// BM 1191810000 -> 1191820000: the endpoint crosses into a new bucket without any price action.
    /// PURR's empty `ExternalPerp` cache advances; halted MATIC (empty) and ARK (stale members) stay exactly equal.
    #[test]
    fn real_empty_and_halted_caches_match_the_action_free_checkpoint() {
        #[derive(serde::Deserialize)]
        struct Cache {
            last_bucket: u64,
            median: Option<u64>,
            members: Vec<(Address, u64, u64)>,
        }
        impl Cache {
            fn state(self) -> MemberCache {
                MemberCache::seeded(ReferenceCache { last_bucket: self.last_bucket, median: self.median, members: self.members })
            }
        }
        #[derive(serde::Deserialize)]
        struct Case { coin: String, halted: bool, before: Cache, after: Cache }
        #[derive(serde::Deserialize)]
        struct Golden { end_time_ns: u64, cases: Vec<Case> }
        let golden: Golden = serde_json::from_str(include_str!("reference_bucket_goldens.json")).unwrap();
        for case in golden.cases {
            let mut coin = NativePrices::seeded(&native_seed()).coins.remove(0);
            coin.halted = case.halted;
            coin.external_perp = case.before.state();
            coin.begin_block(golden.end_time_ns);
            assert_eq!(coin.external_perp, case.after.state(), "{}", case.coin);
        }
    }

    /// 0x2da772e: an empty side gives no Spot input. Unpriced otherwise, the coin has no input at all; with an
    /// external price the Mark member takes (L 828,600, C 828,650, lmpe 823,194).
    #[test]
    fn a_psro_coin_with_an_empty_spot_side_has_no_spot_input() {
        let mut prices = NativePrices::seeded(&spot_referenced_seed());
        let unpriced = MarkAction::Reference { oracle: Vec::new(), external: Vec::new() };
        prices.apply(&unpriced, Some(VALIDATOR), T1, |_| spot_cut((Some(82_870_000), None))).unwrap();
        let btc = &prices.coins[0];
        assert_eq!((btc.spot.members.get(&VALIDATOR), btc.marks.members.get(&VALIDATOR), btc.unsupported), (None, None, None));
        let external = MarkAction::Reference { oracle: Vec::new(), external: vec![("BTC".to_string(), "82865.0".to_string())] };
        prices.apply(&external, Some(VALIDATOR), T1, |_| spot_cut((None, Some(82_870_000)))).unwrap();
        let btc = &prices.coins[0];
        assert_eq!((btc.spot.members.get(&VALIDATOR), btc.marks.members[&VALIDATOR]), (None, (828_600, T1)));
    }

    #[test]
    fn unobserved_paths_make_a_coin_unsupported() {
        let mut prices = NativePrices::seeded(&native_seed());
        let set_global = MarkAction::SetGlobal(vec![Some(("82890.0".to_string(), "90000.0".to_string()))]);
        prices.apply(&set_global, None, T1, |_| CoinCut { best: (Some(828_500), None), spot_reference: None, block_trade: None }).unwrap();
        assert_eq!(prices.coins[0].unsupported, Some("emptyBookSide"));
    }

    /// The Main branch bypasses the Hip3 step limit in both directions.
    #[test]
    fn a_native_mark_takes_the_median_even_more_than_one_percent_away() {
        for (submitted, bid, ask, trade) in [("90000.0", 900_000, 900_100, 900_050), ("70000.0", 700_000, 700_100, 700_000)] {
            let mut prices = NativePrices::seeded(&native_seed());
            let set_global = MarkAction::SetGlobal(vec![Some(("82890.0".to_string(), submitted.to_string()))]);
            prices.apply(&set_global, None, T1, |_| cut(bid, ask, trade)).unwrap();
            assert_eq!(prices.mark_of(0), (StoredMark { px: bid, time_ns: T1 }, None));
        }
    }

    /// Expected marks come from the node's liquidation fills, not a checkpoint or our replay's output.
    /// Includes a cache-selected mark differing from the submitted mark (1192112978).
    #[test]
    fn alt_marks_at_liquidations_match_the_node_without_the_hip3_limit() {
        use crate::accounts::marks::seed::Ema;
        #[derive(serde::Deserialize)]
        struct FixtureEma {
            numerator: f64,
            denominator_s: f64,
            decay_parameter: f64,
            samples: u64,
            last_sample_ns: u64,
        }
        impl FixtureEma {
            fn state(self) -> Ema {
                Ema { numerator: self.numerator, denominator_s: self.denominator_s, decay_parameter: self.decay_parameter,
                    samples: self.samples, last_sample_ns: Some(self.last_sample_ns) }
            }
        }
        #[derive(serde::Deserialize)]
        struct Case {
            block: u64,
            coin: String,
            sz_decimals: u32,
            time_ns: u64,
            previous_mark_units: u64,
            previous_oracle_units: u64,
            submitted_oracle: String,
            submitted_mark: String,
            spot_cache_units: u64,
            mark_cache_units: u64,
            bid_units: u64,
            ask_units: u64,
            last_trade_units: u64,
            basis: FixtureEma,
            local_price_ema: FixtureEma,
            expected_mark: String,
        }
        #[derive(serde::Deserialize)]
        struct Golden { cases: Vec<Case> }
        let golden: Golden = serde_json::from_str(include_str!("native_mark_liquidation_goldens.json")).unwrap();
        assert_eq!(golden.cases.len(), 4);
        for case in golden.cases {
            let mut coin = NativePrices::seeded(&native_seed()).coins.remove(0);
            coin.name = case.coin;
            coin.size_decimals = case.sz_decimals;
            coin.mark.px = case.previous_mark_units;
            coin.stored_oracle = case.previous_oracle_units;
            coin.emas = Some(NativeEmas { basis: case.basis.state(), local_price: case.local_price_ema.state() });
            coin.spot.median = Some(case.spot_cache_units);
            coin.marks.median = Some(case.mark_cache_units);
            coin.set_global(wire_units(&case.submitted_oracle, coin.size_decimals).unwrap(), wire_units(&case.submitted_mark, coin.size_decimals).unwrap(), case.time_ns,
                cut(case.bid_units, case.ask_units, case.last_trade_units)).unwrap();
            assert_eq!(coin.unsupported, None, "block {}", case.block);
            assert_eq!(coin.mark, StoredMark { px: wire_units(&case.expected_mark, case.sz_decimals).unwrap(), time_ns: case.time_ns }, "block {}", case.block);
        }
    }

    /// A live key retains its median when members expire, but advances its bucket. The 56 coins whose buckets
    /// stay old in checkpoints are explicitly halted (including ARK/DOOD/IP/TON/VINE with stale members).
    #[test]
    fn live_caches_refresh_per_bucket_and_keep_only_the_median_without_fresh_members() {
        let bucket = T1 / BUCKET_NS;
        let mut cache = MemberCache { members: FxHashMap::default(), median: Some(7), last_bucket: bucket };
        cache.insert(VALIDATOR, 828_000, T1);
        cache.insert(Address::repeat_byte(1), 828_100, T1 - 9_000_000_000);
        cache.begin_block(T1 + 1);
        assert_eq!((cache.median, cache.last_bucket), (Some(7), bucket), "the same bucket: no refresh");
        cache.begin_block(T1 + BUCKET_NS);
        assert_eq!((cache.median, cache.last_bucket), (Some(828_000), bucket + 1), "the 12 s old member is out");
        cache.begin_block(T1 + 20_000_000_000);
        assert_eq!((cache.median, cache.last_bucket), (Some(828_000), (T1 + 20_000_000_000) / BUCKET_NS), "no fresh member: only the median stays");
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
        let sample = PriceMismatch { coin: "BTC".to_string(), field: "storedOracle", replayed: "828908".to_string(), checkpoint: "828909".to_string() };
        assert_eq!(check.samples, [sample]);
        moved.assets[0].references.mark.median = Some(1);
        let check = prices.check_against(&moved);
        let validator = moved.assets[0].references.mark.members[0].0;
        assert_eq!((check.samples[1].field, check.samples[1].replayed.as_str(), check.samples[1].checkpoint.as_str()), (
            "markCache",
            format!("median Some(828590) bucket 597463947 members [({validator:?}, 828590, 1792391842000000000)]").as_str(),
            format!("median Some(1) bucket 597463947 members [({validator:?}, 828590, 1792391842000000000)]").as_str(),
        ));
    }
}
