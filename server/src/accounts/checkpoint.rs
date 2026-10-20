//! Account state of one node checkpoint (`periodic_abci_states/<day>/<height>.rmp`: `MessagePack`, string keys).
//!
//! Only the fields the liquidation computation needs are read; every other key is skipped. The same pass reads the
//! node marks' seed (`marks::seed`: the main dex's oracle state and books), which is checked there. The meanings below were
//! measured against the node's own answers (research `hl_account_state_verify_20261019`, design 000419 r4), not taken
//! from an official schema, so a missing required field fails the parse instead of being guessed. The node omits
//! zero values (`u`, `s`): an absent one is 0, which the checkpoint-to-checkpoint cash reconciliation confirmed.

use std::{fmt, fs::File, io::BufReader, path::Path, str::FromStr};

use alloy::primitives::Address;
use chrono::NaiveDateTime;
use rustc_hash::FxHashMap;
use rustc_hash::FxHashSet;
use serde::{
    Deserialize, Deserializer,
    de::{DeserializeSeed, Error as _, IgnoredAny, MapAccess, SeqAccess, Visitor},
};

use super::{
    margin::MarginTable,
    marks::seed::{WireNativeBooks, WireNativePrices, WireOracle, WirePx},
};
use crate::prelude::Result;

/// A position omits `M` while it still uses the single-tier 20x table: on 2026-10-19 every such position the node was
/// asked about answered `maxLeverage` 20 (488 main-dex positions over 34 coins; 16 coins sampled). Measured snapshot
/// format, not an official guarantee. A present `M` always wins; the asset's current table is never a fallback.
pub(crate) const OMITTED_POSITION_TABLE: u32 = 20;
/// Table ids below this are an implicit single tier whose max leverage is the id.
const FIRST_LISTED_TABLE: u32 = 50;
/// Asset ids inside a clearinghouse's user states are `PDI_ASSET_STRIDE * pdi + index`.
pub(crate) const PDI_ASSET_STRIDE: u32 = 10_000;
/// Perp prices carry `MAX_PERP_DECIMALS - szDecimals` decimals, so `szDecimals` cannot exceed it.
pub(crate) const MAX_PERP_DECIMALS: u32 = 6;

/// A perp's mark in USD from its price units (10^-(6 - szDecimals)).
pub(crate) fn mark_px(mark_raw: u64, size_decimals: u32) -> f64 {
    mark_raw as f64 / 10f64.powi((MAX_PERP_DECIMALS - size_decimals).cast_signed())
}

/// A signed size in base units from its raw units (10^-szDecimals).
pub(crate) fn base_size(size_raw: i64, size_decimals: u32) -> f64 {
    size_raw as f64 / 10f64.powi(size_decimals.cast_signed())
}
/// Spot prices (a reserve's `o`, a spot fill's `px`) carry `MAX_SPOT_DECIMALS - szDecimals` decimals; USDC
/// (szDecimals 8) stores 1.
pub(crate) const MAX_SPOT_DECIMALS: u32 = 8;
/// Borrow-lend pool indices and LTVs are fixed point with this scale.
const LENDING_SCALE: f64 = 1e14;

// ─── domain types ───────────────────────────────────────────────────────────

#[derive(Debug)]
pub(crate) struct CheckpointAccounts {
    pub(crate) height: u64,
    pub(crate) time_ms: u64,
    pub(crate) dexes: Vec<DexMeta>,
    /// Spot token of every dex's collateral (each dex uses exactly one: `meta.collateralToken`).
    pub(crate) collateral: FxHashMap<u32, CollateralToken>,
    /// Borrow-lend reserves (`blp.r.t`) by spot token: what portfolio-margin accounts borrow and supply.
    pub(crate) reserves: FxHashMap<u32, LendingReserve>,
    /// Every account holding at least one non-zero position on some dex, with all its dex states (cash-only ones
    /// included: a unified account's pool counts the cash of every same-collateral dex).
    pub(crate) accounts: FxHashMap<Address, Account>,
    /// Every spot pair by index: the `N` of a spot fill's coin `@N` (`scl.meta.spot_infos[N].tokens = [base, quote]`;
    /// on 1190480000 the node's `spotMeta` listed 336 pairs, every one at its index here).
    pub(crate) spot_pairs: Vec<SpotPair>,
    /// The first clearinghouse's oracle state and the main dex's books, for `MarkSeed::from_checkpoint`.
    pub(crate) native_prices: WireNativePrices,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpotPair {
    /// The base token's `szDecimals`: fill sizes carry these, prices `MAX_SPOT_DECIMALS` minus these.
    pub(crate) size_decimals: u32,
    pub(crate) quote_token: u32,
}

#[derive(Debug)]
pub(crate) struct DexMeta {
    /// "" for the main dex, else the coin prefix of a HIP-3 dex ("xyz").
    pub(crate) name: String,
    /// The perp dex index (`meta.pdi`, 0 for the main dex).
    pub(crate) pdi: u32,
    pub(crate) collateral_token: u32,
    pub(crate) assets: Vec<AssetMeta>,
    tables: FxHashMap<u32, MarginTable>,
}

impl DexMeta {
    /// Every table a parsed position refers to resolves here (checked at parse time).
    pub(crate) fn table(&self, table_id: u32) -> &MarginTable {
        &self.tables[&table_id]
    }

    /// Every table a position or an asset of this dex refers to.
    pub(crate) const fn tables(&self) -> &FxHashMap<u32, MarginTable> {
        &self.tables
    }

    /// The node's maintenance of `position` held at the stored mark, micro-USD: raw size (10^-szDecimals) times stored
    /// mark (10^-(6 - szDecimals)) is the exact notional in micro-USD. `None` without a stored mark.
    pub(crate) fn maintenance_micro(&self, position: &Position) -> Option<i128> {
        let mark_raw = self.assets[position.asset].mark_raw?;
        let notional_micro = u128::from(position.size_raw.unsigned_abs()) * u128::from(mark_raw);
        Some(self.table(position.table_id).maintenance_micro(notional_micro))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CollateralToken {
    pub(crate) name: String,
    /// 10^weiDecimals: spot balances `t` of this token divided by this are whole tokens (`scl.meta.token_infos[token]
    /// .spec.weiDecimals`, index = token id; USDC, USDE, USDT0, USDH are all 8 and match the node's spotMeta).
    pub(crate) spot_scale: f64,
}

/// One borrow-lend reserve (`blp.r.t[token]`) with units applied: indices and LTV divided by 1e14, caps in whole
/// tokens, the price in USDC. Each meaning was matched against the node's spot answers on 2026-10-19 (research O2
/// "checkpoint 里的输入"): `o` equals `allBorrowLendReserveStates.oraclePx` digit for digit, caps equal what
/// `tokenToPortfolio{Borrow,Supply}Ratio` imply.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LendingReserve {
    /// `l[0].i`: base units (10^-weiDecimals) one borrow share is worth.
    pub(crate) borrow_index: f64,
    /// `l[1].i`: base units one supply share is worth.
    pub(crate) supply_index: f64,
    /// `l[0].u`, per account, whole tokens; `None` when the side has none (HYPE and UBTC cannot be borrowed).
    pub(crate) borrow_cap: Option<f64>,
    /// `l[1].u`, per account, whole tokens.
    pub(crate) supply_cap: Option<f64>,
    /// `v`; null (the borrowable tokens) is 0.
    pub(crate) ltv: f64,
    /// `o / 10^(8 - szDecimals)`, USDC per token.
    pub(crate) oracle_px: f64,
    /// 10^weiDecimals: base units per whole token (spot `t` and shares × index are base units).
    pub(crate) wei_scale: f64,
}

/// A portfolio-margin account's shares in one reserve (`blp.u[user].t[token] = [borrow, supply]`, field `s`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Lending {
    pub(crate) token: u32,
    pub(crate) borrow_shares: u64,
    pub(crate) supply_shares: u64,
}

#[derive(Debug)]
pub(crate) struct AssetMeta {
    pub(crate) name: String,
    /// `szDecimals`: raw sizes are integers in 10^-size_decimals base units (at most `MAX_PERP_DECIMALS`).
    pub(crate) size_decimals: u32,
    /// `oracle.pxs2[asset][1].p`: the node's applied mark at the checkpoint, an integer in 10^-(6 - szDecimals) USD
    /// (slots are [oracle, mark, external perp]; slot 1 matched the node mark for 10/10 main and 306/306 HIP-3 coins).
    /// `None` when the asset stores no price.
    pub(crate) mark_raw: Option<u64>,
    /// `pxs2[asset][1].u`: when the node set the stored mark (block time, unix ns); `None` without one.
    pub(crate) mark_time_ns: Option<u64>,
    pub(crate) margin_mode: AssetMarginMode,
    /// `marginTableId`: the table a position opened from zero takes (research "新开仓位的档位表", 603/603); it resolves
    /// in the dex's tables (checked at parse time).
    pub(crate) margin_table_id: u32,
}

/// A perp's `marginMode`: which margin a new position takes. Measured, not documented (research
/// `hl_account_state_verify_20261019` "新开仓位是全仓还是逐仓", 2,459 of 2,459 new positions over three intervals).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AssetMarginMode {
    /// `normal`: the account's setting for the asset, cross unless it was set isolated.
    Normal,
    /// `noCross`, `strictIsolated`: always isolated.
    IsolatedOnly,
    Unknown(String),
}

impl AssetMarginMode {
    fn of(code: &str) -> Self {
        match code {
            "normal" => Self::Normal,
            "noCross" | "strictIsolated" => Self::IsolatedOnly,
            other => Self::Unknown(other.to_string()),
        }
    }
}

impl AssetMeta {
    /// The stored mark in USD.
    pub(crate) fn mark_px(&self) -> Option<f64> {
        self.mark_raw.map(|raw| mark_px(raw, self.size_decimals))
    }

    /// Signed size in base units.
    pub(crate) fn size(&self, size_raw: i64) -> f64 {
        base_size(size_raw, self.size_decimals)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AccountMode {
    /// `ust.a` absent.
    Default,
    /// `ust.a == "i"`.
    Disabled,
    /// `ust.a == "u"`.
    Unified,
    /// `ust.a == "p"`.
    PortfolioMargin,
    /// Any other code (dexAbstraction has no observed code), or no `ust` entry at all.
    Unknown(String),
}

#[derive(Debug)]
pub(crate) struct Account {
    pub(crate) mode: AccountMode,
    /// Non-zero spot balances, (token, raw `t` in 10^-weiDecimals), sorted by token: the dexes' collateral tokens, and
    /// for a portfolio-margin account also every reserve token (`t` includes what open spot orders hold).
    pub(crate) spot: Vec<(u32, i64)>,
    /// Portfolio-margin accounts only: non-zero borrow-lend shares, sorted by token.
    pub(crate) lending: Vec<Lending>,
    pub(crate) dex_states: Vec<DexAccount>,
}

#[derive(Debug)]
pub(crate) struct DexAccount {
    pub(crate) dex: usize,
    /// Cross cash `u`, micro units of the dex's collateral token.
    pub(crate) cash_micro: i64,
    pub(crate) positions: Vec<Position>,
    /// Assets (indices into the dex's `assets`) without a position whose leverage entry the account keeps isolated
    /// (`l.I`): a position opened there is isolated. A cross entry (`l.C`) or none means cross.
    pub(crate) isolated_settings: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Position {
    /// Index into the dex's `assets`.
    pub(crate) asset: usize,
    /// Signed size in 10^-szDecimals.
    pub(crate) size_raw: i64,
    pub(crate) margin: PositionMargin,
    pub(crate) table_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PositionMargin {
    Cross,
    /// Isolated cash `l.I.u`, micro units of the collateral token.
    Isolated { cash_micro: i64 },
}

// ─── wire types (only the fields read) ──────────────────────────────────────

#[derive(Deserialize)]
struct WireCheckpoint {
    exchange: WireExchange,
}

/// `locus` precedes `perp_dexs` on disk (1190680000); the order does not matter to the parse.
#[derive(Deserialize)]
struct WireExchange {
    locus: ParsedLocus,
    perp_dexs: WireNativeBooks,
}

#[derive(Deserialize)]
struct WireCtx {
    height: u64,
    time: String,
}

#[derive(Deserialize)]
struct WireClearinghouse {
    meta: WireMeta,
    oracle: WireOracle,
    user_states: WireUserStates,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireMeta {
    universe: Vec<WireAsset>,
    pdi: u32,
    margin_table_id_to_margin_table: Vec<(u32, WireMarginTable)>,
    collateral_token: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireAsset {
    name: String,
    sz_decimals: u32,
    margin_mode: String,
    margin_table_id: u32,
}

#[derive(Deserialize)]
struct WireMarginTable {
    margin_tiers: Vec<WireTier>,
}

#[derive(Deserialize)]
struct WireTier {
    lower_bound: i64,
    max_leverage: u32,
    maintenance_deduction: i64,
}

#[derive(Deserialize)]
struct WireUserStates {
    user_to_state: DexUsers,
}

#[derive(Deserialize)]
struct WireUserState {
    #[serde(default)]
    u: i64,
    p: Option<WirePositions>,
}

#[derive(Deserialize)]
struct WirePositions {
    p: Vec<(u32, WirePosition)>,
}

#[derive(Deserialize)]
struct WirePosition {
    #[serde(default)]
    s: i64,
    l: Option<WireLeverage>,
    #[serde(rename = "M")]
    table: Option<u32>,
}

#[derive(Deserialize)]
struct WireLeverage {
    #[serde(rename = "I")]
    isolated: Option<WireIsolated>,
}

#[derive(Deserialize)]
struct WireIsolated {
    u: i64,
}

#[derive(Deserialize)]
struct WireSpotMeta {
    token_infos: Vec<WireTokenInfo>,
    spot_infos: Vec<WireSpotInfo>,
}

#[derive(Deserialize)]
struct WireSpotInfo {
    /// [base, quote]
    tokens: (u32, u32),
}

#[derive(Deserialize)]
struct WireTokenInfo {
    spec: WireTokenSpec,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireTokenSpec {
    name: String,
    wei_decimals: u32,
    sz_decimals: u32,
}

#[derive(Deserialize)]
struct WireSpotUser {
    /// Absent for a spot user without balances (the node omits empty values; seen on mainnet 10-19).
    #[serde(default)]
    b: Vec<(u32, WireBalance)>,
}

#[derive(Deserialize)]
struct WireBalance {
    /// Unsigned on the wire: token deployers / genesis entries reach `u64::MAX` on mainnet (10-19, never on a
    /// collateral token).
    #[serde(default)]
    t: u64,
}

#[derive(Deserialize)]
struct WireUserMode {
    a: Option<String>,
}

/// `blp`: the reserves and every account's shares (about 23,000 accounts on 2026-10-19, so read whole and narrowed
/// in `build`, in whatever order the section comes).
#[derive(Deserialize)]
struct WireLending {
    r: WireReserves,
    u: LendingUsers,
}

/// `blp.u`: shares per account, every token.
struct LendingUsers(FxHashMap<Address, Vec<WireTokenShares>>);

impl<'de> Deserialize<'de> for LendingUsers {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Pairs::new(|_: &Address, entry: WireLendingUser| Some(entry.t)).deserialize(deserializer).map(Self)
    }
}

/// `blp.r`: the reserves.
#[derive(Deserialize)]
struct WireReserves {
    t: Vec<(u32, WireReserve)>,
}

#[derive(Deserialize)]
struct WireReserve {
    /// [borrow side, supply side].
    l: (WireReserveSide, WireReserveSide),
    v: Option<u64>,
    o: u64,
}

#[derive(Deserialize)]
struct WireReserveSide {
    i: u64,
    u: Option<u64>,
}

/// One reserve's (token, (borrow, supply)) shares of an account.
type WireTokenShares = (u32, (WireShares, WireShares));

/// `blp.u[user]`.
#[derive(Deserialize)]
struct WireLendingUser {
    #[serde(default)]
    t: Vec<WireTokenShares>,
}

#[derive(Deserialize)]
struct WireShares {
    #[serde(default)]
    s: u64,
}

/// One dex's `user_to_state`, kept compact: users with non-zero cash, a non-zero position or an isolated setting.
struct DexUsers(FxHashMap<Address, WireDexUser>);

/// One user's state on a dex: cash, non-zero positions, and the asset ids of the zero-size entries set isolated.
struct WireDexUser {
    cash_micro: i64,
    positions: Vec<(u32, WirePosition)>,
    isolated_settings: Vec<u32>,
}

/// `exchange.locus`, read in its on-disk order (ctx, cls, scl, ..., ust): once `cls` is read, spot balances and modes
/// keep only the position holders. Another order fails the parse (`blp` may come anywhere).
struct ParsedLocus {
    ctx: WireCtx,
    cls: Vec<WireClearinghouse>,
    spot: SpotState,
    modes: FxHashMap<Address, Option<String>>,
    lending: WireLending,
}

struct SpotState {
    /// `meta.token_infos[token].spec`, indexed by token id.
    tokens: Vec<WireTokenSpec>,
    /// `meta.spot_infos[pair].tokens`, indexed by pair.
    pairs: Vec<(u32, u32)>,
    /// The position holders' non-zero balances, every token (raw, unsigned on the wire): which tokens an account
    /// keeps depends on its mode, known only once `ust` and `blp` are read (`build` narrows).
    balances: FxHashMap<Address, Vec<(u32, u64)>>,
}

fn address<E: serde::de::Error>(text: &str) -> std::result::Result<Address, E> {
    Address::from_str(text).map_err(|err| E::custom(format!("bad account address {text:?}: {err}")))
}

/// Visits a `[[address, entry], ...]` sequence without materializing it, keeping what `keep` returns.
struct Pairs<T, F> {
    keep: F,
    marker: std::marker::PhantomData<T>,
}

impl<T, F> Pairs<T, F> {
    const fn new(keep: F) -> Self {
        Self { keep, marker: std::marker::PhantomData }
    }
}

impl<'de, T: Deserialize<'de>, V, F: FnMut(&Address, T) -> Option<V>> DeserializeSeed<'de> for Pairs<T, F> {
    type Value = FxHashMap<Address, V>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error> {
        deserializer.deserialize_seq(self)
    }
}

impl<'de, T: Deserialize<'de>, V, F: FnMut(&Address, T) -> Option<V>> Visitor<'de> for Pairs<T, F> {
    type Value = FxHashMap<Address, V>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a sequence of [address, entry] pairs")
    }

    fn visit_seq<A: SeqAccess<'de>>(mut self, mut seq: A) -> std::result::Result<Self::Value, A::Error> {
        let mut out = FxHashMap::default();
        while let Some((user, entry)) = seq.next_element::<(String, T)>()? {
            let user = address(&user)?;
            if let Some(value) = (self.keep)(&user, entry) {
                out.insert(user, value);
            }
        }
        Ok(out)
    }
}

impl<'de> Deserialize<'de> for DexUsers {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Pairs::new(|_: &Address, state: WireUserState| {
            let (positions, settings): (Vec<_>, Vec<_>) =
                state.p.map(|p| p.p).unwrap_or_default().into_iter().partition(|(_, position)| position.s != 0);
            let isolated_settings: Vec<u32> = settings
                .into_iter()
                .filter(|(_, entry)| entry.l.as_ref().is_some_and(|leverage| leverage.isolated.is_some()))
                .map(|(asset_id, _)| asset_id)
                .collect();
            let kept = state.u != 0 || !positions.is_empty() || !isolated_settings.is_empty();
            kept.then_some(WireDexUser { cash_micro: state.u, positions, isolated_settings })
        })
        .deserialize(deserializer)
        .map(Self)
    }
}

impl<'de> Deserialize<'de> for ParsedLocus {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        deserializer.deserialize_map(LocusVisitor)
    }
}

struct LocusVisitor;

impl<'de> Visitor<'de> for LocusVisitor {
    type Value = ParsedLocus;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the exchange.locus map")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<Self::Value, A::Error> {
        let (mut ctx, mut cls, mut holders, mut spot, mut modes, mut lending) = (None, None, None, None, None, None);
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "ctx" => ctx = Some(map.next_value()?),
                "cls" => {
                    let dexes = map.next_value::<Vec<WireClearinghouse>>()?;
                    holders = Some(holders_of(&dexes));
                    cls = Some(dexes);
                }
                "scl" => spot = Some(map.next_value_seed(SpotSeed { holders: after_cls(holders.as_ref(), "scl")? })?),
                "ust" => {
                    let held = after_cls(holders.as_ref(), "ust")?;
                    let keep = |user: &Address, mode: WireUserMode| held.contains(user).then_some(mode.a);
                    modes = Some(map.next_value_seed(Pairs::new(keep))?);
                }
                "blp" => lending = Some(map.next_value::<WireLending>()?),
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(ParsedLocus {
            ctx: ctx.ok_or_else(|| A::Error::missing_field("ctx"))?,
            cls: cls.ok_or_else(|| A::Error::missing_field("cls"))?,
            spot: spot.ok_or_else(|| A::Error::missing_field("scl"))?,
            modes: modes.ok_or_else(|| A::Error::missing_field("ust"))?,
            lending: lending.ok_or_else(|| A::Error::missing_field("blp"))?,
        })
    }
}

/// The holders, once `cls` has been read: sections after it keep only their entries.
fn after_cls<'h, E: serde::de::Error>(holders: Option<&'h FxHashSet<Address>>, key: &str) -> std::result::Result<&'h FxHashSet<Address>, E> {
    holders.ok_or_else(|| E::custom(format!("locus.{key} precedes locus.cls")))
}

/// Users with a non-zero position on some dex.
fn holders_of(dexes: &[WireClearinghouse]) -> FxHashSet<Address> {
    dexes
        .iter()
        .flat_map(|dex| dex.user_states.user_to_state.0.iter())
        .filter(|(_, state)| !state.positions.is_empty())
        .map(|(user, _)| *user)
        .collect()
}

/// `scl`: every token's spec from `meta.token_infos`, every pair's quote from `meta.spot_infos` and the position holders'
/// non-zero balances from `user_states`.
struct SpotSeed<'a> {
    holders: &'a FxHashSet<Address>,
}

impl<'de> DeserializeSeed<'de> for SpotSeed<'_> {
    type Value = SpotState;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for SpotSeed<'_> {
    type Value = SpotState;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the exchange.locus.scl map")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<Self::Value, A::Error> {
        let (mut meta, mut balances) = (None, None);
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "meta" => meta = Some(map.next_value::<WireSpotMeta>()?),
                "user_states" => {
                    let keep = |user: &Address, wire: WireSpotUser| {
                        if !self.holders.contains(user) {
                            return None;
                        }
                        let kept: Vec<(u32, u64)> =
                            wire.b.into_iter().filter(|(_, balance)| balance.t != 0).map(|(token, balance)| (token, balance.t)).collect();
                        (!kept.is_empty()).then_some(kept)
                    };
                    balances = Some(map.next_value_seed(Pairs::new(keep))?);
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        let meta = meta.ok_or_else(|| A::Error::missing_field("scl.meta"))?;
        Ok(SpotState {
            tokens: meta.token_infos.into_iter().map(|info| info.spec).collect(),
            pairs: meta.spot_infos.into_iter().map(|info| info.tokens).collect(),
            balances: balances.ok_or_else(|| A::Error::missing_field("scl.user_states"))?,
        })
    }
}

// ─── parse ──────────────────────────────────────────────────────────────────

/// Parse one checkpoint file, streaming (the 1.8 GB file is never held in memory).
pub(crate) fn parse_checkpoint_file(path: &Path) -> Result<CheckpointAccounts> {
    let file = File::open(path).map_err(|err| format!("open checkpoint {}: {err}", path.display()))?;
    let reader = BufReader::with_capacity(1 << 20, file);
    let wire: WireCheckpoint =
        rmp_serde::from_read(reader).map_err(|err| format!("decode checkpoint {}: {err}", path.display()))?;
    build(wire.exchange).map_err(|err| format!("checkpoint {}: {err}", path.display()).into())
}

/// Parse an in-memory checkpoint (tests).
#[cfg(test)]
pub(crate) fn parse_checkpoint_bytes(bytes: &[u8]) -> Result<CheckpointAccounts> {
    let wire: WireCheckpoint = rmp_serde::from_slice(bytes).map_err(|err| format!("decode checkpoint: {err}"))?;
    build(wire.exchange).map_err(Into::into)
}

fn build(exchange: WireExchange) -> std::result::Result<CheckpointAccounts, String> {
    let WireExchange { locus, perp_dexs } = exchange;
    let time_ms = node_time_ns(&locus.ctx.time).map_err(|err| format!("ctx.time: {err}"))? / 1_000_000;
    let mut dexes = Vec::with_capacity(locus.cls.len());
    let mut dex_users = Vec::with_capacity(locus.cls.len());
    let mut first_oracle = None;
    for WireClearinghouse { meta, oracle, user_states } in locus.cls {
        let pdi = meta.pdi;
        let (meta, users) = build_dex(meta, &oracle.pxs2, user_states).map_err(|err| format!("dex pdi {pdi}: {err}"))?;
        first_oracle.get_or_insert(oracle);
        dexes.push(meta);
        dex_users.push(users);
    }
    let native_prices = WireNativePrices { oracle: first_oracle.ok_or("locus.cls is empty")?, books: perp_dexs.0 };
    let holders: Vec<Address> = {
        let mut holders: Vec<Address> = dex_users
            .iter()
            .flat_map(|users| users.iter().filter(|(_, state)| !state.positions.is_empty()).map(|(user, _)| *user))
            .collect();
        holders.sort_unstable();
        holders.dedup();
        holders
    };
    let collateral = collateral_tokens(&dexes, &locus.spot.tokens)?;
    let reserves = lending_reserves(&locus.lending.r.t, &locus.spot.tokens)?;
    let mut lending_users = locus.lending.u.0;
    let mut accounts = FxHashMap::default();
    for user in holders {
        let dex_states = dex_users
            .iter_mut()
            .enumerate()
            .filter_map(|(dex, users)| users.remove(&user).map(|state| DexAccount { dex, ..state }))
            .collect();
        let mode = account_mode(locus.modes.get(&user));
        let portfolio = mode == AccountMode::PortfolioMargin;
        let kept = |token: u32| collateral.contains_key(&token) || (portfolio && reserves.contains_key(&token));
        let spot = spot_balances(&user, locus.spot.balances.get(&user), kept)?;
        // an account that never borrowed or supplied has no blp.u entry (the node omits empty values)
        let lending = match lending_users.remove(&user) {
            Some(wire) if portfolio => lending_shares(&user, wire, &reserves)?,
            _ => Vec::new(),
        };
        accounts.insert(user, Account { mode, spot, lending, dex_states });
    }
    let spot_pairs = spot_pairs(&locus.spot.pairs, &locus.spot.tokens)?;
    Ok(CheckpointAccounts { height: locus.ctx.height, time_ms, dexes, collateral, reserves, accounts, spot_pairs, native_prices })
}

/// A node time (`2026-10-19T22:09:38.119428098`, UTC, up to nanoseconds) in unix ns.
pub(crate) fn node_time_ns(text: &str) -> std::result::Result<u64, String> {
    let time = NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f").map_err(|err| format!("time {text:?}: {err}"))?;
    time.and_utc()
        .timestamp_nanos_opt()
        .and_then(|nanos| u64::try_from(nanos).ok())
        .ok_or_else(|| format!("time {text:?} outside unix ns"))
}

fn spot_pairs(pairs: &[(u32, u32)], tokens: &[WireTokenSpec]) -> std::result::Result<Vec<SpotPair>, String> {
    pairs
        .iter()
        .enumerate()
        .map(|(index, (base, quote))| {
            let size_decimals = token_spec(tokens, *base).map_err(|err| format!("spot pair {index} base {err}"))?.sz_decimals;
            token_spec(tokens, *quote).map_err(|err| format!("spot pair {index} quote {err}"))?;
            if size_decimals > MAX_SPOT_DECIMALS {
                return Err(format!("spot pair {index}: base szDecimals {size_decimals} above {MAX_SPOT_DECIMALS}"));
            }
            Ok(SpotPair { size_decimals, quote_token: *quote })
        })
        .collect()
}

fn account_mode(code: Option<&Option<String>>) -> AccountMode {
    match code {
        None => AccountMode::Unknown("<no ust entry>".to_string()),
        Some(None) => AccountMode::Default,
        Some(Some(code)) => match code.as_str() {
            "i" => AccountMode::Disabled,
            "u" => AccountMode::Unified,
            "p" => AccountMode::PortfolioMargin,
            other => AccountMode::Unknown(other.to_string()),
        },
    }
}

fn token_spec(tokens: &[WireTokenSpec], token: u32) -> std::result::Result<&WireTokenSpec, String> {
    usize::try_from(token)
        .ok()
        .and_then(|index| tokens.get(index))
        .ok_or_else(|| format!("token {token} missing from scl.meta.token_infos"))
}

fn wei_scale(spec: &WireTokenSpec, token: u32) -> std::result::Result<f64, String> {
    let decimals = i32::try_from(spec.wei_decimals).map_err(|err| format!("token {token} weiDecimals: {err}"))?;
    Ok(10f64.powi(decimals))
}

/// The tokens the dexes settle in.
fn collateral_tokens(dexes: &[DexMeta], tokens: &[WireTokenSpec]) -> std::result::Result<FxHashMap<u32, CollateralToken>, String> {
    let mut out = FxHashMap::default();
    for token in dexes.iter().map(|dex| dex.collateral_token) {
        let spec = token_spec(tokens, token).map_err(|err| format!("collateral {err}"))?;
        out.insert(token, CollateralToken { name: spec.name.clone(), spot_scale: wei_scale(spec, token)? });
    }
    Ok(out)
}

fn lending_reserves(
    wire: &[(u32, WireReserve)],
    tokens: &[WireTokenSpec],
) -> std::result::Result<FxHashMap<u32, LendingReserve>, String> {
    wire.iter()
        .map(|(token, reserve)| {
            lending_reserve(*token, reserve, tokens).map(|reserve| (*token, reserve)).map_err(|err| format!("blp.r.t[{token}]: {err}"))
        })
        .collect()
}

fn lending_reserve(token: u32, wire: &WireReserve, tokens: &[WireTokenSpec]) -> std::result::Result<LendingReserve, String> {
    let spec = token_spec(tokens, token)?;
    let wei_scale = wei_scale(spec, token)?;
    let price_decimals = MAX_SPOT_DECIMALS
        .checked_sub(spec.sz_decimals)
        .ok_or_else(|| format!("szDecimals {} above {MAX_SPOT_DECIMALS}", spec.sz_decimals))?;
    let (borrow, supply) = &wire.l;
    if wire.o == 0 || borrow.i == 0 || supply.i == 0 {
        return Err(format!("zero oracle price {} or pool index {} / {}", wire.o, borrow.i, supply.i));
    }
    let cap = |side: &WireReserveSide| side.u.map(|units| units as f64 / wei_scale);
    Ok(LendingReserve {
        borrow_index: borrow.i as f64 / LENDING_SCALE,
        supply_index: supply.i as f64 / LENDING_SCALE,
        borrow_cap: cap(borrow),
        supply_cap: cap(supply),
        ltv: wire.v.map_or(0.0, |ltv| ltv as f64 / LENDING_SCALE),
        oracle_px: wire.o as f64 / 10f64.powi(price_decimals.cast_signed()),
        wei_scale,
    })
}

/// The holder's non-zero balances of the tokens `kept` selects, sorted by token.
fn spot_balances(
    user: &Address,
    balances: Option<&Vec<(u32, u64)>>,
    kept: impl Fn(u32) -> bool,
) -> std::result::Result<Vec<(u32, i64)>, String> {
    let mut out = balances
        .map_or(&[][..], Vec::as_slice)
        .iter()
        .filter(|(token, _)| kept(*token))
        .map(|(token, t)| {
            i64::try_from(*t).map(|t| (*token, t)).map_err(|err| format!("{user}: spot balance {t} of token {token}: {err}"))
        })
        .collect::<std::result::Result<Vec<_>, String>>()?;
    out.sort_unstable_by_key(|(token, _)| *token);
    Ok(out)
}

/// A portfolio-margin holder's non-zero shares; a share in a token without a reserve contradicts the checkpoint.
fn lending_shares(
    user: &Address,
    wire: Vec<WireTokenShares>,
    reserves: &FxHashMap<u32, LendingReserve>,
) -> std::result::Result<Vec<Lending>, String> {
    let mut out = Vec::new();
    for (token, (borrow, supply)) in wire {
        if borrow.s == 0 && supply.s == 0 {
            continue;
        }
        if !reserves.contains_key(&token) {
            return Err(format!("{user}: borrow-lend shares in token {token}, which has no reserve"));
        }
        out.push(Lending { token, borrow_shares: borrow.s, supply_shares: supply.s });
    }
    out.sort_unstable_by_key(|lending| lending.token);
    Ok(out)
}

fn build_dex(
    meta: WireMeta,
    pxs2: &[Vec<WirePx>],
    user_states: WireUserStates,
) -> std::result::Result<(DexMeta, FxHashMap<Address, DexAccount>), String> {
    let name = dex_name(&meta.universe)?;
    if pxs2.len() != meta.universe.len() {
        return Err(format!("pxs2 has {} assets, universe {}", pxs2.len(), meta.universe.len()));
    }
    let assets = meta
        .universe
        .into_iter()
        .zip(pxs2)
        .map(|(asset, pxs)| {
            // AssetMeta::mark_px relies on szDecimals <= MAX_PERP_DECIMALS
            if asset.sz_decimals > MAX_PERP_DECIMALS {
                return Err(format!("{}: szDecimals {} above {MAX_PERP_DECIMALS}", asset.name, asset.sz_decimals));
            }
            // stored prices are integers in 10^-(6 - szDecimals)
            let (mark_raw, mark_time_ns) = match pxs.as_slice() {
                [] => (None, None),
                [_, mark, ..] if mark.p > 0 => {
                    let time_ns = mark.u.as_deref().map(node_time_ns).transpose().map_err(|err| format!("{}: mark {err}", asset.name))?;
                    (Some(mark.p.unsigned_abs()), time_ns)
                }
                [_, mark, ..] => return Err(format!("{}: stored mark {} is not positive", asset.name, mark.p)),
                [_] => return Err(format!("{}: pxs2 has one slot, the mark is slot 1", asset.name)),
            };
            let margin_mode = AssetMarginMode::of(&asset.margin_mode);
            let margin_table_id = asset.margin_table_id;
            Ok(AssetMeta {
                name: asset.name,
                size_decimals: asset.sz_decimals,
                mark_raw,
                mark_time_ns,
                margin_mode,
                margin_table_id,
            })
        })
        .collect::<std::result::Result<Vec<_>, String>>()?;
    let mut tables = FxHashMap::default();
    for (table_id, table) in meta.margin_table_id_to_margin_table {
        let tiers = table.margin_tiers.iter().map(|tier| (tier.lower_bound, tier.max_leverage, tier.maintenance_deduction));
        tables.insert(table_id, MarginTable::listed(tiers).map_err(|err| format!("margin table {table_id}: {err}"))?);
    }
    for asset in &assets {
        resolve_table(&mut tables, asset.margin_table_id).map_err(|err| format!("{}: {err}", asset.name))?;
    }
    let mut users = FxHashMap::default();
    for (user, WireDexUser { cash_micro, positions: wire_positions, isolated_settings }) in user_states.user_to_state.0 {
        let isolated_settings = isolated_settings
            .into_iter()
            .map(|asset_id| asset_index(asset_id, meta.pdi, assets.len()))
            .collect::<std::result::Result<Vec<_>, String>>()?;
        let mut positions = Vec::with_capacity(wire_positions.len());
        for (asset_id, wire) in wire_positions {
            let asset = asset_index(asset_id, meta.pdi, assets.len())?;
            let table_id = wire.table.unwrap_or(OMITTED_POSITION_TABLE);
            resolve_table(&mut tables, table_id).map_err(|err| format!("{user}: asset {asset_id}: {err}"))?;
            let margin = wire
                .l
                .and_then(|l| l.isolated)
                .map_or(PositionMargin::Cross, |isolated| PositionMargin::Isolated { cash_micro: isolated.u });
            positions.push(Position { asset, size_raw: wire.s, margin, table_id });
        }
        users.insert(user, DexAccount { dex: 0, cash_micro, positions, isolated_settings });
    }
    Ok((DexMeta { name, pdi: meta.pdi, collateral_token: meta.collateral_token, assets, tables }, users))
}

/// Makes `table_id` resolve: an id below `FIRST_LISTED_TABLE` is the implicit single tier, the others must be listed.
fn resolve_table(tables: &mut FxHashMap<u32, MarginTable>, table_id: u32) -> std::result::Result<(), String> {
    if let std::collections::hash_map::Entry::Vacant(entry) = tables.entry(table_id) {
        if table_id >= FIRST_LISTED_TABLE {
            return Err(format!("margin table {table_id} is not listed in the meta"));
        }
        entry.insert(MarginTable::single(table_id).map_err(|err| format!("margin table {table_id}: {err}"))?);
    }
    Ok(())
}

/// The coin prefix shared by every asset of a clearinghouse ("" for the main dex).
fn dex_name(universe: &[WireAsset]) -> std::result::Result<String, String> {
    let prefix = |asset: &WireAsset| asset.name.split_once(':').map_or("", |(prefix, _)| prefix).to_string();
    let first = universe.first().ok_or("empty universe")?;
    let name = prefix(first);
    universe
        .iter()
        .find(|asset| prefix(asset) != name)
        .map_or(Ok(name), |other| Err(format!("mixed coin prefixes: {} and {}", first.name, other.name)))
}

fn asset_index(asset_id: u32, pdi: u32, assets: usize) -> std::result::Result<usize, String> {
    asset_id
        .checked_sub(PDI_ASSET_STRIDE * pdi)
        .and_then(|index| usize::try_from(index).ok())
        .filter(|index| *index < assets)
        .ok_or_else(|| format!("asset id {asset_id} outside dex pdi {pdi} ({assets} assets)"))
}

#[cfg(test)]
pub(crate) mod tests {
    use serde_json::json;

    use super::*;
    use crate::accounts::marks::seed::tests::VALIDATOR;

    pub(crate) const HOLDER: &str = "0x00000000000000000000000000000000000000aa";
    const CASH_ONLY: &str = "0x00000000000000000000000000000000000000bb";
    pub(crate) const ISOLATED: &str = "0x00000000000000000000000000000000000000cc";
    /// Portfolio margin: borrows 3150 USDC (shares 3e11 at index 1.05), supplies 100 HYPE and holds 10 more in spot
    /// orders; cross PUMP 5,000,000 on main (cash -20000), xyz cash 1 and an isolated TSLA 1.0 (cash -300).
    pub(crate) const PORTFOLIO: &str = "0x00000000000000000000000000000000000000dd";

    /// Two USDC dexes in the measured layout: main (pdi 0) and xyz (pdi 1, asset ids 10000 + i), unknown keys mixed in.
    pub(crate) fn fixture() -> Vec<u8> {
        let checkpoint = json!({
            "exchange": {
                "hardfork": {"version": 108},
                "locus": {
                    "ctx": {"height": 1_189_230_000_u64, "time": "2026-10-19T06:37:23.556000667", "round": 7},
                    "cls": [
                        {
                            "meta": {
                                "universe": [
                                    {"szDecimals": 5, "name": "BTC", "marginTableId": 56, "marginMode": "normal"},
                                    {"szDecimals": 0, "name": "PUMP", "marginTableId": 52, "marginMode": "normal"}
                                ],
                                "pdi": 0,
                                "marginTableIdToMarginTable": [[52, {"description": "tiered 10x", "margin_tiers": [
                                    {"lower_bound": 0, "max_leverage": 10, "maintenance_deduction": 0},
                                    {"lower_bound": 20_000_000_000_000_i64, "max_leverage": 5, "maintenance_deduction": 1_000_000_000_000_i64}
                                ], "collateral_token": 0}], [56, {"description": "tiered 40x", "margin_tiers": [
                                    {"lower_bound": 0, "max_leverage": 40, "maintenance_deduction": 0}
                                ], "collateral_token": 0}]],
                                "collateralToken": 0,
                                "collateralTokenName": "USDC"
                            },
                            "oracle": native_oracle_fixture(),
                            "user_states": {
                                "user_to_state": [
                                    [HOLDER, {"u": -12_000_000_000_i64, "p": {"p": [
                                        [1, {"s": 1_000_000, "l": {"C": 10}, "M": 52, "f": {"a": 1}}],
                                        [0, {"s": 10_000, "l": {"C": 20}, "e": 8_000_000_000_i64}],
                                        [0, {"l": {"C": 3}, "M": 5}]
                                    ]}, "S": {"s": 1}}],
                                    [CASH_ONLY, {"u": 3_000_000}],
                                    [ISOLATED, {"p": {"p": [
                                        [1, {"s": -2_000_000, "l": {"I": {"l": 3, "u": 12_000_000_000_i64}}, "M": 52}],
                                        [0, {"l": {"I": {"l": 5, "u": 0}}, "M": 56}]
                                    ]}}],
                                    [PORTFOLIO, {"u": -20_000_000_000_i64, "p": {"p": [[1, {"s": 5_000_000, "l": {"C": 10}, "M": 52}]]}}]
                                ],
                                "users_with_positions": []
                            },
                            "total_net_deposit": 1
                        },
                        {
                            "meta": {
                                "universe": [{"szDecimals": 3, "name": "xyz:TSLA", "marginTableId": 10, "marginMode": "noCross"}],
                                "pdi": 1,
                                "marginTableIdToMarginTable": [],
                                "collateralToken": 0,
                                "collateralTokenName": "USDC"
                            },
                            "oracle": hip3_oracle_fixture(json!([[{"u": T22, "p": 357_970, "d": 1}, {"u": T22, "p": 358_320, "d": 1}, {"u": T22, "p": 357_970, "d": 1}]])),
                            "user_states": {"user_to_state": [
                                [HOLDER, {"u": 30_269_700, "p": {"p": [[10_000, {"l": {"I": {"l": 3, "u": 0}}, "M": 10}]]}}],
                                [PORTFOLIO, {"u": 1_000_000, "p": {"p": [[10_000, {"s": 1000, "l": {"I": {"u": -300_000_000_i64}}, "M": 10}]]}}]
                            ]}
                        },
                        {
                            "meta": {
                                "universe": [{"szDecimals": 2, "name": "flx:GOLD", "marginTableId": 10, "marginMode": "normal"}],
                                "pdi": 2,
                                "marginTableIdToMarginTable": [],
                                "collateralToken": 360,
                                "collateralTokenName": "USDH"
                            },
                            "oracle": hip3_oracle_fixture(json!([[{"u": T22, "p": 41_883_000, "d": 1}, {"u": T22, "p": 41_883_000, "d": 1}, {"p": 41_883_000, "d": 1}]])),
                            "user_states": {"user_to_state": [[HOLDER, {"u": -4_000_000_000_i64, "p": {"p": [[20_000, {"s": 100, "M": 10}]]}}]]}
                        }
                    ],
                    "scl": {"meta": {}, "user_states": [
                        [HOLDER, {"b": [[0, {"t": 20_000_000_000_i64}], [150, {"t": 5, "e": 2}], [360, {"t": 5_000_000_000_i64}]]}],
                        [CASH_ONLY, {"b": [[0, {"t": 1}]]}],
                        [PORTFOLIO, {"b": [[0, {}], [150, {"t": 1_000_000_000}], [299, {"t": 7, "e": 1}]]}]
                    ]},
                    "ust": [[HOLDER, {"a": "u", "C": 1, "s": "v"}], [CASH_ONLY, {"s": "v"}], [ISOLATED, {"P": 1}], [PORTFOLIO, {"a": "p"}]]
                },
                "perp_dexs": books_fixture(),
                "spot_books": []
            }
        });
        let mut checkpoint = checkpoint;
        checkpoint["exchange"]["locus"]["blp"] = lending_fixture();
        // token_infos is indexed by token id; USDC's 8 size decimals make its stored reserve price 1 mean 1.0
        let token_infos: Vec<serde_json::Value> = (0..=360)
            .map(|token| {
                let (name, size_decimals) = match token {
                    0 => ("USDC".to_string(), 8),
                    150 => ("HYPE".to_string(), 2),
                    360 => ("USDH".to_string(), 2),
                    other => (format!("T{other}"), 2),
                };
                json!({"spec": {"name": name, "szDecimals": size_decimals, "weiDecimals": 8}, "spots": []})
            })
            .collect();
        let spot_infos = json!([{"tokens": [1, 0], "deploy_time": "1970-01-01T00:00:00"}, {"tokens": [150, 0]}, {"tokens": [150, 360]}]);
        checkpoint["exchange"]["locus"]["scl"]["meta"] = json!({"token_infos": token_infos, "spot_infos": spot_infos});
        rmp_serde::to_vec(&checkpoint).unwrap()
    }

    const T22: &str = "2026-10-19T06:37:22";
    const T23: &str = "2026-10-19T06:37:23";

    /// The main dex's oracle in the 10-19/20 layout: BTC priced (one validator in each cache, plus a named HYPE input),
    /// PUMP a coin the node no longer prices (no EMAs, empty `Spot` and `ExternalPerp` caches, no median).
    fn native_oracle_fixture() -> serde_json::Value {
        let bucket = json!({"last_bucket": 597_463_947, "bucket_millis": 3000});
        let old_bucket = json!({"last_bucket": 593_580_423, "bucket_millis": 3000});
        let member = |value: u64| json!([[VALIDATOR, {"t": T22, "v": value}]]);
        let ema = |num: f64, denom: f64, decay: f64, samples: u64| {
            json!({"num": num, "denom": denom, "decay_duration": decay, "last_sample_time": T23, "n_samples": samples})
        };
        json!({
            "pxs2": [
                [{"u": T23, "p": 828_908, "d": 1}, {"u": T23, "p": 828_576, "d": 1}, {"u": T23, "p": 828_554, "d": 1}],
                [{"u": T23, "p": 4479, "d": 1}, {"u": T23, "p": 4477, "d": 1}, {"p": 4476, "d": 1}]
            ],
            "err_dur_guard": {"err_duration": 60.0},
            "ror": {"ktm": [
                [{"p": [0, "Spot"]}, {"v": member(828_720), "m": 828_720, "b": bucket}],
                [{"p": [0, "Mark"]}, {"v": member(828_590), "m": 828_590, "b": bucket}],
                [{"p": [0, "ExternalPerp"]}, {"v": member(828_554), "m": 828_554, "b": bucket}],
                [{"s": "usdtUsdcPx"}, {"v": member(998_886), "m": 998_886, "b": bucket}],
                [{"p": [1, "Spot"]}, {"v": [], "m": null, "b": old_bucket}],
                [{"p": [1, "Mark"]}, {"v": member(4477), "m": null, "b": old_bucket}],
                [{"p": [1, "ExternalPerp"]}, {"v": [], "m": null, "b": old_bucket}]
            ], "utl": [], "roc": {}},
            "msoe": {"e": [[0, ema(-0.04, 371.5, 120.0, 2_244_587)]], "g": 1},
            "lmpe": [[0, ema(7_696_308.0, 93.5, 30.0, 269_229)]],
            "psro": [],
            "pmep": []
        })
    }

    /// A HIP-3 dex's oracle: the same keys, its caches not read.
    fn hip3_oracle_fixture(pxs2: serde_json::Value) -> serde_json::Value {
        json!({"pxs2": pxs2, "ror": {"ktm": [[{"p": [0, "Mark"]}, {"v": [], "m": null, "b": {"last_bucket": 1, "bucket_millis": 1000}}]]},
               "msoe": {"e": []}, "lmpe": []})
    }

    /// `perp_dexs`: the main dex's books (BTC with two orders around a free slot; PUMP empty), then a HIP-3 dex whose
    /// books are skipped unread (a slot of no known kind).
    fn books_fixture() -> serde_json::Value {
        json!([
            {"books": [
                {"halfs": [{"levels": []}, {"levels": []}], "asset": 0, "last_trade_px": 828_600, "total_sz": 8, "bod": {"e": [
                    {"o": {"o": 7, "r": 3, "O": 3, "u": HOLDER, "t": "Gtc", "c": {"t": 1, "S": 3, "s": "B", "l": 828_500}}},
                    {"v": 2},
                    {"o": {"n": 1, "o": 9, "r": 5, "u": HOLDER, "t": null, "c": {"t": 2, "S": 6, "s": "A", "l": 828_700}}}
                ], "l": 2, "n": 3}},
                {"asset": 1, "last_trade_px": 4477, "bod": {"e": [], "l": 0, "n": 0}}
            ], "funding_tracker": {}, "twap_tracker": {}},
            {"books": [{"asset": 10_000, "last_trade_px": 358_300, "bod": {"e": [{"x": 1}]}}]}
        ])
    }

    /// The 10-19 reserve layout: `l` = [borrow side, supply side] with pool index `i` and per-account cap `u`.
    fn lending_fixture() -> serde_json::Value {
        json!({"u": [
            [HOLDER, {"t": [[0, [{}, {"b": 5, "s": 5}]]]}],
            [PORTFOLIO, {"o": {"n": 1}, "t": [
                [0, [{"b": 290_000_000_000_i64, "s": 300_000_000_000_i64}, {}]],
                [150, [{}, {"b": 10_000_000_000_i64, "s": 10_000_000_000_i64}]],
                [360, [{}, {}]]
            ]}]
        ], "r": {"u": true, "p": 10_000_000_000_000_i64, "t": [
            [0, {"T": "2026-10-19T06:37:23.5", "m": 1, "o": 1, "v": null, "l": [
                {"t": 1, "m": 1, "u": 5_000_000_000_000_000_i64, "i": 105_000_000_000_000_i64},
                {"t": 1, "m": 1, "u": 20_000_000_000_000_000_i64, "i": 101_000_000_000_000_i64}]}],
            [150, {"o": 80_000_000, "v": 65_000_000_000_000_i64, "l": [
                {"i": 104_000_000_000_000_i64},
                {"t": 1, "m": 1, "u": 100_000_000_000_000_i64, "i": 100_000_000_000_000_i64}]}],
            [268, {"o": 998_880, "v": null, "l": [
                {"u": 100_000_000_000_000_i64, "i": 102_000_000_000_000_i64},
                {"u": 500_000_000_000_000_i64, "i": 101_000_000_000_000_i64}]}],
            [360, {"o": 1_000_000, "v": null, "l": [
                {"u": 1, "i": 104_000_000_000_000_i64},
                {"u": 150_000_000_000_000_i64, "i": 100_000_000_000_000_i64}]}]
        ]}, "e": true})
    }

    pub(crate) fn user(text: &str) -> Address {
        Address::from_str(text).unwrap()
    }

    #[test]
    fn parses_the_measured_layout() {
        let parsed = parse_checkpoint_bytes(&fixture()).unwrap();
        assert_eq!(parsed.height, 1_189_230_000);
        assert_eq!(parsed.time_ms, 1_792_391_843_556);
        assert_eq!(parsed.dexes[0].name, "");
        assert_eq!(parsed.dexes[1].name, "xyz");
        // the mark is pxs2 slot 1, in 10^-(6 - szDecimals)
        assert_eq!(parsed.dexes[0].assets[0].mark_px(), Some(82_857.6));
        assert_eq!(parsed.dexes[0].assets[1].mark_px(), Some(0.004_477));
        assert_eq!(parsed.dexes[1].assets[0].mark_px(), Some(358.32));
    }

    #[test]
    fn keeps_holders_with_every_dex_state_and_drops_cash_only_users() {
        let parsed = parse_checkpoint_bytes(&fixture()).unwrap();
        assert!(!parsed.accounts.contains_key(&user(CASH_ONLY)));
        let holder = &parsed.accounts[&user(HOLDER)];
        assert_eq!(holder.mode, AccountMode::Unified);
        // only collateral tokens are kept (HYPE 150 is not one)
        assert_eq!(holder.spot, vec![(0, 20_000_000_000), (360, 5_000_000_000)]);
        assert_eq!(parsed.collateral[&360].name, "USDH");
        let cash: Vec<(usize, i64, usize)> =
            holder.dex_states.iter().map(|state| (state.dex, state.cash_micro, state.positions.len())).collect();
        // the zero-size leverage entry is dropped; the xyz state holds cash only and is kept
        assert_eq!(cash, vec![(0, -12_000_000_000, 2), (1, 30_269_700, 0), (2, -4_000_000_000, 1)]);
        assert_eq!(parsed.accounts[&user(ISOLATED)].mode, AccountMode::Default);
    }

    /// Research "新开仓位是全仓还是逐仓": a zero-size entry set isolated makes a new position there isolated; a cross one
    /// or none leaves it cross. The asset's `marginMode` decides first.
    #[test]
    fn zero_size_isolated_entries_and_asset_margin_modes_are_kept() {
        let parsed = parse_checkpoint_bytes(&fixture()).unwrap();
        let settings = |holder: &str, dex: usize| {
            let account = &parsed.accounts[&user(holder)];
            account.dex_states.iter().find(|state| state.dex == dex).unwrap().isolated_settings.clone()
        };
        assert_eq!(settings(ISOLATED, 0), [0], "BTC set isolated, no position");
        assert_eq!(settings(HOLDER, 0), Vec::<usize>::new(), "a cross entry is no isolated setting");
        assert_eq!(settings(HOLDER, 1), [0], "a cash-only state keeps its setting");
        assert_eq!(parsed.dexes[0].assets[0].margin_mode, AssetMarginMode::Normal);
        assert_eq!(parsed.dexes[1].assets[0].margin_mode, AssetMarginMode::IsolatedOnly);
        let pair = |size_decimals, quote_token| SpotPair { size_decimals, quote_token };
        assert_eq!(parsed.spot_pairs, [pair(2, 0), pair(2, 0), pair(2, 360)], "@0 T1/USDC, @1 HYPE/USDC, @2 HYPE/USDH");
    }

    #[test]
    fn an_unknown_asset_margin_mode_is_kept_as_read() {
        let mut checkpoint: serde_json::Value = rmp_serde::from_slice(&fixture()).unwrap();
        checkpoint["exchange"]["locus"]["cls"][0]["meta"]["universe"][1]["marginMode"] = json!("hedged");
        let parsed = parse_checkpoint_bytes(&rmp_serde::to_vec(&checkpoint).unwrap()).unwrap();
        assert_eq!(parsed.dexes[0].assets[1].margin_mode, AssetMarginMode::Unknown("hedged".to_string()));
        checkpoint["exchange"]["locus"]["cls"][0]["meta"]["universe"][1].as_object_mut().unwrap().remove("marginMode");
        assert!(parse_checkpoint_bytes(&rmp_serde::to_vec(&checkpoint).unwrap()).is_err(), "required");
    }

    #[test]
    fn an_omitted_position_table_is_the_single_tier_20x_table() {
        let parsed = parse_checkpoint_bytes(&fixture()).unwrap();
        let positions = &parsed.accounts[&user(HOLDER)].dex_states[0].positions;
        let btc = positions.iter().find(|position| position.asset == 0).unwrap();
        assert_eq!(btc.table_id, OMITTED_POSITION_TABLE);
        assert_eq!(parsed.dexes[0].table(btc.table_id).maintenance_micro(1_000_000_000), 25_000_000);
        let pump = positions.iter().find(|position| position.asset == 1).unwrap();
        assert_eq!(pump.table_id, 52);
    }

    #[test]
    fn isolated_cash_is_read_from_the_leverage_entry() {
        let parsed = parse_checkpoint_bytes(&fixture()).unwrap();
        let position = &parsed.accounts[&user(ISOLATED)].dex_states[0].positions[0];
        assert_eq!(position.margin, PositionMargin::Isolated { cash_micro: 12_000_000_000 });
        assert_eq!(position.size_raw, -2_000_000);
    }

    /// Every asset's table resolves: on 1190010000 all 43 assets with a table from 50 up had it listed.
    #[test]
    fn an_assets_current_table_resolves_or_fails_the_parse() {
        let parsed = parse_checkpoint_bytes(&fixture()).unwrap();
        let tsla = &parsed.dexes[1].assets[0];
        assert_eq!(tsla.margin_table_id, 10);
        assert_eq!(parsed.dexes[1].table(10).maintenance_micro(1_000_000_000), 50_000_000, "implicit single tier, 10x");
        let mut checkpoint: serde_json::Value = rmp_serde::from_slice(&fixture()).unwrap();
        checkpoint["exchange"]["locus"]["cls"][0]["meta"]["universe"][0]["marginTableId"] = json!(57);
        let error = parse_checkpoint_bytes(&rmp_serde::to_vec(&checkpoint).unwrap()).unwrap_err().to_string();
        assert!(error.contains("BTC: margin table 57 is not listed"), "{error}");
    }

    #[test]
    fn an_unlisted_table_id_from_50_up_fails_the_parse() {
        let mut checkpoint: serde_json::Value = rmp_serde::from_slice(&fixture()).unwrap();
        checkpoint["exchange"]["locus"]["cls"][0]["user_states"]["user_to_state"][0][1]["p"]["p"][0][1]["M"] = json!(77);
        let error = parse_checkpoint_bytes(&rmp_serde::to_vec(&checkpoint).unwrap()).unwrap_err().to_string();
        assert!(error.contains("margin table 77 is not listed"), "{error}");
    }

    /// Values the solver must never see: a zero max leverage (implicit or listed table) or a non-positive mark.
    #[test]
    fn invalid_leverage_or_mark_fails_the_parse() {
        let base: serde_json::Value = rmp_serde::from_slice(&fixture()).unwrap();
        let position = "/exchange/locus/cls/0/user_states/user_to_state/0/1/p/p/0/1/M";
        let cases: [(&str, serde_json::Value, &str); 3] = [
            (position, json!(0), "max leverage 0"),
            ("/exchange/locus/cls/0/meta/marginTableIdToMarginTable/0/1/margin_tiers/1/max_leverage", json!(0), "max leverage 0"),
            ("/exchange/locus/cls/0/oracle/pxs2/1/1/p", json!(-4477), "not positive"),
        ];
        for (pointer, value, expected) in cases {
            let mut checkpoint = base.clone();
            *checkpoint.pointer_mut(pointer).unwrap() = value;
            let error = parse_checkpoint_bytes(&rmp_serde::to_vec(&checkpoint).unwrap()).unwrap_err().to_string();
            assert!(error.contains(expected), "{pointer}: {error}");
        }
    }

    #[test]
    fn reserves_are_read_in_whole_tokens_and_usdc() {
        let parsed = parse_checkpoint_bytes(&fixture()).unwrap();
        let usdc = &parsed.reserves[&0];
        assert_eq!((usdc.oracle_px, usdc.ltv, usdc.borrow_cap, usdc.supply_cap), (1.0, 0.0, Some(5e7), Some(2e8)));
        assert_eq!((usdc.borrow_index, usdc.supply_index, usdc.wei_scale), (1.05, 1.01, 1e8));
        let hype = &parsed.reserves[&150];
        assert_eq!((hype.oracle_px, hype.ltv, hype.borrow_cap, hype.supply_cap), (80.0, 0.65, None, Some(1e6)));
        assert_eq!(parsed.reserves[&268].oracle_px, 0.998_88);
        assert_eq!(parsed.reserves[&360].borrow_cap, Some(1e-8));
    }

    #[test]
    fn a_portfolio_margin_account_keeps_its_reserve_balances_and_lending_shares() {
        let parsed = parse_checkpoint_bytes(&fixture()).unwrap();
        let account = &parsed.accounts[&user(PORTFOLIO)];
        assert_eq!(account.mode, AccountMode::PortfolioMargin);
        // HYPE is a reserve token, kept; token 299 is neither a reserve nor a collateral token
        assert_eq!(account.spot, vec![(150, 1_000_000_000)]);
        assert_eq!(
            account.lending,
            vec![
                Lending { token: 0, borrow_shares: 300_000_000_000, supply_shares: 0 },
                Lending { token: 150, borrow_shares: 0, supply_shares: 10_000_000_000 },
            ]
        );
        // a unified account's shares are not kept
        assert_eq!(parsed.accounts[&user(HOLDER)].lending, Vec::new());
    }

    #[test]
    fn shares_in_a_token_without_a_reserve_fail_the_parse() {
        let mut checkpoint: serde_json::Value = rmp_serde::from_slice(&fixture()).unwrap();
        let shares = checkpoint.pointer_mut("/exchange/locus/blp/u/1/1/t").unwrap().as_array_mut().unwrap();
        shares.push(json!([197, [{"s": 1}, {}]]));
        let error = parse_checkpoint_bytes(&rmp_serde::to_vec(&checkpoint).unwrap()).unwrap_err().to_string();
        assert!(error.contains("token 197, which has no reserve"), "{error}");
    }

    #[test]
    fn a_missing_borrow_lend_section_fails_the_parse() {
        let mut checkpoint: serde_json::Value = rmp_serde::from_slice(&fixture()).unwrap();
        checkpoint["exchange"]["locus"]["blp"].as_object_mut().unwrap().remove("r");
        assert!(parse_checkpoint_bytes(&rmp_serde::to_vec(&checkpoint).unwrap()).is_err());
        checkpoint["exchange"]["locus"].as_object_mut().unwrap().remove("blp");
        assert!(parse_checkpoint_bytes(&rmp_serde::to_vec(&checkpoint).unwrap()).is_err());
    }

    #[test]
    fn a_missing_required_field_fails_the_parse() {
        let mut checkpoint: serde_json::Value = rmp_serde::from_slice(&fixture()).unwrap();
        checkpoint["exchange"]["locus"]["cls"][0]["meta"].as_object_mut().unwrap().remove("collateralToken");
        assert!(parse_checkpoint_bytes(&rmp_serde::to_vec(&checkpoint).unwrap()).is_err());
    }
}
