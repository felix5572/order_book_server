//! Account state of one node checkpoint (`periodic_abci_states/<day>/<height>.rmp`: `MessagePack`, string keys).
//!
//! Only the fields the liquidation computation needs are read; every other key is skipped. The meanings below were
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

use super::margin::MarginTable;
use crate::prelude::Result;

/// A position omits `M` while it still uses the single-tier 20x table: on 2026-10-19 every such position the node was
/// asked about answered `maxLeverage` 20 (488 main-dex positions over 34 coins; 16 coins sampled). Measured snapshot
/// format, not an official guarantee. A present `M` always wins; the asset's current table is never a fallback.
pub(crate) const OMITTED_POSITION_TABLE: u32 = 20;
/// Table ids below this are an implicit single tier whose max leverage is the id.
const FIRST_LISTED_TABLE: u32 = 50;
/// Asset ids inside a clearinghouse's user states are `PDI_ASSET_STRIDE * pdi + index`.
const PDI_ASSET_STRIDE: u32 = 10_000;
/// Perp prices carry `MAX_PERP_DECIMALS - szDecimals` decimals, so `szDecimals` cannot exceed it.
pub(crate) const MAX_PERP_DECIMALS: u32 = 6;

// ─── domain types ───────────────────────────────────────────────────────────

#[derive(Debug)]
pub(crate) struct CheckpointAccounts {
    pub(crate) height: u64,
    pub(crate) time_ms: u64,
    pub(crate) dexes: Vec<DexMeta>,
    /// Spot token of every dex's collateral (each dex uses exactly one: `meta.collateralToken`).
    pub(crate) collateral: FxHashMap<u32, CollateralToken>,
    /// Every account holding at least one non-zero position on some dex, with all its dex states (cash-only ones
    /// included: a unified account's pool counts the cash of every same-collateral dex).
    pub(crate) accounts: FxHashMap<Address, Account>,
}

#[derive(Debug)]
pub(crate) struct DexMeta {
    /// "" for the main dex, else the coin prefix of a HIP-3 dex ("xyz").
    pub(crate) name: String,
    pub(crate) collateral_token: u32,
    pub(crate) assets: Vec<AssetMeta>,
    tables: FxHashMap<u32, MarginTable>,
}

impl DexMeta {
    /// Every table a parsed position refers to resolves here (checked at parse time).
    pub(crate) fn table(&self, table_id: u32) -> &MarginTable {
        &self.tables[&table_id]
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CollateralToken {
    pub(crate) name: String,
    /// 10^weiDecimals: spot balances `t` of this token divided by this are whole tokens (`scl.meta.token_infos[token]
    /// .spec.weiDecimals`, index = token id; USDC, USDE, USDT0, USDH are all 8 and match the node's spotMeta).
    pub(crate) spot_scale: f64,
}

#[derive(Debug)]
pub(crate) struct AssetMeta {
    pub(crate) name: String,
    /// `szDecimals`: raw sizes are integers in 10^-size_decimals base units (at most `MAX_PERP_DECIMALS`).
    pub(crate) size_decimals: u32,
    /// `oracle.pxs2[asset][1]`: the node's applied mark at the checkpoint (slots are [oracle, mark, external perp];
    /// slot 1 matched the node mark for 10/10 main and 306/306 HIP-3 coins). `None` when the asset stores no price.
    pub(crate) mark_px: Option<f64>,
}

impl AssetMeta {
    /// Signed size in base units.
    pub(crate) fn size(&self, size_raw: i64) -> f64 {
        size_raw as f64 / 10f64.powi(self.size_decimals.cast_signed())
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
    /// Non-zero spot balances of collateral tokens: (token, raw `t` in 10^-weiDecimals).
    pub(crate) spot: Vec<(u32, i64)>,
    pub(crate) dex_states: Vec<DexAccount>,
}

#[derive(Debug)]
pub(crate) struct DexAccount {
    pub(crate) dex: usize,
    /// Cross cash `u`, micro units of the dex's collateral token.
    pub(crate) cash_micro: i64,
    pub(crate) positions: Vec<Position>,
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

#[derive(Deserialize)]
struct WireExchange {
    locus: ParsedLocus,
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
struct WireOracle {
    pxs2: Vec<Vec<WirePx>>,
}

#[derive(Deserialize)]
struct WirePx {
    p: i64,
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

/// One dex's `user_to_state`, kept compact: users with non-zero cash or at least one non-zero position.
struct DexUsers(FxHashMap<Address, (i64, Vec<(u32, WirePosition)>)>);

/// `exchange.locus`, read in its on-disk order (ctx, cls, scl, ..., ust): once `cls` is read, spot balances keep only
/// the dexes' collateral tokens and modes only the position holders. Another order fails the parse.
struct ParsedLocus {
    ctx: WireCtx,
    cls: Vec<WireClearinghouse>,
    spot: SpotState,
    modes: FxHashMap<Address, Option<String>>,
}

struct SpotState {
    collateral: FxHashMap<u32, CollateralToken>,
    /// Non-zero collateral-token balances per user (raw, unsigned on the wire).
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
            let positions: Vec<(u32, WirePosition)> =
                state.p.map(|p| p.p.into_iter().filter(|(_, position)| position.s != 0).collect()).unwrap_or_default();
            (state.u != 0 || !positions.is_empty()).then_some((state.u, positions))
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
        let (mut ctx, mut cls, mut spot, mut modes) = (None, None, None, None);
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "ctx" => ctx = Some(map.next_value()?),
                "cls" => cls = Some(map.next_value::<Vec<WireClearinghouse>>()?),
                "scl" => {
                    let dexes = cls.as_deref().ok_or_else(|| A::Error::custom("locus.scl precedes locus.cls"))?;
                    let collateral: FxHashSet<u32> = dexes.iter().map(|dex| dex.meta.collateral_token).collect();
                    spot = Some(map.next_value_seed(SpotSeed { collateral: &collateral })?);
                }
                "ust" => {
                    let dexes = cls.as_deref().ok_or_else(|| A::Error::custom("locus.ust precedes locus.cls"))?;
                    let holders = holders(dexes);
                    let keep = |user: &Address, mode: WireUserMode| holders.contains(user).then_some(mode.a);
                    modes = Some(map.next_value_seed(Pairs::new(keep))?);
                }
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
        })
    }
}

/// Users with a non-zero position on some dex.
fn holders(dexes: &[WireClearinghouse]) -> FxHashSet<Address> {
    dexes
        .iter()
        .flat_map(|dex| dex.user_states.user_to_state.0.iter())
        .filter(|(_, (_, positions))| !positions.is_empty())
        .map(|(user, _)| *user)
        .collect()
}

/// `scl`: the collateral tokens' specs from `meta.token_infos` and their non-zero balances from `user_states`.
struct SpotSeed<'a> {
    collateral: &'a FxHashSet<u32>,
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
        let (mut collateral, mut balances) = (None, None);
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "meta" => {
                    let meta: WireSpotMeta = map.next_value()?;
                    let mut tokens = FxHashMap::default();
                    for token in self.collateral {
                        let info = usize::try_from(*token).ok().and_then(|index| meta.token_infos.get(index)).ok_or_else(|| {
                            A::Error::custom(format!("collateral token {token} missing from scl.meta.token_infos"))
                        })?;
                        let decimals = i32::try_from(info.spec.wei_decimals)
                            .map_err(|err| A::Error::custom(format!("token {token} weiDecimals: {err}")))?;
                        tokens.insert(*token, CollateralToken { name: info.spec.name.clone(), spot_scale: 10f64.powi(decimals) });
                    }
                    collateral = Some(tokens);
                }
                "user_states" => {
                    let keep = |_: &Address, user: WireSpotUser| {
                        let kept: Vec<(u32, u64)> = user
                            .b
                            .into_iter()
                            .filter(|(token, balance)| balance.t != 0 && self.collateral.contains(token))
                            .map(|(token, balance)| (token, balance.t))
                            .collect();
                        (!kept.is_empty()).then_some(kept)
                    };
                    balances = Some(map.next_value_seed(Pairs::new(keep))?);
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(SpotState {
            collateral: collateral.ok_or_else(|| A::Error::missing_field("scl.meta"))?,
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
    build(wire.exchange.locus).map_err(|err| format!("checkpoint {}: {err}", path.display()).into())
}

/// Parse an in-memory checkpoint (tests).
#[cfg(test)]
pub(crate) fn parse_checkpoint_bytes(bytes: &[u8]) -> Result<CheckpointAccounts> {
    let wire: WireCheckpoint = rmp_serde::from_slice(bytes).map_err(|err| format!("decode checkpoint: {err}"))?;
    build(wire.exchange.locus).map_err(Into::into)
}

fn build(locus: ParsedLocus) -> std::result::Result<CheckpointAccounts, String> {
    let time = NaiveDateTime::parse_from_str(&locus.ctx.time, "%Y-%m-%dT%H:%M:%S%.f")
        .map_err(|err| format!("ctx.time {:?}: {err}", locus.ctx.time))?;
    let time_ms = u64::try_from(time.and_utc().timestamp_millis()).map_err(|err| format!("ctx.time before 1970: {err}"))?;
    let mut dexes = Vec::with_capacity(locus.cls.len());
    let mut dex_users = Vec::with_capacity(locus.cls.len());
    for clearinghouse in locus.cls {
        let pdi = clearinghouse.meta.pdi;
        let (meta, users) = build_dex(clearinghouse).map_err(|err| format!("dex pdi {pdi}: {err}"))?;
        dexes.push(meta);
        dex_users.push(users);
    }
    let holders: Vec<Address> = {
        let mut holders: Vec<Address> = dex_users
            .iter()
            .flat_map(|users| users.iter().filter(|(_, state)| !state.positions.is_empty()).map(|(user, _)| *user))
            .collect();
        holders.sort_unstable();
        holders.dedup();
        holders
    };
    let mut accounts = FxHashMap::default();
    for user in holders {
        let dex_states = dex_users
            .iter_mut()
            .enumerate()
            .filter_map(|(dex, users)| users.remove(&user).map(|state| DexAccount { dex, ..state }))
            .collect();
        let mode = match locus.modes.get(&user) {
            None => AccountMode::Unknown("<no ust entry>".to_string()),
            Some(None) => AccountMode::Default,
            Some(Some(code)) => match code.as_str() {
                "i" => AccountMode::Disabled,
                "u" => AccountMode::Unified,
                "p" => AccountMode::PortfolioMargin,
                other => AccountMode::Unknown(other.to_string()),
            },
        };
        let spot = locus
            .spot
            .balances
            .get(&user)
            .map_or(&[][..], Vec::as_slice)
            .iter()
            .map(|(token, t)| {
                i64::try_from(*t).map(|t| (*token, t)).map_err(|err| format!("{user}: spot balance {t} of token {token}: {err}"))
            })
            .collect::<std::result::Result<Vec<_>, String>>()?;
        accounts.insert(user, Account { mode, spot, dex_states });
    }
    Ok(CheckpointAccounts { height: locus.ctx.height, time_ms, dexes, collateral: locus.spot.collateral, accounts })
}

fn build_dex(clearinghouse: WireClearinghouse) -> std::result::Result<(DexMeta, FxHashMap<Address, DexAccount>), String> {
    let WireClearinghouse { meta, oracle, user_states } = clearinghouse;
    let name = dex_name(&meta.universe)?;
    if oracle.pxs2.len() != meta.universe.len() {
        return Err(format!("pxs2 has {} assets, universe {}", oracle.pxs2.len(), meta.universe.len()));
    }
    let assets = meta
        .universe
        .into_iter()
        .zip(oracle.pxs2)
        .map(|(asset, pxs)| {
            let price_decimals = MAX_PERP_DECIMALS
                .checked_sub(asset.sz_decimals)
                .ok_or_else(|| format!("{}: szDecimals {} above {MAX_PERP_DECIMALS}", asset.name, asset.sz_decimals))?;
            // stored prices are integers in 10^-(6 - szDecimals)
            let mark_px = match pxs.as_slice() {
                [] => None,
                [_, mark, ..] if mark.p > 0 => Some(mark.p as f64 / 10f64.powi(price_decimals.cast_signed())),
                [_, mark, ..] => return Err(format!("{}: stored mark {} is not positive", asset.name, mark.p)),
                [_] => return Err(format!("{}: pxs2 has one slot, the mark is slot 1", asset.name)),
            };
            Ok(AssetMeta { name: asset.name, size_decimals: asset.sz_decimals, mark_px })
        })
        .collect::<std::result::Result<Vec<_>, String>>()?;
    let mut tables = FxHashMap::default();
    for (table_id, table) in meta.margin_table_id_to_margin_table {
        let tiers = table.margin_tiers.iter().map(|tier| (tier.lower_bound, tier.max_leverage, tier.maintenance_deduction));
        tables.insert(table_id, MarginTable::listed(tiers).map_err(|err| format!("margin table {table_id}: {err}"))?);
    }
    let mut users = FxHashMap::default();
    for (user, (cash_micro, wire_positions)) in user_states.user_to_state.0 {
        let mut positions = Vec::with_capacity(wire_positions.len());
        for (asset_id, wire) in wire_positions {
            let asset = asset_index(asset_id, meta.pdi, assets.len())?;
            let table_id = wire.table.unwrap_or(OMITTED_POSITION_TABLE);
            if let std::collections::hash_map::Entry::Vacant(entry) = tables.entry(table_id) {
                if table_id >= FIRST_LISTED_TABLE {
                    return Err(format!("{user}: asset {asset_id} uses margin table {table_id}, not listed in the meta"));
                }
                entry.insert(
                    MarginTable::single(table_id)
                        .map_err(|err| format!("{user}: asset {asset_id} margin table {table_id}: {err}"))?,
                );
            }
            let margin = wire
                .l
                .and_then(|l| l.isolated)
                .map_or(PositionMargin::Cross, |isolated| PositionMargin::Isolated { cash_micro: isolated.u });
            positions.push(Position { asset, size_raw: wire.s, margin, table_id });
        }
        users.insert(user, DexAccount { dex: 0, cash_micro, positions });
    }
    Ok((DexMeta { name, collateral_token: meta.collateral_token, assets, tables }, users))
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

    const HOLDER: &str = "0x00000000000000000000000000000000000000aa";
    const CASH_ONLY: &str = "0x00000000000000000000000000000000000000bb";
    const ISOLATED: &str = "0x00000000000000000000000000000000000000cc";

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
                                ], "collateral_token": 0}]],
                                "collateralToken": 0,
                                "collateralTokenName": "USDC"
                            },
                            "oracle": {"pxs2": [
                                [{"u": "t", "p": 828_908, "d": 1}, {"u": "t", "p": 828_576, "d": 1}, {"u": "t", "p": 828_554, "d": 1}],
                                [{"u": "t", "p": 4479, "d": 1}, {"u": "t", "p": 4477, "d": 1}, {"u": "t", "p": 4476, "d": 1}]
                            ], "ror": {}},
                            "user_states": {
                                "user_to_state": [
                                    [HOLDER, {"u": -12_000_000_000_i64, "p": {"p": [
                                        [1, {"s": 1_000_000, "l": {"C": 10}, "M": 52, "f": {"a": 1}}],
                                        [0, {"s": 10_000, "l": {"C": 20}, "e": 8_000_000_000_i64}],
                                        [0, {"l": {"C": 3}, "M": 5}]
                                    ]}, "S": {"s": 1}}],
                                    [CASH_ONLY, {"u": 3_000_000}],
                                    [ISOLATED, {"p": {"p": [[1, {"s": -2_000_000, "l": {"I": {"l": 3, "u": 12_000_000_000_i64}}, "M": 52}]]}}]
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
                            "oracle": {"pxs2": [[{"u": "t", "p": 357_970, "d": 1}, {"u": "t", "p": 358_320, "d": 1}, {"u": "t", "p": 357_970, "d": 1}]]},
                            "user_states": {"user_to_state": [[HOLDER, {"u": 30_269_700}]]}
                        },
                        {
                            "meta": {
                                "universe": [{"szDecimals": 2, "name": "flx:GOLD", "marginTableId": 10, "marginMode": "normal"}],
                                "pdi": 2,
                                "marginTableIdToMarginTable": [],
                                "collateralToken": 360,
                                "collateralTokenName": "USDH"
                            },
                            "oracle": {"pxs2": [[{"u": "t", "p": 41_883_000, "d": 1}, {"u": "t", "p": 41_883_000, "d": 1}, {"u": "t", "p": 41_883_000, "d": 1}]]},
                            "user_states": {"user_to_state": [[HOLDER, {"u": -4_000_000_000_i64, "p": {"p": [[20_000, {"s": 100, "M": 10}]]}}]]}
                        }
                    ],
                    "scl": {"meta": {}, "user_states": [
                        [HOLDER, {"b": [[0, {"t": 20_000_000_000_i64}], [150, {"t": 5, "e": 2}], [360, {"t": 5_000_000_000_i64}]]}],
                        [CASH_ONLY, {"b": [[0, {"t": 1}]]}]
                    ]},
                    "ust": [[HOLDER, {"a": "u", "C": 1, "s": "v"}], [CASH_ONLY, {"s": "v"}], [ISOLATED, {"P": 1}]],
                    "blp": {"e": true}
                }
            }
        });
        let mut checkpoint = checkpoint;
        // token_infos is indexed by token id; only the collateral tokens' specs are read
        let token_infos: Vec<serde_json::Value> = (0..=360)
            .map(|token| {
                let name = match token {
                    0 => "USDC".to_string(),
                    360 => "USDH".to_string(),
                    other => format!("T{other}"),
                };
                json!({"spec": {"name": name, "szDecimals": 2, "weiDecimals": 8}, "spots": []})
            })
            .collect();
        checkpoint["exchange"]["locus"]["scl"]["meta"] = json!({"token_infos": token_infos, "spot_infos": []});
        rmp_serde::to_vec(&checkpoint).unwrap()
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
        assert_eq!(parsed.dexes[0].assets[0].mark_px, Some(82_857.6));
        assert_eq!(parsed.dexes[0].assets[1].mark_px, Some(0.004_477));
        assert_eq!(parsed.dexes[1].assets[0].mark_px, Some(358.32));
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

    #[test]
    fn an_omitted_position_table_is_the_single_tier_20x_table() {
        let parsed = parse_checkpoint_bytes(&fixture()).unwrap();
        let positions = &parsed.accounts[&user(HOLDER)].dex_states[0].positions;
        let btc = positions.iter().find(|position| position.asset == 0).unwrap();
        assert_eq!(btc.table_id, OMITTED_POSITION_TABLE);
        assert_eq!(parsed.dexes[0].table(btc.table_id).maintenance_usd(1000.0), 25.0);
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

    #[test]
    fn an_unlisted_table_id_from_50_up_fails_the_parse() {
        let mut checkpoint: serde_json::Value = rmp_serde::from_slice(&fixture()).unwrap();
        checkpoint["exchange"]["locus"]["cls"][0]["user_states"]["user_to_state"][0][1]["p"]["p"][0][1]["M"] = json!(77);
        let error = parse_checkpoint_bytes(&rmp_serde::to_vec(&checkpoint).unwrap()).unwrap_err().to_string();
        assert!(error.contains("margin table 77"), "{error}");
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
    fn a_missing_required_field_fails_the_parse() {
        let mut checkpoint: serde_json::Value = rmp_serde::from_slice(&fixture()).unwrap();
        checkpoint["exchange"]["locus"]["cls"][0]["meta"].as_object_mut().unwrap().remove("collateralToken");
        assert!(parse_checkpoint_bytes(&rmp_serde::to_vec(&checkpoint).unwrap()).is_err());
    }
}
