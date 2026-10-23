//! Cash replay between anchors (design 000433 r2 3.1 / 3.3, r3 revision 3): the margin state of every account that
//! held a position at the anchor, moved block by block by its events, and checked against the next anchor.
//!
//! Only USDC pools replay: a default / disabled account's cross cash per USDC dex, a unified account's USDC pool (spot
//! USDC plus the cross cash of every USDC dex), and isolated cash per position. Each rule below is measured (research
//! `hl_account_state_verify_20261019` sections 5, "全仓与逐仓之间的现金划分" and "逐仓成交的划转"); an event no measured
//! rule covers stops the account's replay with a reason, and the next anchor resumes it. Amounts are 10^-8 USDC
//! (`usdc.rs`). Isolated opens/increases/flips fund the larger of fill-notional/leverage and mark-notional/leverage
//! plus immediate execution loss (see `isolated`). Execution mark bounds come from the node mark owner; if the bounds
//! yield different transfers, or no mark is available, the old fill-notional estimate remains explicitly tagged on
//! both isolated and cross rows until the next anchor. Only a split difference with exact pool totals is counted
//! `estimated`; a pool-total difference is still a mismatch (review 000466 r1).

mod isolated;
mod cross_capacity;
pub(crate) mod dust;

use std::{collections::BTreeMap, sync::Arc};

use alloy::primitives::Address;
use rustc_hash::{FxHashMap, FxHashSet};
use serde_json::json;

use super::{
    actions::BlockAction,
    liquidation::{HeldAtMark, LiquidationPx, solve, solve_held},
    margin::MarginTable,
    marks::fill_prices::FillMarkRange,
    checkpoint::{
        Account, AccountMode, AssetMarginMode, CheckpointAccounts, MAX_PERP_DECIMALS, MAX_SPOT_DECIMALS,
        PositionMargin, SpotPair, base_size, spot_pair_of_coin,
    },
    misc::{Balance, LedgerDelta, USDC_NAME},
    portfolio::USDC,
    size::{format_size_raw, parse_size_raw},
    usdc::{WEI_DECIMALS, WEI_PER_MICRO, micro_to_wei, notional_wei, usdc_wei},
};
use crate::{order_book::Side, types::Fill};

/// The direction of an ADL fill; the other side of the same trade carries the same `backstop` method.
const ADL_DIR: &str = "Auto-Deleveraging";
const BACKSTOP_METHOD: &str = "backstop";
/// Mismatches kept as evidence per anchor.
const MAX_MISMATCH_SAMPLES: usize = 50;

/// A perp of the anchor's universe.
#[derive(Debug, Clone)]
struct PerpMarket {
    dex: usize,
    asset: usize,
    size_decimals: u32,
    margin_mode: AssetMarginMode,
    /// A position opened without a setting takes min(first-tier max leverage, 20). Client 6911b792's constructor
    /// 0x44ce291..0x44ce2b6 caps only this default; a stored or successful explicit setting takes precedence.
    /// JPY H1192705810 / checkpoints1192700000..1192710000 confirm 20 on a 50x table (see the golden).
    default_leverage: u32,
}

/// Where an event's money goes, resolved against the anchor's universe.
#[derive(Debug, Clone, Default)]
pub(crate) struct Markets {
    perps: FxHashMap<String, PerpMarket>,
    /// The perp of an action asset id.
    action_assets: FxHashMap<u32, String>,
    /// Index projection of the same universe for checkpoint holdings; avoids scanning all markets per position.
    asset_names: Vec<Vec<String>>,
    /// Dex index by name ("" the main dex).
    dexes: FxHashMap<String, usize>,
    /// Whether each dex settles in USDC.
    usdc_dexes: Vec<bool>,
    /// Each dex's margin tables by id.
    tables: Vec<FxHashMap<u32, MarginTable>>,
    spot_pairs: Vec<SpotPair>,
}

impl Markets {
    fn of(checkpoint: &CheckpointAccounts) -> Result<Self, String> {
        let mut markets = Self { spot_pairs: checkpoint.spot_pairs.clone(), ..Self::default() };
        for (dex_index, dex) in checkpoint.dexes.iter().enumerate() {
            markets.dexes.insert(dex.name.clone(), dex_index);
            markets.asset_names.push(dex.assets.iter().map(|asset| asset.name.clone()).collect());
            markets.usdc_dexes.push(checkpoint.collateral[&dex.collateral_token].name == USDC_NAME);
            markets.tables.push(dex.tables().clone());
            for (asset_index, asset) in dex.assets.iter().enumerate() {
                let action_id = dex.action_asset(asset_index)
                    .ok_or_else(|| format!("dex {:?} asset {asset_index} has no action id", dex.name))?;
                markets.action_assets.insert(action_id, asset.name.clone());
                let table = dex.tables().get(&asset.margin_table_id)
                    .ok_or_else(|| format!("{} without its margin table {}", asset.name, asset.margin_table_id))?;
                let market = PerpMarket {
                    dex: dex_index,
                    asset: asset_index,
                    size_decimals: asset.size_decimals,
                    margin_mode: asset.margin_mode.clone(),
                    default_leverage: table.first_max_leverage().min(20),
                };
                markets.perps.insert(asset.name.clone(), market);
            }
        }
        Ok(markets)
    }
}

/// An account's event of one block, as the replay and the change marks read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AccountEvent {
    Fill(FillEvent),
    /// A fill whose USDC fee did not read: a fills data loss up to its block.
    UnreadableFill(String),
    Funding { coin: String, amount_wei: i64 },
    /// Derived cash-only balancing of one full dex batch, never another wire funding payment.
    FundingBalance { coin: String, amount_wei: i64 },
    /// The batch's balancing recipient cannot be established; all its potential recipients are withdrawn.
    FundingBalanceUnknown,
    Ledger(LedgerDelta),
    Action(BlockAction),
}

/// What a fill moves: `−side × px × sz − fee` into the pool of `coin` (the notional is computed against the market,
/// whose decimals the strings carry).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FillEvent {
    /// Retained wire identity for execution marks rebuilt after checkpoint loading.
    tid: u64,
    coin: String,
    side: Side,
    px: String,
    sz: String,
    /// The position before the fill (`startPosition`), signed.
    start_position: String,
    /// `fee` when `feeToken` is USDC, else 0 (it includes builder and deployer fees).
    usdc_fee_wei: i64,
    /// A backstop liquidation other than ADL.
    backstop: bool,
    /// System conversion of sub-lot spot balances; it does not obey ordinary order quantity precision.
    spot_dust_conversion: bool,
    dust_share: Option<dust::DustShare>,
    /// Execution-time evidence, retained with the event across checkpoint replay; never a later HTTP price.
    mark: Option<FillMarkRange>,
    collateral_marks: Option<Arc<FxHashMap<String, FillMarkRange>>>,
}

impl FillEvent {
    #[cfg(test)]
    pub(crate) fn of(fill: &Fill) -> Result<Self, String> { Self::at_mark(fill, None) }

    pub(crate) fn at_mark(fill: &Fill, mark: Option<FillMarkRange>) -> Result<Self, String> {
        let usdc_fee_wei = if fill.fee_token == USDC_NAME {
            usdc_wei(&fill.fee).map_err(|err| format!("{} tid {} fee: {err}", fill.coin, fill.tid))?
        } else {
            0
        };
        let backstop = fill.liquidation.as_ref().is_some_and(|liquidation| liquidation.method == BACKSTOP_METHOD)
            && fill.dir != ADL_DIR;
        Ok(Self {
            tid: fill.tid,
            coin: fill.coin.clone(),
            side: fill.side,
            px: fill.px.clone(),
            sz: fill.sz.clone(),
            start_position: fill.start_position.clone(),
            usdc_fee_wei,
            backstop,
            spot_dust_conversion: fill.dir == dust::DUST_DIR,
            dust_share: None,
            collateral_marks: None,
            mark,
        })
    }

    pub(crate) fn attach_dust(&mut self, share: Option<dust::DustShare>) { self.dust_share = share; }

    /// Replace only this block/trade's projection using a completed trusted seed replay, never a current mark.
    pub(crate) fn restore_mark(&mut self, block: u64, prices: &super::marks::fill_prices::FillMarkPrices) -> bool {
        let mark = prices.get(block, &self.coin, self.tid);
        let collateral = prices.collateral(block);
        if self.mark == mark && self.collateral_marks == collateral { return false; }
        self.mark = mark;
        self.collateral_marks = collateral;
        true
    }

    /// The pool's cash change for a market of these decimals.
    fn cash_wei(&self, size_decimals: u32, price_decimals: u32) -> Result<i64, String> {
        let notional = notional_wei(&self.px, &self.sz, size_decimals, price_decimals)
            .map_err(|err| format!("{} px {:?} sz {:?}: {err}", self.coin, self.px, self.sz))?;
        let flow = match self.side {
            Side::Bid => -notional,
            Side::Ask => notional,
        };
        flow.checked_sub(self.usdc_fee_wei).ok_or_else(|| format!("{} cash overflows", self.coin))
    }
}

/// Why an account's replay stopped: its rows keep the anchor's values until the next anchor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StopReason {
    /// Portfolio margin accounts do not replay (design 000433 r2 3.3).
    PortfolioMargin,
    FundingBalanceUnknown,
    UnknownAccountMode(String),
    /// A fill of an asset whose isolated setting or leverage changed in the same block: the order of the two inside
    /// the block is not measured (review 000435 r1 P2).
    IsolatedFill,
    /// The cross positions and margin may move to the liquidator wholesale, without events.
    BackstopLiquidation,
    AccountModeSwitch,
    /// Moves a unified account's spot USDC into or out of lending; the direction is unverified.
    BorrowLend,
    UnreadLedger(String),
    /// An event on a dex not settled in USDC.
    NonUsdcCollateral,
    /// A unified account's `spotTransfer` of another token with a USDC fee (r3 revision 3).
    NonUsdcSpotTransferFee,
    /// A transfer USDC fee whose funding balance is unknown outside unified pools: the node takes it from either
    /// side (bm 2026-10-20: `0xb4dca8c9…` sent all 278 USDC of spot and the 0.001566 fee left the main dex;
    /// `0x04447ced…` sent 20,000 USDC from the main dex to spot and the 0.001605 fee did not), which spot outside the
    /// pools cannot tell. Even perp-to-perp `send` can pay from spot (1193767034/1193767240).
    SpotFeeSource,
    /// A unified account's spot fill of a pair not quoted in USDC.
    NonUsdcSpotFill,
    /// Midnight system conversion with incomplete batch evidence or an unverified allocation path.
    SpotDustConversion(String),
    /// A unified account's HIP-4 outcome fill (`#N`): its sizes are not measured.
    OutcomeFill,
    UnknownMarket(String),
    /// Target-leverage top-up needs the mark, size and isolated equity at its execution point.
    /// Client 6911b792's 0x3c94170 rule is known; that action state and a live golden are still absent.
    TopUpIsolatedOnlyMargin { asset: u32, target_leverage: String },
    /// The state contradicts an event or the event does not read (the text says which).
    Contradiction(String),
}

impl StopReason {
    /// The view's `replayStoppedReason`: the label, and what the reason names when it names one.
    pub(crate) fn text(&self) -> String {
        match self {
            Self::UnknownAccountMode(detail) | Self::UnreadLedger(detail) | Self::UnknownMarket(detail) | Self::SpotDustConversion(detail) | Self::Contradiction(detail) => {
                format!("{}:{detail}", self.label())
            }
            Self::TopUpIsolatedOnlyMargin { asset, target_leverage } =>
                format!("{}:asset={asset},targetLeverage={target_leverage},executionStateUnverified", self.label()),
            _ => self.label().to_string(),
        }
    }

    /// The metric label.
    pub(crate) const fn label(&self) -> &'static str {
        match self {
            Self::PortfolioMargin => "portfolioMargin",
            Self::FundingBalanceUnknown => "fundingBalanceUnknown",
            Self::UnknownAccountMode(_) => "unknownAccountMode",
            Self::IsolatedFill => "isolatedFill",
            Self::BackstopLiquidation => "backstopLiquidation",
            Self::AccountModeSwitch => "accountModeSwitch",
            Self::BorrowLend => "borrowLend",
            Self::UnreadLedger(_) => "unreadLedger",
            Self::NonUsdcCollateral => "nonUsdcCollateral",
            Self::NonUsdcSpotTransferFee => "nonUsdcSpotTransferFee",
            Self::SpotFeeSource => "spotFeeSource",
            Self::NonUsdcSpotFill => "nonUsdcSpotFill",
            Self::SpotDustConversion(_) => "spotDustConversion",
            Self::OutcomeFill => "outcomeFill",
            Self::UnknownMarket(_) => "unknownMarket",
            Self::TopUpIsolatedOnlyMargin { .. } => "topUpIsolatedOnlyMargin",
            Self::Contradiction(_) => "contradiction",
        }
    }
}

/// The block an account stopped at (`None`: at the anchor) and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Stop {
    pub(crate) block: Option<u64>,
    pub(crate) reason: StopReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Pools {
    /// default / disabled: each USDC dex's cross cash `u`, by dex index; a dex without state holds 0.
    PerDex(FxHashMap<usize, i64>),
    /// unified: spot USDC `t` plus the cross cash of every USDC dex.
    Unified(i64),
}

/// One account's margin state, 10^-8 USDC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AccountMargin {
    pools: Pools,
    /// Isolated cash `l.I.u` of each open isolated position on a USDC dex, by (dex, asset).
    isolated: FxHashMap<(usize, usize), i64>,
    /// (dex, asset) on USDC dexes the account set isolated, with the leverage (`l.I.l`, `updateLeverage`): a position
    /// opened there is isolated, and its fills move margin at that leverage.
    isolated_leverage: FxHashMap<(usize, usize), u32>,
    /// Dexes with cross exposure at the anchor or a cross fill/setting switch since it. This is a conservative
    /// provenance flag, not a second position book: only a new anchor can clear it. Their transfer capacity needs
    /// execution-time cross marks, which the isolated fill's own mark cannot establish.
    cross_exposure_seen: FxHashSet<usize>,
    /// Frozen cross holdings from the checkpoint, not another live position book. Any cross fill or leverage
    /// action removes that pool's proof until reseeding. A Unified account may consume a single-dex
    /// proof only while all USDC cash and positions stay in that dex; spot/foreign-dex movement clears it.
    cross_capacity: FxHashMap<usize, cross_capacity::CrossCapacitySnapshot>,
    /// The assets whose isolated setting or leverage changed in a block, with that block: a fill of the same block may
    /// come before or after the change (review 000435 r1 P2).
    switched: Option<(u64, Vec<(usize, usize)>)>,
    /// Manual isolated transfers are applied before fills by this stream adapter. Within that same block their
    /// true position relative to fills is unavailable; remember affected collateral pools, not a guessed order.
    margin_updates: Option<(u64, Vec<usize>)>,
    /// A transfer since the anchor lacked execution marks or cross capacity determining one exact micro-USDC amount.
    estimated: bool,
    stop: Option<Stop>,
}

impl AccountMargin {
    fn of(account: &Account, markets: &Markets) -> Self {
        let stopped = |reason| Self {
            pools: Pools::Unified(0),
            isolated: FxHashMap::default(),
            isolated_leverage: FxHashMap::default(),
            cross_exposure_seen: FxHashSet::default(),
            cross_capacity: FxHashMap::default(),
            switched: None,
            margin_updates: None,
            estimated: false,
            stop: Some(Stop { block: None, reason }),
        };
        let unified = match &account.mode {
            AccountMode::Default | AccountMode::Disabled => false,
            AccountMode::Unified => true,
            AccountMode::PortfolioMargin => return stopped(StopReason::PortfolioMargin),
            AccountMode::Unknown(code) => return stopped(StopReason::UnknownAccountMode(code.clone())),
        };
        // Native capacity is per clearinghouse. Unified routing is equivalent only when there is no
        // USDC cash or position outside one dex; do not substitute the summed pool in the general case.
        let single_dex = unified.then(|| Self::single_usdc_dex(account, markets)).flatten();
        let mut cross_exposure_seen = FxHashSet::default();
        let mut cross_capacity = FxHashMap::default();
        let (mut cross, mut isolated, mut isolated_leverage) = (FxHashMap::default(), FxHashMap::default(), FxHashMap::default());
        for state in account.dex_states.iter().filter(|state| markets.usdc_dexes[state.dex]) {
            cross.insert(state.dex, state.cash_micro * WEI_PER_MICRO);
            if !unified || single_dex == Some(state.dex) {
                let holdings = state.positions.iter().filter_map(|position| {
                    let PositionMargin::Cross { leverage } = position.margin else { return None; };
                    if position.size_raw == 0 { return None; }
                    let coin = &markets.asset_names[state.dex][position.asset];
                    let market = &markets.perps[coin];
                    Some(cross_capacity::Holding { coin: coin.clone(), size_raw: position.size_raw, leverage, size_decimals: market.size_decimals })
                }).collect();
                cross_capacity.insert(state.dex, cross_capacity::CrossCapacitySnapshot { holdings });
            }
            for position in &state.positions {
                if let PositionMargin::Isolated { cash_micro, leverage } = position.margin {
                    isolated.insert((state.dex, position.asset), cash_micro * WEI_PER_MICRO);
                    isolated_leverage.insert((state.dex, position.asset), leverage);
                } else if position.size_raw != 0 {
                    cross_exposure_seen.insert(state.dex);
                }
            }
            isolated_leverage.extend(state.isolated_settings.iter().map(|(asset, leverage)| ((state.dex, *asset), *leverage)));
        }
        let pools = if unified {
            let spot = account.spot.iter().find(|(token, _)| *token == USDC).map_or(0, |(_, raw)| *raw);
            Pools::Unified(spot + cross.values().sum::<i64>())
        } else {
            Pools::PerDex(cross)
        };
        Self { pools, isolated, isolated_leverage, cross_exposure_seen, cross_capacity, switched: None, margin_updates: None, estimated: false, stop: None }
    }

    /// A proof of no external USDC funding route at the anchor, not a second cash ledger.
    fn single_usdc_dex(account: &Account, markets: &Markets) -> Option<usize> {
        if account.spot.iter().any(|(token, cash)| *token == USDC && *cash != 0) { return None; }
        let mut active = account.dex_states.iter().filter(|state| markets.usdc_dexes[state.dex]).filter(|state|
            state.cash_micro != 0 || state.positions.iter().any(|position| position.size_raw != 0 ||
                matches!(position.margin, PositionMargin::Isolated { cash_micro, .. } if cash_micro != 0)));
        let dex = active.next()?.dex;
        active.next().is_none().then_some(dex)
    }

    /// Evidence text: pools by dex index (or the unified pool), isolated cash by "dex:asset", in USDC.
    pub(crate) fn json(&self) -> serde_json::Value {
        let usdc = |wei: i64| format_size_raw(wei, WEI_DECIMALS);
        let isolated: BTreeMap<String, String> =
            self.isolated.iter().map(|((dex, asset), wei)| (format!("{dex}:{asset}"), usdc(*wei))).collect();
        let pools = match &self.pools {
            Pools::PerDex(cross) => {
                json!({"perDex": cross.iter().map(|(dex, wei)| (dex.to_string(), usdc(*wei))).collect::<BTreeMap<_, _>>()})
            }
            Pools::Unified(pool) => json!({"unified": usdc(*pool)}),
        };
        json!({"pools": pools, "isolated": isolated})
    }

    fn stop_at(&mut self, block: u64, reason: StopReason) {
        self.stop.get_or_insert(Stop { block: Some(block), reason });
    }

    /// Records that `key`'s isolated setting or leverage changed in `block`.
    fn switch(&mut self, block: u64, key: (usize, usize)) {
        match &mut self.switched {
            Some((switched_block, keys)) if *switched_block == block => keys.push(key),
            switched => *switched = Some((block, vec![key])),
        }
    }

    fn switched_in(&self, block: u64, key: (usize, usize)) -> bool {
        self.switched.as_ref().is_some_and(|(switched_block, keys)| *switched_block == block && keys.contains(&key))
    }

    fn note_margin_update(&mut self, block: u64, dex: usize) {
        match &mut self.margin_updates {
            Some((height, dexes)) if *height == block => { if !dexes.contains(&dex) { dexes.push(dex); } }
            updates => *updates = Some((block, vec![dex])),
        }
    }

    fn margin_updated_in(&self, block: u64, dex: usize) -> bool {
        self.margin_updates.as_ref().is_some_and(|(height, dexes)| *height == block && (self.unified() || dexes.contains(&dex)))
    }

    const fn unified(&self) -> bool {
        matches!(self.pools, Pools::Unified(_))
    }

    /// Adds to a USDC dex's cross cash (a unified account's pool).
    fn add_cross(&mut self, dex: usize, wei: i64) {
        if self.unified() && !self.cross_capacity.contains_key(&dex) {
            self.cross_capacity.clear();
        }
        match &mut self.pools {
            Pools::PerDex(cross) => *cross.entry(dex).or_default() += wei,
            Pools::Unified(pool) => *pool += wei,
        }
    }

    /// Spendable micro USDC before closing the old isolated leg. With no cross exposure the client's
    /// `max(cross_equity - cross_initial_margin, 0)` is simply positive cash. Preserve sub-micro spot dust.
    /// A unified pool shares cross exposure across all USDC dexes; standard pools do not.
    fn isolated_capacity_micro(&self, dex: usize) -> Option<i64> {
        let cash = match &self.pools {
            Pools::PerDex(cross) if !self.cross_exposure_seen.contains(&dex) => cross.get(&dex).copied().unwrap_or(0),
            Pools::Unified(cash) if self.cross_exposure_seen.is_empty() => *cash,
            _ => return None,
        };
        Some((cash / WEI_PER_MICRO).max(0))
    }

    /// Adds to spot USDC: only a unified account's pool counts it.
    fn add_spot(&mut self, wei: i64) {
        if let Pools::Unified(pool) = &mut self.pools {
            if wei != 0 { self.cross_capacity.clear(); }
            *pool += wei;
        }
    }
}

/// The replayed margin of every account that held a position at the anchor, with the anchor's universe.
#[derive(Debug, Clone, Default)]
pub(crate) struct MarginStates {
    markets: Markets,
    accounts: FxHashMap<Address, AccountMargin>,
}

impl MarginStates {
    #[cfg(test)]
    pub(crate) fn stop_reason(&self, user: &Address) -> Option<&StopReason> {
        self.accounts.get(user).and_then(|account| account.stop.as_ref()).map(|stop| &stop.reason)
    }

    pub(crate) fn from_checkpoint(checkpoint: &CheckpointAccounts) -> Result<Self, String> {
        let markets = Markets::of(checkpoint)?;
        let accounts = checkpoint.accounts.iter().map(|(user, account)| (*user, AccountMargin::of(account, &markets))).collect();
        Ok(Self { markets, accounts })
    }

    /// Rebuild only accounts whose retained execution-price evidence changed. Other accounts and the immutable
    /// market universe already belong to this anchor; cloning the full 135k-account state would add avoidable work.
    pub(crate) fn restart_accounts(&mut self, anchor: &Self, users: &FxHashSet<Address>) {
        for user in users {
            if let Some(account) = anchor.accounts.get(user) {
                self.accounts.insert(*user, account.clone());
            } else {
                self.accounts.remove(user);
            }
        }
    }

    pub(crate) fn collateral_coins<'a>(&self, fills: impl Iterator<Item = &'a crate::types::node_data::NodeDataFill>) -> Vec<String> {
        let mut pools = FxHashSet::default();
        let mut coins = FxHashSet::default();
        for crate::types::node_data::NodeDataFill(user, fill) in fills {
            let Some(account) = self.accounts.get(user).filter(|account| account.stop.is_none()) else { continue; };
            let Some(market) = self.markets.perps.get(&fill.coin) else { continue; };
            if matches!(market.margin_mode, AssetMarginMode::Normal) && !account.isolated_leverage.contains_key(&(market.dex, market.asset)) { continue; }
            if !pools.insert((*user, market.dex)) { continue; }
            if let Some(snapshot) = account.cross_capacity.get(&market.dex) {
                coins.extend(snapshot.holdings.iter().map(|held| held.coin.clone()));
            }
        }
        coins.into_iter().collect()
    }

    /// Moves `user`'s state by its event of `block`; accounts not tracked or stopped are left as they are. Returns
    /// whether the account was replaying, so its replayed rows may have changed (moved, or stopped here).
    pub(crate) fn apply(&mut self, block: u64, user: Address, event: &AccountEvent) -> bool {
        let Some(account) = self.accounts.get_mut(&user).filter(|account| account.stop.is_none()) else {
            return false;
        };
        let markets = &self.markets;
        let outcome = match event {
            AccountEvent::Fill(fill) => apply_fill(account, markets, block, fill),
            AccountEvent::UnreadableFill(err) => Err(StopReason::Contradiction(format!("unreadable fill: {err}"))),
            AccountEvent::Funding { coin, amount_wei } | AccountEvent::FundingBalance { coin, amount_wei } =>
                apply_funding(account, markets, coin, *amount_wei),
            AccountEvent::FundingBalanceUnknown => Err(StopReason::FundingBalanceUnknown),
            AccountEvent::Ledger(delta) => apply_ledger(account, markets, user, delta),
            AccountEvent::Action(action) => apply_action(account, markets, block, action),
        };
        if let Err(reason) = outcome {
            account.stop_at(block, reason);
        }
        true
    }
}

type Applied = Result<(), StopReason>;

fn perp<'m>(markets: &'m Markets, coin: &str) -> Result<&'m PerpMarket, StopReason> {
    let market = markets.perps.get(coin).ok_or_else(|| StopReason::UnknownMarket(coin.to_string()))?;
    if !markets.usdc_dexes[market.dex] {
        return Err(StopReason::NonUsdcCollateral);
    }
    Ok(market)
}

/// A main dex amount (deposits, withdrawals, internal and vault transfers).
fn add_main(account: &mut AccountMargin, markets: &Markets, wei: i64) -> Applied {
    add_balance(account, markets, &Balance::Dex(String::new()), wei)
}

fn add_balance(account: &mut AccountMargin, markets: &Markets, balance: &Balance, wei: i64) -> Applied {
    match balance {
        Balance::Spot => account.add_spot(wei),
        Balance::Dex(name) => {
            let dex = *markets.dexes.get(name).ok_or_else(|| StopReason::UnknownMarket(format!("dex {name:?}")))?;
            if !markets.usdc_dexes[dex] {
                return Err(StopReason::NonUsdcCollateral);
            }
            account.add_cross(dex, wei);
        }
    }
    Ok(())
}

fn apply_fill(account: &mut AccountMargin, markets: &Markets, block: u64, fill: &FillEvent) -> Applied {
    match Market::of(&fill.coin) {
        Market::Perp => apply_perp_fill(account, markets, block, fill),
        Market::Spot(index) => apply_spot_fill(account, markets, fill, index),
        Market::Outcome => if account.unified() { Err(StopReason::OutcomeFill) } else { Ok(()) },
    }
}

/// What a fill's coin names: a perp (by name; one listed after the anchor is unknown to it), spot pair `N` (`@N`, or
/// `PURR/USDC`, pair 0 and the only named one), or a HIP-4 outcome (`#N`).
enum Market {
    Perp,
    Spot(Option<usize>),
    Outcome,
}

impl Market {
    fn of(coin: &str) -> Self {
        if let Some(pair) = spot_pair_of_coin(coin) {
            return Self::Spot(Some(pair));
        }
        if coin.starts_with('@') {
            return Self::Spot(None);
        }
        if coin.starts_with('#') { Self::Outcome } else { Self::Perp }
    }
}

/// A cross fill moves its dex's cross cash, an isolated one the position's cash and its dex's cross cash
/// (`apply_isolated_fill`). Whether the position is isolated: the asset's `marginMode`, then the account's setting
/// (research "新开仓位是全仓还是逐仓"). Settings and leverage actions still precede fills in this replay; a setting
/// changed in the fill's block leaves its pool or leverage unknown: a stop. Proven post-fill manual margin transfers
/// are ordered separately by the block owner.
fn apply_perp_fill(account: &mut AccountMargin, markets: &Markets, block: u64, fill: &FillEvent) -> Applied {
    let market = perp(markets, &fill.coin)?;
    if fill.backstop {
        return Err(StopReason::BackstopLiquidation);
    }
    let key = (market.dex, market.asset);
    if account.switched_in(block, key) {
        return Err(StopReason::IsolatedFill);
    }
    let isolated = match &market.margin_mode {
        AssetMarginMode::NoCross | AssetMarginMode::StrictIsolated => true,
        AssetMarginMode::Normal => account.isolated_leverage.contains_key(&key),
        AssetMarginMode::Unknown(mode) => return Err(StopReason::UnknownMarket(format!("{} marginMode {mode:?}", fill.coin))),
    };
    let cash = fill.cash_wei(market.size_decimals, MAX_PERP_DECIMALS - market.size_decimals).map_err(StopReason::Contradiction)?;
    if !isolated {
        account.cross_exposure_seen.insert(market.dex);
        account.cross_capacity.remove(&market.dex);
        account.add_cross(market.dex, cash);
        return Ok(());
    }
    // Same-block updateIsolatedMargin and a fill do not commute: reduction shares and opening capacity change.
    // This is an ordering/estimation gap, not a failed action or a reason to stop the account's replay.
    account.estimated |= account.margin_updated_in(block, market.dex);
    let leverage = account.isolated_leverage.get(&key).copied().unwrap_or(market.default_leverage);
    apply_isolated_fill(account, market, fill, cash, leverage).map_err(StopReason::Contradiction)
}

/// An isolated position's fill, in the node's micro USDC (research "逐仓成交的划转", checkpoints 1192420000 →
/// 1192440000, every single-fill isolated position): the fee leaves the position's cash first;
/// - a partial reduction then moves the reduced share of the rest to cross, truncated toward zero (488 of 488 exact);
/// - a close moves all of it (768 of 768);
/// - an open or increase takes mark-based initial margin plus immediate loss, with fill-notional/leverage as a floor;
/// - a flip closes the old position and funds only the residual new position, leaving the whole fee with the old one.
///
/// The supplemented rule explains 815/815 opens/increases and 58/58 flips with recorded mark evidence; unknown
/// execution prices still use the explicitly estimated floor. Funding is capped by available cross margin; six
/// live cases at 1192650000→1192660000 establish the cash-only path, including five zero/dust final balances.
/// Static per-dex cross holdings use their complete block mark envelopes for exact capacity. Moving holdings or
/// ambiguous prices preserve an explicit estimate; raw cash alone is not free margin.
///
/// `cash_wei` is the fill's whole cash flow (`FillEvent::cash_wei`); cross takes what the position does not.
fn apply_isolated_fill(account: &mut AccountMargin, market: &PerpMarket, fill: &FillEvent, cash_wei: i64, leverage: u32) -> Result<(), String> {
    let key = (market.dex, market.asset);
    let context = |what: &str| format!("isolated {} fill: {what}", fill.coin);
    let start = parse_size_raw(&fill.start_position, market.size_decimals).map_err(|err| context(&err))?;
    let size = parse_size_raw(&fill.sz, market.size_decimals).map_err(|err| context(&err))?;
    let end = match fill.side {
        Side::Bid => start + size,
        Side::Ask => start - size,
    };
    let held = account.isolated.get(&key).copied();
    if (start == 0) == held.is_some() {
        return Err(context(&format!("from {} with isolated cash {held:?}", fill.start_position)));
    }
    let micro = |wei: i64, what: &str| {
        if wei % WEI_PER_MICRO == 0 { Ok(wei / WEI_PER_MICRO) } else { Err(context(&format!("{what} {wei} is not whole micro USDC"))) }
    };
    let cash_before = micro(held.unwrap_or(0), "the cash")?;
    let fee = micro(fill.usdc_fee_wei, "the fee")?;
    let price_raw = parse_size_raw(&fill.px, MAX_PERP_DECIMALS - market.size_decimals).map_err(|err| context(&err))?;
    // px x size in micro USDC: perp prices and sizes carry six decimals between them
    let notional = |size_raw: i64| i64::try_from(i128::from(price_raw) * i128::from(size_raw.abs())).map_err(|err| context(&err.to_string()));
    let flow = |size_raw: i64| -> Result<i64, String> {
        let notional = notional(size_raw)?;
        Ok(if fill.side == Side::Bid { -notional } else { notional })
    };
    let capacity = match account.isolated_capacity_micro(market.dex) {
        Some(capacity) => Some(capacity),
        None => match (&account.pools, account.cross_capacity.get(&market.dex), &fill.collateral_marks) {
            (Pools::PerDex(cash), Some(snapshot), Some(prices)) => snapshot.available_micro(cash.get(&market.dex).copied().unwrap_or(0), prices)?,
            (Pools::Unified(cash), Some(snapshot), Some(prices)) => snapshot.available_micro(*cash, prices)?,
            _ => None,
        },
    };
    let margin = |size_raw: i64, released_micro: i64| {
        let cap = capacity.map(|available| available.checked_add(released_micro)
            .ok_or_else(|| context("transfer capacity overflows"))).transpose()?;
        isolated::transfer(size_raw.abs(), price_raw, fill.side, leverage, market.size_decimals, fill.mark, cap)
            .map_err(|err| context(&err))
    };
    let same_side = (start > 0) == (end > 0);
    let cash_after = if start == 0 || same_side && end.abs() > start.abs() {
        let transfer = margin(size, 0)?;
        account.estimated |= transfer.estimated;
        cash_before + flow(size)? - fee + transfer.micro_usdc
    } else if end == 0 {
        0
    } else if same_side {
        let after_fee = cash_before.checked_sub(fee).ok_or_else(|| context("cash after fee overflows"))?;
        isolated::reduced_cash(after_fee, start, size, price_raw).map_err(|err| context(&err))?
    } else {
        // Close the OLD leg first and charge the WHOLE fee there. Its release is available for the residual
        // new leg, including when the pool was empty before the flip. Negative release retains the old deficit.
        let released = cash_before.checked_add(flow(start)?).and_then(|cash| cash.checked_sub(fee))
            .ok_or_else(|| context("closing cash overflows"))?;
        let transfer = margin(end, released)?;
        account.estimated |= transfer.estimated;
        flow(end)? + transfer.micro_usdc
    };
    if end == 0 {
        account.isolated.remove(&key);
    } else {
        account.isolated.insert(key, cash_after * WEI_PER_MICRO);
    }
    account.add_cross(market.dex, cash_wei - (cash_after - cash_before) * WEI_PER_MICRO);
    Ok(())
}

/// Spot fills of USDC pairs move a unified account's spot USDC; the other modes keep spot out of their pools.
fn apply_spot_fill(account: &mut AccountMargin, markets: &Markets, fill: &FillEvent, index: Option<usize>) -> Applied {
    if !account.unified() {
        return Ok(());
    }
    let pair = index.and_then(|index| markets.spot_pairs.get(index)).ok_or_else(|| StopReason::UnknownMarket(fill.coin.clone()))?;
    if pair.quote_token != USDC {
        return Err(StopReason::NonUsdcSpotFill);
    }
    if fill.spot_dust_conversion {
        let share = fill.dust_share.as_ref().ok_or_else(|| StopReason::SpotDustConversion(fill.coin.clone()))?;
        let cash = share.credit_wei(pair).map_err(|err| StopReason::SpotDustConversion(format!("{}: {err}", fill.coin)))?;
        account.add_spot(cash);
        return Ok(());
    }
    let cash = fill.cash_wei(pair.size_decimals, MAX_SPOT_DECIMALS - pair.size_decimals).map_err(StopReason::Contradiction)?;
    account.add_spot(cash);
    Ok(())
}

/// Funding settles into the position's isolated cash when it is isolated, else into its dex's cross cash.
fn apply_funding(account: &mut AccountMargin, markets: &Markets, coin: &str, amount_wei: i64) -> Applied {
    let market = perp(markets, coin)?;
    match account.isolated.get_mut(&(market.dex, market.asset)) {
        Some(isolated) => *isolated += amount_wei,
        None => account.add_cross(market.dex, amount_wei),
    }
    Ok(())
}

/// The principal source does not identify the fee source. Standard pools omit execution-time spot cash.
const fn spot_fee_source(account: &AccountMargin, usdc_fee_wei: i64) -> Applied {
    if usdc_fee_wei != 0 && !account.unified() { Err(StopReason::SpotFeeSource) } else { Ok(()) }
}

/// `user` is the account the event is applied to; its role (sender, receiver, vault) comes from the delta.
fn apply_ledger(account: &mut AccountMargin, markets: &Markets, user: Address, delta: &LedgerDelta) -> Applied {
    match delta {
        LedgerDelta::Send { user: sender, destination, source, destination_balance, usdc_wei, usdc_fee_wei } => {
            if user == *sender {
                spot_fee_source(account, *usdc_fee_wei)?;
                // another token's send takes only its USDC fee
                let amount = usdc_wei.as_ref().map_or(0, |amount| *amount);
                add_balance(account, markets, source, -(amount + usdc_fee_wei))?;
            }
            if user == *destination
                && let Some(amount) = usdc_wei
            {
                add_balance(account, markets, destination_balance, *amount)?;
            }
        }
        LedgerDelta::SpotTransfer { user: sender, destination, usdc_wei, usdc_fee_wei } => {
            if user == *sender {
                spot_fee_source(account, *usdc_fee_wei)?;
                match usdc_wei {
                    Some(amount) => account.add_spot(-(amount + usdc_fee_wei)),
                    None if *usdc_fee_wei != 0 => return Err(StopReason::NonUsdcSpotTransferFee),
                    None => {}
                }
            }
            if user == *destination
                && let Some(amount) = usdc_wei
            {
                account.add_spot(*amount);
            }
        }
        LedgerDelta::InternalTransfer { user: sender, destination, usdc_wei, fee_wei } => {
            // This delta reports a gross transfer, unlike `send`: H1191755982 debits 50 and credits 49.
            if user == *sender {
                add_main(account, markets, -usdc_wei)?;
            }
            if user == *destination {
                add_main(account, markets, usdc_wei - fee_wei)?;
            }
        }
        LedgerDelta::SubAccountTransfer { user: sender, destination, usdc_wei } => {
            if user == *sender {
                add_main(account, markets, -usdc_wei)?;
            }
            if user == *destination {
                add_main(account, markets, *usdc_wei)?;
            }
        }
        LedgerDelta::ClassTransfer { usdc_wei, to_perp } => {
            let to_main = if *to_perp { *usdc_wei } else { -usdc_wei };
            add_main(account, markets, to_main)?;
            account.add_spot(-to_main);
        }
        LedgerDelta::Deposit { usdc_wei } => add_main(account, markets, *usdc_wei)?,
        LedgerDelta::VaultDeposit { vault, usdc_wei } => add_main(account, markets, if user == *vault { *usdc_wei } else { -usdc_wei })?,
        LedgerDelta::VaultDistribution { vault, usdc_wei } => add_main(account, markets, if user == *vault { -usdc_wei } else { *usdc_wei })?,
        LedgerDelta::VaultWithdraw { user: withdrawer, vault, commission_wei, net_withdrawn_wei } => {
            if user == *vault {
                add_main(account, markets, -(net_withdrawn_wei + commission_wei))?;
            }
            if user == *withdrawer {
                add_main(account, markets, *net_withdrawn_wei)?;
            }
        }
        LedgerDelta::VaultLeaderCommission { user: leader, usdc_wei } => {
            if user == *leader {
                add_main(account, markets, *usdc_wei)?;
            }
        }
        LedgerDelta::RewardsClaim { usdc_wei: Some(amount) } => account.add_spot(*amount),
        LedgerDelta::AccountActivationGas { usdc_wei: Some(amount) } => account.add_spot(-amount),
        // no USDC moves: a withdrawal settles its `withdraw3`, the others are other tokens
        LedgerDelta::Withdraw
        | LedgerDelta::WithoutUsdc(_)
        | LedgerDelta::RewardsClaim { usdc_wei: None }
        | LedgerDelta::AccountActivationGas { usdc_wei: None } => {}
        LedgerDelta::BorrowLend => {
            if account.unified() {
                return Err(StopReason::BorrowLend);
            }
        }
        LedgerDelta::Unread(delta_type) => return Err(StopReason::UnreadLedger(delta_type.clone())),
    }
    Ok(())
}

fn apply_action(account: &mut AccountMargin, markets: &Markets, block: u64, action: &BlockAction) -> Applied {
    let market_of = |asset: u32| {
        let coin = markets.action_assets.get(&asset).ok_or_else(|| StopReason::UnknownMarket(format!("action asset {asset}")))?;
        perp(markets, coin)
    };
    match action {
        BlockAction::IsolatedMargin { asset, ntli_micro } => {
            let market = market_of(*asset)?;
            let ntli = micro_to_wei(*ntli_micro).map_err(StopReason::Contradiction)?;
            let isolated = account.isolated.get_mut(&(market.dex, market.asset)).ok_or_else(|| {
                StopReason::Contradiction(format!("updateIsolatedMargin on asset {asset} without an isolated position"))
            })?;
            *isolated += ntli;
            account.add_cross(market.dex, -ntli);
            account.note_margin_update(block, market.dex);
        }
        BlockAction::Leverage { asset, is_cross, leverage } => {
            let market = market_of(*asset)?;
            // An action can change either membership or a cross IM divisor; invalidate even an idempotent setting.
            account.cross_capacity.remove(&market.dex);
            let key = (market.dex, market.asset);
            // a switch either way, or a new isolated leverage: a fill of the block may precede it
            let changed = if *is_cross {
                account.isolated_leverage.remove(&key).is_some()
            } else {
                account.isolated_leverage.insert(key, *leverage) != Some(*leverage)
            };
            if changed {
                if *is_cross { account.cross_exposure_seen.insert(market.dex); }
                account.switch(block, key);
            }
        }
        BlockAction::TopUpIsolatedOnlyMargin { asset, target_leverage } => {
            return Err(StopReason::TopUpIsolatedOnlyMargin { asset: *asset, target_leverage: target_leverage.clone() });
        }
        BlockAction::SetAbstraction => return Err(StopReason::AccountModeSwitch),
        BlockAction::Withdraw { amount_wei } => add_main(account, markets, -amount_wei)?,
        BlockAction::ReserveRequestWeight { cost_wei } => add_main(account, markets, -cost_wei)?,
    }
    Ok(())
}

/// A liquidation price from the replayed state (designs 000437, 000442): an isolated position's own pool, or a cross
/// position's pool with its other members held at the marks of the same height.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReplayedRow {
    pub(crate) isolated: bool,
    /// The positions of its pool, itself included.
    pub(crate) pool_positions: usize,
    pub(crate) liquidation_px: LiquidationPx,
    /// The account's cash went through an isolated open, increase or flip since the anchor (the modelled transfer):
    /// an isolated position may hold less and a cross pool more than the node's, so a cross price may be farther from
    /// the mark than the node's as well as an isolated one nearer (review 000466 r1 P2-1).
    pub(crate) cash_estimated: bool,
}

/// Another open position of the account, as a replayed row of its pool needs it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HeldPosition<'c> {
    pub(crate) coin: &'c str,
    /// In 10^-szDecimals.
    pub(crate) size_raw: i64,
    /// `None` when unknown: its pool then has no replayed row.
    pub(crate) table_id: Option<u32>,
}

impl MarginStates {
    /// Whether the account is tracked and its replay has not stopped.
    pub(crate) fn replays(&self, user: &Address) -> bool {
        self.accounts.get(user).is_some_and(|account| account.stop.is_none())
    }

    /// Whether `coin` is a perp of a USDC dex of the anchor: the only markets with replayed rows.
    pub(crate) fn usdc_perp(&self, coin: &str) -> bool {
        self.markets.perps.get(coin).is_some_and(|market| self.markets.usdc_dexes[market.dex])
    }

    /// The account's stop, if its replay stopped; `None` also for an account not tracked.
    pub(crate) fn stop(&self, user: &Address) -> Option<&Stop> {
        self.accounts.get(user).and_then(|account| account.stop.as_ref())
    }

    /// The replayed row of `user`'s position in `coin` (`size_raw` in 10^-szDecimals, margin table `table_id`), given
    /// the account's open positions `held` (every coin, `coin` included or not) and `mark_of` the marks of the same
    /// height in price units. `None` when it has none: the account is not tracked or stopped, the coin is not a USDC
    /// perp of the anchor, or a cross position shares its pool (default / disabled: the same dex; unified: every USDC
    /// dex) with a position of an unknown coin or table, or without a mark.
    pub(crate) fn replayed_row<'c>(
        &self,
        user: &Address,
        coin: &str,
        size_raw: i64,
        table_id: u32,
        held: impl IntoIterator<Item = HeldPosition<'c>>,
        mark_of: impl Fn(&str) -> Option<u64>,
    ) -> Option<ReplayedRow> {
        let account = self.accounts.get(user).filter(|account| account.stop.is_none())?;
        let market = self.markets.perps.get(coin).filter(|market| self.markets.usdc_dexes[market.dex])?;
        let table = self.markets.tables[market.dex].get(&table_id)?;
        let size = base_size(size_raw, market.size_decimals);
        if let Some(isolated_wei) = account.isolated.get(&(market.dex, market.asset)) {
            let liquidation_px = solve(wei_to_usdc(*isolated_wei), 0.0, size, table);
            return Some(ReplayedRow { isolated: true, pool_positions: 1, liquidation_px, cash_estimated: account.estimated });
        }
        let mut others = Vec::new();
        for other in held.into_iter().filter(|other| other.coin != coin) {
            let other_market = self.markets.perps.get(other.coin)?;
            if !self.cross_member(account, market, other_market) {
                continue;
            }
            others.push(HeldAtMark {
                size_raw: other.size_raw,
                size_decimals: other_market.size_decimals,
                mark_raw: mark_of(other.coin)?,
                table: self.markets.tables[other_market.dex].get(&other.table_id?)?,
            });
        }
        let cash_wei = match &account.pools {
            Pools::PerDex(cross) => cross.get(&market.dex).copied().unwrap_or(0),
            Pools::Unified(pool) => *pool,
        };
        let liquidation_px = solve_held(wei_to_usdc(cash_wei), &others, size, table);
        Some(ReplayedRow { isolated: false, pool_positions: 1 + others.len(), liquidation_px, cash_estimated: account.estimated })
    }
}

impl MarginStates {
    /// Whether `other` is a cross position of the pool of `market`'s cross position (default / disabled: the same dex;
    /// unified: every USDC dex).
    fn cross_member(&self, account: &AccountMargin, market: &PerpMarket, other: &PerpMarket) -> bool {
        let same_pool = match &account.pools {
            Pools::PerDex(_) => other.dex == market.dex,
            Pools::Unified(_) => self.markets.usdc_dexes[other.dex],
        };
        same_pool && !account.isolated.contains_key(&(other.dex, other.asset))
    }

    /// The other coins of `user`'s cross pool in `coin` among `held` (design 000443 r2 revision 5); empty for an isolated
    /// position, an account not replaying, or a coin that is not a USDC perp of the anchor.
    pub(crate) fn cross_pool_coins<'c>(&self, user: &Address, coin: &str, held: impl IntoIterator<Item = &'c str>) -> Vec<&'c str> {
        let Some(account) = self.accounts.get(user).filter(|account| account.stop.is_none()) else { return Vec::new() };
        let Some(market) = self.markets.perps.get(coin).filter(|market| self.markets.usdc_dexes[market.dex]) else {
            return Vec::new();
        };
        if account.isolated.contains_key(&(market.dex, market.asset)) {
            return Vec::new();
        }
        let member = |other: &str| self.markets.perps.get(other).is_some_and(|other| self.cross_member(account, market, other));
        held.into_iter().filter(|other| *other != coin && member(other)).collect()
    }
}

fn wei_to_usdc(wei: i64) -> f64 {
    wei as f64 / 1e8
}

/// The replay of one anchor interval against the next anchor.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct SelfCheck {
    /// Replayed accounts compared, by mode label (`perDex`, `unified`).
    pub(crate) compared: FxHashMap<&'static str, Compared>,
    /// Accounts whose replay stopped in the interval or at the earlier anchor, by reason label.
    pub(crate) stopped: FxHashMap<&'static str, u64>,
    /// Up to `MAX_MISMATCH_SAMPLES`.
    pub(crate) samples: Vec<Mismatch>,
    /// Up to `MAX_MISMATCH_SAMPLES` of the `estimated` ones: the modelled transfers' residuals.
    pub(crate) estimated_samples: Vec<Mismatch>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Compared {
    pub(crate) exact: u64,
    /// Of the exact ones, those whose cash moved in the interval: what the replay actually proved.
    pub(crate) moved: u64,
    pub(crate) mismatched: u64,
    /// Accounts after an isolated open, increase or flip whose pools' totals equal the anchor's and whose split between
    /// cross and isolated cash does not (the modelled transfer): not mismatches.
    pub(crate) estimated: u64,
}

/// A replayed account the anchor contradicts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Mismatch {
    pub(crate) user: Address,
    pub(crate) replayed: AccountMargin,
    pub(crate) anchor: AccountMargin,
}

impl MarginStates {
    /// Compares this replay, started from `start`, with the anchor's state, account by account: every account tracked
    /// by both whose replay did not stop. Pools and isolated cash must be equal to the last 10^-8.
    pub(crate) fn check_against(&self, anchor: &Self, start: &Self) -> SelfCheck {
        let mut check = SelfCheck::default();
        for (user, replayed) in &self.accounts {
            if let Some(stop) = &replayed.stop {
                *check.stopped.entry(stop.reason.label()).or_default() += 1;
                continue;
            }
            let Some(anchored) = anchor.accounts.get(user).filter(|anchored| anchored.stop.is_none()) else {
                continue;
            };
            let label = if replayed.unified() { "unified" } else { "perDex" };
            let counts = check.compared.entry(label).or_default();
            if same_cash(replayed, anchored) {
                counts.exact += 1;
                counts.moved += u64::from(start.accounts.get(user).is_none_or(|before| !same_cash(replayed, before)));
                continue;
            }
            if only_split_differs(replayed, anchored) {
                counts.estimated += 1;
                if check.estimated_samples.len() < MAX_MISMATCH_SAMPLES {
                    check.estimated_samples.push(Mismatch { user: *user, replayed: replayed.clone(), anchor: anchored.clone() });
                }
                continue;
            }
            counts.mismatched += 1;
            if check.samples.len() < MAX_MISMATCH_SAMPLES {
                check.samples.push(Mismatch { user: *user, replayed: replayed.clone(), anchor: anchored.clone() });
            }
        }
        check
    }
}

impl MarginStates {
    /// Whether `user`'s replayed cash is off `anchor`'s only in the split between cross and isolated after a modelled
    /// transfer (`only_split_differs`): the cash check counts such an account `estimated`, and the row check its rows
    /// off the anchor's price (review 000468 r1 P2: not every account that used the model).
    pub(crate) fn split_estimated(&self, anchor: &Self, user: &Address) -> bool {
        let anchored = anchor.accounts.get(user).filter(|anchored| anchored.stop.is_none());
        self.accounts.get(user).filter(|replayed| replayed.stop.is_none()).zip(anchored).is_some_and(|(replayed, anchored)| only_split_differs(replayed, anchored))
    }

    /// Moves `wei` of `user`'s cross cash on `dex` into its isolated cash of `key` (a test's anchor off the replay's split).
    #[cfg(test)]
    pub(crate) fn shift_to_isolated(&mut self, user: &Address, dex: usize, key: (usize, usize), wei: i64) {
        let account = self.accounts.get_mut(user).expect("a tracked account");
        *account.isolated.get_mut(&key).expect("its isolated cash") += wei;
        account.add_cross(dex, -wei);
    }

    /// Adds `wei` to `user`'s cross cash on `dex` (a test's anchor off the replay's total).
    #[cfg(test)]
    pub(crate) fn add_cross_cash(&mut self, user: &Address, dex: usize, wei: i64) {
        self.accounts.get_mut(user).expect("a tracked account").add_cross(dex, wei);
    }
}

/// A modelled transfer only splits a pool's cash between cross and isolated: off the anchor, but each pool's total exact.
fn only_split_differs(replayed: &AccountMargin, anchored: &AccountMargin) -> bool {
    replayed.estimated && !same_cash(replayed, anchored) && totals(replayed) == totals(anchored)
}

/// Each pool's cash with its isolated cash added (default / disabled: by dex, a dex without state at 0; unified: one).
fn totals(account: &AccountMargin) -> BTreeMap<usize, i64> {
    let mut totals = BTreeMap::new();
    let pool_of = |dex: usize| if account.unified() { 0 } else { dex };
    match &account.pools {
        Pools::PerDex(cross) => cross.iter().for_each(|(dex, wei)| *totals.entry(*dex).or_default() += wei),
        Pools::Unified(pool) => *totals.entry(0).or_default() += pool,
    }
    for ((dex, _), wei) in &account.isolated {
        *totals.entry(pool_of(*dex)).or_default() += wei;
    }
    totals.retain(|_, wei| *wei != 0);
    totals
}

/// Equal pools and isolated cash; a dex without state holds 0.
fn same_cash(replayed: &AccountMargin, anchored: &AccountMargin) -> bool {
    let pools = match (&replayed.pools, &anchored.pools) {
        (Pools::Unified(left), Pools::Unified(right)) => left == right,
        (Pools::PerDex(left), Pools::PerDex(right)) => {
            left.keys().chain(right.keys()).all(|dex| left.get(dex).unwrap_or(&0) == right.get(dex).unwrap_or(&0))
        }
        _ => false,
    };
    pools && replayed.isolated == anchored.isolated
}

#[cfg(test)]
pub(crate) mod tests {
    use serde_json::json;

    use super::*;
    use crate::accounts::checkpoint::{
        parse_checkpoint_bytes,
        tests::{HOLDER, ISOLATED, PORTFOLIO, fixture, user},
    };

    /// The fixture's state (`checkpoint::tests::fixture`), with `edit` applied to its wire JSON first.
    fn states(edit: impl FnOnce(&mut serde_json::Value)) -> MarginStates {
        let mut checkpoint: serde_json::Value = rmp_serde::from_slice(&fixture()).unwrap();
        edit(&mut checkpoint);
        MarginStates::from_checkpoint(&parse_checkpoint_bytes(&rmp_serde::to_vec(&checkpoint).unwrap()).unwrap()).unwrap()
    }

    /// The fixture with HOLDER a default account: main dex cash -12000, xyz 30.2697 (flx settles in USDH).
    fn per_dex() -> MarginStates {
        per_dex_with_main_cash(-12_000_000_000)
    }

    /// `per_dex` with HOLDER's main dex cash at `micro`.
    pub(crate) fn per_dex_with_main_cash(micro: i64) -> MarginStates {
        states(|checkpoint| {
            checkpoint["exchange"]["locus"]["ust"][0][1].as_object_mut().unwrap().remove("a");
            checkpoint["exchange"]["locus"]["cls"][0]["user_states"]["user_to_state"][0][1]["u"] = json!(micro);
        })
    }

    fn fill(coin: &str, side: &str, px: &str, sz: &str, fee: &str, liquidation: Option<(&str, &str)>) -> AccountEvent {
        fill_from(coin, side, px, sz, fee, "0.0", liquidation)
    }

    /// A fill of the position `start` before it.
    fn fill_from(coin: &str, side: &str, px: &str, sz: &str, fee: &str, start: &str, liquidation: Option<(&str, &str)>) -> AccountEvent {
        let (dir, liquidation) = liquidation.map_or(("Open Long", json!(null)), |(method, dir)| {
            (dir, json!({"liquidatedUser": "0x00000000000000000000000000000000000000ee", "markPx": px, "method": method}))
        });
        let wire = json!({"coin": coin, "px": px, "sz": sz, "side": side, "time": 1, "startPosition": start, "dir": dir,
            "closedPnl": "0.0", "hash": "0x0", "oid": 1, "crossed": true, "fee": fee, "tid": 1, "feeToken": "USDC",
            "liquidation": liquidation});
        AccountEvent::Fill(FillEvent::of(&serde_json::from_value::<Fill>(wire).unwrap()).unwrap())
    }

    fn cross(states: &MarginStates, holder: &str) -> Pools {
        states.accounts[&user(holder)].pools.clone()
    }

    fn stop(states: &MarginStates, holder: &str) -> Option<StopReason> {
        states.accounts[&user(holder)].stop.as_ref().map(|stop| stop.reason.clone())
    }

    fn per_dex_pools(entries: &[(usize, i64)]) -> Pools {
        Pools::PerDex(entries.iter().copied().collect())
    }

    #[test]
    fn the_anchor_state_follows_the_account_mode() {
        let states = states(|_| {});
        // unified: spot USDC 200 + main -12000 + xyz 30.2697 (flx is USDH)
        assert_eq!(cross(&states, HOLDER), Pools::Unified(20_000_000_000 - 1_200_000_000_000 + 3_026_970_000));
        let isolated = &states.accounts[&user(ISOLATED)];
        assert_eq!(isolated.pools, per_dex_pools(&[(0, 0)]));
        assert_eq!(isolated.isolated, std::iter::once(((0, 1), 1_200_000_000_000)).collect());
        assert_eq!(isolated.isolated_leverage, [((0, 1), 3), ((0, 0), 5)].into_iter().collect(), "the PUMP position at 3x, BTC set isolated at 5x");
        assert_eq!(stop(&states, PORTFOLIO), Some(StopReason::PortfolioMargin));
        assert_eq!(per_dex().accounts[&user(HOLDER)].pools, per_dex_pools(&[(0, -1_200_000_000_000), (1, 3_026_970_000)]));
    }

    /// README 5: a fill moves −side × px × sz − fee; ADL is an ordinary fill, the other backstop fills stop.
    #[test]
    fn a_cross_fill_moves_its_dex_cash() {
        let mut states = per_dex();
        states.apply(5, user(HOLDER), &fill(PUMP_COIN, "B", "0.004477", "2000.0", "0.001", None));
        states.apply(6, user(HOLDER), &fill("BTC", "A", "82857.6", "0.001", "0.05", Some(("backstop", "Auto-Deleveraging"))));
        let main = -1_200_000_000_000 - 895_400_000 - 100_000 + 8_285_760_000 - 5_000_000;
        assert_eq!(cross(&states, HOLDER), per_dex_pools(&[(0, main), (1, 3_026_970_000)]));
        states.apply(7, user(HOLDER), &fill("BTC", "A", "82857.6", "0.001", "0.0", Some(("backstop", "Liquidated Cross Long"))));
        assert_eq!(states.accounts[&user(HOLDER)].stop, Some(Stop { block: Some(7), reason: StopReason::BackstopLiquidation }));
        states.apply(8, user(HOLDER), &fill(PUMP_COIN, "B", "0.004477", "1.0", "0.0", None));
        assert_eq!(cross(&states, HOLDER), per_dex_pools(&[(0, main), (1, 3_026_970_000)]), "a stopped account stays");
    }

    const PUMP_COIN: &str = "PUMP";

    fn isolated_cash(states: &MarginStates, holder: &str, key: (usize, usize)) -> Option<i64> {
        states.accounts[&user(holder)].isolated.get(&key).copied()
    }

    /// Research "逐仓成交的划转" (checkpoints 1192420000 -> 1192440000), in micro USDC: an open takes
    /// floor(|size| x px / leverage) from cross, a partial reduction moves the reduced share of the cash after the fee
    /// to cross (truncated toward zero), a flip closes and opens the rest without the fee, a close returns everything.
    #[test]
    fn isolated_fills_move_margin_between_cross_and_the_position() {
        let mut states = per_dex();
        // This test exercises the unconstrained formula; cash-constrained fills have separate real goldens.
        const FUNDS: i64 = 100_000_000_000_000;
        states.accounts.get_mut(&user(ISOLATED)).unwrap().add_cross(0, FUNDS);
        let btc = (0, 0);
        let main = |states: &MarginStates| match cross(states, ISOLATED) {
            Pools::PerDex(cross) => cross[&0] - FUNDS,
            Pools::Unified(_) => unreachable!("ISOLATED is a default account"),
        };
        // BTC set isolated at 5x: buy 0.01 at 82857.6, fee 0.37 -> notional 828.576, margin 165.7152 from cross
        states.apply(5, user(ISOLATED), &fill_from("BTC", "B", "82857.6", "0.01", "0.37", "0.0", None));
        assert_eq!(isolated_cash(&states, ISOLATED, btc), Some((-828_576_000 - 370_000 + 165_715_200) * 100));
        assert_eq!(main(&states), -165_715_200 * 100);
        assert!(states.accounts[&user(ISOLATED)].estimated, "an open is the rule short of exact");
        // sell 0.003 of 0.01 at 82860, fee 0.000001: moved = 300 x (-663230800 - 1) / 1000 = -198969240.3, toward zero
        states.apply(6, user(ISOLATED), &fill_from("BTC", "A", "82860.0", "0.003", "0.000001", "0.01", None));
        assert_eq!(isolated_cash(&states, ISOLATED, btc), Some((-663_230_801 + 198_969_240) * 100));
        assert_eq!(main(&states), (-165_715_200 + 248_579_999 - 198_969_239) * 100, "cross takes what the position does not");
        // flip: sell 0.01 of a 0.007 long at 82000, fee 0.5: the short 0.003 opens at 5x without the fee
        let before = main(&states) + isolated_cash(&states, ISOLATED, btc).unwrap();
        states.apply(7, user(ISOLATED), &fill_from("BTC", "A", "82000.0", "0.01", "0.5", "0.007", None));
        assert_eq!(isolated_cash(&states, ISOLATED, btc), Some((246_000_000 + 49_200_000) * 100));
        assert_eq!(main(&states) + isolated_cash(&states, ISOLATED, btc).unwrap(), before + (820_000_000 - 500_000) * 100, "the pool moves by the fill");
        // close the short: everything to cross
        let before = main(&states) + isolated_cash(&states, ISOLATED, btc).unwrap();
        states.apply(8, user(ISOLATED), &fill_from("BTC", "B", "81000.0", "0.003", "0.1", "-0.003", None));
        assert_eq!(isolated_cash(&states, ISOLATED, btc), None);
        assert_eq!(main(&states), before - (243_000_000 + 100_000) * 100);
        // the PUMP short (3x) grows by 1,000,000 at 0.004: margin floor(4000 / 3) = 1333.333333
        states.apply(9, user(ISOLATED), &fill_from(PUMP_COIN, "A", "0.004", "1000000.0", "0.0", "-2000000.0", None));
        assert_eq!(isolated_cash(&states, ISOLATED, (0, 1)), Some((12_000_000_000 + 4_000_000_000 + 1_333_333_333) * 100));
        assert_eq!(stop(&states, ISOLATED), None);
    }

    /// An isolated-only asset without a setting opens at its margin table's first max leverage (xyz:TSLA, table 10);
    /// a fill whose start contradicts the held cash stops the replay.
    #[test]
    fn an_isolated_open_without_a_setting_takes_the_tables_leverage() {
        let mut states = per_dex();
        for holder in [ISOLATED, HOLDER] { states.accounts.get_mut(&user(holder)).unwrap().add_cross(1, 100_000_000_000); }
        // ISOLATED has no xyz state: buy 2 TSLA at 358.32 -> notional 716.64, margin 71.664 at 10x
        states.apply(5, user(ISOLATED), &fill_from("xyz:TSLA", "B", "358.32", "2.0", "0.0", "0.0", None));
        assert_eq!(isolated_cash(&states, ISOLATED, (1, 0)), Some((-716_640_000 + 71_664_000) * 100));
        // HOLDER's setting is 3x: margin 238.88
        let pump_row = |states: &MarginStates| states.replayed_row(&user(HOLDER), PUMP_COIN, 1_000_000, 52, [], |_| None).unwrap();
        assert!(!pump_row(&states).cash_estimated);
        states.apply(5, user(HOLDER), &fill_from("xyz:TSLA", "B", "358.32", "2.0", "0.0", "0.0", None));
        assert_eq!(isolated_cash(&states, HOLDER, (1, 0)), Some((-716_640_000 + 238_880_000) * 100));
        // review 000466 r1 P2-1: the modelled transfer marks every row of the account, the cross ones included
        assert!(pump_row(&states).cash_estimated, "HOLDER's cross PUMP");
        assert!(states.replayed_row(&user(HOLDER), "xyz:TSLA", 2_000, 10, [], |_| None).unwrap().cash_estimated);
        // a fill "from 0" of an asset whose isolated cash is held
        states.apply(6, user(ISOLATED), &fill_from(PUMP_COIN, "B", "0.004", "1.0", "0.0", "0.0", None));
        assert!(matches!(stop(&states, ISOLATED), Some(StopReason::Contradiction(text)) if text.contains("isolated PUMP fill")));
    }

    #[test]
    fn distinct_isolated_market_modes_keep_the_same_fill_cash_routing() {
        let mut results = Vec::new();
        for mode in ["noCross", "strictIsolated"] {
            let mut replay = states(|value| {
                value["exchange"]["locus"]["cls"][1]["meta"]["universe"][0]["marginMode"] = json!(mode);
            });
            replay.accounts.get_mut(&user(ISOLATED)).unwrap().add_cross(1, 100_000_000_000);
            replay.apply(5, user(ISOLATED), &fill_from("xyz:TSLA", "B", "358.32", "2.0", "0.0", "0.0", None));
            assert_eq!(stop(&replay, ISOLATED), None, "{mode}");
            assert_eq!(isolated_cash(&replay, ISOLATED, (1, 0)), Some(-64_497_600_000));
            results.push(replay.accounts[&user(ISOLATED)].json());
        }
        assert_eq!(results[0], results[1], "preserve wire mode without changing existing fill transfers");
    }

    /// Research "新开仓位是全仓还是逐仓": the asset's margin mode, then the account's setting.
    #[test]
    fn other_collateral_and_unknown_markets_stop_the_replay() {
        let cases = [
            (HOLDER, "flx:GOLD", StopReason::NonUsdcCollateral),
            (HOLDER, "ZZZ", StopReason::UnknownMarket("ZZZ".to_string())),
        ];
        for (holder, coin, reason) in cases {
            let mut states = per_dex();
            states.apply(5, user(holder), &fill(coin, "B", "1.0", "1.0", "0.0", None));
            assert_eq!(stop(&states, holder), Some(reason), "{holder} {coin}");
        }
    }

    /// Spot USDC is in a unified account's pool only; pairs quoted otherwise and outcomes stop it.
    #[test]
    fn spot_fills_move_only_a_unified_pool() {
        let mut unified = states(|_| {});
        let before = 20_000_000_000 - 1_200_000_000_000 + 3_026_970_000;
        // @1 HYPE/USDC, base szDecimals 2: 1.5 HYPE at 80.123456 = 120.185184, fee 0.01
        unified.apply(5, user(HOLDER), &fill("@1", "B", "80.123456", "1.5", "0.01", None));
        assert_eq!(cross(&unified, HOLDER), Pools::Unified(before - 12_018_518_400 - 1_000_000));
        let mut per_dex = per_dex();
        per_dex.apply(5, user(HOLDER), &fill("@1", "B", "80.123456", "1.5", "0.01", None));
        assert_eq!(stop(&per_dex, HOLDER), None);
        assert_eq!(cross(&per_dex, HOLDER), per_dex_pools(&[(0, -1_200_000_000_000), (1, 3_026_970_000)]));
        per_dex.apply(6, user(HOLDER), &fill("@999", "B", "1.0", "1.0", "0.0", None));
        assert_eq!(stop(&per_dex, HOLDER), None, "a pair listed after the anchor is still spot");
        for (coin, reason) in [("@2", StopReason::NonUsdcSpotFill), ("#5", StopReason::OutcomeFill), ("@999", StopReason::UnknownMarket("@999".to_string()))] {
            let mut states = states(|_| {});
            states.apply(6, user(HOLDER), &fill(coin, "B", "1.0", "1.0", "0.0", None));
            assert_eq!(stop(&states, HOLDER), Some(reason), "{coin}");
        }
    }

    /// README "全仓与逐仓": funding of an isolated position settles into its `l.I.u`.
    #[test]
    fn funding_settles_into_isolated_or_cross_cash() {
        let mut states = per_dex();
        states.apply(5, user(ISOLATED), &AccountEvent::Funding { coin: PUMP_COIN.to_string(), amount_wei: -1_234 });
        states.apply(5, user(HOLDER), &AccountEvent::Funding { coin: "BTC".to_string(), amount_wei: 5_678 });
        assert_eq!(states.accounts[&user(ISOLATED)].isolated[&(0, 1)], 1_200_000_000_000 - 1_234);
        assert_eq!(cross(&states, HOLDER), per_dex_pools(&[(0, -1_200_000_000_000 + 5_678), (1, 3_026_970_000)]));
    }

    fn send(from: &str, source: Balance, to: &str, destination: Balance, usdc: Option<i64>, fee: i64) -> AccountEvent {
        AccountEvent::Ledger(LedgerDelta::Send {
            user: user(from),
            destination: user(to),
            source,
            destination_balance: destination,
            usdc_wei: usdc,
            usdc_fee_wei: fee,
        })
    }

    fn dust_goldens() -> Vec<Fill> {
        let golden: serde_json::Value = serde_json::from_str(include_str!("spot_dust_goldens.json")).unwrap();
        golden["cases"].as_array().unwrap().iter()
            .map(|row| serde_json::from_value(row["fill"].clone()).unwrap()).collect()
    }

    #[test]
    fn spot_dust_is_an_unsupported_cash_operation_not_a_lot_size_contradiction() {
        for wire in dust_goldens() {
            let mut replay = states(|_| {});
            let before = cross(&replay, HOLDER);
            let event = AccountEvent::Fill(FillEvent::of(&wire).unwrap());
            replay.apply(1_193_705_508, user(HOLDER), &event);
            let reason = stop(&replay, HOLDER).expect("unverified cash conversion must stop only this account");
            assert_eq!(reason.label(), "spotDustConversion");
            assert_eq!(reason.text(), "spotDustConversion:PURR/USDC");
            assert_eq!(cross(&replay, HOLDER), before, "no made-up px-times-size credit");
            assert_eq!(replay.accounts[&user(HOLDER)].stop.as_ref().unwrap().block, Some(1_193_705_508));
            assert_eq!(stop(&replay, ISOLATED), None, "unaffected accounts continue");
            replay.apply(1_193_705_509, user(HOLDER), &fill("BTC", "A", "82857.6", "0.001", "0.0", None));
            assert_eq!(cross(&replay, HOLDER), before, "stopped replay stays withdrawn until a fresh anchor");
            assert_eq!(stop(&states(|_| {}), HOLDER), None, "a fresh anchor restores replay");
        }
    }

    #[test]
    fn complete_dust_batch_credits_the_measured_unified_pool() {
        use crate::types::node_data::NodeDataFill;
        let (pair, fills, expected) = dust::tests::goldens().remove(0);
        let block = dust::DustBlock::of(fills.iter());
        for ((recipient, expected), NodeDataFill(_, wire)) in expected.iter().zip(&fills[1..]) {
            let mut replay = states(|_| {});
            replay.markets.spot_pairs[0] = pair.clone();
            let Pools::Unified(before) = cross(&replay, HOLDER) else { unreachable!() };
            let mut event = FillEvent::of(wire).unwrap();
            event.attach_dust(block.share(*recipient, wire));
            // Map each real recipient into the same isolated unit-test account fixture.
            replay.apply(1_193_705_508, user(HOLDER), &AccountEvent::Fill(event));
            assert_eq!(stop(&replay, HOLDER), None);
            assert_eq!(cross(&replay, HOLDER), Pools::Unified(before + expected));
        }
    }

    #[test]
    fn spot_dust_keeps_standard_accounts_and_regular_lot_checks_unchanged() {
        let mut wire = dust_goldens().remove(0);
        let mut standard = per_dex();
        let before = cross(&standard, HOLDER);
        standard.apply(5, user(HOLDER), &AccountEvent::Fill(FillEvent::of(&wire).unwrap()));
        assert_eq!(stop(&standard, HOLDER), None, "spot is outside standard perp cash pools");
        assert_eq!(cross(&standard, HOLDER), before);
        wire.dir = "Sell".to_string();
        let mut replay = states(|_| {});
        replay.apply(5, user(HOLDER), &AccountEvent::Fill(FillEvent::of(&wire).unwrap()));
        assert!(matches!(stop(&replay, HOLDER), Some(StopReason::Contradiction(_))), "ordinary fills still obey order lot precision");
        wire.dir = "Spot Dust Conversion".to_string();
        wire.fee = "not-a-number".to_string();
        assert!(FillEvent::of(&wire).is_err(), "known business kind does not bypass fee parsing");
    }

    /// README 5 / r3 revision 3: each party's side of a transfer, by its role in the delta.
    #[test]
    fn ledger_deltas_move_each_partys_side() {
        let main = || Balance::Dex(String::new());
        let mut standard = per_dex();
        standard.apply(5, user(HOLDER), &send(HOLDER, main(), ISOLATED, Balance::Dex("xyz".to_string()), Some(100), 0));
        standard.apply(5, user(ISOLATED), &send(HOLDER, main(), ISOLATED, Balance::Dex("xyz".to_string()), Some(100), 7));
        standard.apply(6, user(HOLDER), &send(HOLDER, Balance::Spot, HOLDER, main(), Some(50), 0));
        standard.apply(7, user(HOLDER), &send(HOLDER, main(), PORTFOLIO, main(), None, 0));
        let class = AccountEvent::Ledger(LedgerDelta::ClassTransfer { usdc_wei: 1_000, to_perp: false });
        standard.apply(8, user(HOLDER), &class);
        let vault = AccountEvent::Ledger(LedgerDelta::VaultDeposit { vault: user(ISOLATED), usdc_wei: 30 });
        standard.apply(9, user(HOLDER), &vault);
        standard.apply(9, user(ISOLATED), &vault);
        standard.apply(10, user(HOLDER), &AccountEvent::Action(BlockAction::Withdraw { amount_wei: 2_000 }));
        // vault 0x1e37a337… at block 1189214090: requested 1476.092235 = net 1475.272445 + commission 0.81979
        let withdraw = AccountEvent::Ledger(LedgerDelta::VaultWithdraw {
            user: user(HOLDER),
            vault: user(ISOLATED),
            commission_wei: 81_979_000,
            net_withdrawn_wei: 147_527_244_500,
        });
        standard.apply(11, user(HOLDER), &withdraw);
        standard.apply(11, user(ISOLATED), &withdraw);
        let main_cash = -1_200_000_000_000 - 100 + 50 - 1_000 - 30 - 2_000 + 147_527_244_500;
        assert_eq!(cross(&standard, HOLDER), per_dex_pools(&[(0, main_cash), (1, 3_026_970_000)]), "the user gets the net amount");
        let vault_cash = 30 - 147_609_223_500;
        assert_eq!(cross(&standard, ISOLATED), per_dex_pools(&[(0, vault_cash), (1, 100)]), "the vault pays net plus commission");
        let mut unified = states(|_| {});
        let before = cross(&unified, HOLDER);
        unified.apply(5, user(HOLDER), &class);
        assert_eq!(cross(&unified, HOLDER), before, "between main and spot inside one pool");
        unified.apply(6, user(HOLDER), &AccountEvent::Ledger(LedgerDelta::RewardsClaim { usdc_wei: Some(40) }));
        let Pools::Unified(before) = before else { panic!() };
        assert_eq!(cross(&unified, HOLDER), Pools::Unified(before + 40));
    }

    #[test]
    fn accepted_withdrawal_preserves_unified_dust_and_finalization_does_not_debit_again() {
        use crate::accounts::actions::parse_block_actions;
        let golden: serde_json::Value = serde_json::from_str(include_str!("withdrawal_goldens.json")).unwrap();
        let request = &golden["request"];
        let holder: Address = request["receipt"]["user"].as_str().unwrap().parse().unwrap();
        let bundle = request["bundle"].as_str().unwrap();
        let line = json!({"height":request["height"], "abci_block": {"time":request["time"],
            "signed_action_bundles":[[bundle,{"signed_actions":[{"action":request["action"]}]}]]},
            "resps":{"Full":[[bundle,[request["receipt"]]]]}}).to_string();
        let actions = parse_block_actions(&line).unwrap();
        assert_eq!(actions.actions, [(BlockAction::Withdraw { amount_wei: 6_214_531_400 }, holder)]);
        let mut replay = states(|_| {});
        let mut account = replay.accounts.remove(&user(HOLDER)).unwrap();
        account.pools = Pools::Unified(7_245_993_914);
        let isolated_before = account.isolated.clone();
        replay.accounts.insert(holder, account);
        replay.apply(actions.height, holder, &AccountEvent::Action(actions.actions[0].0.clone()));
        assert_eq!(replay.accounts[&holder].pools, Pools::Unified(1_031_462_514));
        replay.apply(1_191_733_582, holder, &AccountEvent::Ledger(LedgerDelta::Withdraw));
        assert_eq!(replay.accounts[&holder].pools, Pools::Unified(1_031_462_514), "finalization is already debited");
        assert_eq!(replay.accounts[&holder].isolated, isolated_before);
        assert!(replay.accounts[&holder].stop.is_none());
        let failed = line.replace("\"status\":\"ok\"", "\"status\":\"err\"");
        assert!(parse_block_actions(&failed).unwrap().actions.is_empty(), "failed withdrawals never debit");
    }

    /// Real H1191755982: sending 50 with fee 1 leaves the sender at 0 after a 50 deposit;
    /// the unified receiver gains 49, matching both checkpoint endpoints after its other ledger events.
    #[test]
    fn internal_transfer_gross_amount_matches_both_checkpoint_balances() {
        use crate::accounts::misc::MiscEvent;
        let golden: serde_json::Value = serde_json::from_str(include_str!("internal_transfer_golden.json")).unwrap();
        let receiver: Address = golden["receiver"].as_str().unwrap().parse().unwrap();
        let sender: Address = golden["sender"].as_str().unwrap().parse().unwrap();
        let mut replay = per_dex();
        let mut receiving = states(|_| {}).accounts.remove(&user(HOLDER)).unwrap();
        receiving.pools = Pools::Unified(golden["receiver_before_wei"].as_i64().unwrap());
        replay.accounts.insert(receiver, receiving);
        let mut sending = replay.accounts.remove(&user(HOLDER)).unwrap();
        sending.pools = per_dex_pools(&[(0, golden["sender_deposit_wei"].as_i64().unwrap())]);
        replay.accounts.insert(sender, sending);
        for row in golden["rows"].as_array().unwrap() {
            let MiscEvent::Ledger { users, delta } = serde_json::from_value(row["event"].clone()).unwrap() else { panic!("ledger expected") };
            for affected in users {
                replay.apply(row["height"].as_u64().unwrap(), affected, &AccountEvent::Ledger(delta.clone()));
            }
        }
        assert_eq!(replay.accounts[&receiver].pools, Pools::Unified(golden["receiver_after_wei"].as_i64().unwrap()));
        assert_eq!(replay.accounts[&sender].pools, per_dex_pools(&[(0, golden["sender_after_wei"].as_i64().unwrap())]));
        assert!(replay.accounts[&receiver].stop.is_none());
        assert!(replay.accounts[&sender].stop.is_none());
    }

    #[test]
    fn internal_transfer_zero_fee_and_self_transfer_charge_once() {
        for fee_wei in [0, 100] {
            let mut replay = per_dex_with_main_cash(1_000);
            let event = AccountEvent::Ledger(LedgerDelta::InternalTransfer {
                user: user(HOLDER), destination: user(HOLDER), usdc_wei: 10_000, fee_wei,
            });
            replay.apply(5, user(HOLDER), &event);
            assert_eq!(cross(&replay, HOLDER), per_dex_pools(&[(0, 100_000 - fee_wei), (1, 3_026_970_000)]));
            assert_eq!(stop(&replay, HOLDER), None);
        }
    }

    #[test]
    fn request_weight_purchase_debits_unified_cash_without_touching_isolated_margin() {
        let mut replay = states(|_| {});
        let before = replay.accounts[&user(HOLDER)].clone();
        replay.apply(5, user(HOLDER), &AccountEvent::Action(BlockAction::ReserveRequestWeight { cost_wei: 50_000 }));
        let Pools::Unified(cash) = before.pools else { panic!("unified fixture") };
        assert_eq!(cross(&replay, HOLDER), Pools::Unified(cash - 50_000));
        assert_eq!(replay.accounts[&user(HOLDER)].isolated, before.isolated);
        assert_eq!(stop(&replay, HOLDER), None);
    }

    #[test]
    fn request_weight_purchase_matches_payer_and_recipient_checkpoint_cash() {
        use crate::accounts::actions::{parse_block_actions, tests::action_line};
        let golden: serde_json::Value = serde_json::from_str(include_str!("request_weight_golden.json")).unwrap();
        let block = parse_block_actions(&action_line(&golden)).unwrap();
        let payer = golden["receipt"]["user"].as_str().unwrap();
        let recipient = golden["action"]["destination"].as_str().unwrap();
        let wei = |key: &str| golden[key].as_i64().unwrap() * 100;
        let mut replay = per_dex();
        let mut paying = replay.accounts.remove(&user(HOLDER)).unwrap();
        paying.pools = per_dex_pools(&[(0, wei("payer_before_main_micro")), (1, wei("payer_before_xyz_micro"))]);
        let mut receiving = paying.clone();
        receiving.pools = per_dex_pools(&[(0, wei("destination_before_main_micro"))]);
        replay.accounts.insert(user(payer), paying);
        replay.accounts.insert(user(recipient), receiving);
        replay.apply(1_191_755_163, user(payer), &AccountEvent::Funding { coin:"BTC".to_string(), amount_wei:wei("payer_funding_micro") });
        replay.apply(1_191_755_163, user(recipient), &AccountEvent::Funding { coin:"BTC".to_string(), amount_wei:wei("destination_funding_micro") });
        // The fixture uses BTC for these real ETH cross-funding payments; only the main cash pool is under test.
        replay.apply(1_191_758_371, user(payer), &send(payer, Balance::Dex("xyz".to_string()), payer,
            Balance::Dex(String::new()), Some(5_000_000_000), 0));
        for (action, affected) in block.actions {
            replay.apply(block.height, affected, &AccountEvent::Action(action));
        }
        assert_eq!(cross(&replay, payer), per_dex_pools(&[(0, wei("payer_after_main_micro")), (1, wei("payer_after_xyz_micro"))]));
        assert_eq!(cross(&replay, recipient), per_dex_pools(&[(0, wei("destination_after_main_micro"))]));
        assert_eq!(stop(&replay, payer), None);
        assert_eq!(stop(&replay, recipient), None);
    }

    #[test]
    fn vault_distribution_ledger_legs_match_fourteen_checkpoint_cash_balances() {
        use crate::accounts::{actions::{parse_block_actions, tests::action_line}, misc::MiscEvent};
        let golden: serde_json::Value = serde_json::from_str(include_str!("vault_distribution_golden.json")).unwrap();
        assert_eq!(parse_block_actions(&action_line(&golden)).unwrap().actions, [], "ledger owns vaultDistribute cash");
        let mut replay = per_dex();
        let template = replay.accounts.remove(&user(HOLDER)).unwrap();
        for row in golden["accounts"].as_array().unwrap() {
            let mut account = template.clone();
            let cash = row["main_and_unified_spot_before_wei"].as_i64().unwrap();
            account.pools = if row["unified"].as_bool().unwrap() { Pools::Unified(cash) } else { per_dex_pools(&[(0, cash)]) };
            replay.accounts.insert(user(row["user"].as_str().unwrap()), account);
        }
        for row in golden["rows"].as_array().unwrap() {
            let MiscEvent::Ledger { users, delta } = serde_json::from_value(row["event"].clone()).unwrap() else { panic!("ledger expected") };
            assert_eq!(delta.delta_type(), "vaultDistribution");
            for affected in users { replay.apply(1_191_750_497, affected, &AccountEvent::Ledger(delta.clone())); }
        }
        // Apply the other observed cash events; BTC stands for the four cross-funded main-dex positions.
        for row in golden["accounts"].as_array().unwrap() {
            let holder = row["user"].as_str().unwrap();
            replay.apply(1_191_751_582, user(holder), &AccountEvent::Action(BlockAction::Withdraw {
                amount_wei: row["withdrawal_wei"].as_i64().unwrap(),
            }));
            replay.apply(1_191_755_163, user(holder), &AccountEvent::Funding {
                coin: "BTC".to_string(), amount_wei: row["funding_wei"].as_i64().unwrap(),
            });
            let cash = row["main_and_unified_spot_after_wei"].as_i64().unwrap();
            let expected = if row["unified"].as_bool().unwrap() { Pools::Unified(cash) } else { per_dex_pools(&[(0, cash)]) };
            assert_eq!(cross(&replay, holder), expected, "{holder}");
            assert_eq!(stop(&replay, holder), None, "{holder}");
        }
    }

    #[test]
    fn real_perp_to_perp_send_fee_can_debit_spot_not_the_source_dex() {
        use crate::accounts::misc::MiscEvent;
        let g: serde_json::Value = serde_json::from_str(include_str!("send_fee_source_golden.json")).unwrap();
        let payer: Address = g["user"].as_str().unwrap().parse().unwrap();
        let mut standard = per_dex();
        let mut source = standard.accounts.remove(&user(HOLDER)).unwrap();
        source.pools = per_dex_pools(&[(0, g["main_before_wei"].as_i64().unwrap())]);
        standard.accounts.insert(payer, source);
        let mut unified = states(|_| {});
        let mut source = unified.accounts.remove(&user(HOLDER)).unwrap();
        source.pools = Pools::Unified(g["main_before_wei"].as_i64().unwrap() + g["spot_before_wei"].as_i64().unwrap());
        unified.accounts.insert(payer, source);
        let mut fees = 0;
        let mut no_fee_flow = 0;
        for row in g["rows"].as_array().unwrap() {
            let MiscEvent::Ledger { delta, .. } = serde_json::from_value(row["event"].clone()).unwrap() else { panic!("ledger expected") };
            match &delta {
                LedgerDelta::Send { usdc_wei: Some(amount), usdc_fee_wei, .. } => { no_fee_flow -= amount; fees += usdc_fee_wei; },
                LedgerDelta::InternalTransfer { usdc_wei, fee_wei, .. } => no_fee_flow += usdc_wei - fee_wei,
                _ => panic!("fixture changed"),
            }
            let height = row["height"].as_u64().unwrap();
            standard.apply(height, payer, &AccountEvent::Ledger(delta.clone()));
            unified.apply(height, payer, &AccountEvent::Ledger(delta));
        }
        assert_eq!(g["main_before_wei"].as_i64().unwrap() + no_fee_flow, g["main_after_wei"].as_i64().unwrap());
        assert_eq!(g["spot_before_wei"].as_i64().unwrap() - fees, g["spot_after_wei"].as_i64().unwrap());
        assert_eq!(stop(&standard, g["user"].as_str().unwrap()), Some(StopReason::SpotFeeSource), "sourceDex identifies principal, not the fee funding pool");
        assert_eq!(unified.accounts[&payer].stop, None);
        assert_eq!(unified.accounts[&payer].pools, Pools::Unified(g["main_after_wei"].as_i64().unwrap() + g["spot_after_wei"].as_i64().unwrap()));
    }

    #[test]
    fn unverified_ledger_cases_stop_the_replay() {
        let spot_transfer = |usdc_wei| {
            AccountEvent::Ledger(LedgerDelta::SpotTransfer { user: user(HOLDER), destination: user(ISOLATED), usdc_wei, usdc_fee_wei: 100 })
        };
        let cases = [
            (AccountEvent::Ledger(LedgerDelta::BorrowLend), Some(StopReason::BorrowLend), None),
            (spot_transfer(None), Some(StopReason::NonUsdcSpotTransferFee), Some(StopReason::SpotFeeSource)),
            (spot_transfer(Some(27_800_000_000)), None, Some(StopReason::SpotFeeSource)),
            (send(HOLDER, Balance::Spot, ISOLATED, Balance::Spot, None, 100), None, Some(StopReason::SpotFeeSource)),
            (send(HOLDER, Balance::Dex(String::new()), ISOLATED, Balance::Spot, Some(100), 7), None, Some(StopReason::SpotFeeSource)),
            (send(HOLDER, Balance::Dex(String::new()), ISOLATED, Balance::Dex(String::new()), Some(100), 7), None, Some(StopReason::SpotFeeSource)),
            (AccountEvent::Ledger(LedgerDelta::Unread("liquidation".to_string())), Some(StopReason::UnreadLedger("liquidation".to_string())), Some(StopReason::UnreadLedger("liquidation".to_string()))),
        ];
        for (event, unified_stop, per_dex_stop) in cases {
            let mut unified = states(|_| {});
            unified.apply(5, user(HOLDER), &event);
            assert_eq!(stop(&unified, HOLDER), unified_stop, "{event:?}");
            let mut standard = per_dex();
            standard.apply(5, user(HOLDER), &event);
            assert_eq!(stop(&standard, HOLDER), per_dex_stop, "spot is outside a default account's pools: {event:?}");
        }
    }

    /// README "全仓与逐仓": `ntli` moves between the dex's cross cash and the isolated cash; a setting decides a new
    /// position's pool; a mode switch or an unseen action stops the replay.
    #[test]
    fn block_actions_move_margin_and_settings() {
        let mut states = per_dex();
        states.apply(5, user(ISOLATED), &AccountEvent::Action(BlockAction::IsolatedMargin { asset: 1, ntli_micro: -2 }));
        assert_eq!(states.accounts[&user(ISOLATED)].isolated[&(0, 1)], 1_200_000_000_000 - 200);
        assert_eq!(cross(&states, ISOLATED), per_dex_pools(&[(0, 200)]));
        // BTC set back to cross: a BTC fill is cross again
        states.apply(6, user(ISOLATED), &AccountEvent::Action(BlockAction::Leverage { asset: 0, is_cross: true, leverage: 20 }));
        states.apply(7, user(ISOLATED), &fill("BTC", "A", "82857.6", "0.001", "0.0", None));
        assert_eq!(cross(&states, ISOLATED), per_dex_pools(&[(0, 200 + 8_285_760_000)]));
        // review 000435 r1 P2: BTC switched in the fill's block, the fill may precede the switch
        let mut same_block = per_dex();
        same_block.apply(6, user(ISOLATED), &AccountEvent::Action(BlockAction::Leverage { asset: 0, is_cross: true, leverage: 20 }));
        same_block.apply(6, user(ISOLATED), &fill("BTC", "A", "82857.6", "0.001", "0.0", None));
        assert_eq!(stop(&same_block, ISOLATED), Some(StopReason::IsolatedFill));
        // a new isolated leverage in the fill's block: the fill may precede it
        let mut releveraged = per_dex();
        releveraged.apply(6, user(ISOLATED), &AccountEvent::Action(BlockAction::Leverage { asset: 1, is_cross: false, leverage: 2 }));
        releveraged.apply(6, user(ISOLATED), &fill_from(PUMP_COIN, "B", "0.004", "1.0", "0.0", "-2000000.0", None));
        assert_eq!(stop(&releveraged, ISOLATED), Some(StopReason::IsolatedFill));
        let mut same_leverage = per_dex();
        same_leverage.apply(6, user(ISOLATED), &AccountEvent::Action(BlockAction::Leverage { asset: 1, is_cross: false, leverage: 3 }));
        same_leverage.apply(6, user(ISOLATED), &fill_from(PUMP_COIN, "B", "0.004", "1.0", "0.0", "-2000000.0", None));
        assert_eq!(stop(&same_leverage, ISOLATED), None, "an unchanged setting is no change");
        // PUMP set isolated at 4x a block before HOLDER's fill (HOLDER's PUMP is a cross position in the fixture, but its
        // replay reads only the setting; the fill opens the isolated cash at 4x)
        states.apply(8, user(HOLDER), &AccountEvent::Action(BlockAction::Leverage { asset: 1, is_cross: false, leverage: 4 }));
        states.apply(9, user(HOLDER), &fill(PUMP_COIN, "B", "0.004477", "1.0", "0.0", None));
        assert_eq!((stop(&states, HOLDER), isolated_cash(&states, HOLDER, (0, 1))), (None, Some((-4_477 + 1_119) * 100)), "PUMP set isolated before the fill");
        let cases = [
            (BlockAction::IsolatedMargin { asset: 0, ntli_micro: 1 }, "without an isolated position"),
            (BlockAction::SetAbstraction, "accountModeSwitch"),
            (BlockAction::TopUpIsolatedOnlyMargin { asset: 1, target_leverage: "0.75".into() }, "topUpIsolatedOnlyMargin"),
            (BlockAction::Leverage { asset: 77, is_cross: true, leverage: 20 }, "action asset 77"),
        ];
        for (action, expected) in cases {
            let mut states = per_dex();
            states.apply(5, user(HOLDER), &AccountEvent::Action(action.clone()));
            let reason = stop(&states, HOLDER).unwrap();
            assert!(format!("{reason:?} {}", reason.label()).contains(expected), "{action:?}: {reason:?}");
        }
        // xyz:TSLA is asset 0 of pdi 1: action id 110000
        let mut states = per_dex();
        states.apply(5, user(HOLDER), &AccountEvent::Action(BlockAction::Leverage { asset: 110_000, is_cross: false, leverage: 7 }));
        assert_eq!(stop(&states, HOLDER), None);
        assert_eq!(states.accounts[&user(HOLDER)].isolated_leverage.get(&(1, 0)), Some(&7));
    }

    /// Design 000437 1: a unified account's cross pool spans every USDC dex; a dex settled otherwise is a pool of its own.
    /// Design 000442: the pool's other members count at the marks given, all of them needed.
    #[test]
    fn a_unified_pool_counts_every_usdc_dex_and_no_other() {
        let unified = states(|_| {});
        let table = 20;
        let held = |coins: &[&'static str], btc_table| {
            let size_raw = |coin| if coin == "BTC" { 10_000 } else { 1_000_000 };
            coins.iter().map(move |coin| HeldPosition { coin, size_raw: size_raw(*coin), table_id: btc_table }).collect::<Vec<_>>()
        };
        let btc_mark = |coin: &str| (coin == "BTC").then_some(828_576);
        let row = |coins: &[&'static str], mark_of: &dyn Fn(&str) -> Option<u64>| {
            unified.replayed_row(&user(HOLDER), PUMP_COIN, 1_000_000, table, held(coins, Some(20)), mark_of)
        };
        let alone = row(&[PUMP_COIN, "flx:GOLD"], &|_| None).expect("flx settles in USDH");
        assert_eq!(row(&[PUMP_COIN, "BTC"], &|_| None), None, "BTC shares the pool, without a mark");
        assert_eq!(row(&[PUMP_COIN, "ZZZ"], &btc_mark), None, "an unknown coin may share it");
        let unknown_table = unified.replayed_row(&user(HOLDER), PUMP_COIN, 1_000_000, table, held(&["BTC"], None), btc_mark);
        assert_eq!(unknown_table, None, "BTC's table is unknown");
        assert_eq!(unified.replayed_row(&user(HOLDER), "flx:GOLD", 100, 10, held(&["flx:GOLD"], Some(10)), btc_mark), None, "not a USDC perp");
        let pool = (20_000_000_000_i64 - 1_200_000_000_000 + 3_026_970_000) as f64 / 1e8;
        let pump_table = MarginTable::single(table).unwrap();
        assert_eq!(alone, ReplayedRow { isolated: false, pool_positions: 1, liquidation_px: solve(pool, 0.0, 1_000_000.0, &pump_table), cash_estimated: false });
        let shared = row(&[PUMP_COIN, "BTC"], &btc_mark).expect("BTC has a mark");
        // BTC 0.1 at 82,857.6 held at 20x: value 8,285.76, maintenance 207.144
        let btc_table = MarginTable::single(20).unwrap();
        let btc = HeldAtMark { size_raw: 10_000, size_decimals: 5, mark_raw: 828_576, table: &btc_table };
        assert_eq!(shared, ReplayedRow { isolated: false, pool_positions: 2, liquidation_px: solve_held(pool, &[btc], 1_000_000.0, &pump_table), cash_estimated: false });
        let (LiquidationPx::Computed(price), LiquidationPx::Computed(by_hand)) =
            (shared.liquidation_px, solve(pool + 8_285.76, 207.144, 1_000_000.0, &pump_table))
        else {
            panic!("both computed")
        };
        assert!((price / by_hand - 1.0).abs() < 1e-12, "{price} vs {by_hand}");
    }

    #[test]
    fn the_check_compares_pools_and_isolated_cash_of_replayed_accounts() {
        let anchor = per_dex();
        let mut replayed = per_dex();
        let compared = |exact, moved, mismatched| Compared { exact, moved, mismatched, estimated: 0 };
        assert_eq!(replayed.check_against(&anchor, &anchor).compared["perDex"], compared(2, 0, 0), "HOLDER and ISOLATED");
        // review 000466 r1 P2-2: after an isolated open, a split between cross and isolated off the anchor is estimated
        // (a sample of its own); a pool total off it is a mismatch, as an unread deposit would be
        let open = fill_from("BTC", "B", "82857.6", "0.01", "0.0", "0.0", None);
        let mut opened = per_dex();
        opened.accounts.get_mut(&user(ISOLATED)).unwrap().add_cross(0, 100_000_000_000_000);
        opened.apply(5, user(ISOLATED), &open);
        let mut split = per_dex();
        split.accounts.get_mut(&user(ISOLATED)).unwrap().add_cross(0, 100_000_000_000_000);
        split.apply(5, user(ISOLATED), &open);
        let account = split.accounts.get_mut(&user(ISOLATED)).unwrap();
        *account.isolated.get_mut(&(0, 0)).unwrap() += 100_000_000;
        account.add_cross(0, -100_000_000);
        let check = opened.check_against(&split, &anchor);
        assert_eq!(check.compared["perDex"], Compared { exact: 1, moved: 0, mismatched: 0, estimated: 1 });
        assert_eq!((check.samples.len(), check.estimated_samples.len()), (0, 1));
        split.accounts.get_mut(&user(ISOLATED)).unwrap().add_cross(0, 10_000_000_000);
        let check = opened.check_against(&split, &anchor);
        assert_eq!(check.compared["perDex"], Compared { exact: 1, moved: 0, mismatched: 1, estimated: 0 });
        assert_eq!((check.samples[0].user, check.estimated_samples.len()), (user(ISOLATED), 0));
        replayed.apply(5, user(HOLDER), &AccountEvent::Funding { coin: "BTC".to_string(), amount_wei: 1 });
        replayed.apply(5, user(ISOLATED), &AccountEvent::Action(BlockAction::SetAbstraction));
        let check = replayed.check_against(&anchor, &anchor);
        assert_eq!(check.compared["perDex"], compared(0, 0, 1));
        assert_eq!((check.stopped["accountModeSwitch"], check.stopped["portfolioMargin"]), (1, 1));
        assert_eq!(check.samples[0].user, user(HOLDER));
        let evidence = check.samples[0].replayed.json();
        assert_eq!(evidence["pools"]["perDex"]["0"], "-11999.99999999");
        assert_eq!(states(|_| {}).check_against(&states(|_| {}), &states(|_| {})).compared["unified"], compared(1, 0, 0));
        let mut isolated_only = per_dex();
        isolated_only.apply(5, user(ISOLATED), &AccountEvent::Funding { coin: PUMP_COIN.to_string(), amount_wei: -1 });
        let check = isolated_only.check_against(&anchor, &anchor);
        assert_eq!((check.compared["perDex"], check.samples[0].user), (compared(1, 0, 1), user(ISOLATED)), "isolated cash alone differs");
        let moved_anchor = per_dex_with_main_cash(-12_000_000_000 + 1);
        let mut moved = per_dex();
        moved.apply(5, user(HOLDER), &AccountEvent::Funding { coin: "BTC".to_string(), amount_wei: 100 });
        assert_eq!(moved.check_against(&moved_anchor, &per_dex()).compared["perDex"], compared(2, 1, 0), "HOLDER moved +1 micro");
    }
    #[test]
    fn multisig_margin_reaches_cash_replay_once_with_equal_opposite_pool_changes() {
        use crate::accounts::actions::parse_block_actions;
        let cases: Vec<serde_json::Value> = serde_json::from_str(include_str!("actions_multisig_goldens.json")).unwrap();
        let mut case = cases[0].clone();
        // Keep the observed amount and wrapper, route to the fixture's isolated position.
        case["action"]["payload"]["action"]["asset"] = json!(1);
        case["receipt"]["user"] = json!(ISOLATED);
        let line = json!({"height": case["height"], "abci_block": {"time":case["time"],
            "signed_action_bundles":[[case["bundle"],{"signed_actions":[{"action":case["action"]}]}]]},
            "resps":{"Full":[[case["bundle"],[case["receipt"]]]]}}).to_string();
        let block = parse_block_actions(&line).unwrap();
        assert_eq!(block.actions.len(), 1);
        let mut states = per_dex();
        for (action, user) in block.actions {
            states.apply(block.height, user, &AccountEvent::Action(action));
        }
        assert_eq!(cross(&states, ISOLATED), per_dex_pools(&[(0, -6_729_717_000)]));
        assert_eq!(states.accounts[&user(ISOLATED)].isolated[&(0, 1)], 1_200_000_000_000 + 6_729_717_000);
        assert_eq!(stop(&states, ISOLATED), None);
    }

    #[test]
    fn funding_balance_uses_the_same_cross_unified_and_isolated_cash_routing() {
        let adjustment = |coin: &str| AccountEvent::FundingBalance { coin: coin.to_string(), amount_wei: -15100 };
        let mut standard = per_dex();
        standard.apply(5, user(HOLDER), &adjustment("BTC"));
        assert_eq!(cross(&standard, HOLDER), per_dex_pools(&[(0, -1_200_000_015_100), (1, 3_026_970_000)]));
        standard.apply(5, user(ISOLATED), &adjustment(PUMP_COIN));
        assert_eq!(standard.accounts[&user(ISOLATED)].isolated[&(0, 1)], 1_200_000_000_000 - 15100);
        assert_eq!(cross(&standard, ISOLATED), per_dex_pools(&[(0, 0)]));
        let mut unified = states(|_| {});
        let Pools::Unified(before) = cross(&unified, HOLDER) else { panic!() };
        unified.apply(5, user(HOLDER), &adjustment("xyz:TSLA"));
        assert_eq!(cross(&unified, HOLDER), Pools::Unified(before - 15100));
        unified.apply(6, user(HOLDER), &AccountEvent::FundingBalanceUnknown);
        assert_eq!(stop(&unified, HOLDER), Some(StopReason::FundingBalanceUnknown));
        assert_eq!(stop(&unified, ISOLATED), None, "only potential recipients stop");
    }


    #[test]
    fn recorded_isolated_opens_increases_and_flips_match_checkpoint_cash_at_the_recorded_mark() {
        let golden: serde_json::Value = serde_json::from_str(include_str!("replay/isolated_goldens.json")).unwrap();
        for sample in golden["samples"].as_array().unwrap() { check_isolated_cash_sample(sample); }
    }

    /// Large recordings remain outside Git. This checks the actual Rust cash replay against every checkpoint cash
    /// sample conditional on a matching recorded mark; it does not establish intra-block timing from polling.
    #[test]
    #[ignore = "requires ISOLATED_MARGIN_REPORT pointing to the archived full report"]
    fn full_archived_isolated_margin_corpus() {
        let path = std::env::var("ISOLATED_MARGIN_REPORT").expect("explicit archive path");
        let report: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let mut checked = 0;
        for sample in report["results"].as_array().unwrap() {
            let Some(mark) = sample["observations"].as_array().unwrap().iter().find(|mark| mark["exact"] == true) else { continue };
            check_isolated_cash_sample(&json!({"case": sample["case"], "mark_evidence": mark}));
            checked += 1;
        }
        assert_eq!(checked, 873, "the archived corpus must be complete");
        println!("{checked} checkpoint cash samples matched through the Rust isolated-fill implementation");
    }

    fn check_isolated_cash_sample(sample: &serde_json::Value) {
        let case = &sample["case"];
        let fill: Fill = serde_json::from_value(case["fill"].clone()).unwrap();
        let decimals = u32::try_from(case["size_decimals"].as_u64().unwrap()).unwrap();
        let mark = parse_size_raw(sample["mark_evidence"]["mark_px"].as_str().unwrap(), MAX_PERP_DECIMALS - decimals).unwrap().cast_unsigned();
        let event = FillEvent::at_mark(&fill, Some(FillMarkRange { min_px_raw: mark, max_px_raw: mark, size_decimals: decimals })).unwrap();
        let mut states = per_dex();
        let account = states.accounts.get_mut(&user(ISOLATED)).unwrap();
        account.isolated.clear();
        // This older corpus did not record cross funding. Its conditional regression isolates the requested
        // margin formula with explicitly ample funds; the new capacity goldens use recorded pool cash.
        account.pools = per_dex_pools(&[(0, 1_000_000_000_000_000_000)]);
        account.cross_exposure_seen.clear();
        if case["before"][0].as_i64().unwrap() != 0 {
            account.isolated.insert((0, 0), case["before"][1].as_i64().unwrap() * WEI_PER_MICRO);
        }
        let before_total = totals(account).get(&0).copied().unwrap_or(0);
        let market = PerpMarket { dex: 0, asset: 0, size_decimals: decimals, margin_mode: AssetMarginMode::NoCross, default_leverage: 1 };
        let cash_flow = event.cash_wei(decimals, MAX_PERP_DECIMALS - decimals).unwrap();
        let leverage = u32::try_from(case["before"][2].as_u64().unwrap()).unwrap();
        apply_isolated_fill(account, &market, &event, cash_flow, leverage).unwrap();
        assert_eq!(account.isolated[&(0, 0)], case["after"][1].as_i64().unwrap() * WEI_PER_MICRO, "{} {}", fill.coin, case["block"]);
        assert_eq!(totals(account)[&0], before_total + cash_flow, "a transfer conserves the pool total");
        assert!(!account.estimated, "{} {}", fill.coin, case["block"]);
    }

    #[test]
    fn hip3_execution_order_reconciles_isolated_cash_at_both_checkpoint_endpoints() {
        let golden: serde_json::Value = serde_json::from_str(include_str!("marks/fill_prices/hip3_cash_goldens.json")).unwrap();
        for case in golden["cases"].as_array().unwrap() { check_hip3_execution_cash(case, true); }
    }

    #[test]
    #[ignore = "requires HIP3_CASH_CORPUS pointing to the archived 42-position sample"]
    fn archived_hip3_execution_cash_corpus() {
        let path = std::env::var("HIP3_CASH_CORPUS").expect("explicit archive path");
        let golden: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let cases = golden["cases"].as_array().unwrap();
        assert_eq!(cases.len(), 42);
        for case in cases { check_hip3_execution_cash(case, false); }
    }

    fn check_hip3_execution_cash(case: &serde_json::Value, require_no_estimate: bool) {
        let decimals = u32::try_from(case["size_decimals"].as_u64().unwrap()).unwrap();
        let before = &case["checkpoint_before"];
        let after = &case["checkpoint_after"];
        let leverage = u32::try_from(before["l"]["I"]["l"].as_u64().unwrap()).unwrap();
        let mut states = per_dex();
        let account = states.accounts.get_mut(&user(ISOLATED)).unwrap();
        account.isolated.clear();
        if before["s"].as_i64().unwrap_or(0) != 0 {
            account.isolated.insert((0, 0), before["l"]["I"]["u"].as_i64().unwrap() * WEI_PER_MICRO);
        }
        account.pools = per_dex_pools(&[(0, 1_000_000_000_000_000_000)]);
        account.cross_exposure_seen.clear();
        let market = PerpMarket { dex: 0, asset: 0, size_decimals: decimals, margin_mode: AssetMarginMode::NoCross, default_leverage: leverage };
        let markets = Markets { perps: FxHashMap::from_iter([(case["coin"].as_str().unwrap().to_string(), market.clone())]),
            usdc_dexes: vec![true], ..Markets::default() };
        for event in case["events"].as_array().unwrap() {
            if let Some(funding) = event.get("funding") {
                apply_funding(account, &markets, funding["coin"].as_str().unwrap(),
                    usdc_wei(funding["funding_amount"].as_str().unwrap()).unwrap()).unwrap();
                continue;
            }
            let prices = if let Some(oracle) = event.get("oracle_locator") {
                crate::accounts::marks::fill_prices::oracle_golden_prices(oracle)
            } else if let Some(interleaved) = event.get("interleaved_statuses") {
                crate::accounts::marks::fill_prices::interleaved_golden_prices(interleaved)
            } else if event.get("triggered_status").is_some() {
                crate::accounts::marks::fill_prices::trigger_golden_prices(event)
            } else {
                crate::accounts::marks::fill_prices::golden_prices(case, event)
            };
            let fill: Fill = serde_json::from_value(event["fill"].clone()).unwrap();
            let event = FillEvent::at_mark(&fill, prices.get(event["height"].as_u64().unwrap(), &fill.coin, fill.tid)).unwrap();
            let flow = event.cash_wei(decimals, MAX_PERP_DECIMALS - decimals).unwrap();
            apply_isolated_fill(account, &market, &event, flow, leverage).unwrap();
        }
        assert_eq!(account.isolated[&(0, 0)], after["l"]["I"]["u"].as_i64().unwrap() * WEI_PER_MICRO, "{} {}", case["user"], case["coin"]);
        if require_no_estimate { assert!(!account.estimated, "uniquely ordered marks with ample funds are exact"); }
    }

    #[test]
    fn equality_trigger_marks_reconcile_tsla_open_and_meta_increase_cash() {
        let golden: serde_json::Value = serde_json::from_str(include_str!("marks/fill_prices/hip3_trigger_boundary_goldens.json")).unwrap();
        for case in golden["cases"].as_array().unwrap() { check_hip3_execution_cash(case, true); }
    }

    #[test]
    fn trailing_stop_marks_reconcile_mrna_and_crcl_cash_intervals() {
        let golden: serde_json::Value = serde_json::from_str(include_str!("marks/fill_prices/trailing_stop_cash_goldens.json")).unwrap();
        for case in golden["cases"].as_array().unwrap() { check_hip3_execution_cash(case, true); }
    }

    #[test]
    fn oracle_hash_locators_reconcile_full_dram_and_sndk_cash_intervals() {
        let golden: serde_json::Value = serde_json::from_str(include_str!("marks/fill_prices/hip3/oracle_hash_goldens.json")).unwrap();
        for case in golden["cases"].as_array().unwrap() { check_hip3_execution_cash(case, true); }
    }

    #[test]
    fn interleaved_stop_reconciles_the_full_dram_cash_interval() {
        let golden: serde_json::Value = serde_json::from_str(include_str!("marks/fill_prices/hip3/interleaved_golden.json")).unwrap();
        check_hip3_execution_cash(&golden["case"], true);
    }

    #[test]
    fn equality_take_profit_reconciles_the_full_anth_cash_interval() {
        let golden: serde_json::Value = serde_json::from_str(include_str!("marks/fill_prices/anth_trigger_cash_golden.json")).unwrap();
        check_hip3_execution_cash(&golden["case"], true);
    }

    #[test]
    fn liquidation_flip_and_later_reductions_reconcile_the_full_meta_cash_interval() {
        let golden: serde_json::Value = serde_json::from_str(include_str!("marks/fill_prices/liquidation_flip_cash_golden.json")).unwrap();
        for case in golden["cases"].as_array().unwrap() { check_hip3_execution_cash(case, true); }
    }

    #[test]
    fn recorded_liquidation_mark_reconciles_five_live_isolated_makers() {
        let golden: serde_json::Value = serde_json::from_str(include_str!("marks/fill_prices/liquidation_cash_goldens.json")).unwrap();
        let prices = crate::accounts::marks::fill_prices::golden_prices(&golden, &golden["event"]);
        let height = golden["event"]["height"].as_u64().unwrap();
        let decimals = u32::try_from(golden["size_decimals"].as_u64().unwrap()).unwrap();
        for case in golden["cases"].as_array().unwrap() {
            let leverage = u32::try_from(case["leverage"].as_u64().unwrap()).unwrap();
            let mut states = per_dex();
            let account = states.accounts.get_mut(&user(ISOLATED)).unwrap();
            account.isolated.clear();
            if let Some(cash) = case["before"]["l"]["I"]["u"].as_i64() {
                account.isolated.insert((0, 0), cash * WEI_PER_MICRO);
            }
            // The corpus verifies this isolated position, not other coin flows in the same cross pool.
            account.pools = per_dex_pools(&[(0, 1_000_000_000_000_000_000)]);
            account.cross_exposure_seen.clear();
            let market = PerpMarket { dex: 0, asset: 0, size_decimals: decimals,
                margin_mode: AssetMarginMode::NoCross, default_leverage: leverage };
            let mut fills = 0;
            for leg in golden["event"]["legs"].as_array().unwrap().iter().filter(|leg| leg[0] == case["user"]) {
                let fill: Fill = serde_json::from_value(leg[1].clone()).unwrap();
                let mut event = FillEvent::of(&fill).unwrap();
                assert!(event.restore_mark(height, &prices), "seed backfill must use the same evidence");
                let flow = event.cash_wei(decimals, MAX_PERP_DECIMALS - decimals).unwrap();
                let total = totals(account)[&0];
                apply_isolated_fill(account, &market, &event, flow, leverage).unwrap();
                assert_eq!(totals(account)[&0], total + flow);
                fills += 1;
            }
            assert!(fills > 0);
            assert_eq!(account.isolated[&(0, 0)], case["after"]["l"]["I"]["u"].as_i64().unwrap() * WEI_PER_MICRO, "{}", case["user"]);
            assert!(!account.estimated, "a proven liquidation mark with ample funding is exact");
        }
    }

    #[test]
    fn default_leverage_caps_high_tiers_without_changing_explicit_settings() {
        for (maximum, expected) in [(3, 3), (20, 20), (21, 20), (50, 20)] {
            let states = states(|checkpoint| {
                checkpoint["exchange"]["locus"]["cls"][0]["meta"]["marginTableIdToMarginTable"][1][1]
                    ["margin_tiers"][0]["max_leverage"] = json!(maximum);
            });
            assert_eq!(states.markets.perps["BTC"].default_leverage, expected);
            assert_eq!(states.accounts[&user(ISOLATED)].isolated_leverage[&(0, 0)], 5, "stored settings are not defaults");
        }
    }

    #[test]
    fn recorded_jpy_open_uses_twenty_default_and_preserves_explicit_fifty() {
        let golden: serde_json::Value = serde_json::from_str(include_str!("replay/isolated_default_leverage_golden.json")).unwrap();
        assert!(golden["before"]["position"].is_null(), "no prior position or leverage setting");
        let recorded_after = &golden["after"]["position"]["l"]["I"];
        assert_eq!(recorded_after["l"], 20);
        let mark = parse_size_raw(golden["execution_mark"]["previous"]["price"]["px"].as_str().unwrap(), 4).unwrap().cast_unsigned();
        assert!(golden["execution_mark"]["same_block_updates"].as_array().unwrap().is_empty());
        // The fixture maps the recorded dex-1 asset 24 to index 0. Fills, table, mark and cash remain recorded
        // values; unrelated fixture holdings deliberately keep the cross-capacity uncertainty visible.
        for explicit in [None, Some(50)] {
            let mut states = states(|checkpoint| {
                checkpoint["exchange"]["locus"]["ust"][0][1].as_object_mut().unwrap().remove("a");
                let dex = &mut checkpoint["exchange"]["locus"]["cls"][1];
                dex["meta"]["universe"] = json!([golden["market"]]);
                dex["meta"]["marginTableIdToMarginTable"] = json!([[50, golden["margin_table"]]]);
                dex["user_states"]["user_to_state"] = json!([[HOLDER, {"u": golden["before"]["cross_cash_micro"]}]]);
            });
            let holder = user(HOLDER);
            let account = states.accounts.get_mut(&holder).unwrap();
            account.cross_exposure_seen.insert(1); // Real JPY owner also holds cross NVDA and GOLD.
            if let Some(leverage) = explicit { account.isolated_leverage.insert((1, 0), leverage); }
            let before = totals(account);
            let mut flow = 0;
            for wire in golden["fills"].as_array().unwrap() {
                let fill: Fill = serde_json::from_value(wire.clone()).unwrap();
                let event = FillEvent::at_mark(&fill, Some(FillMarkRange { min_px_raw: mark, max_px_raw: mark, size_decimals: 2 })).unwrap();
                flow += event.cash_wei(2, 4).unwrap();
                states.apply(golden["height"].as_u64().unwrap(), holder, &AccountEvent::Fill(event));
            }
            let account = &states.accounts[&holder];
            let expected = if explicit.is_some() { &golden["observed_old_replay_cash_micro"] } else { &recorded_after["u"] };
            assert_eq!(account.isolated[&(1, 0)], expected.as_i64().unwrap() * WEI_PER_MICRO);
            assert_eq!(totals(account)[&1], before[&1] + flow, "margin moves conserve total cash");
            assert!(account.estimated, "cross free collateral is still unknown; this regression verifies the default");
            assert_eq!(stop(&states, HOLDER), None);
        }
    }

    #[test]
    fn exact_fills_do_not_clear_an_earlier_estimated_transfer() {
        let mut states = per_dex();
        states.accounts.get_mut(&user(ISOLATED)).unwrap().add_cross(0, 100_000_000_000_000);
        let event = fill_from("BTC", "B", "82857.6", "0.01", "0.0", "0.0", None);
        states.apply(5, user(ISOLATED), &event);
        let AccountEvent::Fill(mut next) = fill_from("BTC", "B", "82857.6", "0.01", "0.0", "0.01", None) else { unreachable!() };
        next.mark = Some(FillMarkRange { min_px_raw: 828_500, max_px_raw: 828_500, size_decimals: 5 });
        states.apply(6, user(ISOLATED), &AccountEvent::Fill(next));
        assert!(states.accounts[&user(ISOLATED)].estimated, "only a new checkpoint resets the earlier uncertainty");
    }


    #[test]
    fn recorded_cash_capacity_cases_match_micro_cash_and_spot_dust() {
        let golden: serde_json::Value = serde_json::from_str(include_str!("replay/isolated_capacity_goldens.json")).unwrap();
        assert_eq!(golden["cases"].as_array().unwrap().len(), 6);
        for case in golden["cases"].as_array().unwrap() {
            let mut states = per_dex();
            let mut account = states.accounts.remove(&user(ISOLATED)).unwrap();
            account.pools = Pools::Unified(case["before"]["cross_wei"].as_i64().unwrap());
            account.isolated.clear();
            account.cross_exposure_seen.clear();
            let mut leverage = FxHashMap::default();
            for (asset, position) in case["before"]["positions"].as_object().unwrap() {
                let key = (0, asset.parse::<usize>().unwrap());
                account.isolated.insert(key, position["cash_micro"].as_i64().unwrap() * WEI_PER_MICRO);
                leverage.insert(key, u32::try_from(position["leverage"].as_u64().unwrap()).unwrap());
            }
            for event in case["events"].as_array().unwrap() {
                if let Some(funding) = event.get("funding") {
                    let asset = golden["markets"][funding["coin"].as_str().unwrap()]["asset"].as_u64().unwrap() as usize;
                    *account.isolated.get_mut(&(0, asset)).unwrap() += funding["amount_micro"].as_i64().unwrap() * WEI_PER_MICRO;
                    continue;
                }
                let fill: Fill = serde_json::from_value(event["fill"].clone()).unwrap();
                let meta = &golden["markets"][&fill.coin];
                let asset = meta["asset"].as_u64().unwrap() as usize;
                let decimals = u32::try_from(meta["size_decimals"].as_u64().unwrap()).unwrap();
                let market = PerpMarket { dex: 0, asset, size_decimals: decimals, margin_mode: AssetMarginMode::NoCross, default_leverage: 1 };
                let event = FillEvent::of(&fill).unwrap(); // No fabricated execution mark in these samples.
                let cash_flow = event.cash_wei(decimals, MAX_PERP_DECIMALS - decimals).unwrap();
                let before_total = totals(&account)[&0];
                apply_isolated_fill(&mut account, &market, &event, cash_flow, leverage[&(0, asset)]).unwrap();
                assert_eq!(totals(&account)[&0], before_total + cash_flow);
            }
            assert_eq!(account.pools, Pools::Unified(case["after"]["cross_wei"].as_i64().unwrap()), "{}", case["user"]);
            let expected: FxHashMap<_, _> = case["after"]["positions"].as_object().unwrap().iter().map(|(asset, p)|
                ((0, asset.parse::<usize>().unwrap()), p["cash_micro"].as_i64().unwrap() * WEI_PER_MICRO)).collect();
            assert_eq!(account.isolated, expected, "{}", case["user"]);
            assert_eq!(account.estimated, case["expected_estimated"].as_bool().unwrap(), "{}", case["user"]);
        }
    }

    fn unified_capacity_fixture() -> (Account, Markets, serde_json::Value) {
        use crate::accounts::checkpoint::{DexAccount, Position};
        let golden: serde_json::Value = serde_json::from_str(include_str!("replay/unified_capacity_golden.json")).unwrap();
        let mut markets = per_dex().markets;
        markets.usdc_dexes = golden["dex_collateral_names"].as_array().unwrap().iter().map(|name| name.as_str().unwrap() == "USDC").collect();
        markets.asset_names.resize_with(markets.usdc_dexes.len(), Vec::new);
        markets.asset_names[0] = vec![String::new(); 234];
        for (asset, meta) in golden["markets"].as_object().unwrap() {
            let asset = asset.parse::<usize>().unwrap();
            let coin = meta["name"].as_str().unwrap();
            markets.asset_names[0][asset] = coin.into();
            markets.action_assets.insert(u32::try_from(asset).unwrap(), coin.into());
            markets.perps.insert(coin.into(), PerpMarket { dex: 0, asset,
                size_decimals: u32::try_from(meta["szDecimals"].as_u64().unwrap()).unwrap(),
                margin_mode: AssetMarginMode::Normal, default_leverage: 10 });
        }
        let dex_states = golden["before"].as_array().unwrap().iter().map(|dex| {
            let mut positions = Vec::new();
            let mut isolated_settings = Vec::new();
            for pair in dex["positions"].as_array().unwrap() {
                let asset = usize::try_from(pair[0].as_u64().unwrap()).unwrap() % 10_000;
                let raw = &pair[1];
                let size_raw = raw.get("s").map_or(0, |size| size.as_i64().unwrap());
                let margin = if let Some(iso) = raw["l"].get("I") {
                    let leverage = u32::try_from(iso["l"].as_u64().unwrap()).unwrap();
                    if size_raw == 0 { isolated_settings.push((asset, leverage)); continue; }
                    PositionMargin::Isolated { cash_micro: iso["u"].as_i64().unwrap(), leverage }
                } else {
                    if size_raw == 0 { continue; }
                    PositionMargin::Cross { leverage: u32::try_from(raw["l"]["C"].as_u64().unwrap()).unwrap() }
                };
                positions.push(Position { asset, size_raw, margin, table_id: raw.get("M").map_or(20, |v| u32::try_from(v.as_u64().unwrap()).unwrap()) });
            }
            DexAccount { dex: usize::try_from(dex["dex"].as_u64().unwrap()).unwrap(), cash_micro: dex["cash_micro"].as_i64().unwrap(), positions, isolated_settings }
        }).collect();
        (Account { mode: AccountMode::Unified, spot: vec![], lending: vec![], dex_states }, markets, golden)
    }

    #[test]
    fn unified_single_dex_capacity_closes_the_recorded_pump_xmr_cash_split() {
        let (source, markets, golden) = unified_capacity_fixture();
        let mut account = AccountMargin::of(&source, &markets);
        assert_eq!(account.cross_capacity.len(), 1);
        assert_eq!(account.cross_capacity[&0].holdings[0].coin, "XMR");
        let holder = user(golden["provenance"]["user"].as_str().unwrap());
        let fill: Fill = serde_json::from_value(golden["fill"].clone()).unwrap();
        let requested = MarginStates { markets: markets.clone(), accounts: [(holder, account.clone())].into_iter().collect() }
            .collateral_coins([crate::types::node_data::NodeDataFill(holder, fill.clone())].iter());
        assert_eq!(requested, ["XMR"]);
        let mut event = FillEvent::at_mark(&fill, Some(FillMarkRange { min_px_raw: 4_679, max_px_raw: 4_679, size_decimals: 0 })).unwrap();
        event.collateral_marks = Some(Arc::new([("XMR".into(), FillMarkRange { min_px_raw: 611_170, max_px_raw: 611_170, size_decimals: 3 })].into_iter().collect()));
        let before = totals(&account)[&0];
        let flow = event.cash_wei(0, 6).unwrap();
        apply_perp_fill(&mut account, &markets, 1_193_836_595, &event).unwrap();
        assert_eq!(account.isolated[&(0, 200)], 3_741_528_674 * WEI_PER_MICRO);
        assert_eq!(account.pools, Pools::Unified(1_035_566_448 * WEI_PER_MICRO));
        assert_eq!(totals(&account)[&0], before + flow);
        assert!(!account.estimated);
        for missing in [true, false] {
            let mut uncertain = AccountMargin::of(&source, &markets);
            let mut fill = event.clone();
            fill.collateral_marks = if missing { None } else { Some(Arc::new([("XMR".into(), FillMarkRange { min_px_raw: 611_170, max_px_raw: 611_171, size_decimals: 3 })].into_iter().collect())) };
            apply_perp_fill(&mut uncertain, &markets, 1_193_836_595, &fill).unwrap();
            assert!(uncertain.estimated, "missing or varying collateral prices cannot be declared exact");
            assert_eq!(uncertain.isolated[&(0, 200)] - account.isolated[&(0, 200)], 388_913 * WEI_PER_MICRO);
        }
    }

    #[test]
    fn unified_capacity_proof_excludes_foreign_funds_and_never_recovers_without_a_seed() {
        for variant in 0..4 {
            let (mut source, markets, _) = unified_capacity_fixture();
            match variant {
                0 => source.spot.push((USDC, 1)),
                1 => source.dex_states[1].cash_micro = 1,
                2 => source.dex_states[1].cash_micro = -1,
                _ => { let position = source.dex_states[0].positions[0].clone(); source.dex_states[1].positions.push(position); }
            }
            assert!(AccountMargin::of(&source, &markets).cross_capacity.is_empty());
        }
        let (source, markets, _) = unified_capacity_fixture();
        let seeded = AccountMargin::of(&source, &markets);
        let mut local = seeded.clone();
        local.add_cross(0, 100);
        assert!(local.cross_capacity.contains_key(&0), "local cash movement preserves the holding proof");
        for spot in [false, true] {
            let mut account = seeded.clone();
            if spot { account.add_spot(1); account.add_spot(-1); }
            else { account.add_cross(1, 0); }
            assert!(account.cross_capacity.is_empty(), "foreign exposure is invalid even if net cash movement is zero");
            account.add_cross(0, 1);
            assert!(account.cross_capacity.is_empty(), "only a checkpoint rebuild restores this proof");
        }
        let mut replay = MarginStates { markets, accounts: [(user(HOLDER), seeded)].into_iter().collect() };
        replay.apply(10, user(HOLDER), &AccountEvent::Action(BlockAction::Leverage { asset: 224, is_cross: true, leverage: 4 }));
        assert!(replay.accounts[&user(HOLDER)].cross_capacity.is_empty());
    }

    #[test]
    fn live_gigadev_static_cross_capacity_reconciles_nine_fills() {
        let golden: serde_json::Value = serde_json::from_str(include_str!("replay/cross_capacity_golden.json")).unwrap();
        let mut states = per_dex();
        let holder = user(HOLDER);
        let account = states.accounts.get_mut(&holder).unwrap();
        account.stop = None;
        account.estimated = false;
        account.pools = per_dex_pools(&[(0, golden["cross_before_micro"].as_i64().unwrap() * WEI_PER_MICRO)]);
        account.isolated.clear();
        account.isolated.insert((0, 0), golden["isolated_before"]["l"]["I"]["u"].as_i64().unwrap() * WEI_PER_MICRO);
        account.cross_capacity.clear();
        let holdings = golden["holdings"].as_array().unwrap().iter().map(|v| cross_capacity::Holding {
            coin: v["coin"].as_str().unwrap().into(), size_raw: v["position"]["s"].as_i64().unwrap(),
            leverage: v["position"].get("l").map_or(20, |l| u32::try_from(l["C"].as_u64().unwrap()).unwrap()),
            size_decimals: u32::try_from(v["size_decimals"].as_u64().unwrap()).unwrap(),
        }).collect();
        account.cross_capacity.insert(0, cross_capacity::CrossCapacitySnapshot { holdings });
        account.cross_exposure_seen.insert(0);
        let market = PerpMarket { dex: 0, asset: 0, size_decimals: 2, margin_mode: AssetMarginMode::NoCross, default_leverage: 1 };
        for e in golden["events"].as_array().unwrap() {
            let fill: Fill = serde_json::from_value(e["fill"].clone()).unwrap();
            let px = e["isolated_mark_raw"].as_u64().unwrap();
            let mut event = FillEvent::at_mark(&fill, Some(FillMarkRange { min_px_raw: px, max_px_raw: px, size_decimals: 2 })).unwrap();
            let prices = e["cross_prices"].as_object().unwrap().iter().map(|(coin, p)| {
                let px = p["px_raw"].as_u64().unwrap();
                (coin.clone(), FillMarkRange { min_px_raw: px, max_px_raw: px, size_decimals: u32::try_from(p["size_decimals"].as_u64().unwrap()).unwrap() })
            }).collect();
            event.collateral_marks = Some(Arc::new(prices));
            assert_eq!(account.cross_capacity[&0].available_micro(match account.pools { Pools::PerDex(ref cash) => cash[&0], _ => unreachable!() }, event.collateral_marks.as_ref().unwrap()).unwrap(),
                Some(e["capacity_micro"].as_i64().unwrap()));
            let flow = event.cash_wei(2, 4).unwrap();
            let before = totals(account)[&0];
            apply_isolated_fill(account, &market, &event, flow, 1).unwrap();
            assert_eq!(totals(account)[&0], before + flow);
            assert!(!account.estimated);
        }
        assert_eq!(account.pools, per_dex_pools(&[(0, golden["cross_after_micro"].as_i64().unwrap() * WEI_PER_MICRO)]));
        assert_eq!(account.isolated[&(0, 0)], golden["isolated_after"]["l"]["I"]["u"].as_i64().unwrap() * WEI_PER_MICRO);
    }

    #[test]
    fn collateral_requests_only_cover_isolated_fills_and_replay_refreshes_the_projection() {
        let mut replay = per_dex();
        let holder = user(HOLDER);
        // The fixture has BTC and PUMP cross holdings. A new isolated setting is synthetic for this wiring test.
        replay.accounts.get_mut(&holder).unwrap().isolated_leverage.insert((0, 0), 5);
        let raw: serde_json::Value = serde_json::from_str(include_str!("replay/cross_capacity_golden.json")).unwrap();
        let mut fill: Fill = serde_json::from_value(raw["events"][0]["fill"].clone()).unwrap();
        fill.coin = "BTC".into();
        let fills = [crate::types::node_data::NodeDataFill(holder, fill.clone()), crate::types::node_data::NodeDataFill(holder, fill.clone())];
        let mut coins = replay.collateral_coins(fills.iter());
        coins.sort();
        assert_eq!(coins, ["BTC", "PUMP"], "one requested price per coin, not per fill");
        replay.accounts.get_mut(&holder).unwrap().isolated_leverage.clear();
        assert!(replay.collateral_coins(fills.iter()).is_empty(), "ordinary cross fills need no collateral projection");

        let mut marks = crate::accounts::marks::tests::trusted_marks();
        let block_fill = crate::accounts::marks::execution::BlockFill::of(&fills[0]);
        let report = marks.apply_block(1_002, crate::accounts::marks::MarkInputs {
            fills: vec![block_fill], collateral_coins: vec!["BTC".into()], ..Default::default()
        });
        let mut event = FillEvent::of(&fill).unwrap();
        assert!(event.restore_mark(1_002, &report.fill_prices));
        assert!(event.collateral_marks.as_ref().unwrap().contains_key("BTC"));
        assert!(!event.restore_mark(1_002, &report.fill_prices), "identical seed replay does not churn the account");
        assert!(event.restore_mark(1_002, &Default::default()));
        assert!(event.collateral_marks.is_none(), "withheld mark state also withdraws collateral evidence");
    }

    #[test]
    fn cross_snapshot_proof_is_invalidated_by_cross_fills_or_any_leverage_action() {
        let holder = user(HOLDER);
        for action in [BlockAction::Leverage { asset: 0, is_cross: true, leverage: 20 },
                       BlockAction::Leverage { asset: 0, is_cross: false, leverage: 3 }] {
            let mut states = per_dex();
            assert!(!states.accounts[&holder].cross_capacity[&0].holdings.is_empty());
            states.apply(5, holder, &AccountEvent::Action(action));
            assert!(!states.accounts[&holder].cross_capacity.contains_key(&0));
        }
        let mut states = per_dex();
        states.apply(5, holder, &fill_from("BTC", "B", "82857.6", "0.01", "0.0", "0.1", None));
        assert!(!states.accounts[&holder].cross_capacity.contains_key(&0));
        assert!(per_dex().accounts[&holder].cross_capacity.contains_key(&0), "fresh anchor restores proof");
        let mut states = self::states(|_| {});
        assert!(states.accounts[&holder].cross_capacity.is_empty(), "per-dex native evidence does not prove unified execution routing");
        states.accounts.get_mut(&holder).unwrap().estimated = true;
        assert!(states.accounts[&holder].estimated, "new exact evidence never clears prior uncertainty");
    }

    #[test]
    fn capacity_uses_its_own_pool_and_cross_exposure_is_conservative_until_reseed() {
        let mut states = per_dex();
        let isolated = user(ISOLATED);
        let account = states.accounts.get_mut(&isolated).unwrap();
        account.pools = per_dex_pools(&[(0, 12_345), (1, 99_999)]);
        assert_eq!(account.isolated_capacity_micro(0), Some(123));
        account.cross_exposure_seen.insert(1);
        assert_eq!(account.isolated_capacity_micro(0), Some(123), "other dex's cross positions use another pool");
        assert_eq!(account.isolated_capacity_micro(1), None);
        account.pools = Pools::Unified(12_345);
        assert_eq!(account.isolated_capacity_micro(0), None, "unified collateral sees every USDC dex");
        account.cross_exposure_seen.clear();
        account.pools = Pools::Unified(-12_345);
        assert_eq!(account.isolated_capacity_micro(0), Some(0));
        // A newly opened cross position invalidates the cash-only proof, even if subsequently closed.
        account.isolated_leverage.remove(&(0, 0));
        states.apply(5, isolated, &fill_from("BTC", "B", "82857.6", "0.01", "0.0", "0.0", None));
        assert_eq!(states.accounts[&isolated].isolated_capacity_micro(0), None);
        states.apply(6, isolated, &fill_from("BTC", "A", "82857.6", "0.01", "0.0", "0.01", None));
        assert_eq!(states.accounts[&isolated].isolated_capacity_micro(0), None);
        assert_eq!(per_dex().accounts[&isolated].isolated_capacity_micro(0), Some(0));
        // The anchor already records HOLDER's cross positions; negative raw cash is not available margin.
        assert_eq!(per_dex().accounts[&user(HOLDER)].isolated_capacity_micro(0), None);
    }


    #[test]
    fn same_block_manual_margin_and_isolated_fill_are_estimated_without_stopping() {
        let holder = user(ISOLATED);
        let mut states = per_dex();
        let action = AccountEvent::Action(BlockAction::IsolatedMargin { asset: 1, ntli_micro: 1_974_298_274 });
        states.apply(5, holder, &action);
        assert!(!states.accounts[&holder].estimated, "an isolated transfer alone has exact amounts");
        states.apply(5, holder, &fill_from(PUMP_COIN, "B", "0.004", "950000.0", "0.0", "-2000000.0", None));
        assert!(states.accounts[&holder].estimated, "MRVL 1192678957: taking a share of newly added margin assumes action order");
        assert_eq!(stop(&states, ISOLATED), None);
        let mut later = per_dex();
        later.apply(5, holder, &action);
        later.apply(6, holder, &fill_from(PUMP_COIN, "B", "0.004", "950000.0", "0.0", "-2000000.0", None));
        assert!(!later.accounts[&holder].estimated, "separate blocks establish the order");
        let account = later.accounts.get_mut(&holder).unwrap();
        account.note_margin_update(7, 1);
        assert!(!account.margin_updated_in(7, 0), "another standard dex has separate cash");
        account.pools = Pools::Unified(0);
        assert!(account.margin_updated_in(7, 0), "unified capacity is shared");
    }

    #[test]
    fn target_leverage_gap_is_local_to_the_account_and_keeps_diagnostic_inputs() {
        let mut states = per_dex();
        let before = states.accounts[&user(ISOLATED)].json();
        let other = states.accounts[&user(HOLDER)].json();
        let action = BlockAction::TopUpIsolatedOnlyMargin { asset: 1, target_leverage: "0.7500".into() };
        states.apply(7, user(ISOLATED), &AccountEvent::Action(action));
        let stopped = states.accounts[&user(ISOLATED)].stop.as_ref().unwrap();
        assert_eq!(stopped.block, Some(7));
        assert_eq!(stopped.reason.label(), "topUpIsolatedOnlyMargin", "bounded metric label is unchanged");
        assert_eq!(stopped.reason.text(),
            "topUpIsolatedOnlyMargin:asset=1,targetLeverage=0.7500,executionStateUnverified");
        assert_eq!(states.accounts[&user(ISOLATED)].json(), before, "do not guess a transfer or change the leverage setting");
        assert!(states.replays(&user(HOLDER)));
        assert_eq!(states.accounts[&user(HOLDER)].json(), other);
    }

}
