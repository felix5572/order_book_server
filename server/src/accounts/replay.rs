//! Cash replay between anchors (design 000433 r2 3.1 / 3.3, r3 revision 3): the margin state of every account that
//! held a position at the anchor, moved block by block by its events, and checked against the next anchor.
//!
//! Only USDC pools replay: a default / disabled account's cross cash per USDC dex, a unified account's USDC pool (spot
//! USDC plus the cross cash of every USDC dex), and isolated cash per position. Each rule below is measured (research
//! `hl_account_state_verify_20261019` sections 5 and "全仓与逐仓之间的现金划分"); an event no measured rule covers stops
//! the account's replay with a reason, and the next anchor resumes it. Amounts are 10^-8 USDC (`usdc.rs`).

use std::collections::BTreeMap;

use alloy::primitives::Address;
use rustc_hash::{FxHashMap, FxHashSet};
use serde_json::json;

use super::{
    actions::BlockAction,
    liquidation::{HeldAtMark, LiquidationPx, solve, solve_held},
    margin::MarginTable,
    checkpoint::{
        Account, AccountMode, AssetMarginMode, CheckpointAccounts, MAX_PERP_DECIMALS, MAX_SPOT_DECIMALS, PDI_ASSET_STRIDE,
        PositionMargin, SPOT_PAIR_ZERO_COIN, SpotPair, base_size,
    },
    misc::{Balance, LedgerDelta, USDC_NAME},
    portfolio::USDC,
    size::format_size_raw,
    usdc::{WEI_DECIMALS, WEI_PER_MICRO, micro_to_wei, notional_wei, usdc_wei},
};
use crate::{order_book::Side, types::Fill};

/// Action asset ids on a dex other than the main one: `ACTION_ASSET_BASE + PDI_ASSET_STRIDE * pdi + index`.
const ACTION_ASSET_BASE: u32 = 100_000;
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
}

/// Where an event's money goes, resolved against the anchor's universe.
#[derive(Debug, Clone, Default)]
pub(crate) struct Markets {
    perps: FxHashMap<String, PerpMarket>,
    /// The perp of an action asset id.
    action_assets: FxHashMap<u32, String>,
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
            markets.usdc_dexes.push(checkpoint.collateral[&dex.collateral_token].name == USDC_NAME);
            markets.tables.push(dex.tables().clone());
            for (asset_index, asset) in dex.assets.iter().enumerate() {
                let action_id = u32::try_from(asset_index)
                    .ok()
                    .filter(|index| *index < PDI_ASSET_STRIDE)
                    .map(|index| if dex.pdi == 0 { index } else { ACTION_ASSET_BASE + PDI_ASSET_STRIDE * dex.pdi + index })
                    .ok_or_else(|| format!("dex {:?} asset {asset_index} has no action id", dex.name))?;
                markets.action_assets.insert(action_id, asset.name.clone());
                let market = PerpMarket {
                    dex: dex_index,
                    asset: asset_index,
                    size_decimals: asset.size_decimals,
                    margin_mode: asset.margin_mode.clone(),
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
    Ledger(LedgerDelta),
    Action(BlockAction),
}

/// What a fill moves: `−side × px × sz − fee` into the pool of `coin` (the notional is computed against the market,
/// whose decimals the strings carry).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FillEvent {
    coin: String,
    side: Side,
    px: String,
    sz: String,
    /// `fee` when `feeToken` is USDC, else 0 (it includes builder and deployer fees).
    usdc_fee_wei: i64,
    /// A backstop liquidation other than ADL.
    backstop: bool,
}

impl FillEvent {
    pub(crate) fn of(fill: &Fill) -> Result<Self, String> {
        let usdc_fee_wei = if fill.fee_token == USDC_NAME {
            usdc_wei(&fill.fee).map_err(|err| format!("{} tid {} fee: {err}", fill.coin, fill.tid))?
        } else {
            0
        };
        let backstop = fill.liquidation.as_ref().is_some_and(|liquidation| liquidation.method == BACKSTOP_METHOD)
            && fill.dir != ADL_DIR;
        Ok(Self { coin: fill.coin.clone(), side: fill.side, px: fill.px.clone(), sz: fill.sz.clone(), usdc_fee_wei, backstop })
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
    UnknownAccountMode(String),
    /// A fill of an isolated position: how much moves between cross and isolated is not known.
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
    /// A USDC fee on a transfer from or to spot by an account whose pools do not hold spot: the node takes it from either
    /// side (bm 2026-10-20: `0xb4dca8c9…` sent all 278 USDC of spot and the 0.001566 fee left the main dex;
    /// `0x04447ced…` sent 20,000 USDC from the main dex to spot and the 0.001605 fee did not), which spot outside the
    /// pools cannot tell.
    SpotFeeSource,
    /// A unified account's spot fill of a pair not quoted in USDC.
    NonUsdcSpotFill,
    /// A unified account's HIP-4 outcome fill (`#N`): its sizes are not measured.
    OutcomeFill,
    UnknownMarket(String),
    /// `topUpIsolatedOnlyMargin`: never seen.
    TopUpIsolatedOnlyMargin,
    /// The state contradicts an event or the event does not read (the text says which).
    Contradiction(String),
}

impl StopReason {
    /// The view's `replayStoppedReason`: the label, and what the reason names when it names one.
    pub(crate) fn text(&self) -> String {
        match self {
            Self::UnknownAccountMode(detail) | Self::UnreadLedger(detail) | Self::UnknownMarket(detail) | Self::Contradiction(detail) => {
                format!("{}:{detail}", self.label())
            }
            _ => self.label().to_string(),
        }
    }

    /// The metric label.
    pub(crate) const fn label(&self) -> &'static str {
        match self {
            Self::PortfolioMargin => "portfolioMargin",
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
            Self::OutcomeFill => "outcomeFill",
            Self::UnknownMarket(_) => "unknownMarket",
            Self::TopUpIsolatedOnlyMargin => "topUpIsolatedOnlyMargin",
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
    /// (dex, asset) on USDC dexes the account set isolated: a position opened there is isolated.
    isolated_settings: FxHashSet<(usize, usize)>,
    /// The assets whose setting switched between cross and isolated in a block, with that block: a fill of the same
    /// block may come before or after the switch (review 000435 r1 P2).
    switched: Option<(u64, Vec<(usize, usize)>)>,
    stop: Option<Stop>,
}

impl AccountMargin {
    fn of(account: &Account, markets: &Markets) -> Self {
        let stopped = |reason| Self {
            pools: Pools::Unified(0),
            isolated: FxHashMap::default(),
            isolated_settings: FxHashSet::default(),
            switched: None,
            stop: Some(Stop { block: None, reason }),
        };
        let unified = match &account.mode {
            AccountMode::Default | AccountMode::Disabled => false,
            AccountMode::Unified => true,
            AccountMode::PortfolioMargin => return stopped(StopReason::PortfolioMargin),
            AccountMode::Unknown(code) => return stopped(StopReason::UnknownAccountMode(code.clone())),
        };
        let (mut cross, mut isolated, mut isolated_settings) = (FxHashMap::default(), FxHashMap::default(), FxHashSet::default());
        for state in account.dex_states.iter().filter(|state| markets.usdc_dexes[state.dex]) {
            cross.insert(state.dex, state.cash_micro * WEI_PER_MICRO);
            for position in &state.positions {
                if let PositionMargin::Isolated { cash_micro } = position.margin {
                    isolated.insert((state.dex, position.asset), cash_micro * WEI_PER_MICRO);
                    isolated_settings.insert((state.dex, position.asset));
                }
            }
            isolated_settings.extend(state.isolated_settings.iter().map(|asset| (state.dex, *asset)));
        }
        let pools = if unified {
            let spot = account.spot.iter().find(|(token, _)| *token == USDC).map_or(0, |(_, raw)| *raw);
            Pools::Unified(spot + cross.values().sum::<i64>())
        } else {
            Pools::PerDex(cross)
        };
        Self { pools, isolated, isolated_settings, switched: None, stop: None }
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

    /// Records that `key` switched between cross and isolated in `block`.
    fn switch(&mut self, block: u64, key: (usize, usize)) {
        match &mut self.switched {
            Some((switched_block, keys)) if *switched_block == block => keys.push(key),
            switched => *switched = Some((block, vec![key])),
        }
    }

    fn switched_in(&self, block: u64, key: (usize, usize)) -> bool {
        self.switched.as_ref().is_some_and(|(switched_block, keys)| *switched_block == block && keys.contains(&key))
    }

    const fn unified(&self) -> bool {
        matches!(self.pools, Pools::Unified(_))
    }

    /// Adds to a USDC dex's cross cash (a unified account's pool).
    fn add_cross(&mut self, dex: usize, wei: i64) {
        match &mut self.pools {
            Pools::PerDex(cross) => *cross.entry(dex).or_default() += wei,
            Pools::Unified(pool) => *pool += wei,
        }
    }

    /// Adds to spot USDC: only a unified account's pool counts it.
    const fn add_spot(&mut self, wei: i64) {
        if let Pools::Unified(pool) = &mut self.pools {
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
            AccountEvent::Funding { coin, amount_wei } => apply_funding(account, markets, coin, *amount_wei),
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
        if coin == SPOT_PAIR_ZERO_COIN {
            return Self::Spot(Some(0));
        }
        if let Some(index) = coin.strip_prefix('@') {
            return Self::Spot(index.parse().ok());
        }
        if coin.starts_with('#') { Self::Outcome } else { Self::Perp }
    }
}

/// A cross fill moves its dex's cross cash. Whether the position is isolated: the asset's `marginMode`, then the
/// account's setting (research "新开仓位是全仓还是逐仓"); an isolated fill stops the replay. A block applies its actions
/// before its fills, so a setting switched in the fill's block leaves its pool unknown: also a stop, as isolated.
fn apply_perp_fill(account: &mut AccountMargin, markets: &Markets, block: u64, fill: &FillEvent) -> Applied {
    let market = perp(markets, &fill.coin)?;
    if fill.backstop {
        return Err(StopReason::BackstopLiquidation);
    }
    let isolated = match &market.margin_mode {
        AssetMarginMode::IsolatedOnly => true,
        AssetMarginMode::Normal => {
            let key = (market.dex, market.asset);
            account.isolated_settings.contains(&key) || account.switched_in(block, key)
        }
        AssetMarginMode::Unknown(mode) => return Err(StopReason::UnknownMarket(format!("{} marginMode {mode:?}", fill.coin))),
    };
    if isolated {
        return Err(StopReason::IsolatedFill);
    }
    let cash = fill.cash_wei(market.size_decimals, MAX_PERP_DECIMALS - market.size_decimals).map_err(StopReason::Contradiction)?;
    account.add_cross(market.dex, cash);
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

/// A USDC fee on a transfer touching spot, by an account whose pools do not hold spot: which balance pays it is not known.
const fn spot_fee_source(account: &AccountMargin, touches_spot: bool, usdc_fee_wei: i64) -> Applied {
    if usdc_fee_wei != 0 && touches_spot && !account.unified() { Err(StopReason::SpotFeeSource) } else { Ok(()) }
}

/// `user` is the account the event is applied to; its role (sender, receiver, vault) comes from the delta.
fn apply_ledger(account: &mut AccountMargin, markets: &Markets, user: Address, delta: &LedgerDelta) -> Applied {
    match delta {
        LedgerDelta::Send { user: sender, destination, source, destination_balance, usdc_wei, usdc_fee_wei } => {
            if user == *sender {
                spot_fee_source(account, *source == Balance::Spot || *destination_balance == Balance::Spot, *usdc_fee_wei)?;
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
                spot_fee_source(account, true, *usdc_fee_wei)?;
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
            if user == *sender {
                add_main(account, markets, -(usdc_wei + fee_wei))?;
            }
            if user == *destination {
                add_main(account, markets, *usdc_wei)?;
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
        }
        BlockAction::Leverage { asset, is_cross } => {
            let market = market_of(*asset)?;
            let key = (market.dex, market.asset);
            let switched = if *is_cross { account.isolated_settings.remove(&key) } else { account.isolated_settings.insert(key) };
            if switched {
                account.switch(block, key);
            }
        }
        BlockAction::TopUpIsolatedOnlyMargin => return Err(StopReason::TopUpIsolatedOnlyMargin),
        BlockAction::SetAbstraction => return Err(StopReason::AccountModeSwitch),
        BlockAction::Withdraw { amount_wei } => add_main(account, markets, -amount_wei)?,
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
            return Some(ReplayedRow { isolated: true, pool_positions: 1, liquidation_px });
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
        Some(ReplayedRow { isolated: false, pool_positions: 1 + others.len(), liquidation_px })
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
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Compared {
    pub(crate) exact: u64,
    /// Of the exact ones, those whose cash moved in the interval: what the replay actually proved.
    pub(crate) moved: u64,
    pub(crate) mismatched: u64,
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
            counts.mismatched += 1;
            if check.samples.len() < MAX_MISMATCH_SAMPLES {
                check.samples.push(Mismatch { user: *user, replayed: replayed.clone(), anchor: anchored.clone() });
            }
        }
        check
    }
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
        let (dir, liquidation) = liquidation.map_or(("Open Long", json!(null)), |(method, dir)| {
            (dir, json!({"liquidatedUser": "0x00000000000000000000000000000000000000ee", "markPx": px, "method": method}))
        });
        let wire = json!({"coin": coin, "px": px, "sz": sz, "side": side, "time": 1, "startPosition": "0.0", "dir": dir,
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
        assert_eq!(isolated.isolated_settings, [(0, 1), (0, 0)].into_iter().collect(), "the PUMP position and BTC set isolated");
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

    /// Research "新开仓位是全仓还是逐仓": the asset's margin mode, then the account's setting.
    #[test]
    fn isolated_fills_and_other_collateral_stop_the_replay() {
        let cases = [
            (ISOLATED, "BTC", StopReason::IsolatedFill),
            (ISOLATED, PUMP_COIN, StopReason::IsolatedFill),
            (HOLDER, "xyz:TSLA", StopReason::IsolatedFill),
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

    /// README 5 / r3 revision 3: each party's side of a transfer, by its role in the delta.
    #[test]
    fn ledger_deltas_move_each_partys_side() {
        let main = || Balance::Dex(String::new());
        let mut standard = per_dex();
        standard.apply(5, user(HOLDER), &send(HOLDER, main(), ISOLATED, Balance::Dex("xyz".to_string()), Some(100), 7));
        standard.apply(5, user(ISOLATED), &send(HOLDER, main(), ISOLATED, Balance::Dex("xyz".to_string()), Some(100), 7));
        standard.apply(6, user(HOLDER), &send(HOLDER, Balance::Spot, HOLDER, main(), Some(50), 0));
        standard.apply(7, user(HOLDER), &send(HOLDER, main(), PORTFOLIO, main(), None, 9));
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
        let main_cash = -1_200_000_000_000 - 107 + 50 - 9 - 1_000 - 30 - 2_000 + 147_527_244_500;
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
            (send(HOLDER, Balance::Dex(String::new()), ISOLATED, Balance::Dex(String::new()), Some(100), 7), None, None),
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
        states.apply(6, user(ISOLATED), &AccountEvent::Action(BlockAction::Leverage { asset: 0, is_cross: true }));
        states.apply(7, user(ISOLATED), &fill("BTC", "A", "82857.6", "0.001", "0.0", None));
        assert_eq!(cross(&states, ISOLATED), per_dex_pools(&[(0, 200 + 8_285_760_000)]));
        // review 000435 r1 P2: BTC switched in the fill's block, the fill may precede the switch
        let mut same_block = per_dex();
        same_block.apply(6, user(ISOLATED), &AccountEvent::Action(BlockAction::Leverage { asset: 0, is_cross: true }));
        same_block.apply(6, user(ISOLATED), &fill("BTC", "A", "82857.6", "0.001", "0.0", None));
        assert_eq!(stop(&same_block, ISOLATED), Some(StopReason::IsolatedFill));
        states.apply(8, user(HOLDER), &AccountEvent::Action(BlockAction::Leverage { asset: 1, is_cross: false }));
        states.apply(9, user(HOLDER), &fill(PUMP_COIN, "B", "0.004477", "1.0", "0.0", None));
        assert_eq!(stop(&states, HOLDER), Some(StopReason::IsolatedFill), "PUMP set isolated before the fill");
        let cases = [
            (BlockAction::IsolatedMargin { asset: 0, ntli_micro: 1 }, "without an isolated position"),
            (BlockAction::SetAbstraction, "accountModeSwitch"),
            (BlockAction::TopUpIsolatedOnlyMargin, "topUpIsolatedOnlyMargin"),
            (BlockAction::Leverage { asset: 77, is_cross: true }, "action asset 77"),
        ];
        for (action, expected) in cases {
            let mut states = per_dex();
            states.apply(5, user(HOLDER), &AccountEvent::Action(action.clone()));
            let reason = stop(&states, HOLDER).unwrap();
            assert!(format!("{reason:?} {}", reason.label()).contains(expected), "{action:?}: {reason:?}");
        }
        // xyz:TSLA is asset 0 of pdi 1: action id 110000
        let mut states = per_dex();
        states.apply(5, user(HOLDER), &AccountEvent::Action(BlockAction::Leverage { asset: 110_000, is_cross: false }));
        assert_eq!(stop(&states, HOLDER), None);
        assert!(states.accounts[&user(HOLDER)].isolated_settings.contains(&(1, 0)));
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
        assert_eq!(alone, ReplayedRow { isolated: false, pool_positions: 1, liquidation_px: solve(pool, 0.0, 1_000_000.0, &pump_table) });
        let shared = row(&[PUMP_COIN, "BTC"], &btc_mark).expect("BTC has a mark");
        // BTC 0.1 at 82,857.6 held at 20x: value 8,285.76, maintenance 207.144
        let btc_table = MarginTable::single(20).unwrap();
        let btc = HeldAtMark { size_raw: 10_000, size_decimals: 5, mark_raw: 828_576, table: &btc_table };
        assert_eq!(shared, ReplayedRow { isolated: false, pool_positions: 2, liquidation_px: solve_held(pool, &[btc], 1_000_000.0, &pump_table) });
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
        let compared = |exact, moved, mismatched| Compared { exact, moved, mismatched };
        assert_eq!(replayed.check_against(&anchor, &anchor).compared["perDex"], compared(2, 0, 0), "HOLDER and ISOLATED");
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
}
