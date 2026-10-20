//! A parsed checkpoint reduced to what the account view serves: every universe coin with its positions and their
//! liquidation rows computed at the checkpoint, and the account mode of every position holder. Built on the poller
//! thread so the owner only merges.

use alloy::primitives::Address;
use rustc_hash::FxHashMap;

use super::{
    checkpoint::{AccountMode, CheckpointAccounts},
    liquidation::{AccountLiquidations, LiquidationPx, account_liquidations},
    portfolio::PortfolioMargin,
    replay::MarginStates,
};

#[derive(Debug)]
pub(crate) struct Anchor {
    pub(crate) height: u64,
    pub(crate) time_ms: u64,
    /// Every coin of every dex universe, with or without positions. Coin names are unique across dexes.
    pub(crate) coins: FxHashMap<String, AnchorCoin>,
    /// Account mode of every position holder at the anchor.
    pub(crate) account_modes: FxHashMap<Address, AccountMode>,
    /// The margin state of every position holder, where the cash replay starts (design 000433).
    pub(crate) margins: MarginStates,
}

#[derive(Debug)]
pub(crate) struct AnchorCoin {
    /// `szDecimals`: position sizes of this coin are integers in 10^-size_decimals base units.
    pub(crate) size_decimals: u32,
    /// Non-zero positions at the anchor.
    pub(crate) positions: FxHashMap<Address, AnchorPosition>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AnchorPosition {
    pub(crate) size_raw: i64,
    pub(crate) row: AnchorRow,
}

/// The anchor's own view of one position; it belongs to the anchor height even when the size has moved on since.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AnchorRow {
    pub(crate) isolated: bool,
    pub(crate) pool_positions: Option<usize>,
    pub(crate) liquidation_px: LiquidationPx,
    /// The account's portfolio margin at the anchor; `None` for the other modes.
    pub(crate) portfolio_margin: Option<PortfolioMargin>,
}

impl Anchor {
    pub(crate) fn from_checkpoint(checkpoint: &CheckpointAccounts) -> Result<Self, String> {
        let mut coins = FxHashMap::default();
        for asset in checkpoint.dexes.iter().flat_map(|dex| &dex.assets) {
            let coin = AnchorCoin { size_decimals: asset.size_decimals, positions: FxHashMap::default() };
            if coins.insert(asset.name.clone(), coin).is_some() {
                return Err(format!("checkpoint {}: coin {} is listed twice", checkpoint.height, asset.name));
            }
        }
        let mut account_modes = FxHashMap::default();
        for (user, account) in &checkpoint.accounts {
            let AccountLiquidations { portfolio_margin, positions } = account_liquidations(checkpoint, account);
            for position in positions {
                let name = &checkpoint.dexes[position.dex].assets[position.asset].name;
                let row = AnchorRow {
                    isolated: position.isolated,
                    pool_positions: position.pool_positions,
                    liquidation_px: position.liquidation_px,
                    portfolio_margin: portfolio_margin.clone(),
                };
                let positions = &mut coins
                    .get_mut(name)
                    .ok_or_else(|| format!("checkpoint {}: {user} holds unlisted {name}", checkpoint.height))?
                    .positions;
                if positions.insert(*user, AnchorPosition { size_raw: position.size_raw, row }).is_some() {
                    return Err(format!("checkpoint {}: {user} holds {name} twice", checkpoint.height));
                }
            }
            account_modes.insert(*user, account.mode.clone());
        }
        let margins = MarginStates::from_checkpoint(checkpoint).map_err(|err| format!("checkpoint {}: {err}", checkpoint.height))?;
        Ok(Self { height: checkpoint.height, time_ms: checkpoint.time_ms, coins, account_modes, margins })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::checkpoint::{parse_checkpoint_bytes, tests::fixture};

    #[test]
    fn lists_every_coin_and_keys_positions_by_holder() {
        let checkpoint = parse_checkpoint_bytes(&fixture()).unwrap();
        let anchor = Anchor::from_checkpoint(&checkpoint).unwrap();
        assert_eq!(anchor.height, checkpoint.height);
        let mut names: Vec<&str> = anchor.coins.keys().map(String::as_str).collect();
        names.sort_unstable();
        assert_eq!(names, ["BTC", "PUMP", "flx:GOLD", "xyz:TSLA"]);
        assert_eq!(anchor.coins["xyz:TSLA"].size_decimals, 3);
        let holders = checkpoint.accounts.len();
        assert_eq!(anchor.account_modes.len(), holders);
        let positions: usize = anchor.coins.values().map(|coin| coin.positions.len()).sum();
        let expected: usize = checkpoint
            .accounts
            .values()
            .flat_map(|account| &account.dex_states)
            .map(|state| state.positions.len())
            .sum();
        assert_eq!(positions, expected);
    }
}
