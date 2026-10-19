//! Liquidation price of every position of an account at a checkpoint (design 000419 r4, section 3.4).
//!
//! Pools: default / disabled accounts margin each dex separately (cross cash `u` of that dex, no spot); unified
//! accounts pool per collateral token (spot balance + `u` of every dex with that collateral + positions); an isolated
//! position is its own pool (`l.I.u`). Equity = cash + sum of `szi * mark`: `u` already holds the opening cash flow, so
//! no pnl against entry is added. Other positions of the pool stay at the checkpoint mark; the position's own
//! maintenance tier is taken at the liquidation price (official "Liquidations"), and the candidate must fall in it.
//! Amounts are in the pool's collateral token. Verified against the node on USDC pools only: the USDH / USDE / USDT0
//! dexes held no position on 2026-10-19, so their pools follow the same rule unverified.

use super::{
    checkpoint::{Account, AccountMode, CheckpointAccounts, DexAccount, Position, PositionMargin},
    margin::MarginTable,
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LiquidationPx {
    /// A positive price at which the pool's equity equals its maintenance margin.
    Computed(f64),
    /// No positive solution (e.g. an over-collateralized short).
    NoPositiveSolution,
    Unsupported(UnsupportedReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UnsupportedReason {
    PortfolioMargin,
    UnknownAccountMode(String),
    /// A position of the pool has an asset without a stored mark.
    MissingMark,
    /// A positive candidate exists but none falls in the tier it was solved for.
    TierCase,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PositionLiquidation {
    pub(crate) dex: usize,
    pub(crate) asset: usize,
    /// Signed size in base units.
    pub(crate) size: f64,
    pub(crate) isolated: bool,
    /// Positions sharing the pool (more than 1: the price also moves with the other positions' marks); `None` when
    /// the account's pool rule is unknown (portfolio margin, unknown mode).
    pub(crate) pool_positions: Option<usize>,
    pub(crate) liquidation_px: LiquidationPx,
}

/// Pool cash (in the collateral token) and its members (dex index, position).
type Pool<'a> = (f64, Vec<(usize, &'a Position)>);

/// A valued pool member.
struct Valued<'a> {
    position: &'a Position,
    size: f64,
    mark: f64,
    table: &'a MarginTable,
}

pub(crate) fn account_liquidations(checkpoint: &CheckpointAccounts, account: &Account) -> Vec<PositionLiquidation> {
    let mut out = Vec::new();
    for state in &account.dex_states {
        for position in &state.positions {
            out.push(position_liquidation(checkpoint, account, state, position));
        }
    }
    out
}

fn position_liquidation(
    checkpoint: &CheckpointAccounts,
    account: &Account,
    state: &DexAccount,
    position: &Position,
) -> PositionLiquidation {
    let dex = &checkpoint.dexes[state.dex];
    let isolated = matches!(position.margin, PositionMargin::Isolated { .. });
    let size = position.size_raw as f64 / dex.assets[position.asset].size_scale;
    let pool = pool_of(checkpoint, account, state, position);
    let pool_positions = pool.as_ref().ok().map(|(_, members)| members.len());
    let liquidation_px = match pool {
        Err(reason) => LiquidationPx::Unsupported(reason),
        Ok((cash, members)) => solve_pool(checkpoint, position, cash, &members),
    };
    PositionLiquidation { dex: state.dex, asset: position.asset, size, isolated, pool_positions, liquidation_px }
}

/// Pool cash (collateral token) and members (dex index, position) of the pool `position` belongs to.
fn pool_of<'a>(
    checkpoint: &CheckpointAccounts,
    account: &'a Account,
    state: &'a DexAccount,
    position: &'a Position,
) -> Result<Pool<'a>, UnsupportedReason> {
    match &account.mode {
        AccountMode::PortfolioMargin => return Err(UnsupportedReason::PortfolioMargin),
        AccountMode::Unknown(code) => return Err(UnsupportedReason::UnknownAccountMode(code.clone())),
        AccountMode::Default | AccountMode::Disabled | AccountMode::Unified => {}
    }
    if let PositionMargin::Isolated { cash_micro } = position.margin {
        return Ok((micro(cash_micro), vec![(state.dex, position)]));
    }
    let cross = |dex_state: &'a DexAccount| {
        dex_state.positions.iter().filter(|p| p.margin == PositionMargin::Cross).map(move |p| (dex_state.dex, p))
    };
    if account.mode == AccountMode::Unified {
        let token = checkpoint.dexes[state.dex].collateral_token;
        let pool_states: Vec<&DexAccount> = account
            .dex_states
            .iter()
            .filter(|dex_state| checkpoint.dexes[dex_state.dex].collateral_token == token)
            .collect();
        let spot = account
            .spot
            .iter()
            .find(|(spot_token, _)| *spot_token == token)
            .map_or(0.0, |(_, raw)| *raw as f64 / checkpoint.collateral[&token].spot_scale);
        let cash = spot + pool_states.iter().map(|dex_state| micro(dex_state.cash_micro)).sum::<f64>();
        return Ok((cash, pool_states.into_iter().flat_map(cross).collect()));
    }
    Ok((micro(state.cash_micro), cross(state).collect()))
}

fn solve_pool(
    checkpoint: &CheckpointAccounts,
    target: &Position,
    cash: f64,
    members: &[(usize, &Position)],
) -> LiquidationPx {
    let mut valued = Vec::with_capacity(members.len());
    for (dex_index, position) in members {
        let dex = &checkpoint.dexes[*dex_index];
        let asset = &dex.assets[position.asset];
        let Some(mark) = asset.mark_px else {
            return LiquidationPx::Unsupported(UnsupportedReason::MissingMark);
        };
        valued.push(Valued {
            position,
            size: position.size_raw as f64 / asset.size_scale,
            mark,
            table: dex.table(position.table_id),
        });
    }
    let Some(own) = valued.iter().find(|member| std::ptr::eq(member.position, target)) else {
        unreachable!("a pool always contains its own position");
    };
    let others = valued.iter().filter(|member| !std::ptr::eq(member.position, target));
    let (others_value, others_mm) = others.fold((0.0, 0.0), |(value, mm), member| {
        (member.size.mul_add(member.mark, value), mm + member.table.maintenance_usd(member.size.abs() * member.mark))
    });
    solve(cash + others_value, others_mm, own.size, own.table)
}

/// Price `p` with `fixed + size * p == others_mm + maintenance(|size| * p)`, the maintenance tier taken at `p`.
pub(crate) fn solve(fixed: f64, others_mm: f64, size: f64, table: &MarginTable) -> LiquidationPx {
    let mut positive_candidate = false;
    for (index, tier) in table.tiers().iter().enumerate() {
        let price = (others_mm - tier.deduction_usd - fixed) / size.abs().mul_add(-tier.maintenance_rate, size);
        if !(price.is_finite() && price > 0.0) {
            continue;
        }
        positive_candidate = true;
        let notional = size.abs() * price;
        if tier.lower_bound_usd <= notional && notional < table.upper_bound_usd(index) {
            return LiquidationPx::Computed(price);
        }
    }
    if positive_candidate {
        LiquidationPx::Unsupported(UnsupportedReason::TierCase)
    } else {
        LiquidationPx::NoPositiveSolution
    }
}

fn micro(value: i64) -> f64 {
    value as f64 / 1e6
}

#[cfg(test)]
mod tests {
    use super::*;

    fn computed(px: &LiquidationPx) -> f64 {
        match px {
            LiquidationPx::Computed(price) => *price,
            other => panic!("expected a computed price, got {other:?}"),
        }
    }

    /// Codex's 000419 r1 counterexample: short 1 at 100, cash 180, 5% below 120 notional and 10% - 6 above.
    #[test]
    fn solves_in_the_tier_of_the_liquidation_price() {
        let table = MarginTable::listed([(0, 10, 0), (120_000_000, 5, 6_000_000)]).unwrap();
        assert!((computed(&solve(180.0, 0.0, -1.0, &table)) - 169.090_909).abs() < 1e-5);
    }

    /// Long 1 at 100 with 20 of equity at a 5% rate: cash -80 (the opening flow) liquidates at 84.2105.
    #[test]
    fn long_with_standard_pool_cash() {
        let table = MarginTable::single(10).unwrap();
        assert!((computed(&solve(-80.0, 0.0, 1.0, &table)) - 84.210_526).abs() < 1e-5);
    }

    #[test]
    fn over_collateralized_long_has_no_positive_price() {
        assert_eq!(solve(20.0, 0.0, 1.0, &MarginTable::single(10).unwrap()), LiquidationPx::NoPositiveSolution);
    }
}

#[cfg(test)]
mod pool_tests {
    use super::*;
    use crate::accounts::checkpoint::{CheckpointAccounts, parse_checkpoint_bytes};

    fn parsed() -> CheckpointAccounts {
        parse_checkpoint_bytes(&crate::accounts::checkpoint::tests::fixture()).unwrap()
    }

    fn liquidation_of(checkpoint: &CheckpointAccounts, user: &str, dex: usize, asset: usize) -> PositionLiquidation {
        let account = &checkpoint.accounts[&crate::accounts::checkpoint::tests::user(user)];
        account_liquidations(checkpoint, account).into_iter().find(|p| p.dex == dex && p.asset == asset).unwrap()
    }

    /// Unified pool: spot USDC 200 + main cash -12000 + xyz cash 30.2697, BTC 0.1 at 82857.6 (20x single tier, omitted M)
    /// stays at its mark; PUMP 1,000,000 long at table 52.
    #[test]
    fn unified_pool_counts_spot_and_every_usdc_dex_cash() {
        let checkpoint = parsed();
        let pump = liquidation_of(&checkpoint, "0x00000000000000000000000000000000000000aa", 0, 1);
        assert_eq!(pump.pool_positions, Some(2));
        let fixed = 0.1_f64.mul_add(82_857.6, 200.0 - 12_000.0 + 30.2697);
        let btc_mm = 0.1 * 82_857.6 / 40.0;
        let expected = (btc_mm - fixed) / (1_000_000.0 * (1.0 - 0.05));
        match pump.liquidation_px {
            LiquidationPx::Computed(price) => assert!((price / expected - 1.0).abs() < 1e-12, "{price} vs {expected}"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn standard_pool_ignores_spot_and_other_dexes() {
        let mut checkpoint = parsed();
        let holder = crate::accounts::checkpoint::tests::user("0x00000000000000000000000000000000000000aa");
        checkpoint.accounts.get_mut(&holder).unwrap().mode = AccountMode::Disabled;
        let pump = liquidation_of(&checkpoint, "0x00000000000000000000000000000000000000aa", 0, 1);
        let fixed = 0.1_f64.mul_add(82_857.6, -12_000.0);
        let expected = (0.1 * 82_857.6 / 40.0 - fixed) / (1_000_000.0 * 0.95);
        match pump.liquidation_px {
            LiquidationPx::Computed(price) => assert!((price / expected - 1.0).abs() < 1e-12, "{price} vs {expected}"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn isolated_short_uses_its_own_cash() {
        let checkpoint = parsed();
        let short = liquidation_of(&checkpoint, "0x00000000000000000000000000000000000000cc", 0, 1);
        assert!(short.isolated);
        assert_eq!(short.pool_positions, Some(1));
        // 12000 + (-2e6) p = 2e6 p * 0.05  ->  p = 12000 / 2.1e6
        match short.liquidation_px {
            LiquidationPx::Computed(price) => assert!((price - 12_000.0 / 2_100_000.0).abs() < 1e-15),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn portfolio_margin_and_unknown_modes_are_unsupported() {
        let mut checkpoint = parsed();
        let holder = crate::accounts::checkpoint::tests::user("0x00000000000000000000000000000000000000aa");
        checkpoint.accounts.get_mut(&holder).unwrap().mode = AccountMode::PortfolioMargin;
        let pump = liquidation_of(&checkpoint, "0x00000000000000000000000000000000000000aa", 0, 1);
        assert_eq!(pump.liquidation_px, LiquidationPx::Unsupported(UnsupportedReason::PortfolioMargin));
        assert_eq!(pump.pool_positions, None);
        checkpoint.accounts.get_mut(&holder).unwrap().mode = AccountMode::Unknown("x".to_string());
        let pump = liquidation_of(&checkpoint, "0x00000000000000000000000000000000000000aa", 0, 1);
        assert_eq!(pump.liquidation_px, LiquidationPx::Unsupported(UnsupportedReason::UnknownAccountMode("x".to_string())));
    }

    /// The USDH dex (pdi 2) pools USDH spot 50 + its own cash -4000 with its own positions only: no USDC spot, no main
    /// or xyz cash. flx:GOLD long 1 (table 10 -> 5%).
    #[test]
    fn unified_pool_of_another_collateral_uses_only_that_token() {
        let checkpoint = parsed();
        let gold = liquidation_of(&checkpoint, "0x00000000000000000000000000000000000000aa", 2, 0);
        assert_eq!(gold.pool_positions, Some(1));
        let fixed = 50.0 - 4000.0;
        let expected = -fixed / (1.0 - 0.05);
        match gold.liquidation_px {
            LiquidationPx::Computed(price) => assert!((price / expected - 1.0).abs() < 1e-12, "{price} vs {expected}"),
            other => panic!("{other:?}"),
        }
    }
}
