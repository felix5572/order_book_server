//! Liquidation price of every position of an account at a checkpoint (design 000419 r4, section 3.4).
//!
//! Pools: default / disabled accounts margin each dex separately (cross cash `u` of that dex, no spot); unified
//! accounts pool per collateral token (spot balance + `u` of every dex with that collateral + positions); a
//! portfolio-margin account has one USDC cross pool whose cash `portfolio` gives (design 000427 r2), so the price is
//! where its availability after maintenance reaches 0, as the node's `liquidationPx`; an isolated position is its
//! own pool (`l.I.u`) in every known mode. Equity = cash + sum of `szi * mark`: `u` already holds the opening cash flow, so
//! no pnl against entry is added. Other positions of the pool stay at the checkpoint mark; the position's own
//! maintenance tier is taken at the liquidation price (official "Liquidations"), and the candidate must fall in it.
//! Amounts are in the pool's collateral token. Verified against the node on USDC pools only: the USDH / USDE / USDT0
//! dexes held no position on 2026-10-19, so their pools follow the same rule unverified.

use super::{
    checkpoint::{Account, AccountMode, CheckpointAccounts, DexAccount, Position, PositionMargin},
    margin::MarginTable,
    portfolio::{PortfolioMargin, PortfolioState, USDC, portfolio_state},
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
    UnknownAccountMode(String),
    /// A portfolio-margin account with state on a dex not settled in USDC (delisted dexes, no samples).
    PortfolioMarginNonUsdcDex,
    /// A portfolio-margin account whose supplied collateral (with its spot) exceeds, or has no, per-account supply
    /// cap: whether the node truncates is unverified.
    PortfolioMarginSupplyCap,
    /// A borrowable token the portfolio-margin computation needs has no per-account borrow cap.
    PortfolioMarginBorrowCap,
    /// A position of the pool has an asset without a stored mark.
    MissingMark,
    /// A positive candidate exists but none falls in the tier it was solved for.
    TierCase,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PositionLiquidation {
    pub(crate) dex: usize,
    pub(crate) asset: usize,
    /// Signed size in the asset's raw units (10^-szDecimals), as the checkpoint stores it.
    pub(crate) size_raw: i64,
    pub(crate) isolated: bool,
    /// Positions sharing the pool (more than 1: the price also moves with the other positions' marks); `None` when
    /// the account's pool rule is unknown or unsupported.
    pub(crate) pool_positions: Option<usize>,
    pub(crate) liquidation_px: LiquidationPx,
}

/// Pool cash (in the collateral token) and its members (dex index, position).
type Pool<'a> = (f64, Vec<(usize, &'a Position)>);

/// A valued pool member: its maintenance at the mark is the node's integer one.
struct Valued<'a> {
    position: &'a Position,
    size: f64,
    mark: f64,
    maintenance_micro: i128,
    table: &'a MarginTable,
}

/// Every position of an account at the checkpoint, and its portfolio margin when it is a portfolio-margin account.
#[derive(Debug)]
pub(crate) struct AccountLiquidations {
    pub(crate) portfolio_margin: Option<PortfolioMargin>,
    pub(crate) positions: Vec<PositionLiquidation>,
}

/// How an account's cross positions pool, decided once per account.
enum PoolRule {
    /// Each dex on its own (default, disabled).
    PerDex,
    /// Per collateral token, spot included.
    Unified,
    Portfolio(Result<PortfolioState, UnsupportedReason>),
    Unknown(String),
}

impl PoolRule {
    fn of(checkpoint: &CheckpointAccounts, account: &Account) -> Self {
        match &account.mode {
            AccountMode::Default | AccountMode::Disabled => Self::PerDex,
            AccountMode::Unified => Self::Unified,
            AccountMode::PortfolioMargin => Self::Portfolio(portfolio_state(checkpoint, account)),
            AccountMode::Unknown(code) => Self::Unknown(code.clone()),
        }
    }
}

pub(crate) fn account_liquidations(checkpoint: &CheckpointAccounts, account: &Account) -> AccountLiquidations {
    let rule = PoolRule::of(checkpoint, account);
    let mut positions = Vec::new();
    for state in &account.dex_states {
        for position in &state.positions {
            positions.push(position_liquidation(checkpoint, account, &rule, state, position));
        }
    }
    let portfolio_margin = match &rule {
        PoolRule::Portfolio(state) => Some(PortfolioMargin::of(state)),
        PoolRule::PerDex | PoolRule::Unified | PoolRule::Unknown(_) => None,
    };
    AccountLiquidations { portfolio_margin, positions }
}

fn position_liquidation(
    checkpoint: &CheckpointAccounts,
    account: &Account,
    rule: &PoolRule,
    state: &DexAccount,
    position: &Position,
) -> PositionLiquidation {
    let isolated = matches!(position.margin, PositionMargin::Isolated { .. });
    let pool = pool_of(checkpoint, account, rule, state, position);
    let pool_positions = pool.as_ref().ok().map(|(_, members)| members.len());
    let liquidation_px = match pool {
        Err(reason) => LiquidationPx::Unsupported(reason),
        Ok((cash, members)) => solve_pool(checkpoint, position, cash, &members),
    };
    PositionLiquidation {
        dex: state.dex,
        asset: position.asset,
        size_raw: position.size_raw,
        isolated,
        pool_positions,
        liquidation_px,
    }
}

/// Pool cash (collateral token) and members (dex index, position) of the pool `position` belongs to.
fn pool_of<'a>(
    checkpoint: &CheckpointAccounts,
    account: &'a Account,
    rule: &PoolRule,
    state: &'a DexAccount,
    position: &'a Position,
) -> Result<Pool<'a>, UnsupportedReason> {
    let isolated_cash = match position.margin {
        PositionMargin::Isolated { cash_micro } => Some(cash_micro),
        PositionMargin::Cross => None,
    };
    let token = checkpoint.dexes[state.dex].collateral_token;
    match (rule, isolated_cash) {
        (PoolRule::Unknown(code), _) => Err(UnsupportedReason::UnknownAccountMode(code.clone())),
        (_, Some(cash_micro)) => Ok((micro(cash_micro), vec![(state.dex, position)])),
        (PoolRule::PerDex, None) => Ok((micro(state.cash_micro), cross(state).collect())),
        (PoolRule::Unified, None) => {
            let spot = account
                .spot
                .iter()
                .find(|(spot_token, _)| *spot_token == token)
                .map_or(0.0, |(_, raw)| *raw as f64 / checkpoint.collateral[&token].spot_scale);
            let pool_states = settled_in(checkpoint, account, token);
            let cash = spot + pool_states.iter().map(|dex_state| micro(dex_state.cash_micro)).sum::<f64>();
            Ok((cash, pool_states.into_iter().flat_map(cross).collect()))
        }
        // `portfolio_state` rejects accounts with state on a dex not settled in USDC, so `token` is USDC here
        (PoolRule::Portfolio(portfolio), None) => {
            let portfolio = portfolio.as_ref().map_err(Clone::clone)?;
            Ok((portfolio.usdc_pool_cash, settled_in(checkpoint, account, USDC).into_iter().flat_map(cross).collect()))
        }
    }
}

/// The account's states on the dexes settled in `token`.
fn settled_in<'a>(checkpoint: &CheckpointAccounts, account: &'a Account, token: u32) -> Vec<&'a DexAccount> {
    account.dex_states.iter().filter(|dex_state| checkpoint.dexes[dex_state.dex].collateral_token == token).collect()
}

/// The cross positions of one dex state, with its dex index.
fn cross(dex_state: &DexAccount) -> impl Iterator<Item = (usize, &Position)> {
    dex_state.positions.iter().filter(|p| p.margin == PositionMargin::Cross).map(move |p| (dex_state.dex, p))
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
        let (Some(mark), Some(maintenance_micro)) = (asset.mark_px(), dex.maintenance_micro(position)) else {
            return LiquidationPx::Unsupported(UnsupportedReason::MissingMark);
        };
        valued.push(Valued {
            position,
            size: asset.size(position.size_raw),
            mark,
            maintenance_micro,
            table: dex.table(position.table_id),
        });
    }
    let Some(own) = valued.iter().find(|member| std::ptr::eq(member.position, target)) else {
        unreachable!("a pool always contains its own position");
    };
    // the node solves the target continuously in its price and holds the others at their integer maintenance
    let others = valued.iter().filter(|member| !std::ptr::eq(member.position, target));
    let (others_value, others_mm_micro) = others.fold((0.0, 0_i128), |(value, mm), member| {
        (member.size.mul_add(member.mark, value), mm + member.maintenance_micro)
    });
    solve(cash + others_value, others_mm_micro as f64 / 1e6, own.size, own.table)
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
        account_liquidations(checkpoint, account).positions.into_iter().find(|p| p.dex == dex && p.asset == asset).unwrap()
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

    /// The node holds the pool's other positions at their integer maintenance (floored per position to micro-USD) and
    /// solves the target continuously (bm 2026-10-19, research README "维持保证金的整数规则"): BTC 0.10001 at 82857.6
    /// is 8,286,588,576 micro-USD of notional, 207,164,714 micro-USD at 20x (207.1647144 continuous).
    #[test]
    fn other_positions_count_their_integer_maintenance() {
        let mut checkpoint = parsed();
        let holder = crate::accounts::checkpoint::tests::user("0x00000000000000000000000000000000000000aa");
        let main = checkpoint.accounts.get_mut(&holder).unwrap().dex_states.iter_mut().find(|state| state.dex == 0).unwrap();
        main.positions.iter_mut().find(|position| position.asset == 0).unwrap().size_raw = 10_001;
        let pump = liquidation_of(&checkpoint, "0x00000000000000000000000000000000000000aa", 0, 1);
        let fixed = 0.100_01_f64.mul_add(82_857.6, 200.0 - 12_000.0 + 30.2697);
        let expected = (207.164_714 - fixed) / (1_000_000.0 * 0.95);
        assert_computed(&pump.liquidation_px, expected);
        let continuous = (0.100_01 * 82_857.6 / 40.0 - fixed) / (1_000_000.0 * 0.95);
        assert!((expected / continuous - 1.0).abs() > 1e-11, "the case must separate the two rules");
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
    fn an_unknown_mode_is_unsupported_isolated_positions_included() {
        let mut checkpoint = parsed();
        let holder = crate::accounts::checkpoint::tests::user("0x00000000000000000000000000000000000000cc");
        checkpoint.accounts.get_mut(&holder).unwrap().mode = AccountMode::Unknown("x".to_string());
        let short = liquidation_of(&checkpoint, "0x00000000000000000000000000000000000000cc", 0, 1);
        assert_eq!(short.liquidation_px, LiquidationPx::Unsupported(UnsupportedReason::UnknownAccountMode("x".to_string())));
        assert_eq!(short.pool_positions, None);
    }

    fn assert_computed(px: &LiquidationPx, expected: f64) {
        match px {
            LiquidationPx::Computed(price) => assert!((price / expected - 1.0).abs() < 1e-12, "{price} vs {expected}"),
            other => panic!("{other:?}"),
        }
    }

    /// Fixture PORTFOLIO: C = (10 spot + 100 supplied HYPE) x 80 x 0.825 = 7260; pool cash = -3150 borrowed - 20000 main
    /// + 1 xyz + 7260 headroom = -15889; PUMP 5,000,000 long at 0.004477, 5% (table 52, first tier).
    #[test]
    fn portfolio_margin_pools_lending_dex_cash_and_headroom() {
        let checkpoint = parsed();
        let portfolio = crate::accounts::checkpoint::tests::PORTFOLIO;
        let pump = liquidation_of(&checkpoint, portfolio, 0, 1);
        assert_eq!(pump.pool_positions, Some(1));
        assert_computed(&pump.liquidation_px, 15_889.0 / (5_000_000.0 * 0.95));
        // U = -3150 - 19999 + 22385 = -764 < M = 1119.25: (2M - U) / (M + C)
        let account = &checkpoint.accounts[&crate::accounts::checkpoint::tests::user(portfolio)];
        let expected = 2.0f64.mul_add(1_119.25, 764.0) / (1_119.25 + 7_260.0);
        match account_liquidations(&checkpoint, account).portfolio_margin {
            Some(PortfolioMargin::Computed { ratio }) => assert!((ratio / expected - 1.0).abs() < 1e-12, "{ratio} vs {expected}"),
            other => panic!("{other:?}"),
        }
    }

    /// Isolated TSLA 1.0 with cash -300 at 5% (table 10): the portfolio-margin account's isolated rule is the usual one.
    #[test]
    fn a_portfolio_margin_isolated_position_uses_its_own_cash() {
        let checkpoint = parsed();
        let tsla = liquidation_of(&checkpoint, crate::accounts::checkpoint::tests::PORTFOLIO, 1, 0);
        assert!(tsla.isolated);
        assert_computed(&tsla.liquidation_px, 300.0 / 0.95);
    }

    /// Cash on the USDH dex: the whole account's cross side and ratio are unsupported, its isolated position is not.
    #[test]
    fn portfolio_margin_state_on_a_non_usdc_dex_is_unsupported() {
        let mut wire: serde_json::Value = rmp_serde::from_slice(&crate::accounts::checkpoint::tests::fixture()).unwrap();
        let portfolio = crate::accounts::checkpoint::tests::PORTFOLIO;
        let states = wire.pointer_mut("/exchange/locus/cls/2/user_states/user_to_state").unwrap().as_array_mut().unwrap();
        states.push(serde_json::json!([portfolio, {"u": 1_000_000}]));
        let checkpoint = parse_checkpoint_bytes(&rmp_serde::to_vec(&wire).unwrap()).unwrap();
        let reason = UnsupportedReason::PortfolioMarginNonUsdcDex;
        assert_eq!(liquidation_of(&checkpoint, portfolio, 0, 1).liquidation_px, LiquidationPx::Unsupported(reason.clone()));
        assert!(matches!(liquidation_of(&checkpoint, portfolio, 1, 0).liquidation_px, LiquidationPx::Computed(_)));
        let account = &checkpoint.accounts[&crate::accounts::checkpoint::tests::user(portfolio)];
        assert_eq!(account_liquidations(&checkpoint, account).portfolio_margin, Some(PortfolioMargin::Unsupported(reason)));
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
