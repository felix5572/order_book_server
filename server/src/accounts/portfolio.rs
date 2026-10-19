//! Portfolio-margin accounts at a checkpoint (design 000427 r2): the one owner of their rules. The liquidation pool
//! takes its cash from here and the anchor its ratio, so the collateral value, the other tokens' debt, the borrow
//! headroom and its 1 USDC floor exist once.
//!
//! The rules reproduce the node's answers, not the official page (whose 20 USDC offset does not appear, and whose
//! 0.95 trigger is not what the node's `liquidationPx` means): research `hl_account_state_verify_20261019` O2,
//! reference `scripts/pm_model.py` and `pm_checkpoint_reference.py`, Codex 000426. Amounts are USDC at the reserves'
//! oracle prices.
//! - Borrowable tokens: reserves with LTV 0 (USDC, USDT0, USDH). Collateral: LTV > 0 (HYPE, UBTC), counted as
//!   (spot + supply) × px × (0.5 + 0.5 × LTV), only for a token the account supplies (spot held by open orders
//!   counts; unsupplied dust does not).
//! - Per borrowable token T, over the dexes settled in T: U = lent spot T (spot + supply − borrow) + cross cash +
//!   signed cross notional; M = cross maintenance; D = the other borrowable tokens' debt; headroom = min(cap × px, C)
//!   − D, counted only from 1 USDC (below, negative included, it is 0). A token with neither M nor debt does not
//!   take part.
//! - ratio(T) = (M + gap) / (max(U, M) + counted headroom), gap = 0 when U ≥ M, else at least 1 USDC; a
//!   non-positive denominator is 1; capped at 1. The account ratio is the largest, 0 when no token takes part.
//! - Availability after maintenance of the USDC pool = U − M + counted headroom: the node's `liquidationPx` is where
//!   it reaches 0 (one mark moving, everything else frozen).
//!
//! Supported: positions on USDC-settled dexes only (the others are delisted, no samples), collateral within the
//! per-account supply cap, a borrow cap on every token the computation needs. Anything else is unsupported for the
//! whole account; its isolated positions keep their own rule.

use super::{
    checkpoint::{Account, AccountMode, CheckpointAccounts, PositionMargin},
    liquidation::UnsupportedReason,
};

/// The spot token of every supported portfolio-margin pool.
pub(crate) const USDC: u32 = 0;
/// A maintenance gap below this borrows this much; a borrow headroom below it counts as none (node, 2026-10-19).
const MIN_BORROW_USDC: f64 = 1.0;

/// An account's portfolio margin at the anchor.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PortfolioMargin {
    Computed { ratio: f64 },
    Unsupported(UnsupportedReason),
}

/// What the account's USDC cross pool and its ratio need.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PortfolioState {
    pub(crate) ratio: f64,
    /// Cash of the USDC cross pool: lent spot USDC, every USDC dex's cross cash and the counted borrow headroom. The
    /// pool's equity adds its positions' signed notional; equity minus maintenance is the availability.
    pub(crate) usdc_pool_cash: f64,
}

impl PortfolioMargin {
    pub(crate) fn of(state: &Result<PortfolioState, UnsupportedReason>) -> Self {
        match state {
            Ok(state) => Self::Computed { ratio: state.ratio },
            Err(reason) => Self::Unsupported(reason.clone()),
        }
    }
}

/// The portfolio-margin state of a portfolio-margin account, or why it is unsupported.
pub(crate) fn portfolio_state(
    checkpoint: &CheckpointAccounts,
    account: &Account,
) -> Result<PortfolioState, UnsupportedReason> {
    debug_assert_eq!(account.mode, AccountMode::PortfolioMargin);
    let inputs = Inputs::gather(checkpoint, account)?;
    let collateral = inputs.collateral()?;
    Ok(PortfolioState { ratio: inputs.ratio(collateral)?, usdc_pool_cash: inputs.usdc_pool_cash(collateral)? })
}

/// One reserve token of the account, amounts in whole tokens.
#[derive(Debug, Clone, PartialEq)]
struct TokenBook {
    token: u32,
    px: f64,
    ltv: f64,
    borrow_cap: Option<f64>,
    supply_cap: Option<f64>,
    spot: f64,
    supply: f64,
    borrow: f64,
}

impl TokenBook {
    fn borrowable(&self) -> bool {
        self.ltv == 0.0
    }

    fn debt_usdc(&self) -> f64 {
        self.borrow * self.px
    }

    /// Spot plus supply minus borrow, in USDC.
    fn lent_usdc(&self) -> f64 {
        (self.spot + self.supply - self.borrow) * self.px
    }
}

/// The account reduced to plain numbers.
#[derive(Debug, Clone, PartialEq)]
struct Inputs {
    tokens: Vec<TokenBook>,
    /// Cross cash `u` of every USDC dex state (cash-only ones included).
    usdc_dex_cash: f64,
    /// Signed notional (`szi * mark`) of the cross positions on USDC dexes.
    usdc_cross_value: f64,
    /// Maintenance margin of those positions.
    usdc_maintenance: f64,
}

impl Inputs {
    fn gather(checkpoint: &CheckpointAccounts, account: &Account) -> Result<Self, UnsupportedReason> {
        let (mut usdc_dex_cash, mut usdc_cross_value, mut usdc_maintenance) = (0.0, 0.0, 0.0);
        for state in &account.dex_states {
            let dex = &checkpoint.dexes[state.dex];
            if dex.collateral_token != USDC {
                return Err(UnsupportedReason::PortfolioMarginNonUsdcDex);
            }
            usdc_dex_cash += state.cash_micro as f64 / 1e6;
            for position in state.positions.iter().filter(|position| position.margin == PositionMargin::Cross) {
                let asset = &dex.assets[position.asset];
                let mark = asset.mark_px.ok_or(UnsupportedReason::MissingMark)?;
                let size = asset.size(position.size_raw);
                usdc_cross_value = size.mul_add(mark, usdc_cross_value);
                usdc_maintenance += dex.table(position.table_id).maintenance_usd(size.abs() * mark);
            }
        }
        let mut tokens: Vec<TokenBook> = checkpoint
            .reserves
            .iter()
            .map(|(token, reserve)| {
                let spot = account.spot.iter().find(|(spot_token, _)| spot_token == token).map_or(0, |(_, raw)| *raw);
                let shares = account.lending.iter().find(|lending| lending.token == *token);
                let value = |shares: u64, index: f64| shares as f64 * index / reserve.wei_scale;
                TokenBook {
                    token: *token,
                    px: reserve.oracle_px,
                    ltv: reserve.ltv,
                    borrow_cap: reserve.borrow_cap,
                    supply_cap: reserve.supply_cap,
                    spot: spot as f64 / reserve.wei_scale,
                    supply: shares.map_or(0.0, |shares| value(shares.supply_shares, reserve.supply_index)),
                    borrow: shares.map_or(0.0, |shares| value(shares.borrow_shares, reserve.borrow_index)),
                }
            })
            .collect();
        tokens.sort_unstable_by_key(|book| book.token);
        Ok(Self { tokens, usdc_dex_cash, usdc_cross_value, usdc_maintenance })
    }

    /// C: supplied collateral (with its spot) weighted by the liquidation threshold 0.5 + 0.5 × LTV.
    fn collateral(&self) -> Result<f64, UnsupportedReason> {
        let mut total = 0.0;
        for book in self.tokens.iter().filter(|book| !book.borrowable() && book.supply > 0.0) {
            let counted = book.spot + book.supply;
            if book.supply_cap.is_none_or(|cap| counted > cap) {
                return Err(UnsupportedReason::PortfolioMarginSupplyCap);
            }
            total = (counted * book.px).mul_add(0.5f64.mul_add(book.ltv, 0.5), total);
        }
        Ok(total)
    }

    /// min(cap × px, C) minus the other borrowable tokens' debt.
    fn headroom(&self, book: &TokenBook, collateral: f64) -> Result<f64, UnsupportedReason> {
        let cap = book.borrow_cap.ok_or(UnsupportedReason::PortfolioMarginBorrowCap)?;
        let other_debt: f64 =
            self.tokens.iter().filter(|other| other.borrowable() && other.token != book.token).map(TokenBook::debt_usdc).sum();
        Ok((cap * book.px).min(collateral) - other_debt)
    }

    /// (U, M) of borrowable token `book`: only USDC has dexes (`gather` rejects the others).
    fn balance_and_maintenance(&self, book: &TokenBook) -> (f64, f64) {
        if book.token == USDC {
            (book.lent_usdc() + self.usdc_dex_cash + self.usdc_cross_value, self.usdc_maintenance)
        } else {
            (book.lent_usdc(), 0.0)
        }
    }

    /// The headroom the node counts: none below 1 USDC (seen in the availability and, on 10-19 checkpoint 1189710000,
    /// in the ratio of two accounts whose only collateral was worth under 1 USDC).
    fn counted_headroom(&self, book: &TokenBook, collateral: f64) -> Result<f64, UnsupportedReason> {
        let headroom = self.headroom(book, collateral)?;
        Ok(if headroom >= MIN_BORROW_USDC { headroom } else { 0.0 })
    }

    /// `None` when the token takes no part (no maintenance, no debt).
    fn token_ratio(&self, book: &TokenBook, collateral: f64) -> Result<Option<f64>, UnsupportedReason> {
        let (balance, maintenance) = self.balance_and_maintenance(book);
        if maintenance == 0.0 && book.borrow == 0.0 {
            return Ok(None);
        }
        let gap = maintenance - balance;
        let borrowed_for_maintenance = if gap > 0.0 { gap.max(MIN_BORROW_USDC) } else { 0.0 };
        let value = balance.max(maintenance) + self.counted_headroom(book, collateral)?;
        if value <= 0.0 {
            return Ok(Some(1.0));
        }
        Ok(Some(((maintenance + borrowed_for_maintenance) / value).min(1.0)))
    }

    fn ratio(&self, collateral: f64) -> Result<f64, UnsupportedReason> {
        let mut ratio: f64 = 0.0;
        for book in self.tokens.iter().filter(|book| book.borrowable()) {
            if let Some(token_ratio) = self.token_ratio(book, collateral)? {
                ratio = ratio.max(token_ratio);
            }
        }
        Ok(ratio)
    }

    fn usdc_pool_cash(&self, collateral: f64) -> Result<f64, UnsupportedReason> {
        let usdc = self.tokens.iter().find(|book| book.token == USDC).ok_or(UnsupportedReason::PortfolioMarginBorrowCap)?;
        Ok(usdc.lent_usdc() + self.usdc_dex_cash + self.counted_headroom(usdc, collateral)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const USDT0: u32 = 268;
    const USDH: u32 = 360;
    const HYPE: u32 = 150;
    const UBTC: u32 = 197;

    /// Caps of the 2026-10-19 reserves (USDH borrowing: 1 wei).
    fn book(token: u32, px: f64) -> TokenBook {
        let (ltv, borrow_cap, supply_cap) = match token {
            USDC => (0.0, Some(5e7), Some(2e8)),
            HYPE => (0.65, None, Some(1e6)),
            UBTC => (0.5, None, Some(200.0)),
            USDT0 => (0.0, Some(1e6), Some(5e6)),
            USDH => (0.0, Some(1e-8), Some(1.5e6)),
            other => panic!("no reserve {other}"),
        };
        TokenBook { token, px, ltv, borrow_cap, supply_cap, spot: 0.0, supply: 0.0, borrow: 0.0 }
    }

    /// A node answer: the USDC dexes' cross account value (cash + signed notional) and maintenance, besides the books.
    fn inputs(tokens: Vec<TokenBook>, cross_account_value: f64, maintenance: f64) -> Inputs {
        Inputs { tokens, usdc_dex_cash: cross_account_value, usdc_cross_value: 0.0, usdc_maintenance: maintenance }
    }

    fn ratio(inputs: &Inputs) -> f64 {
        inputs.ratio(inputs.collateral().unwrap()).unwrap()
    }

    /// The node prints the ratio with 10 decimals.
    fn assert_node_ratio(ours: f64, node: f64) {
        assert!((ours - node).abs() <= (1e-7 * node).max(1e-10), "{ours} vs node {node}");
    }

    /// `timeseries_1` line 8, 0x94e47db4: borrow 39,173.54 USDC against 2,875.03 HYPE, U < M (gap ≥ 1).
    fn gap_account() -> Inputs {
        let usdc = TokenBook { borrow: 39_173.544_515_63, ..book(USDC, 1.0) };
        let hype = TokenBook { supply: 2_875.033_311_99, ..book(HYPE, 79.256) };
        inputs(vec![usdc, hype, book(USDT0, 0.998_98), book(USDH, 1.0)], 10_797.906_098, 3_245.064_959)
    }

    #[test]
    fn a_maintenance_gap_is_borrowed_in_the_numerator_and_covered_in_the_denominator() {
        assert_node_ratio(ratio(&gap_account()), 0.182_321_289_1);
    }

    /// `timeseries_1` line 516, 0x2365a038: U falls 0.83 short of M; the node borrows 1.
    #[test]
    fn a_gap_below_one_usdc_borrows_one() {
        let usdc = TokenBook { borrow: 798.256_835_93, ..book(USDC, 1.0) };
        let hype = TokenBook { supply: 21.367_843_4, ..book(HYPE, 79.286_66) };
        let account = inputs(vec![usdc, hype, book(USDT0, 0.998_97), book(USDH, 1.0)], 1_414.101_5, 616.670_8);
        assert_node_ratio(ratio(&account), 0.306_631_734_5);
    }

    /// `collect_2` line 1, 0x01c80f69: U ≥ M; USDH neither borrowed nor maintained, so its 1-wei cap minus the USDC
    /// debt must not make it 1 (r1 P2).
    #[test]
    fn a_token_without_debt_or_maintenance_takes_no_part() {
        let usdc = TokenBook { borrow: 24_457.274_438_18, ..book(USDC, 1.0) };
        let hype = TokenBook { supply: 3_500.119_640_5, ..book(HYPE, 79.088_425) };
        let ubtc = TokenBook { supply: 4.000_159_242, ..book(UBTC, 82_165.64) };
        let usdt0 = TokenBook { spot: 0.500_931_98, ..book(USDT0, 0.998_99) };
        let account = inputs(vec![usdc, hype, ubtc, usdt0, book(USDH, 1.0)], 148_894.604_493, 73_978.122_163);
        assert_node_ratio(ratio(&account), 0.123_436_827_7);
    }

    /// `collect_2` line 201, 0xe057a310: 1 USDH borrowed, cap 1 wei, the USDC debt makes the USDH denominator negative.
    #[test]
    fn a_non_positive_denominator_is_one() {
        let usdc = TokenBook { borrow: 8_187.134_869_03, ..book(USDC, 1.0) };
        let hype = TokenBook { supply: 1_075.232_548_7, ..book(HYPE, 79.032) };
        let ubtc = TokenBook { supply: 0.000_062_87, ..book(UBTC, 82_114.0) };
        let one_usdh = TokenBook { spot: 0.977_699_59, borrow: 1.000_000_08, ..book(USDH, 1.0) };
        let account = inputs(vec![usdc, hype, ubtc, book(USDT0, 0.998_99), one_usdh], 7_103.381_43, 3_370.721_708);
        let collateral = account.collateral().unwrap();
        assert!(account.headroom(&account.tokens[4], collateral).unwrap() < 0.0);
        assert_eq!(ratio(&account), 1.0);
    }

    /// 0x738d7fad at checkpoint 1189710000, node answer 4 s later: U ≥ M and 0.0122 HYPE (0.80 USDC of headroom)
    /// as the only collateral; the node's ratio is M / U, the headroom left out.
    #[test]
    fn headroom_below_one_usdc_is_left_out_of_the_ratio_too() {
        let usdc = TokenBook { supply: 46_449.322_996_25, ..book(USDC, 1.0) };
        let hype = TokenBook { supply: 0.012_248_01, ..book(HYPE, 78.809_492) };
        let account = inputs(vec![usdc, hype, book(USDT0, 0.998_96), book(USDH, 1.0)], 8_306.335_728, 3_719.1);
        assert_node_ratio(ratio(&account), 0.067_921_747);
    }

    #[test]
    fn nothing_taking_part_is_a_zero_ratio() {
        let hype = TokenBook { supply: 10.0, ..book(HYPE, 80.0) };
        let account = inputs(vec![book(USDC, 1.0), hype, book(USDH, 1.0)], 500.0, 0.0);
        assert_eq!(ratio(&account), 0.0);
    }

    #[test]
    fn collateral_counts_supplied_tokens_with_their_spot_only() {
        let held = TokenBook { spot: 1_000.0, supply: 1_469.352_084_9, ..book(HYPE, 80.0) };
        let dust = TokenBook { spot: 0.005_636_4, ..book(UBTC, 82_000.0) };
        let account = inputs(vec![book(USDC, 1.0), held, dust], 0.0, 0.0);
        let expected = 2_469.352_084_9 * 80.0 * 0.825;
        assert!((account.collateral().unwrap() - expected).abs() < 1e-9);
    }

    #[test]
    fn collateral_above_the_supply_cap_with_its_spot_is_unsupported() {
        let over = TokenBook { spot: 2.0, supply: 199.0, ..book(UBTC, 82_000.0) };
        let account = inputs(vec![book(USDC, 1.0), over], 0.0, 0.0);
        assert_eq!(account.collateral(), Err(UnsupportedReason::PortfolioMarginSupplyCap));
    }

    #[test]
    fn a_taking_part_token_without_a_borrow_cap_is_unsupported() {
        let uncapped = TokenBook { borrow: 5.0, borrow_cap: None, ..book(USDT0, 1.0) };
        let account = inputs(vec![book(USDC, 1.0), uncapped], 100.0, 0.0);
        assert_eq!(account.ratio(0.0), Err(UnsupportedReason::PortfolioMarginBorrowCap));
    }

    /// Availability = pool cash + signed notional − maintenance = U − M + headroom: 156,366.80 on the node.
    #[test]
    fn the_pool_cash_gives_the_nodes_availability_after_maintenance() {
        let account = gap_account();
        let collateral = account.collateral().unwrap();
        let available = account.usdc_pool_cash(collateral).unwrap() + account.usdc_cross_value - account.usdc_maintenance;
        assert!((available - 156_366.799_766_99).abs() < 1e-3, "{available}");
    }

    /// 0x97abdcd7: the USDT0 debt leaves 0.19 of headroom, below 1 USDC, so the USDC pool gets none.
    #[test]
    fn headroom_below_one_usdc_counts_as_none() {
        let hype = TokenBook { supply: 0.039_081_68, ..book(HYPE, 79.06) };
        let usdt0 = TokenBook { borrow: 2.364_486_86, ..book(USDT0, 0.998_99) };
        let usdc = TokenBook { spot: 1_114.868_460_2, ..book(USDC, 1.0) };
        let account = inputs(vec![usdc, hype, usdt0], 0.0, 0.0);
        let collateral = account.collateral().unwrap();
        let headroom = account.headroom(&account.tokens[0], collateral).unwrap();
        assert!(headroom > 0.0 && headroom < 1.0, "{headroom}");
        assert!((account.usdc_pool_cash(collateral).unwrap() - 1_114.868_460_2).abs() < 1e-9);
    }
}
