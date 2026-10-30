//! Liquidation rows at one immutable replay/mark view. Batch reads share exact pool sums; nothing is cached across
//! blocks. The target's own mark is not an input to its liquidation price, including when that mark is missing.

use super::{AccountMargin, HeldPosition, MarginStates, Markets, PerpMarket, Pools, ReplayedRow, wei_to_usdc};
use crate::accounts::{checkpoint::base_size, liquidation::{HeldAtMark, solve, solve_net}, margin::MarginTable};
use alloy::primitives::Address;
use rustc_hash::FxHashMap;

/// Only replay-supported USDC cross pools; isolated positions are never members of these sums.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum CrossPool {
    Dex(usize),
    Unified,
}

impl CrossPool {
    fn of(account: &AccountMargin, markets: &Markets, market: &PerpMarket) -> Option<Self> {
        if !markets.usdc_dexes[market.dex] || account.isolated.contains_key(&(market.dex, market.asset)) {
            return None;
        }
        Some(match account.pools {
            Pools::PerDex(_) => Self::Dex(market.dex),
            Pools::Unified(_) => Self::Unified,
        })
    }
}

/// Missing contributions are counted separately, so removing a target with no mark does not invent a zero mark.
#[derive(Debug, Default, Clone, Copy)]
struct PoolAtMarks {
    net_micro: i128,
    members: usize,
    missing: usize,
}

impl PoolAtMarks {
    const fn add(&mut self, net_micro: Option<i128>) {
        self.members += 1;
        match net_micro {
            Some(net) => self.net_micro += net,
            None => self.missing += 1,
        }
    }

    const fn without(mut self, net_micro: Option<i128>) -> Self {
        self.members -= 1;
        match net_micro {
            Some(net) => self.net_micro -= net,
            None => self.missing -= 1,
        }
        self
    }
}

struct HeldRow<'s, 'c> {
    position: HeldPosition<'c>,
    market: Option<&'s PerpMarket>,
    table: Option<&'s MarginTable>,
    pool: Option<CrossPool>,
    net_micro: Option<i128>,
}

/// One account, one read: market/table resolution and held maintenance happen once per member. The order is the
/// caller's holdings order. An unknown market retains the old conservative rule: every cross row is unavailable.
struct AccountRows<'s, 'c> {
    account: &'s AccountMargin,
    held: Vec<HeldRow<'s, 'c>>,
    pools: FxHashMap<CrossPool, PoolAtMarks>,
    unknown_market: bool,
}

impl<'s, 'c> AccountRows<'s, 'c> {
    fn new(
        markets: &'s Markets,
        account: &'s AccountMargin,
        held: impl IntoIterator<Item = HeldPosition<'c>>,
        mark_of: impl Fn(&str) -> Option<u64>,
    ) -> Self {
        let held = held.into_iter();
        let mut rows = Self { account, held: Vec::with_capacity(held.size_hint().0), pools: FxHashMap::default(), unknown_market: false };
        for position in held {
            let market = markets.perps.get(position.coin);
            rows.unknown_market |= market.is_none();
            let table = market.and_then(|market| markets.tables[market.dex].get(&position.table_id?));
            let pool = market.and_then(|market| CrossPool::of(account, markets, market));
            let net_micro = pool.and_then(|_| Some(HeldAtMark {
                size_raw: position.size_raw, mark_raw: mark_of(position.coin)?, table: table?,
            }.net_micro()));
            if let Some(pool) = pool {
                rows.pools.entry(pool).or_default().add(net_micro);
            }
            rows.held.push(HeldRow { position, market, table, pool, net_micro });
        }
        rows
    }

    fn held_row(&self, held: &HeldRow<'_, '_>) -> Option<ReplayedRow> {
        let market = held.market?;
        let table = held.table?;
        let others = held.pool.map_or_else(PoolAtMarks::default, |pool| self.pools[&pool].without(held.net_micro));
        RowTarget { account: self.account, market, table, size_raw: held.position.size_raw }.solve(others, self.unknown_market)
    }
}

/// Shared row semantics for a single query and a pool-batched read; only the other-member accumulation differs.
struct RowTarget<'a> {
    account: &'a AccountMargin,
    market: &'a PerpMarket,
    table: &'a MarginTable,
    size_raw: i64,
}

impl RowTarget<'_> {
    fn solve(&self, others: PoolAtMarks, unknown_market: bool) -> Option<ReplayedRow> {
        let Self { account, market, table, size_raw } = *self;
        let size = base_size(size_raw, market.size_decimals);
        if let Some(cash_wei) = account.isolated.get(&(market.dex, market.asset)) {
            return Some(ReplayedRow {
                isolated: true, pool_positions: 1,
                liquidation_px: solve(wei_to_usdc(*cash_wei), 0.0, size, table), cash_estimated: account.estimated,
            });
        }
        if unknown_market || others.missing != 0 {
            return None;
        }
        let cash_wei = match &account.pools {
            Pools::PerDex(cross) => cross.get(&market.dex).copied().unwrap_or(0),
            Pools::Unified(cash) => *cash,
        };
        Some(ReplayedRow {
            isolated: false, pool_positions: 1 + others.members,
            liquidation_px: solve_net(wei_to_usdc(cash_wei), others.net_micro, size, table), cash_estimated: account.estimated,
        })
    }
}

impl MarginStates {
    /// All open rows in holdings order, sharing each pool's exact held-mark sum. `held` lists each coin once, as
    /// `AccountBook::Holdings` does. Unknown tables/marks affect only the rows that need them; no state survives this call.
    pub(crate) fn replayed_rows<'c>(
        &self,
        user: &Address,
        held: impl IntoIterator<Item = HeldPosition<'c>>,
        mark_of: impl Fn(&str) -> Option<u64>,
    ) -> Vec<(HeldPosition<'c>, ReplayedRow)> {
        let mut held = held.into_iter();
        let Some(first) = held.next() else { return Vec::new() };
        let Some(second) = held.next() else {
            // No other member: a pool sum and a read of the target's mark would both be wasted work.
            let Some(table_id) = first.table_id.filter(|_| first.size_raw != 0) else { return Vec::new() };
            return self.replayed_row(user, first.coin, first.size_raw, table_id, [], mark_of)
                .map_or_else(Vec::new, |row| vec![(first, row)]);
        };
        let Some(account) = self.accounts.get(user).filter(|account| account.stop.is_none()) else { return Vec::new() };
        let rows = AccountRows::new(&self.markets, account, [first, second].into_iter().chain(held), mark_of);
        rows.held.iter().filter(|held| {
            held.position.size_raw != 0 && held.market.is_some_and(|market| self.markets.usdc_dexes[market.dex])
        }).filter_map(|held| Some((held.position, rows.held_row(held)?))).collect()
    }

    /// One target, possibly absent from `held`. Its supplied size/table may differ from the held entry (checkpoint
    /// comparison). A single query scans only the other members; it needs no batch allocation or target mark.
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
        let target = RowTarget { account, market, table, size_raw };
        let Some(pool) = CrossPool::of(account, &self.markets, market) else {
            return target.solve(PoolAtMarks::default(), false);
        };
        let mut others = PoolAtMarks::default();
        for other in held.into_iter().filter(|other| other.coin != coin) {
            // Resolve even before pool filtering: an unknown market may share this pool.
            let other_market = self.markets.perps.get(other.coin)?;
            if CrossPool::of(account, &self.markets, other_market) != Some(pool) {
                continue;
            }
            others.add(Some(HeldAtMark {
                size_raw: other.size_raw, mark_raw: mark_of(other.coin)?,
                table: self.markets.tables[other_market.dex].get(&other.table_id?)?,
            }.net_micro()));
        }
        target.solve(others, false)
    }

    /// Other coins in this cross pool; isolated, stopped and unsupported targets have none.
    pub(crate) fn cross_pool_coins<'c>(&self, user: &Address, coin: &str, held: impl IntoIterator<Item = &'c str>) -> Vec<&'c str> {
        let Some(account) = self.accounts.get(user).filter(|account| account.stop.is_none()) else { return Vec::new() };
        let Some(pool) = self.markets.perps.get(coin).and_then(|market| CrossPool::of(account, &self.markets, market)) else {
            return Vec::new();
        };
        held.into_iter().filter(|other| *other != coin && self.markets.perps.get(*other)
            .is_some_and(|market| CrossPool::of(account, &self.markets, market) == Some(pool))).collect()
    }
}

#[cfg(test)]
mod tests;
