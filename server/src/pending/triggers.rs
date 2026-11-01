//! Batch 2m (hyperliquid-trade design 000652 r1-r5): what a `SetGlobalAction` does on one coin's untriggered orders -
//! the replica's trigger table as of the confirmed height and the pending ones the overlay holds, as the overlay
//! gathers them at the action's place in each run (`Overlay::set_global`).
//!
//! - Which fire: those the action's mark reaches - forecast on the merged book where its line runs, frozen as the line
//!   came -: a fall's at or above it, a rise's at or below it (HL fires on equality: research
//!   `forced_flow_trigger_timing_20261027`), one past its price at once; a trailing stop is not modelled.
//! - In what order: ascending (effective size, account, source) - the node's observed activation order (README
//!   "Observed trigger activation order"): the effective size as the action began, on the frozen positions; a known
//!   account before an unknown one; a confirmed order by its oid before a pending one (its oid, to come, is larger), the
//!   pending ones as they came.
//! - At what limit: `forced_flow::trigger_limit` (a market one 10% past its trigger price, as an IOC; a limit one at its
//!   own, as a GTC).
//! - How much: a plain order its size; a reduce-only one at most what it closes of its account's position, a position
//!   TP/SL all of it - each run from the frozen positions, every fill moving its taker's and its known maker's
//!   (`BatchPositions`, review 000642 r1 P2-1, r3).
//! - When none runs (the coin unknown in that run): an order needing its account's position of an unknown account, or
//!   any unknown account's order beside one (it may be that account's, its fill moving the position: r2 P2-3); a
//!   position order whose account's frozen position no longer holds - moved by a fill of the run, kept so on the action
//!   until its line leaves (r4, r5) - or was not frozen (`positions_unfrozen`; `positions_unavailable` with the account
//!   copy unavailable as the line came).

use std::collections::BTreeSet;

use alloy::primitives::Address;
use rustc_hash::{FxHashMap, FxHashSet};

use super::{
    actions::{ActionId, PendingTrigger, Tif},
    overlay::Fill,
};
use crate::{
    accounts::{
        flow_accounts::AccountsCopy,
        forced_flow::{OrderKind, trigger_limit},
    },
    listeners::order_book::triggers::TriggerRule,
    order_book::{Px, Side, Sz},
    types::inner::InnerL4Order,
};

/// A `SetGlobalAction` on one coin: its forecast mark, frozen as its line came; the positions (signed, 1e-8 units) of
/// the accounts of the orders it reached then, by account - none when the account copy was unavailable
/// (`positions_known` false) -; the accounts whose frozen position a fill of a run moved since - only growing, until its
/// line leaves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SetGlobalCoin {
    pub(crate) mark_px: Px,
    pub(crate) positions: Vec<(Address, i128)>,
    pub(crate) positions_known: bool,
    pub(crate) stale: BTreeSet<Address>,
}

impl SetGlobalCoin {
    /// On `coin` at `mark_px`, the positions of `accounts` frozen as the account copy holds them now; none known without
    /// the copy, or the coin outside its universe.
    pub(crate) fn frozen(coin: &str, mark_px: Px, accounts: &[Address], copy: Option<&AccountsCopy>) -> Self {
        let positions = copy.and_then(|copy| {
            let scale = 10_i128.pow(8_u32.checked_sub(copy.size_decimals(coin)?)?);
            Some(accounts.iter().map(|account| (*account, i128::from(copy.position_raw(*account, coin)) * scale)).collect::<Vec<_>>())
        });
        Self { mark_px, positions_known: positions.is_some(), positions: positions.unwrap_or_default(), stale: BTreeSet::new() }
    }
}

/// Where a triggered order comes from: the replica's table, by its oid and account; a pending placement, by its action
/// and its account as known (r2 P2-2: no exchange oid before its block); with its cloid, which its remainder keeps
/// (review 000652 r6 P2-2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TriggerSource {
    Confirmed { oid: u64, user: Address, cloid: Option<String> },
    Pending { placed: ActionId, cloid: Option<String>, account: Option<Address> },
}

impl TriggerSource {
    pub(crate) const fn account(&self) -> Option<Address> {
        match self {
            Self::Confirmed { user, .. } => Some(*user),
            Self::Pending { account, .. } => *account,
        }
    }

    pub(crate) const fn label(&self) -> &'static str {
        match self {
            Self::Confirmed { .. } => "confirmed",
            Self::Pending { .. } => "pending",
        }
    }
}

/// How a triggered order's size is set as it runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Sizing {
    /// Its own size.
    Order,
    /// Its own size, at most what it closes of its account's position.
    ReduceOnly,
    /// What it closes of its account's position (a position TP/SL, of no size).
    WholePosition,
}

impl Sizing {
    const fn of(reduce_only: bool, position_tpsl: bool) -> Self {
        match (position_tpsl, reduce_only) {
            (true, _) => Self::WholePosition,
            (false, true) => Self::ReduceOnly,
            (false, false) => Self::Order,
        }
    }
}

/// A triggered order as it runs: its source, its limit as the node runs it, its own size and how it is sized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TriggerOrder {
    pub(crate) source: TriggerSource,
    pub(crate) side: Side,
    pub(crate) limit_px: Px,
    /// A market trigger order takes as an IOC; a limit one as a GTC.
    pub(crate) tif: Tif,
    pub(crate) sz: Sz,
    pub(crate) sizing: Sizing,
}

impl TriggerOrder {
    /// A replica order at `trigger_px`; `None` for a trailing stop or an order type of no known kind (not modelled).
    pub(crate) fn confirmed(trigger_px: Px, order: &InnerL4Order) -> Result<Option<Self>, String> {
        let Some(kind) = OrderKind::of(&order.order_type) else { return Ok(None) };
        if !matches!(TriggerRule::of(&order.trigger_condition), TriggerRule::At(_)) {
            return Ok(None);
        }
        let source = TriggerSource::Confirmed { oid: order.oid, user: order.user, cloid: order.cloid.clone() };
        Self::of(source, kind, order.side, order.limit_px, trigger_px, order.sz, Sizing::of(order.reduce_only, order.is_position_tpsl)).map(Some)
    }

    /// A pending placement.
    pub(crate) fn pending(placed: ActionId, account: Option<Address>, order: &PendingTrigger) -> Result<Self, String> {
        let source = TriggerSource::Pending { placed, cloid: order.cloid.clone(), account };
        Self::of(source, order.kind, order.side, order.limit_px, order.trigger_px, order.sz, Sizing::of(order.reduce_only, order.position_tpsl))
    }

    fn of(source: TriggerSource, kind: OrderKind, side: Side, own_limit: Px, trigger_px: Px, sz: Sz, sizing: Sizing) -> Result<Self, String> {
        let limit_px = trigger_limit(kind, side, own_limit, trigger_px)?;
        let tif = match kind {
            OrderKind::Market => Tif::Ioc,
            OrderKind::Limit => Tif::Gtc,
        };
        Ok(Self { source, side, limit_px, tif, sz, sizing })
    }

    const fn needs_position(&self) -> bool {
        !matches!(self.sizing, Sizing::Order)
    }
}

/// The batch a `SetGlobal` runs on its coin from `candidates` - those its mark reaches, the confirmed by oid then the
/// pending as they came -, in activation order; or why none runs, `candidates` given back. And the accounts whose
/// frozen position `moved` no longer holds, to keep on the action (r5: every frozen account a fill of the run moved).
pub(crate) fn batch(candidates: Vec<TriggerOrder>, sga: &SetGlobalCoin, moved: &FxHashSet<Address>) -> (Result<Vec<TriggerOrder>, RuledOut>, Vec<Address>) {
    let frozen: FxHashMap<Address, i128> = sga.positions.iter().copied().collect();
    let newly_stale: Vec<Address> = frozen.keys().filter(|account| moved.contains(*account) && !sga.stale.contains(*account)).copied().collect();
    let stale = |account: &Address| sga.stale.contains(account) || newly_stale.contains(account);
    let unknown_account = candidates.iter().any(|order| order.source.account().is_none());
    let mut ruled_out = None;
    for order in candidates.iter().filter(|order| order.needs_position()) {
        let reason = match order.source.account() {
            None => "account_unknown",
            Some(_) if unknown_account => "account_unknown",
            Some(account) if stale(&account) => "position_moved",
            Some(account) if !frozen.contains_key(&account) && sga.positions_known => "positions_unfrozen",
            Some(account) if !frozen.contains_key(&account) => "positions_unavailable",
            Some(_) => continue,
        };
        ruled_out.get_or_insert(reason);
    }
    if let Some(reason) = ruled_out {
        return (Err(RuledOut { reason, reached: candidates }), newly_stale);
    }
    let positions = BatchPositions(frozen);
    let mut keyed: Vec<(ActivationKey, TriggerOrder)> = candidates
        .into_iter()
        .enumerate()
        .map(|(arrival, order)| {
            let effective = positions.size(&order).map_or(0, Sz::value);
            let account = order.source.account();
            let source = match &order.source {
                TriggerSource::Confirmed { oid, .. } => (false, *oid, 0),
                TriggerSource::Pending { .. } => (true, 0, arrival),
            };
            ((effective, account.is_none(), account.unwrap_or_default(), source), order)
        })
        .collect();
    keyed.sort_by_key(|(key, _)| *key);
    (Ok(keyed.into_iter().map(|(_, order)| order).collect()), newly_stale)
}

/// Why a batch runs none, and the orders it reached all the same.
#[derive(Debug)]
pub(crate) struct RuledOut {
    pub(crate) reason: &'static str,
    pub(crate) reached: Vec<TriggerOrder>,
}

/// (effective size, account unknown, account, (pending, oid, arrival)): ascending is the activation order.
type ActivationKey = (u64, bool, Address, (bool, u64, usize));

/// The batch's positions as its orders run: the one book of a fill's bearing on them (review 000642 r3).
pub(crate) struct BatchPositions(FxHashMap<Address, i128>);

impl BatchPositions {
    /// As the action froze them.
    pub(crate) fn of(sga: &SetGlobalCoin) -> Self {
        Self(sga.positions.iter().copied().collect())
    }

    /// What `order` places now: `None` when it places nothing (an order needing a position has its account frozen:
    /// `batch` left the coin out otherwise).
    pub(crate) fn size(&self, order: &TriggerOrder) -> Option<Sz> {
        if !order.needs_position() {
            return (order.sz.value() > 0).then_some(order.sz);
        }
        let position = order.source.account().and_then(|account| self.0.get(&account)).copied().unwrap_or_default();
        let closes = match order.side {
            Side::Ask => position.max(0),
            Side::Bid => (-position).max(0),
        };
        let closes = u64::try_from(closes).unwrap_or(u64::MAX);
        let size = match order.sizing {
            Sizing::Order | Sizing::ReduceOnly => order.sz.value().min(closes),
            Sizing::WholePosition => closes,
        };
        (size > 0).then_some(Sz::new(size))
    }

    /// A fill of the batch's order of `taker` on `side`: the taker's position and a maker's of the batch the other way.
    pub(crate) fn apply_fill(&mut self, taker: Option<Address>, side: Side, fill: &Fill) {
        let signed = i128::from(fill.sz.value()) * if side == Side::Bid { 1 } else { -1 };
        if let Some(position) = taker.and_then(|taker| self.0.get_mut(&taker)) {
            *position += signed;
        }
        if let Some(position) = fill.maker.account().and_then(|maker| self.0.get_mut(&maker)) {
            *position -= signed;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::flow_accounts::{AccountsSnapshot, tests::copy_of};

    /// A `SetGlobalAction` freezes each account's position as the copy holds it, raw (10^-szDecimals) in book units
    /// (1e-8), 0 without one; none known without the copy, or for a coin outside its universe.
    #[test]
    fn a_set_global_freezes_the_positions_in_book_units() {
        let (long, flat) = (Address::with_last_byte(1), Address::with_last_byte(2));
        let snapshot = AccountsSnapshot {
            generation: 1,
            height: 10,
            time_ms: 1_000,
            coins: FxHashMap::from_iter([("BTC".to_string(), 5)]),
            marks: FxHashMap::default(),
            positions: FxHashMap::from_iter([("BTC".to_string(), FxHashMap::from_iter([(long, -12_345)]))]),
            candidates: Err("none".to_string()),
        };
        let (accounts, _validity) = copy_of(snapshot);
        let copy = accounts.copy().ok();
        let mark_px = Px::parse_from_str("100").unwrap();
        let frozen = SetGlobalCoin::frozen("BTC", mark_px, &[long, flat], copy);
        assert_eq!((frozen.positions, frozen.positions_known), (vec![(long, -12_345_000), (flat, 0)], true), "-0.12345 BTC");
        let outside = SetGlobalCoin::frozen("ETH", mark_px, &[long], copy);
        let without = SetGlobalCoin::frozen("BTC", mark_px, &[long], None);
        assert_eq!([(outside.positions.len(), outside.positions_known), (without.positions.len(), without.positions_known)], [(0, false), (0, false)]);
    }
}
