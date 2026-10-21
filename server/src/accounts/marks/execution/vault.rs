//! A vault withdrawal runs as an ordinary category-3 action and can synchronously close positions.
//! BM 1191962420: bundle 0 position 624 creates 16 `Vault Close` IOC orders before bundle 1's SGA.
//! Its successful receipt is only `default`; the close oids are in the status stream, under the vault
//! address (the receipt user is the withdrawing depositor). Their paired fills anchor the maker diffs.
//!
//! Only a unique successful withdrawal of that vault in this block is attributed. Multiple withdrawals,
//! missing actions, other order types or a different creation timestamp leave the effects unplaced under
//! the existing cut policy; they are not business failures or new protocol errors. No global upper-cut guess.

use alloy::primitives::Address;
use rustc_hash::{FxHashMap, FxHashSet};

use super::super::actions::{MarkAction, MarkActions, Rank};
use crate::types::node_data::NodeDataOrderStatus;

#[derive(Debug, Clone, PartialEq, Eq)]
struct CloseOrder { vault: Address, oid: u64, timestamp_ms: u64 }

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct VaultCloses {
    orders: Vec<CloseOrder>,
}

impl VaultCloses {
    pub(super) fn observe(&mut self, status: &NodeDataOrderStatus) {
        let order = &status.order;
        if order.order_type != "Vault Close" || order.is_trigger || !order.reduce_only
            || order.tif.as_deref() != Some("Ioc") { return; }
        self.orders.push(CloseOrder { vault: status.user, oid: order.oid, timestamp_ms: order.timestamp });
    }

    pub(super) const fn len(&self) -> usize { self.orders.len() }

    pub(super) fn attributed(&self, actions: &MarkActions, competing: &FxHashSet<Address>) -> Vec<(Rank, u64)> {
        if self.orders.is_empty() { return Vec::new(); }
        let mut withdrawals: FxHashMap<Address, Option<Rank>> = FxHashMap::default();
        for action in &actions.actions {
            if let MarkAction::VaultWithdraw { vault } = action.action {
                withdrawals.entry(vault).and_modify(|rank| *rank = None).or_insert(Some(action.rank));
            }
        }
        self.orders.iter().filter_map(|order| {
            if competing.contains(&order.vault) || order.timestamp_ms != actions.time_ns / 1_000_000 { return None; }
            let rank = withdrawals.get(&order.vault).copied().flatten()?;
            Some((rank, order.oid))
        }).collect()
    }
}

#[cfg(test)]
mod tests;
