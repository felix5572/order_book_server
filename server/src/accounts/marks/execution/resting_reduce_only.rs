//! BM ZEC1196468228: open(new reduce-only GTC), reduceOnlyCanceled(old same-side order),
//! with a unique resting receipt and no fills. The cleanup belongs to the placement, before the
//! next bundle's SGA. Only that witnessed no-fill path is attributed; no position-capacity rule
//! is predicted. Gaps, duplicate identities, fills, triggers or competing owners supply no proof.

use rustc_hash::FxHashMap;
use super::{BlockFill, StatusOrder};
use super::super::{actions::{MarkAction, MarkActions, Rank}, decimal::ExactDecimal,
    orders::OrderKey, system::SystemAnchors};
use crate::types::node_data::NodeDataOrderStatus;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Run {
    order: StatusOrder,
    key: OrderKey,
    timestamp_ms: u64,
    cancelled: Vec<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct RestingReduceOnly {
    runs: Vec<Run>,
}

/// Duplicate counters are temporary; the block replay log retains only witnessed cleanup runs.
#[derive(Default)]
pub(super) struct StatusRuns {
    pending: Option<Run>,
    runs: Vec<Run>,
    opens: FxHashMap<u64, usize>,
    cancels: FxHashMap<u64, usize>,
}

impl StatusRuns {
    pub(super) fn observe(&mut self, status: &NodeDataOrderStatus) -> Result<(), String> {
        let order = &status.order;
        if status.status == "open" { *self.opens.entry(order.oid).or_default() += 1; }
        if status.status == "reduceOnlyCanceled" { *self.cancels.entry(order.oid).or_default() += 1; }
        if let Some(mut run) = self.pending.take() {
            if status.status == "reduceOnlyCanceled" && plain_reduce_only(status)
                && status.user == run.key.user && order.coin == run.order.coin.as_ref()
                && order.side == run.order.side && order.oid != run.order.oid
                && order.timestamp < run.timestamp_ms {
                run.cancelled.push(order.oid);
                self.pending = Some(run);
                return Ok(());
            }
            if !run.cancelled.is_empty() { self.runs.push(run); }
        }
        // Every intervening status closes a run, even an unrecognized one.
        if status.status != "open" || !plain_reduce_only(status) || order.tif.as_deref() != Some("Gtc") {
            return Ok(());
        }
        let Some(key) = OrderKey::observed(status)? else { return Ok(()) };
        if key.sz > ExactDecimal::from_units(0, 0) && ExactDecimal::parse(&order.sz)? == key.sz {
            self.pending = Some(Run { order: StatusOrder::of(status)?, key, timestamp_ms: order.timestamp, cancelled: Vec::new() });
        }
        Ok(())
    }

    pub(super) fn finish(mut self) -> RestingReduceOnly {
        self.runs.extend(self.pending);
        self.runs.retain(|run| !run.cancelled.is_empty() && self.opens.get(&run.order.oid) == Some(&1)
            && run.cancelled.iter().all(|oid| self.cancels.get(oid) == Some(&1)));
        RestingReduceOnly { runs: self.runs }
    }
}

impl RestingReduceOnly {
    pub(super) fn len(&self) -> usize {
        self.runs.iter().map(|run| 1 + run.cancelled.len()).sum()
    }

    pub(super) fn attributed(&self, actions: &MarkActions, fills: &[BlockFill], triggered: &[StatusOrder],
        asset: &impl Fn(&str) -> Option<u32>, system: &SystemAnchors, cancelled_elsewhere: &impl Fn(u64) -> bool)
        -> Vec<(Rank, Vec<u64>)>
    {
        let claimed = |oid| system.competing_orders.contains(&oid) || system.competing_ioc_cancels.contains(&oid)
            || system.before.created.contains(&oid) || system.after.created.contains(&oid)
            || system.before.cancelled.contains(&oid) || system.after.cancelled.contains(&oid)
            || cancelled_elsewhere(oid);
        let mut out = Vec::new();
        for run in &self.runs {
            if run.cancelled.is_empty() || run.timestamp_ms != actions.time_ns / 1_000_000
                || claimed(run.order.oid)
                || system.unplaced_vault_withdrawals.contains(&run.key.user)
                || run.cancelled.iter().any(|oid| claimed(*oid))
                || fills.iter().any(|fill| fill.coin == run.order.coin)
                || triggered.iter().any(|order| order.coin == run.order.coin && order.user == run.key.user) { continue; }
            let mut owners = actions.actions.iter().filter_map(|action| match &action.action {
                MarkAction::Book(book) if book.created.contains(&run.order.oid) => Some((action, book)),
                _ => None,
            });
            let (Some((owner, book)), None) = (owners.next(), owners.next()) else { continue };
            let Some(request) = &book.resting_reduce_only else { continue };
            if !book.ordinary_order || book.created.as_slice() != [run.order.oid]
                || owner.user != Some(run.key.user) || request.oid != run.order.oid
                || request.key != run.key || asset(&run.order.coin) != Some(request.asset) { continue; }
            out.push((owner.rank, run.cancelled.clone()));
        }
        out
    }
}

fn plain_reduce_only(status: &NodeDataOrderStatus) -> bool {
    let order = &status.order;
    order.reduce_only && !order.is_trigger && !order.is_position_tpsl && order.children.is_empty()
        && order.order_type == "Limit" && matches!(order.tif.as_deref(), Some("Alo" | "Gtc"))
}

#[cfg(test)]
mod tests;
