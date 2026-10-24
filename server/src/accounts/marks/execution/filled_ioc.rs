//! Successful IOC status runs: open(taker), zero or more filled makers, self-trade cancels,
//! filled(the same taker). BM PROVE 1192106175 and PURR 1192117608 show that the cancels
//! can follow the maker book updates, immediately before the next bundle's price action.
//! Require both ends, matching order identity, actual paired fills and one receipt owner.
//! TIA1195513614 also emits contiguous old same-side reduceOnlyCanceled statuses after a full
//! reduce-only IOC terminal. A unique single-IOC receipt and complete paired quantity anchor that cleanup.
//! Incomplete/interrupted status runs stay unplaced; neither maker age nor shared vault ownership is guessed.

use rustc_hash::FxHashMap;
use super::{BlockFill, StatusOrder, OrderSide};
use super::super::{actions::{MarkAction, MarkActions, Rank}, decimal::ExactDecimal,
    orders::OrderKey, system::SystemAnchors};
use crate::types::node_data::NodeDataOrderStatus;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Run {
    taker: StatusOrder,
    key: OrderKey,
    timestamp_ms: u64,
    makers: Vec<u64>,
    cancelled: Vec<u64>,
    complete: bool,
    reduce_only_tail: Vec<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct FilledIocs {
    pending: Option<Run>,
    runs: Vec<Run>,
    heads: FxHashMap<u64, usize>,
}

impl FilledIocs {
    pub(super) fn observe(&mut self, status: &NodeDataOrderStatus) -> Result<(), String> {
        let order = &status.order;
        if status.status == "open" && order.order_type == "Limit" && !order.is_trigger
            && order.tif.as_deref() == Some("Ioc") {
            if let Some(run) = self.pending.take() { self.keep(run); }
            *self.heads.entry(order.oid).or_default() += 1;
            let Some(key) = OrderKey::observed(status)? else { return Ok(()) };
            if ExactDecimal::parse(&order.sz)? == key.sz {
                self.pending = Some(Run { taker: StatusOrder::of(status)?, key, timestamp_ms: order.timestamp,
                    makers: Vec::new(), cancelled: Vec::new(), complete: false, reduce_only_tail: Vec::new() });
            }
            return Ok(());
        }
        let Some(mut run) = self.pending.take() else { return Ok(()) };
        if run.complete {
            // A full reduce-only IOC can close the position, then cancel its old same-side orders.
            // BM TIA1195513614: the cleanup follows filled(taker), before the next bundle's SGA.
            if status.status == "reduceOnlyCanceled" && order.reduce_only && !order.is_trigger
                && order.order_type == "Limit" && status.user == run.taker.user
                && order.coin == run.taker.coin.as_ref() && order.side == run.taker.side
                && order.oid != run.taker.oid && order.timestamp < run.timestamp_ms {
                run.reduce_only_tail.push(order.oid);
                self.pending = Some(run);
            } else { self.keep(run); }
            return Ok(());
        }
        if status.status == "filled" && order.oid == run.taker.oid {
            // `filled` closes even a partially executed IOC: PROVE 1192236393 kept sz=33
            // after 66 of 99 filled. The remainder never rests; zero is not a terminal-status invariant.
            run.complete = order.order_type == "Limit" && order.coin == run.taker.coin.as_ref()
                && order.timestamp == run.timestamp_ms && OrderKey::observed(status)?.as_ref() == Some(&run.key)
                && ExactDecimal::parse(&order.sz)? < run.key.sz;
            if run.complete && run.key.reduce_only && ExactDecimal::parse(&order.sz)? == ExactDecimal::from_units(0, 0) {
                self.pending = Some(run);
            } else { self.keep(run); }
            return Ok(());
        }
        if matches!(status.status.as_str(), "filled" | "selfTradeCanceled")
            && order.oid != run.taker.oid && order.coin == run.taker.coin.as_ref()
            && order.side != run.taker.side {
            let maker = StatusOrder::of(status)?;
            if maker.reached_by(run.taker.limit_px) {
                if status.status == "selfTradeCanceled" {
                    run.cancelled.push(order.oid);
                    self.pending = Some(run);
                    return Ok(());
                }
                if run.cancelled.is_empty() && ExactDecimal::parse(&order.sz)? == ExactDecimal::from_units(0, 0) {
                    run.makers.push(order.oid);
                    self.pending = Some(run);
                    return Ok(());
                }
            }
        }
        self.keep(run);
        Ok(())
    }

    fn keep(&mut self, run: Run) {
        if !run.cancelled.is_empty() || !run.reduce_only_tail.is_empty() { self.runs.push(run); }
    }

    pub(super) fn contains_cancel(&self, oid: u64) -> bool {
        self.runs.iter().chain(self.pending.iter()).any(|run| run.cancelled.contains(&oid) || run.reduce_only_tail.contains(&oid))
    }

    pub(super) fn len(&self) -> usize {
        self.heads.len() + self.runs.iter().chain(self.pending.iter())
            .map(|run| 1 + run.makers.len() + run.cancelled.len() + run.reduce_only_tail.len()).sum::<usize>()
    }

    pub(super) fn attributed(&self, actions: &MarkActions, fills: &[BlockFill],
        asset: &impl Fn(&str) -> Option<u32>, system: &SystemAnchors) -> Vec<(Rank, Vec<u64>)> {
        if self.runs.is_empty() && self.pending.is_none() { return Vec::new(); }
        let mut out = Vec::new();
        let mut tail_claims = FxHashMap::<u64, usize>::default();
        for oid in self.runs.iter().chain(self.pending.iter()).flat_map(|run| &run.reduce_only_tail) {
            *tail_claims.entry(*oid).or_default() += 1;
        }
        for run in self.runs.iter().chain(self.pending.iter()) {
            if !run.complete || run.timestamp_ms != actions.time_ns / 1_000_000
                || self.heads.get(&run.taker.oid) != Some(&1)
                || system.competing_orders.contains(&run.taker.oid)
                || system.before.created.contains(&run.taker.oid) || system.after.created.contains(&run.taker.oid)
                || run.cancelled.iter().any(|oid| system.competing_ioc_cancels.contains(oid)) { continue; }
            let mut owners = actions.actions.iter().filter_map(|action| match &action.action {
                MarkAction::Book(book) if book.created.contains(&run.taker.oid) => Some((action, book)),
                _ => None,
            });
            let (Some((owner, book)), None) = (owners.next(), owners.next()) else { continue };
            let Some(asset) = asset(&run.taker.coin) else { continue };
            if book.created.iter().filter(|oid| **oid == run.taker.oid).count() != 1
                || owner.user != Some(run.key.user) || !book.takers.iter().any(|taker|
                taker.asset == asset && taker.is_buy == (run.taker.side == OrderSide::Bid)
                    && taker.limit_px == run.taker.limit_px) { continue; }
            let taker_fills: Vec<_> = fills.iter().filter(|fill| fill.crossed && !fill.twap
                && fill.oid == run.taker.oid && fill.coin == run.taker.coin).collect();
            if taker_fills.is_empty() || !run.makers.iter().all(|oid| fills.iter().any(|maker|
                !maker.crossed && maker.oid == *oid && maker.coin == run.taker.coin
                    && taker_fills.iter().any(|taker| taker.tid == maker.tid))) { continue; }
            let mut cancelled = run.cancelled.clone();
            if !run.reduce_only_tail.is_empty() && book.single_ioc_order && book.ioc_pairing_mismatch.is_none()
                && paired_full_size(run, &taker_fills, fills)
                && run.reduce_only_tail.iter().all(|oid| tail_claims[oid] == 1 && !system.competing_orders.contains(oid)
                    && !system.before.cancelled.contains(oid) && !system.after.cancelled.contains(oid)
                    && !system.competing_ioc_cancels.contains(oid)) {
                cancelled.extend(&run.reduce_only_tail);
            }
            if !cancelled.is_empty() { out.push((owner.rank, cancelled)); }
        }
        out
    }
}

// A cleanup tail needs all of the full IOC quantity, one real maker per tid and no duplicate legs.
// Six decimals cover native perp sizes; unrepresentable quantities supply no ordering proof.
fn paired_full_size(run: &Run, takers: &[&BlockFill], fills: &[BlockFill]) -> bool {
    let Some(expected) = run.key.sz.units(6).ok() else { return false; };
    let mut total = 0_u64;
    let mut tids = rustc_hash::FxHashSet::default();
    for taker in takers {
        if !tids.insert(taker.tid) { return false; }
        let mut legs = fills.iter().filter(|f| f.coin == taker.coin && f.tid == taker.tid);
        let (Some(a), Some(b), None) = (legs.next(), legs.next(), legs.next()) else { return false; };
        let maker = if a.crossed && !b.crossed { b } else if b.crossed && !a.crossed { a } else { return false; };
        if maker.twap || maker.oid == taker.oid || maker.sz != taker.sz || maker.px != taker.px { return false; }
        let Some(size) = ExactDecimal::parse(&taker.sz).ok().and_then(|s| s.units(6).ok()) else { return false; };
        let Some(sum) = total.checked_add(size) else { return false; };
        total = sum;
    }
    total > 0 && total == expected
}

#[cfg(test)]
mod tests;
