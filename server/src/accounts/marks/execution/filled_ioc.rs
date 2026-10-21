//! Successful IOC status runs: open(taker), zero or more filled makers, self-trade cancels,
//! filled(the same taker). BM PROVE 1192106175 and PURR 1192117608 show that the cancels
//! can follow the maker book updates, immediately before the next bundle's price action.
//! Require both ends, matching order identity, actual paired fills and one receipt owner.
//! Partial/interrupted runs stay unplaced; neither maker age nor shared vault ownership is guessed.

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
                    makers: Vec::new(), cancelled: Vec::new(), complete: false });
            }
            return Ok(());
        }
        let Some(mut run) = self.pending.take() else { return Ok(()) };
        if status.status == "filled" && order.oid == run.taker.oid {
            run.complete = order.order_type == "Limit" && order.coin == run.taker.coin.as_ref()
                && order.timestamp == run.timestamp_ms && OrderKey::observed(status)?.as_ref() == Some(&run.key)
                && ExactDecimal::parse(&order.sz)? == ExactDecimal::from_units(0, 0);
            self.keep(run);
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
        if !run.cancelled.is_empty() { self.runs.push(run); }
    }

    pub(super) fn contains_cancel(&self, oid: u64) -> bool {
        self.runs.iter().chain(self.pending.iter()).any(|run| run.cancelled.contains(&oid))
    }

    pub(super) fn len(&self) -> usize {
        self.heads.len() + self.runs.iter().chain(self.pending.iter())
            .map(|run| 1 + run.makers.len() + run.cancelled.len()).sum::<usize>()
    }

    pub(super) fn attributed(&self, actions: &MarkActions, fills: &[BlockFill],
        asset: &impl Fn(&str) -> Option<u32>, system: &SystemAnchors) -> Vec<(Rank, Vec<u64>)> {
        if self.runs.is_empty() { return Vec::new(); }
        let mut out = Vec::new();
        for run in &self.runs {
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
            out.push((owner.rank, run.cancelled.clone()));
        }
        out
    }
}

#[cfg(test)]
mod tests;
