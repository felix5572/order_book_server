//! A successful single modify can return only `default`, omitting its new oid (BM ZEC 1191964185).
//! Its status sequence supplies the missing identity: open(new), canceled(old), followed by fills/statuses.
//! Require that adjacency, full requested fields, old oid/cloid, current-block timestamp and a one-to-one
//! request/pair match. Oids already claimed by an ordinary receipt or `CoreWriter` remain with that owner.
//! A missing/ambiguous pair contributes no anchor, without turning a normal default receipt into an error.

use super::super::{actions::{MarkAction, MarkActions, OrderRef, Rank, ReplacementRequest}, orders::OrderKey};
use crate::types::node_data::NodeDataOrderStatus;

#[derive(Debug, Clone, PartialEq, Eq)]
struct OpenOrder { key: OrderKey, coin: String, oid: u64, timestamp_ms: u64 }
#[derive(Debug, Clone, PartialEq, Eq)]
struct Pair { new: OpenOrder, old_oid: u64, old_cloid: Option<String> }

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct Replacements {
    pending: Option<OpenOrder>,
    pairs: Vec<Pair>,
}

impl Replacements {
    pub(super) fn observe(&mut self, status: &NodeDataOrderStatus) -> Result<(), String> {
        let order = &status.order;
        // Every status is a boundary, including an unrecognized status; never skip over one to create adjacency.
        if let Some(new) = self.pending.take()
            && status.status == "canceled" && status.user == new.key.user && order.coin == new.coin && order.oid != new.oid {
            self.pairs.push(Pair { new, old_oid: order.oid, old_cloid: order.cloid.clone() });
        }
        if status.status != "open" || order.order_type != "Limit" { return Ok(()); }
        let Some(key) = OrderKey::observed(status)? else { return Ok(()) };
        self.pending = Some(OpenOrder { key, coin: order.coin.clone(), oid: order.oid, timestamp_ms: order.timestamp });
        Ok(())
    }

    pub(super) fn len(&self) -> usize { self.pairs.len() + usize::from(self.pending.is_some()) }

    pub(super) fn attributed(&self, actions: &MarkActions, asset: &impl Fn(&str) -> Option<u32>, system_claims: &impl Fn(u64) -> bool) -> Vec<(Rank, u64)> {
        if self.pairs.is_empty() { return Vec::new(); }
        let books: Vec<_> = actions.actions.iter().filter_map(|a| match &a.action {
            MarkAction::Book(book) => Some((a.rank, book)), _ => None,
        }).collect();
        let requests: Vec<_> = books.iter().filter_map(|(rank, b)| b.unreceipted_modify.as_ref().map(|r| (*rank, r))).collect();
        if requests.is_empty() { return Vec::new(); }
        let mut out = Vec::new();
        for pair in &self.pairs {
            if pair.new.timestamp_ms != actions.time_ns / 1_000_000
                || books.iter().any(|(_, b)| b.created.contains(&pair.new.oid))
                || system_claims(pair.new.oid) { continue; }
            let matches = |request: &ReplacementRequest, pair: &Pair| {
                request.key == pair.new.key && asset(&pair.new.coin) == Some(request.asset)
                    && match &request.old { OrderRef::Oid(oid) => *oid == pair.old_oid,
                        OrderRef::Cloid(cloid) => pair.old_cloid.as_ref() == Some(cloid) }
            };
            let mut candidates = requests.iter().filter(|(_, request)| matches(request, pair));
            let (Some((rank, request)), None) = (candidates.next(), candidates.next()) else { continue };
            if self.pairs.iter().filter(|other| matches(request, other)
                && other.new.timestamp_ms == actions.time_ns / 1_000_000).count() != 1 { continue; }
            out.push((*rank, pair.new.oid));
        }
        out
    }
}

#[cfg(test)]
mod tests;
