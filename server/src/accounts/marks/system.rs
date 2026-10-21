//! `CoreWriter` book effects in the mark replay. Wire rows are execution attempts, not success receipts.
//! Orders are anchored only through unique, full-field status matches; EVM tx hashes are provenance, never fill IDs.
//! Archived client 6911b792: block 0x297e530 drains `ActionDelayer` before signed bundles. `CoreWriter` 0x2dc24c0
//! executes zero-delay operations at the EVM tail. The normal order rule 0x4672d60 delays IOC/GTC by pt.prp.D
//! times `action_delayer.delay_scale`, while all-ALO orders have zero delay. Cancels use their f flag (true: zero; false: D). Timing is read
//! from the checkpoint; an unknown/zero ordinary order delay cannot prove the pre-bundle phase.

use alloy::primitives::{Address, B256};
use rustc_hash::FxHashSet;
use serde::Deserialize;

use super::{actions::{BookAnchors, MarkAction, MarkActions, WireOrder}, decimal::ExactDecimal, execution::BlockStatuses};
use crate::{order_book::Side, types::node_data::NodeDataOrderStatus};

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct SystemAction {
    user: Address,
    evm_tx_hash: Option<B256>,
    action: Action,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
enum Action {
    #[serde(rename = "order")]
    Order { orders: Vec<WireOrder>, grouping: serde_json::Value },
    #[serde(rename = "cancel")]
    Cancel { cancels: Vec<Cancel>, f: Option<bool> },
    #[serde(rename = "cancelByCloid")]
    CancelByCloid { cancels: Vec<CancelByCloid>, f: Option<bool> },
    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, Deserialize)]
struct Cancel { a: u32, o: u64 }
#[derive(Debug, Clone, Deserialize)]
struct CancelByCloid { asset: u32, cloid: String }

#[derive(Debug, Deserialize)]
pub(crate) struct WirePriority { pub(crate) prp: WireDelayParams }
#[derive(Debug, Deserialize)]
pub(crate) struct WireDelayParams { #[serde(rename = "D")] pub(crate) order_delay_s: f64 }
#[derive(Debug, Deserialize)]
pub(crate) struct WireDelayer { pub(crate) delay_scale: f64 }

#[derive(Debug, Clone, PartialEq, Eq)]
struct OrderKey {
    user: Address,
    is_buy: bool,
    limit_px: ExactDecimal,
    sz: ExactDecimal,
    reduce_only: bool,
    cloid: Option<String>,
    tif: String,
}

#[derive(Debug, Clone)]
struct Request { asset: u32, key: OrderKey, delayed: Option<bool> }

#[derive(Debug)]
struct CancelRequest { user: Address, asset: u32, oid: Option<u64>, cloid: Option<String>, delayed: Option<bool> }

#[derive(Debug, Clone)]
struct ObservedOrder { key: OrderKey, coin: String, oid: u64, timestamp_ms: u64, rejected: bool }

#[derive(Debug, Default)]
pub(crate) struct SystemBlock {
    requests: Vec<Request>,
    observed: Vec<ObservedOrder>,
    cancels: Vec<CancelRequest>,
    cancelled: Vec<(Address, String, u64, Option<String>)>,
}

#[derive(Debug, Default)]
pub(crate) struct SystemAnchors {
    pub(crate) before: BookAnchors,
    pub(crate) after: BookAnchors,
    /// A system request may own these runs even when its unique attribution is unresolved.
    pub(crate) competing_ioc_cancels: FxHashSet<u64>,
}

impl SystemBlock {
    pub(crate) fn of(events: impl IntoIterator<Item = SystemAction>, statuses: &[NodeDataOrderStatus]) -> Result<Self, String> {
        let mut out = Self::default();
        for event in events {
            // System transfers also share this stream. They have no book anchor; no synthetic order is created.
            if event.evm_tx_hash.is_none() { continue; }
            match event.action {
                Action::Order { orders, grouping } => {
                    let delayed = (grouping == "na").then(|| !orders.iter().all(WireOrder::is_alo));
                    for order in orders {
                        let Some(limit) = order.t.limit else { continue };
                        if !matches!(limit.tif.as_str(), "Alo" | "Ioc" | "Gtc") { continue; }
                        out.requests.push(Request { asset: order.a, delayed, key: OrderKey {
                            user: event.user, is_buy: order.b,
                            limit_px: ExactDecimal::parse(&order.p)?, sz: ExactDecimal::parse(&order.s)?,
                            reduce_only: order.r, cloid: order.c, tif: limit.tif,
                        }});
                    }
                }
                Action::Cancel { cancels, f } => out.cancels.extend(cancels.into_iter().map(|c| CancelRequest { user: event.user, asset: c.a, oid: Some(c.o), cloid: None, delayed: f.map(|flag| !flag) })),
                Action::CancelByCloid { cancels, f } => out.cancels.extend(cancels.into_iter().map(|c| CancelRequest { user: event.user, asset: c.asset, oid: None, cloid: Some(c.cloid), delayed: f.map(|flag| !flag) })),
                Action::Other => {}
            }
        }
        if out.requests.is_empty() && out.cancels.is_empty() { return Ok(out); }
        for status in statuses {
            let order = &status.order;
            if status.status == "canceled" && out.cancels.iter().any(|c| c.user == status.user
                && (c.oid == Some(order.oid) || c.cloid.is_some() && c.cloid == order.cloid)) {
                out.cancelled.push((status.user, order.coin.clone(), order.oid, order.cloid.clone()));
            }
            if !matches!(status.status.as_str(), "open" | "iocCancelRejected") || order.is_trigger
                || !out.requests.iter().any(|r| r.key.user == status.user && r.key.cloid == order.cloid) { continue; }
            let Some(tif) = &order.tif else { continue };
            out.observed.push(ObservedOrder { coin: order.coin.clone(), oid: order.oid, timestamp_ms: order.timestamp,
                rejected: status.status == "iocCancelRejected", key: OrderKey {
                    user: status.user, is_buy: order.side == Side::Bid,
                    limit_px: ExactDecimal::parse(&order.limit_px)?, sz: ExactDecimal::parse(&order.orig_sz)?,
                    reduce_only: order.reduce_only, cloid: order.cloid.clone(), tif: tif.clone(),
                }});
        }
        Ok(out)
    }

    pub(crate) const fn len(&self) -> usize { self.requests.len() + self.observed.len() + self.cancels.len() + self.cancelled.len() }

    pub(crate) fn anchors(&self, actions: &MarkActions, statuses: &BlockStatuses, timing: Option<(f64, f64)>,
        asset: impl Fn(&str) -> Option<u32>) -> Result<SystemAnchors, String> {
        let mut out = SystemAnchors::default();
        if self.len() == 0 || actions.actions.iter().all(|a| matches!(a.action, MarkAction::Book(_))) { return Ok(out); }
        let books: Vec<_> = actions.actions.iter().filter_map(|a| match &a.action {
            MarkAction::Book(book) => Some((a.user, book)), _ => None,
        }).collect();
        let claimed: FxHashSet<_> = books.iter().flat_map(|(_, b)| b.created.iter().copied()).collect();
        for request in &self.requests {
            for observed in self.observed.iter().filter(|o| o.rejected && Self::matches(request, o, &asset)
                && o.timestamp_ms == actions.time_ns / 1_000_000) {
                out.competing_ioc_cancels.extend(statuses.system_ioc_cancels(observed.oid));
            }
            if self.requests.iter().filter(|r| r.asset == request.asset && r.key == request.key).count() != 1 { continue; }
            let mut matches = self.observed.iter().filter(|o| Self::matches(request, o, &asset)
                && o.timestamp_ms == actions.time_ns / 1_000_000);
            let (Some(observed), None) = (matches.next(), matches.next()) else { continue };
            if claimed.contains(&observed.oid) { continue; }
            // A request without an item receipt may compete for the same status. Do not steal that evidence.
            if books.iter().any(|(user, b)| *user == Some(request.key.user) && (
                b.ioc_pairing_mismatch.is_some() && b.takers.iter().any(|t|
                    (t.asset, t.is_buy, t.limit_px) == (request.asset, request.key.is_buy, request.key.limit_px))
                || b.rejected_iocs.iter().any(|ioc| (ioc.taker.asset, ioc.taker.is_buy, ioc.taker.limit_px, ioc.sz, ioc.reduce_only, &ioc.cloid)
                    == (request.asset, request.key.is_buy, request.key.limit_px, request.key.sz, request.key.reduce_only, &request.key.cloid)))) { continue; }
            if observed.rejected {
                let cancelled = statuses.system_ioc_cancels(observed.oid);
                // A no-fill rejection with no maker cancellation changes no book or trade input.
                if cancelled.is_empty() { continue; }
                out.target(request.delayed, timing)?.cancelled.extend(cancelled);
            } else {
                out.target(request.delayed, timing)?.created.push(observed.oid);
            }
        }
        for (user, coin, oid, cloid) in &self.cancelled {
            let Some(asset_id) = asset(coin) else { continue };
            let mut candidates = self.cancels.iter().filter(|c| c.user == *user && c.asset == asset_id
                && (c.oid == Some(*oid) || c.cloid.is_some() && c.cloid == *cloid));
            let (Some(candidate), None) = (candidates.next(), candidates.next()) else { continue };
            let outcomes = self.cancelled.iter().filter(|(u, coin, o, c)| *u == candidate.user
                && asset(coin) == Some(candidate.asset) && (candidate.oid == Some(*o)
                    || candidate.cloid.is_some() && candidate.cloid == *c)).count();
            if outcomes != 1 { continue; }
            let competing = books.iter().any(|(u, b)| b.cancelled.contains(oid)
                || *u == Some(*user) && cloid.as_ref().is_some_and(|c| b.cancelled_cloids.contains(c)));
            if !competing { out.target(candidate.delayed, timing)?.cancelled.push(*oid); }
        }
        Ok(out)
    }

    fn matches(request: &Request, observed: &ObservedOrder, asset: &impl Fn(&str) -> Option<u32>) -> bool {
        asset(&observed.coin) == Some(request.asset) && observed.key == request.key
    }
}

impl SystemAnchors {
    fn target(&mut self, delayed: Option<bool>, timing: Option<(f64, f64)>) -> Result<&mut BookAnchors, String> {
        match delayed {
            Some(false) => Ok(&mut self.after),
            Some(true) => {
                let Some((delay, scale)) = timing else { return Err("CoreWriter phase requires checkpoint delay parameters".into()); };
                if !delay.is_finite() || !scale.is_finite() || delay <= 0.0 || scale <= 0.0 {
                    return Err("CoreWriter phase is unverified for nonpositive/nonfinite delay parameters".into());
                }
                Ok(&mut self.before)
            }
            None => Err("CoreWriter phase is unverified for this grouping or cancel flag".into()),
        }
    }
}

#[cfg(test)]
mod tests;
