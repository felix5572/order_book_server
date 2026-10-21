//! In the archived client observations, the no-fill IOC path emits the taker status before its maker cancellations (bm 6911b792, the three goldens).
//! Keep only a contiguous, price-compatible run; another status closes it. Match the head to exactly one request
//! with the matching no-fill response and exactly one status head, including heads with no cancellations. This
//! prevents duplicate requests or an indistinguishable system/TWAP rejection from being assigned arbitrarily.
//! Resting-order age and vault ownership are irrelevant to this event association. Other cancellation paths retain
//! the existing conservative attribution; adjacency alone, or just a matching account/price, is insufficient.

use super::{StatusOrder, OrderSide};
use crate::{accounts::marks::{actions::{MarkAction, MarkActions, Rank, RejectedIoc, Taker}, decimal::ExactDecimal}, types::node_data::NodeDataOrderStatus};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct IocRejections {
    groups: Vec<Rejection>,
    current: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Rejection {
    taker: StatusOrder,
    sz: ExactDecimal,
    reduce_only: bool,
    cloid: Option<String>,
    timestamp_ms: u64,
    cancelled: Vec<u64>,
}

impl Rejection {
    fn same_request(&self, other: &Self) -> bool {
        self.taker.user == other.taker.user && self.taker.coin == other.taker.coin
            && self.taker.side == other.taker.side && self.taker.limit_px == other.taker.limit_px
            && self.sz == other.sz && self.reduce_only == other.reduce_only && self.cloid == other.cloid
            && self.timestamp_ms == other.timestamp_ms
    }

    fn matches(&self, ioc: &RejectedIoc, asset: u32) -> bool {
        self.matches_taker(&ioc.taker, asset) && ioc.sz == self.sz
            && ioc.reduce_only == self.reduce_only && ioc.cloid == self.cloid
    }

    fn matches_taker(&self, taker: &Taker, asset: u32) -> bool {
        taker.asset == asset && taker.is_buy == (self.taker.side == OrderSide::Bid)
            && taker.limit_px == self.taker.limit_px
    }
}

impl IocRejections {
    pub(super) fn observe(&mut self, status: &NodeDataOrderStatus) -> Result<(), String> {
        if status.status == "selfTradeCanceled" {
            if let Some(index) = self.current {
                let group = &mut self.groups[index];
                let cancel = StatusOrder::of(status)?;
                if cancel.coin == group.taker.coin && cancel.side != group.taker.side && cancel.reached_by(group.taker.limit_px) {
                    group.cancelled.push(cancel.oid);
                } else {
                    self.current = None;
                }
            }
            return Ok(());
        }
        self.current = None;
        let order = &status.order;
        if status.status != "iocCancelRejected" || order.is_trigger || order.tif.as_deref() != Some("Ioc") {
            return Ok(());
        }
        let sz = ExactDecimal::parse(&order.orig_sz).map_err(|err| format!("IOC rejection oid {} original size: {err}", order.oid))?;
        if ExactDecimal::parse(&order.sz)? != sz {
            return Ok(());
        }
        self.current = Some(self.groups.len());
        self.groups.push(Rejection {
            taker: StatusOrder::of(status)?, sz, reduce_only: order.reduce_only, cloid: order.cloid.clone(),
            timestamp_ms: order.timestamp, cancelled: Vec::new(),
        });
        Ok(())
    }

    pub(super) fn contains_cancel(&self, oid: u64) -> bool {
        self.groups.iter().any(|group| group.cancelled.contains(&oid))
    }

    pub(super) fn cancels_for(&self, oid: u64) -> Vec<u64> {
        let mut groups = self.groups.iter().filter(|g| g.taker.oid == oid);
        match (groups.next(), groups.next()) {
            (Some(group), None) => group.cancelled.clone(),
            _ => Vec::new(),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.groups.iter().map(|group| 1 + group.cancelled.len()).sum()
    }

    pub(super) fn attributed(&self, actions: &MarkActions, order_asset: &impl Fn(&str) -> Option<u32>) -> Vec<(Rank, Vec<u64>)> {
        let mut out = Vec::new();
        for group in &self.groups {
            if group.cancelled.is_empty() || group.timestamp_ms != actions.time_ns / 1_000_000 {
                continue;
            }
            if self.groups.iter().filter(|other| group.same_request(other)).count() != 1 {
                continue;
            }
            let Some(asset) = order_asset(&group.taker.coin) else { continue };
            // An unpaired response cannot identify a rejection, but its request can still compete with another
            // action for this status head. Ignoring it must not make that other action look uniquely identified.
            if actions.actions.iter().any(|action| action.user == Some(group.taker.user)
                && matches!(&action.action, MarkAction::Book(book) if book.ioc_pairing_mismatch.is_some()
                    && book.takers.iter().any(|taker| group.matches_taker(taker, asset))))
            {
                continue;
            }
            let mut candidates = actions.actions.iter().filter(|action| action.user == Some(group.taker.user)).flat_map(|action| {
                let requests = match &action.action { MarkAction::Book(book) => book.rejected_iocs.as_slice(), _ => &[] };
                requests.iter().filter(|ioc| group.matches(ioc, asset)).map(move |_| action.rank)
            });
            if let (Some(rank), None) = (candidates.next(), candidates.next()) {
                out.push((rank, group.cancelled.clone()));
            }
        }
        out
    }
}
