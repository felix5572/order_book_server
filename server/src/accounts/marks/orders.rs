//! Full-field order identity shared by status-based mark attribution.
//! Asset IDs belong to requests; a status supplies a coin which the seeded book maps to that ID.

use alloy::primitives::Address;
use super::decimal::ExactDecimal;
use crate::accounts::replica::WireOrder;
use crate::{order_book::Side, types::node_data::NodeDataOrderStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OrderKey {
    pub(crate) user: Address,
    pub(crate) is_buy: bool,
    pub(crate) limit_px: ExactDecimal,
    pub(crate) sz: ExactDecimal,
    pub(crate) reduce_only: bool,
    pub(crate) cloid: Option<String>,
    pub(crate) tif: String,
}

impl OrderKey {
    pub(super) fn requested(user: Address, order: &WireOrder) -> Result<Option<Self>, String> {
        let Some(limit) = &order.t.limit else { return Ok(None) };
        if !matches!(limit.tif.as_str(), "Alo" | "Ioc" | "Gtc") { return Ok(None); }
        Ok(Some(Self { user, is_buy: order.b, limit_px: ExactDecimal::parse(&order.p)?,
            sz: ExactDecimal::parse(&order.s)?, reduce_only: order.r, cloid: order.c.clone(), tif: limit.tif.clone() }))
    }

    pub(crate) fn observed(status: &NodeDataOrderStatus) -> Result<Option<Self>, String> {
        let order = &status.order;
        if order.is_trigger { return Ok(None); }
        let Some(tif) = &order.tif else { return Ok(None) };
        if !matches!(tif.as_str(), "Alo" | "Ioc" | "Gtc" | "FrontendMarket") { return Ok(None); }
        Ok(Some(Self { user: status.user, is_buy: order.side == Side::Bid,
            limit_px: ExactDecimal::parse(&order.limit_px)?, sz: ExactDecimal::parse(&order.orig_sz)?,
            reduce_only: order.reduce_only, cloid: order.cloid.clone(), tif: tif.clone() }))
    }
}
