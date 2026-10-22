//! Taker identity and ordering evidence shared by cash and price attribution.
use super::{BlockFill, MarkActions, MarkAction, Rank};
use rustc_hash::{FxHashMap, FxHashSet};

#[derive(Clone, Copy)]
pub(crate) enum Placement {
    BeforeActions,
    Action(Rank),
    /// Trigger observed this block; its creation receipt does not place its later execution.
    Trigger(u64),
}

/// Fixed trigger evidence retained from the node status, not inferred from the fill's side.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TriggerPrice {
    coin: Box<str>,
    price: Box<str>,
    direction: crate::listeners::order_book::triggers::Direction,
}

impl TriggerPrice {
    pub(crate) fn of(status: &crate::types::node_data::NodeDataOrderStatus) -> Option<Self> {
        use crate::listeners::order_book::triggers::TriggerRule;
        let order = &status.order;
        if !order.is_trigger || !matches!(order.order_type.as_str(), "Stop Market" | "Stop Limit" | "Take Profit Market" | "Take Profit Limit") { return None; }
        let TriggerRule::At(direction) = TriggerRule::of(&order.trigger_condition) else { return None; };
        // Optional attribution evidence: unknown forms or conflicting textual/structured thresholds keep the
        // full mark range. They must not withdraw the otherwise valid block or become an invented zero price.
        let price = super::ExactDecimal::parse(&order.trigger_px).ok()?;
        let text_price = super::ExactDecimal::parse(order.trigger_condition.rsplit(' ').next()?).ok()?;
        if price != text_price { return None; }
        Some(Self { coin: order.coin.as_str().into(), price: order.trigger_px.as_str().into(), direction })
    }

    pub(crate) fn crossed_by(&self, coin: &str, before: u64, after: u64, size_decimals: u32) -> bool {
        use crate::listeners::order_book::triggers::Direction;
        if self.coin.as_ref() != coin { return false; }
        let Ok(level) = crate::accounts::marks::seed::exact_price_units(&self.price, size_decimals) else { return false; };
        if level == 0 { return false; }
        // BM TSLA 1193424645 and META 1193428334 trigger on reaching the threshold in either direction.
        // Require the opening price to be strictly ineligible: an already-met trigger has no new-write proof.
        match self.direction {
            Direction::Down => before > level && after <= level,
            Direction::Up => before < level && after >= level,
        }
    }
}

/// A receipt identifies a taker's creation, not an old order's later trigger. Both legs use the same taker.
/// Duplicate identities stay unknown, as do `CoreWriter` and vault-close orders without ordinary book receipts.
pub(crate) fn takers<'a>(fills: &'a [BlockFill], actions: &MarkActions, triggered: &FxHashSet<u64>) -> FxHashMap<(&'a str, u64), Option<Placement>> {
    let mut created = FxHashMap::default();
    for action in &actions.actions {
        if let MarkAction::Book(book) = &action.action {
            for oid in &book.created {
                created.entry(*oid).and_modify(|rank| *rank = None).or_insert(Some(action.rank));
            }
        }
    }
    let mut out = FxHashMap::default();
    for fill in fills.iter().filter(|fill| fill.crossed) {
        let placement = if fill.twap {
            Some(Placement::BeforeActions)
        } else if triggered.contains(&fill.oid) {
            Some(Placement::Trigger(fill.oid))
        } else {
            created.get(&fill.oid).copied().flatten().map(Placement::Action)
        };
        out.entry((fill.coin.as_ref(), fill.tid)).and_modify(|old| *old = None).or_insert(placement);
    }
    out
}
