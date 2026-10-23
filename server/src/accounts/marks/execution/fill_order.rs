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

/// Trigger threshold retained from the triggered node status, including a trailing stop's realized threshold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TriggerPrice {
    coin: Box<str>,
    price: Box<str>,
    direction: crate::listeners::order_book::triggers::Direction,
}

impl TriggerPrice {
    pub(crate) fn of(status: &crate::types::node_data::NodeDataOrderStatus) -> Option<Self> {
        use crate::listeners::order_book::triggers::{TriggerRule, stop_direction};
        let order = &status.order;
        if !order.is_trigger { return None; }
        let price = super::ExactDecimal::parse(&order.trigger_px).ok()?;
        let direction = match (order.order_type.as_str(), TriggerRule::of(&order.trigger_condition)) {
            ("Stop Market" | "Stop Limit" | "Take Profit Market" | "Take Profit Limit", TriggerRule::At(direction)) => {
                let text_price = super::ExactDecimal::parse(order.trigger_condition.rsplit(' ').next()?).ok()?;
                if price != text_price { return None; }
                direction
            }
            // MRNA 1193502103: the triggered status retains 201.8898 (best 206.01, retracement 2%).
            // Use that observed threshold, not a prediction of the future trailing price. The caller still
            // requires one Normal write, a strictly ineligible opening mark and a unique taker/status.
            ("Trailing Stop Market", TriggerRule::Trailing) => stop_direction(order.side),
            // Optional attribution only: unknown/mismatched forms keep the full range, never stop the block.
            _ => return None,
        };
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
pub(crate) fn takers<'a>(fills: &'a [BlockFill], actions: &MarkActions, triggered: &FxHashSet<u64>, replacements: &[(Rank, u64)]) -> FxHashMap<(&'a str, u64), Option<Placement>> {
    let mut created = FxHashMap::default();
    for action in &actions.actions {
        if let MarkAction::Book(book) = &action.action {
            for oid in &book.created {
                created.entry(*oid).and_modify(|rank| *rank = None).or_insert(Some(action.rank));
            }
        }
    }
    for (rank, oid) in replacements {
        created.entry(*oid).and_modify(|rank| *rank = None).or_insert(Some(*rank));
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


/// A triggered order can execute between two ordinary orders of one batch (DRAM 1193759676).
/// The open/triggered/filled stream brackets its entire execution. Receipt-backed opens from the
/// same action prove an action rank even when the trigger was already eligible before setOracle.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct InterleavedTriggers {
    // Every open is a boundary; None prevents crossing an unrecognized/trigger order.
    opens: Vec<Option<(u64, alloy::primitives::Address, u64)>>,
    spans: FxHashMap<u64, Option<(usize, bool)>>,
}

impl InterleavedTriggers {
    pub(super) fn observe(&mut self, status: &crate::types::node_data::NodeDataOrderStatus) {
        let order = &status.order;
        match status.status.as_str() {
            "open" => self.opens.push((order.order_type == "Limit" && !order.is_trigger)
                .then_some((order.oid, status.user, order.timestamp))),
            "triggered" => {
                self.spans.entry(order.oid).and_modify(|span| *span = None)
                    .or_insert(Some((self.opens.len(), false)));
            }
            "filled" => {
                if let Some(span) = self.spans.get_mut(&order.oid) {
                    *span = span.and_then(|(index, filled)| (!filled && index == self.opens.len())
                        .then_some((index, true)));
                }
            }
            _ => {},
        }
    }

    pub(super) fn len(&self) -> usize { self.opens.len() + self.spans.len() }

    pub(super) fn attributed(&self, actions: &MarkActions, triggered: &FxHashSet<u64>) -> FxHashMap<u64, Rank> {
        if self.spans.is_empty() { return FxHashMap::default(); }
        let mut created = FxHashMap::default();
        for action in &actions.actions {
            if let MarkAction::Book(book) = &action.action {
                for oid in &book.created {
                    created.entry(*oid).and_modify(|owner| *owner = None)
                        .or_insert_with(|| action.user.map(|user| (action.rank, user)));
                }
            }
        }
        let mut counts = FxHashMap::default();
        for (oid, _, _) in self.opens.iter().flatten() { *counts.entry(*oid).or_insert(0_usize) += 1; }
        let rank = |open: &Option<(u64, alloy::primitives::Address, u64)>| {
            let (oid, user, time_ms) = (*open)?;
            if time_ms != actions.time_ns / 1_000_000 || triggered.contains(&oid) || counts[&oid] != 1 { return None; }
            let (rank, owner) = created.get(&oid).copied().flatten()?;
            (owner == user).then_some(rank)
        };
        self.spans.iter().filter_map(|(oid, span)| {
            let (index, complete) = (*span)?;
            if !complete || index == 0 { return None; }
            let before = rank(&self.opens[index - 1])?;
            let after = rank(self.opens.get(index)?)?;
            (before == after).then_some((*oid, before))
        }).collect()
    }
}
