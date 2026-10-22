//! Receipt-backed ordinary taker placement shared by cash and price attribution.
use super::{BlockFill, MarkActions, MarkAction, Rank};
use rustc_hash::{FxHashMap, FxHashSet};

#[derive(Clone, Copy)]
pub(crate) enum Placement {
    BeforeActions,
    Action(Rank),
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
            None
        } else {
            created.get(&fill.oid).copied().flatten().map(Placement::Action)
        };
        out.entry((fill.coin.as_ref(), fill.tid)).and_modify(|old| *old = None).or_insert(placement);
    }
    out
}
