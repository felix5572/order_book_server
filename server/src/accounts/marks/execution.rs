//! Where inside its block each price action runs (design 000439 r1 section 3; port of the research
//! `execution_point_replay.py`, exact for every `SetGlobalAction` and reference action of three intervals).
//!
//! A block's diff lines are in execution order. For a price action P, the other book actions of the block anchor the
//! lines they produced before or after P: the orders they created, the orders they cancelled (a modify's old order
//! included), and, fill by fill, the resting orders their takers hit (fills pair by tid; TWAP slices run before every
//! action). In a block with one
//! `SetGlobalAction`, the orders it triggered run inside it, after its selector. The anchors bound "lines before P" to an
//! interval [lo, hi]; P reads the book at `lo` (the start, research method 3) and the last trade before its fills cut.
//! An anchor of a later action before one of an earlier action contradicts the line order: the block breaks the
//! contract.
//!
//! A reference by cloid names the order holding the cloid when the action runs: a modify keeps the cloid on its new
//! order, so one cloid can name several orders in a block (bm block 1190808326: a batchModify in bundle 0 and another in
//! bundle 1 of the same cloid; resolving through the last cancelled status, as the research did, placed bundle 1's
//! remove before the price action between them). In execution order: the latest order of the block created with the
//! cloid, else the cloid's first `canceled` status (the order resting before the block).
//!
//! A maker's fills match its `update` lines by the size they removed, in order: an order can shrink without a fill (a
//! reduce-only order shrinking with its position: bm block 1190821813, a ZRO order went 33.6 → 24.9 → 12.9 → 6.0 → 0.2
//! with no fill and then 0.2 → 0 with its one fill; the research's "k-th update is the k-th fill" put the first shrink on
//! that fill, after the price action, and the block contradicted itself). A shrink may remove a fill's size too (review
//! 000444 r1 P2-2), so an update is a fill's only when every order-preserving size match puts that fill on it; any other
//! update anchors nothing.

use alloy::primitives::Address;
use rustc_hash::{FxHashMap, FxHashSet};

use super::{
    actions::{MarkAction, MarkActions, Rank, RankedAction},
    book::{BestPrices, BookDiff, DiffKind},
};
use crate::types::node_data::{NodeDataFill, NodeDataOrderStatus};

/// A fill of the block as the anchors read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlockFill {
    pub(crate) coin: Box<str>,
    pub(crate) px: Box<str>,
    pub(crate) sz: Box<str>,
    /// The taker's leg.
    pub(crate) crossed: bool,
    pub(crate) oid: u64,
    pub(crate) tid: u64,
    /// A TWAP slice: it runs at the start of the block.
    pub(crate) twap: bool,
}

impl BlockFill {
    pub(crate) fn of(NodeDataFill(_, fill): &NodeDataFill) -> Self {
        Self {
            coin: fill.coin.as_str().into(),
            px: fill.px.as_str().into(),
            sz: fill.sz.as_str().into(),
            crossed: fill.crossed,
            oid: fill.oid,
            tid: fill.tid,
            twap: fill.twap_id.is_some(),
        }
    }
}

/// What the block's order statuses tell the anchors.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BlockStatuses {
    /// The first `canceled` status of each (user, cloid): the order that held the cloid before the block.
    pub(crate) cancelled_cloids: FxHashMap<(Address, String), u64>,
    /// `triggered` statuses.
    pub(crate) triggered: FxHashSet<u64>,
}

impl BlockStatuses {
    pub(crate) fn of(statuses: impl IntoIterator<Item = NodeDataOrderStatus>) -> Self {
        let mut out = Self::default();
        for status in statuses {
            match (status.status.as_str(), status.order.cloid) {
                ("canceled", Some(cloid)) => {
                    out.cancelled_cloids.entry((status.user, cloid)).or_insert(status.order.oid);
                }
                ("triggered", _) => {
                    out.triggered.insert(status.order.oid);
                }
                _ => {}
            }
        }
        out
    }

    pub(crate) fn len(&self) -> usize {
        self.cancelled_cloids.len() + self.triggered.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Before,
    After,
}

/// The cut of a price action: diff lines `[0, lo)` and fills `[0, fills_lo)` ran before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Cut {
    pub(crate) lo: usize,
    pub(crate) hi: usize,
    pub(crate) fills_lo: usize,
}

/// One price action of the block, in execution order, with its cut.
#[derive(Debug)]
pub(crate) struct PricePoint<'b> {
    pub(crate) action: &'b RankedAction,
    pub(crate) cut: Cut,
}

/// The price actions of a block in execution order, each with its cut.
pub(crate) fn price_points<'b>(
    actions: &'b MarkActions,
    statuses: &BlockStatuses,
    diffs: &[BookDiff],
    fills: &[BlockFill],
) -> Result<Vec<PricePoint<'b>>, String> {
    let mut points: Vec<&RankedAction> = actions.actions.iter().filter(|action| !matches!(action.action, MarkAction::Book(_))).collect();
    if points.is_empty() {
        return Ok(Vec::new());
    }
    points.sort_by_key(|action| action.rank);
    let set_globals: Vec<Rank> =
        points.iter().filter(|action| matches!(action.action, MarkAction::SetGlobal(_))).map(|action| action.rank).collect();
    let cancelled = cancelled_by_action(actions, statuses);
    points
        .into_iter()
        .map(|action| {
            let anchors = Anchors::around(action.rank, &cancelled, statuses, &set_globals);
            let cut = anchors.cut(diffs, fills).map_err(|err| format!("block {} action at {:?}: {err}", actions.height, action.rank))?;
            Ok(PricePoint { action, cut })
        })
        .collect()
}

/// A book action of the block with every order it removed, its cloid references resolved.
struct Removing<'b> {
    rank: Rank,
    created: &'b [u64],
    cancelled: Vec<u64>,
}

/// Every book action's removed orders, by cloid as well, resolving the cloids in execution order.
fn cancelled_by_action<'b>(actions: &'b MarkActions, statuses: &BlockStatuses) -> Vec<Removing<'b>> {
    let mut books: Vec<(&RankedAction, &super::actions::BookAnchors)> = actions
        .actions
        .iter()
        .filter_map(|action| match &action.action {
            MarkAction::Book(book) => Some((action, book)),
            _ => None,
        })
        .collect();
    books.sort_by_key(|(action, _)| action.rank);
    let mut held: FxHashMap<(Address, &str), u64> = FxHashMap::default();
    let mut out = Vec::with_capacity(books.len());
    for (action, book) in books {
        let mut cancelled = book.cancelled.clone();
        if let Some(user) = action.user {
            for cloid in &book.cancelled_cloids {
                let holder = held.get(&(user, cloid.as_str())).or_else(|| statuses.cancelled_cloids.get(&(user, cloid.clone())));
                cancelled.extend(holder);
            }
            for (cloid, oid) in &book.created_cloids {
                held.insert((user, cloid.as_str()), *oid);
            }
        }
        out.push(Removing { rank: action.rank, created: &book.created, cancelled });
    }
    out
}

/// The other book actions of the block, before or after one price action.
#[derive(Default)]
struct Anchors<'s> {
    created: [FxHashSet<u64>; 2],
    cancelled: [FxHashSet<u64>; 2],
    /// The orders the block's one `SetGlobalAction` triggered, and their side.
    triggered: Option<(&'s FxHashSet<u64>, Side)>,
}

const fn index(side: Side) -> usize {
    match side {
        Side::Before => 0,
        Side::After => 1,
    }
}

impl<'s> Anchors<'s> {
    fn around(rank: Rank, books: &[Removing<'_>], statuses: &'s BlockStatuses, set_globals: &[Rank]) -> Self {
        let mut anchors = Self::default();
        for other in books {
            let side = index(if other.rank < rank { Side::Before } else { Side::After });
            anchors.created[side].extend(other.created);
            anchors.cancelled[side].extend(&other.cancelled);
        }
        // the orders a lone SetGlobalAction triggers run inside it, after its selector, even when an earlier action of
        // the block placed them (bm block 1189996766)
        if let [set_global] = set_globals {
            anchors.triggered = Some((&statuses.triggered, if *set_global < rank { Side::Before } else { Side::After }));
        }
        anchors
    }

    fn sides_of(sets: &[FxHashSet<u64>; 2], oid: u64) -> Vec<Side> {
        [Side::Before, Side::After].into_iter().filter(|side| sets[index(*side)].contains(&oid)).collect()
    }

    fn triggered_side(&self, oid: u64) -> Option<Side> {
        self.triggered.filter(|(triggered, _)| triggered.contains(&oid)).map(|(_, side)| side)
    }

    /// Where a taker leg's action runs.
    fn taker_sides(&self, taker: &BlockFill) -> Vec<Side> {
        if taker.twap {
            return vec![Side::Before];
        }
        if let Some(side) = self.triggered_side(taker.oid) {
            return vec![side];
        }
        Self::sides_of(&self.created, taker.oid)
    }

    fn cut(&self, diffs: &[BookDiff], fills: &[BlockFill]) -> Result<Cut, String> {
        let (lo, hi) = interval(self.line_sides(diffs, fills).into_iter().enumerate())
            .map_err(|(lo, hi)| format!("diff anchors contradict the line order: before up to {lo}, after from {hi}"))?;
        let takers: FxHashMap<u64, Option<Side>> = fills
            .iter()
            .filter(|fill| fill.crossed)
            .map(|fill| (fill.tid, single(self.taker_sides(fill))))
            .collect();
        let (fills_lo, _) = interval(fills.iter().map(|fill| takers.get(&fill.tid).copied().flatten()).enumerate())
            .map_err(|(lo, hi)| format!("fill anchors contradict the fill order: before up to {lo}, after from {hi}"))?;
        Ok(Cut { lo, hi, fills_lo })
    }

    /// The side of each diff line, `None` when nothing places it.
    fn line_sides(&self, diffs: &[BookDiff], fills: &[BlockFill]) -> Vec<Option<Side>> {
        let fill_sides = self.fill_sides(diffs, fills);
        let mut last_fill_side: FxHashMap<u64, Option<Side>> = FxHashMap::default();
        let mut zeroed = FxHashSet::default();
        diffs
            .iter()
            .enumerate()
            .map(|(line, diff)| {
                let oid = diff.oid();
                if let Some(side) = self.triggered_side(oid) {
                    return Some(side);
                }
                match diff.kind() {
                    DiffKind::New => single(Self::sides_of(&self.created, oid)),
                    DiffKind::Update { to_zero } => {
                        let side = fill_sides.get(&line).copied().flatten();
                        last_fill_side.insert(oid, side);
                        if to_zero {
                            zeroed.insert(oid);
                        }
                        side
                    }
                    // the remove right after a fill to zero takes that fill's side
                    DiffKind::Remove if zeroed.contains(&oid) => last_fill_side.get(&oid).copied().flatten(),
                    DiffKind::Remove => single(Self::sides_of(&self.cancelled, oid)),
                }
            })
            .collect()
    }

    /// {update line: side of the fill it is}, for the updates a maker's fill determines.
    fn fill_sides(&self, diffs: &[BookDiff], fills: &[BlockFill]) -> FxHashMap<usize, Option<Side>> {
        let mut updates: FxHashMap<u64, Vec<(usize, f64)>> = FxHashMap::default();
        for (line, diff) in diffs.iter().enumerate() {
            let removed = diff.update_sizes().and_then(|(from, to)| Some(from.parse::<f64>().ok()? - to.parse::<f64>().ok()?));
            if let Some(removed) = removed {
                updates.entry(diff.oid()).or_default().push((line, removed));
            }
        }
        let mut sides = FxHashMap::default();
        for (oid, fills) in self.maker_fills(fills) {
            let Some(updates) = updates.get(&oid) else { continue };
            let removed: Vec<f64> = updates.iter().map(|(_, removed)| *removed).collect();
            let filled: Vec<f64> = fills.iter().map(|(filled, _)| *filled).collect();
            for (fill, update) in determined_matches(&removed, &filled) {
                sides.insert(updates[update].0, fills[fill].1);
            }
        }
        sides
    }

    /// {maker oid: (size, side) of each of its fills, in block order}: the side is its taker's, `None` when unplaced.
    fn maker_fills(&self, fills: &[BlockFill]) -> FxHashMap<u64, Vec<(f64, Option<Side>)>> {
        let takers: FxHashMap<u64, &BlockFill> = fills.iter().filter(|fill| fill.crossed).map(|fill| (fill.tid, fill)).collect();
        let mut out: FxHashMap<u64, Vec<(f64, Option<Side>)>> = FxHashMap::default();
        for maker in fills.iter().filter(|fill| !fill.crossed) {
            let Ok(filled) = maker.sz.parse::<f64>() else { continue };
            let side = takers.get(&maker.tid).and_then(|taker| single(self.taker_sides(taker)));
            out.entry(maker.oid).or_default().push((filled, side));
        }
        out
    }
}

/// (fill, update) index pairs that every order-preserving match of `filled` into `removed` by size shares: the earliest
/// and the latest match agree on that fill. Sizes are printed through floats: equal within 1e-9 of the fill. Empty when
/// no match exists.
fn determined_matches(removed: &[f64], filled: &[f64]) -> Vec<(usize, usize)> {
    let same = |update: usize, fill: usize| (filled[fill] - removed[update]).abs() <= 1e-9 * filled[fill].max(1.0);
    let mut earliest = Vec::with_capacity(filled.len());
    for update in 0..removed.len() {
        if earliest.len() < filled.len() && same(update, earliest.len()) {
            earliest.push(update);
        }
    }
    if earliest.len() < filled.len() {
        return Vec::new();
    }
    let mut latest = vec![0; filled.len()];
    let mut fill = filled.len();
    for update in (0..removed.len()).rev() {
        if fill > 0 && same(update, fill - 1) {
            fill -= 1;
            latest[fill] = update;
        }
    }
    earliest.into_iter().zip(latest).enumerate().filter(|(_, (first, last))| first == last).map(|(fill, (update, _))| (fill, update)).collect()
}

fn single(sides: Vec<Side>) -> Option<Side> {
    match sides.as_slice() {
        [side] => Some(*side),
        _ => None,
    }
}

/// [lo, hi]: lo after the last item before, hi at the first item after; `Err((lo, hi))` when they cross.
fn interval(sides: impl Iterator<Item = (usize, Option<Side>)>) -> Result<(usize, usize), (usize, usize)> {
    let (mut lo, mut hi) = (0, usize::MAX);
    let mut count = 0;
    for (position, side) in sides {
        count = position + 1;
        match side {
            Some(Side::Before) => lo = lo.max(position + 1),
            Some(Side::After) => hi = hi.min(position),
            None => {}
        }
    }
    let hi = hi.min(count);
    if lo > hi { Err((lo, hi)) } else { Ok((lo, hi)) }
}

/// The best prices of each asset after each diff line that moved it: `at(asset, cut)` is the book after lines
/// `[0, cut)`.
#[derive(Debug, Default)]
pub(crate) struct BookCuts {
    /// Per asset with lines in the block: (lines applied, best prices), the first entry at 0.
    moves: FxHashMap<usize, Vec<(usize, BestPrices)>>,
}

impl BookCuts {
    /// Records that line `line` moved `asset` from `before` to `after`.
    pub(crate) fn record(&mut self, asset: usize, line: usize, before: BestPrices, after: BestPrices) {
        self.moves.entry(asset).or_insert_with(|| vec![(0, before)]).push((line + 1, after));
    }

    /// The asset's best prices after `cut` lines; `current` when no line of the block moved it.
    pub(crate) fn at(&self, asset: usize, cut: usize, current: BestPrices) -> BestPrices {
        let Some(moves) = self.moves.get(&asset) else {
            return current;
        };
        let after = moves.partition_point(|(lines, _)| *lines <= cut);
        moves[after - 1].1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::marks::{
        actions::{BookAnchors, RankedAction},
        book::tests::diff_line,
    };

    fn diff(oid: u64, raw: &str) -> BookDiff {
        BookDiff::of(&diff_line(1, "BTC", oid, "B", "82850.0", raw).events()[0]).unwrap()
    }

    fn book(rank: Rank, anchors: BookAnchors) -> RankedAction {
        RankedAction { rank, user: Some(Address::repeat_byte(1)), action: MarkAction::Book(anchors) }
    }

    fn set_global(rank: Rank) -> RankedAction {
        RankedAction { rank, user: None, action: MarkAction::SetGlobal(Vec::new()) }
    }

    fn fill(tid: u64, oid: u64, crossed: bool) -> BlockFill {
        BlockFill { coin: "BTC".into(), px: "82850.0".into(), sz: "0.002".into(), crossed, oid, tid, twap: false }
    }

    const NEW: &str = r#"{"new":{"sz":"0.001"}}"#;
    const REMOVE: &str = r#""remove""#;

    /// An order created before the price action, one cancelled after, and a maker hit by an earlier taker: the cut
    /// sits between them.
    #[test]
    fn the_anchors_bound_the_cut() {
        let created = BookAnchors { created: vec![10], ..BookAnchors::default() };
        let taker = BookAnchors { created: vec![11], ..BookAnchors::default() };
        let cancel = BookAnchors { cancelled: vec![12], ..BookAnchors::default() };
        let actions = MarkActions {
            height: 1,
            time_ns: 1,
            actions: vec![book((0, 1, 0), created), book((0, 3, 1), taker), set_global((1, 3, 0)), book((2, 2, 0), cancel)],
        };
        let diffs = [
            diff(10, NEW),
            diff(20, r#"{"update":{"origSz":"0.002","newSz":"0.0"}}"#),
            diff(20, REMOVE),
            diff(12, REMOVE),
            diff(13, NEW),
        ];
        let fills = [fill(1, 11, true), fill(1, 20, false)];
        let points = price_points(&actions, &BlockStatuses::default(), &diffs, &fills).unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].cut, Cut { lo: 3, hi: 3, fills_lo: 2 }, "after the maker's remove, before the cancel");
    }

    /// Block 1189996766: an order placed earlier in the block and triggered by the lone `SetGlobalAction` runs after its
    /// selector, not where it was placed.
    #[test]
    fn a_triggered_order_runs_inside_the_lone_set_global_action() {
        let placed = BookAnchors { created: vec![30], ..BookAnchors::default() };
        let actions = MarkActions { height: 1, time_ns: 1, actions: vec![book((0, 3, 0), placed), set_global((1, 3, 0))] };
        let statuses = BlockStatuses { triggered: std::iter::once(30).collect(), ..BlockStatuses::default() };
        let diffs = [diff(31, NEW), diff(30, NEW)];
        let points = price_points(&actions, &statuses, &diffs, &[]).unwrap();
        assert_eq!((points[0].cut.lo, points[0].cut.hi), (0, 1));
        let without = price_points(&actions, &BlockStatuses::default(), &diffs, &[]).unwrap();
        assert_eq!(without[0].cut.lo, 2, "as the placing action's");
    }

    #[test]
    fn a_cancel_by_cloid_resolves_through_the_statuses_and_a_contradiction_fails() {
        let cancel = BookAnchors { cancelled_cloids: vec!["0x0a".to_string()], ..BookAnchors::default() };
        let created = BookAnchors { created: vec![41], ..BookAnchors::default() };
        let actions = MarkActions { height: 7, time_ns: 1, actions: vec![book((0, 2, 0), cancel), set_global((1, 3, 0)), book((2, 1, 0), created)] };
        let statuses = BlockStatuses {
            cancelled_cloids: std::iter::once(((Address::repeat_byte(1), "0x0a".to_string()), 40)).collect(),
            ..BlockStatuses::default()
        };
        let points = price_points(&actions, &statuses, &[diff(40, REMOVE), diff(41, NEW)], &[]).unwrap();
        assert_eq!((points[0].cut.lo, points[0].cut.hi), (1, 1));
        let error = price_points(&actions, &statuses, &[diff(41, NEW), diff(40, REMOVE)], &[]).unwrap_err();
        assert!(error.contains("block 7 action at (1, 3, 0): diff anchors contradict the line order"), "{error}");
    }

    /// bm block 1190808326: two modifies by one cloid, one before and one after the price action. Each names the order
    /// holding the cloid when it runs (the first the order resting before the block, the second the first's new order).
    #[test]
    fn a_cloid_names_the_order_holding_it_when_the_action_runs() {
        let user = Address::repeat_byte(1);
        let modify = |rank, new_oid| {
            book(rank, BookAnchors {
                created: vec![new_oid],
                created_cloids: vec![("0xc1".to_string(), new_oid)],
                cancelled_cloids: vec!["0xc1".to_string()],
                ..BookAnchors::default()
            })
        };
        let point = RankedAction { rank: (0, 3, 1), user: None, action: MarkAction::Reference { oracle: vec![], external: vec![] } };
        let actions = MarkActions { height: 1, time_ns: 1, actions: vec![modify((0, 1, 0), 2), point, modify((1, 1, 0), 3)] };
        let mut statuses = BlockStatuses::default();
        // the cancelled statuses in stream order: 1, then 2; the first held the cloid before the block
        statuses.cancelled_cloids.insert((user, "0xc1".to_string()), 1);
        let diffs = [diff(1, REMOVE), diff(2, NEW), diff(2, REMOVE), diff(3, NEW)];
        let points = price_points(&actions, &statuses, &diffs, &[]).unwrap();
        assert_eq!((points[0].cut.lo, points[0].cut.hi), (2, 2), "the second modify's remove of 2 runs after");
    }

    /// bm block 1190821813: a maker shrinks without a fill before its one fill, whose taker runs after the price
    /// action; only the update that removed the fill's size takes the fill's side.
    #[test]
    fn an_update_takes_the_fill_that_removed_its_size() {
        let taker = BookAnchors { created: vec![50], ..BookAnchors::default() };
        let actions = MarkActions { height: 1, time_ns: 1, actions: vec![set_global((0, 3, 0)), book((1, 3, 0), taker)] };
        let diffs = [diff(20, r#"{"update":{"origSz":"0.009","newSz":"0.004"}}"#), diff(20, r#"{"update":{"origSz":"0.004","newSz":"0.002"}}"#)];
        let fills = [fill(7, 50, true), fill(7, 20, false)];
        let points = price_points(&actions, &BlockStatuses::default(), &diffs, &fills).unwrap();
        assert_eq!((points[0].cut.lo, points[0].cut.hi), (0, 1), "the shrink by 0.005 anchors nothing, the fill of 0.002 is after");
    }

    /// Review 000444 r1 P2-2: a TWAP shrinks reduce-only order 20 by 0.002 before ALO order 10 rests and the price action
    /// runs; taker 50 then fills order 20 for 0.002 after it. Either update may be the fill: neither anchors, so the block
    /// holds together.
    #[test]
    fn a_shrink_of_a_fills_size_leaves_both_updates_unplaced() {
        let alo = BookAnchors { created: vec![10], ..BookAnchors::default() };
        let taker = BookAnchors { created: vec![50], ..BookAnchors::default() };
        let actions =
            MarkActions { height: 1, time_ns: 1, actions: vec![book((0, 1, 0), alo), set_global((1, 3, 0)), book((2, 3, 0), taker)] };
        let diffs = [
            diff(20, r#"{"update":{"origSz":"0.006","newSz":"0.004"}}"#),
            diff(10, r#"{"new":{"sz":"0.001"}}"#),
            diff(20, r#"{"update":{"origSz":"0.004","newSz":"0.002"}}"#),
        ];
        let fills = [fill(7, 50, true), fill(7, 20, false)];
        let points = price_points(&actions, &BlockStatuses::default(), &diffs, &fills).unwrap();
        assert_eq!((points[0].cut.lo, points[0].cut.hi), (2, 3), "after order 10's line, the rest open");
    }

    #[test]
    fn a_fill_is_placed_only_where_every_match_puts_it() {
        assert_eq!(determined_matches(&[0.005, 0.002], &[0.002]), [(0, 1)], "one update of that size");
        assert_eq!(determined_matches(&[0.002, 0.002], &[0.002]), [], "either update");
        assert_eq!(determined_matches(&[0.002, 0.002], &[0.002, 0.002]), [(0, 0), (1, 1)], "both, in order");
        assert_eq!(determined_matches(&[0.002, 0.003, 0.002], &[0.003, 0.002]), [(0, 1), (1, 2)], "the order fixes them");
        assert_eq!(determined_matches(&[0.003], &[0.002]), [], "no match at all");
    }

    #[test]
    fn book_cuts_read_the_book_after_a_cut() {
        let mut cuts = BookCuts::default();
        cuts.record(3, 1, (Some(10), Some(12)), (Some(11), Some(12)));
        cuts.record(3, 4, (Some(11), Some(12)), (Some(11), None));
        assert_eq!(cuts.at(3, 0, (Some(11), None)), (Some(10), Some(12)));
        assert_eq!(cuts.at(3, 2, (Some(11), None)), (Some(11), Some(12)));
        assert_eq!(cuts.at(3, 5, (Some(11), None)), (Some(11), None));
        assert_eq!(cuts.at(4, 2, (Some(1), Some(2))), (Some(1), Some(2)), "no line of the block");
    }
}
