//! Takers lately refused for margin (batch 2g, hyperliquid-trade design 000621 r3): HL refuses an account's taker order
//! it cannot margin, and the account's next one on the coin and side mostly the same way (research
//! `mempool_pending_book_20261031`, bm 10-31 19:41-20:11, by the block responses' account: of the next taker orders in a
//! later block, 97.2% refused again within 5 s, 98.4% within 300 s). A pending taker order of an identity whose last
//! outcome on its coin and side, as of the confirmed height, was a margin refusal at block time `t_r` is taken to be
//! refused when its line came at `t_r` up to `t_r + M`.
//!
//! - A block's outcomes wait until the replica confirms its height, then apply in block order, each block in its
//!   execution order: the last one of a key stands. A refusal sets its block time; a fill, a resting order or an IOC
//!   missing every level clears it - an end of the inference, not a proof the next one is margined.
//! - The block log's gaps end it: their blocks', and those taken after a loss, never apply (`clear`).
//! - Batch 2p: a reduce-only taker order refused as it "would increase position" the same way, for the identity's next
//!   reduce-only taker order on the coin and side alone. Any order placed clears the margin inference, as before; a
//!   reduce-only one placed - filled, resting, or an IOC missing every level - clears the reduce-only one too. The rule
//!   as run here (`real_data`: bm 11-02 09:57-11:09's block log, contiguous, the confirmed height 2 blocks behind, each
//!   order judged at its block's time): of 3,742 reduce-only orders judged refused so, 3,513 were (93.9%), the 229
//!   others placed - an IOC missing every level among them -; of 30,960 orders judged refused for margin, 30,205
//!   (97.6%).

use std::collections::BTreeMap;

use rustc_hash::{FxHashMap, FxHashSet};

use super::signer::ActingIdentity;

/// A taker's identity, asset and side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TakerKey {
    pub(crate) identity: ActingIdentity,
    pub(crate) asset: u32,
    pub(crate) is_buy: bool,
}

/// Why a taker order was refused: for margin (batch 2g), or a reduce-only order that would increase its position (2p).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Refusal {
    Margin,
    ReduceOnly,
}

impl Refusal {
    /// An order taken to be refused so, as its effect is counted.
    pub(crate) const fn recently(self) -> &'static str {
        match self {
            Self::Margin => "margin_refused_recently",
            Self::ReduceOnly => "reduce_only_refused_recently",
        }
    }
}

/// A taker order's outcome in a block: refused, and why, or not (`None`: an inference cleared); whether the order was
/// reduce-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TakerOutcome {
    pub(crate) key: TakerKey,
    pub(crate) reduce_only: bool,
    pub(crate) refused: Option<Refusal>,
}

/// The takers' margin refusals as of the confirmed height, and the outcomes of the blocks above it.
pub(crate) struct TakerRefusals {
    /// M, ms.
    window_ms: u64,
    /// Blocks not yet confirmed: height to (block time ms, outcomes in execution order).
    staged: BTreeMap<u64, (u64, Vec<TakerOutcome>)>,
    /// Keys whose last outcome of a kind is a refusal of it: the block time of it, ms.
    refused: FxHashMap<(TakerKey, Refusal), u64>,
}

impl TakerRefusals {
    pub(crate) fn new(window_ms: u64) -> Self {
        Self { window_ms, staged: BTreeMap::new(), refused: FxHashMap::default() }
    }

    /// A block's outcomes, kept until its height is confirmed.
    pub(crate) fn stage(&mut self, height: u64, time_ms: u64, outcomes: Vec<TakerOutcome>) {
        if !outcomes.is_empty() {
            self.staged.insert(height, (time_ms, outcomes));
        }
    }

    /// The staged blocks up to `height` applied in order; the keys whose refusal changed. Idempotent.
    pub(crate) fn apply_through(&mut self, height: u64) -> FxHashSet<TakerKey> {
        let mut changed = FxHashSet::default();
        let later = self.staged.split_off(&height.saturating_add(1));
        for (_, (time_ms, outcomes)) in std::mem::replace(&mut self.staged, later) {
            for TakerOutcome { key, reduce_only, refused } in outcomes {
                let moved = if let Some(refusal) = refused {
                    self.refused.insert((key, refusal), time_ms) != Some(time_ms)
                } else {
                    // any order placed clears the margin inference; a reduce-only one the reduce-only inference too
                    let margin = self.refused.remove(&(key, Refusal::Margin)).is_some();
                    let reduce = reduce_only && self.refused.remove(&(key, Refusal::ReduceOnly)).is_some();
                    margin || reduce
                };
                if moved {
                    changed.insert(key);
                }
            }
        }
        changed
    }

    /// The refusals no action held can fall within any more: `t_r + M` before `held_from_ms`, the oldest arrival a held
    /// action can have (confirmed block time - E; strictly before it, an action of that millisecond possibly still held -
    /// review 000621 r2).
    pub(crate) fn evict(&mut self, held_from_ms: u64) {
        let window_ms = self.window_ms;
        self.refused.retain(|_, refused_ms| *refused_ms + window_ms >= held_from_ms);
    }

    /// Everything dropped: a block log gap.
    pub(crate) fn clear(&mut self) {
        self.staged.clear();
        self.refused.clear();
    }

    /// Why `key`'s taker order - `reduce_only` or not - whose line came at `seen_ms` is taken to be refused, if it is:
    /// its key's last outcome of the kind a refusal at `t_r`, `t_r <= seen_ms <= t_r + M` (a later refusal does not reach
    /// back to an earlier order - review 000621 r2); a reduce-only refusal for a reduce-only order alone.
    pub(crate) fn refused(&self, key: &TakerKey, reduce_only: bool, seen_ms: u64) -> Option<Refusal> {
        let within = |refusal| self.refused.get(&(*key, refusal)).is_some_and(|refused_ms| *refused_ms <= seen_ms && seen_ms <= refused_ms + self.window_ms);
        if within(Refusal::Margin) {
            return Some(Refusal::Margin);
        }
        (reduce_only && within(Refusal::ReduceOnly)).then_some(Refusal::ReduceOnly)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> (usize, usize) {
        (self.staged.len(), self.refused.len())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;

    use super::*;

    const M: u64 = 300_000;

    fn key(signer: u8, asset: u32, is_buy: bool) -> TakerKey {
        TakerKey { identity: ActingIdentity::Signer(Address::repeat_byte(signer)), asset, is_buy }
    }

    fn refused(key: TakerKey) -> TakerOutcome {
        TakerOutcome { key, reduce_only: false, refused: Some(Refusal::Margin) }
    }

    fn cleared(key: TakerKey) -> TakerOutcome {
        TakerOutcome { key, reduce_only: false, refused: None }
    }

    fn changed(keys: &[TakerKey]) -> FxHashSet<TakerKey> {
        keys.iter().copied().collect()
    }

    /// A block's outcomes wait for its height; the last of a key in a block stands, a clear ends a refusal; applying
    /// again changes nothing.
    #[test]
    fn outcomes_apply_once_confirmed_in_order() {
        let (a, b) = (key(1, 159, true), key(1, 159, false));
        let mut refusals = TakerRefusals::new(M);
        refusals.stage(10, 1_000, vec![refused(a), cleared(a), refused(a), refused(b), cleared(b)]);
        refusals.stage(11, 1_100, vec![cleared(a)]);
        refusals.stage(12, 1_200, Vec::new());
        assert_eq!(refusals.len(), (2, 0), "an empty block is not kept");
        assert_eq!(refusals.apply_through(9), changed(&[]));
        assert!(refusals.refused(&a, false, 1_000).is_none());
        assert_eq!(refusals.apply_through(10), changed(&[a, b]));
        assert!(refusals.refused(&a, false, 1_000).is_some());
        assert!(refusals.refused(&b, false, 1_000).is_none());
        assert_eq!(refusals.apply_through(10), changed(&[]));
        assert_eq!(refusals.apply_through(11), changed(&[a]));
        assert!(refusals.refused(&a, false, 1_100).is_none());
        assert_eq!(refusals.len(), (0, 0));
    }

    /// `t_r <= seen_ms <= t_r + M`: a later refusal does not reach an earlier order (review 000621 r2); the key's
    /// identity, asset and side each its own.
    #[test]
    fn a_refusal_holds_from_its_block_for_m() {
        let a = key(1, 159, true);
        let mut refusals = TakerRefusals::new(M);
        refusals.stage(10, 1_100, vec![refused(a)]);
        refusals.apply_through(10);
        assert!(refusals.refused(&a, false, 1_000).is_none(), "a GTC seen before the refusal's block");
        assert!(refusals.refused(&a, false, 1_100).is_some());
        assert!(refusals.refused(&a, false, 1_100 + M).is_some());
        assert!(refusals.refused(&a, false, 1_101 + M).is_none());
        for other in [key(2, 159, true), key(1, 160, true), key(1, 159, false)] {
            assert!(refusals.refused(&other, false, 1_100).is_none(), "{other:?}");
        }
        let vault = TakerKey { identity: ActingIdentity::Vault(Address::repeat_byte(1)), ..a };
        assert!(refusals.refused(&vault, false, 1_100).is_none(), "a vault is not the signer of the same address");
    }

    /// A refusal again moves its block time, a change; one at the same block time is none.
    #[test]
    fn a_new_refusal_of_a_key_moves_its_time() {
        let a = key(1, 159, true);
        let mut refusals = TakerRefusals::new(M);
        refusals.stage(10, 1_000, vec![refused(a)]);
        refusals.stage(11, 1_000, vec![refused(a)]);
        refusals.stage(12, 1_200, vec![refused(a)]);
        assert_eq!(refusals.apply_through(10), changed(&[a]));
        assert_eq!(refusals.apply_through(11), changed(&[]));
        assert_eq!(refusals.apply_through(12), changed(&[a]));
        assert!(refusals.refused(&a, false, 1_100).is_none());
        assert!(refusals.refused(&a, false, 1_200).is_some());
    }

    /// Evicted when `t_r + M` is before the oldest arrival held, kept at it (review 000621 r2: strictly).
    #[test]
    fn a_refusal_is_evicted_strictly_past_its_reach() {
        let a = key(1, 159, true);
        let mut refusals = TakerRefusals::new(M);
        refusals.stage(10, 1_000, vec![refused(a)]);
        refusals.apply_through(10);
        refusals.evict(1_000 + M);
        assert!(refusals.refused(&a, false, 1_000 + M).is_some());
        refusals.evict(1_001 + M);
        assert_eq!(refusals.len(), (0, 0));
    }

    /// Batch 2p: a reduce-only refusal holds for the key's reduce-only orders alone, for M; a margin refusal for both. Any
    /// order placed ends the margin inference; only a reduce-only one placed ends the reduce-only inference.
    #[test]
    fn a_reduce_only_refusal_holds_for_reduce_only_orders() {
        let a = key(1, 159, true);
        let reduce_only = |refused| TakerOutcome { key: a, reduce_only: true, refused };
        let mut refusals = TakerRefusals::new(M);
        refusals.stage(10, 1_000, vec![reduce_only(Some(Refusal::ReduceOnly))]);
        assert_eq!(refusals.apply_through(10), changed(&[a]));
        assert_eq!(refusals.refused(&a, true, 1_000), Some(Refusal::ReduceOnly));
        assert_eq!(refusals.refused(&a, true, 1_001 + M), None);
        assert_eq!(refusals.refused(&a, false, 1_000), None, "a plain order is not refused so");
        refusals.stage(11, 1_100, vec![cleared(a)]);
        assert_eq!(refusals.apply_through(11), changed(&[]), "a plain order placed ends nothing of it");
        assert_eq!(refusals.refused(&a, true, 1_100), Some(Refusal::ReduceOnly));
        refusals.stage(12, 1_200, vec![refused(a)]);
        refusals.apply_through(12);
        assert_eq!(refusals.refused(&a, true, 1_200), Some(Refusal::Margin), "a margin refusal for either");
        refusals.stage(13, 1_300, vec![reduce_only(None)]);
        assert_eq!(refusals.apply_through(13), changed(&[a]));
        assert_eq!((refusals.refused(&a, true, 1_300), refusals.refused(&a, false, 1_300)), (None, None), "a reduce-only order placed ends both");
    }

    /// A gap drops the refusals and the blocks waiting both.
    #[test]
    fn clear_drops_the_refusals_and_the_staged_blocks() {
        let a = key(1, 159, true);
        let mut refusals = TakerRefusals::new(M);
        refusals.stage(10, 1_000, vec![refused(a)]);
        refusals.apply_through(10);
        refusals.stage(11, 1_100, vec![refused(key(2, 159, true))]);
        refusals.clear();
        assert_eq!(refusals.len(), (0, 0));
        assert_eq!(refusals.apply_through(11), changed(&[]));
    }
}

/// Review 000670 r1 P2-2: the rule as the pending book runs it, over real block logs - each block's taker outcomes read
/// by `parse_block` (the takers that may take whose own status tells an outcome, an identity by vault or recovered
/// signer), each judged by the refusals confirmed `PENDING_REFUSAL_LAG` blocks before it (2 by default: the replica's
/// lag), at its block's time (the pending book judges at its line's arrival), then staged - against what the block
/// says it got: "placed" counts an IOC missing every level too. A height not the next one - a hole, a repeat - forgets
/// the refusals as the pending book's block log gap does (review 000670 r2 P2-3); a line of no height stops it.
#[cfg(test)]
mod real_data {
    use std::{
        collections::BTreeMap,
        io::{BufRead, BufReader},
    };

    use super::*;
    use crate::{accounts::replica::line_height, pending::blocks::parse_block};

    #[test]
    #[ignore = "needs PENDING_REFUSAL_BLOCKS archives"]
    fn real_refusals_repeat() {
        let paths = std::env::var("PENDING_REFUSAL_BLOCKS").expect("PENDING_REFUSAL_BLOCKS");
        let lag: u64 = std::env::var("PENDING_REFUSAL_LAG").map_or(2, |lag| lag.parse().expect("a lag"));
        let mut refusals = TakerRefusals::new(300_000);
        let mut counts: BTreeMap<String, u64> = BTreeMap::new();
        let mut last: Option<u64> = None;
        for path in paths.split(',') {
            for (number, line) in BufReader::new(std::fs::File::open(path).expect("a block log")).lines().enumerate() {
                let line = line.expect("a line");
                let height = line_height(&line).unwrap_or_else(|| panic!("{path} line {}: no height", number + 1));
                if last.is_some_and(|last| height != last + 1) {
                    *counts.entry(format!("block log gap: {last:?} then {height}, refusals forgotten")).or_default() += 1;
                    refusals.clear();
                }
                last = Some(height);
                let block = parse_block(&line).unwrap_or_else(|err| panic!("block {height}: {err}"));
                refusals.apply_through(height.saturating_sub(lag));
                refusals.evict(block.time_ms.saturating_sub(1_000));
                for outcome in &block.taker_outcomes {
                    let judged = refusals.refused(&outcome.key, outcome.reduce_only, block.time_ms);
                    let got = match outcome.refused {
                        Some(Refusal::Margin) => "refused for margin",
                        Some(Refusal::ReduceOnly) => "refused as reduce-only",
                        None => "placed",
                    };
                    let kind = if outcome.reduce_only { "reduce-only" } else { "plain" };
                    let judged = judged.map_or("not judged refused", Refusal::recently);
                    *counts.entry(format!("{kind} order, {judged}: {got}")).or_default() += 1;
                }
                refusals.stage(height, block.time_ms, block.taker_outcomes);
            }
        }
        for (row, count) in counts {
            println!("{count:8} {row}");
        }
    }
}
