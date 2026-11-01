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

/// A taker order's outcome in a block: refused for margin, or not (`refused` false: the refusal inference cleared).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TakerOutcome {
    pub(crate) key: TakerKey,
    pub(crate) refused: bool,
}

/// The takers' margin refusals as of the confirmed height, and the outcomes of the blocks above it.
pub(crate) struct TakerRefusals {
    /// M, ms.
    window_ms: u64,
    /// Blocks not yet confirmed: height to (block time ms, outcomes in execution order).
    staged: BTreeMap<u64, (u64, Vec<TakerOutcome>)>,
    /// Keys whose last outcome is a refusal: the block time of it, ms.
    refused: FxHashMap<TakerKey, u64>,
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
            for outcome in outcomes {
                let before = if outcome.refused { self.refused.insert(outcome.key, time_ms) } else { self.refused.remove(&outcome.key) };
                if before != outcome.refused.then_some(time_ms) {
                    changed.insert(outcome.key);
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

    /// Whether `key`'s taker order whose line came at `seen_ms` is taken to be refused: its last outcome a refusal at
    /// `t_r`, `t_r <= seen_ms <= t_r + M` (a later refusal does not reach back to an earlier order - review 000621 r2).
    pub(crate) fn refused(&self, key: &TakerKey, seen_ms: u64) -> bool {
        self.refused.get(key).is_some_and(|refused_ms| *refused_ms <= seen_ms && seen_ms <= refused_ms + self.window_ms)
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
        TakerOutcome { key, refused: true }
    }

    fn cleared(key: TakerKey) -> TakerOutcome {
        TakerOutcome { key, refused: false }
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
        assert!(!refusals.refused(&a, 1_000));
        assert_eq!(refusals.apply_through(10), changed(&[a, b]));
        assert!(refusals.refused(&a, 1_000));
        assert!(!refusals.refused(&b, 1_000));
        assert_eq!(refusals.apply_through(10), changed(&[]));
        assert_eq!(refusals.apply_through(11), changed(&[a]));
        assert!(!refusals.refused(&a, 1_100));
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
        assert!(!refusals.refused(&a, 1_000), "a GTC seen before the refusal's block");
        assert!(refusals.refused(&a, 1_100));
        assert!(refusals.refused(&a, 1_100 + M));
        assert!(!refusals.refused(&a, 1_101 + M));
        for other in [key(2, 159, true), key(1, 160, true), key(1, 159, false)] {
            assert!(!refusals.refused(&other, 1_100), "{other:?}");
        }
        let vault = TakerKey { identity: ActingIdentity::Vault(Address::repeat_byte(1)), ..a };
        assert!(!refusals.refused(&vault, 1_100), "a vault is not the signer of the same address");
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
        assert!(!refusals.refused(&a, 1_100));
        assert!(refusals.refused(&a, 1_200));
    }

    /// Evicted when `t_r + M` is before the oldest arrival held, kept at it (review 000621 r2: strictly).
    #[test]
    fn a_refusal_is_evicted_strictly_past_its_reach() {
        let a = key(1, 159, true);
        let mut refusals = TakerRefusals::new(M);
        refusals.stage(10, 1_000, vec![refused(a)]);
        refusals.apply_through(10);
        refusals.evict(1_000 + M);
        assert!(refusals.refused(&a, 1_000 + M));
        refusals.evict(1_001 + M);
        assert_eq!(refusals.len(), (0, 0));
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
