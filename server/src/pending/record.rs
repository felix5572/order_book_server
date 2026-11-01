//! The record of the blocks the block log showed (hyperliquid-trade design 000604 r6 revision 15, r7 revision 20): each
//! one's bundle hashes, in height order and without a hole, and each hash's height. The pending set settles by it: a
//! mempool line whose hash a block at or below the replica's confirmed height holds is on the replica's book.
//!
//! Blocks at or below the confirmed height are kept R = H + E of block time behind it, for a line late into the
//! mempool to be told apart (r5 revision 13's bound); those above it - the log leads the book streams - at most
//! `MAX_LEAD_BLOCKS`, past which the pending thread takes no more blocks and the log's watcher waits.

use std::collections::VecDeque;

use alloy::primitives::B256;
use rustc_hash::FxHashMap;

use super::blocks::LoggedBlock;

/// Blocks the record holds above the replica's confirmed height at most (~22 min of mainnet blocks; ~2 hashes each).
pub(crate) const MAX_LEAD_BLOCKS: u64 = 20_000;

#[derive(Default)]
pub(crate) struct BlockRecord {
    /// In height order, each the one after the one before.
    blocks: VecDeque<LoggedBlock>,
    heights: FxHashMap<B256, u64>,
    /// The ALO priority of each order the held blocks placed with one (batch 2f); an order not here has none.
    priorities: FxHashMap<u64, u64>,
    /// The last height the log showed: eviction may empty `blocks`, never this (review 000611 r3 P1).
    last_height: Option<u64>,
}

impl BlockRecord {
    /// Whether `block` follows the last one the log showed (any first one does).
    pub(crate) fn follows(&self, block: &LoggedBlock) -> bool {
        self.last_height.is_none_or(|last| block.height == last + 1)
    }

    /// Adds `block`; one that does not follow starts the record anew from it.
    pub(crate) fn add(&mut self, block: LoggedBlock) {
        if !self.follows(&block) {
            self.blocks.clear();
            self.heights.clear();
            self.priorities.clear();
        }
        for hash in &block.hashes {
            self.heights.insert(*hash, block.height);
        }
        self.priorities.extend(block.priorities.iter().copied());
        self.last_height = Some(block.height);
        self.blocks.push_back(block);
    }

    /// The ALO priorities of the orders the held blocks placed with one, by oid.
    pub(crate) const fn priorities(&self) -> &FxHashMap<u64, u64> {
        &self.priorities
    }

    /// The height of the block holding `hash`.
    pub(crate) fn height_of(&self, hash: &B256) -> Option<u64> {
        self.heights.get(hash).copied()
    }

    /// The last height the log showed.
    pub(crate) const fn newest(&self) -> Option<u64> {
        self.last_height
    }

    /// Drops the blocks at or below `confirmed` older than `before_ms`.
    pub(crate) fn evict(&mut self, confirmed: u64, before_ms: u64) {
        while let Some(block) = self.blocks.pop_front_if(|block| block.height <= confirmed && block.time_ms < before_ms) {
            for hash in &block.hashes {
                self.heights.remove(hash);
            }
            for (oid, _) in &block.priorities {
                self.priorities.remove(oid);
            }
        }
    }

    /// Blocks held above `confirmed` (all of them without a confirmed height).
    pub(crate) fn lead(&self, confirmed: Option<u64>) -> u64 {
        let (Some(oldest), Some(newest)) = (self.blocks.front(), self.blocks.back()) else { return 0 };
        let first_above = confirmed.map_or(oldest.height, |confirmed| (confirmed + 1).max(oldest.height));
        (newest.height + 1).saturating_sub(first_above)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pending::blocks::tests::hash;

    fn block(height: u64, hashes: &[u8]) -> LoggedBlock {
        let hashes = hashes.iter().map(|byte| hash(*byte)).collect();
        LoggedBlock { height, time_ms: height * 100, hashes, priorities: Vec::new() }
    }

    #[test]
    fn hashes_are_found_by_their_block_and_evicted_with_it() {
        let mut record = BlockRecord::default();
        record.add(block(10, &[1, 2]));
        record.add(block(11, &[3]));
        assert_eq!((record.height_of(&hash(2)), record.height_of(&hash(3)), record.height_of(&hash(4))), (Some(10), Some(11), None));
        record.evict(11, 1_100);
        assert_eq!((record.height_of(&hash(1)), record.height_of(&hash(3))), (None, Some(11)), "10 older than 1,100 ms");
        record.evict(10, 2_000);
        assert_eq!(record.height_of(&hash(3)), Some(11), "above the confirmed height: kept");
    }

    /// Batch 2f: an order's priority is kept with the block that placed it - evicted with it, dropped with a record
    /// started anew.
    #[test]
    fn priorities_go_with_their_blocks() {
        let mut record = BlockRecord::default();
        record.add(LoggedBlock { priorities: vec![(7, 3)], ..block(10, &[1]) });
        record.add(LoggedBlock { priorities: vec![(8, 55)], ..block(11, &[2]) });
        assert_eq!((record.priorities().get(&7), record.priorities().get(&8)), (Some(&3), Some(&55)));
        record.evict(11, 1_050);
        assert_eq!((record.priorities().get(&7), record.priorities().get(&8)), (None, Some(&55)));
        record.add(LoggedBlock { priorities: vec![(9, 6)], ..block(13, &[3]) });
        assert_eq!((record.priorities().get(&8), record.priorities().get(&9)), (None, Some(&6)));
    }

    /// A block not following the newest starts the record anew.
    #[test]
    fn a_hole_starts_the_record_anew() {
        let mut record = BlockRecord::default();
        record.add(block(10, &[1]));
        assert!(!record.follows(&block(12, &[2])));
        record.add(block(12, &[2]));
        assert_eq!((record.height_of(&hash(1)), record.newest()), (None, Some(12)));
    }

    /// Review 000611 r3 P1: eviction empties the record, the last height read stays - a jump is still a hole.
    #[test]
    fn a_hole_after_eviction_emptied_the_record() {
        let mut record = BlockRecord::default();
        record.add(block(30, &[1]));
        record.evict(90, 9_000);
        assert_eq!((record.height_of(&hash(1)), record.lead(Some(90)), record.newest()), (None, 0, Some(30)));
        assert!(!record.follows(&block(90, &[])));
        assert!(record.follows(&block(31, &[])));
    }

    #[test]
    fn the_lead_counts_the_blocks_above_the_confirmed_height() {
        let mut record = BlockRecord::default();
        assert_eq!(record.lead(Some(5)), 0);
        for height in 10..=14 {
            record.add(block(height, &[]));
        }
        let leads = [record.lead(None), record.lead(Some(5)), record.lead(Some(12)), record.lead(Some(14)), record.lead(Some(20))];
        assert_eq!(leads, [5, 5, 2, 0, 0]);
    }
}
