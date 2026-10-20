//! Block assembly (design 000433 r3, sections 3.2 and 3.4): the account view applies whole blocks in height order,
//! each exactly once. The inputs of the three streams are held per pending block, keyed by identity so a repeated
//! input does not count twice, and a block is released once complete:
//!
//! - its `replica_cmds` line was read (every block has one);
//! - the `replica_cmds` line of the next block was read too, at instant `r`;
//! - the fills and the misc events readers each proved a read-through at an instant later than `r`.
//!
//! The last two rest on the node's write order measured on bm: the fills and misc lines of a block arrive before the
//! `replica_cmds` line of the next block (research README "三路流的写出顺序"). It is a run assumption, not a protocol
//! guarantee. A line for a block already released either breaks it or is a node replay of written blocks; the two
//! cannot be told apart, so either is a loss up to that block and the view waits for a covering anchor. What was
//! released before cannot be taken back.

use std::{collections::BTreeMap, time::Instant};

use alloy::primitives::Address;
use rustc_hash::FxHashSet;

use super::{actions::BlockActions, book::Stream, misc::MiscEvent};
use crate::{
    order_book::Side,
    types::node_data::{Batch, NodeDataFill},
};

/// Pending blocks held at most: normally a few; ~2,000 is minutes of a stalled stream (~14 blocks a second).
pub(crate) const MAX_PENDING_BLOCKS: usize = 2_000;
/// Pending inputs (fills, misc events, block lines) held at most: ~110 fills a second, ~400k funding deltas at an hour.
pub(crate) const MAX_PENDING_INPUTS: usize = 1_000_000;

/// A fill's identity within its block: account, market, trade and side. The two legs of a trade differ by account and
/// side; 290,500 fills lines of 2026-10-19 21:00 UTC held no repeat within a block.
type FillKey = (Address, String, u64, Side);

/// A misc events line's identity within its block, hashed from its `events` text (the node's `local_time` excluded):
/// in 6,835 lines of 2026-10-19 21-22 UTC only the empty hourly Funding lines (one per dex, no deltas, no cash)
/// repeated within a block.
pub(crate) type MiscKey = u64;

/// A released block: its `replica_cmds` actions and every distinct fills and misc events input read for it.
#[derive(Debug)]
pub(crate) struct CompletedBlock {
    pub(crate) actions: BlockActions,
    pub(crate) fills: Vec<Batch<NodeDataFill>>,
    pub(crate) misc_events: Vec<Batch<MiscEvent>>,
}

#[derive(Debug, Default)]
struct PendingBlock {
    actions: Option<BlockActions>,
    /// When the `replica_cmds` line was read.
    actions_read_at: Option<Instant>,
    fills: Vec<Batch<NodeDataFill>>,
    fill_keys: FxHashSet<FillKey>,
    misc_events: Vec<Batch<MiscEvent>>,
    misc_keys: FxHashSet<MiscKey>,
    inputs: usize,
}

/// What one input did.
#[derive(Debug, Default)]
pub(crate) struct Assembly {
    /// Blocks released, in height order.
    pub(crate) completed: Vec<CompletedBlock>,
    /// Inputs that will not be applied, as (stream, block): late (the block was released already) or dropped with a
    /// gap of `replica_cmds` lines or an overflow. Each is a loss of that stream up to the block.
    pub(crate) lost: Vec<(Stream, u64)>,
    /// Inputs already held for their pending block, by stream.
    pub(crate) duplicates: Vec<(Stream, usize)>,
    /// Blocks whose `replica_cmds` lines never arrived, (first, last).
    pub(crate) missing_actions: Option<(u64, u64)>,
    /// The pending inputs overflowed and were dropped up to this block.
    pub(crate) overflowed_to: Option<u64>,
}

#[derive(Debug, Default)]
pub(crate) struct BlockAssembler {
    /// Highest block released; `None` until the first `replica_cmds` line, whose block minus one it then becomes.
    released: Option<u64>,
    /// Highest `replica_cmds` line read.
    actions_height: Option<u64>,
    pending: BTreeMap<u64, PendingBlock>,
    inputs: usize,
    fills_read_through: Option<Instant>,
    misc_read_through: Option<Instant>,
}

impl BlockAssembler {
    #[cfg(test)]
    pub(crate) const fn released(&self) -> Option<u64> {
        self.released
    }

    pub(crate) fn pending_blocks(&self) -> usize {
        self.pending.len()
    }

    fn is_released(&self, block: u64) -> bool {
        self.released.is_some_and(|released| block <= released)
    }

    pub(crate) fn receive_fills(&mut self, mut batch: Batch<NodeDataFill>) -> Assembly {
        let block = batch.block_number();
        let mut assembly = Assembly::default();
        if self.is_released(block) {
            assembly.lost.push((Stream::Fills, block));
            return assembly;
        }
        let pending = self.pending.entry(block).or_default();
        let read = batch.events_len();
        batch.retain_events(|NodeDataFill(user, fill)| pending.fill_keys.insert((*user, fill.coin.clone(), fill.tid, fill.side)));
        if batch.events_len() < read {
            assembly.duplicates.push((Stream::Fills, read - batch.events_len()));
        }
        // a line whose fills are all held already adds nothing; an empty line still marks the stream's progress
        if read == 0 || batch.events_len() > 0 {
            let inputs = batch.events_len().max(1);
            pending.inputs += inputs;
            self.inputs += inputs;
            pending.fills.push(batch);
        }
        self.settle(assembly)
    }

    pub(crate) fn receive_misc_events(&mut self, batch: Batch<MiscEvent>, key: MiscKey) -> Assembly {
        let block = batch.block_number();
        let mut assembly = Assembly::default();
        if self.is_released(block) {
            assembly.lost.push((Stream::MiscEvents, block));
            return assembly;
        }
        let pending = self.pending.entry(block).or_default();
        if !pending.misc_keys.insert(key) {
            assembly.duplicates.push((Stream::MiscEvents, 1));
            return assembly;
        }
        let inputs = batch.events_len().max(1);
        pending.inputs += inputs;
        self.inputs += inputs;
        pending.misc_events.push(batch);
        self.settle(assembly)
    }

    /// One `replica_cmds` line, read at `read_at`.
    pub(crate) fn receive_block_actions(&mut self, actions: BlockActions, read_at: Instant) -> Assembly {
        let height = actions.height;
        let mut assembly = Assembly::default();
        if self.released.is_none() {
            self.start_below(height, &mut assembly);
        }
        if self.is_released(height) {
            assembly.lost.push((Stream::BlockActions, height));
            return assembly;
        }
        if self.actions_height.is_some_and(|read| height <= read) {
            assembly.duplicates.push((Stream::BlockActions, 1));
            return assembly;
        }
        if let Some(read) = self.actions_height
            && height > read + 1
        {
            self.drop_through(height - 1, &mut assembly);
            assembly.missing_actions = Some((read + 1, height - 1));
        }
        self.actions_height = Some(height);
        let pending = self.pending.entry(height).or_default();
        pending.actions = Some(actions);
        pending.actions_read_at = Some(read_at);
        pending.inputs += 1;
        self.inputs += 1;
        self.settle(assembly)
    }

    /// A reader proved that every line it was written before `at` reached the owner.
    pub(crate) fn receive_read_through(&mut self, stream: Stream, at: Instant) -> Assembly {
        let latest = match stream {
            Stream::Fills => &mut self.fills_read_through,
            Stream::MiscEvents => &mut self.misc_read_through,
            // a block's own line completes it on `replica_cmds`
            Stream::BlockActions => return Assembly::default(),
        };
        *latest = Some(latest.map_or(at, |seen| seen.max(at)));
        self.settle(Assembly::default())
    }

    /// The first `replica_cmds` line: blocks below it count as released, and the inputs already held for them are
    /// lost (they precede the start, which the watchers' start skip makes a loss anyway).
    fn start_below(&mut self, height: u64, assembly: &mut Assembly) {
        self.drop_through(height.saturating_sub(1), assembly);
    }

    /// Releases every complete block from the next one on, then enforces the bounds.
    fn settle(&mut self, mut assembly: Assembly) -> Assembly {
        while let Some(block) = self.release_next() {
            assembly.completed.push(block);
        }
        if (self.pending.len() > MAX_PENDING_BLOCKS || self.inputs > MAX_PENDING_INPUTS)
            && let Some(newest) = self.pending.keys().next_back().copied()
        {
            self.drop_through(newest, &mut assembly);
            assembly.overflowed_to = Some(newest);
        }
        assembly
    }

    fn release_next(&mut self) -> Option<CompletedBlock> {
        let block = self.released? + 1;
        let next_read_at = self.pending.get(&(block + 1))?.actions_read_at?;
        let proven = |read_through: Option<Instant>| read_through.is_some_and(|at| at > next_read_at);
        let complete = self.pending.get(&block)?.actions.is_some()
            && proven(self.fills_read_through)
            && proven(self.misc_read_through);
        if !complete {
            return None;
        }
        let PendingBlock { actions, fills, misc_events, inputs, .. } = self.pending.remove(&block)?;
        self.inputs -= inputs;
        self.released = Some(block);
        Some(CompletedBlock { actions: actions?, fills, misc_events })
    }

    /// Drops every block held up to `block`, reporting their fills and misc events as lost, and counts them released.
    fn drop_through(&mut self, block: u64, assembly: &mut Assembly) {
        let held: Vec<u64> = self.pending.range(..=block).map(|(height, _)| *height).collect();
        for height in held {
            let Some(pending) = self.pending.remove(&height) else { continue };
            self.inputs -= pending.inputs;
            if !pending.fills.is_empty() {
                assembly.lost.push((Stream::Fills, height));
            }
            if !pending.misc_events.is_empty() {
                assembly.lost.push((Stream::MiscEvents, height));
            }
        }
        self.released = Some(self.released.map_or(block, |released| released.max(block)));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::accounts::test_support::{PUMP, block_actions, fills, misc, user};

    const SEND: &str = r#"{"LedgerUpdate":{"users":["0x0101010101010101010101010101010101010101"],"delta":{"type":"send"}}}"#;
    const DEPOSIT: &str = r#"{"LedgerUpdate":{"users":["0x0101010101010101010101010101010101010101"],"delta":{"type":"deposit"}}}"#;

    /// Instants `ms` milliseconds after one base.
    fn clock() -> impl Fn(u64) -> Instant {
        let base = Instant::now();
        move |ms| base + Duration::from_millis(ms)
    }

    fn heights(assembly: &Assembly) -> Vec<u64> {
        assembly.completed.iter().map(|block| block.actions.height).collect()
    }

    fn tids(block: &CompletedBlock) -> Vec<u64> {
        block.fills.iter().flat_map(|batch| batch.events_ref().iter().map(|NodeDataFill(_, fill)| fill.tid)).collect()
    }

    /// An assembler that read the `replica_cmds` line of block 100 at `at(0)`: blocks up to 99 are released.
    fn started(at: &impl Fn(u64) -> Instant) -> BlockAssembler {
        let mut assembler = BlockAssembler::default();
        assert_eq!(heights(&assembler.receive_block_actions(block_actions(100, &[]), at(0))), Vec::<u64>::new());
        assert_eq!(assembler.released(), Some(99));
        assembler
    }

    /// Review 000433 r1 P1 / r2: a block is released only when both readers proved a read-through after the next
    /// block's `replica_cmds` line was read, and it carries exactly its own inputs whatever order the streams ran in.
    #[test]
    fn a_block_waits_for_both_proofs_past_the_next_line_and_keeps_only_its_inputs() {
        let at = clock();
        let mut assembler = started(&at);
        assembler.receive_fills(fills(101, &[(user(1), PUMP, "B", 1, "0.0", "1.0")]));
        assembler.receive_misc_events(misc(101, &[SEND]), 1);
        assembler.receive_misc_events(misc(101, &[DEPOSIT]), 2);
        assembler.receive_fills(fills(102, &[(user(1), PUMP, "B", 2, "1.0", "1.0")]));
        assembler.receive_block_actions(block_actions(101, &[]), at(10));
        assembler.receive_block_actions(block_actions(102, &[]), at(20));
        let fills_proof = assembler.receive_read_through(Stream::Fills, at(15));
        assert!(heights(&fills_proof).is_empty(), "the misc events reader has proved nothing yet");
        let misc_proof = assembler.receive_read_through(Stream::MiscEvents, at(25));
        assert_eq!(heights(&misc_proof), [100], "101 needs the fills proof past 102's line too");
        let later = assembler.receive_read_through(Stream::Fills, at(30));
        assert_eq!(heights(&later), [101]);
        let block = &later.completed[0];
        assert_eq!((tids(block), block.misc_events.len()), (vec![1], 2), "block 102's fill stays held");
        assert_eq!((assembler.released(), assembler.pending_blocks()), (Some(101), 1));
    }

    /// Review 000433 r2 P2: a repeat before the block completes is held once; distinct inputs, also the two legs of one
    /// trade, are all kept.
    #[test]
    fn repeats_before_completion_are_held_once_and_distinct_inputs_all_kept() {
        let at = clock();
        let mut assembler = started(&at);
        let legs = fills(101, &[(user(1), PUMP, "B", 1, "0.0", "1.0"), (user(2), PUMP, "A", 1, "0.0", "1.0")]);
        assert!(assembler.receive_fills(legs).duplicates.is_empty(), "both legs of trade 1");
        let repeat = assembler.receive_fills(fills(101, &[(user(1), PUMP, "B", 1, "0.0", "1.0")]));
        assert_eq!(repeat.duplicates, [(Stream::Fills, 1)]);
        assert_eq!(assembler.receive_misc_events(misc(101, &[SEND]), 7).duplicates, []);
        assert_eq!(assembler.receive_misc_events(misc(101, &[SEND]), 7).duplicates, [(Stream::MiscEvents, 1)]);
        assert!(assembler.receive_misc_events(misc(101, &[SEND]), 8).duplicates.is_empty(), "same content, other identity");
        assembler.receive_block_actions(block_actions(101, &[]), at(10));
        assert_eq!(assembler.receive_block_actions(block_actions(101, &[]), at(11)).duplicates, [(Stream::BlockActions, 1)]);
        assembler.receive_block_actions(block_actions(102, &[]), at(20));
        assembler.receive_read_through(Stream::Fills, at(30));
        let released = assembler.receive_read_through(Stream::MiscEvents, at(30));
        assert_eq!(heights(&released), [100, 101]);
        let block = &released.completed[1];
        assert_eq!((tids(block), block.misc_events.len()), (vec![1, 1], 2));
    }

    #[test]
    fn inputs_for_released_blocks_are_lost() {
        let at = clock();
        let mut assembler = started(&at);
        assembler.receive_block_actions(block_actions(101, &[]), at(10));
        assembler.receive_read_through(Stream::Fills, at(20));
        assert_eq!(heights(&assembler.receive_read_through(Stream::MiscEvents, at(20))), [100]);
        assert_eq!(assembler.receive_fills(fills(100, &[(user(1), PUMP, "B", 1, "0.0", "1.0")])).lost, [(Stream::Fills, 100)]);
        assert_eq!(assembler.receive_misc_events(misc(99, &[SEND]), 1).lost, [(Stream::MiscEvents, 99)]);
        assert_eq!(assembler.receive_block_actions(block_actions(100, &[]), at(30)).lost, [(Stream::BlockActions, 100)]);
    }

    #[test]
    fn inputs_held_before_the_first_replica_line_precede_the_start() {
        let at = clock();
        let mut assembler = BlockAssembler::default();
        assembler.receive_fills(fills(99, &[(user(1), PUMP, "B", 1, "0.0", "1.0")]));
        assembler.receive_misc_events(misc(100, &[SEND]), 1);
        let start = assembler.receive_block_actions(block_actions(100, &[]), at(0));
        assert_eq!(start.lost, [(Stream::Fills, 99)], "below the first line; block 100's own inputs stay");
        assert_eq!((assembler.released(), assembler.pending_blocks()), (Some(99), 1));
    }

    #[test]
    fn a_gap_of_replica_lines_drops_the_held_blocks_and_the_next_ones_complete() {
        let at = clock();
        let mut assembler = started(&at);
        assembler.receive_block_actions(block_actions(101, &[]), at(10));
        assembler.receive_fills(fills(101, &[(user(1), PUMP, "B", 1, "0.0", "1.0")]));
        assembler.receive_fills(fills(102, &[(user(1), PUMP, "B", 2, "1.0", "1.0")]));
        let gap = assembler.receive_block_actions(block_actions(104, &[]), at(30));
        assert_eq!(gap.missing_actions, Some((102, 103)));
        assert_eq!(gap.lost, [(Stream::Fills, 101), (Stream::Fills, 102)]);
        assert_eq!(assembler.released(), Some(103));
        assembler.receive_block_actions(block_actions(105, &[]), at(40));
        assembler.receive_read_through(Stream::Fills, at(50));
        assert_eq!(heights(&assembler.receive_read_through(Stream::MiscEvents, at(50))), [104]);
    }

    /// Review 000433 r2: a stalled reader cannot grow the pending blocks without bound.
    #[test]
    fn overflowing_pending_blocks_are_dropped_as_a_loss() {
        let at = clock();
        let mut assembler = started(&at);
        let mut block = 100;
        let overflow = loop {
            block += 1;
            assert!(assembler.pending_blocks() <= MAX_PENDING_BLOCKS, "the bound holds before the overflow");
            let assembly = assembler.receive_fills(fills(block, &[(user(1), PUMP, "B", block, "0.0", "1.0")]));
            if assembly.overflowed_to.is_some() {
                break assembly;
            }
        };
        assert_eq!(overflow.overflowed_to, Some(block), "block 100's line and the fills of 101..=block held");
        assert_eq!(block, 100 + MAX_PENDING_BLOCKS as u64);
        assert_eq!(overflow.lost.len(), MAX_PENDING_BLOCKS, "every held fills block is lost");
        assert_eq!((assembler.released(), assembler.pending_blocks()), (Some(block), 0));
    }
}
