//! Block assembly (design 000433 r3, sections 3.2 and 3.4; design 000439 r2 revision 4): the account view and the node
//! marks apply whole blocks in height order, each exactly once. The inputs of the six streams are held per pending
//! block, keyed by identity so a repeated input does not count twice, and a block is released once complete:
//!
//! - its `replica_cmds` line was read (every block has one);
//! - the `replica_cmds` line of the next block was read too, at instant `r`;
//! - the fills, misc events, HIP-3 oracle updates, raw book diffs and order statuses readers each proved a read-through
//!   at an instant later than `r`.
//!
//! The last two rest on the node's write order measured on bm: those lines of a block arrive before the `replica_cmds`
//! line of the next block (research README "三路流的写出顺序"; over 300 s the HIP-3 stream 382 blocks, minimum 9.5 ms,
//! the diffs 3,987 blocks and the statuses 3,986, minimum 2.4 ms). It is a run assumption, not a protocol guarantee. A line for a block already released either breaks it or is a node replay of written blocks; the two
//! cannot be told apart, so either is a loss up to that block and the view waits for a covering anchor. What was
//! released before cannot be taken back.

use std::{collections::BTreeMap, time::Instant};

use alloy::primitives::Address;
use rustc_hash::FxHashSet;

use super::{actions::BlockActions, book::Stream, marks::{actions::MarkActions, system::SystemAction}, misc::MiscEvent};
use crate::{
    order_book::Side,
    types::node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus, OracleUpdateEvent},
};

/// Pending blocks held at most: normally a few; ~2,000 is minutes of a stalled stream (~14 blocks a second).
pub(crate) const MAX_PENDING_BLOCKS: usize = 2_000;
/// Pending inputs (fills, misc events, HIP-3 oracle updates, block lines) held at most: ~110 fills a second, ~400k
/// funding deltas at an hour.
pub(crate) const MAX_PENDING_INPUTS: usize = 1_000_000;

/// A fill's identity within its block: account, market, trade and side. The two legs of a trade differ by account and
/// side; 290,500 fills lines of 2026-10-19 21:00 UTC held no repeat within a block.
type FillKey = (Address, String, u64, Side);

/// A misc events or HIP-3 oracle updates line's identity within its block, hashed from its `events` text (the node's
/// `local_time` excluded): in 6,835 misc lines of 2026-10-19 21-22 UTC only the empty hourly Funding lines (one per
/// dex, no deltas, no cash) repeated within a block; a HIP-3 line holds one deployer's update (4,920 lines of
/// 2026-10-20 11 UTC, one dex each; 467 blocks had several). A book diffs line holds one diff: none of the 43,224,015
/// lines of 2026-10-20 11 UTC repeated another of its block; an order statuses line holds one status.
pub(crate) type LineKey = u64;

/// A released block: its `replica_cmds` actions and every distinct input of the other streams read for it.
#[derive(Debug)]
pub(crate) struct CompletedBlock {
    pub(crate) actions: BlockActions,
    pub(crate) fills: Vec<Batch<NodeDataFill>>,
    pub(crate) misc_events: Vec<Batch<MiscEvent>>,
    /// In arrival order, the node's write order.
    pub(crate) hip3_oracle_updates: Vec<Batch<OracleUpdateEvent>>,
    /// Every market's diffs, in arrival order: the node's execution order.
    pub(crate) book_diffs: Vec<Batch<NodeDataOrderDiff>>,
    /// The node marks' reading of the `replica_cmds` line; an error breaks only their contract.
    pub(crate) mark_actions: Result<MarkActions, String>,
    /// Every status in stream order: even an unused one closes a rejected IOC cancellation run.
    pub(crate) order_statuses: Vec<Batch<NodeDataOrderStatus>>,
    pub(crate) system_actions: Vec<Batch<SystemAction>>,
}

/// The distinct lines of one stream held for a block.
#[derive(Debug)]
struct KeyedLines<E> {
    lines: Vec<Batch<E>>,
    keys: FxHashSet<LineKey>,
}

impl<E> Default for KeyedLines<E> {
    fn default() -> Self {
        Self { lines: Vec::new(), keys: FxHashSet::default() }
    }
}

#[derive(Debug, Default)]
struct PendingBlock {
    actions: Option<(BlockActions, Result<MarkActions, String>)>,
    /// When the `replica_cmds` line was read.
    actions_read_at: Option<Instant>,
    fills: Vec<Batch<NodeDataFill>>,
    fill_keys: FxHashSet<FillKey>,
    misc_events: KeyedLines<MiscEvent>,
    hip3_oracle_updates: KeyedLines<OracleUpdateEvent>,
    book_diffs: KeyedLines<NodeDataOrderDiff>,
    order_statuses: KeyedLines<NodeDataOrderStatus>,
    system_actions: KeyedLines<SystemAction>,
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
    hip3_read_through: Option<Instant>,
    book_diffs_read_through: Option<Instant>,
    order_statuses_read_through: Option<Instant>,
    system_actions_read_through: Option<Instant>,
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

    pub(crate) fn receive_misc_events(&mut self, batch: Batch<MiscEvent>, key: LineKey) -> Assembly {
        self.receive_keyed_line(Stream::MiscEvents, batch, key, |pending| &mut pending.misc_events)
    }

    pub(crate) fn receive_hip3_oracle_updates(&mut self, batch: Batch<OracleUpdateEvent>, key: LineKey) -> Assembly {
        self.receive_keyed_line(Stream::Hip3OracleUpdates, batch, key, |pending| &mut pending.hip3_oracle_updates)
    }

    pub(crate) fn receive_book_diffs(&mut self, batch: Batch<NodeDataOrderDiff>, key: LineKey) -> Assembly {
        self.receive_keyed_line(Stream::BookDiffs, batch, key, |pending| &mut pending.book_diffs)
    }

    pub(crate) fn receive_order_statuses(&mut self, batch: Batch<NodeDataOrderStatus>, key: LineKey) -> Assembly {
        self.receive_keyed_line(Stream::OrderStatuses, batch, key, |pending| &mut pending.order_statuses)
    }

    pub(crate) fn receive_system_actions(&mut self, batch: Batch<SystemAction>, key: LineKey) -> Assembly {
        self.receive_keyed_line(Stream::SystemActions, batch, key, |pending| &mut pending.system_actions)
    }

    /// Holds a line of `stream` for its block unless a line of the same identity is held already.
    fn receive_keyed_line<E>(
        &mut self,
        stream: Stream,
        batch: Batch<E>,
        key: LineKey,
        held: impl FnOnce(&mut PendingBlock) -> &mut KeyedLines<E>,
    ) -> Assembly {
        let block = batch.block_number();
        let mut assembly = Assembly::default();
        if self.is_released(block) {
            assembly.lost.push((stream, block));
            return assembly;
        }
        let pending = self.pending.entry(block).or_default();
        let inputs = batch.events_len().max(1);
        let lines = held(pending);
        if !lines.keys.insert(key) {
            assembly.duplicates.push((stream, 1));
            return assembly;
        }
        lines.lines.push(batch);
        pending.inputs += inputs;
        self.inputs += inputs;
        self.settle(assembly)
    }

    /// One `replica_cmds` line as the account view and the node marks read it, read at `read_at`.
    pub(crate) fn receive_block_actions(
        &mut self,
        actions: BlockActions,
        mark_actions: Result<MarkActions, String>,
        read_at: Instant,
    ) -> Assembly {
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
        pending.actions = Some((actions, mark_actions));
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
            Stream::Hip3OracleUpdates => &mut self.hip3_read_through,
            Stream::BookDiffs => &mut self.book_diffs_read_through,
            Stream::OrderStatuses => &mut self.order_statuses_read_through,
            Stream::SystemActions => &mut self.system_actions_read_through,
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
            && proven(self.misc_read_through)
            && proven(self.hip3_read_through)
            && proven(self.book_diffs_read_through)
            && proven(self.order_statuses_read_through)
            && proven(self.system_actions_read_through);
        if !complete {
            return None;
        }
        let PendingBlock { actions, fills, misc_events, hip3_oracle_updates, book_diffs, order_statuses, system_actions, inputs, .. } =
            self.pending.remove(&block)?;
        self.inputs -= inputs;
        self.released = Some(block);
        let (actions, mark_actions) = actions?;
        Some(CompletedBlock {
            actions,
            fills,
            misc_events: misc_events.lines,
            hip3_oracle_updates: hip3_oracle_updates.lines,
            book_diffs: book_diffs.lines,
            mark_actions,
            order_statuses: order_statuses.lines,
            system_actions: system_actions.lines,
        })
    }

    /// Drops every block held up to `block`, reporting their other streams' inputs as lost, and counts them released.
    fn drop_through(&mut self, block: u64, assembly: &mut Assembly) {
        let held: Vec<u64> = self.pending.range(..=block).map(|(height, _)| *height).collect();
        for height in held {
            let Some(pending) = self.pending.remove(&height) else { continue };
            self.inputs -= pending.inputs;
            if !pending.fills.is_empty() {
                assembly.lost.push((Stream::Fills, height));
            }
            if !pending.misc_events.lines.is_empty() {
                assembly.lost.push((Stream::MiscEvents, height));
            }
            if !pending.hip3_oracle_updates.lines.is_empty() {
                assembly.lost.push((Stream::Hip3OracleUpdates, height));
            }
            if !pending.book_diffs.lines.is_empty() {
                assembly.lost.push((Stream::BookDiffs, height));
            }
            if !pending.system_actions.lines.is_empty() {
                assembly.lost.push((Stream::SystemActions, height));
            }
            if !pending.order_statuses.lines.is_empty() {
                assembly.lost.push((Stream::OrderStatuses, height));
            }
        }
        self.released = Some(self.released.map_or(block, |released| released.max(block)));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::accounts::test_support::{PUMP, block_actions, fills, hip3_line, misc, user};

    const SEND: &str = r#"{"LedgerUpdate":{"users":["0x0101010101010101010101010101010101010101"],"delta":{"type":"send","user":"0x0101010101010101010101010101010101010101","destination":"0x0101010101010101010101010101010101010101","sourceDex":"","destinationDex":"spot","token":"USDC","amount":"1.0","usdcValue":"1.0","fee":"0.0","nativeTokenFee":"0.0","nonce":1,"feeToken":""}}}"#;
    const DEPOSIT: &str = r#"{"LedgerUpdate":{"users":["0x0101010101010101010101010101010101010101"],"delta":{"type":"deposit","usdc":"1.0"}}}"#;

    /// An account test's line: the marks' reading does not matter.
    fn no_marks() -> Result<MarkActions, String> {
        Err("an account test line".to_string())
    }

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
        assert_eq!(heights(&assembler.receive_block_actions(block_actions(100, &[]), no_marks(), at(0))), Vec::<u64>::new());
        assert_eq!(assembler.released(), Some(99));
        assembler
    }

    /// Proofs of the readers other than `stream`, at `at`.
    fn prove_others(assembler: &mut BlockAssembler, stream: Stream, at: Instant) -> Vec<u64> {
        let others = [Stream::Fills, Stream::MiscEvents, Stream::Hip3OracleUpdates, Stream::BookDiffs, Stream::OrderStatuses, Stream::SystemActions]
            .into_iter()
            .filter(|other| *other != stream);
        others.flat_map(|other| heights(&assembler.receive_read_through(other, at))).collect()
    }

    /// Review 000433 r1 P1 / r2: a block is released only when every reader proved a read-through after the next
    /// block's `replica_cmds` line was read, and it carries exactly its own inputs whatever order the streams ran in.
    #[test]
    fn a_block_waits_for_every_proof_past_the_next_line_and_keeps_only_its_inputs() {
        let at = clock();
        let mut assembler = started(&at);
        assembler.receive_fills(fills(101, &[(user(1), PUMP, "B", 1, "0.0", "1.0")]));
        assembler.receive_misc_events(misc(101, &[SEND]), 1);
        assembler.receive_misc_events(misc(101, &[DEPOSIT]), 2);
        assembler.receive_hip3_oracle_updates(hip3_line(101, &[("xyz:TSLA", "358.0")]), 3);
        assembler.receive_fills(fills(102, &[(user(1), PUMP, "B", 2, "1.0", "1.0")]));
        assembler.receive_block_actions(block_actions(101, &[]), no_marks(), at(10));
        assembler.receive_block_actions(block_actions(102, &[]), no_marks(), at(20));
        assert_eq!(heights(&assembler.receive_read_through(Stream::Hip3OracleUpdates, at(25))), Vec::<u64>::new());
        assert_eq!(heights(&assembler.receive_read_through(Stream::BookDiffs, at(25))), Vec::<u64>::new());
        assert_eq!(heights(&assembler.receive_read_through(Stream::OrderStatuses, at(25))), Vec::<u64>::new());
        assert_eq!(heights(&assembler.receive_read_through(Stream::SystemActions, at(25))), Vec::<u64>::new());
        let fills_proof = assembler.receive_read_through(Stream::Fills, at(15));
        assert!(heights(&fills_proof).is_empty(), "the misc events reader has proved nothing yet");
        let misc_proof = assembler.receive_read_through(Stream::MiscEvents, at(25));
        assert_eq!(heights(&misc_proof), [100], "101 needs the fills proof past 102's line too");
        let later = assembler.receive_read_through(Stream::Fills, at(30));
        assert_eq!(heights(&later), [101]);
        let block = &later.completed[0];
        assert_eq!((tids(block), block.misc_events.len(), block.hip3_oracle_updates.len()), (vec![1], 2, 1), "102's fill stays held");
        assert_eq!((assembler.released(), assembler.pending_blocks()), (Some(101), 1));
    }

    /// Every reader's proof is needed: with the other three proved past block 101's successor, 101 waits for it.
    #[test]
    fn each_readers_proof_is_needed() {
        for stream in [Stream::Fills, Stream::MiscEvents, Stream::Hip3OracleUpdates, Stream::BookDiffs, Stream::OrderStatuses, Stream::SystemActions] {
            let at = clock();
            let mut assembler = started(&at);
            assembler.receive_block_actions(block_actions(101, &[]), no_marks(), at(10));
            assembler.receive_block_actions(block_actions(102, &[]), no_marks(), at(20));
            assert_eq!(prove_others(&mut assembler, stream, at(30)), Vec::<u64>::new(), "{stream:?} not proved");
            assert_eq!(heights(&assembler.receive_read_through(stream, at(30))), [100, 101], "{stream:?}");
        }
    }

    /// Design 000439 r2 revision 4: the HIP-3 reader's proof joins the completion rule, its lines are held once by
    /// identity, and a late one is a loss of its stream.
    #[test]
    fn hip3_oracle_updates_join_the_completion_rule() {
        let at = clock();
        let mut assembler = started(&at);
        let line = || hip3_line(101, &[("xyz:TSLA", "358.0")]);
        assert_eq!(assembler.receive_hip3_oracle_updates(line(), 5).duplicates, []);
        assert_eq!(assembler.receive_hip3_oracle_updates(line(), 5).duplicates, [(Stream::Hip3OracleUpdates, 1)]);
        assembler.receive_hip3_oracle_updates(hip3_line(101, &[("flx:GOLD", "4188.3")]), 6);
        assembler.receive_block_actions(block_actions(101, &[]), no_marks(), at(10));
        assembler.receive_block_actions(block_actions(102, &[]), no_marks(), at(20));
        assert!(prove_others(&mut assembler, Stream::Hip3OracleUpdates, at(30)).is_empty(), "waits for the HIP-3 proof");
        let early = assembler.receive_read_through(Stream::Hip3OracleUpdates, at(15));
        assert_eq!(heights(&early), [100], "a proof before 102's line does not complete 101");
        let released = assembler.receive_read_through(Stream::Hip3OracleUpdates, at(30));
        assert_eq!(heights(&released), [101]);
        assert_eq!(released.completed[0].hip3_oracle_updates.len(), 2, "two distinct lines");
        assert_eq!(assembler.receive_hip3_oracle_updates(line(), 7).lost, [(Stream::Hip3OracleUpdates, 101)]);
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
        assembler.receive_block_actions(block_actions(101, &[]), no_marks(), at(10));
        assert_eq!(assembler.receive_block_actions(block_actions(101, &[]), no_marks(), at(11)).duplicates, [(Stream::BlockActions, 1)]);
        assembler.receive_block_actions(block_actions(102, &[]), no_marks(), at(20));
        assembler.receive_read_through(Stream::Fills, at(30));
        assembler.receive_read_through(Stream::Hip3OracleUpdates, at(30));
        assembler.receive_read_through(Stream::BookDiffs, at(30));
        assembler.receive_read_through(Stream::OrderStatuses, at(30));
        assembler.receive_read_through(Stream::SystemActions, at(30));
        let released = assembler.receive_read_through(Stream::MiscEvents, at(30));
        assert_eq!(heights(&released), [100, 101]);
        let block = &released.completed[1];
        assert_eq!((tids(block), block.misc_events.len()), (vec![1, 1], 2));
    }

    #[test]
    fn inputs_for_released_blocks_are_lost() {
        let at = clock();
        let mut assembler = started(&at);
        assembler.receive_block_actions(block_actions(101, &[]), no_marks(), at(10));
        assembler.receive_read_through(Stream::Fills, at(20));
        assert_eq!(prove_others(&mut assembler, Stream::Fills, at(20)), [100]);
        assert_eq!(assembler.receive_fills(fills(100, &[(user(1), PUMP, "B", 1, "0.0", "1.0")])).lost, [(Stream::Fills, 100)]);
        assert_eq!(assembler.receive_misc_events(misc(99, &[SEND]), 1).lost, [(Stream::MiscEvents, 99)]);
        assert_eq!(assembler.receive_block_actions(block_actions(100, &[]), no_marks(), at(30)).lost, [(Stream::BlockActions, 100)]);
    }

    #[test]
    fn inputs_held_before_the_first_replica_line_precede_the_start() {
        let at = clock();
        let mut assembler = BlockAssembler::default();
        assembler.receive_fills(fills(99, &[(user(1), PUMP, "B", 1, "0.0", "1.0")]));
        assembler.receive_misc_events(misc(100, &[SEND]), 1);
        let start = assembler.receive_block_actions(block_actions(100, &[]), no_marks(), at(0));
        assert_eq!(start.lost, [(Stream::Fills, 99)], "below the first line; block 100's own inputs stay");
        assert_eq!((assembler.released(), assembler.pending_blocks()), (Some(99), 1));
    }

    #[test]
    fn a_gap_of_replica_lines_drops_the_held_blocks_and_the_next_ones_complete() {
        let at = clock();
        let mut assembler = started(&at);
        assembler.receive_block_actions(block_actions(101, &[]), no_marks(), at(10));
        assembler.receive_fills(fills(101, &[(user(1), PUMP, "B", 1, "0.0", "1.0")]));
        assembler.receive_fills(fills(102, &[(user(1), PUMP, "B", 2, "1.0", "1.0")]));
        assembler.receive_hip3_oracle_updates(hip3_line(102, &[("xyz:TSLA", "358.0")]), 1);
        let gap = assembler.receive_block_actions(block_actions(104, &[]), no_marks(), at(30));
        assert_eq!(gap.missing_actions, Some((102, 103)));
        assert_eq!(gap.lost, [(Stream::Fills, 101), (Stream::Fills, 102), (Stream::Hip3OracleUpdates, 102)]);
        assert_eq!(assembler.released(), Some(103));
        assembler.receive_block_actions(block_actions(105, &[]), no_marks(), at(40));
        assembler.receive_read_through(Stream::Fills, at(50));
        assert_eq!(prove_others(&mut assembler, Stream::Fills, at(50)), [104]);
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
    #[test]
    fn system_actions_are_deduplicated_gated_and_late_rows_are_losses() {
        let at = clock();
        let mut a = started(&at);
        let line = || sonic_rs::from_str::<Batch<SystemAction>>(r#"{"local_time":"2026-10-21T02:29:24.78","block_time":"2026-10-21T02:29:24.39","block_number":100,"events":[]}"#).unwrap();
        assert_eq!(a.receive_system_actions(line(), 7).duplicates, []);
        assert_eq!(a.receive_system_actions(line(), 7).duplicates, [(Stream::SystemActions, 1)]);
        a.receive_block_actions(block_actions(101, &[]), no_marks(), at(10));
        assert_eq!(prove_others(&mut a, Stream::SystemActions, at(20)), Vec::<u64>::new());
        assert!(a.receive_read_through(Stream::SystemActions, at(5)).completed.is_empty());
        let released = a.receive_read_through(Stream::SystemActions, at(20));
        assert_eq!(heights(&released), [100]);
        assert_eq!(released.completed[0].system_actions.len(), 1);
        assert_eq!(a.receive_system_actions(line(), 8).lost, [(Stream::SystemActions, 100)]);
    }


}
