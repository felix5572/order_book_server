//! The node's block log for the pending set (hyperliquid-trade design 000604 r6 revision 14):
//! `<data-dir>/replica_cmds/<session>/<date>/<first height>`, a line per block, `{"abci_block": {"time",
//! "signed_action_bundles": [[hash, {...}], ...], ...}, "resps": ..., "height"}`. Of each only its height, time and its
//! bundles' hashes are read: a bundle's hash is the mempool line's `tx_hash` it ran (research
//! `mempool_pending_book_20261031`, 10-31 after the node upgrade: 18,939 of 18,939 bundles). Read by the pending
//! thread's own watcher - the account view parses each line whole on its own, later - from the end of the current file.

use std::{path::Path, thread};

use alloy::primitives::B256;
use log::error;
use serde::{Deserialize, de::IgnoredAny};
use tokio::time::Instant;

use super::feed::{self, Feed, FeedEvent, FeedLosses, FeedReports};
use crate::{
    listeners::order_book::{
        parallel::{FileLayout, LineShape, LineStream},
        throttled_log_due,
    },
    metrics::{PENDING_BOOK_BLOCK_LOG_LINES_TOTAL, PENDING_BOOK_BLOCK_LOG_LOSSES_TOTAL},
    types::node_data::node_time_unix_nanos,
};

/// Cap on one unterminated line, as the account view's reading of the same log.
const MAX_LINE_BYTES: usize = 64 << 20;

/// Blocks queued for the pending thread (~15 a second): one behind holds the watcher back.
const QUEUE_BLOCKS: usize = 1_024;

/// A block's bundle hashes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlockHashes {
    pub(crate) height: u64,
    /// The block time, unix ms.
    pub(crate) time_ms: u64,
    pub(crate) hashes: Vec<B256>,
}

#[derive(Deserialize)]
struct WireBlock<'a> {
    #[serde(borrow)]
    abci_block: WireAbciBlock<'a>,
    height: u64,
}

#[derive(Deserialize)]
struct WireAbciBlock<'a> {
    time: &'a str,
    signed_action_bundles: Vec<(B256, IgnoredAny)>,
}

/// Starts the pending thread's watcher of `<data_dir>/replica_cmds`.
pub(crate) fn watch_blocks(data_dir: &Path) -> (Feed<BlockHashes>, thread::JoinHandle<()>) {
    let stream = LineStream {
        name: "PendingBlockLog",
        lag_label: "pending_block_log",
        dir: data_dir.join("replica_cmds"),
        max_line_bytes: MAX_LINE_BYTES,
        start_skip_is_loss: true,
        layout: FileLayout::ReplicaSessions,
        shape: LineShape::Object,
        loss_counts: None,
    };
    feed::watch(stream, QUEUE_BLOCKS, parse_block)
}

/// One block log line; an `Err` says what does not read.
pub(crate) fn parse_block(line: &str) -> Result<BlockHashes, String> {
    let block: WireBlock<'_> = sonic_rs::from_str(line).map_err(|err| format!("block log line: {err}"))?;
    let time = block.abci_block.time;
    let unix_ns = node_time_unix_nanos(time).ok_or_else(|| format!("block {} time {time:?}", block.height))?;
    let time_ms = u64::try_from(unix_ns / 1_000_000).map_err(|_| format!("block {} time {time:?} before 1970", block.height))?;
    let hashes = block.abci_block.signed_action_bundles.into_iter().map(|(hash, _)| hash).collect();
    Ok(BlockHashes { height: block.height, time_ms, hashes })
}

/// What a block log event brings the pending set.
#[derive(Debug)]
pub(crate) enum BlockInput {
    /// A block; `after_loss` when taken while the watcher had found a loss not yet reported (it may follow it).
    Block { block: BlockHashes, after_loss: bool },
    /// Lines lost.
    Loss,
}

/// The pending thread's reading of the block log.
pub(crate) struct BlockIngest {
    reports: FeedReports,
    last_unreadable_log: Option<Instant>,
}

impl BlockIngest {
    pub(crate) const fn new(losses: FeedLosses) -> Self {
        Self { reports: FeedReports::new(losses), last_unreadable_log: None }
    }

    /// Whether the watcher found a loss no event taken yet reports.
    pub(crate) fn loss_unreported(&self) -> bool {
        self.reports.loss_unreported()
    }

    pub(crate) fn on_event(&mut self, event: FeedEvent<BlockHashes>) -> BlockInput {
        self.reports.take(&event);
        match event {
            FeedEvent::Line(block) => {
                let after_loss = self.loss_unreported();
                let outcome = if after_loss { "read_after_unreported_loss" } else { "read" };
                PENDING_BOOK_BLOCK_LOG_LINES_TOTAL.with_label_values(&[outcome]).inc();
                BlockInput::Block { block, after_loss }
            }
            FeedEvent::Unreadable { error, .. } => {
                PENDING_BOOK_BLOCK_LOG_LINES_TOTAL.with_label_values(&["unreadable"]).inc();
                if throttled_log_due(&mut self.last_unreadable_log, Instant::now()) {
                    error!("pending book: a block log line does not read, its hashes lost: {error}");
                }
                BlockInput::Loss
            }
            FeedEvent::Lost { loss, .. } => {
                PENDING_BOOK_BLOCK_LOG_LOSSES_TOTAL.with_label_values(&[feed::loss_label(loss)]).inc();
                BlockInput::Loss
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A block log line shortened from bm's (2026-10-31 15:33:32, block 1204123719), two bundles.
    const LINE: &str = r#"{"abci_block":{"time":"2026-10-31T15:33:32.681320216","signed_action_bundles":[["0x969bc4d41af39390282ad4f9445295b0b5a8032ce88fd7dbd95798e9802f70ea",{"signed_actions":[{"signature":{"r":"0x1","s":"0x2","v":27},"action":{"type":"noop","nonce":1},"nonce":1}],"broadcaster":"0xc9ba414780c1b75e9806e3469300993fd2bfa883","broadcaster_nonce":1793459645301}],["0x89804023c85e6566f5cf3d3a2b363554de673e82e00b83e8caddab51e445eb6f",{"signed_actions":[]}]],"round":1508694360,"parent_round":1508694359,"hardfork":{"version":0,"round":0},"proposer":"0xa82fe73bbd768bc15d1ef2f6142a21ff8bd762ad"},"resps":{"Full":[]},"height":1204123719}"#;

    pub(crate) fn hash(byte: u8) -> B256 {
        B256::repeat_byte(byte)
    }

    #[test]
    fn a_line_reads_its_height_time_and_bundle_hashes() {
        let block = parse_block(LINE).unwrap();
        assert_eq!((block.height, block.time_ms), (1_204_123_719, 1_793_460_812_681));
        assert_eq!(
            block.hashes,
            [
                "0x969bc4d41af39390282ad4f9445295b0b5a8032ce88fd7dbd95798e9802f70ea".parse::<B256>().unwrap(),
                "0x89804023c85e6566f5cf3d3a2b363554de673e82e00b83e8caddab51e445eb6f".parse::<B256>().unwrap(),
            ]
        );
    }

    #[test]
    fn a_line_without_its_height_or_a_hash_does_not_read() {
        assert!(parse_block(&LINE.replace(r#","height":1204123719"#, "")).is_err());
        assert!(parse_block(&LINE.replace("0x969bc4d41af39390282ad4f9445295b0b5a8032ce88fd7dbd95798e9802f70ea", "0x96")).is_err());
        assert!(parse_block(&LINE.replace("2026-10-31T15:33:32.681320216", "yesterday")).is_err());
    }
}
