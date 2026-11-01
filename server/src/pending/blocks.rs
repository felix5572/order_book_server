//! The node's block log for the pending set (hyperliquid-trade design 000604 r6 revision 14):
//! `<data-dir>/replica_cmds/<session>/<date>/<first height>`, a line per block, `{"abci_block": {"time",
//! "signed_action_bundles": [[hash, {...}], ...], ...}, "resps": ..., "height"}`, read as the account view reads it
//! (`ReplicaLine`). Of each: its height, time and its bundles' hashes - a bundle's hash is the mempool line's `tx_hash`
//! it ran (research `mempool_pending_book_20261031`, 10-31 after the node upgrade: 18,939 of 18,939 bundles) -, and the
//! oids its order actions with an ALO priority placed, with the priority (batch 2f: a confirmed order's place in its
//! level's queue). Read by the pending thread's own watcher, which parses each line - the account view parses each on
//! its own, later - from the end of the current file.

use std::{path::Path, thread};

use alloy::primitives::B256;
use log::error;
use tokio::time::Instant;

use super::feed::{self, Feed, FeedEvent, FeedLosses, FeedReports};
use crate::{
    accounts::replica::{ReplicaLine, WireOrder, WireStatus, WireStatusObject, alo_priority, response_statuses},
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

/// A block as the pending set reads the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LoggedBlock {
    pub(crate) height: u64,
    /// The block time, unix ms.
    pub(crate) time_ms: u64,
    pub(crate) hashes: Vec<B256>,
    /// (oid, priority) of each order an order action with an ALO priority placed, resting or filled in part on entry.
    pub(crate) priorities: Vec<(u64, u64)>,
}

/// Starts the pending thread's watcher of `<data_dir>/replica_cmds`.
pub(crate) fn watch_blocks(data_dir: &Path) -> (Feed<LoggedBlock>, thread::JoinHandle<()>) {
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
pub(crate) fn parse_block(line: &str) -> Result<LoggedBlock, String> {
    let block = ReplicaLine::parse(line).map_err(|err| format!("block log line: {err}"))?;
    let height = block.height;
    let time = block.time().ok_or_else(|| format!("block {height} without its time"))?;
    let unix_ns = node_time_unix_nanos(time).ok_or_else(|| format!("block {height} time {time:?}"))?;
    let time_ms = u64::try_from(unix_ns / 1_000_000).map_err(|_| format!("block {height} time {time:?} before 1970"))?;
    let hashes = block
        .bundle_hashes()
        .map(|hash| hash.parse::<B256>().map_err(|err| format!("block {height} bundle hash {hash:?}: {err}")))
        .collect::<Result<_, _>>()?;
    let priorities = placed_priorities(&block).map_err(|err| format!("block {height}: {err}"))?;
    Ok(LoggedBlock { height, time_ms, hashes, priorities })
}

/// (oid, priority) of each order the block's order actions with an ALO priority placed, by their responses - one a
/// `multiSig` executed too, under its response (review 000620 r2: a priority not read is not none; none seen on bm).
fn placed_priorities(block: &ReplicaLine<'_>) -> Result<Vec<(u64, u64)>, String> {
    let mut placed = Vec::new();
    for paired in block.paired()? {
        // an action refused whole placed nothing; a status neither is not taken as either (review 000620 r4 P2)
        match paired.response.res.status {
            "ok" => {}
            "err" => continue,
            status => return Err(format!("bundle {} action {}: status {status:?}", paired.bundle_hash, paired.position)),
        }
        let inner;
        let action = if paired.action.action_type == "multiSig" {
            inner = paired.action.multi_sig_action()?;
            &inner
        } else {
            paired.action
        };
        if action.action_type != "order" {
            continue;
        }
        let requested = action.requested_priority()?;
        if requested == 0 {
            continue;
        }
        let orders: Vec<WireOrder> = action.field(action.orders.as_ref(), "orders")?;
        let priority = alo_priority(requested, &orders);
        if priority == 0 {
            continue;
        }
        let statuses = response_statuses(&paired)?;
        // one error for them all: a refused batch
        if statuses.len() != orders.len() && matches!(statuses.as_slice(), [status] if refused(status)) {
            continue;
        }
        if statuses.len() != orders.len() {
            return Err(format!("bundle {} action {}: {} orders with {} statuses", paired.bundle_hash, paired.position, orders.len(), statuses.len()));
        }
        for (item, status) in statuses.iter().enumerate() {
            if refused(status) {
                continue;
            }
            // placed, or a status the priority cannot be told by: not taken as none (review 000620 r3 P2)
            let oid = placed_oid(status)
                .ok_or_else(|| format!("bundle {} action {} item {item}: a status neither placed nor refused", paired.bundle_hash, paired.position))?;
            placed.push((oid, priority));
        }
    }
    Ok(placed)
}

/// A status refusing its order: an error alone.
const fn refused(status: &WireStatus) -> bool {
    matches!(status, WireStatus::Object(WireStatusObject { error: Some(_), resting: None, filled: None }))
}

/// The oid a status placed: resting, or filled on entry, alone.
const fn placed_oid(status: &WireStatus) -> Option<u64> {
    match status {
        WireStatus::Object(WireStatusObject { error: None, resting: Some(order), filled: None } | WireStatusObject { error: None, resting: None, filled: Some(order) }) => {
            Some(order.oid)
        }
        _ => None,
    }
}

/// What a block log event brings the pending set.
#[derive(Debug)]
pub(crate) enum BlockInput {
    /// A block; `after_loss` when taken while the watcher had found a loss not yet reported (it may follow it).
    Block { block: LoggedBlock, after_loss: bool },
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

    pub(crate) fn on_event(&mut self, event: FeedEvent<LoggedBlock>) -> BlockInput {
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
    const LINE: &str = r#"{"abci_block":{"time":"2026-10-31T15:33:32.681320216","signed_action_bundles":[["0x969bc4d41af39390282ad4f9445295b0b5a8032ce88fd7dbd95798e9802f70ea",{"signed_actions":[{"signature":{"r":"0x1","s":"0x2","v":27},"action":{"type":"noop","nonce":1},"nonce":1}],"broadcaster":"0xc9ba414780c1b75e9806e3469300993fd2bfa883","broadcaster_nonce":1793459645301}],["0x89804023c85e6566f5cf3d3a2b363554de673e82e00b83e8caddab51e445eb6f",{"signed_actions":[]}]],"round":1508694360,"parent_round":1508694359,"hardfork":{"version":0,"round":0},"proposer":"0xa82fe73bbd768bc15d1ef2f6142a21ff8bd762ad"},"resps":{"Full":[["0x89804023c85e6566f5cf3d3a2b363554de673e82e00b83e8caddab51e445eb6f",[]],["0x969bc4d41af39390282ad4f9445295b0b5a8032ce88fd7dbd95798e9802f70ea",[{"user":null,"res":{"status":"ok","response":{"type":"default"}}}]]]},"height":1204123719}"#;

    /// bm 10-31 block 1204388049's two ALOs, `{"p": 3}`, both resting; block 1204388011's IOC with `{"p": 14136}`,
    /// missing.
    pub(crate) const PRIORITY_ALOS: &str = r#"{"signature":{"r":"0x6e69dfd9b9840996fba8273904e36f635ed9ffcc4bea0c54bc1f17db61bcba82","s":"0x1ba742f46b2d12f3c2cbc761026a9cdbda426c8ab66436ff9a4e8eb0a5c98096","v":28},"action":{"type":"order","orders":[{"a":74,"b":true,"p":"4.4195","s":"226.4","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0xfa6470eddbd87dae00000900031bcee9"},{"a":159,"b":true,"p":"87.949","s":"9.95","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0xc7dd02d78b09e4390000090003139fad"}],"grouping":{"p":3}},"nonce":1793479712578,"expiresAfter":1793479712866}"#;
    const PRIORITY_ALOS_RESPONSE: &str = r#"{"user":"0x09f15dacf13c0fcb64594452506b52393fb13c39","res":{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":591588446177,"cloid":"0xfa6470eddbd87dae00000900031bcee9"}},{"resting":{"oid":591588446178,"cloid":"0xc7dd02d78b09e4390000090003139fad"}}]}}}}"#;
    const PRIORITY_IOC: &str = r#"{"signature":{"r":"0xf6f671f3a3c7bf88c0ee49641dc12d28bf71730da4bb4034f4e1cd5f2504f9ae","s":"0x352db2e6e038239729007cc929741259908d60f5304458b20c367c0bb081d024","v":27},"action":{"type":"order","orders":[{"a":46,"b":true,"p":"1.8048","s":"5.6","r":false,"t":{"limit":{"tif":"Ioc"}},"c":"0x0000000000000000099f650b79e76395"}],"grouping":{"p":14136}},"nonce":1793479709157}"#;
    const PRIORITY_IOC_RESPONSE: &str = r#"{"user":"0x8e555c7f07eebea796dec9b9273e8c1b88a1abdc","res":{"status":"ok","response":{"type":"order","data":{"statuses":[{"error":"Order could not immediately match against any resting orders. asset=46"}]}}}}"#;

    /// A block of one bundle holding `actions`, answered by `responses`.
    fn block_of(actions: &[&str], responses: &[&str]) -> String {
        let hash = "0x969bc4d41af39390282ad4f9445295b0b5a8032ce88fd7dbd95798e9802f70ea";
        format!(
            r#"{{"abci_block":{{"time":"2026-10-31T21:28:32.681320216","signed_action_bundles":[["{hash}",{{"signed_actions":[{}]}}]]}},"resps":{{"Full":[["{hash}",[{}]]]}},"height":1204388049}}"#,
            actions.join(","),
            responses.join(",")
        )
    }

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

    /// Batch 2f: the orders an eligible priority action placed carry its priority; an IOC's, an ALO action's made
    /// ineligible - a reduce-only order, an outcome's asset among its orders -, and a `na` grouping's carry none.
    #[test]
    fn a_line_reads_the_priority_of_each_order_it_placed() {
        let block = parse_block(&block_of(&[PRIORITY_ALOS, PRIORITY_IOC], &[PRIORITY_ALOS_RESPONSE, PRIORITY_IOC_RESPONSE])).unwrap();
        assert_eq!(block.priorities, [(591_588_446_177, 3), (591_588_446_178, 3)]);
        let ineligible = [
            PRIORITY_ALOS.replacen(r#""r":false"#, r#""r":true"#, 1),
            PRIORITY_ALOS.replacen(r#""a":159"#, r#""a":100014730"#, 1),
            PRIORITY_ALOS.replace(r#"{"p":3}"#, r#""na""#),
        ];
        for action in ineligible {
            assert_eq!(parse_block(&block_of(&[&action], &[PRIORITY_ALOS_RESPONSE])).unwrap().priorities, [], "{action}");
        }
    }

    /// A grouping not written as HL writes one, responses not pairing with the bundle's actions, or a priority
    /// action's statuses not pairing with its orders, do not read.
    #[test]
    fn a_line_whose_priorities_do_not_read_does_not_read() {
        assert!(parse_block(&block_of(&[&PRIORITY_ALOS.replace(r#"{"p":3}"#, r#"{"p":-3}"#)], &[PRIORITY_ALOS_RESPONSE])).is_err());
        assert!(parse_block(&block_of(&[PRIORITY_ALOS, PRIORITY_IOC], &[PRIORITY_ALOS_RESPONSE])).is_err());
        let default = r#"{"user":"0x09f15dacf13c0fcb64594452506b52393fb13c39","res":{"status":"ok","response":{"type":"default"}}}"#;
        assert!(parse_block(&block_of(&[PRIORITY_ALOS], &[default])).is_err());
        let refused = r#"{"user":"0x09f15dacf13c0fcb64594452506b52393fb13c39","res":{"status":"ok","response":{"type":"order","data":{"statuses":[{"error":"Price must be divisible by tick size. asset=74"}]}}}}"#;
        assert_eq!(parse_block(&block_of(&[PRIORITY_ALOS], &[refused])).unwrap().priorities, []);
        // review 000620 r3 P2: a status neither placed nor refused, or both, does not read; one refused item is skipped
        let second = r#"{"resting":{"oid":591588446178,"cloid":"0xc7dd02d78b09e4390000090003139fad"}}"#;
        assert!(PRIORITY_ALOS_RESPONSE.contains(second));
        let with = |status: &str| PRIORITY_ALOS_RESPONSE.replace(second, status);
        for unreadable in [r#"{"unknown":1}"#, r#""waitingForFill""#, r#"{"error":"x","resting":{"oid":77}}"#] {
            assert!(parse_block(&block_of(&[PRIORITY_ALOS], &[&with(unreadable)])).is_err(), "{unreadable}");
        }
        let unknown_status = PRIORITY_ALOS_RESPONSE.replace(r#""status":"ok""#, r#""status":"pending""#);
        assert!(parse_block(&block_of(&[PRIORITY_ALOS], &[&unknown_status])).is_err());
        let refused_whole = r#"{"user":"0x09f15dacf13c0fcb64594452506b52393fb13c39","res":{"status":"err","response":"Action already expired"}}"#;
        assert_eq!(parse_block(&block_of(&[PRIORITY_ALOS], &[refused_whole])).unwrap().priorities, []);
        let one_refused = with(r#"{"error":"Post only order would have immediately matched, bbo was 87.948@87.949. asset=159"}"#);
        assert_eq!(parse_block(&block_of(&[PRIORITY_ALOS], &[&one_refused])).unwrap().priorities, [(591_588_446_177, 3)]);
    }

    /// An order action a `multiSig` executed: its orders carry its priority under the multiSig's response.
    #[test]
    fn a_multisig_order_reads_its_priority() {
        let action_start = PRIORITY_ALOS.find(r#""action":"#).unwrap() + r#""action":"#.len();
        let action_end = PRIORITY_ALOS.find(r#","nonce""#).unwrap();
        let inner = &PRIORITY_ALOS[action_start..action_end];
        let multisig = format!(
            r#"{{"signature":{{"r":"0x1","s":"0x2","v":27}},"action":{{"type":"multiSig","signatureChainId":"0x66eee","signatures":[],"payload":{{"multiSigUser":"0x09f15dacf13c0fcb64594452506b52393fb13c39","outerSigner":"0x1","action":{inner}}}}},"nonce":1}}"#
        );
        assert_eq!(parse_block(&block_of(&[&multisig], &[PRIORITY_ALOS_RESPONSE])).unwrap().priorities, [(591_588_446_177, 3), (591_588_446_178, 3)]);
    }

    #[test]
    fn a_line_without_its_height_or_a_hash_does_not_read() {
        assert!(parse_block(&LINE.replace(r#","height":1204123719"#, "")).is_err());
        assert!(parse_block(&LINE.replace("0x969bc4d41af39390282ad4f9445295b0b5a8032ce88fd7dbd95798e9802f70ea", "0x96")).is_err());
        assert!(parse_block(&LINE.replace("2026-10-31T15:33:32.681320216", "yesterday")).is_err());
    }
}
