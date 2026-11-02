//! The node's block log for the pending set (hyperliquid-trade design 000604 r6 revision 14):
//! `<data-dir>/replica_cmds/<session>/<date>/<first height>`, a line per block, `{"abci_block": {"time",
//! "signed_action_bundles": [[hash, {...}], ...], ...}, "resps": ..., "height"}`, read as the account view reads it
//! (`ReplicaLine`). Of each: its height, time and its bundles' hashes - a bundle's hash is the mempool line's `tx_hash`
//! it ran (research `mempool_pending_book_20261031`, 10-31 after the node upgrade: 18,939 of 18,939 bundles) -, and the
//! oids its order actions with an ALO priority placed, with the priority (batch 2f: a confirmed order's place in its
//! level's queue), its taker orders' outcomes for the takers' margin refusals (batch 2g, `refusals`), and the account
//! each signer it recovered acted for (batch 2m-2, `signer_accounts`). Read by the
//! pending thread's own watcher, which parses each line - the account view parses each on its own, later - from the end
//! of the current file.

use std::{path::Path, thread, time::Instant as ReadInstant};

use alloy::primitives::{Address, B256};
use log::error;
use rustc_hash::FxHashMap;
use tokio::time::Instant;

use super::{
    feed::{self, Feed, FeedEvent, FeedLosses, FeedReports},
    mempool::order_tif,
    refusals::{Refusal, TakerKey, TakerOutcome},
    signer::{self, ActingIdentity},
};
use crate::{
    accounts::replica::{Action, PairedAction, ReplicaLine, WireModify, WireOrder, WireStatus, WireStatusObject, alo_priority, response_statuses},
    listeners::order_book::{
        parallel::{FileLayout, LineShape, LineStream},
        throttled_log_due,
    },
    metrics::{PENDING_BOOK_BLOCK_LOG_LINES_TOTAL, PENDING_BOOK_BLOCK_LOG_LOSSES_TOTAL, PENDING_BOOK_SIGNERS_TOTAL, PENDING_BOOK_TAKER_OUTCOMES_TOTAL},
    types::node_data::node_time_unix_nanos,
};

/// The error of an order HL could not margin (bm 10-31: "Insufficient margin to place order. asset=159").
const MARGIN_REFUSED: &str = "Insufficient margin to place order";
/// The error of a reduce-only order that would increase its position (bm 11-02: "Reduce only order would increase
/// position. asset=200"; batch 2p).
const REDUCE_ONLY_REFUSED: &str = "Reduce only order would increase position";

/// The error of an IOC that met no level to take ("Order could not immediately match against any resting orders.
/// asset=46").
const IOC_MISSED: &str = "Order could not immediately match against any resting orders";

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
    /// Its taker orders' outcomes, in execution order (batch 2g).
    pub(crate) taker_outcomes: Vec<TakerOutcome>,
    /// (signer, account) of each action without a vault whose signer was recovered, its account the one its response
    /// names (batch 2m-2).
    pub(crate) signer_accounts: Vec<(Address, Address)>,
    /// The TWAPs its successful `twapCancel`s ended (batch 2h).
    pub(crate) twap_cancels: Vec<u64>,
    /// It holds a `SetGlobalAction`, whatever it did (batch 2n-2: a block that may write the main dex's marks).
    pub(crate) set_global: bool,
    /// When the watcher read its line: the TWAP book's completeness proof (design 000624 r3 P1).
    pub(crate) read_at: ReadInstant,
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

/// One block log line, as its watcher reads it; an `Err` says what does not read.
pub(crate) fn parse_block(line: &str) -> Result<LoggedBlock, String> {
    let read_at = ReadInstant::now();
    let block = ReplicaLine::parse(line).map_err(|err| format!("block log line: {err}"))?;
    let height = block.height;
    let time = block.time().ok_or_else(|| format!("block {height} without its time"))?;
    let unix_ns = node_time_unix_nanos(time).ok_or_else(|| format!("block {height} time {time:?}"))?;
    let time_ms = u64::try_from(unix_ns / 1_000_000).map_err(|_| format!("block {height} time {time:?} before 1970"))?;
    let hashes = block
        .bundle_hashes()
        .map(|hash| hash.parse::<B256>().map_err(|err| format!("block {height} bundle hash {hash:?}: {err}")))
        .collect::<Result<_, _>>()?;
    let paired = block.paired().map_err(|err| format!("block {height}: {err}"))?;
    let priorities = placed_priorities(&paired).map_err(|err| format!("block {height}: {err}"))?;
    let signers = signers(line, &block, &paired).map_err(|err| format!("block {height}: {err}"))?;
    let taker_outcomes = taker_outcomes(&block, &paired, &signers).map_err(|err| format!("block {height}: {err}"))?;
    let signer_accounts = signer_accounts(&paired, &signers).map_err(|err| format!("block {height}: {err}"))?;
    let twap_cancels = twap_cancels(&paired).map_err(|err| format!("block {height}: {err}"))?;
    let set_global = paired.iter().any(|paired| paired.action.action_type == "SetGlobalAction");
    Ok(LoggedBlock { height, time_ms, hashes, priorities, taker_outcomes, signer_accounts, twap_cancels, set_global, read_at })
}

/// The TWAPs the block's successful `twapCancel`s ended (`{"type": "twapCancel", "data": {"status": "success"}}`; an
/// error status ends none - HL API docs "Cancel a TWAP order").
fn twap_cancels(paired_actions: &[PairedAction<'_, '_>]) -> Result<Vec<u64>, String> {
    let mut canceled = Vec::new();
    for paired in paired_actions {
        if paired.action.action_type != "twapCancel" || !ran(paired)? {
            continue;
        }
        let at = |err: String| format!("bundle {} action {}: {err}", paired.bundle_hash, paired.position);
        let raw = paired.response.res.response.as_ref().ok_or_else(|| at("an ok twapCancel without a body".to_string()))?;
        let body: WireTwapCancelResponse = sonic_rs::from_str(raw.as_raw_str()).map_err(|err| at(format!("twapCancel response {}: {err}", raw.as_raw_str())))?;
        if matches!(body.data.status, WireTwapCancelStatus::Text(ref text) if text == "success") {
            canceled.push(paired.action.field(paired.action.t.as_ref(), "t").map_err(at)?);
        }
    }
    Ok(canceled)
}

#[derive(serde::Deserialize)]
struct WireTwapCancelResponse {
    data: WireTwapCancelData,
}

#[derive(serde::Deserialize)]
struct WireTwapCancelData {
    status: WireTwapCancelStatus,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum WireTwapCancelStatus {
    Text(String),
    Error {
        #[expect(dead_code, reason = "an error cancels nothing")]
        error: String,
    },
}

/// Whether an action ran: its status `ok`; one refused whole (`err`) placed nothing, and a status neither is not
/// taken as either (review 000620 r4 P2).
fn ran(paired: &PairedAction<'_, '_>) -> Result<bool, String> {
    match paired.response.res.status {
        "ok" => Ok(true),
        "err" => Ok(false),
        status => Err(format!("bundle {} action {}: status {status:?}", paired.bundle_hash, paired.position)),
    }
}

/// (oid, priority) of each order the block's order actions with an ALO priority placed, by their responses - one a
/// `multiSig` executed too, under its response (review 000620 r2: a priority not read is not none; none seen on bm).
fn placed_priorities(paired_actions: &[PairedAction<'_, '_>]) -> Result<Vec<(u64, u64)>, String> {
    let mut placed = Vec::new();
    for paired in paired_actions {
        if !ran(paired)? {
            continue;
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
        let statuses = response_statuses(paired)?;
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

/// The signer of each action without a vault the pending set reads one of (`reads_signer`) that ran, by (bundle,
/// position) - `None` where it cannot be told -, the actions as written read from `line` in one pass. A `multiSig`'s,
/// whose signer is not its account's, is left out (the pending set never holds it).
fn signers(line: &str, block: &ReplicaLine<'_>, paired_actions: &[PairedAction<'_, '_>]) -> Result<FxHashMap<(usize, usize), Option<Address>>, String> {
    let mut wanted = Vec::new();
    for paired in paired_actions {
        let at = |err: String| format!("bundle {} action {}: {err}", paired.bundle_hash, paired.position);
        if !ran(paired)? || !reads_signer(paired.action).map_err(at)? {
            continue;
        }
        let signed = block.signed_action(paired.bundle, paired.position).ok_or_else(|| at("not in its bundle".to_string()))?;
        if signed.vault().map_err(at)?.is_none() {
            wanted.push((paired, signed));
        }
    }
    if wanted.is_empty() {
        return Ok(FxHashMap::default());
    }
    let paths = wanted
        .iter()
        .map(|(paired, _)| sonic_rs::pointer!["abci_block", "signed_action_bundles", paired.bundle, 1, "signed_actions", paired.position, "action"]);
    let written = signer::actions_written(line, paths)?;
    if written.len() != wanted.len() {
        return Err(format!("{} actions written for {} wanted", written.len(), wanted.len()));
    }
    let mut signers = FxHashMap::default();
    for ((paired, signed), action) in wanted.into_iter().zip(written) {
        let signer = signer::signer(signed, action.as_raw_str()).map_err(|err| format!("bundle {} action {}: {err}", paired.bundle_hash, paired.position))?;
        PENDING_BOOK_SIGNERS_TOTAL.with_label_values(&["block_log", if signer.is_some() { "recovered" } else { "unknown" }]).inc();
        signers.insert((paired.bundle, paired.position), signer);
    }
    Ok(signers)
}

/// Whether the pending set reads an action's signer: an `order` with an order that may take (batch 2g: its taker's
/// margin refusals), or an `order`, `modify` or `batchModify` placing a trigger order (batch 2m-2: its account).
fn reads_signer(action: &Action<'_>) -> Result<bool, String> {
    let orders: Vec<WireOrder> = match action.action_type {
        "order" => action.field(action.orders.as_ref(), "orders")?,
        "modify" => vec![action.field(action.order.as_ref(), "order")?],
        "batchModify" => action.field::<Vec<WireModify>>(action.modifies.as_ref(), "modifies")?.into_iter().map(|modify| modify.order).collect(),
        _ => return Ok(false),
    };
    let places_trigger = orders.iter().any(|order| order.t.trigger.is_some());
    Ok(places_trigger || (action.action_type == "order" && !taking_items(&orders)?.is_empty()))
}

/// (signer, account) of each action whose signer was recovered: the account its response names (batch 2m-2).
fn signer_accounts(paired_actions: &[PairedAction<'_, '_>], signers: &FxHashMap<(usize, usize), Option<Address>>) -> Result<Vec<(Address, Address)>, String> {
    let mut learned = Vec::new();
    for paired in paired_actions {
        let Some(Some(signer)) = signers.get(&(paired.bundle, paired.position)) else { continue };
        let at = |err: String| format!("bundle {} action {}: {err}", paired.bundle_hash, paired.position);
        let user = paired.response.user.ok_or_else(|| at("ran without its user".to_string()))?;
        let account: Address = user.parse().map_err(|err| at(format!("user {user:?}: {err}")))?;
        learned.push((*signer, account));
    }
    Ok(learned)
}

/// Each taker order's outcome (design 000621 r3), in execution order - the block's, each action's orders in theirs; a
/// taker's are all in its bundle's last category, which keeps the proposal order: the orders that may take
/// (`RequestedTif::may_take`) of the `order` actions that ran, by their statuses. Refused for margin, or cleared -
/// placed (filled, resting), or an IOC missing every level -; any other error says nothing, nor does a batch refused
/// whole but for margin. Its identity is the vault the action names, else its signer (`signers`); a `multiSig`'s
/// orders are left out.
fn taker_outcomes(block: &ReplicaLine<'_>, paired_actions: &[PairedAction<'_, '_>], signers: &FxHashMap<(usize, usize), Option<Address>>) -> Result<Vec<TakerOutcome>, String> {
    let mut taken = Vec::new();
    for paired in paired_actions {
        if !ran(paired)? || paired.action.action_type != "order" {
            continue;
        }
        let orders: Vec<WireOrder> = paired.action.field(paired.action.orders.as_ref(), "orders")?;
        let at = |err: String| format!("bundle {} action {}: {err}", paired.bundle_hash, paired.position);
        // an action without an order that may take has no response the batch reads (review 000621 r4 P2-2)
        let taking = taking_items(&orders).map_err(at)?;
        if taking.is_empty() {
            continue;
        }
        let outcomes = order_outcomes(&orders, &taking, &response_statuses(paired)?).map_err(at)?;
        if outcomes.is_empty() {
            continue;
        }
        let signed = block.signed_action(paired.bundle, paired.position).ok_or_else(|| at("not in its bundle".to_string()))?;
        let vault = signed.vault().map_err(at)?;
        let recovered = match vault {
            Some(_) => None,
            None => *signers.get(&(paired.bundle, paired.position)).ok_or_else(|| at("its signer not read".to_string()))?,
        };
        let Some(identity) = ActingIdentity::of(vault, recovered) else {
            PENDING_BOOK_TAKER_OUTCOMES_TOTAL.with_label_values(&["no_identity"]).inc_by(u64::try_from(outcomes.len()).unwrap_or(u64::MAX));
            continue;
        };
        for OrderOutcome { asset, is_buy, reduce_only, refused } in outcomes {
            let label = match refused {
                Some(Refusal::Margin) => "refused",
                Some(Refusal::ReduceOnly) => "refused_reduce_only",
                None => "cleared",
            };
            PENDING_BOOK_TAKER_OUTCOMES_TOTAL.with_label_values(&[label]).inc();
            taken.push(TakerOutcome { key: TakerKey { identity, asset, is_buy }, reduce_only, refused });
        }
    }
    Ok(taken)
}

/// The places of `orders` that may take (`RequestedTif::may_take`).
fn taking_items(orders: &[WireOrder]) -> Result<Vec<usize>, String> {
    let mut taking = Vec::new();
    for (item, order) in orders.iter().enumerate() {
        if order_tif(order).map_err(|err| format!("item {item}: {err}"))?.may_take() {
            taking.push(item);
        }
    }
    Ok(taking)
}

/// A taker order's outcome by its status: its asset, side and reduce-only flag; refused and why, or cleared (`None`).
struct OrderOutcome {
    asset: u32,
    is_buy: bool,
    reduce_only: bool,
    refused: Option<Refusal>,
}

impl OrderOutcome {
    const fn of(order: &WireOrder, refused: Option<Refusal>) -> Self {
        Self { asset: order.a, is_buy: order.b, reduce_only: order.r, refused }
    }
}

/// The outcome of each order of `orders` at `taking` whose status tells one. The statuses are one per order, or one
/// error for them all - a refused batch, which only a margin error tells of: each taker refused; any other, an IOC's
/// miss among them, says nothing of any one (design 000621 r3; review 000621 r4 P2-1).
fn order_outcomes(orders: &[WireOrder], taking: &[usize], statuses: &[WireStatus]) -> Result<Vec<OrderOutcome>, String> {
    if statuses.len() != orders.len() {
        let [status] = statuses else { return Err(format!("{} orders with {} statuses", orders.len(), statuses.len())) };
        if !refused(status) {
            return Err(format!("{} orders with one status not an error", orders.len()));
        }
        let margin = matches!(taker_status(status), Some(TakerStatus::Refused(Refusal::Margin)));
        let refused_each = |item: &usize| OrderOutcome::of(&orders[*item], Some(Refusal::Margin));
        return Ok(if margin { taking.iter().map(refused_each).collect() } else { Vec::new() });
    }
    let mut outcomes = Vec::new();
    for item in taking {
        let order = &orders[*item];
        match taker_status(&statuses[*item]).ok_or_else(|| format!("item {item}: a status neither placed nor refused"))? {
            TakerStatus::Refused(refusal) => outcomes.push(OrderOutcome::of(order, Some(refusal))),
            TakerStatus::Cleared => outcomes.push(OrderOutcome::of(order, None)),
            TakerStatus::Silent => {}
        }
    }
    Ok(outcomes)
}

/// What a taker order's status says of its taker.
enum TakerStatus {
    /// Refused for margin, or - batch 2p - a reduce-only order that would increase its position.
    Refused(Refusal),
    /// Placed (filled, resting), or an IOC that missed every level.
    Cleared,
    /// Any other error.
    Silent,
}

/// `None` for a status neither placed nor an error alone.
fn taker_status(status: &WireStatus) -> Option<TakerStatus> {
    if placed_oid(status).is_some() {
        return Some(TakerStatus::Cleared);
    }
    let WireStatus::Object(WireStatusObject { error: Some(error), resting: None, filled: None }) = status else { return None };
    Some(if error.contains(MARGIN_REFUSED) {
        TakerStatus::Refused(Refusal::Margin)
    } else if error.contains(REDUCE_ONLY_REFUSED) {
        TakerStatus::Refused(Refusal::ReduceOnly)
    } else if error.contains(IOC_MISSED) {
        TakerStatus::Cleared
    } else {
        TakerStatus::Silent
    })
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
    pub(crate) fn block_of(actions: &[&str], responses: &[&str]) -> String {
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

    /// A vault's order action of an IOC (asset 1, buy), an ALO (2), a GTC (3, sell), a `FrontendMarket` (4) and a
    /// trigger order (5).
    const VAULT_TAKERS: &str = r#"{"signature":{"r":"0x1","s":"0x2","v":27},"vaultAddress":"0xbff945932b5ff40699e9f12fa40927c5667df619","action":{"type":"order","orders":[{"a":1,"b":true,"p":"10","s":"1","r":false,"t":{"limit":{"tif":"Ioc"}}},{"a":2,"b":true,"p":"10","s":"1","r":false,"t":{"limit":{"tif":"Alo"}}},{"a":3,"b":false,"p":"10","s":"1","r":false,"t":{"limit":{"tif":"Gtc"}}},{"a":4,"b":true,"p":"10","s":"1","r":false,"t":{"limit":{"tif":"FrontendMarket"}}},{"a":5,"b":true,"p":"10","s":"1","r":false,"t":{"trigger":{"isMarket":true,"triggerPx":"9","tpsl":"sl"}}}],"grouping":"na"},"nonce":1}"#;

    fn vault_response(statuses: &str) -> String {
        format!(r#"{{"user":"0xbff945932b5ff40699e9f12fa40927c5667df619","res":{{"status":"ok","response":{{"type":"order","data":{{"statuses":[{statuses}]}}}}}}}}"#)
    }

    /// (asset, is buy, refused for margin) of the block's taker outcomes, all of the one vault.
    fn vault_outcomes(action: &str, response: &str) -> Result<Vec<(u32, bool, bool)>, String> {
        let outcomes = vault_taker_outcomes(action, response)?;
        Ok(outcomes.iter().map(|outcome| (outcome.key.asset, outcome.key.is_buy, outcome.refused == Some(Refusal::Margin))).collect())
    }

    fn vault_taker_outcomes(action: &str, response: &str) -> Result<Vec<TakerOutcome>, String> {
        let vault = ActingIdentity::Vault("0xbff945932b5ff40699e9f12fa40927c5667df619".parse().unwrap());
        let block = parse_block(&block_of(&[action], &[response]))?;
        assert!(block.taker_outcomes.iter().all(|outcome| outcome.key.identity == vault));
        Ok(block.taker_outcomes)
    }

    /// Batch 2g: the orders that may take, by their statuses - a margin error refuses, a fill, a resting order or an IOC
    /// missing every level clears, another error says nothing; an ALO's and a trigger order's statuses are not read.
    #[test]
    fn a_line_reads_its_taker_orders_outcomes() {
        const MARGIN: &str = r#"{"error":"Insufficient margin to place order. asset=1"}"#;
        const RESTING: &str = r#"{"resting":{"oid":11}}"#;
        const FILLED: &str = r#"{"filled":{"totalSz":"1","avgPx":"10","oid":12}}"#;
        const MISSED: &str = r#"{"error":"Order could not immediately match against any resting orders. asset=1"}"#;
        const OTHER: &str = r#"{"error":"Order price cannot be more than 95% away from the reference price"}"#;
        let statuses = |taker: [&str; 3]| vault_response(&format!(r#"{},{RESTING},{},{},"waitingForTrigger""#, taker[0], taker[1], taker[2]));
        assert_eq!(vault_outcomes(VAULT_TAKERS, &statuses([MARGIN, RESTING, FILLED])), Ok(vec![(1, true, true), (3, false, false), (4, true, false)]));
        assert_eq!(vault_outcomes(VAULT_TAKERS, &statuses([MISSED, MARGIN, OTHER])), Ok(vec![(1, true, false), (3, false, true)]));
        // one error for the batch: a margin one refuses each taker, another says nothing
        assert_eq!(vault_outcomes(VAULT_TAKERS, &vault_response(MARGIN)), Ok(vec![(1, true, true), (3, false, true), (4, true, true)]));
        assert_eq!(vault_outcomes(VAULT_TAKERS, &vault_response(OTHER)), Ok(vec![]));
        // review 000621 r4 P2-1: one IOC's miss for the batch says nothing of each order, unlike one order's own
        assert_eq!(vault_outcomes(VAULT_TAKERS, &vault_response(MISSED)), Ok(vec![]));
        // refused whole: nothing
        let refused_whole = r#"{"user":"0xbff945932b5ff40699e9f12fa40927c5667df619","res":{"status":"err","response":"Insufficient margin to place order. asset=1"}}"#;
        assert_eq!(vault_outcomes(VAULT_TAKERS, refused_whole), Ok(vec![]));
    }

    /// Batch 2p: a reduce-only taker refused as it would increase its position - with its order's reduce-only flag -, the
    /// plain takers' outcomes as before; one such error for the batch says nothing of each order.
    #[test]
    fn a_line_reads_its_reduce_only_refusals() {
        const REDUCE_ONLY: &str = r#"{"error":"Reduce only order would increase position. asset=1"}"#;
        const RESTING: &str = r#"{"resting":{"oid":11}}"#;
        const FILLED: &str = r#"{"filled":{"totalSz":"1","avgPx":"10","oid":12}}"#;
        let action = VAULT_TAKERS.replacen(r#""r":false,"t":{"limit":{"tif":"Ioc"}}"#, r#""r":true,"t":{"limit":{"tif":"Ioc"}}"#, 1);
        let response = vault_response(&format!(r#"{REDUCE_ONLY},{RESTING},{RESTING},{FILLED},"waitingForTrigger""#));
        let outcomes = vault_taker_outcomes(&action, &response).unwrap();
        let read: Vec<_> = outcomes.iter().map(|outcome| (outcome.key.asset, outcome.reduce_only, outcome.refused)).collect();
        assert_eq!(read, [(1, true, Some(Refusal::ReduceOnly)), (3, false, None), (4, false, None)]);
        assert_eq!(vault_taker_outcomes(&action, &vault_response(REDUCE_ONLY)), Ok(vec![]));
    }

    /// Review 000621 r4 P2-2: an action without an order that may take - ALOs without a priority, trigger orders -
    /// reads whatever its response, as before batch 2g; one with such an order and a response telling nothing of it does
    /// not.
    #[test]
    fn a_response_without_a_taker_is_not_read_for_takers() {
        const IOC: &str = r#"{"a":1,"b":true,"p":"10","s":"1","r":false,"t":{"limit":{"tif":"Ioc"}}}"#;
        const ALO: &str = r#"{"a":2,"b":true,"p":"10","s":"1","r":false,"t":{"limit":{"tif":"Alo"}}}"#;
        const TRIGGER: &str = r#"{"a":5,"b":true,"p":"10","s":"1","r":false,"t":{"trigger":{"isMarket":true,"triggerPx":"9","tpsl":"sl"}}}"#;
        assert!(VAULT_TAKERS.contains(IOC) && VAULT_TAKERS.contains(ALO) && VAULT_TAKERS.contains(TRIGGER));
        let action = |orders: &[&str]| {
            format!(
                r#"{{"signature":{{"r":"0x1","s":"0x2","v":27}},"vaultAddress":"0xbff945932b5ff40699e9f12fa40927c5667df619","action":{{"type":"order","orders":[{}],"grouping":"na"}},"nonce":1}}"#,
                orders.join(",")
            )
        };
        let default = r#"{"user":"0xbff945932b5ff40699e9f12fa40927c5667df619","res":{"status":"ok","response":{"type":"default"}}}"#;
        for orders in [&[ALO][..], &[TRIGGER], &[ALO, TRIGGER]] {
            let action = action(orders);
            assert_eq!(parse_block(&block_of(&[&action], &[default])).unwrap().taker_outcomes, [], "{action}");
        }
        assert!(parse_block(&block_of(&[&action(&[IOC, ALO])], &[default])).is_err());
    }

    /// Batch 2h: a successful `twapCancel` ends its TWAP; one refused, or answered with an error status, ends none.
    #[test]
    fn a_line_reads_its_twap_cancels() {
        let cancel = |twap: u64| format!(r#"{{"signature":{{"r":"0x1","s":"0x2","v":27}},"action":{{"type":"twapCancel","a":159,"t":{twap}}},"nonce":1}}"#);
        let answer = |status: &str| format!(r#"{{"user":"0xbff945932b5ff40699e9f12fa40927c5667df619","res":{{"status":"ok","response":{{"type":"twapCancel","data":{{"status":{status}}}}}}}}}"#);
        let refused = r#"{"user":"0xbff945932b5ff40699e9f12fa40927c5667df619","res":{"status":"err","response":"Invalid nonce"}}"#;
        let line = block_of(
            &[&cancel(2_470_033), &cancel(2_470_034), &cancel(2_470_035)],
            &[&answer(r#""success""#), &answer(r#"{"error":"TWAP was never placed, already canceled, or filled."}"#), refused],
        );
        assert_eq!(parse_block(&line).unwrap().twap_cancels, [2_470_033]);
        assert!(parse_block(&block_of(&[&cancel(1)], &[r#"{"user":null,"res":{"status":"ok","response":{"type":"default"}}}"#])).is_err());
    }

    /// A taker's status neither placed nor refused, or statuses not pairing with the orders, do not read; a `multiSig`'s
    /// orders give no outcome.
    #[test]
    fn a_line_whose_taker_outcomes_do_not_read_does_not_read() {
        for status in [r#"{"unknown":1}"#, r#""success""#, r#"{"error":"Insufficient margin to place order. asset=1","resting":{"oid":11}}"#] {
            let response = vault_response(&format!(r#"{status},{{"resting":{{"oid":11}}}},{{"resting":{{"oid":13}}}},{{"resting":{{"oid":14}}}},"waitingForTrigger""#));
            assert!(vault_outcomes(VAULT_TAKERS, &response).is_err(), "{status}");
        }
        assert!(vault_outcomes(VAULT_TAKERS, &vault_response(r#"{"resting":{"oid":11}},{"resting":{"oid":12}}"#)).is_err());
        let action_start = VAULT_TAKERS.find(r#""action":"#).unwrap() + r#""action":"#.len();
        let inner = &VAULT_TAKERS[action_start..VAULT_TAKERS.find(r#","nonce""#).unwrap()];
        let multisig = format!(
            r#"{{"signature":{{"r":"0x1","s":"0x2","v":27}},"action":{{"type":"multiSig","signatureChainId":"0x66eee","signatures":[],"payload":{{"multiSigUser":"0x09f15dacf13c0fcb64594452506b52393fb13c39","outerSigner":"0x1","action":{inner}}}}},"nonce":1}}"#
        );
        assert_eq!(parse_block(&block_of(&[&multisig], &[&vault_response(r#"{"error":"Insufficient margin to place order. asset=1"}"#)])).unwrap().taker_outcomes, []);
    }

    /// Batch 2m-2: a block teaches the account each signer it reads acts for - an `order` that may take, one placing a
    /// trigger order, without a vault -, as its response names it; an ALO-only action and a vault's teach nothing. A ran
    /// action whose response names no user does not read.
    #[test]
    fn a_line_teaches_the_accounts_of_the_signers_it_reads() {
        let trigger = crate::pending::mempool::tests::TRIGGER;
        let waiting = |user: &str| format!(r#"{{"user":{user},"res":{{"status":"ok","response":{{"type":"order","data":{{"statuses":["waitingForTrigger"]}}}}}}}}"#);
        let alice = r#""0x00000000000000000000000000000000000000a1""#;
        let vault_trigger = trigger.replace(r#""action":"#, r#""vaultAddress":"0xbff945932b5ff40699e9f12fa40927c5667df619","action":"#);
        let block = parse_block(&block_of(&[PRIORITY_IOC, trigger, PRIORITY_ALOS, &vault_trigger], &[PRIORITY_IOC_RESPONSE, &waiting(alice), PRIORITY_ALOS_RESPONSE, &waiting(alice)])).unwrap();
        let accounts: Vec<String> = block.signer_accounts.iter().map(|(_, account)| account.to_string().to_lowercase()).collect();
        assert_eq!(accounts, ["0x8e555c7f07eebea796dec9b9273e8c1b88a1abdc", "0x00000000000000000000000000000000000000a1"]);
        assert_eq!(block.signer_accounts[1].0, "0x12cb6a66fe95a62d901eee587499670e89b3a926".parse::<Address>().unwrap(), "the signer the mempool recovers");
        assert!(parse_block(&block_of(&[trigger], &[&waiting("null")])).is_err());
    }

    #[test]
    fn a_line_without_its_height_or_a_hash_does_not_read() {
        assert!(parse_block(&LINE.replace(r#","height":1204123719"#, "")).is_err());
        assert!(parse_block(&LINE.replace("0x969bc4d41af39390282ad4f9445295b0b5a8032ce88fd7dbd95798e9802f70ea", "0x96")).is_err());
        assert!(parse_block(&LINE.replace("2026-10-31T15:33:32.681320216", "yesterday")).is_err());
    }
}
