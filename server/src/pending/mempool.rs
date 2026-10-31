//! The node's mempool (hyperliquid-trade design 000604 section 2): `<data-dir>/mempool_txs/hourly/<YYYYMMDD>/<H>`, a
//! line per transaction the node saw before a block took it, `[local_time, {"tx_hash", "signed_actions": [{"action",
//! "nonce", "vaultAddress"?, "expiresAfter"?, "isFrontend"?, "signature"}]}]` (bm 10-31 09:10-09:11: 2,011 lines, ~140
//! signed actions a line, the longest 1.6 MB). The watcher thread reads the lines from the end of the current file and
//! parses each whole (signatures and unread fields skipped), in the file's order; the pending thread names each asset
//! id by its coin. Batch 2a counts what it reads; the overlay comes with batch 2b.

use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
};

use alloy::primitives::{Address, B256};
use log::error;
use serde::Deserialize;
use sonic_rs::LazyValue;
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};

use crate::{
    accounts::{
        book::ActionCoins,
        replica::{Action, OrderRef, WireCancel, WireCancelByCloid, WireModify, WireOrder},
    },
    listeners::order_book::{
        parallel::{FileLayout, LineShape, LineStream, LossCounts, WatchLoss, seconds_since_node_write, spawn_line_watcher},
        throttled_log_due,
    },
    metrics::{
        NODE_LINE_LAG, PENDING_BOOK_MEMPOOL_ACTIONS_TOTAL, PENDING_BOOK_MEMPOOL_LINES_TOTAL,
        PENDING_BOOK_MEMPOOL_LOSSES_TOTAL, PENDING_BOOK_MEMPOOL_ORDERS_TOTAL, PENDING_BOOK_MEMPOOL_UNNAMED_ASSETS_TOTAL,
    },
    order_book::types::{Px, Sz},
    types::node_data::node_time_unix_nanos,
};

/// Cap on one unterminated line (the longest seen 1.6 MB, above).
const MAX_LINE_BYTES: usize = 64 << 20;

/// Lines queued for the pending thread (~26 a second on bm 10-31): a pending thread behind holds the watcher back,
/// which reads on from where it stopped; the lag shows in `NODE_LINE_LAG`'s `apply` of `mempool`.
const QUEUE_LINES: usize = 1_024;

/// A mempool transaction as read.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MempoolTx {
    /// The node's write time of the line (its `local_time`), unix ns.
    pub(crate) seen_ns: i64,
    pub(crate) tx_hash: B256,
    /// In the line's order.
    pub(crate) actions: Vec<SignedAction>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SignedAction {
    /// The vault or subaccount it acts for; otherwise the signer acts, whom the line does not name.
    pub(crate) vault: Option<Address>,
    pub(crate) kind: ActionKind,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ActionKind {
    Order(Vec<RequestedOrder>),
    /// `fast` is the action's `f` (`Action::fast`).
    Cancel { fast: bool, cancels: Vec<WireCancel> },
    CancelByCloid { fast: bool, cancels: Vec<WireCancelByCloid> },
    /// A `modify` (one) or a `batchModify`; `always_place` is the action's `a`.
    Modify { always_place: bool, modifies: Vec<Modify> },
    /// Any other type (counted only).
    Other,
}

/// An order as requested: one of an `order`'s, or a modify's new order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequestedOrder {
    pub(crate) asset: u32,
    pub(crate) is_buy: bool,
    pub(crate) limit_px: Px,
    pub(crate) sz: Sz,
    pub(crate) reduce_only: bool,
    pub(crate) cloid: Option<String>,
    pub(crate) tif: RequestedTif,
}

/// A limit order's time in force, or a trigger order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestedTif {
    Alo,
    Gtc,
    Ioc,
    FrontendMarket,
    Trigger,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Modify {
    pub(crate) target: OrderRef,
    pub(crate) order: RequestedOrder,
}

/// What the watcher sends, in the file's order.
#[derive(Debug)]
pub(crate) enum MempoolEvent {
    /// A line read whole.
    Tx(MempoolTx),
    /// A terminated line that does not read (`parse_line`'s error): its actions are lost, at no time of its own.
    /// `through` is its own place in the count of unreadable lines.
    Unreadable { error: String, through: u64 },
    /// Lines the reader skipped at its start, or discarded (a cut, overlong or not `[...]` line, a file lost);
    /// `through` is the reader's loss count when it went out: every loss it counted up to then is reported.
    Lost { loss: WatchLoss, through: u64 },
}

/// The watcher's events in the file's order, and its counts of what it lost.
pub(crate) struct MempoolFeed {
    pub(crate) events: mpsc::Receiver<MempoolEvent>,
    pub(crate) losses: MempoolLosses,
}

/// What the watcher lost, each count raised the moment the loss is found (review 000609 r1-r2 P2-1): the lines after
/// it, and the event reporting it, may wait behind a full queue. The reader's losses (`LossCounts`: found as soon as
/// known, an overflow included) are reported by `Lost`, each through those the watcher had flagged when it went out
/// (review 000609 r7: not through those merely found, an overflow still queued behind it); the lines that did not read
/// by `Unreadable`. Each count has its own reports, in its own order, so a report never covers a loss found after the
/// ones it reports.
#[derive(Clone, Default)]
pub(crate) struct MempoolLosses {
    reader: LossCounts,
    unreadable: Arc<AtomicU64>,
}

impl MempoolLosses {
    /// (reader losses, unreadable lines) found so far.
    fn found(&self) -> (u64, u64) {
        (self.reader.found.load(Ordering::Acquire), self.unreadable.load(Ordering::Acquire))
    }

    /// A line read: its transaction, or its error counted.
    fn event(&self, line: &str) -> MempoolEvent {
        parse_line(line).map_or_else(
            |error| MempoolEvent::Unreadable { error, through: self.unreadable.fetch_add(1, Ordering::Release) + 1 },
            MempoolEvent::Tx,
        )
    }

    fn lost(&self, loss: WatchLoss) -> MempoolEvent {
        MempoolEvent::Lost { loss, through: self.reader.flagged.load(Ordering::Acquire) }
    }
}

#[derive(Deserialize)]
struct WireTx<'a> {
    tx_hash: B256,
    #[serde(borrow)]
    signed_actions: Vec<WireSignedAction<'a>>,
}

#[derive(Deserialize)]
struct WireSignedAction<'a> {
    #[serde(borrow)]
    action: Action<'a>,
    #[serde(rename = "vaultAddress")]
    vault_address: Option<Address>,
}

/// Starts the watcher of `<data_dir>/mempool_txs`; its events come in order on the feed, which closes when the
/// watcher ends (its inotify watch not set up).
pub(crate) fn watch_mempool(data_dir: &Path) -> (MempoolFeed, thread::JoinHandle<()>) {
    let (tx, events) = mpsc::channel(QUEUE_LINES);
    let losses = MempoolLosses::default();
    let stream = LineStream {
        name: "Mempool",
        lag_label: "mempool",
        dir: data_dir.join("mempool_txs"),
        max_line_bytes: MAX_LINE_BYTES,
        start_skip_is_loss: true,
        layout: FileLayout::Hourly,
        shape: LineShape::Array,
        loss_counts: Some(losses.reader.clone()),
    };
    let (parsing, reporting) = (losses.clone(), losses.clone());
    let wrap = move |line: String| parsing.event(&line);
    let lost = move |loss| reporting.lost(loss);
    let watcher = spawn_line_watcher(stream, tx, None, wrap, lost, None);
    (MempoolFeed { events, losses }, watcher)
}

/// One mempool line; an `Err` says what does not read.
pub(crate) fn parse_line(line: &str) -> Result<MempoolTx, String> {
    let (local_time, tx): (&str, WireTx<'_>) = sonic_rs::from_str(line).map_err(|err| format!("mempool line: {err}"))?;
    let seen_ns = node_time_unix_nanos(local_time).ok_or_else(|| format!("mempool line local_time {local_time:?}"))?;
    let actions = tx
        .signed_actions
        .iter()
        .enumerate()
        .map(|(index, signed)| signed_action(signed).map_err(|err| format!("tx {} action {index}: {err}", tx.tx_hash)))
        .collect::<Result<_, _>>()?;
    Ok(MempoolTx { seen_ns, tx_hash: tx.tx_hash, actions })
}

fn signed_action(signed: &WireSignedAction<'_>) -> Result<SignedAction, String> {
    let action = &signed.action;
    let kind = match action.action_type {
        "order" => {
            let orders: Vec<WireOrder> = action.field(action.orders.as_ref(), "orders")?;
            ActionKind::Order(orders.iter().map(requested).collect::<Result<_, _>>()?)
        }
        "cancel" => {
            let fast = flag(action, action.fast.as_ref(), "f")?;
            ActionKind::Cancel { fast, cancels: action.field(action.cancels.as_ref(), "cancels")? }
        }
        "cancelByCloid" => {
            let fast = flag(action, action.fast.as_ref(), "f")?;
            ActionKind::CancelByCloid { fast, cancels: action.field(action.cancels.as_ref(), "cancels")? }
        }
        "modify" => {
            let target = action.field(action.oid.as_ref(), "oid")?;
            let order = requested(&action.field(action.order.as_ref(), "order")?)?;
            let always_place = flag(action, action.always_place.as_ref(), "a")?;
            ActionKind::Modify { always_place, modifies: vec![Modify { target, order }] }
        }
        "batchModify" => {
            let modifies: Vec<WireModify> = action.field(action.modifies.as_ref(), "modifies")?;
            let modifies = modifies
                .into_iter()
                .map(|modify| Ok(Modify { order: requested(&modify.order)?, target: modify.oid }))
                .collect::<Result<_, String>>()?;
            ActionKind::Modify { always_place: flag(action, action.always_place.as_ref(), "a")?, modifies }
        }
        _ => ActionKind::Other,
    };
    Ok(SignedAction { vault: signed.vault_address, kind })
}

/// An action's flag that is omitted when false (a modify's `a`, a cancel's `f`).
fn flag(action: &Action<'_>, raw: Option<&LazyValue<'_>>, name: &str) -> Result<bool, String> {
    raw.map_or(Ok(false), |raw| action.field(Some(raw), name))
}

fn requested(order: &WireOrder) -> Result<RequestedOrder, String> {
    let tif = match (&order.t.limit, &order.t.trigger) {
        (Some(limit), None) => limit_tif(&limit.tif)?,
        (None, Some(_)) => RequestedTif::Trigger,
        _ => return Err("order type neither limit nor trigger".to_string()),
    };
    Ok(RequestedOrder {
        asset: order.a,
        is_buy: order.b,
        limit_px: Px::parse_from_str(&order.p).map_err(|err| format!("order price {:?}: {err}", order.p))?,
        sz: Sz::parse_from_str(&order.s).map_err(|err| format!("order size {:?}: {err}", order.s))?,
        reduce_only: order.r,
        cloid: order.c.clone(),
        tif,
    })
}

fn limit_tif(tif: &str) -> Result<RequestedTif, String> {
    match tif {
        "Alo" => Ok(RequestedTif::Alo),
        "Gtc" => Ok(RequestedTif::Gtc),
        "Ioc" => Ok(RequestedTif::Ioc),
        "FrontendMarket" => Ok(RequestedTif::FrontendMarket),
        other => Err(format!("order tif {other:?}")),
    }
}

impl RequestedTif {
    const fn label(self) -> &'static str {
        match self {
            Self::Alo => "alo",
            Self::Gtc => "gtc",
            Self::Ioc => "ioc",
            Self::FrontendMarket => "frontend_market",
            Self::Trigger => "trigger",
        }
    }
}

impl ActionKind {
    const fn label(&self) -> &'static str {
        match self {
            Self::Order(_) => "order",
            Self::Cancel { .. } => "cancel",
            Self::CancelByCloid { .. } => "cancel_by_cloid",
            Self::Modify { .. } => "modify",
            Self::Other => "other",
        }
    }

    /// The asset ids it names, one per order, cancel or modify.
    fn assets(&self) -> Vec<u32> {
        match self {
            Self::Order(orders) => orders.iter().map(|order| order.asset).collect(),
            Self::Cancel { cancels, .. } => cancels.iter().map(|cancel| cancel.a).collect(),
            Self::CancelByCloid { cancels, .. } => cancels.iter().map(|cancel| cancel.asset).collect(),
            Self::Modify { modifies, .. } => modifies.iter().map(|modify| modify.order.asset).collect(),
            Self::Other => Vec::new(),
        }
    }

    fn orders(&self) -> impl Iterator<Item = &RequestedOrder> {
        let (orders, modifies) = match self {
            Self::Order(orders) => (orders.as_slice(), [].as_slice()),
            Self::Modify { modifies, .. } => ([].as_slice(), modifies.as_slice()),
            _ => ([].as_slice(), [].as_slice()),
        };
        orders.iter().chain(modifies.iter().map(|modify| &modify.order))
    }
}

/// The pending thread's reading of the mempool: the anchor's asset table, the losses reported so far, and the events
/// counted (batch 2a).
pub(crate) struct MempoolIngest {
    action_coins: watch::Receiver<Option<Arc<ActionCoins>>>,
    losses: MempoolLosses,
    /// (reader losses, unreadable lines) the events taken so far reported through.
    reported: (u64, u64),
    last_unreadable_log: Option<Instant>,
}

impl MempoolIngest {
    pub(crate) const fn new(
        action_coins: watch::Receiver<Option<Arc<ActionCoins>>>,
        losses: MempoolLosses,
    ) -> Self {
        Self { action_coins, losses, reported: (0, 0), last_unreadable_log: None }
    }

    /// Whether the watcher found a loss no event taken yet reports: what was taken since may follow it in the file.
    pub(crate) fn loss_unreported(&self) -> bool {
        let (reader, unreadable) = self.losses.found();
        reader > self.reported.0 || unreadable > self.reported.1
    }

    pub(crate) fn on_event(&mut self, event: MempoolEvent) {
        match event {
            MempoolEvent::Tx(tx) => self.on_tx(&tx),
            MempoolEvent::Unreadable { error, through } => {
                self.reported.1 = self.reported.1.max(through);
                PENDING_BOOK_MEMPOOL_LINES_TOTAL.with_label_values(&["unreadable"]).inc();
                if throttled_log_due(&mut self.last_unreadable_log, Instant::now()) {
                    error!("pending book: a mempool line does not read, its actions lost: {error}");
                }
            }
            MempoolEvent::Lost { loss, through } => {
                self.reported.0 = self.reported.0.max(through);
                PENDING_BOOK_MEMPOOL_LOSSES_TOTAL.with_label_values(&[loss_label(loss)]).inc();
            }
        }
    }

    fn on_tx(&self, tx: &MempoolTx) {
        NODE_LINE_LAG.with_label_values(&["apply", "mempool"]).observe(seconds_since_node_write(tx.seen_ns));
        let outcome = if self.loss_unreported() { "read_after_unreported_loss" } else { "read" };
        PENDING_BOOK_MEMPOOL_LINES_TOTAL.with_label_values(&[outcome]).inc();
        let coins = self.action_coins.borrow().clone();
        for action in &tx.actions {
            PENDING_BOOK_MEMPOOL_ACTIONS_TOTAL.with_label_values(&[action.kind.label()]).inc();
            for order in action.kind.orders() {
                PENDING_BOOK_MEMPOOL_ORDERS_TOTAL.with_label_values(&[order.tif.label()]).inc();
            }
            let unnamed = unnamed_assets(coins.as_deref(), &action.kind.assets());
            if unnamed > 0 {
                let reason = if coins.is_some() { "not_in_anchor" } else { "no_anchor" };
                PENDING_BOOK_MEMPOOL_UNNAMED_ASSETS_TOTAL.with_label_values(&[reason]).inc_by(unnamed);
            }
        }
    }
}

/// The assets the anchor's table does not name (all of them before the first anchor).
fn unnamed_assets(coins: Option<&ActionCoins>, assets: &[u32]) -> u64 {
    let unnamed = assets.iter().filter(|asset| coins.is_none_or(|coins| !coins.coins.contains_key(asset))).count();
    u64::try_from(unnamed).unwrap_or(u64::MAX)
}

const fn loss_label(loss: WatchLoss) -> &'static str {
    match loss {
        WatchLoss::Positioning => "positioning",
        WatchLoss::Discard => "discard",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEEN: &str = "2026-10-31T09:14:38.693300829";
    const TX_HASH: &str = "0x49ed0336871d4fc91663a015ae3cac1388cf48566a4b25b69f240a89c0ee6666";

    // Signed actions cut from bm's mempool (2026-10-31 09:14:38-09:14:42), whole but for signatures shortened.
    const ALO_BATCH: &str = r#"{"signature":{"r":"0x79e6","s":"0x297b","v":28},"vaultAddress":"0xfcf104006bfff47695c1dc21dad3e9de1e72098e","action":{"type":"order","orders":[{"a":0,"b":true,"p":"77387","s":"0.135","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x0000000000000000000001a18b7b1eef"},{"a":0,"b":true,"p":"77386","s":"0.135","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x0000000000000000000001a18b7b1ef0"}],"grouping":"na"},"nonce":1793438078596}"#;
    const IOC_PRIORITY: &str = r#"{"signature":{"r":"0xe91a","s":"0x4ccb","v":27},"action":{"type":"order","orders":[{"a":207,"b":true,"p":"0.00001","s":"1","r":false,"t":{"limit":{"tif":"Ioc"}},"c":"0x000000000000000018de99016980e8f8"}],"grouping":{"p":80000}},"nonce":1793437815043,"expiresAfter":1793438084016}"#;
    const MARKET_NO_CLOID_BUILDER: &str = r#"{"signature":{"r":"0x4e28","s":"0x3d6c","v":27},"action":{"type":"order","orders":[{"a":159,"b":false,"p":"80.47","s":"0.12","r":false,"t":{"limit":{"tif":"FrontendMarket"}}}],"grouping":"na","builder":{"b":"0xe1f55f2f25884c2ddc86b6f7efa5f45b2ef04221","f":50}},"nonce":1793438081798}"#;
    const TRIGGER: &str = r#"{"signature":{"r":"0x340e","s":"0x2791","v":28},"action":{"type":"order","orders":[{"a":15,"b":true,"p":"0.046304","s":"13490","r":true,"t":{"trigger":{"isMarket":true,"triggerPx":"0.042095","tpsl":"sl"}}}],"grouping":"na","builder":{"b":"0x891dc6f05ad47a3c1a05da55e7a7517971faaf0d","f":40}},"nonce":1793438078536}"#;
    const CANCEL: &str = r#"{"signature":{"r":"0x33a1","s":"0x338e","v":27},"action":{"type":"cancel","cancels":[{"a":151,"o":591362660114}]},"nonce":1793438078289}"#;
    const CANCEL_F: &str = r#"{"signature":{"r":"0x34b5","s":"0x2fca","v":28},"vaultAddress":"0x80196a7475464e06de641df6c66e6e278cd364d5","action":{"type":"cancel","cancels":[{"a":159,"o":591362624677}],"f":true},"nonce":1793438078567}"#;
    const CANCEL_BY_CLOID: &str = r#"{"signature":{"r":"0x6087","s":"0x20ff","v":27},"vaultAddress":"0x7a9d45682babd8a2d79181623c75a7f615990b27","action":{"type":"cancelByCloid","cancels":[{"asset":26,"cloid":"0xe6d9f86348a64e6a84800e9d88ab3b0a"}],"f":true},"nonce":1793438078625,"expiresAfter":1793438083555}"#;
    const MODIFY_BY_CLOID: &str = r#"{"signature":{"r":"0x4c8f","s":"0x550a","v":28},"action":{"type":"modify","oid":"0xaaa4d758927e05848659714de4a1856b","order":{"a":24,"b":false,"p":"0.19326","s":"51226.3","r":false,"t":{"limit":{"tif":"Gtc"}},"c":"0xaaa40e16d2591d831748a08a9b082026"}},"nonce":1793351678006}"#;
    const MODIFY_TRIGGER_ALWAYS: &str = r#"{"signature":{"r":"0x41e0","s":"0x1d3d","v":28},"action":{"type":"modify","oid":591179694313,"order":{"a":200,"b":true,"p":"0.004512","s":"0","r":true,"t":{"trigger":{"isMarket":true,"triggerPx":"0.004102","tpsl":"sl"}}},"a":true},"isFrontend":true,"nonce":1793438080310,"expiresAfter":1793438093687}"#;
    const BATCH_MODIFY_ALWAYS: &str = r#"{"signature":{"r":"0xb519","s":"0x1538","v":28},"action":{"type":"batchModify","modifies":[{"oid":591362641192,"order":{"a":110060,"b":false,"p":"10.772","s":"745","r":false,"t":{"limit":{"tif":"Gtc"}},"c":"0x9468b433303148609f7cc8fab0fbce7f"}}],"a":true},"nonce":1793438079353}"#;
    const BATCH_MODIFY_BY_CLOID: &str = r#"{"signature":{"r":"0xadfe","s":"0x934f","v":28},"action":{"type":"batchModify","modifies":[{"oid":"0x343227a5584a018050484447efd7652f","order":{"a":159,"b":false,"p":"87.481","s":"4.96","r":true,"t":{"limit":{"tif":"Alo"}},"c":"0x343227a5584a018050484447efd7652f"}}]},"nonce":1793438078596}"#;
    const SCHEDULE_CANCEL: &str = r#"{"signature":{"r":"0x9066","s":"0x5242","v":28},"action":{"type":"scheduleCancel","time":1793438078174},"nonce":1793438078174}"#;
    const UPDATE_LEVERAGE: &str = r#"{"signature":{"r":"0xa39b","s":"0x13a3","v":28},"vaultAddress":"0x9e1733b7d499d6a2a6d3f5d74b942bbf5266a34d","action":{"type":"updateLeverage","asset":9,"isCross":true,"leverage":3},"nonce":1793438078450}"#;

    fn line(signed: &[&str]) -> String {
        format!(r#"["{SEEN}",{{"tx_hash":"{TX_HASH}","signed_actions":[{}]}}]"#, signed.join(","))
    }

    fn order(asset: u32, is_buy: bool, px: &str, sz: &str, reduce_only: bool, cloid: Option<&str>, tif: RequestedTif) -> RequestedOrder {
        RequestedOrder {
            asset,
            is_buy,
            limit_px: Px::parse_from_str(px).unwrap(),
            sz: Sz::parse_from_str(sz).unwrap(),
            reduce_only,
            cloid: cloid.map(str::to_string),
            tif,
        }
    }

    fn address(text: &str) -> Address {
        text.parse().unwrap()
    }

    #[test]
    fn a_line_reads_with_every_kind_in_its_order() {
        use RequestedTif::{Alo, FrontendMarket, Gtc, Ioc, Trigger};
        let tx = parse_line(&line(&[
            ALO_BATCH, IOC_PRIORITY, MARKET_NO_CLOID_BUILDER, TRIGGER, CANCEL, CANCEL_F, CANCEL_BY_CLOID, MODIFY_BY_CLOID,
            MODIFY_TRIGGER_ALWAYS, BATCH_MODIFY_ALWAYS, BATCH_MODIFY_BY_CLOID, SCHEDULE_CANCEL, UPDATE_LEVERAGE,
        ]))
        .unwrap();
        assert_eq!(tx.seen_ns, node_time_unix_nanos(SEEN).unwrap());
        assert_eq!(tx.seen_ns % 1_000_000_000, 693_300_829);
        assert_eq!(tx.tx_hash, TX_HASH.parse::<B256>().unwrap());
        let expected = vec![
            SignedAction {
                vault: Some(address("0xfcf104006bfff47695c1dc21dad3e9de1e72098e")),
                kind: ActionKind::Order(vec![
                    order(0, true, "77387", "0.135", false, Some("0x0000000000000000000001a18b7b1eef"), Alo),
                    order(0, true, "77386", "0.135", false, Some("0x0000000000000000000001a18b7b1ef0"), Alo),
                ]),
            },
            SignedAction {
                vault: None,
                kind: ActionKind::Order(vec![order(207, true, "0.00001", "1", false, Some("0x000000000000000018de99016980e8f8"), Ioc)]),
            },
            SignedAction { vault: None, kind: ActionKind::Order(vec![order(159, false, "80.47", "0.12", false, None, FrontendMarket)]) },
            SignedAction { vault: None, kind: ActionKind::Order(vec![order(15, true, "0.046304", "13490", true, None, Trigger)]) },
            SignedAction { vault: None, kind: ActionKind::Cancel { fast: false, cancels: vec![WireCancel { a: 151, o: 591_362_660_114 }] } },
            SignedAction {
                vault: Some(address("0x80196a7475464e06de641df6c66e6e278cd364d5")),
                kind: ActionKind::Cancel { fast: true, cancels: vec![WireCancel { a: 159, o: 591_362_624_677 }] },
            },
            SignedAction {
                vault: Some(address("0x7a9d45682babd8a2d79181623c75a7f615990b27")),
                kind: ActionKind::CancelByCloid {
                    fast: true,
                    cancels: vec![WireCancelByCloid { asset: 26, cloid: "0xe6d9f86348a64e6a84800e9d88ab3b0a".to_string() }],
                },
            },
            SignedAction {
                vault: None,
                kind: ActionKind::Modify {
                    always_place: false,
                    modifies: vec![Modify {
                        target: OrderRef::Cloid("0xaaa4d758927e05848659714de4a1856b".to_string()),
                        order: order(24, false, "0.19326", "51226.3", false, Some("0xaaa40e16d2591d831748a08a9b082026"), Gtc),
                    }],
                },
            },
            SignedAction {
                vault: None,
                kind: ActionKind::Modify {
                    always_place: true,
                    modifies: vec![Modify { target: OrderRef::Oid(591_179_694_313), order: order(200, true, "0.004512", "0", true, None, Trigger) }],
                },
            },
            SignedAction {
                vault: None,
                kind: ActionKind::Modify {
                    always_place: true,
                    modifies: vec![Modify {
                        target: OrderRef::Oid(591_362_641_192),
                        order: order(110_060, false, "10.772", "745", false, Some("0x9468b433303148609f7cc8fab0fbce7f"), Gtc),
                    }],
                },
            },
            SignedAction {
                vault: None,
                kind: ActionKind::Modify {
                    always_place: false,
                    modifies: vec![Modify {
                        target: OrderRef::Cloid("0x343227a5584a018050484447efd7652f".to_string()),
                        order: order(159, false, "87.481", "4.96", true, Some("0x343227a5584a018050484447efd7652f"), Alo),
                    }],
                },
            },
            SignedAction { vault: None, kind: ActionKind::Other },
            SignedAction { vault: Some(address("0x9e1733b7d499d6a2a6d3f5d74b942bbf5266a34d")), kind: ActionKind::Other },
        ];
        assert_eq!(tx.actions, expected);
    }

    /// Design 000604 section 2: an action that does not read loses its whole line, whatever kind it is.
    #[test]
    fn a_line_with_an_action_that_does_not_read_is_unreadable() {
        let unknown_tif = ALO_BATCH.replacen(r#""tif":"Alo""#, r#""tif":"Fok""#, 1);
        let bad_price = IOC_PRIORITY.replace(r#""p":"0.00001""#, r#""p":"-1""#);
        let neither = TRIGGER.replace(r#"{"trigger":{"isMarket":true,"triggerPx":"0.042095","tpsl":"sl"}}"#, "{}");
        let both = TRIGGER.replace(r#"{"trigger":"#, r#"{"limit":{"tif":"Gtc"},"trigger":"#);
        let cancel_without_asset = CANCEL_F.replace(r#""a":159,"#, "");
        let modify_without_order = MODIFY_BY_CLOID.replace(r#","order":{"a":24,"b":false,"p":"0.19326","s":"51226.3","r":false,"t":{"limit":{"tif":"Gtc"}},"c":"0xaaa40e16d2591d831748a08a9b082026"}"#, "");
        let always_place_text = BATCH_MODIFY_ALWAYS.replace(r#""a":true}"#, r#""a":"yes"}"#);
        let fast_text = CANCEL_BY_CLOID.replace(r#""f":true"#, r#""f":1"#);
        let fast_null = CANCEL_BY_CLOID.replace(r#""f":true"#, r#""f":null"#);
        let always_place_null = BATCH_MODIFY_ALWAYS.replace(r#""a":true}"#, r#""a":null}"#);
        for (case, signed) in [
            ("unknown tif", unknown_tif.as_str()),
            ("negative price", &bad_price),
            ("neither limit nor trigger", &neither),
            ("both limit and trigger", &both),
            ("cancel without its asset", &cancel_without_asset),
            ("modify without its order", &modify_without_order),
            ("always_place not a bool", &always_place_text),
            ("fast not a bool", &fast_text),
            ("fast null", &fast_null),
            ("always_place null", &always_place_null),
        ] {
            assert!(parse_line(&line(&[CANCEL_BY_CLOID, signed])).is_err(), "{case}");
        }
        let bad_time = line(&[CANCEL_F]).replace(SEEN, "2026-10-31 09:14:38");
        assert!(parse_line(&bad_time).is_err(), "local_time not a node time");
        assert!(parse_line(&format!(r#"["{SEEN}",{{"signed_actions":[]}}]"#)).is_err(), "no tx_hash");
        // review 000609 r1 P2-4: a transaction's identity is its 32-byte hash
        for (case, hash) in [("empty", ""), ("short", "0x49ed"), ("not hex", &TX_HASH.replace('4', "g"))] {
            assert!(parse_line(&line(&[CANCEL_F]).replace(TX_HASH, hash)).is_err(), "tx_hash {case}");
        }
        assert!(parse_line(&line(&[]).replacen('[', "[[", 1)).is_err(), "not the line's shape");
        assert_eq!(parse_line(&line(&[])).unwrap().actions, Vec::new(), "a transaction of no actions reads");
    }

    #[test]
    fn the_assets_named_are_one_per_order_cancel_or_modify() {
        let tx = parse_line(&line(&[ALO_BATCH, CANCEL_F, CANCEL_BY_CLOID, BATCH_MODIFY_ALWAYS, UPDATE_LEVERAGE])).unwrap();
        let assets: Vec<Vec<u32>> = tx.actions.iter().map(|action| action.kind.assets()).collect();
        assert_eq!(assets, vec![vec![0, 0], vec![159], vec![26], vec![110_060], vec![]]);
        let mut coins = ActionCoins { anchor_height: 1, coins: [(0, "BTC".to_string()), (26, "AVAX".to_string())].into_iter().collect() };
        assert_eq!(unnamed_assets(Some(&coins), &[0, 0, 159, 26]), 1);
        assert_eq!(unnamed_assets(None, &[0, 26]), 2, "no anchor names none");
        coins.coins.insert(159, "xyz:TSLA".to_string());
        assert_eq!(unnamed_assets(Some(&coins), &[0, 0, 159, 26]), 0);
    }

    /// Review 000609 r2 P2-1: a line that does not read is counted as it is parsed, before it waits for the queue; its
    /// own report, taken, clears it alone.
    #[test]
    fn a_line_that_does_not_read_is_counted_before_it_goes_out() {
        let losses = MempoolLosses::default();
        let mut ingest = MempoolIngest::new(watch::channel(None).1, losses.clone());
        let bad_hash = line(&[CANCEL_F]).replace(TX_HASH, "0x49ed");
        let unreadable = losses.event(&bad_hash);
        assert!(ingest.loss_unreported(), "known before the event is taken");
        losses.reader.found.fetch_add(1, Ordering::Release);
        losses.reader.flagged.fetch_add(1, Ordering::Release);
        ingest.on_event(unreadable);
        assert!(ingest.loss_unreported(), "the reader's loss stays unreported");
        ingest.on_event(losses.lost(WatchLoss::Discard));
        assert!(!ingest.loss_unreported());
        assert!(matches!(losses.event(&line(&[CANCEL_F])), MempoolEvent::Tx(_)), "a line that reads counts nothing");
        assert!(!ingest.loss_unreported());
    }

    /// Review 000609 r7: a discarded line found and flagged, then an overflow found while the line's `Lost` waits; that
    /// `Lost` reports the line alone, the overflow stays unreported until its own.
    #[test]
    fn a_report_covers_the_losses_flagged_before_it_alone() {
        let losses = MempoolLosses::default();
        let mut ingest = MempoolIngest::new(watch::channel(None).1, losses.clone());
        losses.reader.found.fetch_add(1, Ordering::Release);
        losses.reader.flagged.fetch_add(1, Ordering::Release);
        // the overflow comes while the watcher waits to send the line's report
        losses.reader.found.fetch_add(1, Ordering::Release);
        let line_lost = losses.lost(WatchLoss::Discard);
        assert!(matches!(line_lost, MempoolEvent::Lost { through: 1, .. }));
        ingest.on_event(line_lost);
        assert!(ingest.loss_unreported(), "the overflow, found before the line's report went out, is not in it");
        losses.reader.flagged.fetch_add(1, Ordering::Release);
        ingest.on_event(losses.lost(WatchLoss::Discard));
        assert!(!ingest.loss_unreported());
    }

    /// The watcher reads `[...]` lines from the end of the current hour file: the start's skip is a loss, a line
    /// that does not read comes in its place between the others, and a line not `[...]` is discarded. Its `Lost`
    /// comes after the lines read with it, but the loss is counted before any of them goes out (review 000609 r1
    /// P2-1): the pending thread takes the cancel after the discarded line knowing of it, and the unreadable line's
    /// report does not clear it.
    #[test]
    fn the_watcher_sends_the_lines_appended_in_order() {
        use std::{fs, io::Write, time::Duration};
        let dir = std::env::temp_dir().join(format!("pending_mempool_watch_test_{}", std::process::id()));
        fs::remove_dir_all(&dir).ok();
        let hour = dir.join("mempool_txs/hourly/20261031");
        fs::create_dir_all(&hour).unwrap();
        let path = hour.join("9");
        fs::write(&path, format!("{}\n", line(&[CANCEL_F]))).unwrap();
        let (MempoolFeed { mut events, losses }, _watcher) = watch_mempool(&dir);
        let mut ingest = MempoolIngest::new(watch::channel(None).1, losses);
        let append = |text: String| fs::OpenOptions::new().append(true).open(&path).unwrap().write_all(text.as_bytes()).unwrap();
        let mut next = || {
            let deadline = Instant::now() + Duration::from_secs(2);
            loop {
                match events.try_recv() {
                    Ok(event) => return event,
                    Err(mpsc::error::TryRecvError::Empty) if Instant::now() < deadline => thread::sleep(Duration::from_millis(5)),
                    Err(err) => panic!("no mempool event: {err}"),
                }
            }
        };
        // let the watcher register its inotify watch: it takes the file at its first modify event, skipping what was
        // written before
        thread::sleep(Duration::from_millis(200));
        append(format!("{}\n", line(&[CANCEL_BY_CLOID])));
        let positioning = next();
        assert!(matches!(positioning, MempoolEvent::Lost { loss: WatchLoss::Positioning, through: 0 }));
        ingest.on_event(positioning);
        assert!(!ingest.loss_unreported());
        let unreadable = line(&[ALO_BATCH.replacen(r#""tif":"Alo""#, r#""tif":"Fok""#, 1).as_str()]);
        append(format!("{}\n{unreadable}\n{{\"local_time\":\"{SEEN}\"}}\n{}\n", line(&[ALO_BATCH]), line(&[CANCEL_F])));
        let alo = next();
        assert!(matches!(&alo, MempoolEvent::Tx(tx) if matches!(tx.actions[0].kind, ActionKind::Order(_))));
        assert!(ingest.loss_unreported(), "counted before the read's lines go out");
        ingest.on_event(alo);
        let unreadable = next();
        assert!(matches!(unreadable, MempoolEvent::Unreadable { through: 1, .. }));
        ingest.on_event(unreadable);
        assert!(ingest.loss_unreported(), "the unreadable line reported, not the discarded one found before it");
        let cancel = next();
        assert!(matches!(&cancel, MempoolEvent::Tx(tx) if matches!(tx.actions[0].kind, ActionKind::Cancel { .. })));
        assert!(ingest.loss_unreported(), "the cancel after the discarded line, taken knowing of the loss");
        ingest.on_event(cancel);
        let lost = next();
        assert!(matches!(lost, MempoolEvent::Lost { loss: WatchLoss::Discard, through: 1 }), "the object line");
        ingest.on_event(lost);
        assert!(!ingest.loss_unreported());
        fs::remove_dir_all(&dir).ok();
    }
}
