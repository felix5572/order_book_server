//! The node's mempool (hyperliquid-trade design 000604 section 2): `<data-dir>/mempool_txs/hourly/<YYYYMMDD>/<H>`, a
//! line per transaction the node saw before a block took it, `[local_time, {"tx_hash", "signed_actions": [{"action",
//! "nonce", "vaultAddress"?, "expiresAfter"?, "isFrontend"?, "signature"}]}]` (bm 10-31 09:10-09:11: 2,011 lines, ~140
//! signed actions a line, the longest 1.6 MB). The watcher thread reads the lines from the end of the current file and
//! parses each whole (signatures and unread fields skipped), in the file's order; the pending thread names each asset
//! id by its coin (`MempoolIngest`), whose pending set takes each transaction (batch 2b, `shadow`).

use std::{path::Path, sync::Arc, thread};

use alloy::primitives::{Address, B256};
use log::error;
use serde::Deserialize;
use sonic_rs::LazyValue;
use tokio::{sync::watch, time::Instant};

use super::{
    actions::Tif,
    feed::{self, Feed, FeedEvent, FeedLosses, FeedReports},
    signer,
};
use crate::{
    accounts::{
        book::{ActionCoins, PendingAnchor},
        marks::actions::{SubmittedPrices, set_global_prices},
        replica::{
            Action, OrderRef, SignedAction as WireSignedAction, TpslGrouping, WireCancel, WireCancelByCloid, WireModify, WireOrder, WireTrigger,
            alo_priority,
        },
    },
    listeners::order_book::{
        parallel::{FileLayout, LineShape, LineStream, seconds_since_node_write},
        throttled_log_due,
    },
    metrics::{
        NODE_LINE_LAG, PENDING_BOOK_MEMPOOL_ACTIONS_TOTAL, PENDING_BOOK_MEMPOOL_LINES_TOTAL,
        PENDING_BOOK_MEMPOOL_LOSSES_TOTAL, PENDING_BOOK_MEMPOOL_ORDERS_TOTAL, PENDING_BOOK_MEMPOOL_UNNAMED_ASSETS_TOTAL,
        PENDING_BOOK_SIGNERS_TOTAL,
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
    /// The vault or subaccount it acts for; otherwise the signer acts.
    pub(crate) vault: Option<Address>,
    /// Its signer, recovered for an action without a vault that may take (batch 2g, `signer`) or places a trigger order
    /// (batch 2m-2); `None` for any other, or when it cannot be told.
    pub(crate) signer: Option<Address>,
    /// Its `expiresAfter`, unix ms: HL refuses it past then.
    pub(crate) expires_after_ms: Option<u64>,
    /// An `order` action's ALO priority (`Action::alo_priority`, batch 2f); 0 for any other.
    pub(crate) priority: u64,
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
    /// A validator's `SetGlobalAction`: per main-dex asset, the prices submitted (batch 2l, `mark_forecast`).
    SetGlobal(Vec<SubmittedPrices>),
    /// A `trailingStop` placement: counted only (batch 2n-2, design 000659 r2 s6: not modelled before its block; then
    /// a trailing stop of the replica's table).
    TrailingStop,
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
    /// The price and the size as written, for the coin's grid; `None` when not a plain decimal.
    pub(crate) px_shape: Option<DecimalShape>,
    pub(crate) sz_shape: Option<DecimalShape>,
    pub(crate) reduce_only: bool,
    pub(crate) cloid: Option<String>,
    pub(crate) tif: RequestedTif,
    /// A trigger order's (batch 2m): `Some` exactly when `tif` is `Trigger`.
    pub(crate) trigger: Option<RequestedTrigger>,
}

/// A trigger order as requested (batch 2m): its trigger price, whether it runs as a market order, take profit or
/// stop loss, and its place in its action's TP/SL grouping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RequestedTrigger {
    pub(crate) trigger_px: Px,
    pub(crate) is_market: bool,
    pub(crate) take_profit: bool,
    pub(crate) grouping: TriggerGrouping,
}

/// A trigger order's place in its action (HL docs "Exchange endpoint", "Take profit and stop loss orders").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TriggerGrouping {
    /// Its own.
    Alone,
    /// Of the account's whole position (`positionTpsl`; a modify's of size 0).
    Position,
    /// A child of its action's first order (`normalTpsl`): it waits for that order to fill.
    Child,
}

/// A plain decimal as written, trailing zeros of its fraction dropped: its decimals and its significant digits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DecimalShape {
    pub(crate) decimals: u32,
    pub(crate) significant: u32,
}

impl DecimalShape {
    /// `None` unless digits, optionally a point and more digits.
    pub(crate) fn of(text: &str) -> Option<Self> {
        let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
        let digits = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
        if whole.is_empty() || !digits(whole) || !digits(fraction) {
            return None;
        }
        let fraction = fraction.trim_end_matches('0');
        let leading = whole.trim_start_matches('0');
        let significant = if leading.is_empty() { fraction.trim_start_matches('0').len() } else { leading.len() + fraction.len() };
        Some(Self { decimals: u32::try_from(fraction.len()).ok()?, significant: u32::try_from(significant).ok()? })
    }
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

#[derive(Deserialize)]
struct WireTx<'a> {
    tx_hash: B256,
    #[serde(borrow)]
    signed_actions: Vec<WireSignedAction<'a>>,
}

/// Starts the watcher of `<data_dir>/mempool_txs`; its events come in order on the feed, which closes when the
/// watcher ends.
pub(crate) fn watch_mempool(data_dir: &Path) -> (Feed<MempoolTx>, thread::JoinHandle<()>) {
    let stream = LineStream {
        name: "Mempool",
        lag_label: "mempool",
        dir: data_dir.join("mempool_txs"),
        max_line_bytes: MAX_LINE_BYTES,
        start_skip_is_loss: true,
        layout: FileLayout::Hourly,
        shape: LineShape::Array,
        loss_counts: None,
    };
    feed::watch(stream, QUEUE_LINES, parse_line)
}

/// One mempool line; an `Err` says what does not read.
pub(crate) fn parse_line(line: &str) -> Result<MempoolTx, String> {
    let (local_time, tx): (&str, WireTx<'_>) = sonic_rs::from_str(line).map_err(|err| format!("mempool line: {err}"))?;
    let seen_ns = node_time_unix_nanos(local_time).ok_or_else(|| format!("mempool line local_time {local_time:?}"))?;
    let in_tx = |err: String| format!("tx {} {err}", tx.tx_hash);
    let mut actions = tx
        .signed_actions
        .iter()
        .enumerate()
        .map(|(index, signed)| signed_action(signed).map_err(|err| format!("action {index}: {err}")))
        .collect::<Result<Vec<_>, _>>()
        .map_err(in_tx)?;
    recover_signers(line, &tx.signed_actions, &mut actions).map_err(in_tx)?;
    Ok(MempoolTx { seen_ns, tx_hash: tx.tx_hash, actions })
}

/// The signer of each action without a vault that may take (batch 2g: what its taker's margin refusals go by) or places
/// a trigger order (batch 2m-2: its account, as the block log taught it), the actions as written read from `line` in one
/// pass.
fn recover_signers(line: &str, wire_actions: &[WireSignedAction<'_>], actions: &mut [SignedAction]) -> Result<(), String> {
    let wanted: Vec<usize> = actions
        .iter()
        .enumerate()
        .filter(|(_, action)| action.vault.is_none() && (action.kind.may_take() || action.kind.places_trigger()))
        .map(|(index, _)| index)
        .collect();
    if wanted.is_empty() {
        return Ok(());
    }
    let written = signer::actions_written(line, wanted.iter().map(|index| sonic_rs::pointer![1, "signed_actions", *index, "action"]))?;
    for (index, action) in wanted.into_iter().zip(written) {
        let signer = signer::signer(&wire_actions[index], action.as_raw_str()).map_err(|err| format!("action {index}: {err}"))?;
        PENDING_BOOK_SIGNERS_TOTAL.with_label_values(&["mempool", if signer.is_some() { "recovered" } else { "unknown" }]).inc();
        actions[index].signer = signer;
    }
    Ok(())
}

fn signed_action(signed: &WireSignedAction<'_>) -> Result<SignedAction, String> {
    let action = &signed.action;
    let mut priority = 0;
    let kind = match action.action_type {
        "order" => {
            let orders: Vec<WireOrder> = action.field(action.orders.as_ref(), "orders")?;
            priority = alo_priority(action.requested_priority()?, &orders);
            ActionKind::Order(grouped(orders.iter().map(requested).collect::<Result<_, _>>()?, action.tpsl_grouping()?))
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
            let order = modified(requested(&action.field(action.order.as_ref(), "order")?)?);
            let always_place = flag(action, action.always_place.as_ref(), "a")?;
            ActionKind::Modify { always_place, modifies: vec![Modify { target, order }] }
        }
        "batchModify" => {
            let modifies: Vec<WireModify> = action.field(action.modifies.as_ref(), "modifies")?;
            let modifies = modifies
                .into_iter()
                .map(|modify| Ok(Modify { order: modified(requested(&modify.order)?), target: modify.oid }))
                .collect::<Result<_, String>>()?;
            ActionKind::Modify { always_place: flag(action, action.always_place.as_ref(), "a")?, modifies }
        }
        "SetGlobalAction" => ActionKind::SetGlobal(set_global_prices(action)?),
        "trailingStop" => ActionKind::TrailingStop,
        _ => ActionKind::Other,
    };
    Ok(SignedAction { vault: signed.vault()?, signer: None, expires_after_ms: signed.expires_after_ms()?, priority, kind })
}

/// An action's flag that is omitted when false (a modify's `a`, a cancel's `f`).
fn flag(action: &Action<'_>, raw: Option<&LazyValue<'_>>, name: &str) -> Result<bool, String> {
    raw.map_or(Ok(false), |raw| action.field(Some(raw), name))
}

fn requested(order: &WireOrder) -> Result<RequestedOrder, String> {
    let tif = order_tif(order)?;
    let trigger = order.t.trigger.as_ref().map(requested_trigger).transpose()?;
    Ok(RequestedOrder {
        asset: order.a,
        is_buy: order.b,
        limit_px: Px::parse_from_str(&order.p).map_err(|err| format!("order price {:?}: {err}", order.p))?,
        sz: Sz::parse_from_str(&order.s).map_err(|err| format!("order size {:?}: {err}", order.s))?,
        px_shape: DecimalShape::of(&order.p),
        sz_shape: DecimalShape::of(&order.s),
        reduce_only: order.r,
        cloid: order.c.clone(),
        tif,
        trigger,
    })
}

/// A trigger order's fields, each required (batch 2m); its grouping its own until its action says otherwise.
fn requested_trigger(trigger: &WireTrigger) -> Result<RequestedTrigger, String> {
    let (Some(is_market), Some(trigger_px), Some(tpsl)) = (trigger.is_market, &trigger.trigger_px, &trigger.tpsl) else {
        return Err(format!("trigger order without isMarket, triggerPx or tpsl: {trigger:?}"));
    };
    let take_profit = match tpsl.as_str() {
        "tp" => true,
        "sl" => false,
        other => return Err(format!("trigger order tpsl {other:?}")),
    };
    let trigger_px = Px::parse_from_str(trigger_px).map_err(|err| format!("trigger price {trigger_px:?}: {err}"))?;
    Ok(RequestedTrigger { trigger_px, is_market, take_profit, grouping: TriggerGrouping::Alone })
}

/// An `order` action's trigger orders placed in its grouping: a `positionTpsl`'s of the whole position, a
/// `normalTpsl`'s after its first order that order's children.
fn grouped(mut orders: Vec<RequestedOrder>, grouping: TpslGrouping) -> Vec<RequestedOrder> {
    for (index, order) in orders.iter_mut().enumerate() {
        let Some(trigger) = &mut order.trigger else { continue };
        trigger.grouping = match grouping {
            TpslGrouping::None => TriggerGrouping::Alone,
            TpslGrouping::Position => TriggerGrouping::Position,
            TpslGrouping::Normal if index == 0 => TriggerGrouping::Alone,
            TpslGrouping::Normal => TriggerGrouping::Child,
        };
    }
    orders
}

/// A modify's new trigger order of size 0 is a position TP/SL's (a modify carries no grouping).
const fn modified(mut order: RequestedOrder) -> RequestedOrder {
    if let Some(trigger) = &mut order.trigger
        && order.sz.is_zero()
    {
        trigger.grouping = TriggerGrouping::Position;
    }
    order
}

/// An order's time in force, or its being a trigger order.
pub(crate) fn order_tif(order: &WireOrder) -> Result<RequestedTif, String> {
    match (&order.t.limit, &order.t.trigger) {
        (Some(limit), None) => limit_tif(&limit.tif),
        (None, Some(_)) => Ok(RequestedTif::Trigger),
        _ => Err("order type neither limit nor trigger".to_string()),
    }
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
    /// Whether it may take on placing (`Tif::may_take`); a trigger order does not then.
    pub(crate) fn may_take(self) -> bool {
        Tif::of_requested(self).is_some_and(Tif::may_take)
    }

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
            Self::SetGlobal(_) => "set_global",
            Self::TrailingStop => "trailing_stop",
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
            Self::SetGlobal(_) | Self::TrailingStop | Self::Other => Vec::new(),
        }
    }

    /// Whether it is an `order` action with an order that may take (`RequestedTif::may_take`): its signer is what
    /// tells its taker's recent margin refusals (batch 2g).
    fn may_take(&self) -> bool {
        matches!(self, Self::Order(orders) if orders.iter().any(|order| order.tif.may_take()))
    }

    /// Whether it places a trigger order: an `order`, or a modify's new order (batch 2m-2: its signer read).
    fn places_trigger(&self) -> bool {
        self.orders().any(|order| order.trigger.is_some())
    }

    /// The orders it places: an `order`'s, a modify's new ones; trigger orders among them.
    pub(crate) fn orders(&self) -> impl Iterator<Item = &RequestedOrder> {
        let (orders, modifies) = match self {
            Self::Order(orders) => (orders.as_slice(), [].as_slice()),
            Self::Modify { modifies, .. } => ([].as_slice(), modifies.as_slice()),
            _ => ([].as_slice(), [].as_slice()),
        };
        orders.iter().chain(modifies.iter().map(|modify| &modify.order))
    }
}

/// What a mempool event brings the pending set.
#[derive(Debug)]
pub(crate) enum MempoolInput {
    /// A transaction; `after_loss` when taken while the watcher had found a loss not yet reported (it may follow it).
    Tx { tx: MempoolTx, after_loss: bool },
    /// Lines lost.
    Loss,
}

/// The pending thread's reading of the mempool: the anchor's asset table, the losses reported so far, and the events
/// counted (batch 2a).
pub(crate) struct MempoolIngest {
    anchor: watch::Receiver<Option<Arc<PendingAnchor>>>,
    reports: FeedReports,
    last_unreadable_log: Option<Instant>,
}

impl MempoolIngest {
    pub(crate) const fn new(anchor: watch::Receiver<Option<Arc<PendingAnchor>>>, losses: FeedLosses) -> Self {
        Self { anchor, reports: FeedReports::new(losses), last_unreadable_log: None }
    }

    /// Whether the watcher found a loss no event taken yet reports: what was taken since may follow it in the file.
    pub(crate) fn loss_unreported(&self) -> bool {
        self.reports.loss_unreported()
    }

    /// The anchor - its asset table -; `None` before the first anchor.
    pub(crate) fn anchor(&self) -> Option<Arc<PendingAnchor>> {
        self.anchor.borrow().clone()
    }

    pub(crate) fn on_event(&mut self, event: FeedEvent<MempoolTx>) -> MempoolInput {
        self.reports.take(&event);
        match event {
            FeedEvent::Line(tx) => {
                let after_loss = self.on_tx(&tx);
                MempoolInput::Tx { tx, after_loss }
            }
            FeedEvent::Unreadable { error, .. } => {
                PENDING_BOOK_MEMPOOL_LINES_TOTAL.with_label_values(&["unreadable"]).inc();
                if throttled_log_due(&mut self.last_unreadable_log, Instant::now()) {
                    error!("pending book: a mempool line does not read, its actions lost: {error}");
                }
                MempoolInput::Loss
            }
            FeedEvent::Lost { loss, .. } => {
                PENDING_BOOK_MEMPOOL_LOSSES_TOTAL.with_label_values(&[feed::loss_label(loss)]).inc();
                MempoolInput::Loss
            }
        }
    }

    /// Counts a transaction; whether it was taken after a loss not yet reported.
    fn on_tx(&self, tx: &MempoolTx) -> bool {
        NODE_LINE_LAG.with_label_values(&["apply", "mempool"]).observe(seconds_since_node_write(tx.seen_ns));
        let after_loss = self.loss_unreported();
        let outcome = if after_loss { "read_after_unreported_loss" } else { "read" };
        PENDING_BOOK_MEMPOOL_LINES_TOTAL.with_label_values(&[outcome]).inc();
        let anchor = self.anchor();
        let coins = anchor.as_deref().map(|anchor| &anchor.coins);
        for action in &tx.actions {
            PENDING_BOOK_MEMPOOL_ACTIONS_TOTAL.with_label_values(&[action.kind.label()]).inc();
            for order in action.kind.orders() {
                PENDING_BOOK_MEMPOOL_ORDERS_TOTAL.with_label_values(&[order.tif.label()]).inc();
            }
            let unnamed = unnamed_assets(coins, &action.kind.assets());
            if unnamed > 0 {
                let reason = if coins.is_some() { "not_in_anchor" } else { "no_anchor" };
                PENDING_BOOK_MEMPOOL_UNNAMED_ASSETS_TOTAL.with_label_values(&[reason]).inc_by(unnamed);
            }
        }
        after_loss
    }
}

/// The assets the anchor's table does not name (all of them before the first anchor).
fn unnamed_assets(coins: Option<&ActionCoins>, assets: &[u32]) -> u64 {
    let unnamed = assets.iter().filter(|asset| coins.is_none_or(|coins| !coins.coins.contains_key(asset))).count();
    u64::try_from(unnamed).unwrap_or(u64::MAX)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::accounts::book::ActionAsset;
    use crate::listeners::order_book::parallel::WatchLoss;

    const SEEN: &str = "2026-10-31T09:14:38.693300829";
    const TX_HASH: &str = "0x49ed0336871d4fc91663a015ae3cac1388cf48566a4b25b69f240a89c0ee6666";

    // Signed actions cut from bm's mempool (2026-10-31 09:14:38-09:14:42), whole but for signatures shortened.
    const ALO_BATCH: &str = r#"{"signature":{"r":"0x79e6","s":"0x297b","v":28},"vaultAddress":"0xfcf104006bfff47695c1dc21dad3e9de1e72098e","action":{"type":"order","orders":[{"a":0,"b":true,"p":"77387","s":"0.135","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x0000000000000000000001a18b7b1eef"},{"a":0,"b":true,"p":"77386","s":"0.135","r":false,"t":{"limit":{"tif":"Alo"}},"c":"0x0000000000000000000001a18b7b1ef0"}],"grouping":"na"},"nonce":1793438078596}"#;
    const IOC_PRIORITY: &str = r#"{"signature":{"r":"0xe91a","s":"0x4ccb","v":27},"action":{"type":"order","orders":[{"a":207,"b":true,"p":"0.00001","s":"1","r":false,"t":{"limit":{"tif":"Ioc"}},"c":"0x000000000000000018de99016980e8f8"}],"grouping":{"p":80000}},"nonce":1793437815043,"expiresAfter":1793438084016}"#;
    const MARKET_NO_CLOID_BUILDER: &str = r#"{"signature":{"r":"0x4e28","s":"0x3d6c","v":27},"action":{"type":"order","orders":[{"a":159,"b":false,"p":"80.47","s":"0.12","r":false,"t":{"limit":{"tif":"FrontendMarket"}}}],"grouping":"na","builder":{"b":"0xe1f55f2f25884c2ddc86b6f7efa5f45b2ef04221","f":50}},"nonce":1793438081798}"#;
    /// A trigger order action as bm read it, its signature shortened (some address recovers it).
    pub(crate) const TRIGGER: &str = r#"{"signature":{"r":"0x340e","s":"0x2791","v":28},"action":{"type":"order","orders":[{"a":15,"b":true,"p":"0.046304","s":"13490","r":true,"t":{"trigger":{"isMarket":true,"triggerPx":"0.042095","tpsl":"sl"}}}],"grouping":"na","builder":{"b":"0x891dc6f05ad47a3c1a05da55e7a7517971faaf0d","f":40}},"nonce":1793438078536}"#;
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
            px_shape: DecimalShape::of(px),
            sz_shape: DecimalShape::of(sz),
            reduce_only,
            cloid: cloid.map(str::to_string),
            tif,
            trigger: None,
        }
    }

    /// `order` a market stop loss at `trigger_px` (the samples' kind), of `grouping`.
    fn triggered(order: RequestedOrder, trigger_px: &str, grouping: TriggerGrouping) -> RequestedOrder {
        let trigger = RequestedTrigger { trigger_px: Px::parse_from_str(trigger_px).unwrap(), is_market: true, take_profit: false, grouping };
        RequestedOrder { trigger: Some(trigger), ..order }
    }

    fn address(text: &str) -> Address {
        text.parse().unwrap()
    }

    /// Batch 2f: an order action's ALO priority, when every order is a non-reduce-only ALO (bm 10-31 block
    /// 1204388049's two ALOs, `{"p": 3}`); an IOC action's `{"p": 80000}` gives none, nor any other action.
    #[test]
    fn an_order_action_reads_its_alo_priority() {
        let tx = parse_line(&line(&[crate::pending::blocks::tests::PRIORITY_ALOS, IOC_PRIORITY, ALO_BATCH, CANCEL])).unwrap();
        assert_eq!(tx.actions.iter().map(|action| action.priority).collect::<Vec<_>>(), [3, 0, 0, 0]);
        assert!(parse_line(&line(&[&ALO_BATCH.replace(r#""grouping":"na""#, r#""grouping":{"q":1}"#)])).is_err());
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
                signer: None,
                expires_after_ms: None,
                priority: 0,
                kind: ActionKind::Order(vec![
                    order(0, true, "77387", "0.135", false, Some("0x0000000000000000000001a18b7b1eef"), Alo),
                    order(0, true, "77386", "0.135", false, Some("0x0000000000000000000001a18b7b1ef0"), Alo),
                ]),
            },
            SignedAction {
                vault: None,
                signer: None,
                expires_after_ms: Some(1_793_438_084_016),
                priority: 0,
                kind: ActionKind::Order(vec![order(207, true, "0.00001", "1", false, Some("0x000000000000000018de99016980e8f8"), Ioc)]),
            },
            SignedAction { vault: None, signer: None, expires_after_ms: None, priority: 0, kind: ActionKind::Order(vec![order(159, false, "80.47", "0.12", false, None, FrontendMarket)]) },
            // a trigger order's signer recovered (batch 2m-2), the sample's shortened signature giving some address
            SignedAction {
                vault: None,
                signer: Some(address("0x12cb6a66fe95a62d901eee587499670e89b3a926")),
                expires_after_ms: None,
                priority: 0,
                kind: ActionKind::Order(vec![triggered(order(15, true, "0.046304", "13490", true, None, Trigger), "0.042095", TriggerGrouping::Alone)]),
            },
            SignedAction { vault: None, signer: None, expires_after_ms: None, priority: 0, kind: ActionKind::Cancel { fast: false, cancels: vec![WireCancel { a: 151, o: 591_362_660_114 }] } },
            SignedAction {
                vault: Some(address("0x80196a7475464e06de641df6c66e6e278cd364d5")),
                signer: None,
                expires_after_ms: None,
                priority: 0,
                kind: ActionKind::Cancel { fast: true, cancels: vec![WireCancel { a: 159, o: 591_362_624_677 }] },
            },
            SignedAction {
                vault: Some(address("0x7a9d45682babd8a2d79181623c75a7f615990b27")),
                signer: None,
                expires_after_ms: Some(1_793_438_083_555),
                priority: 0,
                kind: ActionKind::CancelByCloid {
                    fast: true,
                    cancels: vec![WireCancelByCloid { asset: 26, cloid: "0xe6d9f86348a64e6a84800e9d88ab3b0a".to_string() }],
                },
            },
            SignedAction {
                vault: None,
                signer: None,
                expires_after_ms: None,
                priority: 0,
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
                signer: None,
                expires_after_ms: Some(1_793_438_093_687),
                priority: 0,
                kind: ActionKind::Modify {
                    always_place: true,
                    modifies: vec![Modify { target: OrderRef::Oid(591_179_694_313), order: triggered(order(200, true, "0.004512", "0", true, None, Trigger), "0.004102", TriggerGrouping::Position) }],
                },
            },
            SignedAction {
                vault: None,
                signer: None,
                expires_after_ms: None,
                priority: 0,
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
                signer: None,
                expires_after_ms: None,
                priority: 0,
                kind: ActionKind::Modify {
                    always_place: false,
                    modifies: vec![Modify {
                        target: OrderRef::Cloid("0x343227a5584a018050484447efd7652f".to_string()),
                        order: order(159, false, "87.481", "4.96", true, Some("0x343227a5584a018050484447efd7652f"), Alo),
                    }],
                },
            },
            SignedAction { vault: None, signer: None, expires_after_ms: None, priority: 0, kind: ActionKind::Other },
            SignedAction { vault: Some(address("0x9e1733b7d499d6a2a6d3f5d74b942bbf5266a34d")), signer: None, expires_after_ms: None, priority: 0, kind: ActionKind::Other },
        ];
        assert_eq!(tx.actions, expected);
    }

    /// bm 11-01 15:37-15:38, a validator's `SetGlobalAction` alone on its line (47 of 47 such lines), its `pxs` cut to
    /// three assets and a fourth made partial.
    const SET_GLOBAL: &str = r#"{"signature":{"r":"0x6d36759485b99e20a2c806929afb800dd76235997ed17ab7a78eedda1063f65e","s":"0x75cffeb5d78cd3b14d5de14b4acbbf4e13e28f864d94bc4cd65157e8023ca00e","v":28},"action":{"type":"SetGlobalAction","pxs":[["78129","78115"],["2270.9","2270"],[null,null],["1.473",null]],"externalPerpPxs":[["0G","0.1676"],["2Z","0.02535"]],"usdtUsdcPx":"0.9990109791306606","nativePx":"87.84490829982927"},"nonce":1793547443362,"expiresAfter":1793547518788}"#;

    /// Batch 2l: a `SetGlobalAction` reads as the marks read it (`set_global_prices`), each asset's pair priced, unpriced
    /// or partial.
    #[test]
    fn a_set_global_action_reads_its_submitted_prices() {
        let tx = parse_line(&line(&[SET_GLOBAL])).unwrap();
        let priced = |oracle: &str, mark: &str| SubmittedPrices::Priced { oracle: oracle.to_string(), mark: mark.to_string() };
        let submitted = vec![priced("78129", "78115"), priced("2270.9", "2270"), SubmittedPrices::Unpriced, SubmittedPrices::Partial];
        let expected = SignedAction { vault: None, signer: None, expires_after_ms: Some(1_793_547_518_788), priority: 0, kind: ActionKind::SetGlobal(submitted) };
        assert_eq!(tx.actions, [expected]);
        assert!(parse_line(&line(&[&SET_GLOBAL.replace(r#""pxs":"#, r#""prices":"#)])).is_err(), "no pxs");
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

    /// Batch 2d: a price or a size as written, for its coin's grid; only plain decimals are shaped.
    #[test]
    fn a_decimal_reads_its_decimals_and_significant_digits() {
        let shape = |text: &str| DecimalShape::of(text).map(|shape| (shape.decimals, shape.significant));
        assert_eq!(shape("77655"), Some((0, 5)));
        assert_eq!(shape("0.004246"), Some((6, 4)));
        assert_eq!(shape("1043.630"), Some((2, 6)));
        assert_eq!(shape("100000"), Some((0, 6)));
        assert_eq!(shape("0.0"), Some((0, 0)));
        for text in ["1e-5", ".5", "-1", "1.2.3", ""] {
            assert_eq!(shape(text), None, "{text:?}");
        }
    }

    #[test]
    fn the_assets_named_are_one_per_order_cancel_or_modify() {
        let tx = parse_line(&line(&[ALO_BATCH, CANCEL_F, CANCEL_BY_CLOID, BATCH_MODIFY_ALWAYS, UPDATE_LEVERAGE])).unwrap();
        let assets: Vec<Vec<u32>> = tx.actions.iter().map(|action| action.kind.assets()).collect();
        assert_eq!(assets, vec![vec![0, 0], vec![159], vec![26], vec![110_060], vec![]]);
        let asset = |coin: &str| ActionAsset { coin: coin.to_string(), size_decimals: 2, max_decimals: 6, max_market_value: None };
        let mut coins = ActionCoins { coins: [(0, asset("BTC")), (26, asset("AVAX"))].into_iter().collect() };
        assert_eq!(unnamed_assets(Some(&coins), &[0, 0, 159, 26]), 1);
        assert_eq!(unnamed_assets(None, &[0, 26]), 2, "no anchor names none");
        coins.coins.insert(159, asset("xyz:TSLA"));
        assert_eq!(unnamed_assets(Some(&coins), &[0, 0, 159, 26]), 0);
    }

    /// Review 000609 r2 P2-1: a line that does not read is counted as it is parsed, before it waits for the queue; its
    /// own report, taken, clears it alone.
    #[test]
    fn a_line_that_does_not_read_is_counted_before_it_goes_out() {
        let losses = FeedLosses::default();
        let mut ingest = MempoolIngest::new(watch::channel(None).1, losses.clone());
        let bad_hash = line(&[CANCEL_F]).replace(TX_HASH, "0x49ed");
        let unreadable = losses.event(parse_line(&bad_hash));
        assert!(ingest.loss_unreported(), "known before the event is taken");
        losses.reader.found.fetch_add(1, Ordering::Release);
        losses.reader.flagged.fetch_add(1, Ordering::Release);
        ingest.on_event(unreadable);
        assert!(ingest.loss_unreported(), "the reader's loss stays unreported");
        ingest.on_event(losses.lost::<MempoolTx>(WatchLoss::Discard));
        assert!(!ingest.loss_unreported());
        assert!(matches!(losses.event(parse_line(&line(&[CANCEL_F]))), FeedEvent::Line(_)), "a line that reads counts nothing");
        assert!(!ingest.loss_unreported());
    }

    /// Review 000609 r7: a discarded line found and flagged, then an overflow found while the line's `Lost` waits; that
    /// `Lost` reports the line alone, the overflow stays unreported until its own.
    #[test]
    fn a_report_covers_the_losses_flagged_before_it_alone() {
        let losses = FeedLosses::default();
        let mut ingest = MempoolIngest::new(watch::channel(None).1, losses.clone());
        losses.reader.found.fetch_add(1, Ordering::Release);
        losses.reader.flagged.fetch_add(1, Ordering::Release);
        // the overflow comes while the watcher waits to send the line's report
        losses.reader.found.fetch_add(1, Ordering::Release);
        let line_lost = losses.lost::<MempoolTx>(WatchLoss::Discard);
        assert!(matches!(line_lost, FeedEvent::Lost { through: 1, .. }));
        ingest.on_event(line_lost);
        assert!(ingest.loss_unreported(), "the overflow, found before the line's report went out, is not in it");
        losses.reader.flagged.fetch_add(1, Ordering::Release);
        ingest.on_event(losses.lost::<MempoolTx>(WatchLoss::Discard));
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
        let (Feed { mut events, losses }, _watcher) = watch_mempool(&dir);
        let mut ingest = MempoolIngest::new(watch::channel(None).1, losses);
        let append = |text: String| fs::OpenOptions::new().append(true).open(&path).unwrap().write_all(text.as_bytes()).unwrap();
        let mut next = || {
            let deadline = Instant::now() + Duration::from_secs(2);
            loop {
                match events.try_recv() {
                    Ok(event) => return event,
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) if Instant::now() < deadline => thread::sleep(Duration::from_millis(5)),
                    Err(err) => panic!("no mempool event: {err}"),
                }
            }
        };
        // let the watcher take its first file check: it follows the file from its end, skipping what was written before
        thread::sleep(Duration::from_millis(200));
        let positioning = next();
        assert!(matches!(positioning, FeedEvent::Lost { loss: WatchLoss::Positioning, through: 0 }));
        ingest.on_event(positioning);
        assert!(!ingest.loss_unreported());
        let unreadable = line(&[ALO_BATCH.replacen(r#""tif":"Alo""#, r#""tif":"Fok""#, 1).as_str()]);
        append(format!("{}\n{unreadable}\n{{\"local_time\":\"{SEEN}\"}}\n{}\n", line(&[ALO_BATCH]), line(&[CANCEL_F])));
        let alo = next();
        assert!(matches!(&alo, FeedEvent::Line(tx) if matches!(tx.actions[0].kind, ActionKind::Order(_))));
        assert!(ingest.loss_unreported(), "counted before the read's lines go out");
        ingest.on_event(alo);
        let unreadable = next();
        assert!(matches!(unreadable, FeedEvent::Unreadable { through: 1, .. }));
        ingest.on_event(unreadable);
        assert!(ingest.loss_unreported(), "the unreadable line reported, not the discarded one found before it");
        let cancel = next();
        assert!(matches!(&cancel, FeedEvent::Line(tx) if matches!(tx.actions[0].kind, ActionKind::Cancel { .. })));
        assert!(ingest.loss_unreported(), "the cancel after the discarded line, taken knowing of the loss");
        assert!(matches!(ingest.on_event(cancel), MempoolInput::Tx { after_loss: true, .. }));
        let lost = next();
        assert!(matches!(lost, FeedEvent::Lost { loss: WatchLoss::Discard, through: 1 }), "the object line");
        ingest.on_event(lost);
        assert!(!ingest.loss_unreported());
        fs::remove_dir_all(&dir).ok();
    }
}
