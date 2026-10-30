//! The node marks' view of a `replica_cmds` line (design 000439 r1 section 3): every successful action that anchors the
//! block's book diffs or prices the main dex, with its execution rank.
//!
//! Execution order (Codex, binary 6911b792 0x231ae60, `block_execution_map_r2_20261019/REPORT.txt`; research
//! `execution_point_replay.py` `execution_category`): bundle by bundle; inside a bundle, 0. `noop`, 1. order / modify /
//! batchModify actions whose new orders are all ALO, 2. `cancel` / `cancelByCloid`, 3. everything else (the
//! `SetGlobalAction` and the validators' reference actions included), each group in proposal order.

use alloy::primitives::Address;
use serde::Deserialize;

use super::{decimal::ExactDecimal, orders::OrderKey, pairing::IocPairingMismatch};
use crate::accounts::{checkpoint::node_time_ns, replica::{PairedAction, ReplicaLine}};

/// (bundle, category, position): actions run in this order.
pub(crate) type Rank = (usize, u8, usize);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MarkActions {
    pub(crate) height: u64,
    /// The block time, unix ns.
    pub(crate) time_ns: u64,
    /// In block order.
    pub(crate) actions: Vec<RankedAction>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RankedAction {
    pub(crate) rank: Rank,
    /// The response's account (`null` when the signer is unknown).
    pub(crate) user: Option<Address>,
    pub(crate) action: MarkAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MarkAction {
    /// An order, modify, batchModify, cancel or cancelByCloid that succeeded.
    Book(BookAnchors),
    /// A successful vault withdrawal can create IOC `Vault Close` orders absent from its default receipt.
    VaultWithdraw { vault: Address },
    /// Per main-dex asset, the submitted prices.
    SetGlobal(Vec<SubmittedPrices>),
    /// Successful HIP-3 setOracle: execution identity; written prices come from the oracle stream. `submitted` is its
    /// `markPxs` read as the xyz local model's two terms (design 000582), an `Err` only for that model.
    Hip3Oracle { dex: String, execution_index: Option<usize>, submitted: Result<Vec<SubmittedMark>, String> },
    /// A validator's submitted oracle and external perp prices, by coin.
    Reference { oracle: Vec<(String, String)>, external: Vec<(String, String)> },
}

/// One main-dex asset's prices in a `SetGlobalAction`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SubmittedPrices {
    /// The publisher does not price the asset.
    Unpriced,
    Priced { oracle: String, mark: String },
    /// One of the two: a coin the node has not fully built (bm 10-27, XTEN's first minute after its listing: the mark
    /// alone). The coin alone is unsupported until a seed.
    Partial,
}

impl SubmittedPrices {
    /// Whether the asset is priced; `None` for one of the two, whose submitted coverage is unknown.
    pub(crate) const fn priced(&self) -> Option<bool> {
        match self {
            Self::Unpriced => Some(false),
            Self::Priced { .. } => Some(true),
            Self::Partial => None,
        }
    }
}

impl MarkAction {
    pub(crate) const fn is_price(&self) -> bool {
        matches!(self, Self::SetGlobal(_) | Self::Reference { .. })
    }
}

/// What a book action did to resting orders (research method 2): the orders it created (resting or filled at once,
/// also the takers of its fills) with their cloids, the orders it removed by oid, and those it removed by cloid
/// (`execution` resolves them in execution order).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BookAnchors {
    /// Ordinary order wrapper only; modify/cancel have distinct funding boundaries.
    pub(crate) ordinary_order: bool,
    /// Exactly one ordinary IOC request; successful receipt count alone cannot exclude another rejected order.
    pub(crate) single_ioc_order: bool,
    /// Full identity of one ordinary reduce-only GTC request with an unambiguous resting receipt.
    pub(crate) resting_reduce_only: Option<Box<ReceiptedOrder>>,
    pub(crate) created: Vec<u64>,
    /// The created orders that carry a cloid: (cloid, oid).
    pub(crate) created_cloids: Vec<(String, u64)>,
    pub(crate) cancelled: Vec<u64>,
    pub(crate) cancelled_cloids: Vec<String>,
    /// The orders it submitted that may trade on entry (limit orders but ALO: an ALO only rests, a trigger order waits),
    /// whatever their status: a self-trade cancel names the action by its account, coin, the opposite side and a limit
    /// reaching the removed order (bm block 1191373718: an IOC buy with an order error removed its account's resting
    /// RUNE sell, while the account's ALO batch of the same bundle also bought RUNE; research
    /// `rune_1191370000_1191380000`).
    pub(crate) takers: Vec<Taker>,
    /// IOC requests whose paired response is the no-immediate-match error. Unlike a placed order, that response
    /// carries no oid; the ordered status stream can identify the rejected taker and its self-trade cancels.
    pub(crate) rejected_iocs: Vec<RejectedIoc>,
    /// A known whole-batch rejection or unknown item pairing. Both prevent IOC attribution; only unknown
    /// shapes qualify for bounded action-level evidence. Placed oids still come from the response.
    pub(crate) ioc_pairing_mismatch: Option<IocPairingMismatch>,
    /// Successful single modify with a default receipt: old order known, replacement oid only in statuses.
    pub(crate) unreceipted_modify: Option<Box<ReplacementRequest>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReceiptedOrder {
    pub(crate) oid: u64,
    pub(crate) asset: u32,
    pub(crate) key: OrderKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplacementRequest {
    pub(crate) old: OrderRef,
    pub(crate) asset: u32,
    pub(crate) key: OrderKey,
}

/// An order that may trade on entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Taker {
    pub(crate) asset: u32,
    pub(crate) is_buy: bool,
    pub(crate) limit_px: ExactDecimal,
}

/// Exact request fields used to identify a no-fill IOC status; no address-based self-trade domain is inferred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RejectedIoc {
    pub(crate) taker: Taker,
    pub(crate) sz: ExactDecimal,
    pub(crate) reduce_only: bool,
    pub(crate) cloid: Option<String>,
}

impl MarkActions {
    /// The kept actions of a parsed line; a field that does not read breaks the contract.
    pub(crate) fn of(replica: &ReplicaLine<'_>) -> Result<Self, String> {
        let height = replica.height;
        let time = replica.time().ok_or_else(|| format!("block {height} without abci_block.time"))?;
        let time_ns = node_time_ns(time).map_err(|err| format!("block {height}: {err}"))?;
        let mut actions = Vec::new();
        let paired_actions = replica.paired().map_err(|err| format!("block {height}: {err}"))?;
        for (index, paired) in paired_actions.iter().enumerate() {
            let mut kept = ranked(paired).map_err(|err| format!("block {height}: bundle {}: {err}", paired.bundle_hash))?;
            if let Some(RankedAction { action: MarkAction::Hip3Oracle { execution_index, .. }, .. }) = &mut kept {
                // Category-3 action: only later priority actions in this bundle move ahead of it.
                // A rejected malformed request can prevent this optional proof; it is not a mark failure.
                *execution_index = oracle_execution_index(index, &paired_actions);
            }
            actions.extend(kept);
        }
        Ok(Self { height, time_ns, actions })
    }
}

fn ranked(paired: &PairedAction<'_, '_>) -> Result<Option<RankedAction>, String> {
    // Vault withdrawals and setOracle wrapped by multiSig still execute at the outer action's category-3 rank.
    // Other wrapped actions retain their existing unsupported mark-attribution scope.
    let inner;
    let action = if paired.action.action_type == "multiSig" {
        match paired.response.res.status {
            "err" => return Ok(None),
            "ok" => {},
            status => return Err(format!("multiSig with status {status:?}")),
        }
        inner = paired.action.multi_sig_action()?;
        if !matches!(inner.action_type, "vaultTransfer" | "perpDeploy") { return Ok(None); }
        &inner
    } else { paired.action };
    let kind = action.action_type;
    if !matches!(kind, "order" | "modify" | "batchModify" | "cancel" | "cancelByCloid" | "vaultTransfer" | "perpDeploy" | "SetGlobalAction" | "validatorL1UpdateReferenceOracle") {
        return Ok(None);
    }
    match paired.response.res.status {
        "err" => return Ok(None),
        "ok" => {}
        status => return Err(format!("{kind} with status {status:?}")),
    }
    if kind == "perpDeploy" && action.set_oracle.is_none() { return Ok(None); }
    let user = paired.response.user.map(str::parse).transpose().map_err(|err| format!("{kind} user: {err}"))?;
    let statuses = || response_statuses(paired);
    let (category, action) = match kind {
        "order" => {
            let orders: Vec<WireOrder> = action.field(action.orders.as_ref(), "orders")?;
            let statuses = statuses()?;
            let mut anchors = order_anchors(&orders, &statuses)?;
            anchors.ordinary_order = true;
            anchors.resting_reduce_only = resting_reduce_only(user, &orders, &statuses)?;
            anchors.single_ioc_order = orders.len() == 1 && orders[0].t.limit.as_ref().is_some_and(|limit| limit.tif == "Ioc");
            (category(orders.iter().all(WireOrder::is_alo)), MarkAction::Book(anchors))
        }
        "modify" => {
            let order: WireOrder = action.field(action.order.as_ref(), "order")?;
            let old: OrderRef = action.field(action.oid.as_ref(), "oid")?;
            let orders = std::slice::from_ref(&order);
            let body = response_body(paired)?;
            let default_receipt = body.response_type == Some(WireResponseType::Default) && body.data.is_none();
            let statuses = body.data.map(|data| data.statuses).unwrap_or_default();
            let mut anchors = order_anchors(orders, &statuses)?;
            if default_receipt && let Some(key) = user.map(|user| OrderKey::requested(user, &order)).transpose()?.flatten() {
                anchors.unreceipted_modify = Some(Box::new(ReplacementRequest { old: old.clone(), asset: order.a, key }));
            }
            old.add_to(&mut anchors);
            (category(order.is_alo()), MarkAction::Book(anchors))
        }
        "batchModify" => {
            let modifies: Vec<WireModify> = action.field(action.modifies.as_ref(), "modifies")?;
            let statuses = statuses()?;
            let orders: Vec<&WireOrder> = modifies.iter().map(|modify| &modify.order).collect();
            let mut anchors = order_anchors(orders, &statuses)?;
            // only a modify that placed its new order replaced the old one (research method 2)
            if statuses.len() != modifies.len() {
                return Err(format!("batchModify of {} modifies with {} statuses", modifies.len(), statuses.len()));
            }
            for (modify, status) in modifies.iter().zip(&statuses) {
                if status.placed().is_some() {
                    modify.oid.add_to(&mut anchors);
                }
            }
            (category(modifies.iter().all(|modify| modify.order.is_alo())), MarkAction::Book(anchors))
        }
        "cancel" => {
            let cancels: Vec<WireCancel> = action.field(action.cancels.as_ref(), "cancels")?;
            let cancelled = succeeded(cancels.iter().map(|cancel| cancel.o), &statuses()?)?;
            (2, MarkAction::Book(BookAnchors { cancelled, ..BookAnchors::default() }))
        }
        "cancelByCloid" => {
            let cancels: Vec<WireCancelByCloid> = action.field(action.cancels.as_ref(), "cancels")?;
            let cancelled_cloids = succeeded(cancels.into_iter().map(|cancel| cancel.cloid), &statuses()?)?;
            (2, MarkAction::Book(BookAnchors { cancelled_cloids, ..BookAnchors::default() }))
        }
        "vaultTransfer" => {
            let deposit: bool = action.field(action.is_deposit.as_ref(), "isDeposit")?;
            if deposit { return Ok(None); }
            let vault = action.field(action.vault_address.as_ref(), "vaultAddress")?;
            (3, MarkAction::VaultWithdraw { vault })
        }
        "perpDeploy" => {
            (3, hip3_oracle(action)?)
        }
        "SetGlobalAction" => {
            let pxs: Vec<(Option<String>, Option<String>)> = action.field(action.pxs.as_ref(), "pxs")?;
            let pxs = pxs
                .into_iter()
                .map(|pair| match pair {
                    (Some(oracle), Some(mark)) => SubmittedPrices::Priced { oracle, mark },
                    (None, None) => SubmittedPrices::Unpriced,
                    _ => SubmittedPrices::Partial,
                })
                .collect();
            (3, MarkAction::SetGlobal(pxs))
        }
        _ => {
            let oracle = action.field(action.oracle_pxs.as_ref(), "oraclePxs")?;
            let external = action.field(action.external_perp_pxs.as_ref(), "externalPerpPxs")?;
            (3, MarkAction::Reference { oracle, external })
        }
    };
    Ok(Some(RankedAction { rank: (paired.bundle, category, paired.position), user, action }))
}

/// The hash locator counts all outer actions (including rejects and noops), in execution order.
fn oracle_execution_index(index: usize, actions: &[PairedAction<'_, '_>]) -> Option<usize> {
    let bundle = actions[index].bundle;
    let mut execution_index = index;
    for paired in actions[index + 1..].iter().take_while(|a| a.bundle == bundle) {
        let action = paired.action;
        let priority = match action.action_type {
            "noop" | "cancel" | "cancelByCloid" => true,
            "order" => action.field::<Vec<WireOrder>>(action.orders.as_ref(), "orders").ok()?.iter().all(WireOrder::is_alo),
            "modify" => action.field::<WireOrder>(action.order.as_ref(), "order").ok()?.is_alo(),
            "batchModify" => action.field::<Vec<WireModify>>(action.modifies.as_ref(), "modifies").ok()?.iter().all(|m| m.order.is_alo()),
            _ => false,
        };
        execution_index += usize::from(priority);
    }
    Some(execution_index)
}

/// A successful deployer action supplies timing, not a substitute for the written mark.
fn hip3_oracle(action: &crate::accounts::replica::Action<'_>) -> Result<MarkAction, String> {
    #[derive(Deserialize)]
    struct SetOracle { dex: String }
    let SetOracle { dex } = action.field(action.set_oracle.as_ref(), "setOracle")?;
    if dex.is_empty() || dex.contains(':') { return Err("setOracle with invalid dex".into()); }
    Ok(MarkAction::Hip3Oracle { dex, execution_index: None, submitted: submitted_marks(action) })
}

/// One coin's two `markPxs` terms as submitted: xyz's (1) oracle and (2) oracle plus its EMA (docs.trade.xyz mark
/// price; bm 10-30: every xyz write lists its 127 coins in both, the first equal to `oraclePxs`), wire decimals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubmittedMark {
    pub(crate) coin: String,
    pub(crate) oracle: String,
    pub(crate) ema: String,
}

/// The setOracle's `markPxs` as the xyz local model reads it: two lists of `[coin, px]` of the same coins. The
/// protocol allows 0, 1 or 2 lists of any deployer's meaning, so another form is no anchor for that model, never an
/// error of the action.
fn submitted_marks(action: &crate::accounts::replica::Action<'_>) -> Result<Vec<SubmittedMark>, String> {
    #[derive(Deserialize)]
    struct MarkPxs {
        #[serde(rename = "markPxs")]
        mark_pxs: Vec<Vec<(String, String)>>,
    }
    // the read setOracle object itself: not repeated in the reason (127 coins in 4 lists)
    let raw = action.set_oracle.as_ref().ok_or("no setOracle")?.as_raw_str();
    let MarkPxs { mark_pxs } = sonic_rs::from_str(raw).map_err(|err| format!("markPxs: {err}"))?;
    let [oracle, ema] = <[Vec<(String, String)>; 2]>::try_from(mark_pxs).map_err(|lists| format!("markPxs of {} lists, not 2", lists.len()))?;
    let mut emas: rustc_hash::FxHashMap<String, String> = ema.into_iter().collect();
    let submitted: Vec<SubmittedMark> = oracle
        .into_iter()
        .map(|(coin, oracle)| {
            let ema = emas.remove(&coin).ok_or_else(|| format!("markPxs: {coin} once in the first list, not in the second"))?;
            Ok(SubmittedMark { coin, oracle, ema })
        })
        .collect::<Result<_, String>>()?;
    if let Some(coin) = emas.keys().next() {
        return Err(format!("markPxs: {coin} in the second list only"));
    }
    Ok(submitted)
}

/// Preserve this narrow receipt proof separately from the broader list of placed oids.
fn resting_reduce_only(user: Option<Address>, orders: &[WireOrder], statuses: &[WireStatus])
    -> Result<Option<Box<ReceiptedOrder>>, String>
{
    let (Some(user), [order], [WireStatus::Object(status)]) = (user, orders, statuses) else { return Ok(None) };
    if !order.r || order.t.limit.as_ref().is_none_or(|limit| limit.tif != "Gtc")
        || status.error.is_some() || status.filled.is_some() { return Ok(None); }
    let Some(receipt) = &status.resting else { return Ok(None) };
    if receipt.cloid != order.c { return Ok(None); }
    let Some(key) = OrderKey::requested(user, order)? else { return Ok(None) };
    Ok(Some(Box::new(ReceiptedOrder { oid: receipt.oid, asset: order.a, key })))
}

/// Request/response pairing is optional evidence for no-fill IOC attribution, not a protocol invariant for an
/// order action. A count mismatch (including bm 1191390021's whole-batch error) keeps the placed oids, records
/// the counts, and identifies no rejected IOC. The status run still blocks the weaker same-account heuristic.
fn order_anchors<'o>(orders: impl IntoIterator<Item = &'o WireOrder>, statuses: &[WireStatus]) -> Result<BookAnchors, String> {
    let orders: Vec<_> = orders.into_iter().collect();
    let mut anchors = BookAnchors { takers: takers(orders.iter().copied())?, ..created_by(statuses) };
    // A default modify response has no item statuses; it has never identified a rejected IOC.
    if statuses.is_empty() {
        return Ok(anchors);
    }
    if orders.len() != statuses.len() {
        // A single error rejects the entire batch, including an empty request (BM 1192117276).
        let batch_rejected = matches!(statuses, [WireStatus::Object(status)]
            if status.error.is_some() && status.resting.is_none() && status.filled.is_none());
        anchors.ioc_pairing_mismatch = Some(if batch_rejected {
            IocPairingMismatch::BatchRejected
        } else {
            IocPairingMismatch::Unknown { requests: orders.len(), responses: statuses.len() }
        });
        return Ok(anchors);
    }
    for (order, status) in orders.into_iter().zip(statuses) {
        let WireStatus::Object(status) = status else { continue };
        if order.t.limit.as_ref().is_none_or(|limit| limit.tif != "Ioc")
            || status.error.as_deref() != Some(format!("Order could not immediately match against any resting orders. asset={}", order.a).as_str())
        {
            continue;
        }
        let taker = order.taker().transpose()?.ok_or("rejected IOC without a limit")?;
        anchors.rejected_iocs.push(RejectedIoc {
            taker,
            sz: ExactDecimal::parse(&order.s).map_err(|err| format!("rejected IOC size: {err}"))?,
            reduce_only: order.r,
            cloid: order.c.clone(),
        });
    }
    Ok(anchors)
}

/// The orders that may trade on entry; a limit price that does not read breaks the contract.
fn takers<'o>(orders: impl IntoIterator<Item = &'o WireOrder>) -> Result<Vec<Taker>, String> {
    orders.into_iter().filter_map(WireOrder::taker).collect()
}

const fn category(all_alo: bool) -> u8 {
    if all_alo { 1 } else { 3 }
}

/// The oids an order or batchModify response placed (resting, or filled at once), and their cloids.
fn created_by(statuses: &[WireStatus]) -> BookAnchors {
    let placed: Vec<&WireOid> = statuses.iter().filter_map(WireStatus::placed).collect();
    BookAnchors {
        created: placed.iter().map(|placed| placed.oid).collect(),
        created_cloids: placed.iter().filter_map(|placed| Some((placed.cloid.clone()?, placed.oid))).collect(),
        ..BookAnchors::default()
    }
}

/// The items whose status is `success`; the statuses must pair with the items.
fn succeeded<T>(items: impl ExactSizeIterator<Item = T>, statuses: &[WireStatus]) -> Result<Vec<T>, String> {
    if items.len() != statuses.len() {
        return Err(format!("{} cancels with {} statuses", items.len(), statuses.len()));
    }
    Ok(items.zip(statuses).filter(|(_, status)| matches!(status, WireStatus::Text(text) if text == "success")).map(|(item, _)| item).collect())
}

/// `response.data.statuses`; a response without data (`{"type": "default"}`, a modify's) has none.
fn response_statuses(paired: &PairedAction<'_, '_>) -> Result<Vec<WireStatus>, String> {
    Ok(response_body(paired)?.data.map(|data| data.statuses).unwrap_or_default())
}

fn response_body(paired: &PairedAction<'_, '_>) -> Result<WireResponseBody, String> {
    let raw = paired.response.res.response.as_ref().ok_or("an ok response without a body")?;
    sonic_rs::from_str(raw.as_raw_str()).map_err(|err| format!("response {}: {err}", raw.as_raw_str()))
}

#[derive(Deserialize, PartialEq, Eq)]
enum WireResponseType {
    #[serde(rename = "default")]
    Default,
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
struct WireResponseBody {
    #[serde(rename = "type")]
    response_type: Option<WireResponseType>,
    data: Option<WireResponseData>,
}

#[derive(Deserialize)]
struct WireResponseData {
    statuses: Vec<WireStatus>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum WireStatus {
    /// `"success"`, or another word.
    Text(String),
    Object(WireStatusObject),
}

#[derive(Deserialize)]
struct WireStatusObject {
    error: Option<String>,
    resting: Option<WireOid>,
    filled: Option<WireOid>,
}

#[derive(Deserialize)]
struct WireOid {
    oid: u64,
    cloid: Option<String>,
}

impl WireStatus {
    fn placed(&self) -> Option<&WireOid> {
        let Self::Object(object) = self else { return None };
        object.resting.as_ref().or(object.filled.as_ref())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct WireOrder {
    /// The asset id.
    pub(super) a: u32,
    /// Is buy.
    pub(super) b: bool,
    /// The limit price.
    pub(super) p: String,
    pub(super) s: String,
    pub(super) r: bool,
    pub(super) c: Option<String>,
    pub(super) t: WireOrderType,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct WireOrderType {
    pub(super) limit: Option<WireLimit>,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct WireLimit {
    pub(super) tif: String,
}

impl WireOrder {
    /// An ALO limit order; a trigger order is not.
    pub(super) fn is_alo(&self) -> bool {
        self.t.limit.as_ref().is_some_and(|limit| limit.tif == "Alo")
    }

    /// An order that may trade on entry: a limit order but ALO.
    fn taker(&self) -> Option<Result<Taker, String>> {
        self.t.limit.as_ref().filter(|limit| limit.tif != "Alo")?;
        let limit_px = ExactDecimal::parse(&self.p).map_err(|err| format!("order price {err}"));
        Some(limit_px.map(|limit_px| Taker { asset: self.a, is_buy: self.b, limit_px }))
    }
}

#[derive(Deserialize)]
struct WireModify {
    oid: OrderRef,
    order: WireOrder,
}

/// A modify names the old order by oid or by cloid.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub(crate) enum OrderRef {
    Oid(u64),
    Cloid(String),
}

impl OrderRef {
    fn add_to(&self, anchors: &mut BookAnchors) {
        match self {
            Self::Oid(oid) => anchors.cancelled.push(*oid),
            Self::Cloid(cloid) => anchors.cancelled_cloids.push(cloid.clone()),
        }
    }
}

#[derive(Deserialize)]
struct WireCancel {
    o: u64,
}

#[derive(Deserialize)]
struct WireCancelByCloid {
    cloid: String,
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    const USER: &str = "0x00000000000000000000000000000000000000aa";

    /// A replica line of block 1190783955 (bm 2026-10-20, time 13:36:16.650019121) with these bundles of
    /// (action, response) pairs, cut from real lines.
    pub(crate) fn replica_line(height: u64, bundles: &[&[(&str, &str)]]) -> String {
        let mut signed = Vec::new();
        let mut responses = Vec::new();
        for (index, bundle) in bundles.iter().enumerate() {
            let hash = format!("0x{index:02x}");
            let actions: Vec<String> = bundle.iter().map(|(action, _)| format!(r#"{{"action":{action},"nonce":1}}"#)).collect();
            let resps: Vec<String> = bundle.iter().map(|(_, response)| (*response).to_string()).collect();
            signed.push(format!(r#"["{hash}",{{"signed_actions":[{}]}}]"#, actions.join(",")));
            responses.push(format!(r#"["{hash}",[{}]]"#, resps.join(",")));
        }
        format!(
            r#"{{"abci_block":{{"time":"2026-10-20T13:36:16.650019121","signed_action_bundles":[{}]}},"resps":{{"Full":[{}]}},"height":{height}}}"#,
            signed.join(","),
            responses.join(",")
        )
    }

    pub(crate) fn ok(body: &str) -> String {
        format!(r#"{{"user":"{USER}","res":{{"status":"ok","response":{body}}}}}"#)
    }

    pub(crate) fn mark_actions(line: &str) -> MarkActions {
        MarkActions::of(&ReplicaLine::parse(line).unwrap()).unwrap()
    }

    #[test]
    fn hip3_price_identity_uses_successful_outer_rank_and_ignores_other_deployer_actions() {
        let action = r#"{"type":"perpDeploy","setOracle":{"dex":"xyz","oraclePxs":[],"markPxs":[]}}"#;
        let wrapped = format!(r#"{{"type":"multiSig","payload":{{"action":{action}}}}}"#);
        let accepted = ok(r#"{"type":"default"}"#);
        let rejected = r#"{"user":null,"res":{"status":"err","response":"business refusal"}}"#;
        let unrelated = r#"{"type":"perpDeploy","setFundingInterestRate":{"dex":"xyz"}}"#;
        let line = replica_line(1, &[&[(action, rejected), (unrelated, &accepted)], &[(action, &accepted), (&wrapped, &accepted)]]);
        let parsed = mark_actions(&line);
        assert_eq!(parsed.actions.len(), 2);
        assert_eq!(parsed.actions[0].rank, (1, 3, 0));
        assert_eq!(parsed.actions[1].rank, (1, 3, 1));
        for (index, a) in parsed.actions.into_iter().enumerate() {
            assert_eq!(a.action, MarkAction::Hip3Oracle { dex: "xyz".into(), execution_index: Some(index + 2), submitted: Err("markPxs of 0 lists, not 2".into()) });
            assert!(!a.action.is_price(), "HIP-3 timing evidence cannot alter native price evaluation");
        }
    }

    /// Design 000582: xyz's markPxs read as its two terms by coin; another form is no anchor and keeps the action.
    #[test]
    fn a_set_oracle_reads_its_two_mark_terms_or_says_why_not() {
        let accepted = ok(r#"{"type":"default"}"#);
        let submitted = |set_oracle: &str| {
            let action = format!(r#"{{"type":"perpDeploy","setOracle":{set_oracle}}}"#);
            let parsed = mark_actions(&replica_line(1, &[&[(&action, &accepted)]]));
            let [RankedAction { action: MarkAction::Hip3Oracle { submitted, .. }, .. }] = parsed.actions.as_slice() else { panic!("{parsed:?}") };
            submitted.clone()
        };
        let two = r#"{"dex":"xyz","oraclePxs":[["xyz:NVDA","201.5"]],"markPxs":[[["xyz:NVDA","201.5"],["xyz:TSLA","440.1"]],[["xyz:TSLA","440.31"],["xyz:NVDA","201.48"]]]}"#;
        assert_eq!(submitted(two), Ok(vec![
            SubmittedMark { coin: "xyz:NVDA".into(), oracle: "201.5".into(), ema: "201.48".into() },
            SubmittedMark { coin: "xyz:TSLA".into(), oracle: "440.1".into(), ema: "440.31".into() },
        ]));
        for (set_oracle, why) in [
            (r#"{"dex":"km","markPxs":[[["km:A","1"]]]}"#, "markPxs of 1 lists, not 2"),
            (r#"{"dex":"xyz","markPxs":[[["xyz:NVDA","1"]],[["xyz:TSLA","1"]]]}"#, "markPxs: xyz:NVDA once in the first list, not in the second"),
            (r#"{"dex":"xyz","markPxs":[[],[["xyz:TSLA","1"]]]}"#, "markPxs: xyz:TSLA in the second list only"),
        ] {
            assert_eq!(submitted(set_oracle), Err(why.to_string()), "{set_oracle}");
        }
        assert!(submitted(r#"{"dex":"xyz","markPxs":[[["xyz:NVDA",1]],[]]}"#).unwrap_err().starts_with("markPxs: "), "a number, not a wire decimal");
    }

    #[test]
    fn oracle_locator_counts_rejects_and_later_priority_actions_within_its_bundle() {
        let oracle = r#"{"type":"perpDeploy","setOracle":{"dex":"io"}}"#;
        let noop = r#"{"type":"noop"}"#;
        let alo = r#"{"a":1,"b":true,"p":"1","s":"1","r":false,"t":{"limit":{"tif":"Alo"}}}"#;
        let order = format!(r#"{{"type":"order","orders":[{alo}]}}"#);
        let modify = format!(r#"{{"type":"modify","oid":1,"order":{alo}}}"#);
        let batch = format!(r#"{{"type":"batchModify","modifies":[{{"oid":1,"order":{alo}}}]}}"#);
        let cancel = r#"{"type":"cancel","cancels":[]}"#;
        let wrapped = format!(r#"{{"type":"multiSig","payload":{{"action":{order}}}}}"#);
        let accepted = ok(r#"{"type":"default"}"#);
        let rejected = r#"{"user":null,"res":{"status":"err","response":"business refusal"}}"#;
        let parsed = mark_actions(&replica_line(1, &[
            &[(noop, &accepted)],
            &[(oracle, &accepted), (&order, rejected), (&modify, rejected), (&batch, rejected),
                (cancel, rejected), (noop, &accepted), (&wrapped, rejected)],
            &[(cancel, rejected)],
        ]));
        assert!(matches!(&parsed.actions[0].action, MarkAction::Hip3Oracle { dex, execution_index: Some(6), submitted: Err(_) } if dex == "io"),
            "a setOracle without markPxs keeps its identity: {:?}", parsed.actions[0].action);
        let unknown = r#"{"type":"order","orders":"unknown-wire-form"}"#;
        let parsed = mark_actions(&replica_line(1, &[&[(oracle, &accepted), (unknown, rejected)]]));
        assert!(matches!(&parsed.actions[0].action, MarkAction::Hip3Oracle { dex, execution_index: None, .. } if dex == "io"),
            "unknown optional priority evidence retains the oracle without inventing a locator or stopping marks");
    }

    #[test]
    fn each_kept_action_reads_its_anchors_and_rank() {
        let alo = r#"{"type":"order","orders":[{"a":1,"b":false,"p":"2386.4","s":"2.0943","r":false,"t":{"limit":{"tif":"Alo"}}}],"grouping":"na"}"#;
        let gtc = r#"{"type":"order","orders":[{"a":0,"b":true,"p":"81349","s":"0.1","r":false,"t":{"limit":{"tif":"Gtc"}}}],"grouping":"na"}"#;
        let modify = r#"{"type":"batchModify","modifies":[{"oid":"0xfe63","order":{"a":0,"b":true,"p":"81349","s":"0.00609","r":false,"t":{"limit":{"tif":"Alo"}}}},{"oid":77,"order":{"a":0,"b":true,"p":"81348","s":"0.1","r":false,"t":{"limit":{"tif":"Alo"}}}}]}"#;
        let cancel = r#"{"type":"cancelByCloid","cancels":[{"asset":1,"cloid":"0x0a"},{"asset":1,"cloid":"0x0b"}],"f":true}"#;
        let sga = r#"{"type":"SetGlobalAction","pxs":[["81520","81503"],[null,null]],"externalPerpPxs":[],"usdtUsdcPx":"1","nativePx":"78.7"}"#;
        let line = replica_line(1_190_783_955, &[
            &[
                (gtc, &ok(r#"{"type":"order","data":{"statuses":[{"filled":{"totalSz":"0.1","avgPx":"81349","oid":5}}]}}"#)),
                (alo, &ok(r#"{"type":"order","data":{"statuses":[{"resting":{"oid":6}}]}}"#)),
                (cancel, &ok(r#"{"type":"cancel","data":{"statuses":["success",{"error":"Order was never placed, already canceled, or filled. asset=1"}]}}"#)),
            ],
            &[
                (modify, &ok(r#"{"type":"order","data":{"statuses":[{"resting":{"oid":8,"cloid":"0xfe63"}},{"error":"Cannot modify canceled or filled order"}]}}"#)),
                (sga, &ok(r#"{"type":"setGlobal","data":[]}"#)),
                (r#"{"type":"noop"}"#, &ok(r#"{"type":"default"}"#)),
            ],
        ]);
        let parsed = mark_actions(&line);
        assert_eq!((parsed.height, parsed.time_ns), (1_190_783_955, 1_792_503_376_650_019_121));
        let ranks: Vec<Rank> = parsed.actions.iter().map(|action| action.rank).collect();
        assert_eq!(ranks, [(0, 3, 0), (0, 1, 1), (0, 2, 2), (1, 1, 0), (1, 3, 1)], "the noop anchors nothing");
        let book = |index: usize| match &parsed.actions[index].action {
            MarkAction::Book(anchors) => anchors.clone(),
            other => panic!("{other:?}"),
        };
        let gtc_taker = Taker { asset: 0, is_buy: true, limit_px: ExactDecimal::parse("81349").unwrap() };
        assert_eq!((book(0).created.as_slice(), book(0).takers.as_slice()), (&[5][..], &[gtc_taker][..]), "a GTC may take");
        assert_eq!(book(1).takers, [], "an ALO only rests");
        assert_eq!(book(2).cancelled_cloids, ["0x0a"], "only the cancel that succeeded");
        let modified = BookAnchors {
            ordinary_order: false,
            single_ioc_order: false,
            resting_reduce_only: None,
            created: vec![8],
            created_cloids: vec![("0xfe63".to_string(), 8)],
            cancelled: vec![],
            cancelled_cloids: vec!["0xfe63".to_string()],
            rejected_iocs: vec![],
            ioc_pairing_mismatch: None,
            unreceipted_modify: None,
            takers: Vec::new(),
        };
        assert_eq!(book(3), modified, "the failed modify of 77 removed nothing");
        let priced = SubmittedPrices::Priced { oracle: "81520".to_string(), mark: "81503".to_string() };
        assert_eq!(parsed.actions[4].action, MarkAction::SetGlobal(vec![priced, SubmittedPrices::Unpriced]));
        assert_eq!(parsed.actions[4].user, Some(USER.parse().unwrap()));
    }

    /// bm block 1191373718 (research `rune_1191370000_1191380000`): an IOC that matched nothing has an order error for
    /// a status and creates nothing, yet keeps its taking order, for the self-trade cancel it caused.
    #[test]
    fn an_order_with_an_order_error_keeps_its_taking_order() {
        let ioc = r#"{"type":"order","orders":[{"a":41,"b":true,"p":"0.67331","s":"34.9","r":false,"t":{"limit":{"tif":"Ioc"}}}],"grouping":"na"}"#;
        let error = r#"{"type":"order","data":{"statuses":[{"error":"Order could not immediately match against any resting orders. asset=41"}]}}"#;
        let parsed = mark_actions(&replica_line(1_191_373_718, &[&[(ioc, &ok(error))]]));
        let MarkAction::Book(anchors) = &parsed.actions[0].action else { panic!("{:?}", parsed.actions) };
        let ioc_taker = Taker { asset: 41, is_buy: true, limit_px: ExactDecimal::parse("0.67331").unwrap() };
        assert_eq!((anchors.created.as_slice(), anchors.takers.as_slice()), (&[][..], &[ioc_taker][..]));
    }

    #[test]
    fn batch_modify_keeps_its_required_pairing_and_default_modify_has_no_ioc_evidence() {
        let order = r#"{"a":41,"b":true,"p":"0.67331","s":"34.9","r":false,"t":{"limit":{"tif":"Ioc"}}}"#;
        let batch = format!(r#"{{"type":"batchModify","modifies":[{{"oid":1,"order":{order}}}]}}"#);
        let response = ok(r#"{"type":"order","data":{"statuses":[{"resting":{"oid":2}},{"resting":{"oid":3}}]}}"#);
        let line = replica_line(9, &[&[(&batch, &response)]]);
        let err = MarkActions::of(&ReplicaLine::parse(&line).unwrap()).unwrap_err();
        assert!(err.contains("batchModify of 1 modifies with 2 statuses"), "{err}");
        let modify = format!(r#"{{"type":"modify","oid":1,"order":{order}}}"#);
        let line = replica_line(9, &[&[(&modify, &ok(r#"{"type":"default"}"#))]]);
        let parsed = mark_actions(&line);
        let MarkAction::Book(book) = &parsed.actions[0].action else { panic!("book") };
        assert_eq!(book.cancelled, [1]);
        assert_eq!(book.ioc_pairing_mismatch, None);
        assert_eq!(book.rejected_iocs, []);
    }

    #[test]
    fn failed_and_other_actions_are_left_out_and_a_broken_one_fails() {
        let line = replica_line(9, &[&[
            (r#"{"type":"cancel","cancels":[{"a":1,"o":3}]}"#, r#"{"user":null,"res":{"status":"err","response":"User or API Wallet 0x01 does not exist."}}"#),
            (r#"{"type":"updateLeverage","asset":1,"isCross":true,"leverage":3}"#, &ok(r#"{"type":"default"}"#)),
        ]]);
        assert_eq!(mark_actions(&line).actions, []);
        let broken = replica_line(9, &[&[(r#"{"type":"cancel","cancels":[{"a":1,"o":3}]}"#, &ok(r#"{"type":"cancel","data":{"statuses":[]}}"#))]]);
        let error = MarkActions::of(&ReplicaLine::parse(&broken).unwrap()).unwrap_err();
        assert!(error.contains("block 9: bundle 0x00: 1 cancels with 0 statuses"), "{error}");
        // bm 10-27: XTEN's first minute; the coin alone is unsupported (`native.rs`), the block parses
        let half = replica_line(9, &[&[(r#"{"type":"SetGlobalAction","pxs":[[null,"1"]]}"#, &ok(r#"{"type":"setGlobal","data":[]}"#))]]);
        assert_eq!(mark_actions(&half).actions[0].action, MarkAction::SetGlobal(vec![SubmittedPrices::Partial]));
    }
}
