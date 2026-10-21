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
    /// Per main-dex asset, the submitted (oracle, mark); `None` for an asset the publisher does not price.
    SetGlobal(Vec<Option<(String, String)>>),
    /// A validator's submitted oracle and external perp prices, by coin.
    Reference { oracle: Vec<(String, String)>, external: Vec<(String, String)> },
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
        for paired in replica.paired().map_err(|err| format!("block {height}: {err}"))? {
            let kept = ranked(&paired).map_err(|err| format!("block {height}: bundle {}: {err}", paired.bundle_hash))?;
            actions.extend(kept);
        }
        Ok(Self { height, time_ns, actions })
    }
}

fn ranked(paired: &PairedAction<'_, '_>) -> Result<Option<RankedAction>, String> {
    // Vault withdrawals wrapped by multiSig still execute at the outer action's category-3 rank.
    // Other wrapped actions retain their existing unsupported mark-attribution scope.
    let inner;
    let action = if paired.action.action_type == "multiSig" {
        match paired.response.res.status {
            "err" => return Ok(None),
            "ok" => {},
            status => return Err(format!("multiSig with status {status:?}")),
        }
        inner = paired.action.multi_sig_action()?;
        if inner.action_type != "vaultTransfer" { return Ok(None); }
        &inner
    } else { paired.action };
    let kind = action.action_type;
    if !matches!(kind, "order" | "modify" | "batchModify" | "cancel" | "cancelByCloid" | "vaultTransfer" | "SetGlobalAction" | "validatorL1UpdateReferenceOracle") {
        return Ok(None);
    }
    match paired.response.res.status {
        "err" => return Ok(None),
        "ok" => {}
        status => return Err(format!("{kind} with status {status:?}")),
    }
    let user = paired.response.user.map(str::parse).transpose().map_err(|err| format!("{kind} user: {err}"))?;
    let statuses = || response_statuses(paired);
    let (category, action) = match kind {
        "order" => {
            let orders: Vec<WireOrder> = action.field(action.orders.as_ref(), "orders")?;
            let statuses = statuses()?;
            let anchors = order_anchors(&orders, &statuses)?;
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
        "SetGlobalAction" => {
            let pxs: Vec<(Option<String>, Option<String>)> = action.field(action.pxs.as_ref(), "pxs")?;
            let pxs = pxs
                .into_iter()
                .enumerate()
                .map(|(asset, pair)| match pair {
                    (Some(oracle), Some(mark)) => Ok(Some((oracle, mark))),
                    (None, None) => Ok(None),
                    _ => Err(format!("SetGlobalAction prices asset {asset} with one of oracle and mark")),
                })
                .collect::<Result<Vec<_>, String>>()?;
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
        assert_eq!(parsed.actions[4].action, MarkAction::SetGlobal(vec![Some(("81520".to_string(), "81503".to_string())), None]));
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
        let half = replica_line(9, &[&[(r#"{"type":"SetGlobalAction","pxs":[["1",null]]}"#, &ok(r#"{"type":"setGlobal","data":[]}"#))]]);
        assert!(MarkActions::of(&ReplicaLine::parse(&half).unwrap()).unwrap_err().contains("one of oracle and mark"));
    }
}
