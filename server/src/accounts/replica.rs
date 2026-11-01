//! A `replica_cmds` line, parsed once for the account view's kept actions (`actions`) and the node marks' actions
//! (`marks::actions`); its signed action's `Action` and the book actions' payloads are the mempool's too (the pending
//! book, `pending::mempool`).
//!
//! `{"abci_block": {"time", "signed_action_bundles": [[hash, {"signed_actions": [{"action": {"type", ...}, ...}], ...}]],
//! ...}, "resps": {"Full": [[hash, [{"user", "res": {"status", "response"}}]]]}, "height"}`. A response belongs to the
//! action at the same index of the bundle with the same hash. Its `user` normally identifies the acting account:
//! the `vaultAddress` when given (bm 2026-10-19: 1,387 of 1,387), `null` when the signer is unknown. Some actions
//! explicitly name another target: `userSetAbstraction.user` can be a subaccount of the receipt user. Payload fields are read only
//! once the action's type is known (other action types use the same field names with other value types).

use alloy::primitives::Address;
use rustc_hash::FxHashMap;
use serde::{Deserialize, de::DeserializeOwned};
use sonic_rs::LazyValue;

#[derive(Deserialize)]
pub(crate) struct ReplicaLine<'a> {
    #[serde(borrow)]
    abci_block: AbciBlock<'a>,
    #[serde(borrow)]
    resps: Responses<'a>,
    pub(crate) height: u64,
}

#[derive(Deserialize)]
struct AbciBlock<'a> {
    /// The block time (`2026-10-19T20:35:15.736492512`); the node marks need it.
    time: Option<&'a str>,
    #[serde(borrow)]
    signed_action_bundles: Vec<(&'a str, Bundle<'a>)>,
}

#[derive(Deserialize)]
struct Bundle<'a> {
    #[serde(borrow)]
    signed_actions: Vec<SignedAction<'a>>,
}

/// A signed action: its action, and what else its signature covers (HL's L1 action signing), kept raw - read by the
/// pending book alone (`pending::signer`).
#[derive(Deserialize)]
pub(crate) struct SignedAction<'a> {
    #[serde(borrow)]
    pub(crate) action: Action<'a>,
    #[serde(borrow, rename = "vaultAddress")]
    pub(crate) vault_address: Option<LazyValue<'a>>,
    #[serde(borrow, rename = "expiresAfter")]
    pub(crate) expires_after: Option<LazyValue<'a>>,
    #[serde(borrow)]
    pub(crate) nonce: Option<LazyValue<'a>>,
    #[serde(borrow)]
    pub(crate) signature: Option<LazyValue<'a>>,
}

impl SignedAction<'_> {
    /// The vault or subaccount it acts for; `None` when the signer acts.
    pub(crate) fn vault(&self) -> Result<Option<Address>, String> {
        envelope_field(self.vault_address.as_ref(), "vaultAddress")
    }

    /// Its `expiresAfter`, unix ms: HL refuses it past then.
    pub(crate) fn expires_after_ms(&self) -> Result<Option<u64>, String> {
        envelope_field(self.expires_after.as_ref(), "expiresAfter")
    }
}

/// An optional envelope field, `None` when absent or `null`.
fn envelope_field<T: DeserializeOwned>(raw: Option<&LazyValue<'_>>, name: &str) -> Result<Option<T>, String> {
    raw.map(|raw| sonic_rs::from_str(raw.as_raw_str()).map_err(|err| format!("{name} {}: {err}", raw.as_raw_str()))).transpose()
}

/// An action's type, and the raw payload fields the kept types read.
#[derive(Deserialize)]
pub(crate) struct Action<'a> {
    #[serde(rename = "type")]
    pub(crate) action_type: &'a str,
    /// Read by `userSetAbstraction` only; unrelated actions may use a different payload type.
    #[serde(borrow)]
    pub(crate) user: Option<LazyValue<'a>>,
    /// A `twapCancel`'s TWAP id (the pending book, batch 2h).
    #[serde(borrow)]
    pub(crate) t: Option<LazyValue<'a>>,
    #[serde(borrow)]
    pub(crate) asset: Option<LazyValue<'a>>,
    #[serde(borrow)]
    pub(crate) ntli: Option<LazyValue<'a>>,
    #[serde(borrow, rename = "isCross")]
    pub(crate) is_cross: Option<LazyValue<'a>>,
    #[serde(borrow)]
    pub(crate) leverage: Option<LazyValue<'a>>,
    #[serde(borrow)]
    pub(crate) amount: Option<LazyValue<'a>>,
    #[serde(borrow)]
    pub(crate) weight: Option<LazyValue<'a>>,
    #[serde(borrow, rename = "vaultAddress")]
    pub(crate) vault_address: Option<LazyValue<'a>>,
    #[serde(borrow, rename = "isDeposit")]
    pub(crate) is_deposit: Option<LazyValue<'a>>,
    /// Kept raw: each projection unwraps successful `multiSig` actions only for its supported inner types.
    #[serde(borrow)]
    pub(crate) payload: Option<LazyValue<'a>>,
    #[serde(borrow)]
    pub(crate) orders: Option<LazyValue<'a>>,
    /// An `order`'s grouping: `"na"`, `"normalTpsl"`, `"positionTpsl"`, or a priority `{"p": N}` (HL docs "Priority
    /// fees"; bm 10-31: every order action has one).
    #[serde(borrow)]
    pub(crate) grouping: Option<LazyValue<'a>>,
    #[serde(borrow)]
    pub(crate) cancels: Option<LazyValue<'a>>,
    #[serde(borrow)]
    pub(crate) modifies: Option<LazyValue<'a>>,
    #[serde(borrow)]
    pub(crate) oid: Option<LazyValue<'a>>,
    #[serde(borrow)]
    pub(crate) order: Option<LazyValue<'a>>,
    #[serde(borrow, rename = "setOracle")]
    pub(crate) set_oracle: Option<LazyValue<'a>>,
    #[serde(borrow)]
    pub(crate) pxs: Option<LazyValue<'a>>,
    #[serde(borrow, rename = "oraclePxs")]
    pub(crate) oracle_pxs: Option<LazyValue<'a>>,
    #[serde(borrow, rename = "externalPerpPxs")]
    pub(crate) external_perp_pxs: Option<LazyValue<'a>>,
    /// `sendAsset`'s, read by `actions::signed_send` only.
    #[serde(borrow)]
    pub(crate) destination: Option<LazyValue<'a>>,
    #[serde(borrow, rename = "sourceDex")]
    pub(crate) source_dex: Option<LazyValue<'a>>,
    #[serde(borrow, rename = "destinationDex")]
    pub(crate) destination_dex: Option<LazyValue<'a>>,
    #[serde(borrow)]
    pub(crate) token: Option<LazyValue<'a>>,
    #[serde(borrow, rename = "fromSubAccount")]
    pub(crate) from_sub_account: Option<LazyValue<'a>>,
    /// The signed payload's own nonce (its ledger event's), not the signed action's outer one.
    #[serde(borrow)]
    pub(crate) nonce: Option<LazyValue<'a>>,
    /// A `modify` / `batchModify`'s `always_place` (omitted when false; inside an order, `a` is its asset); a `null`
    /// is kept, to fail as the flag it is not (review 000609 r2 P3).
    #[serde(borrow, rename = "a", default, deserialize_with = "present")]
    pub(crate) always_place: Option<LazyValue<'a>>,
    /// A `cancel` / `cancelByCloid`'s `fast` (omitted when false, a `null` kept as for `always_place`): the cancel goes
    /// first in the mempool, in effect on 2026-10-31 (the user; the API doc still calls it a future upgrade's), and a
    /// fast cancel of a trigger order is rejected.
    #[serde(borrow, rename = "f", default, deserialize_with = "present")]
    pub(crate) fast: Option<LazyValue<'a>>,
}

/// A field that is there, whatever it holds, `null` included (`Option`'s own reading takes a `null` for absent).
fn present<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Option<LazyValue<'de>>, D::Error> {
    LazyValue::deserialize(deserializer).map(Some)
}

#[derive(Deserialize)]
struct MultiSigPayload<'a> {
    #[serde(borrow)]
    action: Action<'a>,
}

impl Action<'_> {
    /// The executed inner action; the caller still uses the outer receipt and execution rank.
    pub(crate) fn multi_sig_action(&self) -> Result<Action<'_>, String> {
        let raw = self.payload.as_ref().ok_or("multiSig without payload")?;
        let payload: MultiSigPayload<'_> = sonic_rs::from_str(raw.as_raw_str())
            .map_err(|err| format!("multiSig payload: {err}"))?;
        if payload.action.action_type == "multiSig" {
            return Err("nested multiSig execution is not verified".to_string());
        }
        Ok(payload.action)
    }

    /// An `order`'s grouping's priority `p`; 0 for a named grouping (`na`, a TP/SL one).
    pub(crate) fn requested_priority(&self) -> Result<u64, String> {
        Ok(match self.field(self.grouping.as_ref(), "grouping")? {
            WireGrouping::Priority { p } => p,
            WireGrouping::Named(_) => 0,
        })
    }

    /// An `order`'s grouping by its TP/SL meaning (batch 2m): `normalTpsl` (the first order's children follow it),
    /// `positionTpsl`, or neither (`na`, a priority's); `Err` for a name HL does not list.
    pub(crate) fn tpsl_grouping(&self) -> Result<TpslGrouping, String> {
        match self.field(self.grouping.as_ref(), "grouping")? {
            WireGrouping::Priority { .. } => Ok(TpslGrouping::None),
            WireGrouping::Named(name) => match name.as_str() {
                "na" => Ok(TpslGrouping::None),
                "normalTpsl" => Ok(TpslGrouping::Normal),
                "positionTpsl" => Ok(TpslGrouping::Position),
                other => Err(format!("order grouping {other:?}")),
            },
        }
    }

    /// A payload field the action's type requires.
    pub(crate) fn field<T: DeserializeOwned>(&self, raw: Option<&LazyValue<'_>>, name: &str) -> Result<T, String> {
        let raw = raw.ok_or_else(|| format!("{} without {name}", self.action_type))?;
        sonic_rs::from_str(raw.as_raw_str()).map_err(|err| format!("{} {name} {}: {err}", self.action_type, raw.as_raw_str()))
    }
}

/// An `order` action's ALO priority: its `requested` one when every order of `orders` is a non-reduce-only ALO on an
/// asset not an outcome's, else 0 (HL docs "Priority fees": "Priority grouping is only supported for order actions
/// where both 1. every order is on a non-outcome asset 2. every order is IOC, or every order is a non-reduce-only
/// ALO"). An action out of it carrying a `p` is accepted all the same (research `mempool_pending_book_20261031`, bm
/// 10-31: IOC, trigger, GTC and reduce-only ALO actions with one, none refused for it); an IOC's priority orders the
/// mempool, not a level's queue.
pub(crate) fn alo_priority(requested: u64, orders: &[WireOrder]) -> u64 {
    let eligible = orders.iter().all(|order| order.a < OUTCOME_ASSETS && order.is_alo() && !order.r);
    if eligible { requested } else { 0 }
}

/// Outcomes' asset ids start here: 100,000,000 + 10 * outcome + side (HL docs "Asset IDs"; a HIP-3 perp's,
/// 100,000 + 10,000 * dex + index, stays under it while there are fewer than 9,990 dexes).
pub(crate) const OUTCOME_ASSETS: u32 = 100_000_000;

/// An `order`'s `grouping`.
#[derive(Deserialize)]
#[serde(untagged)]
enum WireGrouping {
    /// `na`, `normalTpsl` or `positionTpsl`: no priority.
    Named(String),
    Priority { p: u64 },
}

/// An `order` action's TP/SL grouping (HL docs "Exchange endpoint": `na`, `normalTpsl`, `positionTpsl`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TpslGrouping {
    None,
    /// The first order a parent, the trigger orders after it its children: they wait for it.
    Normal,
    /// TP/SL orders of the account's whole position.
    Position,
}

/// An order in an `order` action's `orders`, or a modify's new order.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct WireOrder {
    /// The asset id.
    pub(crate) a: u32,
    /// Is buy.
    pub(crate) b: bool,
    /// The limit price.
    pub(crate) p: String,
    pub(crate) s: String,
    pub(crate) r: bool,
    pub(crate) c: Option<String>,
    pub(crate) t: WireOrderType,
}

/// `{"limit": {"tif"}}`, or a trigger order's `{"trigger": ...}`.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct WireOrderType {
    pub(crate) limit: Option<WireLimit>,
    pub(crate) trigger: Option<WireTrigger>,
}

/// A trigger order's `{"isMarket", "triggerPx", "tpsl"}`: each optional here - the marks read only that an order is
/// one -, the pending book requiring them (`mempool::requested`, batch 2m).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WireTrigger {
    pub(crate) is_market: Option<bool>,
    pub(crate) trigger_px: Option<String>,
    pub(crate) tpsl: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct WireLimit {
    pub(crate) tif: String,
}

impl WireOrder {
    /// An ALO limit order; a trigger order is not.
    pub(crate) fn is_alo(&self) -> bool {
        self.t.limit.as_ref().is_some_and(|limit| limit.tif == "Alo")
    }
}

/// An `order`, `modify` or `batchModify`'s category in its bundle's execution: 1 when its new orders are all ALO, else
/// 3; a `cancel`'s or `cancelByCloid`'s is `CANCEL_CATEGORY`, between them. Each category runs in the bundle's listed
/// order (Codex, binary 6911b792 0x231ae60; research `mempool_pending_book_20261031`, 6.7M actions of 10,000 blocks:
/// 99.83% of neighbours). The node marks' full ladder is in `marks::actions`; the pending book orders the mempool's by it.
pub(crate) const fn placing_category(all_alo: bool) -> u8 {
    if all_alo { 1 } else { 3 }
}

pub(crate) const CANCEL_CATEGORY: u8 = 2;

/// One of a `batchModify`'s `modifies` (a `modify` carries the same two fields in its action).
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct WireModify {
    pub(crate) oid: OrderRef,
    pub(crate) order: WireOrder,
}

/// A modify names the old order by oid or by cloid.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub(crate) enum OrderRef {
    Oid(u64),
    Cloid(String),
}

/// One of a `cancel`'s `cancels`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct WireCancel {
    /// The asset id.
    pub(crate) a: u32,
    pub(crate) o: u64,
}

/// One of a `cancelByCloid`'s `cancels`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct WireCancelByCloid {
    pub(crate) asset: u32,
    pub(crate) cloid: String,
}

#[derive(Deserialize)]
struct Responses<'a> {
    #[serde(rename = "Full", borrow)]
    full: Vec<(&'a str, Vec<Response<'a>>)>,
}

#[derive(Deserialize)]
pub(crate) struct Response<'a> {
    pub(crate) user: Option<&'a str>,
    #[serde(borrow)]
    pub(crate) res: ResponseResult<'a>,
}

#[derive(Deserialize)]
pub(crate) struct ResponseResult<'a> {
    pub(crate) status: &'a str,
    /// An object for a successful action, the error text otherwise.
    #[serde(borrow)]
    pub(crate) response: Option<LazyValue<'a>>,
}

/// `response.data.statuses`; a response without data (`{"type": "default"}`, a modify's) has none.
pub(crate) fn response_statuses(paired: &PairedAction<'_, '_>) -> Result<Vec<WireStatus>, String> {
    Ok(response_body(paired)?.data.map(|data| data.statuses).unwrap_or_default())
}

pub(crate) fn response_body(paired: &PairedAction<'_, '_>) -> Result<WireResponseBody, String> {
    let raw = paired.response.res.response.as_ref().ok_or("an ok response without a body")?;
    sonic_rs::from_str(raw.as_raw_str()).map_err(|err| format!("response {}: {err}", raw.as_raw_str()))
}

#[derive(Deserialize, PartialEq, Eq)]
pub(crate) enum WireResponseType {
    #[serde(rename = "default")]
    Default,
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
pub(crate) struct WireResponseBody {
    #[serde(rename = "type")]
    pub(crate) response_type: Option<WireResponseType>,
    pub(crate) data: Option<WireResponseData>,
}

#[derive(Deserialize)]
pub(crate) struct WireResponseData {
    pub(crate) statuses: Vec<WireStatus>,
}

#[derive(Deserialize)]
#[serde(untagged)]
pub(crate) enum WireStatus {
    /// `"success"`, or another word.
    Text(String),
    Object(WireStatusObject),
}

#[derive(Deserialize)]
pub(crate) struct WireStatusObject {
    pub(crate) error: Option<String>,
    pub(crate) resting: Option<WireOid>,
    pub(crate) filled: Option<WireOid>,
}

#[derive(Deserialize)]
pub(crate) struct WireOid {
    pub(crate) oid: u64,
    pub(crate) cloid: Option<String>,
}

impl WireStatus {
    /// The order a status names as placed: resting, or filled (in part or whole) on entry.
    pub(crate) fn placed(&self) -> Option<&WireOid> {
        let Self::Object(object) = self else { return None };
        object.resting.as_ref().or(object.filled.as_ref())
    }
}

/// An action with its response and its place in the block.
pub(crate) struct PairedAction<'r, 'a> {
    pub(crate) bundle_hash: &'a str,
    pub(crate) bundle: usize,
    /// Index within the bundle, the proposal order.
    pub(crate) position: usize,
    pub(crate) action: &'r Action<'a>,
    pub(crate) response: &'r Response<'a>,
}

#[derive(Deserialize)]
struct ReplicaHeader {
    height: u64,
}

/// The block's height, when the line still reads that far: the loss bound of a line that failed to parse.
pub(crate) fn line_height(line: &str) -> Option<u64> {
    sonic_rs::from_str::<ReplicaHeader>(line).ok().map(|header| header.height)
}

impl<'a> ReplicaLine<'a> {
    pub(crate) fn parse(line: &'a str) -> Result<Self, String> {
        sonic_rs::from_str(line).map_err(|err| err.to_string())
    }

    pub(crate) const fn time(&self) -> Option<&'a str> {
        self.abci_block.time
    }

    /// Its bundles' hashes, in the block's order.
    pub(crate) fn bundle_hashes(&self) -> impl Iterator<Item = &'a str> + '_ {
        self.abci_block.signed_action_bundles.iter().map(|(hash, _)| *hash)
    }

    /// The signed action at `position` of bundle `bundle`, as `PairedAction` places it.
    pub(crate) fn signed_action(&self, bundle: usize, position: usize) -> Option<&SignedAction<'a>> {
        self.abci_block.signed_action_bundles.get(bundle)?.1.signed_actions.get(position)
    }

    /// The two original wire objects for an already parsed action. Used only for a bounded number of unknown
    /// pairing samples, never for ordinary blocks or known batch rejections. Response groups can be reordered.
    pub(crate) fn raw_action_pair(&self, line: &'a str, bundle: usize, position: usize)
        -> Result<(&'a str, LazyValue<'a>, LazyValue<'a>), String>
    {
        let (hash, _) = self.abci_block.signed_action_bundles.get(bundle).ok_or("evidence bundle missing")?;
        let response_bundle = self.resps.full.iter().position(|(candidate, _)| candidate == hash)
            .ok_or("evidence response bundle missing")?;
        let action = sonic_rs::get(line, sonic_rs::pointer!["abci_block", "signed_action_bundles", bundle, 1, "signed_actions", position, "action"])
            .map_err(|err| format!("action evidence: {err}"))?;
        let response = sonic_rs::get(line, sonic_rs::pointer!["resps", "Full", response_bundle, 1, position])
            .map_err(|err| format!("response evidence: {err}"))?;
        Ok((hash, action, response))
    }

    /// Every action with its response, in block order; the response groups must pair with the bundles one to one.
    pub(crate) fn paired(&self) -> Result<Vec<PairedAction<'_, 'a>>, String> {
        let responses = self.responses_by_bundle()?;
        let mut out = Vec::new();
        for (bundle, (hash, actions)) in self.abci_block.signed_action_bundles.iter().enumerate() {
            let group = responses.get(hash).ok_or_else(|| format!("bundle {hash} has no responses"))?;
            if group.len() != actions.signed_actions.len() {
                return Err(format!("bundle {hash}: {} actions but {} responses", actions.signed_actions.len(), group.len()));
            }
            for (position, (signed, response)) in actions.signed_actions.iter().zip(group.iter()).enumerate() {
                out.push(PairedAction { bundle_hash: hash, bundle, position, action: &signed.action, response });
            }
        }
        Ok(out)
    }

    /// Every bundle's responses by its hash; the groups match the bundles one to one.
    fn responses_by_bundle(&self) -> Result<FxHashMap<&'a str, &[Response<'a>]>, String> {
        let (bundles, groups) = (self.abci_block.signed_action_bundles.len(), self.resps.full.len());
        if bundles != groups {
            return Err(format!("{bundles} bundles but {groups} response groups"));
        }
        let mut responses = FxHashMap::default();
        for (hash, group) in &self.resps.full {
            if responses.insert(*hash, group.as_slice()).is_some() {
                return Err(format!("responses of bundle {hash} appear twice"));
            }
        }
        Ok(responses)
    }
}
