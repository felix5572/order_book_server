//! A `replica_cmds` line, parsed once for the account view's kept actions (`actions`) and the node marks' actions
//! (`marks::actions`).
//!
//! `{"abci_block": {"time", "signed_action_bundles": [[hash, {"signed_actions": [{"action": {"type", ...}, ...}], ...}]],
//! ...}, "resps": {"Full": [[hash, [{"user", "res": {"status", "response"}}]]]}, "height"}`. A response belongs to the
//! action at the same index of the bundle with the same hash. Its `user` is the account acted on: the `vaultAddress`
//! when one is given (bm 2026-10-19: 1,387 of 1,387), `null` when the signer is unknown. Payload fields are read only
//! once the action's type is known (other action types use the same field names with other value types).

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

#[derive(Deserialize)]
struct SignedAction<'a> {
    #[serde(borrow)]
    action: Action<'a>,
}

/// An action's type, and the raw payload fields the kept types read.
#[derive(Deserialize)]
pub(crate) struct Action<'a> {
    #[serde(rename = "type")]
    pub(crate) action_type: &'a str,
    #[serde(borrow)]
    pub(crate) asset: Option<LazyValue<'a>>,
    #[serde(borrow)]
    pub(crate) ntli: Option<LazyValue<'a>>,
    #[serde(borrow, rename = "isCross")]
    pub(crate) is_cross: Option<LazyValue<'a>>,
    #[serde(borrow)]
    pub(crate) amount: Option<LazyValue<'a>>,
    #[serde(borrow)]
    pub(crate) orders: Option<LazyValue<'a>>,
    #[serde(borrow)]
    pub(crate) cancels: Option<LazyValue<'a>>,
    #[serde(borrow)]
    pub(crate) modifies: Option<LazyValue<'a>>,
    #[serde(borrow)]
    pub(crate) oid: Option<LazyValue<'a>>,
    #[serde(borrow)]
    pub(crate) order: Option<LazyValue<'a>>,
    #[serde(borrow)]
    pub(crate) pxs: Option<LazyValue<'a>>,
    #[serde(borrow, rename = "oraclePxs")]
    pub(crate) oracle_pxs: Option<LazyValue<'a>>,
    #[serde(borrow, rename = "externalPerpPxs")]
    pub(crate) external_perp_pxs: Option<LazyValue<'a>>,
}

impl Action<'_> {
    /// A payload field the action's type requires.
    pub(crate) fn field<T: DeserializeOwned>(&self, raw: Option<&LazyValue<'_>>, name: &str) -> Result<T, String> {
        let raw = raw.ok_or_else(|| format!("{} without {name}", self.action_type))?;
        sonic_rs::from_str(raw.as_raw_str()).map_err(|err| format!("{} {name} {}: {err}", self.action_type, raw.as_raw_str()))
    }
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
