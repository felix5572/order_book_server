//! `replica_cmds` lines (design 000429 r2): one line per block, so the heights run on by one.
//!
//! `{"abci_block": {"signed_action_bundles": [[hash, {"signed_actions": [{"action": {"type", ...}, ...}], ...}]], ...},
//! "resps": {"Full": [[hash, [{"user", "res": {"status", "response"}}]]]}, "height"}`. A response belongs to the action
//! at the same index of the bundle with the same hash. Its `user` is the account acted on: the `vaultAddress` when one
//! is given (bm 2026-10-19: 1,387 of 1,387), `null` when the signer is unknown.
//!
//! Only the successful actions that move a pool's cash or rules without a fill or misc event are kept. Transfers such
//! as `usdClassTransfer` and `send` are not: their ledger events already mark the account.

use alloy::primitives::Address;
use rustc_hash::FxHashMap;
use serde::Deserialize;

/// The block actions that may invalidate an anchor's liquidation price.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlockActionKind {
    /// `updateIsolatedMargin`, `updateLeverage`: moves isolated margin, or a position between the pools.
    Margin,
    /// `userSetAbstraction`, `agentSetAbstraction`: changes the pool rules.
    AccountMode,
    /// `withdraw3`: debited when requested; its ledger event comes minutes later.
    Withdrawal,
}

impl BlockActionKind {
    fn of(action_type: &str) -> Option<Self> {
        match action_type {
            "updateIsolatedMargin" | "updateLeverage" => Some(Self::Margin),
            "userSetAbstraction" | "agentSetAbstraction" => Some(Self::AccountMode),
            "withdraw3" => Some(Self::Withdrawal),
            _ => None,
        }
    }
}

/// A kept action and the account it acted on.
pub(crate) type KeptAction = (BlockActionKind, Address);

/// The kept actions of one block, in block order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlockActions {
    pub(crate) height: u64,
    pub(crate) actions: Vec<KeptAction>,
}

#[derive(Deserialize)]
struct ReplicaLine<'a> {
    #[serde(borrow)]
    abci_block: AbciBlock<'a>,
    #[serde(borrow)]
    resps: Responses<'a>,
    height: u64,
}

#[derive(Deserialize)]
struct AbciBlock<'a> {
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

#[derive(Deserialize)]
struct Action<'a> {
    #[serde(rename = "type")]
    action_type: &'a str,
}

#[derive(Deserialize)]
struct Responses<'a> {
    #[serde(rename = "Full", borrow)]
    full: Vec<(&'a str, Vec<Response<'a>>)>,
}

#[derive(Deserialize)]
struct Response<'a> {
    user: Option<&'a str>,
    #[serde(borrow)]
    res: ResponseResult<'a>,
}

#[derive(Deserialize)]
struct ResponseResult<'a> {
    status: &'a str,
}

#[derive(Deserialize)]
struct ReplicaHeader {
    height: u64,
}

/// The block's height, when the line still reads that far: the loss bound of a line that failed to parse.
pub(crate) fn line_height(line: &str) -> Option<u64> {
    sonic_rs::from_str::<ReplicaHeader>(line).ok().map(|header| header.height)
}

/// Parses one line; an error names the height once the structure read (a response that does not pair with its action,
/// or a kept action whose status or user breaks the contract).
pub(crate) fn parse_block_actions(line: &str) -> Result<BlockActions, String> {
    let replica: ReplicaLine<'_> = sonic_rs::from_str(line).map_err(|err| err.to_string())?;
    let height = replica.height;
    let actions = replica.kept_actions().map_err(|err| format!("block {height}: {err}"))?;
    Ok(BlockActions { height, actions })
}

impl ReplicaLine<'_> {
    fn kept_actions(&self) -> Result<Vec<KeptAction>, String> {
        let responses = self.responses_by_bundle()?;
        let mut kept = Vec::new();
        for (hash, bundle) in &self.abci_block.signed_action_bundles {
            let group = responses.get(hash).ok_or_else(|| format!("bundle {hash} has no responses"))?;
            bundle.keep_actions(group, &mut kept).map_err(|err| format!("bundle {hash}: {err}"))?;
        }
        Ok(kept)
    }

    /// Every bundle's responses by its hash; the groups match the bundles one to one.
    fn responses_by_bundle(&self) -> Result<FxHashMap<&str, &[Response<'_>]>, String> {
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

impl Bundle<'_> {
    /// Appends the kept actions that succeeded, each with its response's user.
    fn keep_actions(&self, responses: &[Response<'_>], kept: &mut Vec<KeptAction>) -> Result<(), String> {
        if responses.len() != self.signed_actions.len() {
            return Err(format!("{} actions but {} responses", self.signed_actions.len(), responses.len()));
        }
        for (signed, response) in self.signed_actions.iter().zip(responses) {
            let action_type = signed.action.action_type;
            let Some(kind) = BlockActionKind::of(action_type) else {
                continue;
            };
            if let Some(user) = succeeded(action_type, response)? {
                kept.push((kind, user));
            }
        }
        Ok(())
    }
}

/// The account a kept action acted on; `None` when it failed.
fn succeeded(action_type: &str, response: &Response<'_>) -> Result<Option<Address>, String> {
    match response.res.status {
        "err" => Ok(None),
        "ok" => {
            let user = response.user.ok_or_else(|| format!("successful {action_type} without a user"))?;
            user.parse().map(Some).map_err(|err| format!("successful {action_type} user {user:?}: {err}"))
        }
        status => Err(format!("{action_type} with status {status:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real line (block 1189931524, bm 2026-10-19) cut to three actions of its first bundle and its second bundle:
    /// a vault `cancelByCloid` (ok), a vault `updateIsolatedMargin` (err), a vault `updateLeverage` (ok), and a
    /// `noop` whose response has a `null` user.
    const REAL_LINE: &str = r#"{"abci_block":{"time":"2026-10-19T20:35:15.736492512","signed_action_bundles":[["0x46427850137732d6ec0474bfc36737c61e377dd5a4e0c88ee82a1d37f73317dd",{"signed_actions":[{"signature":{"r":"0xfcbe1025c140a460a986f382489d20f6695dd66ec30d145adc993ab22ef4ca87","s":"0x4271417ae78acbd223bf7f39b8fe555e60b9f64b9b13a7626e9fe6aef958d08a","v":27},"vaultAddress":"0x223537ac9a856c31f4043e86ced86bb29f06653e","action":{"type":"cancelByCloid","cancels":[{"asset":204,"cloid":"0x0000000000000000000001a15380e8a2"}],"f":true},"nonce":1792403786740},{"signature":{"r":"0xfe8f00ab50d88c5b85327ee18e0cf05935d44a5144a1dcd3fddb56008e626d1f","s":"0x22755a14eabda67b7717e0b0377cfe981558b949aec8f3301ddc7adae4bc91c0","v":27},"vaultAddress":"0xd33c704c68518e527bdcb36f7966c80edcffbdf2","action":{"type":"updateIsolatedMargin","asset":110015,"isBuy":true,"ntli":17556885255},"nonce":1792442115154},{"signature":{"r":"0x879dba7eea9f9190e1ae5a2e2da7ab31e1f80cebaf52b80c17d9d3ca5ed8d26e","s":"0x2069d97dacd8cf786eb40aab7f518489b9b2559fdbb0fdaf6016caf1828a916","v":27},"vaultAddress":"0x18db1cab14f2886cee69c11ae0ad5b0b691c3a60","action":{"type":"updateLeverage","asset":99,"isCross":true,"leverage":3},"nonce":1792442115089}],"broadcaster":"0xc9ba414780c1b75e9806e3469300993fd2bfa883","broadcaster_nonce":1792441897707}],["0x2b8320fdc32a40ace4f0d8d478cb638b786da7a9669a4ac0edff77652fab46fd",{"signed_actions":[{"signature":{"r":"0xf831fc6c4964be0b26fff04a8fb3dbed321b664e4dc89494892edb5ab233ee43","s":"0x70064ac470000ce1805d9737cbe7eb69cab0e04740197a33bb5e159365f2b1f0","v":28},"action":{"type":"noop"},"nonce":1792441992551,"expiresAfter":1792442145579}],"broadcaster":"0x1e9b90ab34427807dc25c7266beb188e86af7ed6","broadcaster_nonce":1792441992552}]],"round":1493629492,"parent_round":1493629491,"hardfork":{"version":108,"round":1481450749},"proposer":"0x80f0cd23da5bf3a0101110cfd0f89c8a69a1384d"},"resps":{"Full":[["0x46427850137732d6ec0474bfc36737c61e377dd5a4e0c88ee82a1d37f73317dd",[{"user":"0x223537ac9a856c31f4043e86ced86bb29f06653e","res":{"status":"ok","response":{"type":"cancel","data":{"statuses":["success"]}}}},{"user":"0xd33c704c68518e527bdcb36f7966c80edcffbdf2","res":{"status":"err","response":"Position does not use isolated margin."}},{"user":"0x18db1cab14f2886cee69c11ae0ad5b0b691c3a60","res":{"status":"ok","response":{"type":"default"}}}]],["0x2b8320fdc32a40ace4f0d8d478cb638b786da7a9669a4ac0edff77652fab46fd",[{"user":null,"res":{"status":"err","response":"User or API Wallet 0xc6a474dde4a9c81ed7d0b894322bb8af6327b8ea does not exist."}}]]]},"height":1189931524}"#;
    const LEVERAGE_VAULT: &str = "0x18db1cab14f2886cee69c11ae0ad5b0b691c3a60";
    const MARGIN_VAULT: &str = "0xd33c704c68518e527bdcb36f7966c80edcffbdf2";
    const FIRST_BUNDLE: &str = "0x46427850137732d6ec0474bfc36737c61e377dd5a4e0c88ee82a1d37f73317dd";
    const SECOND_BUNDLE: &str = "0x2b8320fdc32a40ace4f0d8d478cb638b786da7a9669a4ac0edff77652fab46fd";

    fn address(text: &str) -> Address {
        text.parse().unwrap()
    }

    #[test]
    fn keeps_the_successful_actions_with_the_responses_user() {
        let parsed = parse_block_actions(REAL_LINE).unwrap();
        let leverage = (BlockActionKind::Margin, address(LEVERAGE_VAULT));
        assert_eq!(parsed, BlockActions { height: 1_189_931_524, actions: vec![leverage] });
    }

    #[test]
    fn every_kept_type_maps_to_its_kind() {
        let cases = [
            ("updateIsolatedMargin", BlockActionKind::Margin),
            ("userSetAbstraction", BlockActionKind::AccountMode),
            ("agentSetAbstraction", BlockActionKind::AccountMode),
            ("withdraw3", BlockActionKind::Withdrawal),
        ];
        for (action_type, kind) in cases {
            let line = REAL_LINE.replace(r#""type":"updateLeverage""#, &format!(r#""type":"{action_type}""#));
            assert_eq!(parse_block_actions(&line).unwrap().actions, [(kind, address(LEVERAGE_VAULT))], "{action_type}");
        }
    }

    #[test]
    fn a_failed_action_is_not_kept() {
        let line = REAL_LINE.replace(
            r#"{"status":"err","response":"Position does not use isolated margin."}"#,
            r#"{"status":"ok","response":{"type":"default"}}"#,
        );
        let kept = parse_block_actions(&line).unwrap().actions;
        let margin = |vault| (BlockActionKind::Margin, address(vault));
        assert_eq!(kept, [margin(MARGIN_VAULT), margin(LEVERAGE_VAULT)], "block order");
    }

    #[test]
    fn responses_pair_by_bundle_hash_not_by_position() {
        let first_group = &REAL_LINE[REAL_LINE.find(&format!(r#"["{FIRST_BUNDLE}",[{{"user""#)).unwrap()..];
        let first_group = &first_group[..first_group.find(&format!(r#",["{SECOND_BUNDLE}""#)).unwrap()];
        let second_group = &REAL_LINE[REAL_LINE.find(&format!(r#"["{SECOND_BUNDLE}",[{{"user":null"#)).unwrap()..];
        let second_group = &second_group[..second_group.find("]]]}").unwrap() + 2];
        let swapped =
            REAL_LINE.replace(&format!("{first_group},{second_group}"), &format!("{second_group},{first_group}"));
        assert_ne!(swapped, REAL_LINE);
        assert_eq!(parse_block_actions(&swapped), parse_block_actions(REAL_LINE));
    }

    #[test]
    fn responses_that_do_not_pair_break_the_contract() {
        let unknown_hash = REAL_LINE.replacen(SECOND_BUNDLE, "0x00", 1);
        let short_group =
            REAL_LINE.replace(&format!(r#",{{"user":"{LEVERAGE_VAULT}","res":{{"status":"ok","response":{{"type":"default"}}}}}}"#), "");
        let null_group = |hash: &str| format!(r#"["{hash}",[{{"user":null"#);
        let twice = REAL_LINE.replace(&null_group(SECOND_BUNDLE), &null_group(FIRST_BUNDLE));
        for (line, expected) in [
            (unknown_hash, "bundle 0x00 has no responses"),
            (short_group, "3 actions but 2 responses"),
            (twice, "appear twice"),
        ] {
            let err = parse_block_actions(&line).expect_err(expected);
            assert!(err.starts_with("block 1189931524: ") && err.contains(expected), "{err}");
        }
    }

    #[test]
    fn a_kept_action_with_an_unknown_status_or_no_user_breaks_the_contract() {
        let pending = REAL_LINE.replace(r#""status":"ok","response":{"type":"default"}"#, r#""status":"queued""#);
        let no_user = REAL_LINE.replace(&format!(r#"{{"user":"{LEVERAGE_VAULT}""#), r#"{"user":null"#);
        let cases = [(pending, r#"updateLeverage with status "queued""#), (no_user, "successful updateLeverage without a user")];
        for (line, expected) in cases {
            assert!(parse_block_actions(&line).expect_err(expected).contains(expected));
        }
    }

    #[test]
    fn a_block_without_kept_actions_still_reads_its_height() {
        let line = REAL_LINE.replace(r#""type":"updateLeverage""#, r#""type":"order""#);
        assert_eq!(parse_block_actions(&line).unwrap(), BlockActions { height: 1_189_931_524, actions: Vec::new() });
    }

    #[test]
    fn the_height_of_a_line_that_breaks_the_schema_still_reads() {
        let broken = REAL_LINE.replace(r#""signed_actions":"#, r#""actions":"#);
        assert!(parse_block_actions(&broken).is_err());
        assert_eq!(line_height(&broken), Some(1_189_931_524));
        assert_eq!(line_height(&REAL_LINE[..REAL_LINE.len() - 30]), None);
    }
}
