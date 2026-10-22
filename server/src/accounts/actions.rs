//! The account view's actions of a `replica_cmds` line (design 000429 r2): one line per block, so the heights run on by
//! one; the line's format and the pairing of actions with responses are `replica`'s.
//!
//! Only the successful actions that move a pool's cash or rules without a fill or misc event are kept. Transfers such
//! as `usdClassTransfer` and `send` are not: their ledger events already mark the account.

use alloy::primitives::Address;

use super::{
    checkpoint::node_time_ns,
    replica::{Action, ReplicaLine, Response},
    usdc::withdrawal_wei,
};

/// The block actions that may invalidate an anchor's liquidation price.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlockActionKind {
    /// `updateIsolatedMargin`, `updateLeverage`, `topUpIsolatedOnlyMargin`: moves isolated margin, or changes the
    /// pool a new position of the asset takes.
    Margin,
    /// `userSetAbstraction`, `agentSetAbstraction`: changes the pool rules.
    AccountMode,
    /// `withdraw3`: debited when requested; its ledger event comes minutes later.
    Withdrawal,
    /// `reserveRequestWeight`: purchase request quota from the payer's main perp balance.
    RequestWeight,
}

impl BlockActionKind {
    fn of(action_type: &str) -> Option<Self> {
        match action_type {
            "updateIsolatedMargin" | "updateLeverage" | "topUpIsolatedOnlyMargin" => Some(Self::Margin),
            "userSetAbstraction" | "agentSetAbstraction" => Some(Self::AccountMode),
            "withdraw3" => Some(Self::Withdrawal),
            "reserveRequestWeight" => Some(Self::RequestWeight),
            _ => None,
        }
    }
}

/// What a kept action does (design 000433 r2 3.3). Asset ids are the actions' own: the index on the main dex,
/// 100000 + 10000 × pdi + index on another dex.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BlockAction {
    /// `updateIsolatedMargin`: `ntli` (micro USDC, signed) from the dex's cross cash into the asset's isolated cash.
    IsolatedMargin { asset: u32, ntli_micro: i64 },
    /// `updateLeverage`: the asset's setting and leverage. The node refuses a switch between cross and isolated while a
    /// position is open ("Cannot switch leverage type with open position."), so a switch moves no cash; an isolated
    /// position's later fills move margin at the leverage (research "逐仓成交的划转").
    Leverage { asset: u32, is_cross: bool, leverage: u32 },
    /// `topUpIsolatedOnlyMargin`: a target effective leverage, NOT the integer leverage setting.
    /// Keep the API float string verbatim for diagnosis until execution-time state permits cash replay.
    TopUpIsolatedOnlyMargin { asset: u32, target_leverage: String },
    /// `userSetAbstraction`, `agentSetAbstraction`.
    SetAbstraction,
    /// `withdraw3`: the amount rounded to micro USDC (fee included), stored in 10^-8 USDC, leaves the main
    /// pool when the request is accepted.
    Withdraw { amount_wei: i64 },
    /// Cost is charged to the receipt user, even when another account receives the quota.
    ReserveRequestWeight { cost_wei: i64 },
}

impl BlockAction {
    pub(crate) const fn kind(&self) -> BlockActionKind {
        match self {
            Self::IsolatedMargin { .. } | Self::Leverage { .. } | Self::TopUpIsolatedOnlyMargin { .. } => BlockActionKind::Margin,
            Self::SetAbstraction => BlockActionKind::AccountMode,
            Self::Withdraw { .. } => BlockActionKind::Withdrawal,
            Self::ReserveRequestWeight { .. } => BlockActionKind::RequestWeight,
        }
    }

    /// Reads the payload of a kept action type.
    fn read(action: &Action<'_>) -> Result<Self, String> {
        Ok(match action.action_type {
            "updateIsolatedMargin" => Self::IsolatedMargin { asset: action.field(action.asset.as_ref(), "asset")?, ntli_micro: action.field(action.ntli.as_ref(), "ntli")? },
            "updateLeverage" => Self::Leverage {
                asset: action.field(action.asset.as_ref(), "asset")?,
                is_cross: action.field(action.is_cross.as_ref(), "isCross")?,
                leverage: action.field(action.leverage.as_ref(), "leverage")?,
            },
            "topUpIsolatedOnlyMargin" => Self::TopUpIsolatedOnlyMargin {
                asset: action.field(action.asset.as_ref(), "asset")?,
                target_leverage: action.field(action.leverage.as_ref(), "leverage")?,
            },
            "userSetAbstraction" | "agentSetAbstraction" => Self::SetAbstraction,
            "withdraw3" => {
                let amount: String = action.field(action.amount.as_ref(), "amount")?;
                Self::Withdraw { amount_wei: withdrawal_wei(&amount).map_err(|err| format!("withdraw3 amount: {err}"))? }
            }
            "reserveRequestWeight" => {
                let weight: u64 = action.field(action.weight.as_ref(), "weight")?;
                // Exchange API: 0.0005 USDC/request, paid from Perps. H1191758386 confirms
                // 100000 requests cost the receipt user 50 USDC, with no separate ledger delta.
                let cost_wei = weight.checked_mul(50_000).and_then(|value| i64::try_from(value).ok())
                    .ok_or_else(|| format!("reserveRequestWeight cost overflows 10^-8 USDC for weight {weight}"))?;
                Self::ReserveRequestWeight { cost_wei }
            }
            other => return Err(format!("{other} is not a kept action type")),
        })
    }
}

/// A kept action and the account it acted on.
pub(crate) type KeptAction = (BlockAction, Address);

/// The kept actions of one block, in block order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlockActions {
    pub(crate) height: u64,
    /// The block's time (`abci_block.time`), unix ms.
    pub(crate) time_ms: u64,
    pub(crate) actions: Vec<KeptAction>,
}

/// Parses one line; an error names the height once the structure read (a response that does not pair with its action,
/// or a kept action whose status or user breaks the contract).
#[cfg(test)]
pub(crate) fn parse_block_actions(line: &str) -> Result<BlockActions, String> {
    block_actions(&ReplicaLine::parse(line)?)
}

/// The kept actions of a parsed line.
pub(crate) fn block_actions(replica: &ReplicaLine<'_>) -> Result<BlockActions, String> {
    let height = replica.height;
    let time = replica.time().ok_or_else(|| format!("block {height} without abci_block.time"))?;
    let time_ms = node_time_ns(time).map_err(|err| format!("block {height}: {err}"))? / 1_000_000;
    let mut kept = Vec::new();
    for paired in replica.paired().map_err(|err| format!("block {height}: {err}"))? {
        let read = || -> Result<Option<KeptAction>, String> {
            if paired.action.action_type != "multiSig" {
                return kept_action(paired.action, paired.response);
            }
            // The outer receipt is the execution result, including failed authorization. Its user is the actual
            // account (possibly a vault/subaccount), not necessarily payload.multiSigUser or outerSigner.
            match paired.response.res.status {
                "err" => return Ok(None),
                "ok" => {}
                status => return Err(format!("multiSig with status {status:?}")),
            }
            kept_action(&paired.action.multi_sig_action()?, paired.response)
        };
        if let Some(action) = read().map_err(|err| format!("block {height}: bundle {}: {err}", paired.bundle_hash))? {
            kept.push(action);
        }
    }
    Ok(BlockActions { height, time_ms, actions: kept })
}

fn kept_action(action: &Action<'_>, response: &Response<'_>) -> Result<Option<KeptAction>, String> {
    if BlockActionKind::of(action.action_type).is_none() {
        return Ok(None);
    }
    let Some(user) = succeeded(action.action_type, response)? else { return Ok(None) };
    Ok(Some((BlockAction::read(action)?, user)))
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
pub(crate) mod tests {
    use super::*;
    use crate::accounts::replica::line_height;

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

    /// The kept `updateLeverage` of `REAL_LINE`.
    const LEVERAGE_ACTION: &str = r#"{"type":"updateLeverage","asset":99,"isCross":true,"leverage":3}"#;

    #[test]
    fn keeps_the_successful_actions_with_the_responses_user() {
        let parsed = parse_block_actions(REAL_LINE).unwrap();
        let leverage = (BlockAction::Leverage { asset: 99, is_cross: true, leverage: 3 }, address(LEVERAGE_VAULT));
        assert_eq!(parsed, BlockActions { height: 1_189_931_524, time_ms: 1_792_442_115_736, actions: vec![leverage] });
    }

    /// Payloads of real successful actions (bm `replica_cmds`, 2026-10-19), put in the place of the kept one.
    #[test]
    fn every_kept_type_reads_its_payload() {
        let cases = [
            (r#"{"type":"updateIsolatedMargin","asset":110086,"isBuy":true,"ntli":-4950000}"#, BlockAction::IsolatedMargin { asset: 110_086, ntli_micro: -4_950_000 }),
            (r#"{"type":"userSetAbstraction","signatureChainId":"0xa4b1","hyperliquidChain":"Mainnet","user":"0x77cb5ed9458def628fdb13e0f9098cc3a454bd04","abstraction":"u","nonce":1792446348734}"#, BlockAction::SetAbstraction),
            (r#"{"type":"agentSetAbstraction","abstraction":"u"}"#, BlockAction::SetAbstraction),
            (r#"{"type":"withdraw3","signatureChainId":"0xa4b1","hyperliquidChain":"Mainnet","destination":"0x619d9addcefccbe3bde5591b72b9cbe1103c5e43","amount":"34.390000","time":1792446358887}"#, BlockAction::Withdraw { amount_wei: 3_439_000_000 }),
        ];
        for (payload, expected) in cases {
            let line = REAL_LINE.replace(LEVERAGE_ACTION, payload);
            assert_eq!(parse_block_actions(&line).unwrap().actions, [(expected.clone(), address(LEVERAGE_VAULT))], "{payload}");
        }
        let kinds = [
            (BlockAction::TopUpIsolatedOnlyMargin { asset: 5, target_leverage: "0.75".into() }, BlockActionKind::Margin),
            (BlockAction::SetAbstraction, BlockActionKind::AccountMode),
            (BlockAction::Withdraw { amount_wei: 1 }, BlockActionKind::Withdrawal),
        ];
        for (action, kind) in kinds {
            assert_eq!(action.kind(), kind);
        }
    }

    /// API-contract fixture, not an observed successful mainnet action (none in the 2026-10-22 scan).
    #[test]
    fn target_leverage_topup_retains_payload_and_rejections_do_not_invalidate_cash() {
        let payload = r#"{"type":"topUpIsolatedOnlyMargin","asset":110104,"leverage":"0.7500"}"#;
        let line = REAL_LINE.replace(LEVERAGE_ACTION, payload);
        assert_eq!(parse_block_actions(&line).unwrap().actions,
            [(BlockAction::TopUpIsolatedOnlyMargin { asset: 110_104, target_leverage: "0.7500".into() }, address(LEVERAGE_VAULT))]);
        for bad in [
            r#"{"type":"topUpIsolatedOnlyMargin","asset":110104}"#,
            r#"{"type":"topUpIsolatedOnlyMargin","asset":110104,"leverage":0.75}"#,
            r#"{"type":"topUpIsolatedOnlyMargin","leverage":"0.75"}"#,
        ] {
            let invalid = REAL_LINE.replace(LEVERAGE_ACTION, bad);
            assert!(parse_block_actions(&invalid).is_err(), "a successful action requires the API fields: {bad}");
            let rejected = invalid.replace(r#"{"status":"ok","response":{"type":"default"}}"#,
                r#"{"status":"err","response":"Asset not strict isolated"}"#);
            assert!(parse_block_actions(&rejected).unwrap().actions.is_empty(), "rejected payloads move no cash");
        }
    }

    /// A kept type's payload must read; another type may use the same field names with other value types.
    #[test]
    fn only_a_kept_type_must_read_its_payload() {
        let without_is_cross = REAL_LINE.replace(LEVERAGE_ACTION, r#"{"type":"updateLeverage","asset":99,"leverage":3}"#);
        let err = parse_block_actions(&without_is_cross).unwrap_err();
        assert!(err.contains("updateLeverage without isCross"), "{err}");
        let numeric_amount = REAL_LINE.replace(LEVERAGE_ACTION, r#"{"type":"withdraw3","amount":34.39,"time":1}"#);
        assert!(parse_block_actions(&numeric_amount).unwrap_err().contains("withdraw3 amount 34.39"));
        let other_type = REAL_LINE.replace(LEVERAGE_ACTION, r#"{"type":"vaultTransfer","isDeposit":true,"usd":5,"amount":7,"asset":"x"}"#);
        assert_eq!(parse_block_actions(&other_type).unwrap().actions, []);
    }

    #[test]
    fn a_failed_action_is_not_kept() {
        let line = REAL_LINE.replace(
            r#"{"status":"err","response":"Position does not use isolated margin."}"#,
            r#"{"status":"ok","response":{"type":"default"}}"#,
        );
        let kept = parse_block_actions(&line).unwrap().actions;
        let margin = (BlockAction::IsolatedMargin { asset: 110_015, ntli_micro: 17_556_885_255 }, address(MARGIN_VAULT));
        let leverage = (BlockAction::Leverage { asset: 99, is_cross: true, leverage: 3 }, address(LEVERAGE_VAULT));
        assert_eq!(kept, [margin, leverage], "block order");
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
        let empty = BlockActions { height: 1_189_931_524, time_ms: 1_792_442_115_736, actions: Vec::new() };
        assert_eq!(parse_block_actions(&line).unwrap(), empty);
    }

    #[test]
    fn the_height_of_a_line_that_breaks_the_schema_still_reads() {
        let broken = REAL_LINE.replace(r#""signed_actions":"#, r#""actions":"#);
        assert!(parse_block_actions(&broken).is_err());
        assert_eq!(line_height(&broken), Some(1_189_931_524));
        assert_eq!(line_height(&REAL_LINE[..REAL_LINE.len() - 30]), None);
    }
    pub(crate) fn action_line(case: &serde_json::Value) -> String {
        serde_json::json!({
            "height": case["height"],
            "abci_block": {"time": case["time"], "signed_action_bundles": [[case["bundle"], {
                "signed_actions": [{"action": case["action"]}]
            }]]},
            "resps": {"Full": [[case["bundle"], [case["receipt"]]]]}
        }).to_string()
    }

    #[test]
    fn request_weight_uses_receipt_payer_and_checks_successful_weight_only() {
        let mut case: serde_json::Value = serde_json::from_str(include_str!("request_weight_golden.json")).unwrap();
        let payer = address(case["receipt"]["user"].as_str().unwrap());
        assert_ne!(payer, address(case["action"]["destination"].as_str().unwrap()));
        let expected = [(BlockAction::ReserveRequestWeight { cost_wei: 5_000_000_000 }, payer)];
        assert_eq!(parse_block_actions(&action_line(&case)).unwrap().actions, expected);
        case["action"].as_object_mut().unwrap().remove("destination");
        assert_eq!(parse_block_actions(&action_line(&case)).unwrap().actions, expected);
        let inner = case["action"].clone();
        case["action"] = serde_json::json!({"type":"multiSig", "payload":{"action":inner}});
        assert_eq!(parse_block_actions(&action_line(&case)).unwrap().actions, expected);
        for weight in [serde_json::Value::Null, serde_json::json!(-1), serde_json::json!(1.5),
            serde_json::json!("100000"), serde_json::json!(u64::MAX)] {
            case["action"] = serde_json::json!({"type":"reserveRequestWeight", "weight":weight});
            case["receipt"]["res"]["status"] = serde_json::json!("ok");
            assert!(parse_block_actions(&action_line(&case)).unwrap_err().contains("reserveRequestWeight"));
            case["receipt"]["res"]["status"] = serde_json::json!("err");
            assert_eq!(parse_block_actions(&action_line(&case)).unwrap().actions, []);
        }
    }

    #[test]
    fn real_multisig_margin_is_kept_but_ledger_owned_transfers_are_not() {
        let cases: Vec<serde_json::Value> = serde_json::from_str(include_str!("actions_multisig_goldens.json")).unwrap();
        for case in cases {
            let block = parse_block_actions(&action_line(&case)).unwrap();
            if case["height"] == 1_191_644_834_u64 {
                assert_eq!(block.actions, [(BlockAction::IsolatedMargin { asset: 110_104, ntli_micro: 67_297_170 },
                    address("0xa38f19c4ccbcfd64cff676d72c436f7e6239dccf"))]);
            } else {
                assert!(block.actions.is_empty(), "order/sendAsset has its own fill/ledger owner");
            }
            let replica = action_line(&case);
            let replica = ReplicaLine::parse(&replica).unwrap();
            assert_eq!(replica.paired().unwrap()[0].action.action_type, "multiSig", "no global unwrap");
        }
    }

    #[test]
    fn multisig_uses_outer_status_and_effective_user_and_validates_kept_payloads() {
        let cases: Vec<serde_json::Value> = serde_json::from_str(include_str!("actions_multisig_goldens.json")).unwrap();
        let original = &cases[0];
        let mut case = original.clone();
        case["receipt"]["user"] = serde_json::json!(LEVERAGE_VAULT);
        assert_eq!(parse_block_actions(&action_line(&case)).unwrap().actions[0].1, address(LEVERAGE_VAULT));
        case["receipt"]["res"]["status"] = serde_json::json!("err");
        case["action"]["payload"] = serde_json::json!(null);
        assert!(parse_block_actions(&action_line(&case)).unwrap().actions.is_empty(), "failed payload need not parse");
        for (value, expected) in [(serde_json::json!(null), "without payload"),
            (serde_json::json!({"action":{"type":"updateIsolatedMargin","asset":110_104}}), "without ntli"),
            (serde_json::json!({"action":{"type":"multiSig"}}), "nested multiSig")] {
            let mut case = original.clone();
            case["action"]["payload"] = value;
            assert!(parse_block_actions(&action_line(&case)).unwrap_err().contains(expected));
        }
        let mut case = original.clone();
        case["receipt"]["user"] = serde_json::json!(null);
        assert!(parse_block_actions(&action_line(&case)).unwrap_err().contains("without a user"));
    }

}
