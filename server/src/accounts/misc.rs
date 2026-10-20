//! `misc_events_streaming` lines: `Batch<MiscEvent>`, one event per line, lines only for blocks that have events.
//!
//! Each event is `{"time", "hash", "inner": {<Kind>: {...}}}`. Two kinds move margin: `LedgerUpdate` (`users`: every
//! account of the update, `delta` read below) and `Funding` (one line per dex; `deltas[]` with the account, the coin
//! and the signed `funding_amount`; may be empty). The other kinds seen on 2026-10-19 (staking, validator rewards,
//! sub-account creation, gossip auction) are skipped; a kind outside both lists is reported as unknown.
//!
//! A ledger delta has one field set per `type` (research `hl_account_state_verify_20261019` "账本事件的 schema": 16
//! types on 2026-10-20 04-07 UTC). The fields the cash replay (design 000433) uses are required for their type; amounts
//! are 10^-8 USDC (`usdc.rs`), read only where the unit is USDC: the `usdc`-named fields, and `amount` / `fee` when
//! `token` / `feeToken` is USDC. Types the replay does not read are kept by name.

use std::fmt;

use alloy::primitives::Address;
use serde::{
    Deserialize, Deserializer,
    de::{IgnoredAny, MapAccess, Visitor},
};

use super::usdc::usdc_wei;

/// Kinds observed that never move perp margin of a USDC pool.
const IRRELEVANT_KINDS: [&str; 6] =
    ["CDeposit", "CWithdrawal", "Delegation", "CreateSubAccount", "ValidatorRewards", "GossipPriorityAuctionRestart"];
/// The token name of every USDC amount.
pub(crate) const USDC_NAME: &str = "USDC";
/// Ledger types whose `token` was only ever HYPE (2026-10-19/20): no USDC moves unless the token says so.
const TOKEN_TYPES_WITHOUT_USDC: [&str; 2] = ["cStakingTransfer", "gossipPriorityGasAuction"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MiscEvent {
    Ledger { users: Vec<Address>, delta: LedgerDelta },
    Funding(Vec<FundingDelta>),
    Irrelevant,
    Unknown(String),
}

/// One position's funding settlement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FundingDelta {
    pub(crate) user: Address,
    pub(crate) coin: String,
    /// `funding_amount`, signed for the account, 10^-8 USDC.
    pub(crate) amount_wei: i64,
}

/// Where a `send` takes or puts funds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Balance {
    /// `"spot"`.
    Spot,
    /// A perp dex by name, `""` the main dex.
    Dex(String),
}

impl Balance {
    fn of(name: String) -> Self {
        if name == "spot" { Self::Spot } else { Self::Dex(name) }
    }
}

/// A ledger delta, with what the cash replay reads. Amounts are 10^-8 USDC; `None` where the token is not USDC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LedgerDelta {
    /// `send` from `user` at `source` to `destination` at `destination_balance` (the same account when one user);
    /// the fee is charged at the source.
    Send { user: Address, destination: Address, source: Balance, destination_balance: Balance, usdc_wei: Option<i64>, usdc_fee_wei: i64 },
    /// `spotTransfer`: spot to spot.
    SpotTransfer { user: Address, destination: Address, usdc_wei: Option<i64>, usdc_fee_wei: i64 },
    /// `internalTransfer`: main dex to main dex, the fee on the sender.
    InternalTransfer { user: Address, destination: Address, usdc_wei: i64, fee_wei: i64 },
    /// `subAccountTransfer`: main dex to main dex.
    SubAccountTransfer { user: Address, destination: Address, usdc_wei: i64 },
    /// `accountClassTransfer`: between the account's main dex and its spot.
    ClassTransfer { usdc_wei: i64, to_perp: bool },
    /// `deposit`: into the main dex.
    Deposit { usdc_wei: i64 },
    /// `withdraw`: settles a `withdraw3`, whose `amount` (this `usdc` + `fee`) was debited when it was accepted.
    Withdraw,
    /// `vaultDeposit`: the depositor's main dex to the vault's.
    VaultDeposit { vault: Address, usdc_wei: i64 },
    /// `vaultWithdraw`: the user receives `netWithdrawnUsd`; the vault pays it plus `commission` (the leader's, credited by
    /// its own `vaultLeaderCommission`), not `closingCost`, which its closing fills of the block already carry
    /// (`requestedUsd` = net + commission + closingCost; bm: vault `0xa1b6d8ef…` 2026-10-20, closing cost; vault
    /// `0x1e37a337…` block 1189214090, commission, review 000436 r1).
    VaultWithdraw { user: Address, vault: Address, commission_wei: i64, net_withdrawn_wei: i64 },
    /// `vaultLeaderCommission`: into the leader's main dex.
    VaultLeaderCommission { user: Address, usdc_wei: i64 },
    /// `rewardsClaim`: into spot.
    RewardsClaim { usdc_wei: Option<i64> },
    /// `accountActivationGas`: out of spot.
    AccountActivationGas { usdc_wei: Option<i64> },
    /// `borrowLend`: moves a unified account's spot collateral into or out of lending (direction unverified).
    BorrowLend,
    /// A type that moves no USDC (`TOKEN_TYPES_WITHOUT_USDC` with a token other than USDC).
    WithoutUsdc(String),
    /// A type the replay does not read: `spotGenesis`, `liquidation` (the documented backstop record, never seen),
    /// a `TOKEN_TYPES_WITHOUT_USDC` type moving USDC, and any type not listed here.
    Unread(String),
}

impl LedgerDelta {
    /// The wire `type`.
    pub(crate) fn delta_type(&self) -> &str {
        match self {
            Self::Send { .. } => "send",
            Self::SpotTransfer { .. } => "spotTransfer",
            Self::InternalTransfer { .. } => "internalTransfer",
            Self::SubAccountTransfer { .. } => "subAccountTransfer",
            Self::ClassTransfer { .. } => "accountClassTransfer",
            Self::Deposit { .. } => "deposit",
            Self::Withdraw => "withdraw",
            Self::VaultDeposit { .. } => "vaultDeposit",
            Self::VaultWithdraw { .. } => "vaultWithdraw",
            Self::VaultLeaderCommission { .. } => "vaultLeaderCommission",
            Self::RewardsClaim { .. } => "rewardsClaim",
            Self::AccountActivationGas { .. } => "accountActivationGas",
            Self::BorrowLend => "borrowLend",
            Self::WithoutUsdc(delta_type) | Self::Unread(delta_type) => delta_type,
        }
    }
}

impl<'de> Deserialize<'de> for MiscEvent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Event {
            inner: Inner,
        }
        Ok(Event::deserialize(deserializer)?.inner.0)
    }
}

#[derive(Deserialize)]
struct LedgerUpdate {
    users: Vec<Address>,
    delta: LedgerDelta,
}

/// Every field any read type uses; which are required depends on the type.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireDelta {
    #[serde(rename = "type")]
    delta_type: String,
    user: Option<Address>,
    destination: Option<Address>,
    vault: Option<Address>,
    source_dex: Option<String>,
    destination_dex: Option<String>,
    token: Option<String>,
    amount: Option<String>,
    fee: Option<String>,
    fee_token: Option<String>,
    usdc: Option<String>,
    to_perp: Option<bool>,
    net_withdrawn_usd: Option<String>,
    commission: Option<String>,
}

impl WireDelta {
    fn need<T>(&self, field: Option<T>, name: &str) -> Result<T, String> {
        field.ok_or_else(|| format!("ledger {} without {name}", self.delta_type))
    }

    fn usdc_wei(&self, field: Option<&String>, name: &str) -> Result<i64, String> {
        usdc_wei(self.need(field, name)?).map_err(|err| format!("ledger {} {name}: {err}", self.delta_type))
    }

    /// `amount` when `token` is USDC.
    fn token_amount_wei(&self) -> Result<Option<i64>, String> {
        let token = self.need(self.token.as_ref(), "token")?;
        if token != USDC_NAME {
            return Ok(None);
        }
        self.usdc_wei(self.amount.as_ref(), "amount").map(Some)
    }

    /// `fee` when `feeToken` is USDC, else 0.
    fn usdc_fee_wei(&self) -> Result<i64, String> {
        if self.need(self.fee_token.as_ref(), "feeToken")? != USDC_NAME {
            return Ok(0);
        }
        self.usdc_wei(self.fee.as_ref(), "fee")
    }
}

impl<'de> Deserialize<'de> for LedgerDelta {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = WireDelta::deserialize(deserializer)?;
        Self::try_from(wire).map_err(serde::de::Error::custom)
    }
}

impl TryFrom<WireDelta> for LedgerDelta {
    type Error = String;

    fn try_from(wire: WireDelta) -> Result<Self, String> {
        let user = || wire.need(wire.user, "user");
        let destination = || wire.need(wire.destination, "destination");
        let usdc = || wire.usdc_wei(wire.usdc.as_ref(), "usdc");
        Ok(match wire.delta_type.as_str() {
            "send" => Self::Send {
                user: user()?,
                destination: destination()?,
                source: Balance::of(wire.need(wire.source_dex.clone(), "sourceDex")?),
                destination_balance: Balance::of(wire.need(wire.destination_dex.clone(), "destinationDex")?),
                usdc_wei: wire.token_amount_wei()?,
                usdc_fee_wei: wire.usdc_fee_wei()?,
            },
            "spotTransfer" => Self::SpotTransfer {
                user: user()?,
                destination: destination()?,
                usdc_wei: wire.token_amount_wei()?,
                usdc_fee_wei: wire.usdc_fee_wei()?,
            },
            "internalTransfer" => Self::InternalTransfer {
                user: user()?,
                destination: destination()?,
                usdc_wei: usdc()?,
                fee_wei: wire.usdc_wei(wire.fee.as_ref(), "fee")?,
            },
            "subAccountTransfer" => Self::SubAccountTransfer { user: user()?, destination: destination()?, usdc_wei: usdc()? },
            "accountClassTransfer" => Self::ClassTransfer { usdc_wei: usdc()?, to_perp: wire.need(wire.to_perp, "toPerp")? },
            "deposit" => Self::Deposit { usdc_wei: usdc()? },
            "withdraw" => Self::Withdraw,
            "vaultDeposit" => Self::VaultDeposit { vault: wire.need(wire.vault, "vault")?, usdc_wei: usdc()? },
            "vaultWithdraw" => Self::VaultWithdraw {
                user: user()?,
                vault: wire.need(wire.vault, "vault")?,
                commission_wei: wire.usdc_wei(wire.commission.as_ref(), "commission")?,
                net_withdrawn_wei: wire.usdc_wei(wire.net_withdrawn_usd.as_ref(), "netWithdrawnUsd")?,
            },
            "vaultLeaderCommission" => Self::VaultLeaderCommission { user: user()?, usdc_wei: usdc()? },
            "rewardsClaim" => Self::RewardsClaim { usdc_wei: wire.token_amount_wei()? },
            "accountActivationGas" => Self::AccountActivationGas { usdc_wei: wire.token_amount_wei()? },
            "borrowLend" => Self::BorrowLend,
            known if TOKEN_TYPES_WITHOUT_USDC.contains(&known) => {
                if wire.need(wire.token.as_ref(), "token")? == USDC_NAME {
                    Self::Unread(wire.delta_type)
                } else {
                    Self::WithoutUsdc(wire.delta_type)
                }
            }
            _ => Self::Unread(wire.delta_type),
        })
    }
}

#[derive(Deserialize)]
struct Funding {
    deltas: Vec<FundingDelta>,
}

#[derive(Deserialize)]
struct WireFundingDelta {
    user: Address,
    coin: String,
    funding_amount: String,
}

impl<'de> Deserialize<'de> for FundingDelta {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let WireFundingDelta { user, coin, funding_amount } = WireFundingDelta::deserialize(deserializer)?;
        let amount_wei =
            usdc_wei(&funding_amount).map_err(|err| serde::de::Error::custom(format!("funding {user} {coin}: {err}")))?;
        Ok(Self { user, coin, amount_wei })
    }
}

/// The single-key `inner` object.
struct Inner(MiscEvent);

impl<'de> Deserialize<'de> for Inner {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(InnerVisitor)
    }
}

struct InnerVisitor;

impl<'de> Visitor<'de> for InnerVisitor {
    type Value = Inner;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a misc event `inner` object with exactly one kind")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Inner, A::Error> {
        use serde::de::Error as _;
        let kind: String = map.next_key()?.ok_or_else(|| A::Error::custom("empty misc event `inner`"))?;
        let event = match kind.as_str() {
            "LedgerUpdate" => {
                let LedgerUpdate { users, delta } = map.next_value()?;
                MiscEvent::Ledger { users, delta }
            }
            "Funding" => MiscEvent::Funding(map.next_value::<Funding>()?.deltas),
            known if IRRELEVANT_KINDS.contains(&known) => {
                map.next_value::<IgnoredAny>()?;
                MiscEvent::Irrelevant
            }
            _ => {
                map.next_value::<IgnoredAny>()?;
                MiscEvent::Unknown(kind)
            }
        };
        if let Some(extra) = map.next_key::<String>()? {
            return Err(A::Error::custom(format!("misc event `inner` has a second kind {extra:?}")));
        }
        Ok(Inner(event))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::node_data::Batch;

    const USER_A: &str = "0x5b51db9432bc4dd2c30cadc4ab6ab44565419c9a";
    const USER_B: &str = "0x0000000000005ef5a16bd687b9b9c41b498435db";

    fn line(inner: &str) -> String {
        format!(
            r#"{{"local_time":"2026-10-19T06:00:00.100","block_time":"2026-10-19T06:00:00.028","block_number":1189198872,"events":[{{"time":"2026-10-19T06:00:00.028881520","hash":"0x00","inner":{inner}}}]}}"#
        )
    }

    fn parse(inner: &str) -> Result<MiscEvent, String> {
        let batch: Batch<MiscEvent> = sonic_rs::from_str(&line(inner)).map_err(|err| err.to_string())?;
        assert_eq!(batch.block_number(), 1_189_198_872);
        Ok(batch.events().remove(0))
    }

    fn address(text: &str) -> Address {
        text.parse().unwrap()
    }

    fn ledger(users: &[&str], delta: &str) -> String {
        let users: Vec<String> = users.iter().map(|user| format!("{user:?}")).collect();
        format!(r#"{{"LedgerUpdate":{{"users":[{}],"delta":{delta}}}}}"#, users.join(","))
    }

    fn delta(users: &[&str], delta: &str) -> Result<LedgerDelta, String> {
        match parse(&ledger(users, delta))? {
            MiscEvent::Ledger { delta, .. } => Ok(delta),
            other => Err(format!("not a ledger update: {other:?}")),
        }
    }

    #[test]
    fn reads_funding_amounts_per_account() {
        let funding = format!(
            r#"{{"Funding":{{"deltas":[{{"user":"{USER_B}","coin":"HYPE","funding_amount":"-1.311558","szi":"-2159.89","funding_rate":"0.0000076023"}}]}}}}"#
        );
        let expected = FundingDelta { user: address(USER_B), coin: "HYPE".to_string(), amount_wei: -131_155_800 };
        assert_eq!(parse(&funding), Ok(MiscEvent::Funding(vec![expected])));
        assert_eq!(parse(r#"{"Funding":{"deltas":[]}}"#), Ok(MiscEvent::Funding(Vec::new())));
        let broken = format!(r#"{{"Funding":{{"deltas":[{{"user":"{USER_B}","coin":"HYPE","funding_amount":"1.0000000001"}}]}}}}"#);
        assert!(parse(&broken).unwrap_err().contains("funding"), "an amount finer than 10^-8");
    }

    /// Real deltas of 2026-10-20 06 UTC (bm `misc_events_streaming`), one per type and fee pattern.
    #[test]
    fn reads_the_measured_ledger_deltas() {
        let (a, b) = ("0x82cd5683ef2013c30915cd84c940f378ad782d4b", "0x997a6967d9ff936453f843c17db6aef887a722a8");
        let send_with_fee = r#"{"type":"send","user":"0x82cd5683ef2013c30915cd84c940f378ad782d4b","destination":"0x997a6967d9ff936453f843c17db6aef887a722a8","sourceDex":"","destinationDex":"","token":"USDC","amount":"1568.627451","usdcValue":"1568.627451","fee":"1.0","nativeTokenFee":"0.0","nonce":1792476044972,"feeToken":"USDC"}"#;
        let expected = LedgerDelta::Send {
            user: address(a),
            destination: address(b),
            source: Balance::Dex(String::new()),
            destination_balance: Balance::Dex(String::new()),
            usdc_wei: Some(156_862_745_100),
            usdc_fee_wei: 100_000_000,
        };
        assert_eq!(delta(&[a, b], send_with_fee), Ok(expected));
        let usol_from_spot = r#"{"type":"send","user":"0xbd334a55db91617545e8d1b09941e036ad8e4200","destination":"0xce554c3132584352f8c202770c8b66d709d0472e","sourceDex":"spot","destinationDex":"spot","token":"USOL","amount":"720.78340032","usdcValue":"75170.500819","fee":"1.0","nativeTokenFee":"0.0","nonce":1792476964919,"feeToken":"USDC"}"#;
        let LedgerDelta::Send { source, usdc_wei, usdc_fee_wei, .. } = delta(&[a, b], usol_from_spot).unwrap() else { panic!() };
        assert_eq!((source, usdc_wei, usdc_fee_wei), (Balance::Spot, None, 100_000_000), "a USDC fee on another token");
        let hfun = r#"{"type":"spotTransfer","token":"HFUN","amount":"1.70013073","usdcValue":"19.495399","user":"0x2000000000000000000000000000000000000002","destination":"0x5a69a60ca8a4d189855b33bd9f1bbe71095c1039","fee":"0.0","nativeTokenFee":"0.0","nonce":4334765,"feeToken":""}"#;
        let LedgerDelta::SpotTransfer { usdc_wei, usdc_fee_wei, .. } = delta(&[a, b], hfun).unwrap() else { panic!() };
        assert_eq!((usdc_wei, usdc_fee_wei), (None, 0), "an empty feeToken");
        let sub = r#"{"type":"subAccountTransfer","usdc":"114.0","user":"0x12b7485201d198e54d814e0d409d3be3f2116fe4","destination":"0xb23c4c38c66559de0db64c897c0746f254622d39"}"#;
        let LedgerDelta::SubAccountTransfer { user, usdc_wei, .. } = delta(&[a, b], sub).unwrap() else { panic!() };
        assert_eq!((user, usdc_wei), (address("0x12b7485201d198e54d814e0d409d3be3f2116fe4"), 11_400_000_000), "roles from the delta");
        let cases = [
            (r#"{"type":"internalTransfer","usdc":"9.1","user":"0x1e9e9f0ca0052509b79fcb14f06014baa2bedf36","destination":"0x27d5ae66a3ade10e42c5fcb3075b33e878bf0f49","fee":"1.0"}"#, "internalTransfer"),
            (r#"{"type":"accountClassTransfer","usdc":"0.02242","toPerp":true}"#, "accountClassTransfer"),
            (r#"{"type":"deposit","usdc":"29.86109"}"#, "deposit"),
            (r#"{"type":"withdraw","usdc":"49.67","nonce":1792475904755000,"fee":"1.0"}"#, "withdraw"),
            (r#"{"type":"vaultDeposit","vault":"0xdfc24b077bc1425ad1dea75bcb6f8158e10df303","usdc":"1996.0"}"#, "vaultDeposit"),
            (r#"{"type":"vaultWithdraw","vault":"0x654016a8c9fcf0c4cb7ed6078aba21f7f399f7b7","user":"0xa5f189f99499c9d7aefc1ae9fdc24e5d2667f9ac","requestedUsd":"23147.480408","commission":"0.0","closingCost":"0.007452","basis":"24089.167369","netWithdrawnUsd":"23147.472956"}"#, "vaultWithdraw"),
            (r#"{"type":"vaultLeaderCommission","user":"0xc8e006b05694165e5459d5ac297c6857f97d8f23","usdc":"0.201984"}"#, "vaultLeaderCommission"),
            (r#"{"type":"rewardsClaim","amount":"3.045825","token":"USDC"}"#, "rewardsClaim"),
            (r#"{"type":"accountActivationGas","amount":"1.0","token":"USDC"}"#, "accountActivationGas"),
            (r#"{"type":"borrowLend","token":"USDC","operation":"supply","amount":"10000.0","interestAmount":"0.0"}"#, "borrowLend"),
            (r#"{"type":"cStakingTransfer","token":"HYPE","amount":"2.85817418","isDeposit":true}"#, "cStakingTransfer"),
            (r#"{"type":"gossipPriorityGasAuction","token":"HYPE","amount":"1.0410909"}"#, "gossipPriorityGasAuction"),
        ];
        for (wire, delta_type) in cases {
            let parsed = delta(&[a], wire).unwrap_or_else(|err| panic!("{delta_type}: {err}"));
            assert_eq!(parsed.delta_type(), delta_type);
            assert!(!matches!(parsed, LedgerDelta::Unread(_)), "{delta_type} is read");
        }
        let withdraw = LedgerDelta::VaultWithdraw {
            user: address("0xa5f189f99499c9d7aefc1ae9fdc24e5d2667f9ac"),
            vault: address("0x654016a8c9fcf0c4cb7ed6078aba21f7f399f7b7"),
            commission_wei: 0,
            net_withdrawn_wei: 2_314_747_295_600,
        };
        assert_eq!(delta(&[a, b], cases[5].0), Ok(withdraw));
    }

    #[test]
    fn types_without_usdc_and_unread_types_are_kept_by_name() {
        let hype_gas = r#"{"type":"gossipPriorityGasAuction","token":"HYPE","amount":"1.0"}"#;
        assert_eq!(delta(&[USER_A], hype_gas), Ok(LedgerDelta::WithoutUsdc("gossipPriorityGasAuction".to_string())));
        let usdc_gas = r#"{"type":"gossipPriorityGasAuction","token":"USDC","amount":"1.0"}"#;
        assert_eq!(delta(&[USER_A], usdc_gas), Ok(LedgerDelta::Unread("gossipPriorityGasAuction".to_string())));
        let liquidation = r#"{"type":"liquidation","anything":[1]}"#;
        assert_eq!(delta(&[USER_A], liquidation), Ok(LedgerDelta::Unread("liquidation".to_string())));
    }

    #[test]
    fn a_read_type_without_a_field_it_needs_fails() {
        for (wire, missing) in [
            (r#"{"type":"send","user":"0x5b51db9432bc4dd2c30cadc4ab6ab44565419c9a","destination":"0x5b51db9432bc4dd2c30cadc4ab6ab44565419c9a","destinationDex":"","token":"USDC","amount":"1.0","fee":"0.0","feeToken":""}"#, "sourceDex"),
            (r#"{"type":"internalTransfer","usdc":"9.1","user":"0x1e9e9f0ca0052509b79fcb14f06014baa2bedf36","destination":"0x27d5ae66a3ade10e42c5fcb3075b33e878bf0f49"}"#, "fee"),
            (r#"{"type":"accountClassTransfer","usdc":"1.0"}"#, "toPerp"),
            (r#"{"type":"rewardsClaim","amount":"1.0"}"#, "token"),
        ] {
            let err = delta(&[USER_A], wire).unwrap_err();
            assert!(err.contains(&format!("without {missing}")), "{err}");
        }
        let finer = r#"{"type":"deposit","usdc":"1.000000001"}"#;
        assert!(delta(&[USER_A], finer).unwrap_err().contains("deposit usdc"));
    }

    #[test]
    fn separates_irrelevant_from_unknown_kinds() {
        let deposit = format!(r#"{{"CDeposit":{{"user":"{USER_A}","amount":"6.24643001"}}}}"#);
        assert_eq!(parse(&deposit), Ok(MiscEvent::Irrelevant));
        assert_eq!(parse(r#"{"BrandNewKind":{"anything":[1,2]}}"#), Ok(MiscEvent::Unknown("BrandNewKind".to_string())));
    }

    #[test]
    fn a_known_kind_with_a_broken_body_fails() {
        assert!(parse(r#"{"LedgerUpdate":{"delta":{"type":"deposit","usdc":"1.0"}}}"#).is_err(), "missing users");
        assert!(parse(&format!(r#"{{"LedgerUpdate":{{"users":["{USER_A}"],"delta":{{}}}}}}"#)).is_err(), "missing type");
        assert!(parse(r#"{"Funding":{"deltas":[{"coin":"BTC"}]}}"#).is_err(), "delta without user");
        assert!(parse(r#"{"LedgerUpdate":{"users":["not an address"],"delta":{"type":"deposit","usdc":"1.0"}}}"#).is_err());
        assert!(parse("{}").is_err(), "no kind");
        assert!(parse(r#"{"CDeposit":{},"Funding":{"deltas":[]}}"#).is_err(), "two kinds");
    }
}
