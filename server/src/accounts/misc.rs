//! `misc_events_streaming` lines: `Batch<MiscEvent>`, one event per line, lines only for blocks that have events.
//!
//! Each event is `{"time", "hash", "inner": {<Kind>: {...}}}`. Only the accounts an event touches are read, and only
//! for the two kinds that move margin: `LedgerUpdate` (`users`: every account of the transfer) and `Funding` (one
//! line per dex, `deltas[].user`; may be empty). A ledger update also keeps its `delta.type`, for the evidence of rare
//! types. The other kinds seen on 2026-10-19 (staking, validator rewards, sub-account creation, gossip auction) are
//! skipped; a kind outside both lists is reported as unknown.

use std::fmt;

use alloy::primitives::Address;
use serde::{
    Deserialize, Deserializer,
    de::{IgnoredAny, MapAccess, Visitor},
};

/// Kinds observed that never move perp margin of a USDC pool.
const IRRELEVANT_KINDS: [&str; 6] =
    ["CDeposit", "CWithdrawal", "Delegation", "CreateSubAccount", "ValidatorRewards", "GossipPriorityAuctionRestart"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MiscEvent {
    Ledger { users: Vec<Address>, delta_type: String },
    Funding(Vec<Address>),
    Irrelevant,
    Unknown(String),
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

#[derive(Deserialize)]
struct LedgerDelta {
    #[serde(rename = "type")]
    delta_type: String,
}

#[derive(Deserialize)]
struct Funding {
    deltas: Vec<FundingDelta>,
}

#[derive(Deserialize)]
struct FundingDelta {
    user: Address,
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
                MiscEvent::Ledger { users, delta_type: delta.delta_type }
            }
            "Funding" => MiscEvent::Funding(map.next_value::<Funding>()?.deltas.into_iter().map(|delta| delta.user).collect()),
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

    #[test]
    fn reads_ledger_and_funding_accounts() {
        let ledger = format!(
            r#"{{"LedgerUpdate":{{"users":["{USER_A}","{USER_B}"],"delta":{{"type":"send","usdc":"1.0","extra":[1]}}}}}}"#
        );
        let users = vec![address(USER_A), address(USER_B)];
        assert_eq!(parse(&ledger), Ok(MiscEvent::Ledger { users, delta_type: "send".to_string() }));
        let funding = format!(
            r#"{{"Funding":{{"deltas":[{{"user":"{USER_B}","coin":"HYPE","funding_amount":"1.311558","szi":"-2159.89","funding_rate":"0.0000076023"}}]}}}}"#
        );
        assert_eq!(parse(&funding), Ok(MiscEvent::Funding(vec![address(USER_B)])));
        assert_eq!(parse(r#"{"Funding":{"deltas":[]}}"#), Ok(MiscEvent::Funding(Vec::new())));
    }

    #[test]
    fn separates_irrelevant_from_unknown_kinds() {
        let deposit = format!(r#"{{"CDeposit":{{"user":"{USER_A}","amount":"6.24643001"}}}}"#);
        assert_eq!(parse(&deposit), Ok(MiscEvent::Irrelevant));
        assert_eq!(parse(r#"{"BrandNewKind":{"anything":[1,2]}}"#), Ok(MiscEvent::Unknown("BrandNewKind".to_string())));
    }

    #[test]
    fn a_known_kind_with_a_broken_body_fails() {
        assert!(parse(r#"{"LedgerUpdate":{"delta":{"type":"send"}}}"#).is_err(), "missing users");
        assert!(parse(&format!(r#"{{"LedgerUpdate":{{"users":["{USER_A}"],"delta":{{}}}}}}"#)).is_err(), "missing type");
        assert!(parse(r#"{"Funding":{"deltas":[{"coin":"BTC"}]}}"#).is_err(), "delta without user");
        assert!(parse(r#"{"LedgerUpdate":{"users":["not an address"],"delta":{"type":"send"}}}"#).is_err());
        assert!(parse("{}").is_err(), "no kind");
        assert!(parse(r#"{"CDeposit":{},"Funding":{"deltas":[]}}"#).is_err(), "two kinds");
    }
}
