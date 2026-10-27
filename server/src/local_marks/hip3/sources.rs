//! The HIP-3 US stocks priced from the SIP (design 000535 r2 revision 7): `server/data/hip3_sip_sources.json`, generated
//! by `scripts/discover_hip3_sip_sources.py` from Alpaca's latest SIP quotes, each checked against the coin's HL oracle,
//! and embedded at build time: a newly listed coin or a delisted stock needs a regeneration and a rebuild.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::local_marks::{price::VenuePrice, sources::FeedMarkets};

const EMBEDDED: &str = include_str!("../../../data/hip3_sip_sources.json");
/// The only feed the file may name.
const SIP: &str = "sip";

#[derive(Deserialize)]
struct SourcesFile {
    feed: String,
    coins: BTreeMap<String, Entry>,
}

#[derive(Deserialize)]
struct Entry {
    symbol: String,
}

/// The SIP feed's markets: one per coin, in coin order, each at multiplier 1 (a share's SIP price is the HL price).
#[derive(Debug)]
pub(crate) struct Hip3SipSources {
    pub(crate) markets: FeedMarkets,
}

impl Hip3SipSources {
    pub(crate) fn embedded() -> Result<Self, String> {
        Self::parse(EMBEDDED)
    }

    /// A coin is `dex:NAME`, its symbol non-empty and listed once; the file lists at least one.
    pub(crate) fn parse(text: &str) -> Result<Self, String> {
        let file: SourcesFile = serde_json::from_str(text).map_err(|err| format!("HIP-3 SIP sources: {err}"))?;
        if file.feed != SIP {
            return Err(format!("HIP-3 SIP sources: feed {:?}, not {SIP:?}", file.feed));
        }
        let one = VenuePrice::parse("1").unwrap_or_else(|err| unreachable!("1 is a price: {err}"));
        let mut markets = FeedMarkets::default();
        for (coin, entry) in file.coins {
            if !coin.split_once(':').is_some_and(|(dex, name)| !dex.is_empty() && !name.is_empty()) || entry.symbol.is_empty() {
                return Err(format!("HIP-3 SIP sources: invalid listing {coin:?} -> {:?}", entry.symbol));
            }
            markets.push(entry.symbol.clone(), coin.clone(), one).map_err(|previous| {
                format!("HIP-3 SIP sources: symbol {} listed for {previous} and {coin}", entry.symbol)
            })?;
        }
        if markets.symbols.is_empty() {
            return Err("HIP-3 SIP sources list no coin".to_string());
        }
        Ok(Self { markets })
    }

    /// The coins, in the markets' order.
    pub(crate) fn coins(&self) -> impl Iterator<Item = &str> {
        self.markets.coins.iter().map(|(coin, _)| coin.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_sources_are_checked_when_loaded() {
        let sources = Hip3SipSources::parse(r#"{"feed": "sip", "coins": {"xyz:TSLA": {"symbol": "TSLA"}, "xyz:AAPL": {"symbol": "AAPL"}}}"#).unwrap();
        assert_eq!(sources.coins().collect::<Vec<_>>(), ["xyz:AAPL", "xyz:TSLA"]);
        assert_eq!(sources.markets.index("TSLA"), Some(1));
        let cases = [
            (r#"{"feed": "boats", "coins": {"xyz:TSLA": {"symbol": "TSLA"}}}"#, "not \"sip\""),
            (r#"{"feed": "sip", "coins": {"TSLA": {"symbol": "TSLA"}}}"#, "invalid listing"),
            (r#"{"feed": "sip", "coins": {"xyz:TSLA": {"symbol": ""}}}"#, "invalid listing"),
            (r#"{"feed": "sip", "coins": {"xyz:TSLA": {"symbol": "TSLA"}, "abc:TSLA": {"symbol": "TSLA"}}}"#, "listed for abc:TSLA and xyz:TSLA"),
            (r#"{"feed": "sip", "coins": {}}"#, "list no coin"),
        ];
        for (text, message) in cases {
            let error = Hip3SipSources::parse(text).unwrap_err();
            assert!(error.contains(message), "{error}");
        }
    }

    /// The discovery run of 10-27 19:38Z: 85 of xyz's 119 perps; the four whose ticker is another US listing are out.
    #[test]
    fn the_embedded_sources_list_the_us_stocks() {
        let sources = Hip3SipSources::embedded().unwrap();
        let coins: Vec<&str> = sources.coins().collect();
        assert_eq!(coins.len(), 85);
        assert!(["xyz:TSLA", "xyz:NVDA", "xyz:AAPL", "xyz:SMH", "xyz:BABA"].iter().all(|coin| coins.contains(coin)));
        assert!(["xyz:CL", "xyz:GOLD", "xyz:SP500", "xyz:SMSN"].iter().all(|coin| !coins.contains(coin)), "commodities, indices, Korean shares");
    }
}
