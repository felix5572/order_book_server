//! The coins of `extHip3ExternalQuotes` (hyperliquid-trade design 000669 r1 1.1): the union, by full coin name, of the
//! two embedded source tables - Binance's `TRADIFI_PERPETUAL` contracts (`external_sources`) and the US stocks' SIP
//! symbols (`sources`) -, whichever feeds the server runs. A coin in neither is outside the channel, the one refusal a
//! client may keep; a coin in one table only has the other one's leg `unlisted`. The WS subscribe and unsubscribe and
//! `GET /extHip3ExternalQuotes` judge a coin by this one table.

use rustc_hash::{FxHashMap, FxHashSet};

use super::{external_sources::Hip3ExternalSources, sources::Hip3SipSources};
use crate::local_marks::price::VenuePrice;

/// A coin's Binance contract: its place among the external feed's markets, and its multiplier (HL price = multiplier
/// × Binance price).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BinanceListing {
    pub(crate) market: usize,
    pub(crate) multiplier: VenuePrice,
}

/// The channel's coins in name order, each with its Binance contract and whether the SIP table lists it.
#[derive(Debug, Default)]
pub(crate) struct Hip3ExternalCoins {
    names: Vec<String>,
    by_name: FxHashMap<String, usize>,
    binance: Vec<Option<BinanceListing>>,
    sip_listed: Vec<bool>,
}

impl Hip3ExternalCoins {
    pub(crate) fn new(binance: &Hip3ExternalSources, sip: &Hip3SipSources) -> Self {
        let mut names: Vec<String> = binance.coins().chain(sip.coins()).map(str::to_string).collect();
        names.sort_unstable();
        names.dedup();
        let by_name = names.iter().enumerate().map(|(index, coin)| (coin.clone(), index)).collect();
        let listings: FxHashMap<&str, BinanceListing> = binance.markets.coins.iter().enumerate()
            .map(|(market, (coin, multiplier))| (coin.as_str(), BinanceListing { market, multiplier: *multiplier }))
            .collect();
        let sip_coins: FxHashSet<&str> = sip.coins().collect();
        let binance = names.iter().map(|coin| listings.get(coin.as_str()).copied()).collect();
        let sip_listed = names.iter().map(|coin| sip_coins.contains(coin.as_str())).collect();
        Self { names, by_name, binance, sip_listed }
    }

    pub(crate) fn names(&self) -> &[String] {
        &self.names
    }

    /// The coin's place in the channel; `None` outside it.
    pub(crate) fn index(&self, coin: &str) -> Option<usize> {
        self.by_name.get(coin).copied()
    }

    /// `None`: Binance's table lists no contract of the coin.
    pub(crate) fn binance(&self, index: usize) -> Option<BinanceListing> {
        self.binance[index]
    }

    pub(crate) fn sip_listed(&self, index: usize) -> bool {
        self.sip_listed[index]
    }

    /// How many Binance markets the coins list: the external feed's.
    pub(crate) fn binance_markets(&self) -> usize {
        self.binance.iter().flatten().count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Design 000669 r1 1.1: the union by full coin name in name order; GOLD Binance's only, BB the SIP's only, TSLA in
    /// both, each Binance coin with its own market and multiplier; KIOXIA in neither.
    #[test]
    fn the_coins_are_the_union_of_both_tables() {
        let binance = Hip3ExternalSources::parse(r#"{"venue": "binance_perp", "coins": {"xyz:GOLD": {"symbol": "XAUUSDT", "pxMultiplier": "1"},
            "xyz:TSLA": {"symbol": "TSLAUSDT", "pxMultiplier": "10"}}}"#).unwrap();
        let sip = Hip3SipSources::parse(r#"{"feed": "sip", "coins": {"xyz:TSLA": {"symbol": "TSLA"}, "xyz:BB": {"symbol": "BB"}}}"#).unwrap();
        let coins = Hip3ExternalCoins::new(&binance, &sip);
        assert_eq!(coins.names(), ["xyz:BB", "xyz:GOLD", "xyz:TSLA"]);
        let legs = |coin: &str| coins.index(coin).map(|index| (coins.binance(index).map(|listing| listing.market), coins.sip_listed(index)));
        assert_eq!((legs("xyz:BB"), legs("xyz:GOLD"), legs("xyz:TSLA"), legs("xyz:KIOXIA")), (Some((None, true)), Some((Some(0), false)), Some((Some(1), true)), None));
        assert_eq!(coins.binance(2).unwrap().multiplier, VenuePrice::parse("10").unwrap());
    }

    /// The embedded tables of 11-02 (design 000669 "已核对的事实"): 92 Binance and 85 SIP coins, 77 in both, 100 in
    /// all; the 8 SIP-only coins are in, the commodities Binance's only.
    #[test]
    fn the_embedded_tables_give_100_coins() {
        let coins = Hip3ExternalCoins::new(&Hip3ExternalSources::embedded().unwrap(), &Hip3SipSources::embedded().unwrap());
        assert_eq!(coins.names().len(), 100);
        let both = (0..coins.names().len()).filter(|index| coins.binance(*index).is_some() && coins.sip_listed(*index)).count();
        assert_eq!(both, 77);
        for coin in ["xyz:BB", "xyz:BIRD", "xyz:CVX", "xyz:MAGS", "xyz:NCLD", "xyz:QNT", "xyz:TLT", "xyz:UMC"] {
            let index = coins.index(coin).unwrap();
            assert_eq!((coins.binance(index), coins.sip_listed(index)), (None, true), "{coin}");
        }
        let gold = coins.index("xyz:GOLD").unwrap();
        assert!(coins.binance(gold).is_some() && !coins.sip_listed(gold));
        assert_eq!(coins.index("xyz:KIOXIA"), None);
    }
}
