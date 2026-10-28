//! HIP-3 US stocks' local marks (design 000535 r1–r2): the mark price doc's three terms
//! (docs.trade.xyz/perpetuals/mechanics/mark-price) - (1) the relayer's oracle, (2) the oracle plus a 150 s EMA of the
//! perp mid over the oracle, (3) median(best bid, best ask, last trade) - with (1) predicted by the stock's SIP NBBO
//! midpoint (Alpaca's stream), (2)'s EMA computed here from the HL book and the stored oracle, and (3) from the HL book
//! now. The relayer publishes (1) and (2); HL computes (3) and stores the median (bm 10-27: 95.9–99.9% of updates over
//! 20 stocks equal median(published (1), published (2), (3) at the block)). v2 (design 000537): (1) adds the offset the
//! relayer's aggregate keeps from the SIP midpoint, carried by the share each coin's offsets persist (`carry`).

pub(crate) mod carry;
pub(crate) mod compute;
pub(crate) mod ema;
pub(crate) mod protocol;
pub(crate) mod session;
pub(crate) mod snapshot;
pub(crate) mod sources;
pub(crate) mod view;
