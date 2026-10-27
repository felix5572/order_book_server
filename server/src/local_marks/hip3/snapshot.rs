//! The HIP-3 coins' legs at one tick (design 000535 r1 §3, r2 revisions 2–4): the node leg is the stream's stored
//! prices, the local leg `compute::hip3_local_mark` on them, the HL book and the SIP quote now. The same progress clocks
//! as the main dex's gate both legs; then, for the local leg, the observation gates in order: the SIP's candidate
//! hours, the relayer on its external price (external == oracle), a known last trade, a valid and recent SIP quote, a
//! warm EMA.

use std::time::Duration;

use rustc_hash::FxHashMap;

use super::{
    compute::{Hip3Inputs, Hip3LocalMark, hip3_local_mark},
    ema::{Hip3Ema, WARMUP},
    session::sip_candidate_hours,
    sources::Hip3SipSources,
};
use crate::{
    accounts::marks::seed::Hip3Coin,
    local_marks::{
        slot::{Quote, Validity},
        snapshot::{Tick, Withheld, book_units},
        subscriptions::FeedProjection,
    },
};

/// A SIP quote whose SIP time is older than this no longer counts (design 000535 r1 §3, starting value): after the
/// close the last NBBO stands still.
const MAX_SIP_QUIET_MS: u64 = 60_000;

/// The node leg: the coin's stored prices, in its price units.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Hip3NodeMark {
    pub(crate) size_decimals: u32,
    pub(crate) mark: u64,
    pub(crate) mark_time_ns: u64,
    pub(crate) stored_oracle: u64,
    pub(crate) external: Option<u64>,
    pub(crate) last_trade: Option<u64>,
}

/// The local leg: the terms, the inputs they took, and the EMA's sampled time.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Hip3LocalLeg {
    pub(crate) size_decimals: u32,
    pub(crate) bid: u64,
    pub(crate) ask: u64,
    pub(crate) sip: Quote,
    pub(crate) sip_event_ms: Option<u64>,
    pub(crate) sip_received_ms: u64,
    /// (2) - (1) of the EMA, price units.
    pub(crate) gap: f64,
    pub(crate) ema_sampled: Duration,
    pub(crate) mark: Hip3LocalMark,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Hip3CoinMarks {
    pub(crate) node: Result<Hip3NodeMark, Withheld>,
    pub(crate) local: Result<Hip3LocalLeg, Withheld>,
    /// The SIP quote's validity, whatever the legs.
    pub(crate) sip: Validity,
}

/// The HIP-3 coins of the SIP sources, in their order (each coin's market index is its own).
#[derive(Debug, Default)]
pub(crate) struct Hip3Table {
    names: Vec<String>,
    by_name: FxHashMap<String, usize>,
}

impl Hip3Table {
    pub(crate) fn new(sources: Option<&Hip3SipSources>) -> Self {
        let names: Vec<String> = sources.into_iter().flat_map(Hip3SipSources::coins).map(str::to_string).collect();
        let by_name = names.iter().enumerate().map(|(index, coin)| (coin.clone(), index)).collect();
        Self { names, by_name }
    }

    pub(crate) fn names(&self) -> &[String] {
        &self.names
    }

    pub(crate) fn index(&self, coin: &str) -> Option<usize> {
        self.by_name.get(coin).copied()
    }
}

/// What gates every HIP-3 leg at this tick, as the main dex's: the node prices and the book.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Hip3Gates<'a> {
    pub(crate) node: Option<&'a Withheld>,
    pub(crate) book: Option<&'a Withheld>,
    /// The feeds stopped or the account owner is gone.
    pub(crate) stopped: Option<&'a Withheld>,
}

/// Every coin's legs; a tick whose node prices and book are trusted samples each coin's EMA first.
pub(crate) fn hip3_marks(table: &Hip3Table, tick: &Tick<'_>, book_offset: usize, gates: Hip3Gates<'_>, emas: &mut [Hip3Ema]) -> Vec<Hip3CoinMarks> {
    assert_eq!(emas.len(), table.names.len(), "one EMA per HIP-3 coin");
    let sip = tick.sip;
    table
        .names
        .iter()
        .enumerate()
        .map(|(index, coin)| {
            let validity = sip.map_or(Validity::NotConnected, |reading| reading.entries[index].1);
            let node = node_leg(coin, tick, gates.node);
            let book = node.as_ref().ok().and_then(|node| book_prices(coin, tick, book_offset + index, node.size_decimals).transpose()).transpose();
            if let (Ok(node), Ok(Some((bid, ask))), None) = (&node, &book, gates.book) {
                #[expect(clippy::cast_precision_loss, reason = "prices are below 2^53")]
                let x = (bid + ask) as f64 / 2.0 - node.stored_oracle as f64;
                emas[index].sample(tick.instant, x);
            }
            let local = match (gates.stopped.or(gates.node).or(gates.book), &node, book) {
                (Some(withheld), _, _) | (None, Err(withheld), _) => Err(withheld.clone()),
                (None, Ok(_), Err(withheld)) => Err(withheld),
                (None, Ok(_), Ok(None)) => Err(Withheld::unavailable("hlBook", format!("the HL book of {coin} has an empty side"))),
                (None, Ok(node), Ok(Some((bid, ask)))) => local_leg(coin, tick, sip, index, node, (bid, ask), &emas[index]),
            };
            Hip3CoinMarks { node, local, sip: validity }
        })
        .collect()
}

fn node_leg(coin: &str, tick: &Tick<'_>, withheld: Option<&Withheld>) -> Result<Hip3NodeMark, Withheld> {
    if let Some(withheld) = withheld {
        return Err(withheld.clone());
    }
    let prices = tick.node.as_ref().unwrap_or_else(|_| unreachable!("unavailable node prices are withheld"));
    let state: &Hip3Coin = prices.hip3.get(coin).ok_or_else(|| Withheld::unavailable("nodeCoin", format!("the node state lists no {coin}")))?;
    let mark = state.mark.ok_or_else(|| Withheld::unavailable("nodeMark", format!("{coin} has no stored mark yet")))?;
    let stored_oracle = state.oracle.ok_or_else(|| Withheld::unavailable("nodeOracle", format!("{coin} has no stored oracle yet")))?;
    Ok(Hip3NodeMark { size_decimals: state.size_decimals, mark: mark.px, mark_time_ns: mark.time_ns, stored_oracle, external: state.external, last_trade: state.last_trade })
}

/// The coin's best bid and ask in its price units; `None` with an empty side.
fn book_prices(coin: &str, tick: &Tick<'_>, book_index: usize, size_decimals: u32) -> Result<Option<(u64, u64)>, Withheld> {
    let Some(book) = tick.book else { return Ok(None) };
    let (Some(bid), Some(ask)) = book.prices[book_index] else { return Ok(None) };
    let units = |px| book_units(px, size_decimals).map_err(|withheld| Withheld { detail: format!("{coin}: {}", withheld.detail), ..withheld });
    Ok(Some((units(bid)?, units(ask)?)))
}

fn local_leg(coin: &str, tick: &Tick<'_>, sip: Option<&FeedProjection>, index: usize, node: &Hip3NodeMark, (bid, ask): (u64, u64), ema: &Hip3Ema) -> Result<Hip3LocalLeg, Withheld> {
    if !sip_candidate_hours(tick.now_ms) {
        return Err(Withheld::unavailable("sipSession", "outside the SIP's candidate hours (Monday to Friday, 04:00–20:00 New York)"));
    }
    let external = node.external.ok_or_else(|| Withheld::unavailable("externalUnknown", format!("{coin} has no stored external price yet")))?;
    if external != node.stored_oracle {
        let detail = format!("{coin}'s stored external {external} differs from its oracle {}: the relayer prices internally", node.stored_oracle);
        return Err(Withheld::unavailable("externalClosed", detail));
    }
    let last_trade = node.last_trade.ok_or_else(|| Withheld::unavailable("lastTradeUnknown", format!("{coin} has no last trade since the state's checkpoint")))?;
    let reading = sip.ok_or_else(|| Withheld::unavailable("sipQuote", "the server runs without the SIP feed"))?;
    let (entry, validity) = reading.entries[index];
    let entry = entry.filter(|_| validity == Validity::Valid).ok_or_else(|| Withheld::unavailable("sipQuote", format!("the SIP quote of {coin} is {validity:?}")))?;
    let quote = entry.quote.unwrap_or_else(|| unreachable!("a valid entry holds a quote"));
    // the SIP parser requires the quote's time; never the receive time in its place (review 000536 P2-2)
    let quoted_ms = entry.event_ms.ok_or_else(|| Withheld::unavailable("sipQuote", format!("the SIP quote of {coin} carries no SIP time")))?;
    let age_ms = tick.now_ms.saturating_sub(quoted_ms);
    if age_ms > MAX_SIP_QUIET_MS {
        return Err(Withheld::stale("sipQuiet", format!("the SIP quote of {coin} is {age_ms} ms old")));
    }
    let (gap, ema_sampled) = ema.gap().map_err(|sampled| {
        Withheld::unavailable("emaWarmup", format!("the EMA of {coin} has sampled {} s of {} s", sampled.as_secs(), WARMUP.as_secs()))
    })?;
    let inputs = Hip3Inputs { size_decimals: node.size_decimals, stored_oracle: node.stored_oracle, sip_bid: quote.bid, sip_ask: quote.ask, gap, bid, ask, last_trade };
    let mark = hip3_local_mark(inputs).map_err(|err| Withheld::unavailable("calculation", format!("{coin}: {err}")))?;
    Ok(Hip3LocalLeg { size_decimals: node.size_decimals, bid, ask, sip: quote, sip_event_ms: entry.event_ms, sip_received_ms: entry.received_ms, gap, ema_sampled, mark })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        accounts::{
            marks::{NativePriceSnapshot, seed::StoredMark},
            publish::NodePrices,
        },
        listeners::order_book::BestPricesAt,
        local_marks::{
            price::VenuePrice,
            slot::Entry,
            snapshot::{CoinTable, Status, assemble},
            sources::MarkSources,
        },
        order_book::Px,
    };

    /// 2026-10-27T17:51:02Z, a Tuesday 13:51 in New York.
    const NOW_MS: u64 = 1_793_123_462_000;
    const MAIN: &str = r#"{"coins": {"BTC": {"spot": {"okx": {"symbol": "BTC-USDT", "pxMultiplier": "1"}}, "perp": {"okx": {"symbol": "BTC-USDT-SWAP", "pxMultiplier": "1"}}}}}"#;
    const HIP3: &str = r#"{"feed": "sip", "coins": {"xyz:TSLA": {"symbol": "TSLA"}}}"#;

    fn price(text: &str) -> VenuePrice {
        VenuePrice::parse(text).unwrap()
    }

    /// TSLA at 17:51:02 (szDecimals 3): stored oracle and external 405.770, mark 405.760, last trade 405.760; the HL book
    /// 405.750 / 405.770; the SIP 405.765 x 405.775 quoted 1 s ago; the EMA warm at a gap of -0.020.
    struct World {
        sources: MarkSources,
        table: Arc<CoinTable>,
        node: NodePrices,
        book: Option<BestPricesAt>,
        sip: FeedProjection,
        emas: Vec<Hip3Ema>,
        instant: tokio::time::Instant,
    }

    impl World {
        fn new() -> Self {
            let sources = MarkSources::parse(MAIN).unwrap();
            let table = Arc::new(CoinTable::new(&sources, Some(&Hip3SipSources::parse(HIP3).unwrap())));
            let tsla = Hip3Coin {
                order_asset: 110_000, size_decimals: 3, oracle: Some(405_770), external: Some(405_770), last_trade: Some(405_760),
                mark: Some(StoredMark { px: 405_760, time_ns: (NOW_MS - 500) * 1_000_000 }),
            };
            let hip3 = FxHashMap::from_iter([("xyz:TSLA".to_string(), tsla)]);
            let node = Ok(Arc::new(NativePriceSnapshot { seed_height: 900, height: 1_000, block_time_ms: NOW_MS - 500, coins: FxHashMap::default(), hip3 }));
            // BTC's book first, then TSLA's (1e-8)
            let prices = vec![(None, None), (Some(Px::new(40_575_000_000)), Some(Px::new(40_577_000_000)))];
            let book = Some(BestPricesAt { time: NOW_MS - 400, height: 1_001, trusted: true, prices });
            let sip = FeedProjection { connections: Vec::new(), messages: 1, entries: vec![quote("405.765", "405.775", NOW_MS - 1_000)] };
            let instant = tokio::time::Instant::now();
            Self { sources, table, node, book, sip, emas: vec![Hip3Ema::warm_for_test(-20.0, instant)], instant }
        }

        fn marks(&mut self, now_ms: u64) -> Hip3CoinMarks {
            let readings: Vec<_> = self.sources.feeds().map(|(feed, markets)| {
                let entries = vec![(None, Validity::NotConnected); markets.symbols.len()];
                (feed, markets, FeedProjection { connections: Vec::new(), messages: 0, entries })
            }).collect();
            self.instant += Duration::from_millis(50);
            let tick = Tick { now_ms, instant: self.instant, stopped: None, node: &self.node, book: self.book.as_ref(), readings: &readings, sip: Some(&self.sip) };
            let snapshot = assemble(&self.table, &tick, &mut self.emas);
            snapshot.hip3_coin("xyz:TSLA").unwrap().clone()
        }

        /// The same world `shift_ms` later: the node's block, the book and the SIP quote with it.
        fn shifted(mut self, shift_ms: u64) -> Self {
            let Ok(node) = &mut self.node else { unreachable!() };
            Arc::make_mut(node).block_time_ms += shift_ms;
            self.book.as_mut().unwrap().time += shift_ms;
            self.sip.entries[0] = quote("405.765", "405.775", NOW_MS - 1_000 + shift_ms);
            self
        }

        fn tsla(&mut self) -> &mut Hip3Coin {
            let Ok(node) = &mut self.node else { unreachable!() };
            Arc::make_mut(node).hip3.get_mut("xyz:TSLA").unwrap()
        }
    }

    /// A quote of SIP time `sip_ms`, received 80 ms later (the trade machine's transport lag, 10-27).
    fn quote(bid: &str, ask: &str, sip_ms: u64) -> (Option<Entry>, Validity) {
        (Some(Entry::for_test_at(Some(Quote { bid: price(bid), ask: price(ask) }), sip_ms + 80, Some(sip_ms), 1)), Validity::Valid)
    }

    fn code(marks: &Hip3CoinMarks) -> Option<&'static str> {
        marks.local.as_ref().err().map(|withheld| withheld.code)
    }

    /// The relayer's (1) 405.77 and (2) 405.75, HL's (3) 405.76: the stored mark 405.76 (bm 10-27 17:51:02).
    #[test]
    fn a_coin_with_every_input_has_both_legs() {
        let mut world = World::new();
        let marks = world.marks(NOW_MS);
        let node = marks.node.unwrap();
        assert_eq!((node.mark, node.stored_oracle, node.external, node.last_trade), (405_760, 405_770, Some(405_770), Some(405_760)));
        let local = marks.local.unwrap();
        assert_eq!((local.mark.oracle, local.mark.ema, local.mark.book, local.mark.predicted), (405_770, 405_750, 405_760, 405_760));
        assert_eq!((local.bid, local.ask, marks.sip), (405_750, 405_770, Validity::Valid));
        assert!((local.gap + 20.0).abs() < 0.01, "the tick sampled x = -10 for 50 ms: {}", local.gap);
    }

    #[test]
    fn each_gate_withholds_the_local_leg_with_its_code() {
        let sunday = 1_793_545_200_000; // 2026-11-01T15:00:00Z
        assert_eq!(code(&World::new().shifted(sunday - NOW_MS).marks(sunday)), Some("sipSession"));
        let mut world = World::new();
        world.tsla().external = Some(405_000);
        assert_eq!(code(&world.marks(NOW_MS)), Some("externalClosed"));
        world.tsla().external = None;
        assert_eq!(code(&world.marks(NOW_MS)), Some("externalUnknown"));
        let mut world = World::new();
        world.tsla().last_trade = None;
        assert_eq!(code(&world.marks(NOW_MS)), Some("lastTradeUnknown"));
        let mut world = World::new();
        world.sip.entries[0].1 = Validity::NotConnected;
        let marks = world.marks(NOW_MS);
        assert_eq!((code(&marks), marks.sip), (Some("sipQuote"), Validity::NotConnected));
        let mut world = World::new();
        world.sip.entries[0] = quote("405.765", "405.775", NOW_MS - 60_001);
        let quiet = world.marks(NOW_MS);
        assert_eq!((code(&quiet), quiet.local.unwrap_err().status), (Some("sipQuiet"), Status::Stale), "by the SIP time, though received later");
        let mut world = World::new();
        world.sip.entries[0].0 = Some(Entry::for_test_at(Some(Quote { bid: price("405.765"), ask: price("405.775") }), NOW_MS, None, 1));
        assert_eq!(code(&world.marks(NOW_MS)), Some("sipQuote"), "no SIP time: no age, no prediction");
        let mut world = World::new();
        world.emas = vec![Hip3Ema::default()];
        assert_eq!(code(&world.marks(NOW_MS)), Some("emaWarmup"));
        let mut world = World::new();
        world.book.as_mut().unwrap().prices[1].1 = None;
        assert_eq!(code(&world.marks(NOW_MS)), Some("hlBook"));
        let mut world = World::new();
        let behind = NOW_MS - 500 + crate::local_marks::snapshot::MAX_PROGRESS_LAG_MS + 1;
        let marks = world.marks(behind);
        assert_eq!((marks.node.as_ref().unwrap_err().code, code(&marks)), ("nodeProgress", Some("nodeProgress")));
    }

    /// The frame of `extHip3MarkPrices` and the forced flow's one entry, `local_predicted`, of the ready TSLA.
    #[test]
    fn the_frame_and_the_local_entry() {
        let mut world = World::new();
        let readings: Vec<_> = world.sources.feeds().map(|(feed, markets)| {
            (feed, markets, FeedProjection { connections: Vec::new(), messages: 0, entries: vec![(None, Validity::NotConnected); markets.symbols.len()] })
        }).collect();
        world.instant += Duration::from_millis(50);
        let tick = Tick { now_ms: NOW_MS, instant: world.instant, stopped: None, node: &world.node, book: world.book.as_ref(), readings: &readings, sip: Some(&world.sip) };
        let snapshot = assemble(&world.table, &tick, &mut world.emas);
        assert_eq!(snapshot.local_predicted("xyz:TSLA").map(|local| local.map_err(|withheld| withheld.code)), Some(Ok(405_760)));
        assert_eq!(snapshot.local_predicted("BTC").map(|local| local.map_err(|withheld| withheld.code)), Some(Err("nodeCoin")), "the main dex's through the same entry");
        assert_eq!(snapshot.local_predicted("xyz:NVDA"), None);
        assert_eq!(snapshot.coins().map(|(coin, _)| coin).collect::<Vec<_>>(), ["BTC"], "the main-dex selectors list no HIP-3 coin");
        let marks = snapshot.hip3_coin("xyz:TSLA").unwrap();
        let mut frame: serde_json::Value = serde_json::from_slice(&super::super::view::Hip3MarksView::of(&snapshot, "xyz:TSLA", marks).to_wire()).unwrap();
        let gap = frame["data"]["local"]["emaGapPx"].take().as_f64().unwrap();
        assert!((gap + 0.02).abs() < 1e-5, "{gap}");
        assert_eq!(frame, serde_json::json!({"channel": "extHip3MarkPrices", "data": {
            "schemaVersion": 1, "coin": "xyz:TSLA", "intervalMs": 50, "publishedTsMs": NOW_MS, "sipStatus": "valid",
            "node": {"status": "ready", "markPx": "405.76", "markTimeNs": (NOW_MS - 500) * 1_000_000, "storedOraclePx": "405.77",
                     "externalPx": "405.77", "lastTradePx": "405.76", "seedHeight": 900, "height": 1_000, "blockTimeMs": NOW_MS - 500},
            "local": {"status": "ready", "predictedMarkPx": "405.76", "oraclePx": "405.77", "sipMidPx": "405.77", "emaPx": "405.75",
                      "emaGapPx": null, "emaSampledS": 750.05, "bookPx": "405.76", "bidPx": "405.75", "askPx": "405.77",
                      "sipBidPx": "405.765", "sipAskPx": "405.775", "sipQuoteTimeMs": NOW_MS - 1_000, "sipReceivedMs": NOW_MS - 920,
                      "nodeHeight": 1_000, "nodeBlockTimeMs": NOW_MS - 500, "bookTimeMs": NOW_MS - 400, "bookHeight": 1_001}}}));
    }

    /// Review 000535 r2: the EMA samples only with a trusted book and node state; a long untrusted span starts it over.
    #[test]
    fn the_ema_samples_only_trusted_ticks() {
        let mut world = World::new();
        world.book.as_mut().unwrap().trusted = false;
        world.instant += Duration::from_secs(10);
        assert_eq!(code(&world.marks(NOW_MS)), Some("hlBook"));
        world.book.as_mut().unwrap().trusted = true;
        assert_eq!(code(&world.marks(NOW_MS)), Some("emaWarmup"), "10 s unsampled: over the gap limit, it started over");
    }
}
