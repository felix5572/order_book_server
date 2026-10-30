//! The HIP-3 coins' legs at one tick (design 000535 r1 §3, r2 revisions 2–4): the node leg is the stream's stored
//! prices, the local leg `compute::hip3_local_mark` on them, the HL book and the session's quote now (the SIP's, BOATS's
//! overnight: design 000543). The same progress clocks as the main dex's gate both legs; then, for the local leg, the
//! observation gates in order: a quote session, the relayer on its external price (external == oracle), a known last
//! trade, a valid and recent quote of the session's feed, a warm EMA. v2 (design 000537): each coin's quote midpoints
//! and offset carry move at every tick before its legs, the carry taking in the node state's oracle writes only while
//! that state is trusted.

use rustc_hash::FxHashMap;

use super::{
    carry::{Hip3Carry, Hip3CarryReading, OracleWrite, QuoteMid, QuoteMidHistory},
    compute::{Hip3Inputs, Hip3LocalMark, hip3_local_mark},
    ema::Hip3Ema,
    protocol::EquityFeed,
    session::equity_session,
    sources::Hip3SipSources,
};
use crate::{
    accounts::marks::seed::{Hip3Coin, NoRelayerGap, RelayerGap},
    local_marks::{
        slot::{Entry, Quote, Validity},
        snapshot::{Tick, Withheld, book_units},
    },
};

/// A quote whose feed's time is older than this no longer counts (design 000535 r1 §3, starting value): after a
/// session the feed's last quote stands still.
const MAX_QUOTE_QUIET_MS: u64 = 60_000;

/// A coin's quote of the session's feed at a tick, and its validity.
type SessionQuote = (EquityFeed, (Option<Entry>, Validity));

/// The node leg: the coin's stored prices, in its price units.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Hip3NodeMark {
    pub(crate) size_decimals: u32,
    pub(crate) mark: u64,
    pub(crate) mark_time_ns: u64,
    pub(crate) stored_oracle: u64,
    /// When the relayer last wrote `stored_oracle` (block time, unix ns); `None` since the checkpoint until a write.
    pub(crate) oracle_time_ns: Option<u64>,
    pub(crate) external: Option<u64>,
    pub(crate) last_trade: Option<u64>,
    /// The (2) - (1) of the relayer's last setOracle (design 000582).
    pub(crate) relayer_gap: Result<RelayerGap, NoRelayerGap>,
}

impl Hip3NodeMark {
    /// The oracle's last write as the carry takes it in; `None` before one since the checkpoint.
    fn oracle_write(&self) -> Option<OracleWrite> {
        let on_external = self.external == Some(self.stored_oracle);
        self.oracle_time_ns.map(|time_ns| OracleWrite { time_ns, oracle: self.stored_oracle, size_decimals: self.size_decimals, on_external })
    }
}

/// The local leg: the terms, the inputs they took, and the EMA's sampled time.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Hip3LocalLeg {
    pub(crate) size_decimals: u32,
    pub(crate) bid: u64,
    pub(crate) ask: u64,
    pub(crate) quote_feed: EquityFeed,
    pub(crate) quote: Quote,
    pub(crate) quote_event_ms: Option<u64>,
    pub(crate) quote_received_ms: u64,
    /// (2) - (1) of the EMA, price units.
    pub(crate) gap: f64,
    /// The block time (unix ns) of the relayer write the EMA is anchored at.
    pub(crate) ema_anchored_ns: u64,
    /// The offset carry this tick (`mark.carried` its result).
    pub(crate) carry: Hip3CarryReading,
    pub(crate) mark: Hip3LocalMark,
}

/// One HIP-3 coin's state across ticks, owned by the local marks' owner.
#[derive(Debug, Clone, Default)]
pub(crate) struct Hip3CoinState {
    pub(crate) ema: Hip3Ema,
    pub(crate) quote_mids: QuoteMidHistory,
    pub(crate) carry: Hip3Carry,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Hip3CoinMarks {
    pub(crate) node: Result<Hip3NodeMark, Withheld>,
    pub(crate) local: Result<Hip3LocalLeg, Withheld>,
    /// The session's feed and its quote's validity, whatever the legs; `None` outside every session.
    pub(crate) quote: Option<(EquityFeed, Validity)>,
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

/// Every coin's legs. First its state moves: the quote midpoints with this tick's quote of the session's feed; with
/// trusted node prices, the carry with the oracle's write, and, the book trusted too, the EMA with this tick's sample.
pub(crate) fn hip3_marks(table: &Hip3Table, tick: &Tick<'_>, book_offset: usize, gates: Hip3Gates<'_>, states: &mut [Hip3CoinState]) -> Vec<Hip3CoinMarks> {
    assert_eq!(states.len(), table.names.len(), "one state per HIP-3 coin");
    let feed = equity_session(tick.now_ms).map(|id| EquityFeed::of(id.session));
    table
        .names
        .iter()
        .enumerate()
        .map(|(index, coin)| {
            let state = &mut states[index];
            let quote: Option<SessionQuote> = feed.map(|feed| (feed, tick.equity.map_or((None, Validity::NotConnected), |readings| readings.of(feed).entries[index])));
            state.quote_mids.observe(quote.and_then(quote_mid), tick.now_ms);
            let node = node_leg(coin, tick, gates.node);
            if let Some(write) = node.as_ref().ok().and_then(Hip3NodeMark::oracle_write) {
                state.carry.write(write, &state.quote_mids);
            }
            let book = node.as_ref().ok().and_then(|node| book_prices(coin, tick, book_offset + index, node.size_decimals).transpose()).transpose();
            if let (Ok(node), Ok(Some((bid, ask))), None) = (&node, &book, gates.book) {
                #[expect(clippy::cast_precision_loss, reason = "prices are below 2^53")]
                let x = (bid + ask) as f64 / 2.0 - node.stored_oracle as f64;
                match node.relayer_gap {
                    Ok(relayer) => state.ema.observe(tick.instant, tick.now_ms, x, relayer),
                    Err(_) => state.ema.withdraw(),
                }
            }
            let local = match (gates.stopped.or(gates.node).or(gates.book), &node, book) {
                (Some(withheld), _, _) | (None, Err(withheld), _) => Err(withheld.clone()),
                (None, Ok(_), Err(withheld)) => Err(withheld),
                (None, Ok(_), Ok(None)) => Err(Withheld::unavailable("hlBook", format!("the HL book of {coin} has an empty side"))),
                (None, Ok(node), Ok(Some((bid, ask)))) => local_leg(coin, tick, quote, node, (bid, ask), state),
            };
            Hip3CoinMarks { node, local, quote: quote.map(|(feed, (_, validity))| (feed, validity)) }
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
    Ok(Hip3NodeMark {
        size_decimals: state.size_decimals, mark: mark.px, mark_time_ns: mark.time_ns, stored_oracle, oracle_time_ns: state.oracle_time_ns,
        external: state.external, last_trade: state.last_trade, relayer_gap: state.relayer_gap,
    })
}

/// A valid quote's midpoint at its feed's time, for the coin's history; `None` otherwise.
fn quote_mid((feed, (entry, validity)): SessionQuote) -> Option<QuoteMid> {
    let entry = entry.filter(|_| validity == Validity::Valid)?;
    let quote = entry.quote?;
    Some(QuoteMid { feed, epoch: entry.epoch(), quote_ms: entry.event_ms?, mid: quote.bid.midpoint(quote.ask)? })
}

/// The coin's best bid and ask in its price units; `None` with an empty side.
fn book_prices(coin: &str, tick: &Tick<'_>, book_index: usize, size_decimals: u32) -> Result<Option<(u64, u64)>, Withheld> {
    let Some(book) = tick.book else { return Ok(None) };
    let (Some(bid), Some(ask)) = book.prices[book_index] else { return Ok(None) };
    let units = |px| book_units(px, size_decimals).map_err(|withheld| Withheld { detail: format!("{coin}: {}", withheld.detail), ..withheld });
    Ok(Some((units(bid)?, units(ask)?)))
}

fn local_leg(coin: &str, tick: &Tick<'_>, quote: Option<SessionQuote>, node: &Hip3NodeMark, (bid, ask): (u64, u64), state: &Hip3CoinState) -> Result<Hip3LocalLeg, Withheld> {
    let Some((feed, (entry, validity))) = quote else {
        let detail = "outside the US stocks' quote sessions (New York: 04:00–20:00 Monday to Friday on the SIP, 20:05–04:00 begun Sunday to Thursday on BOATS)";
        return Err(Withheld::unavailable("quoteSession", detail));
    };
    let external = node.external.ok_or_else(|| Withheld::unavailable("externalUnknown", format!("{coin} has no stored external price yet")))?;
    if external != node.stored_oracle {
        let detail = format!("{coin}'s stored external {external} differs from its oracle {}: the relayer prices internally", node.stored_oracle);
        return Err(Withheld::unavailable("externalClosed", detail));
    }
    let last_trade = node.last_trade.ok_or_else(|| Withheld::unavailable("lastTradeUnknown", format!("{coin} has no last trade since the state's checkpoint")))?;
    let name = feed.name();
    let entry = entry.filter(|_| validity == Validity::Valid).ok_or_else(|| Withheld::unavailable("quoteUnavailable", format!("the {name} quote of {coin} is {validity:?}")))?;
    let quote = entry.quote.unwrap_or_else(|| unreachable!("a valid entry holds a quote"));
    // the Alpaca parser requires the quote's time; never the receive time in its place (review 000536 P2-2)
    let quoted_ms = entry.event_ms.ok_or_else(|| Withheld::unavailable("quoteUnavailable", format!("the {name} quote of {coin} carries no time")))?;
    let age_ms = tick.now_ms.saturating_sub(quoted_ms);
    if age_ms > MAX_QUOTE_QUIET_MS {
        return Err(Withheld::stale("quoteQuiet", format!("the {name} quote of {coin} is {age_ms} ms old")));
    }
    if let Err(missing) = node.relayer_gap {
        let detail = match missing {
            NoRelayerGap::SinceCheckpoint => format!("no setOracle of {coin}'s dex read since the checkpoint"),
            NoRelayerGap::Unreadable => format!("the last setOracle of {coin}'s dex did not read as its (1) and (2): the accounts diagnostics say why"),
        };
        return Err(Withheld::unavailable("relayerGapUnknown", detail));
    }
    let (gap, ema_anchored_ns) = state.ema.gap().ok_or_else(|| {
        Withheld::unavailable("relayerGapUnknown", format!("the EMA of {coin} waits for a relayer write newer than the one it took, after a sampling gap"))
    })?;
    let carry = state.carry.reading(tick.now_ms);
    let inputs = Hip3Inputs {
        size_decimals: node.size_decimals, stored_oracle: node.stored_oracle, quote_bid: quote.bid, quote_ask: quote.ask, gap, carry: carry.carry, bid,
        ask, last_trade,
    };
    let mark = hip3_local_mark(inputs).map_err(|err| Withheld::unavailable("calculation", format!("{coin}: {err}")))?;
    Ok(Hip3LocalLeg {
        size_decimals: node.size_decimals, bid, ask, quote_feed: feed, quote, quote_event_ms: entry.event_ms, quote_received_ms: entry.received_ms, gap,
        ema_anchored_ns, carry, mark,
    })
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::*;
    use crate::{
        local_marks::{
            feeds::EquityReadings,
            hip3::carry::CarryStatus,
            subscriptions::FeedProjection,
        },
        accounts::{
            marks::{NativePriceSnapshot, seed::StoredMark},
            publish::NodePrices,
        },
        listeners::order_book::BestPricesAt,
        local_marks::{
            price::VenuePrice,
            slot::Entry,
            snapshot::{CoinTable, OwnerState, Status, assemble},
            sources::MarkSources,
        },
        order_book::Px,
    };

    /// 2026-10-27T17:51:02Z, a Tuesday 13:51 in New York.
    const NOW_MS: u64 = 1_793_123_462_000;
    /// The relayer's last write, 500 ms before.
    const WRITE_NS: u64 = (NOW_MS - 500) * 1_000_000;
    const MAIN: &str = r#"{"coins": {"BTC": {"spot": {"okx": {"symbol": "BTC-USDT", "pxMultiplier": "1"}}, "perp": {"okx": {"symbol": "BTC-USDT-SWAP", "pxMultiplier": "1"}}}}}"#;
    const HIP3: &str = r#"{"feed": "sip", "coins": {"xyz:TSLA": {"symbol": "TSLA"}}}"#;

    fn price(text: &str) -> VenuePrice {
        VenuePrice::parse(text).unwrap()
    }

    /// TSLA at 17:51:02 (szDecimals 3): stored oracle and external 405.770, mark 405.760, last trade 405.760; the HL book
    /// 405.750 / 405.770; the SIP 405.765 x 405.775 quoted 1 s ago, BOATS not connected; the relayer's last write 500 ms
    /// ago at a gap of -0.020, the EMA anchored at it.
    struct World {
        sources: MarkSources,
        table: Arc<CoinTable>,
        node: NodePrices,
        book: Option<BestPricesAt>,
        equity: EquityReadings,
        state: OwnerState,
        instant: tokio::time::Instant,
    }

    impl World {
        fn new() -> Self {
            let sources = MarkSources::parse(MAIN).unwrap();
            let table = Arc::new(CoinTable::new(&sources, Some(&Hip3SipSources::parse(HIP3).unwrap())));
            let tsla = Hip3Coin {
                order_asset: 110_000, size_decimals: 3, oracle: Some(405_770), oracle_time_ns: Some((NOW_MS - 500) * 1_000_000), external: Some(405_770),
                last_trade: Some(405_760),
                mark: Some(StoredMark { px: 405_760, time_ns: (NOW_MS - 500) * 1_000_000 }),
                relayer_gap: Ok(RelayerGap { gap_raw: -20, time_ns: WRITE_NS }),
            };
            let hip3 = FxHashMap::from_iter([("xyz:TSLA".to_string(), tsla)]);
            let node = Ok(Arc::new(NativePriceSnapshot { seed_height: 900, height: 1_000, block_time_ms: NOW_MS - 500, coins: FxHashMap::default(), hip3 }));
            // BTC's book first, then TSLA's (1e-8)
            let prices = vec![(None, None), (Some(Px::new(40_575_000_000)), Some(Px::new(40_577_000_000)))];
            let book = Some(BestPricesAt { time: NOW_MS - 400, height: 1_001, trusted: true, prices });
            let sip = FeedProjection { connections: Vec::new(), messages: 1, entries: vec![quote("405.765", "405.775", NOW_MS - 1_000)] };
            let boats = FeedProjection { connections: Vec::new(), messages: 0, entries: vec![(None, Validity::NotConnected)] };
            let instant = tokio::time::Instant::now();
            let mut state = table.owner_state();
            state.hip3[0] = Hip3CoinState { ema: Hip3Ema::anchored_for_test(-20.0, WRITE_NS, instant), ..Hip3CoinState::default() };
            Self { sources, table, node, book, equity: EquityReadings { sip, boats }, state, instant }
        }

        fn marks(&mut self, now_ms: u64) -> Hip3CoinMarks {
            let readings: Vec<_> = self.sources.feeds().map(|(feed, markets)| {
                let entries = vec![(None, Validity::NotConnected); markets.symbols.len()];
                (feed, markets, FeedProjection { connections: Vec::new(), messages: 0, entries })
            }).collect();
            self.instant += Duration::from_millis(50);
            let tick = Tick { now_ms, instant: self.instant, stopped: None, node: &self.node, book: self.book.as_ref(), readings: &readings, equity: Some(&self.equity) };
            let snapshot = assemble(&self.table, &tick, &mut self.state);
            snapshot.hip3_coin("xyz:TSLA").unwrap().clone()
        }

        /// The same world `shift_ms` later: the node's block, the book and the SIP quote with it.
        fn shifted(mut self, shift_ms: u64) -> Self {
            let Ok(node) = &mut self.node else { unreachable!() };
            Arc::make_mut(node).block_time_ms += shift_ms;
            self.book.as_mut().unwrap().time += shift_ms;
            self.equity.sip.entries[0] = quote("405.765", "405.775", NOW_MS - 1_000 + shift_ms);
            self
        }

        fn tsla(&mut self) -> &mut Hip3Coin {
            let Ok(node) = &mut self.node else { unreachable!() };
            Arc::make_mut(node).hip3.get_mut("xyz:TSLA").unwrap()
        }
    }

    /// A quote of its feed's time `quote_ms`, received 80 ms later (the trade machine's transport lag, 10-27).
    fn quote(bid: &str, ask: &str, quote_ms: u64) -> (Option<Entry>, Validity) {
        (Some(Entry::for_test_at(Some(Quote { bid: price(bid), ask: price(ask) }), quote_ms + 80, Some(quote_ms), 1)), Validity::Valid)
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
        assert_eq!((local.bid, local.ask, local.quote_feed, marks.quote), (405_750, 405_770, EquityFeed::Sip, Some((EquityFeed::Sip, Validity::Valid))));
        assert!((local.gap + 20.0).abs() < 0.01, "the tick sampled x = -10 for 50 ms: {}", local.gap);
    }

    #[test]
    fn each_gate_withholds_the_local_leg_with_its_code() {
        let sunday = 1_793_545_200_000; // 2026-11-01T15:00:00Z
        assert_eq!(code(&World::new().shifted(sunday - NOW_MS).marks(sunday)), Some("quoteSession"));
        let mut world = World::new();
        world.tsla().external = Some(405_000);
        assert_eq!(code(&world.marks(NOW_MS)), Some("externalClosed"));
        world.tsla().external = None;
        assert_eq!(code(&world.marks(NOW_MS)), Some("externalUnknown"));
        let mut world = World::new();
        world.tsla().last_trade = None;
        assert_eq!(code(&world.marks(NOW_MS)), Some("lastTradeUnknown"));
        let mut world = World::new();
        world.equity.sip.entries[0].1 = Validity::NotConnected;
        let marks = world.marks(NOW_MS);
        assert_eq!((code(&marks), marks.quote), (Some("quoteUnavailable"), Some((EquityFeed::Sip, Validity::NotConnected))));
        let mut world = World::new();
        world.equity.sip.entries[0] = quote("405.765", "405.775", NOW_MS - 60_001);
        let quiet = world.marks(NOW_MS);
        assert_eq!((code(&quiet), quiet.local.unwrap_err().status), (Some("quoteQuiet"), Status::Stale), "by the SIP time, though received later");
        let mut world = World::new();
        world.equity.sip.entries[0].0 = Some(Entry::for_test_at(Some(Quote { bid: price("405.765"), ask: price("405.775") }), NOW_MS, None, 1));
        assert_eq!(code(&world.marks(NOW_MS)), Some("quoteUnavailable"), "no SIP time: no age, no prediction");
        let mut world = World::new();
        world.state.hip3[0] = Hip3CoinState::default();
        assert_eq!(code(&world.marks(NOW_MS)), None, "a fresh EMA anchors at the relayer's write at once: no warmup");
        for missing in [NoRelayerGap::SinceCheckpoint, NoRelayerGap::Unreadable] {
            let mut world = World::new();
            world.tsla().relayer_gap = Err(missing);
            assert_eq!(code(&world.marks(NOW_MS)), Some("relayerGapUnknown"), "{missing:?}");
        }
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
        let tick = Tick { now_ms: NOW_MS, instant: world.instant, stopped: None, node: &world.node, book: world.book.as_ref(), readings: &readings, equity: Some(&world.equity) };
        let snapshot = assemble(&world.table, &tick, &mut world.state);
        assert_eq!(snapshot.local_predicted("xyz:TSLA").map(|local| local.map_err(|withheld| withheld.code)), Some(Ok(405_760)));
        assert_eq!(snapshot.local_predicted("BTC").map(|local| local.map_err(|withheld| withheld.code)), Some(Err("nodeCoin")), "the main dex's through the same entry");
        assert_eq!(snapshot.local_predicted("xyz:NVDA"), None);
        assert_eq!(snapshot.coins().map(|(coin, _)| coin).collect::<Vec<_>>(), ["BTC"], "the main-dex selectors list no HIP-3 coin");
        let marks = snapshot.hip3_coin("xyz:TSLA").unwrap();
        let mut frame: serde_json::Value = serde_json::from_slice(&super::super::view::Hip3MarksView::of(&snapshot, "xyz:TSLA", marks).to_wire()).unwrap();
        let gap = frame["data"]["local"]["emaGapPx"].take().as_f64().unwrap();
        assert!((gap + 0.02).abs() < 1e-5, "{gap}");
        assert_eq!(frame, serde_json::json!({"channel": "extHip3MarkPrices", "data": {
            "schemaVersion": 4, "coin": "xyz:TSLA", "intervalMs": 50, "publishedTsMs": NOW_MS, "quoteFeed": "sip", "quoteStatus": "valid",
            "node": {"status": "ready", "markPx": "405.76", "markTimeNs": (NOW_MS - 500) * 1_000_000, "storedOraclePx": "405.77",
                     "storedOracleTimeNs": (NOW_MS - 500) * 1_000_000, "externalPx": "405.77", "lastTradePx": "405.76", "seedHeight": 900, "height": 1_000, "blockTimeMs": NOW_MS - 500},
            "local": {"status": "ready", "predictedMarkPx": "405.76", "oraclePx": "405.77", "quoteFeed": "sip", "quoteMidPx": "405.77", "carriedPx": null,
                      "carryStatus": "warmup", "carryPx": null, "carryRho": null, "carryPairs": 0.0, "carryOffsetPx": null, "emaPx": "405.75",
                      "emaGapPx": null, "emaAnchorTimeMs": NOW_MS - 500, "bookPx": "405.76", "bidPx": "405.75", "askPx": "405.77",
                      "quoteBidPx": "405.765", "quoteAskPx": "405.775", "quoteTimeMs": NOW_MS - 1_000, "quoteReceivedMs": NOW_MS - 920,
                      "nodeHeight": 1_000, "nodeBlockTimeMs": NOW_MS - 500, "bookTimeMs": NOW_MS - 400, "bookHeight": 1_001}}}));
    }

    /// Design 000537: the oracle write the node state shows pairs with the carry's last; ready, (1) is the midpoint plus
    /// rho times its offset. TSLA's aggregate 0.300 above the SIP midpoint 405.770 for two minutes of writes: (1) 406.07,
    /// (2) 406.05 with the EMA's -0.020, (3) 405.76: the mark 406.05 (without the carry 405.76).
    #[test]
    fn a_ready_carry_moves_the_oracle_term() {
        let mut world = World::new();
        world.equity.sip.entries[0] = quote("405.765", "405.775", NOW_MS - 3_000);
        world.tsla().oracle = Some(406_070);
        world.tsla().external = Some(406_070);
        let mut mids = QuoteMidHistory::default();
        mids.observe(Some(QuoteMid { feed: EquityFeed::Sip, epoch: 1, quote_ms: NOW_MS - 200_000, mid: price("405.77") }), NOW_MS - 200_000);
        for step in (1..=40).rev() {
            let time_ms = NOW_MS - 500 - 3_000 * step;
            let write = OracleWrite { time_ns: time_ms * 1_000_000, oracle: 406_070, size_decimals: 3, on_external: true };
            world.state.hip3[0].carry.write(write, &mids);
        }
        let marks = world.marks(NOW_MS);
        let local = marks.local.unwrap();
        assert_eq!((local.carry.status, local.carry.rho, local.carry.offset, local.carry.carry), (CarryStatus::Ready, Some(1.0), Some(300.0), Some(300.0)));
        assert_eq!((local.mark.quote_mid, local.mark.carried, local.mark.oracle, local.mark.ema, local.mark.predicted), (405_770, Some(406_070), 406_070, 406_050, 406_050));
        // the relayer pricing internally at its next write: the offset unknown, (1) the midpoint again
        world.tsla().external = Some(405_000);
        world.tsla().oracle_time_ns = Some((NOW_MS + 2_500) * 1_000_000);
        let marks = world.shifted(0).marks(NOW_MS + 3_000);
        assert_eq!(code(&marks), Some("externalClosed"));
    }

    /// Design 000543: overnight the local leg reads BOATS's quote, not the SIP's, and only BOATS's outage withholds it.
    #[test]
    fn overnight_the_quote_is_boats() {
        // 2026-10-28T02:00:00Z, Tuesday 22:00 in New York: Tuesday's night
        let night = 1_793_152_800_000;
        let mut world = World::new().shifted(night - NOW_MS);
        world.equity.boats.entries[0] = quote("405.70", "405.90", night - 1_000);
        let marks = world.marks(night);
        let local = marks.local.unwrap();
        assert_eq!((local.quote_feed, marks.quote, local.mark.quote_mid), (EquityFeed::Boats, Some((EquityFeed::Boats, Validity::Valid)), 405_800));
        world.equity.boats.entries[0].1 = Validity::NotConnected;
        let marks = world.marks(night);
        assert_eq!((code(&marks), marks.quote), (Some("quoteUnavailable"), Some((EquityFeed::Boats, Validity::NotConnected))), "the SIP's valid quote does not stand in");
        let gap = 1_793_145_780_000; // 2026-10-28T00:03:00Z, Tuesday 20:03 in New York: between the post-market and the night
        assert_eq!(code(&World::new().shifted(gap - NOW_MS).marks(gap)), Some("quoteSession"));
    }

    /// Design 000582: the EMA samples only with a trusted book and node state; past the node's freshness limit unsampled
    /// it waits for a relayer write newer than the one it took, then anchors at once; a gap within the limit carries on;
    /// the node listing no usable gap withdraws it.
    #[test]
    fn the_ema_samples_only_trusted_ticks() {
        let mut world = World::new();
        world.book.as_mut().unwrap().trusted = false;
        world.instant += Duration::from_secs(4);
        assert_eq!(code(&world.marks(NOW_MS)), Some("hlBook"));
        world.book.as_mut().unwrap().trusted = true;
        assert_eq!(code(&world.marks(NOW_MS)), None, "4 s unsampled: within the limit");
        world.book.as_mut().unwrap().trusted = false;
        world.instant += Duration::from_secs(6);
        assert_eq!(code(&world.marks(NOW_MS)), Some("hlBook"));
        world.book.as_mut().unwrap().trusted = true;
        assert_eq!(code(&world.marks(NOW_MS)), Some("relayerGapUnknown"), "6 s unsampled: the write taken does not serve again");
        world.tsla().relayer_gap = Ok(RelayerGap { gap_raw: -30, time_ns: WRITE_NS + 3_000_000_000 });
        let local = world.marks(NOW_MS).local.unwrap();
        assert_eq!(local.ema_anchored_ns, WRITE_NS + 3_000_000_000, "a newer write anchors at once");
        world.tsla().relayer_gap = Err(NoRelayerGap::Unreadable);
        assert_eq!(code(&world.marks(NOW_MS)), Some("relayerGapUnknown"));
        world.tsla().relayer_gap = Ok(RelayerGap { gap_raw: -30, time_ns: WRITE_NS + 3_000_000_000 });
        assert_eq!(code(&world.marks(NOW_MS)), Some("relayerGapUnknown"), "withdrawn: the same write does not come back");
    }
}
