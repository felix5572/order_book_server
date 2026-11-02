//! The US stock leg of `extHip3ExternalQuotes` (hyperliquid-trade design 000669 r1–r3, 1.2–1.3): a SIP coin's quote
//! of the feed that counts now - the SIP's in the day sessions, BOATS's overnight (`EquityFeed::of`) -, as Alpaca sent
//! it (multiplier 1, off HL's grid), with its exchange time, and the coin's normal basis to it on that feed.
//!
//! Judged at the snapshot's publish time, in order:
//! - a SIP coin while the server runs without the SIP feeds (no `--hip3-sip-marks`) is `equityOff`;
//! - a session the relayer covers now (`equity_session`), else `noSession`;
//! - that session's feed's entry valid (connected, quoted on this connection, two-sided), else the entry's validity;
//! - received by the publish time (`external::received_by`, the Binance leg's own rule), else `receivedAfterPublish`;
//! - its exchange time known (`noQuoteTime`), at most `MAX_QUOTE_LEAD_MS` after the publish time (`quoteFromFuture`),
//!   and of the session now by the venue's own sessions (`venue_session`), else `quoteOutsideSession`: BOATS's quote of
//!   20:02, unchanged, holds from 20:05;
//! - no age limit: the feeds push on change only, and a quiet quote holds, as Binance's and the main dex's do.
//!
//! Holidays quote nothing (the leg unknown); an early close is not known, a quiet quote after it holding till 20:00.
//! The readings are the tick's, read before the publish time is taken: no quote received after `publishedTsMs`.
//!
//! The basis: one `ExternalBasisWindow` a coin and feed (`external_basis`' rule: HL's book midpoint over the quote's -
//! 1), sampled only from a ready leg with HL's book usable at the publish time, so at the night's and the
//! pre-market's start that feed's window warms up anew.

use tokio::time::Instant;

use super::{
    external::{Hip3SourceLeg, received_by},
    protocol::EquityFeed,
    session::{EquitySessionId, venue_session},
};
use crate::{
    local_marks::{
        external_basis::{self, ExternalBasis, ExternalBasisWindow},
        feeds::EquityReadings,
        slot::{Quote, Validity},
        snapshot::Withheld,
    },
    order_book::Px,
};

/// A quote stamped further than this past the publish time is not of the session now (design 000669 r1 1.3).
const MAX_QUOTE_LEAD_MS: u64 = 1_000;

/// A channel coin's place among the SIP feeds' markets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EquityPlace {
    /// Not in the SIP table.
    Unlisted,
    /// In it, the server running without the SIP feeds.
    Off,
    /// Its market in the feeds' readings.
    Read(usize),
}

/// The quote of the feed that counts now, valid at the publish time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EquityQuote {
    pub(crate) feed: EquityFeed,
    pub(crate) quote: Quote,
    /// When this server received it (its wall clock), never after the publish time.
    pub(crate) received_ms: u64,
    /// The venue's exchange time of it.
    pub(crate) quote_ms: u64,
}

/// A SIP coin's quote and its normal basis to it on that feed.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Hip3EquityLeg {
    pub(crate) quote: EquityQuote,
    /// Withheld below its minimum samples.
    pub(crate) basis: Result<ExternalBasis, Withheld>,
}

/// A coin's basis windows, one a feed.
#[derive(Debug, Clone, Default)]
pub(crate) struct EquityBasisWindows {
    sip: ExternalBasisWindow,
    boats: ExternalBasisWindow,
}

impl EquityBasisWindows {
    const fn of(&self, feed: EquityFeed) -> &ExternalBasisWindow {
        match feed {
            EquityFeed::Sip => &self.sip,
            EquityFeed::Boats => &self.boats,
        }
    }

    const fn of_mut(&mut self, feed: EquityFeed) -> &mut ExternalBasisWindow {
        match feed {
            EquityFeed::Sip => &mut self.sip,
            EquityFeed::Boats => &mut self.boats,
        }
    }
}

/// What every coin's leg reads at this snapshot.
#[derive(Clone, Copy)]
pub(crate) struct EquityTick<'a> {
    /// The tick's readings of both feeds; `None` without them.
    pub(crate) readings: Option<&'a EquityReadings>,
    pub(crate) published_ms: u64,
    /// The relayer's session at the publish time.
    pub(crate) session: Option<EquitySessionId>,
    pub(crate) instant: Instant,
}

/// The coin's leg. First its feeds' windows move, the current feed's with this tick's sample when its quote is valid
/// and `book` (HL's two sides, when they may be sampled) is given; `stopped` withholds the leg after.
pub(crate) fn equity_leg(
    coin: &str,
    place: EquityPlace,
    tick: EquityTick<'_>,
    stopped: Option<&Withheld>,
    book: Option<(Px, Px)>,
    windows: &mut EquityBasisWindows,
) -> Hip3SourceLeg<Hip3EquityLeg> {
    let market = match place {
        EquityPlace::Unlisted => return Hip3SourceLeg::Unlisted,
        EquityPlace::Off => return Hip3SourceLeg::Listed(Err(equity_off(coin))),
        EquityPlace::Read(market) => market,
    };
    let readings = tick.readings.unwrap_or_else(|| unreachable!("a coin read among the SIP feeds' markets has their readings"));
    let quote = session_quote(coin, readings, market, tick);
    for feed in EquityFeed::ALL {
        let ready = quote.as_ref().ok().filter(|quote| quote.feed == feed);
        let sample = book.zip(ready).map(|((bid, ask), ready)| external_basis::sample(bid, ask, ready.quote.bid, ready.quote.ask));
        windows.of_mut(feed).observe(tick.instant, sample);
    }
    if let Some(stopped) = stopped {
        return Hip3SourceLeg::Listed(Err(stopped.clone()));
    }
    Hip3SourceLeg::Listed(quote.map(|quote| Hip3EquityLeg { quote, basis: windows.of(quote.feed).published() }))
}

fn equity_off(coin: &str) -> Withheld {
    let detail = format!("{coin} is in the HIP-3 SIP sources, but the server runs without --hip3-sip-marks: no SIP or BOATS feed");
    Withheld::unavailable("equityOff", detail)
}

/// The quote of the session's feed valid at the publish time; why not.
fn session_quote(coin: &str, readings: &EquityReadings, market: usize, tick: EquityTick<'_>) -> Result<EquityQuote, Withheld> {
    let published_ms = tick.published_ms;
    let session = tick.session.ok_or_else(|| {
        let detail = format!(
            "{published_ms} ms is outside the US stocks' quote sessions (New York: 04:00–20:00 Monday to Friday on the SIP, 20:05–04:00 begun \
             Sunday to Thursday on BOATS)"
        );
        Withheld::unavailable("noSession", detail)
    })?;
    let feed = EquityFeed::of(session.session);
    let name = feed.name();
    let (entry, validity) = readings.of(feed).entries[market];
    if validity != Validity::Valid {
        let received = entry.map_or_else(|| "none received".to_string(), |entry| format!("the last received at {} ms", entry.received_ms));
        return Err(Withheld::unavailable(validity.name(), format!("the {name} quote of {coin} is {} at {published_ms} ms ({received})", validity.name())));
    }
    let entry = entry.unwrap_or_else(|| unreachable!("a valid entry holds a quote"));
    received_by(name, coin, entry.received_ms, published_ms)?;
    let quote = entry.quote.unwrap_or_else(|| unreachable!("a valid entry holds a quote"));
    let quote_ms = entry.event_ms.ok_or_else(|| Withheld::unavailable("noQuoteTime", format!("the {name} quote of {coin} carries no exchange time")))?;
    if quote_ms > published_ms + MAX_QUOTE_LEAD_MS {
        let detail = format!("the {name} quote of {coin} is stamped {quote_ms} ms, over {MAX_QUOTE_LEAD_MS} ms after the publish time {published_ms} ms");
        return Err(Withheld::unavailable("quoteFromFuture", detail));
    }
    let quoted_in = venue_session(quote_ms);
    if quoted_in != Some(session) {
        let detail = format!(
            "the {name} quote of {coin} stamped {quote_ms} ms is of {}, not of the session now ({})",
            quoted_in.map_or_else(|| "no session".to_string(), session_name),
            session_name(session)
        );
        return Err(Withheld::unavailable("quoteOutsideSession", detail));
    }
    Ok(EquityQuote { feed, quote, received_ms: entry.received_ms, quote_ms })
}

/// `Regular 2026-10-27`: a session and its New York day (an overnight session's evening).
fn session_name(id: EquitySessionId) -> String {
    format!("{:?} {}", id.session, id.date)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::local_marks::{
        external_basis::MIN_SAMPLES,
        hip3::session::{EquitySession, equity_session},
        price::VenuePrice,
        slot::Entry,
        snapshot::Status,
        subscriptions::FeedProjection,
    };

    /// HL's book 405.75 / 405.77 (1e-8).
    const BOOK: Option<(Px, Px)> = Some((Px::new(40_575_000_000), Px::new(40_577_000_000)));

    fn ms(text: &str) -> u64 {
        u64::try_from(chrono::DateTime::parse_from_rfc3339(text).unwrap().timestamp_millis()).unwrap()
    }

    /// Tuesday 2026-10-27 13:51:02 in New York (EDT, UTC-4): the regular session.
    fn regular() -> u64 {
        ms("2026-10-27T17:51:02Z")
    }

    fn price(text: &str) -> VenuePrice {
        VenuePrice::parse(text).unwrap()
    }

    fn quote(bid: &str, ask: &str) -> Quote {
        Quote { bid: price(bid), ask: price(ask) }
    }

    /// A valid entry stamped `quote_ms` (exchange time), received 80 ms later.
    fn valid(quote: Quote, quote_ms: u64) -> (Option<Entry>, Validity) {
        valid_at(quote, quote_ms, quote_ms + 80)
    }

    /// A valid entry stamped `quote_ms` (exchange time), received at `received_ms` (this server's clock).
    fn valid_at(quote: Quote, quote_ms: u64, received_ms: u64) -> (Option<Entry>, Validity) {
        (Some(Entry::for_test_at(Some(quote), received_ms, Some(quote_ms), 1)), Validity::Valid)
    }

    /// One coin on both feeds, neither connected.
    fn readings() -> EquityReadings {
        let projection = || FeedProjection { connections: Vec::new(), messages: 0, entries: vec![(None, Validity::NotConnected)] };
        EquityReadings { sip: projection(), boats: projection() }
    }

    fn tick(readings: &EquityReadings, published_ms: u64, instant: Instant) -> EquityTick<'_> {
        EquityTick { readings: Some(readings), published_ms, session: equity_session(published_ms), instant }
    }

    /// TSLA's leg at `published_ms`, `instant` that many ms after `start`.
    fn leg_at(readings: &EquityReadings, windows: &mut EquityBasisWindows, start: (Instant, u64), published_ms: u64) -> Hip3SourceLeg<Hip3EquityLeg> {
        let instant = start.0 + Duration::from_millis(published_ms - start.1);
        equity_leg("xyz:TSLA", EquityPlace::Read(0), tick(readings, published_ms, instant), None, BOOK, windows)
    }

    /// TSLA's leg at `published_ms`, its windows new.
    fn leg(readings: &EquityReadings, published_ms: u64) -> Hip3SourceLeg<Hip3EquityLeg> {
        leg_at(readings, &mut EquityBasisWindows::default(), (Instant::now(), published_ms), published_ms)
    }

    fn code(leg: &Hip3SourceLeg<Hip3EquityLeg>) -> Option<&'static str> {
        match leg {
            Hip3SourceLeg::Listed(Err(withheld)) => Some(withheld.code),
            Hip3SourceLeg::Listed(Ok(_)) | Hip3SourceLeg::Unlisted => None,
        }
    }

    fn ready(leg: Hip3SourceLeg<Hip3EquityLeg>) -> Hip3EquityLeg {
        let Hip3SourceLeg::Listed(Ok(leg)) = leg else { panic!("not ready: {leg:?}") };
        leg
    }

    /// A regular-session SIP quote is the leg: as sent, its receipt and exchange times, the SIP named; BOATS's valid
    /// quote does not stand in then.
    #[test]
    fn in_the_day_the_leg_is_the_sip_quote() {
        let now = regular();
        let mut readings = readings();
        readings.sip.entries[0] = valid(quote("405.765", "405.775"), now - 1_000);
        readings.boats.entries[0] = valid(quote("405.7", "405.9"), now - 500);
        let sip = ready(leg(&readings, now));
        let expected = EquityQuote { feed: EquityFeed::Sip, quote: quote("405.765", "405.775"), received_ms: now - 920, quote_ms: now - 1_000 };
        assert_eq!(sip.quote, expected);
        assert_eq!(sip.basis.unwrap_err().code, "externalBasisSamples");
        readings.sip.entries[0].1 = Validity::NotConnected;
        assert_eq!(code(&leg(&readings, now)), Some("notConnected"), "the SIP down: BOATS's quote is not the day's");
    }

    /// Design 000669 r1 1.3: no session, the entry's validity, no exchange time, a time from the future, a quote of
    /// another session; an unlisted coin and the feeds off; the feeds stopped withhold a ready leg.
    #[test]
    fn each_rule_withholds_with_its_code() {
        let now = regular();
        let mut readings = readings();
        assert_eq!(code(&leg(&readings, ms("2026-10-31T15:00:00Z"))), Some("noSession"), "Saturday");
        assert_eq!(code(&leg(&readings, ms("2026-10-28T00:02:00Z"))), Some("noSession"), "20:02: between the post-market and the night");
        assert_eq!(code(&leg(&readings, now)), Some("notConnected"));
        readings.sip.entries[0] = (Some(Entry::for_test_at(None, now - 10, Some(now - 50), 1)), Validity::NoQuote);
        assert_eq!(code(&leg(&readings, now)), Some("noQuote"));
        readings.sip.entries[0] = (None, Validity::NotQuoted);
        assert_eq!(code(&leg(&readings, now)), Some("notQuoted"));
        readings.sip.entries[0] = (Some(Entry::for_test_at(Some(quote("405.765", "405.775")), now - 10, None, 1)), Validity::Valid);
        assert_eq!(code(&leg(&readings, now)), Some("noQuoteTime"));
        // review 000672 P2: the receipt and the exchange time each on its own bound
        readings.sip.entries[0] = valid_at(quote("405.765", "405.775"), now - 50, now);
        assert!(matches!(leg(&readings, now), Hip3SourceLeg::Listed(Ok(_))), "received at the publish time");
        readings.sip.entries[0] = valid_at(quote("405.765", "405.775"), now - 50, now + 1);
        assert_eq!(code(&leg(&readings, now)), Some("receivedAfterPublish"), "received 1 ms after it: the clock stepped back");
        readings.sip.entries[0] = valid_at(quote("405.765", "405.775"), now + 1_000, now);
        assert!(matches!(leg(&readings, now), Hip3SourceLeg::Listed(Ok(_))), "1,000 ms ahead: within a clock's skew");
        readings.sip.entries[0] = valid_at(quote("405.765", "405.775"), now + 1_001, now);
        assert_eq!(code(&leg(&readings, now)), Some("quoteFromFuture"));
        readings.sip.entries[0] = valid(quote("405.765", "405.775"), ms("2026-10-27T13:29:59Z"));
        let outside = leg(&readings, now);
        assert_eq!(code(&outside), Some("quoteOutsideSession"), "the pre-market's last quote, read in the regular session");
        let Hip3SourceLeg::Listed(Err(withheld)) = outside else { unreachable!() };
        assert_eq!(withheld.status, Status::Unavailable);
        assert!(withheld.detail.contains("Pre") && withheld.detail.contains("Regular"), "{}", withheld.detail);
        let instant = Instant::now();
        let mut windows = EquityBasisWindows::default();
        let unlisted = equity_leg("xyz:GOLD", EquityPlace::Unlisted, tick(&readings, now, instant), None, BOOK, &mut windows);
        assert_eq!(unlisted, Hip3SourceLeg::Unlisted);
        let off = EquityTick { readings: None, ..tick(&readings, now, instant) };
        assert_eq!(code(&equity_leg("xyz:BB", EquityPlace::Off, off, None, BOOK, &mut windows)), Some("equityOff"));
        readings.sip.entries[0] = valid(quote("405.765", "405.775"), now - 1_000);
        let stopped = Withheld::unavailable("stopped", "the equity_alpaca shard 0 task failed");
        // the feeds stopped, the caller gives no book to sample (`external::sampled_book`)
        let gone = equity_leg("xyz:TSLA", EquityPlace::Read(0), tick(&readings, now, instant), Some(&stopped), None, &mut windows);
        assert_eq!(code(&gone), Some("stopped"));
    }

    /// Design 000669 r1 1.3: BOATS's quote of 20:02, unchanged since, holds from 20:05; a quote after midnight is the
    /// evening's night; the pre-market reads the SIP again, and a SIP time of the night is none of it.
    #[test]
    fn a_boats_quote_of_20_02_holds_overnight() {
        let (boats_open, night, two_am) = (ms("2026-10-28T00:02:00Z"), ms("2026-10-28T00:05:00Z"), ms("2026-10-28T06:00:00Z"));
        let mut readings = readings();
        readings.boats.entries[0] = valid(quote("405.7", "405.9"), boats_open);
        let first = ready(leg(&readings, night));
        assert_eq!((first.quote.feed, first.quote.quote_ms, first.quote.quote), (EquityFeed::Boats, boats_open, quote("405.7", "405.9")));
        assert_eq!(ready(leg(&readings, two_am)).quote.quote_ms, boats_open, "the evening's quote at 02:00: the same night");
        let after_midnight = ms("2026-10-28T04:05:00Z");
        readings.boats.entries[0] = valid(quote("405.8", "406"), after_midnight);
        assert_eq!(ready(leg(&readings, two_am)).quote.quote_ms, after_midnight, "00:05 Wednesday: Tuesday's night");
        let pre = ms("2026-10-28T08:00:00Z");
        assert_eq!(equity_session(pre).map(|id| id.session), Some(EquitySession::Pre));
        assert_eq!(code(&leg(&readings, pre)), Some("notConnected"), "the pre-market reads the SIP");
        readings.sip.entries[0] = valid(quote("405.7", "405.9"), after_midnight);
        assert_eq!(code(&leg(&readings, pre)), Some("quoteOutsideSession"), "a SIP time of the night is not the pre-market's");
    }

    /// Design 000669 r1 1.3: one window a feed. At 20:05 the leg turns to BOATS, whose window warms up from nothing
    /// while the SIP's still holds the post-market's samples; a withheld leg samples nothing.
    #[test]
    fn the_feeds_switch_with_the_session_and_warm_up_each_window() {
        let (post, night) = (ms("2026-10-27T23:56:00Z"), ms("2026-10-28T00:05:00Z"));
        let start = (Instant::now(), post);
        let mut readings = readings();
        let mut windows = EquityBasisWindows::default();
        readings.sip.entries[0] = valid(quote("405.765", "405.775"), post - 1_000);
        let seconds = u64::try_from(MIN_SAMPLES).unwrap();
        let warm = (0..seconds).map(|second| leg_at(&readings, &mut windows, start, post + 1_000 * second)).last().unwrap();
        let basis = ready(warm).basis.unwrap();
        assert_eq!(basis.samples, MIN_SAMPLES);
        assert!((basis.ratio - (405.76 / 405.77 - 1.0)).abs() < 1e-12, "{}", basis.ratio);
        assert_eq!(windows.boats.published().unwrap_err().detail, "0 of the 120 samples needed over the last 600 s");
        readings.boats.entries[0] = valid(quote("405.7", "405.9"), ms("2026-10-28T00:02:00Z"));
        readings.sip.entries[0].1 = Validity::NotConnected;
        let first = ready(leg_at(&readings, &mut windows, start, night));
        assert_eq!((first.quote.feed, first.basis.unwrap_err().detail.as_str()), (EquityFeed::Boats, "1 of the 120 samples needed over the last 600 s"));
        assert_eq!(windows.sip.published().unwrap().samples, MIN_SAMPLES, "the SIP's window keeps its own samples, none of BOATS's");
        let warm = (1..seconds).map(|second| leg_at(&readings, &mut windows, start, night + 1_000 * second)).last().unwrap();
        let basis = ready(warm).basis.unwrap();
        assert_eq!(basis.samples, MIN_SAMPLES);
        assert!((basis.ratio - (405.76 / 405.8 - 1.0)).abs() < 1e-12, "BOATS's own midpoint: {}", basis.ratio);
        let mut windows = EquityBasisWindows::default();
        readings.boats.entries[0].1 = Validity::NotQuoted;
        leg_at(&readings, &mut windows, start, night);
        assert!(windows.boats.published().unwrap_err().detail.starts_with("0 of"));
    }
}
