//! Which coins an external quote tick publishes (hyperliquid-trade design 000673 r4, r3 P2). Each tick judges every
//! subscribed coin anew at its publish time, but a coin's frame goes out only:
//! - when its decision fingerprint changed (`Fingerprint`): per leg, its status and reason code, its source - the
//!   venues the main dex's medians took, the US stock's feed -, its bid and ask, and its basis' status and ratio; not
//!   for the times a frame carries (`publishedTsMs`, `receivedTsMs`, `quoteTsMs`) nor its reasons' text, which the next
//!   change or heartbeat brings;
//! - on the first tick of a subscription epoch, whatever the fingerprint (`subscribers`);
//! - for every subscribed coin every `HEARTBEAT_TICKS` ticks (the heartbeat, 50 ms).
//!
//! A frame is serialized once, when published, and carries the tick's version: one count of the publishing ticks,
//! rising, so a coin's frames rise in version.
//!
//! A frame published because a leg's quote moved - ready now, and not before, or from other sources, or at another
//! bid or ask - may be sampled for the timing of its way out (`QuoteSample`, review 000676 r1 P2): at most one a
//! channel every `SAMPLE_INTERVAL`, its start the newest receipt among the moved legs' quotes received since the last
//! tick's publish time - a quote this tick is the first to judge. A move with none such - a venue gone or expired, an
//! old quote valid again (review 000676 r2 P2), the basis or a withheld leg's reason alone - is published and counted
//! as any, but not sampled, and takes no sample's place: its newest receipt may be long past.

use std::time::Duration;

use bytes::Bytes;
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::time::Instant;

use super::snapshot::{QuoteChannel, QuoteEntry, QuoteKey, QuoteSample, QuoteSnapshot};
use crate::local_marks::{
    external_basis::ExternalBasis,
    hip3::{
        external::{Hip3ExternalCoin, Hip3SourceLeg},
        external_equity::Hip3EquityLeg,
        protocol::EquityFeed,
    },
    price::VenuePrice,
    snapshot::{ExternalLeg, Status, Withheld},
    sources::Venue,
};

/// Ticks between two heartbeats: 50 ms of 5 ms ticks.
pub(crate) const HEARTBEAT_TICKS: u64 = 10;
/// The least time between two sampled frames of a channel: at most 10 a second, whatever the coins' changes, each
/// timed on every connection that writes it.
pub(crate) const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);

/// A coin's legs as one tick judged them.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Evaluated {
    Main(Result<ExternalLeg, Withheld>),
    Hip3(Hip3ExternalCoin),
}

/// Why a frame was published.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Cause {
    Changed,
    /// The first tick of a subscription epoch.
    First,
    Heartbeat,
}

impl Cause {
    /// The metrics' label.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Changed => "changed",
            Self::First => "first",
            Self::Heartbeat => "heartbeat",
        }
    }
}

/// What a coin's frame decides on.
#[derive(Debug, Clone, PartialEq)]
enum Fingerprint {
    Main(LegFingerprint),
    Hip3 { external: LegFingerprint, equity: LegFingerprint },
}

#[derive(Debug, Clone, PartialEq)]
enum LegFingerprint {
    Unlisted,
    Withheld { status: Status, code: &'static str },
    Ready { source: Source, bid: VenuePrice, ask: VenuePrice, basis: BasisFingerprint },
}

/// Where a ready leg's quote is from: the venues of its medians, in their listings' order; the US stock feed.
#[derive(Debug, Clone, PartialEq)]
enum Source {
    Venues(Vec<Venue>),
    Feed(EquityFeed),
}

#[derive(Debug, Clone, PartialEq)]
enum BasisFingerprint {
    /// The ratio's bits: any change of it is one.
    Ready { ratio_bits: u64 },
    Withheld { status: Status, code: &'static str },
}

impl Fingerprint {
    fn of(legs: &Evaluated) -> Self {
        match legs {
            Evaluated::Main(leg) => Self::Main(listed(leg.as_ref(), external_leg)),
            Evaluated::Hip3(coin) => Self::Hip3 { external: source_leg(&coin.external, external_leg), equity: source_leg(&coin.equity, equity_leg) },
        }
    }
}

fn listed<T>(leg: Result<&T, &Withheld>, ready: impl FnOnce(&T) -> LegFingerprint) -> LegFingerprint {
    leg.map_or_else(|withheld| LegFingerprint::Withheld { status: withheld.status, code: withheld.code }, ready)
}

fn source_leg<T>(leg: &Hip3SourceLeg<T>, ready: impl FnOnce(&T) -> LegFingerprint) -> LegFingerprint {
    match leg {
        Hip3SourceLeg::Unlisted => LegFingerprint::Unlisted,
        Hip3SourceLeg::Listed(leg) => listed(leg.as_ref(), ready),
    }
}

fn external_leg(leg: &ExternalLeg) -> LegFingerprint {
    let source = Source::Venues(leg.quotes.venues.iter().map(|quote| quote.feed.venue).collect());
    LegFingerprint::Ready { source, bid: leg.quotes.bid, ask: leg.quotes.ask, basis: basis(&leg.basis) }
}

fn equity_leg(leg: &Hip3EquityLeg) -> LegFingerprint {
    let quote = &leg.quote;
    LegFingerprint::Ready { source: Source::Feed(quote.feed), bid: quote.quote.bid, ask: quote.quote.ask, basis: basis(&leg.basis) }
}

fn basis(basis: &Result<ExternalBasis, Withheld>) -> BasisFingerprint {
    basis.as_ref().map_or_else(
        |withheld| BasisFingerprint::Withheld { status: withheld.status, code: withheld.code },
        |basis| BasisFingerprint::Ready { ratio_bits: basis.ratio.to_bits() },
    )
}

/// The leg's quote moved: ready now, and not ready before, or from other sources, or at another bid or ask.
fn moved(was: &LegFingerprint, now: &LegFingerprint) -> bool {
    match (was, now) {
        (LegFingerprint::Ready { source: was_source, bid: was_bid, ask: was_ask, .. }, LegFingerprint::Ready { source, bid, ask, .. }) => {
            (was_source, was_bid, was_ask) != (source, bid, ask)
        }
        (LegFingerprint::Unlisted | LegFingerprint::Withheld { .. }, LegFingerprint::Ready { .. }) => true,
        (_, LegFingerprint::Unlisted | LegFingerprint::Withheld { .. }) => false,
    }
}

const fn ready<T>(leg: &Hip3SourceLeg<T>) -> Option<&T> {
    match leg {
        Hip3SourceLeg::Listed(Ok(leg)) => Some(leg),
        Hip3SourceLeg::Listed(Err(_)) | Hip3SourceLeg::Unlisted => None,
    }
}

/// The newest receipt after `since_ms` - the last tick's publish time, every quote received by then judged already -
/// among the quotes of the legs whose quote moved from `was` to `now`; `None` when none moved, or none of their quotes
/// is that new.
fn moved_received_ms(legs: &Evaluated, was: &Fingerprint, now: &Fingerprint, since_ms: u64) -> Option<u64> {
    let fresh = |received_ms: u64| (received_ms > since_ms).then_some(received_ms);
    let newest = |leg: &ExternalLeg| leg.quotes.venues.iter().filter_map(|quote| fresh(quote.received_ms)).max();
    match (legs, was, now) {
        (Evaluated::Main(leg), Fingerprint::Main(was), Fingerprint::Main(now)) => moved(was, now).then(|| leg.as_ref().ok().and_then(newest)).flatten(),
        (Evaluated::Hip3(coin), Fingerprint::Hip3 { external: was_external, equity: was_equity }, Fingerprint::Hip3 { external, equity }) => {
            let binance = moved(was_external, external).then(|| ready(&coin.external).and_then(newest)).flatten();
            let stock = moved(was_equity, equity).then(|| ready(&coin.equity).and_then(|leg| fresh(leg.quote.received_ms))).flatten();
            binance.max(stock)
        }
        _ => unreachable!("a coin's fingerprints are of its channel"),
    }
}

/// What one tick published.
#[derive(Debug)]
pub(crate) struct Publication {
    /// The snapshot to send: a frame published or a coin left; `None` when nothing changed.
    pub(crate) snapshot: Option<QuoteSnapshot>,
    /// The coins judged: the subscribed ones.
    pub(crate) evaluated: usize,
    /// Each coin published, and why.
    pub(crate) published: Vec<(QuoteKey, Cause)>,
    /// The frames sampled, of the published ones.
    pub(crate) samples: Vec<(QuoteKey, QuoteSample)>,
}

impl Publication {
    /// The coins published for a change or a new epoch, the heartbeat's apart.
    pub(crate) fn changed(&self) -> usize {
        self.published.iter().filter(|(_, cause)| *cause != Cause::Heartbeat).count()
    }
}

/// A coin's last published frame and what it decided on.
#[derive(Debug)]
struct Published {
    fingerprint: Fingerprint,
    entry: QuoteEntry,
}

/// The tick's publishing state across ticks.
#[derive(Debug, Default)]
pub(crate) struct Publisher {
    ticks: u64,
    /// The last publishing tick's version.
    version: u64,
    published: FxHashMap<QuoteKey, Published>,
    /// When each channel's last sampled frame was published (`SAMPLE_INTERVAL`), the main dex's then the HIP-3's.
    sampled_at: [Option<Instant>; 2],
    /// The last tick's publish time (wall clock, ms): a quote received after it is this tick's to judge first.
    last_published_ms: Option<u64>,
}

impl Publisher {
    /// This tick's subscribed coins, each with the epoch read before judging it and its legs, judged at the publish
    /// time `published` (wall clock ms, monotonic); `frame` serializes a coin's legs. The coins not among them leave
    /// the snapshot.
    pub(crate) fn publish(
        &mut self,
        evaluated: Vec<(QuoteKey, u32, Evaluated)>,
        published: (u64, Instant),
        frame: impl Fn(QuoteKey, &Evaluated) -> Bytes,
    ) -> Publication {
        let heartbeat = self.ticks.is_multiple_of(HEARTBEAT_TICKS);
        self.ticks += 1;
        let subscribed: FxHashSet<QuoteKey> = evaluated.iter().map(|(key, _, _)| *key).collect();
        let before = self.published.len();
        self.published.retain(|key, _| subscribed.contains(key));
        let left = self.published.len() != before;
        let version = self.version + 1;
        let (mut causes, mut samples) = (Vec::new(), Vec::new());
        let count = evaluated.len();
        let since_ms = self.last_published_ms.replace(published.0);
        for (key, epoch, legs) in evaluated {
            let fingerprint = Fingerprint::of(&legs);
            let was = self.published.get(&key).filter(|last| last.entry.epoch == epoch).map(|last| &last.fingerprint);
            let Some(cause) = cause(was, &fingerprint, heartbeat) else { continue };
            // a changed frame was published before in its epoch: a tick ran before this one
            let moved = was.filter(|_| cause == Cause::Changed).zip(since_ms).and_then(|(was, since_ms)| moved_received_ms(&legs, was, &fingerprint, since_ms));
            let sample = moved.filter(|_| self.sample_due(key.channel, published.1)).map(|received_ms| QuoteSample {
                received_ms,
                published_ms: published.0,
                published_at: published.1,
            });
            let entry = QuoteEntry { version, epoch, frame: frame(key, &legs), sample };
            self.published.insert(key, Published { fingerprint, entry });
            causes.push((key, cause));
            samples.extend(sample.map(|sample| (key, sample)));
        }
        if !causes.is_empty() {
            self.version = version;
        }
        let changed = left || !causes.is_empty();
        let snapshot = changed.then(|| QuoteSnapshot::of(self.published.iter().map(|(key, last)| (*key, last.entry.clone()))));
        Publication { snapshot, evaluated: count, published: causes, samples }
    }

    /// Whether a frame of `channel` published at `at` is sampled: none of it for `SAMPLE_INTERVAL` before. A sampled
    /// one starts the interval anew.
    fn sample_due(&mut self, channel: QuoteChannel, at: Instant) -> bool {
        let last = match channel {
            QuoteChannel::Main => &mut self.sampled_at[0],
            QuoteChannel::Hip3 => &mut self.sampled_at[1],
        };
        let due = last.is_none_or(|last| at.saturating_duration_since(last) >= SAMPLE_INTERVAL);
        if due {
            *last = Some(at);
        }
        due
    }
}

/// Why a coin's frame is published, `was` its fingerprint last published in its subscription epoch.
fn cause(was: Option<&Fingerprint>, fingerprint: &Fingerprint, heartbeat: bool) -> Option<Cause> {
    let Some(was) = was else { return Some(Cause::First) };
    if was != fingerprint {
        return Some(Cause::Changed);
    }
    heartbeat.then_some(Cause::Heartbeat)
}
