//! One feed connection's latest quotes and state (design 000454 r2 revision 4). The feed task overwrites a market's quote as
//! it arrives and never waits for a reader; a reader copies the slot. Quotes are state, not events: an overwritten one
//! loses nothing.
//!
//! Each quote carries the connection epoch it arrived on and holds only while that connection is the current, connected
//! one: a disconnect, or a reconnect, between two reads leaves this connection's earlier quotes invalid until its market quotes anew
//! (r2 review, acceptance point 1). A new connection, a subscription ack or a pong revives nothing. A polled feed's
//! quotes also expire by age. `Stopped` is final until restart: a write after it - a task's poll still running while
//! its cancellation lands (review 000455 r2) - changes nothing.

use std::sync::{Mutex, MutexGuard, PoisonError};

use super::{price::VenuePrice, sources::Feed};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Quote {
    pub(crate) bid: VenuePrice,
    pub(crate) ask: VenuePrice,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Connection {
    /// Not connected since start.
    Connecting,
    Connected { since_ms: u64 },
    /// Lost: being reconnected.
    Disconnected { reason: String, at_ms: u64 },
    /// A contract error stopped this connection until restart.
    Stopped { reason: String, at_ms: u64 },
}

/// A market's latest observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Entry {
    /// `None`: the venue shows no two-sided quote for the market now (an empty book side).
    pub(crate) quote: Option<Quote>,
    pub(crate) received_ms: u64,
    /// The venue's own time of the quote, where its feed gives one.
    pub(crate) event_ms: Option<u64>,
    epoch: u64,
}

impl Entry {
    /// The connection it was observed on: another one after an outage (the HIP-3 SIP midpoints' continuity).
    pub(crate) const fn epoch(&self) -> u64 {
        self.epoch
    }
}

#[cfg(test)]
impl Entry {
    /// An observation on the connection `epoch`.
    pub(crate) const fn for_test(quote: Option<Quote>, received_ms: u64, epoch: u64) -> Self {
        Self { quote, received_ms, event_ms: None, epoch }
    }

    /// The same with the venue's time of the quote.
    pub(crate) const fn for_test_at(quote: Option<Quote>, received_ms: u64, event_ms: Option<u64>, epoch: u64) -> Self {
        Self { quote, received_ms, event_ms, epoch }
    }
}

/// Whether a market's latest observation is a quote that holds now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Validity {
    Valid,
    /// Nothing received on the current connection yet.
    NotQuoted,
    /// The venue shows no two-sided quote.
    NoQuote,
    /// This market's connection is not connected.
    NotConnected,
    /// A polled quote older than the feed's expiry.
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FeedReading {
    pub(crate) connection: Connection,
    /// Frames the feed parsed since start.
    pub(crate) messages: u64,
    /// By market (the feed's `FeedMarkets` order): its latest observation of any connection, and whether it holds.
    pub(crate) entries: Vec<(Option<Entry>, Validity)>,
}

#[derive(Debug)]
struct SlotState {
    epoch: u64,
    connection: Connection,
    messages: u64,
    entries: Vec<Option<Entry>>,
}

impl SlotState {
    const fn is_stopped(&self) -> bool {
        matches!(self.connection, Connection::Stopped { .. })
    }
}

#[derive(Debug)]
pub(crate) struct FeedSlot {
    pub(crate) feed: Feed,
    /// A polled feed's quote age limit; `None` for a WS feed (its venue pushes on change: a quiet market sends nothing).
    expires_after_ms: Option<u64>,
    state: Mutex<SlotState>,
}

impl FeedSlot {
    pub(crate) fn new(feed: Feed, markets: usize, expires_after_ms: Option<u64>) -> Self {
        let state = SlotState { epoch: 0, connection: Connection::Connecting, messages: 0, entries: vec![None; markets] };
        Self { feed, expires_after_ms, state: Mutex::new(state) }
    }

    fn lock(&self) -> MutexGuard<'_, SlotState> {
        // every write is one assignment: a panic elsewhere cannot leave the state half-written
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A new connection (or a poll answering after a failure): the epoch its quotes carry. On a stopped feed it changes
    /// nothing, and the epoch it gives takes no write.
    pub(crate) fn connected(&self, now_ms: u64) -> u64 {
        let mut state = self.lock();
        if state.is_stopped() {
            return state.epoch;
        }
        state.epoch += 1;
        state.connection = Connection::Connected { since_ms: now_ms };
        state.epoch
    }

    pub(crate) fn disconnected(&self, reason: String, now_ms: u64) {
        let mut state = self.lock();
        if !state.is_stopped() {
            state.connection = Connection::Disconnected { reason, at_ms: now_ms };
        }
    }

    /// Final until restart; the first stop's reason stands.
    pub(crate) fn stopped(&self, reason: String, now_ms: u64) {
        let mut state = self.lock();
        if !state.is_stopped() {
            state.connection = Connection::Stopped { reason, at_ms: now_ms };
        }
    }

    /// One frame's observations on connection `epoch`, by market index; a late write of an older connection, or any
    /// write to a stopped feed, is dropped.
    pub(crate) fn record(&self, epoch: u64, received_ms: u64, observations: impl IntoIterator<Item = (usize, Option<Quote>, Option<u64>)>) {
        let mut state = self.lock();
        if state.epoch != epoch || state.is_stopped() {
            return;
        }
        state.messages += 1;
        for (index, quote, event_ms) in observations {
            state.entries[index] = Some(Entry { quote, received_ms, event_ms, epoch });
        }
    }

    pub(crate) fn read(&self, now_ms: u64) -> FeedReading {
        let state = self.lock();
        let connected = matches!(state.connection, Connection::Connected { .. });
        let validity = |entry: &Option<Entry>| match entry {
            _ if !connected => Validity::NotConnected,
            Some(entry) if entry.epoch != state.epoch => Validity::NotQuoted,
            None => Validity::NotQuoted,
            Some(Entry { quote: None, .. }) => Validity::NoQuote,
            Some(entry) if self.expires_after_ms.is_some_and(|limit| now_ms.saturating_sub(entry.received_ms) > limit) => Validity::Expired,
            Some(_) => Validity::Valid,
        };
        FeedReading {
            connection: state.connection.clone(),
            messages: state.messages,
            entries: state.entries.iter().map(|entry| (*entry, validity(entry))).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_marks::sources::{Kind, Venue};

    fn quote(bid: &str, ask: &str) -> Quote {
        Quote { bid: VenuePrice::parse(bid).unwrap(), ask: VenuePrice::parse(ask).unwrap() }
    }

    fn validity(slot: &FeedSlot, now_ms: u64) -> Vec<Validity> {
        slot.read(now_ms).entries.into_iter().map(|(_, validity)| validity).collect()
    }

    const FEED: Feed = Feed { kind: Kind::Spot, venue: Venue::Okx };

    /// r2 review acceptance 1: the reader pauses, the connection drops and comes back, and only one market quotes on
    /// the new connection; the other's quote of the old connection stays invalid.
    #[test]
    fn a_reconnect_between_reads_keeps_old_quotes_invalid_until_quoted_anew() {
        let slot = FeedSlot::new(FEED, 2, None);
        assert_eq!(validity(&slot, 0), [Validity::NotConnected, Validity::NotConnected]);
        let first = slot.connected(1);
        slot.record(first, 2, [(0, Some(quote("100", "101")), Some(2)), (1, Some(quote("5", "6")), None)]);
        assert_eq!(validity(&slot, 3), [Validity::Valid, Validity::Valid]);
        slot.disconnected("ConnectionReset".to_string(), 4);
        let second = slot.connected(5);
        slot.record(second, 6, [(0, Some(quote("100.5", "101")), Some(6))]);
        let reading = slot.read(7);
        assert_eq!(reading.entries[0], (Some(Entry { quote: Some(quote("100.5", "101")), received_ms: 6, event_ms: Some(6), epoch: second }), Validity::Valid));
        assert_eq!(reading.entries[1].1, Validity::NotQuoted, "quoted on the lost connection only");
        assert_eq!(reading.entries[1].0.map(|entry| entry.received_ms), Some(2), "the old observation is still shown");
        // a late write of the first session changes nothing
        slot.record(first, 8, [(1, Some(quote("5", "6")), None)]);
        assert_eq!(validity(&slot, 9), [Validity::Valid, Validity::NotQuoted]);
        assert_eq!(slot.read(9).messages, 2);
    }

    #[test]
    fn a_disconnected_or_stopped_feed_and_an_empty_side_hold_nothing() {
        let slot = FeedSlot::new(FEED, 2, None);
        let epoch = slot.connected(1);
        slot.record(epoch, 2, [(0, Some(quote("100", "101")), None), (1, None, None)]);
        assert_eq!(validity(&slot, 3), [Validity::Valid, Validity::NoQuote]);
        // a quiet WS market's quote holds however old it is
        assert_eq!(validity(&slot, 3_600_000), [Validity::Valid, Validity::NoQuote]);
        slot.disconnected("ReadTimeout".to_string(), 4);
        assert_eq!(validity(&slot, 5), [Validity::NotConnected, Validity::NotConnected]);
        slot.stopped("subscription refused".to_string(), 6);
        assert_eq!(slot.read(7).connection, Connection::Stopped { reason: "subscription refused".to_string(), at_ms: 6 });
    }

    /// Review 000455 r2: a task's poll still running after its feed was stopped (a cancellation landing late) writes
    /// nothing - no reconnect, no quote, no other reason.
    #[test]
    fn a_stopped_feed_takes_no_later_write() {
        let slot = FeedSlot::new(FEED, 1, None);
        let epoch = slot.connected(1);
        slot.record(epoch, 2, [(0, Some(quote("100", "101")), None)]);
        slot.stopped("the perp_okx task failed".to_string(), 3);
        let late = slot.connected(4);
        slot.record(late, 5, [(0, Some(quote("100.5", "101")), None)]);
        slot.record(epoch, 5, [(0, Some(quote("100.5", "101")), None)]);
        slot.disconnected("ConnectionReset".to_string(), 6);
        slot.stopped("another reason".to_string(), 7);
        let reading = slot.read(3_600_000);
        assert_eq!(reading.connection, Connection::Stopped { reason: "the perp_okx task failed".to_string(), at_ms: 3 });
        assert_eq!(reading.entries[0].1, Validity::NotConnected);
        assert_eq!(reading.entries[0].0.map(|entry| entry.received_ms), Some(2), "the stopped feed's last observation, untouched");
        assert_eq!(reading.messages, 1);
    }

    #[test]
    fn a_polled_quote_expires_by_age() {
        let slot = FeedSlot::new(FEED, 1, Some(5_000));
        let epoch = slot.connected(0);
        slot.record(epoch, 1_000, [(0, Some(quote("100", "101")), None)]);
        assert_eq!(validity(&slot, 6_000), [Validity::Valid]);
        assert_eq!(validity(&slot, 6_001), [Validity::Expired]);
    }
}
