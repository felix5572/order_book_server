//! The external quote frames' way out of one connection (hyperliquid-trade review 000676 r1 P2), counted at each step
//! apart: the tick publishes a frame (`orderbook_external_quote_published_frames_total{channel,cause}`), the session
//! hands it to the connection's queue (`orderbook_external_quote_ws_enqueued_frames_total{channel}`, once accepted),
//! and the writer writes it to the socket (`orderbook_external_quote_ws_written_frames_total{channel}` and
//! `orderbook_external_quote_ws_written_bytes_total{channel}`, once the write succeeded). The writer knows a quote
//! frame by its place in the queue, never by reading it: the session, the queue's one producer, notes each quote frame
//! with the place it takes before handing it over, and the writer, its one consumer, counts the frames it takes.
//!
//! A quote frame is judged with its queue slot in hand: when the slot is free, a tick withdrawn meanwhile - its last
//! snapshot naming why or its channel closed - hands nothing more (`send_quote`, review 000676 r2 P1); frames handed
//! before drain in order.
//!
//! A change frame the tick sampled (`QuoteSample`, at most one a channel every 100 ms) is timed on its way, by channel
//! and stage, in `orderbook_external_quote_sample_seconds{channel,stage}`:
//! - `receipt_to_publish`: from the newest receipt of the quotes that moved it to its publish time - the tick's, once a
//!   sampled frame; this server's wall clock, the receipt in whole milliseconds;
//! - `publish_to_enqueue`: from its publish to the session handing it to the connection's queue (monotonic);
//! - `queue`: from that hand-off - a wait for a free slot of a full queue included - to the writer taking it
//!   (monotonic): its time in the queue;
//! - `write`: from the writer taking it to its socket write's success (monotonic);
//! - `receipt_to_written`: from the receipt to the write's success, end to end (wall clock: the receipt's
//!   milliseconds floored, so up to 1 ms over).
//!
//! The last four are each connection's that wrote the frame.

use std::{
    collections::VecDeque,
    sync::{
        Mutex, MutexGuard, PoisonError,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use super::Outbound;
use crate::{
    local_marks::quote_tick::snapshot::{QuoteChannel, QuoteEntry, QuoteSample},
    metrics::{EXTERNAL_QUOTE_SAMPLE_SECONDS, EXTERNAL_QUOTE_WS_ENQUEUED_FRAMES_TOTAL, EXTERNAL_QUOTE_WS_WRITTEN_BYTES_TOTAL, EXTERNAL_QUOTE_WS_WRITTEN_FRAMES_TOTAL},
};

/// A quote frame handed to the queue and not yet taken by the writer.
#[derive(Debug)]
struct PendingQuote {
    /// Its place among the connection's data frames, in the queue's order.
    place: u64,
    channel: QuoteChannel,
    bytes: usize,
    /// A sampled frame's start, and when the session handed it over.
    sample: Option<(QuoteSample, Instant)>,
}

/// A connection's quote frames between the session and the writer.
#[derive(Debug, Default)]
pub(super) struct QuoteDelivery {
    /// The data frames the queue accepted, every channel's: the place the next one takes.
    enqueued: AtomicU64,
    /// The data frames the writer took: the place of the next one it takes.
    taken: AtomicU64,
    /// The quote frames handed over and not yet taken, in place order; at most the queue's depth.
    pending: Mutex<VecDeque<PendingQuote>>,
    /// Their count: the writer locks nothing on a connection without one.
    pending_count: AtomicUsize,
}

impl QuoteDelivery {
    fn lock(&self) -> MutexGuard<'_, VecDeque<PendingQuote>> {
        // every change is one push or pop: a panic elsewhere cannot leave the queue half-written
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The queue accepted a data frame (`Outbound::send_payload`, the queue's one producer).
    pub(super) fn payload_enqueued(&self) {
        self.enqueued.fetch_add(1, Ordering::SeqCst);
    }

    /// Notes the quote frame about to be enqueued - its slot in hand -, at the place the next data frame takes; the
    /// session began handing it over at `handed_at`.
    fn note(&self, channel: QuoteChannel, entry: &QuoteEntry, handed_at: Instant) {
        let place = self.enqueued.load(Ordering::SeqCst);
        self.lock().push_back(PendingQuote { place, channel, bytes: entry.frame.len(), sample: entry.sample.map(|sample| (sample, handed_at)) });
        self.pending_count.fetch_add(1, Ordering::SeqCst);
    }

    /// The writer took a data frame at `taken_at`: the quote frame it is, if one.
    pub(super) fn taken(&self, taken_at: Instant) -> Option<TakenQuote> {
        let place = self.taken.fetch_add(1, Ordering::SeqCst);
        if self.pending_count.load(Ordering::SeqCst) == 0 {
            return None;
        }
        let mut pending = self.lock();
        let quote = match pending.front() {
            Some(front) if front.place == place => pending.pop_front(),
            // noted in place order, each just before its frame is enqueued: none is behind the writer
            Some(front) => {
                assert!(front.place > place, "the quote frame noted at place {} is behind the writer's {place}", front.place);
                None
            }
            None => None,
        };
        drop(pending);
        let quote = quote?;
        self.pending_count.fetch_sub(1, Ordering::SeqCst);
        Some(TakenQuote { channel: quote.channel, bytes: quote.bytes, sample: quote.sample, taken_at })
    }
}

/// A quote frame the writer took.
#[derive(Debug)]
pub(super) struct TakenQuote {
    channel: QuoteChannel,
    bytes: usize,
    sample: Option<(QuoteSample, Instant)>,
    taken_at: Instant,
}

/// A sampled frame's stages after its publish, on one connection.
#[derive(Debug, Clone, Copy, PartialEq)]
struct WrittenStages {
    queue: Duration,
    write: Duration,
    receipt_to_written: Duration,
}

impl TakenQuote {
    /// Its socket write succeeded at `written_at` (monotonic), `written_wall_us` this server's wall clock then (µs).
    pub(super) fn written(self, written_at: Instant, written_wall_us: u64) {
        let channel = self.channel.name();
        EXTERNAL_QUOTE_WS_WRITTEN_FRAMES_TOTAL.with_label_values(&[channel]).inc();
        EXTERNAL_QUOTE_WS_WRITTEN_BYTES_TOTAL.with_label_values(&[channel]).inc_by(self.bytes as u64);
        let Some(stages) = self.stages(written_at, written_wall_us) else { return };
        for (stage, elapsed) in [("queue", stages.queue), ("write", stages.write), ("receipt_to_written", stages.receipt_to_written)] {
            EXTERNAL_QUOTE_SAMPLE_SECONDS.with_label_values(&[channel, stage]).observe(elapsed.as_secs_f64());
        }
    }

    /// A sampled frame's stages; `None` for another.
    fn stages(&self, written_at: Instant, written_wall_us: u64) -> Option<WrittenStages> {
        let (sample, handed_at) = self.sample?;
        Some(WrittenStages {
            queue: self.taken_at.saturating_duration_since(handed_at),
            write: written_at.saturating_duration_since(self.taken_at),
            receipt_to_written: Duration::from_micros(written_wall_us.saturating_sub(sample.received_ms * 1_000)),
        })
    }
}

/// What became of a quote frame handed to the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Handed {
    /// Enqueued, noted for the writer.
    Enqueued,
    /// Not enqueued: the tick withdrew, found with the frame's slot in hand.
    Withdrawn,
    /// The connection is doomed: the writer gone, or no slot in time.
    Dropped,
}

/// Hands the quote frame of `channel` to the connection's queue: its slot taken first - waited for while the queue is
/// full -, then `withdrawn` asked with the slot in hand, and the frame enqueued only while the tick stands (review
/// 000676 r2 P1); noted for the writer, counted enqueued, a sampled one's `publish_to_enqueue` then.
pub(super) async fn send_quote(outbound: &Outbound, channel: QuoteChannel, entry: &QuoteEntry, withdrawn: impl FnOnce() -> bool) -> Handed {
    let handed_at = Instant::now();
    let Some(slot) = outbound.data_slot().await else { return Handed::Dropped };
    if withdrawn() {
        return Handed::Withdrawn;
    }
    outbound.diagnostics.quotes.note(channel, entry, handed_at);
    outbound.enqueue(slot, entry.frame.clone());
    EXTERNAL_QUOTE_WS_ENQUEUED_FRAMES_TOTAL.with_label_values(&[channel.name()]).inc();
    if let Some(sample) = entry.sample {
        let elapsed = handed_at.saturating_duration_since(sample.published_at.into_std());
        EXTERNAL_QUOTE_SAMPLE_SECONDS.with_label_values(&[channel.name(), "publish_to_enqueue"]).observe(elapsed.as_secs_f64());
    }
    Handed::Enqueued
}

/// This server's wall clock, µs; a host clock before 1970 or past u64 µs is broken (`local_marks::now_ms`' rule).
#[expect(clippy::expect_used, reason = "a host clock before 1970 or past u64 µs is broken; quote ages depend on it")]
pub(super) fn wall_us() -> u64 {
    let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH).expect("the host clock is after 1970");
    u64::try_from(since_epoch.as_micros()).expect("unix µs fit u64")
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tokio::sync::mpsc;

    use super::*;

    fn entry(frame: &'static str, sample: Option<QuoteSample>) -> QuoteEntry {
        QuoteEntry { version: 1, epoch: 1, frame: Bytes::from_static(frame.as_bytes()), sample }
    }

    /// Review 000676 r1 P2: the writer knows the quote frames by their place in the queue - among others' frames,
    /// none read -, each with its channel and size; one found withdrawn with its slot in hand, or refused by a queue
    /// whose writer is gone, is neither enqueued nor noted (r2 P1).
    #[tokio::test]
    async fn the_writer_knows_the_quote_frames_by_their_place() {
        let (data_tx, mut data_rx) = mpsc::channel(8);
        let (pong_tx, _pong_rx) = mpsc::channel(1);
        let outbound = Outbound::for_test(data_tx, pong_tx);
        let sample = QuoteSample { received_ms: 1_000, published_ms: 1_002, published_at: tokio::time::Instant::now() };
        assert_eq!(send_quote(&outbound, QuoteChannel::Main, &entry("btc", Some(sample)), || false).await, Handed::Enqueued);
        assert_eq!(send_quote(&outbound, QuoteChannel::Main, &entry("eth", None), || true).await, Handed::Withdrawn);
        assert!(outbound.send_frame(Bytes::from_static(b"l2Book")).await);
        assert_eq!(send_quote(&outbound, QuoteChannel::Hip3, &entry("gold!", None), || false).await, Handed::Enqueued);
        let delivery = &outbound.diagnostics.quotes;
        let mut taken = Vec::new();
        while let Ok(frame) = data_rx.try_recv() {
            taken.push((frame, delivery.taken(Instant::now()).map(|quote| (quote.channel, quote.bytes, quote.sample.is_some()))));
        }
        let expected = [
            (Bytes::from_static(b"btc"), Some((QuoteChannel::Main, 3, true))),
            (Bytes::from_static(b"l2Book"), None),
            (Bytes::from_static(b"gold!"), Some((QuoteChannel::Hip3, 5, false))),
        ];
        assert_eq!(taken, expected);
        drop(data_rx);
        assert_eq!(send_quote(&outbound, QuoteChannel::Main, &entry("eth", None), || false).await, Handed::Dropped, "the writer gone");
        assert_eq!((delivery.lock().len(), delivery.pending_count.load(Ordering::SeqCst)), (0, 0), "none noted but those taken");
    }

    /// A sampled frame's stages: its time in the queue from the hand-off to the writer's taking it, the write, and the
    /// receipt to the written, end to end on the wall clock.
    #[test]
    fn a_sampled_frames_stages_run_from_its_hand_off_and_receipt() {
        let handed_at = Instant::now();
        let sample = QuoteSample { received_ms: 1_000, published_ms: 1_002, published_at: tokio::time::Instant::now() };
        let taken = TakenQuote { channel: QuoteChannel::Main, bytes: 3, sample: Some((sample, handed_at)), taken_at: handed_at + Duration::from_millis(26) };
        let stages = taken.stages(handed_at + Duration::from_micros(26_300), 1_004_500).unwrap();
        assert_eq!(stages, WrittenStages { queue: Duration::from_millis(26), write: Duration::from_micros(300), receipt_to_written: Duration::from_micros(4_500) });
        let plain = TakenQuote { sample: None, ..taken };
        assert_eq!(plain.stages(handed_at, 1_004_500), None);
    }
}
