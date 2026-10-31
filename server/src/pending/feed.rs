//! A node stream the pending thread follows - the mempool (`mempool`), the block log (`blocks`, hyperliquid-trade design
//! 000604 r6 revision 14): each line parsed whole in its watcher thread and handed over in the file's order through a
//! bounded queue that holds the watcher back, never drops a line; its losses counted the moment they are found, ahead
//! of the lines after them, and reported in order (review 000609).

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
};

use tokio::sync::mpsc;

use crate::listeners::order_book::parallel::{LineStream, LossCounts, WatchLoss, spawn_line_watcher};

/// What a feed's watcher sends, in the file's order.
#[derive(Debug)]
pub(crate) enum FeedEvent<T> {
    /// A line read whole.
    Line(T),
    /// A terminated line that does not read (its parser's error): what it held is lost, at no time of its own.
    /// `through` is its own place in the count of unreadable lines.
    Unreadable { error: String, through: u64 },
    /// Lines the reader skipped at its start, or discarded (a cut, overlong or ill-shaped line, a file lost);
    /// `through` is the reader's loss count when it went out: every loss it counted up to then is reported.
    Lost { loss: WatchLoss, through: u64 },
}

/// A feed's events in the file's order, and its counts of what it lost.
pub(crate) struct Feed<T> {
    pub(crate) events: mpsc::Receiver<FeedEvent<T>>,
    pub(crate) losses: FeedLosses,
}

/// What a watcher lost, each count raised the moment the loss is found (review 000609 r1-r2 P2-1): the lines after it,
/// and the event reporting it, may wait behind a full queue. The reader's losses (`LossCounts`: found as soon as known,
/// an overflow included) are reported by `Lost`, each through those the watcher had flagged when it went out (review
/// 000609 r7: not through those merely found, an overflow still queued behind it); the lines that did not read by
/// `Unreadable`. Each count has its own reports, in its own order, so a report never covers a loss found after the
/// ones it reports.
#[derive(Clone, Default)]
pub(crate) struct FeedLosses {
    pub(super) reader: LossCounts,
    unreadable: Arc<AtomicU64>,
}

impl FeedLosses {
    /// (reader losses, unreadable lines) found so far.
    fn found(&self) -> (u64, u64) {
        (self.reader.found.load(Ordering::Acquire), self.unreadable.load(Ordering::Acquire))
    }

    /// A line read: what it holds, or its error counted.
    pub(super) fn event<T>(&self, parsed: Result<T, String>) -> FeedEvent<T> {
        parsed.map_or_else(
            |error| FeedEvent::Unreadable { error, through: self.unreadable.fetch_add(1, Ordering::Release) + 1 },
            FeedEvent::Line,
        )
    }

    pub(super) fn lost<T>(&self, loss: WatchLoss) -> FeedEvent<T> {
        FeedEvent::Lost { loss, through: self.reader.flagged.load(Ordering::Acquire) }
    }
}

/// Starts the watcher of `stream`, each line parsed by `parse`, `queue` events at most waiting; its events come in
/// order on the feed, which closes when the watcher ends (its inotify watch not set up).
pub(crate) fn watch<T: Send + 'static>(
    stream: LineStream,
    queue: usize,
    parse: fn(&str) -> Result<T, String>,
) -> (Feed<T>, thread::JoinHandle<()>) {
    let (tx, events) = mpsc::channel(queue);
    let losses = FeedLosses::default();
    let stream = LineStream { loss_counts: Some(losses.reader.clone()), ..stream };
    let (parsing, reporting) = (losses.clone(), losses.clone());
    let wrap = move |line: String| parsing.event(parse(&line));
    let lost = move |loss| reporting.lost(loss);
    let watcher = spawn_line_watcher(stream, tx, None, wrap, lost, None);
    (Feed { events, losses }, watcher)
}

/// A loss's metric label.
pub(crate) const fn loss_label(loss: WatchLoss) -> &'static str {
    match loss {
        WatchLoss::Positioning => "positioning",
        WatchLoss::Discard => "discard",
    }
}

/// The pending thread's side of a feed: the losses its events taken so far reported.
pub(crate) struct FeedReports {
    losses: FeedLosses,
    /// (reader losses, unreadable lines) the events taken so far reported through.
    reported: (u64, u64),
}

impl FeedReports {
    pub(crate) const fn new(losses: FeedLosses) -> Self {
        Self { losses, reported: (0, 0) }
    }

    /// Whether the watcher found a loss no event taken yet reports: what was taken since may follow it in the file.
    pub(crate) fn loss_unreported(&self) -> bool {
        let (reader, unreadable) = self.losses.found();
        reader > self.reported.0 || unreadable > self.reported.1
    }

    /// The report `event` carries, taken.
    pub(crate) fn take<T>(&mut self, event: &FeedEvent<T>) {
        match event {
            FeedEvent::Line(_) => {}
            FeedEvent::Unreadable { through, .. } => self.reported.1 = self.reported.1.max(*through),
            FeedEvent::Lost { through, .. } => self.reported.0 = self.reported.0.max(*through),
        }
    }
}
