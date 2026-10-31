//! The pending book (--pending-book, hyperliquid-trade design 000604): the listener's book as the actions in the
//! node's mempool will leave it. Batch 1 is the replica of the listener's book alone, on its own thread, serving
//! nothing yet; the mempool, the overlay and the `extPendingBbo` / `extPendingTrades` channels come with batch 2.

use std::{io, sync::Arc, time::Duration};

use log::{error, info};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    accounts::owner_thread::spawn_owner,
    listeners::order_book::replica::{BookReplica, ReplicaBatch, ReplicaControl, ReplicaFork, ReplicaInputs},
    metrics::{
        PENDING_BOOK_ALIGNMENT_BLOCKS, PENDING_BOOK_BACKLOG_EVENTS, PENDING_BOOK_BREAKS_TOTAL, PENDING_BOOK_CONFIRMED_HEIGHT,
        PENDING_BOOK_UNPROVEN_LEVELS,
    },
};

/// How often the thread looks for a generation the listener ended and exports its gauges.
const TICK: Duration = Duration::from_millis(100);

/// The pending thread, owner of the replica (`pending-book`, on its own OS thread like the account owner's).
pub(crate) fn spawn_pending_book(inputs: ReplicaInputs, backlog_events: usize) -> io::Result<JoinHandle<()>> {
    spawn_owner("pending-book", run(inputs, backlog_events))
}

async fn run(inputs: ReplicaInputs, backlog_events: usize) {
    let ReplicaInputs { control, forks, batches } = inputs;
    let mut feeds = Feeds { forks, batches };
    let mut book = PendingBook::new(control, backlog_events);
    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            fork = feeds.forks.recv() => match fork {
                Some(fork) => book.on_fork(fork),
                None => break,
            },
            batch = feeds.batches.recv() => match batch {
                Some(batch) => book.on_batch(batch, &mut feeds.forks),
                None => break,
            },
            _ = tick.tick() => book.on_tick(),
        }
    }
    error!("pending book: the listener's feed closed; the pending thread ends");
}

/// The listener's two queues to the pending thread.
struct Feeds {
    forks: mpsc::Receiver<ReplicaFork>,
    batches: mpsc::Receiver<ReplicaBatch>,
}

/// The replica, and the generation the pending thread wants: whatever of another generation reaches it is dropped.
struct PendingBook {
    control: Arc<ReplicaControl>,
    wanted: u64,
    replica: Option<BookReplica>,
    /// The listener's count of generations it ended, as last seen.
    ended: u64,
    backlog_events: usize,
}

impl PendingBook {
    fn new(control: Arc<ReplicaControl>, backlog_events: usize) -> Self {
        let ended = control.ended();
        control.request(1);
        Self { control, wanted: 1, replica: None, ended, backlog_events }
    }

    fn on_fork(&mut self, fork: ReplicaFork) {
        if fork.generation != self.wanted || self.replica.is_some() {
            return;
        }
        info!("pending book: replica generation {} forked", fork.generation);
        self.replica = Some(BookReplica::new(fork, self.backlog_events));
    }

    /// A batch of the wanted generation: its fork, queued before it, is taken first if it was not yet (review 000606
    /// r1 P2-2: the two queues keep no order between them).
    fn on_batch(&mut self, batch: ReplicaBatch, forks: &mut mpsc::Receiver<ReplicaFork>) {
        self.end_if_ended();
        if batch.generation != self.wanted {
            return;
        }
        while self.replica.is_none() {
            let Ok(fork) = forks.try_recv() else {
                error!("pending book: a batch of generation {} came without its fork", batch.generation);
                return self.request_next("fork_missing");
            };
            self.on_fork(fork);
        }
        let Some(replica) = &mut self.replica else { return };
        if let Err(stop) = replica.push(batch.batch) {
            error!("pending book: the replica stops ({stop:?}); another generation follows");
            self.request_next(stop.cause());
        }
    }

    fn on_tick(&mut self) {
        self.end_if_ended();
        let replica = self.replica.as_ref();
        let confirmed = replica.and_then(BookReplica::confirmed).unwrap_or(0);
        PENDING_BOOK_CONFIRMED_HEIGHT.set(i64::try_from(confirmed).unwrap_or(i64::MAX));
        PENDING_BOOK_ALIGNMENT_BLOCKS.set(replica.map_or(0, |replica| i64::try_from(replica.alignment_blocks()).unwrap_or(i64::MAX)));
        PENDING_BOOK_BACKLOG_EVENTS.set(replica.map_or(0, |replica| i64::try_from(replica.backlog_events()).unwrap_or(i64::MAX)));
        PENDING_BOOK_UNPROVEN_LEVELS.set(replica.map_or(0, |replica| i64::try_from(replica.unproven_levels()).unwrap_or(i64::MAX)));
    }

    /// The listener ended the generation it fed (a full queue, its book untrusted): the replica goes.
    fn end_if_ended(&mut self) {
        let ended = self.control.ended();
        if ended == self.ended {
            return;
        }
        self.ended = ended;
        if self.replica.is_some() {
            info!("pending book: the listener ended replica generation {}; another follows", self.wanted);
        }
        self.request_next_quietly();
    }

    /// The replica stops on the pending thread's side, for `cause`.
    fn request_next(&mut self, cause: &str) {
        PENDING_BOOK_BREAKS_TOTAL.with_label_values(&[cause]).inc();
        self.request_next_quietly();
    }

    /// The next generation requested: the listener forks it once the old one's batches are all taken.
    fn request_next_quietly(&mut self) {
        self.replica = None;
        self.wanted += 1;
        self.control.request(self.wanted);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::listeners::order_book::replica::{
        ReplicaFeed, link,
        tests::{empty_book_batch, empty_line, listener_applied, listener_untrusted},
    };

    fn book_and_feed(queue_batches: usize, backlog_events: usize) -> (PendingBook, ReplicaFeed, Feeds) {
        let (feed, inputs) = link(queue_batches);
        let book = PendingBook::new(Arc::clone(&inputs.control), backlog_events);
        (book, feed, Feeds { forks: inputs.forks, batches: inputs.batches })
    }

    /// Review 000606 r1 P2-2: the listener forks and sends a first batch between the pending thread's looks at its two
    /// queues, and the batch is taken first: its fork is installed before it, and the batch applies.
    #[test]
    fn a_first_batch_taken_before_its_fork_installs_the_fork_first() {
        let (mut book, mut feed, mut feeds) = book_and_feed(16, 1_000);
        empty_book_batch(&mut feed);
        listener_applied(&mut feed, empty_line(true, 1));
        listener_applied(&mut feed, empty_line(false, 1));
        listener_applied(&mut feed, empty_line(true, 2));
        listener_applied(&mut feed, empty_line(false, 2));
        for _ in 0..4 {
            let batch = feeds.batches.try_recv().unwrap();
            book.on_batch(batch, &mut feeds.forks);
        }
        let replica = book.replica.as_ref().unwrap();
        assert_eq!((replica.confirmed(), replica.backlog_events()), (Some(1), 2), "the first batch was not lost");
        assert!(feeds.forks.try_recv().is_err());
    }

    /// A fork or a batch of a generation the pending thread gave up is dropped.
    #[test]
    fn another_generations_fork_and_batches_are_dropped() {
        let (mut book, mut feed, mut feeds) = book_and_feed(16, 1_000);
        empty_book_batch(&mut feed);
        let fork = feeds.forks.try_recv().unwrap();
        book.request_next("test");
        book.on_fork(fork);
        assert!(book.replica.is_none(), "generation 1 given up");
        listener_applied(&mut feed, empty_line(true, 1));
        book.on_batch(feeds.batches.try_recv().unwrap(), &mut feeds.forks);
        assert!(book.replica.is_none());
        empty_book_batch(&mut feed);
        book.on_fork(feeds.forks.try_recv().unwrap());
        assert!(book.replica.is_some(), "generation 2");
    }

    /// The listener ending a generation (here its book turning untrusted, with no batch after) drops the replica at
    /// the next tick and requests the next generation.
    #[test]
    fn a_generation_the_listener_ended_is_dropped() {
        let (mut book, mut feed, mut feeds) = book_and_feed(16, 1_000);
        empty_book_batch(&mut feed);
        book.on_fork(feeds.forks.try_recv().unwrap());
        listener_untrusted(&mut feed);
        book.on_tick();
        assert!(book.replica.is_none());
        assert_eq!(book.wanted, 2);
        empty_book_batch(&mut feed);
        assert_eq!(feeds.forks.try_recv().unwrap().generation, 2);
    }

    /// The replica past its budget stops on the pending thread's side: the listener no longer feeds that generation
    /// and forks the next.
    #[test]
    fn a_replica_past_its_budget_requests_the_next_generation() {
        let (mut book, mut feed, mut feeds) = book_and_feed(16, 1);
        empty_book_batch(&mut feed);
        book.on_fork(feeds.forks.try_recv().unwrap());
        for height in 1..=2 {
            listener_applied(&mut feed, empty_line(false, height));
            book.on_batch(feeds.batches.try_recv().unwrap(), &mut feeds.forks);
        }
        assert!(book.replica.is_none());
        assert_eq!(book.wanted, 2);
        empty_book_batch(&mut feed);
        assert_eq!(feeds.forks.try_recv().unwrap().generation, 2);
        listener_applied(&mut feed, empty_line(false, 3));
        assert_eq!(feeds.batches.try_recv().unwrap().generation, 2, "generation 1 no longer fed");
    }
}
