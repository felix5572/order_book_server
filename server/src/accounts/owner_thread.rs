//! Account/mark CPU work must not hold a network scheduler worker. In the BM
//! 2026-10-22 BBO incidents a receiver paused 1.54–1.86 s around checkpoint
//! installation while socket writes stayed below 4 ms. A two-worker reproduction
//! showed that a woken broadcast receiver can stay in the busy worker's unstealable
//! LIFO slot. One private current-thread executor keeps the existing single owner,
//! bounded inputs and ordering, without making every input a blocking-pool job.
use std::{future::Future, io, sync::mpsc, thread};
use tokio::{runtime::Builder, sync::oneshot, task::JoinHandle};

/// The returned handle supervises the actual owner task: panic and cancellation
/// keep normal Tokio semantics. The OS thread owns its executor until that task
/// is dropped, including an abort before its first poll. The thread then exits; dropping a handle alone still detaches, like `tokio::spawn`.
/// Cancellation is cooperative: a synchronous step finishes before it takes effect.
/// `name`: the thread's (`accounts-owner`, `forced-flow`: each owner on its own, review 000563).
pub(super) fn spawn_owner(name: &'static str, future: impl Future<Output = ()> + Send + 'static) -> io::Result<JoinHandle<()>> {
    let (ready, started) = mpsc::sync_channel(1);
    thread::Builder::new().name(name.into()).spawn(move || {
        let runtime = match Builder::new_current_thread().enable_time().build() {
            Ok(runtime) => runtime,
            Err(error) => { drop(ready.send(Err(error))); return; }
        };
        let (finished, stopped) = oneshot::channel::<()>();
        let owner = runtime.spawn(async move {
            // Captured before the first poll; also dropped on cancellation/panic.
            let _finished = finished;
            future.await;
        });
        if ready.send(Ok(owner)).is_err() { return; }
        // Only lifetime notification: task errors are delivered by its JoinHandle.
        runtime.block_on(async { drop(stopped.await); });
    })?;
    started.recv().map_err(|error| io::Error::other(format!("{name} thread failed to start: {error}")))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::poll_fn,
        sync::{Arc, atomic::{AtomicBool, Ordering}},
        time::Duration,
    };
    use tokio::sync::broadcast;

    #[tokio::test]
    async fn owner_timer_queries_and_completion_run_on_its_own_thread() {
        let caller = thread::current().id();
        let (reply, answer) = oneshot::channel();
        let owner = spawn_owner("accounts-owner", async move {
            let before = thread::current().id();
            tokio::time::sleep(Duration::from_millis(5)).await;
            assert_eq!(before, thread::current().id());
            assert_eq!(thread::current().name(), Some("accounts-owner"));
            reply.send(before).unwrap();
        }).unwrap();
        assert_ne!(answer.await.unwrap(), caller);
        owner.await.unwrap();
    }

    #[tokio::test]
    async fn abort_closes_owner_state_and_join_reports_cancellation() {
        let (started, ready) = oneshot::channel();
        let (state, mut view) = tokio::sync::watch::channel(0);
        let owner = spawn_owner("accounts-owner", async move {
            let _state = state;
            started.send(()).unwrap();
            std::future::pending::<()>().await;
        }).unwrap();
        ready.await.unwrap();
        owner.abort();
        assert!(owner.await.unwrap_err().is_cancelled());
        assert!(view.changed().await.is_err(), "no publisher left alive after owner abort");
    }

    #[tokio::test]
    async fn panic_is_reported_and_closes_owner_state() {
        let (state, mut view) = tokio::sync::watch::channel(0);
        let owner = spawn_owner("accounts-owner", async move {
            let _state = state;
            panic!("simulated account invariant failure");
        }).unwrap();
        assert!(owner.await.unwrap_err().is_panic());
        assert!(view.changed().await.is_err());
    }

    /// Actual production ring size, enough messages to evict a stalled receiver.
    /// The owner wakes a waiting network task then stays synchronous, as publication
    /// followed by checkpoint/candidate work does. Other network workers are free.
    /// The sleep models one non-yielding poll, not a slow peer or smaller queue.
    async fn broadcast_during_sync_work(isolated: bool) -> (usize, u64) {
        let (sender, mut receiver) = broadcast::channel::<usize>(16_384);
        let waiting = Arc::new(AtomicBool::new(false));
        let consumer_waiting = Arc::clone(&waiting);
        let consumer = tokio::spawn(async move {
            let mut delivered = 0;
            let mut skipped = 0;
            loop {
                let mut next = std::pin::pin!(receiver.recv());
                let result = poll_fn(|cx| {
                    let polled = next.as_mut().poll(cx);
                    if polled.is_pending() { consumer_waiting.store(true, Ordering::Release); }
                    polled
                }).await;
                match result {
                    Ok(_) => delivered += 1,
                    Err(broadcast::error::RecvError::Lagged(count)) => skipped += count,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            (delivered, skipped)
        });
        while !waiting.load(Ordering::Acquire) { tokio::task::yield_now().await; }
        let (go, begin) = mpsc::channel();
        let producer_sender = sender.clone();
        let producer = thread::spawn(move || {
            begin.recv().unwrap();
            for value in 1..=24_000 {
                producer_sender.send(value).unwrap();
                if value % 100 == 0 { thread::sleep(Duration::from_millis(5)); }
            }
        });
        let work = async move {
            sender.send(0).unwrap();
            go.send(()).unwrap();
            thread::sleep(Duration::from_millis(1_600));
        };
        let owner = if isolated { spawn_owner("accounts-owner", work).unwrap() } else { tokio::spawn(work) };
        owner.await.unwrap();
        // The producer normally finished before the owner; never join on a network worker.
        tokio::task::spawn_blocking(move || producer.join().unwrap()).await.unwrap();
        consumer.await.unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn synchronous_owner_work_cannot_starve_the_bbo_receiver() {
        let (received, skipped) = broadcast_during_sync_work(true).await;
        assert_eq!(skipped, 0, "the network task must run during the owner's long synchronous step");
        assert_eq!(received, 24_001);
    }

    /// Manual negative control: proves the old topology can overflow the identical
    /// ring even with a second idle network worker. Kept out of routine parallel
    /// tests because task scheduling is deliberately the experimental variable.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "scheduler negative control; run explicitly when investigating BBO starvation"]
    async fn shared_network_worker_reproduces_the_bbo_gap() {
        let (received, skipped) = broadcast_during_sync_work(false).await;
        assert!(skipped > 0, "shared-worker stall did not reproduce in this run");
        assert_eq!(u64::try_from(received).unwrap() + skipped, 24_001);
        eprintln!("shared worker: received={received}, skipped={skipped}");
    }
}
