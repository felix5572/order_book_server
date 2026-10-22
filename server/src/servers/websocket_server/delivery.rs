//! Subscription-owned broadcast lifetimes and bounded per-connection delivery diagnostics.
use super::Outbound;
use crate::{
    listeners::order_book::BboUpdate,
    metrics::{CHANNEL_DROPS_TOTAL, CHANNEL_LAG, WS_BROADCAST_SKIPPED_TOTAL, WS_DELIVERY_WAIT},
    types::{node_data::NodeDataFill, subscription::{Subscription, SubscriptionManager}},
};
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}},
    time::{Duration, Instant},
};
use tokio::sync::broadcast::{Sender, Receiver, error::RecvError};

/// A receiver exists only while the connection consumes its lane. Joining before
/// producer demand is activated prevents a snapshot/stream hole; dropping the last
/// subscription discards inactive backlog rather than replaying it on resubscribe.
struct DemandReceiver<T: Clone> {
    sender: Sender<T>,
    receiver: Option<Receiver<T>>,
    last_received: Instant,
}

impl<T: Clone> DemandReceiver<T> {
    fn new(sender: Sender<T>) -> Self {
        Self { sender, receiver: None, last_received: Instant::now() }
    }

    fn sync(&mut self, wanted: bool) {
        match (wanted, self.receiver.is_some()) {
            (true, false) => {
                self.receiver = Some(self.sender.subscribe());
                self.last_received = Instant::now();
            }
            (false, true) => self.receiver = None,
            _ => {}
        }
    }

    async fn next(&mut self) -> Result<T, RecvError> {
        let Some(receiver) = self.receiver.as_mut() else { return std::future::pending().await; };
        let next = receiver.recv().await;
        if next.is_ok() { self.last_received = Instant::now(); }
        next
    }

    fn pending(&self) -> usize { self.receiver.as_ref().map_or(0, Receiver::len) }
}

pub(super) struct UserFillsDelivery(DemandReceiver<Arc<Vec<NodeDataFill>>>);

impl UserFillsDelivery {
    pub(super) fn new(sender: Sender<Arc<Vec<NodeDataFill>>>) -> Self { Self(DemandReceiver::new(sender)) }

    pub(super) fn sync_subscriptions(&mut self, manager: &SubscriptionManager) {
        self.0.sync(manager.subscriptions().iter().any(|s| matches!(s, Subscription::UserFills { .. })));
    }

    pub(super) async fn next(&mut self) -> Result<Arc<Vec<NodeDataFill>>, RecvError> { self.0.next().await }

    pub(super) fn lagged(&self, skipped: u64, outbound: &Outbound, manager: &SubscriptionManager) {
        record_lag("user_fills", skipped);
        // Batches of all users' raw fill legs, not this user's lost trades.
        log::warn!("userFills broadcast gap; closing connection: {} skipped_batches={skipped} pending_batches={} since_last_receive_ms={} outbound_pending={} user_fills_subscriptions={}",
            outbound.diagnostics, self.0.pending(), self.0.last_received.elapsed().as_millis(), outbound.pending(),
            manager.subscriptions().iter().filter(|s| matches!(s, Subscription::UserFills { .. })).count());
    }
}

pub(super) struct BboDelivery(DemandReceiver<Arc<BboUpdate>>);

impl BboDelivery {
    pub(super) fn new(sender: Sender<Arc<BboUpdate>>) -> Self { Self(DemandReceiver::new(sender)) }

    pub(super) fn sync_subscriptions(&mut self, manager: &SubscriptionManager) {
        self.0.sync(manager.subscriptions().iter().any(|s| matches!(s, Subscription::Bbo { .. })));
    }

    pub(super) async fn next(&mut self) -> Result<Arc<BboUpdate>, RecvError> { self.0.next().await }

    pub(super) fn lagged(&self, skipped: u64, outbound: &Outbound) {
        record_lag("bbo", skipped);
        if outbound.diagnostics.report_gap() {
            log::warn!("BBO broadcast gap; withdrawing BBO subscriptions: {} skipped_messages={skipped} pending_messages={} since_last_receive_ms={} outbound_pending={}",
                outbound.diagnostics, self.0.pending(), self.0.last_received.elapsed().as_millis(), outbound.pending());
        }
    }
}

pub(super) fn record_lag(lane: &'static str, skipped: u64) {
    CHANNEL_LAG.set(i64::try_from(skipped).unwrap_or(i64::MAX));
    CHANNEL_DROPS_TOTAL.inc();
    WS_BROADCAST_SKIPPED_TOTAL.with_label_values(&[lane]).inc_by(skipped);
}

/// IDs are unique within a process; correlate with the startup log/PID. Maxima are
/// lifetime observations, not proof of the cause of a particular broadcast gap.
/// The peer is the actual socket peer (possibly a local proxy), never a trusted XFF claim.
pub(super) struct ConnectionDiagnostics {
    id: u64,
    peer: Option<SocketAddr>,
    queue_wait_max_us: AtomicU64,
    socket_write_max_us: AtomicU64,
    last_gap_report: Mutex<Option<Instant>>,
    last_slow_activity: Mutex<Option<SlowActivity>>,
}

impl ConnectionDiagnostics {
    pub(super) fn new(peer: Option<SocketAddr>) -> Self {
        static NEXT_CONNECTION: AtomicU64 = AtomicU64::new(1);
        Self { id: NEXT_CONNECTION.fetch_add(1, Ordering::Relaxed), peer,
            queue_wait_max_us: AtomicU64::new(0), socket_write_max_us: AtomicU64::new(0), last_gap_report: Mutex::new(None), last_slow_activity: Mutex::new(None) }
    }

    /// Detail logs have a per-connection 30s budget; counters always retain every gap.
    #[allow(clippy::expect_used, reason = "lock only guards an Instant assignment; no fallible/user work inside")]
    pub(super) fn report_gap(&self) -> bool {
        let mut last = self.last_gap_report.lock().expect("diagnostics lock not poisoned");
        if last.is_some_and(|at| at.elapsed() < Duration::from_secs(30)) { return false; }
        *last = Some(Instant::now());
        true
    }

    pub(super) fn activity(&self, stage: &'static str) -> Activity<'_> {
        Activity { owner: self, stage, started: Instant::now() }
    }

    pub(super) fn queue_wait(&self, elapsed: Duration) {
        self.queue_wait_max_us.fetch_max(u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX), Ordering::Relaxed);
        WS_DELIVERY_WAIT.with_label_values(&["outbound_queue"]).observe(elapsed.as_secs_f64());
    }

    pub(super) fn socket_write(&self, elapsed: Duration) {
        self.socket_write_max_us.fetch_max(u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX), Ordering::Relaxed);
        WS_DELIVERY_WAIT.with_label_values(&["socket_write"]).observe(elapsed.as_secs_f64());
    }
}

/// Elapsed branch time includes scheduling and awaited work; it is evidence of where
/// time passed, not proof of CPU saturation or a slow peer. One fixed-size sample per connection.
struct SlowActivity { stage: &'static str, elapsed: Duration, ended: Instant }
pub(super) struct Activity<'a> { owner: &'a ConnectionDiagnostics, stage: &'static str, started: Instant }

impl Drop for Activity<'_> {
    #[allow(clippy::expect_used, reason = "lock only guards a fixed-size sample assignment; no fallible/user work inside")]
    fn drop(&mut self) {
        let elapsed = self.started.elapsed();
        if elapsed >= Duration::from_millis(10) {
            *self.owner.last_slow_activity.lock().expect("diagnostics lock not poisoned") =
                Some(SlowActivity { stage: self.stage, elapsed, ended: Instant::now() });
        }
    }
}

impl std::fmt::Display for ConnectionDiagnostics {
    #[allow(clippy::expect_used, reason = "same diagnostic sample lock; formatting reports errors via fmt::Result")]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "connection={} peer={:?} queue_wait_max_us={} socket_write_max_us={}",
            self.id, self.peer, self.queue_wait_max_us.load(Ordering::Relaxed), self.socket_write_max_us.load(Ordering::Relaxed))?;
        if let Some(slow) = self.last_slow_activity.lock().expect("diagnostics lock not poisoned").as_ref() {
            write!(f, " last_slow_stage={} last_slow_elapsed_ms={} last_slow_ago_ms={}", slow.stage, slow.elapsed.as_millis(), slow.ended.elapsed().as_millis())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn inactive_bbo_subscriptions_do_not_receive_global_bbo_traffic() {
        let (sender, _) = tokio::sync::broadcast::channel(2);
        let mut delivery = BboDelivery::new(sender.clone());
        let mut manager = SubscriptionManager::default();
        manager.subscribe(Subscription::Trades { coin: "BTC".into() }).unwrap();
        delivery.sync_subscriptions(&manager);
        assert_eq!(sender.receiver_count(), 0);
        let sub = Subscription::Bbo { coin: "BTC".into() };
        manager.subscribe(sub.clone()).unwrap();
        delivery.sync_subscriptions(&manager);
        assert_eq!(sender.receiver_count(), 1);
        manager.unsubscribe(sub.clone());
        delivery.sync_subscriptions(&manager);
        assert_eq!(sender.receiver_count(), 0);
        manager.subscribe(sub).unwrap();
        delivery.sync_subscriptions(&manager);
        assert!(tokio::time::timeout(Duration::from_millis(10), delivery.next()).await.is_err());
    }

    #[test]
    fn gap_diagnostics_are_bounded_and_identify_a_slow_branch() {
        let diagnostics = ConnectionDiagnostics::new(None);
        assert!(diagnostics.report_gap());
        assert!(!diagnostics.report_gap());
        *diagnostics.last_gap_report.lock().unwrap() = Some(Instant::now() - Duration::from_secs(31));
        assert!(diagnostics.report_gap());
        drop(Activity { owner: &diagnostics, stage: "client_message", started: Instant::now() - Duration::from_millis(20) });
        let rendered = diagnostics.to_string();
        assert!(rendered.contains("last_slow_stage=client_message"));
        assert!(rendered.contains("last_slow_elapsed_ms="));
    }

    #[tokio::test]
    async fn private_receiver_lifetime_retains_events_for_remaining_users_and_skips_inactive_backlog() {
        let (sender, _) = tokio::sync::broadcast::channel(2);
        let mut delivery = UserFillsDelivery::new(sender.clone());
        let mut manager = SubscriptionManager::default();
        let first = Subscription::UserFills { user: format!("0x{:040x}", 1) };
        let second = Subscription::UserFills { user: format!("0x{:040x}", 2) };
        manager.subscribe(first.clone()).unwrap();
        delivery.sync_subscriptions(&manager);
        let batch = Arc::new(Vec::new());
        sender.send(Arc::clone(&batch)).unwrap();
        manager.subscribe(second.clone()).unwrap();
        delivery.sync_subscriptions(&manager);
        manager.unsubscribe(first);
        delivery.sync_subscriptions(&manager);
        assert!(Arc::ptr_eq(&delivery.next().await.unwrap(), &batch), "remaining subscription retains queued events");
        manager.unsubscribe(second.clone());
        delivery.sync_subscriptions(&manager);
        assert_eq!(sender.receiver_count(), 0);
        assert!(sender.send(Arc::new(Vec::new())).is_err());
        manager.subscribe(second).unwrap();
        delivery.sync_subscriptions(&manager);
        assert!(tokio::time::timeout(Duration::from_millis(10), delivery.next()).await.is_err());
        sender.send(Arc::clone(&batch)).unwrap();
        assert!(Arc::ptr_eq(&delivery.next().await.unwrap(), &batch));
        drop(delivery);
        assert_eq!(sender.receiver_count(), 0);
    }
}
