//! Private fill subscription lifetime and per-connection delivery diagnostics.
use super::Outbound;
use crate::{
    metrics::{CHANNEL_DROPS_TOTAL, CHANNEL_LAG, WS_BROADCAST_SKIPPED_TOTAL, WS_DELIVERY_WAIT},
    types::{node_data::NodeDataFill, subscription::{Subscription, SubscriptionManager}},
};
use std::{
    net::SocketAddr,
    sync::{Arc, atomic::{AtomicU64, Ordering}},
    time::{Duration, Instant},
};
use tokio::sync::broadcast::{Sender, Receiver, error::RecvError};

/// Owned by a connection. Attach before activating the producer's userFills count;
/// detach after its last subscription, so resubscribing never replays an inactive backlog.
pub(super) struct UserFillsDelivery {
    sender: Sender<Arc<Vec<NodeDataFill>>>,
    receiver: Option<Receiver<Arc<Vec<NodeDataFill>>>>,
    last_received: Instant,
}

impl UserFillsDelivery {
    pub(super) fn new(sender: Sender<Arc<Vec<NodeDataFill>>>) -> Self {
        Self { sender, receiver: None, last_received: Instant::now() }
    }

    pub(super) fn sync_subscriptions(&mut self, manager: &SubscriptionManager) {
        let wanted = manager.subscriptions().iter().any(|sub| matches!(sub, Subscription::UserFills { .. }));
        match (wanted, self.receiver.is_some()) {
            (true, false) => {
                self.receiver = Some(self.sender.subscribe());
                self.last_received = Instant::now();
            }
            (false, true) => self.receiver = None,
            _ => {}
        }
    }

    pub(super) async fn next(&mut self) -> Result<Arc<Vec<NodeDataFill>>, RecvError> {
        let Some(receiver) = self.receiver.as_mut() else { return std::future::pending().await; };
        let next = receiver.recv().await;
        if next.is_ok() { self.last_received = Instant::now(); }
        next
    }

    pub(super) fn lagged(&self, skipped: u64, outbound: &Outbound, manager: &SubscriptionManager) {
        record_lag("user_fills", skipped);
        // These are batches of all users' raw fill legs; neither skipped trades nor
        // fills of this particular user can be inferred from this ring's count.
        log::warn!("userFills broadcast gap; closing connection: {} skipped_batches={skipped} pending_batches={} since_last_receive_ms={} outbound_pending={} user_fills_subscriptions={}",
            outbound.diagnostics, self.receiver.as_ref().map_or(0, Receiver::len),
            self.last_received.elapsed().as_millis(), outbound.pending(),
            manager.subscriptions().iter().filter(|s| matches!(s, Subscription::UserFills { .. })).count());
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
}

impl ConnectionDiagnostics {
    pub(super) fn new(peer: Option<SocketAddr>) -> Self {
        static NEXT_CONNECTION: AtomicU64 = AtomicU64::new(1);
        Self { id: NEXT_CONNECTION.fetch_add(1, Ordering::Relaxed), peer,
            queue_wait_max_us: AtomicU64::new(0), socket_write_max_us: AtomicU64::new(0) }
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

impl std::fmt::Display for ConnectionDiagnostics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "connection={} peer={:?} queue_wait_max_us={} socket_write_max_us={}",
            self.id, self.peer, self.queue_wait_max_us.load(Ordering::Relaxed), self.socket_write_max_us.load(Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
