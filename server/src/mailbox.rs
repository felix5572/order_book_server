//! A mailbox (moved from `accounts::walk` for hyperliquid-trade design 000591, the listener posting to one too): the
//! newest pending request of each key, taken by one task.

use std::{
    collections::HashMap,
    sync::{Mutex, PoisonError},
};

use tokio::sync::Notify;

/// The newest pending request of each key; one task drains it.
#[derive(Debug)]
pub(crate) struct Mailbox<K, V> {
    pending: Mutex<HashMap<K, V>>,
    notify: Notify,
}

impl<K, V> Default for Mailbox<K, V> {
    fn default() -> Self {
        Self { pending: Mutex::new(HashMap::new()), notify: Notify::new() }
    }
}

impl<K: Eq + std::hash::Hash, V> Mailbox<K, V> {
    /// Replaces the key's pending request.
    pub(crate) fn post(&self, key: K, request: V) {
        self.lock().insert(key, request);
        self.notify.notify_one();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<K, V>> {
        // the map is replaced or drained in one statement: a panic elsewhere cannot leave it half-written
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The pending requests, taken (a test's view of what the task would get).
    #[cfg(test)]
    pub(crate) fn drain(&self) -> HashMap<K, V> {
        std::mem::take(&mut *self.lock())
    }

    pub(crate) async fn take(&self) -> HashMap<K, V> {
        loop {
            let pending = std::mem::take(&mut *self.lock());
            if !pending.is_empty() {
                return pending;
            }
            self.notify.notified().await;
        }
    }
}
