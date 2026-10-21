//! Local marks (design 000454 r1–r2): the reference mark computed in this process from the venues' quotes and the
//! node replica's state. Batch 1: the markets (`sources`), the venue feeds and their latest-value slots.

pub(crate) mod compute;
pub(crate) mod connection;
pub(crate) mod feeds;
pub(crate) mod owner;
pub(crate) mod price;
pub(crate) mod prices_view;
pub(crate) mod probe;
pub(crate) mod protocol;
pub(crate) mod rest;
pub(crate) mod slot;
pub(crate) mod snapshot;
pub(crate) mod sources;
pub(crate) mod subscriptions;
pub(crate) mod view;

use std::time::{SystemTime, UNIX_EPOCH};

/// Wall-clock milliseconds; a host clock before 1970 or past u64 ms is broken (the accounts' rule).
#[expect(clippy::expect_used, reason = "a host clock before 1970 or past u64 ms is broken; quote ages depend on it")]
pub(crate) fn now_ms() -> u64 {
    let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH).expect("the host clock is after 1970");
    u64::try_from(since_epoch.as_millis()).expect("unix ms fit u64")
}
