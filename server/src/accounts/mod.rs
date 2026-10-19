//! Per-coin positions and checkpoint-anchored liquidation prices (design 000419 r4).
//!
//! - `checkpoint` / `margin` / `liquidation` / `anchor`: parse a node checkpoint and compute its liquidation rows;
//! - `book`: the `AccountBook` owner (positions, merge, change marks, trust); `misc` / `actions` / `size`: stream wire
//!   formats;
//! - `runtime`: watchers, checkpoint poller and owner task; `view`: the served JSON;
//! - `probe` (one checkpoint) and `follow` (live, standalone): acceptance tools. The API comes in batch 3.

mod actions;
mod anchor;
pub(crate) mod book;
mod checkpoint;
mod evidence;
pub(crate) mod follow;
mod liquidation;
mod margin;
mod misc;
mod portfolio;
pub(crate) mod probe;
pub(crate) mod publish;
pub(crate) mod runtime;
mod size;
#[cfg(test)]
pub(crate) mod test_support;
pub(crate) mod view;
