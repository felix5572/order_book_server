//! Per-coin positions and checkpoint-anchored liquidation prices (design 000419 r4).
//!
//! - `checkpoint` / `margin` / `liquidation` / `anchor`: parse a node checkpoint and compute its liquidation rows;
//! - `book`: the `AccountBook` owner (positions, merge, change marks, trust); `misc` / `size`: stream wire formats;
//! - `runtime`: watchers, checkpoint poller and owner task; `view`: the served JSON;
//! - `probe` (one checkpoint) and `follow` (live, standalone): acceptance tools. The API comes in batch 3.

mod anchor;
mod book;
mod checkpoint;
pub(crate) mod follow;
mod liquidation;
mod margin;
mod misc;
pub(crate) mod probe;
pub(crate) mod runtime;
mod size;
mod view;
