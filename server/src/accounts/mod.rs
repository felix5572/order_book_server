//! Per-coin positions and checkpoint-anchored liquidation prices (design 000419 r4).
//!
//! - `checkpoint` / `margin` / `liquidation` / `anchor`: parse a node checkpoint and compute its liquidation rows;
//! - `book`: the `AccountBook` owner (positions, merge, change marks, trust); `blocks`: the block assembly that hands it
//!   complete blocks in height order; `replay`: the cash replay between anchors and its check at each anchor;
//! - `misc` / `actions` / `size` / `usdc`: stream wire formats and units;
//! - `marks`: the node's marks block by block (design 000439), `marks::seed` what a checkpoint seeds them with;
//! - `runtime`: watchers, checkpoint poller and owner task; `view`: the served JSON;
//! - `probe` (one checkpoint) and `follow` (live, standalone): acceptance tools. The API comes in batch 3.

mod actions;
mod anchor;
mod blocks;
pub(crate) mod candidates;
pub(crate) mod candidates_view;
pub(crate) mod book;
mod checkpoint;
mod cooldown;
mod diagnostics;
mod evidence;
mod funding;
pub(crate) mod forced_flow;
pub(crate) mod forced_flow_owner;
pub(crate) mod forced_flow_reader;
pub(crate) mod forced_flow_view;
pub(crate) mod follow;
mod liquidation;
mod margin;
pub(crate) mod marks;
mod misc;
mod portfolio;
pub(crate) mod probe;
pub(crate) mod publish;
mod replay;
mod replica;
pub(crate) mod runtime;
mod size;
mod usdc;
pub(crate) mod walk;
#[cfg(test)]
pub(crate) mod test_support;
pub(crate) mod view;
