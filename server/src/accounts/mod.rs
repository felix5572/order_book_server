//! Per-coin positions and checkpoint-anchored liquidation prices (design 000419 r4).
//!
//! Batch 1: the checkpoint parser and the pure liquidation computation, plus an offline probe used to accept them
//! against real checkpoints. Wiring into the server (watchers, state owner, API) comes in later batches.

mod checkpoint;
mod liquidation;
mod margin;
pub(crate) mod probe;
