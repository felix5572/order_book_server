//! Builders shared by the account tests: real node line shapes, anchors and a trusted book.

use alloy::primitives::Address;
use rustc_hash::FxHashMap;

use super::{
    anchor::{Anchor, AnchorCoin, AnchorPosition, AnchorRow},
    book::AccountBook,
    checkpoint::AccountMode,
    liquidation::LiquidationPx,
    misc::MiscEvent,
};
use crate::types::node_data::{Batch, NodeDataFill};

pub(crate) const PUMP: &str = "PUMP";

pub(crate) fn user(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

/// One fill line: `(user, coin, side "B"/"A", tid, startPosition, sz)`.
pub(crate) type FillSpec<'a> = (Address, &'a str, &'a str, u64, &'a str, &'a str);

/// A `node_fills_streaming` line (no trailing newline).
pub(crate) fn fills_line(block: u64, specs: &[FillSpec<'_>]) -> String {
    let events: Vec<String> = specs
        .iter()
        .map(|(user, coin, side, tid, start, sz)| {
            format!(
                r#"["{user:#x}",{{"coin":"{coin}","px":"1.0","sz":"{sz}","side":"{side}","time":1,"startPosition":"{start}","dir":"Open Long","closedPnl":"0","hash":"0xabc","oid":7,"crossed":true,"fee":"0","tid":{tid},"feeToken":"USDC"}}]"#
            )
        })
        .collect();
    batch_line(block, &events.join(","))
}

pub(crate) fn fills(block: u64, specs: &[FillSpec<'_>]) -> Batch<NodeDataFill> {
    sonic_rs::from_str(&fills_line(block, specs)).unwrap()
}

/// A `misc_events_streaming` line of these `inner` objects (no trailing newline).
pub(crate) fn misc_line(block: u64, inners: &[&str]) -> String {
    let events: Vec<String> =
        inners.iter().map(|inner| format!(r#"{{"time":"2026-10-19T06:00:00.0","hash":"0x00","inner":{inner}}}"#)).collect();
    batch_line(block, &events.join(","))
}

pub(crate) fn misc(block: u64, inners: &[&str]) -> Batch<MiscEvent> {
    sonic_rs::from_str(&misc_line(block, inners)).unwrap()
}

pub(crate) fn batch_line(block: u64, events: &str) -> String {
    format!(
        r#"{{"local_time":"2026-10-19T06:00:00.1","block_time":"2026-10-19T06:00:00.0","block_number":{block},"events":[{events}]}}"#
    )
}

pub(crate) fn row() -> AnchorRow {
    AnchorRow { isolated: false, pool_positions: Some(1), liquidation_px: LiquidationPx::Computed(0.004) }
}

/// `(coin, szDecimals, [(user, size_raw)])`.
pub(crate) type CoinSpec<'a> = (&'a str, u32, &'a [(Address, i64)]);

/// An anchor of these coins at block time `height` seconds; every holder is a unified account.
pub(crate) fn anchor(height: u64, coins: &[CoinSpec<'_>]) -> Anchor {
    let mut account_modes = FxHashMap::default();
    let coins = coins
        .iter()
        .map(|(name, size_decimals, holders)| {
            let positions = holders
                .iter()
                .map(|(user, size_raw)| {
                    account_modes.insert(*user, AccountMode::Unified);
                    (*user, AnchorPosition { size_raw: *size_raw, row: row() })
                })
                .collect();
            ((*name).to_string(), AnchorCoin { size_decimals: *size_decimals, positions })
        })
        .collect();
    Anchor { height, time_ms: height * 1_000, coins, account_modes }
}

/// Watchers started at unix ms 0 and both streams read block 100, then the anchor at 1000 (block time 1,000,000 ms)
/// with these PUMP holders installs: trusted.
pub(crate) fn trusted_book(holders: &[(Address, i64)]) -> AccountBook {
    let mut book = AccountBook::new(0);
    book.apply_fills(fills(100, &[]));
    book.apply_misc_events(misc(100, &[]));
    book.install_anchor(anchor(1_000, &[(PUMP, 0, holders)]));
    assert_eq!(book.availability().map(|available| available.anchor.height), Ok(1_000));
    book
}
