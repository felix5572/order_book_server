//! Builders shared by the account tests: real node line shapes, anchors and a trusted book.

use alloy::primitives::Address;
use rustc_hash::FxHashMap;

use super::{
    actions::{BlockActionKind, BlockActions},
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

/// `(user, fill object)` of an observed fill, verbatim.
pub(crate) type RealFill = (&'static str, &'static str);

/// Official fills of the 2025-10-10 ADL (`userFills` / explorer, Codex handoff
/// `research-data/hyperliquid-trade/hl_account_state_verify_20261019/adl_replay_20261020/verification.json`), each at
/// its explorer block. The fill objects are the API's parsed JSON re-serialized compactly; only the batch envelope
/// around them (`real_fills_line`) is a test wrapper, not a node line.
pub(crate) const ETH_ADL_BLOCK: u64 = 758_801_258;
/// A partial ADL of a short: -95,036.7854 -> -46,127.2925 ETH.
pub(crate) const ETH_ADL_PARTIAL: RealFill = (
    "0x2ea18c23f72a4b6172c55b411823cdc5335923f4",
    r#"{"coin":"ETH","px":"3561.2","sz":"48909.4929","side":"B","time":1760131026037,"startPosition":"-95036.7854","dir":"Auto-Deleveraging","closedPnl":"38045716.3370520025","hash":"0x784fd33f92195e9079c9042d3a636a010f00eb252d1c7d621c187e92511d387b","oid":194019304535,"crossed":true,"fee":"0.0","tid":133066960337337,"liquidation":{"liquidatedUser":"0xb0a55f13d22f66e6d495ac98113841b2326e9540","markPx":"3538.4","method":"backstop"},"feeToken":"USDC","twapId":null}"#,
);
pub(crate) const AVAX_ADL_BLOCK: u64 = 758_800_641;
/// A full ADL of a short: -28.53 -> 0 AVAX, at px 22.075 while the event's markPx is 22.025.
pub(crate) const AVAX_ADL_FULL: RealFill = (
    "0xe92d2041199b18f611b137457f3acee8bedb22d3",
    r#"{"coin":"AVAX","px":"22.075","sz":"28.53","side":"B","time":1760130964831,"startPosition":"-28.53","dir":"Auto-Deleveraging","closedPnl":"194.275035","hash":"0x922ada1583e0175a93a4042d3a6101010100f1fb1ee3362c35f3856842e3f145","oid":194018286846,"crossed":true,"fee":"0.0","tid":771334864158847,"liquidation":{"liquidatedUser":"0x2e3d94f0562703b25c83308a05046ddaf9a8dd14","markPx":"22.025","method":"backstop"},"feeToken":"USDC","twapId":null}"#,
);
pub(crate) const SOL_ADL_BLOCK: u64 = 758_801_157;
/// Both legs of one ADL trade share tid 1091349864617169: the ADL'd long 0.54 -> 0.53 SOL ...
pub(crate) const SOL_ADL_LONG: RealFill = (
    "0x28122e1514b85fa10b5d41e260905c86dcf5051b",
    r#"{"coin":"SOL","px":"174.44","sz":"0.01","side":"A","time":1760131016356,"startPosition":"0.54","dir":"Auto-Deleveraging","closedPnl":"1.53748","hash":"0x4be48dd0dfaa8b414d5e042d3a6305010900a5b67aadaa13efad39239eae652b","oid":194019136006,"crossed":true,"fee":"0.0","tid":1091349864617169,"liquidation":{"liquidatedUser":"0x67ce2b979ee9615cdf634e6043ac3097595ce8d9","markPx":"174.14","method":"backstop"},"feeToken":"USDC","twapId":null}"#,
);
/// ... and the liquidated short -0.01 -> 0 SOL, whose FARTCOIN long went in the same action.
pub(crate) const SOL_LIQUIDATED_SHORT: RealFill = (
    "0x67ce2b979ee9615cdf634e6043ac3097595ce8d9",
    r#"{"coin":"SOL","px":"174.44","sz":"0.01","side":"B","time":1760131016356,"startPosition":"-0.01","dir":"Liquidated Cross Short","closedPnl":"0.023445","hash":"0x4be48dd0dfaa8b414d5e042d3a6305010900a5b67aadaa13efad39239eae652b","oid":194019136006,"crossed":false,"fee":"0.0","tid":1091349864617169,"liquidation":{"liquidatedUser":"0x67ce2b979ee9615cdf634e6043ac3097595ce8d9","markPx":"174.14","method":"backstop"},"feeToken":"USDC","twapId":null}"#,
);
pub(crate) const FARTCOIN_LIQUIDATED_LONG: RealFill = (
    "0x67ce2b979ee9615cdf634e6043ac3097595ce8d9",
    r#"{"coin":"FARTCOIN","px":"0.3873","sz":"45668.0","side":"A","time":1760131016356,"startPosition":"45668.0","dir":"Liquidated Cross Long","closedPnl":"-5558.70896","hash":"0x4be48dd0dfaa8b414d5e042d3a6305010900a5b67aadaa13efad39239eae652b","oid":194019138105,"crossed":false,"fee":"0.0","tid":714230910101034,"liquidation":{"liquidatedUser":"0x67ce2b979ee9615cdf634e6043ac3097595ce8d9","markPx":"0.35465","method":"backstop"},"feeToken":"USDC","twapId":null}"#,
);

/// A fills line (test envelope) of these observed fills, in order.
pub(crate) fn real_fills_line(block: u64, real: &[RealFill]) -> String {
    let events: Vec<String> = real.iter().map(|(user, fill)| format!(r#"["{user}",{fill}]"#)).collect();
    batch_line(block, &events.join(","))
}

pub(crate) fn real_fills(block: u64, real: &[RealFill]) -> Batch<NodeDataFill> {
    sonic_rs::from_str(&real_fills_line(block, real)).unwrap()
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

/// A parsed `replica_cmds` block of these kept actions (the line format is tested in `actions`).
pub(crate) fn block_actions(height: u64, actions: &[(BlockActionKind, Address)]) -> BlockActions {
    BlockActions { height, actions: actions.to_vec() }
}

pub(crate) fn batch_line(block: u64, events: &str) -> String {
    format!(
        r#"{{"local_time":"2026-10-19T06:00:00.1","block_time":"2026-10-19T06:00:00.0","block_number":{block},"events":[{events}]}}"#
    )
}

pub(crate) fn row() -> AnchorRow {
    AnchorRow { isolated: false, pool_positions: Some(1), liquidation_px: LiquidationPx::Computed(0.004), portfolio_margin: None }
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

/// Watchers started at unix ms 0, fills and misc events read block 100 and the block actions block 1000 (they run on
/// by one from there), then the anchor at 1000 (block time 1,000,000 ms) with these PUMP holders installs: trusted.
pub(crate) fn trusted_book(holders: &[(Address, i64)]) -> AccountBook {
    let mut book = AccountBook::new(0);
    book.apply_fills(fills(100, &[]));
    book.apply_misc_events(misc(100, &[]));
    book.apply_block_actions(block_actions(1_000, &[]));
    book.install_anchor(anchor(1_000, &[(PUMP, 0, holders)]));
    assert_eq!(book.availability().map(|available| available.anchor.height), Ok(1_000));
    book
}
