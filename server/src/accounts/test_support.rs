//! Builders shared by the account tests: real node line shapes, anchors and a trusted book.

use alloy::primitives::Address;
use rustc_hash::FxHashMap;

use super::{
    actions::{BlockAction, BlockActions},
    anchor::{Anchor, AnchorCoin, AnchorPosition, AnchorRow},
    book::AccountBook,
    checkpoint::AccountMode,
    checkpoint::{parse_checkpoint_bytes, tests::fixture},
    liquidation::{LiquidationPx, RowMark},
    margin::MarginTable,
    marks::{MarksAt, seed::MarkSeed},
    misc::MiscEvent,
    replay::{MarginStates, tests::per_dex_with_main_cash},
};
use crate::types::node_data::{Batch, NodeDataFill, NodeDataOrderStatus, OracleUpdateEvent};

pub(crate) const PUMP: &str = "PUMP";
/// The table of a test anchor's positions, and the coins' current table (the fixture's PUMP: 52, a 10x table).
pub(crate) const ANCHOR_TABLE: u32 = 20;
pub(crate) const CURRENT_TABLE: u32 = 52;

/// The block time of `hip3_line`, unix ns (2026-10-19T06:40:00.5 UTC).
pub(crate) const HIP3_LINE_TIME_NS: u64 = 1_792_392_000_500_000_000;

/// A `hip3_oracle_updates_streaming` line of one deployer's update writing `marks` [(coin, px)] at the block's time,
/// in the field set of 2026-10-20.
pub(crate) fn hip3_line(block: u64, marks: &[(&str, &str)]) -> Batch<OracleUpdateEvent> {
    let time = "2026-10-19T06:40:00.5";
    let written: Vec<String> = marks
        .iter()
        .map(|(coin, px)| format!(r#"["{coin}",{{"px":"{px}","last_update_time":"{time}","daily_px":"{px}"}}]"#))
        .collect();
    let event = format!(
        r#"{{"update_class":"Normal","mark_px_inputs":[],"spot_px_inputs":[],"external_perp_px_inputs":[],"oracle_pxs":{{"coin_to_mark_px":[{}],"coin_to_oracle_px":[],"coin_to_external_perp_px":[]}}}}"#,
        written.join(",")
    );
    sonic_rs::from_str(&format!(
        r#"{{"local_time":"{time}","block_time":"{time}","block_number":{block},"events":[{event}]}}"#
    ))
    .unwrap()
}

/// A `node_order_statuses_streaming` line of block `height`: one BTC order's `status` (`canceled`, `triggered`, ...),
/// in the field set of 2026-10-20.
pub(crate) fn status_line(height: u64, status: &str, user: Address, oid: u64, cloid: Option<&str>) -> Batch<NodeDataOrderStatus> {
    let cloid = cloid.map_or_else(|| "null".to_string(), |cloid| format!(r#""{cloid}""#));
    sonic_rs::from_str(&format!(
        r#"{{"local_time":"2026-10-19T06:40:00.6","block_time":"2026-10-19T06:40:00.5","block_number":{height},"events":[{{"time":"2026-10-19T06:40:00.5","user":"{user:#x}","hash":null,"builder":null,"status":"{status}","order":{{"coin":"BTC","side":"B","limitPx":"82850.0","sz":"0.0","oid":{oid},"timestamp":1,"triggerCondition":"N/A","isTrigger":false,"triggerPx":"0.0","children":[],"isPositionTpsl":false,"reduceOnly":false,"orderType":"Limit","origSz":"0.001","tif":"Alo","cloid":{cloid}}}}}]}}"#
    ))
    .unwrap()
}

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

/// A `LedgerUpdate` inner: a USDC `send` of `amount` between two dexes by name ("" main, "spot"), no fee; the field
/// set of the real deltas of 2026-10-20.
pub(crate) fn send_inner(from: (Address, &str), to: (Address, &str), amount: &str) -> String {
    let ((user, source), (destination, destination_dex)) = (from, to);
    let users = if user == destination { format!(r#""{user:#x}""#) } else { format!(r#""{user:#x}","{destination:#x}""#) };
    format!(
        r#"{{"LedgerUpdate":{{"users":[{users}],"delta":{{"type":"send","user":"{user:#x}","destination":"{destination:#x}","sourceDex":"{source}","destinationDex":"{destination_dex}","token":"USDC","amount":"{amount}","usdcValue":"{amount}","fee":"0.0","nativeTokenFee":"0.0","nonce":1,"feeToken":""}}}}}}"#
    )
}

/// A `LedgerUpdate` inner with a delta of `delta_type` and no other field: what the replay reads only by name.
pub(crate) fn unread_ledger_inner(user: Address, delta_type: &str) -> String {
    format!(r#"{{"LedgerUpdate":{{"users":["{user:#x}"],"delta":{{"type":"{delta_type}"}}}}}}"#)
}

/// A `Funding` inner of these (user, coin, `funding_amount`) deltas.
pub(crate) fn funding_inner(deltas: &[(Address, &str, &str)]) -> String {
    let deltas: Vec<String> = deltas
        .iter()
        .map(|(user, coin, amount)| {
            format!(r#"{{"user":"{user:#x}","coin":"{coin}","funding_amount":"{amount}","szi":"1.0","funding_rate":"0.0000125"}}"#)
        })
        .collect();
    format!(r#"{{"Funding":{{"deltas":[{}]}}}}"#, deltas.join(","))
}

/// A parsed `replica_cmds` block of these kept actions (the line format is tested in `actions`).
/// Block `height` at block time `height` seconds, as the test anchors.
pub(crate) fn block_actions(height: u64, actions: &[(BlockAction, Address)]) -> BlockActions {
    BlockActions { height, time_ms: height * 1_000, actions: actions.to_vec() }
}

pub(crate) fn batch_line(block: u64, events: &str) -> String {
    format!(
        r#"{{"local_time":"2026-10-19T06:00:00.1","block_time":"2026-10-19T06:00:00.0","block_number":{block},"events":[{events}]}}"#
    )
}

pub(crate) fn row() -> AnchorRow {
    AnchorRow {
        isolated: false,
        pool_positions: Some(1),
        liquidation_px: LiquidationPx::Computed(0.004),
        mark: RowMark { mark_raw: None, distance_bps: None },
        portfolio_margin: None,
    }
}

/// Marks of one height for the view's tests, in price units (10^-(6 - szDecimals)).
pub(crate) struct ListedMarks {
    pub(crate) height: u64,
    pub(crate) marks: Vec<(&'static str, u64)>,
}

impl MarksAt for ListedMarks {
    fn height(&self) -> u64 {
        self.height
    }

    fn mark(&self, coin: &str) -> Option<u64> {
        self.marks.iter().find(|(listed, _)| *listed == coin).map(|(_, mark)| *mark)
    }
}

/// The fixture checkpoint's main-dex margin table `table_id`.
pub(crate) fn fixture_table(table_id: u32) -> MarginTable {
    parse_checkpoint_bytes(&fixture()).unwrap().dexes[0].table(table_id).clone()
}

/// 2026-10-19T06:37:23.556 UTC, the fixture checkpoint's time.
pub(crate) const SEED_TIME_MS: u64 = 1_792_391_843_556;

/// The fixture checkpoint's marks seed, moved to `height` (its HIP-3 coins: xyz:TSLA at 358.32, flx:GOLD at 41883.0).
pub(crate) fn marks_seed(height: u64) -> MarkSeed {
    let mut seed = MarkSeed::from_checkpoint(parse_checkpoint_bytes(&fixture()).unwrap()).unwrap();
    seed.height = height;
    seed.time_ms = SEED_TIME_MS + height;
    seed
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
                    (*user, AnchorPosition { size_raw: *size_raw, table_id: ANCHOR_TABLE, row: row() })
                })
                .collect();
            ((*name).to_string(), AnchorCoin { size_decimals: *size_decimals, table_id: CURRENT_TABLE, mark_raw: None, positions })
        })
        .collect();
    Anchor { height, time_ms: height * 1_000, coins, account_modes, margins: MarginStates::default() }
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

/// A book whose anchor at `height` holds these PUMP positions, with the fixture's margins (HOLDER default with main
/// dex cash -12000, ISOLATED's isolated PUMP cash 12000); every block up to `applied` is applied.
pub(crate) fn replay_book(height: u64, applied: u64, holders: &[(Address, i64)]) -> AccountBook {
    let mut book = AccountBook::new(0);
    book.apply_fills(fills(100, &[]));
    book.apply_misc_events(misc(100, &[]));
    book.apply_block_actions(block_actions(applied, &[]));
    // the fixture's universe has BTC (szDecimals 5) on the main dex too
    let mut first = anchor(height, &[(PUMP, 0, holders), ("BTC", 5, &[])]);
    first.margins = per_dex_with_main_cash(-12_000_000_000);
    book.install_anchor(first);
    book
}
