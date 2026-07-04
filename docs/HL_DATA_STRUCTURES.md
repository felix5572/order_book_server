# Hyperliquid 节点数据结构记录

本文档记录了通过 SSH 探索和代码分析发现的原始节点数据结构，特别是关于 TWAP 订单和 Fill 事件的部分。

## 1. TWAP 状态 (`node_twap_statuses_by_block`)

该目录包含 TWAP 订单的生命周期事件（因为文件名暗示 "statuses"）。

### JSON 示例
*来源: `~/hl/data/node_twap_statuses_by_block/hourly/20260128/15`*

```json
{
  "time": "2026-01-28T15:00:05.423822681",
  "twap_id": 1544034,
  "state": {
    "coin": "ETH",
    "user": "0xb90e02bd0033897061b272186d9f16340f153e87",
    "side": "A",
    "sz": "10.0",
    "executedSz": "0.0",
    "executedNtl": "0.0",
    "minutes": 5,
    "reduceOnly": false,
    "randomize": false,
    "timestamp": 1769612405423
  },
  "status": "activated"
}
```

### Rust Serde 定义

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TwapStatusEvent {
    pub time: String, // ISO 8601 string
    pub twap_id: u64,
    pub state: TwapState,
    pub status: String, // e.g., "activated", "terminated", "finished"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TwapState {
    pub coin: String,
    pub user: String, // Address string (hex)
    pub side: String, // "A" for Ask, "B" for Bid
    pub sz: String,
    #[serde(rename = "executedSz")]
    pub executed_sz: String,
    #[serde(rename = "executedNtl")]
    pub executed_ntl: String,
    pub minutes: u64, // Twap duration
    #[serde(rename = "reduceOnly")]
    pub reduce_only: bool,
    pub randomize: bool,
    pub timestamp: u64,
}
```

## 2. Fill 事件中的附加信息 (`node_fills_by_block`)

`Fill` 事件中的 `side_info` 字段包含了参与成交的具体订单信息，包括 `twap_id`。这对于追踪 TWAP 订单的执行情况至关重要。

### JSON 示例
*来源: `server/src/listeners/directory.rs` (Mock Data)*

```json
{
  "coin": "@151",
  "side": "A",
  "time": "2025-06-24T02:56:36.172847427",
  "px": "2393.9",
  "sz": "0.1539",
  "hash": "0x2b21750229be769650b604261eaac1018c00c45812652efbbdd35fe0ecb201a1",
  "trade_dir_override": "Na",
  "side_info": [
    {
      "user": "0xecb63caa47c7c4e77f60f1ce858cf28dc2b82b00",
      "start_pos": "1166.565307356",
      "oid": 105686971733,
      "twap_id": null,
      "cloid": "0x1070fff92506b3ab5e5aec135e5a5ddd"
    },
    {
      "user": "0xb65117c1e1006e7b2413fa90e96fcbe3fa83ed75",
      "start_pos": "0.153928559",
      "oid": 105686976226,
      "twap_id": 12345, 
      "cloid": null
    }
  ]
}
```
*(注：Mock数据中 `twap_id` 为 null，但字段存在。如果在实际数据中该字段有值，则表示该成交来自 TWAP 订单)*

### Rust Serde 定义

需要扩展现有的 `Fill` 结构体（位于 `server/src/types/mod.rs`）。

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Fill {
    pub coin: String,
    pub px: String,
    pub sz: String,
    pub side: String, // Assuming Side enum serializes to string
    pub time: u64,
    pub start_position: String,
    pub dir: String,
    pub closed_pnl: String,
    pub hash: String,
    pub oid: u64,
    pub crossed: bool,
    pub fee: String,
    pub tid: u64,
    pub fee_token: String,
    pub liquidation: Option<Liquidation>,
    // 新增字段
    pub side_info: Option<Vec<SideInfo>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SideInfo {
    pub user: String,
    pub start_pos: String,
    pub oid: u64,
    pub twap_id: Option<u64>,
    pub cloid: Option<String>,
}
```
