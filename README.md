# Hyperliquid Orderbook WebSocket Server

## Disclaimer

This was a standalone project, not written by the Hyperliquid Labs core team. It is made available "as is", without warranty of any kind, express or implied, including but not limited to warranties of merchantability, fitness for a particular purpose, or noninfringement. Use at your own risk.

This project has been further developed and maintained by [Imperator](https://hyperpc.app) to make the Hyperliquid public release production-ready and more efficient. Imperator provides no warranty, guarantee, or support obligation of any kind. The software is provided "as is" and you assume all risks associated with its use, including but not limited to data loss, system failure, or any other damages. Under no circumstances shall Imperator be held liable for any claim, damages, or other liability arising from the use of this software.

## Features

Real-time orderbook data from a local Hyperliquid node:

- **bbo** - Best Bid/Offer (top of book) with deduplication
- **l2Book** - Aggregated Level 2 orderbook with deduplication
- **trades** - Real-time trade feed
- **bookDiffs** - Raw book diff stream per coin
- **l4Book** - Full Level 4 orderbook with individual order details (price-banded one-shot reads via `GET /l4Book`)
- **untriggered orders** - Pending stop / TP-SL trigger orders per coin (one-shot reads via `GET /untriggeredOrders`)
- **orderUpdates** - User-specific order status stream

## Quick Start

### Prerequisites

1. **Hyperliquid Node** - Running on the same host with streaming enabled, writing its periodic abci checkpoints to `<data-dir>/periodic_abci_states/` (the node does this by default)
2. **Rust** - For building from source

### System Requirements

The server runs co-located with a Hyperliquid node and is memory-light relative to the node itself. Figures below are measured in production with ~630 coins (all markets: perps + spot + HIP-3) and ~440 k live orders.

| Profile | CPU | RAM (steady-state) | Notes |
|---------|-----|---------------------|-------|
| **Full (`--markets all`)** | 2 cores minimum, 4+ recommended | ~750 MB – 2 GB | Rises with connected clients and L2 subscription fanout. Reserve **4 GB** for headroom in production. |
| **Perps only (`--markets perps`)** | 2 cores | ~400 MB – 1 GB | Smaller universe ≈ smaller book and fewer L2 variants to compute. |
| **BBO-only (`--bbo-only`)** | 1 core | ~100 – 150 MB | Top-of-book only, no L2/L4/trades. |

Other requirements:

- **OS:** Linux with inotify (kernel ≥ 2.6.13). Tested on Ubuntu 22.04 / 24.04.
- **Architecture:** x86_64 or aarch64.
- **File descriptors:** raise `LimitNOFILE` in your service unit (the included one sets `1048576`). Each WebSocket client uses one fd.
- **Disk:** negligible for the server itself (~10 MB binary). The Hyperliquid node's `*_streaming/` directories grow with traffic — size them per the node's documentation, not this server's.
- **Network:** outbound bandwidth scales with `(connected clients) × (subscription mix)`. L2 snapshots with many `(coin, n_sig_figs, mantissa)` variants are the largest payloads — use `--compression-level 1` and/or `--markets perps` to bound it.

**Sizing rule of thumb.** Memory grows roughly with (a) the universe of coins the node serves, and (b) the number of distinct L2 subscription tuples your clients open. The 256-per-connection subscription cap and the broadcast channel capacity bound the worst case. If you see `channel_drops_total` rising or `broadcast_channel_lag` non-zero, you have a slow consumer — not a server-side leak.

### Build

```bash
git clone https://github.com/imperator-co/order_book_server.git
cd order_book_server
cargo build --release
```

### Run

The node must run on the same host (systemctl / bare metal); `--snapshot-mode direct` is the default and the only supported mode:
```bash
# IMPORTANT: copy the node binary to a name WITHOUT the string "hl-node" first,
# or the node's process-detection routine will kill itself when the server
# fetches a snapshot. See "Direct mode and hl-node's process detection" below.
cp /path/to/hl-node /usr/local/bin/ob-snapshotter

./target/release/orderbook_server \
    --snapshot-mode direct \
    --hlnode-binary /usr/local/bin/ob-snapshotter \
    --data-dir /path/to/volumes/hl/data
```

## Configuration

### Core Options

| Flag | Default | Description |
|------|---------|-------------|
| `--address` | `0.0.0.0` | Bind address |
| `--port` | `8000` | WebSocket port |
| `--compression-level` | `1` | WebSocket compression level (0-9). See [Compression](#compression) |
| `--markets` | `all` | `perps`, `spot`, `hip3`, `all` |
| `--log-level` | `info` | `error`, `warn`, `info`, `debug`, `trace` |

### Compression

WebSocket messages (especially L2/L4 snapshots) can be large. The `--compression-level` flag controls `permessage-deflate` compression applied to every outgoing frame:

| Level | Behavior | Use case |
|-------|----------|----------|
| `0` | Disabled | Lowest latency. Use for HFT or when your client doesn't support `permessage-deflate` |
| `1` | Fast compression | Best tradeoff for most setups. Minimal CPU cost, significant bandwidth savings |
| `5` | Balanced | Good compression ratio with moderate CPU |
| `9` | Best ratio | Maximum compression. Higher CPU cost, useful for bandwidth-constrained links |

Your WebSocket client must support `permessage-deflate` for levels 1-9 to have any effect. If it doesn't, use `0`.

### Snapshot Mode

On startup, the server needs a **full L4 orderbook snapshot** to initialize its in-memory state. It obtains this by calling the `hl-node` binary's `compute-l4-snapshots` CLI on one of the node's **periodic abci checkpoints** (`<data-dir>/periodic_abci_states/<YYYYMMDD>/<height>.rmp`) and loading the JSON dump of every order on the book.

A checkpoint is the node state after applying block `<height>`, and the node writes one every fixed number of blocks (10,000 on mainnet, about every 12 minutes). The dump itself carries no height, so the server always dumps a specific checkpoint file and takes the height from its name. (`hyperliquid_data/abci_state.rmp` is a hard link to the newest checkpoint; it is not dumped directly because the node can swap it mid-dump.)

`--snapshot-mode direct` (the default) is the only supported mode: the server calls the `hl-node` binary on this host. `--snapshot-mode docker` is rejected at startup.

#### Direct mode and hl-node's process detection

> **Warning:** `hl-node` ships with a self-protection routine that panics with `matching procs found for keywords ["hl-node"]` whenever it sees another process whose command line contains the string `hl-node` - the same mechanism that kills the node if you run `journalctl | grep hl-node`. It applies across OS users and takes down **both** the node and the offending process.
>
> In direct mode this server spawns `hl-node --chain Mainnet compute-l4-snapshots ...` at startup **and again on every background desync re-fetch**, so a stock setup can run fine for a while and then kill the node the moment a re-sync coincides with the scan.
>
> Workaround: copy the node binary to a name that does not contain `hl-node` (e.g. `cp hl-node /usr/local/bin/ob-snapshotter`) and pass that via `--hlnode-binary`. Two extra rules:
>
> 1. **Keep the copy fresh** - hl-visor auto-updates the node binary, and a stale copy may eventually fail to read a newer checkpoint. Re-copy after upgrades (a cron job or an `ExecStartPre=` in your systemd unit works).
> 2. **Never put the string `hl-node` in the `--hlnode-binary` value** (or anywhere else in this server's command line / unit file) - it would sit in the server's own argv permanently and trip the same scan.

After the initial snapshot, the server stays up to date by watching the node's `*_streaming/` directories for real-time order diffs, fills, and status updates via inotify.

The handoff from snapshot to live stream is **gapless** and **checkpoint-aligned**: a fetch targets the next checkpoint on the node's grid, opens a replay cache shortly before both book streams reach it (once both are tracking and neither has passed it), waits for the node to write it, dumps it, and replays every cached event above its height. After the install, book batches at or below that height (a lagging stream's backlog) are skipped - the snapshot already contains them. The cost is waiting for the next checkpoint: a fetch takes up to one checkpoint interval (~12 minutes, ~6 on average) plus the dump, during which the server keeps serving the previous book (startup: not ready until the first install). If the server ever detects that events were provably lost (a corrupt line, an oversized batch, watcher data loss), it marks the book out-of-sync and automatically re-fetches a snapshot this way in the background while continuing to serve — see `orderbook_desyncs_total` in the metrics.

| Flag | Default | Description |
|------|---------|-------------|
| `--snapshot-mode` | `direct` | Only `direct` is supported; `docker` is rejected at startup |
| `--hlnode-binary` | `hl-node` | Path to the hl-node binary on this host (see the process-detection warning above) |
| `--data-dir` | `~` | Path to the folder containing `node_fills_streaming/`, `node_order_statuses_streaming/`, `node_raw_book_diffs_streaming/`, and `periodic_abci_states/`. This is where the node writes its real-time event files and checkpoints |
| `--snapshot-output-path` | auto | Path where `hl-node` writes its JSON snapshot output. Defaults to `/tmp/hl_snapshot.json`. Override if `/tmp` is not writable or you want snapshots stored elsewhere |

### Market Types

| Value | Description |
|-------|-------------|
| `perps` | Perpetual futures only (BTC, ETH, SOL...) |
| `spot` | Spot markets (`@*` coins, PURR/USDC) |
| `hip3` | HIP-3 markets (X:Y format, e.g., xyz:TSLA) |
| `all` | All markets (default) |

### Advanced Options

| Flag | Default | Description |
|------|---------|-------------|
| `--metrics-port` | `9090` | Prometheus metrics port (0 to disable) |
| `--bbo-only` | `false` | Lightweight BBO-only mode (~100 MB RAM, vs ~1 GB for full markets). Disables L2/L4/Trades subscriptions |
| `--l2book-heartbeat-ms` | `0` | If > 0, resend the last `l2Book` payload for each active subscription every N ms when nothing has changed. See [Heartbeats](#heartbeats) |
| `--bbo-heartbeat-ms` | `0` | If > 0, resend the last `bbo` payload for each active subscription every N ms when nothing has changed. See [Heartbeats](#heartbeats) |
| `--no-resync` | `false` | Tolerate drift instead of re-syncing. Data-loss events are still counted in `orderbook_desyncs_total` but never trigger a snapshot re-fetch. See [Drift tolerance](#drift-tolerance) |

### Drift tolerance

By default, whenever the server detects provable data loss (e.g. a node stream stalls and pending order halves are force-evicted) it marks the book out-of-sync and re-fetches a full snapshot to rebuild from a known-good state.

In rare conditions this re-sync can fail to converge: if loss keeps being recorded at the live stream head faster than a snapshot can be fetched, every snapshot lands at a height *below* the recorded loss bound, so the re-sync flag never clears and the server re-fetches in a loop. `--no-resync` is an operator escape hatch for that case:

- Desyncs are still counted in `orderbook_desyncs_total` (per reason), so drift stays visible in metrics.
- No snapshot re-fetch is ever scheduled; the book keeps serving live events through the drift.
- **The book does NOT self-heal** — missing orders stay missing until the process is restarted.

Use it only when a non-converging re-sync loop is worse than serving a knowingly-incomplete book. The startup snapshot still loads normally; only *re*-syncs are suppressed. The right long-term fix is addressing the upstream cause (typically the OrderStatus stream lagging the OrderDiff stream), not leaving this flag on.

### Heartbeats

By default, `l2Book` and `bbo` channels are **change-only**: a snapshot is only sent when the underlying book state actually moves. On quiet markets (low-liquidity coins like `Frudo`) this can produce zero messages for minutes at a time, which breaks downstream clients written against the official Hyperliquid API — that API pushes a snapshot every block as an implicit heartbeat, so consumers often treat silence as a dead stream and reconnect.

The `--l2book-heartbeat-ms` and `--bbo-heartbeat-ms` flags add an opt-in periodic resend:

- When set to `0` (default), the original change-only behavior is preserved. No new messages compared to previous versions.
- When set to e.g. `1000`, every active subscription receives a cached payload at most every 1000 ms even if nothing changed. The `time` field is refreshed to the current server time on every heartbeat so clients can distinguish "fresh-but-unchanged" from "stale".

Each subscription tracks its own `last_sent` timestamp, so a real change resets the heartbeat — you never get a real update and a heartbeat back-to-back. Per-subscription accounting also means many quiet coins do not produce a synchronized burst.

Pick a value that matches your downstream stall timer, typically `1000` ms for clients ported from the official API.

```bash
# Behave like the official HL l2Book stream
./target/release/orderbook_server \
    --l2book-heartbeat-ms 1000 \
    --bbo-heartbeat-ms 1000 \
    --data-dir /path/to/data
```

## Recommended Configurations

### Low Latency Trading

Optimized for sub-millisecond BBO updates. Compression is disabled to eliminate encoding overhead. Market scope is narrowed to perps only, reducing the number of coins tracked and memory usage.

```bash
./target/release/orderbook_server \
    --compression-level 0 \
    --markets perps \
    --data-dir /path/to/data
```

### General Purpose

Balanced configuration for dashboards, analytics, or multi-market monitoring. Light compression (`1`) provides significant bandwidth savings with negligible CPU cost.

```bash
./target/release/orderbook_server \
    --compression-level 1 \
    --markets all \
    --data-dir /path/to/data
```

### BBO-Only (Lightweight)

Track only the top-of-book bid/ask for all coins. Uses ~100 MB RAM (vs ~1 GB for full markets). Ideal for price feeds, alerting, or environments with limited memory.

```bash
./target/release/orderbook_server \
    --bbo-only \
    --compression-level 0 \
    --data-dir /path/to/data
```

## WebSocket API

### Subscribe to BBO
```json
{ "method": "subscribe", "subscription": { "type": "bbo", "coin": "BTC" } }
```
Response:
```json
{ "channel": "bbo", "data": { "coin": "BTC", "time": 1702530000000, "bbo": [{ "px": "100000.0", "sz": "0.5", "n": 1 }, { "px": "100001.0", "sz": "0.3", "n": 1 }] } }
```

### Subscribe to Trades
```json
{ "method": "subscribe", "subscription": { "type": "trades", "coin": "BTC" } }
```
Response:
```json
{ "channel": "trades", "data": [{ "coin": "BTC", "side": "A", "px": "106296.0", "sz": "0.00017", "time": 1751430933565, "hash": "0x...", "tid": 293353986402527, "users": ["0x<buyer>", "0x<seller>"] }] }
```
Each trade is one print per match, following the official API schema: `side` is the aggressing (taker) side and `users` is `[buyer, seller]`.

### Subscribe to Book Diffs
```json
{ "method": "subscribe", "subscription": { "type": "bookDiffs", "coin": "BTC" } }
```
Response:
```json
{ "channel": "bookDiffs", "data": [{ "user": "0x...", "oid": 123, "px": "50000.0", "coin": "BTC", "rawBookDiff": { "new": { "sz": "1.0" } } }] }
```
Streams raw order book diffs as they arrive. Each diff is one of: `new` (order added), `update` (size changed with `origSz` and `newSz`), or `remove` (order removed).

### Subscribe to L2 Orderbook
```json
{ "method": "subscribe", "subscription": { "type": "l2Book", "coin": "BTC" } }
```
Optional parameters: `nSigFigs` (2-5), `nLevels` (max 400, default 20), `mantissa` (2 or 5)

Aggregation matches HL's public API semantics: the **full** book is bucketed by `nSigFigs`/`mantissa` first, then truncated to `nLevels` aggregated buckets — so coarse groupings (e.g. `nSigFigs: 2`) return deep ladders spanning far from the mid, not just the near-mid raw levels.

### Subscribe to L4 Orderbook
```json
{ "method": "subscribe", "subscription": { "type": "l4Book", "coin": "BTC" } }
```
> **Warning:** The initial L4 snapshot contains every individual order in the book and can be **very large** (several MB for liquid coins like BTC/ETH). Some WebSocket clients (e.g., Postman) may not handle payloads of this size. Use a capable client like `wscat` or `websocat`, or connect programmatically. After the initial snapshot, subsequent updates are incremental and lightweight. If you only need part of the book (or a one-shot view), use `GET /l4Book` below instead of subscribing.

### One-shot L4 Snapshot (HTTP)
For request/response use cases — e.g. a click-to-inspect overlay that needs the resting orders around one price — there is a plain HTTP endpoint. No subscription lifecycle, no update stream: every request returns the book slice **as of now**, so repeating the same request later simply returns current state:
```
GET /l4Book?coin=BTC&minPx=64000&maxPx=66000
```
Optional parameters: `minPx`, `maxPx` (decimal strings, e.g. `"64000.0"`). When present, the snapshot includes only orders with `limitPx` in the inclusive range `[minPx, maxPx]`; either bound may be given alone for a one-sided range, and `minPx` must be <= `maxPx`. The band is applied before the snapshot is built, so a narrow band on a deep book returns kilobytes in milliseconds instead of tens of megabytes. Omitting both bounds returns the full book.

The response body is identical to the WS l4Book message's `data` field (`{"Snapshot":{"coin","time","height","levels":[[bids],[asks]]}}`), so parsing can be shared with the WS path. Errors: `400` for an invalid band, `404` when the coin has no book.

Send `Accept-Encoding: gzip` (curl: `--compressed`) — order JSON compresses ~6-10x, so wide bands go from MB to hundreds of KB on the wire; without it, transfer time dwarfs the ~10ms server build for remote clients.

### One-shot Untriggered Orders (HTTP)
Trigger orders (stop-loss / take-profit / stop-market) do **not** rest on the book until their trigger price is crossed, so they are invisible to `l4Book`. The server tracks them separately — bootstrapped from the hl-node snapshot and maintained live from the order-status stream — and serves them over plain HTTP:
```
GET /untriggeredOrders            # all coins
GET /untriggeredOrders?coin=BTC   # one coin
```
Response:
```json
{
  "time": 1702530000000,
  "height": 123456,
  "data": [
    ["BTC", [["0x<owner>", { "coin": "BTC", "side": "B", "limitPx": "64100", "sz": "1.25", "oid": 511698638823, "isTrigger": true, "triggerPx": "64000.0", "orderType": "Stop Market", ... }], ...]],
    ...
  ]
}
```
`data` is sorted by coin, each order an `[ownerAddress, order]` tuple in the same shape as l4Book orders (`isTrigger: true`, `triggerPx` set). A coin with no pending triggers is simply absent (or `data` is empty) — that is a legitimate state, not an error. Returns `503` until the initial snapshot has been installed. Supports `Accept-Encoding: gzip` like `/l4Book`.

Notes:
- The snapshot invocation passes `--include-trigger-orders`, so every install seeds the table with the **full standing set** of pending trigger orders from the node's state (the dump's per-coin `untriggered_orders` section) — coverage is complete from first boot, including stops placed long before the server started. Entries are then maintained live: upserted on `"open"` statuses, evicted on any terminal status (`canceled`, `triggered`, `filled`, `rejected`, ...). If a dump ever yields no triggers (older hl-node without the flag), the accumulated table is carried forward across the install rather than wiped. Evictions are counted per status in `orderbook_untriggered_evictions_total{status}` — a novel status label after a node upgrade is the signal to re-check the eviction rule.
- `limitPx` and `sz` are canonicalized through the server's fixed-point types (trailing zeros stripped: `"64100.0"` → `"64100"`); `triggerPx` passes through as the node wrote it and is safe to use as an aggregation key.
- Position TP/SL orders (`isPositionTpsl: true`, sized to the whole position) carry `sz: "0"` — consumers that drop zero-size rows must special-case them if they need position-attached stops.
- `children` is always `[]` (the server does not retain child order data, same as l4Book). Child TP/SL orders appear as their own top-level entries once the node emits statuses for them.
- Disabled in `--bbo-only` mode (the endpoint serves empty `data`) to preserve its lightweight memory envelope. Track the set's size via `orderbook_untriggered_orders_total`.

### Subscribe to Order Updates (User-Specific)
Stream raw order status data for a specific user address:
```json
{ "method": "subscribe", "subscription": { "type": "orderUpdates", "user": "0x1234567890abcdef1234567890abcdef12345678" } }
```
Response:
```json
{ "channel": "orderUpdates", "data": [{ "user": "0x...", "time": 1702530000000, "height": 12345, "orderStatus": { "time": "...", "user": "0x...", "hash": "0x...", "status": "open", "order": { "coin": "BTC", "side": "B", "limitPx": "100000.0", "sz": "0.5", ... } } }] }
```
> **Note:** Requires node to run with `--write-order-statuses` flag enabled.

> **Ordering:** within one block, updates for the same coin preserve their original order; updates spanning multiple coins may arrive grouped by coin. `time` and `height` are identical across the whole message.

### Ping/Pong
```json
{ "method": "ping" }
```
Response:
```json
{ "channel": "pong" }
```

### Unsubscribe
```json
{ "method": "unsubscribe", "subscription": { "type": "l2Book", "coin": "BTC" } }
```
An unsubscribe must match the original subscription, including any optional parameters.

## Node Requirements

The Hyperliquid node must run with **all** of these flags enabled:
- `--write-fills`
- `--write-order-statuses`
- `--write-raw-book-diffs`
- `--stream-with-block-info` — **required**, the server only reads from `*_streaming` directories
- `--disable-output-file-buffering` — ensures data is flushed immediately for low latency

## Architecture

```
┌──────────────────────┐     ┌──────────────────────────────────────────────┐
│   Hyperliquid Node   │     │  Orderbook Server                           │
│   (same host)        │     │                                             │
│                      │     │  ┌──────────────────────────────┐           │
│  writes to:          │     │  │ Parallel File Watchers       │           │
│  - fills_streaming/  │─────▶  │ (3 inotify threads)          │           │
│  - order_statuses_   │     │  │  - order diffs (BBO-critical)│           │
│    streaming/        │     │  │  - order statuses            │           │
│  - book_diffs_       │     │  │  - fills                     │           │
│    streaming/        │     │  └──────────┬───────────────────┘           │
│                      │     │             │ bounded tokio channel         │
│                      │     │  ┌──────────▼───────────────────┐           │
│  snapshot via:       │     │  │ OrderBook State              │           │
│  hl-node CLI ◀───────│─────│  │  - L4 in-memory book         │           │
│  (startup only)      │     │  │  - L2 snapshot computation   │           │
│                      │     │  │  - BBO deduplication         │           │
│                      │     │  └──────────┬───────────────────┘           │
│                      │     │             │ broadcast channel             │
│                      │     │  ┌──────────▼──────┐  ┌─────────────────┐  │
│                      │     │  │ WebSocket Server │──│ Connected       │  │
│                      │     │  │ (axum + yawc)    │  │ Clients         │  │
└──────────────────────┘     │  └──────────────────┘  └─────────────────┘  │
                             │                                             │
                             │  ┌─────────────────┐                        │
                             │  │ Metrics Server   │  GET /metrics         │
                             │  │ (Prometheus)     │  GET /health          │
                             │  └─────────────────┘                        │
                             └──────────────────────────────────────────────┘
```

**Data flow:**
1. The Hyperliquid node writes real-time events to `*_streaming/` directories as newline-delimited JSON
2. Three parallel inotify file watchers detect changes immediately (one per event source)
3. Watcher threads send events straight into a bounded tokio channel (backpressure parks the readers; data waits on disk)
4. The OrderBook State applies diffs/statuses independently (no block-level batching) for lowest latency
5. Changed BBOs and L2 snapshots are broadcast to subscribed WebSocket clients with deduplication

## Performance

### Consistency (no-drift) guarantees

The in-memory book is kept consistent with the node through three layers:

1. **Checkpoint-aligned snapshots** - every snapshot is a dump of one periodic abci checkpoint, whose height comes from its file name. The replay cache is opened before either book stream passes that height, so nothing between the checkpoint and the live stream is skipped; watchers start at end-of-file because everything older is covered by the checkpoint. (Dumping the newest checkpoint with a cutoff at the node's *current* height used to skip up to a whole checkpoint interval - cancelled orders came back as ghosts, new ones went missing.)
2. **Snapshot replay** - every book-affecting event above the checkpoint height is cached while the snapshot is fetched and replayed onto it; after the install, a lagging stream's batches at or below the checkpoint height are skipped (`orderbook_stale_batches_skipped_total`) instead of rolling the snapshot back.
3. **Pairing-window cancels** - a `Remove`/`Update` diff that arrives while its order's `New` diff is still waiting for its status (the statuses stream lags the diffs stream) acts on the pending entry, so the late status never books a cancelled order or a stale size. Diffs that find their order neither booked nor pending are counted in `orderbook_diff_target_missing_total`.
4. **Desync self-healing** - any provable event loss (parse/apply error on a batch, oversized batch, watcher buffer discard, pending-cache eviction) marks the book out-of-sync and triggers an automatic background snapshot re-fetch. The book keeps serving its current state until the fresh snapshot lands. Each loss is recorded with a block-height bound, and the out-of-sync flag only clears once a snapshot's height actually covers that bound - a snapshot generated from lagging node state cannot mask a newer loss. Each occurrence is counted in `orderbook_desyncs_total{reason}`. The status/diff pairing caches are evicted by age (60 s): expected orphans are dropped silently, while an unpaired diff aging out counts as data loss and re-syncs the book.
5. **Watcher watchdog** - if a file watcher thread dies (or every watcher channel closes), the server exits so the process supervisor restarts it into a clean re-sync, instead of serving a silently frozen book that still reports `ready`. A watcher that is alive but has produced no events for 2 minutes is loud-logged (restarting would not fix a stalled node).

### Deduplication

| Type | Behavior |
|------|----------|
| BBO | Only sends when bid/ask px/sz changes. When a coin's book empties (e.g. delisting), one final update with both `bbo` sides `null` is sent (same `[bid, ask]` shape as the official API) |
| L2Book | Only sends when snapshot hash changes. When a coin's book empties, one final snapshot with empty levels is sent |
| Trades | Only sends on fills |

Dedup state is tracked per `(coin, nSigFigs, mantissa, nLevels)` tuple, so subscriptions that differ only in `nLevels` deduplicate independently.

### Latency

| Metric | Value |
|--------|-------|
| BBO update frequency | ~100+/sec (streaming) |
| BBO latency | ~100ms |
| BBO computation | O(1) - each price level maintains a running (size, count) aggregate |
| L2 base snapshot build | O(price levels), independent of order count per level |
| BBO dedup overhead | <1us (raw fixed-point comparison, no allocation) |
| L2 dedup overhead | ~10us |
| Savings when unchanged | ~500us |

Fan-out payloads (trades, book diffs, L4 updates) are grouped per coin once in the listener and shared across all subscribed connections via `Arc`. The JSON wire frame for every channel - including `l2Book` (per `coin`/`nSigFigs`/`mantissa`/`nLevels` variant, covering the level-export and dedup-hash work too) and `bbo` - is built once per broadcast by the first subscribed connection; every other connection sends the same refcounted bytes zero-copy, so fan-out CPU no longer scales with the subscriber count. `TCP_NODELAY` is set on every accepted socket so small frames (BBO updates) are never delayed by Nagle's algorithm.

## Prometheus Metrics

Metrics are exposed on port 9090 by default (configurable via `--metrics-port`):

```bash
curl http://localhost:9090/metrics
```

### Available Metrics

| Category | Metric | Description |
|----------|--------|-------------|
| **Connections** | `ws_connections_active` | Current WebSocket connections |
| | `ws_connections_total` | Total connections since startup |
| | `ws_subscriptions_active{type}` | Active subscriptions by type (bbo/l2Book/l4Book/trades/bookDiffs/orderUpdates) |
| | `broadcast_receivers` | Number of broadcast channel receivers |
| **Throughput** | `events_processed_total{type}` | Events by type (orders/diffs/fills) |
| | `broadcasts_total{channel}` | Broadcasts by channel (bbo/l2/l4/trades) |
| | `messages_sent_total` | Total WebSocket messages sent |
| | `bbo_changes_total{coin}` | BBO changes per coin |
| **Health** | `orderbook_height` | Current block height |
| | `orderbook_time_ms` | Orderbook timestamp |
| | `orderbook_orders_total` | Total orders in the book |
| | `orderbook_untriggered_orders_total` | Untriggered trigger orders tracked (stops / TP-SL waiting for trigger price) |
| | `orderbook_untriggered_evictions_total{status}` | Untriggered-order evictions by causing status (a novel label flags a possible wrong eviction) |
| | `orderbook_coins_count` | Number of coins tracked |
| | `pending_orders_cache_size` | Pending order statuses in HFT cache |
| | `pending_diffs_cache_size` | Pending book diffs in HFT cache |
| | `uptime_seconds` | Server uptime in seconds |
| | `server_start_time_seconds` | Server start timestamp (unix) |
| **Latency** | `bbo_broadcast_latency_seconds` | BBO broadcast latency histogram |
| | `l2_broadcast_latency_seconds` | L2 broadcast latency histogram |
| | `l2_conflation_batch_size` | Coins rebuilt per L2 broadcast (changed within the 50 ms throttle window) |
| | `event_processing_latency_seconds{event_type}` | Per-event processing latency |
| **File Watcher** | `file_events_total{source}` | File events received by source |
| | `file_lines_parsed_total{source}` | Lines parsed from files by source |
| **Errors** | `parse_errors_total{type}` | JSON parse errors by source |
| | `ws_send_errors_total` | WebSocket send errors |
| | `channel_drops_total` | Messages dropped due to lag |
| | `broadcast_channel_lag` | Broadcast channel lag (receivers behind) |
| | `orderbook_desyncs_total{reason}` | Times the book was marked out-of-sync (each triggers an automatic background snapshot re-fetch). Alert on a sustained rate; occasional self-heals are benign |

### Disable Metrics

```bash
./target/release/orderbook_server --metrics-port 0
```

## Health Check

A health endpoint is available on the same port as the WebSocket server:

```bash
curl http://localhost:8000/health
```

Response:
```json
{"status":"ready","uptime_seconds":3600,"height":123456,"connections":5}
```

| Field | Description |
|-------|-------------|
| `status` | `ready` or `initializing` |
| `uptime_seconds` | Server uptime |
| `height` | Current block height |
| `connections` | Active WebSocket connections |

## Deployment

### Security & exposure model

**This server ships with no TLS, no authentication, no authorization, and no per-IP rate limiting.** Those responsibilities live in your deployment infrastructure. If you bind the server to a public interface without a reverse proxy in front of it, an attacker can DoS it with a handful of TCP sockets. Treat the WebSocket port as you would a Postgres or Redis port: private, fronted, and rate-limited.

Recommended deployment topology:

```
   Public Internet ──HTTPS/WSS──▶  Nginx / Caddy / Cloudflare  ──HTTP/WS──▶  orderbook_server (127.0.0.1:8000)
                                   (TLS, Origin, rate limits)              (no public exposure)
```

Concretely:

1. **Bind the server to `127.0.0.1`** (or a private/VPC interface). The `--address 0.0.0.0` example in *Quick Start* is fine on a private network or behind a firewall but should not be used directly on the public internet.
2. **Terminate TLS at the reverse proxy.** Let's Encrypt + Caddy/Nginx is the easy default; cloud LBs / Cloudflare also work.
3. **Set per-IP connection and message limits at the proxy.** A single client subscribing to a few thousand `(coin, n_sig_figs, mantissa)` tuples can already make every other client pay for it; the server applies a hard cap of 256 subscriptions per WS connection, but the proxy is your first line of defense.
4. **Enforce an `Origin` allowlist** if browser clients will connect, so other websites can't open WS connections from a user's session.
5. **Lock down the metrics endpoint.** `--metrics-port` binds to `0.0.0.0:9090` by default and exposes internal telemetry that helps an attacker fingerprint your fleet. Run with `--metrics-port 0` to disable it, or proxy/firewall it to your Prometheus scraper only. There is no auth on `/metrics`.
6. **Run `hl-node` and `orderbook_server` as separate Unix users.** `orderbook_server` only needs read access to the streaming directories (`node_*_streaming/`). It should *not* be able to write into the node's state.
7. **Use the included systemd unit as a starting point**, but tighten it: set `LimitNOFILE`, `LimitNPROC`, `MemoryHigh` / `MemoryMax`, `Restart=on-failure`, and a short `RestartSec`. A short restart loop is acceptable because the server reloads its snapshot on startup.

Minimal Nginx snippet for fronting the server (adjust paths and TLS as needed):

```nginx
upstream orderbook {
    server 127.0.0.1:8000;
    keepalive 4;
}

map $http_upgrade $connection_upgrade {
    default upgrade;
    ''      close;
}

limit_conn_zone $binary_remote_addr zone=ob_perip:10m;

server {
    listen 443 ssl http2;
    server_name your-domain.example;

    ssl_certificate     /etc/letsencrypt/live/your-domain.example/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/your-domain.example/privkey.pem;

    location /ws {
        limit_conn ob_perip 8;            # at most 8 concurrent WS per IP
        proxy_pass http://orderbook;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $connection_upgrade;
        proxy_read_timeout 600s;          # match your heartbeat cadence
        client_max_body_size 1k;          # subscribe messages are tiny
    }
}
```

If you must run without a proxy (development, internal-only), at least bind to `127.0.0.1` and ssh-tunnel into it.

### Systemd Service

An example service file is included (`orderbook-server.service`):

```bash
# Copy and edit the service file
cp orderbook-server.service /etc/systemd/system/
vim /etc/systemd/system/orderbook-server.service  # adjust paths

# Enable and start
systemctl daemon-reload
systemctl enable orderbook-server
systemctl start orderbook-server

# View logs
journalctl -u orderbook-server -f
```

## Caveats

- **Untriggered orders are HTTP-only** - Pending trigger orders are served via `GET /untriggeredOrders` (no WS subscription); the book channels (`bbo`/`l2Book`/`l4Book`) still show resting orders only
- **Snapshot sync time** - Initial snapshot takes ~10-30 seconds
- **Direct mode requires a renamed node binary** - hl-node's process detection kills the node if it sees the string `hl-node` in any other process's command line, including this server's snapshot invocations. See [Direct mode and hl-node's process detection](#direct-mode-and-hl-nodes-process-detection)

## Differences from the Hyperliquid Public Release

This project started from the [Hyperliquid public orderbook server](https://github.com/hyperliquid-dex/order_book) and was substantially rewritten for production use. Here is what changed:

### Event Processing: Block-Batched vs Event-by-Event

The original server batches events by block number and waits for an entire block's worth of updates before applying them. This adds latency proportional to block time.

This fork processes every order diff, status, and fill **the instant it arrives** from the node's streaming files, without waiting for block boundaries. The result is ~100+ BBO updates per second with ~100ms end-to-end latency.

### File Watching: Single Thread vs 3 Parallel Threads

The original uses a single file watcher thread that handles all three event sources (order statuses, book diffs, fills) sequentially.

This fork spawns **3 dedicated inotify threads** (one per event source) that feed a bounded tokio channel directly. Order diffs (the BBO-critical path) are never blocked by slow fill or status parsing.

### New Subscription Types

| Feature | Original | This Fork |
|---------|----------|-----------|
| L2Book | Yes | Yes |
| L4Book | Yes | Yes |
| Trades | Yes | Yes |
| **BBO** | No | Yes - dedicated top-of-book feed with per-coin deduplication |
| **orderUpdates** | No | Yes - per-user order status stream (filter by address) |

### Deduplication

The original sends every update to every subscriber regardless of whether anything changed.

This fork deduplicates at the WebSocket level:
- **BBO**: only sends when bid/ask px/sz actually changes (~1us overhead)
- **L2Book**: only sends when the snapshot hash changes (~10us overhead)
- Saves ~500us per unchanged update and significantly reduces client-side bandwidth

### Drift Protection & Self-Healing

The original (and earlier versions of this fork) could silently drift from the node: events arriving during the initial snapshot window were dropped, and any later data loss (corrupt line, oversized batch) corrupted the book permanently until a restart.

This fork makes the snapshot-to-stream handoff gapless (checkpoint-aligned snapshots + height-filtered replay of events cached during snapshot generation) and self-heals from any detected data loss by automatically re-fetching a snapshot in the background. See [Consistency (no-drift) guarantees](#consistency-no-drift-guarantees).

### BBO-Only Lightweight Mode

New `--bbo-only` flag reduces memory from ~1 GB to ~100 MB by only tracking the top-of-book bid/ask per coin. L2/L4/Trades subscriptions are disabled. Useful for price feeds, alerting, or memory-constrained environments.

### Snapshots from Periodic Checkpoints

The original fetches snapshots via HTTP POST to `localhost:3001` (the node's local RPC).

This fork calls `hl-node compute-l4-snapshots` on the host against a specific periodic abci checkpoint (`<data-dir>/periodic_abci_states/`), so the snapshot height is exact. Only direct (same-host) mode is supported.

### Market Filtering

The original has a hard-coded `ignore_spot` flag. This fork adds a `--markets` CLI flag supporting `perps`, `spot`, `hip3` (HIP-3 tokens in X:Y format), or `all`.

### Prometheus Metrics & Health Endpoint

The original has no monitoring. This fork adds:
- **`/health`** endpoint with status, uptime, block height, and connection count
- **`/metrics`** endpoint exposing 25+ Prometheus metrics covering connections, subscriptions, latency histograms, throughput, orderbook health, parse errors, and file watcher stats
- Pre-built Grafana dashboard included in `monitoring/`

### WebSocket Compression

Both use `yawc` with `permessage-deflate`. This fork increases the broadcast channel buffer from 100 to 256 to reduce "channel lagged" drops under load, and adds detailed documentation on compression level tradeoffs.

### Other Improvements

- **Graceful shutdown** via `Ctrl+C` / `SIGINT` signal handling
- **Configurable log levels** (`--log-level error|warn|info|debug|trace`)
- **Faster JSON parsing** with `sonic-rs` on the hot path
- **Shared fan-out serialization** - trades/bookDiffs/l4Book wire frames are serialized once per coin and shared (refcounted bytes) across all subscribed connections
- **Batched event draining** - up to 64 watcher events are parsed outside the listener lock and applied under a single acquisition
- **Latency-tuned build & runtime** - thin-LTO single-codegen-unit release profile, `mimalloc` global allocator, `TCP_NODELAY` on accepted sockets
- **Watcher watchdog** - dead watcher threads exit the process (supervisor restarts into a clean re-sync) instead of silently freezing the book
- **Systemd service file** included for production deployment
- **Comprehensive CLI** with auto-detected defaults and full override support

### Summary

| | Original | This Fork |
|---|----------|-----------|
| Event model | Block-batched | Event-by-event |
| Drift protection | None | Checkpoint-aligned snapshot + replay + auto re-sync |
| File watchers | 1 thread | 3 parallel threads |
| BBO subscription | No | Yes + dedup |
| Order updates | No | Yes (per-user) |
| BBO-only mode | No | Yes (~100MB) |
| Metrics | None | 25+ Prometheus metrics |
| Health endpoint | No | Yes |
| Snapshot source | HTTP RPC | hl-node CLI on periodic checkpoints |
| Market filtering | Hard-coded | --markets flag |
| Graceful shutdown | No | Yes |
| JSON parser | serde_json | sonic-rs |

## Managed Orderbook Service

If you'd rather skip running your own infrastructure, or you need capabilities beyond what this open-source server provides, check out our managed offering at **[hyperpc.app/products/data/orderbook-websocket](https://hyperpc.app/products/data/orderbook-websocket)**.

- **Hosted orderbook feeds** - Connect directly and consume real-time BBO, L2, L4, and trade data without managing a node or this server yourself
- **Lower latency via direct sentry peering** - We can peer your orderbook server directly with a Hyperliquid sentry node for reduced hop count and tighter latencies
- **Extended data services** - Recurring L4 snapshots, liquidation feeds, historical orderbook data, and custom data pipelines tailored to your trading infrastructure
- **Custom deployments** - Dedicated instances, co-located setups, or private infrastructure built around your specific latency and throughput requirements

Reach out to us at [hyperpc.app](https://hyperpc.app) to discuss what you need.

## License

MIT
