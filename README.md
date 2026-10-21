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
- **userFills** - User-specific fills in the official `userFills` shape (increments only)

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

The handoff from snapshot to live stream is **gapless** and **checkpoint-aligned**: a fetch targets the next checkpoint on the node's grid, opens a replay cache shortly before both book streams reach it (once both are tracking and neither has passed it), waits for the node to write it, dumps it, and replays every cached event above its height. After the install, book batches at or below that height (a lagging stream's backlog) are skipped - the snapshot already contains them. The cost is waiting for the next checkpoint: a fetch takes up to one checkpoint interval (~12 minutes, ~6 on average) plus the dump (startup: not ready until the first install). If the server ever detects that events were provably lost (a corrupt line, an oversized batch, watcher data loss, a New diff whose status never came), it marks the book out-of-sync, re-fetches a snapshot this way in the background, and **withholds** the book until a snapshot covering the loss is installed — see [Desync self-healing](#consistency-no-drift-guarantees) and `orderbook_desyncs_total` / `orderbook_book_trusted` in the metrics.

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
| `--accounts` | `false` | Run the account view: per-coin positions and checkpoint-anchored liquidation prices at `GET /extCoinPositions` and the `extCoinPositions` subscription, the `extLiquidationCandidates` subscription, and the forced flow (`extForcedFlow`, WS and `GET`). Parses a ~1.8 GB checkpoint every ~12 minutes (~6 s of one core, ~750 MB peak RSS) and follows `node_fills_streaming/`, `misc_events_streaming/` and `replica_cmds/` (the node's block log, 6–10 MB/s). See [Account view](#account-view-extcoinpositions) |
| `--local-marks` | `false` | Compute every main-dex coin's local mark beside the node replay's (needs `--accounts`): 12 venue connections - spot Binance, OKX, Bybit, Kraken, Kucoin, Gate, MEXC; perps Binance, OKX, Bybit, Gate, MEXC - for the oracle and the external price; served as `extMarkPrices` (WS and `GET`) and the inputs at `GET /extMarkInputs`. See [Local marks](#local-marks-extmarkprices-extmarkinputs) |

### Drift tolerance

By default, whenever the server detects provable data loss (e.g. a node stream stalls and pending order halves are force-evicted) it marks the book out-of-sync and re-fetches a full snapshot to rebuild from a known-good state.

In rare conditions this re-sync can fail to converge: if loss keeps being recorded at the live stream head faster than a snapshot can be fetched, every snapshot lands at a height *below* the recorded loss bound, so the re-sync flag never clears and the server re-fetches in a loop. `--no-resync` is an operator escape hatch for that case:

- Desyncs are still counted in `orderbook_desyncs_total` (per reason), so drift stays visible in metrics.
- No snapshot re-fetch is ever scheduled; the book keeps serving live events through the drift (it is never withheld).
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

The response body is identical to the WS l4Book message's `data` field (`{"Snapshot":{"coin","time","height","levels":[[bids],[asks]]}}`), so parsing can be shared with the WS path. Errors: `400` for an invalid band, `404` when the coin has no book, `503` (`{"error":"book resyncing after data loss"}`) while the book is withheld after data loss.

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
  "complete": true,
  "data": [
    ["BTC", [["0x<owner>", { "coin": "BTC", "side": "B", "limitPx": "64100", "sz": "1.25", "oid": 511698638823, "isTrigger": true, "triggerPx": "64000.0", "orderType": "Stop Market", ... }], ...]],
    ...
  ]
}
```
`data` is sorted by coin, each order an `[ownerAddress, order]` tuple in the same shape as l4Book orders (`isTrigger: true`, `triggerPx` set). A coin with no pending triggers is simply absent (or `data` is empty) — that is a legitimate state, not an error. `complete` is true only when these are all the standing trigger orders: the last install's dump carried the trigger section for every market and every order of it converted, and every live trigger order since parsed. Otherwise it is false until the next such install, and the orders the table does hold are still served. A coin of a market the server does not keep (a spot coin under `--markets perps`) is never complete; all coins means all of the kept markets. Returns `503` until the initial snapshot has been installed, and while the book is withheld after data loss. Supports `Accept-Encoding: gzip` like `/l4Book`.

Notes:
- The snapshot invocation passes `--include-trigger-orders`, so every install seeds the table with the **full standing set** of pending trigger orders from the node's state (the dump's per-coin `untriggered_orders` section) — coverage is complete from first boot, including stops placed long before the server started. Entries are then maintained live: upserted on `"open"` statuses, evicted on any terminal status (`canceled`, `triggered`, `filled`, `rejected`, ...). If a dump carries no trigger section at all (an hl-node without the flag), the accumulated table is carried forward across the install rather than wiped, with `complete: false`. Evictions are counted per status in `orderbook_untriggered_evictions_total{status}` — a novel status label after a node upgrade is the signal to re-check the eviction rule.
- A TP/SL placed with an order (the parent's `children`: a limit order with its take profit and stop) waits while the parent is live: the node does not trigger it before the parent leaves the book (bm 2026-10-21, hours 08-09: 48 of 49 such children triggered after their parent's `filled`, the other after its `marginCanceled`; 8,326 of 138,751 untriggered orders of a dump were such children). The table still serves a waiting child here, but keeps it out of its trigger price index, so the forced flow (`extForcedFlow`) does not list it. As Hyperliquid's TP/SL documentation puts it - a full fill of the parent activates its children, a partial fill keeps them waiting, canceling the parent cancels them, a cancel for insufficient margin after a partial fill activates them -, the parent's `filled` and `marginCanceled` release its children (and those the line lists under their own oids: a modify moves them to the parent's successor, whose `open` lists none); every other status leaves them waiting until their own statuses end them (bm hours 08-09: after a `canceled`, 226 children got their own `canceled` in the block, 78 moved to a successor, none triggered later). A dump names them in its live orders' `children`, under their own oids; live, the parent's `open` lists them under the parent's oid and their own `open` lines follow in the block (bm 10-21 hour 09: 1,745 of 1,745 so), matched by user, coin, timestamp, side, trigger price and condition, order type, size and reduce-only; an announced child whose own `open` did not follow is counted in `orderbook_untriggered_unmatched_children_total` and the table is a gap (`childUnmatched`, the forced flow's trigger part `incomplete`) until a complete install.
- `limitPx` and `sz` are canonicalized through the server's fixed-point types (trailing zeros stripped: `"64100.0"` → `"64100"`); `triggerPx` passes through as the node wrote it and is safe to use as an aggregation key.
- Position TP/SL orders (`isPositionTpsl: true`, sized to the whole position) carry `sz: "0"` — consumers that drop zero-size rows must special-case them if they need position-attached stops.
- `children` is always `[]` (the server does not retain child order data, same as l4Book). Child TP/SL orders appear as their own top-level entries once the node emits statuses for them.
- Disabled in `--bbo-only` mode (the endpoint serves empty `data`, `complete: false`) to preserve its lightweight memory envelope. Track the set's size via `orderbook_untriggered_orders_total`.

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

### Subscribe to User Fills (User-Specific)
The user's fills in the same shape as the official API's incremental `userFills` frames (never an `isSnapshot` frame):
```json
{ "method": "subscribe", "subscription": { "type": "userFills", "user": "0x1234567890abcdef1234567890abcdef12345678" } }
```
Response (`user` echoed as subscribed; each fill carries the modeled fields — missing `cloid`/`twapId` come out as `null`, null `builderFee`/`liquidation` are omitted, unmodeled fields are dropped):
```json
{ "channel": "userFills", "data": { "user": "0x...", "fills": [{ "coin": "ETH", "px": "2512.0", "sz": "0.1201", "side": "A", "time": 1791829884085, "startPosition": "13.3473", "dir": "Close Long", "closedPnl": "0.134512", "hash": "0x...", "oid": 572293425349, "crossed": false, "fee": "-0.00905", "tid": 706893944732179, "cloid": "0x...", "feeToken": "USDC", "twapId": null }] } }
```
> **Note:** Requires node to run with `--write-fills`. A raw event stream: it continues while the book is withheld. Replayed fills after a node restart are sent as written: deduplicate by the fill identity `(coin, time, tid)` and gate freshness on the consumer side. A connection holding `userFills` is closed when it lags the internal broadcast (overwritten fills cannot be re-sent).

### Account view: extCoinPositions
Our own feeds are named with an `ext` prefix, apart from the official API's subscription types.

With `--accounts`, the server keeps every perp position of every account (all dexes) from the node itself, without the official API: each periodic checkpoint (`periodic_abci_states`, ~every 12 minutes) is an **anchor**, and the fills stream moves sizes forward in between. Liquidation prices, margin and account modes are the anchor's (computed from the node's own cash, tiers and mark at that checkpoint) and are not re-derived between anchors. Between anchors the view marks the accounts whose state may have moved: from the fills, the misc events (ledger, funding) and the node's block log `replica_cmds/` (successful margin, leverage, account-mode, withdrawal and request-quota purchase actions).
```
GET /extCoinPositions?coin=PUMP
```
```json
{"coin":"PUMP","fillsHeight":1189401234,"miscEventsHeight":1189401230,"blockActionsHeight":1189401236,
 "anchor":{"height":1189400000,"time":1792404043180},
 "positions":[{"user":"0x…","szi":"269620.0","positionHeight":1189400000,"marginMode":"cross","accountMode":"unifiedAccount",
   "poolPositions":1,"liquidationPx":"0.0039515","liquidationPxStatus":"computed","unsupportedReason":null,
   "liquidationPxSource":"replay","liquidationPxHeight":1189401236,"markPx":"0.004477","liquidationDistanceBps":1173.78,
   "changedSinceLiquidationPx":[],
   "replayStoppedReason":null,"replayStoppedHeight":null,"portfolioMargin":null}]}
```
- `szi` is the anchor plus every fill observed since; `positionHeight` is the block that set it.
- `liquidationPxStatus`: `computed` (a positive price), `none` (no positive solution), `unsupported` (`unsupportedReason`: `unknownAccountMode:<code>`, `missingMark`, `tierCase`, `portfolioMarginNonUsdcDex`, `portfolioMarginSupplyCap`, `portfolioMarginBorrowCap`), `pending` (neither a replayed row nor an anchor row: a position opened after the anchor that does not replay). `liquidationPx` is set only for `computed`. Which state the price is of is `liquidationPxSource` and `liquidationPxHeight` (below): a `computed` anchor row is the anchor's state, not a price that still holds.
- Maintenance follows the node's integer rule: a position held at its mark counts `floor(notional / (2 × max leverage)) − deduction` in micro-USDC (notional = raw size × stored mark, exact), and a liquidation price moves only its own position continuously while the pool's other positions keep that integer value. The node's `crossMaintenanceMarginUsed` equalled this in 271 of 271 single-tier pools (2026-10-19); it moves liquidation prices of pools with many small positions by up to a few 1e-4.
- Portfolio-margin accounts (`accountMode: "portfolioMargin"`): a cross position's `liquidationPx` means what the node's `clearinghouseState.liquidationPx` means — the mark at which the USDC pool's availability after maintenance reaches 0 with every other price frozen, collateral oracles included (ratio about 1). It is **not** the official page's "ratio above 0.95" trigger, which the view does not compute. The pool counts spot USDC, borrow-lend supply minus borrow, the cross cash of every USDC dex and the borrow headroom against the supplied collateral (from 1 USDC). Isolated positions use their own margin as in every mode.
- `portfolioMargin`: the anchor's account ratio, the node's `portfolioMarginRatio`: `{"status":"computed","ratio":"0.1823"}`, or `{"status":"unsupported","reason":…}` (state on a dex not settled in USDC, collateral above the per-account supply cap, a needed borrow cap missing); `null` for the other modes and for `pending` rows. A ratio of `"0"` (nothing borrowed, no cross maintenance) is a computed value.
- `poolPositions` > 1: the price also moves with the other positions of that margin pool.
- `liquidationPxSource` (designs 000437, 000442): `replay` for a position on a USDC perp of an account that held a position at the anchor and still replays: the price is computed from the replayed cash and the sizes of block `liquidationPxHeight` (the applied height, once it reached the anchor), with each position's own margin table (the anchor's `M`; the coin's current table for a position opened from zero after it; a flip keeps it). An isolated position is its own pool; a cross position's pool (default / disabled: the same USDC dex; unified: every USDC dex) holds its other cross positions at the node marks of that same block, each at its integer maintenance, as the anchor holds them at the stored marks. A pool member without a node mark (the marks unavailable or at another height, a main-dex coin the marks do not replay, a coin listed since the anchor) or with an unknown table leaves the row to the anchor. `anchor` otherwise: the anchor's row, at the anchor height. `null` while `pending`. A replayed row has `poolPositions` equal to its pool's positions and an empty `changedSinceLiquidationPx`: every event up to its height is in it.
- `markPx` / `liquidationDistanceBps` (design 000442): the coin's mark at `liquidationPxHeight` — the node mark for a replayed row, the stored mark for an anchor's row — and how far it is from `liquidationPx` in bp of the mark, with the size at that height: positive while the mark has not reached the price (a long's mark above it, a short's below). `null` without a mark or without a `computed` price. Both belong to the row's height: an anchor's row keeps the anchor's mark and distance even when the size has moved since.
- `replayStoppedReason` / `replayStoppedHeight`: why and at which block the account's replay stopped (an isolated position's fill, a backstop liquidation, an account mode switch, …; see the cash replay below): its rows then show the anchor's values and marks until the next anchor. `replayStoppedHeight` is `null` for an account stopped at the anchor (`portfolioMargin`, an unknown mode).
- `fillsHeight` / `miscEventsHeight` / `blockActionsHeight`: the highest block each stream has been read to.
- `changedSinceLiquidationPx`: the observed sources that touched the account after the anchor, each with the **first** block after the anchor (from there the anchor's liquidation price may be stale): `fill`, `ledger`, `funding`, and the successful block actions `margin` (`updateIsolatedMargin`, `updateLeverage`: isolated margin moved, leverage or cross/isolated changed), `accountMode` (`userSetAbstraction`, `agentSetAbstraction`) `withdrawal` (`withdraw3`, debited when requested; its ledger event comes minutes later), and `requestWeight` (`reserveRequestWeight`, paid request quota). A block-action mark is an action that may invalidate the anchor, not proof that the state changed (setting the mode an account already has marks it too). The account is the action's response `user`, the vault or sub-account when the action names one. On an anchor row, once a source has a mark, later events of that source no longer change the row by themselves (an account trading every block is not pushed every block); an earlier block arriving late still moves the mark back, and position changes in the coin are pushed as always. An account that replays (design 000437) has every event touch its rows, since they may move a replayed row. **Empty does not prove the liquidation price unchanged**: marks follow the account's own events, not mark-price moves of the other positions in its pool, nor the borrow-lend interest of portfolio-margin accounts. Between two checkpoints of 2026-10-19 every other margin, mode or cash change of a holder came with a fill, ledger event or block action, or was a transfer inside its own pool (unified and portfolio-margin accounts move cash between their dexes, spot and borrow-lend).
- Errors: `404` for a coin outside the anchor's universe, `503` with the reason until the first anchor and whenever data was lost that no anchor covers yet (the reason names the stream and the bound). Every block has a `replica_cmds` line, so a skipped block height is a loss of the `block_actions` stream up to the last missing block.
- Send `Accept-Encoding: gzip` (curl: `--compressed`): PUMP is ~2 MB of JSON.
- The block log costs about 0.7% of one core to follow (bm, 2026-10-19; it parses at 2.3 GB/s, the node writes 6–10 MB/s). At a query the newest block read is a median 205 ms (p99 385 ms) past its block time; that includes the node's own write delay, since `replica_cmds` lines carry no write time.
- Blocks are applied whole and in height order (design 000433): a block's fills, misc events and actions take effect together once the block is complete, that is its `replica_cmds` line and the next block's were read, and the fills, misc events, HIP-3 oracle updates, book diffs and order statuses readers each proved a read-through (every line written before an instant has been read: no newer file, a drained file, no partial line) later than the next block's line was read. The stream heights in the view are those of applied blocks. This rests on the node's measured write order (a block's last fills, misc, HIP-3 oracle update, book diff and order status lines arrive before the next block's `replica_cmds` line; bm 2026-10-20, no exception), not on a protocol guarantee: a line for a block already applied — a node replay of written blocks, or a broken order — is a loss up to that block, so a node restart may withhold the view until a covering checkpoint. Inputs repeated before their block completes count once (fills by account, coin, trade and side; misc lines by their `events` text). Metrics: `orderbook_accounts_applied_height` (complete blocks only), `orderbook_accounts_pending_blocks`, `orderbook_accounts_duplicate_inputs_total{stream}`; the losses `late_or_dropped` and `pending_overflow` count in `orderbook_accounts_data_loss_total`.
- Cash replay between anchors (design 000433, second batch; served as the replayed rows above): the margin state of every account that held a position at the anchor (default / disabled: cross cash per USDC dex; unified: the USDC pool of spot plus every USDC dex; isolated cash per position) moves block by block with its fills, funding, ledger updates and block actions, by the measured rules of the research `hl_account_state_verify_20261019`. An event no measured rule covers (an isolated position's fill, a backstop liquidation, an account mode switch, `borrowLend` on a unified account, a ledger type not read, another collateral, …) stops that account until the next anchor. An anchor parsed before its blocks are applied waits for them, then checks the replay of the interval it closes, account by account to the last 10^-8 USDC: `orderbook_accounts_replay_checked_total{mode,result}`, `orderbook_accounts_replay_stopped_total{reason}`, `orderbook_accounts_replay_checks_skipped_total{reason}`, and up to 50 `replayMismatch` evidence lines.
- Node marks (design 000439, batches 1, 2a and 2b; read by the replayed rows of shared pools and every row's `markPx`, design 000442): the owner of the view also keeps, block by block and applied before the view's block, the node's HIP-3 marks from its oracle update stream (`hip3_oracle_updates_streaming`, node flag `--write-hip3-oracle-updates`: one deployer's update per line, `oracle_pxs.coin_to_mark_px` the mark the node applies, already limited) and the main dex's books order by order from its raw book diffs (`node_raw_book_diffs_streaming`: one diff per line; the watcher keeps every market's diffs, ~12,000 a second on 2026-10-20, since the execution points below anchor on all of them; the books follow only the main-dex perps' diffs, ~7,400 a second on 2026-10-19, and the spot books named below). Each checkpoint seeds them; it also carries the main dex's oracle state, parsed in the same pass and checked apart (a seed breaking its contract withholds only the marks), for the per-action replay of the main dex's prices: block by block, each `SetGlobalAction` and validator reference action of `replica_cmds` runs at its place among the block's diff lines (placed by the orders the block's other actions created or cancelled, the makers their takers hit, the orders a self-trade check removed — a no-fill IOC matched one-to-one to its full request and contiguous cancellation statuses (including older orders and cross-vault cancels), or an order created in the block, inside the one later order of its account that could have taken it (that coin, the other side, a limit reaching it; an IOC rejected for no match included), when no order the block triggered could have taken it too —, and the main-dex orders a lone `SetGlobalAction` triggered (HIP-3 triggers belong to their own price actions); cancels by cloid resolve through `node_order_statuses_streaming`, whose watcher preserves every status as a cancellation-run boundary; only the needed facts survive in the replay log; unmatched request/item-response counts identify no rejected IOC and preserve any placed oids; a multi-order batch with one error-only response is counted, without evidence; other shapes are counted and sampled as `nodeMarkIocPairing`, at most 20 actions per installed seed interval (also 20 before the first seed), with block height, bundle hash, rank, counts, and only that action/response; each wire excerpt is capped at 4 KiB, with original byte length and an explicit truncation flag; no whole replica block enters the log, pending blocks or replay log; quotas renew only when a seed actually rebuilds the marks, not on losses or failed/unreachable seeds; `orderbook_node_marks_ioc_pairings_total{kind="batchRejected"|"unknown"|"suppressed"}` counts observations at ingress (including rereads), while `orderbook_accounts_evidence_total{kind="nodeMarkIocPairing"}` counts logged samples; the existing `batchModify` and cancel pairing contracts remain strict) and moves the coin's stored oracle, mark, basis and local-price EMAs and reference caches by the node's rules. A reference action pricing a coin with one of the two prices takes the local-price EMA, previewed with the book's local price, as the third Mark candidate. HYPE and PURR take their Spot reference from the spot books the checkpoint's `oracle.psro` names (`@107` HYPE/USDC, `PURR/USDC`), seeded from the checkpoint and moved by their diffs: at every validator reference action, the book's floor midpoint at the action's place, rounded half to even into the perp's price units, whatever the validator sent (Codex `hype_purr_internal_reference_20261020`: 12,819 reference actions and 700 `SetGlobalAction`s replayed, both coins' prices, EMAs and caches equal to three checkpoints). A mark more than 1% from the previous one moves by 1% of it, rounded as the archived node's branch rounds it (observed once, for a HIP-3 coin); the checkpoint checks the end state only (a later update overwrites a limited mark), and the check's log lists the coins it moved. A coin whose inputs take a path never observed (an empty book side, a local-price EMA gap, an EMA not yet warm for the third candidate) is not replayed until the next seed. A price action whose place among the lines is not unique reads the book at the first line not known to run before it; such actions are counted in `orderbook_node_marks_ambiguous_cuts_total`, the first 20 per seed interval as `nodeMarkAmbiguousCut` evidence. A diff that does not fit the books (an unknown order, a size it did not hold) is a loss of that stream. At each grid height the marks and books are kept; that height's checkpoint, once its blocks are applied, is compared with them and rebuilds them from itself plus the blocks logged after it. A loss on any stream the marks read (fills, block actions, HIP-3 oracle updates, book diffs, order statuses), a seed breaking its contract or an overflowing log withholds the marks until a later checkpoint rebuilds them from a complete log. A seed that could not rebuild them (its blocks are no longer all logged) leaves them as they were, failure included; an exited watcher or poller stops them until restart, as it stops the view. Metrics: `orderbook_node_marks_available`, `orderbook_node_marks_height`, `orderbook_node_marks_seed_height`, `orderbook_node_marks_logged_inputs`, `orderbook_node_marks_checked_total{market,result}` (`hip3`; `nativeBooks`; `nativeOrders`: equal, differ, missing, extra; each main-dex price field `storedOracle`, `mark`, `msoe`, `lmpe`, `spotCache`, `externalPerpCache`, `markCache`: equal, differ; `nativeUnsupported`: by reason), `orderbook_node_marks_checks_skipped_total{reason}`, `orderbook_node_marks_rebuilds_total{result}`, `orderbook_node_marks_seed_failures_total`, `orderbook_node_marks_unknown_coin_inputs_total{stream}`; stream losses in `orderbook_accounts_data_loss_total{stream="hip3_oracle_updates"|"book_diffs"|"order_statuses"}`, mismatches as `nodeMarkMismatch` / `nodeBookMismatch` / `nodePriceMismatch` evidence, each with the replayed and the checkpoint's value (a price field's as text: numbers and EMAs as written, a cache as its median, bucket and members by validator). The view needs two checkpoints on disk to start: the marks take the node's grid from the newest two.
- `vaultDistribution` ledger rows are separate cash legs: the vault's own row debits its aggregate distribution; each recipient row credits only that recipient's allocation through its main/unified pool. H1191750497 distributed 195999.419995 USDC in 14 rows, including zero allocations and a 0.000001 USDC allocation. All 14 main/unified-spot cash projections match the next checkpoint after the interval's withdrawal and cross-funding payments (`vault_distribution_golden.json`). The initiating `vaultDistribute` action is not charged again.
- Successful `reserveRequestWeight` debits the receipt user's main perp cash (or unified pool) by 0.0005 USDC per request, as specified in the [exchange API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint#reserve-additional-actions). The optional `destination` receives quota, not a cash debit. H1191758386 and `request_weight_golden.json` confirm 100,000 requests cost the payer 50 USDC; checkpoint balances for both parties match, with no separate ledger debit. Failed actions cost nothing in this replay; invalid successful weight is a contract error.
- `internalTransfer.usdc` is the **gross sender debit**; the recipient receives `usdc - fee`. H1191755982 sent 50 USDC with a 1 USDC fee: sender cash went from its 50 USDC deposit to zero, and the unified recipient received 49. `internal_transfer_golden.json` binds both checkpoint balances and the other receiver ledger events in H1191750000→1191760000. This rule is specific to `internalTransfer`; `send` and `spotTransfer` retain their own semantics.
- Accepted `withdraw3.amount` is converted to **micro USDC with decimal arithmetic, midpoint ties to even**, then represented in the cash replay's 10^-8 units. Archived client `6911b792` path `0x2d95420 -> 0x41b1240 / 0x44dbd10 -> 0x40bea60` and `withdrawal_goldens.json` bind the conversion. H1191730512's `62.14531414` request debits `62.145314`, retaining 14 units of spot/unified dust; the later withdrawal ledger never debits again. Ledger/fill amount normalization is unchanged. The prior adapter's rejection of non-decimal request spellings (the client's separate float fallback) remains explicit.
- Funding cash balancing follows archived client `6911b792` (`0x4264330` / `0x3c8f2d0`): each complete dex batch contributes its signed net payment; the largest single position at the preceding block's **stored oracle** receives the opposite cash adjustment. Equal notional selects the lower address, then the first wire position of that user. All wire deltas participate, including zero amounts and users outside the view. Settlement `szi` uses the existing size normalizer. Funding and its derived adjustment apply before this block's account actions/fills and price actions; the event log replays that decision once after a seed install. Wire funding records/cumulative funding are unchanged. Missing settlement prices withdraw only potential recipients' cash replay (`fundingBalanceUnknown`), with one contextual warning per affected batch. HIP-3 stored oracle now shares the mark owner's seed, stream and recovery lifecycle and checkpoint checks (`hip3StoredOracle` metric/evidence). `funding_goldens.json` records ten observed residuals and external archive hashes; the opt-in full-batch test verifies 849,268 records from two hours using preceding checkpoint oracles (recipient stability, not full historical price reconstruction).
- Account cash replay unwraps successful `multiSig.payload.action` only at its own adapter. It uses the outer receipt's effective user and status; ledger-owned transfers remain ledger-owned. Golden block 1191644834 restores a missing 67.297170 USDC cross-to-isolated move. Shared mark/action ordering is unchanged; failed wrappers are skipped, malformed successful kept actions remain explicit contract errors.
- CoreWriter book effects join the node-mark replay through `system_and_core_writer_actions_streaming` (node flag `--write-system-and-core-writer-actions`). This sparse stream must prove read-through with the other readers before a block completes; duplicates and losses use the existing block/seed lifecycle (`system_actions` metric label). Its rows are attempts, so only unique matches to full order-status fields or successful cancel statuses anchor effects. Ordinary IOC/GTC orders use the archived client's pre-bundle delayed-action phase; all-ALO orders use its EVM-tail phase. Cancel timing follows the serialized `f` flag. The checkpoint supplies `pt.prp.D` and `action_delayer.delay_scale`; absent, zero or invalid timing cannot certify a delayed action. Unknown grouping/flags with a matched effect are explicit mark-input failures; ordinary rejected attempts have no fabricated successful effect. Competing native/system requests retain ambiguity. These are version-specific client rules (6911b792), not an official API guarantee; priority order groupings, live delay-configuration transitions and other system action kinds are not reconstructed by this addition. The ETH golden at height 1191429459 preserves the CoreWriter attempt, order statuses, both fill legs and diffs: the ETH trade precedes SGA while the same block's ZEC trigger follows it. Large raw archives stay outside the repository.
- Rare events are logged as first-hand evidence: one `ACCOUNTS_EVIDENCE <json>` WARN line each, counted in `orderbook_accounts_evidence_total{kind}`. `rareLine` keeps the raw line of a liquidation fill whose method is not `market`, a fill direction or ledger type not seen before, and an unknown misc event; `silentPositionChange` (at most 100 per install) records an anchor that disagrees with the size the fills had led to.

Subscription:
```json
{ "method": "subscribe", "subscription": { "type": "extCoinPositions", "coin": "PUMP" } }
```
Frames on `{"channel":"extCoinPositions","data":{…}}` with a per-coin `version`:
- `{"kind":"snapshot","version",…}`: the HTTP shape plus `version`; once on subscribe, again after every anchor install, and whenever the node marks become unavailable or available again (a replayed row of a shared pool falls back to the anchor's, or replays again; the account view itself stays available).
- `{"kind":"changes","coin","version","fillsHeight","miscEventsHeight","blockActionsHeight","changes":[{"user","position"}]}`: the rows one fills, misc or block-actions batch touched, valued at the push; `position` is `null` once closed. Hourly funding touches every holder. Mark moves push nothing: a row not pushed keeps the `liquidationPxHeight`, `markPx` and distance it was pushed with, so the rows of one frame or version need not share a valuation height (HTTP and snapshots value every row at the current height). Watching distances as marks move is the job of a separate feed, not of `extCoinPositions`.
- `{"kind":"withdrawn","coin","reason"}`: the subscription ended (the view became unavailable, the connection fell behind, or a frame was missed); resubscribe for a new snapshot. While the view is unavailable new subscriptions are refused.

Every frame's `version` is the previous one plus one; a subscriber applies frames above its snapshot's version in order. Validated against the node's own `clearinghouseState` on 2026-10-19 (aligned single-position pools within 0.1 bp; sizes equal to the next checkpoint for all ~400k positions); only USDC-collateral pools were checked against live positions.

### Liquidation candidates: extLiquidationCandidates
With `--accounts`, the positions whose node mark is within a distance of their liquidation price (design 000443), with the market liquidation order each would take next:
```json
{ "method": "subscribe", "subscription": { "type": "extLiquidationCandidates", "coin": "BTC", "maxDistanceBps": 200 } }
```
`coin` may be `null` (every coin); `maxDistanceBps` is at most 1000. Frames on `{"channel":"extLiquidationCandidates","data":{"coin","maxDistanceBps",…}}`, each naming its subscription, with the subscription's own `version`:
- `{"kind":"snapshot","version":0,"height","time","candidates":[…]}` on subscribe;
- `{"kind":"changes","version","height","time","enter":[…],"update":[…],"exit":[{"user","coin"}]}`: one frame per refresh with changes, every coin at once;
- `{"kind":"withdrawn","reason"}`: the subscription ended (the account view or the node marks unavailable, a missed frame, a slow connection); subscribe again for a new snapshot.

A candidate row:
```json
{"user":"0x…","coin":"BTC","szi":"-1.5","markPx":"83022.0","liquidationPx":"84213.4","liquidationDistanceBps":143.5,"liquidationPxHeight":1190866310,
 "marginMode":"cross","poolPositions":3,"crossPoolCoins":["ETH","SOL"],
 "expectedOrder":{"side":"B","cooldown":"none","firstOrderSize":"0.3","cooldownOrderSize":"1.5","walkSize":"0.3","notional":"124533.0"},
 "bookWalk":{"status":"walked","midPx":"83022.5","aheadSize":"1.2",
   "together":{"vwapPx":"83031.6","deepestPx":"83034.0","deepestBpsFromMid":1.39,"filledSize":"0.3","levels":4,"end":"filled"},
   "alone":{"vwapPx":"83023.8","deepestPx":"83025.0","deepestBpsFromMid":0.30,"filledSize":"0.3","levels":2,"end":"filled"},
   "assumption":"known","bookHeight":1190866309,"bookTime":1792512887052}}
```
- Only replayed rows (see `liquidationPxSource` above) of the applied height are candidates: their size, pool cash, pool members and node marks all belong to block `liquidationPxHeight`. An anchor's row is never one. The distance is `liquidationDistanceBps` of `extCoinPositions`, against the node mark of that block.
- The server keeps every position within 1000 bp, re-read each block for the accounts the block's events touched and for the accounts sharing a pool with a coin whose mark moved; each subscription projects it onto its coin and distance, so a candidate enters or exits a subscription as its distance crosses the subscription's threshold.
- `update` goes out when the distance moves by a bp (to the integer) or the price, size, pool or expected order changes; the mark and the notional alone push nothing, so a row not updated keeps the `markPx`, `notional` and `liquidationPxHeight` it was sent with.
- `expectedOrder` is the next **market** liquidation (research `hl_liquidation_rules_20261020`, 21,846 node liquidations): `side` `A` sells a long, `B` buys back a short; up to 100,000 USD of notional (`notional`, |size| × mark) the whole position, above it a fifth (`firstOrderSize`, raw size floored, 236 of 236 partials). After a partial market liquidation of any of the account's positions, its market liquidations take whole positions for 30 s (`cooldownOrderSize`; official "Liquidations", 122 of 122). `cooldown`: `inCooldown` (a partial one within the last 30 s of block time), `none` (none within them, every block of them applied without a fills loss), `unknown` (anything else: the view started or lost fills less than 30 s ago — a checkpoint does not restore that history — or a liquidation it could not size). `walkSize` is what an order takes under the known rule, the whole position while `unknown`. Backstop liquidations (off the book) and auto-deleveraging are outside the estimate; when the remainder is liquidated depends on later prices.
- `crossPoolCoins`: the other coins of the cross pool, which may be liquidated in the same block; not a guarantee. Candidates of one coin and side order by how soon the mark reaches them only in the static view where that coin moves alone.
- `bookWalk`: where the order would fill on the public book of this server, static (no cascade, no refill). The candidates of one coin and side walk together in that order — longs from the highest liquidation price, shorts from the lowest, equal prices by address — each taking its `walkSize` from where the ones before it (`aheadSize`) stopped (`together`); `alone` is the same size walked from the best level, as if no other candidate came first. Each gives `vwapPx`, `deepestPx`, `deepestBpsFromMid` (how far the deepest level is from `midPx`, the book's mid at the read; `null` with an empty side), `filledSize`, `levels`; `end` is `filled`, `bookExhausted`, or `truncatedAtLevels` (the walk reads at most 2,000 levels, the book may hold more). `assumption` is `unknownCooldownAsFull` when this candidate or one before it walks a whole position for an unknown cooldown. `bookHeight` / `bookTime` are the book's own progress at the read, not the candidate's height; the walk is redone when the candidates of that coin and side, their order or their sizes change, not on every book move. `status` is `pending` until a walk of the current list is in: a new candidate, and every candidate from the first changed place of the list on (an estimate holds only for the list it walked). `untrusted` while the book is withheld: every estimate goes to it at once, and an estimate read before cannot come back — snapshots and frames publish each walk as of the book's state when they go out, so one from an earlier withheld period shows `pending` until walked anew. `noBook` in a process without the order book.
- Metrics: `orderbook_liquidation_candidates` (published candidates), `orderbook_liquidation_candidates_refresh_seconds` (the owner's work per refresh), `orderbook_liquidation_walk_lock_wait_seconds` / `orderbook_liquidation_walk_lock_hold_seconds` (a walk waiting for the listener lock, and holding it to copy the levels); frames in `orderbook_accounts_frames_total{kind="candidates"|"candidatesWithdrawn"}`.

### Forced flow: extForcedFlow

With `--accounts`, per coin, the market flow a mark move would force out (design 000451): the trigger orders (stops, take profits, trailing stops; the listener's untriggered orders, which fire on the mark) and the liquidations, as facts. Each entry is sized alone by the node's rules against the account's current position — a position TP/SL takes the whole position, a reduce-only order at most what it reduces (`noPositionToReduce` with nothing to reduce), any other order its size, a liquidation its next market order (`expectedOrder`, as `extLiquidationCandidates`) — and walked alone on the public book up to its own limit price (a market trigger order carries its 10% slippage limit; a liquidation takes any price). No scenario: an entry ignores every other entry, so two orders of one account may each cover the same position, and nothing chains (that is the trading system's call).
```json
{ "method": "subscribe", "subscription": { "type": "extForcedFlow", "coin": "BTC", "maxDistanceBps": 100,
  "sources": ["trigger", "liquidation"], "excludeCoins": [], "minNotionalUsd": 10000 } }
```
- `coin` may be `null` (every coin with a node mark); `maxDistanceBps` at most 1000. `sources` (both), `excludeCoins` (none) and `minNotionalUsd` (10,000 USD: the entry's size × its level) may be left out. A coin both subscribed and excluded is refused, and so is an unknown parameter (a misspelt filter never runs as its default).
- Distances in two calibres: `distanceNodeBps` from the node mark (the account view's replay, main dex and HIP-3) and `distanceLocalBps` from the local predicted mark (`extMarkPrices`, main-dex coins of the local marks' sources; `null` with the local leg's `reasonCode` otherwise, `localMarksOff` without `--local-marks`). Positive while the mark has not reached the level. The node one is negative for an order the node mark is past already - placed past its level, it fires at the next price action (bm 2026-10-21: a take profit 94 bp past fired 18 blocks after its placement) -, the local one when the local estimate is past it (the next mark update may fire it — a conditional estimate, not a promise). An entry is in when either calibre is within the distance, however far past its level: the table's band read gives a fall's orders from the range's low end up and a rise's up to its high end (`listeners/order_book/triggers.rs`, by direction: `Price below` and an activated trailing sale fall, `Price above` and an activated trailing buy rise).
- Full frames, by tier: a subscription's distance falls in a tier — up to 10 bp, 100 bp, 1000 bp — computed for every coin with a subscriber (or a query) at most every 100 ms, 1 s and 5 s (a start cadence: a computation under way, the reads' queue and the listener lock come on top). Each frame is the whole content of a coin at its tier (`seq` numbers the coin's contents; replace, never merge); a frame goes out when the content changed — the walked books' heights and times alone are no change. A local leg turning withheld or ending is withdrawn at once - a frame without the local calibre: no local distances, the entries only the local mark took in gone - even when the owner sees a closed channel before its notice; turning withheld, coming back or ending also recomputes at once, whatever the computation under way (a read started with the other validity is voided); a book withheld while the walks were out leaves them unwalked and the trigger part `incomplete` (`bookWithheld`). `{"channel":"extForcedFlow","data":{"coin","tierBps","maxDistanceBps","seq","heights",…}}`, then `status` `unavailable` with its `reason`, or `ready` with `nodeMarkPx`, `local` (`ready` with `markPx`, or `withheld`), `triggers` (`complete`, or `incomplete` with `trackingDisabled` / `snapshotWithoutTriggers` / `snapshotParseFailed` / `liveParseFailed` / `childUnmatched` / `truncated` / `bookWithheld`), `walks` (`walked` with each side's book, `untrusted`, `noBook`), `entries`, `bands` and `unplaced` (the coin's trigger orders not placed: trailing stops waiting for activation — the table hands them with every band —, conditions or order types of another form).
- An entry: `source` (`trigger` / `liquidation`), `direction` (`down` / `up`: the mark move that reaches it), `side` (`A` sells into the bids), `user`, `levelPx` (trigger price or liquidation price), the two distances, `size` and `sizeReason`, `notionalUsd`, `positionSzi`, `limitPx`, `trigger` (`oid`, `orderType`, `triggerCondition`, `triggerPx`, `reduceOnly`, `isPositionTpsl`, `trailing`: an activated trailing stop at its trigger price as it stands, `orderSz`) or `expectedOrder`, and `walk` (`vwapPx`, `deepestPx`, `deepestBpsFromMid`, `filledSize`, `levels`, `end`: `filled`, `limitReached`, `bookExhausted`, `truncatedAtLevels`; `null` unless walked).
- `bands`: `node` and `local`, per (direction, side), cumulative to each edge of 1/5/10/25/50/100/200/500/1000 bp up to the subscription's distance: count, size and notional of the trigger orders, the liquidations, the market and the limit trigger orders, the reduce-only and the other trigger orders (two views of one set: not to be added). The bands take the subscription's `sources`, never its `minNotionalUsd`.
- Completeness: the trigger part is complete only with the listener's whole table, an untruncated band (50,000 orders) and a trusted book; the liquidations do not depend on it.
```
GET /extForcedFlow?maxDistanceBps=100[&coin=BTC][&sources=trigger,liquidation][&excludeCoins=ETH,SOL][&minNotionalUsd=10000]
```
- Each query, and each new subscription, is answered with each coin's frame from a computation started no earlier than it (a frame's age does not prove its inputs unchanged): a computation under way is not voided - it answers the queries that came before it started, then one more runs for the later ones - and answers within 10 s. 400 for parameters a subscription would refuse (unknown ones included), 503 while the forced flow is unavailable.
- Frames in `orderbook_accounts_frames_total{kind="forcedFlow"|"forcedFlowWithdrawn"}`. The reads under the listener lock in `orderbook_forced_flow_lock_wait_seconds{read}` and `orderbook_forced_flow_lock_hold_seconds{read}` (`band`: a coin's trigger orders in a band; `walk`: one side's levels), the entries walked in `orderbook_forced_flow_walked_entries_total`, and the work on the account owner task per step in `orderbook_forced_flow_owner_seconds{step}` (`query`, `read`, `tick`, `local`).

### Local marks (extMarkPrices, extMarkInputs)

With `--local-marks` (and `--accounts`) the server follows the venues whose prices make Hyperliquid's mark: per main-dex coin its spot markets (the oracle) and perp markets (the external price), as listed in `server/data/mark_sources.json`. That file is generated by `scripts/discover_mark_sources.py` from the venues' public listings, each listing checked against HL's own price (oracle for spot, mark for perps) to catch another asset under the same ticker and lot multipliers (`kPEPE` is 1,000 PEPE; Binance's `1000PEPEUSDT` already is), and embedded at build time: a newly listed coin or a delisted market needs a regeneration and a rebuild. 176 coins as of 2026-10-21 (bm, 10 minutes: every coin had a quoting spot and perp venue at every 30 s sample, ~5,100 frames/s over the 12 feeds, ~3% of a core, ~26 MB); HYPE and PURR (USDC-quoted perps) and HIP-3 are not in it.

Every 50 ms the local marks' owner reads the node price state the account view's owner publishes after every applied block (the replay of the node marks above), the HL book (one listener lock per tick, copying the best prices) and every feed, and gives each coin two legs (design 000454):
- `node`: the replay's stored mark, with its stored oracle, Mark reference cache median and last trade, at the applied height.
- `local`, by the node's own rules (the replay's implementation) on that state: the oracle is the weighted median of the midpoints of the spot venues quoting now (Binance 3, OKX 2, Bybit 2, every other venue 1; one venue is enough), rounded half to even into the coin's price units; A = oracle × (1 + basis), the basis EMA previewed at now with the book's midpoint against the stored oracle, as a validator's reference action previews it; L = median(bid, ask, last trade); C = the weighted median of the perp venues quoting now. `referenceMarkPx` = median(A, L, C), the Mark member a validator submitting these venue prices now would leave. `predictedMarkPx` = median(reference, Mark cache, L) limited to 1% from the stored mark: a `SetGlobalAction` now whose validators submitted the reference. It is a conditional estimate, not the next mark: the cache refreshes per 3 s bucket, later submissions and books are unknown. Nothing is written back; the limit's base is always the node's stored mark. Small coins may differ from the validators: their venue sets are not the validators'.
- Withheld legs carry `status` (`unavailable`, or `stale` when a progress clock is behind), `reasonCode` and `reason`. For every coin at once: the node state unavailable (`node`, with the account view's or the node marks' reason) or its block more than 5 s behind the local clock (`nodeProgress`; the applied block runs 0.7 s behind at the median and 2.4–2.9 s at each minute's P99, bm 10-21: at 3 s every coin flickered stale ten times in 14 minutes) withholds both legs; the book with no snapshot yet or withheld after a data loss (`hlBook`) or its block time (both book streams applied) more than 5 s behind (`bookProgress`), the feeds stopped or the account owner gone (`stopped`, until restart) withhold the local leg. A quiet coin is not withheld for its own silence. Per coin: `nodeCoin` (the replay does not claim the coin: not replayed until the next seed, or not listed), `markCache` (no cache median yet), `hlBook` (an empty side), `oracleCoverage` / `externalCoverage` (no venue of the kind quotes it now), `calculation` (a rule's price out of range).

Subscription:
```json
{"method":"subscribe","subscription":{"type":"extMarkPrices","coin":"BTC"}}
```
A frame of the coin every 50 ms (schema 2; the trading system's Python `hl_mark_prices` service wrote 1), the latest at once on subscribing (abridged):
```json
{"channel":"extMarkPrices","data":{"schemaVersion":2,"coin":"BTC","intervalMs":50,"publishedTsMs":1792558254050,
 "node":{"status":"ready","markPx":"82810","storedOraclePx":"82796","markCachePx":"82809","lastTradePx":"82810","seedHeight":1191600000,"height":1191612345,"blockTimeMs":1792558253600},
 "local":{"status":"ready","referenceMarkPx":"82811","predictedMarkPx":"82810","basisAdjustedOraclePx":"82812","localPx":"82810","externalPx":"82811","oraclePx":"82806","basisRatio":0.000072,"basisSamples":2244588,"bidPx":"82810","askPx":"82811","nodeHeight":1191612345,"nodeBlockTimeMs":1792558253600,"bookTimeMs":1792558253700,"bookHeight":1191612346},
 "oracleCoverage":{"usedVenues":["binance","okx","bybit","kraken","kucoin","gate","mexc"],"missingVenues":[],"usedWeight":11,"configuredWeight":11},
 "externalCoverage":{"usedVenues":["binance","okx","bybit","gate","mexc"],"missingVenues":[],"usedWeight":9,"configuredWeight":9}}}
```
A withheld leg reads `{"status":"stale","reasonCode":"bookProgress","reason":"the HL book's block 1191612346 is 3120 ms behind the local clock"}`. A coin outside the sources is refused; without `--local-marks` the subscription is refused. The owner task ending (an internal error) withdraws every subscription with an error frame (`extMarkPrices BTC withdrawn: the local marks owner task ended`); the order book keeps serving.
```
GET /extMarkPrices            # every coin, a list of the frames' data
GET /extMarkPrices?coin=BTC   # one coin (404 outside the sources; 503 once the owner task ended)
```

Each venue's quotes:
```
GET /extMarkInputs            # every coin
GET /extMarkInputs?coin=BTC   # one coin (404 for a coin outside the sources)
```
Response (abridged):
```json
{"timeMs":1792558254000,"stopped":null,
 "feeds":[{"feed":"spot_binance","connection":{"state":"connected","sinceMs":1792557653950},"frames":475913,"markets":148,"quoting":148}, ...],
 "coins":[{"coin":"BTC",
   "spot":{"binance":{"symbol":"BTCUSDT","pxMultiplier":"1","bid":"82806","ask":"82806.01","receivedTsMs":1792558253883,"eventTsMs":null,"status":"valid"}, ...},
   "perp":{"okx":{"symbol":"BTC-USDT-SWAP","pxMultiplier":"1","bid":"82758.5","ask":"82758.6","receivedTsMs":1792558253800,"eventTsMs":1792558253795,"status":"valid"}, ...},
   "oracleCoverage":{"usedVenues":["binance","okx","bybit","kraken","kucoin","gate","mexc"],"missingVenues":[],"usedWeight":11,"configuredWeight":11},
   "externalCoverage":{"usedVenues":["binance","okx","bybit","gate","mexc"],"missingVenues":[],"usedWeight":9,"configuredWeight":9}}]}
```
- Prices are the venue's own, as written, with the listing's `pxMultiplier` (HL price = multiplier × venue price). Weights: Binance 3, OKX 2, Bybit 2, every other venue 1.
- `status`: `valid`; `notQuoted` (nothing on the current connection yet: a reconnect or a pong revives no earlier quote); `noQuote` (the venue shows no two-sided book); `notConnected`; `expired` (MEXC spot is polled every 250 ms; its quotes expire after 5 s). A WS quote holds while its connection lives: these venues push on change, a quiet market sends nothing (Bybit resends a quiet L1 every 3 s).
- Each connection keeps its protocol's health: its subscription acknowledgements within 10 s, its heartbeat answered within 10 s (`spot.ping` / `futures.ping` on Gate's two endpoints), some frame every 30 s. A lost connection withdraws the feed's quotes and reconnects after a backoff (0.5 s doubling to 10 s, reset after 30 s connected). A refused subscription, a known frame that does not parse or a crossed book stops that feed until restart (`connection.state: stopped` with the reason); a frame of an unknown type is warned with its text and not taken. The order book and the accounts never wait on a venue. A feed task failing otherwise (a panic: an internal error) stops every feed and withdraws every quote until restart (`stopped` gives the reason), as the account view's owner ending withdraws the view; the order book keeps serving.
- `local_marks_probe --seconds 600 --every-secs 30 --out DIR` runs the feeds alone beside a running server, without restarting it: one JSON line per interval, then `summary.json` and the whole view in `mark_inputs.json`.
- Metrics: `orderbook_local_marks_feed_frames_total{feed}`, `orderbook_local_marks_feed_disconnects_total{feed}`, `orderbook_local_marks_feed_unknown_frames_total{feed}`, `orderbook_local_marks_feed_stopped{feed}`.

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
4. **Desync self-healing** - any provable event loss (parse/apply error on a batch, oversized batch, watcher buffer discard, pending-cache eviction) marks the book out-of-sync and triggers an automatic background snapshot re-fetch. Until a snapshot covering the loss is installed the book is **withheld**, never published as fresh: no l2Book/bbo frames (heartbeats included), an l4Book subscribe answers an error, and `GET /l4Book` / `GET /untriggeredOrders` answer `503`. Downstream consumers see silence and fail over (the HL market-data gateway's primary/backup switch keys on it). Raw event streams - trades, bookDiffs, orderUpdates, l4Book updates, oracle - continue. A request or frame already past the gate when the loss is marked may still complete with its original time/height. `orderbook_book_trusted` shows the state and `orderbook_untrusted_seconds_total` accumulates the withheld time. Each loss is recorded with a block-height bound, and the out-of-sync flag only clears once a snapshot's height actually covers that bound - a snapshot generated from lagging node state cannot mask a newer loss. Each occurrence is counted in `orderbook_desyncs_total{reason}`. The status/diff pairing caches are evicted by stream progress, not by age: an order's status and New diff share one block, so a pending half is done waiting only once the *other* stream has applied a later block. A status passed that way is an expected orphan (an order that never rested - dropped silently, counted in `orderbook_pending_orphans_evicted_total`); a New diff passed that way lost its status, which counts as data loss and re-syncs the book. However far the two independently read streams drift apart (minutes during a node catch-up, see `orderbook_stream_skew_blocks`), no pairable half is dropped; a 1M-entry cap per cache remains only as a memory backstop.
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
| | `ws_subscriptions_active{type}` | Active subscriptions by type (bbo/l2Book/l4Book/trades/bookDiffs/orderUpdates/userFills) |
| | `broadcast_receivers` | Number of broadcast channel receivers |
| **Throughput** | `events_processed_total{type}` | Events by type (orders/diffs/fills) |
| | `broadcasts_total{channel}` | Broadcasts by channel (bbo/l2/l4/trades/bookDiffs/userFills) |
| | `messages_sent_total` | Total WebSocket messages sent |
| | `bbo_changes_total{coin}` | BBO changes per coin |
| **Health** | `orderbook_height` | Current block height |
| | `orderbook_time_ms` | Orderbook timestamp |
| | `orderbook_orders_total` | Total orders in the book |
| | `orderbook_untriggered_orders_total` | Untriggered trigger orders tracked (stops / TP-SL waiting for trigger price) |
| | `orderbook_untriggered_evictions_total{status}` | Untriggered-order evictions by causing status (a novel label flags a possible wrong eviction) |
| | `orderbook_untriggered_unmatched_children_total` | Children a parent's live `open` announced whose own `open` did not follow in the block |
| | `orderbook_coins_count` | Number of coins tracked |
| | `pending_orders_cache_size` | Pending order statuses in HFT cache |
| | `pending_diffs_cache_size` | Pending book diffs in HFT cache |
| | `orderbook_stream_height{stream}` | Applied block height per book stream (`statuses` / `diffs`) |
| | `orderbook_stream_skew_blocks` | Applied statuses height minus applied diffs height: how far the two book streams have drifted in this process |
| | `orderbook_book_trusted` | 1 = no known unrecovered data loss; 0 = the book is withheld until a covering re-sync |
| | `orderbook_untrusted_seconds_total` | Seconds the book was withheld for known data loss (added when each period ends) |
| | `orderbook_pending_orphans_evicted_total` | Pending statuses dropped unpaired once both streams passed their block, except orders settled in that block and statuses replayed after a node restart; expected to stay ~0 |
| | `uptime_seconds` | Server uptime in seconds |
| | `server_start_time_seconds` | Server start timestamp (unix) |
| **Latency** | `bbo_broadcast_latency_seconds` | BBO broadcast latency histogram |
| | `l2_broadcast_latency_seconds` | L2 broadcast latency histogram |
| | `l2_conflation_batch_size` | Coins rebuilt per L2 broadcast (changed since the previous one) |
| | `node_line_lag_seconds{stage,source}` | Cumulative lag from the node writing a line (`local_time`, same host clock) to `read` (watcher, newest line per read), `apply` (batch applied to the book) and `publish` (l2 built and handed to the broadcast channel); stages sample different sets, so compare side by side rather than subtracting quantiles |
| | `l2_flush_total{trigger}` | L2 publishes by trigger: `quiet` (book streams quiet 2 ms), `max_delay` (20 ms of continuous lines), `recheck` (a deferred publish found a subscriber) |
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
| **userFills** | No | Yes - per-user fills, official `userFills` shape (filter by address) |

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
