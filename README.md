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

**Sizing rule of thumb.** Memory grows roughly with (a) the universe of coins the node serves, and (b) the number of distinct L2 subscription tuples your clients open. The 256-per-connection subscription cap and the broadcast channel capacity bound the worst case. Rising `channel_drops_total` or non-zero `broadcast_channel_lag` means a receiver fell behind. Client backpressure, server scheduling and producer bursts all need distinguishing; these counters alone do not identify the cause.

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
| `--local-marks` | `false` | Compute every main-dex coin's local mark beside the node replay's (needs `--accounts`): 12 venue feeds (Binance perp partitioned across connections) - spot Binance, OKX, Bybit, Kraken, Kucoin, Gate, MEXC; perps Binance, OKX, Bybit, Gate, MEXC - for the oracle and the external price; served as `extMarkPrices` (WS and `GET`), the inputs at `GET /extMarkInputs` and the node's reference caches at `GET /extMarkReferenceCaches`. See [Local marks](#local-marks-extmarkprices-extmarkinputs-extmarkreferencecaches) |

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

BBO producer deduplication happens **before** the shared broadcast ring: coins with no live BBO
subscriber are skipped, and deep-book changes with unchanged best bid/ask price and size do not
consume ring capacity. `bbo_publication_quotes_total{outcome="not_subscribed"|"unchanged"|"published"}`
counts these decisions per coin/book event. `bbo_changes_total{coin}` instead counts enqueued
changes across connections and is not a unique exchange update count.

This does not rate-limit or conflate real quote transitions: A → B → A is delivered in order.
The existing price/size change predicate and optional heartbeat stay unchanged. New subscriptions
invalidate that coin's producer dedup state so their first subsequent book event can carry a quote;
a new book-trust period also forces republication. The last subscription drops that coin's cached
state. The BBO lane remains shared across **actively subscribed** coins; genuine ring overruns still
withdraw subscriptions explicitly rather than silently skip transitions. CPU-owner isolation and
this producer filter address different sources of backlog; live acceptance must cross checkpoints.

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
- A TP/SL placed with an order (the parent's `children`: a limit order with its take profit and stop) waits while the parent is live: the node does not trigger it before the parent leaves the book (bm 2026-10-21, hours 08-09: 48 of 49 such children triggered after their parent's `filled`, the other after its `marginCanceled`; 8,326 of 138,751 untriggered orders of a dump were such children). The table still serves a waiting child here, but keeps it out of its trigger price index, so the forced flow (`extForcedFlow`) does not list it. As Hyperliquid's TP/SL documentation puts it - a full fill of the parent activates its children, a partial fill keeps them waiting, canceling the parent cancels them, a cancel for insufficient margin after a partial fill activates them -, the parent's `filled` and `marginCanceled` release its children (and those the line lists under their own oids: a modify moves them to the parent's successor, whose `open` lists none); every other status leaves them waiting until their own statuses end them (bm hours 08-09: after a `canceled`, 226 children got their own `canceled` in the block, 78 moved to a successor, none triggered later). A modify of a waiting child keeps its successor waiting: the modify opens the new oid and cancels the old one right after it, hash-less, in the block (bm hour 11: 129,306 of 129,342 such pairs were modifies by oid or cloid, all 111 of trigger orders; a bot's `batchModify` of a limit order with its TP and SL moves all three so, and the new parent's `open` lists no children); a trigger order an account opens beside a waiting child of its coin is held one status for it - served here, out of the band, the coin's band meanwhile `incomplete` (`successorPending`, gone with that status). A dump names them in its live orders' `children`, under their own oids; live, the parent's `open` lists them under the parent's oid and their own `open` lines follow in the block (bm 10-21 hour 09: 1,745 of 1,745 so; a parent that fills in its own block - a market or IOC order, a crossing limit order - fills before they open, and they take part as they open: bm 14:35-15:07, 84 such children), matched by user, coin, timestamp, side, trigger price and condition, order type, size and reduce-only; an announced child whose own `open` did not follow is counted in `orderbook_untriggered_unmatched_children_total` and the table is a gap (`childUnmatched`, the forced flow's trigger part `incomplete`) until a complete install.
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
> **Note:** Requires node to run with `--write-fills`. A raw event stream: it continues while the book is withheld. Replayed fills after a node restart are sent as written: deduplicate by the fill identity `(coin, time, tid)` and gate freshness on the consumer side. Private fill legs use an independent 16,384-batch broadcast ring: public L4/BBO/L2/trade traffic cannot evict them. The receiver attaches before the first userFills producer subscription and detaches after the last; a later subscription starts live. A private-ring overrun still closes the connection because overwritten events cannot be recovered here; public-ring lag no longer implies lost private fills, including on mixed connections. Each lane preserves its own order; there is no cross-lane ordering guarantee. The skipped count is batches of all users’ fills, not the number of lost trades or fills for the subscribed user.

Delivery logs carry a process-local connection ID and the actual TCP peer (a local proxy remains a local peer), plus successful ordinary subscription changes (including userFills) for attribution. Private gaps report queued/skipped batch counts, elapsed time since the last receive and outbound queue occupancy. Queue waits and socket writes have separate metrics and per-connection lifetime maxima; a long receive interval or lifetime maximum is evidence to correlate, not proof of the cause of that specific gap. Raw market payloads are not included in these diagnostics.

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
- Held positions' signed notionals and integer maintenance are netted in micro-USDC before floating-point price solving, so their iteration order cannot change a zero-boundary classification. The 40-position ASTER checkpoint at 1196430000 has exactly zero net numerator; the former floating sum produced a spurious `4.54e-15` price. `accounts/aster_zero_boundary_golden.json` preserves the raw inputs; regression covers reversed/rotated holdings and genuine positive solutions one USDC wei either side of zero. This corrects our derived calculation, without changing cash replay or masking a difference with a tolerance.
- Portfolio-margin accounts (`accountMode: "portfolioMargin"`): a cross position's `liquidationPx` means what the node's `clearinghouseState.liquidationPx` means — the mark at which the USDC pool's availability after maintenance reaches 0 with every other price frozen, collateral oracles included (ratio about 1). It is **not** the official page's "ratio above 0.95" trigger, which the view does not compute. The pool counts spot USDC, borrow-lend supply minus borrow, the cross cash of every USDC dex and the borrow headroom against the supplied collateral (from 1 USDC). Isolated positions use their own margin as in every mode.
- `portfolioMargin`: the anchor's account ratio, the node's `portfolioMarginRatio`: `{"status":"computed","ratio":"0.1823"}`, or `{"status":"unsupported","reason":…}` (state on a dex not settled in USDC, collateral above the per-account supply cap, a needed borrow cap missing); `null` for the other modes and for `pending` rows. A ratio of `"0"` (nothing borrowed, no cross maintenance) is a computed value.
- `poolPositions` > 1: the price also moves with the other positions of that margin pool.
- `liquidationPxSource` (designs 000437, 000442): `replay` for a position on a USDC perp of an account that held a position at the anchor and still replays: the price is computed from the replayed cash and the sizes of block `liquidationPxHeight` (the applied height, once it reached the anchor), with each position's own margin table (the anchor's `M`; the coin's current table for a position opened from zero after it; a flip keeps it). An isolated position is its own pool; a cross position's pool (default / disabled: the same USDC dex; unified: every USDC dex) holds its other cross positions at the node marks of that same block, each at its integer maintenance, as the anchor holds them at the stored marks. A pool member without a node mark (the marks unavailable or at another height, a main-dex coin the marks do not replay, a coin listed since the anchor) or with an unknown table leaves the row to the anchor. `anchor` otherwise: the anchor's row, at the anchor height. `null` while `pending`. A replayed row has `poolPositions` equal to its pool's positions and an empty `changedSinceLiquidationPx`: every event up to its height is in it.
- `cashEstimated`: an isolated open, increase or flip since the anchor had no trustworthy execution mark, its possible marks implied different micro-USDC transfers, the cross funding limit needs unavailable execution-time valuations of other cross positions, or a manual isolated-margin transfer and isolated fill share a block without execution-order evidence. Those events retain the fill-notional/leverage estimate: isolated cash may be short and the same pool's cross cash long by the same amount, so either side's liquidation distance can be wrong. Exact later fills do not clear that history; the next checkpoint does. `false` on an anchor's or pending row. Sufficient mark evidence plus a known funding limit determine an exact transfer. A binding cash limit can also resolve a missing or ambiguous mark. Cross exposure is conservatively remembered until the next anchor; a cross close alone does not restore the cash-only proof.
- HIP-3 isolated-fill marks now distinguish ordinary receipt-backed takers before/after one successful `perpDeploy.setOracle` (including a successful `multiSig` wrapper). The oracle stream must contain exactly one `Normal` write for that coin at this block's time. Both trade legs use the taker's rank; TWAP slices precede actions. Unplaced takers, multiple actions/writes and `Fallback` updates retain the original whole-block range. This changes cash input precision, not native mark reconstruction or written HIP-3 prices. `marks/fill_prices/hip3_cash_goldens.json` preserves checkpoint endpoints and real wire fills; large archives remain outside Git. Cross-pool capacity and same-block manual margin timing retain their own estimation boundaries.
- A successful single `modify` with a `default` receipt can also locate a HIP-3 taker, using the existing full-field, unique adjacent `open(new)` / `canceled(old)` matcher. Exchange asset IDs come from the checkpoint universe. Matching CoreWriter attempts prevent this attribution even without a native price action or known system phase. BM ORCL 1193548256 places taker 582992141819 before setOracle: both fill legs use 124.52 instead of the ambiguous 124.52–124.57 range (`marks/fill_prices/hip3/modify_golden.json`). Missing, competing, interrupted or mismatched evidence retains the range. This resolves the execution mark input; independent cross capacity and manual-margin ordering remain separate cash-estimation boundaries.
- HIP-3 fixed-trigger fills also use the new mark when the block has one uniquely attributed Normal write that strictly crosses the recorded trigger threshold (old mark does not satisfy it, new mark does). The triggered taker identifies both trade legs. Duplicate/missing statuses or takers, already-satisfied thresholds, unsupported trailing forms or unknown conditions, Fallback and multiple writes retain their full bounds; manual-margin action ordering still treats triggers as unplaced. BM NET1193124226 and SPCX1193126889 explain 0.178080 / 0.236727 USDC missing isolated cash respectively. Independent full-window replay of the two accounts (199 / 86 adds, no competing cash actions, ample observed cross cash) matches both isolated and unified cash at1193130000 exactly. Compact wire goldens are in `marks/fill_prices/hip3_trigger_cash_goldens.json`; this is offline evidence, not deployment acceptance.
- Manual isolated-margin cash transfers now retain their successful action's `(bundle, category, position)` rank. With trusted block inputs, when every fill of that account is receipt-placed before all its transfers, cash replay applies the fills first and the transfers afterward, including on a late checkpoint rebuild. Both maker and taker legs use the taker's execution rank. BM MRVL block 1192678957 confirms IOC position 1040 before margin position 1049 despite reversed nonces; the old order explains 112.580034 USDC shifted from isolated to cross cash. A second live fixture, EWY 1193068518 (IOC 765, margin 827), exercises the same ordering with a 6933.849699 USDC split. Mixed cash actions, interleaving, missing/duplicate receipts and unplaced triggered/system fills keep the existing `cashEstimated` boundary. This closes one timing cause; it does not supply unknown execution marks or cross-pool capacity. The compact request/receipt evidence is in `replay/isolated_cash_edge_goldens.json`; the numeric ordering regression uses explicitly synthetic pre-fill cash, not a claimed intermediate node balance.
- Fixed HIP-3 triggers also fire when a unique Normal mark write **reaches** the threshold: TSLA 1193424645 (379.96 → 380.00, Stop Limit above 380) and META 1193428334 (644.07 → 644.00, Stop Market below 644). The opening mark must still be strictly ineligible; already-met triggers and ambiguous write/taker/status evidence keep the full range. Recorded trigger marks reconcile the complete TSLA open and META increase cash intervals exactly, resolving 0.004874 / 0.000224 USDC splits (`marks/fill_prices/hip3_trigger_boundary_goldens.json`). A buy Take Profit Market also triggers on a downward touch: io:ANTH 1193462900, 1916.1 → 1916.0. Its maker's full 71-fill interval 1193460000–1193470000 reconciles exactly; the initially missed 1.404667 USDC margin shrank to 1.244043 after partial closes (`anth_trigger_cash_golden.json`, ample cross funding in the test).
- HIP-3 market-liquidation fills retain the node-reported `liquidation.markPx`. A unique maker/taker pair with equal marks can refine a trusted execution-price range containing that mark; invalid, missing, duplicate or contradictory legs withdraw only that trade's cash-price projection. Native and backstop liquidations keep their existing paths. BM para:LRCX block 1193180450 had two possible marks (233.52 and 232.7792); the recorded 232.7792 reconciles all five affected isolated maker balances at the next checkpoint, including opens, increases and flips. This does not change the node mark or infer action ordering from a later checkpoint. Regression evidence: `marks/fill_prices/liquidation_cash_goldens.json`. The additional META interval 1193430000–1193440000 replays 28 fills, including a liquidation flip and later partial closes: the missed 0.057570 USDC transfer shrank to a 0.046728 USDC cash split discrepancy. Recorded liquidation marks reconcile its isolated cash exactly (`liquidation_flip_cash_golden.json`); this regression uses ample cross funding and does not reconstruct the account's other coins. Other cross-pool capacity and ordering boundaries still apply.
- The bounded node-mark replay cache contains inputs already applied to live state. Its overflow pauses retention until the next checkpoint grid, without manufacturing six upstream stream losses or withdrawing continuous prices (BM1194380235). `mark_replay_retention_overflow` diagnostics count this separately. An unreachable checkpoint keeps the previous seed provenance and supplies no historical fill-price backfill. If its comparison finds any supported price/book disagreement, prices are withheld until a newer checkpoint can actually rebuild; real input loss and seed failures retain their existing withdrawal rules. A comparison and a successful checkpoint install are separate outcomes.
- When a checkpoint finishes loading, a successful trusted mark rebuild now passes its historical per-trade marks back into the account event log, keyed by block, coin and `tid`. Previously those startup/catch-up fills kept `mark=None` even after their prices had been reconstructed (BM seed 1192960000, 185 replayed blocks, first native estimated fills at 1192960004..1192960150). Updated accounts replay from their anchor and are republished; other accounts are not cloned or recomputed. Failed/incomplete mark rebuilds supply no backfill, account loss flags stay intact, and the closed interval's checkpoint check remains independent. One INFO summary names seed, rebuilt blocks and affected accounts.
- `markPx` / `liquidationDistanceBps` (design 000442): the coin's mark at `liquidationPxHeight` — the node mark for a replayed row, the stored mark for an anchor's row — and how far it is from `liquidationPx` in bp of the mark, with the size at that height: positive while the mark has not reached the price (a long's mark above it, a short's below). `null` without a mark or without a `computed` price. Both belong to the row's height: an anchor's row keeps the anchor's mark and distance even when the size has moved since.
- `replayStoppedReason` / `replayStoppedHeight`: why and at which block the account's replay stopped (an isolated position's fill, a backstop liquidation, an account mode switch, …; see the cash replay below): its rows then show the anchor's values and marks until the next anchor. `replayStoppedHeight` is `null` for an account stopped at the anchor (`portfolioMargin`, an unknown mode).
- `fillsHeight` / `miscEventsHeight` / `blockActionsHeight`: the highest block each stream has been read to.
- `changedSinceLiquidationPx`: the observed sources that touched the account after the anchor, each with the **first** block after the anchor (from there the anchor's liquidation price may be stale): `fill`, `ledger`, `funding`, and the successful block actions `margin` (`updateIsolatedMargin`, `topUpIsolatedOnlyMargin`, `updateLeverage`: isolated margin moved, leverage or cross/isolated changed), `accountMode` (`userSetAbstraction`, `agentSetAbstraction`) `withdrawal` (`withdraw3`, debited when requested; its ledger event comes minutes later), and `requestWeight` (`reserveRequestWeight`, paid request quota). A block-action mark is an action that may invalidate the anchor, not proof that the state changed (setting the mode an account already has marks it too). The account is the action's response `user`, the vault or sub-account when the action names one. On an anchor row, once a source has a mark, later events of that source no longer change the row by themselves (an account trading every block is not pushed every block); an earlier block arriving late still moves the mark back, and position changes in the coin are pushed as always. An account that replays (design 000437) has every event touch its rows, since they may move a replayed row. **Empty does not prove the liquidation price unchanged**: marks follow the account's own events, not mark-price moves of the other positions in its pool, nor the borrow-lend interest of portfolio-margin accounts. Between two checkpoints of 2026-10-19 every other margin, mode or cash change of a holder came with a fill, ledger event or block action, or was a transfer inside its own pool (unified and portfolio-margin accounts move cash between their dexes, spot and borrow-lend).
- Errors: `404` for a coin outside the anchor's universe, `503` with the reason until the first anchor and whenever data was lost that no anchor covers yet (the reason names the stream and the bound). Every block has a `replica_cmds` line, so a skipped block height is a loss of the `block_actions` stream up to the last missing block.
- Send `Accept-Encoding: gzip` (curl: `--compressed`): PUMP is ~2 MB of JSON.
- The block log costs about 0.7% of one core to follow (bm, 2026-10-19; it parses at 2.3 GB/s, the node writes 6–10 MB/s). At a query the newest block read is a median 205 ms (p99 385 ms) past its block time; that includes the node's own write delay, since `replica_cmds` lines carry no write time.
- Initial file adoption starts at byte EOF. If that byte is inside an unfinished record, the reader skips only its remaining suffix through the next newline; it cannot certify read-through before that boundary. This expected startup positioning is separate from a corrupt complete row or a lost tail during rotation, which still flags stream loss. Startup and malformed-row diagnostics include the file path, inode and byte offsets; raw JSON is not copied into the log.
- Blocks are applied whole and in height order (design 000433): a block's fills, misc events and actions take effect together once the block is complete, that is its `replica_cmds` line and the next block's were read, and the fills, misc events, HIP-3 oracle updates, book diffs and order statuses readers each proved a read-through (every line written before an instant has been read: no newer file, a drained file, no partial line) later than the next block's line was read. The stream heights in the view are those of applied blocks. This rests on the node's measured write order (a block's last fills, misc, HIP-3 oracle update, book diff and order status lines arrive before the next block's `replica_cmds` line; bm 2026-10-20, no exception), not on a protocol guarantee: a line for a block already applied — a node replay of written blocks, or a broken order — is a loss up to that block, so a node restart may withhold the view until a covering checkpoint. Inputs repeated before their block completes count once (fills by account, coin, trade and side; misc lines by their `events` text). Metrics: `orderbook_accounts_applied_height` (complete blocks only), `orderbook_accounts_pending_blocks`, `orderbook_accounts_duplicate_inputs_total{stream}`; the losses `late_or_dropped` and `pending_overflow` count in `orderbook_accounts_data_loss_total`.
- A triggered `Trailing Stop Market` can supply its realized `triggerPx` to HIP-3 cash attribution. BM MRNA block 1193502103 fell from 201.96 to 201.88 through the reported trailing threshold 201.8898 (best 206.01, retracement 2%). The same sole-successful-Normal-write, strictly-ineligible-opening, unique-taker/status checks apply; waiting/zero thresholds, unknown forms, multiple writes and already-satisfied thresholds remain ambiguous. This uses the node's event threshold and does not simulate the trailing order's future evolution. `marks/fill_prices/trailing_stop_cash_goldens.json` preserves two complete MRNA intervals including hourly funding: 20/22 fills plus one funding event each, and old cash splits of 0.122038/0.029300 USDC. An additional CRCL interval 1193550000–1193560000 contains six fills plus one funding event; its recorded trailing trigger resolves the 0.028357 USDC isolated cash split exactly. Tests supply ample cross funding explicitly; execution-time cross capacity remains a separate estimation boundary.
- Cash replay between anchors (design 000433, second batch; served as the replayed rows above): the margin state of every account that held a position at the anchor (default / disabled: cross cash per USDC dex; unified: the USDC pool of spot plus every USDC dex; isolated cash per position) moves block by block with its fills, funding, ledger updates and block actions, by the measured rules of the research `hl_account_state_verify_20261019`. An isolated position's fill moves its cash and its dex's cross cash, in the node's micro USDC (research "逐仓成交的划转", checkpoints 1192420000 → 1192440000, every single-fill isolated position): the fee leaves the position's cash first; a partial reduction then moves the reduced share of the rest to cross, using the client's f64 fraction and multiplication before truncating toward zero (PUMP 1192669509 exposes a one-micro difference from integer arithmetic). If equity at the fill price is negative, the closing trade cash instead stays isolated; a close moves all of it (768 of 768); an open or increase transfers `floor(q × max(P, M + s × L × (P−M)) / L)` in micro USDC, with integer price/size scales adding to six, P the fill price, M the execution mark, L the isolated leverage and s +1 for buys / −1 for sells. This funds initial margin at mark plus immediate loss, with fill-notional/leverage as a lower bound. A flip closes the old position and funds only the remaining new exposure, leaving the whole fee/rebate in the old position. Funding is capped by pre-fill cross free margin plus the old leg's net cash release. With no cross exposure since the anchor, free margin is positive pool cash (whole micro USDC; sub-micro spot dust is retained). This cap explains the six live cash-split mismatches at 1192650000 → 1192660000; five full-window goldens and the LTC single-fill golden preserve their evidence. Standard accounts cap within the same dex; unified accounts use their shared USDC pool. Cross positions need execution-time cross equity and initial-margin valuation: until supplied, the transfer stays explicitly estimated rather than treating raw cash as free collateral. The leverage comes from `l.I.l` / `updateLeverage`; an isolated-only asset without a setting uses its margin table's first max leverage. Recorded marks explain 815/815 open/increase and 58/58 flip samples; seven HYPE/PURR cases had no mark recording. Native execution-price bounds follow the existing action/fill cuts, including TWAP and paired maker/taker legs. HIP-3 bounds include the opening mark and **every** price update in that block because the stream lacks action identity. Only when both capped bounds give the same transfer is it exact; a cap below the fill-notional floor also determines the result without a mark. Missing or discriminating bounds keep the earlier estimate and `cashEstimated`; neither the block-final mark nor a later quote is substituted. A same-block `updateIsolatedMargin` and isolated fill remain estimated: the account adapter applies actions first, but cash transfers and reductions do not commute (xyz:MRVL 1192678957 differed by 112.580034 USDC). This is an explicit ordering gap, not a protocol error, and does not stop replay. Funding still reads the preceding block's oracle. Execution evidence is logged with the cash event for checkpoint replay. The account's rows say so (`cashEstimated`, on `extCoinPositions`, `extLiquidationCandidates` and the forced flow's liquidation entries). An event no measured rule covers (a fill in the block its asset's isolated setting or leverage changed, a backstop liquidation, an account mode switch, `borrowLend` on a unified account, a ledger type not read, another collateral, …) stops that account until the next anchor. An anchor parsed before its blocks are applied waits for them, then checks the replay of the interval it closes, account by account to the last 10^-8 USDC (after an estimated isolated transfer, an account whose pools' totals, cross plus isolated, equal the anchor's and whose split between them does not is counted `estimated` with up to 50 `replayEstimated` evidence lines; a total off the anchor stays a `mismatch`; the replayed rows compared with the anchor's own count the rows off the anchor's price of such an account, and of no other, apart in the same way: `orderbook_accounts_replay_rows_checked_total{result="estimated"}`, `replayRowEstimated`): `orderbook_accounts_replay_checked_total{mode,result}`, `orderbook_accounts_replay_stopped_total{reason}`, `orderbook_accounts_replay_checks_skipped_total{reason}`, and up to 50 `replayMismatch` evidence lines.
- Checkpoint cross portfolios supply isolated transfer capacity for Standard/Disabled per-dex accounts: `max(crossCash + sum(signedMarkNotional) - sum(floor(absMarkNotional / storedLeverage)), 0)`, in micro USDC. Checkpoint `l.C` is retained; omitted `l` means the serialized 20x default. Known holdings advance with fill start sizes, retaining leverage through close/reopen. Unknown holdings, contradictory start sizes or leverage actions discard the projection; Unified routing remains separate. A unique Normal HIP-3 oracle and ordinary taker receipts can select same-dex collateral prices; otherwise only unchanged block envelopes establish exact value. Missing/ambiguous prices and scale mismatches preserve `cashEstimated`. The mark owner records a shared envelope for only the needed coins, retains it for seed replay, and withdraws it with price state. BM GIGADEV 1193760000–1193770000 has nine fills and nine unchanged cross holdings; block 1193768936 can transfer only 121.594406 USDC of the required 121.728550. Applying this cap reconciles both ending cash balances exactly, correcting a 0.126408 USDC split. `replay/cross_capacity_golden.json` retains the recorded endpoints, fills and block prices; full input archives stay outside Git.
- Unified accounts can price aggregate capacity from the validated cross holdings of every USDC dex, including foreign-dex collateral for an isolated fill. Exact physical routing still requires one active USDC dex and zero spot USDC at the anchor; positive spot funds retain `cashEstimated`. A foreign-USDC-dex cash/fill event withdraws the physical-routing proof while keeping validated holdings; a spot mutation still discards the projection. Known cross fills advance quantities; unknown holdings or leverage changes invalidate the affected portfolio, preventing a partial portfolio from claiming aggregate capacity. After intervening cross fills, maker prefunding remains estimated. No movement restores physical-routing confidence before the next seed. Negative cash is combined with integer collateral equity in wei before final micro-USDC truncation, preserving spot dust. Missing or changing collateral marks stay estimated. BM PUMP/XMR 1193830000–1193840000 has one PUMP increase: XMR mark 611.17 caps funding at 165.600036 USDC instead of 165.988949, exactly closing a 0.388913 USDC cash split. The mark replay independently matches 234 coins × seven fields and 319,965 orders at the endpoint; `replay/unified_capacity_golden.json` retains the account endpoints and fill. This does not extend exact capacity to general Unified cross-dex routing. The two complete windows in `replay/unified_cross_dex_capacity_windows.json` cover CL 1194850000–1194860000 (HYPE collateral, 0.004428 USDC split) and CRCL 1194870000–1194880000 (STX flip, then isolated flip funded only by the old isolated release, 0.011466 USDC split). Their amounts reconcile after aggregate capacity across dexes is included; both remain `cashEstimated` because physical routing is not proved.
- `topUpIsolatedOnlyMargin` is parsed separately from `updateLeverage` and fixed `updateIsolatedMargin`: `asset` plus the API's float-string `leverage`. The inspected client (6911b792, handler 0x3c94170) permits only `strictIsolated` assets and target leverage 0.5–1, computes a positive top-up from execution-time mark equity, and delegates to the fixed-margin action (insufficient capacity rejects rather than partially funding). It does not change the leverage setting or remove margin. Cash replay still needs the exact action-time position/cash/mark and an observed golden: a successful action stops only that account, with `replayStoppedReason` carrying `asset`, `targetLeverage`, and `executionStateUnverified`; the metric label stays `topUpIsolatedOnlyMargin`. A rejected action has no effect. The binary range is research evidence, not an extra API parser constraint. See the account-state research's target-leverage section for formula, rounding and remaining inputs.

- Node marks (design 000439, batches 1, 2a and 2b; read by the replayed rows of shared pools and every row's `markPx`, design 000442): the owner of the view also keeps, block by block and applied before the view's block, the node's HIP-3 marks from its oracle update stream (`hip3_oracle_updates_streaming`, node flag `--write-hip3-oracle-updates`: one deployer's update per line, `oracle_pxs.coin_to_mark_px` the mark the node applies, already limited) and the main dex's books order by order from its raw book diffs (`node_raw_book_diffs_streaming`: one diff per line; the watcher keeps every market's diffs, ~12,000 a second on 2026-10-20, since the execution points below anchor on all of them; the books follow only the main-dex perps' diffs, ~7,400 a second on 2026-10-19, and the spot books named below). Each checkpoint seeds them; it also carries the main dex's oracle state, parsed in the same pass and checked apart (a seed breaking its contract withholds only the marks), for the per-action replay of the main dex's prices: block by block, each `SetGlobalAction` and validator reference action of `replica_cmds` runs at its place among the block's diff lines (placed by the orders the block's other actions created or cancelled, the makers their takers hit, the orders a self-trade check removed — a no-fill IOC matched one-to-one to its full request and contiguous cancellation statuses (including older orders and cross-vault cancels), or an order created in the block, inside the one later order of its account that could have taken it (that coin, the other side, a limit reaching it; an IOC rejected for no match included), when no order the block triggered could have taken it too —, and the main-dex orders a lone `SetGlobalAction` triggered (HIP-3 triggers belong to their own price actions); cancels by cloid resolve through `node_order_statuses_streaming`, whose watcher preserves every status as a cancellation-run boundary; only the needed facts survive in the replay log; unmatched request/item-response counts identify no rejected IOC and preserve any placed oids; a multi-order batch with one error-only response is counted, without evidence; other shapes are counted and sampled as `nodeMarkIocPairing`, at most 20 actions per installed seed interval (also 20 before the first seed), with block height, bundle hash, rank, counts, and only that action/response; each wire excerpt is capped at 4 KiB, with original byte length and an explicit truncation flag; no whole replica block enters the log, pending blocks or replay log; quotas renew only when a seed actually rebuilds the marks, not on losses or failed/unreachable seeds; `orderbook_node_marks_ioc_pairings_total{kind="batchRejected"|"unknown"|"suppressed"}` counts observations at ingress (including rereads), while `orderbook_accounts_evidence_total{kind="nodeMarkIocPairing"}` counts logged samples; the existing `batchModify` and cancel pairing contracts remain strict) and moves the coin's stored oracle, mark, basis and local-price EMAs and reference caches by the node's rules. A reference action pricing a coin with one of the two prices takes the local-price EMA, previewed with the book's local price, as the third Mark candidate. HYPE and PURR take their Spot reference from the spot books the checkpoint's `oracle.psro` names (`@107` HYPE/USDC, `PURR/USDC`), seeded from the checkpoint and moved by their diffs: at every validator reference action, the book's floor midpoint at the action's place, rounded half to even into the perp's price units, whatever the validator sent (Codex `hype_purr_internal_reference_20261020`: 12,819 reference actions and 700 `SetGlobalAction`s replayed, both coins' prices, EMAs and caches equal to three checkpoints). Main-dex marks take the selected median without a per-update 1% limit. That limit belongs to the Hip3 branch: binary 6911b792 tests `clearinghouse.moh` at +0x378 (0x2965e0a); Main jumps directly to the write. ALT liquidation fills at blocks 1192112978 and 1192114308 confirm this distinction. HIP-3 still uses its node-provided final-price stream. Checkpoints check only the end state; later updates can hide an incorrect intermediate mark, so liquidation fills provide independent point checks. A coin whose inputs take a path never observed (an empty book side, a local-price EMA gap, an EMA not yet warm for the third candidate) is not replayed until the next seed. A price action whose place among the lines is not unique reads the book at the first line not known to run before it; such actions are counted in `orderbook_node_marks_ambiguous_cuts_total`, the first 20 per seed interval as `nodeMarkAmbiguousCut` evidence. A diff that does not fit the books (an unknown order, a size it did not hold) is a loss of that stream. At each grid height the marks and books are kept; that height's checkpoint, once its blocks are applied, is compared with them and rebuilds them from itself plus the blocks logged after it. A loss on any stream the marks read (fills, block actions, HIP-3 oracle updates, book diffs, order statuses), or a seed breaking its contract withholds the marks until a covering checkpoint rebuilds them from a complete log. Replay-cache overflow alone preserves continuous live state and pauses retention until the next grid. A seed that cannot rebuild leaves live state and seed provenance unchanged, unless its comparison reveals a disagreement: that known error withholds prices until a newer successful rebuild; an exited watcher or poller stops them until restart, as it stops the view. Metrics: `orderbook_node_marks_available`, `orderbook_node_marks_height`, `orderbook_node_marks_seed_height`, `orderbook_node_marks_logged_inputs`, `orderbook_node_marks_checked_total{market,result}` (`hip3`; `nativeBooks`; `nativeOrders`: equal, differ, missing, extra; each main-dex price field `storedOracle`, `mark`, `msoe`, `lmpe`, `spotCache`, `externalPerpCache`, `markCache`: equal, differ; `nativeUnsupported`: by reason), `orderbook_node_marks_checks_skipped_total{reason}`, `orderbook_node_marks_rebuilds_total{result}`, `orderbook_node_marks_seed_failures_total`, `orderbook_node_marks_unknown_coin_inputs_total{stream}`; stream losses in `orderbook_accounts_data_loss_total{stream="hip3_oracle_updates"|"book_diffs"|"order_statuses"}`, mismatches as `nodeMarkMismatch` / `nodeBookMismatch` / `nodePriceMismatch` evidence, each with the replayed and the checkpoint's value (a price field's as text: numbers and EMAs as written, a cache as its median, bucket and members by validator). The view needs two checkpoints on disk to start: the marks take the node's grid from the newest two.
- Main-dex stale-input refresh also runs at block start. It seeds `msc.mou`, `msc.sgd`, `msc.sog`, the outer `begin_block_logic_guard`, `lnsgt`, and startup gates from the same checkpoint. After the outer bucket and startup gate, the stale deadline and inner bucket may reapply cached submissions to the previous block's book and last trade; an automatic write does not advance the explicit submission clock. Its mark is visible to that block's cash replay. Automatic plus explicit writes leave otherwise unplaced trigger effects ambiguous. Missing seed state or changed submitted coverage withholds marks until checkpoint reseed; removal semantics are not guessed. Verified against native a6e2897c guard instructions and the SAND 1197480000–1197490000 retained projection: all seven price fields and book exact, fill 1197481585 uses 0.047053. This is offline evidence; each deployment still needs online checkpoint acceptance. The optional full-window test is `SAND_REPLAY_INPUT=/path/cut_inputs.jsonl cargo test -p server --lib sand_historical_production_replay -- --ignored --nocapture`.

- Vault withdrawals also anchor automatic `Vault Close` IOC fills to the successful withdrawal's category-3 execution rank. The receipt user is the depositor; the matching status user must be the action's `vaultAddress`, with a current-block creation timestamp, `reduceOnly`, and non-trigger IOC shape. Only one successful withdrawal per vault is attributed (including a `multiSig` wrapper). Multiple withdrawals or a competing system/CoreWriter withdrawal attempt leave those effects unplaced; they do not turn a normal business result into a protocol error. Real block 1191962420 contains 16 such orders whose fills preceded the next bundle's SGA; they are neither ordinary order receipts nor liquidation fills. Golden evidence is in `server/src/accounts/marks/execution/vault/golden.json`; full raw intervals stay in the dedicated research-data directory.
- A successful single `modify` may return only `default`, without the replacement oid. Its adjacent `open(new)` / `canceled(old)` statuses supply that oid only when the old oid/cloid, user, asset, side, numeric price/size, reduce-only flag, cloid, TIF and current-block timestamp match, one request to one pair. An interrupted/ambiguous pair or a competing CoreWriter request contributes no attribution; known default receipts do not emit errors or evidence logs. Ordinary receipts retain their oids. Block 1191964185 demonstrates a ZEC replacement immediately filling at 1313.1 before the next bundle's SGA; previously only the old cancellation was placed, leaving the SGA's local price at 1313.2. Its small golden is in `server/src/accounts/marks/execution/replacement/golden.json`.
- Mark replay attributes an observed no-fill reduce-only GTC placement’s contiguous `reduceOnlyCanceled` tail only with a unique full-field resting receipt, old same-user/coin/side orders and no competing fills, triggers or system owner. ZEC block 1196468228 otherwise retained ask 1191.7 instead of 1191.8 at the next SGA; the 1196460000→1196470000 offline window matches all seven price fields after this attribution. Missing or ambiguous evidence keeps the old conservative cut; this does not predict which orders the node should cancel.
- Reference cache refresh uses the main clearinghouse's explicit `halted_assets` set. Every new 3-second block-time bucket advances each non-halted coin's three cache guards, including empty caches and blocks without price actions; a median changes only when there are fresh members. Halted keys keep their complete state. This follows archived node `6911b792` (guard `0x3ca3ece`, median write `0x42c13fe`, halt check `0x42689de–0x4268a36`), correcting the previous inference from stale caches. PURR checkpoint 1191820000 crossed a bucket without a price action and exposed the missed guard update; mark and EMAs were equal. The 2026-10-21 goldens cover PURR plus empty/stale halted keys; two checkpoint intervals' 1,404 cache buckets agree. The seed rejects missing or invalid halt metadata for marks while account seeding remains available. Halt/unhalt actions between seeds are not reconstructed here; these samples have an unchanged halt set.
- `vaultDistribution` ledger rows are separate cash legs: the vault's own row debits its aggregate distribution; each recipient row credits only that recipient's allocation through its main/unified pool. H1191750497 distributed 195999.419995 USDC in 14 rows, including zero allocations and a 0.000001 USDC allocation. All 14 main/unified-spot cash projections match the next checkpoint after the interval's withdrawal and cross-funding payments (`vault_distribution_golden.json`). The initiating `vaultDistribute` action is not charged again.
- Successful `reserveRequestWeight` debits the receipt user's main perp cash (or unified pool) by 0.0005 USDC per request, as specified in the [exchange API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint#reserve-additional-actions). The optional `destination` receives quota, not a cash debit. H1191758386 and `request_weight_golden.json` confirm 100,000 requests cost the payer 50 USDC; checkpoint balances for both parties match, with no separate ledger debit. Failed actions cost nothing in this replay; invalid successful weight is a contract error.
- A new isolated-only position without a stored leverage setting uses **min(first-tier max leverage, 20)**, not the tier maximum alone. Stored settings and successful `updateLeverage` actions retain their requested leverage. Archived client `6911b792` constructor `0x44ce150` caps at `0x44ce291..0x44ce2b6`; JPY H1192705810 uses 20 on a 50x table. `isolated_default_leverage_golden.json` records both fills, pre/post checkpoint position state and the preceding final mark (no same-block update): 20x reproduces 2151.601586 USDC isolated cash, correcting the prior 61.479600 USDC split error. Cross-collateral capacity remains estimated for that account; this verifies the default and isolated cash, not complete account exactness.
- `internalTransfer.usdc` is the **gross sender debit**; the recipient receives `usdc - fee`. H1191755982 sent 50 USDC with a 1 USDC fee: sender cash went from its 50 USDC deposit to zero, and the unified recipient received 49. `internal_transfer_golden.json` binds both checkpoint balances and the other receiver ledger events in H1191750000→1191760000. This rule is specific to `internalTransfer`; `send` and `spotTransfer` retain their own semantics.
- Accepted `withdraw3.amount` is converted to **micro USDC with decimal arithmetic, midpoint ties to even**, then represented in the cash replay's 10^-8 units. Archived client `6911b792` path `0x2d95420 -> 0x41b1240 / 0x44dbd10 -> 0x40bea60` and `withdrawal_goldens.json` bind the conversion. H1191730512's `62.14531414` request debits `62.145314`, retaining 14 units of spot/unified dust; the later withdrawal ledger never debits again. Ledger/fill amount normalization is unchanged. The prior adapter's rejection of non-decimal request spellings (the client's separate float fallback) remains explicit.
- `Spot Dust Conversion` is a normal system operation, distinct from an order-book fill. A complete block is grouped by coin and system order id: the reserved `0xee..ee` seller's actual USDC proceeds **after its fee** are distributed by each recipient's token-wei quantity, not by the printed recipient `px × sz`. The client truncates `f64(quantity / total) × net`, then gives the remaining wei to the largest dust holder. H1193705508's 52 batches / 1,628 credits match the original client allocation routine; its bytes also match running client `a67d59b7`. Shared immutable batch facts survive delayed checkpoint loading, with one metadata-checked allocation projection per batch. Supported scope is one system fill selling the entire combined order-lot quantity; missing/duplicate recipients, missing/multiple/partial system fills, unsupported quote tokens, or a largest-holder tie with a nonzero remainder retain the explicit `spotDustConversion:<coin>:<reason>` withdrawal boundary. Standard/disabled perp pools still exclude spot. `accounts/spot_dust_allocation_goldens.json` retains compact native outputs; the prior two endpoint examples remain in `spot_dust_goldens.json`, and full raw archives stay outside Git. Ordinary fills retain their market precision checks.
- Funding cash balancing follows archived client `6911b792` (`0x4264330` / `0x3c8f2d0`): each complete dex batch contributes its signed net payment; the largest single position at the preceding block's **stored oracle** receives the opposite cash adjustment. Equal notional selects the lower address, then the first wire position of that user. All wire deltas participate, including zero amounts and users outside the view. Settlement `szi` uses the existing size normalizer. Funding and its derived adjustment apply before this block's account actions/fills and price actions; the event log replays that decision once after a seed install. Wire funding records/cumulative funding are unchanged. Missing settlement prices withdraw only potential recipients' cash replay (`fundingBalanceUnknown`), with every affected batch counted and bounded contextual diagnostic summaries. A funding-dependency diagnostic clears only after a trusted anchor covers the settlement block; mark availability alone is not cash-replay recovery. HIP-3 stored oracle now shares the mark owner's seed, stream and recovery lifecycle and checkpoint checks (`hip3StoredOracle` metric/evidence). `funding_goldens.json` records ten observed residuals and external archive hashes; the opt-in full-batch test verifies 849,268 records from two hours using preceding checkpoint oracles (recipient stability, not full historical price reconstruction).
- Account cash replay unwraps successful `multiSig.payload.action` only at its own adapter. It uses the outer receipt's effective user and status; ledger-owned transfers remain ledger-owned. Golden block 1191644834 restores a missing 67.297170 USDC cross-to-isolated move. Shared mark/action ordering is unchanged; failed wrappers are skipped, malformed successful kept actions remain explicit contract errors.
- CoreWriter book effects join the node-mark replay through `system_and_core_writer_actions_streaming` (node flag `--write-system-and-core-writer-actions`). This sparse stream must prove read-through with the other readers before a block completes; duplicates and losses use the existing block/seed lifecycle (`system_actions` metric label). Its rows are attempts, so only unique matches to full order-status fields or successful cancel statuses anchor effects. Ordinary IOC/GTC orders use the archived client's pre-bundle delayed-action phase; all-ALO orders use its EVM-tail phase. Cancel timing follows `f`: true is zero delay; false or an omitted flag uses the ordinary delay. The archived CoreWriter ABI decoders initialize that byte to false, and real block 1193395698 omits it. Explicit null or non-boolean values remain invalid. The selected cancel-plus-IOC golden (`system/core_writer_cancel_golden.json`) verifies that both effects precede a price action; it does not claim a full checkpoint interval replay. The checkpoint supplies `pt.prp.D` and `action_delayer.delay_scale`; absent, zero or invalid timing cannot certify a delayed action. Unknown order grouping with a matched effect is an explicit mark-input failure; ordinary rejected attempts have no fabricated successful effect. Competing native/system requests retain ambiguity. These are version-specific client rules (6911b792), not an official API guarantee; priority order groupings, live delay-configuration transitions and other system action kinds are not reconstructed by this addition. The ETH golden at height 1191429459 preserves the CoreWriter attempt, order statuses, both fill legs and diffs: the ETH trade precedes SGA while the same block's ZEC trigger follows it. Large raw archives stay outside the repository.
- Diagnostics distinguish initialization, input continuity/format, capacity, missing dependencies and our reconstruction. Starting at an existing file's EOF is expected initialization (INFO), while still requiring the first covering checkpoint. `reconstruction` means our replay could not apply the input; it does not establish an upstream protocol violation. Known business rejections remain counters only; unsupported account paths retain their explicit per-account stop reason. A newer waiting checkpoint superseding an older one and retiring delisted records are ordinary lifecycle INFO events.
- The account runtime owns bounded diagnostic episodes keyed only by stream and cause: a first sample (at most 1 KiB of detail plus a truncation notice), then at most one summary per key every 30 s, and a recovery summary once both views are trusted and their anchors cover the affected block. Every loss still advances the original validity owners and counters. `orderbook_accounts_diagnostic_events_total{category,cause,output}` counts sampled and aggregated observations; the legacy `orderbook_accounts_data_loss_total` also includes startup positioning/reconstruction observations and is not a count of proven network losses. File open/read retries similarly keep the first error until a read/EOF succeeds and count every attempt in `orderbook_node_file_io_errors_total{operation}`. A successful reopen alone does not reset the error episode; normal consumer shutdown is INFO.
- Research evidence is not a service failure: `rareLine` and `nodeMarkAmbiguousCut` use INFO. Raw line samples are at most 4 KiB of UTF-8 before JSON escaping, with `rawLineOriginalBytes` and `rawLineTruncated`; full wire facts remain in the node stream archive. Reasons retain at most four examples and a 1 KiB prefix plus a truncation notice. The runtime emits at most 20 rare-line samples per stream per 10 minutes, counts every sampled/suppressed line, and does not reset that quota on checkpoint installation. This selection does not change business processing or validity. Mismatches still use WARN; `silentPositionChange` is capped at 100 samples per install. `orderbook_accounts_evidence_total{kind}` counts emitted evidence lines only.

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
- `cashEstimated`: the row's account's cash is modelled since the anchor (see `extCoinPositions`' `cashEstimated`); such a candidate may also be missing, its cross price farther than the node's.
- `bookWalk`: where the order would fill on the public book of this server, static (no cascade, no refill). The candidates of one coin and side walk together in that order — longs from the highest liquidation price, shorts from the lowest, equal prices by address — each taking its `walkSize` from where the ones before it (`aheadSize`) stopped (`together`); `alone` is the same size walked from the best level, as if no other candidate came first. Each gives `vwapPx`, `firstPx` (the first level it takes, from where its walk starts: for `together` it may lie below the book's best), `deepestPx`, `deepestBpsFromMid` (how far the deepest level is from `midPx`, the book's mid at the read; `null` with an empty side), `spanBps` (from `firstPx` to `deepestPx`, never negative, 0 for one level; `null` without a fill), `filledSize`, `levels`; `end` is `filled`, `bookExhausted`, or `truncatedAtLevels` (the walk reads at most 2,000 levels, the book may hold more). `assumption` is `unknownCooldownAsFull` when this candidate or one before it walks a whole position for an unknown cooldown. `bookHeight` / `bookTime` are the book's own progress at the read, not the candidate's height; the walk is redone when the candidates of that coin and side, their order or their sizes change, not on every book move. `status` is `pending` until a walk of the current list is in: a new candidate, and every candidate from the first changed place of the list on (an estimate holds only for the list it walked). `untrusted` while the book is withheld: every estimate goes to it at once, and an estimate read before cannot come back — snapshots and frames publish each walk as of the book's state when they go out, so one from an earlier withheld period shows `pending` until walked anew. `noBook` in a process without the order book.
- Metrics: `orderbook_liquidation_candidates` (published candidates), `orderbook_liquidation_candidates_refresh_seconds` (the owner's work per refresh), `orderbook_liquidation_walk_lock_wait_seconds` / `orderbook_liquidation_walk_lock_hold_seconds` (a walk waiting for the listener lock, and holding it to copy the levels); frames in `orderbook_accounts_frames_total{kind="candidates"|"candidatesWithdrawn"}`.

### Forced flow: extForcedFlow

With `--accounts`, per coin, the market flow a mark move would force out (design 000451): the trigger orders (stops, take profits, trailing stops; the listener's untriggered orders, which fire on the mark) and the liquidations, as facts. Each entry is sized alone by the node's rules against the account's current position — a position TP/SL takes the whole position, a reduce-only order at most what it reduces (`noPositionToReduce` with nothing to reduce), any other order its size, a liquidation its next market order (`expectedOrder`, as `extLiquidationCandidates`) — and walked alone on the public book up to its own limit price (a market trigger order carries its 10% slippage limit; a liquidation takes any price). No scenario: an entry ignores every other entry, so two orders of one account may each cover the same position, and nothing chains (that is the trading system's call).
```json
{ "method": "subscribe", "subscription": { "type": "extForcedFlow", "coin": "BTC", "maxDistanceBps": 100,
  "sources": ["trigger", "liquidation"], "excludeCoins": [], "minNotionalUsd": 10000 } }
```
- `coin` may be `null` (every coin with a node mark); `maxDistanceBps` at most 1000. `sources` (both), `excludeCoins` (none) and `minNotionalUsd` (10,000 USD: the entry's size × its level) may be left out. A coin both subscribed and excluded is refused, and so is an unknown parameter (a misspelt filter never runs as its default).
- Distances in two calibres: `distanceNodeBps` from the node mark (the account view's replay, main dex and HIP-3) and `distanceLocalBps` from the local predicted mark (`extMarkPrices`, main-dex coins of the local marks' sources; `null` with the local leg's `reasonCode` otherwise, `localMarksOff` without `--local-marks`). Positive while the mark has not reached the level. The node one is negative for an order the node mark is past already - placed past its level, it fires at the next price action (bm 2026-10-21: a take profit 94 bp past fired 18 blocks after its placement) -, the local one when the local estimate is past it (the next mark update may fire it — a conditional estimate, not a promise). An entry is in when either calibre is within the distance, however far past its level: the table's band read gives a fall's orders from the range's low end up and a rise's up to its high end (`listeners/order_book/triggers.rs`, by direction: `Price below` and an activated trailing sale fall, `Price above` and an activated trailing buy rise).
- Full frames, by tier: a subscription's distance falls in a tier — up to 10 bp, 50 bp, 100 bp, 1000 bp — computed for every coin with a subscriber (or a query) at most every 100 ms, 100 ms, 1 s and 5 s (a start cadence: a computation under way, the reads' queue and the listener lock come on top). Each frame is the whole content of a coin at its tier (`seq` numbers the coin's contents; replace, never merge); a frame goes out when the content changed — the walked books' heights and times alone are no change. A local leg turning withheld or ending is withdrawn at once - a frame without the local calibre: no local distances, the entries only the local mark took in gone - even when the owner sees a closed channel before its notice; turning withheld, coming back or ending also recomputes at once, whatever the computation under way (a read started with the other validity is voided); a book withheld while the walks were out leaves them unwalked and the trigger part `incomplete` (`bookWithheld`). `{"channel":"extForcedFlow","data":{"coin","tierBps","maxDistanceBps","seq","heights",…}}` — `heights`: `accountsHeight` / `accountsTimeMs` (the account view: positions, liquidation prices, the node mark), `listenerHeight` / `listenerTime` (the block and block time of the furthest of the two book streams), `triggerStatusHeight` (the block of the last order status the trigger table applied: the trigger part's own progress, behind `listenerHeight` while the diffs lead, its blocks below complete and itself possibly in part, one status a line; bm 10-21: 3 of 6,279 triggers were placed in the blocks between the two), `triggerGeneration` / `triggerVersion`, `localPublishedMs` —, then `status` `unavailable` with its `reason`, or `ready` with `nodeMarkPx`, `local` (`ready` with `markPx`, or `withheld`), `triggers` (`complete`, or `incomplete` with `trackingDisabled` / `snapshotWithoutTriggers` / `snapshotParseFailed` / `liveParseFailed` / `childUnmatched` / `successorPending` / `truncated` / `bookWithheld`), `walks` (`walked` with each side's book, `untrusted`, `noBook`), `entries`, `bands` and `unplaced` (the coin's trigger orders not placed: trailing stops waiting for activation — the table hands them with every band —, conditions or order types of another form).
- An entry: `source` (`trigger` / `liquidation`), `direction` (`down` / `up`: the mark move that reaches it), `side` (`A` sells into the bids), `user`, `levelPx` (trigger price or liquidation price), the two distances, `size` and `sizeReason`, `notionalUsd`, `positionSzi`, `limitPx`, `trigger` (`oid`, `orderType`, `triggerCondition`, `triggerPx`, `reduceOnly`, `isPositionTpsl`, `trailing`: an activated trailing stop at its trigger price as it stands, `orderSz`) or `expectedOrder`, and `walk` (`vwapPx`, `firstPx`, `deepestPx`, `deepestBpsFromMid`, `spanBps`, `filledSize`, `levels`, `end`: `filled`, `limitReached`, `bookExhausted`, `truncatedAtLevels`; `steps`: the levels it takes, best first, each `px`, `levelSize` (the level's whole size in the copy the walk read) and `takenSize` (what this entry takes of it) — at most 100: `levels` and the other fields are always the whole walk, so `levels` > the number of steps means the list is its first 100 (and their `takenSize` does not add up to `filledSize`), while `end: truncatedAtLevels` means the walk itself stopped at the 2,000 levels it copies; `[]` when walked and nothing taken; `null` unless walked). A static estimate of the book as read: no queue position, no refill, no other order taking first. `cumulativeWalk` (user 2026-10-25): the entry's prefix - the market trigger orders and liquidations of its direction and side at its level or nearer the mark, itself and every equal level included, every one of the tier's entries whatever the subscription's sources or notional - walked together from the side's best level (the same fields, no steps, no limit): what a mark move reaching its level fires with it and pushes the book to, whichever of them the node runs first; `null` for a limit trigger order, a prefix of nothing, or unless walked.
- `bands`: `node` and `local`, per (direction, side), cumulative to each edge of 1/5/10/25/50/100/200/500/1000 bp up to the subscription's distance: count, size and notional of the trigger orders, the liquidations, the market and the limit trigger orders, the reduce-only and the other trigger orders (two views of one set: not to be added). The bands take the subscription's `sources`, never its `minNotionalUsd`. Each band also carries `cumulativeWalk` (user 2026-10-25): its market trigger orders and liquidations together, walked from the side's best level on the book the entries walked (`vwapPx`, `firstPx`, `deepestPx`, `deepestBpsFromMid`, `spanBps`, `filledSize`, `levels`, `end`; no steps, no limit), `null` with neither or unless the walks are `walked` with that side's book. Only the sum is walked: the node runs the trigger orders a mark move fires in one block in an order of its own, neither by trigger price nor by oid (bm 10-23 ZEC, 10-24 NEAR, 10-25 WLD, up to 1,282 orders in a block), and what they push the book to together does not depend on that order; the limit trigger orders, each stopping at its own limit in that order, are left out. Like the entries' sizes, two orders of one account may each count the same position.
- Completeness: the trigger part is complete only with the listener's whole table, an untruncated band (50,000 orders) and a trusted book; the liquidations do not depend on it.
```
GET /extForcedFlow?maxDistanceBps=100[&coin=BTC][&sources=trigger,liquidation][&excludeCoins=ETH,SOL][&minNotionalUsd=10000]
```
- Each query, and each new subscription, is answered with each coin's frame from a computation started no earlier than it (a frame's age does not prove its inputs unchanged): a computation under way is not voided - it answers the queries that came before it started, then one more runs for the later ones - and answers within 10 s. 400 for parameters a subscription would refuse (unknown ones included), 503 while the forced flow is unavailable.
- Frames in `orderbook_accounts_frames_total{kind="forcedFlow"|"forcedFlowWithdrawn"}`. The reads under the listener lock in `orderbook_forced_flow_lock_wait_seconds{read}` and `orderbook_forced_flow_lock_hold_seconds{read}` (`band`: a coin's trigger orders in a band; `walk`: one side's levels), the entries walked in `orderbook_forced_flow_walked_entries_total`, and the work on the account owner task per step in `orderbook_forced_flow_owner_seconds{step}` (`query`, `read`, `tick`, `local`).

### Local marks (extMarkPrices, extMarkInputs, extMarkReferenceCaches)

With `--local-marks` (and `--accounts`) the server follows the venues whose prices make Hyperliquid's mark: per main-dex coin its spot markets (the oracle) and perp markets (the external price), as listed in `server/data/mark_sources.json`. That file is generated by `scripts/discover_mark_sources.py` from the venues' public listings, each listing checked against HL's own price (oracle for spot, mark for perps) to catch another asset under the same ticker and lot multipliers (`kPEPE` is 1,000 PEPE; Binance's `1000PEPEUSDT` already is), and embedded at build time: a newly listed coin or a delisted market needs a regeneration and a rebuild. 176 coins as of 2026-10-21 (bm, 10 minutes: every coin had a quoting spot and perp venue at every 30 s sample, ~5,100 frames/s over the 12 feeds, ~3% of a core, ~26 MB); HYPE and PURR (USDC-quoted perps) and HIP-3 are not in it.

Every 50 ms the local marks' owner reads the node price state the account view's owner publishes after every applied block (the replay of the node marks above), the HL book (one listener lock per tick, copying the best prices) and every feed, and gives each coin two legs (design 000454):
- `node`: the replay's state of the coin at the applied height - what the next `SetGlobalAction` takes: the stored oracle median(submitted, Spot cache, L with the basis undone) and the mark median(submitted, Mark cache, L).
  - `markPx`, set at `markTimeNs` (block time, unix ns) by `markUpdate`: `setGlobal` (a validator's `SetGlobalAction`), `automaticRefresh` (no `SetGlobalAction` for the stale window: the last submission again) or `checkpoint` (the seed; a checkpoint does not record which, until the coin's next update).
  - `storedOraclePx`, `lastTradePx`; `submittedOraclePx` / `submittedMarkPx`: the last `SetGlobalAction`'s prices of the coin, each median's first candidate (`null` before one).
  - `spotCachePx` / `markCachePx`: the reference caches' medians (`null` before one has a median); `spotCacheNextRefreshMs` / `markCacheNextRefreshMs`: the first block whose time reaches it (a newer 3 s bucket) takes the median of the members under 10 s old, and keeps the old median when none is. The members are at `GET /extMarkReferenceCaches` (they change at most every bucket and run to kilobytes a coin, so the frames leave them out).
  - `basisEma` (msoe) and `localPriceEma` (lmpe) as stored: `numerator`, `denominatorS`, `decayParameter`, `samples`, `lastSampleTimeNs`. Each price update `dt` s after the last sample: numerator' = sample × dt + numerator × (1 - 1 / decayParameter), denominatorS' = denominatorS × (1 - 1 / decayParameter) + dt (the basis sample: mid / the new stored oracle - 1); a gap longer than `decayParameter` resets it; the average numerator / denominatorS applies from `decayParameter` samples on. The local leg's `basisRatio` is a preview at now, not this state.
- `local`, by the node's own rules (the replay's implementation) on that state: the oracle is the weighted median of the midpoints of the spot venues quoting now (Binance 3, OKX 2, Bybit 2, every other venue 1; one venue is enough), rounded half to even into the coin's price units; A = oracle × (1 + basis), the basis EMA previewed at now with the book's midpoint against the stored oracle, as a validator's reference action previews it; L = median(bid, ask, last trade); C = the weighted median of the perp venues quoting now. `referenceMarkPx` = median(A, L, C), the Mark member a validator submitting these venue prices now would leave. `predictedMarkPx` = median(reference, Mark cache, L), with no Hip3 step limit on this Main-dex path: a `SetGlobalAction` now whose validators submitted the reference. It is a conditional estimate, not the next mark: the cache refreshes per 3 s bucket, later submissions and books are unknown. Nothing is written back. Small coins may differ from the validators: their venue sets are not the validators'.
- Withheld legs carry `status` (`unavailable`, or `stale` when a progress clock is behind), `reasonCode` and `reason`. For every coin at once: the node state unavailable (`node`, with the account view's or the node marks' reason) or its block more than 5 s behind the local clock (`nodeProgress`; the applied block runs 0.7 s behind at the median and 2.4–2.9 s at each minute's P99, bm 10-21: at 3 s every coin flickered stale ten times in 14 minutes) withholds both legs; the book with no snapshot yet or withheld after a data loss (`hlBook`) or its block time (both book streams applied) more than 5 s behind (`bookProgress`), the feeds stopped or the account owner gone (`stopped`, until restart) withhold the local leg. A quiet coin is not withheld for its own silence. Per coin: `nodeCoin` (the replay does not claim the coin: not replayed until the next seed, or not listed), `markCache` (no cache median yet), `hlBook` (an empty side), `oracleCoverage` / `externalCoverage` (no venue of the kind quotes it now), `calculation` (a rule's price out of range).

Subscription:
```json
{"method":"subscribe","subscription":{"type":"extMarkPrices","coin":"BTC"}}
```
A frame of the coin every 50 ms (schema 2; the trading system's Python `hl_mark_prices` service wrote 1), the latest at once on subscribing (abridged):
```json
{"channel":"extMarkPrices","data":{"schemaVersion":2,"coin":"BTC","intervalMs":50,"publishedTsMs":1792558254050,
 "node":{"status":"ready","markPx":"82810","markTimeNs":1792558253412345678,"markUpdate":"setGlobal","storedOraclePx":"82796","submittedOraclePx":"82797","submittedMarkPx":"82811",
  "spotCachePx":"82797","spotCacheNextRefreshMs":1792558254000,"markCachePx":"82809","markCacheNextRefreshMs":1792558254000,
  "basisEma":{"numerator":0.0268,"denominatorS":371.5,"decayParameter":120.0,"samples":2244588,"lastSampleTimeNs":1792558253412345678},
  "localPriceEma":{"numerator":7696308.0,"denominatorS":93.5,"decayParameter":30.0,"samples":269229,"lastSampleTimeNs":1792558253412345678},
  "lastTradePx":"82810","seedHeight":1191600000,"height":1191612345,"blockTimeMs":1792558253600},
 "local":{"status":"ready","referenceMarkPx":"82811","predictedMarkPx":"82810","basisAdjustedOraclePx":"82812","localPx":"82810","externalPx":"82811","oraclePx":"82806","basisRatio":0.000072,"basisSamples":2244588,"bidPx":"82810","askPx":"82811","nodeHeight":1191612345,"nodeBlockTimeMs":1792558253600,"bookTimeMs":1792558253700,"bookHeight":1191612346},
 "oracleCoverage":{"usedVenues":["binance","okx","bybit","kraken","kucoin","gate","mexc"],"missingVenues":[],"usedWeight":11,"configuredWeight":11},
 "externalCoverage":{"usedVenues":["binance","okx","bybit","gate","mexc"],"missingVenues":[],"usedWeight":9,"configuredWeight":9}}}
```
A withheld leg reads `{"status":"stale","reasonCode":"bookProgress","reason":"the HL book's block 1191612346 is 3120 ms behind the local clock"}`. A coin outside the sources is refused; without `--local-marks` the subscription is refused. The owner task ending (an internal error) withdraws every subscription with an error frame (`extMarkPrices BTC withdrawn: the local marks owner task ended`); the order book keeps serving.
```
GET /extMarkPrices            # every coin, a list of the frames' data
GET /extMarkPrices?coin=BTC   # one coin (404 outside the sources; 503 once the owner task ended)
```

The reference caches with their members, of the same tick and node state as the node leg and withheld as it is:
```
GET /extMarkReferenceCaches            # every coin
GET /extMarkReferenceCaches?coin=BTC   # one coin (404 outside the sources; 503 once the owner task ended)
```
```json
[{"schemaVersion":2,"coin":"BTC","publishedTsMs":1792558254050,"node":{"status":"ready",
  "spotCache":{"medianPx":"82797","nextRefreshMs":1792558254000,"members":[{"validator":"0x...","px":"82797","blockTimeNs":1792558252012345678}]},
  "markCache":{"medianPx":"82809","nextRefreshMs":1792558254000,"members":[{"validator":"0x...","px":"82809","blockTimeNs":1792558252012345678}]},
  "seedHeight":1191600000,"height":1191612345,"blockTimeMs":1792558253600}}]
```
A Spot member is the validator's submitted oracle, a Mark member median(A, L, C) at its reference action; each validator's latest, by validator.

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
- Binance perp subscriptions use at most **50 markets per connection** (BTC and ETH each have a dedicated connection; the embedded 173-market list currently uses 7 connections); the other feeds retain their existing connections. This is a measured operating choice, not a venue limit: on BM 2026-10-21, against the same remote IP, a 173-market connection ended after 36.2 s while four partitions all lasted the 182 s observation. A later 10-minute Rust trial found repeated EOFs in the first 50-market partition while the other three remained connected; a 120 s per-symbol count put 66% of the first partition's traffic in BTC/ETH, motivating their dedicated connections. These observations do not identify the hidden backend or prove a per-connection throughput limit. Negotiated compression alone did not prevent later EOFs; the cause of the disconnects remains unproven. Each partition owns its retry/backoff, connection epoch and quotes. A loss or subscription/parse refusal affects only that partition; reconnecting, an ack or a pong never revives its old quotes. Partitions are joined in source market order before pricing, so Binance retains weight 3 per coin, never 3 per connection.
- `feeds[].connections` lists each partition (`shard`, `markets`, `connection`, `frames`, `quoting`). The existing aggregate `connection` remains unchanged for single-connection feeds; a partitioned feed is `connected` only when all connections are up (`sinceMs` is the latest connection start), otherwise `degraded` with `connected` and `total` counts. Each market's `status` remains authoritative for using its quote. A connected partition may still be waiting for a market to quote.
- Each connection keeps its protocol's health: its subscription acknowledgements within 10 s, its heartbeat answered within 10 s (`spot.ping` / `futures.ping` on Gate's two endpoints), some frame every 30 s. A lost connection withdraws only its partition's quotes and reconnects after a backoff (0.5 s doubling to 10 s, reset after 30 s connected). A refused subscription, a known frame that does not parse or a crossed book stops that partition until restart (its `connection.state: stopped` with the reason); a frame of an unknown type is warned with its text and not taken. The order book and the accounts never wait on a venue. A feed task failing otherwise (a panic: an internal error) stops every feed and withdraws every quote until restart (`stopped` gives the reason), as the account view's owner ending withdraws the view; the order book keeps serving.
- Gate `spot.system` / `futures.system` updates with `result.type: upgrade` request a planned reconnect ([spot](https://www.gate.com/docs/developers/apiv4/ws/en/#service-upgrade-notification), [perps](https://www.gate.com/docs/developers/futures/ws/en/#service-upgrade-notification)). They log at INFO, withdraw only that partition's quotes, and enter the existing bounded backoff immediately; reconnect/acks do not revive old quotes. Disconnect counters include these rotations. Other notification types remain unknown diagnostics; malformed upgrade payloads and explicit errors remain contract errors. The perpetual fixture was captured on BM at 2026-10-22 07:03:05 UTC; its old connection closed ten seconds later.
- `local_marks_probe --seconds 600 --every-secs 30 --out DIR` runs the feeds alone beside a running server, without restarting it: one JSON line per interval, then `summary.json` and the whole view in `mark_inputs.json`.
- Metrics: `orderbook_local_marks_feed_frames_total{feed}`, `orderbook_local_marks_feed_disconnects_total{feed}`, `orderbook_local_marks_feed_unknown_frames_total{feed}`, `orderbook_local_marks_feed_stopped{feed}` (any stopped partition), plus `orderbook_local_marks_connection_disconnects_total{feed,shard}`. Existing feed counters sum their partitions.

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
| | `ws_broadcast_skipped_messages_total{lane}` | Messages overwritten in `market`, `bbo` or `user_fills` rings, summed over lagging receivers |
| | `ws_delivery_wait_seconds{stage}` | `outbound_queue`: waits on a full send queue; `socket_write`: each socket write, including failures/timeouts |
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
| | `channel_drops_total` | Receiver lag occurrences (not message or trade count) |
| | `broadcast_channel_lag` | Messages skipped in the most recent receiver lag occurrence |
| | `orderbook_desyncs_total{reason}` | Times the book was marked out-of-sync (each triggers an automatic background snapshot re-fetch). Alert on a sustained rate; occasional self-heals are benign |

### WebSocket broadcast continuity

Account and node-mark reconstruction, checkpoint reconciliation and liquidation-candidate computation run
on one dedicated `accounts-owner` thread with a private current-thread executor. File readers still feed
its existing bounded channels; the owner still applies blocks serially. The order-book listener, WebSocket
sessions and venue feeds stay on the network executor. Owner panic/abort still closes its publishers and
queries through the same supervised Tokio task; its private executor exits after the task is dropped.
Cancellation waits for an in-progress synchronous step, as before.

BM incidents on 2026-10-22 paused BBO reception 1.54–1.86 s around checkpoint installation, despite socket
writes below 4 ms. A two-network-worker reproduction with the production 16,384-message ring demonstrated
Tokio's unstealable LIFO wake slot: synchronous work could hold a woken receiver even while another worker
was free. Three controls skipped 7,617 of 24,001 messages; all three isolated-thread runs received every
message. This removes the reproduced scheduler coupling, not a guarantee against slow clients or all
future overload. Live acceptance must check BBO gaps across checkpoint installs and candidate refreshes.
`orderbook_accounts_checkpoint_install_seconds{phase="marks"|"cash"}` and one INFO timing per phase record
owner-side install work separately from the checkpoint poller's parsing time. Existing candidate-refresh
and socket/queue wait metrics remain independent.

BBO has a dedicated 16,384-message ring, joined only by BBO subscribers. Public trades/L4/L2 and
private `userFills` use separate rings. This prevents per-book-event BBO traffic (measured at 13.4k
messages/s on BM) from evicting trades or making account-only clients drain BBO messages.

A `market` gap forces a fresh L2 evaluation. Event subscriptions (`trades`, `l4Book`, `bookDiffs`,
`orderUpdates`, `oracle`) are withdrawn: the server sends an `unsubscribe` subscription response for
each and then an `error` explaining the gap. A `bbo` gap withdraws BBO subscriptions and clears their
heartbeat cache. Clients must resubscribe and reconcile any missing events; a new trade cannot repair
an earlier missing trade. Other subscriptions on the same socket continue. A private-fill gap still
closes the connection under its existing recovery contract.

Gap WARN logs include the process-local connection ID, socket peer, lane, skipped/pending message
counts, receive interval, outbound queue depth and send-wait maxima. Completed session branches taking
at least 10ms retain one `last_slow_stage` sample with duration and age; elapsed time includes scheduling
and awaited work, so it is not itself a root-cause verdict. Detail logs are limited to one per connection
per 30s; gap counters always increment. Counts are internal messages/batches, not lost trades. Historical
versions logged market details at DEBUG; INFO-only captures cannot identify the affected old connection.

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

This fork runs as `orderbook-server.service` on its node host, operated from the hyperliquid-trade repository
(`operations/orderbook_server/ob_ops.py`): that script holds the only unit definition and command line, builds
`origin/main` into `~/ob_releases/<commit>/`, goes live by moving `~/ob_releases/current` and restarting the unit, and
keeps `~/ob-snapshotter` a copy of the official node build. Logs: `journalctl -u orderbook-server -f`.

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

Both use `yawc` with `permessage-deflate`. This fork uses separate bounded public and private-fill broadcast rings (16,384 messages each), and documents compression level tradeoffs.

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


HIP-3 条件单的成交 mark 另有一条实际时序证据（BM `1193759676`，`io:DRAM`）：
同一个普通批量下单动作的两张订单先后产生 `open`，中间夹着旧条件单的完整
`triggered` → 成交 → `filled`。两个 `open` 的 oid 必须由同一成功动作的回执唯一确认，
用户及本块时间一致，订单不能本身是条件单；重复、缺尾、跨动作和中途另一张 `open`
都会保留价格区间。完整夹在该动作内的条件单使用该动作执行时的 mark，不能只靠
“本次价格更新跨过触发价”判断。此例两次 mark 都已满足止损条件，但 stop 实际在后一个
bundle 的 ALO 批次内执行，因而使用前一 bundle 已写入的 `46.119`，而非块初 `46.112`。
该归属只细化逐仓现金回放的取价，不修改节点最终 mark 或主站 EMA 重建。
金样本 `server/src/accounts/marks/fill_prices/hip3/interleaved_golden.json` 保留真实动作、回执、
连续状态和成交双腿；`1193750000→1193760000` 的 83 条成交/资金费事件逐仓现金回归由
`-3702.801675` 修正为 checkpoint 的 `-3701.998545` USDC。现金测试显式提供充足全仓资金，
验证的是取价和该逐仓账本，不声称已重放此账户其他币种的全部现金及可用额度。


`send` 的 `sourceDex` 只确定本金来源，不能确定 USDC 手续费来源。
BM `1193760000→1193770000`，`0xd6db…0e70` 的两笔主站永续到主站永续转账各收 1 USDC，
实际从现货扣费；七笔转出和一笔转入精确解释永续余额，现货另减少 2 USDC，区间没有成交。
旧实现从永续再次扣费，导致全仓少计 2 USDC。`send_fee_source_golden.json` 保留八条账本事件
及两端余额。现在非统一账户凡遇到非零 USDC 转账费，按已有 `spotFeeSource` 明示现金回放边界；
无费用转账、收款方本金和统一账户合池回放仍支持。此修复撤回错误的精确分池结论，
尚未声称支持该扣费路径的精确分池重建；后者需要执行时现货余额及客户端实际扣费路由。

HIP-3 已满足触发条件、且位于状态流尾部的条件成交，还可用节点成交 hash 的 L1 执行定位证据
（`io:DRAM 1193762464`、`io:SNDK 1193771663`）：字节 10 起是三个长度前缀大端整数，
依次为块高、按 bundle 内优先级排序后的全块动作序号、嵌套定位值。本路径只接受第三值为 0，
且前两值精确对应本块唯一成功的该 dex `setOracle`；不是按原始请求列表位置猜测。
这是实际 wire 观测（452 笔普通 taker 的 oid 与回执全匹配，两个目标 hash 的官方 txDetails
独立确认同一 oracle 动作），不是官方文档保证，也不是 hash 认证。未知格式不停止 mark 重建。
仍要求唯一 taker、已记录的 triggered 状态、同块同时间唯一 Normal mark 写入；两腿分别核验，
缺少可用证据、或状态夹区间位于 oracle 写价之前而与 hash 定位矛盾时，保留价格区间及 `cashEstimated`。
状态夹区间在同一次 oracle 写价之后时，两条证据指向相同新价格，即使动作位置不同也可确定成交 mark；
这只确定价格，不宣称确定了触发动作身份。不扩展到 EVM、TWAP、系统定位或多次写价。
两段 121/98 事件的完整逐仓现金测试在明确提供充足跨仓资金的前提下，消除了原有 -5/-26 微 USDC 差异；
其他市场资金容量不属于该测试证明范围。ANTH 九笔的 665 微 USDC 分池差异仍未解决，不能按舍入抹平。
金样本见 `server/src/accounts/marks/fill_prices/hip3/oracle_hash_goldens.json`；完整原始区块放专用研究数据目录。
`xyz:NBIS 1194139926` 的完整原始块验证了上述后一种情形：唯一 Normal 写价从 164.6 到 164.41，
成交 hash 定位 oracle，完整触发状态串夹在后续 bundle 的两个普通挂单之间。
`oracle_bracket_golden.json` 保留动作、回执、连续状态及五户在 `1194130000→1194140000` 的逐仓窗口；
原偏差 -13,680 / -139,320 / -619,110 / -179,527 / -72,160 微 USDC 在回归中均消除。
测试显式提供充足跨仓资金，验证逐仓划转和后续减仓，不代替这些账户全部市场的可用资金重建。
同一修复在另外五个原始块（`1194109481`、`1194110981`、`1194111419`、`1194111913`、`1194132867`）
也确定了 oracle 写入后的价格。`oracle_bracket_followup_goldens.json` 保留 `xyz:SNDK`、`io:SNDK`、
`xyz:ORCL` 四户逐仓窗口，原分池偏差 -3,857 / -1,809,281 / -18,630 / -314,172 微 USDC 均消除。
窗口包含真实资金费和两笔成功逐仓撤款（16.96、5.24 USDC）；按块顺序调用现有现金回放，
这些正常现金事件不能从样本中省略。完整原始块另验五处 oracle 序号与状态夹区间；跨仓资金仍采用上述充足假设。

Unified isolated funding also needs the physical executing-dex balance: an aggregate pool containing spot or
other dex cash does not prove that amount was prefunded before a maker fill. Native sweep `0x3c92a80` in
archived client `6911b792` retains 101% of open-order reserves; the maker path can pass its size check without
running Unified prefunding. Until that routing is replayed, an open/increase/flip without the existing
single-dex proof retains its numerical estimate and sets `cashEstimated`; reductions alone do not add this
uncertainty, and later exact operations do not clear it. `replay/unified_routing_golden.json` records ANTH's
nine fills at 1193540000–1193550000: the 0.000665 USDC split stays estimated, while any total-cash difference
still fails reconciliation. The native one-order scenario matches the endpoint but is not a complete
historical routing proof or a production cash-amount fix.

The dynamic-capacity regression `replay/cross_capacity_windows.json` covers two complete EWY account windows
(517 fills, 38 funding deltas). Capacity clips transfers by 10,044 and 485 micro USDC; all isolated cash and aggregate
cross/spot cash reconcile. Two recorded order/oracle blocks validate collateral timing. At most two shared maps
per updated dex avoid portfolio clones per fill. Starting spot funds and Unified maker prefunding retain their
independent routing boundary; the fix does not implement a general Unified sweep scheduler.

Closed-asset leverage changes preserve an existing complete cross portfolio and update its zero-size cross
setting. Active-position changes and unknown portfolios still invalidate capacity; a same-block fill cannot
assume the new cross setting's order. The recorded BTC window 1196390000–1196400000 includes 25 fills,
two leverage-setting changes, an incoming transfer and funding. Retaining capacity caps the final isolated fill and
closes its 0.057205 USDC split (`replay/btc_closed_leverage_capacity_golden.json`). The independent ZHIPU
window 1196380000–1196390000 has two fills and 13 unchanged cross holdings across two dexes: the first
fill's requested 0.306225 USDC margin is capped at 0.305154, closing its 0.001071 USDC split
(`replay/zhipu_closed_leverage_capacity_golden.json`). Both complete account endpoints reconcile. Initial
spot funds and Unified physical routing retain their estimated boundary; this is offline regression evidence,
not deployment acceptance.


Unified isolated order-reserve routing (ANTH)
-------------------------------------------

The cash replay now keeps an optional physical executing-dex projection alongside the authoritative aggregate
USDC pool. It reads the checkpoint's full `open_order_tracker` aggregates and spot USDC hold, then retains
receipt-backed ordinary ALO order and cancellation inputs in the existing bounded account event log. Order
observations do not mark an account as manually changing margin or trigger liquidation-candidate refreshes.

The supported numerical path is deliberately scoped to Unified, one active USDC dex, one isolated-only market,
a single margin tier, no lending and no frozen spot USDC or another dex's order reserves. Ordinary ALO wrappers
prefund, update order reservation, then retain the client's `f64(reserve) * 1.01` truncated to micro USDC. Maker
admission distinguishes direct execution from prefunding; opening/increasing/flipping consumes that physical
capacity rather than assuming all spot cash was already supplied. Total cash is unchanged by these transfers.

Missing tracker sections, mixed cash/order/fill boundaries and untrusted mark history preserve the existing
estimate. Cancel-only scheduling does not invent a sweep, and a later block does not automatically sweep or
replenish after a fill. An ALO with a proven unchanged action mark can establish physical funding again while
its tracker remains covered. A late first/new-eligibility checkpoint whose intervening orders were not observed
starts order observation, but withholds that interval's tracker; an already-covered next anchor can activate it.

This numerical path remains `cashEstimated`: its maker rate comes from the observed fee/notional, not recovered
fee-tier state, and the general deferred sweep scheduler is not implemented. `replay/routing_native_golden.json`
contains 50 native reserve and 90 native maker-size cases against archived binary `6911b792`. Under the explicit
last-ALO tracker condition, the ANTH nine-fill regression ends at 7.210095 USDC rather than 7.210760, removing
the 665-micro allocation residual while conserving total cash at every fill. The older raw-history regression
remains estimated because the complete historical tracker was not archived; no fixed 665-micro adjustment exists.


Unified other-dex order-reserve capacity (GOLD)
----------------------------------------------

A receipt-backed single ordinary IOC may now use a tighter aggregate capacity bound when another USDC dex's
cash is retained for resting orders. The replay excludes `min(dex cash, trunc(f64(aggregate order reserve) * 1.01))`
for those other dexes. It verifies the checkpoint tracker against every per-asset reservation and requires zero
positions on its reserved assets, so intervening mark changes cannot change that requirement. Other held positions
must be isolated; lending, frozen spot USDC, cross exposure and ambiguous execution-market selection are excluded.

The bound is retained only with continuous account-input coverage. A foreign-market order/fill, funding, cash or
leverage action, mixed or missing IOC evidence, or untrusted mark history discards it until reseeding. The IOC
needs one original request, one placed receipt, open/terminal IOC statuses and only that order's fills; a batch
with one successful order and another rejected order is not equivalent. Later blocks need a new IOC boundary.
This is an available-collateral bound and stays `cashEstimated`; it is not a general Unified sweep scheduler.

`replay/gold_other_dex_golden.json` preserves the recorded 1194470000→1194480000 endpoints, tracker, request,
receipt, statuses and six fills at block 1194477717, plus archived-client `6911b792` intermediate results.
Main-dex cash 456.515934 USDC is entirely retained by order reserve 451.995975 USDC. Excluding it leaves
171.163023 USDC from spot. All six steps match the native supplied-capacity arithmetic, ending at isolated cash
11479.565555 USDC rather than 11494.025270, correcting the 14.459715 USDC split while conserving total cash.
The golden regression is offline; live rollout and generic routing confidence are separate acceptance steps.


Unified checkpoint maker collateral (SOFTBANK)
---------------------------------------------

Unified maker funding at an untouched checkpoint can use the executing dex's own cash and cross collateral.
SOFTBANK H1194820315 has 3.668831 USDC available on xyz despite 4659.033447 USDC aggregate capacity;
native admission accepts the complete 0.403 maker fill without replenishment, and that local cap explains
the 0.001155 USDC split. `replay/softbank_maker_window.json` verifies all cash across the 20-fill window.
This path requires one eligible isolated-only tracked target, complete execution collateral prices, no
intervening routing boundary, and the whole remaining maker side. It expires after the first fill and remains
`cashEstimated`; missing prices, a changed tracker or admission requiring replenishment keep the prior estimate.

现金回放补充（2026-10-24）：已确认的延迟 `SystemSendAssetAction`（USDC 从 spot 转主 dex）与成功账本流水按账户、nonce、金额和去向唯一匹配，且双方同块成交全部能归属普通下单时，先入账再回放成交。该顺序仍在 funding 之后，并写入同一重放日志；缺时序、重复证据、同块其他现金操作或无法归属成交时保持原估算。EUR 1194920000→1194930000 的四笔成交据此消除 849.373668 USDC 分配误差；统一账户物理划转边界仍保留 `cashEstimated`。NEAR 两个完整反手窗口也补为金样本，核验负现金与现货尾数合并后的微 USDC 取整（由 e8269ea 覆盖）。这些是离线验证，不替代部署后的 checkpoint 验收。
