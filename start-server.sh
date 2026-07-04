#!/usr/bin/env bash
# order_book_server 裸机启动入口(nube 节点机, direct 快照模式)。
# 前提: 节点开着 --stream-with-block-info(*_streaming 目录)+ --write-hip3-oracle-updates。
# 额外参数透传, 如: ./start-server.sh --bbo-only true --log-level debug
set -euo pipefail
cd "$(dirname "$0")"
cargo build --release
exec target/release/orderbook_server \
    --address 0.0.0.0 \
    --port 8000 \
    --snapshot-mode direct \
    --hlnode-binary "$HOME/hl-node" \
    --data-dir "$HOME/hl/data" \
    --metrics-port 9090 \
    --log-level info \
    "$@"
