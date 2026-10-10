#!/usr/bin/env bash
# Run orderbook_server in the foreground (the bm `ob` tmux session) and keep its
# log on disk: one file per start under ~/ob_logs, files of this tool older than
# 30 days removed at start. pipefail keeps the server's own exit status; a slow
# disk or terminal back-pressures the pipe like the plain terminal did before.
set -o pipefail
LOG_DIR="$HOME/ob_logs"
mkdir -p "$LOG_DIR"
find "$LOG_DIR" -maxdepth 1 -name 'orderbook_server_*.log' -mtime +30 -delete
LOG_FILE="$LOG_DIR/orderbook_server_$(date -u +%Y%m%dT%H%M%SZ).log"
echo "logging to $LOG_FILE"

ionice -c2 -n7 cargo run --release --manifest-path ~/order_book_server/Cargo.toml --bin orderbook_server -- \
    --address 0.0.0.0 \
    --port 8000 \
    --snapshot-mode direct \
    --hlnode-binary ~/ob-snapshotter \
    --data-dir ~/hl/data \
    --metrics-port 9090 \
    --log-level info \
    2>&1 | tee -a "$LOG_FILE"
