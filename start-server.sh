ionice -c2 -n7 cargo run --release --manifest-path ~/order_book_server/Cargo.toml --bin orderbook_server -- \
    --address 0.0.0.0 \
    --port 8000 \
    --snapshot-mode direct \
    --hlnode-binary ~/ob-snapshotter \
    --data-dir ~/hl/data \
    --metrics-port 9090 \
    --log-level info
