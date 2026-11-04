//! Prometheus metrics for orderbook server monitoring
//!
//! Provides comprehensive metrics for:
//! - WebSocket connections and subscriptions
//! - Event processing latency and throughput
//! - Orderbook health and state
//! - Errors and anomalies

use lazy_static::lazy_static;
use prometheus::{
    Counter, CounterVec, Gauge, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
    Opts, Registry,
};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    // ==================== CONNECTION METRICS ====================

    /// Current number of active WebSocket connections
    pub static ref WS_CONNECTIONS_ACTIVE: IntGauge = IntGauge::new(
        "ws_connections_active",
        "Number of active WebSocket connections"
    ).expect("metric can be created");

    /// Batches overwritten in each independent delivery ring, not per-user fill counts.
    pub static ref WS_BROADCAST_SKIPPED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("ws_broadcast_skipped_messages_total", "Broadcast messages overwritten per lane"), &["lane"]
    ).expect("metric can be created");

    /// Queue observations only occur when waiting for a full queue; socket observations include every write.
    pub static ref WS_DELIVERY_WAIT: HistogramVec = HistogramVec::new(
        HistogramOpts::new("ws_delivery_wait_seconds", "Time awaiting a queue slot or socket write")
            .buckets(vec![0.0001, 0.001, 0.01, 0.1, 0.5, 1.0, 5.0]), &["stage"]
    ).expect("metric can be created");

    /// Total WebSocket connections since startup
    pub static ref WS_CONNECTIONS_TOTAL: IntCounter = IntCounter::new(
        "ws_connections_total",
        "Total WebSocket connections since startup"
    ).expect("metric can be created");

    /// Active subscriptions by type (bbo, l2Book, l4Book, trades)
    pub static ref WS_SUBSCRIPTIONS_ACTIVE: IntGaugeVec = IntGaugeVec::new(
        Opts::new("ws_subscriptions_active", "Active subscriptions by type"),
        &["type"]
    ).expect("metric can be created");

    // ==================== LATENCY METRICS ====================

    /// BBO broadcast latency in seconds
    pub static ref BBO_BROADCAST_LATENCY: Histogram = Histogram::with_opts(
        HistogramOpts::new("bbo_broadcast_latency_seconds", "BBO broadcast latency")
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0])
    ).expect("metric can be created");

    /// L2 broadcast latency in seconds
    pub static ref L2_BROADCAST_LATENCY: Histogram = Histogram::with_opts(
        HistogramOpts::new("l2_broadcast_latency_seconds", "L2 broadcast latency")
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0])
    ).expect("metric can be created");

    /// Number of coins rebuilt per L2 broadcast (conflation batch size): coins
    /// changed since the previous publish; spikes toward the universe size
    /// indicate full-universe windows (lock-duration pressure).
    pub static ref L2_CONFLATION_BATCH_SIZE: Histogram = Histogram::with_opts(
        HistogramOpts::new("l2_conflation_batch_size", "Coins rebuilt per L2 broadcast")
            .buckets(vec![1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 150.0])
    ).expect("metric can be created");

    /// Event processing latency by type
    pub static ref EVENT_PROCESSING_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new("event_processing_latency_seconds", "Event processing latency")
            .buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1]),
        &["event_type"]
    ).expect("metric can be created");

    // ==================== THROUGHPUT METRICS ====================

    /// Events processed by type (orders, diffs, fills)
    pub static ref EVENTS_PROCESSED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("events_processed_total", "Total events processed"),
        &["type"]
    ).expect("metric can be created");

    /// Broadcasts sent by channel type
    pub static ref BROADCASTS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("broadcasts_total", "Total broadcasts sent"),
        &["channel"]
    ).expect("metric can be created");

    /// Fill legs dropped because no matching counterpart followed (trades pairing).
    /// ~0 in steady state; growth means the node stopped emitting the two legs
    /// of a match adjacently and trade pairing should be revisited.
    pub static ref TRADES_UNPAIRED_FILLS_TOTAL: IntCounter = IntCounter::new(
        "trades_unpaired_fills_total", "Fill legs dropped without a matching counterpart"
    ).expect("metric can be created");

    /// WebSocket messages sent
    pub static ref MESSAGES_SENT_TOTAL: IntCounter = IntCounter::new(
        "messages_sent_total",
        "Total WebSocket messages sent"
    ).expect("metric can be created");

    // ==================== HEALTH METRICS ====================

    /// Current orderbook block height
    pub static ref ORDERBOOK_HEIGHT: IntGauge = IntGauge::new(
        "orderbook_height",
        "Current orderbook block height"
    ).expect("metric can be created");

    /// Orderbook timestamp in milliseconds
    pub static ref ORDERBOOK_TIME_MS: IntGauge = IntGauge::new(
        "orderbook_time_ms",
        "Orderbook timestamp in milliseconds"
    ).expect("metric can be created");

    /// Pending order statuses in HFT cache
    pub static ref PENDING_ORDERS_CACHE: IntGauge = IntGauge::new(
        "pending_orders_cache_size",
        "Pending order statuses in HFT cache"
    ).expect("metric can be created");

    /// Pending order diffs in HFT cache
    pub static ref PENDING_DIFFS_CACHE: IntGauge = IntGauge::new(
        "pending_diffs_cache_size",
        "Pending order diffs in HFT cache"
    ).expect("metric can be created");

    /// Applied block height per book stream: pairing eviction and the l2/bbo
    /// frame time follow these, not the wall clock.
    pub static ref ORDERBOOK_STREAM_HEIGHT: IntGaugeVec = IntGaugeVec::new(
        Opts::new("orderbook_stream_height", "Applied block height per book stream"),
        &["stream"]
    ).expect("metric can be created");

    /// Applied statuses height minus applied diffs height (signed, blocks): how
    /// far the two independently read book streams have drifted in this process.
    pub static ref ORDERBOOK_STREAM_SKEW_BLOCKS: IntGauge = IntGauge::new(
        "orderbook_stream_skew_blocks",
        "Applied statuses height minus applied diffs height"
    ).expect("metric can be created");

    /// Pending statuses dropped once the diff stream passed their block without
    /// a New diff: orders that never rested (expected orphans, not data loss).
    pub static ref PENDING_ORPHANS_EVICTED_TOTAL: IntCounter = IntCounter::new(
        "orderbook_pending_orphans_evicted_total",
        "Pending statuses dropped unpaired after both streams passed their block, except orders settled in that block (filled on entry) and statuses replayed after a node restart"
    ).expect("metric can be created");

    /// 1 = no known unrecovered data loss; 0 = the book is known to miss
    /// orders and l2/bbo/L4 snapshots are withheld until a covering re-sync.
    pub static ref ORDERBOOK_BOOK_TRUSTED: IntGauge = IntGauge::new(
        "orderbook_book_trusted",
        "1 when the book has no known unrecovered data loss (derived outputs published)"
    ).expect("metric can be created");

    /// Seconds spent untrusted, added when each untrusted period ends.
    pub static ref ORDERBOOK_UNTRUSTED_SECONDS_TOTAL: Counter = Counter::new(
        "orderbook_untrusted_seconds_total",
        "Seconds the book was withheld for known data loss (completed periods)"
    ).expect("metric can be created");

    /// Broadcast channel lag (receivers behind)
    pub static ref CHANNEL_LAG: IntGauge = IntGauge::new(
        "broadcast_channel_lag",
        "Messages skipped in the most recent receiver lag occurrence"
    ).expect("metric can be created");

    // ==================== ERROR METRICS ====================

    /// Parse errors by type
    pub static ref PARSE_ERRORS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("parse_errors_total", "Total parse errors"),
        &["type"]
    ).expect("metric can be created");

    /// WebSocket send errors
    pub static ref WS_SEND_ERRORS_TOTAL: IntCounter = IntCounter::new(
        "ws_send_errors_total",
        "Total WebSocket send errors"
    ).expect("metric can be created");

    /// Times the order book was marked out-of-sync (by reason). Every increment
    /// triggers a background snapshot re-fetch that rebuilds the book, so a
    /// non-zero rate here means events were lost but the book self-healed.
    /// Oracle side-stream data loss (watcher discard / oversize batch). Kept
    /// separate from ORDERBOOK_DESYNCS_TOTAL on purpose: oracle loss never
    /// triggers a book re-sync.
    pub static ref ORACLE_DATA_LOSS_TOTAL: IntCounter = IntCounter::new(
        "obs_oracle_data_loss_total",
        "Oracle update lines lost (watcher discard or oversize batch drop)"
    ).unwrap();

    /// Every failed open/read, including retries coalesced in the log until file I/O succeeds.
    pub static ref NODE_FILE_IO_ERRORS_TOTAL: IntCounterVec = {
        #[expect(clippy::expect_used, reason = "the metric name and label descriptor are static valid Prometheus names")]
        let counter = IntCounterVec::new(
            Opts::new("orderbook_node_file_io_errors_total", "Node stream file I/O failures including repeated retries"),
            &["operation"]
        ).expect("metric can be created");
        counter
    };

    /// Seconds from the node writing a line (its `local_time`, same host clock) to
    /// a stage here: `read` = the watcher read it (newest line of each read),
    /// `apply` = the batch was applied to the book (once per book batch; of
    /// `mempool`, the pending book took the line),
    /// `publish` = an l2 publish was built and handed to the broadcast channel
    /// (newest applied book line; not socket delivery). Each stage samples a
    /// different set, so these are cumulative lags to compare side by side -
    /// quantile differences are not per-stage durations (review 000265).
    pub static ref NODE_LINE_LAG: HistogramVec = HistogramVec::new(
        HistogramOpts::new("node_line_lag_seconds", "Node line local_time to this server stage")
            .buckets(vec![
                0.0005, 0.001, 0.0015, 0.002, 0.003, 0.004, 0.005, 0.0075, 0.01, 0.015, 0.02, 0.03, 0.05, 0.1, 0.25, 1.0, 5.0,
            ]),
        &["stage", "source"]
    ).expect("metric can be created");

    /// L2 publishes by what made them due (design 000260): `quiet` = the book
    /// streams went quiet, `max_delay` = lines kept flowing, `recheck` = a
    /// deferred publish found a subscriber.
    pub static ref L2_FLUSH_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("l2_flush_total", "L2 publishes by trigger"),
        &["trigger"]
    ).expect("metric can be created");

    pub static ref ORDERBOOK_DESYNCS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_desyncs_total", "Times the order book was marked out-of-sync"),
        &["reason"]
    ).expect("metric can be created");

    /// Messages dropped due to channel lag
    pub static ref CHANNEL_DROPS_TOTAL: IntCounter = IntCounter::new(
        "channel_drops_total",
        "Receiver lag occurrences, not skipped message count"
    ).expect("metric can be created");

    /// New diffs whose insertBefore anchor was not resting at the level; the
    /// order was rested at the back of the level instead and a re-sync was
    /// scheduled, since queue priority may be wrong until then.
    /// Anchored adds whose insertBefore was honored (spliced at the anchor).
    /// Denominator for the fallback rate: fallback/(honored+fallback) tells
    /// whether a storm is a hole set or normal pairing-race noise.
    pub static ref INSERT_BEFORE_HONORED_TOTAL: IntCounter = IntCounter::new(
        "insert_before_honored_total",
        "Anchored adds whose insertBefore anchor was honored"
    ).expect("metric can be created");

    pub static ref INSERT_BEFORE_FALLBACK_TOTAL: IntCounter = IntCounter::new(
        "insert_before_fallback_total",
        "New diffs whose insertBefore anchor was missing; order rested at back of level"
    ).expect("metric can be created");

    /// Update/Remove diffs whose order is neither on the book nor waiting to
    /// pair in pending_new_diffs. Counted for observation, never re-synced
    /// automatically (a deliberate choice of design 000220; no cause is implied).
    pub static ref DIFF_TARGET_MISSING_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_diff_target_missing_total", "Update/Remove diffs whose order was neither booked nor pending"),
        &["diff"]
    ).expect("metric can be created");

    /// Live book batches at or below the installed snapshot height, skipped
    /// because the snapshot already reflects them (a lagging stream's backlog).
    pub static ref STALE_BATCHES_SKIPPED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_stale_batches_skipped_total", "Book batches at/below the installed snapshot height skipped"),
        &["stream"]
    ).expect("metric can be created");

    /// Book batches below their own source's high-water height and above the installed checkpoint.
    /// Their identity is unproven, so the book is withheld until a covering snapshot; not a trade count.
    pub static ref REGRESSING_BOOK_BATCHES_SKIPPED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_regressing_book_batches_skipped_total", "Regressing book batches skipped pending a covering snapshot"),
        &["stream"]
    ).expect("metric can be created");

    /// Target checkpoints that never appeared on the node's grid in time.
    pub static ref CHECKPOINT_MISSED_TOTAL: IntCounter = IntCounter::new(
        "orderbook_checkpoint_missed_total",
        "Periodic abci checkpoints that did not appear before the stream moved half an interval past them"
    ).expect("metric can be created");

    /// Height of the checkpoint the current book was installed from.
    pub static ref SNAPSHOT_CHECKPOINT_HEIGHT: IntGauge = IntGauge::new(
        "orderbook_snapshot_checkpoint_height",
        "Block height of the periodic abci checkpoint behind the installed book"
    ).expect("metric can be created");

    // ==================== FILE WATCHER METRICS ====================

    /// File events received per source (orders, diffs, fills)
    pub static ref FILE_EVENTS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("file_events_total", "File events received by source"),
        &["source"]
    ).expect("metric can be created");

    /// Lines parsed from files by source
    pub static ref FILE_LINES_PARSED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("file_lines_parsed_total", "Lines parsed from files by source"),
        &["source"]
    ).expect("metric can be created");

    // ==================== ORDERBOOK STATS ====================

    /// Total orders currently in the orderbook
    pub static ref ORDERBOOK_ORDERS_TOTAL: IntGauge = IntGauge::new(
        "orderbook_orders_total",
        "Total orders currently in orderbook"
    ).expect("metric can be created");

    /// Number of coins tracked in orderbook
    pub static ref ORDERBOOK_COINS_COUNT: IntGauge = IntGauge::new(
        "orderbook_coins_count",
        "Number of coins tracked in orderbook"
    ).expect("metric can be created");

    /// Untriggered trigger orders (stops / TP-SL pending their trigger price)
    pub static ref ORDERBOOK_UNTRIGGERED_TOTAL: IntGauge = IntGauge::new(
        "orderbook_untriggered_orders_total",
        "Untriggered trigger orders currently tracked (stops / TP-SL waiting for trigger price)"
    ).expect("metric can be created");

    /// Untriggered-order evictions by the status string that caused them.
    /// Today's node vocabulary is all terminal-for-the-oid; a novel status
    /// showing up here flags a possible wrong eviction after a node upgrade.
    pub static ref UNTRIGGERED_EVICTIONS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_untriggered_evictions_total", "Untriggered trigger order evictions by causing status"),
        &["status"]
    ).expect("metric can be created");

    /// Children a parent's `open` announced whose own `open` did not follow in the block: such a child, if it came
    /// otherwise, is held as a trigger order the node does not trigger yet.
    pub static ref UNTRIGGERED_UNMATCHED_CHILDREN_TOTAL: IntCounter = IntCounter::new(
        "orderbook_untriggered_unmatched_children_total",
        "Children announced by a parent's open whose own open did not follow in the block"
    ).expect("metric can be created");

    /// Coins whose trigger orders the table may miss, counted at each coin's first gap since the install, by the gap:
    /// one coin's gap leaves the other coins complete (bm 10-26: one EWZ child had kept every coin incomplete).
    pub static ref UNTRIGGERED_COIN_GAPS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_untriggered_coin_gaps_total", "Coins whose untriggered trigger orders became incomplete until the next install, by gap"),
        &["gap"]
    ).expect("metric can be created");

    /// Coins whose trigger orders the table may miss now (their own gap, kept until the next install).
    pub static ref ORDERBOOK_UNTRIGGERED_INCOMPLETE_COINS: IntGauge = IntGauge::new(
        "orderbook_untriggered_incomplete_coins",
        "Coins whose untriggered trigger orders the table may miss until the next install"
    ).expect("metric can be created");

    /// 1 when the install's trigger section was complete; 0 when every coin's trigger orders are a gap.
    pub static ref ORDERBOOK_UNTRIGGERED_INSTALL_COMPLETE: IntGauge = IntGauge::new(
        "orderbook_untriggered_install_complete",
        "1 when the last install's trigger section was complete (0: every coin's untriggered orders may miss)"
    ).expect("metric can be created");

    /// Live node triggers of orders the trigger table held otherwise (`TriggerMisread`): `waiting` for a parent, out
    /// of every band, or `notHeld` while the table was complete - forced flow orders the bands missed. Findings of the
    /// check, not distinct orders: a `triggered` line delivered again at the top height counts again (review 000470).
    pub static ref UNTRIGGERED_MISREAD_TRIGGERS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_untriggered_misread_triggers_total", "Node triggers of orders the trigger table held waiting or did not hold"),
        &["kind"]
    ).expect("metric can be created");

    /// Quote candidates before the shared BBO ring; one count per coin/book
    /// event, never per connection. Only `published` enters the ring.
    #[allow(clippy::expect_used, reason = "literal metric name and labels are valid at registration")]
    pub static ref BBO_PUBLICATION_QUOTES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("bbo_publication_quotes_total", "BBO quote candidates by publication outcome"),
        &["outcome"]
    ).expect("metric can be created");

    /// Changed BBO frames enqueued per coin, summed over subscribed connections.
    pub static ref BBO_CHANGES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("bbo_changes_total", "BBO changes by coin"),
        &["coin"]
    ).expect("metric can be created");

    // ==================== RESYNC & LOCK METRICS ====================

    /// Wall-clock duration of each re-sync phase. `fetch_dump` is the hl-node
    /// CLI dump, `parse` the snapshot JSON deserialize, `build` the replacement
    /// book construction, `chase` the off-lock replay loop, `commit` the final
    /// under-lock swap, and `total` the whole fetch-to-install cycle.
    pub static ref RESYNC_PHASE_DURATION: HistogramVec = HistogramVec::new(
        HistogramOpts::new("resync_phase_duration_seconds", "Duration of snapshot re-sync phases")
            .buckets(vec![0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]),
        &["phase"]
    ).expect("metric can be created");

    /// Time the ingest loop spends waiting to acquire the listener lock. The
    /// lock is shared with connection setup and l4Book snapshot builds, so
    /// sustained waits here mean ingest (and everything queued behind it) is
    /// being convoyed by other lock holders.
    pub static ref LISTENER_LOCK_WAIT: Histogram = Histogram::with_opts(
        HistogramOpts::new("listener_lock_wait_seconds", "Ingest-loop wait to acquire the listener lock")
            .buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0])
    ).expect("metric can be created");

    /// 1 once the first snapshot install completed and the book is serving.
    /// Read by /health so the endpoint never has to take the listener lock.
    pub static ref ORDERBOOK_READY: IntGauge = IntGauge::new(
        "orderbook_ready",
        "1 when the order book has installed a snapshot and is serving"
    ).expect("metric can be created");

    /// Unix epoch ms of the last event batch applied to the live book. /health
    /// derives a staleness signal from this so consumers can tell "connected
    /// but not receiving data" apart from a healthy quiet market.
    pub static ref LAST_EVENT_APPLIED_MS: IntGauge = IntGauge::new(
        "last_event_applied_ms",
        "Unix epoch ms of the last event batch applied to the live book"
    ).expect("metric can be created");

    /// 1 while a snapshot fetch + install cycle is in flight, so stalls can be
    /// correlated with re-syncs from the metrics alone.
    pub static ref ORDERBOOK_RESYNC_IN_FLIGHT: IntGauge = IntGauge::new(
        "orderbook_resync_in_flight",
        "1 while a snapshot fetch/install cycle is running"
    ).expect("metric can be created");

    // ==================== UPTIME & SYSTEM ====================

    /// Server uptime in seconds
    pub static ref UPTIME_SECONDS: IntCounter = IntCounter::new(
        "uptime_seconds",
        "Server uptime in seconds"
    ).expect("metric can be created");

    /// Server start timestamp (unix seconds)
    pub static ref SERVER_START_TIME: IntGauge = IntGauge::new(
        "server_start_time_seconds",
        "Server start timestamp (unix seconds)"
    ).expect("metric can be created");

    /// Broadcast channel receiver count
    pub static ref BROADCAST_RECEIVERS: IntGauge = IntGauge::new(
        "broadcast_receivers",
        "Number of broadcast channel receivers"
    ).expect("metric can be created");
}

// The account view's metrics, in a block of their own (one block this long exceeds the macro recursion limit).
lazy_static! {
    // ==================== ACCOUNTS (design 000419) ====================

    /// 1 while the account view may be served (anchor installed, every loss covered, inputs alive).
    pub static ref ACCOUNTS_TRUSTED: IntGauge = IntGauge::new(
        "orderbook_accounts_trusted",
        "1 while the account view is trusted"
    ).expect("metric can be created");

    /// The block-actions height while the account view is trusted; it stays at the last one while not, so the gap to
    /// the book's height says how long the view has been withheld (`ob_ops.py` health).
    pub static ref ACCOUNTS_TRUSTED_HEIGHT: IntGauge = IntGauge::new(
        "orderbook_accounts_trusted_height",
        "Block-actions height of the trusted account view (the last one while it is not)"
    ).expect("metric can be created");

    pub static ref ACCOUNTS_ANCHOR_HEIGHT: IntGauge = IntGauge::new(
        "orderbook_accounts_anchor_height",
        "Height of the installed account anchor checkpoint"
    ).expect("metric can be created");

    /// Wall time to parse the last installed checkpoint and compute its liquidation rows.
    pub static ref ACCOUNTS_ANCHOR_PARSE_SECONDS: Gauge = Gauge::new(
        "orderbook_accounts_anchor_parse_seconds",
        "Seconds to parse the last installed account anchor"
    ).expect("metric can be created");

    /// Synchronous checkpoint work on the dedicated account owner, excluding file parsing.
    pub static ref ACCOUNTS_CHECKPOINT_INSTALL_SECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new("orderbook_accounts_checkpoint_install_seconds", "Account owner checkpoint install wall time")
            .buckets(vec![0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0]),
        &["phase"]
    ).expect("metric can be created");

    /// Checkpoints that appeared while another was parsing and were never parsed (only the newest is).
    pub static ref ACCOUNTS_ANCHOR_SKIPPED_TOTAL: IntCounter = IntCounter::new(
        "orderbook_accounts_anchor_skipped_total",
        "Checkpoints skipped by the account poller"
    ).expect("metric can be created");

    pub static ref ACCOUNTS_CHECKPOINT_FAILURES_TOTAL: IntCounter = IntCounter::new(
        "orderbook_accounts_checkpoint_failures_total",
        "Checkpoints the account poller failed to parse"
    ).expect("metric can be created");

    pub static ref ACCOUNTS_OPEN_POSITIONS: IntGauge = IntGauge::new(
        "orderbook_accounts_open_positions",
        "Open positions in the account view"
    ).expect("metric can be created");

    /// Fills of coins outside the anchor universe waiting for the next anchor.
    pub static ref ACCOUNTS_HELD_FILLS: IntGauge = IntGauge::new(
        "orderbook_accounts_held_fills",
        "Fills held outside the account anchor universe"
    ).expect("metric can be created");

    pub static ref ACCOUNTS_STALE_FILL_BATCHES_SKIPPED_TOTAL: IntCounter = IntCounter::new(
        "orderbook_accounts_stale_fill_batches_skipped_total",
        "Fill batches at or below the installed account anchor"
    ).expect("metric can be created");

    /// Diagnostic, not a completeness proof: anchor sizes that contradict the fill projection.
    pub static ref ACCOUNTS_SILENT_POSITION_CHANGES_TOTAL: IntCounter = IntCounter::new(
        "orderbook_accounts_silent_position_changes_total",
        "Positions an anchor changed without an observed fill"
    ).expect("metric can be created");

    /// Highest block read per account stream.
    pub static ref ACCOUNTS_STREAM_HEIGHT: IntGaugeVec = IntGaugeVec::new(
        Opts::new("orderbook_accounts_stream_height", "Highest block read by the account streams"),
        &["stream"]
    ).expect("metric can be created");

    pub static ref ACCOUNTS_DATA_LOSS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_accounts_data_loss_total", "Input/replay validity observations, including startup positioning (legacy metric name)"),
        &["stream", "reason"]
    ).expect("metric can be created");

    /// Every diagnostic observation, including those coalesced instead of written to the log.
    pub static ref ACCOUNTS_DIAGNOSTIC_EVENTS_TOTAL: IntCounterVec = {
        #[expect(clippy::expect_used, reason = "the metric name and label descriptor are static valid Prometheus names")]
        let counter = IntCounterVec::new(
            Opts::new("orderbook_accounts_diagnostic_events_total", "Diagnostic observations by category, cause and sampling result"),
            &["category", "cause", "output"]
        ).expect("metric can be created");
        counter
    };

    /// First-hand evidence of rare events logged as `ACCOUNTS_EVIDENCE` lines, by kind.
    pub static ref ACCOUNTS_EVIDENCE_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_accounts_evidence_total", "Rare-event evidence lines logged by the account view"),
        &["kind"]
    ).expect("metric can be created");

    /// extCoinPositions frames broadcast by kind (snapshot / changes / withdrawn).
    pub static ref ACCOUNTS_FRAMES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_accounts_frames_total", "extCoinPositions frames broadcast"),
        &["kind"]
    ).expect("metric can be created");

    /// The cash replay of each anchor interval checked against the next anchor (design 000433 r2 3.6): replayed
    /// accounts by pool mode (`perDex`, `unified`) and result (`exact`, `mismatch`; `exactMoved` counts the exact ones
    /// whose cash moved in the interval, a subset of `exact`).
    pub static ref ACCOUNTS_REPLAY_CHECKED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_accounts_replay_checked_total", "Replayed accounts compared with the next anchor"),
        &["mode", "result"]
    ).expect("metric can be created");

    /// Replayed rows at each anchor against the anchor's own rows (design 000437), by result (`equal`, `differ`,
    /// `classDiffers`).
    pub static ref ACCOUNTS_REPLAY_ROWS_CHECKED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_accounts_replay_rows_checked_total", "Replayed rows compared with the anchor's rows"),
        &["result"]
    ).expect("metric can be created");

    /// Accounts whose replay stopped in a checked interval (or at its anchor), by reason.
    pub static ref ACCOUNTS_REPLAY_STOPPED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_accounts_replay_stopped_total", "Accounts whose cash replay stopped in a checked interval"),
        &["reason"]
    ).expect("metric can be created");

    /// Anchor intervals whose replay was not checked, by reason.
    pub static ref ACCOUNTS_REPLAY_CHECKS_SKIPPED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_accounts_replay_checks_skipped_total", "Anchor intervals whose cash replay was not checked"),
        &["reason"]
    ).expect("metric can be created");

    /// Inputs already held for their pending block (design 000433 r3, 3.4), by stream.
    pub static ref ACCOUNTS_DUPLICATE_INPUTS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_accounts_duplicate_inputs_total", "Account view inputs already held for their pending block"),
        &["stream"]
    ).expect("metric can be created");

    /// The highest complete block the account view applied; blocks dropped as a loss (the start, a gap, an overflow)
    /// do not move it (review 000434 r1 P3).
    pub static ref ACCOUNTS_APPLIED_HEIGHT: IntGauge = IntGauge::new(
        "orderbook_accounts_applied_height",
        "Highest complete block the account view applied"
    ).expect("metric can be created");

    pub static ref ACCOUNTS_PENDING_BLOCKS: IntGauge = IntGauge::new(
        "orderbook_accounts_pending_blocks",
        "Blocks the account view holds until they are complete"
    ).expect("metric can be created");

    pub static ref ACCOUNTS_UNKNOWN_MISC_EVENTS_TOTAL: IntCounter = IntCounter::new(
        "orderbook_accounts_unknown_misc_events_total",
        "Misc events of a kind the account view does not know"
    ).expect("metric can be created");

    // ─── node marks (design 000439) ───

    pub static ref NODE_MARKS_AVAILABLE: IntGauge = IntGauge::new(
        "orderbook_node_marks_available",
        "1 while the node marks are trusted"
    ).expect("metric can be created");

    pub static ref NODE_MARKS_HEIGHT: IntGauge = IntGauge::new(
        "orderbook_node_marks_height",
        "Height of the trusted node marks"
    ).expect("metric can be created");

    pub static ref NODE_MARKS_SEED_HEIGHT: IntGauge = IntGauge::new(
        "orderbook_node_marks_seed_height",
        "Checkpoint the node marks were last rebuilt from"
    ).expect("metric can be created");

    /// Item pairing failures observed at ingestion, including known whole-batch rejections; not log lines.
    pub static ref NODE_MARKS_IOC_PAIRINGS_TOTAL: IntCounterVec = {
        #[expect(clippy::expect_used, reason = "the metric name and label descriptor are static valid Prometheus names")]
        let counter = IntCounterVec::new(
            Opts::new("orderbook_node_marks_ioc_pairings_total", "IOC pairing observations by kind (batchRejected, unknown, suppressed)"),
            &["kind"]
        ).expect("static IOC pairing metric descriptor is valid");
        counter
    };

    pub static ref NODE_MARKS_LOGGED_INPUTS: IntGauge = IntGauge::new(
        "orderbook_node_marks_logged_inputs",
        "HIP-3 mark updates and main-dex book diffs held in the node marks' input log"
    ).expect("metric can be created");

    /// Each checkpoint against the node marks' snapshot at its height: HIP-3 marks (`hip3`: `equal`, `differ`,
    /// `unknown`) and the main dex's resting orders (`nativeOrders`: `equal`, `differ`, `missing`, `extra`;
    /// `nativeBooks`: `equal`, `differ`, `unknown`).
    pub static ref NODE_MARKS_CHECKED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_node_marks_checked_total", "Checkpoint marks compared with the node marks' snapshot"),
        &["market", "result"]
    ).expect("metric can be created");

    pub static ref NODE_MARKS_CHECKS_SKIPPED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_node_marks_checks_skipped_total", "Checkpoints the node marks were not checked against"),
        &["reason"]
    ).expect("metric can be created");

    /// Seed installs by rebuild (`replayed`, `ahead`, `logDoesNotReach`).
    pub static ref NODE_MARKS_REBUILDS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_node_marks_rebuilds_total", "Node mark seed installs by rebuild"),
        &["result"]
    ).expect("metric can be created");

    pub static ref NODE_MARKS_SEED_FAILURES_TOTAL: IntCounter = IntCounter::new(
        "orderbook_node_marks_seed_failures_total",
        "Checkpoints whose node mark seed broke its contract"
    ).expect("metric can be created");

    /// Seconds one refresh of the liquidation candidates took (design 000443): the rows re-read and the coins evaluated.
    pub static ref LIQUIDATION_CANDIDATES_REFRESH_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new("orderbook_liquidation_candidates_refresh_seconds", "Liquidation candidates refresh time")
            .buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0])
    ).expect("metric can be created");

    /// A liquidation candidates' book walk waiting for the listener lock, and holding it to copy the levels (design
    /// 000443 r2 revision 4 acceptance).
    pub static ref LIQUIDATION_WALK_LOCK_WAIT_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new("orderbook_liquidation_walk_lock_wait_seconds", "Book walk wait for the listener lock")
            .buckets(vec![0.00001, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5])
    ).expect("metric can be created");

    pub static ref LIQUIDATION_WALK_LOCK_HOLD_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new("orderbook_liquidation_walk_lock_hold_seconds", "Book walk level copy under the listener lock")
            .buckets(vec![0.000001, 0.00001, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05])
    ).expect("metric can be created");

    /// The forced flow's reads under the listener lock (design 000451 batch 3), per read (`band`: a coin's trigger
    /// orders in a band, `walk`: one side's levels): waiting for the lock, and holding it to copy.
    pub static ref FORCED_FLOW_LOCK_WAIT_SECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new("orderbook_forced_flow_lock_wait_seconds", "Forced flow read wait for the listener lock")
            .buckets(vec![0.00001, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5]),
        &["read"]
    ).expect("metric can be created");

    pub static ref FORCED_FLOW_LOCK_HOLD_SECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new("orderbook_forced_flow_lock_hold_seconds", "Forced flow read copy under the listener lock")
            .buckets(vec![0.000001, 0.00001, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1]),
        &["read"]
    ).expect("metric can be created");

    /// The forced flow's work per step (`query`, `read`, `tick`, `local`, `accounts`) on its own thread, apart from the
    /// account owner since hyperliquid-trade review 000563 (the name kept).
    pub static ref FORCED_FLOW_OWNER_SECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new("orderbook_forced_flow_owner_seconds", "Forced flow step time on the forced flow thread")
            .buckets(vec![0.00001, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5]),
        &["step"]
    ).expect("metric can be created");

    /// The forced flow's copy of the account view sent again whole (review 000563 r2): the queue was `full`, the copy
    /// `requested` it (an event out of order), or the forced flow thread is gone (`closed`).
    pub static ref FORCED_FLOW_ACCOUNTS_RESYNC_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_forced_flow_accounts_resync_total", "Forced flow account copy resynchronizations"),
        &["cause"]
    ).expect("metric can be created");

    /// The account events waiting for the forced flow thread after the owner's last send.
    pub static ref FORCED_FLOW_ACCOUNTS_QUEUE_DEPTH: IntGauge = IntGauge::new(
        "orderbook_forced_flow_accounts_queue_depth",
        "Account events queued for the forced flow thread"
    ).expect("metric can be created");

    /// The owner's time to build the forced flow's whole copy of the account view.
    pub static ref FORCED_FLOW_ACCOUNTS_SNAPSHOT_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new("orderbook_forced_flow_accounts_snapshot_seconds", "Forced flow account snapshot build time")
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0])
    ).expect("metric can be created");

    /// The listener's time, under its lock, to fork its book for the pending book's replica (hyperliquid-trade design
    /// 000604).
    pub static ref PENDING_BOOK_FORK_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new("orderbook_pending_book_fork_seconds", "Pending book replica fork time under the listener lock")
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5])
    ).expect("metric can be created");

    /// Forks the pending book's replica took.
    pub static ref PENDING_BOOK_FORKS_TOTAL: IntCounter = IntCounter::new(
        "orderbook_pending_book_forks_total",
        "Pending book replica forks"
    ).expect("metric can be created");

    /// The pending book's replica feed broken, by cause: `queue_full`, `book_untrusted`, `backlog_budget`,
    /// `apply_error`, `pending_cleared`, `consumer_gone`.
    pub static ref PENDING_BOOK_BREAKS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_breaks_total", "Pending book replica feed breaks"),
        &["cause"]
    ).expect("metric can be created");

    /// The height the pending book's replica stands at the end of; 0 while it has none aligned.
    pub static ref PENDING_BOOK_CONFIRMED_HEIGHT: IntGauge = IntGauge::new(
        "orderbook_pending_book_confirmed_height",
        "Pending book replica confirmed height"
    ).expect("metric can be created");

    /// Blocks the pending book's replica stands behind the furthest batch it received.
    pub static ref PENDING_BOOK_ALIGNMENT_BLOCKS: IntGauge = IntGauge::new(
        "orderbook_pending_book_alignment_blocks",
        "Pending book replica blocks behind its newest batch"
    ).expect("metric can be created");

    /// Events in the batches waiting for the slower book stream in the pending book's replica.
    pub static ref PENDING_BOOK_BACKLOG_EVENTS: IntGauge = IntGauge::new(
        "orderbook_pending_book_backlog_events",
        "Pending book replica backlog events"
    ).expect("metric can be created");

    /// Levels of the pending book replica whose queue order is not proven (an insertBefore anchor missing, or a New
    /// diff overtaken while it waited for its status; review 000606 r1 P2-1, r2).
    pub static ref PENDING_BOOK_UNPROVEN_LEVELS: IntGauge = IntGauge::new(
        "orderbook_pending_book_unproven_levels",
        "Pending book replica levels whose queue order is unproven"
    ).expect("metric can be created");

    /// Mempool lines the pending book took, by `outcome`: `read`; `read_after_unreported_loss`, read while the watcher
    /// had found a loss it had not yet reported, so possibly after it in the file (review 000609 r1 P2-1); or
    /// `unreadable` (its actions lost; design 000604).
    pub static ref PENDING_BOOK_MEMPOOL_LINES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_mempool_lines_total", "Pending book mempool lines"),
        &["outcome"]
    ).expect("metric can be created");

    /// Mempool lines the watcher lost, by `cause`: `positioning` (the skip at its start) or `discard` (a cut,
    /// overlong or not `[...]` line).
    pub static ref PENDING_BOOK_MEMPOOL_LOSSES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_mempool_losses_total", "Pending book mempool line losses"),
        &["cause"]
    ).expect("metric can be created");

    /// Signed actions of the mempool lines read, by `kind`: `order`, `cancel`, `cancel_by_cloid`, `modify` (a modify
    /// or a batchModify), `set_global`, `trailing_stop` (batch 2n-2: not modelled before its block), `other`.
    pub static ref PENDING_BOOK_MEMPOOL_ACTIONS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_mempool_actions_total", "Pending book mempool signed actions"),
        &["kind"]
    ).expect("metric can be created");

    /// Orders of the mempool lines read (an order action's, a modify's new one), by `tif`: `alo`, `gtc`, `ioc`,
    /// `frontend_market`, `trigger`.
    pub static ref PENDING_BOOK_MEMPOOL_ORDERS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_mempool_orders_total", "Pending book mempool orders"),
        &["tif"]
    ).expect("metric can be created");

    /// Orders, cancels and modifies of the mempool naming an asset the account view's anchor does not, by `reason`:
    /// `not_in_anchor`, or `no_anchor` (before the first).
    pub static ref PENDING_BOOK_MEMPOOL_UNNAMED_ASSETS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_mempool_unnamed_assets_total", "Pending book mempool unnamed assets"),
        &["reason"]
    ).expect("metric can be created");

    /// Entries the forced flow walked (each alone over its side's levels).
    pub static ref FORCED_FLOW_WALKED_ENTRIES_TOTAL: IntCounter = IntCounter::new(
        "orderbook_forced_flow_walked_entries_total",
        "Forced flow entries walked"
    ).expect("metric can be created");

    /// The published liquidation candidates (within the server's distance).
    pub static ref LIQUIDATION_CANDIDATES: IntGauge = IntGauge::new(
        "orderbook_liquidation_candidates",
        "Positions within the server's distance of their liquidation price"
    ).expect("metric can be created");

    /// The local marks' venue feeds (design 000454): frames parsed, lost connections, frames of an unknown type, and
    /// whether a contract error stopped the feed (1) until restart.
    pub static ref LOCAL_MARKS_FEED_FRAMES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_local_marks_feed_frames_total", "Local marks venue feed frames parsed"),
        &["feed"]
    ).expect("metric can be created");

    pub static ref LOCAL_MARKS_FEED_DISCONNECTS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_local_marks_feed_disconnects_total", "Local marks venue feed connections lost (reconnected)"),
        &["feed"]
    ).expect("metric can be created");

    /// Bounded shard IDs assigned once from this build's source file; aggregate feed counters stay compatible.
    pub static ref LOCAL_MARKS_CONNECTION_DISCONNECTS_TOTAL: IntCounterVec = {
        #[expect(clippy::expect_used, reason = "the metric name and label descriptor are static valid Prometheus names")]
        let counter = IntCounterVec::new(
            Opts::new("orderbook_local_marks_connection_disconnects_total", "Local mark connection losses by subscription partition"),
            &["feed", "shard"]
        ).expect("static connection metric descriptor is valid");
        counter
    };

    pub static ref LOCAL_MARKS_FEED_UNKNOWN_FRAMES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_local_marks_feed_unknown_frames_total", "Local marks venue feed frames of an unknown type"),
        &["feed"]
    ).expect("metric can be created");

    pub static ref LOCAL_MARKS_FEED_STOPPED: IntGaugeVec = IntGaugeVec::new(
        Opts::new("orderbook_local_marks_feed_stopped", "Local marks venue feed has at least one partition stopped by a contract error"),
        &["feed"]
    ).expect("metric can be created");

    /// Price actions whose cut is not unique; each reads the book at the first line not known to run before it.
    pub static ref NODE_MARKS_AMBIGUOUS_CUTS_TOTAL: IntCounter = IntCounter::new(
        "orderbook_node_marks_ambiguous_cuts_total",
        "Main-dex price actions whose place among the block's diff lines is not unique"
    ).expect("metric can be created");

    /// Inputs of coins outside the seed's universe (listed since it), by stream.
    pub static ref NODE_MARKS_UNKNOWN_COIN_INPUTS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_node_marks_unknown_coin_inputs_total", "Node mark inputs of coins outside the seed's universe"),
        &["stream"]
    ).expect("metric can be created");

}

// its own block: the one above is at the macro's recursion limit
lazy_static! {
    /// How old a quote frame's newest quote was when parsed here, from its venue time (clamped at 0 for a venue clock
    /// ahead); read beside a heartbeat timeout's summary (bm 2026-11-04).
    pub static ref LOCAL_MARKS_QUOTE_LAG: HistogramVec = HistogramVec::new(
        HistogramOpts::new("orderbook_local_marks_quote_lag_seconds", "Local marks quote frame arrival minus its newest quote's venue time")
            .buckets(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]),
        &["feed"]
    ).expect("metric can be created");
}


// The external quote tick's (hyperliquid-trade design 000673 §7, r4), in a block of their own.
lazy_static! {
    /// The external quote tick's work per tick: the subscribed coins' partitions copied, their legs judged, the frames
    /// due serialized and published.
    pub static ref EXTERNAL_QUOTE_TICK_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new("orderbook_external_quote_tick_seconds", "External quote tick work per tick")
            .buckets(vec![0.00001, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.05])
    ).expect("metric can be created");

    /// How late each tick woke after the time its interval scheduled it (the tick's drift).
    pub static ref EXTERNAL_QUOTE_TICK_DRIFT_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new("orderbook_external_quote_tick_drift_seconds", "External quote tick wake-up after its scheduled time")
            .buckets(vec![0.00001, 0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.05, 0.1])
    ).expect("metric can be created");

    /// The coins each tick published for a changed decision or a new subscription epoch, the heartbeat's apart.
    pub static ref EXTERNAL_QUOTE_TICK_CHANGED_COINS: Histogram = Histogram::with_opts(
        HistogramOpts::new("orderbook_external_quote_tick_changed_coins", "Coins an external quote tick published for a change")
            .buckets(vec![0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0])
    ).expect("metric can be created");

    /// The coins the last tick judged: those with a subscriber on either channel.
    pub static ref EXTERNAL_QUOTE_TICK_SUBSCRIBED_COINS: IntGauge = IntGauge::new(
        "orderbook_external_quote_tick_subscribed_coins",
        "Coins with an external quote subscriber the last tick judged"
    ).expect("metric can be created");

    /// The frames the tick published, by channel and cause (`changed`, `first`: a subscription epoch's, `heartbeat`).
    pub static ref EXTERNAL_QUOTE_PUBLISHED_FRAMES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_external_quote_published_frames_total", "External quote frames published, by channel and cause"),
        &["channel", "cause"]
    ).expect("metric can be created");

    /// The external quote frames the WS sessions handed to their connections' queues and the queues accepted, by
    /// channel (review 000676 r1 P2: published, enqueued and written counted apart).
    pub static ref EXTERNAL_QUOTE_WS_ENQUEUED_FRAMES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_external_quote_ws_enqueued_frames_total", "External quote frames accepted by WS connection queues, by channel"),
        &["channel"]
    ).expect("metric can be created");

    /// The external quote frames, and their bytes, the WS connections' writers wrote to their sockets, counted once
    /// the write succeeded, by channel.
    pub static ref EXTERNAL_QUOTE_WS_WRITTEN_FRAMES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_external_quote_ws_written_frames_total", "External quote frames written to WS sockets, by channel"),
        &["channel"]
    ).expect("metric can be created");

    pub static ref EXTERNAL_QUOTE_WS_WRITTEN_BYTES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_external_quote_ws_written_bytes_total", "External quote frame bytes written to WS sockets, by channel"),
        &["channel"]
    ).expect("metric can be created");

    /// The sampled change frames' way out, by channel and stage (`receipt_to_publish`, once a frame;
    /// `publish_to_enqueue`, `queue`, `write`, `receipt_to_written`, each connection that wrote it), at most 10
    /// frames a channel a second: `websocket_server::quote_delivery`.
    pub static ref EXTERNAL_QUOTE_SAMPLE_SECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new("orderbook_external_quote_sample_seconds", "Sampled external quote change frames' time by stage")
            .buckets(vec![0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1.0, 5.0]),
        &["channel", "stage"]
    ).expect("metric can be created");
}


// The forced flow's stale spells, in a block of their own (the accounts block is at the macro recursion limit).
lazy_static! {
    /// Stale spells of the forced flow frames' account data whose log line was dropped: the log task's queue `full`, or the
    /// task gone (`closed`) (hyperliquid-trade review 000693).
    pub static ref FORCED_FLOW_STALE_SPELLS_DROPPED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_forced_flow_stale_spells_dropped_total", "Forced flow stale spells not logged"),
        &["cause"]
    ).expect("metric can be created");
}


// The node marks' trailing stops, in a block of their own (the accounts block is at the macro recursion limit).
lazy_static! {
    // ==================== NODE MARKS: TRAILING STOPS (batch 2n-1, hyperliquid-trade design 000659) ====================

    /// Batch 2n-1: the trailing stops' lines the node marks read - `open` (`added`, `unparsed`, `unordered`: opened beside a
    /// mark write of its coin, its placement's rank unknown), `leaving` (the line a stop leaves by, against the stop
    /// held: `match`, `rule_mismatch`, `best_mismatch`, `trigger_mismatch`, `unordered`, `unparsed`, `unknown`,
    /// `untracked`) - and `receipt` (`unread`).
    pub static ref NODE_MARKS_TRAILING_LINES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_node_marks_trailing_lines_total", "Trailing stop lines and receipts the node marks read, by line and result"),
        &["line", "result"]
    ).expect("metric can be created");

    /// Batch 2n-1: the main dex's trailing stops the node marks hold, by state (`waiting`, `active`, `unknown`).
    pub static ref NODE_MARKS_TRAILING_STOPS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("orderbook_node_marks_trailing_stops", "Main-dex trailing stops the node marks hold, by state"),
        &["state"]
    ).expect("metric can be created");

    /// Batch 2n-3: the pending book's reads of the account copy's trailing stops at the replica's height - as the copy or
    /// the replica moves -, by result: `proven` (no mark written between the two: a coin's stops may still be not known -
    /// set or gone after the replica's height, the mark unclaimed -, `trigger_coins_total{outcome="trailing_unknown"}`
    /// tells what that cost), or why not (`account_copy_unavailable`, `no_trailing_copy`, `block_log_short`,
    /// `mark_written`, `refresh_due`).
    pub static ref PENDING_BOOK_TRAILING_READS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_trailing_reads_total", "Pending book reads of the account copy's trailing stops, by result"),
        &["result"]
    ).expect("metric can be created");
}


lazy_static! {
    // ==================== PENDING BOOK, BATCH 2B (hyperliquid-trade design 000604) ====================

    /// Orders the pending book replica's cloid index holds (each coin's, kept from the statuses; batch 2b).
    pub static ref PENDING_BOOK_CLOID_INDEX_ORDERS: IntGauge = IntGauge::new(
        "orderbook_pending_book_cloid_index_orders",
        "Pending book replica cloid index orders"
    ).expect("metric can be created");

    /// The pending set's actions leaving it or never held, by `op` (`place_alo`, `place_gtc`, `place_ioc`, `cancel`,
    /// `cancel_by_cloid`, `modify`; `line`, `any`) and `outcome`: refused by HL's rules before any book `expires_after`
    /// (its action past its `expiresAfter` when its line came), `off_grid` (a price or size off its coin's grid) and
    /// `off_grid_batch` (one on it, refused with its action; batch 2d), `too_large` (worth more than its coin's largest
    /// order; batch 2e); at arrival `held_already` (a line again),
    /// `expired_on_arrival`, `late` (a block the replica confirmed holds its line); settled `confirmed` (its line's
    /// block confirmed), `expired`; `over_cap` (the set dropped whole; hyperliquid-trade design 000604 r6, batch 2b).
    pub static ref PENDING_BOOK_ACTIONS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_actions_total", "Pending book actions by outcome"),
        &["op", "outcome"]
    ).expect("metric can be created");

    /// The pending actions' first application to their coin's overlay, by `op` and `outcome`: `rested`, `took`,
    /// `missed` (an IOC with nothing to take), `alo_crossed`, `min_notional` (worth under HL's 10 of the quote; batch
    /// 2d), `canceled`, `target_missing`, `target_ambiguous`,
    /// `gtc_would_take`, `taker_not_supported` (a modify without always_place).
    pub static ref PENDING_BOOK_OVERLAY_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_overlay_total", "Pending book overlay applications by outcome"),
        &["op", "outcome"]
    ).expect("metric can be created");

    /// Fills predicted for pending takers while the book could be served, by `maker`: `confirmed`, `pending`,
    /// `unattributed` (a level whose queue order the replica cannot prove).
    pub static ref PENDING_BOOK_PREDICTED_FILLS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_predicted_fills_total", "Pending book predicted fills"),
        &["maker"]
    ).expect("metric can be created");

    /// Actions the pending set holds, by `state`: `known` (overlaid) or `unknown` (a gap, its warm-up, ambiguous).
    pub static ref PENDING_BOOK_HELD_ACTIONS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("orderbook_pending_book_held_actions", "Pending book held actions"),
        &["state"]
    ).expect("metric can be created");

    /// Input gaps of the pending book, by `cause`: `start`, `loss`, `read_after_loss`, `no_asset_table`, `silence`,
    /// `over_cap` (the mempool's); `block_log_start`, `block_log_loss`, `block_log_after_loss`, `block_log_hole` (the
    /// block log's); `replica`.
    pub static ref PENDING_BOOK_GAPS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_gaps_total", "Pending book input gaps"),
        &["cause"]
    ).expect("metric can be created");

    /// The newest mempool line's time less the replica's confirmed block time.
    pub static ref PENDING_BOOK_SPAN_SECONDS: Gauge = Gauge::new(
        "orderbook_pending_book_span_seconds",
        "Pending book mempool lead on the confirmed blocks"
    ).expect("metric can be created");

    /// Block log lines the pending book took, by `outcome`: `read`, `read_after_unreported_loss`, `unreadable`.
    pub static ref PENDING_BOOK_BLOCK_LOG_LINES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_block_log_lines_total", "Pending book block log lines"),
        &["outcome"]
    ).expect("metric can be created");

    /// Block log lines the pending book's watcher lost, by `cause`: `positioning`, `discard`.
    pub static ref PENDING_BOOK_BLOCK_LOG_LOSSES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_block_log_losses_total", "Pending book block log line losses"),
        &["cause"]
    ).expect("metric can be created");

    /// Signed actions whose signer the pending book recovered (without a vault: batch 2g's order actions that may take,
    /// batch 2m-2's actions placing a trigger order), by `source` (`mempool`, `block_log`) and `outcome`: `recovered`, or
    /// `unknown` - a number not a 64-bit integer in the action, or a signature no key signed.
    pub static ref PENDING_BOOK_SIGNERS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_signers_total", "Pending book signed actions whose signer was recovered"),
        &["source", "outcome"]
    ).expect("metric can be created");

    /// The trigger orders the mempool placed, by how their account was known (batch 2m-2): `vault`, `learned` (its
    /// signer's, as the block log taught it), `signer_unseen`, `signer_conflicting`, `no_signer`.
    pub static ref PENDING_BOOK_TRIGGER_ACCOUNTS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_trigger_accounts_total", "Pending book trigger orders placed by how their account was known"),
        &["source"]
    ).expect("metric can be created");

    /// The signers whose account the block log taught (batch 2m-2), by `state`: `known`, `conflicting`.
    pub static ref PENDING_BOOK_SIGNER_ACCOUNTS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("orderbook_pending_book_signer_accounts", "Pending book signers whose account the block log taught"),
        &["state"]
    ).expect("metric can be created");

    /// The pending book's TWAP book (batch 2h) by `event`: `anchor_installed`, `anchor_not_linked`, `loss`,
    /// `late_line`, `block_log_hole`, `incomplete_too_long`, `fill_repeated`, `fill_unknown_twap`, `suborder_refused`,
    /// `catch_up_unknown`, `activation_unknown_coin`, `activation_bad_limit`, `seed_unknown_asset`, `seed_bad_limit`,
    /// `seed_bad_size`, `seed_bad_due`, `seed_bad_slot_count`, `waiting_for_trigger`.
    pub static ref PENDING_BOOK_TWAP_EVENTS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_twap_events_total", "Pending book TWAP book events"),
        &["event"]
    ).expect("metric can be created");

    /// Taker orders' outcomes the pending book took from the block log for its margin refusals (batch 2g), by
    /// `outcome`: `refused` (margin), `cleared` (filled, resting, an IOC missing every level), `no_identity` (its signer
    /// unknown).
    pub static ref PENDING_BOOK_TAKER_OUTCOMES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_taker_outcomes_total", "Pending book taker order outcomes from the block log"),
        &["outcome"]
    ).expect("metric can be created");

    /// Blocks the pending book's block record holds above the replica's confirmed height (capped, design 000604 r7).
    pub static ref PENDING_BOOK_BLOCK_LOG_LEAD_BLOCKS: IntGauge = IntGauge::new(
        "orderbook_pending_book_block_log_lead_blocks",
        "Pending book block log blocks ahead of the replica"
    ).expect("metric can be created");

    /// Frames the pending book published, by `kind`: `bbo` (a coin's moved best levels), `trades` (a step's predicted
    /// trades), `availability` (a change of whether it may be served; batch 2c).
    pub static ref PENDING_BOOK_FRAMES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_frames_total", "Pending book frames published"),
        &["kind"]
    ).expect("metric can be created");

    /// Coins whose merged best levels (the overlay over the replica) differ from the replica's.
    pub static ref PENDING_BOOK_MOVED_COINS: IntGauge = IntGauge::new(
        "orderbook_pending_book_moved_coins",
        "Pending book coins whose predicted best levels differ from the confirmed"
    ).expect("metric can be created");

    /// Seconds the pending book could not be served, by `reason`: `replica`, a mempool or block log gap's cause,
    /// `block_log_behind`, `warming_up`, `span`.
    pub static ref PENDING_BOOK_UNAVAILABLE_SECONDS_TOTAL: CounterVec = CounterVec::new(
        Opts::new("orderbook_pending_book_unavailable_seconds_total", "Pending book unavailable seconds"),
        &["reason"]
    ).expect("metric can be created");

    /// The pending thread's time per `step`: `transaction` (a mempool line's actions held and overlaid), `replica` (a
    /// replica push applied, the set settled, the coins replayed), `block_log` (a block's hashes recorded, the set
    /// settled), `sga_forecast` (a `SetGlobalAction`'s marks forecast, batch 2l).
    pub static ref PENDING_BOOK_STEP_SECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new("orderbook_pending_book_step_seconds", "Pending book step time")
            .buckets(vec![0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1]),
        &["step"]
    ).expect("metric can be created");

    /// The mempool's `SetGlobalAction` lines (batch 2l, `pending::mark_forecast`), by `outcome`: `forecast`, or why
    /// none - `no_asset_table`, `no_prices` (the account view's node prices unavailable), `unavailable` (the pending book
    /// may not be served), `late` (its block logged already) -; once its block applies, `checked` or `late` (the prices
    /// read held it already); `expired` (unchecked for 30 s, or pushed out by newer ones).
    pub static ref PENDING_BOOK_SGA_LINES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_sga_lines_total", "Pending book SetGlobalAction lines by outcome"),
        &["outcome"]
    ).expect("metric can be created");

    /// A `SetGlobalAction`'s forecast marks by `book` (`pending`: the merged best prices; `confirmed`: the replica's)
    /// and `outcome`: checked, `exact`, `off`, `superseded` (a later mark stored), `not_set` (its block stored none),
    /// `unclaimed` (the coin left the node prices); none made, `no_state`, `unreadable`, `no_book`, `unit_mismatch`,
    /// `unselected`.
    pub static ref PENDING_BOOK_SGA_MARKS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_sga_marks_total", "Pending book SetGlobalAction forecast marks by outcome"),
        &["book", "outcome"]
    ).expect("metric can be created");

    /// How far an `off` forecast mark was from the stored one, in basis points, by `book`.
    pub static ref PENDING_BOOK_SGA_MARK_ERROR_BPS: HistogramVec = HistogramVec::new(
        HistogramOpts::new("orderbook_pending_book_sga_mark_error_bps", "Pending book SetGlobalAction forecast mark error")
            .buckets(vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 50.0, 200.0]),
        &["book"]
    ).expect("metric can be created");

    /// Whether a `SetGlobalAction`'s line time fell in its block time's reference cache bucket: `same` or `different`.
    pub static ref PENDING_BOOK_SGA_BUCKET_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_sga_bucket_total", "Pending book SetGlobalAction bucket estimates"),
        &["outcome"]
    ).expect("metric can be created");

    /// The pending book's copy of the account view (batch 2m, design 000652 r2): its resynchronizations by `cause` -
    /// `requested`, `full`, `closed` -, its queue, its snapshots' build time.
    pub static ref PENDING_BOOK_ACCOUNTS_RESYNC_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_accounts_resync_total", "Pending book account copy resynchronizations"),
        &["cause"]
    ).expect("metric can be created");

    pub static ref PENDING_BOOK_ACCOUNTS_QUEUE_DEPTH: IntGauge = IntGauge::new(
        "orderbook_pending_book_accounts_queue_depth",
        "Account events queued for the pending book thread"
    ).expect("metric can be created");

    pub static ref PENDING_BOOK_ACCOUNTS_SNAPSHOT_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new("orderbook_pending_book_accounts_snapshot_seconds", "Pending book account snapshot build time")
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0])
    ).expect("metric can be created");

    /// A `SetGlobalAction`'s triggered orders, as first applied, by their source - `confirmed`, `pending` -, and once more
    /// `zero_size` when they had no position left to close (batch 2m).
    pub static ref PENDING_BOOK_TRIGGERS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_triggers_total", "Pending book trigger orders by outcome"),
        &["outcome"]
    ).expect("metric can be created");

    /// A `SetGlobalAction`'s priced coins, as first applied, by `outcome`: `executed`, `none_fired`, or why none ran
    /// (batch 2m) - `no_forecast`, `mark_overflow` as its line came; `no_trigger_table`, `table_incomplete`, `truncated`,
    /// `limit_overflow`, `account_unknown`, `position_moved`, `positions_unfrozen`, `positions_unavailable`; or the run
    /// deciding nothing of the coin from it on (batch 2n-2): `trailing_unknown` (a trailing stop not known it may fire),
    /// or that, an earlier one's, where its line came.
    pub static ref PENDING_BOOK_TRIGGER_COINS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_pending_book_trigger_coins_total", "Pending book SetGlobalAction coins by trigger outcome"),
        &["outcome"]
    ).expect("metric can be created");

    /// Blocks between the node prices a checked forecast read and its `SetGlobalAction`'s block.
    pub static ref PENDING_BOOK_SGA_PRICES_BEHIND_BLOCKS: Histogram = Histogram::with_opts(
        HistogramOpts::new("orderbook_pending_book_sga_prices_behind_blocks", "Pending book SetGlobalAction forecast prices behind")
            .buckets(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 20.0, 50.0])
    ).expect("metric can be created");
}

lazy_static! {
    /// The basis windows' samples store (hyperliquid-trade design 000741 r2): a tick's samples dropped before the
    /// writer (`queue_full`, `writer_gone`), and the store's errors (`write`, `restore`).
    pub static ref BASIS_STORE_DROPPED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_basis_store_dropped_total", "Basis sample batches dropped before the store's writer"),
        &["cause"]
    ).expect("metric can be created");

    pub static ref BASIS_STORE_ERRORS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("orderbook_basis_store_errors_total", "Basis store writes and restores that failed"),
        &["operation"]
    ).expect("metric can be created");
}

/// Register all metrics with the registry
pub fn register_metrics() {
    // Connection metrics
    REGISTRY.register(Box::new(WS_CONNECTIONS_ACTIVE.clone())).ok();
    REGISTRY.register(Box::new(BASIS_STORE_DROPPED_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(BASIS_STORE_ERRORS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(WS_CONNECTIONS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(WS_SUBSCRIPTIONS_ACTIVE.clone())).ok();

    // Latency metrics
    REGISTRY.register(Box::new(BBO_BROADCAST_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(L2_BROADCAST_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(L2_FLUSH_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(NODE_LINE_LAG.clone())).ok();
    REGISTRY.register(Box::new(NODE_FILE_IO_ERRORS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(L2_CONFLATION_BATCH_SIZE.clone())).ok();
    REGISTRY.register(Box::new(EVENT_PROCESSING_LATENCY.clone())).ok();

    // Throughput metrics
    REGISTRY.register(Box::new(EVENTS_PROCESSED_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(BROADCASTS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(TRADES_UNPAIRED_FILLS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(MESSAGES_SENT_TOTAL.clone())).ok();

    // Health metrics
    REGISTRY.register(Box::new(ORDERBOOK_HEIGHT.clone())).ok();
    REGISTRY.register(Box::new(ORDERBOOK_TIME_MS.clone())).ok();
    REGISTRY.register(Box::new(PENDING_ORDERS_CACHE.clone())).ok();
    REGISTRY.register(Box::new(PENDING_DIFFS_CACHE.clone())).ok();
    REGISTRY.register(Box::new(ORDERBOOK_STREAM_HEIGHT.clone())).ok();
    REGISTRY.register(Box::new(ORDERBOOK_STREAM_SKEW_BLOCKS.clone())).ok();
    REGISTRY.register(Box::new(PENDING_ORPHANS_EVICTED_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ORDERBOOK_BOOK_TRUSTED.clone())).ok();
    REGISTRY.register(Box::new(ORDERBOOK_UNTRUSTED_SECONDS_TOTAL.clone())).ok();
    ORDERBOOK_BOOK_TRUSTED.set(1);
    REGISTRY.register(Box::new(CHANNEL_LAG.clone())).ok();

    // Error metrics
    REGISTRY.register(Box::new(PARSE_ERRORS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(WS_SEND_ERRORS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(CHANNEL_DROPS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(WS_BROADCAST_SKIPPED_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(WS_DELIVERY_WAIT.clone())).ok();
    REGISTRY.register(Box::new(ORDERBOOK_DESYNCS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ORACLE_DATA_LOSS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(INSERT_BEFORE_FALLBACK_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(DIFF_TARGET_MISSING_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(STALE_BATCHES_SKIPPED_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(REGRESSING_BOOK_BATCHES_SKIPPED_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(CHECKPOINT_MISSED_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(SNAPSHOT_CHECKPOINT_HEIGHT.clone())).ok();
    REGISTRY.register(Box::new(INSERT_BEFORE_HONORED_TOTAL.clone())).ok();

    // Accounts
    REGISTRY.register(Box::new(ACCOUNTS_TRUSTED.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_ANCHOR_HEIGHT.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_ANCHOR_PARSE_SECONDS.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_CHECKPOINT_INSTALL_SECONDS.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_ANCHOR_SKIPPED_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_CHECKPOINT_FAILURES_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_OPEN_POSITIONS.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_HELD_FILLS.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_STALE_FILL_BATCHES_SKIPPED_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_SILENT_POSITION_CHANGES_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_STREAM_HEIGHT.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_DATA_LOSS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_UNKNOWN_MISC_EVENTS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_DUPLICATE_INPUTS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_REPLAY_CHECKED_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_REPLAY_STOPPED_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_REPLAY_ROWS_CHECKED_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_REPLAY_CHECKS_SKIPPED_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_APPLIED_HEIGHT.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_TRUSTED_HEIGHT.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_PENDING_BLOCKS.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_FRAMES_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_EVIDENCE_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ACCOUNTS_DIAGNOSTIC_EVENTS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(NODE_MARKS_AVAILABLE.clone())).ok();
    REGISTRY.register(Box::new(NODE_MARKS_HEIGHT.clone())).ok();
    REGISTRY.register(Box::new(NODE_MARKS_SEED_HEIGHT.clone())).ok();
    REGISTRY.register(Box::new(NODE_MARKS_LOGGED_INPUTS.clone())).ok();
    REGISTRY.register(Box::new(NODE_MARKS_IOC_PAIRINGS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(NODE_MARKS_CHECKED_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(NODE_MARKS_CHECKS_SKIPPED_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(NODE_MARKS_REBUILDS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(NODE_MARKS_SEED_FAILURES_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(NODE_MARKS_UNKNOWN_COIN_INPUTS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(NODE_MARKS_AMBIGUOUS_CUTS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(NODE_MARKS_TRAILING_LINES_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(NODE_MARKS_TRAILING_STOPS.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_TRAILING_READS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(LIQUIDATION_CANDIDATES_REFRESH_SECONDS.clone())).ok();
    REGISTRY.register(Box::new(LIQUIDATION_CANDIDATES.clone())).ok();
    REGISTRY.register(Box::new(LIQUIDATION_WALK_LOCK_WAIT_SECONDS.clone())).ok();
    REGISTRY.register(Box::new(LIQUIDATION_WALK_LOCK_HOLD_SECONDS.clone())).ok();
    REGISTRY.register(Box::new(FORCED_FLOW_LOCK_WAIT_SECONDS.clone())).ok();
    REGISTRY.register(Box::new(FORCED_FLOW_LOCK_HOLD_SECONDS.clone())).ok();
    REGISTRY.register(Box::new(FORCED_FLOW_OWNER_SECONDS.clone())).ok();
    REGISTRY.register(Box::new(FORCED_FLOW_ACCOUNTS_RESYNC_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(FORCED_FLOW_ACCOUNTS_QUEUE_DEPTH.clone())).ok();
    REGISTRY.register(Box::new(FORCED_FLOW_STALE_SPELLS_DROPPED_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(FORCED_FLOW_ACCOUNTS_SNAPSHOT_SECONDS.clone())).ok();
    REGISTRY.register(Box::new(FORCED_FLOW_WALKED_ENTRIES_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_FORK_SECONDS.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_FORKS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_BREAKS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_CONFIRMED_HEIGHT.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_ALIGNMENT_BLOCKS.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_BACKLOG_EVENTS.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_UNPROVEN_LEVELS.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_MEMPOOL_LINES_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_MEMPOOL_LOSSES_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_MEMPOOL_ACTIONS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_MEMPOOL_ORDERS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_MEMPOOL_UNNAMED_ASSETS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_CLOID_INDEX_ORDERS.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_ACTIONS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_OVERLAY_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_PREDICTED_FILLS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_HELD_ACTIONS.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_GAPS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_SPAN_SECONDS.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_MOVED_COINS.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_FRAMES_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_BLOCK_LOG_LINES_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_SIGNERS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_TRIGGER_ACCOUNTS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_SIGNER_ACCOUNTS.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_TAKER_OUTCOMES_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_TWAP_EVENTS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_BLOCK_LOG_LOSSES_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_BLOCK_LOG_LEAD_BLOCKS.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_UNAVAILABLE_SECONDS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_STEP_SECONDS.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_SGA_LINES_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_SGA_MARKS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_SGA_MARK_ERROR_BPS.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_SGA_BUCKET_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_SGA_PRICES_BEHIND_BLOCKS.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_ACCOUNTS_RESYNC_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_ACCOUNTS_QUEUE_DEPTH.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_ACCOUNTS_SNAPSHOT_SECONDS.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_TRIGGERS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_BOOK_TRIGGER_COINS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(LOCAL_MARKS_QUOTE_LAG.clone())).ok();
    REGISTRY.register(Box::new(LOCAL_MARKS_FEED_FRAMES_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(LOCAL_MARKS_FEED_DISCONNECTS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(LOCAL_MARKS_CONNECTION_DISCONNECTS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(LOCAL_MARKS_FEED_UNKNOWN_FRAMES_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(LOCAL_MARKS_FEED_STOPPED.clone())).ok();
    REGISTRY.register(Box::new(EXTERNAL_QUOTE_TICK_SECONDS.clone())).ok();
    REGISTRY.register(Box::new(EXTERNAL_QUOTE_TICK_DRIFT_SECONDS.clone())).ok();
    REGISTRY.register(Box::new(EXTERNAL_QUOTE_TICK_CHANGED_COINS.clone())).ok();
    REGISTRY.register(Box::new(EXTERNAL_QUOTE_TICK_SUBSCRIBED_COINS.clone())).ok();
    REGISTRY.register(Box::new(EXTERNAL_QUOTE_PUBLISHED_FRAMES_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(EXTERNAL_QUOTE_WS_ENQUEUED_FRAMES_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(EXTERNAL_QUOTE_WS_WRITTEN_FRAMES_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(EXTERNAL_QUOTE_WS_WRITTEN_BYTES_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(EXTERNAL_QUOTE_SAMPLE_SECONDS.clone())).ok();

    // File watcher metrics
    REGISTRY.register(Box::new(FILE_EVENTS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(FILE_LINES_PARSED_TOTAL.clone())).ok();

    // Orderbook stats
    REGISTRY.register(Box::new(ORDERBOOK_ORDERS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ORDERBOOK_COINS_COUNT.clone())).ok();
    REGISTRY.register(Box::new(ORDERBOOK_UNTRIGGERED_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(UNTRIGGERED_EVICTIONS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(UNTRIGGERED_UNMATCHED_CHILDREN_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(UNTRIGGERED_COIN_GAPS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ORDERBOOK_UNTRIGGERED_INCOMPLETE_COINS.clone())).ok();
    REGISTRY.register(Box::new(ORDERBOOK_UNTRIGGERED_INSTALL_COMPLETE.clone())).ok();
    REGISTRY.register(Box::new(UNTRIGGERED_MISREAD_TRIGGERS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(BBO_CHANGES_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(BBO_PUBLICATION_QUOTES_TOTAL.clone())).ok();

    // Resync & lock metrics
    REGISTRY.register(Box::new(RESYNC_PHASE_DURATION.clone())).ok();
    REGISTRY.register(Box::new(LISTENER_LOCK_WAIT.clone())).ok();
    REGISTRY.register(Box::new(ORDERBOOK_READY.clone())).ok();
    REGISTRY.register(Box::new(LAST_EVENT_APPLIED_MS.clone())).ok();
    REGISTRY.register(Box::new(ORDERBOOK_RESYNC_IN_FLIGHT.clone())).ok();

    // Uptime & system
    REGISTRY.register(Box::new(UPTIME_SECONDS.clone())).ok();
    REGISTRY.register(Box::new(SERVER_START_TIME.clone())).ok();
    REGISTRY.register(Box::new(BROADCAST_RECEIVERS.clone())).ok();

    // Set server start time
    let start_time =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0);
    SERVER_START_TIME.set(start_time);
}

/// Get metrics as Prometheus text format
pub fn gather_metrics() -> String {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}
