CREATE TABLE IF NOT EXISTS runtime_event_firehose_outbox (
    workspace_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    ingest_seq INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    dispatched_at TEXT,
    PRIMARY KEY (workspace_id, event_id),
    FOREIGN KEY (event_id) REFERENCES runtime_events(event_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_runtime_event_firehose_outbox_pending
    ON runtime_event_firehose_outbox(dispatched_at, ingest_seq, created_at, workspace_id, event_id);
