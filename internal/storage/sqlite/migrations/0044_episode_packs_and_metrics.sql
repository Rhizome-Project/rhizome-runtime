CREATE TABLE IF NOT EXISTS episode_packs (
    pack_id                     TEXT PRIMARY KEY,
    pack_key                    TEXT NOT NULL UNIQUE,
    workspace_id                TEXT NOT NULL,
    pack_type                   TEXT NOT NULL,
    pack_mode                   TEXT NOT NULL DEFAULT 'COMPLETE',
    schema_version              TEXT NOT NULL DEFAULT 'rmp-1.2/phase2',
    session_id                  TEXT NOT NULL REFERENCES agent_sessions(session_id) ON DELETE CASCADE,
    lineage_session_id          TEXT NOT NULL DEFAULT '',
    agent_id                    TEXT NOT NULL,
    task_id                     TEXT,
    trigger_kind                TEXT NOT NULL,
    compaction_snapshot_id      TEXT UNIQUE REFERENCES session_compaction_snapshots(snapshot_id) ON DELETE SET NULL,
    source_window_start         INTEGER NOT NULL DEFAULT 0,
    source_window_end           INTEGER NOT NULL DEFAULT -1,
    source_window_digest        TEXT NOT NULL DEFAULT '',
    summary_text                TEXT NOT NULL DEFAULT '',
    summary_digest              TEXT NOT NULL DEFAULT '',
    narrative_summary           TEXT NOT NULL DEFAULT '',
    decision_ledger_json        TEXT NOT NULL DEFAULT '[]',
    artifact_delta_ledger_json  TEXT NOT NULL DEFAULT '[]',
    blocker_ledger_json         TEXT NOT NULL DEFAULT '[]',
    failure_repair_chain_json   TEXT NOT NULL DEFAULT '[]',
    open_loops_json             TEXT NOT NULL DEFAULT '[]',
    dissent_state               TEXT NOT NULL DEFAULT 'UNKNOWN',
    dissent_set_json            TEXT NOT NULL DEFAULT '[]',
    fact_candidates_json        TEXT NOT NULL DEFAULT '[]',
    hypothesis_candidates_json  TEXT NOT NULL DEFAULT '[]',
    provenance_refs_json        TEXT NOT NULL DEFAULT '[]',
    summary_workspace_memory    TEXT,
    message_count_before        INTEGER NOT NULL DEFAULT 0,
    message_count_after         INTEGER NOT NULL DEFAULT 0,
    message_tokens_before       INTEGER NOT NULL DEFAULT 0,
    message_tokens_after        INTEGER NOT NULL DEFAULT 0,
    total_input_tokens          INTEGER NOT NULL DEFAULT 0,
    total_output_tokens         INTEGER NOT NULL DEFAULT 0,
    created_at                  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now')),
    updated_at                  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now'))
);

CREATE INDEX IF NOT EXISTS idx_episode_packs_workspace_updated
    ON episode_packs(workspace_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_episode_packs_session
    ON episode_packs(workspace_id, session_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_episode_packs_task
    ON episode_packs(workspace_id, task_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_episode_packs_type
    ON episode_packs(workspace_id, pack_type, updated_at DESC);

CREATE TABLE IF NOT EXISTS memory_node_metrics (
    memory_id      TEXT NOT NULL REFERENCES memory_nodes(memory_id) ON DELETE CASCADE,
    workspace_id   TEXT NOT NULL,
    metric_key     TEXT NOT NULL,
    metric_value   REAL NOT NULL DEFAULT 0,
    metric_unit    TEXT NOT NULL DEFAULT '',
    metric_kind    TEXT NOT NULL DEFAULT 'scalar',
    metadata_json  TEXT NOT NULL DEFAULT '{}',
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now')),
    updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now')),
    PRIMARY KEY (workspace_id, memory_id, metric_key)
);

CREATE INDEX IF NOT EXISTS idx_memory_node_metrics_workspace_memory
    ON memory_node_metrics(workspace_id, memory_id, metric_key);
