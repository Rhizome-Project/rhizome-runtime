ALTER TABLE runtime_events ADD COLUMN ingest_seq INTEGER;

WITH ordered AS (
    SELECT
        rowid,
        ROW_NUMBER() OVER (
            PARTITION BY workspace_id
            ORDER BY created_at ASC, event_id ASC
        ) AS seq
    FROM runtime_events
)
UPDATE runtime_events
SET ingest_seq = (
    SELECT ordered.seq
    FROM ordered
    WHERE ordered.rowid = runtime_events.rowid
)
WHERE ingest_seq IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_workspace_ingest_seq
ON runtime_events(workspace_id, ingest_seq)
WHERE ingest_seq IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_runtime_events_workspace_type_ingest
ON runtime_events(workspace_id, event_type, ingest_seq DESC)
WHERE ingest_seq IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_runtime_events_entity_ingest
ON runtime_events(workspace_id, entity_type, entity_id, ingest_seq DESC)
WHERE ingest_seq IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_runtime_events_session_ingest
ON runtime_events(workspace_id, session_id, ingest_seq DESC)
WHERE session_id IS NOT NULL AND ingest_seq IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_runtime_events_task_ingest
ON runtime_events(workspace_id, task_id, ingest_seq DESC)
WHERE task_id IS NOT NULL AND ingest_seq IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_runtime_events_workspace_dedup_ingest
ON runtime_events(workspace_id, dedup_key, ingest_seq DESC)
WHERE dedup_key IS NOT NULL AND dedup_key <> '' AND ingest_seq IS NOT NULL;
