-- P4D-003: Targeted forensic and operator query shape optimizations
-- Drop previous partial ingest_seq indices that lack event_id DESC in order to eliminate TEMP B-TREE sorts.
DROP INDEX IF EXISTS idx_runtime_events_workspace_type_ingest;
DROP INDEX IF EXISTS idx_runtime_events_entity_ingest;
DROP INDEX IF EXISTS idx_runtime_events_session_ingest;
DROP INDEX IF EXISTS idx_runtime_events_task_ingest;
DROP INDEX IF EXISTS idx_runtime_events_workspace_dedup_ingest;

-- Create covering indexes for the runtime_events cursor pagination spine.
-- Adding `event_id DESC` allows SQLite to fulfill the query's ORDER BY exclusively from the index.
CREATE INDEX IF NOT EXISTS idx_runtime_events_workspace_cursor
ON runtime_events(workspace_id, ingest_seq DESC, event_id DESC);

CREATE INDEX IF NOT EXISTS idx_runtime_events_workspace_type_cursor
ON runtime_events(workspace_id, event_type, ingest_seq DESC, event_id DESC);

CREATE INDEX IF NOT EXISTS idx_runtime_events_entity_cursor
ON runtime_events(workspace_id, entity_type, entity_id, ingest_seq DESC, event_id DESC);

CREATE INDEX IF NOT EXISTS idx_runtime_events_session_cursor
ON runtime_events(workspace_id, session_id, ingest_seq DESC, event_id DESC)
WHERE session_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_runtime_events_task_cursor
ON runtime_events(workspace_id, task_id, ingest_seq DESC, event_id DESC)
WHERE task_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_runtime_events_workspace_dedup_cursor
ON runtime_events(workspace_id, dedup_key, ingest_seq DESC, event_id DESC)
WHERE dedup_key IS NOT NULL AND dedup_key <> '';

-- Also add agent_id cursor, mapping to the new bare `agent_id = ?` query shape
CREATE INDEX IF NOT EXISTS idx_runtime_events_agent_cursor
ON runtime_events(workspace_id, agent_id, ingest_seq DESC, event_id DESC)
WHERE agent_id IS NOT NULL AND agent_id <> '';
