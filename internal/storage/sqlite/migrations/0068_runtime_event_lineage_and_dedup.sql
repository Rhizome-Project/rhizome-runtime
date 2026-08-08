ALTER TABLE runtime_events ADD COLUMN dedup_key TEXT;
ALTER TABLE runtime_events ADD COLUMN root_cause_id TEXT;
ALTER TABLE runtime_events ADD COLUMN provenance_group_id TEXT;
ALTER TABLE runtime_events ADD COLUMN parent_refs_json TEXT NOT NULL DEFAULT '[]';

CREATE INDEX IF NOT EXISTS idx_runtime_events_workspace_dedup
ON runtime_events(workspace_id, dedup_key, created_at DESC)
WHERE dedup_key IS NOT NULL AND dedup_key <> '';

CREATE INDEX IF NOT EXISTS idx_runtime_events_workspace_root_cause
ON runtime_events(workspace_id, root_cause_id, created_at DESC)
WHERE root_cause_id IS NOT NULL AND root_cause_id <> '';

CREATE INDEX IF NOT EXISTS idx_runtime_events_workspace_provenance_group
ON runtime_events(workspace_id, provenance_group_id, created_at DESC)
WHERE provenance_group_id IS NOT NULL AND provenance_group_id <> '';
