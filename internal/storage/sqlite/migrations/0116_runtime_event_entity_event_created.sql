CREATE INDEX IF NOT EXISTS idx_runtime_events_entity_event_created
ON runtime_events(workspace_id, entity_type, entity_id, event_type, created_at DESC, event_id DESC);
