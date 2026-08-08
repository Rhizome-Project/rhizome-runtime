ALTER TABLE runtime_events ADD COLUMN authority_holder_node_id TEXT;
ALTER TABLE runtime_events ADD COLUMN authority_term INTEGER;
ALTER TABLE runtime_events ADD COLUMN authority_lease_token_fingerprint TEXT;

CREATE INDEX IF NOT EXISTS idx_runtime_events_workspace_authority_term_cursor
ON runtime_events(workspace_id, authority_term, ingest_seq DESC, event_id DESC)
WHERE authority_term IS NOT NULL;
