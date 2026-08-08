ALTER TABLE episode_packs
    ADD COLUMN lifecycle_event_id TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_episode_packs_lifecycle_event
    ON episode_packs(workspace_id, lifecycle_event_id, updated_at DESC);
