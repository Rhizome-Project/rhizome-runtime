ALTER TABLE workspace_tensions
    ADD COLUMN last_detected_at TEXT NOT NULL DEFAULT '';

ALTER TABLE workspace_tensions
    ADD COLUMN last_refreshed_at TEXT NOT NULL DEFAULT '';

ALTER TABLE workspace_tensions
    ADD COLUMN stale_refresh_count INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_workspace_tensions_workspace_refresh
    ON workspace_tensions(workspace_id, lifecycle_state, review_status, last_refreshed_at DESC, surface_score DESC);
