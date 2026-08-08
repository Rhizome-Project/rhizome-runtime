CREATE TABLE IF NOT EXISTS workspace_security_settings (
    workspace_id         TEXT PRIMARY KEY REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    password_hash        TEXT NOT NULL,
    password_updated_by  TEXT NOT NULL,
    password_updated_at  TEXT NOT NULL,
    created_at           TEXT NOT NULL,
    updated_at           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workspace_humans (
    workspace_id        TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    human_id            TEXT NOT NULL,
    display_name        TEXT NOT NULL,
    display_name_norm   TEXT NOT NULL,
    password_hash       TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    last_login_at       TEXT,
    PRIMARY KEY (workspace_id, human_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_workspace_humans_name
    ON workspace_humans(workspace_id, display_name_norm);

CREATE TABLE IF NOT EXISTS workspace_auth_tokens (
    token_id        TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    subject_type    TEXT NOT NULL,
    subject_id      TEXT NOT NULL,
    subject_label   TEXT NOT NULL,
    token_hash      TEXT NOT NULL UNIQUE,
    token_prefix    TEXT NOT NULL,
    issued_by       TEXT NOT NULL,
    issued_at       TEXT NOT NULL,
    last_used_at    TEXT,
    revoked_at      TEXT,
    revoked_reason  TEXT NOT NULL DEFAULT '',
    metadata_json   TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_workspace_auth_tokens_subject
    ON workspace_auth_tokens(workspace_id, subject_type, subject_id, issued_at DESC);

CREATE INDEX IF NOT EXISTS idx_workspace_auth_tokens_active
    ON workspace_auth_tokens(workspace_id, revoked_at, issued_at DESC);

CREATE TABLE IF NOT EXISTS workspace_security_events (
    event_id       TEXT PRIMARY KEY,
    workspace_id   TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    event_type     TEXT NOT NULL,
    actor_type     TEXT NOT NULL,
    actor_id       TEXT NOT NULL,
    actor_label    TEXT NOT NULL,
    subject_type   TEXT NOT NULL,
    subject_id     TEXT NOT NULL,
    subject_label  TEXT NOT NULL,
    remote_ip      TEXT,
    user_agent     TEXT,
    detail_json    TEXT NOT NULL DEFAULT '{}',
    created_at     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workspace_security_events_workspace
    ON workspace_security_events(workspace_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_workspace_security_events_type
    ON workspace_security_events(workspace_id, event_type, created_at DESC);
