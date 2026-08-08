-- Canonical workspace-scoped runtime memory backed by the main store.

CREATE TABLE IF NOT EXISTS workspace_memory (
    memory_id    TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id),
    memory_type  TEXT NOT NULL,
    title        TEXT NOT NULL DEFAULT '',
    body         TEXT NOT NULL,
    summary      TEXT NOT NULL DEFAULT '',
    agent_id     TEXT,
    session_id   TEXT REFERENCES agent_sessions(session_id),
    task_id      TEXT,
    source_kind  TEXT NOT NULL DEFAULT 'manual',
    source_id    TEXT NOT NULL DEFAULT '',
    tags_json    TEXT NOT NULL DEFAULT '[]',
    importance   REAL NOT NULL DEFAULT 0,
    confidence   REAL NOT NULL DEFAULT 0,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workspace_memory_workspace_updated
    ON workspace_memory(workspace_id, updated_at DESC, memory_id DESC);
CREATE INDEX IF NOT EXISTS idx_workspace_memory_workspace_type
    ON workspace_memory(workspace_id, memory_type, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_workspace_memory_workspace_agent
    ON workspace_memory(workspace_id, agent_id, updated_at DESC)
    WHERE agent_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_workspace_memory_workspace_session
    ON workspace_memory(workspace_id, session_id, updated_at DESC)
    WHERE session_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_workspace_memory_workspace_task
    ON workspace_memory(workspace_id, task_id, updated_at DESC)
    WHERE task_id IS NOT NULL;

CREATE VIRTUAL TABLE IF NOT EXISTS workspace_memory_fts USING fts5(
    title,
    body,
    summary,
    content='workspace_memory',
    content_rowid='rowid'
);

CREATE TRIGGER IF NOT EXISTS workspace_memory_ai AFTER INSERT ON workspace_memory BEGIN
    INSERT INTO workspace_memory_fts(rowid, title, body, summary)
    VALUES (new.rowid, new.title, new.body, new.summary);
END;

CREATE TRIGGER IF NOT EXISTS workspace_memory_ad AFTER DELETE ON workspace_memory BEGIN
    INSERT INTO workspace_memory_fts(workspace_memory_fts, rowid, title, body, summary)
    VALUES ('delete', old.rowid, old.title, old.body, old.summary);
END;

CREATE TRIGGER IF NOT EXISTS workspace_memory_au AFTER UPDATE ON workspace_memory BEGIN
    INSERT INTO workspace_memory_fts(workspace_memory_fts, rowid, title, body, summary)
    VALUES ('delete', old.rowid, old.title, old.body, old.summary);
    INSERT INTO workspace_memory_fts(rowid, title, body, summary)
    VALUES (new.rowid, new.title, new.body, new.summary);
END;

INSERT INTO workspace_memory_fts(workspace_memory_fts) VALUES ('rebuild');
