-- Agent sessions and message history for internal agent

CREATE TABLE IF NOT EXISTS agent_sessions (
    session_id          TEXT PRIMARY KEY,
    agent_id            TEXT NOT NULL,
    workspace_id        TEXT NOT NULL,
    task_id             TEXT,
    status              TEXT NOT NULL DEFAULT 'RUNNING',
    iterations          INTEGER NOT NULL DEFAULT 0,
    total_input_tokens  INTEGER NOT NULL DEFAULT 0,
    total_output_tokens INTEGER NOT NULL DEFAULT 0,
    tool_calls          INTEGER NOT NULL DEFAULT 0,
    stop_reason         TEXT,
    error_message       TEXT,
    started_at          TEXT NOT NULL,
    completed_at        TEXT,
    created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now'))
);

CREATE INDEX IF NOT EXISTS idx_agent_sessions_agent_id ON agent_sessions(agent_id);
CREATE INDEX IF NOT EXISTS idx_agent_sessions_workspace_id ON agent_sessions(workspace_id);
CREATE INDEX IF NOT EXISTS idx_agent_sessions_task_id ON agent_sessions(task_id) WHERE task_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS agent_session_messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id  TEXT NOT NULL REFERENCES agent_sessions(session_id),
    sequence    INTEGER NOT NULL,
    role        TEXT NOT NULL,
    content_json TEXT NOT NULL,
    token_count INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now')),
    UNIQUE(session_id, sequence)
);

CREATE INDEX IF NOT EXISTS idx_agent_session_messages_session_id ON agent_session_messages(session_id);
