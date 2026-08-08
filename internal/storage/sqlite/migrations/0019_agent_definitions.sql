-- Agent definitions: user-defined agent templates

CREATE TABLE IF NOT EXISTS agent_definitions (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    provider        TEXT NOT NULL DEFAULT 'claude',
    model           TEXT NOT NULL DEFAULT '',
    system_prompt   TEXT NOT NULL DEFAULT '',
    tools_json      TEXT NOT NULL DEFAULT '[]',
    max_iterations  INTEGER NOT NULL DEFAULT 50,
    workspace_id    TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now')),
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now'))
);

CREATE INDEX IF NOT EXISTS idx_agent_definitions_workspace_id ON agent_definitions(workspace_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_definitions_name_workspace ON agent_definitions(name, workspace_id);
