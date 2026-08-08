-- Runtime control-plane foundation: append-only journal, operator queue,
-- claims, execution graph, and capability governance.

CREATE TABLE IF NOT EXISTS runtime_events (
    event_id     TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id),
    event_type   TEXT NOT NULL,
    entity_type  TEXT NOT NULL,
    entity_id    TEXT NOT NULL,
    actor_type   TEXT NOT NULL DEFAULT '',
    actor_id     TEXT NOT NULL DEFAULT '',
    agent_id     TEXT,
    session_id   TEXT REFERENCES agent_sessions(session_id),
    task_id      TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_runtime_events_workspace_created
    ON runtime_events(workspace_id, created_at DESC, event_id DESC);
CREATE INDEX IF NOT EXISTS idx_runtime_events_workspace_type
    ON runtime_events(workspace_id, event_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_runtime_events_entity
    ON runtime_events(workspace_id, entity_type, entity_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_runtime_events_session
    ON runtime_events(workspace_id, session_id, created_at DESC)
    WHERE session_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_runtime_events_task
    ON runtime_events(workspace_id, task_id, created_at DESC)
    WHERE task_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS operator_queue_items (
    queue_id             TEXT PRIMARY KEY,
    workspace_id         TEXT NOT NULL REFERENCES workspaces(workspace_id),
    queue_key            TEXT NOT NULL,
    queue_type           TEXT NOT NULL,
    status               TEXT NOT NULL DEFAULT 'OPEN',
    title                TEXT NOT NULL,
    summary              TEXT NOT NULL DEFAULT '',
    details              TEXT NOT NULL DEFAULT '',
    assigned_to          TEXT NOT NULL DEFAULT '',
    urgency              TEXT NOT NULL DEFAULT 'NORMAL',
    source_kind          TEXT NOT NULL DEFAULT '',
    source_id            TEXT NOT NULL DEFAULT '',
    task_id              TEXT,
    session_id           TEXT REFERENCES agent_sessions(session_id),
    agent_id             TEXT,
    keep_session_active  INTEGER NOT NULL DEFAULT 0,
    due_at               TEXT,
    resolution           TEXT NOT NULL DEFAULT '',
    resolved_at          TEXT,
    resolved_by          TEXT,
    created_at           TEXT NOT NULL,
    updated_at           TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_operator_queue_workspace_key
    ON operator_queue_items(workspace_id, queue_key);
CREATE INDEX IF NOT EXISTS idx_operator_queue_workspace_status
    ON operator_queue_items(workspace_id, status, updated_at DESC, queue_id DESC);
CREATE INDEX IF NOT EXISTS idx_operator_queue_workspace_type
    ON operator_queue_items(workspace_id, queue_type, status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_operator_queue_workspace_session
    ON operator_queue_items(workspace_id, session_id, status, updated_at DESC)
    WHERE session_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_operator_queue_workspace_task
    ON operator_queue_items(workspace_id, task_id, status, updated_at DESC)
    WHERE task_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS knowledge_claims (
    claim_id             TEXT PRIMARY KEY,
    workspace_id         TEXT NOT NULL REFERENCES workspaces(workspace_id),
    claim_type           TEXT NOT NULL,
    status               TEXT NOT NULL DEFAULT 'ACTIVE',
    subject              TEXT NOT NULL,
    body                 TEXT NOT NULL,
    summary              TEXT NOT NULL DEFAULT '',
    confidence           REAL NOT NULL DEFAULT 0,
    source_kind          TEXT NOT NULL DEFAULT 'manual',
    source_id            TEXT NOT NULL DEFAULT '',
    memory_id            TEXT REFERENCES workspace_memory(memory_id),
    task_id              TEXT,
    session_id           TEXT REFERENCES agent_sessions(session_id),
    agent_id             TEXT,
    supersedes_claim_id  TEXT REFERENCES knowledge_claims(claim_id),
    conflicts_claim_id   TEXT REFERENCES knowledge_claims(claim_id),
    evidence_json        TEXT NOT NULL DEFAULT '[]',
    tags_json            TEXT NOT NULL DEFAULT '[]',
    archived_at          TEXT,
    archived_by          TEXT,
    created_at           TEXT NOT NULL,
    updated_at           TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_knowledge_claims_workspace_updated
    ON knowledge_claims(workspace_id, updated_at DESC, claim_id DESC);
CREATE INDEX IF NOT EXISTS idx_knowledge_claims_workspace_status
    ON knowledge_claims(workspace_id, status, claim_type, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_knowledge_claims_workspace_session
    ON knowledge_claims(workspace_id, session_id, updated_at DESC)
    WHERE session_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_knowledge_claims_workspace_task
    ON knowledge_claims(workspace_id, task_id, updated_at DESC)
    WHERE task_id IS NOT NULL;

CREATE VIRTUAL TABLE IF NOT EXISTS knowledge_claims_fts USING fts5(
    subject,
    body,
    summary,
    content='knowledge_claims',
    content_rowid='rowid'
);

CREATE TRIGGER IF NOT EXISTS knowledge_claims_ai AFTER INSERT ON knowledge_claims BEGIN
    INSERT INTO knowledge_claims_fts(rowid, subject, body, summary)
    VALUES (new.rowid, new.subject, new.body, new.summary);
END;

CREATE TRIGGER IF NOT EXISTS knowledge_claims_ad AFTER DELETE ON knowledge_claims BEGIN
    INSERT INTO knowledge_claims_fts(knowledge_claims_fts, rowid, subject, body, summary)
    VALUES ('delete', old.rowid, old.subject, old.body, old.summary);
END;

CREATE TRIGGER IF NOT EXISTS knowledge_claims_au AFTER UPDATE ON knowledge_claims BEGIN
    INSERT INTO knowledge_claims_fts(knowledge_claims_fts, rowid, subject, body, summary)
    VALUES ('delete', old.rowid, old.subject, old.body, old.summary);
    INSERT INTO knowledge_claims_fts(rowid, subject, body, summary)
    VALUES (new.rowid, new.subject, new.body, new.summary);
END;

INSERT INTO knowledge_claims_fts(knowledge_claims_fts) VALUES ('rebuild');

CREATE TABLE IF NOT EXISTS execution_runs (
    run_id       TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id),
    task_id      TEXT,
    session_id   TEXT REFERENCES agent_sessions(session_id),
    agent_id     TEXT,
    title        TEXT NOT NULL,
    summary      TEXT NOT NULL DEFAULT '',
    status       TEXT NOT NULL DEFAULT 'PLANNED',
    outcome      TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    closed_at    TEXT
);

CREATE INDEX IF NOT EXISTS idx_execution_runs_workspace_updated
    ON execution_runs(workspace_id, updated_at DESC, run_id DESC);
CREATE INDEX IF NOT EXISTS idx_execution_runs_workspace_status
    ON execution_runs(workspace_id, status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_execution_runs_workspace_session
    ON execution_runs(workspace_id, session_id, updated_at DESC)
    WHERE session_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS execution_steps (
    step_id          TEXT PRIMARY KEY,
    run_id           TEXT NOT NULL REFERENCES execution_runs(run_id) ON DELETE CASCADE,
    workspace_id     TEXT NOT NULL REFERENCES workspaces(workspace_id),
    parent_step_id   TEXT REFERENCES execution_steps(step_id),
    phase            TEXT NOT NULL,
    title            TEXT NOT NULL,
    summary          TEXT NOT NULL DEFAULT '',
    status           TEXT NOT NULL DEFAULT 'PENDING',
    sort_order       INTEGER NOT NULL DEFAULT 0,
    evidence_json    TEXT NOT NULL DEFAULT '[]',
    verification_json TEXT NOT NULL DEFAULT '{}',
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    completed_at     TEXT
);

CREATE INDEX IF NOT EXISTS idx_execution_steps_run_phase
    ON execution_steps(run_id, phase, sort_order, created_at, step_id);
CREATE INDEX IF NOT EXISTS idx_execution_steps_workspace_status
    ON execution_steps(workspace_id, status, updated_at DESC, step_id DESC);

CREATE TABLE IF NOT EXISTS workspace_capability_policies (
    policy_id     TEXT PRIMARY KEY,
    workspace_id  TEXT NOT NULL REFERENCES workspaces(workspace_id),
    subject_type  TEXT NOT NULL,
    subject_id    TEXT NOT NULL,
    capability    TEXT NOT NULL,
    tool_id       TEXT NOT NULL DEFAULT '*',
    effect        TEXT NOT NULL,
    reason        TEXT NOT NULL DEFAULT '',
    created_by    TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_workspace_capability_policy_match
    ON workspace_capability_policies(workspace_id, subject_type, subject_id, capability, tool_id);
CREATE INDEX IF NOT EXISTS idx_workspace_capability_policy_workspace
    ON workspace_capability_policies(workspace_id, subject_type, subject_id, updated_at DESC);
