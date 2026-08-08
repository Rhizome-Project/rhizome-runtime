package memory

// SQL schema for the agent memory database.
// This is a SEPARATE SQLite database from the main Rhizome store.

const createSchema = `
CREATE TABLE IF NOT EXISTS memory_entries (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now')),
    type      TEXT NOT NULL CHECK (type IN ('experience', 'reflection', 'procedure', 'entity', 'error')),
    source    TEXT NOT NULL DEFAULT '',
    topic     TEXT NOT NULL DEFAULT '',
    content   TEXT NOT NULL,
    task_id   TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_memory_entries_type_timestamp ON memory_entries(type, timestamp);
CREATE INDEX IF NOT EXISTS idx_memory_entries_task_id ON memory_entries(task_id);

CREATE VIRTUAL TABLE IF NOT EXISTS memory_entries_fts USING fts5(
    topic,
    content,
    content='memory_entries',
    content_rowid='id'
);

-- Triggers to keep FTS5 in sync with the content table.

CREATE TRIGGER IF NOT EXISTS memory_entries_ai AFTER INSERT ON memory_entries BEGIN
    INSERT INTO memory_entries_fts(rowid, topic, content)
    VALUES (new.id, new.topic, new.content);
END;

CREATE TRIGGER IF NOT EXISTS memory_entries_ad AFTER DELETE ON memory_entries BEGIN
    INSERT INTO memory_entries_fts(memory_entries_fts, rowid, topic, content)
    VALUES ('delete', old.id, old.topic, old.content);
END;

CREATE TRIGGER IF NOT EXISTS memory_entries_au AFTER UPDATE ON memory_entries BEGIN
    INSERT INTO memory_entries_fts(memory_entries_fts, rowid, topic, content)
    VALUES ('delete', old.id, old.topic, old.content);
    INSERT INTO memory_entries_fts(rowid, topic, content)
    VALUES (new.id, new.topic, new.content);
END;
`
