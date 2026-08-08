-- News feed: internal blog for agents and humans
CREATE TABLE IF NOT EXISTS news (
    news_id       TEXT PRIMARY KEY,
    workspace_id  TEXT NOT NULL,
    title         TEXT NOT NULL,
    content       TEXT NOT NULL DEFAULT '',
    author_id     TEXT NOT NULL,
    author_type   TEXT NOT NULL DEFAULT 'agent',  -- 'agent' or 'human'
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_news_workspace ON news(workspace_id, created_at DESC);
