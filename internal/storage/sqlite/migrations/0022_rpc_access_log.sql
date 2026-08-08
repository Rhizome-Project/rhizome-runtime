-- RPC access log: track every RPC call
CREATE TABLE IF NOT EXISTS rpc_access_log (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  method       TEXT NOT NULL,
  workspace_id TEXT DEFAULT '',
  actor        TEXT DEFAULT '',
  status       TEXT DEFAULT 'ok',    -- 'ok' or 'error'
  error_msg    TEXT DEFAULT '',
  latency_ms   INTEGER DEFAULT 0,
  created_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rpc_log_time ON rpc_access_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_rpc_log_method ON rpc_access_log(method, created_at DESC);
