CREATE TABLE IF NOT EXISTS approval_requests (
  approval_id TEXT PRIMARY KEY,
  request_id TEXT NOT NULL,
  status TEXT NOT NULL,
  ttl_sec INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  decided_at TEXT,
  decided_by TEXT,
  decision_note TEXT,
  FOREIGN KEY (request_id) REFERENCES resource_requests(request_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS approval_events (
  event_id TEXT PRIMARY KEY,
  approval_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  actor_id TEXT,
  payload_json TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY (approval_id) REFERENCES approval_requests(approval_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_approvals_status_expires ON approval_requests(status, created_at);
CREATE INDEX IF NOT EXISTS idx_approvals_request_id ON approval_requests(request_id);
