CREATE TABLE IF NOT EXISTS cluster_stewards (
    cluster_id TEXT NOT NULL,
    epoch_id TEXT NOT NULL,
    steward_agent_id TEXT NOT NULL,
    granted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at DATETIME NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('ACTIVE', 'EXPIRED', 'REVOKED')),
    PRIMARY KEY(cluster_id, epoch_id)
);

CREATE INDEX idx_cluster_stewards_active ON cluster_stewards(cluster_id, status) WHERE status = 'ACTIVE';
