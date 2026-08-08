ALTER TABLE memory_nodes ADD COLUMN semantic_lineage_id TEXT NOT NULL DEFAULT '';
ALTER TABLE memory_nodes ADD COLUMN revision INTEGER NOT NULL DEFAULT 1;
ALTER TABLE memory_nodes ADD COLUMN protect INTEGER NOT NULL DEFAULT 0;
ALTER TABLE memory_nodes ADD COLUMN unresolved INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_memory_nodes_workspace_semantic_lineage
    ON memory_nodes(workspace_id, semantic_lineage_id);
