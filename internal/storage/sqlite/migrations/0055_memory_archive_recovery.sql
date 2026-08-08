-- Phase 9: Memory Archive and Recovery additions

-- Add recovery tracking to workspace memory (archived_reason already exists)
ALTER TABLE workspace_memory ADD COLUMN recovery_reason TEXT NOT NULL DEFAULT '';

-- Add recovery tracking to knowledge claims
ALTER TABLE knowledge_claims ADD COLUMN recovery_reason TEXT NOT NULL DEFAULT '';

-- Add archive/recovery reasons to the unified graph
ALTER TABLE memory_nodes ADD COLUMN archived_reason TEXT NOT NULL DEFAULT '';
ALTER TABLE memory_nodes ADD COLUMN recovery_reason TEXT NOT NULL DEFAULT '';
