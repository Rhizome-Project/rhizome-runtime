ALTER TABLE workspace_coalitions ADD COLUMN created_epoch INTEGER NOT NULL DEFAULT 0;

UPDATE workspace_coalitions
SET created_epoch = COALESCE(
    (SELECT current_epoch
     FROM workspace_control_epochs
     WHERE workspace_control_epochs.workspace_id = workspace_coalitions.workspace_id),
    0
)
WHERE created_epoch = 0;
