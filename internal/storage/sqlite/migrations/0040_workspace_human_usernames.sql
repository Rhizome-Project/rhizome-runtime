ALTER TABLE workspace_humans ADD COLUMN username TEXT NOT NULL DEFAULT '';
ALTER TABLE workspace_humans ADD COLUMN username_norm TEXT NOT NULL DEFAULT '';

UPDATE workspace_humans
   SET username = CASE
           WHEN TRIM(COALESCE(username, '')) = '' THEN display_name
           ELSE username
       END,
       username_norm = CASE
           WHEN TRIM(COALESCE(username_norm, '')) = '' THEN display_name_norm
           ELSE username_norm
       END;

CREATE UNIQUE INDEX IF NOT EXISTS idx_workspace_humans_username
    ON workspace_humans(workspace_id, username_norm);
