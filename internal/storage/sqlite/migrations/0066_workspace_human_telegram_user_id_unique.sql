DROP INDEX IF EXISTS idx_workspace_humans_telegram_user_id;

UPDATE workspace_humans
   SET telegram_user_id = NULL
 WHERE rowid IN (
   SELECT dup.rowid
     FROM workspace_humans AS dup
    WHERE dup.telegram_user_id IS NOT NULL
      AND EXISTS (
        SELECT 1
          FROM workspace_humans AS keep
         WHERE keep.workspace_id = dup.workspace_id
           AND keep.telegram_user_id = dup.telegram_user_id
           AND (
             keep.updated_at > dup.updated_at
             OR (keep.updated_at = dup.updated_at AND keep.human_id > dup.human_id)
           )
      )
 );

CREATE UNIQUE INDEX IF NOT EXISTS idx_workspace_humans_telegram_user_id
    ON workspace_humans(workspace_id, telegram_user_id)
 WHERE telegram_user_id IS NOT NULL;
