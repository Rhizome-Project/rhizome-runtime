ALTER TABLE operator_queue_items ADD COLUMN revision INTEGER NOT NULL DEFAULT 1;

UPDATE operator_queue_items
   SET revision = 1
 WHERE revision IS NULL OR revision <= 0;
