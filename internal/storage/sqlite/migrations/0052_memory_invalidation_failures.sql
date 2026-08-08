ALTER TABLE memory_invalidation_queue
    ADD COLUMN delivery_attempt_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE memory_invalidation_queue
    ADD COLUMN last_delivery_attempt_at TEXT NOT NULL DEFAULT '';

ALTER TABLE memory_invalidation_queue
    ADD COLUMN failure_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE memory_invalidation_queue
    ADD COLUMN last_failure_at TEXT NOT NULL DEFAULT '';

ALTER TABLE memory_invalidation_queue
    ADD COLUMN last_failure_reason TEXT NOT NULL DEFAULT '';

ALTER TABLE memory_invalidation_queue
    ADD COLUMN dead_lettered_at TEXT NOT NULL DEFAULT '';
