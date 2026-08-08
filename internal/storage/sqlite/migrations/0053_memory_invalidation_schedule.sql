ALTER TABLE memory_invalidation_queue
    ADD COLUMN lease_expires_at TEXT NOT NULL DEFAULT '';

ALTER TABLE memory_invalidation_queue
    ADD COLUMN next_delivery_at TEXT NOT NULL DEFAULT '';
