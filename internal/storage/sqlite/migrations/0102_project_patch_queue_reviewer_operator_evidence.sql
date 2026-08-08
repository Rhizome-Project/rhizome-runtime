ALTER TABLE project_patch_queue_items
    ADD COLUMN reviewer_advisory_schema TEXT NOT NULL DEFAULT '';

ALTER TABLE project_patch_queue_items
    ADD COLUMN reviewer_advisory_accepted INTEGER NOT NULL DEFAULT 0;

ALTER TABLE project_patch_queue_items
    ADD COLUMN reviewer_advisory_json TEXT NOT NULL DEFAULT '{}';

ALTER TABLE project_patch_queue_items
    ADD COLUMN reviewer_advisory_digest TEXT NOT NULL DEFAULT '';

ALTER TABLE project_patch_queue_items
    ADD COLUMN reviewer_recorded_by TEXT NOT NULL DEFAULT '';

ALTER TABLE project_patch_queue_items
    ADD COLUMN reviewer_recorded_at TEXT NOT NULL DEFAULT '';

ALTER TABLE project_patch_queue_items
    ADD COLUMN operator_enablement_schema TEXT NOT NULL DEFAULT '';

ALTER TABLE project_patch_queue_items
    ADD COLUMN operator_enablement_accepted INTEGER NOT NULL DEFAULT 0;

ALTER TABLE project_patch_queue_items
    ADD COLUMN operator_enablement_json TEXT NOT NULL DEFAULT '{}';

ALTER TABLE project_patch_queue_items
    ADD COLUMN operator_enablement_digest TEXT NOT NULL DEFAULT '';

ALTER TABLE project_patch_queue_items
    ADD COLUMN operator_enabled_by TEXT NOT NULL DEFAULT '';

ALTER TABLE project_patch_queue_items
    ADD COLUMN operator_enabled_at TEXT NOT NULL DEFAULT '';
