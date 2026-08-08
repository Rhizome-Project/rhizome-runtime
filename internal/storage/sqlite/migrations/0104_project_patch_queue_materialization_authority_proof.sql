ALTER TABLE project_patch_queue_items
    ADD COLUMN materialization_authority_proof_json TEXT NOT NULL DEFAULT '{}';

ALTER TABLE project_patch_queue_items
    ADD COLUMN materialization_authority_proof_digest TEXT NOT NULL DEFAULT '';
