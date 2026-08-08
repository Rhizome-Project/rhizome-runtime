-- migration 0027: Add Phase 8 memory metrics columns

ALTER TABLE memory_access_stats ADD COLUMN dissent_hit_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE memory_access_stats ADD COLUMN dissent_available_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE memory_access_stats ADD COLUMN pollution_count INTEGER NOT NULL DEFAULT 0;
