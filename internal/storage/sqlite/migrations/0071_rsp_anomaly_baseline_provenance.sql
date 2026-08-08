ALTER TABLE rsp_anomaly_telemetry ADD COLUMN task_class TEXT NOT NULL DEFAULT '';
ALTER TABLE rsp_anomaly_telemetry ADD COLUMN shadow_phase TEXT NOT NULL DEFAULT '';
ALTER TABLE rsp_anomaly_telemetry ADD COLUMN baseline_scope TEXT NOT NULL DEFAULT '';
ALTER TABLE rsp_anomaly_telemetry ADD COLUMN baseline_sample_size INTEGER NOT NULL DEFAULT 0;
ALTER TABLE rsp_anomaly_telemetry ADD COLUMN baseline_last_healthy_window_at TEXT NOT NULL DEFAULT '';
