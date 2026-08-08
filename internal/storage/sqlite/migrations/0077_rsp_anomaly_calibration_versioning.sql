ALTER TABLE rsp_anomaly_baseline ADD COLUMN calibration_profile TEXT NOT NULL DEFAULT '';
ALTER TABLE rsp_anomaly_baseline ADD COLUMN calibration_version TEXT NOT NULL DEFAULT '';

ALTER TABLE rsp_anomaly_telemetry ADD COLUMN calibration_profile TEXT NOT NULL DEFAULT '';
ALTER TABLE rsp_anomaly_telemetry ADD COLUMN calibration_version TEXT NOT NULL DEFAULT '';
