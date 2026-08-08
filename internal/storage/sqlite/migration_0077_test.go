package sqlite_test

import (
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
	"testing"
)

func TestMigration0077_RSPAnomalyCalibrationVersioning(t *testing.T) {
	t.Parallel()

	store := sqlite.NewTestStore(t)
	db := store.DB()

	if _, err := db.Exec(`INSERT INTO rsp_anomaly_baseline
		(workspace_id, agent_id, task_class, mode, phase, metric_name, mu_hat, sigma_hat, sample_size, last_healthy_window_at)
		VALUES ('ws-1', 'agent-a', 'INCIDENT', 'DEFAULT', 'S1', 'verifier_fail_rate', 0.1, 0.1, 1, '2026-03-29T09:00:00Z')`); err != nil {
		t.Fatalf("insert legacy anomaly baseline: %v", err)
	}
	var baselineProfile, baselineVersion string
	if err := db.QueryRow(`SELECT calibration_profile, calibration_version FROM rsp_anomaly_baseline WHERE workspace_id = 'ws-1' AND agent_id = 'agent-a' AND metric_name = 'verifier_fail_rate'`).Scan(&baselineProfile, &baselineVersion); err != nil {
		t.Fatalf("query anomaly baseline calibration columns: %v", err)
	}
	if baselineProfile != "" || baselineVersion != "" {
		t.Fatalf("expected legacy anomaly baseline row to default to empty calibration columns, got profile=%q version=%q", baselineProfile, baselineVersion)
	}

	if _, err := db.Exec(`INSERT INTO rsp_anomaly_telemetry
		(alert_id, workspace_id, cluster_mode, metric_name, mu_hat, sigma_hat, current_value, ewma_value, source_diversity_discount, alert_type, detected_at)
		VALUES ('rspan-1', 'ws-1', 'DEFAULT', 'verifier_fail_rate', 0.1, 0.1, 0.8, 0.5, 0.8, 'THRASHING', '2026-03-30T10:00:00Z')`); err != nil {
		t.Fatalf("insert legacy anomaly telemetry: %v", err)
	}
	var logProfile, logVersion string
	if err := db.QueryRow(`SELECT calibration_profile, calibration_version FROM rsp_anomaly_telemetry WHERE alert_id = 'rspan-1'`).Scan(&logProfile, &logVersion); err != nil {
		t.Fatalf("query anomaly telemetry calibration columns: %v", err)
	}
	if logProfile != "" || logVersion != "" {
		t.Fatalf("expected legacy anomaly telemetry row to default to empty calibration columns, got profile=%q version=%q", logProfile, logVersion)
	}
}
