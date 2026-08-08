package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadRuntimeMetricsSnapshots_LastAndParseErrors(t *testing.T) {
	fixture := runtimeMetricsFixturePath(t)

	snapshots, totalValid, parseErrors, err := readRuntimeMetricsSnapshots(fixture, 1)
	if err != nil {
		t.Fatalf("readRuntimeMetricsSnapshots failed: %v", err)
	}

	if totalValid != 2 {
		t.Fatalf("expected totalValid=2, got %d", totalValid)
	}
	if parseErrors != 1 {
		t.Fatalf("expected parseErrors=1, got %d", parseErrors)
	}
	if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot due to --last behavior, got %d", len(snapshots))
	}
	if snapshots[0].Timestamp != "2026-03-12T12:05:00Z" {
		t.Fatalf("expected latest snapshot timestamp, got %q", snapshots[0].Timestamp)
	}
}

func TestRunRuntimeMetrics_JSONOutput(t *testing.T) {
	fixture := runtimeMetricsFixturePath(t)

	out, err := captureStdout(t, func() error {
		return runRuntime([]string{
			"metrics",
			"--last", "1",
			"--metrics-file", fixture,
		})
	})
	if err != nil {
		t.Fatalf("runRuntime metrics failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("decode json output: %v; output=%q", err, out)
	}

	if got := int(payload["snapshots_total_valid"].(float64)); got != 2 {
		t.Fatalf("expected snapshots_total_valid=2, got %d", got)
	}
	if got := int(payload["snapshots_loaded"].(float64)); got != 1 {
		t.Fatalf("expected snapshots_loaded=1, got %d", got)
	}
	if got := int(payload["parse_errors"].(float64)); got != 1 {
		t.Fatalf("expected parse_errors=1, got %d", got)
	}

	summary, ok := payload["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary object in output")
	}
	if got, _ := summary["timestamp"].(string); got != "2026-03-12T12:05:00Z" {
		t.Fatalf("expected summary.timestamp latest value, got %q", got)
	}

	health, ok := payload["health"].(map[string]any)
	if !ok {
		t.Fatalf("expected health object in output")
	}
	if got, _ := health["verdict"].(string); got != "healthy" {
		t.Fatalf("expected health verdict healthy, got %q", got)
	}
}

func TestRunRuntimeMetrics_JSONLOutput(t *testing.T) {
	fixture := runtimeMetricsFixturePath(t)

	out, err := captureStdout(t, func() error {
		return runRuntime([]string{
			"metrics",
			"--last", "2",
			"--format", "jsonl",
			"--metrics-file", fixture,
		})
	})
	if err != nil {
		t.Fatalf("runRuntime metrics jsonl failed: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 jsonl lines (2 snapshots + summary), got %d", len(lines))
	}

	var first map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("decode first line: %v", err)
	}
	if got, _ := first["event"].(string); got != "runtime_metrics_snapshot" {
		t.Fatalf("expected runtime_metrics_snapshot event, got %q", got)
	}

	var summary map[string]any
	if err := json.Unmarshal([]byte(lines[2]), &summary); err != nil {
		t.Fatalf("decode summary line: %v", err)
	}
	if got, _ := summary["event"].(string); got != "runtime_metrics_summary" {
		t.Fatalf("expected runtime_metrics_summary event, got %q", got)
	}
	if got := int(summary["snapshots_total_valid"].(float64)); got != 2 {
		t.Fatalf("expected snapshots_total_valid=2, got %d", got)
	}
	if got := int(summary["parse_errors"].(float64)); got != 1 {
		t.Fatalf("expected parse_errors=1, got %d", got)
	}
	health, ok := summary["health"].(map[string]any)
	if !ok {
		t.Fatalf("expected health object in summary line")
	}
	if got, _ := health["verdict"].(string); got != "healthy" {
		t.Fatalf("expected health verdict healthy, got %q", got)
	}
}

func TestRunRuntimeMetrics_FileNotFound(t *testing.T) {
	err := runRuntime([]string{
		"metrics",
		"--metrics-file", filepath.Join(t.TempDir(), "missing.jsonl"),
	})
	if err == nil {
		t.Fatalf("expected file-not-found error")
	}
	if !strings.Contains(err.Error(), "metrics file not found") {
		t.Fatalf("expected metrics file not found error, got %v", err)
	}
}

func runtimeMetricsFixturePath(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	repoRoot := filepath.Clean(filepath.Join(wd, "..", ".."))
	fixture := filepath.Join(repoRoot, "tests", "fixtures", "runtime_metrics_sample.jsonl")
	if _, err := os.Stat(fixture); err != nil {
		t.Fatalf("metrics fixture not found: %v", err)
	}
	return fixture
}

func TestParseRuntimeMetricsSnapshot_LegacyV0Compatibility(t *testing.T) {
	// Snapshot without schema_version
	raw := []byte(`{
		"timestamp": "2026-04-07T12:00:00Z",
		"profiles": {
			"compute": {
				"total_runs": 1,
				"success_count": 1,
				"failure_count": 0,
				"timeout_count": 0,
				"failure_rate": 0.0,
				"avg_duration_sec": 1.2,
				"avg_startup_ms": 100.5
			}
		},
		"recovery": {"total_recoveries": 0, "successful": 0, "failed": 0, "avg_recovery_time_sec": 0},
		"orphan_containers_cleaned": 5
	}`)

	snap, err := parseRuntimeMetricsSnapshot(raw)
	if err != nil {
		t.Fatalf("unexpected error parsing legacy V0: %v", err)
	}

	if snap.SchemaVersion != "1.0" {
		t.Errorf("expected SchemaVersion to default to 1.0, got %s", snap.SchemaVersion)
	}
	if snap.OrphanContainersCleaned != 5 {
		t.Errorf("expected 5 orphans cleaned, got %d", snap.OrphanContainersCleaned)
	}
}

func TestParseRuntimeMetricsSnapshot_OptionalFields(t *testing.T) {
	// Snapshot with schema_version but missing 'recovery' and 'orphan_containers_cleaned'
	raw := []byte(`{
		"schema_version": "1.0",
		"timestamp": "2026-04-07T12:00:00Z",
		"profiles": {
			"compute": {
				"total_runs": 1,
				"success_count": 1,
				"failure_count": 0,
				"timeout_count": 0,
				"failure_rate": 0.0,
				"avg_duration_sec": 1.2,
				"avg_startup_ms": 100.5
			}
		}
	}`)

	snap, err := parseRuntimeMetricsSnapshot(raw)
	if err != nil {
		t.Fatalf("unexpected error parsing fields with optional omissions: %v", err)
	}

	if snap.SchemaVersion != "1.0" {
		t.Errorf("expected schema_version 1.0, got %s", snap.SchemaVersion)
	}
	if snap.OrphanContainersCleaned != 0 {
		t.Errorf("expected 0 orphans cleaned (default), got %d", snap.OrphanContainersCleaned)
	}
}

func TestParseRuntimeMetricsSnapshot_MissingRequired(t *testing.T) {
	// Missing 'profiles'
	raw := []byte(`{
		"schema_version": "1.0",
		"timestamp": "2026-04-07T12:00:00Z"
	}`)

	_, err := parseRuntimeMetricsSnapshot(raw)
	if err == nil {
		t.Fatal("expected error parsing snapshot missing 'profiles'")
	}
	if !strings.Contains(err.Error(), "missing required field: profiles") {
		t.Errorf("expected missing profiles error, got %v", err)
	}
}

func TestParseRuntimeMetricsSnapshot_UnsupportedVersion(t *testing.T) {
	raw := []byte(`{
		"schema_version": "99.0",
		"timestamp": "2099-04-07T12:00:00Z",
		"profiles": {}
	}`)

	_, err := parseRuntimeMetricsSnapshot(raw)
	if err == nil {
		t.Fatal("expected error parsing unsupported schema_version")
	}
	if !strings.Contains(err.Error(), "unsupported runtime metrics schema_version") {
		t.Errorf("expected unsupported schema_version error, got %v", err)
	}
}

// ---------- Rotation Behavior Tests ----------

func TestReadRuntimeMetricsSnapshots_Rotation_Spanning(t *testing.T) {
	// 1. Setup temp test files
	tmp := t.TempDir()
	mainPath := filepath.Join(tmp, "metrics.jsonl")
	bakPath := mainPath + ".bak"

	// 2. Write 3 lines to backups, and 1 to main
	bakContent := `{"schema_version":"1.0", "timestamp":"T1", "profiles":{}}
{"schema_version":"1.0", "timestamp":"T2", "profiles":{}}
{"schema_version":"1.0", "timestamp":"T3", "profiles":{}}`
	os.WriteFile(bakPath, []byte(bakContent), 0644)

	mainContent := `{"schema_version":"1.0", "timestamp":"T4", "profiles":{}}`
	os.WriteFile(mainPath, []byte(mainContent), 0644)

	// Fetch last 2
	snapshots, validCount, parseErrs, err := readRuntimeMetricsSnapshots(mainPath, 2)
	if err != nil {
		t.Fatalf("unexpected error reading spanned metrics: %v", err)
	}

	if parseErrs != 0 {
		t.Errorf("expected 0 parse errors, got %d", parseErrs)
	}
	if validCount != 4 {
		t.Errorf("expected total valid counter across both files to be 4, got %d", validCount)
	}
	if len(snapshots) != 2 {
		t.Fatalf("expected 2 snapshots returned, got %d", len(snapshots))
	}

	// Should yield T3 (from bak) and T4 (from main) in chronological order
	if snapshots[0].Timestamp != "T3" {
		t.Errorf("expected first snapshot to be T3, got %s", snapshots[0].Timestamp)
	}
	if snapshots[1].Timestamp != "T4" {
		t.Errorf("expected second snapshot to be T4, got %s", snapshots[1].Timestamp)
	}
}

func TestReadRuntimeMetricsSnapshots_Rotation_OnlyBackupExists(t *testing.T) {
	tmp := t.TempDir()
	mainPath := filepath.Join(tmp, "metrics.jsonl")
	bakPath := mainPath + ".bak"

	// Only backup exists (e.g. rotated but main not flushed yet)
	bakContent := `{"schema_version":"1.0", "timestamp":"T1", "profiles":{}}`
	os.WriteFile(bakPath, []byte(bakContent), 0644)

	snapshots, _, errs, err := readRuntimeMetricsSnapshots(mainPath, 5)
	if err != nil {
		t.Fatalf("unexpected error reading when only backup exists: %v", err)
	}
	if errs != 0 {
		t.Errorf("parse errors: %d", errs)
	}
	if len(snapshots) != 1 || snapshots[0].Timestamp != "T1" {
		t.Errorf("expected 1 snapshot (T1), got %d", len(snapshots))
	}
}

func TestReadRuntimeMetricsSnapshots_Rotation_MissingBoth(t *testing.T) {
	tmp := t.TempDir()
	mainPath := filepath.Join(tmp, "metrics.jsonl")

	_, _, _, err := readRuntimeMetricsSnapshots(mainPath, 5)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "metrics file not found") {
		t.Errorf("expected missing file error, got %v", err)
	}
}

func TestReadRuntimeMetricsSnapshots_MainOpenErrorNotMaskedWhenBackupMissing(t *testing.T) {
	tmp := t.TempDir()
	mainPath := filepath.Join(tmp, "metrics.jsonl")
	if err := os.Mkdir(mainPath, 0o755); err != nil {
		t.Fatalf("mkdir metrics path: %v", err)
	}

	_, _, _, err := readRuntimeMetricsSnapshots(mainPath, 5)
	if err == nil {
		t.Fatal("expected scan error for current metrics path")
	}
	if strings.Contains(err.Error(), "metrics file not found") {
		t.Fatalf("expected current-file error, got %v", err)
	}
}

// ---------- Degraded-State Semantics Tests ----------

func TestEvaluateRuntimeMetricsHealth_Semantics(t *testing.T) {
	tests := []struct {
		name     string
		snapshot *runtimeMetricsSnapshot
		expected string
	}{
		{
			name:     "nil snapshot",
			snapshot: nil,
			expected: "unknown",
		},
		{
			name: "no profiles evaluated",
			snapshot: &runtimeMetricsSnapshot{
				Profiles: map[string]runtimeProfileMetrics{
					"compute": {TotalRuns: 0, FailureRate: 1.0}, // ignored if 0 runs
				},
			},
			expected: "unknown",
		},
		{
			name: "healthy profile",
			snapshot: &runtimeMetricsSnapshot{
				Profiles: map[string]runtimeProfileMetrics{
					"compute": {TotalRuns: 10, FailureRate: 0.10},
				},
			},
			expected: "healthy",
		},
		{
			name: "degraded profile",
			snapshot: &runtimeMetricsSnapshot{
				Profiles: map[string]runtimeProfileMetrics{
					"compute":  {TotalRuns: 10, FailureRate: 0.10},
					"firehose": {TotalRuns: 100, FailureRate: 0.50}, // exceeds 0.20
				},
			},
			expected: "degraded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			health := evaluateRuntimeMetricsHealth(tt.snapshot)
			if health.Verdict != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, health.Verdict)
			}
		})
	}
}

func TestCollectServiceMetricsSummary_DegradedOnParseError(t *testing.T) {
	// Write a file with a corrupted line at the tail
	tmp := t.TempDir()
	path := filepath.Join(tmp, "metrics.jsonl")

	content := `{"schema_version":"1.0", "timestamp":"2026-04-07T00:00:00Z", "profiles":{"c":{"total_runs":1,"success_count":1,"failure_count":0,"timeout_count":0,"failure_rate":0,"avg_duration_sec":0,"avg_startup_ms":0}}}
{corrupted_json_line_totally_broken}`
	os.WriteFile(path, []byte(content), 0644)

	// evaluate the summary using the public accessor mapped by health.go
	summary := collectServiceMetricsSummary(path)

	// Corrupted line causes parse error
	if summary.ParseErrors != 1 {
		t.Errorf("expected 1 parse error, got %d", summary.ParseErrors)
	}

	// Overall metrics summary should be formally 'degraded'
	if summary.Status != "degraded" {
		t.Errorf("expected metrics summary to be degraded on parse error, got %s", summary.Status)
	}

	// Health verdict of the parsed valid snapshot should be healthy
	if summary.Health.Verdict != "healthy" {
		t.Errorf("expected internal snapshot health to remain healthy, got %s", summary.Health.Verdict)
	}
}

func TestCollectServiceMetricsSummary_MissingFile(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "missing_metrics.jsonl")

	summary := collectServiceMetricsSummary(path)

	if summary.Status != "missing" {
		t.Errorf("expected metrics summary to be 'missing' when file is absent, got %q", summary.Status)
	}
}

func TestCollectServiceMetricsSummary_UnknownOnNoEvaluableProfiles(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "metrics.jsonl")

	content := `{"schema_version":"1.0","timestamp":"2026-04-11T00:00:00Z","profiles":{"compute":{"total_runs":0,"success_count":0,"failure_count":0,"timeout_count":0,"failure_rate":0.0,"avg_duration_sec":0.0,"avg_startup_ms":0}}}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write metrics file: %v", err)
	}

	summary := collectServiceMetricsSummary(path)
	if summary.Status != "ok" {
		t.Fatalf("expected metrics summary to stay ok while idle metrics have no evaluable profiles, got %q", summary.Status)
	}
	if summary.Health.Verdict != "unknown" {
		t.Fatalf("expected internal health verdict unknown, got %q", summary.Health.Verdict)
	}
}

func TestCollectServiceMetricsSummary_EmptyExistingFileStaysOK(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "metrics.jsonl")
	if err := os.WriteFile(path, []byte(""), 0o644); err != nil {
		t.Fatalf("write metrics file: %v", err)
	}

	summary := collectServiceMetricsSummary(path)
	if summary.Status != "ok" {
		t.Fatalf("expected empty metrics file to remain ok until real samples appear, got %q", summary.Status)
	}
	if summary.Health.Verdict != "unknown" {
		t.Fatalf("expected empty metrics file to report unknown internal verdict, got %q", summary.Health.Verdict)
	}
	if summary.SnapshotsTotalValid != 0 || summary.SnapshotsLoaded != 0 || summary.ParseErrors != 0 {
		t.Fatalf("expected empty metrics file to stay empty without parse errors, got %+v", summary)
	}
}
