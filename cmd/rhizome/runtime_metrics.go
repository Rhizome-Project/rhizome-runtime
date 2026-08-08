package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
)

const runtimeFailureRateThreshold = 0.20

var runtimeMetricsTopLevelRequiredFieldsV1 = []string{
	"timestamp",
	"profiles",
}

var runtimeMetricsProfileFields = []string{
	"total_runs",
	"success_count",
	"failure_count",
	"timeout_count",
	"failure_rate",
	"avg_duration_sec",
	"avg_startup_ms",
}

type runtimeProfileMetrics struct {
	TotalRuns    int     `json:"total_runs"`
	SuccessCount int     `json:"success_count"`
	FailureCount int     `json:"failure_count"`
	TimeoutCount int     `json:"timeout_count"`
	FailureRate  float64 `json:"failure_rate"`
	AvgDuration  float64 `json:"avg_duration_sec"`
	AvgStartupMS float64 `json:"avg_startup_ms"`
	MinStartupMS float64 `json:"min_startup_ms,omitempty"`
	MaxStartupMS float64 `json:"max_startup_ms,omitempty"`
}

type runtimeRecoveryMetrics struct {
	TotalRecoveries int     `json:"total_recoveries"`
	Successful      int     `json:"successful"`
	Failed          int     `json:"failed"`
	AvgRecoverySec  float64 `json:"avg_recovery_time_sec"`
}

type runtimeMetricsSnapshot struct {
	SchemaVersion           string                           `json:"schema_version"`
	Timestamp               string                           `json:"timestamp"`
	Profiles                map[string]runtimeProfileMetrics `json:"profiles"`
	Recovery                runtimeRecoveryMetrics           `json:"recovery,omitempty"`
	OrphanContainersCleaned int                              `json:"orphan_containers_cleaned,omitempty"`
}

type runtimeMetricsHealth struct {
	Verdict              string   `json:"verdict"`
	ThresholdFailureRate float64  `json:"threshold_failure_rate"`
	ProfilesEvaluated    int      `json:"profiles_evaluated"`
	Reasons              []string `json:"reasons,omitempty"`
}

func runRuntime(args []string) error {
	if len(args) < 1 {
		printRuntimeUsage(os.Stderr)
		return errors.New("missing runtime subcommand")
	}

	switch args[0] {
	case "metrics":
		return runRuntimeMetrics(args[1:])
	default:
		printRuntimeUsage(os.Stderr)
		return fmt.Errorf("unknown runtime subcommand: %s", args[0])
	}
}

func runRuntimeMetrics(args []string) error {
	traceID := newTraceID()
	fs := flag.NewFlagSet("runtime metrics", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	last := fs.Int("last", 1, "Number of latest valid snapshots to return")
	format := fs.String("format", "json", "Output format: json|jsonl")
	metricsFile := fs.String("metrics-file", "", "Path to metrics JSONL file (optional)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *last <= 0 {
		return errors.New("--last must be positive")
	}

	outputFormat, err := normalizeOutputFormat(*format)
	if err != nil {
		return err
	}

	cfg := app.LoadConfig()
	metricsPath := strings.TrimSpace(*metricsFile)
	if metricsPath == "" {
		metricsPath = cfg.MetricsPath
	}
	if strings.TrimSpace(metricsPath) == "" {
		return errors.New("metrics path is empty")
	}

	snapshots, totalValid, parseErrors, err := readRuntimeMetricsSnapshots(metricsPath, *last)
	if err != nil {
		return err
	}

	var latest *runtimeMetricsSnapshot
	if len(snapshots) > 0 {
		lastSnapshot := snapshots[len(snapshots)-1]
		latest = &lastSnapshot
	}
	health := evaluateRuntimeMetricsHealth(latest)

	if outputFormat == outputFormatJSONL {
		for i, snapshot := range snapshots {
			if err := writeJSONLine(os.Stdout, map[string]any{
				"event":       "runtime_metrics_snapshot",
				"trace_id":    traceID,
				"source_path": metricsPath,
				"index":       i + 1,
				"snapshot":    snapshot,
				"ts":          time.Now().UTC().Format(time.RFC3339Nano),
			}); err != nil {
				return err
			}
		}

		latestTS := ""
		if latest != nil {
			latestTS = latest.Timestamp
		}
		return writeJSONLine(os.Stdout, map[string]any{
			"event":                 "runtime_metrics_summary",
			"trace_id":              traceID,
			"source_path":           metricsPath,
			"snapshots_loaded":      len(snapshots),
			"snapshots_total_valid": totalValid,
			"parse_errors":          parseErrors,
			"latest_timestamp":      latestTS,
			"summary":               latest,
			"health":                health,
			"ts":                    time.Now().UTC().Format(time.RFC3339Nano),
		})
	}

	return writeJSON(os.Stdout, map[string]any{
		"trace_id":              traceID,
		"source_path":           metricsPath,
		"snapshots_loaded":      len(snapshots),
		"snapshots_total_valid": totalValid,
		"parse_errors":          parseErrors,
		"summary":               latest,
		"health":                health,
	})
}

func readRuntimeMetricsSnapshots(metricsPath string, last int) ([]runtimeMetricsSnapshot, int, int, error) {
	bakPath := metricsPath + ".bak"
	validBak, valCountBak, errsBak, errBak := scanMetricsFile(bakPath, last)
	validMain, valCountMain, errsMain, errMain := scanMetricsFile(metricsPath, last)

	parseErrors := errsBak + errsMain
	totalValid := valCountBak + valCountMain

	bakMissing := os.IsNotExist(errBak)
	mainMissing := os.IsNotExist(errMain)

	if errMain != nil && !mainMissing {
		return nil, totalValid, parseErrors, fmt.Errorf("scan current metrics file: %w", errMain)
	}
	if errBak != nil && !bakMissing && mainMissing {
		return nil, totalValid, parseErrors, fmt.Errorf("scan backup metrics file: %w", errBak)
	}
	if mainMissing && bakMissing {
		return nil, 0, 0, fmt.Errorf("metrics file not found: %s", metricsPath)
	}

	valid := make([]runtimeMetricsSnapshot, 0, len(validBak)+len(validMain))
	if !bakMissing {
		valid = append(valid, validBak...)
	}
	if !mainMissing {
		valid = append(valid, validMain...)
	}
	if len(valid) > last {
		valid = valid[len(valid)-last:]
	}

	return valid, totalValid, parseErrors, nil
}

func scanMetricsFile(path string, last int) ([]runtimeMetricsSnapshot, int, int, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, 0, err
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	valid := make([]runtimeMetricsSnapshot, 0, last)
	parseErrors := 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		snapshot, err := parseRuntimeMetricsSnapshot([]byte(line))
		if err != nil {
			parseErrors++
			continue
		}
		valid = append(valid, snapshot)
	}

	if err := scanner.Err(); err != nil {
		return valid, len(valid), parseErrors, fmt.Errorf("scan metrics file: %w", err)
	}

	totalValid := len(valid)
	if totalValid > last {
		valid = valid[totalValid-last:]
	}
	return valid, totalValid, parseErrors, nil
}

func parseRuntimeMetricsSnapshot(raw []byte) (runtimeMetricsSnapshot, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return runtimeMetricsSnapshot{}, fmt.Errorf("decode metrics line: %w", err)
	}

	// 1. Determine Schema Version
	var schemaVersion string
	if rawVer, ok := envelope["schema_version"]; ok {
		// Strip quotes if any
		val := string(rawVer)
		if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
			schemaVersion = val[1 : len(val)-1]
		} else {
			schemaVersion = val
		}
	} else {
		// Native backward compatibility: Legacy snapshots imply 1.0
		schemaVersion = "1.0"
	}

	switch schemaVersion {
	case "1.0":
		return parseRuntimeMetricsSnapshotV1(raw, envelope)
	default:
		return runtimeMetricsSnapshot{}, fmt.Errorf("unsupported runtime metrics schema_version: %s", schemaVersion)
	}
}

func parseRuntimeMetricsSnapshotV1(raw []byte, envelope map[string]json.RawMessage) (runtimeMetricsSnapshot, error) {
	// 2. Validate Required Top-Level Fields
	for _, key := range runtimeMetricsTopLevelRequiredFieldsV1 {
		if _, ok := envelope[key]; !ok {
			return runtimeMetricsSnapshot{}, fmt.Errorf("missing required field: %s", key)
		}
	}

	// 3. Validate Profiles Map & Required Fields
	var rawProfiles map[string]map[string]json.RawMessage
	if err := json.Unmarshal(envelope["profiles"], &rawProfiles); err != nil {
		return runtimeMetricsSnapshot{}, fmt.Errorf("decode profiles: %w", err)
	}
	for profileName, profileMap := range rawProfiles {
		for _, key := range runtimeMetricsProfileFields {
			if _, ok := profileMap[key]; !ok {
				return runtimeMetricsSnapshot{}, fmt.Errorf("profile %q missing required field: %s", profileName, key)
			}
		}
	}

	// 4. Extract Safe Snapshot
	var snapshot runtimeMetricsSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return runtimeMetricsSnapshot{}, fmt.Errorf("decode metrics snapshot: %w", err)
	}

	snapshot.SchemaVersion = "1.0" // normalize if missing
	if strings.TrimSpace(snapshot.Timestamp) == "" {
		return runtimeMetricsSnapshot{}, errors.New("snapshot timestamp is empty")
	}
	if snapshot.Profiles == nil {
		snapshot.Profiles = map[string]runtimeProfileMetrics{}
	}
	return snapshot, nil
}

func evaluateRuntimeMetricsHealth(snapshot *runtimeMetricsSnapshot) runtimeMetricsHealth {
	if snapshot == nil {
		return runtimeMetricsHealth{
			Verdict:              "unknown",
			ThresholdFailureRate: runtimeFailureRateThreshold,
		}
	}

	profileNames := make([]string, 0, len(snapshot.Profiles))
	for name := range snapshot.Profiles {
		profileNames = append(profileNames, name)
	}
	sort.Strings(profileNames)

	reasons := make([]string, 0)
	profilesEvaluated := 0
	for _, profileName := range profileNames {
		profile := snapshot.Profiles[profileName]
		if profile.TotalRuns <= 0 {
			continue
		}
		profilesEvaluated++
		if profile.FailureRate > runtimeFailureRateThreshold {
			reasons = append(
				reasons,
				fmt.Sprintf(
					"profile=%s failure_rate=%.4f exceeds threshold=%.2f",
					profileName,
					profile.FailureRate,
					runtimeFailureRateThreshold,
				),
			)
		}
	}

	verdict := "healthy"
	if len(reasons) > 0 {
		verdict = "degraded"
	}
	if profilesEvaluated == 0 {
		verdict = "unknown"
	}

	return runtimeMetricsHealth{
		Verdict:              verdict,
		ThresholdFailureRate: runtimeFailureRateThreshold,
		ProfilesEvaluated:    profilesEvaluated,
		Reasons:              reasons,
	}
}
