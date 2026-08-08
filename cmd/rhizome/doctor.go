package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
	"github.com/Rhizome-Project/rhizome-runtime/internal/repoauthority"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

const (
	doctorStatusPass = "informational"
	doctorStatusWarn = "warn"
	doctorStatusFail = "critical"
)

var defaultDeploymentAgentIDs = []string{"alpha", "beta", "gamma"}

// canonicalDeploymentAgentIDs is kept as a compatibility alias for older checks and tests.
var canonicalDeploymentAgentIDs = defaultDeploymentAgentIDs

type doctorCheck struct {
	Name    string         `json:"name"`
	Status  string         `json:"status"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

type doctorReport struct {
	TraceID   string         `json:"trace_id"`
	Verdict   string         `json:"verdict"`
	Config    configSnapshot `json:"config"`
	Checks    []doctorCheck  `json:"checks"`
	Generated string         `json:"generated_at"`
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func runDoctor(args []string) error {
	for _, arg := range args {
		if strings.SplitN(strings.TrimSpace(arg), "=", 2)[0] == "--token" {
			return errors.New("--token is not supported because command-line secrets can leak through shell history and process listings; use RHIZOME_TOKEN")
		}
	}
	traceID := newTraceID()
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	format := fs.String("format", "json", "Output format: json|jsonl")
	metricsFile := fs.String("metrics-file", "", "Path to metrics JSONL file (optional)")
	dbPathOverride := fs.String("db-path", "", "Override DB path for strict preflight validation")
	healthURL := fs.String("health-url", "", "Optional live service health endpoint to compare against the current checkout")
	failOnWarn := fs.Bool("fail-on-warn", false, "Return a non-zero exit when doctor reports warnings")
	strictPreflight := fs.Bool("strict-preflight", false, "Fail on warnings during deployment/topology preflight")
	mode := fs.String("mode", "", "Optional doctor mode: deployment")
	expectedAgents := fs.String("expected-agents", "", "Comma separated expected agent IDs for deployment preflight")
	if err := fs.Parse(args); err != nil {
		return err
	}

	outputFormat, err := normalizeOutputFormat(*format)
	if err != nil {
		return err
	}
	doctorMode, err := normalizeDoctorMode(*mode)
	if err != nil {
		return err
	}
	strictPreflightValue := *strictPreflight || doctorMode == "deployment"
	expectedDeploymentAgentIDs := resolveExpectedDeploymentAgentIDs(*expectedAgents)

	cfg := app.LoadConfig()
	if trimmed := strings.TrimSpace(*dbPathOverride); trimmed != "" {
		cfg.DBPath = trimmed
	}
	report := collectDoctorReportWithExpected(traceID, cfg, strings.TrimSpace(*metricsFile), strings.TrimSpace(*healthURL), strings.TrimSpace(os.Getenv("RHIZOME_TOKEN")), strictPreflightValue, expectedDeploymentAgentIDs)

	if outputFormat == outputFormatJSONL {
		for _, check := range report.Checks {
			if err := writeJSONLine(os.Stdout, map[string]any{
				"event":    "doctor_check",
				"trace_id": traceID,
				"check":    check,
				"ts":       time.Now().UTC().Format(time.RFC3339Nano),
			}); err != nil {
				return err
			}
		}
		if err := writeJSONLine(os.Stdout, map[string]any{
			"event":    "doctor_summary",
			"trace_id": traceID,
			"verdict":  report.Verdict,
			"config":   report.Config,
			"ts":       time.Now().UTC().Format(time.RFC3339Nano),
		}); err != nil {
			return err
		}
		return doctorGateError(report.Verdict, *failOnWarn || strictPreflightValue)
	}

	if err := writeJSON(os.Stdout, report); err != nil {
		return err
	}
	return doctorGateError(report.Verdict, *failOnWarn || strictPreflightValue)
}

func collectDoctorReport(traceID string, cfg app.Config, metricsOverride string, healthURL string, token string, strictPreflight bool) doctorReport {
	return collectDoctorReportWithExpected(traceID, cfg, metricsOverride, healthURL, token, strictPreflight, nil)
}

func collectDoctorReportWithExpected(traceID string, cfg app.Config, metricsOverride string, healthURL string, token string, strictPreflight bool, expectedDeploymentAgentIDs []string) doctorReport {
	checks := make([]doctorCheck, 0, 12)
	metricsPath := metricsOverride
	if metricsPath == "" {
		metricsPath = cfg.MetricsPath
	}
	localCheckout := app.CurrentGitCheckoutInfo()

	checks = append(checks, checkPathConfigured("db_path", cfg.DBPath))
	checks = append(checks, checkPathParent("db_parent_dir", cfg.DBPath))
	checks = append(checks, checkExistingSQLiteDB(cfg.DBPath))
	checks = append(checks, checkWorkspaceRoot(cfg.WorkspaceRoot))
	checks = append(checks, checkExecutorPython(cfg.ExecutorPython))
	checks = append(checks, checkExecutorBridge(cfg.ExecutorBridgeScript))
	checks = append(checks, checkRuntimeMetrics(metricsPath))
	checks = append(checks, checkRuntimeBuildInfo())
	checks = append(checks, checkCurrentCheckout(localCheckout))
	if strictPreflight {
		checks = append(checks, checkStrictPreflightInputs(healthURL))
		checks = append(checks, checkDeploymentAgentRosterWithExpected(cfg.DBPath, expectedDeploymentAgentIDs))
	}
	if healthURL != "" {
		healthCheck := checkServeHealth(healthURL, token, cfg, localCheckout)
		checks = append(checks, healthCheck)

		// Extract loop readiness from live diagnostics if available
		if healthCheck.Details != nil {
			checks = append(checks, checkLoopReadinessFromDetails(healthCheck.Details))
			checks = append(checks, checkDurableRuntimeFromDetails(healthCheck.Details))
			checks = append(checks, checkDaemonPromptCompilerConvergenceFromDetails(healthCheck.Details))
			if strictPreflight {
				checks = append(checks, checkPromptAuthorityScopeFromDetails(healthCheck.Details))
			}
			checks = append(checks, checkExtendedReadinessFromDetails(healthCheck.Details))
			checks = append(checks, checkBudgetLedgerFromDetails(healthCheck.Details))
			checks = append(checks, checkAuthorityNodeFromDetails(healthCheck.Details))
			checks = append(checks, checkAuthorityLeaseFromDetails(healthCheck.Details))
			checks = append(checks, checkRuntimeWorkGateFromDetails(healthCheck.Details))
			checks = append(checks, checkProjectPatchQueueDurabilityFromDetails(healthCheck.Details))
			checks = append(checks, checkRepoMutationActuatorHealthFromDetails(healthCheck.Details))
			checks = append(checks, checkRepoMutationActivationFromDetails(healthCheck.Details))
			checks = append(checks, checkRepoMutationActuatorDryRunFromDetails(healthCheck.Details))
			checks = append(checks, checkTopLevelSemanticsFromDetails(healthCheck.Details)...)
			if strictPreflight && healthCheck.Status == doctorStatusPass {
				checks = append(checks, checkTopologyDriftFromDetailsWithExpected(healthCheck.Details, cfg.DBPath, expectedDeploymentAgentIDs))
			}
		}
	} else {
		checks = append(checks, checkDurableRuntimeFromDB(cfg.DBPath))
		checks = append(checks, checkDaemonPromptCompilerConvergenceFromDB(cfg.DBPath))
	}

	verdict := doctorStatusPass
	for _, check := range checks {
		switch check.Status {
		case doctorStatusFail:
			verdict = doctorStatusFail
			goto done
		case doctorStatusWarn:
			if verdict != doctorStatusFail {
				verdict = doctorStatusWarn
			}
		}
	}

done:
	return doctorReport{
		TraceID:   traceID,
		Verdict:   verdict,
		Config:    snapshotConfig(cfg),
		Checks:    checks,
		Generated: time.Now().UTC().Format(time.RFC3339Nano),
	}
}

func normalizeDoctorMode(raw string) (string, error) {
	mode := strings.ToLower(strings.TrimSpace(raw))
	switch mode {
	case "", "default":
		return "", nil
	case "deployment":
		return mode, nil
	default:
		return "", fmt.Errorf("unsupported --mode value: %s", raw)
	}
}

func resolveExpectedDeploymentAgentIDs(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		raw = os.Getenv("RHIZOME_DEPLOYMENT_EXPECTED_AGENTS")
	}
	ids := splitExpectedAgentIDs(raw)
	if len(ids) == 0 {
		ids = defaultDeploymentAgentIDs
	}
	return append([]string(nil), ids...)
}

func splitExpectedAgentIDs(raw string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, part := range strings.Split(raw, ",") {
		id := strings.TrimSpace(part)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func checkStrictPreflightInputs(healthURL string) doctorCheck {
	if strings.TrimSpace(healthURL) == "" {
		return doctorCheck{
			Name:    "strict_preflight_inputs",
			Status:  doctorStatusFail,
			Message: "strict preflight requires --health-url for authenticated diagnostics",
		}
	}
	return doctorCheck{
		Name:    "strict_preflight_inputs",
		Status:  doctorStatusPass,
		Message: "strict preflight inputs are explicit",
	}
}

func checkTopologyDriftFromDetails(details map[string]any, dbPath string) doctorCheck {
	return checkTopologyDriftFromDetailsWithExpected(details, dbPath, nil)
}

func checkTopologyDriftFromDetailsWithExpected(details map[string]any, dbPath string, expectedDeploymentAgentIDs []string) doctorCheck {
	status := doctorStatusPass
	reasons := make([]string, 0, 4)
	sections := make(map[string]any)

	if payload, ok := serviceHealthPayloadFromDetails(details); ok {
		if len(payload.LoopReadiness) > 0 {
			for _, loop := range payload.LoopReadiness {
				if loop.Name != loopNameDaemon {
					continue
				}
				daemonState := strings.ToLower(strings.TrimSpace(string(loop.State)))
				sections["daemon_loop"] = map[string]any{
					"state":    loop.State,
					"restarts": loop.Restarts,
				}
				if daemonState == string(LoopRunning) {
					status = doctorStatusFail
					reasons = append(reasons, fmt.Sprintf("daemon loop must be disabled for first deployment, got %s", daemonState))
				}
			}
		}

		stuckState := strings.ToLower(strings.TrimSpace(payload.Extended.StuckAgents.State))
		if stuckState == "degraded" || stuckState == "error" {
			status = doctorStatusFail
			reasons = append(reasons, strings.TrimSpace(payload.Extended.StuckAgents.Message))
			if reasons[len(reasons)-1] == "" {
				reasons[len(reasons)-1] = fmt.Sprintf("stuck_agents reported %s", stuckState)
			}
			sections["stuck_agents"] = payload.Extended.StuckAgents
		}
	}

	rosterCheck := checkDeploymentAgentRosterWithExpected(dbPath, expectedDeploymentAgentIDs)
	if rosterCheck.Status == doctorStatusFail {
		status = doctorStatusFail
		reasons = append(reasons, rosterCheck.Message)
		if rosterCheck.Details != nil {
			sections["agent_roster"] = rosterCheck.Details
		}
	}

	if len(reasons) == 0 {
		return doctorCheck{
			Name:    "topology_drift",
			Status:  doctorStatusPass,
			Message: "first-deployment topology drift checks passed",
			Details: sections,
		}
	}

	return doctorCheck{
		Name:    "topology_drift",
		Status:  status,
		Message: fmt.Sprintf("first-deployment topology drift detected: %s", strings.Join(reasons, "; ")),
		Details: sections,
	}
}

func checkDeploymentAgentRoster(dbPath string) doctorCheck {
	return checkDeploymentAgentRosterWithExpected(dbPath, nil)
}

func checkDeploymentAgentRosterWithExpected(dbPath string, expectedDeploymentAgentIDs []string) doctorCheck {
	if len(expectedDeploymentAgentIDs) == 0 {
		expectedDeploymentAgentIDs = defaultDeploymentAgentIDs
	}
	dbPath = strings.TrimSpace(dbPath)
	if dbPath == "" {
		return doctorCheck{
			Name:    "agent_roster",
			Status:  doctorStatusFail,
			Message: "first-deployment agent roster cannot be validated; db path is empty",
		}
	}

	if _, err := os.Stat(dbPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return doctorCheck{
				Name:    "agent_roster",
				Status:  doctorStatusFail,
				Message: "first-deployment agent roster cannot be validated; db file is missing",
				Details: map[string]any{
					"path": dbPath,
				},
			}
		}
		return doctorCheck{
			Name:    "agent_roster",
			Status:  doctorStatusFail,
			Message: fmt.Sprintf("agent roster check failed to stat db: %v", err),
			Details: map[string]any{
				"path": dbPath,
			},
		}
	}

	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		return doctorCheck{
			Name:    "agent_roster",
			Status:  doctorStatusFail,
			Message: fmt.Sprintf("agent roster check failed to open db: %v", err),
			Details: map[string]any{
				"path": dbPath,
			},
		}
	}
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := store.DB().QueryContext(ctx, `SELECT workspace_id FROM workspaces WHERE status = 'ACTIVE' ORDER BY workspace_id`)
	if err != nil {
		return doctorCheck{
			Name:    "agent_roster",
			Status:  doctorStatusFail,
			Message: fmt.Sprintf("agent roster query failed: %v", err),
			Details: map[string]any{
				"path": dbPath,
			},
		}
	}
	defer rows.Close()

	activeWorkspaces := make([]string, 0, 2)
	for rows.Next() {
		var workspaceID string
		if err := rows.Scan(&workspaceID); err != nil {
			return doctorCheck{
				Name:    "agent_roster",
				Status:  doctorStatusFail,
				Message: fmt.Sprintf("agent roster scan failed: %v", err),
				Details: map[string]any{
					"path": dbPath,
				},
			}
		}
		activeWorkspaces = append(activeWorkspaces, strings.TrimSpace(workspaceID))
	}
	if err := rows.Err(); err != nil {
		return doctorCheck{
			Name:    "agent_roster",
			Status:  doctorStatusFail,
			Message: fmt.Sprintf("agent roster iteration failed: %v", err),
			Details: map[string]any{
				"path": dbPath,
			},
		}
	}

	if len(activeWorkspaces) != 1 {
		return doctorCheck{
			Name:   "agent_roster",
			Status: doctorStatusFail,
			Message: fmt.Sprintf("first-deployment topology requires exactly one active workspace, got %d: %s",
				len(activeWorkspaces), strings.Join(activeWorkspaces, ", ")),
			Details: map[string]any{
				"path":              dbPath,
				"active_workspaces": activeWorkspaces,
			},
		}
	}

	agents, err := store.ListWorkspaceAgents(ctx, activeWorkspaces[0])
	if err != nil {
		return doctorCheck{
			Name:    "agent_roster",
			Status:  doctorStatusFail,
			Message: fmt.Sprintf("agent roster list failed: %v", err),
			Details: map[string]any{
				"path":         dbPath,
				"workspace_id": activeWorkspaces[0],
			},
		}
	}

	gotIDs := make([]string, 0, len(agents))
	gotSet := make(map[string]struct{}, len(agents))
	ignoredInfrastructureIDs := make([]string, 0, 2)
	for _, agent := range agents {
		agentID := strings.TrimSpace(agent.AgentID)
		if isFirstDeploymentInfrastructureAgent(agent) {
			ignoredInfrastructureIDs = append(ignoredInfrastructureIDs, agentID)
			continue
		}
		gotIDs = append(gotIDs, agentID)
		gotSet[agentID] = struct{}{}
	}

	wantSet := make(map[string]struct{}, len(expectedDeploymentAgentIDs))
	for _, agentID := range expectedDeploymentAgentIDs {
		wantSet[agentID] = struct{}{}
	}

	missing := make([]string, 0, len(expectedDeploymentAgentIDs))
	for _, wantID := range expectedDeploymentAgentIDs {
		if _, ok := gotSet[wantID]; !ok {
			missing = append(missing, wantID)
		}
	}

	extra := make([]string, 0)
	for _, gotID := range gotIDs {
		if _, ok := wantSet[gotID]; !ok {
			extra = append(extra, gotID)
		}
	}

	if len(gotIDs) != len(expectedDeploymentAgentIDs) || len(missing) > 0 || len(extra) > 0 {
		return doctorCheck{
			Name:   "agent_roster",
			Status: doctorStatusFail,
			Message: fmt.Sprintf(
				"deployment topology requires exactly %d agents (%s), got %d (%s)",
				len(expectedDeploymentAgentIDs),
				strings.Join(expectedDeploymentAgentIDs, ", "),
				len(gotIDs),
				strings.Join(gotIDs, ", "),
			),
			Details: map[string]any{
				"path":                          dbPath,
				"workspace_id":                  activeWorkspaces[0],
				"expected_agents":               expectedDeploymentAgentIDs,
				"observed_agents":               gotIDs,
				"missing_agents":                missing,
				"unexpected_agents":             extra,
				"ignored_infrastructure_agents": ignoredInfrastructureIDs,
			},
		}
	}

	return doctorCheck{
		Name:    "agent_roster",
		Status:  doctorStatusPass,
		Message: "deployment agent roster matches the expected topology",
		Details: map[string]any{
			"path":                          dbPath,
			"workspace_id":                  activeWorkspaces[0],
			"expected_agents":               expectedDeploymentAgentIDs,
			"observed_agents":               gotIDs,
			"ignored_infrastructure_agents": ignoredInfrastructureIDs,
		},
	}
}

func isFirstDeploymentInfrastructureAgent(agent sqlite.AgentRecord) bool {
	agentID := strings.ToLower(strings.TrimSpace(agent.AgentID))
	role := strings.ToLower(strings.TrimSpace(agent.Role))
	protocol := strings.ToLower(strings.TrimSpace(agent.ProtocolVersion))
	switch {
	case agentID == "telegram-bridge":
		return true
	case role == "bridge":
		return true
	case strings.HasPrefix(protocol, "telegram-bridge/"):
		return true
	default:
		return false
	}
}

func checkRuntimeBuildInfo() doctorCheck {
	info := app.CurrentRuntimeBuildInfo()
	if info.BinaryPath == "" && info.WorkingDirectory == "" {
		return doctorCheck{
			Name:    "runtime_build",
			Status:  doctorStatusWarn,
			Message: "runtime build identity is incomplete",
		}
	}

	message := "runtime build identity collected"
	if strings.TrimSpace(info.VCSRevision) == "" {
		message = "runtime build identity collected without embedded vcs revision"
	}

	return doctorCheck{
		Name:    "runtime_build",
		Status:  doctorStatusPass,
		Message: message,
		Details: map[string]any{
			"binary_path":       info.BinaryPath,
			"working_directory": info.WorkingDirectory,
			"repo_root":         info.RepoRoot,
			"go_version":        info.GoVersion,
			"vcs_revision":      info.VCSRevision,
			"vcs_time":          info.VCSTime,
			"vcs_modified":      info.VCSModified,
		},
	}
}

func checkCurrentCheckout(info app.GitCheckoutInfo) doctorCheck {
	if strings.TrimSpace(info.Error) != "" {
		return doctorCheck{
			Name:    "current_checkout",
			Status:  doctorStatusWarn,
			Message: "git checkout metadata is unavailable",
			Details: map[string]any{
				"error": info.Error,
			},
		}
	}

	message := "git checkout resolved"
	if info.Dirty {
		message = "git checkout resolved with local modifications"
	}

	return doctorCheck{
		Name:    "current_checkout",
		Status:  doctorStatusPass,
		Message: message,
		Details: map[string]any{
			"repo_root": info.RepoRoot,
			"branch":    info.Branch,
			"head":      info.Head,
			"dirty":     info.Dirty,
		},
	}
}

func checkServeHealth(healthURL string, token string, cfg app.Config, localCheckout app.GitCheckoutInfo) doctorCheck {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	payload, err := fetchServiceHealth(ctx, healthURL, token)
	if err != nil {
		return doctorCheck{
			Name:    "serve_health",
			Status:  doctorStatusFail,
			Message: fmt.Sprintf("health endpoint request failed: %v", err),
			Details: map[string]any{
				"url": healthURL,
			},
		}
	}

	status := doctorStatusPass
	reasons := make([]string, 0, 6)
	if payload.Status != "ok" && payload.Status != "informational" {
		status = doctorStatusWarn
		reasons = append(reasons, fmt.Sprintf("service status=%s", payload.Status))
	}

	isLimitedPayload := payload.Config.WorkspaceRoot == "" && payload.Runtime.BinaryPath == ""
	if isLimitedPayload {
		return doctorCheck{
			Name:    "serve_health",
			Status:  status,
			Message: "service health endpoint is reachable (limited public payload without token)",
			Details: map[string]any{
				"url":         healthURL,
				"http_status": http.StatusOK,
				"service":     payload,
			},
		}
	}

	if payload.Checkout.Dirty {
		status = doctorStatusWarn
		reasons = append(reasons, "service checkout is dirty")
	}
	if payload.Runtime.VCSModified {
		status = doctorStatusWarn
		reasons = append(reasons, "service binary was built from a modified checkout")
	}
	if strings.TrimSpace(payload.Checkout.Head) != "" &&
		strings.TrimSpace(localCheckout.Head) != "" &&
		strings.TrimSpace(payload.Checkout.Head) != strings.TrimSpace(localCheckout.Head) {
		status = doctorStatusWarn
		reasons = append(
			reasons,
			fmt.Sprintf(
				"service HEAD %s differs from local HEAD %s",
				shortRevision(payload.Checkout.Head),
				shortRevision(localCheckout.Head),
			),
		)
	}
	if isLoopbackHealthURL(healthURL) {
		diffs := diffConfigSnapshots(snapshotConfig(cfg), payload.Config)
		if len(diffs) > 0 {
			status = doctorStatusWarn
			reasons = append(reasons, fmt.Sprintf("service config differs from local config in %d fields", len(diffs)))
		}
		if len(diffs) > 0 {
			return doctorCheck{
				Name:    "serve_health",
				Status:  status,
				Message: serveHealthMessage(reasons),
				Details: map[string]any{
					"url":                healthURL,
					"http_status":        http.StatusOK,
					"loopback_compare":   true,
					"config_differences": diffs,
					"drift_reasons":      reasons,
					"service":            payload,
				},
			}
		}
	}

	return doctorCheck{
		Name:    "serve_health",
		Status:  status,
		Message: serveHealthMessage(reasons),
		Details: map[string]any{
			"url":              healthURL,
			"http_status":      http.StatusOK,
			"loopback_compare": isLoopbackHealthURL(healthURL),
			"drift_reasons":    reasons,
			"service":          payload,
		},
	}
}

func serveHealthMessage(reasons []string) string {
	if len(reasons) == 0 {
		return "service health endpoint is reachable and aligned"
	}
	return "service health endpoint is reachable with drift warnings"
}

func serviceHealthPayloadFromDetails(details map[string]any) (serviceHealthPayload, bool) {
	serviceRaw, ok := details["service"]
	if !ok || serviceRaw == nil {
		return serviceHealthPayload{}, false
	}

	switch payload := serviceRaw.(type) {
	case serviceHealthPayload:
		return payload, true
	case *serviceHealthPayload:
		if payload == nil {
			return serviceHealthPayload{}, false
		}
		return *payload, true
	case json.RawMessage:
		return decodeServiceHealthPayload([]byte(payload))
	case []byte:
		return decodeServiceHealthPayload(payload)
	default:
		return decodeServiceHealthPayload(payload)
	}
}

func decodeServiceHealthPayload(raw any) (serviceHealthPayload, bool) {
	data, err := json.Marshal(raw)
	if err != nil {
		return serviceHealthPayload{}, false
	}

	var payload serviceHealthPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return serviceHealthPayload{}, false
	}
	return payload, true
}

func diagnosticSignalPresent(sig DiagnosticSignal) bool {
	return strings.TrimSpace(sig.State) != "" || strings.TrimSpace(sig.Message) != ""
}

func extendedReadinessPresent(ext ExtendedReadiness) bool {
	return diagnosticSignalPresent(ext.MotifLifecycle) ||
		diagnosticSignalPresent(ext.InvalidationLag) ||
		diagnosticSignalPresent(ext.OperatorQueueLag) ||
		diagnosticSignalPresent(ext.ReviewerScarcity) ||
		strings.TrimSpace(ext.ReviewerScarcityHealth.State) != "" ||
		strings.TrimSpace(ext.ReviewerScarcityHealth.Message) != "" ||
		ext.ReviewerScarcityHealth.WorkspaceCount != 0 ||
		diagnosticSignalPresent(ext.StuckAgents) ||
		diagnosticSignalPresent(ext.NoProgress) ||
		strings.TrimSpace(ext.ProjectionLag.State) != "" ||
		strings.TrimSpace(ext.ProjectionLag.Message) != "" ||
		ext.ProjectionLag.PendingCount != 0 ||
		ext.ProjectionLag.FailedCount != 0 ||
		diagnosticSignalPresent(ext.ReplayHealth)
}

func reviewerScarcityWorkspaceExamplesSummary(snapshot sqlite.ReviewerScarcityHealthSnapshot) string {
	parts := []string{}
	if len(snapshot.SaturatedWorkspaceExamples) > 0 {
		parts = append(parts, "saturated="+strings.Join(snapshot.SaturatedWorkspaceExamples, ","))
	}
	if len(snapshot.ScarceWorkspaceExamples) > 0 {
		parts = append(parts, "scarce="+strings.Join(snapshot.ScarceWorkspaceExamples, ","))
	}
	if len(snapshot.UnknownWorkspaceExamples) > 0 {
		parts = append(parts, "unknown="+strings.Join(snapshot.UnknownWorkspaceExamples, ","))
	}
	return strings.Join(parts, "; ")
}

func topLevelSemanticsPresent(sem TopLevelSemantics) bool {
	return diagnosticSignalPresent(sem.Liveness) ||
		diagnosticSignalPresent(sem.Readiness) ||
		diagnosticSignalPresent(sem.DeploymentReadiness) ||
		diagnosticSignalPresent(sem.Degraded)
}

func doctorGateError(verdict string, failOnWarn bool) error {
	switch verdict {
	case doctorStatusFail:
		return fmt.Errorf("doctor verdict: %s", verdict)
	case doctorStatusWarn:
		if failOnWarn {
			return fmt.Errorf("doctor verdict: %s", verdict)
		}
	}
	return nil
}

func checkPathConfigured(name, value string) doctorCheck {
	value = strings.TrimSpace(value)
	if value == "" {
		return doctorCheck{
			Name:    name,
			Status:  doctorStatusFail,
			Message: "path is empty",
		}
	}
	return doctorCheck{
		Name:    name,
		Status:  doctorStatusPass,
		Message: "path is configured",
		Details: map[string]any{
			"path": value,
		},
	}
}

func checkPathParent(name, targetPath string) doctorCheck {
	targetPath = strings.TrimSpace(targetPath)
	if targetPath == "" {
		return doctorCheck{
			Name:    name,
			Status:  doctorStatusFail,
			Message: "target path is empty",
		}
	}

	parent := filepath.Dir(targetPath)
	if parent == "." || parent == "" {
		parent = "."
	}
	if _, err := os.Stat(parent); err == nil {
		return doctorCheck{
			Name:    name,
			Status:  doctorStatusPass,
			Message: "parent directory exists",
			Details: map[string]any{
				"path": parent,
			},
		}
	}

	return doctorCheck{
		Name:    name,
		Status:  doctorStatusWarn,
		Message: "parent directory does not exist yet",
		Details: map[string]any{
			"path": parent,
		},
	}
}

func checkExistingSQLiteDB(dbPath string) doctorCheck {
	dbPath = strings.TrimSpace(dbPath)
	if dbPath == "" {
		return doctorCheck{
			Name:    "db_open",
			Status:  doctorStatusFail,
			Message: "db path is empty",
		}
	}

	if _, err := os.Stat(dbPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return doctorCheck{
				Name:    "db_open",
				Status:  doctorStatusWarn,
				Message: "db file does not exist yet; CLI will create it on first write",
				Details: map[string]any{
					"path": dbPath,
				},
			}
		}
		return doctorCheck{
			Name:    "db_open",
			Status:  doctorStatusFail,
			Message: fmt.Sprintf("db stat failed: %v", err),
			Details: map[string]any{
				"path": dbPath,
			},
		}
	}

	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		return doctorCheck{
			Name:    "db_open",
			Status:  doctorStatusFail,
			Message: fmt.Sprintf("open sqlite store failed: %v", err),
			Details: map[string]any{
				"path": dbPath,
			},
		}
	}
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := store.ApplyMigrations(ctx); err != nil {
		return doctorCheck{
			Name:    "db_open",
			Status:  doctorStatusFail,
			Message: fmt.Sprintf("apply migrations failed: %v", err),
			Details: map[string]any{
				"path": dbPath,
			},
		}
	}

	return doctorCheck{
		Name:    "db_open",
		Status:  doctorStatusPass,
		Message: "sqlite database opened and migrations are valid",
		Details: map[string]any{
			"path": dbPath,
		},
	}
}

func checkWorkspaceRoot(workspaceRoot string) doctorCheck {
	workspaceRoot = strings.TrimSpace(workspaceRoot)
	if workspaceRoot == "" {
		return doctorCheck{
			Name:    "workspace_root",
			Status:  doctorStatusFail,
			Message: "workspace root is empty",
		}
	}

	sharedDir := filepath.Join(workspaceRoot, "shared")
	stateDir := filepath.Join(workspaceRoot, "state")
	details := map[string]any{
		"workspace_root": workspaceRoot,
		"shared_dir":     sharedDir,
		"state_dir":      stateDir,
	}

	if info, err := os.Stat(workspaceRoot); err == nil && info.IsDir() {
		status := doctorStatusPass
		message := "workspace root exists"
		if _, err := os.Stat(sharedDir); err != nil {
			status = doctorStatusWarn
			message = "workspace root exists, but shared/state directories are incomplete"
		}
		if _, err := os.Stat(stateDir); err != nil {
			status = doctorStatusWarn
			message = "workspace root exists, but shared/state directories are incomplete"
		}
		return doctorCheck{
			Name:    "workspace_root",
			Status:  status,
			Message: message,
			Details: details,
		}
	}

	return doctorCheck{
		Name:    "workspace_root",
		Status:  doctorStatusWarn,
		Message: "workspace root does not exist yet",
		Details: details,
	}
}

func checkExecutorPython(pythonBin string) doctorCheck {
	pythonBin = strings.TrimSpace(pythonBin)
	if pythonBin == "" {
		return doctorCheck{
			Name:    "executor_python",
			Status:  doctorStatusFail,
			Message: "executor python is empty",
		}
	}

	resolved, err := exec.LookPath(pythonBin)
	if err != nil {
		return doctorCheck{
			Name:    "executor_python",
			Status:  doctorStatusFail,
			Message: fmt.Sprintf("python executable not found: %s", pythonBin),
		}
	}

	return doctorCheck{
		Name:    "executor_python",
		Status:  doctorStatusPass,
		Message: "executor python resolved",
		Details: map[string]any{
			"configured": pythonBin,
			"resolved":   resolved,
		},
	}
}

func checkExecutorBridge(bridgePath string) doctorCheck {
	bridgePath = strings.TrimSpace(bridgePath)
	if bridgePath == "" {
		return doctorCheck{
			Name:    "executor_bridge",
			Status:  doctorStatusFail,
			Message: "executor bridge script path is empty",
		}
	}

	resolved, err := resolvePathFromWorkingTree(bridgePath, 5)
	if err != nil {
		return doctorCheck{
			Name:    "executor_bridge",
			Status:  doctorStatusFail,
			Message: fmt.Sprintf("executor bridge script not found: %s", bridgePath),
		}
	}

	return doctorCheck{
		Name:    "executor_bridge",
		Status:  doctorStatusPass,
		Message: "executor bridge script found",
		Details: map[string]any{
			"configured": bridgePath,
			"resolved":   resolved,
		},
	}
}

func checkRuntimeMetrics(metricsPath string) doctorCheck {
	metricsPath = strings.TrimSpace(metricsPath)
	if metricsPath == "" {
		return doctorCheck{
			Name:    "runtime_metrics",
			Status:  doctorStatusWarn,
			Message: "metrics path is empty",
		}
	}

	snapshots, totalValid, parseErrors, err := readRuntimeMetricsSnapshots(metricsPath, 5)
	if err != nil {
		if strings.Contains(err.Error(), "metrics file not found") {
			return doctorCheck{
				Name:    "runtime_metrics",
				Status:  doctorStatusWarn,
				Message: "metrics file does not exist yet",
				Details: map[string]any{
					"path": metricsPath,
				},
			}
		}
		return doctorCheck{
			Name:    "runtime_metrics",
			Status:  doctorStatusFail,
			Message: fmt.Sprintf("metrics read failed: %v", err),
			Details: map[string]any{
				"path": metricsPath,
			},
		}
	}

	status := doctorStatusPass
	message := "runtime metrics file parsed successfully"
	if parseErrors > 0 {
		status = doctorStatusWarn
		message = "runtime metrics parsed with invalid lines"
	}

	latestTS := ""
	var latest *runtimeMetricsSnapshot
	if len(snapshots) > 0 {
		last := snapshots[len(snapshots)-1]
		latestTS = last.Timestamp
		latest = &last
	}
	health := evaluateRuntimeMetricsHealth(latest)
	if health.Verdict == "degraded" || health.Verdict == "unknown" {
		status = doctorStatusWarn
		if health.Verdict == "degraded" {
			message = "runtime metrics health is degraded"
		} else {
			message = "runtime metrics health is unknown"
		}
	}
	return doctorCheck{
		Name:    "runtime_metrics",
		Status:  status,
		Message: message,
		Details: map[string]any{
			"path":             metricsPath,
			"valid_snapshots":  totalValid,
			"parse_errors":     parseErrors,
			"latest_timestamp": latestTS,
			"loaded_snapshots": len(snapshots),
			"health_verdict":   health.Verdict,
		},
	}
}

func resolvePathFromWorkingTree(input string, maxParents int) (string, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", errors.New("path is empty")
	}
	if filepath.IsAbs(input) {
		if _, err := os.Stat(input); err != nil {
			return "", err
		}
		return input, nil
	}

	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	candidate := wd
	for i := 0; i <= maxParents; i++ {
		full := filepath.Clean(filepath.Join(candidate, input))
		if _, err := os.Stat(full); err == nil {
			return full, nil
		}
		next := filepath.Dir(candidate)
		if next == candidate {
			break
		}
		candidate = next
	}
	return "", os.ErrNotExist
}

// checkLoopReadinessFromDetails extracts loop_readiness from the live diagnostics
// payload and evaluates each loop's honest state.
func checkLoopReadinessFromDetails(details map[string]any) doctorCheck {
	payload, ok := serviceHealthPayloadFromDetails(details)
	if !ok {
		return doctorCheck{
			Name:    "loop_readiness",
			Status:  doctorStatusPass,
			Message: "loop readiness not available in health payload",
		}
	}

	if len(payload.LoopReadiness) == 0 {
		return doctorCheck{
			Name:    "loop_readiness",
			Status:  doctorStatusPass,
			Message: "loop readiness not present in diagnostics payload (no registry)",
		}
	}

	status := doctorStatusPass
	reasons := make([]string, 0, len(payload.LoopReadiness))
	loopDetails := make([]LoopReadiness, 0, len(payload.LoopReadiness))

	for _, entry := range payload.LoopReadiness {
		name := entry.Name
		state := entry.State
		restarts := entry.Restarts
		droppedEvents := entry.DroppedEvents

		loopDetails = append(loopDetails, entry)

		switch state {
		case LoopRecovering:
			if restarts > 3 {
				status = doctorStatusFail
				reasons = append(reasons, fmt.Sprintf("%s: recovering with %d restarts", name, restarts))
			} else {
				if status != doctorStatusFail {
					status = doctorStatusWarn
				}
				reasons = append(reasons, fmt.Sprintf("%s: recovering", name))
			}
		case LoopDegraded:
			if status != doctorStatusFail {
				status = doctorStatusWarn
			}
			reasons = append(reasons, fmt.Sprintf("%s: degraded (dropped_events=%d)", name, droppedEvents))
		case LoopNotStarted:
			// not_started for daemon is expected when --with-daemon is not set
			// For firehose and timeout_reaper, not_started is unexpected
			if name != loopNameDaemon {
				if status != doctorStatusFail {
					status = doctorStatusWarn
				}
				reasons = append(reasons, fmt.Sprintf("%s: not started", name))
			}
		case LoopRunning, LoopDisabled, LoopStopped:
			// healthy states
		}
	}

	message := "all loop readiness signals are healthy"
	if len(reasons) > 0 {
		message = fmt.Sprintf("loop readiness issues detected: %s", strings.Join(reasons, "; "))
	}

	return doctorCheck{
		Name:    "loop_readiness",
		Status:  status,
		Message: message,
		Details: map[string]any{
			"loops": loopDetails,
		},
	}
}

func checkDurableRuntimeFromDetails(details map[string]any) doctorCheck {
	payload, ok := serviceHealthPayloadFromDetails(details)
	if !ok {
		return doctorCheck{
			Name:    "durable_runtime_readback",
			Status:  doctorStatusWarn,
			Message: "durable runtime readback not available in health payload",
		}
	}
	if payload.DurableRuntime == nil {
		return doctorCheck{
			Name:    "durable_runtime_readback",
			Status:  doctorStatusWarn,
			Message: "durable runtime readback not present in diagnostics payload",
		}
	}
	return checkDurableRuntimeSnapshot(*payload.DurableRuntime)
}

func checkDurableRuntimeFromDB(dbPath string) doctorCheck {
	return checkDurableRuntimeSnapshotValue(collectDurableRuntimeSnapshot(dbPath))
}

func checkDaemonPromptCompilerConvergenceFromDetails(details map[string]any) doctorCheck {
	payload, ok := serviceHealthPayloadFromDetails(details)
	if !ok || payload.DurableRuntime == nil {
		if ok && daemonLoopDisabled(payload.LoopReadiness) {
			return doctorCheck{
				Name:    "daemon_prompt_compiler_convergence",
				Status:  doctorStatusPass,
				Message: "daemon prompt compiler convergence not required while embedded daemon loop is disabled",
			}
		}
		return doctorCheck{
			Name:    "daemon_prompt_compiler_convergence",
			Status:  doctorStatusWarn,
			Message: "daemon prompt compiler convergence not available in diagnostics payload",
		}
	}
	if payload.DurableRuntime.PromptCompiler == nil && daemonPromptCompilerConvergenceNotApplicable(*payload.DurableRuntime) {
		return doctorCheck{
			Name:    "daemon_prompt_compiler_convergence",
			Status:  doctorStatusPass,
			Message: "daemon prompt compiler convergence not applicable because no durable execution run exists",
		}
	}
	if daemonPromptCompilerConvergenceDeferredForDisabledDaemon(payload, payload.DurableRuntime.PromptCompiler) {
		return doctorCheck{
			Name:    "daemon_prompt_compiler_convergence",
			Status:  doctorStatusPass,
			Message: "daemon prompt compiler convergence not required while embedded daemon loop is disabled",
			Details: daemonPromptCompilerConvergenceDetails(payload.DurableRuntime.PromptCompiler),
		}
	}
	return checkDaemonPromptCompilerConvergenceSnapshot(payload.DurableRuntime.PromptCompiler)
}

func checkDaemonPromptCompilerConvergenceFromDB(dbPath string) doctorCheck {
	snapshot := collectDurableRuntimeSnapshot(dbPath)
	if snapshot == nil {
		return checkDaemonPromptCompilerConvergenceSnapshot(nil)
	}
	if snapshot.PromptCompiler == nil && daemonPromptCompilerConvergenceNotApplicable(*snapshot) {
		return doctorCheck{
			Name:    "daemon_prompt_compiler_convergence",
			Status:  doctorStatusPass,
			Message: "daemon prompt compiler convergence not applicable because no durable execution run exists",
		}
	}
	return checkDaemonPromptCompilerConvergenceSnapshot(snapshot.PromptCompiler)
}

func checkPromptAuthorityScopeFromDetails(details map[string]any) doctorCheck {
	payload, ok := serviceHealthPayloadFromDetails(details)
	if !ok {
		return doctorCheck{
			Name:    "prompt_authority_scope",
			Status:  doctorStatusWarn,
			Message: "prompt authority scope not available in diagnostics payload",
		}
	}
	return checkPromptAuthorityScope(payload.PromptAuthority)
}

func checkPromptAuthorityScope(scope PromptAuthorityScopeDiagnostics) doctorCheck {
	issues := promptAuthorityScopeIssues(scope)
	if len(issues) > 0 {
		return doctorCheck{
			Name:    "prompt_authority_scope",
			Status:  doctorStatusFail,
			Message: strings.Join(issues, "; "),
			Details: map[string]any{
				"state":    scope.State,
				"contract": scope.Contract,
				"surfaces": scope.Surfaces,
			},
		}
	}
	return doctorCheck{
		Name:    "prompt_authority_scope",
		Status:  doctorStatusPass,
		Message: strings.TrimSpace(firstNonEmpty(scope.Message, "prompt authority scope is bounded for first stable preflight")),
		Details: map[string]any{
			"state":         scope.State,
			"contract":      scope.Contract,
			"surface_count": len(scope.Surfaces),
			"surface_names": promptAuthorityScopeSurfaceNames(scope.Surfaces),
		},
	}
}

func promptAuthorityScopeIssues(scope PromptAuthorityScopeDiagnostics) []string {
	issues := make([]string, 0, 4)
	if strings.TrimSpace(scope.Contract) != promptAuthorityScopeContract {
		issues = append(issues, "prompt authority scope contract is missing or unsupported")
	}
	if strings.ToLower(strings.TrimSpace(scope.State)) != "ok" {
		issues = append(issues, "prompt authority scope state is not ok")
	}

	expected := promptAuthorityScopeExpectedSurfaces()
	byName := make(map[string]PromptAuthoritySurfaceBoundary, len(scope.Surfaces))
	for _, surface := range scope.Surfaces {
		name := strings.TrimSpace(surface.Surface)
		if name != "" {
			byName[name] = surface
		}
		if surface.AcceptedAsDaemonConvergence {
			issues = append(issues, fmt.Sprintf("%s must not be accepted as daemon prompt convergence", firstNonEmpty(name, "<unnamed surface>")))
		}
		if strings.EqualFold(strings.TrimSpace(surface.DeploymentEvidence), durablePromptDeploymentEvidenceAccepted) {
			issues = append(issues, fmt.Sprintf("%s claims daemon deployment evidence", firstNonEmpty(name, "<unnamed surface>")))
		}
		if strings.EqualFold(strings.TrimSpace(surface.C21Convergence), durablePromptConvergenceAccepted) {
			issues = append(issues, fmt.Sprintf("%s claims C2.1 daemon convergence", firstNonEmpty(name, "<unnamed surface>")))
		}
	}
	for name, want := range expected {
		surface, ok := byName[name]
		if !ok {
			issues = append(issues, "missing first-stable prompt authority boundary for "+name)
			continue
		}
		issues = append(issues, promptAuthoritySurfaceMismatchIssues(name, surface, want)...)
	}
	return issues
}

func promptAuthorityScopeExpectedSurfaces() map[string]PromptAuthoritySurfaceBoundary {
	return map[string]PromptAuthoritySurfaceBoundary{
		"manager.local_inspect": {
			Decision:                    "excluded_read_only_non_daemon",
			C21Convergence:              promptAuthorityStatusExcluded,
			DeploymentEvidence:          promptAuthorityDeploymentEvidenceRejected,
			FirstDeploymentPreflight:    "excluded_read_only_non_daemon",
			PromptCompilerStatus:        "manager_mediated_local_inspect_non_converged",
			AuthorityBoundary:           "manager_process_read_only_inspect",
			AcceptedAsDaemonConvergence: false,
		},
		"manager.inline_local_tui_chat": {
			Decision:                    "excluded_compatibility_non_daemon",
			C21Convergence:              promptAuthorityStatusExcluded,
			DeploymentEvidence:          promptAuthorityDeploymentEvidenceRejected,
			FirstDeploymentPreflight:    "excluded_compatibility_non_daemon",
			PromptCompilerStatus:        "manager_mediated_local_inspect_non_converged",
			AuthorityBoundary:           "manager_process_compatibility_chat",
			AcceptedAsDaemonConvergence: false,
		},
		"manager.live_attach.runtime_control": {
			Decision:                    "excluded_operator_runtime_control_not_c2_1_convergence",
			C21Convergence:              promptAuthorityStatusExcluded,
			DeploymentEvidence:          promptAuthorityDeploymentEvidenceRejected,
			FirstDeploymentPreflight:    "excluded_operator_runtime_control_not_c2_1_convergence",
			PromptCompilerStatus:        "operator_runtime_control_non_converged",
			AuthorityBoundary:           "operator_runtime_control_not_daemon_prompt_compiler_evidence",
			AcceptedAsDaemonConvergence: false,
		},
		"manager.live_attach.model_ask": {
			Decision:                    "request_carrier_only",
			C21Convergence:              "request_carrier_only",
			DeploymentEvidence:          "accepted_only_for_agent_request_carrier_not_local_control_mutation",
			FirstDeploymentPreflight:    "carrier_only_no_separate_daemon_authority",
			PromptCompilerStatus:        "request_carrier_bounded_by_agent_request_evidence",
			AuthorityBoundary:           "agent_request_carrier_only_no_separate_daemon_local_control_claim",
			AcceptedAsDaemonConvergence: false,
		},
		"manager.web_runtime_control": {
			Decision:                    "excluded_operator_runtime_control_not_c2_1_convergence",
			C21Convergence:              promptAuthorityStatusExcluded,
			DeploymentEvidence:          promptAuthorityDeploymentEvidenceRejected,
			FirstDeploymentPreflight:    "excluded_operator_runtime_control_not_c2_1_convergence",
			PromptCompilerStatus:        "operator_runtime_control_non_converged",
			AuthorityBoundary:           "operator_web_control_not_daemon_prompt_compiler_evidence",
			AcceptedAsDaemonConvergence: false,
		},
		"manager.process_lifecycle_control": {
			Decision:                    "excluded_process_supervision_not_prompt_convergence",
			C21Convergence:              promptAuthorityStatusExcluded,
			DeploymentEvidence:          promptAuthorityDeploymentEvidenceRejected,
			FirstDeploymentPreflight:    "excluded_process_supervision_not_prompt_convergence",
			PromptCompilerStatus:        "manager_process_control_non_converged",
			AuthorityBoundary:           "manager_process_supervision_not_daemon_prompt_compiler_evidence",
			AcceptedAsDaemonConvergence: false,
		},
		"internal_agent.tension_lifecycle_update": {
			Decision:                    "excluded_legacy_native_direct_tool",
			C21Convergence:              promptAuthorityStatusExcluded,
			DeploymentEvidence:          promptAuthorityDeploymentEvidenceRejected,
			FirstDeploymentPreflight:    "excluded_until_migrated_to_prompt_envelope",
			PromptCompilerStatus:        "legacy_native_tool_non_converged",
			AuthorityBoundary:           "legacy_internal_agent_tool_not_first_stable_daemon_surface",
			AcceptedAsDaemonConvergence: false,
		},
		"internal_agent.memory_write": {
			Decision:                    "excluded_legacy_native_direct_tool",
			C21Convergence:              promptAuthorityStatusExcluded,
			DeploymentEvidence:          promptAuthorityDeploymentEvidenceRejected,
			FirstDeploymentPreflight:    "excluded_until_migrated_to_workspace_memory_rpc_envelope",
			PromptCompilerStatus:        "legacy_native_tool_non_converged",
			AuthorityBoundary:           "direct_sqlite_internal_agent_tool_not_first_stable_daemon_surface",
			AcceptedAsDaemonConvergence: false,
		},
		"internal_living.memory_write": {
			Decision:                    "covered_when_routed_through_workspace_memory_rpc",
			C21Convergence:              "covered_when_workspace_memory_rpc_event_present",
			DeploymentEvidence:          "accepted_only_with_workspace_memory_prompt_context_envelope",
			FirstDeploymentPreflight:    "covered_rpc_path_only",
			PromptCompilerStatus:        "covered_by_prompt_context_envelope_when_eventful",
			AuthorityBoundary:           "server_rpc_workspace_memory_envelope_required",
			AcceptedAsDaemonConvergence: false,
		},
		"executor.node.report_progress": {
			Decision:                    "excluded_log_only_non_authority",
			C21Convergence:              promptAuthorityStatusExcluded,
			DeploymentEvidence:          promptAuthorityDeploymentEvidenceRejected,
			FirstDeploymentPreflight:    "excluded_log_only_non_authority",
			PromptCompilerStatus:        "executor_progress_callback_exclusion.v1",
			AuthorityBoundary:           "log_only_non_authority_bearing_callback",
			AcceptedAsDaemonConvergence: false,
		},
	}
}

func promptAuthoritySurfaceMismatchIssues(name string, got, want PromptAuthoritySurfaceBoundary) []string {
	checks := map[string][2]string{
		"decision":                   {got.Decision, want.Decision},
		"authority_boundary":         {got.AuthorityBoundary, want.AuthorityBoundary},
		"prompt_compiler_status":     {got.PromptCompilerStatus, want.PromptCompilerStatus},
		"c2_1_convergence":           {got.C21Convergence, want.C21Convergence},
		"deployment_evidence":        {got.DeploymentEvidence, want.DeploymentEvidence},
		"first_deployment_preflight": {got.FirstDeploymentPreflight, want.FirstDeploymentPreflight},
	}
	issues := make([]string, 0, len(checks))
	for field, pair := range checks {
		if strings.TrimSpace(pair[0]) != strings.TrimSpace(pair[1]) {
			issues = append(issues, fmt.Sprintf("%s %s=%q, want %q", name, field, strings.TrimSpace(pair[0]), strings.TrimSpace(pair[1])))
		}
	}
	if got.AcceptedAsDaemonConvergence != want.AcceptedAsDaemonConvergence {
		issues = append(issues, fmt.Sprintf("%s accepted_as_daemon_convergence=%t, want %t", name, got.AcceptedAsDaemonConvergence, want.AcceptedAsDaemonConvergence))
	}
	return issues
}

func promptAuthorityScopeSurfaceNames(surfaces []PromptAuthoritySurfaceBoundary) []string {
	names := make([]string, 0, len(surfaces))
	for _, surface := range surfaces {
		if name := strings.TrimSpace(surface.Surface); name != "" {
			names = append(names, name)
		}
	}
	return names
}

func daemonPromptCompilerConvergenceNotApplicable(snapshot durableRuntimeSnapshot) bool {
	return strings.EqualFold(strings.TrimSpace(snapshot.State), "unsupported") &&
		strings.TrimSpace(snapshot.RunID) == ""
}

func daemonPromptCompilerConvergenceDeferredForDisabledDaemon(payload serviceHealthPayload, snapshot *durablePromptCompilerSnapshot) bool {
	if !daemonLoopDisabled(payload.LoopReadiness) {
		return false
	}
	return true
}

func checkDaemonPromptCompilerConvergenceSnapshot(snapshot *durablePromptCompilerSnapshot) doctorCheck {
	if snapshot == nil {
		return doctorCheck{
			Name:    "daemon_prompt_compiler_convergence",
			Status:  doctorStatusWarn,
			Message: "daemon prompt compiler convergence not collected",
		}
	}
	state := strings.ToLower(strings.TrimSpace(snapshot.State))
	switch state {
	case "ok":
		return doctorCheck{
			Name:    "daemon_prompt_compiler_convergence",
			Status:  doctorStatusPass,
			Message: strings.TrimSpace(firstNonEmpty(snapshot.Message, "daemon prompt compiler convergence proof is present")),
			Details: daemonPromptCompilerConvergenceDetails(snapshot),
		}
	case "", "unsupported", "not_evaluated":
		return doctorCheck{
			Name:    "daemon_prompt_compiler_convergence",
			Status:  doctorStatusWarn,
			Message: strings.TrimSpace(firstNonEmpty(snapshot.Message, "daemon prompt compiler convergence proof has not been evaluated")),
			Details: map[string]any{
				"state": state,
			},
		}
	case "missing", "mismatch", "error":
		return doctorCheck{
			Name:    "daemon_prompt_compiler_convergence",
			Status:  doctorStatusFail,
			Message: strings.TrimSpace(firstNonEmpty(snapshot.Message, "daemon prompt compiler convergence proof failed")),
			Details: daemonPromptCompilerConvergenceDetails(snapshot),
		}
	default:
		return doctorCheck{
			Name:    "daemon_prompt_compiler_convergence",
			Status:  doctorStatusWarn,
			Message: strings.TrimSpace(firstNonEmpty(snapshot.Message, "daemon prompt compiler convergence reported unexpected state "+state)),
			Details: map[string]any{
				"state": snapshot.State,
			},
		}
	}
}

func daemonPromptCompilerConvergenceDetails(snapshot *durablePromptCompilerSnapshot) map[string]any {
	if snapshot == nil {
		return map[string]any{}
	}
	details := map[string]any{
		"state":                   snapshot.State,
		"step_id":                 snapshot.StepID,
		"capability_snapshot_ref": snapshot.CapabilitySnapshotRef,
		"projection_digest":       snapshot.ProjectionDigest,
	}
	for key, value := range map[string]string{
		"capability_snapshot_path": snapshot.CapabilitySnapshotPath,
		"snapshot_readback_state":  snapshot.SnapshotReadbackState,
		"snapshot_readback_digest": snapshot.SnapshotReadbackDigest,
	} {
		if strings.TrimSpace(value) != "" {
			details[key] = value
		}
	}
	return details
}

func checkDurableRuntimeSnapshotValue(snapshot *durableRuntimeSnapshot) doctorCheck {
	if snapshot == nil {
		return doctorCheck{
			Name:    "durable_runtime_readback",
			Status:  doctorStatusPass,
			Message: "durable runtime readback not collected",
		}
	}
	return checkDurableRuntimeSnapshot(*snapshot)
}

func checkDurableRuntimeSnapshot(snapshot durableRuntimeSnapshot) doctorCheck {
	state := strings.ToLower(strings.TrimSpace(snapshot.State))
	switch state {
	case "", "unsupported":
		return doctorCheck{
			Name:    "durable_runtime_readback",
			Status:  doctorStatusPass,
			Message: strings.TrimSpace(firstNonEmpty(snapshot.Message, "durable runtime readback unavailable")),
			Details: map[string]any{
				"state": state,
			},
		}
	case "ok":
		return doctorCheck{
			Name:    "durable_runtime_readback",
			Status:  doctorStatusPass,
			Message: strings.TrimSpace(firstNonEmpty(snapshot.Message, "durable runtime readback restored")),
			Details: map[string]any{
				"state":      snapshot.State,
				"run_id":     snapshot.RunID,
				"session_id": snapshot.SessionID,
				"task_id":    snapshot.TaskID,
				"agent_id":   snapshot.AgentID,
				"phase":      snapshot.StepPhase,
				"progress":   snapshot.Progress,
			},
		}
	case "missing", "mismatch", "error":
		return doctorCheck{
			Name:    "durable_runtime_readback",
			Status:  doctorStatusFail,
			Message: strings.TrimSpace(firstNonEmpty(snapshot.Message, "durable runtime readback failed")),
			Details: map[string]any{
				"state":      snapshot.State,
				"issues":     snapshot.Issues,
				"run_id":     snapshot.RunID,
				"session_id": snapshot.SessionID,
			},
		}
	default:
		return doctorCheck{
			Name:    "durable_runtime_readback",
			Status:  doctorStatusWarn,
			Message: strings.TrimSpace(firstNonEmpty(snapshot.Message, "durable runtime readback reported unexpected state "+state)),
			Details: map[string]any{
				"state":      snapshot.State,
				"issues":     snapshot.Issues,
				"run_id":     snapshot.RunID,
				"session_id": snapshot.SessionID,
			},
		}
	}
}

func checkExtendedReadinessFromDetails(details map[string]any) doctorCheck {
	payload, ok := serviceHealthPayloadFromDetails(details)
	if !ok {
		return doctorCheck{
			Name:    "extended_readiness",
			Status:  doctorStatusPass,
			Message: "extended readiness not available in health payload",
		}
	}

	if !extendedReadinessPresent(payload.Extended) {
		return doctorCheck{
			Name:    "extended_readiness",
			Status:  doctorStatusPass,
			Message: "extended readiness not explicitly present in service diagnostics payload",
		}
	}

	status := doctorStatusPass
	reasons := make([]string, 0, 5)
	advisoryReasons := make([]string, 0, 2)

	// Since we are checking honest states and failing/missing those that we can't cleanly provide:
	for key, sig := range map[string]DiagnosticSignal{
		"invalidation_lag":   payload.Extended.InvalidationLag,
		"operator_queue_lag": payload.Extended.OperatorQueueLag,
		"reviewer_scarcity":  payload.Extended.ReviewerScarcity,
		"stuck_agents":       payload.Extended.StuckAgents,
		"no_progress":        payload.Extended.NoProgress,
		"replay_health":      payload.Extended.ReplayHealth,
	} {
		switch sig.State {
		case "unsupported":
			if extendedReadinessAdvisoryUnsupportedKey(key) {
				advisoryReasons = append(advisoryReasons, fmt.Sprintf("%s: unsupported (%s)", key, sig.Message))
				continue
			}
			status = doctorStatusWarn
			reasons = append(reasons, fmt.Sprintf("%s: unsupported (%s)", key, sig.Message))
		case "degraded":
			status = doctorStatusWarn
			reason := fmt.Sprintf("%s: degraded (%s)", key, sig.Message)
			if key == "reviewer_scarcity" {
				if examples := reviewerScarcityWorkspaceExamplesSummary(payload.Extended.ReviewerScarcityHealth); examples != "" {
					reason += fmt.Sprintf(" [%s]", examples)
				}
			}
			reasons = append(reasons, reason)
		case "partial":
			status = doctorStatusWarn
			reason := fmt.Sprintf("%s: partial (%s)", key, sig.Message)
			if key == "reviewer_scarcity" {
				if examples := reviewerScarcityWorkspaceExamplesSummary(payload.Extended.ReviewerScarcityHealth); examples != "" {
					reason += fmt.Sprintf(" [%s]", examples)
				}
			}
			reasons = append(reasons, reason)
		case "blocked", "needs_operator":
			status = doctorStatusFail
			reasons = append(reasons, fmt.Sprintf("%s: %s (%s)", key, sig.State, sig.Message))
		case "error":
			status = doctorStatusFail
			reasons = append(reasons, fmt.Sprintf("%s: error (%s)", key, sig.Message))
		}
	}
	lag := payload.Extended.ProjectionLag
	switch lag.State {
	case "ok":
		if lag.PendingCount > 0 || lag.FailedCount > 0 {
			status = doctorStatusWarn
			reasons = append(reasons, fmt.Sprintf("projection_lag: backlog present (pending=%d, failed=%d)", lag.PendingCount, lag.FailedCount))
		}
	case "degraded":
		status = doctorStatusWarn
		reasons = append(reasons, fmt.Sprintf("projection_lag: degraded (pending=%d, failed=%d)", lag.PendingCount, lag.FailedCount))
	case "unsupported", "unknown":
		status = doctorStatusWarn
		reasons = append(reasons, fmt.Sprintf("projection_lag: %s (%s)", lag.State, lag.Message))
	case "":
		status = doctorStatusWarn
		reasons = append(reasons, "projection_lag: missing")
	default:
		status = doctorStatusWarn
		reasons = append(reasons, fmt.Sprintf("projection_lag: unexpected state=%s", lag.State))
	}

	if payload.Extended.MotifLifecycle.State == "degraded" {
		status = doctorStatusWarn
		reasons = append(reasons, fmt.Sprintf("motif_lifecycle: degraded (%s)", payload.Extended.MotifLifecycle.Message))
	} else if payload.Extended.MotifLifecycle.State == "missing" {
		status = doctorStatusWarn
		reasons = append(reasons, "motif_lifecycle: not running")
	}

	message := "extended readiness signals collected cleanly"
	if len(reasons) > 0 {
		messageReasons := append([]string(nil), reasons...)
		messageReasons = append(messageReasons, advisoryReasons...)
		message = fmt.Sprintf("extended readiness signals degraded/unsupported: %s", strings.Join(messageReasons, "; "))
	} else if len(advisoryReasons) > 0 {
		message = fmt.Sprintf("extended readiness signals collected with advisory unsupported signals: %s", strings.Join(advisoryReasons, "; "))
	}

	return doctorCheck{
		Name:    "extended_readiness",
		Status:  status,
		Message: message,
		Details: map[string]any{
			"motif_lifecycle":          payload.Extended.MotifLifecycle,
			"invalidation_lag":         payload.Extended.InvalidationLag,
			"operator_queue_lag":       payload.Extended.OperatorQueueLag,
			"reviewer_scarcity":        payload.Extended.ReviewerScarcity,
			"reviewer_scarcity_health": payload.Extended.ReviewerScarcityHealth,
			"stuck_agents":             payload.Extended.StuckAgents,
			"no_progress":              payload.Extended.NoProgress,
			"projection_lag":           payload.Extended.ProjectionLag,
			"replay_health":            payload.Extended.ReplayHealth,
		},
	}
}

func extendedReadinessAdvisoryUnsupportedKey(key string) bool {
	switch key {
	case "invalidation_lag", "replay_health":
		return true
	default:
		return false
	}
}

func checkBudgetLedgerFromDetails(details map[string]any) doctorCheck {
	payload, ok := serviceHealthPayloadFromDetails(details)
	if !ok {
		return doctorCheck{
			Name:    "budget_ledger",
			Status:  doctorStatusPass,
			Message: "budget ledger diagnostics not available in health payload",
		}
	}
	if payload.BudgetLedger == nil {
		status := doctorStatusWarn
		message := "budget ledger diagnostics missing from authenticated diagnostics payload"
		if isLimitedPublicHealthPayload(payload) {
			status = doctorStatusPass
			message = "budget ledger diagnostics not available in public liveness payload"
		}
		return doctorCheck{
			Name:    "budget_ledger",
			Status:  status,
			Message: message,
		}
	}

	snapshot := *payload.BudgetLedger
	status := doctorStatusPass
	message := strings.TrimSpace(snapshot.Message)
	if message == "" {
		message = "budget ledger diagnostics are healthy"
	}
	if strings.TrimSpace(snapshot.Contract) != sqlite.BudgetLedgerHealthContract {
		return doctorCheck{
			Name:    "budget_ledger",
			Status:  doctorStatusFail,
			Message: "budget ledger health contract is missing or unsupported",
			Details: map[string]any{
				"contract":          snapshot.Contract,
				"required_contract": sqlite.BudgetLedgerHealthContract,
				"status":            snapshot.Status,
				"message":           snapshot.Message,
				"reasons":           snapshot.Reasons,
			},
		}
	}
	switch strings.ToLower(strings.TrimSpace(snapshot.Status)) {
	case "", "ok":
		status = doctorStatusPass
	case "exhausted":
		status = doctorStatusWarn
		if message == "" {
			message = fmt.Sprintf("budget exhausted for %d account(s)", snapshot.ExhaustedAccountCount)
		}
	case "degraded":
		status = doctorStatusWarn
		if snapshot.OverspentAccountCount > 0 {
			status = doctorStatusFail
		}
		if message == "" {
			message = fmt.Sprintf("budget ledger degraded: stale_open_reservations=%d overspent_accounts=%d", snapshot.StaleOpenReservationCount, snapshot.OverspentAccountCount)
		}
	case "error":
		status = doctorStatusFail
		if message == "" {
			message = "budget ledger diagnostics reported an error"
		}
	default:
		status = doctorStatusWarn
		message = "budget ledger diagnostics reported unexpected state " + strings.TrimSpace(snapshot.Status)
	}

	return doctorCheck{
		Name:    "budget_ledger",
		Status:  status,
		Message: message,
		Details: map[string]any{
			"contract":                     snapshot.Contract,
			"status":                       snapshot.Status,
			"message":                      snapshot.Message,
			"error":                        snapshot.Error,
			"reasons":                      snapshot.Reasons,
			"account_count":                snapshot.AccountCount,
			"exhausted_account_count":      snapshot.ExhaustedAccountCount,
			"open_reservation_count":       snapshot.OpenReservationCount,
			"stale_open_reservation_count": snapshot.StaleOpenReservationCount,
			"overspent_account_count":      snapshot.OverspentAccountCount,
			"ledger_entry_count":           snapshot.LedgerEntryCount,
			"last_ledger_entry_at":         snapshot.LastLedgerEntryAt,
			"reference_at":                 snapshot.ReferenceAt,
			"exhausted_account_examples":   snapshot.ExhaustedAccountExamples,
		},
	}
}

func checkProjectPatchQueueDurabilityFromDetails(details map[string]any) doctorCheck {
	payload, ok := serviceHealthPayloadFromDetails(details)
	if !ok {
		return doctorCheck{
			Name:    "project_patch_queue_durability",
			Status:  doctorStatusPass,
			Message: "project patch queue durability diagnostics not available in health payload",
		}
	}
	if payload.PatchQueue == nil {
		status := doctorStatusWarn
		message := "project patch queue durability diagnostics missing from authenticated diagnostics payload"
		if isLimitedPublicHealthPayload(payload) {
			status = doctorStatusPass
			message = "project patch queue durability diagnostics not available in public liveness payload"
		}
		return doctorCheck{
			Name:    "project_patch_queue_durability",
			Status:  status,
			Message: message,
		}
	}

	proof := *payload.PatchQueue
	detailsOut := map[string]any{
		"contract":                  proof.Contract,
		"state":                     proof.State,
		"durable":                   proof.Durable,
		"message":                   proof.Message,
		"table_present":             proof.TablePresent,
		"primary_key_present":       proof.PrimaryKeyPresent,
		"live_branch_index_present": proof.LiveBranchIndexPresent,
		"claim_index_present":       proof.ClaimIndexPresent,
		"lifecycle_columns_present": proof.LifecycleColumnsPresent,
		"missing_lifecycle_columns": proof.MissingLifecycleColumns,
		"item_count":                proof.ItemCount,
		"live_item_count":           proof.LiveItemCount,
		"claimed_item_count":        proof.ClaimedItemCount,
		"terminal_item_count":       proof.TerminalItemCount,
		"checked_at":                proof.CheckedAt,
		"digest":                    proof.Digest,
		"error":                     proof.Error,
		"required_contract":         sqlite.ProjectPatchQueueDurabilityProofContract,
	}
	if strings.TrimSpace(proof.Contract) != sqlite.ProjectPatchQueueDurabilityProofContract {
		return doctorCheck{
			Name:    "project_patch_queue_durability",
			Status:  doctorStatusFail,
			Message: "project patch queue durability contract is missing or unsupported",
			Details: detailsOut,
		}
	}
	if err := sqlite.VerifyProjectPatchQueueDurabilityProof(proof); err != nil {
		return doctorCheck{
			Name:    "project_patch_queue_durability",
			Status:  doctorStatusFail,
			Message: "project patch queue durability is inconsistent: " + err.Error(),
			Details: detailsOut,
		}
	}

	switch strings.ToLower(strings.TrimSpace(proof.State)) {
	case "ok":
		if !proof.Durable {
			return doctorCheck{
				Name:    "project_patch_queue_durability",
				Status:  doctorStatusFail,
				Message: "project patch queue durability reports ok without durable proof",
				Details: detailsOut,
			}
		}
		return doctorCheck{
			Name:    "project_patch_queue_durability",
			Status:  doctorStatusPass,
			Message: firstNonEmpty(proof.Message, "project patch queue durability primitives are present"),
			Details: detailsOut,
		}
	case "degraded", "unsupported":
		return doctorCheck{
			Name:    "project_patch_queue_durability",
			Status:  doctorStatusWarn,
			Message: firstNonEmpty(proof.Message, "project patch queue durability is not fully proven"),
			Details: detailsOut,
		}
	case "error":
		return doctorCheck{
			Name:    "project_patch_queue_durability",
			Status:  doctorStatusFail,
			Message: firstNonEmpty(proof.Message, "project patch queue durability proof failed"),
			Details: detailsOut,
		}
	default:
		return doctorCheck{
			Name:    "project_patch_queue_durability",
			Status:  doctorStatusWarn,
			Message: "project patch queue durability reported unexpected state " + strings.TrimSpace(proof.State),
			Details: detailsOut,
		}
	}
}

func checkRepoMutationActivationFromDetails(details map[string]any) doctorCheck {
	payload, ok := serviceHealthPayloadFromDetails(details)
	if !ok {
		return doctorCheck{
			Name:    "repo_mutation_activation",
			Status:  doctorStatusPass,
			Message: "repo mutation activation diagnostics not available in health payload",
		}
	}

	diag := payload.RepoMutation
	if strings.TrimSpace(diag.Schema) == "" {
		status := doctorStatusWarn
		message := "repo mutation activation diagnostics missing from authenticated diagnostics payload"
		if isLimitedPublicHealthPayload(payload) {
			status = doctorStatusPass
			message = "repo mutation activation diagnostics not available in public liveness payload"
		}
		return doctorCheck{
			Name:    "repo_mutation_activation",
			Status:  status,
			Message: message,
		}
	}

	detailsOut := map[string]any{
		"schema":                    diag.Schema,
		"status":                    diag.Status,
		"mutation_allowed":          diag.MutationAllowed,
		"authority_mode":            diag.AuthorityMode,
		"context_mode":              diag.ContextMode,
		"reviewer_mesh_mode":        diag.ReviewerMeshMode,
		"live_verifier_enabled":     diag.LiveMutationVerifierEnabled,
		"live_verifier_source":      diag.LiveMutationVerifierSource,
		"live_actuator_enabled":     diag.LiveMutationActuatorEnabled,
		"live_actuator_source":      diag.LiveMutationActuatorSource,
		"source":                    diag.Source,
		"source_error":              diag.SourceError,
		"candidate":                 diag.Candidate,
		"worktree_identity":         diag.WorktreeIdentity,
		"mutation_binding_evidence": diag.MutationBindingEvidence,
		"materialization_preflight": diag.MaterializationPreflight,
		"direct_merge_disabled":     diag.DirectMergeDisabled,
		"blocking_reasons":          diag.BlockingReasons,
		"digest":                    diag.Digest,
	}
	if strings.TrimSpace(diag.Schema) != repoauthority.MutationActivationGateSchemaVersion {
		detailsOut["required_schema"] = repoauthority.MutationActivationGateSchemaVersion
		return doctorCheck{
			Name:    "repo_mutation_activation",
			Status:  doctorStatusFail,
			Message: "repo mutation activation contract is missing or unsupported",
			Details: detailsOut,
		}
	}
	if err := repoauthority.VerifyMutationActivationGateResult(diag); err != nil {
		return doctorCheck{
			Name:    "repo_mutation_activation",
			Status:  doctorStatusFail,
			Message: "repo mutation activation is inconsistent: " + err.Error(),
			Details: detailsOut,
		}
	}
	if diag.Status == repoauthority.MutationActivationStatusBlocked && !diag.MutationAllowed {
		message := "repo mutation activation is blocked fail-closed"
		if len(diag.BlockingReasons) > 0 {
			message += ": " + strings.Join(diag.BlockingReasons, "; ")
		}
		return doctorCheck{
			Name:    "repo_mutation_activation",
			Status:  doctorStatusPass,
			Message: message,
			Details: detailsOut,
		}
	}
	if diag.Status == repoauthority.MutationActivationStatusReady && diag.MutationAllowed {
		return doctorCheck{
			Name:    "repo_mutation_activation",
			Status:  doctorStatusPass,
			Message: "repo mutation activation is ready",
			Details: detailsOut,
		}
	}
	return doctorCheck{
		Name:    "repo_mutation_activation",
		Status:  doctorStatusWarn,
		Message: fmt.Sprintf("repo mutation activation has unexpected status %q", diag.Status),
		Details: detailsOut,
	}
}

func checkRepoMutationActuatorHealthFromDetails(details map[string]any) doctorCheck {
	payload, ok := serviceHealthPayloadFromDetails(details)
	if !ok {
		return doctorCheck{
			Name:    "repo_mutation_actuator",
			Status:  doctorStatusPass,
			Message: "repo mutation actuator diagnostics not available in health payload",
		}
	}
	if payload.RepoMutationActuator == nil {
		status := doctorStatusWarn
		message := "repo mutation actuator diagnostics missing from authenticated diagnostics payload"
		if isLimitedPublicHealthPayload(payload) {
			status = doctorStatusPass
			message = "repo mutation actuator diagnostics not available in public liveness payload"
		}
		return doctorCheck{
			Name:    "repo_mutation_actuator",
			Status:  status,
			Message: message,
		}
	}

	snapshot := *payload.RepoMutationActuator
	detailsOut := map[string]any{
		"contract":                        snapshot.Contract,
		"required_contract":               sqlite.ProjectPatchQueueActuatorHealthContract,
		"state":                           snapshot.State,
		"message":                         snapshot.Message,
		"reference_at":                    snapshot.ReferenceAt,
		"started_stale_after_millis":      snapshot.StartedStaleAfterMillis,
		"started_event_count":             snapshot.StartedEventCount,
		"applied_event_count":             snapshot.AppliedEventCount,
		"open_started_count":              snapshot.OpenStartedCount,
		"stale_open_started_count":        snapshot.StaleOpenStartedCount,
		"malformed_started_payload_count": snapshot.MalformedStartedPayloadCount,
		"malformed_started_at_count":      snapshot.MalformedStartedAtCount,
		"oldest_open_started_at":          snapshot.OldestOpenStartedAt,
		"oldest_stale_open_started_at":    snapshot.OldestStaleOpenStartedAt,
		"open_started_examples":           snapshot.OpenStartedExamples,
		"stale_open_started_examples":     snapshot.StaleOpenStartedExamples,
		"error":                           snapshot.Error,
	}
	if strings.TrimSpace(snapshot.Contract) != sqlite.ProjectPatchQueueActuatorHealthContract {
		return doctorCheck{
			Name:    "repo_mutation_actuator",
			Status:  doctorStatusFail,
			Message: "repo mutation actuator health contract is missing or unsupported",
			Details: detailsOut,
		}
	}
	message := firstNonEmpty(snapshot.Message, "repo mutation actuator diagnostics are healthy")
	switch strings.ToLower(strings.TrimSpace(snapshot.State)) {
	case "", "ok":
		return doctorCheck{
			Name:    "repo_mutation_actuator",
			Status:  doctorStatusPass,
			Message: message,
			Details: detailsOut,
		}
	case "degraded":
		return doctorCheck{
			Name:    "repo_mutation_actuator",
			Status:  doctorStatusWarn,
			Message: message,
			Details: detailsOut,
		}
	case "error":
		return doctorCheck{
			Name:    "repo_mutation_actuator",
			Status:  doctorStatusFail,
			Message: message,
			Details: detailsOut,
		}
	case "unsupported":
		return doctorCheck{
			Name:    "repo_mutation_actuator",
			Status:  doctorStatusWarn,
			Message: message,
			Details: detailsOut,
		}
	default:
		return doctorCheck{
			Name:    "repo_mutation_actuator",
			Status:  doctorStatusWarn,
			Message: "repo mutation actuator health reported unexpected state " + strings.TrimSpace(snapshot.State),
			Details: detailsOut,
		}
	}
}

func checkRepoMutationActuatorDryRunFromDetails(details map[string]any) doctorCheck {
	payload, ok := serviceHealthPayloadFromDetails(details)
	if !ok {
		return doctorCheck{
			Name:    "repo_mutation_actuator_dry_run",
			Status:  doctorStatusPass,
			Message: "repo mutation actuator dry-run diagnostics not available in health payload",
		}
	}

	diag := payload.RepoMutationDryRun
	if strings.TrimSpace(diag.Schema) == "" {
		status := doctorStatusWarn
		message := "repo mutation actuator dry-run diagnostics missing from authenticated diagnostics payload"
		if isLimitedPublicHealthPayload(payload) {
			status = doctorStatusPass
			message = "repo mutation actuator dry-run diagnostics not available in public liveness payload"
		}
		return doctorCheck{
			Name:    "repo_mutation_actuator_dry_run",
			Status:  status,
			Message: message,
		}
	}

	detailsOut := map[string]any{
		"schema":                                 diag.Schema,
		"status":                                 diag.Status,
		"source":                                 diag.Source,
		"live_scope":                             diag.LiveScope,
		"allowed_change_kinds":                   diag.AllowedChangeKinds,
		"observed_change_kinds":                  diag.ObservedChangeKinds,
		"unsupported_change_kinds":               diag.UnsupportedChangeKinds,
		"activation_digest":                      diag.ActivationDigest,
		"activation_status":                      diag.ActivationStatus,
		"materialization_authority_proof_digest": diag.MaterializationAuthorityProofDigest,
		"activation_mutation_allowed":            diag.ActivationMutationAllowed,
		"verifier_ready":                         diag.VerifierReady,
		"actuator_enabled":                       diag.ActuatorEnabled,
		"would_mutate":                           diag.WouldMutate,
		"mutation_executed":                      diag.MutationExecuted,
		"blocking_reasons":                       diag.BlockingReasons,
		"digest":                                 diag.Digest,
	}
	if strings.TrimSpace(diag.Schema) != repoauthority.MutationActuatorDryRunSchemaVersion {
		detailsOut["required_schema"] = repoauthority.MutationActuatorDryRunSchemaVersion
		return doctorCheck{
			Name:    "repo_mutation_actuator_dry_run",
			Status:  doctorStatusFail,
			Message: "repo mutation actuator dry-run contract is missing or unsupported",
			Details: detailsOut,
		}
	}
	if err := repoauthority.VerifyMutationActuatorDryRunResult(diag); err != nil {
		return doctorCheck{
			Name:    "repo_mutation_actuator_dry_run",
			Status:  doctorStatusFail,
			Message: "repo mutation actuator dry-run is inconsistent: " + err.Error(),
			Details: detailsOut,
		}
	}
	expectedDiag := collectRepoMutationActuatorDryRunDiagnostics(payload.RepoMutation)
	if diag.Digest != expectedDiag.Digest {
		detailsOut["expected_digest"] = expectedDiag.Digest
		detailsOut["expected_status"] = expectedDiag.Status
		detailsOut["expected_activation_digest"] = expectedDiag.ActivationDigest
		return doctorCheck{
			Name:    "repo_mutation_actuator_dry_run",
			Status:  doctorStatusFail,
			Message: "repo mutation actuator dry-run does not match repo mutation activation diagnostics",
			Details: detailsOut,
		}
	}
	if diag.Status == repoauthority.MutationActuatorDryRunStatusReady {
		return doctorCheck{
			Name:    "repo_mutation_actuator_dry_run",
			Status:  doctorStatusPass,
			Message: "repo mutation actuator dry-run is verifier-ready and did not mutate",
			Details: detailsOut,
		}
	}
	if diag.Status == repoauthority.MutationActuatorDryRunStatusBlocked {
		message := "repo mutation actuator dry-run is blocked fail-closed"
		if len(diag.BlockingReasons) > 0 {
			message += ": " + strings.Join(diag.BlockingReasons, "; ")
		}
		return doctorCheck{
			Name:    "repo_mutation_actuator_dry_run",
			Status:  doctorStatusPass,
			Message: message,
			Details: detailsOut,
		}
	}
	return doctorCheck{
		Name:    "repo_mutation_actuator_dry_run",
		Status:  doctorStatusWarn,
		Message: fmt.Sprintf("repo mutation actuator dry-run has unexpected status %q", diag.Status),
		Details: detailsOut,
	}
}

func checkAuthorityNodeFromDetails(details map[string]any) doctorCheck {
	payload, ok := serviceHealthPayloadFromDetails(details)
	if !ok {
		return doctorCheck{
			Name:    "authority_node",
			Status:  doctorStatusPass,
			Message: "authority node diagnostics not available in health payload",
		}
	}

	diag := payload.AuthorityNode
	state := strings.ToLower(strings.TrimSpace(diag.State))
	status := doctorStatusPass
	message := "authority node diagnostics are healthy"

	switch state {
	case "":
		status = doctorStatusWarn
		message = "authority node diagnostics missing from health payload"
	case "ok":
		nodeStatus := strings.ToUpper(strings.TrimSpace(diag.Status))
		if nodeStatus != "" && nodeStatus != "ONLINE" {
			status = doctorStatusWarn
			message = fmt.Sprintf("authority runtime node status is %s", nodeStatus)
		}
	case "degraded":
		status = doctorStatusWarn
		message = strings.TrimSpace(diag.Message)
		if message == "" {
			message = "authority node diagnostics reported degraded authority health"
		}
	case "missing":
		status = doctorStatusWarn
		message = strings.TrimSpace(diag.Message)
		if message == "" {
			message = "authority node identity is missing"
		}
	case "error":
		status = doctorStatusFail
		message = strings.TrimSpace(diag.Message)
		if message == "" {
			message = "authority node diagnostics reported an error"
		}
	default:
		status = doctorStatusWarn
		message = strings.TrimSpace(diag.Message)
		if message == "" {
			message = "authority node diagnostics reported non-healthy state " + state
		}
	}

	return doctorCheck{
		Name:    "authority_node",
		Status:  status,
		Message: message,
		Details: map[string]any{
			"state":             diag.State,
			"message":           diag.Message,
			"authority_node_id": diag.AuthorityNodeID,
			"node_kind":         diag.NodeKind,
			"host_label":        diag.HostLabel,
			"boot_instance_id":  diag.BootInstanceID,
			"status":            diag.Status,
		},
	}
}

func checkAuthorityLeaseFromDetails(details map[string]any) doctorCheck {
	payload, ok := serviceHealthPayloadFromDetails(details)
	if !ok {
		return doctorCheck{
			Name:    "authority_lease",
			Status:  doctorStatusPass,
			Message: "authority lease diagnostics not available in health payload",
		}
	}

	diag := payload.AuthorityLease
	state := strings.ToLower(strings.TrimSpace(diag.State))
	status := doctorStatusPass
	message := "authority lease diagnostics are healthy"

	switch state {
	case "":
		status = doctorStatusWarn
		message = "authority lease diagnostics missing from health payload"
	case "ok":
		switch {
		case diag.RenewDue > 0:
			status = doctorStatusWarn
			message = strings.TrimSpace(diag.Message)
			if message == "" {
				message = fmt.Sprintf("local authority lease diagnostics report renew_due=%d", diag.RenewDue)
			}
		case diag.TotalHeld == 0:
			message = strings.TrimSpace(diag.Message)
			if message == "" {
				message = "local authority node holds no workspace leases"
			}
		}
	case "degraded":
		status = doctorStatusWarn
		message = strings.TrimSpace(diag.Message)
		if message == "" {
			message = "authority lease diagnostics reported degraded lease health"
		}
	case "missing":
		status = doctorStatusWarn
		message = strings.TrimSpace(diag.Message)
		if message == "" {
			message = "authority lease diagnostics are missing"
		}
	case "error":
		status = doctorStatusFail
		message = strings.TrimSpace(diag.Message)
		if message == "" {
			message = "authority lease diagnostics reported an error"
		}
	default:
		status = doctorStatusWarn
		message = strings.TrimSpace(diag.Message)
		if message == "" {
			message = "authority lease diagnostics reported non-healthy state " + state
		}
	}

	return doctorCheck{
		Name:    "authority_lease",
		Status:  status,
		Message: message,
		Details: map[string]any{
			"state":                   diag.State,
			"message":                 diag.Message,
			"scope":                   diag.Scope,
			"reference_at":            diag.ReferenceAt,
			"local_authority_node_id": diag.LocalAuthorityNodeID,
			"total_held":              diag.TotalHeld,
			"foreign_live":            diag.ForeignLive,
			"healthy":                 diag.Healthy,
			"renew_due":               diag.RenewDue,
			"grace":                   diag.Grace,
			"stale":                   diag.Stale,
			"problems":                diag.Problems,
		},
	}
}

func checkRuntimeWorkGateFromDetails(details map[string]any) doctorCheck {
	payload, ok := serviceHealthPayloadFromDetails(details)
	if !ok || payload.RuntimeWorkGate == nil {
		return doctorCheck{
			Name:    "runtime_work_gate",
			Status:  doctorStatusPass,
			Message: "runtime work-gate diagnostics not present in service payload",
		}
	}

	diag := payload.RuntimeWorkGate
	detailsOut := map[string]any{
		"work_type":                 diag.WorkType,
		"work_coordination_state":   diag.WorkCoordinationState,
		"work_gate_state":           diag.WorkGateState,
		"work_gate_type":            diag.WorkGateType,
		"work_gate_reason":          diag.WorkGateReason,
		"work_gate_needed_from":     diag.WorkGateNeededFrom,
		"work_gate_summary":         diag.WorkGateSummary,
		"profile_gate_state":        diag.ProfileGateState,
		"profile_gate_summary":      diag.ProfileGateSummary,
		"bootstrap_work_fallback":   diag.BootstrapWorkFallback,
		"fallback_can_consume_work": runtimeWorkGateFallbackCanConsumeWork(diag),
	}
	if runtimeWorkGateFallbackCanConsumeWork(diag) {
		return doctorCheck{
			Name:    "runtime_work_gate",
			Status:  doctorStatusWarn,
			Message: "bootstrap compatibility selector can still consume work",
			Details: detailsOut,
		}
	}

	workType := strings.TrimSpace(diag.WorkType)
	message := "runtime work-gate diagnostics show bootstrap fallback cannot consume work"
	if workType != "" && workType != "not_evaluated" {
		message = fmt.Sprintf("runtime work gate visible: %s; bootstrap fallback cannot consume work", workType)
	}
	return doctorCheck{
		Name:    "runtime_work_gate",
		Status:  doctorStatusPass,
		Message: message,
		Details: detailsOut,
	}
}

func checkTopLevelSemanticsFromDetails(details map[string]any) []doctorCheck {
	var semChecks []doctorCheck

	payload, ok := serviceHealthPayloadFromDetails(details)
	if !ok {
		return semChecks
	}

	if topLevelSemanticsPresent(payload.Semantics) {
		readinessStatus := serviceReadinessStatus(payload.Semantics.Readiness)
		if !diagnosticSignalPresent(payload.Semantics.DeploymentReadiness) &&
			strings.EqualFold(strings.TrimSpace(payload.Semantics.Degraded.State), "degraded") {
			readinessStatus = doctorStatusFail
		}
		semChecks = append(semChecks, doctorCheck{
			Name:    "service_liveness",
			Status:  serviceLivenessStatus(payload.Semantics.Liveness),
			Message: fmt.Sprintf("liveness state=%s (%s)", payload.Semantics.Liveness.State, payload.Semantics.Liveness.Message),
		})
		semChecks = append(semChecks, doctorCheck{
			Name:    "service_readiness",
			Status:  readinessStatus,
			Message: fmt.Sprintf("readiness state=%s (%s)", payload.Semantics.Readiness.State, payload.Semantics.Readiness.Message),
		})
		semChecks = append(semChecks, doctorCheck{
			Name:    "service_deployment_readiness",
			Status:  serviceDeploymentReadinessStatus(payload.Semantics.DeploymentReadiness),
			Message: fmt.Sprintf("deployment_readiness state=%s (%s)", payload.Semantics.DeploymentReadiness.State, payload.Semantics.DeploymentReadiness.Message),
		})
		if diagnosticSignalPresent(payload.Semantics.Degraded) {
			status := doctorStatusPass
			if strings.EqualFold(strings.TrimSpace(payload.Semantics.Degraded.State), "degraded") {
				status = doctorStatusWarn
			}
			semChecks = append(semChecks, doctorCheck{
				Name:    "service_degraded",
				Status:  status,
				Message: fmt.Sprintf("degraded state=%s (%s)", payload.Semantics.Degraded.State, payload.Semantics.Degraded.Message),
			})
		}
		return semChecks
	}

	if !isLimitedPublicHealthPayload(payload) {
		return semChecks
	}

	liveStatus := doctorStatusPass
	if payload.Status != "ok" && payload.Status != "informational" {
		liveStatus = doctorStatusWarn
	}
	semChecks = append(semChecks, doctorCheck{
		Name:    "service_liveness",
		Status:  liveStatus,
		Message: fmt.Sprintf("public health state=%s", payload.Status),
	})
	semChecks = append(semChecks, doctorCheck{
		Name:    "service_readiness",
		Status:  doctorStatusWarn,
		Message: "readiness not available in public health payload; use /api/diagnostics",
	})
	semChecks = append(semChecks, doctorCheck{
		Name:    "service_deployment_readiness",
		Status:  doctorStatusFail,
		Message: "public health payload cannot satisfy deployment readiness; use /api/diagnostics",
	})

	return semChecks
}

func serviceLivenessStatus(sig DiagnosticSignal) string {
	if strings.EqualFold(strings.TrimSpace(sig.State), "ok") {
		return doctorStatusPass
	}
	return doctorStatusFail
}

func serviceReadinessStatus(sig DiagnosticSignal) string {
	switch strings.ToLower(strings.TrimSpace(sig.State)) {
	case "ok":
		return doctorStatusPass
	case "not_ready":
		return doctorStatusWarn
	case "degraded", "error":
		return doctorStatusFail
	case "":
		if diagnosticSignalPresent(sig) {
			return doctorStatusWarn
		}
		return doctorStatusWarn
	default:
		return doctorStatusWarn
	}
}

func serviceDeploymentReadinessStatus(sig DiagnosticSignal) string {
	switch strings.ToLower(strings.TrimSpace(sig.State)) {
	case "ok":
		return doctorStatusPass
	case "not_ready":
		return doctorStatusWarn
	case "degraded", "error":
		return doctorStatusFail
	case "":
		if diagnosticSignalPresent(sig) {
			return doctorStatusWarn
		}
		return doctorStatusFail
	default:
		return doctorStatusWarn
	}
}

func isLimitedPublicHealthPayload(payload serviceHealthPayload) bool {
	return strings.TrimSpace(payload.Config.WorkspaceRoot) == "" &&
		strings.TrimSpace(payload.Runtime.BinaryPath) == "" &&
		strings.TrimSpace(payload.Runtime.WorkingDirectory) == "" &&
		!topLevelSemanticsPresent(payload.Semantics)
}
