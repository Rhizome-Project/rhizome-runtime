package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestRunDoctorDBPathOverrideChecksFlagPathNotEnvDB(t *testing.T) {
	tempDir := t.TempDir()
	envDBPath := filepath.Join(tempDir, "env-a.db")
	overrideDBPath := filepath.Join(tempDir, "override-b.db")
	workspaceRoot := filepath.Join(tempDir, "workspace")
	if err := os.MkdirAll(workspaceRoot, 0o755); err != nil {
		t.Fatalf("mkdir workspace root: %v", err)
	}

	envStore, err := sqlite.NewStore(envDBPath)
	if err != nil {
		t.Fatalf("open env store: %v", err)
	}
	if err := envStore.ApplyMigrations(context.Background()); err != nil {
		_ = envStore.Close()
		t.Fatalf("apply env migrations: %v", err)
	}
	if err := envStore.Close(); err != nil {
		t.Fatalf("close env store: %v", err)
	}

	if err := os.WriteFile(overrideDBPath, []byte("not a sqlite database"), 0o644); err != nil {
		t.Fatalf("write override db fixture: %v", err)
	}

	fakeBridge := filepath.Join(findRepoRootWithFixture(t), "tests", "fixtures", "fake_executor_bridge.py")
	t.Setenv("RHIZOME_DB", envDBPath)
	t.Setenv("RHIZOME_WORKSPACE_ROOT", workspaceRoot)
	t.Setenv("RHIZOME_EXECUTOR_PYTHON", "python")
	t.Setenv("RHIZOME_EXECUTOR_BRIDGE_SCRIPT", fakeBridge)

	out, err := captureStdout(t, func() error {
		return runDoctor([]string{"--db-path", overrideDBPath})
	})
	if err == nil {
		t.Fatalf("expected doctor to fail when --db-path points at a corrupt sqlite file")
	}

	var report doctorReport
	if decodeErr := json.Unmarshal([]byte(out), &report); decodeErr != nil {
		t.Fatalf("decode doctor output: %v\n%s", decodeErr, out)
	}
	if report.Config.DBPath != overrideDBPath {
		t.Fatalf("expected config db path %q, got %q", overrideDBPath, report.Config.DBPath)
	}

	dbOpen, ok := doctorCheckByName(report.Checks, "db_open")
	if !ok {
		t.Fatalf("doctor output missing db_open check: %+v", report.Checks)
	}
	if dbOpen.Status != doctorStatusFail {
		t.Fatalf("expected db_open to fail for override db, got %+v", dbOpen)
	}
	if !strings.Contains(dbOpen.Message, "open sqlite store failed") {
		t.Fatalf("expected db_open failure from opening override db, got %+v", dbOpen)
	}
	if gotPath, _ := dbOpen.Details["path"].(string); gotPath != overrideDBPath {
		t.Fatalf("expected db_open path %q, got %q", overrideDBPath, gotPath)
	}
}

func TestRunDoctorRejectsTokenInCommandLine(t *testing.T) {
	err := runDoctor([]string{"--token", "must-not-enter-argv"})
	if err == nil || !strings.Contains(err.Error(), "command-line secrets") || !strings.Contains(err.Error(), "RHIZOME_TOKEN") {
		t.Fatalf("runDoctor(--token) error = %v, want environment-variable guidance", err)
	}
}

func TestCheckDeploymentAgentRosterAcceptsConfiguredFiveAgentTopology(t *testing.T) {
	dbPath := seedDoctorRosterDB(t, "rhizome-main", []string{"alpha", "beta", "gamma", "delta", "epsilon"})

	check := checkDeploymentAgentRosterWithExpected(dbPath, []string{"alpha", "beta", "gamma", "delta", "epsilon"})
	if check.Status != doctorStatusPass {
		t.Fatalf("expected configured five-agent topology to pass, got %+v", check)
	}
	if !strings.Contains(check.Message, "expected topology") {
		t.Fatalf("expected configurable topology message, got %+v", check)
	}
}

func TestCheckDeploymentAgentRosterDefaultStillRejectsUnexpectedDeploymentAgents(t *testing.T) {
	dbPath := seedDoctorRosterDB(t, "rhizome-main", []string{"alpha", "beta", "gamma", "delta"})

	check := checkDeploymentAgentRoster(dbPath)
	if check.Status != doctorStatusFail {
		t.Fatalf("expected default three-agent topology to reject delta, got %+v", check)
	}
	if !strings.Contains(check.Message, "alpha, beta, gamma") || !strings.Contains(check.Message, "delta") {
		t.Fatalf("expected default roster failure to describe expected and observed agents, got %+v", check)
	}
}

func TestResolveExpectedDeploymentAgentIDsUsesEnvAndDedupes(t *testing.T) {
	t.Setenv("RHIZOME_DEPLOYMENT_EXPECTED_AGENTS", "alpha, beta, alpha, gamma")

	got := resolveExpectedDeploymentAgentIDs("")
	want := []string{"alpha", "beta", "gamma"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestCollectDoctorReportSkipsTopologyDriftOutsideStrictPreflight(t *testing.T) {
	dbPath := seedDoctorRosterDB(t, "rhizome-main", nil)
	workspaceRoot := t.TempDir()
	fakeBridge := filepath.Join(findRepoRootWithFixture(t), "tests", "fixtures", "fake_executor_bridge.py")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(serviceHealthPayload{
			Status: "ok",
			TS:     "2026-06-02T20:30:00Z",
			LoopReadiness: []LoopReadiness{
				{Name: loopNameDaemon, State: LoopRunning, Restarts: 0},
			},
			Extended: ExtendedReadiness{
				StuckAgents: DiagnosticSignal{State: "ok", Message: "stuck agent health is stable"},
			},
		})
	}))
	defer server.Close()

	report := collectDoctorReportWithExpected(
		"trace-test",
		app.Config{
			DBPath:               dbPath,
			WorkspaceRoot:        workspaceRoot,
			ExecutorPython:       "python",
			ExecutorBridgeScript: fakeBridge,
		},
		"",
		server.URL,
		"",
		false,
		nil,
	)

	if _, ok := doctorCheckByName(report.Checks, "agent_roster"); ok {
		t.Fatalf("plain health doctor must not run deployment roster preflight: %+v", report.Checks)
	}
	if _, ok := doctorCheckByName(report.Checks, "topology_drift"); ok {
		t.Fatalf("plain health doctor must not run first-deployment topology drift checks: %+v", report.Checks)
	}
}

func doctorCheckByName(checks []doctorCheck, name string) (doctorCheck, bool) {
	for _, check := range checks {
		if check.Name == name {
			return check, true
		}
	}
	return doctorCheck{}, false
}

func seedDoctorRosterDB(t *testing.T, workspaceID string, agentIDs []string) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "doctor-roster.db")
	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()
	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       workspaceID,
		CreatedBy:   "test",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	for _, agentID := range agentIDs {
		if err := store.RegisterAgent(ctx, sqlite.AgentRegisterInput{
			WorkspaceID:     workspaceID,
			AgentID:         agentID,
			OwnerUserID:     "developer",
			DisplayName:     agentID,
			Role:            "generalist",
			Status:          "REGISTERED",
			ProtocolVersion: "rnar/v1",
			Capabilities:    []string{"workspace.docs"},
		}); err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
	}
	return dbPath
}
