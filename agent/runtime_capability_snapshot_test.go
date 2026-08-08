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
	"time"
)

func TestDaemonRunCapabilitySnapshotBindsRunAndRequiredSurfaces(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:                   RuntimeModeDaemon,
		Workdir:                t.TempDir(),
		WorkspaceID:            "ws-1",
		AgentID:                "agent-1",
		DisplayName:            "Agent One",
		OwnerUserID:            "owner-1",
		Role:                   "worker",
		MaxToolLoopIterations:  7,
		MaxPromptDocChars:      3000,
		MaxPromptSpecChars:     2000,
		MaxSmokeCyclesPerAgent: 2,
		MaxSmokeCyclesPerTask:  4,
	}
	cfg.ApplyDefaults()
	agent := &Agent{
		Workdir:              cfg.Workdir,
		Client:               NewRhizomeClient("http://127.0.0.1:1/rpc", "token"),
		WorkspaceID:          cfg.WorkspaceID,
		AgentID:              cfg.AgentID,
		DisableLocalShell:    !runtimeAllowsLocalShell(cfg),
		DisableLocalMutation: !runtimeAllowsLocalMutation(cfg),
		DisableLocalMemory:   true,
	}
	agent.Init()

	boot := buildDaemonBootCapabilitySnapshot(cfg, agent, time.Date(2026, 4, 21, 8, 0, 0, 0, time.UTC))
	task := WorkspaceTaskRecord{TaskID: "task-1", Title: "Task One"}
	session := AgentSessionStateRecord{SessionID: "session-1", TaskID: "task-1", Status: "ACTIVE"}
	run := buildDaemonRunCapabilitySnapshot(cfg, agent, boot.SnapshotID, task, session, "run-1", &AgentWorkPacket{
		WorkType:            "resume_session",
		ClaimAction:         "reuse_claim",
		SessionAction:       "resume_inactive",
		PreferredTransition: "continue",
		WhyNow:              "test",
	}, time.Date(2026, 4, 21, 8, 1, 0, 0, time.UTC))

	if run.Schema != daemonCapabilitySnapshotSchema {
		t.Fatalf("schema = %q, want %q", run.Schema, daemonCapabilitySnapshotSchema)
	}
	if run.SnapshotKind != "run" {
		t.Fatalf("snapshot kind = %q, want run", run.SnapshotKind)
	}
	if want := daemonRunCapabilitySnapshotID(cfg, task, session, "run-1"); run.SnapshotID != want {
		t.Fatalf("snapshot id = %q, want %q", run.SnapshotID, want)
	}
	if run.Binding.TaskID != "task-1" || run.Binding.SessionID != "session-1" || run.Binding.RunID != "run-1" {
		t.Fatalf("run binding did not capture task/session/run: %+v", run.Binding)
	}
	if run.Binding.BootstrapSnapshotRef != boot.SnapshotID {
		t.Fatalf("bootstrap ref = %q, want %q", run.Binding.BootstrapSnapshotRef, boot.SnapshotID)
	}
	if run.Status.Overall != "enabled" {
		t.Fatalf("run snapshot should be enabled, got %+v", run.Status)
	}
	required := []string{"local_tools", "workspace_tools", "workspace_docs", "mcp", "bridges", "executor", "shell", "repo", "memory", "provider", "budget", "ui", "network"}
	for _, key := range required {
		if _, ok := run.Surfaces[key]; !ok {
			t.Fatalf("missing required surface %q in %+v", key, run.Surfaces)
		}
	}
	if got := run.Surfaces["mcp"].Status; got != "disabled" {
		t.Fatalf("mcp status = %q, want disabled", got)
	}
	if got := run.Surfaces["executor"].Status; got != "disabled" {
		t.Fatalf("executor status = %q, want disabled", got)
	}
	if got := run.Surfaces["memory"].Status; got != "disabled" {
		t.Fatalf("memory status = %q, want disabled", got)
	}
	if got := run.Surfaces["ui"].Status; got != "inspection_only" {
		t.Fatalf("ui status = %q, want inspection_only", got)
	}
	if surface := run.Surfaces["workspace_docs"]; surface.Status != "enabled" || !surface.ToolVisible || !surface.MutationAllowed {
		t.Fatalf("workspace docs should expose daemon-safe materialization, got %+v", surface)
	}
	if run.Surfaces["workspace_docs"].AutonomyState != "enabled" ||
		run.Surfaces["workspace_tools"].AutonomyState != "advisory" ||
		run.Surfaces["executor"].AutonomyState != "hard_disabled" {
		t.Fatalf("unexpected autonomy-first capability states: workspace_docs=%+v workspace_tools=%+v executor=%+v", run.Surfaces["workspace_docs"], run.Surfaces["workspace_tools"], run.Surfaces["executor"])
	}
	if !hasCapabilityTool(run.PromptContract.EnabledToolNames, "read_file") {
		t.Fatalf("expected read_file in prompt contract enabled tools, got %+v", run.PromptContract.EnabledToolNames)
	}
	if !hasCapabilityTool(run.PromptContract.EnabledToolNames, "workspace_doc_put") {
		t.Fatalf("expected workspace_doc_put in prompt contract enabled tools, got %+v", run.PromptContract.EnabledToolNames)
	}
	for _, forbidden := range []string{"shell", "write_file", "memory_write", "daily_note"} {
		if hasCapabilityTool(run.PromptContract.EnabledToolNames, forbidden) {
			t.Fatalf("Program B daemon snapshot must not advertise raw mutation tool %q as enabled: %+v", forbidden, run.PromptContract.EnabledToolNames)
		}
	}
	if run.Surfaces["shell"].ToolVisible || run.Surfaces["shell"].MutationAllowed {
		t.Fatalf("shell surface must be disabled for daemon Program B snapshot: %+v", run.Surfaces["shell"])
	}
	if run.Surfaces["repo"].ToolVisible || run.Surfaces["repo"].MutationAllowed {
		t.Fatalf("repo surface must not expose raw mutation without repo authority wrapper: %+v", run.Surfaces["repo"])
	}
	if got := capabilitySnapshotEvidenceRef(run.SnapshotID); got != "capability_snapshot:"+run.SnapshotID {
		t.Fatalf("unexpected evidence ref %q", got)
	}
	proof := daemonPromptCapabilityEvidence(run, filepath.Join(cfg.Workdir, ".rhizome", "capability_snapshots", run.SnapshotID+".json"))
	if proof["contract"] != daemonPromptCapabilityEvidenceContract {
		t.Fatalf("unexpected prompt capability evidence contract %+v", proof)
	}
	if proof["prompt_compiler_status"] != daemonPromptCompilerStatusConverged ||
		proof["c2_1_convergence"] != daemonPromptCompilerConvergenceAccepted ||
		proof["deployment_evidence"] != daemonPromptCompilerDeploymentEvidenceAccepted {
		t.Fatalf("prompt capability evidence must explicitly claim accepted daemon convergence, got %+v", proof)
	}
	if proof["capability_snapshot_ref"] != capabilitySnapshotEvidenceRef(run.SnapshotID) {
		t.Fatalf("prompt capability evidence snapshot ref mismatch: %+v", proof)
	}
	digest, _ := proof["projection_digest"].(string)
	if !strings.HasPrefix(digest, "sha256:") || len(digest) != len("sha256:")+64 {
		t.Fatalf("invalid prompt projection digest in evidence: %+v", proof)
	}
}

func TestDaemonCapabilityPromptProjectionMatchesSharedGolden(t *testing.T) {
	golden, err := os.ReadFile(filepath.Join("testdata", "active_capability_projection.golden"))
	if err != nil {
		t.Fatalf("read projection golden: %v", err)
	}
	got := renderDaemonCapabilityPromptProjection(testDaemonCapabilityProjectionGoldenSnapshot())
	want := normalizeCapabilityProjectionGolden(golden)
	if got != want {
		t.Fatalf("daemon capability projection drifted from shared golden\nwant:\n%s\n\ngot:\n%s", want, got)
	}
	if digest := daemonCapabilityProjectionDigest(got); digest != "sha256:51ddbfdb572fb605ff48d0dc66e5533cc506e57c1414cb98dce96b66a469cf66" {
		t.Fatalf("projection digest drifted: %s", digest)
	}
}

func TestDaemonCapabilityPromptProjectionWorkspaceToolsMatchesSharedGolden(t *testing.T) {
	golden, err := os.ReadFile(filepath.Join("testdata", "active_capability_projection_workspace_tools.golden"))
	if err != nil {
		t.Fatalf("read workspace-tools projection golden: %v", err)
	}
	got := renderDaemonCapabilityPromptProjection(testDaemonCapabilityProjectionWorkspaceToolsGoldenSnapshot())
	want := normalizeCapabilityProjectionGolden(golden)
	if got != want {
		t.Fatalf("daemon workspace-tools projection drifted from shared golden\nwant:\n%s\n\ngot:\n%s", want, got)
	}
	if digest := daemonCapabilityProjectionDigest(got); digest != "sha256:0a029e7dd291006f09afae53ecad33b97928f92ec4f8d0c5ff34b5d4e0c8994b" {
		t.Fatalf("workspace-tools projection digest drifted: %s", digest)
	}
}

func normalizeCapabilityProjectionGolden(data []byte) string {
	return strings.TrimSpace(strings.ReplaceAll(string(data), "\r\n", "\n"))
}

func TestDaemonCapabilitySnapshotFiltersStaleRawMutationTools(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
		OwnerUserID: "owner-1",
		Capabilities: []string{
			"tool.call",
			"local.shell",
			"local.fs.write",
			"local.fs.read",
		},
	}
	cfg.ApplyDefaults()
	agent := &Agent{
		Workdir:              cfg.Workdir,
		WorkspaceID:          cfg.WorkspaceID,
		AgentID:              cfg.AgentID,
		DisableLocalShell:    false,
		DisableLocalMutation: false,
		DisableLocalMemory:   false,
	}
	agent.Init()
	agent.SetDynamicTools([]Tool{
		staticTestTool{name: "mcp__notion__write_page"},
		staticTestTool{name: "executor.run_node"},
		staticTestTool{name: "dev.implement"},
		staticTestTool{name: "bridge.legacy_provider"},
	})

	snapshot := buildDaemonRunCapabilitySnapshot(cfg, agent, "cap_boot", WorkspaceTaskRecord{TaskID: "task-1"}, AgentSessionStateRecord{SessionID: "session-1", TaskID: "task-1", Status: "ACTIVE"}, "run-1", nil, time.Date(2026, 4, 21, 9, 0, 0, 0, time.UTC))

	for _, forbidden := range []string{
		"shell",
		"write_file",
		"memory_write",
		"daily_note",
		"mcp__notion__write_page",
		"executor.run_node",
		"dev.implement",
		"bridge.legacy_provider",
	} {
		if hasCapabilityTool(snapshot.PromptContract.EnabledToolNames, forbidden) {
			t.Fatalf("raw mutation tool %q leaked into enabled tools: %+v", forbidden, snapshot.PromptContract.EnabledToolNames)
		}
	}
	disabledReasons := map[string]string{}
	for _, item := range snapshot.PromptContract.DisabledToolNames {
		disabledReasons[item.Name] = item.ReasonCode
	}
	for name, wantReason := range map[string]string{
		"shell":             "program_b.raw_shell_disabled",
		"write_file":        "program_b.raw_repo_mutation_disabled",
		"memory_write":      "program_b.local_memory_write_disabled",
		"daily_note":        "program_b.local_memory_write_disabled",
		"mcp__*":            "program_b.unknown_mcp_disabled",
		"executor.run_node": "program_b.raw_executor_disabled",
		"dev.implement":     "program_b.bridge_mutation_disabled",
	} {
		if got := disabledReasons[name]; got != wantReason {
			t.Fatalf("disabled reason for %s = %q, want %q; disabled=%+v", name, got, wantReason, snapshot.PromptContract.DisabledToolNames)
		}
	}
	for _, declared := range snapshot.Identity.AgentCapabilitiesDeclared {
		if declared == "local.shell" || declared == "local.fs.write" {
			t.Fatalf("daemon capability identity should not declare raw mutation capability %q: %+v", declared, snapshot.Identity.AgentCapabilitiesDeclared)
		}
	}
}

func TestDaemonCapabilitySnapshotHonorsManagedLocalMutationOptIn(t *testing.T) {
	t.Setenv(managedAgentEnvFlag, "1")
	t.Setenv(managedAgentAllowLocalShellFlag, "1")
	t.Setenv(managedAgentAllowLocalMutationFlag, "1")
	t.Setenv("RHIZOME_OWNER_USER_ID", "owner-1")

	cfg := RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
		OwnerUserID: "owner-1",
		Capabilities: []string{
			"tool.call",
			"local.shell",
			"local.fs.write",
			"local.fs.read",
		},
	}
	cfg.ApplyDefaults()
	agent := &Agent{
		Workdir:              cfg.Workdir,
		WorkspaceID:          cfg.WorkspaceID,
		AgentID:              cfg.AgentID,
		DisableLocalShell:    !runtimeAllowsLocalShell(cfg),
		DisableLocalMutation: !runtimeAllowsLocalMutation(cfg),
		DisableLocalMemory:   true,
	}
	agent.Init()

	snapshot := buildDaemonRunCapabilitySnapshot(cfg, agent, "cap_boot", WorkspaceTaskRecord{TaskID: "task-1"}, AgentSessionStateRecord{SessionID: "session-1", TaskID: "task-1", Status: "ACTIVE"}, "run-1", nil, time.Date(2026, 4, 21, 9, 30, 0, 0, time.UTC))

	for _, want := range []string{"shell", "write_file"} {
		if !hasCapabilityTool(snapshot.PromptContract.EnabledToolNames, want) {
			t.Fatalf("managed daemon opt-in should expose %s, enabled=%+v disabled=%+v", want, snapshot.PromptContract.EnabledToolNames, snapshot.PromptContract.DisabledToolNames)
		}
		if _, disabled := capabilityDisabledToolNameMatch(want, snapshot.PromptContract.DisabledToolNames); disabled {
			t.Fatalf("managed daemon opt-in should not mark %s disabled: %+v", want, snapshot.PromptContract.DisabledToolNames)
		}
	}
	if surface := snapshot.Surfaces["repo"]; surface.Status != "degraded" || !surface.ToolVisible || !surface.MutationAllowed {
		t.Fatalf("repo surface should expose scoped local mutation under managed opt-in, got %+v", surface)
	}
	if surface := snapshot.Surfaces["shell"]; surface.Status != "enabled" || !surface.ToolVisible {
		t.Fatalf("shell surface should expose managed shell under opt-in, got %+v", surface)
	}
	for _, want := range []string{"local.shell", "local.fs.write"} {
		if !containsString(snapshot.Identity.AgentCapabilitiesDeclared, want) {
			t.Fatalf("declared capabilities should retain %s under managed opt-in: %+v", want, snapshot.Identity.AgentCapabilitiesDeclared)
		}
	}
}

func TestDaemonCapabilitySnapshotSurfacesToolBundleMetadataAndDiagnostics(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
		OwnerUserID: "owner-1",
	}
	t.Setenv("RHIZOME_TOOL_BUNDLE_HEALTHCHECK_HELPER", "1")
	writeInstalledToolBundleManifest(t, filepath.Join(cfg.Workdir, ".runtime-config", "tool-bundles", "browser_visual_probe"), InstalledToolBundleManifest{
		SchemaVersion:    "tool_bundle.v2",
		Name:             "browser_visual_probe",
		Description:      "Capture screenshots",
		Command:          []string{os.Args[0], "-test.run=TestInstalledToolBundleHelper", "--"},
		Parameters:       map[string]any{"type": "object"},
		Version:          "2.0.0",
		CapabilitySuites: []string{"browser_read_only", "screenshot_capture"},
		Dependencies:     []InstalledToolBundleDependency{{Name: "node", Kind: "executable", Required: true}},
		Healthcheck: &InstalledToolBundleHealthcheck{
			Command:        []string{os.Args[0], "-test.run=TestInstalledToolBundleHealthcheckHelper", "--"},
			TimeoutSeconds: 5,
		},
		ArtifactContracts: []InstalledToolBundleArtifactContract{{
			Name: "probe_report", Type: "application/json", Path: "probe-report.json", Required: true,
		}},
	})
	badDir := filepath.Join(cfg.Workdir, "tools", "bad")
	if err := os.MkdirAll(badDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(badDir, installedToolBundleManifestName), []byte(`{"name":`), 0o644); err != nil {
		t.Fatal(err)
	}
	agent := &Agent{
		Workdir:              cfg.Workdir,
		WorkspaceID:          cfg.WorkspaceID,
		AgentID:              cfg.AgentID,
		DisableLocalShell:    true,
		DisableLocalMutation: true,
		DisableLocalMemory:   true,
	}
	agent.Init()

	snapshot := buildDaemonRunCapabilitySnapshot(cfg, agent, "cap_boot", WorkspaceTaskRecord{TaskID: "task-1"}, AgentSessionStateRecord{SessionID: "session-1", TaskID: "task-1", Status: "ACTIVE"}, "run-1", nil, time.Date(2026, 4, 21, 10, 0, 0, 0, time.UTC))
	surface, ok := snapshot.Surfaces["tool_bundles"]
	if !ok {
		t.Fatalf("expected tool_bundles surface in %+v", snapshot.Surfaces)
	}
	if surface.Status != "degraded" || !surface.ToolVisible {
		t.Fatalf("tool bundle surface should be degraded but executable bundles visible, got %+v", surface)
	}
	raw, err := json.Marshal(surface.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	metadata := string(raw)
	for _, want := range []string{"browser_visual_probe", "browser_read_only", "screenshot_capture", "probe_report", "healthcheck_passed", "node:executable:required", "malformed_manifest"} {
		if !strings.Contains(metadata, want) {
			t.Fatalf("expected tool bundle metadata to contain %q, got %s", want, metadata)
		}
	}
}

func testDaemonCapabilityProjectionGoldenSnapshot() DaemonCapabilitySnapshot {
	return DaemonCapabilitySnapshot{
		Schema:       daemonCapabilitySnapshotSchema,
		SnapshotID:   "cap_projection_golden",
		SnapshotKind: "run",
		Status:       CapabilitySnapshotStatus{Overall: "enabled"},
		PromptContract: CapabilityPromptContract{
			ContractID:             daemonPromptCapabilityPromptContract,
			EnabledToolNames:       []string{"write_file", "read_file", "shell", "list_directory"},
			DisabledToolNames:      testCapabilityProjectionDisabledTools(),
			InspectionOnlySurfaces: []string{"web", "tui"},
			BudgetSummary: CapabilityBudgetSummary{
				MaxToolIterations:   9,
				MaxShellTimeoutSec:  30,
				MaxPromptDocChars:   1234,
				MaxPromptSpecChars:  2345,
				MaxSmokeCyclesAgent: 2,
				MaxSmokeCyclesTask:  3,
			},
			MustInclude: []string{
				"Only use enabled tools listed in this capability snapshot.",
				"Do not claim MCP, executor, browser, memory promotion, or bridge availability unless enabled here.",
				"Workspace document materialization through workspace_doc_put or structured result materialize is always daemon-safe; local shell and file writes are allowed only when listed as enabled in this snapshot.",
			},
		},
		Surfaces: map[string]CapabilitySurface{
			"executor": testCapabilityProjectionSurface("executor", "disabled", "executor.operation_ledger_required"),
			"memory":   testCapabilityProjectionSurface("memory", "disabled", "program_a.no_direct_memory_promotion", "memory.local.disabled_in_daemon"),
			"ui":       testCapabilityProjectionSurface("ui", "inspection_only", "program_a.ui_no_authority_bearing_actions"),
		},
	}
}

func testDaemonCapabilityProjectionWorkspaceToolsGoldenSnapshot() DaemonCapabilitySnapshot {
	snapshot := testDaemonCapabilityProjectionGoldenSnapshot()
	snapshot.SnapshotID = "cap_workspace_tools_projection_golden"
	snapshot.PromptContract.EnabledToolNames = []string{"fake_ledger_tool", "read_file", "list_directory"}
	snapshot.PromptContract.InspectionOnlySurfaces = []string{"workspace_tools", "bridges", "ui"}
	snapshot.Surfaces["workspace_tools"] = testCapabilityProjectionSurface("workspace_tools", "degraded", "deployment.opt_in_workspace_tools")
	return snapshot
}

func testCapabilityProjectionDisabledTools() []CapabilityDisabledToolName {
	return []CapabilityDisabledToolName{
		{Name: "write_file", ReasonCode: "program_b.raw_repo_mutation_disabled"},
		{Name: "shell", ReasonCode: "program_b.raw_shell_disabled"},
		{Name: "mcp__*", ReasonCode: "program_b.unknown_mcp_disabled"},
	}
}

func testCapabilityProjectionSurface(surfaceID, status string, reasonCodes ...string) CapabilitySurface {
	reasons := make([]CapabilityDisabledReason, 0, len(reasonCodes))
	for _, code := range reasonCodes {
		reasons = append(reasons, CapabilityDisabledReason{Code: code})
	}
	return CapabilitySurface{
		SurfaceID:       surfaceID,
		Status:          status,
		DisabledReasons: reasons,
	}
}

func TestNewRuntimeDaemonDisablesRawLocalMutationRegistry(t *testing.T) {
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
		OwnerUserID: "owner-1",
	}, nil)

	if runtime == nil || runtime.agent == nil || runtime.agent.registry == nil {
		t.Fatal("expected runtime agent registry")
	}
	for _, forbidden := range []string{"shell", "write_file", "memory_write", "daily_note"} {
		if _, ok := runtime.agent.registry.Get(forbidden); ok {
			t.Fatalf("daemon registry must omit raw mutation tool %q", forbidden)
		}
	}
	for _, allowed := range []string{"read_file", "list_directory", "workspace_doc_get", "workspace_doc_put"} {
		if _, ok := runtime.agent.registry.Get(allowed); !ok {
			t.Fatalf("daemon registry should retain daemon-safe tool %q", allowed)
		}
	}
}

func TestRuntimeBindAgentRuntimeStateRefreshesManagedLocalToolFlags(t *testing.T) {
	t.Setenv(managedAgentEnvFlag, "1")
	t.Setenv(managedAgentAllowLocalShellFlag, "1")
	t.Setenv(managedAgentAllowLocalMutationFlag, "1")
	t.Setenv("RHIZOME_OWNER_USER_ID", "owner-1")

	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
		OwnerUserID: "owner-1",
	}, nil)
	t.Cleanup(func() { _ = runtime.Close() })

	for _, want := range []string{"shell", "write_file"} {
		if _, ok := runtime.agent.registry.Get(want); !ok {
			t.Fatalf("managed daemon registry should expose %s before refresh", want)
		}
	}

	runtime.agent.DisableLocalShell = true
	runtime.agent.DisableLocalMutation = true
	runtime.agent.SetDynamicTools(nil)
	for _, forbidden := range []string{"shell", "write_file"} {
		if _, ok := runtime.agent.registry.Get(forbidden); ok {
			t.Fatalf("test setup should remove %s from registry when stale disable flags are true", forbidden)
		}
	}

	runtime.bindAgentRuntimeState()
	runtime.agent.SetDynamicTools(nil)
	for _, want := range []string{"shell", "write_file"} {
		if _, ok := runtime.agent.registry.Get(want); !ok {
			t.Fatalf("bindAgentRuntimeState should refresh managed local tool flag for %s", want)
		}
	}
}

func TestPolicyAwareToolExecutorDeniesProgramBRawMutationBeforeRPCPolicy(t *testing.T) {
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
		OwnerUserID: "owner-1",
	}, nil)
	registry := NewToolRegistry()
	registry.Register(NewWriteFileTool(runtime.cfg.Workdir))

	result := runtime.policyAwareToolExecutor(context.Background(), registry, ToolCall{
		Function: FunctionCall{
			Name:      "write_file",
			Arguments: `{"path":"owned.txt","content":"bypass"}`,
		},
	})

	if !result.IsError || !strings.Contains(result.Output, "program_b.raw_repo_mutation_disabled") {
		t.Fatalf("expected local Program B deny result, got %+v", result)
	}
	if len(runtime.policySignals) != 1 {
		t.Fatalf("expected one recorded policy signal, got %+v", runtime.policySignals)
	}
	signal := runtime.policySignals[0]
	if signal.ToolName != "write_file" || signal.Verdict != "DENY" || !strings.Contains(signal.Description, "program_b.raw_repo_mutation_disabled") {
		t.Fatalf("unexpected policy signal %+v", signal)
	}
	if _, err := os.Stat(filepath.Join(runtime.cfg.Workdir, "owned.txt")); !os.IsNotExist(err) {
		t.Fatalf("denied write_file should not create target file; stat err=%v", err)
	}
}

func TestPolicyAwareToolExecutorTrustFirstExecutesWhenPolicyUnavailableWithAdvisory(t *testing.T) {
	runtime := NewRuntime(RuntimeConfig{
		Mode:             RuntimeModeDaemon,
		Workdir:          t.TempDir(),
		WorkspaceID:      "ws-1",
		AgentID:          "agent-1",
		OwnerUserID:      "owner-1",
		CoordinationMode: CoordinationModeTrustFirst,
	}, nil)
	registry := NewToolRegistry()
	registry.Register(NewWriteFileTool(runtime.cfg.Workdir))

	result := runtime.policyAwareToolExecutor(context.Background(), registry, ToolCall{
		Function: FunctionCall{
			Name:      "write_file",
			Arguments: `{"path":"owned.txt","content":"trusted autonomy"}`,
		},
	})

	if result.IsError || !strings.Contains(result.Output, "trust_first policy advisory") {
		t.Fatalf("expected trust-first advisory success, got %+v", result)
	}
	data, err := os.ReadFile(filepath.Join(runtime.cfg.Workdir, "owned.txt"))
	if err != nil {
		t.Fatalf("expected write_file to execute in trust-first mode: %v", err)
	}
	if string(data) != "trusted autonomy" {
		t.Fatalf("unexpected written content %q", string(data))
	}
	if len(runtime.policySignals) != 1 || runtime.policySignals[0].Verdict != "POLICY_UNAVAILABLE" {
		t.Fatalf("expected advisory policy signal to remain visible, got %+v", runtime.policySignals)
	}
}

func TestPolicyAwareToolExecutorTrustFirstExecutesDeniedPolicyWithAdvisory(t *testing.T) {
	var checks int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "workspace.policy.check" {
			t.Fatalf("unexpected method %s", req.Method)
		}
		checks++
		writeRPCResult(w, req, map[string]any{
			"check": map[string]any{
				"workspace_id": "ws-1",
				"subject_type": "agent",
				"subject_id":   "agent-1",
				"capability":   rpcString(req.Params, "capability"),
				"tool_id":      rpcString(req.Params, "tool_id"),
				"verdict":      "DENY",
			},
		})
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		Mode:             RuntimeModeDaemon,
		Workdir:          t.TempDir(),
		RhizomeRPC:       server.URL,
		RhizomeToken:     "token",
		WorkspaceID:      "ws-1",
		AgentID:          "agent-1",
		OwnerUserID:      "owner-1",
		CoordinationMode: CoordinationModeTrustFirst,
	}, nil)
	runtime.client = NewRhizomeClient(server.URL, "token")
	registry := NewToolRegistry()
	registry.Register(NewWriteFileTool(runtime.cfg.Workdir))

	result := runtime.policyAwareToolExecutor(context.Background(), registry, ToolCall{
		Function: FunctionCall{
			Name:      "write_file",
			Arguments: `{"path":"owned.txt","content":"denied but trusted"}`,
		},
	})

	if checks != 1 {
		t.Fatalf("expected one policy check, got %d", checks)
	}
	if result.IsError || !strings.Contains(result.Output, "trust_first policy advisory") {
		t.Fatalf("expected trust-first advisory success after DENY, got %+v", result)
	}
	data, err := os.ReadFile(filepath.Join(runtime.cfg.Workdir, "owned.txt"))
	if err != nil {
		t.Fatalf("expected denied write_file to execute in trust-first mode: %v", err)
	}
	if string(data) != "denied but trusted" {
		t.Fatalf("unexpected written content %q", string(data))
	}
	if len(runtime.policySignals) != 1 || runtime.policySignals[0].Verdict != "DENY" {
		t.Fatalf("expected advisory DENY signal to remain visible, got %+v", runtime.policySignals)
	}
}

func TestPostProcessToolResultAddsShellFailureGuidance(t *testing.T) {
	runtime := &Runtime{}
	result := runtime.postProcessToolResult(ToolCall{
		Function: FunctionCall{Name: "shell"},
	}, ToolResult{Output: "Access is denied.", IsError: true})

	if !result.IsError || !strings.Contains(result.Output, "Shell is trusted local execution") {
		t.Fatalf("expected shell failure guidance, got %+v", result)
	}
	if len(runtime.scratch.AdvisorySignals) != 1 || !strings.Contains(runtime.scratch.AdvisorySignals[0], "shell failed") {
		t.Fatalf("expected persisted advisory signal, got %+v", runtime.scratch.AdvisorySignals)
	}
}

func TestCaptureRunCapabilitySnapshotPersistsFileAndScratch(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:         RuntimeModeDaemon,
		Workdir:      t.TempDir(),
		WorkspaceID:  "ws-1",
		AgentID:      "agent-1",
		DisplayName:  "Agent One",
		OwnerUserID:  "owner-1",
		RhizomeToken: "token",
	}
	runtime := NewRuntime(cfg, &sequenceLLM{})
	runtime.startupCapabilitySnapshotID = "cap_boot"
	t.Cleanup(func() { _ = runtime.Close() })

	task := WorkspaceTaskRecord{TaskID: "task-1"}
	session := AgentSessionStateRecord{SessionID: "session-1", TaskID: "task-1", Status: "ACTIVE"}
	snapshot, path, err := runtime.captureRunCapabilitySnapshot(task, session, "run-1", nil)
	if err != nil {
		t.Fatalf("captureRunCapabilitySnapshot() error = %v", err)
	}
	if snapshot.SnapshotID == "" || path == "" {
		t.Fatalf("expected snapshot id and path, got id=%q path=%q", snapshot.SnapshotID, path)
	}
	if runtime.scratch.ActiveCapabilitySnapshotID != snapshot.SnapshotID {
		t.Fatalf("scratch active snapshot id = %q, want %q", runtime.scratch.ActiveCapabilitySnapshotID, snapshot.SnapshotID)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read persisted snapshot: %v", err)
	}
	var persisted DaemonCapabilitySnapshot
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatalf("decode persisted snapshot: %v", err)
	}
	if persisted.SnapshotID != snapshot.SnapshotID || persisted.Binding.RunID != "run-1" {
		t.Fatalf("persisted snapshot mismatch: %+v", persisted)
	}
}

type staticTestTool struct {
	name string
}

func (t staticTestTool) Name() string { return t.name }

func (t staticTestTool) Description() string { return "static test tool" }

func (t staticTestTool) Parameters() map[string]any {
	return map[string]any{"type": "object", "properties": map[string]any{}}
}

func (t staticTestTool) Execute(context.Context, map[string]any) *ToolResult {
	return &ToolResult{Output: "static test tool executed"}
}

func TestPersistCapabilitySnapshotOverwritesStaleExistingFile(t *testing.T) {
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
	}, &sequenceLLM{})
	t.Cleanup(func() { _ = runtime.Close() })

	stale := DaemonCapabilitySnapshot{
		Schema:       daemonCapabilitySnapshotSchema,
		SnapshotID:   "cap_same",
		SnapshotKind: "run",
		GeneratedAt:  "2026-04-21T08:00:00Z",
		Status:       CapabilitySnapshotStatus{Overall: "blocked"},
	}
	path, err := runtime.persistCapabilitySnapshot(stale)
	if err != nil {
		t.Fatalf("persist stale snapshot: %v", err)
	}
	current := stale
	current.GeneratedAt = "2026-04-21T08:01:00Z"
	current.Status = CapabilitySnapshotStatus{Overall: "enabled"}
	current.Notes = []string{"current"}
	if _, err := runtime.persistCapabilitySnapshot(current); err != nil {
		t.Fatalf("persist current snapshot: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read persisted snapshot: %v", err)
	}
	var persisted DaemonCapabilitySnapshot
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatalf("decode persisted snapshot: %v", err)
	}
	if persisted.GeneratedAt != current.GeneratedAt || persisted.Status.Overall != "enabled" || len(persisted.Notes) != 1 || persisted.Notes[0] != "current" {
		t.Fatalf("expected current snapshot to overwrite stale file, got %+v", persisted)
	}
}

func TestActiveCapabilitySnapshotPromptProjectionLoadsScratchPath(t *testing.T) {
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
	}, &sequenceLLM{})
	t.Cleanup(func() { _ = runtime.Close() })

	snapshot := buildDaemonRunCapabilitySnapshot(runtime.cfg, runtime.agent, "cap_boot", WorkspaceTaskRecord{TaskID: "task-1"}, AgentSessionStateRecord{SessionID: "session-1", TaskID: "task-1", Status: "ACTIVE"}, "run-1", nil, time.Date(2026, 4, 21, 8, 0, 0, 0, time.UTC))
	path, err := runtime.persistCapabilitySnapshot(snapshot)
	if err != nil {
		t.Fatalf("persist active snapshot: %v", err)
	}
	runtime.mu.Lock()
	runtime.scratch.ActiveCapabilitySnapshotID = snapshot.SnapshotID
	runtime.scratch.ActiveCapabilitySnapshotPath = path
	runtime.mu.Unlock()

	projection := runtime.activeCapabilitySnapshotPromptProjection(DaemonCapabilitySnapshot{SnapshotID: "cap_fallback"})
	if !strings.Contains(projection, "snapshot_id: "+snapshot.SnapshotID) {
		t.Fatalf("expected projection to load scratch active snapshot id, got:\n%s", projection)
	}
	if strings.Contains(projection, "cap_fallback") {
		t.Fatalf("expected scratch snapshot to win over fallback, got:\n%s", projection)
	}
}

func TestActiveCapabilitySnapshotPromptProjectionFallsBackWhenScratchSnapshotUnavailable(t *testing.T) {
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
	}, &sequenceLLM{})
	t.Cleanup(func() { _ = runtime.Close() })

	validSnapshot := buildDaemonRunCapabilitySnapshot(runtime.cfg, runtime.agent, "cap_boot", WorkspaceTaskRecord{TaskID: "task-1"}, AgentSessionStateRecord{SessionID: "session-1", TaskID: "task-1", Status: "ACTIVE"}, "run-1", nil, time.Date(2026, 4, 21, 8, 0, 0, 0, time.UTC))
	validPath, err := runtime.persistCapabilitySnapshot(validSnapshot)
	if err != nil {
		t.Fatalf("persist valid active snapshot: %v", err)
	}
	invalidPath := filepath.Join(t.TempDir(), "invalid.json")
	if err := os.WriteFile(invalidPath, []byte("{not-json"), 0o600); err != nil {
		t.Fatalf("write invalid snapshot: %v", err)
	}

	tests := []struct {
		name        string
		scratchID   string
		scratchPath string
	}{
		{
			name:        "missing file",
			scratchID:   "cap_missing",
			scratchPath: filepath.Join(t.TempDir(), "missing.json"),
		},
		{
			name:        "invalid json",
			scratchID:   "cap_invalid",
			scratchPath: invalidPath,
		},
		{
			name:        "id mismatch",
			scratchID:   "cap_other",
			scratchPath: validPath,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runtime.mu.Lock()
			runtime.scratch.ActiveCapabilitySnapshotID = tc.scratchID
			runtime.scratch.ActiveCapabilitySnapshotPath = tc.scratchPath
			runtime.activeCapabilitySnapshotID = tc.scratchID
			runtime.activeCapabilitySnapshotPath = tc.scratchPath
			runtime.mu.Unlock()

			projection := runtime.activeCapabilitySnapshotPromptProjection(DaemonCapabilitySnapshot{SnapshotID: "cap_fallback"})
			if !strings.Contains(projection, "snapshot_id: cap_fallback") {
				t.Fatalf("expected fallback snapshot projection, got:\n%s", projection)
			}
			if strings.Contains(projection, "snapshot_id: "+validSnapshot.SnapshotID) {
				t.Fatalf("unavailable scratch snapshot should not leak into projection, got:\n%s", projection)
			}
		})
	}
}

func TestDaemonCapabilityPromptContractMatchesSurfaceStates(t *testing.T) {
	cfg := RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
	}
	cfg.ApplyDefaults()
	agent := &Agent{
		Workdir:              cfg.Workdir,
		WorkspaceID:          cfg.WorkspaceID,
		AgentID:              cfg.AgentID,
		DisableLocalShell:    !runtimeAllowsLocalShell(cfg),
		DisableLocalMutation: !runtimeAllowsLocalMutation(cfg),
		DisableLocalMemory:   true,
	}
	agent.Init()

	snapshot := buildDaemonRunCapabilitySnapshot(cfg, agent, "cap_boot", WorkspaceTaskRecord{TaskID: "task-1"}, AgentSessionStateRecord{SessionID: "session-1"}, "run-1", nil, time.Date(2026, 4, 21, 8, 0, 0, 0, time.UTC))

	enabled := map[string]struct{}{}
	for _, name := range snapshot.PromptContract.EnabledToolNames {
		enabled[strings.TrimSpace(name)] = struct{}{}
	}
	for _, disabled := range snapshot.PromptContract.DisabledToolNames {
		name := strings.TrimSpace(disabled.Name)
		if strings.HasSuffix(name, ".*") {
			prefix := strings.TrimSuffix(name, ".*")
			for enabledName := range enabled {
				if strings.HasPrefix(enabledName, prefix+".") || strings.HasPrefix(enabledName, prefix+"_") {
					t.Fatalf("prompt contract enables %q while wildcard disabled tool %q is present", enabledName, name)
				}
			}
			continue
		}
		if _, ok := enabled[name]; ok {
			t.Fatalf("prompt contract both enables and disables tool %q", name)
		}
	}
	for _, surfaceID := range snapshot.PromptContract.InspectionOnlySurfaces {
		surface, ok := snapshot.Surfaces[surfaceID]
		if !ok {
			t.Fatalf("inspection-only surface %q is not present in snapshot surfaces", surfaceID)
		}
		if surface.Status != "inspection_only" {
			t.Fatalf("inspection-only surface %q has status %q", surfaceID, surface.Status)
		}
		if surface.ToolVisible || surface.MutationAllowed {
			t.Fatalf("inspection-only surface %q must not be tool-visible or mutation-allowed: %+v", surfaceID, surface)
		}
	}
}

func TestCaptureStartupCapabilitySnapshotPersistsBootFileAndScratch(t *testing.T) {
	runtime := NewRuntime(RuntimeConfig{
		Mode:         RuntimeModeDaemon,
		Workdir:      t.TempDir(),
		WorkspaceID:  "ws-1",
		AgentID:      "agent-1",
		DisplayName:  "Agent One",
		OwnerUserID:  "owner-1",
		RhizomeToken: "token",
	}, &sequenceLLM{})
	t.Cleanup(func() { _ = runtime.Close() })

	snapshot, path, err := runtime.captureStartupCapabilitySnapshot()
	if err != nil {
		t.Fatalf("captureStartupCapabilitySnapshot() error = %v", err)
	}
	if snapshot.SnapshotKind != "boot" || snapshot.SnapshotID == "" || path == "" {
		t.Fatalf("expected persisted boot snapshot, got snapshot=%+v path=%q", snapshot, path)
	}
	if runtime.scratch.CapabilityBootSnapshotID != snapshot.SnapshotID {
		t.Fatalf("scratch boot snapshot id = %q, want %q", runtime.scratch.CapabilityBootSnapshotID, snapshot.SnapshotID)
	}
	if runtime.scratch.ActiveCapabilitySnapshotID != snapshot.SnapshotID {
		t.Fatalf("scratch active snapshot id = %q, want boot id %q", runtime.scratch.ActiveCapabilitySnapshotID, snapshot.SnapshotID)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read persisted boot snapshot: %v", err)
	}
	var persisted DaemonCapabilitySnapshot
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatalf("decode persisted boot snapshot: %v", err)
	}
	if persisted.SnapshotID != snapshot.SnapshotID || persisted.SnapshotKind != "boot" {
		t.Fatalf("persisted boot snapshot mismatch: %+v", persisted)
	}
}

func TestCaptureRunCapabilitySnapshotRejectsMissingBinding(t *testing.T) {
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
	}, &sequenceLLM{})
	t.Cleanup(func() { _ = runtime.Close() })

	_, _, err := runtime.captureRunCapabilitySnapshot(WorkspaceTaskRecord{TaskID: "task-1"}, AgentSessionStateRecord{}, "", nil)
	if err == nil {
		t.Fatal("expected missing run/session binding to fail closed")
	}
	if !strings.Contains(err.Error(), "authority.missing_claim_term") {
		t.Fatalf("expected missing claim term reason, got %v", err)
	}
}
