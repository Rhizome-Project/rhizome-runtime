package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/app"
	"github.com/Rhizome-Project/rhizome-runtime/internal/dag"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func TestDurableRuntimeReadback_ReportsRestartedStateFromSQLite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "rhizome-durable-runtime.db")
	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(workspaceRoot, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}
	repoRoot := findRepoRootWithFixture(t)
	fakeBridge := filepath.Join(repoRoot, "tests", "fixtures", "fake_executor_bridge.py")
	if _, err := os.Stat(fakeBridge); err != nil {
		t.Fatalf("fake bridge not found: %v", err)
	}
	t.Setenv("RHIZOME_DB", dbPath)
	t.Setenv("RHIZOME_WORKSPACE_ROOT", workspaceRoot)
	t.Setenv("RHIZOME_EXECUTOR_PYTHON", "python")
	t.Setenv("RHIZOME_EXECUTOR_BRIDGE_SCRIPT", fakeBridge)
	const workspaceID = "ws-durable-runtime-readback"

	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Durable Runtime Readback",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, workspaceID)

	const agentID = "agent-durable-runtime"
	if _, err := store.RegisterAgentWithWorkspacePassword(ctx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       workspaceID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           agentID,
		DisplayName:       "Durable Runtime Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	sessionID := "sess-durable-runtime"
	state, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "start durable runtime readback",
		Status:      model.SessionStatusActive,
		OwnerScope:  "task/session",
	})
	if err != nil {
		t.Fatalf("record session coordination: %v", err)
	}

	syncResult, err := store.SyncExecutionRunFromSessionStateWithResult(ctx, state)
	if err != nil {
		t.Fatalf("sync execution run from session state: %v", err)
	}

	if _, err := store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
		RunID:       syncResult.Run.RunID,
		WorkspaceID: workspaceID,
		Phase:       "EXECUTE",
		Title:       "Continue durable work",
		Summary:     "advancing checkpoint for restart readback",
		Status:      "ACTIVE",
		SortOrder:   2,
		Verification: map[string]any{
			"checkpoint_id": "checkpoint-42",
		},
	}); err != nil {
		t.Fatalf("record durable step: %v", err)
	}

	operationUpdatedAt := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		RunID:       syncResult.Run.RunID,
		WorkspaceID: workspaceID,
		SessionID:   state.SessionID,
		AgentID:     state.AgentID,
		Title:       syncResult.Run.Title,
		Summary:     syncResult.Run.Summary,
		Status:      "ACTIVE",
		Verification: map[string]any{
			"operation_ledger": map[string]any{
				"schema":         "operation_ledger.v1",
				"operation_id":   "op-durable-runtime-1",
				"operation_name": "durable-restart-probe",
				"operation_kind": "tool_call",
				"status":         "running",
				"terminal":       false,
				"updated_at":     operationUpdatedAt,
				"binding": map[string]any{
					"run_id":     syncResult.Run.RunID,
					"session_id": state.SessionID,
					"task_id":    state.TaskID,
					"agent_id":   state.AgentID,
				},
			},
		},
	}); err != nil {
		t.Fatalf("upsert durable execution run: %v", err)
	}

	metricsPath := filepath.Join(t.TempDir(), "runtime_metrics.jsonl")
	if err := createDurableRuntimeMetricsFixture(metricsPath); err != nil {
		t.Fatalf("write metrics fixture: %v", err)
	}

	payload := collectServiceHealthPayload(app.Config{
		DBPath:      dbPath,
		MetricsPath: metricsPath,
	}, nil, sqlite.MemoryProjectionLagSnapshot{State: "ok"})
	if payload.DurableRuntime == nil {
		t.Fatal("expected durable runtime snapshot in diagnostics payload")
	}
	if payload.DurableRuntime.State != "ok" {
		t.Fatalf("expected durable runtime snapshot to be ok, got %+v", payload.DurableRuntime)
	}
	if payload.DurableRuntime.RunID != syncResult.Run.RunID {
		t.Fatalf("expected run id %q, got %+v", syncResult.Run.RunID, payload.DurableRuntime)
	}
	if payload.DurableRuntime.SessionID != state.SessionID {
		t.Fatalf("expected session id %q, got %+v", state.SessionID, payload.DurableRuntime)
	}
	if payload.DurableRuntime.StepPhase != "EXECUTE" {
		t.Fatalf("expected durable phase EXECUTE, got %+v", payload.DurableRuntime)
	}
	if !strings.Contains(payload.DurableRuntime.Progress, "checkpoint-42") {
		t.Fatalf("expected durable progress to mention checkpoint-42, got %+v", payload.DurableRuntime)
	}
	if check := checkDurableRuntimeFromDB(dbPath); check.Status != doctorStatusPass {
		t.Fatalf("expected local doctor readback to pass, got %+v", check)
	}
	if check := checkDurableRuntimeFromDetails(map[string]any{"service": payload}); check.Status != doctorStatusPass {
		t.Fatalf("expected health payload readback to pass, got %+v", check)
	}
}

func TestDurableRuntimeReadback_IgnoresTerminalOperationOnlyRunWithoutSession(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "rhizome-durable-runtime-operation-only.db")
	const workspaceID = "ws-durable-runtime-operation-only"
	t.Setenv("RHIZOME_DB", dbPath)

	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Durable Runtime Operation Only",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, workspaceID)

	runID := "tooldeploy-operation-only"
	if _, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Title:       "tool.deploy: fake_ledger_tool",
		Summary:     "DEPLOYED",
		Status:      "COMPLETED",
		Outcome:     "COMPLETED",
		Verification: map[string]any{
			"operation_ledger": map[string]any{
				"schema":         "operation_ledger.v1",
				"operation_id":   runID,
				"operation_name": "tool.deploy:fake_ledger_tool",
				"operation_kind": "tool_deploy",
				"status":         "completed",
				"terminal":       true,
				"binding": map[string]any{
					"run_id": runID,
				},
			},
		},
	}); err != nil {
		t.Fatalf("upsert operation-only execution run: %v", err)
	}

	snapshot := collectDurableRuntimeSnapshot(dbPath)
	if snapshot == nil {
		t.Fatal("expected durable runtime snapshot")
	}
	if snapshot.State != "unsupported" {
		t.Fatalf("expected operation-only run to be ignored by agent durable runtime readback, got %+v", snapshot)
	}
	if check := checkDurableRuntimeFromDB(dbPath); check.Status != doctorStatusPass {
		t.Fatalf("expected doctor readback to pass for operation-only ledger, got %+v", check)
	}
}

func TestDurableRuntimeReadback_IgnoresAgentTaggedOperationOnlyRun(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "rhizome-durable-runtime-agent-operation-only.db")
	const workspaceID = "ws-durable-runtime-agent-operation-only"
	t.Setenv("RHIZOME_DB", dbPath)

	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Durable Runtime Agent Operation Only",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, workspaceID)

	if _, err := store.RegisterAgentWithWorkspacePassword(ctx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       workspaceID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           "telegram-bridge",
		DisplayName:       "Telegram Bridge",
	}); err != nil {
		t.Fatalf("register telegram bridge agent: %v", err)
	}

	// Mirrors the production telegram bridge: an operation-only run carrying a
	// synthetic operator agent_id but no session/task binding and no steps. It
	// must not be treated as agent durable-runtime evidence.
	runID := "telegramop_deadbeefcafef00d_offset-0"
	if _, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		RunID:       runID,
		WorkspaceID: workspaceID,
		AgentID:     "telegram-bridge",
		Title:       "telegram.poll",
		Summary:     "COMPLETED",
		Status:      "COMPLETED",
		Outcome:     "COMPLETED",
		Verification: map[string]any{
			"operation_ledger": map[string]any{
				"schema":         "operation_ledger.v1",
				"operation_id":   runID,
				"operation_name": "telegram.poll",
				"operation_kind": "telegram_poll",
				"status":         "completed",
				"terminal":       true,
				"binding": map[string]any{
					"run_id":   runID,
					"agent_id": "telegram-bridge",
				},
			},
		},
	}); err != nil {
		t.Fatalf("upsert agent-tagged operation-only execution run: %v", err)
	}

	snapshot := collectDurableRuntimeSnapshot(dbPath)
	if snapshot == nil {
		t.Fatal("expected durable runtime snapshot")
	}
	if snapshot.State != "unsupported" {
		t.Fatalf("expected agent-tagged operation-only run to be ignored by agent durable runtime readback, got %+v", snapshot)
	}
	if check := checkDurableRuntimeFromDB(dbPath); check.Status != doctorStatusPass {
		t.Fatalf("expected doctor readback to pass for agent-tagged operation-only ledger, got %+v", check)
	}
}

func TestDurableRuntimeReadback_IgnoresCancelledAgentRun(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "rhizome-durable-runtime-cancelled-agent-run.db")
	const (
		workspaceID = "ws-durable-runtime-cancelled-agent-run"
		agentID     = "agent-cancelled-runtime"
		sessionID   = "sess-cancelled-runtime"
		taskID      = "task-cancelled-runtime"
		runID       = "run-cancelled-runtime"
	)
	t.Setenv("RHIZOME_DB", dbPath)

	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Durable Runtime Cancelled Agent Run",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, workspaceID)
	if _, err := store.RegisterAgentWithWorkspacePassword(ctx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       workspaceID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           agentID,
		DisplayName:       "Cancelled Runtime Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	graph := dag.NormalizeGraph(dag.Graph{Nodes: []dag.NodeSpec{{NodeID: "node-1", Type: "generic"}}})
	if err := dag.ValidateGraph(graph); err != nil {
		t.Fatalf("validate graph: %v", err)
	}
	if err := store.CreateTaskWithGraph(ctx, sqlite.TaskCreateInput{
		TaskID:      taskID,
		OwnerUserID: "developer",
		Priority:    "normal",
		Title:       "Cancelled runtime task",
		TaskKind:    "EXECUTION",
	}, graph); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := store.AttachTaskToWorkspace(ctx, sqlite.TaskAttachmentInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		LinkedBy:    "developer",
	}); err != nil {
		t.Fatalf("attach task: %v", err)
	}
	if err := store.ClaimTask(ctx, sqlite.TaskClaimInput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		AgentID:     agentID,
		Summary:     "claimed before managed stop",
	}); err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "started before managed stop",
		Status:      model.SessionStatusActive,
		OwnerScope:  "task/session",
	}); err != nil {
		t.Fatalf("record started session: %v", err)
	}
	if _, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		RunID:       runID,
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Title:       "Cancelled runtime run",
		Summary:     "cancelled by managed stop",
		Status:      "CANCELLED",
		Outcome:     "STOPPED_BY_MANAGER",
	}); err != nil {
		t.Fatalf("upsert cancelled execution run: %v", err)
	}
	if _, err := store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
		RunID:       runID,
		WorkspaceID: workspaceID,
		Phase:       "VERIFY",
		Title:       "Old cancelled checkpoint",
		Summary:     "old cancelled run must not drive readiness",
		Status:      "CANCELLED",
		SortOrder:   20,
		Evidence:    []string{"capability_snapshot:cap_old_cancelled"},
		Verification: map[string]any{
			"prompt_capability_evidence": validDurableRuntimePromptEvidence("cap_old_cancelled"),
		},
	}); err != nil {
		t.Fatalf("record cancelled execution step: %v", err)
	}
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventEnd,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		TaskID:      taskID,
		Summary:     "ended by managed stop",
		Status:      model.SessionStatusEnded,
		OwnerScope:  "task/session",
	}); err != nil {
		t.Fatalf("record ended session: %v", err)
	}

	snapshot := collectDurableRuntimeSnapshot(dbPath)
	if snapshot == nil {
		t.Fatal("expected durable runtime snapshot")
	}
	if snapshot.State != "unsupported" {
		t.Fatalf("expected cancelled agent run to be ignored by durable runtime readback, got %+v", snapshot)
	}
	if check := checkDurableRuntimeFromDB(dbPath); check.Status != doctorStatusPass {
		t.Fatalf("expected doctor readback to pass for cancelled agent run, got %+v", check)
	}
	if check := checkDaemonPromptCompilerConvergenceFromDB(dbPath); check.Status != doctorStatusPass {
		t.Fatalf("expected daemon prompt convergence to be not applicable for cancelled run, got %+v", check)
	}
}

func TestDurableRuntimePromptCompilerSnapshot_SeparatesDaemonProofFromRuntimeReadback(t *testing.T) {
	t.Parallel()

	noProof := durableRuntimePromptCompilerSnapshot(&sqlite.ExecutionRunDetail{
		Run: sqlite.ExecutionRunRecord{
			RunID:       "run-no-proof",
			WorkspaceID: "ws-prompt-readiness",
		},
		Steps: []sqlite.ExecutionStepRecord{
			{
				StepID:           "step-checkpoint",
				Phase:            "EXECUTE",
				Evidence:         []string{"operation:op-1"},
				VerificationJSON: map[string]any{"checkpoint_id": "checkpoint-1"},
			},
		},
	})
	if noProof.State != "not_evaluated" {
		t.Fatalf("expected ordinary durable runtime step to remain not_evaluated, got %+v", noProof)
	}

	missingProof := durableRuntimePromptCompilerSnapshot(&sqlite.ExecutionRunDetail{
		Run: sqlite.ExecutionRunRecord{
			RunID:       "run-missing-proof",
			WorkspaceID: "ws-prompt-readiness",
		},
		Steps: []sqlite.ExecutionStepRecord{
			{
				StepID:           "step-capability-only",
				Phase:            "EXECUTE",
				Evidence:         []string{"capability_snapshot:cap_missing"},
				VerificationJSON: map[string]any{"capability_snapshot_ref": "capability_snapshot:cap_missing"},
			},
		},
	})
	if missingProof.State != "missing" {
		t.Fatalf("expected capability snapshot without prompt proof to be missing, got %+v", missingProof)
	}

	missingIDOnlyProof := durableRuntimePromptCompilerSnapshot(&sqlite.ExecutionRunDetail{
		Run: sqlite.ExecutionRunRecord{
			RunID:       "run-missing-id-only-proof",
			WorkspaceID: "ws-prompt-readiness",
		},
		Steps: []sqlite.ExecutionStepRecord{
			{
				StepID: "step-capability-id-only",
				Phase:  "EXECUTE",
				VerificationJSON: map[string]any{
					"capability_snapshot_id": "cap_id_only",
				},
			},
		},
	})
	if missingIDOnlyProof.State != "missing" {
		t.Fatalf("expected id-only capability snapshot without prompt proof to be missing, got %+v", missingIDOnlyProof)
	}

	badSnapshotIDProof := validDurableRuntimePromptEvidence("cap bad")
	badSnapshotID := durableRuntimePromptCompilerSnapshot(&sqlite.ExecutionRunDetail{
		Run: sqlite.ExecutionRunRecord{
			RunID:       "run-bad-snapshot-id",
			WorkspaceID: "ws-prompt-readiness",
		},
		Steps: []sqlite.ExecutionStepRecord{
			{
				StepID:   "step-bad-snapshot-id",
				Phase:    "PLAN",
				Evidence: []string{"capability_snapshot:cap bad"},
				VerificationJSON: map[string]any{
					"prompt_capability_evidence": badSnapshotIDProof,
				},
			},
		},
	})
	if badSnapshotID.State != "mismatch" || !strings.Contains(badSnapshotID.Message, "non-canonical capability snapshot ref") {
		t.Fatalf("expected non-canonical snapshot id to mismatch, got %+v", badSnapshotID)
	}

	staleProof := durableRuntimePromptCompilerSnapshot(&sqlite.ExecutionRunDetail{
		Run: sqlite.ExecutionRunRecord{
			RunID:       "run-stale-proof",
			WorkspaceID: "ws-prompt-readiness",
		},
		Steps: []sqlite.ExecutionStepRecord{
			{
				StepID:    "step-plan-old-proof",
				Phase:     "PLAN",
				Evidence:  []string{"capability_snapshot:cap_old"},
				CreatedAt: "2026-04-22T01:00:00Z",
				UpdatedAt: "2026-04-22T01:00:00Z",
				VerificationJSON: map[string]any{
					"prompt_capability_evidence": validDurableRuntimePromptEvidence("cap_old"),
				},
			},
			{
				StepID:    "step-new-snapshot",
				Phase:     "EXECUTE",
				Evidence:  []string{"capability_snapshot:cap_new"},
				CreatedAt: "2026-04-22T01:01:00Z",
				UpdatedAt: "2026-04-22T01:01:00Z",
				VerificationJSON: map[string]any{
					"capability_snapshot_ref": "capability_snapshot:cap_new",
				},
			},
		},
	})
	if staleProof.State != "mismatch" || !strings.Contains(staleProof.Message, "latest capability snapshot ref") {
		t.Fatalf("expected stale prompt proof to mismatch newer capability snapshot, got %+v", staleProof)
	}

	staleIDOnlyProof := durableRuntimePromptCompilerSnapshot(&sqlite.ExecutionRunDetail{
		Run: sqlite.ExecutionRunRecord{
			RunID:       "run-stale-id-only-proof",
			WorkspaceID: "ws-prompt-readiness",
		},
		Steps: []sqlite.ExecutionStepRecord{
			{
				StepID:    "step-plan-old-id-proof",
				Phase:     "PLAN",
				Evidence:  []string{"capability_snapshot:cap_old"},
				CreatedAt: "2026-04-22T01:00:00Z",
				UpdatedAt: "2026-04-22T01:00:00Z",
				VerificationJSON: map[string]any{
					"prompt_capability_evidence": validDurableRuntimePromptEvidence("cap_old"),
				},
			},
			{
				StepID:    "step-new-snapshot-id-only",
				Phase:     "EXECUTE",
				CreatedAt: "2026-04-22T01:01:00Z",
				UpdatedAt: "2026-04-22T01:01:00Z",
				VerificationJSON: map[string]any{
					"capability_snapshot_id": "cap_new",
				},
			},
		},
	})
	if staleIDOnlyProof.State != "mismatch" || !strings.Contains(staleIDOnlyProof.Message, "latest capability snapshot ref") {
		t.Fatalf("expected stale prompt proof to mismatch newer id-only capability snapshot, got %+v", staleIDOnlyProof)
	}

	validProof := durableRuntimePromptCompilerSnapshot(&sqlite.ExecutionRunDetail{
		Run: sqlite.ExecutionRunRecord{
			RunID:       "run-valid-proof",
			WorkspaceID: "ws-prompt-readiness",
		},
		Steps: []sqlite.ExecutionStepRecord{
			{
				StepID:   "step-plan-proof",
				Phase:    "PLAN",
				Evidence: []string{"capability_snapshot:cap_valid"},
				VerificationJSON: map[string]any{
					"prompt_capability_evidence": validDurableRuntimePromptEvidence("cap_valid"),
				},
			},
			{
				StepID:           "step-terminal",
				Phase:            "VERIFY",
				Evidence:         []string{"capability_snapshot:cap_valid"},
				VerificationJSON: map[string]any{"capability_snapshot_ref": "capability_snapshot:cap_valid"},
			},
		},
	})
	if validProof.State != "ok" {
		t.Fatalf("expected valid daemon prompt compiler proof to be ok, got %+v", validProof)
	}
	if validProof.CapabilitySnapshotRef != "capability_snapshot:cap_valid" {
		t.Fatalf("expected capability snapshot ref to be surfaced, got %+v", validProof)
	}
}

func TestDurableRuntimePromptCompilerSnapshot_ReadsSnapshotAndRecomputesProjectionDigest(t *testing.T) {
	t.Parallel()

	snapshot := durableRuntimeCapabilitySnapshot{
		Schema:       "daemon_capability_snapshot.v1",
		SnapshotID:   "cap_readback",
		SnapshotKind: "run",
		Status:       durableRuntimeCapabilitySnapshotStatus{Overall: "enabled"},
		PromptContract: durableRuntimeCapabilityPromptContract{
			ContractID:             "prompt_capabilities.v1",
			EnabledToolNames:       []string{"read_file", "shell"},
			DisabledToolNames:      []durableRuntimeCapabilityDisabledTool{{Name: "write_file", ReasonCode: "repo.no_lease"}},
			InspectionOnlySurfaces: []string{"workspace_tools", "ui"},
			BudgetSummary: durableRuntimeCapabilityBudgetSummary{
				MaxToolIterations:   8,
				MaxShellTimeoutSec:  60,
				MaxPromptDocChars:   1000,
				MaxPromptSpecChars:  2000,
				MaxSmokeCyclesAgent: 2,
				MaxSmokeCyclesTask:  3,
			},
			MustInclude: []string{"Only use enabled tools listed in this capability snapshot."},
		},
		Surfaces: map[string]durableRuntimeCapabilitySurface{
			"executor": {
				SurfaceID: "executor",
				Status:    "disabled",
				DisabledReasons: []durableRuntimeCapabilityDisabledReason{
					{Code: "executor.operation_ledger_required"},
				},
			},
		},
	}
	snapshotPath := writeDurableRuntimeCapabilitySnapshotFixture(t, snapshot)
	digest := durableRuntimeCapabilityProjectionDigest(renderDurableRuntimeCapabilityPromptProjection(snapshot))
	proof := validDurableRuntimePromptEvidence(snapshot.SnapshotID)
	proof["projection_digest"] = digest
	proof["capability_snapshot_path"] = snapshotPath

	detail := sqlite.ExecutionRunDetail{
		Run: sqlite.ExecutionRunRecord{
			RunID:       "run-readback-proof",
			WorkspaceID: "ws-prompt-readback",
		},
		Steps: []sqlite.ExecutionStepRecord{
			{
				StepID:   "step-readback-proof",
				Phase:    "PLAN",
				Evidence: []string{"capability_snapshot:" + snapshot.SnapshotID},
				VerificationJSON: map[string]any{
					"prompt_capability_evidence": proof,
				},
			},
		},
	}

	readback := durableRuntimePromptCompilerSnapshotWithSnapshotReadback(&detail)
	if readback.State != "ok" || readback.SnapshotReadbackState != "ok" {
		t.Fatalf("expected prompt compiler snapshot readback to pass, got %+v", readback)
	}
	if readback.SnapshotReadbackDigest != digest {
		t.Fatalf("expected readback digest %q, got %+v", digest, readback)
	}

	missingPathProof := validDurableRuntimePromptEvidence(snapshot.SnapshotID)
	missingPathProof["projection_digest"] = digest
	detail.Steps[0].VerificationJSON["prompt_capability_evidence"] = missingPathProof
	missingPath := durableRuntimePromptCompilerSnapshotWithSnapshotReadback(&detail)
	if missingPath.State != "mismatch" || !strings.Contains(missingPath.Message, "capability_snapshot_path") {
		t.Fatalf("expected missing snapshot path to mismatch, got %+v", missingPath)
	}

	snapshot.PromptContract.EnabledToolNames = append(snapshot.PromptContract.EnabledToolNames, "write_file")
	writeDurableRuntimeCapabilitySnapshotFixtureAt(t, snapshotPath, snapshot)
	detail.Steps[0].VerificationJSON["prompt_capability_evidence"] = proof
	tampered := durableRuntimePromptCompilerSnapshotWithSnapshotReadback(&detail)
	if tampered.State != "mismatch" || !strings.Contains(tampered.Message, "projection_digest") {
		t.Fatalf("expected tampered snapshot projection digest to mismatch, got %+v", tampered)
	}
	if tampered.SnapshotReadbackDigest == digest {
		t.Fatalf("expected tampered readback digest to differ from proof digest, got %+v", tampered)
	}
}

func TestDurableRuntimePromptCompilerSnapshot_ReadsAgentRuntimeBudgetJSONFields(t *testing.T) {
	t.Parallel()

	snapshot := durableRuntimeCapabilitySnapshot{
		Schema:       "daemon_capability_snapshot.v1",
		SnapshotID:   "cap_agent_budget",
		SnapshotKind: "run",
		Status:       durableRuntimeCapabilitySnapshotStatus{Overall: "enabled"},
		PromptContract: durableRuntimeCapabilityPromptContract{
			ContractID:             "prompt_capabilities.v1",
			EnabledToolNames:       []string{"read_file"},
			DisabledToolNames:      []durableRuntimeCapabilityDisabledTool{{Name: "shell", ReasonCode: "local.shell.disabled_by_policy"}},
			InspectionOnlySurfaces: []string{"workspace_tools"},
			BudgetSummary: durableRuntimeCapabilityBudgetSummary{
				MaxToolIterations:   8,
				MaxShellTimeoutSec:  60,
				MaxPromptDocChars:   1000,
				MaxPromptSpecChars:  2000,
				MaxSmokeCyclesAgent: 2,
				MaxSmokeCyclesTask:  3,
			},
		},
	}
	digest := durableRuntimeCapabilityProjectionDigest(renderDurableRuntimeCapabilityPromptProjection(snapshot))
	rawSnapshot := `{
		"schema":"daemon_capability_snapshot.v1",
		"snapshot_id":"cap_agent_budget",
		"snapshot_kind":"run",
		"status":{"overall":"enabled"},
		"prompt_contract":{
			"contract_id":"prompt_capabilities.v1",
			"enabled_tool_names":["read_file"],
			"disabled_tool_names":[{"name":"shell","reason_code":"local.shell.disabled_by_policy"}],
			"inspection_only_surfaces":["workspace_tools"],
			"budget_summary":{
				"max_tool_iterations":8,
				"max_shell_timeout_sec":60,
				"max_prompt_doc_chars":1000,
				"max_prompt_spec_chars":2000,
				"max_smoke_cycles_per_agent":2,
				"max_smoke_cycles_per_task":3
			}
		}
	}`
	snapshotPath := filepath.Join(t.TempDir(), "cap_agent_budget.json")
	if err := os.WriteFile(snapshotPath, []byte(rawSnapshot), 0o600); err != nil {
		t.Fatalf("write agent budget snapshot fixture: %v", err)
	}
	proof := validDurableRuntimePromptEvidence(snapshot.SnapshotID)
	proof["projection_digest"] = digest
	proof["capability_snapshot_path"] = snapshotPath

	accepted := durablePromptCompilerSnapshot{}
	if err := validateDurableRuntimeCapabilitySnapshotReadback(proof, &accepted); err != nil {
		t.Fatalf("expected agent budget JSON fields to read back without digest drift: %v", err)
	}
	if accepted.SnapshotReadbackDigest != digest {
		t.Fatalf("expected readback digest %q, got %q", digest, accepted.SnapshotReadbackDigest)
	}
}

func TestDurableRuntimeCapabilityPromptProjectionMatchesSharedGolden(t *testing.T) {
	t.Parallel()

	golden, err := os.ReadFile(filepath.Join("..", "..", "agent", "testdata", "active_capability_projection.golden"))
	if err != nil {
		t.Fatalf("read shared projection golden: %v", err)
	}
	got := renderDurableRuntimeCapabilityPromptProjection(testDurableRuntimeCapabilityProjectionGoldenSnapshot())
	want := normalizeDurableRuntimeProjectionGolden(golden)
	if got != want {
		t.Fatalf("durable runtime capability projection drifted from shared golden\nwant:\n%s\n\ngot:\n%s", want, got)
	}
	if digest := durableRuntimeCapabilityProjectionDigest(got); digest != "sha256:51ddbfdb572fb605ff48d0dc66e5533cc506e57c1414cb98dce96b66a469cf66" {
		t.Fatalf("projection digest drifted: %s", digest)
	}
}

func TestDurableRuntimeCapabilityPromptProjectionWorkspaceToolsMatchesSharedGolden(t *testing.T) {
	t.Parallel()

	golden, err := os.ReadFile(filepath.Join("..", "..", "agent", "testdata", "active_capability_projection_workspace_tools.golden"))
	if err != nil {
		t.Fatalf("read shared workspace-tools projection golden: %v", err)
	}
	got := renderDurableRuntimeCapabilityPromptProjection(testDurableRuntimeCapabilityProjectionWorkspaceToolsGoldenSnapshot())
	want := normalizeDurableRuntimeProjectionGolden(golden)
	if got != want {
		t.Fatalf("durable runtime workspace-tools projection drifted from shared golden\nwant:\n%s\n\ngot:\n%s", want, got)
	}
	if digest := durableRuntimeCapabilityProjectionDigest(got); digest != "sha256:0a029e7dd291006f09afae53ecad33b97928f92ec4f8d0c5ff34b5d4e0c8994b" {
		t.Fatalf("workspace-tools projection digest drifted: %s", digest)
	}
}

func normalizeDurableRuntimeProjectionGolden(data []byte) string {
	return strings.TrimSpace(strings.ReplaceAll(string(data), "\r\n", "\n"))
}

func TestDoctorDaemonPromptCompilerConvergenceWarnsWhenNotCollected(t *testing.T) {
	t.Parallel()

	check := checkDaemonPromptCompilerConvergenceSnapshot(nil)
	if check.Status != doctorStatusWarn {
		t.Fatalf("expected missing prompt compiler convergence snapshot to warn, got %+v", check)
	}
}

func TestDoctorDaemonPromptCompilerConvergencePassesWhenDaemonDisabledAndNotEvaluated(t *testing.T) {
	t.Parallel()

	check := checkDaemonPromptCompilerConvergenceFromDetails(map[string]any{
		"service": serviceHealthPayload{
			Status: "ok",
			LoopReadiness: []LoopReadiness{
				{Name: loopNameDaemon, State: LoopDisabled},
			},
			DurableRuntime: &durableRuntimeSnapshot{
				State: "ok",
				PromptCompiler: &durablePromptCompilerSnapshot{
					State:   "not_evaluated",
					Message: "daemon prompt compiler convergence proof not present in latest durable run",
				},
			},
		},
	})
	if check.Status != doctorStatusPass {
		t.Fatalf("expected disabled embedded daemon prompt convergence to pass, got %+v", check)
	}
	if !strings.Contains(check.Message, "embedded daemon loop is disabled") {
		t.Fatalf("expected disabled daemon explanation, got %+v", check)
	}
}

func TestDoctorDaemonPromptCompilerConvergencePassesWhenDaemonDisabledAndMismatched(t *testing.T) {
	t.Parallel()

	check := checkDaemonPromptCompilerConvergenceFromDetails(map[string]any{
		"service": serviceHealthPayload{
			Status: "ok",
			LoopReadiness: []LoopReadiness{
				{Name: loopNameDaemon, State: LoopDisabled},
			},
			DurableRuntime: &durableRuntimeSnapshot{
				State: "ok",
				PromptCompiler: &durablePromptCompilerSnapshot{
					State:                  "mismatch",
					Message:                "capability snapshot readback projection_digest mismatch",
					CapabilitySnapshotRef:  "capability_snapshot:cap_stale",
					ProjectionDigest:       "sha256:c2ed82752c89c4a5096ee2e9003210695e6a4f7350c6a37e0a87c899c6f8290d",
					SnapshotReadbackState:  "embedded",
					SnapshotReadbackDigest: "sha256:60e499add8b4a42606a7220d28674e2dfc1acf68a6f888ada5d69d003e3cec52",
				},
			},
		},
	})
	if check.Status != doctorStatusPass {
		t.Fatalf("expected disabled embedded daemon prompt mismatch to pass as advisory, got %+v", check)
	}
	if !strings.Contains(check.Message, "embedded daemon loop is disabled") {
		t.Fatalf("expected disabled daemon explanation, got %+v", check)
	}
	if got, _ := check.Details["snapshot_readback_state"].(string); got != "embedded" {
		t.Fatalf("expected advisory details to keep snapshot readback evidence, got %+v", check.Details)
	}
}

func testDurableRuntimeCapabilityProjectionGoldenSnapshot() durableRuntimeCapabilitySnapshot {
	return durableRuntimeCapabilitySnapshot{
		Schema:       "daemon_capability_snapshot.v1",
		SnapshotID:   "cap_projection_golden",
		SnapshotKind: "run",
		Status:       durableRuntimeCapabilitySnapshotStatus{Overall: "enabled"},
		PromptContract: durableRuntimeCapabilityPromptContract{
			ContractID:             "prompt_capabilities.v1",
			EnabledToolNames:       []string{"write_file", "read_file", "shell", "list_directory"},
			DisabledToolNames:      testDurableRuntimeCapabilityProjectionDisabledTools(),
			InspectionOnlySurfaces: []string{"web", "tui"},
			BudgetSummary: durableRuntimeCapabilityBudgetSummary{
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
		Surfaces: map[string]durableRuntimeCapabilitySurface{
			"executor": testDurableRuntimeCapabilityProjectionSurface("executor", "disabled", "executor.operation_ledger_required"),
			"memory":   testDurableRuntimeCapabilityProjectionSurface("memory", "disabled", "program_a.no_direct_memory_promotion", "memory.local.disabled_in_daemon"),
			"ui":       testDurableRuntimeCapabilityProjectionSurface("ui", "inspection_only", "program_a.ui_no_authority_bearing_actions"),
		},
	}
}

func testDurableRuntimeCapabilityProjectionWorkspaceToolsGoldenSnapshot() durableRuntimeCapabilitySnapshot {
	snapshot := testDurableRuntimeCapabilityProjectionGoldenSnapshot()
	snapshot.SnapshotID = "cap_workspace_tools_projection_golden"
	snapshot.PromptContract.EnabledToolNames = []string{"fake_ledger_tool", "read_file", "list_directory"}
	snapshot.PromptContract.InspectionOnlySurfaces = []string{"workspace_tools", "bridges", "ui"}
	snapshot.Surfaces["workspace_tools"] = testDurableRuntimeCapabilityProjectionSurface("workspace_tools", "degraded", "deployment.opt_in_workspace_tools")
	return snapshot
}

func testDurableRuntimeCapabilityProjectionDisabledTools() []durableRuntimeCapabilityDisabledTool {
	return []durableRuntimeCapabilityDisabledTool{
		{Name: "write_file", ReasonCode: "program_b.raw_repo_mutation_disabled"},
		{Name: "shell", ReasonCode: "program_b.raw_shell_disabled"},
		{Name: "mcp__*", ReasonCode: "program_b.unknown_mcp_disabled"},
	}
}

func testDurableRuntimeCapabilityProjectionSurface(surfaceID, status string, reasonCodes ...string) durableRuntimeCapabilitySurface {
	reasons := make([]durableRuntimeCapabilityDisabledReason, 0, len(reasonCodes))
	for _, code := range reasonCodes {
		reasons = append(reasons, durableRuntimeCapabilityDisabledReason{Code: code})
	}
	return durableRuntimeCapabilitySurface{
		SurfaceID:       surfaceID,
		Status:          status,
		DisabledReasons: reasons,
	}
}

func TestDoctorDaemonPromptCompilerConvergenceIncludesSnapshotReadbackDetails(t *testing.T) {
	t.Parallel()

	check := checkDaemonPromptCompilerConvergenceSnapshot(&durablePromptCompilerSnapshot{
		State:                  "mismatch",
		Message:                "capability snapshot readback projection_digest mismatch",
		StepID:                 "step-readback",
		CapabilitySnapshotRef:  "capability_snapshot:cap_readback",
		CapabilitySnapshotPath: filepath.Join("tmp", "cap_readback.json"),
		ProjectionDigest:       "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		SnapshotReadbackState:  "ok",
		SnapshotReadbackDigest: "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
	})
	if check.Status != doctorStatusFail {
		t.Fatalf("expected prompt compiler mismatch to fail, got %+v", check)
	}
	details := check.Details
	for key, want := range map[string]string{
		"capability_snapshot_path": filepath.Join("tmp", "cap_readback.json"),
		"snapshot_readback_state":  "ok",
		"snapshot_readback_digest": "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
	} {
		if got, _ := details[key].(string); got != want {
			t.Fatalf("expected doctor detail %s=%q, got %+v", key, want, details)
		}
	}
}

func TestDurableRuntimeReadback_FailsWhenDiagnosticsOmitSnapshot(t *testing.T) {
	payload := serviceHealthPayload{
		Status: "ok",
		Config: configSnapshot{
			DBPath:        "rhizome.db",
			WorkspaceRoot: "workspace",
		},
		Runtime: app.RuntimeBuildInfo{
			BinaryPath:       "rhizome",
			WorkingDirectory: "workspace",
		},
	}
	check := checkDurableRuntimeFromDetails(map[string]any{"service": payload})
	if check.Status != doctorStatusWarn {
		t.Fatalf("expected missing durable runtime section to warn, got %+v", check)
	}
}

func TestDurableRuntimeReadback_SkipsNewerEmptyRunShell(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "rhizome-durable-runtime-empty-shell.db")
	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(workspaceRoot, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}
	repoRoot := findRepoRootWithFixture(t)
	fakeBridge := filepath.Join(repoRoot, "tests", "fixtures", "fake_executor_bridge.py")
	if _, err := os.Stat(fakeBridge); err != nil {
		t.Fatalf("fake bridge not found: %v", err)
	}
	t.Setenv("RHIZOME_DB", dbPath)
	t.Setenv("RHIZOME_WORKSPACE_ROOT", workspaceRoot)
	t.Setenv("RHIZOME_EXECUTOR_PYTHON", "python")
	t.Setenv("RHIZOME_EXECUTOR_BRIDGE_SCRIPT", fakeBridge)

	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	const workspaceID = "ws-durable-runtime-empty-shell"
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Durable Runtime Empty Shell",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, workspaceID)

	const agentID = "agent-durable-runtime-empty-shell"
	if _, err := store.RegisterAgentWithWorkspacePassword(ctx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       workspaceID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           agentID,
		DisplayName:       "Durable Runtime Empty Shell Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	state, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   "sess-durable-runtime-empty-shell",
		AgentID:     agentID,
		Summary:     "start durable runtime empty shell readback",
		Status:      model.SessionStatusActive,
		OwnerScope:  "task/session",
	})
	if err != nil {
		t.Fatalf("record session coordination: %v", err)
	}

	syncResult, err := store.SyncExecutionRunFromSessionStateWithResult(ctx, state)
	if err != nil {
		t.Fatalf("sync execution run from session state: %v", err)
	}
	if _, err := store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
		RunID:       syncResult.Run.RunID,
		WorkspaceID: workspaceID,
		Phase:       "EXECUTE",
		Title:       "Known durable checkpoint",
		Summary:     "checkpoint must survive a newer empty restart shell",
		Status:      "ACTIVE",
		SortOrder:   2,
		Verification: map[string]any{
			"checkpoint_id": "checkpoint-before-empty-shell",
		},
	}); err != nil {
		t.Fatalf("record durable step: %v", err)
	}

	for idx := 0; idx < 101; idx++ {
		if _, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
			RunID:       fmt.Sprintf("zz-newer-empty-restart-shell-%03d", idx),
			WorkspaceID: workspaceID,
			TaskID:      state.TaskID,
			SessionID:   state.SessionID,
			AgentID:     state.AgentID,
			Title:       "Newer empty restart shell",
			Summary:     "new process claimed the run but has not written a step yet",
			Status:      "ACTIVE",
		}); err != nil {
			t.Fatalf("upsert empty restart shell %d: %v", idx, err)
		}
	}

	metricsPath := filepath.Join(t.TempDir(), "runtime_metrics.jsonl")
	if err := createDurableRuntimeMetricsFixture(metricsPath); err != nil {
		t.Fatalf("write metrics fixture: %v", err)
	}
	payload := collectServiceHealthPayload(app.Config{
		DBPath:      dbPath,
		MetricsPath: metricsPath,
	}, nil, sqlite.MemoryProjectionLagSnapshot{State: "ok"})
	if payload.DurableRuntime == nil {
		t.Fatal("expected durable runtime snapshot in diagnostics payload")
	}
	if payload.DurableRuntime.State != "ok" {
		t.Fatalf("expected older durable checkpoint to remain readable, got %+v", payload.DurableRuntime)
	}
	if payload.DurableRuntime.RunID != syncResult.Run.RunID {
		t.Fatalf("expected readback to skip newer empty shell and use %q, got %+v", syncResult.Run.RunID, payload.DurableRuntime)
	}
	if !strings.Contains(payload.DurableRuntime.Progress, "checkpoint-before-empty-shell") {
		t.Fatalf("expected readback progress from older checkpoint, got %+v", payload.DurableRuntime)
	}
}

func TestDurableRuntimeReadback_FailsOnOperationBindingMismatch(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "rhizome-durable-runtime-mismatch.db")
	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(workspaceRoot, 0o755); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}
	repoRoot := findRepoRootWithFixture(t)
	fakeBridge := filepath.Join(repoRoot, "tests", "fixtures", "fake_executor_bridge.py")
	if _, err := os.Stat(fakeBridge); err != nil {
		t.Fatalf("fake bridge not found: %v", err)
	}
	t.Setenv("RHIZOME_DB", dbPath)
	t.Setenv("RHIZOME_WORKSPACE_ROOT", workspaceRoot)
	t.Setenv("RHIZOME_EXECUTOR_PYTHON", "python")
	t.Setenv("RHIZOME_EXECUTOR_BRIDGE_SCRIPT", fakeBridge)
	const workspaceID = "ws-durable-runtime-mismatch"

	store, err := sqlite.NewStore(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.ApplyMigrations(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := store.CreateWorkspace(ctx, sqlite.WorkspaceCreateInput{
		WorkspaceID: workspaceID,
		Title:       "Durable Runtime Mismatch",
		CreatedBy:   "developer",
	}); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	claimCLITestWorkspaceAuthority(t, workspaceID)

	const agentID = "agent-durable-runtime-mismatch"
	if _, err := store.RegisterAgentWithWorkspacePassword(ctx, sqlite.AgentSelfRegisterInput{
		WorkspaceID:       workspaceID,
		WorkspacePassword: sqlite.DefaultWorkspacePassword,
		AgentID:           agentID,
		DisplayName:       "Durable Runtime Mismatch Agent",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	sessionID := "sess-durable-runtime-mismatch"
	state, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   sessionID,
		AgentID:     agentID,
		Summary:     "start durable runtime mismatch readback",
		Status:      model.SessionStatusActive,
		OwnerScope:  "task/session",
	})
	if err != nil {
		t.Fatalf("record session coordination: %v", err)
	}

	syncResult, err := store.SyncExecutionRunFromSessionStateWithResult(ctx, state)
	if err != nil {
		t.Fatalf("sync execution run from session state: %v", err)
	}

	if _, err := store.RecordExecutionStep(ctx, sqlite.ExecutionStepInput{
		RunID:       syncResult.Run.RunID,
		WorkspaceID: workspaceID,
		Phase:       "EXECUTE",
		Title:       "Continue durable mismatch work",
		Summary:     "mismatch probe step",
		Status:      "ACTIVE",
		SortOrder:   2,
		Verification: map[string]any{
			"checkpoint_id": "checkpoint-mismatch",
		},
	}); err != nil {
		t.Fatalf("record mismatch step: %v", err)
	}

	operationUpdatedAt := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := store.UpsertExecutionRun(ctx, sqlite.ExecutionRunInput{
		RunID:       syncResult.Run.RunID,
		WorkspaceID: workspaceID,
		SessionID:   state.SessionID,
		AgentID:     state.AgentID,
		Title:       syncResult.Run.Title,
		Summary:     syncResult.Run.Summary,
		Status:      "ACTIVE",
		Verification: map[string]any{
			"operation_ledger": map[string]any{
				"schema":         "operation_ledger.v1",
				"operation_id":   "op-durable-runtime-mismatch-1",
				"operation_name": "durable-restart-mismatch",
				"operation_kind": "tool_call",
				"status":         "running",
				"terminal":       false,
				"updated_at":     operationUpdatedAt,
				"binding": map[string]any{
					"run_id":     syncResult.Run.RunID,
					"session_id": state.SessionID,
					"task_id":    state.TaskID,
					"agent_id":   state.AgentID,
				},
			},
		},
	}); err != nil {
		t.Fatalf("upsert mismatch execution run: %v", err)
	}

	mismatchSessionID := "sess-durable-runtime-mismatch-restart"
	if _, err := store.RecordAgentSessionCoordination(ctx, sqlite.AgentSessionCoordinationInput{
		EventType:   model.SessionEventStart,
		WorkspaceID: workspaceID,
		SessionID:   mismatchSessionID,
		AgentID:     agentID,
		Summary:     "restart target session for mismatch probe",
		Status:      model.SessionStatusActive,
		OwnerScope:  "task/session",
	}); err != nil {
		t.Fatalf("record mismatch target session: %v", err)
	}

	if _, err := store.DB().ExecContext(ctx, `UPDATE execution_runs SET session_id = ? WHERE run_id = ?`, mismatchSessionID, syncResult.Run.RunID); err != nil {
		t.Fatalf("force session mismatch in execution run: %v", err)
	}

	metricsPath := filepath.Join(t.TempDir(), "runtime_metrics.jsonl")
	if err := createDurableRuntimeMetricsFixture(metricsPath); err != nil {
		t.Fatalf("write metrics fixture: %v", err)
	}

	payload := collectServiceHealthPayload(app.Config{
		DBPath:      dbPath,
		MetricsPath: metricsPath,
	}, nil, sqlite.MemoryProjectionLagSnapshot{State: "ok"})
	if payload.DurableRuntime == nil {
		t.Fatal("expected durable runtime snapshot in diagnostics payload")
	}
	if payload.DurableRuntime.State != "mismatch" {
		t.Fatalf("expected durable runtime mismatch, got %+v", payload.DurableRuntime)
	}
	if payload.Status != "degraded" {
		t.Fatalf("expected diagnostics payload to degrade on mismatch, got %+v", payload)
	}
	if check := checkDurableRuntimeFromDB(dbPath); check.Status != doctorStatusFail {
		t.Fatalf("expected local doctor readback to fail on mismatch, got %+v", check)
	}
	if check := checkDurableRuntimeFromDetails(map[string]any{"service": payload}); check.Status != doctorStatusFail {
		t.Fatalf("expected health payload readback to fail on mismatch, got %+v", check)
	}
}

func createDurableRuntimeMetricsFixture(path string) error {
	return os.WriteFile(path, []byte(`{"schema_version":"1.0","timestamp":"2026-04-12T10:00:00Z","profiles":{"compute":{"total_runs":1,"success_count":1,"failure_count":0,"timeout_count":0,"failure_rate":0.0,"avg_duration_sec":0.1,"avg_startup_ms":10}}}`), 0o644)
}

func findRepoRootWithFixture(t *testing.T) string {
	t.Helper()

	start, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	dir := filepath.Clean(start)
	for {
		fixture := filepath.Join(dir, "tests", "fixtures", "fake_executor_bridge.py")
		if _, err := os.Stat(fixture); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not locate repo root from %s", start)
		}
		dir = parent
	}
}

func validDurableRuntimePromptEvidence(snapshotID string) map[string]any {
	return map[string]any{
		"contract":                "daemon_prompt_capability_evidence.v1",
		"prompt_compiler_status":  "daemon_converged",
		"c2_1_convergence":        "daemon_prompt_compiler_converged",
		"deployment_evidence":     "accepted_for_daemon_prompt_compiler_convergence",
		"capability_snapshot_id":  snapshotID,
		"capability_snapshot_ref": "capability_snapshot:" + snapshotID,
		"projection_source":       "agent.runtime_capability_snapshot",
		"projection_contract":     "active_capability_snapshot_projection.v1",
		"projection_digest":       "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"snapshot_schema":         "daemon_capability_snapshot.v1",
		"snapshot_kind":           "run",
		"snapshot_status":         "enabled",
		"prompt_contract":         "prompt_capabilities.v1",
	}
}

func writeDurableRuntimeCapabilitySnapshotFixture(t *testing.T, snapshot durableRuntimeCapabilitySnapshot) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), strings.TrimSpace(snapshot.SnapshotID)+".json")
	writeDurableRuntimeCapabilitySnapshotFixtureAt(t, path, snapshot)
	return path
}

func writeDurableRuntimeCapabilitySnapshotFixtureAt(t *testing.T, path string, snapshot durableRuntimeCapabilitySnapshot) {
	t.Helper()
	raw, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		t.Fatalf("marshal capability snapshot fixture: %v", err)
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0o600); err != nil {
		t.Fatalf("write capability snapshot fixture: %v", err)
	}
}
