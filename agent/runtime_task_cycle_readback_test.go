package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRuntimeReadLastDurableTaskCyclePhaseFromExecutionStepsAfterRestart(t *testing.T) {
	cfg := RuntimeConfig{WorkspaceID: "ws-1", AgentID: "agent-1"}
	task := WorkspaceTaskRecord{
		TaskID:      "task-readback",
		Title:       "Readback task",
		Description: "Verify restart readback",
		Status:      "RUNNING",
	}
	session := AgentSessionStateRecord{
		SessionID:  "session-readback",
		TaskID:     task.TaskID,
		Status:     "ACTIVE",
		OwnerScope: "task/session",
	}
	work := &AgentWorkPacket{
		WorkType:      "resume_session",
		ClaimAction:   "reuse_claim",
		SessionAction: "resume_inactive",
	}
	result := StructuredTaskResult{
		Outcome: "completed",
		Summary: "Finished after restart",
		Materialize: TaskMaterialization{
			DocKey:     "task.final",
			DocTitle:   "Final",
			DocContent: "done",
		},
	}

	initialRecords := taskCycleInitialPhaseRecords(cfg, task, session, "run-readback", work)
	progressRecords := taskCycleProgressPhaseRecords(cfg, task, session, "run-readback", work, &result, nil)
	terminalRecords := taskCycleTerminalPhaseRecords(cfg, task, session, "run-readback", work, &result, nil)

	persistedSteps := []ExecutionStepRecord{
		{
			StepID:      "step-plan",
			RunID:       "run-readback",
			WorkspaceID: cfg.WorkspaceID,
			Phase:       "PLAN",
			SortOrder:   10,
			VerificationJSON: map[string]any{
				"task_cycle_phase_records": initialRecords,
				"task_cycle_phase_record":  lastTaskCyclePhaseRecord(initialRecords),
			},
		},
		{
			StepID:      "step-execute",
			RunID:       "run-readback",
			WorkspaceID: cfg.WorkspaceID,
			Phase:       "EXECUTE",
			SortOrder:   20,
			VerificationJSON: map[string]any{
				"task_cycle_phase_records": progressRecords,
				"task_cycle_phase_record":  lastTaskCyclePhaseRecord(progressRecords),
			},
		},
		{
			StepID:      "step-verify",
			RunID:       "run-readback",
			WorkspaceID: cfg.WorkspaceID,
			Phase:       "VERIFY",
			SortOrder:   30,
			VerificationJSON: map[string]any{
				"task_cycle_phase_records": terminalRecords,
				"task_cycle_phase_record":  lastTaskCyclePhaseRecord(terminalRecords),
			},
		},
	}

	rawPersisted, err := json.Marshal(persistedSteps)
	if err != nil {
		t.Fatalf("marshal persisted steps: %v", err)
	}

	var readbackSteps []ExecutionStepRecord
	if err := json.Unmarshal(rawPersisted, &readbackSteps); err != nil {
		t.Fatalf("unmarshal persisted steps: %v", err)
	}

	restarted := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: cfg.WorkspaceID, AgentID: cfg.AgentID},
		activeTask: &WorkspaceTaskRecord{
			TaskID: "stale-task",
			Status: "RUNNING",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "stale-session",
			TaskID:    "stale-task",
			Status:    "ACTIVE",
		},
		activeRunID: "stale-run",
	}

	readback, ok := restarted.readLastDurableTaskCyclePhaseFromExecutionSteps(task.TaskID, readbackSteps)
	if !ok {
		t.Fatal("expected durable phase readback to succeed after restart")
	}
	if readback.PhaseName != "TERMINAL_TRANSITIONED" {
		t.Fatalf("phase_name = %q, want TERMINAL_TRANSITIONED", readback.PhaseName)
	}
	if readback.PhaseSequence != 8 {
		t.Fatalf("phase_sequence = %d, want 8", readback.PhaseSequence)
	}
	if readback.TaskID != task.TaskID {
		t.Fatalf("task_id = %q, want %q", readback.TaskID, task.TaskID)
	}
	if readback.RunID != "run-readback" {
		t.Fatalf("run_id = %q, want run-readback", readback.RunID)
	}
	if readback.SessionID != session.SessionID {
		t.Fatalf("session_id = %q, want %q", readback.SessionID, session.SessionID)
	}
	if readback.LastDurableStep != "TERMINAL_TRANSITIONED" {
		t.Fatalf("last_durable_step = %q, want TERMINAL_TRANSITIONED", readback.LastDurableStep)
	}
	if readback.TerminalStatus != "completed" {
		t.Fatalf("terminal_status = %q, want completed", readback.TerminalStatus)
	}
	if readback.CheckpointID == "" || readback.SelectionID == "" {
		t.Fatalf("expected restart readback to preserve durable identities, got %+v", readback)
	}
}

func TestRuntimeReadLastDurableTaskCyclePhaseUsesRPCDurableStoreAfterRestart(t *testing.T) {
	cfg := RuntimeConfig{WorkspaceID: "ws-rpc-readback", AgentID: "agent-rpc"}
	task := WorkspaceTaskRecord{
		TaskID:      "task-rpc-readback",
		Title:       "RPC readback task",
		Description: "Verify live durable readback",
		Status:      "RUNNING",
	}
	session := AgentSessionStateRecord{
		SessionID:  "session-rpc-readback",
		TaskID:     task.TaskID,
		Status:     "ACTIVE",
		OwnerScope: "task/session",
	}
	work := &AgentWorkPacket{
		WorkType:      "resume_session",
		ClaimAction:   "reuse_claim",
		SessionAction: "resume_inactive",
	}
	result := StructuredTaskResult{
		Outcome: "completed",
		Summary: "Finished from RPC readback",
		Materialize: TaskMaterialization{
			DocKey:     "task.rpc.final",
			DocTitle:   "RPC Final",
			DocContent: "done",
		},
	}
	terminalRecords := taskCycleTerminalPhaseRecords(cfg, task, session, "run-rpc-readback", work, &result, nil)
	persistedSteps := []ExecutionStepRecord{
		{
			StepID:      "step-rpc-plan",
			RunID:       "run-rpc-readback",
			WorkspaceID: cfg.WorkspaceID,
			Phase:       "PLAN",
			SortOrder:   10,
			VerificationJSON: map[string]any{
				"task_cycle_phase_records": taskCycleInitialPhaseRecords(cfg, task, session, "run-rpc-readback", work),
			},
		},
		{
			StepID:      "step-rpc-verify",
			RunID:       "run-rpc-readback",
			WorkspaceID: cfg.WorkspaceID,
			Phase:       "VERIFY",
			SortOrder:   30,
			VerificationJSON: map[string]any{
				"task_cycle_phase_records": terminalRecords,
				"task_cycle_phase_record":  lastTaskCyclePhaseRecord(terminalRecords),
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rhizomeRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		if req.Method != "workspace.execution.run.get" {
			t.Fatalf("method = %q, want workspace.execution.run.get", req.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result": map[string]any{
				"detail": ExecutionRunDetail{
					Run: ExecutionRunRecord{
						RunID:       "run-rpc-readback",
						WorkspaceID: cfg.WorkspaceID,
						TaskID:      task.TaskID,
						SessionID:   session.SessionID,
						AgentID:     cfg.AgentID,
						Status:      "COMPLETED",
					},
					Steps: persistedSteps,
				},
			},
		}); err != nil {
			t.Fatalf("encode rpc response: %v", err)
		}
	}))
	defer server.Close()

	restarted := &Runtime{
		cfg:         cfg,
		client:      NewRhizomeClient(server.URL, "test-token"),
		activeRunID: "stale-run",
	}

	readback, ok, err := restarted.readLastDurableTaskCyclePhase(context.Background(), "run-rpc-readback", task.TaskID)
	if err != nil {
		t.Fatalf("read durable phase from rpc: %v", err)
	}
	if !ok {
		t.Fatal("expected rpc durable phase readback to find a task cycle phase")
	}
	if readback.PhaseName != "TERMINAL_TRANSITIONED" {
		t.Fatalf("phase_name = %q, want TERMINAL_TRANSITIONED", readback.PhaseName)
	}
	if readback.RunID != "run-rpc-readback" || readback.TaskID != task.TaskID || readback.SessionID != session.SessionID {
		t.Fatalf("unexpected durable identities from rpc readback: %+v", readback)
	}
}

func TestRuntimeReadLastDurableTaskCyclePhaseRejectsMismatchedRPCIdentity(t *testing.T) {
	cfg := RuntimeConfig{WorkspaceID: "ws-rpc-readback-mismatch", AgentID: "agent-rpc"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rhizomeRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result": map[string]any{
				"detail": ExecutionRunDetail{
					Run: ExecutionRunRecord{
						RunID:       "run-other",
						WorkspaceID: cfg.WorkspaceID,
						TaskID:      "task-other",
						Status:      "ACTIVE",
					},
					Steps: []ExecutionStepRecord{
						{
							StepID:      "step-other",
							RunID:       "run-other",
							WorkspaceID: cfg.WorkspaceID,
							Phase:       "VERIFY",
							VerificationJSON: map[string]any{
								"task_cycle_phase_record": map[string]any{
									"task_id":        "task-target",
									"phase_name":     "TERMINAL_TRANSITIONED",
									"phase_sequence": 8,
									"run": map[string]any{
										"run_id": "run-target",
									},
								},
							},
						},
					},
				},
			},
		}); err != nil {
			t.Fatalf("encode rpc response: %v", err)
		}
	}))
	defer server.Close()

	restarted := &Runtime{
		cfg:    cfg,
		client: NewRhizomeClient(server.URL, "test-token"),
	}

	if _, ok, err := restarted.readLastDurableTaskCyclePhase(context.Background(), "run-target", "task-target"); err == nil {
		t.Fatalf("expected mismatched rpc run identity to fail closed, ok=%v", ok)
	}
}

func TestRuntimeReadLastDurableTaskCyclePhaseRejectsBlankStepIdentity(t *testing.T) {
	cfg := RuntimeConfig{WorkspaceID: "ws-rpc-readback-blank-step", AgentID: "agent-rpc"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rhizomeRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result": map[string]any{
				"detail": ExecutionRunDetail{
					Run: ExecutionRunRecord{
						RunID:       "run-target",
						WorkspaceID: cfg.WorkspaceID,
						TaskID:      "task-target",
						Status:      "ACTIVE",
					},
					Steps: []ExecutionStepRecord{
						{
							StepID: "step-blank-identity",
							Phase:  "VERIFY",
							VerificationJSON: map[string]any{
								"task_cycle_phase_record": map[string]any{
									"task_id":        "task-target",
									"phase_name":     "TERMINAL_TRANSITIONED",
									"phase_sequence": 8,
									"run": map[string]any{
										"run_id": "run-target",
									},
								},
							},
						},
					},
				},
			},
		}); err != nil {
			t.Fatalf("encode rpc response: %v", err)
		}
	}))
	defer server.Close()

	restarted := &Runtime{
		cfg:    cfg,
		client: NewRhizomeClient(server.URL, "test-token"),
	}

	if _, ok, err := restarted.readLastDurableTaskCyclePhase(context.Background(), "run-target", "task-target"); err == nil {
		t.Fatalf("expected blank step identity to fail closed, ok=%v", ok)
	}
}

// TestTaskCycleReadbackTieBreaksTowardNewestStep is the CA-28 regression: when two
// execution-step phase records share the same phase_sequence, the readback must
// resolve SourceStepAt deterministically toward the newest step time rather than
// whichever step the server happened to return last (which previously produced
// order-dependent false stall / false freshness verdicts).
func TestTaskCycleReadbackTieBreaksTowardNewestStep(t *testing.T) {
	record := func(cycleID string) map[string]any {
		return map[string]any{
			"task_cycle_phase_record": map[string]any{
				"phase_sequence": 5,
				"phase_name":     "execute",
				"task_cycle_id":  cycleID,
			},
		}
	}
	older := ExecutionStepRecord{
		StepID:           "step-older",
		Phase:            "execute",
		UpdatedAt:        "2026-01-01T00:00:00Z",
		VerificationJSON: record("cycle-1"),
	}
	newer := ExecutionStepRecord{
		StepID:           "step-newer",
		Phase:            "execute",
		UpdatedAt:        "2026-02-01T00:00:00Z",
		VerificationJSON: record("cycle-1"),
	}

	for _, tc := range []struct {
		name  string
		steps []ExecutionStepRecord
	}{
		{"newer-returned-last", []ExecutionStepRecord{older, newer}},
		{"newer-returned-first", []ExecutionStepRecord{newer, older}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := taskCycleLastDurablePhaseFromExecutionSteps("cycle-1", tc.steps)
			if !ok {
				t.Fatalf("expected a durable phase readback")
			}
			if got.SourceStepAt != newer.UpdatedAt {
				t.Fatalf("phase_sequence tie must resolve to newest step time %q, got %q (step %q)", newer.UpdatedAt, got.SourceStepAt, got.SourceStepID)
			}
			if got.SourceStepID != "step-newer" {
				t.Fatalf("expected SourceStepID step-newer, got %q", got.SourceStepID)
			}
		})
	}
}
