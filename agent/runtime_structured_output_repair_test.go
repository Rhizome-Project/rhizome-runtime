package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestExecuteTaskCycleRepairsStructuredOutputOnceAndCompletes(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	self := "agent-1"
	claimed := "CLAIMED"
	task := WorkspaceTaskRecord{
		TaskID:       "task-1",
		Title:        "Task One",
		Status:       "RUNNING",
		ClaimAgentID: &self,
		ClaimStatus:  &claimed,
	}
	session := AgentSessionStateRecord{
		SessionID: "session-1",
		TaskID:    "task-1",
		Status:    "ACTIVE",
	}

	var (
		stepWrites   []map[string]any
		runOutcomes  []string
		taskComplete int
		taskRelease  int
		docKeys      []string
		methods      []string
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "workspace.execution.step.write":
			stepWrites = append(stepWrites, req.Params)
			writeRPCResult(w, req, map[string]any{"step": map[string]any{"step_id": "step-" + rpcString(req.Params, "phase")}})
		case "workspace.execution.run.write":
			runOutcomes = append(runOutcomes, rpcString(req.Params, "outcome"))
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": rpcString(req.Params, "run_id")}})
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{}})
		case "workspace.instrumentation.locus.bundle":
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"generated_at":     "2026-04-21T00:00:00Z",
					"resolved":         true,
					"proto_cluster_id": "cluster-1",
				},
			})
		case "workspace.instrumentation.control.report":
			writeRPCResult(w, req, map[string]any{"report": map[string]any{}})
		case "workspace.tension.frontier":
			writeRPCResult(w, req, map[string]any{"items": []any{}})
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{
				"tasks": []WorkspaceTaskRecord{
					{TaskID: task.TaskID, Status: "RUNNING", ClaimAgentID: &self, ClaimStatus: &claimed},
				},
			})
		case "workspace.doc.put":
			docKeys = append(docKeys, rpcString(req.Params, "doc_key"))
			writeRPCResult(w, req, map[string]any{"sha": "sha-" + rpcString(req.Params, "doc_key")})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update": map[string]any{"update_id": "update-1"}})
		case "workspace.ops.get":
			writeRPCResult(w, req, map[string]any{"item": map[string]any{}})
		case "workspace.ops.resolve":
			writeRPCResult(w, req, map[string]any{"item": map[string]any{}})
		case "agent.task.complete":
			taskComplete++
			writeRPCResult(w, req, nil)
		case "agent.session.end":
			writeRPCResult(w, req, map[string]any{"state": map[string]any{"session_id": rpcString(req.Params, "session_id"), "status": rpcString(req.Params, "status")}})
		case "agent.state.set":
			var state RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &state); err != nil {
				t.Fatalf("decode scratch state: %v", err)
			}
			writeRPCResult(w, req, nil)
		case "agent.bootstrap":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-04-21T00:00:00Z",
				"agent": map[string]any{
					"agent_id":         self,
					"workspace_id":     "ws-1",
					"owner_user_id":    "owner-1",
					"display_name":     "Agent One",
					"role":             "generalist",
					"status":           "ACTIVE",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"summary":          "bootstrapped",
					"created_at":       "2026-04-21T00:00:00Z",
					"updated_at":       "2026-04-21T00:00:00Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
				"snapshot": map[string]any{
					"workspace": map[string]any{"workspace_id": "ws-1", "title": "Workspace One", "status": "ACTIVE"},
					"docs":      []any{},
					"agents":    []any{},
					"sessions":  []any{},
					"tools":     []any{},
					"tasks":     []any{},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "codex", "daily_remaining": 1000, "weekly_remaining": 5000})
		default:
			t.Fatalf("unexpected method in repair success path: %s", req.Method)
		}
	}))
	defer server.Close()

	llm := &sequenceLLM{responses: []*LLMResponse{
		{Content: `{"outcome":"completed","summary":"broken payload",`},
		{Content: `{"outcome":"completed","summary":"fixed after repair","details":"repaired once","blocked_on":[]}`},
	}}
	runtime := NewRuntime(RuntimeConfig{
		Mode:         RuntimeModeDaemon,
		Workdir:      t.TempDir(),
		RhizomeRPC:   server.URL,
		RhizomeToken: "token",
		WorkspaceID:  "ws-1",
		AgentID:      self,
		DisplayName:  "Agent One",
		OwnerUserID:  "owner-1",
	}, llm)
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.activeTask = &task
	runtime.activeSession = &session
	runtime.activeRunID = "run-1"
	runtime.scratch = RuntimeScratchState{
		ActiveTaskID:    "task-1",
		ActiveSessionID: "session-1",
		ActiveRunID:     "run-1",
		DocSHAs:         map[string]string{},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.executeTaskCycle(context.Background(), task); err != nil {
		t.Fatalf("executeTaskCycle() error = %v", err)
	}

	if len(llm.calls) != 2 {
		t.Fatalf("expected one bounded repair attempt, got %d calls", len(llm.calls))
	}
	if len(llm.calls[1]) < 2 || !strings.Contains(llm.calls[1][1].Content, structuredOutputRepairAction) {
		t.Fatalf("expected second LLM call to be a structured-output repair prompt, got %+v", llm.calls[1])
	}
	assertStructuredRepairContract(t, llm, 1, runtime.cfg, task, session, "run-1")
	if len(stepWrites) != 3 {
		t.Fatalf("expected plan/execute/verify step writes, got %d", len(stepWrites))
	}
	planStep := stepWriteByPhase(t, stepWrites, "PLAN")
	planVerification := nestedMapField(t, planStep, "verification")
	snapshotID := stringMapField(t, planVerification, "capability_snapshot_id")
	if snapshotID == "" {
		t.Fatalf("expected plan step to include capability snapshot id, got %+v", planVerification)
	}
	if planEvidence := rpcStringSlice(planStep, "evidence"); !containsString(planEvidence, capabilitySnapshotEvidenceRef(snapshotID)) {
		t.Fatalf("expected plan evidence to include capability snapshot ref, got %+v", planEvidence)
	}
	snapshot := nestedMapField(t, planVerification, "capability_snapshot")
	if got := stringMapField(t, snapshot, "schema"); got != daemonCapabilitySnapshotSchema {
		t.Fatalf("expected run capability snapshot schema %q, got %q", daemonCapabilitySnapshotSchema, got)
	}
	planPromptEvidence := nestedMapField(t, planVerification, "prompt_capability_evidence")
	if got := stringMapField(t, planPromptEvidence, "contract"); got != daemonPromptCapabilityEvidenceContract {
		t.Fatalf("expected plan prompt capability evidence contract %q, got %+v", daemonPromptCapabilityEvidenceContract, planPromptEvidence)
	}
	if got := stringMapField(t, planPromptEvidence, "capability_snapshot_ref"); got != capabilitySnapshotEvidenceRef(snapshotID) {
		t.Fatalf("expected plan prompt capability evidence snapshot ref %q, got %+v", capabilitySnapshotEvidenceRef(snapshotID), planPromptEvidence)
	}
	executeStep := stepWriteByPhase(t, stepWrites, "EXECUTE")
	if got := rpcString(executeStep, "status"); got != "COMPLETED" {
		t.Fatalf("expected repaired execute step to stay completed, got %q", got)
	}
	verification := nestedMapField(t, executeStep, "verification")
	if got := stringMapField(t, verification, "capability_snapshot_id"); got != snapshotID {
		t.Fatalf("execute capability snapshot id = %q, want %q", got, snapshotID)
	}
	executeSnapshot := nestedMapField(t, verification, "capability_snapshot")
	if got := stringMapField(t, executeSnapshot, "snapshot_id"); got != snapshotID {
		t.Fatalf("expected execute step to carry embedded capability snapshot %q, got %+v", snapshotID, executeSnapshot)
	}
	executePromptEvidence := nestedMapField(t, verification, "prompt_capability_evidence")
	if got := stringMapField(t, executePromptEvidence, "c2_1_convergence"); got != daemonPromptCompilerConvergenceAccepted {
		t.Fatalf("expected accepted execute prompt convergence evidence, got %+v", executePromptEvidence)
	}
	structuredOutput := nestedMapField(t, verification, "structured_output_repair")
	if got := stringMapField(t, structuredOutput, "status"); got != "repaired" {
		t.Fatalf("expected repaired status, got %q", got)
	}
	if got := rpcIntValue(t, structuredOutput, "attempts"); got != 1 {
		t.Fatalf("expected one repair attempt, got %d", got)
	}
	if got := stringMapField(t, structuredOutput, "failure_code"); got != structuredOutputFailureCodeInvalid {
		t.Fatalf("expected failure code %q, got %q", structuredOutputFailureCodeInvalid, got)
	}
	if got := rpcIntValue(t, verification, "assistant_turns"); got != 2 {
		t.Fatalf("expected two assistant turns including repair, got %d", got)
	}
	evidence := rpcStringSlice(executeStep, "evidence")
	if !containsString(evidence, capabilitySnapshotEvidenceRef(snapshotID)) || !containsString(evidence, structuredOutputRepairAction) {
		t.Fatalf("expected snapshot and repair evidence markers, got %+v", evidence)
	}
	if len(runOutcomes) != 1 || runOutcomes[0] != "COMPLETED" {
		t.Fatalf("expected completed run outcome, got %+v methods=%v", runOutcomes, methods)
	}
	if taskComplete != 1 || taskRelease != 0 {
		t.Fatalf("expected completion path without release, got complete=%d release=%d", taskComplete, taskRelease)
	}
	if !containsTrimmed(docKeys, claimedWorkDocKey(self)) || !containsTrimmed(docKeys, agentContextDocKey(self)) {
		t.Fatalf("expected durable doc writes for current context and claimed work, got %+v", docKeys)
	}
}

func TestExecuteTaskCycleTreatsResolvedOutcomeAsCompleted(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	self := "agent-1"
	claimed := "CLAIMED"
	task := WorkspaceTaskRecord{
		TaskID:       "task-1",
		Title:        "Task One",
		Status:       "RUNNING",
		ClaimAgentID: &self,
		ClaimStatus:  &claimed,
	}
	session := AgentSessionStateRecord{
		SessionID: "session-1",
		TaskID:    "task-1",
		Status:    "ACTIVE",
	}

	var (
		runOutcomes  []string
		taskBlock    int
		taskComplete int
		taskRelease  int
		methods      []string
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "workspace.execution.step.write":
			writeRPCResult(w, req, map[string]any{"step": map[string]any{"step_id": "step-" + rpcString(req.Params, "phase")}})
		case "workspace.execution.run.write":
			runOutcomes = append(runOutcomes, rpcString(req.Params, "outcome"))
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": rpcString(req.Params, "run_id")}})
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{}})
		case "workspace.instrumentation.locus.bundle":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{"generated_at": "2026-04-21T00:00:00Z", "resolved": true}})
		case "workspace.instrumentation.control.report":
			writeRPCResult(w, req, map[string]any{"report": map[string]any{}})
		case "workspace.tension.frontier":
			writeRPCResult(w, req, map[string]any{"items": []any{}})
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{
				"tasks": []WorkspaceTaskRecord{
					{TaskID: task.TaskID, Status: "RUNNING", ClaimAgentID: &self, ClaimStatus: &claimed},
				},
			})
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-" + rpcString(req.Params, "doc_key")})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update": map[string]any{"update_id": "update-1"}})
		case "workspace.ops.get":
			writeRPCResult(w, req, map[string]any{"item": map[string]any{}})
		case "workspace.ops.resolve":
			writeRPCResult(w, req, map[string]any{"item": map[string]any{}})
		case "agent.task.complete":
			taskComplete++
			writeRPCResult(w, req, nil)
		case "agent.task.block":
			taskBlock++
			writeRPCResult(w, req, nil)
		case "agent.task.release":
			taskRelease++
			writeRPCResult(w, req, nil)
		case "agent.session.end":
			writeRPCResult(w, req, map[string]any{"state": map[string]any{"session_id": rpcString(req.Params, "session_id"), "status": rpcString(req.Params, "status")}})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "agent.bootstrap":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-04-21T00:00:00Z",
				"agent": map[string]any{
					"agent_id":         self,
					"workspace_id":     "ws-1",
					"owner_user_id":    "owner-1",
					"display_name":     "Agent One",
					"role":             "generalist",
					"status":           "ACTIVE",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"summary":          "bootstrapped",
					"created_at":       "2026-04-21T00:00:00Z",
					"updated_at":       "2026-04-21T00:00:00Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
				"snapshot": map[string]any{
					"workspace": map[string]any{"workspace_id": "ws-1", "title": "Workspace One", "status": "ACTIVE"},
					"docs":      []any{},
					"agents":    []any{},
					"sessions":  []any{},
					"tools":     []any{},
					"tasks":     []any{},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "codex", "daily_remaining": 1000, "weekly_remaining": 5000})
		default:
			t.Fatalf("unexpected method in resolved alias path: %s", req.Method)
		}
	}))
	defer server.Close()

	llm := &sequenceLLM{responses: []*LLMResponse{{
		Content: `{"outcome":"resolved","summary":"resolved alias","details":"all done","blocked_on":[]}`,
	}}}
	runtime := NewRuntime(RuntimeConfig{
		Mode:         RuntimeModeDaemon,
		Workdir:      t.TempDir(),
		RhizomeRPC:   server.URL,
		RhizomeToken: "token",
		WorkspaceID:  "ws-1",
		AgentID:      self,
		DisplayName:  "Agent One",
		OwnerUserID:  "owner-1",
	}, llm)
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.activeTask = &task
	runtime.activeSession = &session
	runtime.activeRunID = "run-1"
	runtime.scratch = RuntimeScratchState{
		ActiveTaskID:    "task-1",
		ActiveSessionID: "session-1",
		ActiveRunID:     "run-1",
		DocSHAs:         map[string]string{},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.executeTaskCycle(context.Background(), task); err != nil {
		t.Fatalf("executeTaskCycle() error = %v", err)
	}
	if taskComplete != 1 || taskBlock != 0 || taskRelease != 0 {
		t.Fatalf("resolved alias should complete without block/release, complete=%d block=%d release=%d methods=%v", taskComplete, taskBlock, taskRelease, methods)
	}
	if len(runOutcomes) != 1 || runOutcomes[0] != "COMPLETED" {
		t.Fatalf("expected completed run for resolved alias, got %+v methods=%v", runOutcomes, methods)
	}
}

func TestResolveStructuredTaskResultTrustFirstTreatsMissingReflectionAsAdvisory(t *testing.T) {
	runtime := &Runtime{cfg: RuntimeConfig{CoordinationMode: CoordinationModeTrustFirst}}
	result, repairInfo, err := runtime.resolveStructuredTaskResult(
		context.Background(),
		WorkspaceTaskRecord{TaskID: "task-1", Title: "Task One"},
		AgentSessionStateRecord{SessionID: "session-1", TaskID: "task-1"},
		"run-1",
		`{"outcome":"completed","summary":"done without reflection"}`,
	)
	if err != nil {
		t.Fatalf("resolveStructuredTaskResult() error = %v", err)
	}
	if normalizeOutcome(result.Outcome) != "completed" {
		t.Fatalf("trust-first missing reflection should remain advisory instead of blocking completion, got %+v", result)
	}
	if result.Reflection != nil {
		t.Fatalf("expected absent reflection to stay visible as telemetry, got %+v", result.Reflection)
	}
	if repairInfo == nil || !repairInfo.Attempted || !strings.Contains(repairInfo.ParseError, "trust_first structured result requested") {
		t.Fatalf("expected bounded reflection repair attempt metadata, got %+v", repairInfo)
	}
	if !strings.Contains(result.Details, "trust_first reflection advisory missing") {
		t.Fatalf("expected advisory reflection detail, got %+v", result)
	}
}

func TestExecuteTaskCycleFailsClosedAfterSingleStructuredRepairAttempt(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	self := "agent-1"
	claimed := "CLAIMED"
	task := WorkspaceTaskRecord{
		TaskID:       "task-1",
		Title:        "Task One",
		Status:       "RUNNING",
		ClaimAgentID: &self,
		ClaimStatus:  &claimed,
	}
	session := AgentSessionStateRecord{
		SessionID: "session-1",
		TaskID:    "task-1",
		Status:    "ACTIVE",
	}

	var (
		stepWrites   []map[string]any
		runOutcomes  []string
		taskComplete int
		taskRelease  int
		methods      []string
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)

		switch req.Method {
		case "workspace.execution.step.write":
			stepWrites = append(stepWrites, req.Params)
			writeRPCResult(w, req, map[string]any{"step": map[string]any{"step_id": "step-" + rpcString(req.Params, "phase")}})
		case "workspace.execution.run.write":
			runOutcomes = append(runOutcomes, rpcString(req.Params, "outcome"))
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": rpcString(req.Params, "run_id")}})
		case "agent.task.hydrate":
			writeRPCResult(w, req, map[string]any{"bundle": map[string]any{}})
		case "workspace.instrumentation.locus.bundle":
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"generated_at":     "2026-04-21T00:00:00Z",
					"resolved":         true,
					"proto_cluster_id": "cluster-1",
				},
			})
		case "workspace.instrumentation.control.report":
			writeRPCResult(w, req, map[string]any{"report": map[string]any{}})
		case "workspace.tension.frontier":
			writeRPCResult(w, req, map[string]any{"items": []any{}})
		case "workspace.tasks.list":
			writeRPCResult(w, req, map[string]any{
				"tasks": []WorkspaceTaskRecord{
					{TaskID: task.TaskID, Status: "RUNNING", ClaimAgentID: &self, ClaimStatus: &claimed},
				},
			})
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-" + rpcString(req.Params, "doc_key")})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"update": map[string]any{"update_id": "update-1"}})
		case "workspace.ops.get":
			writeRPCResult(w, req, map[string]any{"item": map[string]any{}})
		case "workspace.ops.resolve":
			writeRPCResult(w, req, map[string]any{"item": map[string]any{}})
		case "agent.task.complete":
			taskComplete++
			writeRPCResult(w, req, nil)
		case "agent.task.release":
			taskRelease++
			writeRPCResult(w, req, nil)
		case "agent.session.end":
			writeRPCResult(w, req, map[string]any{"state": map[string]any{"session_id": rpcString(req.Params, "session_id"), "status": rpcString(req.Params, "status")}})
		case "agent.state.set":
			writeRPCResult(w, req, nil)
		case "agent.bootstrap":
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-04-21T00:00:00Z",
				"agent": map[string]any{
					"agent_id":         self,
					"workspace_id":     "ws-1",
					"owner_user_id":    "owner-1",
					"display_name":     "Agent One",
					"role":             "generalist",
					"status":           "ACTIVE",
					"protocol_version": "rnar/v1",
					"capabilities":     []string{"tool.call"},
					"summary":          "bootstrapped",
					"created_at":       "2026-04-21T00:00:00Z",
					"updated_at":       "2026-04-21T00:00:00Z",
					"is_online":        true,
					"active_tasks":     []any{},
				},
				"snapshot": map[string]any{
					"workspace": map[string]any{"workspace_id": "ws-1", "title": "Workspace One", "status": "ACTIVE"},
					"docs":      []any{},
					"agents":    []any{},
					"sessions":  []any{},
					"tools":     []any{},
					"tasks":     []any{},
				},
			})
		case "agent.limits.get":
			writeRPCResult(w, req, map[string]any{"group_id": "codex", "daily_remaining": 1000, "weekly_remaining": 5000})
		default:
			t.Fatalf("unexpected method in repair failure path: %s", req.Method)
		}
	}))
	defer server.Close()

	llm := &sequenceLLM{responses: []*LLMResponse{
		{Content: `{"outcome":"completed","summary":"broken payload",`},
		{Content: `{"outcome":"completed","summary":"still broken","unexpected":"value"}`},
	}}
	runtime := NewRuntime(RuntimeConfig{
		Mode:         RuntimeModeDaemon,
		Workdir:      t.TempDir(),
		RhizomeRPC:   server.URL,
		RhizomeToken: "token",
		WorkspaceID:  "ws-1",
		AgentID:      self,
		DisplayName:  "Agent One",
		OwnerUserID:  "owner-1",
	}, llm)
	runtime.client = NewRhizomeClient(server.URL, "token")
	runtime.activeTask = &task
	runtime.activeSession = &session
	runtime.activeRunID = "run-1"
	runtime.scratch = RuntimeScratchState{
		ActiveTaskID:    "task-1",
		ActiveSessionID: "session-1",
		ActiveRunID:     "run-1",
		DocSHAs:         map[string]string{},
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.executeTaskCycle(context.Background(), task); err != nil {
		t.Fatalf("executeTaskCycle() error = %v", err)
	}

	if len(llm.calls) != 2 {
		t.Fatalf("expected one bounded repair attempt, got %d calls", len(llm.calls))
	}
	assertStructuredRepairContract(t, llm, 1, runtime.cfg, task, session, "run-1")
	if len(stepWrites) != 3 {
		t.Fatalf("expected plan/execute/verify step writes, got %d", len(stepWrites))
	}
	executeStep := stepWriteByPhase(t, stepWrites, "EXECUTE")
	if got := rpcString(executeStep, "status"); got != "FAILED" {
		t.Fatalf("expected failed execute step after repair exhaustion, got %q", got)
	}
	verification := nestedMapField(t, executeStep, "verification")
	snapshotID := stringMapField(t, verification, "capability_snapshot_id")
	if snapshotID == "" {
		t.Fatalf("expected execute step to include capability snapshot id, got %+v", verification)
	}
	executePromptEvidence := nestedMapField(t, verification, "prompt_capability_evidence")
	if got := stringMapField(t, executePromptEvidence, "contract"); got != daemonPromptCapabilityEvidenceContract {
		t.Fatalf("expected failed execute prompt capability evidence contract %q, got %+v", daemonPromptCapabilityEvidenceContract, executePromptEvidence)
	}
	structuredOutput := nestedMapField(t, verification, "structured_output_repair")
	if got := stringMapField(t, structuredOutput, "status"); got != "repair_failed" {
		t.Fatalf("expected repair_failed status, got %q", got)
	}
	if got := rpcIntValue(t, structuredOutput, "attempts"); got != 1 {
		t.Fatalf("expected one repair attempt, got %d", got)
	}
	if got := stringMapField(t, structuredOutput, "failure_code"); got != structuredOutputFailureCodeInvalid {
		t.Fatalf("expected failure code %q, got %q", structuredOutputFailureCodeInvalid, got)
	}
	if got := rpcIntValue(t, verification, "assistant_turns"); got != 2 {
		t.Fatalf("expected two assistant turns including repair, got %d", got)
	}
	evidence := rpcStringSlice(executeStep, "evidence")
	if !containsString(evidence, capabilitySnapshotEvidenceRef(snapshotID)) || !containsString(evidence, structuredOutputRepairAction) {
		t.Fatalf("expected snapshot and repair evidence markers, got %+v", evidence)
	}
	if len(runOutcomes) != 1 || runOutcomes[0] != "FAILED" {
		t.Fatalf("expected failed run outcome, got %+v methods=%v", runOutcomes, methods)
	}
	if taskComplete != 0 || taskRelease != 1 {
		t.Fatalf("expected release path without completion, got complete=%d release=%d", taskComplete, taskRelease)
	}
	if !strings.Contains(strings.ToLower(rpcString(executeStep, "summary")), "structured output repair failed") {
		t.Fatalf("expected failure summary to reflect repair exhaustion, got %q", rpcString(executeStep, "summary"))
	}
}

func TestResolveStructuredTaskResultRepairsLegacyExtraFieldOnce(t *testing.T) {
	llm := &sequenceLLM{responses: []*LLMResponse{
		{Content: `{"outcome":"completed","summary":"legacy field stripped","details":null,"blocked_on":[]}`},
	}}
	runtime := &Runtime{
		cfg: RuntimeConfig{
			Mode:        RuntimeModeDaemon,
			WorkspaceID: "ws-legacy",
			AgentID:     "agent-legacy",
			DisplayName: "Agent Legacy",
		},
		agent: &Agent{LLM: llm},
	}
	task := WorkspaceTaskRecord{TaskID: "task-legacy", Title: "Legacy Output"}
	session := AgentSessionStateRecord{SessionID: "sess-legacy", TaskID: "task-legacy"}
	bindStructuredRepairAuthority(runtime, task, session, "run-legacy")

	result, repairInfo, err := runtime.resolveStructuredTaskResult(
		context.Background(),
		task,
		session,
		"run-legacy",
		`{"outcome":"completed","summary":"ok","details":null,"legacy":"x"}`,
	)
	if err != nil {
		t.Fatalf("expected repair to preserve compatibility with benign legacy output, got %v", err)
	}
	if result.Outcome != "completed" || result.Summary != "legacy field stripped" {
		t.Fatalf("unexpected repaired result %+v", result)
	}
	if repairInfo == nil || !repairInfo.Attempted || repairInfo.Attempts != 1 {
		t.Fatalf("expected exactly one repair attempt, got %+v", repairInfo)
	}
	if len(llm.calls) != 1 {
		t.Fatalf("expected one repair LLM call, got %d", len(llm.calls))
	}
	assertStructuredRepairContract(t, llm, 0, runtime.cfg, task, session, "run-legacy")
}

func TestStructuredOutputFailureBlocksPatchQueueConvergenceTask(t *testing.T) {
	task := WorkspaceTaskRecord{
		TaskID:      "task-patchq-integrate-project-item",
		Title:       "Integrate accepted candidate branch-1",
		Description: "Use project_patch_queue_integrate for queue_id=queue-1 item_id=item-1.",
		ProjectLane: "integration",
		Tags:        []string{"project", "patch-queue", "integration"},
	}
	result := StructuredTaskResult{
		Outcome: "failed",
		Summary: "Structured output repair failed; parse error: unexpected end of JSON input",
	}
	if failedPatchQueueConvergenceTask(task, result, nil) {
		t.Fatalf("ordinary failed patch queue task must not terminal-block without structured-output repair evidence")
	}
	repairInfo := &structuredOutputRepairInfo{
		Attempted:  true,
		Attempts:   1,
		ParseError: "unexpected end of JSON input",
	}
	if !failedPatchQueueConvergenceTask(task, result, repairInfo) {
		t.Fatalf("expected patch queue integration task failure to be terminal-blocked")
	}
	blocked := blockedPatchQueueConvergenceFailureResult(result)
	if blocked.Outcome != "blocked" || len(blocked.BlockedOn) == 0 || !strings.Contains(blocked.NextAction, "structured_output_repair") {
		t.Fatalf("unexpected blocked patch queue convergence result: %+v", blocked)
	}
}

func TestResolveStructuredTaskResultRejectsRepairToolCalls(t *testing.T) {
	llm := &sequenceLLM{responses: []*LLMResponse{
		{
			Content: `{"outcome":"completed","summary":"tool call attempted","blocked_on":[]}`,
			ToolCalls: []ToolCall{{
				ID:   "call-1",
				Type: "function",
				Function: FunctionCall{
					Name:      "read_file",
					Arguments: `{"path":"README.md"}`,
				},
			}},
		},
	}}
	runtime := &Runtime{
		cfg: RuntimeConfig{
			Mode:        RuntimeModeDaemon,
			WorkspaceID: "ws-1",
			AgentID:     "agent-1",
			DisplayName: "Agent One",
		},
		agent: &Agent{LLM: llm},
	}
	task := WorkspaceTaskRecord{TaskID: "task-toolcall", Title: "Toolcall Repair"}
	session := AgentSessionStateRecord{SessionID: "sess-toolcall", TaskID: "task-toolcall"}
	bindStructuredRepairAuthority(runtime, task, session, "run-toolcall")

	_, repairInfo, err := runtime.resolveStructuredTaskResult(
		context.Background(),
		task,
		session,
		"run-toolcall",
		`{"outcome":"completed","summary":"broken",`,
	)
	if err == nil {
		t.Fatalf("expected repair tool call to fail closed")
	}
	if repairInfo == nil || !repairInfo.Attempted || repairInfo.Attempts != 1 {
		t.Fatalf("expected exactly one attempted repair, got %+v", repairInfo)
	}
	if !strings.Contains(repairInfo.RepairError, "tool calls") {
		t.Fatalf("expected repair error to mention forbidden tool calls, got %+v", repairInfo)
	}
	assertStructuredRepairContract(t, llm, 0, runtime.cfg, task, session, "run-toolcall")
}

func TestRepairStructuredTaskResultRejectsBlankAuthorityContextBeforeLLM(t *testing.T) {
	llm := &sequenceLLM{responses: []*LLMResponse{{Content: `{"outcome":"failed","summary":"should not run","blocked_on":[]}`}}}
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-repair",
		AgentID:     "agent-repair",
	}, llm)
	t.Cleanup(func() { _ = runtime.Close() })

	_, _, err := runtime.resolveStructuredTaskResult(
		context.Background(),
		WorkspaceTaskRecord{},
		AgentSessionStateRecord{},
		"",
		`{"outcome":"completed","summary":"broken",`,
	)
	if err == nil {
		t.Fatalf("expected blank repair authority context to fail closed")
	}
	if len(llm.calls) != 0 {
		t.Fatalf("blank repair context should fail before LLM call, got %+v", llm.calls)
	}
}

func TestRepairStructuredTaskResultRejectsNonDaemonModeBeforeLLM(t *testing.T) {
	llm := &sequenceLLM{responses: []*LLMResponse{{Content: `{"outcome":"failed","summary":"should not run","blocked_on":[]}`}}}
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeTUI,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-repair",
		AgentID:     "agent-repair",
	}, llm)
	t.Cleanup(func() { _ = runtime.Close() })
	task := WorkspaceTaskRecord{TaskID: "task-repair", Title: "Repair"}
	session := AgentSessionStateRecord{SessionID: "sess-repair", TaskID: "task-repair"}
	bindStructuredRepairAuthority(runtime, task, session, "run-repair")

	_, _, err := runtime.resolveStructuredTaskResult(
		context.Background(),
		task,
		session,
		"run-repair",
		`{"outcome":"completed","summary":"broken",`,
	)
	if err == nil {
		t.Fatalf("expected non-daemon repair authority context to fail closed")
	}
	if len(llm.calls) != 0 {
		t.Fatalf("non-daemon repair should fail before LLM call, got %+v", llm.calls)
	}
}

func TestRepairStructuredTaskResultRejectsMismatchedActiveBindingBeforeLLM(t *testing.T) {
	llm := &sequenceLLM{responses: []*LLMResponse{{Content: `{"outcome":"failed","summary":"should not run","blocked_on":[]}`}}}
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-repair",
		AgentID:     "agent-repair",
	}, llm)
	t.Cleanup(func() { _ = runtime.Close() })
	activeTask := WorkspaceTaskRecord{TaskID: "task-active", Title: "Active"}
	activeSession := AgentSessionStateRecord{SessionID: "sess-active", TaskID: "task-active"}
	bindStructuredRepairAuthority(runtime, activeTask, activeSession, "run-active")

	_, _, err := runtime.resolveStructuredTaskResult(
		context.Background(),
		WorkspaceTaskRecord{TaskID: "task-other", Title: "Other"},
		AgentSessionStateRecord{SessionID: "sess-other", TaskID: "task-other"},
		"run-other",
		`{"outcome":"completed","summary":"broken",`,
	)
	if err == nil {
		t.Fatalf("expected mismatched repair authority context to fail closed")
	}
	if len(llm.calls) != 0 {
		t.Fatalf("mismatched repair context should fail before LLM call, got %+v", llm.calls)
	}
}

func TestRepairStructuredTaskResultRejectsMismatchedSnapshotBindingBeforeLLM(t *testing.T) {
	llm := &sequenceLLM{responses: []*LLMResponse{{Content: `{"outcome":"failed","summary":"should not run","blocked_on":[]}`}}}
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-repair",
		AgentID:     "agent-repair",
	}, llm)
	t.Cleanup(func() { _ = runtime.Close() })
	task := WorkspaceTaskRecord{TaskID: "task-repair", Title: "Repair"}
	session := AgentSessionStateRecord{SessionID: "sess-repair", TaskID: "task-repair"}
	bindStructuredRepairAuthority(runtime, task, session, "run-repair")
	runtime.activeCapabilitySnapshotID = "cap_stale"

	_, _, err := runtime.resolveStructuredTaskResult(
		context.Background(),
		task,
		session,
		"run-repair",
		`{"outcome":"completed","summary":"broken",`,
	)
	if err == nil {
		t.Fatalf("expected mismatched repair snapshot context to fail closed")
	}
	if len(llm.calls) != 0 {
		t.Fatalf("mismatched repair snapshot should fail before LLM call, got %+v", llm.calls)
	}
}

func bindStructuredRepairAuthority(runtime *Runtime, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string) {
	if runtime == nil {
		return
	}
	if strings.TrimSpace(session.TaskID) == "" {
		session.TaskID = task.TaskID
	}
	snapshotID := daemonRunCapabilitySnapshotID(runtime.cfg, task, session, runID)
	runtime.activeTask = &task
	runtime.activeSession = &session
	runtime.activeRunID = strings.TrimSpace(runID)
	runtime.activeCapabilitySnapshotID = snapshotID
	runtime.scratch.ActiveTaskID = strings.TrimSpace(task.TaskID)
	runtime.scratch.ActiveSessionID = strings.TrimSpace(session.SessionID)
	runtime.scratch.ActiveRunID = strings.TrimSpace(runID)
	runtime.scratch.ActiveCapabilitySnapshotID = snapshotID
}

func assertStructuredRepairContract(t *testing.T, llm *sequenceLLM, callIndex int, cfg RuntimeConfig, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string) {
	t.Helper()
	if len(llm.calls) <= callIndex {
		t.Fatalf("missing repair call %d in %+v", callIndex, llm.calls)
	}
	if len(llm.calls[callIndex]) < 2 {
		t.Fatalf("repair call %d has insufficient messages: %+v", callIndex, llm.calls[callIndex])
	}
	if got := llm.calls[callIndex][0].Role; got != "system" {
		t.Fatalf("repair call %d first role = %q, want system", callIndex, got)
	}
	if got := llm.calls[callIndex][1].Role; got != "user" {
		t.Fatalf("repair call %d second role = %q, want user", callIndex, got)
	}
	if len(llm.tools) <= callIndex {
		t.Fatalf("missing recorded tool set for repair call %d", callIndex)
	}
	if got := len(llm.tools[callIndex]); got != 0 {
		t.Fatalf("repair call %d tools = %d, want 0", callIndex, got)
	}

	system := llm.calls[callIndex][0].Content
	user := llm.calls[callIndex][1].Content
	snapshotID := daemonRunCapabilitySnapshotID(cfg, task, session, runID)
	expectedSystem := []string{
		"## Structured Output Repair Contract",
		"- repair_contract: " + structuredOutputRepairContract,
		"- repair_mode: " + structuredOutputRepairMode,
		"- repair_action: " + structuredOutputRepairAction,
		"- tool_call_policy: forbidden",
		"- side_effect_policy: forbidden",
		"- authority_scope: existing_task_cycle_result_repair_only",
		"- task_id: " + strings.TrimSpace(task.TaskID),
		"- session_id: " + strings.TrimSpace(session.SessionID),
		"- run_id: " + strings.TrimSpace(runID),
		"- capability_snapshot_id: " + snapshotID,
		"- capability_snapshot_ref: " + capabilitySnapshotEvidenceRef(snapshotID),
		"Do not call tools, browse, inspect files, mutate state, or invent new work.",
	}
	for _, want := range expectedSystem {
		if !strings.Contains(system, want) {
			t.Fatalf("repair system prompt missing %q:\n%s", want, system)
		}
	}
	expectedUser := []string{
		"This is a bounded " + structuredOutputRepairAction + " attempt.",
		"Structured task result contract:",
		"- task_id: " + strings.TrimSpace(task.TaskID),
		"- run_id: " + strings.TrimSpace(runID),
		"Validation error:",
		"Original output:",
	}
	for _, want := range expectedUser {
		if !strings.Contains(user, want) {
			t.Fatalf("repair user prompt missing %q:\n%s", want, user)
		}
	}
}

func stepWriteByPhase(t *testing.T, writes []map[string]any, phase string) map[string]any {
	t.Helper()
	for _, write := range writes {
		if strings.EqualFold(strings.TrimSpace(rpcString(write, "phase")), strings.TrimSpace(phase)) {
			return write
		}
	}
	t.Fatalf("missing step write for phase %q in %+v", phase, writes)
	return nil
}

func rpcIntValue(t *testing.T, values map[string]any, key string) int {
	t.Helper()
	raw, ok := values[key]
	if !ok {
		t.Fatalf("missing numeric field %q in %+v", key, values)
	}
	switch got := raw.(type) {
	case int:
		return got
	case int32:
		return int(got)
	case int64:
		return int(got)
	case float64:
		return int(got)
	default:
		t.Fatalf("field %q has type %T, want numeric", key, raw)
		return 0
	}
}
