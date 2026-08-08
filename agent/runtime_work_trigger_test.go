package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPendingAuthorityTransitionSurvivesResumeWake(t *testing.T) {
	runtime := &Runtime{
		cfg:              RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		eventWakePlanner: make(chan struct{}, 4),
		scratch:          RuntimeScratchState{DocSHAs: map[string]string{}},
	}

	if err := runtime.setPendingWorkTrigger(context.Background(), "runtime_switch_task", "task-role-scope-beta", ""); err != nil {
		t.Fatalf("set authority trigger: %v", err)
	}
	if err := runtime.setPendingWorkTrigger(context.Background(), "request_resume", "root-clearpress", "session-root"); err != nil {
		t.Fatalf("set root resume trigger: %v", err)
	}

	got := runtime.currentPendingWorkTrigger()
	if got.Trigger != "runtime_switch_task" || got.TaskID != "task-role-scope-beta" || got.SessionID != "" {
		t.Fatalf("authority transition should survive root resume wake, got %+v", got)
	}
}

func TestPendingProjectClaimRepairTransitionSurvivesResumeWake(t *testing.T) {
	runtime := &Runtime{
		cfg:              RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		eventWakePlanner: make(chan struct{}, 4),
		scratch:          RuntimeScratchState{DocSHAs: map[string]string{}},
	}

	if err := runtime.setPendingWorkTrigger(context.Background(), "runtime_switch_task", "task-project-claim-repair-abc123", ""); err != nil {
		t.Fatalf("set claim repair authority trigger: %v", err)
	}
	if err := runtime.setPendingWorkTrigger(context.Background(), "request_resume", "root-clearpress", "session-root"); err != nil {
		t.Fatalf("set root resume trigger: %v", err)
	}

	got := runtime.currentPendingWorkTrigger()
	if got.Trigger != "runtime_switch_task" || got.TaskID != "task-project-claim-repair-abc123" || got.SessionID != "" {
		t.Fatalf("claim repair transition should survive root resume wake, got %+v", got)
	}
}

func TestPendingAuthorityTransitionPreemptsActiveRootToolCalls(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		scratch: RuntimeScratchState{
			DocSHAs:            map[string]string{},
			ActiveTaskID:       "task-clearpress-root",
			ActiveSessionID:    "session-root",
			PendingTrigger:     "runtime_switch_task",
			PendingTriggerTask: "task-project-claim-repair-abc123",
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:      "task-clearpress-root",
			ProjectID:   "project-clearpress",
			ProjectLane: "strategy",
			TaskKind:    "COORDINATION",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-root",
			TaskID:    "task-clearpress-root",
			Status:    "ACTIVE",
		},
	}

	result := runtime.taskCycleToolExecutor(context.Background(), NewToolRegistry(), ToolCall{
		ID: "call-agent-request",
		Function: FunctionCall{
			Name:      "agent_request",
			Arguments: `{"to_agent_id":"kappa","prompt":"please validate the sidecar"}`,
		},
	})

	if !result.IsError || !strings.Contains(result.Output, authorityPreemptionYieldMarker) {
		t.Fatalf("pending authority transition should block active root tool calls, got %+v", result)
	}
	if !strings.Contains(result.Output, "task-project-claim-repair-abc123") || !strings.Contains(result.Output, "task-clearpress-root") {
		t.Fatalf("preemption output should name pending and active task, got %q", result.Output)
	}
}

func TestAuthorityPreemptionYieldResultForcesContinue(t *testing.T) {
	trace := &TaskRunTrace{ToolReceipts: []TaskRunToolReceipt{{
		ToolName: "agent_request",
		IsError:  true,
		Output:   authorityPreemptionYieldMarker + ": blocked tool agent_request",
	}}}
	result := authorityPreemptionYieldResult(WorkspaceTaskRecord{TaskID: "task-clearpress-root"}, StructuredTaskResult{
		Outcome: "failed",
		Summary: "agent_request failed",
	}, trace)

	if normalizeOutcome(result.Outcome) != "continue" {
		t.Fatalf("authority preemption should yield current cycle instead of failing root, got %+v", result)
	}
	if !strings.Contains(result.NextAction, "pending authority transition") {
		t.Fatalf("yield result should route to pending authority transition, got %+v", result)
	}
}

func TestPendingAuthorityTransitionDoesNotPreemptMaterializedAuthorityTask(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		scratch: RuntimeScratchState{
			DocSHAs:            map[string]string{},
			ActiveTaskID:       "task-project-claim-repair-abc123",
			ActiveSessionID:    "session-repair",
			PendingTrigger:     "runtime_switch_task",
			PendingTriggerTask: "task-project-claim-repair-abc123",
		},
		activeTask: &WorkspaceTaskRecord{TaskID: "task-project-claim-repair-abc123"},
		activeSession: &AgentSessionStateRecord{
			SessionID: "session-repair",
			TaskID:    "task-project-claim-repair-abc123",
			Status:    "ACTIVE",
		},
	}

	if trigger, activeTaskID, ok := runtime.pendingAuthorityTransitionPreemptsActiveTask(); ok {
		t.Fatalf("materialized authority task should not preempt itself, trigger=%+v active=%s", trigger, activeTaskID)
	}
}

func TestConsumedProjectClaimRepairTriggerStateClearsAfterMaterializedClaim(t *testing.T) {
	state := RuntimeScratchState{
		ActiveTaskID:       "task-project-claim-repair-abc123",
		ActiveSessionID:    "session-repair",
		PendingTrigger:     "runtime_switch_task",
		PendingTriggerTask: "task-project-claim-repair-abc123",
		LastWakeTrigger:    "request_resume",
	}
	cleared := clearConsumedPendingTriggerInState(state)
	if cleared.PendingTrigger != "" || cleared.PendingTriggerTask != "" || cleared.PendingTriggerSession != "" {
		t.Fatalf("materialized claim repair task/session should consume pending switch, got %+v", cleared)
	}
}

func TestPendingAuthorityTransitionSurvivesLocalResumeWake(t *testing.T) {
	runtime := &Runtime{
		cfg:              RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		eventWakePlanner: make(chan struct{}, 4),
		scratch:          RuntimeScratchState{DocSHAs: map[string]string{}},
	}

	if !runtime.setPendingWorkTriggerLocal("runtime_switch_task", "task-role-scope-beta", "") {
		t.Fatal("expected local authority trigger to queue")
	}
	if !runtime.setPendingWorkTriggerLocal("request_resume", "root-clearpress", "session-root") {
		t.Fatal("expected local resume wake to be accepted without replacing authority trigger")
	}

	got := runtime.currentPendingWorkTrigger()
	if got.Trigger != "runtime_switch_task" || got.TaskID != "task-role-scope-beta" || got.SessionID != "" {
		t.Fatalf("local authority transition should survive root resume wake, got %+v", got)
	}
}

func TestOrdinaryRuntimeSwitchCanYieldToResumeWake(t *testing.T) {
	runtime := &Runtime{
		cfg:              RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		eventWakePlanner: make(chan struct{}, 4),
		scratch:          RuntimeScratchState{DocSHAs: map[string]string{}},
	}

	if err := runtime.setPendingWorkTrigger(context.Background(), "runtime_switch_task", "task-generic-followup", ""); err != nil {
		t.Fatalf("set generic switch trigger: %v", err)
	}
	if err := runtime.setPendingWorkTrigger(context.Background(), "request_resume", "root-clearpress", "session-root"); err != nil {
		t.Fatalf("set root resume trigger: %v", err)
	}

	got := runtime.currentPendingWorkTrigger()
	if got.Trigger != "request_resume" || got.TaskID != "root-clearpress" || got.SessionID != "session-root" {
		t.Fatalf("ordinary runtime switch should keep normal resume semantics, got %+v", got)
	}
}

func TestSetPendingWorkTriggerRollsBackLocalScratchWhenDurableSaveFails(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.state.set":
			writeRPCError(w, req, -32603, "scratch receipt failed")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:              RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch:          RuntimeScratchState{DocSHAs: map[string]string{}},
	}

	err := runtime.setPendingWorkTrigger(context.Background(), "runtime_switch_task", "task-rollback", "")
	if err == nil {
		t.Fatal("expected durable scratch save failure")
	}
	got := runtime.currentPendingWorkTrigger()
	if got.Trigger != "" || got.TaskID != "" || got.SessionID != "" {
		t.Fatalf("failed durable scratch save must not leave local pending trigger, got %+v", got)
	}
	select {
	case <-runtime.eventWakePlanner:
		t.Fatal("failed durable scratch save must not wake planner")
	default:
	}
	if !containsAll(methods, []string{"agent.state.set"}) {
		t.Fatalf("expected durable state write attempt, got %#v", methods)
	}
}

func TestRequestResumeTriggerDropsReleasedBootstrapTask(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		t.Fatalf("stale request_resume target must be dropped before RPC write, got method %q", req.Method)
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:              RuntimeConfig{WorkspaceID: "ws", AgentID: "beta"},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch:          RuntimeScratchState{DocSHAs: map[string]string{}},
		bootstrap: BootstrapResult{Snapshot: WorkspaceSnapshot{
			Tasks: []WorkspaceTaskRecord{{
				TaskID:       "task-submit-released",
				Status:       "PENDING",
				ClaimAgentID: stringPtr("beta"),
				ClaimStatus:  stringPtr("RELEASED"),
			}},
		}},
	}

	if err := runtime.setPendingWorkTrigger(context.Background(), "request_resume", "task-submit-released", ""); err != nil {
		t.Fatalf("drop stale request_resume: %v", err)
	}
	got := runtime.currentPendingWorkTrigger()
	if got.Trigger != "" || got.TaskID != "" || got.SessionID != "" {
		t.Fatalf("released request_resume target must not persist pending trigger, got %+v", got)
	}
	select {
	case <-runtime.eventWakePlanner:
		t.Fatal("released request_resume target must not wake planner")
	default:
	}
	if len(methods) != 0 {
		t.Fatalf("released request_resume target must not write durable scratch, got methods %#v", methods)
	}
}

func TestRequestResumeTriggerAllowsRecoverableBootstrapTask(t *testing.T) {
	var persisted RuntimeScratchState
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &persisted); err != nil {
				t.Fatalf("decode scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:              RuntimeConfig{WorkspaceID: "ws", AgentID: "beta"},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch:          RuntimeScratchState{DocSHAs: map[string]string{}},
		bootstrap: BootstrapResult{Snapshot: WorkspaceSnapshot{
			Tasks: []WorkspaceTaskRecord{{
				TaskID:       "task-product-claim",
				Status:       "RUNNING",
				ClaimAgentID: stringPtr("beta"),
				ClaimStatus:  stringPtr("CLAIMED"),
			}},
			Sessions: []AgentSessionStateRecord{{
				SessionID: "session-product-claim",
				TaskID:    "task-product-claim",
				AgentID:   "beta",
				Status:    "ACTIVE",
			}},
		}},
	}

	if err := runtime.setPendingWorkTrigger(context.Background(), "request_resume", "task-product-claim", "session-product-claim"); err != nil {
		t.Fatalf("persist recoverable request_resume: %v", err)
	}
	if persisted.PendingTrigger != "request_resume" || persisted.PendingTriggerTask != "task-product-claim" || persisted.PendingTriggerSession != "session-product-claim" {
		t.Fatalf("recoverable request_resume should persist exact target, got %+v", persisted)
	}
	got := runtime.currentPendingWorkTrigger()
	if got.Trigger != "request_resume" || got.TaskID != "task-product-claim" || got.SessionID != "session-product-claim" {
		t.Fatalf("recoverable request_resume should remain local, got %+v", got)
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("recoverable request_resume should wake planner")
	}
	if !containsAll(methods, []string{"agent.state.set"}) {
		t.Fatalf("expected durable state write, got %#v", methods)
	}
}

func TestRequestResumeTriggerAllowsBlockedBootstrapSessionWithoutClaim(t *testing.T) {
	var persisted RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &persisted); err != nil {
				t.Fatalf("decode scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:              RuntimeConfig{WorkspaceID: "ws", AgentID: "beta"},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 1),
		scratch:          RuntimeScratchState{DocSHAs: map[string]string{}},
		bootstrap: BootstrapResult{Snapshot: WorkspaceSnapshot{
			Tasks: []WorkspaceTaskRecord{{
				TaskID: "task-doc-blocked",
				Status: "PENDING",
			}},
			Sessions: []AgentSessionStateRecord{{
				SessionID: "session-doc-blocked",
				TaskID:    "task-doc-blocked",
				AgentID:   "beta",
				Status:    "BLOCKED",
			}},
		}},
	}

	if err := runtime.setPendingWorkTrigger(context.Background(), "request_resume", "task-doc-blocked", "session-doc-blocked"); err != nil {
		t.Fatalf("persist blocked-session request_resume: %v", err)
	}
	if persisted.PendingTrigger != "request_resume" || persisted.PendingTriggerTask != "task-doc-blocked" || persisted.PendingTriggerSession != "session-doc-blocked" {
		t.Fatalf("blocked bootstrap session should persist request_resume, got %+v", persisted)
	}
	select {
	case <-runtime.eventWakePlanner:
	default:
		t.Fatal("blocked bootstrap session should wake planner")
	}
}

func TestPendingAuthorityTransitionCanBeReplacedByAnotherAuthorityTransition(t *testing.T) {
	runtime := &Runtime{
		cfg:              RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		eventWakePlanner: make(chan struct{}, 4),
		scratch:          RuntimeScratchState{DocSHAs: map[string]string{}},
	}

	if err := runtime.setPendingWorkTrigger(context.Background(), "runtime_switch_task", "task-role-scope-beta", ""); err != nil {
		t.Fatalf("set first authority trigger: %v", err)
	}
	if err := runtime.setPendingWorkTrigger(context.Background(), "runtime_switch_task", "task-role-scope-gamma", ""); err != nil {
		t.Fatalf("set replacement authority trigger: %v", err)
	}

	got := runtime.currentPendingWorkTrigger()
	if got.Trigger != "runtime_switch_task" || got.TaskID != "task-role-scope-gamma" || got.SessionID != "" {
		t.Fatalf("same-priority authority transition should replace previous authority target, got %+v", got)
	}
}

func TestConsumedPendingTriggerStateClearsBeforePersistence(t *testing.T) {
	state := RuntimeScratchState{
		ActiveTaskID:       "task-role-scope-beta",
		ActiveSessionID:    "session-authority",
		PendingTrigger:     "runtime_switch_task",
		PendingTriggerTask: "task-role-scope-beta",
		LastWakeTrigger:    "runtime_switch_task",
	}
	cleared := clearConsumedPendingTriggerInState(state)
	if cleared.PendingTrigger != "" || cleared.PendingTriggerTask != "" || cleared.PendingTriggerSession != "" {
		t.Fatalf("consumed pending trigger should clear before persistence, got %+v", cleared)
	}
}

func TestConsumedPendingTriggerStateClearsAfterMaterializedClaimWithResumeWake(t *testing.T) {
	state := RuntimeScratchState{
		ActiveTaskID:       "task-role-scope-beta",
		ActiveSessionID:    "session-authority",
		PendingTrigger:     "runtime_switch_task",
		PendingTriggerTask: "task-role-scope-beta",
		LastWakeTrigger:    "request_resume",
	}
	cleared := clearConsumedPendingTriggerInState(state)
	if cleared.PendingTrigger != "" || cleared.PendingTriggerTask != "" || cleared.PendingTriggerSession != "" {
		t.Fatalf("materialized active task/session should consume pending switch despite resume wake, got %+v", cleared)
	}
}

func TestConsumedPendingTriggerStateDoesNotClearOrdinarySameTaskWake(t *testing.T) {
	state := RuntimeScratchState{
		ActiveTaskID:       "task-role-scope-beta",
		ActiveSessionID:    "session-authority",
		PendingTrigger:     "request_resume",
		PendingTriggerTask: "task-role-scope-beta",
		LastWakeTrigger:    "runtime_switch_task",
	}
	cleared := clearConsumedPendingTriggerInState(state)
	if cleared.PendingTrigger != "request_resume" || cleared.PendingTriggerTask != "task-role-scope-beta" {
		t.Fatalf("active session receipt shortcut must not eat ordinary same-task wakes, got %+v", cleared)
	}
}

func TestConsumedPendingTriggerStateClearsGenericSwitchAfterMaterializedSession(t *testing.T) {
	state := RuntimeScratchState{
		ActiveTaskID:       "task-generic-followup",
		ActiveSessionID:    "session-generic",
		PendingTrigger:     "runtime_switch_task",
		PendingTriggerTask: "task-generic-followup",
		LastWakeTrigger:    "request_resume",
	}
	cleared := clearConsumedPendingTriggerInState(state)
	if cleared.PendingTrigger != "" || cleared.PendingTriggerTask != "" || cleared.PendingTriggerSession != "" {
		t.Fatalf("materialized delegated task/session should consume pending switch, got %+v", cleared)
	}
}

func TestStateSetNormalizesConsumedRuntimeScratchState(t *testing.T) {
	var persisted RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			var state RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &state); err != nil {
				t.Fatalf("decode scratch: %v", err)
			}
			persisted = state
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	state := RuntimeScratchState{
		ActiveTaskID:       "task-generic-followup",
		ActiveSessionID:    "session-generic",
		PendingTrigger:     "runtime_switch_task",
		PendingTriggerTask: "task-generic-followup",
		LastWakeTrigger:    "runtime_switch_task",
		DocSHAs:            map[string]string{},
	}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("encode scratch: %v", err)
	}

	client := NewRhizomeClient(server.URL, "token")
	if err := client.StateSet(context.Background(), "ws", "alpha", runtimeScratchStateKey, string(raw)); err != nil {
		t.Fatalf("state set: %v", err)
	}
	if persisted.PendingTrigger != "" || persisted.PendingTriggerTask != "" || persisted.PendingTriggerSession != "" {
		t.Fatalf("StateSet must not persist consumed runtime switch trigger, got %+v", persisted)
	}
}

func TestConsumedPendingTriggerStateDoesNotClearMismatchedPendingSession(t *testing.T) {
	state := RuntimeScratchState{
		ActiveTaskID:          "task-role-scope-beta",
		ActiveSessionID:       "session-authority",
		PendingTrigger:        "runtime_switch_task",
		PendingTriggerTask:    "task-role-scope-beta",
		PendingTriggerSession: "session-newer-switch",
		LastWakeTrigger:       "request_resume",
	}
	cleared := clearConsumedPendingTriggerInState(state)
	if cleared.PendingTrigger != "runtime_switch_task" || cleared.PendingTriggerTask != "task-role-scope-beta" || cleared.PendingTriggerSession != "session-newer-switch" {
		t.Fatalf("mismatched pending session should survive active-session receipt shortcut, got %+v", cleared)
	}
}

func TestConsumedPendingTriggerMergeDoesNotResurrectStaleIntent(t *testing.T) {
	var persisted RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			var state RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &state); err != nil {
				t.Fatalf("decode scratch: %v", err)
			}
			persisted = state
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs:            map[string]string{},
			PendingTrigger:     "runtime_switch_task",
			PendingTriggerTask: "task-role-scope-beta",
		},
	}
	staleDocWriteState := RuntimeScratchState{
		DocSHAs:         map[string]string{"agent.alpha.current_context": "sha"},
		ActiveTaskID:    "task-role-scope-beta",
		ActiveSessionID: "session-authority",
		LastWakeTrigger: "request_resume",
	}

	if err := runtime.saveScratchState(context.Background(), staleDocWriteState); err != nil {
		t.Fatalf("save scratch: %v", err)
	}
	if persisted.PendingTrigger != "" || persisted.PendingTriggerTask != "" || persisted.PendingTriggerSession != "" {
		t.Fatalf("stale doc-sha save must not resurrect consumed trigger, got %+v", persisted)
	}
	if got := runtime.currentPendingWorkTrigger(); got.Trigger != "" || got.TaskID != "" || got.SessionID != "" {
		t.Fatalf("consumed trigger should clear local scratch too, got %+v", got)
	}
}

func TestConsumedTriggerMergePreservesNewerSameTaskResumeSignal(t *testing.T) {
	var persisted RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			var state RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &state); err != nil {
				t.Fatalf("decode scratch: %v", err)
			}
			persisted = state
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs:            map[string]string{},
			PendingTrigger:     "request_resume",
			PendingTriggerTask: "task-role-scope-beta",
			PendingTriggerAt:   "2026-05-23T12:00:02Z",
		},
	}
	staleDocWriteState := RuntimeScratchState{
		DocSHAs:         map[string]string{"agent.alpha.current_context": "sha"},
		ActiveTaskID:    "task-role-scope-beta",
		ActiveSessionID: "session-authority",
		LastWakeTrigger: "runtime_switch_task",
	}

	if err := runtime.saveScratchState(context.Background(), staleDocWriteState); err != nil {
		t.Fatalf("save scratch: %v", err)
	}
	if persisted.PendingTrigger != "request_resume" || persisted.PendingTriggerTask != "task-role-scope-beta" {
		t.Fatalf("newer same-task resume signal should survive consumed-switch merge, got %+v", persisted)
	}
	if got := runtime.currentPendingWorkTrigger(); got.Trigger != "request_resume" || got.TaskID != "task-role-scope-beta" {
		t.Fatalf("newer same-task resume signal should remain local too, got %+v", got)
	}
}

func TestConsumedTriggerFlushDoesNotClearNewDifferentSignal(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		scratch: RuntimeScratchState{
			DocSHAs:            map[string]string{},
			PendingTrigger:     "runtime_switch_task",
			PendingTriggerTask: "task-role-scope-gamma",
		},
	}

	if err := runtime.clearConsumedPendingWorkTrigger(context.Background(), pendingWorkTrigger{Trigger: "runtime_switch_task", TaskID: "task-role-scope-beta"}, "task-role-scope-beta"); err != nil {
		t.Fatalf("clear consumed pending trigger: %v", err)
	}
	got := runtime.currentPendingWorkTrigger()
	if got.Trigger != "runtime_switch_task" || got.TaskID != "task-role-scope-gamma" {
		t.Fatalf("new different authority signal should survive consumed-A flush, got %+v", got)
	}
}

func TestPendingAuthorityTransitionClearsOnTerminalSchedulerOutcome(t *testing.T) {
	var lastScratch RuntimeScratchState
	var postedBlocker bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.work.next":
			if got := rpcString(req.Params, "trigger"); got != "runtime_switch_task" {
				t.Fatalf("expected authority switch trigger, got %q", got)
			}
			if got := rpcString(req.Params, "candidate_task_id"); got != "task-role-scope-beta" {
				t.Fatalf("expected authority candidate, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{
				"generated_at": "2026-05-23T12:00:00Z",
				"workspace_id": "ws",
				"agent_id":     "alpha",
				"has_work":     false,
				"reason":       "trigger_task_terminal",
			})
		case "agent.update.post":
			postedBlocker = true
			writeRPCResult(w, req, map[string]any{"update_id": "upd-terminal"})
		case "agent.state.set":
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &lastScratch); err != nil {
				t.Fatalf("decode scratch: %v", err)
			}
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "alpha",
		},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs:               map[string]string{},
			PendingTrigger:        "runtime_switch_task",
			PendingTriggerTask:    "task-role-scope-beta",
			PendingTriggerSession: "",
		},
	}

	task, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if task != nil {
		t.Fatalf("terminal scheduler outcome should not select work, got %+v", task)
	}
	got := runtime.currentPendingWorkTrigger()
	if got.Trigger != "" || got.TaskID != "" || got.SessionID != "" {
		t.Fatalf("terminal authority outcome should clear pending trigger, got %+v", got)
	}
	if lastScratch.PendingTrigger != "" || lastScratch.PendingTriggerTask != "" || lastScratch.PendingTriggerSession != "" {
		t.Fatalf("terminal authority outcome should persist cleared trigger, got %+v", lastScratch)
	}
	if !postedBlocker {
		t.Fatal("expected durable not-claimed blocker update for terminal authority switch")
	}
}

func TestStartupResumeWakePreservesAuthorityTransition(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		scratch: RuntimeScratchState{
			DocSHAs:               map[string]string{},
			ActiveTaskID:          "root-clearpress",
			ActiveSessionID:       "session-root",
			PendingTrigger:        "runtime_switch_task",
			PendingTriggerTask:    "task-role-scope-beta",
			PendingTriggerSession: "",
		},
	}

	if !runtime.prepareStartupPlannerWakeLocked(time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)) {
		t.Fatal("expected startup planner wake")
	}
	got := runtime.currentPendingWorkTrigger()
	if got.Trigger != "runtime_switch_task" || got.TaskID != "task-role-scope-beta" || got.SessionID != "" {
		t.Fatalf("startup root resume should preserve authority transition, got %+v", got)
	}
}

func TestCompletionCoordinationDoesNotRetargetAuthorityTransition(t *testing.T) {
	runtime := &Runtime{
		cfg: RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		scratch: RuntimeScratchState{
			DocSHAs:               map[string]string{},
			PendingTrigger:        "runtime_switch_task",
			PendingTriggerTask:    "task-role-scope-beta",
			PendingTriggerSession: "",
		},
	}

	task := WorkspaceTaskRecord{TaskID: "root-clearpress"}
	session := AgentSessionStateRecord{SessionID: "session-root", TaskID: "root-clearpress"}
	if err := runtime.setCompletionCoordinationScratch(context.Background(), task, session, "run-root", "beta", "areq-1", completionCoordinationStateReviewReady, "ready", "advisory"); err != nil {
		t.Fatalf("set completion coordination scratch: %v", err)
	}

	got := runtime.currentPendingWorkTrigger()
	if got.Trigger != "runtime_switch_task" || got.TaskID != "task-role-scope-beta" || got.SessionID != "" {
		t.Fatalf("completion coordination should preserve authority transition target, got %+v", got)
	}
}

func TestAuthorityTransitionReplayClaimsAfterWakeBurst(t *testing.T) {
	var workNextCalls int
	var claimTaskID string
	var startedSessionTaskID string
	var lastScratch RuntimeScratchState
	var savedStates []RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.work.next":
			workNextCalls++
			if got := rpcString(req.Params, "trigger"); got != "runtime_switch_task" {
				t.Fatalf("expected authority switch trigger, got %q", got)
			}
			if got := rpcString(req.Params, "candidate_task_id"); got != "task-role-scope-beta" {
				t.Fatalf("expected authority candidate, got %q", got)
			}
			writeRPCResult(w, req, authorityTransitionWorkNextResult("alpha", "task-role-scope-beta"))
		case "agent.task.claim":
			claimTaskID = rpcString(req.Params, "task_id")
			writeRPCResult(w, req, nil)
		case "agent.session.start":
			startedSessionTaskID = rpcString(req.Params, "task_id")
			writeRPCResult(w, req, map[string]any{"state": map[string]any{
				"session_id":          rpcString(req.Params, "session_id"),
				"workspace_id":        "ws",
				"agent_id":            "alpha",
				"task_id":             rpcString(req.Params, "task_id"),
				"status":              "ACTIVE",
				"summary":             rpcString(req.Params, "summary"),
				"updated_at":          "2026-05-23T12:00:01Z",
				"started_at":          "2026-05-23T12:00:01Z",
				"keep_session_active": true,
			}})
		case "workspace.execution.run.write":
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": "run-authority"}})
		case "agent.state.set":
			var state RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &state); err != nil {
				t.Fatalf("decode scratch: %v", err)
			}
			lastScratch = state
			savedStates = append(savedStates, lastScratch)
			writeRPCResult(w, req, nil)
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-authority"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "alpha",
		},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 8),
		scratch: RuntimeScratchState{
			DocSHAs:               map[string]string{},
			ActiveTaskID:          "root-clearpress",
			ActiveSessionID:       "session-root",
			PendingTrigger:        "runtime_switch_task",
			PendingTriggerTask:    "task-role-scope-beta",
			PendingTriggerSession: "",
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:       "root-clearpress",
			Title:        "Clearpress root",
			Status:       "RUNNING",
			TaskKind:     "COORDINATION",
			TaskTemplate: "project",
			ProjectID:    "project-clearpress",
			ProjectLane:  "strategy",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID:   "session-root",
			WorkspaceID: "ws",
			AgentID:     "alpha",
			TaskID:      "root-clearpress",
			Status:      "ACTIVE",
		},
	}

	if err := runtime.setPendingWorkTrigger(context.Background(), "request_resume", "root-clearpress", "session-root"); err != nil {
		t.Fatalf("queue root resume burst: %v", err)
	}
	if err := runtime.queueSystemNewsTrigger(context.Background()); err != nil {
		t.Fatalf("queue system news burst: %v", err)
	}
	if !runtime.prepareStartupPlannerWakeLocked(time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)) {
		t.Fatal("expected startup planner wake")
	}
	rootTask := WorkspaceTaskRecord{TaskID: "root-clearpress"}
	rootSession := AgentSessionStateRecord{SessionID: "session-root", TaskID: "root-clearpress"}
	if err := runtime.setCompletionCoordinationScratch(context.Background(), rootTask, rootSession, "run-root", "beta", "areq-1", completionCoordinationStateReviewReady, "ready", "advisory"); err != nil {
		t.Fatalf("set completion coordination scratch: %v", err)
	}

	task, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if task == nil || task.TaskID != "task-role-scope-beta" {
		t.Fatalf("authority replay should select role/scope task, got %+v", task)
	}
	if workNextCalls != 1 {
		t.Fatalf("expected one work.next call, got %d", workNextCalls)
	}
	if claimTaskID != "task-role-scope-beta" || startedSessionTaskID != "task-role-scope-beta" {
		t.Fatalf("authority transition should materialize claim/session, claim=%q session_task=%q", claimTaskID, startedSessionTaskID)
	}
	if runtime.activeTask == nil || runtime.activeTask.TaskID != "task-role-scope-beta" {
		t.Fatalf("active task should be authority task, got %+v", runtime.activeTask)
	}
	if runtime.activeWorkPacket == nil || runtime.activeWorkPacket.PreferredTransition != "project_role_assign" {
		t.Fatalf("authority task should carry project_role_assign packet, got %+v", runtime.activeWorkPacket)
	}
	if got := runtime.currentPendingWorkTrigger(); got.Trigger != "" || got.TaskID != "" || got.SessionID != "" {
		t.Fatalf("authority trigger should clear locally after durable claim/session, got %+v", got)
	}
	if lastScratch.PendingTrigger != "" || lastScratch.PendingTriggerTask != "" || lastScratch.PendingTriggerSession != "" {
		t.Fatalf("authority trigger should clear after durable claim/session, got %+v saved_states=%d trace=%s", lastScratch, len(savedStates), compactScratchTriggerTrace(savedStates))
	}
}

func TestActiveRootPollsWorkNextForStrategicAuthorityRepair(t *testing.T) {
	var workNextCalls int
	var claimTaskID string
	var startedSessionTaskID string
	var savedStates []RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.state.set":
			var state RuntimeScratchState
			if err := json.Unmarshal([]byte(rpcString(req.Params, "value")), &state); err != nil {
				t.Fatalf("decode scratch: %v", err)
			}
			savedStates = append(savedStates, state)
			writeRPCResult(w, req, nil)
		case "agent.work.next":
			workNextCalls++
			if got := rpcString(req.Params, "trigger"); got != "" {
				t.Fatalf("active root strategic poll should not forge a trigger, got %q", got)
			}
			writeRPCResult(w, req, authorityTransitionWorkNextResult("alpha", "task-role-scope-beta"))
		case "agent.task.claim":
			claimTaskID = rpcString(req.Params, "task_id")
			writeRPCResult(w, req, nil)
		case "agent.session.start":
			startedSessionTaskID = rpcString(req.Params, "task_id")
			writeRPCResult(w, req, map[string]any{"state": map[string]any{
				"session_id":          rpcString(req.Params, "session_id"),
				"workspace_id":        "ws",
				"agent_id":            "alpha",
				"task_id":             rpcString(req.Params, "task_id"),
				"status":              "ACTIVE",
				"summary":             rpcString(req.Params, "summary"),
				"updated_at":          "2026-05-25T00:00:01Z",
				"started_at":          "2026-05-25T00:00:01Z",
				"keep_session_active": true,
			}})
		case "workspace.execution.run.write":
			writeRPCResult(w, req, map[string]any{"run": map[string]any{"run_id": "run-authority"}})
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-authority"})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:    RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		client: NewRhizomeClient(server.URL, "token"),
		scratch: RuntimeScratchState{
			DocSHAs:         map[string]string{},
			ActiveTaskID:    "root-clearpress",
			ActiveSessionID: "session-root",
		},
		activeTask: &WorkspaceTaskRecord{
			TaskID:       "root-clearpress",
			Title:        "Clearpress project root",
			Status:       "RUNNING",
			TaskKind:     "COORDINATION",
			TaskTemplate: "project_root",
			ProjectID:    "project-clearpress",
			ProjectLane:  "strategy",
			OwnerUserID:  "owner",
		},
		activeSession: &AgentSessionStateRecord{
			SessionID:   "session-root",
			WorkspaceID: "ws",
			AgentID:     "alpha",
			TaskID:      "root-clearpress",
			Status:      "ACTIVE",
		},
	}

	task, err := runtime.ensureRunnableTask(context.Background())
	if err != nil {
		t.Fatalf("ensureRunnableTask() error = %v", err)
	}
	if task == nil || task.TaskID != "task-role-scope-beta" {
		t.Fatalf("active root should yield to strategic authority repair poll, got %+v", task)
	}
	if workNextCalls != 1 {
		t.Fatalf("expected one work.next poll, got %d", workNextCalls)
	}
	if claimTaskID != "task-role-scope-beta" || startedSessionTaskID != "task-role-scope-beta" {
		t.Fatalf("authority repair should materialize claim/session, claim=%q session_task=%q", claimTaskID, startedSessionTaskID)
	}
	if len(savedStates) == 0 || strings.TrimSpace(savedStates[0].StrategicRepairPollAt) == "" {
		t.Fatalf("expected strategic repair poll timestamp to be persisted before work.next, states=%+v", savedStates)
	}
}

func TestActiveRootStrategicRepairPollHonorsInterval(t *testing.T) {
	now := time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC)
	root := WorkspaceTaskRecord{
		TaskID:       "root-clearpress",
		Title:        "Clearpress project root",
		TaskTemplate: "project_root",
		ProjectID:    "project-clearpress",
		ProjectLane:  "strategy",
	}
	if activeRootShouldPollStrategicRepair(root, RuntimeScratchState{}, now) != true {
		t.Fatal("root strategy task should poll when no prior poll exists")
	}
	recent := RuntimeScratchState{StrategicRepairPollAt: now.Add(-30 * time.Second).Format(time.RFC3339Nano)}
	if activeRootShouldPollStrategicRepair(root, recent, now) {
		t.Fatal("recent strategic repair poll should suppress immediate repeat")
	}
	stale := RuntimeScratchState{StrategicRepairPollAt: now.Add(-3 * time.Minute).Format(time.RFC3339Nano)}
	if !activeRootShouldPollStrategicRepair(root, stale, now) {
		t.Fatal("stale strategic repair poll should allow work.next refresh")
	}
	implementation := root
	implementation.ProjectLane = "implementation"
	if activeRootShouldPollStrategicRepair(implementation, RuntimeScratchState{}, now) {
		t.Fatal("implementation lane must not use root strategic repair polling")
	}
}

func TestActiveRootStrategicRepairPollRecognizesTaskRootID(t *testing.T) {
	now := time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC)
	task := WorkspaceTaskRecord{
		TaskID:       "task-clearpress-root-20260523-211139",
		Title:        "Clearpress autonomous product MVP run",
		TaskTemplate: "generic",
		TaskKind:     "COORDINATION",
		ProjectID:    "project-task-clearpress-root-20260523-211139",
		ProjectLane:  "strategy",
	}
	if !activeRootShouldPollStrategicRepair(task, RuntimeScratchState{}, now) {
		t.Fatal("production-shaped task-*-root-* strategy task should poll for strategic repair")
	}
}

func TestPendingAuthorityTransitionSurvivesClaimMaterializationFailure(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.work.next":
			if got := rpcString(req.Params, "trigger"); got != "runtime_switch_task" {
				t.Fatalf("expected authority switch trigger, got %q", got)
			}
			if got := rpcString(req.Params, "candidate_task_id"); got != "task-role-scope-beta" {
				t.Fatalf("expected authority candidate, got %q", got)
			}
			writeRPCResult(w, req, authorityTransitionWorkNextResult("alpha", "task-role-scope-beta"))
		case "agent.task.claim":
			writeRPCError(w, req, -32000, "storage temporarily unavailable while claiming authority task")
		case "agent.state.set":
			t.Fatalf("pending authority trigger must not be cleared before claim/session materializes")
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:              RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 4),
		scratch: RuntimeScratchState{
			DocSHAs:            map[string]string{},
			PendingTrigger:     "runtime_switch_task",
			PendingTriggerTask: "task-role-scope-beta",
		},
	}

	task, err := runtime.ensureRunnableTask(context.Background())
	if err == nil {
		t.Fatal("expected claim materialization failure to surface")
	}
	if task != nil {
		t.Fatalf("failed materialization must not return active task, got %+v", task)
	}
	got := runtime.currentPendingWorkTrigger()
	if got.Trigger != "runtime_switch_task" || got.TaskID != "task-role-scope-beta" || got.SessionID != "" {
		t.Fatalf("authority trigger should survive failed materialization, got %+v", got)
	}
	if strings.Join(methods, ",") != "agent.work.next,agent.task.claim" {
		t.Fatalf("unexpected materialization failure path: %#v", methods)
	}
}

func TestPendingAuthorityTransitionSurfacesClaimConflictMaterializationFailure(t *testing.T) {
	var methods []string
	var savedStates []RuntimeScratchState
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.work.next":
			if got := rpcString(req.Params, "trigger"); got != "runtime_switch_task" {
				t.Fatalf("expected authority switch trigger, got %q", got)
			}
			if got := rpcString(req.Params, "candidate_task_id"); got != "task-role-scope-beta" {
				t.Fatalf("expected authority candidate, got %q", got)
			}
			writeRPCResult(w, req, authorityTransitionWorkNextResult("alpha", "task-role-scope-beta"))
		case "agent.task.claim":
			writeRPCError(w, req, -32000, "task claim conflict: project claim repair task task-role-scope-beta may only be claimed by active strategic lead")
		case "agent.state.set":
			savedStates = append(savedStates, decodeRuntimeScratchStateRPC(t, req))
			writeRPCResult(w, req, nil)
		case "agent.bootstrap":
			writeRPCResult(w, req, BootstrapResult{Snapshot: WorkspaceSnapshot{Tasks: []WorkspaceTaskRecord{{
				TaskID:       "task-role-scope-beta",
				Status:       "PENDING",
				TaskKind:     "COORDINATION",
				TaskTemplate: "generic",
				ProjectID:    "project-clearpress",
				ProjectLane:  "coordination",
			}}}})
		case "agent.limits.get":
			writeRPCResult(w, req, nil)
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	runtime := &Runtime{
		cfg:              RuntimeConfig{WorkspaceID: "ws", AgentID: "alpha"},
		client:           NewRhizomeClient(server.URL, "token"),
		eventWakePlanner: make(chan struct{}, 4),
		scratch: RuntimeScratchState{
			DocSHAs:            map[string]string{},
			PendingTrigger:     "runtime_switch_task",
			PendingTriggerTask: "task-role-scope-beta",
		},
	}

	task, err := runtime.ensureRunnableTask(context.Background())
	if err == nil {
		t.Fatal("expected claim conflict materialization failure to surface")
	}
	if !strings.Contains(err.Error(), "runtime_switch_task claim_admission rejected for task task-role-scope-beta") {
		t.Fatalf("expected typed runtime_switch_task admission error, got %v", err)
	}
	if task != nil {
		t.Fatalf("failed materialization must not return active task, got %+v", task)
	}
	got := runtime.currentPendingWorkTrigger()
	if got.Trigger != "runtime_switch_task" || got.TaskID != "task-role-scope-beta" || got.SessionID != "" {
		t.Fatalf("authority trigger should survive claim-conflict materialization failure, got %+v", got)
	}
	if len(savedStates) == 0 {
		t.Fatalf("expected claim-conflict hold scratch to persist, methods=%#v", methods)
	}
	if savedStates[0].ProjectClaimHoldKind != "task_claim_conflict" || savedStates[0].LastWakeTrigger != "task_claim_conflict" {
		t.Fatalf("expected task-claim-conflict hold, got %+v", savedStates[0])
	}
}

func compactScratchTriggerTrace(states []RuntimeScratchState) string {
	parts := make([]string, 0, len(states))
	for i, state := range states {
		parts = append(parts, fmt.Sprintf("%d:%s/%s active=%s wake=%s", i, state.PendingTrigger, state.PendingTriggerTask, state.ActiveTaskID, state.LastWakeTrigger))
	}
	return strings.Join(parts, " | ")
}

func authorityTransitionWorkNextResult(agentID, taskID string) map[string]any {
	return map[string]any{
		"generated_at":   "2026-05-23T12:00:00Z",
		"workspace_id":   "ws",
		"agent_id":       agentID,
		"has_work":       true,
		"reason":         "triggered_task",
		"trigger":        "runtime_switch_task",
		"claim_action":   "claim_required",
		"session_action": "start_new",
		"task": map[string]any{
			"task_id":       taskID,
			"title":         "Resolve project role/scope request for beta",
			"description":   "# Strategic Lead Role/Scope Request\n\nRun project_role_assign.",
			"owner_user_id": "owner",
			"priority":      "HIGH",
			"status":        "PENDING",
			"task_kind":     "COORDINATION",
			"task_template": "generic",
			"project_id":    "project-clearpress",
			"project_lane":  "coordination",
			"linked_by":     "beta",
			"linked_at":     "2026-05-23T12:00:00Z",
			"tags":          []any{"project-role-scope", "strategic-lead", "coordination", "blocker-unblock"},
			"task_requirements_json": `{
				"schema":"project_role_scope_authority_transition.v1",
				"required_transition":"project_role_assign"
			}`,
		},
		"packet": map[string]any{
			"work_type":             "project_role_scope_authority_transition",
			"claim_action":          "claim_required",
			"session_action":        "start_new",
			"coordination_state":    "authority_transition",
			"preferred_transition":  "project_role_assign",
			"why_now":               "authority transition blocks an implementation lane",
			"project_id":            "project-clearpress",
			"project_lane":          "coordination",
			"requires_project_gate": false,
			"gate": map[string]any{
				"gate_state":  "open",
				"gate_type":   "project_role_scope_authority_transition",
				"needed_from": "project_role_assign",
				"summary":     "Run project_role_assign for the authority transition.",
			},
		},
	}
}
