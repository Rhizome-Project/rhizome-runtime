package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type failIfCalledLLM struct {
	t *testing.T
}

func (l failIfCalledLLM) Chat(_ context.Context, _ []Message, _ []ToolDef) (*LLMResponse, error) {
	l.t.Helper()
	l.t.Fatalf("LLM should not be called")
	return nil, errors.New("LLM should not be called")
}

func TestPinnedTelicBuildSuppressesInternalHeartbeats(t *testing.T) {
	runtime := NewRuntime(RuntimeConfig{
		Workdir:          t.TempDir(),
		AgentID:          "delta",
		Role:             "implementer",
		TelicLoopEnabled: true,
		PinnedTaskID:     "task-eval",
	}, failIfCalledLLM{t: t})
	t.Cleanup(func() { _ = runtime.Close() })

	results, err := runtime.executeDueInternalHeartbeatsCoordinated(context.Background(), time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("executeDueInternalHeartbeatsCoordinated() error = %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("pinned telic build should suppress heartbeat execution, got %+v", results)
	}
}

func TestInternalHeartbeatDueQueueOrdersByDuePriorityAndID(t *testing.T) {
	now := time.Date(2026, 5, 14, 10, 0, 0, 0, time.UTC)
	anatomy := testHeartbeatAnatomy([]AgentHeartbeatSpec{
		{ID: "later", Kind: "metacognition", Cadence: "every_10m", Priority: 10, Locks: []string{"local_only"}, ToolSuites: []string{"memory_and_docs_read"}},
		{ID: "first", Kind: "metacognition", Cadence: "every_10m", Priority: 30, Locks: []string{"read_only_artifact"}, ToolSuites: []string{"local_log_read"}},
		{ID: "claimed", Kind: "task_execution", Cadence: heartbeatCadenceWhileClaimed, Priority: 100, Locks: []string{"exclusive_task_mutation"}, ToolSuites: []string{"task_authority"}},
	})
	queue := internalHeartbeatDueQueue(anatomy, now, nil, false)
	if len(queue) != 2 {
		t.Fatalf("due queue len = %d, want 2: %+v", len(queue), queue)
	}
	if queue[0].ID != "first" || queue[1].ID != "later" {
		t.Fatalf("unexpected due order: %+v", queue)
	}
}

func TestInternalHeartbeatCadenceCooldownAndWhileClaimed(t *testing.T) {
	now := time.Date(2026, 5, 14, 10, 0, 0, 0, time.UTC)
	anatomy := testHeartbeatAnatomy([]AgentHeartbeatSpec{
		{ID: "cooling", Kind: "metacognition", Cadence: "every_10m", Priority: 20, Locks: []string{"local_only"}, ToolSuites: []string{"memory_and_docs_read"}},
		{ID: "elapsed", Kind: "metacognition", Cadence: "every_10m", Priority: 10, Locks: []string{"read_only_artifact"}, ToolSuites: []string{"local_log_read"}},
		{ID: "claimed", Kind: "task_execution", Cadence: heartbeatCadenceWhileClaimed, Priority: 100, Locks: []string{"exclusive_task_mutation"}, ToolSuites: []string{"task_authority"}},
	})
	last := map[string]time.Time{
		"cooling": now.Add(-5 * time.Minute),
		"elapsed": now.Add(-11 * time.Minute),
	}
	items := internalHeartbeatDueItems(anatomy, now, last, false)
	assertHeartbeatState(t, items, "cooling", false, "cooldown")
	assertHeartbeatState(t, items, "elapsed", true, "cadence_elapsed")
	assertHeartbeatState(t, items, "claimed", false, "waiting_for_claimed_task")

	items = internalHeartbeatDueItems(anatomy, now, last, true)
	assertHeartbeatState(t, items, "claimed", true, "active_task_present")
}

func TestInternalHeartbeatSchedulerSkipsPausedConcurrencyAndLocks(t *testing.T) {
	now := time.Date(2026, 5, 14, 10, 0, 0, 0, time.UTC)
	anatomy := testHeartbeatAnatomy([]AgentHeartbeatSpec{
		{ID: "running", Kind: "metacognition", Cadence: "every_5m", Priority: 30, Locks: []string{"local_only"}, ToolSuites: []string{"memory_and_docs_read"}, MaxParallel: 1},
		{ID: "locked", Kind: "metacognition", Cadence: "every_5m", Priority: 20, Locks: []string{"read_only_artifact"}, ToolSuites: []string{"local_log_read"}},
	})
	state := newInternalHeartbeatSchedulerState()
	state.Running["running"] = 1
	state.HeldLocks["read_only_artifact"] = 1
	items := internalHeartbeatDueItemsWithState(anatomy, now, state, false, false)
	assertHeartbeatState(t, items, "running", false, "concurrency_limit")
	assertHeartbeatState(t, items, "locked", false, "lock_held")

	items = internalHeartbeatDueItemsWithState(anatomy, now, newInternalHeartbeatSchedulerState(), false, true)
	assertHeartbeatState(t, items, "running", false, "runtime_paused")
	assertHeartbeatState(t, items, "locked", false, "runtime_paused")
}

func TestInternalHeartbeatSchedulerRespectsGlobalConcurrencyLimit(t *testing.T) {
	now := time.Date(2026, 5, 14, 10, 0, 0, 0, time.UTC)
	anatomy := testHeartbeatAnatomy([]AgentHeartbeatSpec{
		{ID: "first", Kind: "metacognition", Cadence: "every_5m", Priority: 30, Locks: []string{"local_only"}, ToolSuites: []string{"memory_and_docs_read"}},
		{ID: "second", Kind: "metacognition", Cadence: "every_5m", Priority: 20, Locks: []string{"read_only_artifact"}, ToolSuites: []string{"local_log_read"}},
	})
	anatomy.Concurrency.MaxParallelInternalSessions = 1
	items := internalHeartbeatDueItemsWithState(anatomy, now, newInternalHeartbeatSchedulerState(), false, false)
	assertHeartbeatState(t, items, "first", true, "never_ran")
	assertHeartbeatState(t, items, "second", false, "global_concurrency_limit")
}

func TestRuntimeInternalHeartbeatLocalAcquireRespectsLocks(t *testing.T) {
	anatomy := testHeartbeatAnatomy([]AgentHeartbeatSpec{
		{ID: "first", Kind: "metacognition", Cadence: "every_5m", Priority: 30, Locks: []string{"shared_lock"}, ToolSuites: []string{"memory_and_docs_read"}},
		{ID: "second", Kind: "metacognition", Cadence: "every_5m", Priority: 20, Locks: []string{"shared_lock"}, ToolSuites: []string{"local_log_read"}},
	})
	runtime := &Runtime{}
	first := normalizeAgentHeartbeatSpec(anatomy.Heartbeats[0])
	second := normalizeAgentHeartbeatSpec(anatomy.Heartbeats[1])
	if !runtime.tryAcquireInternalHeartbeatRun(anatomy, first) {
		t.Fatalf("expected first heartbeat acquire")
	}
	if runtime.tryAcquireInternalHeartbeatRun(anatomy, second) {
		t.Fatalf("expected second heartbeat to be blocked by held lock")
	}
	runtime.releaseInternalHeartbeatRun(first)
	if !runtime.tryAcquireInternalHeartbeatRun(anatomy, second) {
		t.Fatalf("expected second heartbeat acquire after release")
	}
}

func TestRuntimeInternalHeartbeatRemoteLeaseAcquireAndRelease(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		methods = append(methods, req.Method)
		switch req.Method {
		case "agent.heartbeat_lease.acquire":
			if got := rpcString(req.Params, "workspace_id"); got != "ws-lease" {
				t.Fatalf("workspace_id = %q", got)
			}
			if got := rpcString(req.Params, "agent_id"); got != "agent-ui" {
				t.Fatalf("agent_id = %q", got)
			}
			if got := rpcString(req.Params, "heartbeat_id"); got != "visual_audit" {
				t.Fatalf("heartbeat_id = %q", got)
			}
			writeRPCResult(w, req, map[string]any{"lease": map[string]any{
				"acquired":     true,
				"workspace_id": "ws-lease",
				"agent_id":     "agent-ui",
				"heartbeat_id": "visual_audit",
				"lease_token":  rpcString(req.Params, "lease_token"),
				"locks":        []string{"browser"},
				"expires_at":   "2026-05-17T12:01:00Z",
			}})
		case "agent.heartbeat_lease.release":
			writeRPCResult(w, req, map[string]any{"released": true, "status": "RELEASED"})
		default:
			t.Fatalf("unexpected method: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-lease",
		AgentID:     "agent-ui",
		Workdir:     t.TempDir(),
		RhizomeRPC:  server.URL,
	}, nil)
	anatomy := testHeartbeatAnatomy([]AgentHeartbeatSpec{
		{ID: "visual_audit", Kind: "visual_check", Cadence: "every_5m", Locks: []string{"browser"}},
	})
	lease, ok := runtime.tryAcquireInternalHeartbeatRunLease(context.Background(), anatomy, anatomy.Heartbeats[0], true)
	if !ok || !lease.Local || !lease.Remote || lease.LeaseToken == "" {
		t.Fatalf("expected local+remote lease acquire, got lease=%+v ok=%v", lease, ok)
	}
	runtime.releaseInternalHeartbeatRunLease(context.Background(), lease)
	if len(methods) != 2 || methods[0] != "agent.heartbeat_lease.acquire" || methods[1] != "agent.heartbeat_lease.release" {
		t.Fatalf("unexpected lease methods: %#v", methods)
	}
	if len(runtime.internalHeartbeatState.Running) != 0 || len(runtime.internalHeartbeatState.HeldLocks) != 0 {
		t.Fatalf("expected local heartbeat locks released, got %+v", runtime.internalHeartbeatState)
	}
}

func TestRuntimeInternalHeartbeatRemoteLeaseConflictReleasesLocalLock(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "agent.heartbeat_lease.acquire" {
			t.Fatalf("unexpected method: %s", req.Method)
		}
		writeRPCResult(w, req, map[string]any{"lease": map[string]any{
			"acquired":              false,
			"conflict_reason":       "lock_already_leased",
			"conflict_heartbeat_id": "visual_audit",
			"conflict_lock":         "browser",
			"conflict_lease_token":  "other-token",
			"conflict_expires_at":   "2026-05-17T12:01:00Z",
		}})
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-lease",
		AgentID:     "agent-ui",
		Workdir:     t.TempDir(),
		RhizomeRPC:  server.URL,
	}, nil)
	anatomy := testHeartbeatAnatomy([]AgentHeartbeatSpec{
		{ID: "visual_audit", Kind: "visual_check", Cadence: "every_5m", Locks: []string{"browser"}},
	})
	lease, ok := runtime.tryAcquireInternalHeartbeatRunLease(context.Background(), anatomy, anatomy.Heartbeats[0], true)
	if ok || lease.Local || lease.Remote {
		t.Fatalf("expected remote conflict to abort acquire and release local lock, got lease=%+v ok=%v", lease, ok)
	}
	if !runtime.tryAcquireInternalHeartbeatRun(anatomy, anatomy.Heartbeats[0]) {
		t.Fatalf("expected local lock to be available after remote conflict")
	}
	runtime.releaseInternalHeartbeatRun(anatomy.Heartbeats[0])
}

func TestRuntimeInternalHeartbeatLocalOnlySkipsRemoteLeaseWhenEndpointFails(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	calledLease := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.heartbeat_lease.acquire", "agent.heartbeat_lease.refresh", "agent.heartbeat_lease.release":
			calledLease = true
			http.Error(w, "remote lease unavailable", http.StatusInternalServerError)
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"ok": true})
		default:
			writeRPCResult(w, req, nil)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	if err := SaveAgentProfile(workdir, DefaultAgentProfile("sigma", "Sigma", "local self checker")); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-local-only",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		Workdir:     workdir,
		RhizomeRPC:  server.URL,
	}, nil)
	results, err := runtime.executeDueInternalHeartbeatsLeased(context.Background(), time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("executeDueInternalHeartbeatsLeased() error = %v", err)
	}
	if len(results) != 1 || results[0].HeartbeatID != "loop_self_check" {
		t.Fatalf("expected local-only loop_self_check to run without remote lease, got %+v", results)
	}
	if calledLease {
		t.Fatalf("local-only heartbeat should not call remote lease endpoint")
	}
}

func TestRuntimeInternalHeartbeatLeaseRefreshLostCancelsCycle(t *testing.T) {
	refreshCalled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.heartbeat_lease.refresh":
			select {
			case <-refreshCalled:
			default:
				close(refreshCalled)
			}
			writeRPCResult(w, req, map[string]any{"lease": map[string]any{
				"acquired":        false,
				"conflict_reason": "lock_already_leased",
				"conflict_lock":   "browser",
			}})
		default:
			t.Fatalf("unexpected method: %s", req.Method)
		}
	}))
	defer server.Close()

	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-lease",
		AgentID:     "agent-ui",
		Workdir:     t.TempDir(),
		RhizomeRPC:  server.URL,
	}, nil)
	lease := internalHeartbeatRunLease{
		Spec:       normalizeAgentHeartbeatSpec(AgentHeartbeatSpec{ID: "visual_audit", Locks: []string{"browser"}}),
		Remote:     true,
		OwnerID:    "owner",
		LeaseToken: "token",
		TTLSeconds: 1,
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	stop := runtime.startInternalHeartbeatLeaseRefresh(lease, cancel)
	defer stop()
	select {
	case <-refreshCalled:
	case <-time.After(3 * time.Second):
		t.Fatalf("refresh was not called")
	}
	select {
	case <-ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatalf("cycle context was not canceled after lost lease")
	}
	if !errors.Is(context.Cause(ctx), errInternalHeartbeatLeaseLost) {
		t.Fatalf("expected lease-lost cause, got %v", context.Cause(ctx))
	}
}

func TestExecuteDueInternalHeartbeatsCoordinatedUsesConfirmedRemoteLease(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var leaseCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.heartbeat_lease.acquire":
			leaseCalls++
			writeRPCResult(w, req, map[string]any{"lease": map[string]any{
				"acquired":     true,
				"workspace_id": "ws-coordinated",
				"agent_id":     "agent-ui",
				"heartbeat_id": "global_reflection",
				"lease_token":  rpcString(req.Params, "lease_token"),
				"locks":        []string{"non_mutating_coordination"},
			}})
		case "agent.heartbeat_lease.release":
			writeRPCResult(w, req, map[string]any{"released": true, "status": "RELEASED"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"ok": true})
		default:
			writeRPCResult(w, req, nil)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	saveTestRuntimeHeartbeatAnatomy(t, workdir, "agent-ui", []AgentHeartbeatSpec{{
		ID:         "global_reflection",
		Kind:       "metacognition",
		CadenceSec: 1,
		Priority:   1000,
		Locks:      []string{"non_mutating_coordination"},
		ToolSuites: []string{"bounded_task_submit"},
	}})
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeTUI,
		WorkspaceID: "ws-coordinated",
		AgentID:     "agent-ui",
		DisplayName: "Agent UI",
		Workdir:     workdir,
		RhizomeRPC:  server.URL,
	}, &sequenceLLM{responses: []*LLMResponse{
		{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"Nothing to change."}`},
		{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"Nothing to change."}`},
		{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"Nothing to change."}`},
		{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"Nothing to change."}`},
	}})

	results, err := runtime.executeDueInternalHeartbeatsCoordinated(context.Background(), time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("executeDueInternalHeartbeatsCoordinated() before support error = %v", err)
	}
	if !internalHeartbeatTestResultsContain(results, "global_reflection") || leaseCalls != 0 {
		t.Fatalf("expected unconfirmed support to run locally once, results=%+v leaseCalls=%d", results, leaseCalls)
	}

	runtime.markInternalHeartbeatRemoteLeaseSupport(true)
	runtime.internalHeartbeatState.LastRun = map[string]time.Time{}
	results, err = runtime.executeDueInternalHeartbeatsCoordinated(context.Background(), time.Date(2026, 5, 17, 12, 1, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("executeDueInternalHeartbeatsCoordinated() after support error = %v", err)
	}
	if !internalHeartbeatTestResultsContain(results, "global_reflection") || leaseCalls != 1 {
		t.Fatalf("expected confirmed support to use one remote lease, results=%+v leaseCalls=%d", results, leaseCalls)
	}
}

func TestExecuteDueInternalHeartbeatsCoordinatedDiscoversDurableLeaseSupport(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var describeCalls int
	var leaseCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "rpc.describe":
			describeCalls++
			if got := rpcString(req.Params, "method"); got != "agent.heartbeat_lease.acquire" {
				t.Fatalf("rpc.describe method = %q", got)
			}
			writeRPCResult(w, req, map[string]any{
				"method":      "agent.heartbeat_lease.acquire",
				"description": "Acquire an agent heartbeat lease.",
				"params": map[string]any{
					"workspace_id": map[string]any{"type": "string", "required": true},
					"agent_id":     map[string]any{"type": "string", "required": true},
					"heartbeat_id": map[string]any{"type": "string", "required": true},
				},
			})
		case "agent.heartbeat_lease.acquire":
			leaseCalls++
			writeRPCResult(w, req, map[string]any{"lease": map[string]any{
				"acquired":     true,
				"workspace_id": "ws-discovery",
				"agent_id":     "agent-ui",
				"heartbeat_id": "global_reflection",
				"lease_token":  rpcString(req.Params, "lease_token"),
				"locks":        []string{"non_mutating_coordination"},
			}})
		case "agent.heartbeat_lease.release":
			writeRPCResult(w, req, map[string]any{"released": true, "status": "RELEASED"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"ok": true})
		default:
			writeRPCResult(w, req, nil)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	saveTestRuntimeHeartbeatAnatomy(t, workdir, "agent-ui", []AgentHeartbeatSpec{{
		ID:         "global_reflection",
		Kind:       "metacognition",
		CadenceSec: 1,
		Priority:   1000,
		Locks:      []string{"non_mutating_coordination"},
		ToolSuites: []string{"bounded_task_submit"},
	}})
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		WorkspaceID: "ws-discovery",
		AgentID:     "agent-ui",
		DisplayName: "Agent UI",
		Workdir:     workdir,
		RhizomeRPC:  server.URL,
	}, &sequenceLLM{responses: []*LLMResponse{
		{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"Nothing to change."}`},
		{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"Nothing to change."}`},
		{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"Nothing to change."}`},
		{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"Nothing to change."}`},
	}})

	results, err := runtime.executeDueInternalHeartbeatsCoordinated(context.Background(), time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("executeDueInternalHeartbeatsCoordinated() error = %v", err)
	}
	if !internalHeartbeatTestResultsContain(results, "global_reflection") {
		t.Fatalf("expected global_reflection result, got %+v", results)
	}
	if describeCalls != 1 || leaseCalls != 1 {
		t.Fatalf("expected one schema probe and one lease acquire, describeCalls=%d leaseCalls=%d", describeCalls, leaseCalls)
	}
	if checked, supported := runtime.internalHeartbeatRemoteLeaseSupport(); !checked || !supported {
		t.Fatalf("expected discovered durable lease support, checked=%v supported=%v", checked, supported)
	}
}

func TestExecuteDueInternalHeartbeatsCoordinatedFallsBackWhenLeaseDescribeMissing(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var describeCalls int
	var leaseCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "rpc.describe":
			describeCalls++
			writeRPCError(w, req, -32602, "unknown method: agent.heartbeat_lease.acquire. Use rpc.methods.list to see all available methods")
		case "agent.heartbeat_lease.acquire":
			leaseCalls++
			t.Fatalf("coordinated heartbeat should not acquire a remote lease after unsupported schema probe")
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"ok": true})
		default:
			writeRPCResult(w, req, nil)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	saveTestRuntimeHeartbeatAnatomy(t, workdir, "agent-ui", []AgentHeartbeatSpec{{
		ID:         "global_reflection",
		Kind:       "metacognition",
		CadenceSec: 1,
		Priority:   1000,
		Locks:      []string{"non_mutating_coordination"},
		ToolSuites: []string{"bounded_task_submit"},
	}})
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		WorkspaceID: "ws-discovery",
		AgentID:     "agent-ui",
		DisplayName: "Agent UI",
		Workdir:     workdir,
		RhizomeRPC:  server.URL,
	}, &sequenceLLM{responses: []*LLMResponse{
		{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"Nothing to change."}`},
		{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"Nothing to change."}`},
		{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"Nothing to change."}`},
		{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"Nothing to change."}`},
	}})

	results, err := runtime.executeDueInternalHeartbeatsCoordinated(context.Background(), time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("executeDueInternalHeartbeatsCoordinated() error = %v", err)
	}
	if !internalHeartbeatTestResultsContain(results, "global_reflection") {
		t.Fatalf("expected global_reflection to fall back to local execution, got %+v", results)
	}
	if describeCalls != 1 || leaseCalls != 0 {
		t.Fatalf("expected one schema probe and no lease acquire, describeCalls=%d leaseCalls=%d", describeCalls, leaseCalls)
	}
	if checked, supported := runtime.internalHeartbeatRemoteLeaseSupport(); !checked || supported {
		t.Fatalf("expected durable lease support to be marked unsupported, checked=%v supported=%v", checked, supported)
	}
}

func TestExecuteDueInternalHeartbeatsCoordinatedFallsBackWhenRPCDescribeUnavailable(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var describeCalls int
	var leaseCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "rpc.describe":
			describeCalls++
			writeRPCError(w, req, -32601, "method not found: rpc.describe")
		case "agent.heartbeat_lease.acquire":
			leaseCalls++
			t.Fatalf("coordinated heartbeat should not acquire a remote lease after rpc.describe is unavailable")
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"ok": true})
		default:
			writeRPCResult(w, req, nil)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	saveTestRuntimeHeartbeatAnatomy(t, workdir, "agent-ui", []AgentHeartbeatSpec{{
		ID:         "global_reflection",
		Kind:       "metacognition",
		CadenceSec: 1,
		Priority:   1000,
		Locks:      []string{"non_mutating_coordination"},
		ToolSuites: []string{"bounded_task_submit"},
	}})
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		WorkspaceID: "ws-discovery",
		AgentID:     "agent-ui",
		DisplayName: "Agent UI",
		Workdir:     workdir,
		RhizomeRPC:  server.URL,
	}, &sequenceLLM{responses: []*LLMResponse{
		{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"Nothing to change."}`},
		{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"Nothing to change."}`},
		{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"Nothing to change."}`},
		{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"Nothing to change."}`},
	}})

	results, err := runtime.executeDueInternalHeartbeatsCoordinated(context.Background(), time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("executeDueInternalHeartbeatsCoordinated() error = %v", err)
	}
	if !internalHeartbeatTestResultsContain(results, "global_reflection") {
		t.Fatalf("expected global_reflection to fall back to local execution, got %+v", results)
	}
	if describeCalls != 1 || leaseCalls != 0 {
		t.Fatalf("expected one schema probe and no lease acquire, describeCalls=%d leaseCalls=%d", describeCalls, leaseCalls)
	}
	if checked, supported := runtime.internalHeartbeatRemoteLeaseSupport(); !checked || supported {
		t.Fatalf("expected durable lease support to be marked unsupported, checked=%v supported=%v", checked, supported)
	}
}

func TestExecuteDueInternalHeartbeatsCoordinatedFailsClosedOnAmbiguousMethodNotFoundProbe(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var describeCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "rpc.describe":
			describeCalls++
			http.Error(w, "upstream method not found while checking schema", http.StatusBadGateway)
		default:
			t.Fatalf("unexpected method after ambiguous schema probe: %s", req.Method)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	now := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	saveTestRuntimeHeartbeatAnatomy(t, workdir, "agent-ui", []AgentHeartbeatSpec{{
		ID:         "global_reflection",
		Kind:       "metacognition",
		CadenceSec: 1,
		Priority:   1000,
		Locks:      []string{"non_mutating_coordination"},
		ToolSuites: []string{"bounded_task_submit"},
	}})
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		WorkspaceID: "ws-discovery",
		AgentID:     "agent-ui",
		DisplayName: "Agent UI",
		Workdir:     workdir,
		RhizomeRPC:  server.URL,
	}, failIfCalledLLM{t: t})
	anatomy := runtimeAnatomyConfig(runtime.cfg)
	runtime.mu.Lock()
	for _, heartbeat := range anatomy.Heartbeats {
		if heartbeat.ID != "global_reflection" {
			runtime.internalHeartbeatState.LastRun[heartbeat.ID] = now
		}
	}
	runtime.mu.Unlock()

	results, err := runtime.executeDueInternalHeartbeatsCoordinated(context.Background(), now)
	if err == nil {
		t.Fatalf("expected ambiguous method-not-found probe to fail closed")
	}
	if describeCalls != 1 {
		t.Fatalf("expected one schema probe, got %d", describeCalls)
	}
	if !internalHeartbeatTestResultsContain(results, "global_reflection") || results[0].Outcome != "scheduler_failure" {
		t.Fatalf("expected scheduler_failure result for probe failure, got %+v", results)
	}
	if !strings.Contains(results[0].Summary, "Internal heartbeat failed before completing a local session") {
		t.Fatalf("expected scheduler failure summary, got %+v", results)
	}
	if checked, supported := runtime.internalHeartbeatRemoteLeaseSupport(); checked || supported {
		t.Fatalf("ambiguous probe should leave support unchecked, checked=%v supported=%v", checked, supported)
	}
}

func TestExecuteDueInternalHeartbeatsCoordinatedFailsClosedWhenLeaseProbeIndeterminate(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var describeCalls int
	var leaseCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "rpc.describe":
			describeCalls++
			http.Error(w, "schema temporarily unavailable", http.StatusBadGateway)
		case "agent.heartbeat_lease.acquire":
			leaseCalls++
			t.Fatalf("coordinated heartbeat should not acquire after indeterminate schema probe")
		default:
			writeRPCResult(w, req, nil)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	now := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	saveTestRuntimeHeartbeatAnatomy(t, workdir, "agent-ui", []AgentHeartbeatSpec{{
		ID:         "global_reflection",
		Kind:       "metacognition",
		CadenceSec: 1,
		Priority:   1000,
		Locks:      []string{"non_mutating_coordination"},
		ToolSuites: []string{"bounded_task_submit"},
	}})
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		WorkspaceID: "ws-discovery",
		AgentID:     "agent-ui",
		DisplayName: "Agent UI",
		Workdir:     workdir,
		RhizomeRPC:  server.URL,
	}, failIfCalledLLM{t: t})
	anatomy := runtimeAnatomyConfig(runtime.cfg)
	runtime.mu.Lock()
	for _, heartbeat := range anatomy.Heartbeats {
		if heartbeat.ID != "global_reflection" {
			runtime.internalHeartbeatState.LastRun[heartbeat.ID] = now
		}
	}
	runtime.mu.Unlock()

	results, err := runtime.executeDueInternalHeartbeatsCoordinated(context.Background(), now)
	if err == nil {
		t.Fatalf("expected indeterminate lease support probe to fail closed")
	}
	if !internalHeartbeatTestResultsContain(results, "global_reflection") || results[0].Status != "failed" || results[0].Outcome != "scheduler_failure" {
		t.Fatalf("expected scheduler_failure result for probe failure, got %+v", results)
	}
	if !strings.Contains(results[0].Summary, "Internal heartbeat failed before completing a local session") {
		t.Fatalf("expected scheduler failure summary, got %+v", results)
	}
	if describeCalls != 1 || leaseCalls != 0 {
		t.Fatalf("expected one schema probe and no lease acquire, describeCalls=%d leaseCalls=%d", describeCalls, leaseCalls)
	}
	if checked, supported := runtime.internalHeartbeatRemoteLeaseSupport(); checked || supported {
		t.Fatalf("indeterminate probe should leave support unchecked, checked=%v supported=%v", checked, supported)
	}
}

func TestExecuteDueInternalHeartbeatsLeasedCachesUnsupportedRemoteLease(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var acquireCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.heartbeat_lease.acquire":
			acquireCalls++
			writeRPCError(w, req, -32601, "method not found: agent.heartbeat_lease.acquire")
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"ok": true})
		default:
			writeRPCResult(w, req, nil)
		}
	}))
	defer server.Close()

	workdir := t.TempDir()
	saveTestRuntimeHeartbeatAnatomy(t, workdir, "agent-ui", []AgentHeartbeatSpec{{
		ID:         "global_reflection",
		Kind:       "metacognition",
		CadenceSec: 1,
		Priority:   1000,
		Locks:      []string{"non_mutating_coordination"},
		ToolSuites: []string{"bounded_task_submit"},
	}})
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		WorkspaceID: "ws-unsupported",
		AgentID:     "agent-ui",
		DisplayName: "Agent UI",
		Workdir:     workdir,
		RhizomeRPC:  server.URL,
	}, &sequenceLLM{responses: []*LLMResponse{
		{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"First local fallback."}`},
		{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"First self-check."}`},
		{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"Second local fallback."}`},
		{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"Second self-check."}`},
	}})

	results, err := runtime.executeDueInternalHeartbeatsLeased(context.Background(), time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("first executeDueInternalHeartbeatsLeased() error = %v", err)
	}
	if !internalHeartbeatTestResultsContain(results, "global_reflection") || acquireCalls != 1 {
		t.Fatalf("expected first run to probe acquire once and execute locally, results=%+v acquireCalls=%d", results, acquireCalls)
	}
	if checked, supported := runtime.internalHeartbeatRemoteLeaseSupport(); !checked || supported {
		t.Fatalf("expected unsupported remote lease cache, checked=%v supported=%v", checked, supported)
	}

	runtime.internalHeartbeatState.LastRun = map[string]time.Time{}
	results, err = runtime.executeDueInternalHeartbeatsLeased(context.Background(), time.Date(2026, 5, 17, 12, 1, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("second executeDueInternalHeartbeatsLeased() error = %v", err)
	}
	if !internalHeartbeatTestResultsContain(results, "global_reflection") || acquireCalls != 1 {
		t.Fatalf("expected unsupported cache to skip repeated remote acquire, results=%+v acquireCalls=%d", results, acquireCalls)
	}
}

func TestExecuteDueInternalHeartbeatsLeasedBlocksSecondRuntimeWithDurableLease(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var activeToken string
	firstAcquired := make(chan struct{})
	var acquireCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		switch req.Method {
		case "agent.heartbeat_lease.acquire":
			acquireCalls++
			token := rpcString(req.Params, "lease_token")
			if activeToken == "" {
				activeToken = token
				select {
				case <-firstAcquired:
				default:
					close(firstAcquired)
				}
				writeRPCResult(w, req, map[string]any{"lease": map[string]any{
					"acquired":     true,
					"workspace_id": "ws-multiprocess",
					"agent_id":     "agent-ui",
					"heartbeat_id": "global_reflection",
					"lease_token":  token,
					"locks":        []string{"non_mutating_coordination"},
				}})
				return
			}
			writeRPCResult(w, req, map[string]any{"lease": map[string]any{
				"acquired":              false,
				"conflict_reason":       "lock_already_leased",
				"conflict_heartbeat_id": "global_reflection",
				"conflict_lock":         "non_mutating_coordination",
			}})
		case "agent.heartbeat_lease.release":
			if rpcString(req.Params, "lease_token") == activeToken {
				activeToken = ""
			}
			writeRPCResult(w, req, map[string]any{"released": true, "status": "RELEASED"})
		case "agent.update.post":
			writeRPCResult(w, req, map[string]any{"ok": true})
		default:
			writeRPCResult(w, req, nil)
		}
	}))
	defer server.Close()

	workdirA := t.TempDir()
	workdirB := t.TempDir()
	heartbeat := AgentHeartbeatSpec{
		ID:         "global_reflection",
		Kind:       "metacognition",
		CadenceSec: 1,
		Priority:   1000,
		Locks:      []string{"non_mutating_coordination"},
		ToolSuites: []string{"bounded_task_submit"},
	}
	saveTestRuntimeHeartbeatAnatomy(t, workdirA, "agent-ui", []AgentHeartbeatSpec{heartbeat})
	saveTestRuntimeHeartbeatAnatomy(t, workdirB, "agent-ui", []AgentHeartbeatSpec{heartbeat})
	first := NewRuntime(RuntimeConfig{WorkspaceID: "ws-multiprocess", AgentID: "agent-ui", Workdir: workdirA, RhizomeRPC: server.URL, PlannerCycleTimeout: 5 * time.Second}, blockingHeartbeatLLM{})
	second := NewRuntime(RuntimeConfig{WorkspaceID: "ws-multiprocess", AgentID: "agent-ui", Workdir: workdirB, RhizomeRPC: server.URL}, &sequenceLLM{responses: []*LLMResponse{{Content: `{"contract_version":"internal-heartbeat-local-result/v1","outcome":"no_action","summary":"Local-only heartbeat may still run."}`}}})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := first.executeDueInternalHeartbeatsLeased(ctx, time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC))
		done <- err
	}()
	select {
	case <-firstAcquired:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatalf("first runtime did not acquire durable lease")
	}

	results, err := second.executeDueInternalHeartbeatsLeased(context.Background(), time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC))
	if err != nil {
		cancel()
		t.Fatalf("second executeDueInternalHeartbeatsLeased() error = %v", err)
	}
	if internalHeartbeatTestResultsContain(results, "global_reflection") {
		cancel()
		t.Fatalf("expected second runtime to skip leased global_reflection heartbeat, got %+v", results)
	}
	if acquireCalls < 2 {
		cancel()
		t.Fatalf("expected both runtimes to attempt acquire, acquireCalls=%d", acquireCalls)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("first runtime did not stop after cancel")
	}
	if activeToken != "" {
		t.Fatalf("expected first runtime to release durable lease, active token still set")
	}
}

func TestInternalHeartbeatStatusSnapshotExposesAnatomyWithoutSideEffects(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	dir := t.TempDir()
	if err := SaveAgentProfile(dir, DefaultAgentProfile("sigma", "Sigma", "UI/UX evil reality critic")); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		AgentID:     "sigma",
		DisplayName: "Sigma",
		Role:        "generalist",
		Workdir:     dir,
	}, nil)
	now := time.Date(2026, 5, 14, 10, 0, 0, 0, time.UTC)
	snapshot := runtime.internalHeartbeatStatusSnapshot(now)
	if snapshot.Preset != "ui_ux_reality_critic" {
		t.Fatalf("preset = %q", snapshot.Preset)
	}
	if snapshot.Digest == "" {
		t.Fatalf("expected digest")
	}
	if snapshot.MaxBrowserSessions != 1 {
		t.Fatalf("MaxBrowserSessions = %d", snapshot.MaxBrowserSessions)
	}
	if !containsAnatomyTestString(snapshot.Due, "visual_product_audit") {
		t.Fatalf("expected visual_product_audit due in fresh observe-only snapshot: %+v", snapshot)
	}
	if runtime.activeTask != nil || runtime.activeSession != nil {
		t.Fatalf("snapshot should not mutate active work")
	}
}

func TestInternalHeartbeatStatusSnapshotDeclaresLocalScheduledExecution(t *testing.T) {
	runtime := NewRuntime(RuntimeConfig{Workdir: t.TempDir(), AgentID: "agent-serial"}, nil)
	snapshot := runtime.internalHeartbeatStatusSnapshot(time.Date(2026, 5, 16, 9, 0, 0, 0, time.UTC))
	if snapshot.ExecutionMode != "local_scheduled_with_locks" {
		t.Fatalf("expected explicit local scheduled heartbeat execution mode, got %+v", snapshot)
	}
	if snapshot.RemoteLease.Available || snapshot.RemoteLease.State != "unavailable" || snapshot.RemoteLease.FailClosedOnIndeterminate {
		t.Fatalf("expected unavailable remote lease status for local runtime, got %+v", snapshot.RemoteLease)
	}
}

func TestInternalHeartbeatStatusSnapshotDeclaresDurableLeaseExecution(t *testing.T) {
	runtime := NewRuntime(RuntimeConfig{Workdir: t.TempDir(), WorkspaceID: "ws", AgentID: "agent-serial", RhizomeRPC: "http://127.0.0.1:7777"}, nil)
	snapshot := runtime.internalHeartbeatStatusSnapshot(time.Date(2026, 5, 16, 9, 0, 0, 0, time.UTC))
	if snapshot.ExecutionMode != "local_scheduled_with_remote_lease_unconfirmed" {
		t.Fatalf("expected unconfirmed remote lease mode before successful acquire, got %+v", snapshot)
	}
	if !snapshot.RemoteLease.Available || snapshot.RemoteLease.Checked || snapshot.RemoteLease.State != "unconfirmed" || !snapshot.RemoteLease.FailClosedOnIndeterminate {
		t.Fatalf("expected explicit unconfirmed remote lease status, got %+v", snapshot.RemoteLease)
	}
	runtime.markInternalHeartbeatRemoteLeaseSupport(true)
	snapshot = runtime.internalHeartbeatStatusSnapshot(time.Date(2026, 5, 16, 9, 0, 0, 0, time.UTC))
	if snapshot.ExecutionMode != "local_scheduled_with_durable_leases" {
		t.Fatalf("expected durable lease heartbeat execution mode, got %+v", snapshot)
	}
	if !snapshot.RemoteLease.Checked || !snapshot.RemoteLease.Supported || snapshot.RemoteLease.State != "supported" {
		t.Fatalf("expected supported remote lease status, got %+v", snapshot.RemoteLease)
	}
	runtime.markInternalHeartbeatRemoteLeaseSupport(false)
	snapshot = runtime.internalHeartbeatStatusSnapshot(time.Date(2026, 5, 16, 9, 0, 0, 0, time.UTC))
	if snapshot.ExecutionMode != "local_scheduled_with_locks_remote_lease_unsupported" {
		t.Fatalf("expected unsupported remote lease mode, got %+v", snapshot)
	}
	if !snapshot.RemoteLease.Checked || snapshot.RemoteLease.Supported || snapshot.RemoteLease.State != "unsupported" {
		t.Fatalf("expected unsupported remote lease status, got %+v", snapshot.RemoteLease)
	}
}

func TestInternalHeartbeatRuntimePackExposesRemoteLeaseDiscoveryState(t *testing.T) {
	runtime := NewRuntime(RuntimeConfig{
		Mode:        RuntimeModeDaemon,
		Workdir:     t.TempDir(),
		WorkspaceID: "ws",
		AgentID:     "agent-serial",
		RhizomeRPC:  "http://127.0.0.1:7777",
	}, nil)
	pack := runtime.buildInternalHeartbeatRuntimePack(time.Date(2026, 5, 16, 9, 0, 0, 0, time.UTC), 4096)
	for _, want := range []string{
		"## Internal Heartbeat Runtime",
		"- execution_mode: local_scheduled_with_remote_lease_unconfirmed",
		"- remote_lease.state: unconfirmed",
		"- remote_lease.available: true",
		"- remote_lease.fail_closed_on_indeterminate: true",
		"indeterminate discovery failures fail closed",
	} {
		if !strings.Contains(pack, want) {
			t.Fatalf("runtime pack missing %q:\n%s", want, pack)
		}
	}
	runtime.markInternalHeartbeatRemoteLeaseSupport(true)
	pack = runtime.buildInternalHeartbeatRuntimePack(time.Date(2026, 5, 16, 9, 1, 0, 0, time.UTC), 4096)
	if !strings.Contains(pack, "- remote_lease.state: supported") || !strings.Contains(pack, "- execution_mode: local_scheduled_with_durable_leases") {
		t.Fatalf("runtime pack did not reflect supported durable lease:\n%s", pack)
	}
}

func TestInternalHeartbeatRestartHydratesTerminalCooldowns(t *testing.T) {
	now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)
	anatomy := testHeartbeatAnatomy([]AgentHeartbeatSpec{
		{ID: "completed", Kind: "metacognition", Cadence: "every_10m", Priority: 30},
		{ID: "failed", Kind: "metacognition", Cadence: "every_10m", Priority: 20},
		{ID: "abandoned", Kind: "metacognition", Cadence: "every_10m", Priority: 10},
		{ID: "disabled", Kind: "metacognition", Cadence: "every_10m", Enabled: boolPtr(false)},
	})
	digest := AgentAnatomyDigest(anatomy)
	state := AgentInternalSessionState{Sessions: []AgentInternalSessionRecord{
		{HeartbeatID: "completed", AnatomyDigest: digest, Status: "completed", StartedAt: now.Add(-7 * time.Minute).Format(time.RFC3339Nano), EndedAt: now.Add(-5 * time.Minute).Format(time.RFC3339Nano)},
		{HeartbeatID: "failed", AnatomyDigest: digest, Status: "failed", StartedAt: now.Add(-6 * time.Minute).Format(time.RFC3339Nano), EndedAt: now.Add(-4 * time.Minute).Format(time.RFC3339Nano)},
		{HeartbeatID: "abandoned", AnatomyDigest: digest, Status: "abandoned", StartedAt: now.Add(-3 * time.Minute).Format(time.RFC3339Nano)},
		{HeartbeatID: "completed", AnatomyDigest: "other-digest", Status: "completed", EndedAt: now.Format(time.RFC3339Nano)},
		{HeartbeatID: "disabled", AnatomyDigest: digest, Status: "completed", EndedAt: now.Format(time.RFC3339Nano)},
		{HeartbeatID: "missing", AnatomyDigest: digest, Status: "completed", EndedAt: now.Format(time.RFC3339Nano)},
		{HeartbeatID: "failed", AnatomyDigest: digest, Status: "running", StartedAt: now.Format(time.RFC3339Nano)},
	}}

	lastRun := internalHeartbeatCooldownsFromSessions(state, anatomy)
	if got := lastRun["completed"]; !got.Equal(now.Add(-5 * time.Minute)) {
		t.Fatalf("completed cooldown hydrated from EndedAt = %s", got)
	}
	if got := lastRun["failed"]; !got.Equal(now.Add(-4 * time.Minute)) {
		t.Fatalf("failed cooldown hydrated from EndedAt = %s", got)
	}
	if got := lastRun["abandoned"]; !got.Equal(now.Add(-3 * time.Minute)) {
		t.Fatalf("abandoned cooldown hydrated from StartedAt fallback = %s", got)
	}
	for _, unexpected := range []string{"disabled", "missing"} {
		if _, ok := lastRun[unexpected]; ok {
			t.Fatalf("unexpected cooldown for %s in %+v", unexpected, lastRun)
		}
	}
	items := internalHeartbeatDueItems(anatomy, now, lastRun, false)
	assertHeartbeatState(t, items, "completed", false, "cooldown")
	assertHeartbeatState(t, items, "failed", false, "cooldown")
	assertHeartbeatState(t, items, "abandoned", false, "cooldown")
}

func testHeartbeatAnatomy(heartbeats []AgentHeartbeatSpec) AgentAnatomyConfig {
	for idx := range heartbeats {
		heartbeats[idx] = normalizeAgentHeartbeatSpec(heartbeats[idx])
	}
	return AgentAnatomyConfig{
		Schema: agentAnatomySchemaV1,
		Concurrency: AgentAnatomyConcurrency{
			MaxParallelInternalSessions: 3,
			MaxLLMSessions:              1,
		},
		Memory:     AgentAnatomyMemory{Enabled: boolPtr(true)},
		Heartbeats: heartbeats,
	}
}

func saveTestRuntimeHeartbeatAnatomy(t *testing.T, workdir, agentID string, heartbeats []AgentHeartbeatSpec) {
	t.Helper()
	profile := DefaultAgentProfile(agentID, agentID, "test heartbeat agent")
	if err := SaveAgentProfile(workdir, profile); err != nil {
		t.Fatal(err)
	}
	if err := SaveAgentAnatomyConfigFile(agentAnatomyPath(workdir), testHeartbeatAnatomy(heartbeats), profile); err != nil {
		t.Fatal(err)
	}
}

func internalHeartbeatTestResultsContain(results []InternalHeartbeatExecutionResult, heartbeatID string) bool {
	for _, result := range results {
		if result.HeartbeatID == heartbeatID {
			return true
		}
	}
	return false
}

func assertHeartbeatState(t *testing.T, items []InternalHeartbeatDueItem, id string, due bool, reason string) {
	t.Helper()
	for _, item := range items {
		if item.ID != id {
			continue
		}
		if item.Due != due || item.Reason != reason {
			t.Fatalf("heartbeat %s state = due:%v reason:%q, want due:%v reason:%q; all=%+v", id, item.Due, item.Reason, due, reason, items)
		}
		return
	}
	t.Fatalf("heartbeat %s not found in %+v", id, items)
}
