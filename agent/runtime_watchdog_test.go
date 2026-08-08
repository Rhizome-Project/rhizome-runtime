package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func openWatchdogTestMemoryService(t *testing.T) *AgentMemoryService {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service, err := OpenAgentMemoryService("ws-watchdog", "agent-watchdog")
	if err != nil {
		t.Fatalf("OpenAgentMemoryService() error = %v", err)
	}
	t.Cleanup(func() { _ = service.Close() })
	return service
}

func TestWatchdogSnapshotDetectsListenerDegradation(t *testing.T) {
	cfg := RuntimeConfig{}
	cfg.ApplyDefaults()

	startedAt := time.Date(2026, 3, 23, 10, 0, 0, 0, time.UTC)
	r := &Runtime{
		cfg:    cfg,
		health: newRuntimeHealthState(startedAt),
	}
	r.health.Listener.LastSuccessAt = startedAt.Add(-10 * time.Minute)

	snapshot := r.watchdogSnapshot(startedAt.Add(11 * time.Minute))
	if snapshot.MonitorVerdict != "degraded" {
		t.Fatalf("expected degraded watchdog verdict, got %+v", snapshot)
	}
	if snapshot.ListenerState != "degraded" {
		t.Fatalf("expected degraded listener state, got %+v", snapshot)
	}
	if snapshot.RecommendedAction != "refresh_bootstrap" {
		t.Fatalf("unexpected watchdog action: %+v", snapshot)
	}
}

func TestWatchdogLoopHealthDegradesForExplicitLivenessFailures(t *testing.T) {
	startedAt := time.Date(2026, 5, 16, 14, 0, 0, 0, time.UTC)
	cfg := RuntimeConfig{}
	cfg.ApplyDefaults()
	cfg.PlannerCycleTimeout = time.Minute
	runtime := &Runtime{
		cfg:    cfg,
		health: newRuntimeHealthState(startedAt),
	}
	now := startedAt.Add(10 * time.Second)
	runtime.recordLoopFailure("heartbeat", errors.New("heartbeat rpc failed"), now)
	runtime.recordLoopFailure("internal_heartbeat", errors.New("internal heartbeat timeout"), now)
	runtime.recordLoopFailure("sse_event_loop", errors.New("sse receive failed"), now)
	runtime.recordLoopFailure("planner_timeout", errors.New("planner timeout"), now)

	snapshot := runtime.watchdogSnapshot(now)
	if snapshot.MonitorVerdict != "degraded" {
		t.Fatalf("expected degraded verdict for explicit loop-health failures, got %+v", snapshot)
	}
	if snapshot.HeartbeatState != "degraded" || snapshot.InternalHeartbeatState != "degraded" || snapshot.SSEEventLoopState != "degraded" || snapshot.PlannerTimeoutState != "degraded" {
		t.Fatalf("expected explicit liveness lanes to degrade, got %+v", snapshot)
	}
	if snapshot.HeartbeatFailures != 1 || snapshot.InternalHeartbeatFailures != 1 || snapshot.SSEEventLoopFailures != 1 || snapshot.PlannerTimeoutFailures != 1 {
		t.Fatalf("expected failure counts for liveness lanes, got %+v", snapshot)
	}
}

func TestWatchdogSnapshotDetectsTaskStall(t *testing.T) {
	cfg := RuntimeConfig{}
	cfg.ApplyDefaults()

	startedAt := time.Date(2026, 3, 23, 10, 0, 0, 0, time.UTC)
	r := &Runtime{
		cfg:           cfg,
		health:        newRuntimeHealthState(startedAt),
		activeTask:    &WorkspaceTaskRecord{TaskID: "task-1"},
		activeSession: &AgentSessionStateRecord{SessionID: "session-1"},
	}
	r.health.LastTaskStartedAt = startedAt
	r.health.LastTaskProgressAt = startedAt
	r.health.LastTaskSummary = "working"

	snapshot := r.watchdogSnapshot(startedAt.Add(cfg.TaskStallAfter + time.Minute))
	if snapshot.MonitorVerdict != "stalled" {
		t.Fatalf("expected stalled watchdog verdict, got %+v", snapshot)
	}
	if snapshot.CurrentTaskID != "task-1" || snapshot.CurrentSessionID != "session-1" {
		t.Fatalf("unexpected watchdog task/session context: %+v", snapshot)
	}
	if snapshot.RecommendedAction != "summarize_and_replan" {
		t.Fatalf("unexpected stalled watchdog action: %+v", snapshot)
	}
}

func TestWatchdogSnapshotUsesDurableReadbackAfterRestart(t *testing.T) {
	cfg := RuntimeConfig{WorkspaceID: "ws-watchdog-readback", AgentID: "agent-watchdog"}
	cfg.ApplyDefaults()
	cfg.TaskStallAfter = 10 * time.Minute

	now := time.Date(2026, 3, 23, 12, 0, 0, 0, time.UTC)
	startedAt := now.Add(-30 * time.Minute)
	durableAt := now.Add(-1 * time.Minute)
	task := WorkspaceTaskRecord{TaskID: "task-watchdog-readback", Title: "Watchdog readback"}
	session := AgentSessionStateRecord{SessionID: "session-watchdog-readback", TaskID: task.TaskID, Status: "ACTIVE"}
	records := taskCycleProgressPhaseRecords(cfg, task, session, "run-watchdog-readback", nil, &StructuredTaskResult{
		Outcome: "blocked",
		Summary: "still working",
	}, nil)

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
						RunID:       "run-watchdog-readback",
						WorkspaceID: cfg.WorkspaceID,
						TaskID:      task.TaskID,
						SessionID:   session.SessionID,
						AgentID:     cfg.AgentID,
						Status:      "ACTIVE",
					},
					Steps: []ExecutionStepRecord{
						{
							StepID:           "step-watchdog-progress",
							RunID:            "run-watchdog-readback",
							WorkspaceID:      cfg.WorkspaceID,
							Phase:            "EXECUTE",
							Status:           "ACTIVE",
							SortOrder:        20,
							UpdatedAt:        durableAt.Format(time.RFC3339Nano),
							VerificationJSON: map[string]any{"task_cycle_phase_records": records},
						},
					},
				},
			},
		}); err != nil {
			t.Fatalf("encode rpc response: %v", err)
		}
	}))
	defer server.Close()

	r := &Runtime{
		cfg:           cfg,
		client:        NewRhizomeClient(server.URL, "test-token"),
		health:        newRuntimeHealthState(startedAt),
		activeTask:    &task,
		activeSession: &session,
		activeRunID:   "run-watchdog-readback",
	}
	r.health.LastTaskStartedAt = startedAt
	r.health.LastTaskProgressAt = time.Time{}
	r.health.Heartbeat.LastSuccessAt = now
	r.health.Listener.LastSuccessAt = now
	r.health.Requests.LastSuccessAt = now
	r.health.Planner.LastSuccessAt = now

	snapshot := r.watchdogSnapshotWithContext(context.Background(), now)
	if snapshot.MonitorVerdict != "healthy" {
		t.Fatalf("expected durable readback to prevent false restart stall, got %+v", snapshot)
	}
	if snapshot.DurableReadback != "ok" || snapshot.LastDurablePhase != "PROGRESS_RECORDED" {
		t.Fatalf("expected durable readback fields, got %+v", snapshot)
	}
	if snapshot.LastTaskProgressAt != durableAt.Format(time.RFC3339Nano) {
		t.Fatalf("expected last task progress to come from durable step, got %+v", snapshot)
	}
}

func TestWatchdogSnapshotDetectsDurablePhaseStallDespiteFreshLiveProgress(t *testing.T) {
	cfg := RuntimeConfig{WorkspaceID: "ws-watchdog-durable-stall", AgentID: "agent-watchdog"}
	cfg.ApplyDefaults()
	cfg.TaskStallAfter = 10 * time.Minute
	cfg.DurableStallAfter = 30 * time.Minute

	now := time.Date(2026, 3, 23, 12, 0, 0, 0, time.UTC)
	startedAt := now.Add(-60 * time.Minute)
	// Durable phase last advanced 35m ago (> DurableStallAfter), but live progress
	// is fresh (2m ago, < TaskStallAfter). The CT-03 independent durable-phase
	// stall must fire even though the live progress timestamp looks healthy.
	durableAt := now.Add(-35 * time.Minute)
	freshProgressAt := now.Add(-2 * time.Minute)
	task := WorkspaceTaskRecord{TaskID: "task-durable-stall", Title: "Durable phase stall"}
	session := AgentSessionStateRecord{SessionID: "session-durable-stall", TaskID: task.TaskID, Status: "ACTIVE"}
	records := taskCycleProgressPhaseRecords(cfg, task, session, "run-durable-stall", nil, &StructuredTaskResult{
		Outcome: "blocked",
		Summary: "still working",
	}, nil)

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
						RunID:       "run-durable-stall",
						WorkspaceID: cfg.WorkspaceID,
						TaskID:      task.TaskID,
						SessionID:   session.SessionID,
						AgentID:     cfg.AgentID,
						Status:      "ACTIVE",
					},
					Steps: []ExecutionStepRecord{
						{
							StepID:           "step-durable-stall",
							RunID:            "run-durable-stall",
							WorkspaceID:      cfg.WorkspaceID,
							Phase:            "EXECUTE",
							Status:           "ACTIVE",
							SortOrder:        20,
							UpdatedAt:        durableAt.Format(time.RFC3339Nano),
							VerificationJSON: map[string]any{"task_cycle_phase_records": records},
						},
					},
				},
			},
		}); err != nil {
			t.Fatalf("encode rpc response: %v", err)
		}
	}))
	defer server.Close()

	r := &Runtime{
		cfg:           cfg,
		client:        NewRhizomeClient(server.URL, "test-token"),
		health:        newRuntimeHealthState(startedAt),
		activeTask:    &task,
		activeSession: &session,
		activeRunID:   "run-durable-stall",
	}
	r.health.LastTaskStartedAt = startedAt
	r.health.LastTaskProgressAt = freshProgressAt
	r.health.Heartbeat.LastSuccessAt = now
	r.health.Listener.LastSuccessAt = now
	r.health.Requests.LastSuccessAt = now
	r.health.Planner.LastSuccessAt = now

	snapshot := r.watchdogSnapshotWithContext(context.Background(), now)
	if snapshot.MonitorVerdict != "stalled" {
		t.Fatalf("expected durable-phase stall to override fresh live progress, got %+v", snapshot)
	}
	if snapshot.RecommendedAction != "summarize_and_replan" {
		t.Fatalf("unexpected durable-phase stall action: %+v", snapshot)
	}
	if !strings.Contains(snapshot.Reason, "durable phase") {
		t.Fatalf("expected durable-phase stall reason, got %q", snapshot.Reason)
	}
}

func TestWatchdogSnapshotDurablePhaseFreshKeepsHealthy(t *testing.T) {
	cfg := RuntimeConfig{WorkspaceID: "ws-watchdog-durable-fresh", AgentID: "agent-watchdog"}
	cfg.ApplyDefaults()
	cfg.TaskStallAfter = 10 * time.Minute
	cfg.DurableStallAfter = 30 * time.Minute

	now := time.Date(2026, 3, 23, 12, 0, 0, 0, time.UTC)
	startedAt := now.Add(-60 * time.Minute)
	// Durable phase advanced 5m ago (< DurableStallAfter) and live progress is
	// also fresh: no stall should fire on either lane.
	durableAt := now.Add(-5 * time.Minute)
	freshProgressAt := now.Add(-2 * time.Minute)
	task := WorkspaceTaskRecord{TaskID: "task-durable-fresh", Title: "Durable phase fresh"}
	session := AgentSessionStateRecord{SessionID: "session-durable-fresh", TaskID: task.TaskID, Status: "ACTIVE"}
	records := taskCycleProgressPhaseRecords(cfg, task, session, "run-durable-fresh", nil, &StructuredTaskResult{
		Outcome: "blocked",
		Summary: "still working",
	}, nil)

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
						RunID:       "run-durable-fresh",
						WorkspaceID: cfg.WorkspaceID,
						TaskID:      task.TaskID,
						SessionID:   session.SessionID,
						AgentID:     cfg.AgentID,
						Status:      "ACTIVE",
					},
					Steps: []ExecutionStepRecord{
						{
							StepID:           "step-durable-fresh",
							RunID:            "run-durable-fresh",
							WorkspaceID:      cfg.WorkspaceID,
							Phase:            "EXECUTE",
							Status:           "ACTIVE",
							SortOrder:        20,
							UpdatedAt:        durableAt.Format(time.RFC3339Nano),
							VerificationJSON: map[string]any{"task_cycle_phase_records": records},
						},
					},
				},
			},
		}); err != nil {
			t.Fatalf("encode rpc response: %v", err)
		}
	}))
	defer server.Close()

	r := &Runtime{
		cfg:           cfg,
		client:        NewRhizomeClient(server.URL, "test-token"),
		health:        newRuntimeHealthState(startedAt),
		activeTask:    &task,
		activeSession: &session,
		activeRunID:   "run-durable-fresh",
	}
	r.health.LastTaskStartedAt = startedAt
	r.health.LastTaskProgressAt = freshProgressAt
	r.health.Heartbeat.LastSuccessAt = now
	r.health.Listener.LastSuccessAt = now
	r.health.Requests.LastSuccessAt = now
	r.health.Planner.LastSuccessAt = now

	snapshot := r.watchdogSnapshotWithContext(context.Background(), now)
	if snapshot.MonitorVerdict != "healthy" {
		t.Fatalf("expected fresh durable phase to stay healthy, got %+v", snapshot)
	}
}

func TestHeartbeatSnapshotAppendsWatchdogSummaryWhenDegraded(t *testing.T) {
	cfg := RuntimeConfig{}
	cfg.ApplyDefaults()

	startedAt := time.Now().UTC().Add(-cfg.ListenerStaleAfter - 5*time.Minute)
	r := &Runtime{
		cfg:     cfg,
		scratch: RuntimeScratchState{LastSummary: "steady"},
		health:  newRuntimeHealthState(startedAt),
	}
	r.health.Listener.LastSuccessAt = startedAt

	status, summary := r.heartbeatSnapshot()
	if status != "ACTIVE" {
		t.Fatalf("expected degraded watchdog to keep ACTIVE heartbeat status, got %q", status)
	}
	if !strings.Contains(summary, "watchdog rsp=observe_only verdict=degraded") {
		t.Fatalf("expected watchdog summary suffix, got %q", summary)
	}
}

func TestMemoryHealthSnapshotDetectsConsecutiveStalePacketReads(t *testing.T) {
	cfg := RuntimeConfig{}
	cfg.ApplyDefaults()
	cfg.MemoryStalePacketThreshold = 2

	service := openWatchdogTestMemoryService(t)
	if err := service.store.PutDigest(LocalMemoryDigestRecord{
		DigestID: "digest:stale",
		Tier:     "P2",
		Kind:     "TASK_DIGEST",
		Scope: LocalMemoryScope{
			TaskID:    "task-stale",
			SessionID: "session-stale",
		},
		Summary:   "Stale task digest",
		UpdatedAt: time.Date(2026, 3, 23, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
		EpisodeDigest: &LocalEpisodeDigest{
			ScopeKind: "task",
			ScopeKey:  "task-stale",
			UpdatedAt: time.Date(2026, 3, 23, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
		},
		Stale: true,
	}); err != nil {
		t.Fatalf("PutDigest() error = %v", err)
	}

	packetInput := MemoryPacketInput{
		Task:    &WorkspaceTaskRecord{TaskID: "task-stale"},
		Session: &AgentSessionStateRecord{SessionID: "session-stale"},
	}
	for i := 0; i < cfg.MemoryStalePacketThreshold; i++ {
		service.invalidatePacketCache()
		_ = service.buildPacket(packetInput, 400)
	}

	r := &Runtime{
		cfg:    cfg,
		memory: service,
	}
	snapshot := r.memoryHealthSnapshot(time.Now().UTC())
	if snapshot.State != "degraded" {
		t.Fatalf("expected degraded memory health, got %+v", snapshot)
	}
	if snapshot.RecommendedAction != "rebuild_memory" {
		t.Fatalf("expected rebuild_memory action, got %+v", snapshot)
	}
	if snapshot.ConsecutiveStaleReads != cfg.MemoryStalePacketThreshold {
		t.Fatalf("expected consecutive stale reads %d, got %+v", cfg.MemoryStalePacketThreshold, snapshot)
	}
}

func TestMemoryHealthSnapshotDetectsStuckPromotionQueue(t *testing.T) {
	cfg := RuntimeConfig{}
	cfg.ApplyDefaults()
	cfg.MemoryPromotionStaleAfter = time.Minute

	service := openWatchdogTestMemoryService(t)
	if err := service.queuePromotion(LocalPromotionCandidate{
		CandidateID: "prom-1",
		NodeType:    localMemoryNodeProcedure,
		MemoryType:  "PROCEDURE",
		Summary:     "Reusable path",
		Body:        "body",
		CreatedAt:   time.Date(2026, 3, 23, 10, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("queuePromotion() error = %v", err)
	}

	r := &Runtime{
		cfg:    cfg,
		memory: service,
	}
	snapshot := r.memoryHealthSnapshot(time.Date(2026, 3, 23, 10, 2, 0, 0, time.UTC))
	if snapshot.State != "degraded" {
		t.Fatalf("expected degraded memory promotion health, got %+v", snapshot)
	}
	if snapshot.RecommendedAction != "flush_promotions" {
		t.Fatalf("expected flush_promotions action, got %+v", snapshot)
	}
}

func TestActuateMemoryHealthRebuildUsesCooldown(t *testing.T) {
	cfg := RuntimeConfig{}
	cfg.ApplyDefaults()
	cfg.MemoryRepairCooldown = 10 * time.Minute

	service := openWatchdogTestMemoryService(t)
	service.mu.Lock()
	service.packetCache["scope"] = cachedMemoryPacket{
		Key:      "scope",
		Compiled: "old",
		BuiltAt:  time.Now().UTC(),
		Sequence: 1,
	}
	service.state.Stats.LastShadowSyncAt = ""
	service.mu.Unlock()

	r := &Runtime{
		cfg:    cfg,
		memory: service,
		health: newRuntimeHealthState(time.Date(2026, 3, 23, 10, 0, 0, 0, time.UTC)),
	}
	snapshot := RuntimeMemoryHealthSnapshot{
		State:             "degraded",
		Reason:            "memory packet hit stale digests repeatedly",
		RecommendedAction: "rebuild_memory",
	}

	firstTick := time.Date(2026, 3, 23, 10, 1, 0, 0, time.UTC)
	if err := r.actuateMemoryHealth(context.Background(), snapshot, firstTick); err != nil {
		t.Fatalf("actuateMemoryHealth(first) error = %v", err)
	}
	control := service.controlSnapshot()
	if control.PacketCacheEntries != 0 {
		t.Fatalf("expected rebuild to clear packet cache, got %+v", control)
	}
	if control.Stats.LastShadowSyncAt == "" {
		t.Fatalf("expected rebuild to refresh shadow sync timestamp, got %+v", control.Stats)
	}

	lastRepairAt := r.health.LastMemoryRepairAt
	if lastRepairAt.IsZero() {
		t.Fatal("expected memory repair timestamp to be recorded")
	}

	service.mu.Lock()
	service.packetCache["scope"] = cachedMemoryPacket{
		Key:      "scope",
		Compiled: "new",
		BuiltAt:  time.Now().UTC(),
		Sequence: 2,
	}
	service.mu.Unlock()
	if err := r.actuateMemoryHealth(context.Background(), snapshot, firstTick.Add(time.Minute)); err != nil {
		t.Fatalf("actuateMemoryHealth(second) error = %v", err)
	}
	control = service.controlSnapshot()
	if control.PacketCacheEntries != 1 {
		t.Fatalf("expected cooldown to skip second rebuild, got %+v", control)
	}
	if !r.health.LastMemoryRepairAt.Equal(lastRepairAt) {
		t.Fatalf("expected cooldown to preserve last repair time, got %s want %s", r.health.LastMemoryRepairAt, lastRepairAt)
	}
}

// TestWatchdogConsumesOwnLoopHealthLane is the CA-27 regression: a recorded
// watchdog loop failure must surface in the verdict instead of the watchdog
// always self-reporting healthy.
func TestWatchdogConsumesOwnLoopHealthLane(t *testing.T) {
	startedAt := time.Date(2026, 5, 29, 9, 0, 0, 0, time.UTC)
	cfg := RuntimeConfig{}
	cfg.ApplyDefaults()
	runtime := &Runtime{cfg: cfg, health: newRuntimeHealthState(startedAt)}
	now := startedAt.Add(10 * time.Second)
	runtime.recordLoopFailure("watchdog", errors.New("policy epoch tick failed"), now)

	snapshot := runtime.watchdogSnapshot(now)
	if snapshot.MonitorVerdict != "degraded" {
		t.Fatalf("expected degraded verdict when the watchdog loop lane is failing, got %+v", snapshot)
	}
	if !strings.Contains(strings.ToLower(snapshot.Reason), "watchdog") {
		t.Fatalf("expected watchdog degradation reason, got %q", snapshot.Reason)
	}
}

// TestRecordTaskCycleProgressFreezesOnRepeatedBlocked is the CA-07 regression: a
// stuck agent emitting the same blocked result every cycle must stop refreshing
// the no-progress timer so the no_progress detector eventually trips.
func TestRecordTaskCycleProgressFreezesOnRepeatedBlocked(t *testing.T) {
	cfg := RuntimeConfig{}
	cfg.ApplyDefaults()
	startedAt := time.Date(2026, 5, 29, 10, 0, 0, 0, time.UTC)
	r := &Runtime{
		cfg:           cfg,
		health:        newRuntimeHealthState(startedAt),
		activeTask:    &WorkspaceTaskRecord{TaskID: "task-churn"},
		activeSession: &AgentSessionStateRecord{SessionID: "session-churn"},
	}
	r.health.LastTaskStartedAt = startedAt
	r.health.LastTaskProgressAt = startedAt

	blocked := StructuredTaskResult{
		Outcome:    "blocked",
		Summary:    "waiting on the same external approval",
		NextAction: "keep waiting",
		BlockedOn:  []BlockedRef{{Kind: "human_input", Detail: "approve the thing"}},
	}
	firstAt := startedAt.Add(time.Minute)
	r.recordTaskCycleProgress(blocked, nil, nil, firstAt)
	for i := 2; i < 60; i++ {
		r.recordTaskCycleProgress(blocked, nil, nil, startedAt.Add(time.Duration(i)*time.Minute))
	}

	snapshot := r.watchdogSnapshot(firstAt.Add(cfg.TaskStallAfter + time.Minute))
	if snapshot.MonitorVerdict != "stalled" {
		t.Fatalf("repeated identical blocked cycles must let no_progress trip, got verdict %q (%s)", snapshot.MonitorVerdict, snapshot.Reason)
	}
}

// TestRecordTaskCycleProgressAdvancesOnChangedBlocker is the CA-07 counter-case: a
// genuinely progressing agent (a different blocker each cycle) keeps the timer
// fresh and must not be falsely declared stalled.
func TestRecordTaskCycleProgressAdvancesOnChangedBlocker(t *testing.T) {
	cfg := RuntimeConfig{}
	cfg.ApplyDefaults()
	startedAt := time.Date(2026, 5, 29, 11, 0, 0, 0, time.UTC)
	r := &Runtime{
		cfg:           cfg,
		health:        newRuntimeHealthState(startedAt),
		activeTask:    &WorkspaceTaskRecord{TaskID: "task-progress"},
		activeSession: &AgentSessionStateRecord{SessionID: "session-progress"},
	}
	r.health.LastTaskStartedAt = startedAt
	r.health.LastTaskProgressAt = startedAt

	r.recordTaskCycleProgress(StructuredTaskResult{
		Outcome:   "blocked",
		Summary:   "blocked on dependency A",
		BlockedOn: []BlockedRef{{Kind: "dependency", Detail: "task-A"}},
	}, nil, nil, startedAt.Add(time.Minute))
	secondAt := startedAt.Add(2 * time.Minute)
	r.recordTaskCycleProgress(StructuredTaskResult{
		Outcome:   "blocked",
		Summary:   "blocked on dependency B",
		BlockedOn: []BlockedRef{{Kind: "dependency", Detail: "task-B"}},
	}, nil, nil, secondAt)

	snapshot := r.watchdogSnapshot(secondAt.Add(cfg.TaskStallAfter / 2))
	if snapshot.MonitorVerdict == "stalled" {
		t.Fatalf("a changed blocker must refresh progress; got stalled (%s)", snapshot.Reason)
	}
}
