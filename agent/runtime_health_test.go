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

func setRuntimeHealthTestHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	return home
}

func TestRuntimeLoopHealthPersistsNamedLoopsAndRestartState(t *testing.T) {
	setRuntimeHealthTestHome(t)

	cfg := RuntimeConfig{
		WorkspaceID: "ws-health",
		AgentID:     "agent-health",
	}
	cfg.ApplyDefaults()
	startedAt := time.Date(2026, 4, 26, 10, 0, 0, 0, time.UTC)
	r := &Runtime{
		cfg:    cfg,
		health: newRuntimeHealthState(startedAt),
		scratch: RuntimeScratchState{
			ActiveTaskID:    "task-health",
			ActiveSessionID: "session-health",
			ActiveRunID:     "run-health",
			DocSHAs:         map[string]string{},
		},
	}

	r.recordLoopStarted("heartbeat", startedAt.Add(time.Second))
	r.recordLoopSuccess("heartbeat", startedAt.Add(2*time.Second))
	r.recordLoopStarted("internal_heartbeat", startedAt.Add(2500*time.Millisecond))
	r.recordLoopSuccess("message_poll", startedAt.Add(3*time.Second))
	r.recordLoopFailure("request_poll", errors.New("rpc unavailable"), startedAt.Add(4*time.Second))
	r.recordLoopSuccess("planner", startedAt.Add(5*time.Second))
	r.recordLoopSuccess("watchdog", startedAt.Add(6*time.Second))
	r.recordLoopSuccess("scratch_replay", startedAt.Add(7*time.Second))
	r.recordLoopSuccess("memory_sync", startedAt.Add(8*time.Second))

	snapshot, err := loadRuntimeLoopHealthSnapshot(cfg.WorkspaceID, cfg.AgentID)
	if err != nil {
		t.Fatalf("loadRuntimeLoopHealthSnapshot() error = %v", err)
	}
	if snapshot.Version != runtimeLoopHealthStateVersion || snapshot.WorkspaceID != cfg.WorkspaceID || snapshot.AgentID != cfg.AgentID {
		t.Fatalf("unexpected persisted loop health identity: %+v", snapshot)
	}
	if snapshot.ActiveTaskID != "task-health" || snapshot.ActiveSessionID != "session-health" || snapshot.ActiveRunID != "run-health" {
		t.Fatalf("expected restart-observable active ids, got %+v", snapshot)
	}
	for _, name := range runtimeLoopHealthNames {
		entry, ok := snapshot.Loops[name]
		if !ok {
			t.Fatalf("missing loop health entry %q in %+v", name, snapshot.Loops)
		}
		if strings.TrimSpace(entry.State) == "" {
			t.Fatalf("loop health entry %q has empty state: %+v", name, entry)
		}
	}
	if got := snapshot.Loops["request_poll"]; got.State != "degraded" || got.ConsecutiveFailures != 1 || !strings.Contains(got.LastError, "rpc unavailable") {
		t.Fatalf("expected degraded request poll evidence, got %+v", got)
	}
	if got := snapshot.Loops["memory_sync"]; got.State != "healthy" || got.LastSuccessAt == "" {
		t.Fatalf("expected healthy memory sync evidence, got %+v", got)
	}
	if got := snapshot.Loops["internal_heartbeat"]; got.LastStartedAt == "" {
		t.Fatalf("expected internal heartbeat loop startup to be tracked, got %+v", got)
	}

	nextStartedAt := startedAt.Add(time.Hour)
	next := newRuntimeHealthStateFromPrevious(nextStartedAt, snapshot)
	if next.RestartCount != snapshot.RestartCount+1 {
		t.Fatalf("expected restart count to advance, got %d from previous %+v", next.RestartCount, snapshot)
	}
	if !next.PreviousStartedAt.Equal(startedAt) || !next.PreviousObservedAt.Equal(parseRuntimeLoopHealthTime(snapshot.ObservedAt)) {
		t.Fatalf("expected previous runtime timestamps to be retained, got started=%s observed=%s", next.PreviousStartedAt, next.PreviousObservedAt)
	}
}

func TestRuntimeMemorySyncHealthTickRecordsFailure(t *testing.T) {
	setRuntimeHealthTestHome(t)

	cfg := RuntimeConfig{
		WorkspaceID: "ws-health-sync",
		AgentID:     "agent-health-sync",
	}
	cfg.ApplyDefaults()
	service, err := OpenAgentMemoryService(cfg.WorkspaceID, cfg.AgentID)
	if err != nil {
		t.Fatalf("OpenAgentMemoryService() error = %v", err)
	}
	t.Cleanup(func() { _ = service.Close() })
	if err := service.queuePromotion(LocalPromotionCandidate{
		CandidateID: "prom-health-sync",
		MemoryType:  "PROCEDURE",
		Summary:     "Reusable deployment path",
		Body:        "Always preserve restart-observable loop health.",
		CreatedAt:   time.Date(2026, 4, 26, 9, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("queuePromotion() error = %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "workspace.memory.write" {
			t.Fatalf("unexpected rpc method: %s", req.Method)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"error": map[string]any{
				"code":    -32000,
				"message": "memory write unavailable",
			},
		})
	}))
	defer server.Close()

	startedAt := time.Date(2026, 4, 26, 10, 0, 0, 0, time.UTC)
	runtime := &Runtime{
		cfg:    cfg,
		client: NewRhizomeClient(server.URL, "token"),
		memory: service,
		health: newRuntimeHealthState(startedAt),
	}
	observedAt := startedAt.Add(time.Minute)
	if err := runtime.recordMemorySyncHealthTick(context.Background(), observedAt); err == nil {
		t.Fatal("expected memory sync tick to surface write failure")
	}

	snapshot, err := loadRuntimeLoopHealthSnapshot(cfg.WorkspaceID, cfg.AgentID)
	if err != nil {
		t.Fatalf("loadRuntimeLoopHealthSnapshot() error = %v", err)
	}
	entry := snapshot.Loops["memory_sync"]
	if entry.State != "degraded" || entry.ConsecutiveFailures != 1 || !strings.Contains(entry.LastError, "memory write unavailable") {
		t.Fatalf("expected persisted degraded memory sync evidence, got %+v", entry)
	}
	if entry.LastFailureAt != observedAt.Format(time.RFC3339Nano) {
		t.Fatalf("expected memory sync failure timestamp %s, got %+v", observedAt.Format(time.RFC3339Nano), entry)
	}
}
