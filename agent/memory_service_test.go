package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func openTestAgentMemoryService(t *testing.T, workspaceID, agentID string) *AgentMemoryService {
	t.Helper()
	service, err := OpenAgentMemoryService(workspaceID, agentID)
	if err != nil {
		t.Fatalf("OpenAgentMemoryService() error = %v", err)
	}
	t.Cleanup(func() {
		_ = service.Close()
	})
	return service
}

func readPersistedAgentMemoryState(t *testing.T, service *AgentMemoryService) agentMemoryState {
	t.Helper()
	raw, err := os.ReadFile(service.statePath)
	if err != nil {
		t.Fatalf("ReadFile(service_state) error = %v", err)
	}
	var state agentMemoryState
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatalf("decode persisted service state: %v", err)
	}
	return state
}

func writeTestEpisodicLog(t *testing.T, path string, lines ...string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("MkdirAll(log dir) error = %v", err)
	}
	content := strings.Join(lines, "\n")
	if content != "" && !strings.HasSuffix(content, "\n") {
		content += "\n"
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile(log) error = %v", err)
	}
}

func encodeTestMemoryEventLine(t *testing.T, event LocalMemoryEvent) string {
	t.Helper()
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("json.Marshal(event) error = %v", err)
	}
	return string(raw)
}

func TestAgentMemoryServiceAppendEventReleasesLockOnIOError(t *testing.T) {
	// MEM-01: a file-I/O failure in appendEvent must release s.mu before returning. Before the
	// fix, a bare return-while-locked permanently wedged the memory subsystem (and the watchdog
	// that reads under the same lock). Force the append's MkdirAll to fail and assert a later
	// locked op does not deadlock.
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	service := openTestAgentMemoryService(t, "ws-lock", "agent-lock")

	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("seed blocker file: %v", err)
	}
	service.logPath = filepath.Join(blocker, "episodic.jsonl")

	if err := service.appendEvent(LocalMemoryEvent{EventKind: "test", Summary: "io-fail"}); err == nil {
		t.Fatal("expected appendEvent to fail when the log directory cannot be created")
	}

	done := make(chan struct{})
	go func() {
		_ = service.statsSnapshot()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("statsSnapshot blocked: appendEvent leaked the mutex on the I/O error path (MEM-01 regression)")
	}
}

func TestAgentMemoryServicePersistsEventsAndDigests(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws-1", "agent-1")
	if err := service.appendEvent(LocalMemoryEvent{
		NodeType:       localMemoryNodeRawMessage,
		EventKind:      "inbound_message",
		Summary:        "Need clarification on artifact shape",
		TaskID:         "task-1",
		SessionID:      "sess-1",
		TensionID:      "tension-1",
		ProtoClusterID: "cluster-1",
		ArtifactRefs:   []string{"doc:plan"},
		DocKeys:        []string{"task.task-1"},
		SourceID:       "msg-1",
	}); err != nil {
		t.Fatalf("appendEvent() error = %v", err)
	}
	if err := service.queuePromotion(LocalPromotionCandidate{
		NodeType: localMemoryNodeEpisodePack,
		Title:    "Task 1 digest",
		Summary:  "Promotion candidate",
		TaskID:   "task-1",
	}); err != nil {
		t.Fatalf("queuePromotion() error = %v", err)
	}

	reloaded := openTestAgentMemoryService(t, "ws-1", "agent-1")

	stats := reloaded.statsSnapshot()
	if stats.TotalEvents != 1 {
		t.Fatalf("expected one local event, got %d", stats.TotalEvents)
	}
	if stats.RawMessages != 1 {
		t.Fatalf("expected one raw message, got %d", stats.RawMessages)
	}
	if stats.PromotionQueue != 1 {
		t.Fatalf("expected one queued promotion, got %d", stats.PromotionQueue)
	}
	if digest := reloaded.state.TaskDigests["task-1"]; digest.EventCount != 1 || digest.MessageCount != 1 {
		t.Fatalf("unexpected task digest: %+v", digest)
	}
	if digest := reloaded.state.TensionDigests["tension-1"]; digest.EventCount != 1 {
		t.Fatalf("unexpected tension digest: %+v", digest)
	}
	if digest := reloaded.state.ClusterDigests["cluster-1"]; digest.EventCount != 1 {
		t.Fatalf("unexpected cluster digest: %+v", digest)
	}
	if _, err := os.Stat(reloaded.logPath); err != nil {
		t.Fatalf("expected episodic log to exist: %v", err)
	}
	storeStats := reloaded.storeStatsSnapshot()
	if storeStats.Episodes != 1 || storeStats.Digests == 0 {
		t.Fatalf("expected persistent store to track episodes and digests, got %+v", storeStats)
	}
}

func TestAgentMemoryServicePersistsControlStateWithoutShadowContinuity(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws-shadow", "agent-shadow")
	if err := service.appendEvent(LocalMemoryEvent{
		NodeType:       localMemoryNodeRawMessage,
		EventKind:      "inbound_message",
		Summary:        "Shadow-free persistence",
		TaskID:         "task-shadow",
		SessionID:      "sess-shadow",
		TensionID:      "tension-shadow",
		ProtoClusterID: "cluster-shadow",
		SourceID:       "msg-shadow",
	}); err != nil {
		t.Fatalf("appendEvent() error = %v", err)
	}
	if err := service.queuePromotion(LocalPromotionCandidate{
		CandidateID: "prom-shadow",
		NodeType:    localMemoryNodeProcedure,
		MemoryType:  "PROCEDURE",
		Summary:     "Promote me",
		Body:        "body",
	}); err != nil {
		t.Fatalf("queuePromotion() error = %v", err)
	}

	persisted := readPersistedAgentMemoryState(t, service)
	if len(persisted.RecentEvents) != 0 {
		t.Fatalf("expected persisted service state to omit recent_events shadow, got %+v", persisted.RecentEvents)
	}
	if len(persisted.TaskDigests) != 0 || len(persisted.TensionDigests) != 0 || len(persisted.ClusterDigests) != 0 {
		t.Fatalf("expected persisted service state to omit digest shadow, got %+v", persisted)
	}
	if len(persisted.Procedures) != 0 || len(persisted.AntiProcedures) != 0 {
		t.Fatalf("expected persisted service state to omit procedure shadow, got %+v", persisted)
	}
	pending := service.pendingPromotions(10)
	if len(pending) != 1 || pending[0].CandidateID != "prom-shadow" {
		t.Fatalf("expected SQLite to keep promotions, got %+v", pending)
	}
}

func TestAgentMemoryServiceBuildPacketCachesAndInvalidates(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws-2", "agent-2")
	if err := service.appendEvent(LocalMemoryEvent{
		NodeType:       localMemoryNodeRawEvent,
		EventKind:      "task_cycle_result",
		Summary:        "Verified artifact delta",
		TaskID:         "task-2",
		SessionID:      "sess-2",
		TensionID:      "tension-2",
		ProtoClusterID: "cluster-2",
		Outcome:        "continue",
	}); err != nil {
		t.Fatalf("appendEvent() error = %v", err)
	}

	input := MemoryPacketInput{
		Task:    &WorkspaceTaskRecord{TaskID: "task-2", Title: "Task Two", Status: "RUNNING", Priority: "HIGH"},
		Session: &AgentSessionStateRecord{SessionID: "sess-2", TaskID: "task-2", Status: "ACTIVE"},
		Focus:   &RuntimeFocusState{TaskID: "task-2", ProtoClusterID: "cluster-2", FocusTensionID: "tension-2", CorridorReadiness: "READY", ControlAttentionBand: "WATCH"},
	}

	first := service.buildPacket(input, 4000)
	if !strings.Contains(first, "## Agent Memory Body") {
		t.Fatalf("expected memory packet to contain Agent Memory Body section, got %q", first)
	}
	second := service.buildPacket(input, 4000)
	stats := service.statsSnapshot()
	if stats.P1Misses == 0 || stats.P1Hits == 0 {
		t.Fatalf("expected both cache miss and hit, got %+v", stats)
	}

	if err := service.appendEvent(LocalMemoryEvent{
		NodeType:       localMemoryNodeBlocker,
		EventKind:      "task_cycle_result",
		Summary:        "Blocked on auth gate",
		TaskID:         "task-2",
		SessionID:      "sess-2",
		ProtoClusterID: "cluster-2",
		Outcome:        "blocked",
		BlockerKinds:   []string{"credential"},
	}); err != nil {
		t.Fatalf("appendEvent(second) error = %v", err)
	}
	third := service.buildPacket(input, 4000)
	if !strings.Contains(third, "Blocked on auth gate") {
		t.Fatalf("expected rebuilt packet to include latest blocker summary, got %q", third)
	}
	if !strings.Contains(third, "Tension Digest") {
		t.Fatalf("expected rebuilt packet to include tension digest, got %q", third)
	}
	if third == second {
		t.Fatal("expected cache invalidation after new memory event")
	}
}

func TestMemoryPacketKeyTracksArtifactVersions(t *testing.T) {
	input := MemoryPacketInput{
		Task:      &WorkspaceTaskRecord{TaskID: "task-2"},
		Session:   &AgentSessionStateRecord{SessionID: "sess-2"},
		Focus:     &RuntimeFocusState{ProtoClusterID: "cluster-2", FocusTensionID: "tension-2"},
		Hydration: &TaskHydrationBundle{Artifacts: []WorkspaceArtifactRecord{{ArtifactRef: "doc:deliverable.brief", ArtifactID: "artifact-old"}}},
	}
	oldKey := memoryPacketKey(input, map[string]string{"doc:deliverable.brief": "artifact-old"})
	newKey := memoryPacketKey(input, map[string]string{"doc:deliverable.brief": "artifact-new"})
	if oldKey == newKey {
		t.Fatalf("expected packet key to change when artifact version changes, old=%q new=%q", oldKey, newKey)
	}
}

func TestRuntimeBuildDaemonSpecPackIncludesAgentMemoryBody(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws-3", "agent-3")
	if err := service.appendEvent(LocalMemoryEvent{
		NodeType:       localMemoryNodeRawEvent,
		EventKind:      "task_cycle_result",
		Summary:        "Local digest summary",
		TaskID:         "task-3",
		SessionID:      "sess-3",
		ProtoClusterID: "cluster-3",
		Outcome:        "continue",
	}); err != nil {
		t.Fatalf("appendEvent() error = %v", err)
	}

	task := &WorkspaceTaskRecord{
		TaskID:       "task-3",
		Title:        "Task Three",
		OwnerUserID:  "owner-1",
		Priority:     "HIGH",
		Status:       "RUNNING",
		TaskKind:     "EXECUTION",
		TaskTemplate: "integration",
	}
	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID:        "ws-3",
			AgentID:            "agent-3",
			MaxPromptSpecChars: 12000,
		},
		scratch:       RuntimeScratchState{DocSHAs: map[string]string{}, ActiveTaskID: "task-3", ActiveSessionID: "sess-3"},
		activeTask:    task,
		activeSession: &AgentSessionStateRecord{SessionID: "sess-3", TaskID: "task-3", Status: "ACTIVE", Summary: "working"},
		activeWorkPacket: &AgentWorkPacket{
			WorkType: "resume_session",
			Advisory: &AgentWorkAdvisory{ProtoClusterID: "cluster-3"},
		},
		memory: service,
	}

	spec := runtime.buildDaemonSpecPack(context.Background(), task)
	if !strings.Contains(spec, "## Agent Memory Body") {
		t.Fatalf("expected daemon spec pack to include agent memory body, got %q", spec)
	}
	if !strings.Contains(spec, "Local digest summary") {
		t.Fatalf("expected daemon spec pack to include local memory summary, got %q", spec)
	}
}

func TestAgentMemoryServiceMarkPromotionsPromotedBySource(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws-4", "agent-4")
	if err := service.queuePromotion(LocalPromotionCandidate{
		SourceID: "run-42",
		NodeType: localMemoryNodeEpisodePack,
		Title:    "Digest",
		Summary:  "Promotion candidate",
		TaskID:   "task-4",
	}); err != nil {
		t.Fatalf("queuePromotion() error = %v", err)
	}
	if err := service.markPromotionsPromotedBySource("run-42"); err != nil {
		t.Fatalf("markPromotionsPromotedBySource() error = %v", err)
	}
	reloaded := openTestAgentMemoryService(t, "ws-4", "agent-4")
	row := reloaded.store.db.QueryRow("SELECT COUNT(*) FROM local_memory_promotions WHERE promoted_at != ''")
	var count int
	if err := row.Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected promotion to be marked promoted, got %d", count)
	}
}

func TestAgentMemoryServiceQueuePromotionUsesUniqueIDs(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws-5", "agent-5")
	if err := service.queuePromotion(LocalPromotionCandidate{Title: "one", Body: "first"}); err != nil {
		t.Fatalf("queuePromotion(first) error = %v", err)
	}
	if err := service.queuePromotion(LocalPromotionCandidate{Title: "two", Body: "second"}); err != nil {
		t.Fatalf("queuePromotion(second) error = %v", err)
	}
	pending := service.pendingPromotions(10)
	if len(pending) != 2 {
		t.Fatalf("expected two promotions, got %+v", pending)
	}
	if pending[0].CandidateID == "" || pending[0].CandidateID == pending[1].CandidateID {
		t.Fatalf("expected unique candidate ids, got %+v", pending)
	}
}

func TestOpenAgentMemoryServiceQuarantinesCorruptState(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	base := agentMemoryBasePath("ws-6", "agent-6")
	if err := os.MkdirAll(base, 0o700); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	path := filepath.Join(base, "service_state.json")
	if err := os.WriteFile(path, []byte("{not-json"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	service := openTestAgentMemoryService(t, "ws-6", "agent-6")
	if service == nil {
		t.Fatal("expected service after corrupt state quarantine")
	}
	if len(service.state.RecentEvents) != 0 {
		t.Fatalf("expected empty service state after quarantine, got %+v", service.state)
	}
	matches, err := filepath.Glob(path + ".corrupt-*")
	if err != nil {
		t.Fatalf("Glob() error = %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected one quarantined state file, got %#v", matches)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("expected clean state file to be recreated, got err=%v", err)
	}
	if len(data) == 0 || data[0] != '{' {
		t.Fatalf("expected recreated clean state file, got %q", string(data))
	}
}

func TestAgentMemoryServiceRebuildsShadowFromPersistentStoreOnRestart(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws-7", "agent-7")
	if err := service.rememberDocVersions(map[string]string{"task.task-7": "sha-1"}); err != nil {
		t.Fatalf("rememberDocVersions() error = %v", err)
	}
	if err := service.appendEvent(LocalMemoryEvent{
		NodeType:       localMemoryNodeRawEvent,
		EventKind:      "task_cycle_result",
		Summary:        "Recovered from persistent P2",
		TaskID:         "task-7",
		SessionID:      "sess-7",
		TensionID:      "tension-7",
		ProtoClusterID: "cluster-7",
		DocKeys:        []string{"task.task-7"},
		Outcome:        "continue",
	}); err != nil {
		t.Fatalf("appendEvent() error = %v", err)
	}
	if err := os.Remove(service.statePath); err != nil {
		t.Fatalf("Remove(service_state) error = %v", err)
	}

	reloaded := openTestAgentMemoryService(t, "ws-7", "agent-7")
	if len(reloaded.state.RecentEvents) == 0 {
		t.Fatal("expected recent events to be rebuilt from persistent store")
	}
	if digest := reloaded.state.TaskDigests["task-7"]; digest.LastSummary != "Recovered from persistent P2" {
		t.Fatalf("expected task digest to rebuild from store, got %+v", digest)
	}
	if reloaded.state.DocVersions["task.task-7"] != "sha-1" {
		t.Fatalf("expected doc version to survive restart, got %+v", reloaded.state.DocVersions)
	}

	packet := reloaded.buildPacket(MemoryPacketInput{
		Task:    &WorkspaceTaskRecord{TaskID: "task-7", Title: "Task Seven", Status: "RUNNING", Priority: "HIGH"},
		Session: &AgentSessionStateRecord{SessionID: "sess-7", TaskID: "task-7", Status: "ACTIVE"},
		Focus:   &RuntimeFocusState{TaskID: "task-7", ProtoClusterID: "cluster-7", FocusTensionID: "tension-7"},
	}, 4000)
	if !strings.Contains(packet, "Recovered from persistent P2") {
		t.Fatalf("expected packet to use rebuilt persistent memory, got %q", packet)
	}

	snapshot := reloaded.store.Snapshot()
	foundVersionedGuard := false
	for _, digest := range snapshot.Digests {
		for _, guard := range digest.Guards {
			if guard.GuardType == "doc_sha" && guard.Ref == "task.task-7" && guard.Version == "sha-1" {
				foundVersionedGuard = true
			}
		}
	}
	if !foundVersionedGuard {
		t.Fatalf("expected persistent digests to carry doc_sha guard, got %+v", snapshot.Digests)
	}
}

func TestOpenAgentMemoryServiceRebuildsStoreFromEpisodicLogWhenSQLiteIsMissing(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws-replay", "agent-replay")
	if err := service.appendEvent(LocalMemoryEvent{
		NodeType:       localMemoryNodeRawEvent,
		EventKind:      "task_cycle_result",
		Summary:        "First replayable event",
		TaskID:         "task-replay",
		SessionID:      "sess-replay",
		TensionID:      "tension-replay",
		ProtoClusterID: "cluster-replay",
		SourceID:       "run-1",
	}); err != nil {
		t.Fatalf("appendEvent(1) error = %v", err)
	}
	if err := service.appendEvent(LocalMemoryEvent{
		NodeType:       localMemoryNodeProcedure,
		EventKind:      "task_cycle_result",
		Summary:        "Second replayable event",
		TaskID:         "task-replay",
		SessionID:      "sess-replay",
		TensionID:      "tension-replay",
		ProtoClusterID: "cluster-replay",
		SourceID:       "run-2",
	}); err != nil {
		t.Fatalf("appendEvent(2) error = %v", err)
	}
	storePath := service.store.path
	if err := service.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := os.Remove(storePath); err != nil {
		t.Fatalf("Remove(state.db) error = %v", err)
	}

	reloaded := openTestAgentMemoryService(t, "ws-replay", "agent-replay")
	storeStats := reloaded.storeStatsSnapshot()
	if storeStats.Episodes != 2 || storeStats.Digests == 0 {
		t.Fatalf("expected episodic log replay to restore store, got %+v", storeStats)
	}
	if len(reloaded.state.RecentEvents) == 0 {
		t.Fatal("expected shadow to be rebuilt from replayed store")
	}
}

func TestOpenAgentMemoryServiceMigratesLegacyShadowIntoStoreWhenNoReplayLogExists(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	base := agentMemoryBasePath("ws-legacy-shadow", "agent-legacy-shadow")
	if err := os.MkdirAll(base, 0o700); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}

	legacy := agentMemoryState{
		Version:      agentMemoryStateVersion,
		WorkspaceID:  "ws-legacy-shadow",
		AgentID:      "agent-legacy-shadow",
		LastSequence: 1,
		RecentEvents: []LocalMemoryEvent{{
			Sequence:       1,
			OccurredAt:     time.Date(2026, 3, 23, 19, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
			NodeType:       localMemoryNodeRawMessage,
			EventKind:      "inbound_message",
			Summary:        "Legacy shadow event",
			TaskID:         "task-legacy-shadow",
			SessionID:      "sess-legacy-shadow",
			TensionID:      "tension-legacy-shadow",
			ProtoClusterID: "cluster-legacy-shadow",
			SourceID:       "msg-legacy-shadow",
		}},
		TaskDigests: map[string]LocalEpisodeDigest{
			"task-legacy-shadow": {
				ScopeKey:        "task-legacy-shadow",
				ScopeKind:       "task",
				UpdatedAt:       time.Date(2026, 3, 23, 19, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
				EventCount:      1,
				LastSummary:     "Legacy shadow event",
				ProtoClusterID:  "cluster-legacy-shadow",
				LatestTensionID: "tension-legacy-shadow",
				LatestSessionID: "sess-legacy-shadow",
			},
		},

		Stats: LocalMemoryStats{TotalEvents: 1},
	}
	raw, err := json.MarshalIndent(legacy, "", "  ")
	if err != nil {
		t.Fatalf("MarshalIndent(legacy) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(base, "service_state.json"), raw, 0o600); err != nil {
		t.Fatalf("WriteFile(service_state) error = %v", err)
	}

	service := openTestAgentMemoryService(t, "ws-legacy-shadow", "agent-legacy-shadow")
	storeStats := service.storeStatsSnapshot()
	if storeStats.Episodes != 1 || storeStats.Digests == 0 {
		t.Fatalf("expected legacy shadow migration to seed store, got %+v", storeStats)
	}
	if len(service.state.RecentEvents) == 0 {
		t.Fatal("expected service shadow to rebuild from migrated store")
	}
	persisted := readPersistedAgentMemoryState(t, service)
	if len(persisted.RecentEvents) != 0 || len(persisted.TaskDigests) != 0 {
		t.Fatalf("expected persisted service state to be rewritten without shadow after migration, got %+v", persisted)
	}

}

func TestOpenAgentMemoryServiceSalvagesCorruptEpisodicTail(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	base := agentMemoryBasePath("ws-log-tail", "agent-log-tail")
	eventLine := encodeTestMemoryEventLine(t, LocalMemoryEvent{
		Sequence:       1,
		OccurredAt:     time.Date(2026, 3, 23, 20, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
		NodeType:       localMemoryNodeRawEvent,
		EventKind:      "task_cycle_result",
		Summary:        "Valid prefix",
		TaskID:         "task-log-tail",
		SessionID:      "sess-log-tail",
		TensionID:      "tension-log-tail",
		ProtoClusterID: "cluster-log-tail",
		SourceID:       "run-log-tail",
	})
	writeTestEpisodicLog(t, filepath.Join(base, "episodic.jsonl"), eventLine, `{"sequence":2,"event_kind":"broken"`)

	service := openTestAgentMemoryService(t, "ws-log-tail", "agent-log-tail")
	storeStats := service.storeStatsSnapshot()
	if storeStats.Episodes != 1 {
		t.Fatalf("expected one recovered episode after tail salvage, got %+v", storeStats)
	}
	matches, err := filepath.Glob(service.logPath + ".corrupt-*")
	if err != nil {
		t.Fatalf("Glob(corrupt log) error = %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected one quarantined episodic log backup, got %#v", matches)
	}
	data, err := os.ReadFile(service.logPath)
	if err != nil {
		t.Fatalf("ReadFile(salvaged log) error = %v", err)
	}
	if got := strings.Count(strings.TrimSpace(string(data)), "\n") + 1; got != 1 {
		t.Fatalf("expected salvaged log to keep one valid line, got %d lines in %q", got, string(data))
	}
}

func TestOpenAgentMemoryServiceSalvagesCorruptMiddleLineAndKeepsLaterEvents(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	base := agentMemoryBasePath("ws-log-middle", "agent-log-middle")
	first := encodeTestMemoryEventLine(t, LocalMemoryEvent{
		Sequence:       1,
		OccurredAt:     time.Date(2026, 3, 23, 20, 10, 0, 0, time.UTC).Format(time.RFC3339Nano),
		NodeType:       localMemoryNodeRawEvent,
		EventKind:      "task_cycle_result",
		Summary:        "First valid",
		TaskID:         "task-log-middle",
		SessionID:      "sess-log-middle",
		TensionID:      "tension-log-middle",
		ProtoClusterID: "cluster-log-middle",
		SourceID:       "run-1",
	})
	second := encodeTestMemoryEventLine(t, LocalMemoryEvent{
		Sequence:       2,
		OccurredAt:     time.Date(2026, 3, 23, 20, 11, 0, 0, time.UTC).Format(time.RFC3339Nano),
		NodeType:       localMemoryNodeProcedure,
		EventKind:      "task_cycle_result",
		Summary:        "Second valid",
		TaskID:         "task-log-middle",
		SessionID:      "sess-log-middle",
		TensionID:      "tension-log-middle",
		ProtoClusterID: "cluster-log-middle",
		SourceID:       "run-2",
	})
	writeTestEpisodicLog(t, filepath.Join(base, "episodic.jsonl"), first, "{not-json", second)

	service := openTestAgentMemoryService(t, "ws-log-middle", "agent-log-middle")
	storeStats := service.storeStatsSnapshot()
	if storeStats.Episodes != 2 {
		t.Fatalf("expected middle-line salvage to keep later valid event, got %+v", storeStats)
	}
	data, err := os.ReadFile(service.logPath)
	if err != nil {
		t.Fatalf("ReadFile(salvaged middle log) error = %v", err)
	}
	if !strings.Contains(string(data), "First valid") || !strings.Contains(string(data), "Second valid") {
		t.Fatalf("expected salvaged log to keep both valid events, got %q", string(data))
	}
}

func TestOpenAgentMemoryServiceDedupesDuplicateReplaySequence(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	base := agentMemoryBasePath("ws-log-dedupe", "agent-log-dedupe")
	line := encodeTestMemoryEventLine(t, LocalMemoryEvent{
		Sequence:       7,
		OccurredAt:     time.Date(2026, 3, 23, 20, 20, 0, 0, time.UTC).Format(time.RFC3339Nano),
		NodeType:       localMemoryNodeRawMessage,
		EventKind:      "inbound_message",
		Summary:        "Only once",
		TaskID:         "task-log-dedupe",
		SessionID:      "sess-log-dedupe",
		TensionID:      "tension-log-dedupe",
		ProtoClusterID: "cluster-log-dedupe",
		SourceID:       "msg-7",
	})
	writeTestEpisodicLog(t, filepath.Join(base, "episodic.jsonl"), line, line)

	service := openTestAgentMemoryService(t, "ws-log-dedupe", "agent-log-dedupe")
	storeStats := service.storeStatsSnapshot()
	if storeStats.Episodes != 1 {
		t.Fatalf("expected duplicate replay sequence to be deduped, got %+v", storeStats)
	}
	data, err := os.ReadFile(service.logPath)
	if err != nil {
		t.Fatalf("ReadFile(deduped log) error = %v", err)
	}
	if got := strings.Count(strings.TrimSpace(string(data)), "\n") + 1; got != 1 {
		t.Fatalf("expected deduped log to keep one line, got %d in %q", got, string(data))
	}
}

func TestAgentMemoryServiceInvalidateRebuildsPacketFromFreshShadow(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws-8", "agent-8")
	if err := service.rememberDocVersions(map[string]string{"task.task-8": "sha-1"}); err != nil {
		t.Fatalf("rememberDocVersions() error = %v", err)
	}
	if err := service.appendEvent(LocalMemoryEvent{
		NodeType:       localMemoryNodeRawEvent,
		EventKind:      "task_cycle_result",
		Summary:        "Old digest should disappear",
		TaskID:         "task-8",
		SessionID:      "sess-8",
		TensionID:      "tension-8",
		ProtoClusterID: "cluster-8",
		DocKeys:        []string{"task.task-8"},
		Outcome:        "continue",
	}); err != nil {
		t.Fatalf("appendEvent() error = %v", err)
	}

	input := MemoryPacketInput{
		Task:    &WorkspaceTaskRecord{TaskID: "task-8", Title: "Task Eight", Status: "RUNNING", Priority: "HIGH"},
		Session: &AgentSessionStateRecord{SessionID: "sess-8", TaskID: "task-8", Status: "ACTIVE"},
		Focus:   &RuntimeFocusState{TaskID: "task-8", ProtoClusterID: "cluster-8", FocusTensionID: "tension-8"},
	}
	packetBefore := service.buildPacket(input, 4000)
	if !strings.Contains(packetBefore, "Task Digest: events=1") {
		t.Fatalf("expected packet to contain active task digest before invalidation, got %q", packetBefore)
	}

	if err := service.invalidate(LocalMemoryInvalidationInput{
		GuardChanges: []LocalMemoryGuardChange{{
			GuardType:      "doc_sha",
			Ref:            "task.task-8",
			CurrentVersion: "sha-2",
		}},
	}); err != nil {
		t.Fatalf("invalidate() error = %v", err)
	}

	packetAfter := service.buildPacket(input, 4000)
	if strings.Contains(packetAfter, "Task Digest: events=1") {
		t.Fatalf("expected stale digest to disappear after invalidate+rebuild, got %q", packetAfter)
	}
	if strings.Contains(packetAfter, "Open Loops") {
		t.Fatalf("expected packet to rebuild without stale digest shell, got %q", packetAfter)
	}
}

func TestAgentMemoryServiceBuildPacketDowngradesStalePersistentView(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws-stale-packet", "agent-stale-packet")
	now := time.Now().UTC()
	if err := service.store.UpsertEpisode(LocalMemoryEpisodeRecord{
		EpisodeID: "episode-stale-p2",
		Scope: LocalMemoryScope{
			TaskID:         "task-stale-p2",
			SessionID:      "sess-stale-p2",
			TensionID:      "tension-stale-p2",
			ProtoClusterID: "cluster-stale-p2",
		},
		Summary:   "stale persistent episode should not reach prompt",
		CreatedAt: now.Add(-2 * time.Minute).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("UpsertEpisode() error = %v", err)
	}
	if err := service.store.PutDigest(LocalMemoryDigestRecord{
		DigestID:        "task:task-stale-p2",
		Tier:            "P2",
		Kind:            "TASK_DIGEST",
		SourceEpisodeID: "episode-stale-p2",
		Scope: LocalMemoryScope{
			TaskID:         "task-stale-p2",
			SessionID:      "sess-stale-p2",
			TensionID:      "tension-stale-p2",
			ProtoClusterID: "cluster-stale-p2",
		},
		Summary:       "stale persistent digest should not reach prompt",
		Body:          "stale persistent digest body",
		Stale:         true,
		InvalidatedAt: now.Add(-1 * time.Minute).Format(time.RFC3339Nano),
		EpisodeDigest: &LocalEpisodeDigest{
			ScopeKind:      "task",
			ScopeKey:       "task-stale-p2",
			UpdatedAt:      now.Add(-2 * time.Minute).Format(time.RFC3339Nano),
			EventCount:     1,
			LastSummary:    "stale persistent digest should not reach prompt",
			OpenLoops:      []string{"old loop should not reach prompt"},
			ProtoClusterID: "cluster-stale-p2",
		},
	}); err != nil {
		t.Fatalf("PutDigest() error = %v", err)
	}

	input := MemoryPacketInput{
		Task:    &WorkspaceTaskRecord{TaskID: "task-stale-p2", Title: "Task Stale P2", Status: "RUNNING", Priority: "HIGH"},
		Session: &AgentSessionStateRecord{SessionID: "sess-stale-p2", TaskID: "task-stale-p2", Status: "ACTIVE"},
		Focus:   &RuntimeFocusState{TaskID: "task-stale-p2", ProtoClusterID: "cluster-stale-p2", FocusTensionID: "tension-stale-p2"},
	}
	packet := service.buildPacket(input, 4000)
	if !strings.Contains(packet, "### Freshness Guard") || !strings.Contains(packet, "Persistent P2 view: downgraded") {
		t.Fatalf("expected freshness guard in stale packet, got %q", packet)
	}
	if !strings.Contains(packet, "task:task-stale-p2") {
		t.Fatalf("expected stale digest evidence in packet, got %q", packet)
	}
	if strings.Contains(packet, "stale persistent episode should not reach prompt") ||
		strings.Contains(packet, "stale persistent digest should not reach prompt") ||
		strings.Contains(packet, "old loop should not reach prompt") {
		t.Fatalf("expected stale persistent memory to be suppressed, got %q", packet)
	}
	stats := service.statsSnapshot()
	if stats.StaleHits != 1 || stats.ConsecutiveStaleReads != 1 || stats.P2Misses == 0 {
		t.Fatalf("expected stale P2 read to be tracked as degraded miss, got %+v", stats)
	}
	if entries := service.controlSnapshot().PacketCacheEntries; entries != 0 {
		t.Fatalf("expected stale downgraded packet not to be cached, got %d entries", entries)
	}

	_ = service.buildPacket(input, 4000)
	stats = service.statsSnapshot()
	if stats.StaleHits != 2 || stats.ConsecutiveStaleReads != 2 {
		t.Fatalf("expected repeated stale reads to bypass cache and remain visible, got %+v", stats)
	}
}

func TestAgentMemoryServicePacketCacheObservesExternallyStalePersistentView(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws-external-stale-cache", "agent-external-stale-cache")
	now := time.Now().UTC()
	if err := service.store.PutDigest(LocalMemoryDigestRecord{
		DigestID: "task:task-cache-stale",
		Tier:     "P2",
		Kind:     "TASK_DIGEST",
		Scope: LocalMemoryScope{
			TaskID:         "task-cache-stale",
			SessionID:      "sess-cache-stale",
			TensionID:      "tension-cache-stale",
			ProtoClusterID: "cluster-cache-stale",
		},
		Summary: "cached persistent digest must not survive stale mark",
		Body:    "cached persistent digest body",
		EpisodeDigest: &LocalEpisodeDigest{
			ScopeKind:      "task",
			ScopeKey:       "task-cache-stale",
			UpdatedAt:      now.Add(-2 * time.Minute).Format(time.RFC3339Nano),
			EventCount:     1,
			LastSummary:    "cached persistent digest must not survive stale mark",
			ProtoClusterID: "cluster-cache-stale",
		},
	}); err != nil {
		t.Fatalf("PutDigest() error = %v", err)
	}

	input := MemoryPacketInput{
		Task:    &WorkspaceTaskRecord{TaskID: "task-cache-stale", Title: "Task Cache Stale", Status: "RUNNING", Priority: "HIGH"},
		Session: &AgentSessionStateRecord{SessionID: "sess-cache-stale", TaskID: "task-cache-stale", Status: "ACTIVE"},
		Focus:   &RuntimeFocusState{TaskID: "task-cache-stale", ProtoClusterID: "cluster-cache-stale", FocusTensionID: "tension-cache-stale"},
	}
	before := service.buildPacket(input, 4000)
	if !strings.Contains(before, "cached persistent digest must not survive stale mark") {
		t.Fatalf("expected baseline packet to use fresh persistent digest, got %q", before)
	}
	if entries := service.controlSnapshot().PacketCacheEntries; entries != 1 {
		t.Fatalf("expected baseline packet to be cached, got %d entries", entries)
	}

	if _, err := service.store.Invalidate(LocalMemoryInvalidationInput{
		TaskIDs: []string{"task-cache-stale"},
		Reasons: []string{"external stale mark"},
	}); err != nil {
		t.Fatalf("external store invalidate: %v", err)
	}
	after := service.buildPacket(input, 4000)
	if !strings.Contains(after, "### Freshness Guard") || !strings.Contains(after, "task:task-cache-stale") {
		t.Fatalf("expected stale store update to bypass cached prompt and surface freshness guard, got %q", after)
	}
	if strings.Contains(after, "cached persistent digest must not survive stale mark") {
		t.Fatalf("expected externally stale digest to be suppressed despite old cache, got %q", after)
	}
	stats := service.statsSnapshot()
	if stats.P1Hits != 0 || stats.P1Misses < 2 || stats.StaleHits != 1 {
		t.Fatalf("expected external stale update to invalidate packet cache before hit, got %+v", stats)
	}
}

func TestAgentMemoryServiceTracksArtifactVersionsInPacketAndPersistentGuards(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws-9", "agent-9")
	if err := service.rememberArtifactVersions(map[string]string{"doc:deliverable.brief": "artifact-1234567890abcdef"}); err != nil {
		t.Fatalf("rememberArtifactVersions() error = %v", err)
	}
	if err := service.appendEvent(LocalMemoryEvent{
		NodeType:       localMemoryNodeArtifactDelta,
		EventKind:      "task_cycle_result",
		Summary:        "Published deliverable brief",
		TaskID:         "task-9",
		SessionID:      "sess-9",
		TensionID:      "tension-9",
		ProtoClusterID: "cluster-9",
		ArtifactRefs:   []string{"doc:deliverable.brief"},
		Outcome:        "completed",
	}); err != nil {
		t.Fatalf("appendEvent() error = %v", err)
	}

	packet := service.buildPacket(MemoryPacketInput{
		Task:      &WorkspaceTaskRecord{TaskID: "task-9", Title: "Task Nine", Status: "RUNNING", Priority: "HIGH"},
		Session:   &AgentSessionStateRecord{SessionID: "sess-9", TaskID: "task-9", Status: "ACTIVE"},
		Focus:     &RuntimeFocusState{TaskID: "task-9", ProtoClusterID: "cluster-9", FocusTensionID: "tension-9"},
		Hydration: &TaskHydrationBundle{Artifacts: []WorkspaceArtifactRecord{{ArtifactID: "artifact-1234567890abcdef", ArtifactRef: "doc:deliverable.brief"}}},
	}, 4000)
	if !strings.Contains(packet, "Current Artifact Versions") {
		t.Fatalf("expected packet to include current artifact versions, got %q", packet)
	}
	if !strings.Contains(packet, "doc:deliverable.brief @ artifact-1234567") {
		t.Fatalf("expected packet to show abbreviated artifact version, got %q", packet)
	}

	snapshot := service.store.Snapshot()
	foundArtifactGuard := false
	for _, digest := range snapshot.Digests {
		for _, guard := range digest.Guards {
			if guard.GuardType == "artifact_version" && guard.Ref == "doc:deliverable.brief" && guard.Version == "artifact-1234567890abcdef" {
				foundArtifactGuard = true
			}
		}
	}
	if !foundArtifactGuard {
		t.Fatalf("expected persistent digests to carry artifact_version guard, got %+v", snapshot.Digests)
	}
}

func TestAgentMemoryServiceDoesNotPromoteSingleAnecdoteToProcedure(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws-10", "agent-10")
	if err := service.appendEvent(LocalMemoryEvent{
		NodeType:       localMemoryNodeArtifactDelta,
		EventKind:      "task_cycle_result",
		Summary:        "Prepare deliverable brief before sync",
		Details:        "Prepare deliverable brief before sync",
		TaskID:         "task-10",
		SessionID:      "sess-10",
		TensionID:      "tension-10",
		ProtoClusterID: "cluster-10",
		DocKeys:        []string{"task.task-10"},
		Outcome:        "completed",
		MetadataJSON: encodeTaskResultMemoryMetadata(StructuredTaskResult{
			Outcome:    "completed",
			NextAction: "Prepare deliverable brief before sync",
			Materialize: TaskMaterialization{
				DocKey: "task.task-10",
			},
		}),
	}); err != nil {
		t.Fatalf("appendEvent() error = %v", err)
	}

	packet := service.buildPacket(MemoryPacketInput{
		Task:    &WorkspaceTaskRecord{TaskID: "task-10", Title: "Task Ten", Status: "RUNNING", Priority: "HIGH"},
		Session: &AgentSessionStateRecord{SessionID: "sess-10", TaskID: "task-10", Status: "ACTIVE"},
		Focus:   &RuntimeFocusState{TaskID: "task-10", ProtoClusterID: "cluster-10", FocusTensionID: "tension-10"},
	}, 4000)
	if strings.Contains(packet, "Repeatedly Works Here:") {
		t.Fatalf("expected single anecdote to stay out of procedural memory, got %q", packet)
	}
	if got := service.statsSnapshot().Procedures; got != 0 {
		t.Fatalf("expected zero procedures after single anecdote, got %d", got)
	}
}

func TestAgentMemoryServiceBuildPacketInjectsRepeatedProcedures(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws-11", "agent-11")
	for idx := 0; idx < 2; idx++ {
		if err := service.appendEvent(LocalMemoryEvent{
			NodeType:       localMemoryNodeArtifactDelta,
			EventKind:      "task_cycle_result",
			Summary:        "Prepare deliverable brief before sync",
			Details:        "Prepare deliverable brief before sync",
			TaskID:         "task-11",
			SessionID:      "sess-11",
			TensionID:      "tension-11",
			ProtoClusterID: "cluster-11",
			DocKeys:        []string{"task.task-11"},
			ArtifactRefs:   []string{"doc:deliverable.brief"},
			Outcome:        "completed",
			MetadataJSON: encodeTaskResultMemoryMetadata(StructuredTaskResult{
				Outcome:    "completed",
				NextAction: "Prepare deliverable brief before sync",
				Materialize: TaskMaterialization{
					DocKey: "task.task-11",
				},
			}),
		}); err != nil {
			t.Fatalf("appendEvent(%d) error = %v", idx, err)
		}
	}

	packet := service.buildPacket(MemoryPacketInput{
		Task:      &WorkspaceTaskRecord{TaskID: "task-11", Title: "Task Eleven", Status: "RUNNING", Priority: "HIGH"},
		Session:   &AgentSessionStateRecord{SessionID: "sess-11", TaskID: "task-11", Status: "ACTIVE"},
		Focus:     &RuntimeFocusState{TaskID: "task-11", ProtoClusterID: "cluster-11", FocusTensionID: "tension-11"},
		Hydration: &TaskHydrationBundle{Artifacts: []WorkspaceArtifactRecord{{ArtifactRef: "doc:deliverable.brief", ArtifactID: "artifact-11"}}},
	}, 4000)
	if !strings.Contains(packet, "Repeatedly Works Here:") || !strings.Contains(packet, "Prepare deliverable brief before sync") {
		t.Fatalf("expected repeated procedure hint in packet, got %q", packet)
	}
	if got := service.statsSnapshot().Procedures; got != 1 {
		t.Fatalf("expected exactly one derived procedure, got %d", got)
	}
}

func TestAgentMemoryServiceBuildPacketInjectsRepeatedAntiProcedures(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws-12", "agent-12")
	for idx := 0; idx < 2; idx++ {
		if err := service.appendEvent(LocalMemoryEvent{
			NodeType:       localMemoryNodeBlocker,
			EventKind:      "task_cycle_result",
			Summary:        "Retrying planner before refreshing scratch",
			Details:        "Retrying planner before refreshing scratch",
			TaskID:         "task-12",
			SessionID:      "sess-12",
			TensionID:      "tension-12",
			ProtoClusterID: "cluster-12",
			BlockerKinds:   []string{"runtime"},
			Outcome:        "blocked",
			MetadataJSON: encodeTaskResultMemoryMetadata(StructuredTaskResult{
				Outcome:    "blocked",
				NextAction: "Retry planner before refreshing scratch",
				BlockedOn:  []BlockedRef{{Kind: "runtime", Detail: "scratch is stale"}},
			}),
		}); err != nil {
			t.Fatalf("appendEvent(%d) error = %v", idx, err)
		}
	}

	packet := service.buildPacket(MemoryPacketInput{
		Task:    &WorkspaceTaskRecord{TaskID: "task-12", Title: "Task Twelve", Status: "RUNNING", Priority: "HIGH"},
		Session: &AgentSessionStateRecord{SessionID: "sess-12", TaskID: "task-12", Status: "ACTIVE"},
		Focus:   &RuntimeFocusState{TaskID: "task-12", ProtoClusterID: "cluster-12", FocusTensionID: "tension-12"},
	}, 4000)
	if !strings.Contains(packet, "Avoid Repeating:") || !strings.Contains(packet, "runtime") {
		t.Fatalf("expected repeated anti-procedure hint in packet, got %q", packet)
	}
	if got := service.statsSnapshot().AntiProcedures; got != 1 {
		t.Fatalf("expected exactly one derived anti-procedure, got %d", got)
	}
}

func TestAgentMemoryServiceSkipsExternalHumanGateAntiProcedures(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws-13", "agent-13")
	for idx := 0; idx < 2; idx++ {
		if err := service.appendEvent(LocalMemoryEvent{
			NodeType:       localMemoryNodeBlocker,
			EventKind:      "task_cycle_result",
			Summary:        "Waiting for OAuth login",
			Details:        "Waiting for OAuth login",
			TaskID:         "task-13",
			SessionID:      "sess-13",
			TensionID:      "tension-13",
			ProtoClusterID: "cluster-13",
			BlockerKinds:   []string{"credential"},
			Outcome:        "blocked",
			RequiresHuman:  true,
			MetadataJSON: encodeTaskResultMemoryMetadata(StructuredTaskResult{
				Outcome:       "blocked",
				OwnerAction:   "Complete OAuth login",
				HumanReason:   "Interactive credential flow is required",
				RequiresHuman: true,
				BlockedOn:     []BlockedRef{{Kind: "credential", Detail: "Interactive OAuth login"}},
			}),
		}); err != nil {
			t.Fatalf("appendEvent(%d) error = %v", idx, err)
		}
	}

	packet := service.buildPacket(MemoryPacketInput{
		Task:    &WorkspaceTaskRecord{TaskID: "task-13", Title: "Task Thirteen", Status: "RUNNING", Priority: "HIGH"},
		Session: &AgentSessionStateRecord{SessionID: "sess-13", TaskID: "task-13", Status: "ACTIVE"},
		Focus:   &RuntimeFocusState{TaskID: "task-13", ProtoClusterID: "cluster-13", FocusTensionID: "tension-13"},
	}, 4000)
	if strings.Contains(packet, "Avoid Repeating:") {
		t.Fatalf("expected human auth gate to stay out of anti-procedural memory, got %q", packet)
	}
	if got := service.statsSnapshot().AntiProcedures; got != 0 {
		t.Fatalf("expected zero anti-procedures for external human gates, got %d", got)
	}
}

func TestAgentMemoryServiceRebuildsProceduresFromPersistentStoreOnRestart(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws-14", "agent-14")
	if err := service.appendEvent(LocalMemoryEvent{
		NodeType:       localMemoryNodeProcedure,
		EventKind:      "task_cycle_result",
		Summary:        "Publish delta after local verification",
		Details:        "Publish delta after local verification",
		TaskID:         "task-14",
		SessionID:      "sess-14",
		TensionID:      "tension-14",
		ProtoClusterID: "cluster-14",
		DocKeys:        []string{"task.task-14"},
		Outcome:        "completed",
		MetadataJSON: encodeTaskResultMemoryMetadata(StructuredTaskResult{
			Outcome:    "completed",
			MemoryType: "PROCEDURE",
			NextAction: "Publish delta after local verification",
		}),
	}); err != nil {
		t.Fatalf("appendEvent() error = %v", err)
	}
	if err := os.Remove(service.statePath); err != nil {
		t.Fatalf("Remove(service_state) error = %v", err)
	}

	reloaded := openTestAgentMemoryService(t, "ws-14", "agent-14")
	packet := reloaded.buildPacket(MemoryPacketInput{
		Task:    &WorkspaceTaskRecord{TaskID: "task-14", Title: "Task Fourteen", Status: "RUNNING", Priority: "HIGH"},
		Session: &AgentSessionStateRecord{SessionID: "sess-14", TaskID: "task-14", Status: "ACTIVE"},
		Focus:   &RuntimeFocusState{TaskID: "task-14", ProtoClusterID: "cluster-14", FocusTensionID: "tension-14"},
	}, 4000)
	if !strings.Contains(packet, "Repeatedly Works Here:") || !strings.Contains(packet, "Publish delta after local verification") {
		t.Fatalf("expected restarted service to rebuild procedural memory, got %q", packet)
	}
	if got := reloaded.statsSnapshot().Procedures; got != 1 {
		t.Fatalf("expected one rebuilt procedure after restart, got %d", got)
	}
}

func TestAgentMemoryServiceProcedureSelectionDoesNotLeakAcrossTasks(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws-15", "agent-15")
	for idx := 0; idx < 2; idx++ {
		if err := service.appendEvent(LocalMemoryEvent{
			NodeType:       localMemoryNodeArtifactDelta,
			EventKind:      "task_cycle_result",
			Summary:        "Stabilize deliverable before sync",
			Details:        "Stabilize deliverable before sync",
			TaskID:         "task-A",
			SessionID:      "sess-A",
			TensionID:      "tension-shared",
			ProtoClusterID: "cluster-shared",
			ArtifactRefs:   []string{"doc:deliverable.shared"},
			Outcome:        "completed",
			MetadataJSON: encodeTaskResultMemoryMetadata(StructuredTaskResult{
				Outcome:    "completed",
				NextAction: "Stabilize deliverable before sync",
			}),
		}); err != nil {
			t.Fatalf("appendEvent(%d) error = %v", idx, err)
		}
	}

	packet := service.buildPacket(MemoryPacketInput{
		Task:      &WorkspaceTaskRecord{TaskID: "task-B", Title: "Task Bee", Status: "RUNNING", Priority: "HIGH"},
		Session:   &AgentSessionStateRecord{SessionID: "sess-B", TaskID: "task-B", Status: "ACTIVE"},
		Focus:     &RuntimeFocusState{TaskID: "task-B", ProtoClusterID: "cluster-shared", FocusTensionID: "tension-shared"},
		Hydration: &TaskHydrationBundle{Artifacts: []WorkspaceArtifactRecord{{ArtifactRef: "doc:deliverable.shared", ArtifactID: "artifact-B"}}},
	}, 4000)
	if strings.Contains(packet, "Repeatedly Works Here:") {
		t.Fatalf("expected task-local procedure not to leak across tasks, got %q", packet)
	}
}

func TestAgentMemoryServiceRebuildsAntiProceduresFromPersistentStoreOnRestart(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws-16", "agent-16")
	if err := service.appendEvent(LocalMemoryEvent{
		NodeType:       localMemoryNodeAntiProcedure,
		EventKind:      "task_cycle_result",
		Summary:        "Retrying stale planner loop",
		Details:        "Retrying stale planner loop",
		TaskID:         "task-16",
		SessionID:      "sess-16",
		TensionID:      "tension-16",
		ProtoClusterID: "cluster-16",
		BlockerKinds:   []string{"runtime"},
		Outcome:        "blocked",
		MetadataJSON: encodeTaskResultMemoryMetadata(StructuredTaskResult{
			Outcome:    "blocked",
			MemoryType: "ANTI_PROCEDURE",
			NextAction: "Retry stale planner loop",
			BlockedOn:  []BlockedRef{{Kind: "runtime", Detail: "stale scratch state"}},
		}),
	}); err != nil {
		t.Fatalf("appendEvent() error = %v", err)
	}
	if err := os.Remove(service.statePath); err != nil {
		t.Fatalf("Remove(service_state) error = %v", err)
	}

	reloaded := openTestAgentMemoryService(t, "ws-16", "agent-16")
	packet := reloaded.buildPacket(MemoryPacketInput{
		Task:    &WorkspaceTaskRecord{TaskID: "task-16", Title: "Task Sixteen", Status: "RUNNING", Priority: "HIGH"},
		Session: &AgentSessionStateRecord{SessionID: "sess-16", TaskID: "task-16", Status: "ACTIVE"},
		Focus:   &RuntimeFocusState{TaskID: "task-16", ProtoClusterID: "cluster-16", FocusTensionID: "tension-16"},
	}, 4000)
	if !strings.Contains(packet, "Avoid Repeating:") || !strings.Contains(packet, "Retrying stale planner loop") {
		t.Fatalf("expected restarted service to rebuild anti-procedural memory, got %q", packet)
	}
	if got := reloaded.statsSnapshot().AntiProcedures; got != 1 {
		t.Fatalf("expected one rebuilt anti-procedure after restart, got %d", got)
	}
}

func TestAgentMemoryServiceInvalidateDropsStaleProceduralHints(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws-17", "agent-17")
	if err := service.rememberDocVersions(map[string]string{"task.task-17": "sha-1"}); err != nil {
		t.Fatalf("rememberDocVersions() error = %v", err)
	}
	for idx := 0; idx < 2; idx++ {
		if err := service.appendEvent(LocalMemoryEvent{
			NodeType:       localMemoryNodeArtifactDelta,
			EventKind:      "task_cycle_result",
			Summary:        "Prepare proof before publishing",
			Details:        "Prepare proof before publishing",
			TaskID:         "task-17",
			SessionID:      "sess-17",
			TensionID:      "tension-17",
			ProtoClusterID: "cluster-17",
			DocKeys:        []string{"task.task-17"},
			Outcome:        "completed",
			MetadataJSON: encodeTaskResultMemoryMetadata(StructuredTaskResult{
				Outcome:    "completed",
				NextAction: "Prepare proof before publishing",
				Materialize: TaskMaterialization{
					DocKey: "task.task-17",
				},
			}),
		}); err != nil {
			t.Fatalf("appendEvent(%d) error = %v", idx, err)
		}
	}

	input := MemoryPacketInput{
		Task:    &WorkspaceTaskRecord{TaskID: "task-17", Title: "Task Seventeen", Status: "RUNNING", Priority: "HIGH"},
		Session: &AgentSessionStateRecord{SessionID: "sess-17", TaskID: "task-17", Status: "ACTIVE"},
		Focus:   &RuntimeFocusState{TaskID: "task-17", ProtoClusterID: "cluster-17", FocusTensionID: "tension-17"},
	}
	packetBefore := service.buildPacket(input, 4000)
	if !strings.Contains(packetBefore, "Repeatedly Works Here:") {
		t.Fatalf("expected procedural hint before invalidation, got %q", packetBefore)
	}

	if err := service.invalidate(buildCanonicalVersionInvalidation(
		map[string]string{"task.task-17": "sha-2"},
		map[string]string{},
		map[string]string{"task.task-17": "sha-1"},
		map[string]string{},
	)); err != nil {
		t.Fatalf("invalidate() error = %v", err)
	}

	packetAfter := service.buildPacket(input, 4000)
	if strings.Contains(packetAfter, "Repeatedly Works Here:") || strings.Contains(packetAfter, "Prepare proof before publishing") {
		t.Fatalf("expected stale procedural hint to disappear after canonical drift, got %q", packetAfter)
	}
	if got := service.statsSnapshot().Procedures; got != 0 {
		t.Fatalf("expected procedural memory to drop after invalidation, got %d", got)
	}
}
