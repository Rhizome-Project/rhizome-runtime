package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func openTestLocalMemoryStore(t *testing.T, workspaceID, agentID string) *LocalMemoryStore {
	t.Helper()
	store, err := OpenLocalMemoryStore(workspaceID, agentID)
	if err != nil {
		t.Fatalf("OpenLocalMemoryStore() error = %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func queryLocalMemoryDigestRow(t *testing.T, db *sql.DB, digestID string) (int64, string) {
	t.Helper()
	var rowID int64
	var lastAccessed sql.NullString
	if err := db.QueryRow(`SELECT rowid, last_accessed_at FROM local_memory_digests WHERE digest_id = ?`, digestID).Scan(&rowID, &lastAccessed); err != nil {
		t.Fatalf("query digest row %q: %v", digestID, err)
	}
	return rowID, lastAccessed.String
}

func hasLocalMemoryID(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestLocalMemoryStoreRoundTripPersistsEpisodesAndDigests(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store := openTestLocalMemoryStore(t, "ws-1", "agent-1")
	if err := store.UpsertEpisode(LocalMemoryEpisodeRecord{
		EpisodeID: "episode-1",
		Scope: LocalMemoryScope{
			TaskID:         "task-1",
			SessionID:      "session-1",
			TensionID:      "tension-1",
			ProtoClusterID: "cluster-1",
		},
		RunID:        "run-1",
		Trigger:      "work.next",
		Outcome:      "continued",
		Summary:      "Resumed cluster-local execution",
		DigestRefs:   []string{"digest-p1", "digest-p2"},
		DocKeys:      []string{"task.task-1", "decisions"},
		ArtifactRefs: []string{"artifact://doc/1"},
	}); err != nil {
		t.Fatalf("UpsertEpisode() error = %v", err)
	}
	if err := store.PutDigest(LocalMemoryDigestRecord{
		DigestID: "digest-p1",
		Tier:     "P1",
		Kind:     "KERNEL_PACKET",
		Scope: LocalMemoryScope{
			TaskID:         "task-1",
			TensionID:      "tension-1",
			ProtoClusterID: "cluster-1",
		},
		Summary:      "Hot kernel packet",
		Body:         "constraints + blockers + active dissent",
		DocKeys:      []string{"task.task-1", "handoff"},
		ArtifactRefs: []string{"artifact://doc/1"},
		Guards: []LocalMemoryGuard{
			{GuardType: "doc_sha", Ref: "task.task-1", Version: "sha-1"},
		},
	}); err != nil {
		t.Fatalf("PutDigest(p1) error = %v", err)
	}
	if err := store.PutDigest(LocalMemoryDigestRecord{
		DigestID: "digest-p2",
		Tier:     "P2",
		Kind:     "EPISODE_DIGEST",
		Scope: LocalMemoryScope{
			TaskID:         "task-1",
			ProtoClusterID: "cluster-1",
		},
		SourceEpisodeID: "episode-1",
		Summary:         "Longer episodic digest",
		Body:            "Decision ledger and artifact delta chain",
	}); err != nil {
		t.Fatalf("PutDigest(p2) error = %v", err)
	}

	path := localMemoryStorePath("ws-1", "agent-1")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected memory state at %s: %v", path, err)
	}
	if got := filepath.Dir(path); got != localMemoryStoreRootPath("ws-1", "agent-1") {
		t.Fatalf("unexpected memory root %q", got)
	}

	reloaded := openTestLocalMemoryStore(t, "ws-1", "agent-1")
	state := reloaded.Snapshot()
	if len(state.Episodes) != 1 || len(state.Digests) != 2 {
		t.Fatalf("unexpected state sizes: %+v", state)
	}
	if state.Episodes[0].Scope.ProtoClusterID != "cluster-1" {
		t.Fatalf("expected cluster anchor to persist, got %+v", state.Episodes[0])
	}
	stats := reloaded.Stats()
	if stats.Episodes != 1 || stats.Digests != 2 || stats.P1Digests != 1 || stats.P2Digests != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestLocalMemoryStoreNormalizesCanonicalSchemaFields(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store := openTestLocalMemoryStore(t, "ws-schema", "agent-schema")
	episode := LocalMemoryEpisodeRecord{
		EpisodeID: "episode-schema",
		Scope: LocalMemoryScope{
			TaskID:         "task-schema",
			SessionID:      "session-schema",
			TensionID:      "tension-schema",
			ProtoClusterID: "cluster-schema",
		},
		Trigger:        "session_decision_needed",
		Outcome:        "blocked",
		Summary:        "Blocked by credential gate",
		ConstraintRefs: []string{"constraint-1"},
		DocKeys:        []string{"doc-a"},
		ArtifactRefs:   []string{"artifact-a"},
	}
	if err := store.UpsertEpisode(episode); err != nil {
		t.Fatalf("UpsertEpisode() error = %v", err)
	}
	if err := store.PutDigest(LocalMemoryDigestRecord{
		DigestID: "digest-schema",
		Tier:     "P2",
		Kind:     "TASK_DIGEST",
		Scope: LocalMemoryScope{
			TaskID:         "task-schema",
			SessionID:      "session-schema",
			TensionID:      "tension-schema",
			ProtoClusterID: "cluster-schema",
		},
		Summary:       "Decision memo",
		Body:          "Decision memo body",
		InvalidatedAt: time.Date(2026, 3, 26, 8, 20, 0, 0, time.UTC).Format(time.RFC3339Nano),
		DocKeys:       []string{"doc-a"},
		ArtifactRefs:  []string{"artifact-a"},
	}); err != nil {
		t.Fatalf("PutDigest() error = %v", err)
	}
	if err := store.PutPromotion(context.Background(), LocalPromotionCandidate{
		CandidateID:    "candidate-schema-2",
		NodeType:       localMemoryNodeDecision,
		MemoryType:     "DECISION",
		Title:          "Decision candidate",
		Body:           "Decision body",
		Summary:        "Decision candidate",
		TaskID:         "task-schema",
		SessionID:      "session-schema",
		TensionID:      "tension-schema",
		ProtoClusterID: "cluster-schema",
		ArtifactRefs:   []string{"artifact-a"},
		DocKeys:        []string{"doc-a"},
	}); err != nil {
		t.Fatalf("PutPromotion(candidate) error = %v", err)
	}

	pending, err := store.PendingPromotions(context.Background(), 10)
	if err != nil {
		t.Fatalf("PendingPromotions() error = %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected one pending promotion, got %+v", pending)
	}
	if pending[0].ClaimModality != localMemoryClaimModalityDecided || pending[0].WriteState != localMemoryWriteStateCandidate {
		t.Fatalf("expected canonical promotion state, got %+v", pending[0])
	}
	var anchors struct {
		TaskID         string   `json:"task_id"`
		SessionID      string   `json:"session_id"`
		TensionID      string   `json:"tension_id"`
		ProtoClusterID string   `json:"proto_cluster_id"`
		DocKeys        []string `json:"doc_keys"`
		ArtifactRefs   []string `json:"artifact_refs"`
	}
	if err := json.Unmarshal([]byte(pending[0].AnchorsJSON), &anchors); err != nil {
		t.Fatalf("decode pending anchors: %v", err)
	}
	if anchors.TaskID != "task-schema" || anchors.ProtoClusterID != "cluster-schema" || len(anchors.DocKeys) != 1 || len(anchors.ArtifactRefs) != 1 {
		t.Fatalf("unexpected pending anchors: %+v", anchors)
	}

	reloaded := openTestLocalMemoryStore(t, "ws-schema", "agent-schema")
	state := reloaded.Snapshot()
	if len(state.Episodes) != 1 || len(state.Digests) != 1 {
		t.Fatalf("unexpected normalized state: %+v", state)
	}
	if state.Episodes[0].ClaimModality != localMemoryClaimModalityConstrained || state.Episodes[0].WriteState != localMemoryWriteStatePromoted {
		t.Fatalf("unexpected normalized episode schema: %+v", state.Episodes[0])
	}
	if state.Digests[0].ClaimModality != localMemoryClaimModalityDecided || state.Digests[0].WriteState != localMemoryWriteStateSuperseded {
		t.Fatalf("unexpected normalized digest schema: %+v", state.Digests[0])
	}
	if state.Episodes[0].AnchorsJSON == "" || state.Digests[0].AnchorsJSON == "" {
		t.Fatalf("expected anchors to be materialized, got episode=%q digest=%q", state.Episodes[0].AnchorsJSON, state.Digests[0].AnchorsJSON)
	}
}

func TestLocalMemoryStoreInvalidateByLocusRefsAndGuards(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store := openTestLocalMemoryStore(t, "ws-1", "agent-1")
	if err := store.UpsertEpisode(LocalMemoryEpisodeRecord{
		EpisodeID: "episode-a",
		Scope: LocalMemoryScope{
			TaskID:         "task-1",
			TensionID:      "tension-1",
			ProtoClusterID: "cluster-1",
		},
		DocKeys: []string{"task.task-1"},
	}); err != nil {
		t.Fatalf("UpsertEpisode() error = %v", err)
	}
	if err := store.UpsertEpisode(LocalMemoryEpisodeRecord{
		EpisodeID: "episode-b",
		Scope: LocalMemoryScope{
			TaskID:         "task-2",
			ProtoClusterID: "cluster-2",
		},
		DocKeys: []string{"task.task-2"},
	}); err != nil {
		t.Fatalf("UpsertEpisode() other error = %v", err)
	}
	if err := store.PutDigest(LocalMemoryDigestRecord{
		DigestID: "digest-a",
		Tier:     "P1",
		Kind:     "KERNEL_PACKET",
		Scope: LocalMemoryScope{
			TaskID:         "task-1",
			TensionID:      "tension-1",
			ProtoClusterID: "cluster-1",
		},
		DocKeys: []string{"task.task-1"},
		Guards: []LocalMemoryGuard{
			{GuardType: "doc_sha", Ref: "task.task-1", Version: "sha-1"},
		},
	}); err != nil {
		t.Fatalf("PutDigest(digest-a) error = %v", err)
	}
	if err := store.PutDigest(LocalMemoryDigestRecord{
		DigestID: "digest-b",
		Tier:     "P2",
		Kind:     "EPISODE_DIGEST",
		Scope: LocalMemoryScope{
			TaskID:         "task-2",
			ProtoClusterID: "cluster-2",
		},
		DocKeys: []string{"task.task-2"},
	}); err != nil {
		t.Fatalf("PutDigest(digest-b) error = %v", err)
	}

	result, err := store.Invalidate(LocalMemoryInvalidationInput{
		Reasons:         []string{"artifact changed"},
		ProtoClusterIDs: []string{"cluster-1"},
		DocKeys:         []string{"task.task-1"},
		GuardChanges: []LocalMemoryGuardChange{
			{GuardType: "doc_sha", Ref: "task.task-1", CurrentVersion: "sha-2"},
		},
	})
	if err != nil {
		t.Fatalf("Invalidate() error = %v", err)
	}
	if result.DigestsInvalidated != 1 || result.EpisodesSuperseded != 1 {
		t.Fatalf("unexpected invalidation result: %+v", result)
	}
	if len(result.DigestIDs) != 1 || result.DigestIDs[0] != "digest-a" {
		t.Fatalf("unexpected invalidated digests: %+v", result.DigestIDs)
	}
	if len(result.EpisodeIDs) != 1 || result.EpisodeIDs[0] != "episode-a" {
		t.Fatalf("unexpected superseded episodes: %+v", result.EpisodeIDs)
	}

	state := store.Snapshot()
	if !state.Digests[0].Stale || state.Digests[0].InvalidatedAt == "" {
		t.Fatalf("expected digest-a to be marked stale, got %+v", state.Digests[0])
	}
	if state.Episodes[0].SupersededAt == "" {
		t.Fatalf("expected episode-a to be superseded, got %+v", state.Episodes[0])
	}
	if state.Digests[1].Stale || state.Episodes[1].SupersededAt != "" {
		t.Fatalf("expected unrelated records to remain active, got %+v %+v", state.Digests[1], state.Episodes[1])
	}
}

func TestLocalMemoryStoreMarkDigestAccessedPersistsTimestamp(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store := openTestLocalMemoryStore(t, "ws-1", "agent-1")
	if err := store.PutDigest(LocalMemoryDigestRecord{
		DigestID: "digest-a",
		Tier:     "P1",
		Kind:     "KERNEL_PACKET",
		Summary:  "Hot packet",
	}); err != nil {
		t.Fatalf("PutDigest() error = %v", err)
	}
	accessedAt := time.Date(2026, 3, 23, 14, 0, 0, 0, time.UTC)
	if err := store.MarkDigestAccessed("digest-a", accessedAt); err != nil {
		t.Fatalf("MarkDigestAccessed() error = %v", err)
	}
	reloaded := openTestLocalMemoryStore(t, "ws-1", "agent-1")
	state := reloaded.Snapshot()
	if got := state.Digests[0].LastAccessedAt; got != accessedAt.Format(time.RFC3339Nano) {
		t.Fatalf("unexpected last_accessed_at %q", got)
	}
}

func TestOpenLocalMemoryStoreQuarantinesCorruptState(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	path := localMemoryStorePath("ws-1", "agent-1")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(path, []byte("{not-json"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	store := openTestLocalMemoryStore(t, "ws-1", "agent-1")
	if store == nil {
		t.Fatal("expected store to be returned after corrupt state quarantine")
	}
	stats := store.Stats()
	if stats.Episodes != 0 || stats.Digests != 0 {
		t.Fatalf("expected empty store after quarantine, got %+v", stats)
	}
	matches, err := filepath.Glob(path + ".corrupt-*")
	if err != nil {
		t.Fatalf("Glob() error = %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected one quarantined memory file, got %#v", matches)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected clean sqlite db to be recreated, got err=%v", err)
	}
}

func TestOpenLocalMemoryStoreMigratesLegacyJSONState(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	legacyPath := localMemoryLegacyStorePath("ws-legacy", "agent-legacy")
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	legacyState := `{
	  "version": 1,
	  "workspace_id": "ws-legacy",
	  "agent_id": "agent-legacy",
	  "updated_at": "2026-03-23T10:00:00Z",
	  "episodes": [{
	    "episode_id": "episode-legacy",
	    "scope": {
	      "task_id": "task-legacy",
	      "session_id": "session-legacy",
	      "tension_id": "tension-legacy",
	      "proto_cluster_id": "cluster-legacy"
	    },
	    "trigger": "task_cycle_result",
	    "outcome": "continue",
	    "summary": "Legacy continuity"
	  }],
	  "digests": [{
	    "digest_id": "task:task-legacy",
	    "tier": "P2",
	    "kind": "TASK_DIGEST",
	    "scope": {
	      "task_id": "task-legacy",
	      "session_id": "session-legacy",
	      "tension_id": "tension-legacy",
	      "proto_cluster_id": "cluster-legacy"
	    },
	    "summary": "Legacy digest",
	    "guards": [{"guard_type":"doc_sha","ref":"task.task-legacy","version":"sha-legacy"}],
	    "episode_digest": {
	      "scope_key": "task-legacy",
	      "scope_kind": "task",
	      "updated_at": "2026-03-23T10:00:00Z",
	      "event_count": 4,
	      "message_count": 1,
	      "last_summary": "Legacy digest",
	      "latest_session_id": "session-legacy",
	      "latest_tension_id": "tension-legacy",
	      "proto_cluster_id": "cluster-legacy"
	    }
	  }]
	}`
	if err := os.WriteFile(legacyPath, []byte(legacyState), 0o600); err != nil {
		t.Fatalf("WriteFile(legacy) error = %v", err)
	}

	store := openTestLocalMemoryStore(t, "ws-legacy", "agent-legacy")
	state := store.Snapshot()
	if len(state.Episodes) != 1 || len(state.Digests) != 1 {
		t.Fatalf("expected migrated legacy state, got %+v", state)
	}
	if state.Episodes[0].EpisodeID != "episode-legacy" || state.Digests[0].DigestID != "task:task-legacy" {
		t.Fatalf("unexpected migrated records: %+v %+v", state.Episodes, state.Digests)
	}
	if _, err := os.Stat(localMemoryStorePath("ws-legacy", "agent-legacy")); err != nil {
		t.Fatalf("expected sqlite db after migration, got err=%v", err)
	}
	matches, err := filepath.Glob(legacyPath + ".migrated-*")
	if err != nil {
		t.Fatalf("Glob(migrated legacy) error = %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected migrated legacy backup, got %#v", matches)
	}
	if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
		t.Fatalf("expected legacy json to be moved aside, got err=%v", err)
	}
}

func TestLocalMemoryStoreReadPacketViewUsesAnchorsAndSkipsStaleDigests(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store := openTestLocalMemoryStore(t, "ws-2", "agent-2")
	if err := store.UpsertEpisode(LocalMemoryEpisodeRecord{
		EpisodeID: "episode-hot",
		Scope: LocalMemoryScope{
			TaskID:         "task-hot",
			SessionID:      "session-hot",
			TensionID:      "tension-hot",
			ProtoClusterID: "cluster-hot",
		},
		Trigger:   "task_cycle_result",
		Outcome:   "continue",
		Summary:   "Fresh local continuity",
		DocKeys:   []string{"task.task-hot"},
		CreatedAt: time.Date(2026, 3, 23, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("UpsertEpisode() error = %v", err)
	}
	if err := store.PutDigest(LocalMemoryDigestRecord{
		DigestID: "task:task-hot",
		Tier:     "P2",
		Kind:     "TASK_DIGEST",
		Scope: LocalMemoryScope{
			TaskID:         "task-hot",
			SessionID:      "session-hot",
			TensionID:      "tension-hot",
			ProtoClusterID: "cluster-hot",
		},
		Summary: "Hot task digest",
		EpisodeDigest: &LocalEpisodeDigest{
			ScopeKind:       "task",
			ScopeKey:        "task-hot",
			UpdatedAt:       time.Date(2026, 3, 23, 12, 1, 0, 0, time.UTC).Format(time.RFC3339Nano),
			EventCount:      3,
			MessageCount:    1,
			LastSummary:     "Hot task digest",
			LatestSessionID: "session-hot",
			LatestTensionID: "tension-hot",
			ProtoClusterID:  "cluster-hot",
			DocKeys:         []string{"task.task-hot"},
		},
		DocKeys: []string{"task.task-hot"},
	}); err != nil {
		t.Fatalf("PutDigest(task) error = %v", err)
	}
	if err := store.PutDigest(LocalMemoryDigestRecord{
		DigestID: "tension:tension-hot",
		Tier:     "P2",
		Kind:     "TENSION_DIGEST",
		Scope: LocalMemoryScope{
			TaskID:         "task-hot",
			SessionID:      "session-hot",
			TensionID:      "tension-hot",
			ProtoClusterID: "cluster-hot",
		},
		Summary: "Stale tension digest",
		Stale:   true,
		EpisodeDigest: &LocalEpisodeDigest{
			ScopeKind:       "tension",
			ScopeKey:        "tension-hot",
			UpdatedAt:       time.Date(2026, 3, 23, 12, 1, 30, 0, time.UTC).Format(time.RFC3339Nano),
			EventCount:      2,
			LastSummary:     "Stale tension digest",
			LatestSessionID: "session-hot",
			LatestTensionID: "tension-hot",
			ProtoClusterID:  "cluster-hot",
		},
	}); err != nil {
		t.Fatalf("PutDigest(tension) error = %v", err)
	}

	view, err := store.ReadPacketView(LocalMemoryPacketQuery{
		Scope: LocalMemoryScope{
			TaskID:         "task-hot",
			SessionID:      "session-hot",
			TensionID:      "tension-hot",
			ProtoClusterID: "cluster-hot",
		},
		DocKeys:     []string{"task.task-hot"},
		RecentLimit: 4,
	})
	if err != nil {
		t.Fatalf("ReadPacketView() error = %v", err)
	}
	if !view.Matched {
		t.Fatal("expected packet view to match anchored records")
	}
	if len(view.Episodes) != 1 || view.Episodes[0].EpisodeID != "episode-hot" {
		t.Fatalf("unexpected packet episodes: %+v", view.Episodes)
	}
	if view.TaskDigest == nil || view.TaskDigest.DigestID != "task:task-hot" {
		t.Fatalf("expected active task digest, got %+v", view.TaskDigest)
	}
	if view.TensionDigest != nil {
		t.Fatalf("expected stale tension digest to be excluded, got %+v", view.TensionDigest)
	}
	if len(view.StaleDigestIDs) != 1 || view.StaleDigestIDs[0] != "tension:tension-hot" {
		t.Fatalf("expected stale digest tracking, got %+v", view.StaleDigestIDs)
	}

	reloaded := openTestLocalMemoryStore(t, "ws-2", "agent-2")
	state := reloaded.Snapshot()
	if state.Digests[0].LastAccessedAt == "" && state.Digests[1].LastAccessedAt == "" {
		t.Fatalf("expected packet read to persist digest access, got %+v", state.Digests)
	}
}

func TestLocalMemoryStoreInvalidateGuardVersionMatchIsNoOp(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store := openTestLocalMemoryStore(t, "ws-3", "agent-3")
	if err := store.PutDigest(LocalMemoryDigestRecord{
		DigestID: "task:task-3",
		Tier:     "P2",
		Kind:     "TASK_DIGEST",
		Scope:    LocalMemoryScope{TaskID: "task-3"},
		Summary:  "Stable digest",
		Guards: []LocalMemoryGuard{
			{GuardType: "doc_sha", Ref: "task.task-3", Version: "sha-1"},
		},
		EpisodeDigest: &LocalEpisodeDigest{
			ScopeKind:   "task",
			ScopeKey:    "task-3",
			UpdatedAt:   time.Date(2026, 3, 23, 13, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
			EventCount:  1,
			LastSummary: "Stable digest",
		},
	}); err != nil {
		t.Fatalf("PutDigest() error = %v", err)
	}

	result, err := store.Invalidate(LocalMemoryInvalidationInput{
		GuardChanges: []LocalMemoryGuardChange{{
			GuardType:      "doc_sha",
			Ref:            "task.task-3",
			CurrentVersion: "sha-1",
		}},
	})
	if err != nil {
		t.Fatalf("Invalidate() error = %v", err)
	}
	if result.DigestsInvalidated != 0 || result.EpisodesSuperseded != 0 {
		t.Fatalf("expected no-op invalidation on matching version, got %+v", result)
	}

	state := store.Snapshot()
	if len(state.Digests) != 1 || state.Digests[0].Stale {
		t.Fatalf("expected digest to remain active, got %+v", state.Digests)
	}
}

func TestLocalMemoryStoreInvalidateArtifactVersionChangeMarksDigestStale(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store := openTestLocalMemoryStore(t, "ws-4", "agent-4")
	if err := store.PutDigest(LocalMemoryDigestRecord{
		DigestID: "task:task-4",
		Tier:     "P2",
		Kind:     "TASK_DIGEST",
		Scope:    LocalMemoryScope{TaskID: "task-4"},
		Summary:  "Artifact continuity digest",
		ArtifactRefs: []string{
			"doc:deliverable.brief",
		},
		Guards: []LocalMemoryGuard{
			{GuardType: "artifact_version", Ref: "doc:deliverable.brief", Version: "artifact-old"},
		},
		EpisodeDigest: &LocalEpisodeDigest{
			ScopeKind:   "task",
			ScopeKey:    "task-4",
			UpdatedAt:   time.Date(2026, 3, 23, 13, 30, 0, 0, time.UTC).Format(time.RFC3339Nano),
			EventCount:  2,
			LastSummary: "Artifact continuity digest",
			ArtifactRefs: []string{
				"doc:deliverable.brief",
			},
		},
	}); err != nil {
		t.Fatalf("PutDigest() error = %v", err)
	}

	result, err := store.Invalidate(LocalMemoryInvalidationInput{
		Reasons:      []string{"artifact updated"},
		ArtifactRefs: []string{"doc:deliverable.brief"},
		GuardChanges: []LocalMemoryGuardChange{{
			GuardType:      "artifact_version",
			Ref:            "doc:deliverable.brief",
			CurrentVersion: "artifact-new",
		}},
	})
	if err != nil {
		t.Fatalf("Invalidate() error = %v", err)
	}
	if result.DigestsInvalidated != 1 {
		t.Fatalf("expected artifact version change to invalidate digest, got %+v", result)
	}

	state := store.Snapshot()
	if len(state.Digests) != 1 || !state.Digests[0].Stale {
		t.Fatalf("expected digest to be stale after artifact version change, got %+v", state.Digests)
	}
}

func TestLocalMemoryStoreReadPacketViewMatchesIndexedArtifactRefsWithoutScopeReuse(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store := openTestLocalMemoryStore(t, "ws-5", "agent-5")
	if err := store.UpsertEpisode(LocalMemoryEpisodeRecord{
		EpisodeID: "episode-a",
		Scope: LocalMemoryScope{
			TaskID:         "task-a",
			SessionID:      "session-a",
			TensionID:      "tension-a",
			ProtoClusterID: "cluster-a",
		},
		Summary:      "Shared artifact continuity",
		ArtifactRefs: []string{"doc:shared-brief"},
		CreatedAt:    time.Date(2026, 3, 23, 15, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("UpsertEpisode(a) error = %v", err)
	}
	if err := store.UpsertEpisode(LocalMemoryEpisodeRecord{
		EpisodeID: "episode-b",
		Scope: LocalMemoryScope{
			TaskID:         "task-b",
			SessionID:      "session-b",
			TensionID:      "tension-b",
			ProtoClusterID: "cluster-b",
		},
		Summary:      "Unrelated continuity",
		ArtifactRefs: []string{"doc:other"},
		CreatedAt:    time.Date(2026, 3, 23, 15, 1, 0, 0, time.UTC).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("UpsertEpisode(b) error = %v", err)
	}
	if err := store.PutDigest(LocalMemoryDigestRecord{
		DigestID: "task:task-a",
		Tier:     "P2",
		Kind:     "TASK_DIGEST",
		Scope: LocalMemoryScope{
			TaskID:         "task-a",
			SessionID:      "session-a",
			TensionID:      "tension-a",
			ProtoClusterID: "cluster-a",
		},
		Summary:      "Shared artifact digest",
		ArtifactRefs: []string{"doc:shared-brief"},
		EpisodeDigest: &LocalEpisodeDigest{
			ScopeKind:       "task",
			ScopeKey:        "task-a",
			UpdatedAt:       time.Date(2026, 3, 23, 15, 0, 30, 0, time.UTC).Format(time.RFC3339Nano),
			EventCount:      1,
			LastSummary:     "Shared artifact digest",
			LatestSessionID: "session-a",
			LatestTensionID: "tension-a",
			ProtoClusterID:  "cluster-a",
			ArtifactRefs:    []string{"doc:shared-brief"},
		},
	}); err != nil {
		t.Fatalf("PutDigest(task-a) error = %v", err)
	}
	if err := store.PutDigest(LocalMemoryDigestRecord{
		DigestID: "task:task-b",
		Tier:     "P2",
		Kind:     "TASK_DIGEST",
		Scope: LocalMemoryScope{
			TaskID:         "task-b",
			SessionID:      "session-b",
			TensionID:      "tension-b",
			ProtoClusterID: "cluster-b",
		},
		Summary:      "Other digest",
		ArtifactRefs: []string{"doc:other"},
		EpisodeDigest: &LocalEpisodeDigest{
			ScopeKind:       "task",
			ScopeKey:        "task-b",
			UpdatedAt:       time.Date(2026, 3, 23, 15, 1, 30, 0, time.UTC).Format(time.RFC3339Nano),
			EventCount:      1,
			LastSummary:     "Other digest",
			LatestSessionID: "session-b",
			LatestTensionID: "tension-b",
			ProtoClusterID:  "cluster-b",
			ArtifactRefs:    []string{"doc:other"},
		},
	}); err != nil {
		t.Fatalf("PutDigest(task-b) error = %v", err)
	}

	view, err := store.ReadPacketView(LocalMemoryPacketQuery{
		ArtifactRefs: []string{"doc:shared-brief"},
		RecentLimit:  4,
	})
	if err != nil {
		t.Fatalf("ReadPacketView() error = %v", err)
	}
	if !view.Matched {
		t.Fatal("expected indexed artifact ref query to match")
	}
	if view.TaskDigest == nil || view.TaskDigest.DigestID != "task:task-a" {
		t.Fatalf("expected artifact query to resolve only matching digest, got %+v", view.TaskDigest)
	}
	if len(view.Episodes) != 1 || view.Episodes[0].EpisodeID != "episode-a" {
		t.Fatalf("expected artifact query to resolve only matching episode, got %+v", view.Episodes)
	}
}

func TestLocalMemoryStoreReadPacketViewPrefersExactScopeOverSharedArtifactRef(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store := openTestLocalMemoryStore(t, "ws-6", "agent-6")
	if err := store.PutDigest(LocalMemoryDigestRecord{
		DigestID: "task:task-a",
		Tier:     "P2",
		Kind:     "TASK_DIGEST",
		Scope: LocalMemoryScope{
			TaskID:         "task-a",
			SessionID:      "session-a",
			TensionID:      "tension-a",
			ProtoClusterID: "cluster-a",
		},
		Summary:      "Exact scope digest",
		ArtifactRefs: []string{"doc:shared-brief"},
		UpdatedAt:    time.Date(2026, 3, 23, 16, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
		EpisodeDigest: &LocalEpisodeDigest{
			ScopeKind:       "task",
			ScopeKey:        "task-a",
			UpdatedAt:       time.Date(2026, 3, 23, 16, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
			EventCount:      2,
			LastSummary:     "Exact scope digest",
			LatestSessionID: "session-a",
			LatestTensionID: "tension-a",
			ProtoClusterID:  "cluster-a",
			ArtifactRefs:    []string{"doc:shared-brief"},
		},
	}); err != nil {
		t.Fatalf("PutDigest(task-a) error = %v", err)
	}
	if err := store.PutDigest(LocalMemoryDigestRecord{
		DigestID: "task:task-b",
		Tier:     "P2",
		Kind:     "TASK_DIGEST",
		Scope: LocalMemoryScope{
			TaskID:         "task-b",
			SessionID:      "session-b",
			TensionID:      "tension-b",
			ProtoClusterID: "cluster-b",
		},
		Summary:      "Shared ref but different scope",
		ArtifactRefs: []string{"doc:shared-brief"},
		UpdatedAt:    time.Date(2026, 3, 23, 16, 5, 0, 0, time.UTC).Format(time.RFC3339Nano),
		EpisodeDigest: &LocalEpisodeDigest{
			ScopeKind:       "task",
			ScopeKey:        "task-b",
			UpdatedAt:       time.Date(2026, 3, 23, 16, 5, 0, 0, time.UTC).Format(time.RFC3339Nano),
			EventCount:      3,
			LastSummary:     "Shared ref but different scope",
			LatestSessionID: "session-b",
			LatestTensionID: "tension-b",
			ProtoClusterID:  "cluster-b",
			ArtifactRefs:    []string{"doc:shared-brief"},
		},
	}); err != nil {
		t.Fatalf("PutDigest(task-b) error = %v", err)
	}

	view, err := store.ReadPacketView(LocalMemoryPacketQuery{
		Scope: LocalMemoryScope{
			TaskID:         "task-a",
			SessionID:      "session-a",
			TensionID:      "tension-a",
			ProtoClusterID: "cluster-a",
		},
		ArtifactRefs: []string{"doc:shared-brief"},
		RecentLimit:  4,
	})
	if err != nil {
		t.Fatalf("ReadPacketView() error = %v", err)
	}
	if view.TaskDigest == nil || view.TaskDigest.DigestID != "task:task-a" {
		t.Fatalf("expected exact-scope digest to beat newer shared-ref digest, got %+v", view.TaskDigest)
	}
}

func TestSelectLocalMemoryDigestIDsByScopeAnchorsReturnsAnchorUnion(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store := openTestLocalMemoryStore(t, "ws-7", "agent-7")
	for _, digest := range []LocalMemoryDigestRecord{
		{
			DigestID: "task:exact",
			Tier:     "P2",
			Kind:     "TASK_DIGEST",
			Scope: LocalMemoryScope{
				TaskID:         "task-sql",
				SessionID:      "session-sql",
				TensionID:      "tension-sql",
				ProtoClusterID: "cluster-sql",
			},
			Summary:   "Exact scope digest",
			UpdatedAt: time.Date(2026, 3, 23, 16, 10, 0, 0, time.UTC).Format(time.RFC3339Nano),
		},
		{
			DigestID: "task:same-task",
			Tier:     "P2",
			Kind:     "TASK_DIGEST",
			Scope: LocalMemoryScope{
				TaskID:         "task-sql",
				SessionID:      "session-other",
				TensionID:      "tension-other",
				ProtoClusterID: "cluster-other",
			},
			Summary:   "Partial anchor digest",
			UpdatedAt: time.Date(2026, 3, 23, 16, 11, 0, 0, time.UTC).Format(time.RFC3339Nano),
		},
		{
			DigestID: "task:unrelated",
			Tier:     "P2",
			Kind:     "TASK_DIGEST",
			Scope: LocalMemoryScope{
				TaskID:         "task-other",
				SessionID:      "session-other",
				TensionID:      "tension-other",
				ProtoClusterID: "cluster-other",
			},
			Summary:   "Unrelated digest",
			UpdatedAt: time.Date(2026, 3, 23, 16, 12, 0, 0, time.UTC).Format(time.RFC3339Nano),
		},
	} {
		if err := store.PutDigest(digest); err != nil {
			t.Fatalf("PutDigest(%s) error = %v", digest.DigestID, err)
		}
	}

	ids, err := selectLocalMemoryDigestIDsByScopeAnchors(store.db, LocalMemoryScope{
		TaskID:    "task-sql",
		SessionID: "session-sql",
	})
	if err != nil {
		t.Fatalf("selectLocalMemoryDigestIDsByScopeAnchors() error = %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected exact+partial scope candidates, got %+v", ids)
	}
	if !hasLocalMemoryID(ids, "task:exact") || !hasLocalMemoryID(ids, "task:same-task") {
		t.Fatalf("expected exact and partial anchor candidates, got %+v", ids)
	}
	if hasLocalMemoryID(ids, "task:unrelated") {
		t.Fatalf("expected unrelated digest to stay out of SQL anchor preselection, got %+v", ids)
	}
}

func TestSelectLocalMemoryDigestIDsByRefsReturnsSharedCandidates(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store := openTestLocalMemoryStore(t, "ws-7b", "agent-7b")
	for _, digest := range []LocalMemoryDigestRecord{
		{
			DigestID:     "task:artifact",
			Tier:         "P2",
			Kind:         "TASK_DIGEST",
			Scope:        LocalMemoryScope{TaskID: "task-artifact"},
			Summary:      "Artifact-linked digest",
			ArtifactRefs: []string{"doc:shared-brief"},
		},
		{
			DigestID:       "task:constraint",
			Tier:           "P2",
			Kind:           "TASK_DIGEST",
			Scope:          LocalMemoryScope{TaskID: "task-constraint"},
			Summary:        "Constraint-linked digest",
			ConstraintRefs: []string{"needs_auth"},
		},
		{
			DigestID:     "task:unrelated-ref",
			Tier:         "P2",
			Kind:         "TASK_DIGEST",
			Scope:        LocalMemoryScope{TaskID: "task-unrelated"},
			Summary:      "Unrelated digest",
			ArtifactRefs: []string{"doc:other"},
		},
	} {
		if err := store.PutDigest(digest); err != nil {
			t.Fatalf("PutDigest(%s) error = %v", digest.DigestID, err)
		}
	}

	ids, err := selectLocalMemoryDigestIDsByRefs(store.db, normalizeLocalMemoryPacketQuery(LocalMemoryPacketQuery{
		ArtifactRefs:   []string{"doc:shared-brief"},
		ConstraintRefs: []string{"needs_auth"},
	}))
	if err != nil {
		t.Fatalf("selectLocalMemoryDigestIDsByRefs() error = %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected artifact+constraint ref candidates, got %+v", ids)
	}
	if !hasLocalMemoryID(ids, "task:artifact") || !hasLocalMemoryID(ids, "task:constraint") {
		t.Fatalf("expected ref candidates to be unioned, got %+v", ids)
	}
	if hasLocalMemoryID(ids, "task:unrelated-ref") {
		t.Fatalf("expected unrelated ref digest to stay out of SQL ref preselection, got %+v", ids)
	}
}

func TestSelectLocalMemoryEpisodeIDsByRefsReturnsSharedCandidates(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store := openTestLocalMemoryStore(t, "ws-7c", "agent-7c")
	for _, episode := range []LocalMemoryEpisodeRecord{
		{
			EpisodeID:    "episode-artifact",
			Scope:        LocalMemoryScope{TaskID: "task-artifact"},
			Summary:      "Artifact-linked episode",
			ArtifactRefs: []string{"doc:shared-brief"},
			CreatedAt:    time.Date(2026, 3, 23, 16, 20, 0, 0, time.UTC).Format(time.RFC3339Nano),
		},
		{
			EpisodeID:   "episode-segment",
			Scope:       LocalMemoryScope{TaskID: "task-segment"},
			Summary:     "Segment-linked episode",
			SegmentRefs: []string{"segment://handoff"},
			CreatedAt:   time.Date(2026, 3, 23, 16, 21, 0, 0, time.UTC).Format(time.RFC3339Nano),
		},
		{
			EpisodeID:    "episode-unrelated",
			Scope:        LocalMemoryScope{TaskID: "task-unrelated"},
			Summary:      "Unrelated episode",
			ArtifactRefs: []string{"doc:other"},
			CreatedAt:    time.Date(2026, 3, 23, 16, 22, 0, 0, time.UTC).Format(time.RFC3339Nano),
		},
	} {
		if err := store.UpsertEpisode(episode); err != nil {
			t.Fatalf("UpsertEpisode(%s) error = %v", episode.EpisodeID, err)
		}
	}

	ids, err := selectLocalMemoryEpisodeIDsByRefs(store.db, normalizeLocalMemoryPacketQuery(LocalMemoryPacketQuery{
		ArtifactRefs: []string{"doc:shared-brief"},
		SegmentRefs:  []string{"segment://handoff"},
	}))
	if err != nil {
		t.Fatalf("selectLocalMemoryEpisodeIDsByRefs() error = %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected artifact+segment ref candidates, got %+v", ids)
	}
	if !hasLocalMemoryID(ids, "episode-artifact") || !hasLocalMemoryID(ids, "episode-segment") {
		t.Fatalf("expected episode ref candidates to be unioned, got %+v", ids)
	}
	if hasLocalMemoryID(ids, "episode-unrelated") {
		t.Fatalf("expected unrelated episode to stay out of SQL ref preselection, got %+v", ids)
	}
}

func TestLocalMemoryStoreReadPacketViewTaskOnlyAnchorKeepsFreshestDigestTieBreak(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store := openTestLocalMemoryStore(t, "ws-9", "agent-9")
	for _, digest := range []LocalMemoryDigestRecord{
		{
			DigestID: "task:older",
			Tier:     "P2",
			Kind:     "TASK_DIGEST",
			Scope: LocalMemoryScope{
				TaskID:    "task-fresh",
				SessionID: "session-older",
			},
			Summary:   "Older competing digest",
			UpdatedAt: time.Date(2026, 3, 23, 18, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
		},
		{
			DigestID: "task:newer",
			Tier:     "P2",
			Kind:     "TASK_DIGEST",
			Scope: LocalMemoryScope{
				TaskID:    "task-fresh",
				SessionID: "session-newer",
			},
			Summary:   "Newer competing digest",
			UpdatedAt: time.Date(2026, 3, 23, 18, 5, 0, 0, time.UTC).Format(time.RFC3339Nano),
		},
	} {
		if err := store.PutDigest(digest); err != nil {
			t.Fatalf("PutDigest(%s) error = %v", digest.DigestID, err)
		}
	}

	view, err := store.ReadPacketView(LocalMemoryPacketQuery{
		Scope: LocalMemoryScope{
			TaskID: "task-fresh",
		},
		RecentLimit: 4,
	})
	if err != nil {
		t.Fatalf("ReadPacketView() error = %v", err)
	}
	if view.TaskDigest == nil || view.TaskDigest.DigestID != "task:newer" {
		t.Fatalf("expected freshest task digest to win after SQL preselection, got %+v", view.TaskDigest)
	}
}

func TestLocalMemoryStoreReadPacketViewExactScopeEpisodesKeepBandAndRecentLimit(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store := openTestLocalMemoryStore(t, "ws-10", "agent-10")
	for _, episode := range []LocalMemoryEpisodeRecord{
		{
			EpisodeID: "episode-partial",
			Scope: LocalMemoryScope{
				TaskID: "task-band",
			},
			Summary:   "Partial anchor episode",
			CreatedAt: time.Date(2026, 3, 23, 19, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
		},
		{
			EpisodeID: "episode-exact-1",
			Scope: LocalMemoryScope{
				TaskID:    "task-band",
				SessionID: "session-band",
			},
			Summary:   "Exact anchor episode 1",
			CreatedAt: time.Date(2026, 3, 23, 19, 1, 0, 0, time.UTC).Format(time.RFC3339Nano),
		},
		{
			EpisodeID: "episode-exact-2",
			Scope: LocalMemoryScope{
				TaskID:    "task-band",
				SessionID: "session-band",
			},
			Summary:   "Exact anchor episode 2",
			CreatedAt: time.Date(2026, 3, 23, 19, 2, 0, 0, time.UTC).Format(time.RFC3339Nano),
		},
		{
			EpisodeID: "episode-exact-3",
			Scope: LocalMemoryScope{
				TaskID:    "task-band",
				SessionID: "session-band",
			},
			Summary:   "Exact anchor episode 3",
			CreatedAt: time.Date(2026, 3, 23, 19, 3, 0, 0, time.UTC).Format(time.RFC3339Nano),
		},
	} {
		if err := store.UpsertEpisode(episode); err != nil {
			t.Fatalf("UpsertEpisode(%s) error = %v", episode.EpisodeID, err)
		}
	}

	view, err := store.ReadPacketView(LocalMemoryPacketQuery{
		Scope: LocalMemoryScope{
			TaskID:    "task-band",
			SessionID: "session-band",
		},
		RecentLimit: 2,
	})
	if err != nil {
		t.Fatalf("ReadPacketView() error = %v", err)
	}
	if len(view.Episodes) != 2 {
		t.Fatalf("expected exact-scope recent tail, got %+v", view.Episodes)
	}
	if view.Episodes[0].EpisodeID != "episode-exact-2" || view.Episodes[1].EpisodeID != "episode-exact-3" {
		t.Fatalf("expected exact-scope band to survive SQL preselection, got %+v", view.Episodes)
	}
}

func TestLocalMemoryStoreReadPacketViewPersistsAccessWithoutFullSQLiteRewrite(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store := openTestLocalMemoryStore(t, "ws-8", "agent-8")
	if err := store.PutDigest(LocalMemoryDigestRecord{
		DigestID: "task:hot",
		Tier:     "P2",
		Kind:     "TASK_DIGEST",
		Scope: LocalMemoryScope{
			TaskID:    "task-hot",
			SessionID: "session-hot",
		},
		Summary:   "Hot digest",
		UpdatedAt: time.Date(2026, 3, 23, 17, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
		EpisodeDigest: &LocalEpisodeDigest{
			ScopeKind:   "task",
			ScopeKey:    "task-hot",
			UpdatedAt:   time.Date(2026, 3, 23, 17, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
			EventCount:  2,
			LastSummary: "Hot digest",
		},
	}); err != nil {
		t.Fatalf("PutDigest(hot) error = %v", err)
	}
	if err := store.PutDigest(LocalMemoryDigestRecord{
		DigestID: "task:cold",
		Tier:     "P2",
		Kind:     "TASK_DIGEST",
		Scope: LocalMemoryScope{
			TaskID:    "task-cold",
			SessionID: "session-cold",
		},
		Summary:   "Cold digest",
		UpdatedAt: time.Date(2026, 3, 23, 17, 5, 0, 0, time.UTC).Format(time.RFC3339Nano),
		EpisodeDigest: &LocalEpisodeDigest{
			ScopeKind:   "task",
			ScopeKey:    "task-cold",
			UpdatedAt:   time.Date(2026, 3, 23, 17, 5, 0, 0, time.UTC).Format(time.RFC3339Nano),
			EventCount:  1,
			LastSummary: "Cold digest",
		},
	}); err != nil {
		t.Fatalf("PutDigest(cold) error = %v", err)
	}

	hotRowBefore, hotAccessBefore := queryLocalMemoryDigestRow(t, store.db, "task:hot")
	coldRowBefore, coldAccessBefore := queryLocalMemoryDigestRow(t, store.db, "task:cold")

	view, err := store.ReadPacketView(LocalMemoryPacketQuery{
		Scope: LocalMemoryScope{
			TaskID:    "task-hot",
			SessionID: "session-hot",
		},
		RecentLimit: 4,
	})
	if err != nil {
		t.Fatalf("ReadPacketView() error = %v", err)
	}
	if !view.Matched || view.TaskDigest == nil || view.TaskDigest.DigestID != "task:hot" {
		t.Fatalf("expected hot digest match, got %+v", view)
	}

	hotRowAfter, hotAccessAfter := queryLocalMemoryDigestRow(t, store.db, "task:hot")
	coldRowAfter, coldAccessAfter := queryLocalMemoryDigestRow(t, store.db, "task:cold")
	if hotRowAfter != hotRowBefore {
		t.Fatalf("expected hot digest rowid to stay stable on access update, before=%d after=%d", hotRowBefore, hotRowAfter)
	}
	if coldRowAfter != coldRowBefore {
		t.Fatalf("expected cold digest rowid to stay stable on neighbor access, before=%d after=%d", coldRowBefore, coldRowAfter)
	}
	if hotAccessAfter == "" || hotAccessAfter == hotAccessBefore {
		t.Fatalf("expected hot digest last_accessed_at to update, before=%q after=%q", hotAccessBefore, hotAccessAfter)
	}
	if coldAccessAfter != coldAccessBefore {
		t.Fatalf("expected cold digest last_accessed_at to stay untouched, before=%q after=%q", coldAccessBefore, coldAccessAfter)
	}
}

func TestLocalMemoryStorePruneEvictsExpiredDigests(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store := openTestLocalMemoryStore(t, "ws-1", "agent-1")

	now := time.Now().UTC()
	store.PutDigest(LocalMemoryDigestRecord{
		DigestID:  "valid-digest",
		Tier:      "P2",
		ExpiresAt: now.Add(24 * time.Hour).Format(time.RFC3339Nano),
		UpdatedAt: now.Format(time.RFC3339Nano),
	})
	store.PutDigest(LocalMemoryDigestRecord{
		DigestID:  "expired-digest",
		Tier:      "P2",
		ExpiresAt: now.Add(-24 * time.Hour).Format(time.RFC3339Nano),
		UpdatedAt: now.Format(time.RFC3339Nano),
	})

	cfg := DefaultLocalMemoryTTLConfig()
	prunedD, prunedE, err := store.Prune(now, cfg)
	if err != nil {
		t.Fatalf("prune failed: %v", err)
	}
	if prunedD != 1 || prunedE != 0 {
		t.Fatalf("expected 1 digest pruned, got %d, episodes: %d", prunedD, prunedE)
	}

	stats := store.Stats()
	if stats.Digests != 1 {
		t.Fatalf("expected 1 digest remaining, got %d", stats.Digests)
	}
}

func TestLocalMemoryStorePruneEvictsColdStaleDigests(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store := openTestLocalMemoryStore(t, "ws-1", "agent-1")

	now := time.Now().UTC()
	cfg := DefaultLocalMemoryTTLConfig()
	cfg.StaleColdTTL = 24 * time.Hour

	store.PutDigest(LocalMemoryDigestRecord{
		DigestID:       "stale-hot",
		Stale:          true,
		UpdatedAt:      now.Add(-48 * time.Hour).Format(time.RFC3339Nano),
		LastAccessedAt: now.Add(-1 * time.Hour).Format(time.RFC3339Nano),
	})

	store.PutDigest(LocalMemoryDigestRecord{
		DigestID: "stale-cold-updated",
		Stale:    true,
	})

	store.PutDigest(LocalMemoryDigestRecord{
		DigestID:       "fresh-cold",
		Stale:          false,
		LastAccessedAt: now.Add(-48 * time.Hour).Format(time.RFC3339Nano),
	})

	// Manually override UpdatedAt since PutDigest overwrites it with time.Now()
	store.mu.Lock()
	for i := range store.state.Digests {
		d := &store.state.Digests[i]
		if d.DigestID == "stale-hot" || d.DigestID == "stale-cold-updated" {
			d.UpdatedAt = now.Add(-48 * time.Hour).Format(time.RFC3339Nano)
		}
	}
	store.mu.Unlock()

	prunedD, _, err := store.Prune(now, cfg)
	if err != nil {
		t.Fatalf("prune failed: %v", err)
	}

	if prunedD != 1 {
		t.Fatalf("expected 1 digest pruned (stale-cold-updated), got %d", prunedD)
	}

	stats := store.Stats()
	if stats.Digests != 2 {
		t.Fatalf("expected 2 digests remaining, got %d", stats.Digests)
	}
}

func TestLocalMemoryStorePruneTruncatesEpisodeTails(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store := openTestLocalMemoryStore(t, "ws-1", "agent-1")

	now := time.Now().UTC()
	cfg := DefaultLocalMemoryTTLConfig()
	cfg.MaxEpisodesPerTask = 2

	for i := 1; i <= 3; i++ {
		store.UpsertEpisode(LocalMemoryEpisodeRecord{
			EpisodeID: fmt.Sprintf("ep-%d", i),
			Scope:     LocalMemoryScope{TaskID: "task-1"},
			CreatedAt: now.Add(time.Duration(i) * time.Hour).Format(time.RFC3339Nano),
		})
	}

	store.UpsertEpisode(LocalMemoryEpisodeRecord{
		EpisodeID: "ep-other",
		Scope:     LocalMemoryScope{TaskID: "task-2"},
		CreatedAt: now.Add(time.Hour).Format(time.RFC3339Nano),
	})

	_, prunedE, err := store.Prune(now.Add(10*time.Hour), cfg)
	if err != nil {
		t.Fatalf("prune failed: %v", err)
	}

	if prunedE != 1 {
		t.Fatalf("expected 1 episode pruned (the oldest of task-1), got %d", prunedE)
	}

	stats := store.Stats()
	if stats.Episodes != 3 {
		t.Fatalf("expected 3 episodes remaining, got %d", stats.Episodes)
	}

	state := store.Snapshot()
	for _, ep := range state.Episodes {
		if ep.EpisodeID == "ep-1" {
			t.Fatalf("ep-1 should have been pruned")
		}
	}
}

func TestLocalMemoryStoreFTS(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store := openTestLocalMemoryStore(t, "ws-fts", "agent-fts")

	if err := store.UpsertEpisode(LocalMemoryEpisodeRecord{
		EpisodeID: "ep-fts-1",
		Scope:     LocalMemoryScope{TaskID: "task-fts"},
		Summary:   "Fixed a subtle race condition in the React component rendering loop",
		Outcome:   "success",
		Tags:      []string{"frontend", "react", "bugfix"},
	}); err != nil {
		t.Fatalf("UpsertEpisode: %v", err)
	}

	if err := store.PutDigest(LocalMemoryDigestRecord{
		DigestID: "dig-fts-1",
		Tier:     "P2",
		Kind:     "LESSON",
		Scope:    LocalMemoryScope{TaskID: "task-fts-2"},
		Summary:  "React concurrent mode requires strict state updates",
		Body:     "When using transitions, make sure to not mutate state directly.",
		Tags:     []string{"react", "architecture"},
	}); err != nil {
		t.Fatalf("PutDigest: %v", err)
	}

	digests, episodes, err := store.Search(context.Background(), "react", 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(episodes) != 1 || episodes[0].EpisodeID != "ep-fts-1" {
		t.Errorf("Expected 1 episode 'ep-fts-1', got %v", episodes)
	}
	if len(digests) != 1 || digests[0].DigestID != "dig-fts-1" {
		t.Errorf("Expected 1 digest 'dig-fts-1', got %v", digests)
	}

	digests, episodes, err = store.Search(context.Background(), "mutate", 10)
	if err != nil {
		t.Fatalf("Search mutate: %v", err)
	}
	if len(episodes) != 0 {
		t.Errorf("Expected 0 episodes for 'mutate', got %v", episodes)
	}
	if len(digests) != 1 || digests[0].DigestID != "dig-fts-1" {
		t.Errorf("Expected 1 digest 'dig-fts-1' for 'mutate', got %v", digests)
	}
}

func TestLocalMemoryPromotionsArePrunedPredictably(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store := openTestLocalMemoryStore(t, "ws-promotions", "agent-promotions")
	base := time.Date(2026, 3, 23, 13, 0, 0, 0, time.UTC)

	for i := 0; i < 3; i++ {
		candidateID := fmt.Sprintf("pending-%d", i)
		if err := upsertLocalMemoryPromotion(context.Background(), store.db, LocalPromotionCandidate{
			CandidateID: candidateID,
			CreatedAt:   base.Add(time.Duration(i) * time.Minute).Format(time.RFC3339Nano),
			MemoryType:  "NOTE",
			SourceID:    "run-pending",
			Title:       candidateID,
			Body:        "pending body",
			Summary:     candidateID,
		}, "pending"); err != nil {
			t.Fatalf("upsert pending %d: %v", i, err)
		}
	}

	for i := 0; i < 3; i++ {
		candidateID := fmt.Sprintf("terminal-%d", i)
		promotedAt := base.Add(time.Duration(10+i) * time.Minute).Format(time.RFC3339Nano)
		if err := upsertLocalMemoryPromotion(context.Background(), store.db, LocalPromotionCandidate{
			CandidateID: candidateID,
			CreatedAt:   base.Add(time.Duration(3+i) * time.Minute).Format(time.RFC3339Nano),
			MemoryType:  "DECISION",
			SourceID:    "run-terminal",
			Title:       candidateID,
			Body:        "terminal body",
			Summary:     candidateID,
			PromotedAt:  promotedAt,
		}, "promoted"); err != nil {
			t.Fatalf("upsert terminal %d: %v", i, err)
		}
	}

	if err := pruneLocalMemoryPromotionsWithLimits(context.Background(), store.db, 2, 1); err != nil {
		t.Fatalf("pruneLocalMemoryPromotionsWithLimits() error = %v", err)
	}

	pending, err := selectPendingLocalMemoryPromotions(context.Background(), store.db, 10)
	if err != nil {
		t.Fatalf("selectPendingLocalMemoryPromotions() error = %v", err)
	}
	if len(pending) != 2 || pending[0].CandidateID != "pending-1" || pending[1].CandidateID != "pending-2" {
		t.Fatalf("expected newest two pending promotions, got %+v", pending)
	}

	rows, err := store.db.Query("SELECT candidate_id, status FROM local_memory_promotions ORDER BY candidate_id")
	if err != nil {
		t.Fatalf("query promotion rows: %v", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var candidateID, status string
		if err := rows.Scan(&candidateID, &status); err != nil {
			t.Fatalf("scan promotion row: %v", err)
		}
		ids = append(ids, candidateID+":"+status)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}

	if len(ids) != 3 {
		t.Fatalf("expected 3 retained promotion rows, got %+v", ids)
	}
	if !hasLocalMemoryID(ids, "pending-1:candidate") || !hasLocalMemoryID(ids, "pending-2:candidate") || !hasLocalMemoryID(ids, "terminal-2:promoted") {
		t.Fatalf("expected bounded retention to keep newest candidate and terminal rows, got %+v", ids)
	}
}
