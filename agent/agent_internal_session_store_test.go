package main

import (
	"errors"
	"testing"
	"time"
)

func TestAgentInternalSessionStoreRoundTripSessionsAndBacklog(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store, err := OpenAgentInternalSessionStore("ws-1", "agent-1")
	if err != nil {
		t.Fatalf("OpenAgentInternalSessionStore() error: %v", err)
	}
	startedAt := time.Date(2026, 5, 14, 9, 0, 0, 0, time.UTC)
	session, err := store.BeginHeartbeatSession(AgentHeartbeatSpec{
		ID:   "loop_self_check",
		Kind: "metacognition",
	}, "digest-1", "cadence_elapsed", startedAt)
	if err != nil {
		t.Fatalf("BeginHeartbeatSession() error: %v", err)
	}
	if err := store.CompleteSession(session.SessionID, "completed", "not_stuck", "Loop check passed", []string{"doc://self-check"}, nil, startedAt.Add(2*time.Second)); err != nil {
		t.Fatalf("CompleteSession() error: %v", err)
	}
	item, err := store.UpsertBacklogItem(AgentPersonalBacklogItem{
		DedupKey:     "visual:home-overlap",
		HeartbeatID:  "visual_product_audit",
		Kind:         "visual_finding",
		Title:        "Home view overlaps",
		Summary:      "The hero text overlaps controls on narrow viewport.",
		Score:        80,
		EvidenceRefs: []string{"screenshot://narrow"},
	})
	if err != nil {
		t.Fatalf("UpsertBacklogItem() error: %v", err)
	}
	if _, err := store.UpsertBacklogItem(AgentPersonalBacklogItem{
		DedupKey:     "visual:home-overlap",
		HeartbeatID:  "visual_product_audit",
		Title:        "Home overlap duplicate",
		EvidenceRefs: []string{"screenshot://desktop"},
	}); err != nil {
		t.Fatalf("UpsertBacklogItem(duplicate) error: %v", err)
	}
	if err := store.MarkBacklogItemPromoted(item.ItemID, []string{"task://revision-1"}, startedAt.Add(3*time.Second)); err != nil {
		t.Fatalf("MarkBacklogItemPromoted() error: %v", err)
	}
	changed, err := store.MarkBacklogItemsStaleByPromotionRef("task://revision-1", "task_superseded", startedAt.Add(4*time.Second))
	if err != nil {
		t.Fatalf("MarkBacklogItemsStaleByPromotionRef() error: %v", err)
	}
	if changed != 1 {
		t.Fatalf("expected one stale backlog item, got %d", changed)
	}

	reloaded, err := OpenAgentInternalSessionStore("ws-1", "agent-1")
	if err != nil {
		t.Fatalf("reload store: %v", err)
	}
	state := reloaded.Snapshot()
	if len(state.Sessions) != 1 || len(state.Backlog) != 1 {
		t.Fatalf("unexpected state sizes: %+v", state)
	}
	if state.Sessions[0].Status != "completed" || state.Sessions[0].DurationMillis != 2000 {
		t.Fatalf("unexpected session record: %+v", state.Sessions[0])
	}
	if state.Backlog[0].Status != "stale" || !state.Backlog[0].Stale {
		t.Fatalf("expected stale promoted backlog item, got %+v", state.Backlog[0])
	}
	if !containsAnatomyTestString(state.Backlog[0].EvidenceRefs, "screenshot://desktop") {
		t.Fatalf("expected duplicate evidence refs to merge, got %+v", state.Backlog[0].EvidenceRefs)
	}
	stats := reloaded.Stats()
	if stats.Sessions != 1 || stats.Completed != 1 || stats.Backlog != 1 || stats.StaleBacklog != 1 || stats.PromotedItems != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestRecordInternalHeartbeatObservationPersistsLocalSession(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	workdir := t.TempDir()
	if err := SaveAgentProfile(workdir, DefaultAgentProfile("sigma", "Sigma", "UI/UX reality critic")); err != nil {
		t.Fatal(err)
	}
	runtime := NewRuntime(RuntimeConfig{
		WorkspaceID: "ws-1",
		AgentID:     "sigma",
		DisplayName: "Sigma",
		Workdir:     workdir,
	}, nil)
	runtime.recordInternalHeartbeatObservation("visual_product_audit", time.Date(2026, 5, 14, 9, 0, 0, 0, time.UTC))
	snapshot := runtime.internalHeartbeatStatusSnapshot(time.Date(2026, 5, 14, 9, 1, 0, 0, time.UTC))
	if snapshot.Memory.Sessions != 1 {
		t.Fatalf("expected one persisted internal session, got %+v", snapshot.Memory)
	}
	state := LoadAgentInternalSessionState("ws-1", "sigma")
	if len(state.Sessions) != 1 || state.Sessions[0].HeartbeatID != "visual_product_audit" {
		t.Fatalf("unexpected persisted session state: %+v", state)
	}
}

func TestCompleteInternalSessionMarksFailureOnError(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store, err := OpenAgentInternalSessionStore("ws-1", "agent-err")
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.BeginHeartbeatSession(AgentHeartbeatSpec{ID: "probe", Kind: "metacognition"}, "digest", "test", time.Date(2026, 5, 14, 9, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteSession(session.SessionID, "completed", "", "", nil, errors.New("boom"), time.Date(2026, 5, 14, 9, 0, 1, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	state := store.Snapshot()
	if state.Sessions[0].Status != "failed" || state.Sessions[0].Error != "boom" {
		t.Fatalf("expected failed session, got %+v", state.Sessions[0])
	}
}

func TestAgentInternalSessionStoreAbandonsRunningSessionsOnRestart(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	store, err := OpenAgentInternalSessionStore("ws-1", "agent-restart")
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.BeginHeartbeatSession(AgentHeartbeatSpec{ID: "loop_self_check", Kind: "metacognition"}, "digest", "never_ran", time.Date(2026, 5, 14, 9, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	changed, err := store.AbandonRunningSessions("runtime restart", time.Date(2026, 5, 14, 9, 5, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if changed != 1 {
		t.Fatalf("changed = %d, want 1", changed)
	}
	reloaded, err := OpenAgentInternalSessionStore("ws-1", "agent-restart")
	if err != nil {
		t.Fatal(err)
	}
	state := reloaded.Snapshot()
	if len(state.Sessions) != 1 || state.Sessions[0].SessionID != session.SessionID || state.Sessions[0].Status != "abandoned" {
		t.Fatalf("expected abandoned session after reload, got %+v", state)
	}
	if stats := reloaded.Stats(); stats.Abandoned != 1 || stats.Failed != 1 {
		t.Fatalf("unexpected abandoned stats: %+v", stats)
	}
}
