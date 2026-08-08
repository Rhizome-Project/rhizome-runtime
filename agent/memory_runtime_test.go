package main

import (
	"strings"
	"testing"
)

func TestLogSessionTransitionMemoryAnchorsToCurrentFocus(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws", "agent-1")

	runtime := &Runtime{
		memory: service,
		focus: &RuntimeFocusState{
			ProtoClusterID: "cluster-1",
			FocusTensionID: "tension-1",
		},
	}

	runtime.logSessionTransitionMemory(
		"session_decision_needed",
		"task-1",
		"session-1",
		"WAITING_DECISION",
		"Need explicit approval",
		"approval",
		"",
		[]BlockedRef{{Kind: "policy", Detail: "Privileged action requires approval"}},
	)

	if len(service.state.RecentEvents) != 1 {
		t.Fatalf("expected one memory event, got %+v", service.state.RecentEvents)
	}
	event := service.state.RecentEvents[0]
	if event.NodeType != localMemoryNodeDecision || event.EventKind != "session_decision_needed" {
		t.Fatalf("unexpected transition event: %+v", event)
	}
	if event.TaskID != "task-1" || event.SessionID != "session-1" || event.TensionID != "tension-1" || event.ProtoClusterID != "cluster-1" {
		t.Fatalf("expected focus/task/session anchors on transition event, got %+v", event)
	}
	if len(event.BlockerKinds) != 1 || event.BlockerKinds[0] != "policy" {
		t.Fatalf("expected blocker kinds to be preserved, got %+v", event.BlockerKinds)
	}
	if digest := service.state.TensionDigests["tension-1"]; digest.EventCount != 1 {
		t.Fatalf("expected tension digest to update, got %+v", digest)
	}
}

func TestRuntimeSyncMemoryCanonicalVersionsInvalidatesArtifactVersionDrift(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws", "agent-2")
	if err := service.rememberArtifactVersions(map[string]string{"doc:deliverable.brief": "artifact-old"}); err != nil {
		t.Fatalf("rememberArtifactVersions() error = %v", err)
	}
	if err := service.appendEvent(LocalMemoryEvent{
		NodeType:       localMemoryNodeArtifactDelta,
		EventKind:      "task_cycle_result",
		Summary:        "Old artifact continuity",
		TaskID:         "task-2",
		SessionID:      "session-2",
		TensionID:      "tension-2",
		ProtoClusterID: "cluster-2",
		ArtifactRefs:   []string{"doc:deliverable.brief"},
		Outcome:        "completed",
	}); err != nil {
		t.Fatalf("appendEvent() error = %v", err)
	}

	runtime := &Runtime{
		memory:  service,
		scratch: RuntimeScratchState{DocSHAs: map[string]string{}},
	}
	runtime.syncMemoryCanonicalVersions(&TaskHydrationBundle{
		Artifacts: []WorkspaceArtifactRecord{{
			ArtifactID:  "artifact-new",
			ArtifactRef: "doc:deliverable.brief",
		}},
	})

	if got := service.state.ArtifactVersions["doc:deliverable.brief"]; got != "artifact-new" {
		t.Fatalf("expected runtime sync to update artifact version, got %+v", service.state.ArtifactVersions)
	}
	snapshot := service.store.Snapshot()
	foundStale := false
	for _, digest := range snapshot.Digests {
		if digest.DigestID == "task:task-2" && digest.Stale {
			foundStale = true
		}
	}
	if !foundStale {
		t.Fatalf("expected stale digest after artifact drift sync, got %+v", snapshot.Digests)
	}
}

func TestRuntimeLogTaskResultMemoryPersistsProcedureMetadata(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws", "agent-3")
	runtime := &Runtime{
		memory: service,
		focus: &RuntimeFocusState{
			ProtoClusterID: "cluster-3",
			FocusTensionID: "tension-3",
			TaskID:         "task-3",
		},
	}

	runtime.logTaskResultMemory(
		WorkspaceTaskRecord{TaskID: "task-3", Title: "Task Three"},
		AgentSessionStateRecord{SessionID: "session-3", TaskID: "task-3", Status: "ACTIVE"},
		"run-3",
		StructuredTaskResult{
			Outcome:    "completed",
			Summary:    "Publish delta after verification",
			NextAction: "Publish delta after verification",
			MemoryType: "PROCEDURE",
			Materialize: TaskMaterialization{
				DocKey: "task.task-3",
			},
		},
	)

	if len(service.state.RecentEvents) != 1 {
		t.Fatalf("expected one memory event, got %+v", service.state.RecentEvents)
	}
	event := service.state.RecentEvents[0]
	if event.NodeType != localMemoryNodeProcedure {
		t.Fatalf("expected explicit procedure node type, got %+v", event)
	}
	if event.MetadataJSON == "" || !strings.Contains(event.MetadataJSON, "\"memory_type\":\"PROCEDURE\"") {
		t.Fatalf("expected runtime to persist task-result metadata, got %+v", event)
	}

	packet := service.buildPacket(MemoryPacketInput{
		Task:    &WorkspaceTaskRecord{TaskID: "task-3", Title: "Task Three", Status: "RUNNING", Priority: "HIGH"},
		Session: &AgentSessionStateRecord{SessionID: "session-3", TaskID: "task-3", Status: "ACTIVE"},
		Focus:   &RuntimeFocusState{TaskID: "task-3", ProtoClusterID: "cluster-3", FocusTensionID: "tension-3"},
	}, 4000)
	if !strings.Contains(packet, "Repeatedly Works Here:") || !strings.Contains(packet, "Publish delta after verification") {
		t.Fatalf("expected explicit procedure to surface in packet, got %q", packet)
	}
}

func TestRuntimeLogTaskResultMemoryPersistsAntiProcedureMetadata(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	service := openTestAgentMemoryService(t, "ws", "agent-4")
	runtime := &Runtime{
		memory: service,
		focus: &RuntimeFocusState{
			ProtoClusterID: "cluster-4",
			FocusTensionID: "tension-4",
			TaskID:         "task-4",
		},
	}

	runtime.logTaskResultMemory(
		WorkspaceTaskRecord{TaskID: "task-4", Title: "Task Four"},
		AgentSessionStateRecord{SessionID: "session-4", TaskID: "task-4", Status: "ACTIVE"},
		"run-4",
		StructuredTaskResult{
			Outcome:    "blocked",
			Summary:    "Retrying stale planner loop",
			NextAction: "Retry stale planner loop",
			MemoryType: "ANTI_PROCEDURE",
			BlockedOn:  []BlockedRef{{Kind: "runtime", Detail: "scratch is stale"}},
		},
	)

	if len(service.state.RecentEvents) != 1 {
		t.Fatalf("expected one memory event, got %+v", service.state.RecentEvents)
	}
	event := service.state.RecentEvents[0]
	if event.NodeType != localMemoryNodeAntiProcedure {
		t.Fatalf("expected explicit anti-procedure node type, got %+v", event)
	}
	if event.MetadataJSON == "" || !strings.Contains(event.MetadataJSON, "\"memory_type\":\"ANTI_PROCEDURE\"") {
		t.Fatalf("expected runtime to persist anti-procedure metadata, got %+v", event)
	}

	packet := service.buildPacket(MemoryPacketInput{
		Task:    &WorkspaceTaskRecord{TaskID: "task-4", Title: "Task Four", Status: "RUNNING", Priority: "HIGH"},
		Session: &AgentSessionStateRecord{SessionID: "session-4", TaskID: "task-4", Status: "ACTIVE"},
		Focus:   &RuntimeFocusState{TaskID: "task-4", ProtoClusterID: "cluster-4", FocusTensionID: "tension-4"},
	}, 4000)
	if !strings.Contains(packet, "Avoid Repeating:") || !strings.Contains(packet, "Retrying stale planner loop") {
		t.Fatalf("expected explicit anti-procedure to surface in packet, got %q", packet)
	}
}
