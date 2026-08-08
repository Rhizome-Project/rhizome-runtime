package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWritePrimaryArtifactPublishesWorkspaceDocReference(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	var writeParams map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "workspace.artifact.write" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeParams = req.Params
		writeRPCResult(w, req, map[string]any{
			"artifact": map[string]any{
				"artifact_id":  "artifact-1",
				"workspace_id": "ws",
				"task_id":      "task-1",
				"title":        "Deliverable Brief",
				"artifact_ref": "doc:deliverable.brief",
				"kind":         "workspace_doc",
				"content_type": "text/markdown",
				"created_by":   "agent-1",
				"created_at":   "2026-03-23T00:00:00Z",
			},
		})
	}))
	defer server.Close()

	memory := openTestAgentMemoryService(t, "ws", "agent-1")
	if err := memory.rememberArtifactVersions(map[string]string{"doc:deliverable.brief": "artifact-old"}); err != nil {
		t.Fatalf("rememberArtifactVersions() error: %v", err)
	}
	if err := memory.appendEvent(LocalMemoryEvent{
		NodeType:       localMemoryNodeArtifactDelta,
		EventKind:      "task_cycle_result",
		Summary:        "Old brief continuity",
		TaskID:         "task-1",
		SessionID:      "sess-1",
		ArtifactRefs:   []string{"doc:deliverable.brief"},
		Outcome:        "completed",
		ProtoClusterID: "cluster-1",
	}); err != nil {
		t.Fatalf("appendEvent() error: %v", err)
	}

	runtime := &Runtime{
		cfg: RuntimeConfig{
			WorkspaceID: "ws",
			AgentID:     "agent-1",
		},
		client: NewRhizomeClient(server.URL, "token"),
		memory: memory,
	}

	err := runtime.writePrimaryArtifact(context.Background(), WorkspaceTaskRecord{TaskID: "task-1"}, "run-1", StructuredTaskResult{
		Outcome: "completed",
		Summary: "Finished the brief",
		Materialize: TaskMaterialization{
			DocKey:   "deliverable.brief",
			DocTitle: "Deliverable Brief",
		},
	})
	if err != nil {
		t.Fatalf("writePrimaryArtifact() error: %v", err)
	}
	if writeParams["artifact_ref"] != "doc:deliverable.brief" || writeParams["kind"] != "workspace_doc" {
		t.Fatalf("unexpected artifact write params: %+v", writeParams)
	}
	if writeParams["task_id"] != "task-1" || writeParams["created_by"] != "agent-1" {
		t.Fatalf("expected task and creator wiring, got %+v", writeParams)
	}
	if got := memory.state.ArtifactVersions["doc:deliverable.brief"]; got != "artifact-1" {
		t.Fatalf("expected memory artifact version to be updated, got %+v", memory.state.ArtifactVersions)
	}
	snapshot := memory.store.Snapshot()
	foundStale := false
	for _, digest := range snapshot.Digests {
		if digest.DigestID == "task:task-1" && digest.Stale {
			foundStale = true
		}
	}
	if !foundStale {
		t.Fatalf("expected old artifact continuity digest to be invalidated, got %+v", snapshot.Digests)
	}
}

func TestResultArtifactRefsReturnsWorkspaceDocRef(t *testing.T) {
	refs := resultArtifactRefs(StructuredTaskResult{
		Materialize: TaskMaterialization{DocKey: "deliverable.brief"},
	})
	if len(refs) != 1 {
		t.Fatalf("expected one artifact ref, got %+v", refs)
	}
	if refs[0].Ref != "doc:deliverable.brief" || refs[0].Kind != "workspace_doc" || refs[0].ContentType != "text/markdown" {
		t.Fatalf("unexpected artifact refs: %+v", refs)
	}
}

func TestProtectMaterializedDocContentPreservesRichExistingDoc(t *testing.T) {
	existing := "# Acceptance Criteria\n\n" + strings.Repeat("- AC: detailed product criterion with evidence mapping.\n", 20)
	incoming := "Created a draft acceptance-criteria document with AC-01 through AC-09; status draft_pending_peer_review."

	protected, applied := protectMaterializedDocContent(
		"project.project-alpha.acceptance_criteria",
		existing,
		incoming,
		"alpha",
		"task-1",
		"session-1",
		"run-1",
		"done",
		"completed",
	)
	if !applied {
		t.Fatal("expected rich existing document to be protected from thin final materialization")
	}
	if !strings.Contains(protected, "# Acceptance Criteria") || !strings.Contains(protected, incoming) {
		t.Fatalf("expected protected content to keep existing doc and append final note, got:\n%s", protected)
	}
	if !strings.Contains(protected, "rhizome-final-materialize:alpha:run-1") {
		t.Fatalf("expected idempotency marker in protected content, got:\n%s", protected)
	}
}

func TestProtectMaterializedDocContentAllowsFullReplacement(t *testing.T) {
	existing := "# Old Doc\n\n" + strings.Repeat("- stale line\n", 80)
	incoming := "# New Doc\n\n" + strings.Repeat("- updated structured line\n", 80)

	protected, applied := protectMaterializedDocContent("project.project-alpha.design_plan", existing, incoming, "alpha", "task-1", "session-1", "run-1", "done", "completed")
	if applied {
		t.Fatalf("expected full structured replacement to pass through, got:\n%s", protected)
	}
	if protected != strings.TrimSpace(incoming) {
		t.Fatalf("expected incoming content unchanged, got:\n%s", protected)
	}
}
