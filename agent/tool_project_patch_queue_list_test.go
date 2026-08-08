package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProjectPatchQueueListToolReturnsSanitizedSelectors(t *testing.T) {
	var gotMethod string
	var gotParams map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		gotMethod = req.Method
		gotParams = req.Params
		writeRPCResult(w, req, map[string]any{
			"count": 2,
			"patch_queue_items": []map[string]any{
				{
					"queue_id":         "queue-accepted",
					"item_id":          "item-accepted",
					"workspace_id":     "ws",
					"project_id":       "project-demo",
					"repo_id":          "repo-1",
					"branch_id":        "branch-1",
					"state":            "ACCEPTED",
					"review_doc_key":   "project.project-demo.branch.branch-1.review",
					"head_sha":         "head-accepted",
					"base_sha":         "base-1",
					"claim_token":      "secret-token-must-not-leak",
					"decision_summary": "Approved after smoke.",
					"decided_by":       "agent-reviewer",
					"updated_at":       "2026-05-12T10:00:00Z",
				},
				{
					"queue_id":     "queue-rejected",
					"item_id":      "item-rejected",
					"workspace_id": "ws",
					"project_id":   "project-demo",
					"repo_id":      "repo-1",
					"branch_id":    "branch-1",
					"state":        "REJECTED",
					"head_sha":     "head-rejected",
				},
			},
		})
	}))
	defer server.Close()

	tool := NewProjectPatchQueueListTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "`project-demo`",
		"repo_id":    "'repo-1'",
		"branch_id":  "branch-1",
		"state":      "accepted",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected list success, got %+v", result)
	}
	if gotMethod != "project.patch_queue.list" {
		t.Fatalf("method = %q", gotMethod)
	}
	for key, want := range map[string]string{
		"workspace_id": "ws",
		"project_id":   "project-demo",
		"repo_id":      "repo-1",
		"branch_id":    "branch-1",
		"state":        "ACCEPTED",
	} {
		if got := rpcString(gotParams, key); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
	for _, want := range []string{"queue-accepted", "item-accepted", "queue-rejected", "project_patch_queue_integrate", `"count": 2`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	for _, forbidden := range []string{"secret-token-must-not-leak", "claim_token"} {
		if strings.Contains(result.Output, forbidden) {
			t.Fatalf("expected sanitized output to omit %q, got %q", forbidden, result.Output)
		}
	}
}

func TestProjectPatchQueueListToolUsesActiveProjectWhenModelPassesWorkspaceID(t *testing.T) {
	var gotParams map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "project.patch_queue.list" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		gotParams = req.Params
		writeRPCResult(w, req, map[string]any{
			"count":             0,
			"patch_queue_items": []map[string]any{},
		})
	}))
	defer server.Close()

	tool := NewProjectPatchQueueListTool(NewRhizomeClient(server.URL, "token"), "rhizome-main", "agent-alpha").
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-parser", ProjectID: "project-signal01-rq-product-first", SessionID: "session-1"}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "rhizome-main",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected active project correction success, got %+v", result)
	}
	if got := rpcString(gotParams, "project_id"); got != "project-signal01-rq-product-first" {
		t.Fatalf("project_id = %q, want active project", got)
	}
	if !strings.Contains(result.Output, `"project_id": "project-signal01-rq-product-first"`) {
		t.Fatalf("expected output to report active project, got %q", result.Output)
	}
}

func TestProjectPatchQueueListToolSurfacesBlockingVisualEvidenceRoute(t *testing.T) {
	headSHA := "4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f"
	evidenceDocKey := "task.visual_acceptance.blocking"
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.patch_queue.list":
			writeRPCResult(w, req, map[string]any{
				"count": 1,
				"patch_queue_items": []map[string]any{{
					"queue_id":         "patchq-project-demo",
					"item_id":          "patchitem-branch-1",
					"workspace_id":     "ws",
					"project_id":       "project-demo",
					"repo_id":          "repo-1",
					"branch_id":        "branch-1",
					"state":            "BLOCKED",
					"head_sha":         headSHA,
					"decision_summary": "Blocked before visual evidence existed.",
					"updated_at":       "2026-05-12T10:00:00Z",
				}},
			})
		case "workspace.doc.list":
			writeRPCResult(w, req, map[string]any{"docs": []map[string]any{{
				"doc_key": evidenceDocKey,
				"title":   "Visual acceptance for branch-1 4dc89d3c fail",
			}}})
		case "workspace.doc.get":
			writeRPCResult(w, req, WorkspaceDocRecord{
				DocKey: evidenceDocKey,
				Title:  "Visual acceptance for branch-1 4dc89d3c fail",
				Content: `schema: rhizome_visual_acceptance_v1
queue_id: patchq-project-demo
item_id: patchitem-branch-1
branch_id: branch-1
head_sha: 4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f
visual_verdict: fail
severity: blocking`,
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueListTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-demo",
		"state":      "BLOCKED",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected list success, got %+v", result)
	}
	for _, want := range []string{evidenceDocKey, `"visual_evidence_verdict": "fail"`, `"visual_evidence_blocking": true`, `"suggested_next_transition": "project_patch_queue_followup"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Join(calls, ",") != "project.patch_queue.list,workspace.doc.list,workspace.doc.get" {
		t.Fatalf("unexpected call sequence: %+v", calls)
	}
}

func TestProjectPatchQueueListToolSurfacesTaskBoundBlockingVisualEvidenceRoute(t *testing.T) {
	headSHA := "4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f"
	evidenceDocKey := "task.task-visual-refresh.visual_acceptance"
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.patch_queue.list":
			writeRPCResult(w, req, map[string]any{
				"count": 1,
				"patch_queue_items": []map[string]any{{
					"queue_id":         "patchq-project-demo",
					"item_id":          "patchitem-branch-1",
					"workspace_id":     "ws",
					"project_id":       "project-demo",
					"repo_id":          "repo-1",
					"branch_id":        "branch-1",
					"state":            "BLOCKED",
					"head_sha":         headSHA,
					"decision_summary": "Blocked before visual evidence existed.",
					"updated_at":       "2026-05-12T10:00:00Z",
				}},
			})
		case "workspace.doc.list":
			writeRPCResult(w, req, map[string]any{"docs": []map[string]any{{
				"doc_key": evidenceDocKey,
				"title":   "Clearpress Visual Acceptance - beta head 4dc89d3c provisional fail",
			}}})
		case "workspace.doc.get":
			writeRPCResult(w, req, WorkspaceDocRecord{
				DocKey: evidenceDocKey,
				Title:  "Clearpress Visual Acceptance - beta head 4dc89d3c provisional fail",
				Content: `# rhizome_visual_acceptance_v1

- task_id: task-clearpress-visual-acceptance-beta-head-4dc89d3c-refresh
- project_id: project-demo
- candidate_head_sha: 4dc89d3c714e9c1ef7067cd3fcef2b67bec13b8f
- candidate_kind: provisional_non_canonical_validation_checkout

## Verdict
- visual_verdict: fail
- severity: blocking`,
			})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueListTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-demo",
		"state":      "BLOCKED",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected list success, got %+v", result)
	}
	for _, want := range []string{evidenceDocKey, `"visual_evidence_verdict": "fail"`, `"visual_evidence_blocking": true`, `"suggested_next_transition": "project_patch_queue_followup"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if strings.Join(calls, ",") != "project.patch_queue.list,workspace.doc.list,workspace.doc.get" {
		t.Fatalf("unexpected call sequence: %+v", calls)
	}
}

func TestProjectPatchQueueListToolRegisteredAndAmbientAllowed(t *testing.T) {
	agent := &Agent{
		Client:      NewRhizomeClient("http://127.0.0.1/rpc", "token"),
		WorkspaceID: "ws",
		AgentID:     "agent-alpha",
	}
	names := agent.baseToolNames()
	if _, ok := names["project_patch_queue_list"]; !ok {
		t.Fatalf("expected registered project_patch_queue_list tool")
	}
	if !ambientAutonomyToolAllowed("project_patch_queue_list") {
		t.Fatalf("expected ambient autonomy to allow read-only patch queue list tool")
	}
}
