package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProjectPatchQueueMaterializeToolRecordsCandidateFiles(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	const content = "console.log('materialized');\n"
	if err := os.WriteFile(filepath.Join(dir, "web", "app.js"), []byte(content), 0o644); err != nil {
		t.Fatalf("write candidate file: %v", err)
	}
	expectedDigest := projectPatchQueueMaterializationContentDigest(content)

	calls := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueMaterializationCoordinationResult(dir, map[string]any{
				"state":                 "CLAIMED",
				"claimed_by":            "agent-alpha",
				"claim_token":           "claim-token-1",
				"operation_id":          "op-materialize-1",
				"operation_kind":        "repo_patch_apply",
				"cas_evidence_accepted": true,
				"cas_status":            "APPLIED",
				"cas_patch_digest":      "sha256-cas-patch",
				"cas_evaluation_digest": "sha256-cas-eval",
				"cas_result": map[string]any{
					"schema":       "cas_patch_apply.v1",
					"status":       "APPLIED",
					"patch_digest": "sha256-cas-patch",
					"paths": []map[string]any{{
						"path":           "web/app.js",
						"status":         "APPLIED",
						"base_hash":      "sha256-base",
						"candidate_hash": expectedDigest,
					}},
				},
			}))
		case "project.patch_queue.materialization_record":
			if got := rpcString(req.Params, "claim_token"); got != "claim-token-1" {
				t.Fatalf("unexpected claim_token %q", got)
			}
			materialization, ok := req.Params["materialization"].(map[string]any)
			if !ok {
				t.Fatalf("expected materialization object, got %+v", req.Params["materialization"])
			}
			if materialization["schema"] != projectPatchQueueMaterializationSchema ||
				materialization["queue_id"] != "patchq-project-subpixel-projrepo-1" ||
				materialization["item_id"] != "patchitem-branch-1" ||
				materialization["recorded_by"] != "agent-alpha" {
				t.Fatalf("unexpected materialization metadata: %+v", materialization)
			}
			files, ok := materialization["files"].([]any)
			if !ok || len(files) != 1 {
				t.Fatalf("expected one materialized file, got %+v", materialization["files"])
			}
			file, ok := files[0].(map[string]any)
			if !ok {
				t.Fatalf("expected file object, got %+v", files[0])
			}
			if file["path"] != "web/app.js" ||
				file["content"] != content ||
				file["content_digest"] != expectedDigest ||
				file["candidate_hash"] != expectedDigest ||
				file["content_encoding"] != projectPatchQueueMaterializationEncodingUTF8 {
				t.Fatalf("unexpected materialized file: %+v", file)
			}
			writeRPCResult(w, req, map[string]any{"patch_queue_item": patchQueueLifecycleItem(map[string]any{
				"state":                       "CLAIMED",
				"claimed_by":                  "agent-alpha",
				"claim_token":                 "claim-token-1",
				"cas_patch_digest":            "sha256-cas-patch",
				"cas_evaluation_digest":       "sha256-cas-eval",
				"materialization_accepted":    true,
				"materialization_digest":      "sha256-materialization",
				"materialization_recorded_by": "agent-alpha",
			})})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", dir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected success, got %+v", result)
	}
	if !strings.Contains(result.Output, `"materialization_digest": "sha256-materialization"`) ||
		!strings.Contains(result.Output, expectedDigest) ||
		!strings.Contains(result.Output, `"no_git_mutation": true`) {
		t.Fatalf("unexpected materialization output: %s", result.Output)
	}
	if strings.Contains(result.Output, content) || strings.Contains(result.Output, "console.log('materialized');\\n") {
		t.Fatalf("tool output must not expose raw candidate content: %s", result.Output)
	}
	if len(calls) != 2 || calls[0] != "project.coordination.get" || calls[1] != "project.patch_queue.materialization_record" {
		t.Fatalf("unexpected rpc calls: %+v", calls)
	}
}

func TestProjectPatchQueueMaterializeToolRejectsDigestMismatchBeforeRPC(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "web", "app.js"), []byte("actual\n"), 0o644); err != nil {
		t.Fatalf("write candidate file: %v", err)
	}

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, patchQueueMaterializationCoordinationResult(dir, map[string]any{
			"state":                 "CLAIMED",
			"claimed_by":            "agent-alpha",
			"claim_token":           "claim-token-1",
			"operation_id":          "op-materialize-1",
			"operation_kind":        "repo_patch_apply",
			"cas_evidence_accepted": true,
			"cas_status":            "APPLIED",
			"cas_patch_digest":      "sha256-cas-patch",
			"cas_evaluation_digest": "sha256-cas-eval",
			"cas_result": map[string]any{
				"schema":       "cas_patch_apply.v1",
				"status":       "APPLIED",
				"patch_digest": "sha256-cas-patch",
				"paths": []map[string]any{{
					"path":           "web/app.js",
					"status":         "APPLIED",
					"base_hash":      "sha256-base",
					"candidate_hash": projectPatchQueueMaterializationContentDigest("expected\n"),
				}},
			},
		}))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", dir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "does not match CAS candidate_hash") {
		t.Fatalf("expected digest mismatch error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected no materialization RPC after local digest mismatch, got %d calls", calls)
	}
}

func TestProjectPatchQueueMaterializeToolRequiresGitWorktreeForControlledQueue(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	const content = "controlled materialization\n"
	if err := os.WriteFile(filepath.Join(dir, "web", "app.js"), []byte(content), 0o644); err != nil {
		t.Fatalf("write candidate file: %v", err)
	}
	expectedDigest := projectPatchQueueMaterializationContentDigest(content)

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, patchQueueMaterializationCoordinationResult(dir, map[string]any{
			"state":                 "CLAIMED",
			"claimed_by":            "agent-alpha",
			"claim_token":           "claim-token-1",
			"repo_authority_mode":   "repoauthority_controlled_queue",
			"operation_id":          "op-materialize-1",
			"operation_kind":        "repo_patch_apply",
			"cas_evidence_accepted": true,
			"cas_status":            "APPLIED",
			"cas_patch_digest":      "sha256-cas-patch",
			"cas_evaluation_digest": "sha256-cas-eval",
			"cas_result": map[string]any{
				"schema":       projectPatchQueueCASApplySchema,
				"status":       projectPatchQueueCASStatusApplied,
				"patch_digest": "sha256-cas-patch",
				"paths": []map[string]any{{
					"path":           "web/app.js",
					"status":         projectPatchQueueCASStatusApplied,
					"base_hash":      "sha256-base",
					"candidate_hash": expectedDigest,
				}},
			},
		}))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", dir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "requires a git worktree") {
		t.Fatalf("expected controlled non-git checkout error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected no materialization RPC for non-git controlled checkout, got %d calls", calls)
	}
}

func TestProjectPatchQueueMaterializeToolUsesConcreteCASPathsForScopedPathset(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	const content = "scoped materialization\n"
	if err := os.WriteFile(filepath.Join(dir, "web", "app.js"), []byte(content), 0o644); err != nil {
		t.Fatalf("write candidate file: %v", err)
	}
	expectedDigest := projectPatchQueueMaterializationContentDigest(content)

	calls := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueMaterializationCoordinationResult(dir, map[string]any{
				"state":                 "CLAIMED",
				"claimed_by":            "agent-alpha",
				"claim_token":           "claim-token-1",
				"pathset":               []string{"web/**"},
				"operation_id":          "op-materialize-1",
				"operation_kind":        "repo_patch_apply",
				"cas_evidence_accepted": true,
				"cas_status":            "APPLIED",
				"cas_patch_digest":      "sha256-cas-patch",
				"cas_evaluation_digest": "sha256-cas-eval",
				"cas_result": map[string]any{
					"schema":       "cas_patch_apply.v1",
					"status":       "APPLIED",
					"patch_digest": "sha256-cas-patch",
					"paths": []map[string]any{{
						"path":           "web/app.js",
						"status":         "APPLIED",
						"base_hash":      "sha256-base",
						"candidate_hash": expectedDigest,
					}},
				},
			}))
		case "project.patch_queue.materialization_record":
			materialization := req.Params["materialization"].(map[string]any)
			files := materialization["files"].([]any)
			file := files[0].(map[string]any)
			if file["path"] != "web/app.js" || file["content"] != content {
				t.Fatalf("expected concrete CAS path materialization, got %+v", file)
			}
			writeRPCResult(w, req, map[string]any{"patch_queue_item": patchQueueLifecycleItem(map[string]any{
				"state":                    "CLAIMED",
				"claimed_by":               "agent-alpha",
				"claim_token":              "claim-token-1",
				"materialization_accepted": true,
				"materialization_digest":   "sha256-materialization",
			})})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", dir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected scoped pathset materialization success, got %+v", result)
	}
	if len(calls) != 2 || calls[1] != "project.patch_queue.materialization_record" {
		t.Fatalf("unexpected calls: %+v", calls)
	}
}

func TestProjectPatchQueueMaterializeToolAllowsRootWildcardScope(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	const content = "wildcard materialization\n"
	if err := os.WriteFile(filepath.Join(dir, "web", "app.js"), []byte(content), 0o644); err != nil {
		t.Fatalf("write candidate file: %v", err)
	}
	expectedDigest := projectPatchQueueMaterializationContentDigest(content)

	calls := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, patchQueueMaterializationCoordinationResult(dir, map[string]any{
				"state":                 "CLAIMED",
				"claimed_by":            "agent-alpha",
				"claim_token":           "claim-token-1",
				"pathset":               []string{"**"},
				"operation_id":          "op-materialize-1",
				"operation_kind":        "repo_patch_apply",
				"cas_evidence_accepted": true,
				"cas_status":            "APPLIED",
				"cas_patch_digest":      "sha256-cas-patch",
				"cas_evaluation_digest": "sha256-cas-eval",
				"cas_result": map[string]any{
					"schema":       "cas_patch_apply.v1",
					"status":       "APPLIED",
					"patch_digest": "sha256-cas-patch",
					"paths": []map[string]any{{
						"path":           "web/app.js",
						"status":         "APPLIED",
						"base_hash":      "sha256-base",
						"candidate_hash": expectedDigest,
					}},
				},
			}))
		case "project.patch_queue.materialization_record":
			materialization := req.Params["materialization"].(map[string]any)
			files := materialization["files"].([]any)
			file := files[0].(map[string]any)
			if file["path"] != "web/app.js" || file["content"] != content {
				t.Fatalf("expected wildcard scope to materialize concrete CAS path, got %+v", file)
			}
			writeRPCResult(w, req, map[string]any{"patch_queue_item": patchQueueLifecycleItem(map[string]any{
				"state":                    "CLAIMED",
				"claimed_by":               "agent-alpha",
				"claim_token":              "claim-token-1",
				"materialization_accepted": true,
				"materialization_digest":   "sha256-materialization",
			})})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", dir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected wildcard pathset materialization success, got %+v", result)
	}
	if len(calls) != 2 || calls[1] != "project.patch_queue.materialization_record" {
		t.Fatalf("unexpected calls: %+v", calls)
	}
}

func TestProjectPatchQueueMaterializeToolRejectsAmbiguousBranchSelector(t *testing.T) {
	dir := t.TempDir()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		result := patchQueueMaterializationCoordinationResult(dir, map[string]any{
			"state":      "CLAIMED",
			"claimed_by": "agent-alpha",
		})
		coordination := result["coordination"].(map[string]any)
		coordination["patch_queue_items"] = []map[string]any{
			patchQueueLifecycleItem(map[string]any{
				"queue_id":   "patchq-old",
				"item_id":    "patchitem-old",
				"state":      "PROPOSED",
				"claimed_by": "",
			}),
			patchQueueLifecycleItem(map[string]any{
				"queue_id":   "patchq-new",
				"item_id":    "patchitem-new",
				"state":      "PROPOSED",
				"claimed_by": "",
			}),
		}
		writeRPCResult(w, req, result)
	}))
	defer server.Close()

	tool := NewProjectPatchQueueMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", dir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "ambiguous") {
		t.Fatalf("expected ambiguous branch selector error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected no materialization RPC for ambiguous branch selector, got %d calls", calls)
	}
}

func patchQueueMaterializationCoordinationResult(checkoutPath string, itemOverrides map[string]any) map[string]any {
	result := patchQueueLifecycleCoordinationResult(itemOverrides)
	coordination := result["coordination"].(map[string]any)
	checkouts := coordination["checkouts"].([]map[string]any)
	checkouts[0]["local_path"] = checkoutPath
	coordination["checkouts"] = checkouts
	return result
}

func TestProjectPatchQueueMaterializationCheckoutPathPrefersRuntimeCheckoutAndSkipsTerminal(t *testing.T) {
	branch := ProjectBranchRecord{
		BranchID:     "branch-1",
		RepoID:       "repo-1",
		CheckoutID:   "checkout-archived",
		AgentID:      "agent-beta",
		ActiveTaskID: "task-impl",
		BranchName:   "agent/agent-beta/project-rq/task-impl",
		Status:       "READY_FOR_REVIEW",
	}
	coordination := ProjectCoordinationRecord{
		Branches: []ProjectBranchRecord{branch},
		Checkouts: []ProjectCheckoutRecord{
			{
				CheckoutID: "checkout-archived",
				RepoID:     "repo-1",
				AgentID:    "agent-beta",
				LocalPath:  "/tmp/archived",
				Status:     "ARCHIVED",
			},
			{
				CheckoutID:   "checkout-review",
				RepoID:       "repo-1",
				AgentID:      "agent-alpha",
				LocalPath:    "/tmp/review",
				BranchName:   branch.BranchName,
				ActiveTaskID: "task-review",
				Status:       "ACTIVE",
			},
			{
				CheckoutID: "checkout-stale-review",
				RepoID:     "repo-1",
				AgentID:    "agent-alpha",
				LocalPath:  "/tmp/stale-review",
				Status:     "STALE",
			},
		},
	}
	item := ProjectPatchQueueItemRecord{
		RepoID:    "repo-1",
		BranchID:  "branch-1",
		ClaimedBy: "agent-alpha",
	}
	got := projectPatchQueueMaterializationCheckoutPath(coordination, item, "agent-alpha", WorkspaceTaskRecord{TaskID: "task-review"})
	if got != "/tmp/review" {
		t.Fatalf("materialization checkout path = %q, want runtime review checkout", got)
	}
}
