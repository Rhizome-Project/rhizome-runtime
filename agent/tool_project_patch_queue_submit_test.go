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

func TestProjectPatchQueueSubmitToolSubmitsReadyBranch(t *testing.T) {
	repoRoot, baseSHA, headSHA, baseDigest, _ := setupPatchQueueGitRepo(t)
	baseTreeHash := testGitOutput(t, repoRoot, "rev-parse", baseSHA+"^{tree}")

	calls := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, controlledPatchQueueCoordinationResult(repoRoot, baseSHA, headSHA, nil))
		case "project.patch_queue.submit":
			if got := rpcString(req.Params, "workspace_id"); got != "ws" {
				t.Fatalf("unexpected workspace_id %q", got)
			}
			if got := rpcString(req.Params, "actor_id"); got != "agent-alpha" {
				t.Fatalf("unexpected actor_id %q", got)
			}
			if got := rpcString(req.Params, "repo_authority_mode"); got != "repoauthority_controlled_queue" {
				t.Fatalf("unexpected repo_authority_mode %q", got)
			}
			for key, want := range map[string]string{
				"task_id":        "task-1",
				"session_id":     "session-1",
				"run_id":         "run-1",
				"agent_id":       "agent-alpha",
				"base_tree_hash": baseTreeHash,
				"repo_lease_id":  "lease-1",
			} {
				if got := rpcString(req.Params, key); got != want {
					t.Fatalf("%s = %q, want %q; params=%+v", key, got, want, req.Params)
				}
			}
			baseHashes, ok := req.Params["base_file_hashes"].(map[string]any)
			if !ok || baseHashes["web/app.js"] != baseDigest {
				t.Fatalf("base_file_hashes = %+v, want web/app.js=%s", req.Params["base_file_hashes"], baseDigest)
			}
			if req.Params["auto_merge"] == true {
				t.Fatal("tool must not request auto_merge")
			}
			writeRPCResult(w, req, map[string]any{"patch_queue_item": controlledPatchQueueItem(baseSHA, headSHA, baseDigest, map[string]any{
				"state": "PROPOSED",
			})})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha").
		WithWorkdir(repoRoot).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{
				TaskID:                   "task-1",
				SessionID:                "session-1",
				RunID:                    "run-1",
				CapabilitySnapshotID:     "snapshot-1",
				CapabilitySnapshotSchema: daemonCapabilitySnapshotSchema,
			}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":    "project-subpixel",
		"branch_id":     "`branch-1`",
		"repo_lease_id": "lease-1",
		"lease_term":    1,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected success, got %+v", result)
	}
	if !strings.Contains(result.Output, `"state": "PROPOSED"`) || !strings.Contains(result.Output, `"no_git_mutation": true`) {
		t.Fatalf("unexpected output: %s", result.Output)
	}
	if len(calls) != 2 || calls[0] != "project.coordination.get" || calls[1] != "project.patch_queue.submit" {
		t.Fatalf("unexpected rpc calls: %+v", calls)
	}
}

func TestProjectPatchQueueSubmitToolRejectsNoopReviewedHead(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		result := patchQueueCoordinationResult(nil)
		coordination := result["coordination"].(map[string]any)
		branches := coordination["branches"].([]map[string]any)
		branches[0]["base_sha"] = branches[0]["head_sha"]
		writeRPCResult(w, req, result)
	}))
	defer server.Close()

	tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "head_sha must differ from base_sha") {
		t.Fatalf("expected no-op head rejection, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected no submit RPC, got %d calls", calls)
	}
}

func TestProjectPatchQueueSubmitToolRejectsSameHeadRejectedPatchQueueDecision(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, patchQueueCoordinationResult([]map[string]any{
			branchReviewPatchQueueItem("REJECTED", "head123", "UI advertises 24x24 but renders 16x16"),
		}))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected same-head rejected patch queue state to fail, got %+v", result)
	}
	for _, want := range []string{"REJECTED", "24x24", "new commit/head", "fresh patch queue item"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected rejected guidance to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 1 {
		t.Fatalf("expected no submit RPC for same-head rejection, got %d calls", calls)
	}
}

func TestProjectPatchQueueSubmitToolNoopsAcceptedTerminalBranchBeforeReadyCheck(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		result := patchQueueCoordinationResult([]map[string]any{
			branchReviewPatchQueueItem("ACCEPTED", "head123", "integrated to canonical main"),
		})
		coordination := result["coordination"].(map[string]any)
		branches := coordination["branches"].([]map[string]any)
		branches[0]["status"] = "MERGED"
		coordination["branches"] = branches
		writeRPCResult(w, req, result)
	}))
	defer server.Close()

	tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected terminal accepted no-op success, got %+v", result)
	}
	for _, want := range []string{`"state": "ACCEPTED"`, `"terminal_noop": true`, `"already_accepted": true`, `"already_integrated": true`, `"recommended_task_outcome": "completed"`, `"next_action": "close_stale_owner_submit_task"`, `"do_not_create_new_branch": true`, `"do_not_retry_submit": true`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected accepted no-op output to contain %q, got %s", want, result.Output)
		}
	}
	if calls != 1 {
		t.Fatalf("expected no submit RPC for accepted terminal no-op, got %d calls", calls)
	}
}

func TestProjectPatchQueueSubmitToolRejectsFreshItemAfterSameHeadAccepted(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("fresh item after ACCEPTED must not submit a replacement item, method=%s params=%+v", req.Method, req.Params)
		}
		result := patchQueueCoordinationResult([]map[string]any{
			branchReviewPatchQueueItem("ACCEPTED", "head123", "accepted but not integrated"),
		})
		writeRPCResult(w, req, result)
	}))
	defer server.Close()

	tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
		"item_id":    "patchitem-fresh",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "fresh queue identity") || !strings.Contains(result.Output, "ACCEPTED") {
		t.Fatalf("expected same-head accepted fresh item rejection, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected no submit RPC after same-head accepted fresh item rejection, got %d calls", calls)
	}
}

func TestProjectPatchQueueSubmitToolExplicitRejectedSelectorDoesNotNoopAcceptedSameHead(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		accepted := branchReviewPatchQueueItem("ACCEPTED", "head123", "older accepted")
		accepted["item_id"] = "patchitem-accepted"
		accepted["updated_at"] = "2026-05-08T02:00:00Z"
		rejected := branchReviewPatchQueueItem("REJECTED", "head123", "newer explicit rejection")
		rejected["item_id"] = "patchitem-rejected"
		rejected["updated_at"] = "2026-05-08T03:00:00Z"
		result := patchQueueCoordinationResult([]map[string]any{accepted, rejected})
		coordination := result["coordination"].(map[string]any)
		branches := coordination["branches"].([]map[string]any)
		branches[0]["status"] = "MERGED"
		coordination["branches"] = branches
		writeRPCResult(w, req, result)
	}))
	defer server.Close()

	tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
		"item_id":    "patchitem-rejected",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected explicit rejected selector to fail, got %+v", result)
	}
	if !strings.Contains(result.Output, "REJECTED") || strings.Contains(result.Output, "terminal_noop") || strings.Contains(result.Output, "already_accepted") {
		t.Fatalf("expected rejected selector guidance without accepted no-op, got %q", result.Output)
	}
	if calls != 1 {
		t.Fatalf("expected no submit RPC for explicit rejected selector, got %d calls", calls)
	}
}

func setupPatchQueueGitRepoWithReadmeChange(t *testing.T) (repoRoot, baseSHA, headSHA, baseDigest string) {
	t.Helper()
	dir := t.TempDir()
	runTestGit(t, dir, "init")
	runTestGit(t, dir, "config", "user.email", "agent@example.test")
	runTestGit(t, dir, "config", "user.name", "Agent Test")
	runTestGit(t, dir, "config", "core.autocrlf", "false")
	if err := os.MkdirAll(filepath.Join(dir, "cmd", "rq"), 0o755); err != nil {
		t.Fatalf("mkdir cmd/rq: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("# rq\n\nbase cli notes\n"), 0o644); err != nil {
		t.Fatalf("write base README: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cmd", "rq", "main.go"), []byte("package main\n\nfunc main() {}\n"), 0o644); err != nil {
		t.Fatalf("write base cmd/rq/main.go: %v", err)
	}
	runTestGit(t, dir, "add", "README.md", "cmd/rq/main.go")
	runTestGit(t, dir, "commit", "--no-gpg-sign", "-m", "base")
	baseSHA = testGitOutput(t, dir, "rev-parse", "HEAD")
	var err error
	baseDigest, err = projectPatchQueueGitObjectContentDigest(context.Background(), dir, baseSHA, "README.md")
	if err != nil {
		t.Fatalf("base README digest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("# rq\n\nupdated cli usage\n"), 0o644); err != nil {
		t.Fatalf("write head README: %v", err)
	}
	runTestGit(t, dir, "add", "README.md")
	runTestGit(t, dir, "commit", "--no-gpg-sign", "-m", "update readme")
	headSHA = testGitOutput(t, dir, "rev-parse", "HEAD")
	return dir, baseSHA, headSHA, baseDigest
}

func TestProjectPatchQueueSubmitToolDoesNotNoopForeignAcceptedTerminalBranch(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		result := patchQueueCoordinationResult([]map[string]any{
			branchReviewPatchQueueItem("ACCEPTED", "head123", "integrated to canonical main"),
		})
		coordination := result["coordination"].(map[string]any)
		branches := coordination["branches"].([]map[string]any)
		branches[0]["agent_id"] = "agent-beta"
		branches[0]["status"] = "MERGED"
		coordination["branches"] = branches
		writeRPCResult(w, req, result)
	}))
	defer server.Close()

	tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected foreign branch to remain owner-routed, got %+v", result)
	}
	for _, want := range []string{"owned by agent agent-beta", "Do not retry this owner-only tool"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected owner routing to contain %q, got %q", want, result.Output)
		}
	}
	if strings.Contains(result.Output, "terminal_noop") || strings.Contains(result.Output, "already_accepted") {
		t.Fatalf("foreign branch must not return accepted no-op, got %q", result.Output)
	}
	if calls != 1 {
		t.Fatalf("expected no submit RPC for foreign accepted branch, got %d calls", calls)
	}
}

func TestProjectPatchQueueSubmitToolTerminalBranchWithoutAcceptedItemDoesNotSuggestReviewReady(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		result := patchQueueCoordinationResult(nil)
		coordination := result["coordination"].(map[string]any)
		branches := coordination["branches"].([]map[string]any)
		branches[0]["status"] = "MERGED"
		coordination["branches"] = branches
		writeRPCResult(w, req, result)
	}))
	defer server.Close()

	tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected terminal branch without accepted evidence to fail, got %+v", result)
	}
	for _, want := range []string{"terminal branch", "no ACCEPTED patch queue item", "Do not create a replacement branch"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected terminal guard to contain %q, got %q", want, result.Output)
		}
	}
	for _, forbidden := range []string{"project_branch_review_ready", "project_branch_commit"} {
		if strings.Contains(result.Output, forbidden) {
			t.Fatalf("terminal guard must not suggest %q, got %q", forbidden, result.Output)
		}
	}
	if calls != 1 {
		t.Fatalf("expected no submit RPC for terminal branch guard, got %d calls", calls)
	}
}

func TestProjectPatchQueueSubmitToolRequiresFreshItemForSameHeadBlockedPatchQueueDecision(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, patchQueueCoordinationResult([]map[string]any{
			branchReviewPatchQueueItem("BLOCKED", "head123", "Missing browser smoke evidence"),
		}))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected same-head blocked patch queue state to require explicit queue action, got %+v", result)
	}
	for _, want := range []string{"fresh item_id", "validation evidence", "BLOCKED", "Missing browser smoke evidence", "queue-facing"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected blocked guidance to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 1 {
		t.Fatalf("expected no submit RPC for blocked default item, got %d calls", calls)
	}
}

func TestProjectPatchQueueSubmitToolRequiresEvidenceForSameHeadBlockedRequeue(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, patchQueueCoordinationResult([]map[string]any{
			branchReviewPatchQueueItem("BLOCKED", "head123", "Missing browser smoke evidence"),
		}))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":         "project-subpixel",
		"branch_id":          "branch-1",
		"item_id":            "patchitem-branch-1-requeue-1",
		"supersedes_item_id": "patchitem-branch-1",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected same-head blocked requeue without evidence to fail, got %+v", result)
	}
	for _, want := range []string{"validation_doc_key", "evidence_doc_key", "branch_id=branch-1", "head_sha=head123"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected evidence guidance to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 1 {
		t.Fatalf("expected no submit RPC without evidence doc key, got %d calls", calls)
	}
}

func TestProjectPatchQueueSubmitToolAllowsEvidenceBoundSameHeadBlockedRequeue(t *testing.T) {
	repoRoot, baseSHA, headSHA, baseDigest, _ := setupPatchQueueGitRepo(t)
	baseTreeHash := testGitOutput(t, repoRoot, "rev-parse", baseSHA+"^{tree}")

	calls := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			result := controlledPatchQueueCoordinationResult(repoRoot, baseSHA, headSHA, nil)
			coordination := result["coordination"].(map[string]any)
			blocked := branchReviewPatchQueueItem("BLOCKED", headSHA, "Missing browser smoke evidence")
			blocked["base_sha"] = baseSHA
			coordination["patch_queue_items"] = []map[string]any{blocked}
			writeRPCResult(w, req, result)
		case "workspace.doc.get":
			if got := rpcString(req.Params, "doc_key"); got != "task.task-patchq-validation.validation_evidence" {
				t.Fatalf("doc_key = %q, want validation evidence doc", got)
			}
			writeRPCResult(w, req, map[string]any{
				"doc_key":    "task.task-patchq-validation.validation_evidence",
				"title":      "Validation Evidence",
				"content":    "queue_id: patchq-project-subpixel-projrepo-1\nitem_id: patchitem-branch-1\nbranch_id: branch-1\nhead_sha: " + headSHA + "\nbrowser smoke passed",
				"updated_by": "agent-reviewer",
			})
		case "project.patch_queue.submit":
			if got := rpcString(req.Params, "item_id"); got != "patchitem-branch-1-requeue-1" {
				t.Fatalf("item_id = %q, want fresh requeue item", got)
			}
			if got := rpcString(req.Params, "supersedes_item_id"); got != "patchitem-branch-1" {
				t.Fatalf("supersedes_item_id = %q, want patchitem-branch-1", got)
			}
			if got := rpcString(req.Params, "evidence_doc_key"); got != "task.task-patchq-validation.validation_evidence" {
				t.Fatalf("evidence_doc_key = %q, want validation doc key", got)
			}
			if got := rpcString(req.Params, "branch_id"); got != "branch-1" {
				t.Fatalf("branch_id = %q, want branch-1", got)
			}
			if got := rpcString(req.Params, "head_sha"); got != headSHA {
				t.Fatalf("head_sha = %q, want same reviewed head", got)
			}
			if got := rpcString(req.Params, "repo_authority_mode"); got != "repoauthority_controlled_queue" {
				t.Fatalf("repo_authority_mode = %q", got)
			}
			if got := rpcString(req.Params, "base_tree_hash"); got != baseTreeHash {
				t.Fatalf("base_tree_hash = %q, want %q", got, baseTreeHash)
			}
			writeRPCResult(w, req, map[string]any{"patch_queue_item": controlledPatchQueueItem(baseSHA, headSHA, baseDigest, map[string]any{
				"item_id":             "patchitem-branch-1-requeue-1",
				"supersedes_queue_id": "patchq-project-subpixel-projrepo-1",
				"supersedes_item_id":  "patchitem-branch-1",
				"evidence_doc_key":    "task.task-patchq-validation.validation_evidence",
				"state":               "PROPOSED",
			})})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha").
		WithWorkdir(repoRoot).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{
				TaskID:                   "task-1",
				SessionID:                "session-1",
				RunID:                    "run-1",
				CapabilitySnapshotID:     "snapshot-1",
				CapabilitySnapshotSchema: daemonCapabilitySnapshotSchema,
			}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":          "project-subpixel",
		"branch_id":           "branch-1",
		"item_id":             "patchitem-branch-1-requeue-1",
		"supersedes_queue_id": "patchq-project-subpixel-projrepo-1",
		"supersedes_item_id":  "patchitem-branch-1",
		"validation_doc_key":  "task.task-patchq-validation.validation_evidence",
		"repo_lease_id":       "lease-1",
		"lease_term":          1,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected evidence-bound same-head requeue to submit, got %+v", result)
	}
	if !strings.Contains(result.Output, `"item_id": "patchitem-branch-1-requeue-1"`) || !strings.Contains(result.Output, `"already_queued": false`) {
		t.Fatalf("unexpected requeue output: %s", result.Output)
	}
	if len(calls) != 3 || calls[0] != "project.coordination.get" || calls[1] != "workspace.doc.get" || calls[2] != "project.patch_queue.submit" {
		t.Fatalf("expected coordination then submit RPC, got %+v", calls)
	}
}

func TestProjectPatchQueueSubmitToolRejectsActiveBranch(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
			AgentID:        "agent-alpha",
			BranchStatus:   "ACTIVE",
			WriteScopeJSON: `{"paths":["web/app.js"]}`,
		}))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "READY_FOR_REVIEW") || !strings.Contains(result.Output, "project_branch_commit") {
		t.Fatalf("expected READY_FOR_REVIEW error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination call, got %d", calls)
	}
}

func TestProjectPatchQueueSubmitToolRejectsReadyBranchWithoutReviewDoc(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
			AgentID:        "agent-alpha",
			BranchStatus:   "READY_FOR_REVIEW",
			WriteScopeJSON: `{"paths":["web/app.js"]}`,
		}))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "review_doc_key") || !strings.Contains(result.Output, "project_branch_review_ready") {
		t.Fatalf("expected review_doc_key recovery error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination call, got %d", calls)
	}
}

func TestProjectPatchQueueSubmitToolRejectsHeadSHAOverrideMismatch(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, patchQueueCoordinationResult(nil))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
		"head_sha":   "different-head",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "head_sha override") {
		t.Fatalf("expected head_sha mismatch error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected no submit call, got %d calls", calls)
	}
}

func TestProjectPatchQueueSubmitToolReusesExistingQueueItem(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, patchQueueCoordinationResult([]map[string]any{controlledPatchQueueItem("base123", "head123", "sha256:web", map[string]any{
			"queue_id":       "patchq-existing",
			"item_id":        "patchitem-existing",
			"review_doc_key": "project.project-subpixel.branch.branch-1.review",
			"repo_root":      "C:/fixtures/agents/agent-alpha/subpixel",
			"base_tree_hash": "tree-existing",
			"state":          "PROPOSED",
		})}))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError || !strings.Contains(result.Output, `"already_queued": true`) {
		t.Fatalf("expected existing queue item reuse, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected no submit call when already queued, got %d calls", calls)
	}
}

func TestProjectPatchQueueSubmitToolReusesClaimedQueueItem(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, patchQueueCoordinationResult([]map[string]any{controlledPatchQueueItem("base123", "head123", "sha256:web", map[string]any{
			"queue_id":       "patchq-existing",
			"item_id":        "patchitem-existing",
			"review_doc_key": "project.project-subpixel.branch.branch-1.review",
			"repo_root":      "C:/fixtures/agents/agent-alpha/subpixel",
			"base_tree_hash": "tree-existing",
			"state":          "CLAIMED",
			"claimed_by":     "agent-reviewer",
		})}))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || result.IsError || !strings.Contains(result.Output, `"already_queued": true`) || !strings.Contains(result.Output, `"state": "CLAIMED"`) {
		t.Fatalf("expected claimed queue item reuse, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected no submit call when already claimed, got %d calls", calls)
	}
}

func TestProjectPatchQueueSubmitToolControlledQueueBindsRuntimeAndBaseRefs(t *testing.T) {
	repoRoot, baseSHA, headSHA, baseDigest, _ := setupPatchQueueGitRepo(t)
	baseTreeHash := testGitOutput(t, repoRoot, "rev-parse", baseSHA+"^{tree}")

	calls := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, controlledPatchQueueCoordinationResult(repoRoot, baseSHA, headSHA, nil))
		case "project.patch_queue.submit":
			if got := rpcString(req.Params, "repo_authority_mode"); got != "repoauthority_controlled_queue" {
				t.Fatalf("repo_authority_mode = %q", got)
			}
			for key, want := range map[string]string{
				"task_id":                    "task-1",
				"session_id":                 "session-1",
				"run_id":                     "run-1",
				"agent_id":                   "agent-alpha",
				"principal_type":             "agent",
				"principal_id":               "agent-alpha",
				"capability_snapshot_id":     "snapshot-1",
				"capability_snapshot_schema": daemonCapabilitySnapshotSchema,
				"base_tree_hash":             baseTreeHash,
				"repo_lease_id":              "lease-1",
			} {
				if got := rpcString(req.Params, key); got != want {
					t.Fatalf("%s = %q, want %q; params=%+v", key, got, want, req.Params)
				}
			}
			if got := rpcString(req.Params, "repo_root"); !strings.EqualFold(got, repoRoot) {
				t.Fatalf("repo_root = %q, want %q", got, repoRoot)
			}
			if _, ok := req.Params["operation_id"]; ok {
				t.Fatalf("submit must not bind mutation operation_id, got %+v", req.Params)
			}
			baseHashes, ok := req.Params["base_file_hashes"].(map[string]any)
			if !ok || baseHashes["web/app.js"] != baseDigest {
				t.Fatalf("base_file_hashes = %+v, want web/app.js=%s", req.Params["base_file_hashes"], baseDigest)
			}
			if got, ok := req.Params["lease_term"].(float64); !ok || got <= 0 {
				t.Fatalf("lease_term must be positive, got %+v", req.Params["lease_term"])
			}
			writeRPCResult(w, req, map[string]any{"patch_queue_item": controlledPatchQueueItem(baseSHA, headSHA, baseDigest, map[string]any{
				"state": "PROPOSED",
			})})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha").
		WithWorkdir(repoRoot).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{
				TaskID:                   "task-1",
				SessionID:                "session-1",
				RunID:                    "run-1",
				CapabilitySnapshotID:     "snapshot-1",
				CapabilitySnapshotSchema: daemonCapabilitySnapshotSchema,
			}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-subpixel",
		"branch_id":        "branch-1",
		"controlled_queue": true,
		"repo_lease_id":    "lease-1",
		"lease_term":       1,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected controlled submit success, got %+v", result)
	}
	if !strings.Contains(result.Output, `"repo_authority_mode": "repoauthority_controlled_queue"`) ||
		!strings.Contains(result.Output, `"no_git_mutation": true`) {
		t.Fatalf("unexpected output: %s", result.Output)
	}
	if len(calls) != 2 || calls[0] != "project.coordination.get" || calls[1] != "project.patch_queue.submit" {
		t.Fatalf("unexpected RPC calls: %+v", calls)
	}
}

func TestProjectPatchQueueSubmitToolControlledQueueBindsRootReadmePath(t *testing.T) {
	repoRoot, baseSHA, headSHA, baseDigest := setupPatchQueueGitRepoWithReadmeChange(t)
	baseTreeHash := testGitOutput(t, repoRoot, "rev-parse", baseSHA+"^{tree}")

	calls := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			result := controlledPatchQueueCoordinationResult(repoRoot, baseSHA, headSHA, nil)
			coordination := result["coordination"].(map[string]any)
			branches := coordination["branches"].([]map[string]any)
			branches[0]["write_scope_json"] = `{"paths":["cmd/rq/**","README.md"]}`
			coordination["branches"] = branches
			writeRPCResult(w, req, result)
		case "project.patch_queue.submit":
			if got := rpcString(req.Params, "base_tree_hash"); got != baseTreeHash {
				t.Fatalf("base_tree_hash = %q, want %q", got, baseTreeHash)
			}
			baseHashes, ok := req.Params["base_file_hashes"].(map[string]any)
			if !ok || baseHashes["README.md"] != baseDigest {
				t.Fatalf("base_file_hashes = %+v, want README.md=%s", req.Params["base_file_hashes"], baseDigest)
			}
			if _, ok := baseHashes["cmd/rq"]; ok {
				t.Fatalf("base_file_hashes must only include concrete changed paths, got %+v", baseHashes)
			}
			writeRPCResult(w, req, map[string]any{"patch_queue_item": controlledPatchQueueItem(baseSHA, headSHA, "", map[string]any{
				"state":            "PROPOSED",
				"pathset":          []string{"cmd/rq/**", "README.md"},
				"pathset_json":     `{"paths":["cmd/rq/**","README.md"]}`,
				"base_file_hashes": map[string]string{"README.md": baseDigest},
			})})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha").
		WithWorkdir(repoRoot).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{
				TaskID:                   "task-1",
				SessionID:                "session-1",
				RunID:                    "run-1",
				CapabilitySnapshotID:     "snapshot-1",
				CapabilitySnapshotSchema: daemonCapabilitySnapshotSchema,
			}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-subpixel",
		"branch_id":        "branch-1",
		"controlled_queue": true,
		"repo_lease_id":    "lease-1",
		"lease_term":       1,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected controlled README submit success, got %+v", result)
	}
	if !strings.Contains(result.Output, `"state": "PROPOSED"`) ||
		!strings.Contains(result.Output, `"README.md"`) ||
		!strings.Contains(result.Output, `"no_git_mutation": true`) {
		t.Fatalf("unexpected output: %s", result.Output)
	}
	if len(calls) != 2 || calls[0] != "project.coordination.get" || calls[1] != "project.patch_queue.submit" {
		t.Fatalf("unexpected RPC calls: %+v", calls)
	}
}

func TestProjectPatchQueueSubmitToolControlledQueueRecoversFromStaleLocalPathAndBaseHash(t *testing.T) {
	repoRoot, baseSHA, headSHA, baseDigest, _ := setupPatchQueueGitRepo(t)
	baseTreeHash := testGitOutput(t, repoRoot, "rev-parse", baseSHA+"^{tree}")
	staleDir := t.TempDir()

	calls := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			result := controlledPatchQueueCoordinationResult(repoRoot, baseSHA, headSHA, nil)
			coordination := result["coordination"].(map[string]any)
			checkouts := coordination["checkouts"].([]map[string]any)
			checkouts[0]["status"] = "ABANDONED"
			coordination["checkouts"] = checkouts
			writeRPCResult(w, req, result)
		case "project.patch_queue.submit":
			if got := rpcString(req.Params, "repo_root"); !strings.EqualFold(got, repoRoot) {
				t.Fatalf("repo_root = %q, want recovered checkout %q", got, repoRoot)
			}
			if got := rpcString(req.Params, "base_tree_hash"); got != baseTreeHash {
				t.Fatalf("base_tree_hash = %q, want %q", got, baseTreeHash)
			}
			baseHashes, ok := req.Params["base_file_hashes"].(map[string]any)
			if !ok || baseHashes["web/app.js"] != baseDigest {
				t.Fatalf("base_file_hashes = %+v, want git-derived web/app.js=%s", req.Params["base_file_hashes"], baseDigest)
			}
			writeRPCResult(w, req, map[string]any{"patch_queue_item": controlledPatchQueueItem(baseSHA, headSHA, baseDigest, map[string]any{
				"state": "PROPOSED",
			})})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha").
		WithWorkdir(staleDir).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{
				TaskID:                   "task-1",
				SessionID:                "session-1",
				RunID:                    "run-1",
				CapabilitySnapshotID:     "snapshot-1",
				CapabilitySnapshotSchema: daemonCapabilitySnapshotSchema,
			}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":            "project-subpixel",
		"branch_id":             "branch-1",
		"controlled_queue":      true,
		"local_path":            staleDir,
		"base_file_hashes_json": `{"web/app.js":"sha256:stale"}`,
		"repo_lease_id":         "lease-1",
		"lease_term":            1,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected controlled submit to recover binding refs, got %+v", result)
	}
	if len(calls) != 2 || calls[0] != "project.coordination.get" || calls[1] != "project.patch_queue.submit" {
		t.Fatalf("unexpected RPC calls: %+v", calls)
	}
}

func TestProjectPatchQueueSubmitToolControlledQueueDerivesLeaseRefs(t *testing.T) {
	repoRoot, baseSHA, headSHA, baseDigest, _ := setupPatchQueueGitRepo(t)
	baseTreeHash := testGitOutput(t, repoRoot, "rev-parse", baseSHA+"^{tree}")
	expectedLeaseID := projectPatchQueueGeneratedRepoLeaseID("ws", "project-subpixel", "patchq-project-subpixel-projrepo-1", "patchitem-branch-1", "agent-alpha", "run-1")

	calls := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, controlledPatchQueueCoordinationResult(repoRoot, baseSHA, headSHA, nil))
		case "project.patch_queue.submit":
			for key, want := range map[string]string{
				"task_id":                    "task-1",
				"session_id":                 "session-1",
				"run_id":                     "run-1",
				"agent_id":                   "agent-alpha",
				"capability_snapshot_id":     "snapshot-1",
				"capability_snapshot_schema": daemonCapabilitySnapshotSchema,
				"base_tree_hash":             baseTreeHash,
				"repo_lease_id":              expectedLeaseID,
			} {
				if got := rpcString(req.Params, key); got != want {
					t.Fatalf("%s = %q, want %q; params=%+v", key, got, want, req.Params)
				}
			}
			if got, ok := req.Params["lease_term"].(float64); !ok || got != 1 {
				t.Fatalf("lease_term = %+v, want 1", req.Params["lease_term"])
			}
			baseHashes, ok := req.Params["base_file_hashes"].(map[string]any)
			if !ok || baseHashes["web/app.js"] != baseDigest {
				t.Fatalf("base_file_hashes = %+v, want web/app.js=%s", req.Params["base_file_hashes"], baseDigest)
			}
			writeRPCResult(w, req, map[string]any{"patch_queue_item": controlledPatchQueueItem(baseSHA, headSHA, baseDigest, map[string]any{
				"state":         "PROPOSED",
				"repo_lease_id": expectedLeaseID,
				"lease_term":    1,
			})})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha").
		WithWorkdir(repoRoot).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{
				TaskID:                   "task-1",
				SessionID:                "session-1",
				RunID:                    "run-1",
				CapabilitySnapshotID:     "snapshot-1",
				CapabilitySnapshotSchema: daemonCapabilitySnapshotSchema,
			}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-subpixel",
		"branch_id":        "branch-1",
		"controlled_queue": true,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected controlled submit success with derived lease refs, got %+v", result)
	}
	if len(calls) != 2 || calls[0] != "project.coordination.get" || calls[1] != "project.patch_queue.submit" {
		t.Fatalf("unexpected RPC calls: %+v", calls)
	}
}

func TestProjectPatchQueueSubmitToolControlledQueueFailsClosedOnPartialLeaseRefs(t *testing.T) {
	for _, tc := range []struct {
		name string
		args map[string]any
		want string
	}{
		{
			name: "repo lease without term",
			args: map[string]any{
				"repo_lease_id": "lease-1",
			},
			want: "missing lease_term",
		},
		{
			name: "term without repo lease",
			args: map[string]any{
				"lease_term": 1,
			},
			want: "missing repo_lease_id",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repoRoot, baseSHA, headSHA, _, _ := setupPatchQueueGitRepo(t)
			calls := make([]string, 0, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				req := decodeRPCRequest(t, r)
				calls = append(calls, req.Method)
				if req.Method != "project.coordination.get" {
					t.Fatalf("unexpected method %q", req.Method)
				}
				writeRPCResult(w, req, controlledPatchQueueCoordinationResult(repoRoot, baseSHA, headSHA, nil))
			}))
			defer server.Close()

			tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha").
				WithWorkdir(repoRoot).
				WithRuntimeBinding(func() AgentRuntimeBinding {
					return AgentRuntimeBinding{
						TaskID:                   "task-1",
						SessionID:                "session-1",
						RunID:                    "run-1",
						CapabilitySnapshotID:     "snapshot-1",
						CapabilitySnapshotSchema: daemonCapabilitySnapshotSchema,
					}
				})
			args := map[string]any{
				"project_id":       "project-subpixel",
				"branch_id":        "branch-1",
				"controlled_queue": true,
			}
			for key, value := range tc.args {
				args[key] = value
			}
			result := tool.Execute(context.Background(), args)
			if result == nil || !result.IsError || !strings.Contains(result.Output, tc.want) {
				t.Fatalf("expected partial lease ref failure containing %q, got %+v", tc.want, result)
			}
			if len(calls) != 1 || calls[0] != "project.coordination.get" {
				t.Fatalf("expected no submit RPC for partial lease refs, got %+v", calls)
			}
		})
	}
}

func TestProjectPatchQueueSubmitToolControlledQueueFailsClosedWithoutRuntimeRefs(t *testing.T) {
	repoRoot, baseSHA, headSHA, _, _ := setupPatchQueueGitRepo(t)

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, controlledPatchQueueCoordinationResult(repoRoot, baseSHA, headSHA, nil))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha").WithWorkdir(repoRoot)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-subpixel",
		"branch_id":        "branch-1",
		"controlled_queue": true,
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "controlled_queue requires complete binding refs") {
		t.Fatalf("expected missing refs error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected no submit RPC without binding refs, got %d calls", calls)
	}
}

func TestProjectPatchQueueSubmitToolControlledQueueCancelReplacesStalePatchOnlyProposal(t *testing.T) {
	repoRoot, baseSHA, headSHA, baseDigest, _ := setupPatchQueueGitRepo(t)
	baseTreeHash := testGitOutput(t, repoRoot, "rev-parse", baseSHA+"^{tree}")
	staleHeadSHA := strings.Repeat("9", 40)

	calls := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			result := controlledPatchQueueCoordinationResult(repoRoot, baseSHA, headSHA, nil)
			coordination := result["coordination"].(map[string]any)
			coordination["patch_queue_items"] = []map[string]any{patchQueueLifecycleItem(map[string]any{
				"queue_id":            "patchq-existing",
				"item_id":             "patchitem-existing",
				"repo_authority_mode": "patch_only_temp_repo",
				"state":               "PROPOSED",
				"base_sha":            baseSHA,
				"head_sha":            staleHeadSHA,
			})}
			writeRPCResult(w, req, result)
		case "project.patch_queue.submit":
			for key, want := range map[string]string{
				"repo_authority_mode": "repoauthority_controlled_queue",
				"queue_id":            "patchq-existing",
				"item_id":             "patchitem-existing-controlled",
				"supersedes_queue_id": "patchq-existing",
				"supersedes_item_id":  "patchitem-existing",
				"task_id":             "task-1",
				"session_id":          "session-1",
				"run_id":              "run-1",
				"agent_id":            "agent-alpha",
				"base_tree_hash":      baseTreeHash,
				"repo_lease_id":       "lease-1",
			} {
				if got := rpcString(req.Params, key); got != want {
					t.Fatalf("%s = %q, want %q; params=%+v", key, got, want, req.Params)
				}
			}
			baseHashes, ok := req.Params["base_file_hashes"].(map[string]any)
			if !ok || baseHashes["web/app.js"] != baseDigest {
				t.Fatalf("base_file_hashes = %+v, want web/app.js=%s", req.Params["base_file_hashes"], baseDigest)
			}
			writeRPCResult(w, req, map[string]any{"patch_queue_item": controlledPatchQueueItem(baseSHA, headSHA, baseDigest, map[string]any{
				"queue_id":            "patchq-existing",
				"item_id":             "patchitem-existing-controlled",
				"supersedes_queue_id": "patchq-existing",
				"supersedes_item_id":  "patchitem-existing",
				"repo_authority_mode": "repoauthority_controlled_queue",
				"state":               "PROPOSED",
			})})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueSubmitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha").
		WithWorkdir(repoRoot).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{
				TaskID:                   "task-1",
				SessionID:                "session-1",
				RunID:                    "run-1",
				CapabilitySnapshotID:     "snapshot-1",
				CapabilitySnapshotSchema: daemonCapabilitySnapshotSchema,
			}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-subpixel",
		"branch_id":        "branch-1",
		"controlled_queue": true,
		"repo_lease_id":    "lease-1",
		"lease_term":       1,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected cancel+replace success, got %+v", result)
	}
	for _, want := range []string{`"queue_id": "patchq-existing"`, `"item_id": "patchitem-existing-controlled"`, `"supersedes_item_id": "patchitem-existing"`, "Legacy patch-only proposal"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if len(calls) != 2 || calls[0] != "project.coordination.get" || calls[1] != "project.patch_queue.submit" {
		t.Fatalf("unexpected RPC calls: %+v", calls)
	}
}

func patchQueueCoordinationResult(items []map[string]any) map[string]any {
	result := branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		BranchStatus:   "READY_FOR_REVIEW",
		WriteScopeJSON: `{"paths":["web/app.js"]}`,
	})
	coordination := result["coordination"].(map[string]any)
	branches := coordination["branches"].([]map[string]any)
	branches[0]["review_doc_key"] = "project.project-subpixel.branch.branch-1.review"
	branches[0]["head_sha"] = "head123"
	branches[0]["base_sha"] = "base123"
	coordination["branches"] = branches
	if items != nil {
		coordination["patch_queue_items"] = items
	}
	return result
}
