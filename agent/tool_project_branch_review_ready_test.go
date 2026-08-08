package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestProjectBranchReviewReadyToolWritesPacketAndMarksBranch(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	branchName := "agent/agent-alpha/project-subpixel/task-build"
	remote := seedBareGitRepoWithBranch(t, branchName, filepath.Join("web", "app.js"), "export const ready = true;\n")
	headSHA := gitOutput(t, remote, "rev-parse", branchName)
	baseSHA := gitOutput(t, remote, "rev-parse", "main")

	calls := 0
	var docContent string
	var branchParams map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
				AgentID:        "agent-alpha",
				RepoURL:        remote,
				BranchStatus:   "ACTIVE",
				BranchHeadSHA:  headSHA,
				BranchBaseSHA:  baseSHA,
				WriteScopeJSON: `{"paths":["web/app.js","web/style.css"]}`,
				CheckoutPath:   "C:/work/project",
			}))
		case 2:
			if req.Method != "workspace.doc.get" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcString(req.Params, "doc_key"); got != "project.project-subpixel.source_refs" {
				t.Fatalf("source refs doc_key = %q", got)
			}
			writeRPCResult(w, req, WorkspaceDocRecord{
				DocKey: "project.project-subpixel.source_refs",
				Title:  "Source refs",
				Content: "```rhizome_source_refs_v1\n" +
					"project_id: project-subpixel\n" +
					"source_doc_keys:\n" +
					"- operator.clearpress\n" +
					"```\n",
			})
		case 3:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			if got := rpcString(req.Params, "doc_key"); got != "project.project-subpixel.branch.branch-1.review" {
				t.Fatalf("doc_key = %q", got)
			}
			docContent = rpcString(req.Params, "content")
			for _, want := range []string{"Branch Review Packet", "READY_FOR_REVIEW", "Runnable/Review Provenance", "checkout_path: C:/work/project", "core user transformation", "Source Artifact Fidelity", "source_refs_doc_key: project.project-subpixel.source_refs", "source_requirements_trace_doc_key: project.project-subpixel.source_requirements_trace", "patch_queue_acceptance_gate", "Patch Queue Candidate", "auto_merge_enabled: false", "Visual Acceptance Protocol", "rhizome_visual_acceptance_v1", "web/app.js", "web/style.css"} {
				if !strings.Contains(docContent, want) {
					t.Fatalf("review doc missing %q:\n%s", want, docContent)
				}
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-review"})
		case 4:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			branchParams = req.Params
			if got := rpcString(req.Params, "status"); got != "READY_FOR_REVIEW" {
				t.Fatalf("status = %q", got)
			}
			if got := rpcString(req.Params, "head_sha"); got != headSHA {
				t.Fatalf("head_sha = %q", got)
			}
			if got := rpcString(req.Params, "review_doc_key"); got != "project.project-subpixel.branch.branch-1.review" {
				t.Fatalf("review_doc_key = %q", got)
			}
			if got := rpcString(req.Params, "active_task_id"); got != "" {
				t.Fatalf("READY_FOR_REVIEW should not preserve live active refs by default, got %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"branch": map[string]any{
				"branch_id":        "branch-1",
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      "checkout-1",
				"agent_id":         "agent-alpha",
				"branch_name":      "agent/agent-alpha/project-subpixel/task-build",
				"branch_kind":      "feature",
				"base_branch":      "main",
				"head_sha":         headSHA,
				"base_sha":         baseSHA,
				"write_scope_json": `{"paths":["web/app.js","web/style.css"]}`,
				"review_doc_key":   "project.project-subpixel.branch.branch-1.review",
				"status":           "READY_FOR_REVIEW",
			}})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchReviewReadyTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "C:/work/project")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":                  "project-subpixel",
		"branch_id":                   "branch-1",
		"review_summary":              "Frontend slice implemented and smoke-checked.",
		"verification_status":         "passed",
		"verification_command":        "go test ./...",
		"verification_exit_code":      0,
		"verification_output_summary": "ok",
		"head_sha":                    headSHA,
		"base_sha":                    baseSHA,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful review ready, got %+v", result)
	}
	if branchParams == nil {
		t.Fatalf("expected branch register params")
	}
	for _, want := range []string{"branch-1", "project.project-subpixel.branch.branch-1.review", `"receipt_state": "branch_registry_ready_for_review"`, `"mandatory_next_tool": "project_patch_queue_submit"`, "mandatory_next_tool=project_patch_queue_submit", `"patch_queue_auto_submit": "skipped"`, `"patch_queue_auto_submit_skipped_reason": "runtime binding unavailable; project_patch_queue_submit remains mandatory"`, `"auto_merge_enabled": false`, `"no_git_mutation": true`, `"remote_branch_published": true`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 4 {
		t.Fatalf("expected 4 calls, got %d", calls)
	}
}

func TestRenderProjectBranchReviewDocCarriesLaneMapAndIntegrationBoundary(t *testing.T) {
	doc := renderProjectBranchReviewDoc(projectBranchReviewDocInput{
		ProjectID: "project-rq",
		Repo: ProjectRepositoryRecord{
			RepoID: "repo-rq",
		},
		Branch: ProjectBranchRecord{
			BranchID:     "branch-lexer",
			BranchName:   "agent/beta/project-rq/task-lexer",
			ActiveTaskID: "task-rq-lexer",
		},
		SourceTask: WorkspaceTaskRecord{
			TaskID:          "task-rq-lexer",
			Title:           "Implement rq lexer",
			ProjectLane:     "implementation",
			WriteScopeHints: []string{"internal/lexer/**", "internal/token/**", "tests/lexer/**"},
		},
		SourceTaskFound:           true,
		BaseBranch:                "main",
		BaseSHA:                   "base",
		HeadSHA:                   "head",
		WriteScopeJSON:            `{"paths":["internal/lexer/**","internal/token/**","tests/lexer/**","go.mod"]}`,
		Pathset:                   []string{"go.mod", "internal/lexer/**", "internal/token/**", "tests/lexer/**"},
		ReviewSummary:             "Lexer lane implemented.",
		VerificationStatus:        "passed",
		VerificationCommand:       "go test ./...",
		VerificationOutputSummary: "ok",
		PatchQueueCandidate: map[string]any{
			"queue_id": "q",
			"item_id":  "i",
		},
		SourceRefsDocKey: "project.project-rq.source_refs",
	})
	for _, want := range []string{
		"review_granularity: lane_scoped_candidate",
		"source_task_id: task-rq-lexer",
		"source_task_lane: implementation",
		"source_task_write_scope_hints: internal/lexer/**, internal/token/**, tests/lexer/**",
		"lane_acceptance_refs",
		"full_product_acceptance_refs",
		"do not block only because sibling lanes",
		"ACCEPTED may mean this lane is correct and ready for integration",
		"unrelated sibling-lane anchors",
	} {
		if !strings.Contains(doc, want) {
			t.Fatalf("review doc missing %q:\n%s", want, doc)
		}
	}
}

func TestProjectBranchReviewReadyToolAutoSubmitsPatchQueueWithRuntimeBinding(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	branchName := "agent/agent-alpha/project-subpixel/task-build"
	remote := seedBareGitRepoWithBranch(t, branchName, filepath.Join("web", "app.js"), "export const ready = true;\n")
	checkout := filepath.Join(t.TempDir(), "checkout")
	runGitNoDir(t, "clone", remote, checkout)
	runGit(t, checkout, "checkout", branchName)
	headSHA := gitOutput(t, checkout, "rev-parse", "HEAD")
	baseSHA := gitOutput(t, checkout, "rev-parse", "main")

	calls := make([]string, 0, 6)
	var submitParams map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch len(calls) {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
				AgentID:         "agent-alpha",
				RepoURL:         remote,
				BranchStatus:    "ACTIVE",
				BranchHeadSHA:   headSHA,
				BranchBaseSHA:   baseSHA,
				ActiveTaskID:    "task-build",
				ActiveClaimID:   "task-build",
				SourceTaskScope: []string{"web/app.js"},
				WriteScopeJSON:  `{"paths":["web/app.js"]}`,
				CheckoutPath:    checkout,
			}))
		case 2:
			if req.Method != "workspace.doc.get" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCError(w, req, -32000, "workspace doc not found: project.project-subpixel.source_refs")
		case 3:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-review"})
		case 4:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"branch": map[string]any{
				"branch_id":        "branch-1",
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      "checkout-1",
				"agent_id":         "agent-alpha",
				"branch_name":      branchName,
				"branch_kind":      "feature",
				"base_branch":      "main",
				"head_sha":         headSHA,
				"base_sha":         baseSHA,
				"write_scope_json": `{"paths":["web/app.js"]}`,
				"review_doc_key":   "project.project-subpixel.branch.branch-1.review",
				"status":           "READY_FOR_REVIEW",
			}})
		case 5:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected fifth method %q", req.Method)
			}
			writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
				AgentID:        "agent-alpha",
				RepoURL:        remote,
				BranchStatus:   "READY_FOR_REVIEW",
				BranchHeadSHA:  headSHA,
				BranchBaseSHA:  baseSHA,
				ActiveTaskID:   "task-build",
				ActiveClaimID:  "claim-build",
				ReviewDocKey:   "project.project-subpixel.branch.branch-1.review",
				WriteScopeJSON: `{"paths":["web/app.js"]}`,
				CheckoutPath:   checkout,
			}))
		case 6:
			if req.Method != "project.patch_queue.submit" {
				t.Fatalf("unexpected sixth method %q", req.Method)
			}
			submitParams = req.Params
			for _, field := range []string{"task_id", "session_id", "run_id", "agent_id", "principal_type", "principal_id", "capability_snapshot_id", "capability_snapshot_schema", "repo_root", "base_tree_hash", "repo_lease_id"} {
				if got := strings.TrimSpace(rpcString(req.Params, field)); got == "" {
					t.Fatalf("expected controlled submit field %s in %+v", field, req.Params)
				}
			}
			if got := rpcString(req.Params, "repo_authority_mode"); got != "repoauthority_controlled_queue" {
				t.Fatalf("repo_authority_mode = %q", got)
			}
			if got := rpcString(req.Params, "branch_id"); got != "branch-1" {
				t.Fatalf("branch_id = %q", got)
			}
			if got := rpcString(req.Params, "head_sha"); got != headSHA {
				t.Fatalf("head_sha = %q want %q", got, headSHA)
			}
			writeRPCResult(w, req, map[string]any{"patch_queue_item": map[string]any{
				"queue_id":            "patchq-project-subpixel-projrepo-1",
				"item_id":             "patchitem-branch-1",
				"workspace_id":        "ws",
				"project_id":          "project-subpixel",
				"repo_id":             "projrepo-1",
				"branch_id":           "branch-1",
				"review_doc_key":      "project.project-subpixel.branch.branch-1.review",
				"repo_authority_mode": "repoauthority_controlled_queue",
				"state":               "PROPOSED",
				"pathset":             []string{"web/app.js"},
				"base_ref":            "main",
				"base_sha":            baseSHA,
				"head_sha":            headSHA,
			}})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", len(calls), req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchReviewReadyTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", checkout).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{
				TaskID:                   "task-build",
				SessionID:                "session-build",
				RunID:                    "run-build",
				ClaimWriteScopeJSON:      `{"paths":["web/app.js"]}`,
				CapabilitySnapshotID:     "caps-build",
				CapabilitySnapshotSchema: daemonCapabilitySnapshotSchema,
			}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":                  "project-subpixel",
		"branch_id":                   "branch-1",
		"review_summary":              "Frontend slice implemented and smoke-checked.",
		"verification_status":         "passed",
		"verification_command":        "go test ./...",
		"verification_exit_code":      0,
		"verification_output_summary": "ok",
		"head_sha":                    headSHA,
		"base_sha":                    baseSHA,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful review ready with auto-submit, got %+v", result)
	}
	if submitParams == nil {
		t.Fatalf("expected patch queue submit params")
	}
	for _, want := range []string{`"patch_queue_auto_submit": "submitted"`, `"mandatory_next_tool": "none"`, `"queue_id": "patchq-project-subpixel-projrepo-1"`, `"repo_authority_mode": "repoauthority_controlled_queue"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if len(calls) != 6 {
		t.Fatalf("expected 6 calls, got %d: %v", len(calls), calls)
	}
}

func TestProjectBranchReviewReadyToolDerivesGoModuleSidecarFromCommittedDiff(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	branchName := "agent/agent-kappa/project-rq/task-cli"
	remote := seedBareGitRepo(t)
	checkout := filepath.Join(t.TempDir(), "checkout")
	runGitNoDir(t, "clone", remote, checkout)
	runGit(t, checkout, "config", "user.name", "Rhizome Test")
	runGit(t, checkout, "config", "user.email", "rhizome-test@example.invalid")
	runGit(t, checkout, "checkout", "-b", branchName)
	if err := os.MkdirAll(filepath.Join(checkout, "cmd", "rq"), 0o755); err != nil {
		t.Fatalf("mkdir cmd/rq: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkout, "cmd", "rq", "main.go"), []byte("package main\nfunc main() {}\n"), 0o644); err != nil {
		t.Fatalf("write main.go: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkout, "go.mod"), []byte("module rq\n\ngo 1.23\n"), 0o644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}
	runGit(t, checkout, "add", "cmd/rq/main.go", "go.mod")
	runGit(t, checkout, "commit", "-m", "Add rq CLI scaffold")
	runGit(t, checkout, "push", "origin", branchName)
	headSHA := gitOutput(t, checkout, "rev-parse", "HEAD")
	baseSHA := gitOutput(t, checkout, "rev-parse", "main")

	calls := make([]string, 0, 6)
	var branchScope string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch len(calls) {
		case 1:
			writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
				AgentID:         "agent-kappa",
				RepoURL:         remote,
				BranchName:      branchName,
				BranchStatus:    "ACTIVE",
				BranchHeadSHA:   headSHA,
				BranchBaseSHA:   baseSHA,
				ActiveTaskID:    "task-cli",
				ActiveClaimID:   "task-cli",
				SourceTaskScope: []string{"cmd/rq/**", "go.mod"},
				WriteScopeJSON:  `{"paths":["cmd/rq/**","README.md"]}`,
				CheckoutPath:    checkout,
			}))
		case 2:
			writeRPCError(w, req, -32000, "workspace doc not found: project.project-rq.source_refs")
		case 3:
			writeRPCResult(w, req, map[string]any{"sha": "sha-review"})
		case 4:
			branchScope = rpcString(req.Params, "write_scope_json")
			writeRPCResult(w, req, map[string]any{"branch": map[string]any{
				"branch_id":        "branch-1",
				"workspace_id":     "ws",
				"project_id":       "project-rq",
				"repo_id":          "repo-rq",
				"checkout_id":      "checkout-1",
				"agent_id":         "agent-kappa",
				"branch_name":      branchName,
				"branch_kind":      "feature",
				"base_branch":      "main",
				"head_sha":         headSHA,
				"base_sha":         baseSHA,
				"write_scope_json": branchScope,
				"review_doc_key":   "project.project-rq.branch.branch-1.review",
				"status":           "READY_FOR_REVIEW",
			}})
		case 5:
			writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
				AgentID:        "agent-kappa",
				RepoURL:        remote,
				BranchName:     branchName,
				BranchStatus:   "READY_FOR_REVIEW",
				BranchHeadSHA:  headSHA,
				BranchBaseSHA:  baseSHA,
				ReviewDocKey:   "project.project-rq.branch.branch-1.review",
				WriteScopeJSON: branchScope,
				CheckoutPath:   checkout,
			}))
		case 6:
			writeRPCResult(w, req, map[string]any{"patch_queue_item": map[string]any{
				"queue_id":            "patchq-project-rq-repo-rq",
				"item_id":             "patchitem-branch-1",
				"workspace_id":        "ws",
				"project_id":          "project-rq",
				"repo_id":             "repo-rq",
				"branch_id":           "branch-1",
				"review_doc_key":      "project.project-rq.branch.branch-1.review",
				"repo_authority_mode": "repoauthority_controlled_queue",
				"state":               "PROPOSED",
				"pathset":             []string{"cmd/rq/main.go", "go.mod"},
				"base_ref":            "main",
				"base_sha":            baseSHA,
				"head_sha":            headSHA,
			}})
		default:
			t.Fatalf("unexpected RPC call %d method=%s", len(calls), req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchReviewReadyTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-kappa", checkout).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{
				TaskID:                   "task-cli",
				SessionID:                "session-cli",
				RunID:                    "run-cli",
				ClaimWriteScopeJSON:      `{"paths":["cmd/rq/**","go.mod"]}`,
				CapabilitySnapshotID:     "caps-cli",
				CapabilitySnapshotSchema: daemonCapabilitySnapshotSchema,
			}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":           "project-rq",
		"branch_id":            "branch-1",
		"review_summary":       "CLI lane built and checked.",
		"verification_status":  "passed",
		"verification_command": "go test ./...",
		"head_sha":             headSHA,
		"base_sha":             baseSHA,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected review-ready sidecar derivation to succeed, got %+v", result)
	}
	if !strings.Contains(branchScope, "go.mod") {
		t.Fatalf("expected branch write_scope_json to include derived go.mod, got %s", branchScope)
	}
	if !strings.Contains(result.Output, "go.mod") {
		t.Fatalf("expected auto-submit receipt to include go.mod, got %s", result.Output)
	}
}

func TestProjectBranchReviewReadyCheckoutSelectionPrefersRuntimeCheckoutOverStaleBranchCheckout(t *testing.T) {
	branch := ProjectBranchRecord{
		BranchID:     "branch-1",
		RepoID:       "repo-1",
		CheckoutID:   "checkout-stale",
		AgentID:      "agent-alpha",
		ActiveTaskID: "task-runtime",
		BranchName:   "agent/agent-alpha/project-rq/task-runtime",
		Status:       "ACTIVE",
	}
	coordination := ProjectCoordinationRecord{
		Checkouts: []ProjectCheckoutRecord{
			{
				CheckoutID: "checkout-stale",
				RepoID:     "repo-1",
				AgentID:    "agent-alpha",
				LocalPath:  "/tmp/stale-dirty",
				Status:     "STALE",
			},
			{
				CheckoutID:   "checkout-runtime",
				RepoID:       "repo-1",
				AgentID:      "agent-alpha",
				LocalPath:    "/tmp/runtime-clean",
				BranchName:   branch.BranchName,
				ActiveTaskID: "task-runtime",
				Status:       "ACTIVE",
			},
		},
	}
	checkout, ok := selectProjectCompletionCheckout(coordination, branch, "agent-alpha", WorkspaceTaskRecord{TaskID: "task-runtime"})
	if !ok || checkout.CheckoutID != "checkout-runtime" {
		t.Fatalf("expected runtime-bound checkout, got ok=%v checkout=%+v", ok, checkout)
	}
	if got := projectBranchReviewCheckoutPath(coordination.Checkouts, "checkout-stale"); got != "" {
		t.Fatalf("terminal checkout fallback returned %q", got)
	}
}

func TestProjectBranchReviewReadyToolRejectsCommittedDiffOutsideScope(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	branchName := "agent/agent-eta/project-rq/task-builtins"
	remote := seedBareGitRepo(t)
	checkout := filepath.Join(t.TempDir(), "checkout")
	runGitNoDir(t, "clone", remote, checkout)
	runGit(t, checkout, "config", "user.name", "Rhizome Test")
	runGit(t, checkout, "config", "user.email", "rhizome-test@example.invalid")
	runGit(t, checkout, "checkout", "-b", branchName)
	if err := os.MkdirAll(filepath.Join(checkout, "internal", "rq"), 0o755); err != nil {
		t.Fatalf("mkdir internal/rq: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkout, "internal", "rq", "eval.go"), []byte("package rq\n"), 0o644); err != nil {
		t.Fatalf("write eval.go: %v", err)
	}
	runGit(t, checkout, "add", "internal/rq/eval.go")
	runGit(t, checkout, "commit", "-m", "Add rq baseline")
	runGit(t, checkout, "push", "origin", branchName)
	headSHA := gitOutput(t, checkout, "rev-parse", "HEAD")
	baseSHA := gitOutput(t, checkout, "rev-parse", "main")

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if calls != 1 || req.Method != "project.coordination.get" {
			t.Fatalf("unexpected RPC call %d method=%s", calls, req.Method)
		}
		writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
			AgentID:        "agent-eta",
			RepoURL:        remote,
			BranchName:     branchName,
			BranchStatus:   "ACTIVE",
			BranchHeadSHA:  headSHA,
			BranchBaseSHA:  baseSHA,
			WriteScopeJSON: `{"paths":["src/articles/**"]}`,
			CheckoutPath:   checkout,
		}))
	}))
	defer server.Close()

	tool := NewProjectBranchReviewReadyTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-eta", checkout)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":           "project-rq",
		"branch_id":            "branch-1",
		"review_summary":       "Baseline candidate.",
		"verification_status":  "passed",
		"verification_command": "go test ./...",
		"head_sha":             headSHA,
		"base_sha":             baseSHA,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected out-of-scope committed diff to fail, got %+v", result)
	}
	if !strings.Contains(result.Output, "committed diff paths exceed branch write scope") || !strings.Contains(result.Output, "internal/rq/eval.go") {
		t.Fatalf("expected scope error to name committed path, got %q", result.Output)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination lookup, got %d calls", calls)
	}
}

func TestProjectBranchReviewReadyToolRejectsOutOfScopeCommittedDeletion(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	branchName := "agent/agent-beta/project-rq/task-lexer-repair"
	remote := seedBareGitRepo(t)
	checkout := filepath.Join(t.TempDir(), "checkout")
	runGitNoDir(t, "clone", remote, checkout)
	runGit(t, checkout, "config", "user.name", "Rhizome Test")
	runGit(t, checkout, "config", "user.email", "rhizome-test@example.invalid")
	if err := os.MkdirAll(filepath.Join(checkout, "internal", "parser"), 0o755); err != nil {
		t.Fatalf("mkdir internal/parser: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkout, "internal", "parser", "parser.go"), []byte("package parser\n"), 0o644); err != nil {
		t.Fatalf("write parser.go: %v", err)
	}
	runGit(t, checkout, "add", "internal/parser/parser.go")
	runGit(t, checkout, "commit", "-m", "Add parser baseline")
	runGit(t, checkout, "push", "origin", "main")
	baseSHA := gitOutput(t, checkout, "rev-parse", "main")
	runGit(t, checkout, "checkout", "-b", branchName)
	if err := os.Remove(filepath.Join(checkout, "internal", "parser", "parser.go")); err != nil {
		t.Fatalf("remove parser.go: %v", err)
	}
	runGit(t, checkout, "add", "-A")
	runGit(t, checkout, "commit", "-m", "Repair lexer without parser")
	runGit(t, checkout, "push", "origin", branchName)
	headSHA := gitOutput(t, checkout, "rev-parse", "HEAD")

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if calls != 1 || req.Method != "project.coordination.get" {
			t.Fatalf("unexpected RPC call %d method=%s", calls, req.Method)
		}
		writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
			AgentID:        "agent-beta",
			RepoURL:        remote,
			BranchName:     branchName,
			BranchStatus:   "ACTIVE",
			BranchHeadSHA:  headSHA,
			BranchBaseSHA:  baseSHA,
			WriteScopeJSON: `{"paths":["internal/lexer/**","internal/token/**"]}`,
			CheckoutPath:   checkout,
		}))
	}))
	defer server.Close()

	tool := NewProjectBranchReviewReadyTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", checkout)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":           "project-rq",
		"branch_id":            "branch-1",
		"review_summary":       "Lexer repair candidate.",
		"verification_status":  "passed",
		"verification_command": "go test ./...",
		"head_sha":             headSHA,
		"base_sha":             baseSHA,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected out-of-scope deletion to fail, got %+v", result)
	}
	if !strings.Contains(result.Output, "committed diff paths exceed branch write scope") || !strings.Contains(result.Output, "internal/parser/parser.go") {
		t.Fatalf("expected scope error to name deleted committed path, got %q", result.Output)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination lookup, got %d calls", calls)
	}
}

func TestProjectBranchReviewReadyToolMarksPacketAsNonReceiptWhenBranchTransitionFails(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	branchName := "agent/agent-alpha/project-subpixel/task-build"
	remote := seedBareGitRepoWithBranch(t, branchName, filepath.Join("web", "app.js"), "export const ready = true;\n")
	headSHA := gitOutput(t, remote, "rev-parse", branchName)
	baseSHA := gitOutput(t, remote, "rev-parse", "main")

	calls := make([]string, 0, 5)
	var statusDocKey string
	var statusDocContent string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch len(calls) {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
				AgentID:        "agent-alpha",
				RepoURL:        remote,
				BranchStatus:   "ACTIVE",
				BranchHeadSHA:  headSHA,
				BranchBaseSHA:  baseSHA,
				ActiveTaskID:   "task-build",
				WriteScopeJSON: `{"paths":["web/app.js"]}`,
				CheckoutPath:   "C:/work/project",
			}))
		case 2:
			if req.Method != "workspace.doc.get" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCError(w, req, -32000, "workspace doc not found: project.project-subpixel.source_refs")
		case 3:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			if got := rpcString(req.Params, "doc_key"); got != "project.project-subpixel.branch.branch-1.review" {
				t.Fatalf("review doc_key = %q", got)
			}
			writeRPCResult(w, req, map[string]any{"sha": "sha-review"})
		case 4:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			writeRPCError(w, req, -32000, "project branch review evidence invalid: source_requirements_trace doc project.project-subpixel.source_requirements_trace must contain rhizome_source_requirements_trace_v1")
		case 5:
			if req.Method != "workspace.doc.put" {
				t.Fatalf("unexpected fifth method %q", req.Method)
			}
			statusDocKey = rpcString(req.Params, "doc_key")
			statusDocContent = rpcString(req.Params, "content")
			writeRPCResult(w, req, map[string]any{"sha": "sha-status"})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", len(calls), req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchReviewReadyTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "C:/work/project")
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":                  "project-subpixel",
		"branch_id":                   "branch-1",
		"review_summary":              "Frontend slice implemented and smoke-checked.",
		"verification_status":         "passed",
		"verification_command":        "go test ./...",
		"verification_exit_code":      0,
		"verification_output_summary": "ok",
		"head_sha":                    headSHA,
		"base_sha":                    baseSHA,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected branch transition failure, got %+v", result)
	}
	for _, want := range []string{"failed marking branch READY_FOR_REVIEW", "Review packet is not a receipt", "mandatory_next_tool=project_patch_queue_submit", "patch queue item is a separate project_patch_queue_submit receipt", "task.task-build.review_ready_status"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if statusDocKey != "task.task-build.review_ready_status" {
		t.Fatalf("unexpected status doc key %q", statusDocKey)
	}
	for _, want := range []string{
		"schema: rhizome_review_ready_status_v1",
		"review_packet_status: draft_not_receipt",
		"receipt_status: failed_before_branch_registry_transition",
		"project_branch_registry.status=READY_FOR_REVIEW",
		"mandatory_next_tool=project_patch_queue_submit",
		"patch_queue_item_receipt: produced only by project_patch_queue_submit",
	} {
		if !strings.Contains(statusDocContent, want) {
			t.Fatalf("expected status doc to contain %q:\n%s", want, statusDocContent)
		}
	}
	if len(calls) != 5 {
		t.Fatalf("expected 5 RPC calls, got %d: %+v", len(calls), calls)
	}
}

func TestProjectBranchReviewReadyToolRejectsSameHeadRejectedPatchQueueDecision(t *testing.T) {
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
			PatchQueueItems: []map[string]any{
				branchReviewPatchQueueItem("REJECTED", "head123", "UI advertises 24x24 but renders 16x16"),
			},
		}))
	}))
	defer server.Close()

	tool := NewProjectBranchReviewReadyTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", t.TempDir())
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":           "project-subpixel",
		"branch_id":            "branch-1",
		"review_summary":       "same head evidence refresh",
		"verification_status":  "passed",
		"verification_command": "npm test",
		"head_sha":             "head123",
		"base_sha":             "base123",
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
		t.Fatalf("expected only coordination lookup before rejection, got %d calls", calls)
	}
}

func TestProjectBranchReviewReadyToolReportsSameHeadBlockedPatchQueueDecision(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	branchName := "agent/agent-alpha/project-subpixel/task-build"
	remote := seedBareGitRepoWithBranch(t, branchName, filepath.Join("web", "app.js"), "export const ready = true;\n")
	headSHA := gitOutput(t, remote, "rev-parse", branchName)
	baseSHA := gitOutput(t, remote, "rev-parse", "main")
	calls := make([]string, 0, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
				AgentID:        "agent-alpha",
				RepoURL:        remote,
				BranchStatus:   "ACTIVE",
				BranchHeadSHA:  headSHA,
				BranchBaseSHA:  baseSHA,
				WriteScopeJSON: `{"paths":["web/app.js"]}`,
				PatchQueueItems: []map[string]any{
					branchReviewPatchQueueItem("BLOCKED", headSHA, "Missing browser smoke evidence"),
				},
			}))
		case "workspace.doc.get":
			if got := rpcString(req.Params, "doc_key"); got != "project.project-subpixel.source_refs" {
				t.Fatalf("unexpected source refs doc key %q", got)
			}
			writeRPCError(w, req, -32000, "workspace doc not found: project.project-subpixel.source_refs")
		case "workspace.doc.put":
			writeRPCResult(w, req, map[string]any{"sha": "sha-review"})
		case "project.branch.register":
			writeRPCResult(w, req, map[string]any{"branch": map[string]any{
				"branch_id":        "branch-1",
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      "checkout-1",
				"agent_id":         "agent-alpha",
				"branch_name":      "agent/agent-alpha/project-subpixel/task-build",
				"branch_kind":      "feature",
				"base_branch":      "main",
				"head_sha":         headSHA,
				"base_sha":         baseSHA,
				"write_scope_json": `{"paths":["web/app.js"]}`,
				"review_doc_key":   "project.project-subpixel.branch.branch-1.review",
				"status":           "READY_FOR_REVIEW",
			}})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchReviewReadyTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", t.TempDir())
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":           "project-subpixel",
		"branch_id":            "branch-1",
		"review_summary":       "browser evidence added",
		"verification_status":  "passed",
		"verification_command": "npm test",
		"head_sha":             headSHA,
		"base_sha":             baseSHA,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected blocked queue evidence refresh to succeed with warning, got %+v", result)
	}
	for _, want := range []string{`"queue_state_blocking": true`, "BLOCKED", "Missing browser smoke evidence", "queue-facing"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected blocked queue payload to contain %q, got %q", want, result.Output)
		}
	}
	if len(calls) != 4 || calls[0] != "project.coordination.get" || calls[1] != "workspace.doc.get" || calls[2] != "workspace.doc.put" || calls[3] != "project.branch.register" {
		t.Fatalf("unexpected rpc calls: %+v", calls)
	}
}

func TestProjectBranchReviewReadyToolRequiresVerificationBeforeRPC(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		t.Fatalf("unexpected RPC call")
	}))
	defer server.Close()

	tool := NewProjectBranchReviewReadyTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", t.TempDir())
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"branch_id":      "branch-1",
		"review_summary": "done",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "verification_status") {
		t.Fatalf("expected verification error, got %+v", result)
	}
	if calls != 0 {
		t.Fatalf("expected no RPC calls, got %d", calls)
	}
}

func TestProjectBranchReviewReadyToolRejectsUnownedBranch(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
			AgentID:        "agent-beta",
			BranchStatus:   "ACTIVE",
			ActiveTaskID:   "task-build",
			WriteScopeJSON: `{"paths":["web/app.js"]}`,
		}))
	}))
	defer server.Close()

	tool := NewProjectBranchReviewReadyTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", t.TempDir())
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":          "project-subpixel",
		"branch_id":           "branch-1",
		"review_summary":      "done",
		"verification_status": "not_applicable",
		"head_sha":            "abc123",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "owned by this agent") || !strings.Contains(result.Output, "Do not retry this owner-only tool") || !strings.Contains(result.Output, "agent_request request_kind=delegate_task") {
		t.Fatalf("expected ownership error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected one coordination call, got %d", calls)
	}
}

func TestProjectBranchReviewReadyToolRejectsRuntimeTaskRelabel(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
			AgentID:         "agent-beta",
			BranchStatus:    "ACTIVE",
			ActiveTaskID:    "task-parser",
			ActiveClaimID:   "task-parser",
			SourceTaskScope: []string{"internal/parser/**"},
			WriteScopeJSON:  `{"paths":["internal/parser/**"]}`,
		}))
	}))
	defer server.Close()

	tool := NewProjectBranchReviewReadyTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", t.TempDir()).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{
				TaskID:              "task-stdlib",
				ClaimWriteScopeJSON: `{"paths":["internal/stdlib/**"]}`,
			}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":          "project-subpixel",
		"branch_id":           "branch-1",
		"review_summary":      "parser branch cannot be relabeled as stdlib",
		"verification_status": "not_applicable",
		"head_sha":            "abc123",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "active task binding") || !strings.Contains(result.Output, "do not relabel") {
		t.Fatalf("expected active task relabel rejection, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination lookup before relabel rejection, got %d", calls)
	}
}

func TestProjectBranchReviewReadyToolRejectsClaimedTaskPathsetRelabel(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
			AgentID:         "agent-beta",
			BranchStatus:    "ACTIVE",
			ActiveTaskID:    "task-stdlib",
			ActiveClaimID:   "task-stdlib",
			SourceTaskScope: []string{"internal/stdlib/**"},
			WriteScopeJSON:  `{"paths":["internal/parser/**"]}`,
		}))
	}))
	defer server.Close()

	tool := NewProjectBranchReviewReadyTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", t.TempDir()).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{
				TaskID:              "task-stdlib",
				ClaimWriteScopeJSON: `{"paths":["internal/stdlib/**"]}`,
			}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":          "project-subpixel",
		"branch_id":           "branch-1",
		"review_summary":      "parser branch cannot be published under stdlib task",
		"verification_status": "not_applicable",
		"head_sha":            "abc123",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "branch pathset") || !strings.Contains(result.Output, "outside claimed task") {
		t.Fatalf("expected claimed task pathset rejection, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination lookup before pathset rejection, got %d", calls)
	}
}

func TestProjectBranchReviewReadyToolRejectsRepoWithoutRemote(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
			AgentID:           "agent-alpha",
			RepoWithoutRemote: true,
			BranchStatus:      "ACTIVE",
			WriteScopeJSON:    `{"paths":["web/app.js"]}`,
		}))
	}))
	defer server.Close()

	tool := NewProjectBranchReviewReadyTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", t.TempDir())
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":          "project-subpixel",
		"branch_id":           "branch-1",
		"review_summary":      "done",
		"verification_status": "not_applicable",
		"head_sha":            "abc123",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "READY with remote_url") {
		t.Fatalf("expected repo remote error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected one coordination call, got %d", calls)
	}
}

func TestProjectBranchReviewReadyToolRequiresBranchNameDisambiguation(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, branchReviewAmbiguousCoordinationResult())
	}))
	defer server.Close()

	tool := NewProjectBranchReviewReadyTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", t.TempDir())
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":          "project-subpixel",
		"branch_name":         "agent/agent-alpha/project-subpixel/task-build",
		"review_summary":      "done",
		"verification_status": "not_applicable",
		"head_sha":            "abc123",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "ambiguous") {
		t.Fatalf("expected ambiguous branch_name error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected one coordination call, got %d", calls)
	}
}

func TestProjectBranchReviewReadyToolRejectsScopeExpansion(t *testing.T) {
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

	tool := NewProjectBranchReviewReadyTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", t.TempDir())
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":          "project-subpixel",
		"branch_id":           "branch-1",
		"review_summary":      "done",
		"verification_status": "not_applicable",
		"head_sha":            "abc123",
		"write_scope_json":    `{"paths":["web/app.js","api/server.go"]}`,
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "cannot widen") {
		t.Fatalf("expected scope expansion error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected one coordination call, got %d", calls)
	}
}

func TestProjectBranchReviewReadyToolRejectsDirtyCheckout(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	repoRoot := t.TempDir()
	runGitNoDir(t, "init", "-b", "main", repoRoot)
	runGit(t, repoRoot, "config", "user.name", "Rhizome Test")
	runGit(t, repoRoot, "config", "user.email", "rhizome-test@example.invalid")
	if err := os.WriteFile(filepath.Join(repoRoot, "README.md"), []byte("# Branch Choir\n"), 0o644); err != nil {
		t.Fatalf("write README: %v", err)
	}
	runGit(t, repoRoot, "add", "README.md")
	runGit(t, repoRoot, "commit", "-m", "Initial seed")
	if err := os.WriteFile(filepath.Join(repoRoot, "index.html"), []byte("<main>Branch Choir</main>\n"), 0o644); err != nil {
		t.Fatalf("write dirty index.html: %v", err)
	}

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
			WriteScopeJSON: `{"paths":["index.html"]}`,
			CheckoutPath:   repoRoot,
		}))
	}))
	defer server.Close()

	tool := NewProjectBranchReviewReadyTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", repoRoot)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":           "project-subpixel",
		"branch_id":            "`branch-1`",
		"review_summary":       "index.html written",
		"verification_status":  "passed",
		"verification_command": "read_file index.html",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "clean committed checkout") {
		t.Fatalf("expected dirty checkout error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination lookup before dirty rejection, got %d calls", calls)
	}
}

func TestProjectBranchReviewReadyToolRejectsUnpublishedBranchBeforePacket(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method after unpublished branch check %q", req.Method)
		}
		writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
			AgentID:        "agent-alpha",
			RepoURL:        remote,
			BranchStatus:   "ACTIVE",
			WriteScopeJSON: `{"paths":["web/app.js"]}`,
		}))
	}))
	defer server.Close()

	tool := NewProjectBranchReviewReadyTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", t.TempDir())
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":           "project-subpixel",
		"branch_id":            "branch-1",
		"review_summary":       "local candidate exists",
		"verification_status":  "passed",
		"verification_command": "npm run build",
		"head_sha":             "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"base_sha":             "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected unpublished branch rejection, got %+v", result)
	}
	for _, want := range []string{"published on the shared project remote", "push=true", "project_branch_review_ready"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 1 {
		t.Fatalf("expected only coordination lookup before rejection, got %d calls", calls)
	}
}

func TestProjectBranchReviewReadyToolRejectsRemoteHeadMismatchBeforePacket(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	branchName := "agent/agent-alpha/project-subpixel/task-build"
	remote := seedBareGitRepoWithBranch(t, branchName, filepath.Join("web", "app.js"), "export const ready = true;\n")
	remoteHead := gitOutput(t, remote, "rev-parse", branchName)
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method after remote mismatch %q", req.Method)
		}
		writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
			AgentID:        "agent-alpha",
			RepoURL:        remote,
			BranchStatus:   "ACTIVE",
			BranchHeadSHA:  remoteHead,
			WriteScopeJSON: `{"paths":["web/app.js"]}`,
		}))
	}))
	defer server.Close()

	tool := NewProjectBranchReviewReadyTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", t.TempDir())
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":           "project-subpixel",
		"branch_id":            "branch-1",
		"review_summary":       "stale candidate",
		"verification_status":  "passed",
		"verification_command": "npm run build",
		"head_sha":             "1111111111111111111111111111111111111111",
		"base_sha":             "2222222222222222222222222222222222222222",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected remote head mismatch rejection, got %+v", result)
	}
	for _, want := range []string{"remote branch", "requested head_sha", "push=true"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 1 {
		t.Fatalf("expected only coordination lookup before rejection, got %d calls", calls)
	}
}

type branchReviewCoordinationInput struct {
	AgentID           string
	RepoURL           string
	RepoWithoutRemote bool
	BranchName        string
	BranchStatus      string
	BranchHeadSHA     string
	BranchBaseSHA     string
	ActiveTaskID      string
	ActiveClaimID     string
	SourceTaskLane    string
	SourceTaskKind    string
	SourceTaskScope   []string
	ReviewDocKey      string
	WriteScopeJSON    string
	CheckoutPath      string
	PatchQueueItems   []map[string]any
}

func branchReviewPatchQueueItem(state, headSHA, decisionSummary string) map[string]any {
	return map[string]any{
		"queue_id":            "patchq-project-subpixel-projrepo-1",
		"item_id":             "patchitem-branch-1",
		"workspace_id":        "ws",
		"project_id":          "project-subpixel",
		"repo_id":             "projrepo-1",
		"branch_id":           "branch-1",
		"review_doc_key":      "project.project-subpixel.branch.branch-1.review",
		"repo_authority_mode": "patch_only_temp_repo",
		"state":               state,
		"pathset":             []string{"web/app.js"},
		"base_ref":            "main",
		"base_sha":            "base123",
		"head_sha":            headSHA,
		"auto_merge":          false,
		"decision_summary":    decisionSummary,
		"decision_doc_key":    "project.project-subpixel.patch_queue.patchitem-branch-1.decision",
		"decided_by":          "agent-reviewer",
		"decided_at":          "2026-05-08T02:00:00Z",
		"updated_at":          "2026-05-08T02:00:00Z",
	}
}

func branchReviewCoordinationResult(input branchReviewCoordinationInput) map[string]any {
	agentID := firstNonEmpty(input.AgentID, "agent-alpha")
	repoURL := firstNonEmpty(input.RepoURL, "https://github.com/ExampleOrg/subpixel-lab.git")
	if input.RepoWithoutRemote {
		repoURL = ""
	}
	branchName := firstNonEmpty(input.BranchName, "agent/agent-alpha/project-subpixel/task-build")
	branchStatus := firstNonEmpty(input.BranchStatus, "ACTIVE")
	tasks := []map[string]any{}
	if strings.TrimSpace(input.ActiveTaskID) != "" {
		tasks = append(tasks, map[string]any{
			"task_id":                input.ActiveTaskID,
			"title":                  "Source task " + input.ActiveTaskID,
			"owner_user_id":          "developer",
			"priority":               "high",
			"status":                 "PENDING",
			"task_kind":              firstNonEmpty(input.SourceTaskKind, "EXECUTION"),
			"task_template":          "generic",
			"project_id":             "project-subpixel",
			"project_lane":           firstNonEmpty(input.SourceTaskLane, "implementation"),
			"requires_project_gate":  true,
			"write_scope_hints":      input.SourceTaskScope,
			"task_requirements_json": `{}`,
			"linked_by":              "alpha",
			"linked_at":              "2026-06-21T00:00:00Z",
			"updated_at":             "2026-06-21T00:00:00Z",
		})
	}
	return map[string]any{"coordination": map[string]any{
		"coordination_version": "v1",
		"project": map[string]any{
			"workspace_id": "ws",
			"project_id":   "project-subpixel",
			"title":        "Subpixel Pattern Lab",
			"status":       "ACTIVE",
		},
		"profile": map[string]any{
			"workspace_id":        "ws",
			"project_id":          "project-subpixel",
			"current_phase":       "IMPLEMENTATION",
			"repo_required":       true,
			"repo_status":         "READY",
			"repo_url":            repoURL,
			"repo_default_branch": "main",
		},
		"gate_status": map[string]any{
			"workspace_id":         "ws",
			"project_id":           "project-subpixel",
			"overall_state":        "PARTIAL",
			"implementation_ready": false,
		},
		"repositories": []map[string]any{{
			"workspace_id":   "ws",
			"project_id":     "project-subpixel",
			"repo_id":        "projrepo-1",
			"remote_url":     repoURL,
			"remote_kind":    "github",
			"default_branch": "main",
			"repo_status":    "READY",
			"is_canonical":   true,
		}},
		"checkouts": []map[string]any{{
			"checkout_id":  "checkout-1",
			"workspace_id": "ws",
			"project_id":   "project-subpixel",
			"repo_id":      "projrepo-1",
			"agent_id":     agentID,
			"local_path":   input.CheckoutPath,
			"status":       "ACTIVE",
		}},
		"branches": []map[string]any{{
			"branch_id":        "branch-1",
			"workspace_id":     "ws",
			"project_id":       "project-subpixel",
			"repo_id":          "projrepo-1",
			"checkout_id":      "checkout-1",
			"agent_id":         agentID,
			"active_task_id":   input.ActiveTaskID,
			"active_claim_id":  input.ActiveClaimID,
			"branch_name":      branchName,
			"branch_kind":      "feature",
			"base_branch":      "main",
			"head_sha":         input.BranchHeadSHA,
			"base_sha":         input.BranchBaseSHA,
			"write_scope_json": input.WriteScopeJSON,
			"review_doc_key":   input.ReviewDocKey,
			"status":           branchStatus,
		}},
		"patch_queue_items": input.PatchQueueItems,
		"tasks":             tasks,
	}}
}

func branchReviewAmbiguousCoordinationResult() map[string]any {
	result := branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		BranchStatus:   "ACTIVE",
		WriteScopeJSON: `{"paths":["web/app.js"]}`,
	})
	coordination := result["coordination"].(map[string]any)
	coordination["repositories"] = []map[string]any{
		{
			"workspace_id":   "ws",
			"project_id":     "project-subpixel",
			"repo_id":        "projrepo-1",
			"remote_url":     "https://github.com/ExampleOrg/subpixel-lab.git",
			"remote_kind":    "github",
			"default_branch": "main",
			"repo_status":    "READY",
			"is_canonical":   true,
		},
		{
			"workspace_id":   "ws",
			"project_id":     "project-subpixel",
			"repo_id":        "projrepo-2",
			"remote_url":     "https://github.com/ExampleOrg/subpixel-lab-api.git",
			"remote_kind":    "github",
			"default_branch": "main",
			"repo_status":    "READY",
			"is_canonical":   false,
		},
	}
	coordination["branches"] = []map[string]any{
		{
			"branch_id":        "branch-1",
			"workspace_id":     "ws",
			"project_id":       "project-subpixel",
			"repo_id":          "projrepo-1",
			"checkout_id":      "checkout-1",
			"agent_id":         "agent-alpha",
			"branch_name":      "agent/agent-alpha/project-subpixel/task-build",
			"branch_kind":      "feature",
			"base_branch":      "main",
			"write_scope_json": `{"paths":["web/app.js"]}`,
			"status":           "ACTIVE",
		},
		{
			"branch_id":        "branch-2",
			"workspace_id":     "ws",
			"project_id":       "project-subpixel",
			"repo_id":          "projrepo-2",
			"checkout_id":      "checkout-2",
			"agent_id":         "agent-alpha",
			"branch_name":      "agent/agent-alpha/project-subpixel/task-build",
			"branch_kind":      "feature",
			"base_branch":      "main",
			"write_scope_json": `{"paths":["api/server.go"]}`,
			"status":           "ACTIVE",
		},
	}
	return result
}
