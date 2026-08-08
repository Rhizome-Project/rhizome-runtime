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

func TestProjectCheckoutRegisterToolRegistersCheckoutAndBranchEvidence(t *testing.T) {
	localPath := t.TempDir()
	calls := 0
	expectedBranchName := projectClaimBranchName("agent-alpha", "project-subpixel", "task-build")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:     "READY",
				RepoURL:        "https://github.com/ExampleOrg/subpixel-lab.git",
				CurrentPhase:   "IMPLEMENTATION",
				WriteScopeJSON: `{"paths":["web/**"]}`,
			}))
		case 2:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcString(req.Params, "repo_id"); got != "projrepo-1" {
				t.Fatalf("repo_id = %q", got)
			}
			if got := rpcString(req.Params, "local_path"); got != localPath {
				t.Fatalf("local_path = %q, want %q", got, localPath)
			}
			if got := rpcString(req.Params, "active_task_id"); got != "task-build" {
				t.Fatalf("active_task_id = %q", got)
			}
			if got := rpcString(req.Params, "branch_name"); got != "" {
				t.Fatalf("checkout branch_name should not claim intended branch without verification, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":     "checkout-1",
				"workspace_id":    "ws",
				"project_id":      "project-subpixel",
				"repo_id":         "projrepo-1",
				"machine_id":      rpcString(req.Params, "machine_id"),
				"agent_id":        "agent-alpha",
				"local_path":      localPath,
				"checkout_kind":   "clone",
				"branch_name":     "",
				"base_branch":     "main",
				"dirty_state":     "unknown",
				"active_task_id":  "task-build",
				"active_claim_id": "claim-build",
				"status":          "ACTIVE",
			}})
		case 3:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			if got := rpcString(req.Params, "checkout_id"); got != "checkout-1" {
				t.Fatalf("checkout_id = %q", got)
			}
			if got := rpcString(req.Params, "write_scope_json"); got != `{"paths":["web/**"]}` {
				t.Fatalf("write_scope_json = %q", got)
			}
			if got := rpcString(req.Params, "branch_name"); got != expectedBranchName {
				t.Fatalf("unexpected branch reservation name %q", got)
			}
			writeRPCResult(w, req, map[string]any{"branch": map[string]any{
				"branch_id":        "branch-1",
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      "checkout-1",
				"agent_id":         "agent-alpha",
				"active_task_id":   "task-build",
				"active_claim_id":  "claim-build",
				"branch_name":      rpcString(req.Params, "branch_name"),
				"branch_kind":      "feature",
				"base_branch":      "main",
				"write_scope_json": `{"paths":["web/**"]}`,
				"status":           "RESERVED",
			}})
		case 4:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:          "READY",
				RepoURL:             "https://github.com/ExampleOrg/subpixel-lab.git",
				CurrentPhase:        "IMPLEMENTATION",
				WriteScopeJSON:      `{"paths":["web/**"]}`,
				OverallState:        "PARTIAL",
				ImplementationReady: false,
			}))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectCheckoutRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", localPath)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        "project-subpixel",
		"active_task_id":    "task-build",
		"active_claim_id":   "claim-build",
		"verify_git_remote": false,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful checkout registration, got %+v", result)
	}
	for _, want := range []string{"checkout-1", "branch-1", `"no_git_mutation": true`, `"verification_performed": false`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 4 {
		t.Fatalf("expected 4 calls, got %d", calls)
	}
}

func TestProjectCheckoutRegisterToolDoesNotRebindDifferentTaskSamePathCheckout(t *testing.T) {
	localPath := t.TempDir()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:     "READY",
				RepoURL:        "https://github.com/ExampleOrg/subpixel-lab.git",
				CurrentPhase:   "IMPLEMENTATION",
				WriteScopeJSON: `{"paths":["web/**"]}`,
				Tasks: []map[string]any{{
					"task_id":      "task-new",
					"status":       "RUNNING",
					"task_kind":    "EXECUTION",
					"project_lane": "implementation",
				}},
				Checkouts: []map[string]any{{
					"checkout_id":     "checkout-old-task",
					"workspace_id":    "ws",
					"project_id":      "project-subpixel",
					"repo_id":         "projrepo-1",
					"machine_id":      runtimeMachineID(),
					"agent_id":        "agent-alpha",
					"local_path":      localPath,
					"checkout_kind":   "clone",
					"active_task_id":  "task-old",
					"active_claim_id": "claim-old",
					"status":          "ACTIVE",
				}},
			}))
		case 2:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcString(req.Params, "checkout_id"); got != "" {
				t.Fatalf("expected checkout register to avoid rebinding different-task same-path checkout, got checkout_id=%q params=%+v", got, req.Params)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":     "checkout-new-task",
				"workspace_id":    "ws",
				"project_id":      "project-subpixel",
				"repo_id":         "projrepo-1",
				"machine_id":      rpcString(req.Params, "machine_id"),
				"agent_id":        "agent-alpha",
				"local_path":      localPath,
				"checkout_kind":   "clone",
				"branch_name":     "",
				"base_branch":     "main",
				"dirty_state":     "unknown",
				"active_task_id":  "task-new",
				"active_claim_id": "claim-new",
				"status":          "ACTIVE",
			}})
		case 3:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			if got := rpcString(req.Params, "checkout_id"); got != "checkout-new-task" {
				t.Fatalf("branch should bind to newly registered checkout, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{"branch": map[string]any{
				"branch_id":        "branch-new",
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      "checkout-new-task",
				"agent_id":         "agent-alpha",
				"active_task_id":   "task-new",
				"active_claim_id":  "claim-new",
				"branch_name":      rpcString(req.Params, "branch_name"),
				"branch_kind":      "feature",
				"base_branch":      "main",
				"write_scope_json": `{"paths":["web/**"]}`,
				"status":           "RESERVED",
			}})
		case 4:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:          "READY",
				RepoURL:             "https://github.com/ExampleOrg/subpixel-lab.git",
				CurrentPhase:        "IMPLEMENTATION",
				WriteScopeJSON:      `{"paths":["web/**"]}`,
				OverallState:        "PARTIAL",
				ImplementationReady: false,
			}))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectCheckoutRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", localPath)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        "project-subpixel",
		"active_task_id":    "task-new",
		"active_claim_id":   "claim-new",
		"verify_git_remote": false,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful checkout registration without stale rebind, got %+v", result)
	}
	if strings.Contains(result.Output, "checkout-old-task") {
		t.Fatalf("output should not expose stale checkout adoption, got %s", result.Output)
	}
	if calls != 4 {
		t.Fatalf("expected 4 calls, got %d", calls)
	}
}

func TestProjectCheckoutRegisterToolRejectsExplicitDifferentTaskCheckoutID(t *testing.T) {
	localPath := t.TempDir()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if calls != 1 || req.Method != "project.coordination.get" {
			t.Fatalf("unexpected RPC call %d method=%s", calls, req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:     "READY",
			RepoURL:        "https://github.com/ExampleOrg/subpixel-lab.git",
			CurrentPhase:   "IMPLEMENTATION",
			WriteScopeJSON: `{"paths":["web/**"]}`,
			Tasks: []map[string]any{{
				"task_id":      "task-new",
				"status":       "RUNNING",
				"task_kind":    "EXECUTION",
				"project_lane": "implementation",
			}},
			Checkouts: []map[string]any{{
				"checkout_id":     "checkout-old-task",
				"workspace_id":    "ws",
				"project_id":      "project-subpixel",
				"repo_id":         "projrepo-1",
				"machine_id":      runtimeMachineID(),
				"agent_id":        "agent-alpha",
				"local_path":      localPath,
				"checkout_kind":   "clone",
				"active_task_id":  "task-old",
				"active_claim_id": "claim-old",
				"status":          "ACTIVE",
			}},
		}))
	}))
	defer server.Close()

	tool := NewProjectCheckoutRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", localPath)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        "project-subpixel",
		"checkout_id":       "checkout-old-task",
		"active_task_id":    "task-new",
		"active_claim_id":   "claim-new",
		"verify_git_remote": false,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected explicit stale checkout_id to be rejected, got %+v", result)
	}
	if !strings.Contains(result.Output, "blocked stale checkout_id") || !strings.Contains(result.Output, "task-old") {
		t.Fatalf("expected stale checkout ownership error, got %s", result.Output)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination call before stale-id block, got %d", calls)
	}
}

func TestProjectCheckoutRegisterToolRejectsExplicitForeignCheckoutID(t *testing.T) {
	localPath := t.TempDir()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if calls != 1 || req.Method != "project.coordination.get" {
			t.Fatalf("unexpected RPC call %d method=%s", calls, req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:     "READY",
			RepoURL:        "https://github.com/ExampleOrg/subpixel-lab.git",
			CurrentPhase:   "IMPLEMENTATION",
			WriteScopeJSON: `{"paths":["web/**"]}`,
			Tasks: []map[string]any{{
				"task_id":      "task-new",
				"status":       "RUNNING",
				"task_kind":    "EXECUTION",
				"project_lane": "implementation",
			}},
			Checkouts: []map[string]any{{
				"checkout_id":     "checkout-foreign",
				"workspace_id":    "ws",
				"project_id":      "project-subpixel",
				"repo_id":         "projrepo-foreign",
				"machine_id":      runtimeMachineID(),
				"agent_id":        "agent-beta",
				"local_path":      localPath,
				"checkout_kind":   "clone",
				"active_task_id":  "task-foreign",
				"active_claim_id": "claim-foreign",
				"status":          "ACTIVE",
			}},
		}))
	}))
	defer server.Close()

	tool := NewProjectCheckoutRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", localPath)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        "project-subpixel",
		"checkout_id":       "checkout-foreign",
		"active_task_id":    "task-new",
		"active_claim_id":   "claim-new",
		"verify_git_remote": false,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected explicit foreign checkout_id to be rejected, got %+v", result)
	}
	if !strings.Contains(result.Output, "blocked foreign checkout_id") || !strings.Contains(result.Output, "agent-beta") {
		t.Fatalf("expected foreign checkout ownership error, got %s", result.Output)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination call before foreign-id block, got %d", calls)
	}
}

func TestProjectCheckoutRegisterToolPreservesExistingReviewReadyBranchStatus(t *testing.T) {
	localPath := t.TempDir()
	calls := 0
	branchName := "agent/agent-alpha/project-subpixel/task-build"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:     "READY",
				RepoURL:        "https://github.com/ExampleOrg/subpixel-lab.git",
				CurrentPhase:   "IMPLEMENTATION",
				WriteScopeJSON: `{"paths":["web/**"]}`,
				Branches: []map[string]any{{
					"branch_id":        "branch-ready",
					"workspace_id":     "ws",
					"project_id":       "project-subpixel",
					"repo_id":          "projrepo-1",
					"checkout_id":      "checkout-old",
					"agent_id":         "agent-alpha",
					"branch_name":      branchName,
					"branch_kind":      "feature",
					"base_branch":      "main",
					"head_sha":         strings.Repeat("2", 40),
					"base_sha":         strings.Repeat("1", 40),
					"write_scope_json": `{"paths":["web/**"]}`,
					"review_doc_key":   "project.project-subpixel.branch.branch-ready.review",
					"status":           "READY_FOR_REVIEW",
				}},
			}))
		case 2:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":     "checkout-1",
				"workspace_id":    "ws",
				"project_id":      "project-subpixel",
				"repo_id":         "projrepo-1",
				"machine_id":      rpcString(req.Params, "machine_id"),
				"agent_id":        "agent-alpha",
				"local_path":      localPath,
				"checkout_kind":   "clone",
				"branch_name":     "",
				"base_branch":     "main",
				"dirty_state":     "unknown",
				"active_task_id":  "task-build",
				"active_claim_id": "task-build",
				"status":          "ACTIVE",
			}})
		case 3:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			if got := rpcString(req.Params, "branch_id"); got != "branch-ready" {
				t.Fatalf("branch_id = %q", got)
			}
			if got := rpcString(req.Params, "status"); got != "READY_FOR_REVIEW" {
				t.Fatalf("expected existing READY_FOR_REVIEW status to be preserved, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{"branch": map[string]any{
				"branch_id":        "branch-ready",
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      "checkout-1",
				"agent_id":         "agent-alpha",
				"active_task_id":   "task-build",
				"active_claim_id":  "task-build",
				"branch_name":      branchName,
				"branch_kind":      "feature",
				"base_branch":      "main",
				"write_scope_json": `{"paths":["web/**"]}`,
				"review_doc_key":   "project.project-subpixel.branch.branch-ready.review",
				"status":           "READY_FOR_REVIEW",
			}})
		case 4:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:          "READY",
				RepoURL:             "https://github.com/ExampleOrg/subpixel-lab.git",
				CurrentPhase:        "IMPLEMENTATION",
				WriteScopeJSON:      `{"paths":["web/**"]}`,
				OverallState:        "PARTIAL",
				ImplementationReady: false,
			}))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectCheckoutRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", localPath)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        "project-subpixel",
		"active_task_id":    "task-build",
		"branch_name":       branchName,
		"verify_git_remote": false,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful checkout registration, got %+v", result)
	}
	if calls != 4 {
		t.Fatalf("expected 4 calls, got %d", calls)
	}
}

func TestProjectCheckoutRegisterToolAllowsTerminalEvidenceClosureFromCoordinationTask(t *testing.T) {
	localPath := t.TempDir()
	calls := 0
	branchName := "agent/agent-alpha/project-subpixel/task-build"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:   "READY",
				RepoURL:      "https://github.com/ExampleOrg/subpixel-lab.git",
				CurrentPhase: "IMPLEMENTATION",
				Tasks: []map[string]any{{
					"task_id":      "task-close",
					"status":       "RUNNING",
					"task_kind":    "COORDINATION",
					"project_lane": "coordination",
				}},
				Checkouts: []map[string]any{{
					"checkout_id":   "checkout-old",
					"workspace_id":  "ws",
					"project_id":    "project-subpixel",
					"repo_id":       "projrepo-1",
					"agent_id":      "agent-alpha",
					"machine_id":    runtimeMachineID(),
					"local_path":    localPath,
					"checkout_kind": "clone",
					"branch_name":   branchName,
					"base_branch":   "main",
					"dirty_state":   "clean",
					"status":        "ACTIVE",
				}},
				Branches: []map[string]any{{
					"branch_id":        "branch-merged",
					"workspace_id":     "ws",
					"project_id":       "project-subpixel",
					"repo_id":          "projrepo-1",
					"checkout_id":      "checkout-old",
					"agent_id":         "agent-alpha",
					"branch_name":      branchName,
					"branch_kind":      "feature",
					"base_branch":      "main",
					"head_sha":         strings.Repeat("2", 40),
					"base_sha":         strings.Repeat("1", 40),
					"write_scope_json": `{"paths":["web/**"]}`,
					"status":           "MERGED",
				}},
			}))
		case 2:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcString(req.Params, "active_task_id"); got != "" {
				t.Fatalf("terminal checkout active_task_id = %q, want empty", got)
			}
			if got := rpcString(req.Params, "status"); got != "ARCHIVED" {
				t.Fatalf("checkout status = %q, want ARCHIVED", got)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-old",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"machine_id":    rpcString(req.Params, "machine_id"),
				"agent_id":      "agent-alpha",
				"local_path":    localPath,
				"checkout_kind": "clone",
				"branch_name":   branchName,
				"base_branch":   "main",
				"dirty_state":   "clean",
				"status":        "ARCHIVED",
			}})
		case 3:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			if got := rpcString(req.Params, "branch_id"); got != "branch-merged" {
				t.Fatalf("branch_id = %q", got)
			}
			if got := rpcString(req.Params, "status"); got != "MERGED" {
				t.Fatalf("branch status = %q, want MERGED", got)
			}
			if got := rpcString(req.Params, "active_task_id"); got != "" {
				t.Fatalf("terminal branch active_task_id = %q, want empty", got)
			}
			if got := rpcString(req.Params, "write_scope_json"); got != `{"paths":["web/**"]}` {
				t.Fatalf("terminal branch write_scope_json = %q", got)
			}
			writeRPCResult(w, req, map[string]any{"branch": map[string]any{
				"branch_id":        "branch-merged",
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      "checkout-old",
				"agent_id":         "agent-alpha",
				"branch_name":      branchName,
				"branch_kind":      "feature",
				"base_branch":      "main",
				"write_scope_json": `{"paths":["web/**"]}`,
				"status":           "MERGED",
			}})
		case 4:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:   "READY",
				RepoURL:      "https://github.com/ExampleOrg/subpixel-lab.git",
				CurrentPhase: "IMPLEMENTATION",
			}))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectCheckoutRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", localPath).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-close", ProjectID: "project-subpixel"}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        "project-subpixel",
		"checkout_id":       "checkout-old",
		"branch_id":         "branch-merged",
		"branch_name":       branchName,
		"checkout_status":   "ARCHIVED",
		"branch_status":     "MERGED",
		"dirty_state":       "clean",
		"verify_git_remote": false,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected terminal evidence closure to succeed, got %+v", result)
	}
	if calls != 4 {
		t.Fatalf("expected 4 calls, got %d", calls)
	}
}

func TestProjectCheckoutRegisterToolRejectsTerminalEvidenceClosureForForeignBranch(t *testing.T) {
	localPath := t.TempDir()
	calls := 0
	branchName := "agent/agent-alpha/project-subpixel/task-build"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:   "READY",
			RepoURL:      "https://github.com/ExampleOrg/subpixel-lab.git",
			CurrentPhase: "IMPLEMENTATION",
			Tasks: []map[string]any{{
				"task_id":      "task-close",
				"status":       "RUNNING",
				"task_kind":    "COORDINATION",
				"project_lane": "coordination",
			}},
			Checkouts: []map[string]any{{
				"checkout_id":   "checkout-old",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"agent_id":      "agent-alpha",
				"machine_id":    runtimeMachineID(),
				"local_path":    localPath,
				"checkout_kind": "clone",
				"branch_name":   branchName,
				"base_branch":   "main",
				"dirty_state":   "clean",
				"status":        "ACTIVE",
			}},
			Branches: []map[string]any{{
				"branch_id":        "branch-foreign",
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      "checkout-old",
				"agent_id":         "agent-beta",
				"branch_name":      branchName,
				"branch_kind":      "feature",
				"base_branch":      "main",
				"write_scope_json": `{"paths":["web/**"]}`,
				"status":           "MERGED",
			}},
		}))
	}))
	defer server.Close()

	tool := NewProjectCheckoutRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", localPath).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-close", ProjectID: "project-subpixel"}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        "project-subpixel",
		"checkout_id":       "checkout-old",
		"branch_id":         "branch-foreign",
		"branch_name":       branchName,
		"checkout_status":   "ARCHIVED",
		"branch_status":     "MERGED",
		"dirty_state":       "clean",
		"verify_git_remote": false,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected foreign terminal branch closure to be rejected, got %+v", result)
	}
	if !strings.Contains(result.Output, "existing same-agent branch") {
		t.Fatalf("expected same-agent branch error, got %q", result.Output)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination call, got %d", calls)
	}
}

func TestProjectCheckoutRegisterToolStillRejectsCoordinationBranchReservation(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:     "READY",
			RepoURL:        "https://github.com/ExampleOrg/subpixel-lab.git",
			CurrentPhase:   "IMPLEMENTATION",
			WriteScopeJSON: `{"paths":["web/**"]}`,
			Tasks: []map[string]any{{
				"task_id":      "task-coordinate",
				"status":       "RUNNING",
				"task_kind":    "COORDINATION",
				"project_lane": "coordination",
			}},
		}))
	}))
	defer server.Close()

	tool := NewProjectCheckoutRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", t.TempDir()).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-coordinate", ProjectID: "project-subpixel"}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        "project-subpixel",
		"active_task_id":    "task-coordinate",
		"verify_git_remote": false,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected coordination branch reservation to be rejected, got %+v", result)
	}
	for _, want := range []string{"not implementation-shaped", "lane=coordination"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 1 {
		t.Fatalf("expected only coordination call, got %d", calls)
	}
}

func TestProjectCheckoutRegisterToolRequiresReadyRepository(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:   "CREATED",
			RepoURL:      "https://github.com/ExampleOrg/subpixel-lab.git",
			CurrentPhase: "SPEC",
		}))
	}))
	defer server.Close()

	tool := NewProjectCheckoutRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", t.TempDir())
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        "project-subpixel",
		"verify_git_remote": false,
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "READY repository") {
		t.Fatalf("expected READY repository error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination call, got %d", calls)
	}
}

func TestProjectCheckoutRegisterToolRejectsForeignActiveTask(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:     "READY",
			RepoURL:        "https://github.com/ExampleOrg/subpixel-lab.git",
			CurrentPhase:   "IMPLEMENTATION",
			WriteScopeJSON: `{"paths":["web/**"]}`,
			Tasks: []map[string]any{{
				"task_id":      "task-current",
				"status":       "PENDING",
				"task_kind":    "EXECUTION",
				"project_lane": "implementation",
			}},
		}))
	}))
	defer server.Close()

	tool := NewProjectCheckoutRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", t.TempDir())
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        "project-subpixel",
		"active_task_id":    "task-old",
		"verify_git_remote": false,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected stale active task to be rejected, got %+v", result)
	}
	for _, want := range []string{"stale active_task_id", "task-old", "project-subpixel"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 1 {
		t.Fatalf("expected only coordination call, got %d", calls)
	}
}

func TestProjectCheckoutRegisterToolRejectsReadyRepoWithoutRemote(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:   "READY",
			RepoURL:      "",
			CurrentPhase: "IMPLEMENTATION",
		}))
	}))
	defer server.Close()

	tool := NewProjectCheckoutRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", t.TempDir())
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        "project-subpixel",
		"verify_git_remote": false,
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "remote_url") {
		t.Fatalf("expected READY remote_url error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination call, got %d", calls)
	}
}

func TestProjectCheckoutRegisterToolValidatesLocalCheckoutByDefault(t *testing.T) {
	localPath := t.TempDir()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:     "READY",
			RepoURL:        "https://github.com/ExampleOrg/subpixel-lab.git",
			CurrentPhase:   "IMPLEMENTATION",
			WriteScopeJSON: `{"paths":["web/**"]}`,
		}))
	}))
	defer server.Close()

	tool := NewProjectCheckoutRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", localPath)
	result := tool.Execute(context.Background(), map[string]any{"project_id": "project-subpixel"})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "local verification failed") {
		t.Fatalf("expected local verification error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination call, got %d", calls)
	}
}

func TestProjectCheckoutRegisterToolCompensatesCheckoutOnBranchFailure(t *testing.T) {
	localPath := t.TempDir()
	calls := 0
	var cleanupParams map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:     "READY",
				RepoURL:        "https://github.com/ExampleOrg/subpixel-lab.git",
				CurrentPhase:   "IMPLEMENTATION",
				WriteScopeJSON: `{"paths":["web/**"]}`,
			}))
		case 2:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":     "checkout-1",
				"workspace_id":    "ws",
				"project_id":      "project-subpixel",
				"repo_id":         "projrepo-1",
				"machine_id":      rpcString(req.Params, "machine_id"),
				"agent_id":        "agent-alpha",
				"local_path":      localPath,
				"checkout_kind":   "clone",
				"base_branch":     "main",
				"dirty_state":     "unknown",
				"active_task_id":  "task-build",
				"active_claim_id": "claim-build",
				"status":          "ACTIVE",
			}})
		case 3:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			writeRPCError(w, req, -32000, "branch scope collision")
		case 4:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected cleanup method %q", req.Method)
			}
			cleanupParams = req.Params
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-1",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"machine_id":    rpcString(req.Params, "machine_id"),
				"agent_id":      "agent-alpha",
				"local_path":    localPath,
				"checkout_kind": "clone",
				"base_branch":   "main",
				"dirty_state":   "unknown",
				"status":        "ABANDONED",
			}})
		case 5:
			if req.Method != "agent.update.post" {
				t.Fatalf("unexpected reconciliation update method %q", req.Method)
			}
			payload := rpcString(req.Params, "payload_json")
			for _, want := range []string{"evidence_reconciliation", "project_checkout_register", "branch_register", "branch scope collision"} {
				if !strings.Contains(payload, want) {
					t.Fatalf("expected reconciliation update payload to contain %q, got %s", want, payload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		case 6:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected reconciliation task method %q", req.Method)
			}
			requirements, _ := req.Params["task_requirements"].(map[string]any)
			if requirements["schema"] != "project_branch_commit_evidence_reconciliation_task.v1" ||
				requirements["source_tool"] != "project_checkout_register" ||
				requirements["stage"] != "branch_register" ||
				requirements["commit_created"] != false {
				t.Fatalf("expected register evidence-reconciliation requirements, got %+v", requirements)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id": "task-evidence-reconcile-register",
				"status":  "PENDING",
			})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectCheckoutRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", localPath)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        "project-subpixel",
		"active_task_id":    "task-build",
		"active_claim_id":   "claim-build",
		"verify_git_remote": false,
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, `"gate_type": "evidence_reconciliation"`) ||
		!strings.Contains(result.Output, `"source_tool": "project_checkout_register"`) ||
		!strings.Contains(result.Output, `"reconciliation_task_id": "task-evidence-reconcile-register"`) {
		t.Fatalf("expected typed branch evidence reconciliation, got %+v", result)
	}
	if cleanupParams == nil {
		t.Fatalf("expected checkout cleanup params")
	}
	if cleanupParams["status"] != "ABANDONED" {
		t.Fatalf("cleanup status = %#v, want ABANDONED", cleanupParams["status"])
	}
	if rpcString(cleanupParams, "active_task_id") != "" || rpcString(cleanupParams, "active_claim_id") != "" {
		t.Fatalf("cleanup should clear active refs, got %+v", cleanupParams)
	}
	if calls != 6 {
		t.Fatalf("expected 6 calls, got %d", calls)
	}
}

func TestProjectCheckoutRegisterToolFailsBeforeMutationWithoutWriteScope(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:   "READY",
			RepoURL:      "https://github.com/ExampleOrg/subpixel-lab.git",
			CurrentPhase: "IMPLEMENTATION",
		}))
	}))
	defer server.Close()

	tool := NewProjectCheckoutRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", t.TempDir())
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        "project-subpixel",
		"verify_git_remote": false,
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "write_scope_json") {
		t.Fatalf("expected write_scope_json error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected no checkout mutation after coordination, got %d calls", calls)
	}
}

func TestProjectCheckoutRegisterToolDefaultsActiveClaimIDFromTaskID(t *testing.T) {
	localPath := t.TempDir()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:     "READY",
				RepoURL:        "https://github.com/ExampleOrg/subpixel-lab.git",
				CurrentPhase:   "IMPLEMENTATION",
				WriteScopeJSON: `{"paths":["web/**"]}`,
			}))
		case 2:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcString(req.Params, "active_task_id"); got != "task-build" {
				t.Fatalf("active_task_id = %q", got)
			}
			if got := rpcString(req.Params, "active_claim_id"); got != "task-build" {
				t.Fatalf("active_claim_id = %q", got)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":     "checkout-1",
				"workspace_id":    "ws",
				"project_id":      "project-subpixel",
				"repo_id":         "projrepo-1",
				"machine_id":      rpcString(req.Params, "machine_id"),
				"agent_id":        "agent-alpha",
				"local_path":      localPath,
				"checkout_kind":   "clone",
				"base_branch":     "main",
				"dirty_state":     "unknown",
				"active_task_id":  "task-build",
				"active_claim_id": "task-build",
				"status":          "ACTIVE",
			}})
		case 3:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"branch": map[string]any{
				"branch_id":        "branch-1",
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      "checkout-1",
				"agent_id":         "agent-alpha",
				"active_task_id":   "task-build",
				"active_claim_id":  "task-build",
				"branch_name":      rpcString(req.Params, "branch_name"),
				"branch_kind":      "feature",
				"base_branch":      "main",
				"write_scope_json": `{"paths":["web/**"]}`,
				"status":           "RESERVED",
			}})
		case 4:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:          "READY",
				RepoURL:             "https://github.com/ExampleOrg/subpixel-lab.git",
				CurrentPhase:        "IMPLEMENTATION",
				WriteScopeJSON:      `{"paths":["web/**"]}`,
				OverallState:        "PARTIAL",
				ImplementationReady: false,
			}))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectCheckoutRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", localPath)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        "project-subpixel",
		"active_task_id":    "task-build",
		"verify_git_remote": false,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful checkout registration, got %+v", result)
	}
	if calls != 4 {
		t.Fatalf("expected 4 calls, got %d", calls)
	}
}

func TestProjectCheckoutRegisterExistingBranchSkipsBlockedPatchQueueBranchForRevisionName(t *testing.T) {
	branches := []ProjectBranchRecord{{
		BranchID:       "old-branch",
		RepoID:         "repo-1",
		AgentID:        "beta",
		ActiveTaskID:   "task-repair",
		BranchName:     "agent-beta-old",
		Status:         "READY_FOR_REVIEW",
		ReviewDocKey:   "task.old.review",
		WriteScopeJSON: `{"paths":["src/**"]}`,
	}}
	items := []ProjectPatchQueueItemRecord{{
		ItemID:   "item-1",
		RepoID:   "repo-1",
		BranchID: "old-branch",
		State:    "BLOCKED",
	}}
	if branch, ok := selectProjectCheckoutRegisterExistingBranch(branches, items, "repo-1", "beta", "agent-beta-new", "task-repair"); ok {
		t.Fatalf("expected blocked patch queue source branch to be skipped for revision branch name, got %+v", branch)
	}
	if branch, ok := selectProjectCheckoutRegisterExistingBranch(branches, items, "repo-1", "beta", "agent-beta-old", "task-repair"); !ok || branch.BranchID != "old-branch" {
		t.Fatalf("expected exact branch name to remain selectable, got ok=%v branch=%+v", ok, branch)
	}
}

func TestProjectCheckoutRegisterToolRejectsActiveClaimBranchIDMismatch(t *testing.T) {
	localPath := t.TempDir()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if calls != 1 || req.Method != "project.coordination.get" {
			t.Fatalf("unexpected RPC call %d method=%s", calls, req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:     "READY",
			RepoURL:        "https://github.com/ExampleOrg/subpixel-lab.git",
			CurrentPhase:   "IMPLEMENTATION",
			WriteScopeJSON: `{"paths":["web/**"]}`,
			Checkouts: []map[string]any{{
				"checkout_id":     "checkout-active",
				"workspace_id":    "ws",
				"project_id":      "project-subpixel",
				"repo_id":         "projrepo-1",
				"machine_id":      runtimeMachineID(),
				"agent_id":        "agent-alpha",
				"local_path":      localPath,
				"checkout_kind":   "clone",
				"branch_name":     "claim-branch",
				"active_task_id":  "task-build",
				"active_claim_id": "claim-build",
				"status":          "ACTIVE",
			}},
			Branches: []map[string]any{{
				"branch_id":        "branch-stale",
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      "checkout-active",
				"agent_id":         "agent-alpha",
				"active_task_id":   "task-build",
				"active_claim_id":  "claim-build",
				"branch_name":      "stale-branch",
				"branch_kind":      "feature",
				"base_branch":      "main",
				"write_scope_json": `{"paths":["web/**"]}`,
				"status":           "READY_FOR_REVIEW",
			}},
		}))
	}))
	defer server.Close()

	tool := NewProjectCheckoutRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", localPath).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{
				TaskID:          "task-build",
				ProjectID:       "project-subpixel",
				ClaimCheckoutID: "checkout-active",
				ClaimBranchID:   "branch-active",
			}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        "project-subpixel",
		"checkout_id":       "checkout-active",
		"active_task_id":    "task-build",
		"active_claim_id":   "claim-build",
		"branch_name":       "stale-branch",
		"verify_git_remote": false,
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "active claim branch branch-active") {
		t.Fatalf("expected active claim branch mismatch error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected failure before mutation, got %d calls", calls)
	}
}

func TestProjectCheckoutRegisterToolRejectsBranchNameMismatchWithCurrentGitBranch(t *testing.T) {
	remoteURL := seedProjectCheckoutRegisterBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "active")
	if err := os.MkdirAll(filepath.Dir(checkoutPath), 0o755); err != nil {
		t.Fatalf("create checkout parent: %v", err)
	}
	runProjectCheckoutRegisterGit(t, "", "clone", remoteURL, checkoutPath)
	runProjectCheckoutRegisterGit(t, checkoutPath, "checkout", "-b", "claim-branch")

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if calls != 1 || req.Method != "project.coordination.get" {
			t.Fatalf("unexpected RPC call %d method=%s", calls, req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:     "READY",
			RepoURL:        remoteURL,
			CurrentPhase:   "IMPLEMENTATION",
			WriteScopeJSON: `{"paths":["web/**"]}`,
		}))
	}))
	defer server.Close()

	tool := NewProjectCheckoutRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":      "project-subpixel",
		"local_path":      checkoutPath,
		"active_task_id":  "task-build",
		"active_claim_id": "claim-build",
		"branch_name":     "stale-branch",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "currently on branch claim-branch") {
		t.Fatalf("expected current branch mismatch error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected failure before mutation, got %d calls", calls)
	}
}

func TestProjectCheckoutRegisterToolSendsVerifiedHeadSHA(t *testing.T) {
	remoteURL := seedProjectCheckoutRegisterBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "active")
	if err := os.MkdirAll(filepath.Dir(checkoutPath), 0o755); err != nil {
		t.Fatalf("create checkout parent: %v", err)
	}
	runProjectCheckoutRegisterGit(t, "", "clone", remoteURL, checkoutPath)
	runProjectCheckoutRegisterGit(t, checkoutPath, "checkout", "-b", "claim-branch")
	headSHA := runProjectCheckoutRegisterGit(t, checkoutPath, "rev-parse", "HEAD")

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:     "READY",
				RepoURL:        remoteURL,
				CurrentPhase:   "IMPLEMENTATION",
				WriteScopeJSON: `{"paths":["web/**"]}`,
			}))
		case 2:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcString(req.Params, "branch_name"); got != "claim-branch" {
				t.Fatalf("checkout branch_name = %q", got)
			}
			if got := rpcString(req.Params, "head_sha"); got != headSHA {
				t.Fatalf("checkout head_sha = %q, want %q", got, headSHA)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":     "checkout-1",
				"workspace_id":    "ws",
				"project_id":      "project-subpixel",
				"repo_id":         "projrepo-1",
				"machine_id":      rpcString(req.Params, "machine_id"),
				"agent_id":        "agent-alpha",
				"local_path":      checkoutPath,
				"checkout_kind":   "clone",
				"branch_name":     "claim-branch",
				"base_branch":     "main",
				"head_sha":        headSHA,
				"dirty_state":     "unknown",
				"active_task_id":  "task-build",
				"active_claim_id": "claim-build",
				"status":          "ACTIVE",
			}})
		case 3:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			if got := rpcString(req.Params, "branch_name"); got != "claim-branch" {
				t.Fatalf("branch_name = %q", got)
			}
			if got := rpcString(req.Params, "head_sha"); got != headSHA {
				t.Fatalf("branch head_sha = %q, want %q", got, headSHA)
			}
			writeRPCResult(w, req, map[string]any{"branch": map[string]any{
				"branch_id":        "branch-1",
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      "checkout-1",
				"agent_id":         "agent-alpha",
				"active_task_id":   "task-build",
				"active_claim_id":  "claim-build",
				"branch_name":      "claim-branch",
				"branch_kind":      "feature",
				"base_branch":      "main",
				"head_sha":         headSHA,
				"write_scope_json": `{"paths":["web/**"]}`,
				"status":           "RESERVED",
			}})
		case 4:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:          "READY",
				RepoURL:             remoteURL,
				CurrentPhase:        "IMPLEMENTATION",
				WriteScopeJSON:      `{"paths":["web/**"]}`,
				OverallState:        "PARTIAL",
				ImplementationReady: false,
			}))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectCheckoutRegisterTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":      "project-subpixel",
		"local_path":      checkoutPath,
		"active_task_id":  "task-build",
		"active_claim_id": "claim-build",
		"branch_name":     "claim-branch",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful verified registration, got %+v", result)
	}
	if calls != 4 {
		t.Fatalf("expected 4 calls, got %d", calls)
	}
}

func seedProjectCheckoutRegisterBareGitRepo(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is required for project checkout register git verification tests")
	}
	root := t.TempDir()
	repoPath := filepath.Join(root, "repo")
	remotePath := filepath.Join(root, "remote.git")
	runProjectCheckoutRegisterGit(t, "", "init", repoPath)
	runProjectCheckoutRegisterGit(t, repoPath, "checkout", "-b", "main")
	runProjectCheckoutRegisterGit(t, repoPath, "config", "user.email", "tester@example.com")
	runProjectCheckoutRegisterGit(t, repoPath, "config", "user.name", "Rhizome Test")
	if err := os.WriteFile(filepath.Join(repoPath, "README.md"), []byte("seed\n"), 0o644); err != nil {
		t.Fatalf("write seed file: %v", err)
	}
	runProjectCheckoutRegisterGit(t, repoPath, "add", "README.md")
	runProjectCheckoutRegisterGit(t, repoPath, "commit", "-m", "seed")
	runProjectCheckoutRegisterGit(t, "", "init", "--bare", remotePath)
	remoteURL := filepath.ToSlash(remotePath)
	runProjectCheckoutRegisterGit(t, repoPath, "remote", "add", "origin", remoteURL)
	runProjectCheckoutRegisterGit(t, repoPath, "push", "-u", "origin", "main")
	runProjectCheckoutRegisterGit(t, "", "--git-dir", remotePath, "symbolic-ref", "HEAD", "refs/heads/main")
	return remoteURL
}

func runProjectCheckoutRegisterGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmdArgs := append([]string{}, args...)
	if strings.TrimSpace(dir) != "" {
		cmdArgs = append([]string{"-C", dir}, cmdArgs...)
	}
	cmd := exec.Command("git", cmdArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s failed: %v\n%s", strings.Join(cmdArgs, " "), err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out))
}

type projectCheckoutCoordinationInput struct {
	RepoStatus          string
	RepoURL             string
	CurrentPhase        string
	WriteScopeJSON      string
	OverallState        string
	ImplementationReady bool
	Tasks               []map[string]any
	Checkouts           []map[string]any
	Branches            []map[string]any
	PatchQueueItems     []map[string]any
}

func projectCheckoutCoordinationResult(input projectCheckoutCoordinationInput) map[string]any {
	repoStatus := firstNonEmpty(input.RepoStatus, "READY")
	currentPhase := firstNonEmpty(input.CurrentPhase, "IMPLEMENTATION")
	overallState := firstNonEmpty(input.OverallState, "BLOCKED")
	roles := []map[string]any{}
	if input.WriteScopeJSON != "" {
		roles = append(roles, map[string]any{
			"role_id":          "projrole-alpha",
			"workspace_id":     "ws",
			"project_id":       "project-subpixel",
			"agent_id":         "agent-alpha",
			"role_type":        "IMPLEMENTER",
			"status":           "ACTIVE",
			"write_scope_json": input.WriteScopeJSON,
		})
	}
	tasks := input.Tasks
	if tasks == nil {
		tasks = []map[string]any{{
			"task_id":      "task-build",
			"status":       "RUNNING",
			"task_kind":    "EXECUTION",
			"project_lane": "implementation",
		}}
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
			"current_phase":       currentPhase,
			"repo_required":       true,
			"repo_status":         repoStatus,
			"repo_url":            input.RepoURL,
			"repo_default_branch": "main",
		},
		"gate_status": map[string]any{
			"workspace_id":         "ws",
			"project_id":           "project-subpixel",
			"overall_state":        overallState,
			"implementation_ready": input.ImplementationReady,
		},
		"roles": roles,
		"repositories": []map[string]any{{
			"workspace_id":   "ws",
			"project_id":     "project-subpixel",
			"repo_id":        "projrepo-1",
			"remote_url":     input.RepoURL,
			"remote_kind":    "github",
			"owner":          "ExampleOrg",
			"name":           "subpixel-lab",
			"default_branch": "main",
			"repo_status":    repoStatus,
			"is_canonical":   true,
		}},
		"checkouts":         input.Checkouts,
		"branches":          input.Branches,
		"patch_queue_items": input.PatchQueueItems,
		"tasks":             tasks,
	}}
}
