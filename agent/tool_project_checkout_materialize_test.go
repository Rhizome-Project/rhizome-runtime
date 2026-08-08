package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProjectCheckoutMaterializeToolClonesBranchesAndRegistersEvidence(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	expectedPath := projectCheckoutMaterializeDefaultPath(workdir, "project-subpixel", ProjectRepositoryRecord{
		RepoID: "projrepo-1",
		Name:   "subpixel-lab",
	})
	expectedBranchName := projectClaimBranchName("agent-alpha", "project-subpixel", "task-build")

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1, 2:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected coordination method %q on call %d", req.Method, calls)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:     "READY",
				RepoURL:        remote,
				CurrentPhase:   "IMPLEMENTATION",
				WriteScopeJSON: `{"paths":["web/**"]}`,
			}))
		case 3:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected checkout method %q", req.Method)
			}
			if got := rpcString(req.Params, "local_path"); got != expectedPath {
				t.Fatalf("local_path = %q, want %q", got, expectedPath)
			}
			if got := rpcString(req.Params, "branch_name"); got != expectedBranchName {
				t.Fatalf("checkout branch_name = %q", got)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":     "checkout-1",
				"workspace_id":    "ws",
				"project_id":      "project-subpixel",
				"repo_id":         "projrepo-1",
				"machine_id":      rpcString(req.Params, "machine_id"),
				"agent_id":        "agent-alpha",
				"local_path":      rpcString(req.Params, "local_path"),
				"checkout_kind":   "clone",
				"branch_name":     rpcString(req.Params, "branch_name"),
				"base_branch":     "main",
				"dirty_state":     "clean",
				"active_task_id":  "task-build",
				"active_claim_id": "task-build",
				"status":          "ACTIVE",
			}})
		case 4:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected branch method %q", req.Method)
			}
			if got := rpcString(req.Params, "write_scope_json"); got != `{"paths":["web/**"]}` {
				t.Fatalf("write_scope_json = %q", got)
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
		case 5:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:          "READY",
				RepoURL:             remote,
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

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"active_task_id": "task-build",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful materialization, got %+v", result)
	}
	for _, want := range []string{`"clone_performed": true`, `"git_commit_push": false`, "checkout-1", "branch-1"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if _, err := os.Stat(filepath.Join(expectedPath, ".git")); err != nil {
		t.Fatalf("expected cloned checkout at %s: %v", expectedPath, err)
	}
	current := gitOutput(t, expectedPath, "branch", "--show-current")
	if current != expectedBranchName {
		t.Fatalf("unexpected current branch %q", current)
	}
	if calls != 5 {
		t.Fatalf("expected 5 calls, got %d", calls)
	}
}

func TestProjectCheckoutMaterializeToolPropagatesRegisterEvidenceReconciliation(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	repo := ProjectRepositoryRecord{RepoID: "projrepo-1", Name: "subpixel-lab", RemoteURL: remote}
	defaultPath := projectCheckoutMaterializeDefaultPath(workdir, "project-subpixel", repo)
	branchName := projectClaimBranchName("agent-alpha", "project-subpixel", "task-build")
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1, 2:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected coordination method %q on call %d", req.Method, calls)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:     "READY",
				RepoURL:        remote,
				CurrentPhase:   "IMPLEMENTATION",
				WriteScopeJSON: `{"paths":["web/**"]}`,
				Tasks: []map[string]any{{
					"task_id":      "task-build",
					"status":       "RUNNING",
					"task_kind":    "EXECUTION",
					"project_lane": "implementation",
				}},
			}))
		case 3:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected checkout register method %q", req.Method)
			}
			if got := rpcString(req.Params, "branch_name"); got != branchName {
				t.Fatalf("branch_name = %q, want %q", got, branchName)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":     "checkout-new",
				"workspace_id":    "ws",
				"project_id":      "project-subpixel",
				"repo_id":         "projrepo-1",
				"machine_id":      rpcString(req.Params, "machine_id"),
				"agent_id":        "agent-alpha",
				"local_path":      rpcString(req.Params, "local_path"),
				"checkout_kind":   "clone",
				"branch_name":     rpcString(req.Params, "branch_name"),
				"base_branch":     "main",
				"dirty_state":     "clean",
				"active_task_id":  "task-build",
				"active_claim_id": "task-build",
				"status":          "ACTIVE",
			}})
		case 4:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected branch register method %q", req.Method)
			}
			writeRPCError(w, req, -32000, "project branch scope mismatch: branch_name belongs to branch_id=old-branch")
		case 5:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected checkout cleanup method %q", req.Method)
			}
			if req.Params["status"] != "ABANDONED" {
				t.Fatalf("cleanup status = %#v, want ABANDONED", req.Params["status"])
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-new",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"machine_id":    rpcString(req.Params, "machine_id"),
				"agent_id":      "agent-alpha",
				"local_path":    rpcString(req.Params, "local_path"),
				"checkout_kind": "clone",
				"base_branch":   "main",
				"dirty_state":   "clean",
				"status":        "ABANDONED",
			}})
		case 6:
			if req.Method != "agent.update.post" {
				t.Fatalf("unexpected reconciliation update method %q", req.Method)
			}
			payload := rpcString(req.Params, "payload_json")
			for _, want := range []string{"evidence_reconciliation", "project_checkout_register", "branch_register", "old-branch"} {
				if !strings.Contains(payload, want) {
					t.Fatalf("expected reconciliation update payload to contain %q, got %s", want, payload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		case 7:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected reconciliation task method %q", req.Method)
			}
			requirements, _ := req.Params["task_requirements"].(map[string]any)
			if requirements["source_tool"] != "project_checkout_register" || requirements["stage"] != "branch_register" {
				t.Fatalf("expected materialize/register evidence reconciliation requirements, got %+v", requirements)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id": "task-evidence-reconcile-materialize-register",
				"status":  "PENDING",
			})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-build", ProjectID: "project-subpixel"}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"active_task_id": "task-build",
	})
	if result == nil || !result.IsError ||
		!strings.Contains(result.Output, `"gate_type": "evidence_reconciliation"`) ||
		!strings.Contains(result.Output, `"source_tool": "project_checkout_register"`) ||
		!strings.Contains(result.Output, `"reconciliation_task_id": "task-evidence-reconcile-materialize-register"`) {
		t.Fatalf("expected propagated typed evidence reconciliation, got %+v", result)
	}
	if _, err := os.Stat(defaultPath); !os.IsNotExist(err) {
		t.Fatalf("expected failed cloned checkout to be removed, stat err=%v", err)
	}
	if calls != 7 {
		t.Fatalf("expected 7 calls, got %d", calls)
	}
}

func TestProjectCheckoutMaterializeToolRejectsProseWriteScopeBeforeClone(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if calls != 1 || req.Method != "project.coordination.get" {
			t.Fatalf("unexpected RPC call %d method=%s", calls, req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:   "READY",
			RepoURL:      "file:///tmp/clearpress-prose-scope.git",
			CurrentPhase: "IMPLEMENTATION",
		}))
	}))
	defer server.Close()

	workdir := t.TempDir()
	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "beta", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":           "project-clearpress",
		"write_scope_json":     `{"paths":["existing Clearpress shell/workspace checkout","app shell","routing"]}`,
		"register_branch":      true,
		"allow_dirty_existing": true,
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "not prose") {
		t.Fatalf("expected prose write_scope_json rejection before clone, got %+v", result)
	}
	if _, err := os.Stat(filepath.Join(workdir, "project-checkouts")); !os.IsNotExist(err) {
		t.Fatalf("project_checkout_materialize should reject before creating checkout dirs, stat err=%v", err)
	}
}

func TestProjectCheckoutMaterializeDefaultPathCompactsLongProjectAndRepoNames(t *testing.T) {
	workdir := filepath.Join(`C:\fixtures\agents\beta`)
	projectID := "project-task-subpixel-web-processor-deployment-20260507-105918"
	repo := ProjectRepositoryRecord{
		RepoID:    "project-task-subpixel-web-processor-deployment-20260507-105918",
		Name:      "project-task-subpixel-web-processor-deployment-20260507-105918",
		RemoteURL: "file:///C:/fixtures/agents/zeta/project-remotes/project-task-subpixel-web-processor-deployment-20260507-105918/project-task-subpixel-web-processor-deployment-20260507-105918.git",
	}
	path := projectCheckoutMaterializeDefaultPath(workdir, projectID, repo)
	if strings.Contains(path, projectID) || strings.Contains(path, repo.Name) {
		t.Fatalf("default checkout path should not include full long ids, got %q", path)
	}
	wantSuffix := filepath.Join("project-checkouts", "p-"+shortRefHash(projectID)+"-r-"+shortRefHash(repo.RepoID))
	if !strings.HasSuffix(path, wantSuffix) {
		t.Fatalf("default checkout path = %q, want suffix %q", path, wantSuffix)
	}
	refLockPath := filepath.Join(path, ".git", "refs", "heads", projectClaimBranchName("beta", projectID, "task-subpixel-web-ui-scaffold-deployment-20260507-105918")+".lock")
	if len(refLockPath) >= 240 {
		t.Fatalf("checkout path leaves too little Windows path headroom: len=%d path=%q", len(refLockPath), refLockPath)
	}
}

func TestProjectCheckoutMaterializeActiveTaskBranchPrefersExistingOwnedBranch(t *testing.T) {
	coordination := ProjectCoordinationRecord{
		Branches: []ProjectBranchRecord{
			{
				BranchID:     "branch-generated",
				RepoID:       "repo-1",
				AgentID:      "beta",
				ActiveTaskID: "task-repair",
				BranchName:   "agent-beta-p-project-t-generated",
				Status:       "ACTIVE",
			},
			{
				BranchID:     "branch-owned",
				RepoID:       "repo-1",
				CheckoutID:   "checkout-owned",
				AgentID:      "beta",
				ActiveTaskID: "task-repair",
				BranchName:   "agent-beta-p-project-t-flag-narrow",
				HeadSHA:      "329b42a8235dc845453e9b71fa1c19765e4692a8",
				Status:       "RESERVED",
			},
			{
				BranchID:     "branch-old",
				RepoID:       "repo-1",
				CheckoutID:   "checkout-old",
				AgentID:      "beta",
				ActiveTaskID: "task-repair",
				BranchName:   "agent-beta-p-project-t-old",
				HeadSHA:      "old",
				Status:       "ABANDONED",
			},
		},
	}
	branch, ok := projectCheckoutMaterializeActiveTaskBranch(coordination, "repo-1", "beta", "task-repair")
	if !ok {
		t.Fatal("expected active task branch")
	}
	if branch.BranchID != "branch-owned" {
		t.Fatalf("expected existing owned branch with checkout/head evidence, got %+v", branch)
	}
}

func TestProjectCheckoutMaterializeCommittedBranchReturnsLifecycleGate(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	branchName := "agent/beta/project-icon-sprite/task-repair"
	remote := seedBareGitRepoWithBranch(t, branchName, filepath.Join("src", "App.tsx"), "export default function App() { return null }\n")
	workdir := t.TempDir()
	repo := ProjectRepositoryRecord{
		RepoID:        "projrepo-1",
		Name:          "icon-sprite-forge",
		RemoteURL:     remote,
		DefaultBranch: "main",
	}
	checkoutPath := projectCheckoutMaterializeDefaultPath(workdir, "project-icon-sprite", repo)
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", branchName)
	headSHA := gitOutput(t, checkoutPath, "rev-parse", "HEAD")
	coordination := ProjectCoordinationRecord{
		Project: ProjectRecord{ProjectID: "project-icon-sprite"},
		Profile: ProjectProfileRecord{
			ProjectID:         "project-icon-sprite",
			RepoDefaultBranch: "main",
		},
		Checkouts: []ProjectCheckoutRecord{{
			CheckoutID:   "checkout-beta",
			ProjectID:    "project-icon-sprite",
			RepoID:       "projrepo-1",
			AgentID:      "agent-beta",
			LocalPath:    checkoutPath,
			CheckoutKind: "clone",
			BranchName:   branchName,
			HeadSHA:      headSHA,
			ActiveTaskID: "task-repair",
			Status:       "ACTIVE",
		}},
		Branches: []ProjectBranchRecord{{
			BranchID:     "branch-beta",
			ProjectID:    "project-icon-sprite",
			RepoID:       "projrepo-1",
			CheckoutID:   "checkout-beta",
			AgentID:      "agent-beta",
			ActiveTaskID: "task-repair",
			BranchName:   branchName,
			BranchKind:   "feature",
			BaseBranch:   "main",
			HeadSHA:      headSHA,
			Status:       "ACTIVE",
		}},
	}
	output, ok := projectCheckoutMaterializeCommittedBranchOutput(context.Background(), projectCheckoutMaterializeCommittedBranchInput{
		Workdir:         workdir,
		ProjectID:       "project-icon-sprite",
		Coordination:    coordination,
		Repo:            repo,
		AgentID:         "agent-beta",
		ActiveTaskID:    "task-repair",
		BranchName:      branchName,
		ExpectedHeadSHA: headSHA,
	})
	if !ok {
		t.Fatal("expected existing committed branch to produce lifecycle gate output")
	}
	for _, want := range []string{`"already_materialized": true`, `"committed_branch_reused": true`, `"mandatory_next_tool": "project_branch_review_ready"`, `"next_gate": "project_branch_review_ready"`} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, output)
		}
	}

	output, ok = projectCheckoutMaterializeCommittedBranchOutput(context.Background(), projectCheckoutMaterializeCommittedBranchInput{
		Workdir:            workdir,
		ProjectID:          "project-icon-sprite",
		Coordination:       coordination,
		Repo:               repo,
		AgentID:            "agent-beta",
		ActiveTaskID:       "task-repair",
		BranchName:         branchName,
		ExpectedHeadSHA:    headSHA,
		RequestedLocalPath: checkoutPath,
	})
	if !ok || !strings.Contains(output, `"mandatory_next_tool": "project_branch_review_ready"`) {
		t.Fatalf("expected explicit local_path retry to return lifecycle gate, got ok=%v output=%q", ok, output)
	}

	coordination.Branches[0].Status = "READY_FOR_REVIEW"
	output, ok = projectCheckoutMaterializeCommittedBranchOutput(context.Background(), projectCheckoutMaterializeCommittedBranchInput{
		Workdir:         workdir,
		ProjectID:       "project-icon-sprite",
		Coordination:    coordination,
		Repo:            repo,
		AgentID:         "agent-beta",
		ActiveTaskID:    "task-repair",
		BranchName:      branchName,
		ExpectedHeadSHA: headSHA,
	})
	if !ok || !strings.Contains(output, `"mandatory_next_tool": "project_patch_queue_submit"`) {
		t.Fatalf("expected READY_FOR_REVIEW branch to route to patch queue submit, got ok=%v output=%q", ok, output)
	}

	coordination.Branches[0].Status = "ACTIVE"
	generatedProofDir := filepath.Join(checkoutPath, ".tmp-bench")
	if err := os.MkdirAll(generatedProofDir, 0o755); err != nil {
		t.Fatalf("mkdir generated proof dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(generatedProofDir, "iconPipeline.js"), []byte("generated benchmark helper\n"), 0o644); err != nil {
		t.Fatalf("write generated proof helper: %v", err)
	}
	output, ok = projectCheckoutMaterializeCommittedBranchOutput(context.Background(), projectCheckoutMaterializeCommittedBranchInput{
		Workdir:         workdir,
		ProjectID:       "project-icon-sprite",
		Coordination:    coordination,
		Repo:            repo,
		AgentID:         "agent-beta",
		ActiveTaskID:    "task-repair",
		BranchName:      branchName,
		ExpectedHeadSHA: headSHA,
	})
	if !ok || !strings.Contains(output, `"mandatory_next_tool": "project_branch_review_ready"`) {
		t.Fatalf("generated proof temp dirs should not block lifecycle gate, got ok=%v output=%q", ok, output)
	}

	if err := os.WriteFile(filepath.Join(checkoutPath, "dirty.txt"), []byte("uncommitted local change\n"), 0o644); err != nil {
		t.Fatalf("write dirty marker: %v", err)
	}
	if output, ok = projectCheckoutMaterializeCommittedBranchOutput(context.Background(), projectCheckoutMaterializeCommittedBranchInput{
		Workdir:         workdir,
		ProjectID:       "project-icon-sprite",
		Coordination:    coordination,
		Repo:            repo,
		AgentID:         "agent-beta",
		ActiveTaskID:    "task-repair",
		BranchName:      branchName,
		ExpectedHeadSHA: headSHA,
	}); ok {
		t.Fatalf("dirty committed checkout should not be routed directly to review-ready, got %q", output)
	}
}

func TestProjectCheckoutMaterializeToolUsesRemoteBranchForPeerValidation(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	branchName := "agent/gamma/project-subpixel/task-build"
	candidatePath := filepath.Join("web", "index.html")
	remote := seedBareGitRepoWithBranch(t, branchName, candidatePath, "<h1>remote candidate</h1>\n")
	workdir := t.TempDir()
	expectedHead := gitOutput(t, remote, "rev-parse", branchName)
	expectedPath := projectCheckoutMaterializeValidationPath(workdir, "project-subpixel", ProjectRepositoryRecord{
		RepoID:    "projrepo-1",
		Name:      "subpixel-lab",
		RemoteURL: remote,
	}, branchName, expectedHead)

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1, 2:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected coordination method %q on call %d", req.Method, calls)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:   "READY",
				RepoURL:      remote,
				CurrentPhase: "VALIDATION",
				Branches: []map[string]any{{
					"branch_id":   "branch-gamma",
					"repo_id":     "projrepo-1",
					"agent_id":    "agent-gamma",
					"branch_name": branchName,
					"head_sha":    expectedHead,
					"status":      "READY_FOR_REVIEW",
				}},
			}))
		case 3:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected checkout method %q", req.Method)
			}
			if got := rpcString(req.Params, "local_path"); got != expectedPath {
				t.Fatalf("local_path = %q, want %q", got, expectedPath)
			}
			if got := rpcString(req.Params, "branch_name"); got != branchName {
				t.Fatalf("checkout branch_name = %q, want %q", got, branchName)
			}
			if got := rpcString(req.Params, "active_task_id"); got != "" {
				t.Fatalf("peer validation checkout should not fake implementation task binding, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-validation",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"machine_id":    rpcString(req.Params, "machine_id"),
				"agent_id":      "agent-epsilon",
				"local_path":    rpcString(req.Params, "local_path"),
				"checkout_kind": "clone",
				"branch_name":   rpcString(req.Params, "branch_name"),
				"base_branch":   "main",
				"dirty_state":   "clean",
				"status":        "ACTIVE",
			}})
		case 4:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:   "READY",
				RepoURL:      remote,
				CurrentPhase: "VALIDATION",
			}))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-epsilon", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":      "project-subpixel",
		"branch_name":     branchName,
		"register_branch": false,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful peer validation materialization, got %+v", result)
	}
	if !strings.Contains(result.Output, `"remote_branch_used": true`) {
		t.Fatalf("expected output to expose remote branch checkout, got %q", result.Output)
	}
	current := gitOutput(t, expectedPath, "branch", "--show-current")
	if current != branchName {
		t.Fatalf("current branch = %q, want %q", current, branchName)
	}
	head := gitOutput(t, expectedPath, "rev-parse", "HEAD")
	remoteHead := gitOutput(t, expectedPath, "rev-parse", "refs/remotes/origin/"+branchName)
	if head != remoteHead {
		t.Fatalf("HEAD = %s, want remote branch head %s", head, remoteHead)
	}
	if head != expectedHead {
		t.Fatalf("HEAD = %s, want expected coordination head %s", head, expectedHead)
	}
	raw, err := os.ReadFile(filepath.Join(expectedPath, candidatePath))
	if err != nil {
		t.Fatalf("expected candidate file from remote branch: %v", err)
	}
	if strings.ReplaceAll(string(raw), "\r\n", "\n") != "<h1>remote candidate</h1>\n" {
		t.Fatalf("unexpected candidate content %q", string(raw))
	}
	if calls != 4 {
		t.Fatalf("expected 4 calls, got %d", calls)
	}
}

func TestProjectCheckoutMaterializeToolPeerValidationPinsExpectedHeadWhenRemoteTipDrifts(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	branchName := "agent/beta/project-subpixel/task-build"
	candidatePath := filepath.Join("web", "index.html")
	remote := seedBareGitRepoWithBranch(t, branchName, candidatePath, "<h1>old candidate</h1>\n")
	oldHead := gitOutput(t, remote, "rev-parse", branchName)
	source := filepath.Join(t.TempDir(), "advance-source")
	runGitNoDir(t, "clone", remote, source)
	runGit(t, source, "config", "user.name", "Rhizome Test")
	runGit(t, source, "config", "user.email", "rhizome-test@example.invalid")
	runGit(t, source, "checkout", branchName)
	if err := os.WriteFile(filepath.Join(source, candidatePath), []byte("<h1>new tip</h1>\n"), 0o644); err != nil {
		t.Fatalf("advance candidate file: %v", err)
	}
	runGit(t, source, "add", candidatePath)
	runGit(t, source, "commit", "-m", "Advance candidate branch")
	runGit(t, source, "push", "origin", branchName)
	newHead := gitOutput(t, remote, "rev-parse", branchName)
	if oldHead == newHead {
		t.Fatalf("expected remote branch to advance")
	}
	workdir := t.TempDir()
	repo := ProjectRepositoryRecord{RepoID: "projrepo-1", Name: "subpixel-lab", RemoteURL: remote}
	expectedPath := projectCheckoutMaterializeValidationPath(workdir, "project-subpixel", repo, branchName, oldHead)

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1, 2:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected coordination method %q on call %d", req.Method, calls)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:   "READY",
				RepoURL:      remote,
				CurrentPhase: "VALIDATION",
				Branches: []map[string]any{{
					"branch_id":   "branch-beta",
					"repo_id":     "projrepo-1",
					"agent_id":    "agent-beta",
					"branch_name": branchName,
					"head_sha":    oldHead,
					"status":      "READY_FOR_REVIEW",
				}},
			}))
		case 3:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected checkout method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-validation",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"agent_id":      "agent-kappa",
				"local_path":    rpcString(req.Params, "local_path"),
				"checkout_kind": "review",
				"branch_name":   rpcString(req.Params, "branch_name"),
				"base_branch":   "main",
				"dirty_state":   "clean",
				"status":        "ACTIVE",
			}})
		case 4:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:   "READY",
				RepoURL:      remote,
				CurrentPhase: "VALIDATION",
			}))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-kappa", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":      "project-subpixel",
		"branch_name":     branchName,
		"register_branch": false,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected validation to pin old candidate head despite remote tip drift, got %+v", result)
	}
	if !strings.Contains(result.Output, `"head_sha": "`+oldHead+`"`) {
		t.Fatalf("expected output to expose old head %s, got %q", oldHead, result.Output)
	}
	if head := gitOutput(t, expectedPath, "rev-parse", "HEAD"); head != oldHead {
		t.Fatalf("HEAD = %s, want pinned old head %s", head, oldHead)
	}
	if branch := gitOutput(t, expectedPath, "branch", "--show-current"); branch != "" {
		t.Fatalf("expected detached validation checkout, got branch %q", branch)
	}
	raw, err := os.ReadFile(filepath.Join(expectedPath, candidatePath))
	if err != nil {
		t.Fatalf("expected candidate file from pinned head: %v", err)
	}
	if strings.ReplaceAll(string(raw), "\r\n", "\n") != "<h1>old candidate</h1>\n" {
		t.Fatalf("unexpected pinned candidate content %q", string(raw))
	}
	if calls != 4 {
		t.Fatalf("expected 4 calls, got %d", calls)
	}
}

func TestProjectCheckoutMaterializeToolPeerValidationUsesSeparatePathWhenDefaultDirty(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	branchName := "agent/gamma/project-subpixel/task-build"
	remote := seedBareGitRepoWithBranch(t, branchName, filepath.Join("web", "index.html"), "<h1>remote candidate</h1>\n")
	workdir := t.TempDir()
	repo := ProjectRepositoryRecord{RepoID: "projrepo-1", Name: "subpixel-lab", RemoteURL: remote}
	defaultPath := projectCheckoutMaterializeDefaultPath(workdir, "project-subpixel", repo)
	runGitNoDir(t, "clone", remote, defaultPath)
	runGit(t, defaultPath, "checkout", "-B", "agent/agent-epsilon/project-subpixel/local-work")
	if err := os.WriteFile(filepath.Join(defaultPath, "scratch.txt"), []byte("dirty owner work\n"), 0o644); err != nil {
		t.Fatalf("write dirty scratch: %v", err)
	}
	expectedHead := gitOutput(t, remote, "rev-parse", branchName)
	expectedPath := projectCheckoutMaterializeValidationPath(workdir, "project-subpixel", repo, branchName, expectedHead)

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1, 2:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected coordination method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:   "READY",
				RepoURL:      remote,
				CurrentPhase: "VALIDATION",
				Branches: []map[string]any{{
					"branch_id":   "branch-gamma",
					"repo_id":     "projrepo-1",
					"agent_id":    "agent-gamma",
					"branch_name": branchName,
					"head_sha":    expectedHead,
					"status":      "READY_FOR_REVIEW",
				}},
			}))
		case 3:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected checkout method %q", req.Method)
			}
			if got := rpcString(req.Params, "local_path"); got != expectedPath {
				t.Fatalf("validation local_path = %q, want %q", got, expectedPath)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-validation",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"agent_id":      "agent-epsilon",
				"local_path":    rpcString(req.Params, "local_path"),
				"checkout_kind": "clone",
				"branch_name":   rpcString(req.Params, "branch_name"),
				"base_branch":   "main",
				"dirty_state":   "clean",
				"status":        "ACTIVE",
			}})
		case 4:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:   "READY",
				RepoURL:      remote,
				CurrentPhase: "VALIDATION",
			}))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-epsilon", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":      "project-subpixel",
		"branch_name":     branchName,
		"register_branch": false,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected validation checkout to ignore dirty default path, got %+v", result)
	}
	if got := gitOutput(t, defaultPath, "status", "--porcelain"); !strings.Contains(got, "scratch.txt") {
		t.Fatalf("expected dirty owner checkout to remain untouched, got %q", got)
	}
	if _, err := os.Stat(filepath.Join(expectedPath, "web", "index.html")); err != nil {
		t.Fatalf("expected separate validation checkout file: %v", err)
	}
}

func TestProjectCheckoutMaterializeToolPeerValidationRequiresRemoteBranch(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	branchName := "agent/gamma/project-subpixel/missing"

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:   "READY",
			RepoURL:      remote,
			CurrentPhase: "VALIDATION",
		}))
	}))
	defer server.Close()

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-epsilon", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":      "project-subpixel",
		"branch_name":     branchName,
		"register_branch": false,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected missing remote branch to fail closed, got %+v", result)
	}
	for _, want := range []string{"requires branch", "exist on origin", branchName} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 1 {
		t.Fatalf("expected only initial coordination call, got %d", calls)
	}
}

func TestProjectCheckoutMaterializeToolRejectsForeignOwnedBranchForImplementation(t *testing.T) {
	workdir := t.TempDir()
	foreignBranchName := "agent-gamma-p-project-task"
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:     "READY",
			RepoURL:        "file:///tmp/subpixel-lab.git",
			CurrentPhase:   "IMPLEMENTATION",
			WriteScopeJSON: `{"paths":["src/**"]}`,
			Tasks: []map[string]any{{
				"task_id":      "task-revision",
				"status":       "RUNNING",
				"task_kind":    "EXECUTION",
				"project_lane": "implementation",
			}},
			Branches: []map[string]any{{
				"branch_id":   "branch-gamma",
				"repo_id":     "projrepo-1",
				"agent_id":    "agent-gamma",
				"branch_name": foreignBranchName,
				"status":      "READY_FOR_REVIEW",
			}},
		}))
	}))
	defer server.Close()

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-iota", "owner-1", workdir).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-revision", ProjectID: "project-subpixel"}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":       "project-subpixel",
		"branch_name":      foreignBranchName,
		"active_task_id":   "task-revision",
		"write_scope_json": `{"paths":["src/**"]}`,
		"register_branch":  true,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected foreign branch ownership rejection, got %+v", result)
	}
	for _, want := range []string{"already owned by agent agent-gamma", "base_branch", "omit branch_name", "new owned revision branch"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if _, err := os.Stat(filepath.Join(workdir, "project-checkouts")); !os.IsNotExist(err) {
		t.Fatalf("expected no checkout directory after foreign branch rejection, err=%v", err)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination call before clone/register, got %d", calls)
	}
}

func TestProjectCheckoutMaterializeToolDefaultsValidationCheckoutForQATaskWhenBound(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	repo := ProjectRepositoryRecord{RepoID: "projrepo-1", Name: "subpixel-lab", RemoteURL: remote}
	expectedPath := projectCheckoutMaterializeValidationPath(workdir, "project-subpixel", repo, "main", "")

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1, 2:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected coordination method %q on call %d", req.Method, calls)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:   "READY",
				RepoURL:      remote,
				CurrentPhase: "VALIDATION",
				Tasks: []map[string]any{{
					"task_id":      "task-qa",
					"status":       "RUNNING",
					"task_kind":    "EXECUTION",
					"project_lane": "qa",
				}},
			}))
		case 3:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected checkout method %q", req.Method)
			}
			if got := rpcString(req.Params, "local_path"); got != expectedPath {
				t.Fatalf("local_path = %q, want %q", got, expectedPath)
			}
			if got := rpcString(req.Params, "checkout_kind"); got != "review" {
				t.Fatalf("checkout_kind = %q, want review", got)
			}
			if got := rpcString(req.Params, "branch_name"); got != "main" {
				t.Fatalf("branch_name = %q, want main", got)
			}
			if got := rpcString(req.Params, "active_task_id"); got != "task-qa" {
				t.Fatalf("active_task_id = %q, want task-qa", got)
			}
			if got := rpcString(req.Params, "write_scope_json"); got != "" {
				t.Fatalf("validation checkout should not require write scope, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":    "checkout-validation",
				"workspace_id":   "ws",
				"project_id":     "project-subpixel",
				"repo_id":        "projrepo-1",
				"machine_id":     rpcString(req.Params, "machine_id"),
				"agent_id":       "agent-theta",
				"local_path":     rpcString(req.Params, "local_path"),
				"checkout_kind":  "review",
				"branch_name":    rpcString(req.Params, "branch_name"),
				"base_branch":    "main",
				"dirty_state":    "clean",
				"active_task_id": "task-qa",
				"status":         "ACTIVE",
			}})
		case 4:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:   "READY",
				RepoURL:      remote,
				CurrentPhase: "VALIDATION",
			}))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-theta", "owner-1", workdir).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-qa", ProjectID: "project-subpixel"}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful QA validation checkout, got %+v", result)
	}
	for _, want := range []string{`"register_branch": false`, `"checkout_kind": "review"`, `"validation_checkout": true`, `"branch_name": "main"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if current := gitOutput(t, expectedPath, "branch", "--show-current"); current != "main" {
		t.Fatalf("current branch = %q, want main", current)
	}
	if calls != 4 {
		t.Fatalf("expected 4 calls and no project.branch.register call, got %d", calls)
	}
}

func TestProjectCheckoutMaterializeToolDefaultsPeerValidationForQATaskWhenBound(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	branchName := "agent/gamma/project-subpixel/task-build"
	remote := seedBareGitRepoWithBranch(t, branchName, filepath.Join("web", "index.html"), "<h1>remote candidate</h1>\n")
	workdir := t.TempDir()
	expectedHead := gitOutput(t, remote, "rev-parse", branchName)
	expectedPath := projectCheckoutMaterializeValidationPath(workdir, "project-subpixel", ProjectRepositoryRecord{
		RepoID:    "projrepo-1",
		Name:      "subpixel-lab",
		RemoteURL: remote,
	}, branchName, expectedHead)

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1, 2:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected coordination method %q on call %d", req.Method, calls)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:   "READY",
				RepoURL:      remote,
				CurrentPhase: "VALIDATION",
				Tasks: []map[string]any{{
					"task_id":      "task-qa",
					"status":       "RUNNING",
					"task_kind":    "EXECUTION",
					"project_lane": "qa",
				}},
				Branches: []map[string]any{{
					"branch_id":   "branch-gamma",
					"repo_id":     "projrepo-1",
					"agent_id":    "agent-gamma",
					"branch_name": branchName,
					"head_sha":    expectedHead,
					"status":      "READY_FOR_REVIEW",
				}},
			}))
		case 3:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected checkout method %q", req.Method)
			}
			if got := rpcString(req.Params, "local_path"); got != expectedPath {
				t.Fatalf("local_path = %q, want %q", got, expectedPath)
			}
			if got := rpcString(req.Params, "checkout_kind"); got != "review" {
				t.Fatalf("checkout_kind = %q, want review", got)
			}
			if got := rpcString(req.Params, "branch_name"); got != branchName {
				t.Fatalf("branch_name = %q, want %q", got, branchName)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-validation",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"agent_id":      "agent-theta",
				"local_path":    rpcString(req.Params, "local_path"),
				"checkout_kind": "review",
				"branch_name":   rpcString(req.Params, "branch_name"),
				"base_branch":   "main",
				"dirty_state":   "clean",
				"status":        "ACTIVE",
			}})
		case 4:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:   "READY",
				RepoURL:      remote,
				CurrentPhase: "VALIDATION",
			}))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-theta", "owner-1", workdir).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-qa", ProjectID: "project-subpixel"}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":  "project-subpixel",
		"branch_name": branchName,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful QA peer validation checkout, got %+v", result)
	}
	if !strings.Contains(result.Output, `"register_branch": false`) || !strings.Contains(result.Output, `"remote_branch_used": true`) {
		t.Fatalf("expected output to expose validation remote checkout, got %q", result.Output)
	}
	if head := gitOutput(t, expectedPath, "rev-parse", "HEAD"); head != expectedHead {
		t.Fatalf("HEAD = %s, want expected coordination head %s", head, expectedHead)
	}
	if calls != 4 {
		t.Fatalf("expected 4 calls and no project.branch.register call, got %d", calls)
	}
}

func TestProjectCheckoutMaterializeToolRejectsExplicitQABranchBeforeCloneWhenBound(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:   "READY",
			RepoURL:      remote,
			CurrentPhase: "VALIDATION",
			Tasks: []map[string]any{{
				"task_id":      "task-qa",
				"status":       "RUNNING",
				"task_kind":    "EXECUTION",
				"project_lane": "qa",
			}},
		}))
	}))
	defer server.Close()

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-theta", "owner-1", workdir).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-qa", ProjectID: "project-subpixel"}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":      "project-subpixel",
		"register_branch": true,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected explicit QA branch reservation to fail, got %+v", result)
	}
	for _, want := range []string{"not implementation-shaped", "lane=qa", "claimed implementation task"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if _, err := os.Stat(filepath.Join(workdir, "project-checkouts")); !os.IsNotExist(err) {
		t.Fatalf("expected no checkout directory after QA branch rejection, err=%v", err)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination call before QA branch rejection, got %d", calls)
	}
}

func TestProjectCheckoutMaterializeToolRequiresRuntimeClaimForValidationCheckoutWhenBound(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:   "READY",
			RepoURL:      remote,
			CurrentPhase: "VALIDATION",
			Tasks: []map[string]any{{
				"task_id":      "task-qa",
				"status":       "RUNNING",
				"task_kind":    "EXECUTION",
				"project_lane": "qa",
			}},
		}))
	}))
	defer server.Close()

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-theta", "owner-1", workdir).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":      "project-subpixel",
		"register_branch": false,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected validation checkout without runtime claim to fail, got %+v", result)
	}
	for _, want := range []string{"active claimed runtime task", "model.ask", "claim the relevant project task"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if _, err := os.Stat(filepath.Join(workdir, "project-checkouts")); !os.IsNotExist(err) {
		t.Fatalf("expected no checkout directory after missing runtime claim rejection, err=%v", err)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination call, got %d", calls)
	}
}

func TestProjectCheckoutMaterializeToolDoesNotBypassDirtySameBranch(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	repo := ProjectRepositoryRecord{RepoID: "projrepo-1", Name: "subpixel-lab", RemoteURL: remote}
	defaultPath := projectCheckoutMaterializeDefaultPath(workdir, "project-subpixel", repo)
	branchName := "agent/agent-alpha/project-subpixel/task-build"
	runGitNoDir(t, "clone", remote, defaultPath)
	runGit(t, defaultPath, "checkout", "-B", branchName)
	if err := os.WriteFile(filepath.Join(defaultPath, "scratch.txt"), []byte("dirty same branch\n"), 0o644); err != nil {
		t.Fatalf("write dirty scratch: %v", err)
	}

	calls := 0
	classificationTaskID := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1, 5:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:     "READY",
				RepoURL:        remote,
				CurrentPhase:   "IMPLEMENTATION",
				WriteScopeJSON: `{"paths":["web/**"]}`,
				Tasks: []map[string]any{{
					"task_id":      "task-build",
					"status":       "RUNNING",
					"task_kind":    "EXECUTION",
					"project_lane": "implementation",
				}},
			}))
		case 2, 6:
			if req.Method != "agent.task.hydrate" {
				t.Fatalf("expected side-effect denial hydration, got %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"updates": []map[string]any{},
				},
			})
		case 3, 7:
			if req.Method != "agent.update.post" {
				t.Fatalf("expected side-effect update post, got %q", req.Method)
			}
			payload := rpcString(req.Params, "payload_json")
			for _, want := range []string{"artifact_bound_side_effect.v1", "pending_classification", "scratch.txt", "project_checkout_materialize"} {
				if !strings.Contains(payload, want) {
					t.Fatalf("expected side-effect payload to contain %q, got %s", want, payload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		case 4, 8:
			assertSideEffectClassificationTaskSubmit(t, req, "scratch.txt")
			if classificationTaskID == "" {
				classificationTaskID = rpcString(req.Params, "task_id")
			}
			if calls == 8 {
				writeRPCError(w, req, -32602, "workspace task already exists")
				return
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-side-effect-classify-scratch",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		case 9:
			if req.Method != "workspace.tasks.list" {
				t.Fatalf("expected duplicate classification status lookup, got %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"tasks": []map[string]any{{
					"task_id": classificationTaskID,
					"status":  "PENDING",
				}},
			})
		default:
			t.Fatalf("unexpected extra method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":           "project-subpixel",
		"branch_name":          branchName,
		"active_task_id":       "task-build",
		"allow_dirty_existing": true,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected dirty same-branch checkout rejection, got %+v", result)
	}
	for _, want := range []string{"side_effect_classification", "pending_classification", "scratch.txt", "project_checkout_materialize"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected dirty checkout side-effect guidance to contain %q, got %q", want, result.Output)
		}
	}
	alternatePath := projectCheckoutMaterializeBranchPath(workdir, "project-subpixel", repo, branchName, "")
	if _, err := os.Stat(alternatePath); !os.IsNotExist(err) {
		t.Fatalf("dirty same branch must not be bypassed into alternate checkout, err=%v", err)
	}
	if calls != 4 {
		t.Fatalf("expected coordination, hydration, side-effect update, and classification task before dirty rejection, got %d", calls)
	}
	firstRef := firstSideEffectRefFromToolOutput(t, result.Output)

	repeated := tool.Execute(context.Background(), map[string]any{
		"project_id":           "project-subpixel",
		"branch_name":          branchName,
		"active_task_id":       "task-build",
		"allow_dirty_existing": true,
	})
	if repeated == nil || !repeated.IsError {
		t.Fatalf("expected repeated dirty same-branch checkout rejection, got %+v", repeated)
	}
	secondRef := firstSideEffectRefFromToolOutput(t, repeated.Output)
	if firstRef == "" || secondRef == "" || firstRef != secondRef {
		t.Fatalf("expected stable repeated side_effect_ref, first=%q second=%q\nfirst=%s\nsecond=%s", firstRef, secondRef, result.Output, repeated.Output)
	}
	if !strings.Contains(repeated.Output, `"classification_task_created": false`) || !strings.Contains(repeated.Output, "classification task already exists") {
		t.Fatalf("expected repeated materialize to reuse classification task, got %s", repeated.Output)
	}
	if calls != 9 {
		t.Fatalf("expected repeated materialize to use same coordination/hydration/update/classification path, got %d calls", calls)
	}
}

func TestProjectCheckoutMaterializeToolAllowsInBoundaryDirtyReuseAfterPreflight(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	repo := ProjectRepositoryRecord{RepoID: "projrepo-1", Name: "subpixel-lab", RemoteURL: remote}
	defaultPath := projectCheckoutMaterializeDefaultPath(workdir, "project-subpixel", repo)
	branchName := "agent/agent-alpha/project-subpixel/task-build"
	runGitNoDir(t, "clone", remote, defaultPath)
	runGit(t, defaultPath, "checkout", "-B", branchName)
	if err := os.MkdirAll(filepath.Join(defaultPath, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(defaultPath, "web", "draft.ts"), []byte("export const draft = true\n"), 0o644); err != nil {
		t.Fatalf("write in-boundary dirty file: %v", err)
	}

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1, 2:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected coordination method %q on call %d", req.Method, calls)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:     "READY",
				RepoURL:        remote,
				CurrentPhase:   "IMPLEMENTATION",
				WriteScopeJSON: `{"paths":["web/**"]}`,
				Tasks: []map[string]any{{
					"task_id":      "task-build",
					"status":       "RUNNING",
					"task_kind":    "EXECUTION",
					"project_lane": "implementation",
				}},
			}))
		case 3:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected checkout method %q", req.Method)
			}
			if got := rpcString(req.Params, "dirty_state"); got != "dirty" {
				t.Fatalf("dirty_state = %q, want dirty", got)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":     "checkout-1",
				"workspace_id":    "ws",
				"project_id":      "project-subpixel",
				"repo_id":         "projrepo-1",
				"machine_id":      rpcString(req.Params, "machine_id"),
				"agent_id":        "agent-alpha",
				"local_path":      rpcString(req.Params, "local_path"),
				"checkout_kind":   "clone",
				"branch_name":     rpcString(req.Params, "branch_name"),
				"base_branch":     "main",
				"dirty_state":     "dirty",
				"active_task_id":  "task-build",
				"active_claim_id": "task-build",
				"status":          "ACTIVE",
			}})
		case 4:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected branch method %q", req.Method)
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
		case 5:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:     "READY",
				RepoURL:        remote,
				CurrentPhase:   "IMPLEMENTATION",
				WriteScopeJSON: `{"paths":["web/**"]}`,
			}))
		default:
			t.Fatalf("unexpected extra method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":           "project-subpixel",
		"branch_name":          branchName,
		"active_task_id":       "task-build",
		"allow_dirty_existing": true,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected in-boundary dirty reuse to succeed, got %+v", result)
	}
	for _, want := range []string{`"dirty_state": "dirty"`, "web/draft.ts"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected materialize output to contain %q, got %s", want, result.Output)
		}
	}
	if calls != 5 {
		t.Fatalf("expected ordinary materialize/register path without side-effect task, got %d calls", calls)
	}
}

func TestProjectCheckoutMaterializeToolClassifiesDirtyDefaultBeforeRedirect(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	repo := ProjectRepositoryRecord{RepoID: "projrepo-1", Name: "subpixel-lab", RemoteURL: remote}
	defaultPath := projectCheckoutMaterializeDefaultPath(workdir, "project-subpixel", repo)
	oldBranch := "agent/agent-alpha/project-subpixel/task-old"
	newBranch := "agent/agent-alpha/project-subpixel/task-new"
	runGitNoDir(t, "clone", remote, defaultPath)
	runGit(t, defaultPath, "checkout", "-B", oldBranch)
	if err := os.WriteFile(filepath.Join(defaultPath, "package.json"), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write dirty package.json: %v", err)
	}

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected coordination method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:     "READY",
				RepoURL:        remote,
				CurrentPhase:   "IMPLEMENTATION",
				WriteScopeJSON: `{"paths":["**"]}`,
				Tasks: []map[string]any{{
					"task_id":      "task-new",
					"status":       "RUNNING",
					"task_kind":    "EXECUTION",
					"project_lane": "implementation",
				}},
				Branches: []map[string]any{{
					"branch_id":        "branch-old",
					"workspace_id":     "ws",
					"project_id":       "project-subpixel",
					"repo_id":          "projrepo-1",
					"agent_id":         "agent-alpha",
					"active_task_id":   "task-old",
					"active_claim_id":  "task-old",
					"branch_name":      oldBranch,
					"branch_kind":      "feature",
					"base_branch":      "main",
					"write_scope_json": `{"paths":["src/auth/**"]}`,
					"status":           "ACTIVE",
				}},
			}))
		case 2:
			if req.Method != "agent.task.hydrate" {
				t.Fatalf("expected side-effect denial hydration, got %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"updates": []map[string]any{},
				},
			})
		case 3:
			if req.Method != "agent.update.post" {
				t.Fatalf("expected side-effect update post, got %q", req.Method)
			}
			payload := rpcString(req.Params, "payload_json")
			for _, want := range []string{"artifact_bound_side_effect.v1", "pending_classification", "package.json", "branch-old"} {
				if !strings.Contains(payload, want) {
					t.Fatalf("expected side-effect payload to contain %q, got %s", want, payload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		case 4:
			assertSideEffectClassificationTaskSubmit(t, req, "package.json")
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-side-effect-classify-package",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		default:
			t.Fatalf("unexpected extra method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":           "project-subpixel",
		"branch_name":          newBranch,
		"active_task_id":       "task-new",
		"allow_dirty_existing": true,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected dirty default checkout to block before redirect, got %+v", result)
	}
	for _, want := range []string{"side_effect_classification", "pending_classification", "package.json", "project_checkout_materialize"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected dirty checkout side-effect guidance to contain %q, got %q", want, result.Output)
		}
	}
	if !strings.Contains(result.Output, `"write_scope_json": "{\"paths\":[\"src/auth/**\"]}"`) {
		t.Fatalf("expected dirty checkpoint to use existing branch boundary, got %s", result.Output)
	}
	alternatePath := projectCheckoutMaterializeBranchPath(workdir, "project-subpixel", repo, newBranch, "")
	if _, err := os.Stat(alternatePath); !os.IsNotExist(err) {
		t.Fatalf("dirty default checkout must not be redirected before classification, err=%v", err)
	}
	if calls != 4 {
		t.Fatalf("expected coordination, hydration, side-effect update, and classification task before redirect, got %d calls", calls)
	}
}

func firstSideEffectRefFromToolOutput(t *testing.T, output string) string {
	t.Helper()
	var decoded struct {
		SideEffects []struct {
			SideEffectRef string `json:"side_effect_ref"`
		} `json:"side_effects"`
	}
	if err := json.Unmarshal([]byte(output), &decoded); err != nil {
		t.Fatalf("decode tool output: %v\n%s", err, output)
	}
	if len(decoded.SideEffects) == 0 {
		t.Fatalf("expected side_effects in output: %s", output)
	}
	return decoded.SideEffects[0].SideEffectRef
}

func TestProjectCheckoutMaterializeToolSelfHealsInvalidMarkerOnlyDirtyCheckout(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	repo := ProjectRepositoryRecord{RepoID: "projrepo-1", Name: "subpixel-lab", RemoteURL: remote}
	defaultPath := projectCheckoutMaterializeDefaultPath(workdir, "project-subpixel", repo)
	branchName := projectClaimBranchName("agent-alpha", "project-subpixel", "task-build")
	runGitNoDir(t, "clone", remote, defaultPath)
	runGit(t, defaultPath, "checkout", "-B", branchName)
	if err := os.WriteFile(filepath.Join(defaultPath, ".rhizome-invalid-checkout"), []byte("transient registration failure\n"), 0o644); err != nil {
		t.Fatalf("write invalid marker: %v", err)
	}

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1, 2:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected coordination method %q on call %d", req.Method, calls)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:     "READY",
				RepoURL:        remote,
				CurrentPhase:   "IMPLEMENTATION",
				WriteScopeJSON: `{"paths":["web/**"]}`,
				Tasks: []map[string]any{{
					"task_id":      "task-build",
					"status":       "RUNNING",
					"task_kind":    "EXECUTION",
					"project_lane": "implementation",
				}},
			}))
		case 3:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected checkout method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":     "checkout-1",
				"workspace_id":    "ws",
				"project_id":      "project-subpixel",
				"repo_id":         "projrepo-1",
				"machine_id":      rpcString(req.Params, "machine_id"),
				"agent_id":        "agent-alpha",
				"local_path":      rpcString(req.Params, "local_path"),
				"checkout_kind":   "clone",
				"branch_name":     rpcString(req.Params, "branch_name"),
				"base_branch":     rpcString(req.Params, "base_branch"),
				"dirty_state":     "clean",
				"active_task_id":  "task-build",
				"active_claim_id": "task-build",
				"status":          "ACTIVE",
			}})
		case 4:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected branch method %q", req.Method)
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
				"base_branch":      rpcString(req.Params, "base_branch"),
				"write_scope_json": `{"paths":["web/**"]}`,
				"status":           "RESERVED",
			}})
		case 5:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:   "READY",
				RepoURL:      remote,
				CurrentPhase: "IMPLEMENTATION",
			}))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-build", ProjectID: "project-subpixel"}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"branch_name":    branchName,
		"active_task_id": "task-build",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected marker-only checkout to self-heal, got %+v", result)
	}
	if _, err := os.Stat(filepath.Join(defaultPath, ".rhizome-invalid-checkout")); !os.IsNotExist(err) {
		t.Fatalf("expected invalid marker removed after successful retry, err=%v", err)
	}
	if calls != 5 {
		t.Fatalf("expected 5 RPC calls, got %d", calls)
	}
}

func TestProjectCheckoutMaterializeToolRedirectsPriorSameAgentCandidateToRevisionBranch(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	priorBranchName := "agent-beta-p-693356dbab-t-old"
	remote := seedBareGitRepoWithBranch(t, priorBranchName, filepath.Join("src", "App.tsx"), "export default function App() { return null }\n")
	workdir := t.TempDir()
	repo := ProjectRepositoryRecord{RepoID: "projrepo-1", Name: "sprite-workshop", RemoteURL: remote}
	defaultPath := projectCheckoutMaterializeDefaultPath(workdir, "project-sprite", repo)
	runGitNoDir(t, "clone", remote, defaultPath)
	runGit(t, defaultPath, "checkout", priorBranchName)
	priorHead := gitOutput(t, defaultPath, "rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(defaultPath, ".rhizome-invalid-checkout"), []byte("failed prior registration\n"), 0o644); err != nil {
		t.Fatalf("write invalid marker: %v", err)
	}
	revisionBranchName := projectClaimBranchName("beta", "project-sprite", "task-repair")
	revisionPath := projectCheckoutMaterializeBranchPath(workdir, "project-sprite", repo, revisionBranchName, priorHead)

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1, 2:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected coordination method %q on call %d", req.Method, calls)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:     "READY",
				RepoURL:        remote,
				CurrentPhase:   "IMPLEMENTATION",
				WriteScopeJSON: `{"paths":["src/**","tests/**","package.json"]}`,
				Tasks: []map[string]any{{
					"task_id":      "task-repair",
					"status":       "RUNNING",
					"task_kind":    "EXECUTION",
					"project_lane": "implementation",
				}},
				Branches: []map[string]any{{
					"branch_id":        "prior-branch",
					"repo_id":          "projrepo-1",
					"agent_id":         "beta",
					"active_task_id":   "task-repair",
					"branch_name":      priorBranchName,
					"branch_kind":      "feature",
					"base_branch":      "main",
					"head_sha":         priorHead,
					"status":           "READY_FOR_REVIEW",
					"review_doc_key":   "task.old.review",
					"write_scope_json": `{"paths":["src/**"]}`,
				}},
				PatchQueueItems: []map[string]any{{
					"queue_id":  "queue-1",
					"item_id":   "item-1",
					"repo_id":   "projrepo-1",
					"branch_id": "prior-branch",
					"head_sha":  priorHead,
					"state":     "BLOCKED",
				}},
			}))
		case 3:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected checkout method %q", req.Method)
			}
			if got := rpcString(req.Params, "local_path"); got != revisionPath {
				t.Fatalf("revision local_path = %q, want %q", got, revisionPath)
			}
			if got := rpcString(req.Params, "branch_name"); got != revisionBranchName {
				t.Fatalf("checkout branch_name = %q, want %q", got, revisionBranchName)
			}
			if got := rpcString(req.Params, "base_branch"); got != priorBranchName {
				t.Fatalf("checkout base_branch = %q, want %q", got, priorBranchName)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":     "checkout-repair",
				"workspace_id":    "ws",
				"project_id":      "project-sprite",
				"repo_id":         "projrepo-1",
				"machine_id":      rpcString(req.Params, "machine_id"),
				"agent_id":        "beta",
				"local_path":      rpcString(req.Params, "local_path"),
				"checkout_kind":   "clone",
				"branch_name":     rpcString(req.Params, "branch_name"),
				"base_branch":     rpcString(req.Params, "base_branch"),
				"dirty_state":     "clean",
				"active_task_id":  "task-repair",
				"active_claim_id": "task-repair",
				"status":          "ACTIVE",
			}})
		case 4:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected branch method %q", req.Method)
			}
			if got := rpcString(req.Params, "branch_name"); got != revisionBranchName {
				t.Fatalf("branch register branch_name = %q, want %q", got, revisionBranchName)
			}
			if got := rpcString(req.Params, "base_branch"); got != priorBranchName {
				t.Fatalf("branch register base_branch = %q, want %q", got, priorBranchName)
			}
			writeRPCResult(w, req, map[string]any{"branch": map[string]any{
				"branch_id":        "branch-repair",
				"workspace_id":     "ws",
				"project_id":       "project-sprite",
				"repo_id":          "projrepo-1",
				"checkout_id":      "checkout-repair",
				"agent_id":         "beta",
				"active_task_id":   "task-repair",
				"active_claim_id":  "task-repair",
				"branch_name":      rpcString(req.Params, "branch_name"),
				"branch_kind":      "feature",
				"base_branch":      rpcString(req.Params, "base_branch"),
				"head_sha":         priorHead,
				"write_scope_json": `{"paths":["src/**","tests/**","package.json"]}`,
				"status":           "RESERVED",
			}})
		case 5:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:   "READY",
				RepoURL:      remote,
				CurrentPhase: "IMPLEMENTATION",
			}))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "beta", "owner-1", workdir).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-repair", ProjectID: "project-sprite"}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        "project-sprite",
		"branch_name":       priorBranchName,
		"expected_head_sha": priorHead,
		"active_task_id":    "task-repair",
		"write_scope_json":  `{"paths":["src/**","tests/**","package.json"]}`,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected revision branch materialize success, got %+v", result)
	}
	for _, want := range []string{
		`"requested_branch_redirected": true`,
		`"revision_base_branch": "` + priorBranchName + `"`,
		`"branch_name": "` + revisionBranchName + `"`,
	} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if got := gitOutput(t, revisionPath, "branch", "--show-current"); got != revisionBranchName {
		t.Fatalf("revision checkout branch = %q, want %q", got, revisionBranchName)
	}
	if head := gitOutput(t, revisionPath, "rev-parse", "HEAD"); head != priorHead {
		t.Fatalf("revision checkout HEAD = %s, want prior head %s", head, priorHead)
	}
	if calls != 5 {
		t.Fatalf("expected 5 RPC calls, got %d", calls)
	}
}

func TestProjectCheckoutMaterializeToolAlignsStaleRevisionBranchToBaseHead(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	priorBranchName := "agent-beta-p-693356dbab-t-old"
	remote := seedBareGitRepoWithBranch(t, priorBranchName, filepath.Join("src", "App.tsx"), "export default function App() { return null }\n")
	workdir := t.TempDir()
	repo := ProjectRepositoryRecord{RepoID: "projrepo-1", Name: "sprite-workshop", RemoteURL: remote}
	defaultPath := projectCheckoutMaterializeDefaultPath(workdir, "project-sprite", repo)
	runGitNoDir(t, "clone", remote, defaultPath)
	runGit(t, defaultPath, "checkout", priorBranchName)
	priorHead := gitOutput(t, defaultPath, "rev-parse", "HEAD")
	mainHead := gitOutput(t, defaultPath, "rev-parse", "origin/main")
	if priorHead == mainHead {
		t.Fatalf("test setup needs prior branch head to differ from main")
	}
	revisionBranchName := projectClaimBranchName("beta", "project-sprite", "task-repair")
	revisionPath := projectCheckoutMaterializeBranchPath(workdir, "project-sprite", repo, revisionBranchName, priorHead)
	runGitNoDir(t, "clone", remote, revisionPath)
	runGit(t, revisionPath, "checkout", "-B", revisionBranchName, "origin/main")
	if head := gitOutput(t, revisionPath, "rev-parse", "HEAD"); head != mainHead {
		t.Fatalf("stale revision branch HEAD = %s, want main %s", head, mainHead)
	}

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1, 2:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected coordination method %q on call %d", req.Method, calls)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:     "READY",
				RepoURL:        remote,
				CurrentPhase:   "IMPLEMENTATION",
				WriteScopeJSON: `{"paths":["src/**","tests/**","package.json"]}`,
				Tasks: []map[string]any{{
					"task_id":      "task-repair",
					"status":       "RUNNING",
					"task_kind":    "EXECUTION",
					"project_lane": "implementation",
				}},
				Branches: []map[string]any{{
					"branch_id":        "prior-branch",
					"repo_id":          "projrepo-1",
					"agent_id":         "beta",
					"active_task_id":   "task-repair",
					"branch_name":      priorBranchName,
					"branch_kind":      "feature",
					"base_branch":      "main",
					"head_sha":         priorHead,
					"status":           "READY_FOR_REVIEW",
					"review_doc_key":   "task.old.review",
					"write_scope_json": `{"paths":["src/**"]}`,
				}},
				PatchQueueItems: []map[string]any{{
					"queue_id":  "queue-1",
					"item_id":   "item-1",
					"repo_id":   "projrepo-1",
					"branch_id": "prior-branch",
					"head_sha":  priorHead,
					"state":     "BLOCKED",
				}},
			}))
		case 3:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected checkout method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":     "checkout-repair",
				"workspace_id":    "ws",
				"project_id":      "project-sprite",
				"repo_id":         "projrepo-1",
				"machine_id":      rpcString(req.Params, "machine_id"),
				"agent_id":        "beta",
				"local_path":      rpcString(req.Params, "local_path"),
				"checkout_kind":   "clone",
				"branch_name":     rpcString(req.Params, "branch_name"),
				"base_branch":     rpcString(req.Params, "base_branch"),
				"dirty_state":     "clean",
				"active_task_id":  "task-repair",
				"active_claim_id": "task-repair",
				"status":          "ACTIVE",
			}})
		case 4:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected branch method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"branch": map[string]any{
				"branch_id":        "branch-repair",
				"workspace_id":     "ws",
				"project_id":       "project-sprite",
				"repo_id":          "projrepo-1",
				"checkout_id":      "checkout-repair",
				"agent_id":         "beta",
				"active_task_id":   "task-repair",
				"active_claim_id":  "task-repair",
				"branch_name":      rpcString(req.Params, "branch_name"),
				"branch_kind":      "feature",
				"base_branch":      rpcString(req.Params, "base_branch"),
				"head_sha":         priorHead,
				"write_scope_json": `{"paths":["src/**","tests/**","package.json"]}`,
				"status":           "RESERVED",
			}})
		case 5:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:   "READY",
				RepoURL:      remote,
				CurrentPhase: "IMPLEMENTATION",
			}))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "beta", "owner-1", workdir).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-repair", ProjectID: "project-sprite"}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        "project-sprite",
		"branch_name":       priorBranchName,
		"expected_head_sha": priorHead,
		"active_task_id":    "task-repair",
		"write_scope_json":  `{"paths":["src/**","tests/**","package.json"]}`,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected stale revision branch alignment success, got %+v", result)
	}
	if head := gitOutput(t, revisionPath, "rev-parse", "HEAD"); head != priorHead {
		t.Fatalf("revision checkout HEAD = %s, want prior head %s", head, priorHead)
	}
	if got := gitOutput(t, revisionPath, "branch", "--show-current"); got != revisionBranchName {
		t.Fatalf("revision checkout branch = %q, want %q", got, revisionBranchName)
	}
	if calls != 5 {
		t.Fatalf("expected 5 RPC calls, got %d", calls)
	}
}

func TestProjectCheckoutMaterializeToolPeerValidationRejectsHeadMismatch(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	branchName := "agent/gamma/project-subpixel/task-build"
	remote := seedBareGitRepoWithBranch(t, branchName, filepath.Join("web", "index.html"), "<h1>remote candidate</h1>\n")
	workdir := t.TempDir()

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:   "READY",
			RepoURL:      remote,
			CurrentPhase: "VALIDATION",
		}))
	}))
	defer server.Close()

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-epsilon", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        "project-subpixel",
		"branch_name":       branchName,
		"expected_head_sha": "0000000000000000000000000000000000000000",
		"register_branch":   false,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected head mismatch to fail closed, got %+v", result)
	}
	for _, want := range []string{"does not match expected_head_sha", branchName} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if calls != 1 {
		t.Fatalf("expected no registration after head mismatch, got %d calls", calls)
	}
}

func TestProjectCheckoutMaterializeToolInfersImplementationTaskForDefaultBranch(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	expectedBranchName := projectClaimBranchName("agent-alpha", "project-subpixel", "task-impl")

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1, 2:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected coordination method %q on call %d", req.Method, calls)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:     "READY",
				RepoURL:        remote,
				CurrentPhase:   "IMPLEMENTATION",
				WriteScopeJSON: `{"paths":["app/**"]}`,
				Tasks: []map[string]any{
					{
						"task_id":      "task-root",
						"status":       "RUNNING",
						"task_kind":    "COORDINATION",
						"project_lane": "strategy",
					},
					{
						"task_id":      "task-impl",
						"status":       "PENDING",
						"task_kind":    "EXECUTION",
						"project_lane": "implementation",
					},
				},
			}))
		case 3:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected checkout method %q", req.Method)
			}
			if got := rpcString(req.Params, "active_task_id"); got != "" {
				t.Fatalf("inferred task should name the branch but not fake an active claim binding, got active_task_id %q", got)
			}
			if got := rpcString(req.Params, "branch_name"); got != expectedBranchName {
				t.Fatalf("expected inferred task branch, got %q", got)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-1",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"machine_id":    rpcString(req.Params, "machine_id"),
				"agent_id":      "agent-alpha",
				"local_path":    rpcString(req.Params, "local_path"),
				"checkout_kind": "clone",
				"branch_name":   rpcString(req.Params, "branch_name"),
				"base_branch":   "main",
				"dirty_state":   "clean",
				"status":        "ACTIVE",
			}})
		case 4:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected branch method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"branch": map[string]any{
				"branch_id":        "branch-1",
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      "checkout-1",
				"agent_id":         "agent-alpha",
				"branch_name":      rpcString(req.Params, "branch_name"),
				"branch_kind":      "feature",
				"base_branch":      "main",
				"write_scope_json": `{"paths":["app/**"]}`,
				"status":           "RESERVED",
			}})
		case 5:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
				RepoStatus:     "READY",
				RepoURL:        remote,
				CurrentPhase:   "IMPLEMENTATION",
				WriteScopeJSON: `{"paths":["app/**"]}`,
			}))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful inferred materialization, got %+v", result)
	}
	if !strings.Contains(result.Output, `"inferred_task_id": "task-impl"`) {
		t.Fatalf("expected output to expose inferred task id, got %q", result.Output)
	}
	if calls != 5 {
		t.Fatalf("expected 5 calls, got %d", calls)
	}
}

func TestProjectCheckoutMaterializeToolRejectsForeignActiveTaskBeforeClone(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:     "READY",
			RepoURL:        remote,
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

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"active_task_id": "task-old",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected stale active task to be rejected, got %+v", result)
	}
	for _, want := range []string{"stale active_task_id", "task-old", "project-subpixel"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if _, err := os.Stat(filepath.Join(workdir, "project-checkouts")); !os.IsNotExist(err) {
		t.Fatalf("expected no checkout directory after stale active task rejection, err=%v", err)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination call, got %d", calls)
	}
}

func TestProjectCheckoutMaterializeToolRequiresRuntimeClaimBeforeCloneWhenBound(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:     "READY",
			RepoURL:        remote,
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

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"active_task_id": "task-current",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected missing runtime claim binding to be rejected, got %+v", result)
	}
	for _, want := range []string{"active claimed runtime task", "model.ask", "claim the relevant project task"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if _, err := os.Stat(filepath.Join(workdir, "project-checkouts")); !os.IsNotExist(err) {
		t.Fatalf("expected no checkout directory after missing runtime claim rejection, err=%v", err)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination call, got %d", calls)
	}
}

func TestProjectCheckoutMaterializeToolRejectsStrategyBranchBeforeCloneWhenBound(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:     "READY",
			RepoURL:        remote,
			CurrentPhase:   "SPEC",
			WriteScopeJSON: `{"paths":["**"]}`,
			Tasks: []map[string]any{{
				"task_id":      "task-root",
				"status":       "RUNNING",
				"task_kind":    "COORDINATION",
				"project_lane": "strategy",
			}},
		}))
	}))
	defer server.Close()

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-root", ProjectID: "project-subpixel"}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"active_task_id": "task-root",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected strategy branch checkout to be rejected, got %+v", result)
	}
	for _, want := range []string{"not implementation-shaped", "lane=strategy", "claimed implementation task"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if _, err := os.Stat(filepath.Join(workdir, "project-checkouts")); !os.IsNotExist(err) {
		t.Fatalf("expected no checkout directory after strategy branch rejection, err=%v", err)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination call, got %d", calls)
	}
}

func TestProjectCheckoutMaterializeToolRejectsPathOutsideWorkdirBeforeClone(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:     "READY",
			RepoURL:        remote,
			CurrentPhase:   "IMPLEMENTATION",
			WriteScopeJSON: `{"paths":["web/**"]}`,
		}))
	}))
	defer server.Close()

	tool := NewProjectCheckoutMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"local_path": filepath.Join(t.TempDir(), "outside"),
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "outside agent workdir") {
		t.Fatalf("expected outside workdir error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination lookup, got %d", calls)
	}
}

func seedBareGitRepo(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	source := filepath.Join(root, "source")
	bare := filepath.Join(root, "remote.git")
	runGitNoDir(t, "init", "-b", "main", source)
	runGit(t, source, "config", "user.name", "Rhizome Test")
	runGit(t, source, "config", "user.email", "rhizome-test@example.invalid")
	if err := os.WriteFile(filepath.Join(source, "README.md"), []byte("# Subpixel Lab\n"), 0o644); err != nil {
		t.Fatalf("write README: %v", err)
	}
	runGit(t, source, "add", "README.md")
	runGit(t, source, "commit", "-m", "Initial seed")
	runGitNoDir(t, "clone", "--bare", source, bare)
	return bare
}

func seedBareGitRepoWithBranch(t *testing.T, branchName, fileName, contents string) string {
	t.Helper()
	remote := seedBareGitRepo(t)
	source := filepath.Join(t.TempDir(), "branch-source")
	runGitNoDir(t, "clone", remote, source)
	runGit(t, source, "config", "user.name", "Rhizome Test")
	runGit(t, source, "config", "user.email", "rhizome-test@example.invalid")
	runGit(t, source, "checkout", "-b", branchName)
	if err := os.MkdirAll(filepath.Dir(filepath.Join(source, fileName)), 0o755); err != nil {
		t.Fatalf("mkdir candidate parent: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, fileName), []byte(contents), 0o644); err != nil {
		t.Fatalf("write candidate file: %v", err)
	}
	runGit(t, source, "add", fileName)
	runGit(t, source, "commit", "-m", "Add candidate branch file")
	runGit(t, source, "push", "origin", branchName)
	return remote
}

func runGitNoDir(t *testing.T, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", args...)
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		t.Fatalf("git %v timed out after 30s\n%s", args, string(out))
	}
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, string(out))
	}
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		t.Fatalf("git -C %s %v timed out after 30s\n%s", dir, args, string(out))
	}
	if err != nil {
		t.Fatalf("git -C %s %v failed: %v\n%s", dir, args, err, string(out))
	}
}

func gitOutput(t *testing.T, dir string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		t.Fatalf("git -C %s %v timed out after 30s\n%s", dir, args, string(out))
	}
	if err != nil {
		t.Fatalf("git -C %s %v failed: %v\n%s", dir, args, err, string(out))
	}
	return strings.TrimSpace(string(out))
}
