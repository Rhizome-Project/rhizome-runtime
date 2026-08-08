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

func assertSideEffectClassificationTaskSubmit(t *testing.T, req rpcRequest, wantDirtyPath string) {
	t.Helper()
	if req.Method != "task.submit" {
		t.Fatalf("unexpected classification task method %q", req.Method)
	}
	if got := rpcString(req.Params, "task_class"); got != "INCIDENT" {
		t.Fatalf("expected universal INCIDENT task class, got %q params=%+v", got, req.Params)
	}
	if got := rpcString(req.Params, "task_class_source"); got != "EXPLICIT" {
		t.Fatalf("expected explicit task class source, got %q params=%+v", got, req.Params)
	}
	if got := rpcString(req.Params, "project_lane"); got != "coordination" {
		t.Fatalf("expected coordination lane for classification task, got %q", got)
	}
	description := rpcString(req.Params, "description")
	for _, want := range []string{"Classify the side effects", wantDirtyPath, "split_tension", "expand_boundary"} {
		if !strings.Contains(description, want) {
			t.Fatalf("expected classification task description to contain %q, got %s", want, description)
		}
	}
	requirements, _ := req.Params["task_requirements"].(map[string]any)
	if requirements["schema"] != "artifact_bound_side_effect_classification_task.v1" {
		t.Fatalf("expected classification task requirements schema, got %+v", requirements)
	}
	if requirements["abpc_task_class"] != "side_effect_classification" {
		t.Fatalf("expected ABPC task class in requirements, got %+v", requirements)
	}
}

func TestProjectBranchCommitToolCommitsRegistersAndPushes(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "project-subpixel", "subpixel-lab")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-B", "agent/agent-alpha/project-subpixel/task-build")
	if err := os.MkdirAll(filepath.Join(checkoutPath, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "web", "app.js"), []byte("export const ready = true;\n"), 0o644); err != nil {
		t.Fatalf("write app.js: %v", err)
	}

	calls := 0
	var checkoutParams map[string]any
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
				WriteScopeJSON: `{"paths":["web/**"]}`,
				CheckoutPath:   checkoutPath,
			}))
		case 2:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			checkoutParams = req.Params
			if got := rpcString(req.Params, "dirty_state"); got != "clean" {
				t.Fatalf("dirty_state = %q", got)
			}
			if got := rpcString(req.Params, "head_sha"); got == "" {
				t.Fatalf("head_sha missing from checkout params: %+v", req.Params)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-1",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"machine_id":    rpcString(req.Params, "machine_id"),
				"machine_label": rpcString(req.Params, "machine_label"),
				"owner_user_id": rpcString(req.Params, "owner_user_id"),
				"agent_id":      "agent-alpha",
				"local_path":    checkoutPath,
				"checkout_kind": "clone",
				"branch_name":   rpcString(req.Params, "branch_name"),
				"base_branch":   rpcString(req.Params, "base_branch"),
				"head_sha":      rpcString(req.Params, "head_sha"),
				"base_sha":      rpcString(req.Params, "base_sha"),
				"dirty_state":   "clean",
				"status":        "ACTIVE",
			}})
		case 3:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			branchParams = req.Params
			if got := rpcString(req.Params, "status"); got != "ACTIVE" {
				t.Fatalf("status = %q", got)
			}
			if got := rpcString(req.Params, "head_sha"); got == "" || got == rpcString(req.Params, "base_sha") {
				t.Fatalf("expected non-empty changed head/base, got %+v", req.Params)
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
				"base_branch":      rpcString(req.Params, "base_branch"),
				"head_sha":         rpcString(req.Params, "head_sha"),
				"base_sha":         rpcString(req.Params, "base_sha"),
				"write_scope_json": `{"paths":["web/**"]}`,
				"status":           "ACTIVE",
			}})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchCommitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"branch_id":      "branch-1",
		"commit_message": "Implement web slice",
		"push":           true,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected commit success, got %+v", result)
	}
	for _, want := range []string{`"commit_created": true`, `"push_succeeded": true`, `"next_gate": "project_branch_review_ready"`, `"mandatory_next_tool": "project_branch_review_ready"`, `"project_checkout_materialize"`, "web/app.js"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if checkoutParams == nil || branchParams == nil {
		t.Fatalf("expected checkout and branch registration params")
	}
	if got := gitOutput(t, checkoutPath, "status", "--porcelain"); got != "" {
		t.Fatalf("expected clean checkout after commit, got %q", got)
	}
	if got := gitOutput(t, checkoutPath, "log", "-1", "--pretty=%s"); got != "Implement web slice" {
		t.Fatalf("unexpected commit subject %q", got)
	}
	remoteHead := strings.TrimSpace(gitOutputNoDir(t, "ls-remote", "--heads", remote, "agent/agent-alpha/project-subpixel/task-build"))
	if remoteHead == "" {
		t.Fatalf("expected pushed branch to exist in bare remote")
	}
	if calls != 3 {
		t.Fatalf("expected 3 RPC calls, got %d", calls)
	}
}

func TestProjectBranchCommitToolUsesActiveProjectWhenModelPassesWorkspaceID(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "project-subpixel", "subpixel-lab")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-B", "agent/agent-alpha/project-subpixel/task-build")
	if err := os.MkdirAll(filepath.Join(checkoutPath, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "web", "app.js"), []byte("export const activeProject = true;\n"), 0o644); err != nil {
		t.Fatalf("write app.js: %v", err)
	}

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			if got := rpcString(req.Params, "project_id"); got != "project-subpixel" {
				t.Fatalf("coordination project_id = %q, want active project-subpixel; params=%+v", got, req.Params)
			}
			writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
				AgentID:        "agent-alpha",
				RepoURL:        remote,
				BranchStatus:   "ACTIVE",
				WriteScopeJSON: `{"paths":["web/**"]}`,
				CheckoutPath:   checkoutPath,
			}))
		case 2:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcString(req.Params, "project_id"); got != "project-subpixel" {
				t.Fatalf("checkout project_id = %q, want project-subpixel", got)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-1",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"machine_id":    rpcString(req.Params, "machine_id"),
				"machine_label": rpcString(req.Params, "machine_label"),
				"owner_user_id": rpcString(req.Params, "owner_user_id"),
				"agent_id":      "agent-alpha",
				"local_path":    checkoutPath,
				"checkout_kind": "clone",
				"branch_name":   rpcString(req.Params, "branch_name"),
				"base_branch":   rpcString(req.Params, "base_branch"),
				"head_sha":      rpcString(req.Params, "head_sha"),
				"base_sha":      rpcString(req.Params, "base_sha"),
				"dirty_state":   "clean",
				"status":        "ACTIVE",
			}})
		case 3:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			if got := rpcString(req.Params, "project_id"); got != "project-subpixel" {
				t.Fatalf("branch project_id = %q, want project-subpixel", got)
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
				"base_branch":      rpcString(req.Params, "base_branch"),
				"head_sha":         rpcString(req.Params, "head_sha"),
				"base_sha":         rpcString(req.Params, "base_sha"),
				"write_scope_json": `{"paths":["web/**"]}`,
				"status":           "ACTIVE",
			}})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchCommitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{TaskID: "task-build", ProjectID: "project-subpixel", SessionID: "session-1"}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "ws",
		"branch_id":      "branch-1",
		"commit_message": "Implement active project slice",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected active project correction success, got %+v", result)
	}
	if !strings.Contains(result.Output, `"project_id": "project-subpixel"`) {
		t.Fatalf("expected output to report active project, got %q", result.Output)
	}
	if calls != 3 {
		t.Fatalf("expected 3 RPC calls, got %d", calls)
	}
}

func TestProjectBranchCommitResolveBaseSHAPrefersOriginOverStaleLocalMain(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	checkoutPath := filepath.Join(t.TempDir(), "checkout")
	runGitNoDir(t, "clone", remote, checkoutPath)
	staleMain := gitOutput(t, checkoutPath, "rev-parse", "main")

	source := filepath.Join(t.TempDir(), "source")
	runGitNoDir(t, "clone", remote, source)
	runGit(t, source, "config", "user.name", "Rhizome Test")
	runGit(t, source, "config", "user.email", "rhizome-test@example.invalid")
	if err := os.WriteFile(filepath.Join(source, "README.md"), []byte("# Subpixel Lab\n\nIntegrated change.\n"), 0o644); err != nil {
		t.Fatalf("update README: %v", err)
	}
	runGit(t, source, "add", "README.md")
	runGit(t, source, "commit", "-m", "Advance integrated main")
	runGit(t, source, "push", "origin", "main")
	runGit(t, checkoutPath, "fetch", "origin")

	want := gitOutput(t, checkoutPath, "rev-parse", "origin/main")
	if want == staleMain {
		t.Fatalf("test setup failed: origin/main did not advance")
	}
	if got := projectBranchCommitResolveBaseSHA(context.Background(), checkoutPath, "main"); got != want {
		t.Fatalf("base SHA = %s, want current origin/main %s (stale local main was %s)", got, want, staleMain)
	}
}

func TestProjectBranchCommitToolBranchMismatchCreatesReconciliationTask(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "project-subpixel", "subpixel-lab")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-B", "agent/agent-alpha/project-subpixel/old-dirty")
	if err := os.MkdirAll(filepath.Join(checkoutPath, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "web", "app.js"), []byte("export const drift = true;\n"), 0o644); err != nil {
		t.Fatalf("write app.js: %v", err)
	}

	calls := 0
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
				WriteScopeJSON: `{"paths":["web/**"]}`,
				CheckoutPath:   checkoutPath,
			}))
		case 2:
			if req.Method != "agent.update.post" {
				t.Fatalf("expected reconciliation update, got %q", req.Method)
			}
			payload := rpcString(req.Params, "payload_json")
			for _, want := range []string{"evidence_reconciliation", "pre_commit_branch_mismatch", "checkout_identity_unresolved", "old-dirty"} {
				if !strings.Contains(payload, want) {
					t.Fatalf("expected update payload to contain %q, got %s", want, payload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		case 3:
			if req.Method != "task.submit" {
				t.Fatalf("expected reconciliation task submit, got %q", req.Method)
			}
			requirements, _ := req.Params["task_requirements"].(map[string]any)
			if requirements["schema"] != "project_branch_commit_evidence_reconciliation_task.v1" {
				t.Fatalf("expected evidence reconciliation schema, got %+v", requirements)
			}
			if requirements["stage"] != "pre_commit_branch_mismatch" || requirements["commit_created"] != false {
				t.Fatalf("expected pre-commit mismatch requirements, got %+v", requirements)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id": "task-evidence-reconcile-branch-mismatch",
				"status":  "PENDING",
			})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchCommitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected reconciliation block, got %+v", result)
	}
	for _, want := range []string{`"gate_type": "evidence_reconciliation"`, `"stage": "pre_commit_branch_mismatch"`, `"commit_created": false`, `"reconciliation_task_id": "task-evidence-reconcile-branch-mismatch"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if calls != 3 {
		t.Fatalf("expected coordination, update, task submit, got %d", calls)
	}
}

func TestProjectBranchCommitToolPushCleanStaleBranchCreatesReconciliationTask(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "project-subpixel", "subpixel-lab")
	branchName := "agent/agent-alpha/project-subpixel/task-build"
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-B", branchName)
	runGit(t, checkoutPath, "config", "user.name", "Rhizome Test")
	runGit(t, checkoutPath, "config", "user.email", "rhizome-test@example.invalid")
	if err := os.MkdirAll(filepath.Join(checkoutPath, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "web", "app.js"), []byte("export const stale = true;\n"), 0o644); err != nil {
		t.Fatalf("write app.js: %v", err)
	}
	runGit(t, checkoutPath, "add", "web/app.js")
	runGit(t, checkoutPath, "commit", "-m", "Local stale branch work")

	source := filepath.Join(t.TempDir(), "source")
	runGitNoDir(t, "clone", remote, source)
	runGit(t, source, "config", "user.name", "Rhizome Test")
	runGit(t, source, "config", "user.email", "rhizome-test@example.invalid")
	if err := os.WriteFile(filepath.Join(source, "README.md"), []byte("# Subpixel Lab\n\nIntegrated change.\n"), 0o644); err != nil {
		t.Fatalf("advance README: %v", err)
	}
	runGit(t, source, "add", "README.md")
	runGit(t, source, "commit", "-m", "Advance integrated main")
	runGit(t, source, "push", "origin", "main")
	runGit(t, checkoutPath, "fetch", "origin")

	calls := 0
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
				BranchName:     branchName,
				BranchStatus:   "ACTIVE",
				WriteScopeJSON: `{"paths":["web/**"]}`,
				CheckoutPath:   checkoutPath,
			}))
		case 2:
			if req.Method != "agent.update.post" {
				t.Fatalf("expected reconciliation update, got %q", req.Method)
			}
			payload := rpcString(req.Params, "payload_json")
			for _, want := range []string{"evidence_reconciliation", "pre_push_stale_branch", "checkout_identity_unresolved"} {
				if !strings.Contains(payload, want) {
					t.Fatalf("expected update payload to contain %q, got %s", want, payload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		case 3:
			if req.Method != "task.submit" {
				t.Fatalf("expected reconciliation task submit, got %q", req.Method)
			}
			requirements, _ := req.Params["task_requirements"].(map[string]any)
			if requirements["stage"] != "pre_push_stale_branch" || requirements["commit_created"] != false {
				t.Fatalf("expected stale push requirements, got %+v", requirements)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id": "task-evidence-reconcile-stale-push",
				"status":  "PENDING",
			})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchCommitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
		"push":       true,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected stale push reconciliation block, got %+v", result)
	}
	for _, want := range []string{`"gate_type": "evidence_reconciliation"`, `"stage": "pre_push_stale_branch"`, `"commit_created": false`, `"reconciliation_task_id": "task-evidence-reconcile-stale-push"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if calls != 3 {
		t.Fatalf("expected coordination, update, task submit, got %d", calls)
	}
}

func TestProjectBranchCommitToolAdoptsActiveSamePathCheckoutBeforeEvidenceRegistration(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "project-subpixel", "subpixel-lab")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-B", "agent/agent-alpha/project-subpixel/task-revision")
	if err := os.MkdirAll(filepath.Join(checkoutPath, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "web", "app.js"), []byte("export const revision = true;\n"), 0o644); err != nil {
		t.Fatalf("write app.js: %v", err)
	}

	coordination := branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		RepoURL:        remote,
		BranchStatus:   "ACTIVE",
		WriteScopeJSON: `{"paths":["web/**"]}`,
		CheckoutPath:   checkoutPath,
	})
	rawCoordination := coordination["coordination"].(map[string]any)
	rawCoordination["checkouts"] = []map[string]any{
		{
			"checkout_id":  "checkout-old-active-path",
			"workspace_id": "ws",
			"project_id":   "project-subpixel",
			"repo_id":      "projrepo-1",
			"agent_id":     "agent-alpha",
			"machine_id":   runtimeMachineID(),
			"local_path":   filepath.Join("project-checkouts", "project-subpixel", "subpixel-lab"),
			"status":       "ACTIVE",
		},
		{
			"checkout_id":  "checkout-stale-branch-ref",
			"workspace_id": "ws",
			"project_id":   "project-subpixel",
			"repo_id":      "projrepo-1",
			"agent_id":     "agent-alpha",
			"machine_id":   runtimeMachineID(),
			"local_path":   checkoutPath,
			"status":       "ACTIVE",
		},
	}
	rawCoordination["branches"].([]map[string]any)[0]["checkout_id"] = "checkout-stale-branch-ref"
	rawCoordination["branches"].([]map[string]any)[0]["branch_name"] = "agent/agent-alpha/project-subpixel/task-revision"

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, coordination)
		case 2:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcString(req.Params, "checkout_id"); got != "checkout-old-active-path" {
				t.Fatalf("expected commit tool to adopt active same-path checkout id, got %q params=%+v", got, req.Params)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-old-active-path",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"agent_id":      "agent-alpha",
				"local_path":    checkoutPath,
				"checkout_kind": "clone",
				"branch_name":   rpcString(req.Params, "branch_name"),
				"head_sha":      rpcString(req.Params, "head_sha"),
				"base_sha":      rpcString(req.Params, "base_sha"),
				"dirty_state":   "clean",
				"status":        "ACTIVE",
			}})
		case 3:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			if got := rpcString(req.Params, "checkout_id"); got != "checkout-old-active-path" {
				t.Fatalf("expected branch evidence to point at adopted checkout id, got %q params=%+v", got, req.Params)
			}
			writeRPCResult(w, req, map[string]any{"branch": map[string]any{
				"branch_id":        "branch-1",
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      "checkout-old-active-path",
				"agent_id":         "agent-alpha",
				"branch_name":      rpcString(req.Params, "branch_name"),
				"branch_kind":      "feature",
				"base_branch":      rpcString(req.Params, "base_branch"),
				"head_sha":         rpcString(req.Params, "head_sha"),
				"base_sha":         rpcString(req.Params, "base_sha"),
				"write_scope_json": `{"paths":["web/**"]}`,
				"status":           "ACTIVE",
			}})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchCommitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"branch_id":      "branch-1",
		"commit_message": "Adopt active checkout identity",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected commit success through adopted checkout id, got %+v", result)
	}
	if !strings.Contains(result.Output, `"checkout_id": "checkout-old-active-path"`) {
		t.Fatalf("expected output to expose adopted checkout id, got %s", result.Output)
	}
	if calls != 3 {
		t.Fatalf("expected coordination, checkout register, branch register, got %d calls", calls)
	}
}

func TestProjectBranchCommitToolDoesNotAdoptSamePathCheckoutFromDifferentMachine(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "project-subpixel", "subpixel-lab")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-B", "agent/agent-alpha/project-subpixel/task-revision")
	if err := os.MkdirAll(filepath.Join(checkoutPath, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "web", "app.js"), []byte("export const revision = true;\n"), 0o644); err != nil {
		t.Fatalf("write app.js: %v", err)
	}

	coordination := branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		RepoURL:        remote,
		BranchStatus:   "ACTIVE",
		WriteScopeJSON: `{"paths":["web/**"]}`,
		CheckoutPath:   checkoutPath,
	})
	rawCoordination := coordination["coordination"].(map[string]any)
	rawCoordination["checkouts"] = []map[string]any{
		{
			"checkout_id":  "checkout-other-machine",
			"workspace_id": "ws",
			"project_id":   "project-subpixel",
			"repo_id":      "projrepo-1",
			"agent_id":     "agent-alpha",
			"machine_id":   "machine-other",
			"local_path":   filepath.Join("project-checkouts", "project-subpixel", "subpixel-lab"),
			"status":       "ACTIVE",
		},
		{
			"checkout_id":  "checkout-current-machine",
			"workspace_id": "ws",
			"project_id":   "project-subpixel",
			"repo_id":      "projrepo-1",
			"agent_id":     "agent-alpha",
			"machine_id":   runtimeMachineID(),
			"local_path":   checkoutPath,
			"status":       "ACTIVE",
		},
	}
	rawCoordination["branches"].([]map[string]any)[0]["checkout_id"] = "checkout-current-machine"
	rawCoordination["branches"].([]map[string]any)[0]["branch_name"] = "agent/agent-alpha/project-subpixel/task-revision"

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, coordination)
		case 2:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			if got := rpcString(req.Params, "checkout_id"); got != "checkout-current-machine" {
				t.Fatalf("expected current-machine branch checkout id to be preserved, got %q params=%+v", got, req.Params)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-current-machine",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"machine_id":    runtimeMachineID(),
				"agent_id":      "agent-alpha",
				"local_path":    checkoutPath,
				"checkout_kind": "clone",
				"branch_name":   rpcString(req.Params, "branch_name"),
				"head_sha":      rpcString(req.Params, "head_sha"),
				"base_sha":      rpcString(req.Params, "base_sha"),
				"dirty_state":   "clean",
				"status":        "ACTIVE",
			}})
		case 3:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			if got := rpcString(req.Params, "checkout_id"); got != "checkout-current-machine" {
				t.Fatalf("expected branch evidence to keep current-machine checkout id, got %q params=%+v", got, req.Params)
			}
			writeRPCResult(w, req, map[string]any{"branch": map[string]any{
				"branch_id":        "branch-1",
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      "checkout-current-machine",
				"agent_id":         "agent-alpha",
				"branch_name":      rpcString(req.Params, "branch_name"),
				"branch_kind":      "feature",
				"base_branch":      rpcString(req.Params, "base_branch"),
				"head_sha":         rpcString(req.Params, "head_sha"),
				"base_sha":         rpcString(req.Params, "base_sha"),
				"write_scope_json": `{"paths":["web/**"]}`,
				"status":           "ACTIVE",
			}})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchCommitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"branch_id":      "branch-1",
		"commit_message": "Preserve current machine checkout identity",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected commit success through current-machine checkout id, got %+v", result)
	}
	if strings.Contains(result.Output, "checkout-other-machine") {
		t.Fatalf("output should not mention adopted other-machine checkout, got %s", result.Output)
	}
}

func TestProjectBranchCommitToolBlocksForeignBranchCheckoutBeforeCommit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	relativeCheckout := filepath.Join("project-checkouts", "project-subpixel", "subpixel-lab")
	checkoutPath := filepath.Join(workdir, relativeCheckout)
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-B", "agent/agent-alpha/project-subpixel/task-revision")
	if err := os.MkdirAll(filepath.Join(checkoutPath, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "web", "app.js"), []byte("export const revision = true;\n"), 0o644); err != nil {
		t.Fatalf("write app.js: %v", err)
	}
	headBefore := strings.TrimSpace(gitOutput(t, checkoutPath, "rev-parse", "HEAD"))

	coordination := branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		RepoURL:        remote,
		BranchStatus:   "ACTIVE",
		WriteScopeJSON: `{"paths":["web/**"]}`,
		CheckoutPath:   checkoutPath,
	})
	rawCoordination := coordination["coordination"].(map[string]any)
	rawCoordination["checkouts"] = []map[string]any{{
		"checkout_id":     "checkout-foreign-machine",
		"workspace_id":    "ws",
		"project_id":      "project-subpixel",
		"repo_id":         "projrepo-1",
		"agent_id":        "agent-alpha",
		"machine_id":      "machine-other",
		"local_path":      relativeCheckout,
		"active_task_id":  "task-build",
		"active_claim_id": "task-build",
		"status":          "ACTIVE",
	}}
	rawCoordination["branches"].([]map[string]any)[0]["checkout_id"] = "checkout-foreign-machine"
	rawCoordination["branches"].([]map[string]any)[0]["branch_name"] = "agent/agent-alpha/project-subpixel/task-revision"
	rawCoordination["branches"].([]map[string]any)[0]["active_task_id"] = "task-build"
	rawCoordination["branches"].([]map[string]any)[0]["active_claim_id"] = "task-build"

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		switch calls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first method %q", req.Method)
			}
			writeRPCResult(w, req, coordination)
		case 2:
			if req.Method != "agent.update.post" {
				t.Fatalf("unexpected reconciliation update method %q", req.Method)
			}
			payload := rpcString(req.Params, "payload_json")
			for _, want := range []string{"evidence_reconciliation", "pre_commit_checkout_ownership", "machine-other"} {
				if !strings.Contains(payload, want) {
					t.Fatalf("expected update payload to contain %q, got %s", want, payload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		case 3:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected reconciliation task method %q", req.Method)
			}
			requirements, _ := req.Params["task_requirements"].(map[string]any)
			if requirements["source_tool"] != "project_branch_commit" ||
				requirements["stage"] != "pre_commit_checkout_ownership" ||
				requirements["commit_created"] != false {
				t.Fatalf("expected pre-commit ownership reconciliation requirements, got %+v", requirements)
			}
			writeRPCResult(w, req, map[string]any{
				"task_id": "task-evidence-reconcile-foreign-machine",
				"status":  "PENDING",
			})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchCommitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"branch_id":      "branch-1",
		"commit_message": "Should not commit across foreign checkout evidence",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected foreign-machine checkout evidence to block before commit, got %+v", result)
	}
	if !strings.Contains(result.Output, "machine-other") ||
		!strings.Contains(result.Output, `"gate_type": "evidence_reconciliation"`) ||
		!strings.Contains(result.Output, `"stage": "pre_commit_checkout_ownership"`) ||
		!strings.Contains(result.Output, `"reconciliation_task_id": "task-evidence-reconcile-foreign-machine"`) {
		t.Fatalf("expected typed foreign machine pre-commit reconciliation, got %s", result.Output)
	}
	if got := strings.TrimSpace(gitOutput(t, checkoutPath, "rev-parse", "HEAD")); got != headBefore {
		t.Fatalf("HEAD changed despite pre-commit block: got %s want %s", got, headBefore)
	}
	if got := gitOutput(t, checkoutPath, "status", "--short"); !strings.Contains(got, "web/") {
		t.Fatalf("expected dirty implementation file to remain unstaged/uncommitted, got %q", got)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestProjectBranchCommitToolRecordsEvidenceReconciliationWhenPostCommitRegisterFails(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "project-subpixel", "subpixel-lab")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-B", "agent/agent-alpha/project-subpixel/task-build")
	if err := os.MkdirAll(filepath.Join(checkoutPath, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "web", "app.js"), []byte("export const orphaned = true;\n"), 0o644); err != nil {
		t.Fatalf("write app.js: %v", err)
	}

	calls := 0
	var committedHead string
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
				WriteScopeJSON: `{"paths":["web/**"]}`,
				CheckoutPath:   checkoutPath,
			}))
		case 2:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			committedHead = rpcString(req.Params, "head_sha")
			if committedHead == "" {
				t.Fatalf("expected committed head in checkout evidence params: %+v", req.Params)
			}
			writeRPCError(w, req, -32602, "project checkout scope mismatch: active path belongs to checkout=checkout-old")
		case 3:
			if req.Method != "agent.update.post" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			payload := rpcString(req.Params, "payload_json")
			for _, want := range []string{"project_branch_commit_mutation_attempt.v1", "NEEDS_RECONCILIATION", "committed_unregistered", committedHead, "checkout_register"} {
				if !strings.Contains(payload, want) {
					t.Fatalf("expected mutation-attempt update to contain %q, got %s", want, payload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		case 4:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected fourth method %q", req.Method)
			}
			if got := rpcString(req.Params, "task_class"); got != "INCIDENT" {
				t.Fatalf("expected valid INCIDENT task_class for reconciliation task, got %q params=%+v", got, req.Params)
			}
			description := rpcString(req.Params, "description")
			for _, want := range []string{"failed durable checkout/branch evidence registration", committedHead, "Do not retry git commit"} {
				if !strings.Contains(description, want) {
					t.Fatalf("expected reconciliation task description to contain %q, got %s", want, description)
				}
			}
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-evidence-reconcile-orphaned",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchCommitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"branch_id":      "branch-1",
		"commit_message": "Create orphaned local commit",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected evidence reconciliation block, got %+v", result)
	}
	for _, want := range []string{"evidence_reconciliation", "NEEDS_RECONCILIATION", "committed_unregistered", committedHead, `"commit_created": true`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected reconciliation output to contain %q, got %s", want, result.Output)
		}
	}
	if got := gitOutput(t, checkoutPath, "log", "-1", "--pretty=%s"); got != "Create orphaned local commit" {
		t.Fatalf("expected local commit to be preserved for adoption, got subject %q", got)
	}
	if got := gitOutput(t, checkoutPath, "status", "--porcelain"); got != "" {
		t.Fatalf("expected clean checkout after preserved commit, got %q", got)
	}
	if calls != 4 {
		t.Fatalf("expected coordination, failed checkout register, update, and reconciliation task, got %d calls", calls)
	}
}

func TestProjectBranchCommitToolRecordsEvidenceReconciliationWhenPostCommitDirty(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "project-subpixel", "subpixel-lab")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-B", "agent/agent-alpha/project-subpixel/task-build")
	if err := os.MkdirAll(filepath.Join(checkoutPath, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "web", "app.js"), []byte("export const hookDirty = true;\n"), 0o644); err != nil {
		t.Fatalf("write app.js: %v", err)
	}
	hookPath := filepath.Join(checkoutPath, ".git", "hooks", "post-commit")
	hook := "#!/bin/sh\nmkdir -p web\nprintf 'dirty after commit\\n' > web/post-commit-dirty.txt\n"
	if err := os.WriteFile(hookPath, []byte(hook), 0o755); err != nil {
		t.Fatalf("write post-commit hook: %v", err)
	}
	if err := os.Chmod(hookPath, 0o755); err != nil {
		t.Fatalf("chmod post-commit hook: %v", err)
	}

	calls := 0
	var committedHead string
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
				WriteScopeJSON: `{"paths":["web/**"]}`,
				CheckoutPath:   checkoutPath,
			}))
		case 2:
			if req.Method != "agent.update.post" {
				t.Fatalf("expected post-commit dirty state to route to reconciliation before checkout register, got %q", req.Method)
			}
			payload := rpcString(req.Params, "payload_json")
			committedHead = strings.TrimSpace(gitOutput(t, checkoutPath, "rev-parse", "HEAD"))
			for _, want := range []string{"project_branch_commit_mutation_attempt.v1", "NEEDS_RECONCILIATION", "committed_unregistered", committedHead, "post_commit_dirty_state"} {
				if !strings.Contains(payload, want) {
					t.Fatalf("expected mutation-attempt update to contain %q, got %s", want, payload)
				}
			}
			writeRPCResult(w, req, map[string]any{"update_id": "update-commit-dirty"})
		case 3:
			if req.Method != "task.submit" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			desc := rpcString(req.Params, "description")
			if !strings.Contains(desc, committedHead) || !strings.Contains(desc, "Do not retry git commit") {
				t.Fatalf("expected reconciliation task to preserve dirty commit head and no-retry instruction, got %s", desc)
			}
			writeRPCResult(w, req, map[string]any{"task": map[string]any{"task_id": rpcString(req.Params, "task_id")}})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchCommitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"branch_id":      "branch-1",
		"commit_message": "Create post-commit dirty reconciliation",
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected reconciliation block after post-commit dirty state, got %+v", result)
	}
	for _, want := range []string{"NEEDS_RECONCILIATION", "post_commit_dirty_state", committedHead} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %s", want, result.Output)
		}
	}
	if got := gitOutput(t, checkoutPath, "status", "--short"); !strings.Contains(got, "post-commit-dirty.txt") {
		t.Fatalf("expected hook-created dirty file to remain as reconciliation evidence, got %q", got)
	}
}

func TestProjectBranchCommitPathParsingIgnoresGitDiagnostics(t *testing.T) {
	out := strings.Join([]string{
		"warning: in the working copy of 'README.md', LF will be replaced by CRLF the next time Git touches it",
		"README.md",
		"index.html",
		"hint: use --renormalize if needed",
		"",
	}, "\n")
	lines := gitOutputNonDiagnosticLines(out)
	want := []string{"README.md", "index.html"}
	if strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("diagnostic-filtered lines = %#v, want %#v", lines, want)
	}
}

func TestProjectBranchCommitTreatsBlockedPatchQueueDecisionAsRevisionTerminal(t *testing.T) {
	if !projectPatchQueueStateIsTerminal("BLOCKED") {
		t.Fatal("BLOCKED patch queue decisions should allow revision follow-up commits")
	}
}

func TestProjectBranchCommitScopeAllowsRootConfigGlobs(t *testing.T) {
	pathset, err := pathsetFromWriteScopeJSON(`{"paths":["index.html","package.json","src/app/**","src/ui/**","tsconfig*.json","vite.config.*"]}`)
	if err != nil {
		t.Fatalf("pathsetFromWriteScopeJSON: %v", err)
	}
	for _, candidate := range []string{
		"index.html",
		"package.json",
		"tsconfig.json",
		"tsconfig.app.json",
		"vite.config.ts",
		"src/app/main.tsx",
		"src/ui/styles.css",
	} {
		if !pathsetIsCoveredByScope([]string{candidate}, pathset) {
			t.Fatalf("expected write scope to cover %s in %#v", candidate, pathset)
		}
	}
	for _, candidate := range []string{"src/main.tsx", "src/tsconfig.json", "dist/assets/index.js", "node_modules/vite/index.js"} {
		if pathsetIsCoveredByScope([]string{candidate}, pathset) {
			t.Fatalf("expected write scope not to cover %s in %#v", candidate, pathset)
		}
	}
	if !projectPatchQueuePathsetHasScoped([]string{"tsconfig*.json", "vite.config.*"}) {
		t.Fatalf("root globs should count as scoped pathsets for patch queue binding")
	}
}

func TestProjectBranchCommitEffectiveScopeDerivesFrontendSidecars(t *testing.T) {
	pathset, err := pathsetFromWriteScopeJSON(`{"paths":["package*.json","tsconfig*.json","vite.config.*","index.html","src/**"]}`)
	if err != nil {
		t.Fatalf("pathsetFromWriteScopeJSON: %v", err)
	}
	effective, derived := projectBranchCommitEffectiveWriteScope(pathset, []string{".gitignore", "README.md"})
	if len(derived) != 2 {
		t.Fatalf("expected derived sidecar scope entries, got %#v", derived)
	}
	for _, candidate := range []string{".gitignore", "README.md"} {
		if !pathsetIsCoveredByScope([]string{candidate}, effective) {
			t.Fatalf("expected frontend scaffold effective scope to cover %s in %#v", candidate, effective)
		}
	}

	narrow, err := pathsetFromWriteScopeJSON(`{"paths":["src/**"]}`)
	if err != nil {
		t.Fatalf("pathsetFromWriteScopeJSON narrow: %v", err)
	}
	narrowEffective, narrowDerived := projectBranchCommitEffectiveWriteScope(narrow, []string{".gitignore"})
	if len(narrowDerived) != 0 {
		t.Fatalf("narrow implementation scope must not derive sidecar ownership: %#v", narrowDerived)
	}
	if pathsetIsCoveredByScope([]string{".gitignore"}, narrowEffective) {
		t.Fatalf("narrow implementation scope must not derive .gitignore ownership: %#v", narrowEffective)
	}
}

func TestProjectBranchCommitEffectiveScopeDerivesGoFoundationSidecars(t *testing.T) {
	pathset, err := pathsetFromWriteScopeJSON(`{"paths":["cmd/**","internal/cli/**","internal/repl/**"]}`)
	if err != nil {
		t.Fatalf("pathsetFromWriteScopeJSON: %v", err)
	}
	effective, derived := projectBranchCommitEffectiveWriteScope(pathset, []string{"go.mod", "README.md", "main.go"})
	for _, want := range []string{"go.mod", "README.md", "main.go"} {
		if !stringSliceContains(derived, want) || !pathsetIsCoveredByScope([]string{want}, effective) {
			t.Fatalf("expected Go scaffold scope to derive %s, derived=%#v effective=%#v", want, derived, effective)
		}
	}

	feature, err := pathsetFromWriteScopeJSON(`{"paths":["internal/parser/**","internal/lexer/**"]}`)
	if err != nil {
		t.Fatalf("feature pathset parse: %v", err)
	}
	featureEffective, featureDerived := projectBranchCommitEffectiveWriteScope(feature, []string{"go.mod", "README.md", "main.go"})
	if !stringSliceContains(featureDerived, "go.mod") || len(featureDerived) != 1 {
		t.Fatalf("feature lane should derive only Go module sidecars, got %#v", featureDerived)
	}
	if !pathsetIsCoveredByScope([]string{"go.mod"}, featureEffective) {
		t.Fatalf("feature lane should cover go.mod via derived module sidecar scope: %#v", featureEffective)
	}
	for _, forbidden := range []string{"README.md", "main.go"} {
		if pathsetIsCoveredByScope([]string{forbidden}, featureEffective) {
			t.Fatalf("feature lane must not cover %s via derived scope: %#v", forbidden, featureEffective)
		}
	}
}

func TestProjectBranchCommitToolDerivesGitignoreForOwnedFrontendScaffold(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "project-subpixel", "vite-shell")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-B", "agent/agent-alpha/project-subpixel/task-build")
	files := map[string]string{
		".gitignore":                     "node_modules/\ndist/\n*.tsbuildinfo\n",
		"package.json":                   `{"scripts":{"build":"vite build"}}` + "\n",
		"tsconfig.json":                  `{"compilerOptions":{"strict":true}}` + "\n",
		"vite.config.ts":                 "export default {}\n",
		"index.html":                     "<div id=\"root\"></div>\n",
		filepath.Join("src", "main.tsx"): "console.log('ready')\n",
	}
	for rel, body := range files {
		full := filepath.Join(checkoutPath, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", rel, err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}

	calls := 0
	var checkoutParams map[string]any
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
				WriteScopeJSON: `{"paths":["package*.json","tsconfig*.json","vite.config.*","index.html","src/**"]}`,
				CheckoutPath:   checkoutPath,
			}))
		case 2:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			checkoutParams = req.Params
			if got := rpcString(req.Params, "dirty_state"); got != "clean" {
				t.Fatalf("dirty_state = %q", got)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-1",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"machine_id":    rpcString(req.Params, "machine_id"),
				"machine_label": rpcString(req.Params, "machine_label"),
				"owner_user_id": rpcString(req.Params, "owner_user_id"),
				"agent_id":      "agent-alpha",
				"local_path":    checkoutPath,
				"checkout_kind": "clone",
				"branch_name":   rpcString(req.Params, "branch_name"),
				"base_branch":   rpcString(req.Params, "base_branch"),
				"head_sha":      rpcString(req.Params, "head_sha"),
				"base_sha":      rpcString(req.Params, "base_sha"),
				"dirty_state":   "clean",
				"status":        "ACTIVE",
			}})
		case 3:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			branchParams = req.Params
			if !strings.Contains(rpcString(req.Params, "write_scope_json"), ".gitignore") {
				t.Fatalf("expected derived write_scope_json to include .gitignore, got %+v", req.Params)
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
				"base_branch":      rpcString(req.Params, "base_branch"),
				"head_sha":         rpcString(req.Params, "head_sha"),
				"base_sha":         rpcString(req.Params, "base_sha"),
				"write_scope_json": rpcString(req.Params, "write_scope_json"),
				"status":           "ACTIVE",
			}})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchCommitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"branch_id":      "branch-1",
		"commit_message": "Scaffold Vite shell",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected scaffold sidecar commit to succeed, got %+v", result)
	}
	for _, want := range []string{`"commit_created": true`, `"next_gate": "project_branch_review_ready"`, `"derived_write_scope_paths"`, ".gitignore"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if checkoutParams == nil || branchParams == nil {
		t.Fatalf("expected checkout and branch registration params")
	}
	if got := gitOutput(t, checkoutPath, "status", "--porcelain"); got != "" {
		t.Fatalf("expected checkout to be clean after scaffold commit, got %q", got)
	}
	if got := gitOutput(t, checkoutPath, "log", "-1", "--pretty=%s"); got != "Scaffold Vite shell" {
		t.Fatalf("unexpected commit subject %q", got)
	}
	if calls != 3 {
		t.Fatalf("expected coordination read, checkout register, and branch register, got %d", calls)
	}
}

func TestProjectBranchCommitToolRetriesAfterBoundaryExpansion(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "project-subpixel", "expanded-boundary-retry")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-B", "agent/agent-alpha/project-subpixel/task-build")
	if err := os.MkdirAll(filepath.Join(checkoutPath, "src"), 0o755); err != nil {
		t.Fatalf("mkdir src: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "src", "editor.ts"), []byte("export const editor = true;\n"), 0o644); err != nil {
		t.Fatalf("write editor: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "package.json"), []byte(`{"scripts":{"test":"vitest"}}`+"\n"), 0o644); err != nil {
		t.Fatalf("write package: %v", err)
	}

	blockCalls := 0
	blockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		blockCalls++
		switch blockCalls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first block method %q", req.Method)
			}
			writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
				AgentID:        "agent-alpha",
				RepoURL:        remote,
				BranchStatus:   "ACTIVE",
				ActiveTaskID:   "task-editor",
				ActiveClaimID:  "task-editor",
				WriteScopeJSON: `{"paths":["src/**"]}`,
				CheckoutPath:   checkoutPath,
			}))
		case 2:
			if req.Method != "agent.task.hydrate" {
				t.Fatalf("unexpected second block method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{
				"bundle": map[string]any{
					"updates": []map[string]any{},
				},
			})
		case 3:
			if req.Method != "agent.update.post" {
				t.Fatalf("unexpected side-effect update method %q", req.Method)
			}
			payload := rpcString(req.Params, "payload_json")
			for _, want := range []string{"artifact_bound_side_effect.v1", "pending_classification", "package.json"} {
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
			t.Fatalf("unexpected extra block RPC call %d method=%s", blockCalls, req.Method)
		}
	}))
	blockTool := NewProjectBranchCommitTool(NewRhizomeClient(blockServer.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	blocked := blockTool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"branch_id":      "branch-1",
		"commit_message": "Try narrow commit",
	})
	blockServer.Close()
	if blocked == nil || !blocked.IsError {
		t.Fatalf("expected narrow commit to block, got %+v", blocked)
	}
	for _, want := range []string{"side_effect_classification", "pending_classification", "package.json", `"commit_created": false`} {
		if !strings.Contains(blocked.Output, want) {
			t.Fatalf("expected blocked output to contain %q, got %s", want, blocked.Output)
		}
	}
	if got := gitOutput(t, checkoutPath, "status", "--porcelain"); !strings.Contains(got, "package.json") || !strings.Contains(got, "src/") {
		t.Fatalf("expected checkout to remain dirty before expansion, got %q", got)
	}

	retryCalls := 0
	var branchParams map[string]any
	retryServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		retryCalls++
		switch retryCalls {
		case 1:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected first retry method %q", req.Method)
			}
			writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
				AgentID:        "agent-alpha",
				RepoURL:        remote,
				BranchStatus:   "ACTIVE",
				ActiveTaskID:   "task-editor",
				ActiveClaimID:  "task-editor",
				WriteScopeJSON: `{"paths":["src/**","package.json"]}`,
				CheckoutPath:   checkoutPath,
			}))
		case 2:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected second retry method %q", req.Method)
			}
			if got := rpcString(req.Params, "dirty_state"); got != "clean" {
				t.Fatalf("retry checkout dirty_state = %q", got)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-1",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"machine_id":    rpcString(req.Params, "machine_id"),
				"machine_label": rpcString(req.Params, "machine_label"),
				"owner_user_id": rpcString(req.Params, "owner_user_id"),
				"agent_id":      "agent-alpha",
				"local_path":    checkoutPath,
				"checkout_kind": "clone",
				"branch_name":   rpcString(req.Params, "branch_name"),
				"base_branch":   rpcString(req.Params, "base_branch"),
				"head_sha":      rpcString(req.Params, "head_sha"),
				"base_sha":      rpcString(req.Params, "base_sha"),
				"dirty_state":   "clean",
				"status":        "ACTIVE",
			}})
		case 3:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected third retry method %q", req.Method)
			}
			branchParams = req.Params
			writeRPCResult(w, req, map[string]any{"branch": map[string]any{
				"branch_id":        "branch-1",
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      "checkout-1",
				"agent_id":         "agent-alpha",
				"branch_name":      rpcString(req.Params, "branch_name"),
				"branch_kind":      "feature",
				"base_branch":      rpcString(req.Params, "base_branch"),
				"head_sha":         rpcString(req.Params, "head_sha"),
				"base_sha":         rpcString(req.Params, "base_sha"),
				"write_scope_json": `{"paths":["src/**","package.json"]}`,
				"status":           "ACTIVE",
			}})
		default:
			t.Fatalf("unexpected extra retry RPC call %d method=%s", retryCalls, req.Method)
		}
	}))
	defer retryServer.Close()

	retryTool := NewProjectBranchCommitTool(NewRhizomeClient(retryServer.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	retried := retryTool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"branch_id":      "branch-1",
		"commit_message": "Commit after boundary expansion",
	})
	if retried == nil || retried.IsError {
		t.Fatalf("expected expanded-boundary retry to commit, got %+v", retried)
	}
	for _, want := range []string{`"commit_created": true`, `"next_gate": "project_branch_review_ready"`, "package.json", "src/editor.ts"} {
		if !strings.Contains(retried.Output, want) {
			t.Fatalf("expected retry output to contain %q, got %s", want, retried.Output)
		}
	}
	if branchParams == nil {
		t.Fatalf("expected branch register after retry")
	}
	if got := rpcString(branchParams, "write_scope_json"); !strings.Contains(got, "package.json") {
		t.Fatalf("expected expanded scope to be registered on retry, got %q", got)
	}
	if got := gitOutput(t, checkoutPath, "status", "--porcelain"); got != "" {
		t.Fatalf("expected clean checkout after expanded retry, got %q", got)
	}
	if got := gitOutput(t, checkoutPath, "log", "-1", "--pretty=%s"); got != "Commit after boundary expansion" {
		t.Fatalf("unexpected retry commit subject %q", got)
	}
	if blockCalls != 4 || retryCalls != 3 {
		t.Fatalf("expected 4 block calls and 3 retry calls, got %d/%d", blockCalls, retryCalls)
	}
}

func TestProjectBranchCommitToolIgnoresGeneratedPythonCache(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "project-subpixel", "signal-board")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-B", "agent/agent-alpha/project-subpixel/task-build")
	if err := os.MkdirAll(filepath.Join(checkoutPath, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "web", "app.py"), []byte("print('ready')\n"), 0o644); err != nil {
		t.Fatalf("write app.py: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(checkoutPath, "__pycache__"), 0o755); err != nil {
		t.Fatalf("mkdir __pycache__: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "__pycache__", "app.cpython-312.pyc"), []byte("cache"), 0o644); err != nil {
		t.Fatalf("write pycache: %v", err)
	}

	calls := 0
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
				WriteScopeJSON: `{"paths":["web/**"]}`,
				CheckoutPath:   checkoutPath,
			}))
		case 2:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-1",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"machine_id":    rpcString(req.Params, "machine_id"),
				"machine_label": rpcString(req.Params, "machine_label"),
				"owner_user_id": rpcString(req.Params, "owner_user_id"),
				"agent_id":      "agent-alpha",
				"local_path":    checkoutPath,
				"checkout_kind": "clone",
				"branch_name":   rpcString(req.Params, "branch_name"),
				"base_branch":   rpcString(req.Params, "base_branch"),
				"head_sha":      rpcString(req.Params, "head_sha"),
				"base_sha":      rpcString(req.Params, "base_sha"),
				"dirty_state":   "clean",
				"status":        "ACTIVE",
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
				"branch_name":      rpcString(req.Params, "branch_name"),
				"branch_kind":      "feature",
				"base_branch":      rpcString(req.Params, "base_branch"),
				"head_sha":         rpcString(req.Params, "head_sha"),
				"base_sha":         rpcString(req.Params, "base_sha"),
				"write_scope_json": `{"paths":["web/**"]}`,
				"status":           "ACTIVE",
			}})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchCommitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"branch_id":      "branch-1",
		"commit_message": "Implement signal board",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected commit success with ignored cache, got %+v", result)
	}
	if strings.Contains(result.Output, "__pycache__") {
		t.Fatalf("generated cache must not be part of commit evidence, got %q", result.Output)
	}
	if got := gitOutput(t, checkoutPath, "status", "--porcelain"); got != "" {
		t.Fatalf("expected generated cache to be ignored after commit, got %q", got)
	}
	if calls != 3 {
		t.Fatalf("expected 3 RPC calls, got %d", calls)
	}
}

func TestProjectBranchCommitToolIgnoresNodeAndPowerShellCaches(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "project-subpixel", "node-slice")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-B", "agent/agent-alpha/project-subpixel/task-build")
	if err := os.MkdirAll(filepath.Join(checkoutPath, "src", "processing"), 0o755); err != nil {
		t.Fatalf("mkdir src/processing: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "src", "processing", "pipeline.ts"), []byte("export const ready = true;\n"), 0o644); err != nil {
		t.Fatalf("write pipeline.ts: %v", err)
	}
	generatedFiles := map[string]string{
		filepath.Join("node_modules", ".bin", "vitest"):                                  "generated bin",
		filepath.Join("node_modules", "vite", "index.js"):                                "generated dependency",
		filepath.Join("Microsoft", "Windows", "PowerShell", "ModuleAnalysisCache"):       "powershell cache",
		filepath.Join("Microsoft", "Windows", "PowerShell", "StartupProfileData-1.json"): "{}",
	}
	for rel, body := range generatedFiles {
		full := filepath.Join(checkoutPath, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir generated parent %s: %v", rel, err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatalf("write generated %s: %v", rel, err)
		}
	}

	calls := 0
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
				WriteScopeJSON: `{"paths":["src/processing/**"]}`,
				CheckoutPath:   checkoutPath,
			}))
		case 2:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-1",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"machine_id":    rpcString(req.Params, "machine_id"),
				"machine_label": rpcString(req.Params, "machine_label"),
				"owner_user_id": rpcString(req.Params, "owner_user_id"),
				"agent_id":      "agent-alpha",
				"local_path":    checkoutPath,
				"checkout_kind": "clone",
				"branch_name":   rpcString(req.Params, "branch_name"),
				"base_branch":   rpcString(req.Params, "base_branch"),
				"head_sha":      rpcString(req.Params, "head_sha"),
				"base_sha":      rpcString(req.Params, "base_sha"),
				"dirty_state":   "clean",
				"status":        "ACTIVE",
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
				"branch_name":      rpcString(req.Params, "branch_name"),
				"branch_kind":      "feature",
				"base_branch":      rpcString(req.Params, "base_branch"),
				"head_sha":         rpcString(req.Params, "head_sha"),
				"base_sha":         rpcString(req.Params, "base_sha"),
				"write_scope_json": `{"paths":["src/processing/**"]}`,
				"status":           "ACTIVE",
			}})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchCommitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"branch_id":      "branch-1",
		"commit_message": "Implement processing pipeline",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected commit success with ignored generated caches, got %+v", result)
	}
	for _, forbidden := range []string{"node_modules", "ModuleAnalysisCache", "StartupProfileData"} {
		if strings.Contains(result.Output, forbidden) {
			t.Fatalf("generated cache %q must not be part of commit evidence, got %q", forbidden, result.Output)
		}
	}
	if got := gitOutput(t, checkoutPath, "status", "--porcelain"); got != "" {
		t.Fatalf("expected generated caches to be ignored after commit, got %q", got)
	}
	if calls != 3 {
		t.Fatalf("expected 3 RPC calls, got %d", calls)
	}
}

func TestProjectBranchCommitToolIgnoresTypeScriptBuildInfo(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "project-subpixel", "node-slice")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-B", "agent/agent-alpha/project-subpixel/task-build")
	if err := os.MkdirAll(filepath.Join(checkoutPath, "src", "processing"), 0o755); err != nil {
		t.Fatalf("mkdir src/processing: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "src", "processing", "pipeline.ts"), []byte("export const ready = true;\n"), 0o644); err != nil {
		t.Fatalf("write pipeline.ts: %v", err)
	}
	for _, rel := range []string{
		"tsconfig.tsbuildinfo",
		"tsconfig.node.tsbuildinfo",
		filepath.Join("src", "processing", "tsconfig.tsbuildinfo"),
	} {
		full := filepath.Join(checkoutPath, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir generated parent %s: %v", rel, err)
		}
		if err := os.WriteFile(full, []byte("generated"), 0o644); err != nil {
			t.Fatalf("write generated %s: %v", rel, err)
		}
	}

	calls := 0
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
				WriteScopeJSON: `{"paths":["src/processing/**"]}`,
				CheckoutPath:   checkoutPath,
			}))
		case 2:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":  "checkout-1",
				"workspace_id": "ws",
				"project_id":   "project-subpixel",
				"repo_id":      "projrepo-1",
				"agent_id":     "agent-alpha",
				"local_path":   checkoutPath,
				"branch_name":  rpcString(req.Params, "branch_name"),
				"head_sha":     rpcString(req.Params, "head_sha"),
				"base_sha":     rpcString(req.Params, "base_sha"),
				"dirty_state":  "clean",
				"status":       "ACTIVE",
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
				"branch_name":      rpcString(req.Params, "branch_name"),
				"branch_kind":      "feature",
				"base_branch":      rpcString(req.Params, "base_branch"),
				"head_sha":         rpcString(req.Params, "head_sha"),
				"base_sha":         rpcString(req.Params, "base_sha"),
				"write_scope_json": `{"paths":["src/processing/**"]}`,
				"status":           "ACTIVE",
			}})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchCommitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"branch_id":      "branch-1",
		"commit_message": "Implement processing pipeline",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected commit success with ignored tsbuildinfo, got %+v", result)
	}
	if strings.Contains(result.Output, "tsbuildinfo") {
		t.Fatalf("tsbuildinfo must not be part of commit evidence, got %q", result.Output)
	}
	if got := gitOutput(t, checkoutPath, "status", "--porcelain"); got != "" {
		t.Fatalf("expected generated tsbuildinfo files to be ignored after commit, got %q", got)
	}
	if calls != 3 {
		t.Fatalf("expected 3 RPC calls, got %d", calls)
	}
}

func TestProjectBranchCommitToolRejectsTrackedTypeScriptBuildInfoDrift(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "project-subpixel", "node-slice")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-B", "agent/agent-alpha/project-subpixel/task-build")
	runGit(t, checkoutPath, "config", "user.name", "Rhizome Test")
	runGit(t, checkoutPath, "config", "user.email", "rhizome-test@example.invalid")
	if err := os.WriteFile(filepath.Join(checkoutPath, "tsconfig.tsbuildinfo"), []byte("tracked-v1"), 0o644); err != nil {
		t.Fatalf("write tracked tsbuildinfo: %v", err)
	}
	runGit(t, checkoutPath, "add", "tsconfig.tsbuildinfo")
	runGit(t, checkoutPath, "commit", "-m", "Track generated metadata fixture")
	if err := os.MkdirAll(filepath.Join(checkoutPath, "src", "processing"), 0o755); err != nil {
		t.Fatalf("mkdir src/processing: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "src", "processing", "pipeline.ts"), []byte("export const ready = true;\n"), 0o644); err != nil {
		t.Fatalf("write pipeline.ts: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "tsconfig.tsbuildinfo"), []byte("tracked-v2"), 0o644); err != nil {
		t.Fatalf("modify tracked tsbuildinfo: %v", err)
	}

	calls := 0
	var updateParams map[string]any
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
				WriteScopeJSON: `{"paths":["src/processing/**"]}`,
				CheckoutPath:   checkoutPath,
			}))
		case 2:
			if req.Method != "agent.update.post" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			updateParams = req.Params
			payload := rpcString(req.Params, "payload_json")
			for _, want := range []string{"artifact_bound_side_effect.v1", "out_of_boundary", "pending_classification", "tsconfig.tsbuildinfo"} {
				if !strings.Contains(payload, want) {
					t.Fatalf("expected side-effect payload to contain %q, got %s", want, payload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		case 3:
			assertSideEffectClassificationTaskSubmit(t, req, "tsconfig.tsbuildinfo")
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-side-effect-classify-tsbuildinfo",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchCommitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"branch_id":      "branch-1",
		"commit_message": "Implement processing pipeline",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "tsconfig.tsbuildinfo") || !strings.Contains(result.Output, "pending_classification") || !strings.Contains(result.Output, "side_effect_classification") {
		t.Fatalf("expected tracked tsbuildinfo drift to post pending side-effect evidence, got %+v", result)
	}
	if updateParams == nil {
		t.Fatalf("expected side-effect update params")
	}
	if got := gitOutput(t, checkoutPath, "log", "-1", "--pretty=%s"); got == "Implement processing pipeline" {
		t.Fatalf("commit should not be created before side-effect classification")
	}
	if calls != 3 {
		t.Fatalf("expected coordination read, side-effect update, and classification task, got %d", calls)
	}
}

func TestProjectBranchCommitToolDerivesRootScaffoldReadme(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "project-subpixel", "webapp")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-B", "agent/agent-alpha/project-subpixel/task-build")
	if err := os.MkdirAll(filepath.Join(checkoutPath, "src", "app"), 0o755); err != nil {
		t.Fatalf("mkdir src/app: %v", err)
	}
	files := map[string]string{
		"README.md":                            "# Subpixel Lab\n\nRun with npm run build.\n",
		"package.json":                         `{"scripts":{"build":"vite build"},"dependencies":{}}` + "\n",
		"index.html":                           `<div id="root"></div>` + "\n",
		filepath.Join("src", "app", "App.tsx"): "export function App() { return null }\n",
	}
	for rel, body := range files {
		full := filepath.Join(checkoutPath, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir parent %s: %v", rel, err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}

	const originalScope = `{"paths":["package.json","package-lock.json","tsconfig*.json","vite.config.*","index.html","public/**","src/app/**","src/main.*","src/styles/**"]}`
	calls := 0
	var checkoutParams map[string]any
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
				WriteScopeJSON: originalScope,
				CheckoutPath:   checkoutPath,
			}))
		case 2:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			checkoutParams = req.Params
			if got := rpcString(req.Params, "dirty_state"); got != "clean" {
				t.Fatalf("dirty_state = %q", got)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-1",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"machine_id":    rpcString(req.Params, "machine_id"),
				"machine_label": rpcString(req.Params, "machine_label"),
				"owner_user_id": rpcString(req.Params, "owner_user_id"),
				"agent_id":      "agent-alpha",
				"local_path":    checkoutPath,
				"checkout_kind": "clone",
				"branch_name":   rpcString(req.Params, "branch_name"),
				"base_branch":   rpcString(req.Params, "base_branch"),
				"head_sha":      rpcString(req.Params, "head_sha"),
				"base_sha":      rpcString(req.Params, "base_sha"),
				"dirty_state":   "clean",
				"status":        "ACTIVE",
			}})
		case 3:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			branchParams = req.Params
			if !strings.Contains(rpcString(req.Params, "write_scope_json"), "README.md") {
				t.Fatalf("expected derived write_scope_json to include README.md, got %+v", req.Params)
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
				"base_branch":      rpcString(req.Params, "base_branch"),
				"head_sha":         rpcString(req.Params, "head_sha"),
				"base_sha":         rpcString(req.Params, "base_sha"),
				"write_scope_json": rpcString(req.Params, "write_scope_json"),
				"status":           "ACTIVE",
			}})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchCommitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"branch_id":      "branch-1",
		"commit_message": "Implement web scaffold",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected README scaffold commit to succeed, got %+v", result)
	}
	for _, want := range []string{`"commit_created": true`, `"next_gate": "project_branch_review_ready"`, `"derived_write_scope_paths"`, "README.md"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if checkoutParams == nil || branchParams == nil {
		t.Fatalf("expected checkout and branch registration params")
	}
	if got := gitOutput(t, checkoutPath, "status", "--porcelain"); got != "" {
		t.Fatalf("expected clean checkout after README scaffold commit, got %q", got)
	}
	if got := gitOutput(t, checkoutPath, "log", "-1", "--pretty=%s"); got != "Implement web scaffold" {
		t.Fatalf("unexpected commit subject %q", got)
	}
	if calls != 3 {
		t.Fatalf("expected coordination read, checkout register, and branch register, got %d", calls)
	}
}

func TestProjectBranchCommitRootSidecarScopeRequiresScaffoldOwnership(t *testing.T) {
	positive, err := pathsetFromWriteScopeJSON(`{"paths":["package.json","index.html","vite.config.*","src/app/**"]}`)
	if err != nil {
		t.Fatalf("positive scope parse: %v", err)
	}
	effective, derived := projectBranchCommitEffectiveWriteScope(positive, []string{"README.md", ".gitignore"})
	for _, want := range []string{"README.md", ".gitignore"} {
		if !stringSliceContains(derived, want) || !pathsetIsCoveredByScope([]string{want}, effective) {
			t.Fatalf("expected scaffold scope to derive %s, derived=%#v effective=%#v", want, derived, effective)
		}
	}

	for _, raw := range []string{
		`{"paths":["src/**"]}`,
		`{"paths":["web/**"]}`,
		`{"paths":["package.json"]}`,
		`{"paths":["api/**","cmd/**"]}`,
	} {
		pathset, err := pathsetFromWriteScopeJSON(raw)
		if err != nil {
			t.Fatalf("negative scope parse %s: %v", raw, err)
		}
		effective, derived := projectBranchCommitEffectiveWriteScope(pathset, []string{"README.md", ".gitignore"})
		if len(derived) != 0 || pathsetIsCoveredByScope([]string{"README.md"}, effective) || pathsetIsCoveredByScope([]string{".gitignore"}, effective) {
			t.Fatalf("scope %s must not derive root sidecars, derived=%#v effective=%#v", raw, derived, effective)
		}
	}
}

func TestProjectBranchCommitToolRejectsOutOfScopeDirtyPath(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "project-subpixel", "subpixel-lab")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-B", "agent/agent-alpha/project-subpixel/task-build")
	if err := os.MkdirAll(filepath.Join(checkoutPath, "api"), 0o755); err != nil {
		t.Fatalf("mkdir api: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "api", "server.go"), []byte("package api\n"), 0o644); err != nil {
		t.Fatalf("write server.go: %v", err)
	}

	calls := 0
	var updateParams map[string]any
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
				WriteScopeJSON: `{"paths":["web/**"]}`,
				CheckoutPath:   checkoutPath,
			}))
		case 2:
			if req.Method != "agent.update.post" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			updateParams = req.Params
			payload := rpcString(req.Params, "payload_json")
			for _, want := range []string{"artifact_bound_side_effect.v1", "out_of_boundary", "pending_classification", "api/server.go"} {
				if !strings.Contains(payload, want) {
					t.Fatalf("expected side-effect payload to contain %q, got %s", want, payload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		case 3:
			assertSideEffectClassificationTaskSubmit(t, req, "api/server.go")
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-side-effect-classify-api",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchCommitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"branch_id":      "branch-1",
		"commit_message": "Touch api",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "pending_classification") || !strings.Contains(result.Output, "side_effect_classification") || !strings.Contains(result.Output, "api/server.go") {
		t.Fatalf("expected out-of-scope dirty path side-effect block, got %+v", result)
	}
	if updateParams == nil {
		t.Fatalf("expected side-effect update params")
	}
	if got := gitOutput(t, checkoutPath, "status", "--porcelain"); got == "" {
		t.Fatalf("expected checkout to remain dirty before classification")
	}
	if calls != 3 {
		t.Fatalf("expected coordination read, side-effect update, and classification task, got %d", calls)
	}
}

func TestProjectBranchCommitToolTrustFirstBlocksOutOfScopeDirtyPath(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "project-subpixel", "subpixel-lab")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-B", "agent/agent-alpha/project-subpixel/task-build")
	if err := os.MkdirAll(filepath.Join(checkoutPath, "api"), 0o755); err != nil {
		t.Fatalf("mkdir api: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "api", "server.go"), []byte("package api\n"), 0o644); err != nil {
		t.Fatalf("write server.go: %v", err)
	}

	calls := 0
	var updateParams map[string]any
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
				WriteScopeJSON: `{"paths":["web/**"]}`,
				CheckoutPath:   checkoutPath,
			}))
		case 2:
			if req.Method != "agent.update.post" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			updateParams = req.Params
			payload := rpcString(req.Params, "payload_json")
			for _, want := range []string{"artifact_bound_side_effect.v1", "out_of_boundary", "pending_classification", "api/server.go"} {
				if !strings.Contains(payload, want) {
					t.Fatalf("expected side-effect payload to contain %q, got %s", want, payload)
				}
			}
			writeRPCResult(w, req, map[string]any{})
		case 3:
			assertSideEffectClassificationTaskSubmit(t, req, "api/server.go")
			writeRPCResult(w, req, map[string]any{
				"task_id":      "task-side-effect-classify-api-trust",
				"workspace_id": "ws",
				"status":       "PENDING",
			})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchCommitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir).WithCoordinationMode(CoordinationModeTrustFirst)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"branch_id":      "branch-1",
		"commit_message": "Touch api",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "api/server.go") || !strings.Contains(result.Output, "pending_classification") {
		t.Fatalf("expected trust-first out-of-scope classification block, params=%+v result=%+v", updateParams, result)
	}
	if updateParams == nil {
		t.Fatalf("expected side-effect update params")
	}
	if got := gitOutput(t, checkoutPath, "status", "--porcelain"); got == "" {
		t.Fatalf("expected checkout to remain dirty before classification")
	}
	if got := gitOutput(t, checkoutPath, "log", "-1", "--pretty=%s"); got == "Touch api" {
		t.Fatalf("trust-first must not create out-of-boundary commit before classification")
	}
	if calls != 3 {
		t.Fatalf("expected coordination read, side-effect update, and classification task, got %d", calls)
	}
}

func TestProjectBranchCommitToolRejectsNoChanges(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "project-subpixel", "subpixel-lab")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-B", "agent/agent-alpha/project-subpixel/task-build")

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, branchReviewCoordinationResult(branchReviewCoordinationInput{
			AgentID:        "agent-alpha",
			RepoURL:        remote,
			BranchStatus:   "ACTIVE",
			WriteScopeJSON: `{"paths":["README.md"]}`,
			CheckoutPath:   checkoutPath,
		}))
	}))
	defer server.Close()

	tool := NewProjectBranchCommitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "no git changes") {
		t.Fatalf("expected no changes error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected only coordination lookup, got %d", calls)
	}
}

func TestProjectBranchCommitToolPushesCleanExistingCommitWhenPushTrue(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "project-subpixel", "subpixel-lab")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-B", "agent/agent-alpha/project-subpixel/task-build")
	runGit(t, checkoutPath, "config", "user.name", "Rhizome Test")
	runGit(t, checkoutPath, "config", "user.email", "rhizome-test@example.invalid")
	if err := os.MkdirAll(filepath.Join(checkoutPath, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "web", "app.js"), []byte("export const alreadyCommitted = true;\n"), 0o644); err != nil {
		t.Fatalf("write app.js: %v", err)
	}
	runGit(t, checkoutPath, "add", "web/app.js")
	runGit(t, checkoutPath, "commit", "-m", "Existing local implementation")

	calls := 0
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
				WriteScopeJSON: `{"paths":["web/**"]}`,
				CheckoutPath:   checkoutPath,
			}))
		case 2:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":   "checkout-1",
				"workspace_id":  "ws",
				"project_id":    "project-subpixel",
				"repo_id":       "projrepo-1",
				"agent_id":      "agent-alpha",
				"local_path":    checkoutPath,
				"checkout_kind": "clone",
				"branch_name":   rpcString(req.Params, "branch_name"),
				"head_sha":      rpcString(req.Params, "head_sha"),
				"base_sha":      rpcString(req.Params, "base_sha"),
				"dirty_state":   "clean",
				"status":        "ACTIVE",
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
				"branch_name":      rpcString(req.Params, "branch_name"),
				"branch_kind":      "feature",
				"base_branch":      rpcString(req.Params, "base_branch"),
				"head_sha":         rpcString(req.Params, "head_sha"),
				"base_sha":         rpcString(req.Params, "base_sha"),
				"write_scope_json": `{"paths":["web/**"]}`,
				"status":           "ACTIVE",
			}})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchCommitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-subpixel",
		"branch_id":  "branch-1",
		"push":       true,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected push-only success, got %+v", result)
	}
	for _, want := range []string{`"commit_created": false`, `"push_succeeded": true`, `"next_gate": "project_branch_review_ready"`, `"mandatory_next_tool": "project_branch_review_ready"`, `"project_checkout_materialize"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	remoteHead := strings.TrimSpace(gitOutputNoDir(t, "ls-remote", "--heads", remote, "agent/agent-alpha/project-subpixel/task-build"))
	if remoteHead == "" {
		t.Fatalf("expected existing local branch to be pushed to bare remote")
	}
	if calls != 3 {
		t.Fatalf("expected 3 RPC calls, got %d", calls)
	}
}

func TestProjectBranchCommitToolAllowsReadyForReviewRevisionBeforePatchQueue(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	checkoutPath := filepath.Join(workdir, "project-checkouts", "project-subpixel", "subpixel-lab")
	runGitNoDir(t, "clone", remote, checkoutPath)
	runGit(t, checkoutPath, "checkout", "-B", "agent/agent-alpha/project-subpixel/task-build")
	if err := os.MkdirAll(filepath.Join(checkoutPath, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkoutPath, "web", "app.js"), []byte("export const revised = true;\n"), 0o644); err != nil {
		t.Fatalf("write app.js: %v", err)
	}

	calls := 0
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
				BranchStatus:   "READY_FOR_REVIEW",
				WriteScopeJSON: `{"paths":["web/**"]}`,
				CheckoutPath:   checkoutPath,
			}))
		case 2:
			if req.Method != "project.checkout.register" {
				t.Fatalf("unexpected second method %q", req.Method)
			}
			writeRPCResult(w, req, map[string]any{"checkout": map[string]any{
				"checkout_id":  "checkout-1",
				"workspace_id": "ws",
				"project_id":   "project-subpixel",
				"repo_id":      "projrepo-1",
				"agent_id":     "agent-alpha",
				"local_path":   checkoutPath,
				"branch_name":  rpcString(req.Params, "branch_name"),
				"head_sha":     rpcString(req.Params, "head_sha"),
				"base_sha":     rpcString(req.Params, "base_sha"),
				"dirty_state":  "clean",
				"status":       "ACTIVE",
			}})
		case 3:
			if req.Method != "project.branch.register" {
				t.Fatalf("unexpected third method %q", req.Method)
			}
			branchParams = req.Params
			if got := rpcString(req.Params, "status"); got != "ACTIVE" {
				t.Fatalf("status = %q", got)
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
				"base_branch":      rpcString(req.Params, "base_branch"),
				"head_sha":         rpcString(req.Params, "head_sha"),
				"base_sha":         rpcString(req.Params, "base_sha"),
				"write_scope_json": `{"paths":["web/**"]}`,
				"status":           "ACTIVE",
			}})
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectBranchCommitTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":     "project-subpixel",
		"branch_id":      "branch-1",
		"commit_message": "Revise after review findings",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected revision commit success, got %+v", result)
	}
	if branchParams == nil || rpcString(branchParams, "head_sha") == "" {
		t.Fatalf("expected branch head evidence to be registered, got %+v", branchParams)
	}
	for _, want := range []string{`"commit_created": true`, `"next_gate": "project_branch_review_ready"`, `"mandatory_next_tool": "project_branch_review_ready"`, `"project_checkout_materialize"`, "web/app.js"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if got := gitOutput(t, checkoutPath, "status", "--porcelain"); got != "" {
		t.Fatalf("expected clean checkout after revision commit, got %q", got)
	}
	if calls != 3 {
		t.Fatalf("expected 3 RPC calls, got %d", calls)
	}
}

func TestSelectProjectBranchCommitBranchRejectsReadyForReviewWithOpenPatchQueue(t *testing.T) {
	branch := ProjectBranchRecord{
		BranchID:   "branch-1",
		RepoID:     "repo-1",
		AgentID:    "agent-alpha",
		BranchName: "agent/agent-alpha/project-subpixel/task-build",
		HeadSHA:    "head123",
		Status:     "READY_FOR_REVIEW",
	}
	if _, err := selectProjectBranchCommitBranch([]ProjectBranchRecord{branch}, nil, "agent-alpha", "branch-1", "", ""); err != nil {
		t.Fatalf("expected READY_FOR_REVIEW branch without patch queue to be revisable, got %v", err)
	}
	_, err := selectProjectBranchCommitBranch([]ProjectBranchRecord{branch}, []ProjectPatchQueueItemRecord{{
		BranchID: "branch-1",
		State:    "PROPOSED",
	}}, "agent-alpha", "branch-1", "", "")
	if err == nil || !strings.Contains(err.Error(), "open patch queue") {
		t.Fatalf("expected open patch queue rejection, got %v", err)
	}
	_, err = selectProjectBranchCommitBranch([]ProjectBranchRecord{branch}, []ProjectPatchQueueItemRecord{{
		QueueID:  "patchq-1",
		ItemID:   "patchitem-1",
		BranchID: "branch-1",
		HeadSHA:  "head123",
		State:    "ACCEPTED",
	}}, "agent-alpha", "branch-1", "", "")
	if err == nil || !strings.Contains(err.Error(), "accepted source boundary is frozen") {
		t.Fatalf("expected accepted source boundary freeze rejection, got %v", err)
	}
	_, err = selectProjectBranchCommitBranch([]ProjectBranchRecord{branch}, []ProjectPatchQueueItemRecord{{
		QueueID:  "patchq-2",
		ItemID:   "patchitem-2",
		BranchID: "branch-1",
		HeadSHA:  "older-reviewed-head",
		State:    "ACCEPTED",
	}}, "agent-alpha", "branch-1", "", "")
	if err == nil || !strings.Contains(err.Error(), "accepted source boundary is frozen") {
		t.Fatalf("expected any accepted branch boundary to freeze local mutation, got %v", err)
	}
}

func gitOutputNoDir(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, string(out))
	}
	return strings.TrimSpace(string(out))
}
