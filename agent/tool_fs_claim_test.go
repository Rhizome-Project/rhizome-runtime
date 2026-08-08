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

func TestListDirectoryToolFiltersProjectCheckoutsToActiveClaim(t *testing.T) {
	workdir := t.TempDir()
	activePath := filepath.Join(workdir, "project-checkouts", "active-claim")
	stalePath := filepath.Join(workdir, "project-checkouts", "stale-vendored")
	for _, path := range []string{activePath, stalePath} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("mkdir checkout path: %v", err)
		}
	}

	server := activeClaimCheckoutTestServer(t, activePath)
	defer server.Close()

	tool := NewListDirectoryTool(workdir).WithProjectCheckoutGuard(
		NewRhizomeClient(server.URL, "token"),
		"ws",
		"agent-delta",
		activeClaimCheckoutTestBinding,
	)
	result := tool.Execute(context.Background(), map[string]any{"path": "project-checkouts"})
	if result == nil || result.IsError {
		t.Fatalf("expected project-checkouts listing to pass, got %+v", result)
	}
	if !strings.Contains(result.Output, "active-claim/") || strings.Contains(result.Output, "stale-vendored/") {
		t.Fatalf("expected only active claim checkout in listing, got %q", result.Output)
	}
}

func TestListDirectoryToolDefaultsToActiveClaimCheckout(t *testing.T) {
	workdir := t.TempDir()
	activePath := filepath.Join(workdir, "project-checkouts", "active-claim")
	if err := os.MkdirAll(activePath, 0o755); err != nil {
		t.Fatalf("mkdir active checkout: %v", err)
	}
	if err := os.WriteFile(filepath.Join(activePath, "go.mod"), []byte("module active\n"), 0o644); err != nil {
		t.Fatalf("write active go.mod: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workdir, "agent-only.txt"), []byte("root sentinel\n"), 0o644); err != nil {
		t.Fatalf("write root sentinel: %v", err)
	}

	server := activeClaimCheckoutTestServer(t, activePath)
	defer server.Close()

	tool := NewListDirectoryTool(workdir).WithProjectCheckoutGuard(
		NewRhizomeClient(server.URL, "token"),
		"ws",
		"agent-delta",
		activeClaimCheckoutTestBinding,
	)
	result := tool.Execute(context.Background(), map[string]any{"path": "."})
	if result == nil || result.IsError {
		t.Fatalf("expected active checkout listing to pass, got %+v", result)
	}
	if !strings.Contains(result.Output, "go.mod") || strings.Contains(result.Output, "agent-only.txt") {
		t.Fatalf("expected relative listing to use active checkout, got %q", result.Output)
	}
}

func TestReadFileToolBlocksForeignCheckoutDuringActiveClaim(t *testing.T) {
	workdir := t.TempDir()
	activePath := filepath.Join(workdir, "project-checkouts", "active-claim")
	stalePath := filepath.Join(workdir, "project-checkouts", "stale-vendored")
	if err := os.MkdirAll(activePath, 0o755); err != nil {
		t.Fatalf("mkdir active checkout: %v", err)
	}
	if err := os.MkdirAll(stalePath, 0o755); err != nil {
		t.Fatalf("mkdir stale checkout: %v", err)
	}
	if err := os.WriteFile(filepath.Join(activePath, "go.mod"), []byte("module active\n"), 0o644); err != nil {
		t.Fatalf("write active go.mod: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stalePath, "go.mod"), []byte("require github.com/yuin/gopher-lua v1.1.1\n"), 0o644); err != nil {
		t.Fatalf("write stale go.mod: %v", err)
	}

	server := activeClaimCheckoutTestServer(t, activePath)
	defer server.Close()

	tool := NewReadFileTool(workdir).WithProjectCheckoutGuard(
		NewRhizomeClient(server.URL, "token"),
		"ws",
		"agent-delta",
		activeClaimCheckoutTestBinding,
	)
	staleResult := tool.Execute(context.Background(), map[string]any{"path": filepath.Join("project-checkouts", "stale-vendored", "go.mod")})
	if staleResult == nil || !staleResult.IsError {
		t.Fatalf("expected stale checkout read to fail, got %+v", staleResult)
	}
	if !strings.Contains(staleResult.Output, "stale sibling project-checkouts") {
		t.Fatalf("unexpected stale read output: %s", staleResult.Output)
	}

	activeResult := tool.Execute(context.Background(), map[string]any{"path": filepath.Join("project-checkouts", "active-claim", "go.mod")})
	if activeResult == nil || activeResult.IsError {
		t.Fatalf("expected active checkout read to pass, got %+v", activeResult)
	}
	if !strings.Contains(activeResult.Output, "module active") {
		t.Fatalf("unexpected active read output: %s", activeResult.Output)
	}
}

func TestReadFileToolDefaultsToActiveClaimCheckout(t *testing.T) {
	workdir := t.TempDir()
	activePath := filepath.Join(workdir, "project-checkouts", "active-claim")
	if err := os.MkdirAll(activePath, 0o755); err != nil {
		t.Fatalf("mkdir active checkout: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workdir, "go.mod"), []byte("module agent-root\n"), 0o644); err != nil {
		t.Fatalf("write root go.mod: %v", err)
	}
	if err := os.WriteFile(filepath.Join(activePath, "go.mod"), []byte("module active\n"), 0o644); err != nil {
		t.Fatalf("write active go.mod: %v", err)
	}

	server := activeClaimCheckoutTestServer(t, activePath)
	defer server.Close()

	tool := NewReadFileTool(workdir).WithProjectCheckoutGuard(
		NewRhizomeClient(server.URL, "token"),
		"ws",
		"agent-delta",
		activeClaimCheckoutTestBinding,
	)
	result := tool.Execute(context.Background(), map[string]any{"path": "go.mod"})
	if result == nil || result.IsError {
		t.Fatalf("expected relative read to pass, got %+v", result)
	}
	if !strings.Contains(result.Output, "module active") || strings.Contains(result.Output, "module agent-root") {
		t.Fatalf("expected relative read to use active checkout, got %q", result.Output)
	}
}

func TestWriteFileToolDefaultsToActiveClaimCheckout(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	remote := seedBareGitRepo(t)
	workdir := t.TempDir()
	activePath := filepath.Join(workdir, "project-checkouts", "active-claim")
	branchName := "agent-delta-eval"
	runGitNoDir(t, "clone", remote, activePath)
	runGit(t, activePath, "checkout", "-b", branchName)

	server := activeClaimCheckoutWithBranchTestServer(t, remote, activePath, branchName)
	defer server.Close()

	tool := NewWriteFileTool(workdir).WithProjectCheckoutGuard(
		NewRhizomeClient(server.URL, "token"),
		"ws",
		"agent-delta",
		activeClaimCheckoutTestBinding,
	)
	result := tool.Execute(context.Background(), map[string]any{
		"path":    filepath.Join("internal", "eval", "new.go"),
		"content": "package eval\n",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected relative write to pass, got %+v", result)
	}
	writtenPath := filepath.Join(activePath, "internal", "eval", "new.go")
	if data, err := os.ReadFile(writtenPath); err != nil || string(data) != "package eval\n" {
		t.Fatalf("expected file in active checkout, read %q err %v", string(data), err)
	}
	if _, err := os.Stat(filepath.Join(workdir, "internal", "eval", "new.go")); !os.IsNotExist(err) {
		t.Fatalf("expected no relative write in agent root, stat err %v", err)
	}
}

func activeClaimCheckoutTestBinding() AgentRuntimeBinding {
	return AgentRuntimeBinding{
		ProjectID:       "project-subpixel",
		TaskID:          "task-eval",
		ClaimRepoID:     "projrepo-1",
		ClaimCheckoutID: "checkout-active",
		ClaimBranchID:   "branch-active",
	}
}

func activeClaimCheckoutTestServer(t *testing.T, activePath string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:   "READY",
			RepoURL:      "file:///tmp/lua51-subset.git",
			CurrentPhase: "IMPLEMENTATION",
			Tasks: []map[string]any{{
				"task_id":      "task-eval",
				"status":       "RUNNING",
				"task_kind":    "EXECUTION",
				"project_lane": "implementation",
			}},
			Checkouts: []map[string]any{{
				"checkout_id":    "checkout-active",
				"workspace_id":   "ws",
				"project_id":     "project-subpixel",
				"repo_id":        "projrepo-1",
				"agent_id":       "agent-delta",
				"local_path":     activePath,
				"checkout_kind":  "clone",
				"branch_name":    "agent-delta-eval",
				"active_task_id": "task-eval",
				"status":         "ACTIVE",
			}},
		}))
	}))
}

func activeClaimCheckoutWithBranchTestServer(t *testing.T, remote, activePath, branchName string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectCheckoutCoordinationResult(projectCheckoutCoordinationInput{
			RepoStatus:     "READY",
			RepoURL:        remote,
			CurrentPhase:   "IMPLEMENTATION",
			WriteScopeJSON: `{"paths":["internal/eval/**"]}`,
			Tasks: []map[string]any{{
				"task_id":      "task-eval",
				"status":       "RUNNING",
				"task_kind":    "EXECUTION",
				"project_lane": "implementation",
			}},
			Checkouts: []map[string]any{{
				"checkout_id":    "checkout-active",
				"workspace_id":   "ws",
				"project_id":     "project-subpixel",
				"repo_id":        "projrepo-1",
				"agent_id":       "agent-delta",
				"local_path":     activePath,
				"checkout_kind":  "clone",
				"branch_name":    branchName,
				"active_task_id": "task-eval",
				"status":         "ACTIVE",
			}},
			Branches: []map[string]any{{
				"branch_id":        "branch-active",
				"workspace_id":     "ws",
				"project_id":       "project-subpixel",
				"repo_id":          "projrepo-1",
				"checkout_id":      "checkout-active",
				"agent_id":         "agent-delta",
				"active_task_id":   "task-eval",
				"branch_name":      branchName,
				"branch_kind":      "feature",
				"write_scope_json": `{"paths":["internal/eval/**"]}`,
				"status":           "ACTIVE",
			}},
		}))
	}))
}
