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

func TestProjectRepoMaterializeToolCreatesLocalBareRepoAndRegistersReady(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir := t.TempDir()
	projectID := "project-subpixel"
	repoID := "projrepo-existing-requested"
	repoName := "subpixel-lab"
	localRemotePath := filepath.Join(workdir, "project-remotes", projectID, repoName+".git")
	remoteURL, err := projectRepoMaterializeFileURL(localRemotePath)
	if err != nil {
		t.Fatalf("file URL: %v", err)
	}
	existingRepo := map[string]any{
		"workspace_id":        "ws",
		"project_id":          projectID,
		"repo_id":             repoID,
		"remote_kind":         "unknown",
		"name":                repoName,
		"default_branch":      "main",
		"repo_status":         "REQUESTED",
		"is_canonical":        true,
		"created_by_agent_id": "agent-alpha",
	}
	readyRepo := map[string]any{
		"workspace_id":        "ws",
		"project_id":          projectID,
		"repo_id":             repoID,
		"remote_url":          remoteURL,
		"remote_kind":         "local",
		"owner":               "agent-alpha",
		"name":                repoName,
		"default_branch":      "main",
		"repo_status":         "READY",
		"is_canonical":        true,
		"created_by_agent_id": "agent-alpha",
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
			writeRPCResult(w, req, projectRepoCoordinationResult(projectID, []map[string]any{existingRepo}, "BLOCKED", false))
		case 3:
			if req.Method != "project.repository.upsert" {
				t.Fatalf("unexpected repository method %q", req.Method)
			}
			for key, want := range map[string]string{
				"repo_id":        repoID,
				"remote_url":     remoteURL,
				"remote_kind":    "local",
				"owner":          "agent-alpha",
				"name":           repoName,
				"default_branch": "main",
				"repo_status":    "READY",
			} {
				if got := rpcString(req.Params, key); got != want {
					t.Fatalf("%s = %q, want %q; params=%+v", key, got, want, req.Params)
				}
			}
			writeRPCResult(w, req, map[string]any{"repository": readyRepo})
		case 4:
			if req.Method != "project.profile.update" {
				t.Fatalf("unexpected profile method %q", req.Method)
			}
			if got := rpcString(req.Params, "repo_status"); got != "READY" {
				t.Fatalf("profile repo_status = %q, want READY", got)
			}
			if got := rpcString(req.Params, "repo_url"); got != remoteURL {
				t.Fatalf("profile repo_url = %q, want %q", got, remoteURL)
			}
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":        "ws",
				"project_id":          projectID,
				"current_phase":       "SPEC",
				"repo_required":       true,
				"repo_status":         "READY",
				"repo_url":            remoteURL,
				"repo_default_branch": "main",
			}})
		case 5:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectRepoCoordinationResult(projectID, []map[string]any{readyRepo}, "PARTIAL", false))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRepoMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        projectID,
		"local_remote_path": localRemotePath,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful local repo materialization, got %+v", result)
	}
	for _, want := range []string{`"repo_id": "projrepo-existing-requested"`, `"remote_kind": "local"`, `"local_repo_created": true`, `"seed_commit_created": true`, `"repo_status": "READY"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if out, err := gitBareCombined(context.Background(), localRemotePath, "rev-parse", "--verify", "refs/heads/main"); err != nil || strings.TrimSpace(out) == "" {
		t.Fatalf("expected seeded main branch, out=%q err=%v", out, err)
	}
	if calls != 5 {
		t.Fatalf("expected 5 calls, got %d", calls)
	}
}

func TestProjectRepoMaterializeToolRejectsCrossProjectRuntimeBindingBeforeRPC(t *testing.T) {
	tool := NewProjectRepoMaterializeTool(NewRhizomeClient("http://127.0.0.1:1", "token"), "ws", "agent-alpha", "owner-1", t.TempDir()).
		WithRuntimeBinding(func() AgentRuntimeBinding {
			return AgentRuntimeBinding{ProjectID: "project-current", TaskID: "task-current"}
		})
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": "project-old",
		"repo_name":  "old",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "active task is bound to project_id project-current") {
		t.Fatalf("expected active-project binding rejection before RPC, got %+v", result)
	}
}

func TestProjectRepoMaterializeDefaultPathUsesCompactProjectSegment(t *testing.T) {
	workdir := t.TempDir()
	projectID := "project-task-clearpress-autonomous-mvp-20260518-fullscope-rerun3-root"
	repoName := "project-task-clearpress-autonomous-mvp-20260518-fullscope-rerun3-root"

	got := projectRepoMaterializeDefaultPath(workdir, projectID, repoName)
	rel, err := filepath.Rel(workdir, got)
	if err != nil {
		t.Fatalf("relative path: %v", err)
	}
	if strings.Contains(rel, sanitizePathComponent(projectID)) {
		t.Fatalf("default path still embeds long project id: %s", rel)
	}
	wantPrefix := filepath.Join("project-remotes", "p-"+shortRefHash(projectID))
	if !strings.HasPrefix(rel, wantPrefix+string(filepath.Separator)) {
		t.Fatalf("default path = %s, want prefix %s", rel, wantPrefix)
	}
	if !strings.HasSuffix(rel, ".git") {
		t.Fatalf("default path = %s, want .git suffix", rel)
	}
	if len(rel) > 90 {
		t.Fatalf("default path should stay compact relative to workdir, got %d chars: %s", len(rel), rel)
	}
}

func TestMaterializeLocalBareGitRepoResolvesRelativePathFromWorkdir(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir := t.TempDir()
	relativeRemotePath := filepath.Join("r", "cp.git")
	expectedRemotePath := filepath.Join(workdir, relativeRemotePath)

	created, seeded, err := materializeLocalBareGitRepo(context.Background(), workdir, relativeRemotePath, "main", "project-clearpress")
	if err != nil {
		t.Fatalf("materialize relative bare repo: %v", err)
	}
	if !created || !seeded {
		t.Fatalf("created=%v seeded=%v, want true true", created, seeded)
	}
	if out, err := gitBareCombined(context.Background(), expectedRemotePath, "rev-parse", "--verify", "refs/heads/main"); err != nil || strings.TrimSpace(out) == "" {
		t.Fatalf("expected seeded main branch at workdir-relative remote, out=%q err=%v", out, err)
	}
}

func TestProjectRepoMaterializeToolResolvesRelativeLocalRemotePathInsideWorkdir(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir := t.TempDir()
	projectID := "project-clearpress"
	repoID := "projrepo-clearpress"
	repoName := "clearpress"
	relativeRemotePath := filepath.Join("r", "cp.git")
	expectedRemotePath := filepath.Join(workdir, relativeRemotePath)
	expectedRemoteURL, err := projectRepoMaterializeFileURL(expectedRemotePath)
	if err != nil {
		t.Fatalf("file URL: %v", err)
	}
	readyRepo := map[string]any{
		"workspace_id":        "ws",
		"project_id":          projectID,
		"repo_id":             repoID,
		"remote_url":          expectedRemoteURL,
		"remote_kind":         "local",
		"owner":               "agent-alpha",
		"name":                repoName,
		"default_branch":      "main",
		"repo_status":         "READY",
		"is_canonical":        true,
		"created_by_agent_id": "agent-alpha",
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
			writeRPCResult(w, req, projectRepoCoordinationResult(projectID, nil, "BLOCKED", false))
		case 3:
			if req.Method != "project.repository.upsert" {
				t.Fatalf("unexpected repository method %q", req.Method)
			}
			if got := rpcString(req.Params, "remote_url"); got != expectedRemoteURL {
				t.Fatalf("remote_url = %q, want %q", got, expectedRemoteURL)
			}
			writeRPCResult(w, req, map[string]any{"repository": readyRepo})
		case 4:
			if req.Method != "project.profile.update" {
				t.Fatalf("unexpected profile method %q", req.Method)
			}
			if got := rpcString(req.Params, "repo_url"); got != expectedRemoteURL {
				t.Fatalf("profile repo_url = %q, want %q", got, expectedRemoteURL)
			}
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":        "ws",
				"project_id":          projectID,
				"current_phase":       "SPEC",
				"repo_required":       true,
				"repo_status":         "READY",
				"repo_url":            expectedRemoteURL,
				"repo_default_branch": "main",
			}})
		case 5:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectRepoCoordinationResult(projectID, []map[string]any{readyRepo}, "PARTIAL", true))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRepoMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        projectID,
		"repo_id":           repoID,
		"repo_name":         repoName,
		"local_remote_path": relativeRemotePath,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected successful relative repo materialization, got %+v", result)
	}
	if !strings.Contains(result.Output, `"remote_url": "`+expectedRemoteURL+`"`) {
		t.Fatalf("expected output to contain resolved remote URL %q, got %q", expectedRemoteURL, result.Output)
	}
	if out, err := gitBareCombined(context.Background(), expectedRemotePath, "rev-parse", "--verify", "refs/heads/main"); err != nil || strings.TrimSpace(out) == "" {
		t.Fatalf("expected seeded main branch at resolved relative path, out=%q err=%v", out, err)
	}
	if calls != 5 {
		t.Fatalf("expected 5 calls, got %d", calls)
	}
}

func TestProjectRepoMaterializeRemoteURLClonesWithProjectCheckoutMaterialize(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	ctx := context.Background()
	workdir := t.TempDir()
	remotePath := filepath.Join(workdir, "r", "cp.git")
	if _, _, err := materializeLocalBareGitRepo(ctx, workdir, filepath.Join("r", "cp.git"), "main", "project-clearpress"); err != nil {
		t.Fatalf("materialize relative remote: %v", err)
	}
	remoteURL, err := projectRepoMaterializeFileURL(remotePath)
	if err != nil {
		t.Fatalf("file URL: %v", err)
	}
	repo := ProjectRepositoryRecord{
		RepoID:        "repo-clearpress",
		Name:          "clearpress",
		RemoteURL:     remoteURL,
		RemoteKind:    "local",
		DefaultBranch: "main",
	}
	checkoutPath := filepath.Join(workdir, "project-checkouts", "clearpress")
	created, reused, err := materializeGitCheckout(ctx, checkoutPath, repo, "main", false)
	if err != nil {
		t.Fatalf("checkout materialize from repo materialize remote URL: %v", err)
	}
	if !created || reused {
		t.Fatalf("created=%v reused=%v, want true false", created, reused)
	}
	if out, err := gitCombined(ctx, checkoutPath, "rev-parse", "--verify", "HEAD"); err != nil || strings.TrimSpace(out) == "" {
		t.Fatalf("expected checkout HEAD, out=%q err=%v", out, err)
	}
	if _, err := exec.LookPath("git"); err == nil {
		if _, err := os.Stat(filepath.Join(checkoutPath, "README.md")); err != nil {
			t.Fatalf("expected cloned README.md: %v", err)
		}
	}
}

func TestProjectRepoMaterializeToolPreservesExistingReadyCanonicalRemote(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	projectID := "project-subpixel"
	repoID := "projrepo-existing-ready"
	repoName := "subpixel-lab"
	leadWorkdir := t.TempDir()
	remotePath := filepath.Join(leadWorkdir, "project-remotes", projectID, repoName+".git")
	if _, _, err := materializeLocalBareGitRepo(context.Background(), leadWorkdir, remotePath, "main", projectID); err != nil {
		t.Fatalf("seed existing bare repo: %v", err)
	}
	remoteURL, err := projectRepoMaterializeFileURL(remotePath)
	if err != nil {
		t.Fatalf("file URL: %v", err)
	}
	existingRepo := map[string]any{
		"workspace_id":        "ws",
		"project_id":          projectID,
		"repo_id":             repoID,
		"remote_url":          remoteURL,
		"remote_kind":         "local",
		"owner":               "agent-alpha",
		"name":                repoName,
		"default_branch":      "main",
		"repo_status":         "READY",
		"is_canonical":        true,
		"created_by_agent_id": "agent-alpha",
	}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectRepoCoordinationResult(projectID, []map[string]any{existingRepo}, "PARTIAL", true))
	}))
	defer server.Close()

	agentWorkdir := t.TempDir()
	tool := NewProjectRepoMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1", agentWorkdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": projectID,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected existing READY repository reuse, got %+v", result)
	}
	for _, want := range []string{`"repo_id": "projrepo-existing-ready"`, `"remote_url": "` + remoteURL + `"`, `"local_repo_created": false`, `"seed_commit_created": false`, `"canonical_remote_preserved": true`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	defaultRemotePath := projectRepoMaterializeDefaultPath(agentWorkdir, projectID, repoName)
	if _, err := exec.LookPath("git"); err == nil {
		if out, err := gitBareCombined(context.Background(), defaultRemotePath, "rev-parse", "--is-bare-repository"); err == nil || strings.TrimSpace(out) == "true" {
			t.Fatalf("expected no new bare repository at %s, out=%q err=%v", defaultRemotePath, out, err)
		}
	}
	if calls != 1 {
		t.Fatalf("expected only coordination read, got %d calls", calls)
	}
}

func TestProjectRepoMaterializeToolRecreatesMissingReadyLocalCanonicalRemote(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	projectID := "project-subpixel"
	repoID := "projrepo-existing-ready"
	repoName := "subpixel-lab"
	missingRemotePath := filepath.Join(t.TempDir(), "project-remotes", projectID, repoName+".git")
	missingRemoteURL, err := projectRepoMaterializeFileURL(missingRemotePath)
	if err != nil {
		t.Fatalf("file URL: %v", err)
	}
	agentWorkdir := t.TempDir()
	replacementRemotePath := projectRepoMaterializeDefaultPath(agentWorkdir, projectID, repoName)
	replacementRemoteURL, err := projectRepoMaterializeFileURL(replacementRemotePath)
	if err != nil {
		t.Fatalf("replacement file URL: %v", err)
	}
	existingRepo := map[string]any{
		"workspace_id":        "ws",
		"project_id":          projectID,
		"repo_id":             repoID,
		"remote_url":          missingRemoteURL,
		"remote_kind":         "local",
		"owner":               "agent-alpha",
		"name":                repoName,
		"default_branch":      "main",
		"repo_status":         "READY",
		"is_canonical":        true,
		"created_by_agent_id": "agent-alpha",
	}
	readyRepo := map[string]any{
		"workspace_id":        "ws",
		"project_id":          projectID,
		"repo_id":             repoID,
		"remote_url":          replacementRemoteURL,
		"remote_kind":         "local",
		"owner":               "agent-beta",
		"name":                repoName,
		"default_branch":      "main",
		"repo_status":         "READY",
		"is_canonical":        true,
		"created_by_agent_id": "agent-beta",
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
			writeRPCResult(w, req, projectRepoCoordinationResult(projectID, []map[string]any{existingRepo}, "PARTIAL", true))
		case 3:
			if req.Method != "project.repository.upsert" {
				t.Fatalf("unexpected repository method %q", req.Method)
			}
			for key, want := range map[string]string{
				"repo_id":        repoID,
				"remote_url":     replacementRemoteURL,
				"remote_kind":    "local",
				"owner":          "agent-beta",
				"name":           repoName,
				"default_branch": "main",
				"repo_status":    "READY",
			} {
				if got := rpcString(req.Params, key); got != want {
					t.Fatalf("%s = %q, want %q; params=%+v", key, got, want, req.Params)
				}
			}
			writeRPCResult(w, req, map[string]any{"repository": readyRepo})
		case 4:
			if req.Method != "project.profile.update" {
				t.Fatalf("unexpected profile method %q", req.Method)
			}
			if got := rpcString(req.Params, "repo_status"); got != "READY" {
				t.Fatalf("profile repo_status = %q, want READY", got)
			}
			if got := rpcString(req.Params, "repo_url"); got != replacementRemoteURL {
				t.Fatalf("profile repo_url = %q, want %q", got, replacementRemoteURL)
			}
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":        "ws",
				"project_id":          projectID,
				"current_phase":       "SPEC",
				"repo_required":       true,
				"repo_status":         "READY",
				"repo_url":            replacementRemoteURL,
				"repo_default_branch": "main",
			}})
		case 5:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectRepoCoordinationResult(projectID, []map[string]any{readyRepo}, "PARTIAL", true))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRepoMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1", agentWorkdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": projectID,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected missing READY local repository to be recreated, got %+v", result)
	}
	for _, want := range []string{`"repo_id": "projrepo-existing-ready"`, `"remote_url": "` + replacementRemoteURL + `"`, `"local_repo_created": true`, `"seed_commit_created": true`, `"reused_existing_record": true`, `"ready_local_remote_recreated": true`, `"previous_remote_url": "` + missingRemoteURL + `"`, "Prior branch refs/objects were not recovered"} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if strings.Contains(result.Output, `"canonical_remote_preserved": true`) {
		t.Fatalf("expected missing local remote to be recreated, got preserved output %q", result.Output)
	}
	if out, err := gitBareCombined(context.Background(), replacementRemotePath, "rev-parse", "--verify", "refs/heads/main"); err != nil || strings.TrimSpace(out) == "" {
		t.Fatalf("expected recreated seeded main branch, out=%q err=%v", out, err)
	}
	if calls != 5 {
		t.Fatalf("expected 5 calls, got %d", calls)
	}
}

func TestProjectRepoMaterializeToolRecreatesAmbiguousRelativeReadyLocalRemote(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	projectID := "project-clearpress"
	repoID := "projrepo-existing-ready-relative"
	repoName := "clearpress"
	agentWorkdir := t.TempDir()
	replacementRemotePath := projectRepoMaterializeDefaultPath(agentWorkdir, projectID, repoName)
	replacementRemoteURL, err := projectRepoMaterializeFileURL(replacementRemotePath)
	if err != nil {
		t.Fatalf("replacement file URL: %v", err)
	}
	existingRepo := map[string]any{
		"workspace_id":        "ws",
		"project_id":          projectID,
		"repo_id":             repoID,
		"remote_url":          "r/cp.git",
		"remote_kind":         "local",
		"owner":               "agent-alpha",
		"name":                repoName,
		"default_branch":      "main",
		"repo_status":         "READY",
		"is_canonical":        true,
		"created_by_agent_id": "agent-alpha",
	}
	readyRepo := map[string]any{
		"workspace_id":        "ws",
		"project_id":          projectID,
		"repo_id":             repoID,
		"remote_url":          replacementRemoteURL,
		"remote_kind":         "local",
		"owner":               "agent-beta",
		"name":                repoName,
		"default_branch":      "main",
		"repo_status":         "READY",
		"is_canonical":        true,
		"created_by_agent_id": "agent-beta",
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
			writeRPCResult(w, req, projectRepoCoordinationResult(projectID, []map[string]any{existingRepo}, "PARTIAL", true))
		case 3:
			if req.Method != "project.repository.upsert" {
				t.Fatalf("unexpected repository method %q", req.Method)
			}
			if got := rpcString(req.Params, "remote_url"); got != replacementRemoteURL {
				t.Fatalf("remote_url = %q, want %q", got, replacementRemoteURL)
			}
			writeRPCResult(w, req, map[string]any{"repository": readyRepo})
		case 4:
			if req.Method != "project.profile.update" {
				t.Fatalf("unexpected profile method %q", req.Method)
			}
			if got := rpcString(req.Params, "repo_url"); got != replacementRemoteURL {
				t.Fatalf("profile repo_url = %q, want %q", got, replacementRemoteURL)
			}
			writeRPCResult(w, req, map[string]any{"profile": map[string]any{
				"workspace_id":        "ws",
				"project_id":          projectID,
				"current_phase":       "SPEC",
				"repo_required":       true,
				"repo_status":         "READY",
				"repo_url":            replacementRemoteURL,
				"repo_default_branch": "main",
			}})
		case 5:
			if req.Method != "project.coordination.get" {
				t.Fatalf("unexpected final method %q", req.Method)
			}
			writeRPCResult(w, req, projectRepoCoordinationResult(projectID, []map[string]any{readyRepo}, "PARTIAL", true))
		default:
			t.Fatalf("unexpected extra RPC call %d method=%s", calls, req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectRepoMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-beta", "owner-1", agentWorkdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id": projectID,
	})
	if result == nil || result.IsError {
		t.Fatalf("expected ambiguous READY local repository to be recreated, got %+v", result)
	}
	for _, want := range []string{`"remote_url": "` + replacementRemoteURL + `"`, `"ready_local_remote_recreated": true`, `"previous_remote_url": "r/cp.git"`} {
		if !strings.Contains(result.Output, want) {
			t.Fatalf("expected output to contain %q, got %q", want, result.Output)
		}
	}
	if strings.Contains(result.Output, `"canonical_remote_preserved": true`) {
		t.Fatalf("expected ambiguous relative local remote not to be preserved, got %q", result.Output)
	}
	if calls != 5 {
		t.Fatalf("expected 5 calls, got %d", calls)
	}
}

func TestProjectRepoMaterializeToolRejectsPathOutsideWorkdir(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.git")
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectRepoCoordinationResult("project-subpixel", nil, "BLOCKED", false))
	}))
	defer server.Close()

	tool := NewProjectRepoMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        "project-subpixel",
		"local_remote_path": outside,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected outside path rejection, got %+v", result)
	}
	if !strings.Contains(result.Output, "outside agent workdir") {
		t.Fatalf("expected outside workdir error, got %q", result.Output)
	}
	if calls != 1 {
		t.Fatalf("expected one coordination call before path validation, got %d", calls)
	}
}

func TestProjectRepoMaterializeToolRejectsRelativePathTraversal(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir := t.TempDir()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectRepoCoordinationResult("project-clearpress", nil, "BLOCKED", false))
	}))
	defer server.Close()

	tool := NewProjectRepoMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        "project-clearpress",
		"local_remote_path": filepath.Join("..", "cp.git"),
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected relative traversal rejection, got %+v", result)
	}
	if !strings.Contains(result.Output, "outside agent workdir") {
		t.Fatalf("expected outside workdir error, got %q", result.Output)
	}
	if calls != 1 {
		t.Fatalf("expected one coordination call before path validation, got %d", calls)
	}
}

func TestProjectRepoMaterializeToolRejectsFileURLOutsideWorkdir(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	workdir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.git")
	outsideURL, err := projectRepoMaterializeFileURL(outside)
	if err != nil {
		t.Fatalf("file URL: %v", err)
	}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, projectRepoCoordinationResult("project-clearpress", nil, "BLOCKED", false))
	}))
	defer server.Close()

	tool := NewProjectRepoMaterializeTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", "owner-1", workdir)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":        "project-clearpress",
		"local_remote_path": outsideURL,
	})
	if result == nil || !result.IsError {
		t.Fatalf("expected outside file URL rejection, got %+v", result)
	}
	if !strings.Contains(result.Output, "outside agent workdir") {
		t.Fatalf("expected outside workdir error, got %q", result.Output)
	}
	if calls != 1 {
		t.Fatalf("expected one coordination call before path validation, got %d", calls)
	}
}
