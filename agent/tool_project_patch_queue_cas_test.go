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

func TestProjectPatchQueueCASToolBindsOperationAndRecordsImmutableHeadEvidence(t *testing.T) {
	repoRoot, baseSHA, headSHA, baseDigest, headDigest := setupPatchQueueGitRepo(t)
	pathsetJSON := `{"paths":["web/app.js"]}`

	calls := make([]string, 0, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls = append(calls, req.Method)
		switch req.Method {
		case "project.coordination.get":
			writeRPCResult(w, req, controlledPatchQueueCoordinationResult(repoRoot, baseSHA, headSHA, map[string]any{
				"state":                      "CLAIMED",
				"claimed_by":                 "agent-alpha",
				"claim_token":                "claim-token-1",
				"repo_authority_mode":        "repoauthority_controlled_queue",
				"pathset_json":               pathsetJSON,
				"base_file_hashes":           map[string]string{"web/app.js": baseDigest},
				"context_digest":             "sha256:proposal-context",
				"repo_lease_id":              "lease-1",
				"lease_term":                 1,
				"task_id":                    "task-1",
				"session_id":                 "session-1",
				"run_id":                     "run-1",
				"agent_id":                   "agent-alpha",
				"principal_type":             "agent",
				"principal_id":               "agent-alpha",
				"capability_snapshot_id":     "snapshot-1",
				"capability_snapshot_schema": daemonCapabilitySnapshotSchema,
			}))
		case "project.patch_queue.operation_bind":
			if _, ok := req.Params["operation_id"]; ok {
				t.Fatalf("operation_id must be omitted by default, got %+v", req.Params)
			}
			if got := rpcString(req.Params, "mutation_paths_json"); got != pathsetJSON {
				t.Fatalf("mutation_paths_json = %q, want %q", got, pathsetJSON)
			}
			if got := rpcString(req.Params, "claim_token"); got != "claim-token-1" {
				t.Fatalf("claim_token = %q", got)
			}
			writeRPCResult(w, req, map[string]any{"patch_queue_item": controlledPatchQueueItem(baseSHA, headSHA, baseDigest, map[string]any{
				"state":                          "CLAIMED",
				"claimed_by":                     "agent-alpha",
				"claim_token":                    "claim-token-1",
				"operation_id":                   "op-queue-1",
				"operation_kind":                 projectPatchQueueOperationKindPatchApply,
				"operation_binding_schema":       "project_patch_queue_operation_binding.v1",
				"operation_binding_accepted":     true,
				"operation_context_digest":       "sha256:operation-context",
				"operation_lease_context_digest": "sha256:operation-lease-context",
				"operation_mutation_paths_json":  pathsetJSON,
				"operation_bound_by":             "agent-alpha",
				"operation_bound_at":             "2026-04-28T12:00:00Z",
				"context_digest":                 "sha256:proposal-context",
				"repo_lease_id":                  "lease-1",
				"lease_term":                     1,
				"task_id":                        "task-1",
				"session_id":                     "session-1",
				"run_id":                         "run-1",
				"agent_id":                       "agent-alpha",
				"principal_type":                 "agent",
				"principal_id":                   "agent-alpha",
				"capability_snapshot_id":         "snapshot-1",
				"capability_snapshot_schema":     daemonCapabilitySnapshotSchema,
				"pathset_json":                   pathsetJSON,
			})})
		case "project.patch_queue.cas_record":
			casResult, ok := req.Params["cas_result"].(map[string]any)
			if !ok {
				t.Fatalf("cas_result missing: %+v", req.Params)
			}
			if casResult["schema"] != projectPatchQueueCASApplySchema ||
				casResult["status"] != projectPatchQueueCASStatusApplied ||
				casResult["patch_id"] != "op-queue-1" ||
				casResult["context_digest"] != "sha256:operation-context" {
				t.Fatalf("unexpected cas_result metadata: %+v", casResult)
			}
			paths, ok := casResult["paths"].([]any)
			if !ok || len(paths) != 1 {
				t.Fatalf("expected one CAS path, got %+v", casResult["paths"])
			}
			path, ok := paths[0].(map[string]any)
			if !ok {
				t.Fatalf("unexpected CAS path payload: %+v", paths[0])
			}
			if path["path"] != "web/app.js" ||
				path["status"] != projectPatchQueueCASStatusApplied ||
				path["base_hash"] != baseDigest ||
				path["current_hash"] != baseDigest ||
				path["candidate_hash"] != headDigest {
				t.Fatalf("unexpected CAS path evidence: %+v", path)
			}
			testEvidence, ok := req.Params["test_evidence"].(map[string]any)
			if !ok || testEvidence["command"] != "go test ./..." || testEvidence["status"] != projectPatchQueueTestStatusPassed {
				t.Fatalf("unexpected test evidence: %+v", req.Params["test_evidence"])
			}
			if got := rpcString(req.Params, "claim_token"); got != "claim-token-1" {
				t.Fatalf("cas claim_token = %q", got)
			}
			writeRPCResult(w, req, map[string]any{"patch_queue_item": controlledPatchQueueItem(baseSHA, headSHA, baseDigest, map[string]any{
				"state":                      "CLAIMED",
				"claimed_by":                 "agent-alpha",
				"claim_token":                "claim-token-1",
				"operation_id":               "op-queue-1",
				"operation_kind":             projectPatchQueueOperationKindPatchApply,
				"operation_binding_accepted": true,
				"operation_context_digest":   "sha256:operation-context",
				"repo_lease_id":              "lease-1",
				"lease_term":                 1,
				"context_digest":             "sha256:proposal-context",
				"cas_evidence_accepted":      true,
				"cas_status":                 projectPatchQueueCASStatusApplied,
				"cas_patch_digest":           casResult["patch_digest"],
				"cas_evaluation_digest":      "sha256:cas-evaluation",
				"cas_result":                 casResult,
			})})
		default:
			t.Fatalf("unexpected method %q", req.Method)
		}
	}))
	defer server.Close()

	tool := NewProjectPatchQueueCASTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", repoRoot)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":          "project-subpixel",
		"branch_id":           "branch-1",
		"test_name":           "unit suite",
		"test_command":        "go test ./...",
		"test_status":         projectPatchQueueTestStatusPassed,
		"test_exit_code":      0,
		"test_output_summary": "ok",
	})
	if result == nil || result.IsError {
		t.Fatalf("expected CAS success, got %+v", result)
	}
	if !strings.Contains(result.Output, headDigest) ||
		!strings.Contains(result.Output, `"no_git_mutation": true`) ||
		strings.Contains(result.Output, "head version") {
		t.Fatalf("unexpected CAS output: %s", result.Output)
	}
	if len(calls) != 3 ||
		calls[0] != "project.coordination.get" ||
		calls[1] != "project.patch_queue.operation_bind" ||
		calls[2] != "project.patch_queue.cas_record" {
		t.Fatalf("unexpected RPC calls: %+v", calls)
	}
}

func TestProjectPatchQueueCASToolRejectsDirtyCandidatePathsBeforeRPC(t *testing.T) {
	repoRoot, baseSHA, headSHA, baseDigest, _ := setupPatchQueueGitRepo(t)
	if err := os.WriteFile(filepath.Join(repoRoot, "web", "app.js"), []byte("dirty version\n"), 0o644); err != nil {
		t.Fatalf("dirty candidate file: %v", err)
	}

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeRPCRequest(t, r)
		calls++
		if req.Method != "project.coordination.get" {
			t.Fatalf("unexpected method %q", req.Method)
		}
		writeRPCResult(w, req, controlledPatchQueueCoordinationResult(repoRoot, baseSHA, headSHA, map[string]any{
			"state":               "CLAIMED",
			"claimed_by":          "agent-alpha",
			"claim_token":         "claim-token-1",
			"repo_authority_mode": "repoauthority_controlled_queue",
			"pathset_json":        `{"paths":["web/app.js"]}`,
			"base_file_hashes":    map[string]string{"web/app.js": baseDigest},
			"context_digest":      "sha256:proposal-context",
			"repo_lease_id":       "lease-1",
			"lease_term":          1,
		}))
	}))
	defer server.Close()

	tool := NewProjectPatchQueueCASTool(NewRhizomeClient(server.URL, "token"), "ws", "agent-alpha", repoRoot)
	result := tool.Execute(context.Background(), map[string]any{
		"project_id":          "project-subpixel",
		"branch_id":           "branch-1",
		"test_command":        "go test ./...",
		"test_output_summary": "ok",
	})
	if result == nil || !result.IsError || !strings.Contains(result.Output, "candidate paths must be clean") {
		t.Fatalf("expected dirty checkout error, got %+v", result)
	}
	if calls != 1 {
		t.Fatalf("expected no operation/CAS RPC after dirty checkout, got %d calls", calls)
	}
}

func TestProjectPatchQueueCASToolRejectsNonHexTestDigest(t *testing.T) {
	_, err := projectPatchQueueCASTestEvidence(map[string]any{
		"test_command":       "go test ./...",
		"test_output_digest": "sha256:zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
	})
	if err == nil || !strings.Contains(err.Error(), "canonical sha256") {
		t.Fatalf("expected canonical sha256 error, got %v", err)
	}
}

func TestProjectPatchQueueCASBuildMarksAddedFile(t *testing.T) {
	repoRoot, baseSHA, headSHA, headDigest := setupPatchQueueGitRepoWithAddedFile(t)

	hashes, err := projectPatchQueueBaseFileHashes(context.Background(), repoRoot, baseSHA, headSHA, []string{"web/**"}, []string{"web/new.js"}, "")
	if err != nil {
		t.Fatalf("base hashes for added file: %v", err)
	}
	if len(hashes) != 0 {
		t.Fatalf("added-only candidate should not synthesize base hashes, got %+v", hashes)
	}

	result, candidateDigests, err := buildProjectPatchQueueCASEvidenceFromGitHead(context.Background(), repoRoot, ProjectPatchQueueItemRecord{
		BaseSHA:                baseSHA,
		HeadSHA:                headSHA,
		BaseFileHashes:         hashes,
		OperationID:            "op-add-1",
		OperationContextDigest: "sha256:operation-context",
	}, []string{"web/new.js"})
	if err != nil {
		t.Fatalf("build CAS add evidence: %v", err)
	}
	if result.Status != projectPatchQueueCASStatusApplied || len(result.Paths) != 1 {
		t.Fatalf("unexpected CAS add result: %+v", result)
	}
	path := result.Paths[0]
	if path.Path != "web/new.js" || path.ChangeKind != projectPatchQueueCASChangeAdd || path.BaseHash != "" || path.CurrentHash != "" || path.CandidateHash != headDigest {
		t.Fatalf("unexpected CAS add path: %+v", path)
	}
	if candidateDigests["web/new.js"] != headDigest {
		t.Fatalf("candidate digest map = %+v, want %s", candidateDigests, headDigest)
	}
}

func TestProjectPatchQueueConcreteCandidatePathsRejectsDeletedFiles(t *testing.T) {
	repoRoot, baseSHA, headSHA := setupPatchQueueGitRepoWithDeletedFile(t)

	_, err := projectPatchQueueConcreteCandidatePaths(context.Background(), repoRoot, baseSHA, headSHA, []string{"web/**"}, nil)
	if err == nil || !strings.Contains(err.Error(), "deletions are not supported") || !strings.Contains(err.Error(), "web/app.js") {
		t.Fatalf("expected explicit deletion rejection, got %v", err)
	}
}

func TestProjectPatchQueueConcreteCandidatePathsRejectsRenamedFiles(t *testing.T) {
	repoRoot, baseSHA, headSHA := setupPatchQueueGitRepoWithRenamedFile(t)

	_, err := projectPatchQueueConcreteCandidatePaths(context.Background(), repoRoot, baseSHA, headSHA, []string{"web/**"}, nil)
	if err == nil || !strings.Contains(err.Error(), "renames are not supported") || !strings.Contains(err.Error(), "web/app.js -> web/new.js") {
		t.Fatalf("expected explicit rename rejection, got %v", err)
	}
}

func TestProjectPatchQueueBaseFileHashesRejectsSuppliedHashForAddedFile(t *testing.T) {
	repoRoot, baseSHA, headSHA, _ := setupPatchQueueGitRepoWithAddedFile(t)

	_, err := projectPatchQueueBaseFileHashes(context.Background(), repoRoot, baseSHA, headSHA, []string{"web/**"}, []string{"web/new.js"}, `{"web/new.js":"sha256:fake-base"}`)
	if err == nil || !strings.Contains(err.Error(), "supplied for an added path") {
		t.Fatalf("expected supplied base hash for added file to fail, got %v", err)
	}
}

func TestProjectPatchQueueGitObjectExistsRejectsInvalidRef(t *testing.T) {
	repoRoot, _, _, _, _ := setupPatchQueueGitRepo(t)

	_, err := projectPatchQueueGitObjectExists(context.Background(), repoRoot, "missing-ref", "web/app.js")
	if err == nil || !strings.Contains(err.Error(), "not a valid tree-ish") {
		t.Fatalf("expected invalid ref to fail hard, got %v", err)
	}
}

func setupPatchQueueGitRepo(t *testing.T) (repoRoot, baseSHA, headSHA, baseDigest, headDigest string) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	dir := t.TempDir()
	runTestGit(t, dir, "init")
	runTestGit(t, dir, "config", "user.email", "agent@example.test")
	runTestGit(t, dir, "config", "user.name", "Agent Test")
	runTestGit(t, dir, "config", "core.autocrlf", "false")
	if err := os.MkdirAll(filepath.Join(dir, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	const baseContent = "base version\n"
	if err := os.WriteFile(filepath.Join(dir, "web", "app.js"), []byte(baseContent), 0o644); err != nil {
		t.Fatalf("write base file: %v", err)
	}
	runTestGit(t, dir, "add", "web/app.js")
	runTestGit(t, dir, "commit", "--no-gpg-sign", "-m", "base")
	baseSHA = testGitOutput(t, dir, "rev-parse", "HEAD")

	const headContent = "head version\n"
	if err := os.WriteFile(filepath.Join(dir, "web", "app.js"), []byte(headContent), 0o644); err != nil {
		t.Fatalf("write head file: %v", err)
	}
	runTestGit(t, dir, "add", "web/app.js")
	runTestGit(t, dir, "commit", "--no-gpg-sign", "-m", "head")
	headSHA = testGitOutput(t, dir, "rev-parse", "HEAD")

	return dir, baseSHA, headSHA, projectPatchQueueMaterializationContentDigest(baseContent), projectPatchQueueMaterializationContentDigest(headContent)
}

func setupPatchQueueGitRepoWithAddedFile(t *testing.T) (repoRoot, baseSHA, headSHA, headDigest string) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	dir := t.TempDir()
	runTestGit(t, dir, "init")
	runTestGit(t, dir, "config", "user.email", "agent@example.test")
	runTestGit(t, dir, "config", "user.name", "Agent Test")
	runTestGit(t, dir, "config", "core.autocrlf", "false")
	if err := os.MkdirAll(filepath.Join(dir, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "web", "app.js"), []byte("base version\n"), 0o644); err != nil {
		t.Fatalf("write base file: %v", err)
	}
	runTestGit(t, dir, "add", "web/app.js")
	runTestGit(t, dir, "commit", "--no-gpg-sign", "-m", "base")
	baseSHA = testGitOutput(t, dir, "rev-parse", "HEAD")

	const addedContent = "new file\n"
	if err := os.WriteFile(filepath.Join(dir, "web", "new.js"), []byte(addedContent), 0o644); err != nil {
		t.Fatalf("write added file: %v", err)
	}
	runTestGit(t, dir, "add", "web/new.js")
	runTestGit(t, dir, "commit", "--no-gpg-sign", "-m", "add file")
	headSHA = testGitOutput(t, dir, "rev-parse", "HEAD")
	return dir, baseSHA, headSHA, projectPatchQueueMaterializationContentDigest(addedContent)
}

func setupPatchQueueGitRepoWithDeletedFile(t *testing.T) (repoRoot, baseSHA, headSHA string) {
	t.Helper()
	repoRoot, baseSHA, _, _, _ = setupPatchQueueGitRepo(t)
	if err := os.Remove(filepath.Join(repoRoot, "web", "app.js")); err != nil {
		t.Fatalf("delete app.js: %v", err)
	}
	runTestGit(t, repoRoot, "add", "-A", "web/app.js")
	runTestGit(t, repoRoot, "commit", "--no-gpg-sign", "-m", "delete file")
	headSHA = testGitOutput(t, repoRoot, "rev-parse", "HEAD")
	return repoRoot, baseSHA, headSHA
}

func setupPatchQueueGitRepoWithRenamedFile(t *testing.T) (repoRoot, baseSHA, headSHA string) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	dir := t.TempDir()
	runTestGit(t, dir, "init")
	runTestGit(t, dir, "config", "user.email", "agent@example.test")
	runTestGit(t, dir, "config", "user.name", "Agent Test")
	runTestGit(t, dir, "config", "core.autocrlf", "false")
	if err := os.MkdirAll(filepath.Join(dir, "web"), 0o755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "web", "app.js"), []byte("base version\n"), 0o644); err != nil {
		t.Fatalf("write base file: %v", err)
	}
	runTestGit(t, dir, "add", "web/app.js")
	runTestGit(t, dir, "commit", "--no-gpg-sign", "-m", "base")
	baseSHA = testGitOutput(t, dir, "rev-parse", "HEAD")
	runTestGit(t, dir, "mv", "web/app.js", "web/new.js")
	runTestGit(t, dir, "commit", "--no-gpg-sign", "-m", "rename file")
	headSHA = testGitOutput(t, dir, "rev-parse", "HEAD")
	return dir, baseSHA, headSHA
}

func testGitOutput(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, string(out))
	}
	return strings.TrimSpace(string(out))
}

func controlledPatchQueueCoordinationResult(checkoutPath, baseSHA, headSHA string, itemOverrides map[string]any) map[string]any {
	result := branchReviewCoordinationResult(branchReviewCoordinationInput{
		AgentID:        "agent-alpha",
		BranchStatus:   "READY_FOR_REVIEW",
		WriteScopeJSON: `{"paths":["web/app.js"]}`,
		CheckoutPath:   checkoutPath,
	})
	coordination := result["coordination"].(map[string]any)
	branches := coordination["branches"].([]map[string]any)
	branches[0]["review_doc_key"] = "project.project-subpixel.branch.branch-1.review"
	branches[0]["base_sha"] = baseSHA
	branches[0]["head_sha"] = headSHA
	coordination["branches"] = branches
	coordination["roles"] = []map[string]any{{
		"role_id":      "role-integrator",
		"workspace_id": "ws",
		"project_id":   "project-subpixel",
		"agent_id":     "agent-alpha",
		"role_type":    "INTEGRATOR",
		"status":       "ACTIVE",
	}}
	if itemOverrides != nil {
		coordination["patch_queue_items"] = []map[string]any{
			controlledPatchQueueItem(baseSHA, headSHA, "", itemOverrides),
		}
	}
	return result
}

func controlledPatchQueueItem(baseSHA, headSHA, baseDigest string, overrides map[string]any) map[string]any {
	item := patchQueueLifecycleItem(map[string]any{
		"repo_authority_mode":        "repoauthority_controlled_queue",
		"pathset":                    []string{"web/app.js"},
		"pathset_json":               `{"paths":["web/app.js"]}`,
		"base_sha":                   baseSHA,
		"head_sha":                   headSHA,
		"base_file_hashes":           map[string]string{"web/app.js": baseDigest},
		"context_digest":             "sha256:proposal-context",
		"repo_lease_id":              "lease-1",
		"lease_term":                 1,
		"task_id":                    "task-1",
		"session_id":                 "session-1",
		"run_id":                     "run-1",
		"agent_id":                   "agent-alpha",
		"principal_type":             "agent",
		"principal_id":               "agent-alpha",
		"capability_snapshot_id":     "snapshot-1",
		"capability_snapshot_schema": daemonCapabilitySnapshotSchema,
	})
	if strings.TrimSpace(baseDigest) == "" {
		delete(item, "base_file_hashes")
	}
	for key, value := range overrides {
		item[key] = value
	}
	return item
}
