package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestRepoWorktreeEvidenceNonGitStaysEvidenceOnly(t *testing.T) {
	cfg := RuntimeConfig{
		Workdir:     t.TempDir(),
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
	}
	evidence := buildRepoWorktreeEvidence(cfg, time.Date(2026, 4, 24, 9, 0, 0, 0, time.UTC))

	if got := stringMapField(t, evidence, "schema"); got != repoWorktreeEvidenceSchema {
		t.Fatalf("schema = %q", got)
	}
	if got := stringMapField(t, evidence, "status"); got != "not_git" {
		t.Fatalf("status = %q, want not_git; evidence=%+v", got, evidence)
	}
	if got := stringMapField(t, evidence, "repo_authority_mode"); got != repoAuthorityModePatchOnly {
		t.Fatalf("repo authority mode = %q", got)
	}
	if got := boolMapField(t, evidence, "no_direct_merge"); !got {
		t.Fatalf("no_direct_merge = false")
	}
	if got := boolMapField(t, evidence, "mutation_allowed"); got {
		t.Fatalf("mutation_allowed = true")
	}
	if got := stringMapField(t, evidence, "mutation_activation_schema"); got != repoMutationActivationSchema {
		t.Fatalf("mutation activation schema = %q", got)
	}
	if got := stringMapField(t, evidence, "mutation_activation_status"); got != repoMutationActivationStatus {
		t.Fatalf("mutation activation status = %q", got)
	}
}

func TestRepoWorktreeEvidenceDirtyGitCheckoutIsDegradedNotMutable(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	dir := t.TempDir()
	runTestGit(t, dir, "init")
	if err := os.WriteFile(filepath.Join(dir, "owned.go"), []byte("package owned\n"), 0o644); err != nil {
		t.Fatalf("write dirty file: %v", err)
	}

	cfg := RuntimeConfig{
		Workdir:     dir,
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
	}
	evidence := buildRepoWorktreeEvidence(cfg, time.Date(2026, 4, 24, 9, 0, 0, 0, time.UTC))
	if got := stringMapField(t, evidence, "status"); got != "dirty" {
		t.Fatalf("status = %q, want dirty; evidence=%+v", got, evidence)
	}
	if got := boolMapField(t, evidence, "dirty"); !got {
		t.Fatalf("dirty = false; evidence=%+v", evidence)
	}
	if got := boolMapField(t, evidence, "mutation_allowed"); got {
		t.Fatalf("dirty evidence must not enable mutation: %+v", evidence)
	}
}

func TestRepoWorktreeEvidenceCleanGitCheckoutIsStillNonMutable(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	dir := t.TempDir()
	runTestGit(t, dir, "init")
	runTestGit(t, dir, "config", "user.email", "agent@example.test")
	runTestGit(t, dir, "config", "user.name", "Agent Test")
	if err := os.WriteFile(filepath.Join(dir, "owned.go"), []byte("package owned\n"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	runTestGit(t, dir, "add", "owned.go")
	runTestGit(t, dir, "commit", "--no-gpg-sign", "-m", "init")

	cfg := RuntimeConfig{
		Workdir:     dir,
		WorkspaceID: "ws-1",
		AgentID:     "agent-1",
	}
	evidence := buildRepoWorktreeEvidence(cfg, time.Date(2026, 4, 24, 9, 0, 0, 0, time.UTC))
	if got := stringMapField(t, evidence, "status"); got != "ok" {
		t.Fatalf("status = %q, want ok; evidence=%+v", got, evidence)
	}
	if got := boolMapField(t, evidence, "dirty"); got {
		t.Fatalf("dirty = true; evidence=%+v", evidence)
	}
	if got := boolMapField(t, evidence, "mutation_allowed"); got {
		t.Fatalf("clean checkout evidence must not enable mutation: %+v", evidence)
	}
	if got := boolMapField(t, evidence, "no_direct_merge"); !got {
		t.Fatalf("no_direct_merge = false")
	}
}

func TestRepoMergeQueueEvidencePinsPatchOnlyBoundary(t *testing.T) {
	cfg := RuntimeConfig{WorkspaceID: "ws-1", AgentID: "agent-1"}
	task := WorkspaceTaskRecord{TaskID: "task-1"}
	session := AgentSessionStateRecord{SessionID: "session-1"}
	evidence := buildRepoMergeQueueEvidence(cfg, task, session, "run-1")

	if got := stringMapField(t, evidence, "schema"); got != repoMergeQueueEvidenceSchema {
		t.Fatalf("schema = %q", got)
	}
	if got := stringMapField(t, evidence, "repo_authority_mode"); got != repoAuthorityModePatchOnly {
		t.Fatalf("repo authority mode = %q", got)
	}
	if got := boolMapField(t, evidence, "no_direct_merge"); !got {
		t.Fatalf("no_direct_merge = false")
	}
	if got := boolMapField(t, evidence, "mutation_allowed"); got {
		t.Fatalf("merge queue evidence must not enable mutation")
	}
	if got := stringMapField(t, evidence, "mutation_activation_schema"); got != repoMutationActivationSchema {
		t.Fatalf("mutation activation schema = %q", got)
	}
	if got := stringMapField(t, evidence, "mutation_activation_status"); got != repoMutationActivationStatus {
		t.Fatalf("mutation activation status = %q", got)
	}
	if got := boolMapField(t, evidence, "retry_rollback_evidence_required"); !got {
		t.Fatalf("retry_rollback_evidence_required = false")
	}
	if got := stringMapField(t, evidence, "rollback_evidence_contract"); got != "repo_patch_queue_rollback_evidence.v1" {
		t.Fatalf("rollback contract = %q", got)
	}
}

func runTestGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, string(out))
	}
}
