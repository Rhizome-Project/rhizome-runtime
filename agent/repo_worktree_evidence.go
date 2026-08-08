package main

import (
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const (
	repoWorktreeEvidenceSchema   = "repo_worktree_evidence.v1"
	repoMergeQueueEvidenceSchema = "repo_merge_queue_evidence.v1"
	repoMutationActivationSchema = "repo_mutation_activation_gates.v1"
	repoMutationActivationStatus = "blocked"
	repoAuthorityModePatchOnly   = "patch_only_temp_repo"
)

func buildRepoWorktreeEvidence(cfg RuntimeConfig, now time.Time) map[string]any {
	workdir := strings.TrimSpace(cfg.Workdir)
	evidence := map[string]any{
		"schema":                     repoWorktreeEvidenceSchema,
		"workspace_id":               strings.TrimSpace(cfg.WorkspaceID),
		"agent_id":                   strings.TrimSpace(cfg.AgentID),
		"workdir":                    workdir,
		"repo_authority_mode":        repoAuthorityModePatchOnly,
		"no_direct_merge":            true,
		"mutation_allowed":           false,
		"mutation_activation_schema": repoMutationActivationSchema,
		"mutation_activation_status": repoMutationActivationStatus,
		"probed_at":                  formatRepoEvidenceTime(now),
	}
	if workdir == "" {
		evidence["status"] = "error"
		evidence["error"] = "workdir is empty"
		return evidence
	}

	repoRoot, err := runGitReadOnly(workdir, "rev-parse", "--show-toplevel")
	if err != nil {
		evidence["status"] = repoWorktreeProbeStatus(err)
		evidence["error"] = err.Error()
		return evidence
	}
	gitDir, gitDirErr := runGitReadOnly(workdir, "rev-parse", "--absolute-git-dir")
	commonDir, commonDirErr := runGitReadOnly(workdir, "rev-parse", "--git-common-dir")
	branch, branchErr := runGitReadOnly(workdir, "rev-parse", "--abbrev-ref", "HEAD")
	head, headErr := runGitReadOnly(workdir, "rev-parse", "HEAD")
	status, statusErr := runGitReadOnly(workdir, "status", "--porcelain")

	evidence["repo_root"] = filepath.Clean(repoRoot)
	if gitDirErr == nil {
		evidence["git_dir"] = filepath.Clean(gitDir)
	}
	if commonDirErr == nil {
		evidence["git_common_dir"] = cleanGitCommonDir(repoRoot, commonDir)
	}
	if branchErr == nil {
		evidence["branch"] = strings.TrimSpace(branch)
	}
	if headErr == nil {
		evidence["head"] = strings.TrimSpace(head)
	}
	if statusErr != nil {
		evidence["status"] = "error"
		evidence["error"] = statusErr.Error()
		return evidence
	}

	dirty := strings.TrimSpace(status) != ""
	evidence["dirty"] = dirty
	evidence["is_worktree"] = isGitLinkedWorktree(repoRoot, gitDir, commonDir)
	if dirty {
		evidence["status"] = "dirty"
	} else {
		evidence["status"] = "ok"
	}
	return evidence
}

func buildRepoMergeQueueEvidence(cfg RuntimeConfig, task WorkspaceTaskRecord, session AgentSessionStateRecord, runID string) map[string]any {
	return map[string]any{
		"schema":                           repoMergeQueueEvidenceSchema,
		"workspace_id":                     strings.TrimSpace(cfg.WorkspaceID),
		"agent_id":                         strings.TrimSpace(cfg.AgentID),
		"task_id":                          strings.TrimSpace(task.TaskID),
		"session_id":                       strings.TrimSpace(session.SessionID),
		"run_id":                           strings.TrimSpace(runID),
		"repo_authority_mode":              repoAuthorityModePatchOnly,
		"state":                            "evidence_only",
		"queue_id":                         "",
		"item_id":                          "",
		"no_direct_merge":                  true,
		"mutation_allowed":                 false,
		"mutation_activation_schema":       repoMutationActivationSchema,
		"mutation_activation_status":       repoMutationActivationStatus,
		"retry_rollback_evidence_required": true,
		"recovery_decision_contract":       "repo_patch_queue_recovery_decision.v1",
		"rollback_evidence_contract":       "repo_patch_queue_rollback_evidence.v1",
		"worktree_write_authority":         "disabled_until_merge_queue_evidence",
		"reviewer_mesh_permission":         "advisory_only",
	}
}

func runGitReadOnly(workdir string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", workdir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if msg == "" {
			msg = err.Error()
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			msg = "git command timed out"
		}
		return "", errors.New(msg)
	}
	return strings.TrimSpace(string(out)), nil
}

func repoWorktreeProbeStatus(err error) string {
	if err == nil {
		return "ok"
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	if strings.Contains(msg, "not a git repository") || strings.Contains(msg, "not a git repo") {
		return "not_git"
	}
	return "error"
}

func cleanGitCommonDir(repoRoot, commonDir string) string {
	commonDir = strings.TrimSpace(commonDir)
	if commonDir == "" {
		return ""
	}
	if filepath.IsAbs(commonDir) {
		return filepath.Clean(commonDir)
	}
	return filepath.Clean(filepath.Join(repoRoot, commonDir))
}

func isGitLinkedWorktree(repoRoot, gitDir, commonDir string) bool {
	repoRoot = filepath.Clean(strings.TrimSpace(repoRoot))
	gitDir = filepath.Clean(strings.TrimSpace(gitDir))
	commonDir = cleanGitCommonDir(repoRoot, commonDir)
	if repoRoot == "." || gitDir == "." || commonDir == "" {
		return false
	}
	defaultGitDir := filepath.Clean(filepath.Join(repoRoot, ".git"))
	return !strings.EqualFold(gitDir, defaultGitDir) || !strings.EqualFold(commonDir, gitDir)
}

func formatRepoEvidenceTime(t time.Time) string {
	if t.IsZero() {
		t = time.Now().UTC()
	}
	return t.UTC().Format(time.RFC3339Nano)
}
