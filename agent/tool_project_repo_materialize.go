package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

type ProjectRepoMaterializeTool struct {
	client         *RhizomeClient
	workspaceID    string
	agentID        string
	ownerUserID    string
	workdir        string
	runtimeBinding func() AgentRuntimeBinding
}

func NewProjectRepoMaterializeTool(client *RhizomeClient, workspaceID, agentID, ownerUserID, workdir string) *ProjectRepoMaterializeTool {
	return &ProjectRepoMaterializeTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
		ownerUserID: strings.TrimSpace(ownerUserID),
		workdir:     strings.TrimSpace(workdir),
	}
}

func (t *ProjectRepoMaterializeTool) Name() string { return "project_repo_materialize" }

func (t *ProjectRepoMaterializeTool) WithRuntimeBinding(provider func() AgentRuntimeBinding) *ProjectRepoMaterializeTool {
	if t != nil {
		t.runtimeBinding = provider
	}
	return t
}

func (t *ProjectRepoMaterializeTool) Description() string {
	return "Create or reuse a local bare canonical git repository inside this agent workdir, seed its default branch, and register it as READY repository evidence."
}

func (t *ProjectRepoMaterializeTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"project_id": map[string]any{
				"type":        "string",
				"description": "Project id returned by project_bootstrap.",
			},
			"repo_id": map[string]any{
				"type":        "string",
				"description": "Optional repository id. Defaults to an existing canonical repo id or a deterministic project-scoped id.",
			},
			"repo_name": map[string]any{
				"type":        "string",
				"description": "Optional local repository name. Defaults to the existing repo name or the project id.",
			},
			"local_remote_path": map[string]any{
				"type":        "string",
				"description": "Optional bare repository destination inside this agent workdir. Relative paths are resolved from the agent workdir. Defaults to a compact project-remotes/p-<project-hash>/<repo>.git path.",
			},
			"default_branch": map[string]any{
				"type":        "string",
				"description": "Default branch to seed and register. Defaults to existing repo/profile branch or main.",
			},
		},
		"required": []string{"project_id"},
	}
}

func (t *ProjectRepoMaterializeTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "project_repo_materialize is disabled: missing client, workspace, or agent identity", IsError: true}
	}
	if strings.TrimSpace(t.workdir) == "" {
		return &ToolResult{Output: "project_repo_materialize requires an agent workdir", IsError: true}
	}
	projectID, err := resolveProjectToolActiveProjectID("project_repo_materialize", t.workspaceID, projectToolRefArg(args, "project_id"), t.runtimeBinding)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}

	coordination, err := t.client.GetProjectCoordination(ctx, t.workspaceID, projectID)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_repo_materialize failed reading project coordination for %s: %v", projectID, err), IsError: true}
	}
	explicitLocalRemotePath := strings.TrimSpace(stringArg(args, "local_remote_path"))
	existing, hasExisting := selectProjectRepoMaterializeCanonicalRepo(coordination.Repositories, strings.TrimSpace(stringArg(args, "repo_id")))
	repoID := strings.TrimSpace(stringArg(args, "repo_id"))
	if repoID == "" && hasExisting {
		repoID = existing.RepoID
	}
	repoName := firstNonEmpty(strings.TrimSpace(stringArg(args, "repo_name")), strings.TrimSpace(existing.Name), sanitizePathComponent(projectID))
	if repoID == "" {
		repoID = projectRepoRegisterID(projectID, t.agentID, repoName)
	}
	defaultBranch := firstNonEmpty(strings.TrimSpace(stringArg(args, "default_branch")), strings.TrimSpace(existing.DefaultBranch), strings.TrimSpace(coordination.Profile.RepoDefaultBranch), "main")
	existingReadyRemoteUsable := false
	existingReadyLocalRemoteMissing := false
	if hasExisting && projectClaimRepositoryReady(existing) && explicitLocalRemotePath == "" {
		existingReadyRemoteUsable = projectRepoMaterializeReadyRemoteUsable(ctx, existing)
		if !existingReadyRemoteUsable {
			_, existingReadyLocalRemoteMissing = projectRepoMaterializeLocalRemotePath(existing)
			if !existingReadyLocalRemoteMissing && projectRepoMaterializeDeclaresLocalRemote(existing) {
				existingReadyLocalRemoteMissing = true
			}
		}
	}
	if hasExisting && projectClaimRepositoryReady(existing) && explicitLocalRemotePath == "" && existingReadyRemoteUsable {
		payload := map[string]any{
			"project_id":                 projectID,
			"repo_id":                    repoID,
			"repo_name":                  repoName,
			"repo_status":                strings.TrimSpace(existing.RepoStatus),
			"remote_url":                 strings.TrimSpace(existing.RemoteURL),
			"remote_kind":                strings.TrimSpace(existing.RemoteKind),
			"default_branch":             defaultBranch,
			"local_repo_created":         false,
			"seed_commit_created":        false,
			"reused_existing_record":     true,
			"canonical_remote_preserved": true,
			"guidance":                   "Canonical repository is already READY. Use project_checkout_materialize for per-agent clones/branches; this call did not move or rewrite the canonical remote.",
		}
		raw, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			return &ToolResult{Output: fmt.Sprintf("project_repo_materialize reused READY repository %s", repoID)}
		}
		return &ToolResult{Output: string(raw)}
	}

	localRemotePath, err := projectRepoMaterializeResolveLocalRemotePath(t.workdir, firstNonEmpty(explicitLocalRemotePath, projectRepoMaterializeDefaultPath(t.workdir, projectID, repoName)))
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_repo_materialize invalid local_remote_path: %v", err), IsError: true}
	}
	if err := ensureProjectCheckoutMaterializePath(t.workdir, localRemotePath); err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_repo_materialize invalid local_remote_path: %v", err), IsError: true}
	}

	created, seeded, err := materializeLocalBareGitRepo(ctx, t.workdir, localRemotePath, defaultBranch, projectID)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_repo_materialize git init failed: %v", err), IsError: true}
	}
	remoteURL, err := projectRepoMaterializeFileURL(localRemotePath)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_repo_materialize failed building file remote URL: %v", err), IsError: true}
	}

	register := NewProjectRepoRegisterTool(t.client, t.workspaceID, t.agentID, t.ownerUserID).WithRuntimeBinding(t.runtimeBinding)
	registerResult := register.Execute(ctx, map[string]any{
		"project_id":               projectID,
		"repo_id":                  repoID,
		"remote_url":               remoteURL,
		"remote_kind":              "local",
		"owner":                    t.agentID,
		"name":                     repoName,
		"default_branch":           defaultBranch,
		"repo_status":              "READY",
		"is_canonical":             true,
		"request_human_if_missing": false,
	})
	if registerResult == nil {
		return &ToolResult{Output: "project_repo_materialize registered repository returned nil result", IsError: true}
	}
	if registerResult.IsError {
		return &ToolResult{Output: fmt.Sprintf("project_repo_materialize initialized local repository but registration failed: %s", registerResult.Output), IsError: true}
	}

	payload := map[string]any{
		"project_id":             projectID,
		"repo_id":                repoID,
		"repo_name":              repoName,
		"repo_status":            "READY",
		"remote_url":             remoteURL,
		"remote_kind":            "local",
		"local_remote_path":      localRemotePath,
		"default_branch":         defaultBranch,
		"local_repo_created":     created,
		"seed_commit_created":    seeded,
		"reused_existing_record": hasExisting,
		"registration_result":    registerResult.Output,
		"guidance":               "Canonical local repository is READY. Use project_checkout_materialize for per-agent clones/branches before implementation.",
	}
	if existingReadyLocalRemoteMissing {
		payload["ready_local_remote_recreated"] = true
		payload["previous_remote_url"] = strings.TrimSpace(existing.RemoteURL)
		payload["guidance"] = "Canonical repository infrastructure was recreated because existing READY local remote evidence pointed at a missing or invalid bare repository. Prior branch refs/objects were not recovered by this tool; if an accepted candidate head is needed, locate or regenerate that branch before integration/review."
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_repo_materialize completed for %s", localRemotePath)}
	}
	return &ToolResult{Output: string(raw)}
}

func selectProjectRepoMaterializeCanonicalRepo(repos []ProjectRepositoryRecord, repoID string) (ProjectRepositoryRecord, bool) {
	repoID = strings.TrimSpace(repoID)
	for _, repo := range repos {
		if repoID != "" && strings.EqualFold(strings.TrimSpace(repo.RepoID), repoID) {
			return repo, true
		}
	}
	for _, repo := range repos {
		if repo.IsCanonical && !strings.EqualFold(strings.TrimSpace(repo.RepoStatus), "ARCHIVED") {
			return repo, true
		}
	}
	return ProjectRepositoryRecord{}, false
}

func projectRepoMaterializeDefaultPath(workdir, projectID, repoName string) string {
	name := compactRefSegment("repo", strings.TrimSuffix(firstNonEmpty(repoName, projectID, "repo"), ".git"))
	if !strings.HasSuffix(strings.ToLower(name), ".git") {
		name += ".git"
	}
	return filepath.Join(workdir, "project-remotes", "p-"+shortRefHash(firstNonEmpty(projectID, "project")), name)
}

func projectRepoMaterializeReadyRemoteUsable(ctx context.Context, repo ProjectRepositoryRecord) bool {
	localPath, isLocal := projectRepoMaterializeLocalRemotePath(repo)
	if !isLocal {
		if projectRepoMaterializeDeclaresLocalRemote(repo) {
			return false
		}
		return true
	}
	return projectRepoMaterializeBareRepoReady(ctx, localPath)
}

func projectRepoMaterializeDeclaresLocalRemote(repo ProjectRepositoryRecord) bool {
	remoteURL := strings.TrimSpace(repo.RemoteURL)
	lowerRemote := strings.ToLower(remoteURL)
	return strings.EqualFold(strings.TrimSpace(repo.RemoteKind), "local") ||
		strings.HasPrefix(lowerRemote, "file://") ||
		filepath.IsAbs(remoteURL) ||
		looksLikeWindowsPath(remoteURL) ||
		strings.HasPrefix(remoteURL, ".") ||
		strings.HasPrefix(remoteURL, "/")
}

func projectRepoMaterializeLocalRemotePath(repo ProjectRepositoryRecord) (string, bool) {
	remoteURL := strings.TrimSpace(repo.RemoteURL)
	if remoteURL == "" {
		return "", false
	}
	lowerRemote := strings.ToLower(remoteURL)
	if strings.HasPrefix(lowerRemote, "file://") {
		path, err := projectRepoMaterializePathFromFileURL(remoteURL)
		if err != nil {
			return "", true
		}
		return path, true
	}
	if strings.EqualFold(strings.TrimSpace(repo.RemoteKind), "local") {
		if filepath.IsAbs(remoteURL) || looksLikeWindowsPath(remoteURL) {
			return filepath.Clean(remoteURL), true
		}
		if strings.HasPrefix(remoteURL, ".") || strings.HasPrefix(remoteURL, "/") {
			return filepath.Clean(filepath.FromSlash(remoteURL)), true
		}
	}
	return "", false
}

func projectRepoMaterializeResolveLocalRemotePath(workdir, localRemotePath string) (string, error) {
	base, err := filepath.Abs(strings.TrimSpace(workdir))
	if err != nil {
		return "", err
	}
	base = filepath.Clean(base)
	localRemotePath = strings.TrimSpace(localRemotePath)
	if localRemotePath == "" {
		return "", fmt.Errorf("local remote path is empty")
	}
	if strings.HasPrefix(strings.ToLower(localRemotePath), "file://") {
		path, err := projectRepoMaterializePathFromFileURL(localRemotePath)
		if err != nil {
			return "", err
		}
		if filepath.IsAbs(path) || looksLikeWindowsPath(path) {
			return filepath.Clean(path), nil
		}
		return filepath.Clean(filepath.Join(base, path)), nil
	}
	cleaned := filepath.Clean(filepath.FromSlash(localRemotePath))
	if filepath.IsAbs(cleaned) || looksLikeWindowsPath(cleaned) {
		return cleaned, nil
	}
	return filepath.Clean(filepath.Join(base, cleaned)), nil
}

func projectRepoMaterializePathFromFileURL(remoteURL string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(remoteURL))
	if err != nil {
		return "", err
	}
	path := parsed.Path
	if parsed.Host != "" {
		return filepath.Clean(`\\` + parsed.Host + filepath.FromSlash(path)), nil
	}
	if len(path) >= 3 && path[0] == '/' && path[2] == ':' {
		path = path[1:]
	}
	return filepath.Clean(filepath.FromSlash(path)), nil
}

func projectRepoMaterializeBareRepoReady(ctx context.Context, localRemotePath string) bool {
	localRemotePath = strings.TrimSpace(localRemotePath)
	if localRemotePath == "" {
		return false
	}
	info, err := os.Stat(localRemotePath)
	if err != nil || !info.IsDir() {
		return false
	}
	out, err := gitBareCombined(ctx, localRemotePath, "rev-parse", "--is-bare-repository")
	return err == nil && strings.TrimSpace(out) == "true"
}

func materializeLocalBareGitRepo(ctx context.Context, workdir, localRemotePath, defaultBranch, projectID string) (bool, bool, error) {
	var err error
	localRemotePath, err = projectRepoMaterializeResolveLocalRemotePath(workdir, localRemotePath)
	if err != nil {
		return false, false, err
	}
	if err := ensureProjectCheckoutMaterializePath(workdir, localRemotePath); err != nil {
		return false, false, err
	}
	defaultBranch = firstNonEmpty(strings.TrimSpace(defaultBranch), "main")
	created := false
	info, err := os.Stat(localRemotePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return false, false, err
		}
		if err := os.MkdirAll(filepath.Dir(localRemotePath), 0o755); err != nil {
			return false, false, err
		}
		out, err := gitCommand(ctx, "init", "--bare", localRemotePath).CombinedOutput()
		if err != nil {
			return false, false, fmt.Errorf("git init --bare failed: %w\n%s", err, string(out))
		}
		created = true
	} else if !info.IsDir() {
		return false, false, fmt.Errorf("destination %q exists and is not a directory", localRemotePath)
	}
	if out, err := gitBareCombined(ctx, localRemotePath, "rev-parse", "--is-bare-repository"); err != nil || strings.TrimSpace(out) != "true" {
		if err != nil {
			return created, false, fmt.Errorf("destination %q is not a bare git repository: %w\n%s", localRemotePath, err, out)
		}
		return created, false, fmt.Errorf("destination %q is not a bare git repository", localRemotePath)
	}
	if out, err := gitBareCombined(ctx, localRemotePath, "symbolic-ref", "HEAD", "refs/heads/"+defaultBranch); err != nil {
		return created, false, fmt.Errorf("git set HEAD failed: %w\n%s", err, out)
	}
	if _, err := gitBareCombined(ctx, localRemotePath, "rev-parse", "--verify", "refs/heads/"+defaultBranch); err == nil {
		return created, false, nil
	}
	if err := os.MkdirAll(filepath.Join(workdir, ".tmp"), 0o755); err != nil {
		return created, false, err
	}
	seedDir, err := os.MkdirTemp(filepath.Join(workdir, ".tmp"), "project-repo-seed-*")
	if err != nil {
		return created, false, err
	}
	defer os.RemoveAll(seedDir)
	if out, err := gitCommand(ctx, "init", seedDir).CombinedOutput(); err != nil {
		return created, false, fmt.Errorf("git init seed worktree failed: %w\n%s", err, string(out))
	}
	if out, err := gitCombined(ctx, seedDir, "checkout", "-B", defaultBranch); err != nil {
		return created, false, fmt.Errorf("git checkout seed branch failed: %w\n%s", err, out)
	}
	_ = gitConfigLocal(ctx, seedDir, "user.email", "rhizome-agent@example.invalid")
	_ = gitConfigLocal(ctx, seedDir, "user.name", "Rhizome Agent")
	readme := fmt.Sprintf("# %s\n\nSeed commit for Rhizome project %s.\n", firstNonEmpty(projectID, "Rhizome Project"), projectID)
	if err := os.WriteFile(filepath.Join(seedDir, "README.md"), []byte(readme), 0o644); err != nil {
		return created, false, err
	}
	if out, err := gitCombined(ctx, seedDir, "add", "README.md"); err != nil {
		return created, false, fmt.Errorf("git add seed README failed: %w\n%s", err, out)
	}
	if out, err := gitCombined(ctx, seedDir, "commit", "-m", "Initial project seed"); err != nil {
		return created, false, fmt.Errorf("git commit seed README failed: %w\n%s", err, out)
	}
	seedRemoteURL, err := projectRepoMaterializeFileURL(localRemotePath)
	if err != nil {
		return created, false, fmt.Errorf("build seed remote URL failed: %w", err)
	}
	if out, err := gitCombined(ctx, seedDir, "remote", "add", "origin", seedRemoteURL); err != nil {
		return created, false, fmt.Errorf("git remote add seed origin failed: %w\n%s", err, out)
	}
	if out, err := gitCombined(ctx, seedDir, "push", "origin", defaultBranch); err != nil {
		return created, false, fmt.Errorf("git push seed branch failed: %w\n%s", err, out)
	}
	return created, true, nil
}

func gitBareCombined(ctx context.Context, gitDir string, args ...string) (string, error) {
	cmdArgs := append([]string{"--git-dir", strings.TrimSpace(gitDir)}, args...)
	out, err := gitCommand(ctx, cmdArgs...).CombinedOutput()
	return string(out), err
}

func gitConfigLocal(ctx context.Context, localPath, key, value string) error {
	_, err := gitCombined(ctx, localPath, "config", key, value)
	return err
}

func projectRepoMaterializeFileURL(localPath string) (string, error) {
	abs, err := filepath.Abs(strings.TrimSpace(localPath))
	if err != nil {
		return "", err
	}
	slash := filepath.ToSlash(abs)
	if strings.HasPrefix(slash, "/") {
		return "file://" + slash, nil
	}
	return "file:///" + slash, nil
}
