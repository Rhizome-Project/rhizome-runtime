package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

type ProjectRepoRegisterTool struct {
	client         *RhizomeClient
	workspaceID    string
	agentID        string
	ownerUserID    string
	runtimeBinding func() AgentRuntimeBinding
}

func NewProjectRepoRegisterTool(client *RhizomeClient, workspaceID, agentID, ownerUserID string) *ProjectRepoRegisterTool {
	return &ProjectRepoRegisterTool{
		client:      client,
		workspaceID: strings.TrimSpace(workspaceID),
		agentID:     strings.TrimSpace(agentID),
		ownerUserID: strings.TrimSpace(ownerUserID),
	}
}

func (t *ProjectRepoRegisterTool) Name() string { return "project_repo_register" }

func (t *ProjectRepoRegisterTool) WithRuntimeBinding(provider func() AgentRuntimeBinding) *ProjectRepoRegisterTool {
	if t != nil {
		t.runtimeBinding = provider
	}
	return t
}

func (t *ProjectRepoRegisterTool) Description() string {
	return "Register canonical project repository evidence or request operator repository materialization. It never clones, commits, pushes, merges, or mutates a local git checkout."
}

func (t *ProjectRepoRegisterTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"project_id": map[string]any{
				"type":        "string",
				"description": "Project id returned by project_bootstrap.",
			},
			"repo_id": map[string]any{
				"type":        "string",
				"description": "Optional stable repository record id. Leave blank for a deterministic project-scoped id.",
			},
			"remote_url": map[string]any{
				"type":        "string",
				"description": "Canonical git remote URL, if known. Do not pass credentials or secret material.",
			},
			"remote_kind": map[string]any{
				"type":        "string",
				"description": "Remote kind: github, gitlab, local, or unknown. Auto-detected when possible.",
			},
			"owner": map[string]any{
				"type":        "string",
				"description": "Repository owner or namespace. Auto-detected for common GitHub/GitLab URLs.",
			},
			"name": map[string]any{
				"type":        "string",
				"description": "Repository name. Auto-detected for common GitHub/GitLab URLs.",
			},
			"default_branch": map[string]any{
				"type":        "string",
				"description": "Default branch for agent work. Defaults to main.",
			},
			"integration_branch": map[string]any{
				"type":        "string",
				"description": "Integration branch for coordinated review or merge evidence.",
			},
			"credential_vault_entry_id": map[string]any{
				"type":        "string",
				"description": "Vault entry id for credentials. Never pass key, token, or password contents.",
			},
			"repo_status": map[string]any{
				"type":        "string",
				"description": "Repository status: MISSING, REQUESTED, CREATED, READY, BROKEN, or ARCHIVED. Defaults to CREATED when remote_url exists, otherwise REQUESTED. READY must be explicit.",
			},
			"is_canonical": map[string]any{
				"type":        "boolean",
				"description": "Whether this is the canonical project repository. Defaults true.",
			},
			"request_human_if_missing": map[string]any{
				"type":        "boolean",
				"description": "Create an operator request when repository materialization is still missing. Defaults true for missing/requested repository evidence.",
			},
			"request_summary": map[string]any{
				"type":        "string",
				"description": "Optional short operator-facing summary when a repository request is needed.",
			},
			"request_details": map[string]any{
				"type":        "string",
				"description": "Optional operator-facing detail when a repository request is needed.",
			},
		},
		"required": []string{"project_id"},
	}
}

func (t *ProjectRepoRegisterTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	if t.client == nil || t.workspaceID == "" || t.agentID == "" {
		return &ToolResult{Output: "project_repo_register is disabled: missing client, workspace, or agent identity", IsError: true}
	}
	projectID, err := resolveProjectToolActiveProjectID("project_repo_register", t.workspaceID, projectToolRefArg(args, "project_id"), t.runtimeBinding)
	if err != nil {
		return &ToolResult{Output: err.Error(), IsError: true}
	}
	remoteURL := strings.TrimSpace(stringArg(args, "remote_url"))
	owner := strings.TrimSpace(stringArg(args, "owner"))
	name := strings.TrimSpace(stringArg(args, "name"))
	parsedOwner, parsedName := parseProjectRepoOwnerName(remoteURL)
	if owner == "" {
		owner = parsedOwner
	}
	if name == "" {
		name = parsedName
	}
	remoteKind := normalizeProjectRepoRemoteKind(stringArg(args, "remote_kind"), remoteURL)
	rawDefaultBranch := strings.TrimSpace(stringArg(args, "default_branch"))
	defaultBranch := firstNonEmpty(rawDefaultBranch, "main")
	integrationBranch := strings.TrimSpace(stringArg(args, "integration_branch"))
	repoID := strings.TrimSpace(stringArg(args, "repo_id"))
	rawRepoStatus := strings.TrimSpace(stringArg(args, "repo_status"))
	credentialRef := strings.TrimSpace(stringArg(args, "credential_vault_entry_id"))
	if looksLikeCredentialSecret(credentialRef) {
		return &ToolResult{Output: "credential_vault_entry_id must be a vault entry id, not key/token/password material", IsError: true}
	}
	if strings.EqualFold(rawRepoStatus, "READY") && remoteURL == "" {
		return &ToolResult{Output: "repo_status READY requires remote_url evidence", IsError: true}
	}

	coordination, err := t.client.GetProjectCoordination(ctx, t.workspaceID, projectID)
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_repo_register failed reading project coordination for %s: %v", projectID, err), IsError: true}
	}
	existing, hasExisting := selectMatchingProjectRepository(coordination.Repositories, repoID, remoteURL, owner, name)
	if !hasExisting {
		existing, hasExisting = selectExistingCanonicalRepository(coordination.Repositories, args, remoteURL, owner, name)
	}
	if hasExisting {
		if repoID == "" {
			repoID = existing.RepoID
		}
		if remoteURL == "" {
			remoteURL = strings.TrimSpace(existing.RemoteURL)
			remoteKind = normalizeProjectRepoRemoteKind(remoteKind, remoteURL)
		}
		if owner == "" {
			owner = strings.TrimSpace(existing.Owner)
		}
		if name == "" {
			name = strings.TrimSpace(existing.Name)
		}
		if rawDefaultBranch == "" && strings.TrimSpace(existing.DefaultBranch) != "" {
			defaultBranch = strings.TrimSpace(existing.DefaultBranch)
		}
		if integrationBranch == "" {
			integrationBranch = strings.TrimSpace(existing.IntegrationBranch)
		}
		if rawRepoStatus == "" && canonicalRepositoryReadyForReuse(existing) && !projectRepoRegisterHasMetadataChange(args, existing, remoteURL, owner, name, defaultBranch, integrationBranch, credentialRef) {
			return projectRepoRegisterResult(projectRepoRegisterOutput{
				Repo:              existing,
				CreatedOrUpdated:  false,
				ReusedExisting:    true,
				ProfileUpdated:    false,
				HumanRequestMade:  false,
				CoordinationState: coordination.GateStatus.OverallState,
				CoordinationReady: coordination.GateStatus.ImplementationReady,
				Guidance:          "Existing ready repository evidence was reused. Register per-agent checkouts/branches before implementation. No git mutation was performed.",
			})
		}
	}

	repoStatus, statusErr := normalizeProjectRepoRegisterStatus(rawRepoStatus, remoteURL)
	if statusErr != nil {
		return &ToolResult{Output: statusErr.Error(), IsError: true}
	}
	if rawRepoStatus == "" && hasExisting && canonicalRepositoryReadyForReuse(existing) {
		repoStatus = "READY"
	}
	if repoStatus == "READY" && remoteURL == "" {
		return &ToolResult{Output: "repo_status READY requires remote_url evidence", IsError: true}
	}

	if repoID == "" {
		repoID = projectRepoRegisterID(projectID, owner, name)
	}
	isCanonical := true
	if explicit := optionalBoolArg(args, "is_canonical"); explicit != nil {
		isCanonical = *explicit
	}
	repo, err := t.client.UpsertProjectRepository(ctx, ProjectRepositoryUpsertInput{
		WorkspaceID:            t.workspaceID,
		ProjectID:              projectID,
		ActorID:                t.agentID,
		RepoID:                 repoID,
		RemoteURL:              remoteURL,
		RemoteKind:             remoteKind,
		Owner:                  owner,
		Name:                   name,
		DefaultBranch:          defaultBranch,
		IntegrationBranch:      integrationBranch,
		CredentialVaultEntryID: credentialRef,
		RepoStatus:             repoStatus,
		IsCanonical:            &isCanonical,
		CreatedByAgentID:       t.agentID,
	})
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_repo_register failed upserting repository evidence for %s: %v", projectID, err), IsError: true}
	}

	profileUpdated := false
	if isCanonical {
		if _, err := t.client.UpdateProjectProfile(ctx, ProjectProfileUpdateInput{
			WorkspaceID:       t.workspaceID,
			ProjectID:         projectID,
			ActorID:           t.agentID,
			RepoStatus:        projectRepoProfileStatus(repoStatus),
			RepoURL:           remoteURL,
			RepoDefaultBranch: defaultBranch,
		}); err != nil {
			return &ToolResult{Output: fmt.Sprintf("project_repo_register recorded repository %s but failed updating project profile: %v", repo.RepoID, err), IsError: true}
		}
		profileUpdated = true
	}

	humanRequestMade := false
	if shouldProjectRepoRequestHuman(args, repoStatus, remoteURL) {
		if err := t.requestRepositoryMaterialization(ctx, args, projectID, repo, owner, name, defaultBranch); err != nil {
			return &ToolResult{Output: fmt.Sprintf("project_repo_register recorded repository %s but failed creating operator request: %v", repo.RepoID, err), IsError: true}
		}
		humanRequestMade = true
	}

	coordinationAfter, coordErr := t.client.GetProjectCoordination(ctx, t.workspaceID, projectID)
	coordinationState := coordination.GateStatus.OverallState
	coordinationReady := coordination.GateStatus.ImplementationReady
	if coordErr == nil {
		coordinationState = coordinationAfter.GateStatus.OverallState
		coordinationReady = coordinationAfter.GateStatus.ImplementationReady
	}
	return projectRepoRegisterResult(projectRepoRegisterOutput{
		Repo:              repo,
		CreatedOrUpdated:  true,
		ReusedExisting:    false,
		ProfileUpdated:    profileUpdated,
		HumanRequestMade:  humanRequestMade,
		CoordinationState: coordinationState,
		CoordinationReady: coordinationReady,
		Guidance:          "Repository evidence is durable. Register per-agent checkouts and branches before implementation. No clone, commit, push, merge, or local git mutation was performed.",
	})
}

type projectRepoRegisterOutput struct {
	Repo              ProjectRepositoryRecord
	CreatedOrUpdated  bool
	ReusedExisting    bool
	ProfileUpdated    bool
	HumanRequestMade  bool
	CoordinationState string
	CoordinationReady bool
	Guidance          string
}

func projectRepoRegisterResult(output projectRepoRegisterOutput) *ToolResult {
	payload := map[string]any{
		"repo_id":                    output.Repo.RepoID,
		"project_id":                 output.Repo.ProjectID,
		"remote_url":                 output.Repo.RemoteURL,
		"remote_kind":                output.Repo.RemoteKind,
		"owner":                      output.Repo.Owner,
		"name":                       output.Repo.Name,
		"default_branch":             output.Repo.DefaultBranch,
		"integration_branch":         output.Repo.IntegrationBranch,
		"repo_status":                output.Repo.RepoStatus,
		"is_canonical":               output.Repo.IsCanonical,
		"credential_vault_entry_id":  output.Repo.CredentialVaultEntryID,
		"created_or_updated":         output.CreatedOrUpdated,
		"reused_existing":            output.ReusedExisting,
		"profile_updated":            output.ProfileUpdated,
		"human_request_created":      output.HumanRequestMade,
		"coordination_overall_state": output.CoordinationState,
		"implementation_ready":       output.CoordinationReady,
		"no_git_mutation":            true,
		"guidance":                   output.Guidance,
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ToolResult{Output: fmt.Sprintf("project_repo_register completed for %s", output.Repo.RepoID)}
	}
	return &ToolResult{Output: string(raw)}
}

func (t *ProjectRepoRegisterTool) requestRepositoryMaterialization(ctx context.Context, args map[string]any, projectID string, repo ProjectRepositoryRecord, owner, name, defaultBranch string) error {
	keepSessionActive := true
	summary := "Canonical repository is required before implementation work can proceed."
	details := strings.TrimSpace(fmt.Sprintf("Project %s needs a canonical git repository before implementation agents should create checkouts or branches.\n\nrepo_id: %s\nowner: %s\nname: %s\ndefault_branch: %s\n\nRegister a git remote and provide only a Vault credential reference if credentials are needed. Do not paste key, token, or password contents into Rhizome.", projectID, repo.RepoID, owner, name, defaultBranch))
	if value := strings.TrimSpace(stringArg(args, "request_summary")); value != "" {
		summary = value
	}
	if value := strings.TrimSpace(stringArg(args, "request_details")); value != "" {
		details = value
	}
	return t.client.RequestExternalGate(ctx, ExternalGateRequestInput{
		WorkspaceID:       t.workspaceID,
		RequestKey:        projectRepoRegisterRequestKey(projectID, repo.RepoID),
		GateType:          "EXPLICIT_APPROVAL",
		Title:             "Provide canonical repository for " + projectID,
		Summary:           firstNonEmpty(strings.TrimSpace(summary), "Canonical repository required"),
		Details:           details,
		AssignedTo:        firstNonEmpty(t.ownerUserID, t.agentID),
		Urgency:           "high",
		SourceKind:        "project",
		SourceID:          projectID,
		AgentID:           t.agentID,
		KeepSessionActive: &keepSessionActive,
	})
}

func normalizeProjectRepoRegisterStatus(value, remoteURL string) (string, error) {
	status := strings.ToUpper(strings.TrimSpace(value))
	if status == "" {
		if strings.TrimSpace(remoteURL) != "" {
			return "CREATED", nil
		}
		return "REQUESTED", nil
	}
	switch status {
	case "MISSING", "REQUESTED", "CREATED", "READY", "BROKEN", "ARCHIVED":
		return status, nil
	default:
		return "", fmt.Errorf("repo_status must be MISSING, REQUESTED, CREATED, READY, BROKEN, or ARCHIVED")
	}
}

func selectMatchingProjectRepository(repos []ProjectRepositoryRecord, repoID, remoteURL, owner, name string) (ProjectRepositoryRecord, bool) {
	repoID = strings.TrimSpace(repoID)
	remoteKey := normalizeRepoIdentityURL(remoteURL)
	ownerKey := strings.ToLower(strings.TrimSpace(owner))
	nameKey := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(name, ".git")))
	for _, repo := range repos {
		if strings.EqualFold(strings.TrimSpace(repo.RepoStatus), "ARCHIVED") {
			continue
		}
		if repoID != "" && strings.TrimSpace(repo.RepoID) == repoID {
			return repo, true
		}
		if remoteKey != "" && normalizeRepoIdentityURL(repo.RemoteURL) == remoteKey {
			return repo, true
		}
		if ownerKey != "" && nameKey != "" &&
			strings.ToLower(strings.TrimSpace(repo.Owner)) == ownerKey &&
			strings.ToLower(strings.TrimSpace(strings.TrimSuffix(repo.Name, ".git"))) == nameKey {
			return repo, true
		}
	}
	return ProjectRepositoryRecord{}, false
}

func normalizeProjectRepoRemoteKind(value, remoteURL string) string {
	kind := strings.ToLower(strings.TrimSpace(value))
	switch kind {
	case "github", "gitlab", "local", "unknown":
		return kind
	}
	remote := strings.ToLower(strings.TrimSpace(remoteURL))
	switch {
	case strings.Contains(remote, "github.com"):
		return "github"
	case strings.Contains(remote, "gitlab.com"):
		return "gitlab"
	case strings.HasPrefix(remote, "file://") || strings.HasPrefix(remote, ".") || strings.HasPrefix(remote, "/") || looksLikeWindowsPath(remote):
		return "local"
	default:
		return "unknown"
	}
}

func normalizeRepoIdentityURL(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.HasPrefix(value, "git@") {
		value = strings.Replace(value, ":", "/", 1)
	}
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.TrimSuffix(value, "/")
	value = strings.TrimSuffix(value, ".git")
	return value
}

func parseProjectRepoOwnerName(remoteURL string) (string, string) {
	remoteURL = strings.TrimSpace(remoteURL)
	if remoteURL == "" {
		return "", ""
	}
	if strings.HasPrefix(remoteURL, "git@") {
		if idx := strings.Index(remoteURL, ":"); idx >= 0 && idx+1 < len(remoteURL) {
			return ownerNameFromRepoPath(remoteURL[idx+1:])
		}
	}
	if parsed, err := url.Parse(remoteURL); err == nil && parsed.Path != "" {
		return ownerNameFromRepoPath(parsed.Path)
	}
	return ownerNameFromRepoPath(remoteURL)
}

func ownerNameFromRepoPath(path string) (string, string) {
	path = strings.TrimSpace(path)
	path = strings.TrimPrefix(path, "/")
	path = strings.TrimSuffix(path, "/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		return "", ""
	}
	owner := strings.TrimSpace(parts[len(parts)-2])
	name := strings.TrimSpace(parts[len(parts)-1])
	name = strings.TrimSuffix(name, ".git")
	if owner == "" || name == "" {
		return "", ""
	}
	return owner, name
}

func projectRepoRegisterID(projectID, owner, name string) string {
	base := strings.TrimSpace(projectID)
	if strings.TrimSpace(owner) != "" || strings.TrimSpace(name) != "" {
		base += "-" + strings.TrimSpace(owner) + "-" + strings.TrimSpace(name)
	} else {
		base += "-canonical"
	}
	return sanitizeRefSegment("projrepo-" + base)
}

func projectRepoRegisterRequestKey(projectID, repoID string) string {
	return sanitizeRefSegment("project.repo." + strings.TrimSpace(projectID) + "." + strings.TrimSpace(repoID))
}

func shouldProjectRepoRequestHuman(args map[string]any, repoStatus, remoteURL string) bool {
	defaultValue := strings.TrimSpace(remoteURL) == "" && (repoStatus == "MISSING" || repoStatus == "REQUESTED" || repoStatus == "CREATED" || repoStatus == "BROKEN")
	if explicit := optionalBoolArg(args, "request_human_if_missing"); explicit != nil {
		return *explicit && defaultValue
	}
	return defaultValue
}

func selectExistingCanonicalRepository(repos []ProjectRepositoryRecord, args map[string]any, remoteURL, owner, name string) (ProjectRepositoryRecord, bool) {
	if strings.TrimSpace(stringArg(args, "repo_id")) != "" {
		return ProjectRepositoryRecord{}, false
	}
	if explicitCanonical := optionalBoolArg(args, "is_canonical"); explicitCanonical != nil && !*explicitCanonical {
		return ProjectRepositoryRecord{}, false
	}
	for _, repo := range repos {
		if !repo.IsCanonical || strings.EqualFold(strings.TrimSpace(repo.RepoStatus), "ARCHIVED") {
			continue
		}
		if strings.TrimSpace(repo.RepoID) != "" && projectRepoRegisterCanonicalCandidateMatches(repo, remoteURL, owner, name) {
			return repo, true
		}
	}
	return ProjectRepositoryRecord{}, false
}

func projectRepoRegisterCanonicalCandidateMatches(repo ProjectRepositoryRecord, remoteURL, owner, name string) bool {
	remoteURL = strings.TrimSpace(remoteURL)
	owner = strings.TrimSpace(owner)
	name = strings.TrimSpace(name)
	if remoteURL == "" && owner == "" && name == "" {
		return true
	}
	if canonicalRepositoryReadyForReuse(repo) {
		return false
	}
	if strings.TrimSpace(repo.RemoteURL) == "" {
		return true
	}
	if remoteURL != "" && normalizeRepoIdentityURL(repo.RemoteURL) == normalizeRepoIdentityURL(remoteURL) {
		return true
	}
	if owner != "" && name != "" &&
		strings.EqualFold(strings.TrimSpace(repo.Owner), owner) &&
		strings.EqualFold(strings.TrimSpace(strings.TrimSuffix(repo.Name, ".git")), strings.TrimSpace(strings.TrimSuffix(name, ".git"))) {
		return true
	}
	return false
}

func canonicalRepositoryReadyForReuse(repo ProjectRepositoryRecord) bool {
	return repo.IsCanonical &&
		strings.TrimSpace(repo.RepoID) != "" &&
		strings.TrimSpace(repo.RemoteURL) != "" &&
		strings.EqualFold(strings.TrimSpace(repo.RepoStatus), "READY")
}

func projectRepoRegisterHasMetadataChange(args map[string]any, repo ProjectRepositoryRecord, remoteURL, owner, name, defaultBranch, integrationBranch, credentialRef string) bool {
	if optionalBoolArg(args, "is_canonical") != nil {
		return true
	}
	if strings.TrimSpace(stringArg(args, "default_branch")) != "" && strings.TrimSpace(repo.DefaultBranch) != strings.TrimSpace(defaultBranch) {
		return true
	}
	if strings.TrimSpace(stringArg(args, "integration_branch")) != "" && strings.TrimSpace(repo.IntegrationBranch) != strings.TrimSpace(integrationBranch) {
		return true
	}
	if strings.TrimSpace(stringArg(args, "credential_vault_entry_id")) != "" && strings.TrimSpace(repo.CredentialVaultEntryID) != strings.TrimSpace(credentialRef) {
		return true
	}
	if strings.TrimSpace(stringArg(args, "remote_url")) != "" && normalizeRepoIdentityURL(repo.RemoteURL) != normalizeRepoIdentityURL(remoteURL) {
		return true
	}
	if strings.TrimSpace(stringArg(args, "owner")) != "" && !strings.EqualFold(strings.TrimSpace(repo.Owner), strings.TrimSpace(owner)) {
		return true
	}
	if strings.TrimSpace(stringArg(args, "name")) != "" && !strings.EqualFold(strings.TrimSpace(strings.TrimSuffix(repo.Name, ".git")), strings.TrimSpace(strings.TrimSuffix(name, ".git"))) {
		return true
	}
	return false
}

func projectRepoProfileStatus(repoStatus string) string {
	switch strings.ToUpper(strings.TrimSpace(repoStatus)) {
	case "READY":
		return "READY"
	case "MISSING":
		return "MISSING"
	case "REQUESTED", "CREATED", "BROKEN":
		return "BLOCKED"
	default:
		return "UNKNOWN"
	}
}

func looksLikeWindowsPath(value string) bool {
	if len(value) < 3 {
		return false
	}
	return value[1] == ':' && (value[2] == '\\' || value[2] == '/')
}

func looksLikeCredentialSecret(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	lower := strings.ToLower(value)
	if strings.Contains(value, "\n") || strings.Contains(value, "\r") {
		return true
	}
	if strings.Contains(lower, "begin ") || strings.Contains(lower, "private key") {
		return true
	}
	for _, marker := range []string{"sk-", "github_pat_", "ghp_", "gho_", "glpat-", "-----"} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return len(value) > 256
}
