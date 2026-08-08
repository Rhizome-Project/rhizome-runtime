package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

const (
	ProjectRepositoryRemoteKindGitHub  = "github"
	ProjectRepositoryRemoteKindGitLab  = "gitlab"
	ProjectRepositoryRemoteKindLocal   = "local"
	ProjectRepositoryRemoteKindUnknown = "unknown"

	ProjectRepositoryStatusMissing   = "MISSING"
	ProjectRepositoryStatusRequested = "REQUESTED"
	ProjectRepositoryStatusCreated   = "CREATED"
	ProjectRepositoryStatusReady     = "READY"
	ProjectRepositoryStatusBroken    = "BROKEN"
	ProjectRepositoryStatusArchived  = "ARCHIVED"

	ProjectCheckoutKindClone       = "clone"
	ProjectCheckoutKindWorktree    = "worktree"
	ProjectCheckoutKindIntegration = "integration"
	ProjectCheckoutKindReview      = "review"
	ProjectCheckoutKindScratch     = "scratch"

	ProjectCheckoutStatusActive    = "ACTIVE"
	ProjectCheckoutStatusStale     = "STALE"
	ProjectCheckoutStatusBlocked   = "BLOCKED"
	ProjectCheckoutStatusAbandoned = "ABANDONED"
	ProjectCheckoutStatusArchived  = "ARCHIVED"
)

var (
	ErrProjectCredentialVaultReferenceInvalid = errors.New("credential_vault_entry_id must be a vault entry id, not secret material")
	ErrProjectRepositoryScopeMismatch         = errors.New("project repository scope mismatch")
	ErrProjectCheckoutScopeMismatch           = errors.New("project checkout scope mismatch")
	ErrProjectCheckoutActiveReferenceInvalid  = errors.New("project checkout active task or claim reference invalid")
)

type ProjectRepositoryRecord struct {
	RepoID                 string `json:"repo_id"`
	WorkspaceID            string `json:"workspace_id"`
	ProjectID              string `json:"project_id"`
	RemoteURL              string `json:"remote_url,omitempty"`
	RemoteKind             string `json:"remote_kind"`
	Owner                  string `json:"owner,omitempty"`
	Name                   string `json:"name,omitempty"`
	DefaultBranch          string `json:"default_branch,omitempty"`
	IntegrationBranch      string `json:"integration_branch,omitempty"`
	CredentialVaultEntryID string `json:"credential_vault_entry_id,omitempty"`
	RepoStatus             string `json:"repo_status"`
	IsCanonical            bool   `json:"is_canonical"`
	CreatedByAgentID       string `json:"created_by_agent_id,omitempty"`
	UpdatedBy              string `json:"updated_by,omitempty"`
	CreatedAt              string `json:"created_at"`
	UpdatedAt              string `json:"updated_at"`
}

type ProjectRepositoryUpsertInput struct {
	RepoID                 string
	WorkspaceID            string
	ProjectID              string
	RemoteURL              string
	RemoteKind             string
	Owner                  string
	Name                   string
	DefaultBranch          string
	IntegrationBranch      string
	CredentialVaultEntryID string
	RepoStatus             string
	IsCanonical            bool
	CreatedByAgentID       string
	ActorID                string
	ActorType              string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ProjectRepositoryListFilter struct {
	WorkspaceID     string
	ProjectID       string
	IncludeArchived bool
}

type ProjectCheckoutRecord struct {
	CheckoutID   string `json:"checkout_id"`
	WorkspaceID  string `json:"workspace_id"`
	ProjectID    string `json:"project_id"`
	RepoID       string `json:"repo_id"`
	MachineID    string `json:"machine_id"`
	MachineLabel string `json:"machine_label,omitempty"`
	OwnerUserID  string `json:"owner_user_id,omitempty"`
	AgentID      string `json:"agent_id,omitempty"`
	LocalPath    string `json:"local_path"`
	CheckoutKind string `json:"checkout_kind"`
	BranchName   string `json:"branch_name,omitempty"`
	BaseBranch   string `json:"base_branch,omitempty"`
	HeadSHA      string `json:"head_sha,omitempty"`
	BaseSHA      string `json:"base_sha,omitempty"`
	DirtyState   string `json:"dirty_state"`
	ActiveTaskID string `json:"active_task_id,omitempty"`
	// ActiveClaimID stores the active task-claim key. Task claims are keyed by task_id.
	ActiveClaimID string `json:"active_claim_id,omitempty"`
	Status        string `json:"status"`
	DerivedStatus string `json:"derived_status,omitempty"`
	LastSeenAt    string `json:"last_seen_at"`
	UpdatedBy     string `json:"updated_by,omitempty"`
	CreatedAt     string `json:"created_at"`
	UpdatedAt     string `json:"updated_at"`
}

type ProjectCheckoutRegisterInput struct {
	CheckoutID   string
	WorkspaceID  string
	ProjectID    string
	RepoID       string
	MachineID    string
	MachineLabel string
	OwnerUserID  string
	AgentID      string
	LocalPath    string
	CheckoutKind string
	BranchName   string
	BaseBranch   string
	HeadSHA      string
	BaseSHA      string
	DirtyState   string
	ActiveTaskID string
	// ActiveClaimID stores the active task-claim key. Task claims are keyed by task_id.
	ActiveClaimID string
	Status        string
	LastSeenAt    string
	ActorID       string
	ActorType     string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ProjectCheckoutListFilter struct {
	WorkspaceID        string
	ProjectID          string
	RepoID             string
	AgentID            string
	IncludeInactive    bool
	StaleAfterSeconds  int
	ReferenceTimestamp string
}

func (s *Store) UpsertProjectRepositoryWithEvent(ctx context.Context, input ProjectRepositoryUpsertInput) (ProjectRepositoryRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || projectID == "" {
		return ProjectRepositoryRecord{}, RuntimeEventRecord{}, errors.New("workspace_id and project_id are required")
	}
	if actorID == "" {
		return ProjectRepositoryRecord{}, RuntimeEventRecord{}, errors.New("actor_id is required")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectRepositoryRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	remoteKind, err := normalizeProjectRepositoryRemoteKind(input.RemoteKind)
	if err != nil {
		return ProjectRepositoryRecord{}, RuntimeEventRecord{}, err
	}
	repoStatus, err := normalizeProjectRepositoryStatus(input.RepoStatus)
	if err != nil {
		return ProjectRepositoryRecord{}, RuntimeEventRecord{}, err
	}
	credentialRef, err := normalizeCredentialVaultEntryID(input.CredentialVaultEntryID)
	if err != nil {
		return ProjectRepositoryRecord{}, RuntimeEventRecord{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ProjectRepositoryRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ProjectRepositoryRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin project repository tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var repo ProjectRepositoryRecord
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureProjectProfileTx(ctx, tx, workspaceID, projectID, actorID, now); err != nil {
			return err
		}
		repoID := strings.TrimSpace(input.RepoID)
		if repoID == "" {
			repoID = nextID("projrepo")
		}
		if err := validateProjectRepositoryIDScopeTx(ctx, tx, repoID, workspaceID, projectID); err != nil {
			return err
		}
		effectiveActorType, err := s.effectiveProjectGitActorTypeTx(ctx, tx, workspaceID, actorID, input.ActorType)
		if err != nil {
			return err
		}
		if err := s.requireProjectRepositoryAgentLeadTx(ctx, tx, workspaceID, projectID, actorID, effectiveActorType, now, input); err != nil {
			return err
		}
		if input.IsCanonical {
			if _, err := tx.ExecContext(ctx, `
UPDATE project_repositories
   SET is_canonical = 0, updated_at = ?, updated_by = ?
 WHERE workspace_id = ? AND project_id = ? AND is_canonical = 1 AND repo_id <> ?`,
				now, actorID, workspaceID, projectID, repoID); err != nil {
				return fmt.Errorf("clear prior canonical project repositories: %w", err)
			}
		}
		repo = ProjectRepositoryRecord{
			RepoID:                 repoID,
			WorkspaceID:            workspaceID,
			ProjectID:              projectID,
			RemoteURL:              strings.TrimSpace(input.RemoteURL),
			RemoteKind:             remoteKind,
			Owner:                  strings.TrimSpace(input.Owner),
			Name:                   strings.TrimSpace(input.Name),
			DefaultBranch:          firstNonEmpty(strings.TrimSpace(input.DefaultBranch), "main"),
			IntegrationBranch:      strings.TrimSpace(input.IntegrationBranch),
			CredentialVaultEntryID: credentialRef,
			RepoStatus:             repoStatus,
			IsCanonical:            input.IsCanonical,
			CreatedByAgentID:       strings.TrimSpace(input.CreatedByAgentID),
			UpdatedBy:              actorID,
			CreatedAt:              now,
			UpdatedAt:              now,
		}
		if err := upsertProjectRepositoryTx(ctx, tx, repo); err != nil {
			return err
		}
		if repo.IsCanonical {
			if err := s.updateProjectProfileRepoFromRepositoryTx(ctx, tx, repo, actorID, now); err != nil {
				return err
			}
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project repository upsert"); err != nil {
			return err
		}
		payload, err := attachProjectPromptContextEnvelope(projectRepositoryEventPayload(repo, actorID, "repository.upsert"), input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.repository.upsert"), map[string]string{
			"workspace_id":   workspaceID,
			"project_id":     projectID,
			"repo_id":        repoID,
			"actor_id":       actorID,
			"principal_type": effectiveActorType,
			"principal_id":   actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "project.repository.upserted",
			EntityType:  "project_repository",
			EntityID:    repoID,
			ActorType:   effectiveActorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return ProjectRepositoryRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ProjectRepositoryRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit project repository tx: %w", err)
	}
	return repo, event, nil
}

func (s *Store) ListProjectRepositories(ctx context.Context, filter ProjectRepositoryListFilter) ([]ProjectRepositoryRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	projectID := strings.TrimSpace(filter.ProjectID)
	if workspaceID == "" || projectID == "" {
		return nil, errors.New("workspace_id and project_id are required")
	}
	query := `
SELECT repo_id, workspace_id, project_id, remote_url, remote_kind, owner, name, default_branch,
       integration_branch, credential_vault_entry_id, repo_status, is_canonical, created_by_agent_id,
       updated_by, created_at, updated_at
  FROM project_repositories
 WHERE workspace_id = ? AND project_id = ?`
	args := []any{workspaceID, projectID}
	if !filter.IncludeArchived {
		query += ` AND repo_status <> 'ARCHIVED'`
	}
	query += ` ORDER BY is_canonical DESC, updated_at DESC, repo_id ASC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list project repositories: %w", err)
	}
	defer rows.Close()
	var repos []ProjectRepositoryRecord
	for rows.Next() {
		repo, err := scanProjectRepository(rows)
		if err != nil {
			return nil, err
		}
		repos = append(repos, repo)
	}
	return repos, rows.Err()
}

func (s *Store) RegisterProjectCheckoutWithEvent(ctx context.Context, input ProjectCheckoutRegisterInput) (ProjectCheckoutRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	repoID := strings.TrimSpace(input.RepoID)
	machineID := strings.TrimSpace(input.MachineID)
	localPath := strings.TrimSpace(input.LocalPath)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || projectID == "" || repoID == "" {
		return ProjectCheckoutRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, project_id, and repo_id are required")
	}
	if machineID == "" || localPath == "" {
		return ProjectCheckoutRecord{}, RuntimeEventRecord{}, errors.New("machine_id and local_path are required")
	}
	if actorID == "" {
		return ProjectCheckoutRecord{}, RuntimeEventRecord{}, errors.New("actor_id is required")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectCheckoutRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	checkoutKind, err := normalizeProjectCheckoutKind(input.CheckoutKind)
	if err != nil {
		return ProjectCheckoutRecord{}, RuntimeEventRecord{}, err
	}
	checkoutStatus, err := normalizeProjectCheckoutStatus(input.Status)
	if err != nil {
		return ProjectCheckoutRecord{}, RuntimeEventRecord{}, err
	}
	dirtyState := normalizeProjectCheckoutDirtyState(input.DirtyState)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	lastSeenAt := strings.TrimSpace(input.LastSeenAt)
	if lastSeenAt == "" {
		lastSeenAt = now
	}
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ProjectCheckoutRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ProjectCheckoutRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin project checkout tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var checkout ProjectCheckoutRecord
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureProjectProfileTx(ctx, tx, workspaceID, projectID, actorID, now); err != nil {
			return err
		}
		if _, err := getProjectRepositoryTx(ctx, tx, workspaceID, projectID, repoID); err != nil {
			return err
		}
		effectiveActorType, err := s.effectiveProjectGitActorTypeTx(ctx, tx, workspaceID, actorID, input.ActorType)
		if err != nil {
			return err
		}
		agentID, err := resolveProjectCheckoutAgentID(effectiveActorType, actorID, input.AgentID)
		if err != nil {
			return err
		}
		if agentID != "" {
			if err := s.ensureAgentInWorkspaceTx(ctx, tx, workspaceID, agentID); err != nil {
				return err
			}
		}
		checkoutID := strings.TrimSpace(input.CheckoutID)
		if checkoutID != "" {
			existing, ok, err := validateProjectCheckoutIDScopeTx(ctx, tx, checkoutID, workspaceID, projectID, repoID)
			if err != nil {
				return err
			}
			if ok {
				if err := requireProjectCheckoutActorOwnsExisting(effectiveActorType, agentID, existing); err != nil {
					return err
				}
			}
		}
		if existing, ok, err := findActiveProjectCheckoutByPathTx(ctx, tx, workspaceID, machineID, localPath); err != nil {
			return err
		} else if ok {
			if err := requireProjectCheckoutActorOwnsExisting(effectiveActorType, agentID, existing); err != nil {
				return err
			}
			if existing.ProjectID != projectID || existing.RepoID != repoID {
				return fmt.Errorf("%w: active path belongs to project=%s repo=%s", ErrProjectCheckoutScopeMismatch, existing.ProjectID, existing.RepoID)
			}
			if checkoutID == "" {
				checkoutID = existing.CheckoutID
			} else if checkoutID != existing.CheckoutID {
				return fmt.Errorf("%w: active path belongs to checkout=%s", ErrProjectCheckoutScopeMismatch, existing.CheckoutID)
			}
		} else if checkoutID == "" {
			checkoutID = nextID("projcheckout")
		}
		activeTaskID := strings.TrimSpace(input.ActiveTaskID)
		activeClaimID := strings.TrimSpace(input.ActiveClaimID)
		if err := s.ensureProjectCheckoutActiveReferencesTx(ctx, tx, workspaceID, projectID, agentID, activeTaskID, activeClaimID); err != nil {
			return err
		}
		checkout = ProjectCheckoutRecord{
			CheckoutID:    checkoutID,
			WorkspaceID:   workspaceID,
			ProjectID:     projectID,
			RepoID:        repoID,
			MachineID:     machineID,
			MachineLabel:  strings.TrimSpace(input.MachineLabel),
			OwnerUserID:   strings.TrimSpace(input.OwnerUserID),
			AgentID:       agentID,
			LocalPath:     localPath,
			CheckoutKind:  checkoutKind,
			BranchName:    strings.TrimSpace(input.BranchName),
			BaseBranch:    strings.TrimSpace(input.BaseBranch),
			HeadSHA:       strings.TrimSpace(input.HeadSHA),
			BaseSHA:       strings.TrimSpace(input.BaseSHA),
			DirtyState:    dirtyState,
			ActiveTaskID:  activeTaskID,
			ActiveClaimID: activeClaimID,
			Status:        checkoutStatus,
			LastSeenAt:    lastSeenAt,
			UpdatedBy:     actorID,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		if err := upsertProjectCheckoutTx(ctx, tx, checkout); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project checkout register"); err != nil {
			return err
		}
		payload, err := attachProjectPromptContextEnvelope(projectCheckoutEventPayload(checkout, actorID, "checkout.register"), input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.checkout.register"), map[string]string{
			"workspace_id":   workspaceID,
			"project_id":     projectID,
			"repo_id":        repoID,
			"checkout_id":    checkoutID,
			"actor_id":       actorID,
			"principal_type": effectiveActorType,
			"principal_id":   actorID,
		})
		if err != nil {
			return err
		}
		var appendErr error
		event, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "project.checkout.registered",
			EntityType:  "project_checkout",
			EntityID:    checkoutID,
			ActorType:   effectiveActorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return ProjectCheckoutRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ProjectCheckoutRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit project checkout tx: %w", err)
	}
	return checkout, event, nil
}

func (s *Store) ListProjectCheckouts(ctx context.Context, filter ProjectCheckoutListFilter) ([]ProjectCheckoutRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	projectID := strings.TrimSpace(filter.ProjectID)
	if workspaceID == "" || projectID == "" {
		return nil, errors.New("workspace_id and project_id are required")
	}
	query := `
SELECT checkout_id, workspace_id, project_id, repo_id, machine_id, machine_label, owner_user_id, agent_id,
       local_path, checkout_kind, branch_name, base_branch, head_sha, base_sha, dirty_state,
       active_task_id, active_claim_id, status, last_seen_at, updated_by, created_at, updated_at
  FROM project_checkout_registry
 WHERE workspace_id = ? AND project_id = ?`
	args := []any{workspaceID, projectID}
	if repoID := strings.TrimSpace(filter.RepoID); repoID != "" {
		query += ` AND repo_id = ?`
		args = append(args, repoID)
	}
	if agentID := strings.TrimSpace(filter.AgentID); agentID != "" {
		query += ` AND agent_id = ?`
		args = append(args, agentID)
	}
	if !filter.IncludeInactive {
		query += ` AND status IN ('ACTIVE', 'BLOCKED', 'STALE')`
	}
	query += ` ORDER BY updated_at DESC, checkout_id ASC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list project checkouts: %w", err)
	}
	defer rows.Close()
	referenceAt := parseOptionalRFC3339(filter.ReferenceTimestamp, time.Now().UTC())
	staleAfter := time.Duration(filter.StaleAfterSeconds) * time.Second
	if staleAfter <= 0 {
		staleAfter = time.Hour
	}
	var checkouts []ProjectCheckoutRecord
	for rows.Next() {
		checkout, err := scanProjectCheckout(rows)
		if err != nil {
			return nil, err
		}
		checkout.DerivedStatus = deriveProjectCheckoutStatus(checkout, referenceAt, staleAfter)
		checkouts = append(checkouts, checkout)
	}
	return checkouts, rows.Err()
}

func upsertProjectRepositoryTx(ctx context.Context, tx *sql.Tx, repo ProjectRepositoryRecord) error {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO project_repositories (
  repo_id, workspace_id, project_id, remote_url, remote_kind, owner, name, default_branch,
  integration_branch, credential_vault_entry_id, repo_status, is_canonical, created_by_agent_id,
  updated_by, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(repo_id) DO UPDATE SET
  remote_url = excluded.remote_url,
  remote_kind = excluded.remote_kind,
  owner = excluded.owner,
  name = excluded.name,
  default_branch = excluded.default_branch,
  integration_branch = excluded.integration_branch,
  credential_vault_entry_id = excluded.credential_vault_entry_id,
  repo_status = excluded.repo_status,
  is_canonical = excluded.is_canonical,
  created_by_agent_id = CASE WHEN project_repositories.created_by_agent_id = '' THEN excluded.created_by_agent_id ELSE project_repositories.created_by_agent_id END,
  updated_by = excluded.updated_by,
  updated_at = excluded.updated_at`,
		repo.RepoID, repo.WorkspaceID, repo.ProjectID, repo.RemoteURL, repo.RemoteKind, repo.Owner, repo.Name, repo.DefaultBranch,
		repo.IntegrationBranch, repo.CredentialVaultEntryID, repo.RepoStatus, intFromBool(repo.IsCanonical), repo.CreatedByAgentID,
		repo.UpdatedBy, repo.CreatedAt, repo.UpdatedAt); err != nil {
		return fmt.Errorf("upsert project repository: %w", err)
	}
	return nil
}

func (s *Store) updateProjectProfileRepoFromRepositoryTx(ctx context.Context, tx *sql.Tx, repo ProjectRepositoryRecord, actorID, now string) error {
	profileStatus := projectProfileRepoStatusFromRepositoryStatus(repo.RepoStatus)
	if _, err := tx.ExecContext(ctx, `
UPDATE project_profiles
   SET repo_required = 1,
       repo_status = ?,
       repo_url = ?,
       repo_default_branch = ?,
       updated_by = ?,
       updated_at = ?
 WHERE workspace_id = ? AND project_id = ?`,
		profileStatus, repo.RemoteURL, repo.DefaultBranch, actorID, now, repo.WorkspaceID, repo.ProjectID); err != nil {
		return fmt.Errorf("sync project profile repository fields: %w", err)
	}
	return nil
}

func projectProfileRepoStatusFromRepositoryStatus(repoStatus string) string {
	switch strings.ToUpper(strings.TrimSpace(repoStatus)) {
	case ProjectRepositoryStatusReady:
		return ProjectRepoStatusReady
	case ProjectRepositoryStatusBroken, ProjectRepositoryStatusArchived:
		return ProjectRepoStatusBlocked
	case ProjectRepositoryStatusMissing, ProjectRepositoryStatusRequested, ProjectRepositoryStatusCreated:
		return ProjectRepoStatusMissing
	default:
		return ProjectRepoStatusUnknown
	}
}

func getProjectRepositoryTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, repoID string) (ProjectRepositoryRecord, error) {
	row := tx.QueryRowContext(ctx, `
SELECT repo_id, workspace_id, project_id, remote_url, remote_kind, owner, name, default_branch,
       integration_branch, credential_vault_entry_id, repo_status, is_canonical, created_by_agent_id,
       updated_by, created_at, updated_at
  FROM project_repositories
 WHERE workspace_id = ? AND project_id = ? AND repo_id = ?`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(projectID), strings.TrimSpace(repoID))
	repo, err := scanProjectRepository(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProjectRepositoryRecord{}, errors.New("project repository not found")
		}
		return ProjectRepositoryRecord{}, err
	}
	return repo, nil
}

func getProjectRepositoryByIDTx(ctx context.Context, tx *sql.Tx, repoID string) (ProjectRepositoryRecord, error) {
	row := tx.QueryRowContext(ctx, `
SELECT repo_id, workspace_id, project_id, remote_url, remote_kind, owner, name, default_branch,
       integration_branch, credential_vault_entry_id, repo_status, is_canonical, created_by_agent_id,
       updated_by, created_at, updated_at
  FROM project_repositories
 WHERE repo_id = ?`,
		strings.TrimSpace(repoID))
	return scanProjectRepository(row)
}

func validateProjectRepositoryIDScopeTx(ctx context.Context, tx *sql.Tx, repoID, workspaceID, projectID string) error {
	repo, err := getProjectRepositoryByIDTx(ctx, tx, repoID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}
	if repo.WorkspaceID != strings.TrimSpace(workspaceID) || repo.ProjectID != strings.TrimSpace(projectID) {
		return fmt.Errorf("%w: repo_id=%s belongs to workspace=%s project=%s", ErrProjectRepositoryScopeMismatch, repoID, repo.WorkspaceID, repo.ProjectID)
	}
	return nil
}

func scanProjectRepository(row interface{ Scan(dest ...any) error }) (ProjectRepositoryRecord, error) {
	var repo ProjectRepositoryRecord
	var isCanonical int
	if err := row.Scan(
		&repo.RepoID,
		&repo.WorkspaceID,
		&repo.ProjectID,
		&repo.RemoteURL,
		&repo.RemoteKind,
		&repo.Owner,
		&repo.Name,
		&repo.DefaultBranch,
		&repo.IntegrationBranch,
		&repo.CredentialVaultEntryID,
		&repo.RepoStatus,
		&isCanonical,
		&repo.CreatedByAgentID,
		&repo.UpdatedBy,
		&repo.CreatedAt,
		&repo.UpdatedAt,
	); err != nil {
		return ProjectRepositoryRecord{}, err
	}
	repo.IsCanonical = isCanonical != 0
	return repo, nil
}

func upsertProjectCheckoutTx(ctx context.Context, tx *sql.Tx, checkout ProjectCheckoutRecord) error {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO project_checkout_registry (
  checkout_id, workspace_id, project_id, repo_id, machine_id, machine_label, owner_user_id, agent_id,
  local_path, checkout_kind, branch_name, base_branch, head_sha, base_sha, dirty_state,
  active_task_id, active_claim_id, status, last_seen_at, updated_by, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(checkout_id) DO UPDATE SET
  repo_id = excluded.repo_id,
  machine_id = excluded.machine_id,
  machine_label = excluded.machine_label,
  owner_user_id = excluded.owner_user_id,
  agent_id = excluded.agent_id,
  local_path = excluded.local_path,
  checkout_kind = excluded.checkout_kind,
  branch_name = excluded.branch_name,
  base_branch = excluded.base_branch,
  head_sha = excluded.head_sha,
  base_sha = excluded.base_sha,
  dirty_state = excluded.dirty_state,
  active_task_id = excluded.active_task_id,
  active_claim_id = excluded.active_claim_id,
  status = excluded.status,
  last_seen_at = excluded.last_seen_at,
  updated_by = excluded.updated_by,
  updated_at = excluded.updated_at`,
		checkout.CheckoutID, checkout.WorkspaceID, checkout.ProjectID, checkout.RepoID, checkout.MachineID, checkout.MachineLabel, checkout.OwnerUserID, checkout.AgentID,
		checkout.LocalPath, checkout.CheckoutKind, checkout.BranchName, checkout.BaseBranch, checkout.HeadSHA, checkout.BaseSHA, checkout.DirtyState,
		checkout.ActiveTaskID, checkout.ActiveClaimID, checkout.Status, checkout.LastSeenAt, checkout.UpdatedBy, checkout.CreatedAt, checkout.UpdatedAt); err != nil {
		return fmt.Errorf("upsert project checkout: %w", err)
	}
	return nil
}

func getProjectCheckoutByIDTx(ctx context.Context, tx *sql.Tx, checkoutID string) (ProjectCheckoutRecord, error) {
	row := tx.QueryRowContext(ctx, `
SELECT checkout_id, workspace_id, project_id, repo_id, machine_id, machine_label, owner_user_id, agent_id,
       local_path, checkout_kind, branch_name, base_branch, head_sha, base_sha, dirty_state,
       active_task_id, active_claim_id, status, last_seen_at, updated_by, created_at, updated_at
  FROM project_checkout_registry
 WHERE checkout_id = ?`,
		strings.TrimSpace(checkoutID))
	return scanProjectCheckout(row)
}

func validateProjectCheckoutIDScopeTx(ctx context.Context, tx *sql.Tx, checkoutID, workspaceID, projectID, repoID string) (ProjectCheckoutRecord, bool, error) {
	checkout, err := getProjectCheckoutByIDTx(ctx, tx, checkoutID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProjectCheckoutRecord{}, false, nil
		}
		return ProjectCheckoutRecord{}, false, err
	}
	if checkout.WorkspaceID != strings.TrimSpace(workspaceID) || checkout.ProjectID != strings.TrimSpace(projectID) || checkout.RepoID != strings.TrimSpace(repoID) {
		return ProjectCheckoutRecord{}, false, fmt.Errorf("%w: checkout_id=%s belongs to workspace=%s project=%s repo=%s", ErrProjectCheckoutScopeMismatch, checkoutID, checkout.WorkspaceID, checkout.ProjectID, checkout.RepoID)
	}
	return checkout, true, nil
}

func findActiveProjectCheckoutByPathTx(ctx context.Context, tx *sql.Tx, workspaceID, machineID, localPath string) (ProjectCheckoutRecord, bool, error) {
	row := tx.QueryRowContext(ctx, `
SELECT checkout_id, workspace_id, project_id, repo_id, machine_id, machine_label, owner_user_id, agent_id,
       local_path, checkout_kind, branch_name, base_branch, head_sha, base_sha, dirty_state,
       active_task_id, active_claim_id, status, last_seen_at, updated_by, created_at, updated_at
  FROM project_checkout_registry
 WHERE workspace_id = ? AND machine_id = ? AND local_path = ? AND status IN ('ACTIVE', 'BLOCKED', 'STALE')
 ORDER BY updated_at DESC
 LIMIT 1`,
		workspaceID, machineID, localPath)
	checkout, err := scanProjectCheckout(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProjectCheckoutRecord{}, false, nil
		}
		return ProjectCheckoutRecord{}, false, err
	}
	return checkout, true, nil
}

func (s *Store) requireProjectRepositoryAgentLeadTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, actorID, actorType, now string, input ProjectRepositoryUpsertInput) error {
	if !strings.EqualFold(strings.TrimSpace(actorType), "agent") {
		return nil
	}
	lead, ok, err := s.getActiveProjectStrategicLeadTx(ctx, tx, workspaceID, projectID, now)
	if err != nil {
		return err
	}
	if !ok {
		hasActiveRole, err := projectAgentHasActiveProjectRoleTx(ctx, tx, workspaceID, projectID, actorID)
		if err != nil {
			return err
		}
		if hasActiveRole {
			if err := requireProjectRepositoryRoleFallbackRecoveryTx(ctx, tx, workspaceID, projectID, input); err != nil {
				return err
			}
			return nil
		}
		return ErrProjectLeadRequired
	}
	return requireProjectLeadActor(lead, actorID, actorType)
}

func requireProjectRepositoryRoleFallbackRecoveryTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID string, input ProjectRepositoryUpsertInput) error {
	if !input.IsCanonical {
		return ErrProjectLeadRequired
	}
	remoteKind, err := normalizeProjectRepositoryRemoteKind(input.RemoteKind)
	if err != nil {
		return err
	}
	if remoteKind != ProjectRepositoryRemoteKindLocal {
		return ErrProjectLeadRequired
	}
	repoStatus, err := normalizeProjectRepositoryStatus(input.RepoStatus)
	if err != nil {
		return err
	}
	switch repoStatus {
	case ProjectRepositoryStatusReady, ProjectRepositoryStatusCreated:
	default:
		return ErrProjectLeadRequired
	}
	canonical, ok, err := getCanonicalProjectRepositoryTx(ctx, tx, workspaceID, projectID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	repoID := strings.TrimSpace(input.RepoID)
	if repoID != "" && repoID == strings.TrimSpace(canonical.RepoID) {
		return nil
	}
	return ErrProjectLeadRequired
}

func projectAgentHasActiveProjectRoleTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, actorID string) (bool, error) {
	var exists int
	err := tx.QueryRowContext(ctx, `
SELECT 1
  FROM project_agent_roles
 WHERE workspace_id = ?
   AND project_id = ?
   AND agent_id = ?
   AND status = ?
 LIMIT 1`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(projectID),
		strings.TrimSpace(actorID),
		ProjectRoleStatusActive,
	).Scan(&exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return exists == 1, nil
}

func getCanonicalProjectRepositoryTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID string) (ProjectRepositoryRecord, bool, error) {
	row := tx.QueryRowContext(ctx, `
SELECT repo_id, workspace_id, project_id, remote_url, remote_kind, owner, name, default_branch,
       integration_branch, credential_vault_entry_id, repo_status, is_canonical, created_by_agent_id,
       updated_by, created_at, updated_at
  FROM project_repositories
 WHERE workspace_id = ?
   AND project_id = ?
   AND is_canonical = 1
   AND repo_status <> ?
 ORDER BY updated_at DESC, repo_id ASC
 LIMIT 1`,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(projectID),
		ProjectRepositoryStatusArchived,
	)
	repo, err := scanProjectRepository(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProjectRepositoryRecord{}, false, nil
		}
		return ProjectRepositoryRecord{}, false, err
	}
	return repo, true, nil
}

func resolveProjectCheckoutAgentID(actorType, actorID, inputAgentID string) (string, error) {
	actorID = strings.TrimSpace(actorID)
	agentID := strings.TrimSpace(inputAgentID)
	if !strings.EqualFold(strings.TrimSpace(actorType), "agent") {
		return agentID, nil
	}
	if agentID == "" {
		return actorID, nil
	}
	if agentID != actorID {
		return "", ErrProjectLeadMismatch
	}
	return agentID, nil
}

func requireProjectCheckoutActorOwnsExisting(actorType, agentID string, existing ProjectCheckoutRecord) error {
	if !strings.EqualFold(strings.TrimSpace(actorType), "agent") {
		return nil
	}
	agentID = strings.TrimSpace(agentID)
	existingAgentID := strings.TrimSpace(existing.AgentID)
	if agentID == "" || existingAgentID != agentID {
		return fmt.Errorf("%w: checkout %s belongs to agent %s", ErrProjectCheckoutScopeMismatch, strings.TrimSpace(existing.CheckoutID), existingAgentID)
	}
	return nil
}

func (s *Store) effectiveProjectGitActorTypeTx(ctx context.Context, tx *sql.Tx, workspaceID, actorID, declaredActorType string) (string, error) {
	actorType := strings.ToLower(strings.TrimSpace(declaredActorType))
	if actorType == "" {
		actorType = "human"
	}
	switch actorType {
	case "human", "agent", "system":
	default:
		return "", fmt.Errorf("invalid actor_type: %s", declaredActorType)
	}
	isAgent, err := s.projectGitActorIsWorkspaceAgentTx(ctx, tx, workspaceID, actorID)
	if err != nil {
		return "", err
	}
	if isAgent {
		return "agent", nil
	}
	return actorType, nil
}

func (s *Store) projectGitActorIsWorkspaceAgentTx(ctx context.Context, tx *sql.Tx, workspaceID, actorID string) (bool, error) {
	var exists int
	err := tx.QueryRowContext(ctx, `
SELECT 1
  FROM agents
 WHERE workspace_id = ? AND agent_id = ?
 LIMIT 1`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(actorID)).Scan(&exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("check project git actor agent identity: %w", err)
	}
	return true, nil
}

func (s *Store) ensureProjectCheckoutActiveReferencesTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, agentID, activeTaskID, activeClaimID string) error {
	if activeTaskID != "" {
		if err := s.ensureProjectCheckoutTaskRefTx(ctx, tx, workspaceID, projectID, activeTaskID); err != nil {
			return err
		}
	}
	if activeTaskID != "" && activeClaimID == "" {
		return fmt.Errorf("%w: active_task_id requires active_claim_id", ErrProjectCheckoutActiveReferenceInvalid)
	}
	if activeClaimID != "" && activeTaskID == "" {
		return fmt.Errorf("%w: active_claim_id requires active_task_id", ErrProjectCheckoutActiveReferenceInvalid)
	}
	if activeClaimID == "" {
		return nil
	}
	claimTaskID, claimAgentID, claimStatus, err := s.projectCheckoutClaimRefTx(ctx, tx, workspaceID, activeClaimID)
	if err != nil {
		return err
	}
	if claimStatus != model.TaskClaimStatusClaimed {
		return fmt.Errorf("%w: active claim %s has status %s", ErrProjectCheckoutActiveReferenceInvalid, activeClaimID, claimStatus)
	}
	if activeTaskID != "" && claimTaskID != activeTaskID {
		return fmt.Errorf("%w: active_claim_id must reference active_task_id", ErrProjectCheckoutActiveReferenceInvalid)
	}
	if agentID == "" {
		return fmt.Errorf("%w: active_claim_id requires checkout agent_id", ErrProjectCheckoutActiveReferenceInvalid)
	}
	if claimAgentID != agentID {
		return fmt.Errorf("%w: active claim belongs to agent %s", ErrProjectCheckoutActiveReferenceInvalid, claimAgentID)
	}
	return s.ensureProjectCheckoutTaskRefTx(ctx, tx, workspaceID, projectID, claimTaskID)
}

func (s *Store) ensureProjectCheckoutTaskRefTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, taskID string) error {
	if resolved, rerr := s.resolveTaskIDWithQuerier(ctx, tx, workspaceID, taskID); rerr == nil {
		taskID = resolved
	}
	var taskProjectID string
	err := tx.QueryRowContext(ctx, `
SELECT COALESCE(t.project_id, '')
  FROM workspace_tasks wt
  JOIN tasks t ON t.task_id = wt.task_id
 WHERE wt.workspace_id = ? AND wt.task_id = ?`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(taskID)).Scan(&taskProjectID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: active task %s is not attached to workspace", ErrProjectCheckoutActiveReferenceInvalid, taskID)
		}
		return fmt.Errorf("validate project checkout active task: %w", err)
	}
	if strings.TrimSpace(taskProjectID) != strings.TrimSpace(projectID) {
		return fmt.Errorf("%w: active task %s belongs to project %s", ErrProjectCheckoutActiveReferenceInvalid, taskID, strings.TrimSpace(taskProjectID))
	}
	return nil
}

func (s *Store) projectCheckoutClaimRefTx(ctx context.Context, tx *sql.Tx, workspaceID, activeClaimID string) (string, string, string, error) {
	// active_claim_id is keyed as task_id in task_claims; canonicalize a case/alias variant (SA-4).
	if resolved, rerr := s.resolveTaskIDWithQuerier(ctx, tx, workspaceID, activeClaimID); rerr == nil {
		activeClaimID = resolved
	}
	var taskID, agentID, claimStatus string
	err := tx.QueryRowContext(ctx, `
SELECT task_id, agent_id, claim_status
  FROM task_claims
 WHERE workspace_id = ? AND task_id = ?`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(activeClaimID)).Scan(&taskID, &agentID, &claimStatus)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", "", fmt.Errorf("%w: active claim %s not found", ErrProjectCheckoutActiveReferenceInvalid, activeClaimID)
		}
		return "", "", "", fmt.Errorf("validate project checkout active claim: %w", err)
	}
	return strings.TrimSpace(taskID), strings.TrimSpace(agentID), strings.TrimSpace(claimStatus), nil
}

func scanProjectCheckout(row interface{ Scan(dest ...any) error }) (ProjectCheckoutRecord, error) {
	var checkout ProjectCheckoutRecord
	if err := row.Scan(
		&checkout.CheckoutID,
		&checkout.WorkspaceID,
		&checkout.ProjectID,
		&checkout.RepoID,
		&checkout.MachineID,
		&checkout.MachineLabel,
		&checkout.OwnerUserID,
		&checkout.AgentID,
		&checkout.LocalPath,
		&checkout.CheckoutKind,
		&checkout.BranchName,
		&checkout.BaseBranch,
		&checkout.HeadSHA,
		&checkout.BaseSHA,
		&checkout.DirtyState,
		&checkout.ActiveTaskID,
		&checkout.ActiveClaimID,
		&checkout.Status,
		&checkout.LastSeenAt,
		&checkout.UpdatedBy,
		&checkout.CreatedAt,
		&checkout.UpdatedAt,
	); err != nil {
		return ProjectCheckoutRecord{}, err
	}
	return checkout, nil
}

func normalizeProjectRepositoryRemoteKind(value string) (string, error) {
	kind := strings.ToLower(strings.TrimSpace(value))
	if kind == "" {
		kind = ProjectRepositoryRemoteKindUnknown
	}
	switch kind {
	case ProjectRepositoryRemoteKindGitHub, ProjectRepositoryRemoteKindGitLab, ProjectRepositoryRemoteKindLocal, ProjectRepositoryRemoteKindUnknown:
		return kind, nil
	default:
		return "", fmt.Errorf("invalid project repository remote_kind: %s", value)
	}
}

func normalizeProjectRepositoryStatus(value string) (string, error) {
	status := strings.ToUpper(strings.TrimSpace(value))
	if status == "" {
		status = ProjectRepositoryStatusMissing
	}
	switch status {
	case ProjectRepositoryStatusMissing, ProjectRepositoryStatusRequested, ProjectRepositoryStatusCreated, ProjectRepositoryStatusReady, ProjectRepositoryStatusBroken, ProjectRepositoryStatusArchived:
		return status, nil
	default:
		return "", fmt.Errorf("invalid project repository status: %s", value)
	}
}

func normalizeProjectCheckoutKind(value string) (string, error) {
	kind := strings.ToLower(strings.TrimSpace(value))
	if kind == "" {
		kind = ProjectCheckoutKindClone
	}
	switch kind {
	case ProjectCheckoutKindClone, ProjectCheckoutKindWorktree, ProjectCheckoutKindIntegration, ProjectCheckoutKindReview, ProjectCheckoutKindScratch:
		return kind, nil
	default:
		return "", fmt.Errorf("invalid project checkout kind: %s", value)
	}
}

func normalizeProjectCheckoutStatus(value string) (string, error) {
	status := strings.ToUpper(strings.TrimSpace(value))
	if status == "" {
		status = ProjectCheckoutStatusActive
	}
	switch status {
	case ProjectCheckoutStatusActive, ProjectCheckoutStatusStale, ProjectCheckoutStatusBlocked, ProjectCheckoutStatusAbandoned, ProjectCheckoutStatusArchived:
		return status, nil
	default:
		return "", fmt.Errorf("invalid project checkout status: %s", value)
	}
}

func normalizeProjectCheckoutDirtyState(value string) string {
	dirty := strings.ToLower(strings.TrimSpace(value))
	switch dirty {
	case "clean", "dirty", "unknown":
		return dirty
	default:
		return "unknown"
	}
}

func normalizeCredentialVaultEntryID(value string) (string, error) {
	ref := strings.TrimSpace(value)
	if ref == "" {
		return "", nil
	}
	lower := strings.ToLower(ref)
	if strings.ContainsAny(ref, "\r\n\t ") || strings.Contains(ref, "-----") || strings.Contains(lower, "private key") {
		return "", ErrProjectCredentialVaultReferenceInvalid
	}
	return ref, nil
}

func deriveProjectCheckoutStatus(checkout ProjectCheckoutRecord, referenceAt time.Time, staleAfter time.Duration) string {
	if checkout.Status != ProjectCheckoutStatusActive {
		return checkout.Status
	}
	lastSeenAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(checkout.LastSeenAt))
	if err != nil {
		return ProjectCheckoutStatusStale
	}
	if referenceAt.Sub(lastSeenAt.UTC()) > staleAfter {
		return ProjectCheckoutStatusStale
	}
	return checkout.Status
}

func parseOptionalRFC3339(value string, fallback time.Time) time.Time {
	if strings.TrimSpace(value) == "" {
		return fallback.UTC()
	}
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
	if err != nil {
		return fallback.UTC()
	}
	return parsed.UTC()
}

func projectRepositoryEventPayload(repo ProjectRepositoryRecord, actorID, operation string) map[string]any {
	return map[string]any{
		"workspace_id":       repo.WorkspaceID,
		"project_id":         repo.ProjectID,
		"repo_id":            repo.RepoID,
		"actor_id":           strings.TrimSpace(actorID),
		"entity_type":        "project_repository",
		"entity_id":          repo.RepoID,
		"summary":            "Project repository mutation: " + repo.RepoStatus,
		"mutation_operation": operation,
		"repository":         repo,
	}
}

func projectCheckoutEventPayload(checkout ProjectCheckoutRecord, actorID, operation string) map[string]any {
	return map[string]any{
		"workspace_id":       checkout.WorkspaceID,
		"project_id":         checkout.ProjectID,
		"repo_id":            checkout.RepoID,
		"checkout_id":        checkout.CheckoutID,
		"actor_id":           strings.TrimSpace(actorID),
		"entity_type":        "project_checkout",
		"entity_id":          checkout.CheckoutID,
		"summary":            "Project checkout mutation: " + checkout.Status,
		"mutation_operation": operation,
		"checkout":           checkout,
	}
}

func intFromBool(value bool) int {
	if value {
		return 1
	}
	return 0
}
