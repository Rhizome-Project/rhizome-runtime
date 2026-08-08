package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"strings"
	"time"
)

const (
	ProjectBranchKindFeature     = "feature"
	ProjectBranchKindIntegration = "integration"
	ProjectBranchKindReview      = "review"
	ProjectBranchKindRelease     = "release"
	ProjectBranchKindScratch     = "scratch"

	ProjectBranchStatusReserved       = "RESERVED"
	ProjectBranchStatusActive         = "ACTIVE"
	ProjectBranchStatusBlocked        = "BLOCKED"
	ProjectBranchStatusReadyForReview = "READY_FOR_REVIEW"
	ProjectBranchStatusMerged         = "MERGED"
	ProjectBranchStatusAbandoned      = "ABANDONED"
	ProjectBranchStatusArchived       = "ARCHIVED"
)

var (
	ErrProjectBranchScopeMismatch          = errors.New("project branch scope mismatch")
	ErrProjectBranchActiveReferenceInvalid = errors.New("project branch active task or claim reference invalid")
	ErrProjectBranchReviewEvidenceInvalid  = errors.New("project branch review evidence invalid")
)

type ProjectBranchRecord struct {
	BranchID     string `json:"branch_id"`
	WorkspaceID  string `json:"workspace_id"`
	ProjectID    string `json:"project_id"`
	RepoID       string `json:"repo_id"`
	CheckoutID   string `json:"checkout_id,omitempty"`
	AgentID      string `json:"agent_id,omitempty"`
	ActiveTaskID string `json:"active_task_id,omitempty"`
	// ActiveClaimID stores the active task-claim key. Task claims are keyed by task_id.
	ActiveClaimID  string `json:"active_claim_id,omitempty"`
	BranchName     string `json:"branch_name"`
	BranchKind     string `json:"branch_kind"`
	BaseBranch     string `json:"base_branch,omitempty"`
	HeadSHA        string `json:"head_sha,omitempty"`
	BaseSHA        string `json:"base_sha,omitempty"`
	WriteScopeJSON string `json:"write_scope_json,omitempty"`
	ReviewDocKey   string `json:"review_doc_key,omitempty"`
	Status         string `json:"status"`
	UpdatedBy      string `json:"updated_by,omitempty"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
}

type ProjectBranchRegisterInput struct {
	BranchID     string
	WorkspaceID  string
	ProjectID    string
	RepoID       string
	CheckoutID   string
	AgentID      string
	ActiveTaskID string
	// ActiveClaimID stores the active task-claim key. Task claims are keyed by task_id.
	ActiveClaimID  string
	BranchName     string
	BranchKind     string
	BaseBranch     string
	HeadSHA        string
	BaseSHA        string
	WriteScopeJSON string
	ReviewDocKey   string
	Status         string
	ActorID        string
	ActorType      string

	PromptContextEnvelope map[string]any
	PromptContextSurface  string
}

type ProjectBranchListFilter struct {
	WorkspaceID     string
	ProjectID       string
	RepoID          string
	AgentID         string
	ActiveTaskID    string
	IncludeInactive bool
}

func (s *Store) RegisterProjectBranchWithEvent(ctx context.Context, input ProjectBranchRegisterInput) (ProjectBranchRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	repoID := strings.TrimSpace(input.RepoID)
	branchName := strings.TrimSpace(input.BranchName)
	actorID := strings.TrimSpace(input.ActorID)
	if workspaceID == "" || projectID == "" || repoID == "" {
		return ProjectBranchRecord{}, RuntimeEventRecord{}, errors.New("workspace_id, project_id, and repo_id are required")
	}
	if branchName == "" {
		return ProjectBranchRecord{}, RuntimeEventRecord{}, errors.New("branch_name is required")
	}
	if actorID == "" {
		return ProjectBranchRecord{}, RuntimeEventRecord{}, errors.New("actor_id is required")
	}
	if input.PromptContextEnvelope == nil {
		return ProjectBranchRecord{}, RuntimeEventRecord{}, errors.New("prompt_context_envelope is required")
	}
	branchKind, err := normalizeProjectBranchKind(input.BranchKind)
	if err != nil {
		return ProjectBranchRecord{}, RuntimeEventRecord{}, err
	}
	status, err := normalizeProjectBranchStatus(input.Status)
	if err != nil {
		return ProjectBranchRecord{}, RuntimeEventRecord{}, err
	}
	explicitWriteScopeJSON := strings.TrimSpace(input.WriteScopeJSON)
	writeScopeJSON, err := normalizeProjectBranchWriteScope(input.WriteScopeJSON)
	if err != nil {
		return ProjectBranchRecord{}, RuntimeEventRecord{}, err
	}
	reviewDocKey := strings.TrimSpace(input.ReviewDocKey)
	headSHA := strings.TrimSpace(input.HeadSHA)
	baseSHA := strings.TrimSpace(input.BaseSHA)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return ProjectBranchRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ProjectBranchRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin project branch tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var branch ProjectBranchRecord
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
		agentID, err := resolveProjectBranchAgentID(effectiveActorType, actorID, input.AgentID, input.BranchID, status)
		if err != nil {
			return err
		}
		checkoutID := strings.TrimSpace(input.CheckoutID)
		baseBranch := strings.TrimSpace(input.BaseBranch)
		if checkoutID != "" {
			checkout, err := getProjectCheckoutByIDTx(ctx, tx, checkoutID)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return fmt.Errorf("%w: checkout_id=%s not found", ErrProjectBranchScopeMismatch, checkoutID)
				}
				return err
			}
			if checkout.WorkspaceID != workspaceID || checkout.ProjectID != projectID || checkout.RepoID != repoID {
				return fmt.Errorf("%w: checkout_id=%s belongs to workspace=%s project=%s repo=%s", ErrProjectBranchScopeMismatch, checkoutID, checkout.WorkspaceID, checkout.ProjectID, checkout.RepoID)
			}
			if agentID == "" {
				agentID = strings.TrimSpace(checkout.AgentID)
			}
			if strings.TrimSpace(checkout.AgentID) != "" && strings.TrimSpace(checkout.AgentID) != strings.TrimSpace(agentID) {
				return fmt.Errorf("%w: checkout %s belongs to agent %s", ErrProjectBranchScopeMismatch, checkoutID, checkout.AgentID)
			}
		}
		if agentID != "" {
			if err := s.ensureAgentInWorkspaceTx(ctx, tx, workspaceID, agentID); err != nil {
				return err
			}
		}
		branchID := strings.TrimSpace(input.BranchID)
		var existingBranch *ProjectBranchRecord
		if branchID != "" {
			existing, ok, err := validateProjectBranchIDScopeTx(ctx, tx, branchID, workspaceID, projectID, repoID)
			if err != nil {
				return err
			}
			if ok {
				if err := s.requireProjectBranchActorOwnsExistingTx(ctx, tx, workspaceID, projectID, effectiveActorType, actorID, agentID, status, headSHA, now, existing); err != nil {
					return err
				}
				existingBranch = &existing
			}
		}
		if existing, ok, err := findLiveProjectBranchByNameTx(ctx, tx, workspaceID, repoID, branchName); err != nil {
			return err
		} else if ok {
			if err := s.requireProjectBranchActorOwnsExistingTx(ctx, tx, workspaceID, projectID, effectiveActorType, actorID, agentID, status, headSHA, now, existing); err != nil {
				return err
			}
			if branchID == "" {
				branchID = existing.BranchID
				existingBranch = &existing
			} else if branchID != existing.BranchID {
				return fmt.Errorf("%w: branch_name belongs to branch_id=%s", ErrProjectBranchScopeMismatch, existing.BranchID)
			} else if existingBranch == nil {
				existingBranch = &existing
			}
		}
		activeTaskID := strings.TrimSpace(input.ActiveTaskID)
		activeClaimID := strings.TrimSpace(input.ActiveClaimID)
		if err := validateProjectBranchTerminalActiveReferences(status, activeTaskID, activeClaimID); err != nil {
			return err
		}
		if branchID == "" && existingBranch == nil && projectBranchTerminalIntegrationTargetReregisterRequested(branchKind, status, branchName, headSHA, activeTaskID, activeClaimID) {
			existing, ok, err := findMergedProjectIntegrationTargetBranchTx(ctx, tx, workspaceID, projectID, repoID, branchName, headSHA)
			if err != nil {
				return err
			}
			if ok {
				allowed, err := s.projectBranchActorHasMergeAuthorityTx(ctx, tx, workspaceID, projectID, actorID, now)
				if err != nil {
					return err
				}
				if !allowed {
					return fmt.Errorf("%w: terminal integration target branch %s MERGED registration requires integration authority", ErrProjectBranchScopeMismatch, strings.TrimSpace(existing.BranchID))
				}
				branch = existing
				payload, err := attachProjectPromptContextEnvelope(projectBranchEventPayload(branch, actorID, "branch.register.idempotent_terminal_integration"), input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.branch.register"), map[string]string{
					"workspace_id":   workspaceID,
					"project_id":     projectID,
					"repo_id":        repoID,
					"branch_id":      branch.BranchID,
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
					EventType:   "project.branch.registered",
					EntityType:  "project_branch",
					EntityID:    branch.BranchID,
					ActorType:   effectiveActorType,
					ActorID:     actorID,
					PayloadJSON: mustJSON(payload),
					CreatedAt:   now,
				})
				return appendErr
			}
		}
		if branchID == "" {
			branchID = nextID("projbranch")
		}
		if existingBranch != nil {
			if err := validateProjectBranchTerminalTransition(*existingBranch, status); err != nil {
				return err
			}
			if err := validateProjectBranchAbandonTransition(*existingBranch, status, activeTaskID, activeClaimID); err != nil {
				return err
			}
			if reviewDocKey == "" {
				reviewDocKey = strings.TrimSpace(existingBranch.ReviewDocKey)
			}
			if headSHA == "" {
				headSHA = strings.TrimSpace(existingBranch.HeadSHA)
			}
			if baseSHA == "" {
				baseSHA = strings.TrimSpace(existingBranch.BaseSHA)
			}
			if writeScopeJSON == "" {
				writeScopeJSON = strings.TrimSpace(existingBranch.WriteScopeJSON)
			}
			if baseBranch == "" {
				baseBranch = strings.TrimSpace(existingBranch.BaseBranch)
			}
			// CA-03 (assessed false-positive): a branch may legitimately carry empty
			// active references while a task claim still references it -- agent_work then
			// falls back to claim scope (see
			// TestGetAgentWorkNextFallsBackToClaimScopeWhenBranchHasNoActiveRefs), and
			// cross-agent double-booking is independently blocked by the branch.AgentID
			// guard in validateTaskClaimBranchBinding even when ActiveTaskID is empty.
			// Preserving active refs here broke those intended transitions, so the clear
			// is intentionally left as-is.
			if err := validateProjectBranchScopeTransition(*existingBranch, writeScopeJSON, status); err != nil {
				return err
			}
			if status == ProjectBranchStatusMerged {
				if err := s.validateProjectBranchMergedTransitionTx(ctx, tx, workspaceID, projectID, actorID, branchID, headSHA, *existingBranch, now); err != nil {
					return err
				}
			}
			if liveItem, ok, err := getLiveProjectPatchQueueItemByBranchTx(ctx, tx, workspaceID, projectID, branchID); err != nil {
				return err
			} else if ok {
				if status != ProjectBranchStatusReadyForReview {
					return fmt.Errorf("%w: branch %s has live patch queue item %s/%s; finish or cancel it before leaving READY_FOR_REVIEW", ErrProjectBranchReviewEvidenceInvalid, branchID, liveItem.QueueID, liveItem.ItemID)
				}
				if projectBranchQueueEvidenceChanged(*existingBranch, branchName, branchKind, headSHA, baseSHA, reviewDocKey, writeScopeJSON) {
					return fmt.Errorf("%w: branch %s has live patch queue item %s/%s; finish or cancel it before changing review evidence", ErrProjectBranchReviewEvidenceInvalid, branchID, liveItem.QueueID, liveItem.ItemID)
				}
			}
			if acceptedItem, ok, err := getLatestAcceptedProjectPatchQueueItemByBranchTx(ctx, tx, workspaceID, projectID, branchID); err != nil {
				return err
			} else if ok {
				if projectBranchQueueEvidenceChanged(*existingBranch, branchName, branchKind, headSHA, baseSHA, reviewDocKey, writeScopeJSON) {
					return fmt.Errorf("%w: branch %s has ACCEPTED patch queue item %s/%s; accepted source boundary is frozen until integration, rebuild, or explicit supersession", ErrProjectBranchReviewEvidenceInvalid, branchID, acceptedItem.QueueID, acceptedItem.ItemID)
				}
				if status != ProjectBranchStatusReadyForReview {
					mergeAllowed := false
					if status == ProjectBranchStatusMerged {
						allowed, err := s.projectBranchActorHasMergeAuthorityTx(ctx, tx, workspaceID, projectID, actorID, now)
						if err != nil {
							return err
						}
						if allowed {
							mergeAllowed = true
						}
					}
					if !mergeAllowed {
						return fmt.Errorf("%w: branch %s has ACCEPTED patch queue item %s/%s; accepted source boundary is frozen until integration, rebuild, or explicit supersession", ErrProjectBranchReviewEvidenceInvalid, branchID, acceptedItem.QueueID, acceptedItem.ItemID)
					}
				}
			}
			status = preserveProjectBranchReadyStatusOnNoopReregister(*existingBranch, status, branchName, branchKind, headSHA, baseSHA, reviewDocKey, writeScopeJSON)
		}
		if err := s.ensureProjectBranchActiveReferencesTx(ctx, tx, workspaceID, projectID, agentID, activeTaskID, activeClaimID); err != nil {
			return err
		}
		if status == ProjectBranchStatusReadyForReview {
			if activeTaskID == "" || activeClaimID == "" {
				return fmt.Errorf("%w: READY_FOR_REVIEW for branch %s requires active_task_id and active_claim_id bound to this branch", ErrProjectBranchActiveReferenceInvalid, branchID)
			}
			candidateBranch := ProjectBranchRecord{
				BranchID:       branchID,
				WorkspaceID:    workspaceID,
				ProjectID:      projectID,
				RepoID:         repoID,
				CheckoutID:     checkoutID,
				AgentID:        agentID,
				ActiveTaskID:   activeTaskID,
				ActiveClaimID:  activeClaimID,
				WriteScopeJSON: writeScopeJSON,
			}
			if err := s.validateProjectBranchActiveTaskPathsetBindingTx(ctx, tx, candidateBranch, projectBranchReviewScopePaths(writeScopeJSON)); err != nil {
				return err
			}
		}
		boundScopeJSON, err := s.projectBranchWriteScopeBoundToActiveClaimTx(ctx, tx, workspaceID, agentID, activeTaskID, activeClaimID, writeScopeJSON, explicitWriteScopeJSON != "", status)
		if err != nil {
			return err
		}
		writeScopeJSON = boundScopeJSON
		if existingBranch != nil {
			if err := validateProjectBranchScopeTransition(*existingBranch, writeScopeJSON, status); err != nil {
				return err
			}
		}
		if status == ProjectBranchStatusReadyForReview {
			if len(projectBranchReviewScopePaths(writeScopeJSON)) == 0 {
				return fmt.Errorf("%w: READY_FOR_REVIEW requires non-empty write_scope_json paths", ErrProjectBranchScopeMismatch)
			}
			candidateBranch := ProjectBranchRecord{
				BranchID:       branchID,
				WorkspaceID:    workspaceID,
				ProjectID:      projectID,
				RepoID:         repoID,
				CheckoutID:     checkoutID,
				AgentID:        agentID,
				ActiveTaskID:   activeTaskID,
				ActiveClaimID:  activeClaimID,
				WriteScopeJSON: writeScopeJSON,
			}
			if err := s.validateProjectBranchActiveTaskPathsetBindingTx(ctx, tx, candidateBranch, projectBranchReviewScopePaths(writeScopeJSON)); err != nil {
				return err
			}
			if err := s.validateProjectBranchReviewReadyEvidenceTx(ctx, tx, workspaceID, projectID, reviewDocKey); err != nil {
				return err
			}
			if err := validateProjectBranchReadyObjectIDs(headSHA, baseSHA); err != nil {
				return err
			}
		}
		branch = ProjectBranchRecord{
			BranchID:       branchID,
			WorkspaceID:    workspaceID,
			ProjectID:      projectID,
			RepoID:         repoID,
			CheckoutID:     checkoutID,
			AgentID:        agentID,
			ActiveTaskID:   activeTaskID,
			ActiveClaimID:  activeClaimID,
			BranchName:     branchName,
			BranchKind:     branchKind,
			BaseBranch:     baseBranch,
			HeadSHA:        headSHA,
			BaseSHA:        baseSHA,
			WriteScopeJSON: writeScopeJSON,
			ReviewDocKey:   reviewDocKey,
			Status:         status,
			UpdatedBy:      actorID,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		if err := upsertProjectBranchTx(ctx, tx, branch); err != nil {
			return err
		}
		if err := s.touchWorkspaceTx(ctx, tx, workspaceID, now, "project branch register"); err != nil {
			return err
		}
		payload, err := attachProjectPromptContextEnvelope(projectBranchEventPayload(branch, actorID, "branch.register"), input.PromptContextEnvelope, firstNonEmpty(strings.TrimSpace(input.PromptContextSurface), "project.branch.register"), map[string]string{
			"workspace_id":   workspaceID,
			"project_id":     projectID,
			"repo_id":        repoID,
			"branch_id":      branchID,
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
			EventType:   "project.branch.registered",
			EntityType:  "project_branch",
			EntityID:    branchID,
			ActorType:   effectiveActorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		return ProjectBranchRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ProjectBranchRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit project branch tx: %w", err)
	}
	return branch, event, nil
}

func (s *Store) ListProjectBranches(ctx context.Context, filter ProjectBranchListFilter) ([]ProjectBranchRecord, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	projectID := strings.TrimSpace(filter.ProjectID)
	if workspaceID == "" || projectID == "" {
		return nil, errors.New("workspace_id and project_id are required")
	}
	query := `
SELECT branch_id, workspace_id, project_id, repo_id, checkout_id, agent_id, active_task_id,
       active_claim_id, branch_name, branch_kind, base_branch, head_sha, base_sha,
       write_scope_json, review_doc_key, status, updated_by, created_at, updated_at
  FROM project_branch_registry
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
	if activeTaskID := strings.TrimSpace(filter.ActiveTaskID); activeTaskID != "" {
		query += ` AND active_task_id = ?`
		args = append(args, activeTaskID)
	}
	if !filter.IncludeInactive {
		query += ` AND status IN ('RESERVED', 'ACTIVE', 'BLOCKED', 'READY_FOR_REVIEW')`
	}
	query += ` ORDER BY updated_at DESC, branch_id ASC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list project branches: %w", err)
	}
	defer rows.Close()
	var branches []ProjectBranchRecord
	for rows.Next() {
		branch, err := scanProjectBranch(rows)
		if err != nil {
			return nil, err
		}
		branches = append(branches, branch)
	}
	return branches, rows.Err()
}

func upsertProjectBranchTx(ctx context.Context, tx *sql.Tx, branch ProjectBranchRecord) error {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO project_branch_registry (
  branch_id, workspace_id, project_id, repo_id, checkout_id, agent_id, active_task_id,
  active_claim_id, branch_name, branch_kind, base_branch, head_sha, base_sha,
  write_scope_json, review_doc_key, status, updated_by, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(branch_id) DO UPDATE SET
  checkout_id = excluded.checkout_id,
  agent_id = excluded.agent_id,
  active_task_id = excluded.active_task_id,
  active_claim_id = excluded.active_claim_id,
  branch_name = excluded.branch_name,
  branch_kind = excluded.branch_kind,
  base_branch = excluded.base_branch,
  head_sha = excluded.head_sha,
  base_sha = excluded.base_sha,
  write_scope_json = excluded.write_scope_json,
  review_doc_key = excluded.review_doc_key,
  status = excluded.status,
  updated_by = excluded.updated_by,
  updated_at = excluded.updated_at`,
		branch.BranchID, branch.WorkspaceID, branch.ProjectID, branch.RepoID, branch.CheckoutID, branch.AgentID, branch.ActiveTaskID,
		branch.ActiveClaimID, branch.BranchName, branch.BranchKind, branch.BaseBranch, branch.HeadSHA, branch.BaseSHA,
		branch.WriteScopeJSON, branch.ReviewDocKey, branch.Status, branch.UpdatedBy, branch.CreatedAt, branch.UpdatedAt); err != nil {
		return fmt.Errorf("upsert project branch: %w", err)
	}
	return nil
}

func getProjectBranchByIDTx(ctx context.Context, tx *sql.Tx, branchID string) (ProjectBranchRecord, error) {
	row := tx.QueryRowContext(ctx, `
SELECT branch_id, workspace_id, project_id, repo_id, checkout_id, agent_id, active_task_id,
       active_claim_id, branch_name, branch_kind, base_branch, head_sha, base_sha,
       write_scope_json, review_doc_key, status, updated_by, created_at, updated_at
  FROM project_branch_registry
 WHERE branch_id = ?`,
		strings.TrimSpace(branchID))
	return scanProjectBranch(row)
}

func validateProjectBranchIDScopeTx(ctx context.Context, tx *sql.Tx, branchID, workspaceID, projectID, repoID string) (ProjectBranchRecord, bool, error) {
	branch, err := getProjectBranchByIDTx(ctx, tx, branchID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProjectBranchRecord{}, false, nil
		}
		return ProjectBranchRecord{}, false, err
	}
	if branch.WorkspaceID != strings.TrimSpace(workspaceID) || branch.ProjectID != strings.TrimSpace(projectID) || branch.RepoID != strings.TrimSpace(repoID) {
		return ProjectBranchRecord{}, false, fmt.Errorf("%w: branch_id=%s belongs to workspace=%s project=%s repo=%s", ErrProjectBranchScopeMismatch, branchID, branch.WorkspaceID, branch.ProjectID, branch.RepoID)
	}
	return branch, true, nil
}

func findLiveProjectBranchByNameTx(ctx context.Context, tx *sql.Tx, workspaceID, repoID, branchName string) (ProjectBranchRecord, bool, error) {
	row := tx.QueryRowContext(ctx, `
SELECT branch_id, workspace_id, project_id, repo_id, checkout_id, agent_id, active_task_id,
       active_claim_id, branch_name, branch_kind, base_branch, head_sha, base_sha,
       write_scope_json, review_doc_key, status, updated_by, created_at, updated_at
  FROM project_branch_registry
 WHERE workspace_id = ? AND repo_id = ? AND branch_name = ? AND status IN ('RESERVED', 'ACTIVE', 'BLOCKED', 'READY_FOR_REVIEW')
 ORDER BY updated_at DESC
 LIMIT 1`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(repoID), strings.TrimSpace(branchName))
	branch, err := scanProjectBranch(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProjectBranchRecord{}, false, nil
		}
		return ProjectBranchRecord{}, false, err
	}
	return branch, true, nil
}

func projectBranchTerminalIntegrationTargetReregisterRequested(branchKind, status, branchName, headSHA, activeTaskID, activeClaimID string) bool {
	return strings.EqualFold(strings.TrimSpace(branchKind), ProjectBranchKindIntegration) &&
		strings.ToUpper(strings.TrimSpace(status)) == ProjectBranchStatusMerged &&
		strings.TrimSpace(branchName) != "" &&
		strings.TrimSpace(headSHA) != "" &&
		strings.TrimSpace(activeTaskID) == "" &&
		strings.TrimSpace(activeClaimID) == ""
}

func findMergedProjectIntegrationTargetBranchTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, repoID, branchName, headSHA string) (ProjectBranchRecord, bool, error) {
	row := tx.QueryRowContext(ctx, `
SELECT branch_id, workspace_id, project_id, repo_id, checkout_id, agent_id, active_task_id,
       active_claim_id, branch_name, branch_kind, base_branch, head_sha, base_sha,
       write_scope_json, review_doc_key, status, updated_by, created_at, updated_at
  FROM project_branch_registry
 WHERE workspace_id = ? AND project_id = ? AND repo_id = ?
   AND branch_kind = ? AND branch_name = ? AND status = ?
   AND TRIM(head_sha) = ?
 ORDER BY created_at ASC, branch_id ASC
 LIMIT 1`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(projectID), strings.TrimSpace(repoID),
		ProjectBranchKindIntegration, strings.TrimSpace(branchName), ProjectBranchStatusMerged, strings.TrimSpace(headSHA))
	branch, err := scanProjectBranch(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProjectBranchRecord{}, false, nil
		}
		return ProjectBranchRecord{}, false, err
	}
	return branch, true, nil
}

func resolveProjectBranchAgentID(actorType, actorID, inputAgentID, branchID, status string) (string, error) {
	actorID = strings.TrimSpace(actorID)
	agentID := strings.TrimSpace(inputAgentID)
	if !strings.EqualFold(strings.TrimSpace(actorType), "agent") {
		return agentID, nil
	}
	if agentID == "" {
		return actorID, nil
	}
	if agentID != actorID && !projectBranchCrossAgentTerminalMergeRequested(branchID, status) {
		return "", ErrProjectLeadMismatch
	}
	return agentID, nil
}

func projectBranchCrossAgentTerminalMergeRequested(branchID, status string) bool {
	return strings.TrimSpace(branchID) != "" && strings.ToUpper(strings.TrimSpace(status)) == ProjectBranchStatusMerged
}

func (s *Store) requireProjectBranchActorOwnsExistingTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, actorType, actorID, agentID, nextStatus, nextHeadSHA, now string, existing ProjectBranchRecord) error {
	if !strings.EqualFold(strings.TrimSpace(actorType), "agent") {
		return nil
	}
	actorID = strings.TrimSpace(actorID)
	agentID = strings.TrimSpace(agentID)
	existingAgentID := strings.TrimSpace(existing.AgentID)
	if actorID != "" && existingAgentID == actorID {
		return nil
	}
	if agentID != "" && existingAgentID != "" && existingAgentID == agentID &&
		projectBranchCrossAgentTerminalMergeRequested(existing.BranchID, nextStatus) {
		switch strings.ToUpper(strings.TrimSpace(existing.Status)) {
		case ProjectBranchStatusReadyForReview, ProjectBranchStatusMerged:
		default:
			return fmt.Errorf("%w: cross-agent MERGED branch registration requires READY_FOR_REVIEW source branch, got %s", ErrProjectBranchActiveReferenceInvalid, strings.TrimSpace(existing.Status))
		}
		allowed, err := s.projectBranchActorHasMergeAuthorityTx(ctx, tx, workspaceID, projectID, actorID, now)
		if err != nil {
			return err
		}
		if !allowed {
			return fmt.Errorf("%w: branch %s belongs to agent %s", ErrProjectBranchScopeMismatch, strings.TrimSpace(existing.BranchID), existingAgentID)
		}
		accepted, err := s.projectBranchHasAcceptedOrIntegratedPatchQueueEvidenceTx(ctx, tx, workspaceID, projectID, nextHeadSHA, existing)
		if err != nil {
			return err
		}
		if accepted {
			return nil
		}
		return fmt.Errorf("%w: branch %s requires ACCEPTED patch queue evidence or INTEGRATED patch queue lifecycle evidence before cross-agent MERGED registration", ErrProjectBranchReviewEvidenceInvalid, strings.TrimSpace(existing.BranchID))
	}
	return fmt.Errorf("%w: branch %s belongs to agent %s", ErrProjectBranchScopeMismatch, strings.TrimSpace(existing.BranchID), existingAgentID)
}

func (s *Store) projectBranchHasAcceptedOrIntegratedPatchQueueEvidenceTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, nextHeadSHA string, existing ProjectBranchRecord) (bool, error) {
	headSHA := firstNonEmpty(strings.TrimSpace(nextHeadSHA), strings.TrimSpace(existing.HeadSHA))
	if headSHA == "" {
		return false, nil
	}
	var count int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM project_patch_queue_items
 WHERE workspace_id = ? AND project_id = ? AND repo_id = ? AND branch_id = ?
   AND state IN (?, ?) AND TRIM(head_sha) = ?`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(projectID), strings.TrimSpace(existing.RepoID), strings.TrimSpace(existing.BranchID),
		ProjectPatchQueueStateAccepted, ProjectPatchQueueStateIntegrated, headSHA).Scan(&count); err != nil {
		return false, fmt.Errorf("check accepted or integrated project patch queue evidence: %w", err)
	}
	return count > 0, nil
}

func projectBranchIsIntegrationTarget(existing ProjectBranchRecord) bool {
	return strings.EqualFold(strings.TrimSpace(existing.BranchKind), ProjectBranchKindIntegration)
}

func getAcceptedOrIntegratedProjectPatchQueueItemByBranchHeadTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, branchID, headSHA string) (ProjectPatchQueueItemRecord, bool, error) {
	row := tx.QueryRowContext(ctx, `
SELECT `+projectPatchQueueItemSelectColumns+`
  FROM project_patch_queue_items
 WHERE workspace_id = ? AND project_id = ? AND branch_id = ? AND state IN ('ACCEPTED', 'INTEGRATED')
   AND TRIM(head_sha) = ?
 ORDER BY updated_at DESC, decided_at DESC, queue_id ASC, item_id ASC
 LIMIT 1`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(projectID), strings.TrimSpace(branchID), strings.TrimSpace(headSHA))
	item, err := scanProjectPatchQueueItem(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProjectPatchQueueItemRecord{}, false, nil
		}
		return ProjectPatchQueueItemRecord{}, false, err
	}
	return item, true, nil
}

func projectBranchHasIntegratedPatchQueueReceiptTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID string, accepted ProjectPatchQueueItemRecord, sourceHeadSHA string) (bool, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM runtime_events
 WHERE workspace_id = ?
   AND event_type = ?
   AND entity_type = 'project_patch_queue_item'
   AND entity_id = ?
   AND json_valid(payload_json)
   AND TRIM(COALESCE(json_extract(payload_json, '$.project_id'), '')) = ?
   AND TRIM(COALESCE(json_extract(payload_json, '$.source_branch_id'), '')) = ?
   AND TRIM(COALESCE(json_extract(payload_json, '$.source_head_sha'), '')) = ?`,
		strings.TrimSpace(workspaceID),
		ProjectPatchQueueIntegratedEventType,
		strings.TrimSpace(accepted.QueueID)+"/"+strings.TrimSpace(accepted.ItemID),
		strings.TrimSpace(projectID),
		strings.TrimSpace(accepted.BranchID),
		strings.TrimSpace(sourceHeadSHA),
	).Scan(&count); err != nil {
		return false, fmt.Errorf("check integrated project patch queue receipt: %w", err)
	}
	return count > 0, nil
}

func (s *Store) validateProjectBranchMergedTransitionTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, actorID, branchID, nextHeadSHA string, existing ProjectBranchRecord, now string) error {
	allowed, err := s.projectBranchActorHasMergeAuthorityTx(ctx, tx, workspaceID, projectID, actorID, now)
	if err != nil {
		return err
	}
	if !allowed {
		return fmt.Errorf("%w: branch %s MERGED registration requires integration authority", ErrProjectBranchScopeMismatch, strings.TrimSpace(branchID))
	}
	headSHA := firstNonEmpty(strings.TrimSpace(nextHeadSHA), strings.TrimSpace(existing.HeadSHA))
	accepted, ok, err := getAcceptedOrIntegratedProjectPatchQueueItemByBranchHeadTx(ctx, tx, workspaceID, projectID, existing.BranchID, headSHA)
	if err != nil {
		return err
	}
	if ok {
		switch strings.ToUpper(strings.TrimSpace(existing.Status)) {
		case ProjectBranchStatusReadyForReview, ProjectBranchStatusMerged:
		default:
			return fmt.Errorf("%w: MERGED branch registration requires READY_FOR_REVIEW source branch, got %s", ErrProjectBranchActiveReferenceInvalid, strings.TrimSpace(existing.Status))
		}
		integrated, err := projectBranchHasIntegratedPatchQueueReceiptTx(ctx, tx, workspaceID, projectID, accepted, headSHA)
		if err != nil {
			return err
		}
		if !integrated {
			return fmt.Errorf("%w: branch %s requires durable integrated patch queue receipt before MERGED registration", ErrProjectBranchReviewEvidenceInvalid, strings.TrimSpace(branchID))
		}
		return nil
	}
	if latestAccepted, acceptedExists, err := getLatestAcceptedProjectPatchQueueItemByBranchTx(ctx, tx, workspaceID, projectID, existing.BranchID); err != nil {
		return err
	} else if acceptedExists {
		return fmt.Errorf("%w: branch %s has ACCEPTED patch queue item %s/%s at head %s; accepted source boundary cannot use integration-target MERGED exemption without matching integrated receipt", ErrProjectBranchReviewEvidenceInvalid, strings.TrimSpace(branchID), latestAccepted.QueueID, latestAccepted.ItemID, strings.TrimSpace(latestAccepted.HeadSHA))
	}
	if projectBranchIsIntegrationTarget(existing) {
		return nil
	}
	switch strings.ToUpper(strings.TrimSpace(existing.Status)) {
	case ProjectBranchStatusReadyForReview, ProjectBranchStatusMerged:
	default:
		return fmt.Errorf("%w: MERGED branch registration requires READY_FOR_REVIEW source branch, got %s", ErrProjectBranchActiveReferenceInvalid, strings.TrimSpace(existing.Status))
	}
	return fmt.Errorf("%w: branch %s requires matching ACCEPTED patch queue evidence or INTEGRATED patch queue lifecycle evidence before MERGED registration", ErrProjectBranchReviewEvidenceInvalid, strings.TrimSpace(branchID))
}

func (s *Store) projectBranchActorHasMergeAuthorityTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, actorID, now string) (bool, error) {
	actorID = strings.TrimSpace(actorID)
	if actorID == "" {
		return false, nil
	}
	var count int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM project_agent_roles
 WHERE workspace_id = ? AND project_id = ? AND agent_id = ? AND status = 'ACTIVE'
   AND (
     role_type = ?
     OR (role_type = ? AND (lease_expires_at = '' OR lease_expires_at > ?))
   )`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(projectID), actorID,
		ProjectRoleIntegrator, ProjectRoleStrategicLead, strings.TrimSpace(now)).Scan(&count); err != nil {
		return false, fmt.Errorf("check project branch merge authority: %w", err)
	}
	if count > 0 {
		return true, nil
	}
	var registeredRole string
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(role, '')
  FROM agents
 WHERE workspace_id = ? AND agent_id = ?`,
		strings.TrimSpace(workspaceID), actorID).Scan(&registeredRole); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("check project branch registered agent role: %w", err)
	}
	return projectBranchRegisteredAgentRoleAllowsMerge(registeredRole), nil
}

func projectBranchRegisteredAgentRoleAllowsMerge(role string) bool {
	role = strings.ToLower(strings.TrimSpace(role))
	switch role {
	case "integrator", "integration", "synthesis", "synthesizer":
		return true
	default:
		return strings.Contains(role, "integrat") ||
			strings.Contains(role, "synthes")
	}
}

func validateProjectBranchTerminalTransition(existing ProjectBranchRecord, nextStatus string) error {
	existingStatus := strings.ToUpper(strings.TrimSpace(existing.Status))
	nextStatus = strings.ToUpper(strings.TrimSpace(nextStatus))
	if !projectBranchStatusIsTerminal(existingStatus) {
		return nil
	}
	if nextStatus == existingStatus {
		return nil
	}
	if existingStatus != ProjectBranchStatusArchived && nextStatus == ProjectBranchStatusArchived {
		return nil
	}
	return fmt.Errorf("%w: terminal branch %s status %s cannot transition to %s", ErrProjectBranchScopeMismatch, strings.TrimSpace(existing.BranchID), existingStatus, nextStatus)
}

func validateProjectBranchTerminalActiveReferences(nextStatus, activeTaskID, activeClaimID string) error {
	if !projectBranchStatusIsTerminal(nextStatus) {
		return nil
	}
	if strings.TrimSpace(activeTaskID) == "" && strings.TrimSpace(activeClaimID) == "" {
		return nil
	}
	return fmt.Errorf("%w: terminal branch status %s cannot be registered with active_task_id=%s active_claim_id=%s", ErrProjectBranchActiveReferenceInvalid, strings.ToUpper(strings.TrimSpace(nextStatus)), strings.TrimSpace(activeTaskID), strings.TrimSpace(activeClaimID))
}

func validateProjectBranchAbandonTransition(existing ProjectBranchRecord, nextStatus, nextActiveTaskID, nextActiveClaimID string) error {
	if strings.ToUpper(strings.TrimSpace(nextStatus)) != ProjectBranchStatusAbandoned {
		return nil
	}
	if strings.TrimSpace(nextActiveTaskID) != "" || strings.TrimSpace(nextActiveClaimID) != "" {
		return fmt.Errorf("%w: branch %s cannot be abandoned with active_task_id=%s active_claim_id=%s", ErrProjectBranchActiveReferenceInvalid, strings.TrimSpace(existing.BranchID), strings.TrimSpace(nextActiveTaskID), strings.TrimSpace(nextActiveClaimID))
	}
	if strings.TrimSpace(existing.ActiveTaskID) != "" || strings.TrimSpace(existing.ActiveClaimID) != "" {
		return fmt.Errorf("%w: branch %s cannot be abandoned while active_task_id=%s active_claim_id=%s", ErrProjectBranchActiveReferenceInvalid, strings.TrimSpace(existing.BranchID), strings.TrimSpace(existing.ActiveTaskID), strings.TrimSpace(existing.ActiveClaimID))
	}
	switch strings.ToUpper(strings.TrimSpace(existing.Status)) {
	case ProjectBranchStatusReserved, ProjectBranchStatusAbandoned:
		return nil
	default:
		return fmt.Errorf("%w: branch %s status %s cannot be abandoned through project.branch.register; release or complete the active task claim first", ErrProjectBranchActiveReferenceInvalid, strings.TrimSpace(existing.BranchID), strings.TrimSpace(existing.Status))
	}
}

func validateProjectBranchScopeTransition(existing ProjectBranchRecord, nextScopeJSON, nextStatus string) error {
	if strings.ToUpper(strings.TrimSpace(nextStatus)) != ProjectBranchStatusReadyForReview {
		return nil
	}
	existingPaths := projectBranchReviewScopePaths(existing.WriteScopeJSON)
	nextPaths := projectBranchReviewScopePaths(nextScopeJSON)
	if len(nextPaths) == 0 {
		return fmt.Errorf("%w: READY_FOR_REVIEW requires non-empty write_scope_json paths", ErrProjectBranchScopeMismatch)
	}
	if len(existingPaths) == 0 {
		return nil
	}
	if !writeScopePathsCoveredByWithAdjacentSidecars(nextPaths, existingPaths) {
		return fmt.Errorf("%w: READY_FOR_REVIEW write_scope_json cannot widen existing branch scope", ErrProjectBranchScopeMismatch)
	}
	return nil
}

func (s *Store) validateProjectBranchReviewReadyEvidenceTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, reviewDocKey string) error {
	reviewDocKey = strings.TrimSpace(reviewDocKey)
	if reviewDocKey == "" {
		return fmt.Errorf("%w: READY_FOR_REVIEW requires review_doc_key", ErrProjectBranchReviewEvidenceInvalid)
	}
	doc, err := s.loadWorkspaceDocTx(ctx, tx, workspaceID, reviewDocKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: review_doc_key %s not found", ErrProjectBranchReviewEvidenceInvalid, reviewDocKey)
		}
		return err
	}
	if doc.ArchivedAt != nil {
		return fmt.Errorf("%w: review_doc_key %s is archived", ErrProjectBranchReviewEvidenceInvalid, reviewDocKey)
	}
	fidelity, err := s.projectSourceFidelityContextTx(ctx, tx, workspaceID, projectID)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrProjectBranchReviewEvidenceInvalid, err)
	}
	if err := s.validateProjectSourceFidelityTraceTx(ctx, tx, workspaceID, fidelity); err != nil {
		return fmt.Errorf("%w: %v", ErrProjectBranchReviewEvidenceInvalid, err)
	}
	if err := validateProjectSourceFidelityReviewDoc(doc, fidelity); err != nil {
		return fmt.Errorf("%w: %v", ErrProjectBranchReviewEvidenceInvalid, err)
	}
	return nil
}

func validateProjectBranchReadyObjectIDs(headSHA, baseSHA string) error {
	if !isCanonicalProjectGitObjectID(headSHA) {
		return fmt.Errorf("%w: READY_FOR_REVIEW requires canonical head_sha", ErrProjectBranchReviewEvidenceInvalid)
	}
	if !isCanonicalProjectGitObjectID(baseSHA) {
		return fmt.Errorf("%w: READY_FOR_REVIEW requires canonical base_sha", ErrProjectBranchReviewEvidenceInvalid)
	}
	return nil
}

func projectBranchQueueEvidenceChanged(existing ProjectBranchRecord, branchName, branchKind, headSHA, baseSHA, reviewDocKey, writeScopeJSON string) bool {
	return strings.TrimSpace(existing.BranchName) != strings.TrimSpace(branchName) ||
		strings.TrimSpace(existing.BranchKind) != strings.TrimSpace(branchKind) ||
		strings.TrimSpace(existing.HeadSHA) != strings.TrimSpace(headSHA) ||
		strings.TrimSpace(existing.BaseSHA) != strings.TrimSpace(baseSHA) ||
		strings.TrimSpace(existing.ReviewDocKey) != strings.TrimSpace(reviewDocKey) ||
		strings.TrimSpace(existing.WriteScopeJSON) != strings.TrimSpace(writeScopeJSON)
}

func preserveProjectBranchReadyStatusOnNoopReregister(existing ProjectBranchRecord, nextStatus, branchName, branchKind, headSHA, baseSHA, reviewDocKey, writeScopeJSON string) string {
	existingStatus := strings.ToUpper(strings.TrimSpace(existing.Status))
	nextStatus = strings.ToUpper(strings.TrimSpace(nextStatus))
	if existingStatus != ProjectBranchStatusReadyForReview || nextStatus == ProjectBranchStatusReadyForReview || projectBranchStatusIsTerminal(nextStatus) {
		return nextStatus
	}
	if projectBranchQueueEvidenceChanged(existing, branchName, branchKind, headSHA, baseSHA, reviewDocKey, writeScopeJSON) {
		return nextStatus
	}
	return ProjectBranchStatusReadyForReview
}

func isCanonicalProjectGitObjectID(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, r := range value {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') {
			continue
		}
		return false
	}
	return true
}

func writeScopePathsCoveredBy(childPaths, parentPaths []string) bool {
	if len(childPaths) == 0 || len(parentPaths) == 0 {
		return false
	}
	for _, child := range childPaths {
		covered := false
		for _, parent := range parentPaths {
			if writeScopePathCoveredBy(child, parent) {
				covered = true
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}

func writeScopePathCoveredBy(child, parent string) bool {
	child = normalizeProjectBranchReviewScopePath(child)
	parent = normalizeProjectBranchReviewScopePath(parent)
	if parent == "" || parent == "*" || parent == "**" {
		return true
	}
	if child == "" {
		return false
	}
	if child == parent {
		return true
	}
	if strings.HasSuffix(parent, "/**") {
		prefix := strings.TrimSuffix(parent, "/**")
		return child == prefix || strings.HasPrefix(child, prefix+"/")
	}
	if strings.HasSuffix(parent, "/*") {
		prefix := strings.TrimSuffix(parent, "/*")
		if child == prefix || !strings.HasPrefix(child, prefix+"/") {
			return false
		}
		return !strings.Contains(strings.TrimPrefix(child, prefix+"/"), "/")
	}
	return false
}

func projectBranchReviewScopePaths(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" || !json.Valid([]byte(raw)) {
		return nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil
	}
	seen := map[string]struct{}{}
	var paths []string
	add := func(value string) {
		value = normalizeProjectBranchReviewScopePath(value)
		if value == "" {
			return
		}
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		paths = append(paths, value)
	}
	var walk func(any)
	walk = func(value any) {
		switch typed := value.(type) {
		case string:
			add(typed)
		case []any:
			for _, item := range typed {
				walk(item)
			}
		}
	}
	if object, ok := decoded.(map[string]any); ok {
		for _, key := range []string{"paths", "files", "path_prefixes", "write_paths", "scopes"} {
			walk(object[key])
		}
		return paths
	}
	walk(decoded)
	return paths
}

func normalizeProjectBranchReviewScopePath(value string) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, `\`, `/`))
	value = strings.Trim(value, "/")
	if value == "." {
		return ""
	}
	return strings.ToLower(value)
}

func projectBranchStatusIsTerminal(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case ProjectBranchStatusMerged, ProjectBranchStatusAbandoned, ProjectBranchStatusArchived:
		return true
	default:
		return false
	}
}

func (s *Store) ensureProjectBranchActiveReferencesTx(ctx context.Context, tx *sql.Tx, workspaceID, projectID, agentID, activeTaskID, activeClaimID string) error {
	if err := s.ensureProjectCheckoutActiveReferencesTx(ctx, tx, workspaceID, projectID, agentID, activeTaskID, activeClaimID); err != nil {
		if errors.Is(err, ErrProjectCheckoutActiveReferenceInvalid) {
			return fmt.Errorf("%w: %v", ErrProjectBranchActiveReferenceInvalid, err)
		}
		return err
	}
	return nil
}

func (s *Store) projectBranchWriteScopeBoundToActiveClaimTx(ctx context.Context, tx *sql.Tx, workspaceID, agentID, activeTaskID, activeClaimID, writeScopeJSON string, explicitWriteScope bool, status string) (string, error) {
	activeClaimID = strings.TrimSpace(activeClaimID)
	if activeClaimID == "" {
		return strings.TrimSpace(writeScopeJSON), nil
	}
	// active_claim_id is keyed as task_id in task_claims; canonicalize a case/alias variant (SA-4).
	if resolved, rerr := s.resolveTaskIDWithQuerier(ctx, tx, workspaceID, activeClaimID); rerr == nil {
		activeClaimID = resolved
	}
	var claimTaskID, claimAgentID, claimStatus, claimWriteScopeJSON string
	err := tx.QueryRowContext(ctx, `
SELECT task_id, agent_id, claim_status, COALESCE(write_scope_json, '')
  FROM task_claims
 WHERE workspace_id = ? AND task_id = ?`,
		strings.TrimSpace(workspaceID), activeClaimID).Scan(&claimTaskID, &claimAgentID, &claimStatus, &claimWriteScopeJSON)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("%w: active claim %s not found", ErrProjectBranchActiveReferenceInvalid, activeClaimID)
		}
		return "", fmt.Errorf("load active claim write scope: %w", err)
	}
	if strings.TrimSpace(activeTaskID) != "" && strings.TrimSpace(claimTaskID) != strings.TrimSpace(activeTaskID) {
		return "", fmt.Errorf("%w: active_claim_id must reference active_task_id", ErrProjectBranchActiveReferenceInvalid)
	}
	if strings.TrimSpace(agentID) != "" && strings.TrimSpace(claimAgentID) != strings.TrimSpace(agentID) {
		return "", fmt.Errorf("%w: active claim belongs to agent %s", ErrProjectBranchActiveReferenceInvalid, strings.TrimSpace(claimAgentID))
	}
	if strings.ToUpper(strings.TrimSpace(claimStatus)) != model.TaskClaimStatusClaimed {
		return "", fmt.Errorf("%w: active claim %s has status %s", ErrProjectBranchActiveReferenceInvalid, activeClaimID, strings.TrimSpace(claimStatus))
	}
	claimPaths := projectBranchReviewScopePaths(claimWriteScopeJSON)
	if len(claimPaths) == 0 {
		return strings.TrimSpace(writeScopeJSON), nil
	}
	writeScopeJSON = strings.TrimSpace(writeScopeJSON)
	nextPaths := projectBranchReviewScopePaths(writeScopeJSON)
	if len(nextPaths) == 0 {
		return strings.TrimSpace(claimWriteScopeJSON), nil
	}
	if explicitWriteScope && !writeScopePathsSameSet(nextPaths, claimPaths) {
		if writeScopePathsCoveredBy(claimPaths, nextPaths) && writeScopePathsCoveredByWithAdjacentSidecars(nextPaths, claimPaths) {
			return strings.TrimSpace(writeScopeJSON), nil
		}
		if strings.ToUpper(strings.TrimSpace(status)) == ProjectBranchStatusReadyForReview && writeScopePathsCoveredByWithAdjacentSidecars(nextPaths, claimPaths) {
			return strings.TrimSpace(writeScopeJSON), nil
		}
		return "", fmt.Errorf("%w: active branch write_scope_json must match active claim scope", ErrProjectBranchScopeMismatch)
	}
	return strings.TrimSpace(claimWriteScopeJSON), nil
}

func (s *Store) validateProjectBranchActiveTaskPathsetBindingTx(ctx context.Context, tx *sql.Tx, branch ProjectBranchRecord, pathset []string) error {
	activeTaskID := strings.TrimSpace(branch.ActiveTaskID)
	if activeTaskID == "" {
		return nil
	}
	activeClaimID := strings.TrimSpace(branch.ActiveClaimID)
	if activeClaimID == "" {
		return fmt.Errorf("%w: branch %s active_task_id=%s requires active_claim_id for lane/pathset binding", ErrProjectBranchActiveReferenceInvalid, strings.TrimSpace(branch.BranchID), activeTaskID)
	}
	if len(pathset) == 0 {
		return fmt.Errorf("%w: branch %s active_task_id=%s cannot be bound to an empty pathset", ErrProjectBranchScopeMismatch, strings.TrimSpace(branch.BranchID), activeTaskID)
	}
	if resolved, rerr := s.resolveTaskIDWithQuerier(ctx, tx, strings.TrimSpace(branch.WorkspaceID), activeTaskID); rerr == nil {
		activeTaskID = resolved
	}
	if resolved, rerr := s.resolveTaskIDWithQuerier(ctx, tx, strings.TrimSpace(branch.WorkspaceID), activeClaimID); rerr == nil {
		activeClaimID = resolved
	}
	if activeClaimID != activeTaskID {
		return fmt.Errorf("%w: branch %s active_claim_id must reference active_task_id", ErrProjectBranchActiveReferenceInvalid, strings.TrimSpace(branch.BranchID))
	}
	branchID := strings.TrimSpace(branch.BranchID)
	if branchID == "" {
		return fmt.Errorf("%w: active task binding requires branch_id", ErrProjectBranchActiveReferenceInvalid)
	}
	var claimTaskID, claimAgentID, claimStatus, claimBranchID, claimScopeJSON string
	err := tx.QueryRowContext(ctx, `
SELECT task_id, agent_id, claim_status, COALESCE(branch_id, ''), COALESCE(write_scope_json, '')
  FROM task_claims INDEXED BY idx_task_claims_workspace_branch_status
 WHERE workspace_id = ?
   AND branch_id = ?
   AND claim_status = ?
   AND task_id = ?`,
		strings.TrimSpace(branch.WorkspaceID), branchID, model.TaskClaimStatusClaimed, activeClaimID).Scan(&claimTaskID, &claimAgentID, &claimStatus, &claimBranchID, &claimScopeJSON)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: branch %s active claim %s is not CLAIMED on this branch", ErrProjectBranchActiveReferenceInvalid, strings.TrimSpace(branch.BranchID), activeClaimID)
		}
		return fmt.Errorf("load branch active claim binding: %w", err)
	}
	if strings.TrimSpace(claimTaskID) != activeTaskID {
		return fmt.Errorf("%w: branch %s active claim %s does not reference active_task_id %s", ErrProjectBranchActiveReferenceInvalid, strings.TrimSpace(branch.BranchID), activeClaimID, activeTaskID)
	}
	if strings.TrimSpace(branch.AgentID) == "" {
		return fmt.Errorf("%w: branch %s active task binding requires agent_id", ErrProjectBranchActiveReferenceInvalid, strings.TrimSpace(branch.BranchID))
	}
	if strings.TrimSpace(claimBranchID) == "" || strings.TrimSpace(claimBranchID) != branchID {
		return fmt.Errorf("%w: branch %s active claim %s is bound to branch_id=%s", ErrProjectBranchActiveReferenceInvalid, branchID, activeClaimID, firstNonEmpty(strings.TrimSpace(claimBranchID), "<empty>"))
	}
	if strings.TrimSpace(claimAgentID) != strings.TrimSpace(branch.AgentID) {
		return fmt.Errorf("%w: branch %s active claim belongs to agent %s", ErrProjectBranchActiveReferenceInvalid, strings.TrimSpace(branch.BranchID), strings.TrimSpace(claimAgentID))
	}
	if strings.ToUpper(strings.TrimSpace(claimStatus)) != model.TaskClaimStatusClaimed {
		return fmt.Errorf("%w: branch %s active claim %s has status %s", ErrProjectBranchActiveReferenceInvalid, strings.TrimSpace(branch.BranchID), activeClaimID, strings.TrimSpace(claimStatus))
	}
	if err := s.ensureProjectCheckoutTaskRefTx(ctx, tx, strings.TrimSpace(branch.WorkspaceID), strings.TrimSpace(branch.ProjectID), activeTaskID); err != nil {
		if errors.Is(err, ErrProjectCheckoutActiveReferenceInvalid) {
			return fmt.Errorf("%w: %v", ErrProjectBranchActiveReferenceInvalid, err)
		}
		return err
	}
	claimPaths := projectBranchReviewScopePaths(claimScopeJSON)
	if len(claimPaths) == 0 {
		return fmt.Errorf("%w: branch %s active task %s has no claim write_scope_json paths", ErrProjectBranchScopeMismatch, strings.TrimSpace(branch.BranchID), activeTaskID)
	}
	authoritativeScopeJSON, ok, err := taskClaimAuthoritativeWriteScopeJSONTx(ctx, tx, strings.TrimSpace(branch.WorkspaceID), activeTaskID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: branch %s active task %s has no authoritative implementation lane scope", ErrProjectBranchScopeMismatch, strings.TrimSpace(branch.BranchID), activeTaskID)
	}
	authoritativePaths := projectBranchReviewScopePaths(authoritativeScopeJSON)
	if !writeScopePathsCoveredByWithAdjacentSidecars(claimPaths, authoritativePaths) {
		return fmt.Errorf("%w: branch %s active task %s claim write_scope_json is outside authoritative lane scope", ErrProjectBranchScopeMismatch, strings.TrimSpace(branch.BranchID), activeTaskID)
	}
	if !writeScopePathsCoveredByWithAdjacentSidecars(pathset, claimPaths) {
		return fmt.Errorf("%w: branch %s pathset is outside active task %s claim write_scope_json", ErrProjectBranchScopeMismatch, strings.TrimSpace(branch.BranchID), activeTaskID)
	}
	if !writeScopePathsCoveredByWithAdjacentSidecars(pathset, authoritativePaths) {
		return fmt.Errorf("%w: branch %s pathset is outside active task %s authoritative lane scope", ErrProjectBranchScopeMismatch, strings.TrimSpace(branch.BranchID), activeTaskID)
	}
	return nil
}

func writeScopePathsCoveredByWithAdjacentSidecars(childPaths, parentPaths []string) bool {
	if len(childPaths) == 0 || len(parentPaths) == 0 {
		return false
	}
	for _, child := range childPaths {
		child = normalizeProjectBranchReviewScopePath(child)
		if child == "" {
			continue
		}
		if writeScopePathsCoveredBy([]string{child}, parentPaths) {
			continue
		}
		if projectBranchAdjacentSidecarAllowed(parentPaths, child) {
			continue
		}
		return false
	}
	return true
}

func writeScopePathsCoverScopeJSON(childScopeJSON, parentScopeJSON string) bool {
	return writeScopePathsCoveredByWithAdjacentSidecars(
		projectBranchReviewScopePaths(childScopeJSON),
		projectBranchReviewScopePaths(parentScopeJSON),
	)
}

func projectBranchAdjacentSidecarAllowed(basePaths []string, candidate string) bool {
	candidate = normalizeProjectBranchReviewScopePath(candidate)
	switch candidate {
	case "readme.md", ".gitignore":
		return projectBranchScopeAllowsRootScaffoldSidecars(basePaths) || projectBranchScopeAllowsGoRootScaffoldSidecars(basePaths)
	case "go.mod", "go.sum":
		return projectBranchScopeAllowsGoRootScaffoldSidecars(basePaths) || projectBranchScopeAllowsGoModuleSidecars(basePaths)
	case "main.go":
		return projectBranchScopeAllowsGoRootScaffoldSidecars(basePaths)
	default:
		return false
	}
}

func projectBranchScopeAllowsRootScaffoldSidecars(paths []string) bool {
	hasPackageManifest := writeScopePathsCoveredBy([]string{"package.json"}, paths)
	hasRootConfig := writeScopePathsCoveredBy([]string{"index.html"}, paths) ||
		writeScopePathsCoveredBy([]string{"vite.config.ts"}, paths) ||
		writeScopePathsCoveredBy([]string{"vite.config.js"}, paths) ||
		writeScopePathsCoveredBy([]string{"next.config.js"}, paths) ||
		writeScopePathsCoveredBy([]string{"next.config.mjs"}, paths) ||
		writeScopePathsCoveredBy([]string{"tsconfig.json"}, paths)
	hasAppSurface := writeScopePathsCoveredBy([]string{"src/main.tsx"}, paths) ||
		writeScopePathsCoveredBy([]string{"src/main.ts"}, paths) ||
		writeScopePathsCoveredBy([]string{"src/index.tsx"}, paths) ||
		writeScopePathsCoveredBy([]string{"src/index.ts"}, paths) ||
		writeScopePathsCoveredBy([]string{"src/app/app.tsx"}, paths) ||
		writeScopePathsCoveredBy([]string{"src/styles/global.css"}, paths) ||
		writeScopePathsCoveredBy([]string{"public/index.html"}, paths) ||
		writeScopePathsCoveredBy([]string{"app/page.tsx"}, paths) ||
		writeScopePathsCoveredBy([]string{"pages/index.tsx"}, paths)
	return hasPackageManifest && hasRootConfig && hasAppSurface
}

func projectBranchScopeAllowsGoRootScaffoldSidecars(paths []string) bool {
	hasCommandSurface := writeScopePathsCoveredBy([]string{"cmd/rq/main.go"}, paths) ||
		writeScopePathsCoveredBy([]string{"cmd/main.go"}, paths) ||
		writeScopePathsCoveredBy([]string{"main.go"}, paths)
	hasCLISurface := writeScopePathsCoveredBy([]string{"internal/cli/repl.go"}, paths) ||
		writeScopePathsCoveredBy([]string{"internal/repl/repl.go"}, paths)
	hasGoImplementationSurface := writeScopePathsCoveredBy([]string{"internal/module.go"}, paths) ||
		writeScopePathsCoveredBy([]string{"pkg/module.go"}, paths)
	return hasCommandSurface && (hasCLISurface || hasGoImplementationSurface)
}

func projectBranchScopeAllowsGoModuleSidecars(paths []string) bool {
	for _, item := range paths {
		item = normalizeProjectBranchReviewScopePath(item)
		switch {
		case item == "main.go",
			item == "cmd/**",
			item == "cmd/*",
			strings.HasPrefix(item, "cmd/"),
			item == "internal/**",
			strings.HasPrefix(item, "internal/"),
			item == "pkg/**",
			strings.HasPrefix(item, "pkg/"):
			return true
		}
	}
	return false
}

func writeScopePathsSameSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	seen := make(map[string]int, len(left))
	for _, path := range left {
		path = normalizeProjectBranchReviewScopePath(path)
		if path == "" {
			continue
		}
		seen[path]++
	}
	for _, path := range right {
		path = normalizeProjectBranchReviewScopePath(path)
		if path == "" {
			continue
		}
		seen[path]--
	}
	for _, count := range seen {
		if count != 0 {
			return false
		}
	}
	return true
}

func scanProjectBranch(row interface{ Scan(dest ...any) error }) (ProjectBranchRecord, error) {
	var branch ProjectBranchRecord
	if err := row.Scan(
		&branch.BranchID,
		&branch.WorkspaceID,
		&branch.ProjectID,
		&branch.RepoID,
		&branch.CheckoutID,
		&branch.AgentID,
		&branch.ActiveTaskID,
		&branch.ActiveClaimID,
		&branch.BranchName,
		&branch.BranchKind,
		&branch.BaseBranch,
		&branch.HeadSHA,
		&branch.BaseSHA,
		&branch.WriteScopeJSON,
		&branch.ReviewDocKey,
		&branch.Status,
		&branch.UpdatedBy,
		&branch.CreatedAt,
		&branch.UpdatedAt,
	); err != nil {
		return ProjectBranchRecord{}, err
	}
	return branch, nil
}

func normalizeProjectBranchKind(value string) (string, error) {
	kind := strings.ToLower(strings.TrimSpace(value))
	if kind == "" {
		kind = ProjectBranchKindFeature
	}
	switch kind {
	case ProjectBranchKindFeature, ProjectBranchKindIntegration, ProjectBranchKindReview, ProjectBranchKindRelease, ProjectBranchKindScratch:
		return kind, nil
	default:
		return "", fmt.Errorf("invalid project branch kind: %s", value)
	}
}

func normalizeProjectBranchStatus(value string) (string, error) {
	status := strings.ToUpper(strings.TrimSpace(value))
	if status == "" {
		status = ProjectBranchStatusReserved
	}
	switch status {
	case ProjectBranchStatusReserved, ProjectBranchStatusActive, ProjectBranchStatusBlocked, ProjectBranchStatusReadyForReview, ProjectBranchStatusMerged, ProjectBranchStatusAbandoned, ProjectBranchStatusArchived:
		return status, nil
	default:
		return "", fmt.Errorf("invalid project branch status: %s", value)
	}
}

func normalizeProjectBranchWriteScope(value string) (string, error) {
	scope := strings.TrimSpace(value)
	if scope == "" {
		return "", nil
	}
	if _, err := validateWriteScopeJSONPathset(scope); err != nil {
		return "", fmt.Errorf("%w: %v", ErrProjectBranchScopeMismatch, err)
	}
	return scope, nil
}

func projectBranchEventPayload(branch ProjectBranchRecord, actorID, operation string) map[string]any {
	return map[string]any{
		"workspace_id":       branch.WorkspaceID,
		"project_id":         branch.ProjectID,
		"repo_id":            branch.RepoID,
		"branch_id":          branch.BranchID,
		"branch_name":        branch.BranchName,
		"review_doc_key":     branch.ReviewDocKey,
		"actor_id":           strings.TrimSpace(actorID),
		"entity_type":        "project_branch",
		"entity_id":          branch.BranchID,
		"summary":            "Project branch mutation: " + branch.BranchName + " " + branch.Status,
		"mutation_operation": operation,
		"branch":             branch,
	}
}
