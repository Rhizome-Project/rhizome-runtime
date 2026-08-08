package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

type ClaimLiberationNarrowInput struct {
	WorkspaceID       string
	ProjectID         string
	BranchID          string
	TaskID            string
	AgentID           string
	NewWriteScopeJSON string
	Reason            string
	EvidenceJSON      string
	ActorType         string
	ActorID           string
	ReferenceAt       string

	// Optional guardrails for watchdog-driven NARROW. Manual callers leave these
	// false/empty and still get the narrower CAS/branch-claim binding checks.
	RequireOwnerUnable    bool
	BlockedWriteScopeJSON string
}

type ClaimLiberationNarrowResult struct {
	Narrowed            bool               `json:"narrowed"`
	SkipReason          string             `json:"skip_reason,omitempty"`
	WorkspaceID         string             `json:"workspace_id"`
	ProjectID           string             `json:"project_id"`
	BranchID            string             `json:"branch_id"`
	TaskID              string             `json:"task_id"`
	AgentID             string             `json:"agent_id"`
	PreviousClaimScope  string             `json:"previous_claim_scope_json,omitempty"`
	PreviousBranchScope string             `json:"previous_branch_scope_json,omitempty"`
	NewWriteScopeJSON   string             `json:"new_write_scope_json,omitempty"`
	Event               RuntimeEventRecord `json:"event,omitempty"`
}

func (s *Store) NarrowProjectClaimAndBranchScopeWithEvent(ctx context.Context, input ClaimLiberationNarrowInput) (ClaimLiberationNarrowResult, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	projectID := strings.TrimSpace(input.ProjectID)
	branchID := strings.TrimSpace(input.BranchID)
	taskID := strings.TrimSpace(input.TaskID)
	agentID := strings.TrimSpace(input.AgentID)
	if workspaceID == "" || projectID == "" || branchID == "" || taskID == "" || agentID == "" {
		return ClaimLiberationNarrowResult{}, errors.New("workspace_id, project_id, branch_id, task_id, and agent_id are required")
	}
	newScopeJSON, err := normalizeProjectBranchWriteScope(input.NewWriteScopeJSON)
	if err != nil {
		return ClaimLiberationNarrowResult{}, err
	}
	newPaths := writeScopePaths(newScopeJSON)
	if len(newPaths) == 0 {
		return ClaimLiberationNarrowResult{}, errors.New("new_write_scope_json must include non-empty paths")
	}
	actorType, actorID, err := normalizeLocalAuthorityActor(input.ActorType, input.ActorID)
	if err != nil {
		return ClaimLiberationNarrowResult{}, err
	}
	if actorID == "" || actorID == "authority_spine" {
		actorType = authorityActorTypeSystem
		actorID = "claim_liberation_watchdog"
	}
	referenceAt, err := parseSessionReclaimReferenceAt(input.ReferenceAt)
	if err != nil {
		return ClaimLiberationNarrowResult{}, err
	}
	referenceAtRaw := referenceAt.Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, referenceAtRaw)
	if err != nil {
		return ClaimLiberationNarrowResult{}, err
	}

	result := ClaimLiberationNarrowResult{
		WorkspaceID:       workspaceID,
		ProjectID:         projectID,
		BranchID:          branchID,
		TaskID:            taskID,
		AgentID:           agentID,
		NewWriteScopeJSON: newScopeJSON,
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return result, fmt.Errorf("begin claim liberation narrow tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		branch, err := getProjectBranchByIDTx(ctx, tx, branchID)
		if err != nil {
			return err
		}
		if strings.TrimSpace(branch.WorkspaceID) != workspaceID || strings.TrimSpace(branch.ProjectID) != projectID {
			return fmt.Errorf("branch %s belongs to workspace=%s project=%s", branchID, branch.WorkspaceID, branch.ProjectID)
		}
		if strings.TrimSpace(branch.AgentID) != agentID {
			return fmt.Errorf("branch %s belongs to agent %s", branchID, branch.AgentID)
		}
		if strings.TrimSpace(branch.ActiveClaimID) != taskID {
			return fmt.Errorf("branch %s active_claim_id=%s does not match task claim %s", branchID, strings.TrimSpace(branch.ActiveClaimID), taskID)
		}
		if activeTask := strings.TrimSpace(branch.ActiveTaskID); activeTask != "" && activeTask != taskID {
			return fmt.Errorf("branch %s active_task_id=%s does not match task %s", branchID, activeTask, taskID)
		}
		switch strings.ToUpper(strings.TrimSpace(branch.Status)) {
		case ProjectBranchStatusReserved, ProjectBranchStatusActive, ProjectBranchStatusBlocked:
		default:
			result.SkipReason = "branch_status_not_narrowable:" + strings.TrimSpace(branch.Status)
			return nil
		}
		if liveItem, ok, err := getLiveProjectPatchQueueItemByBranchTx(ctx, tx, workspaceID, projectID, branchID); err != nil {
			return err
		} else if ok {
			result.SkipReason = "live_patch_queue_item:" + strings.TrimSpace(liveItem.QueueID) + "/" + strings.TrimSpace(liveItem.ItemID)
			return nil
		}

		var claimAgentID, claimStatus, claimBranchID, claimScopeJSON, claimUpdatedAt string
		if err := tx.QueryRowContext(ctx, `
SELECT agent_id, claim_status, COALESCE(branch_id, ''), COALESCE(write_scope_json, ''), updated_at
  FROM task_claims
 WHERE workspace_id = ? AND task_id = ?`,
			workspaceID, taskID,
		).Scan(&claimAgentID, &claimStatus, &claimBranchID, &claimScopeJSON, &claimUpdatedAt); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("task claim %s not found", taskID)
			}
			return fmt.Errorf("query task claim for liberation narrow: %w", err)
		}
		if strings.TrimSpace(claimAgentID) != agentID {
			return fmt.Errorf("claim %s belongs to agent %s", taskID, strings.TrimSpace(claimAgentID))
		}
		if strings.TrimSpace(claimBranchID) != branchID {
			return fmt.Errorf("claim %s branch_id=%s does not match branch %s", taskID, strings.TrimSpace(claimBranchID), branchID)
		}
		if claimStatus != model.TaskClaimStatusClaimed && claimStatus != model.TaskClaimStatusBlocked {
			result.SkipReason = "claim_status_not_narrowable:" + strings.TrimSpace(claimStatus)
			return nil
		}
		claimPaths := writeScopePaths(claimScopeJSON)
		branchPaths := writeScopePaths(branch.WriteScopeJSON)
		if input.RequireOwnerUnable {
			activeSessionID, err := claimLiberationActiveOwnerSessionTx(ctx, tx, workspaceID, taskID, agentID)
			if err != nil {
				return err
			}
			if activeSessionID != "" {
				result.SkipReason = "active_owner_session:" + activeSessionID
				return nil
			}
			activeExecutionState, err := lookupActiveExecutionEvidenceForTaskTx(ctx, tx, workspaceID, taskID)
			if err != nil {
				return err
			}
			if strings.TrimSpace(activeExecutionState) != "" {
				result.SkipReason = "active_owner_execution:" + activeExecutionState
				return nil
			}
			lastSeenAt, err := lookupAgentLastSeenAtTx(ctx, tx, workspaceID, agentID)
			if errors.Is(err, ErrAgentNotFound) {
				lastSeenAt = ""
			} else if err != nil {
				return err
			}
			abandoned, err := localSessionOwnershipAbandoned(claimUpdatedAt, lastSeenAt, referenceAt)
			if err != nil {
				return err
			}
			if !abandoned {
				result.SkipReason = "owner_recent"
				return nil
			}
		}
		if blockedScope := strings.TrimSpace(input.BlockedWriteScopeJSON); blockedScope != "" {
			blockedPaths := writeScopePaths(blockedScope)
			if len(blockedPaths) == 0 {
				result.SkipReason = "blocked_scope_missing"
				return nil
			}
			if !writeScopesOverlap(branchPaths, blockedPaths) && !writeScopesOverlap(claimPaths, blockedPaths) {
				result.SkipReason = "no_current_blocking_overlap"
				return nil
			}
			if writeScopesOverlapExcludingSharedSidecars(branchPaths, blockedPaths) ||
				writeScopesOverlapExcludingSharedSidecars(claimPaths, blockedPaths) {
				result.SkipReason = "owned_overlap_not_safe_for_auto_narrow"
				return nil
			}
		}
		if !writeScopePathsCoveredBy(newPaths, claimPaths) {
			return fmt.Errorf("new_write_scope_json must be covered by current claim scope")
		}
		if !writeScopePathsCoveredBy(newPaths, branchPaths) {
			return fmt.Errorf("new_write_scope_json must be covered by current branch scope")
		}
		result.PreviousClaimScope = strings.TrimSpace(claimScopeJSON)
		result.PreviousBranchScope = strings.TrimSpace(branch.WriteScopeJSON)
		if sameWriteScopePathset(newPaths, claimPaths) && sameWriteScopePathset(newPaths, branchPaths) {
			result.SkipReason = "already_narrowed"
			return nil
		}

		if res, err := tx.ExecContext(ctx, `
UPDATE task_claims
   SET write_scope_json = ?, summary = ?, updated_at = ?
 WHERE workspace_id = ? AND task_id = ? AND agent_id = ? AND updated_at = ?`,
			newScopeJSON,
			firstNonEmpty(strings.TrimSpace(input.Reason), "Claim scope narrowed by stale-claim liberation watchdog"),
			referenceAtRaw,
			workspaceID,
			taskID,
			agentID,
			claimUpdatedAt,
		); err != nil {
			return fmt.Errorf("update narrowed task claim scope: %w", err)
		} else if affected, _ := res.RowsAffected(); affected != 1 {
			return ErrTaskClaimStaleTransition
		}
		if res, err := tx.ExecContext(ctx, `
UPDATE project_branch_registry
   SET write_scope_json = ?, updated_by = ?, updated_at = ?
 WHERE workspace_id = ? AND project_id = ? AND branch_id = ? AND agent_id = ? AND active_claim_id = ?`,
			newScopeJSON,
			actorID,
			referenceAtRaw,
			workspaceID,
			projectID,
			branchID,
			agentID,
			taskID,
		); err != nil {
			return fmt.Errorf("update narrowed project branch scope: %w", err)
		} else if affected, _ := res.RowsAffected(); affected != 1 {
			return ErrProjectBranchActiveReferenceInvalid
		}
		payload := map[string]any{
			"schema":                     "claim_liberation_narrowed.v1",
			"workspace_id":               workspaceID,
			"project_id":                 projectID,
			"branch_id":                  branchID,
			"task_id":                    taskID,
			"agent_id":                   agentID,
			"previous_claim_scope_json":  result.PreviousClaimScope,
			"previous_branch_scope_json": result.PreviousBranchScope,
			"new_write_scope_json":       newScopeJSON,
			"reason":                     strings.TrimSpace(input.Reason),
			"evidence_json":              strings.TrimSpace(input.EvidenceJSON),
			"remedy":                     "NARROW",
			"release_deferred":           true,
		}
		event, err := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "claim.liberation_narrowed",
			EntityType:  "task_claim",
			EntityID:    taskID,
			AgentID:     agentID,
			TaskID:      taskID,
			ActorType:   actorType,
			ActorID:     actorID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   referenceAtRaw,
		})
		if err != nil {
			return err
		}
		result.Event = event
		result.Narrowed = true
		return nil
	}); err != nil {
		return result, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit claim liberation narrow tx: %w", err)
	}
	return result, nil
}

type ClaimLiberationReconcileInput struct {
	Scope       string `json:"scope,omitempty"`
	ActorType   string `json:"actor_type,omitempty"`
	ActorID     string `json:"actor_id,omitempty"`
	ReferenceAt string `json:"reference_at,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}

type ClaimLiberationReconcileItem struct {
	WorkspaceID      string `json:"workspace_id"`
	ProjectID        string `json:"project_id,omitempty"`
	BranchID         string `json:"branch_id,omitempty"`
	TaskID           string `json:"task_id,omitempty"`
	AgentID          string `json:"agent_id,omitempty"`
	BlockedTaskID    string `json:"blocked_task_id,omitempty"`
	EvidenceUpdateID string `json:"evidence_update_id,omitempty"`
	ReleaseRequestID string `json:"release_request_id,omitempty"`
	Action           string `json:"action"`
	SkipReason       string `json:"skip_reason,omitempty"`
	Error            string `json:"error,omitempty"`
}

type ClaimLiberationReconcileResult struct {
	Scope       string                         `json:"scope"`
	ReferenceAt string                         `json:"reference_at"`
	Scanned     int                            `json:"scanned"`
	Narrowed    int                            `json:"narrowed"`
	Skipped     int                            `json:"skipped"`
	Problems    int                            `json:"problems"`
	Items       []ClaimLiberationReconcileItem `json:"items,omitempty"`
}

type claimLiberationNarrowCandidate struct {
	WorkspaceID                  string
	ProjectID                    string
	BranchID                     string
	TaskID                       string
	AgentID                      string
	BlockedTaskID                string
	BlockedWriteScopeJSON        string
	BlockedCurrentWriteScopeJSON string
	ClaimWriteScopeJSON          string
	BranchWriteScopeJSON         string
	EvidenceUpdateID             string
	EvidenceCreatedAt            string
	EvidenceAgentID              string
	EvidencePayloadJSON          string
	ReleaseRequestID             string
	ReleaseRequestKind           string
}

func (s *Store) ReconcileStaleProjectClaimLiberationNarrow(ctx context.Context, input ClaimLiberationReconcileInput) (ClaimLiberationReconcileResult, error) {
	scope := normalizeWorkspaceAuthorityScope(input.Scope)
	actorType, actorID, err := normalizeLocalAuthorityActor(input.ActorType, input.ActorID)
	if err != nil {
		return ClaimLiberationReconcileResult{}, err
	}
	if actorType == authorityActorTypeSystem && strings.TrimSpace(actorID) == "authority_spine" {
		actorID = "claim_liberation_watchdog"
	}
	referenceAt, err := parseSessionReclaimReferenceAt(input.ReferenceAt)
	if err != nil {
		return ClaimLiberationReconcileResult{}, err
	}
	limit := input.Limit
	if limit <= 0 {
		limit = 32
	}
	localNode, err := s.EnsureLocalAuthorityNode(ctx)
	if err != nil {
		return ClaimLiberationReconcileResult{}, fmt.Errorf("ensure local authority node for claim liberation: %w", err)
	}
	candidates, err := s.listStaleProjectClaimLiberationNarrowCandidates(ctx, scope, strings.TrimSpace(localNode.AuthorityNodeID), referenceAt, limit)
	if err != nil {
		return ClaimLiberationReconcileResult{}, err
	}
	result := ClaimLiberationReconcileResult{
		Scope:       scope,
		ReferenceAt: referenceAt.Format(time.RFC3339Nano),
		Items:       make([]ClaimLiberationReconcileItem, 0, len(candidates)),
	}
	for _, candidate := range candidates {
		result.Scanned++
		item := ClaimLiberationReconcileItem{
			WorkspaceID:      candidate.WorkspaceID,
			ProjectID:        candidate.ProjectID,
			BranchID:         candidate.BranchID,
			TaskID:           candidate.TaskID,
			AgentID:          candidate.AgentID,
			BlockedTaskID:    candidate.BlockedTaskID,
			EvidenceUpdateID: candidate.EvidenceUpdateID,
			ReleaseRequestID: candidate.ReleaseRequestID,
			Action:           "UNCHANGED",
		}
		if ok, skipReason := claimLiberationBlockedTaskScopeStillMatches(candidate.BlockedWriteScopeJSON, candidate.BlockedCurrentWriteScopeJSON); !ok {
			item.Action = "SKIPPED"
			item.SkipReason = skipReason
			result.Skipped++
			result.Items = append(result.Items, item)
			continue
		}
		newScopeJSON, ok, skipReason, err := claimLiberationSidecarNarrowScopeJSON(candidate.ClaimWriteScopeJSON, candidate.BranchWriteScopeJSON)
		if err != nil {
			item.Action = "PROBLEM"
			item.Error = err.Error()
			result.Problems++
			result.Items = append(result.Items, item)
			continue
		}
		if !ok {
			item.Action = "SKIPPED"
			item.SkipReason = skipReason
			result.Skipped++
			result.Items = append(result.Items, item)
			continue
		}
		evidenceJSON := mustJSON(map[string]any{
			"schema":                           "claim_liberation_watchdog_evidence.v1",
			"evidence_update_id":               candidate.EvidenceUpdateID,
			"evidence_created_at":              candidate.EvidenceCreatedAt,
			"evidence_agent_id":                candidate.EvidenceAgentID,
			"release_request_id":               candidate.ReleaseRequestID,
			"release_request_kind":             candidate.ReleaseRequestKind,
			"blocked_task_id":                  candidate.BlockedTaskID,
			"blocked_write_scope_json":         candidate.BlockedWriteScopeJSON,
			"blocked_current_write_scope_json": candidate.BlockedCurrentWriteScopeJSON,
			"source_payload_json":              candidate.EvidencePayloadJSON,
			"server_verified_predicates": []string{
				"agent_update.project_claim_scope_busy_evidence",
				"agent_request.project_claim_owner_resume_pending",
				"blocked_task_nonterminal_scope_still_matches",
				"owner_has_no_active_session",
				"owner_has_no_active_execution",
				"owner_abandoned_after_grace",
				"current_overlap_is_shared_sidecar_only",
			},
		})
		narrowed, err := s.NarrowProjectClaimAndBranchScopeWithEvent(ctx, ClaimLiberationNarrowInput{
			WorkspaceID:           candidate.WorkspaceID,
			ProjectID:             candidate.ProjectID,
			BranchID:              candidate.BranchID,
			TaskID:                candidate.TaskID,
			AgentID:               candidate.AgentID,
			NewWriteScopeJSON:     newScopeJSON,
			Reason:                "Stale claim liberation narrowed shared sidecar scope after unanswered release/resume request",
			EvidenceJSON:          evidenceJSON,
			ActorType:             actorType,
			ActorID:               actorID,
			ReferenceAt:           result.ReferenceAt,
			RequireOwnerUnable:    true,
			BlockedWriteScopeJSON: candidate.BlockedWriteScopeJSON,
		})
		if err != nil {
			item.Action = "PROBLEM"
			item.Error = err.Error()
			result.Problems++
			result.Items = append(result.Items, item)
			continue
		}
		if narrowed.Narrowed {
			item.Action = "NARROWED"
			result.Narrowed++
		} else {
			item.Action = "SKIPPED"
			item.SkipReason = firstNonEmpty(strings.TrimSpace(narrowed.SkipReason), "not_narrowed")
			result.Skipped++
		}
		result.Items = append(result.Items, item)
	}
	return result, nil
}

func (s *Store) listStaleProjectClaimLiberationNarrowCandidates(ctx context.Context, scope, holderAuthorityNodeID string, referenceAt time.Time, limit int) ([]claimLiberationNarrowCandidate, error) {
	if limit <= 0 {
		limit = 32
	}
	evidenceCutoff := referenceAt.Add(-localOwnershipReclaimGrace).UTC().Format(time.RFC3339Nano)
	rows, err := s.db.QueryContext(ctx, `
WITH evidence AS (
SELECT u.update_id,
       u.workspace_id,
       u.agent_id,
       u.created_at,
       u.payload_json,
       TRIM(COALESCE(json_extract(u.payload_json, '$.project_id'), '')) AS project_id,
       TRIM(COALESCE(json_extract(u.payload_json, '$.conflict_branch_id'), '')) AS branch_id,
       TRIM(COALESCE(json_extract(u.payload_json, '$.conflict_task_id'), '')) AS conflict_task_id,
       TRIM(COALESCE(json_extract(u.payload_json, '$.conflict_owner_agent_id'), '')) AS conflict_owner_agent_id,
       TRIM(COALESCE(json_extract(u.payload_json, '$.blocked_task_id'), '')) AS blocked_task_id,
       TRIM(COALESCE(json_extract(u.payload_json, '$.blocked_write_scope_json'), '')) AS blocked_write_scope_json,
       TRIM(COALESCE(json_extract(u.payload_json, '$.request_id'), json_extract(u.payload_json, '$.release_request_id'), '')) AS release_request_id,
       TRIM(COALESCE(json_extract(u.payload_json, '$.release_request_kind'), '')) AS release_request_kind
  FROM agent_updates u
 WHERE json_valid(u.payload_json)
   AND TRIM(COALESCE(json_extract(u.payload_json, '$.schema'), '')) = 'project_claim_scope_busy_evidence.v1'
   AND TRIM(COALESCE(json_extract(u.payload_json, '$.evidence_kind'), '')) = 'project_claim_scope_busy'
   AND TRIM(COALESCE(json_extract(u.payload_json, '$.liberation_candidate_kind'), '')) = 'stale_claim_liberation_narrow_candidate'
   AND TRIM(COALESCE(json_extract(u.payload_json, '$.release_request_id'), json_extract(u.payload_json, '$.request_id'), '')) <> ''
   AND TRIM(COALESCE(json_extract(u.payload_json, '$.release_request_kind'), '')) = 'project_claim_owner_resume'
   AND u.created_at <= ?
)
SELECT e.update_id,
       e.workspace_id,
       e.agent_id,
       e.created_at,
       e.payload_json,
       e.project_id,
       e.branch_id,
       COALESCE(NULLIF(e.conflict_task_id, ''), b.active_claim_id) AS conflict_task_id,
       b.agent_id AS conflict_owner_agent_id,
       e.blocked_task_id,
       e.blocked_write_scope_json,
       e.release_request_id,
       e.release_request_kind,
       COALESCE(tc.write_scope_json, ''),
       COALESCE(b.write_scope_json, ''),
       COALESCE(bt.write_scope_hints_json, '[]')
  FROM evidence e
  JOIN workspace_authority wa
    ON wa.workspace_id = e.workspace_id
   AND wa.scope = ?
   AND wa.status = ?
   AND wa.holder_authority_node_id = ?
  JOIN project_branch_registry b
    ON b.workspace_id = e.workspace_id
   AND b.project_id = e.project_id
   AND b.branch_id = e.branch_id
  JOIN task_claims tc
    ON tc.workspace_id = b.workspace_id
   AND tc.task_id = COALESCE(NULLIF(e.conflict_task_id, ''), b.active_claim_id)
   AND tc.agent_id = b.agent_id
   AND COALESCE(tc.branch_id, '') = b.branch_id
  JOIN agent_requests req
    ON req.workspace_id = e.workspace_id
   AND req.request_id = e.release_request_id
   AND req.from_agent_id = e.agent_id
   AND req.to_agent_id = b.agent_id
  JOIN workspace_tasks blocked_wt
    ON blocked_wt.workspace_id = e.workspace_id
   AND blocked_wt.task_id = e.blocked_task_id
  JOIN tasks bt
    ON bt.task_id = blocked_wt.task_id
   AND COALESCE(bt.project_id, '') = e.project_id
 WHERE e.project_id <> ''
   AND e.branch_id <> ''
   AND e.blocked_task_id <> ''
   AND e.blocked_write_scope_json <> ''
   AND (e.conflict_owner_agent_id = '' OR e.conflict_owner_agent_id = b.agent_id)
   AND json_valid(req.payload)
   AND TRIM(COALESCE(json_extract(req.payload, '$.schema'), '')) = 'project_claim_owner_resume_request.v1'
   AND TRIM(COALESCE(json_extract(req.payload, '$.request_kind'), '')) = 'project_claim_owner_resume'
   AND TRIM(COALESCE(json_extract(req.payload, '$.evidence_kind'), '')) = 'project_claim_owner_resume_request'
   AND TRIM(COALESCE(json_extract(req.payload, '$.project_id'), '')) = e.project_id
   AND TRIM(COALESCE(json_extract(req.payload, '$.conflict_branch_id'), '')) = e.branch_id
   AND TRIM(COALESCE(json_extract(req.payload, '$.active_task_id'), json_extract(req.payload, '$.task_id'), '')) = COALESCE(NULLIF(e.conflict_task_id, ''), b.active_claim_id)
   AND TRIM(COALESCE(json_extract(req.payload, '$.blocked_task_id'), '')) = e.blocked_task_id
   AND req.status IN ('PENDING', 'PROCESSING', 'TIMEOUT')
   AND TRIM(COALESCE(req.response, '')) = ''
   AND req.created_at <= ?
   AND b.status IN (?, ?, ?)
   AND tc.claim_status IN (?, ?)
   AND bt.status NOT IN (?, ?, ?)
   AND NOT EXISTS (
       SELECT 1
         FROM runtime_events ev
        WHERE ev.workspace_id = e.workspace_id
          AND ev.event_type = 'claim.liberation_narrowed'
          AND ev.entity_type = 'task_claim'
          AND ev.entity_id = tc.task_id
          AND ev.payload_json LIKE '%' || e.update_id || '%'
   )
 ORDER BY e.created_at ASC, e.update_id ASC
 LIMIT ?`,
		evidenceCutoff,
		normalizeWorkspaceAuthorityScope(scope),
		string(WorkspaceAuthorityStatusActive),
		strings.TrimSpace(holderAuthorityNodeID),
		evidenceCutoff,
		ProjectBranchStatusReserved,
		ProjectBranchStatusActive,
		ProjectBranchStatusBlocked,
		model.TaskClaimStatusClaimed,
		model.TaskClaimStatusBlocked,
		model.TaskStatusResolved,
		model.TaskStatusFailed,
		model.TaskStatusCancelled,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query stale project claim liberation candidates: %w", err)
	}
	defer rows.Close()

	candidates := make([]claimLiberationNarrowCandidate, 0, limit)
	for rows.Next() {
		var candidate claimLiberationNarrowCandidate
		if err := rows.Scan(
			&candidate.EvidenceUpdateID,
			&candidate.WorkspaceID,
			&candidate.EvidenceAgentID,
			&candidate.EvidenceCreatedAt,
			&candidate.EvidencePayloadJSON,
			&candidate.ProjectID,
			&candidate.BranchID,
			&candidate.TaskID,
			&candidate.AgentID,
			&candidate.BlockedTaskID,
			&candidate.BlockedWriteScopeJSON,
			&candidate.ReleaseRequestID,
			&candidate.ReleaseRequestKind,
			&candidate.ClaimWriteScopeJSON,
			&candidate.BranchWriteScopeJSON,
			&candidate.BlockedCurrentWriteScopeJSON,
		); err != nil {
			return nil, fmt.Errorf("scan stale project claim liberation candidate: %w", err)
		}
		candidate.TaskID = strings.TrimSpace(candidate.TaskID)
		candidate.AgentID = strings.TrimSpace(candidate.AgentID)
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stale project claim liberation candidates: %w", err)
	}
	return candidates, nil
}

func claimLiberationBlockedTaskScopeStillMatches(evidenceScopeJSON, currentScopeJSON string) (bool, string) {
	evidencePaths := writeScopePaths(evidenceScopeJSON)
	if len(evidencePaths) == 0 {
		return false, "blocked_scope_missing"
	}
	currentPaths := writeScopePaths(currentScopeJSON)
	if len(currentPaths) == 0 {
		return false, "blocked_task_current_scope_missing"
	}
	if !writeScopesOverlap(evidencePaths, currentPaths) {
		return false, "blocked_task_scope_changed"
	}
	return true, ""
}

func claimLiberationActiveOwnerSessionTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID string) (string, error) {
	var sessionID string
	if err := tx.QueryRowContext(ctx, `
SELECT session_id
  FROM agent_sessions
 WHERE workspace_id = ?
   AND task_id = ?
   AND agent_id = ?
   AND status IN (?, ?, ?, ?, ?)
 ORDER BY started_at DESC, session_id DESC
 LIMIT 1`,
		workspaceID,
		taskID,
		agentID,
		model.SessionStatusActive,
		model.SessionStatusBlocked,
		model.SessionStatusWaitingDecision,
		model.SessionStatusHandoffPending,
		"RUNNING",
	).Scan(&sessionID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("query active owner session for claim liberation: %w", err)
	}
	return strings.TrimSpace(sessionID), nil
}

func claimLiberationSidecarNarrowScopeJSON(claimScopeJSON, branchScopeJSON string) (string, bool, string, error) {
	claimPaths := writeScopePaths(claimScopeJSON)
	branchPaths := writeScopePaths(branchScopeJSON)
	if len(claimPaths) == 0 || len(branchPaths) == 0 {
		return "", false, "current_scope_missing", nil
	}
	kept := make([]string, 0, len(claimPaths))
	removed := 0
	for _, path := range claimPaths {
		normalized := normalizeWriteScopePath(path)
		if normalized == "" {
			continue
		}
		if writeScopeSharedSidecarPath(normalized) {
			removed++
			continue
		}
		if writeScopePathsCoveredBy([]string{normalized}, branchPaths) {
			kept = append(kept, normalized)
		}
	}
	if removed == 0 {
		return "", false, "no_shared_sidecar_to_narrow", nil
	}
	if len(kept) == 0 {
		return "", false, "would_empty_owner_scope", nil
	}
	raw, err := json.Marshal(map[string]any{"paths": uniqueTrimmedStrings(kept)})
	if err != nil {
		return "", false, "", err
	}
	normalized, err := normalizeProjectBranchWriteScope(string(raw))
	if err != nil {
		return "", false, "", err
	}
	return normalized, true, "", nil
}

func sameWriteScopePathset(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	leftSet := make(map[string]int, len(left))
	for _, path := range left {
		leftSet[normalizeWriteScopePath(path)]++
	}
	for _, path := range right {
		normalized := normalizeWriteScopePath(path)
		if leftSet[normalized] == 0 {
			return false
		}
		leftSet[normalized]--
	}
	return true
}
