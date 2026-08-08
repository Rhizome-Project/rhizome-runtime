package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	taskFrontierDecisionOffered         = "offered"
	taskFrontierDecisionSelected        = "selected"
	taskFrontierDecisionDeclined        = "declined"
	taskFrontierDecisionModelFailed     = "model_failed"
	taskFrontierDecisionHydrationFailed = "hydration_failed"
	taskFrontierDecisionClaimFailed     = "claim_failed"
	taskFrontierDecisionAdmissionFailed = "admission_failed"
	taskFrontierDecisionConsumed        = "consumed"

	taskFrontierDefaultTTL = 15 * time.Minute
)

type AgentTaskFrontierDecisionInput struct {
	WorkspaceID           string
	AgentID               string
	FrontierGenerationID  string
	DecisionState         string
	SelectedTaskID        string
	Summary               string
	PromptContextEnvelope map[string]any
}

func (s *Store) recordAgentWorkTaskFrontier(ctx context.Context, workspaceID, agentID string, frontier AgentWorkTaskFrontier) error {
	workspaceID = strings.TrimSpace(workspaceID)
	agentID = strings.TrimSpace(agentID)
	generationID := strings.TrimSpace(frontier.GenerationID)
	claimableTaskIDs := make([]string, 0, len(frontier.Candidates))
	diagnosticBlockedTaskIDs := make([]string, 0, len(frontier.Candidates))
	for _, candidate := range frontier.Candidates {
		taskID := strings.TrimSpace(candidate.Task.TaskID)
		if taskID == "" {
			continue
		}
		if candidate.Blocked {
			if taskFrontierCandidateHasDiagnosticBlock(candidate) {
				diagnosticBlockedTaskIDs = append(diagnosticBlockedTaskIDs, taskID)
			}
			continue
		}
		claimableTaskIDs = append(claimableTaskIDs, taskID)
	}
	claimableTaskIDs = normalizeStringSlice(claimableTaskIDs)
	diagnosticBlockedTaskIDs = normalizeStringSlice(diagnosticBlockedTaskIDs)
	evidenceCount := len(claimableTaskIDs) + len(diagnosticBlockedTaskIDs)
	if evidenceCount == 0 {
		return nil
	}
	if workspaceID == "" || agentID == "" || generationID == "" {
		return fmt.Errorf("%w: incomplete task frontier evidence for %d candidate(s)", ErrTaskClaimAdmissionInvalid, evidenceCount)
	}
	rawTaskIDs, err := json.Marshal(claimableTaskIDs)
	if err != nil {
		return fmt.Errorf("marshal task frontier ids: %w", err)
	}
	rawDiagnosticTaskIDs, err := json.Marshal(diagnosticBlockedTaskIDs)
	if err != nil {
		return fmt.Errorf("marshal diagnostic task frontier ids: %w", err)
	}
	generatedAt := strings.TrimSpace(frontier.GeneratedAt)
	expiresAt := taskFrontierExpiresAt(generatedAt)
	res, err := s.writeDB.ExecContext(ctx, `
		INSERT INTO agent_task_frontiers(
		  workspace_id, generation_id, agent_id, generated_at, selection_mode, candidate_task_ids_json, diagnostic_candidate_task_ids_json, candidate_count,
		  decision_state, selected_task_id, decision_summary, decided_at, consumed_task_id, consumed_event_id, consumed_at, expires_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '', '', '', '', '', '', ?)
		ON CONFLICT(workspace_id, generation_id) DO UPDATE SET
		  agent_id = excluded.agent_id,
		  generated_at = excluded.generated_at,
		  selection_mode = excluded.selection_mode,
		  candidate_task_ids_json = CASE
		    WHEN agent_task_frontiers.decision_state <> 'offered' THEN agent_task_frontiers.candidate_task_ids_json
		    ELSE excluded.candidate_task_ids_json
		  END,
		  diagnostic_candidate_task_ids_json = CASE
		    WHEN agent_task_frontiers.decision_state <> 'offered' THEN agent_task_frontiers.diagnostic_candidate_task_ids_json
		    ELSE excluded.diagnostic_candidate_task_ids_json
		  END,
		  candidate_count = CASE
		    WHEN agent_task_frontiers.decision_state <> 'offered' THEN agent_task_frontiers.candidate_count
		    ELSE excluded.candidate_count
		  END,
		  decision_state = CASE
		    WHEN agent_task_frontiers.decision_state <> 'offered' THEN agent_task_frontiers.decision_state
		    ELSE excluded.decision_state
		  END,
		  selected_task_id = CASE
		    WHEN agent_task_frontiers.decision_state <> 'offered' THEN agent_task_frontiers.selected_task_id
		    ELSE ''
		  END,
		  decision_summary = CASE
		    WHEN agent_task_frontiers.decision_state <> 'offered' THEN agent_task_frontiers.decision_summary
		    ELSE ''
		  END,
		  decided_at = CASE
		    WHEN agent_task_frontiers.decision_state <> 'offered' THEN agent_task_frontiers.decided_at
		    ELSE ''
		  END,
		  expires_at = CASE
		    WHEN agent_task_frontiers.decision_state <> 'offered' THEN agent_task_frontiers.expires_at
		    ELSE excluded.expires_at
		  END
		WHERE agent_task_frontiers.agent_id = excluded.agent_id`,
		workspaceID,
		generationID,
		agentID,
		generatedAt,
		firstNonEmpty(strings.TrimSpace(frontier.SelectionMode), "agent_self_select"),
		string(rawTaskIDs),
		string(rawDiagnosticTaskIDs),
		evidenceCount,
		taskFrontierDecisionOffered,
		expiresAt,
	)
	if err != nil {
		return fmt.Errorf("record task frontier: %w", err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		return fmt.Errorf("%w: frontier generation %s is already bound to another agent", ErrTaskClaimAdmissionInvalid, generationID)
	}
	return nil
}

func taskFrontierCandidateHasDiagnosticBlock(candidate AgentWorkTaskFrontierCandidate) bool {
	if !candidate.Blocked || strings.TrimSpace(candidate.Task.TaskID) == "" {
		return false
	}
	return strings.TrimSpace(candidate.BlockReason) != "" || strings.TrimSpace(candidate.BlockSummary) != ""
}

func taskFrontierExpiresAt(generatedAt string) string {
	reference := time.Now().UTC()
	if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(generatedAt)); err == nil {
		reference = parsed.UTC()
	}
	return reference.Add(taskFrontierDefaultTTL).Format(time.RFC3339Nano)
}

func (s *Store) ensureTaskFrontierClaimEvidenceTx(ctx context.Context, tx *sql.Tx, workspaceID, agentID, taskID, generationID string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	agentID = strings.TrimSpace(agentID)
	taskID = strings.TrimSpace(taskID)
	generationID = strings.TrimSpace(generationID)
	if workspaceID == "" || agentID == "" || taskID == "" || generationID == "" {
		return fmt.Errorf("%w: incomplete task frontier claim evidence", ErrTaskClaimAdmissionInvalid)
	}
	var rawTaskIDs, decisionState, selectedTaskID, expiresAt, consumedTaskID string
	if err := tx.QueryRowContext(ctx, `
		SELECT candidate_task_ids_json, decision_state, selected_task_id, expires_at, consumed_task_id
		FROM agent_task_frontiers
		WHERE workspace_id = ? AND generation_id = ? AND agent_id = ?
		LIMIT 1`,
		workspaceID,
		generationID,
		agentID,
	).Scan(&rawTaskIDs, &decisionState, &selectedTaskID, &expiresAt, &consumedTaskID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: frontier generation %s was not recorded for agent %s", ErrTaskClaimAdmissionInvalid, generationID, agentID)
		}
		return fmt.Errorf("load task frontier claim evidence: %w", err)
	}
	decisionState = strings.TrimSpace(decisionState)
	switch decisionState {
	case taskFrontierDecisionSelected:
	case taskFrontierDecisionConsumed:
		if strings.TrimSpace(consumedTaskID) == taskID {
			return ErrTaskClaimStaleTransition
		}
		return fmt.Errorf("%w: frontier generation %s was already consumed by task %s", ErrTaskClaimAdmissionInvalid, generationID, strings.TrimSpace(consumedTaskID))
	default:
		return fmt.Errorf("%w: frontier generation %s requires selected decision receipt before claim, got state %s", ErrTaskClaimAdmissionInvalid, generationID, firstNonEmpty(decisionState, "<empty>"))
	}
	if selectedTaskID = strings.TrimSpace(selectedTaskID); selectedTaskID == "" {
		return fmt.Errorf("%w: frontier generation %s selected decision has no selected_task_id", ErrTaskClaimAdmissionInvalid, generationID)
	}
	if selectedTaskID != taskID {
		return fmt.Errorf("%w: frontier generation %s selected task %s, not %s", ErrTaskClaimAdmissionInvalid, generationID, selectedTaskID, taskID)
	}
	if expiresAt = strings.TrimSpace(expiresAt); expiresAt != "" {
		expiry, err := time.Parse(time.RFC3339Nano, expiresAt)
		if err != nil {
			return fmt.Errorf("%w: frontier generation %s has invalid expiry", ErrTaskClaimAdmissionInvalid, generationID)
		}
		if !time.Now().UTC().Before(expiry.UTC()) {
			return fmt.Errorf("%w: frontier generation %s expired at %s", ErrTaskClaimAdmissionInvalid, generationID, expiresAt)
		}
	}
	var taskIDs []string
	if err := json.Unmarshal([]byte(strings.TrimSpace(rawTaskIDs)), &taskIDs); err != nil {
		return fmt.Errorf("%w: frontier generation %s has invalid candidate evidence", ErrTaskClaimAdmissionInvalid, generationID)
	}
	for _, candidateTaskID := range taskIDs {
		if strings.TrimSpace(candidateTaskID) == taskID {
			return nil
		}
	}
	return fmt.Errorf("%w: task %s was not in frontier generation %s for agent %s", ErrTaskClaimAdmissionInvalid, taskID, generationID, agentID)
}

func (s *Store) consumeTaskFrontierClaimEvidenceTx(ctx context.Context, tx *sql.Tx, workspaceID, agentID, taskID, generationID, eventID, summary, now string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	agentID = strings.TrimSpace(agentID)
	taskID = strings.TrimSpace(taskID)
	generationID = strings.TrimSpace(generationID)
	eventID = strings.TrimSpace(eventID)
	now = strings.TrimSpace(now)
	if workspaceID == "" || agentID == "" || taskID == "" || generationID == "" || eventID == "" || now == "" {
		return fmt.Errorf("%w: incomplete task frontier consumption evidence", ErrTaskClaimAdmissionInvalid)
	}
	// CA-12: expiry is validated at ensure-time, but a long admission tx can cross
	// the generation TTL between ensure and consume. Re-validate liveness here
	// against a fresh wall clock (not the pre-tx `now`, which was captured before
	// the admission body ran) so an expired generation cannot be consumed.
	var consumeExpiresAt string
	if err := tx.QueryRowContext(ctx, `
		SELECT expires_at
		FROM agent_task_frontiers
		WHERE workspace_id = ? AND generation_id = ? AND agent_id = ?
		LIMIT 1`,
		workspaceID, generationID, agentID,
	).Scan(&consumeExpiresAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: frontier generation %s was not recorded for agent %s", ErrTaskClaimAdmissionInvalid, generationID, agentID)
		}
		return fmt.Errorf("load task frontier consumption evidence: %w", err)
	}
	if consumeExpiresAt = strings.TrimSpace(consumeExpiresAt); consumeExpiresAt != "" {
		expiry, err := time.Parse(time.RFC3339Nano, consumeExpiresAt)
		if err != nil {
			return fmt.Errorf("%w: frontier generation %s has invalid expiry", ErrTaskClaimAdmissionInvalid, generationID)
		}
		if !time.Now().UTC().Before(expiry.UTC()) {
			return fmt.Errorf("%w: frontier generation %s expired at %s before consumption", ErrTaskClaimAdmissionInvalid, generationID, consumeExpiresAt)
		}
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE agent_task_frontiers
		SET decision_state = ?,
		    selected_task_id = ?,
		    decision_summary = ?,
		    decided_at = CASE WHEN trim(decided_at) = '' THEN ? ELSE decided_at END,
		    consumed_task_id = ?,
		    consumed_event_id = ?,
		    consumed_at = ?
		WHERE workspace_id = ? AND generation_id = ? AND agent_id = ? AND decision_state <> ?`,
		taskFrontierDecisionConsumed,
		taskID,
		strings.TrimSpace(summary),
		now,
		taskID,
		eventID,
		now,
		workspaceID,
		generationID,
		agentID,
		taskFrontierDecisionConsumed,
	)
	if err != nil {
		return fmt.Errorf("consume task frontier claim evidence: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("%w: frontier generation %s was already consumed", ErrTaskClaimAdmissionInvalid, generationID)
	}
	return nil
}

// invalidateSelectedTaskFrontierGenerationsTx marks any un-consumed 'selected'
// frontier generations for a task as claim_failed when the task's claim is
// released or reaper-reset (CA-13/CA-14). A later re-claim that replays a now-stale
// generation then fails the ensure gate (which requires a live 'selected' decision)
// instead of silently undoing the reset off a generation minted before it, and a
// generation left dangling by a lost claim CAS stops being claim-eligible once the
// winning claim is released. Consumed generations are untouched (decision_state
// filter), so a normal claim->work->release cycle is unaffected.
func (s *Store) invalidateSelectedTaskFrontierGenerationsTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, now string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	if workspaceID == "" || taskID == "" {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE agent_task_frontiers
		SET decision_state = ?
		WHERE workspace_id = ? AND selected_task_id = ? AND decision_state = ?`,
		taskFrontierDecisionClaimFailed,
		workspaceID,
		taskID,
		taskFrontierDecisionSelected,
	); err != nil {
		return fmt.Errorf("invalidate selected task frontier generations on release: %w", err)
	}
	return nil
}

func (s *Store) RecordAgentTaskFrontierDecision(ctx context.Context, input AgentTaskFrontierDecisionInput) (RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	agentID := strings.TrimSpace(input.AgentID)
	if agentID == "" {
		return RuntimeEventRecord{}, errors.New("agent_id is required")
	}
	generationID := strings.TrimSpace(input.FrontierGenerationID)
	if generationID == "" {
		return RuntimeEventRecord{}, errors.New("frontier_generation_id is required")
	}
	decisionState := normalizeTaskFrontierDecisionState(input.DecisionState)
	switch decisionState {
	case taskFrontierDecisionSelected, taskFrontierDecisionDeclined, taskFrontierDecisionModelFailed, taskFrontierDecisionHydrationFailed, taskFrontierDecisionClaimFailed, taskFrontierDecisionAdmissionFailed:
	default:
		return RuntimeEventRecord{}, fmt.Errorf("%w: unsupported task frontier decision state %q", ErrTaskClaimAdmissionInvalid, strings.TrimSpace(input.DecisionState))
	}
	selectedTaskID := strings.TrimSpace(input.SelectedTaskID)
	if decisionState == taskFrontierDecisionSelected || decisionState == taskFrontierDecisionHydrationFailed || decisionState == taskFrontierDecisionClaimFailed || decisionState == taskFrontierDecisionAdmissionFailed {
		if selectedTaskID == "" {
			return RuntimeEventRecord{}, fmt.Errorf("%w: selected_task_id is required for frontier decision state %s", ErrTaskClaimAdmissionInvalid, decisionState)
		}
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("begin task frontier decision tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var runtimeEvent RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureAgentInWorkspaceTx(ctx, tx, workspaceID, agentID); err != nil {
			return err
		}
		var rawTaskIDs, rawDiagnosticTaskIDs, currentState, currentSelectedTaskID, expiresAt string
		if err := tx.QueryRowContext(ctx, `
			SELECT candidate_task_ids_json, diagnostic_candidate_task_ids_json, decision_state, selected_task_id, expires_at
			FROM agent_task_frontiers
			WHERE workspace_id = ? AND generation_id = ? AND agent_id = ?
			LIMIT 1`,
			workspaceID,
			generationID,
			agentID,
		).Scan(&rawTaskIDs, &rawDiagnosticTaskIDs, &currentState, &currentSelectedTaskID, &expiresAt); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("%w: frontier generation %s was not recorded for agent %s", ErrTaskClaimAdmissionInvalid, generationID, agentID)
			}
			return fmt.Errorf("load task frontier decision evidence: %w", err)
		}
		currentState = strings.TrimSpace(currentState)
		currentSelectedTaskID = strings.TrimSpace(currentSelectedTaskID)
		idempotentReplay := taskFrontierDecisionReplayMatches(currentState, currentSelectedTaskID, decisionState, selectedTaskID)
		if currentState == taskFrontierDecisionConsumed {
			return fmt.Errorf("%w: frontier generation %s was already consumed", ErrTaskClaimAdmissionInvalid, generationID)
		}
		if !idempotentReplay {
			if err := validateTaskFrontierDecisionTransition(generationID, currentState, currentSelectedTaskID, decisionState, selectedTaskID); err != nil {
				return err
			}
			if err := validateTaskFrontierFreshForDecision(generationID, expiresAt); err != nil {
				return err
			}
		}
		if selectedTaskID != "" {
			ok, err := taskFrontierEvidenceContainsTask(rawTaskIDs, selectedTaskID)
			if err != nil {
				return fmt.Errorf("%w: frontier generation %s has invalid candidate evidence", ErrTaskClaimAdmissionInvalid, generationID)
			}
			if !ok && decisionState == taskFrontierDecisionAdmissionFailed {
				ok, err = taskFrontierEvidenceContainsTask(rawDiagnosticTaskIDs, selectedTaskID)
				if err != nil {
					return fmt.Errorf("%w: frontier generation %s has invalid diagnostic candidate evidence", ErrTaskClaimAdmissionInvalid, generationID)
				}
			}
			if !ok {
				return fmt.Errorf("%w: task %s was not in frontier generation %s for agent %s", ErrTaskClaimAdmissionInvalid, selectedTaskID, generationID, agentID)
			}
		}
		if idempotentReplay {
			if existing, ok, err := s.latestRuntimeEventTx(ctx, tx, RuntimeEventFilter{
				WorkspaceID: workspaceID,
				EventType:   "task_frontier.decision",
				EntityType:  "agent_task_frontier",
				EntityID:    generationID,
			}); err != nil {
				return err
			} else if ok {
				runtimeEvent = existing
				return nil
			}
		}
		res, err := tx.ExecContext(ctx, `
			UPDATE agent_task_frontiers
			SET decision_state = ?,
			    selected_task_id = ?,
			    decision_summary = ?,
			    decided_at = ?
			WHERE workspace_id = ? AND generation_id = ? AND agent_id = ? AND decision_state <> ?`,
			decisionState,
			selectedTaskID,
			strings.TrimSpace(input.Summary),
			now,
			workspaceID,
			generationID,
			agentID,
			taskFrontierDecisionConsumed,
		)
		if err != nil {
			return fmt.Errorf("record task frontier decision: %w", err)
		}
		affected, _ := res.RowsAffected()
		if affected == 0 {
			return fmt.Errorf("%w: frontier generation %s was already consumed", ErrTaskClaimAdmissionInvalid, generationID)
		}
		payload := map[string]any{
			"workspace_id":           workspaceID,
			"agent_id":               agentID,
			"frontier_generation_id": generationID,
			"decision_state":         decisionState,
			"selected_task_id":       selectedTaskID,
			"summary":                strings.TrimSpace(input.Summary),
		}
		payload, err = attachAgentTaskPromptContextEnvelope(payload, input.PromptContextEnvelope, "agent.task_frontier.decision", workspaceID, selectedTaskID, agentID)
		if err != nil {
			return err
		}
		var appendErr error
		runtimeEvent, appendErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "task_frontier.decision",
			EntityType:  "agent_task_frontier",
			EntityID:    generationID,
			ActorType:   "agent",
			ActorID:     agentID,
			AgentID:     agentID,
			TaskID:      selectedTaskID,
			PayloadJSON: mustJSON(payload),
			CreatedAt:   now,
		})
		return appendErr
	}); err != nil {
		_ = tx.Rollback()
		return RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("commit task frontier decision tx: %w", err)
	}
	return runtimeEvent, nil
}

func taskFrontierDecisionReplayMatches(currentState, currentSelectedTaskID, decisionState, selectedTaskID string) bool {
	currentState = strings.TrimSpace(currentState)
	decisionState = strings.TrimSpace(decisionState)
	if currentState == "" || currentState != decisionState {
		return false
	}
	switch decisionState {
	case taskFrontierDecisionSelected, taskFrontierDecisionHydrationFailed, taskFrontierDecisionClaimFailed, taskFrontierDecisionAdmissionFailed:
		return strings.TrimSpace(currentSelectedTaskID) == strings.TrimSpace(selectedTaskID)
	case taskFrontierDecisionDeclined, taskFrontierDecisionModelFailed:
		return strings.TrimSpace(selectedTaskID) == ""
	default:
		return false
	}
}

func validateTaskFrontierDecisionTransition(generationID, currentState, currentSelectedTaskID, decisionState, selectedTaskID string) error {
	currentState = strings.TrimSpace(currentState)
	if currentState == "" {
		currentState = taskFrontierDecisionOffered
	}
	switch currentState {
	case taskFrontierDecisionOffered:
		switch decisionState {
		case taskFrontierDecisionSelected, taskFrontierDecisionDeclined, taskFrontierDecisionModelFailed, taskFrontierDecisionClaimFailed, taskFrontierDecisionAdmissionFailed:
			return nil
		}
	case taskFrontierDecisionSelected:
		switch decisionState {
		case taskFrontierDecisionHydrationFailed, taskFrontierDecisionClaimFailed, taskFrontierDecisionAdmissionFailed:
			if strings.TrimSpace(currentSelectedTaskID) == strings.TrimSpace(selectedTaskID) {
				return nil
			}
		}
	case taskFrontierDecisionDeclined, taskFrontierDecisionModelFailed, taskFrontierDecisionHydrationFailed, taskFrontierDecisionClaimFailed, taskFrontierDecisionAdmissionFailed:
		return fmt.Errorf("%w: frontier generation %s decision state %s is terminal; only identical replay is accepted", ErrTaskClaimAdmissionInvalid, generationID, currentState)
	}
	return fmt.Errorf("%w: frontier generation %s cannot transition decision state %s -> %s", ErrTaskClaimAdmissionInvalid, generationID, currentState, decisionState)
}

func validateTaskFrontierFreshForDecision(generationID, expiresAt string) error {
	expiresAt = strings.TrimSpace(expiresAt)
	if expiresAt == "" {
		return nil
	}
	expiry, err := time.Parse(time.RFC3339Nano, expiresAt)
	if err != nil {
		return fmt.Errorf("%w: frontier generation %s has invalid expiry", ErrTaskClaimAdmissionInvalid, generationID)
	}
	if !time.Now().UTC().Before(expiry.UTC()) {
		return fmt.Errorf("%w: frontier generation %s expired at %s", ErrTaskClaimAdmissionInvalid, generationID, expiresAt)
	}
	return nil
}

func normalizeTaskFrontierDecisionState(state string) string {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case taskFrontierDecisionSelected, "select":
		return taskFrontierDecisionSelected
	case taskFrontierDecisionDeclined, "decline":
		return taskFrontierDecisionDeclined
	case taskFrontierDecisionModelFailed, "model-failed":
		return taskFrontierDecisionModelFailed
	case taskFrontierDecisionHydrationFailed, "hydration-failed":
		return taskFrontierDecisionHydrationFailed
	case taskFrontierDecisionClaimFailed, "claim-failed":
		return taskFrontierDecisionClaimFailed
	case taskFrontierDecisionAdmissionFailed, "admission-failed":
		return taskFrontierDecisionAdmissionFailed
	case taskFrontierDecisionConsumed, "consume":
		return taskFrontierDecisionConsumed
	case taskFrontierDecisionOffered, "":
		return strings.ToLower(strings.TrimSpace(state))
	default:
		return strings.ToLower(strings.TrimSpace(state))
	}
}

func taskFrontierEvidenceContainsTask(rawTaskIDs, taskID string) (bool, error) {
	taskID = strings.TrimSpace(taskID)
	var taskIDs []string
	if err := json.Unmarshal([]byte(strings.TrimSpace(rawTaskIDs)), &taskIDs); err != nil {
		return false, err
	}
	for _, candidateTaskID := range taskIDs {
		if strings.TrimSpace(candidateTaskID) == taskID {
			return true, nil
		}
	}
	return false, nil
}
