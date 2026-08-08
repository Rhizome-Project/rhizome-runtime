package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

var ErrExecutionRunBindingInvalid = errors.New("execution run binding invalid")

// ErrExecutionRunTerminalRegression is returned when an upsert would move an
// execution run from a terminal status back to a live (non-terminal) one. Run
// terminalization is monotonic: a late or out-of-order projection must not
// re-materialize a closed run as live.
var ErrExecutionRunTerminalRegression = errors.New("execution run terminal status cannot regress to live")

type ExecutionRunLiveBindingInput struct {
	WorkspaceID string
	AgentID     string
	TaskID      string
	SessionID   string
	ParentRunID string
}

type ExecutionRunBindingProof struct {
	WorkspaceID              string `json:"workspace_id,omitempty"`
	AgentID                  string `json:"agent_id,omitempty"`
	TaskID                   string `json:"task_id,omitempty"`
	SessionID                string `json:"session_id,omitempty"`
	ParentRunID              string `json:"parent_run_id,omitempty"`
	ClaimAgentID             string `json:"claim_agent_id,omitempty"`
	ClaimStatusAtStart       string `json:"claim_status_at_start,omitempty"`
	TaskStatusAtStart        string `json:"task_status_at_start,omitempty"`
	SessionStatusAtStart     string `json:"session_status_at_start,omitempty"`
	ParentRunStatusAtStart   string `json:"parent_run_status_at_start,omitempty"`
	ParentRunOutcomeAtStart  string `json:"parent_run_outcome_at_start,omitempty"`
	BindingReceiptContract   string `json:"binding_receipt_contract,omitempty"`
	ReceiptlessSessionPolicy string `json:"receiptless_session_policy,omitempty"`
}

type executionBindingQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Store) VerifyLiveExecutionRunBinding(ctx context.Context, input ExecutionRunLiveBindingInput) (ExecutionRunBindingProof, error) {
	return s.verifyLiveExecutionRunBindingWithQuerier(ctx, s.db, input)
}

func (s *Store) verifyLiveExecutionRunBindingWithQuerier(ctx context.Context, q executionBindingQueryer, input ExecutionRunLiveBindingInput) (ExecutionRunBindingProof, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	agentID := strings.TrimSpace(input.AgentID)
	taskID := strings.TrimSpace(input.TaskID)
	sessionID := strings.TrimSpace(input.SessionID)
	parentRunID := strings.TrimSpace(input.ParentRunID)

	if workspaceID == "" {
		return ExecutionRunBindingProof{}, fmt.Errorf("%w: workspace_id is required", ErrExecutionRunBindingInvalid)
	}
	if taskID == "" && sessionID == "" && parentRunID == "" {
		return ExecutionRunBindingProof{}, nil
	}
	if agentID == "" || sessionID == "" {
		return ExecutionRunBindingProof{}, fmt.Errorf("%w: live execution binding requires agent_id and session_id", ErrExecutionRunBindingInvalid)
	}
	if (taskID != "" || parentRunID != "") && taskID == "" {
		return ExecutionRunBindingProof{}, fmt.Errorf("%w: task or parent execution binding requires task_id", ErrExecutionRunBindingInvalid)
	}

	proof := ExecutionRunBindingProof{
		WorkspaceID:              workspaceID,
		AgentID:                  agentID,
		TaskID:                   taskID,
		SessionID:                sessionID,
		ParentRunID:              parentRunID,
		BindingReceiptContract:   "execution_run.live_binding.v1",
		ReceiptlessSessionPolicy: "reject",
	}

	var sessionAgentID, sessionTaskID, sessionStatus string
	if err := q.QueryRowContext(ctx, `
SELECT agent_id, COALESCE(task_id,''), status
  FROM agent_sessions
 WHERE workspace_id = ? AND session_id = ?`,
		workspaceID, sessionID,
	).Scan(&sessionAgentID, &sessionTaskID, &sessionStatus); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ExecutionRunBindingProof{}, fmt.Errorf("%w: session not found for live execution binding: %s", ErrExecutionRunBindingInvalid, sessionID)
		}
		return ExecutionRunBindingProof{}, fmt.Errorf("query session for live execution binding: %w", err)
	}
	sessionAgentID = strings.TrimSpace(sessionAgentID)
	sessionTaskID = strings.TrimSpace(sessionTaskID)
	sessionStatus = strings.TrimSpace(sessionStatus)
	if sessionAgentID != agentID {
		return ExecutionRunBindingProof{}, fmt.Errorf("%w: session agent mismatch for live execution binding: session_agent_id=%s agent_id=%s", ErrExecutionRunBindingInvalid, sessionAgentID, agentID)
	}
	if sessionTaskID != taskID {
		return ExecutionRunBindingProof{}, fmt.Errorf("%w: session task mismatch for live execution binding: session_task_id=%s task_id=%s", ErrExecutionRunBindingInvalid, firstNonEmpty(sessionTaskID, "<none>"), taskID)
	}
	if !model.IsSessionStatusActive(sessionStatus) {
		return ExecutionRunBindingProof{}, fmt.Errorf("%w: session is not active for live execution binding: session_id=%s status=%s", ErrExecutionRunBindingInvalid, sessionID, firstNonEmpty(sessionStatus, "<none>"))
	}
	if ok, err := executionSessionHasStartReceipt(ctx, q, workspaceID, sessionID, agentID, taskID); err != nil {
		return ExecutionRunBindingProof{}, err
	} else if !ok {
		return ExecutionRunBindingProof{}, fmt.Errorf("%w: session has no durable start receipt for live execution binding: session_id=%s", ErrExecutionRunBindingInvalid, sessionID)
	}
	proof.SessionStatusAtStart = sessionStatus
	if taskID == "" {
		return proof, nil
	}

	var taskStatus string
	var claimAgentID sql.NullString
	var claimStatus sql.NullString
	if err := q.QueryRowContext(ctx, `
SELECT t.status, tc.agent_id, tc.claim_status
  FROM tasks t
  JOIN workspace_tasks wt ON wt.task_id = t.task_id
  LEFT JOIN task_claims tc ON tc.task_id = t.task_id AND tc.workspace_id = wt.workspace_id
 WHERE wt.workspace_id = ? AND t.task_id = ?`,
		workspaceID, taskID,
	).Scan(&taskStatus, &claimAgentID, &claimStatus); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ExecutionRunBindingProof{}, fmt.Errorf("%w: task not found for live execution binding: %s", ErrExecutionRunBindingInvalid, taskID)
		}
		return ExecutionRunBindingProof{}, fmt.Errorf("query task claim for live execution binding: %w", err)
	}
	taskStatus = strings.TrimSpace(taskStatus)
	claimOwner := strings.TrimSpace(claimAgentID.String)
	claimState := strings.ToUpper(strings.TrimSpace(claimStatus.String))
	if claimOwner != agentID || claimState != model.TaskClaimStatusClaimed {
		return ExecutionRunBindingProof{}, fmt.Errorf("%w: task claim is not live same-owner for execution binding: task_id=%s claim_agent_id=%s claim_status=%s agent_id=%s", ErrExecutionRunBindingInvalid, taskID, firstNonEmpty(claimOwner, "<none>"), firstNonEmpty(claimState, "<none>"), agentID)
	}
	if isTerminalTaskStatus(taskStatus) {
		return ExecutionRunBindingProof{}, fmt.Errorf("%w: task is terminal for live execution binding: task_id=%s task_status=%s", ErrExecutionRunBindingInvalid, taskID, taskStatus)
	}
	proof.ClaimAgentID = claimOwner
	proof.ClaimStatusAtStart = claimState
	proof.TaskStatusAtStart = taskStatus

	if parentRunID != "" {
		parent, err := getExecutionRunWithQuerier(ctx, q, workspaceID, parentRunID)
		if err != nil {
			return ExecutionRunBindingProof{}, fmt.Errorf("%w: parent run not found for live execution binding: %s", ErrExecutionRunBindingInvalid, parentRunID)
		}
		if parent.AgentID != agentID || parent.TaskID != taskID || parent.SessionID != sessionID {
			return ExecutionRunBindingProof{}, fmt.Errorf("%w: parent run binding mismatch: parent_run_id=%s parent_agent_id=%s parent_task_id=%s parent_session_id=%s agent_id=%s task_id=%s session_id=%s", ErrExecutionRunBindingInvalid, parentRunID, parent.AgentID, parent.TaskID, parent.SessionID, agentID, taskID, sessionID)
		}
		if isExecutionRunTerminal(parent.Status) {
			return ExecutionRunBindingProof{}, fmt.Errorf("%w: parent run is terminal for live execution binding: parent_run_id=%s status=%s", ErrExecutionRunBindingInvalid, parentRunID, parent.Status)
		}
		proof.ParentRunStatusAtStart = parent.Status
		proof.ParentRunOutcomeAtStart = parent.Outcome
	}

	return proof, nil
}

func (s *Store) verifyBlockedExecutionRunBindingWithQuerier(ctx context.Context, q executionBindingQueryer, input ExecutionRunLiveBindingInput) (ExecutionRunBindingProof, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	agentID := strings.TrimSpace(input.AgentID)
	taskID := strings.TrimSpace(input.TaskID)
	sessionID := strings.TrimSpace(input.SessionID)
	parentRunID := strings.TrimSpace(input.ParentRunID)

	if workspaceID == "" {
		return ExecutionRunBindingProof{}, fmt.Errorf("%w: workspace_id is required", ErrExecutionRunBindingInvalid)
	}
	if taskID == "" || agentID == "" || sessionID == "" {
		return ExecutionRunBindingProof{}, fmt.Errorf("%w: blocked execution binding requires agent_id, task_id, and session_id", ErrExecutionRunBindingInvalid)
	}

	proof := ExecutionRunBindingProof{
		WorkspaceID:              workspaceID,
		AgentID:                  agentID,
		TaskID:                   taskID,
		SessionID:                sessionID,
		ParentRunID:              parentRunID,
		BindingReceiptContract:   "execution_run.blocked_binding.v1",
		ReceiptlessSessionPolicy: "reject",
	}

	var sessionAgentID, sessionTaskID, sessionStatus string
	if err := q.QueryRowContext(ctx, `
SELECT agent_id, COALESCE(task_id,''), status
  FROM agent_sessions
 WHERE workspace_id = ? AND session_id = ?`,
		workspaceID, sessionID,
	).Scan(&sessionAgentID, &sessionTaskID, &sessionStatus); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ExecutionRunBindingProof{}, fmt.Errorf("%w: session not found for blocked execution binding: %s", ErrExecutionRunBindingInvalid, sessionID)
		}
		return ExecutionRunBindingProof{}, fmt.Errorf("query session for blocked execution binding: %w", err)
	}
	sessionAgentID = strings.TrimSpace(sessionAgentID)
	sessionTaskID = strings.TrimSpace(sessionTaskID)
	sessionStatus = strings.TrimSpace(sessionStatus)
	if sessionAgentID != agentID {
		return ExecutionRunBindingProof{}, fmt.Errorf("%w: session agent mismatch for blocked execution binding: session_agent_id=%s agent_id=%s", ErrExecutionRunBindingInvalid, sessionAgentID, agentID)
	}
	if sessionTaskID != taskID {
		return ExecutionRunBindingProof{}, fmt.Errorf("%w: session task mismatch for blocked execution binding: session_task_id=%s task_id=%s", ErrExecutionRunBindingInvalid, firstNonEmpty(sessionTaskID, "<none>"), taskID)
	}
	if !model.IsSessionStatusActive(sessionStatus) {
		return ExecutionRunBindingProof{}, fmt.Errorf("%w: session is not active or blocked for blocked execution binding: session_id=%s status=%s", ErrExecutionRunBindingInvalid, sessionID, firstNonEmpty(sessionStatus, "<none>"))
	}
	if ok, err := executionSessionHasStartReceipt(ctx, q, workspaceID, sessionID, agentID, taskID); err != nil {
		return ExecutionRunBindingProof{}, err
	} else if !ok {
		return ExecutionRunBindingProof{}, fmt.Errorf("%w: session has no durable start receipt for blocked execution binding: session_id=%s", ErrExecutionRunBindingInvalid, sessionID)
	}
	proof.SessionStatusAtStart = sessionStatus

	var taskStatus string
	var claimAgentID sql.NullString
	var claimStatus sql.NullString
	if err := q.QueryRowContext(ctx, `
SELECT t.status, tc.agent_id, tc.claim_status
  FROM tasks t
  JOIN workspace_tasks wt ON wt.task_id = t.task_id
  LEFT JOIN task_claims tc ON tc.task_id = t.task_id AND tc.workspace_id = wt.workspace_id
 WHERE wt.workspace_id = ? AND t.task_id = ?`,
		workspaceID, taskID,
	).Scan(&taskStatus, &claimAgentID, &claimStatus); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ExecutionRunBindingProof{}, fmt.Errorf("%w: task not found for blocked execution binding: %s", ErrExecutionRunBindingInvalid, taskID)
		}
		return ExecutionRunBindingProof{}, fmt.Errorf("query task claim for blocked execution binding: %w", err)
	}
	taskStatus = strings.TrimSpace(taskStatus)
	claimOwner := strings.TrimSpace(claimAgentID.String)
	claimState := strings.ToUpper(strings.TrimSpace(claimStatus.String))
	if claimOwner != agentID || (claimState != model.TaskClaimStatusClaimed && claimState != model.TaskClaimStatusBlocked) {
		return ExecutionRunBindingProof{}, fmt.Errorf("%w: task claim is not same-owner claimed/blocked for blocked execution binding: task_id=%s claim_agent_id=%s claim_status=%s agent_id=%s", ErrExecutionRunBindingInvalid, taskID, firstNonEmpty(claimOwner, "<none>"), firstNonEmpty(claimState, "<none>"), agentID)
	}
	if isTerminalTaskStatus(taskStatus) {
		return ExecutionRunBindingProof{}, fmt.Errorf("%w: task is terminal for blocked execution binding: task_id=%s task_status=%s", ErrExecutionRunBindingInvalid, taskID, taskStatus)
	}
	proof.ClaimAgentID = claimOwner
	proof.ClaimStatusAtStart = claimState
	proof.TaskStatusAtStart = taskStatus

	if parentRunID != "" {
		parent, err := getExecutionRunWithQuerier(ctx, q, workspaceID, parentRunID)
		if err != nil {
			return ExecutionRunBindingProof{}, fmt.Errorf("%w: parent run not found for blocked execution binding: %s", ErrExecutionRunBindingInvalid, parentRunID)
		}
		if parent.AgentID != agentID || parent.TaskID != taskID || parent.SessionID != sessionID {
			return ExecutionRunBindingProof{}, fmt.Errorf("%w: parent run binding mismatch for blocked execution binding: parent_run_id=%s parent_agent_id=%s parent_task_id=%s parent_session_id=%s agent_id=%s task_id=%s session_id=%s", ErrExecutionRunBindingInvalid, parentRunID, parent.AgentID, parent.TaskID, parent.SessionID, agentID, taskID, sessionID)
		}
		proof.ParentRunStatusAtStart = parent.Status
		proof.ParentRunOutcomeAtStart = parent.Outcome
	}

	return proof, nil
}

func executionSessionHasStartReceipt(ctx context.Context, q executionBindingQueryer, workspaceID, sessionID, agentID, taskID string) (bool, error) {
	var count int
	if err := q.QueryRowContext(ctx, `
SELECT COUNT(1)
  FROM runtime_events
 WHERE workspace_id = ?
   AND entity_type = 'agent_session'
   AND entity_id = ?
   AND COALESCE(session_id,'') = ?
   AND COALESCE(agent_id,'') = ?
   AND COALESCE(task_id,'') = ?
   AND event_type IN ('agent.session.started', 'session.start')`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(sessionID), strings.TrimSpace(sessionID), strings.TrimSpace(agentID), strings.TrimSpace(taskID),
	).Scan(&count); err != nil {
		return false, fmt.Errorf("query session start receipt for execution binding: %w", err)
	}
	return count > 0, nil
}

func getExecutionRunWithQuerier(ctx context.Context, q executionBindingQueryer, workspaceID, runID string) (ExecutionRunRecord, error) {
	row := q.QueryRowContext(ctx,
		`SELECT run_id, workspace_id, COALESCE(task_id,''), COALESCE(session_id,''), COALESCE(agent_id,''),
		        title, COALESCE(summary,''), status, COALESCE(outcome,''), COALESCE(verification_json,'{}'),
		        created_at, updated_at, COALESCE(closed_at,'')
		   FROM execution_runs
		  WHERE workspace_id = ? AND run_id = ?`,
		strings.TrimSpace(workspaceID), strings.TrimSpace(runID),
	)
	return scanExecutionRunRecord(row)
}

func preserveExistingExecutionRunBinding(record ExecutionRunRecord, existingTaskID, existingSessionID, existingAgentID string) (ExecutionRunRecord, error) {
	existingTaskID = strings.TrimSpace(existingTaskID)
	existingSessionID = strings.TrimSpace(existingSessionID)
	existingAgentID = strings.TrimSpace(existingAgentID)
	if err := preserveExecutionRunBindingField("task_id", &record.TaskID, existingTaskID); err != nil {
		return ExecutionRunRecord{}, err
	}
	if err := preserveExecutionRunBindingField("session_id", &record.SessionID, existingSessionID); err != nil {
		return ExecutionRunRecord{}, err
	}
	if err := preserveExecutionRunBindingField("agent_id", &record.AgentID, existingAgentID); err != nil {
		return ExecutionRunRecord{}, err
	}
	return record, nil
}

func preserveExecutionRunBindingField(field string, target *string, existing string) error {
	current := strings.TrimSpace(*target)
	existing = strings.TrimSpace(existing)
	if existing == "" {
		*target = current
		return nil
	}
	if current == "" {
		*target = existing
		return nil
	}
	if current != existing {
		return fmt.Errorf("%w: %s is immutable for existing execution run: got=%s want=%s", ErrExecutionRunBindingInvalid, strings.TrimSpace(field), current, existing)
	}
	*target = current
	return nil
}

func (s *Store) validateExecutionRunBindingTx(ctx context.Context, tx *sql.Tx, record ExecutionRunRecord, existingRunFound bool, existingStatus string) (ExecutionRunRecord, error) {
	if record.SessionID != "" {
		var sessionAgentID, sessionTaskID string
		if err := tx.QueryRowContext(ctx, `
SELECT agent_id, COALESCE(task_id,'')
  FROM agent_sessions
 WHERE workspace_id = ? AND session_id = ?`,
			record.WorkspaceID, record.SessionID,
		).Scan(&sessionAgentID, &sessionTaskID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ExecutionRunRecord{}, fmt.Errorf("%w: session not found for execution run binding: %s", ErrExecutionRunBindingInvalid, record.SessionID)
			}
			return ExecutionRunRecord{}, fmt.Errorf("query execution run session binding: %w", err)
		}
		sessionAgentID = strings.TrimSpace(sessionAgentID)
		sessionTaskID = strings.TrimSpace(sessionTaskID)
		if record.AgentID == "" {
			record.AgentID = sessionAgentID
		} else if record.AgentID != sessionAgentID {
			return ExecutionRunRecord{}, fmt.Errorf("%w: session agent mismatch for execution run binding: session_agent_id=%s agent_id=%s", ErrExecutionRunBindingInvalid, sessionAgentID, record.AgentID)
		}
		if record.TaskID == "" {
			record.TaskID = sessionTaskID
		} else if record.TaskID != sessionTaskID {
			return ExecutionRunRecord{}, fmt.Errorf("%w: session task mismatch for execution run binding: session_task_id=%s task_id=%s", ErrExecutionRunBindingInvalid, firstNonEmpty(sessionTaskID, "<none>"), record.TaskID)
		}
	}
	if record.TaskID != "" {
		if err := s.ensureWorkspaceTaskAttachedTx(ctx, tx, record.WorkspaceID, record.TaskID); err != nil {
			if errors.Is(err, ErrWorkspaceTaskAbsent) {
				return ExecutionRunRecord{}, fmt.Errorf("%w: task not attached for execution run binding: %s", ErrExecutionRunBindingInvalid, record.TaskID)
			}
			return ExecutionRunRecord{}, err
		}
	}

	surface := executionRunPromptContextSurface(record.VerificationJSON)
	parentRunID := executionRunParentRunID(record.VerificationJSON)
	if !executionRunRequiresLiveBinding(record, surface, parentRunID) {
		return record, nil
	}
	if normalizeExecutionRunStatus(record.Status) == "BLOCKED" && strings.TrimSpace(record.TaskID) != "" {
		if existingRunFound && executionRunHasReusableBindingProof(record.VerificationJSON) {
			return record, nil
		}
		proof, err := s.verifyBlockedExecutionRunBindingWithQuerier(ctx, tx, ExecutionRunLiveBindingInput{
			WorkspaceID: record.WorkspaceID,
			AgentID:     record.AgentID,
			TaskID:      record.TaskID,
			SessionID:   record.SessionID,
			ParentRunID: parentRunID,
		})
		if err != nil {
			return ExecutionRunRecord{}, err
		}
		record.VerificationJSON = attachExecutionRunBindingProof(record.VerificationJSON, proof)
		return record, nil
	}
	if isExecutionRunTerminal(record.Status) && existingRunFound {
		if executionRunHasReusableBindingProof(record.VerificationJSON) {
			record.VerificationJSON = attachExecutionRunTerminalizationProof(record.VerificationJSON, record, existingStatus)
			return record, nil
		}
	}
	proof, err := s.verifyLiveExecutionRunBindingWithQuerier(ctx, tx, ExecutionRunLiveBindingInput{
		WorkspaceID: record.WorkspaceID,
		AgentID:     record.AgentID,
		TaskID:      record.TaskID,
		SessionID:   record.SessionID,
		ParentRunID: parentRunID,
	})
	if err != nil {
		return ExecutionRunRecord{}, err
	}
	record.VerificationJSON = attachExecutionRunBindingProof(record.VerificationJSON, proof)
	return record, nil
}

func executionRunRequiresLiveBinding(record ExecutionRunRecord, surface, parentRunID string) bool {
	if strings.TrimSpace(record.TaskID) == "" && strings.TrimSpace(record.SessionID) == "" && strings.TrimSpace(parentRunID) == "" {
		return false
	}
	surface = strings.TrimSpace(surface)
	if surface == "tool.call" {
		return true
	}
	if surface == "project.patch_queue.operation_bind" {
		return false
	}
	if strings.TrimSpace(record.SessionID) != "" || strings.TrimSpace(parentRunID) != "" {
		return true
	}
	if executionRunPromptContextPrincipalType(record.VerificationJSON) == "agent" {
		return true
	}
	return false
}

func executionRunPromptContextSurface(verification map[string]any) string {
	rawEnvelope, ok := verification[executionPromptContextEnvelopeKey]
	if !ok {
		return ""
	}
	envelope, ok := rawEnvelope.(map[string]any)
	if !ok {
		return ""
	}
	return strings.TrimSpace(executionRunAnyString(envelope["surface"]))
}

func executionRunPromptContextPrincipalType(verification map[string]any) string {
	rawEnvelope, ok := verification[executionPromptContextEnvelopeKey]
	if !ok {
		return ""
	}
	envelope, ok := rawEnvelope.(map[string]any)
	if !ok {
		return ""
	}
	return strings.TrimSpace(executionRunAnyString(envelope["principal_type"]))
}

func executionRunParentRunID(verification map[string]any) string {
	parentRunID := executionRunVerificationNestedString(verification, "operation_ledger", "binding", "parent_run_id")
	if parentRunID == "" {
		parentRunID = executionRunVerificationNestedString(verification, "operation_ledger", "request_details", "parent_run_id")
	}
	return parentRunID
}

func executionRunHasLiveBindingProof(verification map[string]any) bool {
	proof := executionRunReusableBindingProofValue(verification)
	if proof == nil {
		return false
	}
	return strings.TrimSpace(executionRunAnyString(proof["binding_receipt_contract"])) == "execution_run.live_binding.v1"
}

func executionRunHasReusableBindingProof(verification map[string]any) bool {
	return executionRunReusableBindingProofValue(verification) != nil
}

func executionRunReusableBindingProofValue(verification map[string]any) map[string]any {
	raw, ok := verification["live_execution_binding"]
	if !ok || raw == nil {
		return nil
	}
	proof, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	switch strings.TrimSpace(executionRunAnyString(proof["binding_receipt_contract"])) {
	case "execution_run.live_binding.v1", "execution_run.blocked_binding.v1":
		return proof
	default:
		return nil
	}
}

func stripExecutionRunLiveBindingProof(verification map[string]any) map[string]any {
	if verification == nil {
		return nil
	}
	if _, ok := verification["live_execution_binding"]; !ok {
		return verification
	}
	out := make(map[string]any, len(verification))
	for key, value := range verification {
		if key == "live_execution_binding" {
			continue
		}
		out[key] = value
	}
	return out
}

func attachExecutionRunTerminalizationProof(verification map[string]any, record ExecutionRunRecord, existingStatus string) map[string]any {
	if verification == nil {
		verification = map[string]any{}
	}
	out := make(map[string]any, len(verification)+1)
	for key, value := range verification {
		out[key] = value
	}
	out["terminal_execution_binding"] = map[string]any{
		"binding_receipt_contract": "execution_run.terminalizes_existing_binding.v1",
		"workspace_id":             strings.TrimSpace(record.WorkspaceID),
		"agent_id":                 strings.TrimSpace(record.AgentID),
		"task_id":                  strings.TrimSpace(record.TaskID),
		"session_id":               strings.TrimSpace(record.SessionID),
		"previous_run_status":      strings.TrimSpace(existingStatus),
		"terminal_run_status":      strings.TrimSpace(record.Status),
	}
	return out
}

func executionRunVerificationNestedString(root map[string]any, path ...string) string {
	var current any = root
	for _, key := range path {
		m, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current = m[strings.TrimSpace(key)]
	}
	return strings.TrimSpace(executionRunAnyString(current))
}

func executionRunAnyString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return ""
	}
}

func attachExecutionRunBindingProof(verification map[string]any, proof ExecutionRunBindingProof) map[string]any {
	if verification == nil {
		verification = map[string]any{}
	}
	if strings.TrimSpace(proof.BindingReceiptContract) == "" {
		return verification
	}
	out := make(map[string]any, len(verification)+1)
	for key, value := range verification {
		out[key] = value
	}
	out["live_execution_binding"] = proof
	return out
}
