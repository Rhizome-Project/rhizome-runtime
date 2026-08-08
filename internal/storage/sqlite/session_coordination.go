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

type AgentSessionCoordinationInput struct {
	EventType             string
	WorkspaceID           string
	SessionID             string
	AgentID               string
	TaskID                string
	Summary               string
	Status                string
	OwnerScope            string
	BlockedOn             []model.AgentUpdateBlockedRef
	DecisionNeededFrom    string
	DecisionType          string
	KeepSessionActive     *bool
	HandoffTo             string
	RelatedDocKeys        []string
	RelatedArtifactRefs   []model.AgentUpdateArtifactRef
	Iterations            int
	TotalInputTokens      int
	TotalOutputTokens     int
	ToolCalls             int
	UpdatedAt             string
	PromptContextEnvelope map[string]any
}

type AgentSessionStateRecord struct {
	SessionID           string                         `json:"session_id"`
	WorkspaceID         string                         `json:"workspace_id"`
	AgentID             string                         `json:"agent_id"`
	TaskID              string                         `json:"task_id,omitempty"`
	Status              string                         `json:"status"`
	Summary             string                         `json:"summary"`
	OwnerScope          string                         `json:"owner_scope,omitempty"`
	BlockedOn           []model.AgentUpdateBlockedRef  `json:"blocked_on,omitempty"`
	DecisionNeededFrom  string                         `json:"decision_needed_from,omitempty"`
	DecisionType        string                         `json:"decision_type,omitempty"`
	KeepSessionActive   *bool                          `json:"keep_session_active,omitempty"`
	HandoffTo           string                         `json:"handoff_to,omitempty"`
	RelatedDocKeys      []string                       `json:"related_doc_keys,omitempty"`
	RelatedArtifactRefs []model.AgentUpdateArtifactRef `json:"related_artifact_refs,omitempty"`
	Iterations          int                            `json:"iterations,omitempty"`
	TotalInputTokens    int                            `json:"total_input_tokens,omitempty"`
	TotalOutputTokens   int                            `json:"total_output_tokens,omitempty"`
	ToolCalls           int                            `json:"tool_calls,omitempty"`
	UpdateType          string                         `json:"update_type,omitempty"`
	UpdatedAt           string                         `json:"updated_at"`
	StartedAt           string                         `json:"started_at"`
	CompletedAt         string                         `json:"completed_at,omitempty"`
}

type AgentSessionTakeoverInput struct {
	WorkspaceID           string
	SessionID             string
	SuccessorSessionID    string
	TakeoverAgentID       string
	Summary               string
	SuccessorSummary      string
	UpdatedAt             string
	PromptContextEnvelope map[string]any
}

type AgentSessionTakeoverRecord struct {
	SourceState    AgentSessionStateRecord `json:"source_state"`
	SuccessorState AgentSessionStateRecord `json:"successor_state"`
}

func (s *Store) RecordAgentSessionCoordination(ctx context.Context, input AgentSessionCoordinationInput) (AgentSessionStateRecord, error) {
	referenceAt := strings.TrimSpace(input.UpdatedAt)
	if referenceAt == "" {
		referenceAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, strings.TrimSpace(input.WorkspaceID), authorityScopeWorkspace, referenceAt)
	if err != nil {
		return AgentSessionStateRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return AgentSessionStateRecord{}, fmt.Errorf("begin agent session coordination tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var state AgentSessionStateRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		state, _, innerErr = s.recordAgentSessionCoordinationWithAuthorityTx(ctx, tx, authority, input)
		return innerErr
	}); err != nil {
		_ = tx.Rollback()
		return AgentSessionStateRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	if err := tx.Commit(); err != nil {
		return AgentSessionStateRecord{}, fmt.Errorf("commit session coordination tx: %w", err)
	}
	s.bestEffortReconcileMemoryProjectionWorkspace(ctx, state.WorkspaceID)
	return state, nil
}

func (s *Store) RecordAgentSessionCoordinationWithEvent(ctx context.Context, input AgentSessionCoordinationInput) (AgentSessionStateRecord, RuntimeEventRecord, error) {
	referenceAt := strings.TrimSpace(input.UpdatedAt)
	if referenceAt == "" {
		referenceAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, strings.TrimSpace(input.WorkspaceID), authorityScopeWorkspace, referenceAt)
	if err != nil {
		return AgentSessionStateRecord{}, RuntimeEventRecord{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return AgentSessionStateRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin agent session coordination tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var state AgentSessionStateRecord
	var event RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		state, event, innerErr = s.recordAgentSessionCoordinationWithAuthorityTx(ctx, tx, authority, input)
		return innerErr
	}); err != nil {
		_ = tx.Rollback()
		return AgentSessionStateRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return AgentSessionStateRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit session coordination tx: %w", err)
	}
	s.bestEffortReconcileMemoryProjectionWorkspace(ctx, state.WorkspaceID)
	return state, event, nil
}

func (s *Store) recordAgentSessionCoordinationTx(ctx context.Context, tx *sql.Tx, input AgentSessionCoordinationInput) (AgentSessionStateRecord, RuntimeEventRecord, error) {
	return s.recordAgentSessionCoordinationWithAuthorityTx(ctx, tx, WorkspaceAuthorityRecord{}, input)
}

func (s *Store) recordAgentSessionCoordinationWithAuthorityTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, input AgentSessionCoordinationInput) (AgentSessionStateRecord, RuntimeEventRecord, error) {
	return s.recordAgentSessionCoordinationWithAuthorityActorTx(ctx, tx, authority, input, "agent", strings.TrimSpace(input.AgentID))
}

func nonNegativeSessionMetric(value int) int {
	if value < 0 {
		return 0
	}
	return value
}

func maxSessionMetric(current, incoming int) int {
	current = nonNegativeSessionMetric(current)
	incoming = nonNegativeSessionMetric(incoming)
	if incoming > current {
		return incoming
	}
	return current
}

func (s *Store) recordAgentSessionCoordinationWithAuthorityActorTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, input AgentSessionCoordinationInput, runtimeActorType, runtimeActorID string) (AgentSessionStateRecord, RuntimeEventRecord, error) {
	eventType := model.NormalizeSessionEventType(input.EventType)
	if !model.ValidSessionEventType(eventType) {
		return AgentSessionStateRecord{}, RuntimeEventRecord{}, fmt.Errorf("invalid session event type: %s", strings.TrimSpace(input.EventType))
	}
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return AgentSessionStateRecord{}, RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	sessionID := strings.TrimSpace(input.SessionID)
	if sessionID == "" {
		return AgentSessionStateRecord{}, RuntimeEventRecord{}, errors.New("session_id is required")
	}
	agentID := strings.TrimSpace(input.AgentID)
	if agentID == "" {
		return AgentSessionStateRecord{}, RuntimeEventRecord{}, errors.New("agent_id is required")
	}
	actorType := strings.TrimSpace(runtimeActorType)
	if actorType == "" {
		actorType = "agent"
	}
	actorID := strings.TrimSpace(runtimeActorID)
	if actorID == "" {
		actorID = agentID
	}

	status := model.NormalizeStatus(input.Status)
	if status == "" {
		status = model.DefaultSessionStatusForEvent(eventType)
	}
	updatedAt := strings.TrimSpace(input.UpdatedAt)
	if updatedAt == "" {
		updatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	keepSessionActive := input.KeepSessionActive
	if keepSessionActive == nil {
		defaultKeep := model.DefaultKeepSessionActiveForEvent(eventType)
		keepSessionActive = &defaultKeep
	}
	inputIterations := nonNegativeSessionMetric(input.Iterations)
	inputTotalInputTokens := nonNegativeSessionMetric(input.TotalInputTokens)
	inputTotalOutputTokens := nonNegativeSessionMetric(input.TotalOutputTokens)
	inputToolCalls := nonNegativeSessionMetric(input.ToolCalls)

	payload := model.SessionCoordinationPayloadV1{
		SessionID:           sessionID,
		TaskID:              strings.TrimSpace(input.TaskID),
		Status:              status,
		Summary:             strings.TrimSpace(input.Summary),
		OwnerScope:          strings.TrimSpace(input.OwnerScope),
		BlockedOn:           input.BlockedOn,
		DecisionNeededFrom:  strings.TrimSpace(input.DecisionNeededFrom),
		DecisionType:        strings.TrimSpace(input.DecisionType),
		KeepSessionActive:   keepSessionActive,
		HandoffTo:           strings.TrimSpace(input.HandoffTo),
		RelatedDocKeys:      input.RelatedDocKeys,
		RelatedArtifactRefs: input.RelatedArtifactRefs,
		UpdatedAt:           updatedAt,
	}
	payload.Normalize()
	if err := payload.ValidateForEvent(eventType); err != nil {
		return AgentSessionStateRecord{}, RuntimeEventRecord{}, err
	}

	if err := s.ensureAgentInWorkspaceTx(ctx, tx, workspaceID, agentID); err != nil {
		if !(eventType == model.SessionEventEnd && errors.Is(err, ErrAgentNotFound)) {
			return AgentSessionStateRecord{}, RuntimeEventRecord{}, err
		}
		if err := s.ensureTerminalSessionAgentRowTx(ctx, tx, workspaceID, agentID, updatedAt, actorID); err != nil {
			return AgentSessionStateRecord{}, RuntimeEventRecord{}, err
		}
	}
	if payload.HandoffTo != "" {
		if err := s.ensureAgentInWorkspaceTx(ctx, tx, workspaceID, payload.HandoffTo); err != nil {
			return AgentSessionStateRecord{}, RuntimeEventRecord{}, fmt.Errorf("invalid handoff target: %w", err)
		}
	}
	startedAt := updatedAt
	resultStatus := payload.Status
	var completedAt sql.NullString
	var existingTaskID sql.NullString
	var existingWorkspaceID string
	var existingAgentID string
	var existingStatus string
	var existingIterations int
	var existingTotalInputTokens int
	var existingTotalOutputTokens int
	var existingToolCalls int
	err := tx.QueryRowContext(
		ctx,
		`SELECT workspace_id, agent_id, started_at, completed_at, task_id, status,
		        iterations, total_input_tokens, total_output_tokens, tool_calls
		 FROM agent_sessions
		 WHERE session_id = ?`,
		sessionID,
	).Scan(
		&existingWorkspaceID, &existingAgentID, &startedAt, &completedAt, &existingTaskID, &existingStatus,
		&existingIterations, &existingTotalInputTokens, &existingTotalOutputTokens, &existingToolCalls,
	)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if eventType == model.SessionEventEnd {
			return AgentSessionStateRecord{}, RuntimeEventRecord{}, ErrSessionNotFound
		}
		if eventType != model.SessionEventStart && payload.TaskID != "" {
			return AgentSessionStateRecord{}, RuntimeEventRecord{}, fmt.Errorf("%w: task-bound session %s update requires durable session start receipt", ErrSessionNotFound, sessionID)
		}
		if payload.TaskID != "" {
			if err := s.ensureWorkspaceTaskAttachedTx(ctx, tx, workspaceID, payload.TaskID); err != nil {
				return AgentSessionStateRecord{}, RuntimeEventRecord{}, err
			}
			if err := s.requireLiveSameOwnerClaimForSessionStartTx(ctx, tx, workspaceID, payload.TaskID, agentID); err != nil {
				return AgentSessionStateRecord{}, RuntimeEventRecord{}, err
			}
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO agent_sessions (
			     session_id, agent_id, workspace_id, task_id, status,
			     iterations, total_input_tokens, total_output_tokens, tool_calls,
			     started_at, completed_at
			 )
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			sessionID,
			agentID,
			workspaceID,
			blankStringOrNil(payload.TaskID),
			payload.Status,
			inputIterations,
			inputTotalInputTokens,
			inputTotalOutputTokens,
			inputToolCalls,
			startedAt,
			blankStringOrNil(sessionCompletedAt(eventType, updatedAt)),
		); err != nil {
			return AgentSessionStateRecord{}, RuntimeEventRecord{}, fmt.Errorf("insert coordination session: %w", err)
		}
	case err != nil:
		return AgentSessionStateRecord{}, RuntimeEventRecord{}, fmt.Errorf("query coordination session: %w", err)
	default:
		if !strings.EqualFold(strings.TrimSpace(existingWorkspaceID), workspaceID) ||
			!strings.EqualFold(strings.TrimSpace(existingAgentID), agentID) {
			return AgentSessionStateRecord{}, RuntimeEventRecord{}, sessionOwnershipMismatchError(sessionID)
		}
		if !model.IsSessionStatusActive(existingStatus) &&
			(eventType != model.SessionEventEnd || !shouldPreserveTerminalAgentSessionStatus(eventType, existingStatus)) {
			return AgentSessionStateRecord{}, RuntimeEventRecord{}, sessionNotActiveError(sessionID, existingStatus)
		}
		taskID := payload.TaskID
		if taskID == "" && existingTaskID.Valid {
			taskID = existingTaskID.String
			payload.TaskID = taskID
		}
		if taskID != "" {
			if err := s.ensureWorkspaceTaskAttachedTx(ctx, tx, workspaceID, taskID); err != nil {
				return AgentSessionStateRecord{}, RuntimeEventRecord{}, err
			}
			if eventType == model.SessionEventStart {
				if err := s.requireLiveSameOwnerClaimForSessionStartTx(ctx, tx, workspaceID, taskID, agentID); err != nil {
					return AgentSessionStateRecord{}, RuntimeEventRecord{}, err
				}
			} else if eventType != model.SessionEventEnd {
				if err := s.requireLiveSameOwnerClaimForSessionStartTx(ctx, tx, workspaceID, taskID, agentID); err != nil {
					return AgentSessionStateRecord{}, RuntimeEventRecord{}, err
				}
				hasStartReceipt, err := executionSessionHasStartReceipt(ctx, tx, workspaceID, sessionID, agentID, taskID)
				if err != nil {
					return AgentSessionStateRecord{}, RuntimeEventRecord{}, err
				}
				if !hasStartReceipt {
					return AgentSessionStateRecord{}, RuntimeEventRecord{}, fmt.Errorf("%w: task-bound session %s update requires durable session start receipt", ErrSessionNotFound, sessionID)
				}
			}
		}
		storedStatus := payload.Status
		if shouldPreserveTerminalAgentSessionStatus(eventType, existingStatus) {
			storedStatus = strings.TrimSpace(existingStatus)
		}
		storedIterations := maxSessionMetric(existingIterations, inputIterations)
		storedTotalInputTokens := maxSessionMetric(existingTotalInputTokens, inputTotalInputTokens)
		storedTotalOutputTokens := maxSessionMetric(existingTotalOutputTokens, inputTotalOutputTokens)
		storedToolCalls := maxSessionMetric(existingToolCalls, inputToolCalls)
		resultStatus = storedStatus
		completed := sessionCompletedAt(eventType, updatedAt)
		if eventType == model.SessionEventStart {
			completed = ""
		}
		updateResult, err := tx.ExecContext(
			ctx,
			`UPDATE agent_sessions
			 SET task_id = COALESCE(?, task_id),
			     status = ?,
			     iterations = ?,
			     total_input_tokens = ?,
			     total_output_tokens = ?,
			     tool_calls = ?,
			     completed_at = ?
			 WHERE session_id = ? AND workspace_id = ? AND agent_id = ?`,
			blankStringOrNil(taskID),
			storedStatus,
			storedIterations,
			storedTotalInputTokens,
			storedTotalOutputTokens,
			storedToolCalls,
			blankStringOrNil(completed),
			sessionID,
			workspaceID,
			agentID,
		)
		if err != nil {
			return AgentSessionStateRecord{}, RuntimeEventRecord{}, fmt.Errorf("update coordination session: %w", err)
		}
		rowsAffected, err := updateResult.RowsAffected()
		if err != nil {
			return AgentSessionStateRecord{}, RuntimeEventRecord{}, fmt.Errorf("inspect coordination session update: %w", err)
		}
		if rowsAffected != 1 {
			return AgentSessionStateRecord{}, RuntimeEventRecord{}, sessionOwnershipMismatchError(sessionID)
		}
	}

	payloadJSON, err := marshalSessionPromptContextPayloadJSON(payload, input.PromptContextEnvelope)
	if err != nil {
		return AgentSessionStateRecord{}, RuntimeEventRecord{}, fmt.Errorf("encode session payload: %w", err)
	}

	updateID := nextID("agent_update")
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO agent_updates(update_id, workspace_id, agent_id, update_type, summary, payload_json, requires_human, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		updateID,
		workspaceID,
		agentID,
		eventType,
		payload.Summary,
		string(payloadJSON),
		boolToInt(eventType == model.SessionEventDecisionNeeded),
		updatedAt,
	); err != nil {
		return AgentSessionStateRecord{}, RuntimeEventRecord{}, fmt.Errorf("insert session coordination update: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, updatedAt, workspaceID); err != nil {
		return AgentSessionStateRecord{}, RuntimeEventRecord{}, fmt.Errorf("touch workspace after session coordination: %w", err)
	}

	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:    nextID("audit"),
		EventType:  "agent_session_coordination_recorded",
		EntityType: "agent_session",
		EntityID:   sessionID,
		ActorID:    actorID,
		PayloadJSON: mustJSON(map[string]any{
			"workspace_id": workspaceID,
			"actor_type":   actorType,
			"actor_id":     actorID,
			"agent_id":     agentID,
			"event_type":   eventType,
			"payload":      payload,
		}),
	}); err != nil {
		return AgentSessionStateRecord{}, RuntimeEventRecord{}, err
	}

	record := AgentSessionStateRecord{
		SessionID:           payload.SessionID,
		WorkspaceID:         workspaceID,
		AgentID:             agentID,
		TaskID:              payload.TaskID,
		Status:              resultStatus,
		Summary:             payload.Summary,
		OwnerScope:          payload.OwnerScope,
		BlockedOn:           payload.BlockedOn,
		DecisionNeededFrom:  payload.DecisionNeededFrom,
		DecisionType:        payload.DecisionType,
		KeepSessionActive:   payload.KeepSessionActive,
		HandoffTo:           payload.HandoffTo,
		RelatedDocKeys:      payload.RelatedDocKeys,
		RelatedArtifactRefs: payload.RelatedArtifactRefs,
		Iterations:          maxSessionMetric(existingIterations, inputIterations),
		TotalInputTokens:    maxSessionMetric(existingTotalInputTokens, inputTotalInputTokens),
		TotalOutputTokens:   maxSessionMetric(existingTotalOutputTokens, inputTotalOutputTokens),
		ToolCalls:           maxSessionMetric(existingToolCalls, inputToolCalls),
		UpdateType:          eventType,
		UpdatedAt:           updatedAt,
		StartedAt:           startedAt,
		CompletedAt:         sessionCompletedAt(eventType, updatedAt),
	}
	recordPayloadJSON, err := marshalSessionPromptContextPayloadJSON(record, input.PromptContextEnvelope)
	if err != nil {
		return AgentSessionStateRecord{}, RuntimeEventRecord{}, fmt.Errorf("encode session runtime payload: %w", err)
	}
	runtimeEventID := nextID("rtev")
	runtimeEvent, err := s.appendRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
		EventID:     runtimeEventID,
		WorkspaceID: workspaceID,
		EventType:   eventType,
		EntityType:  "agent_session",
		EntityID:    sessionID,
		ActorType:   actorType,
		ActorID:     actorID,
		AgentID:     agentID,
		SessionID:   sessionID,
		TaskID:      payload.TaskID,
		PayloadJSON: recordPayloadJSON,
		CreatedAt:   updatedAt,
	})
	if err != nil {
		return AgentSessionStateRecord{}, RuntimeEventRecord{}, err
	}
	if _, _, err := s.recordSessionLifecycleEpisodePackTx(ctx, tx, record, eventType, runtimeEventID); err != nil {
		return AgentSessionStateRecord{}, RuntimeEventRecord{}, err
	}

	return record, runtimeEvent, nil
}

func (s *Store) requireLiveSameOwnerClaimForSessionStartTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, agentID string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	agentID = strings.TrimSpace(agentID)
	if workspaceID == "" || taskID == "" || agentID == "" {
		return errors.New("workspace_id, task_id, and agent_id are required for session claim binding")
	}
	var taskStatus string
	var claimAgentID sql.NullString
	var claimStatus sql.NullString
	err := tx.QueryRowContext(ctx, `
		SELECT t.status, tc.agent_id, tc.claim_status
		  FROM tasks t
		  JOIN workspace_tasks wt ON wt.task_id = t.task_id
		  LEFT JOIN task_claims tc ON tc.task_id = t.task_id AND tc.workspace_id = wt.workspace_id
		 WHERE wt.workspace_id = ? AND t.task_id = ?`,
		workspaceID, taskID,
	).Scan(&taskStatus, &claimAgentID, &claimStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrWorkspaceTaskAbsent
	}
	if err != nil {
		return fmt.Errorf("check task claim for session start: %w", err)
	}
	claimOwner := strings.TrimSpace(claimAgentID.String)
	claimState := strings.ToUpper(strings.TrimSpace(claimStatus.String))
	if !strings.EqualFold(claimOwner, agentID) || claimState != model.TaskClaimStatusClaimed {
		return fmt.Errorf("session start requires live same-owner task claim: task_id=%s task_status=%s claim_agent_id=%s claim_status=%s agent_id=%s", taskID, strings.TrimSpace(taskStatus), firstNonEmpty(claimOwner, "<none>"), firstNonEmpty(claimState, "<none>"), agentID)
	}
	if isTerminalTaskStatus(taskStatus) {
		return fmt.Errorf("session start requires non-terminal claimed task: task_id=%s task_status=%s agent_id=%s", taskID, strings.TrimSpace(taskStatus), agentID)
	}
	if strings.TrimSpace(taskStatus) != model.TaskStatusRunning {
		return fmt.Errorf("session start requires running claimed task: task_id=%s task_status=%s agent_id=%s", taskID, strings.TrimSpace(taskStatus), agentID)
	}
	return nil
}

func marshalSessionPromptContextPayloadJSON(value any, envelope map[string]any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	payload := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &payload); err != nil {
			return "", err
		}
		if payload == nil {
			payload = map[string]any{}
		}
	}
	if envelope != nil {
		payload, err = AttachSessionPromptContextEnvelope(payload, envelope)
		if err != nil {
			return "", err
		}
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func (s *Store) TakeOverAgentSession(ctx context.Context, input AgentSessionTakeoverInput) (AgentSessionTakeoverRecord, error) {
	record, _, err := s.takeOverAgentSessionWithEvent(ctx, input)
	return record, err
}

func (s *Store) transferTaskClaimForSessionTakeoverTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, workspaceID, taskID, sourceAgentID, takeoverAgentID, sourceSessionID, successorSessionID, summary, now string) (RuntimeEventRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID = strings.TrimSpace(taskID)
	sourceAgentID = strings.TrimSpace(sourceAgentID)
	takeoverAgentID = strings.TrimSpace(takeoverAgentID)
	if workspaceID == "" || taskID == "" || sourceAgentID == "" || takeoverAgentID == "" {
		return RuntimeEventRecord{}, errors.New("workspace_id, task_id, source_agent_id, and takeover_agent_id are required for session claim takeover")
	}
	if err := s.ensureWorkspaceTaskAttachedTx(ctx, tx, workspaceID, taskID); err != nil {
		return RuntimeEventRecord{}, err
	}
	if err := s.ensureAgentInWorkspaceTx(ctx, tx, workspaceID, takeoverAgentID); err != nil {
		return RuntimeEventRecord{}, err
	}
	taskSnapshot, ok, err := loadTaskTransitionSnapshotTx(ctx, tx, taskID)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	if !ok {
		return RuntimeEventRecord{}, ErrWorkspaceTaskAbsent
	}
	if isTerminalTaskStatus(taskSnapshot.Status) {
		return RuntimeEventRecord{}, ErrTaskClaimStaleTransition
	}
	claimSnapshot, ok, err := loadTaskClaimTransitionSnapshotTx(ctx, tx, taskID, workspaceID)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	if !ok {
		return RuntimeEventRecord{}, ErrTaskClaimNotFound
	}
	if claimSnapshot.AgentID != sourceAgentID || claimSnapshot.ClaimStatus != model.TaskClaimStatusClaimed {
		return RuntimeEventRecord{}, ErrTaskClaimStaleTransition
	}
	res, err := tx.ExecContext(ctx,
		`UPDATE task_claims
		    SET agent_id = ?, summary = ?, updated_at = ?
		  WHERE task_id = ? AND workspace_id = ? AND agent_id = ? AND claim_status = ? AND updated_at = ?`,
		takeoverAgentID,
		firstNonEmpty(strings.TrimSpace(summary), "Session takeover from "+sourceAgentID),
		now,
		taskID,
		workspaceID,
		sourceAgentID,
		model.TaskClaimStatusClaimed,
		claimSnapshot.UpdatedAt,
	)
	if err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("transfer task claim for session takeover: %w", err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		return RuntimeEventRecord{}, ErrTaskClaimStaleTransition
	}
	if _, err := tx.ExecContext(ctx, `UPDATE tasks SET status = ?, updated_at = ? WHERE task_id = ? AND status = ?`, model.TaskStatusRunning, now, taskID, taskSnapshot.Status); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("touch task after session claim takeover: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
		return RuntimeEventRecord{}, fmt.Errorf("touch workspace after session claim takeover: %w", err)
	}
	if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
		EventID:    nextID("audit"),
		EventType:  "task_claim_takeover",
		EntityType: "task_claim",
		EntityID:   taskID,
		ActorID:    takeoverAgentID,
		PayloadJSON: mustJSON(map[string]any{
			"workspace_id":         workspaceID,
			"task_id":              taskID,
			"source_agent_id":      sourceAgentID,
			"takeover_agent_id":    takeoverAgentID,
			"source_session_id":    strings.TrimSpace(sourceSessionID),
			"successor_session_id": strings.TrimSpace(successorSessionID),
			"summary":              strings.TrimSpace(summary),
		}),
	}); err != nil {
		return RuntimeEventRecord{}, err
	}
	runtimePayload, err := attachAgentTaskPromptContextEnvelope(map[string]any{
		"workspace_id":           workspaceID,
		"task_id":                taskID,
		"agent_id":               takeoverAgentID,
		"claim_status":           model.TaskClaimStatusClaimed,
		"takeover_from_agent_id": sourceAgentID,
		"source_session_id":      strings.TrimSpace(sourceSessionID),
		"successor_session_id":   strings.TrimSpace(successorSessionID),
		"summary":                strings.TrimSpace(summary),
	}, nil, "agent.task.claim", workspaceID, taskID, takeoverAgentID)
	if err != nil {
		return RuntimeEventRecord{}, err
	}
	return s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
		WorkspaceID: workspaceID,
		EventType:   "task.claimed",
		EntityType:  "task",
		EntityID:    taskID,
		ActorType:   "agent",
		ActorID:     takeoverAgentID,
		AgentID:     takeoverAgentID,
		TaskID:      taskID,
		PayloadJSON: mustJSON(runtimePayload),
		CreatedAt:   now,
	})
}

func (s *Store) takeOverAgentSessionWithEvent(ctx context.Context, input AgentSessionTakeoverInput) (AgentSessionTakeoverRecord, RuntimeEventRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return AgentSessionTakeoverRecord{}, RuntimeEventRecord{}, errors.New("workspace_id is required")
	}
	sessionID := strings.TrimSpace(input.SessionID)
	if sessionID == "" {
		return AgentSessionTakeoverRecord{}, RuntimeEventRecord{}, errors.New("session_id is required")
	}
	takeoverAgentID := strings.TrimSpace(input.TakeoverAgentID)
	if takeoverAgentID == "" {
		return AgentSessionTakeoverRecord{}, RuntimeEventRecord{}, errors.New("takeover_agent_id is required")
	}
	summary := strings.TrimSpace(input.Summary)
	if summary == "" {
		return AgentSessionTakeoverRecord{}, RuntimeEventRecord{}, errors.New("summary is required")
	}
	updatedAt := strings.TrimSpace(input.UpdatedAt)
	if updatedAt == "" {
		updatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, updatedAt)
	if err != nil {
		return AgentSessionTakeoverRecord{}, RuntimeEventRecord{}, err
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return AgentSessionTakeoverRecord{}, RuntimeEventRecord{}, fmt.Errorf("begin agent session takeover tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var record AgentSessionTakeoverRecord
	var takeoverEvent RuntimeEventRecord
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureAgentInWorkspaceTx(ctx, tx, workspaceID, takeoverAgentID); err != nil {
			return err
		}

		sourceState, err := s.getAgentSessionStateTx(ctx, tx, workspaceID, sessionID)
		if err != nil {
			return err
		}
		if !model.IsSessionStatusActive(sourceState.Status) {
			return sessionNotActiveError(sourceState.SessionID, sourceState.Status)
		}
		if strings.EqualFold(strings.TrimSpace(sourceState.AgentID), takeoverAgentID) {
			return ErrSessionTakeoverAgentSame
		}
		if !strings.EqualFold(strings.TrimSpace(sourceState.HandoffTo), takeoverAgentID) {
			return sessionTakeoverNotAuthorizedError(sourceState.SessionID, takeoverAgentID)
		}

		successorSessionID := strings.TrimSpace(input.SuccessorSessionID)
		if successorSessionID == "" {
			successorSessionID = nextID("session")
		}
		if successorSessionID == sessionID {
			return ErrSessionTakeoverSuccessorSame
		}
		var successorExists int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM agent_sessions WHERE session_id = ?`, successorSessionID).Scan(&successorExists); err != nil {
			return fmt.Errorf("check successor session existence: %w", err)
		}
		if successorExists > 0 {
			return fmt.Errorf("%w: %s", ErrSessionTakeoverSuccessorExists, successorSessionID)
		}

		sourceSummary := "Handed off to " + takeoverAgentID + ": " + summary
		sourceEndState, _, err := s.recordAgentSessionCoordinationWithAuthorityTx(ctx, tx, authority, AgentSessionCoordinationInput{
			EventType:             model.SessionEventEnd,
			WorkspaceID:           workspaceID,
			SessionID:             sourceState.SessionID,
			AgentID:               sourceState.AgentID,
			TaskID:                sourceState.TaskID,
			Summary:               sourceSummary,
			Status:                model.SessionStatusEnded,
			OwnerScope:            sourceState.OwnerScope,
			BlockedOn:             sourceState.BlockedOn,
			DecisionNeededFrom:    sourceState.DecisionNeededFrom,
			DecisionType:          sourceState.DecisionType,
			HandoffTo:             takeoverAgentID,
			RelatedDocKeys:        sourceState.RelatedDocKeys,
			RelatedArtifactRefs:   sourceState.RelatedArtifactRefs,
			UpdatedAt:             updatedAt,
			PromptContextEnvelope: input.PromptContextEnvelope,
		})
		if err != nil {
			return err
		}

		// CA-17: resolve the source session's operator queues inside the takeover tx,
		// mirroring the reclaim path (session_reclaim.go). The source session is now
		// ENDED; its OPEN BLOCKER/DECISION/HANDOFF queues are only clearable by a
		// source-keyed sync (the lineage guard requires source_id == session_id), so
		// without this they would be orphaned -- an OPEN queue referencing a dead
		// session -- on every caller, including the CLI/store-direct TakeOverAgentSession
		// path (both public entrypoints share this private tx). sourceEndState carries
		// Status=ENDED + UpdateType=session.end, which opens nothing and resolves the
		// source-lineage queues.
		if _, err := s.syncOperatorQueueFromSessionStateWithAuthorityTx(ctx, tx, authority, sourceEndState, updatedAt, ""); err != nil {
			return err
		}

		if strings.TrimSpace(sourceState.TaskID) != "" {
			if _, err := s.transferTaskClaimForSessionTakeoverTx(ctx, tx, authority, workspaceID, sourceState.TaskID, sourceState.AgentID, takeoverAgentID, sourceState.SessionID, successorSessionID, summary, updatedAt); err != nil {
				return err
			}
		}

		successorSummary := strings.TrimSpace(input.SuccessorSummary)
		if successorSummary == "" {
			successorSummary = "Takeover from " + sourceState.AgentID + ": " + summary
		}
		successorState, _, err := s.recordAgentSessionCoordinationWithAuthorityTx(ctx, tx, authority, AgentSessionCoordinationInput{
			EventType:             model.SessionEventStart,
			WorkspaceID:           workspaceID,
			SessionID:             successorSessionID,
			AgentID:               takeoverAgentID,
			TaskID:                sourceState.TaskID,
			Summary:               successorSummary,
			Status:                model.SessionStatusActive,
			OwnerScope:            sourceState.OwnerScope,
			BlockedOn:             sourceState.BlockedOn,
			DecisionNeededFrom:    sourceState.DecisionNeededFrom,
			DecisionType:          sourceState.DecisionType,
			HandoffTo:             "",
			RelatedDocKeys:        sourceState.RelatedDocKeys,
			RelatedArtifactRefs:   sourceState.RelatedArtifactRefs,
			UpdatedAt:             updatedAt,
			PromptContextEnvelope: input.PromptContextEnvelope,
		})
		if err != nil {
			return err
		}

		if err := s.addAuditEventTx(ctx, tx, AuditEventInput{
			EventID:    nextID("audit"),
			EventType:  "agent_session_taken_over",
			EntityType: "agent_session",
			EntityID:   sourceState.SessionID,
			ActorID:    takeoverAgentID,
			PayloadJSON: mustJSON(map[string]any{
				"workspace_id":         workspaceID,
				"source_session_id":    sourceState.SessionID,
				"source_agent_id":      sourceState.AgentID,
				"successor_session_id": successorSessionID,
				"takeover_agent_id":    takeoverAgentID,
				"summary":              summary,
			}),
		}); err != nil {
			return err
		}
		takeoverEventID := nextID("rtev")
		takeoverPayloadJSON, err := marshalSessionPromptContextPayloadJSON(map[string]any{
			"source_state":    sourceEndState,
			"successor_state": successorState,
		}, input.PromptContextEnvelope)
		if err != nil {
			return err
		}
		takeoverEvent, err = s.appendRuntimeEventWithOptionalAuthorityTx(ctx, tx, authority, RuntimeEventInput{
			EventID:     takeoverEventID,
			WorkspaceID: workspaceID,
			EventType:   "session.takeover",
			EntityType:  "agent_session",
			EntityID:    sourceState.SessionID,
			ActorType:   "agent",
			ActorID:     takeoverAgentID,
			AgentID:     takeoverAgentID,
			SessionID:   sourceState.SessionID,
			TaskID:      firstNonEmpty(sourceEndState.TaskID, successorState.TaskID),
			PayloadJSON: takeoverPayloadJSON,
			CreatedAt:   updatedAt,
		})
		if err != nil {
			return err
		}
		if _, err := s.recordSessionTakeoverEpisodePackTx(ctx, tx, sourceEndState, successorState, summary, takeoverEventID, updatedAt); err != nil {
			return err
		}
		record = AgentSessionTakeoverRecord{
			SourceState:    sourceEndState,
			SuccessorState: successorState,
		}
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return AgentSessionTakeoverRecord{}, RuntimeEventRecord{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	if err := tx.Commit(); err != nil {
		return AgentSessionTakeoverRecord{}, RuntimeEventRecord{}, fmt.Errorf("commit session takeover tx: %w", err)
	}
	s.bestEffortReconcileMemoryProjectionWorkspace(ctx, workspaceID)
	return record, takeoverEvent, nil
}

func (s *Store) TakeOverAgentSessionWithEvent(ctx context.Context, input AgentSessionTakeoverInput) (AgentSessionTakeoverRecord, RuntimeEventRecord, error) {
	return s.takeOverAgentSessionWithEvent(ctx, input)
}

func (s *Store) ListWorkspaceSessionStates(ctx context.Context, workspaceID string, activeOnly bool, limit int) ([]AgentSessionStateRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if limit <= 0 {
		limit = 20
	}

	var rows *sql.Rows
	var err error
	if activeOnly {
		rows, err = s.db.QueryContext(
			ctx,
			`SELECT session_id, workspace_id, agent_id, COALESCE(task_id,''), status, started_at, COALESCE(completed_at,'')
			 FROM agent_sessions
			 WHERE workspace_id = ?
			   AND UPPER(TRIM(status)) IN ('ACTIVE', 'BLOCKED', 'WAITING_DECISION', 'HANDOFF_PENDING', 'RUNNING')
			 ORDER BY started_at DESC, session_id DESC`,
			workspaceID,
		)
	} else {
		rows, err = s.db.QueryContext(
			ctx,
			`SELECT session_id, workspace_id, agent_id, COALESCE(task_id,''), status, started_at, COALESCE(completed_at,'')
			 FROM agent_sessions
			 WHERE workspace_id = ?
			 ORDER BY started_at DESC, session_id DESC
			 LIMIT ?`,
			workspaceID,
			limit,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("query workspace session states: %w", err)
	}
	defer rows.Close()

	sessionOrder := make([]string, 0, limit)
	states := make(map[string]AgentSessionStateRecord, limit)
	for rows.Next() {
		var record AgentSessionStateRecord
		if err := rows.Scan(
			&record.SessionID,
			&record.WorkspaceID,
			&record.AgentID,
			&record.TaskID,
			&record.Status,
			&record.StartedAt,
			&record.CompletedAt,
		); err != nil {
			return nil, fmt.Errorf("scan workspace session state: %w", err)
		}
		record.UpdatedAt = record.StartedAt
		sessionOrder = append(sessionOrder, record.SessionID)
		states[record.SessionID] = record
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspace session states: %w", err)
	}
	if len(states) == 0 {
		return []AgentSessionStateRecord{}, nil
	}

	sessionUpdates, err := s.listLatestSessionCoordinationUpdates(ctx, workspaceID, sessionOrder)
	if err != nil {
		return nil, err
	}
	for sessionID, update := range sessionUpdates {
		record := states[sessionID]
		if shouldIgnoreStaleActiveSessionUpdate(record.Status, update.Status) {
			continue
		}
		record.Status = update.Status
		record.Summary = update.Summary
		record.OwnerScope = update.OwnerScope
		record.BlockedOn = update.BlockedOn
		record.DecisionNeededFrom = update.DecisionNeededFrom
		record.DecisionType = update.DecisionType
		record.KeepSessionActive = update.KeepSessionActive
		record.HandoffTo = update.HandoffTo
		record.RelatedDocKeys = update.RelatedDocKeys
		record.RelatedArtifactRefs = update.RelatedArtifactRefs
		record.UpdateType = update.UpdateType
		record.UpdatedAt = update.UpdatedAt
		if update.TaskID != "" {
			record.TaskID = update.TaskID
		}
		states[sessionID] = record
	}

	out := make([]AgentSessionStateRecord, 0, len(sessionOrder))
	for _, sessionID := range sessionOrder {
		record := states[sessionID]
		if activeOnly && !model.IsSessionStatusActive(record.Status) {
			continue
		}
		if activeOnly {
			hasReceipt, err := executionSessionHasStartReceipt(ctx, s.db, record.WorkspaceID, record.SessionID, record.AgentID, record.TaskID)
			if err != nil {
				return nil, err
			}
			if !hasReceipt {
				continue
			}
		}
		out = append(out, record)
		if activeOnly && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *Store) getAgentSessionStateTx(ctx context.Context, tx *sql.Tx, workspaceID, sessionID string) (AgentSessionStateRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	sessionID = strings.TrimSpace(sessionID)
	if workspaceID == "" {
		return AgentSessionStateRecord{}, errors.New("workspace_id is required")
	}
	if sessionID == "" {
		return AgentSessionStateRecord{}, errors.New("session_id is required")
	}

	var record AgentSessionStateRecord
	var row interface{ Scan(dest ...any) error }
	if tx != nil {
		row = tx.QueryRowContext(
			ctx,
			`SELECT session_id, workspace_id, agent_id, COALESCE(task_id,''), status, started_at, COALESCE(completed_at,'')
			 FROM agent_sessions
			 WHERE workspace_id = ? AND session_id = ?`,
			workspaceID,
			sessionID,
		)
	} else {
		row = s.db.QueryRowContext(
			ctx,
			`SELECT session_id, workspace_id, agent_id, COALESCE(task_id,''), status, started_at, COALESCE(completed_at,'')
			 FROM agent_sessions
			 WHERE workspace_id = ? AND session_id = ?`,
			workspaceID,
			sessionID,
		)
	}
	err := row.Scan(
		&record.SessionID,
		&record.WorkspaceID,
		&record.AgentID,
		&record.TaskID,
		&record.Status,
		&record.StartedAt,
		&record.CompletedAt,
	)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return AgentSessionStateRecord{}, ErrSessionNotFound
	case err != nil:
		return AgentSessionStateRecord{}, fmt.Errorf("query workspace session state: %w", err)
	}
	record.UpdatedAt = record.StartedAt

	queryFn := s.db.QueryContext
	if tx != nil {
		queryFn = tx.QueryContext
	}
	update, found, err := loadLatestSessionCoordinationUpdateFromQuery(ctx, queryFn, workspaceID, sessionID)
	if err != nil {
		return AgentSessionStateRecord{}, err
	}
	if found {
		if shouldIgnoreStaleActiveSessionUpdate(record.Status, update.Status) {
			return record, nil
		}
		record.Status = update.Status
		record.Summary = update.Summary
		record.OwnerScope = update.OwnerScope
		record.BlockedOn = update.BlockedOn
		record.DecisionNeededFrom = update.DecisionNeededFrom
		record.DecisionType = update.DecisionType
		record.KeepSessionActive = update.KeepSessionActive
		record.HandoffTo = update.HandoffTo
		record.RelatedDocKeys = update.RelatedDocKeys
		record.RelatedArtifactRefs = update.RelatedArtifactRefs
		record.UpdateType = update.UpdateType
		record.UpdatedAt = update.UpdatedAt
		if update.TaskID != "" {
			record.TaskID = update.TaskID
		}
	}
	return record, nil
}

func shouldIgnoreStaleActiveSessionUpdate(baseStatus, updateStatus string) bool {
	return !model.IsSessionStatusActive(baseStatus) && model.IsSessionStatusActive(updateStatus)
}

func (s *Store) GetAgentSessionState(ctx context.Context, workspaceID, sessionID string) (AgentSessionStateRecord, error) {
	return s.getAgentSessionStateTx(ctx, nil, workspaceID, sessionID)
}

func (s *Store) listLatestSessionCoordinationUpdates(ctx context.Context, workspaceID string, sessionOrder []string) (map[string]AgentSessionStateRecord, error) {
	targets := make(map[string]struct{}, len(sessionOrder))
	for _, sessionID := range sessionOrder {
		targets[sessionID] = struct{}{}
	}

	rows, err := s.db.QueryContext(
		ctx,
		`SELECT update_type, summary, payload_json, created_at
		 FROM agent_updates
		 WHERE workspace_id = ?
		   AND update_type IN (?, ?, ?, ?, ?, ?)
		 ORDER BY created_at DESC, update_id DESC`,
		workspaceID,
		model.SessionEventStart,
		model.SessionEventStatus,
		model.SessionEventBlocked,
		model.SessionEventDecisionNeeded,
		model.SessionEventKeepalive,
		model.SessionEventEnd,
	)
	if err != nil {
		return nil, fmt.Errorf("query session coordination updates: %w", err)
	}
	defer rows.Close()

	out := make(map[string]AgentSessionStateRecord, len(targets))
	for rows.Next() {
		var updateType, summary, payloadJSON, createdAt string
		if err := rows.Scan(&updateType, &summary, &payloadJSON, &createdAt); err != nil {
			return nil, fmt.Errorf("scan session coordination update: %w", err)
		}
		var payload model.SessionCoordinationPayloadV1
		if err := json.Unmarshal([]byte(strings.TrimSpace(payloadJSON)), &payload); err != nil {
			continue
		}
		payload.Normalize()
		if payload.SessionID == "" {
			continue
		}
		if _, ok := targets[payload.SessionID]; !ok {
			continue
		}
		if _, already := out[payload.SessionID]; already {
			continue
		}
		updatedAt := payload.UpdatedAt
		if updatedAt == "" {
			updatedAt = createdAt
		}
		out[payload.SessionID] = AgentSessionStateRecord{
			SessionID:           payload.SessionID,
			TaskID:              payload.TaskID,
			Status:              payload.Status,
			Summary:             summary,
			OwnerScope:          payload.OwnerScope,
			BlockedOn:           payload.BlockedOn,
			DecisionNeededFrom:  payload.DecisionNeededFrom,
			DecisionType:        payload.DecisionType,
			KeepSessionActive:   payload.KeepSessionActive,
			HandoffTo:           payload.HandoffTo,
			RelatedDocKeys:      payload.RelatedDocKeys,
			RelatedArtifactRefs: payload.RelatedArtifactRefs,
			UpdateType:          updateType,
			UpdatedAt:           updatedAt,
		}
		if len(out) == len(targets) {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate session coordination updates: %w", err)
	}
	return out, nil
}

func loadLatestSessionCoordinationUpdateFromQuery(
	ctx context.Context,
	queryFn func(context.Context, string, ...any) (*sql.Rows, error),
	workspaceID, sessionID string,
) (AgentSessionStateRecord, bool, error) {
	rows, err := queryFn(
		ctx,
		`SELECT update_type, summary, payload_json, created_at
		 FROM agent_updates
		 WHERE workspace_id = ?
		   AND update_type IN (?, ?, ?, ?, ?, ?)
		 ORDER BY created_at DESC, update_id DESC`,
		workspaceID,
		model.SessionEventStart,
		model.SessionEventStatus,
		model.SessionEventBlocked,
		model.SessionEventDecisionNeeded,
		model.SessionEventKeepalive,
		model.SessionEventEnd,
	)
	if err != nil {
		return AgentSessionStateRecord{}, false, fmt.Errorf("query session coordination updates: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var updateType, summary, payloadJSON, createdAt string
		if err := rows.Scan(&updateType, &summary, &payloadJSON, &createdAt); err != nil {
			return AgentSessionStateRecord{}, false, fmt.Errorf("scan session coordination update: %w", err)
		}
		var payload model.SessionCoordinationPayloadV1
		if err := json.Unmarshal([]byte(strings.TrimSpace(payloadJSON)), &payload); err != nil {
			continue
		}
		payload.Normalize()
		if payload.SessionID != sessionID {
			continue
		}
		updatedAt := payload.UpdatedAt
		if updatedAt == "" {
			updatedAt = createdAt
		}
		return AgentSessionStateRecord{
			SessionID:           payload.SessionID,
			TaskID:              payload.TaskID,
			Status:              payload.Status,
			Summary:             summary,
			OwnerScope:          payload.OwnerScope,
			BlockedOn:           payload.BlockedOn,
			DecisionNeededFrom:  payload.DecisionNeededFrom,
			DecisionType:        payload.DecisionType,
			KeepSessionActive:   payload.KeepSessionActive,
			HandoffTo:           payload.HandoffTo,
			RelatedDocKeys:      payload.RelatedDocKeys,
			RelatedArtifactRefs: payload.RelatedArtifactRefs,
			UpdateType:          updateType,
			UpdatedAt:           updatedAt,
		}, true, nil
	}
	if err := rows.Err(); err != nil {
		return AgentSessionStateRecord{}, false, fmt.Errorf("iterate session coordination updates: %w", err)
	}
	return AgentSessionStateRecord{}, false, nil
}

func sessionCompletedAt(eventType, updatedAt string) string {
	if model.NormalizeSessionEventType(eventType) == model.SessionEventEnd {
		return updatedAt
	}
	return ""
}

func shouldPreserveTerminalAgentSessionStatus(eventType, existingStatus string) bool {
	if model.NormalizeSessionEventType(eventType) != model.SessionEventEnd {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(existingStatus)) {
	case "COMPLETED", "FAILED", "ABORTED", "CANCELLED":
		return true
	default:
		return false
	}
}

func (s *Store) ensureTerminalSessionAgentRowTx(ctx context.Context, tx *sql.Tx, workspaceID, agentID, updatedAt, actorID string) error {
	if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
		return err
	}
	capabilitiesJSON, err := json.Marshal([]string{})
	if err != nil {
		return fmt.Errorf("encode terminal session agent capabilities: %w", err)
	}
	ownerUserID := strings.TrimSpace(actorID)
	if ownerUserID == "" {
		ownerUserID = "system"
	}
	displayName := strings.TrimSpace(agentID)
	if displayName == "" {
		displayName = "missing-agent"
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO agents(
		  workspace_id, agent_id, owner_user_id, display_name, role, status, protocol_version,
		  capabilities_json, summary, created_at, updated_at, last_seen_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(workspace_id, agent_id) DO NOTHING`,
		workspaceID,
		agentID,
		ownerUserID,
		displayName,
		"generalist",
		model.AgentStatusOffline,
		"workspace-reclaim/v1",
		string(capabilitiesJSON),
		"Restored for terminal session reclaim bookkeeping",
		updatedAt,
		updatedAt,
		nil,
	); err != nil {
		return fmt.Errorf("restore missing agent row for terminal session coordination: %w", err)
	}
	return nil
}

func sessionOwnershipMismatchError(sessionID string) error {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return ErrSessionOwnershipMismatch
	}
	return fmt.Errorf("%w: %s", ErrSessionOwnershipMismatch, sessionID)
}

func sessionNotActiveError(sessionID, status string) error {
	sessionID = strings.TrimSpace(sessionID)
	status = strings.ToUpper(strings.TrimSpace(status))
	switch {
	case sessionID != "" && status != "":
		return fmt.Errorf("%w: %s (%s)", ErrSessionNotActive, sessionID, status)
	case sessionID != "":
		return fmt.Errorf("%w: %s", ErrSessionNotActive, sessionID)
	case status != "":
		return fmt.Errorf("%w: %s", ErrSessionNotActive, status)
	default:
		return ErrSessionNotActive
	}
}

func sessionTakeoverNotAuthorizedError(sessionID, takeoverAgentID string) error {
	sessionID = strings.TrimSpace(sessionID)
	takeoverAgentID = strings.TrimSpace(takeoverAgentID)
	switch {
	case sessionID != "" && takeoverAgentID != "":
		return fmt.Errorf("%w: %s -> %s", ErrSessionTakeoverNotAuthorized, sessionID, takeoverAgentID)
	case sessionID != "":
		return fmt.Errorf("%w: %s", ErrSessionTakeoverNotAuthorized, sessionID)
	case takeoverAgentID != "":
		return fmt.Errorf("%w: %s", ErrSessionTakeoverNotAuthorized, takeoverAgentID)
	default:
		return ErrSessionTakeoverNotAuthorized
	}
}
