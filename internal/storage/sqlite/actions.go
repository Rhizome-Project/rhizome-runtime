package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
)

// HumanActionInput is the input for creating a human action.
type HumanActionInput struct {
	WorkspaceID           string
	TaskID                string
	AgentID               string
	AssignedTo            string
	Title                 string
	Description           string
	Blocking              bool
	PromptContextEnvelope map[string]any
}

// HumanActionRecord represents a human action row.
type HumanActionRecord struct {
	ActionID          string `json:"action_id"`
	WorkspaceID       string `json:"workspace_id"`
	TaskID            string `json:"task_id"`
	AgentID           string `json:"agent_id"`
	AssignedTo        string `json:"assigned_to,omitempty"`
	Title             string `json:"title"`
	Description       string `json:"description,omitempty"`
	Blocking          bool   `json:"blocking"`
	Status            string `json:"status"`
	Revision          int64  `json:"revision"`
	ResolutionComment string `json:"resolution_comment,omitempty"`
	CreatedAt         string `json:"created_at"`
	ResolvedAt        string `json:"resolved_at,omitempty"`
	ResolvedBy        string `json:"resolved_by,omitempty"`
	TaskTitle         string `json:"task_title,omitempty"`
	TaskPriority      string `json:"task_priority,omitempty"`
	TaskStatus        string `json:"task_status,omitempty"`
}

// ActionMessageRecord represents a chat message for an action.
type ActionMessageRecord struct {
	MessageID   string `json:"message_id"`
	ActionID    string `json:"action_id"`
	WorkspaceID string `json:"workspace_id"`
	FromID      string `json:"from_id"`
	Content     string `json:"content"`
	CreatedAt   string `json:"created_at"`
}

type taskClaimBlockerSnapshot struct {
	PriorExists      bool
	PriorAgentID     string
	PriorClaimStatus string
	PriorSummary     string
	PriorClaimedAt   string
	PriorReleasedAt  sql.NullString
}

type HumanActionQueueResolutionResult struct {
	Action            HumanActionRecord
	ActionQueue       *OperatorQueueSyncEvent
	LinkedSourceQueue []OperatorQueueSyncEvent
	ActionEvent       *RuntimeEventRecord
}

type HumanActionQueueCreateResult struct {
	Action            HumanActionRecord
	ActionQueue       *OperatorQueueSyncEvent
	LinkedSourceQueue *OperatorQueueSyncEvent
	ActionEvent       *RuntimeEventRecord
}

type OperatorQueueRuntimeEventResult struct {
	QueueEvent   OperatorQueueSyncEvent
	RuntimeEvent RuntimeEventRecord
}

var (
	ErrRebaseFollowupStewardRequired = errors.New("rebase follow-up requires active steward lease")
	ErrRebaseFollowupStewardMismatch = errors.New("rebase follow-up steward lease owned by different agent")
)

func mergeRuntimeEventParentRefsJSON(raw string, refs ...string) (string, error) {
	parentRefs, err := parseRuntimeEventParentRefs(raw)
	if err != nil {
		return "", err
	}
	for _, ref := range refs {
		if trimmed := strings.TrimSpace(ref); trimmed != "" {
			parentRefs = append(parentRefs, trimmed)
		}
	}
	if len(parentRefs) == 0 {
		return "", nil
	}
	return normalizeRuntimeEventParentRefs(mustJSON(parentRefs))
}

func optionalRuntimeEventInput(overrides []RuntimeEventInput) *RuntimeEventInput {
	if len(overrides) == 0 {
		return nil
	}
	override := overrides[0]
	return &override
}

func mergeRuntimeEventInputDefaults(base RuntimeEventInput, override *RuntimeEventInput) RuntimeEventInput {
	if override == nil {
		return base
	}
	if trimmed := strings.TrimSpace(override.EventID); trimmed != "" {
		base.EventID = trimmed
	}
	if trimmed := strings.TrimSpace(override.WorkspaceID); trimmed != "" {
		base.WorkspaceID = trimmed
	}
	if trimmed := strings.TrimSpace(override.EventType); trimmed != "" {
		base.EventType = trimmed
	}
	if trimmed := strings.TrimSpace(override.EntityType); trimmed != "" {
		base.EntityType = trimmed
	}
	if trimmed := strings.TrimSpace(override.EntityID); trimmed != "" {
		base.EntityID = trimmed
	}
	if trimmed := strings.TrimSpace(override.ActorType); trimmed != "" {
		base.ActorType = trimmed
	}
	if trimmed := strings.TrimSpace(override.ActorID); trimmed != "" {
		base.ActorID = trimmed
	}
	if trimmed := strings.TrimSpace(override.AgentID); trimmed != "" {
		base.AgentID = trimmed
	}
	if trimmed := strings.TrimSpace(override.TaskID); trimmed != "" {
		base.TaskID = trimmed
	}
	if trimmed := strings.TrimSpace(override.PayloadJSON); trimmed != "" {
		base.PayloadJSON = trimmed
	}
	if trimmed := strings.TrimSpace(override.RootCauseID); trimmed != "" {
		base.RootCauseID = trimmed
	}
	if trimmed := strings.TrimSpace(override.ProvenanceGroupID); trimmed != "" {
		base.ProvenanceGroupID = trimmed
	}
	if trimmed := strings.TrimSpace(override.ParentRefsJSON); trimmed != "" {
		base.ParentRefsJSON = trimmed
	}
	if trimmed := strings.TrimSpace(override.CreatedAt); trimmed != "" {
		base.CreatedAt = trimmed
	}
	return base
}

func payloadParentRefsJSON(refs []string) (string, error) {
	if len(refs) == 0 {
		return "", nil
	}
	return normalizeRuntimeEventParentRefs(mustJSON(refs))
}

func buildActionCreatedRuntimeInput(action HumanActionRecord, sourceQueue *OperatorQueueRecord, rootCauseID, provenanceGroupID, parentRefsJSON string, promptContextEnvelope map[string]any, override *RuntimeEventInput) (RuntimeEventInput, error) {
	payload := map[string]any{
		"action_id":   strings.TrimSpace(action.ActionID),
		"task_id":     strings.TrimSpace(action.TaskID),
		"assigned_to": strings.TrimSpace(action.AssignedTo),
		"blocking":    action.Blocking,
		"title":       strings.TrimSpace(action.Title),
		"description": strings.TrimSpace(action.Description),
	}
	if sourceQueue != nil {
		payload["source_queue_id"] = strings.TrimSpace(sourceQueue.QueueID)
		payload["source_queue_key"] = strings.TrimSpace(sourceQueue.QueueKey)
	}
	if trimmed := strings.TrimSpace(rootCauseID); trimmed != "" {
		payload["root_cause_id"] = trimmed
	}
	if trimmed := strings.TrimSpace(provenanceGroupID); trimmed != "" {
		payload["provenance_group_id"] = trimmed
	}
	if trimmed := strings.TrimSpace(parentRefsJSON); trimmed != "" {
		var refs []string
		if err := json.Unmarshal([]byte(trimmed), &refs); err != nil {
			return RuntimeEventInput{}, fmt.Errorf("decode action.created parent refs: %w", err)
		}
		if len(refs) > 0 {
			payload["parent_refs_json"] = refs
		}
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return RuntimeEventInput{}, fmt.Errorf("marshal action.created payload: %w", err)
	}
	base := RuntimeEventInput{
		EventID:           nextID("rtev"),
		WorkspaceID:       strings.TrimSpace(action.WorkspaceID),
		EventType:         "action.created",
		EntityType:        "human_action",
		EntityID:          strings.TrimSpace(action.ActionID),
		ActorType:         "agent",
		ActorID:           strings.TrimSpace(action.AgentID),
		AgentID:           strings.TrimSpace(action.AgentID),
		TaskID:            strings.TrimSpace(action.TaskID),
		PayloadJSON:       string(payloadJSON),
		RootCauseID:       strings.TrimSpace(rootCauseID),
		ProvenanceGroupID: strings.TrimSpace(provenanceGroupID),
		ParentRefsJSON:    strings.TrimSpace(parentRefsJSON),
	}
	merged := mergeRuntimeEventInputDefaults(base, override)
	if promptContextEnvelope != nil {
		mergedPayload := map[string]any{}
		if trimmedPayload := strings.TrimSpace(merged.PayloadJSON); trimmedPayload != "" {
			if err := json.Unmarshal([]byte(trimmedPayload), &mergedPayload); err != nil {
				return RuntimeEventInput{}, fmt.Errorf("decode action.created payload for prompt context envelope: %w", err)
			}
			if mergedPayload == nil {
				mergedPayload = map[string]any{}
			}
		}
		mergedPayload, err = AttachHumanActionPromptContextEnvelope(mergedPayload, promptContextEnvelope)
		if err != nil {
			return RuntimeEventInput{}, err
		}
		encodedPayload, err := json.Marshal(mergedPayload)
		if err != nil {
			return RuntimeEventInput{}, fmt.Errorf("marshal action.created prompt context payload: %w", err)
		}
		merged.PayloadJSON = string(encodedPayload)
	}
	return merged, nil
}

func (s *Store) UpsertOperatorQueueItemWithRuntimeEvent(ctx context.Context, queueInput OperatorQueueUpsertInput, runtimeInput RuntimeEventInput) (OperatorQueueRuntimeEventResult, error) {
	record := normalizeOperatorQueueUpsertInput(queueInput)
	if record.WorkspaceID == "" {
		return OperatorQueueRuntimeEventResult{}, errors.New("workspace_id is required")
	}
	if record.QueueKey == "" {
		return OperatorQueueRuntimeEventResult{}, errors.New("queue_key is required")
	}
	if record.Title == "" {
		return OperatorQueueRuntimeEventResult{}, errors.New("title is required")
	}
	existingQueueID, err := s.lookupOperatorQueueIDByKey(ctx, record.WorkspaceID, record.QueueKey)
	if err != nil {
		return OperatorQueueRuntimeEventResult{}, err
	}
	record.QueueID = firstNonEmpty(existingQueueID, record.QueueID, nextID("opq"))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return OperatorQueueRuntimeEventResult{}, err
	}
	queueRecord := OperatorQueueRecord{}
	queueEvent := RuntimeEventRecord{}
	runtimeEvent := RuntimeEventRecord{}
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		var innerErr error
		queueRecord, queueEvent, innerErr = s.upsertOperatorQueueItemWithAuthorityTx(ctx, tx, authority, record, now)
		if innerErr != nil {
			return innerErr
		}

		runtimeInput.WorkspaceID = firstNonEmpty(strings.TrimSpace(runtimeInput.WorkspaceID), queueRecord.WorkspaceID)
		if strings.TrimSpace(runtimeInput.CreatedAt) == "" {
			runtimeInput.CreatedAt = now
		}
		mergedParents, err := mergeRuntimeEventParentRefsJSON(strings.TrimSpace(runtimeInput.ParentRefsJSON), queueEvent.EventID)
		if err != nil {
			return err
		}
		runtimeInput.ParentRefsJSON = mergedParents
		runtimeEvent, innerErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, runtimeInput)
		return innerErr
	}); err != nil {
		return OperatorQueueRuntimeEventResult{}, err
	}

	hydratedQueue, err := s.getOperatorQueueItem(ctx, queueRecord.WorkspaceID, queueRecord.QueueID, "")
	if err != nil {
		return OperatorQueueRuntimeEventResult{}, err
	}
	return OperatorQueueRuntimeEventResult{
		QueueEvent:   OperatorQueueSyncEvent{Record: hydratedQueue, Event: queueEvent},
		RuntimeEvent: runtimeEvent,
	}, nil
}

func humanActionRevisionMismatchError(actionID string) error {
	return fmt.Errorf("human action was updated concurrently: %s", strings.TrimSpace(actionID))
}

func validateCurrentHumanActionSnapshot(currentAction HumanActionRecord, actionID string) error {
	actionID = strings.TrimSpace(actionID)
	if actionID == "" {
		return errors.New("action_id is required")
	}
	if strings.TrimSpace(currentAction.ActionID) != actionID {
		return fmt.Errorf("current human action snapshot does not match action_id: %s", actionID)
	}
	if currentAction.Revision <= 0 {
		return fmt.Errorf("current human action revision is required: %s", actionID)
	}
	return nil
}

func (s *Store) requirePendingHumanActionTx(ctx context.Context, tx *sql.Tx, workspaceID, actionID string, currentAction *HumanActionRecord) (HumanActionRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	actionID = strings.TrimSpace(actionID)
	if actionID == "" {
		return HumanActionRecord{}, errors.New("action_id is required")
	}

	record, err := s.getHumanActionTx(ctx, tx, actionID)
	if err != nil {
		if isHumanActionNotFoundError(err) {
			return HumanActionRecord{}, fmt.Errorf("action %s not found or already resolved", actionID)
		}
		return HumanActionRecord{}, err
	}
	if workspaceID != "" && record.WorkspaceID != workspaceID {
		return HumanActionRecord{}, fmt.Errorf("action %s not found or already resolved", actionID)
	}
	if strings.ToUpper(strings.TrimSpace(record.Status)) != model.ActionStatusPending {
		return HumanActionRecord{}, fmt.Errorf("action %s not found or already resolved", actionID)
	}
	if currentAction != nil && currentAction.Revision > 0 && record.Revision != currentAction.Revision {
		return HumanActionRecord{}, humanActionRevisionMismatchError(actionID)
	}
	return record, nil
}

func (s *Store) requireCurrentOpenActionQueueTx(ctx context.Context, tx *sql.Tx, currentActionQueue OperatorQueueRecord) error {
	currentRecord, err := s.getOperatorQueueItemTx(ctx, tx, strings.TrimSpace(currentActionQueue.WorkspaceID), strings.TrimSpace(currentActionQueue.QueueID), strings.TrimSpace(currentActionQueue.QueueKey))
	if err != nil {
		return err
	}
	if normalizeOperatorQueueStatus(currentRecord.Status) != "OPEN" {
		return operatorQueueStatusMismatchError("OPEN", currentRecord.QueueID)
	}
	if currentActionQueue.Revision > 0 && currentRecord.Revision != currentActionQueue.Revision {
		return operatorQueueRevisionMismatchError(currentRecord.QueueID)
	}
	if trimmedUpdatedAt := strings.TrimSpace(currentActionQueue.UpdatedAt); trimmedUpdatedAt != "" && strings.TrimSpace(currentRecord.UpdatedAt) != trimmedUpdatedAt {
		return operatorQueueRevisionMismatchError(currentRecord.QueueID)
	}
	return nil
}

func (s *Store) upsertOperatorQueueItemWithActionGuard(ctx context.Context, actionID string, currentAction *HumanActionRecord, currentActionQueue *OperatorQueueRecord, queueInput OperatorQueueUpsertInput, runtimeInput *RuntimeEventInput) (OperatorQueueRuntimeEventResult, error) {
	record := normalizeOperatorQueueUpsertInput(queueInput)
	if record.WorkspaceID == "" {
		return OperatorQueueRuntimeEventResult{}, errors.New("workspace_id is required")
	}
	if record.QueueKey == "" {
		return OperatorQueueRuntimeEventResult{}, errors.New("queue_key is required")
	}
	if record.Title == "" {
		return OperatorQueueRuntimeEventResult{}, errors.New("title is required")
	}
	if err := validateGenericOperatorQueueUpsertRecord(record); err != nil {
		return OperatorQueueRuntimeEventResult{}, err
	}
	existingQueueID, err := s.lookupOperatorQueueIDByKey(ctx, record.WorkspaceID, record.QueueKey)
	if err != nil {
		return OperatorQueueRuntimeEventResult{}, err
	}
	record.QueueID = firstNonEmpty(existingQueueID, record.QueueID, nextID("opq"))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, record.WorkspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return OperatorQueueRuntimeEventResult{}, err
	}

	queueRecord := OperatorQueueRecord{}
	queueEvent := RuntimeEventRecord{}
	runtimeEvent := RuntimeEventRecord{}
	if _, err := s.WithFencedWorkspaceAuthority(ctx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if _, err := s.requirePendingHumanActionTx(ctx, tx, record.WorkspaceID, actionID, currentAction); err != nil {
			return err
		}
		if currentActionQueue != nil {
			if err := s.requireCurrentOpenActionQueueTx(ctx, tx, *currentActionQueue); err != nil {
				return err
			}
		}

		var innerErr error
		queueRecord, queueEvent, innerErr = s.upsertOperatorQueueItemWithAuthorityTx(ctx, tx, authority, record, now)
		if innerErr != nil {
			return innerErr
		}
		if runtimeInput == nil {
			return nil
		}

		currentRuntimeInput := *runtimeInput
		currentRuntimeInput.WorkspaceID = firstNonEmpty(strings.TrimSpace(currentRuntimeInput.WorkspaceID), queueRecord.WorkspaceID)
		if strings.TrimSpace(currentRuntimeInput.CreatedAt) == "" {
			currentRuntimeInput.CreatedAt = now
		}
		mergedParents, err := mergeRuntimeEventParentRefsJSON(strings.TrimSpace(currentRuntimeInput.ParentRefsJSON), queueEvent.EventID)
		if err != nil {
			return err
		}
		currentRuntimeInput.ParentRefsJSON = mergedParents
		runtimeEvent, innerErr = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, currentRuntimeInput)
		return innerErr
	}); err != nil {
		return OperatorQueueRuntimeEventResult{}, err
	}

	hydratedQueue, err := s.getOperatorQueueItem(ctx, queueRecord.WorkspaceID, queueRecord.QueueID, "")
	if err != nil {
		return OperatorQueueRuntimeEventResult{}, err
	}
	return OperatorQueueRuntimeEventResult{
		QueueEvent:   OperatorQueueSyncEvent{Record: hydratedQueue, Event: queueEvent},
		RuntimeEvent: runtimeEvent,
	}, nil
}

func (s *Store) UpsertOperatorQueueItemWithEventForPendingHumanAction(ctx context.Context, actionID string, currentActionQueue *OperatorQueueRecord, queueInput OperatorQueueUpsertInput, currentAction HumanActionRecord) (OperatorQueueRecord, RuntimeEventRecord, error) {
	if err := validateCurrentHumanActionSnapshot(currentAction, actionID); err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	result, err := s.upsertOperatorQueueItemWithActionGuard(ctx, actionID, &currentAction, currentActionQueue, queueInput, nil)
	if err != nil {
		return OperatorQueueRecord{}, RuntimeEventRecord{}, err
	}
	return result.QueueEvent.Record, result.QueueEvent.Event, nil
}

func (s *Store) UpsertOperatorQueueItemWithRuntimeEventForPendingHumanAction(ctx context.Context, actionID string, currentActionQueue *OperatorQueueRecord, queueInput OperatorQueueUpsertInput, runtimeInput RuntimeEventInput, currentAction HumanActionRecord) (OperatorQueueRuntimeEventResult, error) {
	if err := validateCurrentHumanActionSnapshot(currentAction, actionID); err != nil {
		return OperatorQueueRuntimeEventResult{}, err
	}
	return s.upsertOperatorQueueItemWithActionGuard(ctx, actionID, &currentAction, currentActionQueue, queueInput, &runtimeInput)
}

// CreateHumanAction creates a new human action and blocks the task claim atomically when requested.
//
// This is a low-level legacy/test helper. Operator-facing action creation should
// use CreateHumanActionWithQueueEffects or another event-writing path so visible
// pending action state is paired with durable runtime/queue receipts.
func (s *Store) CreateHumanAction(ctx context.Context, input HumanActionInput) (string, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return "", errors.New("workspace_id is required")
	}
	taskID := strings.TrimSpace(input.TaskID)
	if taskID == "" {
		return "", errors.New("task_id is required")
	}
	title := strings.TrimSpace(input.Title)
	if title == "" {
		return "", errors.New("title is required")
	}

	actionID := nextID("action")
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return "", err
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return "", fmt.Errorf("begin human action tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
			return err
		}
		if err := s.ensureWorkspaceTaskAttachedTx(ctx, tx, workspaceID, taskID); err != nil {
			return err
		}

		if _, err := tx.ExecContext(ctx,
			`INSERT INTO human_actions(action_id, workspace_id, task_id, agent_id, assigned_to, title, description, blocking, status, created_at, revision)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, 1)`,
			actionID,
			workspaceID,
			taskID,
			strings.TrimSpace(input.AgentID),
			strings.TrimSpace(input.AssignedTo),
			title,
			strings.TrimSpace(input.Description),
			boolToInt(input.Blocking),
			now,
		); err != nil {
			return fmt.Errorf("insert human action: %w", err)
		}
		if input.Blocking {
			if err := s.blockTaskClaimTx(ctx, tx, taskID, workspaceID, input.AgentID); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
			return fmt.Errorf("touch workspace after human action create: %w", err)
		}
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return "", s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit human action create: %w", err)
	}

	return actionID, nil
}

func (s *Store) CreateHumanActionWithQueueEffects(ctx context.Context, input HumanActionInput, linkedSourceQueue *OperatorQueueRecord, linkedSourcePayload *model.RebaseFollowupPayload, actionRuntimeOverrides ...RuntimeEventInput) (HumanActionQueueCreateResult, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return HumanActionQueueCreateResult{}, errors.New("workspace_id is required")
	}
	taskID := strings.TrimSpace(input.TaskID)
	if taskID == "" {
		return HumanActionQueueCreateResult{}, errors.New("task_id is required")
	}
	title := strings.TrimSpace(input.Title)
	if title == "" {
		return HumanActionQueueCreateResult{}, errors.New("title is required")
	}

	actionID := nextID("action")
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return HumanActionQueueCreateResult{}, err
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return HumanActionQueueCreateResult{}, fmt.Errorf("begin human action tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var (
		action                 HumanActionRecord
		actionQueueEvent       *OperatorQueueSyncEvent
		linkedSourceQueueEvent *OperatorQueueSyncEvent
		actionEvent            RuntimeEventRecord
	)
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
			return err
		}
		if linkedSourceQueue != nil && linkedSourcePayload != nil {
			currentQueue, currentPayload, err := s.requireCreateableLinkedSourceQueueTx(ctx, tx, *linkedSourceQueue)
			if err != nil {
				return err
			}
			tensionTaskID, tensionAgentID := s.resolveLinkedSourceQueueTensionContextTx(ctx, tx, currentQueue.WorkspaceID, []string{currentPayload.RepairTensionID, currentPayload.ForkTensionID})
			if err := validateHumanActionInputLinkedSourceContext(
				input,
				firstNonEmpty(strings.TrimSpace(currentQueue.TaskID), tensionTaskID),
				firstNonEmpty(strings.TrimSpace(currentQueue.AgentID), tensionAgentID),
			); err != nil {
				return err
			}
			currentPayload, err = s.refreshCreateableLinkedSourceQueueLineageTx(ctx, tx, currentQueue, currentPayload)
			if err != nil {
				return err
			}
			linkedSourceQueue = &currentQueue
			linkedSourcePayload = &currentPayload
		}
		if err := s.ensureWorkspaceTaskAttachedTx(ctx, tx, workspaceID, taskID); err != nil {
			return err
		}

		if _, err := tx.ExecContext(ctx,
			`INSERT INTO human_actions(action_id, workspace_id, task_id, agent_id, assigned_to, title, description, blocking, status, created_at, revision)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, 1)`,
			actionID,
			workspaceID,
			taskID,
			strings.TrimSpace(input.AgentID),
			strings.TrimSpace(input.AssignedTo),
			title,
			strings.TrimSpace(input.Description),
			boolToInt(input.Blocking),
			now,
		); err != nil {
			return fmt.Errorf("insert human action: %w", err)
		}
		if input.Blocking {
			if err := s.blockTaskClaimTx(ctx, tx, taskID, workspaceID, input.AgentID); err != nil {
				return err
			}
		}

		if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
			return fmt.Errorf("touch workspace after human action create: %w", err)
		}

		action, err = s.getHumanActionTx(ctx, tx, actionID)
		if err != nil {
			return err
		}

		actionQueueRecord, actionQueueRuntime, err := s.upsertOperatorQueueItemWithAuthorityTx(ctx, tx, authority, normalizeOperatorQueueUpsertInput(humanActionOperatorQueueUpsertInput(action, linkedSourceQueue, linkedSourcePayload, nil)), now)
		if err != nil {
			return err
		}
		actionQueueEvent = &OperatorQueueSyncEvent{Record: actionQueueRecord, Event: actionQueueRuntime}

		linkedSourceQueueEvent = nil
		if linkedSourceQueue != nil && linkedSourcePayload != nil {
			linkRecord, linkRuntime, err := s.upsertOperatorQueueItemWithAuthorityTx(ctx, tx, authority, normalizeOperatorQueueUpsertInput(linkedActionSourceQueueCreateUpsertInput(*linkedSourceQueue, *linkedSourcePayload, action)), now)
			if err != nil {
				return err
			}
			linkedSourceQueueEvent = &OperatorQueueSyncEvent{Record: linkRecord, Event: linkRuntime}
		}

		parentRefsJSON, err := payloadParentRefsJSON(func() []string {
			if linkedSourcePayload == nil {
				return nil
			}
			return linkedSourcePayload.ParentRefsJSON
		}())
		if err != nil {
			return err
		}
		actionRuntimeInput, err := buildActionCreatedRuntimeInput(
			action,
			linkedSourceQueue,
			func() string {
				if linkedSourcePayload == nil {
					return ""
				}
				return linkedSourcePayload.RootCauseID
			}(),
			func() string {
				if linkedSourcePayload == nil {
					return ""
				}
				return linkedSourcePayload.ProvenanceGroupID
			}(),
			parentRefsJSON,
			input.PromptContextEnvelope,
			optionalRuntimeEventInput(actionRuntimeOverrides),
		)
		if err != nil {
			return err
		}
		actionRuntimeInput.ParentRefsJSON, err = mergeRuntimeEventParentRefsJSON(
			actionRuntimeInput.ParentRefsJSON,
			strings.TrimSpace(actionQueueEvent.Event.EventID),
			func() string {
				if linkedSourceQueueEvent == nil {
					return ""
				}
				return strings.TrimSpace(linkedSourceQueueEvent.Event.EventID)
			}(),
		)
		if err != nil {
			return err
		}
		actionEvent, err = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, actionRuntimeInput)
		return err
	}); err != nil {
		_ = tx.Rollback()
		return HumanActionQueueCreateResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	if err := tx.Commit(); err != nil {
		return HumanActionQueueCreateResult{}, fmt.Errorf("commit human action create tx: %w", err)
	}

	result := HumanActionQueueCreateResult{Action: action}
	hydratedActionQueue, err := s.getOperatorQueueItem(ctx, workspaceID, actionQueueEvent.Record.QueueID, "")
	if err != nil {
		return HumanActionQueueCreateResult{}, err
	}
	actionQueueEvent.Record = hydratedActionQueue
	result.ActionQueue = actionQueueEvent
	result.ActionEvent = &actionEvent
	if linkedSourceQueueEvent != nil {
		hydrated, err := s.getOperatorQueueItem(ctx, workspaceID, linkedSourceQueueEvent.Record.QueueID, "")
		if err != nil {
			return HumanActionQueueCreateResult{}, err
		}
		linkedSourceQueueEvent.Record = hydrated
		result.LinkedSourceQueue = linkedSourceQueueEvent
	}
	return result, nil
}

func (s *Store) CreateHumanActionWithRollbackFailureQueueEffects(ctx context.Context, input HumanActionInput, linkedSourceQueue *OperatorQueueRecord, linkedSourcePayload *model.RebaseRollbackFailurePayload, actionRuntimeOverrides ...RuntimeEventInput) (HumanActionQueueCreateResult, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	if workspaceID == "" {
		return HumanActionQueueCreateResult{}, errors.New("workspace_id is required")
	}
	taskID := strings.TrimSpace(input.TaskID)
	if taskID == "" {
		return HumanActionQueueCreateResult{}, errors.New("task_id is required")
	}
	title := strings.TrimSpace(input.Title)
	if title == "" {
		return HumanActionQueueCreateResult{}, errors.New("title is required")
	}

	actionID := nextID("action")
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return HumanActionQueueCreateResult{}, err
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return HumanActionQueueCreateResult{}, fmt.Errorf("begin human action tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var (
		action                 HumanActionRecord
		actionQueueEvent       *OperatorQueueSyncEvent
		linkedSourceQueueEvent *OperatorQueueSyncEvent
		actionEvent            RuntimeEventRecord
	)
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
			return err
		}
		if linkedSourceQueue != nil && linkedSourcePayload != nil {
			currentQueue, currentPayload, err := s.requireCreateableRollbackFailureSourceQueueTx(ctx, tx, *linkedSourceQueue)
			if err != nil {
				return err
			}
			tensionTaskID, tensionAgentID := s.resolveLinkedSourceQueueTensionContextTx(ctx, tx, currentQueue.WorkspaceID, []string{currentPayload.RepairTensionID})
			if err := validateHumanActionInputLinkedSourceContext(
				input,
				firstNonEmpty(strings.TrimSpace(currentQueue.TaskID), strings.TrimSpace(currentPayload.TaskID), tensionTaskID),
				firstNonEmpty(strings.TrimSpace(currentQueue.AgentID), strings.TrimSpace(currentPayload.AgentID), tensionAgentID),
			); err != nil {
				return err
			}
			currentPayload, err = s.refreshCreateableRollbackFailureSourceQueueLineageTx(ctx, tx, currentQueue, currentPayload)
			if err != nil {
				return err
			}
			linkedSourceQueue = &currentQueue
			linkedSourcePayload = &currentPayload
		}
		if err := s.ensureWorkspaceTaskAttachedTx(ctx, tx, workspaceID, taskID); err != nil {
			return err
		}

		if _, err := tx.ExecContext(ctx,
			`INSERT INTO human_actions(action_id, workspace_id, task_id, agent_id, assigned_to, title, description, blocking, status, created_at, revision)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, 1)`,
			actionID,
			workspaceID,
			taskID,
			strings.TrimSpace(input.AgentID),
			strings.TrimSpace(input.AssignedTo),
			title,
			strings.TrimSpace(input.Description),
			boolToInt(input.Blocking),
			now,
		); err != nil {
			return fmt.Errorf("insert human action: %w", err)
		}
		if input.Blocking {
			if err := s.blockTaskClaimTx(ctx, tx, taskID, workspaceID, input.AgentID); err != nil {
				return err
			}
		}

		if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
			return fmt.Errorf("touch workspace after human action create: %w", err)
		}

		action, err = s.getHumanActionTx(ctx, tx, actionID)
		if err != nil {
			return err
		}

		actionQueueRecord, actionQueueRuntime, err := s.upsertOperatorQueueItemWithAuthorityTx(ctx, tx, authority, normalizeOperatorQueueUpsertInput(humanActionOperatorQueueUpsertInput(action, linkedSourceQueue, nil, linkedSourcePayload)), now)
		if err != nil {
			return err
		}
		actionQueueEvent = &OperatorQueueSyncEvent{Record: actionQueueRecord, Event: actionQueueRuntime}

		linkedSourceQueueEvent = nil
		if linkedSourceQueue != nil && linkedSourcePayload != nil {
			linkRecord, linkRuntime, err := s.upsertOperatorQueueItemWithAuthorityTx(ctx, tx, authority, normalizeOperatorQueueUpsertInput(linkedRollbackFailureSourceQueueCreateUpsertInput(*linkedSourceQueue, *linkedSourcePayload, action)), now)
			if err != nil {
				return err
			}
			linkedSourceQueueEvent = &OperatorQueueSyncEvent{Record: linkRecord, Event: linkRuntime}
		}

		parentRefsJSON, err := payloadParentRefsJSON(func() []string {
			if linkedSourcePayload == nil {
				return nil
			}
			return linkedSourcePayload.ParentRefsJSON
		}())
		if err != nil {
			return err
		}
		actionRuntimeInput, err := buildActionCreatedRuntimeInput(
			action,
			linkedSourceQueue,
			func() string {
				if linkedSourcePayload == nil {
					return ""
				}
				return linkedSourcePayload.RootCauseID
			}(),
			func() string {
				if linkedSourcePayload == nil {
					return ""
				}
				return linkedSourcePayload.ProvenanceGroupID
			}(),
			parentRefsJSON,
			input.PromptContextEnvelope,
			optionalRuntimeEventInput(actionRuntimeOverrides),
		)
		if err != nil {
			return err
		}
		actionRuntimeInput.ParentRefsJSON, err = mergeRuntimeEventParentRefsJSON(
			actionRuntimeInput.ParentRefsJSON,
			strings.TrimSpace(actionQueueEvent.Event.EventID),
			func() string {
				if linkedSourceQueueEvent == nil {
					return ""
				}
				return strings.TrimSpace(linkedSourceQueueEvent.Event.EventID)
			}(),
		)
		if err != nil {
			return err
		}
		actionEvent, err = s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, actionRuntimeInput)
		return err
	}); err != nil {
		_ = tx.Rollback()
		return HumanActionQueueCreateResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	if err := tx.Commit(); err != nil {
		return HumanActionQueueCreateResult{}, fmt.Errorf("commit human action create tx: %w", err)
	}

	result := HumanActionQueueCreateResult{Action: action}
	hydratedActionQueue, err := s.getOperatorQueueItem(ctx, workspaceID, actionQueueEvent.Record.QueueID, "")
	if err != nil {
		return HumanActionQueueCreateResult{}, err
	}
	actionQueueEvent.Record = hydratedActionQueue
	result.ActionQueue = actionQueueEvent
	result.ActionEvent = &actionEvent
	if linkedSourceQueueEvent != nil {
		hydrated, err := s.getOperatorQueueItem(ctx, workspaceID, linkedSourceQueueEvent.Record.QueueID, "")
		if err != nil {
			return HumanActionQueueCreateResult{}, err
		}
		linkedSourceQueueEvent.Record = hydrated
		result.LinkedSourceQueue = linkedSourceQueueEvent
	}
	return result, nil
}

func (s *Store) requireCreateableLinkedSourceQueueTx(ctx context.Context, tx *sql.Tx, queue OperatorQueueRecord) (OperatorQueueRecord, model.RebaseFollowupPayload, error) {
	currentQueue, err := s.getOperatorQueueItemTx(ctx, tx, strings.TrimSpace(queue.WorkspaceID), strings.TrimSpace(queue.QueueID), strings.TrimSpace(queue.QueueKey))
	if err != nil {
		return OperatorQueueRecord{}, model.RebaseFollowupPayload{}, err
	}
	if strings.TrimSpace(currentQueue.AssignedTo) != strings.TrimSpace(queue.AssignedTo) {
		queueRef := firstNonEmpty(strings.TrimSpace(currentQueue.QueueID), strings.TrimSpace(currentQueue.QueueKey), strings.TrimSpace(queue.QueueID), strings.TrimSpace(queue.QueueKey))
		return OperatorQueueRecord{}, model.RebaseFollowupPayload{}, operatorQueueRevisionMismatchError(queueRef)
	}
	if normalizeOperatorQueueStatus(currentQueue.Status) != "OPEN" {
		return OperatorQueueRecord{}, model.RebaseFollowupPayload{}, operatorQueueStatusMismatchError("OPEN", currentQueue.QueueID)
	}
	payload := model.RebaseFollowupPayload{}
	if strings.TrimSpace(currentQueue.PayloadJSON) != "" {
		if err := json.Unmarshal([]byte(currentQueue.PayloadJSON), &payload); err != nil {
			return OperatorQueueRecord{}, model.RebaseFollowupPayload{}, fmt.Errorf("decode linked source queue payload: %w", err)
		}
	}
	payload.Normalize()
	if !payload.IsRebaseFollowup(currentQueue.QueueKey) {
		return OperatorQueueRecord{}, model.RebaseFollowupPayload{}, errors.New("linked source queue is not a rebase follow-up")
	}
	if payload.LinkedActionExists() {
		return OperatorQueueRecord{}, model.RebaseFollowupPayload{}, fmt.Errorf("linked source queue already linked to action %s", payload.ActionID)
	}
	if err := s.requireRebaseFollowupStewardLeaseTx(ctx, tx, currentQueue, payload); err != nil {
		return OperatorQueueRecord{}, model.RebaseFollowupPayload{}, err
	}
	return currentQueue, payload, nil
}

func validateHumanActionInputLinkedSourceContext(input HumanActionInput, authoritativeTaskID, authoritativeAgentID string) error {
	requestedTaskID := strings.TrimSpace(input.TaskID)
	requestedAgentID := strings.TrimSpace(input.AgentID)
	authoritativeTaskID = strings.TrimSpace(authoritativeTaskID)
	authoritativeAgentID = strings.TrimSpace(authoritativeAgentID)
	if requestedTaskID != "" && authoritativeTaskID != "" && requestedTaskID != authoritativeTaskID {
		return fmt.Errorf("source queue belongs to task %s", authoritativeTaskID)
	}
	if requestedAgentID != "" && authoritativeAgentID != "" && requestedAgentID != authoritativeAgentID {
		return fmt.Errorf("source queue belongs to agent %s", authoritativeAgentID)
	}
	return nil
}

func (s *Store) resolveLinkedSourceQueueTensionContextTx(ctx context.Context, tx *sql.Tx, workspaceID string, tensionIDs []string) (string, string) {
	var taskID string
	var agentID string
	for _, tensionID := range tensionIDs {
		tensionID = strings.TrimSpace(tensionID)
		if tensionID == "" {
			continue
		}
		record, err := s.loadTensionRecord(ctx, tx, strings.TrimSpace(workspaceID), tensionID)
		if err != nil {
			continue
		}
		if taskID == "" && len(record.TaskIDs) > 0 {
			taskID = strings.TrimSpace(record.TaskIDs[0])
		}
		if agentID == "" && len(record.AgentIDs) > 0 {
			agentID = strings.TrimSpace(record.AgentIDs[0])
		}
		if taskID != "" && agentID != "" {
			return taskID, agentID
		}
	}
	return taskID, agentID
}

func rebaseFollowupQueueNeedsStewardLease(queue OperatorQueueRecord, payload model.RebaseFollowupPayload) bool {
	return payload.StewardLeaseRequired && payload.IsRebaseFollowup(queue.QueueKey)
}

func rebaseFollowupProtoClusterID(workspaceID string, queue OperatorQueueRecord, payload model.RebaseFollowupPayload) string {
	workspaceID = strings.TrimSpace(workspaceID)
	taskID := firstNonEmpty(
		strings.TrimSpace(queue.TaskID),
		strings.TrimSpace(payload.TaskID),
		taskIDFromTaskIDs(payload.TaskIDs),
	)
	if workspaceID == "" || taskID == "" {
		return ""
	}
	return "task:" + workspaceID + "/" + taskID
}

func (s *Store) requireRebaseFollowupStewardLeaseTx(ctx context.Context, tx *sql.Tx, queue OperatorQueueRecord, payload model.RebaseFollowupPayload) error {
	if !rebaseFollowupQueueNeedsStewardLease(queue, payload) {
		return nil
	}

	clusterID := rebaseFollowupProtoClusterID(strings.TrimSpace(queue.WorkspaceID), queue, payload)
	if clusterID == "" {
		return fmt.Errorf("%w: missing proto-cluster context", ErrRebaseFollowupStewardRequired)
	}

	steward, err := s.getActiveStewardTx(ctx, tx, clusterID)
	if err != nil {
		if errors.Is(err, ErrStewardNotFound) {
			return fmt.Errorf("%w for %s", ErrRebaseFollowupStewardRequired, clusterID)
		}
		return err
	}

	queueAgentID := strings.TrimSpace(queue.AgentID)
	if queueAgentID == "" {
		return fmt.Errorf("%w: cluster %s missing queue agent authority", ErrRebaseFollowupStewardRequired, clusterID)
	}
	if !strings.EqualFold(strings.TrimSpace(steward.StewardAgentID), queueAgentID) {
		return fmt.Errorf(
			"%w: cluster %s steward=%s queue_agent=%s",
			ErrRebaseFollowupStewardMismatch,
			clusterID,
			strings.TrimSpace(steward.StewardAgentID),
			queueAgentID,
		)
	}
	return nil
}

func (s *Store) requireCreateableRollbackFailureSourceQueueTx(ctx context.Context, tx *sql.Tx, queue OperatorQueueRecord) (OperatorQueueRecord, model.RebaseRollbackFailurePayload, error) {
	currentQueue, err := s.getOperatorQueueItemTx(ctx, tx, strings.TrimSpace(queue.WorkspaceID), strings.TrimSpace(queue.QueueID), strings.TrimSpace(queue.QueueKey))
	if err != nil {
		return OperatorQueueRecord{}, model.RebaseRollbackFailurePayload{}, err
	}
	if strings.TrimSpace(currentQueue.AssignedTo) != strings.TrimSpace(queue.AssignedTo) {
		queueRef := firstNonEmpty(strings.TrimSpace(currentQueue.QueueID), strings.TrimSpace(currentQueue.QueueKey), strings.TrimSpace(queue.QueueID), strings.TrimSpace(queue.QueueKey))
		return OperatorQueueRecord{}, model.RebaseRollbackFailurePayload{}, operatorQueueRevisionMismatchError(queueRef)
	}
	if normalizeOperatorQueueStatus(currentQueue.Status) != "OPEN" {
		return OperatorQueueRecord{}, model.RebaseRollbackFailurePayload{}, operatorQueueStatusMismatchError("OPEN", currentQueue.QueueID)
	}
	payload := model.RebaseRollbackFailurePayload{}
	if strings.TrimSpace(currentQueue.PayloadJSON) != "" {
		if err := json.Unmarshal([]byte(currentQueue.PayloadJSON), &payload); err != nil {
			return OperatorQueueRecord{}, model.RebaseRollbackFailurePayload{}, fmt.Errorf("decode linked rollback-failure queue payload: %w", err)
		}
	}
	payload.Normalize()
	if !payload.IsRollbackFailure(currentQueue.QueueKey) {
		return OperatorQueueRecord{}, model.RebaseRollbackFailurePayload{}, errors.New("linked source queue is not a rollback-failure follow-up")
	}
	if payload.FollowupActionID != "" {
		return OperatorQueueRecord{}, model.RebaseRollbackFailurePayload{}, fmt.Errorf("linked source queue already linked to action %s", payload.FollowupActionID)
	}
	return currentQueue, payload, nil
}

func (s *Store) latestRuntimeEventTx(ctx context.Context, tx *sql.Tx, filter RuntimeEventFilter) (RuntimeEventRecord, bool, error) {
	workspaceID := strings.TrimSpace(filter.WorkspaceID)
	if workspaceID == "" {
		return RuntimeEventRecord{}, false, errors.New("workspace_id is required")
	}

	query := strings.Builder{}
	query.WriteString(`SELECT `)
	query.WriteString(runtimeEventSelectColumns)
	query.WriteString(` FROM runtime_events WHERE workspace_id = ?`)
	args := []any{workspaceID}
	if trimmed := strings.TrimSpace(filter.EventType); trimmed != "" {
		query.WriteString(` AND event_type = ?`)
		args = append(args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.EntityType); trimmed != "" {
		query.WriteString(` AND entity_type = ?`)
		args = append(args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.EntityID); trimmed != "" {
		query.WriteString(` AND entity_id = ?`)
		args = append(args, trimmed)
	}
	query.WriteString(` ORDER BY ingest_seq DESC, event_id DESC LIMIT 1`)

	row := tx.QueryRowContext(ctx, query.String(), args...)
	var record RuntimeEventRecord
	if err := scanRuntimeEvent(row, &record); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RuntimeEventRecord{}, false, nil
		}
		return RuntimeEventRecord{}, false, fmt.Errorf("query latest runtime event: %w", err)
	}
	return record, true, nil
}

func refreshActionCreateLineageFallback(currentValue, eventValue, eventID string) string {
	return firstNonEmpty(strings.TrimSpace(currentValue), strings.TrimSpace(eventValue), strings.TrimSpace(eventID))
}

func actionLineageValueIsSyntheticEventFallback(value string, parentRefs []string) bool {
	value = strings.TrimSpace(value)
	if value == "" || len(parentRefs) != 1 {
		return false
	}
	return value == strings.TrimSpace(parentRefs[0])
}

func normalizeActionCreateLineageParentRefs(eventIDs ...string) ([]string, error) {
	return parseRuntimeEventParentRefs(mustJSON(eventIDs))
}

func (s *Store) refreshCreateableLinkedSourceQueueLineageTx(ctx context.Context, tx *sql.Tx, queue OperatorQueueRecord, payload model.RebaseFollowupPayload) (model.RebaseFollowupPayload, error) {
	payload.Normalize()
	if strings.TrimSpace(payload.LastFailedActionID) == "" &&
		strings.TrimSpace(payload.RollbackReason) == "" &&
		strings.TrimSpace(payload.RebaseWorkflowStep) != model.RebaseWorkflowStepAwaitRestart {
		return payload, nil
	}

	parentRefs := make([]string, 0, 2)
	queueEvent, ok, err := s.latestRuntimeEventTx(ctx, tx, RuntimeEventFilter{
		WorkspaceID: strings.TrimSpace(queue.WorkspaceID),
		EntityType:  "operator_queue",
		EntityID:    strings.TrimSpace(queue.QueueID),
		Limit:       1,
	})
	if err != nil {
		return model.RebaseFollowupPayload{}, err
	}
	if ok {
		payload.RootCauseID = refreshActionCreateLineageFallback(payload.RootCauseID, queueEvent.RootCauseID, queueEvent.EventID)
		payload.ProvenanceGroupID = refreshActionCreateLineageFallback(payload.ProvenanceGroupID, queueEvent.ProvenanceGroupID, queueEvent.EventID)
		parentRefs = append(parentRefs, strings.TrimSpace(queueEvent.EventID))
	}
	if failedActionID := strings.TrimSpace(payload.LastFailedActionID); failedActionID != "" {
		failedEvent, ok, err := s.latestRuntimeEventTx(ctx, tx, RuntimeEventFilter{
			WorkspaceID: strings.TrimSpace(queue.WorkspaceID),
			EventType:   "action.resolved",
			EntityType:  "human_action",
			EntityID:    failedActionID,
			Limit:       1,
		})
		if err != nil {
			return model.RebaseFollowupPayload{}, err
		}
		if ok {
			payload.RootCauseID = refreshActionCreateLineageFallback(payload.RootCauseID, failedEvent.RootCauseID, failedEvent.EventID)
			payload.ProvenanceGroupID = refreshActionCreateLineageFallback(payload.ProvenanceGroupID, failedEvent.ProvenanceGroupID, failedEvent.EventID)
			parentRefs = append(parentRefs, strings.TrimSpace(failedEvent.EventID))
		}
	}
	normalizedParents, err := normalizeActionCreateLineageParentRefs(parentRefs...)
	if err != nil {
		return model.RebaseFollowupPayload{}, err
	}
	if len(normalizedParents) > 0 {
		payload.ParentRefsJSON = normalizedParents
	}
	payload.Normalize()
	return payload, nil
}

func (s *Store) refreshCreateableRollbackFailureSourceQueueLineageTx(ctx context.Context, tx *sql.Tx, queue OperatorQueueRecord, payload model.RebaseRollbackFailurePayload) (model.RebaseRollbackFailurePayload, error) {
	payload.Normalize()
	if strings.TrimSpace(payload.LastFailedFollowupActionID) == "" {
		return payload, nil
	}

	parentRefs := make([]string, 0, 2)
	queueEvent, ok, err := s.latestRuntimeEventTx(ctx, tx, RuntimeEventFilter{
		WorkspaceID: strings.TrimSpace(queue.WorkspaceID),
		EntityType:  "operator_queue",
		EntityID:    strings.TrimSpace(queue.QueueID),
		Limit:       1,
	})
	if err != nil {
		return model.RebaseRollbackFailurePayload{}, err
	}
	if ok {
		payload.RootCauseID = refreshActionCreateLineageFallback(payload.RootCauseID, queueEvent.RootCauseID, queueEvent.EventID)
		payload.ProvenanceGroupID = refreshActionCreateLineageFallback(payload.ProvenanceGroupID, queueEvent.ProvenanceGroupID, queueEvent.EventID)
		parentRefs = append(parentRefs, strings.TrimSpace(queueEvent.EventID))
	}
	failedEvent, ok, err := s.latestRuntimeEventTx(ctx, tx, RuntimeEventFilter{
		WorkspaceID: strings.TrimSpace(queue.WorkspaceID),
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    strings.TrimSpace(payload.LastFailedFollowupActionID),
		Limit:       1,
	})
	if err != nil {
		return model.RebaseRollbackFailurePayload{}, err
	}
	if ok {
		payload.RootCauseID = refreshActionCreateLineageFallback(payload.RootCauseID, failedEvent.RootCauseID, failedEvent.EventID)
		payload.ProvenanceGroupID = refreshActionCreateLineageFallback(payload.ProvenanceGroupID, failedEvent.ProvenanceGroupID, failedEvent.EventID)
		parentRefs = append(parentRefs, strings.TrimSpace(failedEvent.EventID))
	}
	normalizedParents, err := normalizeActionCreateLineageParentRefs(parentRefs...)
	if err != nil {
		return model.RebaseRollbackFailurePayload{}, err
	}
	if len(normalizedParents) > 0 {
		payload.ParentRefsJSON = normalizedParents
	}
	payload.Normalize()
	return payload, nil
}

func (s *Store) refreshActionResolveQueueLineageBasisTx(ctx context.Context, tx *sql.Tx, queue OperatorQueueRecord) (string, string, []string, error) {
	queueLineage, _ := operatorQueuePayloadLineage(queue.PayloadJSON)
	rootCauseID := strings.TrimSpace(queueLineage.RootCauseID)
	provenanceGroupID := strings.TrimSpace(queueLineage.ProvenanceGroupID)
	parentRefs := append([]string(nil), queueLineage.ParentRefsJSON...)

	queueEvent, ok, err := s.latestRuntimeEventTx(ctx, tx, RuntimeEventFilter{
		WorkspaceID: strings.TrimSpace(queue.WorkspaceID),
		EntityType:  "operator_queue",
		EntityID:    strings.TrimSpace(queue.QueueID),
		Limit:       1,
	})
	if err != nil {
		return "", "", nil, err
	}
	if ok {
		rootCauseID = refreshActionCreateLineageFallback(rootCauseID, queueEvent.RootCauseID, queueEvent.EventID)
		provenanceGroupID = refreshActionCreateLineageFallback(provenanceGroupID, queueEvent.ProvenanceGroupID, queueEvent.EventID)
		parentRefs = []string{strings.TrimSpace(queueEvent.EventID)}
	}
	normalizedParents, err := normalizeActionCreateLineageParentRefs(parentRefs...)
	if err != nil {
		return "", "", nil, err
	}
	return rootCauseID, provenanceGroupID, normalizedParents, nil
}

func mergeFailedResolveLinkedSourceQueueLineage(preferredRootCauseID, preferredProvenanceGroupID string, preferredParentRefs []string, currentRootCauseID, currentProvenanceGroupID string, currentParentRefs []string) ([]string, string, string, error) {
	preferredRootCauseID = strings.TrimSpace(preferredRootCauseID)
	preferredProvenanceGroupID = strings.TrimSpace(preferredProvenanceGroupID)
	normalizedPreferredParents, err := normalizeActionCreateLineageParentRefs(preferredParentRefs...)
	if err != nil {
		return nil, "", "", err
	}
	currentRootCauseID = strings.TrimSpace(currentRootCauseID)
	currentProvenanceGroupID = strings.TrimSpace(currentProvenanceGroupID)
	normalizedCurrentParents, err := normalizeActionCreateLineageParentRefs(currentParentRefs...)
	if err != nil {
		return nil, "", "", err
	}

	currentRootAuthoritative := currentRootCauseID != "" && !actionLineageValueIsSyntheticEventFallback(currentRootCauseID, normalizedCurrentParents)
	currentProvenanceAuthoritative := currentProvenanceGroupID != "" && !actionLineageValueIsSyntheticEventFallback(currentProvenanceGroupID, normalizedCurrentParents)
	if (preferredRootCauseID != "" && currentRootAuthoritative && preferredRootCauseID != currentRootCauseID) ||
		(preferredProvenanceGroupID != "" && currentProvenanceAuthoritative && preferredProvenanceGroupID != currentProvenanceGroupID) {
		return append([]string(nil), normalizedCurrentParents...), currentRootCauseID, currentProvenanceGroupID, nil
	}

	rootCauseID := currentRootCauseID
	if !currentRootAuthoritative {
		rootCauseID = preferredRootCauseID
		if rootCauseID == "" {
			rootCauseID = currentRootCauseID
		}
	}
	provenanceGroupID := currentProvenanceGroupID
	if !currentProvenanceAuthoritative {
		provenanceGroupID = preferredProvenanceGroupID
		if provenanceGroupID == "" {
			provenanceGroupID = currentProvenanceGroupID
		}
	}
	parentRefs := append([]string(nil), normalizedCurrentParents...)
	if len(parentRefs) == 0 && len(normalizedPreferredParents) > 0 && !currentRootAuthoritative && !currentProvenanceAuthoritative {
		parentRefs = append([]string(nil), normalizedPreferredParents...)
	}
	return parentRefs, rootCauseID, provenanceGroupID, nil
}

func requireFailedResolveLinkedSourceQueueLink(currentQueue OperatorQueueRecord, actionID string) error {
	actionID = strings.TrimSpace(actionID)
	queueRef := firstNonEmpty(strings.TrimSpace(currentQueue.QueueID), strings.TrimSpace(currentQueue.QueueKey))
	if queueRef == "" {
		queueRef = "operator_queue"
	}
	if actionID == "" {
		return fmt.Errorf("failed linked source queue %s requires action context", queueRef)
	}
	if payload, ok := decodeLinkedRollbackFailurePayload(currentQueue.PayloadJSON, currentQueue.QueueKey); ok {
		if strings.TrimSpace(payload.FollowupActionID) != actionID {
			return fmt.Errorf("linked source queue %s is not linked to action %s", queueRef, actionID)
		}
		return nil
	}
	if payload, ok := decodeLinkedRebaseFollowupPayload(currentQueue.PayloadJSON, currentQueue.QueueKey); ok {
		if strings.TrimSpace(payload.ActionID) != actionID {
			return fmt.Errorf("linked source queue %s is not linked to action %s", queueRef, actionID)
		}
		return nil
	}
	return fmt.Errorf("linked source queue %s is not a linked rollback queue for action %s", queueRef, actionID)
}

func requireFailedResolveLinkedSourceQueueCanonicalPayload(queueKey, payloadJSON, actionID string) error {
	actionID = strings.TrimSpace(actionID)
	queueRef := firstNonEmpty(strings.TrimSpace(queueKey), "linked_source_queue")
	if payload, ok := decodeLinkedRollbackFailurePayload(payloadJSON, queueKey); ok {
		if strings.TrimSpace(payload.FollowupActionID) != "" {
			return fmt.Errorf("linked source queue %s failed rollback payload must clear active followup action", queueRef)
		}
		if strings.TrimSpace(payload.LastFailedFollowupActionID) != actionID {
			return fmt.Errorf("linked source queue %s failed rollback payload must record failed followup action %s", queueRef, actionID)
		}
		return nil
	}
	if payload, ok := decodeLinkedRebaseFollowupPayload(payloadJSON, queueKey); ok {
		if strings.TrimSpace(payload.ActionID) != "" {
			return fmt.Errorf("linked source queue %s failed rollback payload must clear active action", queueRef)
		}
		if strings.TrimSpace(payload.LastFailedActionID) != actionID {
			return fmt.Errorf("linked source queue %s failed rollback payload must record failed action %s", queueRef, actionID)
		}
		if strings.TrimSpace(payload.RollbackReason) == "" {
			return fmt.Errorf("linked source queue %s failed rollback payload must include rollback reason", queueRef)
		}
		if strings.TrimSpace(payload.RebaseWorkflowState) != model.RebaseWorkflowStateClaimed {
			return fmt.Errorf("linked source queue %s failed rollback payload must move workflow_state to %s", queueRef, model.RebaseWorkflowStateClaimed)
		}
		if strings.TrimSpace(payload.RebaseWorkflowStep) != model.RebaseWorkflowStepAwaitRestart {
			return fmt.Errorf("linked source queue %s failed rollback payload must move workflow_step to %s", queueRef, model.RebaseWorkflowStepAwaitRestart)
		}
		return nil
	}
	return fmt.Errorf("linked source queue %s failed rollback payload is not a supported linked queue payload", queueRef)
}

func canonicalFailedResolveRebaseFollowupUpsert(queue OperatorQueueRecord, payload model.RebaseFollowupPayload, action HumanActionRecord, resolvedBy, resolution, comment string, requiredRevision int64, requiredUpdatedAt, preferredRollbackReason, preferredRootCauseID, preferredProvenanceGroupID string, preferredParentRefs []string) OperatorQueueUpsertInput {
	reason := firstNonEmpty(strings.TrimSpace(preferredRollbackReason), linkedActionFailedResolveRollbackReason(resolution))
	payload.LastFailedActionID = strings.TrimSpace(action.ActionID)
	payload.LastFailedActionKey = "action:" + strings.TrimSpace(action.ActionID)
	payload.LastFailedStatus = strings.TrimSpace(action.Status)
	payload.RollbackReason = reason
	payload.ActionID = ""
	payload.ActionQueueKey = ""
	payload.ActionStatus = ""
	payload.ActionTitle = ""
	payload.ActionAssignedTo = ""
	payload.ActionBlocking = false
	payload.ActionStartedBy = ""
	payload.ActionStartedComment = ""
	payload.ActionPausedBy = ""
	payload.ActionPauseComment = ""
	payload.RebaseWorkflowState = model.RebaseWorkflowStateClaimed
	payload.RebaseWorkflowStep = model.RebaseWorkflowStepAwaitRestart
	payload.RootCauseID = strings.TrimSpace(preferredRootCauseID)
	payload.ProvenanceGroupID = strings.TrimSpace(preferredProvenanceGroupID)
	payload.ParentRefsJSON = append([]string(nil), preferredParentRefs...)
	payload.Normalize()

	details := strings.TrimSpace(queue.Details)
	details = actionQueueRemoveDetailLine(details, "Linked action:")
	details = actionQueueRemoveDetailLine(details, "Action queue:")
	details = actionQueueRemoveDetailLine(details, "Started by:")
	details = actionQueueRemoveDetailLine(details, "Start comment:")
	details = actionQueueRemoveDetailLine(details, "Paused by:")
	details = actionQueueRemoveDetailLine(details, "Pause comment:")
	details = actionQueueUpsertDetailLine(details, "Last failed action:", strings.TrimSpace(action.ActionID))
	details = actionQueueUpsertDetailLine(details, "Rollback reason:", reason)
	details = actionQueueUpsertDetailLine(details, "Workflow state:", model.RebaseWorkflowStateClaimed)
	details = actionQueueUpsertDetailLine(details, "Workflow step:", model.RebaseWorkflowStepAwaitRestart)
	if trimmed := strings.TrimSpace(resolvedBy); trimmed != "" {
		details = actionQueueUpsertDetailLine(details, "Failed by:", trimmed)
	}
	if trimmed := strings.TrimSpace(comment); trimmed != "" {
		details = actionQueueUpsertDetailLine(details, "Failure comment:", trimmed)
	}

	summary := actionQueueSummaryRemoveActionLink(strings.TrimSpace(queue.Summary))
	summary = actionQueueSummaryWithWorkflowMarker(summary, "Claimed")

	return OperatorQueueUpsertInput{
		QueueID:                 strings.TrimSpace(queue.QueueID),
		WorkspaceID:             strings.TrimSpace(queue.WorkspaceID),
		QueueKey:                strings.TrimSpace(queue.QueueKey),
		QueueType:               strings.TrimSpace(queue.QueueType),
		Title:                   strings.TrimSpace(queue.Title),
		Summary:                 strings.TrimSpace(summary),
		Details:                 strings.TrimSpace(details),
		PayloadJSON:             marshalActionQueuePayload(payload),
		AssignedTo:              firstNonEmpty(strings.TrimSpace(queue.AssignedTo), strings.TrimSpace(action.AssignedTo)),
		Urgency:                 strings.TrimSpace(queue.Urgency),
		SourceKind:              strings.TrimSpace(queue.SourceKind),
		SourceID:                strings.TrimSpace(queue.SourceID),
		TaskID:                  firstNonEmpty(strings.TrimSpace(queue.TaskID), strings.TrimSpace(action.TaskID)),
		SessionID:               strings.TrimSpace(queue.SessionID),
		AgentID:                 firstNonEmpty(strings.TrimSpace(queue.AgentID), strings.TrimSpace(action.AgentID)),
		KeepSessionActive:       queue.KeepSessionActive,
		DueAt:                   derefString(queue.DueAt),
		RequireCurrentStatus:    "OPEN",
		RequireCurrentRevision:  requiredRevision,
		RequireCurrentUpdatedAt: strings.TrimSpace(requiredUpdatedAt),
	}
}

func canonicalFailedResolveRollbackFailureUpsert(queue OperatorQueueRecord, payload model.RebaseRollbackFailurePayload, action HumanActionRecord, resolvedBy, comment string, requiredRevision int64, requiredUpdatedAt, preferredRootCauseID, preferredProvenanceGroupID string, preferredParentRefs []string) OperatorQueueUpsertInput {
	payload.LastFailedFollowupActionID = strings.TrimSpace(action.ActionID)
	payload.LastFailedFollowupActionStatus = strings.TrimSpace(action.Status)
	payload.FollowupActionID = ""
	payload.FollowupActionQueueKey = ""
	payload.FollowupActionStatus = ""
	payload.RootCauseID = strings.TrimSpace(preferredRootCauseID)
	payload.ProvenanceGroupID = strings.TrimSpace(preferredProvenanceGroupID)
	payload.ParentRefsJSON = append([]string(nil), preferredParentRefs...)
	payload.Normalize()

	details := strings.TrimSpace(queue.Details)
	details = actionQueueRemoveDetailLine(details, "Follow-up action:")
	details = actionQueueRemoveDetailLine(details, "Follow-up action queue:")
	details = actionQueueRemoveDetailLine(details, "Follow-up action status:")
	details = actionQueueUpsertDetailLine(details, "Last failed follow-up action:", strings.TrimSpace(action.ActionID))
	if trimmed := strings.TrimSpace(resolvedBy); trimmed != "" {
		details = actionQueueUpsertDetailLine(details, "Failed by:", trimmed)
	}
	if trimmed := strings.TrimSpace(comment); trimmed != "" {
		details = actionQueueUpsertDetailLine(details, "Failure comment:", trimmed)
	}

	summary := actionQueueSummaryRemoveActionLink(strings.TrimSpace(queue.Summary))
	summary = actionQueueSummaryWithWorkflowMarker(summary, "Claimed")

	return OperatorQueueUpsertInput{
		QueueID:                 strings.TrimSpace(queue.QueueID),
		WorkspaceID:             strings.TrimSpace(queue.WorkspaceID),
		QueueKey:                strings.TrimSpace(queue.QueueKey),
		QueueType:               strings.TrimSpace(queue.QueueType),
		Title:                   strings.TrimSpace(queue.Title),
		Summary:                 strings.TrimSpace(summary),
		Details:                 strings.TrimSpace(details),
		PayloadJSON:             marshalRollbackFailurePayload(payload),
		AssignedTo:              firstNonEmpty(strings.TrimSpace(queue.AssignedTo), strings.TrimSpace(action.AssignedTo)),
		Urgency:                 strings.TrimSpace(queue.Urgency),
		SourceKind:              strings.TrimSpace(queue.SourceKind),
		SourceID:                strings.TrimSpace(queue.SourceID),
		TaskID:                  firstNonEmpty(strings.TrimSpace(queue.TaskID), strings.TrimSpace(action.TaskID)),
		SessionID:               strings.TrimSpace(queue.SessionID),
		AgentID:                 firstNonEmpty(strings.TrimSpace(queue.AgentID), strings.TrimSpace(action.AgentID)),
		KeepSessionActive:       queue.KeepSessionActive,
		DueAt:                   derefString(queue.DueAt),
		RequireCurrentStatus:    "OPEN",
		RequireCurrentRevision:  requiredRevision,
		RequireCurrentUpdatedAt: strings.TrimSpace(requiredUpdatedAt),
	}
}

func (s *Store) canonicalizeFailedResolveLinkedSourceQueueUpsertTx(ctx context.Context, tx *sql.Tx, workspaceID string, action HumanActionRecord, resolvedBy, resolution, comment string, input OperatorQueueUpsertInput) (OperatorQueueUpsertInput, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	input.WorkspaceID = firstNonEmpty(strings.TrimSpace(input.WorkspaceID), workspaceID)
	if workspaceID == "" || input.WorkspaceID != workspaceID {
		return OperatorQueueUpsertInput{}, fmt.Errorf("linked source queue workspace mismatch for action %s", strings.TrimSpace(action.ActionID))
	}
	requiredUpdatedAt := strings.TrimSpace(input.RequireCurrentUpdatedAt)
	requiredRevision := input.RequireCurrentRevision
	if requiredRevision <= 0 && requiredUpdatedAt == "" {
		queueRef := firstNonEmpty(strings.TrimSpace(input.QueueID), strings.TrimSpace(input.QueueKey))
		return OperatorQueueUpsertInput{}, fmt.Errorf("linked source queue %s requires current revision for failed rollback", queueRef)
	}
	currentQueue, err := s.getOperatorQueueItemTx(ctx, tx, input.WorkspaceID, strings.TrimSpace(input.QueueID), strings.TrimSpace(input.QueueKey))
	if err != nil {
		return OperatorQueueUpsertInput{}, err
	}
	if err := requireFailedResolveLinkedSourceQueueLink(currentQueue, action.ActionID); err != nil {
		return OperatorQueueUpsertInput{}, err
	}

	var canonical OperatorQueueUpsertInput
	switch {
	case func() bool {
		payload, ok := decodeLinkedRollbackFailurePayload(currentQueue.PayloadJSON, currentQueue.QueueKey)
		if !ok {
			return false
		}
		preferredRootCauseID := ""
		preferredProvenanceGroupID := ""
		var preferredParentRefs []string
		if preferred, ok := decodeLinkedRollbackFailurePayload(input.PayloadJSON, currentQueue.QueueKey); ok {
			preferredRootCauseID = preferred.RootCauseID
			preferredProvenanceGroupID = preferred.ProvenanceGroupID
			preferredParentRefs = append([]string(nil), preferred.ParentRefsJSON...)
		}
		canonical = canonicalFailedResolveRollbackFailureUpsert(
			currentQueue,
			payload,
			action,
			resolvedBy,
			comment,
			requiredRevision,
			requiredUpdatedAt,
			preferredRootCauseID,
			preferredProvenanceGroupID,
			preferredParentRefs,
		)
		return true
	}():
	case func() bool {
		payload, ok := decodeLinkedRebaseFollowupPayload(currentQueue.PayloadJSON, currentQueue.QueueKey)
		if !ok {
			return false
		}
		preferredRollbackReason := ""
		preferredRootCauseID := ""
		preferredProvenanceGroupID := ""
		var preferredParentRefs []string
		if preferred, ok := decodeLinkedRebaseFollowupPayload(input.PayloadJSON, currentQueue.QueueKey); ok {
			preferredRollbackReason = preferred.RollbackReason
			preferredRootCauseID = preferred.RootCauseID
			preferredProvenanceGroupID = preferred.ProvenanceGroupID
			preferredParentRefs = append([]string(nil), preferred.ParentRefsJSON...)
		}
		canonical = canonicalFailedResolveRebaseFollowupUpsert(
			currentQueue,
			payload,
			action,
			resolvedBy,
			resolution,
			comment,
			requiredRevision,
			requiredUpdatedAt,
			preferredRollbackReason,
			preferredRootCauseID,
			preferredProvenanceGroupID,
			preferredParentRefs,
		)
		return true
	}():
	default:
		queueRef := firstNonEmpty(strings.TrimSpace(currentQueue.QueueID), strings.TrimSpace(currentQueue.QueueKey))
		return OperatorQueueUpsertInput{}, fmt.Errorf("linked source queue %s is not a supported failed rollback carrier", queueRef)
	}

	canonical, err = s.refreshFailedResolveLinkedSourceQueueUpsertLineageTx(ctx, tx, canonical)
	if err != nil {
		return OperatorQueueUpsertInput{}, err
	}
	if err := requireFailedResolveLinkedSourceQueueCanonicalPayload(currentQueue.QueueKey, canonical.PayloadJSON, action.ActionID); err != nil {
		return OperatorQueueUpsertInput{}, err
	}
	return canonical, nil
}

func (s *Store) refreshFailedResolveLinkedSourceQueueUpsertLineageTx(ctx context.Context, tx *sql.Tx, input OperatorQueueUpsertInput) (OperatorQueueUpsertInput, error) {
	currentQueue, err := s.getOperatorQueueItemTx(ctx, tx, strings.TrimSpace(input.WorkspaceID), strings.TrimSpace(input.QueueID), strings.TrimSpace(input.QueueKey))
	if err != nil {
		return OperatorQueueUpsertInput{}, err
	}
	rootCauseID, provenanceGroupID, parentRefs, err := s.refreshActionResolveQueueLineageBasisTx(ctx, tx, currentQueue)
	if err != nil {
		return OperatorQueueUpsertInput{}, err
	}

	if payload, ok := decodeLinkedRollbackFailurePayload(input.PayloadJSON, currentQueue.QueueKey); ok {
		parentRefs, rootCauseID, provenanceGroupID, err = mergeFailedResolveLinkedSourceQueueLineage(
			payload.RootCauseID,
			payload.ProvenanceGroupID,
			payload.ParentRefsJSON,
			rootCauseID,
			provenanceGroupID,
			parentRefs,
		)
		if err != nil {
			return OperatorQueueUpsertInput{}, err
		}
		payload.RootCauseID = rootCauseID
		payload.ProvenanceGroupID = provenanceGroupID
		payload.ParentRefsJSON = append([]string(nil), parentRefs...)
		payload.Normalize()
		input.PayloadJSON = marshalRollbackFailurePayload(payload)
		return input, nil
	}

	if payload, ok := decodeLinkedRebaseFollowupPayload(input.PayloadJSON, currentQueue.QueueKey); ok {
		parentRefs, rootCauseID, provenanceGroupID, err = mergeFailedResolveLinkedSourceQueueLineage(
			payload.RootCauseID,
			payload.ProvenanceGroupID,
			payload.ParentRefsJSON,
			rootCauseID,
			provenanceGroupID,
			parentRefs,
		)
		if err != nil {
			return OperatorQueueUpsertInput{}, err
		}
		payload.RootCauseID = rootCauseID
		payload.ProvenanceGroupID = provenanceGroupID
		payload.ParentRefsJSON = append([]string(nil), parentRefs...)
		payload.Normalize()
		input.PayloadJSON = marshalActionQueuePayload(payload)
		return input, nil
	}

	return input, nil
}

func actionResolveRuntimeLineageFromLinkedEvents(linkedEvents []OperatorQueueSyncEvent) (string, string, string, bool, error) {
	haveAuthoritativeLineage := false
	authoritativeRootCauseID := ""
	authoritativeProvenanceGroupID := ""
	var authoritativeParentRefs []string
	for _, item := range linkedEvents {
		lineage, ok := operatorQueuePayloadLineage(item.Record.PayloadJSON)
		if !ok {
			continue
		}
		normalizedParentRefs, err := normalizeActionCreateLineageParentRefs(lineage.ParentRefsJSON...)
		if err != nil {
			return "", "", "", false, err
		}
		rootCauseID := strings.TrimSpace(lineage.RootCauseID)
		provenanceGroupID := strings.TrimSpace(lineage.ProvenanceGroupID)
		if !haveAuthoritativeLineage {
			haveAuthoritativeLineage = true
			authoritativeRootCauseID = rootCauseID
			authoritativeProvenanceGroupID = provenanceGroupID
			authoritativeParentRefs = append([]string(nil), normalizedParentRefs...)
			continue
		}
		if rootCauseID != authoritativeRootCauseID ||
			provenanceGroupID != authoritativeProvenanceGroupID ||
			!reflect.DeepEqual(normalizedParentRefs, authoritativeParentRefs) {
			return "", "", "", false, fmt.Errorf("linked source queue lineage updated concurrently")
		}
	}
	if haveAuthoritativeLineage {
		parentRefsJSON, err := payloadParentRefsJSON(authoritativeParentRefs)
		if err != nil {
			return "", "", "", false, err
		}
		return authoritativeRootCauseID, authoritativeProvenanceGroupID, strings.TrimSpace(parentRefsJSON), true, nil
	}
	for _, item := range linkedEvents {
		eventID := strings.TrimSpace(item.Event.EventID)
		if eventID == "" {
			continue
		}
		parentRefsJSON, err := payloadParentRefsJSON([]string{eventID})
		if err != nil {
			return "", "", "", false, err
		}
		return eventID, eventID, parentRefsJSON, true, nil
	}
	return "", "", "", false, nil
}

func rewriteRuntimeEventPayloadLineage(payloadJSON, rootCauseID, provenanceGroupID, parentRefsJSON string) (string, error) {
	payload := map[string]any{}
	if trimmed := strings.TrimSpace(payloadJSON); trimmed != "" {
		if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
			return "", fmt.Errorf("decode runtime event payload lineage: %w", err)
		}
	}
	if trimmed := strings.TrimSpace(rootCauseID); trimmed != "" {
		payload["root_cause_id"] = trimmed
	} else {
		delete(payload, "root_cause_id")
	}
	if trimmed := strings.TrimSpace(provenanceGroupID); trimmed != "" {
		payload["provenance_group_id"] = trimmed
	} else {
		delete(payload, "provenance_group_id")
	}
	if trimmed := strings.TrimSpace(parentRefsJSON); trimmed != "" {
		var refs []string
		if err := json.Unmarshal([]byte(trimmed), &refs); err != nil {
			return "", fmt.Errorf("decode runtime event payload parent refs: %w", err)
		}
		if len(refs) > 0 {
			payload["parent_refs_json"] = refs
		} else {
			delete(payload, "parent_refs_json")
		}
	} else {
		delete(payload, "parent_refs_json")
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal runtime event payload lineage: %w", err)
	}
	return string(encoded), nil
}

func overlayRebaseFollowupPayloadLineage(dst *model.RebaseFollowupPayload, override *model.RebaseFollowupPayload) {
	if dst == nil || override == nil {
		return
	}
	if trimmed := strings.TrimSpace(override.RootCauseID); trimmed != "" {
		dst.RootCauseID = trimmed
	}
	if trimmed := strings.TrimSpace(override.ProvenanceGroupID); trimmed != "" {
		dst.ProvenanceGroupID = trimmed
	}
	if len(override.ParentRefsJSON) > 0 {
		dst.ParentRefsJSON = append([]string(nil), override.ParentRefsJSON...)
	}
	dst.Normalize()
}

func overlayRollbackFailurePayloadLineage(dst *model.RebaseRollbackFailurePayload, override *model.RebaseRollbackFailurePayload) {
	if dst == nil || override == nil {
		return
	}
	if trimmed := strings.TrimSpace(override.RootCauseID); trimmed != "" {
		dst.RootCauseID = trimmed
	}
	if trimmed := strings.TrimSpace(override.ProvenanceGroupID); trimmed != "" {
		dst.ProvenanceGroupID = trimmed
	}
	if len(override.ParentRefsJSON) > 0 {
		dst.ParentRefsJSON = append([]string(nil), override.ParentRefsJSON...)
	}
	dst.Normalize()
}

func humanActionOperatorQueueUpsertInput(action HumanActionRecord, linkedSourceQueue *OperatorQueueRecord, linkedRebasePayload *model.RebaseFollowupPayload, linkedRollbackPayload *model.RebaseRollbackFailurePayload) OperatorQueueUpsertInput {
	payload := model.RebaseFollowupPayload{
		ActionID:         strings.TrimSpace(action.ActionID),
		ActionQueueKey:   "action:" + strings.TrimSpace(action.ActionID),
		ActionStatus:     strings.TrimSpace(action.Status),
		ActionTitle:      strings.TrimSpace(action.Title),
		ActionAssignedTo: strings.TrimSpace(action.AssignedTo),
		TaskID:           strings.TrimSpace(action.TaskID),
	}
	if linkedSourceQueue != nil {
		payload.SourceQueueID = strings.TrimSpace(linkedSourceQueue.QueueID)
		payload.SourceQueueKey = strings.TrimSpace(linkedSourceQueue.QueueKey)
	}
	if linkedRebasePayload != nil {
		payload.RootCauseID = strings.TrimSpace(linkedRebasePayload.RootCauseID)
		payload.ProvenanceGroupID = strings.TrimSpace(linkedRebasePayload.ProvenanceGroupID)
		payload.ParentRefsJSON = append([]string(nil), linkedRebasePayload.ParentRefsJSON...)
	}
	if linkedRollbackPayload != nil {
		payload.RootCauseID = strings.TrimSpace(linkedRollbackPayload.RootCauseID)
		payload.ProvenanceGroupID = strings.TrimSpace(linkedRollbackPayload.ProvenanceGroupID)
		payload.ParentRefsJSON = append([]string(nil), linkedRollbackPayload.ParentRefsJSON...)
	}
	payload.Normalize()
	return OperatorQueueUpsertInput{
		WorkspaceID:       strings.TrimSpace(action.WorkspaceID),
		QueueKey:          "action:" + strings.TrimSpace(action.ActionID),
		QueueType:         "FOLLOW_UP",
		Title:             strings.TrimSpace(action.Title),
		Summary:           strings.TrimSpace(action.Description),
		Details:           strings.TrimSpace(action.Description),
		PayloadJSON:       marshalActionQueuePayload(payload),
		AssignedTo:        strings.TrimSpace(action.AssignedTo),
		Urgency:           humanActionQueueUrgency(action.Blocking),
		SourceKind:        "human_action",
		SourceID:          strings.TrimSpace(action.ActionID),
		TaskID:            strings.TrimSpace(action.TaskID),
		AgentID:           strings.TrimSpace(action.AgentID),
		KeepSessionActive: action.Blocking,
	}
}

func decodeLinkedRebaseFollowupPayload(payloadJSON, queueKey string) (model.RebaseFollowupPayload, bool) {
	trimmed := strings.TrimSpace(payloadJSON)
	if trimmed == "" {
		return model.RebaseFollowupPayload{}, false
	}
	var payload model.RebaseFollowupPayload
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return model.RebaseFollowupPayload{}, false
	}
	payload.Normalize()
	if !payload.IsRebaseFollowup(queueKey) {
		return model.RebaseFollowupPayload{}, false
	}
	return payload, true
}

func decodeLinkedRollbackFailurePayload(payloadJSON, queueKey string) (model.RebaseRollbackFailurePayload, bool) {
	trimmed := strings.TrimSpace(payloadJSON)
	if trimmed == "" {
		return model.RebaseRollbackFailurePayload{}, false
	}
	var payload model.RebaseRollbackFailurePayload
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return model.RebaseRollbackFailurePayload{}, false
	}
	payload.Normalize()
	if !payload.IsRollbackFailure(queueKey) {
		return model.RebaseRollbackFailurePayload{}, false
	}
	return payload, true
}

func decodeHumanActionQueuePayload(payloadJSON, queueKey string) (model.RebaseFollowupPayload, bool) {
	queueKey = strings.TrimSpace(strings.ToLower(queueKey))
	if !strings.HasPrefix(queueKey, "action:") {
		return model.RebaseFollowupPayload{}, false
	}
	trimmed := strings.TrimSpace(payloadJSON)
	if trimmed == "" {
		return model.RebaseFollowupPayload{}, false
	}
	var payload model.RebaseFollowupPayload
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return model.RebaseFollowupPayload{}, false
	}
	payload.Normalize()
	if strings.TrimSpace(payload.ActionID) == "" {
		return model.RebaseFollowupPayload{}, false
	}
	return payload, true
}

func isHumanActionNotFoundError(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "action not found")
}

func (s *Store) syncEscalatedStandaloneHumanActionQueueTx(ctx context.Context, tx *sql.Tx, queue *OperatorQueueRecord) error {
	if queue == nil || !strings.EqualFold(strings.TrimSpace(queue.SourceKind), "human_action") {
		return nil
	}
	payload, ok := decodeHumanActionQueuePayload(queue.PayloadJSON, queue.QueueKey)
	if !ok {
		return nil
	}
	if strings.TrimSpace(payload.SourceQueueID) != "" || strings.TrimSpace(payload.SourceQueueKey) != "" {
		return nil
	}
	actionID := firstNonEmpty(strings.TrimSpace(queue.SourceID), strings.TrimSpace(payload.ActionID))
	if actionID == "" {
		return nil
	}
	action, err := s.getHumanActionTx(ctx, tx, actionID)
	if err != nil {
		if isHumanActionNotFoundError(err) {
			return nil
		}
		return err
	}
	if strings.ToUpper(strings.TrimSpace(action.Status)) != model.ActionStatusPending {
		return nil
	}
	if strings.TrimSpace(action.AssignedTo) != strings.TrimSpace(queue.AssignedTo) {
		if _, err := tx.ExecContext(ctx,
			`UPDATE human_actions SET assigned_to = ?, revision = revision + 1 WHERE action_id = ?`,
			blankStringOrNil(strings.TrimSpace(queue.AssignedTo)),
			action.ActionID,
		); err != nil {
			return fmt.Errorf("sync escalated standalone human action assignee: %w", err)
		}
		action.Revision++
	}
	payload.ActionID = strings.TrimSpace(action.ActionID)
	payload.ActionQueueKey = "action:" + strings.TrimSpace(action.ActionID)
	payload.ActionAssignedTo = strings.TrimSpace(queue.AssignedTo)
	payload.ActionStatus = strings.TrimSpace(action.Status)
	payload.ActionTitle = strings.TrimSpace(action.Title)
	payload.TaskID = firstNonEmpty(strings.TrimSpace(action.TaskID), strings.TrimSpace(payload.TaskID))
	payload.Normalize()
	queue.PayloadJSON = marshalActionQueuePayload(payload)
	return nil
}

func (s *Store) syncEscalatedLinkedHumanActionTx(ctx context.Context, tx *sql.Tx, authority WorkspaceAuthorityRecord, queue *OperatorQueueRecord, now string) (*OperatorQueueSyncEvent, error) {
	if queue == nil {
		return nil, nil
	}

	rollbackPayload, hasRollback := decodeLinkedRollbackFailurePayload(queue.PayloadJSON, queue.QueueKey)
	rebasePayload := model.RebaseFollowupPayload{}
	hasRebase := false
	if !hasRollback {
		rebasePayload, hasRebase = decodeLinkedRebaseFollowupPayload(queue.PayloadJSON, queue.QueueKey)
	}

	if hasRebase {
		rebasePayload.ActionAssignedTo = strings.TrimSpace(queue.AssignedTo)
		rebasePayload.Normalize()
		queue.PayloadJSON = marshalActionQueuePayload(rebasePayload)
	}

	linkedActionID := ""
	switch {
	case hasRebase:
		linkedActionID = strings.TrimSpace(rebasePayload.ActionID)
	case hasRollback:
		linkedActionID = strings.TrimSpace(rollbackPayload.FollowupActionID)
	}
	if linkedActionID == "" {
		return nil, nil
	}

	action, err := s.getHumanActionTx(ctx, tx, linkedActionID)
	if err != nil {
		if isHumanActionNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}
	if strings.ToUpper(strings.TrimSpace(action.Status)) != model.ActionStatusPending {
		return nil, nil
	}
	if strings.TrimSpace(action.AssignedTo) != strings.TrimSpace(queue.AssignedTo) {
		if _, err := tx.ExecContext(ctx,
			`UPDATE human_actions SET assigned_to = ?, revision = revision + 1 WHERE action_id = ?`,
			blankStringOrNil(strings.TrimSpace(queue.AssignedTo)),
			action.ActionID,
		); err != nil {
			return nil, fmt.Errorf("sync escalated linked human action assignee: %w", err)
		}
		action.AssignedTo = strings.TrimSpace(queue.AssignedTo)
		action.Revision++
	}

	actionQueueInput := humanActionOperatorQueueUpsertInput(action, queue, func() *model.RebaseFollowupPayload {
		if hasRebase {
			return &rebasePayload
		}
		return nil
	}(), func() *model.RebaseRollbackFailurePayload {
		if hasRollback {
			return &rollbackPayload
		}
		return nil
	}())
	record, event, err := s.upsertOperatorQueueItemWithAuthorityTx(ctx, tx, authority, normalizeOperatorQueueUpsertInput(actionQueueInput), now)
	if err != nil {
		return nil, fmt.Errorf("sync escalated linked action queue assignee: %w", err)
	}

	return &OperatorQueueSyncEvent{Record: record, Event: event}, nil
}

func linkedActionSourceQueueCreateUpsertInput(queue OperatorQueueRecord, payload model.RebaseFollowupPayload, action HumanActionRecord) OperatorQueueUpsertInput {
	payload.ActionID = strings.TrimSpace(action.ActionID)
	payload.ActionQueueKey = "action:" + strings.TrimSpace(action.ActionID)
	payload.ActionStatus = strings.TrimSpace(action.Status)
	payload.ActionTitle = strings.TrimSpace(action.Title)
	payload.ActionAssignedTo = strings.TrimSpace(action.AssignedTo)
	payload.ActionBlocking = action.Blocking
	payload.RebaseWorkflowState = humanActionLinkedSourceQueueWorkflowState(action.Status)
	payload.RebaseWorkflowStep = humanActionLinkedSourceQueueWorkflowStep(action.Status)
	payload.Normalize()

	details := strings.TrimSpace(queue.Details)
	details = operatorQueueDetailLine(details, "Linked action:", strings.TrimSpace(action.ActionID))
	details = operatorQueueDetailLine(details, "Action queue:", "action:"+strings.TrimSpace(action.ActionID))
	details = operatorQueueDetailLine(details, "Workflow state:", humanActionLinkedSourceQueueWorkflowState(action.Status))
	details = operatorQueueDetailLine(details, "Workflow step:", humanActionLinkedSourceQueueWorkflowStep(action.Status))

	summary := strings.TrimSpace(queue.Summary)
	actionSummary := strings.TrimSpace(firstNonEmpty(action.Title, action.ActionID))
	if summary == "" {
		summary = actionSummary
	} else if actionSummary != "" && !strings.Contains(summary, strings.TrimSpace(action.ActionID)) {
		summary += " | Action: " + actionSummary + " (" + strings.TrimSpace(action.ActionID) + ")"
	}

	return OperatorQueueUpsertInput{
		QueueID:                 strings.TrimSpace(queue.QueueID),
		WorkspaceID:             strings.TrimSpace(queue.WorkspaceID),
		QueueKey:                strings.TrimSpace(queue.QueueKey),
		QueueType:               strings.TrimSpace(queue.QueueType),
		Title:                   strings.TrimSpace(queue.Title),
		Summary:                 strings.TrimSpace(summary),
		Details:                 strings.TrimSpace(details),
		PayloadJSON:             marshalActionQueuePayload(payload),
		AssignedTo:              firstNonEmpty(strings.TrimSpace(action.AssignedTo), strings.TrimSpace(queue.AssignedTo)),
		Urgency:                 strings.TrimSpace(queue.Urgency),
		SourceKind:              strings.TrimSpace(queue.SourceKind),
		SourceID:                strings.TrimSpace(queue.SourceID),
		TaskID:                  firstNonEmpty(strings.TrimSpace(queue.TaskID), strings.TrimSpace(action.TaskID)),
		SessionID:               strings.TrimSpace(queue.SessionID),
		AgentID:                 firstNonEmpty(strings.TrimSpace(queue.AgentID), strings.TrimSpace(action.AgentID)),
		KeepSessionActive:       queue.KeepSessionActive,
		DueAt:                   derefString(queue.DueAt),
		RequireCurrentStatus:    "OPEN",
		RequireCurrentRevision:  queue.Revision,
		RequireCurrentUpdatedAt: strings.TrimSpace(queue.UpdatedAt),
	}
}

func linkedRollbackFailureSourceQueueCreateUpsertInput(queue OperatorQueueRecord, payload model.RebaseRollbackFailurePayload, action HumanActionRecord) OperatorQueueUpsertInput {
	payload.FollowupActionID = strings.TrimSpace(action.ActionID)
	payload.FollowupActionQueueKey = "action:" + strings.TrimSpace(action.ActionID)
	payload.FollowupActionStatus = strings.TrimSpace(action.Status)
	payload.Normalize()

	details := strings.TrimSpace(queue.Details)
	details = operatorQueueDetailLine(details, "Follow-up action:", strings.TrimSpace(action.ActionID))
	details = operatorQueueDetailLine(details, "Follow-up action queue:", "action:"+strings.TrimSpace(action.ActionID))
	details = operatorQueueDetailLine(details, "Follow-up action status:", strings.TrimSpace(action.Status))

	summary := strings.TrimSpace(queue.Summary)
	actionSummary := strings.TrimSpace(firstNonEmpty(action.Title, action.ActionID))
	if summary == "" {
		summary = actionSummary
	} else if actionSummary != "" && !strings.Contains(summary, strings.TrimSpace(action.ActionID)) {
		summary += " | Action: " + actionSummary + " (" + strings.TrimSpace(action.ActionID) + ")"
	}

	return OperatorQueueUpsertInput{
		QueueID:                 strings.TrimSpace(queue.QueueID),
		WorkspaceID:             strings.TrimSpace(queue.WorkspaceID),
		QueueKey:                strings.TrimSpace(queue.QueueKey),
		QueueType:               strings.TrimSpace(queue.QueueType),
		Title:                   strings.TrimSpace(queue.Title),
		Summary:                 strings.TrimSpace(summary),
		Details:                 strings.TrimSpace(details),
		PayloadJSON:             marshalRollbackFailurePayload(payload),
		AssignedTo:              firstNonEmpty(strings.TrimSpace(action.AssignedTo), strings.TrimSpace(queue.AssignedTo)),
		Urgency:                 strings.TrimSpace(queue.Urgency),
		SourceKind:              strings.TrimSpace(queue.SourceKind),
		SourceID:                strings.TrimSpace(queue.SourceID),
		TaskID:                  firstNonEmpty(strings.TrimSpace(queue.TaskID), strings.TrimSpace(action.TaskID)),
		SessionID:               strings.TrimSpace(queue.SessionID),
		AgentID:                 firstNonEmpty(strings.TrimSpace(queue.AgentID), strings.TrimSpace(action.AgentID)),
		KeepSessionActive:       queue.KeepSessionActive,
		DueAt:                   derefString(queue.DueAt),
		RequireCurrentStatus:    "OPEN",
		RequireCurrentRevision:  queue.Revision,
		RequireCurrentUpdatedAt: strings.TrimSpace(queue.UpdatedAt),
	}
}

func humanActionQueueUrgency(blocking bool) string {
	if blocking {
		return "HIGH"
	}
	return "NORMAL"
}

func humanActionLinkedSourceQueueWorkflowState(actionStatus string) string {
	switch strings.ToUpper(strings.TrimSpace(actionStatus)) {
	case model.ActionStatusCompleted:
		return model.RebaseWorkflowStateCompleted
	case model.ActionStatusFailed:
		return model.RebaseWorkflowStateFailed
	default:
		return model.RebaseWorkflowStateClaimed
	}
}

func humanActionLinkedSourceQueueWorkflowStep(actionStatus string) string {
	switch strings.ToUpper(strings.TrimSpace(actionStatus)) {
	case model.ActionStatusCompleted, model.ActionStatusFailed:
		return model.RebaseWorkflowStepActionResolved
	default:
		return model.RebaseWorkflowStepAwaitResolution
	}
}

func marshalActionQueuePayload(payload model.RebaseFollowupPayload) string {
	encoded, err := json.Marshal(payload)
	if err != nil {
		panic(fmt.Sprintf("marshal rebase followup payload: %v", err))
	}
	return string(encoded)
}

func marshalRollbackFailurePayload(payload model.RebaseRollbackFailurePayload) string {
	encoded, err := json.Marshal(payload)
	if err != nil {
		panic(fmt.Sprintf("marshal rollback failure payload: %v", err))
	}
	return string(encoded)
}

func operatorQueueDetailLine(details, prefix, value string) string {
	prefix = strings.TrimSpace(prefix)
	value = strings.TrimSpace(value)
	if prefix == "" || value == "" {
		return strings.TrimSpace(details)
	}
	lines := []string{}
	if trimmed := strings.TrimSpace(details); trimmed != "" {
		lines = strings.Split(trimmed, "\n")
	}
	replaced := false
	for idx, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), prefix) {
			lines[idx] = prefix + " " + value
			replaced = true
		}
	}
	if !replaced {
		lines = append(lines, prefix+" "+value)
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func linkedActionFailedResolveRollbackReason(resolution string) string {
	switch strings.ToUpper(strings.TrimSpace(resolution)) {
	case model.ActionStatusFailed:
		return "linked_action_failed"
	default:
		return "linked_action_resolved"
	}
}

func actionQueueUpsertDetailLine(details, prefix, value string) string {
	return operatorQueueDetailLine(details, prefix, value)
}

func actionQueueRemoveDetailLine(details, prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return strings.TrimSpace(details)
	}
	lines := []string{}
	if trimmed := strings.TrimSpace(details); trimmed != "" {
		lines = strings.Split(trimmed, "\n")
	}
	kept := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), prefix) {
			continue
		}
		kept = append(kept, line)
	}
	return strings.TrimSpace(strings.Join(kept, "\n"))
}

func actionQueueSummaryWithWorkflowMarker(summary, marker string) string {
	summary = strings.TrimSpace(summary)
	for _, candidate := range []string{"In progress", "Claimed"} {
		if summary == candidate {
			summary = ""
			continue
		}
		summary = strings.ReplaceAll(summary, " | "+candidate, "")
		summary = strings.ReplaceAll(summary, candidate+" | ", "")
	}
	summary = strings.TrimSpace(strings.Trim(summary, "| "))
	marker = strings.TrimSpace(marker)
	if marker == "" {
		return summary
	}
	if summary == "" {
		return marker
	}
	return summary + " | " + marker
}

func actionQueueSummaryRemoveActionLink(summary string) string {
	summary = strings.TrimSpace(summary)
	if idx := strings.Index(summary, " | Action: "); idx >= 0 {
		summary = strings.TrimSpace(summary[:idx])
	}
	if strings.HasPrefix(summary, "Action: ") {
		if idx := strings.Index(summary, " | "); idx >= 0 {
			summary = strings.TrimSpace(summary[idx+3:])
		} else {
			summary = ""
		}
	}
	return strings.TrimSpace(strings.Trim(summary, "| "))
}

// ListHumanActions returns human actions for a workspace, optionally filtered by status.
func (s *Store) ListHumanActions(ctx context.Context, workspaceID, status string) ([]HumanActionRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}

	query := `SELECT ha.action_id, ha.workspace_id, ha.task_id, ha.agent_id, ha.assigned_to,
	                 ha.title, ha.description, ha.blocking, ha.status, ha.revision, ha.resolution_comment,
	                 ha.created_at, ha.resolved_at, ha.resolved_by,
	                 t.title, t.priority, t.status
	          FROM human_actions ha
	          JOIN tasks t ON t.task_id = ha.task_id
	          WHERE ha.workspace_id = ?`
	args := []any{workspaceID}

	status = strings.TrimSpace(status)
	if status != "" {
		query += ` AND ha.status = ?`
		args = append(args, status)
	}
	query += ` ORDER BY
	           CASE ha.status WHEN 'PENDING' THEN 0 WHEN 'COMPLETED' THEN 1 ELSE 2 END,
	           ha.created_at DESC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query human actions: %w", err)
	}
	defer rows.Close()

	out := []HumanActionRecord{}
	for rows.Next() {
		var row HumanActionRecord
		var blocking int
		var resolvedAt sql.NullString
		if err := rows.Scan(
			&row.ActionID, &row.WorkspaceID, &row.TaskID, &row.AgentID, &row.AssignedTo,
			&row.Title, &row.Description, &blocking, &row.Status, &row.Revision, &row.ResolutionComment,
			&row.CreatedAt, &resolvedAt, &row.ResolvedBy,
			&row.TaskTitle, &row.TaskPriority, &row.TaskStatus,
		); err != nil {
			return nil, fmt.Errorf("scan human action: %w", err)
		}
		row.Blocking = blocking != 0
		if resolvedAt.Valid {
			row.ResolvedAt = resolvedAt.String
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func scanHumanActionRecord(scanner interface{ Scan(dest ...any) error }) (HumanActionRecord, error) {
	var row HumanActionRecord
	var blocking int
	var resolvedAt sql.NullString
	if err := scanner.Scan(
		&row.ActionID, &row.WorkspaceID, &row.TaskID, &row.AgentID, &row.AssignedTo,
		&row.Title, &row.Description, &blocking, &row.Status, &row.Revision, &row.ResolutionComment,
		&row.CreatedAt, &resolvedAt, &row.ResolvedBy,
		&row.TaskTitle, &row.TaskPriority, &row.TaskStatus,
	); err != nil {
		return HumanActionRecord{}, err
	}
	row.Blocking = blocking != 0
	if resolvedAt.Valid {
		row.ResolvedAt = resolvedAt.String
	}
	return row, nil
}

func (s *Store) getHumanActionTx(ctx context.Context, tx *sql.Tx, actionID string) (HumanActionRecord, error) {
	actionID = strings.TrimSpace(actionID)
	if actionID == "" {
		return HumanActionRecord{}, errors.New("action_id is required")
	}
	query := `SELECT ha.action_id, ha.workspace_id, ha.task_id, ha.agent_id, ha.assigned_to,
		        ha.title, ha.description, ha.blocking, ha.status, ha.revision, ha.resolution_comment,
		        ha.created_at, ha.resolved_at, ha.resolved_by,
		        t.title, t.priority, t.status
		 FROM human_actions ha
		 JOIN tasks t ON t.task_id = ha.task_id
		 WHERE ha.action_id = ?`
	var row interface{ Scan(dest ...any) error }
	if tx != nil {
		row = tx.QueryRowContext(ctx, query, actionID)
	} else {
		row = s.db.QueryRowContext(ctx, query, actionID)
	}
	record, err := scanHumanActionRecord(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return HumanActionRecord{}, fmt.Errorf("action not found: %s", actionID)
		}
		return HumanActionRecord{}, fmt.Errorf("query human action: %w", err)
	}
	return record, nil
}

// GetHumanAction returns a single human action by ID.
func (s *Store) GetHumanAction(ctx context.Context, actionID string) (HumanActionRecord, error) {
	return s.getHumanActionTx(ctx, nil, actionID)
}

// ResolveHumanAction completes or fails a human action and only unblocks the task
// when there are no remaining pending blocking actions.
//
// This is a low-level legacy/test helper. Operator-facing action resolution
// should use ResolveHumanActionWithQueueEffects or another event-writing path.
func (s *Store) ResolveHumanAction(ctx context.Context, actionID, resolution, comment, resolvedBy string, currentAction HumanActionRecord) error {
	actionID = strings.TrimSpace(actionID)
	if actionID == "" {
		return errors.New("action_id is required")
	}
	resolution = strings.TrimSpace(resolution)
	if resolution != "COMPLETED" && resolution != "FAILED" {
		return fmt.Errorf("invalid resolution: %s (must be COMPLETED or FAILED)", resolution)
	}

	var workspaceID string
	if err := s.db.QueryRowContext(ctx, `SELECT workspace_id FROM human_actions WHERE action_id = ?`, actionID).Scan(&workspaceID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("action %s not found or already resolved", actionID)
		}
		return fmt.Errorf("query human action: %w", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin human action resolve tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := validateCurrentHumanActionSnapshot(currentAction, actionID); err != nil {
		return err
	}
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		current, err := s.requirePendingHumanActionTx(ctx, tx, workspaceID, actionID, &currentAction)
		if err != nil {
			return err
		}
		workspaceID = current.WorkspaceID
		taskID := current.TaskID

		if _, err := tx.ExecContext(ctx,
			`UPDATE human_actions SET status = ?, resolution_comment = ?, resolved_at = ?, resolved_by = ?, revision = revision + 1
			 WHERE action_id = ?`,
			resolution,
			strings.TrimSpace(comment),
			now,
			strings.TrimSpace(resolvedBy),
			actionID,
		); err != nil {
			return fmt.Errorf("update human action: %w", err)
		}

		if current.Blocking {
			remaining, err := s.countPendingBlockingActionsTx(ctx, tx, workspaceID, taskID, actionID)
			if err != nil {
				return err
			}
			if remaining == 0 {
				if err := s.unblockTaskClaimTx(ctx, tx, taskID, workspaceID); err != nil {
					return err
				}
			}
		}

		if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
			return fmt.Errorf("touch workspace after human action resolve: %w", err)
		}
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit human action resolve: %w", err)
	}
	return nil
}

func (s *Store) ResolveHumanActionWithQueueEffects(ctx context.Context, actionID, resolution, comment, resolvedBy string, actionQueueInput *OperatorQueueResolveInput, linkedSourceQueueInputs []OperatorQueueResolveInput, linkedSourceQueueUpserts []OperatorQueueUpsertInput, actionRuntimeInput *RuntimeEventInput, currentAction HumanActionRecord) (HumanActionQueueResolutionResult, error) {
	actionID = strings.TrimSpace(actionID)
	if actionID == "" {
		return HumanActionQueueResolutionResult{}, errors.New("action_id is required")
	}
	if err := validateCurrentHumanActionSnapshot(currentAction, actionID); err != nil {
		return HumanActionQueueResolutionResult{}, err
	}
	resolution = strings.TrimSpace(resolution)
	if resolution != model.ActionStatusCompleted && resolution != model.ActionStatusFailed {
		return HumanActionQueueResolutionResult{}, fmt.Errorf("invalid resolution: %s (must be COMPLETED or FAILED)", resolution)
	}

	var workspaceID string
	if err := s.db.QueryRowContext(ctx, `SELECT workspace_id FROM human_actions WHERE action_id = ?`, actionID).Scan(&workspaceID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return HumanActionQueueResolutionResult{}, fmt.Errorf("action %s not found or already resolved", actionID)
		}
		return HumanActionQueueResolutionResult{}, fmt.Errorf("query human action: %w", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, now)
	if err != nil {
		return HumanActionQueueResolutionResult{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return HumanActionQueueResolutionResult{}, fmt.Errorf("begin human action resolve tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var (
		taskID             string
		action             HumanActionRecord
		actionQueueEvent   *OperatorQueueSyncEvent
		linkedEvents       []OperatorQueueSyncEvent
		actionRuntimeEvent *RuntimeEventRecord
	)
	currentActionSnapshot := &currentAction
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		current, err := s.requirePendingHumanActionTx(ctx, tx, workspaceID, actionID, currentActionSnapshot)
		if err != nil {
			return err
		}
		workspaceID = current.WorkspaceID
		taskID = current.TaskID

		if _, err := tx.ExecContext(ctx,
			`UPDATE human_actions SET status = ?, resolution_comment = ?, resolved_at = ?, resolved_by = ?, revision = revision + 1
			 WHERE action_id = ?`,
			resolution,
			strings.TrimSpace(comment),
			now,
			strings.TrimSpace(resolvedBy),
			actionID,
		); err != nil {
			return fmt.Errorf("update human action: %w", err)
		}

		action, err = s.getHumanActionTx(ctx, tx, actionID)
		if err != nil {
			return err
		}

		resolvedLinkedSourceQueueUpserts := append([]OperatorQueueUpsertInput(nil), linkedSourceQueueUpserts...)
		if resolution == model.ActionStatusFailed && len(resolvedLinkedSourceQueueUpserts) > 0 {
			for i, input := range resolvedLinkedSourceQueueUpserts {
				input, err = s.canonicalizeFailedResolveLinkedSourceQueueUpsertTx(ctx, tx, workspaceID, action, resolvedBy, resolution, comment, input)
				if err != nil {
					return err
				}
				resolvedLinkedSourceQueueUpserts[i] = input
			}
		}

		preserveBlocking := resolution == model.ActionStatusFailed && len(resolvedLinkedSourceQueueUpserts) > 0
		if current.Blocking {
			remaining, err := s.countPendingBlockingActionsTx(ctx, tx, workspaceID, taskID, actionID)
			if err != nil {
				return err
			}
			if remaining == 0 && !preserveBlocking {
				if err := s.unblockTaskClaimTx(ctx, tx, taskID, workspaceID); err != nil {
					return err
				}
			}
		}

		actionQueueEvent = nil
		if actionQueueInput != nil {
			record, event, err := s.resolveOperatorQueueItemWithAuthorityTx(
				ctx,
				tx,
				authority,
				workspaceID,
				strings.TrimSpace(actionQueueInput.QueueID),
				strings.TrimSpace(actionQueueInput.QueueKey),
				normalizeOperatorQueueStatus(actionQueueInput.Status),
				strings.TrimSpace(actionQueueInput.ResolvedBy),
				strings.TrimSpace(actionQueueInput.Resolution),
				strings.TrimSpace(actionQueueInput.Summary),
				strings.TrimSpace(actionQueueInput.Details),
				strings.TrimSpace(actionQueueInput.PayloadJSON),
				"OPEN",
				actionQueueInput.RequireCurrentRevision,
				strings.TrimSpace(actionQueueInput.RequireCurrentUpdatedAt),
				nil,
				now,
			)
			if err != nil {
				return err
			}
			actionQueueEvent = &OperatorQueueSyncEvent{Record: record, Event: event}
		}

		linkedEvents = make([]OperatorQueueSyncEvent, 0, len(linkedSourceQueueInputs))
		for _, input := range linkedSourceQueueInputs {
			record, event, err := s.resolveOperatorQueueItemWithAuthorityTx(
				ctx,
				tx,
				authority,
				workspaceID,
				strings.TrimSpace(input.QueueID),
				strings.TrimSpace(input.QueueKey),
				normalizeOperatorQueueStatus(input.Status),
				strings.TrimSpace(input.ResolvedBy),
				strings.TrimSpace(input.Resolution),
				strings.TrimSpace(input.Summary),
				strings.TrimSpace(input.Details),
				strings.TrimSpace(input.PayloadJSON),
				"OPEN",
				input.RequireCurrentRevision,
				strings.TrimSpace(input.RequireCurrentUpdatedAt),
				nil,
				now,
			)
			if err != nil {
				return err
			}
			linkedEvents = append(linkedEvents, OperatorQueueSyncEvent{Record: record, Event: event})
		}
		for _, input := range resolvedLinkedSourceQueueUpserts {
			record, event, err := s.upsertOperatorQueueItemWithAuthorityTx(ctx, tx, authority, normalizeOperatorQueueUpsertInput(input), now)
			if err != nil {
				return err
			}
			linkedEvents = append(linkedEvents, OperatorQueueSyncEvent{Record: record, Event: event})
		}

		actionRuntimeEvent = nil
		if actionRuntimeInput != nil {
			runtimeInput := *actionRuntimeInput
			runtimeInput.WorkspaceID = firstNonEmpty(strings.TrimSpace(runtimeInput.WorkspaceID), workspaceID)
			runtimeInput.TaskID = firstNonEmpty(strings.TrimSpace(runtimeInput.TaskID), taskID)
			if strings.TrimSpace(runtimeInput.CreatedAt) == "" {
				runtimeInput.CreatedAt = now
			}
			if resolution == model.ActionStatusFailed && len(resolvedLinkedSourceQueueUpserts) > 0 {
				rootCauseID, provenanceGroupID, parentRefsJSON, ok, err := actionResolveRuntimeLineageFromLinkedEvents(linkedEvents)
				if err != nil {
					return err
				}
				if ok {
					runtimeInput.RootCauseID = firstNonEmpty(strings.TrimSpace(rootCauseID), strings.TrimSpace(runtimeInput.RootCauseID))
					runtimeInput.ProvenanceGroupID = firstNonEmpty(strings.TrimSpace(provenanceGroupID), strings.TrimSpace(runtimeInput.ProvenanceGroupID))
					if strings.TrimSpace(parentRefsJSON) != "" {
						runtimeInput.ParentRefsJSON = parentRefsJSON
					}
					payloadJSON, err := rewriteRuntimeEventPayloadLineage(runtimeInput.PayloadJSON, runtimeInput.RootCauseID, runtimeInput.ProvenanceGroupID, runtimeInput.ParentRefsJSON)
					if err != nil {
						return err
					}
					runtimeInput.PayloadJSON = payloadJSON
				}
			}
			parentRefs := make([]string, 0, 1+len(linkedEvents))
			if actionQueueEvent != nil {
				parentRefs = append(parentRefs, strings.TrimSpace(actionQueueEvent.Event.EventID))
			}
			for _, item := range linkedEvents {
				parentRefs = append(parentRefs, strings.TrimSpace(item.Event.EventID))
			}
			mergedParents, err := mergeRuntimeEventParentRefsJSON(strings.TrimSpace(runtimeInput.ParentRefsJSON), parentRefs...)
			if err != nil {
				return err
			}
			runtimeInput.ParentRefsJSON = mergedParents
			event, err := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, runtimeInput)
			if err != nil {
				return err
			}
			actionRuntimeEvent = &event
		}

		if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?`, now, workspaceID); err != nil {
			return fmt.Errorf("touch workspace after human action resolve: %w", err)
		}
		return nil
	}); err != nil {
		_ = tx.Rollback()
		return HumanActionQueueResolutionResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}

	if err := tx.Commit(); err != nil {
		return HumanActionQueueResolutionResult{}, fmt.Errorf("commit human action resolve tx: %w", err)
	}

	result := HumanActionQueueResolutionResult{Action: action}
	if actionQueueEvent != nil {
		hydrated, err := s.getOperatorQueueItem(ctx, workspaceID, actionQueueEvent.Record.QueueID, "")
		if err != nil {
			return HumanActionQueueResolutionResult{}, err
		}
		actionQueueEvent.Record = hydrated
		result.ActionQueue = actionQueueEvent
	}
	for _, item := range linkedEvents {
		hydrated, err := s.getOperatorQueueItem(ctx, workspaceID, item.Record.QueueID, "")
		if err != nil {
			return HumanActionQueueResolutionResult{}, err
		}
		item.Record = hydrated
		result.LinkedSourceQueue = append(result.LinkedSourceQueue, item)
	}
	if actionRuntimeEvent != nil {
		eventCopy := *actionRuntimeEvent
		result.ActionEvent = &eventCopy
	}
	return result, nil
}

// SendActionMessage sends a chat message for a human action.
func (s *Store) SendActionMessage(ctx context.Context, actionID, workspaceID, fromID, content string) (string, error) {
	actionID = strings.TrimSpace(actionID)
	if actionID == "" {
		return "", errors.New("action_id is required")
	}
	content = strings.TrimSpace(content)
	if content == "" {
		return "", errors.New("content is required")
	}

	messageID := nextID("actmsg")
	now := time.Now().UTC().Format(time.RFC3339Nano)

	if _, err := s.writeDB.ExecContext(ctx,
		`INSERT INTO action_messages(message_id, action_id, workspace_id, from_id, content, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		messageID,
		actionID,
		strings.TrimSpace(workspaceID),
		strings.TrimSpace(fromID),
		content,
		now,
	); err != nil {
		return "", fmt.Errorf("insert action message: %w", err)
	}

	return messageID, nil
}

type ActionChatSendResult struct {
	MessageID         string
	ActionEvent       RuntimeEventRecord
	AgentMessageID    string
	AgentMessageEvent *RuntimeEventRecord
}

// SendActionMessageWithAuthorityEffects sends a public action chat message from an authority-backed surface.
func (s *Store) SendActionMessageWithAuthorityEffects(ctx context.Context, action HumanActionRecord, fromID, content string, promptContextEnvelope map[string]any) (ActionChatSendResult, error) {
	workspaceID := strings.TrimSpace(action.WorkspaceID)
	if workspaceID == "" {
		return ActionChatSendResult{}, errors.New("workspace_id is required")
	}
	actionID := strings.TrimSpace(action.ActionID)
	if actionID == "" {
		return ActionChatSendResult{}, errors.New("action_id is required")
	}
	content = strings.TrimSpace(content)
	if content == "" {
		return ActionChatSendResult{}, errors.New("content is required")
	}

	referenceAt := time.Now().UTC().Format(time.RFC3339Nano)
	fenceInput, err := s.currentLocalWorkspaceAuthorityFenceInput(ctx, workspaceID, authorityScopeWorkspace, referenceAt)
	if err != nil {
		return ActionChatSendResult{}, err
	}
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return ActionChatSendResult{}, fmt.Errorf("begin action chat authority tx: %w", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	result := ActionChatSendResult{}
	if _, err := s.WithFencedWorkspaceAuthorityTx(ctx, tx, fenceInput, func(tx *sql.Tx, authority WorkspaceAuthorityRecord) error {
		now := time.Now().UTC().Format(time.RFC3339Nano)
		result.MessageID = nextID("actmsg")
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO action_messages(message_id, action_id, workspace_id, from_id, content, created_at)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			result.MessageID,
			actionID,
			workspaceID,
			strings.TrimSpace(fromID),
			content,
			now,
		); err != nil {
			return fmt.Errorf("insert action message: %w", err)
		}

		actionChatPayload := map[string]any{
			"action_id":  actionID,
			"message_id": result.MessageID,
			"from":       strings.TrimSpace(fromID),
			"content":    content,
		}
		if promptContextEnvelope != nil {
			var err error
			actionChatPayload, err = AttachHumanActionPromptContextEnvelope(actionChatPayload, promptContextEnvelope)
			if err != nil {
				return err
			}
		}
		actionEvent, err := s.appendAuthorityBackedRuntimeEventTx(ctx, tx, authority, RuntimeEventInput{
			EventID:     nextID("rtev"),
			WorkspaceID: workspaceID,
			EventType:   "action.chat",
			EntityType:  "action_message",
			EntityID:    result.MessageID,
			ActorType:   "operator",
			ActorID:     strings.TrimSpace(fromID),
			AgentID:     strings.TrimSpace(action.AgentID),
			TaskID:      strings.TrimSpace(action.TaskID),
			PayloadJSON: string(mustJSON(actionChatPayload)),
			CreatedAt:   now,
		})
		if err != nil {
			return err
		}
		result.ActionEvent = actionEvent

		if strings.TrimSpace(action.AgentID) != "" {
			agentMessageID, agentMessageEvent, err := s.sendMessageWithOptionalAuthorityTx(ctx, tx, authority, MessageSendInput{
				WorkspaceID: workspaceID,
				FromAgentID: strings.TrimSpace(fromID),
				ToAgentID:   strings.TrimSpace(action.AgentID),
				Channel:     "action:" + actionID,
				Content:     content,
			})
			if err != nil {
				return err
			}
			result.AgentMessageID = agentMessageID
			result.AgentMessageEvent = &agentMessageEvent
		}

		return nil
	}); err != nil {
		_ = tx.Rollback()
		return ActionChatSendResult{}, s.journalWorkspaceAuthorityReject(ctx, err, fenceInput)
	}
	if err := tx.Commit(); err != nil {
		return ActionChatSendResult{}, fmt.Errorf("commit action chat authority tx: %w", err)
	}
	tx = nil
	return result, nil
}

// ListActionMessages returns all chat messages for a human action.
func (s *Store) ListActionMessages(ctx context.Context, actionID string) ([]ActionMessageRecord, error) {
	actionID = strings.TrimSpace(actionID)
	if actionID == "" {
		return nil, errors.New("action_id is required")
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT message_id, action_id, workspace_id, from_id, content, created_at
		 FROM action_messages
		 WHERE action_id = ?
		 ORDER BY created_at ASC`,
		actionID,
	)
	if err != nil {
		return nil, fmt.Errorf("query action messages: %w", err)
	}
	defer rows.Close()

	out := []ActionMessageRecord{}
	for rows.Next() {
		var row ActionMessageRecord
		if err := rows.Scan(&row.MessageID, &row.ActionID, &row.WorkspaceID, &row.FromID, &row.Content, &row.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan action message: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// UnblockTaskClaim transitions a task claim from BLOCKED to CLAIMED.
func (s *Store) UnblockTaskClaim(ctx context.Context, taskID, workspaceID string) error {
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin unblock task claim tx: %w", err)
	}
	if err := s.unblockTaskClaimTx(ctx, tx, taskID, workspaceID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit unblock task claim tx: %w", err)
	}
	return nil
}

func (s *Store) unblockTaskClaimTx(ctx context.Context, tx *sql.Tx, taskID, workspaceID string) error {
	snapshot, ok, err := loadTaskClaimBlockerSnapshotTx(ctx, tx, taskID, workspaceID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if !snapshot.PriorExists {
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM task_claims WHERE task_id = ? AND workspace_id = ? AND claim_status = ?`,
			taskID,
			workspaceID,
			model.TaskClaimStatusBlocked,
		); err != nil {
			return fmt.Errorf("delete synthetic blocked claim: %w", err)
		}
		return clearTaskClaimBlockerSnapshotTx(ctx, tx, taskID, workspaceID)
	}

	res, err := tx.ExecContext(ctx,
		`UPDATE task_claims
		    SET agent_id = ?, claim_status = ?, summary = ?, claimed_at = ?, released_at = ?, updated_at = ?
		  WHERE task_id = ? AND workspace_id = ?`,
		strings.TrimSpace(snapshot.PriorAgentID),
		strings.TrimSpace(snapshot.PriorClaimStatus),
		strings.TrimSpace(snapshot.PriorSummary),
		strings.TrimSpace(snapshot.PriorClaimedAt),
		nullableStringValue(snapshot.PriorReleasedAt),
		now,
		taskID,
		workspaceID,
	)
	if err != nil {
		return fmt.Errorf("restore blocked task claim: %w", err)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO task_claims(task_id, workspace_id, agent_id, claim_status, summary, claimed_at, released_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			taskID,
			workspaceID,
			strings.TrimSpace(snapshot.PriorAgentID),
			strings.TrimSpace(snapshot.PriorClaimStatus),
			strings.TrimSpace(snapshot.PriorSummary),
			strings.TrimSpace(snapshot.PriorClaimedAt),
			nullableStringValue(snapshot.PriorReleasedAt),
			now,
		); err != nil {
			return fmt.Errorf("recreate restored task claim: %w", err)
		}
	}
	return clearTaskClaimBlockerSnapshotTx(ctx, tx, taskID, workspaceID)
}

// BlockTaskClaim transitions a task claim to BLOCKED when an action is created.
func (s *Store) BlockTaskClaim(ctx context.Context, taskID, workspaceID, agentID string) error {
	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return fmt.Errorf("begin block task claim tx: %w", err)
	}
	if err := s.blockTaskClaimTx(ctx, tx, taskID, workspaceID, agentID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit block task claim tx: %w", err)
	}
	return nil
}

func (s *Store) blockTaskClaimTx(ctx context.Context, tx *sql.Tx, taskID, workspaceID, agentID string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var existing taskClaimBlockerSnapshot
	var existingClaimFound bool
	if err := tx.QueryRowContext(ctx,
		`SELECT agent_id, claim_status, summary, claimed_at, released_at
		   FROM task_claims
		  WHERE task_id = ? AND workspace_id = ?`,
		taskID,
		workspaceID,
	).Scan(
		&existing.PriorAgentID,
		&existing.PriorClaimStatus,
		&existing.PriorSummary,
		&existing.PriorClaimedAt,
		&existing.PriorReleasedAt,
	); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("query task claim before block: %w", err)
		}
	} else {
		existingClaimFound = true
	}

	if existingClaimFound {
		if existing.PriorClaimStatus == model.TaskClaimStatusBlocked {
			return nil
		}
		if err := upsertTaskClaimBlockerSnapshotTx(ctx, tx, taskID, workspaceID, true, existing, now); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE task_claims
			    SET claim_status = ?, summary = 'Blocked: awaiting human action', updated_at = ?
			  WHERE task_id = ? AND workspace_id = ?`,
			model.TaskClaimStatusBlocked,
			now,
			taskID,
			workspaceID,
		); err != nil {
			return fmt.Errorf("block task claim: %w", err)
		}
		return nil
	}

	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return errors.New("agent_id is required to create blocking claim")
	}
	if err := upsertTaskClaimBlockerSnapshotTx(ctx, tx, taskID, workspaceID, false, taskClaimBlockerSnapshot{}, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO task_claims(task_id, workspace_id, agent_id, claim_status, summary, claimed_at, updated_at)
		 VALUES (?, ?, ?, ?, 'Blocked: awaiting human action', ?, ?)`,
		taskID, workspaceID, agentID, model.TaskClaimStatusBlocked, now, now,
	); err != nil {
		return fmt.Errorf("create blocked claim: %w", err)
	}
	return nil
}

func loadTaskClaimBlockerSnapshotTx(ctx context.Context, tx *sql.Tx, taskID, workspaceID string) (taskClaimBlockerSnapshot, bool, error) {
	var snapshot taskClaimBlockerSnapshot
	var priorExists int
	var priorAgentID, priorClaimStatus, priorSummary, priorClaimedAt sql.NullString
	err := tx.QueryRowContext(ctx,
		`SELECT prior_exists, prior_agent_id, prior_claim_status, prior_summary, prior_claimed_at, prior_released_at
		   FROM task_claim_blockers
		  WHERE task_id = ? AND workspace_id = ?`,
		taskID,
		workspaceID,
	).Scan(
		&priorExists,
		&priorAgentID,
		&priorClaimStatus,
		&priorSummary,
		&priorClaimedAt,
		&snapshot.PriorReleasedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return taskClaimBlockerSnapshot{}, false, nil
		}
		return taskClaimBlockerSnapshot{}, false, fmt.Errorf("load task claim blocker snapshot: %w", err)
	}
	snapshot.PriorExists = priorExists == 1
	if priorAgentID.Valid {
		snapshot.PriorAgentID = strings.TrimSpace(priorAgentID.String)
	}
	if priorClaimStatus.Valid {
		snapshot.PriorClaimStatus = strings.TrimSpace(priorClaimStatus.String)
	}
	if priorSummary.Valid {
		snapshot.PriorSummary = strings.TrimSpace(priorSummary.String)
	}
	if priorClaimedAt.Valid {
		snapshot.PriorClaimedAt = strings.TrimSpace(priorClaimedAt.String)
	}
	return snapshot, true, nil
}

func upsertTaskClaimBlockerSnapshotTx(ctx context.Context, tx *sql.Tx, taskID, workspaceID string, priorExists bool, snapshot taskClaimBlockerSnapshot, now string) error {
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO task_claim_blockers(
		     task_id, workspace_id, prior_exists, prior_agent_id, prior_claim_status, prior_summary, prior_claimed_at, prior_released_at, created_at, updated_at
		 )
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(workspace_id, task_id) DO NOTHING`,
		taskID,
		workspaceID,
		boolToInt(priorExists),
		nullableTrimmedString(snapshot.PriorAgentID),
		nullableTrimmedString(snapshot.PriorClaimStatus),
		nullableTrimmedString(snapshot.PriorSummary),
		nullableTrimmedString(snapshot.PriorClaimedAt),
		nullableStringValue(snapshot.PriorReleasedAt),
		now,
		now,
	); err != nil {
		return fmt.Errorf("upsert task claim blocker snapshot: %w", err)
	}
	return nil
}

func clearTaskClaimBlockerSnapshotTx(ctx context.Context, tx *sql.Tx, taskID, workspaceID string) error {
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM task_claim_blockers WHERE task_id = ? AND workspace_id = ?`,
		taskID,
		workspaceID,
	); err != nil {
		return fmt.Errorf("clear task claim blocker snapshot: %w", err)
	}
	return nil
}

func nullableTrimmedString(value string) any {
	if trimmed := strings.TrimSpace(value); trimmed != "" {
		return trimmed
	}
	return nil
}

func nullableStringValue(value sql.NullString) any {
	if value.Valid {
		return strings.TrimSpace(value.String)
	}
	return nil
}

func (s *Store) countPendingBlockingActionsTx(ctx context.Context, tx *sql.Tx, workspaceID, taskID, excludeActionID string) (int, error) {
	query := `SELECT COUNT(1) FROM human_actions WHERE workspace_id = ? AND task_id = ? AND blocking = 1 AND status = 'PENDING'`
	args := []any{workspaceID, taskID}
	if strings.TrimSpace(excludeActionID) != "" {
		query += ` AND action_id != ?`
		args = append(args, excludeActionID)
	}
	var count int
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("count pending blocking actions: %w", err)
	}
	return count, nil
}
