package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type actionCreateParams struct {
	WorkspaceID string `json:"workspace_id"`
	TaskID      string `json:"task_id"`
	AgentID     string `json:"agent_id"`
	AssignedTo  string `json:"assigned_to"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Blocking    *bool  `json:"blocking"`
	QueueID     string `json:"queue_id"`
	QueueKey    string `json:"queue_key"`
}

const (
	humanActionStatusPending   = model.ActionStatusPending
	humanActionStatusCompleted = model.ActionStatusCompleted
	humanActionStatusFailed    = model.ActionStatusFailed

	rebaseFollowupNextActionAttemptRebase = model.RebaseNextActionAttempt

	rebaseWorkflowStateClaimed    = model.RebaseWorkflowStateClaimed
	rebaseWorkflowStateInProgress = model.RebaseWorkflowStateInProgress
	rebaseWorkflowStateCompleted  = model.RebaseWorkflowStateCompleted
	rebaseWorkflowStateFailed     = model.RebaseWorkflowStateFailed

	rebaseWorkflowStepAwaitResolution = model.RebaseWorkflowStepAwaitResolution
	rebaseWorkflowStepAwaitRestart    = model.RebaseWorkflowStepAwaitRestart
	rebaseWorkflowStepOperatorClaimed = model.RebaseWorkflowStepOperatorClaimed
	rebaseWorkflowStepActionResolved  = model.RebaseWorkflowStepActionResolved
)

func (h *Handler) humanActionPromptContextEnvelope(ctx context.Context, workspaceID, surface string) map[string]any {
	principalType, principalID := h.promptContextPrincipal(ctx)
	return sqlite.BuildHumanActionPromptContextEnvelope(surface, "server_rpc", workspaceID, principalType, principalID)
}

func (h *Handler) attachHumanActionPromptContext(ctx context.Context, workspaceID, surface string, payload map[string]any) (map[string]any, *RPCError) {
	attached, err := sqlite.AttachHumanActionPromptContextEnvelope(payload, h.humanActionPromptContextEnvelope(ctx, workspaceID, surface))
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return attached, nil
}

func (h *Handler) actionCreate(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p actionCreateParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	sourceQueue, err := h.resolveActionCreateSourceQueue(ctx, &p)
	if err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	p.WorkspaceID = strings.TrimSpace(p.WorkspaceID)
	p.TaskID = strings.TrimSpace(p.TaskID)
	p.AgentID = strings.TrimSpace(p.AgentID)
	p.AssignedTo = strings.TrimSpace(p.AssignedTo)
	p.Title = strings.TrimSpace(p.Title)
	p.Description = strings.TrimSpace(p.Description)
	if p.WorkspaceID == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "workspace_id is required"}
	}
	if p.TaskID == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "task_id is required"}
	}
	if p.Title == "" {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "title is required"}
	}
	isBlocking := p.Blocking != nil && *p.Blocking

	var linkedQueueRecord *sqlite.OperatorQueueRecord
	var linkedQueuePayload *model.RebaseFollowupPayload
	var linkedRollbackFailurePayload *model.RebaseRollbackFailurePayload
	if sourceQueue != nil {
		queueCopy := sourceQueue.queue
		linkedQueueRecord = &queueCopy
		switch sourceQueue.kind {
		case actionCreateSourceQueueKindRollbackFailure:
			payloadCopy := sourceQueue.rollbackFailurePayload
			linkedRollbackFailurePayload = &payloadCopy
		default:
			payloadCopy := sourceQueue.payload
			linkedQueuePayload = &payloadCopy
		}
		lineage, err := h.actionCreateSourceQueueLineage(ctx, sourceQueue)
		if err != nil {
			return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
		}
		switch {
		case linkedQueuePayload != nil:
			applyRebaseFollowupPayloadLineage(linkedQueuePayload, lineage)
		case linkedRollbackFailurePayload != nil:
			applyRebaseRollbackFailurePayloadLineage(linkedRollbackFailurePayload, lineage)
		}
	}
	actionInput := sqlite.HumanActionInput{
		WorkspaceID:           p.WorkspaceID,
		TaskID:                p.TaskID,
		AgentID:               p.AgentID,
		AssignedTo:            p.AssignedTo,
		Title:                 p.Title,
		Description:           p.Description,
		Blocking:              isBlocking,
		PromptContextEnvelope: h.humanActionPromptContextEnvelope(ctx, p.WorkspaceID, "action.create"),
	}
	if h.beforeActionCreateQueueEffectsOverride != nil {
		h.beforeActionCreateQueueEffectsOverride(ctx)
	}
	var created sqlite.HumanActionQueueCreateResult
	switch {
	case linkedRollbackFailurePayload != nil:
		created, err = h.store.CreateHumanActionWithRollbackFailureQueueEffects(ctx, actionInput, linkedQueueRecord, linkedRollbackFailurePayload)
	default:
		created, err = h.store.CreateHumanActionWithQueueEffects(ctx, actionInput, linkedQueueRecord, linkedQueuePayload)
	}
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "action.create"); rpcErr != nil {
			return nil, rpcErr
		}
		if isHumanActionWorkflowConflictError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	action := created.Action
	actionID := action.ActionID
	if created.ActionQueue != nil {
		if strings.TrimSpace(created.ActionQueue.Event.EventID) != "" {
			h.publishOperatorQueueEventRecord(created.ActionQueue.Event, "workspace.ops.updated", created.ActionQueue.Record)
		} else {
			log.Printf("[human-action] queue sync returned no runtime event on create action=%s workspace=%s", actionID, p.WorkspaceID)
		}
	}
	if created.LinkedSourceQueue != nil {
		if strings.TrimSpace(created.LinkedSourceQueue.Event.EventID) != "" {
			h.publishOperatorQueueEventRecord(created.LinkedSourceQueue.Event, "workspace.ops.updated", created.LinkedSourceQueue.Record)
		} else if sourceQueue != nil {
			log.Printf("[human-action] source queue link returned no runtime event action=%s workspace=%s queue=%s", actionID, p.WorkspaceID, sourceQueue.queue.QueueID)
		}
	}
	if created.ActionEvent != nil && strings.TrimSpace(created.ActionEvent.EventID) != "" {
		h.publishRuntimeEventRecord(*created.ActionEvent, "Action required: "+p.Title)
	}

	response := map[string]any{
		"action_id":    actionID,
		"workspace_id": p.WorkspaceID,
		"task_id":      p.TaskID,
		"blocking":     isBlocking,
		"status":       action.Status,
	}
	if sourceQueue != nil {
		response["source_queue_id"] = sourceQueue.queue.QueueID
		response["source_queue_key"] = sourceQueue.queue.QueueKey
	}
	return response, nil
}

type actionListParams struct {
	WorkspaceID string `json:"workspace_id"`
	Status      string `json:"status"`
}

func (h *Handler) actionList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p actionListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}

	actions, err := h.store.ListHumanActions(ctx, p.WorkspaceID, p.Status)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	return map[string]any{
		"actions": actions,
		"count":   len(actions),
	}, nil
}

type actionResolveParams struct {
	ActionID   string `json:"action_id"`
	Resolution string `json:"resolution"`
	Comment    string `json:"comment"`
	ResolvedBy string `json:"resolved_by"`
}

type actionResolveOptions struct {
	RollbackReason string
	Lineage        rebaseRuntimeLineage
}

type rebaseRuntimeLineage struct {
	RootCauseID       string
	ProvenanceGroupID string
	ParentRefsJSON    []string
}

type actionCreateSourceQueueKind string

const (
	actionCreateSourceQueueKindRebaseFollowup  actionCreateSourceQueueKind = "rebase_followup"
	actionCreateSourceQueueKindRollbackFailure actionCreateSourceQueueKind = "rollback_failure"
)

func normalizeRuntimeLineage(lineage rebaseRuntimeLineage) rebaseRuntimeLineage {
	lineage.RootCauseID = strings.TrimSpace(lineage.RootCauseID)
	lineage.ProvenanceGroupID = strings.TrimSpace(lineage.ProvenanceGroupID)
	if len(lineage.ParentRefsJSON) == 0 {
		lineage.ParentRefsJSON = nil
		return lineage
	}
	seen := make(map[string]struct{}, len(lineage.ParentRefsJSON))
	normalized := make([]string, 0, len(lineage.ParentRefsJSON))
	for _, ref := range lineage.ParentRefsJSON {
		trimmed := strings.TrimSpace(ref)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		normalized = append(normalized, trimmed)
	}
	sort.Strings(normalized)
	if len(normalized) == 0 {
		lineage.ParentRefsJSON = nil
		return lineage
	}
	lineage.ParentRefsJSON = normalized
	return lineage
}

func runtimeLineageFromRuntimeEvent(event sqlite.RuntimeEventRecord) rebaseRuntimeLineage {
	lineage := rebaseRuntimeLineage{
		RootCauseID:       firstNonEmpty(strings.TrimSpace(event.RootCauseID), strings.TrimSpace(event.EventID)),
		ProvenanceGroupID: firstNonEmpty(strings.TrimSpace(event.ProvenanceGroupID), strings.TrimSpace(event.EventID)),
	}
	if trimmed := strings.TrimSpace(event.EventID); trimmed != "" {
		lineage.ParentRefsJSON = []string{trimmed}
	}
	return normalizeRuntimeLineage(lineage)
}

func runtimeLineageFromEventMessage(msg EventMessage) rebaseRuntimeLineage {
	lineage := rebaseRuntimeLineage{
		RootCauseID:       firstNonEmpty(strings.TrimSpace(msg.RootCauseID), strings.TrimSpace(msg.EventID)),
		ProvenanceGroupID: firstNonEmpty(strings.TrimSpace(msg.ProvenanceGroupID), strings.TrimSpace(msg.EventID)),
	}
	return normalizeRuntimeLineage(lineage)
}

func runtimeLineageFromEventIDs(eventIDs ...string) rebaseRuntimeLineage {
	lineage := rebaseRuntimeLineage{ParentRefsJSON: append([]string(nil), eventIDs...)}
	return normalizeRuntimeLineage(lineage)
}

func rebaseFollowupPayloadLineage(payload model.RebaseFollowupPayload) rebaseRuntimeLineage {
	return normalizeRuntimeLineage(rebaseRuntimeLineage{
		RootCauseID:       payload.RootCauseID,
		ProvenanceGroupID: payload.ProvenanceGroupID,
		ParentRefsJSON:    append([]string(nil), payload.ParentRefsJSON...),
	})
}

func rebaseRollbackFailurePayloadLineage(payload model.RebaseRollbackFailurePayload) rebaseRuntimeLineage {
	return normalizeRuntimeLineage(rebaseRuntimeLineage{
		RootCauseID:       payload.RootCauseID,
		ProvenanceGroupID: payload.ProvenanceGroupID,
		ParentRefsJSON:    append([]string(nil), payload.ParentRefsJSON...),
	})
}

func applyRebaseFollowupPayloadLineage(payload *model.RebaseFollowupPayload, lineage rebaseRuntimeLineage) {
	if payload == nil {
		return
	}
	lineage = normalizeRuntimeLineage(lineage)
	if lineage.RootCauseID == "" && lineage.ProvenanceGroupID == "" && len(lineage.ParentRefsJSON) == 0 {
		return
	}
	payload.RootCauseID = lineage.RootCauseID
	payload.ProvenanceGroupID = lineage.ProvenanceGroupID
	payload.ParentRefsJSON = append([]string(nil), lineage.ParentRefsJSON...)
}

func applyRebaseRollbackFailurePayloadLineage(payload *model.RebaseRollbackFailurePayload, lineage rebaseRuntimeLineage) {
	if payload == nil {
		return
	}
	lineage = normalizeRuntimeLineage(lineage)
	if lineage.RootCauseID == "" && lineage.ProvenanceGroupID == "" && len(lineage.ParentRefsJSON) == 0 {
		return
	}
	payload.RootCauseID = lineage.RootCauseID
	payload.ProvenanceGroupID = lineage.ProvenanceGroupID
	payload.ParentRefsJSON = append([]string(nil), lineage.ParentRefsJSON...)
}

func runtimePayloadApplyLineage(payload map[string]any, lineage rebaseRuntimeLineage) {
	lineage = normalizeRuntimeLineage(lineage)
	if payload == nil {
		return
	}
	if lineage.RootCauseID != "" {
		payload["root_cause_id"] = lineage.RootCauseID
	}
	if lineage.ProvenanceGroupID != "" {
		payload["provenance_group_id"] = lineage.ProvenanceGroupID
	}
	if len(lineage.ParentRefsJSON) > 0 {
		payload["parent_refs_json"] = append([]string(nil), lineage.ParentRefsJSON...)
	}
}

func runtimeLineageParentRefsJSON(lineage rebaseRuntimeLineage) string {
	lineage = normalizeRuntimeLineage(lineage)
	if len(lineage.ParentRefsJSON) == 0 {
		return ""
	}
	return string(mustJSON(lineage.ParentRefsJSON))
}

func actionRuntimeEventInputWithLineage(input sqlite.RuntimeEventInput, lineage rebaseRuntimeLineage) sqlite.RuntimeEventInput {
	lineage = normalizeRuntimeLineage(lineage)
	input.RootCauseID = firstNonEmpty(strings.TrimSpace(input.RootCauseID), lineage.RootCauseID)
	input.ProvenanceGroupID = firstNonEmpty(strings.TrimSpace(input.ProvenanceGroupID), lineage.ProvenanceGroupID)
	if strings.TrimSpace(input.ParentRefsJSON) == "" {
		input.ParentRefsJSON = runtimeLineageParentRefsJSON(lineage)
	}
	return input
}

func (h *Handler) latestRuntimeEvent(ctx context.Context, filter sqlite.RuntimeEventFilter) (sqlite.RuntimeEventRecord, bool, error) {
	events, err := h.store.ListRuntimeEvents(ctx, filter)
	if err != nil {
		return sqlite.RuntimeEventRecord{}, false, err
	}
	if len(events) == 0 {
		return sqlite.RuntimeEventRecord{}, false, nil
	}
	return events[0], true, nil
}

func actionCreateLineageWithFallback(lineage rebaseRuntimeLineage, event sqlite.RuntimeEventRecord) rebaseRuntimeLineage {
	lineage.RootCauseID = firstNonEmpty(lineage.RootCauseID, strings.TrimSpace(event.RootCauseID))
	lineage.ProvenanceGroupID = firstNonEmpty(lineage.ProvenanceGroupID, strings.TrimSpace(event.ProvenanceGroupID))
	return normalizeRuntimeLineage(lineage)
}

func linkedSourceQueueSyncLineage(synced []sqlite.OperatorQueueSyncEvent) rebaseRuntimeLineage {
	lineage := rebaseRuntimeLineage{}
	parentRefs := make([]string, 0, len(synced))
	for idx, item := range synced {
		if idx == 0 {
			if payload, err := actionCreateDecodeQueuePayload(item.Record.PayloadJSON); err == nil {
				lineage = rebaseFollowupPayloadLineage(payload)
			}
			lineage = actionCreateLineageWithFallback(lineage, item.Event)
		}
		parentRefs = append(parentRefs, strings.TrimSpace(item.Event.EventID))
	}
	if normalizedParents := normalizeRuntimeLineage(rebaseRuntimeLineage{ParentRefsJSON: parentRefs}); len(normalizedParents.ParentRefsJSON) > 0 {
		lineage.ParentRefsJSON = normalizedParents.ParentRefsJSON
	}
	return normalizeRuntimeLineage(lineage)
}

func actionResolveLineageFromInputs(sourceQueues []actionCreateSourceQueue, rollbackFailureQueue *actionCreateSourceQueue, sourceQueueUpserts []sqlite.OperatorQueueUpsertInput, lineage rebaseRuntimeLineage) rebaseRuntimeLineage {
	lineage = normalizeRuntimeLineage(lineage)
	switch {
	case len(sourceQueues) > 0:
		base := rebaseFollowupPayloadLineage(sourceQueues[0].payload)
		if len(sourceQueueUpserts) > 0 {
			if payload, err := actionCreateDecodeQueuePayload(sourceQueueUpserts[0].PayloadJSON); err == nil {
				base = rebaseFollowupPayloadLineage(payload)
			}
		}
		lineage.RootCauseID = firstNonEmpty(base.RootCauseID, lineage.RootCauseID)
		lineage.ProvenanceGroupID = firstNonEmpty(base.ProvenanceGroupID, lineage.ProvenanceGroupID)
		if len(lineage.ParentRefsJSON) == 0 {
			lineage.ParentRefsJSON = append([]string(nil), base.ParentRefsJSON...)
		}
	case rollbackFailureQueue != nil:
		base := rebaseRollbackFailurePayloadLineage(rollbackFailureQueue.rollbackFailurePayload)
		if len(sourceQueueUpserts) > 0 {
			if payload, err := actionCreateDecodeRollbackFailurePayload(sourceQueueUpserts[0].PayloadJSON); err == nil {
				base = rebaseRollbackFailurePayloadLineage(payload)
			}
		}
		lineage.RootCauseID = firstNonEmpty(base.RootCauseID, lineage.RootCauseID)
		lineage.ProvenanceGroupID = firstNonEmpty(base.ProvenanceGroupID, lineage.ProvenanceGroupID)
		if len(lineage.ParentRefsJSON) == 0 {
			lineage.ParentRefsJSON = append([]string(nil), base.ParentRefsJSON...)
		}
	}
	return normalizeRuntimeLineage(lineage)
}

func (h *Handler) actionCreateSourceQueueLineage(ctx context.Context, sourceQueue *actionCreateSourceQueue) (rebaseRuntimeLineage, error) {
	if sourceQueue == nil {
		return rebaseRuntimeLineage{}, nil
	}
	switch sourceQueue.kind {
	case actionCreateSourceQueueKindRollbackFailure:
		return h.rollbackFailureActionCreateSourceQueueLineage(ctx, sourceQueue.queue, sourceQueue.rollbackFailurePayload)
	default:
		return h.rebaseFollowupActionCreateSourceQueueLineage(ctx, sourceQueue.queue, sourceQueue.payload)
	}
}

func (h *Handler) rebaseFollowupActionCreateSourceQueueLineage(ctx context.Context, queue sqlite.OperatorQueueRecord, payload model.RebaseFollowupPayload) (rebaseRuntimeLineage, error) {
	lineage := rebaseFollowupPayloadLineage(payload)
	if strings.TrimSpace(payload.LastFailedActionID) == "" &&
		strings.TrimSpace(payload.RollbackReason) == "" &&
		strings.TrimSpace(payload.RebaseWorkflowStep) != rebaseWorkflowStepAwaitRestart {
		return lineage, nil
	}

	parentRefs := make([]string, 0, 2)
	queueEvent, ok, err := h.latestRuntimeEvent(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: strings.TrimSpace(queue.WorkspaceID),
		EntityType:  "operator_queue",
		EntityID:    strings.TrimSpace(queue.QueueID),
		Limit:       1,
	})
	if err != nil {
		return rebaseRuntimeLineage{}, err
	}
	if ok {
		lineage = actionCreateLineageWithFallback(lineage, queueEvent)
		parentRefs = append(parentRefs, strings.TrimSpace(queueEvent.EventID))
	}
	if failedActionID := strings.TrimSpace(payload.LastFailedActionID); failedActionID != "" {
		failedEvent, ok, err := h.latestRuntimeEvent(ctx, sqlite.RuntimeEventFilter{
			WorkspaceID: strings.TrimSpace(queue.WorkspaceID),
			EventType:   "action.resolved",
			EntityType:  "human_action",
			EntityID:    failedActionID,
			Limit:       1,
		})
		if err != nil {
			return rebaseRuntimeLineage{}, err
		}
		if ok {
			lineage = actionCreateLineageWithFallback(lineage, failedEvent)
			parentRefs = append(parentRefs, strings.TrimSpace(failedEvent.EventID))
		}
	}
	if normalizedParents := normalizeRuntimeLineage(rebaseRuntimeLineage{ParentRefsJSON: parentRefs}); len(normalizedParents.ParentRefsJSON) > 0 {
		lineage.ParentRefsJSON = normalizedParents.ParentRefsJSON
	}
	return normalizeRuntimeLineage(lineage), nil
}

func (h *Handler) rollbackFailureActionCreateSourceQueueLineage(ctx context.Context, queue sqlite.OperatorQueueRecord, payload model.RebaseRollbackFailurePayload) (rebaseRuntimeLineage, error) {
	lineage := rebaseRollbackFailurePayloadLineage(payload)
	if strings.TrimSpace(payload.LastFailedFollowupActionID) == "" {
		return lineage, nil
	}

	parentRefs := make([]string, 0, 2)
	queueEvent, ok, err := h.latestRuntimeEvent(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: strings.TrimSpace(queue.WorkspaceID),
		EntityType:  "operator_queue",
		EntityID:    strings.TrimSpace(queue.QueueID),
		Limit:       1,
	})
	if err != nil {
		return rebaseRuntimeLineage{}, err
	}
	if ok {
		lineage = actionCreateLineageWithFallback(lineage, queueEvent)
		parentRefs = append(parentRefs, strings.TrimSpace(queueEvent.EventID))
	}
	failedEvent, ok, err := h.latestRuntimeEvent(ctx, sqlite.RuntimeEventFilter{
		WorkspaceID: strings.TrimSpace(queue.WorkspaceID),
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    strings.TrimSpace(payload.LastFailedFollowupActionID),
		Limit:       1,
	})
	if err != nil {
		return rebaseRuntimeLineage{}, err
	}
	if ok {
		lineage = actionCreateLineageWithFallback(lineage, failedEvent)
		parentRefs = append(parentRefs, strings.TrimSpace(failedEvent.EventID))
	}
	if normalizedParents := normalizeRuntimeLineage(rebaseRuntimeLineage{ParentRefsJSON: parentRefs}); len(normalizedParents.ParentRefsJSON) > 0 {
		lineage.ParentRefsJSON = normalizedParents.ParentRefsJSON
	}
	return normalizeRuntimeLineage(lineage), nil
}

type actionStartParams struct {
	ActionID  string `json:"action_id"`
	StartedBy string `json:"started_by"`
	Comment   string `json:"comment"`
}

type actionPauseParams struct {
	ActionID string `json:"action_id"`
	PausedBy string `json:"paused_by"`
	Comment  string `json:"comment"`
}

func (h *Handler) actionStart(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p actionStartParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	p.StartedBy = strings.TrimSpace(p.StartedBy)

	action, err := h.store.GetHumanAction(ctx, p.ActionID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if strings.ToUpper(strings.TrimSpace(action.Status)) != humanActionStatusPending {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "action is not pending"}
	}

	sourceQueues, err := h.listLinkedRebaseFollowupQueuesForAction(ctx, action)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if len(sourceQueues) == 0 {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "action is not linked to a rebase follow-up queue"}
	}
	if len(sourceQueues) > 1 {
		return nil, &RPCError{Code: errCodeInternal, Message: "multiple linked rebase follow-up queues for action"}
	}

	for _, sourceQueue := range sourceQueues {
		if rpcErr := validateWorkflowQueueActorAuthority(sourceQueue.queue, p.StartedBy, "linked rebase follow-up", "started_by"); rpcErr != nil {
			return nil, rpcErr
		}
		if sourceQueue.payload.RebaseWorkflowState == rebaseWorkflowStateInProgress {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: "linked rebase follow-up is already in progress"}
		}
	}

	sourceQueue := sourceQueues[0]
	currentActionQueue, err := h.store.GetOperatorQueueItem(ctx, strings.TrimSpace(action.WorkspaceID), "", "action:"+strings.TrimSpace(action.ActionID))
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	startLineage, err := h.rebaseFollowupActionCreateSourceQueueLineage(ctx, sourceQueue.queue, sourceQueue.payload)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	startPayload := sourceQueue.payload
	applyRebaseFollowupPayloadLineage(&startPayload, startLineage)
	actionStartedPayload := map[string]any{
		"action_id":      action.ActionID,
		"task_id":        action.TaskID,
		"started_by":     strings.TrimSpace(p.StartedBy),
		"comment":        strings.TrimSpace(p.Comment),
		"workflow_state": rebaseWorkflowStateInProgress,
		"workflow_step":  rebaseWorkflowStepOperatorClaimed,
		"source_queue_id": func() string {
			return sourceQueue.queue.QueueID
		}(),
		"source_queue_key": func() string {
			return sourceQueue.queue.QueueKey
		}(),
	}
	runtimePayloadApplyLineage(actionStartedPayload, startLineage)
	if payloadWithContext, rpcErr := h.attachHumanActionPromptContext(ctx, action.WorkspaceID, "action.start", actionStartedPayload); rpcErr != nil {
		return nil, rpcErr
	} else {
		actionStartedPayload = payloadWithContext
	}
	if h.beforeActionStartSyncOverride != nil {
		h.beforeActionStartSyncOverride(ctx)
	}
	startResult, syncErr := h.syncLinkedActionSourceQueueStartWithActionEvent(ctx, sourceQueue.queue, &currentActionQueue, startPayload, action, p.StartedBy, p.Comment, actionRuntimeEventInputWithLineage(sqlite.RuntimeEventInput{
		WorkspaceID: action.WorkspaceID,
		EventType:   "action.started",
		EntityType:  "human_action",
		EntityID:    action.ActionID,
		ActorType:   "operator",
		ActorID:     strings.TrimSpace(p.StartedBy),
		AgentID:     action.AgentID,
		TaskID:      action.TaskID,
		PayloadJSON: string(mustJSON(actionStartedPayload)),
	}, startLineage))
	if syncErr != nil {
		if isHumanActionWorkflowConflictError(syncErr) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: syncErr.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(syncErr, "action.start")
	}
	if strings.TrimSpace(startResult.QueueEvent.Event.EventID) != "" {
		h.publishOperatorQueueEventRecord(startResult.QueueEvent.Event, "workspace.ops.updated", startResult.QueueEvent.Record)
	}
	if strings.TrimSpace(startResult.RuntimeEvent.EventID) != "" {
		h.publishRuntimeEventRecord(startResult.RuntimeEvent, "Action started: "+action.Title)
	}

	return map[string]any{
		"action_id":        action.ActionID,
		"status":           action.Status,
		"workflow_state":   rebaseWorkflowStateInProgress,
		"workflow_step":    rebaseWorkflowStepOperatorClaimed,
		"source_queue_id":  startResult.QueueEvent.Record.QueueID,
		"source_queue_key": startResult.QueueEvent.Record.QueueKey,
	}, nil
}

func (h *Handler) actionPause(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p actionPauseParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}
	p.PausedBy = strings.TrimSpace(p.PausedBy)

	action, err := h.store.GetHumanAction(ctx, p.ActionID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if strings.ToUpper(strings.TrimSpace(action.Status)) != humanActionStatusPending {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "action is not pending"}
	}

	sourceQueues, err := h.listLinkedRebaseFollowupQueuesForAction(ctx, action)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if len(sourceQueues) == 0 {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: "action is not linked to a rebase follow-up queue"}
	}
	if len(sourceQueues) > 1 {
		return nil, &RPCError{Code: errCodeInternal, Message: "multiple linked rebase follow-up queues for action"}
	}
	for _, sourceQueue := range sourceQueues {
		if rpcErr := validateWorkflowQueueActorAuthority(sourceQueue.queue, p.PausedBy, "linked rebase follow-up", "paused_by"); rpcErr != nil {
			return nil, rpcErr
		}
		if sourceQueue.payload.RebaseWorkflowState != linkedActionSourceQueueStartedState() {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: "linked rebase follow-up is not in progress"}
		}
	}

	sourceQueue := sourceQueues[0]
	currentActionQueue, err := h.store.GetOperatorQueueItem(ctx, strings.TrimSpace(action.WorkspaceID), "", "action:"+strings.TrimSpace(action.ActionID))
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	pauseLineage, err := h.rebaseFollowupActionCreateSourceQueueLineage(ctx, sourceQueue.queue, sourceQueue.payload)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	pausePayload := sourceQueue.payload
	applyRebaseFollowupPayloadLineage(&pausePayload, pauseLineage)
	actionPausedPayload := map[string]any{
		"action_id":        action.ActionID,
		"task_id":          action.TaskID,
		"paused_by":        strings.TrimSpace(p.PausedBy),
		"comment":          strings.TrimSpace(p.Comment),
		"workflow_state":   linkedActionSourceQueuePausedState(),
		"workflow_step":    linkedActionSourceQueuePausedStep(),
		"source_queue_id":  sourceQueue.queue.QueueID,
		"source_queue_key": sourceQueue.queue.QueueKey,
	}
	runtimePayloadApplyLineage(actionPausedPayload, pauseLineage)
	if payloadWithContext, rpcErr := h.attachHumanActionPromptContext(ctx, action.WorkspaceID, "action.pause", actionPausedPayload); rpcErr != nil {
		return nil, rpcErr
	} else {
		actionPausedPayload = payloadWithContext
	}
	if h.beforeActionPauseSyncOverride != nil {
		h.beforeActionPauseSyncOverride(ctx)
	}
	pauseResult, syncErr := h.syncLinkedActionSourceQueuePauseWithActionEvent(ctx, sourceQueue.queue, &currentActionQueue, pausePayload, action, p.PausedBy, p.Comment, actionRuntimeEventInputWithLineage(sqlite.RuntimeEventInput{
		WorkspaceID: action.WorkspaceID,
		EventType:   "action.paused",
		EntityType:  "human_action",
		EntityID:    action.ActionID,
		ActorType:   "operator",
		ActorID:     strings.TrimSpace(p.PausedBy),
		AgentID:     action.AgentID,
		TaskID:      action.TaskID,
		PayloadJSON: string(mustJSON(actionPausedPayload)),
	}, pauseLineage))
	if syncErr != nil {
		if isHumanActionWorkflowConflictError(syncErr) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: syncErr.Error()}
		}
		return nil, rpcErrorFromStoreAuthority(syncErr, "action.pause")
	}
	if strings.TrimSpace(pauseResult.QueueEvent.Event.EventID) != "" {
		h.publishOperatorQueueEventRecord(pauseResult.QueueEvent.Event, "workspace.ops.updated", pauseResult.QueueEvent.Record)
	}
	if strings.TrimSpace(pauseResult.RuntimeEvent.EventID) != "" {
		h.publishRuntimeEventRecord(pauseResult.RuntimeEvent, "Action paused: "+action.Title)
	}

	return map[string]any{
		"action_id":        action.ActionID,
		"status":           action.Status,
		"workflow_state":   linkedActionSourceQueuePausedState(),
		"workflow_step":    linkedActionSourceQueuePausedStep(),
		"source_queue_id":  pauseResult.QueueEvent.Record.QueueID,
		"source_queue_key": pauseResult.QueueEvent.Record.QueueKey,
	}, nil
}

func (h *Handler) actionResolve(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p actionResolveParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}

	action, err := h.store.GetHumanAction(ctx, p.ActionID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return h.resolveActionWithEffects(ctx, action, p, actionResolveOptions{})
}

func (h *Handler) resolveActionWithEffects(ctx context.Context, action sqlite.HumanActionRecord, p actionResolveParams, opts actionResolveOptions) (any, *RPCError) {
	p.ActionID = strings.TrimSpace(firstNonEmpty(p.ActionID, action.ActionID))
	p.Resolution = strings.ToUpper(strings.TrimSpace(p.Resolution))
	p.Comment = strings.TrimSpace(p.Comment)
	p.ResolvedBy = strings.TrimSpace(p.ResolvedBy)
	rollbackReason := strings.TrimSpace(opts.RollbackReason)

	sourceQueues, err := h.listLinkedRebaseFollowupQueuesForAction(ctx, action)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	rollbackFailureQueue, rollbackFailureLinked, err := h.linkedRollbackFailureQueueFromActionQueue(ctx, action)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	if len(sourceQueues) > 1 {
		return nil, &RPCError{Code: errCodeInternal, Message: "multiple linked rebase follow-up queues for action"}
	}
	if rollbackFailureLinked && len(sourceQueues) > 0 {
		return nil, &RPCError{Code: errCodeInternal, Message: "action is linked to multiple source queue workflows"}
	}
	if rollbackReason == "" {
		for _, sourceQueue := range sourceQueues {
			if rpcErr := validateWorkflowQueueActorAuthority(sourceQueue.queue, p.ResolvedBy, "linked rebase follow-up", "resolved_by"); rpcErr != nil {
				return nil, rpcErr
			}
		}
		if rollbackFailureLinked {
			if rpcErr := validateWorkflowQueueActorAuthority(rollbackFailureQueue.queue, p.ResolvedBy, "rollback-failure follow-up", "resolved_by"); rpcErr != nil {
				return nil, rpcErr
			}
		}
		if len(sourceQueues) == 0 && !rollbackFailureLinked {
			if rpcErr := h.validateStandaloneHumanActionResolveAuthority(ctx, action, p.ResolvedBy); rpcErr != nil {
				return nil, rpcErr
			}
		}
	}
	resolvedAction := action
	resolvedAction.Status = strings.ToUpper(strings.TrimSpace(p.Resolution))
	resolvedAction.ResolutionComment = strings.TrimSpace(p.Comment)
	resolvedAction.ResolvedBy = strings.TrimSpace(p.ResolvedBy)
	var currentActionQueue *sqlite.OperatorQueueRecord
	actionQueueRecord, err := h.store.GetOperatorQueueItem(ctx, strings.TrimSpace(action.WorkspaceID), "", "action:"+strings.TrimSpace(action.ActionID))
	if err != nil {
		if isOperatorQueueItemNotFoundError(err) {
			if len(sourceQueues) > 0 || rollbackFailureLinked {
				return nil, &RPCError{Code: errCodeInternal, Message: "linked action queue is missing"}
			}
		} else {
			return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
		}
	} else {
		currentActionQueue = &actionQueueRecord
	}

	actionQueueInput := humanActionResolutionQueueInput(resolvedAction, currentActionQueue, p.ResolvedBy, p.Resolution, p.Comment)
	actionQueueInputPtr := &actionQueueInput
	if currentActionQueue == nil {
		actionQueueInputPtr = nil
	}
	sourceQueueInputs := make([]sqlite.OperatorQueueResolveInput, 0, len(sourceQueues))
	sourceQueueUpserts := make([]sqlite.OperatorQueueUpsertInput, 0, len(sourceQueues))
	for _, sourceQueue := range sourceQueues {
		if strings.EqualFold(strings.TrimSpace(p.Resolution), humanActionStatusFailed) && sourceQueue.payload.IsRebaseFollowup(sourceQueue.queue.QueueKey) {
			sourceQueueUpserts = append(sourceQueueUpserts, linkedActionSourceQueueFailureRollbackUpsertInput(sourceQueue.queue, sourceQueue.payload, resolvedAction, p.ResolvedBy, p.Resolution, p.Comment, rollbackReason, opts.Lineage))
			continue
		}
		sourceQueueInputs = append(sourceQueueInputs, linkedActionSourceQueueResolutionInput(sourceQueue.queue, sourceQueue.payload, resolvedAction, p.ResolvedBy, p.Resolution))
	}
	if rollbackFailureLinked && rollbackFailureQueue != nil {
		if strings.EqualFold(strings.TrimSpace(p.Resolution), humanActionStatusFailed) {
			sourceQueueUpserts = append(sourceQueueUpserts, linkedRollbackFailureSourceQueueFailureUpsertInput(rollbackFailureQueue.queue, rollbackFailureQueue.rollbackFailurePayload, resolvedAction, p.ResolvedBy, p.Comment, opts.Lineage))
		} else {
			sourceQueueInputs = append(sourceQueueInputs, linkedRollbackFailureSourceQueueResolutionInput(rollbackFailureQueue.queue, rollbackFailureQueue.rollbackFailurePayload, resolvedAction, p.ResolvedBy, p.Resolution))
		}
	}
	responseWorkflowState := ""
	responseWorkflowStep := ""
	responseSourceQueueID := ""
	responseSourceQueueKey := ""
	hasResponseSourceQueue := false
	if len(sourceQueues) > 0 {
		responseSourceQueueID = sourceQueues[0].queue.QueueID
		responseSourceQueueKey = sourceQueues[0].queue.QueueKey
		responseWorkflowState = linkedActionSourceQueueWorkflowState(resolvedAction.Status)
		responseWorkflowStep = linkedActionSourceQueueWorkflowStep(resolvedAction.Status)
		if strings.EqualFold(strings.TrimSpace(p.Resolution), humanActionStatusFailed) {
			responseWorkflowState = linkedActionSourceQueuePausedState()
			responseWorkflowStep = linkedActionSourceQueuePausedStep()
		}
		hasResponseSourceQueue = true
	} else if rollbackFailureLinked && rollbackFailureQueue != nil {
		responseSourceQueueID = rollbackFailureQueue.queue.QueueID
		responseSourceQueueKey = rollbackFailureQueue.queue.QueueKey
		hasResponseSourceQueue = true
	}
	if rollbackReason == "" && strings.EqualFold(strings.TrimSpace(p.Resolution), humanActionStatusFailed) {
		if len(sourceQueues) > 0 {
			rollbackReason = linkedActionSourceQueueRollbackReason(p.Resolution)
		} else if rollbackFailureLinked && rollbackFailureQueue != nil {
			rollbackReason = linkedRollbackFailureSourceQueueRollbackReason(p.Resolution)
		}
	}

	var rollbackLineageSource *actionCreateSourceQueue
	if rollbackFailureLinked && rollbackFailureQueue != nil {
		rollbackLineageSource = rollbackFailureQueue
	}
	resolveLineage := actionResolveLineageFromInputs(sourceQueues, rollbackLineageSource, sourceQueueUpserts, opts.Lineage)
	actionResolvedPayload := map[string]any{
		"action_id":   p.ActionID,
		"task_id":     action.TaskID,
		"resolution":  p.Resolution,
		"comment":     p.Comment,
		"resolved_by": p.ResolvedBy,
	}
	if rollbackReason != "" {
		actionResolvedPayload["rollback_reason"] = rollbackReason
	}
	if hasResponseSourceQueue {
		actionResolvedPayload["source_queue_id"] = responseSourceQueueID
		actionResolvedPayload["source_queue_key"] = responseSourceQueueKey
	}
	if responseWorkflowState != "" {
		actionResolvedPayload["workflow_state"] = responseWorkflowState
	}
	if responseWorkflowStep != "" {
		actionResolvedPayload["workflow_step"] = responseWorkflowStep
	}
	runtimePayloadApplyLineage(actionResolvedPayload, resolveLineage)
	if payloadWithContext, rpcErr := h.attachHumanActionPromptContext(ctx, action.WorkspaceID, "action.resolve", actionResolvedPayload); rpcErr != nil {
		return nil, rpcErr
	} else {
		actionResolvedPayload = payloadWithContext
	}
	resolveRuntimeInput := actionRuntimeEventInputWithLineage(sqlite.RuntimeEventInput{
		WorkspaceID: action.WorkspaceID,
		EventType:   "action.resolved",
		EntityType:  "human_action",
		EntityID:    p.ActionID,
		ActorType:   "operator",
		ActorID:     p.ResolvedBy,
		AgentID:     action.AgentID,
		TaskID:      action.TaskID,
		PayloadJSON: string(mustJSON(actionResolvedPayload)),
	}, resolveLineage)
	if h.beforeActionResolveQueueEffectsOverride != nil {
		h.beforeActionResolveQueueEffectsOverride(ctx)
	}
	resolveResult, err := h.store.ResolveHumanActionWithQueueEffects(ctx, p.ActionID, p.Resolution, p.Comment, p.ResolvedBy, actionQueueInputPtr, sourceQueueInputs, sourceQueueUpserts, &resolveRuntimeInput, action)
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "action.resolve"); rpcErr != nil {
			return nil, rpcErr
		}
		if isHumanActionWorkflowConflictError(err) {
			return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	action = resolveResult.Action

	if action.AgentID != "" {
		resultText := "Action " + humanActionStatusCompleted
		if p.Resolution == humanActionStatusFailed {
			resultText = "Action " + humanActionStatusFailed
		}
		msgContent := resultText + ": " + action.Title
		if p.Comment != "" {
			msgContent += "\nComment: " + p.Comment
		}
		messageID, messageEvent, sendErr := h.store.SendMessageWithAuthorityEvent(ctx, sqlite.MessageSendInput{
			WorkspaceID: action.WorkspaceID,
			FromAgentID: p.ResolvedBy,
			ToAgentID:   action.AgentID,
			Channel:     "action:" + p.ActionID,
			Content:     msgContent,
		})
		if sendErr != nil {
			log.Printf("[human-action] resolution message send failed action=%s workspace=%s err=%v", p.ActionID, action.WorkspaceID, sendErr)
		} else if strings.TrimSpace(messageID) != "" {
			liveAgentID := action.AgentID
			h.publishRuntimeEventRecordAlias(messageEvent, "agent.message", &liveAgentID, "", "Action resolution for "+p.ActionID)
		}
	}
	if resolveResult.ActionQueue != nil {
		record := resolveResult.ActionQueue.Record
		event := resolveResult.ActionQueue.Event
		if strings.TrimSpace(event.EventID) != "" {
			h.publishOperatorQueueEventRecord(event, "workspace.ops.resolved", record)
		} else {
			log.Printf("[human-action] queue resolve returned no runtime event action=%s workspace=%s", p.ActionID, action.WorkspaceID)
		}
	}
	for _, synced := range resolveResult.LinkedSourceQueue {
		if strings.TrimSpace(synced.Event.EventID) != "" {
			h.publishOperatorQueueEventRecord(synced.Event, operatorQueueRuntimeEventLiveType(synced.Event.EventType), synced.Record)
		} else {
			log.Printf("[human-action] source queue sync returned no runtime event action=%s workspace=%s queue=%s", p.ActionID, action.WorkspaceID, synced.Record.QueueID)
		}
	}
	if resolveResult.ActionEvent != nil && strings.TrimSpace(resolveResult.ActionEvent.EventID) != "" {
		h.publishRuntimeEventRecord(*resolveResult.ActionEvent, p.Resolution+": "+action.Title)
	}

	if len(sourceQueues) > 0 && len(resolveResult.LinkedSourceQueue) > 0 {
		responseSourceQueueID = resolveResult.LinkedSourceQueue[0].Record.QueueID
		responseSourceQueueKey = resolveResult.LinkedSourceQueue[0].Record.QueueKey
		if payload, err := actionCreateDecodeQueuePayload(resolveResult.LinkedSourceQueue[0].Record.PayloadJSON); err == nil {
			if strings.TrimSpace(payload.RebaseWorkflowState) != "" {
				responseWorkflowState = strings.TrimSpace(payload.RebaseWorkflowState)
			}
			if strings.TrimSpace(payload.RebaseWorkflowStep) != "" {
				responseWorkflowStep = strings.TrimSpace(payload.RebaseWorkflowStep)
			}
		}
	}
	if rollbackFailureLinked && rollbackFailureQueue != nil && len(resolveResult.LinkedSourceQueue) > 0 {
		responseSourceQueueID = resolveResult.LinkedSourceQueue[0].Record.QueueID
		responseSourceQueueKey = resolveResult.LinkedSourceQueue[0].Record.QueueKey
	}

	response := map[string]any{
		"action_id":  p.ActionID,
		"resolution": p.Resolution,
		"task_id":    action.TaskID,
		"status":     action.Status,
	}
	if hasResponseSourceQueue {
		response["source_queue_id"] = responseSourceQueueID
		response["source_queue_key"] = responseSourceQueueKey
	}
	if responseWorkflowState != "" {
		response["workflow_state"] = responseWorkflowState
	}
	if responseWorkflowStep != "" {
		response["workflow_step"] = responseWorkflowStep
	}
	return response, nil
}

func (h *Handler) syncHumanActionOperatorQueue(ctx context.Context, action sqlite.HumanActionRecord) (sqlite.OperatorQueueRecord, sqlite.RuntimeEventRecord, error) {
	return h.store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       action.WorkspaceID,
		QueueKey:          "action:" + action.ActionID,
		QueueType:         "FOLLOW_UP",
		Title:             action.Title,
		Summary:           strings.TrimSpace(action.Description),
		Details:           strings.TrimSpace(action.Description),
		AssignedTo:        strings.TrimSpace(action.AssignedTo),
		Urgency:           humanActionQueueUrgency(action.Blocking),
		SourceKind:        "human_action",
		SourceID:          action.ActionID,
		TaskID:            action.TaskID,
		AgentID:           action.AgentID,
		KeepSessionActive: action.Blocking,
	})
}

func (h *Handler) syncHumanActionResolution(ctx context.Context, action sqlite.HumanActionRecord, resolvedBy, resolution, comment string) (sqlite.OperatorQueueRecord, sqlite.RuntimeEventRecord, error) {
	currentActionQueue, err := h.store.GetOperatorQueueItem(ctx, strings.TrimSpace(action.WorkspaceID), "", "action:"+strings.TrimSpace(action.ActionID))
	if err != nil {
		return sqlite.OperatorQueueRecord{}, sqlite.RuntimeEventRecord{}, err
	}
	return h.store.ResolveOperatorQueueItemWithEvent(ctx, humanActionResolutionQueueInput(action, &currentActionQueue, resolvedBy, resolution, comment))
}

func (h *Handler) listLinkedRebaseFollowupQueuesForAction(ctx context.Context, action sqlite.HumanActionRecord) ([]actionCreateSourceQueue, error) {
	if explicit, ok, err := h.linkedRebaseFollowupQueueFromActionQueue(ctx, action); err != nil {
		return nil, err
	} else if ok {
		return []actionCreateSourceQueue{*explicit}, nil
	}
	filter := sqlite.OperatorQueueFilter{
		WorkspaceID: action.WorkspaceID,
		QueueType:   "FOLLOW_UP",
		Status:      "OPEN",
		Limit:       200,
	}
	if trimmedTaskID := strings.TrimSpace(action.TaskID); trimmedTaskID != "" {
		filter.TaskID = trimmedTaskID
	}
	items, err := h.store.ListOperatorQueueItems(ctx, filter)
	if err != nil {
		return nil, err
	}
	linked := make([]actionCreateSourceQueue, 0, 1)
	for _, item := range items {
		rollbackPayload, rollbackErr := actionCreateDecodeRollbackFailurePayload(item.PayloadJSON)
		if rollbackErr == nil && rollbackPayload.IsRollbackFailure(item.QueueKey) {
			continue
		}
		payload, err := actionCreateDecodeQueuePayload(item.PayloadJSON)
		if err != nil {
			log.Printf("[human-action] skip malformed source queue payload workspace=%s queue=%s err=%v", action.WorkspaceID, item.QueueID, err)
			continue
		}
		if !payload.IsRebaseFollowup(item.QueueKey) {
			continue
		}
		if payload.ActionID != strings.TrimSpace(action.ActionID) {
			continue
		}
		linked = append(linked, actionCreateSourceQueue{queue: item, payload: payload})
	}
	return linked, nil
}

func (h *Handler) linkedRebaseFollowupQueueFromActionQueue(ctx context.Context, action sqlite.HumanActionRecord) (*actionCreateSourceQueue, bool, error) {
	actionQueue, err := h.store.GetOperatorQueueItem(ctx, action.WorkspaceID, "", "action:"+strings.TrimSpace(action.ActionID))
	if err != nil {
		if isOperatorQueueItemNotFoundError(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	payload, err := actionCreateDecodeQueuePayload(actionQueue.PayloadJSON)
	if err != nil {
		return nil, false, err
	}
	if strings.TrimSpace(payload.SourceQueueID) == "" && strings.TrimSpace(payload.SourceQueueKey) == "" {
		return nil, false, nil
	}
	queue, err := h.store.GetOperatorQueueItem(ctx, action.WorkspaceID, strings.TrimSpace(payload.SourceQueueID), strings.TrimSpace(payload.SourceQueueKey))
	if err != nil {
		return nil, false, err
	}
	rollbackPayload, rollbackErr := actionCreateDecodeRollbackFailurePayload(queue.PayloadJSON)
	if rollbackErr == nil && rollbackPayload.IsRollbackFailure(queue.QueueKey) {
		return nil, false, nil
	}
	linkedPayload, err := actionCreateDecodeQueuePayload(queue.PayloadJSON)
	if err != nil {
		return nil, false, err
	}
	if !linkedPayload.IsRebaseFollowup(queue.QueueKey) {
		return nil, false, errors.New("linked source queue is not a rebase follow-up")
	}
	if linkedPayload.LinkedActionExists() && strings.TrimSpace(linkedPayload.ActionID) != strings.TrimSpace(action.ActionID) {
		return nil, false, fmt.Errorf("linked source queue points to different action %s", strings.TrimSpace(linkedPayload.ActionID))
	}
	return &actionCreateSourceQueue{kind: actionCreateSourceQueueKindRebaseFollowup, queue: queue, payload: linkedPayload}, true, nil
}

func (h *Handler) linkedRollbackFailureQueueFromActionQueue(ctx context.Context, action sqlite.HumanActionRecord) (*actionCreateSourceQueue, bool, error) {
	actionQueue, err := h.store.GetOperatorQueueItem(ctx, action.WorkspaceID, "", "action:"+strings.TrimSpace(action.ActionID))
	if err != nil {
		if isOperatorQueueItemNotFoundError(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	payload, err := actionCreateDecodeQueuePayload(actionQueue.PayloadJSON)
	if err != nil {
		return nil, false, err
	}
	if strings.TrimSpace(payload.SourceQueueID) == "" && strings.TrimSpace(payload.SourceQueueKey) == "" {
		return nil, false, nil
	}
	queue, err := h.store.GetOperatorQueueItem(ctx, action.WorkspaceID, strings.TrimSpace(payload.SourceQueueID), strings.TrimSpace(payload.SourceQueueKey))
	if err != nil {
		return nil, false, err
	}
	linkedPayload, err := actionCreateDecodeRollbackFailurePayload(queue.PayloadJSON)
	if err != nil {
		return nil, false, err
	}
	if !linkedPayload.IsRollbackFailure(queue.QueueKey) {
		return nil, false, nil
	}
	if linkedPayload.FollowupActionID != "" && strings.TrimSpace(linkedPayload.FollowupActionID) != strings.TrimSpace(action.ActionID) {
		return nil, false, fmt.Errorf("linked rollback-failure queue points to different action %s", strings.TrimSpace(linkedPayload.FollowupActionID))
	}
	return &actionCreateSourceQueue{kind: actionCreateSourceQueueKindRollbackFailure, queue: queue, rollbackFailurePayload: linkedPayload}, true, nil
}

func (h *Handler) syncLinkedActionSourceQueueResolution(ctx context.Context, queue sqlite.OperatorQueueRecord, payload model.RebaseFollowupPayload, action sqlite.HumanActionRecord, resolvedBy, resolution string) (sqlite.OperatorQueueRecord, sqlite.RuntimeEventRecord, error) {
	return h.store.ResolveOperatorQueueItemWithEvent(ctx, linkedActionSourceQueueResolutionInput(queue, payload, action, resolvedBy, resolution))
}

func linkedActionSourceQueueResolutionInput(queue sqlite.OperatorQueueRecord, payload model.RebaseFollowupPayload, action sqlite.HumanActionRecord, resolvedBy, resolution string) sqlite.OperatorQueueResolveInput {
	payload.ActionID = strings.TrimSpace(action.ActionID)
	payload.ActionQueueKey = "action:" + strings.TrimSpace(action.ActionID)
	payload.ActionStatus = strings.TrimSpace(action.Status)
	payload.ActionTitle = strings.TrimSpace(action.Title)
	payload.ActionAssignedTo = strings.TrimSpace(action.AssignedTo)
	payload.RebaseWorkflowState = linkedActionSourceQueueWorkflowState(action.Status)
	payload.RebaseWorkflowStep = linkedActionSourceQueueWorkflowStep(action.Status)
	payload.Normalize()

	details := strings.TrimSpace(queue.Details)
	details = actionQueueUpsertDetailLine(details, "Linked action:", strings.TrimSpace(action.ActionID))
	details = actionQueueUpsertDetailLine(details, "Action queue:", "action:"+strings.TrimSpace(action.ActionID))
	details = actionQueueUpsertDetailLine(details, "Workflow state:", linkedActionSourceQueueWorkflowState(action.Status))
	details = actionQueueUpsertDetailLine(details, "Workflow step:", linkedActionSourceQueueWorkflowStep(action.Status))

	summary := strings.TrimSpace(queue.Summary)
	actionSummary := strings.TrimSpace(firstNonEmpty(action.Title, action.ActionID))
	if summary == "" {
		summary = actionSummary
	}
	if actionSummary != "" && !strings.Contains(summary, strings.TrimSpace(action.ActionID)) {
		summary += " | Action: " + actionSummary + " (" + strings.TrimSpace(action.ActionID) + ")"
	}
	switch strings.ToUpper(strings.TrimSpace(action.Status)) {
	case humanActionStatusCompleted:
		summary = actionQueueSummaryWithWorkflowMarker(summary, "Completed")
	case humanActionStatusFailed:
		summary = actionQueueSummaryWithWorkflowMarker(summary, "Failed")
	default:
		summary = actionQueueSummaryWithWorkflowMarker(summary, "Resolved")
	}

	return sqlite.OperatorQueueResolveInput{
		WorkspaceID:             queue.WorkspaceID,
		QueueID:                 queue.QueueID,
		Status:                  "RESOLVED",
		ResolvedBy:              resolvedBy,
		Resolution:              linkedActionSourceQueueResolution(action.ActionID, resolution),
		Summary:                 strings.TrimSpace(summary),
		Details:                 strings.TrimSpace(details),
		PayloadJSON:             string(mustJSON(payload)),
		RequireCurrentStatus:    "OPEN",
		RequireCurrentRevision:  queue.Revision,
		RequireCurrentUpdatedAt: strings.TrimSpace(queue.UpdatedAt),
	}
}

func humanActionResolutionQueueInput(action sqlite.HumanActionRecord, currentQueue *sqlite.OperatorQueueRecord, resolvedBy, resolution, comment string) sqlite.OperatorQueueResolveInput {
	input := sqlite.OperatorQueueResolveInput{
		WorkspaceID:          action.WorkspaceID,
		QueueKey:             "action:" + action.ActionID,
		Status:               "RESOLVED",
		ResolvedBy:           resolvedBy,
		Resolution:           strings.TrimSpace(firstNonEmpty(comment, resolution, action.Status)),
		RequireCurrentStatus: "OPEN",
	}
	if currentQueue != nil {
		input.QueueID = strings.TrimSpace(currentQueue.QueueID)
		input.RequireCurrentRevision = currentQueue.Revision
		input.RequireCurrentUpdatedAt = strings.TrimSpace(currentQueue.UpdatedAt)
	}
	return input
}

func linkedActionSourceQueueResolution(actionID, resolution string) string {
	base := "linked_action_resolved"
	switch strings.ToUpper(strings.TrimSpace(resolution)) {
	case humanActionStatusCompleted:
		base = "linked_action_completed"
	case humanActionStatusFailed:
		base = "linked_action_failed"
	}
	actionID = strings.TrimSpace(actionID)
	if actionID == "" {
		return base
	}
	return base + ":" + actionID
}

func linkedRollbackFailureSourceQueueResolution(actionID, resolution string) string {
	base := "followup_action_resolved"
	switch strings.ToUpper(strings.TrimSpace(resolution)) {
	case humanActionStatusCompleted:
		base = "followup_action_completed"
	case humanActionStatusFailed:
		base = "followup_action_failed"
	}
	actionID = strings.TrimSpace(actionID)
	if actionID == "" {
		return base
	}
	return base + ":" + actionID
}

func linkedActionSourceQueueRollbackReason(resolution string) string {
	switch strings.ToUpper(strings.TrimSpace(resolution)) {
	case humanActionStatusFailed:
		return "linked_action_failed"
	default:
		return "linked_action_resolved"
	}
}

func linkedRollbackFailureSourceQueueRollbackReason(resolution string) string {
	switch strings.ToUpper(strings.TrimSpace(resolution)) {
	case humanActionStatusFailed:
		return "followup_action_failed"
	default:
		return "followup_action_resolved"
	}
}

// Canonical workflow authority for promoted rebase actions:
// - human_actions.status stores only coarse action lifecycle state
// - linked operator-queue payload stores the fine-grained rebase workflow state/step
// - dashboard and runtime-event surfaces mirror this contract and must not invent alternate workflow truth
func linkedActionSourceQueueWorkflowState(actionStatus string) string {
	switch strings.ToUpper(strings.TrimSpace(actionStatus)) {
	case humanActionStatusCompleted:
		return rebaseWorkflowStateCompleted
	case humanActionStatusFailed:
		return rebaseWorkflowStateFailed
	default:
		return rebaseWorkflowStateClaimed
	}
}

func linkedActionSourceQueueWorkflowStep(actionStatus string) string {
	switch strings.ToUpper(strings.TrimSpace(actionStatus)) {
	case humanActionStatusCompleted, humanActionStatusFailed:
		return rebaseWorkflowStepActionResolved
	default:
		return rebaseWorkflowStepAwaitResolution
	}
}

func linkedActionSourceQueueStartedState() string {
	return rebaseWorkflowStateInProgress
}

func linkedActionSourceQueueStartedStep() string {
	return rebaseWorkflowStepOperatorClaimed
}

func linkedActionSourceQueuePausedState() string {
	return rebaseWorkflowStateClaimed
}

func linkedActionSourceQueuePausedStep() string {
	return rebaseWorkflowStepAwaitRestart
}

func linkedActionSourceQueueFailureRollbackUpsertInput(queue sqlite.OperatorQueueRecord, payload model.RebaseFollowupPayload, action sqlite.HumanActionRecord, resolvedBy, resolution, comment, rollbackReason string, lineage rebaseRuntimeLineage) sqlite.OperatorQueueUpsertInput {
	reason := strings.TrimSpace(rollbackReason)
	if reason == "" {
		reason = linkedActionSourceQueueRollbackReason(resolution)
	}
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
	payload.RebaseWorkflowState = linkedActionSourceQueuePausedState()
	payload.RebaseWorkflowStep = linkedActionSourceQueuePausedStep()
	applyRebaseFollowupPayloadLineage(&payload, lineage)
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
	details = actionQueueUpsertDetailLine(details, "Workflow state:", linkedActionSourceQueuePausedState())
	details = actionQueueUpsertDetailLine(details, "Workflow step:", linkedActionSourceQueuePausedStep())
	if resolvedBy != "" {
		details = actionQueueUpsertDetailLine(details, "Failed by:", strings.TrimSpace(resolvedBy))
	}
	if strings.TrimSpace(comment) != "" {
		details = actionQueueUpsertDetailLine(details, "Failure comment:", strings.TrimSpace(comment))
	}

	summary := actionQueueSummaryRemoveActionLink(strings.TrimSpace(queue.Summary))
	summary = actionQueueSummaryWithWorkflowMarker(summary, "Claimed")

	return sqlite.OperatorQueueUpsertInput{
		QueueID:                 queue.QueueID,
		WorkspaceID:             queue.WorkspaceID,
		QueueKey:                queue.QueueKey,
		QueueType:               queue.QueueType,
		Title:                   queue.Title,
		Summary:                 strings.TrimSpace(summary),
		Details:                 details,
		PayloadJSON:             string(mustJSON(payload)),
		AssignedTo:              strings.TrimSpace(queue.AssignedTo),
		Urgency:                 queue.Urgency,
		SourceKind:              queue.SourceKind,
		SourceID:                queue.SourceID,
		TaskID:                  firstNonEmpty(strings.TrimSpace(queue.TaskID), strings.TrimSpace(action.TaskID)),
		SessionID:               queue.SessionID,
		AgentID:                 firstNonEmpty(strings.TrimSpace(queue.AgentID), strings.TrimSpace(action.AgentID)),
		KeepSessionActive:       queue.KeepSessionActive,
		DueAt:                   actionCreateOptionalString(queue.DueAt),
		RequireCurrentStatus:    "OPEN",
		RequireCurrentUpdatedAt: strings.TrimSpace(queue.UpdatedAt),
	}
}

func linkedRollbackFailureSourceQueueResolutionInput(queue sqlite.OperatorQueueRecord, payload model.RebaseRollbackFailurePayload, action sqlite.HumanActionRecord, resolvedBy, resolution string) sqlite.OperatorQueueResolveInput {
	payload.FollowupActionID = strings.TrimSpace(action.ActionID)
	payload.FollowupActionQueueKey = "action:" + strings.TrimSpace(action.ActionID)
	payload.FollowupActionStatus = strings.TrimSpace(action.Status)
	payload.Normalize()

	details := strings.TrimSpace(queue.Details)
	details = actionQueueUpsertDetailLine(details, "Follow-up action:", strings.TrimSpace(action.ActionID))
	details = actionQueueUpsertDetailLine(details, "Follow-up action queue:", "action:"+strings.TrimSpace(action.ActionID))
	details = actionQueueUpsertDetailLine(details, "Follow-up action status:", strings.TrimSpace(action.Status))

	summary := strings.TrimSpace(queue.Summary)
	actionSummary := strings.TrimSpace(firstNonEmpty(action.Title, action.ActionID))
	if summary == "" {
		summary = actionSummary
	}
	if actionSummary != "" && !strings.Contains(summary, strings.TrimSpace(action.ActionID)) {
		summary += " | Action: " + actionSummary + " (" + strings.TrimSpace(action.ActionID) + ")"
	}
	switch strings.ToUpper(strings.TrimSpace(action.Status)) {
	case humanActionStatusCompleted:
		summary = actionQueueSummaryWithWorkflowMarker(summary, "Completed")
	case humanActionStatusFailed:
		summary = actionQueueSummaryWithWorkflowMarker(summary, "Failed")
	default:
		summary = actionQueueSummaryWithWorkflowMarker(summary, "Resolved")
	}

	return sqlite.OperatorQueueResolveInput{
		WorkspaceID:             queue.WorkspaceID,
		QueueID:                 queue.QueueID,
		Status:                  "RESOLVED",
		ResolvedBy:              resolvedBy,
		Resolution:              linkedRollbackFailureSourceQueueResolution(action.ActionID, resolution),
		Summary:                 strings.TrimSpace(summary),
		Details:                 strings.TrimSpace(details),
		PayloadJSON:             string(mustJSON(payload)),
		RequireCurrentStatus:    "OPEN",
		RequireCurrentRevision:  queue.Revision,
		RequireCurrentUpdatedAt: strings.TrimSpace(queue.UpdatedAt),
	}
}

func linkedRollbackFailureSourceQueueFailureUpsertInput(queue sqlite.OperatorQueueRecord, payload model.RebaseRollbackFailurePayload, action sqlite.HumanActionRecord, resolvedBy, comment string, lineage rebaseRuntimeLineage) sqlite.OperatorQueueUpsertInput {
	payload.LastFailedFollowupActionID = strings.TrimSpace(action.ActionID)
	payload.LastFailedFollowupActionStatus = strings.TrimSpace(action.Status)
	payload.FollowupActionID = ""
	payload.FollowupActionQueueKey = ""
	payload.FollowupActionStatus = ""
	applyRebaseRollbackFailurePayloadLineage(&payload, lineage)
	payload.Normalize()

	details := strings.TrimSpace(queue.Details)
	details = actionQueueRemoveDetailLine(details, "Follow-up action:")
	details = actionQueueRemoveDetailLine(details, "Follow-up action queue:")
	details = actionQueueRemoveDetailLine(details, "Follow-up action status:")
	details = actionQueueUpsertDetailLine(details, "Last failed follow-up action:", strings.TrimSpace(action.ActionID))
	if resolvedBy != "" {
		details = actionQueueUpsertDetailLine(details, "Failed by:", strings.TrimSpace(resolvedBy))
	}
	if strings.TrimSpace(comment) != "" {
		details = actionQueueUpsertDetailLine(details, "Failure comment:", strings.TrimSpace(comment))
	}

	summary := actionQueueSummaryRemoveActionLink(strings.TrimSpace(queue.Summary))
	summary = actionQueueSummaryWithWorkflowMarker(summary, "Claimed")

	return sqlite.OperatorQueueUpsertInput{
		QueueID:                 queue.QueueID,
		WorkspaceID:             queue.WorkspaceID,
		QueueKey:                queue.QueueKey,
		QueueType:               queue.QueueType,
		Title:                   queue.Title,
		Summary:                 strings.TrimSpace(summary),
		Details:                 details,
		PayloadJSON:             string(mustJSON(payload)),
		AssignedTo:              firstNonEmpty(strings.TrimSpace(queue.AssignedTo), strings.TrimSpace(action.AssignedTo)),
		Urgency:                 queue.Urgency,
		SourceKind:              queue.SourceKind,
		SourceID:                queue.SourceID,
		TaskID:                  firstNonEmpty(strings.TrimSpace(queue.TaskID), strings.TrimSpace(action.TaskID)),
		SessionID:               queue.SessionID,
		AgentID:                 firstNonEmpty(strings.TrimSpace(queue.AgentID), strings.TrimSpace(action.AgentID)),
		KeepSessionActive:       queue.KeepSessionActive,
		DueAt:                   actionCreateOptionalString(queue.DueAt),
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

type actionCreateSourceQueue struct {
	kind                   actionCreateSourceQueueKind
	queue                  sqlite.OperatorQueueRecord
	payload                model.RebaseFollowupPayload
	rollbackFailurePayload model.RebaseRollbackFailurePayload
}

func (h *Handler) resolveActionCreateSourceQueue(ctx context.Context, params *actionCreateParams) (*actionCreateSourceQueue, error) {
	if params == nil {
		return nil, nil
	}
	requestedTaskID := strings.TrimSpace(params.TaskID)
	requestedAgentID := strings.TrimSpace(params.AgentID)
	queueID := strings.TrimSpace(params.QueueID)
	queueKey := strings.TrimSpace(params.QueueKey)
	if queueID == "" && queueKey == "" {
		return nil, nil
	}
	workspaceID := strings.TrimSpace(params.WorkspaceID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required when queue_id or queue_key is provided")
	}
	queue, err := h.store.GetOperatorQueueItem(ctx, workspaceID, queueID, queueKey)
	if err != nil {
		return nil, err
	}
	status := strings.ToUpper(strings.TrimSpace(queue.Status))
	if status == "RESOLVED" || status == "CANCELLED" {
		return nil, fmt.Errorf("source queue is not open: %s", strings.ToLower(status))
	}
	rollbackPayload, rollbackErr := actionCreateDecodeRollbackFailurePayload(queue.PayloadJSON)
	if rollbackErr == nil && rollbackPayload.IsRollbackFailure(queue.QueueKey) {
		if rollbackPayload.FollowupActionID != "" {
			return nil, fmt.Errorf("source queue already linked to action %s", rollbackPayload.FollowupActionID)
		}
		tensionTaskID, tensionAgentID := resolveActionCreateQueueTensionDefaults(ctx, h, queue.WorkspaceID, []string{rollbackPayload.RepairTensionID})
		authoritativeTaskID := firstNonEmpty(strings.TrimSpace(queue.TaskID), strings.TrimSpace(rollbackPayload.TaskID), tensionTaskID)
		authoritativeAgentID := firstNonEmpty(strings.TrimSpace(queue.AgentID), strings.TrimSpace(rollbackPayload.AgentID), tensionAgentID)
		params.WorkspaceID = firstNonEmpty(workspaceID, queue.WorkspaceID)
		if strings.TrimSpace(params.TaskID) == "" {
			params.TaskID = authoritativeTaskID
		}
		if strings.TrimSpace(params.AgentID) == "" {
			params.AgentID = authoritativeAgentID
		}
		if strings.TrimSpace(params.AssignedTo) == "" {
			params.AssignedTo = strings.TrimSpace(queue.AssignedTo)
		}
		if strings.TrimSpace(params.Title) == "" {
			params.Title = strings.TrimSpace(queue.Title)
		}
		if strings.TrimSpace(params.Description) == "" {
			params.Description = strings.TrimSpace(firstNonEmpty(queue.Details, queue.Summary, queue.Title))
		}
		if params.Blocking == nil {
			blocking := queue.KeepSessionActive
			params.Blocking = &blocking
		}
		if strings.TrimSpace(params.TaskID) == "" {
			return nil, errors.New("source queue is a queue-only rollback-failure follow-up without deterministic task context; use workspace.ops.resolve or workspace.ops.escalate")
		}
		if err := validateActionCreateSourceQueueTaskAgentContext(
			requestedTaskID,
			requestedAgentID,
			authoritativeTaskID,
			authoritativeAgentID,
		); err != nil {
			return nil, err
		}
		if err := validateActionCreateSourceQueueAssignment(queue, params.AssignedTo); err != nil {
			return nil, err
		}
		return &actionCreateSourceQueue{kind: actionCreateSourceQueueKindRollbackFailure, queue: queue, rollbackFailurePayload: rollbackPayload}, nil
	}
	payload, err := actionCreateDecodeQueuePayload(queue.PayloadJSON)
	if err == nil && payload.IsRebaseFollowup(queue.QueueKey) {
		if payload.LinkedActionExists() {
			return nil, fmt.Errorf("source queue already linked to action %s", payload.ActionID)
		}
		tensionTaskID, tensionAgentID := resolveActionCreateQueueTensionDefaults(ctx, h, queue.WorkspaceID, []string{payload.RepairTensionID, payload.ForkTensionID})
		authoritativeTaskID := firstNonEmpty(strings.TrimSpace(queue.TaskID), tensionTaskID)
		authoritativeAgentID := firstNonEmpty(strings.TrimSpace(queue.AgentID), tensionAgentID)
		params.WorkspaceID = firstNonEmpty(workspaceID, queue.WorkspaceID)
		if strings.TrimSpace(params.TaskID) == "" {
			params.TaskID = authoritativeTaskID
		}
		if strings.TrimSpace(params.AgentID) == "" {
			params.AgentID = authoritativeAgentID
		}
		if strings.TrimSpace(params.AssignedTo) == "" {
			params.AssignedTo = strings.TrimSpace(queue.AssignedTo)
		}
		if strings.TrimSpace(params.Title) == "" {
			params.Title = strings.TrimSpace(queue.Title)
		}
		if strings.TrimSpace(params.Description) == "" {
			params.Description = strings.TrimSpace(firstNonEmpty(queue.Details, queue.Summary, queue.Title))
		}
		if params.Blocking == nil {
			blocking := queue.KeepSessionActive
			params.Blocking = &blocking
		}
		if err := validateActionCreateSourceQueueTaskAgentContext(
			requestedTaskID,
			requestedAgentID,
			authoritativeTaskID,
			authoritativeAgentID,
		); err != nil {
			return nil, err
		}
		if err := validateActionCreateSourceQueueAssignment(queue, params.AssignedTo); err != nil {
			return nil, err
		}
		return &actionCreateSourceQueue{kind: actionCreateSourceQueueKindRebaseFollowup, queue: queue, payload: payload}, nil
	}

	if err != nil {
		return nil, err
	}
	return nil, errors.New("source queue is not a rebase follow-up or rollback-failure queue")
}

func validateActionCreateSourceQueueAssignment(queue sqlite.OperatorQueueRecord, assignedTo string) error {
	holder := strings.TrimSpace(queue.AssignedTo)
	requested := strings.TrimSpace(assignedTo)
	if holder == "" || requested == "" || holder == requested {
		return nil
	}
	return fmt.Errorf("source queue is assigned to %s", holder)
}

func validateActionCreateSourceQueueTaskAgentContext(requestedTaskID, requestedAgentID, authoritativeTaskID, authoritativeAgentID string) error {
	requestedTaskID = strings.TrimSpace(requestedTaskID)
	requestedAgentID = strings.TrimSpace(requestedAgentID)
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

func validateWorkflowQueueActorAuthority(queue sqlite.OperatorQueueRecord, actor, workflowLabel, actorField string) *RPCError {
	holder := strings.TrimSpace(queue.AssignedTo)
	actor = strings.TrimSpace(actor)
	if holder == "" {
		return nil
	}
	if actor == "" {
		return &RPCError{
			Code:    errCodeInvalidParams,
			Message: fmt.Sprintf("%s is required because the %s is assigned to %s", strings.TrimSpace(actorField), strings.TrimSpace(workflowLabel), holder),
		}
	}
	if actor != holder {
		return &RPCError{
			Code:    errCodeInvalidParams,
			Message: fmt.Sprintf("%s is assigned to %s", strings.TrimSpace(workflowLabel), holder),
		}
	}
	return nil
}

func (h *Handler) validateStandaloneHumanActionResolveAuthority(ctx context.Context, action sqlite.HumanActionRecord, resolvedBy string) *RPCError {
	if h == nil {
		return nil
	}
	queue, err := h.store.GetOperatorQueueItem(ctx, strings.TrimSpace(action.WorkspaceID), "", "action:"+strings.TrimSpace(action.ActionID))
	if err != nil {
		if isOperatorQueueItemNotFoundError(err) {
			return nil
		}
		return &RPCError{Code: errCodeInternal, Message: err.Error()}
	}
	return validateWorkflowQueueActorAuthority(queue, resolvedBy, "human action", "resolved_by")
}

func isOperatorQueueItemNotFoundError(err error) bool {
	return isOperatorQueueItemNotFoundErr(err)
}

func hydrateActionCreateQueueTensionDefaults(ctx context.Context, h *Handler, workspaceID string, tensionIDs []string, params *actionCreateParams) {
	if h == nil || params == nil {
		return
	}
	taskID, agentID := resolveActionCreateQueueTensionDefaults(ctx, h, workspaceID, tensionIDs)
	if strings.TrimSpace(params.TaskID) == "" {
		params.TaskID = taskID
	}
	if strings.TrimSpace(params.AgentID) == "" {
		params.AgentID = agentID
	}
}

func resolveActionCreateQueueTensionDefaults(ctx context.Context, h *Handler, workspaceID string, tensionIDs []string) (string, string) {
	if h == nil {
		return "", ""
	}
	var taskID string
	var agentID string
	for _, tensionID := range tensionIDs {
		tensionID = strings.TrimSpace(tensionID)
		if tensionID == "" {
			continue
		}
		detail, err := h.store.GetTension(ctx, workspaceID, tensionID)
		if err != nil {
			continue
		}
		if taskID == "" && len(detail.Tension.TaskIDs) > 0 {
			taskID = strings.TrimSpace(detail.Tension.TaskIDs[0])
		}
		if agentID == "" && len(detail.Tension.AgentIDs) > 0 {
			agentID = strings.TrimSpace(detail.Tension.AgentIDs[0])
		}
		if taskID != "" && agentID != "" {
			return taskID, agentID
		}
	}
	return taskID, agentID
}

func actionCreateDecodeQueuePayload(payloadJSON string) (model.RebaseFollowupPayload, error) {
	trimmed := strings.TrimSpace(payloadJSON)
	if trimmed == "" {
		return model.RebaseFollowupPayload{}, nil
	}
	payload := model.RebaseFollowupPayload{}
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return model.RebaseFollowupPayload{}, fmt.Errorf("decode source queue payload: %w", err)
	}
	payload.Normalize()
	return payload, nil
}

func actionCreateDecodeRollbackFailurePayload(payloadJSON string) (model.RebaseRollbackFailurePayload, error) {
	trimmed := strings.TrimSpace(payloadJSON)
	if trimmed == "" {
		return model.RebaseRollbackFailurePayload{}, nil
	}
	payload := model.RebaseRollbackFailurePayload{}
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return model.RebaseRollbackFailurePayload{}, fmt.Errorf("decode rollback failure queue payload: %w", err)
	}
	payload.Normalize()
	return payload, nil
}

func actionCreateQueuePayloadString(payload any, key string) string {
	switch v := payload.(type) {
	case model.RebaseFollowupPayload:
		switch strings.TrimSpace(key) {
		case "action_id":
			return v.ActionID
		case "action_queue_key":
			return v.ActionQueueKey
		case "action_status":
			return v.ActionStatus
		case "source_queue_id":
			return v.SourceQueueID
		case "source_queue_key":
			return v.SourceQueueKey
		case "action_title":
			return v.ActionTitle
		case "action_assigned_to":
			return v.ActionAssignedTo
		case "action_started_by":
			return v.ActionStartedBy
		case "action_started_comment":
			return v.ActionStartedComment
		case "action_paused_by":
			return v.ActionPausedBy
		case "action_pause_comment":
			return v.ActionPauseComment
		case "rebase_workflow_state":
			return v.RebaseWorkflowState
		case "rebase_workflow_step":
			return v.RebaseWorkflowStep
		case "next_action":
			return v.NextAction
		case "rebase_plan_class":
			return v.RebasePlanClass
		case "conflict_safe_class":
			return v.ConflictSafeClass
		case "repair_tension_id":
			return v.RepairTensionID
		case "fork_tension_id":
			return v.ForkTensionID
		case "task_id":
			return v.TaskID
		default:
			return ""
		}
	case model.RebaseRollbackFailurePayload:
		switch strings.TrimSpace(key) {
		case "followup_action_id":
			return v.FollowupActionID
		case "followup_action_queue_key":
			return v.FollowupActionQueueKey
		case "followup_action_status":
			return v.FollowupActionStatus
		case "last_failed_followup_action_id":
			return v.LastFailedFollowupActionID
		case "last_failed_followup_action_status":
			return v.LastFailedFollowupActionStatus
		case "task_id":
			return v.TaskID
		case "agent_id":
			return v.AgentID
		case "repair_tension_id":
			return v.RepairTensionID
		case "failure_scope":
			return v.FailureScope
		case "failure_trigger":
			return v.FailureTrigger
		default:
			return ""
		}
	case map[string]any:
		raw, ok := v[strings.TrimSpace(key)]
		if !ok {
			return ""
		}
		switch item := raw.(type) {
		case string:
			return strings.TrimSpace(item)
		default:
			return strings.TrimSpace(fmt.Sprint(item))
		}
	default:
		return ""
	}
}

func (h *Handler) linkActionCreateSourceQueue(ctx context.Context, queue sqlite.OperatorQueueRecord, payload model.RebaseFollowupPayload, action sqlite.HumanActionRecord) (sqlite.OperatorQueueRecord, sqlite.RuntimeEventRecord, error) {
	payload.ActionID = strings.TrimSpace(action.ActionID)
	payload.ActionQueueKey = "action:" + strings.TrimSpace(action.ActionID)
	payload.ActionStatus = strings.TrimSpace(action.Status)
	payload.ActionTitle = strings.TrimSpace(action.Title)
	payload.ActionAssignedTo = strings.TrimSpace(action.AssignedTo)
	payload.ActionBlocking = action.Blocking
	payload.RebaseWorkflowState = linkedActionSourceQueueWorkflowState(action.Status)
	payload.RebaseWorkflowStep = linkedActionSourceQueueWorkflowStep(action.Status)
	payload.Normalize()

	details := strings.TrimSpace(queue.Details)
	details = actionQueueUpsertDetailLine(details, "Linked action:", strings.TrimSpace(action.ActionID))
	details = actionQueueUpsertDetailLine(details, "Action queue:", "action:"+strings.TrimSpace(action.ActionID))
	details = actionQueueUpsertDetailLine(details, "Workflow state:", linkedActionSourceQueueWorkflowState(action.Status))
	details = actionQueueUpsertDetailLine(details, "Workflow step:", linkedActionSourceQueueWorkflowStep(action.Status))
	summary := strings.TrimSpace(queue.Summary)
	actionSummary := strings.TrimSpace(firstNonEmpty(action.Title, action.ActionID))
	if summary == "" {
		summary = actionSummary
	} else if actionSummary != "" && !strings.Contains(summary, strings.TrimSpace(action.ActionID)) {
		summary += " | Action: " + actionSummary + " (" + strings.TrimSpace(action.ActionID) + ")"
	}
	return h.store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		QueueID:                 queue.QueueID,
		WorkspaceID:             queue.WorkspaceID,
		QueueKey:                queue.QueueKey,
		QueueType:               queue.QueueType,
		Title:                   queue.Title,
		Summary:                 summary,
		Details:                 details,
		PayloadJSON:             string(mustJSON(payload)),
		AssignedTo:              firstNonEmpty(strings.TrimSpace(action.AssignedTo), strings.TrimSpace(queue.AssignedTo)),
		Urgency:                 queue.Urgency,
		SourceKind:              queue.SourceKind,
		SourceID:                queue.SourceID,
		TaskID:                  firstNonEmpty(strings.TrimSpace(queue.TaskID), strings.TrimSpace(action.TaskID)),
		SessionID:               queue.SessionID,
		AgentID:                 firstNonEmpty(strings.TrimSpace(queue.AgentID), strings.TrimSpace(action.AgentID)),
		KeepSessionActive:       queue.KeepSessionActive,
		DueAt:                   actionCreateOptionalString(queue.DueAt),
		RequireCurrentStatus:    "OPEN",
		RequireCurrentRevision:  queue.Revision,
		RequireCurrentUpdatedAt: strings.TrimSpace(queue.UpdatedAt),
	})
}

func linkedActionSourceQueueStartUpsertInput(queue sqlite.OperatorQueueRecord, payload model.RebaseFollowupPayload, action sqlite.HumanActionRecord, startedBy, comment string) sqlite.OperatorQueueUpsertInput {
	startedBy = strings.TrimSpace(startedBy)
	comment = strings.TrimSpace(comment)
	payload.ActionID = strings.TrimSpace(action.ActionID)
	payload.ActionQueueKey = "action:" + strings.TrimSpace(action.ActionID)
	payload.ActionStatus = strings.TrimSpace(action.Status)
	payload.ActionTitle = strings.TrimSpace(action.Title)
	payload.ActionAssignedTo = strings.TrimSpace(action.AssignedTo)
	payload.RebaseWorkflowState = linkedActionSourceQueueStartedState()
	payload.RebaseWorkflowStep = linkedActionSourceQueueStartedStep()
	payload.ActionStartedBy = startedBy
	payload.ActionPausedBy = ""
	payload.ActionPauseComment = ""
	if comment != "" {
		payload.ActionStartedComment = comment
	}
	payload.Normalize()

	details := strings.TrimSpace(queue.Details)
	details = actionQueueUpsertDetailLine(details, "Linked action:", strings.TrimSpace(action.ActionID))
	details = actionQueueUpsertDetailLine(details, "Action queue:", "action:"+strings.TrimSpace(action.ActionID))
	details = actionQueueUpsertDetailLine(details, "Workflow state:", linkedActionSourceQueueStartedState())
	details = actionQueueUpsertDetailLine(details, "Workflow step:", linkedActionSourceQueueStartedStep())
	details = actionQueueRemoveDetailLine(details, "Paused by:")
	details = actionQueueRemoveDetailLine(details, "Pause comment:")
	if startedBy != "" {
		details = actionQueueUpsertDetailLine(details, "Started by:", startedBy)
	}
	if comment != "" {
		details = actionQueueUpsertDetailLine(details, "Start comment:", comment)
	}

	summary := strings.TrimSpace(queue.Summary)
	actionSummary := strings.TrimSpace(firstNonEmpty(action.Title, action.ActionID))
	if summary == "" {
		summary = actionSummary
	}
	if actionSummary != "" && !strings.Contains(summary, strings.TrimSpace(action.ActionID)) {
		summary += " | Action: " + actionSummary + " (" + strings.TrimSpace(action.ActionID) + ")"
	}
	summary = actionQueueSummaryWithWorkflowMarker(summary, "In progress")

	return sqlite.OperatorQueueUpsertInput{
		QueueID:                 queue.QueueID,
		WorkspaceID:             queue.WorkspaceID,
		QueueKey:                queue.QueueKey,
		QueueType:               queue.QueueType,
		Title:                   queue.Title,
		Summary:                 strings.TrimSpace(summary),
		Details:                 details,
		PayloadJSON:             string(mustJSON(payload)),
		AssignedTo:              firstNonEmpty(startedBy, strings.TrimSpace(queue.AssignedTo), strings.TrimSpace(action.AssignedTo)),
		Urgency:                 queue.Urgency,
		SourceKind:              queue.SourceKind,
		SourceID:                queue.SourceID,
		TaskID:                  firstNonEmpty(strings.TrimSpace(queue.TaskID), strings.TrimSpace(action.TaskID)),
		SessionID:               queue.SessionID,
		AgentID:                 firstNonEmpty(strings.TrimSpace(queue.AgentID), strings.TrimSpace(action.AgentID)),
		KeepSessionActive:       queue.KeepSessionActive,
		DueAt:                   actionCreateOptionalString(queue.DueAt),
		RequireCurrentStatus:    "OPEN",
		RequireCurrentRevision:  queue.Revision,
		RequireCurrentUpdatedAt: strings.TrimSpace(queue.UpdatedAt),
	}
}

func (h *Handler) syncLinkedActionSourceQueueStart(ctx context.Context, queue sqlite.OperatorQueueRecord, currentActionQueue *sqlite.OperatorQueueRecord, payload model.RebaseFollowupPayload, action sqlite.HumanActionRecord, startedBy, comment string) (sqlite.OperatorQueueRecord, sqlite.RuntimeEventRecord, error) {
	if currentActionQueue == nil {
		fetchedQueue, err := h.store.GetOperatorQueueItem(ctx, strings.TrimSpace(action.WorkspaceID), "", "action:"+strings.TrimSpace(action.ActionID))
		if err != nil {
			return sqlite.OperatorQueueRecord{}, sqlite.RuntimeEventRecord{}, err
		}
		currentActionQueue = &fetchedQueue
	}
	return h.store.UpsertOperatorQueueItemWithEventForPendingHumanAction(ctx, action.ActionID, currentActionQueue, linkedActionSourceQueueStartUpsertInput(queue, payload, action, startedBy, comment), action)
}

func (h *Handler) syncLinkedActionSourceQueueStartWithActionEvent(ctx context.Context, queue sqlite.OperatorQueueRecord, currentActionQueue *sqlite.OperatorQueueRecord, payload model.RebaseFollowupPayload, action sqlite.HumanActionRecord, startedBy, comment string, runtimeInput sqlite.RuntimeEventInput) (sqlite.OperatorQueueRuntimeEventResult, error) {
	if currentActionQueue == nil {
		fetchedQueue, err := h.store.GetOperatorQueueItem(ctx, strings.TrimSpace(action.WorkspaceID), "", "action:"+strings.TrimSpace(action.ActionID))
		if err != nil {
			return sqlite.OperatorQueueRuntimeEventResult{}, err
		}
		currentActionQueue = &fetchedQueue
	}
	return h.store.UpsertOperatorQueueItemWithRuntimeEventForPendingHumanAction(ctx, action.ActionID, currentActionQueue, linkedActionSourceQueueStartUpsertInput(queue, payload, action, startedBy, comment), runtimeInput, action)
}

func linkedActionSourceQueuePauseUpsertInput(queue sqlite.OperatorQueueRecord, payload model.RebaseFollowupPayload, action sqlite.HumanActionRecord, pausedBy, comment string) sqlite.OperatorQueueUpsertInput {
	pausedBy = strings.TrimSpace(pausedBy)
	comment = strings.TrimSpace(comment)
	payload.ActionID = strings.TrimSpace(action.ActionID)
	payload.ActionQueueKey = "action:" + strings.TrimSpace(action.ActionID)
	payload.ActionStatus = strings.TrimSpace(action.Status)
	payload.ActionTitle = strings.TrimSpace(action.Title)
	payload.ActionAssignedTo = strings.TrimSpace(action.AssignedTo)
	payload.RebaseWorkflowState = linkedActionSourceQueuePausedState()
	payload.RebaseWorkflowStep = linkedActionSourceQueuePausedStep()
	payload.ActionPausedBy = pausedBy
	if comment != "" {
		payload.ActionPauseComment = comment
	}
	payload.Normalize()

	details := strings.TrimSpace(queue.Details)
	details = actionQueueUpsertDetailLine(details, "Linked action:", strings.TrimSpace(action.ActionID))
	details = actionQueueUpsertDetailLine(details, "Action queue:", "action:"+strings.TrimSpace(action.ActionID))
	details = actionQueueUpsertDetailLine(details, "Workflow state:", linkedActionSourceQueuePausedState())
	details = actionQueueUpsertDetailLine(details, "Workflow step:", linkedActionSourceQueuePausedStep())
	if pausedBy != "" {
		details = actionQueueUpsertDetailLine(details, "Paused by:", pausedBy)
	}
	if comment != "" {
		details = actionQueueUpsertDetailLine(details, "Pause comment:", comment)
	}

	summary := strings.TrimSpace(queue.Summary)
	actionSummary := strings.TrimSpace(firstNonEmpty(action.Title, action.ActionID))
	if summary == "" {
		summary = actionSummary
	}
	if actionSummary != "" && !strings.Contains(summary, strings.TrimSpace(action.ActionID)) {
		summary += " | Action: " + actionSummary + " (" + strings.TrimSpace(action.ActionID) + ")"
	}
	summary = actionQueueSummaryWithWorkflowMarker(summary, "Claimed")

	return sqlite.OperatorQueueUpsertInput{
		QueueID:                 queue.QueueID,
		WorkspaceID:             queue.WorkspaceID,
		QueueKey:                queue.QueueKey,
		QueueType:               queue.QueueType,
		Title:                   queue.Title,
		Summary:                 strings.TrimSpace(summary),
		Details:                 details,
		PayloadJSON:             string(mustJSON(payload)),
		AssignedTo:              firstNonEmpty(strings.TrimSpace(queue.AssignedTo), strings.TrimSpace(action.AssignedTo), pausedBy),
		Urgency:                 queue.Urgency,
		SourceKind:              queue.SourceKind,
		SourceID:                queue.SourceID,
		TaskID:                  firstNonEmpty(strings.TrimSpace(queue.TaskID), strings.TrimSpace(action.TaskID)),
		SessionID:               queue.SessionID,
		AgentID:                 firstNonEmpty(strings.TrimSpace(queue.AgentID), strings.TrimSpace(action.AgentID)),
		KeepSessionActive:       queue.KeepSessionActive,
		DueAt:                   actionCreateOptionalString(queue.DueAt),
		RequireCurrentStatus:    "OPEN",
		RequireCurrentRevision:  queue.Revision,
		RequireCurrentUpdatedAt: strings.TrimSpace(queue.UpdatedAt),
	}
}

func (h *Handler) syncLinkedActionSourceQueuePause(ctx context.Context, queue sqlite.OperatorQueueRecord, currentActionQueue *sqlite.OperatorQueueRecord, payload model.RebaseFollowupPayload, action sqlite.HumanActionRecord, pausedBy, comment string) (sqlite.OperatorQueueRecord, sqlite.RuntimeEventRecord, error) {
	if currentActionQueue == nil {
		fetchedQueue, err := h.store.GetOperatorQueueItem(ctx, strings.TrimSpace(action.WorkspaceID), "", "action:"+strings.TrimSpace(action.ActionID))
		if err != nil {
			return sqlite.OperatorQueueRecord{}, sqlite.RuntimeEventRecord{}, err
		}
		currentActionQueue = &fetchedQueue
	}
	return h.store.UpsertOperatorQueueItemWithEventForPendingHumanAction(ctx, action.ActionID, currentActionQueue, linkedActionSourceQueuePauseUpsertInput(queue, payload, action, pausedBy, comment), action)
}

func (h *Handler) syncLinkedActionSourceQueuePauseWithActionEvent(ctx context.Context, queue sqlite.OperatorQueueRecord, currentActionQueue *sqlite.OperatorQueueRecord, payload model.RebaseFollowupPayload, action sqlite.HumanActionRecord, pausedBy, comment string, runtimeInput sqlite.RuntimeEventInput) (sqlite.OperatorQueueRuntimeEventResult, error) {
	if currentActionQueue == nil {
		fetchedQueue, err := h.store.GetOperatorQueueItem(ctx, strings.TrimSpace(action.WorkspaceID), "", "action:"+strings.TrimSpace(action.ActionID))
		if err != nil {
			return sqlite.OperatorQueueRuntimeEventResult{}, err
		}
		currentActionQueue = &fetchedQueue
	}
	return h.store.UpsertOperatorQueueItemWithRuntimeEventForPendingHumanAction(ctx, action.ActionID, currentActionQueue, linkedActionSourceQueuePauseUpsertInput(queue, payload, action, pausedBy, comment), runtimeInput, action)
}

func actionQueueUpsertDetailLine(details, prefix, value string) string {
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

func isHumanActionWorkflowConflictError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sqlite.ErrRebaseFollowupStewardRequired) || errors.Is(err, sqlite.ErrRebaseFollowupStewardMismatch) {
		return true
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(message, "not found or already resolved") ||
		strings.Contains(message, "already linked to action") ||
		strings.Contains(message, "operator queue item is not open") ||
		strings.Contains(message, "updated concurrently")
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

func actionCreateOptionalString(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

type actionChatSendParams struct {
	ActionID string `json:"action_id"`
	FromID   string `json:"from_id"`
	Content  string `json:"content"`
}

func (h *Handler) actionChatSend(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p actionChatSendParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}

	action, err := h.store.GetHumanAction(ctx, p.ActionID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	chatResult, err := h.store.SendActionMessageWithAuthorityEffects(
		ctx,
		action,
		p.FromID,
		p.Content,
		h.humanActionPromptContextEnvelope(ctx, action.WorkspaceID, "action.chat.send"),
	)
	if err != nil {
		if rpcErr := authorityRejectRPCError(err, "action.chat.send"); rpcErr != nil {
			return nil, rpcErr
		}
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	if strings.TrimSpace(chatResult.ActionEvent.EventID) != "" {
		h.publishRuntimeEventRecord(chatResult.ActionEvent, "Chat in action "+p.ActionID)
	}
	if chatResult.AgentMessageEvent != nil && strings.TrimSpace(chatResult.AgentMessageID) != "" {
		liveAgentID := action.AgentID
		h.publishRuntimeEventRecordAlias(*chatResult.AgentMessageEvent, "agent.message", &liveAgentID, "", "Chat in action "+p.ActionID)
	}

	return map[string]any{
		"message_id": chatResult.MessageID,
		"action_id":  p.ActionID,
		"status":     "SENT",
	}, nil
}

type actionChatListParams struct {
	ActionID string `json:"action_id"`
}

func (h *Handler) actionChatList(ctx context.Context, raw json.RawMessage) (any, *RPCError) {
	var p actionChatListParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
	}

	messages, err := h.store.ListActionMessages(ctx, p.ActionID)
	if err != nil {
		return nil, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	return map[string]any{
		"messages": messages,
		"count":    len(messages),
	}, nil
}
