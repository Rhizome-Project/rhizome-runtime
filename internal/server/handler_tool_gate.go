package server

import (
	"context"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/bridgepolicy"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func (h *Handler) enforceHighRiskToolCallGate(
	ctx context.Context,
	authority sqlite.WorkspaceAuthorityRecord,
	record sqlite.WorkspaceToolRecord,
	workspaceID string,
	actorType string,
	actorID string,
	requestedCapability string,
	publishToolRuntimeEvent func(sqlite.RuntimeEventRecord),
) (bool, *RPCError) {
	if !bridgepolicy.RequiresOperatorGate(record.PolicyEnvelope, record.Capabilities) {
		return false, nil
	}

	if requestedCapability == "" {
		requestedCapability = "tool.call"
	}

	if actorType == "" || actorID == "" {
		eventActorType := firstNonEmpty(actorType, "system")
		eventActorID := firstNonEmpty(actorID, "missing_actor")
		details := map[string]any{
			"requested_capability": requestedCapability,
			"policy_envelope":      record.PolicyEnvelope,
			"reason":               "actor_context_required_for_high_risk_tool",
		}
		payloadJSON, rpcErr := h.toolCallRuntimePayloadJSON(ctx, workspaceID, record.ToolID, eventActorType, eventActorID, "tool.call.denied", requestedCapability, "", details)
		if rpcErr != nil {
			return true, rpcErr
		}
		if event, rpcErr := h.recordAuthorityBackedRuntimeEvent(ctx, authority, "tool.call", sqlite.RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "tool.call.denied",
			EntityType:  "tool",
			EntityID:    record.ToolID,
			ActorType:   eventActorType,
			ActorID:     eventActorID,
			PayloadJSON: payloadJSON,
		}); rpcErr != nil {
			return true, rpcErr
		} else {
			publishToolRuntimeEvent(event)
		}
		return true, &RPCError{
			Code:    errCodePermissionDenied,
			Message: "high-risk tool call requires actor context",
			Details: details,
		}
	}

	check, err := h.store.CheckCapabilityPolicy(ctx, sqlite.CapabilityCheckInput{
		WorkspaceID: workspaceID,
		SubjectType: actorType,
		SubjectID:   actorID,
		Capability:  requestedCapability,
		ToolID:      record.ToolID,
	})
	if err != nil {
		if isControlPlaneValidationError(err) {
			return true, &RPCError{Code: errCodeInvalidParams, Message: err.Error()}
		}
		return true, &RPCError{Code: errCodeInternal, Message: err.Error()}
	}

	explicitPolicy := check.MatchedPolicy != nil
	switch {
	case check.Verdict == "DENY":
		details := map[string]any{
			"requested_capability": requestedCapability,
			"policy_check":         check,
			"policy_envelope":      record.PolicyEnvelope,
			"policy_verdict":       check.Verdict,
		}
		payloadJSON, rpcErr := h.toolCallRuntimePayloadJSON(ctx, workspaceID, record.ToolID, actorType, actorID, "tool.call.denied", requestedCapability, "", details)
		if rpcErr != nil {
			return true, rpcErr
		}
		if event, rpcErr := h.recordAuthorityBackedRuntimeEvent(ctx, authority, "tool.call", sqlite.RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "tool.call.denied",
			EntityType:  "tool",
			EntityID:    record.ToolID,
			ActorType:   actorType,
			ActorID:     actorID,
			PayloadJSON: payloadJSON,
		}); rpcErr != nil {
			return true, rpcErr
		} else {
			publishToolRuntimeEvent(event)
		}
		return true, &RPCError{
			Code:    errCodePermissionDenied,
			Message: "high-risk tool call denied by capability policy",
			Details: details,
		}
	case check.Verdict != "ALLOW" || !explicitPolicy:
		queue, err := h.ensureHighRiskToolApprovalQueue(ctx, workspaceID, record, actorType, actorID, requestedCapability)
		if err != nil {
			return true, &RPCError{Code: errCodeInternal, Message: err.Error()}
		}
		details := map[string]any{
			"requested_capability": requestedCapability,
			"policy_check":         check,
			"policy_envelope":      record.PolicyEnvelope,
			"approval_queue":       queue,
			"approval_mode":        "explicit_allow_policy_required",
			"policy_verdict":       check.Verdict,
		}
		if !explicitPolicy {
			details["implicit_allow_suppressed"] = true
		}
		payloadJSON, rpcErr := h.toolCallRuntimePayloadJSON(ctx, workspaceID, record.ToolID, actorType, actorID, "tool.call.approval_required", requestedCapability, "", details)
		if rpcErr != nil {
			return true, rpcErr
		}
		if event, rpcErr := h.recordAuthorityBackedRuntimeEvent(ctx, authority, "tool.call", sqlite.RuntimeEventInput{
			WorkspaceID: workspaceID,
			EventType:   "tool.call.approval_required",
			EntityType:  "tool",
			EntityID:    record.ToolID,
			ActorType:   actorType,
			ActorID:     actorID,
			PayloadJSON: payloadJSON,
		}); rpcErr != nil {
			return true, rpcErr
		} else {
			publishToolRuntimeEvent(event)
		}
		return true, &RPCError{
			Code:    errCodePermissionDenied,
			Message: "high-risk tool call requires explicit operator approval",
			Details: details,
		}
	default:
		_ = h.resolveHighRiskToolApprovalQueueIfOpen(ctx, workspaceID, record.ToolID, actorType, actorID, requestedCapability)
		return true, nil
	}
}

func (h *Handler) ensureHighRiskToolApprovalQueue(
	ctx context.Context,
	workspaceID string,
	record sqlite.WorkspaceToolRecord,
	actorType string,
	actorID string,
	requestedCapability string,
) (sqlite.OperatorQueueRecord, error) {
	requestKey := bridgepolicy.ApprovalRequestKey(record.ToolID, actorType, actorID, requestedCapability)
	queueKey := externalGateQueueKey("EXPLICIT_APPROVAL", requestKey)
	if existing, err := h.store.GetOperatorQueueItem(ctx, workspaceID, "", queueKey); err == nil {
		if strings.EqualFold(strings.TrimSpace(existing.Status), "OPEN") {
			return existing, nil
		}
	} else if !isOperatorQueueNotFound(err) {
		return sqlite.OperatorQueueRecord{}, err
	}

	payload := map[string]any{
		"request_key":               requestKey,
		"gate_type":                 "EXPLICIT_APPROVAL",
		"queue_key":                 queueKey,
		"queue_type":                "DECISION",
		"title":                     bridgepolicy.ApprovalTitle(record.DisplayName, record.ToolID),
		"summary":                   bridgepolicy.ApprovalSummary(record.DisplayName, record.ToolID, actorType, actorID),
		"details":                   bridgepolicy.ApprovalDetails(record.DisplayName, record.ToolID, actorType, actorID, requestedCapability, record.PolicyEnvelope),
		"assigned_to":               "operator",
		"urgency":                   "HIGH",
		"source_kind":               "bridge_high_risk_tool",
		"source_id":                 record.ToolID,
		"keep_session_active":       false,
		"tool_id":                   record.ToolID,
		"display_name":              strings.TrimSpace(record.DisplayName),
		"requested_capability":      requestedCapability,
		"subject_type":              actorType,
		"subject_id":                actorID,
		"approval_mode":             "explicit_allow_policy_required",
		"policy_envelope":           record.PolicyEnvelope,
		"operator_control_required": true,
	}

	queue, event, err := h.store.UpsertOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueUpsertInput{
		WorkspaceID:       workspaceID,
		QueueKey:          queueKey,
		QueueType:         "DECISION",
		Title:             bridgepolicy.ApprovalTitle(record.DisplayName, record.ToolID),
		Summary:           bridgepolicy.ApprovalSummary(record.DisplayName, record.ToolID, actorType, actorID),
		Details:           bridgepolicy.ApprovalDetails(record.DisplayName, record.ToolID, actorType, actorID, requestedCapability, record.PolicyEnvelope),
		PayloadJSON:       string(mustJSON(payload)),
		AssignedTo:        "operator",
		Urgency:           "HIGH",
		SourceKind:        "bridge_high_risk_tool",
		SourceID:          record.ToolID,
		KeepSessionActive: false,
	})
	if err != nil {
		return sqlite.OperatorQueueRecord{}, err
	}
	h.publishOperatorQueueEventRecord(event, "workspace.ops.updated", queue)
	return queue, nil
}

func (h *Handler) resolveHighRiskToolApprovalQueueIfOpen(ctx context.Context, workspaceID, toolID, actorType, actorID, requestedCapability string) error {
	requestKey := bridgepolicy.ApprovalRequestKey(toolID, actorType, actorID, requestedCapability)
	queueKey := externalGateQueueKey("EXPLICIT_APPROVAL", requestKey)
	queue, err := h.store.GetOperatorQueueItem(ctx, workspaceID, "", queueKey)
	if err != nil {
		if isOperatorQueueNotFound(err) {
			return nil
		}
		return err
	}
	if !strings.EqualFold(strings.TrimSpace(queue.Status), "OPEN") {
		return nil
	}
	resolved, event, err := h.store.ResolveOperatorQueueItemWithEvent(ctx, sqlite.OperatorQueueResolveInput{
		WorkspaceID: workspaceID,
		QueueKey:    queueKey,
		Status:      "RESOLVED",
		ResolvedBy:  "system:high_risk_bridge_gate",
		Resolution:  "approved_by_capability_policy",
		Summary:     "Explicit ALLOW capability policy now covers this high-risk bridge path",
	})
	if err != nil {
		if isOperatorQueueNotFound(err) || strings.Contains(strings.ToLower(strings.TrimSpace(err.Error())), "not open") {
			return nil
		}
		return err
	}
	h.publishOperatorQueueEventRecord(event, "workspace.ops.resolved", resolved)
	return nil
}

func isOperatorQueueNotFound(err error) bool {
	return isOperatorQueueItemNotFoundErr(err)
}
