package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/model"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

type rspAnomalyPayload struct {
	Family          string                    `json:"family"`
	AlertType       string                    `json:"alert_type"`
	EntityID        string                    `json:"entity_id"`
	ClusterID       string                    `json:"cluster_id"`
	EvidenceRefs    []string                  `json:"evidence_refs"`
	ActuationClass  string                    `json:"actuation_class"`
	ShadowOnly      bool                      `json:"shadow_only"`
	CapabilityFlags sqlite.RSPCapabilityFlags `json:"capability_flags"`
}

// StartRSPListener spawns a non-blocking worker pool to process global systematic events
// like ANOMALY_ALERT and TENSION_HINT from the statistical layer (RSP).
func (h *Handler) StartRSPListener(ctx context.Context) {
	ch := h.eventBus.SubscribeGlobal()

	// Worker pool channel
	workCh := make(chan EventMessage, 1024)

	// Spawn 3 workers for non-blocking I/O
	for i := 0; i < 3; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-workCh:
					h.processRSPEvent(ctx, msg)
				}
			}
		}()
	}

	// Dispatch loop
	go func() {
		defer h.eventBus.UnsubscribeGlobal(ch)
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-ch:
				if msg.Type == "ANOMALY_ALERT" || msg.Type == "TENSION_HINT" {
					select {
					case workCh <- msg:
					default:
						log.Printf("[RSP] warning: RSP worker pool queue full, dropping alert %s", msg.Type)
					}
				}
			}
		}
	}()
}

func (h *Handler) processRSPEvent(ctx context.Context, msg EventMessage) {
	if msg.Type == "ANOMALY_ALERT" {
		var p rspAnomalyPayload
		if err := json.Unmarshal([]byte(msg.PayloadJSON), &p); err != nil {
			return // Cannot process invalid payload
		}
		liveFlags := h.store.GetRSPCapabilityFlags(ctx, msg.WorkspaceID)
		if !strings.EqualFold(strings.TrimSpace(p.ActuationClass), "governed_hint") ||
			p.ShadowOnly ||
			!p.CapabilityFlags.GovernedHintsLive ||
			!liveFlags.GovernedHintsLive {
			return
		}
		entityID := firstNonEmpty(strings.TrimSpace(p.EntityID), strings.TrimSpace(msg.EntityID))
		if entityID == "" {
			return
		}
		clusterID := firstNonEmpty(strings.TrimSpace(p.ClusterID), entityID)
		evidenceRefs := append([]string(nil), p.EvidenceRefs...)
		if len(evidenceRefs) == 0 && strings.TrimSpace(msg.EventID) != "" {
			evidenceRefs = []string{strings.TrimSpace(msg.EventID)}
		}

		targetType := ""
		family := strings.ToLower(strings.TrimSpace(firstNonEmpty(p.Family, p.AlertType)))
		switch family {
		case "thrashing", "motif_thrash":
			targetType = "dissent_followup"
		case "stagnation", "verifier_pressure", "motif_bounce":
			targetType = "failure"
		default:
			return // Unmapped anomaly
		}

		// Title and Summary are generic, RSP should have provided reason in the payload if needed
		title := "Statistical Anomaly: " + family
		summary := "Detected pathological regime: " + family

		if err := h.store.EnsureGovernedTension(ctx, sqlite.EnsureGovernedTensionInput{
			WorkspaceID:    msg.WorkspaceID,
			TensionType:    targetType,
			ProtoClusterID: clusterID,
			AnchorRef:      entityID,
			Title:          title,
			Summary:        summary,
			EvidenceRefs:   evidenceRefs,
		}); err != nil {
			log.Printf("[RSP] error ensuring governed tension for %s: %v", targetType, err)
		}
		h.rollbackLinkedRebaseFollowupsForAnomaly(ctx, msg, entityID, family)
	} else if msg.Type == "TENSION_HINT" {
		// Placeholder for direct Hints
	}
}

type anomalyRollbackCandidate struct {
	item     sqlite.OperatorQueueRecord
	payload  model.RebaseFollowupPayload
	action   sqlite.HumanActionRecord
	actionID string
}

func (h *Handler) rollbackLinkedRebaseFollowupsForAnomaly(ctx context.Context, msg EventMessage, entityID, family string) {
	if !rspAnomalyShouldRollbackLinkedRebase(family) {
		return
	}
	lineage := runtimeLineageFromEventMessage(msg)
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return
	}
	items, err := h.listOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: msg.WorkspaceID,
		QueueType:   "FOLLOW_UP",
		Status:      "OPEN",
		Limit:       -1,
	})
	if err != nil {
		h.queueRebaseRollbackFailure(ctx, rebaseRollbackFailureInput{
			WorkspaceID:    msg.WorkspaceID,
			FailureScope:   "rsp_anomaly_list",
			FailureTrigger: "verifier_late_fail_queue_list",
			FailureMessage: err.Error(),
			EventID:        msg.EventID,
			SourceID:       entityID,
			EntityID:       entityID,
			Family:         family,
			Lineage:        lineage,
		})
		log.Printf("[RSP] error listing operator queues for anomaly rollback workspace=%s entity=%s: %v", msg.WorkspaceID, entityID, err)
		return
	}
	seenActions := map[string]struct{}{}
	invalidActions := map[string]struct{}{}
	candidates := make([]anomalyRollbackCandidate, 0)
	for _, item := range items {
		payload, err := actionCreateDecodeQueuePayload(item.PayloadJSON)
		if err != nil {
			if rebaseFollowupQueueMatchesAnomalyEntityWithoutPayload(item, entityID) {
				h.queueRebaseRollbackFailureForAnomalyCandidate(ctx, msg, family, entityID, item, model.RebaseFollowupPayload{}, "verifier_late_fail_payload_decode", err.Error())
			}
			log.Printf("[RSP] skip malformed queue payload during anomaly rollback workspace=%s queue=%s err=%v", msg.WorkspaceID, item.QueueID, err)
			continue
		}
		if !payload.IsRebaseFollowup(item.QueueKey) || !payload.LinkedActionExists() {
			continue
		}
		if !rebaseFollowupWorkflowIsActivelyClaimed(payload) {
			continue
		}
		if !rebaseFollowupMatchesAnomalyEntity(item, payload, entityID) {
			continue
		}
		actionID := strings.TrimSpace(payload.ActionID)
		if actionID == "" {
			continue
		}
		if _, invalid := invalidActions[actionID]; invalid {
			continue
		}
		if _, ok := seenActions[actionID]; ok {
			continue
		}
		action, err := h.store.GetHumanAction(ctx, actionID)
		if err != nil {
			h.queueRebaseRollbackFailureForAnomalyCandidate(ctx, msg, family, entityID, item, payload, "verifier_late_fail_action_lookup", err.Error())
			log.Printf("[RSP] error loading linked action during anomaly rollback workspace=%s action=%s: %v", msg.WorkspaceID, actionID, err)
			continue
		}
		if strings.ToUpper(strings.TrimSpace(action.Status)) != humanActionStatusPending {
			continue
		}
		if resolved, ok := h.currentRebaseRollbackFailureCreateInputFromAction(ctx, rebaseRollbackFailureInput{
			WorkspaceID:     msg.WorkspaceID,
			ActionID:        actionID,
			RepairTensionID: payload.RepairTensionID,
		}); ok {
			currentSourceQueueID := strings.TrimSpace(resolved.SourceQueueID)
			currentSourceQueueKey := strings.TrimSpace(resolved.SourceQueueKey)
			scannedQueueID := strings.TrimSpace(item.QueueID)
			scannedQueueKey := strings.TrimSpace(item.QueueKey)
			if (currentSourceQueueID != "" || currentSourceQueueKey != "") &&
				currentSourceQueueID != scannedQueueID &&
				currentSourceQueueKey != scannedQueueKey {
				invalidActions[actionID] = struct{}{}
				h.queueRebaseRollbackFailure(ctx, rebaseRollbackFailureInput{
					WorkspaceID:    msg.WorkspaceID,
					FailureScope:   "rsp_anomaly_list",
					FailureTrigger: "verifier_late_fail_stale_scanned_carrier",
					FailureMessage: fmt.Sprintf("scanned carrier %s no longer matches current action carrier %s", firstNonEmpty(scannedQueueID, scannedQueueKey), firstNonEmpty(currentSourceQueueID, currentSourceQueueKey)),
					EventID:        msg.EventID,
					SourceID:       entityID,
					EntityID:       entityID,
					Family:         family,
					Lineage:        lineage,
				})
				log.Printf("[RSP] stale scanned carrier during anomaly rollback workspace=%s action=%s scanned=%s current=%s", msg.WorkspaceID, actionID, firstNonEmpty(scannedQueueID, scannedQueueKey), firstNonEmpty(currentSourceQueueID, currentSourceQueueKey))
				continue
			}
			if candidate, ok := h.currentPendingLinkedRebaseCandidateForAnomalyAction(ctx, msg.WorkspaceID, entityID, action, payload.RepairTensionID); ok {
				candidates = append(candidates, candidate)
				continue
			}
		}
		invalidActions[actionID] = struct{}{}
		h.queueRebaseRollbackFailure(ctx, rebaseRollbackFailureInput{
			WorkspaceID:    msg.WorkspaceID,
			FailureScope:   "rsp_anomaly_list",
			FailureTrigger: "verifier_late_fail_current_carrier_unprovable",
			FailureMessage: fmt.Sprintf("current carrier for action %s could not be re-derived for anomaly entity %s", actionID, entityID),
			EventID:        msg.EventID,
			SourceID:       entityID,
			EntityID:       entityID,
			Family:         family,
			Lineage:        lineage,
		})
		log.Printf("[RSP] current carrier unprovable during anomaly rollback workspace=%s entity=%s action=%s scanned=%s", msg.WorkspaceID, entityID, actionID, firstNonEmpty(strings.TrimSpace(item.QueueID), strings.TrimSpace(item.QueueKey)))
	}
	if len(candidates) == 0 {
		return
	}
	if len(candidates) > 1 {
		uniqueActions := make(map[string]struct{}, len(candidates))
		for _, candidate := range candidates {
			uniqueActions[candidate.actionID] = struct{}{}
		}
		failureMessage := fmt.Sprintf("multiple active linked rebase carriers match anomaly entity %s", entityID)
		h.queueRebaseRollbackFailure(ctx, rebaseRollbackFailureInput{
			WorkspaceID:    msg.WorkspaceID,
			FailureScope:   "rsp_anomaly_list",
			FailureTrigger: "verifier_late_fail_ambiguous_carriers",
			FailureMessage: failureMessage,
			EventID:        msg.EventID,
			SourceID:       entityID,
			EntityID:       entityID,
			Family:         family,
			Lineage:        lineage,
		})
		log.Printf("[RSP] ambiguous linked rebase carriers during anomaly rollback workspace=%s entity=%s carriers=%d actions=%d", msg.WorkspaceID, entityID, len(candidates), len(uniqueActions))
		return
	}
	for _, candidate := range candidates {
		if h != nil && h.beforeRSPAnomalyRollbackResolveOverride != nil {
			h.beforeRSPAnomalyRollbackResolveOverride(ctx, candidate.actionID)
		}
		currentCandidates, err := h.currentPendingLinkedRebaseCandidatesForAnomaly(ctx, msg.WorkspaceID, entityID)
		if err != nil {
			h.queueRebaseRollbackFailure(ctx, rebaseRollbackFailureInput{
				WorkspaceID:    msg.WorkspaceID,
				FailureScope:   "rsp_anomaly_list",
				FailureTrigger: "verifier_late_fail_current_carrier_recheck",
				FailureMessage: err.Error(),
				EventID:        msg.EventID,
				SourceID:       entityID,
				EntityID:       entityID,
				Family:         family,
				Lineage:        lineage,
			})
			log.Printf("[RSP] error rechecking current anomaly carriers workspace=%s entity=%s: %v", msg.WorkspaceID, entityID, err)
			return
		}
		if len(currentCandidates) != 1 || strings.TrimSpace(currentCandidates[0].actionID) != strings.TrimSpace(candidate.actionID) {
			h.queueRebaseRollbackFailure(ctx, rebaseRollbackFailureInput{
				WorkspaceID:    msg.WorkspaceID,
				FailureScope:   "rsp_anomaly_list",
				FailureTrigger: "verifier_late_fail_interleaving_ambiguous_carriers",
				FailureMessage: fmt.Sprintf("current linked rebase carriers changed before rollback for anomaly entity %s", entityID),
				EventID:        msg.EventID,
				SourceID:       entityID,
				EntityID:       entityID,
				Family:         family,
				Lineage:        lineage,
			})
			log.Printf("[RSP] anomaly carrier set changed before rollback workspace=%s entity=%s original_action=%s current_candidates=%d", msg.WorkspaceID, entityID, candidate.actionID, len(currentCandidates))
			return
		}
		candidate = currentCandidates[0]
		item := candidate.item
		payload := candidate.payload
		action := candidate.action
		actionID := candidate.actionID
		resolveParams := actionResolveParams{
			ActionID:   actionID,
			Resolution: humanActionStatusFailed,
			Comment:    rspAnomalyRollbackComment(msg, family, entityID),
			ResolvedBy: "system:rsp",
		}
		resolveOpts := actionResolveOptions{RollbackReason: "verifier_late_fail", Lineage: lineage}
		if _, rpcErr := h.resolveActionWithEffects(ctx, action, resolveParams, resolveOpts); rpcErr != nil {
			if isHumanActionWorkflowConflictRPCError(rpcErr) && h.rspAnomalyRollbackAlreadyWon(ctx, actionID) {
				log.Printf("[RSP] rollback already resolved by concurrent winner workspace=%s action=%s queue=%s: %s", msg.WorkspaceID, actionID, item.QueueID, rpcErr.Message)
				continue
			}
			if isHumanActionWorkflowConflictRPCError(rpcErr) {
				if retryErr, retried := h.retryRSPAnomalyRollbackOnCurrentCarrier(ctx, msg.WorkspaceID, entityID, candidate, resolveParams, resolveOpts); retried {
					if retryErr == nil {
						continue
					}
					rpcErr = retryErr
				}
			}
			h.queueRebaseRollbackFailureWithCurrentAnomalyContext(ctx, rebaseRollbackFailureInput{
				WorkspaceID:     msg.WorkspaceID,
				FailureScope:    "rsp_anomaly",
				FailureTrigger:  "verifier_late_fail",
				FailureMessage:  rpcErr.Message,
				SourceID:        entityID,
				EntityID:        entityID,
				Family:          family,
				TaskID:          item.TaskID,
				SessionID:       item.SessionID,
				AgentID:         item.AgentID,
				ActionID:        actionID,
				SourceQueueID:   item.QueueID,
				SourceQueueKey:  item.QueueKey,
				RepairTensionID: payload.RepairTensionID,
				Lineage:         lineage,
			})
			log.Printf("[RSP] error rolling back linked rebase follow-up workspace=%s action=%s queue=%s: %s", msg.WorkspaceID, actionID, item.QueueID, rpcErr.Message)
			continue
		}
		seenActions[actionID] = struct{}{}
	}
}

func (h *Handler) currentPendingLinkedRebaseCandidatesForAnomaly(ctx context.Context, workspaceID, entityID string) ([]anomalyRollbackCandidate, error) {
	items, err := h.listOperatorQueueItems(ctx, sqlite.OperatorQueueFilter{
		WorkspaceID: workspaceID,
		QueueType:   "FOLLOW_UP",
		Status:      "OPEN",
		Limit:       -1,
	})
	if err != nil {
		return nil, err
	}
	candidates := make([]anomalyRollbackCandidate, 0)
	seenActions := make(map[string]struct{})
	for _, item := range items {
		payload, err := actionCreateDecodeQueuePayload(item.PayloadJSON)
		if err != nil {
			continue
		}
		if !payload.IsRebaseFollowup(item.QueueKey) || !payload.LinkedActionExists() {
			continue
		}
		if !rebaseFollowupWorkflowIsActivelyClaimed(payload) {
			continue
		}
		if !rebaseFollowupMatchesAnomalyEntity(item, payload, entityID) {
			continue
		}
		actionID := strings.TrimSpace(payload.ActionID)
		if actionID == "" {
			continue
		}
		if _, ok := seenActions[actionID]; ok {
			continue
		}
		seenActions[actionID] = struct{}{}
		action, err := h.store.GetHumanAction(ctx, actionID)
		if err != nil {
			continue
		}
		if strings.ToUpper(strings.TrimSpace(action.Status)) != humanActionStatusPending {
			continue
		}
		if candidate, ok := h.currentPendingLinkedRebaseCandidateForAnomalyAction(ctx, workspaceID, entityID, action, payload.RepairTensionID); ok {
			candidates = append(candidates, candidate)
		}
	}
	return candidates, nil
}

func (h *Handler) currentPendingLinkedRebaseCandidateForAnomalyAction(ctx context.Context, workspaceID, entityID string, action sqlite.HumanActionRecord, repairTensionID string) (anomalyRollbackCandidate, bool) {
	actionID := strings.TrimSpace(action.ActionID)
	if actionID == "" {
		return anomalyRollbackCandidate{}, false
	}
	resolved, ok := h.currentRebaseRollbackFailureCreateInputFromAction(ctx, rebaseRollbackFailureInput{
		WorkspaceID:     workspaceID,
		ActionID:        actionID,
		RepairTensionID: repairTensionID,
	})
	if !ok {
		return anomalyRollbackCandidate{}, false
	}
	item, err := h.store.GetOperatorQueueItem(ctx, workspaceID, resolved.SourceQueueID, resolved.SourceQueueKey)
	if err != nil {
		return anomalyRollbackCandidate{}, false
	}
	payload, err := actionCreateDecodeQueuePayload(item.PayloadJSON)
	if err != nil {
		return anomalyRollbackCandidate{}, false
	}
	if !payload.IsRebaseFollowup(item.QueueKey) || !payload.LinkedActionExists() {
		return anomalyRollbackCandidate{}, false
	}
	if !rebaseFollowupWorkflowIsActivelyClaimed(payload) {
		return anomalyRollbackCandidate{}, false
	}
	if !rebaseFollowupMatchesAnomalyEntity(item, payload, entityID) {
		return anomalyRollbackCandidate{}, false
	}
	if strings.TrimSpace(payload.ActionID) != strings.TrimSpace(actionID) {
		return anomalyRollbackCandidate{}, false
	}
	return anomalyRollbackCandidate{
		item:     item,
		payload:  payload,
		action:   action,
		actionID: actionID,
	}, true
}

func (h *Handler) queueRebaseRollbackFailureForAnomalyCandidate(ctx context.Context, msg EventMessage, family, entityID string, item sqlite.OperatorQueueRecord, payload model.RebaseFollowupPayload, trigger, failureMessage string) {
	h.queueRebaseRollbackFailureWithCurrentAnomalyContext(ctx, rebaseRollbackFailureInput{
		WorkspaceID:     msg.WorkspaceID,
		FailureScope:    "rsp_anomaly",
		FailureTrigger:  trigger,
		FailureMessage:  failureMessage,
		SourceID:        firstNonEmpty(entityID, strings.TrimSpace(item.SourceID)),
		EntityID:        entityID,
		Family:          family,
		TaskID:          item.TaskID,
		SessionID:       item.SessionID,
		AgentID:         item.AgentID,
		ActionID:        payload.ActionID,
		SourceQueueID:   item.QueueID,
		SourceQueueKey:  item.QueueKey,
		RepairTensionID: payload.RepairTensionID,
		Lineage:         runtimeLineageFromEventMessage(msg),
	})
}

func (h *Handler) queueRebaseRollbackFailureWithCurrentAnomalyContext(ctx context.Context, input rebaseRollbackFailureInput) {
	if resolved, ok := h.currentRebaseRollbackFailureCreateInputFromAction(ctx, input); ok {
		input.ActionID = resolved.ActionID
		input.RepairTensionID = firstNonEmpty(input.RepairTensionID, resolved.RepairTensionID)
		input.SourceQueueID = ""
		input.SourceQueueKey = ""
	} else if resolved, ok := h.currentRebaseRollbackFailureCreateInputFromRepairTension(ctx, input); ok {
		input.RepairTensionID = resolved.RepairTensionID
		input.ActionID = ""
		input.SourceQueueID = ""
		input.SourceQueueKey = ""
		if _, actionOK := h.currentRebaseRollbackFailureCreateInputFromAction(ctx, rebaseRollbackFailureInput{
			WorkspaceID:     input.WorkspaceID,
			ActionID:        resolved.ActionID,
			RepairTensionID: resolved.RepairTensionID,
		}); !actionOK {
			input.DisableCurrentCreateContext = true
		}
	} else if strings.TrimSpace(input.RepairTensionID) != "" {
		input.ActionID = ""
		input.SourceQueueID = ""
		input.SourceQueueKey = ""
		input.DisableCurrentCreateContext = true
	} else if strings.TrimSpace(input.SourceQueueID) != "" || strings.TrimSpace(input.SourceQueueKey) != "" {
		input.ActionID = ""
		input.DisableCurrentCreateContext = true
	}
	h.queueRebaseRollbackFailure(ctx, input)
}

func (h *Handler) retryRSPAnomalyRollbackOnCurrentCarrier(ctx context.Context, workspaceID, entityID string, candidate anomalyRollbackCandidate, params actionResolveParams, opts actionResolveOptions) (*RPCError, bool) {
	if h == nil {
		return nil, false
	}
	actionID := strings.TrimSpace(candidate.actionID)
	if actionID == "" {
		return nil, false
	}
	currentAction, err := h.store.GetHumanAction(ctx, actionID)
	if err != nil {
		return &RPCError{Code: errCodeInternal, Message: err.Error()}, true
	}
	if strings.ToUpper(strings.TrimSpace(currentAction.Status)) != humanActionStatusPending {
		return nil, false
	}
	currentCandidate, ok := h.currentPendingLinkedRebaseCandidateForAnomalyAction(ctx, workspaceID, entityID, currentAction, candidate.payload.RepairTensionID)
	if !ok {
		return nil, false
	}
	if strings.TrimSpace(currentCandidate.item.QueueID) != strings.TrimSpace(candidate.item.QueueID) ||
		strings.TrimSpace(currentCandidate.item.QueueKey) != strings.TrimSpace(candidate.item.QueueKey) {
		return nil, false
	}
	if strings.TrimSpace(currentCandidate.payload.RebaseWorkflowState) != strings.TrimSpace(candidate.payload.RebaseWorkflowState) ||
		strings.TrimSpace(currentCandidate.payload.RebaseWorkflowStep) != strings.TrimSpace(candidate.payload.RebaseWorkflowStep) {
		return nil, false
	}
	if strings.TrimSpace(currentAction.AssignedTo) != strings.TrimSpace(candidate.action.AssignedTo) {
		return nil, false
	}
	_, rpcErr := h.resolveActionWithEffects(ctx, currentAction, params, opts)
	return rpcErr, true
}

func isHumanActionWorkflowConflictRPCError(rpcErr *RPCError) bool {
	if rpcErr == nil {
		return false
	}
	return isHumanActionWorkflowConflictError(fmt.Errorf("%s", strings.TrimSpace(rpcErr.Message)))
}

func (h *Handler) rspAnomalyRollbackAlreadyWon(ctx context.Context, actionID string) bool {
	if h == nil {
		return false
	}
	actionID = strings.TrimSpace(actionID)
	if actionID == "" {
		return false
	}
	currentAction, err := h.store.GetHumanAction(ctx, actionID)
	if err != nil {
		return false
	}
	return strings.ToUpper(strings.TrimSpace(currentAction.Status)) != humanActionStatusPending
}

func (h *Handler) listOperatorQueueItems(ctx context.Context, filter sqlite.OperatorQueueFilter) ([]sqlite.OperatorQueueRecord, error) {
	if h != nil && h.listOperatorQueueItemsOverride != nil {
		return h.listOperatorQueueItemsOverride(ctx, filter)
	}
	return h.store.ListOperatorQueueItems(ctx, filter)
}

func rspAnomalyShouldRollbackLinkedRebase(family string) bool {
	switch strings.ToLower(strings.TrimSpace(family)) {
	case "verifier_pressure":
		return true
	default:
		return false
	}
}

func rebaseFollowupMatchesAnomalyEntity(queue sqlite.OperatorQueueRecord, payload model.RebaseFollowupPayload, entityID string) bool {
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return false
	}
	for _, candidate := range []string{
		strings.TrimSpace(queue.SourceID),
		strings.TrimSpace(payload.RepairTensionID),
		strings.TrimSpace(payload.ForkTensionID),
	} {
		if candidate != "" && candidate == entityID {
			return true
		}
	}
	return false
}

func rebaseFollowupQueueMatchesAnomalyEntityWithoutPayload(queue sqlite.OperatorQueueRecord, entityID string) bool {
	entityID = strings.TrimSpace(entityID)
	if entityID == "" || !strings.HasPrefix(strings.ToLower(strings.TrimSpace(queue.QueueKey)), model.RebaseFollowupQueueKeyPrefix) {
		return false
	}
	return strings.TrimSpace(queue.SourceID) == entityID
}

func rspAnomalyRollbackComment(msg EventMessage, family, entityID string) string {
	family = strings.TrimSpace(firstNonEmpty(family, "verifier_pressure"))
	entityID = strings.TrimSpace(entityID)
	eventRef := strings.TrimSpace(msg.EventID)
	parts := []string{"RSP late verifier fail rollback"}
	if family != "" {
		parts = append(parts, "family="+family)
	}
	if entityID != "" {
		parts = append(parts, "entity="+entityID)
	}
	if eventRef != "" {
		parts = append(parts, "event="+eventRef)
	}
	return strings.Join(parts, " | ")
}

func rebaseFollowupWorkflowIsActivelyClaimed(payload model.RebaseFollowupPayload) bool {
	return strings.EqualFold(strings.TrimSpace(payload.RebaseWorkflowState), rebaseWorkflowStateInProgress) &&
		strings.EqualFold(strings.TrimSpace(payload.RebaseWorkflowStep), rebaseWorkflowStepOperatorClaimed)
}
