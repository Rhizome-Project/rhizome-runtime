package server

import "github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"

func operatorQueueRuntimeEventLiveType(eventType string) string {
	switch eventType {
	case "operator_queue.created", "operator_queue.updated", "operator_queue.reopened", "operator_queue.rebase_followup_created":
		return "workspace.ops.updated"
	case "operator_queue.resolved", "operator_queue.cancelled":
		return "workspace.ops.resolved"
	default:
		return ""
	}
}

func (h *Handler) publishWorkspaceMemoryRecordedEvent(record sqlite.WorkspaceMemoryRecord, event sqlite.RuntimeEventRecord) {
	h.publishRuntimeEventRecordAs(event, "workspace.memory.recorded", workspaceMemoryEventSummary(record), record.MemoryID)
}

func (h *Handler) publishWorkspaceMemoryRemovedEvent(record sqlite.WorkspaceMemoryRecord, event sqlite.RuntimeEventRecord) {
	h.publishRuntimeEventRecordAs(event, "workspace.memory.removed", workspaceMemoryEventSummary(record), record.MemoryID)
}

func (h *Handler) publishWorkspaceMemoryRestoredEvent(record sqlite.WorkspaceMemoryRecord, event sqlite.RuntimeEventRecord) {
	h.publishRuntimeEventRecordAs(event, "workspace.memory.restored", workspaceMemoryEventSummary(record), record.MemoryID)
}

func (h *Handler) workspaceMemoryEventActions(liveType string, record sqlite.WorkspaceMemoryRecord, event sqlite.RuntimeEventRecord, effects *sqlite.PromotedKnowledgeClaimSyncEffects) []runtimeEventPublishAction {
	actions := make([]runtimeEventPublishAction, 0, 1)
	if event.EventID != "" {
		recordCopy := record
		actions = append(actions, runtimeEventPublishAction{
			Event: event,
			Publish: func(runtimeEvent sqlite.RuntimeEventRecord) {
				switch liveType {
				case "workspace.memory.recorded":
					h.publishWorkspaceMemoryRecordedEvent(recordCopy, runtimeEvent)
				case "workspace.memory.removed":
					h.publishWorkspaceMemoryRemovedEvent(recordCopy, runtimeEvent)
				case "workspace.memory.restored":
					h.publishWorkspaceMemoryRestoredEvent(recordCopy, runtimeEvent)
				default:
					h.publishRuntimeEventRecordAs(runtimeEvent, liveType, workspaceMemoryEventSummary(recordCopy), recordCopy.MemoryID)
				}
			},
		})
	}
	actions = append(actions, h.promotedKnowledgeClaimSyncActions(effects)...)
	return actions
}

func (h *Handler) promotedKnowledgeClaimSyncActions(effects *sqlite.PromotedKnowledgeClaimSyncEffects) []runtimeEventPublishAction {
	if effects == nil {
		return nil
	}
	actions := make([]runtimeEventPublishAction, 0, 2+len(effects.InvalidationEvents))
	if effects.Claim != nil && effects.ClaimEvent != nil && effects.ClaimEvent.EventID != "" {
		record := *effects.Claim
		liveType := ""
		switch effects.ClaimEvent.EventType {
		case "knowledge_claim.written":
			liveType = "workspace.claim.written"
		case "knowledge_claim.archived":
			liveType = "workspace.claim.archived"
		}
		if liveType != "" {
			actions = append(actions, runtimeEventPublishAction{
				Event: *effects.ClaimEvent,
				Publish: func(event sqlite.RuntimeEventRecord) {
					h.publishKnowledgeClaimEventRecord(event, liveType, record)
				},
			})
		}
	}
	if effects.Queue != nil && effects.QueueEvent != nil && effects.QueueEvent.EventID != "" {
		record := *effects.Queue
		liveType := operatorQueueRuntimeEventLiveType(effects.QueueEvent.EventType)
		if liveType != "" {
			actions = append(actions, runtimeEventPublishAction{
				Event: *effects.QueueEvent,
				Publish: func(event sqlite.RuntimeEventRecord) {
					h.publishOperatorQueueEventRecord(event, liveType, record)
				},
			})
		}
	}
	for _, invalidationEvent := range effects.InvalidationEvents {
		if invalidationEvent.EventID == "" {
			continue
		}
		eventCopy := invalidationEvent
		actions = append(actions, runtimeEventPublishAction{
			Event: eventCopy,
			Publish: func(event sqlite.RuntimeEventRecord) {
				h.publishRuntimeEventRecord(event, "memory invalidation")
			},
		})
	}
	return actions
}
