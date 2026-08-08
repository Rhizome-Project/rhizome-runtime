package server

import (
	"context"
	"log"

	"github.com/Rhizome-Project/rhizome-runtime/internal/sessionmemory"
	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func (h *Handler) syncSessionEventMemory(ctx context.Context, state sqlite.AgentSessionStateRecord) {
	h.publishRuntimeEventActionsChronological(h.syncSessionEventMemoryActions(ctx, state)...)
}

func (h *Handler) syncSessionEventMemoryActions(ctx context.Context, state sqlite.AgentSessionStateRecord) []runtimeEventPublishAction {
	result, err := sessionmemory.SyncEventWithResult(ctx, h.store, state.WorkspaceID, state)
	if err != nil {
		log.Printf("[session-memory] sync failed workspace=%s session=%s event=%s err=%v", state.WorkspaceID, state.SessionID, state.UpdateType, err)
		return nil
	}

	switch result.Action {
	case sessionmemory.SyncEventActionRecorded:
		return h.workspaceMemoryEventActions("workspace.memory.recorded", result.Record, result.Event, result.PromotedClaimEffects)
	case sessionmemory.SyncEventActionArchived:
		return h.workspaceMemoryEventActions("workspace.memory.removed", result.Record, result.Event, result.PromotedClaimEffects)
	}
	return nil
}
