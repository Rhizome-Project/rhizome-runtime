package server

import (
	"context"
	"log"
	"strings"

	"github.com/Rhizome-Project/rhizome-runtime/internal/storage/sqlite"
)

func (h *Handler) recordRuntimeEvent(ctx context.Context, input sqlite.RuntimeEventInput) (sqlite.RuntimeEventRecord, bool) {
	record, err := h.store.RecordRuntimeEvent(ctx, input)
	if err != nil {
		log.Printf("[runtime-events] append failed workspace=%s event=%s entity=%s/%s err=%v", input.WorkspaceID, input.EventType, input.EntityType, input.EntityID, err)
		return sqlite.RuntimeEventRecord{}, false
	}
	return record, true
}

func (h *Handler) recordAuthorityBackedRuntimeEvent(ctx context.Context, authority sqlite.WorkspaceAuthorityRecord, surface string, input sqlite.RuntimeEventInput) (sqlite.RuntimeEventRecord, *RPCError) {
	record, err := h.store.RecordRuntimeEventWithAuthority(ctx, authority, input)
	if err != nil {
		return sqlite.RuntimeEventRecord{}, rpcErrorFromStoreAuthority(err, surface)
	}
	return record, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
