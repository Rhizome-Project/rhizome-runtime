package sqlite

import (
	"context"
	"strings"
)

const rspRootCauseEntityFallbackLimit = 8

func rspRootCauseSupportedEvidenceKind(kind string) bool {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "runtime_event", "cluster", "tension", "segment", "memory_metrics", "memory_residency":
		return true
	default:
		return false
	}
}

func (s *Store) rspResolveRuntimeEventRootCauseGroup(ctx context.Context, workspaceID, eventID string) string {
	workspaceID = strings.TrimSpace(workspaceID)
	eventID = strings.TrimSpace(eventID)
	if s == nil || workspaceID == "" || eventID == "" {
		return ""
	}
	record, err := s.lookupRuntimeEventByID(ctx, workspaceID, eventID)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(firstNonEmpty(record.RootCauseID, record.ProvenanceGroupID))
}

func (s *Store) rspResolveEntityRuntimeRootCauseGroups(ctx context.Context, workspaceID, entityID string, limit int) []string {
	workspaceID = strings.TrimSpace(workspaceID)
	entityID = strings.TrimSpace(entityID)
	if s == nil || workspaceID == "" || entityID == "" {
		return nil
	}
	if limit <= 0 {
		limit = rspRootCauseEntityFallbackLimit
	}
	events, err := s.ListRuntimeEvents(ctx, RuntimeEventFilter{
		WorkspaceID: workspaceID,
		EntityID:    entityID,
		Limit:       limit,
	})
	if err != nil {
		return nil
	}
	groups := make([]string, 0, len(events))
	for _, record := range events {
		group := strings.TrimSpace(firstNonEmpty(record.RootCauseID, record.ProvenanceGroupID))
		if group == "" {
			continue
		}
		groups = append(groups, group)
	}
	return uniqueTrimmedLocusStrings(groups)
}

func (s *Store) rspResolveRootCauseGroupsForRef(ctx context.Context, workspaceID, ref string) ([]string, string) {
	ref = strings.TrimSpace(ref)
	if s == nil || strings.TrimSpace(workspaceID) == "" || ref == "" {
		return nil, "NONE"
	}
	kind, remainder, found := strings.Cut(ref, ":")
	if !found {
		return nil, "NONE"
	}
	kind = strings.ToLower(strings.TrimSpace(kind))
	remainder = strings.TrimSpace(remainder)
	if remainder == "" || !rspRootCauseSupportedEvidenceKind(kind) {
		return nil, "NONE"
	}
	if kind == "runtime_event" {
		if group := s.rspResolveRuntimeEventRootCauseGroup(ctx, workspaceID, remainder); group != "" {
			return []string{group}, "DIRECT_RUNTIME_EVENT_REF"
		}
		return nil, "NONE"
	}
	groups := s.rspResolveEntityRuntimeRootCauseGroups(ctx, workspaceID, remainder, rspRootCauseEntityFallbackLimit)
	if len(groups) > 0 {
		return groups, "EVIDENCE_REF_ENTITY_FALLBACK"
	}
	return nil, "NONE"
}

func (s *Store) rspResolveRootCauseGroupsForSource(ctx context.Context, workspaceID, sourceKind, sourceID string) ([]string, string) {
	sourceKind = strings.ToLower(strings.TrimSpace(sourceKind))
	sourceID = strings.TrimSpace(sourceID)
	if s == nil || strings.TrimSpace(workspaceID) == "" || sourceKind == "" || sourceID == "" {
		return nil, "NONE"
	}
	if !rspRootCauseSupportedEvidenceKind(sourceKind) {
		return nil, "NONE"
	}
	return s.rspResolveRootCauseGroupsForRef(ctx, workspaceID, sourceKind+":"+sourceID)
}
