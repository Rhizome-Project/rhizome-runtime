package sqlite

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

const memoryNodeSearchDefaultLimit = 20

type MemoryNodeSearchFilter struct {
	WorkspaceID     string `json:"workspace_id"`
	Query           string `json:"query,omitempty"`
	MemoryType      string `json:"memory_type,omitempty"`
	MemoryLayer     string `json:"memory_layer,omitempty"`
	Visibility      string `json:"visibility,omitempty"`
	EpistemicStatus string `json:"epistemic_status,omitempty"`
	LifecycleState  string `json:"lifecycle_state,omitempty"`
	OriginKind      string `json:"origin_kind,omitempty"`
	OriginID        string `json:"origin_id,omitempty"`
	SourceKind      string `json:"source_kind,omitempty"`
	AgentID         string `json:"agent_id,omitempty"`
	SessionID       string `json:"session_id,omitempty"`
	TaskID          string `json:"task_id,omitempty"`
	IncludeArchived bool   `json:"include_archived,omitempty"`
	Limit           int    `json:"limit,omitempty"`
}

type MemoryNodeSearchHit struct {
	MemoryID             string                    `json:"memory_id"`
	WorkspaceID          string                    `json:"workspace_id"`
	MemoryType           string                    `json:"memory_type"`
	CompatType           string                    `json:"compat_type,omitempty"`
	CanonicalAuthority   string                    `json:"canonical_authority"`
	SurfaceAuthority     string                    `json:"surface_authority"`
	SurfaceRole          string                    `json:"surface_role"`
	CompatibilityOnly    bool                      `json:"compatibility_only"`
	SemanticLineageID    string                    `json:"semantic_lineage_id"`
	Revision             int                       `json:"revision"`
	Protect              bool                      `json:"protect"`
	Unresolved           bool                      `json:"unresolved"`
	LastAnyAccess        *string                   `json:"last_any_access,omitempty"`
	LastTrustedAccess    *string                   `json:"last_trusted_access,omitempty"`
	TLife                float64                   `json:"t_life"`
	RetentionBand        string                    `json:"retention_band,omitempty"`
	RetentionPrunable    bool                      `json:"retention_prunable"`
	RetentionGuardReason string                    `json:"retention_guard_reason,omitempty"`
	RetentionHotUntil    *string                   `json:"retention_hot_until,omitempty"`
	RetentionWarmUntil   *string                   `json:"retention_warm_until,omitempty"`
	RetentionExpiresAt   *string                   `json:"retention_expires_at,omitempty"`
	TemporalContracts    []TemporalHorizonContract `json:"temporal_contracts,omitempty"`
	RecoveryCandidate    bool                      `json:"recovery_candidate"`
	RecoveryTriggerCount int                       `json:"recovery_trigger_count,omitempty"`
	RecoveryTriggerKinds []string                  `json:"recovery_trigger_kinds,omitempty"`
	RecoveryGuardReason  string                    `json:"recovery_guard_reason,omitempty"`
	Visibility           string                    `json:"visibility"`
	MemoryLayer          string                    `json:"memory_layer"`
	EpistemicStatus      string                    `json:"epistemic_status"`
	LifecycleState       string                    `json:"lifecycle_state"`
	OriginKind           string                    `json:"origin_kind"`
	OriginID             string                    `json:"origin_id"`
	SourceKind           string                    `json:"source_kind,omitempty"`
	SourceID             string                    `json:"source_id,omitempty"`
	AgentID              string                    `json:"agent_id,omitempty"`
	SessionID            string                    `json:"session_id,omitempty"`
	TaskID               string                    `json:"task_id,omitempty"`
	Title                string                    `json:"title,omitempty"`
	Summary              string                    `json:"summary,omitempty"`
	ClaimSubject         string                    `json:"claim_subject,omitempty"`
	ClaimPredicate       string                    `json:"claim_predicate,omitempty"`
	ClaimObject          string                    `json:"claim_object,omitempty"`
	ArchivedReason       string                    `json:"archived_reason,omitempty"`
	RecoveryReason       string                    `json:"recovery_reason,omitempty"`
	Snippet              string                    `json:"snippet,omitempty"`
	DriftState           string                    `json:"drift_state,omitempty"`
	DriftScore           float64                   `json:"drift_score,omitempty"`
	RefCount             int                       `json:"ref_count"`
	VersionCount         int                       `json:"version_count"`
	OutboundEdgeCount    int                       `json:"outbound_edge_count"`
	InboundEdgeCount     int                       `json:"inbound_edge_count"`
	RefKinds             []string                  `json:"ref_kinds,omitempty"`
	UpdatedAt            string                    `json:"updated_at"`
}

type MemoryNodeSearchResult struct {
	WorkspaceID      string                      `json:"workspace_id"`
	TimeAuthority    WorkspaceTimeAuthority      `json:"time_authority"`
	BoundaryContract MemoryShapeBoundaryContract `json:"boundary_contract"`
	Query            string                      `json:"query,omitempty"`
	GeneratedAt      string                      `json:"generated_at"`
	Hits             []MemoryNodeSearchHit       `json:"hits,omitempty"`
	Count            int                         `json:"count"`
}

func (s *Store) SearchMemoryNodes(ctx context.Context, filter MemoryNodeSearchFilter) (MemoryNodeSearchResult, error) {
	filter = normalizeMemoryNodeSearchFilter(filter)
	if filter.WorkspaceID == "" {
		return MemoryNodeSearchResult{}, errors.New("workspace_id is required")
	}
	graphFilter := MemoryGraphNodeFilter{
		WorkspaceID:     filter.WorkspaceID,
		MemoryType:      filter.MemoryType,
		MemoryLayer:     filter.MemoryLayer,
		Visibility:      filter.Visibility,
		EpistemicStatus: filter.EpistemicStatus,
		LifecycleState:  filter.LifecycleState,
		OriginKind:      filter.OriginKind,
		OriginID:        filter.OriginID,
		SourceKind:      filter.SourceKind,
		AgentID:         filter.AgentID,
		SessionID:       filter.SessionID,
		TaskID:          filter.TaskID,
		IncludeArchived: filter.IncludeArchived,
		Limit:           filter.Limit,
	}
	if err := validateMemoryGraphNodeFilter(graphFilter); err != nil {
		return MemoryNodeSearchResult{}, err
	}

	query := strings.Builder{}
	query.WriteString(`SELECT n.memory_id, n.workspace_id, n.memory_type, n.compat_type, n.semantic_lineage_id, n.revision, n.protect, n.unresolved,
	        s.t_i_acc, s.t_i_star, s.h_i, s.t_hot, s.t_warm, s.t_gc,
	        n.visibility, n.memory_layer,
	        n.epistemic_status, n.lifecycle_state, n.origin_kind, n.origin_id, n.source_kind, n.source_id,
	        n.agent_id, n.session_id, n.task_id, n.title, n.body, n.summary,
	        n.claim_subject, n.claim_predicate, n.claim_object, n.claim_qualifiers_json, n.claim_time_scope_json,
	        n.claim_modality, n.source_set_json, n.provenance_json,
	        n.temperature, n.importance, n.confidence, n.activation, n.drift, n.volatility, n.pin_strength,
	        n.archived_at, n.archived_reason, n.recovery_reason, n.created_at, n.updated_at
	   FROM memory_nodes n
	   LEFT JOIN memory_node_salience s
	     ON s.workspace_id = n.workspace_id AND s.memory_id = n.memory_id`)
	args := make([]any, 0, 24)
	where := buildMemoryGraphNodeWhere(graphFilter, "n", &args)
	if filter.Query != "" {
		needle := "%" + filter.Query + "%"
		where = append(where, `(n.title LIKE ? OR n.summary LIKE ? OR n.body LIKE ? OR n.claim_subject LIKE ? OR n.claim_predicate LIKE ? OR n.claim_object LIKE ? OR n.memory_type LIKE ? OR n.compat_type LIKE ?)`)
		args = append(args, needle, needle, needle, needle, needle, needle, needle, needle)
	}
	if len(where) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(where, " AND "))
	}
	if filter.Query != "" {
		needle := "%" + filter.Query + "%"
		query.WriteString(` ORDER BY
		 (CASE WHEN n.title LIKE ? THEN 8 ELSE 0 END +
		  CASE WHEN n.summary LIKE ? THEN 6 ELSE 0 END +
		  CASE WHEN n.body LIKE ? THEN 4 ELSE 0 END +
		  CASE WHEN n.claim_subject LIKE ? THEN 5 ELSE 0 END +
		  CASE WHEN n.claim_predicate LIKE ? THEN 2 ELSE 0 END +
		  CASE WHEN n.claim_object LIKE ? THEN 3 ELSE 0 END) DESC,
		 n.importance DESC,
		 n.updated_at DESC,
		 n.memory_id DESC`)
		args = append(args, needle, needle, needle, needle, needle, needle)
	} else {
		query.WriteString(` ORDER BY n.updated_at DESC, n.importance DESC, n.memory_id DESC`)
	}
	query.WriteString(` LIMIT ?`)
	args = append(args, filter.Limit)

	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return MemoryNodeSearchResult{}, fmt.Errorf("search memory nodes: %w", err)
	}
	defer rows.Close()
	items, err := collectMemoryGraphNodeRows(rows)
	if err != nil {
		return MemoryNodeSearchResult{}, err
	}
	authority, err := s.GetWorkspaceTimeAuthority(ctx, filter.WorkspaceID)
	if err != nil {
		return MemoryNodeSearchResult{}, err
	}
	boundary, err := s.MemoryGraphBoundaryContractForWorkspace(ctx, filter.WorkspaceID, filter.OriginKind)
	if err != nil {
		return MemoryNodeSearchResult{}, err
	}

	hits := make([]MemoryNodeSearchHit, 0, len(items))
	for _, item := range items {
		detail, err := s.GetMemoryGraphNode(ctx, filter.WorkspaceID, item.MemoryID)
		if err != nil {
			return MemoryNodeSearchResult{}, err
		}
		hits = append(hits, buildMemoryNodeSearchHit(detail, filter.Query))
	}
	return MemoryNodeSearchResult{
		WorkspaceID:      filter.WorkspaceID,
		TimeAuthority:    authority,
		BoundaryContract: boundary,
		Query:            filter.Query,
		GeneratedAt:      generatedAtFromWorkspaceTimeAuthority(authority),
		Hits:             hits,
		Count:            len(hits),
	}, nil
}

func normalizeMemoryNodeSearchFilter(filter MemoryNodeSearchFilter) MemoryNodeSearchFilter {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	filter.Query = strings.TrimSpace(filter.Query)
	filter.MemoryType = strings.TrimSpace(filter.MemoryType)
	filter.MemoryLayer = strings.TrimSpace(filter.MemoryLayer)
	filter.Visibility = strings.TrimSpace(filter.Visibility)
	filter.EpistemicStatus = strings.TrimSpace(filter.EpistemicStatus)
	filter.LifecycleState = strings.TrimSpace(filter.LifecycleState)
	filter.OriginKind = strings.TrimSpace(filter.OriginKind)
	filter.OriginID = strings.TrimSpace(filter.OriginID)
	filter.SourceKind = strings.TrimSpace(filter.SourceKind)
	filter.AgentID = strings.TrimSpace(filter.AgentID)
	filter.SessionID = strings.TrimSpace(filter.SessionID)
	filter.TaskID = strings.TrimSpace(filter.TaskID)
	if filter.Limit <= 0 {
		filter.Limit = memoryNodeSearchDefaultLimit
	}
	return filter
}

func buildMemoryNodeSearchHit(detail MemoryGraphNodeDetail, query string) MemoryNodeSearchHit {
	refKinds := make([]string, 0, len(detail.Refs))
	seenRefKinds := map[string]struct{}{}
	for _, ref := range detail.Refs {
		refKind := strings.TrimSpace(ref.RefKind)
		if refKind == "" {
			continue
		}
		if _, ok := seenRefKinds[refKind]; ok {
			continue
		}
		seenRefKinds[refKind] = struct{}{}
		refKinds = append(refKinds, refKind)
	}
	sort.Strings(refKinds)

	driftState := ""
	driftScore := 0.0
	if detail.DriftReport != nil {
		driftState = strings.TrimSpace(detail.DriftReport.Status)
		driftScore = detail.DriftReport.Drift
	}

	record := detail.Node
	return MemoryNodeSearchHit{
		MemoryID:             record.MemoryID,
		WorkspaceID:          record.WorkspaceID,
		MemoryType:           record.MemoryType,
		CompatType:           record.CompatType,
		CanonicalAuthority:   record.CanonicalAuthority,
		SurfaceAuthority:     record.SurfaceAuthority,
		SurfaceRole:          record.SurfaceRole,
		CompatibilityOnly:    record.CompatibilityOnly,
		SemanticLineageID:    record.SemanticLineageID,
		Revision:             record.Revision,
		Protect:              record.Protect,
		Unresolved:           record.Unresolved,
		LastAnyAccess:        record.LastAnyAccess,
		LastTrustedAccess:    record.LastTrustedAccess,
		TLife:                record.TLife,
		RetentionBand:        record.RetentionBand,
		RetentionPrunable:    record.RetentionPrunable,
		RetentionGuardReason: record.RetentionGuardReason,
		RetentionHotUntil:    record.RetentionHotUntil,
		RetentionWarmUntil:   record.RetentionWarmUntil,
		RetentionExpiresAt:   record.RetentionExpiresAt,
		TemporalContracts:    append([]TemporalHorizonContract{}, record.TemporalContracts...),
		RecoveryCandidate:    record.RecoveryCandidate,
		RecoveryTriggerCount: record.RecoveryTriggerCount,
		RecoveryTriggerKinds: append([]string{}, record.RecoveryTriggerKinds...),
		RecoveryGuardReason:  record.RecoveryGuardReason,
		Visibility:           record.Visibility,
		MemoryLayer:          record.MemoryLayer,
		EpistemicStatus:      record.EpistemicStatus,
		LifecycleState:       record.LifecycleState,
		OriginKind:           record.OriginKind,
		OriginID:             record.OriginID,
		SourceKind:           record.SourceKind,
		SourceID:             record.SourceID,
		AgentID:              record.AgentID,
		SessionID:            record.SessionID,
		TaskID:               record.TaskID,
		Title:                strings.TrimSpace(record.Title),
		Summary:              firstNonEmpty(strings.TrimSpace(record.Summary), strings.TrimSpace(record.Title), strings.TrimSpace(record.ClaimSubject)),
		ClaimSubject:         strings.TrimSpace(record.ClaimSubject),
		ClaimPredicate:       strings.TrimSpace(record.ClaimPredicate),
		ClaimObject:          strings.TrimSpace(record.ClaimObject),
		ArchivedReason:       strings.TrimSpace(record.ArchivedReason),
		RecoveryReason:       strings.TrimSpace(record.RecoveryReason),
		Snippet:              buildMemoryNodeSearchSnippet(record, query),
		DriftState:           driftState,
		DriftScore:           driftScore,
		RefCount:             len(detail.Refs),
		VersionCount:         len(detail.Versions),
		OutboundEdgeCount:    len(detail.OutboundEdges),
		InboundEdgeCount:     len(detail.InboundEdges),
		RefKinds:             refKinds,
		UpdatedAt:            record.UpdatedAt,
	}
}

func buildMemoryNodeSearchSnippet(record MemoryGraphNodeRecord, query string) string {
	candidates := []string{
		strings.TrimSpace(record.Summary),
		strings.TrimSpace(record.Body),
		strings.TrimSpace(record.Title),
		strings.TrimSpace(record.ClaimSubject),
		strings.TrimSpace(record.ClaimObject),
	}
	if strings.TrimSpace(query) == "" {
		for _, candidate := range candidates {
			if candidate != "" {
				return clipMemoryNodeSearchSnippet(candidate, 220)
			}
		}
		return ""
	}
	lowerQuery := strings.ToLower(strings.TrimSpace(query))
	for _, candidate := range candidates {
		lowerCandidate := strings.ToLower(candidate)
		if idx := strings.Index(lowerCandidate, lowerQuery); idx >= 0 {
			start := maxInt(0, idx-60)
			end := minInt(len(candidate), idx+len(lowerQuery)+120)
			return clipMemoryNodeSearchSnippet(strings.TrimSpace(candidate[start:end]), 220)
		}
	}
	for _, candidate := range candidates {
		if candidate != "" {
			return clipMemoryNodeSearchSnippet(candidate, 220)
		}
	}
	return ""
}

func clipMemoryNodeSearchSnippet(text string, maxChars int) string {
	text = strings.TrimSpace(text)
	if maxChars <= 0 || len(text) <= maxChars {
		return text
	}
	if maxChars <= 3 {
		return text[:maxChars]
	}
	return strings.TrimSpace(text[:maxChars-3]) + "..."
}
