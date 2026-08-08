package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

func safeTaskStatus(status string) string {
	status = strings.TrimSpace(status)
	if status == "" {
		return "UNKNOWN"
	}
	return strings.ToUpper(status)
}

type MemoryGraphNodeInput struct {
	MemoryID            string
	WorkspaceID         string
	MemoryType          string
	CompatType          string
	SemanticLineageID   string
	Revision            int
	Protect             bool
	Unresolved          bool
	Visibility          string
	MemoryLayer         string
	EpistemicStatus     string
	LifecycleState      string
	OriginKind          string
	OriginID            string
	SourceKind          string
	SourceID            string
	AgentID             string
	SessionID           string
	TaskID              string
	Title               string
	Body                string
	Summary             string
	ClaimSubject        string
	ClaimPredicate      string
	ClaimObject         string
	ClaimQualifiersJSON string
	ClaimTimeScopeJSON  string
	ClaimModality       string
	SourceSetJSON       string
	ProvenanceJSON      string
	Temperature         float64
	Importance          float64
	Confidence          float64
	Activation          float64
	Drift               float64
	Volatility          float64
	PinStrength         float64
	ArchivedAt          *string
	ArchivedReason      string
	RecoveryReason      string
	CreatedAt           string
	UpdatedAt           string
}

type MemoryGraphNodeRefInput struct {
	MemoryID     string
	WorkspaceID  string
	RefKind      string
	RefID        string
	RefRole      string
	RefValue     string
	Weight       float64
	MetadataJSON string
}

type MemoryGraphNodeVersionInput struct {
	MemoryID     string
	WorkspaceID  string
	RefKind      string
	RefID        string
	VersionToken string
	Weight       float64
}

type MemoryGraphEdgeInput struct {
	EdgeID       string
	WorkspaceID  string
	FromMemoryID string
	ToMemoryID   string
	EdgeType     string
	SourceKind   string
	SourceID     string
	Weight       float64
	MetadataJSON string
}

type MemoryGraphNodeMetricInput struct {
	MemoryID     string
	WorkspaceID  string
	MetricKey    string
	MetricValue  float64
	MetricUnit   string
	MetricKind   string
	MetadataJSON string
}

type MemoryGraphNodeFilter struct {
	WorkspaceID     string
	MemoryType      string
	MemoryLayer     string
	Visibility      string
	EpistemicStatus string
	LifecycleState  string
	OriginKind      string
	OriginID        string
	SourceKind      string
	AgentID         string
	SessionID       string
	TaskID          string
	IncludeArchived bool
	Limit           int
}

type MemoryGraphNodeRecord struct {
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
	Body                 string                    `json:"body,omitempty"`
	Summary              string                    `json:"summary,omitempty"`
	ClaimSubject         string                    `json:"claim_subject,omitempty"`
	ClaimPredicate       string                    `json:"claim_predicate,omitempty"`
	ClaimObject          string                    `json:"claim_object,omitempty"`
	ClaimQualifiersJSON  string                    `json:"claim_qualifiers_json,omitempty"`
	ClaimTimeScopeJSON   string                    `json:"claim_time_scope_json,omitempty"`
	ClaimModality        string                    `json:"claim_modality,omitempty"`
	SourceSet            []string                  `json:"source_set,omitempty"`
	Provenance           []string                  `json:"provenance,omitempty"`
	Temperature          float64                   `json:"temperature"`
	Importance           float64                   `json:"importance"`
	Confidence           float64                   `json:"confidence"`
	Activation           float64                   `json:"activation"`
	Drift                float64                   `json:"drift"`
	Volatility           float64                   `json:"volatility"`
	PinStrength          float64                   `json:"pin_strength"`
	ArchivedAt           *string                   `json:"archived_at,omitempty"`
	ArchivedReason       string                    `json:"archived_reason,omitempty"`
	RecoveryReason       string                    `json:"recovery_reason,omitempty"`
	CreatedAt            string                    `json:"created_at"`
	UpdatedAt            string                    `json:"updated_at"`
}

type MemoryGraphNodeRefRecord struct {
	MemoryID     string  `json:"memory_id"`
	WorkspaceID  string  `json:"workspace_id"`
	RefKind      string  `json:"ref_kind"`
	RefID        string  `json:"ref_id"`
	RefRole      string  `json:"ref_role,omitempty"`
	RefValue     string  `json:"ref_value,omitempty"`
	Weight       float64 `json:"weight"`
	MetadataJSON string  `json:"metadata_json,omitempty"`
	CreatedAt    string  `json:"created_at"`
	UpdatedAt    string  `json:"updated_at"`
}

type MemoryGraphNodeVersionRecord struct {
	MemoryID     string  `json:"memory_id"`
	WorkspaceID  string  `json:"workspace_id"`
	RefKind      string  `json:"ref_kind"`
	RefID        string  `json:"ref_id"`
	VersionToken string  `json:"version_token"`
	Weight       float64 `json:"weight"`
	CreatedAt    string  `json:"created_at"`
	UpdatedAt    string  `json:"updated_at"`
}

type MemoryGraphEdgeRecord struct {
	EdgeID       string  `json:"edge_id"`
	WorkspaceID  string  `json:"workspace_id"`
	FromMemoryID string  `json:"from_memory_id"`
	ToMemoryID   string  `json:"to_memory_id"`
	EdgeType     string  `json:"edge_type"`
	SourceKind   string  `json:"source_kind"`
	SourceID     string  `json:"source_id,omitempty"`
	Weight       float64 `json:"weight"`
	MetadataJSON string  `json:"metadata_json,omitempty"`
	CreatedAt    string  `json:"created_at"`
	UpdatedAt    string  `json:"updated_at"`
}

type MemoryGraphNodeMetricRecord struct {
	MemoryID     string  `json:"memory_id"`
	WorkspaceID  string  `json:"workspace_id"`
	MetricKey    string  `json:"metric_key"`
	MetricValue  float64 `json:"metric_value"`
	MetricUnit   string  `json:"metric_unit,omitempty"`
	MetricKind   string  `json:"metric_kind,omitempty"`
	MetadataJSON string  `json:"metadata_json,omitempty"`
	CreatedAt    string  `json:"created_at"`
	UpdatedAt    string  `json:"updated_at"`
}

type MemoryGraphNodeDetail struct {
	TimeAuthority    WorkspaceTimeAuthority         `json:"time_authority"`
	BoundaryContract MemoryShapeBoundaryContract    `json:"boundary_contract"`
	Node             MemoryGraphNodeRecord          `json:"node"`
	Refs             []MemoryGraphNodeRefRecord     `json:"refs,omitempty"`
	Versions         []MemoryGraphNodeVersionRecord `json:"versions,omitempty"`
	DriftReport      *MemoryGraphDriftReport        `json:"drift_report,omitempty"`
	Metrics          []MemoryGraphNodeMetricRecord  `json:"metrics,omitempty"`
	OutboundEdges    []MemoryGraphEdgeRecord        `json:"outbound_edges,omitempty"`
	InboundEdges     []MemoryGraphEdgeRecord        `json:"inbound_edges,omitempty"`
}

func memoryGraphProjectionKindsForOriginKind(originKind string) []string {
	switch strings.ToLower(strings.TrimSpace(originKind)) {
	case "workspace_memory":
		return []string{memoryProjectionKindWorkspaceMemory}
	case "knowledge_claim":
		return []string{memoryProjectionKindKnowledgeClaim}
	case "episode_pack":
		return []string{memoryProjectionKindEpisodePack}
	default:
		return nil
	}
}

func parseMemoryGraphNodeID(memoryID string) (originKind string, originID string, ok bool) {
	parts := strings.SplitN(strings.TrimSpace(memoryID), ":", 3)
	if len(parts) != 3 || strings.TrimSpace(parts[0]) != "memnode" {
		return "", "", false
	}
	originKind = strings.TrimSpace(parts[1])
	originID = strings.TrimSpace(parts[2])
	if originKind == "" || originID == "" {
		return "", "", false
	}
	return originKind, originID, true
}

func (s *Store) MemoryGraphBoundaryContractForWorkspace(ctx context.Context, workspaceID, originKind string) (MemoryShapeBoundaryContract, error) {
	snapshot, err := s.memoryProjectionLagSnapshotForWorkspaceKinds(ctx, workspaceID, memoryGraphProjectionKindsForOriginKind(originKind))
	if err != nil {
		return MemoryShapeBoundaryContract{}, err
	}
	return memoryGraphBoundaryContractWithProjectionLag(snapshot), nil
}

func (s *Store) MemoryGraphBoundaryContractForMemoryID(ctx context.Context, workspaceID, memoryID string) (MemoryShapeBoundaryContract, error) {
	originKind, _, ok := parseMemoryGraphNodeID(memoryID)
	if !ok {
		return memoryGraphBoundaryContract(), nil
	}
	return s.MemoryGraphBoundaryContractForWorkspace(ctx, workspaceID, originKind)
}

type MemoryGraphSyncResult struct {
	WorkspaceID           string `json:"workspace_id"`
	WorkspaceMemorySynced int    `json:"workspace_memory_synced"`
	KnowledgeClaimsSynced int    `json:"knowledge_claims_synced"`
	EpisodePacksSynced    int    `json:"episode_packs_synced"`
}

const (
	memoryGraphAtlasDefaultDepth      = 1
	memoryGraphAtlasMaxDepth          = 2
	memoryGraphAtlasDefaultLimitNodes = 80
	memoryGraphAtlasMaxLimitNodes     = 120
	memoryGraphAtlasDefaultLimitEdges = 140
	memoryGraphAtlasMaxLimitEdges     = 220
)

type MemoryGraphAtlasRequest struct {
	WorkspaceID     string  `json:"workspace_id"`
	CenterMemoryID  string  `json:"center_memory_id,omitempty"`
	Query           string  `json:"query,omitempty"`
	MemoryType      string  `json:"memory_type,omitempty"`
	MemoryLayer     string  `json:"memory_layer,omitempty"`
	Visibility      string  `json:"visibility,omitempty"`
	EpistemicStatus string  `json:"epistemic_status,omitempty"`
	LifecycleState  string  `json:"lifecycle_state,omitempty"`
	OriginKind      string  `json:"origin_kind,omitempty"`
	IncludeAnchors  bool    `json:"include_anchors,omitempty"`
	IncludeArchived bool    `json:"include_archived,omitempty"`
	CanonicalOnly   bool    `json:"canonical_only,omitempty"`
	Depth           int     `json:"depth,omitempty"`
	LimitNodes      int     `json:"limit_nodes,omitempty"`
	LimitEdges      int     `json:"limit_edges,omitempty"`
	MinImportance   float64 `json:"min_importance,omitempty"`
	MinActivation   float64 `json:"min_activation,omitempty"`
}

func normalizeMemoryGraphAtlasRequest(req MemoryGraphAtlasRequest) MemoryGraphAtlasRequest {
	req.WorkspaceID = strings.TrimSpace(req.WorkspaceID)
	req.CenterMemoryID = strings.TrimSpace(req.CenterMemoryID)
	req.Query = strings.TrimSpace(req.Query)
	req.MemoryType = strings.TrimSpace(req.MemoryType)
	req.MemoryLayer = strings.TrimSpace(req.MemoryLayer)
	req.Visibility = strings.TrimSpace(req.Visibility)
	req.EpistemicStatus = strings.TrimSpace(req.EpistemicStatus)
	req.LifecycleState = strings.TrimSpace(req.LifecycleState)
	req.OriginKind = strings.TrimSpace(req.OriginKind)
	if req.Depth <= 0 {
		req.Depth = memoryGraphAtlasDefaultDepth
	}
	if req.Depth > memoryGraphAtlasMaxDepth {
		req.Depth = memoryGraphAtlasMaxDepth
	}
	if req.LimitNodes <= 0 {
		req.LimitNodes = memoryGraphAtlasDefaultLimitNodes
	}
	if req.LimitNodes > memoryGraphAtlasMaxLimitNodes {
		req.LimitNodes = memoryGraphAtlasMaxLimitNodes
	}
	if req.LimitEdges <= 0 {
		req.LimitEdges = memoryGraphAtlasDefaultLimitEdges
	}
	if req.LimitEdges > memoryGraphAtlasMaxLimitEdges {
		req.LimitEdges = memoryGraphAtlasMaxLimitEdges
	}
	req.MinImportance = clampUnitInterval(req.MinImportance)
	req.MinActivation = clampUnitInterval(req.MinActivation)
	return req
}

func (s *Store) ListMemoryGraphNodes(ctx context.Context, filter MemoryGraphNodeFilter) ([]MemoryGraphNodeRecord, error) {
	filter.WorkspaceID = strings.TrimSpace(filter.WorkspaceID)
	if filter.WorkspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if err := validateMemoryGraphNodeFilter(filter); err != nil {
		return nil, err
	}
	if filter.Limit <= 0 {
		filter.Limit = 50
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
	args := make([]any, 0, 12)
	where := buildMemoryGraphNodeWhere(filter, "n", &args)
	if len(where) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(where, " AND "))
	}
	query.WriteString(` ORDER BY n.updated_at DESC, n.importance DESC, n.memory_id DESC LIMIT ?`)
	args = append(args, filter.Limit)

	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list memory graph nodes: %w", err)
	}
	defer rows.Close()
	items, err := collectMemoryGraphNodeRows(rows)
	if err != nil {
		return nil, err
	}
	authority, err := s.GetWorkspaceTimeAuthority(ctx, filter.WorkspaceID)
	if err != nil {
		return nil, err
	}
	applyMemoryGraphRetentionToRecords(items, authority.ReferenceAt)
	if err := s.applyMemoryGraphDriftToNodes(ctx, filter.WorkspaceID, items); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Store) GetMemoryGraphNode(ctx context.Context, workspaceID, memoryID string) (MemoryGraphNodeDetail, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	memoryID = strings.TrimSpace(memoryID)
	if workspaceID == "" {
		return MemoryGraphNodeDetail{}, errors.New("workspace_id is required")
	}
	if memoryID == "" {
		return MemoryGraphNodeDetail{}, errors.New("memory_id is required")
	}

	node, err := s.loadMemoryGraphNode(ctx, workspaceID, memoryID)
	if err != nil {
		return MemoryGraphNodeDetail{}, err
	}
	boundary, err := s.MemoryGraphBoundaryContractForMemoryID(ctx, workspaceID, memoryID)
	if err != nil {
		return MemoryGraphNodeDetail{}, err
	}
	refs, err := s.listMemoryGraphNodeRefs(ctx, workspaceID, memoryID)
	if err != nil {
		return MemoryGraphNodeDetail{}, err
	}
	versions, err := s.listMemoryGraphNodeVersions(ctx, workspaceID, memoryID)
	if err != nil {
		return MemoryGraphNodeDetail{}, err
	}
	authority, err := s.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return MemoryGraphNodeDetail{}, err
	}
	applyMemoryGraphRetentionState(&node, authority.ReferenceAt)
	driftReport, err := s.buildMemoryGraphDriftReport(ctx, workspaceID, versions)
	if err != nil {
		return MemoryGraphNodeDetail{}, err
	}
	node.Drift = memoryGraphEffectiveDrift(node.Drift, driftReport)
	applyMemoryGraphRecoveryState(&node, &driftReport)
	metrics, err := s.listMemoryGraphNodeMetrics(ctx, workspaceID, memoryID)
	if err != nil {
		return MemoryGraphNodeDetail{}, err
	}
	outbound, err := s.listMemoryGraphEdges(ctx, workspaceID, memoryID, true)
	if err != nil {
		return MemoryGraphNodeDetail{}, err
	}
	inbound, err := s.listMemoryGraphEdges(ctx, workspaceID, memoryID, false)
	if err != nil {
		return MemoryGraphNodeDetail{}, err
	}

	return MemoryGraphNodeDetail{
		TimeAuthority:    authority,
		BoundaryContract: boundary,
		Node:             node,
		Refs:             refs,
		Versions:         versions,
		DriftReport:      &driftReport,
		Metrics:          metrics,
		OutboundEdges:    outbound,
		InboundEdges:     inbound,
	}, nil
}

func (s *Store) listMemoryGraphNodesByIDs(ctx context.Context, workspaceID string, memoryIDs []string) ([]MemoryGraphNodeRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	memoryIDs = uniqueTrimmedStrings(memoryIDs)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if len(memoryIDs) == 0 {
		return nil, nil
	}

	args := make([]any, 0, 1+len(memoryIDs))
	args = append(args, workspaceID)
	for _, memoryID := range memoryIDs {
		args = append(args, memoryID)
	}

	query := graphMemoryNodeSelectSQL() +
		` WHERE n.workspace_id = ? AND n.memory_id IN (` + graphPlaceholders(len(memoryIDs)) + `)` +
		` ORDER BY n.updated_at DESC, n.importance DESC, n.memory_id DESC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list memory graph nodes by ids: %w", err)
	}
	defer rows.Close()
	items, err := collectMemoryGraphNodeRows(rows)
	if err != nil {
		return nil, err
	}
	authority, err := s.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return nil, err
	}
	applyMemoryGraphRetentionToRecords(items, authority.ReferenceAt)
	if err := s.applyMemoryGraphDriftToNodes(ctx, workspaceID, items); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Store) listMemoryGraphEdgesByMemoryIDs(ctx context.Context, workspaceID string, memoryIDs []string, limit int) ([]MemoryGraphEdgeRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	memoryIDs = uniqueTrimmedStrings(memoryIDs)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if len(memoryIDs) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = memoryGraphAtlasDefaultLimitEdges
	}

	args := make([]any, 0, 2+len(memoryIDs)*2)
	args = append(args, workspaceID)
	for _, memoryID := range memoryIDs {
		args = append(args, memoryID)
	}
	for _, memoryID := range memoryIDs {
		args = append(args, memoryID)
	}
	args = append(args, limit)

	query := `SELECT edge_id, workspace_id, from_memory_id, to_memory_id, edge_type, source_kind, source_id, weight, metadata_json, created_at, updated_at
		FROM memory_edges
		WHERE workspace_id = ? AND (from_memory_id IN (` + graphPlaceholders(len(memoryIDs)) + `) OR to_memory_id IN (` + graphPlaceholders(len(memoryIDs)) + `))
		ORDER BY weight DESC, updated_at DESC, edge_id DESC
		LIMIT ?`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list memory graph edges by ids: %w", err)
	}
	defer rows.Close()
	return collectGraphMemoryEdgeRows(rows)
}

func (s *Store) listMemoryGraphAtlasOverviewSeeds(ctx context.Context, req MemoryGraphAtlasRequest) ([]string, error) {
	seedLimit := req.LimitNodes / 6
	if seedLimit < 10 {
		seedLimit = 10
	}
	if seedLimit > 24 {
		seedLimit = 24
	}

	filter := MemoryGraphNodeFilter{
		WorkspaceID:     req.WorkspaceID,
		MemoryType:      req.MemoryType,
		MemoryLayer:     req.MemoryLayer,
		Visibility:      req.Visibility,
		EpistemicStatus: req.EpistemicStatus,
		LifecycleState:  req.LifecycleState,
		OriginKind: firstNonEmpty(req.OriginKind, func() string {
			if req.CanonicalOnly {
				return "workspace_memory"
			}
			return ""
		}()),
		IncludeArchived: req.IncludeArchived || strings.EqualFold(req.LifecycleState, "ARCHIVED"),
		Limit:           maxInt(seedLimit*4, seedLimit+6),
	}
	records, err := s.ListMemoryGraphNodes(ctx, filter)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(records, func(i, j int) bool {
		if records[i].Activation != records[j].Activation {
			return records[i].Activation > records[j].Activation
		}
		if records[i].Importance != records[j].Importance {
			return records[i].Importance > records[j].Importance
		}
		if strings.TrimSpace(records[i].UpdatedAt) != strings.TrimSpace(records[j].UpdatedAt) {
			return strings.TrimSpace(records[i].UpdatedAt) > strings.TrimSpace(records[j].UpdatedAt)
		}
		return strings.TrimSpace(records[i].MemoryID) < strings.TrimSpace(records[j].MemoryID)
	})

	seedIDs := make([]string, 0, seedLimit)
	for _, record := range records {
		if !memoryGraphAtlasNodeVisible(record, req) {
			continue
		}
		seedIDs = append(seedIDs, strings.TrimSpace(record.MemoryID))
		if len(seedIDs) >= seedLimit {
			break
		}
	}
	return uniqueTrimmedStrings(seedIDs), nil
}

func (s *Store) normalizeMemoryGraphAtlasSeedID(ctx context.Context, workspaceID, memoryID string, canonicalOnly bool) (string, error) {
	memoryID = strings.TrimSpace(memoryID)
	if memoryID == "" {
		return "", nil
	}
	if !canonicalOnly {
		return memoryID, nil
	}
	originKind, originID, ok := parseMemoryGraphNodeID(memoryID)
	if ok && strings.EqualFold(originKind, "workspace_memory") {
		return memoryGraphNodeID("workspace_memory", originID), nil
	}
	detail, err := s.GetMemoryGraphNode(ctx, workspaceID, memoryID)
	if err != nil {
		return "", err
	}
	if strings.EqualFold(strings.TrimSpace(detail.Node.OriginKind), "workspace_memory") && strings.TrimSpace(detail.Node.OriginID) != "" {
		return memoryGraphNodeID("workspace_memory", detail.Node.OriginID), nil
	}
	for _, ref := range detail.Refs {
		if strings.EqualFold(strings.TrimSpace(ref.RefKind), "workspace_memory") && strings.TrimSpace(ref.RefID) != "" {
			return memoryGraphNodeID("workspace_memory", ref.RefID), nil
		}
	}
	return "", nil
}

func (s *Store) listMemoryGraphNodesBySemanticLineage(ctx context.Context, workspaceID, semanticLineageID string, limit int) ([]MemoryGraphNodeRecord, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	semanticLineageID = strings.TrimSpace(semanticLineageID)
	if workspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if semanticLineageID == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 8
	}
	rows, err := s.db.QueryContext(ctx, graphMemoryNodeSelectSQL()+`
	 WHERE n.workspace_id = ? AND n.semantic_lineage_id = ?
	 ORDER BY n.updated_at DESC, n.importance DESC, n.memory_id DESC
	 LIMIT ?`, workspaceID, semanticLineageID, limit)
	if err != nil {
		return nil, fmt.Errorf("list memory graph nodes by semantic lineage: %w", err)
	}
	defer rows.Close()
	items, err := collectMemoryGraphNodeRows(rows)
	if err != nil {
		return nil, err
	}
	authority, err := s.GetWorkspaceTimeAuthority(ctx, workspaceID)
	if err != nil {
		return nil, err
	}
	applyMemoryGraphRetentionToRecords(items, authority.ReferenceAt)
	if err := s.applyMemoryGraphDriftToNodes(ctx, workspaceID, items); err != nil {
		return nil, err
	}
	return items, nil
}

func memoryGraphAtlasEdgePriority(edge GraphEdge) int {
	switch strings.TrimSpace(edge.Label) {
	case "semantic_lineage":
		return 40
	case "anchors_memory", "emits_memory", "holds_memory":
		return 35
	}
	if strings.EqualFold(strings.TrimSpace(edge.Authority), "authoritative") {
		return 25
	}
	if strings.TrimSpace(edge.SourceModel) == "lineage" {
		return 30
	}
	return 10
}

func memoryGraphAtlasNodeVisible(record MemoryGraphNodeRecord, req MemoryGraphAtlasRequest) bool {
	if req.CanonicalOnly && !strings.EqualFold(strings.TrimSpace(record.OriginKind), "workspace_memory") {
		return false
	}
	if strings.TrimSpace(req.MemoryType) != "" {
		canonicalTypes, compatTypes := memoryGraphFilterTypes(req.MemoryType)
		typeMatch := false
		for _, value := range canonicalTypes {
			if strings.EqualFold(strings.TrimSpace(record.MemoryType), strings.TrimSpace(value)) {
				typeMatch = true
				break
			}
		}
		if !typeMatch {
			for _, value := range compatTypes {
				if strings.EqualFold(strings.TrimSpace(record.CompatType), strings.TrimSpace(value)) {
					typeMatch = true
					break
				}
			}
		}
		if !typeMatch {
			return false
		}
	}
	if strings.TrimSpace(req.MemoryLayer) != "" && !strings.EqualFold(strings.TrimSpace(record.MemoryLayer), strings.TrimSpace(req.MemoryLayer)) {
		return false
	}
	if strings.TrimSpace(req.Visibility) != "" && !strings.EqualFold(strings.TrimSpace(record.Visibility), strings.TrimSpace(req.Visibility)) {
		return false
	}
	if strings.TrimSpace(req.EpistemicStatus) != "" && !strings.EqualFold(strings.TrimSpace(record.EpistemicStatus), strings.TrimSpace(req.EpistemicStatus)) {
		return false
	}
	if strings.TrimSpace(req.LifecycleState) != "" && !strings.EqualFold(strings.TrimSpace(record.LifecycleState), strings.TrimSpace(req.LifecycleState)) {
		return false
	}
	if strings.TrimSpace(req.OriginKind) != "" && !strings.EqualFold(strings.TrimSpace(record.OriginKind), strings.TrimSpace(req.OriginKind)) {
		return false
	}
	if !req.IncludeArchived && strings.TrimSpace(record.LifecycleState) == "ARCHIVED" {
		return false
	}
	if record.Importance < req.MinImportance {
		return false
	}
	if record.Activation < req.MinActivation {
		return false
	}
	return true
}

func (s *Store) GetMemoryGraphAtlas(ctx context.Context, req MemoryGraphAtlasRequest) (*GraphSnapshot, error) {
	req = normalizeMemoryGraphAtlasRequest(req)
	if req.WorkspaceID == "" {
		return nil, errors.New("workspace_id is required")
	}
	if err := validateOptionalMemoryGraphType(req.MemoryType); err != nil {
		return nil, err
	}
	if err := validateOptionalMemoryGraphOriginKind(req.OriginKind); err != nil {
		return nil, err
	}
	if req.CanonicalOnly && strings.TrimSpace(req.OriginKind) != "" && !strings.EqualFold(strings.TrimSpace(req.OriginKind), "workspace_memory") {
		return nil, errors.New("canonical_only requires origin_kind to be empty or workspace_memory")
	}

	nowStr := time.Now().UTC().Format(time.RFC3339Nano)
	authority, err := s.GetWorkspaceTimeAuthority(ctx, req.WorkspaceID)
	if err != nil {
		return nil, err
	}
	snap := newGraphSnapshot(nowStr, authority, "MEMORY_ATLAS", req.CenterMemoryID, req.LimitNodes)
	stats := snap.Stats.(map[string]any)
	stats["view"] = "memory_atlas"
	stats["supports_focus"] = true
	stats["focus_type"] = "memory_node"
	stats["node_budget"] = req.LimitNodes
	stats["edge_budget"] = req.LimitEdges
	stats["depth"] = req.Depth
	stats["canonical_only"] = req.CanonicalOnly
	stats["anchors_included"] = req.IncludeAnchors
	if req.Query != "" {
		stats["query"] = req.Query
	}
	if req.MemoryType != "" {
		stats["memory_type"] = req.MemoryType
	}
	if req.MemoryLayer != "" {
		stats["memory_layer"] = req.MemoryLayer
	}
	if req.Visibility != "" {
		stats["visibility"] = req.Visibility
	}
	if req.EpistemicStatus != "" {
		stats["epistemic_status"] = req.EpistemicStatus
	}
	if req.LifecycleState != "" {
		stats["lifecycle_state"] = req.LifecycleState
	}
	if req.OriginKind != "" {
		stats["origin_kind"] = req.OriginKind
	}
	if req.IncludeArchived {
		stats["include_archived"] = true
	}
	if req.MinImportance > 0 {
		stats["min_importance"] = req.MinImportance
	}
	if req.MinActivation > 0 {
		stats["min_activation"] = req.MinActivation
	}

	seedIDs := make([]string, 0)
	if req.CenterMemoryID != "" {
		seedID, err := s.normalizeMemoryGraphAtlasSeedID(ctx, req.WorkspaceID, req.CenterMemoryID, req.CanonicalOnly)
		if err != nil {
			return nil, err
		}
		if seedID != "" {
			seedIDs = append(seedIDs, seedID)
			snap.Focus = seedID
			stats["center_memory_id"] = seedID
		}
	} else if req.Query != "" {
		querySeedLimit := req.LimitNodes / 5
		if querySeedLimit < 10 {
			querySeedLimit = 10
		}
		if querySeedLimit > 24 {
			querySeedLimit = 24
		}
		originKind := strings.TrimSpace(req.OriginKind)
		if originKind == "" && req.CanonicalOnly {
			originKind = "workspace_memory"
		}
		search, err := s.SearchMemoryNodes(ctx, MemoryNodeSearchFilter{
			WorkspaceID:     req.WorkspaceID,
			Query:           req.Query,
			MemoryType:      req.MemoryType,
			MemoryLayer:     req.MemoryLayer,
			Visibility:      req.Visibility,
			EpistemicStatus: req.EpistemicStatus,
			LifecycleState:  req.LifecycleState,
			OriginKind:      originKind,
			IncludeArchived: req.IncludeArchived || strings.EqualFold(req.LifecycleState, "ARCHIVED"),
			Limit:           maxInt(querySeedLimit*3, querySeedLimit),
		})
		if err != nil {
			return nil, err
		}
		for _, hit := range search.Hits {
			seedID, err := s.normalizeMemoryGraphAtlasSeedID(ctx, req.WorkspaceID, hit.MemoryID, req.CanonicalOnly)
			if err != nil {
				return nil, err
			}
			if seedID != "" {
				seedIDs = append(seedIDs, seedID)
			}
			if len(seedIDs) >= 8 {
				break
			}
		}
	} else {
		var err error
		seedIDs, err = s.listMemoryGraphAtlasOverviewSeeds(ctx, req)
		if err != nil {
			return nil, err
		}
	}
	seedIDs = uniqueTrimmedStrings(seedIDs)
	stats["seed_count"] = len(seedIDs)
	if len(seedIDs) == 0 {
		stats["memory_node_count"] = 0
		stats["memory_edge_count"] = 0
		stats["empty_reason"] = "no_matching_memory_seeds"
		return snap, nil
	}

	candidateIDs := append([]string{}, seedIDs...)
	seenIDs := make(map[string]struct{}, len(candidateIDs))
	for _, memoryID := range candidateIDs {
		seenIDs[memoryID] = struct{}{}
	}
	frontier := append([]string{}, seedIDs...)
	edgeSeen := make(map[string]struct{})
	edgeRecords := make([]MemoryGraphEdgeRecord, 0)
	expansionNodeBudget := req.LimitNodes * 2
	if expansionNodeBudget < req.LimitNodes+12 {
		expansionNodeBudget = req.LimitNodes + 12
	}
	frontierHops := 0

	for depth := 0; depth < req.Depth && len(frontier) > 0 && len(candidateIDs) < expansionNodeBudget; depth++ {
		incidentEdges, err := s.listMemoryGraphEdgesByMemoryIDs(ctx, req.WorkspaceID, frontier, maxInt(req.LimitEdges*3, 64))
		if err != nil {
			return nil, err
		}
		nextFrontier := make([]string, 0)
		for _, edge := range incidentEdges {
			edgeID := strings.TrimSpace(edge.EdgeID)
			if edgeID != "" {
				if _, exists := edgeSeen[edgeID]; exists {
					continue
				}
				edgeSeen[edgeID] = struct{}{}
			}
			edgeRecords = append(edgeRecords, edge)
			for _, endpoint := range []string{strings.TrimSpace(edge.FromMemoryID), strings.TrimSpace(edge.ToMemoryID)} {
				if endpoint == "" {
					continue
				}
				if _, exists := seenIDs[endpoint]; exists {
					continue
				}
				seenIDs[endpoint] = struct{}{}
				candidateIDs = append(candidateIDs, endpoint)
				nextFrontier = append(nextFrontier, endpoint)
				if len(candidateIDs) >= expansionNodeBudget {
					break
				}
			}
			if len(candidateIDs) >= expansionNodeBudget {
				break
			}
		}
		frontierHops = depth + 1
		frontier = uniqueTrimmedStrings(nextFrontier)
	}

	nodeRecords, err := s.listMemoryGraphNodesByIDs(ctx, req.WorkspaceID, candidateIDs)
	if err != nil {
		return nil, err
	}
	recordByID := make(map[string]MemoryGraphNodeRecord, len(nodeRecords))
	for _, record := range nodeRecords {
		recordByID[strings.TrimSpace(record.MemoryID)] = record
	}
	if req.CenterMemoryID != "" {
		if centerID := strings.TrimSpace(snap.Focus); centerID != "" {
			if center, ok := recordByID[centerID]; ok && strings.TrimSpace(center.SemanticLineageID) != "" {
				lineageRecords, err := s.listMemoryGraphNodesBySemanticLineage(ctx, req.WorkspaceID, center.SemanticLineageID, 8)
				if err != nil {
					return nil, err
				}
				for _, record := range lineageRecords {
					recordID := strings.TrimSpace(record.MemoryID)
					if recordID == "" {
						continue
					}
					if _, exists := recordByID[recordID]; exists {
						continue
					}
					nodeRecords = append(nodeRecords, record)
					recordByID[recordID] = record
				}
			}
		}
	}

	seedSet := make(map[string]struct{}, len(seedIDs))
	for _, memoryID := range seedIDs {
		seedSet[memoryID] = struct{}{}
	}

	filtered := make([]MemoryGraphNodeRecord, 0, len(nodeRecords))
	focusSeedID := strings.TrimSpace(snap.Focus)
	for _, record := range nodeRecords {
		recordID := strings.TrimSpace(record.MemoryID)
		if _, isSeed := seedSet[recordID]; isSeed {
			if recordID == focusSeedID || memoryGraphAtlasNodeVisible(record, req) {
				filtered = append(filtered, record)
			}
			continue
		}
		if memoryGraphAtlasNodeVisible(record, req) {
			filtered = append(filtered, record)
		}
	}

	sort.SliceStable(filtered, func(i, j int) bool {
		_, leftSeed := seedSet[strings.TrimSpace(filtered[i].MemoryID)]
		_, rightSeed := seedSet[strings.TrimSpace(filtered[j].MemoryID)]
		if leftSeed != rightSeed {
			return leftSeed
		}
		if filtered[i].Activation != filtered[j].Activation {
			return filtered[i].Activation > filtered[j].Activation
		}
		if filtered[i].Importance != filtered[j].Importance {
			return filtered[i].Importance > filtered[j].Importance
		}
		if strings.TrimSpace(filtered[i].UpdatedAt) != strings.TrimSpace(filtered[j].UpdatedAt) {
			return strings.TrimSpace(filtered[i].UpdatedAt) > strings.TrimSpace(filtered[j].UpdatedAt)
		}
		return strings.TrimSpace(filtered[i].MemoryID) < strings.TrimSpace(filtered[j].MemoryID)
	})

	nodesClipped := false
	if len(filtered) > req.LimitNodes {
		filtered = filtered[:req.LimitNodes]
		nodesClipped = true
	}

	visibleMemoryIDs := make(map[string]struct{}, len(filtered))
	visibleLineageCounts := make(map[string]int)
	for _, record := range filtered {
		visibleMemoryIDs[strings.TrimSpace(record.MemoryID)] = struct{}{}
		if lineageID := strings.TrimSpace(record.SemanticLineageID); lineageID != "" {
			visibleLineageCounts[lineageID]++
		}
		snap.Nodes = append(snap.Nodes, graphMemoryNodeFromRecord(record))
	}

	finalEdges := make([]GraphEdge, 0, len(edgeRecords))
	for _, edge := range edgeRecords {
		fromID := strings.TrimSpace(edge.FromMemoryID)
		toID := strings.TrimSpace(edge.ToMemoryID)
		if _, ok := visibleMemoryIDs[fromID]; !ok {
			continue
		}
		if _, ok := visibleMemoryIDs[toID]; !ok {
			continue
		}
		finalEdges = append(finalEdges, GraphEdge{
			Source:      graphMemoryNodeID(fromID),
			Target:      graphMemoryNodeID(toID),
			Label:       firstNonEmpty(strings.TrimSpace(edge.EdgeType), "memory_link"),
			Semantics:   "solid",
			Authority:   "authoritative",
			Strength:    edge.Weight,
			SourceModel: firstNonEmpty(strings.TrimSpace(edge.SourceKind), "memory"),
		})
	}
	lineageEdgeCount := 0
	if centerID := strings.TrimSpace(snap.Focus); centerID != "" {
		if center, ok := recordByID[centerID]; ok {
			centerLineageID := strings.TrimSpace(center.SemanticLineageID)
			if centerLineageID != "" && visibleLineageCounts[centerLineageID] > 1 {
				for _, record := range filtered {
					recordID := strings.TrimSpace(record.MemoryID)
					if recordID == "" || recordID == centerID {
						continue
					}
					if !strings.EqualFold(strings.TrimSpace(record.SemanticLineageID), centerLineageID) {
						continue
					}
					finalEdges = append(finalEdges, GraphEdge{
						Source:      graphMemoryNodeID(centerID),
						Target:      graphMemoryNodeID(recordID),
						Label:       "semantic_lineage",
						Semantics:   "dashed",
						Authority:   "derived",
						SourceModel: "lineage",
					})
					lineageEdgeCount++
				}
			}
		}
	}
	sort.SliceStable(finalEdges, func(i, j int) bool {
		if finalEdges[i].Strength != finalEdges[j].Strength {
			return finalEdges[i].Strength > finalEdges[j].Strength
		}
		return finalEdges[i].Label < finalEdges[j].Label
	})
	edgesClipped := false
	if len(finalEdges) > req.LimitEdges {
		finalEdges = finalEdges[:req.LimitEdges]
		edgesClipped = true
	}
	snap.Edges = append(snap.Edges, finalEdges...)

	if req.IncludeAnchors {
		taskIDs := make([]string, 0)
		sessionIDs := make([]string, 0)
		agentIDs := make([]string, 0)
		taskSeen := map[string]struct{}{}
		sessionSeen := map[string]struct{}{}
		agentSeen := map[string]struct{}{}
		for _, record := range filtered {
			if taskID := strings.TrimSpace(record.TaskID); taskID != "" {
				if _, exists := taskSeen[taskID]; !exists {
					taskSeen[taskID] = struct{}{}
					taskIDs = append(taskIDs, taskID)
				}
			}
			if sessionID := strings.TrimSpace(record.SessionID); sessionID != "" {
				if _, exists := sessionSeen[sessionID]; !exists {
					sessionSeen[sessionID] = struct{}{}
					sessionIDs = append(sessionIDs, sessionID)
				}
			}
			if agentID := strings.TrimSpace(record.AgentID); agentID != "" {
				if _, exists := agentSeen[agentID]; !exists {
					agentSeen[agentID] = struct{}{}
					agentIDs = append(agentIDs, agentID)
				}
			}
		}

		if len(taskIDs) > 0 {
			args := []any{req.WorkspaceID}
			for _, taskID := range taskIDs {
				args = append(args, taskID)
			}
			rows, err := s.db.QueryContext(ctx, `SELECT wt.task_id, t.title, t.status
				FROM workspace_tasks wt
				JOIN tasks t ON t.task_id = wt.task_id
				WHERE wt.workspace_id = ? AND wt.task_id IN (`+graphPlaceholders(len(taskIDs))+`)
				ORDER BY wt.task_id`, args...)
			if err != nil {
				return nil, err
			}
			for rows.Next() {
				var id, title, status string
				if err := rows.Scan(&id, &title, &status); err != nil {
					_ = rows.Close()
					return nil, err
				}
				snap.Nodes = append(snap.Nodes, GraphNode{
					ID:     strings.TrimSpace(id),
					RefID:  strings.TrimSpace(id),
					Label:  firstNonEmpty(strings.TrimSpace(title), strings.TrimSpace(id)),
					Type:   "task",
					Status: safeTaskStatus(status),
				})
			}
			if err := rows.Err(); err != nil {
				_ = rows.Close()
				return nil, err
			}
			_ = rows.Close()
		}
		if len(sessionIDs) > 0 {
			args := []any{req.WorkspaceID}
			for _, sessionID := range sessionIDs {
				args = append(args, sessionID)
			}
			rows, err := s.db.QueryContext(ctx, `SELECT session_id, status
				FROM agent_sessions
				WHERE workspace_id = ? AND session_id IN (`+graphPlaceholders(len(sessionIDs))+`)
				ORDER BY session_id`, args...)
			if err != nil {
				return nil, err
			}
			for rows.Next() {
				var id, status string
				if err := rows.Scan(&id, &status); err != nil {
					_ = rows.Close()
					return nil, err
				}
				shortSessionID := shortGraphSessionID(id)
				snap.Nodes = append(snap.Nodes, GraphNode{
					ID:     strings.TrimSpace(id),
					RefID:  strings.TrimSpace(id),
					Label:  "Session " + shortSessionID,
					Type:   "session",
					Status: firstNonEmpty(strings.TrimSpace(status), "ACTIVE"),
				})
			}
			if err := rows.Err(); err != nil {
				_ = rows.Close()
				return nil, err
			}
			_ = rows.Close()
		}
		if len(agentIDs) > 0 {
			args := []any{req.WorkspaceID}
			for _, agentID := range agentIDs {
				args = append(args, agentID)
			}
			rows, err := s.db.QueryContext(ctx, `SELECT agent_id, display_name, status, last_seen_at
				FROM agents
				WHERE workspace_id = ? AND agent_id IN (`+graphPlaceholders(len(agentIDs))+`)
				ORDER BY COALESCE(display_name, agent_id), agent_id`, args...)
			if err != nil {
				return nil, err
			}
			for rows.Next() {
				var id, displayName, status string
				var lastSeen sql.NullString
				if err := rows.Scan(&id, &displayName, &status, &lastSeen); err != nil {
					_ = rows.Close()
					return nil, err
				}
				snap.Nodes = append(snap.Nodes, GraphNode{
					ID:     strings.TrimSpace(id),
					RefID:  strings.TrimSpace(id),
					Label:  firstNonEmpty(strings.TrimSpace(displayName), strings.TrimSpace(id)),
					Type:   "agent",
					Status: safeAgentStatus(status, lastSeen),
				})
			}
			if err := rows.Err(); err != nil {
				_ = rows.Close()
				return nil, err
			}
			_ = rows.Close()
		}

		nodeSeen := make(map[string]struct{}, len(snap.Nodes))
		dedupedNodes := make([]GraphNode, 0, len(snap.Nodes))
		for _, node := range snap.Nodes {
			if _, exists := nodeSeen[node.ID]; exists {
				continue
			}
			nodeSeen[node.ID] = struct{}{}
			dedupedNodes = append(dedupedNodes, node)
		}
		snap.Nodes = dedupedNodes

		for _, record := range filtered {
			memoryNodeID := graphMemoryNodeID(strings.TrimSpace(record.MemoryID))
			if taskID := strings.TrimSpace(record.TaskID); taskID != "" {
				snap.Edges = append(snap.Edges, GraphEdge{
					Source:      strings.TrimSpace(taskID),
					Target:      memoryNodeID,
					Label:       "anchors_memory",
					Semantics:   "muted",
					Authority:   "authoritative",
					SourceModel: "memory",
				})
			}
			if sessionID := strings.TrimSpace(record.SessionID); sessionID != "" {
				snap.Edges = append(snap.Edges, GraphEdge{
					Source:      strings.TrimSpace(sessionID),
					Target:      memoryNodeID,
					Label:       "emits_memory",
					Semantics:   "muted",
					Authority:   "authoritative",
					SourceModel: "memory",
				})
			}
			if agentID := strings.TrimSpace(record.AgentID); agentID != "" {
				snap.Edges = append(snap.Edges, GraphEdge{
					Source:      strings.TrimSpace(agentID),
					Target:      memoryNodeID,
					Label:       "holds_memory",
					Semantics:   "muted",
					Authority:   "authoritative",
					SourceModel: "memory",
				})
			}
		}
	}

	anchorNodeCount := len(snap.Nodes) - len(filtered)
	if len(snap.Nodes) > req.LimitNodes {
		snap.Nodes = snap.Nodes[:req.LimitNodes]
		nodesClipped = true
	}

	visibleNodeIDs := make(map[string]struct{}, len(snap.Nodes))
	for _, node := range snap.Nodes {
		visibleNodeIDs[node.ID] = struct{}{}
	}

	trimmedEdges := make([]GraphEdge, 0, len(snap.Edges))
	for _, edge := range snap.Edges {
		if _, ok := visibleNodeIDs[strings.TrimSpace(edge.Source)]; !ok {
			edgesClipped = true
			continue
		}
		if _, ok := visibleNodeIDs[strings.TrimSpace(edge.Target)]; !ok {
			edgesClipped = true
			continue
		}
		trimmedEdges = append(trimmedEdges, edge)
	}
	sort.SliceStable(trimmedEdges, func(i, j int) bool {
		leftPriority := memoryGraphAtlasEdgePriority(trimmedEdges[i])
		rightPriority := memoryGraphAtlasEdgePriority(trimmedEdges[j])
		if leftPriority != rightPriority {
			return leftPriority > rightPriority
		}
		if trimmedEdges[i].Strength != trimmedEdges[j].Strength {
			return trimmedEdges[i].Strength > trimmedEdges[j].Strength
		}
		if trimmedEdges[i].Label != trimmedEdges[j].Label {
			return trimmedEdges[i].Label < trimmedEdges[j].Label
		}
		if trimmedEdges[i].Source != trimmedEdges[j].Source {
			return trimmedEdges[i].Source < trimmedEdges[j].Source
		}
		return trimmedEdges[i].Target < trimmedEdges[j].Target
	})
	if len(trimmedEdges) > req.LimitEdges {
		trimmedEdges = trimmedEdges[:req.LimitEdges]
		edgesClipped = true
	}
	snap.Edges = trimmedEdges

	visibleSeedCount := 0
	seedSourceCounts := map[string]int{}
	for _, record := range filtered {
		recordID := strings.TrimSpace(record.MemoryID)
		if _, ok := seedSet[recordID]; ok {
			visibleSeedCount++
			seedSourceCounts[firstNonEmpty(strings.TrimSpace(record.OriginKind), "unknown")]++
		}
	}
	stats["seed_count"] = visibleSeedCount
	stats["memory_node_count"] = len(filtered)
	stats["memory_edge_count"] = len(finalEdges)
	stats["returned_node_count"] = len(snap.Nodes)
	stats["returned_edge_count"] = len(snap.Edges)
	stats["expanded_node_count"] = len(candidateIDs)
	stats["frontier_hops"] = frontierHops
	stats["anchor_node_count"] = maxInt(anchorNodeCount, 0)
	stats["lineage_edge_count"] = lineageEdgeCount
	stats["dropped_by_node_budget"] = maxInt(len(filtered)+maxInt(anchorNodeCount, 0)-len(snap.Nodes), 0)
	stats["dropped_by_edge_budget"] = maxInt(len(finalEdges)-len(snap.Edges), 0)
	stats["clipped_nodes"] = nodesClipped
	stats["clipped_edges"] = edgesClipped
	stats["overview"] = snap.Focus == ""
	stats["seed_source_counts"] = seedSourceCounts
	return snap, nil
}

func (s *Store) SyncMemoryGraphWorkspace(ctx context.Context, workspaceID string) (MemoryGraphSyncResult, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return MemoryGraphSyncResult{}, errors.New("workspace_id is required")
	}

	tx, err := s.BeginTxImmediate(ctx)
	if err != nil {
		return MemoryGraphSyncResult{}, fmt.Errorf("begin memory graph sync tx: %w", err)
	}
	if err := s.ensureWorkspaceExistsTx(ctx, tx, workspaceID); err != nil {
		_ = tx.Rollback()
		return MemoryGraphSyncResult{}, err
	}

	result := MemoryGraphSyncResult{WorkspaceID: workspaceID}

	memoryRows, err := tx.QueryContext(
		ctx,
		`SELECT memory_id, workspace_id, memory_type, title, body, summary,
		        COALESCE(agent_id,''), COALESCE(session_id,''), COALESCE(task_id,''),
		        source_kind, source_id, tags_json, importance, confidence,
		        created_at, updated_at, archived_at, archived_by, archived_reason, recovery_reason
		   FROM workspace_memory
		  WHERE workspace_id = ?
		  ORDER BY updated_at ASC, memory_id ASC`,
		workspaceID,
	)
	if err != nil {
		_ = tx.Rollback()
		return MemoryGraphSyncResult{}, fmt.Errorf("query workspace memory for graph sync: %w", err)
	}
	defer memoryRows.Close()
	memoryRecords, err := collectWorkspaceMemoryRows(memoryRows)
	if err != nil {
		_ = tx.Rollback()
		return MemoryGraphSyncResult{}, err
	}
	for _, record := range memoryRecords {
		if err := s.syncWorkspaceMemoryGraphTx(ctx, tx, record); err != nil {
			_ = tx.Rollback()
			return MemoryGraphSyncResult{}, err
		}
		result.WorkspaceMemorySynced++
	}

	claimRows, err := tx.QueryContext(
		ctx,
		`SELECT `+knowledgeClaimSelectColumns("")+`
		   FROM knowledge_claims
		  WHERE workspace_id = ?
		  ORDER BY updated_at ASC, claim_id ASC`,
		workspaceID,
	)
	if err != nil {
		_ = tx.Rollback()
		return MemoryGraphSyncResult{}, fmt.Errorf("query knowledge claims for graph sync: %w", err)
	}
	claimRecords, err := collectKnowledgeClaimRows(claimRows)
	_ = claimRows.Close()
	if err != nil {
		_ = tx.Rollback()
		return MemoryGraphSyncResult{}, err
	}
	result.KnowledgeClaimsSynced = len(claimRecords)

	snapshotRows, err := tx.QueryContext(
		ctx,
		`SELECT snapshot_id, session_id, workspace_id, agent_id, trigger_kind, token_budget,
		        message_count_before, message_count_after, message_tokens_before, message_tokens_after,
		        total_input_tokens, total_output_tokens, summary_text, COALESCE(summary_workspace_memory,''), created_at,
		        '' AS task_id, '' AS pack_mode, '' AS source_window_digest, '' AS episode_pack_id, '' AS canonical_memory_id
		   FROM session_compaction_snapshots
		  WHERE workspace_id = ?
		  ORDER BY created_at ASC, snapshot_id ASC`,
		workspaceID,
	)
	if err != nil {
		_ = tx.Rollback()
		return MemoryGraphSyncResult{}, fmt.Errorf("query session compaction snapshots for graph sync: %w", err)
	}
	snapshotRecords, err := collectSessionCompactionSnapshotRows(snapshotRows)
	_ = snapshotRows.Close()
	if err != nil {
		_ = tx.Rollback()
		return MemoryGraphSyncResult{}, err
	}
	for _, snapshot := range snapshotRecords {
		taskID, sessionStatus, err := loadEpisodePackSessionContextTx(ctx, tx, snapshot.SessionID)
		if err != nil {
			_ = tx.Rollback()
			return MemoryGraphSyncResult{}, err
		}
		pack, err := s.recordCompactionEpisodePackTx(ctx, tx, snapshot, episodePackCompactionContext{
			TaskID:        taskID,
			SessionStatus: sessionStatus,
		})
		if err != nil {
			_ = tx.Rollback()
			return MemoryGraphSyncResult{}, err
		}
		_ = pack
		result.EpisodePacksSynced++
	}

	if err := tx.Commit(); err != nil {
		return MemoryGraphSyncResult{}, fmt.Errorf("commit memory graph sync tx: %w", err)
	}
	if _, err := s.RebuildMemoryProjectionWorkspace(ctx, MemoryProjectionRebuildFilter{
		WorkspaceID: workspaceID,
		Kinds: []string{
			memoryProjectionKindKnowledgeClaim,
			memoryProjectionKindEpisodePack,
		},
		Limit: maxInt(result.KnowledgeClaimsSynced+result.EpisodePacksSynced, memoryProjectionDefaultReconcileLimit),
	}); err != nil {
		return result, err
	}
	return result, nil
}

func (s *Store) syncWorkspaceMemoryGraphTx(ctx context.Context, tx *sql.Tx, record WorkspaceMemoryRecord) error {
	node, refs, versions := memoryGraphNodeFromWorkspaceMemory(record)
	grounding, err := s.memoryGraphGroundingForWorkspaceMemoryTx(ctx, tx, node.MemoryID, record)
	if err != nil {
		return err
	}
	refs = uniqueMemoryGraphNodeRefs(append(refs, grounding.refs...))
	versions = uniqueMemoryGraphNodeVersions(append(versions, grounding.versions...))
	edges := uniqueMemoryGraphEdges(grounding.edges)
	if _, err := s.upsertMemoryGraphNodeTx(ctx, tx, node); err != nil {
		return err
	}
	if err := s.replaceMemoryGraphNodeRefsTx(ctx, tx, node.MemoryID, record.WorkspaceID, refs); err != nil {
		return err
	}
	if err := s.replaceMemoryGraphNodeVersionsTx(ctx, tx, node.MemoryID, record.WorkspaceID, versions); err != nil {
		return err
	}
	if err := s.replaceMemoryGraphNodeMetricsTx(ctx, tx, node.MemoryID, record.WorkspaceID, nil); err != nil {
		return err
	}
	return s.replaceMemoryGraphEdgesForSourceTx(ctx, tx, record.WorkspaceID, "workspace_memory", record.MemoryID, edges)
}

func (s *Store) syncWorkspaceMemoryGraphAnchorTx(ctx context.Context, tx *sql.Tx, record WorkspaceMemoryRecord) error {
	node, _, _ := memoryGraphNodeFromWorkspaceMemory(record)
	if _, err := s.upsertMemoryGraphNodeTx(ctx, tx, node); err != nil {
		return err
	}
	versions, err := s.memoryGraphAnchorVersionsForWorkspaceMemoryTx(ctx, tx, node.MemoryID, record)
	if err != nil {
		return err
	}
	return s.replaceMemoryGraphNodeVersionsTx(ctx, tx, node.MemoryID, record.WorkspaceID, versions)
}

func (s *Store) syncKnowledgeClaimGraphTx(ctx context.Context, tx *sql.Tx, record KnowledgeClaimRecord) error {
	syncNow := firstNonEmpty(strings.TrimSpace(record.UpdatedAt), strings.TrimSpace(record.CreatedAt), time.Now().UTC().Format(time.RFC3339Nano))
	if err := s.syncKnowledgeClaimRelationsTx(ctx, tx, record, syncNow); err != nil {
		return err
	}
	relations, err := s.listKnowledgeClaimRelationsTx(ctx, tx, record.WorkspaceID, record.ClaimID)
	if err != nil {
		return err
	}
	node, refs, versions, edges := memoryGraphNodeFromKnowledgeClaim(record, relations)
	grounding, err := s.memoryGraphGroundingForKnowledgeClaimTx(ctx, tx, node.MemoryID, record, relations)
	if err != nil {
		return err
	}
	refs = uniqueMemoryGraphNodeRefs(append(refs, grounding.refs...))
	versions = uniqueMemoryGraphNodeVersions(append(versions, grounding.versions...))
	edges = uniqueMemoryGraphEdges(append(edges, grounding.edges...))
	if _, err := s.upsertMemoryGraphNodeTx(ctx, tx, node); err != nil {
		return err
	}
	if err := s.replaceMemoryGraphNodeRefsTx(ctx, tx, node.MemoryID, record.WorkspaceID, refs); err != nil {
		return err
	}
	if err := s.replaceMemoryGraphNodeVersionsTx(ctx, tx, node.MemoryID, record.WorkspaceID, versions); err != nil {
		return err
	}
	if err := s.replaceMemoryGraphNodeMetricsTx(ctx, tx, node.MemoryID, record.WorkspaceID, nil); err != nil {
		return err
	}
	return s.replaceMemoryGraphEdgesForSourceTx(ctx, tx, record.WorkspaceID, "knowledge_claim", record.ClaimID, edges)
}

func (s *Store) upsertMemoryGraphNodeTx(ctx context.Context, tx *sql.Tx, input MemoryGraphNodeInput) (MemoryGraphNodeRecord, error) {
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	memoryID := strings.TrimSpace(input.MemoryID)
	originKind := strings.TrimSpace(input.OriginKind)
	originID := strings.TrimSpace(input.OriginID)
	if workspaceID == "" {
		return MemoryGraphNodeRecord{}, errors.New("workspace_id is required")
	}
	if memoryID == "" {
		return MemoryGraphNodeRecord{}, errors.New("memory_id is required")
	}
	if originKind == "" || originID == "" {
		return MemoryGraphNodeRecord{}, errors.New("origin_kind and origin_id are required")
	}
	now := firstNonEmpty(strings.TrimSpace(input.UpdatedAt), time.Now().UTC().Format(time.RFC3339Nano))
	createdAt := firstNonEmpty(strings.TrimSpace(input.CreatedAt), now)
	memoryType := normalizeMemoryGraphType(input.MemoryType)
	compatType := strings.ToUpper(strings.TrimSpace(input.CompatType))
	visibility := normalizeMemoryGraphVisibility(input.Visibility)
	memoryLayer := normalizeMemoryGraphLayer(input.MemoryLayer)
	epistemicStatus := normalizeMemoryGraphEpistemicStatus(input.EpistemicStatus)
	lifecycleState := normalizeMemoryGraphLifecycleState(input.LifecycleState)
	sourceKind := strings.TrimSpace(input.SourceKind)
	sourceID := strings.TrimSpace(input.SourceID)
	agentID := strings.TrimSpace(input.AgentID)
	sessionID := strings.TrimSpace(input.SessionID)
	taskID := strings.TrimSpace(input.TaskID)
	title := strings.TrimSpace(input.Title)
	body := strings.TrimSpace(input.Body)
	summary := strings.TrimSpace(input.Summary)
	claimSubject := strings.TrimSpace(input.ClaimSubject)
	claimPredicate := strings.TrimSpace(input.ClaimPredicate)
	claimObject := strings.TrimSpace(input.ClaimObject)
	claimQualifiersJSON := firstNonEmpty(strings.TrimSpace(input.ClaimQualifiersJSON), "{}")
	claimTimeScopeJSON := firstNonEmpty(strings.TrimSpace(input.ClaimTimeScopeJSON), "{}")
	claimModality := strings.TrimSpace(input.ClaimModality)
	sourceSetJSON := firstNonEmpty(strings.TrimSpace(input.SourceSetJSON), "[]")
	provenanceJSON := firstNonEmpty(strings.TrimSpace(input.ProvenanceJSON), "[]")
	temperature := clampUnitInterval(input.Temperature)
	importance := clampUnitInterval(input.Importance)
	confidence := clampUnitInterval(input.Confidence)
	activation := clampUnitInterval(input.Activation)
	drift := clampUnitInterval(input.Drift)
	volatility := clampUnitInterval(input.Volatility)
	pinStrength := clampUnitInterval(input.PinStrength)
	archivedAt := blankStringOrNil(derefString(input.ArchivedAt))
	archivedReason := strings.TrimSpace(input.ArchivedReason)
	recoveryReason := strings.TrimSpace(input.RecoveryReason)
	semanticLineageID := firstNonEmpty(strings.TrimSpace(input.SemanticLineageID), memoryGraphSemanticLineageID(originKind, originID, memoryID))
	protect := memoryGraphAnchorProtect(memoryLayer, pinStrength, input.Protect)
	unresolved := memoryGraphAnchorUnresolved(epistemicStatus, input.Unresolved)

	var previous *MemoryGraphNodeRecord
	if existing, err := s.loadMemoryGraphNodeTx(ctx, tx, workspaceID, memoryID); err == nil {
		previous = &existing
	} else if !errors.Is(err, sql.ErrNoRows) {
		return MemoryGraphNodeRecord{}, err
	}
	nextRecord := MemoryGraphNodeRecord{
		MemoryID:            memoryID,
		WorkspaceID:         workspaceID,
		MemoryType:          memoryType,
		CompatType:          compatType,
		SemanticLineageID:   semanticLineageID,
		Protect:             protect,
		Unresolved:          unresolved,
		Visibility:          visibility,
		MemoryLayer:         memoryLayer,
		EpistemicStatus:     epistemicStatus,
		LifecycleState:      lifecycleState,
		OriginKind:          originKind,
		OriginID:            originID,
		SourceKind:          sourceKind,
		SourceID:            sourceID,
		AgentID:             agentID,
		SessionID:           sessionID,
		TaskID:              taskID,
		Title:               title,
		Body:                body,
		Summary:             summary,
		ClaimSubject:        claimSubject,
		ClaimPredicate:      claimPredicate,
		ClaimObject:         claimObject,
		ClaimQualifiersJSON: claimQualifiersJSON,
		ClaimTimeScopeJSON:  claimTimeScopeJSON,
		ClaimModality:       claimModality,
		SourceSet:           decodeStringArrayJSON(sourceSetJSON),
		Provenance:          decodeStringArrayJSON(provenanceJSON),
		Temperature:         temperature,
		Importance:          importance,
		Confidence:          confidence,
		Activation:          activation,
		Drift:               drift,
		Volatility:          volatility,
		PinStrength:         pinStrength,
		ArchivedReason:      archivedReason,
		RecoveryReason:      recoveryReason,
		CreatedAt:           createdAt,
		UpdatedAt:           now,
	}
	if archivedAtStr, ok := archivedAt.(string); ok && strings.TrimSpace(archivedAtStr) != "" {
		nextRecord.ArchivedAt = &archivedAtStr
	}
	revision := 1
	if previous != nil {
		revision = previous.Revision
		if revision <= 0 {
			revision = 1
		}
		if memoryGraphNodeRevisionShouldBump(*previous, nextRecord) {
			revision++
		}
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO memory_nodes(
		    memory_id, workspace_id, memory_type, compat_type, semantic_lineage_id, revision, protect, unresolved, visibility, memory_layer,
		    epistemic_status, lifecycle_state, origin_kind, origin_id, source_kind, source_id,
		    agent_id, session_id, task_id, title, body, summary,
		    claim_subject, claim_predicate, claim_object, claim_qualifiers_json, claim_time_scope_json,
		    claim_modality, source_set_json, provenance_json,
		    temperature, importance, confidence, activation, drift, volatility, pin_strength,
		    archived_at, archived_reason, recovery_reason, created_at, updated_at
		  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		  ON CONFLICT(memory_id) DO UPDATE SET
		    workspace_id = excluded.workspace_id,
		    memory_type = excluded.memory_type,
		    compat_type = excluded.compat_type,
		    semantic_lineage_id = excluded.semantic_lineage_id,
		    revision = excluded.revision,
		    protect = excluded.protect,
		    unresolved = excluded.unresolved,
		    visibility = excluded.visibility,
		    memory_layer = excluded.memory_layer,
		    epistemic_status = excluded.epistemic_status,
		    lifecycle_state = excluded.lifecycle_state,
		    origin_kind = excluded.origin_kind,
		    origin_id = excluded.origin_id,
		    source_kind = excluded.source_kind,
		    source_id = excluded.source_id,
		    agent_id = excluded.agent_id,
		    session_id = excluded.session_id,
		    task_id = excluded.task_id,
		    title = excluded.title,
		    body = excluded.body,
		    summary = excluded.summary,
		    claim_subject = excluded.claim_subject,
		    claim_predicate = excluded.claim_predicate,
		    claim_object = excluded.claim_object,
		    claim_qualifiers_json = excluded.claim_qualifiers_json,
		    claim_time_scope_json = excluded.claim_time_scope_json,
		    claim_modality = excluded.claim_modality,
		    source_set_json = excluded.source_set_json,
		    provenance_json = excluded.provenance_json,
		    temperature = excluded.temperature,
		    importance = excluded.importance,
		    confidence = excluded.confidence,
		    activation = excluded.activation,
		    drift = excluded.drift,
		    volatility = excluded.volatility,
		    pin_strength = excluded.pin_strength,
		    archived_at = excluded.archived_at,
		    archived_reason = excluded.archived_reason,
		    recovery_reason = excluded.recovery_reason,
		    updated_at = excluded.updated_at`,
		memoryID,
		workspaceID,
		memoryType,
		compatType,
		semanticLineageID,
		revision,
		boolToInt(protect),
		boolToInt(unresolved),
		visibility,
		memoryLayer,
		epistemicStatus,
		lifecycleState,
		originKind,
		originID,
		sourceKind,
		sourceID,
		agentID,
		sessionID,
		taskID,
		title,
		body,
		summary,
		claimSubject,
		claimPredicate,
		claimObject,
		claimQualifiersJSON,
		claimTimeScopeJSON,
		claimModality,
		sourceSetJSON,
		provenanceJSON,
		temperature,
		importance,
		confidence,
		activation,
		drift,
		volatility,
		pinStrength,
		archivedAt,
		archivedReason,
		recoveryReason,
		createdAt,
		now,
	); err != nil {
		return MemoryGraphNodeRecord{}, fmt.Errorf("upsert memory graph node: %w", err)
	}
	return s.loadMemoryGraphNodeTx(ctx, tx, workspaceID, memoryID)
}

func (s *Store) replaceMemoryGraphNodeRefsTx(ctx context.Context, tx *sql.Tx, memoryID, workspaceID string, refs []MemoryGraphNodeRefInput) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM memory_node_refs WHERE workspace_id = ? AND memory_id = ?`, workspaceID, memoryID); err != nil {
		return fmt.Errorf("clear memory node refs: %w", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, ref := range refs {
		refKind := strings.TrimSpace(ref.RefKind)
		refID := strings.TrimSpace(ref.RefID)
		if refKind == "" || refID == "" {
			continue
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO memory_node_refs(memory_id, workspace_id, ref_kind, ref_id, ref_role, ref_value, weight, metadata_json, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			memoryID,
			workspaceID,
			refKind,
			refID,
			strings.TrimSpace(ref.RefRole),
			strings.TrimSpace(ref.RefValue),
			clampUnitInterval(ref.Weight),
			firstNonEmpty(strings.TrimSpace(ref.MetadataJSON), "{}"),
			now,
			now,
		); err != nil {
			return fmt.Errorf("insert memory node ref: %w", err)
		}
	}
	return nil
}

func (s *Store) replaceMemoryGraphNodeVersionsTx(ctx context.Context, tx *sql.Tx, memoryID, workspaceID string, versions []MemoryGraphNodeVersionInput) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM memory_node_versions WHERE workspace_id = ? AND memory_id = ?`, workspaceID, memoryID); err != nil {
		return fmt.Errorf("clear memory node versions: %w", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, version := range versions {
		refKind := strings.TrimSpace(version.RefKind)
		refID := strings.TrimSpace(version.RefID)
		if refKind == "" || refID == "" {
			continue
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO memory_node_versions(memory_id, workspace_id, ref_kind, ref_id, version_token, weight, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			memoryID,
			workspaceID,
			refKind,
			refID,
			strings.TrimSpace(version.VersionToken),
			clampUnitInterval(version.Weight),
			now,
			now,
		); err != nil {
			return fmt.Errorf("insert memory node version: %w", err)
		}
	}
	return nil
}

func (s *Store) replaceMemoryGraphNodeMetricsTx(ctx context.Context, tx *sql.Tx, memoryID, workspaceID string, metrics []MemoryGraphNodeMetricInput) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM memory_node_metrics WHERE workspace_id = ? AND memory_id = ?`, workspaceID, memoryID); err != nil {
		return fmt.Errorf("clear memory node metrics: %w", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, metric := range metrics {
		metricKey := strings.TrimSpace(metric.MetricKey)
		if metricKey == "" {
			continue
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO memory_node_metrics(memory_id, workspace_id, metric_key, metric_value, metric_unit, metric_kind, metadata_json, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			memoryID,
			workspaceID,
			metricKey,
			metric.MetricValue,
			strings.TrimSpace(metric.MetricUnit),
			firstNonEmpty(strings.TrimSpace(metric.MetricKind), "scalar"),
			firstNonEmpty(strings.TrimSpace(metric.MetadataJSON), "{}"),
			now,
			now,
		); err != nil {
			return fmt.Errorf("insert memory node metric: %w", err)
		}
	}
	return nil
}

func (s *Store) replaceMemoryGraphEdgesForSourceTx(ctx context.Context, tx *sql.Tx, workspaceID, sourceKind, sourceID string, edges []MemoryGraphEdgeInput) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM memory_edges WHERE workspace_id = ? AND source_kind = ? AND source_id = ?`, workspaceID, sourceKind, sourceID); err != nil {
		return fmt.Errorf("clear memory graph edges: %w", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, edge := range edges {
		if strings.TrimSpace(edge.FromMemoryID) == "" || strings.TrimSpace(edge.ToMemoryID) == "" || strings.TrimSpace(edge.EdgeType) == "" {
			continue
		}
		edgeID := strings.TrimSpace(edge.EdgeID)
		if edgeID == "" {
			edgeID = memoryGraphEdgeID(edge.WorkspaceID, edge.FromMemoryID, edge.ToMemoryID, edge.EdgeType, edge.SourceKind, edge.SourceID)
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO memory_edges(edge_id, workspace_id, from_memory_id, to_memory_id, edge_type, source_kind, source_id, weight, metadata_json, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(edge_id) DO UPDATE SET
			   workspace_id = excluded.workspace_id,
			   from_memory_id = excluded.from_memory_id,
			   to_memory_id = excluded.to_memory_id,
			   edge_type = excluded.edge_type,
			   source_kind = excluded.source_kind,
			   source_id = excluded.source_id,
			   weight = excluded.weight,
			   metadata_json = excluded.metadata_json,
			   updated_at = excluded.updated_at`,
			edgeID,
			workspaceID,
			strings.TrimSpace(edge.FromMemoryID),
			strings.TrimSpace(edge.ToMemoryID),
			strings.TrimSpace(edge.EdgeType),
			firstNonEmpty(strings.TrimSpace(edge.SourceKind), sourceKind),
			firstNonEmpty(strings.TrimSpace(edge.SourceID), sourceID),
			clampUnitInterval(edge.Weight),
			firstNonEmpty(strings.TrimSpace(edge.MetadataJSON), "{}"),
			now,
			now,
		); err != nil {
			return fmt.Errorf("insert memory graph edge: %w", err)
		}
	}
	return nil
}

func (s *Store) loadMemoryGraphNode(ctx context.Context, workspaceID, memoryID string) (MemoryGraphNodeRecord, error) {
	row := s.db.QueryRowContext(
		ctx,
		`SELECT n.memory_id, n.workspace_id, n.memory_type, n.compat_type, n.semantic_lineage_id, n.revision, n.protect, n.unresolved,
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
		     ON s.workspace_id = n.workspace_id AND s.memory_id = n.memory_id
		  WHERE n.workspace_id = ? AND n.memory_id = ?`,
		workspaceID,
		memoryID,
	)
	record, err := scanMemoryGraphNodeRecord(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return MemoryGraphNodeRecord{}, fmt.Errorf("memory graph node not found: %s/%s", workspaceID, memoryID)
		}
		return MemoryGraphNodeRecord{}, err
	}
	return record, nil
}

func (s *Store) loadMemoryGraphNodeTx(ctx context.Context, tx *sql.Tx, workspaceID, memoryID string) (MemoryGraphNodeRecord, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT n.memory_id, n.workspace_id, n.memory_type, n.compat_type, n.semantic_lineage_id, n.revision, n.protect, n.unresolved,
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
		     ON s.workspace_id = n.workspace_id AND s.memory_id = n.memory_id
		  WHERE n.workspace_id = ? AND n.memory_id = ?`,
		workspaceID,
		memoryID,
	)
	return scanMemoryGraphNodeRecord(row)
}

func (s *Store) listMemoryGraphNodeRefs(ctx context.Context, workspaceID, memoryID string) ([]MemoryGraphNodeRefRecord, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT memory_id, workspace_id, ref_kind, ref_id, ref_role, ref_value, weight, metadata_json, created_at, updated_at
		   FROM memory_node_refs
		  WHERE workspace_id = ? AND memory_id = ?
		  ORDER BY ref_kind ASC, ref_role ASC, ref_id ASC`,
		workspaceID,
		memoryID,
	)
	if err != nil {
		return nil, fmt.Errorf("list memory node refs: %w", err)
	}
	defer rows.Close()
	out := make([]MemoryGraphNodeRefRecord, 0)
	for rows.Next() {
		var row MemoryGraphNodeRefRecord
		if err := rows.Scan(&row.MemoryID, &row.WorkspaceID, &row.RefKind, &row.RefID, &row.RefRole, &row.RefValue, &row.Weight, &row.MetadataJSON, &row.CreatedAt, &row.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan memory node ref: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate memory node refs: %w", err)
	}
	return out, nil
}

func (s *Store) listMemoryGraphNodeVersions(ctx context.Context, workspaceID, memoryID string) ([]MemoryGraphNodeVersionRecord, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT memory_id, workspace_id, ref_kind, ref_id, version_token, weight, created_at, updated_at
		   FROM memory_node_versions
		  WHERE workspace_id = ? AND memory_id = ?
		  ORDER BY ref_kind ASC, ref_id ASC`,
		workspaceID,
		memoryID,
	)
	if err != nil {
		return nil, fmt.Errorf("list memory node versions: %w", err)
	}
	defer rows.Close()
	out := make([]MemoryGraphNodeVersionRecord, 0)
	for rows.Next() {
		var row MemoryGraphNodeVersionRecord
		if err := rows.Scan(&row.MemoryID, &row.WorkspaceID, &row.RefKind, &row.RefID, &row.VersionToken, &row.Weight, &row.CreatedAt, &row.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan memory node version: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate memory node versions: %w", err)
	}
	return out, nil
}

func (s *Store) listMemoryGraphNodeMetrics(ctx context.Context, workspaceID, memoryID string) ([]MemoryGraphNodeMetricRecord, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT memory_id, workspace_id, metric_key, metric_value, metric_unit, metric_kind, metadata_json, created_at, updated_at
		   FROM memory_node_metrics
		  WHERE workspace_id = ? AND memory_id = ?
		  ORDER BY metric_key ASC`,
		workspaceID,
		memoryID,
	)
	if err != nil {
		return nil, fmt.Errorf("list memory node metrics: %w", err)
	}
	defer rows.Close()
	out := make([]MemoryGraphNodeMetricRecord, 0)
	for rows.Next() {
		var row MemoryGraphNodeMetricRecord
		if err := rows.Scan(&row.MemoryID, &row.WorkspaceID, &row.MetricKey, &row.MetricValue, &row.MetricUnit, &row.MetricKind, &row.MetadataJSON, &row.CreatedAt, &row.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan memory node metric: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate memory node metrics: %w", err)
	}
	return out, nil
}

func (s *Store) listMemoryGraphEdges(ctx context.Context, workspaceID, memoryID string, outbound bool) ([]MemoryGraphEdgeRecord, error) {
	column := "to_memory_id"
	if outbound {
		column = "from_memory_id"
	}
	rows, err := s.db.QueryContext(
		ctx,
		fmt.Sprintf(`SELECT edge_id, workspace_id, from_memory_id, to_memory_id, edge_type, source_kind, source_id, weight, metadata_json, created_at, updated_at
		   FROM memory_edges
		  WHERE workspace_id = ? AND %s = ?
		  ORDER BY updated_at DESC, edge_id DESC`, column),
		workspaceID,
		memoryID,
	)
	if err != nil {
		return nil, fmt.Errorf("list memory graph edges: %w", err)
	}
	defer rows.Close()
	out := make([]MemoryGraphEdgeRecord, 0)
	for rows.Next() {
		var row MemoryGraphEdgeRecord
		if err := rows.Scan(&row.EdgeID, &row.WorkspaceID, &row.FromMemoryID, &row.ToMemoryID, &row.EdgeType, &row.SourceKind, &row.SourceID, &row.Weight, &row.MetadataJSON, &row.CreatedAt, &row.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan memory graph edge: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate memory graph edges: %w", err)
	}
	return out, nil
}

func collectMemoryGraphNodeRows(rows *sql.Rows) ([]MemoryGraphNodeRecord, error) {
	out := make([]MemoryGraphNodeRecord, 0)
	for rows.Next() {
		record, err := scanMemoryGraphNodeRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate memory graph nodes: %w", err)
	}
	return out, nil
}

func scanMemoryGraphNodeRecord(scanner interface{ Scan(dest ...any) error }) (MemoryGraphNodeRecord, error) {
	var record MemoryGraphNodeRecord
	var sourceSetJSON, provenanceJSON string
	var archivedAt sql.NullString
	var lastAnyAccess sql.NullString
	var lastTrustedAccess sql.NullString
	var retentionHotUntil sql.NullString
	var retentionWarmUntil sql.NullString
	var retentionExpiresAt sql.NullString
	var tLife sql.NullFloat64
	var protectInt int
	var unresolvedInt int
	if err := scanner.Scan(
		&record.MemoryID,
		&record.WorkspaceID,
		&record.MemoryType,
		&record.CompatType,
		&record.SemanticLineageID,
		&record.Revision,
		&protectInt,
		&unresolvedInt,
		&lastAnyAccess,
		&lastTrustedAccess,
		&tLife,
		&retentionHotUntil,
		&retentionWarmUntil,
		&retentionExpiresAt,
		&record.Visibility,
		&record.MemoryLayer,
		&record.EpistemicStatus,
		&record.LifecycleState,
		&record.OriginKind,
		&record.OriginID,
		&record.SourceKind,
		&record.SourceID,
		&record.AgentID,
		&record.SessionID,
		&record.TaskID,
		&record.Title,
		&record.Body,
		&record.Summary,
		&record.ClaimSubject,
		&record.ClaimPredicate,
		&record.ClaimObject,
		&record.ClaimQualifiersJSON,
		&record.ClaimTimeScopeJSON,
		&record.ClaimModality,
		&sourceSetJSON,
		&provenanceJSON,
		&record.Temperature,
		&record.Importance,
		&record.Confidence,
		&record.Activation,
		&record.Drift,
		&record.Volatility,
		&record.PinStrength,
		&archivedAt,
		&record.ArchivedReason,
		&record.RecoveryReason,
		&record.CreatedAt,
		&record.UpdatedAt,
	); err != nil {
		return MemoryGraphNodeRecord{}, err
	}
	record.SourceSet = decodeStringArrayJSON(sourceSetJSON)
	record.Provenance = decodeStringArrayJSON(provenanceJSON)
	record.ArchivedAt = nullStringPtr(archivedAt)
	record.Protect = protectInt != 0
	record.Unresolved = unresolvedInt != 0
	record.LastAnyAccess = nullStringPtr(lastAnyAccess)
	record.LastTrustedAccess = nullStringPtr(lastTrustedAccess)
	record.RetentionHotUntil = nullStringPtr(retentionHotUntil)
	record.RetentionWarmUntil = nullStringPtr(retentionWarmUntil)
	record.RetentionExpiresAt = nullStringPtr(retentionExpiresAt)
	if tLife.Valid {
		record.TLife = tLife.Float64
	}
	applyMemoryGraphBoundary(&record)
	return record, nil
}

func applyMemoryGraphRetentionToRecords(records []MemoryGraphNodeRecord, referenceAt string) {
	for idx := range records {
		applyMemoryGraphRetentionState(&records[idx], referenceAt)
	}
}

func applyMemoryGraphRetentionState(record *MemoryGraphNodeRecord, referenceAt string) {
	if record == nil {
		return
	}
	record.RetentionBand = memoryGraphRetentionBand(referenceAt, record.RetentionHotUntil, record.RetentionWarmUntil, record.RetentionExpiresAt)
	record.RetentionGuardReason = memoryGraphRetentionGuardReason(*record)
	record.RetentionPrunable = record.RetentionBand == "PRUNABLE" && strings.TrimSpace(record.RetentionGuardReason) == ""
	record.TemporalContracts = memoryGraphRetentionTemporalContracts(*record, referenceAt)
}

func applyMemoryGraphRecoveryState(record *MemoryGraphNodeRecord, report *MemoryGraphDriftReport) {
	if record == nil {
		return
	}
	record.RecoveryCandidate = false
	record.RecoveryTriggerCount = 0
	record.RecoveryTriggerKinds = nil
	record.RecoveryGuardReason = ""

	if record.ArchivedAt == nil || strings.TrimSpace(*record.ArchivedAt) == "" {
		return
	}
	if !strings.EqualFold(strings.TrimSpace(record.OriginKind), "workspace_memory") {
		return
	}
	if strings.TrimSpace(record.ArchivedReason) != rmpArchivedReasonExpired {
		return
	}
	triggerKinds := map[string]struct{}{}
	triggerCount := 0
	if report != nil {
		for _, item := range report.Items {
			switch strings.TrimSpace(item.State) {
			case "STALE", "MISSING_SOURCE", "UNRESOLVED":
				triggerCount++
				if refKind := strings.TrimSpace(item.RefKind); refKind != "" {
					triggerKinds[refKind] = struct{}{}
				}
			}
		}
	}
	if triggerCount == 0 {
		record.RecoveryGuardReason = "NO_TRIGGERED_LINKAGE"
		return
	}
	record.RecoveryCandidate = true
	record.RecoveryTriggerCount = triggerCount
	record.RecoveryTriggerKinds = sortedMemoryGraphStringSet(triggerKinds)
}

func sortedMemoryGraphStringSet(values map[string]struct{}) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	sort.Strings(out)
	return out
}

func memoryGraphRetentionBand(referenceAt string, hotUntil, warmUntil, expiresAt *string) string {
	refTime, ok := parseMemoryGraphTimestamp(referenceAt)
	if !ok {
		return ""
	}
	if expiresTime, ok := parseMemoryGraphTimestamp(derefString(hotUntil)); ok {
		if refTime.Before(expiresTime) {
			return "HOT"
		}
	}
	if warmTime, ok := parseMemoryGraphTimestamp(derefString(warmUntil)); ok {
		if refTime.Before(warmTime) {
			return "WARM"
		}
	}
	if expiresTime, ok := parseMemoryGraphTimestamp(derefString(expiresAt)); ok {
		if refTime.Before(expiresTime) {
			return "COLD"
		}
		return "PRUNABLE"
	}
	return ""
}

func memoryGraphRetentionGuardReason(record MemoryGraphNodeRecord) string {
	if record.Protect {
		return "PROTECT"
	}
	if record.Unresolved {
		return "UNRESOLVED"
	}
	switch normalizeMemoryGraphType(record.MemoryType) {
	case "DISSENT", "DISSENT_MARKER", "DISSENT_CONTENT", "ALTERNATIVE_BRANCH":
		return normalizeMemoryGraphType(record.MemoryType)
	default:
		return ""
	}
}

func parseMemoryGraphTimestamp(raw string) (time.Time, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339Nano, trimmed)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

func buildMemoryGraphNodeWhere(filter MemoryGraphNodeFilter, alias string, args *[]any) []string {
	prefix := ""
	if trimmed := strings.TrimSpace(alias); trimmed != "" {
		prefix = trimmed + "."
	}
	where := []string{prefix + `workspace_id = ?`}
	*args = append(*args, strings.TrimSpace(filter.WorkspaceID))
	if !filter.IncludeArchived {
		where = append(where, prefix+`archived_at IS NULL`)
	}
	if trimmed := strings.TrimSpace(filter.MemoryType); trimmed != "" {
		canonicalTypes, compatTypes := memoryGraphFilterTypes(trimmed)
		typeClauses := make([]string, 0, 2)
		if len(canonicalTypes) > 0 {
			placeholders := make([]string, len(canonicalTypes))
			for idx, value := range canonicalTypes {
				placeholders[idx] = "?"
				*args = append(*args, value)
			}
			typeClauses = append(typeClauses, prefix+`memory_type IN (`+strings.Join(placeholders, ", ")+`)`)
		}
		if len(compatTypes) > 0 {
			placeholders := make([]string, len(compatTypes))
			for idx, value := range compatTypes {
				placeholders[idx] = "?"
				*args = append(*args, value)
			}
			typeClauses = append(typeClauses, prefix+`compat_type IN (`+strings.Join(placeholders, ", ")+`)`)
		}
		if len(typeClauses) == 1 {
			where = append(where, typeClauses[0])
		} else if len(typeClauses) > 1 {
			where = append(where, `(`+strings.Join(typeClauses, ` OR `)+`)`)
		}
	}
	if trimmed := strings.TrimSpace(filter.MemoryLayer); trimmed != "" {
		where = append(where, prefix+`memory_layer = ?`)
		*args = append(*args, normalizeMemoryGraphLayer(trimmed))
	}
	if trimmed := strings.TrimSpace(filter.Visibility); trimmed != "" {
		where = append(where, prefix+`visibility = ?`)
		*args = append(*args, normalizeMemoryGraphVisibility(trimmed))
	}
	if trimmed := strings.TrimSpace(filter.EpistemicStatus); trimmed != "" {
		where = append(where, prefix+`epistemic_status = ?`)
		*args = append(*args, normalizeMemoryGraphEpistemicStatus(trimmed))
	}
	if trimmed := strings.TrimSpace(filter.LifecycleState); trimmed != "" {
		where = append(where, prefix+`lifecycle_state = ?`)
		*args = append(*args, normalizeMemoryGraphLifecycleState(trimmed))
	}
	if trimmed := strings.TrimSpace(filter.OriginKind); trimmed != "" {
		where = append(where, prefix+`origin_kind = ?`)
		*args = append(*args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.OriginID); trimmed != "" {
		where = append(where, prefix+`origin_id = ?`)
		*args = append(*args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.SourceKind); trimmed != "" {
		where = append(where, prefix+`source_kind = ?`)
		*args = append(*args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.AgentID); trimmed != "" {
		where = append(where, prefix+`agent_id = ?`)
		*args = append(*args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.SessionID); trimmed != "" {
		where = append(where, prefix+`session_id = ?`)
		*args = append(*args, trimmed)
	}
	if trimmed := strings.TrimSpace(filter.TaskID); trimmed != "" {
		where = append(where, prefix+`task_id = ?`)
		*args = append(*args, trimmed)
	}
	return where
}

func memoryGraphSemanticLineageID(originKind, originID, memoryID string) string {
	originKind = strings.TrimSpace(originKind)
	originID = strings.TrimSpace(originID)
	if originKind != "" && originID != "" {
		return originKind + ":" + originID
	}
	return strings.TrimSpace(memoryID)
}

func memoryGraphAnchorProtect(memoryLayer string, pinStrength float64, explicit bool) bool {
	if explicit {
		return true
	}
	switch normalizeMemoryGraphLayer(memoryLayer) {
	case "PROCEDURAL", "IDENTITY":
		return true
	default:
		return clampUnitInterval(pinStrength) >= 0.8
	}
}

func memoryGraphAnchorUnresolved(epistemicStatus string, explicit bool) bool {
	if explicit {
		return true
	}
	switch normalizeMemoryGraphEpistemicStatus(epistemicStatus) {
	case "ALLEGED", "DISPUTED":
		return true
	default:
		return false
	}
}

func memoryGraphNodeRevisionShouldBump(previous, next MemoryGraphNodeRecord) bool {
	if previous.MemoryType != next.MemoryType ||
		previous.CompatType != next.CompatType ||
		previous.SemanticLineageID != next.SemanticLineageID ||
		previous.Protect != next.Protect ||
		previous.Unresolved != next.Unresolved ||
		previous.Visibility != next.Visibility ||
		previous.MemoryLayer != next.MemoryLayer ||
		previous.EpistemicStatus != next.EpistemicStatus ||
		previous.LifecycleState != next.LifecycleState ||
		previous.OriginKind != next.OriginKind ||
		previous.OriginID != next.OriginID ||
		previous.SourceKind != next.SourceKind ||
		previous.SourceID != next.SourceID ||
		previous.AgentID != next.AgentID ||
		previous.SessionID != next.SessionID ||
		previous.TaskID != next.TaskID ||
		previous.Title != next.Title ||
		previous.Body != next.Body ||
		previous.Summary != next.Summary ||
		previous.ClaimSubject != next.ClaimSubject ||
		previous.ClaimPredicate != next.ClaimPredicate ||
		previous.ClaimObject != next.ClaimObject ||
		previous.ClaimQualifiersJSON != next.ClaimQualifiersJSON ||
		previous.ClaimTimeScopeJSON != next.ClaimTimeScopeJSON ||
		previous.ClaimModality != next.ClaimModality ||
		previous.Temperature != next.Temperature ||
		previous.Importance != next.Importance ||
		previous.Confidence != next.Confidence ||
		previous.Activation != next.Activation ||
		previous.Drift != next.Drift ||
		previous.Volatility != next.Volatility ||
		previous.PinStrength != next.PinStrength ||
		derefString(previous.ArchivedAt) != derefString(next.ArchivedAt) ||
		previous.ArchivedReason != next.ArchivedReason ||
		previous.RecoveryReason != next.RecoveryReason {
		return true
	}
	return !memoryGraphStringSlicesEqual(previous.SourceSet, next.SourceSet) ||
		!memoryGraphStringSlicesEqual(previous.Provenance, next.Provenance)
}

func memoryGraphStringSlicesEqual(left, right []string) bool {
	left = uniqueTrimmedStrings(left)
	right = uniqueTrimmedStrings(right)
	if len(left) != len(right) {
		return false
	}
	for idx := range left {
		if left[idx] != right[idx] {
			return false
		}
	}
	return true
}

func memoryGraphNodeFromWorkspaceMemory(record WorkspaceMemoryRecord) (MemoryGraphNodeInput, []MemoryGraphNodeRefInput, []MemoryGraphNodeVersionInput) {
	now := firstNonEmpty(strings.TrimSpace(record.UpdatedAt), strings.TrimSpace(record.CreatedAt), time.Now().UTC().Format(time.RFC3339Nano))
	memoryType := canonicalMemoryTypeFromWorkspaceMemory(record)
	nodeID := memoryGraphNodeID("workspace_memory", record.MemoryID)
	memoryLayer := memoryGraphLayerForType(memoryType)
	if record.ArchivedAt != nil && strings.TrimSpace(*record.ArchivedAt) != "" {
		memoryLayer = "ARCHIVE"
	}
	node := MemoryGraphNodeInput{
		MemoryID:            nodeID,
		WorkspaceID:         strings.TrimSpace(record.WorkspaceID),
		MemoryType:          memoryType,
		CompatType:          strings.TrimSpace(record.MemoryType),
		Visibility:          "WORKSPACE",
		MemoryLayer:         memoryLayer,
		EpistemicStatus:     memoryGraphEpistemicForWorkspaceMemory(record, memoryType),
		LifecycleState:      memoryGraphLifecycleForWorkspaceMemory(record),
		OriginKind:          "workspace_memory",
		OriginID:            strings.TrimSpace(record.MemoryID),
		SourceKind:          strings.TrimSpace(record.SourceKind),
		SourceID:            strings.TrimSpace(record.SourceID),
		AgentID:             strings.TrimSpace(record.AgentID),
		SessionID:           strings.TrimSpace(record.SessionID),
		TaskID:              strings.TrimSpace(record.TaskID),
		Title:               strings.TrimSpace(record.Title),
		Body:                strings.TrimSpace(record.Body),
		Summary:             strings.TrimSpace(record.Summary),
		ClaimSubject:        strings.TrimSpace(record.Title),
		ClaimPredicate:      memoryGraphPredicateForType(memoryType),
		ClaimObject:         firstNonEmpty(strings.TrimSpace(record.Summary), strings.TrimSpace(record.Body)),
		ClaimQualifiersJSON: "{}",
		ClaimTimeScopeJSON:  "{}",
		ClaimModality:       memoryGraphModalityForType(memoryType),
		SourceSetJSON:       encodeStringArrayJSON(memoryGraphSourceSet(record.SourceKind, record.SourceID)),
		ProvenanceJSON:      encodeStringArrayJSON(memoryGraphWorkspaceMemoryProvenance(record)),
		Temperature:         record.Importance,
		Importance:          record.Importance,
		Confidence:          record.Confidence,
		Activation:          record.Importance,
		Drift:               0,
		Volatility:          0,
		PinStrength:         0,
		ArchivedAt:          record.ArchivedAt,
		ArchivedReason:      record.ArchivedReason,
		RecoveryReason:      record.RecoveryReason,
		CreatedAt:           firstNonEmpty(strings.TrimSpace(record.CreatedAt), now),
		UpdatedAt:           now,
	}
	refs := []MemoryGraphNodeRefInput{
		{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "origin", RefID: record.MemoryID, RefRole: "workspace_memory", RefValue: record.MemoryType, Weight: 1, MetadataJSON: "{}"},
	}
	if trimmed := strings.TrimSpace(record.AgentID); trimmed != "" {
		refs = append(refs, MemoryGraphNodeRefInput{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "agent", RefID: trimmed, RefRole: "owner", Weight: 1, MetadataJSON: "{}"})
	}
	if trimmed := strings.TrimSpace(record.SessionID); trimmed != "" {
		refs = append(refs, MemoryGraphNodeRefInput{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "session", RefID: trimmed, RefRole: "source", Weight: 1, MetadataJSON: "{}"})
	}
	if trimmed := strings.TrimSpace(record.TaskID); trimmed != "" {
		refs = append(refs, MemoryGraphNodeRefInput{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "task", RefID: trimmed, RefRole: "scope", Weight: 1, MetadataJSON: "{}"})
	}
	if trimmed := strings.TrimSpace(record.SourceID); trimmed != "" {
		refs = append(refs, MemoryGraphNodeRefInput{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "source", RefID: trimmed, RefRole: strings.TrimSpace(record.SourceKind), Weight: 1, MetadataJSON: "{}"})
	}
	versions := []MemoryGraphNodeVersionInput{
		{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "workspace_memory", RefID: record.MemoryID, VersionToken: now, Weight: 1},
	}
	return node, refs, versions
}

func memoryGraphNodeFromKnowledgeClaim(record KnowledgeClaimRecord, relations []KnowledgeClaimRelationRecord) (MemoryGraphNodeInput, []MemoryGraphNodeRefInput, []MemoryGraphNodeVersionInput, []MemoryGraphEdgeInput) {
	now := firstNonEmpty(strings.TrimSpace(record.UpdatedAt), strings.TrimSpace(record.CreatedAt), time.Now().UTC().Format(time.RFC3339Nano))
	memoryType := canonicalMemoryTypeFromKnowledgeClaim(record)
	nodeID := memoryGraphNodeID("knowledge_claim", record.ClaimID)
	memoryLayer := memoryGraphLayerForType(memoryType)
	if record.ArchivedAt != nil && strings.TrimSpace(*record.ArchivedAt) != "" {
		memoryLayer = "ARCHIVE"
	}
	node := MemoryGraphNodeInput{
		MemoryID:            nodeID,
		WorkspaceID:         strings.TrimSpace(record.WorkspaceID),
		MemoryType:          memoryType,
		CompatType:          strings.TrimSpace(record.ClaimType),
		Visibility:          "WORKSPACE",
		MemoryLayer:         memoryLayer,
		EpistemicStatus:     memoryGraphEpistemicForKnowledgeClaim(record),
		LifecycleState:      memoryGraphLifecycleForKnowledgeClaim(record),
		OriginKind:          "knowledge_claim",
		OriginID:            strings.TrimSpace(record.ClaimID),
		SourceKind:          strings.TrimSpace(record.SourceKind),
		SourceID:            strings.TrimSpace(record.SourceID),
		AgentID:             strings.TrimSpace(record.AgentID),
		SessionID:           strings.TrimSpace(record.SessionID),
		TaskID:              strings.TrimSpace(record.TaskID),
		Title:               firstNonEmpty(strings.TrimSpace(record.Subject), strings.TrimSpace(record.Summary)),
		Body:                strings.TrimSpace(record.Body),
		Summary:             strings.TrimSpace(record.Summary),
		ClaimSubject:        strings.TrimSpace(record.Subject),
		ClaimPredicate:      memoryGraphPredicateForType(memoryType),
		ClaimObject:         firstNonEmpty(strings.TrimSpace(record.Summary), strings.TrimSpace(record.Body)),
		ClaimQualifiersJSON: encodeStringMapJSON(map[string]any{"claim_type": strings.TrimSpace(record.ClaimType)}),
		ClaimTimeScopeJSON:  "{}",
		ClaimModality:       memoryGraphClaimModality(record, memoryType),
		SourceSetJSON:       encodeStringArrayJSON(memoryGraphKnowledgeClaimSourceSet(record)),
		ProvenanceJSON:      encodeStringArrayJSON(memoryGraphKnowledgeClaimProvenance(record, relations)),
		Temperature:         record.Confidence,
		Importance:          clampUnitInterval(record.Confidence),
		Confidence:          record.Confidence,
		Activation:          clampUnitInterval(record.Confidence),
		Drift:               0,
		Volatility:          0,
		PinStrength:         0,
		ArchivedAt:          record.ArchivedAt,
		ArchivedReason:      record.LifecycleReason,
		RecoveryReason:      record.RecoveryReason,
		CreatedAt:           firstNonEmpty(strings.TrimSpace(record.CreatedAt), now),
		UpdatedAt:           now,
	}
	refs := []MemoryGraphNodeRefInput{
		{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "origin", RefID: record.ClaimID, RefRole: "knowledge_claim", RefValue: record.ClaimType, Weight: 1, MetadataJSON: "{}"},
	}
	if trimmed := strings.TrimSpace(record.AgentID); trimmed != "" {
		refs = append(refs, MemoryGraphNodeRefInput{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "agent", RefID: trimmed, RefRole: "author", Weight: 1, MetadataJSON: "{}"})
	}
	if trimmed := strings.TrimSpace(record.SessionID); trimmed != "" {
		refs = append(refs, MemoryGraphNodeRefInput{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "session", RefID: trimmed, RefRole: "source", Weight: 1, MetadataJSON: "{}"})
	}
	if trimmed := strings.TrimSpace(record.TaskID); trimmed != "" {
		refs = append(refs, MemoryGraphNodeRefInput{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "task", RefID: trimmed, RefRole: "scope", Weight: 1, MetadataJSON: "{}"})
	}
	if trimmed := strings.TrimSpace(record.MemoryID); trimmed != "" {
		refs = append(refs, MemoryGraphNodeRefInput{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "workspace_memory", RefID: trimmed, RefRole: "backing_memory", Weight: 1, MetadataJSON: "{}"})
	}
	for _, relation := range relations {
		refs = append(refs, MemoryGraphNodeRefInput{
			MemoryID:     nodeID,
			WorkspaceID:  record.WorkspaceID,
			RefKind:      "knowledge_claim_relation",
			RefID:        relation.RelationID,
			RefRole:      strings.ToLower(relation.RelationType),
			RefValue:     relation.ToClaimID,
			Weight:       relation.Weight,
			MetadataJSON: encodeStringMapJSON(map[string]any{"source_kind": relation.SourceKind, "source_id": relation.SourceID}),
		})
	}
	versions := []MemoryGraphNodeVersionInput{
		{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "knowledge_claim", RefID: record.ClaimID, VersionToken: now, Weight: 1},
	}
	if trimmed := strings.TrimSpace(record.MemoryID); trimmed != "" {
		versions = append(versions, MemoryGraphNodeVersionInput{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "workspace_memory", RefID: trimmed, VersionToken: now, Weight: 1})
	}
	for _, relation := range relations {
		versions = append(versions, MemoryGraphNodeVersionInput{
			MemoryID:     nodeID,
			WorkspaceID:  record.WorkspaceID,
			RefKind:      "knowledge_claim_relation",
			RefID:        relation.RelationID,
			VersionToken: firstNonEmpty(strings.TrimSpace(relation.UpdatedAt), now),
			Weight:       relation.Weight,
		})
	}
	edges := make([]MemoryGraphEdgeInput, 0, len(relations)+2)
	if trimmed := strings.TrimSpace(record.MemoryID); trimmed != "" {
		edges = append(edges, MemoryGraphEdgeInput{WorkspaceID: record.WorkspaceID, FromMemoryID: nodeID, ToMemoryID: memoryGraphNodeID("workspace_memory", trimmed), EdgeType: "DERIVED_FROM", SourceKind: "knowledge_claim", SourceID: record.ClaimID, Weight: 1, MetadataJSON: "{}"})
	}
	for _, relation := range relations {
		edgeType := normalizeKnowledgeClaimRelationType(relation.RelationType)
		if edgeType == "" {
			continue
		}
		edges = append(edges, MemoryGraphEdgeInput{
			WorkspaceID:  record.WorkspaceID,
			FromMemoryID: nodeID,
			ToMemoryID:   memoryGraphNodeID("knowledge_claim", relation.ToClaimID),
			EdgeType:     edgeType,
			SourceKind:   "knowledge_claim",
			SourceID:     record.ClaimID,
			Weight:       relation.Weight,
			MetadataJSON: encodeStringMapJSON(map[string]any{
				"relation_id": relation.RelationID,
				"source_kind": relation.SourceKind,
				"source_id":   relation.SourceID,
			}),
		})
	}
	if trimmed := strings.TrimSpace(record.SupersededByClaimID); trimmed != "" {
		edges = append(edges, MemoryGraphEdgeInput{WorkspaceID: record.WorkspaceID, FromMemoryID: nodeID, ToMemoryID: memoryGraphNodeID("knowledge_claim", trimmed), EdgeType: "SUPERSEDED_BY", SourceKind: "knowledge_claim", SourceID: record.ClaimID, Weight: 1, MetadataJSON: "{}"})
	}
	return node, refs, versions, edges
}

func memoryGraphNodeFromEpisodePack(record EpisodePackRecord) (MemoryGraphNodeInput, []MemoryGraphNodeRefInput, []MemoryGraphNodeVersionInput, []MemoryGraphNodeMetricInput, []MemoryGraphEdgeInput) {
	now := firstNonEmpty(strings.TrimSpace(record.UpdatedAt), strings.TrimSpace(record.CreatedAt), time.Now().UTC().Format(time.RFC3339Nano))
	nodeID := memoryGraphNodeID("episode_pack", record.PackID)
	sourceKind, sourceID := memoryGraphEpisodePackSource(record)
	node := MemoryGraphNodeInput{
		MemoryID:            nodeID,
		WorkspaceID:         strings.TrimSpace(record.WorkspaceID),
		MemoryType:          "EPISODE_PACK",
		CompatType:          strings.TrimSpace(record.PackType),
		Visibility:          "WORKSPACE",
		MemoryLayer:         "EPISODIC",
		EpistemicStatus:     memoryGraphEpisodePackEpistemic(record),
		LifecycleState:      "ACTIVE",
		OriginKind:          "episode_pack",
		OriginID:            strings.TrimSpace(record.PackID),
		SourceKind:          sourceKind,
		SourceID:            sourceID,
		AgentID:             strings.TrimSpace(record.AgentID),
		SessionID:           strings.TrimSpace(record.SessionID),
		TaskID:              strings.TrimSpace(record.TaskID),
		Title:               firstNonEmpty(strings.TrimSpace(record.NarrativeSummary), "Episode pack "+strings.TrimSpace(record.PackID)),
		Body:                firstNonEmpty(strings.TrimSpace(record.SummaryText), strings.TrimSpace(record.NarrativeSummary)),
		Summary:             strings.TrimSpace(record.NarrativeSummary),
		ClaimSubject:        strings.TrimSpace(record.SessionID),
		ClaimPredicate:      "summarizes",
		ClaimObject:         strings.TrimSpace(record.NarrativeSummary),
		ClaimQualifiersJSON: encodeStringMapJSON(map[string]any{"pack_type": record.PackType, "pack_mode": record.PackMode, "trigger_kind": record.TriggerKind}),
		ClaimTimeScopeJSON:  encodeStringMapJSON(map[string]any{"window_start": record.SourceWindowStart, "window_end": record.SourceWindowEnd}),
		ClaimModality:       "observed",
		SourceSetJSON:       encodeStringArrayJSON(memoryGraphEpisodePackSourceSet(record)),
		ProvenanceJSON:      encodeStringArrayJSON(memoryGraphEpisodePackProvenance(record)),
		Temperature:         episodePackCompressionRatio(record.MessageTokensBefore, record.MessageTokensAfter),
		Importance:          clampUnitInterval(0.45 + episodePackCompressionRatio(record.MessageTokensBefore, record.MessageTokensAfter)),
		Confidence:          episodePackGraphConfidence(record.PackMode),
		Activation:          clampUnitInterval(0.35 + episodePackCompressionRatio(record.MessageCountBefore, record.MessageCountAfter)),
		Drift:               0,
		Volatility:          0.15,
		PinStrength:         0,
		CreatedAt:           firstNonEmpty(strings.TrimSpace(record.CreatedAt), now),
		UpdatedAt:           now,
	}
	refs := []MemoryGraphNodeRefInput{
		{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "origin", RefID: record.PackID, RefRole: "episode_pack", RefValue: record.PackType, Weight: 1, MetadataJSON: "{}"},
		{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "session", RefID: record.SessionID, RefRole: "source", Weight: 1, MetadataJSON: "{}"},
		{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "message_window", RefID: record.SessionID, RefRole: "source_span", Weight: 1, MetadataJSON: encodeStringMapJSON(map[string]any{
			"window_start":       record.SourceWindowStart,
			"window_end":         record.SourceWindowEnd,
			"source_window_hash": record.SourceWindowDigest,
			"pack_mode":          record.PackMode,
		})},
	}
	if trimmed := strings.TrimSpace(record.AgentID); trimmed != "" {
		refs = append(refs, MemoryGraphNodeRefInput{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "agent", RefID: trimmed, RefRole: "author", Weight: 1, MetadataJSON: "{}"})
	}
	if trimmed := strings.TrimSpace(record.TaskID); trimmed != "" {
		refs = append(refs, MemoryGraphNodeRefInput{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "task", RefID: trimmed, RefRole: "scope", Weight: 1, MetadataJSON: "{}"})
	}
	if trimmed := strings.TrimSpace(record.CompactionSnapshotID); trimmed != "" {
		refs = append(refs, MemoryGraphNodeRefInput{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "compaction_snapshot", RefID: trimmed, RefRole: "source", Weight: 1, MetadataJSON: "{}"})
	}
	if trimmed := strings.TrimSpace(record.LifecycleEventID); trimmed != "" {
		refs = append(refs, MemoryGraphNodeRefInput{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "runtime_event", RefID: trimmed, RefRole: "source", Weight: 1, MetadataJSON: "{}"})
	}
	if trimmed := strings.TrimSpace(record.LineageSessionID); trimmed != "" && trimmed != strings.TrimSpace(record.SessionID) {
		refs = append(refs, MemoryGraphNodeRefInput{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "session", RefID: trimmed, RefRole: "lineage", Weight: 1, MetadataJSON: "{}"})
	}
	if trimmed := strings.TrimSpace(record.SummaryWorkspaceMemory); trimmed != "" {
		refs = append(refs, MemoryGraphNodeRefInput{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "workspace_memory", RefID: trimmed, RefRole: "compat_summary", Weight: 1, MetadataJSON: "{}"})
	}
	refs = append(refs, memoryGraphEpisodePackDerivedRefs(nodeID, record)...)
	versions := []MemoryGraphNodeVersionInput{
		{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "episode_pack", RefID: record.PackID, VersionToken: firstNonEmpty(strings.TrimSpace(record.SummaryDigest), strings.TrimSpace(record.CreatedAt), now), Weight: 1},
	}
	if trimmed := strings.TrimSpace(record.CompactionSnapshotID); trimmed != "" {
		versions = append(versions, MemoryGraphNodeVersionInput{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "session_compaction_snapshot", RefID: trimmed, VersionToken: record.CreatedAt, Weight: 1})
	}
	if trimmed := strings.TrimSpace(record.LifecycleEventID); trimmed != "" {
		versions = append(versions, MemoryGraphNodeVersionInput{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "runtime_event", RefID: trimmed, VersionToken: record.CreatedAt, Weight: 1})
	}
	metrics := []MemoryGraphNodeMetricInput{
		{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, MetricKey: "message_count_before", MetricValue: float64(record.MessageCountBefore), MetricUnit: "messages", MetricKind: "count"},
		{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, MetricKey: "message_count_after", MetricValue: float64(record.MessageCountAfter), MetricUnit: "messages", MetricKind: "count"},
		{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, MetricKey: "message_tokens_before", MetricValue: float64(record.MessageTokensBefore), MetricUnit: "tokens", MetricKind: "count"},
		{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, MetricKey: "message_tokens_after", MetricValue: float64(record.MessageTokensAfter), MetricUnit: "tokens", MetricKind: "count"},
		{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, MetricKey: "total_input_tokens", MetricValue: float64(record.TotalInputTokens), MetricUnit: "tokens", MetricKind: "count"},
		{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, MetricKey: "total_output_tokens", MetricValue: float64(record.TotalOutputTokens), MetricUnit: "tokens", MetricKind: "count"},
		{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, MetricKey: "message_compression_ratio", MetricValue: episodePackCompressionRatio(record.MessageCountBefore, record.MessageCountAfter), MetricUnit: "ratio", MetricKind: "ratio"},
		{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, MetricKey: "token_compression_ratio", MetricValue: episodePackCompressionRatio(record.MessageTokensBefore, record.MessageTokensAfter), MetricUnit: "ratio", MetricKind: "ratio"},
	}
	edges := make([]MemoryGraphEdgeInput, 0, 1)
	if trimmed := strings.TrimSpace(record.SummaryWorkspaceMemory); trimmed != "" {
		edges = append(edges, MemoryGraphEdgeInput{
			WorkspaceID:  record.WorkspaceID,
			FromMemoryID: nodeID,
			ToMemoryID:   memoryGraphNodeID("workspace_memory", trimmed),
			EdgeType:     "TRANSFERRED_TO",
			SourceKind:   "episode_pack",
			SourceID:     record.PackID,
			Weight:       1,
			MetadataJSON: encodeStringMapJSON(map[string]any{"role": "compat_summary"}),
		})
	}
	return node, refs, versions, metrics, edges
}

func memoryGraphNodeID(originKind, originID string) string {
	return "memnode:" + strings.TrimSpace(originKind) + ":" + strings.TrimSpace(originID)
}

func memoryGraphEdgeID(workspaceID, fromMemoryID, toMemoryID, edgeType, sourceKind, sourceID string) string {
	return strings.Join([]string{"medge", strings.TrimSpace(workspaceID), strings.TrimSpace(edgeType), strings.TrimSpace(sourceKind), strings.TrimSpace(sourceID), strings.TrimSpace(fromMemoryID), strings.TrimSpace(toMemoryID)}, ":")
}

func canonicalMemoryTypeFromWorkspaceMemory(record WorkspaceMemoryRecord) string {
	memoryType := strings.ToUpper(strings.TrimSpace(record.MemoryType))
	titleBody := strings.ToLower(strings.TrimSpace(record.Title + " " + record.Body + " " + record.Summary + " " + strings.Join(record.Tags, " ")))
	switch memoryType {
	case "DECISION", "DECISION_RECORD":
		return "DECISION_RECORD"
	case "BLOCKER_SYMPTOM":
		return "BLOCKER_SYMPTOM"
	case "BLOCKER_HYPOTHESIS":
		return "BLOCKER_HYPOTHESIS"
	case "BLOCKER", "INCIDENT":
		return "BLOCKER_SYMPTOM"
	case "ALTERNATIVE_BRANCH":
		return "ALTERNATIVE_BRANCH"
	case "DISSENT_MARKER":
		return "DISSENT_MARKER"
	case "DISSENT_CONTENT":
		return "DISSENT_CONTENT"
	case "PROCEDURE":
		return "PROCEDURE"
	case "ANTI_PROCEDURE":
		return "ANTI_PROCEDURE"
	case "SELF_MODEL":
		return "SELF_MODEL"
	case "GOAL_COMMITMENT":
		return "GOAL_COMMITMENT"
	case "POLICY_TRACE":
		return "POLICY_TRACE"
	case "LESSON", "ENTITY":
		return "FACT"
	case "UPDATE_DIGEST", "SUMMARY", "EXPERIENCE", "NOTE":
		if strings.Contains(titleBody, "handoff") {
			return "HANDOFF"
		}
		if strings.Contains(titleBody, "blocked") {
			return "BLOCKER_SYMPTOM"
		}
		return "EPISODE_PACK"
	default:
		return "EPISODE_PACK"
	}
}

func canonicalMemoryTypeFromKnowledgeClaim(record KnowledgeClaimRecord) string {
	switch strings.ToUpper(strings.TrimSpace(record.ClaimType)) {
	case "DECISION", "DECISION_RECORD":
		return "DECISION_RECORD"
	case "BLOCKER_SYMPTOM":
		return "BLOCKER_SYMPTOM"
	case "BLOCKER_HYPOTHESIS":
		return "BLOCKER_HYPOTHESIS"
	case "ALTERNATIVE_BRANCH":
		return "ALTERNATIVE_BRANCH"
	case "PROCEDURE":
		return "PROCEDURE"
	case "ANTI_PROCEDURE":
		return "ANTI_PROCEDURE"
	case "SELF_MODEL":
		return "SELF_MODEL"
	case "GOAL_COMMITMENT":
		return "GOAL_COMMITMENT"
	case "LESSON", "ENTITY", "FACT":
		return "FACT"
	case "INCIDENT", "BLOCKER":
		return "BLOCKER_SYMPTOM"
	case "CONSTRAINT":
		return "CONSTRAINT"
	case "DISSENT":
		return "DISSENT"
	case "DISSENT_MARKER":
		return "DISSENT_MARKER"
	case "DISSENT_CONTENT":
		return "DISSENT_CONTENT"
	case "HYPOTHESIS":
		return "HYPOTHESIS"
	case "UPDATE_DIGEST", "SUMMARY", "EXPERIENCE":
		return "EPISODE_PACK"
	default:
		return "FACT"
	}
}

func memoryGraphLayerForType(memoryType string) string {
	switch normalizeMemoryGraphType(memoryType) {
	case "EPISODE_PACK", "RAW_EVENT", "RAW_MESSAGE":
		return "EPISODIC"
	case "PROCEDURE", "ANTI_PROCEDURE", "COALITION_MOTIF":
		return "PROCEDURAL"
	case "SELF_MODEL", "GOAL_COMMITMENT", "POLICY_TRACE":
		return "IDENTITY"
	default:
		return "SEMANTIC"
	}
}

func memoryGraphEpistemicForWorkspaceMemory(_ WorkspaceMemoryRecord, memoryType string) string {
	switch normalizeMemoryGraphType(memoryType) {
	case "EPISODE_PACK", "HANDOFF", "BRIDGE_NOTE":
		return "ALLEGED"
	default:
		return "SUPPORTED"
	}
}

func memoryGraphEpistemicForKnowledgeClaim(record KnowledgeClaimRecord) string {
	switch normalizeKnowledgeClaimStatus(record.Status) {
	case "CONFIRMED":
		return "VERIFIED"
	case "DISPUTED":
		return "DISPUTED"
	default:
		return "SUPPORTED"
	}
}

func memoryGraphLifecycleForWorkspaceMemory(record WorkspaceMemoryRecord) string {
	if record.ArchivedAt != nil && strings.TrimSpace(*record.ArchivedAt) != "" {
		return "ARCHIVED"
	}
	return "ACTIVE"
}

func memoryGraphLifecycleForKnowledgeClaim(record KnowledgeClaimRecord) string {
	if record.ArchivedAt != nil && strings.TrimSpace(*record.ArchivedAt) != "" {
		return "ARCHIVED"
	}
	switch normalizeKnowledgeClaimStatus(record.Status) {
	case "SUPERSEDED":
		return "SUPERSEDED"
	case "STALE":
		return "DORMANT"
	default:
		return "ACTIVE"
	}
}

func memoryGraphPredicateForType(memoryType string) string {
	switch normalizeMemoryGraphType(memoryType) {
	case "DECISION_RECORD":
		return "decides"
	case "BLOCKER_SYMPTOM":
		return "blocks"
	case "BLOCKER_HYPOTHESIS":
		return "explains_blocker"
	case "ALTERNATIVE_BRANCH":
		return "branches"
	case "DISSENT_MARKER":
		return "signals_dissent"
	case "DISSENT_CONTENT":
		return "critiques"
	case "PROCEDURE":
		return "prescribes"
	case "ANTI_PROCEDURE":
		return "proscribes"
	case "SELF_MODEL":
		return "models"
	case "GOAL_COMMITMENT":
		return "commits_to"
	case "EPISODE_PACK":
		return "summarizes"
	case "HANDOFF":
		return "transfers_to"
	default:
		return "states"
	}
}

func memoryGraphModalityForType(memoryType string) string {
	return memoryGraphCanonicalModalityForType(memoryType)
}

func memoryGraphClaimModality(_ KnowledgeClaimRecord, memoryType string) string {
	return memoryGraphCanonicalModalityForType(memoryType)
}

func memoryGraphCanonicalModalityForType(memoryType string) string {
	switch normalizeMemoryGraphType(memoryType) {
	case "DECISION_RECORD":
		return "decided"
	case "ALTERNATIVE_BRANCH", "DISSENT_CONTENT", "HYPOTHESIS", "BLOCKER_HYPOTHESIS":
		return "proposed"
	case "SELF_MODEL", "PROCEDURE", "ANTI_PROCEDURE", "FACT":
		return "inferred"
	case "GOAL_COMMITMENT", "CONSTRAINT":
		return "constrained"
	case "BLOCKER_SYMPTOM", "DISSENT", "DISSENT_MARKER", "HANDOFF", "EPISODE_PACK":
		return "observed"
	default:
		return "inferred"
	}
}

func memoryGraphWorkspaceMemoryProvenance(record WorkspaceMemoryRecord) []string {
	values := []string{"workspace_memory:" + strings.TrimSpace(record.MemoryID)}
	if trimmed := strings.TrimSpace(record.SourceKind); trimmed != "" && strings.TrimSpace(record.SourceID) != "" {
		values = append(values, trimmed+":"+strings.TrimSpace(record.SourceID))
	}
	if trimmed := strings.TrimSpace(record.SessionID); trimmed != "" {
		values = append(values, "session:"+trimmed)
	}
	if trimmed := strings.TrimSpace(record.TaskID); trimmed != "" {
		values = append(values, "task:"+trimmed)
	}
	return uniqueTrimmedStrings(values)
}

func memoryGraphKnowledgeClaimProvenance(record KnowledgeClaimRecord, relations []KnowledgeClaimRelationRecord) []string {
	values := []string{"knowledge_claim:" + strings.TrimSpace(record.ClaimID)}
	if trimmed := strings.TrimSpace(record.MemoryID); trimmed != "" {
		values = append(values, "workspace_memory:"+trimmed)
	}
	if trimmed := strings.TrimSpace(record.SessionID); trimmed != "" {
		values = append(values, "session:"+trimmed)
	}
	if trimmed := strings.TrimSpace(record.TaskID); trimmed != "" {
		values = append(values, "task:"+trimmed)
	}
	for _, relation := range relations {
		if trimmed := strings.TrimSpace(relation.RelationID); trimmed != "" {
			values = append(values, "knowledge_claim_relation:"+trimmed)
		}
	}
	return uniqueTrimmedStrings(values)
}

func memoryGraphSourceSet(sourceKind, sourceID string) []string {
	sourceKind = strings.TrimSpace(sourceKind)
	sourceID = strings.TrimSpace(sourceID)
	if sourceKind == "" {
		return nil
	}
	if sourceID == "" {
		return []string{sourceKind}
	}
	return []string{sourceKind + ":" + sourceID}
}

func memoryGraphKnowledgeClaimSourceSet(record KnowledgeClaimRecord) []string {
	values := memoryGraphSourceSet(record.SourceKind, record.SourceID)
	if trimmed := strings.TrimSpace(record.MemoryID); trimmed != "" {
		values = append(values, "workspace_memory:"+trimmed)
	}
	return uniqueTrimmedStrings(values)
}

func memoryGraphEpisodePackSourceSet(record EpisodePackRecord) []string {
	values := []string{"episode_pack:" + strings.TrimSpace(record.PackID)}
	if trimmed := strings.TrimSpace(record.CompactionSnapshotID); trimmed != "" {
		values = append(values, "session_compaction_snapshot:"+trimmed)
	}
	if trimmed := strings.TrimSpace(record.LifecycleEventID); trimmed != "" {
		values = append(values, "runtime_event:"+trimmed)
	}
	if trimmed := strings.TrimSpace(record.SummaryWorkspaceMemory); trimmed != "" {
		values = append(values, "workspace_memory:"+trimmed)
	}
	return uniqueTrimmedStrings(values)
}

func memoryGraphEpisodePackProvenance(record EpisodePackRecord) []string {
	values := append([]string(nil), record.ProvenanceRefs...)
	values = append(values, "episode_pack:"+strings.TrimSpace(record.PackID))
	return uniqueTrimmedStrings(values)
}

func memoryGraphEpisodePackEpistemic(record EpisodePackRecord) string {
	if normalizeEpisodePackMode(record.PackMode, record.SummaryText) == episodePackModeFallback {
		return "ALLEGED"
	}
	return "SUPPORTED"
}

func memoryGraphEpisodePackSource(record EpisodePackRecord) (string, string) {
	if trimmed := strings.TrimSpace(record.CompactionSnapshotID); trimmed != "" {
		return "session_compaction_snapshot", trimmed
	}
	if trimmed := strings.TrimSpace(record.LifecycleEventID); trimmed != "" {
		return "runtime_event", trimmed
	}
	return "episode_pack", strings.TrimSpace(record.PackID)
}

func memoryGraphEpisodePackDerivedRefs(nodeID string, record EpisodePackRecord) []MemoryGraphNodeRefInput {
	refs := make([]MemoryGraphNodeRefInput, 0)
	for _, value := range record.ProvenanceRefs {
		prefix, rest, ok := strings.Cut(strings.TrimSpace(value), ":")
		if !ok || strings.TrimSpace(rest) == "" {
			continue
		}
		switch strings.TrimSpace(prefix) {
		case "workspace_doc":
			refs = append(refs, MemoryGraphNodeRefInput{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "workspace_doc", RefID: strings.TrimSpace(rest), RefRole: "related", Weight: 0.75, MetadataJSON: "{}"})
		case "artifact_ref":
			refs = append(refs, MemoryGraphNodeRefInput{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "artifact_ref", RefID: strings.TrimSpace(rest), RefRole: "related", Weight: 0.75, MetadataJSON: "{}"})
		case "successor_session":
			refs = append(refs, MemoryGraphNodeRefInput{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "session", RefID: strings.TrimSpace(rest), RefRole: "successor", Weight: 1, MetadataJSON: "{}"})
		case "takeover_agent":
			refs = append(refs, MemoryGraphNodeRefInput{MemoryID: nodeID, WorkspaceID: record.WorkspaceID, RefKind: "agent", RefID: strings.TrimSpace(rest), RefRole: "takeover", Weight: 1, MetadataJSON: "{}"})
		}
	}
	return refs
}

func episodePackCompressionRatio(before, after int) float64 {
	if before <= 0 {
		return 0
	}
	value := 1 - (float64(after) / float64(before))
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func episodePackGraphConfidence(packMode string) float64 {
	if normalizeEpisodePackMode(packMode, "") == episodePackModeFallback {
		return 0.58
	}
	return 0.72
}

func normalizeMemoryGraphType(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "RAW_EVENT", "RAW_MESSAGE", "EPISODE_PACK", "FACT", "HYPOTHESIS", "DECISION", "DECISION_RECORD", "DISSENT", "DISSENT_MARKER", "DISSENT_CONTENT", "ALTERNATIVE_BRANCH", "CONSTRAINT", "BLOCKER", "BLOCKER_SYMPTOM", "BLOCKER_HYPOTHESIS", "ARTIFACT_DELTA", "BRIDGE_NOTE", "HANDOFF", "CLUSTER_BRIEF", "PROCEDURE", "ANTI_PROCEDURE", "COALITION_MOTIF", "SELF_MODEL", "GOAL_COMMITMENT", "POLICY_TRACE":
		return strings.ToUpper(strings.TrimSpace(raw))
	default:
		return "EPISODE_PACK"
	}
}

func memoryGraphFilterTypes(raw string) ([]string, []string) {
	switch normalizeMemoryGraphType(raw) {
	case "DECISION":
		return []string{"DECISION_RECORD"}, []string{"DECISION"}
	case "DECISION_RECORD":
		return []string{"DECISION_RECORD"}, []string{"DECISION", "DECISION_RECORD"}
	case "BLOCKER":
		return []string{"BLOCKER_SYMPTOM", "BLOCKER_HYPOTHESIS"}, []string{"BLOCKER", "INCIDENT"}
	case "BLOCKER_SYMPTOM":
		return []string{"BLOCKER_SYMPTOM"}, []string{"BLOCKER", "INCIDENT", "BLOCKER_SYMPTOM"}
	case "BLOCKER_HYPOTHESIS":
		return []string{"BLOCKER_HYPOTHESIS"}, []string{"BLOCKER_HYPOTHESIS"}
	default:
		return []string{normalizeMemoryGraphType(raw)}, nil
	}
}

func normalizeMemoryGraphVisibility(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "PRIVATE", "COALITION", "CLUSTER", "WORKSPACE":
		return strings.ToUpper(strings.TrimSpace(raw))
	default:
		return "WORKSPACE"
	}
}

func normalizeMemoryGraphLayer(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "EPISODIC", "SEMANTIC", "PROCEDURAL", "IDENTITY", "ARCHIVE":
		return strings.ToUpper(strings.TrimSpace(raw))
	default:
		return "SEMANTIC"
	}
}

func normalizeMemoryGraphEpistemicStatus(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "ALLEGED", "SUPPORTED", "VERIFIED", "DISPUTED", "RETRACTED":
		return strings.ToUpper(strings.TrimSpace(raw))
	default:
		return "SUPPORTED"
	}
}

func normalizeMemoryGraphLifecycleState(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "ACTIVE", "DORMANT", "SUPERSEDED", "ARCHIVED":
		return strings.ToUpper(strings.TrimSpace(raw))
	default:
		return "ACTIVE"
	}
}

func encodeStringArrayJSON(values []string) string {
	payload, err := json.Marshal(uniqueTrimmedStrings(values))
	if err != nil {
		return "[]"
	}
	return string(payload)
}

func encodeStringMapJSON(values map[string]any) string {
	payload, err := json.Marshal(values)
	if err != nil {
		return "{}"
	}
	return string(payload)
}

func decodeStringArrayJSON(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil
	}
	return uniqueTrimmedStrings(out)
}

func validateMemoryGraphNodeFilter(filter MemoryGraphNodeFilter) error {
	if err := validateOptionalMemoryGraphLayer(filter.MemoryLayer); err != nil {
		return err
	}
	if err := validateOptionalMemoryGraphVisibility(filter.Visibility); err != nil {
		return err
	}
	if err := validateOptionalMemoryGraphEpistemicStatus(filter.EpistemicStatus); err != nil {
		return err
	}
	if err := validateOptionalMemoryGraphLifecycleState(filter.LifecycleState); err != nil {
		return err
	}
	return nil
}

func validateOptionalMemoryGraphLayer(raw string) error {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	switch strings.ToUpper(trimmed) {
	case "EPISODIC", "SEMANTIC", "PROCEDURAL", "IDENTITY", "ARCHIVE":
		return nil
	default:
		return fmt.Errorf("memory_layer must be one of EPISODIC, SEMANTIC, PROCEDURAL, IDENTITY, or ARCHIVE")
	}
}

func ValidateOptionalMemoryGraphLayer(raw string) error {
	return validateOptionalMemoryGraphLayer(raw)
}

func validateOptionalMemoryGraphType(raw string) error {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	switch strings.ToUpper(trimmed) {
	case "RAW_EVENT", "RAW_MESSAGE", "EPISODE_PACK", "FACT", "HYPOTHESIS", "DECISION", "DECISION_RECORD", "DISSENT", "DISSENT_MARKER", "DISSENT_CONTENT", "ALTERNATIVE_BRANCH", "CONSTRAINT", "BLOCKER", "BLOCKER_SYMPTOM", "BLOCKER_HYPOTHESIS", "ARTIFACT_DELTA", "BRIDGE_NOTE", "HANDOFF", "CLUSTER_BRIEF", "PROCEDURE", "ANTI_PROCEDURE", "COALITION_MOTIF", "SELF_MODEL", "GOAL_COMMITMENT", "POLICY_TRACE":
		return nil
	default:
		return fmt.Errorf("memory_type must be one of RAW_EVENT, RAW_MESSAGE, EPISODE_PACK, FACT, HYPOTHESIS, DECISION, DECISION_RECORD, DISSENT, DISSENT_MARKER, DISSENT_CONTENT, ALTERNATIVE_BRANCH, CONSTRAINT, BLOCKER, BLOCKER_SYMPTOM, BLOCKER_HYPOTHESIS, ARTIFACT_DELTA, BRIDGE_NOTE, HANDOFF, CLUSTER_BRIEF, PROCEDURE, ANTI_PROCEDURE, COALITION_MOTIF, SELF_MODEL, GOAL_COMMITMENT, or POLICY_TRACE")
	}
}

func ValidateOptionalMemoryGraphType(raw string) error {
	return validateOptionalMemoryGraphType(raw)
}

func validateOptionalMemoryGraphVisibility(raw string) error {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	switch strings.ToUpper(trimmed) {
	case "PRIVATE", "COALITION", "CLUSTER", "WORKSPACE":
		return nil
	default:
		return fmt.Errorf("visibility must be one of PRIVATE, COALITION, CLUSTER, or WORKSPACE")
	}
}

func ValidateOptionalMemoryGraphVisibility(raw string) error {
	return validateOptionalMemoryGraphVisibility(raw)
}

func validateOptionalMemoryGraphOriginKind(raw string) error {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	switch strings.ToLower(trimmed) {
	case "workspace_memory", "knowledge_claim", "episode_pack":
		return nil
	default:
		return fmt.Errorf("origin_kind must be one of workspace_memory, knowledge_claim, or episode_pack")
	}
}

func ValidateOptionalMemoryGraphOriginKind(raw string) error {
	return validateOptionalMemoryGraphOriginKind(raw)
}

func validateOptionalMemoryGraphEpistemicStatus(raw string) error {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	switch strings.ToUpper(trimmed) {
	case "ALLEGED", "SUPPORTED", "VERIFIED", "DISPUTED", "RETRACTED":
		return nil
	default:
		return fmt.Errorf("epistemic_status must be one of ALLEGED, SUPPORTED, VERIFIED, DISPUTED, or RETRACTED")
	}
}

func ValidateOptionalMemoryGraphEpistemicStatus(raw string) error {
	return validateOptionalMemoryGraphEpistemicStatus(raw)
}

func validateOptionalMemoryGraphLifecycleState(raw string) error {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	switch strings.ToUpper(trimmed) {
	case "ACTIVE", "DORMANT", "SUPERSEDED", "ARCHIVED":
		return nil
	default:
		return fmt.Errorf("lifecycle_state must be one of ACTIVE, DORMANT, SUPERSEDED, or ARCHIVED")
	}
}

func ValidateOptionalMemoryGraphLifecycleState(raw string) error {
	return validateOptionalMemoryGraphLifecycleState(raw)
}

func uniqueTrimmedStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}
